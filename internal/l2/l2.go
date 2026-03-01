// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// l2.go — Redis-backed L2 cache tier: standard CRUD operations, pre-computed
// key-prefix hot-path variants (SetP/GetP/ExistsP/DeleteP), pipeline batch
// ops, pub/sub invalidation support, and the ErrMiss sentinel that drives
// clean tier fallthrough in the router.

// Package l2 provides the Redis tier cache adapter.
package l2

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/AndrewDonelson/strata/internal/codec"
	"github.com/redis/go-redis/v9"
)

// ErrMiss is returned by Get and GetP when the key does not exist in Redis.
// Callers use errors.Is(err, l2.ErrMiss) to distinguish a cache miss from a
// genuine Redis error.
var ErrMiss = errors.New("l2: miss")

// cmdSlicePool pools []*redis.StringCmd slices to eliminate the per-call
// make() allocation inside GetMany.  Slices are reset to zero length before
// pooling so that the GC can collect any stale *StringCmd references.
var cmdSlicePool = sync.Pool{
	New: func() any {
		s := make([]*redis.StringCmd, 0, 16)
		return &s
	},
}

// setArgsPool pools the []interface{} slice used to build Redis SET command
// arguments, eliminating the make([]interface{}, 3, 5) allocation inside
// go-redis cmdable.Set on every hot-path write.
// The pattern mirrors cmdSlicePool used in GetMany.
var setArgsPool = sync.Pool{
	New: func() any {
		s := make([]interface{}, 0, 6) // "set", key, value, "ex"/"px", ttl, (spare)
		return &s
	},
}

// Store is the L2 Redis cache adapter.
type Store struct {
	client    redis.UniversalClient
	codec     codec.Codec
	keyPrefix string
	hits      int64
	misses    int64
}

// Options configures a new L2 Store.
type Options struct {
	Client    redis.UniversalClient
	Codec     codec.Codec
	KeyPrefix string
}

// New creates a new L2 Store.
func New(opts Options) *Store {
	if opts.Codec == nil {
		opts.Codec = codec.MsgPack{}
	}
	return &Store{client: opts.Client, codec: opts.Codec, keyPrefix: opts.KeyPrefix}
}

// key returns the Redis key for the given schema and id.
// Uses string concatenation instead of fmt.Sprintf to eliminate intermediate
// allocations on the hot path.
func (s *Store) key(schema, keyPrefix, id string) string {
	prefix := schema
	if keyPrefix != "" {
		prefix = keyPrefix
	}
	if s.keyPrefix != "" {
		return s.keyPrefix + ":" + prefix + ":" + schema + ":" + id
	}
	return prefix + ":" + schema + ":" + id
}

// set sends a Redis SET command using a pooled args slice, avoiding the
// make([]interface{}, 3, 5) that go-redis cmdable.Set allocates on every call.
// The expiry logic mirrors go-redis's own usePrecise/formatMs/formatSec logic:
//   - ttl < 1s  → PX (millisecond precision)
//   - ttl >= 1s → EX (second precision)
//   - ttl == redis.KeepTTL → KEEPTTL
//   - ttl <= 0 (other) → no expiry argument
//
// Safety: client.Do() is synchronous — it fully encodes args to RESP and reads
// the server response before returning.  The returned *Cmd has no external
// references after .Err() is called, so resetting the pooled slice is safe.
func (s *Store) set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	ap := setArgsPool.Get().(*[]interface{})
	args := (*ap)[:0]
	switch {
	case ttl > 0 && ttl < time.Second: // sub-second → PX
		args = append(args, "set", key, value, "px", ttl.Milliseconds())
	case ttl > 0: // whole-second → EX
		args = append(args, "set", key, value, "ex", int64(ttl.Seconds()))
	case ttl == redis.KeepTTL: // keep existing TTL (requires Redis ≥ 6.0)
		args = append(args, "set", key, value, "keepttl")
	default: // ttl == 0 or negative (non-KeepTTL): persist indefinitely
		args = append(args, "set", key, value)
	}
	err := s.client.Do(ctx, args...).Err()
	// Clear element references before pooling so the GC can reclaim old values.
	for i := range args {
		args[i] = nil
	}
	*ap = args[:0]
	setArgsPool.Put(ap)
	return err
}

// Set stores a value in Redis with the given TTL.
func (s *Store) Set(ctx context.Context, schema, keyPrefix, id string, value any, ttl time.Duration) error {
	b, err := s.codec.Marshal(value)
	if err != nil {
		return fmt.Errorf("l2 marshal: %w", err)
	}
	k := s.key(schema, keyPrefix, id)
	if err := s.set(ctx, k, b, ttl); err != nil {
		return fmt.Errorf("l2 set %s: %w", k, err)
	}
	return nil
}

// Get retrieves and deserializes a value from Redis into dest.
// Returns ErrMiss when key is missing; caller must check errors.Is(err, l2.ErrMiss).
func (s *Store) Get(ctx context.Context, schema, keyPrefix, id string, dest any) error {
	k := s.key(schema, keyPrefix, id)
	b, err := s.client.Get(ctx, k).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			s.misses++
			return ErrMiss
		}
		return fmt.Errorf("l2 get %s: %w", k, err)
	}
	s.hits++
	if err := s.codec.Unmarshal(b, dest); err != nil {
		return fmt.Errorf("l2 unmarshal: %w", err)
	}
	return nil
}

// Exists checks whether a key exists in Redis.
func (s *Store) Exists(ctx context.Context, schema, keyPrefix, id string) (bool, error) {
	k := s.key(schema, keyPrefix, id)
	n, err := s.client.Exists(ctx, k).Result()
	if err != nil {
		return false, fmt.Errorf("l2 exists %s: %w", k, err)
	}
	return n > 0, nil
}

// Delete removes a key from Redis.
func (s *Store) Delete(ctx context.Context, schema, keyPrefix, id string) error {
	k := s.key(schema, keyPrefix, id)
	if err := s.client.Del(ctx, k).Err(); err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("l2 delete %s: %w", k, err)
	}
	return nil
}

// ── Pre-computed-prefix hot-path methods ─────────────────────────────────────
// These variants accept a pre-built base prefix (e.g. compiledSchema.l2Prefix)
// so the caller never recalculates it on every call.  The compiled prefix is
// stored once at schema-registration time in the form
//   schemaName + ":" + schemaName + ":"
// which matches the output of key(schema, "", id) when no global keyPrefix is set.

// keyP builds a Redis key from a pre-computed base prefix and an id.
func (s *Store) keyP(base, id string) string {
	if s.keyPrefix != "" {
		return s.keyPrefix + ":" + base + id
	}
	return base + id
}

// SetP stores a value using a pre-computed key prefix.
func (s *Store) SetP(ctx context.Context, l2Prefix, id string, value any, ttl time.Duration) error {
	b, err := s.codec.Marshal(value)
	if err != nil {
		return fmt.Errorf("l2 marshal: %w", err)
	}
	k := s.keyP(l2Prefix, id)
	if err := s.set(ctx, k, b, ttl); err != nil {
		return fmt.Errorf("l2 set %s: %w", k, err)
	}
	return nil
}

// GetP retrieves and deserializes a value using a pre-computed key prefix.
func (s *Store) GetP(ctx context.Context, l2Prefix, id string, dest any) error {
	k := s.keyP(l2Prefix, id)
	b, err := s.client.Get(ctx, k).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			s.misses++
			return ErrMiss
		}
		return fmt.Errorf("l2 get %s: %w", k, err)
	}
	s.hits++
	if err := s.codec.Unmarshal(b, dest); err != nil {
		return fmt.Errorf("l2 unmarshal: %w", err)
	}
	return nil
}

// ExistsP checks key existence using a pre-computed key prefix.
func (s *Store) ExistsP(ctx context.Context, l2Prefix, id string) (bool, error) {
	k := s.keyP(l2Prefix, id)
	n, err := s.client.Exists(ctx, k).Result()
	if err != nil {
		return false, fmt.Errorf("l2 exists %s: %w", k, err)
	}
	return n > 0, nil
}

// DeleteP removes a key using a pre-computed key prefix.
func (s *Store) DeleteP(ctx context.Context, l2Prefix, id string) error {
	k := s.keyP(l2Prefix, id)
	if err := s.client.Del(ctx, k).Err(); err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("l2 delete %s: %w", k, err)
	}
	return nil
}

// KV is a key-value pair for batch operations.
type KV struct {
	ID    string
	Value any
}

// SetMany writes multiple values using a Redis pipeline (single round-trip).
func (s *Store) SetMany(ctx context.Context, schema, keyPrefix string, kvs []KV, ttl time.Duration) error {
	pipe := s.client.Pipeline()
	for _, kv := range kvs {
		b, err := s.codec.Marshal(kv.Value)
		if err != nil {
			return fmt.Errorf("l2 marshal id=%s: %w", kv.ID, err)
		}
		pipe.Set(ctx, s.key(schema, keyPrefix, kv.ID), b, ttl)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// GetMany retrieves multiple values using a Redis pipeline.
// Returns a map of id -> raw bytes; missing keys are absent.
func (s *Store) GetMany(ctx context.Context, schema, keyPrefix string, ids []string) (map[string][]byte, error) {
	pipe := s.client.Pipeline()

	// Borrow a *redis.StringCmd slice from the pool to avoid a per-call
	// make() allocation.  We clear the slice before returning it so that
	// stale *StringCmd pointers do not prevent GC of those objects.
	sp := cmdSlicePool.Get().(*[]*redis.StringCmd)
	cmds := (*sp)[:0]
	if cap(cmds) < len(ids) {
		cmds = make([]*redis.StringCmd, 0, len(ids))
	}
	cmds = cmds[:len(ids)]
	defer func() {
		for i := range cmds {
			cmds[i] = nil // clear refs so GC can collect the StringCmd values
		}
		*sp = cmds[:0]
		cmdSlicePool.Put(sp)
	}()

	for i, id := range ids {
		cmds[i] = pipe.Get(ctx, s.key(schema, keyPrefix, id))
	}
	_, _ = pipe.Exec(ctx)
	result := make(map[string][]byte, len(ids))
	for i, cmd := range cmds {
		b, err := cmd.Bytes()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			return nil, fmt.Errorf("l2 get-many id=%s: %w", ids[i], err)
		}
		result[ids[i]] = b
	}
	return result, nil
}

// InvalidateAll removes all keys for a schema using SCAN+DEL (production-safe).
func (s *Store) InvalidateAll(ctx context.Context, schema, keyPrefix string) error {
	prefix := schema
	if keyPrefix != "" {
		prefix = keyPrefix
	}
	if s.keyPrefix != "" {
		prefix = s.keyPrefix + ":" + prefix
	}
	pattern := prefix + ":" + schema + ":*"
	var cursor uint64
	for {
		keys, next, err := s.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return fmt.Errorf("l2 scan: %w", err)
		}
		if len(keys) > 0 {
			_ = s.client.Del(ctx, keys...).Err() // best-effort; mirrors routerDelete L2 treatment
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return nil
}

// Publish sends an invalidation message to the given channel.
func (s *Store) Publish(ctx context.Context, channel string, payload []byte) error {
	return s.client.Publish(ctx, channel, payload).Err()
}

// Subscribe returns a pub/sub subscription on the given channel.
func (s *Store) Subscribe(ctx context.Context, channel string) *redis.PubSub {
	return s.client.Subscribe(ctx, channel)
}

// Ping checks that Redis is reachable.
func (s *Store) Ping(ctx context.Context) error {
	return s.client.Ping(ctx).Err()
}

// Stats returns hit and miss counts.
type Stats struct {
	Hits   int64
	Misses int64
}

// Stats returns current statistics.
func (s *Store) Stats() Stats {
	return Stats{Hits: s.hits, Misses: s.misses}
}

// SetRaw stores pre-encoded bytes directly.
func (s *Store) SetRaw(ctx context.Context, key string, data []byte, ttl time.Duration) error {
	return s.client.Set(ctx, key, data, ttl).Err()
}

// GetRaw retrieves raw bytes without decoding.
func (s *Store) GetRaw(ctx context.Context, key string) ([]byte, error) {
	b, err := s.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	return b, err
}

// DelRaw removes an arbitrary key.
func (s *Store) DelRaw(ctx context.Context, key string) error {
	return s.client.Del(ctx, key).Err()
}

// FormatKey returns the formatted cache key for external use.
func (s *Store) FormatKey(schema, keyPrefix, id string) string {
	return s.key(schema, keyPrefix, id)
}
