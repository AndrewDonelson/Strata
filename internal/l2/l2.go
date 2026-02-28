// Package l2 provides the Redis tier cache adapter.
package l2

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/AndrewDonelson/strata/internal/codec"
	"github.com/redis/go-redis/v9"
)

const keyFmt = "%s:%s:%v"

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
		opts.Codec = codec.JSON{}
	}
	return &Store{client: opts.Client, codec: opts.Codec, keyPrefix: opts.KeyPrefix}
}

// key returns the Redis key for the given schema and id.
func (s *Store) key(schema, keyPrefix string, id any) string {
	prefix := schema
	if keyPrefix != "" {
		prefix = keyPrefix
	}
	if s.keyPrefix != "" {
		return fmt.Sprintf(keyFmt, s.keyPrefix+":"+prefix, schema, id)
	}
	return fmt.Sprintf(keyFmt, prefix, schema, id)
}

// Set stores a value in Redis with the given TTL.
func (s *Store) Set(ctx context.Context, schema, keyPrefix string, id any, value any, ttl time.Duration) error {
	b, err := s.codec.Marshal(value)
	if err != nil {
		return fmt.Errorf("l2 marshal: %w", err)
	}
	k := s.key(schema, keyPrefix, id)
	if err := s.client.Set(ctx, k, b, ttl).Err(); err != nil {
		return fmt.Errorf("l2 set %s: %w", k, err)
	}
	return nil
}

// Get retrieves and deserializes a value from Redis into dest.
// Returns (without error) when key is missing; caller detects miss by checking dest.
func (s *Store) Get(ctx context.Context, schema, keyPrefix string, id, dest any) error {
	k := s.key(schema, keyPrefix, id)
	b, err := s.client.Get(ctx, k).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			s.misses++
			return nil
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
func (s *Store) Exists(ctx context.Context, schema, keyPrefix string, id any) (bool, error) {
	k := s.key(schema, keyPrefix, id)
	n, err := s.client.Exists(ctx, k).Result()
	if err != nil {
		return false, fmt.Errorf("l2 exists %s: %w", k, err)
	}
	return n > 0, nil
}

// Delete removes a key from Redis.
func (s *Store) Delete(ctx context.Context, schema, keyPrefix string, id any) error {
	k := s.key(schema, keyPrefix, id)
	if err := s.client.Del(ctx, k).Err(); err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("l2 delete %s: %w", k, err)
	}
	return nil
}

// KV is a key-value pair for batch operations.
type KV struct {
	ID    any
	Value any
}

// SetMany writes multiple values using a Redis pipeline (single round-trip).
func (s *Store) SetMany(ctx context.Context, schema, keyPrefix string, kvs []KV, ttl time.Duration) error {
	pipe := s.client.Pipeline()
	for _, kv := range kvs {
		b, err := s.codec.Marshal(kv.Value)
		if err != nil {
			return fmt.Errorf("l2 marshal id=%v: %w", kv.ID, err)
		}
		pipe.Set(ctx, s.key(schema, keyPrefix, kv.ID), b, ttl)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// GetMany retrieves multiple values using a Redis pipeline.
// Returns a map of id (formatted) -> raw bytes; missing keys are absent.
func (s *Store) GetMany(ctx context.Context, schema, keyPrefix string, ids []any) (map[string][]byte, error) {
	pipe := s.client.Pipeline()
	cmds := make([]*redis.StringCmd, len(ids))
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
			return nil, fmt.Errorf("l2 get-many id=%v: %w", ids[i], err)
		}
		result[fmt.Sprintf("%v", ids[i])] = b
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
			if err := s.client.Del(ctx, keys...).Err(); err != nil {
				return fmt.Errorf("l2 del: %w", err)
			}
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
func (s *Store) FormatKey(schema, keyPrefix string, id any) string {
	return s.key(schema, keyPrefix, id)
}
