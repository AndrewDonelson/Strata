// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// strata.go — public DataStore API: NewDataStore, Get, Set, Delete, Search,
// SearchTyped, SearchCached, Count, Exists, WarmCache, InvalidateAll, Tx,
// Stats, Close, and all top-level configuration and policy types.

package strata

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/AndrewDonelson/strata/internal/clock"
	"github.com/AndrewDonelson/strata/internal/codec"
	"github.com/AndrewDonelson/strata/internal/l1"
	"github.com/AndrewDonelson/strata/internal/l2"
	l3pkg "github.com/AndrewDonelson/strata/internal/l3"
	l4pkg "github.com/AndrewDonelson/strata/internal/l4"
	"github.com/AndrewDonelson/strata/internal/metrics"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// Re-export types so callers only import this package.
type MetricsRecorder = metrics.MetricsRecorder
type Codec = codec.Codec

// ────────────────────────────────────────────────────────────────────────────
// Config
// ────────────────────────────────────────────────────────────────────────────

// L1PoolConfig configures the in-memory L1 cache tier.
type L1PoolConfig struct {
	MaxEntries int
	Eviction   EvictionPolicy
}

// L2PoolConfig configures the Redis L2 cache tier client.
type L2PoolConfig struct {
	PoolSize     int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// L3PoolConfig configures the PostgreSQL L3 connection pool.
type L3PoolConfig struct {
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
}

// L4Config configures the optional L4 distributed peer-to-peer sync layer.
// Set Enabled = true to activate; choose Mode and set Port/DataDir/Quorum as
// needed.  Individual schemas opt-in via Schema.L4.Enabled.
type L4Config struct {
	Enabled        bool          // false = L4 is entirely inactive (default)
	Mode           string        // "peer" (in-memory) or "ledger" (BoltDB-backed)
	Port           int           // TCP listen port; default 7743
	DataDir        string        // BoltDB directory for ledger mode; default "/var/lib/strata/l4"
	SyncInterval   time.Duration // gossip sync frequency; default 30s
	MaxPeers       int           // max simultaneous peer connections; default 50
	Quorum         int           // confirmations needed for pending → confirmed; default 3
	BootstrapPeers []string      // "host:port" addresses to dial on startup
	DNSSeed        string        // DNS seed hostname for peer discovery
	NodeKeyPath    string        // path to load/persist the Ed25519 node private key
}

// Config contains all DataStore configuration.
type Config struct {
	// DSNs
	PostgresDSN   string
	RedisAddr     string
	RedisPassword string
	RedisDB       int

	// Pool sizes
	L1Pool L1PoolConfig
	L2Pool L2PoolConfig
	L3Pool L3PoolConfig

	// TTLs
	DefaultL1TTL time.Duration
	DefaultL2TTL time.Duration

	// Write behaviour
	DefaultWriteMode          WriteMode
	WriteBehindFlushInterval  time.Duration
	WriteBehindFlushThreshold int
	WriteBehindMaxRetry       int

	// Invalidation
	InvalidationChannel string

	// L4 distributed peer sync (optional; schemas opt-in via Schema.L4.Enabled)
	L4 L4Config

	// Optional overrideable components
	Codec   codec.Codec
	Clock   clock.Clock
	Metrics metrics.MetricsRecorder
	Logger  Logger

	// Encryption key (must be 32 bytes for AES-256-GCM; nil = disabled).
	EncryptionKey []byte

	// EmbeddingProvider — required when any registered schema has a strata:"vector" field.
	// If nil and a vector schema is registered, ds.Migrate() returns ErrNoEmbeddingProvider.
	// Use NewOllamaProvider or NewOpenAIProvider to create a provider.
	EmbeddingProvider EmbeddingProvider
}

func (c *Config) defaults() {
	if c.Codec == nil {
		c.Codec = codec.MsgPack{}
	}
	if c.Clock == nil {
		c.Clock = clock.Real{}
	}
	if c.Metrics == nil {
		c.Metrics = metrics.Noop{}
	}
	if c.Logger == nil {
		c.Logger = noopLogger{}
	}
	if c.DefaultL1TTL == 0 {
		c.DefaultL1TTL = 5 * time.Minute
	}
	if c.DefaultL2TTL == 0 {
		c.DefaultL2TTL = 30 * time.Minute
	}
	if c.L1Pool.MaxEntries == 0 {
		c.L1Pool.MaxEntries = 100_000
	}
	if c.L3Pool.MaxConns == 0 {
		c.L3Pool.MaxConns = 20
	}
	if c.L3Pool.MinConns == 0 {
		c.L3Pool.MinConns = 2
	}
	if c.L3Pool.MaxConnLifetime == 0 {
		c.L3Pool.MaxConnLifetime = 30 * time.Minute
	}
	if c.L3Pool.MaxConnIdleTime == 0 {
		c.L3Pool.MaxConnIdleTime = 10 * time.Minute
	}
}

// ────────────────────────────────────────────────────────────────────────────
// l3Backend — private interface that *l3.Store satisfies
// ────────────────────────────────────────────────────────────────────────────

// l3Backend is the private persistence interface satisfied by *l3.Store and
// by test mocks. Extracting it lets unit tests inject deliberate L3 failures
// without a live PostgreSQL instance.
type l3Backend interface {
	Upsert(ctx context.Context, table string, columns []string, values []any, pkColumn string) error
	DeleteByID(ctx context.Context, table, pkColumn string, id any) error
	Query(ctx context.Context, sql string, args []any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args []any) pgx.Row
	Exec(ctx context.Context, sql string, args []any) error
	Exists(ctx context.Context, table, pkColumn string, id any) (bool, error)
	Count(ctx context.Context, table, where string, args []any) (int64, error)
	BeginTx(ctx context.Context) (pgx.Tx, error)
	Close()
}

// Compile-time assertion: *l3.Store must implement l3Backend.
var _ l3Backend = (*l3pkg.Store)(nil)

// ────────────────────────────────────────────────────────────────────────────
// Stats
// ────────────────────────────────────────────────────────────────────────────

type storeStats struct {
	Gets    atomic.Int64
	Sets    atomic.Int64
	Deletes atomic.Int64
	Errors  atomic.Int64
}

// Stats is the snapshot returned by DataStore.Stats().
type Stats struct {
	Gets       int64
	Sets       int64
	Deletes    int64
	Errors     int64
	DirtyCount int64
	L1Entries  int64
}

// ────────────────────────────────────────────────────────────────────────────
// DataStore
// ────────────────────────────────────────────────────────────────────────────

// DataStore is the main entry-point for the Strata library.
type DataStore struct {
	cfg       Config
	registry  *schemaRegistry
	l1        *l1.Store
	l2        *l2.Store
	l3        l3Backend
	l4layer   l4pkg.L4Layer // nil when L4 is disabled
	l4nodeID  string        // cached node identity (Ed25519 pub-key hex)
	sync      *syncEngine
	stats     storeStats
	metrics   metrics.MetricsRecorder
	logger    Logger
	encryptor Encryptor
	closed    atomic.Bool
}

// NewDataStore creates and initialises a DataStore from the provided Config.
func NewDataStore(cfg Config) (*DataStore, error) {
	cfg.defaults()

	ds := &DataStore{
		cfg:      cfg,
		registry: newSchemaRegistry(),
		metrics:  cfg.Metrics,
		logger:   cfg.Logger,
	}

	// Encryption
	if len(cfg.EncryptionKey) > 0 {
		enc, err := NewAES256GCM(cfg.EncryptionKey)
		if err != nil {
			return nil, fmt.Errorf("strata: encryption init: %w", err)
		}
		ds.encryptor = enc
	}

	// L1
	ds.l1 = l1.New(l1.Options{
		MaxEntries: cfg.L1Pool.MaxEntries,
		Eviction:   l1.EvictionPolicy(cfg.L1Pool.Eviction),
		TTL:        cfg.DefaultL1TTL,
		Clock:      cfg.Clock,
	})

	// L2
	if cfg.RedisAddr != "" {
		redisClient := redis.NewClient(&redis.Options{
			Addr:         cfg.RedisAddr,
			Password:     cfg.RedisPassword,
			DB:           cfg.RedisDB,
			PoolSize:     cfg.L2Pool.PoolSize,
			DialTimeout:  cfg.L2Pool.DialTimeout,
			ReadTimeout:  cfg.L2Pool.ReadTimeout,
			WriteTimeout: cfg.L2Pool.WriteTimeout,
		})
		ds.l2 = l2.New(l2.Options{
			Client: redisClient,
			Codec:  cfg.Codec,
		})
	}

	// L3
	if cfg.PostgresDSN != "" {
		pgCfg, err := pgxpool.ParseConfig(cfg.PostgresDSN)
		if err != nil {
			return nil, fmt.Errorf("strata: postgres config: %w", err)
		}
		pgCfg.MaxConns = cfg.L3Pool.MaxConns
		pgCfg.MinConns = cfg.L3Pool.MinConns
		pgCfg.MaxConnLifetime = cfg.L3Pool.MaxConnLifetime
		pgCfg.MaxConnIdleTime = cfg.L3Pool.MaxConnIdleTime

		pool, err := pgxpool.NewWithConfig(context.Background(), pgCfg)
		if err != nil {
			return nil, fmt.Errorf("strata: postgres pool: %w", err)
		}
		ds.l3 = l3pkg.New(pool, nil)
	}

	// L4 distributed sync layer (optional)
	if cfg.L4.Enabled {
		l4cfg := l4pkg.Config{
			Enabled:        true,
			Mode:           cfg.L4.Mode,
			Port:           cfg.L4.Port,
			DataDir:        cfg.L4.DataDir,
			SyncInterval:   cfg.L4.SyncInterval,
			MaxPeers:       cfg.L4.MaxPeers,
			Quorum:         cfg.L4.Quorum,
			BootstrapPeers: cfg.L4.BootstrapPeers,
			DNSSeed:        cfg.L4.DNSSeed,
			NodeKeyPath:    cfg.L4.NodeKeyPath,
		}
		if err := l4cfg.Validate(); err != nil {
			return nil, fmt.Errorf("strata: l4 config: %w", err)
		}
		layer, err := l4pkg.New(l4cfg)
		if err != nil {
			return nil, fmt.Errorf("strata: l4 init: %w", err)
		}
		ds.l4layer = layer
		ds.l4nodeID = layer.Status().NodeID
	}

	// Sync engine
	ds.sync = newSyncEngine(ds)
	ds.sync.start()

	return ds, nil
}

// ────────────────────────────────────────────────────────────────────────────
// L4 helpers — called by the router after successful L3 operations
// ────────────────────────────────────────────────────────────────────────────

// syncToL4 publishes value to the L4 layer for schemas that have L4.Enabled.
// Called by the router immediately after a successful L3 write.
// Errors are logged but never returned — L4 is best-effort.
func (ds *DataStore) syncToL4(_ context.Context, cs *compiledSchema, id string, value any) {
	if ds.l4layer == nil || !cs.L4.Enabled {
		return
	}
	payload, err := structToL4Payload(value)
	if err != nil || payload == nil {
		if ds.logger != nil {
			ds.logger.Warn("strata: l4 payload marshal failed", "schema", cs.Name, "id", id, "err", err)
		}
		return
	}
	appID := cs.L4.AppID
	if appID == "" {
		appID = cs.Name
	}
	if _, err := ds.l4layer.Publish(appID, ds.l4nodeID, payload); err != nil {
		if ds.logger != nil {
			ds.logger.Warn("strata: l4 publish failed", "schema", cs.Name, "id", id, "err", err)
		}
	}
}

// revokeFromL4 revokes the L4 record on a schema Delete if SyncDeletes is enabled.
// Errors are logged but never returned.
func (ds *DataStore) revokeFromL4(_ context.Context, cs *compiledSchema, id string) {
	if ds.l4layer == nil || !cs.L4.Enabled || !cs.L4.SyncDeletes {
		return
	}
	appID := cs.L4.AppID
	if appID == "" {
		appID = cs.Name
	}
	if err := ds.l4layer.Revoke(appID, id); err != nil {
		if ds.logger != nil {
			ds.logger.Warn("strata: l4 revoke failed", "schema", cs.Name, "id", id, "err", err)
		}
	}
}

// structToL4Payload converts an arbitrary struct/value to map[string]interface{}
// via JSON round-trip so it can be stored as an L4 record payload.
func structToL4Payload(value any) (map[string]interface{}, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Schema registration
// ────────────────────────────────────────────────────────────────────────────

// Register compiles and stores a Schema definition.
// Returns ErrInvalidTagForType if a strata:"vector" tag is applied to a
// non-pgvector.Vector field, or ErrInvalidIndexType if an IVFFlat/HNSW index
// is applied to a non-vector field.
func (ds *DataStore) Register(s Schema) error {
	cs, err := ds.registry.register(s)
	if err != nil {
		return err
	}
	return ds.validateAndWireVectorSchema(cs, s)
}

// validateAndWireVectorSchema validates vector field types and index types,
// then populates cs.vectorDimension from the EmbeddingProvider if available.
func (ds *DataStore) validateAndWireVectorSchema(cs *compiledSchema, s Schema) error {
	pgvecType := l3pkg.PgvectorType()

	// 1. Validate: any strata:"vector" field must be of type pgvector.Vector
	if cs.hasVectorFields {
		for _, col := range cs.columns {
			if col.IsVector {
				ft := cs.modelType.Field(col.FieldIndex).Type
				if ft != pgvecType {
					return fmt.Errorf("%w: field %q has type %s, expected pgvector.Vector",
						ErrInvalidTagForType, col.FieldName, ft)
				}
			}
		}
	}

	// 2. Validate: IVFFlat / HNSW indexes must target a vector field
	vectorFieldNames := make(map[string]bool)
	for _, col := range cs.columns {
		if col.IsVector {
			vectorFieldNames[col.Name] = true
		}
	}
	for _, idx := range s.Indexes {
		if idx.Type == IndexIVFFlat || idx.Type == IndexHNSW {
			if len(idx.Fields) == 0 {
				return fmt.Errorf("%w: index %q has no fields", ErrInvalidIndexType, idx.Name)
			}
			if !vectorFieldNames[idx.Fields[0]] {
				return fmt.Errorf("%w: index on field %q is not a vector field",
					ErrInvalidIndexType, idx.Fields[0])
			}
		}
	}

	// 3. Wire dimension from EmbeddingProvider (if present) and validate it.
	if cs.hasVectorFields && ds.cfg.EmbeddingProvider != nil {
		cs.vectorDimension = ds.cfg.EmbeddingProvider.Dimensions()
		if cs.vectorDimension <= 0 {
			return fmt.Errorf("%w: provider %q returned dimension %d",
				ErrVectorDimensionMismatch, ds.cfg.EmbeddingProvider.ModelID(), cs.vectorDimension)
		}
	}

	return nil
}

// ────────────────────────────────────────────────────────────────────────────
// CRUD
// ────────────────────────────────────────────────────────────────────────────

// Get fetches the record with the given id into dest.
// dest must be a pointer to the model type of the registered schema.
func (ds *DataStore) Get(ctx context.Context, schemaName, id string, dest any) error {
	if ds.closed.Load() {
		return ErrUnavailable
	}
	cs, err := ds.registry.get(schemaName)
	if err != nil {
		return err
	}
	ds.stats.Gets.Add(1)
	start := ds.cfg.Clock.Now()
	err = ds.routerGet(ctx, cs, id, dest)
	ds.metrics.RecordLatency(schemaName, "get", time.Since(start))
	if err != nil {
		ds.stats.Errors.Add(1)
		ds.metrics.RecordError(schemaName, "get")
	}
	if cs.Hooks.AfterGet != nil {
		cs.Hooks.AfterGet(ctx, dest)
	}
	return err
}

// GetTyped is a generic convenience wrapper around Get.
func GetTyped[T any](ctx context.Context, ds *DataStore, schemaName, id string) (*T, error) {
	var dest T
	if err := ds.Get(ctx, schemaName, id, &dest); err != nil {
		return nil, err
	}
	return &dest, nil
}

// Set stores value under schemaName with the given id.
func (ds *DataStore) Set(ctx context.Context, schemaName, id string, value any) error {
	if ds.closed.Load() {
		return ErrUnavailable
	}
	cs, err := ds.registry.get(schemaName)
	if err != nil {
		return err
	}
	if cs.Hooks.BeforeSet != nil {
		if err := cs.Hooks.BeforeSet(ctx, value); err != nil {
			return err
		}
	}
	ds.stats.Sets.Add(1)
	start := ds.cfg.Clock.Now()
	err = ds.routerSet(ctx, cs, id, value)
	ds.metrics.RecordLatency(schemaName, "set", time.Since(start))
	if err != nil {
		ds.stats.Errors.Add(1)
		ds.metrics.RecordError(schemaName, "set")
	} else if cs.Hooks.AfterSet != nil {
		cs.Hooks.AfterSet(ctx, value)
	}
	return err
}

// SetMany stores multiple id→value pairs for the given schema.
func (ds *DataStore) SetMany(ctx context.Context, schemaName string, pairs map[string]any) error {
	if ds.closed.Load() {
		return ErrUnavailable
	}
	cs, err := ds.registry.get(schemaName)
	if err != nil {
		return err
	}
	for id, v := range pairs {
		if err := ds.routerSet(ctx, cs, id, v); err != nil {
			return err
		}
	}
	return nil
}

// Delete removes a record from all tiers.
func (ds *DataStore) Delete(ctx context.Context, schemaName, id string) error {
	if ds.closed.Load() {
		return ErrUnavailable
	}
	cs, err := ds.registry.get(schemaName)
	if err != nil {
		return err
	}
	ds.stats.Deletes.Add(1)
	return ds.routerDelete(ctx, cs, id)
}

// ────────────────────────────────────────────────────────────────────────────
// Search
// ────────────────────────────────────────────────────────────────────────────

// Search runs q against L3 and returns the results in destSlice (pointer to slice).
func (ds *DataStore) Search(ctx context.Context, schemaName string, q *Query, destSlice any) error {
	if ds.closed.Load() {
		return ErrUnavailable
	}
	cs, err := ds.registry.get(schemaName)
	if err != nil {
		return err
	}
	return ds.routerSearch(ctx, cs, q, destSlice)
}

// SearchTyped is a generic convenience wrapper around Search.
func SearchTyped[T any](ctx context.Context, ds *DataStore, schemaName string, q *Query) ([]T, error) {
	var results []T
	if err := ds.Search(ctx, schemaName, q, &results); err != nil {
		return nil, err
	}
	return results, nil
}

// SearchCached runs q against L3; caches list result in L2 by SQL fingerprint.
func (ds *DataStore) SearchCached(ctx context.Context, schemaName string, q *Query, destSlice any) error {
	if ds.l2 == nil {
		return ds.Search(ctx, schemaName, q, destSlice)
	}
	cs, err := ds.registry.get(schemaName)
	if err != nil {
		return err
	}
	if q == nil {
		empty := Q().Build()
		q = &empty
	}
	cols := colNames(cs)
	sql, args := q.ToSQL(cs.tableName, cols, 100)
	cacheKey := fmt.Sprintf("search:%s:%08x", schemaName, hashString(sql+fmt.Sprint(args)))
	ttl := cs.L2.TTL
	if ttl == 0 {
		ttl = ds.cfg.DefaultL2TTL
	}

	if raw, err2 := ds.l2.GetRaw(ctx, cacheKey); err2 == nil && raw != nil {
		return ds.cfg.Codec.Unmarshal(raw, destSlice)
	}

	if err := ds.routerSearch(ctx, cs, q, destSlice); err != nil {
		return err
	}
	b, err := ds.cfg.Codec.Marshal(destSlice)
	if err == nil {
		_ = ds.l2.SetRaw(ctx, cacheKey, b, ttl)
	}
	return nil
}

func hashString(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

// ────────────────────────────────────────────────────────────────────────────
// Exists / Count
// ────────────────────────────────────────────────────────────────────────────

// Exists returns true if a record exists in any tier.
func (ds *DataStore) Exists(ctx context.Context, schemaName, id string) (bool, error) {
	if ds.closed.Load() {
		return false, ErrUnavailable
	}
	cs, err := ds.registry.get(schemaName)
	if err != nil {
		return false, err
	}
	l1Key := cs.l1Prefix + id
	if ds.l1 != nil {
		if _, ok := ds.l1.Get(l1Key); ok {
			return true, nil
		}
	}
	if ds.l2 != nil {
		ok, err := ds.l2.ExistsP(ctx, cs.l2Prefix, id)
		if err == nil && ok {
			return true, nil
		}
	}
	if ds.l3 != nil {
		return ds.l3.Exists(ctx, cs.tableName, cs.pkColumn.Name, id)
	}
	return false, nil
}

// Count returns the number of rows matching q (nil q = all rows).
func (ds *DataStore) Count(ctx context.Context, schemaName string, q *Query) (int64, error) {
	if ds.closed.Load() {
		return 0, ErrUnavailable
	}
	cs, err := ds.registry.get(schemaName)
	if err != nil {
		return 0, err
	}
	if ds.l3 == nil {
		return 0, ErrL3Unavailable
	}
	where := ""
	var args []any
	if q != nil {
		where = q.Where
		args = q.Args
	}
	return ds.l3.Count(ctx, cs.tableName, where, args)
}

// ────────────────────────────────────────────────────────────────────────────
// Cache invalidation
// ────────────────────────────────────────────────────────────────────────────

// Invalidate removes a key from all cache tiers and publishes an invalidation event.
func (ds *DataStore) Invalidate(ctx context.Context, schemaName, id string) error {
	if ds.closed.Load() {
		return ErrUnavailable
	}
	l1Key := schemaName + ":" + id
	if ds.l1 != nil {
		ds.l1.Delete(l1Key)
	}
	if ds.l2 != nil {
		_ = ds.l2.Delete(ctx, schemaName, "", id)
	}
	if ds.sync != nil {
		ds.sync.publishInvalidation(ctx, schemaName, id, "delete")
	}
	return nil
}

// InvalidateAll flushes all cached entries for schemaName across L1 and L2.
func (ds *DataStore) InvalidateAll(ctx context.Context, schemaName string) error {
	if ds.closed.Load() {
		return ErrUnavailable
	}
	cs, err := ds.registry.get(schemaName)
	if err != nil {
		return err
	}
	if ds.l1 != nil {
		ds.l1.FlushSchema(schemaName + ":")
	}
	if ds.l2 != nil {
		_ = ds.l2.InvalidateAll(ctx, cs.Name, "")
	}
	if ds.sync != nil {
		ds.sync.publishInvalidation(ctx, cs.Name, "", "invalidate_all")
	}
	return nil
}

// ────────────────────────────────────────────────────────────────────────────
// Transactions
// ────────────────────────────────────────────────────────────────────────────

// Tx is a lightweight transaction helper that queues L3 operations and
// updates caches on commit.
type Tx struct {
	ds  *DataStore
	ops []txOp
	ctx context.Context
}

type txOp struct {
	schema string
	id     string
	value  any
	del    bool
}

// Tx returns a new transaction bound to ctx.
func (ds *DataStore) Tx(ctx context.Context) *Tx {
	return &Tx{ds: ds, ctx: ctx}
}

// Set queues a set operation in the transaction.
func (tx *Tx) Set(schemaName, id string, value any) *Tx {
	tx.ops = append(tx.ops, txOp{schema: schemaName, id: id, value: value})
	return tx
}

// Delete queues a delete operation in the transaction.
func (tx *Tx) Delete(schemaName, id string) *Tx {
	tx.ops = append(tx.ops, txOp{schema: schemaName, id: id, del: true})
	return tx
}

// Commit executes all queued operations inside a single L3 transaction.
func (tx *Tx) Commit() error {
	if tx.ds.l3 == nil {
		return ErrL3Unavailable
	}
	pgxTx, err := tx.ds.l3.BeginTx(tx.ctx)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrTxFailed, err)
	}
	for _, op := range tx.ops {
		cs, err := tx.ds.registry.get(op.schema)
		if err != nil {
			_ = pgxTx.Rollback(tx.ctx)
			return err
		}
		if op.del {
			if _, err := pgxTx.Exec(tx.ctx,
				fmt.Sprintf("DELETE FROM %s WHERE %s = $1", cs.tableName, cs.pkColumn.Name),
				op.id); err != nil {
				_ = pgxTx.Rollback(tx.ctx)
				return fmt.Errorf("%w: %v", ErrTxFailed, err)
			}
		} else {
			if err := tx.ds.writeToL3(tx.ctx, cs, op.value); err != nil {
				_ = pgxTx.Rollback(tx.ctx)
				return fmt.Errorf("%w: %v", ErrTxFailed, err)
			}
		}
	}
	if err := pgxTx.Commit(tx.ctx); err != nil {
		return fmt.Errorf("%w: %v", ErrTxFailed, err)
	}
	// Post-commit: update caches
	for _, op := range tx.ops {
		cs, _ := tx.ds.registry.get(op.schema)
		if op.del {
			_ = tx.ds.routerDelete(tx.ctx, cs, op.id)
		} else {
			_ = tx.ds.routerSet(tx.ctx, cs, op.id, op.value)
		}
	}
	return nil
}

// ────────────────────────────────────────────────────────────────────────────
// WarmCache
// ────────────────────────────────────────────────────────────────────────────

// WarmCache pre-loads up to limit records from L3 into L1 and L2.
// If limit <= 0, all rows are loaded.
func (ds *DataStore) WarmCache(ctx context.Context, schemaName string, limit int) error {
	if ds.closed.Load() {
		return ErrUnavailable
	}
	cs, err := ds.registry.get(schemaName)
	if err != nil {
		return err
	}
	if ds.l3 == nil {
		return ErrL3Unavailable
	}
	q := &Query{}
	if limit > 0 {
		q.Limit = limit
	}
	cols := colNames(cs)
	sql, args := q.ToSQL(cs.tableName, cols, 10000)
	rows, err := ds.l3.Query(ctx, sql, args)
	if err != nil {
		return err
	}
	defer rows.Close()

	modelType := cs.modelType
	for rows.Next() {
		elem := reflect.New(modelType).Elem()
		dests := buildScanDest(elem, cs)
		if err := rows.Scan(dests...); err != nil {
			return err
		}
		id := fmt.Sprintf("%v", elem.Field(cs.pkIndex).Interface())
		v := elem.Addr().Interface()
		l1Key := cs.l1Prefix + id
		if ds.l1 != nil {
			ds.setL1(cs, l1Key, v)
		}
		if ds.l2 != nil {
			_ = ds.setL2(ctx, cs, id, v)
		}
	}
	return rows.Err()
}

// ────────────────────────────────────────────────────────────────────────────
// Stats / Close
// ────────────────────────────────────────────────────────────────────────────

// Stats returns a snapshot of operational metrics.
func (ds *DataStore) Stats() Stats {
	s := Stats{
		Gets:    ds.stats.Gets.Load(),
		Sets:    ds.stats.Sets.Load(),
		Deletes: ds.stats.Deletes.Load(),
		Errors:  ds.stats.Errors.Load(),
	}
	if ds.sync != nil {
		s.DirtyCount = ds.sync.dirtyCount.Load()
	}
	if ds.l1 != nil {
		st := ds.l1.Stats()
		s.L1Entries = int64(st.Entries)
	}
	return s
}

// Close gracefully shuts down the DataStore.
func (ds *DataStore) Close() error {
	if !ds.closed.CompareAndSwap(false, true) {
		return nil
	}
	if ds.sync != nil {
		ds.sync.stop()
	}
	if ds.l1 != nil {
		ds.l1.Close()
	}
	if ds.l3 != nil {
		ds.l3.Close()
	}
	if ds.l4layer != nil {
		_ = ds.l4layer.Shutdown()
	}
	return nil
}
