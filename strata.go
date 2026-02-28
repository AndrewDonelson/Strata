package strata

import (
	"context"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/AndrewDonelson/strata/internal/clock"
	"github.com/AndrewDonelson/strata/internal/codec"
	"github.com/AndrewDonelson/strata/internal/l1"
	"github.com/AndrewDonelson/strata/internal/l2"
	"github.com/AndrewDonelson/strata/internal/l3"
	"github.com/AndrewDonelson/strata/internal/metrics"
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

	// Optional overrideable components
	Codec   codec.Codec
	Clock   clock.Clock
	Metrics metrics.MetricsRecorder
	Logger  Logger

	// Encryption key (must be 32 bytes for AES-256-GCM; nil = disabled).
	EncryptionKey []byte
}

func (c *Config) defaults() {
	if c.Codec == nil {
		c.Codec = codec.JSON{}
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
	l3        *l3.Store
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
		ds.l3 = l3.New(pool, nil)
	}

	// Sync engine
	ds.sync = newSyncEngine(ds)
	ds.sync.start()

	return ds, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Schema registration
// ────────────────────────────────────────────────────────────────────────────

// Register compiles and stores a Schema definition.
func (ds *DataStore) Register(s Schema) error {
	_, err := ds.registry.register(s)
	return err
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
	l1Key := fmt.Sprintf("%s:%s", schemaName, id)
	if ds.l1 != nil {
		if _, ok := ds.l1.Get(l1Key); ok {
			return true, nil
		}
	}
	if ds.l2 != nil {
		ok, err := ds.l2.Exists(ctx, cs.Name, "", id)
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
	l1Key := fmt.Sprintf("%s:%s", schemaName, id)
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
		l1Key := fmt.Sprintf("%s:%s", cs.Name, id)
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
		ds.l3.Pool().Close()
	}
	return nil
}
