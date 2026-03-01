# Strata — AI Agent Skill

## Purpose

This skill enables AI coding agents to use, extend, and test the
`github.com/AndrewDonelson/strata` package correctly and efficiently.
Strata is a **three-tier auto-caching data library for Go** that unifies
in-memory cache (L1), Redis (L2), and PostgreSQL (L3) behind a single API.

---

## Package identity

```
import "github.com/AndrewDonelson/strata"
```

Minimum requirements: Go 1.21+, PostgreSQL 14+, Redis 6+.
The package is `package strata`.
Internal sub-packages (`internal/l1`, `internal/l2`, `internal/l3`,
`internal/codec`, `internal/clock`, `internal/metrics`) are **not** part of
the public API—never import them in application code.

---

## Read tier behaviour

```
Get(ctx, schema, id, &dest)
  │
  ├─► L1 hit?  → return immediately           (~100 ns)
  ├─► L2 hit?  → populate L1 → return         (~500 µs)
  └─► L3 hit?  → populate L2, populate L1 → return  (~5 ms)
               └─► not found → ErrNotFound
```

Writes flow in the **opposite** direction determined by `WriteMode`.

---

## Lifecycle — exactly four steps

```go
// 1. Create
ds, err := strata.NewDataStore(strata.Config{
    PostgresDSN: "postgres://user:pass@host:5432/mydb",
    RedisAddr:   "localhost:6379",
})
if err != nil { /* handle */ }
defer ds.Close() // ← always defer Close; it flushes write-behind and stops workers

// 2. Register (once per schema, at startup, before any data operation)
err = ds.Register(strata.Schema{Name: "players", Model: &Player{}})

// 3. Migrate (idempotent — safe to call on every startup)
err = ds.Migrate(ctx)

// 4. Use
_ = ds.Set(ctx, "players", "p1", &Player{...})
```

---

## Struct definition rules

- Every model **must** have exactly one field tagged `strata:"primary_key"`.
  It must be of type `string`. The field name is conventionally `ID`.
- The model **must** be a concrete struct — not a map, interface, or slice.
- Pass a **pointer** to the struct as `Schema.Model` and as `dest` in `Get`.
- The `Name` field in `Schema` defaults to the snake_case struct name if omitted.

```go
type Player struct {
    ID        string    `strata:"primary_key"`
    Username  string    `strata:"unique,index"`
    Email     string    `strata:"index,nullable"`
    Level     int       `strata:"default:1"`
    Score     int64
    APIKey    string    `strata:"encrypted"`    // AES-256-GCM, string fields only
    Password  string    `strata:"omit_cache"`   // Postgres only; never in L1/L2
    Notes     string    `strata:"nullable"`
    CreatedAt time.Time `strata:"auto_now_add"` // set once on first write
    UpdatedAt time.Time `strata:"auto_now"`     // updated on every write
    Internal  string    `strata:"-"`            // completely ignored
}
```

### Full tag reference

| Tag | Effect |
|-----|--------|
| `primary_key` | Required. Identity field for Get/Set routing. |
| `unique` | UNIQUE constraint in Postgres. |
| `index` | Non-unique Postgres index. |
| `nullable` | Column allows NULL (default is NOT NULL). |
| `omit_cache` | Excluded from L1 **and** L2; Postgres only. |
| `omit_l1` | Excluded from L1 only; still in L2. |
| `default:X` | SQL `DEFAULT X` in the DDL. |
| `auto_now_add` | Set to `time.Now()` on first insert, never updated. |
| `auto_now` | Set to `time.Now()` on every write. |
| `encrypted` | AES-256-GCM encrypted at rest. Requires `EncryptionKey` in Config. String fields only. Stored plaintext in L1/L2 for read speed. |
| `-` | Completely ignored — not persisted, not cached. |

Multiple tags are **comma-separated** with no spaces: `strata:"unique,index,nullable"`.

### Go → Postgres type mapping

| Go | PostgreSQL |
|----|-----------|
| `string` | `TEXT` |
| `int`, `int32`, `int64` | `BIGINT` |
| `float32`, `float64` | `DOUBLE PRECISION` |
| `bool` | `BOOLEAN` |
| `time.Time` | `TIMESTAMPTZ` |
| `[]byte` | `BYTEA` |
| struct / map / slice | `JSONB` |

---

## `Schema` struct — all fields

```go
type Schema struct {
    Name      string         // table/collection name (defaults to snake_case struct name)
    Model     any            // *StructType — pointer to zero value of model
    L1        MemPolicy      // in-memory cache settings
    L2        RedisPolicy    // Redis cache settings
    L3        PostgresPolicy // Postgres persistence settings
    WriteMode WriteMode      // WriteThrough | WriteBehind | WriteThroughL1Async
    Indexes   []Index        // extra database indexes
    Hooks     SchemaHooks    // lifecycle callbacks
}

type MemPolicy struct {
    TTL        time.Duration  // 0 = use Config.DefaultL1TTL
    MaxEntries int            // 0 = use Config.L1Pool.MaxEntries; PER SHARD (256 shards)
    Eviction   EvictionPolicy // EvictLRU (default) | EvictLFU | EvictFIFO
}

type RedisPolicy struct {
    TTL       time.Duration // 0 = use Config.DefaultL2TTL
    KeyPrefix string        // optional; defaults to schema name
}

type PostgresPolicy struct {
    TableName   string // optional; defaults to schema name
    ReadReplica string // optional DSN for a read-only replica
    PartitionBy string // optional column for table partitioning
}

type Index struct {
    Fields []string // column names
    Unique bool
    Name   string   // optional; auto-generated if empty
}
```

---

## `Config` struct — all fields with defaults

```go
type Config struct {
    // Connections (no defaults — set via environment variables)
    PostgresDSN   string
    RedisAddr     string
    RedisPassword string
    RedisDB       int

    // Pool sizes — defaults applied by NewDataStore if zero
    L1Pool L1PoolConfig{ MaxEntries: 100_000, Eviction: EvictLRU }
    L2Pool L2PoolConfig{ /* zero values use go-redis defaults */ }
    L3Pool L3PoolConfig{ MaxConns: 20, MinConns: 2,
                         MaxConnLifetime: 30*time.Minute,
                         MaxConnIdleTime: 10*time.Minute }

    // TTL defaults (per-schema TTLs override these)
    DefaultL1TTL time.Duration // default: 5m
    DefaultL2TTL time.Duration // default: 30m

    // Write behaviour
    DefaultWriteMode          WriteMode     // default: WriteThrough
    WriteBehindFlushInterval  time.Duration // default: 500ms
    WriteBehindFlushThreshold int           // default: 100
    WriteBehindMaxRetry       int           // default: 5

    // Multi-node invalidation
    InvalidationChannel string // default: "strata:invalidate"

    // Pluggable components (nil → sensible no-op defaults applied)
    Codec   codec.Codec            // default: MsgPack (NOT JSON — see note below)
    Clock   clock.Clock            // default: real wall clock
    Metrics metrics.MetricsRecorder // default: no-op
    Logger  Logger                  // default: no-op

    // Encryption — nil = disabled; must be exactly 32 bytes when set
    EncryptionKey []byte
}
```

> **IMPORTANT:** The default `Codec` is **MsgPack**, not JSON.
> Values stored in Redis are MsgPack-encoded by default.
> If you need human-readable Redis keys for debugging, set `Codec: codec.JSON{}`.

---

## Full public API

### DataStore construction

```go
ds, err := strata.NewDataStore(cfg Config) (*DataStore, error)
ds.Close() error  // idempotent; always defer this
ds.Stats() Stats  // snapshot of Gets/Sets/Deletes/Errors/L1Entries/DirtyCount
strata.Version() string  // "YYYY.MM.DD-HHMM-env"
```

### Schema registration

```go
ds.Register(s Schema) error
// Returns ErrSchemaDuplicate if called twice with the same Name.
// Must be called before any data operation that references the schema.
```

### CRUD

```go
// Read
ds.Get(ctx, schemaName, id string, dest any) error
// Generic helper — preferred when T is known at compile time
p, err := strata.GetTyped[Player](ctx, ds, "players", "p1")

// Write
ds.Set(ctx, schemaName, id string, value any) error
ds.SetMany(ctx, schemaName string, pairs map[string]any) error  // id → *Model

// Delete (removes from all tiers)
ds.Delete(ctx, schemaName, id string) error
```

### Search

```go
// Standard — queries L3, populates L1/L2 as side-effect
ds.Search(ctx, schemaName string, q *Query, destSlice any) error
// q == nil means "all rows" up to the default limit (100)
// q must be a pointer to a Query value, e.g.: q := ...; ds.Search(ctx, name, &q, &results)

// Generic helper
q := strata.Q().Where("level > $1", 10).Build()
players, err := strata.SearchTyped[Player](ctx, ds, "players", &q)

// Cached — caches full result set in L2 by SQL fingerprint
ds.SearchCached(ctx, schemaName string, q *Query, destSlice any) error
```

### Exists & Count

```go
ok, err := ds.Exists(ctx, schemaName, id string) (bool, error)
// Checks L1 → L2 → L3 in order; returns true on first hit.

n, err := ds.Count(ctx, schemaName string, q *Query) (int64, error)
// Always hits L3. q == nil counts all rows.
```

### Cache control

```go
ds.Invalidate(ctx, schemaName, id string) error      // remove one key from L1+L2
ds.InvalidateAll(ctx, schemaName string) error        // flush all keys for a schema
ds.WarmCache(ctx, schemaName string, limit int) error // pre-load L3 → L1+L2 (0 = all rows)
ds.FlushDirty(ctx) error                              // force write-behind flush now
```

### Migrations

```go
ds.Migrate(ctx) error                     // apply DDL for all registered schemas
ds.MigrateFrom(ctx, dir string) error     // apply NNN_description.sql files from dir
records, err := ds.MigrationStatus(ctx)   // []MigrationRecord — inspect applied state
```

Migrations are **additive only** — Strata never drops or renames columns.
Destructive changes must use manual SQL files with `MigrateFrom`.

### Transactions

```go
err := ds.Tx(ctx).
    Set("players", "p1", &Player{...}).
    Set("scores",  "p1", &Score{...}).
    Delete("sessions", "old-sid").
    Commit()
// L3 is committed atomically; L1/L2 are updated only after a successful commit.
// Returns ErrTxFailed (wrapping the Postgres error) on rollback.
```

---

## Query builder

```go
q := strata.Q().
    Where("level > $1 AND region = $2", 10, "eu-west").
    OrderBy("score").Desc().
    Limit(25).Offset(50).
    Fields("id", "username", "score").  // column projection
    ForceL3().                           // bypass L1+L2 entirely
    Build()                              // returns Query (value type)

// Pass as pointer to Search/SearchTyped/Count:
ds.Search(ctx, "players", &q, &results)

// Inline (Build returns a value; take its address):
q2 := strata.Q().Where("level > $1", 10).Limit(50).Build()
ds.Search(ctx, "players", &q2, &results)

// Do NOT share a *Query across goroutines — Query is a value type; copy it first.
```

Passing `nil` as `*Query` is always valid and means "no filter, default limit".

---

## Error handling — always use `errors.Is`

```go
err := ds.Get(ctx, "players", id, &p)
switch {
case errors.Is(err, strata.ErrNotFound):
    // record does not exist in any tier
case errors.Is(err, strata.ErrSchemaNotFound):
    // Register was not called for this schema name
case errors.Is(err, strata.ErrL3Unavailable):
    // Postgres is down
case err != nil:
    // unexpected error
}
```

### Full error catalogue

```go
// Schema registration
strata.ErrSchemaNotFound    // schema name not registered
strata.ErrSchemaDuplicate   // Register called twice for same name
strata.ErrNoPrimaryKey      // struct has no primary_key tag
strata.ErrInvalidModel      // nil or non-pointer model
strata.ErrMissingPrimaryKey // struct passed to Set has zero/empty PK field

// Data
strata.ErrNotFound     // record absent from all tiers
strata.ErrDecodeFailed // codec or decryption error on read
strata.ErrEncodeFailed // codec or encryption error on write

// Infrastructure
strata.ErrL1Unavailable // L1 not initialised
strata.ErrL2Unavailable // Redis unavailable
strata.ErrL3Unavailable // Postgres unavailable
strata.ErrUnavailable   // called after Close()

// Transactions
strata.ErrTxFailed  // Postgres rolled back — original error is wrapped
strata.ErrTxTimeout // transaction deadline exceeded

// Configuration
strata.ErrInvalidConfig // missing required fields

// Hooks
strata.ErrHookPanic // BeforeSet/BeforeGet hook panicked (recovered)

// Write-behind
strata.ErrWriteBehindMaxRetry // dirty entry exhausted retry budget
```

---

## Write modes

| Mode | When to use |
|------|-------------|
| `WriteThrough` (default) | Normal CRUD where durability matters. L3 written before returning. |
| `WriteThroughL1Async` | L3+L2 written synchronously; L1 populated lazily on next read. Good when L1 hit rate is low. |
| `WriteBehind` | High-frequency writes (scores, counters). L3 written asynchronously in batches. Risk: data loss if process crashes before flush. |

Set per-schema or globally:

```go
strata.Schema{
    Name:      "leaderboard",
    Model:     &Score{},
    WriteMode: strata.WriteBehind,
}

// or globally:
strata.Config{DefaultWriteMode: strata.WriteBehind}
```

---

## Encryption

```go
key := make([]byte, 32)
if _, err := rand.Read(key); err != nil { ... }

ds, _ := strata.NewDataStore(strata.Config{
    EncryptionKey: key, // enables AES-256-GCM for all fields tagged `encrypted`
})
```

- Only `string` fields support `encrypted`.
- Plaintext is stored in L1/L2 (fast path); ciphertext only in Postgres.
- The nonce is randomly generated per write and prepended to the ciphertext.
- If `EncryptionKey` is not set, `encrypted` tags are silently ignored (no error).

---

## Lifecycle hooks

```go
strata.Schema{
    Name:  "users",
    Model: &User{},
    Hooks: strata.SchemaHooks{
        BeforeSet: func(ctx context.Context, v any) error {
            u := v.(*User)
            if u.Email == "" {
                return errors.New("email required")
            }
            return nil
        },
        AfterSet: func(ctx context.Context, v any) {
            // fire domain event, update search index, etc.
        },
        OnWriteError: func(ctx context.Context, key string, err error) {
            // write-behind entry exhausted retries — alert here
        },
    },
}
```

`BeforeSet` returning a non-nil error **aborts the write** — no tiers are touched.
`AfterSet` fires only after a fully successful write.

---

## Observability

### Logger

```go
type Logger interface {
    Info(msg string, keysAndValues ...any)
    Warn(msg string, keysAndValues ...any)
    Error(msg string, keysAndValues ...any)
    Debug(msg string, keysAndValues ...any)
}
```

Wrapping `log/slog`:

```go
type SlogAdapter struct{ L *slog.Logger }
func (a SlogAdapter) Info(m string, kv ...any)  { a.L.Info(m, kv...) }
func (a SlogAdapter) Warn(m string, kv ...any)  { a.L.Warn(m, kv...) }
func (a SlogAdapter) Error(m string, kv ...any) { a.L.Error(m, kv...) }
func (a SlogAdapter) Debug(m string, kv ...any) { a.L.Debug(m, kv...) }
```

### Stats snapshot

```go
s := ds.Stats()
// s.Gets, s.Sets, s.Deletes, s.Errors int64
// s.L1Entries  int64 — current in-memory entry count
// s.DirtyCount int64 — write-behind entries pending flush
```

---

## Testing without a database

All unit tests use white-box injection — set `ds.l3` (unexported) to a mock
that implements the private `l3Backend` interface, or simply create a
`DataStore` with an empty `Config{}` (no DSN = no Postgres, no Redis).

### Pattern 1 — no external services

```go
func TestMyFeature(t *testing.T) {
    ds, err := strata.NewDataStore(strata.Config{}) // L1 only, no Postgres, no Redis
    require.NoError(t, err)
    defer ds.Close()

    require.NoError(t, ds.Register(strata.Schema{
        Name:  "items",
        Model: &MyModel{},
    }))
    // L3 operations will return ErrL3Unavailable — expected in unit tests.
}
```

### Pattern 2 — mock Postgres (white-box, same package)

```go
// In package strata (white-box test file):
type errL3 struct{ err error }
func (e *errL3) Upsert(_ context.Context, _ string, _ []string, _ []any, _ string) error { return e.err }
func (e *errL3) DeleteByID(_ context.Context, _, _ string, _ any) error { return e.err }
func (e *errL3) Query(_ context.Context, _ string, _ []any) (pgx.Rows, error) { return nil, e.err }
func (e *errL3) QueryRow(_ context.Context, _ string, _ []any) pgx.Row { return &errRow{e.err} }
func (e *errL3) Exec(_ context.Context, _ string, _ []any) error { return e.err }
func (e *errL3) Exists(_ context.Context, _, _ string, _ any) (bool, error) { return false, e.err }
func (e *errL3) Count(_ context.Context, _, _ string, _ []any) (int64, error) { return 0, e.err }
func (e *errL3) BeginTx(_ context.Context) (pgx.Tx, error) { return nil, e.err }
func (e *errL3) Close() {}

ds.l3 = &errL3{err: strata.ErrL3Unavailable}
```

### Pattern 3 — integration tests with Testcontainers

```go
//go:build integration
package strata_test

import (
    "testing"
    "github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestIntegration(t *testing.T) {
    pgc, _ := postgres.Run(ctx, "postgres:15")
    defer pgc.Terminate(ctx)
    dsn, _ := pgc.ConnectionString(ctx, "sslmode=disable")
    ds, _ := strata.NewDataStore(strata.Config{PostgresDSN: dsn})
    defer ds.Close()
    ...
}
```

---

## Common patterns

### Service initialization

```go
func NewPlayerService(cfg strata.Config) (*PlayerService, error) {
    ds, err := strata.NewDataStore(cfg)
    if err != nil {
        return nil, fmt.Errorf("strata init: %w", err)
    }
    if err := ds.Register(strata.Schema{
        Name:  "players",
        Model: &Player{},
        L1:    strata.MemPolicy{TTL: time.Minute, MaxEntries: 10_000},
        L2:    strata.RedisPolicy{TTL: 30 * time.Minute},
    }); err != nil {
        return nil, err
    }
    if err := ds.Migrate(context.Background()); err != nil {
        return nil, err
    }
    return &PlayerService{ds: ds}, nil
}
```

### Paged search

```go
func (s *PlayerService) ListTopPlayers(ctx context.Context, page, size int) ([]Player, error) {
    q := strata.Q().
        OrderBy("score").Desc().
        Limit(size).Offset(page*size).
        Build()
    return strata.SearchTyped[Player](ctx, s.ds, "players", &q)
}
```

### Upsert pattern

```go
// ds.Set always upserts (INSERT … ON CONFLICT DO UPDATE in Postgres).
// If the record may or may not already exist, just call Set.
err := ds.Set(ctx, "players", player.ID, player)
```

### Conditional write (check-then-set)

```go
// Strata has no native CAS. Use Exists + Set.
// WARNING: not atomic — suitable only for low-contention scenarios.
exists, err := ds.Exists(ctx, "players", id)
if err != nil { return err }
if !exists {
    return ds.Set(ctx, "players", id, &newPlayer)
}
return strata.ErrSchemaDuplicate // or a domain error
```

### Bulk load at startup

```go
if err := ds.WarmCache(ctx, "config", 0); err != nil { // 0 = all rows
    log.Warn("cache warm failed — first reads will hit Postgres", "err", err)
}
```

### Atomic multi-schema write

```go
err := ds.Tx(ctx).
    Set("orders",   order.ID,   order).
    Set("inventory", item.ID,   item).
    Delete("carts", cartID).
    Commit()
if errors.Is(err, strata.ErrTxFailed) {
    // all operations rolled back; safe to retry
}
```

---

## Anti-patterns and mistakes to avoid

| Wrong | Right |
|-------|-------|
| `Register` after `Set`/`Get` | Always `Register` before data operations |
| Ignoring `Close()` | Always `defer ds.Close()` after a successful `NewDataStore` |
| Calling `Migrate` before `Register` | `Migrate` acts on registered schemas — register first |
| Sharing a `*Query` across goroutines | `Query` is a value type; copy or build a new one per goroutine |
| Hardcoding `EncryptionKey` | Load from env / secrets manager  |
| Using `omit_cache` for frequently-read fields | Use `omit_cache` only for write-only / rarely-read fields like password hashes |
| `MaxEntries` set globally thinking it limits total memory | `MaxEntries` is **per-shard** (256 shards). For ~50 000 total limit, set `MaxEntries: 200`. |
| Passing `*T` where `T` is not the model type to `Get` | `dest` must be `*ModelType` matching the registered schema |
| Calling `MigrateFrom` with destructive SQL | Strata does not prevent destructive DDL via `MigrateFrom` — review files carefully |
| Assuming `SearchCached` invalidates automatically | `SearchCached` caches by SQL fingerprint and expires by TTL only — invalidate manually with `Invalidate` or `InvalidateAll` if data changes frequently |
| Not handling `ErrTxFailed` as retriable | `ErrTxFailed` is almost always a serialization failure or conflict — safe to retry with backoff |

---

## Configuration checklist for production

```go
strata.Config{
    PostgresDSN: os.Getenv("POSTGRES_DSN"),
    RedisAddr:   os.Getenv("REDIS_ADDR"),

    L1Pool: strata.L1PoolConfig{
        MaxEntries: 200,          // per-shard; tune for model size vs available RAM
        Eviction:   strata.EvictLRU,
    },
    L3Pool: strata.L3PoolConfig{
        MaxConns: 30,             // match your Postgres max_connections budget
        MinConns: 5,
    },

    DefaultL1TTL: 2 * time.Minute,
    DefaultL2TTL: time.Hour,

    EncryptionKey: loadKeyFromVault(), // 32 bytes; nil disables encryption

    Logger:  mySlogAdapter,
    Metrics: myPrometheusRecorder,

    // Leave Codec as default (MsgPack) unless you need human-readable Redis values.
}
```

---

## Build tags

| Tag | Purpose |
|-----|---------|
| `dev` | Activates development helpers (build info, extra logging). Required for `make test` and many test files in this repo. |
| `integration` | Enables integration tests that spin up real Postgres + Redis via Testcontainers. Not run by default. |

```bash
go test -tags dev ./...         # unit tests
go test -tags integration ./... # integration tests (requires Docker)
make all                         # lint + vet + test + coverage report
```

The binary's version string is injected via `-ldflags`:

```bash
go build -tags dev \
  -ldflags "-X 'github.com/AndrewDonelson/strata.BuildDate=2026.03.01-1200' \
            -X 'github.com/AndrewDonelson/strata.BuildEnv=prod'" .
```

---

## Key internal constants (relevant for white-box tests)

| Identifier | Value | Meaning |
|-----------|-------|---------|
| `l1WriteBufSize` | `512` | Capacity of the L1 async-write channel |
| default L1 shards | `256` | Number of independent L1 shards |
| `defaultInvalidationChannel` | `"strata:invalidate"` | Redis pub/sub channel for cross-node invalidation |

---

*Skill maintained alongside the Strata source at `/home/andrew/Development/Golang/Strata/SKILL.md`.*
