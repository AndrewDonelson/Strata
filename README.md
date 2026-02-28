<p align="center">
  <img src="logo.png" alt="Strata" width="320" />
</p>

# Strata

**Three-tier auto-caching data library for Go — L1 (memory) → L2 (Redis) → L3 (PostgreSQL) behind a single API.**

[![Go Reference](https://pkg.go.dev/badge/github.com/AndrewDonelson/strata.svg)](https://pkg.go.dev/github.com/AndrewDonelson/strata)
[![Go Version](https://img.shields.io/badge/go-1.21%2B-blue)](https://go.dev/dl/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## Overview

Strata removes the boilerplate of cache management from Go services. You define a schema once and call `Get`, `Set`, `Delete`, or `Search`. Strata automatically routes reads through L1 → L2 → L3, propagates writes, evicts stale entries, and keeps a cluster of server instances consistent via Redis pub/sub invalidation.

```
Get(ctx, "players", id, &dest)
  │
  ├─► L1 hit?  → return immediately   (~100 ns)
  ├─► L2 hit?  → populate L1 → return (~500 μs)
  └─► L3 hit?  → populate L2+L1 → return (~5 ms)
               └─► not found → ErrNotFound
```

## Table of Contents

1. [Installation](#installation)
2. [Quick Start](#quick-start)
3. [Schema Definition](#schema-definition)
   - [Struct Tags](#struct-tags)
   - [Cache Policies](#cache-policies)
   - [Indexes](#indexes)
   - [Lifecycle Hooks](#lifecycle-hooks)
4. [Core Operations](#core-operations)
   - [Get](#get)
   - [Set](#set)
   - [SetMany](#setmany)
   - [Delete](#delete)
   - [Search](#search)
   - [SearchCached](#searchcached)
   - [Exists & Count](#exists--count)
5. [Query Builder](#query-builder)
6. [Transactions](#transactions)
7. [Cache Control](#cache-control)
   - [Invalidate](#invalidate)
   - [WarmCache](#warmcache)
   - [FlushDirty](#flushdirty)
8. [Write Modes](#write-modes)
9. [Schema Migration](#schema-migration)
10. [Encryption](#encryption)
11. [Observability](#observability)
12. [Configuration Reference](#configuration-reference)
13. [Error Reference](#error-reference)
14. [Architecture Notes](#architecture-notes)
15. [Contributing](#contributing)

---

## Installation

```bash
go get github.com/AndrewDonelson/strata
```

**Requirements:** Go 1.21+, PostgreSQL 14+, Redis 6+

---

## Quick Start

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/AndrewDonelson/strata"
)

type Player struct {
    ID        string    `strata:"primary_key"`
    Username  string    `strata:"unique,index"`
    Email     string    `strata:"index,nullable"`
    Level     int       `strata:"default:1"`
    CreatedAt time.Time `strata:"auto_now_add"`
    UpdatedAt time.Time `strata:"auto_now"`
}

func main() {
    ctx := context.Background()

    // 1. Create the data store
    ds, err := strata.NewDataStore(strata.Config{
        PostgresDSN: "postgres://user:pass@localhost:5432/mydb",
        RedisAddr:   "localhost:6379",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer ds.Close()

    // 2. Register schemas (once, at startup)
    err = ds.Register(strata.Schema{
        Name:  "players",
        Model: &Player{},
        L1:    strata.MemPolicy{TTL: 60 * time.Second, MaxEntries: 50_000},
        L2:    strata.RedisPolicy{TTL: 30 * time.Minute},
        L3:    strata.PostgresPolicy{},
    })
    if err != nil {
        log.Fatal(err)
    }

    // 3. Run migrations
    if err := ds.Migrate(ctx); err != nil {
        log.Fatal(err)
    }

    // 4. Use it
    player := &Player{ID: "p1", Username: "andrew", Level: 1}
    if err := ds.Set(ctx, "players", "p1", player); err != nil {
        log.Fatal(err)
    }

    // Typed retrieval — no type assertion needed
    p, err := strata.GetTyped[Player](ctx, ds, "players", "p1")
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("player: %+v", p)
}
```

---

## Schema Definition

A `Schema` binds a Go struct to three cache tiers and optionally to a PostgreSQL table.

```go
type Schema struct {
    Name      string         // collection/table name; derived from struct name if empty
    Model     any            // pointer to a struct
    L1        MemPolicy      // in-memory cache settings
    L2        RedisPolicy    // Redis cache settings
    L3        PostgresPolicy // Postgres persistence settings
    WriteMode WriteMode      // WriteThrough (default), WriteBehind, WriteThroughL1Async
    Indexes   []Index        // additional database indexes
    Hooks     SchemaHooks    // lifecycle callbacks
}
```

Register schemas at application startup before any data operations:

```go
err := ds.Register(strata.Schema{
    Name:  "sessions",
    Model: &Session{},
    L1:    strata.MemPolicy{TTL: 5 * time.Minute, MaxEntries: 100_000},
    L2:    strata.RedisPolicy{TTL: 4 * time.Hour},
    // No L3 — sessions are ephemeral, Redis is source of truth
})
```

### Struct Tags

Control column behaviour in PostgreSQL and caching behaviour with `strata` struct tags.

| Tag | Effect |
|-----|--------|
| `primary_key` | marks the identity field used in Get/Set routing (required) |
| `unique` | adds UNIQUE constraint in Postgres |
| `index` | creates a non-unique database index |
| `nullable` | column is NULL-able (default: NOT NULL) |
| `omit_cache` | field excluded from L1 **and** L2 — stored in Postgres only |
| `omit_l1` | field excluded from L1 only; still cached in L2 |
| `default:X` | generates `DEFAULT X` in the DDL |
| `auto_now_add` | set to `time.Now()` on first insert, never updated |
| `auto_now` | set to `time.Now()` on every write |
| `encrypted` | AES-256-GCM encrypted at rest (requires `EncryptionKey` in `Config`) |
| `-` | field is ignored entirely |

```go
type User struct {
    ID           string    `strata:"primary_key"`
    Email        string    `strata:"unique,index"`
    PasswordHash string    `strata:"omit_cache"`       // only stored in Postgres
    APIKey       string    `strata:"encrypted"`        // encrypted at rest
    Role         string    `strata:"default:viewer"`
    Notes        string    `strata:"nullable"`
    CreatedAt    time.Time `strata:"auto_now_add"`
    UpdatedAt    time.Time `strata:"auto_now"`
    Internal     string    `strata:"-"`                // not persisted at all
}
```

**Supported Go → Postgres type mappings:**

| Go type | PostgreSQL type |
|---------|-----------------|
| `string` | `TEXT` |
| `int`, `int32`, `int64` | `BIGINT` |
| `float32`, `float64` | `DOUBLE PRECISION` |
| `bool` | `BOOLEAN` |
| `time.Time` | `TIMESTAMPTZ` |
| `[]byte` | `BYTEA` |
| struct / map / slice | `JSONB` |

### Cache Policies

```go
type MemPolicy struct {
    TTL        time.Duration  // 0 = never expire
    MaxEntries int            // 0 = unlimited (per shard — 256 shards total)
    Eviction   EvictionPolicy // EvictLRU (default), EvictLFU, EvictFIFO
}

type RedisPolicy struct {
    TTL       time.Duration  // 0 = never expire
    KeyPrefix string         // optional; defaults to schema name
}

type PostgresPolicy struct {
    TableName   string // optional; defaults to schema name
    ReadReplica string // optional DSN for a read-only replica
    PartitionBy string // optional column for table partitioning
}
```

### Indexes

Extra database indexes are declared alongside the schema:

```go
strata.Schema{
    Name:  "events",
    Model: &Event{},
    Indexes: []strata.Index{
        {Fields: []string{"user_id"}, Name: "idx_events_user"},
        {Fields: []string{"user_id", "created_at"}, Unique: false},
        {Fields: []string{"trace_id"}, Unique: true},
    },
}
```

### Lifecycle Hooks

```go
type SchemaHooks struct {
    BeforeSet    func(ctx context.Context, value any) error
    AfterSet     func(ctx context.Context, value any)
    BeforeGet    func(ctx context.Context, id string)
    AfterGet     func(ctx context.Context, value any)
    OnEvict      func(ctx context.Context, key string, value any)
    OnWriteError func(ctx context.Context, key string, err error)
}
```

- `BeforeSet` — validate or mutate the value before any write; return a non-nil error to abort.
- `AfterSet` — post-write notification (e.g. emit a domain event).
- `BeforeGet` — log or trace the read.
- `AfterGet` — populate computed fields.
- `OnEvict` — called when L1 evicts an entry.
- `OnWriteError` — called when a write-behind write exhausts its retries.

---

## Core Operations

### Get

Reads a record by primary key. Cascade: L1 → L2 → L3. Each cache miss populates the tiers above it.

```go
// Generic form — preferred
p, err := strata.GetTyped[Player](ctx, ds, "players", "p123")
if errors.Is(err, strata.ErrNotFound) {
    // record does not exist
}

// Non-generic form with destination pointer
var p Player
err := ds.Get(ctx, "players", "p123", &p)
```

### Set

Writes a record. The tier order depends on the schema's `WriteMode` (see [Write Modes](#write-modes)).

```go
player := &Player{ID: "p1", Username: "andrew", Level: 5}
err := ds.Set(ctx, "players", "p1", player)
```

### SetMany

Writes multiple records in one call using a `map[string]any` of `id → value`:

```go
err := ds.SetMany(ctx, "players", map[string]any{
    "p1": &Player{ID: "p1", Username: "a"},
    "p2": &Player{ID: "p2", Username: "b"},
})
```

Internally `SetMany` uses a Redis pipeline for L2 and PostgreSQL `COPY` for L3.

### Delete

Removes a record from all three tiers:

```go
err := ds.Delete(ctx, "players", "p1")
```

### Search

Queries PostgreSQL (L3) by default. Each matching record is individually populated into L1 and L2 as a side-effect.

```go
// Non-generic form
var results []Player
err := ds.Search(ctx, "players", strata.Q().Where("level > $1", 10).Limit(50).Build().Ptr(), &results)

// Generic form
players, err := strata.SearchTyped[Player](ctx, ds, "players",
    strata.Q().Where("level > $1", 10).OrderBy("created_at").Desc().Limit(50).Build().Ptr())
```

### SearchCached

Caches the entire result set under a composite key derived from the query. Subsequent calls with the same `*Query` return the cached slice until the L2 TTL expires.

```go
var leaderboard []Player
err := ds.SearchCached(ctx, "players",
    strata.Q().OrderBy("level").Desc().Limit(100).Build().Ptr(),
    &leaderboard)
```

### Exists & Count

```go
ok, err := ds.Exists(ctx, "players", "p1")

n, err := ds.Count(ctx, "players", strata.Q().Where("level >= $1", 50).Build().Ptr())
```

`Count` always hits L3.

---

## Query Builder

The fluent `Q()` builder constructs `Query` values without struct literals:

```go
q := strata.Q().
    Where("region = $1 AND level > $2", "eu-west", 10).
    OrderBy("score").
    Desc().
    Limit(25).
    Offset(50).
    Fields("id", "username", "score").
    Build()

// Force tiers
strata.Q().Where("id = $1", id).ForceL3().Build() // bypass all caches
strata.Q().Where("id = $1", id).ForceL2().Build() // skip L1 only
```

`Query` fields at a glance:

| Field | Type | Description |
|-------|------|-------------|
| `Where` | `string` | Parameterised SQL WHERE clause |
| `Args` | `[]any` | Positional arguments for WHERE (`$1`, `$2`, …) |
| `OrderBy` | `string` | Column name to sort by |
| `Desc` | `bool` | Descending sort |
| `Limit` | `int` | Max rows (0 = use default: 100) |
| `Offset` | `int` | Rows to skip |
| `Fields` | `[]string` | Column projection (empty = all) |
| `ForceL3` | `bool` | Skip L1 and L2 entirely |
| `ForceL2` | `bool` | Skip L1 only |

---

## Transactions

`Tx` queues Set and Delete operations and commits them atomically to L3. Caches (L1 + L2) are updated only after a successful commit.

```go
err := ds.Tx(ctx).
    Set("players", "p1", &Player{ID: "p1", Level: 99}).
    Set("scores", "p1", &Score{PlayerID: "p1", Points: 9999}).
    Delete("sessions", "old-session-id").
    Commit()
if errors.Is(err, strata.ErrTxFailed) {
    // all operations were rolled back
}
```

---

## Cache Control

### Invalidate

Removes a single key from L1 and L2 without touching L3. The next `Get` will re-fetch from Postgres and repopulate the caches.

```go
err := ds.Invalidate(ctx, "players", "p1")
```

### WarmCache

Pre-loads up to `limit` records from L3 into L1 and L2. Use at startup to avoid cold-cache spikes.

```go
// load first 10,000 active players
err := ds.WarmCache(ctx, "players", 10_000)
// 0 = load all rows
err = ds.WarmCache(ctx, "config", 0)
```

### FlushDirty

Forces all pending write-behind entries to be written to L3 immediately. Called automatically by `Close()`.

```go
err := ds.FlushDirty(ctx)
```

---

## Write Modes

Set per-schema or globally via `Config.DefaultWriteMode`.

| Mode | L3 | L2 | L1 | Latency | Durability |
|------|----|----|----|---------|-----------| 
| `WriteThrough` (default) | sync | sync | sync | highest | maximum — L3 is written before returning |
| `WriteThroughL1Async` | sync | sync | lazy | medium | L3 + L2 durable; L1 populated on next read |
| `WriteBehind` | async | async | immediate | lowest | L1 written first; L3 durable within flush interval |

```go
strata.Schema{
    Name:      "leaderboard",
    Model:     &Score{},
    WriteMode: strata.WriteBehind,          // high-frequency score updates
    L1:        strata.MemPolicy{TTL: 10 * time.Second},
    L2:        strata.RedisPolicy{TTL: time.Minute},
    L3:        strata.PostgresPolicy{},
}
```

**Write-behind tuning** (`Config` fields):

| Field | Default | Purpose |
|-------|---------|---------|
| `WriteBehindFlushInterval` | 500 ms | how often the dirty buffer flushes |
| `WriteBehindFlushThreshold` | 100 | flush immediately when dirty count hits this |
| `WriteBehindMaxRetry` | 5 | max L3 retries before `OnWriteError` hook fires |

---

## Schema Migration

Strata manages its own DDL. Migrations are additive-only (new tables, new columns, new indexes) and idempotent.

```go
// Apply all pending migrations for all registered schemas
err := ds.Migrate(ctx)

// Apply SQL files from a directory (files must be named NNN_description.sql)
err = ds.MigrateFrom(ctx, "./migrations")

// Inspect migration state
records, err := ds.MigrationStatus(ctx)
for _, r := range records {
    fmt.Printf("%-30s applied: %s\n", r.FileName, r.AppliedAt.Format(time.RFC3339))
}
```

`Migrate` is safe to call on every startup — it only acts when something has changed. Migration state is persisted in the `_strata_migrations` table.

> **Note:** Destructive changes (drop column, rename column, change type) must be handled via manual SQL files in `MigrateFrom`. Strata will never drop or rename columns automatically.

---

## Encryption

Enable field-level AES-256-GCM encryption for any field tagged `encrypted`.

```go
// Generate a 32-byte key and store it in a secrets manager.
key := make([]byte, 32)
rand.Read(key)

ds, err := strata.NewDataStore(strata.Config{
    PostgresDSN:   "...",
    RedisAddr:     "...",
    EncryptionKey: key, // enables the built-in AES256GCM encryptor
})
```

Fields tagged `encrypted` are:

- Encrypted with AES-256-GCM (random nonce per write) before being written to Postgres.
- Decrypted transparently on reads from L3.
- **Not** cached in L1 or L2 while encrypted — Strata stores plaintext in the fast tiers so reads are always as fast as possible.

Only `string` fields support the `encrypted` tag today.

---

## Observability

### Stats

```go
s := ds.Stats()
fmt.Printf("gets=%d  sets=%d  deletes=%d  errors=%d  l1_entries=%d  dirty=%d\n",
    s.Gets, s.Sets, s.Deletes, s.Errors, s.L1Entries, s.DirtyCount)
```

| Field | Type | Description |
|-------|------|-------------|
| `Gets` | `int64` | Total Get calls |
| `Sets` | `int64` | Total Set calls |
| `Deletes` | `int64` | Total Delete calls |
| `Errors` | `int64` | Total errors |
| `L1Entries` | `int64` | Current L1 entry count |
| `DirtyCount` | `int64` | Write-behind entries pending flush |

### Logger

Implement the `Logger` interface to integrate with any logging library:

```go
type Logger interface {
    Info(msg string, keysAndValues ...any)
    Warn(msg string, keysAndValues ...any)
    Error(msg string, keysAndValues ...any)
    Debug(msg string, keysAndValues ...any)
}
```

Example — wrap `log/slog`:

```go
type slogAdapter struct{ l *slog.Logger }

func (a slogAdapter) Info(msg string, kv ...any)  { a.l.Info(msg, kv...) }
func (a slogAdapter) Warn(msg string, kv ...any)  { a.l.Warn(msg, kv...) }
func (a slogAdapter) Error(msg string, kv ...any) { a.l.Error(msg, kv...) }
func (a slogAdapter) Debug(msg string, kv ...any) { a.l.Debug(msg, kv...) }

ds, _ := strata.NewDataStore(strata.Config{
    Logger: slogAdapter{slog.Default()},
})
```

### Metrics

Implement `MetricsRecorder` (defined in `internal/metrics`) to emit counters and histograms to Prometheus, Datadog, etc. Pass `nil` or omit the field for a no-op recorder.

### Codec

Swap the serialisation format used for L1 and L2:

```go
import "github.com/AndrewDonelson/strata/internal/codec"

ds, _ := strata.NewDataStore(strata.Config{
    Codec: codec.MsgPack{}, // faster than JSON; default is codec.JSON{}
})
```

---

## Configuration Reference

```go
type Config struct {
    // ── Connections ──────────────────────────────────────────────────
    PostgresDSN   string   // "postgres://user:pass@host:5432/db?sslmode=disable"
    RedisAddr     string   // "localhost:6379"
    RedisPassword string
    RedisDB       int

    // ── Pool sizes ───────────────────────────────────────────────────
    L1Pool L1PoolConfig{
        MaxEntries int            // per-shard limit (256 shards)
        Eviction   EvictionPolicy // EvictLRU | EvictLFU | EvictFIFO
    }
    L2Pool L2PoolConfig{
        PoolSize     int
        DialTimeout  time.Duration
        ReadTimeout  time.Duration
        WriteTimeout time.Duration
    }
    L3Pool L3PoolConfig{
        MaxConns        int32
        MinConns        int32
        MaxConnLifetime time.Duration
        MaxConnIdleTime time.Duration
    }

    // ── TTL defaults (overridden per schema) ─────────────────────────
    DefaultL1TTL time.Duration // default: 5m
    DefaultL2TTL time.Duration // default: 30m

    // ── Write behaviour ──────────────────────────────────────────────
    DefaultWriteMode          WriteMode     // default: WriteThrough
    WriteBehindFlushInterval  time.Duration // default: 500ms
    WriteBehindFlushThreshold int           // default: 100
    WriteBehindMaxRetry       int           // default: 5

    // ── Invalidation ─────────────────────────────────────────────────
    InvalidationChannel string // Redis pub/sub channel; default: "strata:invalidate"

    // ── Pluggable components ─────────────────────────────────────────
    Codec   codec.Codec           // default: codec.JSON{}
    Metrics metrics.MetricsRecorder // default: no-op
    Logger  Logger                // default: no-op

    // ── Encryption ───────────────────────────────────────────────────
    EncryptionKey []byte // must be exactly 32 bytes; nil = disabled
}
```

**Minimal valid config** (only `PostgresDSN` and `RedisAddr` are required; all other fields have sensible defaults):

```go
strata.Config{
    PostgresDSN: os.Getenv("POSTGRES_DSN"),
    RedisAddr:   os.Getenv("REDIS_ADDR"),
}
```

---

## Error Reference

All errors are exported sentinel values compatible with `errors.Is`:

```go
// Schema
strata.ErrSchemaNotFound    // schema name not registered
strata.ErrSchemaDuplicate   // Register called twice with same name
strata.ErrNoPrimaryKey      // struct has no primary_key tag
strata.ErrInvalidModel      // nil or non-pointer model
strata.ErrMissingPrimaryKey // value passed to Set has empty/zero PK

// Data
strata.ErrNotFound     // record does not exist in any tier
strata.ErrDecodeFailed // codec or encryption decode error
strata.ErrEncodeFailed // codec or encryption encode error

// Infrastructure
strata.ErrL1Unavailable // in-memory store not initialised
strata.ErrL2Unavailable // Redis unavailable
strata.ErrL3Unavailable // Postgres unavailable
strata.ErrUnavailable   // all tiers unavailable

// Transaction
strata.ErrTxFailed  // Commit returned a Postgres error (rolled back)
strata.ErrTxTimeout // transaction deadline exceeded

// Config
strata.ErrInvalidConfig // missing required fields

// Hook
strata.ErrHookPanic // BeforeSet/BeforeGet hook panicked (recovered)

// Write-behind
strata.ErrWriteBehindMaxRetry // dirty entry exceeded max retry count
```

---

## Architecture Notes

### L1 — Sharded In-Memory Store

L1 uses 256 independent shards (FNV-32a hash → shard index) each protected by its own `sync.RWMutex`. This eliminates global lock contention under high concurrency. Eviction (LRU, LFU, or FIFO) runs per-shard. TTL expiry is checked lazily on read plus a background sweep every 30 seconds.

> `MaxEntries` in `MemPolicy` is the limit **per shard**. For a total limit of ~50 000, set `MaxEntries: 200`.

### L2 — Redis

Strata accepts any `redis.UniversalClient` (standalone, Sentinel, or Cluster). Keys follow the format `strata:{schema}:{id}`. Batch operations use a Redis pipeline for single round-trip performance.

### L3 — PostgreSQL

Strata uses `pgxpool` for connection pooling. `SetMany` uses the PostgreSQL COPY protocol for bulk inserts. Upsert is `INSERT … ON CONFLICT DO UPDATE`. Read replica connections (PostgresPolicy.ReadReplica) are used for `Search` and `Count` queries.

### Cross-Instance Invalidation

Every write publishes a JSON message to the `strata:invalidate` Redis channel:

```json
{"schema": "players", "id": "p1", "op": "set"}
```

Every running instance (including the writer) subscribes to this channel and removes the affected L1 entry on receipt. This keeps the L1 caches of all servers in a cluster consistent within ~50 ms of any write.

---

## Contributing

1. Fork the repository and create a feature branch.
2. Write tests first — the TDD plan in [STRATA_TDD.md] drives all development.
3. Run `go test -race ./...` — all tests must pass.
4. Run `go vet ./...` — no warnings.
5. Open a pull request with a clear description.

**Running tests:**

```bash
# Unit tests (no external dependencies)
go test -race ./...

# With verbose output
go test -race -v ./...

# Benchmarks
go test -bench=. -benchmem ./...
```

**Integration tests** (require Docker with Postgres and Redis) are tagged `integration` and not run by default:

```bash
STRATA_POSTGRES_DSN="postgres://..." STRATA_REDIS_ADDR="localhost:6379" \
  go test -tags integration -race ./...
```

---

*Strata — built by [Nlaak Studios](https://github.com/AndrewDonelson) and released as open-source software.*
