<p align="center">
  <img src="logo.png" alt="Strata" width="320" />
</p>

# Strata

**Four-tier data library for Go — L1 (memory) → L2 (Redis) → L3 (PostgreSQL) → L4 (distributed ledger/gossip) behind a single API.**

[![Go Reference](https://pkg.go.dev/badge/github.com/AndrewDonelson/strata.svg)](https://pkg.go.dev/github.com/AndrewDonelson/strata)
[![Go Version](https://img.shields.io/badge/go-1.21%2B-blue)](https://go.dev/dl/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## Overview

Strata removes the boilerplate of cache management from Go services. You define a schema once and call `Get`, `Set`, `Delete`, or `Search`. Strata automatically routes reads through L1 → L2 → L3, propagates writes, evicts stale entries, and keeps a cluster of server instances consistent via Redis pub/sub invalidation.

The optional **L4 distributed sync layer** adds a peer-to-peer gossip network with cryptographically signed, hash-chained records — providing an immutable audit trail, cross-node publishing, quorum-confirmed records, and revocation support, with no centralised coordinator required.

```
Get(ctx, "players", id, &dest)
  │
  ├─► L1 hit?  → return immediately   (~100 ns)
  ├─► L2 hit?  → populate L1 → return (~500 μs)
  └─► L3 hit?  → populate L2+L1 → return (~5 ms)
               └─► not found → ErrNotFound

L4 (optional, independent of the read path above):
  Publish(appID, nodeID, payload) → gossip to peers → quorum confirm → immutable record
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
15. [L4 — Distributed Sync Layer](#l4--distributed-sync-layer)
    - [Concepts](#concepts)
    - [Quick Start — Peer Mode](#quick-start--peer-mode)
    - [Quick Start — Ledger Mode](#quick-start--ledger-mode)
    - [L4 API Reference](#l4-api-reference)
    - [HTTP API Server](#http-api-server)
    - [L4 Configuration](#l4-configuration)
    - [L4 Errors](#l4-errors)
    - [L4 Transport Options](#l4-transport-options)
    - [L4 Store Options](#l4-store-options)
    - [L4 Testing Patterns](#l4-testing-patterns)
16. [Contributing](#contributing)

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

### L4 — Distributed Gossip Ledger

L4 is a standalone, leaderless peer-to-peer sync layer. Each node maintains its own copy of all records it receives; there is no central store and no leader election.

Records form a per-AppID hash chain: each record's `Hash` is computed over `prevHash|appID|uuid|payload|timestamp` using SHA-256. Every record is signed by its publisher using Ed25519 (`NodeSig`). Quorum confirmation advances a record from `pending` to `confirmed`; a revocation tombstone is gossiped immediately to all connected peers.

In **peer mode**, records reside in an in-memory store and are lost on restart. This is suited for ephemeral coordination, audit trails, or single-run integration tests.

In **ledger mode**, records are persisted to a per-node BoltDB file (`DataDir/l4.db`). The block height (`store.Height()`) represents the total number of records stored locally. The optional `APIServer` exposes query access over HTTP.

### Cross-Instance Invalidation

Every write publishes a JSON message to the `strata:invalidate` Redis channel:

```json
{"schema": "players", "id": "p1", "op": "set"}
```

Every running instance (including the writer) subscribes to this channel and removes the affected L1 entry on receipt. This keeps the L1 caches of all servers in a cluster consistent within ~50 ms of any write.

---

## L4 — Distributed Sync Layer

The L4 layer is an **optional**, **independent** module (`internal/l4`) that adds a peer-to-peer distributed ledger to Strata. It operates entirely separately from the L1/L2/L3 read-write path and requires no PostgreSQL or Redis.

### Concepts

| Concept | Description |
|---------|-------------|
| **Record** | An immutable, hash-chained, Ed25519-signed data entry stored across peers. |
| **AppID** | Logical namespace/application identifier that partitions records. |
| **Quorum** | Number of peer confirmations required before a record changes from `pending` → `confirmed`. |
| **Revocation** | Cryptographically signed tombstone that marks a record as revoked without physically deleting it. |
| **Peer mode** | In-memory store; records are gossiped and confirmed across nodes but not persisted to disk. Best for ephemeral audit trails. |
| **Ledger mode** | BoltDB-backed; records are persisted locally and survive restarts. Best for durable, append-only ledgers. |
| **Gossip** | Nodes exchange `publish`, `confirm`, `peer_list`, `peer_request`, `ping`/`pong`, and `revoke` messages over the wire. |

### Quick Start — Peer Mode

```go
package main

import (
    "fmt"
    "log"
    "time"

    "github.com/AndrewDonelson/strata/internal/l4"
)

func main() {
    // Create a layer with in-memory storage (no disk, no deps).
    layer, err := l4.New(l4.Config{
        Enabled:      true,
        Mode:         "peer",
        Port:         7743,
        Quorum:       2,               // 2 peers must confirm
        SyncInterval: 10 * time.Second,
        MaxPeers:     50,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer layer.Shutdown()

    // Subscribe to records for an application.
    _ = layer.Subscribe("myapp", func(rec l4.L4Record) {
        fmt.Printf("new record: uuid=%s status=%s\n", rec.UUID, rec.Status)
    })

    // Publish a record — gossips to all connected peers.
    rec, err := layer.Publish("myapp", "node-1", map[string]interface{}{
        "action": "purchase",
        "amount": 42.50,
        "userID": "u-8823",
    })
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("published: hash=%s status=%s\n", rec.Hash, rec.Status)

    // Query a record by AppID + UUID.
    got, err := layer.Query("myapp", rec.UUID)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("queried: confirmed=%v\n", got.Confirmed)
}
```

### Quick Start — Ledger Mode

Ledger mode persists all records to a local BoltDB file. Combine with `NewWithComponents` to inject a specific store and transport:

```go
package main

import (
    "log"

    "github.com/AndrewDonelson/strata/internal/l4"
)

func main() {
    cfg := l4.Config{
        Enabled:  true,
        Mode:     "ledger",
        Port:     7743,
        DataDir:  "/var/lib/myapp/ledger",
        Quorum:   1,
    }
    if err := cfg.Validate(); err != nil {
        log.Fatal(err)
    }

    // Ed25519 node identity — generates a fresh keypair.
    signer, err := l4.NewSigner()
    if err != nil {
        log.Fatal(err)
    }

    // Persistent BoltDB store.
    store, err := l4.NewBoltStore(cfg.DataDir)
    if err != nil {
        log.Fatal(err)
    }

    // TCP transport for real network peers.
    transport := l4.NewTCPTransport(signer.PublicKeyHex(), cfg.MaxPeers, nil)

    layer, err := l4.NewWithComponents(cfg, signer, store, transport)
    if err != nil {
        log.Fatal(err)
    }
    defer layer.Shutdown()

    // Start listening for peer connections.
    go transport.Listen(":7743")

    // Publish + revoke.
    rec, _ := layer.Publish("audit", "node-1", map[string]interface{}{
        "event": "login",
        "ip":    "1.2.3.4",
    })

    // Revoke the record (creates a signed tombstone).
    _ = layer.Revoke("audit", rec.UUID)

    got, _ := layer.Query("audit", rec.UUID)
    if got.Status == l4.StatusRevoked {
        log.Println("record successfully revoked")
    }
}
```

### L4 API Reference

#### `l4.L4Layer` interface

```go
type L4Layer interface {
    // Publish creates, signs, hashes, and gossips a new record.
    // Returns ErrAlreadyPublished if a record with the same UUID already exists.
    Publish(appID, nodeID string, payload map[string]interface{}) (L4Record, error)

    // Query retrieves a record by AppID + UUID.
    // Returns ErrNotFound if the record does not exist locally.
    Query(appID, recordID string) (L4Record, error)

    // Revoke marks a record as revoked via a signed revocation record.
    // Returns ErrNotFound if the record does not exist.
    Revoke(appID, recordID string) error

    // Subscribe registers a callback invoked whenever a record for appID is
    // stored locally (including records received from peers).
    Subscribe(appID string, handler RecordHandler) error

    // Unsubscribe removes the callback for appID.
    Unsubscribe(appID string) error

    // Status returns a snapshot of the layer's operational state.
    Status() L4Status

    // PeerCount returns the number of currently connected peers.
    PeerCount() int

    // Shutdown stops the layer, closes transport, and flushes the store.
    Shutdown() error
}

// RecordHandler is called in a goroutine when a record is stored locally.
type RecordHandler func(rec L4Record)
```

#### `l4.L4Record` struct

```go
type L4Record struct {
    UUID       string                 `json:"uuid"`
    AppID      string                 `json:"app_id"`
    Payload    map[string]interface{} `json:"payload"`
    Hash       string                 `json:"hash"`       // SHA-256 of chain fields
    PrevHash   string                 `json:"prev_hash"`  // previous record's hash ("genesis" for first)
    Timestamp  int64                  `json:"timestamp"`  // UnixNano
    VerifiedAt string                 `json:"verified_at"` // "YYYY-MM"
    NodeID     string                 `json:"node_id"`    // publisher's Ed25519 pub key (hex)
    NodeSig    []byte                 `json:"node_sig"`   // Ed25519 signature
    UserSig    []byte                 `json:"user_sig,omitempty"` // optional application-level sig
    Revoked    bool                   `json:"revoked"`
    Confirmed  bool                   `json:"confirmed"`  // true when Quorum confirmations received
    Status     string                 `json:"status"`     // "pending" | "confirmed" | "revoked"
}
```

Record status constants:

```go
l4.StatusPending   = "pending"
l4.StatusConfirmed = "confirmed"
l4.StatusRevoked   = "revoked"
l4.GenesisHash     = "genesis"  // PrevHash for the first record in a chain
```

#### `l4.L4Status` struct

```go
type L4Status struct {
    Enabled     bool
    Mode        string   // "peer" or "ledger"
    PeerCount   int
    BlockHeight int64
    Pending     int      // records awaiting quorum
    NodeID      string   // this node's Ed25519 public key (hex)
    Uptime      string
}
```

#### Constructor functions

```go
// New creates an L4Layer from config. Uses MemStore + MemTransport internally.
layer, err := l4.New(cfg l4.Config) (l4.L4Layer, error)

// NewWithComponents creates a fully-wired layer with injected components.
// Pass nil for signer to use an anonymous node (NodeID will be "").
layer, err := l4.NewWithComponents(
    cfg       l4.Config,
    signer    l4.L4Signer,    // nil = anonymous
    store     l4.L4Store,
    transport l4.L4Transport,
) (l4.L4Layer, error)

// DefaultConfig returns defaults with Enabled = false.
cfg := l4.DefaultConfig()
```

#### `l4.L4Signer`

```go
type L4Signer interface {
    PublicKeyHex() string
    Sign(record *L4Record) ([]byte, error)
    Verify(record *L4Record, publicKeyHex string, sig []byte) bool
    CanonicalBytes(record *L4Record) []byte
}

// NewSigner generates a fresh Ed25519 keypair.
signer, err := l4.NewSigner()

// NewSignerFromKey recreates a signer from an existing Ed25519 private key.
signer := l4.NewSignerFromKey(privKey ed25519.PrivateKey)
```

### HTTP API Server

`APIServer` exposes an L4 layer over HTTP. Intended for **ledger mode** deployments where external services need to query the ledger.

```go
srv := l4.NewAPIServer(layer, store, transport)

// Start listening (blocks).
go srv.Listen(":8080")

// Graceful shutdown.
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
_ = srv.Shutdown(ctx)
```

#### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/query/{uuid}?app_id=<appID>` | Fetch a record by UUID and AppID. Returns 200 with JSON `L4Record`, 404 if not found. |
| `GET` | `/peers` | Returns JSON array of connected `L4Peer` objects. |
| `POST` | `/sync` | Returns `{"height": N}` — the local ledger block height. |

All endpoints enforce:
- **CORS**: `Access-Control-Allow-Origin: *` header on every response.
- **Rate limiting**: 100 requests per minute per source IP (sliding window).

### L4 Configuration

```go
type l4.Config struct {
    Enabled        bool          // false = layer is a no-op; all methods return ErrL4Disabled
    Mode           string        // "peer" (in-memory) or "ledger" (BoltDB-backed)
    Port           int           // TCP listen port; default 7743
    DataDir        string        // BoltDB directory for ledger mode; default "/var/lib/strata/l4"
    SyncInterval   time.Duration // peer gossip frequency; default 30s
    MaxPeers       int           // max simultaneous peer connections; default 50
    Quorum         int           // confirmations needed for pending → confirmed; default 3
    BootstrapPeers []string      // "host:port" peers to dial on startup
    DNSSeed        string        // DNS seed hostname for peer discovery
    NodeKeyPath    string        // path to load/persist the node's Ed25519 private key
}
```

Call `cfg.Validate()` before use — it applies defaults and returns `ErrInvalidL4Mode` or `ErrInvalidQuorum` on bad values.

### L4 Errors

```go
l4.ErrL4Disabled       // layer is disabled (Enabled: false)
l4.ErrInvalidL4Mode    // Mode must be "peer" or "ledger"
l4.ErrInvalidQuorum    // Quorum must be >= 1
l4.ErrAlreadyPublished // duplicate UUID+AppID
l4.ErrAlreadyRevoked   // record is already revoked
l4.ErrNotFound         // record not found locally
l4.ErrInvalidSignature // Ed25519 signature check failed
l4.ErrChainBreak       // hash chain integrity violated
l4.ErrNoPeers          // no connected peers to gossip to
l4.ErrQuorumNotMet     // record is still pending (not enough confirmations)
l4.ErrStoreUnavailable // store not accessible (peer mode restriction)
```

All errors are compatible with `errors.Is`.

### L4 Transport Options

```go
// TCP — for real network deployments.
transport := l4.NewTCPTransport(nodeID string, maxPeers int, handler msgHandler)

// In-memory hub — for in-process testing and local multi-node simulations.
hub := l4.NewMemTransportHub()
transportA := l4.NewMemTransport(nodeIDa string, maxPeers int, hub, handler)
transportB := l4.NewMemTransport(nodeIDB string, maxPeers int, hub, handler)
hub.Connect(nodeIDA, nodeIDB) // wire them together

// Both implement L4Transport:
type L4Transport interface {
    Listen(addr string) error
    Connect(peer L4Peer) error
    Disconnect(nodeID string) error
    Broadcast(msg L4Message) error
    Send(nodeID string, msg L4Message) error
    Peers() []L4Peer
    Close() error
}
```

### L4 Store Options

```go
// MemStore — in-process, no persistence. Default for peer mode and tests.
store := l4.NewMemStore()

// BoltStore — BoltDB-backed, persists to disk. Required for ledger mode.
store, err := l4.NewBoltStore(dataDir string)

// Both implement L4Store:
type L4Store interface {
    Put(record L4Record) error
    Get(appID, uuid string) (*L4Record, error)
    GetByHash(hash string) (*L4Record, error)
    Latest(appID string, limit int) ([]L4Record, error)
    Height() (int64, error)
    Close() error
}
```

### L4 Testing Patterns

#### Pattern 1 — In-memory multi-node

```go
func TestTwoNodes(t *testing.T) {
    cfg := l4.Config{Enabled: true, Mode: "peer", Quorum: 1}
    _ = cfg.Validate()
    hub := l4.NewMemTransportHub()

    signerA, _ := l4.NewSigner()
    signerB, _ := l4.NewSigner()

    layerA, _ := l4.NewWithComponents(cfg, signerA, l4.NewMemStore(),
        l4.NewMemTransport(signerA.PublicKeyHex(), 10, hub, nil))
    layerB, _ := l4.NewWithComponents(cfg, signerB, l4.NewMemStore(),
        l4.NewMemTransport(signerB.PublicKeyHex(), 10, hub, nil))
    defer layerA.Shutdown()
    defer layerB.Shutdown()

    // Wire them together.
    hub.Connect(signerA.PublicKeyHex(), signerB.PublicKeyHex())

    rec, _ := layerA.Publish("app", "node-a", map[string]interface{}{"k": "v"})
    time.Sleep(50 * time.Millisecond) // allow gossip

    got, err := layerB.Query("app", rec.UUID)
    if err != nil {
        t.Fatalf("node B should have received the record: %v", err)
    }
    t.Logf("node B has record: status=%s confirmed=%v", got.Status, got.Confirmed)
}
```

#### Pattern 2 — BoltDB ledger with revocation

```go
func TestLedgerRevoke(t *testing.T) {
    dir := t.TempDir()
    store, _ := l4.NewBoltStore(dir)

    cfg := l4.Config{Enabled: true, Mode: "ledger", Quorum: 1}
    _ = cfg.Validate()
    signer, _ := l4.NewSigner()
    hub := l4.NewMemTransportHub()
    transport := l4.NewMemTransport(signer.PublicKeyHex(), 10, hub, nil)

    layer, _ := l4.NewWithComponents(cfg, signer, store, transport)
    defer layer.Shutdown()

    rec, _ := layer.Publish("audit", "node-1", map[string]interface{}{"ip": "1.2.3.4"})
    _ = layer.Revoke("audit", rec.UUID)

    got, _ := layer.Query("audit", rec.UUID)
    if got.Status != l4.StatusRevoked {
        t.Errorf("expected revoked, got %s", got.Status)
    }
}
```

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
