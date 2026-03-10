# Strata — pgvector Extension: TDD Design Document

**Project:** `github.com/AndrewDonelson/strata`  
**Feature:** First-class vector search via `pgvector` (L3 semantic layer) with smart `EmbeddingProvider` integration  
**Author:** AI Coding Agent Task  
**Status:** Pre-implementation — tests must be written and failing before any implementation code  
**Document version:** 2.0

---

## Overview

This document defines the complete Test-Driven Development plan to extend Strata with native `pgvector` support. The result is a `VectorSearch` method on `DataStore`, a new `vector` struct tag, new `IndexType` constants, updated `Schema`/`Index` structs, a pluggable `EmbeddingProvider` interface, automatic dimension management, schema metadata persistence, and a `ReEmbed` migration path — all tested before implementation.

Strata owns the **full embedding lifecycle**: it calls the provider to generate vectors, stores them, indexes them, detects model changes, and orchestrates re-embedding migrations. The calling application only needs to configure which provider to use. Dimension numbers never appear in application code.

---

## Scope

### In Scope
- `pgvector-go` dependency integration
- `strata:"vector"` struct tag (implies `omit_cache` — never in L1/L2)
- `pgvector.Vector` as a valid model field type
- `IndexType` enum: `IndexIVFFlat`, `IndexHNSW`
- `Index` struct extended with `Type`, `Lists` (IVFFlat), `M` / `EfConstruction` (HNSW)
- `EmbeddingProvider` interface with `OllamaProvider` and `OpenAIProvider` built-in implementations
- Automatic dimension detection from provider — no hardcoded dimension numbers in application code
- `strata_schema_meta` persistence table — tracks model ID and dimension per schema
- `ErrEmbeddingModelChanged` detection on startup when provider model changes
- `ds.VectorSearch(ctx, schema, query, topK, filters)` — accepts plain string, Strata embeds internally
- `ds.ReEmbed(ctx, schemaName, textFieldName)` — background re-embedding migration
- `ds.Migrate()` correctly emitting `CREATE INDEX ... USING ivfflat/hnsw`
- Cache warming: VectorSearch hits → populate L2 and L1 on result
- Updated `SKILL.md`

### Out of Scope
- L1/L2 vector similarity (not feasible; vector search is L3-only by design)
- Batch vector insert optimization (future)
- Multi-vector fields per model (future)
- Custom embedding provider authentication beyond API key (future)

---

## Dependencies

```
github.com/pgvector/pgvector-go v0.2.0+
```

Requires pgvector Postgres extension installed on the server:
```sql
CREATE EXTENSION IF NOT EXISTS vector;
```

This must be documented and verified during `Migrate()`. If the extension is absent, `Migrate()` should return a clear `ErrPgvectorExtensionMissing` error when any registered schema contains a vector field.

Built-in `EmbeddingProvider` implementations have no additional Go dependencies beyond the standard library — they communicate with Ollama and OpenAI via HTTP using `net/http`.

---

## Target Infrastructure (CQAI Cluster)

This implementation is designed to run against the following local AI infrastructure. The AI coding agent must understand the distinction between these two model roles — they are **not interchangeable**.

### Embedding Model — `nomic-embed-text`
- **Role:** Converts text to fixed-size float32 vectors for semantic similarity search
- **Ollama command:** `ollama run nomic-embed-text`
- **Output dimensions:** 768
- **Used by:** `OllamaProvider` → `strata.Config.EmbeddingProvider`
- **Called when:** A question enters the system (to search L3) and when a new FAQ is stored (to index it)
- **Characteristics:** Tiny (~274MB VRAM), fast, purpose-built for embeddings — cannot generate prose

### LLM — `qwen3-32b-q8-128k:latest`
- **Role:** Generates natural language answers when L1/L2/L3 all return no match above confidence threshold
- **Ollama command:** `ollama run qwen3-32b-q8-128k:latest`
- **Used by:** The chatbot service's LLM batch queue — **not by Strata**
- **Called when:** VectorSearch and pg_trgm both fail to find a confident answer
- **Characteristics:** Large (~32GB+ VRAM), slower, full reasoning capability — cannot produce embeddings

### Why They Cannot Be Swapped
A text generation model like Qwen3 can produce text that *describes* similarity but cannot output a numerically meaningful fixed-length vector for ANN index queries. An embedding model like `nomic-embed-text` produces vectors but cannot answer a question in prose. They serve entirely different roles in the pipeline and both must be running on CQAI for the full system to operate.

### Config Reference for AI Agent

```go
// Strata config — embedding only
strata.Config{
    PostgresDSN:       os.Getenv("POSTGRES_DSN"),
    RedisAddr:         os.Getenv("REDIS_ADDR"),
    EmbeddingProvider: strata.NewOllamaProvider(
        "http://cqai:11434",    // CQAI cluster Ollama endpoint
        "nomic-embed-text",     // embedding model — 768 dimensions
    ),
}

// LLM client config — separate, not part of Strata
llm := ollama.NewClient(
    "http://cqai:11434",
    "qwen3-32b-q8-128k:latest", // generation model — chatbot last resort
)
```

> **Note for AI coding agent:** When writing tests that require an embedding provider, always use `MockProvider` or `OllamaProvider` pointing at `nomic-embed-text`. Never attempt to use `qwen3-32b-q8-128k:latest` as an embedding source — it will not produce valid vectors and tests will fail in unexpected ways.

---

## New Public API

### 1. `EmbeddingProvider` Interface

```go
type EmbeddingProvider interface {
    Embed(ctx context.Context, text string) (pgvector.Vector, error)
    Dimensions() int    // provider declares its own output size — Strata reads this at Register time
    ModelID() string    // e.g. "nomic-embed-text", "text-embedding-3-small" — stored in schema meta
}
```

Strata calls `Dimensions()` once at `Register()` to determine the vector column size. No dimension number ever appears in application code.

### 2. Built-in Providers

```go
// OllamaProvider — for local Ollama instances (CQAI cluster, dev machines)
func NewOllamaProvider(baseURL, modelName string) EmbeddingProvider
// Example:
//   strata.NewOllamaProvider("http://cqai:11434", "nomic-embed-text")
//   Dimensions() auto-detected from first Embed() call and cached.

// OpenAIProvider — for OpenAI-compatible embedding APIs
func NewOpenAIProvider(apiKey, modelName string) EmbeddingProvider
// Example:
//   strata.NewOpenAIProvider(os.Getenv("OPENAI_API_KEY"), "text-embedding-3-small")
//   Dimensions() resolved from known model dimension table; falls back to first Embed() call.
```

### 3. `Config` addition

```go
type Config struct {
    // ... existing fields unchanged ...

    // EmbeddingProvider — required when any registered schema has a strata:"vector" field.
    // If nil and a vector schema is registered, ds.Migrate() returns ErrNoEmbeddingProvider.
    EmbeddingProvider EmbeddingProvider
}
```

### 4. `strata_schema_meta` — Internal Persistence Table

Created automatically by `Migrate()`. Never accessed directly by application code.

```sql
CREATE TABLE IF NOT EXISTS strata_schema_meta (
    schema_name      TEXT PRIMARY KEY,
    vector_field     TEXT NOT NULL,
    embedding_model  TEXT NOT NULL,
    dimensions       INT  NOT NULL,
    reembed_status   TEXT NOT NULL DEFAULT 'idle',  -- 'idle' | 'running' | 'failed'
    reembed_progress INT  NOT NULL DEFAULT 0,       -- records processed so far
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

On every `Migrate()` call, Strata compares the registered provider's `ModelID()` and `Dimensions()` against stored values:

| Situation | Action |
|---|---|
| No row exists | Insert row, create column + index at correct dimension |
| Same model ID + same dimension | No-op (idempotent) |
| Same model ID, different dimension | Return `ErrVectorDimensionMismatch` |
| **Different model ID** | Return `ErrEmbeddingModelChanged` with old and new model names |

### 5. `SimilarityResult`

```go
type SimilarityResult struct {
    ID    string
    Score float64  // cosine similarity: 1.0 = identical, 0.0 = orthogonal
    Value any      // hydrated model struct (same as Get would return)
}
```

### 6. `VectorSearch` — accepts plain string, Strata embeds internally

```go
func (ds *DataStore) VectorSearch(
    ctx        context.Context,
    schemaName string,
    query      string,           // plain text — Strata calls provider.Embed() internally
    topK       int,
    filters    map[string]any,   // optional column = value filters (AND logic), nil = no filter
) ([]SimilarityResult, error)
```

- Calls `provider.Embed(ctx, query)` to produce the query vector.
- Returns up to `topK` results ordered by cosine similarity descending.
- `filters` supports `customer_id` scoping and any other column.
- Results are **cache-warmed**: each returned record is written to L2 then L1 before returning.
- Returns `ErrSchemaNotFound` if schema is not registered.
- Returns `ErrNoVectorField` if the schema model has no `strata:"vector"` field.
- Returns `ErrPgvectorExtensionMissing` if extension is not installed.
- Returns `ErrNoEmbeddingProvider` if `Config.EmbeddingProvider` is nil.

### 7. `ReEmbed` — model migration

```go
func (ds *DataStore) ReEmbed(
    ctx           context.Context,
    schemaName    string,
    textFieldName string,   // name of the string field to re-embed (e.g. "question")
) error
```

- Runs as a resumable background migration — safe to interrupt and restart.
- Walks all L3 records for the schema in batches of 100.
- For each record: calls `provider.Embed(ctx, record[textFieldName])`, updates the vector column.
- Tracks progress in `strata_schema_meta.reembed_progress`.
- On completion: drops old vector index, rebuilds with new dimension, updates `strata_schema_meta`.
- Returns immediately if `reembed_status = 'running'` (prevents duplicate runs).
- Emits log entries per batch with progress percentage.

### 8. New Index fields

```go
type IndexType string

const (
    IndexDefault IndexType = ""         // standard btree (existing behaviour)
    IndexIVFFlat IndexType = "ivfflat"
    IndexHNSW    IndexType = "hnsw"
    IndexTrigram IndexType = "gin"      // for pg_trgm — added here for completeness
)

type Index struct {
    Fields         []string  // existing
    Unique         bool      // existing
    Name           string    // existing
    Type           IndexType // NEW — defaults to IndexDefault
    Lists          int       // NEW — IVFFlat: number of lists (default: 100)
    M              int       // NEW — HNSW: max connections per layer (default: 16)
    EfConstruction int       // NEW — HNSW: build-time search width (default: 64)
    DistanceFunc   string    // NEW — "cosine" | "l2" | "ip" (default: "cosine")
}
```

### 9. New struct tag

| Tag | Effect |
|-----|--------|
| `vector` | Marks field as `vector(N)` column in Postgres. Implies `omit_cache`. Field type must be `pgvector.Vector`. Dimension is set automatically from `Config.EmbeddingProvider.Dimensions()`. |

### 10. New errors

```go
var (
    ErrNoVectorField            = errors.New("strata: schema has no vector field")
    ErrPgvectorExtensionMissing = errors.New("strata: pgvector extension not installed")
    ErrVectorDimensionMismatch  = errors.New("strata: vector dimension does not match index")
    ErrInvalidIndexType         = errors.New("strata: invalid index type for field")
    ErrTopKInvalid              = errors.New("strata: topK must be >= 1")
    ErrNoEmbeddingProvider      = errors.New("strata: EmbeddingProvider is required for vector schemas")
    ErrEmbeddingModelChanged    = errors.New("strata: embedding model has changed since last migration")
    ErrReEmbedAlreadyRunning    = errors.New("strata: re-embed migration already in progress")
    ErrReEmbedTextFieldMissing  = errors.New("strata: specified text field not found in schema model")
)

---

## File Structure

New and modified files (no existing files deleted):

```
strata/
├── embedding.go            NEW  — EmbeddingProvider interface, OllamaProvider, OpenAIProvider
├── embedding_test.go       NEW  — Groups 9 and 10 tests
├── vector.go               NEW  — VectorSearch, ReEmbed, vector field detection
├── vector_test.go          NEW  — Groups 1-8 tests
├── schema_meta.go          NEW  — strata_schema_meta table read/write helpers
├── schema.go               MOD  — IndexType, updated Index struct
├── tags.go                 MOD  — "vector" tag parsing, implies omit_cache
├── migrate.go              MOD  — vector column DDL, pgvector index DDL, extension check, meta sync
├── config.go               MOD  — EmbeddingProvider field added to Config
├── errors.go               MOD  — new error vars
└── SKILL.md                MOD  — vector + provider sections appended
```

---

## Test Plan

All tests in `vector_test.go` unless noted. Tests are written **before** implementation. Run `go test ./... -run TestVector` to execute only vector tests.

---

### Group 1: Struct Tag Parsing (Unit — no DB required)

#### T1.1 — Vector tag detected on pgvector.Vector field
```
Given: a struct with a pgvector.Vector field tagged strata:"vector"
When:  schema is reflected
Then:  field is classified as vector type
And:   omit_cache is implicitly set (field absent from L1/L2 serialization)
```

#### T1.2 — Vector tag on non-pgvector.Vector type returns error at Register
```
Given: a struct with a string field tagged strata:"vector"
When:  ds.Register(schema) is called
Then:  returns ErrInvalidTagForType
```

#### T1.3 — Vector field without explicit index still registers (no index = sequential scan)
```
Given: a struct with strata:"vector" but no Index entry for that field
When:  ds.Register(schema) is called
Then:  no error
And:   Migrate() emits vector(N) column DDL without index DDL
```

#### T1.4 — Vector field with IVFFlat index registers cleanly
```
Given: a schema with Index{Fields:["embedding"], Type:IndexIVFFlat, Lists:100}
When:  ds.Register(schema) is called
Then:  no error
```

#### T1.5 — Vector field with HNSW index registers cleanly
```
Given: a schema with Index{Fields:["embedding"], Type:IndexHNSW, M:16, EfConstruction:64}
When:  ds.Register(schema) is called
Then:  no error
```

#### T1.6 — IVFFlat index on non-vector field returns error at Register
```
Given: a schema with Index{Fields:["username"], Type:IndexIVFFlat}
When:  ds.Register(schema) is called
Then:  returns ErrInvalidIndexType
```

---

### Group 2: Migration DDL (Integration — requires Postgres + pgvector extension)

#### T2.1 — Migrate emits correct vector column DDL using provider dimension
```
Given: Config.EmbeddingProvider is OllamaProvider("nomic-embed-text") reporting Dimensions()=768
And:   a schema with a pgvector.Vector field tagged strata:"vector"
When:  ds.Migrate(ctx) is called
Then:  the column is created as vector(768) in Postgres
And:   strata_schema_meta row is inserted with embedding_model="nomic-embed-text", dimensions=768
And:   subsequent Migrate() is idempotent (no error, no duplicate column)
```

#### T2.2 — Migrate emits IVFFlat index DDL
```
Given: schema with IVFFlat index on embedding field, Lists:100, DistanceFunc:"cosine"
When:  ds.Migrate(ctx) is called
Then:  SQL contains: CREATE INDEX ... USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100)
```

#### T2.3 — Migrate emits HNSW index DDL
```
Given: schema with HNSW index, M:16, EfConstruction:64, DistanceFunc:"cosine"
When:  ds.Migrate(ctx) is called
Then:  SQL contains: CREATE INDEX ... USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64)
```

#### T2.4 — Migrate returns ErrPgvectorExtensionMissing when extension absent
```
Given: a Postgres instance without the vector extension installed
And:   a schema with a vector field registered
When:  ds.Migrate(ctx) is called
Then:  returns ErrPgvectorExtensionMissing
And:   error message includes instructions to run: CREATE EXTENSION IF NOT EXISTS vector;
```

#### T2.5 — Migrate on schema with no vector fields does NOT check for extension
```
Given: only non-vector schemas registered
When:  ds.Migrate(ctx) is called on a Postgres instance without vector extension
Then:  no error (extension check is skipped entirely)
```

---

### Group 3: VectorSearch — Input Validation (Unit)

#### T3.1 — topK < 1 returns ErrTopKInvalid
```
Given: valid schema and non-empty query string
When:  ds.VectorSearch(ctx, schema, "hello", 0, nil) is called
Then:  returns ErrTopKInvalid immediately, no provider or DB call
```

#### T3.2 — Unknown schema returns ErrSchemaNotFound
```
Given: schema "faqs" is not registered
When:  ds.VectorSearch(ctx, "faqs", "hello", 5, nil)
Then:  returns ErrSchemaNotFound
```

#### T3.3 — Schema with no vector field returns ErrNoVectorField
```
Given: schema "players" registered with no vector field
When:  ds.VectorSearch(ctx, "players", "hello", 5, nil)
Then:  returns ErrNoVectorField
```

#### T3.4 — Nil EmbeddingProvider returns ErrNoEmbeddingProvider
```
Given: Config.EmbeddingProvider is nil
And:   a vector schema is registered
When:  ds.VectorSearch(ctx, "faqs", "hello", 5, nil)
Then:  returns ErrNoEmbeddingProvider
```

#### T3.5 — Empty query string returns error
```
Given: valid schema with vector field and configured provider
When:  ds.VectorSearch(ctx, "faqs", "", 5, nil)
Then:  returns non-nil error indicating empty query
```

#### T3.6 — Nil context returns error
```
Given: valid schema and query
When:  ds.VectorSearch(nil, schema, "hello", 5, nil)
Then:  returns non-nil error
```

---

### Group 4: VectorSearch — Correctness (Integration)

#### T4.1 — Returns top K results ordered by cosine similarity descending
```
Given: 10 FAQ records stored with known embeddings (pre-computed via mock provider)
And:   a query string whose embedding is closest to records A, B, C in that order
When:  ds.VectorSearch(ctx, "faqs", "query text", 3, nil)
Then:  returns exactly 3 results
And:   results are ordered [A, B, C]
And:   each result.Score is between 0.0 and 1.0
And:   result[0].Score >= result[1].Score >= result[2].Score
```

#### T4.2 — Returns fewer than topK when fewer records exist
```
Given: 2 FAQ records stored
When:  ds.VectorSearch(ctx, "faqs", "query text", 10, nil)
Then:  returns exactly 2 results, no error
```

#### T4.3 — Returns empty slice (not error) when no records exist
```
Given: schema registered and migrated, zero records stored
When:  ds.VectorSearch(ctx, "faqs", "query text", 5, nil)
Then:  returns empty []SimilarityResult{}, nil
```

#### T4.4 — filters parameter scopes results by customer_id
```
Given: 6 FAQ records — 3 with customer_id "biz-001", 3 with customer_id "biz-002"
And:   query string whose embedding is close to all 6
When:  ds.VectorSearch(ctx, "faqs", "query", 5, map[string]any{"customer_id": "biz-001"})
Then:  returns exactly 3 results, all with customer_id "biz-001"
```

#### T4.5 — filters with multiple keys applies AND logic
```
Given: records with varying customer_id and status fields
When:  VectorSearch with filters {"customer_id": "biz-001", "status": "active"}
Then:  only records matching both conditions are returned
```

#### T4.6 — SimilarityResult.Value is fully hydrated model struct
```
Given: FAQ records stored with all fields populated
When:  VectorSearch returns results
Then:  result.Value is a *FAQ with all non-omit_cache fields populated
And:   the vector field itself is populated in the returned struct
```

#### T4.7 — Provider Embed() error propagates cleanly
```
Given: mock provider configured to return error on Embed()
When:  ds.VectorSearch(ctx, "faqs", "query", 5, nil)
Then:  returns the provider error wrapped with context
And:   no DB query is made
```

---

### Group 5: Cache Warming (Integration)

#### T5.1 — VectorSearch results are written to L2 after query
```
Given: 3 FAQ records in Postgres, none in Redis
When:  ds.VectorSearch returns 3 results
Then:  each result's record is now retrievable via L2 (verified by disconnecting L3 and calling Get)
```

#### T5.2 — VectorSearch results are written to L1 after query
```
Given: 3 FAQ records in Postgres, none in memory
When:  ds.VectorSearch returns 3 results
Then:  subsequent ds.Get() for each returned ID resolves from L1 (0 L2/L3 calls)
```

#### T5.3 — Cache warming failure does not fail VectorSearch
```
Given: Redis is unavailable
When:  ds.VectorSearch completes a successful L3 query
Then:  results are returned successfully
And:   L2 warming error is logged but not returned
And:   L1 warming succeeds independently
```

---

### Group 6: Set() with Vector Field (Integration)

#### T6.1 — Set() stores vector field to L3, skips L1/L2
```
Given: FAQ model with embedding field
When:  ds.Set(ctx, "faqs", id, &faq) where faq.Embedding is populated
Then:  embedding is stored in Postgres vector column
And:   embedding is NOT present in Redis (verified by direct Redis inspection)
And:   embedding is NOT present in L1 (verified via Stats or direct cache inspection)
```

#### T6.2 — Get() retrieves vector field from L3 correctly
```
Given: FAQ record stored with known embedding
When:  ds.Get(ctx, "faqs", id, &dest) after flushing L1/L2
Then:  dest.Embedding equals the original embedding (within float32 precision)
```

#### T6.3 — Get() from L1/L2 returns record with zero-value embedding (omit_cache)
```
Given: FAQ record in L1 cache (put there by a prior Get or VectorSearch warm)
When:  ds.Get() hits L1
Then:  dest.Embedding is zero-value (not populated from cache — by design)
Note:  This is expected and correct behaviour; callers needing the vector must use VectorSearch or go to L3
```

---

### Group 7: Dimension Management via Provider (Unit + Integration)

#### T7.1 — Dimension sourced from provider.Dimensions() at Register time
```
Given: Config.EmbeddingProvider mock returning Dimensions()=768
And:   schema with strata:"vector" field
When:  ds.Register(schema) is called
Then:  internal schema metadata records dimension=768
And:   no dimension number appears anywhere in application model definition
```

#### T7.2 — Nil EmbeddingProvider with vector schema returns ErrNoEmbeddingProvider at Migrate
```
Given: Config.EmbeddingProvider is nil
And:   schema with vector field registered
When:  ds.Migrate(ctx) is called
Then:  returns ErrNoEmbeddingProvider with message recommending NewOllamaProvider or NewOpenAIProvider
```

#### T7.3 — Same model ID on re-migration is no-op
```
Given: schema previously migrated with model="nomic-embed-text", dimensions=768
And:   same provider still configured
When:  ds.Migrate(ctx) is called again
Then:  no DDL executed, no error, strata_schema_meta unchanged
```

#### T7.4 — Different model ID on re-migration returns ErrEmbeddingModelChanged
```
Given: schema previously migrated with model="nomic-embed-text", dimensions=768
And:   Config.EmbeddingProvider now returns ModelID()="text-embedding-3-small", Dimensions()=1536
When:  ds.Migrate(ctx) is called
Then:  returns ErrEmbeddingModelChanged
And:   error message includes both old model name and new model name
And:   no DDL changes are made (safe — caller must explicitly call ReEmbed)
```

#### T7.5 — Same model ID but different dimension returns ErrVectorDimensionMismatch
```
Given: schema migrated with model="custom-model", dimensions=512
And:   new provider returns same ModelID() but Dimensions()=768
When:  ds.Migrate(ctx) is called
Then:  returns ErrVectorDimensionMismatch with old and new dimension in message
```

---

### Group 8: Concurrent Safety (Integration)

#### T8.1 — Concurrent VectorSearch calls on same schema are safe
```
Given: 100 FAQ records in Postgres
When:  50 goroutines call VectorSearch simultaneously with different query strings
Then:  all calls return valid results
And:   no data races (run with -race flag)
And:   no deadlocks (completes within 10 second timeout)
```

#### T8.2 — Concurrent Set() and VectorSearch() on same schema are safe
```
Given: ongoing Set() calls inserting new FAQ records
When:  VectorSearch() is called concurrently
Then:  no data races, no panics
```

#### T8.3 — Concurrent ReEmbed() calls return ErrReEmbedAlreadyRunning for second caller
```
Given: ReEmbed() is in progress on schema "faqs"
When:  a second goroutine calls ReEmbed(ctx, "faqs", "question")
Then:  second call returns ErrReEmbedAlreadyRunning immediately
And:   first call continues and completes successfully
```

---

### Group 9: EmbeddingProvider — OllamaProvider (Integration — requires local Ollama)

Tests in `embedding_test.go`. Skip automatically if `OLLAMA_TEST_URL` env var is not set.

#### T9.1 — NewOllamaProvider connects and reports correct ModelID
```
Given: NewOllamaProvider(ollamaURL, "nomic-embed-text")
When:  provider.ModelID() is called
Then:  returns "nomic-embed-text"
```

#### T9.2 — OllamaProvider.Dimensions() returns correct value for model
```
Given: NewOllamaProvider(ollamaURL, "nomic-embed-text")
When:  provider.Dimensions() is called
Then:  returns 768 (auto-detected via single Embed call on first invocation, then cached)
```

#### T9.3 — OllamaProvider.Embed() returns vector of correct dimension
```
Given: NewOllamaProvider(ollamaURL, "nomic-embed-text")
When:  provider.Embed(ctx, "What are your business hours?") is called
Then:  returns pgvector.Vector of length 768
And:   no element is NaN or Inf
```

#### T9.4 — OllamaProvider.Embed() returns different vectors for different inputs
```
Given: NewOllamaProvider(ollamaURL, "nomic-embed-text")
When:  Embed("What are your business hours?") and Embed("How do I reset my password?") are called
Then:  the two vectors are not equal
And:   cosine similarity between them is < 0.99 (they are semantically distinct)
```

#### T9.5 — OllamaProvider.Embed() returns similar vectors for similar inputs
```
Given: NewOllamaProvider(ollamaURL, "nomic-embed-text")
When:  Embed("What time do you open?") and Embed("What are your opening hours?") are called
Then:  cosine similarity between the two vectors is > 0.85 (semantically close)
```

#### T9.6 — OllamaProvider returns error when Ollama is unreachable
```
Given: NewOllamaProvider("http://localhost:19999", "nomic-embed-text") (bad port)
When:  provider.Embed(ctx, "hello") is called
Then:  returns non-nil error with network context
And:   error message includes the URL attempted
```

#### T9.7 — OllamaProvider caches Dimensions() after first resolution
```
Given: NewOllamaProvider connecting to a mock HTTP server
When:  provider.Dimensions() is called 100 times
Then:  the mock server receives exactly 1 HTTP request (dimension is cached after first call)
```

---

### Group 10: EmbeddingProvider — ReEmbed Migration (Integration)

Tests in `embedding_test.go`.

#### T10.1 — ReEmbed updates all vector fields with new provider
```
Given: 20 FAQ records stored with embeddings from provider A (dim=768)
And:   Config.EmbeddingProvider updated to provider B (dim=1536, different ModelID)
When:  ds.ReEmbed(ctx, "faqs", "question") is called
Then:  all 20 records have updated embeddings of dimension 1536 in Postgres
And:   strata_schema_meta updated with new model ID and dimension
And:   new vector index rebuilt at correct dimension
```

#### T10.2 — ReEmbed is resumable after interruption
```
Given: 100 FAQ records
And:   ReEmbed starts but is cancelled by context after processing 40 records
When:  ds.ReEmbed(ctx, "faqs", "question") is called again with fresh context
Then:  resumes from record 41 (skips already-migrated records)
And:   completes successfully
And:   all 100 records have updated embeddings
```

#### T10.3 — ReEmbed updates strata_schema_meta.reembed_progress during migration
```
Given: 50 FAQ records
When:  ReEmbed is running
Then:  strata_schema_meta.reembed_progress increases monotonically during the run
And:   reembed_status = 'running' while in progress
And:   reembed_status = 'idle' and reembed_progress = 50 on completion
```

#### T10.4 — ReEmbed fails cleanly if text field does not exist on model
```
Given: FAQ schema with no field named "body"
When:  ds.ReEmbed(ctx, "faqs", "body") is called
Then:  returns ErrReEmbedTextFieldMissing immediately
And:   no records are modified
And:   reembed_status remains 'idle'
```

#### T10.5 — ReEmbed with provider error on a record logs and continues
```
Given: 10 FAQ records
And:   mock provider returns error for records 3 and 7
When:  ds.ReEmbed(ctx, "faqs", "question") is called
Then:  records 1,2,4,5,6,8,9,10 are successfully re-embedded
And:   records 3 and 7 retain their original embeddings
And:   errors for records 3 and 7 are logged with record IDs
And:   ReEmbed returns a summary error indicating 2 partial failures
```

#### T10.6 — After successful ReEmbed, VectorSearch uses new embeddings
```
Given: FAQ records re-embedded from model A to model B
When:  ds.VectorSearch(ctx, "faqs", "query", 5, nil)
Then:  returns results (provider B is used for query embedding)
And:   no dimension mismatch errors
```

---

## Implementation Guidance for AI Agent

Work strictly in this order. Do not write implementation code for a group until all tests in the prior group are written and confirmed failing.

### Phase 1 — Foundation
1. Add `github.com/pgvector/pgvector-go` to `go.mod`
2. Define `EmbeddingProvider` interface in `embedding.go` (interface only, no implementations yet)
3. Add `EmbeddingProvider` field to `Config` in `config.go`
4. Write all Group 1 tests → confirm they fail → implement tag parsing in `tags.go`

### Phase 2 — Provider Implementations
5. Write all Group 9 tests → confirm they fail (skip integration tests if no Ollama)
6. Implement `OllamaProvider` in `embedding.go` — HTTP client, dimension caching
7. Implement `OpenAIProvider` in `embedding.go` — known model dimension table + fallback
8. Implement `MockProvider` in `embedding_test.go` — deterministic vectors for unit tests
9. Confirm Group 9 unit tests pass; integration tests pass if Ollama available

### Phase 3 — Schema Metadata + Migration
10. Write all Group 7 tests → confirm they fail
11. Create `strata_schema_meta` DDL in `schema_meta.go`
12. Implement meta read/write helpers in `schema_meta.go`
13. Update `migrate.go`: extension check, dimension sourced from provider, meta comparison logic
14. Implement vector column DDL and IVFFlat/HNSW index DDL emission in `migrate.go`
15. Confirm Groups 2 and 7 pass

### Phase 4 — VectorSearch
16. Write all Group 3 tests → confirm they fail → implement input validation in `vector.go`
17. Write all Group 4 tests → confirm they fail
18. Implement `VectorSearch` in `vector.go` — calls provider.Embed, runs pgvector query
19. Confirm Groups 3 and 4 pass

### Phase 5 — Cache Integration
20. Write all Group 5 tests → confirm they fail
21. Implement L2/L1 warming in `VectorSearch` return path
22. Confirm Group 5 passes

### Phase 6 — Set/Get Integration
23. Write all Group 6 tests → confirm they fail
24. Update codec/router to handle `pgvector.Vector` type (omit from L1/L2 serialization)
25. Confirm Group 6 passes

### Phase 7 — ReEmbed Migration
26. Write all Group 10 tests → confirm they fail
27. Implement `ReEmbed` in `vector.go` — batched walk, progress tracking, index rebuild
28. Confirm Group 10 passes

### Phase 8 — Concurrency
29. Write Group 8 tests → run with `-race` → confirm no races before any changes
30. Address any race conditions found
31. Confirm all groups pass under `-race`

---

## SKILL.md Update Requirements

Append a new `## Vector Search (L3 Semantic Layer)` section to `SKILL.md` covering:

- Dependency and Postgres extension requirement
- `EmbeddingProvider` interface and when to use each built-in implementation
- `NewOllamaProvider` and `NewOpenAIProvider` constructor examples
- `Config.EmbeddingProvider` field usage
- `strata:"vector"` tag behaviour and `omit_cache` implication
- Model definition example with `pgvector.Vector` field (no dimension number needed)
- Index definition examples for IVFFlat and HNSW
- `VectorSearch` method signature (string query, not raw vector)
- `SimilarityResult` struct
- `ReEmbed` method signature and when to call it
- Cache warming behaviour
- `strata_schema_meta` table (internal — do not query directly)
- New error variables
- Anti-patterns table:

| ❌ Don't | ✅ Do instead |
|----------|--------------|
| Hardcode dimension numbers in model structs | Let provider.Dimensions() set it automatically |
| Use `strata:"vector"` on a non-`pgvector.Vector` field | Field type must be `pgvector.Vector` |
| Expect embedding in L1/L2 cache | Vector fields are L3-only by design |
| Call VectorSearch before ds.Migrate() | Always Migrate() on startup |
| Change embedding models without calling ReEmbed | Migrate() returns ErrEmbeddingModelChanged — call ReEmbed to migrate |
| Use VectorSearch without pgvector extension installed | Run `CREATE EXTENSION IF NOT EXISTS vector;` first |
| Pass raw vectors to VectorSearch | Pass plain query strings — Strata calls the provider internally |
| Use IVFFlat with Lists > sqrt(row_count) | Lists ≈ sqrt(row_count) is the recommended heuristic |

---

## Test Helper Utilities

### `vector_test.go`

```go
// mustMakeVec returns a pgvector.Vector of the given dimension with all values set to val
func mustMakeVec(dim int, val float32) pgvector.Vector

// cosineSimilarity computes expected similarity for test assertions
func cosineSimilarity(a, b pgvector.Vector) float64

// requireVectorExtension skips the test if pgvector is not installed
func requireVectorExtension(t *testing.T, db *pgxpool.Pool)
```

### `embedding_test.go`

```go
// MockProvider — deterministic embedding provider for unit tests
// Returns a vector where element[0] = hash(text) % 1.0, all others = 0.1
// Configurable to return errors for specific input strings
type MockProvider struct {
    dim       int
    errors    map[string]error  // input → error to return
    callCount int64             // atomic — for verifying call counts
}

func NewMockProvider(dim int) *MockProvider
func (m *MockProvider) SetError(input string, err error)
func (m *MockProvider) CallCount() int

// requireOllama skips the test if OLLAMA_TEST_URL env var is not set
func requireOllama(t *testing.T) string
```

---

## Acceptance Criteria

The implementation is complete when:

- [ ] `go test ./... -race` passes with zero failures and zero races
- [ ] `go vet ./...` reports no issues
- [ ] `golint ./...` reports no new issues
- [ ] All 10 test groups pass (Groups 9-10 integration skipped gracefully when Ollama unavailable)
- [ ] `SKILL.md` vector + provider section is complete and accurate
- [ ] `go doc github.com/AndrewDonelson/strata VectorSearch` renders correct documentation
- [ ] `go doc github.com/AndrewDonelson/strata EmbeddingProvider` renders correct documentation
- [ ] `ds.Migrate()` is idempotent (safe to run 10× in a row against same DB)
- [ ] Changing provider model triggers `ErrEmbeddingModelChanged` — never silent data corruption
- [ ] `ds.ReEmbed()` is resumable after interruption with no duplicate work
- [ ] No existing tests broken
- [ ] Application code contains zero hardcoded dimension numbers

---

*Document version: 2.0 — updated with EmbeddingProvider interface, OllamaProvider, OpenAIProvider, schema metadata persistence, ReEmbed migration, and Groups 9-10.*
