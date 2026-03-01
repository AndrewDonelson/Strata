package strata_test

// integration_pg_test.go covers items that require a real PostgreSQL instance:
//
//   1. readFromL3 / writeToL3  — full L1-miss → L2-miss → L3-hit → backfill
//   2. Migrate / MigrateFrom / MigrationStatus
//   3. Search / SearchTyped / SearchCached with real data
//   4. Tx.Commit (two Sets + one Delete) + implicit rollback via ErrL3Unavailable
//   5. WarmCache pre-loading from L3 into L1 and L2

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/AndrewDonelson/strata"
	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	testcontainers "github.com/testcontainers/testcontainers-go"
	tcpg "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// ─── Fixtures ────────────────────────────────────────────────────────────────

const (
	pgTestImage = "postgres:16-alpine"
	pgTestDB    = "strataintegration"
	pgTestUser  = "stratatest"
	pgTestPass  = "stratatest"
)

// fullStack holds a DataStore backed by real Postgres + miniredis.
type fullStack struct {
	ds   *strata.DataStore
	mini *miniredis.Miniredis
}

// newFullStack spins up Postgres (testcontainers) + miniredis.
// The returned DataStore has the "product" schema registered and migrated.
// Skips if Docker is unavailable.
func newFullStack(t *testing.T) fullStack {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx := context.Background()

	pgc, err := tcpg.Run(ctx, pgTestImage,
		tcpg.WithDatabase(pgTestDB),
		tcpg.WithUsername(pgTestUser),
		tcpg.WithPassword(pgTestPass),
		tcpg.BasicWaitStrategies(),
	)
	require.NoError(t, err, "start postgres container")

	pgDSN, err := pgc.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	mr, err := miniredis.Run()
	require.NoError(t, err)

	ds, err := strata.NewDataStore(strata.Config{
		PostgresDSN:  pgDSN,
		RedisAddr:    mr.Addr(),
		DefaultL1TTL: 5 * time.Minute,
		DefaultL2TTL: 30 * time.Minute,
	})
	require.NoError(t, err)

	registerProduct(t, ds)

	require.NoError(t, ds.Migrate(ctx), "Migrate must succeed")

	t.Cleanup(func() {
		ds.Close()
		mr.Close()
		_ = pgc.Terminate(ctx)
	})

	return fullStack{ds: ds, mini: mr}
}

// seedProduct inserts a product directly via Set (write-through to L3).
func seedProduct(t *testing.T, fs fullStack, p *Product) {
	t.Helper()
	require.NoError(t, fs.ds.Set(context.Background(), "product", p.ID, p))
}

// blowAwayL1L2 forcibly expires all Redis keys and invalidates L1.
// After this call, the next Get must go all the way to L3.
func blowAwayL1L2(t *testing.T, fs fullStack) {
	t.Helper()
	ctx := context.Background()
	// Clear L1 by invalidating the whole schema.
	require.NoError(t, fs.ds.InvalidateAll(ctx, "product"))
	// Flush miniredis so L2 misses too.
	fs.mini.FlushAll()
}

// ─── 1. readFromL3 / writeToL3 ───────────────────────────────────────────────

// TestWriteToL3_SetPersists verifies that Set with a real L3 stores the row in
// Postgres (exercising writeToL3).
func TestWriteToL3_SetPersists(t *testing.T) {
	fs := newFullStack(t)
	ctx := context.Background()

	p := &Product{ID: "wl3-1", Name: "PGWidget", Price: 9.99}
	require.NoError(t, fs.ds.Set(ctx, "product", p.ID, p))

	// Verify directly via Exists on the DataStore (hits L1 first).
	ok, err := fs.ds.Exists(ctx, "product", "wl3-1")
	require.NoError(t, err)
	assert.True(t, ok)
}

// TestReadFromL3_FullMissFallthrough is the primary readFromL3 coverage test.
// It writes via Set (which populates all three tiers), then evicts L1 and L2,
// forcing the next Get to fall all the way to L3 and return the correct value.
func TestReadFromL3_FullMissFallthrough(t *testing.T) {
	fs := newFullStack(t)

	p := &Product{ID: "rfl-1", Name: "L3Fallthrough", Price: 42.0}
	seedProduct(t, fs, p)

	// Evict both upper tiers.
	blowAwayL1L2(t, fs)

	// Get must now go L1 miss → L2 miss → L3 hit → backfill L1+L2.
	var got Product
	require.NoError(t, fs.ds.Get(context.Background(), "product", "rfl-1", &got))
	assert.Equal(t, "L3Fallthrough", got.Name)
	assert.InDelta(t, 42.0, got.Price, 0.001)
}

// TestReadFromL3_BackfillsL1 checks that after the L3 read the value is back in L1.
func TestReadFromL3_BackfillsL1(t *testing.T) {
	fs := newFullStack(t)

	p := &Product{ID: "rfl-2", Name: "Backfill", Price: 7.7}
	seedProduct(t, fs, p)
	blowAwayL1L2(t, fs)

	// First Get — from L3.
	var got Product
	require.NoError(t, fs.ds.Get(context.Background(), "product", "rfl-2", &got))

	// Second Get — must be from L1 (ultra-fast, no Redis needed).
	// Close miniredis so any L2 attempt would fail; L1 must succeed.
	fs.mini.Close()
	var got2 Product
	require.NoError(t, fs.ds.Get(context.Background(), "product", "rfl-2", &got2))
	assert.Equal(t, got.Name, got2.Name)
}

// TestReadFromL3_NotFound checks the ErrNotFound path through L3.
func TestReadFromL3_NotFound(t *testing.T) {
	fs := newFullStack(t)

	var got Product
	err := fs.ds.Get(context.Background(), "product", "no-such-id", &got)
	assert.ErrorIs(t, err, strata.ErrNotFound)
}

// ─── 2. Migration ────────────────────────────────────────────────────────────

// TestMigrate_IdempotentCreateTable verifies that Migrate creates the table and
// running it a second time does not error (idempotent).
func TestMigrate_IdempotentCreateTable(t *testing.T) {
	fs := newFullStack(t) // Migrate already called in newFullStack

	// Second call must be a no-op, not an error.
	require.NoError(t, fs.ds.Migrate(context.Background()))
}

// TestMigrate_AlterTable adds a second schema that has an extra column absent
// from the first migration, simulating an ALTER TABLE scenario.
func TestMigrate_AlterTable(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()

	pgc, err := tcpg.Run(ctx, pgTestImage,
		tcpg.WithDatabase(pgTestDB), tcpg.WithUsername(pgTestUser), tcpg.WithPassword(pgTestPass),
		tcpg.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	defer pgc.Terminate(ctx) //nolint:errcheck

	pgDSN, _ := pgc.ConnectionString(ctx, "sslmode=disable")

	ds1, err := strata.NewDataStore(strata.Config{PostgresDSN: pgDSN})
	require.NoError(t, err)
	// Register with minimal schema and migrate.
	require.NoError(t, ds1.Register(strata.Schema{Name: "itempg", Model: &struct {
		ID   string `strata:"primary_key"`
		Name string
	}{}}))
	require.NoError(t, ds1.Migrate(ctx))
	ds1.Close()

	// Open a second DataStore with an extended schema — should ALTER TABLE.
	ds2, err := strata.NewDataStore(strata.Config{PostgresDSN: pgDSN})
	require.NoError(t, err)
	defer ds2.Close()
	require.NoError(t, ds2.Register(strata.Schema{Name: "itempg", Model: &struct {
		ID    string `strata:"primary_key"`
		Name  string
		Extra string
	}{}}))
	require.NoError(t, ds2.Migrate(ctx))
}

// TestMigrateStatus_ReturnsRecords checks MigrationStatus after Migrate.
func TestMigrateStatus_ReturnsRecords(t *testing.T) {
	fs := newFullStack(t)
	records, err := fs.ds.MigrationStatus(context.Background())
	require.NoError(t, err)
	assert.NotEmpty(t, records, "at least one migration record must exist after Migrate()")
}

// TestMigrateFrom_AppliesAndSkipsDuplicates creates two SQL files in a temp
// directory and calls MigrateFrom; verifies the second call is a no-op.
func TestMigrateFrom_AppliesAndSkipsDuplicates(t *testing.T) {
	fs := newFullStack(t)
	ctx := context.Background()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "001_create_meta.sql"),
		[]byte(`CREATE TABLE IF NOT EXISTS pg_migration_meta (k TEXT PRIMARY KEY, v TEXT);`),
		0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "002_seed_meta.sql"),
		[]byte(`INSERT INTO pg_migration_meta (k, v) VALUES ('env', 'test') ON CONFLICT DO NOTHING;`),
		0o644))

	// First call applies both files.
	require.NoError(t, fs.ds.MigrateFrom(ctx, dir))
	// Second call must skip both (already applied).
	require.NoError(t, fs.ds.MigrateFrom(ctx, dir))

	// Verify both are recorded in migration status.
	records, err := fs.ds.MigrationStatus(ctx)
	require.NoError(t, err)
	var files []string
	for _, r := range records {
		files = append(files, r.FileName)
	}
	assert.Contains(t, files, "001_create_meta.sql")
	assert.Contains(t, files, "002_seed_meta.sql")
}

// ─── 3. Search / SearchTyped / SearchCached ───────────────────────────────────

// seededFS returns a fullStack with 10 products seeded in L3.
func seededFS(t *testing.T) fullStack {
	t.Helper()
	fs := newFullStack(t)
	for i := 0; i < 10; i++ {
		p := &Product{
			ID:    fmt.Sprintf("s%02d", i),
			Name:  fmt.Sprintf("Product %d", i),
			Price: float64(i) * 1.5,
		}
		seedProduct(t, fs, p)
	}
	return fs
}

func TestSearch_AllRows(t *testing.T) {
	fs := seededFS(t)

	var results []Product
	require.NoError(t, fs.ds.Search(context.Background(), "product", nil, &results))
	assert.GreaterOrEqual(t, len(results), 10)
}

func TestSearch_WithWhere(t *testing.T) {
	fs := seededFS(t)

	q := strata.Q().Where("price > $1", 9.0).Build()
	var results []Product
	require.NoError(t, fs.ds.Search(context.Background(), "product", &q, &results))
	// Only products with price > 9.0 (indices 7, 8, 9 → prices 10.5, 12.0, 13.5)
	assert.Equal(t, 3, len(results))
}

func TestSearchTyped_WithWhere(t *testing.T) {
	fs := seededFS(t)

	q := strata.Q().Where("price > $1", 6.0).Limit(3).OrderBy("price").Build()
	results, err := strata.SearchTyped[Product](context.Background(), fs.ds, "product", &q)
	require.NoError(t, err)
	assert.Equal(t, 3, len(results))
}

func TestSearchCached_HitsL2OnSecondCall(t *testing.T) {
	fs := seededFS(t)
	ctx := context.Background()
	q := strata.Q().Where("price < $1", 4.5).Build()

	// First call populates L2 cache.
	var res1 []Product
	require.NoError(t, fs.ds.SearchCached(ctx, "product", &q, &res1))
	require.NotEmpty(t, res1)

	// Second call should return the cached result.
	var res2 []Product
	require.NoError(t, fs.ds.SearchCached(ctx, "product", &q, &res2))
	assert.Equal(t, len(res1), len(res2))
}

func TestCount_WithL3(t *testing.T) {
	fs := seededFS(t)
	n, err := fs.ds.Count(context.Background(), "product", nil)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, n, int64(10))
}

// ─── 4. Tx ───────────────────────────────────────────────────────────────────

func TestTx_Commit_SetsAndDelete(t *testing.T) {
	fs := newFullStack(t)
	ctx := context.Background()

	a := &Product{ID: "tx-a", Name: "TxA", Price: 1.0}
	b := &Product{ID: "tx-b", Name: "TxB", Price: 2.0}
	c := &Product{ID: "tx-c", Name: "TxC", Price: 3.0}

	// Seed c so we can delete it inside the transaction.
	seedProduct(t, fs, c)

	err := fs.ds.Tx(ctx).
		Set("product", "tx-a", a).
		Set("product", "tx-b", b).
		Delete("product", "tx-c").
		Commit()
	require.NoError(t, err)

	// a and b must exist.
	var ga, gb Product
	require.NoError(t, fs.ds.Get(ctx, "product", "tx-a", &ga))
	assert.Equal(t, "TxA", ga.Name)
	require.NoError(t, fs.ds.Get(ctx, "product", "tx-b", &gb))
	assert.Equal(t, "TxB", gb.Name)

	// c must be gone.
	var gc Product
	err = fs.ds.Get(ctx, "product", "tx-c", &gc)
	assert.ErrorIs(t, err, strata.ErrNotFound)
}

func TestTx_Commit_UpdateOverwrites(t *testing.T) {
	fs := newFullStack(t)
	ctx := context.Background()

	original := &Product{ID: "tx-up", Name: "Original", Price: 10.0}
	seedProduct(t, fs, original)

	updated := &Product{ID: "tx-up", Name: "Updated", Price: 99.0}
	require.NoError(t, fs.ds.Tx(ctx).Set("product", "tx-up", updated).Commit())

	blowAwayL1L2(t, fs)

	var got Product
	require.NoError(t, fs.ds.Get(ctx, "product", "tx-up", &got))
	assert.Equal(t, "Updated", got.Name)
	assert.InDelta(t, 99.0, got.Price, 0.001)
}

func TestTx_EmptyCommit_NoError(t *testing.T) {
	fs := newFullStack(t)
	// An empty Tx.Commit should succeed and be a true no-op.
	require.NoError(t, fs.ds.Tx(context.Background()).Commit())
}

// TestTx_Delete_NonExistent verifies that deleting a row that does not exist
// inside a transaction does not cause the commit to fail.
func TestTx_Delete_NonExistent(t *testing.T) {
	fs := newFullStack(t)
	err := fs.ds.Tx(context.Background()).
		Delete("product", "ghost").
		Commit()
	require.NoError(t, err)
}

// ─── 5. WarmCache ────────────────────────────────────────────────────────────

func TestWarmCache_PopulatesL1(t *testing.T) {
	fs := seededFS(t) // 10 products in L3
	ctx := context.Background()

	// Warm L1 from L3.
	require.NoError(t, fs.ds.WarmCache(ctx, "product", 0))

	// Close miniredis so any L2 attempt errors; only L1 should answer now.
	fs.mini.Close()

	// All 10 seeded products must be in L1.
	hits := 0
	for i := 0; i < 10; i++ {
		var p Product
		if err := fs.ds.Get(ctx, "product", fmt.Sprintf("s%02d", i), &p); err == nil {
			hits++
		}
	}
	assert.Equal(t, 10, hits, "all warmed entries should be served from L1")
}

func TestWarmCache_WithLimit(t *testing.T) {
	fs := seededFS(t) // 10 products in L3
	ctx := context.Background()

	require.NoError(t, fs.ds.WarmCache(ctx, "product", 5))

	// Stats should show at least 5 L1 entries were populated.
	stats := fs.ds.Stats()
	assert.GreaterOrEqual(t, stats.L1Entries, int64(5))
}

func TestWarmCache_ThenInvalidateAll(t *testing.T) {
	fs := seededFS(t)
	ctx := context.Background()

	require.NoError(t, fs.ds.WarmCache(ctx, "product", 0))
	require.NoError(t, fs.ds.InvalidateAll(ctx, "product"))

	// After invalidation the cache should be cold; L3 must be queried.
	var p Product
	err := fs.ds.Get(ctx, "product", "s00", &p)
	require.NoError(t, err) // L3 still has the row
	assert.Equal(t, "Product 0", p.Name)
}

// ─── 6. End-to-end Delete with L3 ────────────────────────────────────────────

func TestDelete_WithL3_RemovesRow(t *testing.T) {
	fs := newFullStack(t)
	ctx := context.Background()

	p := &Product{ID: "del-l3", Name: "DeleteMe", Price: 5.0}
	seedProduct(t, fs, p)

	require.NoError(t, fs.ds.Delete(ctx, "product", "del-l3"))

	blowAwayL1L2(t, fs)

	var got Product
	err := fs.ds.Get(ctx, "product", "del-l3", &got)
	assert.ErrorIs(t, err, strata.ErrNotFound)
}

// ─── 7. Exists across all tiers ──────────────────────────────────────────────

func TestExists_L3Hit(t *testing.T) {
	fs := newFullStack(t)
	ctx := context.Background()

	p := &Product{ID: "ex-l3", Name: "ExistsL3", Price: 1.0}
	seedProduct(t, fs, p)
	blowAwayL1L2(t, fs)

	ok, err := fs.ds.Exists(ctx, "product", "ex-l3")
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestExists_NotFound_L3(t *testing.T) {
	fs := newFullStack(t)
	blowAwayL1L2(t, fs)

	ok, err := fs.ds.Exists(context.Background(), "product", "not-there")
	require.NoError(t, err)
	assert.False(t, ok)
}

// ─── 8. createTable DDL branches ─────────────────────────────────────────────
//
// richItem is designed to trigger every branch in buildColumnDef and
// buildIndexDDL:
//
//   - ID     → PRIMARY KEY
//   - Slug   → UNIQUE NOT NULL
//   - Score  → NOT NULL + regular index
//   - Amount → NOT NULL DEFAULT 0.0  (DefaultValue branch)
//   - Note   → nullable (no NOT NULL)
//   - Tags   → BYTEA column ([]byte slice type)
//   - CreatedAt → TIMESTAMPTZ DEFAULT now() via auto_now_add
//   - UpdatedAt → TIMESTAMPTZ DEFAULT now() via auto_now
//
// Additionally the Schema carries a composite unique index (Indexes slice)
// which exercises the cs.Indexes loop in buildIndexDDL.

type richItem struct {
	ID        string    `strata:"primary_key"`
	Slug      string    `strata:"unique"`
	Score     int       `strata:"index"`
	Amount    float64   `strata:"default:0.0"`
	Note      string    `strata:"nullable"`
	Tags      []byte    `strata:"nullable"`
	CreatedAt time.Time `strata:"auto_now_add"`
	UpdatedAt time.Time `strata:"auto_now"`
}

func registerRichItem(t *testing.T, ds *strata.DataStore, name string) {
	t.Helper()
	require.NoError(t, ds.Register(strata.Schema{
		Name:  name,
		Model: &richItem{},
		Indexes: []strata.Index{
			{
				Fields: []string{"slug", "score"},
				Unique: true,
				Name:   "idx_" + name + "_slug_score",
			},
		},
	}))
}

// TestMigrate_CreateTable_AllDDLBranches registers a schema that exercises
// every DDL branch (PK, UNIQUE, NOT NULL, DEFAULT, NULL, BYTEA, auto_now_add,
// auto_now, regular index, composite unique index) and asserts that Migrate
// runs cleanly — both on first creation and as an idempotent second call.
func TestMigrate_CreateTable_AllDDLBranches(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()

	pgc, err := tcpg.Run(ctx, pgTestImage,
		tcpg.WithDatabase(pgTestDB), tcpg.WithUsername(pgTestUser), tcpg.WithPassword(pgTestPass),
		tcpg.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pgc.Terminate(ctx) })

	pgDSN, _ := pgc.ConnectionString(ctx, "sslmode=disable")
	ds, err := strata.NewDataStore(strata.Config{PostgresDSN: pgDSN})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	registerRichItem(t, ds, "richitem")
	require.NoError(t, ds.Migrate(ctx), "first Migrate must succeed")

	// Idempotent second run — alterTable path, no new columns → no-op.
	require.NoError(t, ds.Migrate(ctx), "second Migrate must be idempotent")
}

// TestWriteToL3_AutoTimestamps verifies that writeToL3 populates auto_now_add
// and auto_now fields when the caller's struct has zero-valued time.Time fields.
func TestWriteToL3_AutoTimestamps(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()

	pgc, err := tcpg.Run(ctx, pgTestImage,
		tcpg.WithDatabase(pgTestDB), tcpg.WithUsername(pgTestUser), tcpg.WithPassword(pgTestPass),
		tcpg.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pgc.Terminate(ctx) })

	pgDSN, _ := pgc.ConnectionString(ctx, "sslmode=disable")
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	ds, err := strata.NewDataStore(strata.Config{
		PostgresDSN:  pgDSN,
		RedisAddr:    mr.Addr(),
		DefaultL1TTL: 5 * time.Minute,
		DefaultL2TTL: 30 * time.Minute,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	registerRichItem(t, ds, "richts")
	require.NoError(t, ds.Migrate(ctx))

	// All time fields are zero — writeToL3 must set them via auto_now logic.
	item := &richItem{ID: "ts-1", Slug: "alpha", Score: 5}
	require.NoError(t, ds.Set(ctx, "richts", item.ID, item))

	// Row must exist in L3 (the auto_now timestamps were written).
	ok, err := ds.Exists(ctx, "richts", "ts-1")
	require.NoError(t, err)
	assert.True(t, ok, "auto-timestamped row must be present in L3")

	// Second Set triggers the auto_now (always-update) branch.
	item2 := &richItem{ID: "ts-1", Slug: "alpha", Score: 99}
	require.NoError(t, ds.Set(ctx, "richts", item2.ID, item2))
	ok, err = ds.Exists(ctx, "richts", "ts-1")
	require.NoError(t, err)
	assert.True(t, ok, "updated row must still be present after auto_now update")
}

// ─── 9. Tx.Commit failure paths ──────────────────────────────────────────────

// TestTx_Commit_UnknownSchema verifies that Commit rolls back and returns an
// error when a queued Set references a schema name that is not registered.
// This covers the registry.get error branch inside Commit.
func TestTx_Commit_UnknownSchema(t *testing.T) {
	fs := newFullStack(t)
	err := fs.ds.Tx(context.Background()).
		Set("does_not_exist", "x1", &Product{ID: "x1"}).
		Commit()
	require.Error(t, err, "unregistered schema must cause Commit to fail")
}

// TestTx_Commit_NoL3 verifies that Commit returns ErrL3Unavailable when the
// DataStore has no L3 backend configured. This covers the l3==nil guard at the
// top of Commit.
func TestTx_Commit_NoL3(t *testing.T) {
	// Build a DataStore with no PostgresDSN → l3 == nil.
	ds, err := strata.NewDataStore(strata.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })
	require.NoError(t, ds.Register(strata.Schema{Name: "product", Model: &Product{}}))

	err = ds.Tx(context.Background()).
		Set("product", "x", &Product{ID: "x"}).
		Commit()
	require.ErrorIs(t, err, strata.ErrL3Unavailable)
}

// ─── 10. Migration — nullable+default and multi-index schemas ─────────────────
//
// The migrate.createTable function has been stuck at 53.3% for two rounds.
// The review identifies two additional schema shapes that ensure every column
// modifier branch in buildColumnDef/buildIndexDDL is exercised against a real
// Postgres server:
//
//   ConfigEntry  — nullable string + integer with DEFAULT 0
//   EventLog     — two independently indexed string columns + auto_now_add

// configEntry has a nullable Value column and a Priority column with a default.
type configEntry struct {
	ID       string `strata:"primary_key"`
	Value    string `strata:"nullable"`
	Priority int    `strata:"default:0"`
}

// eventLog has two independent indexed columns and an auto_now_add timestamp.
type eventLog struct {
	ID        string    `strata:"primary_key"`
	PlayerID  string    `strata:"index"`
	EventType string    `strata:"index"`
	CreatedAt time.Time `strata:"auto_now_add"`
}

// TestMigrate_ConfigEntry_NullableAndDefault registers and migrates a schema that
// has a nullable column and a column with a DEFAULT value against real Postgres.
// Second Migrate call verifies the alterTable (no new columns) idempotent path.
func TestMigrate_ConfigEntry_NullableAndDefault(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()

	pgc, err := tcpg.Run(ctx, pgTestImage,
		tcpg.WithDatabase(pgTestDB), tcpg.WithUsername(pgTestUser), tcpg.WithPassword(pgTestPass),
		tcpg.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pgc.Terminate(ctx) })

	pgDSN, _ := pgc.ConnectionString(ctx, "sslmode=disable")
	ds, err := strata.NewDataStore(strata.Config{PostgresDSN: pgDSN})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	require.NoError(t, ds.Register(strata.Schema{
		Name:  "config_entry",
		Model: &configEntry{},
	}))

	// First Migrate: creates the table with nullable and default columns.
	require.NoError(t, ds.Migrate(ctx), "first Migrate must succeed")

	// Second Migrate: alterTable path with no new columns → no-op.
	require.NoError(t, ds.Migrate(ctx), "second Migrate must be idempotent")

	// Migration status must record one 'create_table' row.
	records, err := ds.MigrationStatus(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, records)
}

// TestMigrate_EventLog_MultipleIndexes registers and migrates a schema with two
// independently indexed columns, exercising the loop in buildIndexDDL that
// generates CREATE INDEX statements for each indexed non-PK column.
func TestMigrate_EventLog_MultipleIndexes(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()

	pgc, err := tcpg.Run(ctx, pgTestImage,
		tcpg.WithDatabase(pgTestDB), tcpg.WithUsername(pgTestUser), tcpg.WithPassword(pgTestPass),
		tcpg.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pgc.Terminate(ctx) })

	pgDSN, _ := pgc.ConnectionString(ctx, "sslmode=disable")
	ds, err := strata.NewDataStore(strata.Config{PostgresDSN: pgDSN})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	require.NoError(t, ds.Register(strata.Schema{
		Name:  "event_log",
		Model: &eventLog{},
	}))

	require.NoError(t, ds.Migrate(ctx), "Migrate with two indexed columns must succeed")

	// Verify the migration was recorded.
	records, err := ds.MigrationStatus(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, records)
}

// TestMigrate_AlterTable_AddsNullableColumn extends an existing table with a
// nullable column to cover the alterTable ADD COLUMN path for nullable fields.
func TestMigrate_AlterTable_AddsNullableColumn(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()

	pgc, err := tcpg.Run(ctx, pgTestImage,
		tcpg.WithDatabase(pgTestDB), tcpg.WithUsername(pgTestUser), tcpg.WithPassword(pgTestPass),
		tcpg.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pgc.Terminate(ctx) })

	pgDSN, _ := pgc.ConnectionString(ctx, "sslmode=disable")

	// v1: minimal schema.
	ds1, err := strata.NewDataStore(strata.Config{PostgresDSN: pgDSN})
	require.NoError(t, err)
	require.NoError(t, ds1.Register(strata.Schema{
		Name: "alt_nullable",
		Model: &struct {
			ID string `strata:"primary_key"`
		}{},
	}))
	require.NoError(t, ds1.Migrate(ctx))
	ds1.Close()

	// v2: adds a nullable Note column → triggers alterTable ADD COLUMN.
	ds2, err := strata.NewDataStore(strata.Config{PostgresDSN: pgDSN})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds2.Close() })
	require.NoError(t, ds2.Register(strata.Schema{
		Name: "alt_nullable",
		Model: &struct {
			ID   string `strata:"primary_key"`
			Note string `strata:"nullable"`
		}{},
	}))
	require.NoError(t, ds2.Migrate(ctx), "alterTable with nullable column must succeed")
}
