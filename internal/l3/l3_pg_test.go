package l3_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/AndrewDonelson/strata/internal/l3"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	testcontainers "github.com/testcontainers/testcontainers-go"
	tcpg "github.com/testcontainers/testcontainers-go/modules/postgres"
)

const (
	pgImage    = "postgres:16-alpine"
	pgDatabase = "stratapgtest"
	pgUser     = "stratapguser"
	pgPassword = "stratapgpass"
)

// setupPG spins up a Postgres container and returns a connected Store.
// skips the test if Docker is not available.
func setupPG(t *testing.T) (*l3.Store, *pgxpool.Pool, func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx := context.Background()

	pgc, err := tcpg.Run(ctx, pgImage,
		tcpg.WithDatabase(pgDatabase),
		tcpg.WithUsername(pgUser),
		tcpg.WithPassword(pgPassword),
		tcpg.BasicWaitStrategies(),
	)
	require.NoError(t, err, "start postgres container")

	connStr, err := pgc.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS items (
			id    TEXT PRIMARY KEY,
			name  TEXT    NOT NULL DEFAULT '',
			value INTEGER NOT NULL DEFAULT 0
		)`)
	require.NoError(t, err, "create test table")

	store := l3.New(pool, nil)

	cleanup := func() {
		pool.Close()
		if err := pgc.Terminate(ctx); err != nil {
			t.Logf("cleanup: terminate container: %v", err)
		}
	}

	return store, pool, cleanup
}

// ── Ping ─────────────────────────────────────────────────────────────────────

func TestL3_Ping(t *testing.T) {
	store, _, cleanup := setupPG(t)
	defer cleanup()
	require.NoError(t, store.Ping(context.Background()))
}

// ── Pool ─────────────────────────────────────────────────────────────────────

func TestL3_Pool(t *testing.T) {
	store, pool, cleanup := setupPG(t)
	defer cleanup()
	assert.Same(t, pool, store.Pool(), "Pool() should return the primary pool")
}

// ── Upsert ───────────────────────────────────────────────────────────────────

func TestL3_Upsert_Insert(t *testing.T) {
	store, _, cleanup := setupPG(t)
	defer cleanup()
	ctx := context.Background()

	err := store.Upsert(ctx, "items",
		[]string{"id", "name", "value"},
		[]any{"u1", "Widget", 10},
		"id",
	)
	require.NoError(t, err)

	// Verify via raw pool
	var name string
	var val int
	err = store.Pool().QueryRow(ctx, "SELECT name, value FROM items WHERE id = $1", "u1").Scan(&name, &val)
	require.NoError(t, err)
	assert.Equal(t, "Widget", name)
	assert.Equal(t, 10, val)
}

func TestL3_Upsert_ConflictUpdate(t *testing.T) {
	store, _, cleanup := setupPG(t)
	defer cleanup()
	ctx := context.Background()

	// Insert
	require.NoError(t, store.Upsert(ctx, "items",
		[]string{"id", "name", "value"}, []any{"u2", "Original", 1}, "id"))

	// Conflict update — same id, different values
	require.NoError(t, store.Upsert(ctx, "items",
		[]string{"id", "name", "value"}, []any{"u2", "Updated", 99}, "id"))

	var name string
	var val int
	require.NoError(t, store.Pool().QueryRow(ctx, "SELECT name, value FROM items WHERE id = $1", "u2").Scan(&name, &val))
	assert.Equal(t, "Updated", name)
	assert.Equal(t, 99, val)
}

// ── DeleteByID ───────────────────────────────────────────────────────────────

func TestL3_DeleteByID(t *testing.T) {
	store, _, cleanup := setupPG(t)
	defer cleanup()
	ctx := context.Background()

	require.NoError(t, store.Upsert(ctx, "items",
		[]string{"id", "name", "value"}, []any{"d1", "ToDelete", 1}, "id"))

	require.NoError(t, store.DeleteByID(ctx, "items", "id", "d1"))

	ok, err := store.Exists(ctx, "items", "id", "d1")
	require.NoError(t, err)
	assert.False(t, ok, "deleted row should not exist")
}

func TestL3_DeleteByID_NonExistent(t *testing.T) {
	store, _, cleanup := setupPG(t)
	defer cleanup()
	// Deleting a row that doesn't exist should not error
	require.NoError(t, store.DeleteByID(context.Background(), "items", "id", "ghost"))
}

// ── Exists ───────────────────────────────────────────────────────────────────

func TestL3_Exists_Found(t *testing.T) {
	store, _, cleanup := setupPG(t)
	defer cleanup()
	ctx := context.Background()

	require.NoError(t, store.Upsert(ctx, "items",
		[]string{"id", "name", "value"}, []any{"e1", "Exist", 0}, "id"))

	ok, err := store.Exists(ctx, "items", "id", "e1")
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestL3_Exists_NotFound(t *testing.T) {
	store, _, cleanup := setupPG(t)
	defer cleanup()
	ok, err := store.Exists(context.Background(), "items", "id", "never-inserted")
	require.NoError(t, err)
	assert.False(t, ok)
}

// ── Count ────────────────────────────────────────────────────────────────────

func TestL3_Count_All(t *testing.T) {
	store, pool, cleanup := setupPG(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		require.NoError(t, store.Upsert(ctx, "items",
			[]string{"id", "name", "value"},
			[]any{fmt.Sprintf("c%d", i), "item", i}, "id"))
	}

	n, err := store.Count(ctx, "items", "", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(5), n)

	_ = pool // avoid unused
}

func TestL3_Count_WithWhere(t *testing.T) {
	store, _, cleanup := setupPG(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		require.NoError(t, store.Upsert(ctx, "items",
			[]string{"id", "name", "value"},
			[]any{fmt.Sprintf("cw%d", i), "item", i}, "id"))
	}

	n, err := store.Count(ctx, "items", "value > $1", []any{4})
	require.NoError(t, err)
	assert.Equal(t, int64(5), n) // values 5..9
}

// ── Query ────────────────────────────────────────────────────────────────────

func TestL3_Query(t *testing.T) {
	store, _, cleanup := setupPG(t)
	defer cleanup()
	ctx := context.Background()

	for _, row := range []struct {
		id, name string
		val      int
	}{
		{"q1", "Alpha", 1}, {"q2", "Beta", 2}, {"q3", "Gamma", 3},
	} {
		require.NoError(t, store.Upsert(ctx, "items",
			[]string{"id", "name", "value"}, []any{row.id, row.name, row.val}, "id"))
	}

	rows, err := store.Query(ctx, "SELECT id, name FROM items WHERE id LIKE 'q%' ORDER BY id", nil)
	require.NoError(t, err)
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id, name string
		require.NoError(t, rows.Scan(&id, &name))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []string{"q1", "q2", "q3"}, ids)
}

func TestL3_QueryPrimary(t *testing.T) {
	store, _, cleanup := setupPG(t)
	defer cleanup()
	ctx := context.Background()

	require.NoError(t, store.Upsert(ctx, "items",
		[]string{"id", "name", "value"}, []any{"qp1", "Primary", 42}, "id"))

	rows, err := store.QueryPrimary(ctx, "SELECT id FROM items WHERE id = $1", []any{"qp1"})
	require.NoError(t, err)
	defer rows.Close()

	var found bool
	for rows.Next() {
		var id string
		require.NoError(t, rows.Scan(&id))
		found = true
	}
	assert.True(t, found)
}

// ── QueryRow ─────────────────────────────────────────────────────────────────

func TestL3_QueryRow_Found(t *testing.T) {
	store, _, cleanup := setupPG(t)
	defer cleanup()
	ctx := context.Background()

	require.NoError(t, store.Upsert(ctx, "items",
		[]string{"id", "name", "value"}, []any{"qr1", "Row", 7}, "id"))

	row := store.QueryRow(ctx, "SELECT name, value FROM items WHERE id = $1", []any{"qr1"})
	var name string
	var val int
	require.NoError(t, row.Scan(&name, &val))
	assert.Equal(t, "Row", name)
	assert.Equal(t, 7, val)
}

func TestL3_QueryRow_NotFound(t *testing.T) {
	store, _, cleanup := setupPG(t)
	defer cleanup()
	row := store.QueryRow(context.Background(), "SELECT name FROM items WHERE id = $1", []any{"ghost"})
	var name string
	err := row.Scan(&name)
	assert.ErrorIs(t, err, pgx.ErrNoRows)
}

// ── Exec ─────────────────────────────────────────────────────────────────────

func TestL3_Exec_Insert(t *testing.T) {
	store, _, cleanup := setupPG(t)
	defer cleanup()
	ctx := context.Background()

	require.NoError(t, store.Exec(ctx,
		"INSERT INTO items (id, name, value) VALUES ($1, $2, $3)",
		[]any{"ex1", "Exec", 55}))

	ok, err := store.Exists(ctx, "items", "id", "ex1")
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestL3_Exec_NoArgs(t *testing.T) {
	store, _, cleanup := setupPG(t)
	defer cleanup()
	// Exec with nil args — covers the nil-args branch in Exec
	require.NoError(t, store.Exec(context.Background(),
		"CREATE TEMP TABLE exec_test (x INT)", nil))
}

// ── Transactions ─────────────────────────────────────────────────────────────

func TestL3_BeginTx_Commit(t *testing.T) {
	store, _, cleanup := setupPG(t)
	defer cleanup()
	ctx := context.Background()

	tx, err := store.BeginTx(ctx)
	require.NoError(t, err)

	_, err = tx.Exec(ctx, "INSERT INTO items (id, name, value) VALUES ($1, $2, $3)",
		"tx1", "Committed", 100)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	ok, err := store.Exists(ctx, "items", "id", "tx1")
	require.NoError(t, err)
	assert.True(t, ok, "committed row should exist")
}

func TestL3_BeginTx_Rollback(t *testing.T) {
	store, _, cleanup := setupPG(t)
	defer cleanup()
	ctx := context.Background()

	tx, err := store.BeginTx(ctx)
	require.NoError(t, err)

	_, err = tx.Exec(ctx, "INSERT INTO items (id, name, value) VALUES ($1, $2, $3)",
		"tx2", "Rolled", 99)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback(ctx))

	ok, err := store.Exists(ctx, "items", "id", "tx2")
	require.NoError(t, err)
	assert.False(t, ok, "rolled-back row must not exist")
}

// ── CopyFromRows / CopyFromSlice ──────────────────────────────────────────────

func TestL3_CopyFromSlice(t *testing.T) {
	store, _, cleanup := setupPG(t)
	defer cleanup()
	ctx := context.Background()

	rows := [][]any{
		{"cp1", "Copy1", 1},
		{"cp2", "Copy2", 2},
		{"cp3", "Copy3", 3},
	}
	src := l3.CopyFromSlice(rows)
	n, err := store.CopyFromRows(ctx, "items", []string{"id", "name", "value"}, src)
	require.NoError(t, err)
	assert.Equal(t, int64(3), n)

	cnt, _ := store.Count(ctx, "items", "id LIKE 'cp%'", nil)
	assert.Equal(t, int64(3), cnt)
}

// ── Replica (readPool fallback) ───────────────────────────────────────────────

func TestL3_ReplicaFallback(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx := context.Background()
	pgc, err := tcpg.Run(ctx, pgImage,
		tcpg.WithDatabase(pgDatabase),
		tcpg.WithUsername(pgUser),
		tcpg.WithPassword(pgPassword),
		tcpg.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	defer pgc.Terminate(ctx) //nolint:errcheck

	connStr, err := pgc.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	primary, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)
	defer primary.Close()

	// Use same pool as both primary and "replica" — tests the readPool() path
	replica, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)
	defer replica.Close()

	_, err = primary.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS replica_test (
			id TEXT PRIMARY KEY, val TEXT
		)`)
	require.NoError(t, err)

	store := l3.New(primary, replica)

	require.NoError(t, store.Upsert(ctx, "replica_test",
		[]string{"id", "val"}, []any{"r1", "hello"}, "id"))

	// Query via readPool (which is the replica)
	rows, err := store.Query(ctx, "SELECT id FROM replica_test WHERE id = $1", []any{"r1"})
	require.NoError(t, err)
	defer rows.Close()
	var found bool
	for rows.Next() {
		found = true
	}
	assert.True(t, found)

	// Exists uses readPool
	ok, err := store.Exists(ctx, "replica_test", "id", "r1")
	require.NoError(t, err)
	assert.True(t, ok)
}

// ── Stress / concurrent read-write ───────────────────────────────────────────

func TestL3_ConcurrentUpsert(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent L3 test in short mode")
	}
	store, _, cleanup := setupPG(t)
	defer cleanup()
	ctx := context.Background()

	done := make(chan struct{})
	start := time.Now()
	for i := 0; i < 20; i++ {
		go func(n int) {
			id := fmt.Sprintf("conc%d", n)
			_ = store.Upsert(ctx, "items",
				[]string{"id", "name", "value"}, []any{id, "concurrent", n}, "id")
			done <- struct{}{}
		}(i)
	}
	for i := 0; i < 20; i++ {
		<-done
	}
	t.Logf("concurrent upsert finished in %v", time.Since(start))

	n, err := store.Count(ctx, "items", "id LIKE 'conc%'", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(20), n)
}

// ── Error paths — non-existent table triggers real DB errors ─────────────────
//
// Using a non-existent table produces a Postgres "relation does not exist"
// error. This is distinct from "no rows" and exercises every error-return
// path that the happy-path tests never reach.

func TestL3_Upsert_Error(t *testing.T) {
	store, _, cleanup := setupPG(t)
	defer cleanup()
	err := store.Upsert(context.Background(), "no_such_table",
		[]string{"id", "name"}, []any{"e1", "oops"}, "id")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "l3 upsert")
}

func TestL3_DeleteByID_Error(t *testing.T) {
	store, _, cleanup := setupPG(t)
	defer cleanup()
	err := store.DeleteByID(context.Background(), "no_such_table", "id", "x")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "l3 delete")
}

func TestL3_Query_Error(t *testing.T) {
	store, _, cleanup := setupPG(t)
	defer cleanup()
	_, err := store.Query(context.Background(), "SELECT * FROM no_such_table", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "l3 query")
}

func TestL3_QueryPrimary_Error(t *testing.T) {
	store, _, cleanup := setupPG(t)
	defer cleanup()
	_, err := store.QueryPrimary(context.Background(), "SELECT * FROM no_such_table", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "l3 query-primary")
}

func TestL3_CopyFromRows_Error(t *testing.T) {
	store, _, cleanup := setupPG(t)
	defer cleanup()
	src := l3.CopyFromSlice([][]any{{"x1", "Name", 1}})
	_, err := store.CopyFromRows(context.Background(),
		"no_such_table", []string{"id", "name", "value"}, src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "l3 copy")
}

// TestL3_Exists_RealError verifies the non-"no rows" error path in Exists.
// A missing TABLE (not a missing row) produces "relation does not exist",
// which isNoRows() correctly returns false for, triggering the error return.
func TestL3_Exists_RealError(t *testing.T) {
	store, _, cleanup := setupPG(t)
	defer cleanup()
	_, err := store.Exists(context.Background(), "no_such_table", "id", "x")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "l3 exists")
}

func TestL3_Count_Error(t *testing.T) {
	store, _, cleanup := setupPG(t)
	defer cleanup()
	_, err := store.Count(context.Background(), "no_such_table", "", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "l3 count")
}
