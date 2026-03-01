// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// strata_coverage6_test.go — white-box tests targeting the remaining stuck
// coverage gaps identified in Review #6:
//
//   • sync.flushDirty — registry.get error path (unknown schema)
//   • migrate.MigrateFrom — ensureTable, readDir, tx.Exec, recordMigration error paths
//   • migrate.alterTable — existingColumns Query error and BeginTx failure paths
//
// All tests live in package strata (white-box) so they can inject unexported
// fields and mocks without a live PostgreSQL instance.

package strata

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── successRow — pgx.Row whose Scan populates the first *int dest with 1 ─────

// successRow simulates a single-column row scanning a dummy integer value.
// Used by migrateTestL3 to make tableExists return true.
type successRow struct{}

func (r *successRow) Scan(dest ...any) error {
	if len(dest) > 0 {
		if p, ok := dest[0].(*int); ok {
			*p = 1
		}
	}
	return nil
}

// ── emptyRows — pgx.Rows with zero rows ──────────────────────────────────────

// emptyRows is a pgx.Rows implementation that immediately signals EOF.
// It is returned by migrateTestL3.Query when no error is configured, so that
// existingColumns sees an empty column set (all schema columns are "new").
type emptyRows struct{}

func (r *emptyRows) Close()                                       {}
func (r *emptyRows) Err() error                                   { return nil }
func (r *emptyRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *emptyRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *emptyRows) Next() bool                                   { return false }
func (r *emptyRows) Scan(_ ...any) error                          { return nil }
func (r *emptyRows) Values() ([]any, error)                       { return nil, nil }
func (r *emptyRows) RawValues() [][]byte                          { return nil }
func (r *emptyRows) Conn() *pgx.Conn                              { return nil }

// ── mockPGXTxNthExecFails — tx whose N-th Exec call returns an error ─────────

// mockPGXTxNthExecFails lets tests make exactly the N-th Exec call fail.
// Use n=1 to fail the SQL-content exec; n=2 to fail the recordMigration INSERT.
type mockPGXTxNthExecFails struct {
	n   int // 1-indexed call that returns the error
	cur int
	err error
}

func (m *mockPGXTxNthExecFails) Begin(_ context.Context) (pgx.Tx, error) { return m, nil }
func (m *mockPGXTxNthExecFails) Commit(_ context.Context) error          { return nil }
func (m *mockPGXTxNthExecFails) Rollback(_ context.Context) error        { return nil }
func (m *mockPGXTxNthExecFails) Exec(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	m.cur++
	if m.cur == m.n {
		return pgconn.CommandTag{}, m.err
	}
	return pgconn.CommandTag{}, nil
}
func (m *mockPGXTxNthExecFails) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
	return nil, nil
}
func (m *mockPGXTxNthExecFails) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	return &errRow{}
}
func (m *mockPGXTxNthExecFails) CopyFrom(_ context.Context, _ pgx.Identifier, _ []string, _ pgx.CopyFromSource) (int64, error) {
	return 0, nil
}
func (m *mockPGXTxNthExecFails) SendBatch(_ context.Context, _ *pgx.Batch) pgx.BatchResults {
	return nil
}
func (m *mockPGXTxNthExecFails) LargeObjects() pgx.LargeObjects { return pgx.LargeObjects{} }
func (m *mockPGXTxNthExecFails) Prepare(_ context.Context, _, _ string) (*pgconn.StatementDescription, error) {
	return nil, nil
}
func (m *mockPGXTxNthExecFails) Conn() *pgx.Conn { return nil }

// ── migrateTestL3 — configurable l3Backend for migrate/alterTable paths ──────

// migrateTestL3 provides fine-grained control over l3 behaviour for migration
// tests without a live database.  Each field is independently configurable:
//
//   - execErr        — returned by l3.Exec (ensureMigrationTable)
//   - queryErr       — returned by l3.Query (existingColumns)
//   - tableExists    — when true, QueryRow for tableExists returns scannable row
//   - tableExistsErr — when set, QueryRow for tableExists returns this error
//   - queryRowErr    — when set, QueryRow for non-tableExists calls returns error
//   - beginTxErr     — returned by BeginTx when non-nil
//   - tx             — tx returned by BeginTx when beginTxErr is nil and non-nil
type migrateTestL3 struct {
	execErr        error  // l3.Exec error (nil = success)
	queryErr       error  // l3.Query error (nil → emptyRows returned)
	tableExists    bool   // true → QueryRow for tableExists returns scannable row
	tableExistsErr error  // when set, QueryRow for tableExists returns this error
	queryRowErr    error  // when set, QueryRow for other queries returns this error
	beginTxErr     error  // BeginTx error (nil → uses tx field)
	tx             pgx.Tx // tx returned by BeginTx when beginTxErr is nil
}

func (m *migrateTestL3) QueryRow(_ context.Context, sql string, _ []any) pgx.Row {
	if strings.Contains(sql, "information_schema.tables") {
		if m.tableExistsErr != nil {
			return &errRow{m.tableExistsErr} // simulates tableExists query error
		}
		if m.tableExists {
			return &successRow{} // simulates table-exists = true
		}
		return &errRow{pgx.ErrNoRows}
	}
	// Non-tableExists queries (e.g. isMigrationApplied)
	if m.queryRowErr != nil {
		return &errRow{m.queryRowErr}
	}
	return &errRow{pgx.ErrNoRows}
}

func (m *migrateTestL3) Query(_ context.Context, _ string, _ []any) (pgx.Rows, error) {
	if m.queryErr != nil {
		return nil, m.queryErr
	}
	return &emptyRows{}, nil
}

func (m *migrateTestL3) BeginTx(_ context.Context) (pgx.Tx, error) {
	if m.beginTxErr != nil {
		return nil, m.beginTxErr
	}
	if m.tx != nil {
		return m.tx, nil
	}
	return nil, ErrL3Unavailable
}

func (m *migrateTestL3) Exec(_ context.Context, _ string, _ []any) error { return m.execErr }
func (m *migrateTestL3) Upsert(_ context.Context, _ string, _ []string, _ []any, _ string) error {
	return nil
}
func (m *migrateTestL3) DeleteByID(_ context.Context, _, _ string, _ any) error { return nil }
func (m *migrateTestL3) Exists(_ context.Context, _, _ string, _ any) (bool, error) {
	return false, nil
}
func (m *migrateTestL3) Count(_ context.Context, _, _ string, _ []any) (int64, error) {
	return 0, nil
}
func (m *migrateTestL3) Close() {}

// ── sync.flushDirty — registry miss ──────────────────────────────────────────

// TestFlushDirty_UnknownSchema_SkipsEntry covers the `registry.get → err != nil`
// branch inside the main flushDirty loop.  This path is exercised when a dirty
// entry's schemaName is not present in the registry (e.g. the schema was never
// registered or was registered in a different DataStore instance).
//
// The branch has been stuck for multiple rounds because all prior tests
// register the schema before queuing the dirty entry, so registry.get always
// succeeds.  Here we inject the dirty entry directly via white-box access
// WITHOUT calling ds.Register.
func TestFlushDirty_UnknownSchema_SkipsEntry(t *testing.T) {
	ds, err := NewDataStore(Config{
		// Very long flush interval prevents the background ticker from racing.
		WriteBehindFlushInterval: 24 * time.Hour,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	// Inject an L3 whose Upsert would fail loudly — it must NEVER be reached
	// for an entry whose schema is not in the registry.
	ds.l3 = &errL3{err: errors.New("l3 must not be called for unknown schema")}

	// Directly inject a dirty entry with a schema name that is NOT registered.
	ds.sync.dirtyMu.Lock()
	ds.sync.dirty["ghost_schema:g1"] = &dirtyEntry{
		schemaName: "ghost_schema",
		id:         "g1",
		value:      &cov5Item{ID: "g1"},
	}
	ds.sync.dirtyCount.Store(1)
	ds.sync.dirtyMu.Unlock()

	// FlushDirty: registry.get("ghost_schema") returns an error → continue.
	// The entry must be silently discarded (no retry, no panic).
	require.NoError(t, ds.FlushDirty(context.Background()))

	st := ds.Stats()
	assert.Equal(t, int64(0), st.DirtyCount,
		"dirty entry with an unregistered schema must be silently discarded")
}

// ── MigrateFrom error paths ───────────────────────────────────────────────────

// TestMigrateFrom_EnsureTableFails covers the early return when
// ensureMigrationTable (l3.Exec) returns an error.  This is the first
// internal call inside MigrateFrom; it has been missed because all prior
// tests either succeed or exercise later paths.
func TestMigrateFrom_EnsureTableFails(t *testing.T) {
	ds, err := NewDataStore(Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	tableErr := errors.New("cannot create migration table")
	ds.l3 = &migrateTestL3{execErr: tableErr}

	err = ds.MigrateFrom(context.Background(), t.TempDir())
	require.ErrorIs(t, err, tableErr,
		"MigrateFrom must propagate ensureMigrationTable failure")
}

// TestMigrateFrom_ReadDirFails covers the os.ReadDir failure path (the
// directory does not exist).  ensureMigrationTable succeeds; os.ReadDir fails.
func TestMigrateFrom_ReadDirFails(t *testing.T) {
	ds, err := NewDataStore(Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	ds.l3 = &migrateTestL3{} // execErr = nil → ensureMigrationTable succeeds

	err = ds.MigrateFrom(context.Background(), "/does/not/exist/strata_test_12345")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "readdir",
		"MigrateFrom error must mention readdir when the directory is missing")
}

// TestMigrateFrom_SQLExecFails covers the tx.Exec failure when the SQL
// migration file content is rejected (exercises the rollback path).
//
// Setup:
//   - l3.Exec:    nil  (ensureMigrationTable succeeds)
//   - l3.QueryRow: ErrNoRows (isMigrationApplied → not applied)
//   - l3.BeginTx: returns mockPGXTx whose Exec always returns an error
//   - temp dir:   one .sql file
func TestMigrateFrom_SQLExecFails(t *testing.T) {
	ds, err := NewDataStore(Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	sqlErr := errors.New("syntax error in migration SQL")
	ds.l3 = &migrateTestL3{
		tx: &mockPGXTx{execErr: sqlErr},
	}

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "001_init.sql"),
		[]byte("INVALID SQL CONTENT;"), 0o644,
	))

	err = ds.MigrateFrom(context.Background(), dir)
	require.ErrorIs(t, err, sqlErr,
		"MigrateFrom must propagate the tx.Exec error for the SQL file")
}

// TestMigrateFrom_RecordMigrationFails covers the rollback path when the
// INSERT INTO _strata_migrations record fails AFTER the SQL has been applied.
// The first tx.Exec (SQL content) succeeds; the second (INSERT) fails.
func TestMigrateFrom_RecordMigrationFails(t *testing.T) {
	ds, err := NewDataStore(Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	recordErr := errors.New("insert migration record failed")
	ds.l3 = &migrateTestL3{
		// n=2: first tx.Exec (SQL content) succeeds; second (INSERT) fails.
		tx: &mockPGXTxNthExecFails{n: 2, err: recordErr},
	}

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "001_init.sql"),
		[]byte("CREATE TABLE foo (id TEXT PRIMARY KEY);"), 0o644,
	))

	err = ds.MigrateFrom(context.Background(), dir)
	require.ErrorIs(t, err, recordErr,
		"MigrateFrom must propagate the recordMigration error and roll back")
}

// ── alterTable error paths ────────────────────────────────────────────────────

// TestAlterTable_ExistingColumnsQueryFails covers the `existingColumns` error
// path inside alterTable — when the l3.Query for column info fails.
//
// Setup:
//   - tableExists  = true  → alterTable is called (not createTable)
//   - queryErr     != nil  → existingColumns returns an error immediately
func TestAlterTable_ExistingColumnsQueryFails(t *testing.T) {
	ds, err := NewDataStore(Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	colQueryErr := errors.New("information_schema columns query failed")
	ds.l3 = &migrateTestL3{
		tableExists: true,
		queryErr:    colQueryErr,
	}
	require.NoError(t, ds.Register(Schema{Name: "alterfail", Model: &cov5Item{}}))

	err = ds.Migrate(context.Background())
	require.ErrorIs(t, err, colQueryErr,
		"alterTable must propagate the existingColumns query error")
}

// TestAlterTable_BeginTxFails covers the BeginTx failure inside alterTable
// when there are genuinely new columns to add (stmts is non-empty).
//
// Setup:
//   - tableExists  = true   → alterTable is called
//   - queryErr     = nil    → existingColumns returns emptyRows (no existing cols)
//     → all schema columns are "new" → stmts non-empty
//   - beginTxErr   != nil   → BeginTx returns error
func TestAlterTable_BeginTxFails(t *testing.T) {
	ds, err := NewDataStore(Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	beginErr := errors.New("cannot start ALTER TABLE transaction")
	ds.l3 = &migrateTestL3{
		tableExists: true,
		beginTxErr:  beginErr,
	}
	require.NoError(t, ds.Register(Schema{Name: "alterbeginfail", Model: &cov5Item{}}))

	err = ds.Migrate(context.Background())
	require.ErrorIs(t, err, beginErr,
		"alterTable must propagate the BeginTx error when there are new columns")
}

// TestAlterTable_TxExecFails covers the tx.Exec failure branch inside
// alterTable (the ALTER TABLE ADD COLUMN statement is rejected).
func TestAlterTable_TxExecFails(t *testing.T) {
	ds, err := NewDataStore(Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	execErr := errors.New("ALTER TABLE rejected")
	ds.l3 = &migrateTestL3{
		tableExists: true, // → alterTable path
		// First tx.Exec (ALTER ADD COLUMN) fails.
		tx: &mockPGXTx{execErr: execErr},
	}
	require.NoError(t, ds.Register(Schema{Name: "altertxfail", Model: &cov5Item{}}))

	err = ds.Migrate(context.Background())
	require.ErrorIs(t, err, execErr,
		"alterTable must propagate the tx.Exec error for ADD COLUMN")
}

// TestAlterTable_RecordMigrationFails covers the rollback path when the
// INSERT migration record fails after all ALTER TABLE stmts succeed.
// cov5Item has 1 column → 1 ALTER stmt → 2nd tx.Exec is the INSERT record.
func TestAlterTable_RecordMigrationFails(t *testing.T) {
	ds, err := NewDataStore(Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	recordErr := errors.New("migration record insert failed in alterTable")
	ds.l3 = &migrateTestL3{
		tableExists: true,
		// n=2: first Exec (ALTER ADD COLUMN) succeeds; second (INSERT record) fails.
		tx: &mockPGXTxNthExecFails{n: 2, err: recordErr},
	}
	require.NoError(t, ds.Register(Schema{Name: "alterrecordfail", Model: &cov5Item{}}))

	err = ds.Migrate(context.Background())
	require.ErrorIs(t, err, recordErr,
		"alterTable must propagate the recordMigration error and roll back")
}

// ── FlushDirty nil sync ───────────────────────────────────────────────────────

// TestFlushDirty_NilSync covers the `if ds.sync == nil { return nil }` branch
// inside the public FlushDirty wrapper.  NewDataStore always initialises sync,
// so we manually nil it out via white-box access.
func TestFlushDirty_NilSync(t *testing.T) {
	ds, err := NewDataStore(Config{})
	require.NoError(t, err)
	// Stop the sync engine before clearing the pointer so goroutines exit cleanly.
	ds.sync.stop()
	ds.sync = nil
	t.Cleanup(func() { _ = ds.Close() }) // Close checks sync != nil before stopping

	require.NoError(t, ds.FlushDirty(context.Background()),
		"FlushDirty with no sync engine must return nil immediately")
}

// ── Migrate / MigrateFrom — L3 not configured ────────────────────────────────

// TestMigrate_L3NotConfigured covers the `if ds.l3 == nil { return nil }` path
// inside Migrate.  Every other Migrate test injects a mock l3, so this path
// has never been exercised.
func TestMigrate_L3NotConfigured(t *testing.T) {
	ds, err := NewDataStore(Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })
	// ds.l3 is nil by default when no PostgreSQL pool is provided.
	require.NoError(t, ds.Migrate(context.Background()),
		"Migrate without L3 must succeed (no-op)")
}

// TestMigrateFrom_L3NotConfigured covers the `if ds.l3 == nil { return ErrL3Unavailable }`
// path inside MigrateFrom.
func TestMigrateFrom_L3NotConfigured(t *testing.T) {
	ds, err := NewDataStore(Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	err = ds.MigrateFrom(context.Background(), t.TempDir())
	require.ErrorIs(t, err, ErrL3Unavailable,
		"MigrateFrom without L3 must return ErrL3Unavailable")
}

// ── MigrateFrom — remaining error paths ──────────────────────────────────────

// TestMigrateFrom_BeginTxFails covers the tx begin failure inside the file
// processing loop (isMigrationApplied succeeds; BeginTx fails).
func TestMigrateFrom_BeginTxFails(t *testing.T) {
	ds, err := NewDataStore(Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	beginErr := errors.New("begin tx failed for migration file")
	ds.l3 = &migrateTestL3{beginTxErr: beginErr}

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "001_init.sql"),
		[]byte("CREATE TABLE foo (id TEXT PRIMARY KEY);"), 0o644,
	))

	err = ds.MigrateFrom(context.Background(), dir)
	require.ErrorIs(t, err, beginErr,
		"MigrateFrom must propagate the BeginTx error for the migration file")
}

// TestMigrateFrom_CommitFails covers the tx.Commit failure branch: SQL exec
// and recordMigration both succeed, but commit is rejected.
func TestMigrateFrom_CommitFails(t *testing.T) {
	ds, err := NewDataStore(Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	commitErr := errors.New("commit failed after migration applied")
	// execErr=nil: all tx.Exec calls succeed; commitErr set on Commit.
	ds.l3 = &migrateTestL3{tx: &mockPGXTx{commitErr: commitErr}}

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "001_init.sql"),
		[]byte("CREATE TABLE foo (id TEXT PRIMARY KEY);"), 0o644,
	))

	err = ds.MigrateFrom(context.Background(), dir)
	require.ErrorIs(t, err, commitErr,
		"MigrateFrom must propagate the tx.Commit error")
}

// TestMigrateFrom_IsMigrationAppliedFails covers the error path when the
// _strata_migrations check query returns an unexpected (non-ErrNoRows) error.
func TestMigrateFrom_IsMigrationAppliedFails(t *testing.T) {
	ds, err := NewDataStore(Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	checkErr := errors.New("migration table query error")
	// queryRowErr causes isMigrationApplied to return (false, checkErr).
	ds.l3 = &migrateTestL3{queryRowErr: checkErr}

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "001_init.sql"),
		[]byte("CREATE TABLE foo (id TEXT PRIMARY KEY);"), 0o644,
	))

	err = ds.MigrateFrom(context.Background(), dir)
	require.ErrorIs(t, err, checkErr,
		"MigrateFrom must propagate the isMigrationApplied query error")
}

// ── createTable — remaining error paths ──────────────────────────────────────

// TestMigrate_CreateTable_BeginTxFails covers the `BeginTx fails` branch inside
// createTable.  partialErrL3 with tx=nil and upsertErr=nil returns
// (nil, ErrL3Unavailable) from BeginTx, which is the only uncovered path.
func TestMigrate_CreateTable_BeginTxFails(t *testing.T) {
	ds, err := NewDataStore(Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	// partialErrL3{}: Exec=nil, QueryRow=ErrNoRows (tableExists=false → createTable),
	// BeginTx returns (nil, ErrL3Unavailable) because tx is nil.
	ds.l3 = &partialErrL3{}
	require.NoError(t, ds.Register(Schema{Name: "ct_beginfail", Model: &cov5Item{}}))

	err = ds.Migrate(context.Background())
	require.ErrorIs(t, err, ErrL3Unavailable,
		"createTable must propagate the BeginTx error")
}

// TestMigrate_CreateTable_IndexExecFails covers the tx.Exec failure for the
// CREATE INDEX statement inside createTable.  whiteBoxModel has a Name field
// tagged `strata:"index"` so buildIndexDDL returns one index statement.
// The first tx.Exec (CREATE TABLE) succeeds; the second (CREATE INDEX) fails.
func TestMigrate_CreateTable_IndexExecFails(t *testing.T) {
	ds, err := NewDataStore(Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	idxErr := errors.New("CREATE INDEX rejected")
	ds.l3 = &partialErrL3{
		// n=2: 1st Exec (CREATE TABLE) succeeds; 2nd Exec (CREATE INDEX) fails.
		tx: &mockPGXTxNthExecFails{n: 2, err: idxErr},
	}
	require.NoError(t, ds.Register(Schema{Name: "ct_idxfail", Model: &whiteBoxModel{}}))

	err = ds.Migrate(context.Background())
	require.ErrorIs(t, err, idxErr,
		"createTable must propagate the index CREATE INDEX exec error")
}

// TestMigrate_CreateTable_RecordMigrationFails covers the recordMigration
// failure rollback path inside createTable.  cov5Item has no index, so the
// first Exec is CREATE TABLE (succeeds) and the second is the migration INSERT
// (fails).
func TestMigrate_CreateTable_RecordMigrationFails(t *testing.T) {
	ds, err := NewDataStore(Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	recordErr := errors.New("migration record insert failed in createTable")
	ds.l3 = &partialErrL3{
		// n=2: CREATE TABLE succeeds; INSERT _strata_migrations fails.
		tx: &mockPGXTxNthExecFails{n: 2, err: recordErr},
	}
	require.NoError(t, ds.Register(Schema{Name: "ct_recordfail", Model: &cov5Item{}}))

	err = ds.Migrate(context.Background())
	require.ErrorIs(t, err, recordErr,
		"createTable must propagate the recordMigration error and roll back")
}

// ── MigrationStatus / tableExists — nil-L3 and query-error paths ─────────────

// TestMigrationStatus_L3NotConfigured covers the `if ds.l3 == nil` early return
// inside MigrationStatus (returns nil, ErrL3Unavailable).
func TestMigrationStatus_L3NotConfigured(t *testing.T) {
	ds, err := NewDataStore(Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	_, err = ds.MigrationStatus(context.Background())
	require.ErrorIs(t, err, ErrL3Unavailable,
		"MigrationStatus without L3 must return ErrL3Unavailable")
}

// TestMigrateSchema_TableExistsQueryError covers the `tableExists` error path
// where QueryRow returns a non-ErrNoRows error (unexpected DB error).
func TestMigrateSchema_TableExistsQueryError(t *testing.T) {
	ds, err := NewDataStore(Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	tableErr := errors.New("information_schema.tables query failed")
	ds.l3 = &migrateTestL3{tableExistsErr: tableErr}
	require.NoError(t, ds.Register(Schema{Name: "te_err", Model: &cov5Item{}}))

	err = ds.Migrate(context.Background())
	require.ErrorIs(t, err, tableErr,
		"Migrate must propagate the tableExists query error")
}
