// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// strata_coverage5_test.go — white-box tests for stuck coverage paths
// (Review #5).  All tests live in package strata so they can reach
// unexported DataStore fields (l3, stats, sync) and inject the errL3
// mock without a live PostgreSQL instance.

package strata

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── errL3 — l3Backend mock that returns configurable errors ──────────────────

// errL3 implements l3Backend; every method returns the wrapped error.
// It is used to exercise L3-failure branches in DataStore without Postgres.
type errL3 struct{ err error }

func (e *errL3) Upsert(_ context.Context, _ string, _ []string, _ []any, _ string) error {
	return e.err
}
func (e *errL3) DeleteByID(_ context.Context, _, _ string, _ any) error { return e.err }
func (e *errL3) Query(_ context.Context, _ string, _ []any) (pgx.Rows, error) {
	return nil, e.err
}
func (e *errL3) QueryRow(_ context.Context, _ string, _ []any) pgx.Row { return &errRow{e.err} }
func (e *errL3) Exec(_ context.Context, _ string, _ []any) error       { return e.err }
func (e *errL3) Exists(_ context.Context, _, _ string, _ any) (bool, error) {
	return false, e.err
}
func (e *errL3) Count(_ context.Context, _, _ string, _ []any) (int64, error) { return 0, e.err }
func (e *errL3) BeginTx(_ context.Context) (pgx.Tx, error)                    { return nil, e.err }
func (e *errL3) Close()                                                       {}

// errRow is a pgx.Row whose Scan always returns the stored error.
type errRow struct{ err error }

func (r *errRow) Scan(_ ...any) error { return r.err }

// ── test model shared by this file ───────────────────────────────────────────

type cov5Item struct {
	ID string `strata:"primary_key"`
}

func registerCov5Schema(t *testing.T, ds *DataStore, name string, mode WriteMode) {
	t.Helper()
	require.NoError(t, ds.Register(Schema{Name: name, Model: &cov5Item{}, WriteMode: mode}))
}

// newDSWithBrokenL3 creates a DataStore with NO real L3, then injects an
// errL3 stub so that any operation that reaches L3 returns ErrL3Unavailable.
func newDSWithBrokenL3(t *testing.T) *DataStore {
	t.Helper()
	ds, err := NewDataStore(Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })
	ds.l3 = &errL3{err: ErrL3Unavailable}
	return ds
}

// ── strata.Set — routerSet error path ────────────────────────────────────────

// TestSet_L3Error_IncrementsErrorStat covers the `ds.stats.Errors.Add(1)` and
// `ds.metrics.RecordError` branches inside Set, which are only reachable when
// routerSet returns an error (i.e. when L3 rejects the upsert).
func TestSet_L3Error_IncrementsErrorStat(t *testing.T) {
	ds := newDSWithBrokenL3(t)
	registerCov5Schema(t, ds, "set_l3err", WriteThrough)

	ctx := context.Background()
	err := ds.Set(ctx, "set_l3err", "e1", &cov5Item{ID: "e1"})
	require.ErrorIs(t, err, ErrL3Unavailable)

	assert.Equal(t, int64(1), ds.stats.Errors.Load(),
		"stats.Errors must increment when routerSet fails")
}

// ── strata.SetMany — routerSet error path ────────────────────────────────────

// TestSetMany_L3Error_ReturnsOnFirstFailure covers the `return err` inside the
// SetMany loop when routerSet fails, which has been stuck at 77.8% for 4 rounds.
func TestSetMany_L3Error_ReturnsOnFirstFailure(t *testing.T) {
	ds := newDSWithBrokenL3(t)
	registerCov5Schema(t, ds, "setmany_l3err", WriteThrough)

	ctx := context.Background()
	err := ds.SetMany(ctx, "setmany_l3err", map[string]any{
		"e1": &cov5Item{ID: "e1"},
		"e2": &cov5Item{ID: "e2"},
	})
	require.ErrorIs(t, err, ErrL3Unavailable,
		"SetMany must propagate the L3 error immediately")
}

// ── routerSetL1Async — L3 error path ─────────────────────────────────────────

// TestRouterSetL1Async_L3Error covers the `return err` inside routerSetL1Async
// when writeToL3 fails; this path has been stuck at 80.0% for 3 rounds.
func TestRouterSetL1Async_L3Error(t *testing.T) {
	ds := newDSWithBrokenL3(t)
	registerCov5Schema(t, ds, "l1async_l3err", WriteThroughL1Async)

	err := ds.Set(context.Background(), "l1async_l3err", "e1", &cov5Item{ID: "e1"})
	require.ErrorIs(t, err, ErrL3Unavailable)
}

// ── routerSearch — L3.Query error path ───────────────────────────────────────

// TestRouterSearch_L3QueryError covers the `return err` after ds.l3.Query fails
// inside routerSearch; this path has been stuck at 83.3% for 3 rounds.
func TestRouterSearch_L3QueryError(t *testing.T) {
	ds := newDSWithBrokenL3(t)
	registerCov5Schema(t, ds, "search_l3err", WriteThrough)

	var results []cov5Item
	err := ds.Search(context.Background(), "search_l3err", nil, &results)
	require.ErrorIs(t, err, ErrL3Unavailable)
}

// ── Tx.Commit — BeginTx failure path ─────────────────────────────────────────

// TestTxCommit_BeginTxFailure covers the `fmt.Errorf("%w: ...", ErrTxFailed, err)`
// return inside Tx.Commit when BeginTx itself fails; this path has never been
// reachable without a real database going down.
func TestTxCommit_BeginTxFailure(t *testing.T) {
	ds := newDSWithBrokenL3(t)
	registerCov5Schema(t, ds, "tx_beginfail", WriteThrough)

	err := ds.Tx(context.Background()).
		Set("tx_beginfail", "x1", &cov5Item{ID: "x1"}).
		Commit()
	require.Error(t, err)
	require.ErrorIs(t, err, ErrTxFailed)
}

// ── sync.FlushDirty — retry and max-retry paths ───────────────────────────────

// TestFlushDirty_L3Error_RequeuesEntry verifies that when writeToL3 fails for a
// write-behind entry, the entry is re-queued with retries++ (the "failed" slice
// path inside flushDirty).  This path has been stuck at 66.7% for 4 rounds.
//
// NOTE: we deliberately do NOT configure DefaultWriteMode: WriteBehind on the
// DataStore (which would start the background writeBehindLoop), because that
// would race with our manual FlushDirty calls.  Only the per-schema WriteMode
// is set to WriteBehind.
func TestFlushDirty_L3Error_RequeuesEntry(t *testing.T) {
	ds, err := NewDataStore(Config{
		// Very long flush interval prevents the background ticker from
		// interfering with our deterministic manual FlushDirty calls.
		WriteBehindFlushInterval: 24 * time.Hour,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })
	ds.l3 = &errL3{err: errors.New("simulated pg down")}

	require.NoError(t, ds.Register(Schema{
		Name:      "wb_retry",
		Model:     &cov5Item{},
		WriteMode: WriteBehind,
	}))

	ctx := context.Background()
	// Set populates L1 and queues one dirty entry (no L3 write yet).
	_ = ds.Set(ctx, "wb_retry", "r1", &cov5Item{ID: "r1"})

	// FlushDirty: writeToL3 fails → entry re-queued with retries=1.
	require.NoError(t, ds.FlushDirty(ctx))

	st := ds.Stats()
	assert.Equal(t, int64(1), st.DirtyCount,
		"failed dirty entry must be re-queued after an L3 error")
}

// TestFlushDirty_MaxRetryExceeded_CallsHook verifies the max-retry exceeded
// branch: after WriteBehindMaxRetry flush attempts, the OnWriteError hook is
// invoked and the entry is permanently dropped from the dirty queue.
func TestFlushDirty_MaxRetryExceeded_CallsHook(t *testing.T) {
	const maxRetry = 2

	ds, err := NewDataStore(Config{
		WriteBehindFlushInterval: 24 * time.Hour,
		WriteBehindMaxRetry:      maxRetry,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })
	ds.l3 = &errL3{err: errors.New("simulated pg down")}

	var hookCalled bool
	require.NoError(t, ds.Register(Schema{
		Name:  "wb_maxretry",
		Model: &cov5Item{},
		Hooks: SchemaHooks{
			OnWriteError: func(_ context.Context, _ string, _ error) {
				hookCalled = true
			},
		},
		WriteMode: WriteBehind,
	}))

	ctx := context.Background()
	_ = ds.Set(ctx, "wb_maxretry", "r1", &cov5Item{ID: "r1"})

	// Flush maxRetry times to accumulate retries up to (but not including)
	// the threshold.  Each call fails and re-queues with retries++.
	for i := 0; i < maxRetry; i++ {
		require.NoError(t, ds.FlushDirty(ctx))
	}
	// One more flush: retries (== maxRetry) >= maxRetry → hook + discard.
	require.NoError(t, ds.FlushDirty(ctx))

	assert.True(t, hookCalled,
		"OnWriteError must be called when max retries are exhausted")

	st := ds.Stats()
	assert.Equal(t, int64(0), st.DirtyCount,
		"permanently failed entry must be removed from the dirty queue")
}

// ── mockPGXTx — minimal pgx.Tx for white-box tests ───────────────────────────

// mockPGXTx implements pgx.Tx with configurable Exec/Commit behaviour and
// no-ops for all other methods.  It is used with partialErrL3 below.
type mockPGXTx struct {
	execErr   error // returned by every Exec call; nil = success
	commitErr error // returned by Commit; nil = success
}

func (m *mockPGXTx) Begin(_ context.Context) (pgx.Tx, error) { return m, nil }
func (m *mockPGXTx) Commit(_ context.Context) error          { return m.commitErr }
func (m *mockPGXTx) Rollback(_ context.Context) error        { return nil }
func (m *mockPGXTx) Exec(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, m.execErr
}
func (m *mockPGXTx) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
	return nil, nil
}
func (m *mockPGXTx) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	return &errRow{}
}
func (m *mockPGXTx) CopyFrom(_ context.Context, _ pgx.Identifier, _ []string, _ pgx.CopyFromSource) (int64, error) {
	return 0, nil
}
func (m *mockPGXTx) SendBatch(_ context.Context, _ *pgx.Batch) pgx.BatchResults { return nil }
func (m *mockPGXTx) LargeObjects() pgx.LargeObjects                             { return pgx.LargeObjects{} }
func (m *mockPGXTx) Prepare(_ context.Context, _, _ string) (*pgconn.StatementDescription, error) {
	return nil, nil
}
func (m *mockPGXTx) Conn() *pgx.Conn { return nil }

// ── partialErrL3 — l3Backend where BeginTx succeeds but Upsert/Exec fail ─────

// partialErrL3 lets tests configure exactly which l3Backend operations fail:
//   - BeginTx  → returns tx (if non-nil) or upsertErr
//   - Exec     → returns execErr (nil = success; covers ensureMigrationTable)
//   - QueryRow → always returns pgx.ErrNoRows (tableExists = false)
//   - Upsert   → returns upsertErr
//   - All others → succeed/zero
type partialErrL3 struct {
	tx        pgx.Tx // returned by BeginTx; nil → BeginTx returns upsertErr
	execErr   error  // l3Backend.Exec error (nil = success)
	upsertErr error  // l3Backend.Upsert error (nil = success)
}

func (p *partialErrL3) BeginTx(_ context.Context) (pgx.Tx, error) {
	if p.tx == nil {
		if p.upsertErr != nil {
			return nil, p.upsertErr
		}
		return nil, ErrL3Unavailable
	}
	return p.tx, nil
}
func (p *partialErrL3) Exec(_ context.Context, _ string, _ []any) error { return p.execErr }
func (p *partialErrL3) Upsert(_ context.Context, _ string, _ []string, _ []any, _ string) error {
	return p.upsertErr
}
func (p *partialErrL3) QueryRow(_ context.Context, _ string, _ []any) pgx.Row {
	// Returning pgx.ErrNoRows makes tableExists/isMigrationApplied return (false, nil).
	return &errRow{pgx.ErrNoRows}
}
func (p *partialErrL3) Query(_ context.Context, _ string, _ []any) (pgx.Rows, error) {
	return nil, nil
}
func (p *partialErrL3) DeleteByID(_ context.Context, _, _ string, _ any) error { return nil }
func (p *partialErrL3) Exists(_ context.Context, _, _ string, _ any) (bool, error) {
	return false, nil
}
func (p *partialErrL3) Count(_ context.Context, _, _ string, _ []any) (int64, error) {
	return 0, nil
}
func (p *partialErrL3) Close() {}

// ── Tx.Commit — writeToL3 failure path ───────────────────────────────────────

// TestTxCommit_WriteToL3Failure covers the rollback+return branch inside
// Tx.Commit when writeToL3 (ds.l3.Upsert) fails AFTER BeginTx has succeeded.
// This is the remaining ~8% of Tx.Commit that has never been covered.
func TestTxCommit_WriteToL3Failure(t *testing.T) {
	ds, err := NewDataStore(Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	upsertFail := errors.New("forced upsert failure")
	// BeginTx returns a working no-op tx; Upsert returns upsertFail.
	ds.l3 = &partialErrL3{
		tx:        &mockPGXTx{},
		upsertErr: upsertFail,
	}
	registerCov5Schema(t, ds, "tx_upsertfail", WriteThrough)

	err = ds.Tx(context.Background()).
		Set("tx_upsertfail", "x1", &cov5Item{ID: "x1"}).
		Commit()
	require.Error(t, err)
	require.ErrorIs(t, err, ErrTxFailed,
		"writeToL3 failure inside Tx loop must wrap ErrTxFailed")
}

// ── migrate.createTable — DDL exec failure path ───────────────────────────────

// TestMigrate_CreateTable_DDLExecFailure covers the `tx.Rollback + return error`
// branch inside createTable when the CREATE TABLE DDL statement is rejected by
// the database.  This is one of the primary reasons createTable has been stuck
// at 53.3% for two rounds.
func TestMigrate_CreateTable_DDLExecFailure(t *testing.T) {
	ds, err := NewDataStore(Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	ddlErr := errors.New("DDL syntax error from test")
	// Exec (l3Backend) → nil: ensureMigrationTable CREATE TABLE succeeds.
	// QueryRow               → pgx.ErrNoRows: tableExists returns false.
	// BeginTx                → mockPGXTx with execErr: the DDL tx.Exec fails.
	ds.l3 = &partialErrL3{
		tx: &mockPGXTx{execErr: ddlErr},
	}
	registerCov5Schema(t, ds, "ddlfail_schema", WriteThrough)

	migrateErr := ds.Migrate(context.Background())
	require.Error(t, migrateErr)
	require.ErrorIs(t, migrateErr, ddlErr,
		"createTable DDL failure must propagate out through Migrate")
}
