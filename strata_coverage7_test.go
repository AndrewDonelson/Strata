// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// strata_coverage7_test.go — white-box tests targeting the remaining stuck
// coverage gaps identified in Review #7:
//
//   • routerDelete  — l3.DeleteByID error return path            (90.9% → 100%)
//   • routerSearch  — rows.Scan error inside the rows loop       (87.5% → 100%)
//   • readFromL3    — non-ErrNoRows scan error path              (91.7% → 100%)
//   • writeToL3     — encryptFields error path (via ds.Set)      (87.5% → 100%)
//   • l1WriteWorker — drain inner loop (case op on stopCh fire)  (87.5% → 100%)
//   • crypto.Encrypt — nonce-generation error: intentionally left uncovered
//                      (see comment in crypto.go for rationale)
//
// All tests live in package strata (white-box) so they can reach unexported
// DataStore fields (l3, encryptor, sync, registry) without a live Postgres.

package strata

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── oneScanErrRows — pgx.Rows that yields exactly one row then fails Scan ────

// oneScanErrRows is a pgx.Rows implementation that returns true on the first
// call to Next() and then returns an error from Scan().  This lets
// TestRouterSearch_ScanError exercise the `return err` branch inside the
// rows loop of routerSearch without a live database.
type oneScanErrRows struct {
	err  error
	done bool
}

func (r *oneScanErrRows) Next() bool {
	if r.done {
		return false
	}
	r.done = true
	return true
}
func (r *oneScanErrRows) Scan(_ ...any) error                          { return r.err }
func (r *oneScanErrRows) Close()                                       {}
func (r *oneScanErrRows) Err() error                                   { return nil }
func (r *oneScanErrRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *oneScanErrRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *oneScanErrRows) Values() ([]any, error)                       { return nil, nil }
func (r *oneScanErrRows) RawValues() [][]byte                          { return nil }
func (r *oneScanErrRows) Conn() *pgx.Conn                              { return nil }

// ── scanErrL3 — l3Backend whose Query returns a scan-error Rows ──────────────

// scanErrL3 is an l3Backend where Query always returns an oneScanErrRows whose
// Scan fails with the configured error.  All other methods are no-ops so the
// mock does not interfere with any path that doesn't touch Search.
type scanErrL3 struct{ err error }

func (s *scanErrL3) Upsert(_ context.Context, _ string, _ []string, _ []any, _ string) error {
	return nil
}
func (s *scanErrL3) DeleteByID(_ context.Context, _, _ string, _ any) error { return nil }
func (s *scanErrL3) Query(_ context.Context, _ string, _ []any) (pgx.Rows, error) {
	return &oneScanErrRows{err: s.err}, nil
}
func (s *scanErrL3) QueryRow(_ context.Context, _ string, _ []any) pgx.Row {
	return &errRow{pgx.ErrNoRows}
}
func (s *scanErrL3) Exec(_ context.Context, _ string, _ []any) error { return nil }
func (s *scanErrL3) Exists(_ context.Context, _, _ string, _ any) (bool, error) {
	return false, nil
}
func (s *scanErrL3) Count(_ context.Context, _, _ string, _ []any) (int64, error) { return 0, nil }
func (s *scanErrL3) BeginTx(_ context.Context) (pgx.Tx, error) {
	return nil, ErrL3Unavailable
}
func (s *scanErrL3) Close() {}

// ── TestRouterDelete_L3Error ──────────────────────────────────────────────────

// TestRouterDelete_L3Error covers the `return err` inside routerDelete when
// l3.DeleteByID returns an error.  This was stuck at 90.9% (1 stmt uncovered).
func TestRouterDelete_L3Error(t *testing.T) {
	ds := newDSWithBrokenL3(t)
	registerCov5Schema(t, ds, "del_l3err", WriteThrough)

	err := ds.Delete(context.Background(), "del_l3err", "id1")
	require.ErrorIs(t, err, ErrL3Unavailable)
}

// ── TestGet_L3NonErrNoRowsScanError ──────────────────────────────────────────

// TestGet_L3NonErrNoRowsScanError covers the `return err` branch inside
// readFromL3 when row.Scan returns an error that is NOT a "no rows" sentinel.
//
// Path:  ds.Get → routerGet → readFromL3 → errRow.Scan → isNoRowsError=false
//
//	→ return err  ← was uncovered (readFromL3 stuck at 91.7%)
//
// L1 and L2 are cold (neither was populated), so execution always reaches
// readFromL3.  The errL3 mock's QueryRow returns an errRow that yields
// ErrL3Unavailable, which isNoRowsError returns false for (no "no rows"
// substring), hitting the previously-uncovered return.
func TestGet_L3NonErrNoRowsScanError(t *testing.T) {
	ds := newDSWithBrokenL3(t)
	registerCov5Schema(t, ds, "get_scanerr", WriteThrough)

	var got cov5Item
	err := ds.Get(context.Background(), "get_scanerr", "x1", &got)
	require.ErrorIs(t, err, ErrL3Unavailable)
}

// ── TestWriteToL3_EncryptFieldsError ─────────────────────────────────────────

// TestWriteToL3_EncryptFieldsError covers the `return err` inside writeToL3
// when encryptFields fails.
//
// Note: TestEncryptFields_EncryptError (router_white_test.go) calls
// ds.encryptFields() directly and already covers that function's error return.
// However it does NOT cover the `return err` inside writeToL3 that calls it.
// This test exercises that gap via the public ds.Set path.
// writeToL3 was stuck at 87.5% (1 stmt uncovered).
func TestWriteToL3_EncryptFieldsError(t *testing.T) {
	ds := newDSWithBrokenL3(t) // ds.l3 is non-nil — required to enter writeToL3
	ds.encryptor = failEncryptor{}
	require.NoError(t, ds.Register(Schema{
		Name:      "wt_encerr",
		Model:     &encWBModel{},
		WriteMode: WriteThrough,
	}))

	err := ds.Set(context.Background(), "wt_encerr", "e1",
		&encWBModel{ID: "e1", Token: "secret"})
	require.Error(t, err)
	// encryptFields wraps: "strata: encrypt field Token: forced encrypt error"
	assert.Contains(t, err.Error(), "strata: encrypt field")
}

// ── TestRouterSearch_ScanError ────────────────────────────────────────────────

// TestRouterSearch_ScanError covers the `return err` inside the routerSearch
// rows loop when rows.Scan returns an error.
//
// scanErrL3.Query returns an oneScanErrRows that reports one row via Next()
// but fails on Scan(), hitting the uncovered branch.
// routerSearch was stuck at 87.5% (1 stmt uncovered).
func TestRouterSearch_ScanError(t *testing.T) {
	ds, err := NewDataStore(Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })
	ds.l3 = &scanErrL3{err: ErrL3Unavailable}
	registerCov5Schema(t, ds, "search_scanerr", WriteThrough)

	var results []cov5Item
	err = ds.Search(context.Background(), "search_scanerr", nil, &results)
	require.ErrorIs(t, err, ErrL3Unavailable)
}

// ── TestRouterSearch_SliceOfPointers ─────────────────────────────────────────

// TestRouterSearch_SliceOfPointers covers the `elemType = elemType.Elem()`
// branch inside routerSearch that is only reached when the caller passes a
// slice of pointers (e.g. *[]*Model) rather than a slice of values (*[]Model).
// The call still fails with ErrL3Unavailable from the scan mock, but the
// elem-type unwrap line is executed before the loop error fires.
func TestRouterSearch_SliceOfPointers(t *testing.T) {
	ds, err := NewDataStore(Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })
	ds.l3 = &scanErrL3{err: ErrL3Unavailable}
	registerCov5Schema(t, ds, "search_ptrs", WriteThrough)

	// Passing a slice of *pointers* causes elemType.Kind()==reflect.Ptr and
	// triggers the `elemType = elemType.Elem()` branch.
	var results []*cov5Item
	err = ds.Search(context.Background(), "search_ptrs", nil, &results)
	require.ErrorIs(t, err, ErrL3Unavailable)
}

// ── TestWriteToL3_OmitCacheSkipped ───────────────────────────────────────────

// TestWriteToL3_OmitCacheSkipped covers the `col.OmitCache { continue }`
// branch inside the writeToL3 column-collection loop.  It also covers the
// `!f.IsValid() { continue }` guard for completeness.
// Uses whiteBoxModel which has Secret tagged `strata:"omit_cache"` so the
// loop must skip that column, hitting the previously-uncovered continue.
func TestWriteToL3_OmitCacheSkipped(t *testing.T) {
	ds := newDSWithBrokenL3(t)
	require.NoError(t, ds.Register(Schema{
		Name:      "wt_omitcache",
		Model:     &whiteBoxModel{},
		WriteMode: WriteThrough,
	}))

	// Upsert will fail (errL3), but the OmitCache continue fires before Upsert.
	err := ds.Set(context.Background(), "wt_omitcache", "o1",
		&whiteBoxModel{ID: "o1", Name: "n", Email: "e@test.com", Level: 1, Secret: "hidden"})
	require.ErrorIs(t, err, ErrL3Unavailable)
}

// ── TestL1WriteWorker_DrainOnStop ─────────────────────────────────────────────

// TestL1WriteWorker_DrainOnStop covers the drain inner loop inside
// l1WriteWorker:
//
//	case <-se.stopCh:
//	    for {
//	        select {
//	        case op := <-se.l1WriteCh:   ← was uncovered
//	            se.ds.setL1(op.cs, op.key, op.value)
//	        default:
//	            return
//	        }
//	    }
//
// Strategy: fill l1WriteCh to its full capacity (512) synchronously from the
// test goroutine — no scheduler switch can empty it during the fill loop.
// Then call ds.sync.stop() to close stopCh.  The worker MUST drain the ops
// via the inner select before returning, hitting the previously-uncovered stmt.
//
// After stop() ds.sync is set to nil so that the deferred ds.Close() skips
// the already-closed engine (Close guards with a nil check).
//
// l1WriteWorker was stuck at 87.5% (1 stmt uncovered).
func TestL1WriteWorker_DrainOnStop(t *testing.T) {
	ds, err := NewDataStore(Config{})
	require.NoError(t, err)
	// Intentionally do NOT add t.Cleanup here yet — we call stop() manually
	// and nil-out ds.sync below, then add the cleanup afterward.

	require.NoError(t, ds.Register(Schema{Name: "drain_wb", Model: &cov5Item{}}))
	cs, regErr := ds.registry.get("drain_wb")
	require.NoError(t, regErr)

	// Fill l1WriteCh to full capacity synchronously.
	// The tight loop runs in the test goroutine without yielding, so the
	// l1WriteWorker goroutine cannot empty the channel while we're filling it.
	for i := 0; i < l1WriteBufSize; i++ {
		select {
		case ds.sync.l1WriteCh <- l1WriteOp{
			cs:    cs,
			key:   fmt.Sprintf("drain_wb:drain_wb:k%d", i),
			value: &cov5Item{ID: fmt.Sprintf("k%d", i)},
		}:
		default:
			// Channel slot unexpectedly taken — still fine, more items follow.
		}
	}

	// Stop the sync engine while the channel holds buffered items.
	// The worker receives the stopCh signal and must drain remaining ops via
	// the inner `case op := <-se.l1WriteCh` branch before returning.
	ds.sync.stop()
	ds.sync = nil // prevent ds.Close() from double-stopping the closed engine

	t.Cleanup(func() { _ = ds.Close() })
}
