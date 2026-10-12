// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// tx.go — interactive transactions (RunTx / TxScope) that can read before they
// write, isolation levels, and automatic retry of serialization failures.

package strata

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"time"

	l3pkg "github.com/AndrewDonelson/strata/internal/l3"
	"github.com/jackc/pgx/v5"
)

// IsolationLevel is a Postgres transaction isolation level.
type IsolationLevel string

const (
	// ReadCommitted is the Postgres default. Use GetForUpdate to lock rows you
	// read and then write.
	ReadCommitted IsolationLevel = "read committed"
	// RepeatableRead gives the transaction one snapshot for all its reads.
	RepeatableRead IsolationLevel = "repeatable read"
	// Serializable makes concurrent transactions behave as if run one at a
	// time. Conflicting transactions fail with a serialization error, which
	// RunTx retries automatically.
	Serializable IsolationLevel = "serializable"
)

func (l IsolationLevel) pgx() pgx.TxIsoLevel {
	switch l {
	case RepeatableRead:
		return pgx.RepeatableRead
	case Serializable:
		return pgx.Serializable
	case ReadCommitted:
		return pgx.ReadCommitted
	default:
		return "" // server default
	}
}

// TxOptions configures RunTx.
type TxOptions struct {
	// Isolation level; zero value uses the server default (ReadCommitted).
	Isolation IsolationLevel
	// MaxRetries is how many times RunTx re-runs fn after a serialization
	// failure or deadlock. 0 means DefaultTxMaxRetries; negative disables retry.
	MaxRetries int
}

// DefaultTxMaxRetries is used when TxOptions.MaxRetries is 0.
const DefaultTxMaxRetries = 5

// txExecer adapts a pgx.Tx to rowsExecer.
func txExecer(tx pgx.Tx) rowsExecer {
	return func(ctx context.Context, sql string, args []any) (int64, error) {
		tag, err := tx.Exec(ctx, sql, args...)
		if err != nil {
			return 0, err
		}
		return tag.RowsAffected(), nil
	}
}

// RunTx runs fn inside one Postgres transaction and commits if fn returns nil.
//
// Inside fn, use the TxScope for every read and write: reads go straight to
// Postgres within the transaction (caches are bypassed), and caches are only
// updated after a successful commit. If fn returns an error the transaction is
// rolled back and that error is returned unchanged.
//
// Serialization failures and deadlocks (from fn or from commit) are retried up
// to MaxRetries times, so fn may run more than once: keep side effects outside
// the database out of it.
func (ds *DataStore) RunTx(ctx context.Context, opts TxOptions, fn func(tx *TxScope) error) error {
	if ds.closed.Load() {
		return ErrUnavailable
	}
	if ds.l3 == nil {
		return ErrL3Unavailable
	}
	retries := opts.MaxRetries
	if retries == 0 {
		retries = DefaultTxMaxRetries
	}
	var lastErr error
	for attempt := 0; ; attempt++ {
		err := ds.runTxOnce(ctx, opts, fn)
		if err == nil {
			return nil
		}
		var commitErr *txCommitError
		isCommit := errors.As(err, &commitErr)
		if !l3pkg.IsRetryable(err) || attempt >= retries {
			if isCommit {
				return fmt.Errorf("%w: %w", ErrTxFailed, commitErr.err)
			}
			if l3pkg.IsRetryable(err) {
				return fmt.Errorf("%w: gave up after %d retries: %w", ErrTxFailed, retries, err)
			}
			return err
		}
		lastErr = err
		if err := sleepBackoff(ctx, attempt); err != nil {
			return fmt.Errorf("%w: %w (last error: %v)", ErrTxFailed, err, lastErr)
		}
	}
}

// txCommitError marks an error that came from Begin or Commit, not from fn.
type txCommitError struct{ err error }

func (e *txCommitError) Error() string { return e.err.Error() }
func (e *txCommitError) Unwrap() error { return e.err }

func (ds *DataStore) runTxOnce(ctx context.Context, opts TxOptions, fn func(tx *TxScope) error) error {
	pgxTx, err := ds.l3.BeginTx(ctx, pgx.TxOptions{IsoLevel: opts.Isolation.pgx()})
	if err != nil {
		return &txCommitError{err}
	}
	scope := &TxScope{ds: ds, ctx: ctx, tx: pgxTx, exec: txExecer(pgxTx)}
	if err := fn(scope); err != nil {
		_ = pgxTx.Rollback(ctx)
		return err
	}
	if err := pgxTx.Commit(ctx); err != nil {
		return &txCommitError{err}
	}
	for _, op := range scope.ops {
		if op.del {
			ds.afterL3Delete(ctx, op.cs, op.id)
			continue
		}
		ds.afterL3Set(ctx, op.cs, op.id, op.value)
		if op.cs.Hooks.AfterSet != nil {
			op.cs.Hooks.AfterSet(ctx, op.value)
		}
	}
	return nil
}

// sleepBackoff waits a jittered, growing delay before retry number attempt+1.
func sleepBackoff(ctx context.Context, attempt int) error {
	base := time.Duration(1<<min(attempt, 6)) * time.Millisecond
	d := base + time.Duration(rand.Int64N(int64(base)+1))
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// TxScope is the handle passed to a RunTx callback. It's only valid until the
// callback returns and must not be used from other goroutines.
type TxScope struct {
	ds   *DataStore
	ctx  context.Context
	tx   pgx.Tx
	exec rowsExecer
	ops  []scopeOp
}

type scopeOp struct {
	cs    *compiledSchema
	id    string
	value any
	del   bool
}

func (s *TxScope) queryRow(ctx context.Context, sql string, args []any) pgx.Row {
	return s.tx.QueryRow(ctx, sql, args...)
}

// Get reads the record from Postgres inside the transaction, including
// omit_cache fields.
func (s *TxScope) Get(schemaName, id string, dest any) error {
	cs, err := s.ds.registry.get(schemaName)
	if err != nil {
		return err
	}
	return s.ds.readRow(s.ctx, s.queryRow, cs, id, dest, "")
}

// GetForUpdate is Get with SELECT ... FOR UPDATE: the row stays locked until
// the transaction ends. Use it under ReadCommitted for read-modify-write.
func (s *TxScope) GetForUpdate(schemaName, id string, dest any) error {
	cs, err := s.ds.registry.get(schemaName)
	if err != nil {
		return err
	}
	return s.ds.readRow(s.ctx, s.queryRow, cs, id, dest, " FOR UPDATE")
}

// Exists reports whether the record exists, as seen by the transaction.
func (s *TxScope) Exists(schemaName, id string) (bool, error) {
	cs, err := s.ds.registry.get(schemaName)
	if err != nil {
		return false, err
	}
	var one int
	err = s.tx.QueryRow(s.ctx,
		fmt.Sprintf("SELECT 1 FROM %s WHERE %s = $1", cs.tableName, cs.pkColumn.Name), id).Scan(&one)
	if err != nil {
		if isNoRowsError(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Search runs q inside the transaction. Like DataStore.Search it doesn't
// return omit_cache fields.
func (s *TxScope) Search(schemaName string, q *Query, destSlice any) error {
	cs, err := s.ds.registry.get(schemaName)
	if err != nil {
		return err
	}
	return s.ds.searchWith(s.ctx, func(ctx context.Context, sql string, args []any) (pgx.Rows, error) {
		return s.tx.Query(ctx, sql, args...)
	}, cs, q, destSlice)
}

// Set upserts value inside the transaction (versioned schemas are
// conflict-checked). BeforeSet runs now; caches and AfterSet run after commit.
func (s *TxScope) Set(schemaName, id string, value any) error {
	return s.write(schemaName, id, value, l3Upsert)
}

// Insert writes value only if its primary key and UNIQUE columns are free;
// otherwise it returns ErrAlreadyExists. The transaction can't continue after
// that error, so return it from the callback.
func (s *TxScope) Insert(schemaName, id string, value any) error {
	return s.write(schemaName, id, value, l3Insert)
}

func (s *TxScope) write(schemaName, id string, value any, kind l3WriteKind) error {
	cs, err := s.ds.registry.get(schemaName)
	if err != nil {
		return err
	}
	if reflect.ValueOf(value).Kind() != reflect.Ptr {
		return ErrInvalidModel
	}
	if cs.Hooks.BeforeSet != nil {
		if err := cs.Hooks.BeforeSet(s.ctx, value); err != nil {
			return err
		}
	}
	if err := s.ds.writeL3(s.ctx, s.exec, cs, value, kind); err != nil {
		return err
	}
	s.ops = append(s.ops, scopeOp{cs: cs, id: id, value: value})
	return nil
}

// Delete removes the record inside the transaction.
func (s *TxScope) Delete(schemaName, id string) error {
	cs, err := s.ds.registry.get(schemaName)
	if err != nil {
		return err
	}
	if _, err := s.exec(s.ctx,
		fmt.Sprintf("DELETE FROM %s WHERE %s = $1", cs.tableName, cs.pkColumn.Name),
		[]any{id}); err != nil {
		return fmt.Errorf("l3 delete %s: %w", cs.tableName, err)
	}
	s.ops = append(s.ops, scopeOp{cs: cs, id: id, del: true})
	return nil
}
