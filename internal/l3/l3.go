// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// l3.go — PostgreSQL persistence tier: upsert, parameterised query, bulk
// COPY, transaction management, Exists/Count helpers, and optional read-
// replica routing via a secondary pgxpool.

// Package l3 provides the PostgreSQL persistence tier adapter.
package l3

import (
	"context"
	"fmt"
	"strings"

	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store is the L3 PostgreSQL adapter.
type Store struct {
	pool    *pgxpool.Pool
	replica *pgxpool.Pool
}

// New creates a new L3 Store from an existing pool.
func New(pool *pgxpool.Pool, replica *pgxpool.Pool) *Store {
	return &Store{pool: pool, replica: replica}
}

// readPool returns the read replica if available, otherwise the primary.
func (s *Store) readPool() *pgxpool.Pool {
	if s.replica != nil {
		return s.replica
	}
	return s.pool
}

// Ping verifies the primary pool is reachable.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Upsert performs an INSERT ... ON CONFLICT DO UPDATE for the given table.
func (s *Store) Upsert(ctx context.Context, table string, columns []string, values []any, pkColumn string) error {
	_, err := s.pool.Exec(ctx, UpsertSQL(table, columns, pkColumn), values...)
	if err != nil {
		return fmt.Errorf("l3 upsert %s: %w", table, err)
	}
	return nil
}

// UpsertSQL builds INSERT ... ON CONFLICT (pk) DO UPDATE with $1..$n placeholders
// in column order.
func UpsertSQL(table string, columns []string, pkColumn string) string {
	updates := make([]string, 0, len(columns))
	for _, col := range columns {
		if col != pkColumn {
			updates = append(updates, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
		}
	}
	if len(updates) == 0 {
		return InsertSQL(table, columns) + fmt.Sprintf(" ON CONFLICT (%s) DO NOTHING", pkColumn)
	}
	return InsertSQL(table, columns) +
		fmt.Sprintf(" ON CONFLICT (%s) DO UPDATE SET %s", pkColumn, strings.Join(updates, ", "))
}

// InsertSQL builds a plain INSERT with $1..$n placeholders in column order.
func InsertSQL(table string, columns []string) string {
	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		table, strings.Join(columns, ", "), strings.Join(placeholders, ", "))
}

// ExecRows executes a statement on the primary and returns the affected row count.
func (s *Store) ExecRows(ctx context.Context, sql string, args []any) (int64, error) {
	tag, err := s.pool.Exec(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// PostgreSQL SQLSTATE codes Strata maps to its own errors.
const (
	codeUniqueViolation      = "23505"
	codeSerializationFailure = "40001"
	codeDeadlockDetected     = "40P01"
)

func pgCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// IsUniqueViolation reports whether err is a primary-key or UNIQUE violation.
func IsUniqueViolation(err error) bool { return pgCode(err) == codeUniqueViolation }

// IsRetryable reports whether err is a serialization failure or deadlock, i.e.
// the transaction can succeed if run again.
func IsRetryable(err error) bool {
	c := pgCode(err)
	return c == codeSerializationFailure || c == codeDeadlockDetected
}

// DeleteByID removes a row by primary key.
func (s *Store) DeleteByID(ctx context.Context, table, pkColumn string, id any) error {
	sql := fmt.Sprintf("DELETE FROM %s WHERE %s = $1", table, pkColumn)
	_, err := s.pool.Exec(ctx, sql, id)
	if err != nil {
		return fmt.Errorf("l3 delete %s: %w", table, err)
	}
	return nil
}

// Query runs a parameterized SELECT and returns rows.
func (s *Store) Query(ctx context.Context, sql string, args []any) (pgx.Rows, error) {
	rows, err := s.readPool().Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("l3 query: %w", err)
	}
	return rows, nil
}

// QueryPrimary runs a query on the primary pool.
func (s *Store) QueryPrimary(ctx context.Context, sql string, args []any) (pgx.Rows, error) {
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("l3 query-primary: %w", err)
	}
	return rows, nil
}

// QueryRow runs a single-row query on the read pool.
func (s *Store) QueryRow(ctx context.Context, sql string, args []any) pgx.Row {
	return s.readPool().QueryRow(ctx, sql, args...)
}

// Exec executes a non-query statement on the primary.
func (s *Store) Exec(ctx context.Context, sql string, args []any) error {
	if args == nil {
		_, err := s.pool.Exec(ctx, sql)
		return err
	}
	_, err := s.pool.Exec(ctx, sql, args...)
	return err
}

// CopyFromRows performs a bulk COPY using pgx CopyFrom.
func (s *Store) CopyFromRows(ctx context.Context, table string, columns []string, rows pgx.CopyFromSource) (int64, error) {
	n, err := s.pool.CopyFrom(ctx, pgx.Identifier{table}, columns, rows)
	if err != nil {
		return 0, fmt.Errorf("l3 copy %s: %w", table, err)
	}
	return n, nil
}

// CopyFromSlice wraps a slice of row values for CopyFromRows.
func CopyFromSlice(rows [][]any) pgx.CopyFromSource {
	return pgx.CopyFromRows(rows)
}

// BeginTx starts a transaction with the given options (zero value = server defaults).
func (s *Store) BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
	return s.pool.BeginTx(ctx, opts)
}

// Exists checks if a row exists.
func (s *Store) Exists(ctx context.Context, table, pkColumn string, id any) (bool, error) {
	sql := fmt.Sprintf("SELECT 1 FROM %s WHERE %s = $1 LIMIT 1", table, pkColumn)
	var dummy int
	err := s.readPool().QueryRow(ctx, sql, id).Scan(&dummy)
	if err != nil {
		if isNoRows(err) {
			return false, nil
		}
		return false, fmt.Errorf("l3 exists %s: %w", table, err)
	}
	return true, nil
}

// Count returns the number of rows matching the WHERE clause.
func (s *Store) Count(ctx context.Context, table, where string, args []any) (int64, error) {
	sqlStr := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	if where != "" {
		sqlStr += " WHERE " + where
	}
	var n int64
	if err := s.pool.QueryRow(ctx, sqlStr, args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("l3 count %s: %w", table, err)
	}
	return n, nil
}

func isNoRows(err error) bool {
	// Callers always guard with 'if err != nil' before calling isNoRows.
	return strings.Contains(err.Error(), "no rows")
}

// Pool returns the underlying primary connection pool.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Close shuts down the underlying connection pool.
// Satisfies the l3Backend interface consumed by the top-level DataStore.
func (s *Store) Close() { s.pool.Close() }
