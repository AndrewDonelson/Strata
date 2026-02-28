// Package l3 provides the PostgreSQL persistence tier adapter.
package l3

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
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
	placeholders := make([]string, len(columns))
	updates := make([]string, 0, len(columns))
	for i, col := range columns {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		if col != pkColumn {
			updates = append(updates, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
		}
	}
	sql := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
		table,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
		pkColumn,
		strings.Join(updates, ", "),
	)
	_, err := s.pool.Exec(ctx, sql, values...)
	if err != nil {
		return fmt.Errorf("l3 upsert %s: %w", table, err)
	}
	return nil
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

// BeginTx starts a transaction.
func (s *Store) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return s.pool.Begin(ctx)
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
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "no rows")
}

// Pool returns the underlying primary connection pool.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }
