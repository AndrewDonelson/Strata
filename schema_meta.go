// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// schema_meta.go — strata_schema_meta table: creation, read, and upsert helpers.
// This table tracks embedding model identity and dimensions per vector schema,
// enabling Migrate() to detect model changes that require re-embedding.
// Application code never reads this table directly.

package strata

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const schemaMeta = "strata_schema_meta"

// schemaMeta describes one row in strata_schema_meta.
type schemaMetaRow struct {
	SchemaName      string
	VectorField     string
	EmbeddingModel  string
	Dimensions      int
	ReembedStatus   string
	ReembedProgress int
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// ensureSchemaMetaTable creates the strata_schema_meta table if it does not exist.
func (ds *DataStore) ensureSchemaMetaTable(ctx context.Context) error {
	sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    schema_name      TEXT PRIMARY KEY,
    vector_field     TEXT NOT NULL,
    embedding_model  TEXT NOT NULL,
    dimensions       INT  NOT NULL,
    reembed_status   TEXT NOT NULL DEFAULT 'idle',
    reembed_progress INT  NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
)`, schemaMeta)
	return ds.l3.Exec(ctx, sql, nil)
}

// readSchemaMeta returns the metadata row for schemaName, or nil if absent.
func (ds *DataStore) readSchemaMeta(ctx context.Context, schemaName string) (*schemaMetaRow, error) {
	sql := fmt.Sprintf(
		`SELECT schema_name, vector_field, embedding_model, dimensions,
                reembed_status, reembed_progress, created_at, updated_at
           FROM %s WHERE schema_name = $1`, schemaMeta)

	row := ds.l3.QueryRow(ctx, sql, []any{schemaName})
	var m schemaMetaRow
	err := row.Scan(&m.SchemaName, &m.VectorField, &m.EmbeddingModel, &m.Dimensions,
		&m.ReembedStatus, &m.ReembedProgress, &m.CreatedAt, &m.UpdatedAt)
	if err != nil {
		if err == pgx.ErrNoRows || strings.Contains(err.Error(), "no rows") {
			return nil, nil
		}
		return nil, fmt.Errorf("read schema meta %q: %w", schemaName, err)
	}
	return &m, nil
}

// insertSchemaMeta inserts a new row; called when no row exists yet.
func (ds *DataStore) insertSchemaMeta(ctx context.Context, m schemaMetaRow) error {
	sql := fmt.Sprintf(
		`INSERT INTO %s (schema_name, vector_field, embedding_model, dimensions)
           VALUES ($1, $2, $3, $4)`, schemaMeta)
	return ds.l3.Exec(ctx, sql, []any{m.SchemaName, m.VectorField, m.EmbeddingModel, m.Dimensions})
}

// updateSchemaMetaModel updates the model ID and dimension after a successful ReEmbed.
func (ds *DataStore) updateSchemaMetaModel(ctx context.Context, schemaName, model string, dims int) error {
	sql := fmt.Sprintf(
		`UPDATE %s SET embedding_model=$1, dimensions=$2, updated_at=now()
           WHERE schema_name=$3`, schemaMeta)
	return ds.l3.Exec(ctx, sql, []any{model, dims, schemaName})
}

// updateSchemaMetaReembedStatus sets the reembed_status and reembed_progress columns.
func (ds *DataStore) updateSchemaMetaReembedStatus(ctx context.Context, schemaName, status string, progress int) error {
	sql := fmt.Sprintf(
		`UPDATE %s SET reembed_status=$1, reembed_progress=$2, updated_at=now()
           WHERE schema_name=$3`, schemaMeta)
	return ds.l3.Exec(ctx, sql, []any{status, progress, schemaName})
}
