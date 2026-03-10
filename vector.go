// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// vector.go — VectorSearch and ReEmbed: L3-only semantic similarity search
// and background re-embedding migration. Strata owns the full embedding
// lifecycle: it calls the EmbeddingProvider to generate vectors, executes
// pgvector ANN queries, and warms L2/L1 on results.

package strata

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	pgvector "github.com/pgvector/pgvector-go"
)

// SimilarityResult is one item returned by VectorSearch.
type SimilarityResult struct {
	// ID is the primary-key value of the matching record.
	ID string
	// Score is the cosine similarity (0.0 = orthogonal, 1.0 = identical).
	// Computed as 1 − cosine_distance, assuming unit-normalised vectors.
	Score float64
	// Value is the fully-hydrated model struct (same type as the schema Model).
	// The embedding/vector field IS populated when results come directly from L3.
	Value any
}

// VectorSearch performs an approximate nearest-neighbour search over the
// vector field of the named schema, using the configured EmbeddingProvider
// to embed the plain-text query string internally.
//
// topK must be ≥ 1; filters maps column names to equality values (AND logic).
// Results are ordered by cosine similarity (highest first) and are back-filled
// into L2 then L1 before returning.
//
// Returns ErrTopKInvalid, ErrSchemaNotFound, ErrNoVectorField,
// ErrNoEmbeddingProvider, ErrPgvectorExtensionMissing, or ErrEmptyVectorQuery
// for invalid inputs.
func (ds *DataStore) VectorSearch(
	ctx context.Context,
	schemaName string,
	query string,
	topK int,
	filters map[string]any,
) ([]SimilarityResult, error) {
	// Input validation
	if ctx == nil {
		return nil, ErrNilContext
	}
	if topK < 1 {
		return nil, ErrTopKInvalid
	}
	if strings.TrimSpace(query) == "" {
		return nil, ErrEmptyVectorQuery
	}

	cs, err := ds.registry.get(schemaName)
	if err != nil {
		return nil, err
	}
	if !cs.hasVectorFields || cs.vectorField == nil {
		return nil, ErrNoVectorField
	}
	if ds.cfg.EmbeddingProvider == nil {
		return nil, ErrNoEmbeddingProvider
	}
	if ds.l3 == nil {
		return nil, ErrL3Unavailable
	}

	// Embed the query string.
	queryVec, err := ds.cfg.EmbeddingProvider.Embed(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("strata vector search: embed query: %w", err)
	}

	return ds.vectorSearchWithVec(ctx, cs, queryVec, topK, filters)
}

// vectorSearchWithVec executes the pgvector ANN query with a pre-computed vector.
// Separated from VectorSearch to allow internal tests to inject known vectors.
func (ds *DataStore) vectorSearchWithVec(
	ctx context.Context,
	cs *compiledSchema,
	queryVec pgvector.Vector,
	topK int,
	filters map[string]any,
) ([]SimilarityResult, error) {
	vectorFieldName := cs.vectorField.Name

	// Build column list: all L3-visible columns (including vector field).
	selectCols := colNamesWithVector(cs)

	// Build WHERE clause from filters.
	var whereParts []string
	var filterArgs []any
	argIdx := 2 // $1 = queryVec
	for col, val := range filters {
		whereParts = append(whereParts, fmt.Sprintf("%s = $%d", col, argIdx))
		filterArgs = append(filterArgs, val)
		argIdx++
	}
	whereClause := ""
	if len(whereParts) > 0 {
		whereClause = "WHERE " + strings.Join(whereParts, " AND ")
	}

	// Build SQL.
	// Score = 1 - cosine_distance (normalised embedding models produce vectors
	// where cosine_distance ∈ [0,1], so score ∈ [0,1]).
	sql := fmt.Sprintf(
		`SELECT %s, 1 - (%s <=> $1::vector) AS _score
FROM %s
%s
ORDER BY %s <=> $1::vector ASC
LIMIT %d`,
		strings.Join(selectCols, ", "),
		vectorFieldName,
		cs.tableName,
		whereClause,
		vectorFieldName,
		topK,
	)

	args := make([]any, 0, 1+len(filterArgs))
	args = append(args, queryVec)
	args = append(args, filterArgs...)

	rows, err := ds.l3.Query(ctx, sql, args)
	if err != nil {
		return nil, fmt.Errorf("strata vector search: query: %w", err)
	}
	defer rows.Close()

	var results []SimilarityResult
	for rows.Next() {
		elem := reflect.New(cs.modelType).Elem()
		scanDests := buildScanDestWithVector(elem, cs)

		var score float64
		allDests := append(scanDests, &score)

		if err := rows.Scan(allDests...); err != nil {
			return nil, fmt.Errorf("strata vector search: scan: %w", err)
		}

		pkVal := elem.Field(cs.pkIndex).Interface()
		id := fmt.Sprintf("%v", pkVal)

		value := elem.Addr().Interface()

		// Back-fill caches (strip vector field so it doesn't appear in L1/L2).
		if ds.l2 != nil {
			if err := ds.setL2(ctx, cs, id, value); err != nil {
				// Log but do not fail VectorSearch on cache warming errors.
				if ds.logger != nil {
					ds.logger.Warn("strata: vector search l2 warm failed",
						"schema", cs.Name, "id", id, "err", err)
				}
			}
		}
		if ds.l1 != nil {
			ds.setL1(cs, cs.l1Prefix+id, value)
		}

		results = append(results, SimilarityResult{
			ID:    id,
			Score: score,
			Value: value,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("strata vector search: rows error: %w", err)
	}

	if results == nil {
		results = []SimilarityResult{}
	}
	return results, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// ReEmbed
// ─────────────────────────────────────────────────────────────────────────────

// ReEmbed walks all L3 records for schemaName in batches of 100, re-embedding
// the textFieldName field using the current EmbeddingProvider. It is resumable:
// if interrupted, a subsequent call picks up where it left off using the
// reembed_progress counter in strata_schema_meta.
//
// On completion it updates strata_schema_meta with the new model ID and
// dimension. If the dimension changed, it also rebuilds the vector index.
//
// Returns ErrReEmbedAlreadyRunning if another migration is in progress,
// ErrReEmbedTextFieldMissing if textFieldName does not match any struct field,
// or a summary error if some records failed to embed.
func (ds *DataStore) ReEmbed(ctx context.Context, schemaName, textFieldName string) error {
	// Cheap input validation first — no external dependencies required.
	if ctx == nil {
		return ErrNilContext
	}

	cs, err := ds.registry.get(schemaName)
	if err != nil {
		return err
	}
	if !cs.hasVectorFields || cs.vectorField == nil {
		return ErrNoVectorField
	}

	provider := ds.cfg.EmbeddingProvider
	if provider == nil {
		return ErrNoEmbeddingProvider
	}

	// Validate that textFieldName maps to a column in this schema before
	// touching external state.
	textColName := ""
	for _, col := range cs.columns {
		if strings.EqualFold(col.FieldName, textFieldName) ||
			strings.EqualFold(col.Name, textFieldName) {
			textColName = col.Name
			break
		}
	}
	if textColName == "" {
		return fmt.Errorf("%w: %q not found in schema %q", ErrReEmbedTextFieldMissing, textFieldName, schemaName)
	}

	if ds.l3 == nil {
		return ErrL3Unavailable
	}

	// Ensure meta table exists and read current state.
	if err := ds.ensureSchemaMetaTable(ctx); err != nil {
		return err
	}

	meta, err := ds.readSchemaMeta(ctx, schemaName)
	if err != nil {
		return err
	}
	if meta != nil && meta.ReembedStatus == "running" {
		return ErrReEmbedAlreadyRunning
	}

	// Determine resumption offset.
	startOffset := 0
	if meta != nil {
		startOffset = meta.ReembedProgress
	}

	// Mark as running.
	if err := ds.updateSchemaMetaReembedStatus(ctx, schemaName, "running", startOffset); err != nil {
		return err
	}

	const batchSize = 100
	vectorColName := cs.vectorField.Name
	pkColName := cs.pkColumn.Name
	tableName := cs.tableName

	progress := startOffset
	partialFailures := 0

	for {
		// Fetch next batch: PK + text field, ordered by PK.
		fetchSQL := fmt.Sprintf(
			`SELECT %s, %s FROM %s ORDER BY %s LIMIT %d OFFSET %d`,
			pkColName, textColName, tableName, pkColName, batchSize, progress,
		)
		rows, err := ds.l3.Query(ctx, fetchSQL, nil)
		if err != nil {
			_ = ds.updateSchemaMetaReembedStatus(ctx, schemaName, "failed", progress)
			return fmt.Errorf("reembed batch query at offset %d: %w", progress, err)
		}

		type record struct {
			pk   string
			text string
		}
		var batch []record
		for rows.Next() {
			var pkVal, textVal string
			if err := rows.Scan(&pkVal, &textVal); err != nil {
				rows.Close()
				_ = ds.updateSchemaMetaReembedStatus(ctx, schemaName, "failed", progress)
				return fmt.Errorf("reembed scan: %w", err)
			}
			batch = append(batch, record{pk: pkVal, text: textVal})
		}
		if err := rows.Err(); err != nil {
			_ = ds.updateSchemaMetaReembedStatus(ctx, schemaName, "failed", progress)
			return fmt.Errorf("reembed rows error: %w", err)
		}
		rows.Close()

		if len(batch) == 0 {
			break // done
		}

		for _, rec := range batch {
			vec, embedErr := provider.Embed(ctx, rec.text)
			if embedErr != nil {
				partialFailures++
				if ds.logger != nil {
					ds.logger.Warn("strata: reembed failed for record",
						"schema", schemaName, "id", rec.pk, "err", embedErr)
				}
				progress++
				continue
			}

			updateSQL := fmt.Sprintf(
				`UPDATE %s SET %s = $1::vector WHERE %s = $2`,
				tableName, vectorColName, pkColName,
			)
			if err := ds.l3.Exec(ctx, updateSQL, []any{vec, rec.pk}); err != nil {
				partialFailures++
				if ds.logger != nil {
					ds.logger.Warn("strata: reembed update failed",
						"schema", schemaName, "id", rec.pk, "err", err)
				}
			}
			progress++
		}

		// Persist progress after each batch.
		if err := ds.updateSchemaMetaReembedStatus(ctx, schemaName, "running", progress); err != nil {
			return fmt.Errorf("reembed persist progress: %w", err)
		}

		percentage := float64(progress) / float64(progress+batchSize) * 100
		if ds.logger != nil {
			ds.logger.Info("strata: reembed progress",
				"schema", schemaName,
				"processed", progress,
				"progress_pct", fmt.Sprintf("%.1f%%", percentage))
		}

		// Check context cancellation between batches.
		select {
		case <-ctx.Done():
			_ = ds.updateSchemaMetaReembedStatus(ctx, schemaName, "failed", progress)
			return ctx.Err()
		default:
		}
	}

	// Update model metadata.
	newModel := provider.ModelID()
	newDim := provider.Dimensions()
	if err := ds.updateSchemaMetaModel(ctx, schemaName, newModel, newDim); err != nil {
		return fmt.Errorf("reembed update meta: %w", err)
	}

	// Update compiledSchema dimension so subsequent migrations use the new value.
	cs.vectorDimension = newDim

	// Rebuild indexes for this schema (drop old, create new).
	if err := ds.rebuildVectorIndexes(ctx, cs); err != nil {
		// Log but do not fail — data is re-embedded, just index might be stale.
		if ds.logger != nil {
			ds.logger.Warn("strata: reembed index rebuild failed",
				"schema", schemaName, "err", err)
		}
	}

	// Mark complete.
	if err := ds.updateSchemaMetaReembedStatus(ctx, schemaName, "idle", progress); err != nil {
		return err
	}

	if partialFailures > 0 {
		return fmt.Errorf("strata: reembed completed %d records with %d partial failure(s)", progress, partialFailures)
	}
	return nil
}

// rebuildVectorIndexes drops and recreates all vector indexes for the schema.
func (ds *DataStore) rebuildVectorIndexes(ctx context.Context, cs *compiledSchema) error {
	for _, idx := range cs.Indexes {
		if idx.Type != IndexIVFFlat && idx.Type != IndexHNSW {
			continue
		}
		idxName := idx.Name
		if idxName == "" {
			idxName = fmt.Sprintf("idx_%s_%s", cs.tableName, strings.Join(idx.Fields, "_"))
		}
		dropSQL := fmt.Sprintf("DROP INDEX IF EXISTS %s", idxName)
		if err := ds.l3.Exec(ctx, dropSQL, nil); err != nil {
			return fmt.Errorf("drop index %s: %w", idxName, err)
		}
		createSQL := buildOneIndexDDL(cs, idx)
		if err := ds.l3.Exec(ctx, createSQL, nil); err != nil {
			return fmt.Errorf("create index %s: %w", idxName, err)
		}
	}
	return nil
}
