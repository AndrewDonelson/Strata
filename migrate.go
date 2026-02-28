package strata

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/AndrewDonelson/strata/internal/l3"
	"github.com/jackc/pgx/v5"
)

const migrationTable = "_strata_migrations"

// MigrationRecord describes a single applied migration.
type MigrationRecord struct {
	ID        int
	Schema    string
	FileName  string
	AppliedAt time.Time
}

// Migrate applies all pending schema changes to PostgreSQL (idempotent).
func (ds *DataStore) Migrate(ctx context.Context) error {
	if ds.l3 == nil {
		return nil
	}
	if err := ds.ensureMigrationTable(ctx); err != nil {
		return err
	}
	for _, cs := range ds.registry.all() {
		if err := ds.migrateSchema(ctx, cs); err != nil {
			return fmt.Errorf("migrate schema %q: %w", cs.Name, err)
		}
	}
	return nil
}

// MigrateFrom applies SQL migration files from dir in NNN_description.sql order.
func (ds *DataStore) MigrateFrom(ctx context.Context, dir string) error {
	if ds.l3 == nil {
		return ErrL3Unavailable
	}
	if err := ds.ensureMigrationTable(ctx); err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("migrate-from readdir %q: %w", dir, err)
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)

	for _, fname := range files {
		applied, err := ds.isMigrationApplied(ctx, fname)
		if err != nil {
			return err
		}
		if applied {
			continue
		}
		content, err := os.ReadFile(filepath.Join(dir, fname))
		if err != nil {
			return fmt.Errorf("migrate-from read %q: %w", fname, err)
		}
		tx, err := ds.l3.BeginTx(ctx)
		if err != nil {
			return fmt.Errorf("migrate-from begin tx %q: %w", fname, err)
		}
		if _, err := tx.Exec(ctx, string(content)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("migrate-from exec %q: %w", fname, err)
		}
		if err := ds.recordMigration(ctx, tx, "file", fname); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("migrate-from commit %q: %w", fname, err)
		}
	}
	return nil
}

// MigrationStatus returns current migration state.
func (ds *DataStore) MigrationStatus(ctx context.Context) ([]MigrationRecord, error) {
	if ds.l3 == nil {
		return nil, ErrL3Unavailable
	}
	rows, err := ds.l3.Query(ctx,
		fmt.Sprintf("SELECT id, schema_name, file_name, applied_at FROM %s ORDER BY id", migrationTable),
		nil,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []MigrationRecord
	for rows.Next() {
		var r MigrationRecord
		if err := rows.Scan(&r.ID, &r.Schema, &r.FileName, &r.AppliedAt); err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

func (ds *DataStore) ensureMigrationTable(ctx context.Context) error {
	sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
id          SERIAL PRIMARY KEY,
schema_name TEXT NOT NULL,
file_name   TEXT NOT NULL DEFAULT '',
applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
)`, migrationTable)
	return ds.l3.Exec(ctx, sql, nil)
}

func (ds *DataStore) migrateSchema(ctx context.Context, cs *compiledSchema) error {
	exists, err := ds.tableExists(ctx, cs.tableName)
	if err != nil {
		return err
	}
	if !exists {
		return ds.createTable(ctx, cs)
	}
	return ds.alterTable(ctx, cs)
}

func (ds *DataStore) tableExists(ctx context.Context, table string) (bool, error) {
	sql := `SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name=$1 LIMIT 1`
	var dummy int
	err := ds.l3.QueryRow(ctx, sql, []any{table}).Scan(&dummy)
	if err != nil {
		if strings.Contains(err.Error(), "no rows") || err == pgx.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (ds *DataStore) createTable(ctx context.Context, cs *compiledSchema) error {
	ddl := buildCreateTableDDL(cs)
	tx, err := ds.l3.BeginTx(ctx)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, ddl); err != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("create table %q: %w", cs.tableName, err)
	}
	for _, idx := range buildIndexDDL(cs) {
		if _, err := tx.Exec(ctx, idx); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("create index on %q: %w", cs.tableName, err)
		}
	}
	if err := ds.recordMigration(ctx, tx, cs.Name, "create_table"); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

func (ds *DataStore) alterTable(ctx context.Context, cs *compiledSchema) error {
	existing, err := ds.existingColumns(ctx, cs.tableName)
	if err != nil {
		return err
	}
	var stmts []string
	for _, col := range cs.columns {
		if _, seen := existing[col.Name]; seen {
			continue
		}
		stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", cs.tableName, buildColumnDef(col))
		stmts = append(stmts, stmt)
	}
	if len(stmts) == 0 {
		return nil
	}
	tx, err := ds.l3.BeginTx(ctx)
	if err != nil {
		return err
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(ctx, stmt); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("alter table %q: %w", cs.tableName, err)
		}
	}
	if err := ds.recordMigration(ctx, tx, cs.Name, "alter_table"); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

func (ds *DataStore) existingColumns(ctx context.Context, table string) (map[string]struct{}, error) {
	sql := `SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name=$1`
	rows, err := ds.l3.Query(ctx, sql, []any{table})
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]struct{})
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out[name] = struct{}{}
	}
	return out, rows.Err()
}

func (ds *DataStore) recordMigration(ctx context.Context, tx pgx.Tx, schema, fileName string) error {
	_, err := tx.Exec(ctx,
		fmt.Sprintf("INSERT INTO %s (schema_name, file_name) VALUES ($1, $2)", migrationTable),
		schema, fileName,
	)
	return err
}

func (ds *DataStore) isMigrationApplied(ctx context.Context, fileName string) (bool, error) {
	sql := fmt.Sprintf("SELECT 1 FROM %s WHERE file_name=$1 LIMIT 1", migrationTable)
	var dummy int
	err := ds.l3.QueryRow(ctx, sql, []any{fileName}).Scan(&dummy)
	if err != nil {
		if err == pgx.ErrNoRows || strings.Contains(err.Error(), "no rows") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// DDL generation

func buildCreateTableDDL(cs *compiledSchema) string {
	var cols []string
	for _, col := range cs.columns {
		cols = append(cols, buildColumnDef(col))
	}
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n  %s\n)",
		cs.tableName, strings.Join(cols, ",\n  "))
}

func buildColumnDef(col l3.ColumnDef) string {
	def := col.Name + " " + col.SQLType
	if col.IsPK {
		def += " PRIMARY KEY"
	} else {
		if col.IsUnique {
			def += " UNIQUE"
		}
		if !col.IsNullable {
			def += " NOT NULL"
		}
	}
	if col.DefaultValue != "" {
		def += " DEFAULT " + col.DefaultValue
	} else if (col.IsAutoNowAdd || col.IsAutoNow) && col.SQLType == "TIMESTAMPTZ" {
		def += " DEFAULT now()"
	}
	return def
}

func buildIndexDDL(cs *compiledSchema) []string {
	var out []string
	for _, col := range cs.columns {
		if col.IsIndexed && !col.IsPK && !col.IsUnique {
			name := fmt.Sprintf("idx_%s_%s", cs.tableName, col.Name)
			out = append(out, fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (%s)", name, cs.tableName, col.Name))
		}
	}
	for _, idx := range cs.Indexes {
		name := idx.Name
		if name == "" {
			name = fmt.Sprintf("idx_%s_%s", cs.tableName, strings.Join(idx.Fields, "_"))
		}
		unique := ""
		if idx.Unique {
			unique = "UNIQUE "
		}
		out = append(out, fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS %s ON %s (%s)",
			unique, name, cs.tableName, strings.Join(idx.Fields, ", ")))
	}
	return out
}
