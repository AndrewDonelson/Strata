// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// router.go — tri-tier routing logic that moves data between L1 (in-memory),
// L2 (Redis), and L3 (PostgreSQL). Implements write-through, write-behind,
// and L1-async write modes, cache population on read, and cascade delete.

package strata

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	l2pkg "github.com/AndrewDonelson/strata/internal/l2"
	l3pkg "github.com/AndrewDonelson/strata/internal/l3"
	"github.com/jackc/pgx/v5"
)

// ────────────────────────────────────────────────────────────────────────────
// Column name helpers
// ────────────────────────────────────────────────────────────────────────────

// colNames returns the DB column names for all non-omitted columns.
// Excludes OmitCache fields (including vector fields) — used for L1/L2 operations
// and Search/WarmCache queries where vector data is not needed.
func colNames(cs *compiledSchema) []string {
	names := make([]string, 0, len(cs.columns))
	for _, col := range cs.columns {
		if !col.OmitCache {
			names = append(names, col.Name)
		}
	}
	return names
}

// persistedColNames returns the DB column names of every persisted column,
// including omit_cache and vector columns. Used wherever Strata reads a full
// record from Postgres (Get on a cache miss, VectorSearch).
func persistedColNames(cs *compiledSchema) []string {
	names := make([]string, len(cs.columns))
	for i, col := range cs.columns {
		names[i] = col.Name
	}
	return names
}

// stripUncached returns a shallow copy of value with every omit_cache field
// (vector fields included) zeroed, so they never reach L1 or L2. If the schema
// has no such fields the original value is returned unchanged.
func stripUncached(cs *compiledSchema, value any) any {
	if !cs.hasUncachedFields {
		return value
	}
	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	copy := reflect.New(v.Type()).Elem()
	copy.Set(v)
	for _, col := range cs.columns {
		if col.OmitCache {
			f := copy.FieldByName(col.FieldName)
			if f.IsValid() && f.CanSet() {
				f.Set(reflect.Zero(f.Type()))
			}
		}
	}
	return copy.Addr().Interface()
}

// ────────────────────────────────────────────────────────────────────────────
// Read path
// ────────────────────────────────────────────────────────────────────────────

// routerGet attempts L1 → L2 → L3 and back-fills upper tiers on a miss.
func (ds *DataStore) routerGet(ctx context.Context, cs *compiledSchema, id string, dest any) error {
	l1Key := cs.l1Prefix + id

	// L1 hit
	if ds.l1 != nil {
		if raw, ok := ds.l1.Get(l1Key); ok {
			if populated, err := populateDest(raw, dest); err == nil && populated {
				ds.metrics.RecordHit(cs.Name, "l1")
				return ds.loadHiddenFields(ctx, cs, id, dest)
			}
		}
	}
	ds.metrics.RecordMiss(cs.Name, "l1")

	// L2 — ErrMiss means key absent (fall through); other errors also fall through.
	if ds.l2 != nil {
		if err := ds.l2.GetP(ctx, cs.l2Prefix, id, dest); err == nil {
			ds.metrics.RecordHit(cs.Name, "l2")
			// back-fill L1
			if ds.l1 != nil {
				ds.setL1(cs, l1Key, dest)
			}
			return ds.loadHiddenFields(ctx, cs, id, dest)
		} else if !errors.Is(err, l2pkg.ErrMiss) {
			// Genuine Redis error — log and fall through to L3.
			if ds.logger != nil {
				ds.logger.Warn("strata: l2 get error", "schema", cs.Name, "id", id, "err", err)
			}
		}
	}
	ds.metrics.RecordMiss(cs.Name, "l2")

	// L3 read
	if ds.l3 != nil {
		if err := ds.readFromL3(ctx, cs, id, dest); err != nil {
			return err
		}
		ds.metrics.RecordHit(cs.Name, "l3")
		// back-fill L2 then L1
		if ds.l2 != nil {
			_ = ds.setL2(ctx, cs, id, dest)
		}
		if ds.l1 != nil {
			ds.setL1(cs, l1Key, dest)
		}
		return nil
	}
	return ErrNotFound
}

// ────────────────────────────────────────────────────────────────────────────
// Write path
// ────────────────────────────────────────────────────────────────────────────

func (ds *DataStore) routerSet(ctx context.Context, cs *compiledSchema, id string, value any) error {
	switch cs.WriteMode {
	case WriteBehind:
		return ds.routerSetWriteBehind(ctx, cs, id, value)
	case WriteThroughL1Async:
		return ds.routerSetL1Async(ctx, cs, id, value)
	default: // WriteThrough
		return ds.routerSetWriteThrough(ctx, cs, id, value)
	}
}

func (ds *DataStore) routerSetWriteThrough(ctx context.Context, cs *compiledSchema, id string, value any) error {
	// L3 first
	if ds.l3 != nil {
		if err := ds.writeToL3(ctx, cs, value); err != nil {
			ds.dropOnConflict(ctx, cs, id, err)
			return err
		}
	}
	ds.afterL3Set(ctx, cs, id, value)
	return nil
}

// dropOnConflict evicts id from L1 and L2 (here and on other nodes) after a
// version conflict. The conflict proves the cached copy may be stale; without
// this, a "re-read and retry" loop could keep reading the same stale version.
func (ds *DataStore) dropOnConflict(ctx context.Context, cs *compiledSchema, id string, err error) {
	if !errors.Is(err, ErrConflict) {
		return
	}
	if ds.l1 != nil {
		ds.l1.Delete(cs.l1Prefix + id)
	}
	if ds.l2 != nil {
		_ = ds.l2.DeleteP(ctx, cs.l2Prefix, id)
	}
	if ds.sync != nil {
		ds.sync.publishInvalidation(ctx, cs.Name, id, "delete")
	}
}

// afterL3Set propagates a value that is already durable in L3 to L4, L2 and L1,
// then tells other nodes to drop their copy.
func (ds *DataStore) afterL3Set(ctx context.Context, cs *compiledSchema, id string, value any) {
	ds.syncToL4(ctx, cs, id, value)
	if ds.l2 != nil {
		_ = ds.setL2(ctx, cs, id, value)
	}
	if ds.l1 != nil {
		ds.setL1(cs, cs.l1Prefix+id, value)
	}
	if ds.sync != nil {
		ds.sync.publishInvalidation(ctx, cs.Name, id, "set")
	}
}

// afterL3Delete removes a record that is already deleted in L3 from every
// other tier.
func (ds *DataStore) afterL3Delete(ctx context.Context, cs *compiledSchema, id string) {
	if ds.l1 != nil {
		ds.l1.Delete(cs.l1Prefix + id)
	}
	if ds.l2 != nil {
		_ = ds.l2.DeleteP(ctx, cs.l2Prefix, id)
	}
	ds.revokeFromL4(ctx, cs, id)
	if ds.sync != nil {
		ds.sync.publishInvalidation(ctx, cs.Name, id, "delete")
	}
}

func (ds *DataStore) routerSetWriteBehind(ctx context.Context, cs *compiledSchema, id string, value any) error {
	// L1 immediately
	if ds.l1 != nil {
		ds.setL1(cs, cs.l1Prefix+id, value)
	}
	// L2 immediately
	if ds.l2 != nil {
		_ = ds.setL2(ctx, cs, id, value)
	}
	// Queue for async L3 flush
	if ds.sync != nil {
		ds.sync.queueDirty(cs.Name, id, value)
	}
	return nil
}

func (ds *DataStore) routerSetL1Async(ctx context.Context, cs *compiledSchema, id string, value any) error {
	// L3 + L2 synchronously
	if ds.l3 != nil {
		if err := ds.writeToL3(ctx, cs, value); err != nil {
			ds.dropOnConflict(ctx, cs, id, err)
			return err
		}
	}
	// L4 — sync after confirmed L3 write
	ds.syncToL4(ctx, cs, id, value)
	if ds.l2 != nil {
		_ = ds.setL2(ctx, cs, id, value)
	}
	// L1 asynchronously via pooled worker — avoids unbounded goroutine creation.
	if ds.l1 != nil {
		ds.sync.enqueueL1Write(cs, cs.l1Prefix+id, value)
	}
	if ds.sync != nil {
		ds.sync.publishInvalidation(ctx, cs.Name, id, "set")
	}
	return nil
}

// ────────────────────────────────────────────────────────────────────────────
// Delete path
// ────────────────────────────────────────────────────────────────────────────

func (ds *DataStore) routerDelete(ctx context.Context, cs *compiledSchema, id string) error {
	l1Key := cs.l1Prefix + id
	if ds.l1 != nil {
		ds.l1.Delete(l1Key)
	}
	if ds.l2 != nil {
		_ = ds.l2.DeleteP(ctx, cs.l2Prefix, id)
	}
	if ds.l3 != nil {
		if err := ds.l3.DeleteByID(ctx, cs.tableName, cs.pkColumn.Name, id); err != nil {
			return err
		}
	}
	// L4 — revoke the record if SyncDeletes is enabled for this schema
	ds.revokeFromL4(ctx, cs, id)
	if ds.sync != nil {
		ds.sync.publishInvalidation(ctx, cs.Name, id, "delete")
	}
	return nil
}

// ────────────────────────────────────────────────────────────────────────────
// Search path
// ────────────────────────────────────────────────────────────────────────────

func (ds *DataStore) routerSearch(ctx context.Context, cs *compiledSchema, q *Query, destSlice any) error {
	if ds.l3 == nil {
		return ErrL3Unavailable
	}
	return ds.searchWith(ctx, ds.l3.Query, cs, q, destSlice)
}

// searchWith runs q through query (the l3Backend or a transaction) and appends
// decoded rows to destSlice. omit_cache columns are not selected.
func (ds *DataStore) searchWith(ctx context.Context, query func(ctx context.Context, sql string, args []any) (pgx.Rows, error), cs *compiledSchema, q *Query, destSlice any) error {
	if q == nil {
		empty := Q().Build()
		q = &empty
	}
	cols := colNames(cs)
	sql, args := q.ToSQL(cs.tableName, cols, 100)

	rows, err := query(ctx, sql, args)
	if err != nil {
		return err
	}
	defer rows.Close()

	sliceVal := reflect.ValueOf(destSlice).Elem()
	elemType := sliceVal.Type().Elem()
	if elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
	}

	for rows.Next() {
		elem := reflect.New(elemType).Elem()
		dests := buildScanDest(elem, cs)
		if err := rows.Scan(dests...); err != nil {
			return err
		}
		if err := ds.decryptFields(cs, elem); err != nil {
			return err
		}
		sliceVal.Set(reflect.Append(sliceVal, elem))
	}
	return rows.Err()
}

// ────────────────────────────────────────────────────────────────────────────
// Tier helpers
// ────────────────────────────────────────────────────────────────────────────

func (ds *DataStore) setL1(cs *compiledSchema, key string, value any) {
	// Strip omit_cache fields (incl. vectors) — they must not be stored in L1.
	if cs.hasUncachedFields {
		value = stripUncached(cs, value)
	}
	ttl := cs.L1.TTL
	if ttl == 0 {
		ttl = ds.cfg.DefaultL1TTL
	}
	ds.l1.Set(key, value, ttl)
}

func (ds *DataStore) setL2(ctx context.Context, cs *compiledSchema, id string, value any) error {
	// Strip omit_cache fields (incl. vectors) — they must not be stored in L2 (Redis).
	if cs.hasUncachedFields {
		value = stripUncached(cs, value)
	}
	ttl := cs.L2.TTL
	if ttl == 0 {
		ttl = ds.cfg.DefaultL2TTL
	}
	return ds.l2.SetP(ctx, cs.l2Prefix, id, value, ttl)
}

func (ds *DataStore) readFromL3(ctx context.Context, cs *compiledSchema, id string, dest any) error {
	return ds.readRow(ctx, ds.l3.QueryRow, cs, id, dest, "")
}

// rowQuerier is satisfied by l3Backend.QueryRow and by an adapter over pgx.Tx,
// so the same read code runs inside and outside a transaction.
type rowQuerier func(ctx context.Context, sql string, args []any) pgx.Row

// readRow reads the full persisted record (every column) for id into dest.
// suffix is appended to the SQL, e.g. " FOR UPDATE".
func (ds *DataStore) readRow(ctx context.Context, query rowQuerier, cs *compiledSchema, id string, dest any, suffix string) error {
	sql := fmt.Sprintf("SELECT %s FROM %s WHERE %s = $1%s",
		strings.Join(persistedColNames(cs), ", "), cs.tableName, cs.pkColumn.Name, suffix)
	destVal := reflect.ValueOf(dest)
	if destVal.Kind() == reflect.Ptr {
		destVal = destVal.Elem()
	}
	if err := query(ctx, sql, []any{id}).Scan(buildPersistedScanDest(destVal, cs)...); err != nil {
		if isNoRowsError(err) {
			return ErrNotFound
		}
		return err
	}
	return ds.decryptColumns(cs.columns, destVal)
}

// loadHiddenFields fills dest's non-vector omit_cache fields after a cache hit.
// Those fields are never cached, so without this a Get served from L1/L2 would
// return them zeroed while the same Get served from L3 would not. A missing row
// (e.g. a write-behind entry not yet flushed) leaves them zero.
func (ds *DataStore) loadHiddenFields(ctx context.Context, cs *compiledSchema, id string, dest any) error {
	if len(cs.hiddenColumns) == 0 || ds.l3 == nil {
		return nil
	}
	destVal := reflect.ValueOf(dest)
	if destVal.Kind() == reflect.Ptr {
		destVal = destVal.Elem()
	}
	names := make([]string, len(cs.hiddenColumns))
	for i, col := range cs.hiddenColumns {
		names[i] = col.Name
	}
	sql := fmt.Sprintf("SELECT %s FROM %s WHERE %s = $1",
		strings.Join(names, ", "), cs.tableName, cs.pkColumn.Name)
	if err := ds.l3.QueryRow(ctx, sql, []any{id}).Scan(scanDests(destVal, cs.hiddenColumns)...); err != nil {
		if isNoRowsError(err) {
			return nil
		}
		return err
	}
	return ds.decryptColumns(cs.hiddenColumns, destVal)
}

// l3WriteKind selects the SQL a write uses.
type l3WriteKind int

const (
	l3Upsert l3WriteKind = iota // INSERT ... ON CONFLICT DO UPDATE (versioned: create-or-compare)
	l3Insert                    // plain INSERT; any unique violation → ErrAlreadyExists
)

// rowsExecer runs a write and reports affected rows. It's implemented over the
// l3Backend (autocommit) and over pgx.Tx, so one write path serves both.
type rowsExecer func(ctx context.Context, sql string, args []any) (int64, error)

func (ds *DataStore) writeToL3(ctx context.Context, cs *compiledSchema, value any) error {
	if ds.l3 == nil {
		return ErrL3Unavailable
	}
	if cs.versionColumn == nil {
		// Unversioned upsert outside a transaction keeps using Upsert directly.
		return ds.writeL3(ctx, nil, cs, value, l3Upsert, nil)
	}
	return ds.writeL3(ctx, ds.l3.ExecRows, cs, value, l3Upsert, nil)
}

// insertToL3 writes value with a plain INSERT outside any transaction.
func (ds *DataStore) insertToL3(ctx context.Context, cs *compiledSchema, value any) error {
	if ds.l3 == nil {
		return ErrL3Unavailable
	}
	return ds.writeL3(ctx, ds.l3.ExecRows, cs, value, l3Insert, nil)
}

// writeL3 persists every column of value (omit_cache and vector fields
// included). exec == nil means "unversioned upsert via l3Backend.Upsert".
//
// Versioned schemas (strata:"version"):
//   - version 0 on an upsert → create with version 1; ErrConflict if the id exists
//   - version N on an upsert → UPDATE ... WHERE version = N, bump to N+1;
//     ErrConflict if no row matched
//   - insert → version 1
//
// The caller's version field is bumped only after the write succeeds. Inside a
// transaction pass undo: each bump is recorded so it can be reverted if the
// transaction doesn't commit.
func (ds *DataStore) writeL3(ctx context.Context, exec rowsExecer, cs *compiledSchema, value any, kind l3WriteKind, undo *versionUndo) error {
	val := reflect.ValueOf(value)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	now := ds.cfg.Clock.Now()

	// Handle auto_now / auto_now_add
	for _, col := range cs.columns {
		if col.IsAutoNow || col.IsAutoNowAdd {
			f := val.FieldByName(col.FieldName)
			if !f.IsValid() || !f.CanSet() {
				continue
			}
			if col.IsAutoNow {
				f.Set(reflect.ValueOf(now))
			} else if col.IsAutoNowAdd {
				if f.Type() == reflect.TypeOf(time.Time{}) {
					if f.Interface().(time.Time).IsZero() {
						f.Set(reflect.ValueOf(now))
					}
				}
			}
		}
	}

	// Encrypt fields before writing
	if err := ds.encryptFields(cs, val); err != nil {
		return err
	}
	defer func() {
		// Restore plaintext so caller's struct is not left with ciphertext
		_ = ds.decryptFields(cs, val)
	}()

	// Every column is persisted — omit_cache only keeps a field out of L1/L2.
	cols := make([]string, 0, len(cs.columns))
	vals := make([]any, 0, len(cs.columns))
	versionIdx := -1
	var pkVal any
	for _, col := range cs.columns {
		f := val.FieldByName(col.FieldName)
		if !f.IsValid() {
			continue
		}
		if col.IsVersion {
			versionIdx = len(vals)
		}
		if col.IsPK {
			pkVal = f.Interface()
		}
		cols = append(cols, col.Name)
		vals = append(vals, f.Interface())
	}

	if cs.versionColumn == nil || versionIdx < 0 {
		switch {
		case kind == l3Insert:
			_, err := exec(ctx, l3pkg.InsertSQL(cs.tableName, cols), vals)
			return mapInsertErr(cs, err)
		case exec == nil:
			return ds.l3.Upsert(ctx, cs.tableName, cols, vals, cs.pkColumn.Name)
		default:
			_, err := exec(ctx, l3pkg.UpsertSQL(cs.tableName, cols, cs.pkColumn.Name), vals)
			if err != nil {
				return fmt.Errorf("l3 upsert %s: %w", cs.tableName, err)
			}
			return nil
		}
	}

	versionField := val.FieldByName(cs.versionColumn.FieldName)
	if !versionField.CanSet() {
		return fmt.Errorf("%w: versioned schema %s needs a pointer value so the version can be updated",
			ErrInvalidModel, cs.Name)
	}
	oldVersion := versionField.Int()

	if kind == l3Insert || oldVersion == 0 {
		vals[versionIdx] = int64(1)
		sql := l3pkg.InsertSQL(cs.tableName, cols)
		if kind == l3Upsert {
			sql += fmt.Sprintf(" ON CONFLICT (%s) DO NOTHING", cs.pkColumn.Name)
		}
		n, err := exec(ctx, sql, vals)
		if kind == l3Insert {
			if err := mapInsertErr(cs, err); err != nil {
				return err
			}
		} else if err != nil {
			return fmt.Errorf("l3 create %s: %w", cs.tableName, err)
		}
		if n == 0 {
			return fmt.Errorf("%w: %s %v already exists (version 0 means create)", ErrConflict, cs.Name, pkVal)
		}
		undo.bump(versionField, 1)
		return nil
	}

	// UPDATE t SET a = $1, ..., version = $n WHERE pk = $n+1 AND version = $n+2
	sets := make([]string, 0, len(cols))
	args := make([]any, 0, len(cols)+2)
	for i, c := range cols {
		if c == cs.pkColumn.Name {
			continue
		}
		v := vals[i]
		if i == versionIdx {
			v = oldVersion + 1
		}
		args = append(args, v)
		sets = append(sets, fmt.Sprintf("%s = $%d", c, len(args)))
	}
	args = append(args, pkVal, oldVersion)
	sql := fmt.Sprintf("UPDATE %s SET %s WHERE %s = $%d AND %s = $%d",
		cs.tableName, strings.Join(sets, ", "),
		cs.pkColumn.Name, len(args)-1, cs.versionColumn.Name, len(args))
	n, err := exec(ctx, sql, args)
	if err != nil {
		return fmt.Errorf("l3 update %s: %w", cs.tableName, err)
	}
	if n == 0 {
		return fmt.Errorf("%w: %s %v is not at version %d", ErrConflict, cs.Name, pkVal, oldVersion)
	}
	undo.bump(versionField, oldVersion+1)
	return nil
}

// versionUndo records version bumps made inside a transaction so they can be
// reverted when it rolls back. A nil *versionUndo just applies the bump.
type versionUndo []func()

func (u *versionUndo) bump(field reflect.Value, to int64) {
	if u != nil {
		old := field.Int()
		*u = append(*u, func() { field.SetInt(old) })
	}
	field.SetInt(to)
}

// revert restores every recorded version in reverse order and clears the log.
func (u *versionUndo) revert() {
	for i := len(*u) - 1; i >= 0; i-- {
		(*u)[i]()
	}
	*u = nil
}

func mapInsertErr(cs *compiledSchema, err error) error {
	if err == nil {
		return nil
	}
	if l3pkg.IsUniqueViolation(err) {
		return fmt.Errorf("%w: %s: %v", ErrAlreadyExists, cs.Name, err)
	}
	return fmt.Errorf("l3 insert %s: %w", cs.tableName, err)
}

// ────────────────────────────────────────────────────────────────────────────
// Scan helpers
// ────────────────────────────────────────────────────────────────────────────

func buildScanDest(val reflect.Value, cs *compiledSchema) []any {
	dests := make([]any, 0, len(cs.columns))
	for _, col := range cs.columns {
		if col.OmitCache {
			continue
		}
		f := val.FieldByName(col.FieldName)
		if !f.IsValid() || !f.CanAddr() {
			var dummy any
			dests = append(dests, &dummy)
			continue
		}
		dests = append(dests, f.Addr().Interface())
	}
	return dests
}

// buildPersistedScanDest returns scan targets for every persisted column, in
// the same order as persistedColNames.
func buildPersistedScanDest(val reflect.Value, cs *compiledSchema) []any {
	return scanDests(val, cs.columns)
}

// scanDests returns scan targets for cols, in order. Columns without an
// addressable field get a throwaway target.
func scanDests(val reflect.Value, cols []l3pkg.ColumnDef) []any {
	dests := make([]any, 0, len(cols))
	for _, col := range cols {
		f := val.FieldByName(col.FieldName)
		if !f.IsValid() || !f.CanAddr() {
			var dummy any
			dests = append(dests, &dummy)
			continue
		}
		dests = append(dests, f.Addr().Interface())
	}
	return dests
}

func isNoRowsError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "no rows")
}

// populateDest copies a value from raw (any) into dest via reflection.
func populateDest(raw any, dest any) (bool, error) {
	if raw == nil {
		return false, nil
	}
	rawVal := reflect.ValueOf(raw)
	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Ptr {
		return false, nil
	}
	destElem := destVal.Elem()
	if rawVal.Type().AssignableTo(destElem.Type()) {
		destElem.Set(rawVal)
		return true, nil
	}
	if rawVal.Kind() == reflect.Ptr && rawVal.Elem().Type().AssignableTo(destElem.Type()) {
		destElem.Set(rawVal.Elem())
		return true, nil
	}
	return false, nil
}

// encryptedPrefix marks a stored value as Strata ciphertext ("enc:v1:" +
// base64(nonce||ciphertext)), so decryption never mistakes plaintext for it.
const encryptedPrefix = "enc:v1:"

// encryptFields AES-encrypts any field tagged `encrypted`.
func (ds *DataStore) encryptFields(cs *compiledSchema, val reflect.Value) error {
	if ds.encryptor == nil {
		return nil
	}
	for _, col := range cs.columns {
		if !col.IsEncrypted {
			continue
		}
		f := val.FieldByName(col.FieldName)
		if !f.IsValid() || !f.CanSet() || f.Kind() != reflect.String {
			continue
		}
		plain := f.String()
		if plain == "" {
			continue
		}
		cipher, err := ds.encryptor.Encrypt([]byte(plain))
		if err != nil {
			return fmt.Errorf("strata: encrypt field %s: %w", col.FieldName, err)
		}
		// Base64 so the ciphertext is valid UTF-8 for the TEXT column.
		f.SetString(encryptedPrefix + base64.StdEncoding.EncodeToString(cipher))
	}
	return nil
}

// decryptFields AES-decrypts any field tagged `encrypted`.
func (ds *DataStore) decryptFields(cs *compiledSchema, val reflect.Value) error {
	return ds.decryptColumns(cs.columns, val)
}

// decryptColumns AES-decrypts the `encrypted` fields among cols.
func (ds *DataStore) decryptColumns(cols []l3pkg.ColumnDef, val reflect.Value) error {
	if ds.encryptor == nil {
		return nil
	}
	for _, col := range cols {
		if !col.IsEncrypted {
			continue
		}
		f := val.FieldByName(col.FieldName)
		if !f.IsValid() || !f.CanSet() || f.Kind() != reflect.String {
			continue
		}
		stored := f.String()
		if !strings.HasPrefix(stored, encryptedPrefix) {
			continue // plaintext (e.g. a value already decrypted in place)
		}
		// The prefix marks Strata ciphertext, so a failure here is a wrong key or
		// corrupt data — never hand the ciphertext back as if it were plaintext.
		cipher, err := base64.StdEncoding.DecodeString(stored[len(encryptedPrefix):])
		if err != nil {
			return fmt.Errorf("%w: field %s: %v", ErrDecodeFailed, col.FieldName, err)
		}
		plain, err := ds.encryptor.Decrypt(cipher)
		if err != nil {
			return fmt.Errorf("%w: field %s: %v", ErrDecodeFailed, col.FieldName, err)
		}
		f.SetString(string(plain))
	}
	return nil
}
