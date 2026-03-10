// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// router.go — tri-tier routing logic that moves data between L1 (in-memory),
// L2 (Redis), and L3 (PostgreSQL). Implements write-through, write-behind,
// and L1-async write modes, cache population on read, and cascade delete.

package strata

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	l2pkg "github.com/AndrewDonelson/strata/internal/l2"
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

// colNamesWithVector returns DB column names including vector fields.
// Used for L3 SELECT queries in routerGet / readFromL3 where the full record
// (including the vector field) is needed for cache-warming.
func colNamesWithVector(cs *compiledSchema) []string {
	names := make([]string, 0, len(cs.columns))
	for _, col := range cs.columns {
		if !col.OmitCache || col.IsVector {
			names = append(names, col.Name)
		}
	}
	return names
}

// stripVectors returns a shallow copy of value with all vector fields zeroed out,
// so they are not stored in L1 or L2. If the schema has no vector fields the
// original value is returned unchanged.
func stripVectors(cs *compiledSchema, value any) any {
	if !cs.hasVectorFields {
		return value
	}
	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	copy := reflect.New(v.Type()).Elem()
	copy.Set(v)
	for _, col := range cs.columns {
		if col.IsVector {
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
				return nil
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
			return nil
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
			return err
		}
	}
	// L4 — sync after confirmed L3 write
	ds.syncToL4(ctx, cs, id, value)
	// L2
	if ds.l2 != nil {
		_ = ds.setL2(ctx, cs, id, value)
	}
	// L1
	if ds.l1 != nil {
		ds.setL1(cs, cs.l1Prefix+id, value)
	}
	// Invalidate other nodes
	if ds.sync != nil {
		ds.sync.publishInvalidation(ctx, cs.Name, id, "set")
	}
	return nil
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
	if q == nil {
		empty := Q().Build()
		q = &empty
	}
	cols := colNames(cs)
	sql, args := q.ToSQL(cs.tableName, cols, 100)

	rows, err := ds.l3.Query(ctx, sql, args)
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
	// Strip vector fields — they must not be stored in L1 cache.
	if cs.hasVectorFields {
		value = stripVectors(cs, value)
	}
	ttl := cs.L1.TTL
	if ttl == 0 {
		ttl = ds.cfg.DefaultL1TTL
	}
	ds.l1.Set(key, value, ttl)
}

func (ds *DataStore) setL2(ctx context.Context, cs *compiledSchema, id string, value any) error {
	// Strip vector fields — they must not be stored in L2 (Redis) cache.
	if cs.hasVectorFields {
		value = stripVectors(cs, value)
	}
	ttl := cs.L2.TTL
	if ttl == 0 {
		ttl = ds.cfg.DefaultL2TTL
	}
	return ds.l2.SetP(ctx, cs.l2Prefix, id, value, ttl)
}

func (ds *DataStore) readFromL3(ctx context.Context, cs *compiledSchema, id string, dest any) error {
	// Include vector fields in the SELECT so Get() from L3 returns the full record.
	cols := colNamesWithVector(cs)
	sql := fmt.Sprintf("SELECT %s FROM %s WHERE %s = $1",
		strings.Join(cols, ", "), cs.tableName, cs.pkColumn.Name)
	row := ds.l3.QueryRow(ctx, sql, []any{id})

	destVal := reflect.ValueOf(dest)
	if destVal.Kind() == reflect.Ptr {
		destVal = destVal.Elem()
	}
	dests := buildScanDestWithVector(destVal, cs)
	if err := row.Scan(dests...); err != nil {
		if isNoRowsError(err) {
			return ErrNotFound
		}
		return err
	}
	return ds.decryptFields(cs, destVal)
}

func (ds *DataStore) writeToL3(ctx context.Context, cs *compiledSchema, value any) error {
	if ds.l3 == nil {
		return ErrL3Unavailable
	}
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

	// Collect columns and values.
	// OmitCache fields are excluded UNLESS they are vector fields (which are L3-only).
	cols := make([]string, 0, len(cs.columns))
	vals := make([]any, 0, len(cs.columns))
	for _, col := range cs.columns {
		if col.OmitCache && !col.IsVector {
			continue // skip non-vector omit_cache fields
		}
		f := val.FieldByName(col.FieldName)
		if !f.IsValid() {
			continue
		}
		cols = append(cols, col.Name)
		vals = append(vals, f.Interface())
	}
	return ds.l3.Upsert(ctx, cs.tableName, cols, vals, cs.pkColumn.Name)
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

// buildScanDestWithVector is like buildScanDest but also includes vector fields.
// Used by readFromL3 to populate the full record including the embedding.
func buildScanDestWithVector(val reflect.Value, cs *compiledSchema) []any {
	dests := make([]any, 0, len(cs.columns))
	for _, col := range cs.columns {
		if col.OmitCache && !col.IsVector {
			continue // skip non-vector omit_cache columns
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
		f.SetString(string(cipher))
	}
	return nil
}

// decryptFields AES-decrypts any field tagged `encrypted`.
func (ds *DataStore) decryptFields(cs *compiledSchema, val reflect.Value) error {
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
		cipher := f.String()
		if cipher == "" {
			continue
		}
		plain, err := ds.encryptor.Decrypt([]byte(cipher))
		if err != nil {
			// Not ciphertext (plain text), skip silently
			continue
		}
		f.SetString(string(plain))
	}
	return nil
}
