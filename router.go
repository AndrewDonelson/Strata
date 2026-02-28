package strata

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// ────────────────────────────────────────────────────────────────────────────
// Column name helpers
// ────────────────────────────────────────────────────────────────────────────

// colNames returns the DB column names for all non-omitted columns.
func colNames(cs *compiledSchema) []string {
	names := make([]string, 0, len(cs.columns))
	for _, col := range cs.columns {
		if !col.OmitCache {
			names = append(names, col.Name)
		}
	}
	return names
}

// ────────────────────────────────────────────────────────────────────────────
// Read path
// ────────────────────────────────────────────────────────────────────────────

// routerGet attempts L1 → L2 → L3 and back-fills upper tiers on a miss.
func (ds *DataStore) routerGet(ctx context.Context, cs *compiledSchema, id string, dest any) error {
	l1Key := fmt.Sprintf("%s:%s", cs.Name, id)

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

	// L2 hit
	if ds.l2 != nil {
		if err := ds.l2.Get(ctx, cs.Name, "", id, dest); err == nil {
			ds.metrics.RecordHit(cs.Name, "l2")
			// back-fill L1
			if ds.l1 != nil {
				ds.setL1(cs, l1Key, dest)
			}
			return nil
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
	// L2
	if ds.l2 != nil {
		_ = ds.setL2(ctx, cs, id, value)
	}
	// L1
	if ds.l1 != nil {
		ds.setL1(cs, fmt.Sprintf("%s:%s", cs.Name, id), value)
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
		ds.setL1(cs, fmt.Sprintf("%s:%s", cs.Name, id), value)
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
	if ds.l2 != nil {
		_ = ds.setL2(ctx, cs, id, value)
	}
	// L1 asynchronously
	if ds.l1 != nil {
		key := fmt.Sprintf("%s:%s", cs.Name, id)
		go ds.setL1(cs, key, value)
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
	l1Key := fmt.Sprintf("%s:%s", cs.Name, id)
	if ds.l1 != nil {
		ds.l1.Delete(l1Key)
	}
	if ds.l2 != nil {
		_ = ds.l2.Delete(ctx, cs.Name, "", id)
	}
	if ds.l3 != nil {
		if err := ds.l3.DeleteByID(ctx, cs.tableName, cs.pkColumn.Name, id); err != nil {
			return err
		}
	}
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
	ttl := cs.L1.TTL
	if ttl == 0 {
		ttl = ds.cfg.DefaultL1TTL
	}
	ds.l1.Set(key, value, ttl)
}

func (ds *DataStore) setL2(ctx context.Context, cs *compiledSchema, id string, value any) error {
	ttl := cs.L2.TTL
	if ttl == 0 {
		ttl = ds.cfg.DefaultL2TTL
	}
	return ds.l2.Set(ctx, cs.Name, "", id, value, ttl)
}

func (ds *DataStore) readFromL3(ctx context.Context, cs *compiledSchema, id string, dest any) error {
	cols := colNames(cs)
	sql := fmt.Sprintf("SELECT %s FROM %s WHERE %s = $1",
		strings.Join(cols, ", "), cs.tableName, cs.pkColumn.Name)
	row := ds.l3.QueryRow(ctx, sql, []any{id})

	destVal := reflect.ValueOf(dest)
	if destVal.Kind() == reflect.Ptr {
		destVal = destVal.Elem()
	}
	dests := buildScanDest(destVal, cs)
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

	// Collect columns and values
	cols := make([]string, 0, len(cs.columns))
	vals := make([]any, 0, len(cs.columns))
	for _, col := range cs.columns {
		if col.OmitCache {
			continue
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
