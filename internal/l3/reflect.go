// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// reflect.go — struct introspection helpers for the L3 tier: Go type →
// PostgreSQL SQL type mapping, ColumnDef derivation from struct tags, embedded
// struct flattening, snake_case conversion, and GetFieldValue/SetFieldValue.

package l3

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

// ColumnDef describes one table column derived from struct reflection.
type ColumnDef struct {
	Name         string
	SQLType      string
	IsPK         bool
	IsNullable   bool
	IsAutoNowAdd bool
	IsAutoNow    bool
	OmitCache    bool
	OmitL1       bool
	DefaultValue string
	IsUnique     bool
	IsIndexed    bool
	IsEncrypted  bool
	FieldIndex   int
	FieldName    string
}

// ReflectSchema derives a slice of ColumnDef from a struct pointer.
func ReflectSchema(model any) ([]ColumnDef, error) {
	t := reflect.TypeOf(model)
	if t == nil || t.Kind() != reflect.Ptr || t.Elem().Kind() != reflect.Struct {
		return nil, fmt.Errorf("model must be a non-nil pointer to a struct")
	}
	t = t.Elem()
	var cols []ColumnDef
	flattenStruct(t, &cols)
	return cols, nil
}

func flattenStruct(t reflect.Type, cols *[]ColumnDef) {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.Anonymous && f.Type.Kind() == reflect.Struct {
			flattenStruct(f.Type, cols)
			continue
		}
		tag := f.Tag.Get("strata")
		if tag == "-" {
			continue
		}
		col := ColumnDef{
			Name:       ToSnakeCase(f.Name),
			FieldName:  f.Name,
			FieldIndex: i,
			SQLType:    goTypeToSQL(f.Type),
		}
		for _, part := range strings.Split(tag, ",") {
			part = strings.TrimSpace(part)
			switch {
			case part == "primary_key":
				col.IsPK = true
			case part == "unique":
				col.IsUnique = true
			case part == "index":
				col.IsIndexed = true
			case part == "nullable":
				col.IsNullable = true
			case part == "omit_cache":
				col.OmitCache = true
			case part == "omit_l1":
				col.OmitL1 = true
			case part == "auto_now_add":
				col.IsAutoNowAdd = true
			case part == "auto_now":
				col.IsAutoNow = true
			case part == "encrypted":
				col.IsEncrypted = true
			case strings.HasPrefix(part, "default:"):
				col.DefaultValue = strings.TrimPrefix(part, "default:")
			}
		}
		*cols = append(*cols, col)
	}
}

// goTypeToSQL maps a Go reflect.Type to the appropriate Postgres column type.
func goTypeToSQL(t reflect.Type) string {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.String:
		return "TEXT"
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		return "INTEGER"
	case reflect.Int64:
		return "BIGINT"
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return "INTEGER"
	case reflect.Uint64:
		return "BIGINT"
	case reflect.Float32:
		return "REAL"
	case reflect.Float64:
		return "DOUBLE PRECISION"
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return "BYTEA"
		}
		return "JSONB"
	case reflect.Struct:
		if t == reflect.TypeOf(time.Time{}) {
			return "TIMESTAMPTZ"
		}
		return "JSONB"
	case reflect.Map, reflect.Interface:
		return "JSONB"
	default:
		return "TEXT"
	}
}

// ToSnakeCase converts CamelCase to snake_case.
func ToSnakeCase(s string) string {
	var b strings.Builder
	for i, r := range s {
		if r >= 'A' && r <= 'Z' {
			if i > 0 {
				b.WriteByte('_')
			}
			b.WriteRune(r + 32)
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// GetFieldValue returns the value of a struct field by ColumnDef.
func GetFieldValue(v reflect.Value, col ColumnDef) any {
	return v.Field(col.FieldIndex).Interface()
}

// SetFieldValue sets a struct field by ColumnDef.
func SetFieldValue(v reflect.Value, col ColumnDef, val any) {
	fv := v.Field(col.FieldIndex)
	if val == nil {
		fv.Set(reflect.Zero(fv.Type()))
		return
	}
	rv := reflect.ValueOf(val)
	if rv.Type().AssignableTo(fv.Type()) {
		fv.Set(rv)
	} else if rv.Type().ConvertibleTo(fv.Type()) {
		fv.Set(rv.Convert(fv.Type()))
	}
}
