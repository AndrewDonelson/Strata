package l3_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/AndrewDonelson/strata/internal/l3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── test models ───────────────────────────────────────────────────────────────

type BasicModel struct {
	ID        string `strata:"primary_key"`
	Name      string
	Age       int       `strata:"index"`
	Email     string    `strata:"unique,nullable"`
	Score     float64   `strata:"default:0.0"`
	CreatedAt time.Time `strata:"auto_now_add"`
	UpdatedAt time.Time `strata:"auto_now"`
	Notes     string    `strata:"omit_cache,nullable"`
	Secret    string    `strata:"encrypted"`
	Badge     string    `strata:"omit_l1"`
	Internal  string    `strata:"-"`
}

type EmbeddedBase struct {
	BaseID string `strata:"primary_key"`
}

type EmbeddedModel struct {
	EmbeddedBase
	Value string
}

type AllTypes struct {
	ID        string `strata:"primary_key"`
	TextVal   string
	IntVal    int
	Int64Val  int64
	Int32Val  int32
	FloatVal  float32
	Float64   float64
	BoolVal   bool
	TimeVal   time.Time
	BytesVal  []byte
	SliceVal  []string
	UintVal   uint
	Uint64Val uint64
	MapVal    map[string]any
}

// ── ReflectSchema ─────────────────────────────────────────────────────────────

func TestReflectSchema_BasicModel(t *testing.T) {
	cols, err := l3.ReflectSchema(&BasicModel{})
	require.NoError(t, err)
	require.NotEmpty(t, cols)
	for _, c := range cols {
		assert.NotEqual(t, "internal", c.Name, "ignored field leaked into schema")
	}
}

func TestReflectSchema_PrimaryKey(t *testing.T) {
	cols, err := l3.ReflectSchema(&BasicModel{})
	require.NoError(t, err)

	var pks []l3.ColumnDef
	for _, c := range cols {
		if c.IsPK {
			pks = append(pks, c)
		}
	}
	require.Len(t, pks, 1)
	assert.Equal(t, "i_d", pks[0].Name, "ToSnakeCase(\"ID\") = \"i_d\"")
	assert.Equal(t, "TEXT", pks[0].SQLType)
}

func TestReflectSchema_ColumnFlags(t *testing.T) {
	cols, err := l3.ReflectSchema(&BasicModel{})
	require.NoError(t, err)

	byName := make(map[string]l3.ColumnDef)
	for _, c := range cols {
		byName[c.Name] = c
	}

	assert.True(t, byName["age"].IsIndexed)
	assert.True(t, byName["email"].IsUnique)
	assert.True(t, byName["email"].IsNullable)
	assert.Equal(t, "0.0", byName["score"].DefaultValue)
	assert.True(t, byName["created_at"].IsAutoNowAdd)
	assert.True(t, byName["updated_at"].IsAutoNow)
	assert.True(t, byName["notes"].OmitCache)
	assert.True(t, byName["notes"].IsNullable)
	assert.True(t, byName["secret"].IsEncrypted)
	assert.True(t, byName["badge"].OmitL1)
}

func TestReflectSchema_NonPointerError(t *testing.T) {
	_, err := l3.ReflectSchema(BasicModel{})
	assert.Error(t, err)
}

func TestReflectSchema_NilError(t *testing.T) {
	_, err := l3.ReflectSchema(nil)
	assert.Error(t, err)
}

func TestReflectSchema_NonStructPointerError(t *testing.T) {
	s := "not a struct"
	_, err := l3.ReflectSchema(&s)
	assert.Error(t, err)
}

func TestReflectSchema_EmbeddedStruct(t *testing.T) {
	cols, err := l3.ReflectSchema(&EmbeddedModel{})
	require.NoError(t, err)

	byName := make(map[string]l3.ColumnDef)
	for _, c := range cols {
		byName[c.Name] = c
	}
	assert.Contains(t, byName, "base_i_d", "embedded PK should be flattened; ToSnakeCase(\"BaseID\")=\"base_i_d\"")
	assert.Contains(t, byName, "value")
	assert.True(t, byName["base_i_d"].IsPK)
}

// ── Type mapping ─────────────────────────────────────────────────────────────

func TestReflectSchema_TypeMapping(t *testing.T) {
	cols, err := l3.ReflectSchema(&AllTypes{})
	require.NoError(t, err)

	byName := make(map[string]l3.ColumnDef)
	for _, c := range cols {
		byName[c.Name] = c
	}

	tests := []struct{ field, want string }{
		{"text_val", "TEXT"},
		{"int64_val", "BIGINT"},
		{"int32_val", "INTEGER"},
		{"float_val", "REAL"},
		{"float64", "DOUBLE PRECISION"},
		{"bool_val", "BOOLEAN"},
		{"time_val", "TIMESTAMPTZ"},
		{"bytes_val", "BYTEA"},
		{"slice_val", "JSONB"},
		{"uint_val", "INTEGER"},
		{"uint64_val", "BIGINT"},
		{"map_val", "JSONB"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, byName[tt.field].SQLType, "field %s", tt.field)
	}
}

// ── ToSnakeCase ───────────────────────────────────────────────────────────────

func TestToSnakeCase(t *testing.T) {
	tests := []struct{ in, want string }{
		{"UserName", "user_name"},
		{"CreatedAt", "created_at"},
		{"simplevalue", "simplevalue"},
		{"", ""},
		{"A", "a"},
		{"AB", "a_b"},
		{"PostgresDSN", "postgres_d_s_n"},
	}
	for _, tt := range tests {
		got := l3.ToSnakeCase(tt.in)
		assert.Equal(t, tt.want, got, "ToSnakeCase(%q)", tt.in)
	}
}

// ── GetFieldValue / SetFieldValue ─────────────────────────────────────────────

func TestGetSetFieldValue_StringRoundtrip(t *testing.T) {
	cols, err := l3.ReflectSchema(&BasicModel{})
	require.NoError(t, err)

	var nameCol l3.ColumnDef
	for _, c := range cols {
		if c.Name == "name" {
			nameCol = c
			break
		}
	}
	require.NotEmpty(t, nameCol.FieldName)

	m := &BasicModel{}
	v := reflect.ValueOf(m).Elem()
	l3.SetFieldValue(v, nameCol, "hello strata")
	assert.Equal(t, "hello strata", l3.GetFieldValue(v, nameCol))
}

func TestSetFieldValue_NilZeros(t *testing.T) {
	cols, err := l3.ReflectSchema(&BasicModel{})
	require.NoError(t, err)

	var nameCol l3.ColumnDef
	for _, c := range cols {
		if c.Name == "name" {
			nameCol = c
			break
		}
	}

	m := &BasicModel{Name: "original"}
	v := reflect.ValueOf(m).Elem()
	l3.SetFieldValue(v, nameCol, nil)
	assert.Equal(t, "", m.Name)
}

func TestSetFieldValue_ConvertibleType(t *testing.T) {
	cols, err := l3.ReflectSchema(&BasicModel{})
	require.NoError(t, err)

	var ageCol l3.ColumnDef
	for _, c := range cols {
		if c.Name == "age" {
			ageCol = c
			break
		}
	}

	m := &BasicModel{}
	v := reflect.ValueOf(m).Elem()
	// int32 -> int: convertible
	l3.SetFieldValue(v, ageCol, int32(42))
	assert.Equal(t, 42, m.Age)
}

func TestGetFieldValue_IntType(t *testing.T) {
	cols, err := l3.ReflectSchema(&BasicModel{})
	require.NoError(t, err)

	var ageCol l3.ColumnDef
	for _, c := range cols {
		if c.Name == "age" {
			ageCol = c
			break
		}
	}

	m := &BasicModel{Age: 99}
	v := reflect.ValueOf(m).Elem()
	assert.Equal(t, 99, l3.GetFieldValue(v, ageCol))
}

// ── goTypeToSQL — exotic / default branches ───────────────────────────────────
//
// Three branches in goTypeToSQL are not exercised by the AllTypes fixture:
//
//   1. Pointer-dereference loop   (*string → TEXT)
//   2. Non-time.Time struct field (struct{X int} → JSONB)
//   3. Default case               (chan int → TEXT)
//
// ExoticTypes is purpose-built to hit all three.

type ExoticTypes struct {
	ID    string          `strata:"primary_key"`
	Ptr   *string         // pointer → loops t = t.Elem() then maps to TEXT
	Inner struct{ X int } // non-time struct → JSONB
	Ch    chan int        // chan kind falls through to default → TEXT
}

func TestReflectSchema_ExoticTypes(t *testing.T) {
	cols, err := l3.ReflectSchema(&ExoticTypes{})
	require.NoError(t, err)

	byName := make(map[string]l3.ColumnDef)
	for _, c := range cols {
		byName[c.Name] = c
	}

	// *string: pointer dereferenced → underlying TEXT
	assert.Equal(t, "TEXT", byName["ptr"].SQLType, "pointer to string should resolve to TEXT")
	// struct{X int}: non-time.Time struct → JSONB
	assert.Equal(t, "JSONB", byName["inner"].SQLType, "non-time.Time struct should map to JSONB")
	// chan int: not in any explicit switch case → default TEXT
	assert.Equal(t, "TEXT", byName["ch"].SQLType, "chan type should fall through to default TEXT")
}
