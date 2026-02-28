package strata

import (
	"strings"
	"testing"

	"github.com/AndrewDonelson/strata/internal/l3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// whiteBoxModel is a test model used exclusively within the strata package tests.
type whiteBoxModel struct {
	ID     string `strata:"primary_key"`
	Name   string `strata:"index"`
	Email  string `strata:"unique,nullable"`
	Level  int    `strata:"default:1"`
	Secret string `strata:"omit_cache"`
}

func mustCompileSchema(t *testing.T, s Schema) *compiledSchema {
	t.Helper()
	r := newSchemaRegistry()
	cs, err := r.register(s)
	require.NoError(t, err)
	return cs
}

// ── buildColumnDef ────────────────────────────────────────────────────────────

func TestBuildColumnDef_PrimaryKey(t *testing.T) {
	col := l3.ColumnDef{Name: "id", SQLType: "TEXT", IsPK: true}
	result := buildColumnDef(col)
	assert.Contains(t, result, "id TEXT")
	assert.Contains(t, result, "PRIMARY KEY")
	// PK should not add UNIQUE or NOT NULL separately
	assert.NotContains(t, result, "UNIQUE")
	assert.NotContains(t, result, "NOT NULL")
}

func TestBuildColumnDef_NotNull(t *testing.T) {
	col := l3.ColumnDef{Name: "username", SQLType: "TEXT"}
	result := buildColumnDef(col)
	assert.Contains(t, result, "NOT NULL")
}

func TestBuildColumnDef_Nullable(t *testing.T) {
	col := l3.ColumnDef{Name: "notes", SQLType: "TEXT", IsNullable: true}
	result := buildColumnDef(col)
	assert.NotContains(t, result, "NOT NULL")
}

func TestBuildColumnDef_Unique(t *testing.T) {
	col := l3.ColumnDef{Name: "email", SQLType: "TEXT", IsUnique: true}
	result := buildColumnDef(col)
	assert.Contains(t, result, "UNIQUE")
}

func TestBuildColumnDef_DefaultValue(t *testing.T) {
	col := l3.ColumnDef{Name: "level", SQLType: "BIGINT", DefaultValue: "1"}
	result := buildColumnDef(col)
	assert.Contains(t, result, "DEFAULT 1")
}

func TestBuildColumnDef_AutoNowAdd_Timestamp(t *testing.T) {
	col := l3.ColumnDef{Name: "created_at", SQLType: "TIMESTAMPTZ", IsAutoNowAdd: true}
	result := buildColumnDef(col)
	assert.Contains(t, result, "DEFAULT now()")
}

func TestBuildColumnDef_AutoNow_Timestamp(t *testing.T) {
	col := l3.ColumnDef{Name: "updated_at", SQLType: "TIMESTAMPTZ", IsAutoNow: true}
	result := buildColumnDef(col)
	assert.Contains(t, result, "DEFAULT now()")
}

func TestBuildColumnDef_AutoNow_NonTimestamp_NoDefault(t *testing.T) {
	// auto_now on non-timestamp type should NOT add DEFAULT now()
	col := l3.ColumnDef{Name: "some_field", SQLType: "TEXT", IsAutoNow: true}
	result := buildColumnDef(col)
	assert.NotContains(t, result, "DEFAULT now()")
}

func TestBuildColumnDef_UniqueAndInsideNotNull(t *testing.T) {
	col := l3.ColumnDef{Name: "code", SQLType: "TEXT", IsUnique: true, IsNullable: false}
	result := buildColumnDef(col)
	assert.Contains(t, result, "UNIQUE")
	assert.Contains(t, result, "NOT NULL")
}

// ── buildCreateTableDDL ───────────────────────────────────────────────────────

func TestBuildCreateTableDDL_Structure(t *testing.T) {
	cs := mustCompileSchema(t, Schema{Name: "widgets", Model: &whiteBoxModel{}})
	ddl := buildCreateTableDDL(cs)

	assert.Contains(t, ddl, "CREATE TABLE IF NOT EXISTS widgets")
	assert.Contains(t, ddl, "i_d TEXT PRIMARY KEY")
	assert.Contains(t, ddl, "name TEXT")
}

func TestBuildCreateTableDDL_AllColumns(t *testing.T) {
	cs := mustCompileSchema(t, Schema{Name: "wb_full", Model: &whiteBoxModel{}})
	ddl := buildCreateTableDDL(cs)

	expected := []string{"i_d", "name", "email", "level", "secret"}
	for _, col := range expected {
		assert.Contains(t, ddl, col, "column %q missing from DDL", col)
	}
}

func TestBuildCreateTableDDL_DefaultIncluded(t *testing.T) {
	cs := mustCompileSchema(t, Schema{Name: "wb_defaults", Model: &whiteBoxModel{}})
	ddl := buildCreateTableDDL(cs)
	// level has default:1
	assert.Contains(t, ddl, "DEFAULT 1")
}

// ── buildIndexDDL ─────────────────────────────────────────────────────────────

func TestBuildIndexDDL_IndexTag(t *testing.T) {
	cs := mustCompileSchema(t, Schema{Name: "wb_idx", Model: &whiteBoxModel{}})
	ddls := buildIndexDDL(cs)

	hasNameIdx := false
	for _, d := range ddls {
		if strings.Contains(d, "name") && strings.Contains(d, "CREATE INDEX") {
			hasNameIdx = true
		}
	}
	assert.True(t, hasNameIdx, "expected CREATE INDEX for 'name' column")
}

func TestBuildIndexDDL_SchemaIndexes_NonUnique(t *testing.T) {
	type SimpleModel struct {
		ID    string `strata:"primary_key"`
		Level int
	}
	cs := mustCompileSchema(t, Schema{
		Name:  "si_test",
		Model: &SimpleModel{},
		Indexes: []Index{
			{Fields: []string{"level"}, Unique: false},
		},
	})
	ddls := buildIndexDDL(cs)
	require.NotEmpty(t, ddls)
	assert.Contains(t, ddls[0], "level")
	assert.NotContains(t, ddls[0], "UNIQUE")
}

func TestBuildIndexDDL_SchemaIndexes_Unique(t *testing.T) {
	type SimpleModel struct {
		ID   string `strata:"primary_key"`
		Code string
	}
	cs := mustCompileSchema(t, Schema{
		Name:  "si_unique",
		Model: &SimpleModel{},
		Indexes: []Index{
			{Fields: []string{"code"}, Unique: true, Name: "idx_si_code_unique"},
		},
	})
	ddls := buildIndexDDL(cs)
	require.NotEmpty(t, ddls)

	hasUnique := false
	for _, d := range ddls {
		if strings.Contains(d, "UNIQUE") {
			hasUnique = true
		}
	}
	assert.True(t, hasUnique)
}

func TestBuildIndexDDL_AutoGeneratedName(t *testing.T) {
	type SimpleModel struct {
		ID  string `strata:"primary_key"`
		Tag string
	}
	cs := mustCompileSchema(t, Schema{
		Name:  "autoidx",
		Model: &SimpleModel{},
		Indexes: []Index{
			{Fields: []string{"tag"}, Name: ""}, // name empty → auto-generated
		},
	})
	ddls := buildIndexDDL(cs)
	require.NotEmpty(t, ddls)
	assert.Contains(t, ddls[0], "idx_autoidx_tag")
}

func TestBuildIndexDDL_MultiField(t *testing.T) {
	type SimpleModel struct {
		ID     string `strata:"primary_key"`
		UserID string
		Time   string
	}
	cs := mustCompileSchema(t, Schema{
		Name:  "mfidx",
		Model: &SimpleModel{},
		Indexes: []Index{
			{Fields: []string{"user_id", "time"}},
		},
	})
	ddls := buildIndexDDL(cs)
	require.NotEmpty(t, ddls)
	assert.Contains(t, ddls[0], "user_id")
	assert.Contains(t, ddls[0], "time")
}
