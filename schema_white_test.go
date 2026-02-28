package strata

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── toSnakeCase ───────────────────────────────────────────────────────────────

func TestToSnakeCase_Internal(t *testing.T) {
	tests := []struct{ in, want string }{
		{"UserName", "user_name"},
		{"CreatedAt", "created_at"},
		{"simplevalue", "simplevalue"},
		{"", ""},
		{"A", "a"},
	}
	for _, tt := range tests {
		got := toSnakeCase(tt.in)
		assert.Equal(t, tt.want, got, "toSnakeCase(%q)", tt.in)
	}
}

// ── isZeroValue ───────────────────────────────────────────────────────────────

func TestIsZeroValue_String(t *testing.T) {
	assert.True(t, isZeroValue(""))
	assert.False(t, isZeroValue("hello"))
}

func TestIsZeroValue_Int(t *testing.T) {
	assert.True(t, isZeroValue(0))
	assert.False(t, isZeroValue(1))
	assert.True(t, isZeroValue(int64(0)))
}

func TestIsZeroValue_Bool(t *testing.T) {
	assert.True(t, isZeroValue(false))
	assert.False(t, isZeroValue(true))
}

func TestIsZeroValue_Nil(t *testing.T) {
	assert.True(t, isZeroValue(nil))
}

func TestIsZeroValue_Time(t *testing.T) {
	var zeroTime time.Time
	assert.True(t, isZeroValue(zeroTime))
	assert.False(t, isZeroValue(time.Now()))
}

func TestIsZeroValue_Float(t *testing.T) {
	assert.True(t, isZeroValue(float64(0)))
	assert.False(t, isZeroValue(float64(1.5)))
}

// ── schemaRegistry helpers ────────────────────────────────────────────────────

type wbProduct struct {
	ID    string `strata:"primary_key"`
	Name  string
	Price float64
}

func TestRegistry_All_Returns_All(t *testing.T) {
	r := newSchemaRegistry()
	_, err := r.register(Schema{Name: "alpha_wb", Model: &wbProduct{}})
	require.NoError(t, err)
	_, err = r.register(Schema{Name: "beta_wb", Model: &wbProduct{}})
	require.NoError(t, err)

	all := r.all()
	assert.Len(t, all, 2)
}

func TestRegistry_All_Empty(t *testing.T) {
	r := newSchemaRegistry()
	assert.Empty(t, r.all())
}

func TestRegistry_Get_NotFound(t *testing.T) {
	r := newSchemaRegistry()
	_, err := r.get("missing_schema_xyz")
	assert.ErrorIs(t, err, ErrSchemaNotFound)
}

func TestCompiledSchema_NewModel(t *testing.T) {
	r := newSchemaRegistry()
	cs, err := r.register(Schema{Name: "newmodel_wb", Model: &wbProduct{}})
	require.NoError(t, err)

	v := cs.newModel()
	require.NotNil(t, v)
	_, ok := v.(*wbProduct)
	assert.True(t, ok, "newModel should return *wbProduct")
}

func TestCompiledSchema_GetPK_Present(t *testing.T) {
	r := newSchemaRegistry()
	cs, err := r.register(Schema{Name: "getpk_wb", Model: &wbProduct{}})
	require.NoError(t, err)

	p := &wbProduct{ID: "pk-123", Name: "test"}
	pk, err := cs.getPK(p)
	require.NoError(t, err)
	assert.Equal(t, "pk-123", pk)
}

func TestCompiledSchema_GetPK_Missing(t *testing.T) {
	r := newSchemaRegistry()
	cs, err := r.register(Schema{Name: "getpk_missing_wb", Model: &wbProduct{}})
	require.NoError(t, err)

	p := &wbProduct{Name: "no id"}
	_, err = cs.getPK(p)
	assert.ErrorIs(t, err, ErrMissingPrimaryKey)
}

func TestCompiledSchema_GetPK_NonPointer(t *testing.T) {
	r := newSchemaRegistry()
	cs, err := r.register(Schema{Name: "getpk_noptr_wb", Model: &wbProduct{}})
	require.NoError(t, err)

	// Value (not pointer) should still work
	p := wbProduct{ID: "val-123"}
	pk, err := cs.getPK(p)
	require.NoError(t, err)
	assert.Equal(t, "val-123", pk)
}

// ── Schema.Name auto-derive ───────────────────────────────────────────────────

func TestRegister_AutoSnakeCaseName(t *testing.T) {
	type MyAutoModel struct {
		ID string `strata:"primary_key"`
	}
	r := newSchemaRegistry()
	cs, err := r.register(Schema{Model: &MyAutoModel{}})
	require.NoError(t, err)
	assert.Equal(t, "my_auto_model", cs.Name)
}
