// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// vector_white_test.go — Group 1: Struct Tag Parsing (white-box unit tests,
// no external services required).

package strata

import (
	"context"
	"testing"

	pgvector "github.com/pgvector/pgvector-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── Fixtures ─────────────────────────────────────────────────────────────────

// vectorModel is the canonical test model with a pgvector.Vector field.
type vectorModel struct {
	ID        string `strata:"primary_key"`
	Question  string
	Answer    string
	Embedding pgvector.Vector `strata:"vector"`
}

// badVectorModel has a non-pgvector.Vector field tagged strata:"vector".
type badVectorModel struct {
	ID        string `strata:"primary_key"`
	Embedding string `strata:"vector"` // wrong type -- must be pgvector.Vector
}

// noIndexVectorModel has a vector field but no Index entry.
type noIndexVectorModel struct {
	ID        string `strata:"primary_key"`
	Question  string
	Embedding pgvector.Vector `strata:"vector"`
}

// ─── Helper ───────────────────────────────────────────────────────────────────

// simpleWhiteBoxDS returns a DataStore with the given EmbeddingProvider but
// no Postgres / Redis -- sufficient for structural validation tests.
func simpleWhiteBoxDS(t *testing.T, provider EmbeddingProvider) *DataStore {
	t.Helper()
	ds, err := NewDataStore(Config{
		EmbeddingProvider: provider,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })
	return ds
}

// ─── Group 1 Tests ────────────────────────────────────────────────────────────

// T1.1 -- Vector tag detected on pgvector.Vector field; OmitCache set implicitly.
func TestVector_T1_1_VectorTagDetected(t *testing.T) {
	ds := simpleWhiteBoxDS(t, &whiteBoxMockProvider{dims: 768, model: "test-model"})

	err := ds.Register(Schema{
		Name:  "vec_model",
		Model: &vectorModel{},
	})
	require.NoError(t, err)

	cs, csErr := ds.registry.get("vec_model")
	require.NoError(t, csErr)

	assert.True(t, cs.hasVectorFields, "hasVectorFields must be true")
	require.NotNil(t, cs.vectorField, "vectorField should not be nil")
	assert.Equal(t, "embedding", cs.vectorField.Name)
	assert.Equal(t, 768, cs.vectorDimension, "dimension should be set from provider")

	// Verify that the vector column has OmitCache = true (never in L1/L2)
	found := false
	for _, col := range cs.columns {
		if col.IsVector {
			found = true
			assert.True(t, col.OmitCache,
				"vector field must implicitly set OmitCache=true (never in L1/L2)")
		}
	}
	assert.True(t, found, "there should be at least one IsVector column")
}

// T1.2 -- Vector tag on non-pgvector.Vector type returns ErrInvalidTagForType.
func TestVector_T1_2_VectorTagOnWrongType(t *testing.T) {
	ds := simpleWhiteBoxDS(t, &whiteBoxMockProvider{dims: 768, model: "test-model"})

	err := ds.Register(Schema{
		Name:  "bad_vec",
		Model: &badVectorModel{},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidTagForType,
		"strata:\"vector\" on a non-pgvector.Vector field should return ErrInvalidTagForType")
}

// T1.3 -- Vector field without explicit Index entry registers without error.
func TestVector_T1_3_VectorFieldNoIndex(t *testing.T) {
	ds := simpleWhiteBoxDS(t, &whiteBoxMockProvider{dims: 768, model: "test-model"})

	err := ds.Register(Schema{
		Name:  "no_idx_vec",
		Model: &noIndexVectorModel{},
	})
	require.NoError(t, err, "vector field with no index should register fine (sequential scan)")

	cs, csErr := ds.registry.get("no_idx_vec")
	require.NoError(t, csErr)
	assert.True(t, cs.hasVectorFields)
}

// T1.4 -- Vector field with IVFFlat index registers cleanly.
func TestVector_T1_4_IVFFlatIndexRegisters(t *testing.T) {
	ds := simpleWhiteBoxDS(t, &whiteBoxMockProvider{dims: 768, model: "test-model"})

	err := ds.Register(Schema{
		Name:  "ivfflat_vec",
		Model: &vectorModel{},
		Indexes: []Index{
			{Fields: []string{"embedding"}, Type: IndexIVFFlat, Lists: 100, DistanceFunc: "cosine"},
		},
	})
	require.NoError(t, err, "IVFFlat index on vector field should register without error")
}

// T1.5 -- Vector field with HNSW index registers cleanly.
func TestVector_T1_5_HNSWIndexRegisters(t *testing.T) {
	ds := simpleWhiteBoxDS(t, &whiteBoxMockProvider{dims: 768, model: "test-model"})

	err := ds.Register(Schema{
		Name:  "hnsw_vec",
		Model: &vectorModel{},
		Indexes: []Index{
			{Fields: []string{"embedding"}, Type: IndexHNSW, M: 16, EfConstruction: 64, DistanceFunc: "cosine"},
		},
	})
	require.NoError(t, err, "HNSW index on vector field should register without error")
}

// T1.6 -- IVFFlat index on non-vector field returns ErrInvalidIndexType.
func TestVector_T1_6_IVFFlatOnNonVectorField(t *testing.T) {
	type plainModel struct {
		ID       string `strata:"primary_key"`
		Username string
	}
	ds := simpleWhiteBoxDS(t, &whiteBoxMockProvider{dims: 768, model: "test-model"})

	err := ds.Register(Schema{
		Name:  "ivfflat_bad",
		Model: &plainModel{},
		Indexes: []Index{
			{Fields: []string{"username"}, Type: IndexIVFFlat},
		},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidIndexType,
		"IVFFlat on a non-vector field should return ErrInvalidIndexType")
}

// ─── White-box mock provider ──────────────────────────────────────────────────

// whiteBoxMockProvider is a minimal EmbeddingProvider for white-box structural
// tests that do not require realistic embeddings.
type whiteBoxMockProvider struct {
	dims  int
	model string
}

func (m *whiteBoxMockProvider) Embed(_ context.Context, _ string) (pgvector.Vector, error) {
	v := make([]float32, m.dims)
	v[0] = 1.0
	return pgvector.NewVector(v), nil
}

func (m *whiteBoxMockProvider) Dimensions() int { return m.dims }
func (m *whiteBoxMockProvider) ModelID() string { return m.model }
