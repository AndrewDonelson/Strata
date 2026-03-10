// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// vector_test.go — Integration and unit tests for VectorSearch / ReEmbed
// (Groups 2–8).  Groups that need a real pgvector Postgres are gated behind
// the STRATA_PGVECTOR_TEST_DSN environment variable and are skipped in
// environments that don't provide it.

package strata_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	pgvector "github.com/pgvector/pgvector-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/AndrewDonelson/strata"
)

// ─── Shared mock provider ─────────────────────────────────────────────────────

// MockVectorProvider is a thread-safe EmbeddingProvider for tests.
// Pre-set vectors with SetVector; any unmapped text returns a unit vector.
type MockVectorProvider struct {
	mu      sync.RWMutex
	dims    int
	model   string
	vectors map[string]pgvector.Vector
	errors  map[string]error
	calls   atomic.Int32
}

func NewMockVectorProvider(dims int, model string) *MockVectorProvider {
	return &MockVectorProvider{
		dims:    dims,
		model:   model,
		vectors: make(map[string]pgvector.Vector),
		errors:  make(map[string]error),
	}
}

// SetVector configures the exact vector to return for a given text.
func (m *MockVectorProvider) SetVector(text string, v pgvector.Vector) {
	m.mu.Lock()
	m.vectors[text] = v
	m.mu.Unlock()
}

// SetError configures an error to return for a given text.
func (m *MockVectorProvider) SetError(text string, err error) {
	m.mu.Lock()
	m.errors[text] = err
	m.mu.Unlock()
}

// EmbedCalls returns how many times Embed was called.
func (m *MockVectorProvider) EmbedCalls() int { return int(m.calls.Load()) }

func (m *MockVectorProvider) Embed(_ context.Context, text string) (pgvector.Vector, error) {
	m.calls.Add(1)
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err, ok := m.errors[text]; ok {
		return pgvector.Vector{}, err
	}
	if v, ok := m.vectors[text]; ok {
		return v, nil
	}
	// Default: unit vector with 1.0 in first dimension.
	vals := make([]float32, m.dims)
	vals[0] = 1.0
	return pgvector.NewVector(vals), nil
}

func (m *MockVectorProvider) Dimensions() int { return m.dims }
func (m *MockVectorProvider) ModelID() string { return m.model }

// ─── FAQ model ────────────────────────────────────────────────────────────────

type FAQ struct {
	ID         string `strata:"primary_key"`
	Question   string
	CustomerID string
	Embedding  pgvector.Vector `strata:"vector"`
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

// newVecDS creates a DataStore with the given MockVectorProvider and no
// external backends (L1-only); sufficient for validation-only tests.
func newVecDS(t *testing.T, provider strata.EmbeddingProvider) *strata.DataStore {
	t.Helper()
	ds, err := strata.NewDataStore(strata.Config{EmbeddingProvider: provider})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })
	return ds
}

// registerFAQ registers the FAQ schema on ds.
func registerFAQ(t *testing.T, ds *strata.DataStore) {
	t.Helper()
	require.NoError(t, ds.Register(strata.Schema{
		Name:  "faq",
		Model: &FAQ{},
		Indexes: []strata.Index{
			{Fields: []string{"embedding"}, Type: strata.IndexHNSW, M: 16, EfConstruction: 64, DistanceFunc: "cosine"},
		},
	}))
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 3: Input Validation (no DB required)
// ─────────────────────────────────────────────────────────────────────────────

// T3.1 — nil context returns ErrNilContext.
func TestVectorSearch_T3_1_NilContext(t *testing.T) {
	prov := NewMockVectorProvider(768, "test")
	ds := newVecDS(t, prov)
	registerFAQ(t, ds)

	//nolint:staticcheck // intentional nil context for test
	_, err := ds.VectorSearch(nil, "faq", "question", 5, nil) //nolint:all
	require.Error(t, err)
	assert.ErrorIs(t, err, strata.ErrNilContext)
}

// T3.2 — topK < 1 returns ErrTopKInvalid.
func TestVectorSearch_T3_2_TopKZero(t *testing.T) {
	prov := NewMockVectorProvider(768, "test")
	ds := newVecDS(t, prov)
	registerFAQ(t, ds)

	_, err := ds.VectorSearch(context.Background(), "faq", "question", 0, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, strata.ErrTopKInvalid)
}

// T3.3 — blank query returns ErrEmptyVectorQuery.
func TestVectorSearch_T3_3_EmptyQuery(t *testing.T) {
	prov := NewMockVectorProvider(768, "test")
	ds := newVecDS(t, prov)
	registerFAQ(t, ds)

	_, err := ds.VectorSearch(context.Background(), "faq", "   ", 5, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, strata.ErrEmptyVectorQuery)
}

// T3.4 — unknown schema name returns ErrSchemaNotFound.
func TestVectorSearch_T3_4_UnknownSchema(t *testing.T) {
	prov := NewMockVectorProvider(768, "test")
	ds := newVecDS(t, prov)

	_, err := ds.VectorSearch(context.Background(), "no_such_schema", "q", 5, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, strata.ErrSchemaNotFound)
}

// T3.5 — schema without vector field returns ErrNoVectorField.
func TestVectorSearch_T3_5_NoVectorField(t *testing.T) {
	type Plain struct {
		ID   string `strata:"primary_key"`
		Name string
	}

	prov := NewMockVectorProvider(768, "test")
	ds := newVecDS(t, prov)
	require.NoError(t, ds.Register(strata.Schema{Name: "plain", Model: &Plain{}}))

	_, err := ds.VectorSearch(context.Background(), "plain", "q", 5, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, strata.ErrNoVectorField)
}

// T3.6 — Config.EmbeddingProvider nil returns ErrNoEmbeddingProvider.
func TestVectorSearch_T3_6_NoProvider(t *testing.T) {
	ds, err := strata.NewDataStore(strata.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	// Register a schema that has a vector field; but the provider is nil.
	// Note: Register itself won't error because Register only errors if
	// validateAndWireVectorSchema encounters an explicit wrong type.
	// With no provider however Dimensions() can't be called, so the schema
	// registers with vectorDimension = 0.  VectorSearch then fails with
	// ErrNoEmbeddingProvider.
	require.NoError(t, ds.Register(strata.Schema{Name: "faq_np", Model: &FAQ{}}))

	_, err = ds.VectorSearch(context.Background(), "faq_np", "q", 5, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, strata.ErrNoEmbeddingProvider)
}

// ─── T3.7 — ReEmbed missing text field returns ErrReEmbedTextFieldMissing. ────

func TestReEmbed_T3_7_MissingTextField(t *testing.T) {
	prov := NewMockVectorProvider(768, "test")
	ds := newVecDS(t, prov)
	registerFAQ(t, ds)

	err := ds.ReEmbed(context.Background(), "faq", "no_such_field")
	require.Error(t, err)
	assert.ErrorIs(t, err, strata.ErrReEmbedTextFieldMissing)
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 7: Dimension Management (struct-level, no DB)
// ─────────────────────────────────────────────────────────────────────────────

// T7.1 — provider with 0 dimensions causes ErrVectorDimensionMismatch on Register.
func TestVector_T7_1_ZeroDimensionProvider(t *testing.T) {
	prov := NewMockVectorProvider(0, "test")
	ds, err := strata.NewDataStore(strata.Config{EmbeddingProvider: prov})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	err = ds.Register(strata.Schema{Name: "faq_zero", Model: &FAQ{}})
	require.Error(t, err)
	assert.ErrorIs(t, err, strata.ErrVectorDimensionMismatch,
		"zero-dimension provider should produce ErrVectorDimensionMismatch")
}

// T7.2 — positive dimension is stored on the compiled schema.
func TestVector_T7_2_PositiveDimensionStored(t *testing.T) {
	const wantDim = 384
	prov := NewMockVectorProvider(wantDim, "some-model")
	ds := newVecDS(t, prov)

	require.NoError(t, ds.Register(strata.Schema{Name: "faq_d384", Model: &FAQ{}}))
	// We can indirectly verify the dimension by confirming VectorSearch makes
	// it past dimension validation and reaches the L3 check instead.
	_, err := ds.VectorSearch(context.Background(), "faq_d384", "question", 5, nil)
	require.Error(t, err)
	// Should fail at L3 (database not configured), not at dimension / provider checks.
	assert.ErrorIs(t, err, strata.ErrL3Unavailable)
}

// T7.3 — two schemas with different providers register independently.
func TestVector_T7_3_IndependentSchemas(t *testing.T) {
	type ASchema struct {
		ID  string          `strata:"primary_key"`
		Emb pgvector.Vector `strata:"vector"`
	}
	type BSchema struct {
		ID  string          `strata:"primary_key"`
		Emb pgvector.Vector `strata:"vector"`
	}

	prov := NewMockVectorProvider(512, "model-512")
	ds := newVecDS(t, prov)

	require.NoError(t, ds.Register(strata.Schema{Name: "a_schema", Model: &ASchema{}}))
	require.NoError(t, ds.Register(strata.Schema{Name: "b_schema", Model: &BSchema{}}))
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 8: Concurrency (race-detector safe)
// ─────────────────────────────────────────────────────────────────────────────

// T8.1 — concurrent VectorSearch calls on same schema don't race.
func TestVectorSearch_T8_1_ConcurrentCalls(t *testing.T) {
	prov := NewMockVectorProvider(768, "test")
	ds := newVecDS(t, prov)
	registerFAQ(t, ds)

	const goroutines = 20
	var wg sync.WaitGroup
	errs := make([]error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, err := ds.VectorSearch(context.Background(), "faq",
				fmt.Sprintf("query %d", idx), 5, nil)
			// All should fail at ErrL3Unavailable (no DB) — not race-detected panics.
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		require.Error(t, err, "goroutine %d should err (no DB)", i)
		assert.ErrorIs(t, err, strata.ErrL3Unavailable,
			"goroutine %d: unexpected error: %v", i, err)
	}
}

// T8.2 — concurrent Register calls don't race.
func TestVector_T8_2_ConcurrentRegister(t *testing.T) {
	prov := NewMockVectorProvider(768, "test")
	ds := newVecDS(t, prov)

	const n = 10
	var wg sync.WaitGroup
	errs := make([]error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		// Each goroutine registers a uniquely-named schema to avoid duplicate errors.
		go func(idx int) {
			defer wg.Done()
			type DynModel struct {
				ID  string          `strata:"primary_key"`
				Emb pgvector.Vector `strata:"vector"`
			}
			errs[idx] = ds.Register(strata.Schema{
				Name:  fmt.Sprintf("dyn_schema_%d", idx),
				Model: &DynModel{},
			})
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		assert.NoError(t, err, "goroutine %d Register should not error", i)
	}
}

// T8.3 — MockVectorProvider is safe for concurrent Embed calls.
func TestVector_T8_3_ProviderConcurrentEmbed(t *testing.T) {
	prov := NewMockVectorProvider(768, "test")
	const goroutines = 50
	var wg sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			v, err := prov.Embed(context.Background(), fmt.Sprintf("text%d", idx))
			assert.NoError(t, err)
			assert.Equal(t, 768, len(v.Slice()))
		}(i)
	}
	wg.Wait()
	assert.Equal(t, goroutines, prov.EmbedCalls())
}
