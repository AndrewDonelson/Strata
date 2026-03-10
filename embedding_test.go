// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// embedding_test.go — Tests for EmbeddingProvider implementations and
// ReEmbed (Groups 9–10).  Ollama / OpenAI tests are gated behind env vars
// and skipped when they are not set.  ReEmbed integration tests require a
// pgvector Postgres instance (STRATA_PGVECTOR_TEST_DSN env var).

package strata_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/AndrewDonelson/strata"
)

// ─── Helpers ──────────────────────────────────────────────────────────────────

// requireOllama skips the test unless OLLAMA_TEST_URL is set.
func requireOllama(t *testing.T) string {
	t.Helper()
	url := os.Getenv("OLLAMA_TEST_URL")
	if url == "" {
		t.Skip("OLLAMA_TEST_URL not set — skipping Ollama provider tests")
	}
	return url
}

// requireOpenAI skips the test unless OPENAI_TEST_API_KEY is set.
func requireOpenAI(t *testing.T) string {
	t.Helper()
	key := os.Getenv("OPENAI_TEST_API_KEY")
	if key == "" {
		t.Skip("OPENAI_TEST_API_KEY not set — skipping OpenAI provider tests")
	}
	return key
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 9: OllamaProvider Integration (OLLAMA_TEST_URL required)
// ─────────────────────────────────────────────────────────────────────────────

const ollamaTestModel = "nomic-embed-text" // must be pulled: ollama pull nomic-embed-text

// T9.1 — Embed returns a non-zero vector of length Dimensions().
func TestOllamaProvider_T9_1_EmbedReturnsVector(t *testing.T) {
	url := requireOllama(t)
	prov := strata.NewOllamaProvider(url, ollamaTestModel)

	vec, err := prov.Embed(context.Background(), "hello world")
	require.NoError(t, err)
	slc := vec.Slice()
	assert.Greater(t, len(slc), 0, "vector must be non-empty")
	assert.Equal(t, prov.Dimensions(), len(slc),
		"vector length must equal Dimensions()")
}

// T9.2 — Dimensions() returns the same value on repeated calls.
func TestOllamaProvider_T9_2_DimensionsStable(t *testing.T) {
	url := requireOllama(t)
	prov := strata.NewOllamaProvider(url, ollamaTestModel)

	d1 := prov.Dimensions()
	d2 := prov.Dimensions()
	assert.Equal(t, d1, d2, "Dimensions() must be stable across calls")
	assert.Greater(t, d1, 0, "dimension must be positive")
}

// T9.3 — ModelID returns the model name passed to NewOllamaProvider.
func TestOllamaProvider_T9_3_ModelID(t *testing.T) {
	url := requireOllama(t)
	prov := strata.NewOllamaProvider(url, ollamaTestModel)
	assert.Equal(t, ollamaTestModel, prov.ModelID())
}

// T9.4 — Embed on empty string still returns a vector (model-dependent).
func TestOllamaProvider_T9_4_EmbedEmptyString(t *testing.T) {
	url := requireOllama(t)
	prov := strata.NewOllamaProvider(url, ollamaTestModel)

	vec, err := prov.Embed(context.Background(), "")
	// Some models error on empty string, others return a vector — either is valid.
	if err != nil {
		t.Logf("Ollama returned error for empty string (acceptable): %v", err)
		return
	}
	assert.Equal(t, prov.Dimensions(), len(vec.Slice()))
}

// T9.5 — Two different texts produce different vectors.
func TestOllamaProvider_T9_5_DifferentTexts(t *testing.T) {
	url := requireOllama(t)
	prov := strata.NewOllamaProvider(url, ollamaTestModel)

	v1, err := prov.Embed(context.Background(), "cats are good")
	require.NoError(t, err)
	v2, err := prov.Embed(context.Background(), "quantum physics research")
	require.NoError(t, err)

	// The two vectors should not be identical.
	s1, s2 := v1.Slice(), v2.Slice()
	require.Equal(t, len(s1), len(s2))
	allSame := true
	for i := range s1 {
		if s1[i] != s2[i] {
			allSame = false
			break
		}
	}
	assert.False(t, allSame, "different texts should produce different vectors")
}

// T9.6 — Bad URL returns an error from Embed.
func TestOllamaProvider_T9_6_BadURL(t *testing.T) {
	prov := strata.NewOllamaProvider("http://localhost:1", "nomic-embed-text")
	_, err := prov.Embed(context.Background(), "test")
	require.Error(t, err, "unreachable URL should return error")
}

// T9.7 — OpenAI provider returns non-empty vector for a known model.
func TestOpenAIProvider_T9_7_EmbedSmall(t *testing.T) {
	key := requireOpenAI(t)
	prov := strata.NewOpenAIProvider(key, "text-embedding-3-small")

	vec, err := prov.Embed(context.Background(), "semantic search test")
	require.NoError(t, err)
	slc := vec.Slice()
	assert.Greater(t, len(slc), 0)
	assert.Equal(t, prov.Dimensions(), len(slc))
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 10: ReEmbed (L1-only, mock validation — no DB required)
// ─────────────────────────────────────────────────────────────────────────────

// T10.1 — ReEmbed with no L3 returns ErrL3Unavailable.
func TestReEmbed_T10_1_NoL3(t *testing.T) {
	prov := NewMockVectorProvider(768, "model-v1")
	ds := newVecDS(t, prov)
	require.NoError(t, ds.Register(strata.Schema{Name: "faq_re", Model: &FAQ{}}))

	err := ds.ReEmbed(context.Background(), "faq_re", "Question")
	require.Error(t, err)
	assert.ErrorIs(t, err, strata.ErrL3Unavailable)
}

// T10.2 — ReEmbed with unknown schema returns ErrSchemaNotFound.
func TestReEmbed_T10_2_UnknownSchema(t *testing.T) {
	prov := NewMockVectorProvider(768, "model-v1")
	ds := newVecDS(t, prov)

	err := ds.ReEmbed(context.Background(), "ghost_schema", "Question")
	require.Error(t, err)
	assert.ErrorIs(t, err, strata.ErrSchemaNotFound)
}

// T10.3 — ReEmbed with wrong field name returns ErrReEmbedTextFieldMissing.
func TestReEmbed_T10_3_WrongTextField(t *testing.T) {
	prov := NewMockVectorProvider(768, "model-v1")
	ds := newVecDS(t, prov)
	require.NoError(t, ds.Register(strata.Schema{Name: "faq_rf", Model: &FAQ{}}))

	err := ds.ReEmbed(context.Background(), "faq_rf", "NonExistentField")
	require.Error(t, err)
	assert.ErrorIs(t, err, strata.ErrReEmbedTextFieldMissing)
}

// T10.4 — ReEmbed with nil context returns ErrNilContext.
func TestReEmbed_T10_4_NilContext(t *testing.T) {
	prov := NewMockVectorProvider(768, "model-v1")
	ds := newVecDS(t, prov)
	require.NoError(t, ds.Register(strata.Schema{Name: "faq_nc", Model: &FAQ{}}))

	//nolint:staticcheck
	err := ds.ReEmbed(nil, "faq_nc", "Question") //nolint:all
	require.Error(t, err)
	assert.ErrorIs(t, err, strata.ErrNilContext)
}

// T10.5 — ReEmbed with no EmbeddingProvider returns ErrNoEmbeddingProvider.
func TestReEmbed_T10_5_NoProvider(t *testing.T) {
	ds, err := strata.NewDataStore(strata.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	require.NoError(t, ds.Register(strata.Schema{Name: "faq_np2", Model: &FAQ{}}))

	err = ds.ReEmbed(context.Background(), "faq_np2", "Question")
	require.Error(t, err)
	assert.ErrorIs(t, err, strata.ErrNoEmbeddingProvider)
}

// T10.6 — ReEmbed on a schema without vector field returns ErrNoVectorField.
func TestReEmbed_T10_6_NoVectorField(t *testing.T) {
	type NoVec struct {
		ID   string `strata:"primary_key"`
		Name string
	}
	prov := NewMockVectorProvider(768, "model-v1")
	ds := newVecDS(t, prov)
	require.NoError(t, ds.Register(strata.Schema{Name: "novecschema", Model: &NoVec{}}))

	err := ds.ReEmbed(context.Background(), "novecschema", "Name")
	require.Error(t, err)
	assert.ErrorIs(t, err, strata.ErrNoVectorField)
}
