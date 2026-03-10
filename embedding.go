// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// embedding.go — EmbeddingProvider interface with OllamaProvider and
// OpenAIProvider built-in implementations. Strata uses the provider to
// generate vectors for VectorSearch queries and ReEmbed migrations.
// Applications only set Config.EmbeddingProvider; dimension numbers never
// appear in application code.

package strata

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	pgvector "github.com/pgvector/pgvector-go"
)

// EmbeddingProvider converts text to a fixed-size float32 vector.
// Strata calls it once per VectorSearch query and once per record during ReEmbed.
// Implementations must be safe for concurrent use.
type EmbeddingProvider interface {
	// Embed converts text to a vector. The returned vector length must equal Dimensions().
	Embed(ctx context.Context, text string) (pgvector.Vector, error)

	// Dimensions returns the number of dimensions this provider produces.
	// Strata calls this once at schema Register time to size the Postgres column.
	// Implementations should cache the value after the first resolution.
	Dimensions() int

	// ModelID returns a stable identifier for the current embedding model
	// (e.g. "nomic-embed-text", "text-embedding-3-small").
	// Strata stores this in strata_schema_meta and compares it on every Migrate()
	// to detect model changes that require re-embedding.
	ModelID() string
}

// ─────────────────────────────────────────────────────────────────────────────
// OllamaProvider
// ─────────────────────────────────────────────────────────────────────────────

// OllamaProvider calls a local Ollama instance to generate embeddings.
// Dimensions() is auto-detected by calling Embed() once on startup and cached.
//
// Example:
//
//	strata.NewOllamaProvider("http://cqai:11434", "nomic-embed-text")
type OllamaProvider struct {
	baseURL   string
	modelName string
	client    *http.Client
	dimOnce   sync.Once
	dim       int32 // atomic after dimOnce resolves
	dimErr    error
}

// NewOllamaProvider creates an OllamaProvider that embeds text using the
// model running at baseURL/api/embed. The dimension is resolved on the
// first Embed() or Dimensions() call and cached for subsequent calls.
func NewOllamaProvider(baseURL, modelName string) EmbeddingProvider {
	return &OllamaProvider{
		baseURL:   strings.TrimRight(baseURL, "/"),
		modelName: modelName,
		client:    &http.Client{},
	}
}

// ModelID implements EmbeddingProvider.
func (p *OllamaProvider) ModelID() string { return p.modelName }

// Dimensions implements EmbeddingProvider.
// Calls Embed("") once to resolve the dimension, then caches it.
func (p *OllamaProvider) Dimensions() int {
	p.dimOnce.Do(func() {
		vec, err := p.embed(context.Background(), "dimension probe")
		if err != nil {
			p.dimErr = err
			return
		}
		atomic.StoreInt32(&p.dim, int32(len(vec.Slice())))
	})
	return int(atomic.LoadInt32(&p.dim))
}

type ollamaEmbedRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
}

type ollamaEmbedResponse struct {
	Embedding []float32 `json:"embedding"`
}

func (p *OllamaProvider) embed(ctx context.Context, text string) (pgvector.Vector, error) {
	reqBody, err := json.Marshal(ollamaEmbedRequest{Model: p.modelName, Prompt: text})
	if err != nil {
		return pgvector.Vector{}, fmt.Errorf("ollama provider: marshal request: %w", err)
	}

	url := p.baseURL + "/api/embeddings"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(reqBody))
	if err != nil {
		return pgvector.Vector{}, fmt.Errorf("ollama provider: create request to %s: %w", url, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return pgvector.Vector{}, fmt.Errorf("ollama provider: POST %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return pgvector.Vector{}, fmt.Errorf("ollama provider: server returned %d from %s", resp.StatusCode, url)
	}

	var result ollamaEmbedResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return pgvector.Vector{}, fmt.Errorf("ollama provider: decode response from %s: %w", url, err)
	}
	if len(result.Embedding) == 0 {
		return pgvector.Vector{}, fmt.Errorf("ollama provider: empty embedding in response from %s", url)
	}
	return pgvector.NewVector(result.Embedding), nil
}

// Embed implements EmbeddingProvider.
func (p *OllamaProvider) Embed(ctx context.Context, text string) (pgvector.Vector, error) {
	vec, err := p.embed(ctx, text)
	if err != nil {
		return pgvector.Vector{}, err
	}
	// Cache dimension on first successful call via the dimOnce path
	p.dimOnce.Do(func() {
		atomic.StoreInt32(&p.dim, int32(len(vec.Slice())))
	})
	// If dim was already resolved, update it (could be a no-op due to sync.Once)
	if atomic.LoadInt32(&p.dim) == 0 {
		atomic.StoreInt32(&p.dim, int32(len(vec.Slice())))
	}
	return vec, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// OpenAIProvider
// ─────────────────────────────────────────────────────────────────────────────

// knownOpenAIDimensions maps well-known OpenAI embedding model names to their
// output dimensions. Used to avoid an extra API call on startup.
var knownOpenAIDimensions = map[string]int{
	"text-embedding-3-small": 1536,
	"text-embedding-3-large": 3072,
	"text-embedding-ada-002": 1536,
}

// OpenAIProvider calls the OpenAI embeddings API (or compatible endpoints).
//
// Example:
//
//	strata.NewOpenAIProvider(os.Getenv("OPENAI_API_KEY"), "text-embedding-3-small")
type OpenAIProvider struct {
	apiKey    string
	modelName string
	baseURL   string
	client    *http.Client
	dimOnce   sync.Once
	dim       int32
}

// NewOpenAIProvider creates an OpenAIProvider using the official embeddings endpoint.
func NewOpenAIProvider(apiKey, modelName string) EmbeddingProvider {
	return &OpenAIProvider{
		apiKey:    apiKey,
		modelName: modelName,
		baseURL:   "https://api.openai.com",
		client:    &http.Client{},
	}
}

// ModelID implements EmbeddingProvider.
func (p *OpenAIProvider) ModelID() string { return p.modelName }

// Dimensions implements EmbeddingProvider.
// Returns the known dimension for well-known models without an API call,
// falling back to a real Embed() call for unknown models.
func (p *OpenAIProvider) Dimensions() int {
	if d, ok := knownOpenAIDimensions[p.modelName]; ok {
		return d
	}
	p.dimOnce.Do(func() {
		vec, err := p.Embed(context.Background(), "dimension probe")
		if err == nil {
			atomic.StoreInt32(&p.dim, int32(len(vec.Slice())))
		}
	})
	return int(atomic.LoadInt32(&p.dim))
}

type openAIEmbedRequest struct {
	Model string `json:"model"`
	Input string `json:"input"`
}

type openAIEmbedResponse struct {
	Data []struct {
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
}

// Embed implements EmbeddingProvider.
func (p *OpenAIProvider) Embed(ctx context.Context, text string) (pgvector.Vector, error) {
	reqBody, err := json.Marshal(openAIEmbedRequest{Model: p.modelName, Input: text})
	if err != nil {
		return pgvector.Vector{}, fmt.Errorf("openai provider: marshal request: %w", err)
	}

	url := p.baseURL + "/v1/embeddings"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(reqBody))
	if err != nil {
		return pgvector.Vector{}, fmt.Errorf("openai provider: create request to %s: %w", url, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+p.apiKey)

	resp, err := p.client.Do(req)
	if err != nil {
		return pgvector.Vector{}, fmt.Errorf("openai provider: POST %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return pgvector.Vector{}, fmt.Errorf("openai provider: server returned %d from %s", resp.StatusCode, url)
	}

	var result openAIEmbedResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return pgvector.Vector{}, fmt.Errorf("openai provider: decode response from %s: %w", url, err)
	}
	if len(result.Data) == 0 || len(result.Data[0].Embedding) == 0 {
		return pgvector.Vector{}, fmt.Errorf("openai provider: empty embedding in response from %s", url)
	}
	vec := pgvector.NewVector(result.Data[0].Embedding)

	// Cache dimension
	p.dimOnce.Do(func() {
		atomic.StoreInt32(&p.dim, int32(len(vec.Slice())))
	})
	return vec, nil
}
