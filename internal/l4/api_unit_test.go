// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// api_unit_test.go -- Unit tests for the HTTP API server without build tags.

package l4

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"
)

func newAPILayerAndServer(t *testing.T) (*APIServer, L4Layer, string) {
	t.Helper()
	cfg := Config{Enabled: true, Mode: "ledger", Quorum: 1}
	_ = cfg.Validate()
	signer, _ := NewSigner()
	store := NewMemStore()
	hub := NewMemTransportHub()
	transport := NewMemTransport(signer.PublicKeyHex(), 50, hub, nil)
	layer, err := NewWithComponents(cfg, signer, store, transport)
	if err != nil {
		t.Fatalf("layer: %v", err)
	}
	srv := NewAPIServer(layer, store, transport)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := "http://" + ln.Addr().String()
	go func() { _ = srv.ListenOnListener(ln) }()
	time.Sleep(30 * time.Millisecond)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		_ = layer.Shutdown()
	})
	return srv, layer, addr
}

func TestAPIServer_NewAPIServer(t *testing.T) {
	cfg := Config{Enabled: true, Mode: "ledger", Quorum: 1}
	_ = cfg.Validate()
	signer, _ := NewSigner()
	store := NewMemStore()
	hub := NewMemTransportHub()
	transport := NewMemTransport(signer.PublicKeyHex(), 50, hub, nil)
	layer, _ := NewWithComponents(cfg, signer, store, transport)
	defer layer.Shutdown()
	srv := NewAPIServer(layer, store, transport)
	if srv == nil {
		t.Fatal("NewAPIServer returned nil")
	}
}

func TestAPIServer_Shutdown_NilServer(t *testing.T) {
	cfg := Config{Enabled: true, Mode: "ledger", Quorum: 1}
	_ = cfg.Validate()
	signer, _ := NewSigner()
	store := NewMemStore()
	hub := NewMemTransportHub()
	transport := NewMemTransport(signer.PublicKeyHex(), 50, hub, nil)
	layer, _ := NewWithComponents(cfg, signer, store, transport)
	defer layer.Shutdown()
	srv := NewAPIServer(layer, store, transport)
	// Shutdown before Listen should be a no-op.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown on unstarted server: %v", err)
	}
}

func TestAPIServer_Listen_BadAddr(t *testing.T) {
	cfg := Config{Enabled: true, Mode: "ledger", Quorum: 1}
	_ = cfg.Validate()
	signer, _ := NewSigner()
	store := NewMemStore()
	hub := NewMemTransportHub()
	transport := NewMemTransport(signer.PublicKeyHex(), 50, hub, nil)
	layer, _ := NewWithComponents(cfg, signer, store, transport)
	defer layer.Shutdown()
	srv := NewAPIServer(layer, store, transport)
	// Listen on an invalid address should return an error.
	err := srv.Listen("127.0.0.1:99999")
	if err == nil {
		t.Error("expected error for bad address")
	}
}

func TestAPI_Query_MissingUUID(t *testing.T) {
	_, _, addr := newAPILayerAndServer(t)
	resp, err := http.Get(addr + "/query/?app_id=app1")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestAPI_Query_MissingAppID(t *testing.T) {
	_, _, addr := newAPILayerAndServer(t)
	resp, err := http.Get(addr + "/query/some-uuid")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestAPI_Query_NotFound(t *testing.T) {
	_, _, addr := newAPILayerAndServer(t)
	resp, err := http.Get(addr + "/query/nonexistent?app_id=app1")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404, got %d", resp.StatusCode)
	}
}

func TestAPI_Query_Found(t *testing.T) {
	_, layer, addr := newAPILayerAndServer(t)
	rec, _ := layer.Publish("app1", "node1", map[string]interface{}{"key": "v"})
	resp, err := http.Get(fmt.Sprintf("%s/query/%s?app_id=app1", addr, rec.UUID))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
	var got L4Record
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.UUID != rec.UUID {
		t.Errorf("UUID mismatch: %s", got.UUID)
	}
}

func TestAPI_Query_WrongMethod(t *testing.T) {
	_, _, addr := newAPILayerAndServer(t)
	resp, err := http.Post(addr+"/query/uuid?app_id=app1", "", nil)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", resp.StatusCode)
	}
}

func TestAPI_Peers_OK(t *testing.T) {
	_, _, addr := newAPILayerAndServer(t)
	resp, err := http.Get(addr + "/peers")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestAPI_Peers_WrongMethod(t *testing.T) {
	_, _, addr := newAPILayerAndServer(t)
	resp, err := http.Post(addr+"/peers", "", nil)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", resp.StatusCode)
	}
}

func TestAPI_Sync_OK(t *testing.T) {
	_, _, addr := newAPILayerAndServer(t)
	resp, err := http.Post(addr+"/sync", "application/json", nil)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
	var result map[string]int64
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := result["height"]; !ok {
		t.Error("expected height in response")
	}
}

func TestAPI_Sync_WrongMethod(t *testing.T) {
	_, _, addr := newAPILayerAndServer(t)
	resp, err := http.Get(addr + "/sync")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", resp.StatusCode)
	}
}

func TestAPI_RateLimit(t *testing.T) {
	_, _, addr := newAPILayerAndServer(t)
	var got429 bool
	for i := 0; i < 110; i++ {
		resp, err := http.Get(addr + "/peers")
		if err != nil {
			t.Fatalf("GET %d: %v", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests {
			got429 = true
			break
		}
	}
	if !got429 {
		t.Error("expected 429 after exceeding rate limit")
	}
}

func TestAPI_CORS_Header(t *testing.T) {
	_, _, addr := newAPILayerAndServer(t)
	resp, err := http.Get(addr + "/peers")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.Header.Get("Access-Control-Allow-Origin") != "*" {
		t.Error("expected CORS header")
	}
}

func TestAPI_ClientIP_WithPort(t *testing.T) {
	req := &http.Request{RemoteAddr: "192.168.1.1:1234"}
	ip := clientIP(req)
	if ip != "192.168.1.1" {
		t.Errorf("expected 192.168.1.1, got %s", ip)
	}
}

func TestAPI_ClientIP_NoPort(t *testing.T) {
	req := &http.Request{RemoteAddr: "192.168.1.1"}
	ip := clientIP(req)
	if ip != "192.168.1.1" {
		t.Errorf("expected 192.168.1.1, got %s", ip)
	}
}

func TestRateLimiter_Allow(t *testing.T) {
	rl := newRateLimiter()
	// 100 requests should be allowed.
	for i := 0; i < 100; i++ {
		if !rl.Allow("test-ip") {
			t.Fatalf("request %d should be allowed", i)
		}
	}
	// 101st should be rejected.
	if rl.Allow("test-ip") {
		t.Error("101st request should be rejected")
	}
}

func TestRateLimiter_DifferentIPs(t *testing.T) {
	rl := newRateLimiter()
	for i := 0; i < 100; i++ {
		_ = rl.Allow("ip-a")
	}
	// Different IP should still be allowed.
	if !rl.Allow("ip-b") {
		t.Error("different IP should not be rate limited")
	}
}

// ------------------------------------------------------------------
// API 500 error: Query returns non-ErrNotFound error
// ------------------------------------------------------------------

type errorQueryLayer struct {
	L4Layer
	inner    L4Layer
	queryErr error
}

func (e *errorQueryLayer) Query(appID, uuid string) (L4Record, error) {
	if e.queryErr != nil {
		return L4Record{}, e.queryErr
	}
	return e.inner.Query(appID, uuid)
}

func newAPIErrorLayerAndServer(t *testing.T, queryErr error) (*APIServer, string) {
	t.Helper()
	cfg := Config{Enabled: true, Mode: "ledger", Quorum: 1}
	_ = cfg.Validate()
	signer, _ := NewSigner()
	store := NewMemStore()
	hub := NewMemTransportHub()
	transport := NewMemTransport(signer.PublicKeyHex(), 50, hub, nil)
	inner, err := NewWithComponents(cfg, signer, store, transport)
	if err != nil {
		t.Fatalf("layer: %v", err)
	}
	layer := &errorQueryLayer{inner: inner, queryErr: queryErr}
	srv := NewAPIServer(layer, store, transport)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := "http://" + ln.Addr().String()
	go func() { _ = srv.ListenOnListener(ln) }()
	time.Sleep(30 * time.Millisecond)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		_ = inner.Shutdown()
	})
	return srv, addr
}

func TestAPI_Query_InternalServerError(t *testing.T) {
	_, addr := newAPIErrorLayerAndServer(t, fmt.Errorf("db failure"))
	resp, err := http.Get(addr + "/query/some-uuid?app_id=app1")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", resp.StatusCode)
	}
}
