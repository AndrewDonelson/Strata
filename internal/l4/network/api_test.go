//go:build networktests

// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)

package network_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/AndrewDonelson/strata/internal/l4"
)

func startAPIServer(t *testing.T) (*l4.APIServer, l4.L4Layer, string) {
	t.Helper()
	cfg := l4.Config{Enabled: true, Mode: "ledger", Quorum: 1}
	_ = cfg.Validate()
	signer, _ := l4.NewSigner()
	store := l4.NewMemStore()
	hub := l4.NewMemTransportHub()
	transport := l4.NewMemTransport(signer.PublicKeyHex(), 50, hub, nil)
	layer, err := l4.NewWithComponents(cfg, signer, store, transport)
	if err != nil {
		t.Fatalf("layer: %v", err)
	}
	srv := l4.NewAPIServer(layer, store, transport)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	go func() { _ = srv.ListenOnListener(ln) }()
	time.Sleep(50 * time.Millisecond)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		_ = layer.Shutdown()
	})
	return srv, layer, addr
}

func TestAPI_Query_NotFound(t *testing.T) {
	_, _, addr := startAPIServer(t)
	resp, err := http.Get(fmt.Sprintf("http://%s/query/nonexistent?app_id=app1", addr))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404, got %d", resp.StatusCode)
	}
}

func TestAPI_Query_Found(t *testing.T) {
	_, layer, addr := startAPIServer(t)
	rec, _ := layer.Publish("app1", "node1", map[string]interface{}{"key": "v"})
	resp, err := http.Get(fmt.Sprintf("http://%s/query/%s?app_id=app1", addr, rec.UUID))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
	var got l4.L4Record
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.UUID != rec.UUID {
		t.Errorf("UUID mismatch: %s", got.UUID)
	}
}

func TestAPI_Peers(t *testing.T) {
	_, _, addr := startAPIServer(t)
	resp, err := http.Get(fmt.Sprintf("http://%s/peers", addr))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestAPI_Sync(t *testing.T) {
	_, _, addr := startAPIServer(t)
	resp, err := http.Post(fmt.Sprintf("http://%s/sync", addr), "application/json", nil)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestAPI_RateLimit(t *testing.T) {
	_, _, addr := startAPIServer(t)
	// Send 110 requests; after 100, should start getting 429
	var got429 bool
	for i := 0; i < 110; i++ {
		resp, err := http.Get(fmt.Sprintf("http://%s/peers", addr))
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
		t.Error("expected at least one 429 after 100 requests")
	}
}

func TestAPI_CORS_Header(t *testing.T) {
	_, _, addr := startAPIServer(t)
	resp, err := http.Get(fmt.Sprintf("http://%s/peers", addr))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.Header.Get("Access-Control-Allow-Origin") != "*" {
		t.Error("expected CORS header Access-Control-Allow-Origin: *")
	}
}
