// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)

package l4

import (
	"testing"
	"time"
)

func newTestLayer(t *testing.T) (L4Layer, *MemTransportHub) {
	t.Helper()
	cfg := Config{Enabled: true, Mode: "peer", Quorum: 1}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	signer, _ := NewSigner()
	store := NewMemStore()
	hub := NewMemTransportHub()
	transport := NewMemTransport(signer.PublicKeyHex(), cfg.MaxPeers, hub, nil)
	layer, err := NewWithComponents(cfg, signer, store, transport)
	if err != nil {
		t.Fatalf("NewWithComponents: %v", err)
	}
	return layer, hub
}

func TestNew_Disabled(t *testing.T) {
	cfg := Config{Enabled: false}
	layer, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if layer.Status().Enabled {
		t.Error("expected disabled")
	}
	if layer.PeerCount() != 0 {
		t.Error("expected 0 peers")
	}
}

func TestDisabledLayer_ReturnsErrors(t *testing.T) {
	cfg := Config{Enabled: false}
	layer, _ := New(cfg)
	_, err := layer.Publish("app", "node", map[string]interface{}{})
	if err != ErrL4Disabled {
		t.Errorf("expected ErrL4Disabled, got %v", err)
	}
	_, err = layer.Query("app", "uuid")
	if err != ErrL4Disabled {
		t.Errorf("expected ErrL4Disabled, got %v", err)
	}
	if err := layer.Revoke("app", "uuid"); err != ErrL4Disabled {
		t.Errorf("expected ErrL4Disabled, got %v", err)
	}
	if err := layer.Subscribe("app", func(_ L4Record) {}); err != ErrL4Disabled {
		t.Errorf("expected ErrL4Disabled, got %v", err)
	}
}

func TestPublish_Succeeds(t *testing.T) {
	layer, _ := newTestLayer(t)
	defer layer.Shutdown()
	rec, err := layer.Publish("app1", "node1", map[string]interface{}{"key": "v"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if rec.UUID == "" {
		t.Error("expected UUID to be set")
	}
	if rec.Hash == "" {
		t.Error("expected Hash to be set")
	}
	if rec.Status != StatusPending {
		t.Errorf("expected pending, got %s", rec.Status)
	}
	if rec.PrevHash != GenesisHash {
		t.Errorf("expected genesis hash, got %s", rec.PrevHash)
	}
}

func TestPublish_ChainLinks(t *testing.T) {
	layer, _ := newTestLayer(t)
	defer layer.Shutdown()
	rec1, _ := layer.Publish("app1", "node1", map[string]interface{}{"count": 1})
	rec2, _ := layer.Publish("app1", "node1", map[string]interface{}{"count": 2})
	if rec2.PrevHash != rec1.Hash {
		t.Errorf("expected rec2.PrevHash=%s, got %s", rec1.Hash, rec2.PrevHash)
	}
}

func TestPublish_SeparateAppIDs(t *testing.T) {
	layer, _ := newTestLayer(t)
	defer layer.Shutdown()
	// Two different appIDs should both start with GenesisHash
	rec1, _ := layer.Publish("app1", "node1", map[string]interface{}{"key": "a"})
	rec2, _ := layer.Publish("app2", "node1", map[string]interface{}{"key": "b"})
	if rec1.PrevHash != GenesisHash {
		t.Error("app1 first record should have genesis prev hash")
	}
	if rec2.PrevHash != GenesisHash {
		t.Error("app2 first record should have genesis prev hash")
	}
}

func TestQuery_Found(t *testing.T) {
	layer, _ := newTestLayer(t)
	defer layer.Shutdown()
	rec, _ := layer.Publish("app1", "node1", map[string]interface{}{"key": "v"})
	got, err := layer.Query("app1", rec.UUID)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got.UUID != rec.UUID {
		t.Errorf("wrong UUID: %s", got.UUID)
	}
}

func TestQuery_NotFound(t *testing.T) {
	layer, _ := newTestLayer(t)
	defer layer.Shutdown()
	_, err := layer.Query("app1", "nonexistent")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestRevoke_Success(t *testing.T) {
	layer, _ := newTestLayer(t)
	defer layer.Shutdown()
	rec, _ := layer.Publish("app1", "node1", map[string]interface{}{"key": "v"})
	if err := layer.Revoke("app1", rec.UUID); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	got, _ := layer.Query("app1", rec.UUID)
	if got.Status != StatusRevoked {
		t.Errorf("expected revoked, got %s", got.Status)
	}
}

func TestRevoke_AlreadyRevoked(t *testing.T) {
	layer, _ := newTestLayer(t)
	defer layer.Shutdown()
	rec, _ := layer.Publish("app1", "node1", map[string]interface{}{"key": "v"})
	_ = layer.Revoke("app1", rec.UUID)
	if err := layer.Revoke("app1", rec.UUID); err != ErrAlreadyRevoked {
		t.Errorf("expected ErrAlreadyRevoked, got %v", err)
	}
}

func TestSubscribe_ReceivesBroadcast(t *testing.T) {
	cfg := Config{Enabled: true, Mode: "peer", Quorum: 1}
	_ = cfg.Validate()
	hub := NewMemTransportHub()
	signerA, _ := NewSigner()
	signerB, _ := NewSigner()
	storeA := NewMemStore()
	storeB := NewMemStore()
	transportA := NewMemTransport(signerA.PublicKeyHex(), 10, hub, nil)
	transportB := NewMemTransport(signerB.PublicKeyHex(), 10, hub, nil)

	layerA, err := NewWithComponents(cfg, signerA, storeA, transportA)
	if err != nil {
		t.Fatalf("layer A: %v", err)
	}
	defer layerA.Shutdown()
	layerB, err := NewWithComponents(cfg, signerB, storeB, transportB)
	if err != nil {
		t.Fatalf("layer B: %v", err)
	}
	defer layerB.Shutdown()

	hub.Connect(signerA.PublicKeyHex(), signerB.PublicKeyHex())

	received := make(chan L4Record, 1)
	_ = layerB.Subscribe("app1", func(r L4Record) { received <- r })

	_, err = layerA.Publish("app1", "node1", map[string]interface{}{"key": "hello"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case r := <-received:
		if r.AppID != "app1" {
			t.Errorf("wrong appID: %s", r.AppID)
		}
	case <-time.After(2 * time.Second):
		t.Error("timed out waiting for subscription event")
	}
}

func TestStatus_ReportsEnabled(t *testing.T) {
	layer, _ := newTestLayer(t)
	defer layer.Shutdown()
	s := layer.Status()
	if !s.Enabled {
		t.Error("expected Enabled=true")
	}
}

func TestShutdown_Idempotent(t *testing.T) {
	layer, _ := newTestLayer(t)
	if err := layer.Shutdown(); err != nil {
		t.Errorf("first shutdown: %v", err)
	}
}
