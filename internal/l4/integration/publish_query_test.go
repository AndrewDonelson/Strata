// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)

package integration_test

import (
	"testing"

	"github.com/AndrewDonelson/strata/internal/l4"
)

func newLayer(t *testing.T) l4.L4Layer {
	t.Helper()
	cfg := l4.Config{Enabled: true, Mode: "peer", Quorum: 1}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	signer, _ := l4.NewSigner()
	store := l4.NewMemStore()
	hub := l4.NewMemTransportHub()
	transport := l4.NewMemTransport(signer.PublicKeyHex(), cfg.MaxPeers, hub, nil)
	layer, err := l4.NewWithComponents(cfg, signer, store, transport)
	if err != nil {
		t.Fatalf("NewWithComponents: %v", err)
	}
	t.Cleanup(func() { _ = layer.Shutdown() })
	return layer
}

func TestPublishThenQuery_Local(t *testing.T) {
	layer := newLayer(t)
	rec, err := layer.Publish("app1", "node1", map[string]interface{}{"key": "hello"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got, err := layer.Query("app1", rec.UUID)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got.UUID != rec.UUID {
		t.Errorf("UUID mismatch: got %s", got.UUID)
	}
}

func TestPublish_SetsHash(t *testing.T) {
	layer := newLayer(t)
	rec, _ := layer.Publish("app1", "node1", map[string]interface{}{"count": 1})
	if rec.Hash == "" {
		t.Error("Hash should be set after Publish")
	}
}

func TestPublish_SetsPrevHash(t *testing.T) {
	layer := newLayer(t)
	rec1, _ := layer.Publish("app1", "node1", map[string]interface{}{"count": 1})
	rec2, _ := layer.Publish("app1", "node1", map[string]interface{}{"count": 2})
	if rec1.PrevHash != l4.GenesisHash {
		t.Errorf("first record PrevHash should be genesis, got %s", rec1.PrevHash)
	}
	if rec2.PrevHash != rec1.Hash {
		t.Errorf("second record PrevHash should be %s, got %s", rec1.Hash, rec2.PrevHash)
	}
}

func TestPublish_NodeSigPresent(t *testing.T) {
	layer := newLayer(t)
	rec, _ := layer.Publish("app1", "node1", map[string]interface{}{"flag": true})
	if len(rec.NodeSig) == 0 {
		t.Error("NodeSig should be set")
	}
}

func TestPublish_StatusPending(t *testing.T) {
	layer := newLayer(t)
	rec, _ := layer.Publish("app1", "node1", map[string]interface{}{"key": "v"})
	if rec.Status != l4.StatusPending {
		t.Errorf("expected pending, got %s", rec.Status)
	}
}

func TestPublish_AlreadyPublished(t *testing.T) {
	// Same UUID should not be publishable twice; Publish generates unique UUIDs
	// This test verifies that two Publishes with same AppID+different payloads work.
	layer := newLayer(t)
	rec1, err1 := layer.Publish("app1", "node1", map[string]interface{}{"count": 1})
	rec2, err2 := layer.Publish("app1", "node1", map[string]interface{}{"count": 2})
	if err1 != nil || err2 != nil {
		t.Fatalf("unexpected error: %v %v", err1, err2)
	}
	if rec1.UUID == rec2.UUID {
		t.Error("expected different UUIDs for different Publishes")
	}
}

func TestRevoke_QueryReturnsRevoked(t *testing.T) {
	layer := newLayer(t)
	rec, _ := layer.Publish("app1", "node1", map[string]interface{}{"key": "v"})
	if err := layer.Revoke("app1", rec.UUID); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	got, err := layer.Query("app1", rec.UUID)
	if err != nil {
		t.Fatalf("Query after revoke: %v", err)
	}
	if got.Status != l4.StatusRevoked {
		t.Errorf("expected revoked, got %s", got.Status)
	}
}

func TestRevoke_NotFound(t *testing.T) {
	layer := newLayer(t)
	if err := layer.Revoke("app1", "nonexistent"); err != l4.ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}
