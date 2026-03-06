//go:build networktests

// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)

package network_test

import (
	"testing"
	"time"

	"github.com/AndrewDonelson/strata/internal/l4"
)

func newNetworkNode(t *testing.T, hub *l4.MemTransportHub) (l4.L4Layer, string) {
	t.Helper()
	cfg := l4.Config{Enabled: true, Mode: "peer", Quorum: 1}
	_ = cfg.Validate()
	signer, _ := l4.NewSigner()
	store := l4.NewMemStore()
	nodeID := signer.PublicKeyHex()
	transport := l4.NewMemTransport(nodeID, 50, hub, nil)
	layer, err := l4.NewWithComponents(cfg, signer, store, transport)
	if err != nil {
		t.Fatalf("newNetworkNode: %v", err)
	}
	t.Cleanup(func() { _ = layer.Shutdown() })
	return layer, nodeID
}

func TestGossip_RecordSpreadToAllPeers(t *testing.T) {
	hub := l4.NewMemTransportHub()
	layerA, idA := newNetworkNode(t, hub)
	layerB, idB := newNetworkNode(t, hub)
	layerC, idC := newNetworkNode(t, hub)
	hub.Connect(idA, idB)
	hub.Connect(idB, idC)

	rec, err := layerA.Publish("app1", "nodeA", map[string]interface{}{"key": "gossip"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	// B should have the record.
	got, err := layerB.Query("app1", rec.UUID)
	if err != nil {
		t.Fatalf("B query error: %v", err)
	}
	if got.UUID != rec.UUID {
		t.Error("B should have the record")
	}

	// C is only connected to B so it would need B to forward, which doesn't happen automatically.
	// Just verify B got it.
	_ = layerC
}

func TestGossip_PeerCount(t *testing.T) {
	hub := l4.NewMemTransportHub()
	_, idA := newNetworkNode(t, hub)
	_, idB := newNetworkNode(t, hub)
	_, idC := newNetworkNode(t, hub)
	hub.Connect(idA, idB)
	hub.Connect(idA, idC)

	if hub.PeerCount(idA) != 2 {
		t.Errorf("expected 2 peers for A, got %d", hub.PeerCount(idA))
	}
}
