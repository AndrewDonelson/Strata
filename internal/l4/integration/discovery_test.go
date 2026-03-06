// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)

package integration_test

import (
	"testing"

	"github.com/AndrewDonelson/strata/internal/l4"
)

func TestBootstrapConnect(t *testing.T) {
	hub := l4.NewMemTransportHub()
	nA := newTestNode(t, hub)
	nB := newTestNode(t, hub)
	hub.Connect(nA.nodeID, nB.nodeID)
	if hub.PeerCount(nA.nodeID) != 1 {
		t.Errorf("expected 1 peer for A, got %d", hub.PeerCount(nA.nodeID))
	}
	if hub.PeerCount(nB.nodeID) != 1 {
		t.Errorf("expected 1 peer for B, got %d", hub.PeerCount(nB.nodeID))
	}
}

func TestPeerListExchange(t *testing.T) {
	hub := l4.NewMemTransportHub()
	nA := newTestNode(t, hub)
	nB := newTestNode(t, hub)
	nC := newTestNode(t, hub)
	hub.Connect(nA.nodeID, nB.nodeID)
	hub.Connect(nA.nodeID, nC.nodeID)

	if hub.PeerCount(nA.nodeID) != 2 {
		t.Errorf("A should have 2 peers, got %d", hub.PeerCount(nA.nodeID))
	}
}

func TestMaxPeers_Respected(t *testing.T) {
	hub := l4.NewMemTransportHub()
	// Create nodes with maxPeers=1
	cfg := l4.Config{Enabled: true, Mode: "peer", Quorum: 1, MaxPeers: 1}
	_ = cfg.Validate()

	var nodeIDs []string
	for i := 0; i < 3; i++ {
		signer, _ := l4.NewSigner()
		store := l4.NewMemStore()
		nodeID := signer.PublicKeyHex()
		transport := l4.NewMemTransport(nodeID, cfg.MaxPeers, hub, nil)
		layer, _ := l4.NewWithComponents(cfg, signer, store, transport)
		t.Cleanup(func() { _ = layer.Shutdown() })
		nodeIDs = append(nodeIDs, nodeID)
	}

	// Connect A-B and A-C; A should only accept 1
	hub.ConnectSafe(nodeIDs[0], nodeIDs[1], cfg.MaxPeers)
	ok := hub.ConnectSafe(nodeIDs[0], nodeIDs[2], cfg.MaxPeers)
	if ok {
		t.Error("second connection should be rejected by maxPeers=1")
	}
	if hub.PeerCount(nodeIDs[0]) > 1 {
		t.Errorf("A should have at most 1 peer, got %d", hub.PeerCount(nodeIDs[0]))
	}
}
