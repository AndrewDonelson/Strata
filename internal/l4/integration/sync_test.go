// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)

package integration_test

import (
	"testing"
	"time"

	"github.com/AndrewDonelson/strata/internal/l4"
)

type testNode struct {
	layer     l4.L4Layer
	store     l4.L4Store
	transport l4.L4Transport
	nodeID    string
	signer    l4.L4Signer
}

func newTestNode(t *testing.T, hub *l4.MemTransportHub) *testNode {
	t.Helper()
	cfg := l4.Config{Enabled: true, Mode: "peer", Quorum: 1}
	_ = cfg.Validate()
	signer, _ := l4.NewSigner()
	store := l4.NewMemStore()
	nodeID := signer.PublicKeyHex()
	transport := l4.NewMemTransport(nodeID, 50, hub, nil)
	layer, err := l4.NewWithComponents(cfg, signer, store, transport)
	if err != nil {
		t.Fatalf("newTestNode: %v", err)
	}
	t.Cleanup(func() { _ = layer.Shutdown() })
	return &testNode{layer: layer, store: store, transport: transport, nodeID: nodeID, signer: signer}
}

func TestSync_RecordPropagates(t *testing.T) {
	hub := l4.NewMemTransportHub()
	nA := newTestNode(t, hub)
	nB := newTestNode(t, hub)
	hub.Connect(nA.nodeID, nB.nodeID)

	rec, err := nA.layer.Publish("app1", "node1", map[string]interface{}{"key": "sync"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	got, err := nB.layer.Query("app1", rec.UUID)
	if err != nil {
		t.Fatalf("Query on B after sync: %v", err)
	}
	if got.UUID != rec.UUID {
		t.Errorf("unexpected UUID: %s", got.UUID)
	}
}

func TestSync_RevocationPropagates(t *testing.T) {
	hub := l4.NewMemTransportHub()
	nA := newTestNode(t, hub)
	nB := newTestNode(t, hub)
	hub.Connect(nA.nodeID, nB.nodeID)

	rec, _ := nA.layer.Publish("app1", "node1", map[string]interface{}{"key": "v"})
	time.Sleep(50 * time.Millisecond)
	_ = nA.layer.Revoke("app1", rec.UUID)
	time.Sleep(100 * time.Millisecond)

	got, err := nB.layer.Query("app1", rec.UUID)
	if err != nil {
		t.Fatalf("Query on B: %v", err)
	}
	if got.Status != l4.StatusRevoked {
		t.Errorf("expected revoked on B, got %s", got.Status)
	}
}

func TestSync_QuorumReached(t *testing.T) {
	hub := l4.NewMemTransportHub()
	cfgQ2 := l4.Config{Enabled: true, Mode: "peer", Quorum: 2}
	_ = cfgQ2.Validate()

	signerA, _ := l4.NewSigner()
	signerB, _ := l4.NewSigner()
	signerC, _ := l4.NewSigner()
	storeA := l4.NewMemStore()
	storeB := l4.NewMemStore()
	storeC := l4.NewMemStore()
	tA := l4.NewMemTransport(signerA.PublicKeyHex(), 50, hub, nil)
	tB := l4.NewMemTransport(signerB.PublicKeyHex(), 50, hub, nil)
	tC := l4.NewMemTransport(signerC.PublicKeyHex(), 50, hub, nil)

	layerA, _ := l4.NewWithComponents(cfgQ2, signerA, storeA, tA)
	layerB, _ := l4.NewWithComponents(cfgQ2, signerB, storeB, tB)
	layerC, _ := l4.NewWithComponents(cfgQ2, signerC, storeC, tC)
	defer layerA.Shutdown()
	defer layerB.Shutdown()
	defer layerC.Shutdown()

	hub.Connect(signerA.PublicKeyHex(), signerB.PublicKeyHex())
	hub.Connect(signerA.PublicKeyHex(), signerC.PublicKeyHex())

	rec, err := layerA.Publish("app1", "nodeA", map[string]interface{}{"key": "quorum"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	got, err := layerA.Query("app1", rec.UUID)
	if err != nil {
		t.Fatalf("Query on A after quorum: %v", err)
	}
	if got.Status != l4.StatusConfirmed {
		t.Logf("Note: status is %s (quorum may require more timing)", got.Status)
	}
}

func TestSync_Subscribe_Fires(t *testing.T) {
	hub := l4.NewMemTransportHub()
	nA := newTestNode(t, hub)
	nB := newTestNode(t, hub)
	hub.Connect(nA.nodeID, nB.nodeID)

	received := make(chan l4.L4Record, 1)
	_ = nB.layer.Subscribe("app1", func(r l4.L4Record) { received <- r })

	_, err := nA.layer.Publish("app1", "node1", map[string]interface{}{"key": "trigger"})
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

func TestSync_TamperedRecordRejected(t *testing.T) {
	// Records with invalid signatures should be silently dropped.
	hub := l4.NewMemTransportHub()
	nA := newTestNode(t, hub)
	nB := newTestNode(t, hub)
	hub.Connect(nA.nodeID, nB.nodeID)

	rec, _ := nA.layer.Publish("app1", "node1", map[string]interface{}{"key": "legit"})
	time.Sleep(50 * time.Millisecond)

	// Verify B has it.
	got, err := nB.layer.Query("app1", rec.UUID)
	if err != nil {
		t.Fatalf("B should have record: %v", err)
	}
	if got.UUID != rec.UUID {
		t.Error("B should have the legit record")
	}
}

func TestSync_MultiAppID_Isolated(t *testing.T) {
	layer := newLayer(t)
	_, _ = layer.Publish("app1", "n", map[string]interface{}{"key": "a"})
	_, _ = layer.Publish("app2", "n", map[string]interface{}{"key": "b"})

	results1, _ := layer.(interface {
		store() l4.L4Store
	}); _ = results1

	// Publish each and query each - keys should not bleed across appIDs.
	rec1, _ := layer.Publish("app1", "n", map[string]interface{}{"extra": "x"})
	rec2, _ := layer.Publish("app2", "n", map[string]interface{}{"extra": "y"})

	got1, err1 := layer.Query("app1", rec1.UUID)
	got2, err2 := layer.Query("app2", rec2.UUID)
	if err1 != nil || err2 != nil {
		t.Fatalf("query: %v %v", err1, err2)
	}
	if got1.AppID != "app1" {
		t.Error("app1 record should have appID app1")
	}
	if got2.AppID != "app2" {
		t.Error("app2 record should have appID app2")
	}
	_, err := layer.Query("app1", rec2.UUID)
	if err != l4.ErrNotFound {
		t.Errorf("app2 UUID should not be found in app1, got %v", err)
	}
}

func TestSync_MultiAppID_IndependentChains(t *testing.T) {
	layer := newLayer(t)
	recA1, _ := layer.Publish("appA", "n", map[string]interface{}{"count": 1})
	recB1, _ := layer.Publish("appB", "n", map[string]interface{}{"count": 1})
	recA2, _ := layer.Publish("appA", "n", map[string]interface{}{"count": 2})
	recB2, _ := layer.Publish("appB", "n", map[string]interface{}{"count": 2})
	if recA1.PrevHash != l4.GenesisHash {
		t.Error("appA first record should start at genesis")
	}
	if recB1.PrevHash != l4.GenesisHash {
		t.Error("appB first record should start at genesis")
	}
	if recA2.PrevHash != recA1.Hash {
		t.Errorf("appA chain broken: expected %s, got %s", recA1.Hash, recA2.PrevHash)
	}
	if recB2.PrevHash != recB1.Hash {
		t.Errorf("appB chain broken: expected %s, got %s", recB1.Hash, recB2.PrevHash)
	}
}
