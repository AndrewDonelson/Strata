// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// final_cover_test.go -- Edge cases for remaining coverage gaps.

package l4

import (
	"os"
	"testing"

	bolt "go.etcd.io/bbolt"
)

// ------------------------------------------------------------------
// handleRevocation: already-revoked early return
// ------------------------------------------------------------------

func TestHandleRevocation_AlreadyRevoked(t *testing.T) {
	cfg := Config{Enabled: true, Mode: "ledger", Quorum: 1}
	_ = cfg.Validate()
	hub := NewMemTransportHub()
	signer, _ := NewSigner()
	store := NewMemStore()
	transport := NewMemTransport(signer.PublicKeyHex(), 10, hub, nil)
	layer, _ := NewWithComponents(cfg, signer, store, transport)
	defer layer.Shutdown()
	al := layer.(*activeLayer)

	// Put a record with StatusRevoked directly.
	rec := L4Record{
		UUID:    "rev-uuid",
		AppID:   "app1",
		Status:  StatusRevoked,
		Revoked: true,
		Payload: map[string]interface{}{"k": "v"},
	}
	rec.ComputeHash()
	if err := store.Put(rec); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// handleRevocation should return immediately (already revoked).
	al.handleRevocation(L4RevocationRecord{AppID: "app1", UUID: "rev-uuid"})

	// Verify record still StatusRevoked (not changed).
	got, _ := store.Get("app1", "rev-uuid")
	if got.Status != StatusRevoked {
		t.Errorf("expected StatusRevoked, got %s", got.Status)
	}
}

// ------------------------------------------------------------------
// boltStore.GetByHash: hash entry exists but record key missing (orphaned)
// ------------------------------------------------------------------

func TestBoltStore_GetByHash_OrphanedEntry(t *testing.T) {
	dir, _ := os.MkdirTemp("", "l4bolt_orphan*")
	defer os.RemoveAll(dir)
	bs, err := NewBoltStore(dir)
	if err != nil {
		t.Fatalf("NewBoltStore: %v", err)
	}
	rawBS := bs.(*boltStore)

	// Directly inject a hash entry pointing to a non-existent record key.
	err = rawBS.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bucketByHash)).Put([]byte("orphan-hash"), []byte("app1:nonexistent-uuid"))
	})
	if err != nil {
		t.Fatalf("inject hash: %v", err)
	}

	_, err = bs.GetByHash("orphan-hash")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound for orphaned hash entry, got %v", err)
	}
}

// ------------------------------------------------------------------
// boltStore.Latest: short key that triggers len(k) < len(prefix) break
// ------------------------------------------------------------------

func TestBoltStore_Latest_ShortKeyBreak(t *testing.T) {
	dir, _ := os.MkdirTemp("", "l4bolt_shortkey*")
	defer os.RemoveAll(dir)
	bs, err := NewBoltStore(dir)
	if err != nil {
		t.Fatalf("NewBoltStore: %v", err)
	}
	rawBS := bs.(*boltStore)

	// Use appID "aaa" so prefix is "aaa:" (4 bytes).
	// Insert a real record for "aaa".
	rec1 := L4Record{UUID: "uid1", AppID: "aaa", Payload: map[string]interface{}{"k": "v"}}
	rec1.ComputeHash()
	if err := bs.Put(rec1); err != nil {
		t.Fatalf("Put aaa: %v", err)
	}

	// Directly inject key "z" (1 byte, lexicographically > "aaa:") into records bucket.
	// When iterating after "aaa:" entries, cursor will hit "z" which has len("z")=1 < len("aaa:")=4.
	err = rawBS.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bucketRecords)).Put([]byte("z"), []byte("garbage"))
	})
	if err != nil {
		t.Fatalf("inject short key: %v", err)
	}

	// Latest on "aaa" should still return rec1, stopping before "z" on len check.
	results, err := bs.Latest("aaa", 10)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}
	if results[0].UUID != "uid1" {
		t.Errorf("expected uid1, got %s", results[0].UUID)
	}
}

// ------------------------------------------------------------------
// handleInboundRecord: first verify fails (al.nodeID wrong), but
// second verify (from.NodeID) passes - record accepted from peer
// ------------------------------------------------------------------

func TestHandleInboundRecord_CrossNodeSig_Accepted(t *testing.T) {
	cfg := Config{Enabled: true, Mode: "peer", Quorum: 1}
	_ = cfg.Validate()
	hub := NewMemTransportHub()

	// Node A is our layer.
	signerA, _ := NewSigner()
	storeA := NewMemStore()
	tA := NewMemTransport(signerA.PublicKeyHex(), 10, hub, nil)
	layerA, _ := NewWithComponents(cfg, signerA, storeA, tA)
	defer layerA.Shutdown()
	alA := layerA.(*activeLayer)

	// Node B is a remote peer.
	signerB, _ := NewSigner()

	// B creates and signs a record.
	rec := &L4Record{
		UUID:    "cross-node-uuid",
		AppID:   "app1",
		Payload: map[string]interface{}{"key": "cross-val"},
	}
	_, _ = rec.ComputeHash()
	sig, _ := signerB.Sign(rec)
	rec.NodeSig = sig

	// A receives this record from B.
	// A tries to verify with al.nodeID (A's key) first -> FAILS.
	// Then tries with from.NodeID (B's key) -> PASSES.
	from := L4Peer{NodeID: signerB.PublicKeyHex()}
	alA.handleInboundRecord(from, *rec)

	// Record should be stored.
	got, err := storeA.Get("app1", "cross-node-uuid")
	if err != nil {
		t.Fatalf("record not stored after cross-node sig verification: %v", err)
	}
	if got.UUID != "cross-node-uuid" {
		t.Error("wrong record stored")
	}
}

// ------------------------------------------------------------------
// handleInboundRecord: store.Put returns unexpected error -> return
// ------------------------------------------------------------------

func TestHandleInboundRecord_StorePutError(t *testing.T) {
	cfg := Config{Enabled: true, Mode: "peer", Quorum: 1}
	_ = cfg.Validate()
	hub := NewMemTransportHub()
	signer, _ := NewSigner()
	// Use a failing store that returns ErrL4Disabled (not ErrAlreadyExists) on Put.
	mockStore := &fixedErrStore{inner: NewMemStore(), putErr: ErrL4Disabled}
	transport := NewMemTransport(signer.PublicKeyHex(), 10, hub, nil)
	layer, _ := NewWithComponents(cfg, signer, mockStore, transport)
	defer layer.Shutdown()
	al := layer.(*activeLayer)

	rec := L4Record{
		UUID:    "put-err-uuid",
		AppID:   "app1",
		Payload: map[string]interface{}{"k": "v"},
	}
	_, _ = rec.ComputeHash()
	// No NodeSig so sig verification is skipped; store.Put returns ErrL4Disabled -> early return.
	al.handleInboundRecord(L4Peer{NodeID: "other"}, rec)
	// Record should NOT be stored (early return on Put error).
	_, err := mockStore.inner.Get("app1", "put-err-uuid")
	if err != ErrNotFound {
		t.Error("record should not be stored when store.Put fails")
	}
}
