// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// misc_cover_test.go -- Edge cases for record, signer, store coverage.

package l4

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"
)

// ------------------------------------------------------------------
// record.go
// ------------------------------------------------------------------

func TestComputeRevocationHash(t *testing.T) {
	rev := L4RevocationRecord{
		AppID:     "app1",
		UUID:      "test-uuid",
		Reason:    "test reason",
		RevokedAt: time.Now().UnixNano(),
	}
	hash, err := rev.ComputeRevocationHash()
	if err != nil {
		t.Fatalf("ComputeRevocationHash: %v", err)
	}
	if len(hash) == 0 {
		t.Error("expected non-empty hash")
	}
}

func TestMarshalPayloadSorted_EmptyPayload(t *testing.T) {
	b, err := marshalPayloadSorted(map[string]interface{}{})
	if err != nil {
		t.Fatalf("marshalPayloadSorted empty: %v", err)
	}
	if string(b) != "{}" {
		t.Errorf("expected {}, got %s", b)
	}
}

func TestComputeHash_Stable(t *testing.T) {
	rec := L4Record{
		UUID:    "uuid-1",
		AppID:   "app1",
		Payload: map[string]interface{}{"b": 2, "a": 1},
	}
	h1, err := rec.ComputeHash()
	if err != nil {
		t.Fatalf("first ComputeHash: %v", err)
	}
	h2, err := rec.ComputeHash()
	if err != nil {
		t.Fatalf("second ComputeHash: %v", err)
	}
	if h1 != h2 {
		t.Errorf("expected stable hash, got %s vs %s", h1, h2)
	}
}

// ------------------------------------------------------------------
// signer.go
// ------------------------------------------------------------------

func TestNewSignerFromKey_RoundTrip(t *testing.T) {
	// Generate ed25519 keypair directly.
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	signer := NewSignerFromKey(priv)
	want := hex.EncodeToString(pub)
	if got := signer.PublicKeyHex(); got != want {
		t.Errorf("public key mismatch: got %s, want %s", got, want)
	}
}

func TestSigner_Verify_InvalidHexPubKey(t *testing.T) {
	signer, _ := NewSigner()
	rec := &L4Record{
		UUID:    "verify-uuid",
		AppID:   "app",
		Payload: map[string]interface{}{"k": "v"},
	}
	sig, _ := signer.Sign(rec)
	// Use invalid hex as public key.
	result := signer.Verify(rec, "GG_NOT_HEX", sig)
	if result {
		t.Error("expected false for invalid hex public key")
	}
}

func TestSigner_Verify_WrongLengthPubKey(t *testing.T) {
	signer, _ := NewSigner()
	rec := &L4Record{
		UUID:    "verify-uuid2",
		AppID:   "app",
		Payload: map[string]interface{}{"k": "v"},
	}
	sig, _ := signer.Sign(rec)
	// 1-byte key = 2 hex chars; wrong length for ed25519 (need 32 bytes).
	result := signer.Verify(rec, "AB", sig)
	if result {
		t.Error("expected false for wrong-length public key")
	}
}

func TestSigner_Verify_ValidSig(t *testing.T) {
	signer, _ := NewSigner()
	rec := &L4Record{
		UUID:    "verify-uuid3",
		AppID:   "app",
		Payload: map[string]interface{}{"k": "v"},
	}
	sig, err := signer.Sign(rec)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if !signer.Verify(rec, signer.PublicKeyHex(), sig) {
		t.Error("expected valid verification to succeed")
	}
}

// ------------------------------------------------------------------
// store.go - memStore edge cases
// ------------------------------------------------------------------

func TestMemStore_GetByHash_OrphanedKey(t *testing.T) {
	store := NewMemStore().(*memStore)
	// Inject an orphaned hash -> key mapping (key doesn't exist in main store).
	store.byHash["orphan-hash"] = "app1:uuid-does-not-exist"
	_, err := store.GetByHash("orphan-hash")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound for orphaned hash, got %v", err)
	}
}

func TestMemStore_GetByHash_NotFound(t *testing.T) {
	store := NewMemStore()
	_, err := store.GetByHash("no-such-hash")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

// ------------------------------------------------------------------
// store.go - boltStore edge cases
// ------------------------------------------------------------------

func TestBoltStore_GetByHash_NotFound(t *testing.T) {
	dir, _ := os.MkdirTemp("", "l4bolt_hash*")
	defer os.RemoveAll(dir)
	store, _ := NewBoltStore(dir)
	_, err := store.GetByHash("no-such-hash")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestBoltStore_Latest_MultipleApps_PrefixBoundary(t *testing.T) {
	dir, _ := os.MkdirTemp("", "l4bolt_prefix*")
	defer os.RemoveAll(dir)
	store, _ := NewBoltStore(dir)

	rec1 := L4Record{UUID: "uuid-1", AppID: "app1", Payload: map[string]interface{}{"k": "v1"}}
	rec1.ComputeHash()
	rec2 := L4Record{UUID: "uuid-2", AppID: "app2", Payload: map[string]interface{}{"k": "v2"}}
	rec2.ComputeHash()
	if err := store.Put(rec1); err != nil {
		t.Fatalf("Put app1: %v", err)
	}
	if err := store.Put(rec2); err != nil {
		t.Fatalf("Put app2: %v", err)
	}

	latest, err := store.Latest("app1", 10)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	for _, r := range latest {
		if r.AppID != "app1" {
			t.Errorf("expected app1, got %s", r.AppID)
		}
	}
}

func TestBoltStore_UpdateRecord_ViaRevoke(t *testing.T) {
	dir, _ := os.MkdirTemp("", "l4bolt_update*")
	defer os.RemoveAll(dir)
	bs, err := NewBoltStore(dir)
	if err != nil {
		t.Fatalf("NewBoltStore: %v", err)
	}

	rec := L4Record{
		UUID:    "upd-uuid",
		AppID:   "app1",
		Status:  StatusPending,
		Payload: map[string]interface{}{"k": "v"},
	}
	rec.ComputeHash()
	if err := bs.Put(rec); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// updateRecord changes status.
	rec.Status = StatusRevoked
	if err := bs.(*boltStore).updateRecord(rec); err != nil {
		t.Fatalf("updateRecord: %v", err)
	}

	got, err := bs.Get("app1", "upd-uuid")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != StatusRevoked {
		t.Errorf("expected revoked, got %s", got.Status)
	}
}

func TestBoltStore_UpdateRecord_Upsert(t *testing.T) {
	// updateRecord uses bbolt Put which is an upsert - it creates the record
	// even if it doesn't exist (no ErrNotFound semantics).
	dir, _ := os.MkdirTemp("", "l4bolt_upd_notfound*")
	defer os.RemoveAll(dir)
	bs, _ := NewBoltStore(dir)
	rec := L4Record{UUID: "newrec", AppID: "app1", Status: StatusConfirmed, Payload: map[string]interface{}{"k": "v"}}
	rec.ComputeHash()
	if err := bs.(*boltStore).updateRecord(rec); err != nil {
		t.Errorf("updateRecord on non-existent should succeed (upsert), got %v", err)
	}
	got, err := bs.Get("app1", "newrec")
	if err != nil {
		t.Fatalf("Get after upsert: %v", err)
	}
	if got.Status != StatusConfirmed {
		t.Errorf("expected confirmed status, got %s", got.Status)
	}
}

func TestBoltStore_NewBoltStore_BadPath(t *testing.T) {
	f, err := os.CreateTemp("", "l4bolt_file*")
	if err != nil {
		t.Fatalf("temp file: %v", err)
	}
	f.Close()
	defer os.Remove(f.Name())
	// Use a sub-path of the temp file (which is a file, not a dir).
	_, err = NewBoltStore(f.Name() + "/subdir")
	if err == nil {
		t.Error("expected error for bad path")
	}
}

func TestBoltStore_Put_AlreadyExists(t *testing.T) {
	dir, _ := os.MkdirTemp("", "l4bolt_dup*")
	defer os.RemoveAll(dir)
	bs, _ := NewBoltStore(dir)

	rec := L4Record{UUID: "dup-uuid", AppID: "app1", Payload: map[string]interface{}{"k": "v"}}
	rec.ComputeHash()
	if err := bs.Put(rec); err != nil {
		t.Fatalf("first Put: %v", err)
	}
	if err := bs.Put(rec); err != ErrAlreadyExists {
		t.Errorf("expected ErrAlreadyExists on duplicate, got %v", err)
	}
}
