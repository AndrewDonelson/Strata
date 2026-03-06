// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)

package l4

import (
	"os"
	"testing"
	"time"
)

func newTestRecord(appID, uuid string) L4Record {
	rec := L4Record{
		UUID:      uuid,
		AppID:     appID,
		NodeID:    "node1",
		Payload:   map[string]interface{}{"key": "value"},
		Timestamp: time.Now().UnixNano(),
		PrevHash:  GenesisHash,
		Status:    StatusPending,
	}
	_, _ = rec.ComputeHash()
	return rec
}

func runStoreTests(t *testing.T, store L4Store) {
	t.Helper()
	rec := newTestRecord("app1", "uuid-001")

	// Put
	if err := store.Put(rec); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Duplicate Put
	if err := store.Put(rec); err != ErrAlreadyExists {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}
	// Get
	got, err := store.Get("app1", "uuid-001")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.UUID != "uuid-001" {
		t.Errorf("unexpected UUID: %s", got.UUID)
	}
	// GetByHash
	gotByHash, err := store.GetByHash(rec.Hash)
	if err != nil {
		t.Fatalf("GetByHash: %v", err)
	}
	if gotByHash.UUID != "uuid-001" {
		t.Errorf("unexpected UUID from GetByHash: %s", gotByHash.UUID)
	}
	// Get not found
	_, err = store.Get("app1", "nonexistent")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
	// Latest
	rec2 := newTestRecord("app1", "uuid-002")
	_ = store.Put(rec2)
	latest, err := store.Latest("app1", 10)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if len(latest) != 2 {
		t.Errorf("expected 2 records, got %d", len(latest))
	}
	// Height
	h, err := store.Height()
	if err != nil {
		t.Fatalf("Height: %v", err)
	}
	if h != 2 {
		t.Errorf("expected height 2, got %d", h)
	}
	// Latest with limit
	results, _ := store.Latest("app1", 1)
	if len(results) != 1 {
		t.Errorf("expected 1 result with limit=1, got %d", len(results))
	}
	// Close
	if err := store.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestMemStore(t *testing.T) {
	store := NewMemStore()
	runStoreTests(t, store)
}

func TestBoltStore(t *testing.T) {
	dir, err := os.MkdirTemp("", "l4bolt*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(dir)
	store, err := NewBoltStore(dir)
	if err != nil {
		t.Fatalf("NewBoltStore: %v", err)
	}
	runStoreTests(t, store)
}

func TestMemStore_Update(t *testing.T) {
	store := NewMemStore()
	rec := newTestRecord("app1", "uuid-003")
	_ = store.Put(rec)
	rec.Status = StatusConfirmed
	store.(*memStore).update(rec)
	got, _ := store.Get("app1", "uuid-003")
	if got.Status != StatusConfirmed {
		t.Errorf("expected confirmed status, got %s", got.Status)
	}
}

func TestMemStore_LatestSortedByTimestamp(t *testing.T) {
	store := NewMemStore()
	recOlder := L4Record{
		UUID: "older", AppID: "app1", Timestamp: 1000,
		Payload: map[string]interface{}{"key": "v"}, Status: StatusPending,
	}
	_, _ = recOlder.ComputeHash()
	recNewer := L4Record{
		UUID: "newer", AppID: "app1", Timestamp: 9000,
		Payload: map[string]interface{}{"key": "v2"}, Status: StatusPending,
	}
	_, _ = recNewer.ComputeHash()
	// Put older first
	_ = store.Put(recOlder)
	_ = store.Put(recNewer)
	results, _ := store.Latest("app1", 10)
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].UUID != "newer" {
		t.Errorf("expected newer first, got %s", results[0].UUID)
	}
}
