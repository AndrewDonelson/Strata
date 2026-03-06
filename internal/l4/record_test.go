// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)

package l4

import (
	"testing"
	"time"
)

func makeRecord(appID, uid string, payload map[string]interface{}) L4Record {
	now := time.Now().UnixNano()
	return L4Record{
		UUID:      uid,
		AppID:     appID,
		NodeID:    "node1",
		Payload:   payload,
		Timestamp: now,
		PrevHash:  GenesisHash,
		Status:    StatusPending,
	}
}

func TestComputeHash_Deterministic(t *testing.T) {
	rec := makeRecord("app1", "uuid-1", map[string]interface{}{"key": "val"})
	h1, _ := rec.ComputeHash()
	h2, _ := rec.ComputeHash()
	if h1 != h2 {
		t.Error("hash not deterministic")
	}
}

func TestComputeHash_DifferentPayload(t *testing.T) {
	rec1 := makeRecord("app1", "uuid-1", map[string]interface{}{"key": "a"})
	rec2 := makeRecord("app1", "uuid-1", map[string]interface{}{"key": "b"})
	rec1.Timestamp = 1000
	rec2.Timestamp = 1000
	h1, _ := rec1.ComputeHash()
	h2, _ := rec2.ComputeHash()
	if h1 == h2 {
		t.Error("different payloads should produce different hashes")
	}
}

func TestComputeHash_SetsField(t *testing.T) {
	rec := makeRecord("app1", "uuid-2", map[string]interface{}{"count": 1})
	if rec.Hash != "" {
		t.Error("hash should start empty")
	}
	_, err := rec.ComputeHash()
	if err != nil {
		t.Fatalf("ComputeHash error: %v", err)
	}
	if rec.Hash == "" {
		t.Error("hash should be set after ComputeHash")
	}
}

func TestVerifiedAtFromTimestamp(t *testing.T) {
	// Use a known time: 2024-03
	ts := time.Date(2024, 3, 15, 12, 0, 0, 0, time.UTC).UnixNano()
	got := VerifiedAtFromTimestamp(ts)
	if got != "2024-03" {
		t.Errorf("expected 2024-03, got %s", got)
	}
}

func TestStatusConstants(t *testing.T) {
	if StatusPending == StatusConfirmed || StatusPending == StatusRevoked {
		t.Error("status constants should be distinct")
	}
}

func TestGenesisHash(t *testing.T) {
	if GenesisHash == "" {
		t.Error("GenesisHash should not be empty")
	}
}
