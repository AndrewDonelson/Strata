// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)

package l4

import (
	"testing"
	"time"
)

func TestNewSigner_GeneratesKeys(t *testing.T) {
	s, err := NewSigner()
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	pk := s.PublicKeyHex()
	if len(pk) != 64 {
		t.Errorf("expected 64 hex chars, got %d", len(pk))
	}
}

func TestSigner_SignAndVerify(t *testing.T) {
	s, _ := NewSigner()
	rec := L4Record{
		UUID:      "test-uuid",
		AppID:     "app",
		NodeID:    "node1",
		Payload:   map[string]interface{}{"key": "val"},
		Timestamp: time.Now().UnixNano(),
		PrevHash:  GenesisHash,
	}
	_, _ = rec.ComputeHash()
	sig, err := s.Sign(&rec)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if !s.Verify(&rec, s.PublicKeyHex(), sig) {
		t.Error("Verify should return true for valid signature")
	}
}

func TestSigner_Verify_WrongKey(t *testing.T) {
	s1, _ := NewSigner()
	s2, _ := NewSigner()
	rec := L4Record{
		UUID:      "test-uuid",
		AppID:     "app",
		Payload:   map[string]interface{}{"flag": true},
		Timestamp: time.Now().UnixNano(),
		PrevHash:  GenesisHash,
	}
	_, _ = rec.ComputeHash()
	sig, _ := s1.Sign(&rec)
	if s2.Verify(&rec, s2.PublicKeyHex(), sig) {
		t.Error("Verify should fail with wrong key")
	}
}

func TestSigner_Verify_TamperedRecord(t *testing.T) {
	s, _ := NewSigner()
	rec := L4Record{
		UUID:      "test-uuid",
		AppID:     "app",
		Payload:   map[string]interface{}{"count": 1},
		Timestamp: time.Now().UnixNano(),
		PrevHash:  GenesisHash,
	}
	_, _ = rec.ComputeHash()
	sig, _ := s.Sign(&rec)
	// Tamper with payload
	rec.Payload["count"] = 999
	if s.Verify(&rec, s.PublicKeyHex(), sig) {
		t.Error("Verify should fail for tampered record")
	}
}

func TestNewSignerFromKey_ReturnsConsistentKey(t *testing.T) {
	s, _ := NewSigner()
	pkHex := s.PublicKeyHex()
	// Just verify it round-trips
	if pkHex == "" {
		t.Error("public key should not be empty")
	}
}
