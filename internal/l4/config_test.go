// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)

package l4

import (
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Mode != "peer" {
		t.Errorf("expected mode peer, got %s", cfg.Mode)
	}
	if cfg.Port != 7743 {
		t.Errorf("expected port 7743, got %d", cfg.Port)
	}
	if cfg.MaxPeers != 50 {
		t.Errorf("expected maxPeers 50, got %d", cfg.MaxPeers)
	}
	if cfg.Quorum != 3 {
		t.Errorf("expected quorum 3, got %d", cfg.Quorum)
	}
	if cfg.SyncInterval != 30*time.Second {
		t.Errorf("expected syncInterval 30s, got %v", cfg.SyncInterval)
	}
}

func TestConfig_Validate_Disabled(t *testing.T) {
	cfg := Config{Enabled: false}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("disabled config should validate: %v", err)
	}
}

func TestConfig_Validate_InvalidMode(t *testing.T) {
	cfg := Config{Enabled: true, Mode: "invalid", Quorum: 1}
	if err := cfg.Validate(); err != ErrInvalidL4Mode {
		t.Fatalf("expected ErrInvalidL4Mode, got %v", err)
	}
}

func TestConfig_Validate_InvalidQuorum(t *testing.T) {
	// Quorum: -1 is not auto-fixed by defaults() (which only fills 0)
	cfg := Config{Enabled: true, Mode: "peer", Quorum: -1}
	if err := cfg.Validate(); err != ErrInvalidQuorum {
		t.Fatalf("expected ErrInvalidQuorum, got %v", err)
	}
}

func TestConfig_Validate_Valid(t *testing.T) {
	cfg := Config{Enabled: true, Mode: "ledger", Quorum: 2}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestConfig_Validate_AppliesDefaults(t *testing.T) {
	cfg := Config{Enabled: true, Mode: "peer", Quorum: 1}
	_ = cfg.Validate()
	if cfg.Port == 0 {
		t.Error("expected Port to be set by defaults")
	}
}
