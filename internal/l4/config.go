// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// config.go -- L4 configuration struct, validation, and defaults.

package l4

import "time"

// Config holds all configuration for the L4 distributed sync layer.
type Config struct {
Enabled        bool          `yaml:"enabled"`
Mode           string        `yaml:"mode"`
Port           int           `yaml:"port"`
DataDir        string        `yaml:"data_dir"`
SyncInterval   time.Duration `yaml:"sync_interval"`
MaxPeers       int           `yaml:"max_peers"`
Quorum         int           `yaml:"quorum"`
BootstrapPeers []string      `yaml:"bootstrap_peers"`
DNSSeed        string        `yaml:"dns_seed"`
NodeKeyPath    string        `yaml:"node_key_path"`
}

func (c *Config) defaults() {
if c.Mode == "" { c.Mode = "peer" }
if c.Port == 0 { c.Port = 7743 }
if c.DataDir == "" { c.DataDir = "/var/lib/strata/l4" }
if c.SyncInterval == 0 { c.SyncInterval = 30 * time.Second }
if c.MaxPeers == 0 { c.MaxPeers = 50 }
if c.Quorum == 0 { c.Quorum = 3 }
}

// Validate checks Config is valid. Applies defaults first.
func (c *Config) Validate() error {
if !c.Enabled {
return nil
}
c.defaults()
if c.Mode != "peer" && c.Mode != "ledger" {
return ErrInvalidL4Mode
}
if c.Quorum < 1 {
return ErrInvalidQuorum
}
return nil
}

// DefaultConfig returns a Config with defaults applied and Enabled = false.
func DefaultConfig() Config {
c := Config{}
c.defaults()
return c
}
