// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// record.go -- core L4 data structures and hash computation helpers.

package l4

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

const StatusPending   = "pending"
const StatusConfirmed = "confirmed"
const StatusRevoked   = "revoked"
const GenesisHash     = "genesis"

// L4Record represents a single entry in the distributed ledger.
type L4Record struct {
	UUID       string                 `json:"uuid"`
	AppID      string                 `json:"app_id"`
	Payload    map[string]interface{} `json:"payload"`
	Hash       string                 `json:"hash"`
	PrevHash   string                 `json:"prev_hash"`
	Timestamp  int64                  `json:"timestamp"`
	VerifiedAt string                 `json:"verified_at"`
	NodeID     string                 `json:"node_id"`
	NodeSig    []byte                 `json:"node_sig"`
	UserSig    []byte                 `json:"user_sig,omitempty"`
	Revoked    bool                   `json:"revoked"`
	Confirmed  bool                   `json:"confirmed"`
	Status     string                 `json:"status"`
}

// L4RevocationRecord is a signed revocation referencing an existing record.
type L4RevocationRecord struct {
	UUID          string `json:"uuid"`
	AppID         string `json:"app_id"`
	Reason        string `json:"reason"`
	RevokedAt     int64  `json:"revoked_at"`
	RevokerNodeID string `json:"revoker_node_id"`
	RevokerSig    []byte `json:"revoker_sig"`
	UserSig       []byte `json:"user_sig,omitempty"`
	Hash          string `json:"hash"`
	PrevHash      string `json:"prev_hash"`
}

// L4Status reports the operational status of the L4 layer.
type L4Status struct {
	Enabled     bool
	Mode        string
	PeerCount   int
	BlockHeight int64
	Pending     int
	NodeID      string
	Uptime      string
}

// L4Peer holds information about a peer node.
type L4Peer struct {
	Address     string
	NodeID      string
	LastSeen    time.Time
	BlockHeight int64
	Latency     time.Duration
	Trusted     bool
}

// ComputeHash calculates the SHA-256 hash of the record and stores it in r.Hash.
func (r *L4Record) ComputeHash() (string, error) {
	payloadBytes, err := marshalPayloadSorted(r.Payload)
	if err != nil {
		return "", fmt.Errorf("l4: marshal payload for hash: %w", err)
	}
	raw := fmt.Sprintf("%s|%s|%s|%s|%d", r.PrevHash, r.AppID, r.UUID, string(payloadBytes), r.Timestamp)
	sum := sha256.Sum256([]byte(raw))
	h := hex.EncodeToString(sum[:])
	r.Hash = h
	return h, nil
}

// marshalPayloadSorted serialises a map with sorted keys for deterministic output.
func marshalPayloadSorted(payload map[string]interface{}) ([]byte, error) {
	if len(payload) == 0 {
		return []byte("{}"), nil
	}
	keys := make([]string, 0, len(payload))
	for k := range payload {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	type kv struct {
		K string      `json:"K"`
		V interface{} `json:"V"`
	}
	ordered := make([]kv, len(keys))
	for i, k := range keys {
		ordered[i] = kv{K: k, V: payload[k]}
	}
	return json.Marshal(ordered)
}

// ComputeRevocationHash calculates the hash for a revocation record.
func (r *L4RevocationRecord) ComputeRevocationHash() (string, error) {
	raw := fmt.Sprintf("%s|%s|%s|%s|%d", r.PrevHash, r.AppID, r.UUID, r.Reason, r.RevokedAt)
	sum := sha256.Sum256([]byte(raw))
	h := hex.EncodeToString(sum[:])
	r.Hash = h
	return h, nil
}

// VerifiedAtFromTimestamp derives the coarse "YYYY-MM" string from Unix nanoseconds.
func VerifiedAtFromTimestamp(ts int64) string {
	t := time.Unix(0, ts).UTC()
	return fmt.Sprintf("%04d-%02d", t.Year(), t.Month())
}
