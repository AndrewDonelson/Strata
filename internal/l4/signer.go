// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// signer.go -- L4Signer implementation using Ed25519.

package l4

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// L4Signer manages the node Ed25519 cryptographic identity.
type L4Signer interface {
	PublicKeyHex() string
	Sign(record *L4Record) ([]byte, error)
	Verify(record *L4Record, publicKeyHex string, sig []byte) bool
	CanonicalBytes(record *L4Record) []byte
}

type nodeSigner struct {
	pubKey  ed25519.PublicKey
	privKey ed25519.PrivateKey
}

// NewSigner generates a fresh Ed25519 keypair.
func NewSigner() (L4Signer, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("l4: generate keypair: %w", err)
	}
	return &nodeSigner{pubKey: pub, privKey: priv}, nil
}

// NewSignerFromKey creates a signer from an existing Ed25519 private key.
func NewSignerFromKey(privKey ed25519.PrivateKey) L4Signer {
	return &nodeSigner{pubKey: privKey.Public().(ed25519.PublicKey), privKey: privKey}
}

func (s *nodeSigner) PublicKeyHex() string {
	return hex.EncodeToString(s.pubKey)
}

func (s *nodeSigner) CanonicalBytes(record *L4Record) []byte {
	payloadBytes, _ := marshalPayloadSorted(record.Payload)
	raw := fmt.Sprintf("%s|%s|%s|%s|%d",
		record.PrevHash, record.AppID, record.UUID, string(payloadBytes), record.Timestamp)
	return []byte(raw)
}

func (s *nodeSigner) Sign(record *L4Record) ([]byte, error) {
	msg := s.CanonicalBytes(record)
	return ed25519.Sign(s.privKey, msg), nil
}

func (s *nodeSigner) Verify(record *L4Record, publicKeyHex string, sig []byte) bool {
	pubBytes, err := hex.DecodeString(publicKeyHex)
	if err != nil || len(pubBytes) != ed25519.PublicKeySize {
		return false
	}
	msg := s.CanonicalBytes(record)
	return ed25519.Verify(ed25519.PublicKey(pubBytes), msg, sig)
}
