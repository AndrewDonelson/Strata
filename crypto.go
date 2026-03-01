// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// crypto.go — AES-256-GCM field-level encryption and decryption helpers
// used by Strata to protect sensitive struct fields before they are written
// to L2 (Redis) or L3 (PostgreSQL).

package strata

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
)

// Encryptor encrypts and decrypts field values for fields tagged with "encrypted".
type Encryptor interface {
	Encrypt(plaintext []byte) ([]byte, error)
	Decrypt(ciphertext []byte) ([]byte, error)
}

// AES256GCM implements AES-256-GCM authenticated encryption.
type AES256GCM struct {
	block cipher.Block
}

// NewAES256GCM creates an AES-256-GCM encryptor from a 32-byte key.
func NewAES256GCM(key []byte) (*AES256GCM, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("strata: encryption key must be exactly 32 bytes (got %d)", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return &AES256GCM{block: block}, nil
}

// Encrypt encrypts plaintext using AES-256-GCM with a random nonce.
// Output: nonce (12 bytes) || ciphertext.
func (e *AES256GCM) Encrypt(plaintext []byte) ([]byte, error) {
	gcm, err := cipher.NewGCM(e.block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	// io.ReadFull on rand.Reader (backed by /dev/urandom on Linux) never
	// returns an error in practice.  The branch exists for correctness on
	// exotic platforms or future OS changes.  Covering it would require
	// making the random reader injectable as a production-code change purely
	// for a dead-code path — intentionally left uncovered.
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// Decrypt decrypts ciphertext produced by Encrypt.
func (e *AES256GCM) Decrypt(ciphertext []byte) ([]byte, error) {
	gcm, err := cipher.NewGCM(e.block)
	if err != nil {
		return nil, err
	}
	nsize := gcm.NonceSize()
	if len(ciphertext) < nsize {
		return nil, fmt.Errorf("strata: ciphertext too short")
	}
	return gcm.Open(nil, ciphertext[:nsize], ciphertext[nsize:], nil)
}
