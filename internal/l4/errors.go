// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// errors.go -- sentinel errors for the L4 distributed sync layer.

// Package l4 provides the optional distributed ledger sync layer for Strata.
package l4

import "errors"

var (
	ErrL4Disabled       = errors.New("l4: layer is disabled in config")
	ErrInvalidL4Mode    = errors.New("l4: mode must be 'peer' or 'ledger'")
	ErrInvalidQuorum    = errors.New("l4: quorum must be >= 1")
	ErrAlreadyPublished = errors.New("l4: record with this UUID and AppID already exists")
	ErrAlreadyRevoked   = errors.New("l4: record is already revoked")
	ErrNotFound         = errors.New("l4: record not found")
	ErrInvalidSignature = errors.New("l4: record signature is invalid")
	ErrChainBreak       = errors.New("l4: chain integrity check failed")
	ErrNoPeers          = errors.New("l4: no peers available")
	ErrQuorumNotMet     = errors.New("l4: quorum not met; record is pending")
	ErrStoreUnavailable = errors.New("l4: local store is unavailable (peer mode)")
	ErrAlreadyExists    = errors.New("l4: store: record already exists")
)
