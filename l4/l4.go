// Package l4 re-exports the Strata L4 distributed sync layer for first-party
// consumers (such as LADL) that need direct access to the ledger primitives.
//
// This package is a thin shim; all implementation lives in internal/l4.
package l4

import (
	internal "github.com/AndrewDonelson/strata/internal/l4"
)

// Re-export core interfaces.
type (
	L4Layer         = internal.L4Layer
	L4Signer        = internal.L4Signer
	L4Store         = internal.L4Store
	L4Transport     = internal.L4Transport
	MemTransportHub = internal.MemTransportHub
	L4Record        = internal.L4Record
	L4Status        = internal.L4Status
	RecordHandler   = internal.RecordHandler
)

// Re-export Config.
type Config = internal.Config

// Re-export sentinel errors.
var (
	ErrAlreadyPublished = internal.ErrAlreadyPublished
	ErrNotFound         = internal.ErrNotFound
	ErrAlreadyRevoked   = internal.ErrAlreadyRevoked
	ErrInvalidSignature = internal.ErrInvalidSignature
	ErrChainBreak       = internal.ErrChainBreak
	ErrNoPeers          = internal.ErrNoPeers
	ErrQuorumNotMet     = internal.ErrQuorumNotMet
	ErrL4Disabled       = internal.ErrL4Disabled
	ErrStoreUnavailable = internal.ErrStoreUnavailable
)

// Re-export constructors.

// New creates a fully-wired L4Layer from a Config (uses BoltDB + TCP transport).
var New = internal.New

// NewWithComponents creates an L4Layer with injected dependencies (useful for tests).
var NewWithComponents = internal.NewWithComponents

// NewSigner creates a fresh Ed25519 L4Signer.
var NewSigner = internal.NewSigner

// NewSignerFromKey creates an L4Signer from an existing private key.
var NewSignerFromKey = internal.NewSignerFromKey

// NewMemStore creates an in-memory L4Store for tests.
var NewMemStore = internal.NewMemStore

// NewBoltStore creates a persistent BoltDB-backed L4Store.
var NewBoltStore = internal.NewBoltStore

// NewMemTransportHub creates a shared in-memory transport hub for tests.
var NewMemTransportHub = internal.NewMemTransportHub

// NewMemTransport creates an in-memory L4Transport connected to hub, for tests.
var NewMemTransport = internal.NewMemTransport
