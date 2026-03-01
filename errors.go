// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// errors.go — sentinel error variables returned by the public Strata API,
// covering schema registration, cache misses, L3 unavailability, and
// transaction failures.

// Package strata provides a three-tier auto-caching data library unifying
// in-memory (L1), Redis (L2), and PostgreSQL (L3) behind a single API.
package strata

import "errors"

// Schema errors
var (
	ErrSchemaNotFound    = errors.New("strata: schema not registered")
	ErrSchemaDuplicate   = errors.New("strata: schema already registered")
	ErrNoPrimaryKey      = errors.New("strata: struct has no primary_key field")
	ErrInvalidModel      = errors.New("strata: model must be a non-nil pointer to a struct")
	ErrMissingPrimaryKey = errors.New("strata: value is missing primary key")
)

// Data errors
var (
	ErrNotFound     = errors.New("strata: record not found")
	ErrDecodeFailed = errors.New("strata: failed to decode stored value")
	ErrEncodeFailed = errors.New("strata: failed to encode value for storage")
)

// Infrastructure errors
var (
	ErrL1Unavailable = errors.New("strata: L1 memory store unavailable")
	ErrL2Unavailable = errors.New("strata: L2 Redis unavailable")
	ErrL3Unavailable = errors.New("strata: L3 Postgres unavailable")
	ErrUnavailable   = errors.New("strata: all tiers unavailable")
)

// Transaction errors
var (
	ErrTxFailed  = errors.New("strata: transaction failed")
	ErrTxTimeout = errors.New("strata: transaction timeout")
)

// Config errors
var (
	ErrInvalidConfig = errors.New("strata: invalid configuration")
)

// Hook errors
var (
	ErrHookPanic = errors.New("strata: hook panicked")
)

// Write-behind errors
var (
	ErrWriteBehindMaxRetry = errors.New("strata: write-behind exceeded max retries")
)

// Domain errors
var (
	ErrInsufficientFunds = errors.New("strata: insufficient funds")
)
