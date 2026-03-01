// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// l3_close_test.go — pure unit test for (*Store).Close without Docker.
// A pool with MinConns=0 is created pointing at an unreachable address;
// pgxpool with MinConns=0 does not attempt any connection, so Close()
// exercises pool.Close() on an idle pool (always safe, never panics).

package l3_test

import (
"context"
"testing"

"github.com/AndrewDonelson/strata/internal/l3"
"github.com/jackc/pgx/v5/pgxpool"
"github.com/stretchr/testify/require"
)

// TestL3Store_Close verifies that (*Store).Close() delegates to pool.Close()
// without panicking.  No Docker or live PostgreSQL instance is required.
func TestL3Store_Close(t *testing.T) {
cfg, err := pgxpool.ParseConfig("postgresql://nobody:nothing@127.0.0.1:19999/testdb")
require.NoError(t, err, "DSN must parse successfully")
cfg.MinConns = 0 // no eager connect — pool is idle until first Acquire

pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
require.NoError(t, err, "pool with MinConns=0 must be created without connecting")

store := l3.New(pool, nil)
store.Close() // exercises (*Store).Close → pool.Close; must not panic
}
