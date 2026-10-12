package strata_test

// correctness_pg_test.go — integration tests for transactional atomicity,
// omit_cache persistence, optimistic concurrency (strata:"version"),
// insert-only writes, and interactive transactions (RunTx).
// Requires Docker; skipped otherwise.

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AndrewDonelson/strata"
	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	testcontainers "github.com/testcontainers/testcontainers-go"
	tcpg "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// ─── Fixtures ────────────────────────────────────────────────────────────────

type ccAccount struct {
	ID           string `strata:"primary_key"`
	Email        string `strata:"unique"`
	PasswordHash string `strata:"omit_cache"`
	Secret       string `strata:"omit_cache,encrypted"`
}

type ccWallet struct {
	ID      string `strata:"primary_key"`
	Balance int64
	Version int64 `strata:"version"`
}

type ccLedger struct {
	ID      string `strata:"primary_key"`
	Wallet  string `strata:"index"`
	IdemKey string `strata:"unique"`
	Amount  int64
}

type ccItem struct {
	ID   string `strata:"primary_key"`
	Name string
}

// pgDSN starts one Postgres container for the calling test and returns its DSN.
func pgDSN(t *testing.T) string {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	pgc, err := tcpg.Run(ctx, pgTestImage,
		tcpg.WithDatabase(pgTestDB),
		tcpg.WithUsername(pgTestUser),
		tcpg.WithPassword(pgTestPass),
		tcpg.BasicWaitStrategies(),
	)
	require.NoError(t, err, "start postgres container")
	t.Cleanup(func() { _ = pgc.Terminate(ctx) })
	dsn, err := pgc.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	return dsn
}

// newCCStore builds a full L1+L2+L3 DataStore with all correctness schemas
// registered and migrated. Each call gets its own Redis.
func newCCStore(t *testing.T, dsn string) *strata.DataStore {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	ds, err := strata.NewDataStore(strata.Config{
		PostgresDSN:   dsn,
		RedisAddr:     mr.Addr(),
		DefaultL1TTL:  5 * time.Minute,
		DefaultL2TTL:  30 * time.Minute,
		EncryptionKey: key,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	for _, s := range []strata.Schema{
		{Name: "cc_account", Model: &ccAccount{}},
		{Name: "cc_wallet", Model: &ccWallet{}},
		{Name: "cc_ledger", Model: &ccLedger{}},
		{Name: "cc_item", Model: &ccItem{}},
	} {
		require.NoError(t, ds.Register(s))
	}
	require.NoError(t, ds.Migrate(context.Background()))
	return ds
}

// ─── Tests ───────────────────────────────────────────────────────────────────

func TestCorrectnessPG(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	dsn := pgDSN(t)
	ctx := context.Background()

	// Bug 1: queued Sets must run inside the Postgres transaction. When a later
	// op fails, earlier Sets must be rolled back.
	t.Run("TxRollsBackSets", func(t *testing.T) {
		ds := newCCStore(t, dsn)
		require.NoError(t, ds.Set(ctx, "cc_ledger", "taken",
			&ccLedger{ID: "taken", IdemKey: "dup-key"}))

		err := ds.Tx(ctx).
			Set("cc_item", "tx-a", &ccItem{ID: "tx-a", Name: "should roll back"}).
			Set("cc_ledger", "dup", &ccLedger{ID: "dup", IdemKey: "dup-key"}). // unique violation
			Commit()
		require.ErrorIs(t, err, strata.ErrTxFailed)

		// A fresh store has empty caches, so this reads Postgres.
		fresh := newCCStore(t, dsn)
		var it ccItem
		err = fresh.Get(ctx, "cc_item", "tx-a", &it)
		assert.ErrorIs(t, err, strata.ErrNotFound, "Set before the failing op must be rolled back")

		// The failed Tx must not have polluted the caches of the store that ran it.
		err = ds.Get(ctx, "cc_item", "tx-a", &it)
		assert.ErrorIs(t, err, strata.ErrNotFound)
	})

	// Bug 2: omit_cache fields are persisted to Postgres, kept out of L1/L2,
	// and still returned by Get whichever tier serves it.
	t.Run("OmitCachePersistedNotCached", func(t *testing.T) {
		ds := newCCStore(t, dsn)
		require.NoError(t, ds.Set(ctx, "cc_account", "u1", &ccAccount{
			ID: "u1", Email: "u1@example.com", PasswordHash: "argon2-hash", Secret: "s3cret",
		}))

		// Served from L1 (just written): hidden fields still come back.
		var a ccAccount
		require.NoError(t, ds.Get(ctx, "cc_account", "u1", &a))
		assert.Equal(t, "argon2-hash", a.PasswordHash)
		assert.Equal(t, "s3cret", a.Secret)

		// Served from Postgres on a cold store.
		fresh := newCCStore(t, dsn)
		var b ccAccount
		require.NoError(t, fresh.Get(ctx, "cc_account", "u1", &b))
		assert.Equal(t, "argon2-hash", b.PasswordHash, "omit_cache field must be stored in Postgres")
		assert.Equal(t, "s3cret", b.Secret, "encrypted omit_cache field must round-trip")

		// Served from the cold store's L1 after back-fill: still complete.
		var c ccAccount
		require.NoError(t, fresh.Get(ctx, "cc_account", "u1", &c))
		assert.Equal(t, "argon2-hash", c.PasswordHash)
	})

	t.Run("OmitCacheInsideTx", func(t *testing.T) {
		ds := newCCStore(t, dsn)
		require.NoError(t, ds.Tx(ctx).
			Set("cc_account", "u2", &ccAccount{ID: "u2", Email: "u2@example.com", PasswordHash: "h2"}).
			Commit())
		fresh := newCCStore(t, dsn)
		var a ccAccount
		require.NoError(t, fresh.Get(ctx, "cc_account", "u2", &a))
		assert.Equal(t, "h2", a.PasswordHash)
	})

	// Feature: optimistic concurrency via strata:"version".
	t.Run("VersionConflict", func(t *testing.T) {
		ds := newCCStore(t, dsn)
		w := &ccWallet{ID: "w1", Balance: 100}
		require.NoError(t, ds.Set(ctx, "cc_wallet", "w1", w))
		assert.Equal(t, int64(1), w.Version, "first write sets version 1")

		// A second create of the same id is a conflict, not an overwrite.
		err := ds.Set(ctx, "cc_wallet", "w1", &ccWallet{ID: "w1", Balance: 999})
		require.ErrorIs(t, err, strata.ErrConflict)

		// Two writers read version 1; only the first update wins.
		a := &ccWallet{ID: "w1", Balance: 90, Version: 1}
		b := &ccWallet{ID: "w1", Balance: 80, Version: 1}
		require.NoError(t, ds.Set(ctx, "cc_wallet", "w1", a))
		assert.Equal(t, int64(2), a.Version)
		err = ds.Set(ctx, "cc_wallet", "w1", b)
		require.ErrorIs(t, err, strata.ErrConflict)
		assert.Equal(t, int64(1), b.Version, "failed write must not bump the caller's version")

		err = ds.Set(ctx, "cc_wallet", "w1", ccWallet{ID: "w1", Balance: 1, Version: 2})
		require.ErrorIs(t, err, strata.ErrInvalidModel, "versioned Set needs a pointer")

		fresh := newCCStore(t, dsn)
		var got ccWallet
		require.NoError(t, fresh.Get(ctx, "cc_wallet", "w1", &got))
		assert.Equal(t, int64(90), got.Balance)
		assert.Equal(t, int64(2), got.Version)
	})

	t.Run("VersionConflictInTx", func(t *testing.T) {
		ds := newCCStore(t, dsn)
		require.NoError(t, ds.Set(ctx, "cc_wallet", "w2", &ccWallet{ID: "w2", Balance: 5}))
		err := ds.Tx(ctx).
			Set("cc_item", "tx-v", &ccItem{ID: "tx-v"}).
			Set("cc_wallet", "w2", &ccWallet{ID: "w2", Balance: 1, Version: 7}). // stale
			Commit()
		require.ErrorIs(t, err, strata.ErrTxFailed)
		require.ErrorIs(t, err, strata.ErrConflict)
		ok, err := newCCStore(t, dsn).Exists(ctx, "cc_item", "tx-v")
		require.NoError(t, err)
		assert.False(t, ok)
	})

	// Feature: insert-only writes.
	t.Run("Insert", func(t *testing.T) {
		ds := newCCStore(t, dsn)
		require.NoError(t, ds.Insert(ctx, "cc_ledger", "l1",
			&ccLedger{ID: "l1", Wallet: "w", IdemKey: "order-1", Amount: 10}))

		err := ds.Insert(ctx, "cc_ledger", "l1", &ccLedger{ID: "l1", Wallet: "w", IdemKey: "order-x", Amount: 99})
		require.ErrorIs(t, err, strata.ErrAlreadyExists, "same primary key")

		err = ds.Insert(ctx, "cc_ledger", "l2", &ccLedger{ID: "l2", Wallet: "w", IdemKey: "order-1", Amount: 10})
		require.ErrorIs(t, err, strata.ErrAlreadyExists, "same unique idempotency key")

		var got ccLedger
		require.NoError(t, newCCStore(t, dsn).Get(ctx, "cc_ledger", "l1", &got))
		assert.Equal(t, int64(10), got.Amount, "Insert must never overwrite")

		err = ds.Tx(ctx).
			Insert("cc_item", "tx-i", &ccItem{ID: "tx-i"}).
			Insert("cc_ledger", "l1", &ccLedger{ID: "l1", IdemKey: "order-y"}).
			Commit()
		require.ErrorIs(t, err, strata.ErrAlreadyExists)
		ok, err := newCCStore(t, dsn).Exists(ctx, "cc_item", "tx-i")
		require.NoError(t, err)
		assert.False(t, ok)
	})

	// Feature: interactive transactions with isolation and retry.
	t.Run("RunTxSerializableNoLostUpdates", func(t *testing.T) {
		ds := newCCStore(t, dsn)
		require.NoError(t, ds.Insert(ctx, "cc_item", "counter", &ccItem{ID: "counter", Name: "0"}))
		require.NoError(t, ds.Insert(ctx, "cc_wallet", "w3", &ccWallet{ID: "w3", Balance: 100}))

		// 20 concurrent debits of 10 against a balance of 100: exactly 10 succeed.
		var ok, insufficient atomic.Int32
		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := ds.RunTx(ctx, strata.TxOptions{Isolation: strata.Serializable, MaxRetries: 50},
					func(tx *strata.TxScope) error {
						var w ccWallet
						if err := tx.Get("cc_wallet", "w3", &w); err != nil {
							return err
						}
						if w.Balance < 10 {
							return strata.ErrInsufficientFunds
						}
						w.Balance -= 10
						return tx.Set("cc_wallet", "w3", &w)
					})
				switch {
				case err == nil:
					ok.Add(1)
				case errors.Is(err, strata.ErrInsufficientFunds):
					insufficient.Add(1)
				default:
					t.Errorf("unexpected error: %v", err)
				}
			}()
		}
		wg.Wait()
		assert.Equal(t, int32(10), ok.Load())
		assert.Equal(t, int32(10), insufficient.Load())

		var w ccWallet
		require.NoError(t, newCCStore(t, dsn).Get(ctx, "cc_wallet", "w3", &w))
		assert.Equal(t, int64(0), w.Balance)

		// The store that ran the transactions must serve the committed value.
		require.NoError(t, ds.Get(ctx, "cc_wallet", "w3", &w))
		assert.Equal(t, int64(0), w.Balance)
	})

	t.Run("RunTxRollbackOnError", func(t *testing.T) {
		ds := newCCStore(t, dsn)
		boom := errors.New("boom")
		err := ds.RunTx(ctx, strata.TxOptions{}, func(tx *strata.TxScope) error {
			if err := tx.Set("cc_item", "rt-1", &ccItem{ID: "rt-1"}); err != nil {
				return err
			}
			return boom
		})
		require.ErrorIs(t, err, boom, "fn's own error is returned unchanged")
		ok, err := ds.Exists(ctx, "cc_item", "rt-1")
		require.NoError(t, err)
		assert.False(t, ok, "rolled-back write must not reach any tier")
	})

	t.Run("RunTxReadsOwnWritesAndSearch", func(t *testing.T) {
		ds := newCCStore(t, dsn)
		err := ds.RunTx(ctx, strata.TxOptions{Isolation: strata.RepeatableRead}, func(tx *strata.TxScope) error {
			for _, id := range []string{"s1", "s2"} {
				if err := tx.Insert("cc_ledger", id, &ccLedger{ID: id, Wallet: "wx", IdemKey: "k-" + id, Amount: 5}); err != nil {
					return err
				}
			}
			q := strata.Q().Where("wallet = $1", "wx").Build()
			var rows []ccLedger
			if err := tx.Search("cc_ledger", &q, &rows); err != nil {
				return err
			}
			if len(rows) != 2 {
				return errors.New("transaction must see its own inserts")
			}
			var one ccLedger
			if err := tx.GetForUpdate("cc_ledger", "s1", &one); err != nil {
				return err
			}
			if ok, err := tx.Exists("cc_ledger", "s2"); err != nil || !ok {
				return errors.New("Exists must see own insert")
			}
			return tx.Delete("cc_ledger", "s2")
		})
		require.NoError(t, err)
		fresh := newCCStore(t, dsn)
		ok, err := fresh.Exists(ctx, "cc_ledger", "s1")
		require.NoError(t, err)
		assert.True(t, ok)
		ok, err = fresh.Exists(ctx, "cc_ledger", "s2")
		require.NoError(t, err)
		assert.False(t, ok)
	})
}
