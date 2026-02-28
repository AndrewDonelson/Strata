package strata_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/AndrewDonelson/strata"
	"github.com/AndrewDonelson/strata/internal/l2"
	"github.com/AndrewDonelson/strata/internal/metrics"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── Query builder ForceL3 / ForceL2 ──────────────────────────────────────────

func TestQuery_ForceL3(t *testing.T) {
	q := strata.Q().ForceL3().Build()
	assert.True(t, q.ForceL3)
}

func TestQuery_ForceL2(t *testing.T) {
	q := strata.Q().ForceL2().Build()
	assert.True(t, q.ForceL2)
}

func TestQuery_ForceL3_ForceL2_Combined(t *testing.T) {
	q := strata.Q().ForceL3().ForceL2().Build()
	assert.True(t, q.ForceL3)
	assert.True(t, q.ForceL2)
}

// ── Tx.Delete covers the Delete branch ───────────────────────────────────────

func TestTx_Delete_ErrL3Unavailable(t *testing.T) {
	ds := newDS(t)
	registerProduct(t, ds)

	ctx := context.Background()
	// Queue a Delete op then Commit → ErrL3Unavailable (no L3 configured)
	err := ds.Tx(ctx).Delete("product", "some-id").Commit()
	assert.ErrorIs(t, err, strata.ErrL3Unavailable)
}

func TestTx_SetAndDelete_ErrL3Unavailable(t *testing.T) {
	ds := newDS(t)
	registerProduct(t, ds)

	ctx := context.Background()
	tx := ds.Tx(ctx).
		Set("product", "p1", &Product{ID: "p1"}).
		Delete("product", "p2")
	err := tx.Commit()
	assert.ErrorIs(t, err, strata.ErrL3Unavailable)
}

// ── NewDataStore with custom Logger and Metrics ───────────────────────────────

type testLogger struct {
	buf bytes.Buffer
}

func (l *testLogger) Info(msg string, kv ...any)  { l.buf.WriteString("INFO:" + msg + "\n") }
func (l *testLogger) Warn(msg string, kv ...any)  { l.buf.WriteString("WARN:" + msg + "\n") }
func (l *testLogger) Error(msg string, kv ...any) { l.buf.WriteString("ERR:" + msg + "\n") }
func (l *testLogger) Debug(msg string, kv ...any) { l.buf.WriteString("DBG:" + msg + "\n") }

func TestDataStore_NewDataStore_WithLogger(t *testing.T) {
	log := &testLogger{}
	ds, err := strata.NewDataStore(strata.Config{Logger: log})
	require.NoError(t, err)
	defer ds.Close()
	// DataStore created without error; logger is stored
	assert.NotNil(t, ds)
}

func TestDataStore_NewDataStore_WithMetrics(t *testing.T) {
	ds, err := strata.NewDataStore(strata.Config{Metrics: metrics.Noop{}})
	require.NoError(t, err)
	defer ds.Close()

	// Perform operations to exercise metrics recording paths
	require.NoError(t, ds.Register(strata.Schema{Name: "met_prod", Model: &Product{}}))
	ctx := context.Background()
	_ = ds.Set(ctx, "met_prod", "m1", &Product{ID: "m1"})
	var p Product
	_ = ds.Get(ctx, "met_prod", "m1", &p)
	_ = ds.Get(ctx, "met_prod", "no-such", &p) // L1 miss, L2 miss, no L3
}

// ── AES256GCM: short ciphertext ───────────────────────────────────────────────

func TestAES256GCM_ShortCiphertext(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	enc, err := strata.NewAES256GCM(key)
	require.NoError(t, err)

	// AES-GCM nonce size is 12 bytes; a 5-byte "ciphertext" is too short
	_, err = enc.Decrypt([]byte("short"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ciphertext too short")
}

// ── l2.New with nil Codec defaults to JSON ────────────────────────────────────

func TestL2_New_NilCodec(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	store := l2.New(l2.Options{Client: client}) // nil Codec → defaults to JSON
	require.NotNil(t, store)

	ctx := context.Background()
	type Item struct{ Name string }
	require.NoError(t, store.SetRaw(ctx, "testkey", []byte(`"hello"`), 0))
	raw, err := store.GetRaw(ctx, "testkey")
	require.NoError(t, err)
	assert.Equal(t, []byte(`"hello"`), raw)
}

// ── FlushDirty with no sync engine ────────────────────────────────────────────

func TestDataStore_FlushDirty_NoSync(t *testing.T) {
	// A DataStore always has a sync engine, but test FlushDirty when it exists
	ds := newDS(t)
	err := ds.FlushDirty(context.Background())
	assert.NoError(t, err) // No dirty entries → succeeds immediately
}

// ── Close with L2 ─────────────────────────────────────────────────────────────

func TestDataStore_Close_WithL2(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	ds, err := strata.NewDataStore(strata.Config{RedisAddr: mr.Addr()})
	require.NoError(t, err)
	assert.NoError(t, ds.Close())
}

// ── NewDataStore with custom L1 pool settings ─────────────────────────────────

func TestDataStore_NewDataStore_CustomL1Pool(t *testing.T) {
	ds, err := strata.NewDataStore(strata.Config{
		L1Pool: strata.L1PoolConfig{
			MaxEntries: 500,
			Eviction:   strata.EvictLFU,
		},
	})
	require.NoError(t, err)
	defer ds.Close()

	require.NoError(t, ds.Register(strata.Schema{Name: "custom_l1", Model: &Product{}}))
	ctx := context.Background()
	require.NoError(t, ds.Set(ctx, "custom_l1", "c1", &Product{ID: "c1"}))
	var p Product
	require.NoError(t, ds.Get(ctx, "custom_l1", "c1", &p))
	assert.Equal(t, "c1", p.ID)
}
