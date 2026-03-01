package strata_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/AndrewDonelson/strata"
	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── Error path tests ──────────────────────────────────────────────────────────

func TestDataStore_Search_NoL3(t *testing.T) {
	ds := newDS(t)
	registerProduct(t, ds)

	var results []Product
	err := ds.Search(context.Background(), "product", nil, &results)
	// No L3 configured → ErrL3Unavailable
	assert.ErrorIs(t, err, strata.ErrL3Unavailable)
}

func TestDataStore_SearchTyped_NoL3(t *testing.T) {
	ds := newDS(t)
	registerProduct(t, ds)

	_, err := strata.SearchTyped[Product](context.Background(), ds, "product", nil)
	assert.ErrorIs(t, err, strata.ErrL3Unavailable)
}

func TestDataStore_SearchCached_NoL2NoL3(t *testing.T) {
	ds := newDS(t)
	registerProduct(t, ds)

	var results []Product
	err := ds.SearchCached(context.Background(), "product", nil, &results)
	// No L2 → falls back to Search → No L3 → ErrL3Unavailable
	assert.ErrorIs(t, err, strata.ErrL3Unavailable)
}

func TestDataStore_Count_NoL3(t *testing.T) {
	ds := newDS(t)
	registerProduct(t, ds)

	_, err := ds.Count(context.Background(), "product", nil)
	assert.ErrorIs(t, err, strata.ErrL3Unavailable)
}

func TestDataStore_WarmCache_NoL3(t *testing.T) {
	ds := newDS(t)
	registerProduct(t, ds)

	err := ds.WarmCache(context.Background(), "product", 100)
	assert.ErrorIs(t, err, strata.ErrL3Unavailable)
}

func TestDataStore_Tx_NoL3(t *testing.T) {
	ds := newDS(t)
	registerProduct(t, ds)

	err := ds.Tx(context.Background()).
		Set("product", "p1", &Product{ID: "p1", Name: "tx", Price: 1}).
		Commit()
	assert.ErrorIs(t, err, strata.ErrL3Unavailable)
}

// ── Unknown schema errors ─────────────────────────────────────────────────────

func TestDataStore_Get_UnknownSchema(t *testing.T) {
	ds := newDS(t)
	var p Product
	err := ds.Get(context.Background(), "no-such-schema", "id", &p)
	assert.ErrorIs(t, err, strata.ErrSchemaNotFound)
}

func TestDataStore_Set_UnknownSchema(t *testing.T) {
	ds := newDS(t)
	err := ds.Set(context.Background(), "no-such-schema", "id", &Product{ID: "id"})
	assert.ErrorIs(t, err, strata.ErrSchemaNotFound)
}

func TestDataStore_Delete_UnknownSchema(t *testing.T) {
	ds := newDS(t)
	err := ds.Delete(context.Background(), "no-such-schema", "id")
	assert.ErrorIs(t, err, strata.ErrSchemaNotFound)
}

func TestDataStore_Exists_UnknownSchema(t *testing.T) {
	ds := newDS(t)
	_, err := ds.Exists(context.Background(), "no-such-schema", "id")
	assert.ErrorIs(t, err, strata.ErrSchemaNotFound)
}

func TestDataStore_Invalidate_UnknownSchema(t *testing.T) {
	// Invalidate does not consult the schema registry; it silently clears caches.
	ds := newDS(t)
	err := ds.Invalidate(context.Background(), "no-such-schema", "id")
	assert.NoError(t, err, "Invalidate on unknown schema should not error")
}

func TestDataStore_InvalidateAll_UnknownSchema(t *testing.T) {
	ds := newDS(t)
	err := ds.InvalidateAll(context.Background(), "no-such-schema")
	assert.ErrorIs(t, err, strata.ErrSchemaNotFound)
}

func TestDataStore_Search_UnknownSchema(t *testing.T) {
	ds := newDS(t)
	var results []Product
	err := ds.Search(context.Background(), "no-such-schema", nil, &results)
	assert.ErrorIs(t, err, strata.ErrSchemaNotFound)
}

func TestDataStore_Count_UnknownSchema(t *testing.T) {
	ds := newDS(t)
	_, err := ds.Count(context.Background(), "no-such-schema", nil)
	assert.ErrorIs(t, err, strata.ErrSchemaNotFound)
}

func TestDataStore_WarmCache_UnknownSchema(t *testing.T) {
	ds := newDS(t)
	err := ds.WarmCache(context.Background(), "no-such-schema", 10)
	assert.ErrorIs(t, err, strata.ErrSchemaNotFound)
}

// ── Closed DataStore ──────────────────────────────────────────────────────────

func TestDataStore_ClosedOperations(t *testing.T) {
	ds, err := strata.NewDataStore(strata.Config{})
	require.NoError(t, err)
	require.NoError(t, ds.Register(strata.Schema{Name: "product_closed", Model: &Product{}}))
	require.NoError(t, ds.Close())

	ctx := context.Background()
	var p Product

	// All operations on closed store should return ErrUnavailable
	assert.ErrorIs(t, ds.Get(ctx, "product_closed", "x", &p), strata.ErrUnavailable)
	assert.ErrorIs(t, ds.Set(ctx, "product_closed", "x", &Product{ID: "x"}), strata.ErrUnavailable)
	assert.ErrorIs(t, ds.Delete(ctx, "product_closed", "x"), strata.ErrUnavailable)
	assert.ErrorIs(t, ds.Invalidate(ctx, "product_closed", "x"), strata.ErrUnavailable)
	assert.ErrorIs(t, ds.InvalidateAll(ctx, "product_closed"), strata.ErrUnavailable)
	_, exErr := ds.Exists(ctx, "product_closed", "x")
	assert.ErrorIs(t, exErr, strata.ErrUnavailable)
	_, cntErr := ds.Count(ctx, "product_closed", nil)
	assert.ErrorIs(t, cntErr, strata.ErrUnavailable)

	var results []Product
	assert.ErrorIs(t, ds.Search(ctx, "product_closed", nil, &results), strata.ErrUnavailable)
	assert.ErrorIs(t, ds.WarmCache(ctx, "product_closed", 10), strata.ErrUnavailable)
}

func TestDataStore_Close_Idempotent(t *testing.T) {
	ds, err := strata.NewDataStore(strata.Config{})
	require.NoError(t, err)
	assert.NoError(t, ds.Close())
	assert.NoError(t, ds.Close()) // second close must not error
	assert.NoError(t, ds.Close()) // third close must not error
}

// ── Write modes ───────────────────────────────────────────────────────────────

func TestDataStore_WriteBehind_SetAndRetrieve(t *testing.T) {
	ds, err := strata.NewDataStore(strata.Config{DefaultWriteMode: strata.WriteBehind})
	require.NoError(t, err)
	defer ds.Close()
	require.NoError(t, ds.Register(strata.Schema{Name: "wb_prod", Model: &Product{}}))

	ctx := context.Background()
	p := &Product{ID: "wb1", Name: "WriteBehind", Price: 42}
	require.NoError(t, ds.Set(ctx, "wb_prod", "wb1", p))

	var got Product
	require.NoError(t, ds.Get(ctx, "wb_prod", "wb1", &got))
	assert.Equal(t, "WriteBehind", got.Name)
}

func TestDataStore_WriteThroughL1Async_Set(t *testing.T) {
	ds, err := strata.NewDataStore(strata.Config{DefaultWriteMode: strata.WriteThroughL1Async})
	require.NoError(t, err)
	defer ds.Close()
	require.NoError(t, ds.Register(strata.Schema{Name: "l1a_prod", Model: &Product{}}))

	ctx := context.Background()
	p := &Product{ID: "la1", Name: "L1Async", Price: 7}
	require.NoError(t, ds.Set(ctx, "l1a_prod", "la1", p))

	// Give tiny pause for async L1 goroutine
	time.Sleep(5 * time.Millisecond)

	var got Product
	require.NoError(t, ds.Get(ctx, "l1a_prod", "la1", &got))
	assert.Equal(t, "L1Async", got.Name)
}

// ── Schema registration errors ────────────────────────────────────────────────

func TestDataStore_Register_DuplicateName(t *testing.T) {
	ds := newDS(t)
	require.NoError(t, ds.Register(strata.Schema{Name: "dup", Model: &Product{}}))
	err := ds.Register(strata.Schema{Name: "dup", Model: &Product{}})
	assert.ErrorIs(t, err, strata.ErrSchemaDuplicate)
}

func TestDataStore_Register_NilModel(t *testing.T) {
	ds := newDS(t)
	err := ds.Register(strata.Schema{Name: "nilmodel", Model: nil})
	assert.ErrorIs(t, err, strata.ErrInvalidModel)
}

func TestDataStore_Register_NonPointerModel(t *testing.T) {
	ds := newDS(t)
	err := ds.Register(strata.Schema{Name: "nonptr", Model: Product{}})
	assert.ErrorIs(t, err, strata.ErrInvalidModel)
}

func TestDataStore_Register_NoPrimaryKey(t *testing.T) {
	type NoPK struct{ Name string }
	ds := newDS(t)
	err := ds.Register(strata.Schema{Name: "nopk", Model: &NoPK{}})
	assert.ErrorIs(t, err, strata.ErrNoPrimaryKey)
}

// ── Query options ─────────────────────────────────────────────────────────────

func TestDataStore_Search_WithNilQuery(t *testing.T) {
	ds := newDS(t)
	registerProduct(t, ds)
	var results []Product
	err := ds.Search(context.Background(), "product", nil, &results)
	// no L3 → ErrL3Unavailable; important: nil query is accepted
	assert.ErrorIs(t, err, strata.ErrL3Unavailable)
}

func TestDataStore_Count_WithQuery(t *testing.T) {
	ds := newDS(t)
	registerProduct(t, ds)
	q := strata.Q().Where("price > $1", 5.0).Build()
	_, err := ds.Count(context.Background(), "product", &q)
	assert.ErrorIs(t, err, strata.ErrL3Unavailable)
}

// ── Hooks ─────────────────────────────────────────────────────────────────────

func TestDataStore_BeforeSet_Hook_Abort(t *testing.T) {
	ds := newDS(t)
	errSentinel := errors.New("hook abort")
	require.NoError(t, ds.Register(strata.Schema{
		Name:  "hooked",
		Model: &Product{},
		Hooks: strata.SchemaHooks{
			BeforeSet: func(_ context.Context, _ any) error { return errSentinel },
		},
	}))

	err := ds.Set(context.Background(), "hooked", "h1", &Product{ID: "h1"})
	assert.ErrorIs(t, err, errSentinel)
}

func TestDataStore_AfterSet_Hook_Called(t *testing.T) {
	ds := newDS(t)
	var called bool
	var mu sync.Mutex
	require.NoError(t, ds.Register(strata.Schema{
		Name:  "afterset",
		Model: &Product{},
		Hooks: strata.SchemaHooks{
			AfterSet: func(_ context.Context, _ any) {
				mu.Lock()
				called = true
				mu.Unlock()
			},
		},
	}))

	require.NoError(t, ds.Set(context.Background(), "afterset", "a1", &Product{ID: "a1"}))
	mu.Lock()
	defer mu.Unlock()
	assert.True(t, called)
}

// TestDataStore_BeforeGet_Hook_Register verifies that a schema with a BeforeGet
// hook can be registered and used. (BeforeGet is declared in SchemaHooks but is
// not currently invoked by the DataStore runtime.)
func TestDataStore_BeforeGet_Hook_Register(t *testing.T) {
	ds := newDS(t)
	require.NoError(t, ds.Register(strata.Schema{
		Name:  "beforeget",
		Model: &Product{},
		Hooks: strata.SchemaHooks{
			BeforeGet: func(_ context.Context, id string) {
				// hook declared but not called by runtime
			},
		},
	}))

	_ = ds.Set(context.Background(), "beforeget", "bg1", &Product{ID: "bg1"})
	var p Product
	assert.NoError(t, ds.Get(context.Background(), "beforeget", "bg1", &p))
	assert.Equal(t, "bg1", p.ID)
}

func TestDataStore_AfterGet_Hook_Called(t *testing.T) {
	ds := newDS(t)
	var hookVal any
	var mu sync.Mutex
	require.NoError(t, ds.Register(strata.Schema{
		Name:  "afterget",
		Model: &Product{},
		Hooks: strata.SchemaHooks{
			AfterGet: func(_ context.Context, v any) {
				mu.Lock()
				hookVal = v
				mu.Unlock()
			},
		},
	}))

	p := &Product{ID: "ag1", Name: "AfterGet"}
	_ = ds.Set(context.Background(), "afterget", "ag1", p)
	var got Product
	_ = ds.Get(context.Background(), "afterget", "ag1", &got)
	mu.Lock()
	defer mu.Unlock()
	assert.NotNil(t, hookVal)
}

// ── SetMany edge cases ────────────────────────────────────────────────────────

func TestDataStore_SetMany_EmptyMap(t *testing.T) {
	ds := newDS(t)
	registerProduct(t, ds)

	err := ds.SetMany(context.Background(), "product", map[string]any{})
	assert.NoError(t, err)
}

func TestDataStore_SetMany_UnknownSchema(t *testing.T) {
	ds := newDS(t)
	err := ds.SetMany(context.Background(), "no-schema", map[string]any{"k": &Product{ID: "k"}})
	assert.ErrorIs(t, err, strata.ErrSchemaNotFound)
}

// ── Config edge cases ─────────────────────────────────────────────────────────

func TestNewDataStore_InvalidEncryptionKey(t *testing.T) {
	_, err := strata.NewDataStore(strata.Config{EncryptionKey: []byte("short")})
	assert.Error(t, err)
}

func TestNewDataStore_InvalidPostgresDSN(t *testing.T) {
	_, err := strata.NewDataStore(strata.Config{PostgresDSN: "not://a-valid-dsn"})
	assert.Error(t, err)
}

// ── FlushDirty ────────────────────────────────────────────────────────────────

func TestDataStore_FlushDirty_L1Only(t *testing.T) {
	ds := newDS(t)
	require.NoError(t, ds.FlushDirty(context.Background()))
}

// ── Eviction hook via L1 schema config ───────────────────────────────────────

// TestDataStore_L1_OnEvict_Hook verifies that a schema with an OnEvict hook can
// be registered and that normal Set/Get operations complete without error.
func TestDataStore_L1_OnEvict_Hook(t *testing.T) {
	ds, err := strata.NewDataStore(strata.Config{})
	require.NoError(t, err)
	defer ds.Close()

	require.NoError(t, ds.Register(strata.Schema{
		Name:  "evict_test",
		Model: &Product{},
		L1:    strata.MemPolicy{MaxEntries: 1},
		Hooks: strata.SchemaHooks{
			OnEvict: func(_ context.Context, _ string, _ any) {},
		},
	}))

	ctx := context.Background()
	require.NoError(t, ds.Set(ctx, "evict_test", "e1", &Product{ID: "e1"}))
	require.NoError(t, ds.Set(ctx, "evict_test", "e2", &Product{ID: "e2"}))
	var p Product
	assert.NoError(t, ds.Get(ctx, "evict_test", "e2", &p))
}

// ── GetTyped error path ───────────────────────────────────────────────────────

// TestGetTyped_ErrorPropagates covers the `return nil, err` branch in GetTyped
// (the function has been stuck at 75.0% for 4 rounds because only the success
// path was ever exercised).
func TestGetTyped_ErrorPropagates(t *testing.T) {
	ds := newDS(t)
	registerProduct(t, ds)

	result, err := strata.GetTyped[Product](context.Background(), ds, "product", "does-not-exist")
	require.ErrorIs(t, err, strata.ErrNotFound,
		"missing key must return ErrNotFound via GetTyped")
	assert.Nil(t, result)
}

// ── routerGet: genuine L2 error logged as warning ────────────────────────────

// TestRouterGet_GenuineL2Error_LogsWarning exercises the `ds.logger.Warn` call
// inside routerGet when L2 returns a genuine connection error (not ErrMiss).
// The branch has never been covered because all existing tests use a live
// miniredis which either returns valid data or redis.Nil — never a transport
// error.  We trigger it by closing miniredis after the Set so that the
// subsequent Get gets a connection-refused-style error from the L2 layer.
func TestRouterGet_GenuineL2Error_LogsWarning(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	// NOTE: We deliberately do NOT defer mr.Close() here; we close it manually
	// mid-test to trigger the genuine L2 error path.

	ds, err := strata.NewDataStore(strata.Config{RedisAddr: mr.Addr()})
	require.NoError(t, err)
	defer ds.Close()

	s := strata.Schema{
		Name:  "l2_genuine_err",
		Model: &Product{},
		L1:    strata.MemPolicy{TTL: 5 * time.Millisecond},
		L2:    strata.RedisPolicy{TTL: time.Minute},
	}
	require.NoError(t, ds.Register(s))

	ctx := context.Background()
	p := &Product{ID: "ge1", Name: "GenuineErrItem", Price: 1}
	require.NoError(t, ds.Set(ctx, "l2_genuine_err", "ge1", p))

	// Let L1 TTL expire so the next Get misses L1 and hits L2.
	time.Sleep(20 * time.Millisecond)

	// Close miniredis — next L2 call will get a genuine connection error,
	// causing logger.Warn to be called inside routerGet.
	mr.Close()

	var got Product
	err = ds.Get(ctx, "l2_genuine_err", "ge1", &got)
	// No L3 configured → after genuine L2 error and fall-through, ErrNotFound.
	assert.ErrorIs(t, err, strata.ErrNotFound,
		"genuine L2 error should not surface as a Redis error; fall-through returns ErrNotFound")
}
