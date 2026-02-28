package strata_test

// coverage3_test.go covers:
//   - routerSetL1Async   (WriteThroughL1Async path)
//   - routerGet L2-hit path (L1 TTL expiry → L2 hit → backfill L1)
//   - Exists L2 hit path
//   - routerDelete with L2 present (already partly covered; confirm L2 delete)
//   - Count with nil q path (ErrL3Unavailable)

import (
	"context"
	"time"

	"testing"

	strata "github.com/AndrewDonelson/strata"
	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── WriteThroughL1Async ───────────────────────────────────────────────────────

func TestRouterSetL1Async_WriteThroughL1Async(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	ds, err := strata.NewDataStore(strata.Config{RedisAddr: mr.Addr()})
	require.NoError(t, err)
	defer ds.Close()

	s := strata.Schema{
		Name:      "async_schema",
		Model:     &Product{},
		WriteMode: strata.WriteThroughL1Async,
		L1:        strata.MemPolicy{TTL: time.Minute},
	}
	require.NoError(t, ds.Register(s))

	ctx := context.Background()
	p := &Product{ID: "al1", Name: "AsyncL1Product", Price: 42}
	require.NoError(t, ds.Set(ctx, "async_schema", "al1", p))

	// L2 should be set synchronously; Get should find it
	var got Product
	err = ds.Get(ctx, "async_schema", "al1", &got)
	require.NoError(t, err)
	assert.Equal(t, "AsyncL1Product", got.Name)
}

func TestRouterSetL1Async_MultipleItems(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	ds, err := strata.NewDataStore(strata.Config{
		RedisAddr:        mr.Addr(),
		DefaultWriteMode: strata.WriteThroughL1Async,
	})
	require.NoError(t, err)
	defer ds.Close()

	s := strata.Schema{
		Name:  "async_multi",
		Model: &Product{},
		L1:    strata.MemPolicy{TTL: time.Minute},
	}
	require.NoError(t, ds.Register(s))

	ctx := context.Background()
	for i, id := range []string{"am1", "am2", "am3"} {
		p := &Product{ID: id, Name: "item", Price: float64(i + 1)}
		require.NoError(t, ds.Set(ctx, "async_multi", id, p))
	}

	// All three should be retrievable
	for _, id := range []string{"am1", "am2", "am3"} {
		var got Product
		require.NoError(t, ds.Get(ctx, "async_multi", id, &got))
	}
}

// ── L2 hit path (L1 TTL expiry) ──────────────────────────────────────────────

func TestRouterGet_L2Hit_AfterL1Expiry(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	ds, err := strata.NewDataStore(strata.Config{RedisAddr: mr.Addr()})
	require.NoError(t, err)
	defer ds.Close()

	// Use a very short L1 TTL so it expires quickly
	s := strata.Schema{
		Name:  "l2hit",
		Model: &Product{},
		L1:    strata.MemPolicy{TTL: 5 * time.Millisecond},
		L2:    strata.RedisPolicy{TTL: time.Minute},
	}
	require.NoError(t, ds.Register(s))

	ctx := context.Background()
	p := &Product{ID: "l2h1", Name: "L2HitItem", Price: 7}
	require.NoError(t, ds.Set(ctx, "l2hit", "l2h1", p))

	// Wait for L1 to expire
	time.Sleep(20 * time.Millisecond)

	// Get should miss L1 and hit L2 (back-fills L1)
	var got Product
	err = ds.Get(ctx, "l2hit", "l2h1", &got)
	require.NoError(t, err)
	assert.Equal(t, "L2HitItem", got.Name)
	assert.Equal(t, float64(7), got.Price)

	// Second get should hit warm L1 again
	var got2 Product
	err = ds.Get(ctx, "l2hit", "l2h1", &got2)
	require.NoError(t, err)
	assert.Equal(t, "L2HitItem", got2.Name)
}

func TestRouterGet_L2Hit_MultipleBackfills(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	ds, err := strata.NewDataStore(strata.Config{RedisAddr: mr.Addr()})
	require.NoError(t, err)
	defer ds.Close()

	s := strata.Schema{
		Name:  "l2backfill",
		Model: &Product{},
		L1:    strata.MemPolicy{TTL: 2 * time.Millisecond},
		L2:    strata.RedisPolicy{TTL: time.Minute},
	}
	require.NoError(t, ds.Register(s))

	ctx := context.Background()
	for _, id := range []string{"bf1", "bf2"} {
		p := &Product{ID: id, Name: "Backfill", Price: 1}
		require.NoError(t, ds.Set(ctx, "l2backfill", id, p))
	}

	time.Sleep(15 * time.Millisecond)

	// All should be found via L2 backfill
	for _, id := range []string{"bf1", "bf2"} {
		var got Product
		err := ds.Get(ctx, "l2backfill", id, &got)
		require.NoError(t, err, "id=%s", id)
	}
}

// ── Exists L2 hit ─────────────────────────────────────────────────────────────

func TestExists_L2Hit_AfterL1Expiry(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	ds, err := strata.NewDataStore(strata.Config{RedisAddr: mr.Addr()})
	require.NoError(t, err)
	defer ds.Close()

	s := strata.Schema{
		Name:  "exists_l2",
		Model: &Product{},
		L1:    strata.MemPolicy{TTL: 5 * time.Millisecond},
		L2:    strata.RedisPolicy{TTL: time.Minute},
	}
	require.NoError(t, ds.Register(s))

	ctx := context.Background()
	p := &Product{ID: "ex1", Name: "Existing"}
	require.NoError(t, ds.Set(ctx, "exists_l2", "ex1", p))

	// Wait for L1 to expire
	time.Sleep(20 * time.Millisecond)

	// Exists should hit L2
	ok, err := ds.Exists(ctx, "exists_l2", "ex1")
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestExists_NotInAnyCacheTier(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	ds, err := strata.NewDataStore(strata.Config{RedisAddr: mr.Addr()})
	require.NoError(t, err)
	defer ds.Close()

	s := strata.Schema{
		Name:  "exists_miss",
		Model: &Product{},
		L1:    strata.MemPolicy{TTL: time.Minute},
		L2:    strata.RedisPolicy{TTL: time.Minute},
	}
	require.NoError(t, ds.Register(s))

	ctx := context.Background()

	// Item never stored — should be false from both L1 and L2, then no L3
	ok, err := ds.Exists(ctx, "exists_miss", "nonexistent")
	require.NoError(t, err)
	assert.False(t, ok)
}

// ── SearchCached with L2 ───────────────────────────────────────────────────

func TestSearchCached_L2Hit(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	ds, err := strata.NewDataStore(strata.Config{RedisAddr: mr.Addr()})
	require.NoError(t, err)
	defer ds.Close()

	s := strata.Schema{
		Name:  "sc_l2",
		Model: &Product{},
		L1:    strata.MemPolicy{TTL: 5 * time.Millisecond},
		L2:    strata.RedisPolicy{TTL: time.Minute},
	}
	require.NoError(t, ds.Register(s))

	ctx := context.Background()
	for _, id := range []string{"sc1", "sc2"} {
		p := &Product{ID: id, Name: "SearchItem", Price: 1}
		require.NoError(t, ds.Set(ctx, "sc_l2", id, p))
	}

	// Wait for L1 to expire
	time.Sleep(20 * time.Millisecond)

	// SearchCached with L2 but no cached search result and no L3:
	// it correctly falls through to L3 and returns ErrL3Unavailable.
	var results []Product
	err = ds.SearchCached(ctx, "sc_l2", nil, &results)
	assert.ErrorIs(t, err, strata.ErrL3Unavailable)
}

// ── Count with nil q (ErrL3Unavailable) ─────────────────────────────────────

func TestCount_NilQ_NoL3(t *testing.T) {
	ds := newDS(t)
	registerProduct(t, ds) // must register before Count
	n, err := ds.Count(context.Background(), "product", nil)
	assert.ErrorIs(t, err, strata.ErrL3Unavailable)
	assert.Equal(t, int64(0), n)
}

func TestCount_WithQ_NoL3(t *testing.T) {
	ds := newDS(t)
	registerProduct(t, ds) // must register before Count
	q := strata.Q().Where("price > 0").Build()
	n, err := ds.Count(context.Background(), "product", &q)
	assert.ErrorIs(t, err, strata.ErrL3Unavailable)
	assert.Equal(t, int64(0), n)
}

// ── routerDelete with L2 ─────────────────────────────────────────────────────

func TestRouterDelete_WithL2(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	ds, err := strata.NewDataStore(strata.Config{RedisAddr: mr.Addr()})
	require.NoError(t, err)
	defer ds.Close()

	s := strata.Schema{
		Name:  "del_l2",
		Model: &Product{},
		L1:    strata.MemPolicy{TTL: time.Minute},
		L2:    strata.RedisPolicy{TTL: time.Minute},
	}
	require.NoError(t, ds.Register(s))

	ctx := context.Background()
	p := &Product{ID: "d1", Name: "ToDelete"}
	require.NoError(t, ds.Set(ctx, "del_l2", "d1", p))

	// Confirm stored
	var got Product
	require.NoError(t, ds.Get(ctx, "del_l2", "d1", &got))

	// Delete — covers routerDelete L2 path
	require.NoError(t, ds.Delete(ctx, "del_l2", "d1"))

	// Stats should reflect the delete
	st := ds.Stats()
	assert.GreaterOrEqual(t, st.Deletes, int64(1))

	// Exists should return false (checks L1 then L2 — both cleared)
	ok, err := ds.Exists(ctx, "del_l2", "d1")
	require.NoError(t, err)
	assert.False(t, ok, "item should not exist after delete")
}

// ── SetMany with L2 ──────────────────────────────────────────────────────────

func TestSetMany_WithL2(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	ds, err := strata.NewDataStore(strata.Config{RedisAddr: mr.Addr()})
	require.NoError(t, err)
	defer ds.Close()

	s := strata.Schema{
		Name:  "many_l2",
		Model: &Product{},
		L1:    strata.MemPolicy{TTL: time.Minute},
		L2:    strata.RedisPolicy{TTL: time.Minute},
	}
	require.NoError(t, ds.Register(s))

	ctx := context.Background()
	pairs := map[string]any{
		"m1": &Product{ID: "m1", Name: "Multi1", Price: 1},
		"m2": &Product{ID: "m2", Name: "Multi2", Price: 2},
	}
	require.NoError(t, ds.SetMany(ctx, "many_l2", pairs))

	// All should be retrievable
	for id := range pairs {
		var got Product
		require.NoError(t, ds.Get(ctx, "many_l2", id, &got))
		assert.Equal(t, id, got.ID)
	}
}
