package strata_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AndrewDonelson/strata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── Load: concurrent Set+Get ──────────────────────────────────────────────────

func TestLoad_ConcurrentSetGet(t *testing.T) {
	t.Parallel()

	ds, err := strata.NewDataStore(strata.Config{})
	require.NoError(t, err)
	defer ds.Close()
	require.NoError(t, ds.Register(strata.Schema{
		Name:  "load_sg",
		Model: &Product{},
	}))

	const goroutines = 50
	const opsPerGoroutine = 200

	var errs atomic.Int64
	var wg sync.WaitGroup
	ctx := context.Background()

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				id := fmt.Sprintf("g%d-i%d", gid, i%10)
				p := &Product{ID: id, Name: fmt.Sprintf("G%d", gid), Price: float64(i)}
				if err := ds.Set(ctx, "load_sg", id, p); err != nil {
					errs.Add(1)
					continue
				}
				var got Product
				if err := ds.Get(ctx, "load_sg", id, &got); err != nil {
					errs.Add(1)
				}
			}
		}(g)
	}
	wg.Wait()

	assert.Equal(t, int64(0), errs.Load(),
		"%d errors during %d concurrent Set+Get operations", errs.Load(), goroutines*opsPerGoroutine)
}

// ── Load: concurrent Set + Delete ────────────────────────────────────────────

func TestLoad_ConcurrentSetDelete(t *testing.T) {
	t.Parallel()

	ds, err := strata.NewDataStore(strata.Config{})
	require.NoError(t, err)
	defer ds.Close()
	require.NoError(t, ds.Register(strata.Schema{
		Name:  "load_sd",
		Model: &Product{},
	}))

	const goroutines = 30
	const keys = 20
	var errs atomic.Int64
	var wg sync.WaitGroup
	ctx := context.Background()

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for k := 0; k < keys; k++ {
				id := fmt.Sprintf("k%d", k)
				_ = ds.Set(ctx, "load_sd", id, &Product{ID: id, Name: "x"})
				if err := ds.Delete(ctx, "load_sd", id); err != nil {
					errs.Add(1)
				}
			}
		}(g)
	}
	wg.Wait()
	assert.Equal(t, int64(0), errs.Load())
}

// ── Load: hot key (all goroutines hit the same ID) ────────────────────────────

func TestLoad_HotKey(t *testing.T) {
	t.Parallel()

	ds, err := strata.NewDataStore(strata.Config{})
	require.NoError(t, err)
	defer ds.Close()
	require.NoError(t, ds.Register(strata.Schema{
		Name:  "load_hot",
		Model: &Product{},
	}))

	ctx := context.Background()
	_ = ds.Set(ctx, "load_hot", "hot", &Product{ID: "hot", Name: "initial", Price: 0})

	const goroutines = 100
	var wg sync.WaitGroup
	var readErrors atomic.Int64

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				var p Product
				if err := ds.Get(ctx, "load_hot", "hot", &p); err != nil {
					readErrors.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, int64(0), readErrors.Load())
}

// ── Load: WriteBehind high-volume ─────────────────────────────────────────────

func TestLoad_WriteBehind_HighVolume(t *testing.T) {
	t.Parallel()

	ds, err := strata.NewDataStore(strata.Config{
		DefaultWriteMode:          strata.WriteBehind,
		WriteBehindFlushInterval:  50 * time.Millisecond,
		WriteBehindFlushThreshold: 20,
	})
	require.NoError(t, err)
	defer ds.Close()
	require.NoError(t, ds.Register(strata.Schema{
		Name:      "load_wb",
		Model:     &Product{},
		WriteMode: strata.WriteBehind,
	}))

	const goroutines = 20
	const writes = 50
	var errs atomic.Int64
	var wg sync.WaitGroup
	ctx := context.Background()

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < writes; i++ {
				id := fmt.Sprintf("wb-g%d-i%d", gid, i)
				if err := ds.Set(ctx, "load_wb", id, &Product{ID: id, Price: float64(i)}); err != nil {
					errs.Add(1)
				}
			}
		}(g)
	}
	wg.Wait()

	// Wait for async flush
	time.Sleep(200 * time.Millisecond)
	require.NoError(t, ds.FlushDirty(ctx))

	assert.Equal(t, int64(0), errs.Load(),
		"%d errors during WriteBehind load test", errs.Load())
}

// ── Load: mixed read/write/delete/exists ──────────────────────────────────────

func TestLoad_Mixed_Operations(t *testing.T) {
	t.Parallel()

	ds, err := strata.NewDataStore(strata.Config{})
	require.NoError(t, err)
	defer ds.Close()
	require.NoError(t, ds.Register(strata.Schema{
		Name:  "load_mix",
		Model: &Product{},
	}))

	const goroutines = 40
	const iterations = 100
	var errs atomic.Int64
	var wg sync.WaitGroup
	ctx := context.Background()

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				id := fmt.Sprintf("mix-%d", i%15)
				switch i % 4 {
				case 0: // Set
					if e := ds.Set(ctx, "load_mix", id, &Product{ID: id, Price: float64(i)}); e != nil {
						errs.Add(1)
					}
				case 1: // Get
					var p Product
					_ = ds.Get(ctx, "load_mix", id, &p) // ErrNotFound is acceptable
				case 2: // Exists
					if _, e := ds.Exists(ctx, "load_mix", id); e != nil {
						errs.Add(1)
					}
				case 3: // Delete
					_ = ds.Delete(ctx, "load_mix", id) // ok if not found
				}
			}
		}(g)
	}
	wg.Wait()
	assert.Equal(t, int64(0), errs.Load())
}

// ── Load: Invalidate under heavy traffic ─────────────────────────────────────

func TestLoad_Invalidate_Concurrent(t *testing.T) {
	t.Parallel()

	ds, err := strata.NewDataStore(strata.Config{})
	require.NoError(t, err)
	defer ds.Close()
	require.NoError(t, ds.Register(strata.Schema{
		Name:  "load_inv",
		Model: &Product{},
	}))

	const goroutines = 20
	var wg sync.WaitGroup
	var errs atomic.Int64
	ctx := context.Background()

	// Pre-populate
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("inv%d", i)
		_ = ds.Set(ctx, "load_inv", id, &Product{ID: id})
	}

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				id := fmt.Sprintf("inv%d", i%10)
				if gid%2 == 0 {
					if e := ds.Invalidate(ctx, "load_inv", id); e != nil {
						errs.Add(1)
					}
				} else {
					var p Product
					_ = ds.Get(ctx, "load_inv", id, &p)
				}
			}
		}(g)
	}
	wg.Wait()
	assert.Equal(t, int64(0), errs.Load())
}

// ── Load: SetMany concurrent ──────────────────────────────────────────────────

func TestLoad_SetMany_Concurrent(t *testing.T) {
	t.Parallel()

	ds, err := strata.NewDataStore(strata.Config{})
	require.NoError(t, err)
	defer ds.Close()
	require.NoError(t, ds.Register(strata.Schema{
		Name:  "load_sm",
		Model: &Product{},
	}))

	const goroutines = 10
	var wg sync.WaitGroup
	var errs atomic.Int64
	ctx := context.Background()

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			pairs := map[string]any{
				fmt.Sprintf("g%d-1", gid): &Product{ID: fmt.Sprintf("g%d-1", gid)},
				fmt.Sprintf("g%d-2", gid): &Product{ID: fmt.Sprintf("g%d-2", gid)},
			}
			for i := 0; i < 20; i++ {
				if e := ds.SetMany(ctx, "load_sm", pairs); e != nil {
					errs.Add(1)
				}
			}
		}(g)
	}
	wg.Wait()
	assert.Equal(t, int64(0), errs.Load())
}
