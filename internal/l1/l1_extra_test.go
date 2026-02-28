package l1_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/AndrewDonelson/strata/internal/clock"
	"github.com/AndrewDonelson/strata/internal/l1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── GetWithMeta ───────────────────────────────────────────────────────────────

func TestL1_GetWithMeta_Hit(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	s := l1.New(l1.Options{MaxEntries: 100, TTL: time.Hour, Clock: clk})
	defer s.Close()

	s.Set("meta1", "value", time.Hour)

	meta, ok := s.GetWithMeta("meta1")
	require.True(t, ok)
	assert.Equal(t, "value", meta.Value)
	assert.Greater(t, meta.TTLRemaining, time.Duration(0))
	assert.GreaterOrEqual(t, meta.HitCount, 1)
}

func TestL1_GetWithMeta_Miss(t *testing.T) {
	s := l1.New(l1.Options{MaxEntries: 100, TTL: time.Minute, Clock: clock.Real{}})
	defer s.Close()

	_, ok := s.GetWithMeta("missing")
	assert.False(t, ok)
}

func TestL1_GetWithMeta_Expired(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	s := l1.New(l1.Options{MaxEntries: 100, Clock: clk})
	defer s.Close()

	s.Set("exp", "gone", 100*time.Millisecond)
	clk.Advance(200 * time.Millisecond)

	_, ok := s.GetWithMeta("exp")
	assert.False(t, ok)
}

func TestL1_GetWithMeta_NoTTL(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	s := l1.New(l1.Options{MaxEntries: 100, Clock: clk})
	defer s.Close()

	s.Set("permanent", "here", 0)

	meta, ok := s.GetWithMeta("permanent")
	require.True(t, ok)
	assert.Equal(t, time.Duration(0), meta.TTLRemaining)
}

// ── LFU eviction ──────────────────────────────────────────────────────────────

func TestL1_LFU_Eviction(t *testing.T) {
	// L1 uses 256 shards with MaxEntries per shard. To guarantee at least one
	// shard evicts, insert 256*2+1 items (MaxEntries=1 per shard pigeonholes).
	s := l1.New(l1.Options{MaxEntries: 1, Eviction: l1.LFU, Clock: clock.Real{}})
	defer s.Close()

	const n = 600
	for i := 0; i < n; i++ {
		s.Set(fmt.Sprintf("lfu_key_%d", i), i, 0)
	}

	st := s.Stats()
	// Some shards must have evicted — entries should be less than n
	assert.Less(t, st.Entries, int64(n), "LFU eviction should have reduced entry count")
}

// ── FIFO eviction ─────────────────────────────────────────────────────────────

func TestL1_FIFO_Eviction(t *testing.T) {
	// L1 uses 256 shards with MaxEntries per shard. Use enough keys to trigger
	// at least one shard eviction (MaxEntries=1, 600 keys → pigeonhole applies).
	s := l1.New(l1.Options{MaxEntries: 1, Eviction: l1.FIFO, Clock: clock.Real{}})
	defer s.Close()

	const n = 600
	for i := 0; i < n; i++ {
		s.Set(fmt.Sprintf("fifo_key_%d", i), i, 0)
	}

	st := s.Stats()
	assert.Less(t, st.Entries, int64(n), "FIFO eviction should have reduced entry count")
}

// ── OnEvict callback ─────────────────────────────────────────────────────────

func TestL1_OnEvict_Callback(t *testing.T) {
	// L1 distributes across 256 shards; MaxEntries is per shard.
	// Insert 600 items with MaxEntries=1 to guarantee evictions in multiple shards.
	var mu sync.Mutex
	var evictCount int

	s := l1.New(l1.Options{
		MaxEntries: 1,
		Eviction:   l1.LRU,
		Clock:      clock.Real{},
		OnEvict: func(key string, value any) {
			mu.Lock()
			evictCount++
			mu.Unlock()
		},
	})
	defer s.Close()

	const n = 600
	for i := 0; i < n; i++ {
		s.Set(fmt.Sprintf("evict_key_%d", i), i, 0)
	}

	mu.Lock()
	defer mu.Unlock()
	assert.Greater(t, evictCount, 0, "OnEvict should have been called at least once")
}

// ── Sweep / expiry ────────────────────────────────────────────────────────────

func TestL1_Sweep_RemovesExpiredOnGet(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	s := l1.New(l1.Options{MaxEntries: 100, Clock: clk})
	defer s.Close()

	s.Set("short", 1, 100*time.Millisecond)
	s.Set("long", 2, 10*time.Minute)

	clk.Advance(200 * time.Millisecond)

	_, okShort := s.Get("short")
	_, okLong := s.Get("long")
	assert.False(t, okShort, "expired entry should be gone after clock advance")
	assert.True(t, okLong)
}

func TestL1_SweepInterval_Triggered(t *testing.T) {
	// Use a very short sweep interval with real clock to exercise sweepLoop
	s := l1.New(l1.Options{
		MaxEntries:    100,
		Clock:         clock.Real{},
		SweepInterval: 20 * time.Millisecond,
	})
	defer s.Close()

	s.Set("expire-me", "v", 10*time.Millisecond)
	time.Sleep(60 * time.Millisecond)

	// After sleep, sweep should have cleaned up the expired entry
	_, ok := s.Get("expire-me")
	assert.False(t, ok)
}

// ── Flush / FlushSchema ───────────────────────────────────────────────────────

func TestL1_FlushAll(t *testing.T) {
	s := l1.New(l1.Options{MaxEntries: 100, Clock: clock.Real{}})
	defer s.Close()

	for i := 0; i < 10; i++ {
		s.Set(fmt.Sprintf("key%d", i), i, 0)
	}
	s.Flush()

	st := s.Stats()
	assert.Equal(t, int64(0), st.Entries)
}

func TestL1_FlushSchemaPrefix(t *testing.T) {
	s := l1.New(l1.Options{MaxEntries: 100, Clock: clock.Real{}})
	defer s.Close()

	s.Set("players:1", "a", 0)
	s.Set("players:2", "b", 0)
	s.Set("sessions:1", "c", 0)

	s.FlushSchema("players:")

	_, ok1 := s.Get("players:1")
	_, ok2 := s.Get("players:2")
	_, ok3 := s.Get("sessions:1")
	assert.False(t, ok1)
	assert.False(t, ok2)
	assert.True(t, ok3)
}

// ── Update-in-place ───────────────────────────────────────────────────────────

func TestL1_UpdateExisting(t *testing.T) {
	clk := clock.NewMock(time.Time{})
	s := l1.New(l1.Options{MaxEntries: 100, Clock: clk})
	defer s.Close()

	s.Set("k", "v1", 0)
	s.Set("k", "v2", 0)

	v, ok := s.Get("k")
	require.True(t, ok)
	assert.Equal(t, "v2", v)
}

// ── Concurrent safety ─────────────────────────────────────────────────────────

func TestL1_Concurrent_SetGet(t *testing.T) {
	s := l1.New(l1.Options{MaxEntries: 1000, Clock: clock.Real{}})
	defer s.Close()

	const goroutines = 50
	const ops = 200
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", id%20)
			for i := 0; i < ops; i++ {
				s.Set(key, id*ops+i, 0)
				_, _ = s.Get(key)
			}
		}(g)
	}
	wg.Wait()
	// no race condition = test passes
}

func TestL1_Concurrent_Delete(t *testing.T) {
	s := l1.New(l1.Options{MaxEntries: 1000, Clock: clock.Real{}})
	defer s.Close()

	for i := 0; i < 100; i++ {
		s.Set(fmt.Sprintf("k%d", i), i, 0)
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s.Delete(fmt.Sprintf("k%d", i))
		}(i)
	}
	wg.Wait()
}

// ── Stats ─────────────────────────────────────────────────────────────────────

func TestL1_Stats_Count(t *testing.T) {
	s := l1.New(l1.Options{MaxEntries: 100, Clock: clock.Real{}})
	defer s.Close()

	for i := 0; i < 5; i++ {
		s.Set(fmt.Sprintf("item%d", i), i, 0)
	}

	st := s.Stats()
	assert.Equal(t, int64(5), st.Entries)
}

// ── Benchmarks ────────────────────────────────────────────────────────────────

func BenchmarkL1_Set(b *testing.B) {
	s := l1.New(l1.Options{MaxEntries: 10000, Clock: clock.Real{}})
	defer s.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Set("bench-key", i, 0)
	}
}

func BenchmarkL1_Get_Hit(b *testing.B) {
	s := l1.New(l1.Options{MaxEntries: 10000, Clock: clock.Real{}})
	defer s.Close()
	s.Set("bench-key", "value", 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = s.Get("bench-key")
	}
}

func BenchmarkL1_Get_Parallel(b *testing.B) {
	s := l1.New(l1.Options{MaxEntries: 10000, Clock: clock.Real{}})
	defer s.Close()
	s.Set("bench-key", "value", 0)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = s.Get("bench-key")
		}
	})
}

func BenchmarkL1_GetWithMeta(b *testing.B) {
	s := l1.New(l1.Options{MaxEntries: 10000, Clock: clock.Real{}})
	defer s.Close()
	s.Set("bench-key", "value", time.Hour)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = s.GetWithMeta("bench-key")
	}
}
