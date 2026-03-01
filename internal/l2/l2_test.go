package l2_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/AndrewDonelson/strata/internal/codec"
	"github.com/AndrewDonelson/strata/internal/l2"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testVal struct {
	ID    string
	Value string
	Score int
}

func newTestStore(t *testing.T) (*l2.Store, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := l2.New(l2.Options{Client: client, Codec: codec.JSON{}})
	return store, mr
}

func TestL2_SetGet(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)

	val := &testVal{ID: "1", Value: "hello", Score: 42}
	require.NoError(t, s.Set(ctx, "schema", "", "1", val, time.Minute))

	var got testVal
	require.NoError(t, s.Get(ctx, "schema", "", "1", &got))
	assert.Equal(t, *val, got)
}

func TestL2_Get_Miss(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)

	var got testVal
	err := s.Get(ctx, "schema", "", "missing", &got)
	require.ErrorIs(t, err, l2.ErrMiss)
	assert.Empty(t, got.ID)
}

func TestL2_Exists_Present(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)

	require.NoError(t, s.Set(ctx, "schema", "", "2", &testVal{ID: "2"}, time.Minute))
	ok, err := s.Exists(ctx, "schema", "", "2")
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestL2_Exists_Absent(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)

	ok, err := s.Exists(ctx, "schema", "", "absent")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestL2_Delete(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)

	require.NoError(t, s.Set(ctx, "schema", "", "3", &testVal{ID: "3"}, time.Minute))
	require.NoError(t, s.Delete(ctx, "schema", "", "3"))

	ok, err := s.Exists(ctx, "schema", "", "3")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestL2_Delete_NonExistent(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)
	// deleting a non-existent key should not error
	assert.NoError(t, s.Delete(ctx, "schema", "", "ghost"))
}

func TestL2_TTL_Expiry(t *testing.T) {
	ctx := context.Background()
	s, mr := newTestStore(t)

	require.NoError(t, s.Set(ctx, "schema", "", "4", &testVal{ID: "4"}, 100*time.Millisecond))
	mr.FastForward(200 * time.Millisecond)

	var got testVal
	err := s.Get(ctx, "schema", "", "4", &got)
	require.ErrorIs(t, err, l2.ErrMiss) // expired — key gone
	assert.Empty(t, got.ID)
}

func TestL2_SetMany_GetMany(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)

	kvs := []l2.KV{
		{ID: "10", Value: &testVal{ID: "10", Value: "ten"}},
		{ID: "20", Value: &testVal{ID: "20", Value: "twenty"}},
		{ID: "30", Value: &testVal{ID: "30", Value: "thirty"}},
	}
	require.NoError(t, s.SetMany(ctx, "schema", "", kvs, time.Minute))

	result, err := s.GetMany(ctx, "schema", "", []string{"10", "20", "30", "missing"})
	require.NoError(t, err)
	assert.Len(t, result, 3)
	assert.Contains(t, result, "10")
	assert.NotContains(t, result, "missing")
}

func TestL2_InvalidateAll(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)

	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("%d", i)
		require.NoError(t, s.Set(ctx, "players", "", id, &testVal{ID: id}, time.Minute))
	}

	require.NoError(t, s.InvalidateAll(ctx, "players", ""))

	for i := 0; i < 5; i++ {
		ok, err := s.Exists(ctx, "players", "", fmt.Sprintf("%d", i))
		require.NoError(t, err)
		assert.False(t, ok)
	}
}

func TestL2_FormatKey_NoPrefix(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"}) // won't connect
	s := l2.New(l2.Options{Client: client, Codec: codec.JSON{}})

	k := s.FormatKey("players", "", "abc")
	assert.Equal(t, "players:players:abc", k)
}

func TestL2_FormatKey_WithKeyPrefix(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	s := l2.New(l2.Options{Client: client, Codec: codec.JSON{}, KeyPrefix: "app"})

	k := s.FormatKey("players", "", "abc")
	assert.Contains(t, k, "app")
	assert.Contains(t, k, "players")
}

func TestL2_FormatKey_WithSchemaPrefix(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	s := l2.New(l2.Options{Client: client, Codec: codec.JSON{}})

	k := s.FormatKey("players", "players-v2", "abc")
	assert.Contains(t, k, "players-v2")
}

func TestL2_Stats_HitsAndMisses(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)

	require.NoError(t, s.Set(ctx, "schema", "", "stat1", &testVal{ID: "s"}, time.Minute))

	var got testVal
	require.NoError(t, s.Get(ctx, "schema", "", "stat1", &got))                  // hit
	require.ErrorIs(t, s.Get(ctx, "schema", "", "missing999", &got), l2.ErrMiss) // miss

	st := s.Stats()
	assert.Equal(t, int64(1), st.Hits)
	assert.Equal(t, int64(1), st.Misses)
}

func TestL2_Ping(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)
	assert.NoError(t, s.Ping(ctx))
}

func TestL2_SetRaw_GetRaw(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)

	data := []byte(`{"raw":true}`)
	require.NoError(t, s.SetRaw(ctx, "my:raw:key", data, time.Minute))

	got, err := s.GetRaw(ctx, "my:raw:key")
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestL2_GetRaw_Missing(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)

	got, err := s.GetRaw(ctx, "no:such:key")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestL2_DelRaw(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)

	require.NoError(t, s.SetRaw(ctx, "raw:del:key", []byte("data"), time.Minute))
	require.NoError(t, s.DelRaw(ctx, "raw:del:key"))

	gone, err := s.GetRaw(ctx, "raw:del:key")
	require.NoError(t, err)
	assert.Nil(t, gone)
}

func TestL2_Subscribe_ReturnsNonNil(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)

	sub := s.Subscribe(ctx, "test-chan")
	require.NotNil(t, sub)
	_ = sub.Close()
}

func TestL2_Publish(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)
	// just make sure Publish doesn't error
	assert.NoError(t, s.Publish(ctx, "test-channel", []byte(`{"op":"set"}`)))
}

func TestL2_KeyPrefix_SchemaOverride(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)

	val := &testVal{ID: "kp1", Value: "schema-prefix-test"}
	require.NoError(t, s.Set(ctx, "players", "players-v2", "kp1", val, time.Minute))

	var got testVal
	require.NoError(t, s.Get(ctx, "players", "players-v2", "kp1", &got))
	assert.Equal(t, "schema-prefix-test", got.Value)
}

// ── Keyed (P) variants ───────────────────────────────────────────────────────
// These test SetP / GetP / ExistsP / DeleteP which accept a pre-computed key
// prefix (stored once in compiledSchema.l2Prefix) to skip a string concat on
// every hot-path call.

func TestL2P_SetGet(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)

	prefix := "schema:schema:" // mirrors compiledSchema.l2Prefix
	val := &testVal{ID: "p1", Value: "pipeline", Score: 77}
	require.NoError(t, s.SetP(ctx, prefix, "p1", val, time.Minute))

	var got testVal
	require.NoError(t, s.GetP(ctx, prefix, "p1", &got))
	assert.Equal(t, *val, got)
}

func TestL2P_GetP_Miss(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)

	var got testVal
	err := s.GetP(ctx, "schema:schema:", "missing", &got)
	require.ErrorIs(t, err, l2.ErrMiss)
	assert.Empty(t, got.ID)
}

func TestL2P_ExistsP_Present(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)

	prefix := "ep:ep:"
	require.NoError(t, s.SetP(ctx, prefix, "x1", &testVal{ID: "x1"}, time.Minute))
	ok, err := s.ExistsP(ctx, prefix, "x1")
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestL2P_ExistsP_Absent(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)

	ok, err := s.ExistsP(ctx, "ep:ep:", "absent")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestL2P_DeleteP(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)

	prefix := "dp:dp:"
	require.NoError(t, s.SetP(ctx, prefix, "d1", &testVal{ID: "d1"}, time.Minute))
	require.NoError(t, s.DeleteP(ctx, prefix, "d1"))

	ok, err := s.ExistsP(ctx, prefix, "d1")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestL2P_DeleteP_NonExistent(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)
	// Deleting a missing key via DeleteP must not error.
	assert.NoError(t, s.DeleteP(ctx, "dp:dp:", "ghost"))
}

func TestL2P_SetP_GetP_WithGlobalKeyPrefix(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	// KeyPrefix exercises the s.keyPrefix != "" branch inside keyP.
	s := l2.New(l2.Options{Client: client, Codec: codec.JSON{}, KeyPrefix: "myapp"})

	ctx := context.Background()
	prefix := "schema:schema:"
	val := &testVal{ID: "gp1", Value: "prefixed", Score: 3}
	require.NoError(t, s.SetP(ctx, prefix, "gp1", val, time.Minute))

	var got testVal
	require.NoError(t, s.GetP(ctx, prefix, "gp1", &got))
	assert.Equal(t, val.Value, got.Value)

	// ExistsP and DeleteP must round-trip through the same prefixed key.
	ok, err := s.ExistsP(ctx, prefix, "gp1")
	require.NoError(t, err)
	assert.True(t, ok)

	require.NoError(t, s.DeleteP(ctx, prefix, "gp1"))
	ok, err = s.ExistsP(ctx, prefix, "gp1")
	require.NoError(t, err)
	assert.False(t, ok)
}

// ── Benchmarks ────────────────────────────────────────────────────────────────

func BenchmarkL2_Set(b *testing.B) {
	mr, _ := miniredis.Run()
	defer mr.Close()
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	s := l2.New(l2.Options{Client: client, Codec: codec.JSON{}})
	ctx := context.Background()
	val := &testVal{ID: "b1", Value: "bench", Score: 1}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Set(ctx, "bench", "", "b1", val, time.Minute)
	}
}

func BenchmarkL2_Get(b *testing.B) {
	mr, _ := miniredis.Run()
	defer mr.Close()
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	s := l2.New(l2.Options{Client: client, Codec: codec.JSON{}})
	ctx := context.Background()
	val := &testVal{ID: "b1", Value: "bench", Score: 1}
	_ = s.Set(ctx, "bench", "", "b1", val, time.Minute)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var got testVal
		_ = s.Get(ctx, "bench", "", "b1", &got)
	}
}

func BenchmarkL2_SetP(b *testing.B) {
	mr, _ := miniredis.Run()
	defer mr.Close()
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	s := l2.New(l2.Options{Client: client, Codec: codec.JSON{}})
	ctx := context.Background()
	val := &testVal{ID: "b1", Value: "bench", Score: 1}
	prefix := "bench:bench:" // pre-computed as in compiledSchema.l2Prefix

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.SetP(ctx, prefix, "b1", val, time.Minute)
	}
}

func BenchmarkL2_GetP(b *testing.B) {
	mr, _ := miniredis.Run()
	defer mr.Close()
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	s := l2.New(l2.Options{Client: client, Codec: codec.JSON{}})
	ctx := context.Background()
	val := &testVal{ID: "b1", Value: "bench", Score: 1}
	prefix := "bench:bench:"
	_ = s.SetP(ctx, prefix, "b1", val, time.Minute)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var got testVal
		_ = s.GetP(ctx, prefix, "b1", &got)
	}
}
