package strata_test

import (
	"context"
	"testing"

	"github.com/AndrewDonelson/strata"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func benchNewDS(b *testing.B) *strata.DataStore {
	b.Helper()
	ds, err := strata.NewDataStore(strata.Config{})
	if err != nil {
		b.Fatal(err)
	}
	return ds
}

// ── L1 tier benchmarks ────────────────────────────────────────────────────────

func BenchmarkDataStore_Set_L1(b *testing.B) {
	ds := benchNewDS(b)
	defer ds.Close()
	if err := ds.Register(strata.Schema{Name: "bench_set", Model: &Product{}}); err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	p := &Product{ID: "b1", Name: "BenchSet", Price: 1.0}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ds.Set(ctx, "bench_set", "b1", p)
	}
}

func BenchmarkDataStore_Get_L1_Hit(b *testing.B) {
	ds := benchNewDS(b)
	defer ds.Close()
	if err := ds.Register(strata.Schema{Name: "bench_get", Model: &Product{}}); err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	p := &Product{ID: "b1", Name: "BenchGet", Price: 1.0}
	_ = ds.Set(ctx, "bench_get", "b1", p)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var got Product
			_ = ds.Get(ctx, "bench_get", "b1", &got)
		}
	})
}

func BenchmarkDataStore_Get_L1_Miss(b *testing.B) {
	ds := benchNewDS(b)
	defer ds.Close()
	if err := ds.Register(strata.Schema{Name: "bench_miss", Model: &Product{}}); err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var got Product
		_ = ds.Get(ctx, "bench_miss", "never-set", &got)
	}
}

func BenchmarkDataStore_Delete_L1(b *testing.B) {
	ds := benchNewDS(b)
	defer ds.Close()
	if err := ds.Register(strata.Schema{Name: "bench_del", Model: &Product{}}); err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := &Product{ID: "bd1", Name: "del", Price: 1}
		_ = ds.Set(ctx, "bench_del", "bd1", p)
		_ = ds.Delete(ctx, "bench_del", "bd1")
	}
}

func BenchmarkDataStore_GetTyped_L1(b *testing.B) {
	ds := benchNewDS(b)
	defer ds.Close()
	if err := ds.Register(strata.Schema{Name: "bench_typed", Model: &Product{}}); err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	_ = ds.Set(ctx, "bench_typed", "bt1", &Product{ID: "bt1", Name: "Typed", Price: 1.0})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = strata.GetTyped[Product](ctx, ds, "bench_typed", "bt1")
	}
}

func BenchmarkDataStore_SetMany_L1(b *testing.B) {
	ds := benchNewDS(b)
	defer ds.Close()
	if err := ds.Register(strata.Schema{Name: "bench_many", Model: &Product{}}); err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	pairs := map[string]any{
		"p1": &Product{ID: "p1", Name: "A", Price: 1},
		"p2": &Product{ID: "p2", Name: "B", Price: 2},
		"p3": &Product{ID: "p3", Name: "C", Price: 3},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ds.SetMany(ctx, "bench_many", pairs)
	}
}

func BenchmarkDataStore_Exists(b *testing.B) {
	ds := benchNewDS(b)
	defer ds.Close()
	if err := ds.Register(strata.Schema{Name: "bench_exists", Model: &Product{}}); err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	_ = ds.Set(ctx, "bench_exists", "e1", &Product{ID: "e1"})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ds.Exists(ctx, "bench_exists", "e1")
	}
}

func BenchmarkDataStore_Stats(b *testing.B) {
	ds := benchNewDS(b)
	defer ds.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ds.Stats()
	}
}

// ── Query builder benchmarks ──────────────────────────────────────────────────

func BenchmarkQueryBuilder_Simple(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = strata.Q().Where("id = $1", "xyz").Limit(1).Build()
	}
}

func BenchmarkQueryBuilder_Complex(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = strata.Q().
			Where("level > $1 AND region = $2", 10, "eu").
			OrderBy("score").
			Desc().
			Limit(50).
			Offset(100).
			Fields("id", "name", "score").
			Build()
	}
}

func BenchmarkQuery_ToSQL(b *testing.B) {
	q := strata.Q().Where("level > $1", 5).OrderBy("created_at").Desc().Limit(25).Build()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = q.ToSQL("players", []string{"id", "name", "level"}, 100)
	}
}

// ── Crypto benchmarks ─────────────────────────────────────────────────────────

func BenchmarkAES256GCM_Encrypt(b *testing.B) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	enc, err := strata.NewAES256GCM(key)
	if err != nil {
		b.Fatal(err)
	}
	plaintext := []byte("sensitive data that needs encryption in storage")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = enc.Encrypt(plaintext)
	}
}

func BenchmarkAES256GCM_Decrypt(b *testing.B) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	enc, err := strata.NewAES256GCM(key)
	if err != nil {
		b.Fatal(err)
	}
	plaintext := []byte("sensitive data that needs encryption in storage")
	ciphertext, _ := enc.Encrypt(plaintext)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = enc.Decrypt(ciphertext)
	}
}

func BenchmarkDataStore_Set_Encrypted(b *testing.B) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	ds, err := strata.NewDataStore(strata.Config{EncryptionKey: key})
	if err != nil {
		b.Fatal(err)
	}
	defer ds.Close()

	type Secret struct {
		ID    string `strata:"primary_key"`
		Token string `strata:"encrypted"`
	}
	if err := ds.Register(strata.Schema{Name: "bench_enc", Model: &Secret{}}); err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	s := &Secret{ID: "s1", Token: "super-secret-token-value"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ds.Set(ctx, "bench_enc", "s1", s)
	}
}
