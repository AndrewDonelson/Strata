package strata_test

// schema_evolution_pg_test.go — adding fields to a schema whose table already has rows, and
// writing nil slices/maps/pointers into non-nullable JSONB columns. Requires Docker.

import (
	"context"
	"testing"
	"time"

	"github.com/AndrewDonelson/strata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type evoV1 struct {
	ID   string `strata:"primary_key"`
	Name string
}

type evoDetail struct {
	Level int `json:"level"`
}

// evoV2 is evoV1 plus one new non-nullable field of every SQL type Strata maps to.
type evoV2 struct {
	ID      string `strata:"primary_key"`
	Name    string
	Title   string
	Count   int64
	Ratio   float64
	Enabled bool
	SeenAt  time.Time
	Raw     []byte
	Tags    []string
	Attrs   map[string]string
	Detail  evoDetail
	Maybe   *evoDetail
}

type evoNil struct {
	ID    string `strata:"primary_key"`
	Tags  []string
	Attrs map[string]int
	Maybe *evoDetail
	Opt   []string `strata:"nullable"`
}

func plainStore(t *testing.T, dsn string, schemas ...strata.Schema) *strata.DataStore {
	t.Helper()
	ds, err := strata.NewDataStore(strata.Config{PostgresDSN: dsn})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })
	for _, s := range schemas {
		require.NoError(t, ds.Register(s))
	}
	require.NoError(t, ds.Migrate(context.Background()))
	return ds
}

func TestSchemaEvolutionPG(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	dsn := pgDSN(t)
	ctx := context.Background()

	t.Run("AddNotNullColumnsToTableWithRows", func(t *testing.T) {
		v1 := plainStore(t, dsn, strata.Schema{Name: "evo", Model: &evoV1{}})
		require.NoError(t, v1.Set(ctx, "evo", "a", &evoV1{ID: "a", Name: "old row"}))

		// A new deploy with more fields must migrate cleanly over the existing row.
		v2 := plainStore(t, dsn, strata.Schema{Name: "evo", Model: &evoV2{}})
		var got evoV2
		require.NoError(t, v2.Get(ctx, "evo", "a", &got))
		assert.Equal(t, "old row", got.Name)
		assert.Equal(t, "", got.Title)
		assert.Zero(t, got.Count)
		assert.Zero(t, got.Ratio)
		assert.False(t, got.Enabled)
		assert.True(t, got.SeenAt.IsZero(), "new time columns read back as the Go zero time, got %v", got.SeenAt)
		assert.NotNil(t, got.Raw, "NOT NULL BYTEA reads back as empty bytes, not nil")
		assert.Empty(t, got.Raw)
		assert.Nil(t, got.Tags)
		assert.Nil(t, got.Attrs)
		assert.Equal(t, evoDetail{}, got.Detail)
		assert.Nil(t, got.Maybe)

		// New rows still store real values.
		now := time.Now().UTC().Truncate(time.Microsecond)
		require.NoError(t, v2.Set(ctx, "evo", "b", &evoV2{ID: "b", Title: "t", Count: 3, SeenAt: now, Tags: []string{"x"}, Detail: evoDetail{Level: 2}}))
		fresh := plainStore(t, dsn, strata.Schema{Name: "evo", Model: &evoV2{}})
		var b evoV2
		require.NoError(t, fresh.Get(ctx, "evo", "b", &b))
		assert.Equal(t, int64(3), b.Count)
		assert.True(t, b.SeenAt.Equal(now))
		assert.Equal(t, []string{"x"}, b.Tags)
		assert.Equal(t, 2, b.Detail.Level)
	})

	t.Run("NilJSONBInNotNullColumns", func(t *testing.T) {
		ds := plainStore(t, dsn, strata.Schema{Name: "evo_nil", Model: &evoNil{}})
		require.NoError(t, ds.Set(ctx, "evo_nil", "n", &evoNil{ID: "n"}), "nil slice, map and pointer must not violate NOT NULL")
		require.NoError(t, ds.Tx(ctx).Set("evo_nil", "m", &evoNil{ID: "m"}).Commit())
		require.NoError(t, ds.Insert(ctx, "evo_nil", "i", &evoNil{ID: "i"}))

		fresh := plainStore(t, dsn, strata.Schema{Name: "evo_nil", Model: &evoNil{}})
		var got evoNil
		require.NoError(t, fresh.Get(ctx, "evo_nil", "n", &got))
		assert.Nil(t, got.Tags)
		assert.Nil(t, got.Attrs)
		assert.Nil(t, got.Maybe)
		assert.Nil(t, got.Opt)
	})
}
