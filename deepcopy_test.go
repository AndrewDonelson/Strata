package strata

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type copyLine struct {
	Name  string
	Price int64
	Tags  []string
}

type copyOrder struct {
	ID      string `strata:"primary_key"`
	Lines   []copyLine
	Meta    map[string]string
	Note    *string
	At      time.Time
	Any     any
	private []int
}

func TestDeepCopyIsIndependent(t *testing.T) {
	note := "n"
	at := time.Date(2026, 10, 12, 1, 2, 3, 0, time.UTC)
	o := &copyOrder{ID: "o1", Lines: []copyLine{{Name: "a", Tags: []string{"x"}}}, Meta: map[string]string{"k": "v"},
		Note: &note, At: at, Any: []int{1}, private: []int{9}}
	c := deepCopy(o).(*copyOrder)
	require.Equal(t, o.ID, c.ID)
	assert.Equal(t, at, c.At)
	assert.Equal(t, []int{9}, c.private, "unexported fields are copied by value")

	o.Lines[0].Name = "changed"
	o.Lines[0].Tags[0] = "changed"
	o.Lines = append(o.Lines, copyLine{Name: "b"})
	o.Meta["k"] = "changed"
	*o.Note = "changed"
	o.Any.([]int)[0] = 2
	assert.Equal(t, "a", c.Lines[0].Name)
	assert.Equal(t, "x", c.Lines[0].Tags[0])
	assert.Len(t, c.Lines, 1)
	assert.Equal(t, "v", c.Meta["k"])
	assert.Equal(t, "n", *c.Note)
	assert.Equal(t, []int{1}, c.Any)
	assert.Nil(t, deepCopy(nil))
}

// TestL1HoldsItsOwnCopy: what a caller does to its struct after Set, or to what Get gave it,
// never reaches the cache (it used to: L1 stored the caller's pointer).
func TestL1HoldsItsOwnCopy(t *testing.T) {
	ds, err := NewDataStore(Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })
	require.NoError(t, ds.Register(Schema{Name: "orders", Model: &copyOrder{}}))
	cs, err := ds.registry.get("orders")
	require.NoError(t, err)

	o := &copyOrder{ID: "o1", Lines: []copyLine{{Name: "a"}}}
	ds.setL1(cs, cs.l1Prefix+"o1", o)
	o.Lines[0].Name = "mutated after Set"
	o.ID = "mutated"

	var got copyOrder
	raw, ok := ds.l1.Get(cs.l1Prefix + "o1")
	require.True(t, ok)
	_, err = populateDest(raw, &got)
	require.NoError(t, err)
	assert.Equal(t, "o1", got.ID)
	assert.Equal(t, "a", got.Lines[0].Name)

	got.Lines[0].Name = "mutated after Get"
	var again copyOrder
	raw, _ = ds.l1.Get(cs.l1Prefix + "o1")
	_, _ = populateDest(raw, &again)
	assert.Equal(t, "a", again.Lines[0].Name)

	// Concurrent writers mutating their own structs while readers read: clean under -race.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			mine := &copyOrder{ID: "o1", Lines: []copyLine{{Name: "w"}}}
			for j := 0; j < 50; j++ {
				ds.setL1(cs, cs.l1Prefix+"o1", mine)
				mine.Lines[0].Price++
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				var r copyOrder
				if raw, ok := ds.l1.Get(cs.l1Prefix + "o1"); ok {
					_, _ = populateDest(raw, &r)
				}
			}
		}()
	}
	wg.Wait()
}
