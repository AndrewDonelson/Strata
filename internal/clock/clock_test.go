package clock_test

import (
"testing"
"time"

"github.com/AndrewDonelson/strata/internal/clock"
"github.com/stretchr/testify/assert"
)

func TestMockClock_Set(t *testing.T) {
clk := clock.NewMock(time.Time{})
ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
clk.Set(ts)
assert.Equal(t, ts, clk.Now())
}

func TestMockClock_Advance(t *testing.T) {
clk := clock.NewMock(time.Time{})
before := clk.Now()
clk.Advance(10 * time.Second)
after := clk.Now()
assert.Equal(t, 10*time.Second, after.Sub(before))
}

func TestRealClock(t *testing.T) {
clk := clock.Real{}
before := time.Now()
got := clk.Now()
after := time.Now()
assert.True(t, !got.Before(before))
assert.True(t, !got.After(after))
}
