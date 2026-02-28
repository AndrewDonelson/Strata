package metrics_test

import (
	"testing"
	"time"

	"github.com/AndrewDonelson/strata/internal/metrics"
)

func TestNoop_AllMethods(t *testing.T) {
	n := metrics.Noop{}
	n.RecordHit("l1", "product")
	n.RecordMiss("l2", "user")
	n.RecordLatency("l1", "get", 100*time.Millisecond)
	n.RecordError("l2", "set")
	n.RecordDirtyCount(5)
}
