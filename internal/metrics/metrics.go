// Package metrics provides the MetricsRecorder interface and a noop implementation.
package metrics

import "time"

// MetricsRecorder is the interface for recording operational metrics.
type MetricsRecorder interface {
	RecordHit(tier, schema string)
	RecordMiss(tier, schema string)
	RecordLatency(tier, op string, d time.Duration)
	RecordError(tier, op string)
	RecordDirtyCount(count int64)
}

// Noop is a MetricsRecorder that discards all data.
type Noop struct{}

func (Noop) RecordHit(tier, schema string)                  {}
func (Noop) RecordMiss(tier, schema string)                 {}
func (Noop) RecordLatency(tier, op string, d time.Duration) {}
func (Noop) RecordError(tier, op string)                    {}
func (Noop) RecordDirtyCount(count int64)                   {}
