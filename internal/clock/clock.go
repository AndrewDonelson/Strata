// Package clock provides a testable clock interface for TTL calculations.
package clock

import "time"

// Clock is an interface for getting the current time.
type Clock interface {
	Now() time.Time
}

// Real is the production clock -- uses system time.
type Real struct{}

// Now returns the current system time.
func (Real) Now() time.Time { return time.Now() }

// Mock is a controllable clock for tests.
type Mock struct {
	current time.Time
}

// NewMock creates a Mock clock set to the given time.
func NewMock(t time.Time) *Mock {
	if t.IsZero() {
		t = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	return &Mock{current: t}
}

// Now returns the mock clock's current time.
func (m *Mock) Now() time.Time {
	return m.current
}

// Set sets the mock clock to an absolute time.
func (m *Mock) Set(t time.Time) {
	m.current = t
}

// Advance moves the clock forward by the given duration.
func (m *Mock) Advance(d time.Duration) {
	m.current = m.current.Add(d)
}
