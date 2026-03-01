// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// logger.go — Logger interface and noop implementation used internally by
// Strata for structured logging; swap in zap, slog, or logrus by passing
// a custom implementation to Config.Logger.

package strata

// Logger is the logging interface used internally by Strata.
// Implement this to route logs to zap, slog, logrus, etc.
type Logger interface {
	Info(msg string, keysAndValues ...any)
	Warn(msg string, keysAndValues ...any)
	Error(msg string, keysAndValues ...any)
	Debug(msg string, keysAndValues ...any)
}

type noopLogger struct{}

func (noopLogger) Info(_ string, _ ...any)  {}
func (noopLogger) Warn(_ string, _ ...any)  {}
func (noopLogger) Error(_ string, _ ...any) {}
func (noopLogger) Debug(_ string, _ ...any) {}
