// Package logger is a thin wrapper over log/slog with the few helpers the
// rest of the codebase needs. Keeping it behind our own package means we can
// swap the backend (audit sink, JSON to file, etc.) without touching callers.
package logger

import (
	"context"
	"log/slog"
	"os"
)

var base = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

// Setup (re)configures the global logger. level is one of debug|info|warn|error.
func Setup(level string, json bool) {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler
	if json {
		h = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		h = slog.NewTextHandler(os.Stderr, opts)
	}
	base = slog.New(h)
	slog.SetDefault(base)
}

func Debug(msg string, args ...any) { base.Debug(msg, args...) }
func Info(msg string, args ...any)  { base.Info(msg, args...) }
func Warn(msg string, args ...any)  { base.Warn(msg, args...) }
func Error(msg string, args ...any) { base.Error(msg, args...) }

// Fatal logs at error level and exits with status 1.
func Fatal(msg string, args ...any) {
	base.Error(msg, args...)
	os.Exit(1)
}

// With returns a child logger carrying the given attributes.
func With(args ...any) *slog.Logger { return base.With(args...) }

// FromContext is a placeholder for request-scoped loggers (populated by
// api middleware in M2+). For now it always returns the base logger.
func FromContext(_ context.Context) *slog.Logger { return base }
