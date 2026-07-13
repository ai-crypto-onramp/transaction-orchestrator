// Package logging wraps a slog-style structured logger with request-ID
// propagation.  It is deliberately small and dependency-light.
package logging

import (
	"context"
	"log/slog"
	"os"
)

type ctxKey struct{}

// WithLogger returns a context that carries l.
func WithLogger(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, ctxKey{}, l)
}

// From returns the logger stored in ctx, or a default if absent.
func From(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(ctxKey{}).(*slog.Logger); ok && l != nil {
		return l
	}
	return slog.Default()
}

// New constructs a logger at the given level ("debug"/"info"/"warn"/"error").
func New(level string) *slog.Logger {
	var lv slog.Level
	switch level {
	case "debug":
		lv = slog.LevelDebug
	case "warn":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	default:
		lv = slog.LevelInfo
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lv})
	return slog.New(h)
}