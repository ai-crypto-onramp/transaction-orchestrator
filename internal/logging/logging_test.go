package logging

import (
	"context"
	"testing"
)

func TestNewAndFrom(t *testing.T) {
	l := New("debug")
	ctx := WithLogger(context.Background(), l)
	if got := From(ctx); got == nil {
		t.Fatal("expected logger from ctx")
	}
	if From(context.Background()) == nil {
		t.Fatal("default logger should not be nil")
	}
}

func TestNewLevels(t *testing.T) {
	for _, lvl := range []string{"debug", "info", "warn", "error", "unknown"} {
		if l := New(lvl); l == nil {
			t.Fatalf("New(%q) returned nil", lvl)
		}
	}
}