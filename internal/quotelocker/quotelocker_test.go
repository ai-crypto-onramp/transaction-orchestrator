package quotelocker

import (
	"context"
	"testing"
)

func TestNoopLockerAcquire(t *testing.T) {
	l := NewNoop()
	rel, ok, err := l.Acquire(context.Background(), "q1")
	if err != nil || !ok {
		t.Fatalf("expected ok=true nil err, got ok=%v err=%v", ok, err)
	}
	rel()
	rel() // idempotent
}