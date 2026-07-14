package lease

import (
	"context"
	"testing"
	"time"
)

func TestInMemoryAcquireRelease(t *testing.T) {
	l := NewInMemory(time.Second)
	ctx := context.Background()
	rel, ok, err := l.Acquire(ctx, "tx-1", "owner-A", time.Second)
	if err != nil || !ok {
		t.Fatalf("first acquire: ok=%v err=%v", ok, err)
	}
	// Second acquire by a different owner must fail.
	if _, ok2, err := l.Acquire(ctx, "tx-1", "owner-B", time.Second); err != nil || ok2 {
		t.Fatalf("second acquire should fail: ok=%v err=%v", ok2, err)
	}
	rel()
	// After release, the lease is free again.
	if _, ok3, err := l.Acquire(ctx, "tx-1", "owner-B", time.Second); err != nil || !ok3 {
		t.Fatalf("acquire after release: ok=%v err=%v", ok3, err)
	}
}

func TestInMemoryReleaseIsIdempotent(t *testing.T) {
	l := NewInMemory(time.Second)
	rel, _, _ := l.Acquire(context.Background(), "tx-2", "o", time.Second)
	rel()
	rel() // must not panic
}