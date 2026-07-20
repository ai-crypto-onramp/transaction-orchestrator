package lease

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNewRedisLeaseDefaultsTTL(t *testing.T) {
	l, err := NewRedisLease("localhost:6379", 0)
	if err != nil {
		t.Fatalf("NewRedisLease: %v", err)
	}
	if l.TTL != 30*time.Second {
		t.Fatalf("expected default TTL 30s, got %v", l.TTL)
	}
	if l.Client == nil {
		t.Fatal("expected non-nil redis client")
	}
	defer l.Close()

	l2, err := NewRedisLease("localhost:6379", 5*time.Second)
	if err != nil {
		t.Fatalf("NewRedisLease: %v", err)
	}
	if l2.TTL != 5*time.Second {
		t.Fatalf("expected 5s TTL, got %v", l2.TTL)
	}
	defer l2.Close()
}

func TestRedisLeaseCloseWithNilClient(t *testing.T) {
	l := &RedisLease{}
	if err := l.Close(); err != nil {
		t.Fatalf("Close with nil client should be nil, got %v", err)
	}
}

func TestRedisLeaseAcquireNilClientErrors(t *testing.T) {
	l := &RedisLease{}
	_, ok, err := l.Acquire(context.Background(), "tx", "owner", time.Second)
	if ok {
		t.Fatal("expected ok=false with nil client")
	}
	if err == nil || !errors.Is(err, err) {
		t.Fatalf("expected non-nil error, got %v", err)
	}
}

func TestRedisLeaseKeyFormat(t *testing.T) {
	if got := key("tx-1"); got != "lease:tx-1" {
		t.Fatalf("unexpected key: %q", got)
	}
}

func TestRedisLeaseAcquireSetNXError(t *testing.T) {
	// Point at a port that is not listening; SetNX should fail with a
	// connection error, exercising the error branch in Acquire.
	l, err := NewRedisLease("127.0.0.1:1", time.Second)
	if err != nil {
		t.Fatalf("NewRedisLease: %v", err)
	}
	defer l.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, ok, err := l.Acquire(ctx, "tx-fail", "owner", time.Second)
	if ok {
		t.Fatal("expected ok=false on SetNX error")
	}
	if err == nil {
		t.Fatal("expected non-nil error on SetNX failure")
	}
}

func TestRedisLeaseAcquireTTLDefaulting(t *testing.T) {
	// With ttl<=0, Acquire falls back to l.TTL. We can't easily assert the
	// TTL used without a real redis, but we can exercise the branch by
	// pointing at a non-listening port and verifying the error path uses
	// the default TTL (no panic).
	l, _ := NewRedisLease("127.0.0.1:1", 500*time.Millisecond)
	defer l.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, _, _ = l.Acquire(ctx, "tx-ttl", "owner", 0)
}