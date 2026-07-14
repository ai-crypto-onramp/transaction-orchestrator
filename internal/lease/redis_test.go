package lease

import (
	"context"
	"testing"
	"time"

	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

func TestRedisLeaseAcquireRelease(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping redis test in -short mode")
	}
	ctx := context.Background()
	redisC, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Skipf("redis container unavailable: %v", err)
	}
	t.Cleanup(func() { _ = redisC.Terminate(context.Background()) })
	addr, err := redisC.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("conn string: %v", err)
	}
	hostPort := stripSchemeRedis(addr)

	rl, err := NewRedisLease(hostPort, time.Second)
	if err != nil {
		t.Fatalf("NewRedisLease: %v", err)
	}
	defer rl.Close()

	rel, ok, err := rl.Acquire(ctx, "tx-rl-1", "owner-A", time.Second)
	if err != nil || !ok {
		t.Fatalf("first acquire: ok=%v err=%v", ok, err)
	}
	if _, ok2, err := rl.Acquire(ctx, "tx-rl-1", "owner-B", time.Second); err != nil || ok2 {
		t.Fatalf("second acquire should fail: ok=%v err=%v", ok2, err)
	}
	rel()
	if _, ok3, err := rl.Acquire(ctx, "tx-rl-1", "owner-B", time.Second); err != nil || !ok3 {
		t.Fatalf("acquire after release: ok=%v err=%v", ok3, err)
	}
}

func TestRedisLeaseNilClient(t *testing.T) {
	rl := &RedisLease{}
	if _, _, err := rl.Acquire(context.Background(), "t", "o", time.Second); err == nil {
		t.Fatal("expected error on nil client")
	}
}

func TestRedisLeaseCloseNil(t *testing.T) {
	rl := &RedisLease{}
	if err := rl.Close(); err != nil {
		t.Fatalf("Close nil should be nil, got %v", err)
	}
}

func stripSchemeRedis(s string) string {
	for _, p := range []string{"redis://", "rediss://", "http://", "https://"} {
		if len(s) > len(p) && s[:len(p)] == p {
			return s[len(p):]
		}
	}
	return s
}