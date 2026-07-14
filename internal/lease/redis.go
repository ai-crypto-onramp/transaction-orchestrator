// Package lease provides a Redis-backed implementation of saga.LeaseManager
// for single-flight step execution across replicas.
//
// Acquire takes a per-tx Redis lock key `lease:<txID>` with the configured
// TTL.  A background renewer extends the TTL while the step is running so
// long-running steps do not lose the lease.  Release stops the renewer and
// deletes the key (only if this owner still holds it).
package lease

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisLease is a saga.LeaseManager backed by Redis SET NX EX + a renewer
// goroutine.
type RedisLease struct {
	Client *redis.Client
	TTL    time.Duration
}

// NewRedisLease returns a RedisLease connected to addr (e.g. "localhost:6379").
func NewRedisLease(addr string, ttl time.Duration) (*RedisLease, error) {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	c := redis.NewClient(&redis.Options{Addr: addr})
	return &RedisLease{Client: c, TTL: ttl}, nil
}

// Close releases the Redis connection.
func (l *RedisLease) Close() error {
	if l.Client == nil {
		return nil
	}
	return l.Client.Close()
}

// Acquire tries to take the lease for txID.  On success it starts a renewer
// goroutine and returns a release function.  ok=false means another owner
// holds the lease.
func (l *RedisLease) Acquire(ctx context.Context, txID, owner string, ttl time.Duration) (release func(), ok bool, err error) {
	if l.Client == nil {
		return func() {}, false, errors.New("lease: redis client not configured")
	}
	if ttl <= 0 {
		ttl = l.TTL
	}
	key := key(txID)
	got, err := l.Client.SetNX(ctx, key, owner, ttl).Result()
	if err != nil {
		return func() {}, false, fmt.Errorf("lease SetNX %s: %w", key, err)
	}
	if !got {
		return func() {}, false, nil
	}
	stop := make(chan struct{})
	var once sync.Once
	var renewWg sync.WaitGroup
	renewWg.Add(1)
	go func() {
		defer renewWg.Done()
		t := time.NewTicker(ttl / 2)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				// Renew only if we still own the lease.
				lease, lerr := l.Client.Get(ctx, key).Result()
				if lerr != nil || lease != owner {
					return
				}
				_ = l.Client.Expire(ctx, key, ttl).Err()
			}
		}
	}()
	release = func() {
		once.Do(func() {
			close(stop)
			renewWg.Wait()
			// Delete only if we still own the lease.
			lease, _ := l.Client.Get(context.Background(), key).Result()
			if lease == owner {
				_ = l.Client.Del(context.Background(), key).Err()
			}
		})
	}
	return release, true, nil
}

func key(txID string) string { return "lease:" + txID }