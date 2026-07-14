package lease

import (
	"context"
	"sync"
	"time"
)

// InMemoryLease is a LeaseManager backed by a process-local map.  It is useful
// for unit tests and single-replica deployments where Redis is unavailable.
// Two separate InMemoryLease instances do not share state.
type InMemoryLease struct {
	mu    sync.Mutex
	ttl   time.Duration
	locks map[string]string // key -> owner
}

// NewInMemory returns a process-local InMemoryLease.
func NewInMemory(ttl time.Duration) *InMemoryLease {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &InMemoryLease{ttl: ttl, locks: make(map[string]string)}
}

// Acquire tries to take the lease for txID.
func (l *InMemoryLease) Acquire(ctx context.Context, txID, owner string, ttl time.Duration) (func(), bool, error) {
	if ttl <= 0 {
		ttl = l.ttl
	}
	key := key(txID)
	l.mu.Lock()
	if _, taken := l.locks[key]; taken {
		l.mu.Unlock()
		return func() {}, false, nil
	}
	l.locks[key] = owner
	l.mu.Unlock()
	stop := make(chan struct{})
	var once sync.Once
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(ttl / 2)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				l.mu.Lock()
				if cur, ok := l.locks[key]; ok && cur == owner {
					// no-op: ownership retained; real impl would reset TTL.
				}
				l.mu.Unlock()
			}
		}
	}()
	release := func() {
		once.Do(func() {
			close(stop)
			wg.Wait()
			l.mu.Lock()
			if cur, ok := l.locks[key]; ok && cur == owner {
				delete(l.locks, key)
			}
			l.mu.Unlock()
		})
	}
	return release, true, nil
}