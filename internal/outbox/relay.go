// Package outbox implements the outbox relay worker and the event-bus
// publisher abstraction.
//
// The relay polls outbox_events for pending rows in batches and publishes each
// event to the event bus, then marks them published.  The Postgres-backed
// store uses SELECT ... FOR UPDATE SKIP LOCKED so multiple relay replicas can
// poll concurrently without double-publishing.
//
// Two publisher implementations are provided:
//   - NATSPublisher (github.com/nats-io/nats.go)
//   - InMemoryPublisher (for unit tests)
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/logging"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/store"
)

// Publisher is the event-bus abstraction.  Publish is at-least-once; consumers
// must dedup on the outbox dedup_key.
type Publisher interface {
	Publish(ctx context.Context, subject string, event store.OutboxEvent) error
	Close() error
}

// Relay polls the store for pending outbox events and publishes them.
type Relay struct {
	Store      store.Store
	Publisher  Publisher
	BatchSize  int
	Interval   time.Duration
	SubjectFn  func(eventType string) string

	stop chan struct{}
	wg   sync.WaitGroup
}

// NewRelay returns a Relay with sensible defaults.
func NewRelay(s store.Store, p Publisher, batchSize int, interval time.Duration) *Relay {
	if batchSize <= 0 {
		batchSize = 100
	}
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	return &Relay{
		Store: s, Publisher: p, BatchSize: batchSize, Interval: interval,
		SubjectFn: defaultSubject,
		stop:      make(chan struct{}),
	}
}

func defaultSubject(eventType string) string {
	return "transactions." + eventType
}

// Start spins up the relay loop.  Idempotent.
func (r *Relay) Start(ctx context.Context) {
	r.wg.Add(1)
	go r.loop(ctx)
}

// Stop signals the relay to exit and waits.
func (r *Relay) Stop() {
	close(r.stop)
	r.wg.Wait()
}

func (r *Relay) loop(ctx context.Context) {
	defer r.wg.Done()
	log := logging.From(ctx)
	tick := time.NewTicker(r.Interval)
	defer tick.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-ctx.Done():
			return
		case <-tick.C:
			if err := r.drainOnce(ctx); err != nil {
				log.Warn("outbox drain error", "err", err)
			}
		}
	}
}

// drainOnce claims one batch of pending events (atomically marking them
// inflight), publishes each, then marks the published ones.  Claim + mark
// run in separate transactions so the network Publish call does not hold the
// row lock; a relay crash between publish and mark-published leaves the row
// inflight for a reaper to re-queue (at-least-once).
func (r *Relay) drainOnce(ctx context.Context) error {
	var events []store.OutboxEvent
	if err := r.Store.RunInTx(ctx, func(ts store.TxStore) error {
		var err error
		events, err = ts.ClaimOutboxPending(ctx, r.BatchSize)
		return err
	}); err != nil {
		return fmt.Errorf("claim pending: %w", err)
	}
	if len(events) == 0 {
		return nil
	}
	publishedIDs := make([]string, 0, len(events))
	for _, e := range events {
		subject := r.SubjectFn(e.EventType)
		if err := r.Publisher.Publish(ctx, subject, e); err != nil {
			break
		}
		publishedIDs = append(publishedIDs, e.EventID)
	}
	if len(publishedIDs) == 0 {
		return nil
	}
	return r.Store.RunInTx(ctx, func(ts store.TxStore) error {
		return ts.MarkOutboxPublished(ctx, publishedIDs, time.Now().UTC())
	})
}

// --- in-memory publisher -----------------------------------------------------

// InMemoryPublisher records every published event for assertions in tests.
type InMemoryPublisher struct {
	mu      sync.Mutex
	Events  []store.OutboxEvent
	Subjects []string
	FailOn  int // if >0, the Nth call (1-indexed) returns an error
	calls   int
}

// NewInMemoryPublisher returns a fresh in-memory publisher.
func NewInMemoryPublisher() *InMemoryPublisher { return &InMemoryPublisher{} }

// Publish appends the event to the recorded list.
func (p *InMemoryPublisher) Publish(ctx context.Context, subject string, event store.OutboxEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if p.FailOn > 0 && p.calls == p.FailOn {
		return errors.New("in-memory publisher: simulated failure")
	}
	p.Events = append(p.Events, event)
	p.Subjects = append(p.Subjects, subject)
	return nil
}

// Len returns the number of recorded events under the mutex.
func (p *InMemoryPublisher) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.Events)
}

// Close is a no-op.
func (p *InMemoryPublisher) Close() error { return nil }

// Encode returns the JSON-encoded payload of an event (helper for publishers).
func Encode(e store.OutboxEvent) []byte {
	b, _ := json.Marshal(map[string]any{
		"event_id":   e.EventID,
		"tx_id":      e.TxID,
		"event_type": e.EventType,
		"step":       e.Step,
		"attempt":    e.Attempt,
		"dedup_key":  e.DedupKey,
		"payload":    e.Payload,
		"created_at": e.CreatedAt,
	})
	return b
}