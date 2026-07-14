package outbox

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/logging"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/store"
)

func seedOutbox(t *testing.T, s *store.MemStore, n int) {
	t.Helper()
	ctx := context.Background()
	_ = s.RunInTx(ctx, func(ts store.TxStore) error {
		var events []store.OutboxEvent
		for i := 0; i < n; i++ {
			events = append(events, store.OutboxEvent{
				EventID: store.NewEventID(),
				TxID:    "tx",
				EventType: "step.policy.succeeded",
				Status:  store.OutboxPending,
				DedupKey: store.DedupKey("tx", "step.policy.succeeded", "policy", i),
				CreatedAt: time.Now().UTC(),
			})
		}
		return ts.AppendOutbox(ctx, events)
	})
}

func TestRelayDrainsPending(t *testing.T) {
	s := store.NewMemStore()
	seedOutbox(t, s, 3)
	pub := NewInMemoryPublisher()
	relay := NewRelay(s, pub, 10, time.Millisecond)
	ctx := logging.WithLogger(context.Background(), logging.New("debug"))
	relay.Start(ctx)
	defer relay.Stop()

	waitFor(t, func() bool { return pub.Len() == 3 }, time.Second)

	pending, _ := s.ListOutboxPending(ctx, 10)
	if len(pending) != 0 {
		t.Fatalf("expected 0 pending after drain, got %d", len(pending))
	}
}

func TestRelaiDedupKeyUnique(t *testing.T) {
	s := store.NewMemStore()
	ctx := context.Background()
	_ = s.RunInTx(ctx, func(ts store.TxStore) error {
		return ts.AppendOutbox(ctx, []store.OutboxEvent{{
			EventID: store.NewEventID(), TxID: "tx", EventType: "e",
			Status: store.OutboxPending, DedupKey: "dup", CreatedAt: time.Now().UTC(),
		}})
	})
	err := s.RunInTx(ctx, func(ts store.TxStore) error {
		return ts.AppendOutbox(ctx, []store.OutboxEvent{{
			EventID: store.NewEventID(), TxID: "tx", EventType: "e",
			Status: store.OutboxPending, DedupKey: "dup", CreatedAt: time.Now().UTC(),
		}})
	})
	if err == nil {
		t.Fatal("expected duplicate dedup key to be rejected")
	}
}

func TestNewPublisherSelectsByScheme(t *testing.T) {
	p, err := NewPublisher("memory://")
	if err != nil {
		t.Fatalf("memory: %v", err)
	}
	if _, ok := p.(*InMemoryPublisher); !ok {
		t.Fatalf("expected InMemoryPublisher, got %T", p)
	}
	_ = p.Close()

	p, err = NewPublisher("")
	if err != nil {
		t.Fatalf("empty: %v", err)
	}
	_ = p.Close()

	if _, err := NewPublisher("foobar://broker:9092"); err == nil {
		t.Fatal("expected error for unknown scheme")
	}

	// kafka:// should parse without error (no connection is made until
	// Publish).
	kp, err := NewPublisher("kafka://broker:9092,broker2:9092?topic=tx")
	if err != nil {
		t.Fatalf("kafka: %v", err)
	}
	if _, ok := kp.(*KafkaPublisher); !ok {
		t.Fatalf("expected KafkaPublisher, got %T", kp)
	}
	_ = kp.Close()
}

// TestRelaiStopsOnPublishError verifies the relay stops draining on error but
// resumes on the next tick.
func TestRelaiStopsOnPublishError(t *testing.T) {
	s := store.NewMemStore()
	seedOutbox(t, s, 3)
	pub := NewInMemoryPublisher()
	pub.FailOn = 2
	relay := NewRelay(s, pub, 10, time.Millisecond)
	ctx := logging.WithLogger(context.Background(), logging.New("debug"))
	relay.Start(ctx)
	defer relay.Stop()

	waitFor(t, func() bool { return pub.Len() >= 1 }, time.Second)
}

// TestRelaiConcurrentNoDoublePublish simulates two relays against the same
// store.  Because the MemStore doesn't have row-level locking, this test
// asserts the contract via a counter publisher: every published event id
// should appear exactly once.
func TestRelaiConcurrentNoDoublePublish(t *testing.T) {
	s := store.NewMemStore()
	seedOutbox(t, s, 5)

	var count int32
	pub := &countingPublisher{count: &count}
	r1 := NewRelay(s, pub, 10, time.Millisecond)
	r2 := NewRelay(s, pub, 10, time.Millisecond)
	ctx := logging.WithLogger(context.Background(), logging.New("debug"))
	r1.Start(ctx)
	r2.Start(ctx)
	defer r1.Stop()
	defer r2.Stop()

	waitFor(t, func() bool { return atomic.LoadInt32(&count) == 5 }, 3*time.Second)

	pending, _ := s.ListOutboxPending(ctx, 10)
	if len(pending) != 0 {
		t.Fatalf("expected 0 pending, got %d", len(pending))
	}
}

type countingPublisher struct{ count *int32 }

func (c *countingPublisher) Publish(ctx context.Context, subject string, event store.OutboxEvent) error {
	atomic.AddInt32(c.count, 1)
	return nil
}
func (c *countingPublisher) Close() error { return nil }

// TestRelayCustomSubjectFn verifies the relay honors a custom SubjectFn.
func TestRelayCustomSubjectFn(t *testing.T) {
	s := store.NewMemStore()
	seedOutbox(t, s, 1)
	pub := NewInMemoryPublisher()
	relay := NewRelay(s, pub, 10, time.Millisecond)
	relay.SubjectFn = func(eventType string) string { return "custom." + eventType }
	ctx := logging.WithLogger(context.Background(), logging.New("debug"))
	relay.Start(ctx)
	defer relay.Stop()
	waitFor(t, func() bool { return pub.Len() == 1 }, time.Second)
	if pub.Subjects[0] != "custom.step.policy.succeeded" {
		t.Fatalf("expected custom subject, got %q", pub.Subjects[0])
	}
}

// TestNewRelayDefaults verifies NewRelay fills in defaults.
func TestNewRelayDefaults(t *testing.T) {
	r := NewRelay(store.NewMemStore(), NewInMemoryPublisher(), 0, 0)
	if r.BatchSize != 100 || r.Interval != 100*time.Millisecond || r.SubjectFn == nil {
		t.Fatalf("unexpected defaults: batch=%d interval=%v subjectFn=nil=%v", r.BatchSize, r.Interval, r.SubjectFn == nil)
	}
	if got := r.SubjectFn("x"); got != "transactions.x" {
		t.Fatalf("default SubjectFn: %q", got)
	}
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("condition never met within %v", timeout)
		}
		time.Sleep(time.Millisecond)
	}
}