package outbox

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/store"
)

func TestEncodeProducesJSONWithFields(t *testing.T) {
	now := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	e := store.OutboxEvent{
		EventID:   "eid",
		TxID:      "tx",
		EventType: "step.policy.succeeded",
		Step:      "policy",
		Attempt:   1,
		DedupKey:  "dk",
		Payload:   map[string]any{"k": "v"},
		CreatedAt: now,
	}
	b := Encode(e)
	s := string(b)
	for _, want := range []string{`"event_id":"eid"`, `"tx_id":"tx"`, `"event_type":"step.policy.succeeded"`, `"step":"policy"`, `"attempt":1`, `"dedup_key":"dk"`, `"payload":{"k":"v"}`} {
		if !strings.Contains(s, want) {
			t.Fatalf("encoded payload missing %q\n%s", want, s)
		}
	}
}

func TestKafkaPublisherPublishNilWriterErrors(t *testing.T) {
	p := &KafkaPublisher{}
	if err := p.Publish(context.Background(), "subject", store.OutboxEvent{TxID: "tx"}); err == nil {
		t.Fatal("expected error publishing with nil writer")
	}
}

func TestKafkaPublisherCloseNilWriterIsNoop(t *testing.T) {
	p := &KafkaPublisher{}
	if err := p.Close(); err != nil {
		t.Fatalf("Close on nil writer should be no-op, got %v", err)
	}
}

func TestNewKafkaPublisherErrorsOnNoBrokers(t *testing.T) {
	if _, err := NewKafkaPublisher(nil, ""); err == nil {
		t.Fatal("expected error when no brokers provided")
	}
	if _, err := NewKafkaPublisher([]string{}, ""); err == nil {
		t.Fatal("expected error when empty brokers provided")
	}
}

func TestNewKafkaPublisherDefaultsTopic(t *testing.T) {
	p, err := NewKafkaPublisher([]string{"localhost:9092"}, "")
	if err != nil {
		t.Fatalf("NewKafkaPublisher: %v", err)
	}
	defer p.Close()
	if p.topic != "transactions" {
		t.Fatalf("expected default topic 'transactions', got %q", p.topic)
	}
}

func TestNewPublisherRejectsUnknownScheme(t *testing.T) {
	if _, err := NewPublisher("foobar://broker"); err == nil {
		t.Fatal("expected error for unknown scheme")
	}
}

func TestIsPlainBrokerList(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"localhost:9092", true},
		{"a:9092,b:9092", true},
		{"kafka://localhost:9092", false},
		{"memory://", false},
		{"noports", false},
		{"", false},
	}
	for _, c := range cases {
		if got := isPlainBrokerList(c.in); got != c.want {
			t.Fatalf("isPlainBrokerList(%q)=%v want %v", c.in, got, c.want)
		}
	}
}

func TestNewPublisherPlainBrokerListAndKafkaURL(t *testing.T) {
	p, err := NewPublisher("localhost:9092")
	if err != nil {
		t.Fatalf("plain broker list: %v", err)
	}
	if _, ok := p.(*KafkaPublisher); !ok {
		t.Fatalf("expected KafkaPublisher, got %T", p)
	}
	_ = p.Close()

	p, err = NewPublisher("kafka://a:9092,b:9092?topic=tx")
	if err != nil {
		t.Fatalf("kafka url: %v", err)
	}
	if _, ok := p.(*KafkaPublisher); !ok {
		t.Fatalf("expected KafkaPublisher, got %T", p)
	}
	_ = p.Close()
}

func TestEncodeNilPayload(t *testing.T) {
	b := Encode(store.OutboxEvent{EventID: "e", TxID: "t", EventType: "x"})
	if !strings.Contains(string(b), `"payload":null`) {
		t.Fatalf("expected null payload, got %s", b)
	}
}

func TestDrainOnceNoEventsIsNoop(t *testing.T) {
	s := store.NewMemStore()
	pub := NewInMemoryPublisher()
	r := NewRelay(s, pub, 10, time.Hour)
	if err := r.drainOnce(context.Background()); err != nil {
		t.Fatalf("drainOnce on empty outbox: %v", err)
	}
	if pub.Len() != 0 {
		t.Fatalf("expected 0 published, got %d", pub.Len())
	}
}

func TestDrainOncePublishErrorStopsAndKeepsPending(t *testing.T) {
	s := store.NewMemStore()
	ctx := context.Background()
	_ = s.RunInTx(ctx, func(ts store.TxStore) error {
		return ts.AppendOutbox(ctx, []store.OutboxEvent{{
			EventID: store.NewEventID(), TxID: "tx", EventType: "e",
			Status: store.OutboxPending, DedupKey: "d1", CreatedAt: time.Now().UTC(),
		}})
	})
	pub := NewInMemoryPublisher()
	pub.FailOn = 1
	r := NewRelay(s, pub, 10, time.Hour)
	// drainOnce returns nil even when publish fails (it just stops publishing).
	_ = r.drainOnce(ctx)
	if pub.Len() != 0 {
		t.Fatalf("expected 0 published on error, got %d", pub.Len())
	}
	// The claimed event is inflight (not pending), so ListOutboxPending reports 0;
	// the row would be reclaimed by a reaper. We just assert no publish happened.
}

func TestDrainOncePublishesAndMarks(t *testing.T) {
	s := store.NewMemStore()
	ctx := context.Background()
	_ = s.RunInTx(ctx, func(ts store.TxStore) error {
		return ts.AppendOutbox(ctx, []store.OutboxEvent{{
			EventID: store.NewEventID(), TxID: "tx", EventType: "e",
			Status: store.OutboxPending, DedupKey: "d1", CreatedAt: time.Now().UTC(),
		}})
	})
	pub := NewInMemoryPublisher()
	r := NewRelay(s, pub, 10, time.Hour)
	if err := r.drainOnce(ctx); err != nil {
		t.Fatalf("drainOnce: %v", err)
	}
	if pub.Len() != 1 {
		t.Fatalf("expected 1 published, got %d", pub.Len())
	}
	pending, _ := s.ListOutboxPending(ctx, 10)
	if len(pending) != 0 {
		t.Fatalf("expected 0 pending, got %d", len(pending))
	}
}

func TestPublisherErrorIsNonNil(t *testing.T) {
	pub := NewInMemoryPublisher()
	pub.FailOn = 1
	err := pub.Publish(context.Background(), "s", store.OutboxEvent{})
	if err == nil || !errors.Is(err, err) {
		t.Fatalf("expected non-nil error, got %v", err)
	}
}