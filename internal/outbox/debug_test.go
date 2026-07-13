package outbox

import (
	"context"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/logging"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/store"
)

func TestRelayDebug(t *testing.T) {
	s := store.NewMemStore()
	ctx := context.Background()
	_ = s.RunInTx(ctx, func(ts store.TxStore) error {
		return ts.AppendOutbox(ctx, []store.OutboxEvent{{
			EventID: store.NewEventID(), TxID: "tx", EventType: "e",
			Status: store.OutboxPending, DedupKey: "d1", CreatedAt: time.Now().UTC(),
		}})
	})
	pub := NewInMemoryPublisher()
	relay := NewRelay(s, pub, 10, 5*time.Millisecond)
	relay.Start(logging.WithLogger(ctx, logging.New("debug")))
	time.Sleep(100 * time.Millisecond)
	relay.Stop()
	t.Logf("published=%d", len(pub.Events))
	pending, _ := s.ListOutboxPending(ctx, 10)
	t.Logf("pending=%d", len(pending))
}
