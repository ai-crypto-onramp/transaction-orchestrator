package outbox

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/store"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func startNats(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping nats test in -short mode")
	}
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "nats:2-alpine",
		ExposedPorts: []string{"4222/tcp"},
		WaitingFor:   wait.ForLog("Listening for client connections").WithStartupTimeout(30 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req, Started: true,
	})
	if err != nil {
		t.Skipf("nats container unavailable: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("host: %v", err)
	}
	port, err := c.MappedPort(ctx, "4222")
	if err != nil {
		t.Fatalf("port: %v", err)
	}
	return fmt.Sprintf("nats://%s:%s", host, port.Port())
}

func TestNATSPublisherRoundTrip(t *testing.T) {
	url := startNats(t)
	pub, err := NewPublisher(url)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	defer pub.Close()
	ev := store.OutboxEvent{EventID: "e1", TxID: "t1", EventType: "transaction.created", DedupKey: "dk"}
	if err := pub.Publish(context.Background(), "transactions.transaction.created", ev); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	// Close on a fresh nil-conn publisher should be a no-op.
	p2 := &NATSPublisher{}
	if err := p2.Close(); err != nil {
		t.Fatalf("Close nil: %v", err)
	}
	if err := p2.Publish(context.Background(), "x", ev); err == nil {
		t.Fatal("expected error on nil conn publish")
	}
}

func TestNATSPublisherDialError(t *testing.T) {
	if _, err := NewNATSPublisher("nats://127.0.0.1:1"); err == nil {
		t.Fatal("expected dial error")
	}
}