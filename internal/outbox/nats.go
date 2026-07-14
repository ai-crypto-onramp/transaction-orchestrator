package outbox

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/store"
	"github.com/nats-io/nats.go"
)

// NewPublisher selects a publisher implementation based on the URL scheme:
//   - "nats://" or "tls://"        -> NATSPublisher
//   - "kafka://"                  -> KafkaPublisher (brokers parsed from host
//     parts; optional "?topic=foo" sets the default topic)
//   - "memory://" or empty         -> InMemoryPublisher (testing only)
//
// Any other scheme returns an error.
func NewPublisher(url string) (Publisher, error) {
	switch {
	case url == "" || strings.HasPrefix(url, "memory://"):
		return NewInMemoryPublisher(), nil
	case strings.HasPrefix(url, "nats://") || strings.HasPrefix(url, "tls://"):
		return NewNATSPublisher(url)
	case strings.HasPrefix(url, "kafka://"):
		return newKafkaPublisherFromURL(url)
	default:
		return nil, fmt.Errorf("outbox: unknown event bus scheme in %q", url)
	}
}

func newKafkaPublisherFromURL(url string) (*KafkaPublisher, error) {
	rest := strings.TrimPrefix(url, "kafka://")
	topic := ""
	if i := strings.Index(rest, "?"); i >= 0 {
		q := rest[i+1:]
		rest = rest[:i]
		for _, kv := range strings.Split(q, "&") {
			if strings.HasPrefix(kv, "topic=") {
				topic = strings.TrimPrefix(kv, "topic=")
			}
		}
	}
	brokers := strings.Split(rest, ",")
	for i, b := range brokers {
		brokers[i] = strings.TrimSpace(b)
	}
	return NewKafkaPublisher(brokers, topic)
}

// NATSPublisher publishes events to a NATS subject.
type NATSPublisher struct {
	conn *nats.Conn
}

// NewNATSPublisher connects to the NATS cluster at url.
func NewNATSPublisher(url string) (*NATSPublisher, error) {
	nc, err := nats.Connect(url, nats.Timeout(5*time.Second), nats.MaxReconnects(-1))
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}
	return &NATSPublisher{conn: nc}, nil
}

// Publish encodes the event and publishes it to subject.
func (p *NATSPublisher) Publish(ctx context.Context, subject string, event store.OutboxEvent) error {
	if p.conn == nil {
		return errors.New("nats publisher: not connected")
	}
	return p.conn.Publish(subject, Encode(event))
}

// Close drains and closes the NATS connection.
func (p *NATSPublisher) Close() error {
	if p.conn == nil {
		return nil
	}
	p.conn.Drain()
	p.conn.Close()
	return nil
}