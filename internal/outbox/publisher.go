package outbox

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/store"
	"github.com/segmentio/kafka-go"
)

// NewPublisher selects a publisher implementation based on the URL scheme:
//   - "kafka://"                 -> KafkaPublisher (brokers parsed from host
//     parts; optional "?topic=foo" sets the default topic)
//   - plain "host:9092[,host2]"  -> KafkaPublisher (no scheme; treated as
//     comma-separated Kafka bootstrap brokers)
//   - "memory://" or empty       -> InMemoryPublisher (testing only)
//
// Any other scheme returns an error. The legacy "nats://" / "tls://" schemes
// are no longer supported; callers should switch to "kafka://".
func NewPublisher(url string) (Publisher, error) {
	switch {
	case url == "" || strings.HasPrefix(url, "memory://"):
		return NewInMemoryPublisher(), nil
	case strings.HasPrefix(url, "kafka://"):
		return newKafkaPublisherFromURL(url)
	case isPlainBrokerList(url):
		return newKafkaPublisherFromURL("kafka://" + url)
	default:
		return nil, fmt.Errorf("outbox: unknown event bus scheme in %q (use kafka://<brokers> or memory://)", url)
	}
}

// isPlainBrokerList reports whether url looks like a comma-separated list of
// host:port brokers with no scheme. We treat any value that contains no
// "://" and at least one ":" (port separator) as a broker list.
func isPlainBrokerList(url string) bool {
	if strings.Contains(url, "://") {
		return false
	}
	return strings.Contains(url, ":")
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

// KafkaPublisher publishes events to a Kafka topic derived from the event
// type (subject).  It implements the Publisher interface.
type KafkaPublisher struct {
	writer *kafka.Writer
	topic  string
}

// NewKafkaPublisher returns a KafkaPublisher targeting the given brokers and
// default topic.  Events are keyed by tx_id so consumers receive per-tx
// ordering.
func NewKafkaPublisher(brokers []string, topic string) (*KafkaPublisher, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafka: no brokers provided")
	}
	if topic == "" {
		topic = "transactions"
	}
	w := &kafka.Writer{
		Addr:          kafka.TCP(brokers...),
		Topic:         topic,
		Balancer:      &kafka.LeastBytes{},
		BatchTimeout:  10 * time.Millisecond,
		RequiredAcks:  kafka.RequireAll,
	}
	return &KafkaPublisher{writer: w, topic: topic}, nil
}

// Publish writes the encoded event to Kafka, keyed by tx_id.  The default
// topic set on the writer is used; subject is currently advisory (callers
// route by event type via consumer groups).
func (p *KafkaPublisher) Publish(ctx context.Context, subject string, event store.OutboxEvent) error {
	if p.writer == nil {
		return fmt.Errorf("kafka publisher: not connected")
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(event.TxID),
		Value: Encode(event),
	})
}

// Close flushes and closes the underlying writer.
func (p *KafkaPublisher) Close() error {
	if p.writer == nil {
		return nil
	}
	return p.writer.Close()
}