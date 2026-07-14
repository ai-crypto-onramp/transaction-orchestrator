package outbox

import (
	"context"
	"fmt"
	"time"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/store"
	"github.com/segmentio/kafka-go"
)

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
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
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