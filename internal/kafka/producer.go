package kafka

import kafkago "github.com/segmentio/kafka-go"

// NewWriter returns a kafka-go Writer for topic. It uses the Hash balancer so a
// message's key always maps to the same partition — this is what makes the
// wiki:title key give per-page ordering (see docs/adr/0002).
func NewWriter(broker, topic string) *kafkago.Writer {
	return &kafkago.Writer{
		Addr:                   kafkago.TCP(broker),
		Topic:                  topic,
		Balancer:               &kafkago.Hash{},
		AllowAutoTopicCreation: false, // topics are created explicitly via cmd/cli
	}
}
