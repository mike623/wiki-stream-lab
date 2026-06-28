package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"

	// Aliased because this package is also named "kafka".
	kafkago "github.com/segmentio/kafka-go"
)

// EnsureTopics creates the given topics on the broker. It is idempotent:
// topics that already exist are treated as success, so it is safe to re-run.
func EnsureTopics(ctx context.Context, broker string, specs []TopicSpec) error {
	client := &kafkago.Client{Addr: kafkago.TCP(broker)}

	configs := make([]kafkago.TopicConfig, len(specs))
	for i, s := range specs {
		configs[i] = kafkago.TopicConfig{
			Topic:             s.Name,
			NumPartitions:     s.Partitions,
			ReplicationFactor: 1,
		}
	}

	resp, err := client.CreateTopics(ctx, &kafkago.CreateTopicsRequest{Topics: configs})
	if err != nil {
		return fmt.Errorf("kafka: create topics: %w", err)
	}
	for name, topicErr := range resp.Errors {
		if topicErr != nil && !errors.Is(topicErr, kafkago.TopicAlreadyExists) {
			return fmt.Errorf("kafka: create topic %s: %w", name, topicErr)
		}
	}
	return nil
}

// ListTopics returns the sorted, unique topic names the broker knows about.
func ListTopics(ctx context.Context, broker string) ([]string, error) {
	conn, err := kafkago.DialContext(ctx, "tcp", broker)
	if err != nil {
		return nil, fmt.Errorf("kafka: dial %s: %w", broker, err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, fmt.Errorf("kafka: read partitions: %w", err)
	}

	seen := make(map[string]struct{})
	var topics []string
	for _, p := range partitions {
		if _, ok := seen[p.Topic]; ok {
			continue
		}
		seen[p.Topic] = struct{}{}
		topics = append(topics, p.Topic)
	}
	sort.Strings(topics)
	return topics, nil
}
