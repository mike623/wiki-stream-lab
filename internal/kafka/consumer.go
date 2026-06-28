package kafka

import kafkago "github.com/segmentio/kafka-go"

// NewReader returns a kafka-go Reader that consumes topic as part of the named
// consumer group. Using a GroupID means partitions are balanced across all
// readers sharing that group (the basis for the lag/scaling demo in PR 8), and
// committed offsets let a restarted consumer resume where it left off.
//
// Offsets are committed manually (CommitMessages) after downstream work
// succeeds, giving at-least-once processing.
func NewReader(brokers []string, groupID, topic string) *kafkago.Reader {
	return kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers,
		GroupID: groupID,
		Topic:   topic,
		// A new group with no committed offset starts from the beginning, so a
		// fresh validator processes the whole log.
		StartOffset: kafkago.FirstOffset,
	})
}
