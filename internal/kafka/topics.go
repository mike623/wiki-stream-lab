// Package kafka holds the pipeline's topic names and thin admin helpers built
// on segmentio/kafka-go. Keeping topic names in one place means producers,
// consumers, and the CLI cannot drift apart.
package kafka

// Topic names for the pipeline.
const (
	TopicRaw        = "wikimedia.recentchange.raw"
	TopicValidated  = "wikimedia.recentchange.validated"
	TopicDeadLetter = "wikimedia.dead_letter"
)

// TopicSpec describes a topic to create.
type TopicSpec struct {
	Name       string
	Partitions int
}

// PipelineTopics returns the topics the MVP pipeline needs. raw and validated
// get 6 partitions so the consumer-group lag demo (PR 8) can scale; the
// dead-letter topic is low-volume and ordering-insensitive, so 1 is enough.
func PipelineTopics() []TopicSpec {
	return []TopicSpec{
		{Name: TopicRaw, Partitions: 6},
		{Name: TopicValidated, Partitions: 6},
		{Name: TopicDeadLetter, Partitions: 1},
	}
}
