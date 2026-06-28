package kafka

import "testing"

func TestPipelineTopics(t *testing.T) {
	specs := PipelineTopics()
	if len(specs) == 0 {
		t.Fatal("PipelineTopics() returned no topics")
	}

	seen := make(map[string]bool)
	for _, s := range specs {
		if s.Name == "" {
			t.Error("found a topic with an empty name")
		}
		if s.Partitions < 1 {
			t.Errorf("%s: partitions = %d, want >= 1", s.Name, s.Partitions)
		}
		if seen[s.Name] {
			t.Errorf("duplicate topic %s", s.Name)
		}
		seen[s.Name] = true
	}

	// raw and validated must exist and carry the names the rest of the
	// pipeline imports.
	for _, want := range []string{TopicRaw, TopicValidated, TopicDeadLetter} {
		if !seen[want] {
			t.Errorf("missing required topic %s", want)
		}
	}
}
