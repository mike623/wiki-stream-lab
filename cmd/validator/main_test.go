package main

import (
	"strings"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
)

func msg(value string, partition int, offset int64) kafkago.Message {
	return kafkago.Message{
		Topic:     "wikimedia.recentchange.raw",
		Partition: partition,
		Offset:    offset,
		Value:     []byte(value),
	}
}

func TestClassify(t *testing.T) {
	t.Run("valid event becomes an envelope", func(t *testing.T) {
		env, dl := classify(msg(`{"meta":{"id":"u1"},"type":"edit","title":"A","wiki":"enwiki","user":"x","timestamp":5}`, 2, 7))
		if dl != nil {
			t.Fatalf("dead letter = %+v, want nil", dl)
		}
		if env == nil || env.EventID != "u1" || env.Wiki != "enwiki" {
			t.Fatalf("envelope = %+v, want EventID u1 / wiki enwiki", env)
		}
	})

	t.Run("missing meta.id becomes a dead letter with source coords", func(t *testing.T) {
		raw := `{"type":"edit","title":"A","wiki":"enwiki","user":"x","timestamp":5}`
		env, dl := classify(msg(raw, 3, 42))
		if env != nil {
			t.Fatalf("envelope = %+v, want nil", env)
		}
		if dl == nil {
			t.Fatal("dead letter nil, want non-nil")
		}
		if dl.Partition != 3 || dl.Offset != 42 {
			t.Errorf("coords = p%d o%d, want p3 o42", dl.Partition, dl.Offset)
		}
		if !strings.Contains(dl.Reason, "invalid") {
			t.Errorf("reason = %q, want it to mention 'invalid'", dl.Reason)
		}
		if dl.Raw != raw {
			t.Errorf("raw not preserved: %q", dl.Raw)
		}
	})

	t.Run("malformed json becomes a dead letter", func(t *testing.T) {
		env, dl := classify(msg(`{"meta":`, 0, 1))
		if env != nil || dl == nil {
			t.Fatalf("want dead letter for malformed json, got env=%+v dl=%+v", env, dl)
		}
	})
}
