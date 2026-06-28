package event

import "testing"

func TestNewEnvelope(t *testing.T) {
	raw := RawEvent{
		Meta:      Meta{ID: "uuid-1"},
		Type:      "edit",
		Wiki:      "enwiki",
		Title:     "Example",
		User:      "Alice",
		Bot:       true,
		Timestamp: 1782669720,
	}
	got := NewEnvelope(raw)
	want := Envelope{
		EventID:    "uuid-1",
		EventType:  "edit",
		Wiki:       "enwiki",
		Title:      "Example",
		User:       "Alice",
		Bot:        true,
		OccurredAt: 1782669720,
	}
	if got != want {
		t.Errorf("NewEnvelope = %+v, want %+v", got, want)
	}
}

func TestNewDeadLetter(t *testing.T) {
	dl := NewDeadLetter("invalid: missing meta.id", "wikimedia.recentchange.raw", 3, 42, []byte(`{"x":1}`))
	if dl.Reason == "" || dl.SourceTopic != "wikimedia.recentchange.raw" {
		t.Errorf("reason/topic wrong: %+v", dl)
	}
	if dl.Partition != 3 || dl.Offset != 42 {
		t.Errorf("coords wrong: partition=%d offset=%d", dl.Partition, dl.Offset)
	}
	if dl.Raw != `{"x":1}` {
		t.Errorf("raw not preserved: %q", dl.Raw)
	}
}
