package event

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ErrInvalid is wrapped by errors returned when a decoded event is missing a
// field the pipeline requires. The validator consumer routes records that
// match errors.Is(err, ErrInvalid) to the dead-letter topic.
var ErrInvalid = errors.New("invalid recentchange event")

// ParseRaw decodes one SSE data payload into a RawEvent and validates the
// fields the pipeline depends on. A JSON syntax error is returned wrapped as a
// decode error; a structurally valid but incomplete event is returned wrapped
// in ErrInvalid.
func ParseRaw(data []byte) (RawEvent, error) {
	var e RawEvent
	if err := json.Unmarshal(data, &e); err != nil {
		return RawEvent{}, fmt.Errorf("event: decode: %w", err)
	}
	if err := e.Validate(); err != nil {
		return RawEvent{}, err
	}
	return e, nil
}

// PageKey returns the Kafka partition key for a raw recentchange payload:
// "<wiki>:<title>". It does a minimal decode of just wiki and title so the
// producer can key events it would not necessarily fully validate — keeping
// raw bytes flowing onto the log, with full validation deferred to the
// validator consumer. The upstream stream has no page_id, so title is the
// page identity within a wiki (see docs/adr/0002).
func PageKey(raw []byte) (string, error) {
	var k struct {
		Wiki  string `json:"wiki"`
		Title string `json:"title"`
	}
	if err := json.Unmarshal(raw, &k); err != nil {
		return "", fmt.Errorf("event: key decode: %w", err)
	}
	if k.Wiki == "" || k.Title == "" {
		return "", fmt.Errorf("%w: key needs wiki and title", ErrInvalid)
	}
	return k.Wiki + ":" + k.Title, nil
}

// Validate checks that the fields required downstream are present.
func (e RawEvent) Validate() error {
	switch {
	case e.Meta.ID == "":
		return fmt.Errorf("%w: missing meta.id", ErrInvalid)
	case e.Type == "":
		return fmt.Errorf("%w: missing type", ErrInvalid)
	case e.Title == "":
		return fmt.Errorf("%w: missing title", ErrInvalid)
	case e.Wiki == "":
		return fmt.Errorf("%w: missing wiki", ErrInvalid)
	case e.User == "":
		return fmt.Errorf("%w: missing user", ErrInvalid)
	case e.Timestamp <= 0:
		return fmt.Errorf("%w: missing or non-positive timestamp", ErrInvalid)
	}
	return nil
}
