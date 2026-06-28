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
