package event

// Envelope is the normalized internal event written to the validated topic.
// It carries exactly what downstream projections need (PR 6), decoupling them
// from the upstream Wikimedia shape. EventID (the per-event UUID) is the
// idempotency key.
type Envelope struct {
	EventID    string `json:"event_id"`
	EventType  string `json:"event_type"`
	Wiki       string `json:"wiki"`
	Title      string `json:"title"`
	User       string `json:"user"`
	Bot        bool   `json:"bot"`
	OccurredAt int64  `json:"occurred_at"` // unix seconds, from the source timestamp
}

// NewEnvelope projects a validated RawEvent onto the internal Envelope.
func NewEnvelope(e RawEvent) Envelope {
	return Envelope{
		EventID:    e.Meta.ID,
		EventType:  e.Type,
		Wiki:       e.Wiki,
		Title:      e.Title,
		User:       e.User,
		Bot:        e.Bot,
		OccurredAt: e.Timestamp,
	}
}

// DeadLetter is the record written to the dead-letter topic when a raw message
// cannot be validated. It keeps the failure reason and the source coordinates so
// a bad event can be found and reprocessed, plus the original payload verbatim.
type DeadLetter struct {
	Reason      string `json:"reason"`
	SourceTopic string `json:"source_topic"`
	Partition   int    `json:"partition"`
	Offset      int64  `json:"offset"`
	Raw         string `json:"raw"`
}

// NewDeadLetter builds a DeadLetter from a failure reason, the source message's
// coordinates, and its raw bytes.
func NewDeadLetter(reason, sourceTopic string, partition int, offset int64, raw []byte) DeadLetter {
	return DeadLetter{
		Reason:      reason,
		SourceTopic: sourceTopic,
		Partition:   partition,
		Offset:      offset,
		Raw:         string(raw),
	}
}
