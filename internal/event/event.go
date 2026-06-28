// Package event models the Wikimedia Recent Changes events the lab ingests.
// We decode only the subset the pipeline needs; the full upstream schema is at
// https://stream.wikimedia.org/?doc#/streams/get_v2_stream_recentchange
package event

// RawEvent is the subset of a Wikimedia recentchange SSE event we use. Field
// names track the source JSON. Note the upstream payload has no page_id, so
// page identity is (Wiki, Title).
type RawEvent struct {
	Schema     string    `json:"$schema"`
	Meta       Meta      `json:"meta"`
	ID         int64     `json:"id"`
	Type       string    `json:"type"`
	Namespace  int       `json:"namespace"`
	Title      string    `json:"title"`
	TitleURL   string    `json:"title_url"`
	Comment    string    `json:"comment"`
	Timestamp  int64     `json:"timestamp"`
	User       string    `json:"user"`
	Bot        bool      `json:"bot"`
	Minor      bool      `json:"minor"`
	Length     *Length   `json:"length"`
	Revision   *Revision `json:"revision"`
	ServerName string    `json:"server_name"`
	ServerURL  string    `json:"server_url"`
	Wiki       string    `json:"wiki"`
}

// Meta is the event envelope. Meta.ID is a UUID unique per event and is the
// idempotency key for downstream projections.
type Meta struct {
	ID     string `json:"id"`
	Domain string `json:"domain"`
	Stream string `json:"stream"`
	Dt     string `json:"dt"`
}

// Length and Revision appear on edit-type events but are absent on others
// (e.g. "categorize"). They are pointers so nil distinguishes "not present"
// from a real zero value.
type Length struct {
	Old int64 `json:"old"`
	New int64 `json:"new"`
}

type Revision struct {
	Old int64 `json:"old"`
	New int64 `json:"new"`
}
