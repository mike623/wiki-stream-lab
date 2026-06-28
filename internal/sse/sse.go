// Package sse parses a text/event-stream, surfacing the "data" payload of each
// event. It is deliberately tiny: it covers only what the Wikimedia stream uses
// (data lines, comment heartbeats, blank-line event boundaries).
package sse

import (
	"bufio"
	"bytes"
	"io"
	"strings"
)

// maxEvent is the scanner's max token size. Recentchange events are a few KB;
// 1 MB is generous headroom against the default 64 KB line limit.
const maxEvent = 1024 * 1024

// Scan reads server-sent events from r and calls fn with the raw bytes of each
// event's data field. A blank line ends an event. Comment lines (starting ":")
// and non-data fields are ignored; multi-line data is joined with "\n" per the
// SSE spec. Scan returns when r is exhausted, r errors, or fn returns an error.
func Scan(r io.Reader, fn func(data []byte) error) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), maxEvent)

	var data bytes.Buffer
	flush := func() error {
		if data.Len() == 0 {
			return nil
		}
		payload := append([]byte(nil), data.Bytes()...) // copy; buffer is reused
		data.Reset()
		return fn(payload)
	}

	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if err := flush(); err != nil {
				return err
			}
		case strings.HasPrefix(line, ":"):
			// comment / heartbeat — ignore
		case strings.HasPrefix(line, "data:"):
			v := strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " ")
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(v)
		default:
			// event:, id:, retry: and others are not needed here
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return flush() // emit a trailing event with no final blank line
}
