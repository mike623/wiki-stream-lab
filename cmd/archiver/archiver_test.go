package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"io"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
)

// gzipJSONL must preserve each message's bytes verbatim, one per line — that
// verbatim guarantee is the whole point of the raw backup.
func TestGzipJSONLRoundTrip(t *testing.T) {
	want := []string{
		`{"event_id":"a","wiki":"enwiki"}`,
		`{"event_id":"b","title":"has,comma and \"quotes\""}`,
		`{"event_id":"c"}`,
	}
	var msgs []kafkago.Message
	for _, line := range want {
		msgs = append(msgs, kafkago.Message{Value: []byte(line)})
	}

	body, err := gzipJSONL(msgs)
	if err != nil {
		t.Fatalf("gzipJSONL: %v", err)
	}

	zr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	plain, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var got []string
	sc := bufio.NewScanner(bytes.NewReader(plain))
	for sc.Scan() {
		got = append(got, sc.Text())
	}
	if len(got) != len(want) {
		t.Fatalf("line count = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("line %d = %q, want %q", i, got[i], want[i])
		}
	}
}

// Empty input still produces a valid (empty) gzip stream, not an error.
func TestGzipJSONLEmpty(t *testing.T) {
	body, err := gzipJSONL(nil)
	if err != nil {
		t.Fatalf("gzipJSONL(nil): %v", err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	plain, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(plain) != 0 {
		t.Errorf("decoded %d bytes, want 0", len(plain))
	}
}
