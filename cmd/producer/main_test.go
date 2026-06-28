package main

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
)

func TestProduce(t *testing.T) {
	// Two keyable events and one that cannot be keyed (no wiki/title) — the
	// last must be skipped, not produced.
	input := strings.Join([]string{
		`data: {"wiki":"enwiki","title":"Alpha"}`,
		"",
		`data: {"wiki":"dewiki","title":"Beta"}`,
		"",
		`data: {"type":"log"}`,
		"",
	}, "\n")

	type msg struct{ key, val string }
	var got []msg
	sink := func(k, v []byte) error {
		got = append(got, msg{string(k), string(v)})
		return nil
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	n, err := produce(context.Background(), logger, strings.NewReader(input), 0, sink)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if n != 2 {
		t.Fatalf("produced %d, want 2 (third event is unkeyable)", n)
	}
	if got[0].key != "enwiki:Alpha" || got[1].key != "dewiki:Beta" {
		t.Errorf("keys = %q, %q; want enwiki:Alpha, dewiki:Beta", got[0].key, got[1].key)
	}
	if got[0].val != `{"wiki":"enwiki","title":"Alpha"}` {
		t.Errorf("value not stored verbatim: %q", got[0].val)
	}
}

func TestProduceStopsOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	input := `data: {"wiki":"enwiki","title":"Alpha"}` + "\n\n"
	sink := func(k, v []byte) error {
		t.Fatal("sink should not be called when context is already cancelled")
		return nil
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	n, _ := produce(ctx, logger, strings.NewReader(input), 0, sink)
	if n != 0 {
		t.Errorf("produced %d, want 0", n)
	}
}
