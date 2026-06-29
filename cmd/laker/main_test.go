package main

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go"
	kafkago "github.com/segmentio/kafka-go"
)

// parquetBytes must produce a readable Parquet file whose rows round-trip back
// to the originals — the columnar encoding is the whole point of the lake.
func TestParquetRoundTrip(t *testing.T) {
	want := []Row{
		{EventID: "a", EventType: "edit", Wiki: "enwiki", Title: "Title", User: "Alice", Bot: false, OccurredAt: 1000},
		{EventID: "b", EventType: "log", Wiki: "commonswiki", Title: "has,comma", User: "Bot", Bot: true, OccurredAt: 2000},
		{EventID: "c", EventType: "edit", Wiki: "dewiki", Title: "", User: "", Bot: false, OccurredAt: 0},
	}
	es := make([]entry, len(want))
	for i, r := range want {
		es[i] = entry{row: r}
	}

	body, err := parquetBytes(es)
	if err != nil {
		t.Fatalf("parquetBytes: %v", err)
	}

	r := parquet.NewGenericReader[Row](bytes.NewReader(body))
	defer r.Close()

	got := make([]Row, len(want))
	n, err := r.Read(got)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("read: %v", err)
	}
	if n != len(want) {
		t.Fatalf("read %d rows, want %d", n, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("row %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// A batch straddling midnight (UTC) must split into one bucket per day, each
// keeping its rows in offset order — that one-day-per-object invariant is what
// makes dt= partition pruning correct.
func TestGroupByDateStraddlesMidnight(t *testing.T) {
	// 2026-06-29 23:59:50 UTC, then 00:00:30 the next day, then a late one
	// back on the 29th — interleaved on purpose.
	const before = int64(1782777590) // 2026-06-29 23:59:50 UTC
	const after = int64(1782777630)  // 2026-06-30 00:00:30 UTC
	es := []entry{
		{row: Row{EventID: "a", OccurredAt: before}, msg: kafkago.Message{Offset: 10}},
		{row: Row{EventID: "b", OccurredAt: after}, msg: kafkago.Message{Offset: 11}},
		{row: Row{EventID: "c", OccurredAt: before}, msg: kafkago.Message{Offset: 12}},
	}

	got := groupByDate(es)
	if len(got) != 2 {
		t.Fatalf("got %d date buckets, want 2 (%v)", len(got), sortedDates(got))
	}
	if d := dateOf(before); len(got[d]) != 2 || got[d][0].row.EventID != "a" || got[d][1].row.EventID != "c" {
		t.Errorf("bucket %s = %+v, want [a c] in order", d, got[d])
	}
	if d := dateOf(after); len(got[d]) != 1 || got[d][0].row.EventID != "b" {
		t.Errorf("bucket %s = %+v, want [b]", d, got[d])
	}
}

func TestDateOfUTC(t *testing.T) {
	if d := dateOf(1782777590); d != "2026-06-29" {
		t.Errorf("dateOf = %q, want 2026-06-29", d)
	}
}
