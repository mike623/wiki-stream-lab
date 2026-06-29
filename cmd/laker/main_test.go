package main

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go"
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
