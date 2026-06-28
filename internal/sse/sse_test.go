package sse

import (
	"errors"
	"strings"
	"testing"
)

func TestScan(t *testing.T) {
	input := strings.Join([]string{
		": welcome heartbeat",
		"event: message",
		"data: {\"a\":1}",
		"",
		"data: line one",
		"data: line two",
		"",
		"id: 42",
		"data: {\"b\":2}",
		"", // trailing blank
	}, "\n")

	var got []string
	err := Scan(strings.NewReader(input), func(d []byte) error {
		got = append(got, string(d))
		return nil
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	want := []string{`{"a":1}`, "line one\nline two", `{"b":2}`}
	if len(got) != len(want) {
		t.Fatalf("got %d events %q, want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("event[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestScanStopsOnFnError(t *testing.T) {
	input := "data: a\n\ndata: b\n\n"
	boom := errors.New("boom")
	calls := 0
	err := Scan(strings.NewReader(input), func(d []byte) error {
		calls++
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
	if calls != 1 {
		t.Errorf("fn called %d times, want 1 (should stop on first error)", calls)
	}
}
