package main

import "testing"

func TestRawLogRingBuffer(t *testing.T) {
	rl := newRawLog(3)
	for _, s := range []string{"a", "b", "c", "d", "e"} {
		rl.add(s)
	}
	got := rl.snapshot() // newest first, capped at 3
	want := []string{"e", "d", "c"}
	if len(got) != len(want) {
		t.Fatalf("len = %d (%v), want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("snapshot[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestRawLogEmpty(t *testing.T) {
	if got := newRawLog(5).snapshot(); len(got) != 0 {
		t.Errorf("empty snapshot = %v, want []", got)
	}
}
