package projection

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/mike623/wiki-stream-lab/internal/event"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestApplyCountsAndIdempotency(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)

	ev := func(id, user string, bot bool, ts int64) event.Envelope {
		return event.Envelope{EventID: id, EventType: "edit", Wiki: "enwiki", Title: "Go", User: user, Bot: bot, OccurredAt: ts}
	}

	// First event applies.
	applied, err := s.Apply(ctx, ev("e1", "alice", false, 100))
	if err != nil || !applied {
		t.Fatalf("first apply: applied=%v err=%v, want true/nil", applied, err)
	}

	// Same event_id again: must be a no-op (the at-least-once dedupe).
	applied, err = s.Apply(ctx, ev("e1", "mallory", true, 200))
	if err != nil {
		t.Fatalf("duplicate apply err: %v", err)
	}
	if applied {
		t.Error("duplicate event_id was applied; want skipped")
	}

	p, ok, err := s.GetPage(ctx, "enwiki", "Go")
	if err != nil || !ok {
		t.Fatalf("get page: ok=%v err=%v", ok, err)
	}
	if p.EditCount != 1 {
		t.Errorf("edit_count = %d, want 1 (duplicate must not double-count)", p.EditCount)
	}
	if p.LastUser != "alice" || p.LastEventAt != 100 {
		t.Errorf("duplicate leaked through: last_user=%q last_event_at=%d, want alice/100", p.LastUser, p.LastEventAt)
	}

	// A genuinely new event on the same page increments.
	if _, err := s.Apply(ctx, ev("e2", "bob", true, 150)); err != nil {
		t.Fatalf("second apply: %v", err)
	}
	p, _, _ = s.GetPage(ctx, "enwiki", "Go")
	if p.EditCount != 2 || p.BotEditCount != 1 {
		t.Errorf("after e2: edit_count=%d bot_edit_count=%d, want 2/1", p.EditCount, p.BotEditCount)
	}
	if p.LastUser != "bob" || p.LastEventAt != 150 {
		t.Errorf("after e2: last_user=%q last_event_at=%d, want bob/150", p.LastUser, p.LastEventAt)
	}
}

func TestReaders(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	apply := func(id, wiki, title, user string, bot bool, ts int64) {
		if _, err := s.Apply(ctx, event.Envelope{EventID: id, Wiki: wiki, Title: title, User: user, Bot: bot, OccurredAt: ts}); err != nil {
			t.Fatal(err)
		}
	}
	apply("1", "enwiki", "Go", "a", false, 10)
	apply("2", "enwiki", "Go", "b", true, 20) // Go now 2 edits (1 bot)
	apply("3", "enwiki", "Rust", "c", false, 30)
	apply("4", "dewiki", "Auto", "d", true, 40)

	pages, processed, err := s.Counts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pages != 3 || processed != 4 {
		t.Errorf("Counts = pages %d, processed %d; want 3, 4", pages, processed)
	}

	top, err := s.TopPages(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(top) != 3 || top[0].Title != "Go" || top[0].EditCount != 2 {
		t.Errorf("TopPages[0] = %+v, want Go with 2 edits first", top[0])
	}

	stats, err := s.WikiStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(stats) != 2 || stats[0].Wiki != "enwiki" || stats[0].TotalEvents != 3 {
		t.Errorf("WikiStats[0] = %+v, want enwiki total 3 first", stats[0])
	}
}

func TestLastEventAtNeverGoesBackwards(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	mk := func(id string, ts int64) event.Envelope {
		return event.Envelope{EventID: id, Wiki: "enwiki", Title: "Go", User: "u", OccurredAt: ts}
	}
	if _, err := s.Apply(ctx, mk("a", 500)); err != nil {
		t.Fatal(err)
	}
	// An older event (out-of-order delivery) must not lower last_event_at.
	if _, err := s.Apply(ctx, mk("b", 200)); err != nil {
		t.Fatal(err)
	}
	p, _, _ := s.GetPage(ctx, "enwiki", "Go")
	if p.LastEventAt != 500 {
		t.Errorf("last_event_at = %d, want 500 (MAX kept)", p.LastEventAt)
	}
}
