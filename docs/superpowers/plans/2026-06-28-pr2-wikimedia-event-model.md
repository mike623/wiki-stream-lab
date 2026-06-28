# PR 2: Wikimedia SSE Event Model + Parser — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Model the subset of a Wikimedia recentchange event the pipeline needs, decode it from JSON, and validate required fields — with table-driven tests over real fixtures.

**Architecture:** A new `internal/event` package: `RawEvent`/`Meta`/`Length`/`Revision` structs with JSON tags, a `ParseRaw([]byte) (RawEvent, error)` decoder, and a `Validate()` method that returns errors wrapping a sentinel `ErrInvalid` so the future validator consumer can route bad records to the DLQ with `errors.Is`. Fixtures in `internal/event/testdata/`.

**Tech Stack:** Go 1.22+, standard library only (`encoding/json`, `errors`, `fmt`, `os`).

## Global Constraints

- Go, stdlib-first. NO third-party deps (`go.mod` stays without a `require` block).
- Module path `github.com/mike623/wiki-stream-lab`. Go floor `go 1.22`.
- Explicit errors wrapped with `%w`. No panics in normal flow.
- PR < ~300 changed lines, one concept (the raw event model + parser), table-driven tests.
- Decode only the subset needed; do not copy the upstream schema verbatim — link to it. (`AGENTS.md`)
- Real source reality: the recentchange payload has **no `page_id`**; `Length`/`Revision` appear on edits but not on e.g. `categorize`, so they are pointers (nil = absent).

---

## File Structure

- `internal/event/event.go` — `RawEvent`, `Meta`, `Length`, `Revision` structs.
- `internal/event/parse.go` — `ErrInvalid`, `ParseRaw`, `Validate`.
- `internal/event/parse_test.go` — table-driven tests over fixtures.
- `internal/event/testdata/categorize.json` — real categorize event (no revision/length).
- `internal/event/testdata/edit.json` — edit event (has revision + length).
- `internal/event/testdata/invalid_missing_meta_id.json` — structurally valid JSON, missing `meta.id`.
- `internal/event/testdata/malformed.json` — not valid JSON.
- `docs/adr/0002-partition-key-wiki-page-id.md` — append a Correction noting page_id is absent → key becomes `wiki:title` at PR 4.

---

## Task 1: event package + parser + fixtures

**Files:** all of the above.

**Interfaces produced (later PRs rely on these):**
- `type event.RawEvent struct { ... Meta event.Meta; Title, Type, User, Wiki string; Timestamp, ID int64; Bot, Minor bool; Namespace int; Length *event.Length; Revision *event.Revision; ... }`
- `type event.Meta struct { ID, Domain, Stream, Dt string }` — `Meta.ID` is the UUID idempotency key.
- `var event.ErrInvalid error`
- `func event.ParseRaw(data []byte) (event.RawEvent, error)` — JSON syntax errors returned as `event: decode: %w`; missing required field returned wrapping `ErrInvalid`.
- `func (event.RawEvent) Validate() error`

- [ ] **Step 1: create the fixtures**

`internal/event/testdata/categorize.json`:
```json
{
  "$schema": "/mediawiki/recentchange/1.0.0",
  "meta": {
    "uri": "https://commons.wikimedia.org/wiki/Category:Media_contributed_by_the_National_Archives_and_Records_Administration",
    "request_id": "a8b69609-e6c7-4106-9c00-92e23f3221ff",
    "id": "547b4bc8-1a5e-4f95-aec7-52121333d843",
    "domain": "commons.wikimedia.org",
    "stream": "mediawiki.recentchange",
    "dt": "2026-06-28T18:01:51.863Z",
    "topic": "eqiad.mediawiki.recentchange",
    "partition": 0,
    "offset": 6287132813
  },
  "id": 3377407805,
  "type": "categorize",
  "namespace": 14,
  "title": "Category:Media contributed by the National Archives and Records Administration",
  "title_url": "https://commons.wikimedia.org/wiki/Category:Media_contributed_by_the_National_Archives_and_Records_Administration",
  "comment": "added to category",
  "timestamp": 1782669709,
  "user": "DPLA bot",
  "bot": true,
  "server_url": "https://commons.wikimedia.org",
  "server_name": "commons.wikimedia.org",
  "server_script_path": "/w",
  "wiki": "commonswiki"
}
```

`internal/event/testdata/edit.json`:
```json
{
  "$schema": "/mediawiki/recentchange/1.0.0",
  "meta": {
    "uri": "https://en.wikipedia.org/wiki/Example",
    "request_id": "00000000-0000-0000-0000-000000000001",
    "id": "11111111-2222-3333-4444-555555555555",
    "domain": "en.wikipedia.org",
    "stream": "mediawiki.recentchange",
    "dt": "2026-06-28T18:02:00.000Z",
    "topic": "eqiad.mediawiki.recentchange",
    "partition": 0,
    "offset": 6287132814
  },
  "id": 1700000000,
  "type": "edit",
  "namespace": 0,
  "title": "Example",
  "title_url": "https://en.wikipedia.org/wiki/Example",
  "comment": "fix typo",
  "timestamp": 1782669720,
  "user": "Alice",
  "bot": false,
  "minor": true,
  "length": { "old": 1200, "new": 1240 },
  "revision": { "old": 1000000, "new": 1000001 },
  "server_url": "https://en.wikipedia.org",
  "server_name": "en.wikipedia.org",
  "server_script_path": "/w",
  "wiki": "enwiki"
}
```

`internal/event/testdata/invalid_missing_meta_id.json` (same as categorize but `meta` has no `id`):
```json
{
  "$schema": "/mediawiki/recentchange/1.0.0",
  "meta": {
    "domain": "commons.wikimedia.org",
    "stream": "mediawiki.recentchange",
    "dt": "2026-06-28T18:01:51.863Z"
  },
  "id": 3377407805,
  "type": "categorize",
  "namespace": 14,
  "title": "Category:Example",
  "timestamp": 1782669709,
  "user": "DPLA bot",
  "bot": true,
  "wiki": "commonswiki"
}
```

`internal/event/testdata/malformed.json` (intentionally truncated, not valid JSON):
```json
{ "type": "edit", "meta": {
```

- [ ] **Step 2: write the failing test** — `internal/event/parse_test.go`:
```go
package event

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return b
}

func TestParseRaw(t *testing.T) {
	tests := []struct {
		name     string
		fixture  string
		wantErr  bool
		errIs    error // err must satisfy errors.Is(err, errIs)
		errIsNot error // err must NOT satisfy errors.Is(err, errIsNot)
		check    func(t *testing.T, e RawEvent)
	}{
		{
			name:    "valid categorize has nil revision and length",
			fixture: "categorize.json",
			check: func(t *testing.T, e RawEvent) {
				if e.Type != "categorize" {
					t.Errorf("type = %q, want categorize", e.Type)
				}
				if e.Wiki != "commonswiki" {
					t.Errorf("wiki = %q, want commonswiki", e.Wiki)
				}
				if e.Meta.ID == "" {
					t.Error("meta.id empty, want a uuid")
				}
				if !e.Bot {
					t.Error("bot = false, want true")
				}
				if e.Revision != nil {
					t.Errorf("revision = %+v, want nil", e.Revision)
				}
				if e.Length != nil {
					t.Errorf("length = %+v, want nil", e.Length)
				}
			},
		},
		{
			name:    "valid edit has revision and length",
			fixture: "edit.json",
			check: func(t *testing.T, e RawEvent) {
				if e.Revision == nil {
					t.Fatal("revision nil, want non-nil")
				}
				if e.Revision.New != 1000001 {
					t.Errorf("revision.new = %d, want 1000001", e.Revision.New)
				}
				if e.Length == nil || e.Length.New != 1240 {
					t.Errorf("length = %+v, want new=1240", e.Length)
				}
				if e.Minor != true {
					t.Error("minor = false, want true")
				}
			},
		},
		{
			name:    "missing meta.id is ErrInvalid",
			fixture: "invalid_missing_meta_id.json",
			wantErr: true,
			errIs:   ErrInvalid,
		},
		{
			name:     "malformed json is a decode error, not ErrInvalid",
			fixture:  "malformed.json",
			wantErr:  true,
			errIsNot: ErrInvalid,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, err := ParseRaw(readFixture(t, tt.fixture))
			if tt.wantErr {
				if err == nil {
					t.Fatal("want error, got nil")
				}
				if tt.errIs != nil && !errors.Is(err, tt.errIs) {
					t.Errorf("error = %v, want errors.Is %v", err, tt.errIs)
				}
				if tt.errIsNot != nil && errors.Is(err, tt.errIsNot) {
					t.Errorf("error = %v, must NOT be %v", err, tt.errIsNot)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.check != nil {
				tt.check(t, e)
			}
		})
	}
}
```

- [ ] **Step 3: run `go test ./internal/event/ -v`** — FAIL to compile (undefined `ParseRaw`, `RawEvent`, `ErrInvalid`).

- [ ] **Step 4: implement `internal/event/event.go`:**
```go
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
```

- [ ] **Step 5: implement `internal/event/parse.go`:**
```go
package event

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ErrInvalid is wrapped by errors returned when a decoded event is missing a
// field the pipeline requires. The validator consumer routes records that
// match errors.Is(err, ErrInvalid) to the dead-letter topic.
var ErrInvalid = errors.New("invalid recentchange event")

// ParseRaw decodes one SSE data payload into a RawEvent and validates the
// fields the pipeline depends on. A JSON syntax error is returned wrapped as a
// decode error; a structurally valid but incomplete event is returned wrapped
// in ErrInvalid.
func ParseRaw(data []byte) (RawEvent, error) {
	var e RawEvent
	if err := json.Unmarshal(data, &e); err != nil {
		return RawEvent{}, fmt.Errorf("event: decode: %w", err)
	}
	if err := e.Validate(); err != nil {
		return RawEvent{}, err
	}
	return e, nil
}

// Validate checks that the fields required downstream are present.
func (e RawEvent) Validate() error {
	switch {
	case e.Meta.ID == "":
		return fmt.Errorf("%w: missing meta.id", ErrInvalid)
	case e.Type == "":
		return fmt.Errorf("%w: missing type", ErrInvalid)
	case e.Title == "":
		return fmt.Errorf("%w: missing title", ErrInvalid)
	case e.Wiki == "":
		return fmt.Errorf("%w: missing wiki", ErrInvalid)
	case e.User == "":
		return fmt.Errorf("%w: missing user", ErrInvalid)
	case e.Timestamp <= 0:
		return fmt.Errorf("%w: missing or non-positive timestamp", ErrInvalid)
	}
	return nil
}
```

- [ ] **Step 6: run `go test ./internal/event/ -v`** — PASS (4 subtests).

- [ ] **Step 7: append a Correction to `docs/adr/0002-partition-key-wiki-page-id.md`** (at end of file):
```markdown

## Correction (PR 2)

Inspecting real recentchange events showed the stream carries **no `page_id`** field — only `title` (page identity within a wiki) and a per-change `id`. The page-oriented key is therefore implemented as **`wiki:title`** in PR 4, not `wiki:page_id`. The reasoning above is unchanged: keying by page identity spreads load evenly and preserves per-page order. Caveat: a page move changes its title and thus its key; acceptable for this lab.
```

- [ ] **Step 8: full sweep** — `go test ./...`, `go vet ./...`, `gofmt -l .` (tests PASS, vet + gofmt silent).

- [ ] **Step 9: commit (Mike authorized commits)** — `git add internal/event docs/adr/0002-partition-key-wiki-page-id.md` then:
```
feat(event): model and parse the Wikimedia recentchange subset
```
with the trailers:
```
Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_017m4xzkm35CUBQbB8vqmP4z
```

---

## Reviewer checklist for Mike (PR 2)

- [ ] `Length`/`Revision` are pointers so nil = "field absent" — see the categorize fixture (no revision) vs the edit fixture. Note this vs TS optional `?:`.
- [ ] `ErrInvalid` is a sentinel wrapped with `%w`; `errors.Is` is how the DLQ routing will distinguish "bad data" from "decode failed". Confirm the malformed-JSON case is NOT classified as `ErrInvalid`.
- [ ] `Validate` is a value receiver (no mutation) — agree it shouldn't be a pointer receiver here?
- [ ] Required-field set: meta.id, type, title, wiki, user, timestamp. Anything you'd add/drop?
- [ ] No third-party imports; diff < 300 lines.

## Go idioms introduced
- Struct tags for JSON (`json:"$schema"`, pointers for optional nested objects).
- Sentinel error + `%w` wrapping + `errors.Is` for typed error classification.
- Value receiver for a non-mutating method.
- `testdata/` directory convention + `os.ReadFile` + `t.Helper()`.

## TypeScript habits avoided
- No Zod/io-ts runtime schema — explicit struct + a `Validate` method.
- No `null | undefined` union juggling — a nil pointer is the single "absent" signal.
- No throwing — `(RawEvent, error)` with a typed sentinel.

## Stop condition
After the sweep passes, **stop and wait for Mike's review.** Do not start PR 3 (Redpanda Docker Compose + topics).
