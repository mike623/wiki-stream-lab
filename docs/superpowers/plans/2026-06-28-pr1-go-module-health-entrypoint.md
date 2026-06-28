# PR 1: Minimal Go Module + Health Entrypoint — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create the Go module and a single runnable entrypoint that loads config from the environment, logs structured output via `log/slog`, and shuts down cleanly on SIGINT/SIGTERM.

**Architecture:** One Go module. A pure, table-tested `internal/config` loader (env access injected as a `func(string) string` so tests need no real environment). A thin `cmd/wiki-stream-lab/main.go` that wires `slog` + a signal-cancelled `context.Context` + the config loader and blocks until a shutdown signal. No Kafka, no SSE, no SQLite yet — those arrive in later PRs.

**Tech Stack:** Go 1.22+ (developed on 1.26), standard library only (no third-party dependencies in this PR).

## Global Constraints

- Language: Go, standard-library-first. No TypeScript/pnpm. (`AGENTS.md`)
- Go floor: `go 1.22` in `go.mod`; current local toolchain is `go1.26.4`.
- Module path: `github.com/mike623/wiki-stream-lab` (adjust only if Mike says so).
- `context.Context` is the first argument for I/O / long-running work; honor cancellation. (`docs/PR_CONSTITUTION.md`)
- Structured logs via `log/slog`. Explicit errors wrapped with `fmt.Errorf("...: %w", err)`. No panics in normal control flow.
- PR size < ~300 changed lines, one concept, table-driven tests. (`docs/PR_CONSTITUTION.md`)
- Env var names are fixed by `.env.example`: `KAFKA_BROKERS`, `WIKIMEDIA_STREAM_URL`, `SQLITE_PATH` (this PR uses these three; the rest arrive when their consumer does).
- **Do not commit unless Mike explicitly asks.** The final step stages and stops for review; the actual `git commit` runs only on Mike's go-ahead.

---

## File Structure

- `go.mod` — module declaration + Go version floor. No dependencies this PR.
- `internal/config/config.go` — `Config` struct + `Load(getenv)` pure loader with defaults. The only logic worth unit-testing in this PR.
- `internal/config/config_test.go` — table-driven tests for `Load`.
- `cmd/wiki-stream-lab/main.go` — entrypoint glue: slog logger, signal context, load config, log, block until shutdown. Intentionally not unit-tested (it is wiring); verified by a manual run.

---

## Task 1: Go module + config loader

**Files:**
- Create: `go.mod`
- Create: `internal/config/config.go`
- Test: `internal/config/config_test.go`

**Interfaces:**
- Consumes: nothing (first task).
- Produces:
  - `type config.Config struct { KafkaBrokers []string; WikimediaStreamURL string; SQLitePath string }`
  - `func config.Load(getenv func(string) string) (config.Config, error)` — applies defaults; returns a non-nil error only when `KAFKA_BROKERS` resolves to zero brokers.

- [ ] **Step 1: Initialize the module**

Run:
```bash
cd /Users/mikewong/workspace/wiki-stream-lab
go mod init github.com/mike623/wiki-stream-lab
go mod edit -go=1.22
```
Expected: creates `go.mod` containing:
```
module github.com/mike623/wiki-stream-lab

go 1.22
```

- [ ] **Step 2: Write the failing test**

Create `internal/config/config_test.go`:
```go
package config

import (
	"reflect"
	"testing"
)

func TestLoad(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		want    Config
		wantErr bool
	}{
		{
			name: "defaults when env empty",
			env:  map[string]string{},
			want: Config{
				KafkaBrokers:       []string{"localhost:19092"},
				WikimediaStreamURL: "https://stream.wikimedia.org/v2/stream/recentchange",
				SQLitePath:         ".data/wiki-stream-lab.sqlite",
			},
		},
		{
			name: "multiple brokers are split and trimmed",
			env:  map[string]string{"KAFKA_BROKERS": "a:9092, b:9092 ,c:9092"},
			want: Config{
				KafkaBrokers:       []string{"a:9092", "b:9092", "c:9092"},
				WikimediaStreamURL: "https://stream.wikimedia.org/v2/stream/recentchange",
				SQLitePath:         ".data/wiki-stream-lab.sqlite",
			},
		},
		{
			name: "overrides are honored",
			env: map[string]string{
				"KAFKA_BROKERS":        "broker:9092",
				"WIKIMEDIA_STREAM_URL": "http://localhost:8080/fixture",
				"SQLITE_PATH":          "/tmp/test.sqlite",
			},
			want: Config{
				KafkaBrokers:       []string{"broker:9092"},
				WikimediaStreamURL: "http://localhost:8080/fixture",
				SQLitePath:         "/tmp/test.sqlite",
			},
		},
		{
			name:    "blank KAFKA_BROKERS is an error",
			env:     map[string]string{"KAFKA_BROKERS": " , "},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			getenv := func(k string) string { return tt.env[k] }
			got, err := Load(getenv)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Load() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Load() = %#v, want %#v", got, tt.want)
			}
		})
	}
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `go test ./internal/config/ -v`
Expected: FAIL — compilation error `undefined: Load` / `undefined: Config` (the package has no implementation yet).

- [ ] **Step 4: Write the minimal implementation**

Create `internal/config/config.go`:
```go
// Package config loads runtime settings from environment variables.
package config

import (
	"fmt"
	"strings"
)

// Config holds the settings every wiki-stream-lab command needs.
// Fields grow as later PRs introduce components that consume them.
type Config struct {
	KafkaBrokers       []string
	WikimediaStreamURL string
	SQLitePath         string
}

// Load builds a Config from environment variables, applying defaults.
// getenv is injected (pass os.Getenv in production) so tests need no real
// process environment. It returns an error only when KAFKA_BROKERS resolves
// to zero usable brokers.
func Load(getenv func(string) string) (Config, error) {
	cfg := Config{
		KafkaBrokers:       splitBrokers(getOr(getenv, "KAFKA_BROKERS", "localhost:19092")),
		WikimediaStreamURL: getOr(getenv, "WIKIMEDIA_STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange"),
		SQLitePath:         getOr(getenv, "SQLITE_PATH", ".data/wiki-stream-lab.sqlite"),
	}
	if len(cfg.KafkaBrokers) == 0 {
		return Config{}, fmt.Errorf("config: KAFKA_BROKERS resolved to no brokers")
	}
	return cfg, nil
}

// getOr returns the env value for key, or def when it is unset/empty.
func getOr(getenv func(string) string, key, def string) string {
	if v := getenv(key); v != "" {
		return v
	}
	return def
}

// splitBrokers parses a comma-separated broker list, trimming spaces and
// dropping empty entries.
func splitBrokers(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `go test ./internal/config/ -v`
Expected: PASS — `ok  github.com/mike623/wiki-stream-lab/internal/config` with all four subtests `--- PASS`.

- [ ] **Step 6: Vet and format**

Run:
```bash
go vet ./...
gofmt -l .
```
Expected: `go vet` prints nothing and exits 0; `gofmt -l .` prints nothing (no unformatted files).

- [ ] **Step 7: Stage (commit only on Mike's go-ahead)**

Run:
```bash
git add go.mod internal/config/config.go internal/config/config_test.go
```
Do **not** run `git commit` unless Mike has explicitly authorized it (repo rule). If authorized, the message is:
```
feat(config): add env-based config loader with table tests
```

---

## Task 2: Health entrypoint with slog + signal context

**Files:**
- Create: `cmd/wiki-stream-lab/main.go`

**Interfaces:**
- Consumes: `config.Load(os.Getenv)` and the `config.Config` fields `KafkaBrokers`, `WikimediaStreamURL`, `SQLitePath` from Task 1.
- Produces: a runnable binary `wiki-stream-lab` (no exported Go API).

- [ ] **Step 1: Write the entrypoint**

Create `cmd/wiki-stream-lab/main.go`:
```go
// Command wiki-stream-lab is the health/skeleton entrypoint. It loads config,
// logs it, and blocks until a shutdown signal — proving the module layout,
// structured logging, and graceful cancellation work end to end. Later PRs
// add the producer, validator, and projector commands under cmd/.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/mike623/wiki-stream-lab/internal/config"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load(os.Getenv)
	if err != nil {
		logger.Error("load config", "err", err)
		os.Exit(1)
	}

	// signal.NotifyContext gives a context that is cancelled on SIGINT/SIGTERM.
	// Everything downstream takes this ctx so shutdown propagates cleanly.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("wiki-stream-lab starting",
		"brokers", cfg.KafkaBrokers,
		"stream_url", cfg.WikimediaStreamURL,
		"sqlite_path", cfg.SQLitePath,
	)

	<-ctx.Done()
	logger.Info("shutdown signal received, exiting cleanly")
}
```

- [ ] **Step 2: Build the whole module**

Run: `go build ./...`
Expected: exits 0, no output (compiles cleanly).

- [ ] **Step 3: Run it and verify clean startup + shutdown**

Run:
```bash
go run ./cmd/wiki-stream-lab
```
Expected: a JSON log line like
```
{"time":"...","level":"INFO","msg":"wiki-stream-lab starting","brokers":["localhost:19092"],"stream_url":"https://stream.wikimedia.org/v2/stream/recentchange","sqlite_path":".data/wiki-stream-lab.sqlite"}
```
Then press `Ctrl-C`. Expected: a second JSON line
```
{"time":"...","level":"INFO","msg":"shutdown signal received, exiting cleanly"}
```
and the process exits 0 (no panic, no stack trace).

- [ ] **Step 4: Confirm the error path**

Run:
```bash
KAFKA_BROKERS=" , " go run ./cmd/wiki-stream-lab; echo "exit: $?"
```
Expected: an `ERROR`-level JSON line whose `err` is `config: KAFKA_BROKERS resolved to no brokers`, and `exit: 1`.

- [ ] **Step 5: Full verification sweep**

Run:
```bash
go test ./... -v
go vet ./...
gofmt -l .
```
Expected: tests PASS, `go vet` silent (exit 0), `gofmt -l .` silent.

- [ ] **Step 6: Stage (commit only on Mike's go-ahead)**

Run:
```bash
git add cmd/wiki-stream-lab/main.go
```
Do **not** `git commit` unless Mike has explicitly authorized it. If authorized:
```
feat(cmd): add health entrypoint with slog and signal-cancelled context
```

---

## Reviewer checklist for Mike (PR 1)

- [ ] `Load` takes `getenv func(string) string` rather than calling `os.Getenv` directly — see why that makes the test need no real environment (the Go idiom for testable config).
- [ ] The error path returns `(Config{}, err)` and the caller `os.Exit(1)`s — no panic. Note this vs throwing in TS.
- [ ] `main.go` uses `signal.NotifyContext` + `<-ctx.Done()`; confirm `defer stop()` is present (releases the signal handler).
- [ ] No third-party imports anywhere in this PR (`go.mod` has no `require` block).
- [ ] Table-driven test covers defaults, multi-broker split/trim, overrides, and the blank-broker error — is any case missing you'd want?
- [ ] Diff is well under 300 lines and teaches exactly one thing: a runnable Go skeleton.

## Go idioms introduced (for the handoff)

- Dependency injection via a plain function value (`getenv`) instead of a mock framework.
- `context.Context` from `signal.NotifyContext` for graceful shutdown.
- `log/slog` JSON structured logging.
- Error wrapping with `fmt.Errorf(... %w ...)` (available for later; this PR returns a flat sentinel-style error — fine, no wrap needed yet).
- `internal/` package (not importable outside the module) + `cmd/<name>/main.go` entrypoint convention.

## TypeScript habits avoided (for the handoff)

- No class/constructor for config — a plain struct + function.
- No `try/catch` — explicit `(value, error)` return checked at the call site.
- No DI container / decorators — a function parameter is the seam.
- No `process.on('SIGINT', ...)` callback soup — one cancellable context.

## Stop condition

After Task 2's verification passes, **stop and wait for Mike's review.** Do not start PR 2 (Wikimedia SSE event model + parser).
