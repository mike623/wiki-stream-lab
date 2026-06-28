# PR Constitution

The development contract for `wiki-stream-lab`. Every agent-implemented PR follows this. The repo exists so Mike — an advanced TypeScript developer — learns Go by **reviewing** changes like a tech lead, so the PR is the product. A correct feature in an unreviewable diff is a failed PR.

## Every PR must

- **Be reviewable in one sitting.** Prefer **< 300 changed lines**. If a change is genuinely bigger, split it or state why it cannot be split at the top of the handoff.
- **Teach exactly one main concept.** One Go concept and/or one Kafka concept per PR. No "while I was in there" extras.
- **Include tests, or a one-line reason they don't apply.** Table-driven where it fits.
- **Include real verification.** Actual commands run and their actual output pasted in the handoff — never "should pass."
- **Explain the Go idioms used.** Name them so Mike learns the vocabulary (e.g. "accept `context.Context` as the first arg", "wrap with `%w`", "return errors, don't panic").
- **Explain the TypeScript habits intentionally avoided.** The bridge from what Mike knows to what Go prefers (e.g. "no class with one method — a plain function", "no `try/catch` — explicit error return").
- **Include a reviewer checklist for Mike** — concrete things to look at in *this* diff, not generic advice.
- **Stop after the PR-sized change and wait for review.** Do not start the next PR.

## Every PR must NOT

- Do a broad rewrite or rename sweep unrelated to the concept.
- Introduce a hidden abstraction (interface with one impl, factory, framework layer) before there are two real callers.
- Land a giant "framework setup" / scaffolding diff. Grow structure as features need it.
- Add a dependency a few lines of stdlib would cover.

## Go defaults (the house style)

- Go 1.22+ (current stable). Standard library first.
- `context.Context` as the first parameter for all I/O and long-running work; honor cancellation.
- `log/slog` for structured logs.
- Small packages, short lower-case names; `internal/` for non-public packages, `cmd/<name>/main.go` for entrypoints.
- Explicit errors; wrap with `fmt.Errorf("...: %w", err)`. No panics in normal control flow.
- Table-driven tests.
- `net/http` directly; a tiny router only when justified — no Gin/Fiber.
- Kafka: `segmentio/kafka-go` unless a PR documents a better choice.
- SQLite: `modernc.org/sqlite` (pure Go, no cgo) via `database/sql`.
- Redpanda via Docker Compose.

## Avoid (until explicitly justified)

TypeScript/pnpm/Vite/Nest · Gin/Fiber · GORM · DI frameworks · over-abstracted Clean Architecture templates · unnecessary generics · clever channel choreography before it's needed · giant initial scaffolds.

## Handoff report format (use verbatim for every implementation PR)

1. **PR title**
2. **Learning goal**
3. **Summary of diff**
4. **Files changed**
5. **Go idioms used**
6. **TypeScript habits avoided**
7. **Commands run + real output**
8. **What Mike should review carefully**
9. **Known risks / follow-ups**
10. **Stop condition** — waiting for review before the next PR.
