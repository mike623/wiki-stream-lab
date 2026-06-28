// Package projection maintains the SQLite read model built from validated
// events. It is deliberately a thin wrapper over database/sql: the schema and
// the idempotent upserts are the whole point.
package projection

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"github.com/mike623/wiki-stream-lab/internal/event"

	// Pure-Go SQLite driver (no cgo). Registers under the name "sqlite".
	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS processed_events (
	event_id     TEXT PRIMARY KEY,
	processed_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS page_activity (
	wiki           TEXT NOT NULL,
	title          TEXT NOT NULL,
	edit_count     INTEGER NOT NULL,
	bot_edit_count INTEGER NOT NULL,
	last_event_at  INTEGER NOT NULL,
	last_user      TEXT NOT NULL,
	PRIMARY KEY (wiki, title)
);
CREATE TABLE IF NOT EXISTS wiki_stats (
	wiki          TEXT PRIMARY KEY,
	total_events  INTEGER NOT NULL,
	bot_events    INTEGER NOT NULL,
	human_events  INTEGER NOT NULL,
	last_event_at INTEGER NOT NULL
);`

// Store is the SQLite-backed projection.
type Store struct {
	db *sql.DB
}

// Open opens (creating if needed) the SQLite database at path and ensures the
// schema exists.
func Open(path string) (*Store, error) {
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("projection: mkdir %s: %w", dir, err)
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("projection: open %s: %w", path, err)
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("projection: create schema: %w", err)
	}
	return &Store{db: db}, nil
}

// Close closes the underlying database.
func (s *Store) Close() error { return s.db.Close() }

// Page is one row of the page_activity read model.
type Page struct {
	Wiki         string
	Title        string
	EditCount    int
	BotEditCount int
	LastEventAt  int64
	LastUser     string
}

// GetPage returns the page_activity row for (wiki, title). ok is false if no
// row exists yet.
func (s *Store) GetPage(ctx context.Context, wiki, title string) (p Page, ok bool, err error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT wiki, title, edit_count, bot_edit_count, last_event_at, last_user
		 FROM page_activity WHERE wiki = ? AND title = ?`, wiki, title)
	err = row.Scan(&p.Wiki, &p.Title, &p.EditCount, &p.BotEditCount, &p.LastEventAt, &p.LastUser)
	if err == sql.ErrNoRows {
		return Page{}, false, nil
	}
	if err != nil {
		return Page{}, false, fmt.Errorf("projection: get page: %w", err)
	}
	return p, true, nil
}

// WikiStat is one row of the wiki_stats read model.
type WikiStat struct {
	Wiki        string
	TotalEvents int
	BotEvents   int
	HumanEvents int
	LastEventAt int64
}

// Counts returns the number of distinct pages tracked and the number of
// processed (deduped) events.
func (s *Store) Counts(ctx context.Context) (pages, processed int, err error) {
	if err = s.db.QueryRowContext(ctx, `SELECT count(*) FROM page_activity`).Scan(&pages); err != nil {
		return 0, 0, fmt.Errorf("projection: count pages: %w", err)
	}
	if err = s.db.QueryRowContext(ctx, `SELECT count(*) FROM processed_events`).Scan(&processed); err != nil {
		return 0, 0, fmt.Errorf("projection: count processed: %w", err)
	}
	return pages, processed, nil
}

// TopPages returns the most-edited pages, highest edit_count first.
func (s *Store) TopPages(ctx context.Context, limit int) ([]Page, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT wiki, title, edit_count, bot_edit_count, last_event_at, last_user
		 FROM page_activity ORDER BY edit_count DESC, wiki, title LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("projection: top pages: %w", err)
	}
	defer rows.Close()
	var out []Page
	for rows.Next() {
		var p Page
		if err := rows.Scan(&p.Wiki, &p.Title, &p.EditCount, &p.BotEditCount, &p.LastEventAt, &p.LastUser); err != nil {
			return nil, fmt.Errorf("projection: scan page: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// WikiStats returns per-wiki totals, busiest first.
func (s *Store) WikiStats(ctx context.Context) ([]WikiStat, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT wiki, total_events, bot_events, human_events, last_event_at
		 FROM wiki_stats ORDER BY total_events DESC, wiki`)
	if err != nil {
		return nil, fmt.Errorf("projection: wiki stats: %w", err)
	}
	defer rows.Close()
	var out []WikiStat
	for rows.Next() {
		var w WikiStat
		if err := rows.Scan(&w.Wiki, &w.TotalEvents, &w.BotEvents, &w.HumanEvents, &w.LastEventAt); err != nil {
			return nil, fmt.Errorf("projection: scan wiki stat: %w", err)
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// Apply records one validated event into the read model, idempotently. It
// returns applied=false (and changes nothing) if the event_id was already
// processed. The dedupe insert and the projection updates share one
// transaction, so at-least-once redelivery never double-counts.
func (s *Store) Apply(ctx context.Context, env event.Envelope) (applied bool, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("projection: begin: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Dedupe gate: a duplicate event_id inserts 0 rows.
	res, err := tx.ExecContext(ctx,
		`INSERT OR IGNORE INTO processed_events(event_id, processed_at) VALUES(?, ?)`,
		env.EventID, env.OccurredAt)
	if err != nil {
		return false, fmt.Errorf("projection: dedupe insert: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("projection: rows affected: %w", err)
	}
	if n == 0 {
		// Already processed: commit the no-op so the read is consistent.
		return false, tx.Commit()
	}

	botInc := 0
	if env.Bot {
		botInc = 1
	}
	humanInc := 1 - botInc

	if _, err = tx.ExecContext(ctx, `
		INSERT INTO page_activity(wiki, title, edit_count, bot_edit_count, last_event_at, last_user)
		VALUES(?, ?, 1, ?, ?, ?)
		ON CONFLICT(wiki, title) DO UPDATE SET
			edit_count     = edit_count + 1,
			bot_edit_count = bot_edit_count + excluded.bot_edit_count,
			last_event_at  = MAX(last_event_at, excluded.last_event_at),
			last_user      = excluded.last_user`,
		env.Wiki, env.Title, botInc, env.OccurredAt, env.User); err != nil {
		return false, fmt.Errorf("projection: page_activity upsert: %w", err)
	}

	if _, err = tx.ExecContext(ctx, `
		INSERT INTO wiki_stats(wiki, total_events, bot_events, human_events, last_event_at)
		VALUES(?, 1, ?, ?, ?)
		ON CONFLICT(wiki) DO UPDATE SET
			total_events  = total_events + 1,
			bot_events    = bot_events + excluded.bot_events,
			human_events  = human_events + excluded.human_events,
			last_event_at = MAX(last_event_at, excluded.last_event_at)`,
		env.Wiki, botInc, humanInc, env.OccurredAt); err != nil {
		return false, fmt.Errorf("projection: wiki_stats upsert: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return false, fmt.Errorf("projection: commit: %w", err)
	}
	return true, nil
}
