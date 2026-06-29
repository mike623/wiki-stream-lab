// Command laker consumes the validated topic and builds a columnar Parquet
// "lake" in an S3-compatible store (RustFS), queryable directly by DuckDB or
// ClickHouse's s3() function.
//
// It is the curated, query-optimized copy — distinct from the archiver's
// verbatim JSONL backup. Parquet is columnar, so rows must be batched into a
// row group in memory before a file can be written; the batch is the encoder's
// working set, not a durability buffer (Kafka is the durable log). As with the
// archiver, offsets commit only after the object lands, so a crash re-reads and
// rewrites the same key — at-least-once, idempotent.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/mike623/wiki-stream-lab/internal/config"
	"github.com/mike623/wiki-stream-lab/internal/event"
	wkafka "github.com/mike623/wiki-stream-lab/internal/kafka"
	"github.com/mike623/wiki-stream-lab/internal/objstore"

	"github.com/parquet-go/parquet-go"
	kafkago "github.com/segmentio/kafka-go"
)

const consumerGroup = "lake-parquet"

// Row is the Parquet schema, mirroring event.Envelope one-to-one. Struct tags
// name the columns and apply snappy compression to the high-cardinality string
// columns (fast to decompress on scan). occurred_at stays int64 unix-seconds —
// faithful to the source; convert to a timestamp in the query if wanted.
type Row struct {
	EventID    string `parquet:"event_id,snappy"`
	EventType  string `parquet:"event_type,snappy"`
	Wiki       string `parquet:"wiki,snappy"`
	Title      string `parquet:"title,snappy"`
	User       string `parquet:"user,snappy"`
	Bot        bool   `parquet:"bot"`
	OccurredAt int64  `parquet:"occurred_at"`
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("laker", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := objstore.Open(ctx, objstore.Config{
		Endpoint:  cfg.S3Endpoint,
		Region:    cfg.S3Region,
		AccessKey: cfg.S3AccessKey,
		SecretKey: cfg.S3SecretKey,
		Bucket:    cfg.S3Bucket,
	})
	if err != nil {
		return err
	}

	reader := wkafka.NewReader(cfg.KafkaBrokers, consumerGroup, wkafka.TopicValidated)
	defer reader.Close()

	logger.Info("laker starting",
		"group", consumerGroup, "source", wkafka.TopicValidated,
		"bucket", cfg.S3Bucket, "endpoint", cfg.S3Endpoint,
		"max_rows", cfg.LakeMaxRows, "flush_seconds", cfg.LakeFlushSeconds)

	// FetchMessage blocks, so the flush ticker can't fire while parked in it.
	// Pull fetches into a channel; the main loop selects message / ticker /
	// shutdown. (Same shape as the archiver — if a third consumer appears,
	// extract this scaffolding into a shared helper.)
	msgs := make(chan kafkago.Message)
	go func() {
		defer close(msgs)
		for {
			m, err := reader.FetchMessage(ctx)
			if err != nil {
				return
			}
			select {
			case msgs <- m:
			case <-ctx.Done():
				return
			}
		}
	}()

	l := &laker{store: store, logger: logger}
	ticker := time.NewTicker(time.Duration(cfg.LakeFlushSeconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case m, ok := <-msgs:
			if !ok {
				return l.flush(context.WithoutCancel(ctx), reader)
			}
			if err := l.add(m); err != nil {
				return err
			}
			if l.rows >= cfg.LakeMaxRows {
				if err := l.flush(ctx, reader); err != nil {
					return err
				}
			}
		case <-ticker.C:
			if err := l.flush(ctx, reader); err != nil {
				return err
			}
		case <-ctx.Done():
			return l.flush(context.WithoutCancel(ctx), reader)
		}
	}
}

// entry pairs a decoded row with its source message, so the flush can both
// write the row and commit the right offset.
type entry struct {
	row Row
	msg kafkago.Message
}

// laker accumulates decoded rows bucketed by partition. Per-partition objects
// keep each file's offset range contiguous, so a replayed batch overwrites the
// same key instead of duplicating.
type laker struct {
	store   *objstore.Client
	logger  *slog.Logger
	batch   map[int][]entry
	rows    int
	flushes int
}

func (l *laker) add(m kafkago.Message) error {
	var env event.Envelope
	if err := json.Unmarshal(m.Value, &env); err != nil {
		// Validated events are already well-formed; a decode failure is a bug,
		// not bad upstream data — fail loudly.
		return fmt.Errorf("laker: decode validated event: %w", err)
	}
	if l.batch == nil {
		l.batch = map[int][]entry{}
	}
	l.batch[m.Partition] = append(l.batch[m.Partition], entry{
		row: Row(env), // Row mirrors Envelope field-for-field
		msg: m,
	})
	l.rows++
	return nil
}

// flush writes one Parquet object per partition, then commits offsets. Commit
// is last and only on full success: a failed Put leaves offsets uncommitted so
// the next run rewrites the same key.
func (l *laker) flush(ctx context.Context, reader *kafkago.Reader) error {
	if l.rows == 0 {
		return nil
	}

	var commit []kafkago.Message
	for _, part := range sortedParts(l.batch) {
		es := l.batch[part]
		lo, hi := es[0].msg.Offset, es[len(es)-1].msg.Offset
		key := fmt.Sprintf("lake/topic=%s/p=%d/%020d-%020d.parquet",
			wkafka.TopicValidated, part, lo, hi)

		body, err := parquetBytes(es)
		if err != nil {
			return err
		}
		if err := l.store.Put(ctx, key, body); err != nil {
			return err
		}
		commit = append(commit, es[len(es)-1].msg)
	}

	if err := reader.CommitMessages(ctx, commit...); err != nil {
		return fmt.Errorf("laker: commit: %w", err)
	}

	l.flushes++
	l.logger.Info("laker flushed", "rows", l.rows, "objects", len(commit), "flush", l.flushes)
	l.batch = nil
	l.rows = 0
	return nil
}

// parquetBytes encodes the batch's rows into one in-memory Parquet file. Close
// writes the footer and must happen before the bytes are uploaded.
func parquetBytes(es []entry) ([]byte, error) {
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[Row](&buf)
	rows := make([]Row, len(es))
	for i, e := range es {
		rows[i] = e.row
	}
	if _, err := w.Write(rows); err != nil {
		return nil, fmt.Errorf("laker: parquet write: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("laker: parquet close: %w", err)
	}
	return buf.Bytes(), nil
}

func sortedParts(batch map[int][]entry) []int {
	parts := make([]int, 0, len(batch))
	for p := range batch {
		parts = append(parts, p)
	}
	sort.Ints(parts)
	return parts
}
