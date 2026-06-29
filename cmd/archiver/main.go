// Command archiver consumes the validated topic and writes a verbatim,
// gzipped JSONL backup of every event to an S3-compatible store (RustFS).
//
// It is a backup, not a transform: each line is the validated message's bytes
// exactly as they sat in Kafka, so the backup can be replayed back onto the
// topic with no decode step. The Parquet "lake" (separate consumer) owns the
// query-optimized copy.
//
// Crash-safety is the whole lesson: offsets are committed only after the
// object lands in S3. A crash before commit re-reads those messages and writes
// the same key (offset range) again, overwriting identical bytes — at-least-
// once delivery, idempotent storage.
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/mike623/wiki-stream-lab/internal/config"
	wkafka "github.com/mike623/wiki-stream-lab/internal/kafka"
	"github.com/mike623/wiki-stream-lab/internal/objstore"

	kafkago "github.com/segmentio/kafka-go"
)

const consumerGroup = "archiver-raw"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("archiver", "err", err)
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

	logger.Info("archiver starting",
		"group", consumerGroup, "source", wkafka.TopicValidated,
		"bucket", cfg.S3Bucket, "endpoint", cfg.S3Endpoint,
		"max_rows", cfg.ArchiveMaxRows, "flush_seconds", cfg.ArchiveFlushSeconds)

	// FetchMessage blocks, so a time-based flush can't fire while we're parked
	// in it. Pull fetches into a channel and let the main loop select between
	// new messages, the flush ticker, and shutdown.
	msgs := make(chan kafkago.Message)
	go func() {
		defer close(msgs)
		for {
			m, err := reader.FetchMessage(ctx)
			if err != nil {
				return // ctx canceled on shutdown, or reader closed
			}
			select {
			case msgs <- m:
			case <-ctx.Done():
				return
			}
		}
	}()

	a := &archiver{store: store, logger: logger, bucket: cfg.S3Bucket}
	ticker := time.NewTicker(time.Duration(cfg.ArchiveFlushSeconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case m, ok := <-msgs:
			if !ok {
				// Fetch goroutine stopped (shutdown): flush what we have so the
				// final partial batch isn't lost, then exit.
				return a.flush(context.WithoutCancel(ctx), reader)
			}
			a.add(m)
			if a.rows >= cfg.ArchiveMaxRows {
				if err := a.flush(ctx, reader); err != nil {
					return err
				}
			}
		case <-ticker.C:
			if err := a.flush(ctx, reader); err != nil {
				return err
			}
		case <-ctx.Done():
			return a.flush(context.WithoutCancel(ctx), reader)
		}
	}
}

// archiver accumulates messages, bucketed by partition. Keying objects by
// partition keeps the offset range in each object contiguous, so a replayed
// batch maps to the same key and overwrites cleanly.
type archiver struct {
	store   *objstore.Client
	logger  *slog.Logger
	bucket  string
	batch   map[int][]kafkago.Message
	rows    int
	flushes int
}

func (a *archiver) add(m kafkago.Message) {
	if a.batch == nil {
		a.batch = map[int][]kafkago.Message{}
	}
	a.batch[m.Partition] = append(a.batch[m.Partition], m)
	a.rows++
}

// flush writes one gzipped JSONL object per partition, then commits offsets.
// Commit happens last and only on full success: if any Put fails we return the
// error with offsets uncommitted, so the next run re-reads and rewrites.
func (a *archiver) flush(ctx context.Context, reader *kafkago.Reader) error {
	if a.rows == 0 {
		return nil
	}

	var commit []kafkago.Message
	for _, part := range sortedParts(a.batch) {
		ms := a.batch[part]
		lo, hi := ms[0].Offset, ms[len(ms)-1].Offset
		key := fmt.Sprintf("raw/topic=%s/p=%d/%020d-%020d.jsonl.gz",
			wkafka.TopicValidated, part, lo, hi)

		body, err := gzipJSONL(ms)
		if err != nil {
			return err
		}
		if err := a.store.Put(ctx, key, body); err != nil {
			return err
		}
		commit = append(commit, ms[len(ms)-1]) // committing the last commits all prior in the partition
	}

	if err := reader.CommitMessages(ctx, commit...); err != nil {
		return fmt.Errorf("archiver: commit: %w", err)
	}

	a.flushes++
	a.logger.Info("archiver flushed", "rows", a.rows, "objects", len(commit), "flush", a.flushes)
	a.batch = nil
	a.rows = 0
	return nil
}

// gzipJSONL writes each message's verbatim value as one gzipped JSONL line.
func gzipJSONL(ms []kafkago.Message) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	for _, m := range ms {
		if _, err := gz.Write(m.Value); err != nil {
			return nil, fmt.Errorf("archiver: gzip write: %w", err)
		}
		if _, err := gz.Write([]byte{'\n'}); err != nil {
			return nil, fmt.Errorf("archiver: gzip newline: %w", err)
		}
	}
	if err := gz.Close(); err != nil {
		return nil, fmt.Errorf("archiver: gzip close: %w", err)
	}
	return buf.Bytes(), nil
}

func sortedParts(batch map[int][]kafkago.Message) []int {
	parts := make([]int, 0, len(batch))
	for p := range batch {
		parts = append(parts, p)
	}
	sort.Ints(parts)
	return parts
}
