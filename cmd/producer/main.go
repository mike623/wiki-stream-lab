// Command producer streams the Wikimedia Recent Changes SSE firehose and writes
// each raw event to the wikimedia.recentchange.raw topic, keyed by wiki:title.
//
// The raw bytes are stored verbatim (no transformation) so the log stays the
// source of truth; validation happens downstream in the validator (PR 5).
//
//	go run ./cmd/producer                 # live stream, stops after PRODUCER_MAX_SECONDS
//	go run ./cmd/producer -file events.txt # replay SSE from a file (offline/demo)
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mike623/wiki-stream-lab/internal/config"
	"github.com/mike623/wiki-stream-lab/internal/event"
	wkafka "github.com/mike623/wiki-stream-lab/internal/kafka"
	"github.com/mike623/wiki-stream-lab/internal/sse"
	kafkago "github.com/segmentio/kafka-go"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("producer", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	fileSrc := flag.String("file", "", "read SSE from a file instead of the live stream")
	flag.Parse()

	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return err
	}

	// Cancel on SIGINT/SIGTERM, and additionally after ProducerMaxSeconds so a
	// demo run stops itself. Both paths cancel the same ctx, which unblocks the
	// HTTP body read and the Kafka writes.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if cfg.ProducerMaxSeconds > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(cfg.ProducerMaxSeconds)*time.Second)
		defer cancel()
	}

	src, err := openSource(ctx, *fileSrc, cfg.WikimediaStreamURL)
	if err != nil {
		return err
	}
	defer src.Close()

	writer := wkafka.NewWriter(cfg.KafkaBrokers[0], wkafka.TopicRaw)
	defer writer.Close()
	sink := func(key, value []byte) error {
		return writer.WriteMessages(ctx, kafkago.Message{Key: key, Value: value})
	}

	logger.Info("producer starting", "topic", wkafka.TopicRaw, "max_seconds", cfg.ProducerMaxSeconds)
	n, err := produce(ctx, logger, src, cfg.ProducerLogEvery, sink)
	logger.Info("producer stopped", "produced", n)

	// A cancelled context (signal or max-seconds) is a clean stop, not a failure.
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return nil
}

// openSource returns the SSE byte stream: a file when path is set, otherwise the
// live Wikimedia endpoint.
func openSource(ctx context.Context, path, url string) (io.ReadCloser, error) {
	if path != "" {
		f, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("open file: %w", err)
		}
		return f, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/event-stream")
	// Wikimedia returns 403 to requests without a descriptive User-Agent.
	// https://meta.wikimedia.org/wiki/User-Agent_policy
	req.Header.Set("User-Agent", "wiki-stream-lab/0.1 (https://github.com/mike623/wiki-stream-lab) learning-project")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("connect stream: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("stream returned %s", resp.Status)
	}
	return resp.Body, nil
}

// produce reads SSE events from r, derives the wiki:title key, and sends each
// raw payload to sink. Events that cannot be keyed (missing wiki/title, or not
// JSON — the firehose occasionally emits control payloads) are skipped, not
// failed. It returns the number of messages produced.
func produce(ctx context.Context, logger *slog.Logger, r io.Reader, logEvery int, sink func(key, value []byte) error) (int, error) {
	count := 0
	err := sse.Scan(r, func(data []byte) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		key, kerr := event.PageKey(data)
		if kerr != nil {
			logger.Warn("skip unkeyable event", "err", kerr)
			return nil
		}
		if err := sink([]byte(key), data); err != nil {
			return err
		}
		count++
		if logEvery > 0 && count%logEvery == 0 {
			logger.Info("producing", "count", count)
		}
		return nil
	})
	return count, err
}
