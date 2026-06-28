// Command projector consumes validated events and maintains the SQLite
// page-activity read model. Writes are idempotent (deduped on event_id), so
// replaying the log — or at-least-once redelivery — rebuilds the same numbers.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mike623/wiki-stream-lab/internal/config"
	"github.com/mike623/wiki-stream-lab/internal/event"
	wkafka "github.com/mike623/wiki-stream-lab/internal/kafka"
	"github.com/mike623/wiki-stream-lab/internal/projection"
)

const consumerGroup = "projector"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("projector", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return err
	}

	store, err := projection.Open(cfg.SQLitePath)
	if err != nil {
		return err
	}
	defer store.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reader := wkafka.NewReader(cfg.KafkaBrokers, consumerGroup, wkafka.TopicValidated)
	defer reader.Close()

	logger.Info("projector starting", "group", consumerGroup, "source", wkafka.TopicValidated, "db", cfg.SQLitePath, "slow_ms", cfg.SlowConsumerMS)

	var applied, skipped int
loop:
	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				break
			}
			return err
		}

		var env event.Envelope
		if err := json.Unmarshal(m.Value, &env); err != nil {
			// Validated events are already well-formed; a decode failure here is
			// a bug, not bad upstream data — fail loudly.
			return err
		}

		ok, err := store.Apply(ctx, env)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				break
			}
			return err
		}
		if ok {
			applied++
		} else {
			skipped++
		}

		// Backpressure demo: deliberately slow processing so lag builds up.
		if cfg.SlowConsumerMS > 0 {
			select {
			case <-time.After(time.Duration(cfg.SlowConsumerMS) * time.Millisecond):
			case <-ctx.Done():
				break loop // shutting down mid-delay is clean
			}
		}

		if err := reader.CommitMessages(ctx, m); err != nil {
			if errors.Is(err, context.Canceled) {
				break
			}
			return err
		}
	}

	logger.Info("projector stopped", "applied", applied, "skipped_duplicates", skipped)
	return nil
}
