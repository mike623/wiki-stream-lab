// Command validator consumes raw recentchange events, validates them, and
// routes each one:
//
//	valid   -> wikimedia.recentchange.validated  (as a normalized Envelope)
//	invalid -> wikimedia.dead_letter             (as a DeadLetter, with source coords)
//
// Offsets are committed only after the downstream write succeeds, so processing
// is at-least-once: a crash mid-message reprocesses it rather than dropping it.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/mike623/wiki-stream-lab/internal/config"
	"github.com/mike623/wiki-stream-lab/internal/event"
	wkafka "github.com/mike623/wiki-stream-lab/internal/kafka"
	kafkago "github.com/segmentio/kafka-go"
)

const consumerGroup = "validator"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("validator", "err", err)
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

	reader := wkafka.NewReader(cfg.KafkaBrokers, consumerGroup, wkafka.TopicRaw)
	defer reader.Close()
	validated := wkafka.NewWriter(cfg.KafkaBrokers[0], wkafka.TopicValidated)
	defer validated.Close()
	dead := wkafka.NewWriter(cfg.KafkaBrokers[0], wkafka.TopicDeadLetter)
	defer dead.Close()

	logger.Info("validator starting", "group", consumerGroup, "source", wkafka.TopicRaw)

	var nValid, nDead int
	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				break // clean shutdown
			}
			return err
		}

		if err := handle(ctx, m, validated, dead, &nValid, &nDead); err != nil {
			// A cancelled context mid-write is shutdown, not failure: don't
			// commit, so the message is redelivered next run (at-least-once).
			if errors.Is(err, context.Canceled) {
				break
			}
			return err
		}
		if err := reader.CommitMessages(ctx, m); err != nil {
			if errors.Is(err, context.Canceled) {
				break
			}
			return err
		}
	}

	logger.Info("validator stopped", "validated", nValid, "dead_lettered", nDead)
	return nil
}

// handle classifies one raw message and writes it to the validated or
// dead-letter topic. Counters are bumped per outcome.
func handle(ctx context.Context, m kafkago.Message, validated, dead *kafkago.Writer, nValid, nDead *int) error {
	env, dl := classify(m)
	if dl != nil {
		body, err := json.Marshal(dl)
		if err != nil {
			return err
		}
		if err := dead.WriteMessages(ctx, kafkago.Message{Key: m.Key, Value: body}); err != nil {
			return err
		}
		*nDead++
		return nil
	}
	body, err := json.Marshal(env)
	if err != nil {
		return err
	}
	// Preserve the wiki:title key so the validated topic keeps per-page ordering.
	if err := validated.WriteMessages(ctx, kafkago.Message{Key: m.Key, Value: body}); err != nil {
		return err
	}
	*nValid++
	return nil
}

// classify decides a raw message's destination. Exactly one return is non-nil:
// the Envelope for valid events, or the DeadLetter for invalid ones.
func classify(m kafkago.Message) (*event.Envelope, *event.DeadLetter) {
	e, err := event.ParseRaw(m.Value)
	if err != nil {
		dl := event.NewDeadLetter(err.Error(), m.Topic, m.Partition, m.Offset, m.Value)
		return nil, &dl
	}
	env := event.NewEnvelope(e)
	return &env, nil
}
