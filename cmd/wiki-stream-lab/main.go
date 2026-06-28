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
