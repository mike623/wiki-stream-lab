// Command cli runs operational commands for the lab.
//
//	cli topics create   create the pipeline topics on the broker
//	cli topics list     list the topics the broker knows about
//	cli db inspect      show the SQLite projection (counts, top pages, wiki stats)
//	cli db reset        delete the SQLite projection so it can be rebuilt by replay
//
// Broker address comes from KAFKA_BROKERS; DB path from SQLITE_PATH (.env.example).
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/mike623/wiki-stream-lab/internal/config"
	wkafka "github.com/mike623/wiki-stream-lab/internal/kafka"
	"github.com/mike623/wiki-stream-lab/internal/projection"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: cli <topics create|topics list|db inspect|db reset|lag [group] [topic]>")
	}

	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return err
	}
	ctx := context.Background()

	switch args[0] {
	case "topics":
		if len(args) < 2 {
			return fmt.Errorf("usage: cli topics <create|list>")
		}
		return runTopics(ctx, cfg, args[1])
	case "db":
		if len(args) < 2 {
			return fmt.Errorf("usage: cli db <inspect|reset>")
		}
		return runDB(ctx, cfg, args[1])
	case "lag":
		return runLag(ctx, cfg, args[1:])
	default:
		return fmt.Errorf("unknown command %q (want topics|db|lag)", args[0])
	}
}

// runLag reports consumer-group lag. Defaults to the projector group on the
// validated topic; optional args override group and topic.
func runLag(ctx context.Context, cfg config.Config, args []string) error {
	group, topic := "projector", wkafka.TopicValidated
	if len(args) >= 1 && args[0] != "" {
		group = args[0]
	}
	if len(args) >= 2 && args[1] != "" {
		topic = args[1]
	}
	lags, total, err := wkafka.GroupLag(ctx, cfg.KafkaBrokers, group, topic)
	if err != nil {
		return err
	}
	fmt.Printf("group %q on %s — total lag %d\n", group, topic, total)
	for _, l := range lags {
		fmt.Printf("  p%-2d  committed=%-8d high=%-8d  lag=%d\n", l.Partition, l.Committed, l.HighWater, l.Lag)
	}
	return nil
}

func runTopics(ctx context.Context, cfg config.Config, sub string) error {
	broker := cfg.KafkaBrokers[0]
	switch sub {
	case "create":
		specs := wkafka.PipelineTopics()
		if err := wkafka.EnsureTopics(ctx, broker, specs); err != nil {
			return err
		}
		for _, s := range specs {
			fmt.Printf("ensured topic %s (%d partitions)\n", s.Name, s.Partitions)
		}
		return nil
	case "list":
		topics, err := wkafka.ListTopics(ctx, broker)
		if err != nil {
			return err
		}
		for _, t := range topics {
			fmt.Println(t)
		}
		return nil
	default:
		return fmt.Errorf("unknown topics subcommand %q (want create|list)", sub)
	}
}

func runDB(ctx context.Context, cfg config.Config, sub string) error {
	switch sub {
	case "reset":
		// Removing the file (and any WAL/SHM sidecars) is the whole reset: the
		// projector recreates the schema on next start. The log stays the
		// source of truth, so the projection can be rebuilt by replay.
		removed := false
		for _, p := range []string{cfg.SQLitePath, cfg.SQLitePath + "-wal", cfg.SQLitePath + "-shm"} {
			err := os.Remove(p)
			if err == nil {
				removed = true
			} else if !os.IsNotExist(err) {
				return fmt.Errorf("cli: remove %s: %w", p, err)
			}
		}
		if removed {
			fmt.Printf("deleted projection at %s\n", cfg.SQLitePath)
		} else {
			fmt.Printf("no projection at %s (already clean)\n", cfg.SQLitePath)
		}
		return nil
	case "inspect":
		store, err := projection.Open(cfg.SQLitePath)
		if err != nil {
			return err
		}
		defer store.Close()

		pages, processed, err := store.Counts(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("pages: %d   processed_events: %d\n", pages, processed)

		top, err := store.TopPages(ctx, 10)
		if err != nil {
			return err
		}
		fmt.Println("\ntop pages by edits:")
		for _, p := range top {
			fmt.Printf("  %4d edits (%d bot)  %s:%s  last=%s\n", p.EditCount, p.BotEditCount, p.Wiki, p.Title, p.LastUser)
		}

		stats, err := store.WikiStats(ctx)
		if err != nil {
			return err
		}
		fmt.Println("\nwiki stats:")
		for _, w := range stats {
			fmt.Printf("  %-16s total=%d bot=%d human=%d\n", w.Wiki, w.TotalEvents, w.BotEvents, w.HumanEvents)
		}
		return nil
	default:
		return fmt.Errorf("unknown db subcommand %q (want inspect|reset)", sub)
	}
}
