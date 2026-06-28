// Command cli runs operational commands for the lab.
//
//	cli topics create   create the pipeline topics on the broker
//	cli topics list     list the topics the broker knows about
//
// The broker address comes from KAFKA_BROKERS (see .env.example).
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/mike623/wiki-stream-lab/internal/config"
	wkafka "github.com/mike623/wiki-stream-lab/internal/kafka"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) < 2 || args[0] != "topics" {
		return fmt.Errorf("usage: cli topics <create|list>")
	}

	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return err
	}
	broker := cfg.KafkaBrokers[0]
	ctx := context.Background()

	switch args[1] {
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
		return fmt.Errorf("unknown topics subcommand %q (want create|list)", args[1])
	}
}
