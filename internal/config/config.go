// Package config loads runtime settings from environment variables.
package config

import (
	"fmt"
	"strconv"
	"strings"
)

// Config holds the settings every wiki-stream-lab command needs.
// Fields grow as later PRs introduce components that consume them.
type Config struct {
	KafkaBrokers       []string
	WikimediaStreamURL string
	SQLitePath         string

	// ProducerMaxSeconds bounds how long the producer streams before it stops
	// itself; 0 means run until interrupted. ProducerLogEvery controls how
	// often the producer logs a running count (every N messages).
	ProducerMaxSeconds int
	ProducerLogEvery   int
}

// Load builds a Config from environment variables, applying defaults.
// getenv is injected (pass os.Getenv in production) so tests need no real
// process environment. It returns an error only when KAFKA_BROKERS resolves
// to zero usable brokers.
func Load(getenv func(string) string) (Config, error) {
	maxSeconds, err := getIntOr(getenv, "PRODUCER_MAX_SECONDS", 30)
	if err != nil {
		return Config{}, err
	}
	logEvery, err := getIntOr(getenv, "PRODUCER_LOG_EVERY", 100)
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		KafkaBrokers:       splitBrokers(getOr(getenv, "KAFKA_BROKERS", "localhost:19092")),
		WikimediaStreamURL: getOr(getenv, "WIKIMEDIA_STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange"),
		SQLitePath:         getOr(getenv, "SQLITE_PATH", ".data/wiki-stream-lab.sqlite"),
		ProducerMaxSeconds: maxSeconds,
		ProducerLogEvery:   logEvery,
	}
	if len(cfg.KafkaBrokers) == 0 {
		return Config{}, fmt.Errorf("config: KAFKA_BROKERS resolved to no brokers")
	}
	return cfg, nil
}

// getIntOr returns the env value for key parsed as an int, or def when unset.
func getIntOr(getenv func(string) string, key string, def int) (int, error) {
	v := getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("config: %s=%q: %w", key, v, err)
	}
	return n, nil
}

// getOr returns the env value for key, or def when it is unset/empty.
func getOr(getenv func(string) string, key, def string) string {
	if v := getenv(key); v != "" {
		return v
	}
	return def
}

// splitBrokers parses a comma-separated broker list, trimming spaces and
// dropping empty entries.
func splitBrokers(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
