// Package config loads runtime settings from environment variables.
package config

import (
	"fmt"
	"strings"
)

// Config holds the settings every wiki-stream-lab command needs.
// Fields grow as later PRs introduce components that consume them.
type Config struct {
	KafkaBrokers       []string
	WikimediaStreamURL string
	SQLitePath         string
}

// Load builds a Config from environment variables, applying defaults.
// getenv is injected (pass os.Getenv in production) so tests need no real
// process environment. It returns an error only when KAFKA_BROKERS resolves
// to zero usable brokers.
func Load(getenv func(string) string) (Config, error) {
	cfg := Config{
		KafkaBrokers:       splitBrokers(getOr(getenv, "KAFKA_BROKERS", "localhost:19092")),
		WikimediaStreamURL: getOr(getenv, "WIKIMEDIA_STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange"),
		SQLitePath:         getOr(getenv, "SQLITE_PATH", ".data/wiki-stream-lab.sqlite"),
	}
	if len(cfg.KafkaBrokers) == 0 {
		return Config{}, fmt.Errorf("config: KAFKA_BROKERS resolved to no brokers")
	}
	return cfg, nil
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
