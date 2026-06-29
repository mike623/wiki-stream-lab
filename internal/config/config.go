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

	// SlowConsumerMS artificially delays the projector by this many
	// milliseconds per message; 0 = full speed. Used to induce consumer lag
	// for the backpressure demo.
	SlowConsumerMS int

	// S3* point the archiver at an S3-compatible object store (RustFS locally).
	S3Endpoint  string
	S3Region    string
	S3AccessKey string
	S3SecretKey string
	S3Bucket    string

	// ArchiveMaxRows / ArchiveFlushSeconds bound the archiver's in-memory
	// batch: it flushes a raw-backup object when either limit is hit. A backup
	// wants a short window, so both default low.
	ArchiveMaxRows      int
	ArchiveFlushSeconds int

	// LakeMaxRows / LakeFlushSeconds bound the laker's in-memory batch. Parquet
	// wants large row groups, so both default high — bigger objects, better
	// columnar compression and scan performance.
	LakeMaxRows      int
	LakeFlushSeconds int
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
	slowMS, err := getIntOr(getenv, "SLOW_CONSUMER_MS", 0)
	if err != nil {
		return Config{}, err
	}
	archiveMaxRows, err := getIntOr(getenv, "ARCHIVE_MAX_ROWS", 5000)
	if err != nil {
		return Config{}, err
	}
	archiveFlushSeconds, err := getIntOr(getenv, "ARCHIVE_FLUSH_SECONDS", 10)
	if err != nil {
		return Config{}, err
	}
	lakeMaxRows, err := getIntOr(getenv, "LAKE_MAX_ROWS", 50000)
	if err != nil {
		return Config{}, err
	}
	lakeFlushSeconds, err := getIntOr(getenv, "LAKE_FLUSH_SECONDS", 60)
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		KafkaBrokers:       splitBrokers(getOr(getenv, "KAFKA_BROKERS", "localhost:19092")),
		WikimediaStreamURL: getOr(getenv, "WIKIMEDIA_STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange"),
		SQLitePath:         getOr(getenv, "SQLITE_PATH", ".data/wiki-stream-lab.sqlite"),
		ProducerMaxSeconds: maxSeconds,
		ProducerLogEvery:   logEvery,
		SlowConsumerMS:     slowMS,

		S3Endpoint:  getOr(getenv, "S3_ENDPOINT", "http://localhost:9100"),
		S3Region:    getOr(getenv, "S3_REGION", "us-east-1"),
		S3AccessKey: getOr(getenv, "S3_ACCESS_KEY", "rustfsadmin"),
		S3SecretKey: getOr(getenv, "S3_SECRET_KEY", "rustfsadmin"),
		S3Bucket:    getOr(getenv, "S3_BUCKET", "wiki-stream-lab"),

		ArchiveMaxRows:      archiveMaxRows,
		ArchiveFlushSeconds: archiveFlushSeconds,

		LakeMaxRows:      lakeMaxRows,
		LakeFlushSeconds: lakeFlushSeconds,
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
