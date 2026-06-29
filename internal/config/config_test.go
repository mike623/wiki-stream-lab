package config

import (
	"reflect"
	"testing"
)

func TestLoad(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		want    Config
		wantErr bool
	}{
		{
			name: "defaults when env empty",
			env:  map[string]string{},
			want: Config{
				KafkaBrokers:        []string{"localhost:19092"},
				WikimediaStreamURL:  "https://stream.wikimedia.org/v2/stream/recentchange",
				SQLitePath:          ".data/wiki-stream-lab.sqlite",
				ProducerMaxSeconds:  30,
				ProducerLogEvery:    100,
				SlowConsumerMS:      0,
				S3Endpoint:          "http://localhost:9100",
				S3Region:            "us-east-1",
				S3AccessKey:         "rustfsadmin",
				S3SecretKey:         "rustfsadmin",
				S3Bucket:            "wiki-stream-lab",
				ArchiveMaxRows:      5000,
				ArchiveFlushSeconds: 10,
			},
		},
		{
			name: "multiple brokers are split and trimmed",
			env:  map[string]string{"KAFKA_BROKERS": "a:9092, b:9092 ,c:9092"},
			want: Config{
				KafkaBrokers:        []string{"a:9092", "b:9092", "c:9092"},
				WikimediaStreamURL:  "https://stream.wikimedia.org/v2/stream/recentchange",
				SQLitePath:          ".data/wiki-stream-lab.sqlite",
				ProducerMaxSeconds:  30,
				ProducerLogEvery:    100,
				SlowConsumerMS:      0,
				S3Endpoint:          "http://localhost:9100",
				S3Region:            "us-east-1",
				S3AccessKey:         "rustfsadmin",
				S3SecretKey:         "rustfsadmin",
				S3Bucket:            "wiki-stream-lab",
				ArchiveMaxRows:      5000,
				ArchiveFlushSeconds: 10,
			},
		},
		{
			name: "overrides are honored",
			env: map[string]string{
				"KAFKA_BROKERS":         "broker:9092",
				"WIKIMEDIA_STREAM_URL":  "http://localhost:8080/fixture",
				"SQLITE_PATH":           "/tmp/test.sqlite",
				"PRODUCER_MAX_SECONDS":  "0",
				"PRODUCER_LOG_EVERY":    "500",
				"SLOW_CONSUMER_MS":      "1000",
				"S3_ENDPOINT":           "http://rustfs:9000",
				"S3_REGION":             "eu-west-1",
				"S3_ACCESS_KEY":         "key",
				"S3_SECRET_KEY":         "secret",
				"S3_BUCKET":             "bucket",
				"ARCHIVE_MAX_ROWS":      "100",
				"ARCHIVE_FLUSH_SECONDS": "3",
			},
			want: Config{
				KafkaBrokers:        []string{"broker:9092"},
				WikimediaStreamURL:  "http://localhost:8080/fixture",
				SQLitePath:          "/tmp/test.sqlite",
				ProducerMaxSeconds:  0,
				ProducerLogEvery:    500,
				SlowConsumerMS:      1000,
				S3Endpoint:          "http://rustfs:9000",
				S3Region:            "eu-west-1",
				S3AccessKey:         "key",
				S3SecretKey:         "secret",
				S3Bucket:            "bucket",
				ArchiveMaxRows:      100,
				ArchiveFlushSeconds: 3,
			},
		},
		{
			name:    "blank KAFKA_BROKERS is an error",
			env:     map[string]string{"KAFKA_BROKERS": " , "},
			wantErr: true,
		},
		{
			name:    "non-numeric PRODUCER_MAX_SECONDS is an error",
			env:     map[string]string{"PRODUCER_MAX_SECONDS": "abc"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			getenv := func(k string) string { return tt.env[k] }
			got, err := Load(getenv)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Load() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Load() = %#v, want %#v", got, tt.want)
			}
		})
	}
}
