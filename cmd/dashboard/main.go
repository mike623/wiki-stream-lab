// Command dashboard powers the live view of the pipeline. It consumes the raw
// topic and:
//   - exports Prometheus metrics at /metrics (total, by type, by wiki) that
//     Grafana graphs (throughput, event-type pie, top wikis, total gauge)
//   - keeps the last N raw events in memory, served as JSON at /raw and as a
//     small auto-refreshing page at /
//
// Run it on the host; Prometheus (in compose) scrapes it at host.docker.internal:8090.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/mike623/wiki-stream-lab/internal/config"
	wkafka "github.com/mike623/wiki-stream-lab/internal/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	eventsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "wsl_events_total", Help: "Total recentchange events ingested.",
	})
	eventsByType = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wsl_events_by_type_total", Help: "Recentchange events by type.",
	}, []string{"type"})
	eventsByWiki = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wsl_events_by_wiki_total", Help: "Recentchange events by wiki.",
	}, []string{"wiki"})
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	addr := flag.String("addr", ":8090", "HTTP listen address")
	flag.Parse()

	if err := run(logger, *addr); err != nil {
		logger.Error("dashboard", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger, addr string) error {
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rl := newRawLog(10)
	go consume(ctx, logger, cfg.KafkaBrokers, rl)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/raw", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(rl.snapshot())
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(rawLogPage))
	})

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		<-ctx.Done()
		srv.Shutdown(context.Background())
	}()

	logger.Info("dashboard listening", "addr", addr, "metrics", "/metrics", "rawlog", "/")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// consume reads raw events, updates metrics, and feeds the raw-log ring buffer.
func consume(ctx context.Context, logger *slog.Logger, brokers []string, rl *rawLog) {
	reader := wkafka.NewReader(brokers, "dashboard", wkafka.TopicRaw)
	defer reader.Close()

	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Warn("dashboard read stopped", "err", err)
			}
			return
		}

		var meta struct {
			Type string `json:"type"`
			Wiki string `json:"wiki"`
		}
		if err := json.Unmarshal(m.Value, &meta); err == nil {
			eventsTotal.Inc()
			if meta.Type != "" {
				eventsByType.WithLabelValues(meta.Type).Inc()
			}
			if meta.Wiki != "" {
				eventsByWiki.WithLabelValues(meta.Wiki).Inc()
			}
		}
		rl.add(string(m.Value))
		_ = reader.CommitMessages(ctx, m)
	}
}

// rawLog is a fixed-size ring buffer of the most recent raw events.
type rawLog struct {
	mu  sync.Mutex
	buf []string
	max int
}

func newRawLog(max int) *rawLog { return &rawLog{max: max} }

func (r *rawLog) add(s string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.buf = append(r.buf, s)
	if len(r.buf) > r.max {
		r.buf = r.buf[len(r.buf)-r.max:]
	}
}

// snapshot returns the buffered events newest-first.
func (r *rawLog) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.buf))
	for i, s := range r.buf {
		out[len(r.buf)-1-i] = s
	}
	return out
}

const rawLogPage = `<!doctype html><html><head><meta charset="utf-8">
<title>wiki-stream-lab — last 10 raw events</title>
<style>body{font:13px/1.5 ui-monospace,Menlo,monospace;margin:1.5rem;background:#0b0f14;color:#cdd6f4}
h1{font-size:15px}li{margin:.4rem 0;word-break:break-all;border-left:3px solid #313244;padding-left:.6rem}</style>
</head><body><h1>last 10 raw events <small id="t"></small></h1><ul id="log"></ul>
<script>
async function tick(){
  try{const r=await fetch('/raw');const xs=await r.json();
    document.getElementById('log').innerHTML=(xs||[]).map(x=>'<li>'+x.replace(/</g,'&lt;')+'</li>').join('');
    document.getElementById('t').textContent='('+new Date().toLocaleTimeString()+')';
  }catch(e){}
}
tick();setInterval(tick,2000);
</script></body></html>`
