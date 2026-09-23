// Package daemon runs the scheduler loop: cron-triggered task execution,
// Prometheus metrics and health endpoints, graceful shutdown.
package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/Cikouyanqu/replicron/internal/config"
	"github.com/Cikouyanqu/replicron/internal/engine"
	"github.com/Cikouyanqu/replicron/internal/metrics"
	"github.com/Cikouyanqu/replicron/internal/registry"
	"github.com/Cikouyanqu/replicron/internal/runlog"
	"github.com/Cikouyanqu/replicron/internal/ui"
)

// Options configures the daemon.
type Options struct {
	Config   *config.Config
	DBPath   string          // run log (and lease) database file
	Addr     string          // HTTP listen address for metrics/healthz/ui
	KeepRuns int             // retain at most this many run records
	Lock     registry.Locker // nil = in-process mutex
	UI       bool            // mount the read-only run log UI at /ui
}

// Run blocks until ctx is cancelled, executing tasks on their schedules.
// Cancelling ctx also cancels in-flight runs, which then finalize with
// status "failed" before the process exits.
func Run(ctx context.Context, opts Options, log *slog.Logger) error {
	store, err := runlog.Open(opts.DBPath)
	if err != nil {
		return fmt.Errorf("open run log: %w", err)
	}
	defer func() { _ = store.Close() }()
	if err := store.Prune(opts.KeepRuns); err != nil {
		log.Warn("run log prune failed", "err", err)
	}

	met := metrics.New()
	eng := engine.New(store, met, log)
	if opts.Lock != nil {
		eng.Reg = opts.Lock
	}

	c := cron.New(cron.WithSeconds())
	scheduled := 0
	for _, t := range opts.Config.Tasks {
		if t.Schedule == "" || !t.IsEnabled {
			continue
		}
		if _, err := c.AddFunc(t.Schedule, func() {
			// A panic inside one task must not take down the scheduler.
			defer func() {
				if r := recover(); r != nil {
					log.Error("panic in scheduled task", "task", t.Name, "panic", r)
				}
			}()
			_ = eng.RunTask(ctx, t, "cron")
		}); err != nil {
			return fmt.Errorf("register schedule for %q: %w", t.Name, err)
		}
		scheduled++
	}
	c.Start()
	defer c.Stop()
	log.Info("scheduler started", "tasks", len(opts.Config.Tasks), "scheduled", scheduled)

	mux := http.NewServeMux()
	mux.Handle("/metrics", met.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	if opts.UI {
		h, err := ui.New(store, opts.Config)
		if err != nil {
			return fmt.Errorf("init ui: %w", err)
		}
		h.Register(mux)
	}
	srv := &http.Server{Addr: opts.Addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()
	log.Info("http server started", "addr", opts.Addr,
		"endpoints", "/metrics /healthz"+map[bool]string{true: " /ui", false: ""}[opts.UI])

	select {
	case <-ctx.Done():
	case err := <-errCh:
		return err
	}
	shctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shctx)
	return nil
}
