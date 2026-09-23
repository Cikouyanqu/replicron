package cli

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/Cikouyanqu/replicron/internal/config"
	"github.com/Cikouyanqu/replicron/internal/daemon"
	"github.com/Cikouyanqu/replicron/internal/lease"
	"github.com/Cikouyanqu/replicron/internal/registry"
)

func newScheduleCmd() *cobra.Command {
	var configPath, dbPath, addr, locking string
	var keep int
	var leaseTTL time.Duration
	var uiEnabled bool
	cmd := &cobra.Command{
		Use:   "schedule",
		Short: "Run as a daemon: execute tasks on their cron schedules, serve /metrics and /healthz",
		RunE: func(_ *cobra.Command, _ []string) error {
			cfg, err := config.Load(configPath)
			if err != nil {
				return err
			}
			if locking != "process" && locking != "db" {
				return fmt.Errorf("--locking must be process or db, got %q", locking)
			}
			var lock registry.Locker = registry.New()
			if locking == "db" {
				l, err := lease.New(dbPath, leaseTTL)
				if err != nil {
					return err
				}
				defer func() { _ = l.Close() }()
				lock = l
			}
			log := slog.New(slog.NewTextHandler(os.Stderr, nil))
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			return daemon.Run(ctx, daemon.Options{
				Config:   cfg,
				DBPath:   dbPath,
				Addr:     addr,
				KeepRuns: keep,
				Lock:     lock,
				UI:       uiEnabled,
			}, log)
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", "replicron.yaml", "task configuration file")
	cmd.Flags().StringVar(&dbPath, "db", "replicron.db", "run log database path")
	cmd.Flags().StringVar(&addr, "addr", ":9101", "HTTP listen address for /metrics and /healthz")
	cmd.Flags().IntVar(&keep, "keep-runs", 10000, "retain at most this many run records")
	cmd.Flags().StringVar(&locking, "locking", "process", "task mutex: process (in-memory) or db (lease table in --db, enables multi-instance)")
	cmd.Flags().DurationVar(&leaseTTL, "lease-ttl", 10*time.Minute, "database lease TTL; renewed on progress, pick a value above the slowest page")
	cmd.Flags().BoolVar(&uiEnabled, "ui", false, "serve the read-only run log UI at /ui")
	return cmd
}
