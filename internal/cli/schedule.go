package cli

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/Cikouyanqu/replicron/internal/config"
	"github.com/Cikouyanqu/replicron/internal/daemon"
)

func newScheduleCmd() *cobra.Command {
	var configPath, dbPath, addr string
	var keep int
	cmd := &cobra.Command{
		Use:   "schedule",
		Short: "Run as a daemon: execute tasks on their cron schedules, serve /metrics and /healthz",
		RunE: func(_ *cobra.Command, _ []string) error {
			cfg, err := config.Load(configPath)
			if err != nil {
				return err
			}
			log := slog.New(slog.NewTextHandler(os.Stderr, nil))
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			return daemon.Run(ctx, cfg, dbPath, addr, keep, log)
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", "replicron.yaml", "task configuration file")
	cmd.Flags().StringVar(&dbPath, "db", "replicron.db", "run log database path")
	cmd.Flags().StringVar(&addr, "addr", ":9101", "HTTP listen address for /metrics and /healthz")
	cmd.Flags().IntVar(&keep, "keep-runs", 10000, "retain at most this many run records")
	return cmd
}
