package cli

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/spf13/cobra"

	"github.com/Cikouyanqu/replicron/internal/config"
	"github.com/Cikouyanqu/replicron/internal/engine"
	"github.com/Cikouyanqu/replicron/internal/model"
	"github.com/Cikouyanqu/replicron/internal/runlog"
)

func newRunCmd() *cobra.Command {
	var configPath, taskName, dbPath string
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run matching tasks once and exit",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load(configPath)
			if err != nil {
				return err
			}
			log := slog.New(slog.NewTextHandler(os.Stderr, nil))
			store, err := runlog.Open(dbPath)
			if err != nil {
				return err
			}
			defer store.Close()

			eng := engine.New(store, engine.NoopMetrics{}, log)
			ran, failed := 0, 0
			for _, t := range cfg.Tasks {
				if !t.IsEnabled {
					continue
				}
				if taskName != "" && t.Name != taskName {
					continue
				}
				res := eng.RunTask(cmd.Context(), t, "manual")
				ran++
				if res.Status != model.RunStatusSucceeded {
					failed++
					fmt.Fprintf(os.Stderr, "task %s: %s (%s)\n", t.Name, res.Status, res.Err)
				}
			}
			if ran == 0 {
				return fmt.Errorf("no enabled tasks matched")
			}
			fmt.Printf("%d task(s) run, %d failed\n", ran, failed)
			if failed > 0 {
				return fmt.Errorf("%d task(s) failed", failed)
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", "replicron.yaml", "task configuration file")
	cmd.Flags().StringVarP(&taskName, "task", "t", "", "run only this task")
	cmd.Flags().StringVar(&dbPath, "db", "replicron.db", "run log database path")
	return cmd
}
