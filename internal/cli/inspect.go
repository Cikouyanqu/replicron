package cli

import (
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/Cikouyanqu/replicron/internal/config"
	"github.com/Cikouyanqu/replicron/internal/runlog"
)

func newValidateCmd() *cobra.Command {
	var configPath string
	cmd := &cobra.Command{
		Use:   "validate",
		Short: "Validate a task configuration file",
		RunE: func(_ *cobra.Command, _ []string) error {
			cfg, err := config.Load(configPath)
			if err != nil {
				return err
			}
			w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
			fmt.Fprintln(w, "TASK\tSCHEDULE\tSOURCE\tTARGET TABLE\tMODE")
			for _, t := range cfg.Tasks {
				sched := t.Schedule
				if sched == "" {
					sched = "(manual)"
				}
				if !t.IsEnabled {
					sched += " [disabled]"
				}
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
					t.Name, sched, t.Source.Type, t.Target.Table, t.Target.Mode)
			}
			if err := w.Flush(); err != nil {
				return err
			}
			fmt.Printf("%d task(s) valid\n", len(cfg.Tasks))
			return nil
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", "replicron.yaml", "task configuration file")
	return cmd
}

func newRunsCmd() *cobra.Command {
	var dbPath, taskName string
	var limit int
	cmd := &cobra.Command{
		Use:   "runs",
		Short: "List recent run records",
		RunE: func(_ *cobra.Command, _ []string) error {
			store, err := runlog.Open(dbPath)
			if err != nil {
				return err
			}
			defer store.Close()
			runs, err := store.List(taskName, limit)
			if err != nil {
				return err
			}
			w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
			fmt.Fprintln(w, "ID\tTASK\tTRIGGER\tSTATUS\tTOTAL\tOK\tFAIL\tSTARTED (UTC)\tERROR")
			for _, r := range runs {
				started := ""
				if !r.StartedAt.IsZero() {
					started = r.StartedAt.Format(time.RFC3339)
				}
				errMsg := r.Error
				if errMsg == "" && len(r.Samples) > 0 {
					errMsg = fmt.Sprintf("%d sample(s)", len(r.Samples))
				}
				fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%d\t%d\t%d\t%s\t%s\n",
					r.ID, r.Task, r.Trigger, r.Status,
					r.TotalRows, r.OKRows, r.FailRows, started, errMsg)
			}
			return w.Flush()
		},
	}
	cmd.Flags().StringVar(&dbPath, "db", "replicron.db", "run log database path")
	cmd.Flags().StringVarP(&taskName, "task", "t", "", "filter by task name")
	cmd.Flags().IntVarP(&limit, "limit", "n", 20, "number of runs to show")
	return cmd
}
