// Package cli wires the replicron command-line interface.
package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// NewRootCmd builds the command tree.
func NewRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:     "replicron",
		Short:   "Cron-driven, idempotent table replication between SQL databases",
		Version: "0.1.0",
	}
	root.AddCommand(newRunCmd(), newScheduleCmd(), newValidateCmd(), newRunsCmd())
	return root
}

// Execute runs the CLI and exits non-zero on error.
func Execute() {
	if err := NewRootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
