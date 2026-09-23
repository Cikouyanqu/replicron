// Package model defines the core domain types shared across replicron.
package model

import "time"

// Row is a single record keyed by column name. Column names come from the
// source query and must match target table columns exactly (case-sensitive).
type Row map[string]any

// RunStatus is the lifecycle state of a task run.
type RunStatus string

const (
	RunStatusRunning     RunStatus = "running"
	RunStatusSucceeded   RunStatus = "succeeded"
	RunStatusFailed      RunStatus = "failed"
	RunStatusSkipped     RunStatus = "skipped"
	RunStatusInterrupted RunStatus = "interrupted"
)

// RunResult is the outcome of a single task execution.
type RunResult struct {
	Task      string
	Trigger   string // "manual" | "cron"
	Status    RunStatus
	TotalRows int64
	OKRows    int64
	FailRows  int64
	Duration  time.Duration
	Err       string
	Samples   []string // first N row-level failure messages
}

// Run is a persisted run record.
type Run struct {
	ID         int64
	Task       string
	Trigger    string
	Status     RunStatus
	TotalRows  int64
	OKRows     int64
	FailRows   int64
	Error      string
	Samples    []string
	StartedAt  time.Time
	FinishedAt *time.Time
}
