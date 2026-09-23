// Package engine executes replication tasks: stream from source, batch-write
// to target, degrade to row-by-row on batch failure, record everything.
package engine

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/Cikouyanqu/replicron/internal/config"
	"github.com/Cikouyanqu/replicron/internal/connector"
	"github.com/Cikouyanqu/replicron/internal/model"
	"github.com/Cikouyanqu/replicron/internal/registry"
	"github.com/Cikouyanqu/replicron/internal/runlog"
)

const maxSamples = 20

// Metrics is the observability hook; the daemon wires a Prometheus
// implementation, one-shot runs use NoopMetrics.
type Metrics interface {
	ObserveRun(task string, status model.RunStatus, ok, fail int64, d time.Duration)
}

// NoopMetrics discards observations.
type NoopMetrics struct{}

func (NoopMetrics) ObserveRun(string, model.RunStatus, int64, int64, time.Duration) {}

// Engine runs tasks. The opener functions are indirection points so tests
// can substitute in-memory connectors.
type Engine struct {
	Reg  *registry.Registry
	Log  *slog.Logger
	Met  Metrics
	Runs *runlog.Store

	OpenSource func(ctx context.Context, typ, dsn string) (connector.SourceConn, error)
	OpenTarget func(ctx context.Context, typ, dsn string) (connector.TargetConn, error)
}

func New(runs *runlog.Store, met Metrics, log *slog.Logger) *Engine {
	return &Engine{
		Reg:        registry.New(),
		Log:        log,
		Met:        met,
		Runs:       runs,
		OpenSource: connector.OpenSource,
		OpenTarget: connector.OpenTarget,
	}
}

// RunTask executes one task once. It never returns nil: contention with a
// still-active run yields a skipped result rather than an error.
func (e *Engine) RunTask(ctx context.Context, t config.Task, trigger string) *model.RunResult {
	start := time.Now()
	res := &model.RunResult{Task: t.Name, Trigger: trigger}

	tok, ok := e.Reg.Acquire(t.Name)
	if !ok {
		res.Status = model.RunStatusSkipped
		res.Err = "previous run still active"
		res.Duration = time.Since(start)
		e.Met.ObserveRun(t.Name, res.Status, 0, 0, res.Duration)
		if id, err := e.Runs.StartRun(t.Name, trigger); err == nil {
			_ = e.Runs.FinishRun(id, *res)
		}
		return res
	}
	// Registered before the finish defer so the run record is finalized
	// before the lease is released.
	defer e.Reg.Release(tok)

	runID, err := e.Runs.StartRun(t.Name, trigger)
	if err != nil {
		e.Log.Warn("run log start failed", "task", t.Name, "err", err)
	}
	defer func() {
		res.Duration = time.Since(start)
		if runID > 0 {
			if err := e.Runs.FinishRun(runID, *res); err != nil {
				e.Log.Warn("run log finish failed", "task", t.Name, "err", err)
			}
		}
		e.Met.ObserveRun(t.Name, res.Status, res.OKRows, res.FailRows, res.Duration)
		e.Log.Info("run finished",
			"task", t.Name, "trigger", trigger, "status", string(res.Status),
			"ok", res.OKRows, "fail", res.FailRows, "duration", res.Duration.Round(time.Millisecond))
	}()

	rctx, cancel := context.WithTimeout(ctx, t.TimeoutDur)
	defer cancel()

	src, err := e.OpenSource(rctx, t.Source.Type, t.Source.DSN)
	if err != nil {
		res.Status = model.RunStatusFailed
		res.Err = fmt.Sprintf("open source: %v", err)
		return res
	}
	defer func() {
		if err := src.Close(); err != nil {
			e.Log.Warn("source close failed", "task", t.Name, "err", err)
		}
	}()

	tgt, err := e.OpenTarget(rctx, t.Target.Type, t.Target.DSN)
	if err != nil {
		res.Status = model.RunStatusFailed
		res.Err = fmt.Sprintf("open target: %v", err)
		return res
	}
	defer func() {
		if err := tgt.Close(); err != nil {
			e.Log.Warn("target close failed", "task", t.Name, "err", err)
		}
	}()

	// Incremental tasks substitute the tracked watermark into the query
	// before streaming; the tracker folds the new maximum as rows arrive.
	query := t.Source.Query
	var wm *watermarkTracker
	if t.Incremental != nil {
		if !strings.Contains(query, watermarkToken) {
			res.Status = model.RunStatusFailed
			res.Err = "incremental task query must reference " + watermarkToken
			return res
		}
		prev, werr := e.Runs.GetWatermark(t.Name)
		if werr != nil {
			res.Status = model.RunStatusFailed
			res.Err = fmt.Sprintf("read watermark: %v", werr)
			return res
		}
		lit := t.Incremental.Initial
		if prev != nil {
			lit, werr = watermarkLiteral(prev)
			if werr != nil {
				res.Status = model.RunStatusFailed
				res.Err = fmt.Sprintf("render watermark: %v", werr)
				return res
			}
		}
		query = strings.ReplaceAll(query, watermarkToken, lit)
		wm = &watermarkTracker{column: t.Incremental.Column}
		e.Log.Info("incremental run", "task", t.Name, "watermark", lit)
	}

	keysChecked := false
	var totalSeen int64
	_, err = src.Stream(rctx, query, t.PageSize, func(page []model.Row) error {
		if len(page) == 0 {
			return nil
		}
		cols := sortedCols(page[0])
		if err := connector.ValidateColumns(cols); err != nil {
			return fmt.Errorf("source column names rejected: %w", err)
		}
		if !keysChecked {
			for _, k := range t.Target.Keys {
				if _, ok := page[0][k]; !ok {
					return fmt.Errorf("upsert key %q missing from source columns %v", k, cols)
				}
			}
			if t.Incremental != nil {
				if _, ok := page[0][t.Incremental.Column]; !ok {
					return fmt.Errorf("incremental.column %q missing from source columns %v", t.Incremental.Column, cols)
				}
			}
			keysChecked = true
		}
		for i := range page {
			if len(page[i]) != len(cols) {
				return fmt.Errorf("row %d has %d columns, expected %d: heterogeneous result sets are unsupported", i, len(page[i]), len(cols))
			}
		}
		if wm != nil {
			if err := wm.observe(page); err != nil {
				return fmt.Errorf("watermark column %q: %w", wm.column, err)
			}
		}

		batch := clampBatch(t.Target.BatchSize, len(cols))
		for off := 0; off < len(page); {
			end := min(off+batch, len(page))
			chunk := page[off:end]
			n, err := writeChunk(rctx, tgt, t, cols, chunk)
			if err != nil {
				// Degrade to row-by-row so one bad row does not sink the
				// whole batch; failures are kept in samples.
				e.Log.Warn("batch write failed, falling back to row-by-row",
					"task", t.Name, "rows", len(chunk), "err", err)
				for _, r := range chunk {
					if _, err := writeChunk(rctx, tgt, t, cols, []model.Row{r}); err != nil {
						res.FailRows++
						if len(res.Samples) < maxSamples {
							res.Samples = append(res.Samples, err.Error())
						}
					} else {
						res.OKRows++
					}
				}
			} else {
				res.OKRows += n
			}
			off = end
		}

		totalSeen += int64(len(page))
		if runID > 0 {
			_ = e.Runs.Event(runID, "progress", map[string]int64{
				"seen": totalSeen, "ok": res.OKRows, "fail": res.FailRows,
			})
		}
		return nil
	})
	res.TotalRows = totalSeen
	if err != nil {
		res.Status = model.RunStatusFailed
		res.Err = err.Error()
		return res
	}
	res.Status = model.RunStatusSucceeded
	if wm != nil {
		e.finalizeWatermark(t, runID, res, wm)
	}
	return res
}

// finalizeWatermark advances the stored watermark only after a clean run:
// with failed rows, holding the previous watermark makes the next run
// re-read the same window, which idempotent upserts absorb safely.
// A failed persist is logged, not fatal, for the same reason.
func (e *Engine) finalizeWatermark(t config.Task, runID int64, res *model.RunResult, wm *watermarkTracker) {
	switch {
	case res.FailRows > 0:
		e.Log.Warn("watermark held back: run had failed rows", "task", t.Name, "failed", res.FailRows)
		if runID > 0 {
			_ = e.Runs.Event(runID, "watermark_held", map[string]any{"reason": "failed rows", "count": res.FailRows})
		}
	case wm.max == nil:
		// no non-NULL values this run: keep the previous watermark
	default:
		if err := e.Runs.SetWatermark(t.Name, wm.max); err != nil {
			e.Log.Warn("watermark persist failed; next run re-reads the same window", "task", t.Name, "err", err)
			return
		}
		e.Log.Info("watermark advanced", "task", t.Name, "value", wm.max)
		if runID > 0 {
			_ = e.Runs.Event(runID, "watermark_advanced", map[string]any{"value": fmt.Sprintf("%v", wm.max)})
		}
	}
}

func writeChunk(ctx context.Context, tgt connector.TargetConn, t config.Task, cols []string, chunk []model.Row) (int64, error) {
	if t.Target.Mode == "insert" {
		return tgt.InsertRows(ctx, t.Target.Table, cols, chunk)
	}
	return tgt.UpsertRows(ctx, t.Target.Table, t.Target.Keys, cols, chunk)
}

// clampBatch keeps rows*cols below SQL Server's 2100-parameter ceiling;
// harmless for the other dialects.
func clampBatch(batch, cols int) int {
	const paramLimit = 2000
	if cols <= 0 {
		return 1
	}
	if m := paramLimit / cols; m < batch {
		return max(m, 1)
	}
	return batch
}

func sortedCols(r model.Row) []string {
	cols := make([]string, 0, len(r))
	for c := range r {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	return cols
}
