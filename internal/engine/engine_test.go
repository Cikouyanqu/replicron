package engine

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Cikouyanqu/replicron/internal/config"
	"github.com/Cikouyanqu/replicron/internal/connector"
	"github.com/Cikouyanqu/replicron/internal/model"
	"github.com/Cikouyanqu/replicron/internal/runlog"
)

// ---- fakes ----

type fakeSource struct {
	pages [][]model.Row
	block chan struct{} // non-nil: wait for it or ctx cancellation per page
}

func (f *fakeSource) Connect(context.Context) error { return nil }
func (f *fakeSource) Close() error                  { return nil }

func (f *fakeSource) Stream(ctx context.Context, _ string, _ int, fn func([]model.Row) error) (int64, error) {
	var total int64
	for _, p := range f.pages {
		if f.block != nil {
			select {
			case <-f.block:
			case <-ctx.Done():
				return total, ctx.Err()
			}
		}
		if err := fn(p); err != nil {
			return total, err
		}
		total += int64(len(p))
	}
	return total, nil
}

type fakeTarget struct {
	mu              sync.Mutex
	upsertCalls     int
	insertCalls     int
	failBatchMarker string // batches containing this string value fail as a batch
	failRowMarker   string // individual rows containing this value always fail
}

func (f *fakeTarget) Connect(context.Context) error { return nil }
func (f *fakeTarget) Close() error                  { return nil }

func (f *fakeTarget) hasMarker(rows []model.Row, marker string) bool {
	if marker == "" {
		return false
	}
	for _, r := range rows {
		for _, v := range r {
			if s, ok := v.(string); ok && s == marker {
				return true
			}
		}
	}
	return false
}

func (f *fakeTarget) UpsertRows(_ context.Context, table string, _, _ []string, rows []model.Row) (int64, error) {
	f.mu.Lock()
	f.upsertCalls++
	f.mu.Unlock()
	if f.hasMarker(rows, f.failBatchMarker) || f.hasMarker(rows, f.failRowMarker) {
		return 0, errors.New("synthetic write failure")
	}
	return int64(len(rows)), nil
}

func (f *fakeTarget) InsertRows(_ context.Context, table string, _ []string, rows []model.Row) (int64, error) {
	f.mu.Lock()
	f.insertCalls++
	f.mu.Unlock()
	if f.hasMarker(rows, f.failRowMarker) {
		return 0, errors.New("synthetic write failure")
	}
	return int64(len(rows)), nil
}

// ---- helpers ----

func newTestEngine(t *testing.T, src connector.SourceConn, tgt connector.TargetConn) (*Engine, *runlog.Store) {
	t.Helper()
	store, err := runlog.Open(filepath.Join(t.TempDir(), "runs.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	e := New(store, NoopMetrics{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	e.OpenSource = func(context.Context, string, string) (connector.SourceConn, error) { return src, nil }
	e.OpenTarget = func(context.Context, string, string) (connector.TargetConn, error) { return tgt, nil }
	return e, store
}

func testTask() config.Task {
	return config.Task{
		Name:       "demo",
		TimeoutDur: 5 * time.Second,
		PageSize:   10,
		IsEnabled:  true,
		Source:     config.Endpoint{Type: "sqlite", DSN: "src", Query: "SELECT id, name FROM src"},
		Target: config.Target{
			Endpoint:  config.Endpoint{Type: "sqlite", DSN: "dst"},
			Table:     "dst",
			Mode:      "upsert",
			Keys:      []string{"id"},
			BatchSize: 2,
		},
	}
}

// ---- tests ----

func TestHappyPath(t *testing.T) {
	src := &fakeSource{pages: [][]model.Row{
		{{"id": 1, "name": "a"}, {"id": 2, "name": "b"}, {"id": 3, "name": "c"}},
	}}
	tgt := &fakeTarget{}
	e, store := newTestEngine(t, src, tgt)

	res := e.RunTask(context.Background(), testTask(), "manual")
	if res.Status != model.RunStatusSucceeded {
		t.Fatalf("status = %s, err = %s", res.Status, res.Err)
	}
	if res.OKRows != 3 || res.FailRows != 0 || res.TotalRows != 3 {
		t.Errorf("counts wrong: %+v", res)
	}
	// batch size 2 over 3 rows -> at least 2 batch calls
	if tgt.upsertCalls < 2 {
		t.Errorf("expected >=2 upsert calls, got %d", tgt.upsertCalls)
	}
	runs, _ := store.List("demo", 5)
	if len(runs) != 1 || runs[0].Status != model.RunStatusSucceeded {
		t.Fatalf("run not persisted correctly: %+v", runs)
	}
}

func TestEmptySourceSucceeds(t *testing.T) {
	// Empty sources must be a clean success, not "skipped".
	e, _ := newTestEngine(t, &fakeSource{}, &fakeTarget{})
	res := e.RunTask(context.Background(), testTask(), "manual")
	if res.Status != model.RunStatusSucceeded || res.TotalRows != 0 {
		t.Errorf("empty source: %+v", res)
	}
}

func TestBatchFallbackRowByRow(t *testing.T) {
	src := &fakeSource{pages: [][]model.Row{
		{{"id": 1, "name": "fine"}, {"id": 2, "name": "bad"}, {"id": 3, "name": "fine"}},
	}}
	tgt := &fakeTarget{failBatchMarker: "bad", failRowMarker: "bad"}
	e, _ := newTestEngine(t, src, tgt)

	res := e.RunTask(context.Background(), testTask(), "manual")
	if res.Status != model.RunStatusSucceeded {
		t.Fatalf("status = %s, err = %s", res.Status, res.Err)
	}
	if res.OKRows != 2 || res.FailRows != 1 {
		t.Errorf("counts wrong: %+v", res)
	}
	if len(res.Samples) != 1 {
		t.Errorf("want 1 failure sample, got %v", res.Samples)
	}
}

func TestSkippedWhenLeaseHeld(t *testing.T) {
	e, store := newTestEngine(t, &fakeSource{}, &fakeTarget{})
	if _, ok := e.Reg.Acquire("demo"); !ok {
		t.Fatal("pre-acquire failed")
	}
	res := e.RunTask(context.Background(), testTask(), "cron")
	if res.Status != model.RunStatusSkipped {
		t.Fatalf("status = %s, want skipped", res.Status)
	}
	runs, _ := store.List("demo", 5)
	if len(runs) != 1 || runs[0].Status != model.RunStatusSkipped {
		t.Fatalf("skipped run not persisted: %+v", runs)
	}
}

func TestMissingKeyFails(t *testing.T) {
	src := &fakeSource{pages: [][]model.Row{{{"id": 1, "name": "a"}}}}
	e, _ := newTestEngine(t, src, &fakeTarget{})
	task := testTask()
	task.Target.Keys = []string{"missing_key"}
	res := e.RunTask(context.Background(), task, "manual")
	if res.Status != model.RunStatusFailed {
		t.Fatalf("status = %s, want failed", res.Status)
	}
	if want := `upsert key "missing_key" missing`; !strings.Contains(res.Err, want) {
		t.Errorf("err = %q, want substring %q", res.Err, want)
	}
}

func TestBadColumnNameFails(t *testing.T) {
	src := &fakeSource{pages: [][]model.Row{{{"id": 1, "na me": "a"}}}}
	e, _ := newTestEngine(t, src, &fakeTarget{})
	res := e.RunTask(context.Background(), testTask(), "manual")
	if res.Status != model.RunStatusFailed {
		t.Fatalf("status = %s, want failed (column allowlist)", res.Status)
	}
}

func TestTimeout(t *testing.T) {
	src := &fakeSource{pages: [][]model.Row{{{"id": 1}}}, block: make(chan struct{})}
	e, _ := newTestEngine(t, src, &fakeTarget{})
	task := testTask()
	task.TimeoutDur = 100 * time.Millisecond
	start := time.Now()
	res := e.RunTask(context.Background(), task, "manual")
	if res.Status != model.RunStatusFailed {
		t.Fatalf("status = %s, want failed", res.Status)
	}
	if !strings.Contains(res.Err, "context deadline exceeded") {
		t.Errorf("err = %q, want deadline error", res.Err)
	}
	if time.Since(start) > 2*time.Second {
		t.Error("run did not respect timeout")
	}
}

func TestInsertModeUsesInsertRows(t *testing.T) {
	src := &fakeSource{pages: [][]model.Row{{{"id": 1, "name": "a"}}}}
	tgt := &fakeTarget{}
	e, _ := newTestEngine(t, src, tgt)
	task := testTask()
	task.Target.Mode = "insert"
	res := e.RunTask(context.Background(), task, "manual")
	if res.Status != model.RunStatusSucceeded {
		t.Fatalf("status = %s, err = %s", res.Status, res.Err)
	}
	if tgt.insertCalls == 0 || tgt.upsertCalls != 0 {
		t.Errorf("insert=%d upsert=%d, want insert path only", tgt.insertCalls, tgt.upsertCalls)
	}
}
