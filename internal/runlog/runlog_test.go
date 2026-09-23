package runlog

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/Cikouyanqu/replicron/internal/model"
)

func TestRunRoundtrip(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "runs.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	id, err := s.StartRun("demo", "manual")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Event(id, "progress", map[string]int64{"ok": 5}); err != nil {
		t.Fatal(err)
	}
	res := model.RunResult{
		Task: "demo", Trigger: "manual", Status: model.RunStatusSucceeded,
		TotalRows: 10, OKRows: 9, FailRows: 1,
		Samples: []string{"row 7: synthetic failure"},
	}
	if err := s.FinishRun(id, res); err != nil {
		t.Fatal(err)
	}

	runs, err := s.List("demo", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 {
		t.Fatalf("want 1 run, got %d", len(runs))
	}
	r := runs[0]
	if r.Status != model.RunStatusSucceeded || r.OKRows != 9 || r.FailRows != 1 {
		t.Errorf("unexpected run: %+v", r)
	}
	if len(r.Samples) != 1 || r.Samples[0] != "row 7: synthetic failure" {
		t.Errorf("samples not persisted: %v", r.Samples)
	}
	if r.StartedAt.IsZero() {
		t.Error("started_at not parsed")
	}
	if r.FinishedAt == nil || r.FinishedAt.Before(r.StartedAt) {
		t.Errorf("finished_at invalid: %v -> %v", r.StartedAt, r.FinishedAt)
	}

	n, err := s.EventsCount(id)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("want 1 event, got %d", n)
	}
}

// TestSweepInterrupted is the design-level fix for orphan "running" rows:
// reopening a store must close out runs left running by a crashed process.
func TestSweepInterrupted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.db")

	s1, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s1.StartRun("demo", "cron"); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash: close without finishing, then reopen.
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()
	runs, err := s2.List("", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 || runs[0].Status != model.RunStatusInterrupted {
		t.Fatalf("orphan running row not swept: %+v", runs)
	}
	if runs[0].FinishedAt == nil {
		t.Error("swept run should have finished_at set")
	}
}

func TestPrune(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "runs.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	for i := 0; i < 5; i++ {
		id, err := s.StartRun("demo", "manual")
		if err != nil {
			t.Fatal(err)
		}
		if err := s.FinishRun(id, model.RunResult{Status: model.RunStatusSucceeded}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Prune(2); err != nil {
		t.Fatal(err)
	}
	runs, err := s.List("", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 {
		t.Fatalf("want 2 runs after prune, got %d", len(runs))
	}
	// keep the newest
	if runs[0].ID <= runs[1].ID {
		t.Errorf("prune should keep newest runs, got ids %d,%d", runs[0].ID, runs[1].ID)
	}
}

func TestWatermarkRoundtrip(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "runs.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	if wm, err := s.GetWatermark("missing"); err != nil || wm != nil {
		t.Errorf("missing task: wm=%v err=%v, want nil/nil", wm, err)
	}

	tm := time.Date(2026, 9, 23, 1, 2, 3, 500000000, time.UTC)
	if err := s.SetWatermark("a", tm); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetWatermark("a")
	if err != nil {
		t.Fatal(err)
	}
	if g, ok := got.(time.Time); !ok || !g.Equal(tm) {
		t.Errorf("time watermark = %v, want %v", got, tm)
	}

	if err := s.SetWatermark("a", int64(7)); err != nil {
		t.Fatal(err)
	}
	if got, _ = s.GetWatermark("a"); got != int64(7) {
		t.Errorf("int watermark = %v (%T), want 7", got, got)
	}

	if err := s.SetWatermark("a", "it's a string"); err != nil {
		t.Fatal(err)
	}
	if got, _ = s.GetWatermark("a"); got != "it's a string" {
		t.Errorf("string watermark = %v", got)
	}

	if err := s.SetWatermark("a", true); err == nil {
		t.Error("bool watermark should be rejected")
	}
}
