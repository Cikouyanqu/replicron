package ui

import (
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/Cikouyanqu/replicron/internal/config"
	"github.com/Cikouyanqu/replicron/internal/model"
	"github.com/Cikouyanqu/replicron/internal/runlog"
)

func newTestServer(t *testing.T, store *runlog.Store) *httptest.Server {
	t.Helper()
	cfg := &config.Config{Tasks: []config.Task{
		{Name: "demo-task", Schedule: "0 0 4 * * *"},
	}}
	h, err := New(store, cfg)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	h.Register(mux)
	return httptest.NewServer(mux)
}

func newStore(t *testing.T) *runlog.Store {
	t.Helper()
	store, err := runlog.Open(filepath.Join(t.TempDir(), "runs.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(b)
}

func TestIndexListsRunsAndTasks(t *testing.T) {
	store := newStore(t)
	id, err := store.StartRun("demo-task", "cron")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.FinishRun(id, model.RunResult{
		Task: "demo-task", Trigger: "cron", Status: model.RunStatusSucceeded,
		TotalRows: 5, OKRows: 5,
	}); err != nil {
		t.Fatal(err)
	}
	srv := newTestServer(t, store)
	defer srv.Close()

	code, body := get(t, srv.URL+"/ui")
	if code != http.StatusOK {
		t.Fatalf("status = %d", code)
	}
	for _, want := range []string{"demo-task", "succeeded", "demo-task · 0 0 4 * * *", `http-equiv="refresh"`} {
		if !strings.Contains(body, want) {
			t.Errorf("index missing %q:\n%s", want, body)
		}
	}
}

func TestIndexStatusFilter(t *testing.T) {
	store := newStore(t)
	id, _ := store.StartRun("demo-task", "cron")
	_ = store.FinishRun(id, model.RunResult{Status: model.RunStatusSucceeded})
	id2, _ := store.StartRun("demo-task", "cron")
	_ = store.FinishRun(id2, model.RunResult{Status: model.RunStatusFailed})
	srv := newTestServer(t, store)
	defer srv.Close()

	_, body := get(t, srv.URL+"/ui?status=failed")
	if strings.Contains(body, "succeeded</span>") {
		t.Error("status filter did not hide succeeded runs")
	}
	if !strings.Contains(body, "failed</span>") {
		t.Error("filtered run missing")
	}
}

func TestDetailShowsEventsAndSamples(t *testing.T) {
	store := newStore(t)
	id, _ := store.StartRun("demo-task", "cron")
	_ = store.Event(id, "watermark_advanced", map[string]any{"value": "2026-09-23"})
	_ = store.FinishRun(id, model.RunResult{
		Status: model.RunStatusSucceeded, FailRows: 1,
		Samples: []string{"row 3: constraint violation"},
	})
	srv := newTestServer(t, store)
	defer srv.Close()

	code, body := get(t, srv.URL+"/ui/run/"+strconv.FormatInt(id, 10))
	if code != http.StatusOK {
		t.Fatalf("status = %d", code)
	}
	for _, want := range []string{"watermark_advanced", "constraint violation", "Failure samples"} {
		if !strings.Contains(body, want) {
			t.Errorf("detail missing %q:\n%s", want, body)
		}
	}
}

func TestDetailNotFoundAndBadID(t *testing.T) {
	srv := newTestServer(t, newStore(t))
	defer srv.Close()
	if code, _ := get(t, srv.URL+"/ui/run/999"); code != http.StatusNotFound {
		t.Errorf("missing run = %d, want 404", code)
	}
	if code, _ := get(t, srv.URL+"/ui/run/abc"); code != http.StatusBadRequest {
		t.Errorf("bad id = %d, want 400", code)
	}
}

func TestXSSIsEscaped(t *testing.T) {
	store := newStore(t)
	id, _ := store.StartRun("<script>alert(1)</script>", "manual")
	_ = store.FinishRun(id, model.RunResult{Status: model.RunStatusFailed})
	srv := newTestServer(t, store)
	defer srv.Close()

	_, body := get(t, srv.URL+"/ui")
	if strings.Contains(body, "<script>alert(1)</script>") {
		t.Error("task name was not HTML-escaped")
	}
	if !strings.Contains(body, "&lt;script&gt;") {
		t.Error("escaped task name missing")
	}
}
