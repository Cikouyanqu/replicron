// Package ui serves a minimal, read-only HTML view over the run log.
// It is deliberately dependency-free: server-rendered html/template with a
// meta-refresh on the index and no client-side JavaScript. It exposes no
// mutations; put auth in front of it if the daemon port is reachable beyond
// localhost.
package ui

import (
	_ "embed"
	"html/template"
	"net/http"
	"strconv"
	"time"

	"github.com/Cikouyanqu/replicron/internal/config"
	"github.com/Cikouyanqu/replicron/internal/model"
	"github.com/Cikouyanqu/replicron/internal/runlog"
)

//go:embed templates.html
var templatesHTML string

type UI struct {
	store *runlog.Store
	cfg   *config.Config
	tmpl  *template.Template
}

// New parses the embedded templates.
func New(store *runlog.Store, cfg *config.Config) (*UI, error) {
	t, err := template.New("replicron").Parse(templatesHTML)
	if err != nil {
		return nil, err
	}
	return &UI{store: store, cfg: cfg, tmpl: t}, nil
}

// Register mounts the UI on /ui and /ui/run/{id}.
func (u *UI) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /ui", u.index)
	mux.HandleFunc("GET /ui/run/{id}", u.detail)
}

type taskView struct {
	Name     string
	Schedule string
	Enabled  bool
}

type runView struct {
	ID       int64
	Task     string
	Trigger  string
	Status   string
	OK       int64
	Fail     int64
	Started  string
	Duration string
	Err      string
}

type indexData struct {
	Tasks        []taskView
	Statuses     []string
	FilterTask   string
	FilterStatus string
	Runs         []runView
}

func (u *UI) index(w http.ResponseWriter, r *http.Request) {
	filterTask := r.URL.Query().Get("task")
	filterStatus := r.URL.Query().Get("status")

	tasks := make([]taskView, 0, len(u.cfg.Tasks))
	for _, t := range u.cfg.Tasks {
		sched := t.Schedule
		if sched == "" {
			sched = "manual"
		}
		tasks = append(tasks, taskView{Name: t.Name, Schedule: sched, Enabled: t.IsEnabled})
	}

	runs, err := u.store.List(filterTask, 100)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	views := make([]runView, 0, len(runs))
	for _, run := range runs {
		if filterStatus != "" && string(run.Status) != filterStatus {
			continue
		}
		started := run.StartedAt.UTC().Format("01-02 15:04:05")
		took := "—"
		if run.FinishedAt != nil {
			took = run.FinishedAt.Sub(run.StartedAt).Round(time.Millisecond).String()
		}
		errMsg := run.Error
		if len(errMsg) > 120 {
			errMsg = errMsg[:120] + "…"
		}
		views = append(views, runView{
			ID: run.ID, Task: run.Task, Trigger: run.Trigger, Status: string(run.Status),
			OK: run.OKRows, Fail: run.FailRows, Started: started, Duration: took, Err: errMsg,
		})
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = u.tmpl.ExecuteTemplate(w, "index", indexData{
		Tasks: tasks,
		Statuses: []string{
			string(model.RunStatusRunning), string(model.RunStatusSucceeded),
			string(model.RunStatusFailed), string(model.RunStatusSkipped),
			string(model.RunStatusInterrupted),
		},
		FilterTask:   filterTask,
		FilterStatus: filterStatus,
		Runs:         views,
	})
}

func (u *UI) detail(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		http.Error(w, "invalid run id", http.StatusBadRequest)
		return
	}
	run, err := u.store.GetRun(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if run == nil {
		http.Error(w, "run not found", http.StatusNotFound)
		return
	}
	events, err := u.store.ListEvents(id, 500)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = u.tmpl.ExecuteTemplate(w, "detail", struct {
		Run    *model.Run
		Events []runlog.RunEvent
	}{Run: run, Events: events})
}
