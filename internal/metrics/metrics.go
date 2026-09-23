// Package metrics implements the engine's observability hook with Prometheus.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Cikouyanqu/replicron/internal/model"
)

type M struct {
	reg  *prometheus.Registry
	runs *prometheus.CounterVec
	rows *prometheus.CounterVec
	dur  *prometheus.HistogramVec
}

// New builds a self-contained registry (no global state, safe for tests).
func New() *M {
	reg := prometheus.NewRegistry()
	m := &M{
		reg: reg,
		runs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "replicron",
			Name:      "runs_total",
			Help:      "Total task runs by status.",
		}, []string{"task", "status"}),
		rows: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "replicron",
			Name:      "rows_total",
			Help:      "Rows written by outcome.",
		}, []string{"task", "outcome"}),
		dur: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "replicron",
			Name:      "run_duration_seconds",
			Help:      "Task run duration in seconds.",
			Buckets:   prometheus.ExponentialBuckets(0.1, 2, 16),
		}, []string{"task"}),
	}
	reg.MustRegister(m.runs, m.rows, m.dur)
	return m
}

// ObserveRun implements engine.Metrics.
func (m *M) ObserveRun(task string, status model.RunStatus, ok, fail int64, d time.Duration) {
	m.runs.WithLabelValues(task, string(status)).Inc()
	if ok > 0 {
		m.rows.WithLabelValues(task, "ok").Add(float64(ok))
	}
	if fail > 0 {
		m.rows.WithLabelValues(task, "fail").Add(float64(fail))
	}
	m.dur.WithLabelValues(task).Observe(d.Seconds())
}

// Handler serves the /metrics endpoint.
func (m *M) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}
