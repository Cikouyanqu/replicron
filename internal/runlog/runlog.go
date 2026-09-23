// Package runlog persists task runs in an append-only SQLite store.
//
// The run record is created with status "running" and updated once at
// finalization; progress details live in the append-only run_events table.
// Opening a store sweeps any rows left "running" by a crashed or restarted
// process and marks them "interrupted", so stale running states cannot
// accumulate.
package runlog

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	_ "modernc.org/sqlite" // registers database/sql driver "sqlite" (pure Go, no CGO)

	"github.com/Cikouyanqu/replicron/internal/model"
)

type Store struct {
	db *sql.DB
}

func nowUTC() string { return time.Now().UTC().Format(time.RFC3339) }

// Open opens (creating if needed) the run log database and applies
// migrations plus the interrupted-run sweep.
func Open(path string) (*Store, error) {
	dsn := path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := s.sweepInterrupted(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) migrate() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS runs (
			id           INTEGER PRIMARY KEY AUTOINCREMENT,
			task         TEXT NOT NULL,
			trigger_kind TEXT NOT NULL DEFAULT 'manual',
			status      TEXT NOT NULL,
			total_rows  INTEGER NOT NULL DEFAULT 0,
			ok_rows     INTEGER NOT NULL DEFAULT 0,
			fail_rows   INTEGER NOT NULL DEFAULT 0,
			error       TEXT,
			samples     TEXT,
			started_at  TEXT NOT NULL,
			finished_at TEXT
		)`,
		`CREATE INDEX IF NOT EXISTS idx_runs_task_id ON runs(task, id DESC)`,
		`CREATE TABLE IF NOT EXISTS run_events (
			id      INTEGER PRIMARY KEY AUTOINCREMENT,
			run_id  INTEGER NOT NULL,
			kind    TEXT NOT NULL,
			payload TEXT,
			ts      TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_events_run ON run_events(run_id)`,
		`CREATE TABLE IF NOT EXISTS task_state (
			task  TEXT PRIMARY KEY,
			kind  TEXT NOT NULL,
			value TEXT NOT NULL
		)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	return nil
}

// sweepInterrupted closes out runs left "running" by a previous process.
func (s *Store) sweepInterrupted() error {
	_, err := s.db.Exec(
		`UPDATE runs
		   SET status = ?, finished_at = ?,
		       error = COALESCE(NULLIF(error, ''), 'run interrupted by process restart')
		 WHERE status = ?`,
		model.RunStatusInterrupted, nowUTC(), model.RunStatusRunning)
	if err != nil {
		return fmt.Errorf("sweep interrupted runs: %w", err)
	}
	return nil
}

// StartRun appends a new running record and returns its id.
func (s *Store) StartRun(task, trigger string) (int64, error) {
	res, err := s.db.Exec(
		`INSERT INTO runs (task, trigger_kind, status, started_at) VALUES (?, ?, ?, ?)`,
		task, trigger, model.RunStatusRunning, nowUTC())
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// FinishRun finalizes a run record from the result.
func (s *Store) FinishRun(id int64, r model.RunResult) error {
	samples := "[]"
	if len(r.Samples) > 0 {
		if b, err := json.Marshal(r.Samples); err == nil {
			samples = string(b)
		}
	}
	_, err := s.db.Exec(
		`UPDATE runs
		   SET status = ?, total_rows = ?, ok_rows = ?, fail_rows = ?,
		       error = ?, samples = ?, finished_at = ?
		 WHERE id = ?`,
		string(r.Status), r.TotalRows, r.OKRows, r.FailRows,
		r.Err, samples, nowUTC(), id)
	return err
}

// Event appends a progress/diagnostic event for a run.
func (s *Store) Event(runID int64, kind string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		b = []byte("{}")
	}
	_, err = s.db.Exec(
		`INSERT INTO run_events (run_id, kind, payload, ts) VALUES (?, ?, ?, ?)`,
		runID, kind, string(b), nowUTC())
	return err
}

// EventsCount returns the number of events recorded for a run.
func (s *Store) EventsCount(runID int64) (int, error) {
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM run_events WHERE run_id = ?`, runID).Scan(&n)
	return n, err
}

// RunEvent is one append-only progress/diagnostic record of a run.
type RunEvent struct {
	ID      int64
	Kind    string
	Payload string
	TS      time.Time
}

// ListEvents returns events for a run, oldest first.
func (s *Store) ListEvents(runID int64, limit int) ([]RunEvent, error) {
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.db.Query(
		`SELECT id, kind, payload, ts FROM run_events WHERE run_id = ? ORDER BY id LIMIT ?`,
		runID, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []RunEvent
	for rows.Next() {
		var e RunEvent
		var payload, ts sql.NullString
		if err := rows.Scan(&e.ID, &e.Kind, &payload, &ts); err != nil {
			return nil, err
		}
		e.Payload = payload.String
		e.TS, _ = time.Parse(time.RFC3339, ts.String)
		out = append(out, e)
	}
	return out, rows.Err()
}

// rowScanner is satisfied by both *sql.Row and *sql.Rows.
type rowScanner interface{ Scan(dest ...any) error }

func scanRun(row rowScanner) (*model.Run, error) {
	var r model.Run
	var errStr, samples, started, finished sql.NullString
	if err := row.Scan(&r.ID, &r.Task, &r.Trigger, &r.Status, &r.TotalRows,
		&r.OKRows, &r.FailRows, &errStr, &samples, &started, &finished); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	r.Error = errStr.String
	if samples.Valid && samples.String != "" {
		_ = json.Unmarshal([]byte(samples.String), &r.Samples)
	}
	r.StartedAt, _ = time.Parse(time.RFC3339, started.String)
	if finished.Valid && finished.String != "" {
		if t, err := time.Parse(time.RFC3339, finished.String); err == nil {
			r.FinishedAt = &t
		}
	}
	return &r, nil
}

// GetRun returns a single run by id, or nil when it does not exist.
func (s *Store) GetRun(id int64) (*model.Run, error) {
	row := s.db.QueryRow(`SELECT id, task, trigger_kind, status, total_rows, ok_rows, fail_rows,
	             error, samples, started_at, finished_at FROM runs WHERE id = ?`, id)
	return scanRun(row)
}

// List returns the most recent runs, optionally filtered by task.
func (s *Store) List(task string, n int) ([]model.Run, error) {
	if n <= 0 {
		n = 20
	}
	q := `SELECT id, task, trigger_kind, status, total_rows, ok_rows, fail_rows,
	             error, samples, started_at, finished_at
	        FROM runs`
	var args []any
	if task != "" {
		q += ` WHERE task = ?`
		args = append(args, task)
	}
	q += ` ORDER BY id DESC LIMIT ?`
	args = append(args, n)

	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []model.Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *r)
	}
	return out, rows.Err()
}

// Prune keeps only the most recent `keep` runs (and their events).
func (s *Store) Prune(keep int) error {
	if keep <= 0 {
		keep = 10000
	}
	if _, err := s.db.Exec(
		`DELETE FROM run_events WHERE run_id IN
		  (SELECT id FROM runs ORDER BY id DESC LIMIT -1 OFFSET ?)`, keep); err != nil {
		return err
	}
	_, err := s.db.Exec(
		`DELETE FROM runs WHERE id NOT IN
		  (SELECT id FROM runs ORDER BY id DESC LIMIT ?)`, keep)
	return err
}

// GetWatermark returns the tracked incremental watermark for a task, or nil
// when the task has never completed an incremental run.
func (s *Store) GetWatermark(task string) (any, error) {
	var kind, raw string
	err := s.db.QueryRow(`SELECT kind, value FROM task_state WHERE task = ?`, task).Scan(&kind, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return decodeWatermark(kind, raw)
}

// SetWatermark persists the incremental watermark for a task.
func (s *Store) SetWatermark(task string, v any) error {
	kind, raw, err := encodeWatermark(v)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`INSERT INTO task_state (task, kind, value) VALUES (?, ?, ?)
		 ON CONFLICT(task) DO UPDATE SET kind = excluded.kind, value = excluded.value`,
		task, kind, raw)
	return err
}

func encodeWatermark(v any) (kind, raw string, err error) {
	switch x := v.(type) {
	case time.Time:
		return "time", x.UTC().Format(time.RFC3339Nano), nil
	case int64:
		return "int", strconv.FormatInt(x, 10), nil
	case int:
		return "int", strconv.Itoa(x), nil
	case float64:
		return "float", strconv.FormatFloat(x, 'g', -1, 64), nil
	case string:
		return "string", x, nil
	default:
		return "", "", fmt.Errorf("unsupported watermark value type %T", v)
	}
}

func decodeWatermark(kind, raw string) (any, error) {
	switch kind {
	case "time":
		return time.Parse(time.RFC3339Nano, raw)
	case "int":
		return strconv.ParseInt(raw, 10, 64)
	case "float":
		return strconv.ParseFloat(raw, 64)
	case "string":
		return raw, nil
	default:
		return nil, fmt.Errorf("unknown watermark kind %q", kind)
	}
}

func (s *Store) Close() error { return s.db.Close() }
