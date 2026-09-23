package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "replicron.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

const validYAML = `
tasks:
  - name: demo
    schedule: "0 */5 * * * *"
    timeout: 15m
    source:
      type: sqlite
      dsn: "file:src.db"
      query: "SELECT id, name FROM users"
    target:
      type: postgres
      dsn_env: DEMO_TARGET_DSN
      table: public.users
      mode: upsert
      keys: [id]
      batch_size: 100
`

func TestLoadValid(t *testing.T) {
	t.Setenv("DEMO_TARGET_DSN", "postgres://u:p@localhost:5432/db")
	cfg, err := Load(writeConfig(t, validYAML))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.Tasks) != 1 {
		t.Fatalf("want 1 task, got %d", len(cfg.Tasks))
	}
	task := cfg.Tasks[0]
	if task.TimeoutDur != 15*time.Minute {
		t.Errorf("TimeoutDur = %v, want 15m", task.TimeoutDur)
	}
	if !task.IsEnabled {
		t.Error("IsEnabled should default to true")
	}
	if task.PageSize != defaultPageSize {
		t.Errorf("PageSize = %d, want default %d", task.PageSize, defaultPageSize)
	}
	if task.Target.BatchSize != 100 {
		t.Errorf("BatchSize = %d, want 100", task.Target.BatchSize)
	}
	if task.Target.DSN != "postgres://u:p@localhost:5432/db" {
		t.Errorf("target DSN not resolved from env: %q", task.Target.DSN)
	}
}

func TestDefaults(t *testing.T) {
	yaml := `
tasks:
  - name: bare
    source: {type: sqlite, dsn: "a.db", query: "SELECT 1 AS id"}
    target: {type: sqlite, dsn: "b.db", table: t, keys: [id]}
`
	cfg, err := Load(writeConfig(t, yaml))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	task := cfg.Tasks[0]
	if task.TimeoutDur != defaultTimeout {
		t.Errorf("TimeoutDur = %v, want %v", task.TimeoutDur, defaultTimeout)
	}
	if task.Target.Mode != "upsert" {
		t.Errorf("Mode = %q, want upsert", task.Target.Mode)
	}
	if task.PageSize != defaultPageSize || task.Target.BatchSize != defaultBatchSize {
		t.Errorf("defaults not applied: page=%d batch=%d", task.PageSize, task.Target.BatchSize)
	}
}

func TestValidationErrors(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{"missing name", `
tasks:
  - source: {type: sqlite, dsn: a, query: q}
    target: {type: sqlite, dsn: b, table: t}
`, "name is required"},
		{"bad source type", `
tasks:
  - name: t
    source: {type: oracle, dsn: a, query: q}
    target: {type: sqlite, dsn: b, table: t, keys: [id]}
`, "not supported"},
		{"missing query", `
tasks:
  - name: t
    source: {type: sqlite, dsn: a}
    target: {type: sqlite, dsn: b, table: t, keys: [id]}
`, "source.query is required"},
		{"upsert needs keys", `
tasks:
  - name: t
    source: {type: sqlite, dsn: a, query: q}
    target: {type: sqlite, dsn: b, table: t}
`, "keys is required"},
		{"bad mode", `
tasks:
  - name: t
    source: {type: sqlite, dsn: a, query: q}
    target: {type: sqlite, dsn: b, table: t, mode: replace}
`, "mode must be"},
		{"bad schedule", `
tasks:
  - name: t
    schedule: "not a cron"
    source: {type: sqlite, dsn: a, query: q}
    target: {type: sqlite, dsn: b, table: t, keys: [id]}
`, "invalid schedule"},
		{"bad timeout", `
tasks:
  - name: t
    timeout: 5x
    source: {type: sqlite, dsn: a, query: q}
    target: {type: sqlite, dsn: b, table: t, keys: [id]}
`, "invalid timeout"},
		{"missing dsn", `
tasks:
  - name: t
    source: {type: sqlite, query: q}
    target: {type: sqlite, dsn: b, table: t, keys: [id]}
`, "dsn or dsn_env"},
		{"duplicate names", `
tasks:
  - name: dup
    source: {type: sqlite, dsn: a, query: q}
    target: {type: sqlite, dsn: b, table: t, keys: [id]}
  - name: dup
    source: {type: sqlite, dsn: a, query: q}
    target: {type: sqlite, dsn: b, table: t, keys: [id]}
`, "duplicate task name"},
		{"no tasks", "tasks: []\n", "no tasks defined"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, tc.yaml))
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.want)
			}
		})
	}
}

func TestUnsetEnvDSN(t *testing.T) {
	t.Setenv("DEMO_TARGET_DSN", "")
	yaml := `
tasks:
  - name: t
    source: {type: sqlite, dsn: a, query: q}
    target: {type: postgres, dsn_env: DEMO_TARGET_DSN, table: t, keys: [id]}
`
	_, err := Load(writeConfig(t, yaml))
	if err == nil || !strings.Contains(err.Error(), "environment variable DEMO_TARGET_DSN is empty") {
		t.Errorf("want empty env error, got %v", err)
	}
}
