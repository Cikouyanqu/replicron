// Package config loads and validates task definitions.
package config

import (
	"fmt"
	"os"
	"time"

	"github.com/robfig/cron/v3"
	"gopkg.in/yaml.v3"
)

const (
	defaultPageSize  = 5000
	defaultBatchSize = 500
	defaultTimeout   = 30 * time.Minute
)

// Endpoint describes a database connection. Prefer dsn_env so credentials
// never live in the task file; resolved DSNs are kept in memory only.
type Endpoint struct {
	Type   string `yaml:"type"`
	DSN    string `yaml:"dsn,omitempty"`
	DSNEnv string `yaml:"dsn_env,omitempty"`
	Query  string `yaml:"query,omitempty"` // source side only
}

// Target is the write side of a task.
type Target struct {
	Endpoint  `yaml:",inline"`
	Table     string   `yaml:"table"`
	Mode      string   `yaml:"mode"` // upsert (default) | insert
	Keys      []string `yaml:"keys"`
	BatchSize int      `yaml:"batch_size,omitempty"`
}

// Task is one replication unit.
type Task struct {
	Name     string   `yaml:"name"`
	Schedule string   `yaml:"schedule,omitempty"` // 6-field cron with seconds; empty = manual only
	Timeout  string   `yaml:"timeout,omitempty"`  // Go duration string, e.g. 30m
	Enabled  *bool    `yaml:"enabled,omitempty"`
	PageSize int      `yaml:"page_size,omitempty"`
	Source   Endpoint `yaml:"source"`
	Target   Target   `yaml:"target"`

	// Normalized fields derived by Validate; not part of the YAML surface.
	TimeoutDur time.Duration `yaml:"-"`
	IsEnabled  bool          `yaml:"-"`
}

type Config struct {
	Tasks []Task `yaml:"tasks"`
}

var supportedTypes = map[string]bool{
	"postgres": true, "mysql": true, "sqlite": true, "sqlserver": true,
}

var cronParser = cron.NewParser(cron.Second | cron.Minute | cron.Hour |
	cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// Load reads, parses and validates a configuration file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// Validate normalizes defaults and rejects invalid or ambiguous task
// definitions, mutating tasks in place.
func (c *Config) Validate() error {
	if len(c.Tasks) == 0 {
		return fmt.Errorf("no tasks defined")
	}
	seen := make(map[string]bool, len(c.Tasks))
	for i := range c.Tasks {
		t := &c.Tasks[i]
		if err := validateTask(t); err != nil {
			return fmt.Errorf("task[%d]: %w", i, err)
		}
		if seen[t.Name] {
			return fmt.Errorf("duplicate task name %q", t.Name)
		}
		seen[t.Name] = true
	}
	return nil
}

func validateTask(t *Task) error {
	if t.Name == "" {
		return fmt.Errorf("name is required")
	}

	if !supportedTypes[t.Source.Type] {
		return fmt.Errorf("source.type %q not supported (want postgres|mysql|sqlite|sqlserver)", t.Source.Type)
	}
	if t.Source.Query == "" {
		return fmt.Errorf("source.query is required")
	}
	dsn, err := resolveDSN(t.Source)
	if err != nil {
		return fmt.Errorf("source: %w", err)
	}
	t.Source.DSN = dsn

	if !supportedTypes[t.Target.Type] {
		return fmt.Errorf("target.type %q not supported", t.Target.Type)
	}
	if t.Target.Table == "" {
		return fmt.Errorf("target.table is required")
	}
	if t.Target.Mode == "" {
		t.Target.Mode = "upsert"
	}
	if t.Target.Mode != "upsert" && t.Target.Mode != "insert" {
		return fmt.Errorf("target.mode must be upsert or insert, got %q", t.Target.Mode)
	}
	if t.Target.Mode == "upsert" && len(t.Target.Keys) == 0 {
		return fmt.Errorf("target.keys is required for upsert mode")
	}
	dsn, err = resolveDSN(t.Target.Endpoint)
	if err != nil {
		return fmt.Errorf("target: %w", err)
	}
	t.Target.DSN = dsn

	if t.Schedule != "" {
		if _, err := cronParser.Parse(t.Schedule); err != nil {
			return fmt.Errorf("invalid schedule %q: %w", t.Schedule, err)
		}
	}

	switch {
	case t.Timeout == "":
		t.TimeoutDur = defaultTimeout
	default:
		d, err := time.ParseDuration(t.Timeout)
		if err != nil {
			return fmt.Errorf("invalid timeout %q: %w", t.Timeout, err)
		}
		if d <= 0 {
			return fmt.Errorf("timeout must be positive")
		}
		t.TimeoutDur = d
	}

	t.IsEnabled = t.Enabled == nil || *t.Enabled
	if t.PageSize <= 0 {
		t.PageSize = defaultPageSize
	}
	if t.Target.BatchSize <= 0 {
		t.Target.BatchSize = defaultBatchSize
	}
	return nil
}

func resolveDSN(ep Endpoint) (string, error) {
	switch {
	case ep.DSNEnv != "":
		v := os.Getenv(ep.DSNEnv)
		if v == "" {
			return "", fmt.Errorf("environment variable %s is empty", ep.DSNEnv)
		}
		return v, nil
	case ep.DSN != "":
		return ep.DSN, nil
	default:
		return "", fmt.Errorf("either dsn or dsn_env is required")
	}
}
