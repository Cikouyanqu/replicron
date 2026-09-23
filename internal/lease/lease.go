// Package lease implements a registry.Locker backed by a SQLite lease table,
// enabling mutual exclusion across multiple replicron instances that share
// the same run-log database file.
//
// Semantics mirror the in-process registry: acquire is atomic, release is
// owner-checked, and leases expire after a TTL so a crashed instance cannot
// block a task forever. Long runs renew per page via the Renewer extension;
// pick a TTL comfortably above the slowest expected page.
package lease

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	_ "modernc.org/sqlite" // registers database/sql driver "sqlite" (pure Go, no CGO)

	"github.com/Cikouyanqu/replicron/internal/registry"
)

// leaseTimeFormat is fixed-width UTC so expiry comparisons in SQL are plain
// string comparisons.
const leaseTimeFormat = "2006-01-02 15:04:05.000000000"

// opTimeout bounds each lease statement; lease operations must not inherit
// the run's context, which is cancelled on timeout exactly when release
// still needs to work.
const opTimeout = 5 * time.Second

type Lease struct {
	db    *sql.DB
	ttl   time.Duration
	owner string
}

// New opens (creating if needed) the lease table in the given SQLite file.
// The file is typically the same run-log database (--db).
func New(path string, ttl time.Duration) (*Lease, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS lease (
		name       TEXT PRIMARY KEY,
		owner      TEXT NOT NULL,
		acquired_at TEXT NOT NULL,
		expires_at TEXT NOT NULL
	)`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("lease: migrate: %w", err)
	}
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		_ = db.Close()
		return nil, err
	}
	host, _ := os.Hostname()
	owner := fmt.Sprintf("%s:%d:%s", host, os.Getpid(), hex.EncodeToString(buf))
	return &Lease{db: db, ttl: ttl, owner: owner}, nil
}

// Acquire takes the lease on a task name, first evicting expired rows.
// It satisfies registry.Locker.
func (l *Lease) Acquire(name string) (*registry.Token, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	now := time.Now().UTC()
	if _, err := l.db.ExecContext(ctx,
		`DELETE FROM lease WHERE expires_at < ?`, now.Format(leaseTimeFormat)); err != nil {
		return nil, false
	}
	res, err := l.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO lease (name, owner, acquired_at, expires_at) VALUES (?, ?, ?, ?)`,
		name, l.owner, now.Format(leaseTimeFormat), now.Add(l.ttl).Format(leaseTimeFormat))
	if err != nil {
		return nil, false
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		return nil, false
	}
	return &registry.Token{Name: name}, true
}

// Release drops the lease only if this instance still owns it.
// It satisfies registry.Locker.
func (l *Lease) Release(t *registry.Token) {
	if t == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	_, _ = l.db.ExecContext(ctx, `DELETE FROM lease WHERE name = ? AND owner = ?`, t.Name, l.owner)
}

// Renew extends the lease expiry while this instance still owns it. Failures
// are swallowed: the TTL is the backstop, and the next page retries.
// It satisfies registry.Renewer.
func (l *Lease) Renew(t *registry.Token) {
	if t == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	_, _ = l.db.ExecContext(ctx, `UPDATE lease SET expires_at = ? WHERE name = ? AND owner = ?`,
		time.Now().UTC().Add(l.ttl).Format(leaseTimeFormat), t.Name, l.owner)
}

func (l *Lease) Close() error { return l.db.Close() }
