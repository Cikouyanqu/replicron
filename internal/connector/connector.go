// Package connector provides database dialect adapters.
//
// Security invariants for every statement this package emits:
//   - identifiers (table, schema, column, key names) pass an allowlist
//     regex AND dialect-specific quoting before interpolation;
//   - all data values are bound parameters, never interpolated.
package connector

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/Cikouyanqu/replicron/internal/model"
)

var identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// SplitTablePath validates and splits a possibly schema-qualified table name
// ("schema.table" or "table").
func SplitTablePath(table string) ([]string, error) {
	parts := strings.Split(table, ".")
	if len(parts) > 2 {
		return nil, fmt.Errorf("invalid table %q: at most one '.' allowed", table)
	}
	for _, p := range parts {
		if !identRe.MatchString(p) {
			return nil, fmt.Errorf("invalid identifier %q in table %q", p, table)
		}
	}
	return parts, nil
}

// ValidateColumns enforces the identifier allowlist on a column set.
func ValidateColumns(cols []string) error {
	for _, c := range cols {
		if !identRe.MatchString(c) {
			return fmt.Errorf("invalid column name %q", c)
		}
	}
	return nil
}

// SourceConn is the read side of a sync: open, stream, close.
type SourceConn interface {
	Connect(ctx context.Context) error
	Close() error
	// Stream executes the query and hands pages of rows to fn. Returning an
	// error from fn aborts the stream. Pages are only valid for the duration
	// of the call.
	Stream(ctx context.Context, query string, pageSize int, fn func([]model.Row) error) (int64, error)
}

// TargetConn is the write side of a sync.
type TargetConn interface {
	Connect(ctx context.Context) error
	Close() error
	UpsertRows(ctx context.Context, table string, keys, cols []string, rows []model.Row) (int64, error)
	InsertRows(ctx context.Context, table string, cols []string, rows []model.Row) (int64, error)
}

// OpenSource opens and pings a source connection.
func OpenSource(ctx context.Context, typ, dsn string) (SourceConn, error) {
	c, err := open(typ, dsn)
	if err != nil {
		return nil, err
	}
	if err := c.Connect(ctx); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

// OpenTarget opens and pings a target connection.
func OpenTarget(ctx context.Context, typ, dsn string) (TargetConn, error) {
	c, err := open(typ, dsn)
	if err != nil {
		return nil, err
	}
	if err := c.Connect(ctx); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

func open(typ, dsn string) (*sqlConn, error) {
	switch typ {
	case "postgres":
		return &sqlConn{dialect: pgDialect, dsn: dsn}, nil
	case "mysql":
		return &sqlConn{dialect: mysqlDialect, dsn: dsn}, nil
	case "sqlite":
		return &sqlConn{dialect: sqliteDialect, dsn: dsn}, nil
	case "sqlserver":
		return &sqlConn{dialect: mssqlDialect, dsn: dsn}, nil
	default:
		return nil, fmt.Errorf("unsupported database type %q", typ)
	}
}

type dialect struct {
	name   string
	driver string
	ph     func(i int) string // placeholder for 0-based parameter index
	upsert func(parts []string, keys, cols []string, n int, ph func(int) string) string
	insert func(parts []string, cols []string, n int, ph func(int) string) string
}

// sqlConn is the shared database/sql implementation; dialects only differ
// in driver name, placeholder style and statement text.
type sqlConn struct {
	dialect dialect
	dsn     string
	db      *sql.DB
}

func (c *sqlConn) Connect(ctx context.Context) error {
	db, err := sql.Open(c.dialect.driver, c.dsn)
	if err != nil {
		return err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return err
	}
	c.db = db
	return nil
}

func (c *sqlConn) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

func (c *sqlConn) Stream(ctx context.Context, query string, pageSize int, fn func([]model.Row) error) (int64, error) {
	if pageSize <= 0 {
		pageSize = 5000
	}
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("%s query failed: %w", c.dialect.name, err)
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return 0, err
	}
	var total int64
	page := make([]model.Row, 0, pageSize)
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return total, err
		}
		m := make(model.Row, len(cols))
		for i, col := range cols {
			m[col] = normalize(vals[i])
		}
		page = append(page, m)
		if len(page) >= pageSize {
			if err := fn(page); err != nil {
				return total, err
			}
			total += int64(len(page))
			page = page[:0]
		}
	}
	if err := rows.Err(); err != nil {
		return total, err
	}
	if len(page) > 0 {
		if err := fn(page); err != nil {
			return total, err
		}
		total += int64(len(page))
	}
	return total, nil
}

func (c *sqlConn) UpsertRows(ctx context.Context, table string, keys, cols []string, rows []model.Row) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	parts, err := SplitTablePath(table)
	if err != nil {
		return 0, err
	}
	if err := ValidateColumns(cols); err != nil {
		return 0, err
	}
	if err := ValidateColumns(keys); err != nil {
		return 0, err
	}
	q := c.dialect.upsert(parts, keys, cols, len(rows), c.dialect.ph)
	args := flatArgs(cols, rows)
	if _, err := c.db.ExecContext(ctx, q, args...); err != nil {
		return 0, fmt.Errorf("%s upsert into %s failed: %w", c.dialect.name, table, err)
	}
	return int64(len(rows)), nil
}

func (c *sqlConn) InsertRows(ctx context.Context, table string, cols []string, rows []model.Row) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	parts, err := SplitTablePath(table)
	if err != nil {
		return 0, err
	}
	if err := ValidateColumns(cols); err != nil {
		return 0, err
	}
	q := c.dialect.insert(parts, cols, len(rows), c.dialect.ph)
	args := flatArgs(cols, rows)
	if _, err := c.db.ExecContext(ctx, q, args...); err != nil {
		return 0, fmt.Errorf("%s insert into %s failed: %w", c.dialect.name, table, err)
	}
	return int64(len(rows)), nil
}

func flatArgs(cols []string, rows []model.Row) []any {
	args := make([]any, 0, len(rows)*len(cols))
	for _, r := range rows {
		for _, col := range cols {
			args = append(args, r[col])
		}
	}
	return args
}

// normalize converts driver values that do not round-trip well across
// dialects. []byte (returned by mysql/sqlite for text columns) becomes
// string; BLOB columns are therefore unsupported in v1.
func normalize(v any) any {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return v
}

// ---- shared SQL text builders ----

func quoteDouble(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
func quoteBacktick(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}
func quoteBracket(s string) string { return "[" + strings.ReplaceAll(s, "]", "]]") + "]" }

func qualify(parts []string, q func(string) string) string {
	qs := make([]string, len(parts))
	for i, p := range parts {
		qs[i] = q(p)
	}
	return strings.Join(qs, ".")
}

func quotedList(cols []string, q func(string) string) string {
	qs := make([]string, len(cols))
	for i, c := range cols {
		qs[i] = q(c)
	}
	return strings.Join(qs, ", ")
}

func valuesClause(cols, n int, ph func(int) string) string {
	var b strings.Builder
	for r := 0; r < n; r++ {
		if r > 0 {
			b.WriteString(",")
		}
		b.WriteString("(")
		for c := 0; c < cols; c++ {
			if c > 0 {
				b.WriteString(",")
			}
			b.WriteString(ph(r*cols + c))
		}
		b.WriteString(")")
	}
	return b.String()
}

func insertWith(q func(string) string) func(parts []string, cols []string, n int, ph func(int) string) string {
	return func(parts []string, cols []string, n int, ph func(int) string) string {
		return "INSERT INTO " + qualify(parts, q) + " (" + quotedList(cols, q) +
			") VALUES " + valuesClause(len(cols), n, ph)
	}
}

// upsertOnConflict builds the PostgreSQL / SQLite shape.
func upsertOnConflict(q func(string) string) func(parts []string, keys, cols []string, n int, ph func(int) string) string {
	return func(parts []string, keys, cols []string, n int, ph func(int) string) string {
		keySet := make(map[string]bool, len(keys))
		for _, k := range keys {
			keySet[k] = true
		}
		sets := make([]string, 0, len(cols))
		for _, c := range cols {
			if !keySet[c] {
				sets = append(sets, q(c)+" = excluded."+q(c))
			}
		}
		return "INSERT INTO " + qualify(parts, q) + " (" + quotedList(cols, q) +
			") VALUES " + valuesClause(len(cols), n, ph) +
			" ON CONFLICT (" + quotedList(keys, q) + ") DO UPDATE SET " + strings.Join(sets, ", ")
	}
}
