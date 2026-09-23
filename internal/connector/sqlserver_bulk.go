package connector

import (
	"context"
	"fmt"
	"strings"
	"time"

	mssql "github.com/microsoft/go-mssqldb"

	"github.com/Cikouyanqu/replicron/internal/model"
)

// BulkLoad implements the native bulk-protocol fast path for SQL Server
// targets: rows stream through mssql.CopyIn into a ##global temp staging
// table (visible across pooled connections), then a single MERGE (or plain
// INSERT for keyless tasks) applies them to the target.
func (c *sqlConn) bulkSQLServer(ctx context.Context, parts []string, keys, cols []string, rows []model.Row) (int64, error) {
	stage := fmt.Sprintf("##replicron_bulk_%d", time.Now().UnixNano())
	create := "SELECT TOP 0 " + quotedList(cols, quoteBracket) + " INTO " + quoteBracket(stage) +
		" FROM " + qualify(parts, quoteBracket)
	if _, err := c.db.ExecContext(ctx, create); err != nil {
		return 0, fmt.Errorf("mssql bulk staging: %w", err)
	}
	// Cleanup must not inherit ctx: it runs after failures where ctx may
	// already be cancelled.
	drop := func() {
		_, _ = c.db.ExecContext(context.Background(),
			"IF OBJECT_ID('tempdb.."+stage+"') IS NOT NULL DROP TABLE "+quoteBracket(stage))
	}
	defer drop()

	stmt, err := c.db.PrepareContext(ctx, mssql.CopyIn(stage, mssql.BulkOptions{}, cols...))
	if err != nil {
		return 0, fmt.Errorf("mssql bulk prepare: %w", err)
	}
	for _, r := range rows {
		vals := make([]any, len(cols))
		for i, col := range cols {
			vals[i] = r[col]
		}
		if _, err := stmt.ExecContext(ctx, vals...); err != nil {
			_ = stmt.Close()
			return 0, fmt.Errorf("mssql bulk send: %w", err)
		}
	}
	if _, err := stmt.ExecContext(ctx); err != nil { // no-arg exec flushes the buffer
		_ = stmt.Close()
		return 0, fmt.Errorf("mssql bulk flush: %w", err)
	}
	if err := stmt.Close(); err != nil {
		return 0, fmt.Errorf("mssql bulk close: %w", err)
	}

	final := "INSERT INTO " + qualify(parts, quoteBracket) + " (" + quotedList(cols, quoteBracket) +
		") SELECT " + quotedList(cols, quoteBracket) + " FROM " + quoteBracket(stage)
	if len(keys) > 0 {
		final = mergeFromStage(parts, stage, keys, cols)
	}
	if _, err := c.db.ExecContext(ctx, final); err != nil {
		return 0, fmt.Errorf("mssql bulk apply: %w", err)
	}
	return int64(len(rows)), nil
}

// mergeFromStage builds a MERGE whose source is the staging table (which
// carries exactly the synced columns).
func mergeFromStage(parts []string, stage string, keys, cols []string) string {
	q := quoteBracket
	on := make([]string, 0, len(keys))
	keySet := make(map[string]bool, len(keys))
	for _, k := range keys {
		on = append(on, "tgt."+q(k)+" = src."+q(k))
		keySet[k] = true
	}
	sets := make([]string, 0, len(cols))
	for _, c := range cols {
		if !keySet[c] {
			sets = append(sets, q(c)+" = src."+q(c))
		}
	}
	insVals := make([]string, 0, len(cols))
	for _, c := range cols {
		insVals = append(insVals, "src."+q(c))
	}
	var b strings.Builder
	b.WriteString("MERGE INTO ")
	b.WriteString(qualify(parts, q))
	b.WriteString(" AS tgt USING ")
	b.WriteString(q(stage))
	b.WriteString(" AS src ON ")
	b.WriteString(strings.Join(on, " AND "))
	b.WriteString(" WHEN MATCHED THEN UPDATE SET ")
	b.WriteString(strings.Join(sets, ", "))
	b.WriteString(" WHEN NOT MATCHED THEN INSERT (")
	b.WriteString(quotedList(cols, q))
	b.WriteString(") VALUES (")
	b.WriteString(strings.Join(insVals, ", "))
	b.WriteString(");")
	return b.String()
}
