package connector

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/Cikouyanqu/replicron/internal/model"
)

// stageTempTable is a session-scoped temp table used for bulk upserts; each
// BulkLoad call owns its dedicated connection, so the name never collides.
const pgStageTempTable = "_replicron_bulk_stage"

// BulkLoad implements the native COPY fast path for PostgreSQL targets.
//
// Without keys (insert mode) rows are COPYed straight into the target and
// any conflict surfaces as an error, making the engine fall back to batch
// writes. With keys, rows are COPYed into a session temp table mirroring
// the target's column types and then merged with ON CONFLICT, so the
// statement stays idempotent.
func (c *sqlConn) bulkPostgres(ctx context.Context, parts []string, keys, cols []string, rows []model.Row) (int64, error) {
	// The temp table and the COPY must share one session.
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close() }()

	err = conn.Raw(func(driverConn any) error {
		pc, ok := driverConn.(*stdlib.Conn)
		if !ok {
			return ErrBulkUnsupported
		}
		pg := pc.Conn()
		dst := pgx.Identifier(parts)
		if len(keys) > 0 {
			create := "CREATE TEMP TABLE " + quoteDouble(pgStageTempTable) +
				" AS SELECT " + quotedList(cols, quoteDouble) +
				" FROM " + qualify(parts, quoteDouble) + " WITH NO DATA"
			if _, err := pg.Exec(ctx, create); err != nil {
				return fmt.Errorf("pg bulk staging: %w", err)
			}
			dst = pgx.Identifier{pgStageTempTable}
		}
		if _, err := pg.CopyFrom(ctx, dst, cols, &rowCopySource{rows: rows, cols: cols}); err != nil {
			return fmt.Errorf("pg copy: %w", err)
		}
		if len(keys) > 0 {
			merge := "INSERT INTO " + qualify(parts, quoteDouble) +
				" (" + quotedList(cols, quoteDouble) + ") SELECT " + quotedList(cols, quoteDouble) +
				" FROM " + quoteDouble(pgStageTempTable) +
				" ON CONFLICT (" + quotedList(keys, quoteDouble) + ") DO UPDATE SET " +
				conflictSets(keys, cols, quoteDouble)
			if _, err := pg.Exec(ctx, merge); err != nil {
				return fmt.Errorf("pg bulk merge: %w", err)
			}
			_, _ = pg.Exec(ctx, "DROP TABLE "+quoteDouble(pgStageTempTable))
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return int64(len(rows)), nil
}

// conflictSets builds the "col = excluded.col" list excluding key columns.
func conflictSets(keys, cols []string, q func(string) string) string {
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
	return strings.Join(sets, ", ")
}

// rowCopySource adapts in-memory rows to pgx.CopyFromSource.
type rowCopySource struct {
	rows []model.Row
	cols []string
	idx  int
	cur  []any
}

func (s *rowCopySource) Next() bool {
	if s.idx >= len(s.rows) {
		return false
	}
	r := s.rows[s.idx]
	s.cur = make([]any, len(s.cols))
	for i, c := range s.cols {
		s.cur[i] = r[c]
	}
	s.idx++
	return true
}

func (s *rowCopySource) Values() ([]any, error) { return s.cur, nil }
func (s *rowCopySource) Err() error             { return nil }
