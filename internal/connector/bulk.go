package connector

import (
	"context"

	"github.com/Cikouyanqu/replicron/internal/model"
)

// BulkLoad routes to the dialect's native bulk path. Identifier validation
// happens here once; dialect implementations receive pre-validated parts.
func (c *sqlConn) BulkLoad(ctx context.Context, table string, keys, cols []string, rows []model.Row) (int64, error) {
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
	switch c.dialect.name {
	case pgDialect.name:
		return c.bulkPostgres(ctx, parts, keys, cols, rows)
	case mssqlDialect.name:
		return c.bulkSQLServer(ctx, parts, keys, cols, rows)
	default:
		return 0, ErrBulkUnsupported
	}
}
