package connector

import (
	"strings"

	_ "github.com/go-sql-driver/mysql" // registers database/sql driver "mysql"
)

var mysqlDialect = dialect{
	name:   "mysql",
	driver: "mysql",
	ph:     func(int) string { return "?" },
	upsert: upsertMySQL,
	insert: insertWith(quoteBacktick),
}

// upsertMySQL builds INSERT ... ON DUPLICATE KEY UPDATE. Conflict detection
// uses whatever UNIQUE index the target table has; the configured keys are
// excluded from the SET clause.
func upsertMySQL(parts []string, keys, cols []string, n int, ph func(int) string) string {
	q := quoteBacktick
	keySet := make(map[string]bool, len(keys))
	for _, k := range keys {
		keySet[k] = true
	}
	sets := make([]string, 0, len(cols))
	for _, c := range cols {
		if !keySet[c] {
			sets = append(sets, q(c)+" = VALUES("+q(c)+")")
		}
	}
	return "INSERT INTO " + qualify(parts, q) + " (" + quotedList(cols, q) +
		") VALUES " + valuesClause(len(cols), n, ph) +
		" ON DUPLICATE KEY UPDATE " + strings.Join(sets, ", ")
}
