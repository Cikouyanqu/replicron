package connector

import (
	"strconv"

	_ "github.com/jackc/pgx/v5/stdlib" // registers database/sql driver "pgx"
)

var pgDialect = dialect{
	name:   "postgres",
	driver: "pgx",
	ph:     func(i int) string { return "$" + strconv.Itoa(i+1) },
	upsert: upsertOnConflict(quoteDouble),
	insert: insertWith(quoteDouble),
}
