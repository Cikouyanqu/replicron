package connector

import (
	"strconv"

	_ "modernc.org/sqlite" // registers database/sql driver "sqlite" (pure Go, no CGO)
)

var sqliteDialect = dialect{
	name:   "sqlite",
	driver: "sqlite",
	ph:     func(i int) string { return "$" + strconv.Itoa(i+1) },
	upsert: upsertOnConflict(quoteDouble),
	insert: insertWith(quoteDouble),
}
