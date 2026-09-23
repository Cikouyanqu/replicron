package connector

import (
	"strings"

	_ "github.com/microsoft/go-mssqldb" // registers database/sql driver "sqlserver"
)

var mssqlDialect = dialect{
	name:   "sqlserver",
	driver: "sqlserver",
	ph:     func(int) string { return "?" },
	upsert: mergeMSSQL,
	insert: insertWith(quoteBracket),
}

// mergeMSSQL builds a MERGE statement over a VALUES row constructor. The
// engine caps batch size so rows*cols stays under SQL Server's 2100-parameter
// limit; "?" placeholders are rewritten to @pN by the driver.
func mergeMSSQL(parts []string, keys, cols []string, n int, ph func(int) string) string {
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
	b.WriteString(" AS tgt USING (VALUES ")
	b.WriteString(valuesClause(len(cols), n, ph))
	b.WriteString(") AS src (")
	b.WriteString(quotedList(cols, q))
	b.WriteString(") ON ")
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
