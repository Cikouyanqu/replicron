//go:build integration

// Integration tests for the native bulk paths. Skipped unless the matching
// DSN environment variable is set; run with:
//
//	go test -tags integration ./internal/connector/ -run Bulk -v
//
// The docker-compose demo (examples/docker-compose.yml) provides suitable
// PostgreSQL and MySQL instances; SQL Server needs a local instance.
package connector

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/Cikouyanqu/replicron/internal/model"
)

func bulkRows(n int) []model.Row {
	rows := make([]model.Row, 0, n)
	for i := 1; i <= n; i++ {
		rows = append(rows, model.Row{"id": i, "name": "row-" + string(rune('a'+i%26))})
	}
	return rows
}

func TestPostgresBulkLoadIntegration(t *testing.T) {
	dsn := os.Getenv("REPLICRON_IT_PG_DSN")
	if dsn == "" {
		t.Skip("set REPLICRON_IT_PG_DSN (e.g. postgres://demo:demo@localhost:5432/demo) to run")
	}
	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(`DROP TABLE IF EXISTS it_bulk`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE it_bulk (id int PRIMARY KEY, name text)`); err != nil {
		t.Fatal(err)
	}

	tgt, err := OpenTarget(ctx, "postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tgt.Close() }()

	bl, ok := tgt.(BulkLoader)
	if !ok {
		t.Fatal("postgres target should implement BulkLoader")
	}
	cols := []string{"id", "name"}
	if _, err := bl.BulkLoad(ctx, "public.it_bulk", []string{"id"}, cols, bulkRows(10)); err != nil {
		t.Fatalf("bulk upsert: %v", err)
	}
	// Re-running the same window must not duplicate rows (idempotency).
	if _, err := bl.BulkLoad(ctx, "public.it_bulk", []string{"id"}, cols, bulkRows(10)); err != nil {
		t.Fatalf("bulk re-run: %v", err)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM it_bulk`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 10 {
		t.Errorf("row count = %d, want 10 after idempotent re-run", n)
	}
}

func TestSQLServerBulkLoadIntegration(t *testing.T) {
	dsn := os.Getenv("REPLICRON_IT_MSSQL_DSN")
	if dsn == "" {
		t.Skip("set REPLICRON_IT_MSSQL_DSN (e.g. sqlserver://user:pass@localhost:1433?database=master) to run")
	}
	ctx := context.Background()
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(`IF OBJECT_ID('it_bulk','U') IS NOT NULL DROP TABLE it_bulk`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE it_bulk (id int PRIMARY KEY, name nvarchar(100))`); err != nil {
		t.Fatal(err)
	}

	tgt, err := OpenTarget(ctx, "sqlserver", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tgt.Close() }()

	bl, ok := tgt.(BulkLoader)
	if !ok {
		t.Fatal("sqlserver target should implement BulkLoader")
	}
	cols := []string{"id", "name"}
	if _, err := bl.BulkLoad(ctx, "dbo.it_bulk", []string{"id"}, cols, bulkRows(10)); err != nil {
		t.Fatalf("bulk upsert: %v", err)
	}
	if _, err := bl.BulkLoad(ctx, "dbo.it_bulk", []string{"id"}, cols, bulkRows(10)); err != nil {
		t.Fatalf("bulk re-run: %v", err)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM it_bulk`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 10 {
		t.Errorf("row count = %d, want 10 after idempotent re-run", n)
	}
}
