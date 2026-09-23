package connector

import "testing"

func TestSplitTablePath(t *testing.T) {
	cases := []struct {
		in   string
		want []string
		ok   bool
	}{
		{"users", []string{"users"}, true},
		{"public.users", []string{"public", "users"}, true},
		{"a.b.c", nil, false},
		{"users; DROP TABLE x", nil, false},
		{"users]--", nil, false},
		{"bad-name", nil, false},
		{"_ok9", []string{"_ok9"}, true},
	}
	for _, tc := range cases {
		got, err := SplitTablePath(tc.in)
		if tc.ok && err != nil {
			t.Errorf("SplitTablePath(%q) unexpected error: %v", tc.in, err)
			continue
		}
		if !tc.ok {
			if err == nil {
				t.Errorf("SplitTablePath(%q) should reject, got %v", tc.in, got)
			}
			continue
		}
		if len(got) != len(tc.want) {
			t.Errorf("SplitTablePath(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestValidateColumns(t *testing.T) {
	if err := ValidateColumns([]string{"id", "name"}); err != nil {
		t.Errorf("valid columns rejected: %v", err)
	}
	if err := ValidateColumns([]string{"id", "col; xp_cmdshell"}); err == nil {
		t.Error("injection-like column accepted")
	}
}

func TestPostgresUpsertSQL(t *testing.T) {
	got := pgDialect.upsert([]string{"public", "users"}, []string{"id"}, []string{"id", "name"}, 2, pgDialect.ph)
	want := `INSERT INTO "public"."users" ("id", "name") VALUES ($1,$2),($3,$4) ON CONFLICT ("id") DO UPDATE SET "name" = excluded."name"`
	if got != want {
		t.Errorf("pg upsert:\n got %s\nwant %s", got, want)
	}
}

func TestSQLiteUpsertSQL(t *testing.T) {
	got := sqliteDialect.upsert([]string{"users"}, []string{"id"}, []string{"id", "name"}, 1, sqliteDialect.ph)
	want := `INSERT INTO "users" ("id", "name") VALUES ($1,$2) ON CONFLICT ("id") DO UPDATE SET "name" = excluded."name"`
	if got != want {
		t.Errorf("sqlite upsert:\n got %s\nwant %s", got, want)
	}
}

func TestMySQLUpsertSQL(t *testing.T) {
	got := mysqlDialect.upsert([]string{"db", "users"}, []string{"id"}, []string{"id", "name"}, 2, mysqlDialect.ph)
	want := "INSERT INTO `db`.`users` (`id`, `name`) VALUES (?,?),(?,?) ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)"
	if got != want {
		t.Errorf("mysql upsert:\n got %s\nwant %s", got, want)
	}
}

func TestMSSQLMergeSQL(t *testing.T) {
	got := mssqlDialect.upsert([]string{"dbo", "users"}, []string{"id"}, []string{"id", "name"}, 2, mssqlDialect.ph)
	want := `MERGE INTO [dbo].[users] AS tgt USING (VALUES (?,?),(?,?)) AS src ([id], [name]) ON tgt.[id] = src.[id] WHEN MATCHED THEN UPDATE SET [name] = src.[name] WHEN NOT MATCHED THEN INSERT ([id], [name]) VALUES (src.[id], src.[name]);`
	if got != want {
		t.Errorf("mssql merge:\n got %s\nwant %s", got, want)
	}
}

func TestInsertSQL(t *testing.T) {
	got := insertWith(quoteDouble)([]string{"t"}, []string{"a", "b"}, 2, pgDialect.ph)
	want := `INSERT INTO "t" ("a", "b") VALUES ($1,$2),($3,$4)`
	if got != want {
		t.Errorf("insert:\n got %s\nwant %s", got, want)
	}
}

func TestQuotingEscapes(t *testing.T) {
	// The allowlist rejects these identifiers long before quoting matters;
	// quoting is defense in depth for the day the allowlist is relaxed.
	if got := quoteDouble(`a"b`); got != `"a""b"` {
		t.Errorf("quoteDouble = %s", got)
	}
	if got := quoteBacktick("a`b"); got != "`a``b`" {
		t.Errorf("quoteBacktick = %s", got)
	}
	if got := quoteBracket("a]b"); got != "[a]]b]" {
		t.Errorf("quoteBracket = %s", got)
	}
}
