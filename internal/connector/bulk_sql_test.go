package connector

import "testing"

func TestConflictSets(t *testing.T) {
	got := conflictSets([]string{"id"}, []string{"id", "name", "qty"}, quoteDouble)
	want := `"name" = excluded."name", "qty" = excluded."qty"`
	if got != want {
		t.Errorf("conflictSets = %s, want %s", got, want)
	}
}

func TestMergeFromStageSQL(t *testing.T) {
	got := mergeFromStage([]string{"dbo", "t"}, "#replicron_bulk_1", []string{"id"}, []string{"id", "name"})
	want := `MERGE INTO [dbo].[t] AS tgt USING [#replicron_bulk_1] AS src ON tgt.[id] = src.[id] WHEN MATCHED THEN UPDATE SET [name] = src.[name] WHEN NOT MATCHED THEN INSERT ([id], [name]) VALUES (src.[id], src.[name]);`
	if got != want {
		t.Errorf("mergeFromStage:\n got %s\nwant %s", got, want)
	}
}
