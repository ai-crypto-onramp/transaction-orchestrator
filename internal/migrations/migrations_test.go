package migrations

import (
	"sort"
	"testing"
)

func TestUpAndDownFilesOrdering(t *testing.T) {
	up := upFiles()
	if len(up) == 0 {
		t.Fatal("expected at least one up migration")
	}
	if !sort.StringsAreSorted(up) {
		t.Fatalf("up files not sorted: %v", up)
	}
	// The init migration must be present.
	found := false
	for _, f := range up {
		if f == "0001_init.up.sql" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected 0001_init.up.sql in up files, got %v", up)
	}

	down := downFiles()
	if len(down) == 0 {
		t.Fatal("expected at least one down migration")
	}
	// down files are reverse-sorted.
	for i := 1; i < len(down); i++ {
		if down[i-1] < down[i] {
			t.Fatalf("down files not reverse-sorted: %v", down)
		}
	}
}

func TestUpAndDownMigrationMapsContainInitFiles(t *testing.T) {
	if _, ok := upMigrations["0001_init.up.sql"]; !ok {
		t.Fatal("upMigrations missing 0001_init.up.sql")
	}
	if _, ok := downMigrations["0001_init.down.sql"]; !ok {
		t.Fatal("downMigrations missing 0001_init.down.sql")
	}
	if initUp == "" {
		t.Fatal("embedded initUp is empty")
	}
	if initDown == "" {
		t.Fatal("embedded initDown is empty")
	}
}