package migrations

import (
	"io/fs"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestEveryEmbeddedFileIsAMigration(t *testing.T) {
	names, err := fs.Glob(files, "*.sql")
	if err != nil {
		t.Fatalf("list embedded files: %v", err)
	}
	if len(names) == 0 {
		t.Fatal("no migration files are embedded")
	}

	for _, name := range names {
		if _, err := goose.NumericComponent(name); err != nil {
			t.Errorf("goose would skip %s: %v", name, err)
		}
	}
}
