package store

import (
	"context"
	"errors"
	"testing"
	"testing/fstest"

	"github.com/jackc/pgx/v5/pgconn"
)

type fakeExecer struct {
	executed []string
	execErr  error
}

func (f *fakeExecer) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	if f.execErr != nil {
		return pgconn.CommandTag{}, f.execErr
	}
	f.executed = append(f.executed, sql)
	return pgconn.CommandTag{}, nil
}

func TestListMigrationFilesSorted(t *testing.T) {
	fsys := fstest.MapFS{
		"migrations/002_second.sql": {Data: []byte("SELECT 2;")},
		"migrations/001_first.sql":  {Data: []byte("SELECT 1;")},
		"migrations/README.md":      {Data: []byte("ignored")},
	}

	files, err := listMigrationFiles(fsys, "migrations")
	if err != nil {
		t.Fatalf("listMigrationFiles() error = %v", err)
	}

	if len(files) != 2 {
		t.Fatalf("len(files) = %d, want 2", len(files))
	}
	if files[0] != "migrations/001_first.sql" || files[1] != "migrations/002_second.sql" {
		t.Fatalf("files order = %#v, want sorted SQL files", files)
	}
}

func TestApplyMigrationsFromFSAppliesInOrder(t *testing.T) {
	fsys := fstest.MapFS{
		"migrations/002_second.sql": {Data: []byte("SELECT 2;")},
		"migrations/001_first.sql":  {Data: []byte("SELECT 1;")},
		"migrations/003_empty.sql":  {Data: []byte(" \n\t ")},
	}
	execer := &fakeExecer{}

	err := applyMigrationsFromFS(context.Background(), execer, fsys, "migrations")
	if err != nil {
		t.Fatalf("applyMigrationsFromFS() error = %v", err)
	}

	if len(execer.executed) != 2 {
		t.Fatalf("executed statements = %d, want 2", len(execer.executed))
	}
	if execer.executed[0] != "SELECT 1;" {
		t.Fatalf("first migration = %q, want SELECT 1;", execer.executed[0])
	}
	if execer.executed[1] != "SELECT 2;" {
		t.Fatalf("second migration = %q, want SELECT 2;", execer.executed[1])
	}
}

func TestApplyMigrationsFromFSExecError(t *testing.T) {
	fsys := fstest.MapFS{
		"migrations/001_first.sql": {Data: []byte("SELECT 1;")},
	}
	execer := &fakeExecer{execErr: errors.New("boom")}

	err := applyMigrationsFromFS(context.Background(), execer, fsys, "migrations")
	if err == nil {
		t.Fatal("applyMigrationsFromFS() expected error, got nil")
	}
}
