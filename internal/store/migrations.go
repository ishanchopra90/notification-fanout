package store

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type migrationExecer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// ApplyMigrations connects to Postgres and applies SQL files from migrations/.
func ApplyMigrations(ctx context.Context, databaseURL string) error {
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("connect for migrations: %w", err)
	}
	defer conn.Close(ctx)

	return applyMigrationsFromFS(ctx, conn, os.DirFS("."), "migrations")
}

func applyMigrationsFromFS(ctx context.Context, execer migrationExecer, fsys fs.FS, dir string) error {
	migrationFiles, err := listMigrationFiles(fsys, dir)
	if err != nil {
		return err
	}

	for _, migrationPath := range migrationFiles {
		sqlBytes, readErr := fs.ReadFile(fsys, migrationPath)
		if readErr != nil {
			return fmt.Errorf("read migration %s: %w", migrationPath, readErr)
		}
		if strings.TrimSpace(string(sqlBytes)) == "" {
			continue
		}
		if _, execErr := execer.Exec(ctx, string(sqlBytes)); execErr != nil {
			return fmt.Errorf("apply migration %s: %w", migrationPath, execErr)
		}
	}

	return nil
}

func listMigrationFiles(fsys fs.FS, dir string) ([]string, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, fmt.Errorf("read migrations dir %s: %w", dir, err)
	}

	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if filepath.Ext(name) != ".sql" {
			continue
		}
		files = append(files, filepath.Join(dir, name))
	}

	sort.Strings(files)
	return files, nil
}
