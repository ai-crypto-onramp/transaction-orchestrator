// Package migrations embeds the SQL migration files so they can be applied
// programmatically by `make migrate-up` / `make migrate-down` against a
// Postgres instance, without requiring a separate migration tool binary.
package migrations

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

//go:embed sql/*.up.sql sql/*.down.sql
var fsys embed.FS

// Up applies all *.up.sql migrations in lexicographic order against dsn.
func Up(ctx context.Context, dsn string) error {
	return apply(ctx, dsn, "*.up.sql")
}

// Down applies all *.down.sql migrations in reverse lexicographic order.
func Down(ctx context.Context, dsn string) error {
	return apply(ctx, dsn, "*.down.sql")
}

func apply(ctx context.Context, dsn, pattern string) error {
	names, err := fs.Glob(fsys, "sql/"+pattern)
	if err != nil {
		return fmt.Errorf("glob migrations: %w", err)
	}
	sort.Strings(names)
	if strings.HasSuffix(pattern, "down.sql") {
		sort.Sort(sort.Reverse(sort.StringSlice(names)))
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer conn.Close(ctx)
	for _, name := range names {
		b, err := fsys.ReadFile(name)
		if err != nil {
			return fmt.Errorf("read %s: %w", name, err)
		}
		if _, err := conn.Exec(ctx, string(b)); err != nil {
			return fmt.Errorf("apply %s: %w", name, err)
		}
	}
	return nil
}