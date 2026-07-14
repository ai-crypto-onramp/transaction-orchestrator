// Package migrations embeds the SQL migration files for transaction-orchestrator
// and applies them against a pgxpool.Pool. Up is called automatically by
// store.NewPgStore at startup; cmd/migrate exposes Up/Down for operators.
package migrations

import (
	"context"
	_ "embed"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed 0001_init.up.sql
var initUp string

//go:embed 0001_init.down.sql
var initDown string

// upMigrations is the ordered set of up migrations keyed by filename.
var upMigrations = map[string]string{
	"0001_init.up.sql": initUp,
}

// downMigrations is the ordered set of down migrations keyed by filename,
// applied in reverse filename order.
var downMigrations = map[string]string{
	"0001_init.down.sql": initDown,
}

// Up applies all embedded up migrations in filename order against pool.
func Up(ctx context.Context, pool *pgxpool.Pool) error {
	files := upFiles()
	for _, f := range files {
		ddl, ok := upMigrations[f]
		if !ok {
			continue
		}
		if _, err := pool.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("apply %s: %w", f, err)
		}
	}
	return nil
}

// Down applies all embedded down migrations in reverse filename order against
// pool, dropping the schema created by Up.
func Down(ctx context.Context, pool *pgxpool.Pool) error {
	files := downFiles()
	for _, f := range files {
		ddl, ok := downMigrations[f]
		if !ok {
			continue
		}
		if _, err := pool.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("apply %s: %w", f, err)
		}
	}
	return nil
}

func upFiles() []string {
	out := make([]string, 0, len(upMigrations))
	for k := range upMigrations {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func downFiles() []string {
	out := make([]string, 0, len(downMigrations))
	for k := range downMigrations {
		out = append(out, k)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(out)))
	return out
}