// Command migrate applies the embedded transaction-orchestrator migrations
// against the database named by DB_URL. It backs the Makefile migrate-up /
// migrate-down targets without pulling in an external migration tool. The
// orchestrator server also auto-migrates at startup via store.NewPgStore;
// this command is provided for operators and CI flows that need to apply or
// roll back schema out-of-band.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	direction := flag.String("direction", "up", "up or down")
	flag.Parse()

	if err := run(*direction); err != nil {
		log.Fatalf("migrate: %v", err)
	}
}

func run(direction string) error {
	dsn := os.Getenv("DB_URL")
	if dsn == "" {
		return fmt.Errorf("DB_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("pgxpool.New: %w", err)
	}
	defer pool.Close()

	switch direction {
	case "up":
		if err := migrations.Up(ctx, pool); err != nil {
			return err
		}
		fmt.Println("migrations applied")
	case "down":
		if err := migrations.Down(ctx, pool); err != nil {
			return err
		}
		fmt.Println("migrations rolled back")
	default:
		return fmt.Errorf("unknown direction %q (want up or down)", direction)
	}
	return nil
}