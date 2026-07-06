// Command migrate applies embedded SQL migrations against a Postgres instance.
// Usage: migrate <up|down> [DSN]
// If DSN is omitted, the DB_URL environment variable is used.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/migrations"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: migrate <up|down> [DSN]")
	}
	dir := os.Args[1]
	dsn := ""
	if len(os.Args) >= 3 {
		dsn = os.Args[2]
	} else {
		dsn = os.Getenv("DB_URL")
	}
	if dsn == "" {
		log.Fatal("DSN required: pass as arg or set DB_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var err error
	switch dir {
	case "up":
		err = migrations.Up(ctx, dsn)
	case "down":
		err = migrations.Down(ctx, dsn)
	default:
		log.Fatalf("unknown direction %q (use up|down)", dir)
	}
	if err != nil {
		log.Fatalf("migrate %s: %v", dir, err)
	}
	fmt.Printf("migrate %s: ok\n", dir)
}