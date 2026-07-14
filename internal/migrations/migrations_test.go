package migrations

import (
	"context"
	"testing"
	"time"

	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestUpAndDown(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping migrations test in -short mode")
	}
	ctx := context.Background()
	pgC, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("to"), tcpostgres.WithUsername("u"), tcpostgres.WithPassword("p"))
	if err != nil {
		t.Skipf("postgres container unavailable: %v", err)
	}
	t.Cleanup(func() { _ = pgC.Terminate(context.Background()) })
	dsn, err := pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("dsn: %v", err)
	}
	// Retry until the db is ready (the testcontainers readiness wait sometimes
	// returns just before the server accepts connections on all interfaces).
	var pool *pgxpool.Pool
	for i := 0; i < 60; i++ {
		pool, err = pgxpool.New(ctx, dsn)
		if err == nil {
			// Ping.
			if perr := pool.Ping(ctx); perr == nil {
				break
			}
			pool.Close()
		}
		select {
		case <-ctx.Done():
			t.Fatalf("ctx done: %v", ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
	if pool == nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	// Up.
	if err := Up(ctx, pool); err != nil {
		t.Fatalf("Up: %v", err)
	}
	// Verify tables exist by inserting a row.
	if _, err := pool.Exec(ctx, `
		INSERT INTO transactions (tx_id, user_id, quote_id, amount, asset, rail, dest_address, status, version)
		VALUES ('t1','u','q','1','BTC','card','0x','created',1)`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// Up is idempotent (IF NOT EXISTS).
	if err := Up(ctx, pool); err != nil {
		t.Fatalf("Up(2): %v", err)
	}
	// Down drops the schema.
	if err := Down(ctx, pool); err != nil {
		t.Fatalf("Down: %v", err)
	}
	// After down, the table should be gone.
	if _, err := pool.Exec(ctx, `
		INSERT INTO transactions (tx_id, user_id, quote_id, amount, asset, rail, dest_address, status, version)
		VALUES ('t2','u','q','1','BTC','card','0x','created',1)`); err == nil {
		t.Fatal("expected insert to fail after Down")
	}
}

func TestUpFilesAndDownFilesOrdering(t *testing.T) {
	ups := upFiles()
	if len(ups) == 0 || ups[0] != "0001_init.up.sql" {
		t.Fatalf("unexpected up files: %v", ups)
	}
	downs := downFiles()
	if len(downs) == 0 || downs[0] != "0001_init.down.sql" {
		t.Fatalf("unexpected down files: %v", downs)
	}
}