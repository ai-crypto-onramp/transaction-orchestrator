// Command orchctl is the operational CLI for the transaction-orchestrator.
//
// Subcommands:
//
//	orchctl retry      --tx-id <id> --step <name>   force-retry a failed step
//	orchctl compensate  --tx-id <id>              run the compensation cascade
//	orchctl replay      --tx-id <id> --dry-run     replay the saga from the
//	                                              persisted step history against
//	                                              stub partner services
//
// DB_URL selects the target store (Postgres when set, in-memory otherwise).
// The retry/compensate subcommands talk to the running orchestrator over its
// REST API (ORCHESTRATOR_URL, default http://localhost:8080); the replay
// subcommand reads persisted state directly from the DB and exercises stubs
// in-process so it produces no partner side effects.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/logging"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/partner"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/saga"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/statemachine"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/store"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "orchctl:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, strings.TrimSpace(`
orchctl — transaction-orchestrator operational CLI

Usage:
  orchctl retry      --tx-id <id> --step <name>
  orchctl compensate  --tx-id <id>
  orchctl replay      --tx-id <id> --dry-run
`))
}

func run(args []string) error {
	if len(args) == 0 {
		usage()
		return errors.New("missing subcommand")
	}
	switch args[0] {
	case "retry":
		return retryCmd(args[1:])
	case "compensate":
		return compensateCmd(args[1:])
	case "replay":
		return replayCmd(args[1:])
	case "-h", "--help", "help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown subcommand %q", args[0])
	}
}

func retryCmd(args []string) error {
	fs := flag.NewFlagSet("retry", flag.ExitOnError)
	txID := fs.String("tx-id", "", "transaction id (required)")
	step := fs.String("step", "", "step name to retry (required)")
	apiURL := fs.String("url", envOr("ORCHESTRATOR_URL", "http://localhost:8080"), "orchestrator base URL")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *txID == "" || *step == "" {
		return errors.New("--tx-id and --step are required")
	}
	body := fmt.Sprintf(`{"step":%q}`, *step)
	return postJSON(*apiURL+"/v1/transactions/"+*txID+"/retry", []byte(body))
}

func compensateCmd(args []string) error {
	fs := flag.NewFlagSet("compensate", flag.ExitOnError)
	txID := fs.String("tx-id", "", "transaction id (required)")
	apiURL := fs.String("url", envOr("ORCHESTRATOR_URL", "http://localhost:8080"), "orchestrator base URL")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *txID == "" {
		return errors.New("--tx-id is required")
	}
	return postJSON(*apiURL+"/v1/transactions/"+*txID+"/compensate", nil)
}

func replayCmd(args []string) error {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	txID := fs.String("tx-id", "", "transaction id (required)")
	dryRun := fs.Bool("dry-run", false, "replay against stubs with no side effects (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *txID == "" {
		return errors.New("--tx-id is required")
	}
	if !*dryRun {
		return errors.New("only --dry-run is supported (no-side-effects replay)")
	}
	return replayDryRun(*txID)
}

// replayDryRun loads the persisted tx + step history from the store and
// re-runs the saga from the beginning against in-memory stub partner services.
// It compares the resulting step history against the persisted one and
// reports mismatches.  No partner side effects are produced.
func replayDryRun(txID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ctx = logging.WithLogger(ctx, logging.New("info"))

	s, err := openStore(ctx)
	if err != nil {
		return err
	}
	defer s.Close()

	tx, err := s.LoadTx(ctx, txID)
	if err != nil {
		return fmt.Errorf("load tx: %w", err)
	}
	persistedSteps, err := s.ListSteps(ctx, txID)
	if err != nil {
		return fmt.Errorf("list steps: %w", err)
	}

	// Build a fresh in-memory store seeded with the tx's input fields, then
	// run the saga against default stubs.
	mem := store.NewMemStore()
	now := time.Now().UTC()
	seed := store.Transaction{
		TxID: tx.TxID, UserID: tx.UserID, QuoteID: tx.QuoteID, Amount: tx.Amount,
		Asset: tx.Asset, Rail: tx.Rail, DestAddress: tx.DestAddress,
		Status: statemachine.StateCreated, CreatedAt: now, UpdatedAt: now, Version: 1,
	}
	var steps []store.StepRow
	for _, st := range statemachine.StepOrder {
		steps = append(steps, store.StepRow{
			TxID: tx.TxID, StepName: st, Status: store.StepPending, Attempt: 1,
			IdempotencyKey: store.IdempotencyKey(tx.TxID, st, 1),
		})
	}
	sg := store.SagaState{
		TxID: tx.TxID, CurrentStep: statemachine.StepPolicy, State: statemachine.StateCreated,
		Payload: map[string]any{}, Version: 1,
	}
	evts := []store.OutboxEvent{{
		EventID: store.NewEventID(), TxID: tx.TxID, EventType: "transaction.created",
		Status: store.OutboxPending, DedupKey: store.DedupKey(tx.TxID, "transaction.created", "", 0),
		CreatedAt: now,
	}}
	if err := mem.RunInTx(ctx, func(ts store.TxStore) error {
		return ts.CreateTx(ctx, seed, steps, sg, evts)
	}); err != nil {
		return fmt.Errorf("seed: %w", err)
	}

	stub := partner.NewStub(partner.DefaultStubConfig())
	clients := &saga.Clients{Policy: stub, Payment: stub, Kyt: stub, Mpc: stub, Blockchain: stub, Ledger: stub, Audit: stub}
	ex := saga.NewExecutor(mem, clients, saga.DefaultConfig())
	if err := ex.Run(ctx, tx.TxID, "orchctl-replay"); err != nil {
		return fmt.Errorf("replay run: %w", err)
	}

	replayedSteps, _ := mem.ListSteps(ctx, tx.TxID)
	replayedTx, _ := mem.LoadTx(ctx, tx.TxID)

	fmt.Println("replay dry-run complete:")
	fmt.Printf("  persisted final state: %s\n", tx.Status)
	fmt.Printf("  replayed  final state: %s\n", replayedTx.Status)
	fmt.Printf("  persisted step count: %d\n", len(persistedSteps))
	fmt.Printf("  replayed  step count: %d\n", len(replayedSteps))
	mismatch := false
	for _, ps := range persistedSteps {
		if ps.Status == store.StepSucceeded {
			matched := false
			for _, rs := range replayedSteps {
				if rs.StepName == ps.StepName && rs.Status == store.StepSucceeded {
					matched = true
					break
				}
			}
			if !matched {
				fmt.Printf("  MISMATCH: step %s succeeded in persisted but not in replay\n", ps.StepName)
				mismatch = true
			}
		}
	}
	if mismatch {
		return errors.New("replay produced a different step history")
	}
	fmt.Println("  step history matches persisted record")
	return nil
}

// --- helpers ----------------------------------------------------------------

func openStore(ctx context.Context) (store.Store, error) {
	dsn := os.Getenv("DB_URL")
	if dsn == "" {
		return store.NewMemStore(), nil
	}
	ps, err := store.NewPgStore(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return ps, nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func postJSON(url string, body []byte) error {
	var r io.Reader
	if body != nil {
		r = strings.NewReader(string(body))
	}
	req, err := http.NewRequest(http.MethodPost, url, r)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	var pretty map[string]any
	if json.Unmarshal(respBody, &pretty) == nil {
		out, _ := json.MarshalIndent(pretty, "", "  ")
		fmt.Println(string(out))
	} else {
		fmt.Println(string(respBody))
	}
	return nil
}