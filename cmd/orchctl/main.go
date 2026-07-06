// Command orchctl is the operational CLI for the transaction orchestrator.
// Stage 1 lays out the entry point; subcommands (retry/compensate/replay) are
// implemented in later stages.
package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: orchctl <command> [flags]")
		fmt.Fprintln(os.Stderr, "commands: retry, compensate, replay")
		os.Exit(1)
	}
	switch os.Args[1] {
	case "retry":
		fs := flag.NewFlagSet("retry", flag.ExitOnError)
		txID := fs.String("tx-id", "", "transaction id")
		step := fs.String("step", "", "step name")
		_ = fs.Parse(os.Args[2:])
		if *txID == "" || *step == "" {
			fmt.Fprintln(os.Stderr, "retry: --tx-id and --step are required")
			os.Exit(1)
		}
		fmt.Println("retry: not yet implemented (stage 8)")
	case "compensate":
		fs := flag.NewFlagSet("compensate", flag.ExitOnError)
		txID := fs.String("tx-id", "", "transaction id")
		_ = fs.Parse(os.Args[2:])
		if *txID == "" {
			fmt.Fprintln(os.Stderr, "compensate: --tx-id is required")
			os.Exit(1)
		}
		fmt.Println("compensate: not yet implemented (stage 8)")
	case "replay":
		fs := flag.NewFlagSet("replay", flag.ExitOnError)
		txID := fs.String("tx-id", "", "transaction id")
		dryRun := fs.Bool("dry-run", false, "no side effects")
		_ = fs.Parse(os.Args[2:])
		if *txID == "" {
			fmt.Fprintln(os.Stderr, "replay: --tx-id is required")
			os.Exit(1)
		}
		_ = dryRun
		fmt.Println("replay: not yet implemented (stage 8)")
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		os.Exit(1)
	}
}