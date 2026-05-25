package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bkmashiro/loom/pkg/loom"
)

func cmdRun(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	stream  := fs.Bool("stream", false, "stream results as steps complete (one JSON object per line)")
	timeout := fs.Duration("timeout", 60*time.Second, "execution timeout")
	pretty  := fs.Bool("pretty", false, "pretty-print JSON output")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: loom run [flags]\n\nRead a Loom plan from stdin and execute it.\n\nFlags:\n")
		fs.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExample:\n  cat plan.txt | loom run\n  echo '...' | loom run --stream\n")
	}
	fs.Parse(args) //nolint:errcheck

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	ctx, cancelTimeout := context.WithTimeout(ctx, *timeout)
	defer cancelTimeout()

	l := loom.New()

	if *stream {
		ch := l.Stream(ctx, os.Stdin)
		enc := json.NewEncoder(os.Stdout)
		for sr := range ch {
			if *pretty {
				enc.SetIndent("", "  ")
			}
			if err := enc.Encode(sr); err != nil {
				fmt.Fprintf(os.Stderr, "encode error: %v\n", err)
			}
		}
		return
	}

	result, err := l.Run(ctx, os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	enc := json.NewEncoder(os.Stdout)
	if *pretty {
		enc.SetIndent("", "  ")
	}
	enc.Encode(result) //nolint:errcheck
}
