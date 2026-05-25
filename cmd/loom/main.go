package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}
	switch os.Args[1] {
	case "run":
		cmdRun(os.Args[2:])
	case "serve":
		cmdServe(os.Args[2:])
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `loom — LLM execution runtime

Usage:
  loom run [flags]      Read a plan from stdin, execute it, print result to stdout
  loom serve [flags]    Start an HTTP server that accepts plans and returns results

Run 'loom <command> -h' for command-specific flags.
`)
}
