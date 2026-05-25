package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/bkmashiro/loom/pkg/loom"
)

func cmdServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr    := fs.String("addr", ":8080", "listen address")
	timeout := fs.Duration("timeout", 30*time.Second, "per-request timeout")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: loom serve [flags]\n\nStart an HTTP server that accepts Loom plans and returns results.\n\nFlags:\n")
		fs.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nEndpoints:\n  POST /run     Execute a plan (body = plan text)\n  GET  /health  Health check\n  GET  /stats   Runtime statistics\n")
	}
	fs.Parse(args) //nolint:errcheck

	startTime := time.Now()

	mux := http.NewServeMux()

	// GET /health
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{ //nolint:errcheck
			"status": "ok",
			"time":   time.Now().UTC().Format(time.RFC3339),
		})
	})

	// GET /stats
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
			"uptime_seconds": int(time.Since(startTime).Seconds()),
		})
	})

	// POST /run
	mux.HandleFunc("/run", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), *timeout)
		defer cancel()

		l := loom.New()
		w.Header().Set("Content-Type", "application/json")

		streamHeader := strings.ToLower(r.Header.Get("X-Stream"))
		if streamHeader == "true" || streamHeader == "1" {
			// NDJSON streaming mode
			flusher, canFlush := w.(http.Flusher)
			enc := json.NewEncoder(w)
			ch := l.Stream(ctx, r.Body)
			for sr := range ch {
				if err := enc.Encode(sr); err != nil {
					// Client disconnected; drain the channel
					for range ch {
					}
					return
				}
				if canFlush {
					flusher.Flush()
				}
			}
			return
		}

		// Non-streaming: return final result
		result, err := l.Run(ctx, r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
				"step_id": result.StepID,
				"data":    result.Data,
				"err":     err.Error(),
			})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
			"step_id": result.StepID,
			"data":    result.Data,
			"err":     nil,
		})
	})

	srv := &http.Server{
		Addr:    *addr,
		Handler: mux,
	}

	// Graceful shutdown on SIGINT/SIGTERM
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		fmt.Fprintf(os.Stderr, "shutting down server...\n")
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutCtx); err != nil {
			fmt.Fprintf(os.Stderr, "shutdown error: %v\n", err)
		}
	}()

	fmt.Fprintf(os.Stderr, "loom serve listening on %s\n", *addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		os.Exit(1)
	}
}
