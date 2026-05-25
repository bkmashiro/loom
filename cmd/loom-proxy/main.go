package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bkmashiro/loom/pkg/loom"
	"github.com/bkmashiro/loom/pkg/proxy"
)

func main() {
	cfg := proxy.ConfigFromEnv()

	// CLI flags override env vars
	flag.StringVar(&cfg.Addr, "addr", cfg.Addr, "Listen address")
	flag.StringVar(&cfg.Upstream, "upstream", cfg.Upstream, "Upstream LLM API base URL")
	flag.StringVar(&cfg.APIKey, "api-key", cfg.APIKey, "Override auth for upstream calls")
	flag.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "Log level: debug, info, warn, error")
	flag.StringVar(&cfg.SystemPromptFile, "system-prompt-file", cfg.SystemPromptFile, "Path to Loom system prompt to inject")
	flag.StringVar(&cfg.SystemPromptMode, "system-prompt-mode", cfg.SystemPromptMode, "How to merge system prompt: prepend, append, replace")
	flag.DurationVar(&cfg.Timeout, "timeout", cfg.Timeout, "Per-request timeout (also bounds background plan execution)")
	flag.DurationVar(&cfg.SessionTTL, "session-ttl", cfg.SessionTTL, "Session expiration after inactivity")
	flag.StringVar(&cfg.InjectionRole, "injection-role", cfg.InjectionRole, "Role for injected results: tool or user")

	planVisibility := flag.String("plan-visibility", "", "Plan visibility: passthrough, suppress, indicator")
	flag.Parse()

	if *planVisibility != "" {
		switch *planVisibility {
		case "passthrough":
			cfg.PlanVisibility = proxy.TeeModePassthrough
		case "suppress":
			cfg.PlanVisibility = proxy.TeeModeSuppress
		case "indicator":
			cfg.PlanVisibility = proxy.TeeModeIndicator
		}
	}

	level := slog.LevelInfo
	switch cfg.LogLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	l := loom.New()

	h, err := proxy.NewHandler(cfg, l)
	if err != nil {
		slog.Error("failed to create handler", "err", err)
		os.Exit(1)
	}

	srv := &http.Server{
		Addr:         cfg.Addr,
		Handler:      h,
		ReadTimeout:  cfg.Timeout,
		WriteTimeout: cfg.Timeout,
	}

	// Graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		slog.Info("loom-proxy starting", "addr", cfg.Addr, "upstream", cfg.Upstream)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	<-stop
	slog.Info("shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("shutdown error", "err", err)
		os.Exit(1)
	}
	slog.Info("stopped")
}
