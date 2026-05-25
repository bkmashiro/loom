package proxy

import (
	"os"
	"strconv"
	"time"
)

// TeeMode controls how the proxy handles plan fence content in the SSE stream.
type TeeMode int

const (
	TeeModePassthrough TeeMode = iota // forward plan fences as-is to client
	TeeModeSuppress                   // hide plan fences from client
	TeeModeIndicator                  // hide fences, show indicator text instead
)

// Config holds all Loom Proxy configuration.
type Config struct {
	Addr     string
	Upstream string // e.g. "http://localhost:8080/v1"
	APIKey   string // if set, overrides client's Authorization header

	PlanVisibility TeeMode
	IndicatorText  string

	Timeout time.Duration // per-request timeout (also bounds background execution)

	// Session management (v2)
	SessionTTL    time.Duration // session expiration after inactivity
	InjectionRole string        // "tool" or "user" — role for injected result messages

	SystemPromptFile string
	SystemPromptMode string // "prepend", "append", "replace"

	LogLevel    string
	MaxPlanSize int
}

// DefaultConfig returns sensible defaults for Loom Proxy v2.
func DefaultConfig() Config {
	return Config{
		Addr:             ":8081",
		Upstream:         "http://localhost:8080/v1",
		PlanVisibility:   TeeModeSuppress,
		IndicatorText:    "Executing plan...",
		Timeout:          120 * time.Second,
		SessionTTL:       5 * time.Minute,
		InjectionRole:    "tool",
		SystemPromptMode: "prepend",
		LogLevel:         "info",
		MaxPlanSize:      65536,
	}
}

// ConfigFromEnv builds a Config from environment variables, with DefaultConfig() as base.
func ConfigFromEnv() Config {
	c := DefaultConfig()
	if v := os.Getenv("LOOM_ADDR"); v != "" {
		c.Addr = v
	}
	if v := os.Getenv("LOOM_UPSTREAM"); v != "" {
		c.Upstream = v
	}
	if v := os.Getenv("LOOM_API_KEY"); v != "" {
		c.APIKey = v
	}
	if v := os.Getenv("LOOM_PLAN_VISIBILITY"); v != "" {
		switch v {
		case "passthrough":
			c.PlanVisibility = TeeModePassthrough
		case "suppress":
			c.PlanVisibility = TeeModeSuppress
		case "indicator":
			c.PlanVisibility = TeeModeIndicator
		}
	}
	if v := os.Getenv("LOOM_INDICATOR_TEXT"); v != "" {
		c.IndicatorText = v
	}
	if v := os.Getenv("LOOM_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			c.Timeout = d
		}
	}
	if v := os.Getenv("LOOM_SESSION_TTL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			c.SessionTTL = d
		}
	}
	if v := os.Getenv("LOOM_INJECTION_ROLE"); v != "" {
		if v == "tool" || v == "user" {
			c.InjectionRole = v
		}
	}
	if v := os.Getenv("LOOM_SYSTEM_PROMPT_FILE"); v != "" {
		c.SystemPromptFile = v
	}
	if v := os.Getenv("LOOM_SYSTEM_PROMPT_MODE"); v != "" {
		c.SystemPromptMode = v
	}
	if v := os.Getenv("LOOM_LOG_LEVEL"); v != "" {
		c.LogLevel = v
	}
	if v := os.Getenv("LOOM_MAX_PLAN_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.MaxPlanSize = n
		}
	}
	return c
}
