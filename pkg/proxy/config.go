package proxy

import (
	"os"
	"strconv"
	"time"
)

type TeeMode int

const (
	TeeModePassthrough TeeMode = iota
	TeeModeSuppress
	TeeModeIndicator
)

type Config struct {
	Addr             string
	Upstream         string        // e.g. "http://localhost:8080/v1"
	APIKey           string        // if set, overrides client's Authorization header
	PlanVisibility   TeeMode
	IndicatorText    string
	Timeout          time.Duration
	PlanTimeout      time.Duration
	SystemPromptFile string
	SystemPromptMode string // "prepend", "append", "replace"
	LogLevel         string
	MaxPlanSize      int
}

func DefaultConfig() Config {
	return Config{
		Addr:             ":8081",
		Upstream:         "http://localhost:8080/v1",
		PlanVisibility:   TeeModeSuppress,
		IndicatorText:    "Executing plan...",
		Timeout:          120 * time.Second,
		PlanTimeout:      30 * time.Second,
		SystemPromptMode: "prepend",
		LogLevel:         "info",
		MaxPlanSize:      65536,
	}
}

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
	if v := os.Getenv("LOOM_PLAN_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			c.PlanTimeout = d
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
