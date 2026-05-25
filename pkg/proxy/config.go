package proxy

import (
	"encoding/json"
	"errors"
	"fmt"
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

// SandboxMountConfig is the JSON representation of one sandbox mount.
type SandboxMountConfig struct {
	Guest string `json:"guest"`          // guest path prefix, e.g. "/work"
	Host  string `json:"host,omitempty"` // host directory; empty for ephemeral
	Mode  string `json:"mode"`           // "ro", "rw", "ephemeral", "persistent"
}

// SandboxFileConfig is the JSON sandbox section.
type SandboxFileConfig struct {
	Mounts []SandboxMountConfig `json:"mounts"`
}

// FileConfig mirrors all Config fields as JSON-tagged pointers so absent keys
// do not override defaults.
type FileConfig struct {
	Addr             *string            `json:"addr,omitempty"`
	Upstream         *string            `json:"upstream,omitempty"`
	APIKey           *string            `json:"api_key,omitempty"`
	LogLevel         *string            `json:"log_level,omitempty"`
	SystemPromptFile *string            `json:"system_prompt_file,omitempty"`
	SystemPromptMode *string            `json:"system_prompt_mode,omitempty"`
	Timeout          *string            `json:"timeout,omitempty"`    // duration string e.g. "30s"
	SessionTTL       *string            `json:"session_ttl,omitempty"`
	InjectionRole    *string            `json:"injection_role,omitempty"`
	PlanVisibility   *string            `json:"plan_visibility,omitempty"`
	Sandbox          *SandboxFileConfig `json:"sandbox,omitempty"`
}

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

	LoomDescribeEnabled bool

	SandboxConfig *SandboxFileConfig // populated from config file sandbox section
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
		LogLevel:             "info",
		MaxPlanSize:          65536,
		LoomDescribeEnabled:  true,
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
	if v := os.Getenv("LOOM_DESCRIBE_ENABLED"); v == "false" {
		c.LoomDescribeEnabled = false
	}
	return c
}

// LoadConfigFile reads path and merges non-nil fields into dst.
// Fields already set in dst (i.e. non-zero) are NOT overwritten.
// Absent JSON fields (nil pointers) are skipped.
// If path is empty, returns nil immediately.
// If file does not exist, returns a wrapped os.ErrNotExist error.
func LoadConfigFile(path string, dst *Config) error {
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("config file %q: %w", path, os.ErrNotExist)
		}
		return fmt.Errorf("reading config file %q: %w", path, err)
	}
	var fc FileConfig
	if err := json.Unmarshal(data, &fc); err != nil {
		return fmt.Errorf("parsing config file %q: %w", path, err)
	}

	if fc.Addr != nil && dst.Addr == "" {
		dst.Addr = *fc.Addr
	}
	if fc.Upstream != nil && dst.Upstream == "" {
		dst.Upstream = *fc.Upstream
	}
	if fc.APIKey != nil && dst.APIKey == "" {
		dst.APIKey = *fc.APIKey
	}
	if fc.LogLevel != nil && dst.LogLevel == "" {
		dst.LogLevel = *fc.LogLevel
	}
	if fc.SystemPromptFile != nil && dst.SystemPromptFile == "" {
		dst.SystemPromptFile = *fc.SystemPromptFile
	}
	if fc.SystemPromptMode != nil && dst.SystemPromptMode == "" {
		dst.SystemPromptMode = *fc.SystemPromptMode
	}
	if fc.InjectionRole != nil && dst.InjectionRole == "" {
		dst.InjectionRole = *fc.InjectionRole
	}
	if fc.Timeout != nil && dst.Timeout == 0 {
		d, err := time.ParseDuration(*fc.Timeout)
		if err != nil {
			return fmt.Errorf("config file %q: invalid timeout %q: %w", path, *fc.Timeout, err)
		}
		dst.Timeout = d
	}
	if fc.SessionTTL != nil && dst.SessionTTL == 0 {
		d, err := time.ParseDuration(*fc.SessionTTL)
		if err != nil {
			return fmt.Errorf("config file %q: invalid session_ttl %q: %w", path, *fc.SessionTTL, err)
		}
		dst.SessionTTL = d
	}
	if fc.PlanVisibility != nil {
		// Only apply if dst is still at the zero value (TeeModePassthrough == 0).
		// We use a sentinel approach: only skip if dst was explicitly set to a
		// non-zero TeeMode (Suppress or Indicator). Since TeeModePassthrough is 0,
		// we apply the file value when the field equals TeeModePassthrough AND
		// the caller hasn't explicitly toggled it — this is a best-effort merge.
		// For a clean design, we apply file value only when dst.PlanVisibility == TeeModePassthrough
		// (the zero value) to avoid overriding env-set values.
		if dst.PlanVisibility == TeeModePassthrough {
			switch *fc.PlanVisibility {
			case "passthrough":
				dst.PlanVisibility = TeeModePassthrough
			case "suppress":
				dst.PlanVisibility = TeeModeSuppress
			case "indicator":
				dst.PlanVisibility = TeeModeIndicator
			}
		}
	}
	if fc.Sandbox != nil && dst.SandboxConfig == nil {
		dst.SandboxConfig = fc.Sandbox
	}
	return nil
}
