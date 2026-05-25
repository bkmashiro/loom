package proxy

import (
	"errors"
	"os"
	"testing"
	"time"
)

func TestLoadConfigFile_AllFields(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/loom.json"
	content := `{
		"addr": ":9090",
		"upstream": "https://api.example.com",
		"api_key": "sk-test-123",
		"log_level": "debug",
		"system_prompt_file": "/tmp/prompt.txt",
		"system_prompt_mode": "append",
		"timeout": "60s",
		"session_ttl": "10m",
		"injection_role": "user",
		"plan_visibility": "indicator"
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	var cfg Config
	if err := LoadConfigFile(path, &cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Addr != ":9090" {
		t.Errorf("Addr: got %q, want %q", cfg.Addr, ":9090")
	}
	if cfg.Upstream != "https://api.example.com" {
		t.Errorf("Upstream: got %q, want %q", cfg.Upstream, "https://api.example.com")
	}
	if cfg.APIKey != "sk-test-123" {
		t.Errorf("APIKey: got %q, want %q", cfg.APIKey, "sk-test-123")
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel: got %q, want %q", cfg.LogLevel, "debug")
	}
	if cfg.SystemPromptFile != "/tmp/prompt.txt" {
		t.Errorf("SystemPromptFile: got %q", cfg.SystemPromptFile)
	}
	if cfg.SystemPromptMode != "append" {
		t.Errorf("SystemPromptMode: got %q", cfg.SystemPromptMode)
	}
	if cfg.Timeout != 60*time.Second {
		t.Errorf("Timeout: got %v, want 60s", cfg.Timeout)
	}
	if cfg.SessionTTL != 10*time.Minute {
		t.Errorf("SessionTTL: got %v, want 10m", cfg.SessionTTL)
	}
	if cfg.InjectionRole != "user" {
		t.Errorf("InjectionRole: got %q", cfg.InjectionRole)
	}
	if cfg.PlanVisibility != TeeModeIndicator {
		t.Errorf("PlanVisibility: got %v, want TeeModeIndicator", cfg.PlanVisibility)
	}
}

func TestLoadConfigFile_PartialOverride(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/loom.json"
	content := `{"upstream": "https://file.example.com", "addr": ":7070"}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	// Simulate "env var already set Upstream"
	cfg := Config{
		Upstream: "https://env.example.com",
	}
	if err := LoadConfigFile(path, &cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Upstream was already set — file must NOT override it
	if cfg.Upstream != "https://env.example.com" {
		t.Errorf("Upstream overridden: got %q, want https://env.example.com", cfg.Upstream)
	}
	// Addr was empty — file should fill it
	if cfg.Addr != ":7070" {
		t.Errorf("Addr: got %q, want :7070", cfg.Addr)
	}
}

func TestLoadConfigFile_SandboxMounts(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/loom.json"
	content := `{
		"sandbox": {
			"mounts": [
				{"guest": "/data", "host": "/mnt/datasets", "mode": "ro"},
				{"guest": "/work", "host": "/mnt/workdir",  "mode": "rw"},
				{"guest": "/tmp",                           "mode": "ephemeral"}
			]
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	var cfg Config
	if err := LoadConfigFile(path, &cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.SandboxConfig == nil {
		t.Fatal("SandboxConfig is nil")
	}
	mounts := cfg.SandboxConfig.Mounts
	if len(mounts) != 3 {
		t.Fatalf("expected 3 mounts, got %d", len(mounts))
	}
	if mounts[0].Guest != "/data" || mounts[0].Host != "/mnt/datasets" || mounts[0].Mode != "ro" {
		t.Errorf("mount[0] mismatch: %+v", mounts[0])
	}
	if mounts[1].Guest != "/work" || mounts[1].Host != "/mnt/workdir" || mounts[1].Mode != "rw" {
		t.Errorf("mount[1] mismatch: %+v", mounts[1])
	}
	if mounts[2].Guest != "/tmp" || mounts[2].Host != "" || mounts[2].Mode != "ephemeral" {
		t.Errorf("mount[2] mismatch: %+v", mounts[2])
	}
}

func TestLoadConfigFile_EmptyPath(t *testing.T) {
	cfg := Config{Addr: ":1234"}
	if err := LoadConfigFile("", &cfg); err != nil {
		t.Fatalf("expected nil error for empty path, got: %v", err)
	}
	if cfg.Addr != ":1234" {
		t.Errorf("cfg modified unexpectedly: Addr = %q", cfg.Addr)
	}
}

func TestLoadConfigFile_MissingFile(t *testing.T) {
	var cfg Config
	err := LoadConfigFile("/nonexistent/path/loom.json", &cfg)
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("expected os.ErrNotExist, got: %v", err)
	}
}

func TestLoadConfigFile_InvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/bad.json"
	if err := os.WriteFile(path, []byte(`{not valid json`), 0o644); err != nil {
		t.Fatal(err)
	}

	var cfg Config
	err := LoadConfigFile(path, &cfg)
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
}

func TestLoadConfigFile_DurationParsing(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/loom.json"
	content := `{"timeout": "45s", "session_ttl": "15m"}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	var cfg Config
	if err := LoadConfigFile(path, &cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Timeout != 45*time.Second {
		t.Errorf("Timeout: got %v, want 45s", cfg.Timeout)
	}
	if cfg.SessionTTL != 15*time.Minute {
		t.Errorf("SessionTTL: got %v, want 15m", cfg.SessionTTL)
	}
}
