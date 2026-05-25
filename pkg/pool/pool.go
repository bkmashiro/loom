// Package pool implements a pre-warmed WASM instance pool for fast,
// isolated code execution. Each language gets its own channel-based pool
// of wasmInstance values. Acquire pulls an instance from the channel;
// Release restores its memory snapshot and returns it to the channel.
package pool

import (
	"context"
	"errors"
	"time"
)

// Language identifies the execution language for a pool instance.
type Language string

const (
	LangPython Language = "python"
	LangJS     Language = "js"
	LangShell  Language = "shell"
)

// Sentinel errors returned by Pool operations.
var (
	// ErrLanguageNotSupported is returned when Acquire is called for a language
	// that has no module path configured in PoolConfig.Modules.
	ErrLanguageNotSupported = errors.New("pool: language not supported (no module configured)")

	// ErrPoolTimeout is returned when Acquire times out waiting for an available instance.
	ErrPoolTimeout = errors.New("pool: acquire timeout: no instance available")
)

// Instance is an acquired WASM sandbox ready to execute code.
// The caller must call Pool.Release after use.
type Instance interface {
	// Run executes code in the sandbox. After Run, the instance's memory
	// is restored to its clean snapshot state.
	Run(ctx context.Context, code string, inputs map[string]any) (any, error)

	// Language returns the language this instance was created for.
	Language() Language
}

// Pool manages pre-warmed WASM instances.
type Pool interface {
	// Acquire waits for an idle instance for the given language. Returns
	// ErrLanguageNotSupported or ErrPoolTimeout on failure.
	Acquire(ctx context.Context, lang Language) (Instance, error)

	// Release restores the instance's snapshot state and returns it to the pool.
	Release(inst Instance)

	// Start compiles WASM modules and warms up the instance pool.
	Start(ctx context.Context) error

	// Shutdown drains the pool and closes the wazero runtime.
	Shutdown(ctx context.Context) error

	// Stats returns a snapshot of current pool statistics.
	Stats() PoolStats
}

// PoolStats is a point-in-time snapshot of pool utilisation.
type PoolStats struct {
	ByLanguage map[Language]LanguageStats
}

// LanguageStats holds per-language pool metrics.
type LanguageStats struct {
	Total     int
	Idle      int
	InUse     int
	WaitCount int
}

// PoolConfig configures a Pool created by NewPool.
type PoolConfig struct {
	// Modules maps language → absolute path to .wasm file.
	// Only languages present in this map are served by the pool.
	Modules map[Language]string

	// InstancesPerLang is the number of pre-warmed instances per language.
	// Defaults to runtime.NumCPU() if <= 0.
	InstancesPerLang int

	// AcquireTimeout is the maximum time Acquire will block waiting for an
	// idle instance. Defaults to 5 seconds if zero.
	AcquireTimeout time.Duration
}

// NewPool creates a new Pool with the given configuration.
// Call Pool.Start before acquiring instances.
func NewPool(cfg PoolConfig) Pool {
	return newWazeroPool(cfg)
}
