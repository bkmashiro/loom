package loom

import (
	"context"
	"io"
	"net/http"

	"github.com/bkmashiro/loom/pkg/dag"
	"github.com/bkmashiro/loom/pkg/exec"
	"github.com/bkmashiro/loom/pkg/parser"
	"github.com/bkmashiro/loom/pkg/pool"
	"github.com/bkmashiro/loom/pkg/primitives"
)

// Re-export key types so callers don't need to import sub-packages.
type Result = dag.Result
type StepResult = dag.StepResult
type ToolFunc = exec.ToolFunc

// Loom is the top-level runtime that wires parser, DAG scheduler, and executor together.
type Loom struct {
	pool       pool.Pool
	httpClient *http.Client
	tools      *exec.ToolRegistry
	kv         primitives.KVStore
}

// Option configures a Loom runtime.
type Option func(*Loom)

// WithWASMPool sets the WASM pool used for pure/shell steps.
func WithWASMPool(p pool.Pool) Option {
	return func(l *Loom) {
		l.pool = p
	}
}

// WithHTTPClient sets the HTTP client used for IO/Write steps.
func WithHTTPClient(c *http.Client) Option {
	return func(l *Loom) {
		l.httpClient = c
	}
}

// WithKV sets the KV store for the runtime.
func WithKV(store primitives.KVStore) Option {
	return func(l *Loom) {
		l.kv = store
	}
}

// New creates a Loom with defaults (http.DefaultClient, in-memory KV, no WASM pool).
func New(opts ...Option) *Loom {
	l := &Loom{
		httpClient: http.DefaultClient,
		tools:      exec.NewToolRegistry(),
		kv:         primitives.NewMemoryKV(),
	}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

// RegisterTool registers an escape-hatch tool by name.
func (l *Loom) RegisterTool(name string, fn ToolFunc) {
	l.tools.Register(name, fn)
}

// newExecutor builds a StepExecutor from the current Loom configuration.
func (l *Loom) newExecutor() *exec.StepExecutor {
	return exec.NewStepExecutor(exec.StepExecutorConfig{
		Pool:       l.pool,
		HTTPClient: l.httpClient,
		Tools:      l.tools,
		KV:         l.kv,
	})
}

// Run parses a complete plan from r and executes it, returning the final result.
// Blocks until the return step (or all steps) complete.
func (l *Loom) Run(ctx context.Context, r io.Reader) (Result, error) {
	executor := l.newExecutor()
	sched := dag.NewScheduler(ctx, executor)
	p := parser.NewParser(r)
	defer p.Close()

	for event := range p.Events() {
		if event.Step != nil {
			if err := sched.Submit(*event.Step); err != nil {
				// log and continue — malformed steps don't crash the plan
				_ = err
			}
		}
		if event.Return != nil {
			sched.SetReturn(event.Return.StepID)
		}
	}

	sched.Seal() // signal no more steps coming

	if err := p.Err(); err != nil {
		// parser error — but some steps may have executed; still wait
		_ = err
	}

	return sched.Wait()
}

// Stream parses a plan from r and returns a channel emitting StepResults as
// each step completes. Channel is closed when all steps are done.
func (l *Loom) Stream(ctx context.Context, r io.Reader) <-chan StepResult {
	executor := l.newExecutor()
	sched := dag.NewScheduler(ctx, executor)

	ch := sched.Stream() // subscribe before parsing starts

	go func() {
		p := parser.NewParser(r)
		defer p.Close()
		for event := range p.Events() {
			if event.Step != nil {
				sched.Submit(*event.Step) //nolint
			}
			if event.Return != nil {
				sched.SetReturn(event.Return.StepID)
			}
		}
		sched.Seal()
	}()

	return ch
}
