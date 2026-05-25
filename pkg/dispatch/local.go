package dispatch

import (
	"context"

	"github.com/bkmashiro/loom/pkg/dag"
	"github.com/bkmashiro/loom/pkg/parser"
)

// LocalWorker wraps an existing dag.Executor with declared capabilities.
type LocalWorker struct {
	exec dag.Executor
	cap  Capability
}

// NewLocalWorker creates a LocalWorker with the given executor and capabilities.
func NewLocalWorker(exec dag.Executor, cap Capability) *LocalWorker {
	return &LocalWorker{exec: exec, cap: cap}
}

// Capabilities returns the worker's declared capabilities.
func (w *LocalWorker) Capabilities() Capability { return w.cap }

// Execute delegates to the underlying dag.Executor.
func (w *LocalWorker) Execute(ctx context.Context, step parser.Step, inputs map[string]dag.Result) (dag.Result, error) {
	return w.exec.Execute(ctx, step, inputs)
}

// DefaultLocalWorker creates a LocalWorker that handles all step types and langs.
func DefaultLocalWorker(exec dag.Executor) *LocalWorker {
	return NewLocalWorker(exec, Capability{}) // nil Types + nil Langs = all
}
