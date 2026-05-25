package dispatch

import (
	"context"
	"errors"

	"github.com/bkmashiro/loom/pkg/dag"
	"github.com/bkmashiro/loom/pkg/parser"
)

var ErrNoWorker = errors.New("dispatch: no worker available for step")

// Capability describes what a worker can handle.
type Capability struct {
	Types []parser.StepType // step types this worker handles; nil = all types
	Langs []string          // lang suffixes (e.g. "python", "torch"); nil = all langs
	// PlacementHint is an optional tag steps can use to prefer this worker.
	PlacementHint string
}

// Worker is any executor with declared capabilities.
type Worker interface {
	Capabilities() Capability
	Execute(ctx context.Context, step parser.Step, inputs map[string]dag.Result) (dag.Result, error)
}

// Dispatcher routes steps to the appropriate worker.
type Dispatcher struct {
	workers []Worker
}

// New creates an empty Dispatcher.
func New() *Dispatcher { return &Dispatcher{} }

// Register adds a worker. Workers are checked in registration order.
func (d *Dispatcher) Register(w Worker) {
	d.workers = append(d.workers, w)
}

// Execute implements dag.Executor — routes the step to the first matching worker.
func (d *Dispatcher) Execute(ctx context.Context, step parser.Step, inputs map[string]dag.Result) (dag.Result, error) {
	for _, w := range d.workers {
		if canHandle(w, step) {
			return w.Execute(ctx, step, inputs)
		}
	}
	return dag.Result{StepID: step.ID, Err: ErrNoWorker}, ErrNoWorker
}

// canHandle returns true if w's capabilities match step.
func canHandle(w Worker, step parser.Step) bool {
	cap := w.Capabilities()
	// nil Types = handles all types
	if cap.Types != nil {
		found := false
		for _, t := range cap.Types {
			if t == step.Type {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	// nil Langs = handles all langs
	if cap.Langs != nil && step.Lang != "" {
		found := false
		for _, l := range cap.Langs {
			if l == step.Lang {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
