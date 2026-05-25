package dag

import (
	"context"
	"errors"
	"sync"

	"github.com/bkmashiro/loom/pkg/parser"
)

// Sentinel errors
var (
	ErrDuplicateStep      = errors.New("dag: duplicate step ID")
	ErrForwardRef         = errors.New("dag: dependency references unknown step (forward references not allowed)") // kept for compatibility; Submit no longer returns this
	ErrCycle              = errors.New("dag: dependency cycle detected")
	ErrReturnStepNotFound = errors.New("dag: return step was never submitted")
	ErrReturnCancelled    = errors.New("dag: return step was cancelled due to upstream failure")
)

// Result holds the outcome of a step execution.
type Result struct {
	StepID string
	Data   any
	Err    error
}

// StepResult is emitted on the Stream channel as each step completes.
type StepResult struct {
	Result
	Pending   int
	Completed int
}

// Executor is the interface the scheduler uses to run steps.
type Executor interface {
	Execute(ctx context.Context, step parser.Step, inputs map[string]Result) (Result, error)
}

type nodeState int

const (
	nodeQueued    nodeState = iota
	nodeRunning
	nodeComplete
	nodeCancelled
)

type node struct {
	step       parser.Step
	deps       map[string]struct{} // set of dep IDs
	dependents []string            // step IDs that depend on this node
	state      nodeState
	result     Result
}

// Scheduler accepts steps, tracks dependencies, and dispatches for parallel execution.
type Scheduler struct {
	ctx    context.Context
	cancel context.CancelFunc
	exec   Executor

	mu        sync.Mutex
	cond      *sync.Cond // broadcast when any step completes
	nodes     map[string]*node
	awaiters  map[string][]string // dep step ID → list of step IDs waiting for it to be registered
	completed int
	pending   int // queued + running (decremented on complete/cancel)
	sealed    bool

	returnStepID string

	// streamSubs holds subscriber channels for Stream()
	streamMu   sync.Mutex
	streamSubs []chan StepResult

	wg sync.WaitGroup
}

// NewScheduler creates a scheduler that dispatches to exec.
func NewScheduler(ctx context.Context, exec Executor) *Scheduler {
	s := &Scheduler{
		ctx:      ctx,
		exec:     exec,
		nodes:    make(map[string]*node),
		awaiters: make(map[string][]string),
	}
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.cond = sync.NewCond(&s.mu)
	return s
}

// Submit adds a step to the DAG. Dispatches immediately if all deps satisfied.
// Forward references (deps not yet submitted) are allowed — the node is queued
// until all deps exist and are complete.
// Returns ErrDuplicateStep or ErrCycle on error.
func (s *Scheduler) Submit(step parser.Step) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.nodes[step.ID]; exists {
		return ErrDuplicateStep
	}

	// Build dep set; record forward references in awaiters.
	depSet := make(map[string]struct{}, len(step.Deps))
	for _, dep := range step.Deps {
		depSet[dep] = struct{}{}
		if _, ok := s.nodes[dep]; !ok {
			// Forward reference — register this step as an awaiter of dep.
			s.awaiters[dep] = append(s.awaiters[dep], step.ID)
		}
	}

	n := &node{
		step:  step,
		deps:  depSet,
		state: nodeQueued,
	}
	s.nodes[step.ID] = n

	// Wire up reverse deps for already-known deps.
	for dep := range depSet {
		if depNode, ok := s.nodes[dep]; ok {
			depNode.dependents = append(depNode.dependents, step.ID)
		}
	}

	// Cycle detection through known nodes (unknown deps are simply skipped).
	if err := s.detectCycle(step.ID); err != nil {
		// Rollback: remove the node and any reverse-dep wiring we just did.
		delete(s.nodes, step.ID)
		for dep := range depSet {
			if depNode, ok := s.nodes[dep]; ok {
				for i, d := range depNode.dependents {
					if d == step.ID {
						depNode.dependents = append(depNode.dependents[:i], depNode.dependents[i+1:]...)
						break
					}
				}
			}
			// Also remove from awaiters if we added a forward ref entry.
			waiters := s.awaiters[dep]
			for i, w := range waiters {
				if w == step.ID {
					s.awaiters[dep] = append(waiters[:i], waiters[i+1:]...)
					break
				}
			}
		}
		return err
	}

	s.pending++

	// Resolve awaiters: steps that were waiting for step.ID to be registered.
	if waiters, ok := s.awaiters[step.ID]; ok {
		delete(s.awaiters, step.ID)
		for _, waiterID := range waiters {
			waiterNode := s.nodes[waiterID]
			if waiterNode != nil {
				// Wire up the reverse dependent relationship now that this dep is known.
				n.dependents = append(n.dependents, waiterID)
			}
		}
	}

	// Dispatch if all deps already complete (handles zero-dep and all-satisfied cases).
	if s.allDepsComplete(n) {
		s.dispatch(n)
	}

	return nil
}

// detectCycle performs DFS from startID through its deps to detect if startID is reachable.
// Must be called with s.mu held.
func (s *Scheduler) detectCycle(startID string) error {
	visited := make(map[string]bool)
	var dfs func(id string) bool
	dfs = func(id string) bool {
		if id == startID {
			return true
		}
		if visited[id] {
			return false
		}
		visited[id] = true
		n := s.nodes[id]
		if n == nil {
			return false
		}
		for dep := range n.deps {
			if dfs(dep) {
				return true
			}
		}
		return false
	}

	n := s.nodes[startID]
	for dep := range n.deps {
		if dfs(dep) {
			return ErrCycle
		}
	}
	return nil
}

// allDepsComplete returns true if all deps of n are in nodeComplete state.
// Must be called with s.mu held.
func (s *Scheduler) allDepsComplete(n *node) bool {
	for dep := range n.deps {
		depNode := s.nodes[dep]
		if depNode == nil || depNode.state != nodeComplete {
			return false
		}
	}
	return true
}

// collectInputs gathers the results of all deps of n.
// Must be called with s.mu held.
func (s *Scheduler) collectInputs(n *node) map[string]Result {
	inputs := make(map[string]Result, len(n.deps))
	for dep := range n.deps {
		inputs[dep] = s.nodes[dep].result
	}
	return inputs
}

// publishToStream sends a StepResult to all stream subscribers (best-effort, non-blocking).
// Uses recover to safely handle sends to channels that were closed concurrently by
// the Stream() goroutine between the subscriber snapshot and the actual send.
func (s *Scheduler) publishToStream(sr StepResult) {
	s.streamMu.Lock()
	subs := s.streamSubs
	s.streamMu.Unlock()
	for _, ch := range subs {
		streamSafeSend(ch, sr)
	}
}

// streamSafeSend attempts a non-blocking send to ch.
// If ch is closed (race with Stream() goroutine), the panic is recovered silently.
func streamSafeSend(ch chan StepResult, sr StepResult) {
	defer func() { recover() }() //nolint:errcheck
	select {
	case ch <- sr:
	default:
		// Channel full — drop rather than block.
	}
}

// dispatch starts executing a node in a goroutine.
// Must be called with s.mu held.
func (s *Scheduler) dispatch(n *node) {
	n.state = nodeRunning
	inputs := s.collectInputs(n)
	step := n.step

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		result, err := s.exec.Execute(s.ctx, step, inputs)
		if err != nil {
			result = Result{StepID: step.ID, Err: err}
		}
		if result.StepID == "" {
			result.StepID = step.ID
		}

		s.mu.Lock()
		n.state = nodeComplete
		n.result = result
		s.completed++
		s.pending--
		completed := s.completed
		pending := s.pending
		hasFailed := result.Err != nil
		dependents := make([]string, len(n.dependents))
		copy(dependents, n.dependents)
		var cancelledResults []StepResult
		if hasFailed {
			cancelledResults = s.cancelTransitive(n)
		}
		s.cond.Broadcast()
		s.mu.Unlock()

		// Publish to stream subscribers (outside lock)
		s.publishToStream(StepResult{
			Result:    result,
			Pending:   pending,
			Completed: completed,
		})
		for _, cr := range cancelledResults {
			s.publishToStream(cr)
		}

		if !hasFailed {
			// Check each dependent to see if it's now ready
			for _, depID := range dependents {
				s.mu.Lock()
				depNode := s.nodes[depID]
				if depNode != nil && depNode.state == nodeQueued && s.allDepsComplete(depNode) {
					s.dispatch(depNode)
				}
				s.mu.Unlock()
			}
		}
	}()
}

// cancelTransitive cancels all queued transitive dependents of n.
// Must be called with s.mu held.
// Returns a slice of StepResults for cancelled nodes (to be published outside the lock).
func (s *Scheduler) cancelTransitive(n *node) []StepResult {
	var cancelled []StepResult
	for _, depID := range n.dependents {
		depNode := s.nodes[depID]
		if depNode == nil {
			continue
		}
		if depNode.state == nodeQueued {
			depNode.state = nodeCancelled
			depNode.result = Result{
				StepID: depID,
				Err:    ErrReturnCancelled,
			}
			s.pending--
			s.completed++

			cancelled = append(cancelled, StepResult{
				Result:    depNode.result,
				Pending:   s.pending,
				Completed: s.completed,
			})

			// Recurse
			cancelled = append(cancelled, s.cancelTransitive(depNode)...)
		}
	}
	return cancelled
}

// SetReturn declares which step's result is the final output.
func (s *Scheduler) SetReturn(stepID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.returnStepID = stepID
}

// Wait blocks until the return step completes (or all steps if no return set).
func (s *Scheduler) Wait() (Result, error) {
	// Start a single goroutine that broadcasts when context is cancelled,
	// so cond.Wait() can unblock in the loop below.
	ctxWake := make(chan struct{})
	go func() {
		select {
		case <-s.ctx.Done():
			s.cond.Broadcast()
		case <-ctxWake:
		}
	}()
	defer close(ctxWake)

	s.mu.Lock()
	defer s.mu.Unlock()

	returnID := s.returnStepID

	if returnID != "" {
		// Wait until the return node is complete or cancelled, or context done
		for {
			if n, ok := s.nodes[returnID]; ok {
				if n.state == nodeComplete || n.state == nodeCancelled {
					return n.result, n.result.Err
				}
			}
			// Check context
			select {
			case <-s.ctx.Done():
				return Result{}, s.ctx.Err()
			default:
			}
			s.cond.Wait()
		}
	}

	// No return step — wait for all pending work to finish
	for s.pending > 0 {
		select {
		case <-s.ctx.Done():
			return Result{}, s.ctx.Err()
		default:
		}
		s.cond.Wait()
	}
	return Result{}, nil
}

// Seal signals that no more steps will be submitted.
// Stream() will close its channel only after Seal() is called and all pending steps complete.
func (s *Scheduler) Seal() {
	s.mu.Lock()
	s.sealed = true
	s.cond.Broadcast()
	s.mu.Unlock()
}

// Stream returns a channel of StepResults emitted as each step completes.
// Channel is closed when Seal() has been called and all steps are done, or context cancelled.
func (s *Scheduler) Stream() <-chan StepResult {
	ch := make(chan StepResult, 512)

	s.streamMu.Lock()
	s.streamSubs = append(s.streamSubs, ch)
	s.streamMu.Unlock()

	go func() {
		defer close(ch)
		s.mu.Lock()
		defer s.mu.Unlock()
		for {
			if s.sealed && s.pending == 0 {
				return
			}
			select {
			case <-s.ctx.Done():
				return
			default:
			}
			s.cond.Wait()
		}
	}()

	return ch
}

// Cancel aborts all in-flight executions.
func (s *Scheduler) Cancel() {
	s.cancel()
}
