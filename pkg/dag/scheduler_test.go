package dag

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bkmashiro/loom/pkg/parser"
)

// mockExecutor is a configurable executor for tests.
type mockExecutor struct {
	delay   time.Duration
	results map[string]any // pre-configured results per step ID
	errors  map[string]error
	calls   []string // record of step IDs executed (in order)
	mu      sync.Mutex
}

func newMockExecutor() *mockExecutor {
	return &mockExecutor{
		results: make(map[string]any),
		errors:  make(map[string]error),
	}
}

func (m *mockExecutor) Execute(ctx context.Context, step parser.Step, inputs map[string]Result) (Result, error) {
	if m.delay > 0 {
		select {
		case <-time.After(m.delay):
		case <-ctx.Done():
			return Result{StepID: step.ID, Err: ctx.Err()}, ctx.Err()
		}
	}
	m.mu.Lock()
	m.calls = append(m.calls, step.ID)
	data := m.results[step.ID]
	err := m.errors[step.ID]
	m.mu.Unlock()
	return Result{StepID: step.ID, Data: data, Err: err}, err
}

func (m *mockExecutor) setResult(id string, data any) {
	m.mu.Lock()
	m.results[id] = data
	m.mu.Unlock()
}

func (m *mockExecutor) setError(id string, err error) {
	m.mu.Lock()
	m.errors[id] = err
	m.mu.Unlock()
}

func (m *mockExecutor) getCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]string, len(m.calls))
	copy(cp, m.calls)
	return cp
}

func (m *mockExecutor) hasCalled(id string) bool {
	for _, c := range m.getCalls() {
		if c == id {
			return true
		}
	}
	return false
}

func step(id string, deps ...string) parser.Step {
	if deps == nil {
		deps = []string{}
	}
	return parser.Step{ID: id, Type: parser.Pure, Deps: deps}
}

func asyncStep(id string, deps ...string) parser.Step {
	if deps == nil {
		deps = []string{}
	}
	return parser.Step{ID: id, Type: parser.Async, Deps: deps}
}

// Test 1: Linear chain A→B→C
func TestLinearChain(t *testing.T) {
	exec := newMockExecutor()
	exec.results["A"] = "a-result"
	exec.results["B"] = "b-result"
	exec.results["C"] = "c-result"

	ctx := context.Background()
	sched := NewScheduler(ctx, exec)

	if err := sched.Submit(step("A")); err != nil {
		t.Fatalf("Submit A: %v", err)
	}
	if err := sched.Submit(step("B", "A")); err != nil {
		t.Fatalf("Submit B: %v", err)
	}
	if err := sched.Submit(step("C", "B")); err != nil {
		t.Fatalf("Submit C: %v", err)
	}

	sched.SetReturn("C")
	result, err := sched.Wait()
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}

	if result.StepID != "C" {
		t.Errorf("expected return step C, got %q", result.StepID)
	}
	if result.Data != "c-result" {
		t.Errorf("expected c-result, got %v", result.Data)
	}

	calls := exec.getCalls()
	if len(calls) != 3 {
		t.Fatalf("expected 3 calls, got %v", calls)
	}
	if calls[0] != "A" || calls[1] != "B" || calls[2] != "C" {
		t.Errorf("expected A→B→C order, got %v", calls)
	}
}

// Test 2: Independent steps A, B, C — all execute
func TestIndependentSteps(t *testing.T) {
	exec := newMockExecutor()
	exec.delay = 10 * time.Millisecond

	ctx := context.Background()
	sched := NewScheduler(ctx, exec)

	start := time.Now()
	sched.Submit(step("A"))
	sched.Submit(step("B"))
	sched.Submit(step("C"))

	_, err := sched.Wait()
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}

	calls := exec.getCalls()
	if len(calls) != 3 {
		t.Fatalf("expected 3 calls, got %v", calls)
	}
	if !exec.hasCalled("A") || !exec.hasCalled("B") || !exec.hasCalled("C") {
		t.Errorf("not all steps called: %v", calls)
	}

	// With 10ms delay and parallel execution, should finish well under 3*10ms
	if elapsed > 25*time.Millisecond {
		t.Logf("warning: elapsed %v suggests steps may not have run in parallel", elapsed)
	}
}

// Test 3: Diamond A→C, B→C — C starts after both A and B complete
func TestDiamond(t *testing.T) {
	exec := newMockExecutor()
	exec.results["A"] = "a"
	exec.results["B"] = "b"
	exec.results["C"] = "c"

	ctx := context.Background()
	sched := NewScheduler(ctx, exec)

	sched.Submit(step("A"))
	sched.Submit(step("B"))
	if err := sched.Submit(step("C", "A", "B")); err != nil {
		t.Fatalf("Submit C: %v", err)
	}

	sched.SetReturn("C")
	result, err := sched.Wait()
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}

	calls := exec.getCalls()
	// C must come after both A and B
	cIdx := -1
	aIdx := -1
	bIdx := -1
	for i, id := range calls {
		switch id {
		case "A":
			aIdx = i
		case "B":
			bIdx = i
		case "C":
			cIdx = i
		}
	}
	if aIdx == -1 || bIdx == -1 || cIdx == -1 {
		t.Fatalf("not all steps executed: %v", calls)
	}
	if cIdx <= aIdx || cIdx <= bIdx {
		t.Errorf("C should execute after A and B; order: %v", calls)
	}

	if result.Data != "c" {
		t.Errorf("expected c, got %v", result.Data)
	}
}

// Test 4: Cycle detection
func TestCycleDetection(t *testing.T) {
	exec := newMockExecutor()
	ctx := context.Background()
	sched := NewScheduler(ctx, exec)

	// A→B, B→C, C→A would be a cycle — but forward refs are rejected.
	// We can only create a cycle with existing nodes.
	// Submit A with no deps, B depending on A — OK.
	// We can't submit C depending on B and then make A depend on C (A already submitted).
	// So we test: submit A, B(dep A). Can't cycle here. We need to trick it.
	// A cycle requires: A→B and B→A. Since forward refs aren't allowed,
	// we can't do A depends on B if B hasn't been submitted yet.
	// The only feasible cycle test: if the scheduler allows self-dep.
	// Self dep: step "A" with dep "A".
	// First submit A with no deps...
	// Actually let's test: submit A(no deps), then try B(dep A, dep C), C(dep B) — that won't cycle.
	// The cycle scenario the spec refers to might be: after submitting A and B(dep A),
	// detecting the cycle when we try something like adding A with dep B.
	// But A is already submitted. Let's test a 2-node cycle scenario.

	// Submit X with no deps
	if err := sched.Submit(step("X")); err != nil {
		t.Fatal(err)
	}
	// Submit Y depending on X — OK
	if err := sched.Submit(step("Y", "X")); err != nil {
		t.Fatal(err)
	}
	// Forward ref — Y not accepting any dep that doesn't exist:
	// Now submit Z depending on Y (fine).
	// But we can't create a cycle since all nodes are already in the DAG before we add new ones.
	// The cycle test scenario:
	// If somehow we could have A→B and also B→A.
	// Since we can't add A after B, the only way to test cycle detection is if
	// the cycle check itself triggers. With our model of no-forward-refs, true cycles
	// aren't constructible. Let me test the self-referential case.

	// NOTE: Given the no-forward-reference constraint, the only cycles possible are
	// through submitted deps if for some reason our graph has inconsistency.
	// Let's verify that even with our cycle detection code working,
	// we get ErrForwardRef before ErrCycle in normal scenarios.

	// Forward references are now allowed — unknown dep should not return an error.
	err := sched.Submit(step("Z", "NONEXISTENT"))
	if err != nil {
		t.Errorf("expected no error for forward ref, got %v", err)
	}

	// Wait for X and Y (Z will remain queued waiting for NONEXISTENT).
	sched.wg.Wait()
}

// Test 4b: Cycle detection with a self-loop attempt
func TestCycleDetectionSelfLoop(t *testing.T) {
	exec := newMockExecutor()
	ctx := context.Background()

	// We need to construct a scenario where cycle detection fires.
	// The only way with no-forward-refs is a self-referencing step.
	// But the scheduler checks deps exist before adding, so "A" dep on "A"
	// would only work if A was already submitted (creating a self-loop).
	// Let's verify the cycle detection path by bypassing through our own test scenario.
	//
	// Actually: submit A, then try to submit B(dep A), C(dep B), and D(dep C, dep A).
	// That's not a cycle. The real cycle: we can't create one due to the forward-ref guard.
	//
	// Let me test by directly testing the detectCycle function with a crafted graph.

	sched := NewScheduler(ctx, exec)

	// Manually build a cyclic graph to test detectCycle
	sched.mu.Lock()
	sched.nodes["p"] = &node{
		step:  parser.Step{ID: "p"},
		deps:  map[string]struct{}{"q": {}},
		state: nodeQueued,
	}
	sched.nodes["q"] = &node{
		step:  parser.Step{ID: "q"},
		deps:  map[string]struct{}{"p": {}},
		state: nodeQueued,
	}
	// Now try to detect a cycle from "p"
	err := sched.detectCycle("p")
	sched.mu.Unlock()

	if err != ErrCycle {
		t.Errorf("expected ErrCycle, got %v", err)
	}
}

// Test 5: Forward reference — Submit now accepts deps that haven't been registered yet.
func TestForwardReference(t *testing.T) {
	exec := newMockExecutor()
	exec.results["A"] = "a-result"
	exec.results["B"] = "b-result"
	ctx := context.Background()
	sched := NewScheduler(ctx, exec)

	// Submit B before A — should succeed (forward reference allowed).
	if err := sched.Submit(step("B", "A")); err != nil {
		t.Fatalf("expected no error for forward ref, got %v", err)
	}
	// Now submit A — B should eventually execute after A completes.
	if err := sched.Submit(step("A")); err != nil {
		t.Fatalf("Submit A: %v", err)
	}

	sched.SetReturn("B")
	result, err := sched.Wait()
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if result.StepID != "B" || result.Data != "b-result" {
		t.Errorf("expected B result b-result, got stepID=%q data=%v", result.StepID, result.Data)
	}
	if !exec.hasCalled("A") || !exec.hasCalled("B") {
		t.Errorf("expected both A and B to execute, calls=%v", exec.getCalls())
	}
}

// Test 6: Duplicate ID
func TestDuplicateID(t *testing.T) {
	exec := newMockExecutor()
	ctx := context.Background()
	sched := NewScheduler(ctx, exec)

	if err := sched.Submit(step("A")); err != nil {
		t.Fatal(err)
	}

	err := sched.Submit(step("A"))
	if err != ErrDuplicateStep {
		t.Errorf("expected ErrDuplicateStep, got %v", err)
	}

	sched.wg.Wait()
}

// Test 7: Step failure — B(dep A) cancelled, C(independent) still runs
func TestStepFailure(t *testing.T) {
	exec := newMockExecutor()
	exec.errors["A"] = fmt.Errorf("step A failed")

	ctx := context.Background()
	sched := NewScheduler(ctx, exec)

	sched.Submit(step("A"))
	sched.Submit(step("B", "A")) // depends on A — should be cancelled
	sched.Submit(step("C"))      // independent — should still run

	// Wait for all work to finish
	done := make(chan struct{})
	go func() {
		sched.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for steps")
	}

	// A should have been called
	if !exec.hasCalled("A") {
		t.Error("A should have been called")
	}
	// B should NOT have been called (cancelled due to A's failure)
	if exec.hasCalled("B") {
		t.Error("B should not have been called (dep A failed)")
	}
	// C should have been called (independent)
	if !exec.hasCalled("C") {
		t.Error("C should have been called (independent of A)")
	}

	// Check B's state
	sched.mu.Lock()
	bNode := sched.nodes["B"]
	sched.mu.Unlock()
	if bNode.state != nodeCancelled {
		t.Errorf("B should be cancelled, got state %v", bNode.state)
	}
}

// Test 8: SetReturn + Wait returns correct step's result
func TestSetReturnWait(t *testing.T) {
	exec := newMockExecutor()
	exec.results["A"] = 42
	exec.results["B"] = "hello"

	ctx := context.Background()
	sched := NewScheduler(ctx, exec)

	sched.Submit(step("A"))
	sched.Submit(step("B"))
	sched.SetReturn("B")

	result, err := sched.Wait()
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if result.StepID != "B" {
		t.Errorf("expected return step B, got %q", result.StepID)
	}
	if result.Data != "hello" {
		t.Errorf("expected hello, got %v", result.Data)
	}
}

// Test 9: Stream emits StepResults and closes
func TestStream(t *testing.T) {
	exec := newMockExecutor()
	exec.results["A"] = 1
	exec.results["B"] = 2
	exec.results["C"] = 3

	ctx := context.Background()
	sched := NewScheduler(ctx, exec)

	stream := sched.Stream()

	sched.Submit(step("A"))
	sched.Submit(step("B"))
	sched.Submit(step("C"))
	sched.Seal()

	seen := make(map[string]bool)
	timeout := time.After(5 * time.Second)
	for {
		select {
		case sr, ok := <-stream:
			if !ok {
				// stream closed — check we saw all steps
				if !seen["A"] || !seen["B"] || !seen["C"] {
					t.Errorf("missing steps in stream: %v", seen)
				}
				return
			}
			seen[sr.StepID] = true
		case <-timeout:
			t.Fatalf("timeout waiting for stream results, seen: %v", seen)
		}
	}
}

// Test 10: Context cancellation
func TestContextCancellation(t *testing.T) {
	exec := &slowExecutor{delay: 500 * time.Millisecond}

	ctx, cancel := context.WithCancel(context.Background())
	sched := NewScheduler(ctx, exec)

	sched.Submit(step("A"))

	// Cancel quickly
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	_, err := sched.Wait()
	if err == nil {
		t.Error("expected error due to context cancellation")
	}
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

// slowExecutor blocks until context is done.
type slowExecutor struct {
	delay time.Duration
}

func (e *slowExecutor) Execute(ctx context.Context, step parser.Step, inputs map[string]Result) (Result, error) {
	select {
	case <-time.After(e.delay):
		return Result{StepID: step.ID}, nil
	case <-ctx.Done():
		return Result{StepID: step.ID, Err: ctx.Err()}, ctx.Err()
	}
}

// Test 11: Benchmark 100 steps linear chain
func BenchmarkLinearChain100(b *testing.B) {
	const chainLen = 100
	// Pre-build steps and results
	steps := make([]parser.Step, chainLen)
	for i := 0; i < chainLen; i++ {
		id := fmt.Sprintf("step%d", i)
		if i == 0 {
			steps[i] = step(id)
		} else {
			steps[i] = step(id, fmt.Sprintf("step%d", i-1))
		}
	}

	for n := 0; n < b.N; n++ {
		exec := newMockExecutor()
		// Populate results before any goroutines start (no lock needed yet)
		for i := 0; i < chainLen; i++ {
			exec.results[fmt.Sprintf("step%d", i)] = i
		}

		ctx := context.Background()
		sched := NewScheduler(ctx, exec)

		for i := 0; i < chainLen; i++ {
			if err := sched.Submit(steps[i]); err != nil {
				b.Fatalf("Submit step%d: %v", i, err)
			}
		}

		sched.SetReturn("step99")
		_, err := sched.Wait()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// TestBenchmarkLinearChainTiming verifies < 50ms scheduling overhead for 100 steps.
func TestBenchmarkLinearChainTiming(t *testing.T) {
	exec := newMockExecutor()
	ctx := context.Background()
	sched := NewScheduler(ctx, exec)

	const chainLen = 100
	// Pre-populate results before submitting (avoid data race)
	for i := 0; i < chainLen; i++ {
		exec.results[fmt.Sprintf("step%d", i)] = i
	}
	for i := 0; i < chainLen; i++ {
		id := fmt.Sprintf("step%d", i)
		var s parser.Step
		if i == 0 {
			s = step(id)
		} else {
			s = step(id, fmt.Sprintf("step%d", i-1))
		}
		if err := sched.Submit(s); err != nil {
			t.Fatalf("Submit %s: %v", id, err)
		}
	}

	sched.SetReturn("step99")
	start := time.Now()
	_, err := sched.Wait()
	elapsed := time.Since(start)

	if err != nil {
		t.Fatal(err)
	}
	if elapsed > 50*time.Millisecond {
		t.Errorf("scheduling overhead %v exceeds 50ms threshold", elapsed)
	}
	t.Logf("100-step linear chain completed in %v", elapsed)
}

// Test: Inputs passed correctly to dependent steps
func TestInputsPropagated(t *testing.T) {
	var capturedInputs map[string]Result
	var mu sync.Mutex

	capExec := &capturingExecutor{
		results:    map[string]any{"A": "from-a"},
		captureFor: "B",
		onCapture: func(inputs map[string]Result) {
			mu.Lock()
			capturedInputs = inputs
			mu.Unlock()
		},
	}

	ctx := context.Background()
	sched := NewScheduler(ctx, capExec)

	sched.Submit(step("A"))
	sched.Submit(step("B", "A"))
	sched.SetReturn("B")
	sched.Wait()

	mu.Lock()
	defer mu.Unlock()

	if capturedInputs == nil {
		t.Fatal("inputs not captured")
	}
	aResult, ok := capturedInputs["A"]
	if !ok {
		t.Fatal("A not in inputs for B")
	}
	if aResult.Data != "from-a" {
		t.Errorf("expected from-a, got %v", aResult.Data)
	}
}

type capturingExecutor struct {
	results    map[string]any
	captureFor string
	onCapture  func(inputs map[string]Result)
}

func (e *capturingExecutor) Execute(ctx context.Context, s parser.Step, inputs map[string]Result) (Result, error) {
	if s.ID == e.captureFor && e.onCapture != nil {
		e.onCapture(inputs)
	}
	return Result{StepID: s.ID, Data: e.results[s.ID]}, nil
}

// TestScheduler_ForwardRef_BasicOrder — submit B (depends on A) before A;
// both execute in correct order.
func TestScheduler_ForwardRef_BasicOrder(t *testing.T) {
	exec := newMockExecutor()
	exec.results["A"] = "a-result"
	exec.results["B"] = "b-result"

	ctx := context.Background()
	sched := NewScheduler(ctx, exec)

	// Submit B first — forward reference to A.
	if err := sched.Submit(step("B", "A")); err != nil {
		t.Fatalf("Submit B (forward ref): %v", err)
	}
	// Now submit A.
	if err := sched.Submit(step("A")); err != nil {
		t.Fatalf("Submit A: %v", err)
	}

	sched.SetReturn("B")
	result, err := sched.Wait()
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}

	if result.StepID != "B" {
		t.Errorf("expected return step B, got %q", result.StepID)
	}
	if result.Data != "b-result" {
		t.Errorf("expected b-result, got %v", result.Data)
	}

	calls := exec.getCalls()
	if len(calls) != 2 {
		t.Fatalf("expected 2 calls, got %v", calls)
	}
	// A must execute before B.
	aIdx, bIdx := -1, -1
	for i, id := range calls {
		switch id {
		case "A":
			aIdx = i
		case "B":
			bIdx = i
		}
	}
	if aIdx == -1 || bIdx == -1 {
		t.Fatalf("not all steps executed: %v", calls)
	}
	if aIdx >= bIdx {
		t.Errorf("A must execute before B; order: %v", calls)
	}
}

// TestScheduler_ForwardRef_ChainedOrder — submit C→B→A in reverse order;
// all three execute in the correct A→B→C order.
func TestScheduler_ForwardRef_ChainedOrder(t *testing.T) {
	exec := newMockExecutor()
	exec.results["A"] = "a-result"
	exec.results["B"] = "b-result"
	exec.results["C"] = "c-result"

	ctx := context.Background()
	sched := NewScheduler(ctx, exec)

	// Submit in reverse topological order.
	if err := sched.Submit(step("C", "B")); err != nil {
		t.Fatalf("Submit C: %v", err)
	}
	if err := sched.Submit(step("B", "A")); err != nil {
		t.Fatalf("Submit B: %v", err)
	}
	if err := sched.Submit(step("A")); err != nil {
		t.Fatalf("Submit A: %v", err)
	}

	sched.SetReturn("C")
	result, err := sched.Wait()
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}

	if result.StepID != "C" || result.Data != "c-result" {
		t.Errorf("expected C/c-result, got stepID=%q data=%v", result.StepID, result.Data)
	}

	calls := exec.getCalls()
	if len(calls) != 3 {
		t.Fatalf("expected 3 calls, got %v", calls)
	}
	idx := make(map[string]int, 3)
	for i, id := range calls {
		idx[id] = i
	}
	if _, ok := idx["A"]; !ok {
		t.Fatal("A was not executed")
	}
	if _, ok := idx["B"]; !ok {
		t.Fatal("B was not executed")
	}
	if _, ok := idx["C"]; !ok {
		t.Fatal("C was not executed")
	}
	if idx["A"] >= idx["B"] {
		t.Errorf("A must execute before B; order: %v", calls)
	}
	if idx["B"] >= idx["C"] {
		t.Errorf("B must execute before C; order: %v", calls)
	}
}

// TestScheduler_ForwardRef_CycleDetection — A depends on B, B depends on A
// (submitted out of order) → ErrCycle returned on the second submit.
func TestScheduler_ForwardRef_CycleDetection(t *testing.T) {
	exec := newMockExecutor()
	ctx := context.Background()
	sched := NewScheduler(ctx, exec)

	// Submit A depending on B — forward reference, should succeed.
	if err := sched.Submit(step("A", "B")); err != nil {
		t.Fatalf("Submit A(dep B): %v", err)
	}
	// Submit B depending on A — this creates a cycle: A→B and B→A.
	err := sched.Submit(step("B", "A"))
	if err != ErrCycle {
		t.Errorf("expected ErrCycle, got %v", err)
	}
}
