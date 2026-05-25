// Package main demonstrates the loom parser + DAG working together end-to-end.
//
// It feeds a multi-step plan string through pkg/parser, submits steps to
// pkg/dag, and prints results as they complete via Stream().
package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/bkmashiro/loom/pkg/dag"
	"github.com/bkmashiro/loom/pkg/parser"
)

// plan is the multi-step plan to execute.
const plan = `
` + "```" + `io fetch_user
GET /api/users/42
` + "```" + `

` + "```" + `io fetch_posts
GET /api/posts?user=42
` + "```" + `

` + "```" + `io fetch_recs
POST /ml/recommend {"user": 42, "n": 5}
` + "```" + `

` + "```" + `pure(fetch_user, fetch_posts, fetch_recs) build_feed
merged = fetch_user + fetch_posts + fetch_recs
return merged
` + "```" + `

` + "```" + `async
POST /api/analytics {"action": "feed_view"}
` + "```" + `

return build_feed
`

// stepTypeName returns a short display name for a StepType.
func stepTypeName(t parser.StepType) string {
	switch t {
	case parser.IO:
		return "io"
	case parser.Write:
		return "write"
	case parser.Pure:
		return "pure"
	case parser.Shell:
		return "shell"
	case parser.Async:
		return "async"
	case parser.Escape:
		return "escape"
	default:
		return "unknown"
	}
}

// mockExecutor simulates step execution with canned results.
type mockExecutor struct{}

func (m *mockExecutor) Execute(ctx context.Context, step parser.Step, inputs map[string]dag.Result) (dag.Result, error) {
	fmt.Printf("[loom] executing: %s\n", step.ID)

	switch step.Type {
	case parser.IO:
		// Simulate IO latency.
		select {
		case <-time.After(50 * time.Millisecond):
		case <-ctx.Done():
			return dag.Result{}, ctx.Err()
		}
	case parser.Pure:
		// Simulate computation.
		select {
		case <-time.After(5 * time.Millisecond):
		case <-ctx.Done():
			return dag.Result{}, ctx.Err()
		}
	default:
		// Async and other types: minimal delay.
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			return dag.Result{}, ctx.Err()
		}
	}

	return dag.Result{
		StepID: step.ID,
		Data:   map[string]any{"result": step.ID + "_data"},
	}, nil
}

func main() {
	start := time.Now()

	// Phase 1: parse the full plan and collect all events.
	p := parser.NewParser(strings.NewReader(plan))

	var steps []parser.Step
	var returnID string
	for ev := range p.Events() {
		switch {
		case ev.Step != nil:
			steps = append(steps, *ev.Step)
		case ev.Return != nil:
			returnID = ev.Return.StepID
		}
	}
	if err := p.Err(); err != nil {
		fmt.Printf("parse error: %v\n", err)
		return
	}

	// Phase 2: print what we're about to submit.
	for _, step := range steps {
		if len(step.Deps) == 0 {
			fmt.Printf("[loom] submitting: %s (%s)\n", step.ID, stepTypeName(step.Type))
		} else {
			fmt.Printf("[loom] submitting: %s (%s) deps=%v\n", step.ID, stepTypeName(step.Type), step.Deps)
		}
	}

	// Phase 3: build the scheduler and submit all steps.
	// Stream() is called AFTER all steps are submitted so that wg is non-zero
	// before the stream's wg.Wait() goroutine runs.
	ctx := context.Background()
	exec := &mockExecutor{}
	sched := dag.NewScheduler(ctx, exec)

	for _, step := range steps {
		if err := sched.Submit(step); err != nil {
			fmt.Printf("submit %q: %v\n", step.ID, err)
			return
		}
	}
	if returnID != "" {
		sched.SetReturn(returnID)
	}

	// Phase 4: stream results as they complete.
	// Stream() is called after all submits so wg > 0 and the channel stays
	// open until all goroutines finish.
	stream := sched.Stream()

	for sr := range stream {
		elapsed := time.Since(start)
		if sr.Err != nil {
			fmt.Printf("✗ %s failed [%v]: %v\n", sr.StepID, elapsed.Round(time.Millisecond), sr.Err)
		} else {
			fmt.Printf("✓ %s completed [%v]\n", sr.StepID, elapsed.Round(time.Millisecond))
		}
	}

	// Phase 5: retrieve and print the final return step's result.
	finalRes, err := sched.Wait()
	if err != nil {
		fmt.Printf("\nERROR waiting for result: %v\n", err)
		return
	}

	fmt.Printf("\n=== Final result ===\n")
	fmt.Printf("step:   %s\n", finalRes.StepID)
	fmt.Printf("data:   %v\n", finalRes.Data)
	fmt.Printf("total:  %v\n", time.Since(start).Round(time.Millisecond))
}
