package loom_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bkmashiro/loom/pkg/dag"
	"github.com/bkmashiro/loom/pkg/loom"
	"github.com/bkmashiro/loom/pkg/parser"
)

// noopExecutor is a dag.Executor that returns immediately with no data.
type noopExecutor struct{}

func (noopExecutor) Execute(_ context.Context, step parser.Step, _ map[string]dag.Result) (dag.Result, error) {
	return dag.Result{StepID: step.ID, Data: nil}, nil
}

// buildLinearPlan generates a plan string with n sequential io steps.
// Each step depends on the previous one, forming a linear chain.
func buildLinearPlan(n int, url string) string {
	var sb strings.Builder
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("step%d", i)
		if i == 0 {
			fmt.Fprintf(&sb, "```io %s\nGET %s\n```\n\n", id, url)
		} else {
			prevID := fmt.Sprintf("step%d", i-1)
			fmt.Fprintf(&sb, "```io(%s) %s\nGET %s\n```\n\n", prevID, id, url)
		}
	}
	fmt.Fprintf(&sb, "return step%d\n", n-1)
	return sb.String()
}

// buildParallelPlan generates a plan string with n independent io steps.
func buildParallelPlan(n int, url string) string {
	var sb strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&sb, "```io step%d\nGET %s\n```\n\n", i, url)
	}
	fmt.Fprintf(&sb, "return step0\n")
	return sb.String()
}

// BenchmarkParser_100Steps measures parser throughput over 100 io steps.
// Target: < 1ms per iteration (100 steps in < 1ms).
func BenchmarkParser_100Steps(b *testing.B) {
	plan := buildLinearPlan(100, "http://example.com/data")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := parser.NewParser(strings.NewReader(plan))
		for range p.Events() {
		}
		if err := p.Err(); err != nil {
			b.Fatalf("parser error: %v", err)
		}
		p.Close()
	}
}

// BenchmarkDAG_100Steps_Linear measures scheduling overhead for a linear chain of 100 steps.
// Uses a no-op executor so only DAG overhead is measured.
// Target: < 2ms total scheduling overhead.
func BenchmarkDAG_100Steps_Linear(b *testing.B) {
	exec := noopExecutor{}

	// Pre-build the step list for a linear chain.
	steps := make([]parser.Step, 100)
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("step%d", i)
		var deps []string
		if i > 0 {
			deps = []string{fmt.Sprintf("step%d", i-1)}
		}
		steps[i] = parser.Step{
			ID:   id,
			Type: parser.IO,
			Deps: deps,
			Body: "GET http://example.com/data",
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sched := dag.NewScheduler(context.Background(), exec)
		for _, s := range steps {
			if err := sched.Submit(s); err != nil {
				b.Fatalf("Submit %s: %v", s.ID, err)
			}
		}
		sched.Seal()
		if _, err := sched.Wait(); err != nil {
			b.Fatalf("Wait: %v", err)
		}
	}
}

// BenchmarkLoom_20ParallelIO measures total time to run 20 independent io steps
// against a mock HTTP server that returns instantly.
// Target: < 50ms harness overhead.
func BenchmarkLoom_20ParallelIO(b *testing.B) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	}))
	defer srv.Close()

	plan := buildParallelPlan(20, srv.URL)

	l := loom.New(loom.WithHTTPClient(srv.Client()))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := l.Run(context.Background(), strings.NewReader(plan))
		if err != nil {
			b.Fatalf("Run: %v", err)
		}
	}
}

// BenchmarkDAG_SchedulingOverhead measures per-step scheduling overhead using a no-op executor.
// b.N steps submitted sequentially (no deps).
// Target: < 2,000,000 ns/op (< 2ms per step).
func BenchmarkDAG_SchedulingOverhead(b *testing.B) {
	exec := noopExecutor{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sched := dag.NewScheduler(context.Background(), exec)
		step := parser.Step{
			ID:   fmt.Sprintf("step%d", i),
			Type: parser.IO,
			Body: "GET http://example.com/data",
		}
		if err := sched.Submit(step); err != nil {
			b.Fatalf("Submit: %v", err)
		}
		sched.Seal()
		if _, err := sched.Wait(); err != nil {
			b.Fatalf("Wait: %v", err)
		}
	}
}

// BenchmarkLoom_E2E_20Steps measures end-to-end plan execution with mock HTTP (0ms latency).
// Plan: 15 parallel io + 3 pure merge steps + return.
// Target: < 50ms per iteration.
func BenchmarkLoom_E2E_20Steps(b *testing.B) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "data")
	}))
	defer srv.Close()

	// Build plan: 15 independent io steps, then 3 pure merge steps chaining them, then return.
	var sb strings.Builder
	for i := 0; i < 15; i++ {
		fmt.Fprintf(&sb, "```io io%d\nGET %s\n```\n\n", i, srv.URL)
	}
	// merge0 depends on io0..io4
	fmt.Fprintf(&sb, "```pure(io0, io1, io2, io3, io4) merge0\n${io0}\n```\n\n")
	// merge1 depends on io5..io9
	fmt.Fprintf(&sb, "```pure(io5, io6, io7, io8, io9) merge1\n${io5}\n```\n\n")
	// merge2 depends on io10..io14
	fmt.Fprintf(&sb, "```pure(io10, io11, io12, io13, io14) merge2\n${io10}\n```\n\n")
	// final depends on merge0, merge1, merge2
	fmt.Fprintf(&sb, "```pure(merge0, merge1, merge2) final\n${merge0}\n```\n\n")
	fmt.Fprintf(&sb, "return final\n")

	plan := sb.String()
	l := loom.New(loom.WithHTTPClient(srv.Client()))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := l.Run(context.Background(), strings.NewReader(plan))
		if err != nil {
			b.Fatalf("Run: %v", err)
		}
	}
}
