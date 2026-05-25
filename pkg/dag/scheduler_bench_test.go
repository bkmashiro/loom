package dag

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bkmashiro/loom/pkg/parser"
)

// instantExecutor completes immediately with no delay — pure harness overhead.
type instantExecutor struct{}

func (e *instantExecutor) Execute(_ context.Context, step parser.Step, _ map[string]Result) (Result, error) {
	return Result{StepID: step.ID, Data: step.ID}, nil
}

// delayExecutor simulates real IO latency.
type delayExecutor struct{ d time.Duration }

func (e *delayExecutor) Execute(ctx context.Context, step parser.Step, _ map[string]Result) (Result, error) {
	select {
	case <-time.After(e.d):
	case <-ctx.Done():
		return Result{StepID: step.ID, Err: ctx.Err()}, ctx.Err()
	}
	return Result{StepID: step.ID, Data: step.ID}, nil
}

// ----- topology helpers -----

// submitLinear submits N steps in a chain: s0 → s1 → ... → sN-1.
func submitLinear(s *Scheduler, n int) error {
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("s%d", i)
		deps := []string{}
		if i > 0 {
			deps = []string{fmt.Sprintf("s%d", i-1)}
		}
		if err := s.Submit(parser.Step{ID: id, Deps: deps}); err != nil {
			return err
		}
	}
	return nil
}

// submitParallel submits N independent steps (no deps).
func submitParallel(s *Scheduler, n int) error {
	for i := 0; i < n; i++ {
		if err := s.Submit(parser.Step{ID: fmt.Sprintf("s%d", i)}); err != nil {
			return err
		}
	}
	return nil
}

// submitDiamond submits a fan-out / fan-in DAG:
//
//	root → [w0..wN-1] → sink
func submitDiamond(s *Scheduler, width int) error {
	if err := s.Submit(parser.Step{ID: "root"}); err != nil {
		return err
	}
	deps := make([]string, width)
	for i := 0; i < width; i++ {
		id := fmt.Sprintf("w%d", i)
		deps[i] = id
		if err := s.Submit(parser.Step{ID: id, Deps: []string{"root"}}); err != nil {
			return err
		}
	}
	return s.Submit(parser.Step{ID: "sink", Deps: deps})
}

// submitSwarm submits a coordinator → N parallel agents → aggregator DAG,
// modelling a swarm-calling pattern where multiple sub-agents run in parallel.
func submitSwarm(s *Scheduler, agents int) error {
	// coordinator
	if err := s.Submit(parser.Step{ID: "coordinator"}); err != nil {
		return err
	}
	// N agents, each depending on coordinator
	agentIDs := make([]string, agents)
	for i := 0; i < agents; i++ {
		id := fmt.Sprintf("agent%d", i)
		agentIDs[i] = id
		if err := s.Submit(parser.Step{ID: id, Deps: []string{"coordinator"}}); err != nil {
			return err
		}
	}
	// aggregator depending on all agents
	return s.Submit(parser.Step{ID: "aggregator", Deps: agentIDs})
}

// ----- benchmarks -----

// BenchmarkScheduler_Linear measures scheduling overhead for a serial chain.
// This is the worst case: no parallelism, maximum serialisation cost.
func BenchmarkScheduler_Linear(b *testing.B) {
	for _, n := range []int{4, 16, 64} {
		b.Run(fmt.Sprintf("steps=%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				s := NewScheduler(context.Background(), &instantExecutor{})
				if err := submitLinear(s, n); err != nil {
					b.Fatal(err)
				}
				s.SetReturn(fmt.Sprintf("s%d", n-1))
				s.Seal()
				if _, err := s.Wait(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkScheduler_Parallel measures scheduling overhead for embarrassingly
// parallel steps — ideal Loom case (e.g. fetching N independent resources).
func BenchmarkScheduler_Parallel(b *testing.B) {
	for _, n := range []int{4, 16, 64} {
		b.Run(fmt.Sprintf("steps=%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				s := NewScheduler(context.Background(), &instantExecutor{})
				if err := submitParallel(s, n); err != nil {
					b.Fatal(err)
				}
				s.Seal()
				if _, err := s.Wait(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkScheduler_Diamond measures a fan-out/fan-in topology.
// Typical of: scatter-gather, map-reduce, multi-source aggregation.
func BenchmarkScheduler_Diamond(b *testing.B) {
	for _, w := range []int{4, 16, 64} {
		b.Run(fmt.Sprintf("width=%d", w), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				s := NewScheduler(context.Background(), &instantExecutor{})
				if err := submitDiamond(s, w); err != nil {
					b.Fatal(err)
				}
				s.SetReturn("sink")
				s.Seal()
				if _, err := s.Wait(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkScheduler_Swarm measures a coordinator → N agent → aggregator topology.
// Models a swarm-calling pattern: one orchestrator spins up N specialist agents
// that all run in parallel, then their results are merged.
func BenchmarkScheduler_Swarm(b *testing.B) {
	for _, agents := range []int{4, 16, 64} {
		b.Run(fmt.Sprintf("agents=%d", agents), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				s := NewScheduler(context.Background(), &instantExecutor{})
				if err := submitSwarm(s, agents); err != nil {
					b.Fatal(err)
				}
				s.SetReturn("aggregator")
				s.Seal()
				if _, err := s.Wait(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkScheduler_Stream measures the overhead of consuming the result stream
// for a parallel workload. Stream() is registered before Submit; we drain until
// the channel closes (results count may vary due to publish-vs-close races at
// high throughput — correctness tests cover that separately).
func BenchmarkScheduler_Stream(b *testing.B) {
	for _, n := range []int{8, 32} {
		b.Run(fmt.Sprintf("steps=%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				s := NewScheduler(context.Background(), &instantExecutor{})
				ch := s.Stream()
				if err := submitParallel(s, n); err != nil {
					b.Fatal(err)
				}
				s.Seal()
				for range ch {
				}
			}
		})
	}
}

// BenchmarkScheduler_ForwardRef benchmarks Submit() with forward references —
// all steps submitted in reverse topological order (worst case for resolution).
func BenchmarkScheduler_ForwardRef(b *testing.B) {
	for _, n := range []int{8, 32} {
		b.Run(fmt.Sprintf("steps=%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				s := NewScheduler(context.Background(), &instantExecutor{})
				// Submit in reverse: sN-1 → sN-2 → ... → s0
				for j := n - 1; j >= 0; j-- {
					id := fmt.Sprintf("s%d", j)
					deps := []string{}
					if j > 0 {
						deps = []string{fmt.Sprintf("s%d", j-1)}
					}
					if err := s.Submit(parser.Step{ID: id, Deps: deps}); err != nil {
						b.Fatal(err)
					}
				}
				s.SetReturn(fmt.Sprintf("s%d", n-1))
				s.Seal()
				if _, err := s.Wait(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkScheduler_WithDelay models real IO latency to show parallelism gains.
// 16 parallel steps with 10ms IO each — should complete in ~10ms regardless of N.
func BenchmarkScheduler_WithDelay(b *testing.B) {
	exec := &delayExecutor{d: 10 * time.Millisecond}
	for _, n := range []int{4, 16} {
		b.Run(fmt.Sprintf("parallel=%d_10ms", n), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s := NewScheduler(context.Background(), exec)
				if err := submitParallel(s, n); err != nil {
					b.Fatal(err)
				}
				s.Seal()
				if _, err := s.Wait(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
