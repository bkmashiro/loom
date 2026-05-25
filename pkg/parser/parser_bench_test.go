package parser

import (
	"fmt"
	"strings"
	"testing"
)

// buildPlan generates a synthetic LLM response with n io steps.
func buildPlan(n int) string {
	var sb strings.Builder
	sb.WriteString("Here is my plan:\n\n")
	for i := 0; i < n; i++ {
		if i == 0 {
			fmt.Fprintf(&sb, "```io step%d\nGET https://api.example.com/resource/%d\n```\n\n", i, i)
		} else {
			fmt.Fprintf(&sb, "```io(step%d) step%d\nGET https://api.example.com/resource/%d\n```\n\n", i-1, i, i)
		}
	}
	sb.WriteString("All done.\n")
	return sb.String()
}

// BenchmarkParser_SingleStep measures parsing a single-step plan.
func BenchmarkParser_SingleStep(b *testing.B) {
	input := buildPlan(1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := NewParser(strings.NewReader(input))
		for range p.Events() {
		}
	}
}

// BenchmarkParser_TenSteps measures parsing a realistic 10-step plan.
func BenchmarkParser_TenSteps(b *testing.B) {
	input := buildPlan(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := NewParser(strings.NewReader(input))
		for range p.Events() {
		}
	}
}

// BenchmarkParser_NoPlan measures the no-plan fast path (no fences).
func BenchmarkParser_NoPlan(b *testing.B) {
	input := strings.Repeat("The quick brown fox jumps over the lazy dog. ", 20)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := NewParser(strings.NewReader(input))
		for range p.Events() {
		}
	}
}

// BenchmarkParser_WideParallel measures a wide parallel plan (N independent steps).
func BenchmarkParser_WideParallel(b *testing.B) {
	// All steps independent (no deps) — models a swarm fanout.
	var sb strings.Builder
	for i := 0; i < 16; i++ {
		fmt.Fprintf(&sb, "```io step%d\nGET https://api.example.com/item/%d\n```\n\n", i, i)
	}
	input := sb.String()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := NewParser(strings.NewReader(input))
		for range p.Events() {
		}
	}
}
