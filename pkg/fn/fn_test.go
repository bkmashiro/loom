package fn

import (
	"errors"
	"strings"
	"testing"

	"github.com/bkmashiro/loom/pkg/parser"
)

// helper to register a function with a given name, params string, and body.
func mustRegister(t *testing.T, r *Registry, name, params, body string) {
	t.Helper()
	step := parser.Step{
		ID:   name,
		Type: parser.FuncDef,
		Lang: params,
		Body: body,
	}
	if err := r.Register(step); err != nil {
		t.Fatalf("Register(%q) failed: %v", name, err)
	}
}

func TestRegistry_Register(t *testing.T) {
	r := NewRegistry()
	body := `
[io search]
GET https://example.com

[pure(search) result]
process(search)
`
	mustRegister(t, r, "myFunc", "query, limit=10", body)

	def, ok := r.defs["myFunc"]
	if !ok {
		t.Fatal("expected 'myFunc' to be registered")
	}
	if def.Name != "myFunc" {
		t.Errorf("name = %q, want %q", def.Name, "myFunc")
	}
	if len(def.Params) != 2 {
		t.Fatalf("params count = %d, want 2", len(def.Params))
	}
	if def.Params[0].Name != "query" || def.Params[0].HasDefault {
		t.Errorf("param[0] = %+v, want {Name:query HasDefault:false}", def.Params[0])
	}
	if def.Params[1].Name != "limit" || !def.Params[1].HasDefault || def.Params[1].Default != "10" {
		t.Errorf("param[1] = %+v, want {Name:limit Default:10 HasDefault:true}", def.Params[1])
	}
	if len(def.Steps) != 2 {
		t.Errorf("step count = %d, want 2", len(def.Steps))
	}
}

func TestRegistry_Expand_NoArgs(t *testing.T) {
	r := NewRegistry()
	body := `
[io fetch]
GET https://example.com/data

[pure(fetch) process]
return fetch
`
	mustRegister(t, r, "noArgs", "", body)

	callStep := parser.Step{
		ID:   "myCall",
		Type: parser.FuncCall,
		Body: "fn: noArgs\n",
	}

	steps, returnID, err := r.Expand(callStep)
	if err != nil {
		t.Fatalf("Expand failed: %v", err)
	}

	// IDs should be namespaced
	if len(steps) != 2 {
		t.Fatalf("expanded step count = %d, want 2", len(steps))
	}
	if steps[0].ID != "myCall.fetch" {
		t.Errorf("steps[0].ID = %q, want %q", steps[0].ID, "myCall.fetch")
	}
	if steps[1].ID != "myCall.process" {
		t.Errorf("steps[1].ID = %q, want %q", steps[1].ID, "myCall.process")
	}
	// Deps should also be namespaced
	if len(steps[1].Deps) != 1 || steps[1].Deps[0] != "myCall.fetch" {
		t.Errorf("steps[1].Deps = %v, want [myCall.fetch]", steps[1].Deps)
	}
	// Return ID should point to the last step
	if returnID != "myCall.process" {
		t.Errorf("returnID = %q, want %q", returnID, "myCall.process")
	}
}

func TestRegistry_Expand_WithArgs(t *testing.T) {
	r := NewRegistry()
	body := `
[io search]
GET https://search.api/q=${query}

[pure(search) result]
summarize(${query})
`
	mustRegister(t, r, "searchFunc", "query", body)

	callStep := parser.Step{
		ID:   "call1",
		Type: parser.FuncCall,
		Body: "fn: searchFunc\nargs:\n  query: hello\n",
	}

	steps, _, err := r.Expand(callStep)
	if err != nil {
		t.Fatalf("Expand failed: %v", err)
	}

	if len(steps) != 2 {
		t.Fatalf("expanded step count = %d, want 2", len(steps))
	}

	// Verify ${query} was substituted with "hello"
	if !strings.Contains(steps[0].Body, "hello") {
		t.Errorf("steps[0].Body = %q, expected 'hello' substitution", steps[0].Body)
	}
	if !strings.Contains(steps[1].Body, "hello") {
		t.Errorf("steps[1].Body = %q, expected 'hello' substitution", steps[1].Body)
	}
	// Should not contain the placeholder anymore
	if strings.Contains(steps[0].Body, "${query}") {
		t.Errorf("steps[0].Body still contains ${query} placeholder")
	}
}

func TestRegistry_Expand_DefaultArgs(t *testing.T) {
	r := NewRegistry()
	body := `
[io fetch]
GET https://api.example.com/items?limit=${limit}
`
	mustRegister(t, r, "fetchItems", "limit=5", body)

	// Call without providing the 'limit' arg — should use default "5"
	callStep := parser.Step{
		ID:   "callDefault",
		Type: parser.FuncCall,
		Body: "fn: fetchItems\n",
	}

	steps, _, err := r.Expand(callStep)
	if err != nil {
		t.Fatalf("Expand with defaults failed: %v", err)
	}

	if len(steps) != 1 {
		t.Fatalf("expanded step count = %d, want 1", len(steps))
	}

	if !strings.Contains(steps[0].Body, "5") {
		t.Errorf("steps[0].Body = %q, expected default value '5'", steps[0].Body)
	}
	if strings.Contains(steps[0].Body, "${limit}") {
		t.Errorf("steps[0].Body still contains ${limit} placeholder after default substitution")
	}
}

func TestRegistry_Expand_UnknownFunc(t *testing.T) {
	r := NewRegistry()

	callStep := parser.Step{
		ID:   "badCall",
		Type: parser.FuncCall,
		Body: "fn: nonexistent\n",
	}

	_, _, err := r.Expand(callStep)
	if err == nil {
		t.Fatal("expected error for unknown function, got nil")
	}
	if !errors.Is(err, ErrFuncNotFound) {
		t.Errorf("expected ErrFuncNotFound, got: %v", err)
	}
}

func TestRegistry_Expand_NamespaceIDs(t *testing.T) {
	r := NewRegistry()
	body := `
[pure step1]
do something

[pure(step1) step2]
do more
`
	mustRegister(t, r, "sharedFunc", "", body)

	callA := parser.Step{
		ID:   "callA",
		Type: parser.FuncCall,
		Body: "fn: sharedFunc\n",
	}
	callB := parser.Step{
		ID:   "callB",
		Type: parser.FuncCall,
		Body: "fn: sharedFunc\n",
	}

	stepsA, returnA, err := r.Expand(callA)
	if err != nil {
		t.Fatalf("Expand callA failed: %v", err)
	}
	stepsB, returnB, err := r.Expand(callB)
	if err != nil {
		t.Fatalf("Expand callB failed: %v", err)
	}

	// Collect all IDs and verify no conflicts.
	ids := make(map[string]bool)
	for _, s := range stepsA {
		if ids[s.ID] {
			t.Errorf("duplicate ID: %q", s.ID)
		}
		ids[s.ID] = true
	}
	for _, s := range stepsB {
		if ids[s.ID] {
			t.Errorf("ID conflict between callA and callB: %q", s.ID)
		}
		ids[s.ID] = true
	}

	// Return IDs must also be different.
	if returnA == returnB {
		t.Errorf("returnA == returnB: %q (should differ)", returnA)
	}

	// Verify namespace prefixes.
	if stepsA[0].ID != "callA.step1" {
		t.Errorf("stepsA[0].ID = %q, want callA.step1", stepsA[0].ID)
	}
	if stepsB[0].ID != "callB.step1" {
		t.Errorf("stepsB[0].ID = %q, want callB.step1", stepsB[0].ID)
	}
}
