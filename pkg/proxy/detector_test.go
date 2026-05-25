package proxy

import (
	"strings"
	"testing"
)

// feedAll feeds all tokens and collects all actions.
func feedAll(d *PlanDetector, tokens []string) []DetectorAction {
	var all []DetectorAction
	for _, tok := range tokens {
		all = append(all, d.Feed(tok)...)
	}
	return all
}

// collectByType filters actions by type.
func countByType(actions []DetectorAction, t ActionType) int {
	n := 0
	for _, a := range actions {
		if a.Type == t {
			n++
		}
	}
	return n
}

func hasAction(actions []DetectorAction, t ActionType) bool {
	for _, a := range actions {
		if a.Type == t {
			return true
		}
	}
	return false
}

// TestDetector_NoFences — plain text, all tokens ActionForward, state stays Idle.
func TestDetector_NoFences(t *testing.T) {
	d := &PlanDetector{}
	tokens := []string{"Hello, ", "world!\n", "This is plain text.\n"}
	actions := feedAll(d, tokens)

	if d.State() != StateIdle {
		t.Errorf("expected StateIdle, got %v", d.State())
	}
	for _, a := range actions {
		// Buffer actions are allowed for partial lines; Forward for completed lines
		if a.Type != ActionForward && a.Type != ActionBuffer {
			t.Errorf("unexpected action type %v for plain text", a.Type)
		}
	}
}

// TestDetector_NonLoomFence — ```python fence, treated as plain text, forwarded.
func TestDetector_NonLoomFence(t *testing.T) {
	d := &PlanDetector{}
	input := "```python\nprint('hello')\n```\n"
	actions := feedAll(d, []string{input})

	if d.State() != StateIdle {
		t.Errorf("expected StateIdle after non-Loom fence, got %v", d.State())
	}
	for _, a := range actions {
		if a.Type == ActionSuppress || a.Type == ActionPlanComplete {
			t.Errorf("unexpected action %v for non-Loom fence", a.Type)
		}
	}
	if hasAction(actions, ActionPlanComplete) {
		t.Error("should not have PlanComplete for non-Loom fence")
	}
}

// TestDetector_SingleStep_NoReturn — one io fence but no return → plan-free, text suppressed.
func TestDetector_SingleStep_NoReturn(t *testing.T) {
	d := &PlanDetector{}
	input := "```io fetch\nGET https://api.example.com/users\n```\n"
	actions := feedAll(d, []string{input})

	// State should be StateBetweenSteps (fence closed, no return yet)
	if d.State() != StateBetweenSteps {
		t.Errorf("expected StateBetweenSteps, got %v", d.State())
	}
	if hasAction(actions, ActionPlanComplete) {
		t.Error("should not have PlanComplete without return directive")
	}
	// Fence content should be suppressed
	if !hasAction(actions, ActionSuppress) {
		t.Error("expected suppress actions for fence content")
	}
}

// TestDetector_CompletePlan — io fence + return → ActionPlanComplete emitted, planText correct.
func TestDetector_CompletePlan(t *testing.T) {
	d := &PlanDetector{}
	plan := "```io fetch_user\nGET https://api.example.com/user/1\n```\nreturn fetch_user\n"
	actions := feedAll(d, []string{plan})

	if d.State() != StatePlanComplete {
		t.Errorf("expected StatePlanComplete, got %v", d.State())
	}
	if !hasAction(actions, ActionPlanComplete) {
		t.Error("expected ActionPlanComplete")
	}
	if d.ReturnID() != "fetch_user" {
		t.Errorf("expected returnID=fetch_user, got %q", d.ReturnID())
	}
	planText := d.PlanText()
	if !strings.Contains(planText, "GET https://api.example.com/user/1") {
		t.Errorf("plan text missing content: %q", planText)
	}
	if !strings.Contains(planText, "return fetch_user") {
		t.Errorf("plan text missing return directive: %q", planText)
	}
}

// TestDetector_MultiStep — 3 steps + return → correct plan captured.
func TestDetector_MultiStep(t *testing.T) {
	d := &PlanDetector{}
	plan := strings.Join([]string{
		"```io fetch_user\nGET https://api.example.com/user/1\n```\n",
		"```io fetch_posts\nGET https://api.example.com/posts?user=1\n```\n",
		"```pure build_feed\nreturn merge(fetch_user, fetch_posts)\n```\n",
		"return build_feed\n",
	}, "")

	actions := feedAll(d, []string{plan})

	if d.State() != StatePlanComplete {
		t.Errorf("expected StatePlanComplete, got %v", d.State())
	}
	if d.ReturnID() != "build_feed" {
		t.Errorf("expected returnID=build_feed, got %q", d.ReturnID())
	}
	if !hasAction(actions, ActionPlanComplete) {
		t.Error("expected ActionPlanComplete")
	}
	planText := d.PlanText()
	if !strings.Contains(planText, "fetch_user") {
		t.Error("plan text missing fetch_user step")
	}
	if !strings.Contains(planText, "fetch_posts") {
		t.Error("plan text missing fetch_posts step")
	}
	if !strings.Contains(planText, "build_feed") {
		t.Error("plan text missing build_feed step")
	}
}

// TestDetector_SplitTokens — same plan but each char is a separate token → same result.
func TestDetector_SplitTokens(t *testing.T) {
	d := &PlanDetector{}
	plan := "```io fetch_user\nGET https://api.example.com/user/1\n```\nreturn fetch_user\n"

	// Feed one character at a time
	var tokens []string
	for _, ch := range plan {
		tokens = append(tokens, string(ch))
	}
	actions := feedAll(d, tokens)

	if d.State() != StatePlanComplete {
		t.Errorf("expected StatePlanComplete with split tokens, got %v", d.State())
	}
	if !hasAction(actions, ActionPlanComplete) {
		t.Error("expected ActionPlanComplete with split tokens")
	}
	if d.ReturnID() != "fetch_user" {
		t.Errorf("expected returnID=fetch_user, got %q", d.ReturnID())
	}
}

// TestDetector_ProseBetweenSteps — text between fences → StateBetweenSteps stays.
func TestDetector_ProseBetweenSteps(t *testing.T) {
	d := &PlanDetector{}
	plan := strings.Join([]string{
		"```io step1\nGET https://api.example.com/a\n```\n",
		"Now fetching step 2...\n",
		"```io step2\nGET https://api.example.com/b\n```\n",
		"return step2\n",
	}, "")

	actions := feedAll(d, []string{plan})

	if d.State() != StatePlanComplete {
		t.Errorf("expected StatePlanComplete, got %v", d.State())
	}
	if !hasAction(actions, ActionPlanComplete) {
		t.Error("expected ActionPlanComplete")
	}
	planText := d.PlanText()
	if !strings.Contains(planText, "Now fetching step 2") {
		t.Errorf("plan text should include prose between steps: %q", planText)
	}
}

// TestDetector_Reset — reset clears state.
func TestDetector_Reset(t *testing.T) {
	d := &PlanDetector{}
	plan := "```io fetch_user\nGET https://api.example.com/user/1\n```\nreturn fetch_user\n"
	feedAll(d, []string{plan})

	if d.State() != StatePlanComplete {
		t.Fatalf("pre-reset: expected StatePlanComplete, got %v", d.State())
	}

	d.Reset()

	if d.State() != StateIdle {
		t.Errorf("after reset: expected StateIdle, got %v", d.State())
	}
	if d.ReturnID() != "" {
		t.Errorf("after reset: expected empty returnID, got %q", d.ReturnID())
	}
	if d.PlanText() != "" {
		t.Errorf("after reset: expected empty planText, got %q", d.PlanText())
	}

	// Should work again after reset
	feedAll(d, []string{plan})
	if d.State() != StatePlanComplete {
		t.Errorf("after reset+refeed: expected StatePlanComplete, got %v", d.State())
	}
}
