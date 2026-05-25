package proxy

import "strings"

// DetectorState is the state of the plan detector.
type DetectorState int

const (
	// StateIdle — no Loom fence seen yet. Forward everything.
	StateIdle DetectorState = iota

	// StateInFence — inside a Loom code fence. Accumulating body.
	StateInFence

	// StateHasPlan — at least one valid Loom fence has completed.
	// Looking for more fences or end of stream.
	StateHasPlan
)

// ActionType describes how the tee should handle a piece of content.
type ActionType int

const (
	ActionForward  ActionType = iota // send content to client
	ActionBuffer                     // hold, might be fence start (partial line)
	ActionSuppress                   // plan content — suppress unless passthrough mode
)

// DetectorAction is returned by Feed for each processed token.
type DetectorAction struct {
	Type    ActionType
	Content string
}

// PlanDetector parses the LLM token stream, identifying Loom fences.
//
// v2 changes from v1:
//   - StateBetweenSteps and StatePlanComplete removed; replaced by StateHasPlan.
//   - ActionPlanComplete removed; plan completion is determined by HasPlan() after [DONE].
//   - No `return` directive detection — not part of the v2 protocol.
//   - PrePlanText() exposes text accumulated before the first fence.
type PlanDetector struct {
	state       DetectorState
	lineBuf     strings.Builder  // partial line accumulator
	planText    strings.Builder  // all fence content (and prose between fences)
	prePlanText strings.Builder  // text before the first fence (StateIdle output)
	stepCount   int              // number of completed fences
}

// loomStepTypes is the set of known Loom step type keywords.
var loomStepTypes = map[string]bool{
	"io": true, "write": true, "pure": true,
	"shell": true, "async": true, "escape": true,
}

// Feed processes a content token. Returns actions for the tee.
// Tokens may not be complete lines — the detector buffers partial lines.
func (d *PlanDetector) Feed(token string) []DetectorAction {
	var actions []DetectorAction

	for i := 0; i < len(token); i++ {
		ch := token[i]
		if ch == '\n' {
			line := d.lineBuf.String()
			d.lineBuf.Reset()
			actions = append(actions, d.processLine(line+"\n")...)
		} else {
			d.lineBuf.WriteByte(ch)
		}
	}

	// Partial line — emit signal so the tee knows we're holding content.
	if d.lineBuf.Len() > 0 {
		switch d.state {
		case StateIdle:
			// Could be the start of a fence opener — buffer.
			actions = append(actions, DetectorAction{Type: ActionBuffer, Content: d.lineBuf.String()})
		case StateInFence, StateHasPlan:
			// Inside plan context — suppress partial content.
			actions = append(actions, DetectorAction{Type: ActionSuppress, Content: d.lineBuf.String()})
		}
	}

	return actions
}

// processLine handles a complete line (including the trailing \n).
func (d *PlanDetector) processLine(line string) []DetectorAction {
	trimmed := strings.TrimRight(line, "\n\r")

	switch d.state {
	case StateIdle:
		if fenceOpenType(trimmed) != "" {
			// Loom fence opener — transition to StateInFence.
			d.state = StateInFence
			d.planText.WriteString(line)
			return []DetectorAction{{Type: ActionSuppress, Content: line}}
		}
		// Regular prose — accumulate in prePlanText and forward.
		d.prePlanText.WriteString(line)
		return []DetectorAction{{Type: ActionForward, Content: line}}

	case StateInFence:
		d.planText.WriteString(line)
		if trimmed == "```" {
			// Fence closer — step completed.
			d.stepCount++
			d.state = StateHasPlan
		}
		return []DetectorAction{{Type: ActionSuppress, Content: line}}

	case StateHasPlan:
		if fenceOpenType(trimmed) != "" {
			// Another fence — transition back to StateInFence.
			d.state = StateInFence
			d.planText.WriteString(line)
			return []DetectorAction{{Type: ActionSuppress, Content: line}}
		}
		// Prose between/after fences — part of the plan, suppress.
		d.planText.WriteString(line)
		return []DetectorAction{{Type: ActionSuppress, Content: line}}
	}

	return nil
}

// fenceOpenType checks if a line is a Loom fence opener (```<type>).
// Returns the type keyword if it is, empty string otherwise.
func fenceOpenType(line string) string {
	if !strings.HasPrefix(line, "```") {
		return ""
	}
	rest := line[3:]
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return ""
	}
	keyword := fields[0]
	if loomStepTypes[keyword] {
		return keyword
	}
	return ""
}

// HasPlan returns true if at least one valid Loom fence was completed.
// This is the primary signal used by the proxy after [DONE].
func (d *PlanDetector) HasPlan() bool {
	return d.stepCount > 0
}

// State returns the current detector state.
func (d *PlanDetector) State() DetectorState {
	return d.state
}

// PlanText returns the accumulated plan text (all fences and inter-fence prose).
func (d *PlanDetector) PlanText() string {
	return d.planText.String()
}

// PrePlanText returns assistant text that appeared before the first fence.
func (d *PlanDetector) PrePlanText() string {
	return d.prePlanText.String()
}

// StepCount returns the number of completed Loom fences seen.
func (d *PlanDetector) StepCount() int {
	return d.stepCount
}

// Reset clears all state for reuse.
func (d *PlanDetector) Reset() {
	d.state = StateIdle
	d.lineBuf.Reset()
	d.planText.Reset()
	d.prePlanText.Reset()
	d.stepCount = 0
}
