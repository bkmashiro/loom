package fn

import (
	"errors"
	"fmt"
	"strings"

	"github.com/bkmashiro/loom/pkg/parser"
)

var (
	ErrFuncNotFound     = errors.New("fn: function not defined")
	ErrArgCountMismatch = errors.New("fn: argument count mismatch")
)

// Param describes one function parameter.
type Param struct {
	Name       string
	Default    string // empty = required
	HasDefault bool
}

// FuncDef holds a registered function definition.
type FuncDef struct {
	Name   string
	Params []Param
	Steps  []parser.Step // the expanded steps of the function body
	Return string        // step ID that is the return value (last step ID)
}

// Registry stores function definitions by name.
type Registry struct {
	defs map[string]*FuncDef
}

// NewRegistry creates a new empty Registry.
func NewRegistry() *Registry {
	return &Registry{defs: make(map[string]*FuncDef)}
}

// Register parses and stores a FuncDef step.
// step.ID is the function name, step.Lang is the param string,
// step.Body is the mini-plan notation.
func (r *Registry) Register(step parser.Step) error {
	name := step.ID
	if name == "" {
		return fmt.Errorf("fn: function definition missing name")
	}

	params, err := parseParams(step.Lang)
	if err != nil {
		return fmt.Errorf("fn: parse params for %q: %w", name, err)
	}

	steps, err := parseMiniPlan(step.Body)
	if err != nil {
		return fmt.Errorf("fn: parse body for %q: %w", name, err)
	}

	// The return value is the last step's ID by default.
	returnID := ""
	if len(steps) > 0 {
		returnID = steps[len(steps)-1].ID
	}

	r.defs[name] = &FuncDef{
		Name:   name,
		Params: params,
		Steps:  steps,
		Return: returnID,
	}
	return nil
}

// Expand takes a FuncCall step and returns the expanded steps to submit,
// plus the result step ID (the call site maps its ID to this).
// All internal step IDs are namespaced as "callID.origID" to avoid conflicts.
func (r *Registry) Expand(callStep parser.Step) (steps []parser.Step, returnStepID string, err error) {
	// Parse the call body to get function name and args.
	funcName, args, err := parseFuncCallBody(callStep.Body)
	if err != nil {
		return nil, "", fmt.Errorf("fn: parse call body for %q: %w", callStep.ID, err)
	}

	def, ok := r.defs[funcName]
	if !ok {
		return nil, "", fmt.Errorf("%w: %s", ErrFuncNotFound, funcName)
	}

	// Build the full arg map: fill in defaults for missing args.
	argMap, err := resolveArgs(def.Params, args)
	if err != nil {
		return nil, "", fmt.Errorf("fn: %w calling %q", err, funcName)
	}

	ns := callStep.ID // namespace prefix

	// Expand each step: namespace IDs and deps, substitute args in bodies.
	expanded := make([]parser.Step, len(def.Steps))
	for i, s := range def.Steps {
		newStep := parser.Step{
			ID:   ns + "." + s.ID,
			Type: s.Type,
			Lang: s.Lang,
		}

		// Namespace deps.
		newDeps := make([]string, len(s.Deps))
		for j, dep := range s.Deps {
			newDeps[j] = ns + "." + dep
		}
		newStep.Deps = newDeps

		// Substitute arg placeholders in the body.
		newStep.Body = substituteArgs(s.Body, argMap)

		expanded[i] = newStep
	}

	// The return step ID is the namespaced version of the function's return step.
	returnStepID = ns + "." + def.Return

	return expanded, returnStepID, nil
}

// parseParams parses the parameter string from a defun header.
// Format: "param1, param2=default, param3"
func parseParams(s string) ([]Param, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return []Param{}, nil
	}

	parts := strings.Split(s, ",")
	params := make([]Param, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		eqIdx := strings.Index(p, "=")
		if eqIdx == -1 {
			params = append(params, Param{Name: p})
		} else {
			name := strings.TrimSpace(p[:eqIdx])
			def := strings.TrimSpace(p[eqIdx+1:])
			params = append(params, Param{Name: name, Default: def, HasDefault: true})
		}
	}
	return params, nil
}

// parseMiniPlan parses the mini-plan notation from a function body.
// Lines starting with '[' begin a new step header: [type(deps) id]
func parseMiniPlan(body string) ([]parser.Step, error) {
	lines := strings.Split(body, "\n")
	var steps []parser.Step

	var currentStep *parser.Step
	var bodyLines []string

	flush := func() {
		if currentStep != nil {
			currentStep.Body = strings.TrimRight(strings.Join(bodyLines, "\n"), "\n\r ")
			steps = append(steps, *currentStep)
			currentStep = nil
			bodyLines = nil
		}
	}

	for _, line := range lines {
		if strings.HasPrefix(line, "[") {
			// New step header.
			flush()
			step, err := parseMiniPlanHeader(line)
			if err != nil {
				return nil, err
			}
			currentStep = step
			bodyLines = nil
		} else if currentStep != nil {
			bodyLines = append(bodyLines, line)
		}
		// Lines before the first header are ignored.
	}
	flush()

	return steps, nil
}

// parseMiniPlanHeader parses a line like "[type(dep1, dep2) id]".
func parseMiniPlanHeader(line string) (*parser.Step, error) {
	// Strip surrounding brackets.
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "[") || !strings.HasSuffix(line, "]") {
		return nil, fmt.Errorf("fn: invalid mini-plan header: %q", line)
	}
	inner := line[1 : len(line)-1]
	inner = strings.TrimSpace(inner)

	// Split type token from rest.
	typeEnd := strings.IndexAny(inner, " (")
	var typeToken, rest string
	if typeEnd == -1 {
		typeToken = inner
		rest = ""
	} else {
		typeToken = inner[:typeEnd]
		rest = strings.TrimSpace(inner[typeEnd:])
	}

	// Map keyword to StepType.
	stepTypeMap := map[string]parser.StepType{
		"io":     parser.IO,
		"write":  parser.Write,
		"pure":   parser.Pure,
		"shell":  parser.Shell,
		"async":  parser.Async,
		"escape": parser.Escape,
		"agent":  parser.Agent,
	}

	// Handle lang suffix (e.g. "pure.python").
	lang := ""
	keyword := typeToken
	if dotIdx := strings.Index(typeToken, "."); dotIdx != -1 {
		keyword = typeToken[:dotIdx]
		lang = typeToken[dotIdx+1:]
	}

	stepType, ok := stepTypeMap[keyword]
	if !ok {
		return nil, fmt.Errorf("fn: unknown step type %q in mini-plan header", keyword)
	}

	// Parse deps and id from rest.
	var deps []string
	var id string

	if strings.HasPrefix(rest, "(") {
		closeIdx := strings.Index(rest, ")")
		if closeIdx == -1 {
			return nil, fmt.Errorf("fn: unclosed deps in mini-plan header: %q", line)
		}
		depsStr := rest[1:closeIdx]
		deps = splitDeps(depsStr)
		id = strings.TrimSpace(rest[closeIdx+1:])
	} else {
		parenIdx := strings.Index(rest, "(")
		if parenIdx == -1 {
			id = rest
		} else {
			id = strings.TrimSpace(rest[:parenIdx])
			closeIdx := strings.Index(rest[parenIdx:], ")")
			if closeIdx == -1 {
				return nil, fmt.Errorf("fn: unclosed deps in mini-plan header: %q", line)
			}
			depsStr := rest[parenIdx+1 : parenIdx+closeIdx]
			deps = splitDeps(depsStr)
		}
	}

	if deps == nil {
		deps = []string{}
	}

	return &parser.Step{
		ID:   id,
		Type: stepType,
		Deps: deps,
		Lang: lang,
	}, nil
}

func splitDeps(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return []string{}
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// parseFuncCallBody parses the call step body:
//
//	fn: name
//	args:
//	  key: value
func parseFuncCallBody(body string) (funcName string, args map[string]string, err error) {
	args = make(map[string]string)
	lines := strings.Split(body, "\n")

	inArgs := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "fn:") {
			funcName = strings.TrimSpace(trimmed[3:])
			inArgs = false
			continue
		}
		if trimmed == "args:" {
			inArgs = true
			continue
		}
		if inArgs {
			// Each arg line is indented: "  key: value"
			colonIdx := strings.Index(trimmed, ":")
			if colonIdx == -1 {
				continue
			}
			k := strings.TrimSpace(trimmed[:colonIdx])
			v := strings.TrimSpace(trimmed[colonIdx+1:])
			args[k] = v
		}
	}

	if funcName == "" {
		return "", nil, fmt.Errorf("fn: call body missing 'fn:' field")
	}
	return funcName, args, nil
}

// resolveArgs builds the final arg map by merging provided args with defaults.
func resolveArgs(params []Param, provided map[string]string) (map[string]string, error) {
	result := make(map[string]string, len(params))

	for _, p := range params {
		if v, ok := provided[p.Name]; ok {
			result[p.Name] = v
		} else if p.HasDefault {
			result[p.Name] = p.Default
		} else {
			return nil, fmt.Errorf("%w: missing required argument %q", ErrArgCountMismatch, p.Name)
		}
	}

	return result, nil
}

// substituteArgs replaces ${paramName} in body with argMap values.
func substituteArgs(body string, argMap map[string]string) string {
	var sb strings.Builder
	rest := body
	for {
		start := strings.Index(rest, "${")
		if start == -1 {
			sb.WriteString(rest)
			break
		}
		sb.WriteString(rest[:start])
		rest = rest[start+2:]
		end := strings.Index(rest, "}")
		if end == -1 {
			sb.WriteString("${")
			sb.WriteString(rest)
			break
		}
		key := rest[:end]
		rest = rest[end+1:]
		if val, ok := argMap[key]; ok {
			sb.WriteString(val)
		} else {
			// Not an arg placeholder — leave it for runtime interpolation.
			sb.WriteString("${")
			sb.WriteString(key)
			sb.WriteString("}")
		}
	}
	return sb.String()
}
