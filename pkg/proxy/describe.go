package proxy

const loomSpec = `# Loom Parallel Execution

Write fenced code blocks with Loom step types to execute tasks in parallel. Results appear in the next message automatically.

## Step Types
- ` + "`io`" + ` — HTTP requests (parallel, retried)
- ` + "`write`" + ` — Side-effecting writes (isolated)
- ` + "`pure`" + ` — Pure computation (no IO)
- ` + "`shell`" + ` — Shell commands (sandboxed)
- ` + "`async`" + ` — Fire-and-forget (non-blocking)
- ` + "`escape`" + ` — External tool call

## Syntax
` + "```" + `{type}[(dep1, dep2)] {step_id}
{body}
` + "```" + `

- Dependencies: list step IDs in parens after type
- Reference results: ` + "`${step_id}`" + ` in body (JSON-encoded)
- HTTP body: ` + "`METHOD URL\nbody`" + `

## Example
` + "```" + `io fetch_user
GET https://api.example.com/user/42
` + "```" + `

` + "```" + `io fetch_posts
GET https://api.example.com/posts?user=42
` + "```" + `

` + "```" + `pure(fetch_user, fetch_posts) build_feed
{"user": ${fetch_user}, "posts": ${fetch_posts}}
` + "```" + `

The two ` + "`io`" + ` steps run in parallel. ` + "`build_feed`" + ` runs after both complete.
Results are injected into the next conversational round.`

// BuildLoomSpec returns the Loom fence syntax specification as a string.
func BuildLoomSpec() string {
	return loomSpec
}

// LoomDescribeToolDef returns a Tool struct for the loom_describe built-in tool.
func LoomDescribeToolDef() Tool {
	return Tool{
		Type: "function",
		Function: ToolFunction{
			Name:        "loom_describe",
			Description: "Returns the Loom parallel execution spec, including fence syntax, step types, dependency notation, and examples. Call this to learn how to write Loom plans.",
			Parameters: map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
		},
	}
}
