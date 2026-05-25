package proxy

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bkmashiro/loom/pkg/dag"
)

// Message is an OpenAI chat message.
type Message struct {
	Role       string      `json:"role"`
	Content    string      `json:"content"`
	ToolCallID string      `json:"tool_call_id,omitempty"`
	ToolCalls  []ToolCall  `json:"tool_calls,omitempty"`
}

// ToolCall represents a synthetic tool call entry on an assistant message.
type ToolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"`
	Function ToolCallFunction `json:"function"`
}

// ToolCallFunction holds the function name and arguments for a tool call.
type ToolCallFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// ChatCompletionRequest is a minimal OpenAI-compatible request struct.
type ChatCompletionRequest struct {
	Model    string    `json:"model"`
	Messages []Message `json:"messages"`
	Stream   bool      `json:"stream"`
	Tools    []Tool    `json:"tools,omitempty"`
}

// Tool represents an OpenAI-compatible tool definition.
type Tool struct {
	Type     string       `json:"type"`
	Function ToolFunction `json:"function"`
}

// ToolFunction holds the function name, description, and parameters schema.
type ToolFunction struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Parameters  any    `json:"parameters"`
}

// loomToolCallID is the synthetic tool_call_id used for injected results.
const loomToolCallID = "loom_exec_001"

// InjectResults modifies the messages slice to include pending step results
// from the previous round. It inserts two messages before the final user message:
//
//  1. An assistant message containing the full round-N LLM output (including
//     plan fences), replacing any assistant message the client may have sent.
//  2. A tool (or user) message containing the formatted step results.
//
// role must be "tool" or "user". When "tool", the assistant message is patched
// with a synthetic tool_calls entry and the result message includes tool_call_id.
func InjectResults(messages []Message, lastAssistantMsg string, results []dag.StepResult, role string) []Message {
	// Find the last user message.
	lastUserIdx := -1
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role == "user" {
			lastUserIdx = i
			break
		}
	}
	if lastUserIdx < 0 {
		return messages // no user message to inject before
	}

	formatted := FormatResults(results)

	// Build the injected assistant message.
	assistantMsg := Message{
		Role:    "assistant",
		Content: lastAssistantMsg,
	}
	if role == "tool" {
		assistantMsg.ToolCalls = []ToolCall{
			{
				ID:   loomToolCallID,
				Type: "function",
				Function: ToolCallFunction{
					Name:      "loom_execute",
					Arguments: "{}",
				},
			},
		}
	}

	// Build the results message.
	var resultsMsg Message
	switch role {
	case "tool":
		resultsMsg = Message{
			Role:       "tool",
			ToolCallID: loomToolCallID,
			Content:    "Loom execution results:\n\n" + formatted,
		}
	default: // "user"
		resultsMsg = Message{
			Role:    "user",
			Content: "[System: Loom execution results]\n\n" + formatted,
		}
	}

	// Determine insertion point: just before the last user message.
	// If there's an existing assistant message immediately before the last user
	// message (from the client's history), replace it with our stored version.
	insertIdx := lastUserIdx
	if insertIdx > 0 && messages[insertIdx-1].Role == "assistant" {
		// Replace the client's assistant message with the proxy-stored full version.
		out := make([]Message, 0, len(messages)+1)
		out = append(out, messages[:insertIdx-1]...)
		out = append(out, assistantMsg)
		out = append(out, resultsMsg)
		out = append(out, messages[insertIdx:]...)
		return out
	}

	// No assistant message before last user — insert both.
	out := make([]Message, 0, len(messages)+2)
	out = append(out, messages[:insertIdx]...)
	out = append(out, assistantMsg)
	out = append(out, resultsMsg)
	out = append(out, messages[insertIdx:]...)
	return out
}

// FormatResults renders step results as XML-tagged blocks.
//
//	<step id="fetch_user" type="io" status="ok">
//	{"name": "Alice"}
//	</step>
//
// Error steps use status="error" with the error message as body.
func FormatResults(results []dag.StepResult) string {
	var sb strings.Builder
	for _, r := range results {
		status := "ok"
		if r.Err != nil {
			status = "error"
		}
		// stepType from Result.StepID — we don't store type in dag.Result,
		// so omit type attribute for now. The ID is sufficient for the LLM.
		fmt.Fprintf(&sb, "<step id=%q status=%q>\n", r.StepID, status)
		if r.Err != nil {
			sb.WriteString(r.Err.Error())
		} else {
			data, _ := json.Marshal(r.Data)
			sb.Write(data)
		}
		sb.WriteString("\n</step>\n")
	}
	return sb.String()
}
