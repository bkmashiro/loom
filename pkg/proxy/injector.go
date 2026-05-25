package proxy

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bkmashiro/loom/pkg/dag"
)

// Message is an OpenAI chat message.
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ChatCompletionRequest is a minimal OpenAI-compatible request struct.
type ChatCompletionRequest struct {
	Model    string    `json:"model"`
	Messages []Message `json:"messages"`
	Stream   bool      `json:"stream"`
}

// ResultInjector builds the second LLM call message structure.
type ResultInjector struct {
	OriginalMessages []Message
	PrePlanText      string
	Results          []dag.StepResult
	Model            string
}

// BuildRequest creates the ChatCompletionRequest for the summary call.
// Structure:
//   - original messages
//   - assistant message: pre-plan text (if any)
//   - user message: "<results>...</results>\n\nPlease provide a natural response..."
func (ri *ResultInjector) BuildRequest() ChatCompletionRequest {
	messages := make([]Message, 0, len(ri.OriginalMessages)+2)
	messages = append(messages, ri.OriginalMessages...)

	if ri.PrePlanText != "" {
		messages = append(messages, Message{
			Role:    "assistant",
			Content: ri.PrePlanText,
		})
	}

	messages = append(messages, Message{
		Role:    "user",
		Content: ri.FormatResults(),
	})

	return ChatCompletionRequest{
		Model:    ri.Model,
		Messages: messages,
		Stream:   true,
	}
}

// FormatResults renders step results as XML-tagged blocks.
// Format per step:
//
//	<step id="fetch_user" status="ok">
//	{"name": "Alice"}
//	</step>
//
// Error steps use status="error" and the error message as content.
func (ri *ResultInjector) FormatResults() string {
	var sb strings.Builder
	sb.WriteString("Here are the results of the execution plan:\n\n<results>\n")
	for _, r := range ri.Results {
		status := "ok"
		if r.Err != nil {
			status = "error"
		}
		fmt.Fprintf(&sb, "<step id=%q status=%q>\n", r.StepID, status)
		if r.Err != nil {
			sb.WriteString(r.Err.Error())
		} else {
			data, _ := json.Marshal(r.Data)
			sb.Write(data)
		}
		sb.WriteString("\n</step>\n")
	}
	sb.WriteString("</results>\n\n")
	sb.WriteString("Please provide a natural response to the user based on these results. ")
	sb.WriteString("Do not output any Loom execution plans.")
	return sb.String()
}
