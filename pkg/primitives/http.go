package primitives

import (
	"encoding/json"
	"fmt"
	"strings"
)

// HTTPRequest represents a parsed HTTP request from a step body line.
type HTTPRequest struct {
	Method  string
	URL     string
	Body    string            // raw body string (may be JSON)
	Headers map[string]string
}

var validMethods = map[string]bool{
	"GET":    true,
	"POST":   true,
	"PUT":    true,
	"DELETE": true,
	"PATCH":  true,
	"HEAD":   true,
}

// ParseHTTPRequest parses a step body line into an HTTPRequest.
// Formats supported:
//
//	GET https://example.com
//	POST https://example.com {"key": "val"}
//	PUT https://example.com {"key": "val"} headers: {"Authorization": "Bearer tok"}
//	DELETE https://example.com
//
// Returns error on unknown method or missing URL.
func ParseHTTPRequest(line string) (HTTPRequest, error) {
	line = strings.TrimSpace(line)
	tokens := strings.Fields(line)

	if len(tokens) == 0 {
		return HTTPRequest{}, fmt.Errorf("empty input")
	}

	method := strings.ToUpper(tokens[0])
	if !validMethods[method] {
		return HTTPRequest{}, fmt.Errorf("unknown HTTP method: %q", tokens[0])
	}

	if len(tokens) < 2 {
		return HTTPRequest{}, fmt.Errorf("missing URL")
	}

	url := tokens[1]
	rest := tokens[2:]

	// Split on "headers:" token
	var bodyParts []string
	var headerJSON string

	for i, tok := range rest {
		if tok == "headers:" {
			headerJSON = strings.Join(rest[i+1:], " ")
			break
		}
		bodyParts = append(bodyParts, tok)
	}

	body := strings.Join(bodyParts, " ")

	var headers map[string]string
	if headerJSON != "" {
		if err := json.Unmarshal([]byte(headerJSON), &headers); err != nil {
			return HTTPRequest{}, fmt.Errorf("invalid headers JSON: %w", err)
		}
	}

	return HTTPRequest{
		Method:  method,
		URL:     url,
		Body:    body,
		Headers: headers,
	}, nil
}
