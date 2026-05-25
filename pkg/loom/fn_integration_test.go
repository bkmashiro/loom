package loom_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bkmashiro/loom/pkg/loom"
)

// TestLoom_FuncDef_Expand defines a function and calls it, verifying the call result
// equals the function's return step result.
func TestLoom_FuncDef_Expand(t *testing.T) {
	// Mock server that returns a known response.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "function result data")
	}))
	defer srv.Close()

	// Plan:
	//   1. Define a function "fetchData" with one param "url"
	//      - it does an IO step to fetch the URL
	//      - returns the IO step result
	//   2. Call the function with the mock server URL
	//   3. Return the call result
	plan := fmt.Sprintf(`
`+"```"+"defun fetchData(url)"+`
[io fetch]
GET ${url}
`+"```"+`

`+"```"+"call myFetch"+`
fn: fetchData
args:
  url: %s
`+"```"+`

return myFetch
`, srv.URL)

	l := loom.New(loom.WithHTTPClient(srv.Client()))
	result, err := l.Run(context.Background(), strings.NewReader(plan))
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if result.Data == nil {
		t.Fatal("expected non-nil result data")
	}

	data, ok := result.Data.(string)
	if !ok {
		t.Fatalf("expected string result, got %T: %v", result.Data, result.Data)
	}

	if !strings.Contains(data, "function result data") {
		t.Errorf("expected result to contain 'function result data', got %q", data)
	}
}

// TestLoom_FuncDef_MultipleParams verifies functions with multiple params and defaults.
func TestLoom_FuncDef_MultipleParams(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Echo the path back so we can verify substitution.
		fmt.Fprintf(w, "path=%s", r.URL.Path)
	}))
	defer srv.Close()

	plan := fmt.Sprintf(`
`+"```"+"defun fetchPath(base, path=/default)"+`
[io fetch]
GET ${base}${path}
`+"```"+`

`+"```"+"call result1"+`
fn: fetchPath
args:
  base: %s
  path: /custom
`+"```"+`

return result1
`, srv.URL)

	l := loom.New(loom.WithHTTPClient(srv.Client()))
	result, err := l.Run(context.Background(), strings.NewReader(plan))
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	data, ok := result.Data.(string)
	if !ok {
		t.Fatalf("expected string result, got %T", result.Data)
	}

	if !strings.Contains(data, "/custom") {
		t.Errorf("expected path '/custom' in result, got %q", data)
	}
}
