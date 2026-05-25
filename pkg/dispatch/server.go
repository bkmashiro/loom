package dispatch

import (
	"encoding/json"
	"net/http"

	"github.com/bkmashiro/loom/pkg/dag"
	"github.com/bkmashiro/loom/pkg/parser"
)

// Server wraps a dag.Executor as an HTTP server that RemoteWorker can call.
type Server struct {
	exec dag.Executor
	mux  *http.ServeMux
}

// NewServer creates a Server backed by exec.
func NewServer(exec dag.Executor) *Server {
	s := &Server{
		exec: exec,
		mux:  http.NewServeMux(),
	}
	s.mux.HandleFunc("/execute", s.ServeHTTP)
	return s
}

// Handler returns the http.Handler for mounting (e.g. at "/").
func (s *Server) Handler() http.Handler {
	return s.mux
}

// ServeHTTP handles POST /execute.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req executeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Reconstruct parser.Step.
	step := parser.Step{
		ID:   req.Step.ID,
		Type: req.Step.Type,
		Deps: req.Step.Deps,
		Body: req.Step.Body,
		Lang: req.Step.Lang,
	}

	// Reconstruct inputs map.
	inputs := make(map[string]dag.Result, len(req.Inputs))
	for k, wi := range req.Inputs {
		var inputErr error
		if wi.Err != nil {
			inputErr = errorString(*wi.Err)
		}
		inputs[k] = dag.Result{
			StepID: wi.StepID,
			Data:   wi.Data,
			Err:    inputErr,
		}
	}

	result, _ := s.exec.Execute(r.Context(), step, inputs)

	// Build wire response.
	raw, _ := json.Marshal(result.Data)
	wr := wireResult{
		StepID: result.StepID,
		Data:   json.RawMessage(raw),
	}
	if result.Err != nil {
		s := result.Err.Error()
		wr.Err = &s
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(wr) //nolint:errcheck
}

// errorString is a simple error implementation for deserialized errors.
type errorString string

func (e errorString) Error() string { return string(e) }
