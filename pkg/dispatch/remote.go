package dispatch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/bkmashiro/loom/pkg/dag"
	"github.com/bkmashiro/loom/pkg/parser"
)

// RemoteWorker calls a remote worker endpoint over HTTP JSON.
type RemoteWorker struct {
	endpoint   string // e.g. "http://gpu-node:9000"
	cap        Capability
	httpClient *http.Client
}

// NewRemoteWorker creates a RemoteWorker targeting the given endpoint.
func NewRemoteWorker(endpoint string, cap Capability) *RemoteWorker {
	return &RemoteWorker{
		endpoint:   endpoint,
		cap:        cap,
		httpClient: http.DefaultClient,
	}
}

// Capabilities returns the worker's declared capabilities.
func (w *RemoteWorker) Capabilities() Capability { return w.cap }

// wireStep is the JSON representation of a step for the wire protocol.
type wireStep struct {
	ID   string           `json:"id"`
	Type parser.StepType  `json:"type"`
	Deps []string         `json:"deps"`
	Body string           `json:"body"`
	Lang string           `json:"lang"`
}

// wireResult is the JSON representation of a result on the wire.
type wireResult struct {
	StepID string          `json:"step_id"`
	Data   json.RawMessage `json:"data"`
	Err    *string         `json:"err"`
}

// executeRequest is the JSON request body for POST /execute.
type executeRequest struct {
	Step   wireStep                    `json:"step"`
	Inputs map[string]wireInputResult  `json:"inputs"`
}

// wireInputResult is the wire format for a single input result entry.
type wireInputResult struct {
	StepID string          `json:"step_id"`
	Data   json.RawMessage `json:"data"`
	Err    *string         `json:"err"`
}

// Execute marshals the step and inputs, POSTs to the remote endpoint, and returns the result.
func (w *RemoteWorker) Execute(ctx context.Context, step parser.Step, inputs map[string]dag.Result) (dag.Result, error) {
	// Build wire inputs.
	wireInputs := make(map[string]wireInputResult, len(inputs))
	for k, r := range inputs {
		raw, err := json.Marshal(r.Data)
		if err != nil {
			raw = json.RawMessage("null")
		}
		var errPtr *string
		if r.Err != nil {
			s := r.Err.Error()
			errPtr = &s
		}
		wireInputs[k] = wireInputResult{
			StepID: r.StepID,
			Data:   raw,
			Err:    errPtr,
		}
	}

	reqBody := executeRequest{
		Step: wireStep{
			ID:   step.ID,
			Type: step.Type,
			Deps: step.Deps,
			Body: step.Body,
			Lang: step.Lang,
		},
		Inputs: wireInputs,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return dag.Result{StepID: step.ID, Err: err}, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, w.endpoint+"/execute", bytes.NewReader(body))
	if err != nil {
		return dag.Result{StepID: step.ID, Err: err}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := w.httpClient.Do(httpReq)
	if err != nil {
		return dag.Result{StepID: step.ID, Err: err}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("dispatch: remote worker returned HTTP %d", resp.StatusCode)
		return dag.Result{StepID: step.ID, Err: err}, err
	}

	var wr wireResult
	if err := json.NewDecoder(resp.Body).Decode(&wr); err != nil {
		return dag.Result{StepID: step.ID, Err: err}, err
	}

	result := dag.Result{
		StepID: wr.StepID,
		Data:   wr.Data, // json.RawMessage preserves the wire data
	}
	if wr.Err != nil {
		result.Err = fmt.Errorf("%s", *wr.Err)
	}
	return result, result.Err
}
