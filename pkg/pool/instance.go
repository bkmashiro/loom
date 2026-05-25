package pool

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/tetratelabs/wazero/api"
)

// wasmInstance implements Instance. It holds a single instantiated WASM module
// together with its memory snapshot. Run calls the guest's evaluate export and
// restores the snapshot afterwards so the next caller sees a clean state.
type wasmInstance struct {
	lang    Language
	mod     api.Module
	allocFn api.Function
	evalFn  api.Function

	// snapshot holds a copy of the guest's linear memory taken right after
	// instantiation. It is used to restore state after every Run.
	snapshot []byte

	// healthy is false when snapshot restore fails; an unhealthy instance must
	// not be returned to the pool.
	healthy bool

	// pool is a back-reference used by Release to return the instance.
	pool *wazeroPool

	// ephemeral marks instances created by AcquireWithFS that should be closed
	// on Release rather than returned to the pre-warm channel.
	ephemeral bool
}

// requestEnvelope is marshalled and sent into the guest via alloc+evaluate.
type requestEnvelope struct {
	Method string         `json:"method"`
	Params map[string]any `json:"params"`
}

// Language implements Instance.
func (i *wasmInstance) Language() Language { return i.lang }

// IsHealthy reports whether the instance can be safely returned to the pool.
func (i *wasmInstance) IsHealthy() bool { return i.healthy }

// Run implements Instance.
func (i *wasmInstance) Run(ctx context.Context, code string, inputs map[string]any) (any, error) {
	if inputs == nil {
		inputs = map[string]any{}
	}

	params := map[string]any{
		"code":   code,
		"inputs": inputs,
	}

	result, err := i.call(ctx, "execute", params)

	// Always restore snapshot — even on error — so state is clean for the next
	// caller. If restore fails, mark the instance unhealthy.
	if restoreErr := i.restoreSnapshot(); restoreErr != nil {
		i.healthy = false
		if err == nil {
			err = fmt.Errorf("pool: restore snapshot: %w", restoreErr)
		}
	}

	if err != nil {
		return nil, err
	}
	return result, nil
}

// call marshals the request, writes it into guest memory via alloc, calls
// evaluate, and reads back the length-prefixed JSON response.
func (i *wasmInstance) call(ctx context.Context, method string, params map[string]any) (map[string]any, error) {
	env := requestEnvelope{Method: method, Params: params}
	reqBytes, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("pool: marshal request: %w", err)
	}
	reqLen := uint64(len(reqBytes))

	// Allocate guest memory for the request.
	if i.allocFn == nil {
		return nil, fmt.Errorf("pool: guest module missing 'alloc' export")
	}
	allocRes, err := i.allocFn.Call(ctx, reqLen)
	if err != nil {
		return nil, fmt.Errorf("pool: alloc(%d): %w", reqLen, err)
	}
	reqPtr := allocRes[0]
	if reqPtr == 0 {
		return nil, fmt.Errorf("pool: alloc returned NULL")
	}

	// Write request into guest memory.
	mem := i.mod.Memory()
	if mem == nil {
		return nil, fmt.Errorf("pool: guest module has no linear memory")
	}
	if !mem.Write(uint32(reqPtr), reqBytes) {
		return nil, fmt.Errorf("pool: failed to write %d bytes at ptr=%d", len(reqBytes), reqPtr)
	}

	// Call evaluate.
	if i.evalFn == nil {
		return nil, fmt.Errorf("pool: guest module missing 'evaluate' export")
	}
	evalRes, err := i.evalFn.Call(ctx, reqPtr, reqLen)
	if err != nil {
		return nil, fmt.Errorf("pool: evaluate: %w", err)
	}
	resPtr := uint32(evalRes[0])

	// Read 4-byte LE length prefix.
	lenBytes, ok := mem.Read(resPtr, 4)
	if !ok {
		return nil, fmt.Errorf("pool: failed to read response length at ptr=%d", resPtr)
	}
	resLen := binary.LittleEndian.Uint32(lenBytes)

	// Bounds check before reading body.
	if uint64(resPtr)+4+uint64(resLen) > uint64(mem.Size()) {
		return nil, fmt.Errorf("pool: response out of bounds: resPtr=%d resLen=%d memSize=%d",
			resPtr, resLen, mem.Size())
	}
	resBytes, ok := mem.Read(resPtr+4, resLen)
	if !ok {
		return nil, fmt.Errorf("pool: failed to read %d response bytes at ptr=%d", resLen, resPtr+4)
	}

	var result map[string]any
	if err := json.Unmarshal(resBytes, &result); err != nil {
		return nil, fmt.Errorf("pool: unmarshal response: %w", err)
	}
	return result, nil
}

// takeSnapshot copies the current linear memory into i.snapshot.
func (i *wasmInstance) takeSnapshot() error {
	mem := i.mod.Memory()
	if mem == nil {
		i.snapshot = nil
		return nil
	}
	size := mem.Size()
	if size == 0 {
		i.snapshot = nil
		return nil
	}
	buf, ok := mem.Read(0, size)
	if !ok {
		return fmt.Errorf("pool: snapshot: could not read %d bytes", size)
	}
	// Make an owned copy — mem.Read may return a wazero-owned slice.
	i.snapshot = make([]byte, len(buf))
	copy(i.snapshot, buf)
	return nil
}

// restoreSnapshot writes i.snapshot back into guest linear memory.
func (i *wasmInstance) restoreSnapshot() error {
	if i.snapshot == nil {
		return nil
	}
	mem := i.mod.Memory()
	if mem == nil {
		return nil
	}
	if !mem.Write(0, i.snapshot) {
		return fmt.Errorf("pool: restore: failed to write %d bytes", len(i.snapshot))
	}
	return nil
}

// shutdown closes the underlying module instance.
func (i *wasmInstance) shutdown(ctx context.Context) error {
	if i.mod == nil {
		return nil
	}
	if err := i.mod.Close(ctx); err != nil {
		return fmt.Errorf("pool: close module: %w", err)
	}
	i.mod = nil
	return nil
}
