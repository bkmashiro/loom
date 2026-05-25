package primitives

import (
	"sync"
	"testing"
)

// ---- KV Store tests ----

func TestKV_SetGet(t *testing.T) {
	kv := NewMemoryKV()
	kv.Set("name", "loom")
	v, ok := kv.Get("name")
	if !ok {
		t.Fatal("expected key 'name' to exist")
	}
	if v != "loom" {
		t.Fatalf("expected 'loom', got %v", v)
	}
}

func TestKV_MissingKey(t *testing.T) {
	kv := NewMemoryKV()
	_, ok := kv.Get("missing")
	if ok {
		t.Fatal("expected missing key to return false")
	}
}

func TestKV_Del(t *testing.T) {
	kv := NewMemoryKV()
	kv.Set("x", 42)
	kv.Del("x")
	_, ok := kv.Get("x")
	if ok {
		t.Fatal("expected key 'x' to be deleted")
	}
}

func TestKV_DelMissing(t *testing.T) {
	kv := NewMemoryKV()
	// Deleting a non-existent key should not panic
	kv.Del("nonexistent")
}

func TestKV_ConcurrentAccess(t *testing.T) {
	kv := NewMemoryKV()
	var wg sync.WaitGroup
	const goroutines = 100

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := "key"
			kv.Set(key, n)
			kv.Get(key)
			kv.Del(key)
		}(i)
	}
	wg.Wait()
}

// ---- HTTP parser tests ----

func TestParseHTTPRequest_GET(t *testing.T) {
	req, err := ParseHTTPRequest("GET https://example.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Method != "GET" {
		t.Errorf("expected GET, got %q", req.Method)
	}
	if req.URL != "https://example.com" {
		t.Errorf("expected URL https://example.com, got %q", req.URL)
	}
	if req.Body != "" {
		t.Errorf("expected empty body, got %q", req.Body)
	}
	if req.Headers != nil {
		t.Errorf("expected nil headers, got %v", req.Headers)
	}
}

func TestParseHTTPRequest_DELETE(t *testing.T) {
	req, err := ParseHTTPRequest("DELETE https://example.com/resource/1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Method != "DELETE" {
		t.Errorf("expected DELETE, got %q", req.Method)
	}
}

func TestParseHTTPRequest_POST_WithBody(t *testing.T) {
	req, err := ParseHTTPRequest(`POST https://api.example.com/data {"key": "val"}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Method != "POST" {
		t.Errorf("expected POST, got %q", req.Method)
	}
	if req.URL != "https://api.example.com/data" {
		t.Errorf("unexpected URL: %q", req.URL)
	}
	if req.Body != `{"key": "val"}` {
		t.Errorf("unexpected body: %q", req.Body)
	}
}

func TestParseHTTPRequest_PUT_WithBodyAndHeaders(t *testing.T) {
	line := `PUT https://api.example.com/item/1 {"name": "test"} headers: {"Authorization": "Bearer tok"}`
	req, err := ParseHTTPRequest(line)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Method != "PUT" {
		t.Errorf("expected PUT, got %q", req.Method)
	}
	if req.Body != `{"name": "test"}` {
		t.Errorf("unexpected body: %q", req.Body)
	}
	if req.Headers == nil {
		t.Fatal("expected headers to be non-nil")
	}
	if req.Headers["Authorization"] != "Bearer tok" {
		t.Errorf("unexpected Authorization header: %q", req.Headers["Authorization"])
	}
}

func TestParseHTTPRequest_UnknownMethod(t *testing.T) {
	_, err := ParseHTTPRequest("FETCH https://example.com")
	if err == nil {
		t.Fatal("expected error for unknown method")
	}
}

func TestParseHTTPRequest_MissingURL(t *testing.T) {
	_, err := ParseHTTPRequest("GET")
	if err == nil {
		t.Fatal("expected error for missing URL")
	}
}

func TestParseHTTPRequest_LowercaseMethod(t *testing.T) {
	// method should be normalized to uppercase
	req, err := ParseHTTPRequest("get https://example.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Method != "GET" {
		t.Errorf("expected GET, got %q", req.Method)
	}
}

// ---- FS parser tests ----

func TestParseFSCommand_Read(t *testing.T) {
	cmd, err := ParseFSCommand("read /etc/hosts")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cmd.Op != "read" {
		t.Errorf("expected op 'read', got %q", cmd.Op)
	}
	if cmd.Path != "/etc/hosts" {
		t.Errorf("unexpected path: %q", cmd.Path)
	}
	if cmd.Content != "" {
		t.Errorf("expected empty content, got %q", cmd.Content)
	}
}

func TestParseFSCommand_Write(t *testing.T) {
	cmd, err := ParseFSCommand("write /tmp/out.txt hello world")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cmd.Op != "write" {
		t.Errorf("expected op 'write', got %q", cmd.Op)
	}
	if cmd.Path != "/tmp/out.txt" {
		t.Errorf("unexpected path: %q", cmd.Path)
	}
	if cmd.Content != "hello world" {
		t.Errorf("unexpected content: %q", cmd.Content)
	}
}

func TestParseFSCommand_Append(t *testing.T) {
	cmd, err := ParseFSCommand("append /var/log/app.log new line")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cmd.Op != "append" {
		t.Errorf("expected op 'append', got %q", cmd.Op)
	}
	if cmd.Content != "new line" {
		t.Errorf("unexpected content: %q", cmd.Content)
	}
}

func TestParseFSCommand_LS(t *testing.T) {
	cmd, err := ParseFSCommand("ls /home/user")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cmd.Op != "ls" {
		t.Errorf("expected op 'ls', got %q", cmd.Op)
	}
	if cmd.Path != "/home/user" {
		t.Errorf("unexpected path: %q", cmd.Path)
	}
}

func TestParseFSCommand_UnknownOp(t *testing.T) {
	_, err := ParseFSCommand("delete /tmp/file")
	if err == nil {
		t.Fatal("expected error for unknown op")
	}
}

func TestParseFSCommand_Empty(t *testing.T) {
	_, err := ParseFSCommand("")
	if err == nil {
		t.Fatal("expected error for empty input")
	}
}
