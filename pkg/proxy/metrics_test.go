package proxy

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetrics_ServeHTTP(t *testing.T) {
	m := &Metrics{}
	m.RequestsTotal.Store(42)
	m.PlansDetected.Store(10)
	m.Injections.Store(8)
	m.PendingExecs.Store(2)

	// Create handler with session count getter
	handler := m.Handler(func() int { return 5 })

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	body := rr.Body.String()
	checks := []string{
		"loom_requests_total 42",
		"loom_plans_detected_total 10",
		"loom_injections_total 8",
		"loom_pending_executions 2",
		"loom_sessions_active 5",
	}
	for _, c := range checks {
		if !strings.Contains(body, c) {
			t.Errorf("missing %q in metrics output:\n%s", c, body)
		}
	}
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); !strings.Contains(ct, "text/plain") {
		t.Errorf("expected text/plain content-type, got %q", ct)
	}
}

func TestMetrics_AtomicCounters(t *testing.T) {
	m := &Metrics{}
	m.RequestsTotal.Add(1)
	m.RequestsTotal.Add(1)
	m.PlansDetected.Add(1)
	m.PendingExecs.Add(1)
	m.PendingExecs.Add(-1)

	if m.RequestsTotal.Load() != 2 {
		t.Errorf("expected RequestsTotal=2, got %d", m.RequestsTotal.Load())
	}
	if m.PendingExecs.Load() != 0 {
		t.Errorf("expected PendingExecs=0, got %d", m.PendingExecs.Load())
	}
}
