package proxy

import (
	"fmt"
	"net/http"
	"sync/atomic"
)

// Metrics holds counters for the proxy.
type Metrics struct {
	PlansDetected atomic.Int64
	Injections    atomic.Int64
	PendingExecs  atomic.Int64 // incremented when bg exec starts, decremented when done
	RequestsTotal atomic.Int64
}

// Handler returns an http.Handler that writes Prometheus-format metrics.
// sessionCount is called at request time to get the current active session count.
func (m *Metrics) Handler(sessionCount func() int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "# HELP loom_requests_total Total chat completion requests handled\n")
		fmt.Fprintf(w, "# TYPE loom_requests_total counter\n")
		fmt.Fprintf(w, "loom_requests_total %d\n", m.RequestsTotal.Load())
		fmt.Fprintf(w, "\n")
		fmt.Fprintf(w, "# HELP loom_plans_detected_total Requests where a Loom plan was detected\n")
		fmt.Fprintf(w, "# TYPE loom_plans_detected_total counter\n")
		fmt.Fprintf(w, "loom_plans_detected_total %d\n", m.PlansDetected.Load())
		fmt.Fprintf(w, "\n")
		fmt.Fprintf(w, "# HELP loom_injections_total Times results were injected into a new round\n")
		fmt.Fprintf(w, "# TYPE loom_injections_total counter\n")
		fmt.Fprintf(w, "loom_injections_total %d\n", m.Injections.Load())
		fmt.Fprintf(w, "\n")
		fmt.Fprintf(w, "# HELP loom_pending_executions Current background executions in flight\n")
		fmt.Fprintf(w, "# TYPE loom_pending_executions gauge\n")
		fmt.Fprintf(w, "loom_pending_executions %d\n", m.PendingExecs.Load())
		fmt.Fprintf(w, "\n")
		fmt.Fprintf(w, "# HELP loom_sessions_active Current active sessions in the session store\n")
		fmt.Fprintf(w, "# TYPE loom_sessions_active gauge\n")
		fmt.Fprintf(w, "loom_sessions_active %d\n", sessionCount())
	})
}

// ServeHTTP implements http.Handler with zero active sessions (for use without a session store).
func (m *Metrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.Handler(func() int { return 0 }).ServeHTTP(w, r)
}
