package proxy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"
	"time"

	"github.com/bkmashiro/loom/pkg/dag"
)

// SessionState holds per-session state between conversational rounds.
// The background executor writes PendingResults; the next request reads them.
type SessionState struct {
	// PendingResults holds step results from the last plan execution.
	// nil means no pending results (pass-through mode for next request).
	PendingResults []dag.StepResult

	// LastAssistantMessage is the full text the LLM produced in the round
	// that contained the plan (including fence text). Stored so the proxy
	// can reconstruct it correctly in round N+1's injected messages,
	// regardless of what the client sends.
	LastAssistantMessage string

	// ExecutionDone is closed when background plan execution completes.
	// The next request for this session blocks on this before forwarding.
	// nil means no execution is in progress.
	ExecutionDone <-chan struct{}

	// LastActivity is updated on every request for TTL expiration.
	LastActivity time.Time

	// Mu protects concurrent access.
	Mu sync.Mutex
}

// SessionStore is an in-memory store of SessionState with TTL expiration.
type SessionStore struct {
	mu       sync.Mutex
	sessions map[string]*SessionState
	ttl      time.Duration
	stop     chan struct{}
}

// NewSessionStore creates a SessionStore with the given TTL.
// A cleanup goroutine is started; call Close() to stop it.
func NewSessionStore(ttl time.Duration) *SessionStore {
	s := &SessionStore{
		sessions: make(map[string]*SessionState),
		ttl:      ttl,
		stop:     make(chan struct{}),
	}
	go s.runCleanup()
	return s
}

// Get returns the session state for id, or nil if not found.
func (s *SessionStore) Get(id string) *SessionState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[id]
}

// GetOrCreate returns an existing session or creates a new one.
func (s *SessionStore) GetOrCreate(id string) *SessionState {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sess, ok := s.sessions[id]; ok {
		return sess
	}
	sess := &SessionState{LastActivity: time.Now()}
	s.sessions[id] = sess
	return sess
}

// Len returns the number of active sessions (for metrics/testing).
func (s *SessionStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sessions)
}

// cleanup removes sessions that have been inactive longer than TTL.
// Sessions with an in-flight execution (ExecutionDone != nil) are never removed.
func (s *SessionStore) cleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for id, sess := range s.sessions {
		sess.Mu.Lock()
		inFlight := sess.ExecutionDone != nil
		expired := now.Sub(sess.LastActivity) > s.ttl
		sess.Mu.Unlock()
		if expired && !inFlight {
			delete(s.sessions, id)
		}
	}
}

// runCleanup runs cleanup on a 60-second ticker until Close() is called.
func (s *SessionStore) runCleanup() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.cleanup()
		case <-s.stop:
			return
		}
	}
}

// Close stops the background cleanup goroutine.
func (s *SessionStore) Close() {
	close(s.stop)
}

// DeriveSessionID computes a stable session identifier from the conversation
// messages. It hashes all messages up to (but not including) the last user
// message, so the same conversation prefix always yields the same ID.
//
// If the client sends X-Loom-Session-ID, that takes priority over this.
func DeriveSessionID(messages []Message) string {
	// Find the last user message index.
	lastUserIdx := -1
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role == "user" {
			lastUserIdx = i
			break
		}
	}

	var prefix []Message
	if lastUserIdx > 0 {
		prefix = messages[:lastUserIdx]
	}
	// If lastUserIdx == 0 or -1, prefix is nil → hash of empty slice.

	h := sha256.New()
	enc := json.NewEncoder(h)
	enc.Encode(prefix) //nolint:errcheck
	return hex.EncodeToString(h.Sum(nil))[:16]
}
