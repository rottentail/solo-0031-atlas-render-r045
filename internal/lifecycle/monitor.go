// Package lifecycle tracks whether the process is merely alive or actually
// able to accept work. The two are deliberately separate: a container whose
// data volume is not writable must stay alive and report the real cause,
// instead of crash-looping or answering liveness probes as if it were ready.
package lifecycle

import (
	"atlas-render/internal/persist"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Reason classifies why the service is not ready. It is stable enough for
// probe consumers and dashboards.
const (
	ReasonStarting    = "state_starting"
	ReasonLocked      = "state_locked"
	ReasonInvalid     = "state_invalid"
	ReasonUnusable    = "state_unusable"
	ReasonCommit      = "state_commit_failed"
	ReasonUnavailable = "state_unavailable"
)

// readyCacheTTL bounds how often readiness touches the filesystem. Probes and
// API requests share the cached verdict.
const readyCacheTTL = time.Second

type Monitor struct {
	mu         sync.Mutex
	path       string
	store      *persist.Store
	startupErr error
	checkedAt  time.Time
	cachedErr  error
	hasCached  bool
	now        func() time.Time
}

func NewMonitor(statePath string) *Monitor {
	return &Monitor{path: statePath, now: time.Now}
}

func (m *Monitor) StatePath() string { return m.path }

// SetStartupError records the latest reason the state could not be opened.
// The process keeps retrying, so the value is informative, not terminal.
func (m *Monitor) SetStartupError(e error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startupErr = e
	m.store = nil
	m.hasCached = false
}

// Attach records that the store was opened and is owned by this process.
func (m *Monitor) Attach(st *persist.Store) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.store = st
	m.startupErr = nil
	m.hasCached = false
}

// Ready returns nil when the process can durably accept and commit work.
// Readiness requires an attached store and a state directory that still
// accepts writes; this catches a volume that turned read-only after startup.
func (m *Monitor) Ready() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.store == nil {
		if m.startupErr != nil {
			return m.startupErr
		}
		return fmt.Errorf("%w: state has not been opened yet", persist.ErrUnusable)
	}
	if m.hasCached && m.now().Sub(m.checkedAt) < readyCacheTTL {
		return m.cachedErr
	}
	e := m.store.CheckWritable()
	m.checkedAt = m.now()
	m.cachedErr = e
	m.hasCached = true
	return e
}

// ReadyNow bypasses the cache. It is used by tests and by the check command's
// server side, where a stale positive would be misleading.
func (m *Monitor) ReadyNow() error {
	m.mu.Lock()
	m.hasCached = false
	m.mu.Unlock()
	return m.Ready()
}

func (m *Monitor) IsReady() bool { return m.Ready() == nil }

// Reason maps an error to a stable readiness reason.
func Reason(e error) string {
	switch {
	case e == nil:
		return ""
	case errors.Is(e, persist.ErrLocked):
		return ReasonLocked
	case errors.Is(e, persist.ErrStateInvalid):
		return ReasonInvalid
	case errors.Is(e, persist.ErrCommit):
		return ReasonCommit
	case errors.Is(e, persist.ErrUnusable):
		return ReasonUnusable
	default:
		return ReasonUnavailable
	}
}
