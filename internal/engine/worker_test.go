package engine

import (
	"atlas-render/internal/model"
	"atlas-render/internal/persist"
	"atlas-render/internal/scene"
	"errors"
	"sync"
	"testing"
	"time"
)

// faultyStore wraps a real store and fails commits while fail is set.
type faultyStore struct {
	inner *persist.Store
	mu    sync.Mutex
	fail  error
}

func (f *faultyStore) Read() model.State { return f.inner.Read() }
func (f *faultyStore) Update(fn func(*model.State) error) error {
	f.mu.Lock()
	e := f.fail
	f.mu.Unlock()
	if e != nil {
		return e
	}
	return f.inner.Update(fn)
}
func (f *faultyStore) setFailure(e error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail = e
}

// seed queues one tiny render and returns its job id.
func seed(t *testing.T, st *persist.Store) string {
	t.Helper()
	doc := model.Document{Width: 4, Height: 4, Background: "#000000", Layers: []model.Layer{{
		ID: "box", Kind: "rectangle", Width: 4, Height: 4, Opacity: 1,
		Transform: model.Transform{ScaleX: 1, ScaleY: 1},
		Fill:      model.Paint{Kind: "solid", Color: "#FFFFFF"},
	}}}
	if e := scene.Validate(doc); e != nil {
		t.Fatalf("invalid test document: %v", e)
	}
	snap := model.Snapshot{ID: "snap-1", SceneID: "scene", Revision: 1, State: "active", Document: doc, Assets: map[string]model.Asset{}, CreatedAt: time.Now().UTC()}
	snap.Digest = model.SnapshotDigest(snap)
	e := st.Update(func(s *model.State) error {
		s.Scenes["scene"] = model.Scene{ID: "scene", Name: "s", Revision: 1, State: "active", Document: doc, UpdatedAt: time.Now().UTC()}
		s.Snapshots[snap.ID] = snap
		s.Jobs["job-1"] = model.Job{ID: "job-1", SnapshotID: snap.ID, State: "queued", Scale: 1, Attempt: 1, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(), Payload: snap}
		return nil
	})
	if e != nil {
		t.Fatalf("seed: %v", e)
	}
	return "job-1"
}

func TestResultCommitIsRetriedAfterTransientFailure(t *testing.T) {
	st, e := persist.Open(t.TempDir() + "/state.json")
	if e != nil {
		t.Fatalf("open: %v", e)
	}
	defer st.Close()
	jobID := seed(t, st)
	faulty := &faultyStore{inner: st, fail: persist.ErrCommit}
	w := New(faulty)
	// Claiming also commits, so allow the claim and fail only the result.
	faulty.fail = nil
	job, ok, e := w.claim()
	if e != nil || !ok || job.ID != jobID {
		t.Fatalf("claim: ok=%v e=%v", ok, e)
	}
	if got := st.Read().Jobs[jobID].State; got != "running" {
		t.Fatalf("claimed state: %s", got)
	}
	// Simulate a rendered outcome whose terminal commit fails once.
	faulty.setFailure(persist.ErrCommit)
	w.pending = &pendingResult{job: job}
	if e = w.retryPending(); !errors.Is(e, persist.ErrCommit) {
		t.Fatalf("expected deferred commit error, got %v", e)
	}
	if w.pending == nil {
		t.Fatal("pending result was dropped instead of retried")
	}
	if got := st.Read().Jobs[jobID].State; got != "running" {
		t.Fatalf("failed commit changed the job state: %s", got)
	}
	faulty.setFailure(nil)
	if e = w.retryPending(); e != nil {
		t.Fatalf("retry: %v", e)
	}
	if w.pending != nil {
		t.Fatal("pending result not cleared after commit")
	}
	if got := st.Read().Jobs[jobID].State; got != "succeeded" {
		t.Fatalf("job did not succeed after retry: %s", got)
	}
}

func TestDeferredResultDoesNotOverrideCancellation(t *testing.T) {
	st, e := persist.Open(t.TempDir() + "/state.json")
	if e != nil {
		t.Fatalf("open: %v", e)
	}
	defer st.Close()
	jobID := seed(t, st)
	faulty := &faultyStore{inner: st}
	w := New(faulty)
	job, ok, e := w.claim()
	if e != nil || !ok {
		t.Fatalf("claim: %v", e)
	}
	// Cancel commits while the result waits, then the deferred commit runs.
	if e = st.Update(func(s *model.State) error {
		j := s.Jobs[jobID]
		j.State = "cancelled"
		s.Jobs[jobID] = j
		return nil
	}); e != nil {
		t.Fatalf("cancel: %v", e)
	}
	w.pending = &pendingResult{job: job}
	if e = w.retryPending(); e != nil {
		t.Fatalf("retry: %v", e)
	}
	if got := st.Read().Jobs[jobID].State; got != "cancelled" {
		t.Fatalf("stale result overwrote cancellation: %s", got)
	}
	if got := len(st.Read().Artifacts); got != 0 {
		t.Fatalf("stale result produced %d artifacts", got)
	}
}
