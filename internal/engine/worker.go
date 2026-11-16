package engine

import (
	"atlas-render/internal/codec"
	"atlas-render/internal/model"
	"atlas-render/internal/persist"
	"atlas-render/internal/raster"
	"context"
	"errors"
	"log"
	"sort"
	"sync"
	"time"
)

// StateStore is the persistence capability the worker needs. It is an
// interface so the deferred-commit path can be tested with a fault injector.
type StateStore interface {
	Read() model.State
	Update(func(*model.State) error) error
}

type Worker struct {
	store  StateStore
	mu     sync.Mutex
	active map[string]context.CancelFunc
	// pending keeps a finished render whose terminal commit failed (for
	// example because the state volume turned read-only). The result stays in
	// memory and is retried on later ticks instead of being lost or leaving
	// the task running forever.
	pending *pendingResult
}

type pendingResult struct {
	job   model.Job
	plan  raster.Plan
	data  []byte
	err   error
	tries int
}

func New(st StateStore) *Worker {
	return &Worker{store: st, active: map[string]context.CancelFunc{}}
}
func (w *Worker) Recover() error {
	return w.store.Update(func(s *model.State) error {
		for id, j := range s.Jobs {
			if j.State == "running" {
				j.State = "queued"
				j.UpdatedAt = time.Now().UTC()
				s.Jobs[id] = j
				s.Emit("job.recovered", id)
			}
		}
		return nil
	})
}
func (w *Worker) Cancel(id string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if cancel := w.active[id]; cancel != nil {
		cancel()
	}
}
func (w *Worker) Run(ctx context.Context) {
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if e := w.retryPending(); e != nil {
				log.Printf("render worker: retrying result commit: %v", e)
				continue
			}
			if e := w.step(ctx); e != nil {
				log.Printf("render worker: %v", e)
			}
		}
	}
}

// retryPending re-attempts a terminal commit that previously failed. Because
// the claim and the outcome are compared under the store lock, a stale result
// still cannot overwrite a cancellation or a newer attempt.
func (w *Worker) retryPending() error {
	p := w.pending
	if p == nil {
		return nil
	}
	e := w.commit(p)
	if e == nil {
		w.pending = nil
		log.Printf("render worker: deferred result for %s committed after %d retries", p.job.ID, p.tries)
		return nil
	}
	p.tries++
	if !errors.Is(e, persist.ErrCommit) && !errors.Is(e, persist.ErrUnusable) {
		// The state itself rejected the outcome (validation or a conflicting
		// transition). Dropping the retry is safer than looping forever.
		w.pending = nil
	}
	return e
}

func (w *Worker) claim() (model.Job, bool, error) {
	var job model.Job
	found := false
	state := w.store.Read()
	ids := []string{}
	for id, j := range state.Jobs {
		if j.State == "queued" {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return job, false, nil
	}
	sort.Slice(ids, func(i, j int) bool {
		a, b := state.Jobs[ids[i]], state.Jobs[ids[j]]
		if a.CreatedAt.Equal(b.CreatedAt) {
			return a.ID < b.ID
		}
		return a.CreatedAt.Before(b.CreatedAt)
	})
	e := w.store.Update(func(s *model.State) error {
		j := s.Jobs[ids[0]]
		if j.State != "queued" {
			return nil
		}
		j.State = "running"
		j.UpdatedAt = time.Now().UTC()
		s.Jobs[j.ID] = j
		s.Emit("job.running", j.ID)
		job = j
		found = true
		return nil
	})
	return job, found, e
}
func (w *Worker) step(parent context.Context) error {
	j, ok, e := w.claim()
	if e != nil || !ok {
		return e
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	w.mu.Lock()
	w.active[j.ID] = cancel
	w.mu.Unlock()
	defer func() { w.mu.Lock(); delete(w.active, j.ID); w.mu.Unlock() }()
	// Cancellation can commit between claim and tracking; consult the committed state.
	current := w.store.Read().Jobs[j.ID]
	if current.State != "running" {
		cancel()
	}
	plan, e := raster.Compile(j.Payload, j.Scale)
	var data []byte
	if e == nil {
		rendered, imgErr := plan.Render(ctx)
		e = imgErr
		if e == nil {
			data, e = codec.Encode(rendered)
		}
	}
	if parent.Err() != nil {
		return nil
	}
	p := &pendingResult{job: j, plan: plan, data: data, err: e}
	if e = w.commit(p); e != nil {
		if errors.Is(e, persist.ErrCommit) || errors.Is(e, persist.ErrUnusable) {
			w.pending = p
			log.Printf("render worker: result for %s not yet committed, will retry: %v", j.ID, e)
		}
		return e
	}
	return nil
}

func (w *Worker) commit(p *pendingResult) error {
	j := p.job
	e := p.err
	return w.store.Update(func(s *model.State) error {
		v := s.Jobs[j.ID]
		if v.State != "running" || v.Attempt != j.Attempt {
			return nil
		}
		v.UpdatedAt = time.Now().UTC()
		if e != nil {
			if errors.Is(e, context.Canceled) {
				v.State = "cancelled"
			} else {
				v.State = "failed"
				v.Error = e.Error()
			}
			s.Jobs[v.ID] = v
			s.Emit("job."+v.State, v.ID)
			return nil
		}
		a := model.Artifact{ID: model.ID("png", s.Sequence+1), JobID: v.ID, State: "live", Digest: model.Digest(p.data), Width: p.plan.Width, Height: p.plan.Height, PNG: p.data}
		s.Artifacts[a.ID] = a
		v.State = "succeeded"
		v.ArtifactID = a.ID
		s.Jobs[v.ID] = v
		s.Emit("job.succeeded", v.ID)
		return nil
	})
}
