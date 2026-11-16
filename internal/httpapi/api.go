package httpapi

import (
	"atlas-render/internal/engine"
	"atlas-render/internal/lifecycle"
	"atlas-render/internal/model"
	"atlas-render/internal/persist"
	"atlas-render/internal/service"
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

type API struct {
	Service *service.Service
	Worker  *engine.Worker
	Health  *lifecycle.Monitor
}

func write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func fail(w http.ResponseWriter, e error) {
	var de *model.Error
	if errors.As(e, &de) {
		write(w, de.Status, map[string]string{"code": de.Code, "error": de.Message})
		return
	}
	switch {
	case errors.Is(e, persist.ErrLocked):
		write(w, 503, map[string]string{"code": lifecycle.ReasonLocked, "error": e.Error()})
	case errors.Is(e, persist.ErrStateInvalid):
		write(w, 503, map[string]string{"code": lifecycle.ReasonInvalid, "error": e.Error()})
	case errors.Is(e, persist.ErrUnusable):
		// Same vocabulary as /readyz's reason field.
		write(w, 503, map[string]string{"code": lifecycle.ReasonUnusable, "error": e.Error()})
	case errors.Is(e, persist.ErrCommit):
		write(w, 503, map[string]string{"code": lifecycle.ReasonCommit, "error": e.Error()})
	default:
		write(w, 500, map[string]string{"code": "internal", "error": "operation could not be committed"})
	}
}
func decode(w http.ResponseWriter, r *http.Request, dst any) error {
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 12<<20))
	d.DisallowUnknownFields()
	if e := d.Decode(dst); e != nil {
		return model.Invalid("invalid JSON: %v", e)
	}
	var extra any
	if e := d.Decode(&extra); !errors.Is(e, io.EOF) {
		return model.Invalid("expected exactly one JSON value")
	}
	return nil
}
func (a API) storeReady(w http.ResponseWriter, r *http.Request) bool {
	if a.Service != nil && a.Service.Store() != nil {
		return true
	}
	e := persist.ErrUnusable
	if a.Health != nil {
		e = a.Health.Ready()
	}
	write(w, 503, map[string]string{"code": lifecycle.Reason(e), "error": e.Error()})
	return false
}
func (a API) ready(w http.ResponseWriter, r *http.Request) {
	if a.Health == nil {
		write(w, 200, map[string]any{"status": "ready", "store": "attached"})
		return
	}
	e := a.Health.Ready()
	if e != nil {
		write(w, 503, map[string]any{"status": "not_ready", "reason": lifecycle.Reason(e), "error": e.Error(), "statePath": a.Health.StatePath()})
		return
	}
	write(w, 200, map[string]any{"status": "ready", "statePath": a.Health.StatePath()})
}

func (a API) Handler() http.Handler {
	m := http.NewServeMux()
	// Liveness answers whether the process is alive and serving HTTP. It does
	// not consult the data volume, so a permission or volume problem is never
	// mistaken for a dead process that should be restarted.
	live := func(w http.ResponseWriter, r *http.Request) {
		write(w, 200, map[string]string{"status": "live"})
	}
	m.HandleFunc("GET /healthz", live)
	m.HandleFunc("GET /livez", live)
	// Readiness answers whether committed work can be accepted right now.
	m.HandleFunc("GET /readyz", a.ready)
	guard := func(h http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if a.storeReady(w, r) {
				h(w, r)
			}
		}
	}
	m.HandleFunc("PUT /v1/scenes/{id}", guard(a.putScene))
	m.HandleFunc("GET /v1/scenes/{id}", guard(a.getScene))
	m.HandleFunc("POST /v1/scenes/{id}/state", guard(a.sceneState))
	m.HandleFunc("POST /v1/scenes/{id}/snapshots", guard(a.freeze))
	m.HandleFunc("PUT /v1/assets/{id}", guard(a.importAsset))
	m.HandleFunc("POST /v1/assets/{id}/retire", guard(a.retireAsset))
	m.HandleFunc("GET /v1/snapshots/{id}", guard(a.getSnapshot))
	m.HandleFunc("POST /v1/snapshots/{id}/revoke", guard(a.revoke))
	m.HandleFunc("GET /v1/snapshots/{id}/package", guard(a.exportPackage))
	m.HandleFunc("POST /v1/imports/{id}", guard(a.importPackage))
	m.HandleFunc("POST /v1/jobs", guard(a.submit))
	m.HandleFunc("GET /v1/jobs/{id}", guard(a.getJob))
	m.HandleFunc("POST /v1/jobs/{id}/cancel", guard(a.cancel))
	m.HandleFunc("POST /v1/jobs/{id}/retry", guard(a.retry))
	m.HandleFunc("GET /v1/artifacts/{id}", guard(a.download))
	m.HandleFunc("DELETE /v1/artifacts/{id}", guard(a.purge))
	m.HandleFunc("POST /v1/compare", guard(a.compare))
	m.HandleFunc("GET /v1/status", guard(a.status))
	m.HandleFunc("GET /v1/events", guard(a.events))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		m.ServeHTTP(w, r)
	})
}
