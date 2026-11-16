package main

import (
	"atlas-render/internal/engine"
	"atlas-render/internal/httpapi"
	"atlas-render/internal/lifecycle"
	"atlas-render/internal/persist"
	"atlas-render/internal/service"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	defaultAddr  = "127.0.0.1:8763"
	defaultState = "var/atlas-state.json"

	// retryInterval is how often a state path that could not be opened (for
	// example because the data volume is not mounted yet or lacks write
	// permission) is tried again while the process keeps serving liveness.
	retryInterval = 2 * time.Second
	// shutdownLimit bounds how long in-flight HTTP requests may drain.
	shutdownLimit = 15 * time.Second
	// workerLimit bounds how long an interrupted render may take to stop.
	workerLimit = 10 * time.Second
)

// Exit codes are part of the container contract and are documented in
// docs/operations.md.
const (
	exitFailure = 1 // configuration, listener, or generic fatal failure
	exitLocked  = 3 // another process holds the same state path
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "check" {
		os.Exit(checkCommand(os.Args[2:]))
	}
	e := run()
	if e == nil {
		return
	}
	log.Print(e)
	if errors.Is(e, persist.ErrLocked) {
		log.Print("refusing to run: exactly one instance may own a state path")
		os.Exit(exitLocked)
	}
	os.Exit(exitFailure)
}

type config struct {
	addr  string
	state string
}

func configFromEnv() config {
	c := config{addr: os.Getenv("ATLAS_ADDR"), state: os.Getenv("ATLAS_STATE")}
	if c.addr == "" {
		c.addr = defaultAddr
	}
	if c.state == "" {
		c.state = defaultState
	}
	return c
}

func run() error {
	cfg := configFromEnv()
	health := lifecycle.NewMonitor(cfg.state)
	api := httpapi.API{Service: &service.Service{}, Health: health}

	// The listener is opened first so liveness is observable even when the
	// data volume is missing or not writable. Readiness reports that separately.
	ln, e := net.Listen("tcp", cfg.addr)
	if e != nil {
		return fmt.Errorf("listen on %s: %w", cfg.addr, e)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	mgr := newManager(cfg, health, &api)
	e = mgr.start()
	if errors.Is(e, persist.ErrLocked) {
		_ = ln.Close()
		return e
	}

	srv := &http.Server{
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       20 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16384,
	}
	errc := make(chan error, 1)
	go func() {
		log.Printf("Atlas Render listening on %s (state %s)", cfg.addr, cfg.state)
		errc <- srv.Serve(ln)
	}()

	retry := time.NewTicker(retryInterval)
	defer retry.Stop()
	for {
		select {
		case e = <-errc:
			stop()
			mgr.stop()
			if errors.Is(e, http.ErrServerClosed) {
				return nil
			}
			return e
		case <-ctx.Done():
			// Restore the default signal action: a second signal now stops the
			// process immediately instead of being swallowed.
			stop()
			log.Printf("shutdown signal received; draining requests")
			shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownLimit)
			_ = srv.Shutdown(shutdownCtx)
			cancel()
			mgr.stop()
			if e = <-errc; e != nil && !errors.Is(e, http.ErrServerClosed) {
				return e
			}
			return nil
		case <-retry.C:
			if !mgr.attached() {
				if e = mgr.start(); errors.Is(e, persist.ErrLocked) {
					// A competing process appeared after we were live. Do not
					// keep two writers: stop and fail clearly.
					_ = srv.Close()
					return e
				}
			}
		}
	}
}

// manager owns the optional store and its worker. The store stays absent when
// the data volume is unusable; readiness explains why while liveness stays OK.
type manager struct {
	cfg       config
	health    *lifecycle.Monitor
	api       *httpapi.API
	store     *persist.Store
	workerCtx context.Context
	stopWork  context.CancelFunc
	done      chan struct{}
}

func newManager(cfg config, health *lifecycle.Monitor, api *httpapi.API) *manager {
	return &manager{cfg: cfg, health: health, api: api}
}

func (m *manager) attached() bool { return m.store != nil }

// start opens the state and launches the worker. A lock conflict is returned
// to the caller (a second writer must not run); every other failure is
// recorded in the readiness monitor and retried later.
func (m *manager) start() error {
	st, e := persist.Open(m.cfg.state)
	if e != nil {
		m.health.SetStartupError(e)
		if errors.Is(e, persist.ErrLocked) {
			log.Printf("state lock is held by another instance: %v", e)
			return e
		}
		log.Printf("state not ready, will retry every %s: %v", retryInterval, e)
		return nil
	}
	worker := engine.New(st)
	if e = worker.Recover(); e != nil {
		log.Printf("state recovery failed, will retry: %v", e)
		_ = st.Close()
		m.health.SetStartupError(fmt.Errorf("%w: startup recovery failed: %v", persist.ErrStateInvalid, e))
		return nil
	}
	m.store = st
	m.workerCtx, m.stopWork = context.WithCancel(context.Background())
	done := make(chan struct{})
	m.done = done
	m.api.Service.Attach(st)
	m.api.Worker = worker
	m.health.Attach(st)
	go func() {
		defer close(done)
		worker.Run(m.workerCtx)
	}()
	log.Printf("state opened and recovered from %s", st.Path())
	return nil
}

// stop cancels the worker, waits briefly for the interrupted render to finish,
// and only then releases the state lock.
func (m *manager) stop() {
	if m.store == nil {
		return
	}
	if m.stopWork != nil {
		m.stopWork()
	}
	if m.done != nil {
		select {
		case <-m.done:
		case <-time.After(workerLimit):
			log.Printf("render worker did not stop within %s; releasing state anyway", workerLimit)
		}
	}
	if e := m.store.Close(); e != nil {
		log.Printf("close state: %v", e)
	}
	m.store = nil
}

// checkCommand probes a running instance. Exit status is 0 when the selected
// mode passes, 1 when it fails, and 2 for a usage error.
func checkCommand(args []string) int {
	fs := flag.NewFlagSet("check", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	url := fs.String("url", "", "health endpoint; defaults to the port in ATLAS_ADDR")
	mode := fs.String("mode", "ready", "probe mode: live or ready")
	timeout := fs.Duration("timeout", 3*time.Second, "request timeout")
	if e := fs.Parse(args); e != nil {
		fmt.Fprintln(os.Stderr, "usage: atlas check [-mode live|ready] [-url URL] [-timeout DURATION]")
		return 2
	}
	path := ""
	switch *mode {
	case "live":
		path = "/livez"
	case "ready":
		path = "/readyz"
	default:
		fmt.Fprintf(os.Stderr, "atlas check: unknown mode %q (want live or ready)\n", *mode)
		return 2
	}
	target := *url
	if target == "" {
		target = "http://" + probeAddress(os.Getenv("ATLAS_ADDR")) + path
	}
	client := &http.Client{Timeout: *timeout}
	resp, e := client.Get(target)
	if e != nil {
		report := map[string]any{"mode": *mode, "ok": false, "url": target, "error": e.Error()}
		emit(report)
		return 1
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
	ok := resp.StatusCode == http.StatusOK
	report := map[string]any{"mode": *mode, "ok": ok, "url": target, "status": resp.StatusCode, "body": jsonBody(body)}
	emit(report)
	if !ok {
		return 1
	}
	return 0
}

// probeAddress turns a listen address into a dialable loopback address, so a
// container probing 0.0.0.0:8763 still connects to itself.
func probeAddress(addr string) string {
	if addr == "" {
		return defaultAddr
	}
	host, port, e := net.SplitHostPort(addr)
	if e != nil {
		return addr
	}
	switch host {
	case "", "0.0.0.0":
		host = "127.0.0.1"
	case "::", "[::]":
		host = "::1"
	}
	return net.JoinHostPort(host, port)
}

func jsonBody(b []byte) any {
	var v any
	if e := json.Unmarshal(b, &v); e == nil {
		return v
	}
	return string(b)
}

func emit(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}
