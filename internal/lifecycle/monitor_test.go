package lifecycle

import (
	"atlas-render/internal/persist"
	"errors"
	"os"
	"testing"
	"time"
)

func TestReadyRequiresAttachedStore(t *testing.T) {
	m := NewMonitor("state.json")
	if e := m.Ready(); !errors.Is(e, persist.ErrUnusable) {
		t.Fatalf("expected unusable before attach, got %v", e)
	}
	if r := Reason(m.Ready()); r != ReasonUnusable {
		t.Fatalf("expected %s, got %s", ReasonUnusable, r)
	}
}

func TestReadyReportsLockConflict(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/state.json"
	first, e := persist.Open(path)
	if e != nil {
		t.Fatalf("open first: %v", e)
	}
	defer first.Close()
	second, e := persist.Open(path)
	if e == nil {
		second.Close()
		t.Fatal("second open should have been refused")
	}
	m := NewMonitor(path)
	m.SetStartupError(e)
	if r := Reason(m.Ready()); r != ReasonLocked {
		t.Fatalf("expected %s, got %s (%v)", ReasonLocked, r, e)
	}
}

func TestReadyDetectsReadOnlyDirectory(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permission checks")
	}
	dir := t.TempDir()
	st, e := persist.Open(dir + "/state.json")
	if e != nil {
		t.Fatalf("open: %v", e)
	}
	defer st.Close()
	m := NewMonitor(dir + "/state.json")
	m.Attach(st)
	if e := m.ReadyNow(); e != nil {
		t.Fatalf("expected ready, got %v", e)
	}
	if e := os.Chmod(dir, 0500); e != nil {
		t.Fatalf("chmod: %v", e)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0700) })
	if e := m.ReadyNow(); !errors.Is(e, persist.ErrUnusable) {
		t.Fatalf("expected unusable on read-only directory, got %v", e)
	}
	if r := Reason(m.ReadyNow()); r != ReasonUnusable {
		t.Fatalf("expected %s, got %s", ReasonUnusable, r)
	}
}

func TestReadyCacheExpires(t *testing.T) {
	dir := t.TempDir()
	st, e := persist.Open(dir + "/state.json")
	if e != nil {
		t.Fatalf("open: %v", e)
	}
	defer st.Close()
	now := time.Unix(1000, 0)
	m := NewMonitor(dir + "/state.json")
	m.now = func() time.Time { return now }
	m.Attach(st)
	if e := m.Ready(); e != nil {
		t.Fatalf("expected ready, got %v", e)
	}
	now = now.Add(2 * readyCacheTTL)
	if e := m.Ready(); e != nil {
		t.Fatalf("expected ready after cache advance, got %v", e)
	}
}
