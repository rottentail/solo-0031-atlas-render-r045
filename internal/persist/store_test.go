package persist

import (
	"atlas-render/internal/model"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestSecondOpenIsRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	first, e := Open(path)
	if e != nil {
		t.Fatalf("first open: %v", e)
	}
	defer first.Close()
	second, e := Open(path)
	if e == nil {
		second.Close()
		t.Fatal("second open should be refused")
	}
	if !errors.Is(e, ErrLocked) {
		t.Fatalf("expected ErrLocked, got %v", e)
	}
}

func TestLockIsReleasedOnClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	first, e := Open(path)
	if e != nil {
		t.Fatalf("first open: %v", e)
	}
	if e = first.Close(); e != nil {
		t.Fatalf("close: %v", e)
	}
	second, e := Open(path)
	if e != nil {
		t.Fatalf("reopen after close: %v", e)
	}
	second.Close()
}

func TestOpenRejectsInvalidState(t *testing.T) {
	d := t.TempDir()
	path := filepath.Join(d, "state.json")
	if e := os.WriteFile(path, []byte("{not json"), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e := Open(path); !errors.Is(e, ErrStateInvalid) {
		t.Fatalf("expected ErrStateInvalid, got %v", e)
	}
	// A refused open must not leave the lock held.
	if st, e := Open(path); e == nil {
		st.Close()
		t.Fatal("invalid state should still be refused")
	}
}

func TestUpdateSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	st, e := Open(path)
	if e != nil {
		t.Fatalf("open: %v", e)
	}
	if e = st.Update(func(s *model.State) error {
		s.Sequence = 41
		return nil
	}); e != nil {
		t.Fatalf("update: %v", e)
	}
	st.Close()
	st, e = Open(path)
	if e != nil {
		t.Fatalf("reopen: %v", e)
	}
	defer st.Close()
	if got := st.Read().Sequence; got != 41 {
		t.Fatalf("sequence not durable: %d", got)
	}
}

func TestFailedUpdateKeepsCommittedState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	st, e := Open(path)
	if e != nil {
		t.Fatalf("open: %v", e)
	}
	defer st.Close()
	if e = st.Update(func(s *model.State) error { s.Sequence = 7; return nil }); e != nil {
		t.Fatalf("update: %v", e)
	}
	st.write = func(string, []byte) error { return errors.New("disk full") }
	e = st.Update(func(s *model.State) error { s.Sequence = 9; return nil })
	if !errors.Is(e, ErrCommit) {
		t.Fatalf("expected ErrCommit, got %v", e)
	}
	if got := st.Read().Sequence; got != 7 {
		t.Fatalf("failed commit advanced memory: %d", got)
	}
}

func TestLeftoverTemporaryFilesAreRemoved(t *testing.T) {
	d := t.TempDir()
	path := filepath.Join(d, "state.json")
	leftover := filepath.Join(d, tempPrefix(path)+"stale")
	if e := os.WriteFile(leftover, []byte("partial"), 0600); e != nil {
		t.Fatal(e)
	}
	st, e := Open(path)
	if e != nil {
		t.Fatalf("open: %v", e)
	}
	defer st.Close()
	if _, e := os.Stat(leftover); !os.IsNotExist(e) {
		t.Fatalf("stale temporary file was not removed: %v", e)
	}
}

func TestCleanupDoesNotTouchOtherStateFiles(t *testing.T) {
	d := t.TempDir()
	// A sibling state file's in-flight temporary must survive our startup.
	other := filepath.Join(d, tempPrefix(filepath.Join(d, "other.json"))+"inflight")
	if e := os.WriteFile(other, []byte("other"), 0600); e != nil {
		t.Fatal(e)
	}
	st, e := Open(filepath.Join(d, "state.json"))
	if e != nil {
		t.Fatalf("open: %v", e)
	}
	defer st.Close()
	if _, e := os.Stat(other); e != nil {
		t.Fatalf("unrelated temporary file was removed: %v", e)
	}
}

func TestEmptyStatePathIsUnusable(t *testing.T) {
	if _, e := Open("  "); !errors.Is(e, ErrUnusable) {
		t.Fatalf("expected ErrUnusable, got %v", e)
	}
}

func TestCheckWritableDetectsReadOnlyDirectory(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permissions")
	}
	d := t.TempDir()
	st, e := Open(filepath.Join(d, "state.json"))
	if e != nil {
		t.Fatalf("open: %v", e)
	}
	defer st.Close()
	if e = st.CheckWritable(); e != nil {
		t.Fatalf("expected writable, got %v", e)
	}
	if e = os.Chmod(d, 0500); e != nil {
		t.Fatal(e)
	}
	defer os.Chmod(d, 0700)
	if e = st.CheckWritable(); !errors.Is(e, ErrUnusable) {
		t.Fatalf("expected ErrUnusable, got %v", e)
	}
}
