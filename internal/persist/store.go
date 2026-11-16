package persist

import (
	"atlas-render/internal/model"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ErrLocked reports that another process already holds the exclusive state
// lock. A second instance must refuse to start instead of committing
// independent facts over the same file.
var ErrLocked = errors.New("state is already opened by another process")

// ErrStateInvalid reports that the persisted bytes cannot be used: unsupported
// version, corrupt JSON, failed validation, or an orphaned record.
var ErrStateInvalid = errors.New("persisted state cannot be used")

// ErrUnusable reports that the state path cannot be used at all right now:
// missing permissions, a read-only volume, a directory in place of a file, or
// a closed store.
var ErrUnusable = errors.New("state store is unavailable")

// ErrCommit reports that a transaction could not be committed. The in-memory
// facts are only advanced after the atomic replacement really happened.
var ErrCommit = errors.New("state transaction could not be committed")

type Store struct {
	mu     sync.RWMutex
	path   string
	state  model.State
	lock   *os.File
	closed bool
	write  func(string, []byte) error
}

// Open acquires the exclusive lock first and only then reads the state file.
// Two containers that mistakenly mount the same data directory therefore fail
// safely instead of interleaving writes. The lock file itself may be left on
// disk; its validity comes from the operating system lock, not its presence.
func Open(path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("%w: state path is empty", ErrUnusable)
	}
	dir := filepath.Dir(path)
	if e := os.MkdirAll(dir, 0700); e != nil {
		return nil, unusable("create state directory", dir, e)
	}
	if fi, e := os.Stat(dir); e != nil {
		return nil, unusable("inspect state directory", dir, e)
	} else if !fi.IsDir() {
		return nil, fmt.Errorf("%w: state directory %s is not a directory", ErrUnusable, dir)
	}
	lockPath := path + ".lock"
	lock, e := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, unusable("open state lock", lockPath, e)
	}
	release := func() {
		_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
		_ = lock.Close()
	}
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		holder := lockHolder(lockPath)
		_ = lock.Close()
		if errors.Is(e, syscall.EWOULDBLOCK) || errors.Is(e, syscall.EAGAIN) {
			return nil, fmt.Errorf("%w: lock file %s%s", ErrLocked, lockPath, holder)
		}
		return nil, unusable("lock state", lockPath, e)
	}
	// The lock is held from here on; every failure must release it.
	if e = writeLockHolder(lock); e != nil {
		release()
		return nil, unusable("record state lock", lockPath, e)
	}
	if e = removeLeftovers(path); e != nil {
		release()
		return nil, unusable("clean stale temporary files", dir, e)
	}
	if e = probeWritable(path); e != nil {
		release()
		return nil, e
	}
	data, e := os.ReadFile(path)
	if e != nil && !errors.Is(e, fs.ErrNotExist) {
		release()
		return nil, unusable("read state file", path, e)
	}
	st := model.NewState()
	if e == nil {
		if e = json.Unmarshal(data, &st); e != nil {
			release()
			return nil, fmt.Errorf("%w: %s: %v", ErrStateInvalid, path, e)
		}
		if e = Validate(st); e != nil {
			release()
			return nil, fmt.Errorf("%w: %s: %v", ErrStateInvalid, path, e)
		}
	}
	return &Store{path: path, state: st, lock: lock, write: atomicWrite}, nil
}

func (s *Store) Path() string { return s.path }

func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	lock := s.lock
	s.lock = nil
	s.mu.Unlock()
	if lock == nil {
		return nil
	}
	_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	return lock.Close()
}

func (s *Store) Read() model.State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return model.Clone(s.state)
}

func (s *Store) Update(fn func(*model.State) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("%w: state store is closed", ErrUnusable)
	}
	next := model.Clone(s.state)
	if e := fn(&next); e != nil {
		return e
	}
	if e := Validate(next); e != nil {
		return e
	}
	b, e := json.Marshal(next)
	if e != nil {
		return e
	}
	if e = s.write(s.path, b); e != nil {
		return fmt.Errorf("%w: %w", ErrCommit, e)
	}
	s.state = next
	return nil
}

// CheckWritable proves that the state directory can still accept a commit
// without touching committed facts. Readiness uses it so a volume that became
// read-only is reported separately from process liveness.
func (s *Store) CheckWritable() error {
	s.mu.RLock()
	closed, path := s.closed, s.path
	s.mu.RUnlock()
	if closed {
		return fmt.Errorf("%w: state store is closed", ErrUnusable)
	}
	return probeWritable(path)
}

// atomicWrite writes through a sibling temporary file, fsyncs it, then renames
// over the target. The temporary name is derived from the target so that two
// independent state files sharing a directory never remove each other's
// in-flight writes during crash cleanup.
func atomicWrite(path string, data []byte) error {
	dir := filepath.Dir(path)
	f, e := os.CreateTemp(dir, tempPrefix(path))
	if e != nil {
		return e
	}
	name := f.Name()
	defer os.Remove(name)
	if e = f.Chmod(0600); e == nil {
		if _, e = f.Write(data); e == nil {
			e = f.Sync()
		}
	}
	closeErr := f.Close()
	if e == nil {
		e = closeErr
	}
	if e != nil {
		return e
	}
	if e = os.Rename(name, path); e != nil {
		return e
	}
	// Visibility is guaranteed by rename. Syncing the directory additionally
	// hardens the commit against host crashes; some filesystems cannot sync a
	// directory, and the project does not promise power-loss durability.
	syncDir(dir)
	return nil
}

// tempPrefix identifies temporary files owned by one state path, e.g.
// ".atlas-state.json-write-".
func tempPrefix(path string) string {
	return ".atlas-" + filepath.Base(path) + "-write-"
}

func probePrefix(path string) string {
	return ".atlas-" + filepath.Base(path) + "-probe-"
}

func syncDir(dir string) {
	d, e := os.Open(dir)
	if e != nil {
		return
	}
	defer d.Close()
	// Some filesystems reject directory syncs; the rename already made the
	// new state visible, so a failure here is not treated as a failed commit.
	_ = d.Sync()
}

func probeWritable(path string) error {
	dir := filepath.Dir(path)
	f, e := os.CreateTemp(dir, probePrefix(path))
	if e != nil {
		return fmt.Errorf("%w: state directory %s is not writable: %w", ErrUnusable, dir, e)
	}
	name := f.Name()
	if _, e = f.Write([]byte("probe")); e == nil {
		e = f.Sync()
	}
	closeErr := f.Close()
	_ = os.Remove(name)
	if e == nil {
		e = closeErr
	}
	if e != nil {
		return fmt.Errorf("%w: state directory %s is not writable: %w", ErrUnusable, dir, e)
	}
	return nil
}

// removeLeftovers deletes temporary files left by an interrupted writer of this
// state path. It only touches names scoped to the same target, so unrelated
// state files in the same directory are never disturbed.
func removeLeftovers(path string) error {
	dir := filepath.Dir(path)
	for _, prefix := range []string{tempPrefix(path), probePrefix(path)} {
		names, e := filepath.Glob(filepath.Join(dir, prefix+"*"))
		if e != nil {
			return e
		}
		for _, name := range names {
			if e = os.Remove(name); e != nil && !errors.Is(e, fs.ErrNotExist) {
				return e
			}
		}
	}
	return nil
}

func writeLockHolder(f *os.File) error {
	info := fmt.Sprintf("pid=%d started=%s\n", os.Getpid(), time.Now().UTC().Format(time.RFC3339))
	if _, e := f.Seek(0, 0); e != nil {
		return e
	}
	if e := f.Truncate(0); e != nil {
		return e
	}
	if _, e := f.WriteString(info); e != nil {
		return e
	}
	return f.Sync()
}

func lockHolder(path string) string {
	f, e := os.Open(path)
	if e != nil {
		return ""
	}
	defer f.Close()
	b := make([]byte, 256)
	n, _ := f.Read(b)
	holder := strings.TrimSpace(string(b[:n]))
	if holder == "" {
		return " (lock file is held by another process)"
	}
	return fmt.Sprintf(" (recorded holder: %s; PIDs are container-local)", holder)
}

func unusable(action, path string, e error) error {
	return fmt.Errorf("%w: cannot %s %s: %w", ErrUnusable, action, path, e)
}
