package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// FileStorage persists Raft state as a single JSON file. Every SaveState
// writes to a temporary file in the same directory, fsyncs it, and then
// renames it over the real file; the rename is atomic on both POSIX
// filesystems and NTFS, so a crash mid-write can never leave a corrupt or
// half-written state file behind.
type FileStorage struct {
	mu   sync.Mutex
	path string
}

type onDiskState struct {
	Term     uint64     `json:"term"`
	VotedFor string     `json:"voted_for"`
	Log      []LogEntry `json:"log"`
}

// NewFileStorage returns a FileStorage that persists to <dir>/raft-state.json.
// The directory is created if it does not already exist.
func NewFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("storage: create data dir: %w", err)
	}
	return &FileStorage{path: filepath.Join(dir, "raft-state.json")}, nil
}

// SaveState implements Storage.
func (f *FileStorage) SaveState(term uint64, votedFor string, log []LogEntry) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	state := onDiskState{Term: term, VotedFor: votedFor, Log: log}
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("storage: marshal state: %w", err)
	}

	dir := filepath.Dir(f.path)
	tmp, err := os.CreateTemp(dir, "raft-state-*.tmp")
	if err != nil {
		return fmt.Errorf("storage: create temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op once the rename below succeeds

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("storage: write temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("storage: fsync temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("storage: close temp file: %w", err)
	}
	if err := os.Rename(tmpName, f.path); err != nil {
		return fmt.Errorf("storage: rename temp file: %w", err)
	}
	return nil
}

// LoadState implements Storage.
func (f *FileStorage) LoadState() (term uint64, votedFor string, log []LogEntry, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := os.ReadFile(f.path)
	if os.IsNotExist(err) {
		return 0, "", nil, nil
	}
	if err != nil {
		return 0, "", nil, fmt.Errorf("storage: read state file: %w", err)
	}

	var state onDiskState
	if err := json.Unmarshal(data, &state); err != nil {
		return 0, "", nil, fmt.Errorf("storage: unmarshal state: %w", err)
	}
	return state.Term, state.VotedFor, state.Log, nil
}
