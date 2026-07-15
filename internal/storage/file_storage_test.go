package storage

import (
	"testing"
)

func TestFileStorage_SaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewFileStorage(dir)
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}

	log := []LogEntry{
		{Term: 0, Index: 0},
		{Term: 1, Index: 1, Command: []byte("set x=1")},
		{Term: 1, Index: 2, Command: []byte("set y=2")},
	}
	if err := fs.SaveState(1, "node-a", log); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	term, votedFor, gotLog, err := fs.LoadState()
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if term != 1 {
		t.Errorf("term = %d, want 1", term)
	}
	if votedFor != "node-a" {
		t.Errorf("votedFor = %q, want node-a", votedFor)
	}
	if len(gotLog) != len(log) {
		t.Fatalf("log length = %d, want %d", len(gotLog), len(log))
	}
	for i := range log {
		if gotLog[i].Term != log[i].Term || gotLog[i].Index != log[i].Index || string(gotLog[i].Command) != string(log[i].Command) {
			t.Errorf("log[%d] = %+v, want %+v", i, gotLog[i], log[i])
		}
	}
}

func TestFileStorage_LoadState_FreshNode(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewFileStorage(dir)
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}

	term, votedFor, log, err := fs.LoadState()
	if err != nil {
		t.Fatalf("LoadState on fresh node returned error: %v", err)
	}
	if term != 0 || votedFor != "" || log != nil {
		t.Errorf("fresh node state = (%d, %q, %v), want zero values", term, votedFor, log)
	}
}

func TestFileStorage_OverwritePersists(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewFileStorage(dir)
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}

	if err := fs.SaveState(1, "a", nil); err != nil {
		t.Fatalf("SaveState 1: %v", err)
	}
	if err := fs.SaveState(5, "b", []LogEntry{{Term: 5, Index: 1, Command: []byte("x")}}); err != nil {
		t.Fatalf("SaveState 2: %v", err)
	}

	term, votedFor, log, err := fs.LoadState()
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if term != 5 || votedFor != "b" || len(log) != 1 {
		t.Errorf("state after overwrite = (%d, %q, %v), want (5, b, 1 entry)", term, votedFor, log)
	}
}

// TestFileStorage_SurvivesReopen simulates a process restart: a new
// FileStorage pointed at the same directory must see the previously
// persisted state.
func TestFileStorage_SurvivesReopen(t *testing.T) {
	dir := t.TempDir()

	fs1, err := NewFileStorage(dir)
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	if err := fs1.SaveState(42, "node-z", []LogEntry{{Term: 42, Index: 1, Command: []byte("payload")}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	fs2, err := NewFileStorage(dir)
	if err != nil {
		t.Fatalf("NewFileStorage (reopen): %v", err)
	}
	term, votedFor, log, err := fs2.LoadState()
	if err != nil {
		t.Fatalf("LoadState (reopen): %v", err)
	}
	if term != 42 || votedFor != "node-z" || len(log) != 1 {
		t.Errorf("reopened state = (%d, %q, %v), want (42, node-z, 1 entry)", term, votedFor, log)
	}
}
