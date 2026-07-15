package fsm

import (
	"encoding/json"
	"fmt"
	"sync"
)

// Op identifies the kind of operation a Command encodes.
type Op string

const (
	OpSet    Op = "set"
	OpDelete Op = "delete"
)

// Command is the wire format for entries this FSM knows how to Apply. It is
// what callers marshal into the []byte passed to Raft.Propose.
type Command struct {
	Op    Op     `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// Encode marshals a Command to the byte slice format Apply expects.
func (c Command) Encode() ([]byte, error) {
	return json.Marshal(c)
}

// Result is what Apply returns, marshaled to JSON so callers on the other
// side of Raft.Commits can decode it uniformly.
type Result struct {
	OK      bool   `json:"ok"`
	Value   string `json:"value,omitempty"`
	Existed bool   `json:"existed"`
	Error   string `json:"error,omitempty"`
}

// KVFSM is an in-memory key-value store implementing StateMachine. It has
// its own mutex because it is read directly by GET requests (which do not
// need to go through Raft consensus in this project's design -- only
// mutating operations are replicated), independent of Raft's internal
// single-goroutine state.
type KVFSM struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewKVFSM returns an empty KVFSM.
func NewKVFSM() *KVFSM {
	return &KVFSM{data: make(map[string]string)}
}

// Get reads a key directly, without going through Raft. Safe for concurrent
// use.
func (f *KVFSM) Get(key string) (value string, ok bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	value, ok = f.data[key]
	return value, ok
}

// Apply implements StateMachine. cmd must be a JSON-encoded Command; the
// return value is a JSON-encoded Result.
func (f *KVFSM) Apply(cmd []byte) []byte {
	var c Command
	if err := json.Unmarshal(cmd, &c); err != nil {
		return mustEncodeResult(Result{OK: false, Error: fmt.Sprintf("invalid command: %v", err)})
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch c.Op {
	case OpSet:
		f.data[c.Key] = c.Value
		return mustEncodeResult(Result{OK: true})
	case OpDelete:
		_, existed := f.data[c.Key]
		delete(f.data, c.Key)
		return mustEncodeResult(Result{OK: true, Existed: existed})
	default:
		return mustEncodeResult(Result{OK: false, Error: fmt.Sprintf("unknown op %q", c.Op)})
	}
}

// Snapshot implements StateMachine.
func (f *KVFSM) Snapshot() ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return json.Marshal(f.data)
}

// Restore implements StateMachine.
func (f *KVFSM) Restore(data []byte) error {
	m := make(map[string]string)
	if len(data) > 0 {
		if err := json.Unmarshal(data, &m); err != nil {
			return fmt.Errorf("kvfsm: restore: %w", err)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data = m
	return nil
}

func mustEncodeResult(r Result) []byte {
	b, err := json.Marshal(r)
	if err != nil {
		// Result only contains strings/bools; this cannot realistically
		// fail, but fall back to a minimal valid JSON object rather than
		// panicking the FSM apply loop.
		return []byte(`{"ok":false,"error":"internal encode error"}`)
	}
	return b
}
