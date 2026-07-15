// Package fsm defines the state machine interface that sits on top of
// Raft's replicated log. Raft itself only knows how to replicate opaque
// byte slices ([]byte commands) and tell the application which ones have
// committed, in order; it is the state machine's job to interpret and apply
// them.
package fsm

// StateMachine is applied, in commit order, to every committed log entry.
// Implementations must be deterministic: given the same sequence of Apply
// calls, every node in the cluster must end up in the same state.
type StateMachine interface {
	// Apply interprets and applies cmd, returning an application-defined
	// result (e.g. the previous value for a "get" command).
	Apply(cmd []byte) []byte
	// Snapshot returns a serialized copy of the current state, suitable
	// for passing to Restore later.
	Snapshot() ([]byte, error)
	// Restore replaces the current state with the one encoded in data (as
	// previously produced by Snapshot).
	Restore(data []byte) error
}
