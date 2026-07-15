// Package storage defines and implements persistence for Raft's durable
// state: the current term, the candidate voted for in that term, and the
// replicated log. Raft only makes progress-affecting promises (granting a
// vote, acknowledging an append) after this state has survived to disk.
//
// LogEntry is defined here (rather than imported from internal/raft) so
// that this package has no dependency on the raft package -- it is the raft
// package that depends on storage, not the other way around. internal/raft
// converts to/from raft.LogEntry at the call site.
package storage

// LogEntry mirrors raft.LogEntry field-for-field; it exists so this package
// does not need to import internal/raft.
type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
}

// Storage is implemented by anything that can durably persist and reload a
// Raft node's term, vote and log. The raft package depends only on this
// interface, never on a concrete implementation.
type Storage interface {
	// SaveState persists term, votedFor and the full log, fsync'd before
	// returning.
	SaveState(term uint64, votedFor string, log []LogEntry) error
	// LoadState reads back the last persisted state. On a fresh node (no
	// state ever saved) it returns zero values and a nil error.
	LoadState() (term uint64, votedFor string, log []LogEntry, err error)
}
