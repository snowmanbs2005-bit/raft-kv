// Package raft implements the Raft consensus algorithm from scratch,
// following the extended paper "In Search of an Understandable Consensus
// Algorithm" (Ongaro & Ousterhout).
//
// The package is deliberately transport-agnostic: it never imports net,
// net/http or grpc. All communication with other nodes happens through the
// Transport interface (see transport.go), which lets the whole algorithm be
// unit-tested in-process, with microsecond-level RPC latency, using
// MemoryTransport.
package raft

// State is the role a Raft node currently plays.
type State int

const (
	// Follower is the default state: replicates the leader's log and votes
	// in elections.
	Follower State = iota
	// Candidate is the state a node enters while trying to get elected.
	Candidate
	// Leader is the state a node enters once it wins an election; it
	// accepts client proposals and replicates them to followers.
	Leader
	// Dead marks a node that has been stopped; the run loop exits.
	Dead
)

// String implements fmt.Stringer for readable logs and test failures.
func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		return "Unknown"
	}
}

// LogEntry is a single entry in the replicated log.
type LogEntry struct {
	Term    uint64 `json:"term"`
	Index   uint64 `json:"index"`
	Command []byte `json:"command"`
}

// CommitEntry is delivered on the Raft node's commit channel once an entry
// has been replicated to a majority of the cluster and is safe to apply to
// the state machine.
type CommitEntry struct {
	Index   uint64
	Term    uint64
	Command []byte
}
