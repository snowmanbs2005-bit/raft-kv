package raft

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/snowmanbs2005-bit/raft-kv/internal/storage"
)

// Config holds everything needed to construct a Raft node.
type Config struct {
	// ID uniquely identifies this node among its peers.
	ID string
	// Peers lists the IDs of every other node in the cluster (not
	// including ID).
	Peers []string

	// ElectionTimeoutMin/Max bound the randomized election timeout. Each
	// election round a fresh value is drawn from this range.
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	// HeartbeatInterval is how often a leader sends AppendEntries (empty
	// or not) to keep followers from timing out.
	HeartbeatInterval time.Duration

	Storage   storage.Storage
	Transport Transport
}

func (c Config) validate() error {
	if c.ID == "" {
		return fmt.Errorf("raft: Config.ID must not be empty")
	}
	if c.Storage == nil {
		return fmt.Errorf("raft: Config.Storage must not be nil")
	}
	if c.Transport == nil {
		return fmt.Errorf("raft: Config.Transport must not be nil")
	}
	if c.ElectionTimeoutMin <= 0 || c.ElectionTimeoutMax <= c.ElectionTimeoutMin {
		return fmt.Errorf("raft: ElectionTimeoutMax must be greater than ElectionTimeoutMin > 0")
	}
	if c.HeartbeatInterval <= 0 {
		return fmt.Errorf("raft: HeartbeatInterval must be > 0")
	}
	return nil
}

type rpcRequest struct {
	requestVote   *RequestVoteArgs
	appendEntries *AppendEntriesArgs
	respCh        chan rpcResponse
}

type rpcResponse struct {
	voteReply   *RequestVoteReply
	appendReply *AppendEntriesReply
}

type proposeRequest struct {
	command []byte
	respCh  chan proposeResponse
}

type proposeResponse struct {
	index    uint64
	term     uint64
	isLeader bool
}

// Raft is a single node participating in a Raft cluster. All mutable
// consensus state (currentTerm, votedFor, log, commitIndex, ...) is owned
// exclusively by the goroutine running (*Raft).run -- nothing outside that
// goroutine ever writes it, and nothing outside it reads it directly. RPCs
// arrive as messages on rpcCh, client proposals arrive on proposeCh, and the
// small amount of state that must be readable from other goroutines
// (current role, current term, last known leader) is published through
// atomics after every change made inside the loop.
type Raft struct {
	id    string
	peers []string

	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration
	heartbeatInterval  time.Duration

	storage   storage.Storage
	transport Transport

	// --- state owned by the run() goroutine only ---
	currentTerm uint64
	votedFor    string
	log         *raftLog
	commitIndex uint64
	lastApplied uint64
	nextIndex   map[string]uint64
	matchIndex  map[string]uint64
	state       State

	// --- channels: the only way into the loop from other goroutines ---
	rpcCh      chan rpcRequest
	proposeCh  chan proposeRequest
	shutdownCh chan struct{}
	doneCh     chan struct{}

	commitCh chan CommitEntry

	// --- published for lock-free reads from outside the loop ---
	atomicState  atomic.Int64 // State
	atomicTerm   atomic.Uint64
	atomicLeader atomic.Value // string
}

// New constructs a Raft node from cfg. The node does not start running
// until Start is called.
func New(cfg Config) (*Raft, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	term, votedFor, entries, err := cfg.Storage.LoadState()
	if err != nil {
		return nil, fmt.Errorf("raft: load persisted state: %w", err)
	}

	log := newRaftLog()
	if len(entries) > 0 {
		converted := make([]LogEntry, len(entries))
		for i, e := range entries {
			converted[i] = LogEntry{Term: e.Term, Index: e.Index, Command: e.Command}
		}
		log = newRaftLogFrom(converted)
	}

	r := &Raft{
		id:                 cfg.ID,
		peers:              append([]string(nil), cfg.Peers...),
		electionTimeoutMin: cfg.ElectionTimeoutMin,
		electionTimeoutMax: cfg.ElectionTimeoutMax,
		heartbeatInterval:  cfg.HeartbeatInterval,
		storage:            cfg.Storage,
		transport:          cfg.Transport,
		currentTerm:        term,
		votedFor:           votedFor,
		log:                log,
		nextIndex:          make(map[string]uint64),
		matchIndex:         make(map[string]uint64),
		state:              Follower,
		rpcCh:              make(chan rpcRequest),
		proposeCh:          make(chan proposeRequest),
		shutdownCh:         make(chan struct{}),
		doneCh:             make(chan struct{}),
		commitCh:           make(chan CommitEntry, 4096),
	}
	r.atomicLeader.Store("")
	r.publishState()
	r.atomicTerm.Store(term)
	return r, nil
}

// Start launches the node's single event-loop goroutine.
func (r *Raft) Start() {
	go r.run()
}

// Stop terminates the event loop and waits for it to exit.
func (r *Raft) Stop() {
	select {
	case <-r.doneCh:
		return // already stopped
	default:
	}
	close(r.shutdownCh)
	<-r.doneCh
}

// ID returns the node's own ID.
func (r *Raft) ID() string { return r.id }

// State returns the node's current role. Safe to call from any goroutine.
func (r *Raft) State() State { return State(r.atomicState.Load()) }

// Term returns the node's current term. Safe to call from any goroutine.
func (r *Raft) Term() uint64 { return r.atomicTerm.Load() }

// IsLeader reports whether the node currently believes it is the leader.
func (r *Raft) IsLeader() bool { return r.State() == Leader }

// LeaderHint returns the ID of the node this node last believed to be the
// leader (possibly stale, and possibly empty if unknown). Useful for HTTP
// redirect-to-leader logic.
func (r *Raft) LeaderHint() string {
	v := r.atomicLeader.Load()
	if v == nil {
		return ""
	}
	return v.(string)
}

// Commits returns the channel on which committed log entries are delivered,
// in strictly increasing index order. Consumers (typically an FSM applier)
// must drain it promptly.
func (r *Raft) Commits() <-chan CommitEntry { return r.commitCh }

// Propose appends command to the log if this node is currently the leader.
// It returns the index and term the entry was assigned, and false if this
// node is not the leader (the caller should retry against the leader).
// Propose does not wait for the entry to commit; callers watch Commits (or
// poll) for that.
func (r *Raft) Propose(ctx context.Context, command []byte) (index uint64, term uint64, isLeader bool) {
	req := proposeRequest{command: command, respCh: make(chan proposeResponse, 1)}
	select {
	case r.proposeCh <- req:
	case <-ctx.Done():
		return 0, 0, false
	case <-r.doneCh:
		return 0, 0, false
	}
	select {
	case resp := <-req.respCh:
		return resp.index, resp.term, resp.isLeader
	case <-ctx.Done():
		return 0, 0, false
	}
}

// HandleRequestVote implements RPCHandler. It is called by a Transport's
// server side; it hands the RPC to the event loop and blocks for the reply.
func (r *Raft) HandleRequestVote(ctx context.Context, args *RequestVoteArgs) (*RequestVoteReply, error) {
	req := rpcRequest{requestVote: args, respCh: make(chan rpcResponse, 1)}
	select {
	case r.rpcCh <- req:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.doneCh:
		return nil, fmt.Errorf("raft: node %s stopped", r.id)
	}
	select {
	case resp := <-req.respCh:
		return resp.voteReply, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// HandleAppendEntries implements RPCHandler.
func (r *Raft) HandleAppendEntries(ctx context.Context, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	req := rpcRequest{appendEntries: args, respCh: make(chan rpcResponse, 1)}
	select {
	case r.rpcCh <- req:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.doneCh:
		return nil, fmt.Errorf("raft: node %s stopped", r.id)
	}
	select {
	case resp := <-req.respCh:
		return resp.appendReply, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (r *Raft) publishState() {
	r.atomicState.Store(int64(r.state))
	r.atomicTerm.Store(r.currentTerm)
}

func (r *Raft) setLeaderHint(id string) {
	r.atomicLeader.Store(id)
}

// persist saves currentTerm, votedFor and the log to durable storage. Per
// the Raft paper this MUST happen before replying to any RPC whose
// correctness depends on it (granting a vote, acknowledging an append), and
// before a leader can safely count its own vote/entry.
func (r *Raft) persist() error {
	entries := r.log.all()
	converted := make([]storage.LogEntry, len(entries))
	for i, e := range entries {
		converted[i] = storage.LogEntry{Term: e.Term, Index: e.Index, Command: e.Command}
	}
	return r.storage.SaveState(r.currentTerm, r.votedFor, converted)
}

func (r *Raft) randomElectionTimeout() time.Duration {
	return randomTimeout(r.electionTimeoutMin, r.electionTimeoutMax)
}

// run is the single event loop that owns all mutable Raft state.
func (r *Raft) run() {
	defer close(r.doneCh)
	for {
		switch r.state {
		case Follower:
			if !r.runFollower() {
				return
			}
		case Candidate:
			if !r.runCandidate() {
				return
			}
		case Leader:
			if !r.runLeader() {
				return
			}
		default:
			return
		}
	}
}
