// Package kvstore exposes the replicated key-value store as an HTTP API.
// Writes (PUT/DELETE) are proposed to the local Raft node and the response
// is only sent once the proposed entry has actually been committed and
// applied to the state machine; reads (GET) are served the same way (via a
// no-op "get" proposal path is avoided -- for simplicity and to keep the
// hot read path cheap, GET is answered directly from the local FSM, but
// only on the node that is currently the leader; followers redirect GETs
// to the leader too, so every client request in this project is served by
// a single, consistent view. This is not a full read-index/lease-based
// linearizable-read implementation, and that trade-off is called out in
// the README.
package kvstore

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/snowmanbs2005-bit/raft-kv/internal/fsm"
	"github.com/snowmanbs2005-bit/raft-kv/internal/raft"
)

// RaftNode is the subset of *raft.Raft the HTTP server needs.
type RaftNode interface {
	Propose(ctx context.Context, command []byte) (index uint64, term uint64, isLeader bool)
	IsLeader() bool
	LeaderHint() string
	Commits() <-chan raft.CommitEntry
}

// Server is the HTTP KV API for one node.
type Server struct {
	node RaftNode
	fsm  *fsm.KVFSM

	// leaderAddrOf resolves a raft node ID (as returned by LeaderHint) to
	// that node's HTTP API address, for redirects.
	leaderAddrOf func(nodeID string) (string, bool)

	requestTimeout time.Duration

	waitMu  sync.Mutex
	waiters map[uint64]chan waitResult
}

type waitResult struct {
	term   uint64
	output []byte
}

// NewServer builds a Server. leaderAddrOf must map a node ID to its HTTP
// listen address (used only to build redirect URLs).
func NewServer(node RaftNode, machine *fsm.KVFSM, leaderAddrOf func(nodeID string) (string, bool)) *Server {
	s := &Server{
		node:           node,
		fsm:            machine,
		leaderAddrOf:   leaderAddrOf,
		requestTimeout: 3 * time.Second,
		waiters:        make(map[uint64]chan waitResult),
	}
	go s.applyLoop()
	return s
}

// applyLoop drains committed entries, applies them to the FSM in order, and
// wakes up any HTTP handler waiting on that index.
func (s *Server) applyLoop() {
	for entry := range s.node.Commits() {
		output := s.fsm.Apply(entry.Command)

		s.waitMu.Lock()
		ch, ok := s.waiters[entry.Index]
		if ok {
			delete(s.waiters, entry.Index)
		}
		s.waitMu.Unlock()

		if ok {
			ch <- waitResult{term: entry.Term, output: output}
		}
	}
}

func (s *Server) registerWaiter(index uint64) chan waitResult {
	ch := make(chan waitResult, 1)
	s.waitMu.Lock()
	s.waiters[index] = ch
	s.waitMu.Unlock()
	return ch
}

func (s *Server) forgetWaiter(index uint64) {
	s.waitMu.Lock()
	delete(s.waiters, index)
	s.waitMu.Unlock()
}

// Handler returns the http.Handler serving the KV API (PUT/GET/DELETE on
// /kv/{key}).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", s.handleKV)
	mux.HandleFunc("/status", s.handleStatus)
	return mux
}

// StatusResponse describes this node's view of cluster leadership, used by
// raftctl's "leader" subcommand and by operators for debugging.
type StatusResponse struct {
	IsLeader   bool   `json:"is_leader"`
	LeaderHint string `json:"leader_hint"`
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	resp := StatusResponse{IsLeader: s.node.IsLeader(), LeaderHint: s.node.LeaderHint()}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	if !s.node.IsLeader() {
		s.redirectToLeader(w, r)
		return
	}

	switch r.Method {
	case http.MethodPut:
		s.handleSet(w, r, key)
	case http.MethodGet:
		s.handleGet(w, r, key)
	case http.MethodDelete:
		s.handleDelete(w, r, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleSet(w http.ResponseWriter, r *http.Request, key string) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	cmd := fsm.Command{Op: fsm.OpSet, Key: key, Value: string(body)}
	if err := s.proposeAndWait(r.Context(), cmd, w); err != nil {
		writeProposeError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, key string) {
	cmd := fsm.Command{Op: fsm.OpDelete, Key: key}
	if err := s.proposeAndWait(r.Context(), cmd, w); err != nil {
		writeProposeError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGet(w http.ResponseWriter, _ *http.Request, key string) {
	value, ok := s.fsm.Get(key)
	if !ok {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte(value))
}

var errNotLeader = errors.New("kvstore: lost leadership before entry committed")

// proposeAndWait proposes cmd to Raft and blocks until the resulting log
// entry is actually committed and applied (or the request context expires,
// or the proposal is superseded because this node stopped being leader for
// that term).
func (s *Server) proposeAndWait(ctx context.Context, cmd fsm.Command, w http.ResponseWriter) error {
	encoded, err := cmd.Encode()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()

	index, term, isLeader := s.node.Propose(ctx, encoded)
	if !isLeader {
		return errNotLeader
	}

	ch := s.registerWaiter(index)
	select {
	case res := <-ch:
		if res.term != term {
			return errNotLeader
		}
		var result fsm.Result
		if err := json.Unmarshal(res.output, &result); err == nil && !result.OK {
			return errors.New(result.Error)
		}
		return nil
	case <-ctx.Done():
		s.forgetWaiter(index)
		return ctx.Err()
	}
}

func writeProposeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errNotLeader):
		http.Error(w, "lost leadership, retry", http.StatusServiceUnavailable)
	case errors.Is(err, context.DeadlineExceeded):
		http.Error(w, "timed out waiting for commit", http.StatusGatewayTimeout)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
