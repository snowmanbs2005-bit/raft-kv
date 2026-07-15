// Package httprpc implements internal/raft's Transport interface over
// plain JSON-over-HTTP.
//
// The original design called for gRPC, matching internal/raft/rpc.go
// field-for-field via a .proto definition. protoc (and protoc-gen-go /
// protoc-gen-go-grpc) are not available in this environment, so this
// package is a pragmatic, dependency-free substitute: it uses only the
// standard library (net/http, encoding/json) and exposes the exact same
// two RPCs -- RequestVote and AppendEntries -- with the exact same request
// and reply shapes as internal/raft/rpc.go, just serialized as JSON over
// HTTP POST instead of protobuf over HTTP/2. Swapping this package for a
// real gRPC transport later would not require any change to internal/raft,
// since it only depends on the raft.Transport interface.
package httprpc

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/snowmanbs2005-bit/raft-kv/internal/raft"
)

const (
	requestVotePath   = "/raft/rpc/request-vote"
	appendEntriesPath = "/raft/rpc/append-entries"
)

// Server exposes a raft.RPCHandler over HTTP so that peers' Client values
// can reach it.
type Server struct {
	handler raft.RPCHandler
}

// NewServer wraps handler (typically a *raft.Raft) for HTTP delivery.
func NewServer(handler raft.RPCHandler) *Server {
	return &Server{handler: handler}
}

// RegisterOn adds this server's routes to mux, so it can share an HTTP
// server/port with other handlers (e.g. the KV API) if desired.
func (s *Server) RegisterOn(mux *http.ServeMux) {
	mux.HandleFunc(requestVotePath, s.handleRequestVote)
	mux.HandleFunc(appendEntriesPath, s.handleAppendEntries)
}

func (s *Server) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var args raft.RequestVoteArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	reply, err := s.handler.HandleRequestVote(r.Context(), &args)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, reply)
}

func (s *Server) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var args raft.AppendEntriesArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	reply, err := s.handler.HandleAppendEntries(r.Context(), &args)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, reply)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("httprpc: encode response: %v", err)
	}
}
