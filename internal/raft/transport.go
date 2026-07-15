package raft

import "context"

// Transport is how a Raft node talks to its peers. The raft package never
// touches the network directly: every concrete transport (in-memory for
// tests, HTTP/JSON or gRPC for real deployments) implements this interface
// and is injected into the node at construction time.
type Transport interface {
	RequestVote(ctx context.Context, peerID string, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, peerID string, args *AppendEntriesArgs) (*AppendEntriesReply, error)
}

// RPCHandler is implemented by *Raft and is what a Transport's server side
// calls into when an RPC arrives from a peer. Handlers hand the request off
// to the node's single event-loop goroutine via a channel rather than
// mutating Raft state directly, so incoming RPCs never race with the loop.
type RPCHandler interface {
	HandleRequestVote(ctx context.Context, args *RequestVoteArgs) (*RequestVoteReply, error)
	HandleAppendEntries(ctx context.Context, args *AppendEntriesArgs) (*AppendEntriesReply, error)
}
