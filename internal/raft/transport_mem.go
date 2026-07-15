package raft

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"
)

// MemoryTransport is an in-process Transport implementation that delivers
// RPCs directly to other *Raft instances registered in a shared registry.
// It exists so the election/replication/partition logic in this package can
// be unit-tested at full speed, with no sockets involved, while also
// supporting fault injection: network partitions, added delay, and random
// packet loss.
type MemoryTransport struct {
	mu        sync.RWMutex
	handlers  map[string]RPCHandler
	partition map[[2]string]bool // unordered pair -> unreachable
	delay     time.Duration
	dropRate  float64
}

// NewMemoryTransport returns an empty MemoryTransport. Call Register for
// every node that should be reachable through it.
func NewMemoryTransport() *MemoryTransport {
	return &MemoryTransport{
		handlers:  make(map[string]RPCHandler),
		partition: make(map[[2]string]bool),
	}
}

// Register makes id reachable via this transport, delivering RPCs to
// handler. Every *Raft node sharing a MemoryTransport should register
// itself (Raft satisfies RPCHandler).
func (m *MemoryTransport) Register(id string, handler RPCHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handlers[id] = handler
}

func pairKey(a, b string) [2]string {
	if a < b {
		return [2]string{a, b}
	}
	return [2]string{b, a}
}

// SetPartition marks the link between a and b as up (reachable=true) or cut
// (reachable=false). It is symmetric: partitioning a from b also partitions
// b from a.
func (m *MemoryTransport) SetPartition(a, b string, reachable bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := pairKey(a, b)
	if reachable {
		delete(m.partition, key)
	} else {
		m.partition[key] = true
	}
}

// HealAll clears every partition previously set with SetPartition.
func (m *MemoryTransport) HealAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.partition = make(map[[2]string]bool)
}

// SetDelay adds a fixed artificial delay before every RPC is delivered.
func (m *MemoryTransport) SetDelay(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.delay = d
}

// SetDropRate makes a fraction (0..1) of RPCs fail outright, simulating
// packet loss.
func (m *MemoryTransport) SetDropRate(p float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dropRate = p
}

func (m *MemoryTransport) reachable(from, to string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return !m.partition[pairKey(from, to)]
}

func (m *MemoryTransport) shouldDrop() bool {
	m.mu.RLock()
	p := m.dropRate
	m.mu.RUnlock()
	return p > 0 && rand.Float64() < p
}

func (m *MemoryTransport) currentDelay() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.delay
}

func (m *MemoryTransport) handlerFor(id string) (RPCHandler, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	h, ok := m.handlers[id]
	return h, ok
}

var errUnreachable = errors.New("raft: memorytransport: peer unreachable")

// callerID is not tracked automatically by MemoryTransport since Transport
// does not carry a "from" argument; instead each call site (Raft) is always
// the same node, so RequestVote/AppendEntries below are invoked once per
// *Raft via a thin per-node wrapper. See boundMemoryTransport.

// BoundMemoryTransport is a Transport bound to a specific node ID `self`;
// pass it to raft.Config.Transport for that node. Multiple
// BoundMemoryTransport values typically share one underlying
// MemoryTransport (and therefore one fault-injection configuration).
type BoundMemoryTransport struct {
	self string
	mt   *MemoryTransport
}

// Bind returns a Transport for use by the node identified by self.
func (m *MemoryTransport) Bind(self string) *BoundMemoryTransport {
	return &BoundMemoryTransport{self: self, mt: m}
}

func (b *BoundMemoryTransport) deliver(ctx context.Context, peerID string, call func(RPCHandler) (any, error)) (any, error) {
	if !b.mt.reachable(b.self, peerID) {
		return nil, errUnreachable
	}
	if b.mt.shouldDrop() {
		return nil, errUnreachable
	}
	if d := b.mt.currentDelay(); d > 0 {
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	handler, ok := b.mt.handlerFor(peerID)
	if !ok {
		return nil, errUnreachable
	}
	return call(handler)
}

// RequestVote implements raft.Transport.
func (b *BoundMemoryTransport) RequestVote(ctx context.Context, peerID string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	result, err := b.deliver(ctx, peerID, func(h RPCHandler) (any, error) {
		return h.HandleRequestVote(ctx, args)
	})
	if err != nil {
		return nil, err
	}
	return result.(*RequestVoteReply), nil
}

// AppendEntries implements raft.Transport.
func (b *BoundMemoryTransport) AppendEntries(ctx context.Context, peerID string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	result, err := b.deliver(ctx, peerID, func(h RPCHandler) (any, error) {
		return h.HandleAppendEntries(ctx, args)
	})
	if err != nil {
		return nil, err
	}
	return result.(*AppendEntriesReply), nil
}
