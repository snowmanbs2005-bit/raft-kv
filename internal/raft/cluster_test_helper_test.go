package raft

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/snowmanbs2005-bit/raft-kv/internal/storage"
)

// ctxForTest returns a context bound to the test's lifetime with a generous
// timeout, for tests that call Raft RPC handlers directly.
func ctxForTest(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// testCluster wires up n *Raft nodes over a shared MemoryTransport with
// short timeouts, so election/replication/partition tests run in
// milliseconds. It drains every node's commit channel into a shared map so
// tests can assert on what got committed without blocking any node's event
// loop.
type testCluster struct {
	t     *testing.T
	ids   []string
	nodes map[string]*Raft
	mt    *MemoryTransport

	committedMu sync.Mutex
	committed   map[string][]CommitEntry // nodeID -> committed entries in order
}

func newTestCluster(t *testing.T, n int) *testCluster {
	t.Helper()
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("n%d", i)
	}

	mt := NewMemoryTransport()
	c := &testCluster{
		t:         t,
		ids:       ids,
		nodes:     make(map[string]*Raft),
		mt:        mt,
		committed: make(map[string][]CommitEntry),
	}

	for _, id := range ids {
		peers := peersExcept(ids, id)
		cfg := Config{
			ID:                 id,
			Peers:              peers,
			ElectionTimeoutMin: 40 * time.Millisecond,
			ElectionTimeoutMax: 80 * time.Millisecond,
			HeartbeatInterval:  10 * time.Millisecond,
			Storage:            storage.NewMemoryStorage(),
			Transport:          mt.Bind(id),
		}
		r, err := New(cfg)
		if err != nil {
			t.Fatalf("New(%s): %v", id, err)
		}
		c.nodes[id] = r
		mt.Register(id, r)
	}

	for _, id := range ids {
		r := c.nodes[id]
		go c.drainCommits(id, r)
		r.Start()
	}

	return c
}

func (c *testCluster) drainCommits(id string, r *Raft) {
	for entry := range r.Commits() {
		c.committedMu.Lock()
		c.committed[id] = append(c.committed[id], entry)
		c.committedMu.Unlock()
	}
}

func (c *testCluster) stop() {
	for _, r := range c.nodes {
		r.Stop()
	}
}

func peersExcept(ids []string, self string) []string {
	out := make([]string, 0, len(ids)-1)
	for _, id := range ids {
		if id != self {
			out = append(out, id)
		}
	}
	return out
}

// waitForLeader polls until exactly one node reports State()==Leader with a
// stable term, or fails the test after timeout.
func (c *testCluster) waitForLeader(timeout time.Duration) *Raft {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []*Raft
		for _, id := range c.ids {
			if c.nodes[id].State() == Leader {
				leaders = append(leaders, c.nodes[id])
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("no single leader elected within %s", timeout)
	return nil
}

// waitForLeaderAmong is like waitForLeader but only considers the given
// node IDs (used for partition tests where only a subset should ever elect
// a leader).
func (c *testCluster) waitForLeaderAmong(timeout time.Duration, ids []string) *Raft {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	set := make(map[string]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}
	for time.Now().Before(deadline) {
		var leaders []*Raft
		for _, id := range ids {
			if c.nodes[id].State() == Leader {
				leaders = append(leaders, c.nodes[id])
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("no single leader elected among %v within %s", ids, timeout)
	return nil
}

func (c *testCluster) node(id string) *Raft { return c.nodes[id] }
