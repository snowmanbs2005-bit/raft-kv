// Package integration spins up multiple in-process *raft.Raft nodes wired
// together via raft.MemoryTransport and drives full cluster scenarios:
// startup election, leader crash and re-election, network partitions, and
// concurrent client writes. Unlike internal/raft's own unit tests (which
// exercise individual mechanisms in isolation), these tests treat the
// cluster as a black box and only assert on externally observable
// behavior: who becomes leader, what gets committed, and that every node
// ends up agreeing on the same log.
package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/snowmanbs2005-bit/raft-kv/internal/raft"
	"github.com/snowmanbs2005-bit/raft-kv/internal/storage"
)

type cluster struct {
	t     *testing.T
	ids   []string
	nodes map[string]*raft.Raft
	mt    *raft.MemoryTransport

	mu        sync.Mutex
	committed map[string][]raft.CommitEntry
}

func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("node%d", i)
	}

	mt := raft.NewMemoryTransport()
	c := &cluster{t: t, ids: ids, nodes: make(map[string]*raft.Raft), mt: mt, committed: make(map[string][]raft.CommitEntry)}

	for _, id := range ids {
		peers := except(ids, id)
		node, err := raft.New(raft.Config{
			ID:                 id,
			Peers:              peers,
			ElectionTimeoutMin: 50 * time.Millisecond,
			ElectionTimeoutMax: 100 * time.Millisecond,
			HeartbeatInterval:  15 * time.Millisecond,
			Storage:            storage.NewMemoryStorage(),
			Transport:          mt.Bind(id),
		})
		if err != nil {
			t.Fatalf("raft.New(%s): %v", id, err)
		}
		c.nodes[id] = node
		mt.Register(id, node)
	}
	for _, id := range ids {
		node := c.nodes[id]
		go c.drain(id, node)
		node.Start()
	}
	return c
}

func (c *cluster) drain(id string, node *raft.Raft) {
	for entry := range node.Commits() {
		c.mu.Lock()
		c.committed[id] = append(c.committed[id], entry)
		c.mu.Unlock()
	}
}

func (c *cluster) stop() {
	for _, n := range c.nodes {
		n.Stop()
	}
}

func except(ids []string, self string) []string {
	out := make([]string, 0, len(ids)-1)
	for _, id := range ids {
		if id != self {
			out = append(out, id)
		}
	}
	return out
}

func (c *cluster) waitForLeaderAmong(timeout time.Duration, ids []string) *raft.Raft {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []*raft.Raft
		for _, id := range ids {
			if c.nodes[id].State() == raft.Leader {
				leaders = append(leaders, c.nodes[id])
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("no single leader among %v within %s", ids, timeout)
	return nil
}

func (c *cluster) waitForLeader(timeout time.Duration) *raft.Raft {
	return c.waitForLeaderAmong(timeout, c.ids)
}

func (c *cluster) committedCount(id string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.committed[id])
}

func (c *cluster) waitForCommitCount(id string, want int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.committedCount(id) >= want {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

func propose(t *testing.T, node *raft.Raft, cmd string) (uint64, uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	idx, term, isLeader := node.Propose(ctx, []byte(cmd))
	if !isLeader {
		t.Fatalf("propose(%q) failed: node %s is not leader", cmd, node.ID())
	}
	return idx, term
}

// TestCluster_LeaderElectedAfterStart verifies a freshly started 5-node
// cluster converges on exactly one leader, and that leader can commit.
func TestCluster_LeaderElectedAfterStart(t *testing.T) {
	c := newCluster(t, 5)
	defer c.stop()

	leader := c.waitForLeader(2 * time.Second)
	propose(t, leader, "hello")

	if !c.waitForCommitCount(leader.ID(), 1, time.Second) {
		t.Fatalf("leader %s never committed the proposed entry", leader.ID())
	}
}

// TestCluster_LeaderCrash_NewLeaderElected stops the current leader (as if
// it crashed) and verifies the remaining nodes elect a new leader that can
// keep making progress.
func TestCluster_LeaderCrash_NewLeaderElected(t *testing.T) {
	c := newCluster(t, 5)
	defer c.stop()

	firstLeader := c.waitForLeader(2 * time.Second)
	propose(t, firstLeader, "before-crash")
	if !c.waitForCommitCount(firstLeader.ID(), 1, time.Second) {
		t.Fatal("first leader failed to commit before crash")
	}

	firstLeaderID := firstLeader.ID()
	firstLeader.Stop()

	remaining := except(c.ids, firstLeaderID)
	newLeader := c.waitForLeaderAmong(3*time.Second, remaining)
	if newLeader.ID() == firstLeaderID {
		t.Fatalf("new leader should differ from crashed node %s", firstLeaderID)
	}

	propose(t, newLeader, "after-crash")
	if !c.waitForCommitCount(newLeader.ID(), 1, time.Second) {
		t.Fatalf("new leader %s failed to commit after taking over", newLeader.ID())
	}
}

// TestCluster_NetworkPartition_MinorityCannotCommit splits a 5-node cluster
// into a 3/2 partition and verifies only the majority side can make
// progress.
func TestCluster_NetworkPartition_MinorityCannotCommit(t *testing.T) {
	c := newCluster(t, 5)
	defer c.stop()

	leader := c.waitForLeader(2 * time.Second)

	var minority, majority []string
	for _, id := range c.ids {
		if id != leader.ID() && len(minority) < 2 {
			minority = append(minority, id)
		} else {
			majority = append(majority, id)
		}
	}
	for _, a := range minority {
		for _, b := range majority {
			c.mt.SetPartition(a, b, false)
		}
	}

	majLeader := c.waitForLeaderAmong(2*time.Second, majority)
	propose(t, majLeader, "majority-progress")
	if !c.waitForCommitCount(majLeader.ID(), 1, 2*time.Second) {
		t.Fatal("majority side failed to commit despite having quorum")
	}

	time.Sleep(300 * time.Millisecond)
	for _, id := range minority {
		if n := c.committedCount(id); n > 0 {
			t.Errorf("minority node %s committed %d entries without a quorum", id, n)
		}
	}

	// Heal and confirm the whole cluster reconverges.
	c.mt.HealAll()
	c.waitForLeader(2 * time.Second)
	for _, id := range c.ids {
		if !c.waitForCommitCount(id, 1, 3*time.Second) {
			t.Errorf("node %s never caught up after partition healed", id)
		}
	}
}

// TestCluster_ConcurrentClientWrites_NoDataLoss fires many concurrent
// proposals at the leader from multiple goroutines and verifies every
// accepted proposal is eventually committed and that all nodes end up with
// identical logs -- this is also the test to run with -race.
func TestCluster_ConcurrentClientWrites_NoDataLoss(t *testing.T) {
	c := newCluster(t, 3)
	defer c.stop()

	leader := c.waitForLeader(2 * time.Second)

	const goroutines = 8
	const perGoroutine = 15
	total := goroutines * perGoroutine

	var wg sync.WaitGroup
	var acceptedMu sync.Mutex
	accepted := 0
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_, _, isLeader := leader.Propose(ctx, []byte(fmt.Sprintf("g%d-%d", g, i)))
				cancel()
				if isLeader {
					acceptedMu.Lock()
					accepted++
					acceptedMu.Unlock()
				}
			}
		}(g)
	}
	wg.Wait()

	if accepted == 0 {
		t.Fatal("no proposals were accepted")
	}
	if accepted > total {
		t.Fatalf("accepted %d > total issued %d", accepted, total)
	}

	if !c.waitForCommitCount(leader.ID(), accepted, 3*time.Second) {
		t.Fatalf("leader only committed %d/%d accepted proposals", c.committedCount(leader.ID()), accepted)
	}
	for _, id := range c.ids {
		if !c.waitForCommitCount(id, accepted, 3*time.Second) {
			t.Errorf("node %s only committed %d/%d accepted proposals", id, c.committedCount(id), accepted)
		}
	}

	// Every node must agree on the exact same sequence (no lost or
	// reordered entries).
	c.mu.Lock()
	defer c.mu.Unlock()
	var reference []raft.CommitEntry
	for _, id := range c.ids {
		entries := c.committed[id][:accepted]
		if reference == nil {
			reference = entries
			continue
		}
		for i := range reference {
			if entries[i].Term != reference[i].Term || string(entries[i].Command) != string(reference[i].Command) {
				t.Fatalf("node %s entry %d = %+v, want %+v", id, i, entries[i], reference[i])
			}
		}
	}
}
