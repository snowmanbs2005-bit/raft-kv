package raft

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestReplication_LogsConverge(t *testing.T) {
	c := newTestCluster(t, 3)
	defer c.stop()

	leader := c.waitForLeader(2 * time.Second)

	const n = 20
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, _, isLeader := leader.Propose(ctx, []byte(fmt.Sprintf("cmd-%d", i)))
		cancel()
		if !isLeader {
			t.Fatalf("Propose(%d) failed: node is no longer leader", i)
		}
	}

	waitForCommittedCount(t, c, n, 3*time.Second)

	// Every node must apply the same sequence of commands in the same
	// order.
	var reference []CommitEntry
	for _, id := range c.ids {
		c.committedMu.Lock()
		entries := append([]CommitEntry(nil), c.committed[id]...)
		c.committedMu.Unlock()
		if len(entries) < n {
			t.Fatalf("node %s only committed %d/%d entries", id, len(entries), n)
		}
		entries = entries[:n]
		if reference == nil {
			reference = entries
			continue
		}
		for i := range reference {
			if entries[i].Index != reference[i].Index || entries[i].Term != reference[i].Term || string(entries[i].Command) != string(reference[i].Command) {
				t.Fatalf("node %s diverges from reference at position %d: got %+v, want %+v", id, i, entries[i], reference[i])
			}
		}
	}
}

func TestReplication_FollowerCatchesUpAfterDisconnect(t *testing.T) {
	c := newTestCluster(t, 3)
	defer c.stop()

	leader := c.waitForLeader(2 * time.Second)

	var laggingID string
	for _, id := range c.ids {
		if id != leader.ID() {
			laggingID = id
			break
		}
	}

	// Disconnect the lagging follower from everyone.
	for _, id := range c.ids {
		if id != laggingID {
			c.mt.SetPartition(laggingID, id, false)
		}
	}

	const whileDown = 10
	for i := 0; i < whileDown; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, _, isLeader := leader.Propose(ctx, []byte(fmt.Sprintf("a-%d", i)))
		cancel()
		if !isLeader {
			t.Fatalf("Propose(%d) failed while follower down", i)
		}
	}
	waitForLeaderCommittedCount(t, c, leader.ID(), whileDown, 2*time.Second)

	// Reconnect and propose a few more.
	c.mt.HealAll()

	const afterUp = 5
	for i := 0; i < afterUp; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, _, isLeader := leader.Propose(ctx, []byte(fmt.Sprintf("b-%d", i)))
		cancel()
		if !isLeader {
			t.Fatalf("Propose(b-%d) failed after reconnect", i)
		}
	}

	total := whileDown + afterUp
	waitForCommittedCount(t, c, total, 3*time.Second)

	c.committedMu.Lock()
	got := len(c.committed[laggingID])
	c.committedMu.Unlock()
	if got < total {
		t.Errorf("previously-lagging follower %s committed %d entries, want >= %d", laggingID, got, total)
	}
}

func TestCommitIndex_AdvancesOnMajority(t *testing.T) {
	c := newTestCluster(t, 3)
	defer c.stop()

	leader := c.waitForLeader(2 * time.Second)

	// Cut off one follower entirely; a 2-of-3 majority (leader + 1
	// follower) must still be able to commit.
	var isolated string
	for _, id := range c.ids {
		if id != leader.ID() {
			isolated = id
			break
		}
	}
	for _, id := range c.ids {
		if id != isolated {
			c.mt.SetPartition(isolated, id, false)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	idx, term, isLeader := leader.Propose(ctx, []byte("majority-write"))
	cancel()
	if !isLeader {
		t.Fatal("propose failed: not leader")
	}
	if idx == 0 || term == 0 {
		t.Fatalf("unexpected index/term: %d/%d", idx, term)
	}

	waitForLeaderCommittedCount(t, c, leader.ID(), 1, 2*time.Second)
}

// waitForCommittedCount waits until every node in the cluster has committed
// at least `want` entries. Callers should heal any partitions first if they
// need all nodes to catch up.
func waitForCommittedCount(t *testing.T, c *testCluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c.committedMu.Lock()
		minCount := want
		allPresent := len(c.committed) == len(c.ids)
		if allPresent {
			minCount = -1
			for _, id := range c.ids {
				n := len(c.committed[id])
				if minCount == -1 || n < minCount {
					minCount = n
				}
			}
		}
		c.committedMu.Unlock()
		if allPresent && minCount >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d committed entries on every node", want)
}

// waitForLeaderCommittedCount is like waitForCommittedCount but only
// requires the given node (typically the leader, or a node known to be in
// the majority partition) to reach the count -- used when some nodes are
// deliberately isolated and are not expected to make progress.
func waitForLeaderCommittedCount(t *testing.T, c *testCluster, id string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c.committedMu.Lock()
		n := len(c.committed[id])
		c.committedMu.Unlock()
		if n >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for node %s to commit %d entries", id, want)
}
