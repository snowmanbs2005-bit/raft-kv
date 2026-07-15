package raft

import (
	"context"
	"testing"
	"time"
)

// TestPartition_MinorityCannotCommit splits a 5-node cluster into a
// majority side (3 nodes) and a minority side (2 nodes). The minority side
// must never be able to elect a leader that can commit anything, because it
// can never gather a majority of votes/acks on its own.
func TestPartition_MinorityCannotCommit(t *testing.T) {
	c := newTestCluster(t, 5)
	defer c.stop()

	leader := c.waitForLeader(2 * time.Second)

	// Pick a minority side of 2 nodes not containing the current leader
	// where possible, to make the scenario about a genuine minority
	// losing its leader/quorum rather than the majority re-electing.
	var minority, majority []string
	for _, id := range c.ids {
		if id != leader.ID() && len(minority) < 2 {
			minority = append(minority, id)
		} else {
			majority = append(majority, id)
		}
	}

	// Cut every link between minority and majority sides.
	for _, a := range minority {
		for _, b := range majority {
			c.mt.SetPartition(a, b, false)
		}
	}

	// The majority side (which still includes the original leader or can
	// elect a new one) must be able to commit.
	newLeader := c.waitForLeaderAmong(2*time.Second, majority)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, _, isLeader := newLeader.Propose(ctx, []byte("majority-write"))
	cancel()
	if !isLeader {
		t.Fatal("majority-side leader rejected propose")
	}
	waitForLeaderCommittedCount(t, c, newLeader.ID(), 1, 2*time.Second)

	// The minority side must NOT converge on a leader that can commit.
	// It's fine (and expected, per the paper) for a minority node to
	// become Candidate or even briefly believe itself Leader after
	// winning votes only from itself in tiny minorities -- what must NOT
	// happen is any commit progress on that side.
	time.Sleep(300 * time.Millisecond)
	for _, id := range minority {
		c.committedMu.Lock()
		n := len(c.committed[id])
		c.committedMu.Unlock()
		if n > 0 {
			t.Errorf("minority node %s committed %d entries, want 0 (no quorum available)", id, n)
		}
	}
}

// TestPartition_HealsAndReconverges verifies that after a partition is
// healed, all nodes' logs converge to the same committed sequence.
func TestPartition_HealsAndReconverges(t *testing.T) {
	c := newTestCluster(t, 5)
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
	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, _, isLeader := majLeader.Propose(ctx, []byte("during-partition"))
		cancel()
		if !isLeader {
			t.Fatalf("propose %d failed during partition", i)
		}
	}
	waitForLeaderCommittedCount(t, c, majLeader.ID(), 5, 2*time.Second)

	c.mt.HealAll()

	// After healing, a single leader must emerge cluster-wide and every
	// node must eventually commit the same 5 entries.
	c.waitForLeader(2 * time.Second)
	waitForCommittedCount(t, c, 5, 3*time.Second)

	var reference []CommitEntry
	for _, id := range c.ids {
		c.committedMu.Lock()
		entries := append([]CommitEntry(nil), c.committed[id][:5]...)
		c.committedMu.Unlock()
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
