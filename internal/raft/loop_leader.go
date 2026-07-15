package raft

import (
	"context"
	"time"
)

type appendResult struct {
	peer        string
	reply       *AppendEntriesReply
	err         error
	sentPrevIdx uint64
	sentLastIdx uint64
	requestTerm uint64
}

// runLeader drives the node while it believes it is the Leader: it sends
// periodic heartbeats/replication AppendEntries to every peer, accepts
// client proposals, and advances commitIndex once entries are acknowledged
// by a majority.
func (r *Raft) runLeader() bool {
	resultsCh := make(chan appendResult, len(r.peers)*4+1)

	r.broadcastAppendEntries(resultsCh)

	ticker := time.NewTicker(r.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.shutdownCh:
			r.state = Dead
			r.publishState()
			return false

		case <-ticker.C:
			if r.state != Leader {
				return true
			}
			r.broadcastAppendEntries(resultsCh)

		case req := <-r.proposeCh:
			if r.state != Leader {
				req.respCh <- proposeResponse{isLeader: false}
				continue
			}
			entries := r.log.append(r.currentTerm, req.command)
			if err := r.persist(); err != nil {
				req.respCh <- proposeResponse{isLeader: false}
				continue
			}
			entry := entries[0]
			r.matchIndex[r.id] = entry.Index
			req.respCh <- proposeResponse{index: entry.Index, term: entry.Term, isLeader: true}
			r.broadcastAppendEntries(resultsCh)

		case req := <-r.rpcCh:
			r.dispatchRPC(req)
			if r.state != Leader {
				return true
			}

		case res := <-resultsCh:
			if r.state != Leader {
				return true
			}
			if res.err != nil || res.reply == nil {
				continue // peer unreachable this round; will retry next heartbeat
			}
			if res.reply.Term > r.currentTerm {
				r.maybeStepDown(res.reply.Term)
				return true
			}
			if res.requestTerm != r.currentTerm {
				continue // stale response from a previous term
			}
			if res.reply.Success {
				if res.sentLastIdx > r.matchIndex[res.peer] {
					r.matchIndex[res.peer] = res.sentLastIdx
				}
				r.nextIndex[res.peer] = res.sentLastIdx + 1
				r.tryAdvanceCommitIndex()
			} else {
				// Fast backtrack using the conflict hint.
				if res.reply.ConflictIndex > 0 {
					r.nextIndex[res.peer] = res.reply.ConflictIndex
				} else if r.nextIndex[res.peer] > 1 {
					r.nextIndex[res.peer]--
				}
			}
		}
	}
}

// broadcastAppendEntries sends an AppendEntries RPC to every peer, carrying
// whatever entries that peer is missing according to nextIndex. Results are
// delivered asynchronously on resultsCh so the loop never blocks on a slow
// or unreachable peer.
func (r *Raft) broadcastAppendEntries(resultsCh chan<- appendResult) {
	for _, peer := range r.peers {
		peer := peer
		next := r.nextIndex[peer]
		if next == 0 {
			next = r.log.lastIndex() + 1
		}
		prevIndex := next - 1
		prevTerm := r.log.termAt(prevIndex)
		entries := r.log.slice(next)

		args := &AppendEntriesArgs{
			Term:         r.currentTerm,
			LeaderID:     r.id,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: r.commitIndex,
		}
		lastIdx := prevIndex
		if len(entries) > 0 {
			lastIdx = entries[len(entries)-1].Index
		}
		term := r.currentTerm

		ctx, cancel := context.WithTimeout(context.Background(), r.heartbeatInterval*4)
		go func() {
			defer cancel()
			reply, err := r.transport.AppendEntries(ctx, peer, args)
			select {
			case resultsCh <- appendResult{peer: peer, reply: reply, err: err, sentPrevIdx: prevIndex, sentLastIdx: lastIdx, requestTerm: term}:
			case <-r.shutdownCh:
			}
		}()
	}
}

// tryAdvanceCommitIndex implements the commitIndex-advancement rule from
// the Raft paper: commit N if a majority of matchIndex[i] >= N AND
// log[N].Term == currentTerm. The term restriction (figure 8 in the paper)
// is essential -- a leader may never commit an entry from a previous term
// merely because it is replicated on a majority; it must wait until an
// entry from its own term is.
func (r *Raft) tryAdvanceCommitIndex() {
	for n := r.log.lastIndex(); n > r.commitIndex; n-- {
		entry, ok := r.log.get(n)
		if !ok || entry.Term != r.currentTerm {
			continue
		}
		count := 1 // self
		for _, peer := range r.peers {
			if r.matchIndex[peer] >= n {
				count++
			}
		}
		if count*2 > len(r.peers)+1 {
			r.advanceCommitIndex(n)
			return
		}
	}
}
