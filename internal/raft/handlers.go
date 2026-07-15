package raft

// This file implements the request-processing logic for the two Raft RPCs.
// Both functions are only ever called from inside the run() goroutine, so
// they can read and write currentTerm/votedFor/log/state freely without
// synchronization.

// maybeStepDown converts the node to Follower if it sees a strictly higher
// term, as required by the Raft paper ("Rules for Servers": all servers).
// Returns true if the node stepped down (or already was a follower in this
// term).
func (r *Raft) maybeStepDown(term uint64) {
	if term > r.currentTerm {
		r.currentTerm = term
		r.votedFor = ""
		r.state = Follower
		r.publishState()
	}
}

// processRequestVote implements the RequestVote RPC handler (Raft paper
// Figure 2). It persists state before granting a vote.
func (r *Raft) processRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	r.maybeStepDown(args.Term)

	reply := &RequestVoteReply{Term: r.currentTerm}

	if args.Term < r.currentTerm {
		reply.VoteGranted = false
		return reply
	}

	alreadyVotedForOther := r.votedFor != "" && r.votedFor != args.CandidateID
	candidateUpToDate := args.LastLogTerm > r.log.lastTerm() ||
		(args.LastLogTerm == r.log.lastTerm() && args.LastLogIndex >= r.log.lastIndex())

	if alreadyVotedForOther || !candidateUpToDate {
		reply.VoteGranted = false
		return reply
	}

	r.votedFor = args.CandidateID
	if err := r.persist(); err != nil {
		// Persistence failure: do not grant the vote, since we cannot
		// guarantee we will remember having granted it after a crash.
		r.votedFor = ""
		reply.VoteGranted = false
		return reply
	}
	reply.VoteGranted = true
	// Granting a vote resets our own election timer (handled by caller
	// returning a "reset timer" signal); record the candidate as a
	// plausible future leader hint only once it actually wins.
	return reply
}

// processAppendEntries implements the AppendEntries RPC handler (Raft paper
// Figure 2), including the fast-backtrack conflict optimization.
func (r *Raft) processAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	r.maybeStepDown(args.Term)

	reply := &AppendEntriesReply{Term: r.currentTerm}

	if args.Term < r.currentTerm {
		reply.Success = false
		return reply
	}

	// A valid AppendEntries from the current term's leader: stay/become
	// follower and remember the leader.
	r.state = Follower
	r.publishState()
	r.setLeaderHint(args.LeaderID)

	// Consistency check on PrevLogIndex/PrevLogTerm.
	if args.PrevLogIndex > 0 {
		entry, ok := r.log.get(args.PrevLogIndex)
		if !ok {
			reply.Success = false
			reply.ConflictIndex = r.log.lastIndex() + 1
			reply.ConflictTerm = 0
			return reply
		}
		if entry.Term != args.PrevLogTerm {
			reply.Success = false
			reply.ConflictTerm = entry.Term
			// Find the first index of ConflictTerm to let the leader skip
			// the whole conflicting term in one round trip.
			idx := args.PrevLogIndex
			for idx > 1 {
				prev, ok := r.log.get(idx - 1)
				if !ok || prev.Term != entry.Term {
					break
				}
				idx--
			}
			reply.ConflictIndex = idx
			return reply
		}
	}

	if len(args.Entries) > 0 {
		r.log.appendReplicated(args.Entries)
		if err := r.persist(); err != nil {
			reply.Success = false
			return reply
		}
	}

	if args.LeaderCommit > r.commitIndex {
		newCommit := args.LeaderCommit
		if r.log.lastIndex() < newCommit {
			newCommit = r.log.lastIndex()
		}
		r.advanceCommitIndex(newCommit)
	}

	reply.Success = true
	return reply
}

// advanceCommitIndex moves commitIndex forward to target (if greater) and
// publishes every newly committed entry on commitCh, in order.
func (r *Raft) advanceCommitIndex(target uint64) {
	if target <= r.commitIndex {
		return
	}
	for idx := r.commitIndex + 1; idx <= target; idx++ {
		entry, ok := r.log.get(idx)
		if !ok {
			break
		}
		select {
		case r.commitCh <- CommitEntry{Index: entry.Index, Term: entry.Term, Command: entry.Command}:
		case <-r.shutdownCh:
			return
		}
	}
	r.commitIndex = target
}
