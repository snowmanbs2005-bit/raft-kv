package raft

import "time"

// runFollower drives the node while it believes it is a Follower. It
// returns true to keep the loop running (possibly in a new state) and false
// once the node has been permanently stopped.
func (r *Raft) runFollower() bool {
	timeout := r.randomElectionTimeout()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-r.shutdownCh:
			r.state = Dead
			r.publishState()
			return false

		case <-timer.C:
			// No AppendEntries/RequestVote-granting activity within the
			// timeout: start an election.
			r.state = Candidate
			r.publishState()
			return true

		case req := <-r.rpcCh:
			resetTimer := r.dispatchRPC(req)
			if resetTimer {
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(r.randomElectionTimeout())
			}

		case req := <-r.proposeCh:
			req.respCh <- proposeResponse{isLeader: false}
		}
	}
}

// dispatchRPC processes one incoming RPC request and replies on its
// response channel. It returns true if the election timer should be reset
// (i.e. the RPC was legitimate leader/candidate activity).
func (r *Raft) dispatchRPC(req rpcRequest) bool {
	switch {
	case req.requestVote != nil:
		reply := r.processRequestVote(req.requestVote)
		req.respCh <- rpcResponse{voteReply: reply}
		return reply.VoteGranted
	case req.appendEntries != nil:
		reply := r.processAppendEntries(req.appendEntries)
		req.respCh <- rpcResponse{appendReply: reply}
		return reply.Success || req.appendEntries.Term >= r.currentTerm
	default:
		return false
	}
}
