package raft

import (
	"context"
	"time"
)

type voteResult struct {
	peer  string
	reply *RequestVoteReply
	err   error
}

// runCandidate runs one election round: increments the term, votes for
// itself, requests votes from every peer in parallel, and waits for either
// a majority, a discovered higher term, valid leader activity, or a
// timeout (which causes the top-level loop to re-enter runCandidate and
// start a fresh round with an incremented term and a new random timeout --
// this is what makes split votes self-healing).
func (r *Raft) runCandidate() bool {
	r.currentTerm++
	r.votedFor = r.id
	r.publishState()
	if err := r.persist(); err != nil {
		// Could not durably record our own vote; fall back to follower and
		// try again later rather than risk double-voting after a crash.
		r.state = Follower
		r.publishState()
		return true
	}

	votesNeeded := (len(r.peers)+1)/2 + 1
	votesGranted := 1 // vote for self

	args := &RequestVoteArgs{
		Term:         r.currentTerm,
		CandidateID:  r.id,
		LastLogIndex: r.log.lastIndex(),
		LastLogTerm:  r.log.lastTerm(),
	}

	resultsCh := make(chan voteResult, len(r.peers))
	ctx, cancel := context.WithTimeout(context.Background(), r.electionTimeoutMax)
	defer cancel()
	for _, peer := range r.peers {
		peer := peer
		go func() {
			reply, err := r.transport.RequestVote(ctx, peer, args)
			resultsCh <- voteResult{peer: peer, reply: reply, err: err}
		}()
	}

	if votesGranted >= votesNeeded {
		r.becomeLeader()
		return true
	}

	timer := time.NewTimer(r.randomElectionTimeout())
	defer timer.Stop()

	for {
		select {
		case <-r.shutdownCh:
			r.state = Dead
			r.publishState()
			return false

		case <-timer.C:
			// Election timed out with no winner; loop back into
			// runCandidate (term will be incremented again) or, if an RPC
			// already demoted us, into runFollower.
			return true

		case res := <-resultsCh:
			if res.err != nil || res.reply == nil {
				continue
			}
			if res.reply.Term > r.currentTerm {
				r.maybeStepDown(res.reply.Term)
				return true
			}
			if res.reply.VoteGranted && res.reply.Term == r.currentTerm {
				votesGranted++
				if r.state == Candidate && votesGranted >= votesNeeded {
					r.becomeLeader()
					return true
				}
			}

		case req := <-r.rpcCh:
			r.dispatchRPC(req)
			if r.state != Candidate {
				return true
			}

		case req := <-r.proposeCh:
			req.respCh <- proposeResponse{isLeader: false}
		}
	}
}

// becomeLeader transitions the node to Leader and initializes leader-only
// volatile state. Must be called from within the event loop.
func (r *Raft) becomeLeader() {
	r.state = Leader
	r.publishState()
	r.setLeaderHint(r.id)
	for _, p := range r.peers {
		r.nextIndex[p] = r.log.lastIndex() + 1
		r.matchIndex[p] = 0
	}
}
