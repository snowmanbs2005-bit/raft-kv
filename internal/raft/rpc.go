package raft

// RequestVoteArgs is sent by a candidate to gather votes.
type RequestVoteArgs struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

// RequestVoteReply is the response to a RequestVote RPC.
type RequestVoteReply struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

// AppendEntriesArgs is sent by the leader both to replicate log entries and
// as a heartbeat (when Entries is empty).
type AppendEntriesArgs struct {
	Term         uint64     `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex uint64     `json:"prev_log_index"`
	PrevLogTerm  uint64     `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit uint64     `json:"leader_commit"`
}

// AppendEntriesReply is the response to an AppendEntries RPC. ConflictIndex
// and ConflictTerm implement the fast log back-tracking optimization
// described in the Raft paper's extended appendix, so a lagging follower can
// catch up in O(1) round trips per conflicting term instead of one RPC per
// entry.
type AppendEntriesReply struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	ConflictIndex uint64 `json:"conflict_index"`
	ConflictTerm  uint64 `json:"conflict_term"`
}
