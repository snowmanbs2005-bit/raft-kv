package raft

// raftLog is a thin wrapper around a slice of LogEntry that keeps a dummy
// sentinel entry at position 0 (Term 0, Index 0). This mirrors the Raft
// paper's 1-based log indexing and removes a whole class of off-by-one bugs:
// entries[i].Index == i always holds, and "no previous entry" is simply
// index 0.
//
// A raftLog is only ever touched from the single Raft event-loop goroutine;
// it has no internal locking.
type raftLog struct {
	entries []LogEntry
}

func newRaftLog() *raftLog {
	return &raftLog{entries: []LogEntry{{Term: 0, Index: 0}}}
}

func newRaftLogFrom(entries []LogEntry) *raftLog {
	if len(entries) == 0 || entries[0].Index != 0 {
		full := make([]LogEntry, 0, len(entries)+1)
		full = append(full, LogEntry{Term: 0, Index: 0})
		full = append(full, entries...)
		return &raftLog{entries: full}
	}
	return &raftLog{entries: entries}
}

func (l *raftLog) lastIndex() uint64 {
	return l.entries[len(l.entries)-1].Index
}

func (l *raftLog) lastTerm() uint64 {
	return l.entries[len(l.entries)-1].Term
}

// get returns the entry at index, and whether it exists.
func (l *raftLog) get(index uint64) (LogEntry, bool) {
	if index > l.lastIndex() {
		return LogEntry{}, false
	}
	return l.entries[index], true
}

func (l *raftLog) termAt(index uint64) uint64 {
	e, ok := l.get(index)
	if !ok {
		return 0
	}
	return e.Term
}

// append adds new entries after the current last index, assigning
// sequential indexes.
func (l *raftLog) append(term uint64, commands ...[]byte) []LogEntry {
	added := make([]LogEntry, 0, len(commands))
	for _, cmd := range commands {
		e := LogEntry{Term: term, Index: l.lastIndex() + 1, Command: cmd}
		l.entries = append(l.entries, e)
		added = append(added, e)
	}
	return added
}

// truncateFrom removes all entries with index >= index (used to resolve log
// conflicts when a follower's log diverges from the leader's).
func (l *raftLog) truncateFrom(index uint64) {
	if index == 0 {
		index = 1
	}
	if index > l.lastIndex() {
		return
	}
	l.entries = l.entries[:index]
}

// appendReplicated installs entries received from the leader, starting at
// the index of the first entry, truncating any conflicting suffix first.
func (l *raftLog) appendReplicated(entries []LogEntry) {
	for _, e := range entries {
		existing, ok := l.get(e.Index)
		if ok && existing.Term == e.Term {
			continue // already present, identical
		}
		if ok {
			// conflict: diverging entry, drop it and everything after.
			l.truncateFrom(e.Index)
		}
		if e.Index == l.lastIndex()+1 {
			l.entries = append(l.entries, e)
		}
	}
}

// slice returns a copy of entries in [from, last].
func (l *raftLog) slice(from uint64) []LogEntry {
	if from > l.lastIndex() {
		return nil
	}
	if from == 0 {
		from = 1
	}
	out := make([]LogEntry, len(l.entries)-int(from))
	copy(out, l.entries[from:])
	return out
}

// all returns every entry including the sentinel, for persistence.
func (l *raftLog) all() []LogEntry {
	return l.entries
}
