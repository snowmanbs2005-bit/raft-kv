package storage

import "sync"

// MemoryStorage is a non-durable, in-memory Storage implementation. It is
// useful for tests that want realistic persist/reload semantics without
// touching disk (see internal/raft's election/replication/partition
// tests), since a real crash-and-restart is not being simulated there --
// only PersistenceTests exercise FileStorage.
type MemoryStorage struct {
	mu       sync.Mutex
	term     uint64
	votedFor string
	log      []LogEntry
}

// NewMemoryStorage returns an empty MemoryStorage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{}
}

// SaveState implements Storage.
func (m *MemoryStorage) SaveState(term uint64, votedFor string, log []LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.term = term
	m.votedFor = votedFor
	m.log = append([]LogEntry(nil), log...)
	return nil
}

// LoadState implements Storage.
func (m *MemoryStorage) LoadState() (uint64, string, []LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.term, m.votedFor, append([]LogEntry(nil), m.log...), nil
}
