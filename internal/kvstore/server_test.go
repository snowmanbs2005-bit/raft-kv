package kvstore

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/snowmanbs2005-bit/raft-kv/internal/fsm"
	"github.com/snowmanbs2005-bit/raft-kv/internal/raft"
)

// fakeRaftNode is a minimal RaftNode double: it plays leader (or not) and
// immediately "commits" whatever is proposed by pushing it onto its own
// commit channel, so the whole propose -> commit -> apply -> HTTP response
// loop can be tested without spinning up a real cluster.
type fakeRaftNode struct {
	isLeader   bool
	leaderHint string
	nextIndex  uint64
	term       uint64
	commitCh   chan raft.CommitEntry
}

func newFakeRaftNode(isLeader bool) *fakeRaftNode {
	return &fakeRaftNode{isLeader: isLeader, term: 1, commitCh: make(chan raft.CommitEntry, 16)}
}

func (f *fakeRaftNode) Propose(ctx context.Context, command []byte) (uint64, uint64, bool) {
	if !f.isLeader {
		return 0, 0, false
	}
	f.nextIndex++
	entry := raft.CommitEntry{Index: f.nextIndex, Term: f.term, Command: command}
	f.commitCh <- entry
	return entry.Index, entry.Term, true
}

func (f *fakeRaftNode) IsLeader() bool                   { return f.isLeader }
func (f *fakeRaftNode) LeaderHint() string               { return f.leaderHint }
func (f *fakeRaftNode) Commits() <-chan raft.CommitEntry { return f.commitCh }

func TestServer_PutGetDelete_AsLeader(t *testing.T) {
	node := newFakeRaftNode(true)
	machine := fsm.NewKVFSM()
	s := NewServer(node, machine, func(string) (string, bool) { return "", false })
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	client := &http.Client{Timeout: 2 * time.Second}

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/kv/foo", strings.NewReader("bar"))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200", resp.StatusCode)
	}

	resp, err = client.Get(ts.URL + "/kv/foo")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	bodyBytes, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	body := string(bodyBytes)
	if resp.StatusCode != http.StatusOK || body != "bar" {
		t.Fatalf("GET = (%d, %q), want (200, bar)", resp.StatusCode, body)
	}

	req, _ = http.NewRequest(http.MethodDelete, ts.URL+"/kv/foo", nil)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE status = %d, want 200", resp.StatusCode)
	}

	resp, err = client.Get(ts.URL + "/kv/foo")
	if err != nil {
		t.Fatalf("GET after delete: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET after delete status = %d, want 404", resp.StatusCode)
	}
}

func TestServer_RedirectsWhenNotLeader(t *testing.T) {
	node := newFakeRaftNode(false)
	node.leaderHint = "node2"
	machine := fsm.NewKVFSM()
	s := NewServer(node, machine, func(id string) (string, bool) {
		if id == "node2" {
			return "127.0.0.1:9999", true
		}
		return "", false
	})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	client := &http.Client{
		Timeout: 2 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/kv/foo", strings.NewReader("bar"))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("status = %d, want 307", resp.StatusCode)
	}
	loc := resp.Header.Get("Location")
	if loc != "http://127.0.0.1:9999/kv/foo" {
		t.Errorf("Location = %q, want http://127.0.0.1:9999/kv/foo", loc)
	}
}

func TestServer_Status(t *testing.T) {
	node := newFakeRaftNode(true)
	machine := fsm.NewKVFSM()
	s := NewServer(node, machine, func(string) (string, bool) { return "", false })
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/status")
	if err != nil {
		t.Fatalf("GET /status: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}
