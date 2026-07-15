// Package config parses node configuration: this node's ID, the full peer
// list (ID, Raft RPC address, HTTP API address) and where to keep durable
// state on disk.
package config

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

// Peer describes one cluster member's network endpoints.
type Peer struct {
	ID       string
	RaftAddr string // host:port the peer's RPC transport listens on
	HTTPAddr string // host:port the peer's KV HTTP API listens on
}

// Config is a single node's full configuration.
type Config struct {
	ID      string
	Peers   []Peer // every node in the cluster, including this one
	DataDir string

	RaftListenAddr string // address this node's RPC server binds to
	HTTPListenAddr string // address this node's HTTP API binds to
}

// Self returns this node's own Peer entry.
func (c Config) Self() (Peer, error) {
	for _, p := range c.Peers {
		if p.ID == c.ID {
			return p, nil
		}
	}
	return Peer{}, fmt.Errorf("config: node id %q not found in --peers", c.ID)
}

// PeerIDs returns the IDs of every peer other than this node.
func (c Config) PeerIDs() []string {
	out := make([]string, 0, len(c.Peers)-1)
	for _, p := range c.Peers {
		if p.ID != c.ID {
			out = append(out, p.ID)
		}
	}
	return out
}

// RaftAddrOf returns the Raft RPC address of the given peer ID.
func (c Config) RaftAddrOf(id string) (string, bool) {
	for _, p := range c.Peers {
		if p.ID == id {
			return p.RaftAddr, true
		}
	}
	return "", false
}

// HTTPAddrOf returns the HTTP API address of the given peer ID.
func (c Config) HTTPAddrOf(id string) (string, bool) {
	for _, p := range c.Peers {
		if p.ID == id {
			return p.HTTPAddr, true
		}
	}
	return "", false
}

// ParseFlags parses node configuration from command-line flags (and falls
// back to environment variables RAFTKV_ID, RAFTKV_PEERS, RAFTKV_DATA_DIR,
// RAFTKV_RAFT_ADDR, RAFTKV_HTTP_ADDR when the corresponding flag is not
// set), out of fs/args so it is testable without touching the real
// flag.CommandLine or os.Args.
//
// --peers format: comma-separated "id=raftAddr:httpAddr" entries, e.g.
//
//	--peers=node1=127.0.0.1:9001:127.0.0.1:8081,node2=127.0.0.1:9002:127.0.0.1:8082
func ParseFlags(fs *flag.FlagSet, args []string) (Config, error) {
	id := fs.String("id", envDefault("RAFTKV_ID", ""), "this node's unique ID")
	peersRaw := fs.String("peers", envDefault("RAFTKV_PEERS", ""), "comma-separated id=raftAddr:httpAddr entries for every cluster member")
	dataDir := fs.String("data-dir", envDefault("RAFTKV_DATA_DIR", "./data"), "directory for persistent Raft state")
	raftAddr := fs.String("raft-addr", envDefault("RAFTKV_RAFT_ADDR", ""), "override for this node's own Raft RPC listen address (defaults to the address in --peers)")
	httpAddr := fs.String("http-addr", envDefault("RAFTKV_HTTP_ADDR", ""), "override for this node's own HTTP listen address (defaults to the address in --peers)")

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	if *id == "" {
		return Config{}, fmt.Errorf("config: --id is required")
	}
	peers, err := parsePeers(*peersRaw)
	if err != nil {
		return Config{}, err
	}
	if len(peers) == 0 {
		return Config{}, fmt.Errorf("config: --peers must list at least one node")
	}

	cfg := Config{ID: *id, Peers: peers, DataDir: *dataDir}
	self, err := cfg.Self()
	if err != nil {
		return Config{}, err
	}
	cfg.RaftListenAddr = self.RaftAddr
	cfg.HTTPListenAddr = self.HTTPAddr
	if *raftAddr != "" {
		cfg.RaftListenAddr = *raftAddr
	}
	if *httpAddr != "" {
		cfg.HTTPListenAddr = *httpAddr
	}
	return cfg, nil
}

func parsePeers(raw string) ([]Peer, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	peers := make([]Peer, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		fields := strings.SplitN(part, "=", 2)
		if len(fields) != 2 {
			return nil, fmt.Errorf("config: malformed --peers entry %q, want id=raftHost:raftPort:httpHost:httpPort", part)
		}
		id := fields[0]
		addrs := fields[1]
		raftAddr, httpAddr, err := splitAddrPair(addrs)
		if err != nil {
			return nil, fmt.Errorf("config: peer %q: %w", id, err)
		}
		peers = append(peers, Peer{ID: id, RaftAddr: raftAddr, HTTPAddr: httpAddr})
	}
	return peers, nil
}

// splitAddrPair splits "host:raftPort:host:httpPort" into
// ("host:raftPort", "host:httpPort"). Addresses are expected to have
// exactly one colon each (host and port), so the combined string has
// exactly three colons.
func splitAddrPair(s string) (raftAddr, httpAddr string, err error) {
	parts := strings.Split(s, ":")
	if len(parts) != 4 {
		return "", "", fmt.Errorf("expected host:raftPort:host:httpPort, got %q", s)
	}
	return parts[0] + ":" + parts[1], parts[2] + ":" + parts[3], nil
}

func envDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
