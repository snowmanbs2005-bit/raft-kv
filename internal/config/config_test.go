package config

import (
	"flag"
	"testing"
)

func TestParseFlags_Basic(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	args := []string{
		"--id=node1",
		"--peers=node1=127.0.0.1:9001:127.0.0.1:8081,node2=127.0.0.1:9002:127.0.0.1:8082",
		"--data-dir=/tmp/data1",
	}
	cfg, err := ParseFlags(fs, args)
	if err != nil {
		t.Fatalf("ParseFlags: %v", err)
	}
	if cfg.ID != "node1" {
		t.Errorf("ID = %q, want node1", cfg.ID)
	}
	if cfg.RaftListenAddr != "127.0.0.1:9001" {
		t.Errorf("RaftListenAddr = %q, want 127.0.0.1:9001", cfg.RaftListenAddr)
	}
	if cfg.HTTPListenAddr != "127.0.0.1:8081" {
		t.Errorf("HTTPListenAddr = %q, want 127.0.0.1:8081", cfg.HTTPListenAddr)
	}
	if len(cfg.PeerIDs()) != 1 || cfg.PeerIDs()[0] != "node2" {
		t.Errorf("PeerIDs() = %v, want [node2]", cfg.PeerIDs())
	}
	addr, ok := cfg.RaftAddrOf("node2")
	if !ok || addr != "127.0.0.1:9002" {
		t.Errorf("RaftAddrOf(node2) = (%q, %v), want (127.0.0.1:9002, true)", addr, ok)
	}
}

func TestParseFlags_MissingID(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	_, err := ParseFlags(fs, []string{"--peers=node1=127.0.0.1:9001:127.0.0.1:8081"})
	if err == nil {
		t.Fatal("expected error for missing --id")
	}
}

func TestParseFlags_UnknownSelfID(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	_, err := ParseFlags(fs, []string{
		"--id=ghost",
		"--peers=node1=127.0.0.1:9001:127.0.0.1:8081",
	})
	if err == nil {
		t.Fatal("expected error when --id is not present in --peers")
	}
}

func TestParseFlags_MalformedPeers(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	_, err := ParseFlags(fs, []string{
		"--id=node1",
		"--peers=node1-127.0.0.1",
	})
	if err == nil {
		t.Fatal("expected error for malformed --peers")
	}
}
