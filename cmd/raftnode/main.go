// Command raftnode runs a single Raft cluster member: the Raft consensus
// core, an HTTP/JSON transport for talking to peers, a replicated KV state
// machine, and an HTTP API for clients.
package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/snowmanbs2005-bit/raft-kv/internal/config"
	"github.com/snowmanbs2005-bit/raft-kv/internal/fsm"
	"github.com/snowmanbs2005-bit/raft-kv/internal/kvstore"
	"github.com/snowmanbs2005-bit/raft-kv/internal/raft"
	"github.com/snowmanbs2005-bit/raft-kv/internal/storage"
	"github.com/snowmanbs2005-bit/raft-kv/internal/transport/httprpc"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("raftnode: %v", err)
	}
}

func run() error {
	cfg, err := config.ParseFlags(flag.NewFlagSet("raftnode", flag.ExitOnError), os.Args[1:])
	if err != nil {
		return err
	}

	fileStorage, err := storage.NewFileStorage(cfg.DataDir)
	if err != nil {
		return err
	}

	rpcClient := httprpc.NewClient(func(peerID string) (string, bool) {
		return cfg.RaftAddrOf(peerID)
	})

	node, err := raft.New(raft.Config{
		ID:                 cfg.ID,
		Peers:              cfg.PeerIDs(),
		ElectionTimeoutMin: 300 * time.Millisecond,
		ElectionTimeoutMax: 600 * time.Millisecond,
		HeartbeatInterval:  75 * time.Millisecond,
		Storage:            fileStorage,
		Transport:          rpcClient,
	})
	if err != nil {
		return err
	}

	rpcServer := httprpc.NewServer(node)
	raftMux := http.NewServeMux()
	rpcServer.RegisterOn(raftMux)
	raftHTTPServer := &http.Server{Addr: cfg.RaftListenAddr, Handler: raftMux}

	machine := fsm.NewKVFSM()
	kvServer := kvstore.NewServer(node, machine, func(nodeID string) (string, bool) {
		return cfg.HTTPAddrOf(nodeID)
	})
	apiHTTPServer := &http.Server{Addr: cfg.HTTPListenAddr, Handler: kvServer.Handler()}

	errCh := make(chan error, 2)
	go func() {
		log.Printf("raftnode %s: raft RPC listening on %s", cfg.ID, cfg.RaftListenAddr)
		if err := raftHTTPServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()
	go func() {
		log.Printf("raftnode %s: kv HTTP API listening on %s", cfg.ID, cfg.HTTPListenAddr)
		if err := apiHTTPServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	node.Start()
	log.Printf("raftnode %s: started (peers: %v, data dir: %s)", cfg.ID, cfg.PeerIDs(), cfg.DataDir)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Printf("raftnode %s: received %s, shutting down", cfg.ID, sig)
	case err := <-errCh:
		log.Printf("raftnode %s: server error: %v", cfg.ID, err)
	}

	node.Stop()
	_ = raftHTTPServer.Close()
	_ = apiHTTPServer.Close()
	return nil
}
