// Command raftctl is a small CLI client for the raft-kv HTTP API.
//
// Usage:
//
//	raftctl -peers=host1:8081,host2:8082,host3:8083 put <key> <value>
//	raftctl -peers=host1:8081,host2:8082,host3:8083 get <key>
//	raftctl -peers=host1:8081,host2:8082,host3:8083 delete <key>
//	raftctl -peers=host1:8081,host2:8082,host3:8083 leader
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/snowmanbs2005-bit/raft-kv/internal/kvstore"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "raftctl:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("raftctl", flag.ContinueOnError)
	peersFlag := fs.String("peers", "", "comma-separated list of known node HTTP addresses, e.g. localhost:8081,localhost:8082,localhost:8083")
	if err := fs.Parse(args); err != nil {
		return err
	}

	rest := fs.Args()
	if len(rest) == 0 {
		return usageError()
	}
	if *peersFlag == "" {
		return fmt.Errorf("-peers is required")
	}
	peers := strings.Split(*peersFlag, ",")
	client := kvstore.NewClient(peers)

	switch cmd := rest[0]; cmd {
	case "put":
		if len(rest) != 3 {
			return fmt.Errorf("usage: raftctl -peers=... put <key> <value>")
		}
		if err := client.Put(rest[1], rest[2]); err != nil {
			return err
		}
		fmt.Println("OK")
	case "get":
		if len(rest) != 2 {
			return fmt.Errorf("usage: raftctl -peers=... get <key>")
		}
		value, err := client.Get(rest[1])
		if err != nil {
			return err
		}
		fmt.Println(value)
	case "delete":
		if len(rest) != 2 {
			return fmt.Errorf("usage: raftctl -peers=... delete <key>")
		}
		if err := client.Delete(rest[1]); err != nil {
			return err
		}
		fmt.Println("OK")
	case "leader":
		addr, err := client.Leader()
		if err != nil {
			return err
		}
		fmt.Println(addr)
	default:
		return usageError()
	}
	return nil
}

func usageError() error {
	return fmt.Errorf("usage: raftctl -peers=host1:port1,host2:port2,... <put|get|delete|leader> [args]")
}
