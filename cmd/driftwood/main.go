// Command driftwood runs and talks to a Driftwood cluster.
//
//	driftwood serve  -id 0 -peers 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 -data ./data/0
//	driftwood put    -servers ... KEY VALUE
//	driftwood append -servers ... KEY VALUE
//	driftwood get    -servers ... KEY
//	driftwood status -servers ...
//	driftwood bench  -servers ... -clients 8 -duration 10s
//	driftwood demo                    # 5 local nodes; kills the leader mid-write
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/suhas-svg/driftwood/kv"
	"github.com/suhas-svg/driftwood/transport"
)

const defaultServers = "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003"

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "serve":
		err = serve(args)
	case "get", "put", "append":
		err = clientOp(cmd, args)
	case "status":
		err = status(args)
	case "bench":
		err = bench(args)
	case "demo":
		err = demo(args)
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "driftwood:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: driftwood <command> [flags]

commands:
  serve    run one server
  get      read a key
  put      set a key
  append   append to a key
  status   show every server's view of the cluster
  bench    measure throughput and latency
  demo     start 5 local servers, write continuously, kill the leader

run "driftwood <command> -h" for a command's flags`)
	os.Exit(2)
}

func splitAddrs(s string) []string {
	var out []string
	for _, a := range strings.Split(s, ",") {
		if a = strings.TrimSpace(a); a != "" {
			out = append(out, a)
		}
	}
	return out
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	id := fs.Int("id", 0, "this server's index in -peers")
	peers := fs.String("peers", defaultServers, "comma-separated addresses of every server, same order on all")
	dir := fs.String("data", "", "directory for this server's state (default ./data/<id>)")
	maxRaftState := fs.Int("maxraftstate", 64<<10, "snapshot when Raft state reaches this many bytes; 0 never")
	noSync := fs.Bool("nosync", false, "skip fsync on every write (faster, but a crash can lose acknowledged writes)")
	fs.Parse(args)
	if *dir == "" {
		*dir = fmt.Sprintf("data/%d", *id)
	}

	node, err := transport.StartNode(transport.NodeConfig{
		ID:           *id,
		Addrs:        splitAddrs(*peers),
		Dir:          *dir,
		MaxRaftState: *maxRaftState,
		NoSync:       *noSync,
	})
	if err != nil {
		return err
	}
	fmt.Printf("driftwood: server %d listening on %s, state in %s\n", *id, node.Addr(), *dir)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	select {
	case <-sig:
		node.Stop()
		return nil
	case err := <-waitErr(node):
		return err
	}
}

func waitErr(n *transport.Node) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- n.Wait() }()
	return ch
}

func newClerk(servers string) *kv.Clerk {
	return kv.MakeClerk(transport.Peers(splitAddrs(servers), "KV", transport.ClientTimeout))
}

func clientOp(cmd string, args []string) error {
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	servers := fs.String("servers", defaultServers, "comma-separated server addresses")
	timeout := fs.Duration("timeout", 10*time.Second, "give up after this long")
	fs.Parse(args)
	want := map[string]int{"get": 1, "put": 2, "append": 2}[cmd]
	if fs.NArg() != want {
		return fmt.Errorf("%s takes %d argument(s), got %d", cmd, want, fs.NArg())
	}

	ck := newClerk(*servers)
	result := make(chan string, 1)
	go func() {
		switch cmd {
		case "get":
			result <- ck.Get(fs.Arg(0))
		case "put":
			ck.Put(fs.Arg(0), fs.Arg(1))
			result <- ""
		case "append":
			ck.Append(fs.Arg(0), fs.Arg(1))
			result <- ""
		}
	}()
	select {
	case v := <-result:
		if cmd == "get" {
			fmt.Println(v)
		}
		return nil
	case <-time.After(*timeout):
		// The operation may still commit later: the clerk was retrying it.
		return fmt.Errorf("%s: no majority reachable within %v", cmd, *timeout)
	}
}

func status(args []string) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	servers := fs.String("servers", defaultServers, "comma-separated server addresses")
	fs.Parse(args)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tADDRESS\tROLE\tTERM\tAPPLIED\tKEYS\tRAFT STATE")
	for i, addr := range splitAddrs(*servers) {
		var r kv.StatusReply
		p := transport.NewPeer(addr, "KV", time.Second)
		if !p.Call("Status", &kv.StatusArgs{}, &r) {
			fmt.Fprintf(w, "%d\t%s\tdown\t\t\t\t\n", i, addr)
			continue
		}
		p.Close()
		role := "follower"
		if r.IsLeader {
			role = "leader"
		}
		fmt.Fprintf(w, "%d\t%s\t%s\t%d\t%d\t%d\t%d B\n", r.ID, addr, role, r.Term, r.LastApplied, r.Keys, r.RaftStateSize)
	}
	return w.Flush()
}
