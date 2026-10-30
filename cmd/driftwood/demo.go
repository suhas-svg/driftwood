package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/suhas-svg/driftwood/kv"
	"github.com/suhas-svg/driftwood/transport"
)

// demo starts a local cluster as separate processes, keeps clients writing,
// hard-kills the leader, and later restarts it. At the end it reads back
// every client's writes to check that none were lost or applied twice.
func demo(args []string) error {
	fs := flag.NewFlagSet("demo", flag.ExitOnError)
	n := fs.Int("n", 5, "number of servers")
	basePort := fs.Int("port", 7101, "first server port; servers use port, port+1, ...")
	clients := fs.Int("clients", 4, "concurrent writers")
	killAt := fs.Duration("kill", 4*time.Second, "when to kill the leader")
	restartAt := fs.Duration("restart", 10*time.Second, "when to restart it")
	total := fs.Duration("duration", 15*time.Second, "how long to write")
	noSync := fs.Bool("nosync", false, "skip fsync on the servers")
	fs.Parse(args)

	self, err := os.Executable()
	if err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "driftwood-demo-")
	if err != nil {
		return err
	}
	addrs := make([]string, *n)
	for i := range addrs {
		addrs[i] = fmt.Sprintf("127.0.0.1:%d", *basePort+i)
	}
	peers := strings.Join(addrs, ",")

	procs := make([]*exec.Cmd, *n)
	exited := make([]chan struct{}, *n) // closed when that process exits
	start := func(i int) error {
		log, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("server-%d.log", i)), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		args := []string{"serve", "-id", fmt.Sprint(i), "-peers", peers,
			"-data", filepath.Join(dir, fmt.Sprint(i)), "-maxraftstate", "16384"}
		if *noSync {
			args = append(args, "-nosync")
		}
		cmd := exec.Command(self, args...)
		cmd.Stdout, cmd.Stderr = log, log
		if err := cmd.Start(); err != nil {
			return err
		}
		procs[i] = cmd
		done := make(chan struct{})
		exited[i] = done
		go func() { cmd.Wait(); log.Close(); close(done) }()
		return nil
	}
	defer func() {
		for _, p := range procs {
			if p != nil && p.Process != nil {
				p.Process.Kill()
			}
		}
	}()

	fmt.Printf("starting %d servers on %s ... (logs and data in %s)\n", *n, peers, dir)
	for i := range procs {
		if err := start(i); err != nil {
			return err
		}
	}
	if leaderOf(addrs, 10*time.Second) == -1 {
		return fmt.Errorf("no leader after 10s; is something else using ports %d-%d? see logs in %s",
			*basePort, *basePort+*n-1, dir)
	}

	// Writers: each appends numbered tokens to its own key and remembers
	// what it expects that key to hold.
	var (
		writes  atomic.Int64
		gapMu   sync.Mutex
		lastOK  = time.Now()
		maxGap  time.Duration
		stop    = make(chan struct{})
		wg      sync.WaitGroup
		expects = make([]strings.Builder, *clients)
	)
	for c := range *clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ck := kv.MakeClerk(transport.Peers(addrs, "KV", transport.ClientTimeout))
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				token := fmt.Sprintf("%d.%d;", c, i)
				ck.Append(fmt.Sprintf("demo-%d", c), token)
				expects[c].WriteString(token)
				writes.Add(1)
				gapMu.Lock()
				if gap := time.Since(lastOK); gap > maxGap {
					maxGap = gap
				}
				lastOK = time.Now()
				gapMu.Unlock()
			}
		}()
	}

	began := time.Now()
	killed := -1
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	prev := int64(0)
	for range tick.C {
		elapsed := time.Since(began)
		if killed == -1 && elapsed >= *killAt {
			if leader := leaderOf(addrs, 0); leader != -1 {
				procs[leader].Process.Kill()
				killed = leader
				fmt.Printf("\n*** %5.1fs  hard-killed leader, server %d (pid %d)\n\n", elapsed.Seconds(), leader, procs[leader].Process.Pid)
			}
		}
		if killed != -1 && elapsed >= *restartAt && isClosed(exited[killed]) {
			if err := start(killed); err != nil {
				return err
			}
			fmt.Printf("\n*** %5.1fs  restarted server %d; it recovers from disk and catches up\n\n", elapsed.Seconds(), killed)
		}

		now := writes.Load()
		leader := "-"
		if l := leaderOf(addrs, 0); l != -1 {
			leader = fmt.Sprint(l)
		}
		fmt.Printf("  %5.1fs  %5d writes/s  %7d total  leader %s\n", elapsed.Seconds(), (now-prev)*2, now, leader)
		prev = now
		if elapsed >= *total {
			break
		}
	}

	close(stop)
	wg.Wait()

	// Read every key back through the cluster and compare.
	ck := kv.MakeClerk(transport.Peers(addrs, "KV", transport.ClientTimeout))
	for c := range *clients {
		if got, want := ck.Get(fmt.Sprintf("demo-%d", c)), expects[c].String(); got != want {
			return fmt.Errorf("client %d: key holds %d bytes, expected %d: writes were lost or duplicated", c, len(got), len(want))
		}
	}
	fmt.Printf("\n%d writes acknowledged; read back all %d keys: every write present, exactly once, in order.\n", writes.Load(), *clients)
	fmt.Printf("longest pause between acknowledged writes: %v (leader failure, election, client retry)\n", maxGap.Round(time.Millisecond))
	return nil
}

func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// leaderOf returns the index of a server that reports being leader, polling
// for up to wait. It returns -1 if none does.
func leaderOf(addrs []string, wait time.Duration) int {
	deadline := time.Now().Add(wait)
	for {
		best, bestTerm := -1, -1
		for i, a := range addrs {
			var r kv.StatusReply
			p := transport.NewPeer(a, "KV", 200*time.Millisecond)
			if p.Call("Status", &kv.StatusArgs{}, &r) && r.IsLeader && r.Term > bestTerm {
				best, bestTerm = i, r.Term
			}
			p.Close()
		}
		if best != -1 || time.Now().After(deadline) {
			return best
		}
		time.Sleep(100 * time.Millisecond)
	}
}
