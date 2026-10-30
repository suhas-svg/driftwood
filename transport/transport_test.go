package transport

import (
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/suhas-svg/driftwood/kv"
)

// These tests run real nodes in one process: each listens on its own TCP
// port on localhost and keeps its state in a file, so a stopped node comes
// back only with what it actually wrote to disk.

type tcpCluster struct {
	t     *testing.T
	cfgs  []NodeConfig
	nodes []*Node
}

func newTCPCluster(t *testing.T, n int) *tcpCluster {
	t.Helper()
	addrs := make([]string, n)
	for i := range addrs {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addrs[i] = ln.Addr().String()
		ln.Close()
	}
	c := &tcpCluster{t: t, nodes: make([]*Node, n)}
	dir := t.TempDir()
	for i := range addrs {
		c.cfgs = append(c.cfgs, NodeConfig{
			ID: i, Addrs: addrs, Dir: filepath.Join(dir, fmt.Sprint(i)), MaxRaftState: 4096,
		})
		c.start(i)
	}
	t.Cleanup(func() {
		for i := range c.nodes {
			c.stop(i)
		}
	})
	return c
}

func (c *tcpCluster) start(i int) {
	c.t.Helper()
	// The port may take a moment to free up after the previous node closed it.
	var err error
	for attempt := 0; attempt < 20; attempt++ {
		if c.nodes[i], err = StartNode(c.cfgs[i]); err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatalf("starting node %d: %v", i, err)
}

func (c *tcpCluster) stop(i int) {
	if c.nodes[i] != nil {
		c.nodes[i].Stop()
		c.nodes[i] = nil
	}
}

func (c *tcpCluster) clerk() *kv.Clerk {
	return kv.MakeClerk(Peers(c.cfgs[0].Addrs, "KV", ClientTimeout))
}

func (c *tcpCluster) leader() int {
	for attempt := 0; attempt < 50; attempt++ {
		for i, n := range c.nodes {
			if n == nil {
				continue
			}
			var r kv.StatusReply
			n.KV.Status(&kv.StatusArgs{}, &r)
			if r.IsLeader {
				return i
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatal("no leader after 5s")
	return -1
}

// within runs f and fails the test if it does not finish in time. Clerks
// retry forever, so a broken cluster would otherwise hang the test.
func within(t *testing.T, d time.Duration, what string, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { f(); close(done) }()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("%s did not finish within %v", what, d)
	}
}

func TestTCPBasic(t *testing.T) {
	c := newTCPCluster(t, 3)
	ck := c.clerk()
	within(t, 10*time.Second, "Put/Append/Get", func() {
		ck.Put("a", "1")
		ck.Append("a", "2")
		if got := ck.Get("a"); got != "12" {
			t.Errorf("Get(a) = %q, want 12", got)
		}
	})
}

// Kill the leader of a 3-node TCP cluster mid-workload. Writes continue
// on the other two. The old leader then restarts from its state file, and
// a different node is stopped, so the restarted node is needed for every
// commit: if it lost data on disk, the reads below would show it.
func TestTCPLeaderKillAndRecovery(t *testing.T) {
	c := newTCPCluster(t, 3)
	ck := c.clerk()

	var want strings.Builder
	write := func(n int) {
		for i := 0; i < n; i++ {
			v := fmt.Sprintf("%d;", want.Len())
			ck.Append("log", v)
			want.WriteString(v)
		}
	}
	within(t, 10*time.Second, "initial writes", func() { write(100) })

	leader := c.leader()
	c.stop(leader)
	within(t, 10*time.Second, "writes after leader kill", func() { write(100) })

	c.start(leader)
	other := (leader + 1) % 3
	c.stop(other)
	within(t, 15*time.Second, "writes needing the restarted node", func() {
		write(50)
		if got := ck.Get("log"); got != want.String() {
			t.Errorf("Get(log) has %d bytes, want %d", len(got), want.Len())
		}
	})
	c.start(other)
}

// Every node stops and restarts. Everything acknowledged before must still
// be there, recovered from the state files alone.
func TestTCPFullRestart(t *testing.T) {
	c := newTCPCluster(t, 3)
	ck := c.clerk()
	within(t, 10*time.Second, "writes", func() {
		for i := 0; i < 200; i++ {
			ck.Put(fmt.Sprintf("k%d", i), fmt.Sprint(i))
		}
	})

	for i := range c.nodes {
		c.stop(i)
	}
	for i := range c.nodes {
		c.start(i)
	}

	ck = c.clerk()
	within(t, 15*time.Second, "reads after full restart", func() {
		for i := 0; i < 200; i += 17 {
			if got := ck.Get(fmt.Sprintf("k%d", i)); got != fmt.Sprint(i) {
				t.Errorf("Get(k%d) = %q after restart, want %d", i, got, i)
			}
		}
	})
}
