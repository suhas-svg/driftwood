package kv

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/suhas-svg/driftwood/sim"
)

// Week 5 tests. Each one records every client operation and checks the whole
// history with Porcupine, so "the test passed" means "every Get returned a
// value some linearizable execution could have returned".
//
//	go test ./kv -count=50

const electionWait = time.Second

func TestKVBasic(t *testing.T) {
	c := newCluster(t, 3, true)
	ck := c.makeClerk()

	if got := ck.Get("a"); got != "" {
		t.Fatalf("Get of missing key = %q, want \"\"", got)
	}
	ck.Put("a", "1")
	if got := ck.Get("a"); got != "1" {
		t.Fatalf("Get = %q, want 1", got)
	}
	ck.Append("a", "2")
	ck.Append("a", "3")
	if got := ck.Get("a"); got != "123" {
		t.Fatalf("Get = %q, want 123", got)
	}
	ck.Put("a", "x")
	if got := ck.Get("a"); got != "x" {
		t.Fatalf("Get = %q, want x", got)
	}
	c.checkLinearizable()
}

func TestKVConcurrentClients(t *testing.T) {
	c := newCluster(t, 5, true)
	clerks := make([]*clerk, 5)
	for i := range clerks {
		clerks[i] = c.makeClerk()
	}

	stop := make(chan struct{})
	time.AfterFunc(3*time.Second, func() { close(stop) })
	c.workload(clerks, stop)
	c.checkLinearizable()
}

// With 10% of messages lost, clients time out and retry constantly. Every
// lost reply means a retry of a request that already committed, so this is
// mainly a test of duplicate detection.
func TestKVUnreliable(t *testing.T) {
	c := newCluster(t, 5, false)
	clerks := make([]*clerk, 5)
	for i := range clerks {
		clerks[i] = c.makeClerk()
	}

	stop := make(chan struct{})
	time.AfterFunc(5*time.Second, func() { close(stop) })
	c.workload(clerks, stop)
	c.checkLinearizable()
}

// An old leader cut off in a minority partition still believes it leads. It
// must not answer a Get from its stale state.
func TestKVStaleLeaderRead(t *testing.T) {
	c := newCluster(t, 5, true)
	ck := c.makeClerk()
	ck.Put("x", "1")

	leader := c.leader()
	if leader == -1 {
		t.Fatal("no leader after a successful Put")
	}
	fresh := c.makeClerk()
	stale := c.makeClerk()
	minority := []sim.NodeID{leader, (leader + 1) % 5, stale.node}
	majority := []sim.NodeID{(leader + 2) % 5, (leader + 3) % 5, (leader + 4) % 5, fresh.node, ck.node}
	c.partition(minority, majority)

	// The majority elects a new leader and moves on.
	fresh.Put("x", "2")

	// This Get starts after Put("x", "2") returned, so the only correct
	// answer is "2". The minority can only offer "1", so it must not answer.
	got := make(chan string, 1)
	go func() { got <- stale.Get("x") }()
	select {
	case v := <-got:
		t.Fatalf("minority served Get(x) = %q while partitioned", v)
	case <-time.After(2 * time.Second):
	}

	c.heal()
	select {
	case v := <-got:
		if v != "2" {
			t.Fatalf("Get(x) = %q after heal, want 2", v)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Get(x) never returned after heal")
	}
	c.checkLinearizable()
}

// A leader alone in a partition keeps accepting a client's Append, so its log
// fills with entries that can never commit. When the partition heals, the new
// leader overwrites them, and the old leader applies someone else's entries at
// the indexes its handlers are waiting on. Those handlers must not report
// success: the client's Append never committed.
func TestKVOverwrittenEntryNotAcked(t *testing.T) {
	c := newCluster(t, 5, true)
	setup := c.makeClerk()
	setup.Put("k", "")

	for attempt := 0; attempt < 3; attempt++ {
		leader := c.leader()
		if leader == -1 {
			time.Sleep(electionWait)
			continue
		}
		lonely := c.makeClerk()
		fresh := c.makeClerk()
		others := []sim.NodeID{fresh.node, setup.node}
		for k := 1; k < 5; k++ {
			others = append(others, (leader+k)%5)
		}
		c.partition([]sim.NodeID{leader, lonely.node}, others)

		// The lonely clerk keeps retrying against the old leader, which keeps
		// appending its request at new indexes while handlers wait on them.
		done := make(chan struct{})
		token := fmt.Sprintf("lost%d ", attempt)
		go func() { lonely.Append("k", token); close(done) }()

		// The majority fills the same indexes with its own entries.
		for i := 0; i < 40; i++ {
			fresh.Put(fmt.Sprintf("filler%d", i%4), "x")
		}

		c.heal()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("Append never returned after heal")
		}
		// Append returned, so it must be in the store.
		if got := fresh.Get("k"); !strings.Contains(got, token) {
			t.Fatalf("Append(%q) returned but k = %q", token, got)
		}
	}
	c.checkLinearizable()
}

// Servers crash and restart while clients keep writing. A restarted server
// rebuilds its map and dedup table by replaying the Raft log.
func TestKVCrashRestart(t *testing.T) {
	c := newCluster(t, 5, true)
	clerks := make([]*clerk, 4)
	for i := range clerks {
		clerks[i] = c.makeClerk()
	}

	stop := make(chan struct{})
	go func() {
		defer close(stop)
		for iter := 0; iter < 8; iter++ {
			time.Sleep(time.Duration(300+rand.Intn(300)) * time.Millisecond)
			c.crash(rand.Intn(5))
			c.crash(rand.Intn(5)) // sometimes the same one; at most 2 down
			time.Sleep(time.Duration(200+rand.Intn(300)) * time.Millisecond)
			for i := 0; i < 5; i++ {
				if !c.running(i) {
					c.startServer(i)
				}
			}
		}
	}()
	c.workload(clerks, stop)
	c.checkLinearizable()
}

// Random partitions of both servers and clients, with a lossy network. Clerks
// stranded in a minority block until the partition heals.
func TestKVPartitionsLinearizable(t *testing.T) {
	c := newCluster(t, 5, false)
	clerks := make([]*clerk, 5)
	for i := range clerks {
		clerks[i] = c.makeClerk()
	}

	stop := make(chan struct{})
	go func() {
		for iter := 0; iter < 8; iter++ {
			time.Sleep(time.Duration(200+rand.Intn(400)) * time.Millisecond)
			nodes := rand.Perm(5)
			var a, b []sim.NodeID
			for _, s := range nodes[:2] {
				a = append(a, s)
			}
			for _, s := range nodes[2:] {
				b = append(b, s)
			}
			for _, ck := range clerks {
				if rand.Intn(2) == 0 {
					a = append(a, ck.node)
				} else {
					b = append(b, ck.node)
				}
			}
			c.partition(a, b)
			time.Sleep(time.Duration(300+rand.Intn(500)) * time.Millisecond)
			c.heal()
		}
		close(stop)
	}()
	c.workload(clerks, stop)
	c.checkLinearizable()
}

// Partitions and crashes together, which is where persistence, dedup, and
// leader changes all interact.
func TestKVPartitionsAndCrashes(t *testing.T) {
	c := newCluster(t, 5, false)
	clerks := make([]*clerk, 4)
	for i := range clerks {
		clerks[i] = c.makeClerk()
	}

	stop := make(chan struct{})
	go func() {
		for iter := 0; iter < 8; iter++ {
			time.Sleep(time.Duration(200+rand.Intn(400)) * time.Millisecond)
			if rand.Intn(2) == 0 {
				nodes := rand.Perm(5)
				majority := []sim.NodeID{nodes[2], nodes[3], nodes[4]}
				for _, ck := range clerks {
					majority = append(majority, ck.node)
				}
				c.partition([]sim.NodeID{nodes[0], nodes[1]}, majority)
			} else {
				c.crash(rand.Intn(5))
			}
			time.Sleep(time.Duration(300+rand.Intn(400)) * time.Millisecond)
			c.heal()
			for i := 0; i < 5; i++ {
				if !c.running(i) {
					c.startServer(i)
				}
			}
		}
		close(stop)
	}()
	c.workload(clerks, stop)
	c.checkLinearizable()
}
