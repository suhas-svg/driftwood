package raft

import (
	"sync"
	"testing"
	"time"

	"github.com/suhas-svg/driftwood/sim"
)

// cluster is an n-node Raft cluster on a simulated network.
type cluster struct {
	t          *testing.T
	mu         sync.Mutex
	n          int
	net        *sim.Network
	nodes      []*Raft
	persisters []*Persister
	connected  []bool
	applyChs   []chan ApplyMsg
	start      time.Time
}

func newCluster(t *testing.T, n int, reliable bool) *cluster {
	t.Helper()
	c := &cluster{
		t:          t,
		n:          n,
		net:        sim.New(time.Now().UnixNano()),
		nodes:      make([]*Raft, n),
		persisters: make([]*Persister, n),
		connected:  make([]bool, n),
		applyChs:   make([]chan ApplyMsg, n),
		start:      time.Now(),
	}
	if !reliable {
		c.net.SetDropRate(0.1)
		c.net.SetMaxDelay(25 * time.Millisecond)
	}
	for i := 0; i < n; i++ {
		c.persisters[i] = NewPersister()
		c.startNode(i)
		c.connect(i)
	}
	t.Cleanup(c.cleanup)
	return c
}

// startNode (re)starts node i from whatever its persister holds.
func (c *cluster) startNode(i int) {
	c.crash(i)

	c.mu.Lock()
	c.persisters[i] = c.persisters[i].Copy()
	persister := c.persisters[i]
	applyCh := make(chan ApplyMsg, 1000)
	c.applyChs[i] = applyCh
	c.mu.Unlock()

	peers := make([]Peer, c.n)
	for j := range peers {
		peers[j] = c.net.End(i, j)
	}
	rf := Make(peers, i, persister, applyCh)

	c.mu.Lock()
	c.nodes[i] = rf
	c.mu.Unlock()
	c.net.Register(i, rf)

	go c.drain(applyCh)
}

// crash kills node i. Its persister survives for a later startNode.
func (c *cluster) crash(i int) {
	c.mu.Lock()
	rf := c.nodes[i]
	c.nodes[i] = nil
	c.mu.Unlock()
	c.net.Unregister(i)
	if rf != nil {
		rf.Kill()
	}
}

// drain discards applied messages for now. Week 3 replaces this with a checker
// that every node applies the same command at each index.
func (c *cluster) drain(ch chan ApplyMsg) {
	for range ch {
	}
}

func (c *cluster) connect(i int) {
	c.mu.Lock()
	c.connected[i] = true
	c.mu.Unlock()
	c.net.Connect(i)
}

func (c *cluster) disconnect(i int) {
	c.mu.Lock()
	c.connected[i] = false
	c.mu.Unlock()
	c.net.Disconnect(i)
}

func (c *cluster) cleanup() {
	for i := 0; i < c.n; i++ {
		c.crash(i)
	}
}

// checkOneLeader waits for exactly one leader among the connected nodes and
// returns its id. It fails the test if two nodes lead in the same term.
func (c *cluster) checkOneLeader() int {
	c.t.Helper()
	for attempt := 0; attempt < 10; attempt++ {
		time.Sleep(time.Duration(450+attempt*50) * time.Millisecond)

		leaders := map[int][]int{} // term -> leader ids
		for i := 0; i < c.n; i++ {
			if rf := c.node(i); rf != nil && c.isConnected(i) {
				if term, isLeader := rf.GetState(); isLeader {
					leaders[term] = append(leaders[term], i)
				}
			}
		}

		latest := -1
		for term, ids := range leaders {
			if len(ids) > 1 {
				c.t.Fatalf("term %d has %d leaders: %v", term, len(ids), ids)
			}
			if term > latest {
				latest = term
			}
		}
		if latest != -1 {
			return leaders[latest][0]
		}
	}
	c.t.Fatal("expected one leader, got none")
	return -1
}

// checkTerms returns the term all connected nodes agree on, failing if they disagree.
func (c *cluster) checkTerms() int {
	c.t.Helper()
	term := -1
	for i := 0; i < c.n; i++ {
		if rf := c.node(i); rf != nil && c.isConnected(i) {
			t, _ := rf.GetState()
			if term == -1 {
				term = t
			} else if t != term {
				c.t.Fatalf("nodes disagree on term: %d vs %d", term, t)
			}
		}
	}
	return term
}

// checkNoLeader fails if any connected node thinks it is the leader.
func (c *cluster) checkNoLeader() {
	c.t.Helper()
	for i := 0; i < c.n; i++ {
		if rf := c.node(i); rf != nil && c.isConnected(i) {
			if _, isLeader := rf.GetState(); isLeader {
				c.t.Fatalf("node %d is leader, expected no leader", i)
			}
		}
	}
}

func (c *cluster) node(i int) *Raft {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[i]
}

func (c *cluster) isConnected(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected[i]
}
