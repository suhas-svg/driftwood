package raft

import (
	"fmt"
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
	applied    []map[int]any // per node: log index -> command it applied there
	applyErr   string        // first safety violation seen by the apply checker
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
		applied:    make([]map[int]any, n),
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
	c.applied[i] = map[int]any{} // a restarted node replays its log from the start
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

	go c.checkApplies(i, applyCh)
}

// crash kills node i. Its persister survives for a later startNode.
func (c *cluster) crash(i int) {
	c.mu.Lock()
	rf := c.nodes[i]
	c.nodes[i] = nil
	c.applyChs[i] = nil // ignore anything the dying node still applies
	c.mu.Unlock()
	c.net.Unregister(i)
	if rf == nil {
		return
	}
	rf.Kill()

	// Freeze the disk as of the crash. Goroutines of the killed node can
	// still be running and calling persist; those writes go to the old
	// Persister and must not leak into the next incarnation.
	c.mu.Lock()
	c.persisters[i] = c.persisters[i].Copy()
	c.mu.Unlock()
}

// restart crashes node i if it is running and starts it again from its disk.
func (c *cluster) restart(i int) { c.startNode(i) }

func (c *cluster) running(i int) bool { return c.node(i) != nil }

// checkApplies records what node i applies and checks the two safety rules
// the service depends on: entries arrive in index order with no gaps, and no
// two nodes ever apply different commands at the same index.
func (c *cluster) checkApplies(i int, ch chan ApplyMsg) {
	for m := range ch {
		if !m.CommandValid {
			continue
		}
		c.mu.Lock()
		if c.applyChs[i] != ch {
			c.mu.Unlock()
			continue // from a crashed incarnation of this node
		}
		if err := c.checkApplyLocked(i, m); err != "" && c.applyErr == "" {
			c.applyErr = err
		}
		c.mu.Unlock()
	}
}

func (c *cluster) checkApplyLocked(i int, m ApplyMsg) string {
	idx := m.CommandIndex
	if idx > 1 {
		if _, ok := c.applied[i][idx-1]; !ok {
			return fmt.Sprintf("node %d applied index %d before index %d", i, idx, idx-1)
		}
	}
	if _, ok := c.applied[i][idx]; ok {
		return fmt.Sprintf("node %d applied index %d twice", i, idx)
	}
	for j, log := range c.applied {
		if other, ok := log[idx]; ok && other != m.Command {
			return fmt.Sprintf("index %d: node %d applied %v but node %d applied %v",
				idx, i, m.Command, j, other)
		}
	}
	c.applied[i][idx] = m.Command
	return ""
}

// failIfUnsafe stops the test if the apply checker saw a violation.
func (c *cluster) failIfUnsafe() {
	c.t.Helper()
	c.mu.Lock()
	err := c.applyErr
	c.mu.Unlock()
	if err != "" {
		c.t.Fatal(err)
	}
}

// nCommitted returns how many nodes have applied index, and the command there.
func (c *cluster) nCommitted(index int) (int, any) {
	c.t.Helper()
	c.failIfUnsafe()
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	var cmd any
	for _, log := range c.applied {
		if v, ok := log[index]; ok {
			count++
			cmd = v
		}
	}
	return count, cmd
}

// one submits cmd and waits until at least expected nodes have applied it,
// returning its index. It keeps looking for a leader for up to 10 seconds.
// With retry, it resubmits if the first attempt does not commit, which is
// what a real client does after a timeout.
func (c *cluster) one(cmd any, expected int, retry bool) int {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	starts := 0
	for time.Now().Before(deadline) {
		index := -1
		for k := 0; k < c.n; k++ {
			starts = (starts + 1) % c.n
			if rf := c.node(starts); rf != nil && c.isConnected(starts) {
				if i, _, ok := rf.Start(cmd); ok {
					index = i
					break
				}
			}
		}

		if index != -1 {
			// Someone claimed leadership. Give the entry time to commit.
			for wait := time.Now(); time.Since(wait) < 2*time.Second; {
				if n, got := c.nCommitted(index); n >= expected && got == cmd {
					return index
				}
				time.Sleep(20 * time.Millisecond)
			}
			if !retry {
				c.t.Fatalf("one(%v) failed to reach agreement", cmd)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	c.t.Fatalf("one(%v) failed to reach agreement within 10s", cmd)
	return -1
}

// wait blocks until n nodes have applied index, and returns the command.
// It gives up early, returning nil, if startTerm is set and some node moves
// past it, since the entry may then never commit.
func (c *cluster) wait(index, n, startTerm int) any {
	c.t.Helper()
	sleep := 10 * time.Millisecond
	for iter := 0; iter < 30; iter++ {
		if got, _ := c.nCommitted(index); got >= n {
			break
		}
		time.Sleep(sleep)
		sleep = min(2*sleep, time.Second)
		if startTerm > -1 {
			for i := 0; i < c.n; i++ {
				if rf := c.node(i); rf != nil {
					if t, _ := rf.GetState(); t > startTerm {
						return nil
					}
				}
			}
		}
	}
	got, cmd := c.nCommitted(index)
	if got < n {
		c.t.Fatalf("only %d nodes committed index %d, wanted %d", got, index, n)
	}
	return cmd
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
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.applyErr != "" {
		c.t.Error(c.applyErr)
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
