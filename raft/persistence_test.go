package raft

import (
	"math/rand"
	"testing"
	"time"
)

// Week 4 tests: nodes crash and restart, keeping only what they persisted.
//
//	go test ./raft -run 'Persist|Figure8|Crash' -count=100

// deadPeer is a peer that never answers, for tests that drive one node by hand.
type deadPeer struct{}

func (deadPeer) Call(string, any, any) bool { return false }

func lonelyNode(t *testing.T, p *Persister) *Raft {
	t.Helper()
	peers := []Peer{deadPeer{}, deadPeer{}, deadPeer{}}
	rf := Make(peers, 0, p, make(chan ApplyMsg, 100))
	t.Cleanup(rf.Kill)
	return rf
}

// A node that votes, crashes, and restarts must remember the vote. Otherwise
// it could vote for a second candidate in the same term and elect two leaders.
func TestPersistVote(t *testing.T) {
	p := NewPersister()
	rf := lonelyNode(t, p)

	var reply RequestVoteReply
	rf.RequestVote(&RequestVoteArgs{Term: 5, CandidateID: 1}, &reply)
	if !reply.VoteGranted {
		t.Fatal("first vote in term 5 was refused")
	}
	rf.Kill()

	rf2 := lonelyNode(t, p.Copy())
	var reply2 RequestVoteReply
	rf2.RequestVote(&RequestVoteArgs{Term: 5, CandidateID: 2}, &reply2)
	if reply2.VoteGranted {
		t.Fatal("restarted node voted twice in term 5")
	}
	if reply2.Term != 5 {
		t.Fatalf("restarted node is in term %d, want 5", reply2.Term)
	}
}

// Entries a follower accepted must survive a restart, or a leader could count
// it toward a majority for an entry it no longer has.
func TestPersistLog(t *testing.T) {
	p := NewPersister()
	rf := lonelyNode(t, p)

	var reply AppendEntriesReply
	rf.AppendEntries(&AppendEntriesArgs{
		Term:     3,
		LeaderID: 1,
		Entries:  []LogEntry{{Term: 2, Command: "a"}, {Term: 3, Command: "b"}},
	}, &reply)
	if !reply.Success {
		t.Fatal("AppendEntries was rejected")
	}
	rf.Kill()

	rf2 := lonelyNode(t, p.Copy())
	rf2.mu.Lock()
	defer rf2.mu.Unlock()
	if rf2.currentTerm < 3 {
		t.Fatalf("term %d after restart, want at least 3", rf2.currentTerm)
	}
	if rf2.lastLogIndex() != 2 || rf2.log[1].Command != "a" || rf2.log[2].Command != "b" {
		t.Fatalf("log after restart is %+v", rf2.log)
	}
}

func TestPersistBasic(t *testing.T) {
	c := newCluster(t, 3, true)

	c.one(11, 3, true)

	// Everyone crashes at once and comes back from disk.
	for i := 0; i < 3; i++ {
		c.restart(i)
	}
	c.one(12, 3, true)

	// The leader crashes and comes back.
	leader1 := c.checkOneLeader()
	c.restart(leader1)
	c.one(13, 3, true)

	// A follower stays down while the others commit, then catches up.
	leader2 := c.checkOneLeader()
	follower := (leader2 + 1) % 3
	c.crash(follower)
	c.one(14, 2, true)
	c.restart(follower)
	c.wait(4, 3, -1)

	c.crash(follower)
	c.one(15, 2, true)
	c.restart(follower)
	c.one(16, 3, true)
}

func TestPersistMoreCrashes(t *testing.T) {
	c := newCluster(t, 5, true)

	index := 1
	for iter := 0; iter < 5; iter++ {
		c.one(10+index, 5, true)
		index++

		// Take down the leader and one follower; three remain.
		leader := c.checkOneLeader()
		c.crash((leader + 1) % 5)
		c.crash((leader + 2) % 5)
		c.one(10+index, 3, true)
		index++

		// Swap which three are up, so the new majority includes nodes that
		// only know about the latest entry from disk.
		c.crash(leader)
		c.crash((leader + 3) % 5)
		c.crash((leader + 4) % 5)
		c.restart((leader + 1) % 5)
		c.restart((leader + 2) % 5)
		time.Sleep(electionTimeoutMax)
		c.restart((leader + 3) % 5)
		c.one(10+index, 3, true)
		index++

		c.restart((leader + 4) % 5)
		c.restart(leader)
	}
	c.one(1000, 5, true)
}

func TestPersistPartitionedCrash(t *testing.T) {
	c := newCluster(t, 3, true)

	c.one(101, 3, true)

	leader := c.checkOneLeader()
	c.disconnect((leader + 2) % 3)
	c.one(102, 2, true)

	// The leader and the remaining follower crash; the one that missed
	// entry 102 comes back, along with the follower that has it.
	c.crash(leader)
	c.crash((leader + 1) % 3)
	c.connect((leader + 2) % 3)
	c.restart((leader + 1) % 3)

	// The node with the shorter log must not win: the election restriction
	// keeps it from erasing 102.
	c.one(103, 2, true)

	c.restart(leader)
	c.one(104, 3, true)
	if n, cmd := c.nCommitted(2); n < 3 || cmd != 102 {
		t.Fatalf("index 2 holds %v on %d nodes, want 102 on all 3", cmd, n)
	}
}

// TestFigure8CommitRule checks the commit rule directly, since the randomized
// TestFigure8 below only rarely hits the exact crash timing that breaks it.
//
// Figure 8 (c): the leader of term 4 holds an entry from term 2 that has
// reached a majority. It must not commit it by counting replicas, because a
// node holding a term-3 entry at that index could still win an election and
// overwrite it. Once the leader's own term-4 entry reaches a majority, both
// commit.
func TestFigure8CommitRule(t *testing.T) {
	peers := []Peer{deadPeer{}, deadPeer{}, deadPeer{}, deadPeer{}, deadPeer{}}
	rf := Make(peers, 0, NewPersister(), make(chan ApplyMsg, 100))
	defer rf.Kill()

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = 4
	rf.log = []LogEntry{{Term: 0}, {Term: 1}, {Term: 2}}
	rf.state = Leader
	rf.commitIndex = 1
	rf.matchIndex = []int{2, 2, 2, 0, 0} // index 2 (term 2) is on 3 of 5 nodes
	rf.nextIndex = []int{3, 3, 3, 1, 1}

	rf.advanceCommitIndex()
	if rf.commitIndex != 1 {
		t.Fatalf("leader in term 4 committed index %d, a term-2 entry, by counting replicas", rf.commitIndex)
	}

	rf.log = append(rf.log, LogEntry{Term: 4})
	rf.matchIndex = []int{3, 3, 3, 0, 0}
	rf.advanceCommitIndex()
	if rf.commitIndex != 3 {
		t.Fatalf("commitIndex is %d once the term-4 entry reached a majority, want 3", rf.commitIndex)
	}
}

// TestFigure8 recreates the scenario from Figure 8 of the paper: leaders crash
// right after replicating an entry to some followers, so entries from old
// terms are left on a majority without being committed. A leader that commits
// an old-term entry just by counting replicas lets a later leader overwrite a
// committed entry, which the apply checker catches.
func TestFigure8(t *testing.T) {
	const n = 5
	c := newCluster(t, n, true)

	c.one(rand.Int(), 1, true)

	up := n
	for iter := 0; iter < 100; iter++ {
		leader := -1
		for i := 0; i < n; i++ {
			if rf := c.node(i); rf != nil {
				if _, _, ok := rf.Start(rand.Int()); ok {
					leader = i
				}
			}
		}

		// Usually crash quickly, before the entry reaches everyone.
		if rand.Intn(1000) < 100 {
			time.Sleep(time.Duration(rand.Intn(int(electionTimeoutMax / 2))))
		} else {
			time.Sleep(time.Duration(rand.Intn(13)) * time.Millisecond)
		}

		if leader != -1 {
			c.crash(leader)
			up--
		}
		if up < 3 {
			if i := rand.Intn(n); !c.running(i) {
				c.restart(i)
				up++
			}
		}
	}

	for i := 0; i < n; i++ {
		if !c.running(i) {
			c.restart(i)
		}
	}
	c.one(rand.Int(), n, true)
}

// TestFigure8Unreliable stresses the same commit rule with a lossy, reordering
// network and many uncommitted entries piling up on partitioned leaders.
func TestFigure8Unreliable(t *testing.T) {
	const n = 5
	c := newCluster(t, n, false)

	c.one(rand.Intn(10000), 1, true)

	connected := n
	for iter := 0; iter < 300; iter++ {
		if iter == 200 {
			c.net.SetMaxDelay(100 * time.Millisecond) // long reordering
		}
		leader := -1
		for i := 0; i < n; i++ {
			if _, _, ok := c.node(i).Start(rand.Intn(10000)); ok && c.isConnected(i) {
				leader = i
			}
		}

		if rand.Intn(1000) < 100 {
			time.Sleep(time.Duration(rand.Intn(int(electionTimeoutMax / 2))))
		} else {
			time.Sleep(time.Duration(rand.Intn(13)) * time.Millisecond)
		}

		if leader != -1 && rand.Intn(1000) < 300 {
			c.disconnect(leader)
			connected--
		}
		if connected < 3 {
			if i := rand.Intn(n); !c.isConnected(i) {
				c.connect(i)
				connected++
			}
		}
	}

	for i := 0; i < n; i++ {
		c.connect(i)
	}
	c.net.SetDropRate(0)
	c.net.SetMaxDelay(0)
	c.one(rand.Intn(10000), n, true)
}

// TestCrashChurn keeps clients writing while nodes crash and restart at
// random, over a lossy network.
func TestCrashChurn(t *testing.T) {
	const n = 5
	c := newCluster(t, n, false)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for v := 1; ; v++ {
			select {
			case <-stop:
				return
			default:
			}
			for i := 0; i < n; i++ {
				if rf := c.node(i); rf != nil {
					rf.Start(v)
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	for iter := 0; iter < 20; iter++ {
		i := rand.Intn(n)
		switch rand.Intn(3) {
		case 0:
			c.crash(i)
		case 1:
			c.restart(i)
		case 2:
			c.disconnect(i)
			time.Sleep(50 * time.Millisecond)
			c.connect(i)
		}
		// Keep a majority alive most of the time so the log grows.
		alive := 0
		for j := 0; j < n; j++ {
			if c.running(j) {
				alive++
			}
		}
		if alive < 3 {
			for j := 0; j < n && alive < 3; j++ {
				if !c.running(j) {
					c.restart(j)
					alive++
				}
			}
		}
		time.Sleep(time.Duration(100+rand.Intn(200)) * time.Millisecond)
	}
	close(stop)
	<-done

	for i := 0; i < n; i++ {
		if !c.running(i) {
			c.restart(i)
		}
		c.connect(i)
	}
	c.net.SetDropRate(0)
	c.net.SetMaxDelay(0)
	c.one(-1, n, true)
}
