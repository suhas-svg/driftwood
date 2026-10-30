package raft

import (
	"math/rand"
	"testing"
	"time"
)

// Week 2 tests. Run them many times, not once:
//
//	go test ./raft -run Election -race -count=100

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 3, true)

	c.checkOneLeader()

	// All nodes should have caught up to the leader's term.
	time.Sleep(50 * time.Millisecond)
	term1 := c.checkTerms()
	if term1 < 1 {
		t.Fatalf("term is %d, expected at least 1", term1)
	}

	// With no failures, the leader should keep its job and the term should not move.
	time.Sleep(2 * electionTimeoutMax)
	if term2 := c.checkTerms(); term1 != term2 {
		t.Logf("warning: term changed from %d to %d with no failures", term1, term2)
	}

	c.checkOneLeader()
}

// A one-node cluster is its own majority. It never gets a vote reply, so it
// must notice it has won without one.
func TestSingleNodeCluster(t *testing.T) {
	c := newCluster(t, 1, true)
	c.checkOneLeader()
	for i := 1; i <= 5; i++ {
		c.one(i*10, 1, false)
	}
}

func TestReElection(t *testing.T) {
	c := newCluster(t, 3, true)

	leader1 := c.checkOneLeader()

	// The remaining two still form a majority.
	c.disconnect(leader1)
	c.checkOneLeader()

	// The old leader rejoins. It must not disturb the new one.
	c.connect(leader1)
	leader2 := c.checkOneLeader()

	// Without a majority there must be no leader.
	c.disconnect(leader2)
	c.disconnect((leader2 + 1) % 3)
	time.Sleep(2 * electionTimeoutMax)
	c.checkNoLeader()

	// A majority again: elect a leader.
	c.connect((leader2 + 1) % 3)
	c.checkOneLeader()

	// Everyone back.
	c.connect(leader2)
	c.checkOneLeader()
}

func TestManyElections(t *testing.T) {
	const n = 7
	c := newCluster(t, n, true)

	c.checkOneLeader()

	for iter := 0; iter < 10; iter++ {
		// Disconnect three random nodes; four remain, still a majority.
		picks := rand.Perm(n)[:3]
		for _, i := range picks {
			c.disconnect(i)
		}
		c.checkOneLeader()
		for _, i := range picks {
			c.connect(i)
		}
	}

	c.checkOneLeader()
}

func TestElectionUnderPartition(t *testing.T) {
	c := newCluster(t, 5, true)
	leader := c.checkOneLeader()

	// Cut off two followers. They will time out and campaign, but two of five
	// is not a majority, so neither may win. (A cut-off *leader* would keep
	// believing it leads until it hears a higher term; that is expected.)
	minority := []int{(leader + 1) % 5, (leader + 2) % 5}
	majority := []int{leader, (leader + 3) % 5, (leader + 4) % 5}
	c.net.Partition(minority, majority)
	time.Sleep(2 * electionTimeoutMax)

	for _, i := range minority {
		if _, isLeader := c.node(i).GetState(); isLeader {
			t.Fatalf("node %d leads the 2-node minority", i)
		}
	}
	c.checkOneLeader()

	c.net.Heal()
	c.checkOneLeader()
}

func TestElectionUnreliableNetwork(t *testing.T) {
	c := newCluster(t, 5, false) // 10% drops plus reordering

	for iter := 0; iter < 5; iter++ {
		leader := c.checkOneLeader()
		c.disconnect(leader)
		c.checkOneLeader()
		c.connect(leader)
	}
}
