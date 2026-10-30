package raft

import (
	"testing"
	"time"
)

// PreVote tests. Without PreVote, a cut-off node bumps its term on every
// election timeout and, when it rejoins, forces a healthy leader to step down.

func TestPreVotePartitionedFollowerDoesNotDisrupt(t *testing.T) {
	c := newCluster(t, 3, true)
	leader := c.checkOneLeader()
	term := c.checkTerms()

	// Cut a follower off for long enough to time out many times.
	f := (leader + 1) % 3
	c.disconnect(f)
	time.Sleep(6 * electionTimeoutMax)
	if got, _ := c.node(f).GetState(); got != term {
		t.Fatalf("cut-off follower moved from term %d to %d; it never had a majority", term, got)
	}

	// When it comes back, the leader keeps its job and nobody changes term.
	c.connect(f)
	time.Sleep(2 * electionTimeoutMax)
	if got := c.checkOneLeader(); got != leader {
		t.Fatalf("leader changed from %d to %d after a follower rejoined", leader, got)
	}
	if got := c.checkTerms(); got != term {
		t.Fatalf("term changed from %d to %d after a follower rejoined", term, got)
	}
	c.one(101, 3, false)
}

// A pre-vote asks a hypothetical question. Granting one must not change the
// receiver's term or vote, or a node with no hope of winning could still
// push everyone's term up.
func TestPreVoteLeavesStateUnchanged(t *testing.T) {
	c := newCluster(t, 3, true)
	leader := c.checkOneLeader()
	c.disconnect(leader)
	time.Sleep(electionTimeoutMax) // the followers stop hearing from it

	f := c.node((leader + 1) % 3)
	f.mu.Lock()
	term, votedFor := f.currentTerm, f.votedFor
	args := RequestVoteArgs{
		Term: term + 5, CandidateID: (leader + 2) % 3,
		LastLogIndex: f.lastLogIndex() + 10, LastLogTerm: f.lastLogTerm() + 1,
	}
	f.mu.Unlock()

	var reply RequestVoteReply
	f.PreVote(&args, &reply)

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.currentTerm != term || f.votedFor != votedFor {
		t.Fatalf("PreVote changed (term, votedFor) from (%d, %d) to (%d, %d)",
			term, votedFor, f.currentTerm, f.votedFor)
	}
}

// While a leader is in charge, nobody grants a pre-vote, even to a node
// whose log is up to date. Otherwise, in a 3-node cluster, one follower that
// missed a few heartbeats could win the other follower's pre-vote and depose
// a working leader.
func TestPreVoteRefusedWhileLeaderActive(t *testing.T) {
	c := newCluster(t, 3, true)
	leader := c.checkOneLeader()
	c.one(1, 3, false)

	for _, i := range []int{leader, (leader + 1) % 3} {
		rf := c.node(i)
		rf.mu.Lock()
		args := RequestVoteArgs{
			Term: rf.currentTerm + 1, CandidateID: (leader + 2) % 3,
			LastLogIndex: rf.lastLogIndex(), LastLogTerm: rf.lastLogTerm(),
		}
		rf.mu.Unlock()

		var reply RequestVoteReply
		rf.PreVote(&args, &reply)
		if reply.VoteGranted {
			t.Fatalf("node %d granted a pre-vote while %d leads", i, leader)
		}
	}

	// Once the follower stops hearing from a leader, the same request
	// succeeds. Cut it off from both others so no new leader contacts it.
	c.disconnect(leader)
	c.disconnect((leader + 2) % 3)
	time.Sleep(electionTimeoutMax)
	rf := c.node((leader + 1) % 3)
	rf.mu.Lock()
	args := RequestVoteArgs{
		Term: rf.currentTerm + 1, CandidateID: (leader + 2) % 3,
		LastLogIndex: rf.lastLogIndex(), LastLogTerm: rf.lastLogTerm(),
	}
	rf.mu.Unlock()
	var reply RequestVoteReply
	rf.PreVote(&args, &reply)
	if !reply.VoteGranted {
		t.Fatal("pre-vote refused after the leader went away")
	}
}
