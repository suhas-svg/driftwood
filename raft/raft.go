// Package raft implements the Raft consensus algorithm as described in
// "In Search of an Understandable Consensus Algorithm" (Ongaro & Ousterhout).
// Figure 2 of the paper is the spec; the comments below point back to it.
package raft

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Peer is how a node talks to another node. sim.End implements it in tests;
// a TCP or gRPC transport can implement it later.
//
// Call returns false if the request or reply was lost. Always pass a fresh,
// zero-valued reply: the transport only overwrites fields the remote set.
type Peer interface {
	Call(method string, args, reply any) bool
}

// ApplyMsg is sent on the apply channel once an entry is committed, so the
// service layered on top (the KV store) can apply it.
type ApplyMsg struct {
	CommandValid bool
	Command      any
	CommandIndex int
	CommandTerm  int

	// Used once snapshots are implemented.
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	return [...]string{"Follower", "Candidate", "Leader"}[s]
}

type LogEntry struct {
	Term    int
	Command any
}

const (
	heartbeatInterval  = 100 * time.Millisecond
	electionTimeoutMin = 300 * time.Millisecond
	electionTimeoutMax = 600 * time.Millisecond
	tickInterval       = 10 * time.Millisecond
)

// Raft is a single node of a Raft cluster.
type Raft struct {
	mu        sync.Mutex
	peers     []Peer // peers[me] is this node; never call it
	me        int
	persister *Persister
	applyCh   chan<- ApplyMsg
	dead      atomic.Bool

	// Persistent state on all servers (Figure 2). Call persist() after
	// changing any of these and before replying to the RPC that changed them.
	currentTerm int
	votedFor    int        // -1 when no vote cast in currentTerm
	log         []LogEntry // log[0] is a sentinel with Term 0; real entries start at index 1

	// Volatile state on all servers.
	commitIndex int
	lastApplied int

	// Volatile state on leaders, reinitialized after each election.
	nextIndex  []int
	matchIndex []int

	state         State
	electionReset time.Time     // last time we heard from a valid leader or granted a vote
	electionDelay time.Duration // randomized timeout for the current wait
}

// Make creates a Raft node. peers[me] is this node. persister holds any state
// saved before a crash. Committed entries are delivered on applyCh in order.
// Make must return quickly; long-running work belongs in goroutines.
func Make(peers []Peer, me int, persister *Persister, applyCh chan<- ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		me:        me,
		persister: persister,
		applyCh:   applyCh,
		votedFor:  -1,
		log:       []LogEntry{{Term: 0}},
		state:     Follower,
	}
	rf.readPersist(persister.ReadRaftState())
	rf.resetElectionTimer()

	go rf.ticker()
	return rf
}

// GetState returns the current term and whether this node believes it is the leader.
func (rf *Raft) GetState() (term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// Start asks Raft to append command to the replicated log. It returns
// immediately, with no guarantee the command will ever commit: the leader
// may crash or lose an election. The returned index is where the command
// will appear if it does commit.
func (rf *Raft) Start(command any) (index, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}
	// TODO(week 3): append to the log, persist, and kick off replication.
	return -1, rf.currentTerm, true
}

// Kill stops the node. Long-running goroutines should check killed() and exit.
func (rf *Raft) Kill() { rf.dead.Store(true) }

func (rf *Raft) killed() bool { return rf.dead.Load() }

// ticker starts an election whenever the election timeout passes without
// word from a leader.
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader && time.Since(rf.electionReset) >= rf.electionDelay {
			rf.startElection()
		}
		rf.mu.Unlock()
		time.Sleep(tickInterval)
	}
}

// resetElectionTimer restarts the election countdown with a fresh random
// timeout. Per the paper, call it only when you grant a vote, receive a valid
// AppendEntries from the current leader, or start an election. Caller holds rf.mu.
func (rf *Raft) resetElectionTimer() {
	rf.electionReset = time.Now()
	spread := int64(electionTimeoutMax - electionTimeoutMin)
	rf.electionDelay = electionTimeoutMin + time.Duration(rand.Int63n(spread))
}

// startElection turns this node into a candidate and asks for votes.
// Caller holds rf.mu.
func (rf *Raft) startElection() {
	// TODO(week 2), from Figure 2 "Rules for Servers: Candidates":
	//   1. Increment currentTerm, become Candidate, vote for self, persist.
	//   2. Reset the election timer.
	//   3. Send RequestVote to every other peer in its own goroutine.
	//      Do NOT hold rf.mu while calling peers[i].Call.
	//   4. When a reply comes back, re-lock and check that you are still a
	//      Candidate in the same term before counting it. Stale replies happen.
	//   5. If a reply carries a higher term, step down to Follower.
	//   6. With votes from a majority (len(peers)/2 + 1), become Leader,
	//      initialize nextIndex/matchIndex, and start sending heartbeats.
	rf.resetElectionTimer()
}

// becomeFollower steps down when we see a higher term. Caller holds rf.mu.
func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) lastLogIndex() int { return len(rf.log) - 1 }
func (rf *Raft) lastLogTerm() int  { return rf.log[len(rf.log)-1].Term }
