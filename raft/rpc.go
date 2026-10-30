package raft

import "time"

// RPC argument and reply types. Field names must be exported (capitalized)
// or gob will silently drop them on the wire.

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// Optional fast-backup hints (Students' Guide to Raft, "accelerated log
	// backtracking") so the leader can skip a whole term per round trip.
	ConflictTerm  int
	ConflictIndex int
}

// RequestVote handles a vote request from a candidate.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Figure 2, "RequestVote RPC: Receiver implementation".
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	canVote := rf.votedFor == -1 || rf.votedFor == args.CandidateID
	if canVote && rf.candidateUpToDate(args.LastLogTerm, args.LastLogIndex) {
		rf.votedFor = args.CandidateID
		rf.persist()
		// Reset only when granting. Resetting on every RequestVote would let
		// a candidate with a stale log keep everyone from timing out.
		rf.resetElectionTimer()
		reply.VoteGranted = true
	}
}

// candidateUpToDate reports whether a candidate's log is at least as
// up-to-date as ours (section 5.4.1): the later last term wins, and with
// equal last terms the longer log wins. Caller holds rf.mu.
func (rf *Raft) candidateUpToDate(lastTerm, lastIndex int) bool {
	if lastTerm != rf.lastLogTerm() {
		return lastTerm > rf.lastLogTerm()
	}
	return lastIndex >= rf.lastLogIndex()
}

// AppendEntries handles log replication and heartbeats from the leader.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return // a stale leader; our higher term in the reply makes it step down
	}
	// Same term: this is the one legitimate leader. A candidate in this term
	// has lost the election.
	rf.state = Follower
	rf.resetElectionTimer()
	rf.leaderContact = time.Now()

	// Entries at or before snapIndex are committed and already folded into our
	// snapshot, so they match the leader by definition. Drop that part of a
	// (delayed) request and check consistency at snapIndex instead.
	if args.PrevLogIndex < rf.snapIndex {
		skip := min(rf.snapIndex-args.PrevLogIndex, len(args.Entries))
		args.Entries = args.Entries[skip:]
		args.PrevLogIndex = rf.snapIndex
		args.PrevLogTerm = rf.log[0].Term
	}

	// Consistency check: our log must contain the entry just before the new
	// ones. On failure, tell the leader where to back up to.
	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.ConflictTerm = -1 // our log is too short
		reply.ConflictIndex = rf.lastLogIndex() + 1
		return
	}
	if term := rf.termAt(args.PrevLogIndex); term != args.PrevLogTerm {
		// Point at the first entry of the conflicting term so the leader
		// can skip the whole term.
		first := args.PrevLogIndex
		for first > rf.snapIndex+1 && rf.termAt(first-1) == term {
			first--
		}
		reply.ConflictTerm = term
		reply.ConflictIndex = first
		return
	}

	// Merge the new entries. Truncate only at the first real conflict: an
	// old, delayed AppendEntries whose entries all match must not cut off
	// entries that a newer one already appended.
	changed := false
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx <= rf.lastLogIndex() && rf.termAt(idx) == entry.Term {
			continue
		}
		pos := idx - rf.snapIndex
		rf.log = append(rf.log[:pos:pos], args.Entries[i:]...)
		changed = true
		break
	}
	if changed {
		rf.persist()
	}
	reply.Success = true

	// Commit no further than what this RPC proved we share with the leader;
	// entries past that may still be stale leftovers from an old term.
	lastNew := args.PrevLogIndex + len(args.Entries)
	if commit := min(args.LeaderCommit, lastNew); commit > rf.commitIndex {
		rf.commitIndex = commit
		rf.applyCond.Signal()
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("RequestVote", args, reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("AppendEntries", args, reply)
}
