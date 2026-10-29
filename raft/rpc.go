package raft

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

	// Consistency check: our log must contain the entry just before the new
	// ones. On failure, tell the leader where to back up to.
	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.ConflictTerm = -1 // our log is too short
		reply.ConflictIndex = rf.lastLogIndex() + 1
		return
	}
	if term := rf.log[args.PrevLogIndex].Term; term != args.PrevLogTerm {
		// Point at the first entry of the conflicting term so the leader
		// can skip the whole term.
		first := args.PrevLogIndex
		for first > 1 && rf.log[first-1].Term == term {
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
		if idx <= rf.lastLogIndex() && rf.log[idx].Term == entry.Term {
			continue
		}
		rf.log = append(rf.log[:idx:idx], args.Entries[i:]...)
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
