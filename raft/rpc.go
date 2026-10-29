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

	// TODO(week 3), "AppendEntries RPC: Receiver implementation":
	//   4. Reply false if the log has no entry at PrevLogIndex with PrevLogTerm.
	//   5. For each new entry, if an existing entry conflicts (same index,
	//      different term), truncate from there. Only truncate on a CONFLICT;
	//      a stale, matching AppendEntries must not shorten the log.
	//   6. Append any entries not already present, then persist.
	//   7. If LeaderCommit > commitIndex, set
	//      commitIndex = min(LeaderCommit, index of last new entry).
	reply.Success = false
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("RequestVote", args, reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("AppendEntries", args, reply)
}
