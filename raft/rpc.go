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

	// TODO(week 2), from Figure 2 "RequestVote RPC: Receiver implementation":
	//   1. If args.Term > currentTerm, becomeFollower(args.Term) first.
	//   2. Reply false if args.Term < currentTerm.
	//   3. Grant the vote if votedFor is -1 or args.CandidateID, AND the
	//      candidate's log is at least as up-to-date as ours (section 5.4.1):
	//      compare last log terms first, then last log indexes.
	//   4. On granting: set votedFor, persist, reset the election timer.
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

// AppendEntries handles log replication and heartbeats from the leader.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// TODO(week 2): enough to make heartbeats work.
	//   1. If args.Term > currentTerm, becomeFollower(args.Term).
	//   2. Reply false if args.Term < currentTerm.
	//   3. Otherwise this is the real leader: become (or stay) Follower and
	//      reset the election timer.
	//
	// TODO(week 3), "AppendEntries RPC: Receiver implementation":
	//   4. Reply false if the log has no entry at PrevLogIndex with PrevLogTerm.
	//   5. For each new entry, if an existing entry conflicts (same index,
	//      different term), truncate from there. Only truncate on a CONFLICT;
	//      a stale, matching AppendEntries must not shorten the log.
	//   6. Append any entries not already present, then persist.
	//   7. If LeaderCommit > commitIndex, set
	//      commitIndex = min(LeaderCommit, index of last new entry).
	reply.Term = rf.currentTerm
	reply.Success = false
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("RequestVote", args, reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("AppendEntries", args, reply)
}
