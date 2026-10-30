package raft

// Log compaction (section 7 of the paper). The service periodically hands
// Raft a snapshot of its state, and Raft discards the log entries the
// snapshot covers. A follower so far behind that the leader no longer has the
// entries it needs gets the snapshot itself, through InstallSnapshot.

// Snapshot tells Raft that the service's state up to and including index is
// captured in snapshot, so the log through index can be discarded. The
// service calls it with an index it has already applied.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Ignore a snapshot that is not newer than ours (a leader's
	// InstallSnapshot may have overtaken it) or covers unapplied entries.
	if index <= rf.snapIndex || index > rf.lastApplied {
		return
	}
	rf.compactThrough(index, rf.termAt(index))
	rf.snapshot = snapshot
	rf.persist()
}

// compactThrough drops log entries up to and including index, keeping any
// after it. Caller holds rf.mu and has checked that index > snapIndex.
func (rf *Raft) compactThrough(index, term int) {
	// Copy into a fresh slice so the old entries can be garbage collected.
	var rest []LogEntry
	if index < rf.lastLogIndex() {
		rest = rf.log[index-rf.snapIndex+1:]
	}
	log := make([]LogEntry, 1, 1+len(rest))
	log[0] = LogEntry{Term: term}
	rf.log = append(log, rest...)
	rf.snapIndex = index
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// InstallSnapshot replaces this follower's state with the leader's snapshot
// (Figure 13). The whole snapshot is sent in one RPC rather than in chunks.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.state = Follower
	rf.resetElectionTimer()

	// A delayed or duplicate snapshot that we have already moved past.
	// Installing it would roll back entries we have committed.
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	if args.LastIncludedIndex <= rf.lastLogIndex() && rf.termAt(args.LastIncludedIndex) == args.LastIncludedTerm {
		// We hold the snapshot's last entry, so the entries after it agree
		// with the leader too (Log Matching). Keep them.
		rf.compactThrough(args.LastIncludedIndex, args.LastIncludedTerm)
	} else {
		// Our log is behind or diverges; the snapshot replaces all of it.
		rf.log = []LogEntry{{Term: args.LastIncludedTerm}}
		rf.snapIndex = args.LastIncludedIndex
	}
	rf.snapshot = args.Data
	rf.persist()

	// Everything in the snapshot is committed. The applier hands it to the
	// service before any entry after it.
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	rf.pendingSnapshot = true
	rf.applyCond.Signal()
}

// sendSnapshotTo sends the current snapshot to a follower that needs entries
// we no longer have. Caller holds rf.mu and is the leader.
func (rf *Raft) sendSnapshotTo(server int) {
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.snapIndex,
		LastIncludedTerm:  rf.log[0].Term,
		Data:              rf.snapshot,
	}
	go func() {
		var reply InstallSnapshotReply
		if !rf.peers[server].Call("InstallSnapshot", &args, &reply) {
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			return
		}
		if rf.state != Leader || rf.currentTerm != args.Term {
			return
		}
		rf.matchIndex[server] = max(rf.matchIndex[server], args.LastIncludedIndex)
		rf.nextIndex[server] = max(rf.nextIndex[server], args.LastIncludedIndex+1)
		// Follow up with whatever entries came after the snapshot.
		if rf.nextIndex[server] <= rf.lastLogIndex() {
			rf.sendEntriesTo(server)
		}
	}()
}
