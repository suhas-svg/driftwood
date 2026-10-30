package raft

// broadcastAppendEntries sends each follower the entries it is missing, or an
// empty heartbeat if it is caught up. Caller holds rf.mu and is the leader.
func (rf *Raft) broadcastAppendEntries() {
	for i := range rf.peers {
		if i != rf.me {
			rf.sendEntriesTo(i)
		}
	}
}

// sendEntriesTo sends one AppendEntries to server, starting at its nextIndex,
// and handles the reply in the background. If the entries it needs were
// already replaced by a snapshot, it sends the snapshot instead.
// Caller holds rf.mu.
func (rf *Raft) sendEntriesTo(server int) {
	prev := rf.nextIndex[server] - 1
	if prev < rf.snapIndex {
		rf.sendSnapshotTo(server)
		return
	}
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prev,
		PrevLogTerm:  rf.termAt(prev),
		Entries:      rf.entriesFrom(prev + 1),
		LeaderCommit: rf.commitIndex,
	}

	go func() {
		var reply AppendEntriesReply
		if !rf.sendAppendEntries(server, &args, &reply) {
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.handleAppendReply(server, &args, &reply)
	}()
}

// handleAppendReply updates replication progress for server. Caller holds rf.mu.
func (rf *Raft) handleAppendReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}
	// We may have lost leadership, or moved to a newer term, while the RPC
	// was in flight. A stale reply says nothing about the current term.
	if rf.state != Leader || rf.currentTerm != args.Term {
		return
	}

	if reply.Success {
		// Replies can arrive out of order, so never move progress backward.
		match := args.PrevLogIndex + len(args.Entries)
		rf.matchIndex[server] = max(rf.matchIndex[server], match)
		rf.nextIndex[server] = max(rf.nextIndex[server], match+1)
		rf.advanceCommitIndex()
		return
	}

	// The follower's log does not match at PrevLogIndex. Use its hints to
	// skip a whole conflicting term at once instead of one entry per RPC.
	next := reply.ConflictIndex
	if reply.ConflictTerm != -1 {
		if last := rf.lastIndexOfTerm(reply.ConflictTerm); last != -1 {
			next = last + 1
		}
	}
	// Everything up to matchIndex is known to match, so never back up past it.
	// (A delayed rejection from before the follower caught up would try to.)
	rf.nextIndex[server] = max(rf.matchIndex[server]+1, min(next, rf.lastLogIndex()+1))
	rf.sendEntriesTo(server) // retry right away
}

// advanceCommitIndex commits the highest entry stored on a majority, but only
// if it comes from the current term (section 5.4.2, Figure 8). Entries from
// earlier terms then commit indirectly, because everything before a committed
// entry is committed too. Caller holds rf.mu and is the leader.
func (rf *Raft) advanceCommitIndex() {
	for n := rf.lastLogIndex(); n > rf.commitIndex; n-- {
		if rf.termAt(n) != rf.currentTerm {
			// Terms only decrease going backward, so nothing earlier qualifies.
			return
		}
		count := 0
		for i := range rf.peers {
			if rf.matchIndex[i] >= n {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.applyCond.Signal()
			return
		}
	}
}

// lastIndexOfTerm returns the index of our last entry with the given term, or
// -1 if we have none outside the snapshot. Caller holds rf.mu.
func (rf *Raft) lastIndexOfTerm(term int) int {
	for i := rf.lastLogIndex(); i > rf.snapIndex; i-- {
		if t := rf.termAt(i); t == term {
			return i
		} else if t < term {
			break
		}
	}
	return -1
}
