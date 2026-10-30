package raft

import "time"

// Replication runs two ways. Each follower has a replicator goroutine that
// keeps at most one AppendEntries in flight: entries that arrive while a
// request is out pile up and go together in the next one, so a busy leader
// sends (and the follower saves) one batch per round trip instead of one
// entry. Heartbeats go out separately on a timer, so a slow follower does not
// delay the others hearing from the leader.

// broadcastAppendEntries sends each follower the entries it is missing, or an
// empty heartbeat if it is caught up. Caller holds rf.mu and is the leader.
func (rf *Raft) broadcastAppendEntries() {
	for i := range rf.peers {
		if i != rf.me {
			if args, snap := rf.prepareSend(i); snap != nil {
				go rf.sendSnapshot(i, snap)
			} else {
				go rf.sendAppend(i, args)
			}
		}
	}
}

// triggerReplication wakes server's replicator. It never blocks, and many
// triggers before the replicator runs count as one. Caller holds rf.mu.
func (rf *Raft) triggerReplication(server int) {
	if rf.state != Leader || rf.replicate == nil {
		return
	}
	select {
	case rf.replicate[server] <- struct{}{}:
	default:
	}
}

// replicator ships new entries to one follower for as long as this node
// leads term. It waits for a trigger, sends everything the follower is
// missing in one RPC, waits for the reply, and repeats if more arrived.
func (rf *Raft) replicator(server, term int, wake chan struct{}) {
	for !rf.killed() {
		select {
		case <-wake:
		case <-time.After(heartbeatInterval):
			// Recheck leadership even without work, so a replicator from an
			// old term exits.
		}

		rf.mu.Lock()
		if rf.state != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		if rf.nextIndex[server] > rf.lastLogIndex() {
			rf.mu.Unlock()
			continue // follower is caught up; heartbeats keep it that way
		}
		args, snap := rf.prepareSend(server)
		rf.mu.Unlock()

		if snap != nil {
			rf.sendSnapshot(server, snap)
		} else {
			rf.sendAppend(server, args)
		}
	}
}

// prepareSend builds the next message for server: AppendEntries from its
// nextIndex, or InstallSnapshot if those entries were compacted away.
// Exactly one result is non-nil. Caller holds rf.mu and is the leader.
func (rf *Raft) prepareSend(server int) (*AppendEntriesArgs, *InstallSnapshotArgs) {
	prev := rf.nextIndex[server] - 1
	if prev < rf.snapIndex {
		return nil, &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderID:          rf.me,
			LastIncludedIndex: rf.snapIndex,
			LastIncludedTerm:  rf.log[0].Term,
			Data:              rf.snapshot,
		}
	}
	return &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prev,
		PrevLogTerm:  rf.termAt(prev),
		Entries:      rf.entriesFrom(prev + 1),
		LeaderCommit: rf.commitIndex,
	}, nil
}

// sendAppend sends one AppendEntries and handles the reply. Called without rf.mu.
func (rf *Raft) sendAppend(server int, args *AppendEntriesArgs) {
	var reply AppendEntriesReply
	if !rf.sendAppendEntries(server, args, &reply) {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.handleAppendReply(server, args, &reply)
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
		if rf.nextIndex[server] <= rf.lastLogIndex() {
			rf.triggerReplication(server) // more arrived while this was in flight
		}
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
	rf.triggerReplication(server) // retry right away
}

// advanceCommitIndex commits the highest entry stored on a majority, but only
// if it comes from the current term (section 5.4.2, Figure 8). Entries from
// earlier terms then commit indirectly, because everything before a committed
// entry is committed too. matchIndex[me] counts only entries the leader has
// saved (see logSaver). Caller holds rf.mu and is the leader.
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
