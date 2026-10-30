package raft

import "time"

// PreVote (section 9.6 of Ongaro's thesis). Without it, a node cut off from
// the cluster times out again and again, bumping its term each time. When it
// rejoins, its higher term forces the healthy leader to step down, and the
// cluster sits through an election for nothing.
//
// With PreVote, a node whose election timer fires first asks the others
// whether they would vote for it in the next term, without anyone changing
// state. Only if a majority says yes does it start a real election. A
// partitioned node never gets that majority, so its term stays put.

// PreVote handles a pre-vote request. It uses the RequestVote types: Term is
// the term the sender would campaign in. Nothing here changes our state, so
// there is nothing to persist.
func (rf *Raft) PreVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	// Refuse while a leader is in charge: we lead, or heard from a leader
	// more recently than the minimum election timeout. Otherwise one node
	// that missed a few heartbeats could still win a majority and depose a
	// healthy leader.
	if rf.state == Leader || rf.heardFromLeaderRecently() {
		return
	}
	reply.VoteGranted = rf.candidateUpToDate(args.LastLogTerm, args.LastLogIndex)
}

// heardFromLeaderRecently reports whether a current leader contacted us
// within the minimum election timeout. It must not use the election timer:
// a node resets that when it starts its own pre-vote round, so two nodes
// that time out together would refuse each other forever. Caller holds rf.mu.
func (rf *Raft) heardFromLeaderRecently() bool {
	return time.Since(rf.leaderContact) < electionTimeoutMin
}

// startPreVote runs a pre-vote round and starts a real election only if a
// majority would vote for us. Caller holds rf.mu.
func (rf *Raft) startPreVote() {
	rf.resetElectionTimer() // if this round fails, try again after another timeout
	epoch := rf.electionEpoch
	term := rf.currentTerm
	args := RequestVoteArgs{
		Term:         term + 1,
		CandidateID:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.lastLogTerm(),
	}
	votes := 1 // our own

	// The round is stale once anything else resets the election timer: a
	// valid leader's AppendEntries, a vote we granted, or a newer round.
	current := func() bool {
		return rf.state != Leader && rf.currentTerm == term && rf.electionEpoch == epoch
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			var reply RequestVoteReply
			if !rf.peers[server].Call("PreVote", &args, &reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			if !current() || !reply.VoteGranted {
				return
			}
			votes++
			if votes > len(rf.peers)/2 { // startElection makes later replies stale
				rf.startElection()
			}
		}(i)
	}
	if votes > len(rf.peers)/2 { // a single-node cluster
		rf.startElection()
	}
}
