package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

// persistentState is what survives a crash (Figure 2, "Persistent state on
// all servers"), plus where the snapshot ends. Everything else is rebuilt:
// commitIndex and lastApplied restart at the snapshot, the leader brings them
// forward, and the service replays the log from there.
type persistentState struct {
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry // Log[0] stands in for the snapshot
	SnapIndex   int
}

// persist saves currentTerm, votedFor, and log to stable storage, together
// with the current snapshot. It must run before the node replies to any RPC,
// or acts on any decision, that depends on the new values: a vote forgotten
// in a crash lets a node vote twice in one term, which can elect two leaders.
// Caller holds rf.mu.
//
// Commands inside LogEntry are interfaces, so the service must gob.Register
// every concrete command type it uses.
func (rf *Raft) persist() {
	state, snapshot, version := rf.encodeState()
	rf.save(state, snapshot, version)
}

// encodeState captures the persistent state, tagged with a version that
// increases with every capture. Caller holds rf.mu.
func (rf *Raft) encodeState() (state, snapshot []byte, version uint64) {
	var buf bytes.Buffer
	s := persistentState{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		Log:         rf.log,
		SnapIndex:   rf.snapIndex,
	}
	if err := gob.NewEncoder(&buf).Encode(s); err != nil {
		panic(fmt.Sprintf("raft %d: persist: %v", rf.me, err))
	}
	rf.stateVersion++
	return buf.Bytes(), rf.snapshot, rf.stateVersion
}

// save writes one captured state to disk, unless a newer capture is already
// there. The leader's background saves run without rf.mu, so a capture can
// reach save after a later one (say, a vote) has been written; letting the
// older one overwrite it would forget the vote.
//
// Raft state and snapshot are saved in one step. Saving them separately
// would let a crash in between leave a log that starts after a snapshot the
// disk does not have.
func (rf *Raft) save(state, snapshot []byte, version uint64) {
	rf.saveMu.Lock()
	defer rf.saveMu.Unlock()
	if version <= rf.savedVersion {
		return
	}
	rf.persister.Save(state, snapshot)
	rf.savedVersion = version
}

// logSaver is the leader's group commit. Start only appends to the log in
// memory and wakes this goroutine, which saves everything appended since its
// last save in one write, without holding rf.mu. Only then does the leader
// count its own copy toward a majority (matchIndex[me]), so no entry commits
// before the leader has it on disk. Followers still save before replying,
// once per AppendEntries, and replicators batch those too.
func (rf *Raft) logSaver() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		if !rf.logDirty {
			rf.saveCond.Wait()
			continue
		}
		rf.logDirty = false
		state, snapshot, version := rf.encodeState()
		term, last := rf.currentTerm, rf.lastLogIndex()

		rf.mu.Unlock()
		rf.save(state, snapshot, version)
		rf.mu.Lock()

		// A leader never removes entries from its own log in its own term,
		// so if we still lead that term, entries through last are on disk.
		if rf.state == Leader && rf.currentTerm == term {
			rf.matchIndex[rf.me] = max(rf.matchIndex[rf.me], last)
			rf.advanceCommitIndex()
		}
	}
}

// readPersist restores state saved by persist. Called from Make.
func (rf *Raft) readPersist(data, snapshot []byte) {
	if len(data) == 0 {
		return // fresh node, nothing saved yet
	}
	var state persistentState
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&state); err != nil {
		panic(fmt.Sprintf("raft %d: readPersist: %v", rf.me, err))
	}
	rf.currentTerm = state.CurrentTerm
	rf.votedFor = state.VotedFor
	rf.log = state.Log
	rf.snapIndex = state.SnapIndex
	rf.snapshot = snapshot

	// The snapshot only ever holds committed, applied entries, and the
	// service restores it on its own at startup.
	rf.commitIndex = rf.snapIndex
	rf.lastApplied = rf.snapIndex
}
