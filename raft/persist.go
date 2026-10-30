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
	var buf bytes.Buffer
	state := persistentState{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		Log:         rf.log,
		SnapIndex:   rf.snapIndex,
	}
	if err := gob.NewEncoder(&buf).Encode(state); err != nil {
		panic(fmt.Sprintf("raft %d: persist: %v", rf.me, err))
	}
	// Raft state and snapshot are saved in one step. Saving them separately
	// would let a crash in between leave a log that starts after a snapshot
	// the disk does not have.
	rf.persister.Save(buf.Bytes(), rf.snapshot)
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
