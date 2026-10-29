package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

// persistentState is what survives a crash (Figure 2, "Persistent state on
// all servers"). Everything else is rebuilt: commitIndex and lastApplied
// start at 0 and the leader brings them forward, and the service replays the
// log from the start.
type persistentState struct {
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
}

// persist saves currentTerm, votedFor, and log to stable storage. It must run
// before the node replies to any RPC, or acts on any decision, that depends on
// the new values: a vote forgotten in a crash lets a node vote twice in one
// term, which can elect two leaders. Caller holds rf.mu.
//
// Commands inside LogEntry are interfaces, so the service must gob.Register
// every concrete command type it uses.
func (rf *Raft) persist() {
	var buf bytes.Buffer
	state := persistentState{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		Log:         rf.log,
	}
	if err := gob.NewEncoder(&buf).Encode(state); err != nil {
		panic(fmt.Sprintf("raft %d: persist: %v", rf.me, err))
	}
	rf.persister.Save(buf.Bytes(), rf.persister.ReadSnapshot())
}

// readPersist restores state saved by persist. Called from Make.
func (rf *Raft) readPersist(data []byte) {
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
}
