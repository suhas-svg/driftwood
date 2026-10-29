package raft

// persist saves currentTerm, votedFor, and log to stable storage.
// Caller holds rf.mu.
func (rf *Raft) persist() {
	// TODO(week 4): gob-encode currentTerm, votedFor, and log into a buffer
	// and call rf.persister.Save(buf, rf.persister.ReadSnapshot()).
	// Commands inside LogEntry are interfaces, so the service must
	// gob.Register every concrete command type it uses.
}

// readPersist restores state saved by persist. Called from Make.
func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 {
		return // fresh node, nothing saved yet
	}
	// TODO(week 4): decode in the same order persist encodes.
}
