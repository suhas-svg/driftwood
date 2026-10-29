package raft

import "sync"

// Persister stands in for a node's disk. It survives a simulated crash:
// the test harness hands the same Persister (or a copy) to the restarted node.
//
// Raft must save currentTerm, votedFor, and log[] here before replying to any
// RPC that changed them.
type Persister struct {
	mu        sync.Mutex
	raftState []byte
	snapshot  []byte
}

func NewPersister() *Persister { return &Persister{} }

// Copy returns an independent Persister holding the same bytes.
func (p *Persister) Copy() *Persister {
	p.mu.Lock()
	defer p.mu.Unlock()
	return &Persister{raftState: clone(p.raftState), snapshot: clone(p.snapshot)}
}

// Save atomically writes Raft state and the snapshot together.
func (p *Persister) Save(raftState, snapshot []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.raftState = clone(raftState)
	p.snapshot = clone(snapshot)
}

func (p *Persister) ReadRaftState() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return clone(p.raftState)
}

func (p *Persister) RaftStateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.raftState)
}

func (p *Persister) ReadSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return clone(p.snapshot)
}

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}
