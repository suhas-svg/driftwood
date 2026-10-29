package kv

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/suhas-svg/driftwood/raft"
)

// How long a server waits for its entry to commit before telling the client
// to retry. A leader stuck in a minority partition never commits, so without
// this the client would hang on it forever.
const commitTimeout = 500 * time.Millisecond

// Server is one replica of the key-value store.
type Server struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    atomic.Bool

	// State machine. Rebuilt on restart by replaying the Raft log.
	data        map[string]string
	lastSeq     map[int64]int64 // per client: highest Seq applied, for dedup
	lastApplied int

	// RPC handlers waiting for the entry at a log index to be applied.
	waiters map[int]chan applied
}

// applied is what the applier tells a waiting handler about one log entry.
type applied struct {
	clientID, seq int64 // which request actually landed at that index
	reply         CommandReply
}

// StartServer creates a replica. peers are the Raft peers, peers[me] being
// this server, and persister is this server's disk.
func StartServer(peers []raft.Peer, me int, persister *raft.Persister) *Server {
	kv := &Server{
		me:      me,
		applyCh: make(chan raft.ApplyMsg),
		data:    map[string]string{},
		lastSeq: map[int64]int64{},
		waiters: map[int]chan applied{},
	}
	kv.rf = raft.Make(peers, me, persister, kv.applyCh)
	go kv.applier()
	return kv
}

// Raft returns the underlying Raft node, so the network can route its RPCs.
func (kv *Server) Raft() *raft.Raft { return kv.rf }

// Kill stops the server and its Raft node.
func (kv *Server) Kill() {
	kv.dead.Store(true)
	kv.rf.Kill()
}

func (kv *Server) killed() bool { return kv.dead.Load() }

// Command is the RPC clients call for every operation.
func (kv *Server) Command(args *CommandArgs, reply *CommandReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	op := args.Op

	// Hold kv.mu across Start so the waiter is registered before the applier
	// can possibly see the entry. Lock order is always kv.mu, then rf.mu.
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan applied, 1)
	kv.waiters[index] = ch
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		if kv.waiters[index] == ch {
			delete(kv.waiters, index)
		}
		kv.mu.Unlock()
	}()

	select {
	case a := <-ch:
		if a.clientID != op.ClientID || a.seq != op.Seq {
			// A different entry won this index: we lost leadership and a new
			// leader overwrote ours. The client must retry elsewhere.
			reply.Err = ErrWrongLeader
			return
		}
		*reply = a.reply
	case <-time.After(commitTimeout):
		reply.Err = ErrTimeout
	}
}

// applier applies committed entries to the state machine, in log order.
func (kv *Server) applier() {
	for msg := range kv.applyCh {
		if !msg.CommandValid {
			continue
		}
		kv.mu.Lock()
		if msg.CommandIndex <= kv.lastApplied {
			kv.mu.Unlock()
			continue
		}
		kv.lastApplied = msg.CommandIndex

		op := msg.Command.(Op)
		a := applied{clientID: op.ClientID, seq: op.Seq, reply: kv.applyOp(op)}
		if ch, ok := kv.waiters[msg.CommandIndex]; ok {
			ch <- a // buffered; never blocks
			delete(kv.waiters, msg.CommandIndex)
		}
		kv.mu.Unlock()
	}
}

// applyOp runs one operation against the state machine. Every replica runs
// the same ops in the same order, so every replica computes the same result.
// Caller holds kv.mu.
func (kv *Server) applyOp(op Op) CommandReply {
	if op.Kind == OpGet {
		v, ok := kv.data[op.Key]
		if !ok {
			return CommandReply{Err: ErrNoKey}
		}
		return CommandReply{Err: OK, Value: v}
	}

	// Exactly-once: a retried Put or Append may be in the log several times.
	// Clients send one request at a time with increasing Seq, so anything at
	// or below the last Seq applied for that client is a duplicate.
	if op.Seq <= kv.lastSeq[op.ClientID] {
		return CommandReply{Err: OK}
	}
	kv.lastSeq[op.ClientID] = op.Seq

	switch op.Kind {
	case OpPut:
		kv.data[op.Key] = op.Value
	case OpAppend:
		kv.data[op.Key] += op.Value
	}
	return CommandReply{Err: OK}
}
