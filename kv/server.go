package kv

import (
	"bytes"
	"encoding/gob"
	"fmt"
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

	persister         *raft.Persister
	maxRaftState      int // snapshot once Raft's saved state reaches this many bytes; 0 never
	sizeAfterSnapshot int // Raft state size right after our last snapshot

	// State machine. Rebuilt on restart from the snapshot plus the log after it.
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
// this server, and persister is this server's disk. Once Raft's saved state
// grows to maxRaftState bytes the server snapshots and Raft trims its log;
// pass 0 to never snapshot.
func StartServer(peers []raft.Peer, me int, persister *raft.Persister, maxRaftState int) *Server {
	kv := &Server{
		me:           me,
		applyCh:      make(chan raft.ApplyMsg),
		persister:    persister,
		maxRaftState: maxRaftState,
		data:         map[string]string{},
		lastSeq:      map[int64]int64{},
		waiters:      map[int]chan applied{},
	}
	kv.restoreSnapshot(persister.ReadSnapshot())
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

// Status reports this server's view of itself. It does not go through the
// log, so it answers even without a majority.
func (kv *Server) Status(args *StatusArgs, reply *StatusReply) {
	term, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	defer kv.mu.Unlock()
	*reply = StatusReply{
		ID:            kv.me,
		Term:          term,
		IsLeader:      isLeader,
		LastApplied:   kv.lastApplied,
		Keys:          len(kv.data),
		RaftStateSize: kv.persister.RaftStateSize(),
		Saves:         kv.persister.SaveCount(),
	}
}

// applier applies committed entries to the state machine, in log order.
func (kv *Server) applier() {
	for msg := range kv.applyCh {
		kv.mu.Lock()
		switch {
		case msg.SnapshotValid:
			// The leader's snapshot replaces our state. Handlers waiting on
			// indexes it covers time out and their clients retry; dedup makes
			// that safe.
			if msg.SnapshotIndex > kv.lastApplied {
				kv.restoreSnapshot(msg.Snapshot)
				kv.sizeAfterSnapshot = 0
			}
		case msg.CommandValid && msg.CommandIndex > kv.lastApplied:
			kv.lastApplied = msg.CommandIndex
			op := msg.Command.(Op)
			a := applied{clientID: op.ClientID, seq: op.Seq, reply: kv.applyOp(op)}
			if ch, ok := kv.waiters[msg.CommandIndex]; ok {
				ch <- a // buffered; never blocks
				delete(kv.waiters, msg.CommandIndex)
			}
			kv.maybeSnapshot()
		}
		kv.mu.Unlock()
	}
}

// maybeSnapshot snapshots once Raft's saved state reaches maxRaftState, and
// it has grown by at least half of that since the last snapshot. Caller holds
// kv.mu.
//
// The growth rule matters for a follower that falls behind. A snapshot only
// removes applied entries, so when the unapplied backlog alone exceeds the
// limit, the state stays over it after every snapshot. Without the rule the
// server then snapshots on every applied entry, rewriting the whole backlog
// to disk each time, and falls further behind.
func (kv *Server) maybeSnapshot() {
	if kv.maxRaftState <= 0 {
		return
	}
	size := kv.persister.RaftStateSize()
	if size < kv.maxRaftState || size-kv.sizeAfterSnapshot < kv.maxRaftState/2 {
		return
	}
	kv.rf.Snapshot(kv.lastApplied, kv.encodeSnapshot())
	kv.sizeAfterSnapshot = kv.persister.RaftStateSize()
}

// kvSnapshot is the state machine as of one log index. The dedup table is
// part of it: without it, a client retrying a request that the snapshot
// already covers would have that request applied a second time.
type kvSnapshot struct {
	Data        map[string]string
	LastSeq     map[int64]int64
	LastApplied int
}

// encodeSnapshot captures the state machine. Caller holds kv.mu.
func (kv *Server) encodeSnapshot() []byte {
	var buf bytes.Buffer
	s := kvSnapshot{Data: kv.data, LastSeq: kv.lastSeq, LastApplied: kv.lastApplied}
	if err := gob.NewEncoder(&buf).Encode(s); err != nil {
		panic(fmt.Sprintf("kv %d: encode snapshot: %v", kv.me, err))
	}
	return buf.Bytes()
}

// restoreSnapshot replaces the state machine with a snapshot. Caller holds
// kv.mu, or is StartServer before anything else runs.
func (kv *Server) restoreSnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	var s kvSnapshot
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&s); err != nil {
		panic(fmt.Sprintf("kv %d: decode snapshot: %v", kv.me, err))
	}
	// gob decodes an empty map as nil; writes to a nil map panic.
	kv.data = s.Data
	if kv.data == nil {
		kv.data = map[string]string{}
	}
	kv.lastSeq = s.LastSeq
	if kv.lastSeq == nil {
		kv.lastSeq = map[int64]int64{}
	}
	kv.lastApplied = s.LastApplied
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
