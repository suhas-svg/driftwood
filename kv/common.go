// Package kv is a linearizable key-value store replicated with Raft.
//
// Raft orders opaque commands; this package is the state machine those
// commands drive. Every operation, reads included, goes through the Raft log,
// so a leader cut off by a partition can never serve stale data: it cannot
// commit anything, so it cannot answer.
package kv

import "encoding/gob"

type Err string

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"       // Get of a key that was never written
	ErrWrongLeader Err = "ErrWrongLeader" // try another server
	ErrTimeout     Err = "ErrTimeout"     // not committed in time; may still commit, so retry
)

type OpKind string

const (
	OpGet    OpKind = "Get"
	OpPut    OpKind = "Put"
	OpAppend OpKind = "Append"
)

// Op is the command stored in the Raft log.
//
// ClientID and Seq give each request a unique identity. A client retries a
// request with the same Seq until it gets an answer, so the same Op can land
// in the log more than once; the server applies only the first.
type Op struct {
	Kind     OpKind
	Key      string
	Value    string
	ClientID int64
	Seq      int64
}

type CommandArgs struct {
	Op Op
}

type CommandReply struct {
	Err   Err
	Value string // for Get
}

type StatusArgs struct{}

// StatusReply describes one server, for operators and the CLI. It is not
// linearizable: a server that thinks it leads may already have been replaced.
type StatusReply struct {
	ID            int
	Term          int
	IsLeader      bool
	LastApplied   int
	Keys          int
	RaftStateSize int // bytes of persisted Raft state (log, term, vote)
}

func init() {
	// Op travels inside raft.LogEntry.Command, an interface, so gob needs the
	// concrete type registered for both RPCs and persistence.
	gob.Register(Op{})
}
