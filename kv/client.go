package kv

import (
	"math/rand/v2"
	"time"

	"github.com/suhas-svg/driftwood/raft"
)

// Clerk is a client of the key-value store. A Clerk issues one request at a
// time; use several Clerks for concurrency.
type Clerk struct {
	servers []raft.Peer
	id      int64
	seq     int64
	leader  int // the server that last answered; try it first
}

func MakeClerk(servers []raft.Peer) *Clerk {
	return &Clerk{servers: servers, id: rand.Int64()}
}

// Get returns the value of key, or "" if it does not exist.
func (ck *Clerk) Get(key string) string {
	return ck.do(Op{Kind: OpGet, Key: key})
}

// Put sets key to value.
func (ck *Clerk) Put(key, value string) {
	ck.do(Op{Kind: OpPut, Key: key, Value: value})
}

// Append adds value to the end of key's current value.
func (ck *Clerk) Append(key, value string) {
	ck.do(Op{Kind: OpAppend, Key: key, Value: value})
}

// do sends op until some server commits it. It never gives up: in this model
// the cluster eventually recovers, and every retry carries the same Seq, so
// retries never apply an operation twice.
func (ck *Clerk) do(op Op) string {
	ck.seq++
	op.ClientID = ck.id
	op.Seq = ck.seq
	args := CommandArgs{Op: op}

	for {
		for range ck.servers {
			var reply CommandReply
			ok := ck.servers[ck.leader].Call("Command", &args, &reply)
			if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
				return reply.Value
			}
			// Lost message, wrong leader, or no commit in time: next server.
			ck.leader = (ck.leader + 1) % len(ck.servers)
		}
		// Nobody could serve it; probably mid-election. Back off briefly.
		time.Sleep(50 * time.Millisecond)
	}
}
