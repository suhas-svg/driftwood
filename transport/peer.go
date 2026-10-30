// Package transport carries Raft and KV RPCs over TCP using net/rpc.
//
// It is the real-network counterpart of package sim. Peer implements
// raft.Peer, so the same Raft and KV code runs unchanged on either. Arguments
// travel gob-encoded, as they do through sim.
package transport

import (
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/suhas-svg/driftwood/raft"
)

const dialTimeout = 300 * time.Millisecond

// Peer is a connection to one remote service ("Raft" or "KV") at addr. It
// dials lazily and redials after any failure, so a peer that is down, or
// restarts, needs no special handling. Safe for concurrent use.
type Peer struct {
	addr    string
	service string
	timeout time.Duration

	mu     sync.Mutex
	client *rpc.Client
}

// NewPeer returns a Peer for service at addr. A call that gets no reply
// within timeout counts as lost, the way a dropped message does in sim.
func NewPeer(addr, service string, timeout time.Duration) *Peer {
	return &Peer{addr: addr, service: service, timeout: timeout}
}

// Peers returns one Peer per address, all for the same service, in the form
// raft.Make and kv.MakeClerk take.
func Peers(addrs []string, service string, timeout time.Duration) []raft.Peer {
	out := make([]raft.Peer, len(addrs))
	for i, a := range addrs {
		out[i] = NewPeer(a, service, timeout)
	}
	return out
}

// Call invokes service.method on the remote node. It returns false if the
// node is unreachable or does not answer in time; as with any lost message,
// the handler may or may not have run.
func (p *Peer) Call(method string, args, reply any) bool {
	client := p.connect()
	if client == nil {
		return false
	}
	call := client.Go(p.service+"."+method, args, reply, make(chan *rpc.Call, 1))
	select {
	case <-call.Done:
		if call.Error != nil {
			// A broken connection shows up as an error on every call;
			// drop it so the next call redials.
			if _, isServerErr := call.Error.(rpc.ServerError); !isServerErr {
				p.reset(client)
			}
			return false
		}
		return true
	case <-time.After(p.timeout):
		// The caller treats the reply as lost and never reads it, so it is
		// fine for net/rpc to fill it in later.
		return false
	}
}

func (p *Peer) connect() *rpc.Client {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client != nil {
		return p.client
	}
	conn, err := net.DialTimeout("tcp", p.addr, dialTimeout)
	if err != nil {
		return nil
	}
	p.client = rpc.NewClient(conn)
	return p.client
}

func (p *Peer) reset(client *rpc.Client) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client == client {
		p.client.Close()
		p.client = nil
	}
}

// Close drops the connection, if any.
func (p *Peer) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client != nil {
		p.client.Close()
		p.client = nil
	}
}
