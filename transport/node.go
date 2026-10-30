package transport

import (
	"fmt"
	"net"
	"time"

	"github.com/suhas-svg/driftwood/kv"
	"github.com/suhas-svg/driftwood/raft"
)

const (
	// Raft RPCs are small and the election timeout is 300-600ms, so a peer
	// that has not answered in 250ms is treated as gone for this round.
	raftTimeout = 250 * time.Millisecond
	// A KV server waits up to 500ms for its entry to commit before telling
	// the client to retry, so clients must wait longer than that.
	ClientTimeout = 2 * time.Second
)

// NodeConfig describes one server process.
type NodeConfig struct {
	ID           int      // index into Addrs
	Addrs        []string // every node's address, in the same order on every node
	Dir          string   // where this node keeps its state file
	MaxRaftState int      // snapshot threshold in bytes; 0 never snapshots
	NoSync       bool     // skip fsync; unsafe, for benchmarks
}

// Node is a running server: disk, Raft, KV store, and TCP listener.
type Node struct {
	KV    *kv.Server
	srv   *Server
	ln    net.Listener
	peers []raft.Peer
	err   chan error
}

// StartNode recovers the node's state from disk and starts serving on its
// address.
func StartNode(cfg NodeConfig) (*Node, error) {
	if cfg.ID < 0 || cfg.ID >= len(cfg.Addrs) {
		return nil, fmt.Errorf("node id %d out of range for %d addresses", cfg.ID, len(cfg.Addrs))
	}
	persister, err := raft.NewFilePersister(cfg.Dir, !cfg.NoSync)
	if err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", cfg.Addrs[cfg.ID])
	if err != nil {
		return nil, err
	}

	peers := Peers(cfg.Addrs, "Raft", raftTimeout)
	n := &Node{
		KV:    kv.StartServer(peers, cfg.ID, persister, cfg.MaxRaftState),
		ln:    ln,
		peers: peers,
		err:   make(chan error, 1),
	}
	n.srv = NewServer(n.KV)
	go func() { n.err <- n.srv.Serve(ln) }()
	return n, nil
}

// Addr is the address the node listens on.
func (n *Node) Addr() string { return n.ln.Addr().String() }

// Wait blocks until the node stops serving.
func (n *Node) Wait() error { return <-n.err }

// Stop kills the node: it stops serving, drops its connections, and halts
// Raft. Whatever it persisted stays on disk for the next StartNode.
func (n *Node) Stop() {
	n.srv.Close()
	n.KV.Kill()
	for _, p := range n.peers {
		p.(*Peer).Close()
	}
}
