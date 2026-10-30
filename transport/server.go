package transport

import (
	"errors"
	"net"
	"net/rpc"
	"sync"

	"github.com/suhas-svg/driftwood/kv"
	"github.com/suhas-svg/driftwood/raft"
)

// Server accepts TCP connections and dispatches RPCs to one node's Raft and
// KV handlers.
type Server struct {
	rpc *rpc.Server
	ln  net.Listener

	mu     sync.Mutex
	conns  map[net.Conn]bool
	closed bool
}

// NewServer exposes srv and its Raft node as the "KV" and "Raft" services.
func NewServer(srv *kv.Server) *Server {
	s := &Server{rpc: rpc.NewServer(), conns: map[net.Conn]bool{}}
	// net/rpc wants methods that return error; these adapters supply one.
	if err := s.rpc.RegisterName("Raft", &raftService{srv.Raft()}); err != nil {
		panic(err)
	}
	if err := s.rpc.RegisterName("KV", &kvService{srv}); err != nil {
		panic(err)
	}
	return s
}

// Serve accepts connections on ln until Close. It always returns an error;
// after Close, net.ErrClosed.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			conn.Close()
			return net.ErrClosed
		}
		s.conns[conn] = true
		s.mu.Unlock()
		go func() {
			s.rpc.ServeConn(conn)
			s.mu.Lock()
			delete(s.conns, conn)
			s.mu.Unlock()
		}()
	}
}

// Close stops accepting and drops every open connection, so peers see this
// node vanish just as they would if its process died.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	for c := range s.conns {
		c.Close()
	}
	if s.ln == nil {
		return errors.New("transport: server not serving")
	}
	return s.ln.Close()
}

type raftService struct{ rf *raft.Raft }

func (s *raftService) RequestVote(a *raft.RequestVoteArgs, r *raft.RequestVoteReply) error {
	s.rf.RequestVote(a, r)
	return nil
}

func (s *raftService) AppendEntries(a *raft.AppendEntriesArgs, r *raft.AppendEntriesReply) error {
	s.rf.AppendEntries(a, r)
	return nil
}

func (s *raftService) InstallSnapshot(a *raft.InstallSnapshotArgs, r *raft.InstallSnapshotReply) error {
	s.rf.InstallSnapshot(a, r)
	return nil
}

type kvService struct{ srv *kv.Server }

func (s *kvService) Command(a *kv.CommandArgs, r *kv.CommandReply) error {
	s.srv.Command(a, r)
	return nil
}

func (s *kvService) Status(a *kv.StatusArgs, r *kv.StatusReply) error {
	s.srv.Status(a, r)
	return nil
}
