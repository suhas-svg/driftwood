package sim

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type EchoArgs struct{ X int }
type EchoReply struct {
	X    int
	From int
}

type echo struct {
	id    int
	calls atomic.Int64
}

func (e *echo) Echo(args *EchoArgs, reply *EchoReply) {
	e.calls.Add(1)
	reply.X = args.X
	reply.From = e.id
}

func cluster(n int) (*Network, []*echo) {
	net := New(1)
	svcs := make([]*echo, n)
	for i := range svcs {
		svcs[i] = &echo{id: i}
		net.Register(i, svcs[i])
	}
	return net, svcs
}

func TestBasicCall(t *testing.T) {
	net, _ := cluster(2)
	var reply EchoReply
	if !net.End(0, 1).Call("Echo", &EchoArgs{X: 42}, &reply) {
		t.Fatal("call failed on a reliable network")
	}
	if reply.X != 42 || reply.From != 1 {
		t.Fatalf("got %+v, want X=42 From=1", reply)
	}
}

func TestDisconnect(t *testing.T) {
	net, svcs := cluster(2)
	net.Disconnect(1)
	if net.End(0, 1).Call("Echo", &EchoArgs{X: 1}, &EchoReply{}) {
		t.Fatal("call to a disconnected node succeeded")
	}
	if svcs[1].calls.Load() != 0 {
		t.Fatal("disconnected node's handler ran")
	}
	net.Connect(1)
	if !net.End(0, 1).Call("Echo", &EchoArgs{X: 1}, &EchoReply{}) {
		t.Fatal("call failed after reconnecting")
	}
}

func TestPartition(t *testing.T) {
	net, _ := cluster(5)
	net.Partition([]NodeID{0, 1}, []NodeID{2, 3, 4})

	cases := []struct {
		from, to NodeID
		want     bool
	}{
		{0, 1, true},
		{2, 4, true},
		{0, 2, false},
		{4, 1, false},
	}
	for _, c := range cases {
		got := net.End(c.from, c.to).Call("Echo", &EchoArgs{}, &EchoReply{})
		if got != c.want {
			t.Errorf("%d -> %d: got %v, want %v", c.from, c.to, got, c.want)
		}
	}

	net.Heal()
	if !net.End(0, 4).Call("Echo", &EchoArgs{}, &EchoReply{}) {
		t.Fatal("call failed after healing")
	}
}

func TestDropRate(t *testing.T) {
	net, _ := cluster(2)
	net.SetDropRate(0.5)

	const n = 200
	var ok atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if net.End(0, 1).Call("Echo", &EchoArgs{}, &EchoReply{}) {
				ok.Add(1)
			}
		}()
	}
	wg.Wait()

	// Requests and replies are each dropped half the time, so about a quarter
	// of calls should get through.
	if got := ok.Load(); got < n/10 || got > n/2 {
		t.Fatalf("%d of %d calls succeeded, expected about %d", got, n, n/4)
	}
}

func TestRestartDiscardsStaleReplies(t *testing.T) {
	net := New(1)
	slow := &slowEcho{release: make(chan struct{})}
	net.Register(0, &echo{id: 0})
	net.Register(1, slow)

	done := make(chan bool)
	go func() { done <- net.End(0, 1).Call("Echo", &EchoArgs{X: 7}, &EchoReply{}) }()

	time.Sleep(20 * time.Millisecond)
	net.Register(1, &echo{id: 1}) // crash and restart while the handler runs
	close(slow.release)

	if <-done {
		t.Fatal("reply from the pre-restart node was delivered")
	}
}

type slowEcho struct{ release chan struct{} }

func (s *slowEcho) Echo(args *EchoArgs, reply *EchoReply) {
	<-s.release
	reply.X = args.X
}
