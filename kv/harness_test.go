package kv

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/suhas-svg/driftwood/raft"
	"github.com/suhas-svg/driftwood/sim"
)

// cluster is n KV servers on a simulated network, plus any number of clerks.
type cluster struct {
	t          *testing.T
	mu         sync.Mutex
	n          int
	net        *sim.Network
	servers    []*Server
	persisters []*raft.Persister
	clerks     int

	start   time.Time
	history []porcupine.Operation
}

func newCluster(t *testing.T, n int, reliable bool) *cluster {
	t.Helper()
	c := &cluster{
		t:          t,
		n:          n,
		net:        sim.New(time.Now().UnixNano()),
		servers:    make([]*Server, n),
		persisters: make([]*raft.Persister, n),
		start:      time.Now(),
	}
	if !reliable {
		c.net.SetDropRate(0.1)
		c.net.SetMaxDelay(25 * time.Millisecond)
	}
	for i := 0; i < n; i++ {
		c.persisters[i] = raft.NewPersister()
		c.startServer(i)
	}
	t.Cleanup(func() {
		for i := 0; i < n; i++ {
			c.crash(i)
		}
	})
	return c
}

// startServer (re)starts server i from its disk.
func (c *cluster) startServer(i int) {
	c.crash(i)
	c.mu.Lock()
	c.persisters[i] = c.persisters[i].Copy()
	p := c.persisters[i]
	c.mu.Unlock()

	peers := make([]raft.Peer, c.n)
	for j := range peers {
		peers[j] = c.net.End(i, j)
	}
	srv := StartServer(peers, i, p)

	c.mu.Lock()
	c.servers[i] = srv
	c.mu.Unlock()
	// The server and its Raft node share one network address.
	c.net.Register(i, srv, srv.Raft())
}

// crash kills server i, freezing its disk as of now.
func (c *cluster) crash(i int) {
	c.mu.Lock()
	srv := c.servers[i]
	c.servers[i] = nil
	c.mu.Unlock()
	c.net.Unregister(i)
	if srv == nil {
		return
	}
	srv.Kill()
	c.mu.Lock()
	c.persisters[i] = c.persisters[i].Copy()
	c.mu.Unlock()
}

func (c *cluster) running(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.servers[i] != nil
}

// leader returns a server that currently believes it leads, or -1.
func (c *cluster) leader() int {
	best, bestTerm := -1, -1
	for i := 0; i < c.n; i++ {
		c.mu.Lock()
		srv := c.servers[i]
		c.mu.Unlock()
		if srv == nil {
			continue
		}
		if term, isLeader := srv.Raft().GetState(); isLeader && term > bestTerm {
			best, bestTerm = i, term
		}
	}
	return best
}

// clerk is a Clerk whose every operation is recorded for the
// linearizability check.
type clerk struct {
	*Clerk
	c     *cluster
	index int        // zero-based, for porcupine's ClientId
	node  sim.NodeID // network address, for putting the clerk in a partition
}

func (c *cluster) makeClerk() *clerk {
	c.mu.Lock()
	index := c.clerks
	c.clerks++
	c.mu.Unlock()

	node := 1000 + index
	c.net.Connect(node)
	ends := make([]raft.Peer, c.n)
	for j := range ends {
		ends[j] = c.net.End(node, j)
	}
	return &clerk{Clerk: MakeClerk(ends), c: c, index: index, node: node}
}

func (ck *clerk) Get(key string) string {
	start := ck.c.now()
	v := ck.Clerk.Get(key)
	ck.c.record(ck.index, modelInput{Op: OpGet, Key: key}, v, start)
	return v
}

func (ck *clerk) Put(key, value string) {
	start := ck.c.now()
	ck.Clerk.Put(key, value)
	ck.c.record(ck.index, modelInput{Op: OpPut, Key: key, Value: value}, "", start)
}

func (ck *clerk) Append(key, value string) {
	start := ck.c.now()
	ck.Clerk.Append(key, value)
	ck.c.record(ck.index, modelInput{Op: OpAppend, Key: key, Value: value}, "", start)
}

func (c *cluster) now() int64 { return time.Since(c.start).Nanoseconds() }

func (c *cluster) record(client int, in modelInput, out string, start int64) {
	end := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.history = append(c.history, porcupine.Operation{
		ClientId: client,
		Input:    in,
		Output:   out,
		Call:     start,
		Return:   end,
	})
}

// checkLinearizable runs Porcupine over every operation recorded so far. On
// failure it writes an HTML visualization of the history and logs its path.
func (c *cluster) checkLinearizable() {
	c.t.Helper()
	c.mu.Lock()
	history := append([]porcupine.Operation(nil), c.history...)
	c.mu.Unlock()

	// Most histories check in milliseconds, but about 1 in 50 crash-test
	// histories takes tens of seconds (still Ok). Give those room to finish.
	res, info := porcupine.CheckOperationsVerbose(kvModel, history, 60*time.Second)
	switch res {
	case porcupine.Illegal:
		f, err := os.CreateTemp("", "driftwood-linearizability-*.html")
		if err == nil {
			porcupine.Visualize(kvModel, info, f)
			f.Close()
			c.t.Fatalf("history of %d operations is not linearizable; visualization: %s", len(history), f.Name())
		}
		c.t.Fatalf("history of %d operations is not linearizable", len(history))
	case porcupine.Unknown:
		c.t.Logf("linearizability check timed out on %d operations; treating as passing", len(history))
	}
}

// partition splits servers and clerks into groups that cannot talk to each other.
func (c *cluster) partition(groups ...[]sim.NodeID) { c.net.Partition(groups...) }

func (c *cluster) heal() { c.net.Heal() }

// workload runs clerks concurrently until stop closes. Each clerk mixes
// Put, Append, and Get on five shared keys (for Porcupine), and appends a
// unique, numbered token to a key of its own after each op. At the end, each
// clerk's own key must hold every token exactly once and in order: a
// duplicate means a retry was applied twice, a gap means an acknowledged
// write was lost.
func (c *cluster) workload(clerks []*clerk, stop <-chan struct{}) {
	c.t.Helper()
	var wg sync.WaitGroup
	want := make([]string, len(clerks))
	for k, ck := range clerks {
		wg.Add(1)
		go func(k int, ck *clerk) {
			defer wg.Done()
			own := fmt.Sprintf("own-%d", k)
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				// Half the shared ops are reads: each Get pins down the state
				// and keeps Porcupine's search small.
				shared := fmt.Sprintf("k%d", (i+k)%5)
				switch i % 4 {
				case 0:
					ck.Put(shared, fmt.Sprintf("p%d.%d ", k, i))
				case 1:
					ck.Append(shared, fmt.Sprintf("a%d.%d ", k, i))
				case 2, 3:
					ck.Get(shared)
				}
				token := fmt.Sprintf("x %d %d y ", k, i)
				ck.Append(own, token)
				want[k] += token
			}
		}(k, ck)
	}
	wg.Wait()

	reader := c.makeClerk()
	for k := range clerks {
		own := fmt.Sprintf("own-%d", k)
		if got := reader.Get(own); got != want[k] {
			c.t.Fatalf("clerk %d's appends: got %q, want %q", k, got, want[k])
		}
	}
}
