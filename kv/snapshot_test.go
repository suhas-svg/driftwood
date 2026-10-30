package kv

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/suhas-svg/driftwood/sim"
)

// Week 6 tests: servers snapshot once their Raft state reaches 1000 bytes,
// so the log is trimmed every few dozen operations.
//
//	go test ./kv -run Snapshot -count=50

const snapshotThreshold = 1000

// A server restored from a snapshot must still recognize a retry of a request
// the snapshot already covers. If the dedup table were left out, the retry
// would be applied twice. The workload tests catch this only when a retry
// happens to straddle a snapshot, so check it directly.
func TestKVSnapshotKeepsDedup(t *testing.T) {
	newState := func() *Server {
		return &Server{data: map[string]string{}, lastSeq: map[int64]int64{}}
	}
	op := Op{Kind: OpAppend, Key: "k", Value: "x", ClientID: 7, Seq: 1}

	before := newState()
	before.applyOp(op)
	before.lastApplied = 1

	after := newState()
	after.restoreSnapshot(before.encodeSnapshot())
	after.applyOp(op) // the client's retry, applied after the snapshot

	if got := after.data["k"]; got != "x" {
		t.Fatalf("k = %q after a retry following snapshot restore, want \"x\"", got)
	}
}

func TestKVSnapshotSize(t *testing.T) {
	c := newSnapshotCluster(t, 3, true, snapshotThreshold)
	ck := c.makeClerk()

	for i := 0; i < 300; i++ {
		ck.Append("k", fmt.Sprintf("%d ", i))
		ck.Put(fmt.Sprintf("key%d", i%10), fmt.Sprint(i))
		c.checkLogSize()
	}
	// Snapshots must keep the data, not just shrink the log.
	want := ""
	for i := 0; i < 300; i++ {
		want += fmt.Sprintf("%d ", i)
	}
	if got := ck.Get("k"); got != want {
		t.Fatalf("Get(k) after snapshots: got %d bytes, want %d", len(got), len(want))
	}
	c.checkLinearizable()
}

// Every server restarts from its own snapshot plus the log after it.
func TestKVSnapshotRestartAll(t *testing.T) {
	c := newSnapshotCluster(t, 3, true, snapshotThreshold)
	ck := c.makeClerk()

	want := ""
	for round := 0; round < 4; round++ {
		for i := 0; i < 40; i++ {
			v := fmt.Sprintf("%d.%d ", round, i)
			ck.Append("k", v)
			want += v
		}
		for i := 0; i < 3; i++ {
			c.startServer(i)
		}
		if got := ck.Get("k"); got != want {
			t.Fatalf("after restart %d: Get(k) has %d bytes, want %d", round, len(got), len(want))
		}
		c.checkLogSize()
	}
	c.checkLinearizable()
}

// A server cut off for hundreds of operations comes back after the others
// have compacted their logs, so it can only catch up from a snapshot. Then
// another server is cut off, leaving the returning server as the only way to
// form a majority: if its state is wrong, reads will show it.
func TestKVSnapshotLaggingServer(t *testing.T) {
	c := newSnapshotCluster(t, 3, true, snapshotThreshold)
	ck := c.makeClerk()
	ck.Put("k", "")

	lagging := 0
	c.partition([]sim.NodeID{lagging}, []sim.NodeID{1, 2, ck.node})
	want := ""
	for i := 0; i < 200; i++ {
		v := fmt.Sprintf("%d ", i)
		ck.Append("k", v)
		want += v
	}

	c.heal()
	ck.Append("k", "end ")
	want += "end "
	// Give the returning server time to receive the snapshot.
	time.Sleep(time.Second)

	c.partition([]sim.NodeID{lagging, 1, ck.node}, []sim.NodeID{2})
	if got := ck.Get("k"); got != want {
		t.Fatalf("Get(k) served by {0, 1}: got %d bytes, want %d", len(got), len(want))
	}
	ck.Append("k", "more ")
	c.heal()
	c.checkLogSize()
	c.checkLinearizable()
}

// The crash/restart workload, with snapshots: restarted servers restore their
// dedup table from the snapshot, so retries are still applied once.
func TestKVSnapshotCrashRestart(t *testing.T) {
	c := newSnapshotCluster(t, 5, true, snapshotThreshold)
	clerks := make([]*clerk, 4)
	for i := range clerks {
		clerks[i] = c.makeClerk()
	}

	stop := make(chan struct{})
	go func() {
		defer close(stop)
		for iter := 0; iter < 8; iter++ {
			time.Sleep(time.Duration(300+rand.Intn(300)) * time.Millisecond)
			c.crash(rand.Intn(5))
			c.crash(rand.Intn(5))
			time.Sleep(time.Duration(200+rand.Intn(300)) * time.Millisecond)
			for i := 0; i < 5; i++ {
				if !c.running(i) {
					c.startServer(i)
				}
			}
		}
	}()
	c.workload(clerks, stop)
	c.checkLogSize()
	c.checkLinearizable()
}

// Partitions, crashes, and message loss with snapshots: lagging servers keep
// getting InstallSnapshot while clients retry through leader changes.
func TestKVSnapshotPartitionsAndCrashes(t *testing.T) {
	c := newSnapshotCluster(t, 5, false, snapshotThreshold)
	clerks := make([]*clerk, 4)
	for i := range clerks {
		clerks[i] = c.makeClerk()
	}

	stop := make(chan struct{})
	go func() {
		for iter := 0; iter < 8; iter++ {
			time.Sleep(time.Duration(200+rand.Intn(400)) * time.Millisecond)
			if rand.Intn(2) == 0 {
				nodes := rand.Perm(5)
				majority := []sim.NodeID{nodes[2], nodes[3], nodes[4]}
				for _, ck := range clerks {
					majority = append(majority, ck.node)
				}
				c.partition([]sim.NodeID{nodes[0], nodes[1]}, majority)
			} else {
				c.crash(rand.Intn(5))
			}
			time.Sleep(time.Duration(300+rand.Intn(400)) * time.Millisecond)
			c.heal()
			for i := 0; i < 5; i++ {
				if !c.running(i) {
					c.startServer(i)
				}
			}
		}
		close(stop)
	}()
	c.workload(clerks, stop)
	c.checkLogSize()
	c.checkLinearizable()
}
