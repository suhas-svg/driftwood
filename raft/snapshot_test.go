package raft

import (
	"math/rand"
	"testing"
	"time"
)

// Week 6 tests. Nodes snapshot every 10 applied entries, so a follower that
// falls more than a few entries behind can only catch up via InstallSnapshot.
// The apply checker treats a snapshot as applying every index it covers.
//
//	go test ./raft -run Snapshot -count=50

// With snapshots, the persisted Raft state must stay small no matter how
// many entries have committed. A few hundred bytes of log plus term and vote.
const maxSnapshotLogBytes = 2000

// followerWithLog makes a lone follower holding entries 1..n, all in term 1,
// with the first `committed` of them committed.
func followerWithLog(t *testing.T, n, committed int) *Raft {
	t.Helper()
	rf := lonelyNode(t, NewPersister())
	entries := make([]LogEntry, n)
	for i := range entries {
		entries[i] = LogEntry{Term: 1, Command: i + 1}
	}
	var reply AppendEntriesReply
	rf.AppendEntries(&AppendEntriesArgs{Term: 1, LeaderID: 1, Entries: entries, LeaderCommit: committed}, &reply)
	if !reply.Success {
		t.Fatal("setup AppendEntries rejected")
	}
	return rf
}

// A snapshot that arrives late, after the follower has committed past it,
// must be ignored. Installing it would roll back committed state, and the
// service would see entries it already applied a second time. The randomized
// tests rarely delay an InstallSnapshot long enough to hit this.
func TestSnapshotDelayedInstallIgnored(t *testing.T) {
	rf := followerWithLog(t, 20, 20)

	var reply InstallSnapshotReply
	rf.InstallSnapshot(&InstallSnapshotArgs{
		Term: 1, LeaderID: 1, LastIncludedIndex: 15, LastIncludedTerm: 1, Data: []byte("old"),
	}, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// (lastApplied is not checked: the applier advances it in the background.)
	if rf.commitIndex != 20 || rf.snapIndex != 0 || rf.lastLogIndex() != 20 {
		t.Fatalf("delayed snapshot through 15 changed state: commitIndex=%d snapIndex=%d lastLogIndex=%d",
			rf.commitIndex, rf.snapIndex, rf.lastLogIndex())
	}
	if rf.pendingSnapshot {
		t.Fatal("delayed snapshot was queued for the service")
	}
}

// A follower that already holds entries past the snapshot must keep them.
// It may have acknowledged them, so the leader may be counting them toward
// a commit; dropping them would lose committed entries.
func TestSnapshotInstallKeepsMatchingSuffix(t *testing.T) {
	rf := followerWithLog(t, 20, 5)

	var reply InstallSnapshotReply
	rf.InstallSnapshot(&InstallSnapshotArgs{
		Term: 1, LeaderID: 1, LastIncludedIndex: 10, LastIncludedTerm: 1, Data: []byte("snap"),
	}, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.snapIndex != 10 {
		t.Fatalf("snapIndex = %d, want 10", rf.snapIndex)
	}
	if rf.lastLogIndex() != 20 {
		t.Fatalf("lastLogIndex = %d after installing a snapshot through 10, want 20: entries 11-20 matched and were dropped", rf.lastLogIndex())
	}
	if got := rf.entry(15).Command; got != 15 {
		t.Fatalf("entry 15 = %v, want 15", got)
	}
}

// When the follower's entry at the snapshot's last index has a different
// term, its log diverges from the leader's there, and all of it must go.
func TestSnapshotInstallDropsConflictingLog(t *testing.T) {
	rf := followerWithLog(t, 20, 5)

	var reply InstallSnapshotReply
	rf.InstallSnapshot(&InstallSnapshotArgs{
		Term: 2, LeaderID: 2, LastIncludedIndex: 10, LastIncludedTerm: 2, Data: []byte("snap"),
	}, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.snapIndex != 10 || rf.lastLogIndex() != 10 {
		t.Fatalf("snapIndex=%d lastLogIndex=%d, want 10 and 10", rf.snapIndex, rf.lastLogIndex())
	}
}

func TestSnapshotBasic(t *testing.T) {
	c := snapshotCluster(t, 3, true)

	for i := 0; i < 200; i++ {
		c.one(rand.Int(), 3, true)
		if size := c.maxRaftStateSize(); size > maxSnapshotLogBytes {
			t.Fatalf("raft state is %d bytes after %d commands; snapshots are not trimming the log", size, i+1)
		}
	}
}

// A follower that missed many entries comes back after the leader has
// compacted them away. It must catch up through InstallSnapshot.
func TestSnapshotInstall(t *testing.T) {
	c := snapshotCluster(t, 3, true)
	c.one(rand.Int(), 3, true)

	for iter := 0; iter < 5; iter++ {
		leader := c.checkOneLeader()
		victim := (leader + 1 + rand.Intn(2)) % 3
		c.disconnect(victim)
		for i := 0; i < 30; i++ {
			c.one(rand.Int(), 2, true)
		}
		before := c.installedSnapshots(victim)

		c.connect(victim)
		c.one(rand.Int(), 3, true)
		if c.installedSnapshots(victim) == before {
			t.Fatalf("node %d caught up 30 entries without a snapshot; the leader should have compacted them", victim)
		}
	}
}

// Same, but the lagging follower crashes instead of being cut off, so it
// restarts from its own (older) snapshot first.
func TestSnapshotInstallCrash(t *testing.T) {
	c := snapshotCluster(t, 3, true)
	c.one(rand.Int(), 3, true)

	for iter := 0; iter < 5; iter++ {
		leader := c.checkOneLeader()
		victim := (leader + 1 + rand.Intn(2)) % 3
		c.crash(victim)
		for i := 0; i < 30; i++ {
			c.one(rand.Int(), 2, true)
		}
		c.restart(victim)
		c.one(rand.Int(), 3, true)
	}
}

// The whole cluster crashes and restarts from snapshots, repeatedly.
func TestSnapshotAllCrash(t *testing.T) {
	c := snapshotCluster(t, 3, true)
	c.one(rand.Int(), 3, true)

	for iter := 0; iter < 5; iter++ {
		for i := 0; i < 15; i++ {
			c.one(rand.Int(), 3, true)
		}
		index1 := c.one(rand.Int(), 3, true)

		for i := 0; i < 3; i++ {
			c.restart(i)
		}

		// Nothing committed may be lost, and the log must keep growing from
		// where it was.
		index2 := c.one(rand.Int(), 3, true)
		if index2 <= index1 {
			t.Fatalf("index went backward after restart: %d, then %d", index1, index2)
		}
		if size := c.maxRaftStateSize(); size > maxSnapshotLogBytes {
			t.Fatalf("raft state is %d bytes; snapshots are not trimming the log", size)
		}
	}
}

// A follower down for 10,000 entries comes back. With the leader's log long
// since compacted, one InstallSnapshot plus a short tail must catch it up.
func TestSnapshotFarBehind(t *testing.T) {
	if testing.Short() {
		t.Skip("10,000 entries; skipped with -short")
	}
	// Snapshot every 100 entries here: the harness snapshot holds every
	// command so far, and re-encoding 10,000 of them every 10 entries is slow.
	c := newClusterOpts(t, 3, true, 100)
	c.one(0, 3, true)

	leader := c.checkOneLeader()
	victim := (leader + 1) % 3
	c.disconnect(victim)

	// Submit in batches straight to the leader and wait for each batch to
	// commit; one() per entry would take minutes.
	const total, batch = 10000, 200
	last := 0
	for sent := 0; sent < total; sent += batch {
		for k := 0; k < batch; k++ {
			index, _, ok := c.node(leader).Start(sent + k + 1)
			if !ok {
				t.Fatal("leader lost leadership in a quiet cluster")
			}
			last = index
		}
		c.wait(last, 2, -1)
	}
	if size := c.maxRaftStateSize(); size > 20*maxSnapshotLogBytes {
		t.Fatalf("raft state is %d bytes after %d entries", size, total)
	}

	c.connect(victim)
	c.wait(last, 3, -1)
	if c.installedSnapshots(victim) == 0 {
		t.Fatalf("node %d caught up %d entries without a snapshot", victim, total)
	}
}

// Snapshots under message loss, reordering, crashes, and partitions together.
// Delayed InstallSnapshot and AppendEntries RPCs that arrive after newer ones
// must never roll a node back.
func TestSnapshotUnreliableChurn(t *testing.T) {
	const n = 5
	c := snapshotCluster(t, n, false)
	c.one(rand.Int(), 1, true)

	for iter := 0; iter < 10; iter++ {
		victim := rand.Intn(n)
		switch rand.Intn(3) {
		case 0:
			c.crash(victim)
		case 1:
			c.disconnect(victim)
		case 2:
			// Long delays make old snapshots and entries arrive late.
			c.net.SetMaxDelay(time.Duration(25+rand.Intn(75)) * time.Millisecond)
		}

		for i := 0; i < 1+rand.Intn(20); i++ {
			c.one(rand.Int(), n-1, true)
		}

		if !c.running(victim) {
			c.restart(victim)
		}
		c.connect(victim)
		c.net.SetMaxDelay(25 * time.Millisecond)
		c.one(rand.Int(), n, true)
	}
}
