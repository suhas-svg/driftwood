package raft

import (
	"math/rand"
	"sync"
	"testing"
	"time"
)

// Week 3 tests. Every test also runs the apply checker in the harness, which
// fails the test if two nodes ever apply different commands at one index.
//
//	go test ./raft -run 'Agree|Rejoin|Backup|Replication' -count=100

func TestBasicAgree(t *testing.T) {
	c := newCluster(t, 3, true)

	for want := 1; want <= 3; want++ {
		if n, _ := c.nCommitted(want); n > 0 {
			t.Fatalf("index %d committed before Start", want)
		}
		if got := c.one(want*100, 3, false); got != want {
			t.Fatalf("command landed at index %d, want %d", got, want)
		}
	}
}

func TestFollowerFailure(t *testing.T) {
	c := newCluster(t, 3, true)

	c.one(101, 3, false)

	// One follower down: the other two are still a majority.
	leader1 := c.checkOneLeader()
	c.disconnect((leader1 + 1) % 3)
	c.one(102, 2, false)
	time.Sleep(electionTimeoutMax)
	c.one(103, 2, false)

	// Both followers down: the leader alone must not commit anything.
	leader2 := c.checkOneLeader()
	c.disconnect((leader2 + 1) % 3)
	c.disconnect((leader2 + 2) % 3)

	index, _, ok := c.node(leader2).Start(104)
	if !ok {
		t.Fatal("leader rejected Start")
	}
	if index != 4 {
		t.Fatalf("Start returned index %d, want 4", index)
	}
	time.Sleep(2 * electionTimeoutMax)
	if n, _ := c.nCommitted(index); n > 0 {
		t.Fatalf("%d nodes committed without a majority", n)
	}
}

func TestLeaderFailure(t *testing.T) {
	c := newCluster(t, 3, true)

	c.one(101, 3, false)

	leader1 := c.checkOneLeader()
	c.disconnect(leader1)
	c.one(102, 2, false)
	time.Sleep(electionTimeoutMax)
	c.one(103, 2, false)

	// Down to one connected node: nothing can commit.
	leader2 := c.checkOneLeader()
	c.disconnect(leader2)
	for i := 0; i < 3; i++ {
		c.node(i).Start(104)
	}
	time.Sleep(2 * electionTimeoutMax)
	if n, _ := c.nCommitted(4); n > 0 {
		t.Fatalf("%d nodes committed without a majority", n)
	}
}

func TestFailNoAgree(t *testing.T) {
	c := newCluster(t, 5, true)

	c.one(10, 5, false)

	// Three of five followers down: no majority.
	leader := c.checkOneLeader()
	for k := 1; k <= 3; k++ {
		c.disconnect((leader + k) % 5)
	}
	index, _, ok := c.node(leader).Start(20)
	if !ok {
		t.Fatal("leader rejected Start")
	}
	if index != 2 {
		t.Fatalf("Start returned index %d, want 2", index)
	}
	time.Sleep(2 * electionTimeoutMax)
	if n, _ := c.nCommitted(index); n > 0 {
		t.Fatalf("%d nodes committed without a majority", n)
	}

	// Heal. The uncommitted entry may or may not survive, but the cluster
	// must make progress and everyone must agree.
	for k := 1; k <= 3; k++ {
		c.connect((leader + k) % 5)
	}
	leader2 := c.checkOneLeader()
	index2, _, ok := c.node(leader2).Start(30)
	if !ok {
		t.Fatal("new leader rejected Start")
	}
	if index2 < 2 || index2 > 3 {
		t.Fatalf("unexpected index %d", index2)
	}
	c.one(1000, 5, true)
}

func TestConcurrentStarts(t *testing.T) {
	c := newCluster(t, 3, true)

	for attempt := 0; attempt < 5; attempt++ {
		leader := c.checkOneLeader()
		_, term, ok := c.node(leader).Start(1)
		if !ok {
			continue // lost leadership in the meantime; try again
		}

		const n = 5
		var wg sync.WaitGroup
		indexes := make(chan int, n)
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				index, term1, ok := c.node(leader).Start(100 + i)
				if ok && term1 == term {
					indexes <- index
				}
			}(i)
		}
		wg.Wait()
		close(indexes)

		if t1, _ := c.node(leader).GetState(); t1 != term {
			continue // term moved on; the entries may not commit
		}

		got := map[any]bool{}
		for index := range indexes {
			cmd := c.wait(index, 3, term)
			if cmd == nil {
				break
			}
			got[cmd] = true
		}
		if len(got) == n {
			return
		}
		if t1, _ := c.node(leader).GetState(); t1 == term {
			t.Fatalf("only %d of %d concurrent commands committed in a stable term", len(got), n)
		}
	}
	t.Fatal("term changed too often to test concurrent starts")
}

func TestRejoin(t *testing.T) {
	c := newCluster(t, 3, true)

	c.one(101, 3, true)

	// The leader is cut off and accepts entries it can never commit.
	leader1 := c.checkOneLeader()
	c.disconnect(leader1)
	c.node(leader1).Start(102)
	c.node(leader1).Start(103)
	c.node(leader1).Start(104)

	// The other two elect a new leader and commit at index 2.
	c.one(103, 2, true)

	// Swap: the new leader goes away and the old one comes back. The old
	// leader's uncommitted entries must be overwritten, not committed.
	leader2 := c.checkOneLeader()
	c.disconnect(leader2)
	c.connect(leader1)
	c.one(104, 2, true)

	c.connect(leader2)
	c.one(105, 3, true)
}

func TestBackup(t *testing.T) {
	c := newCluster(t, 5, true)

	c.one(rand.Int(), 5, true)

	// Leader plus one follower in a minority. They pile up uncommitted entries.
	leader1 := c.checkOneLeader()
	c.disconnect((leader1 + 2) % 5)
	c.disconnect((leader1 + 3) % 5)
	c.disconnect((leader1 + 4) % 5)
	for i := 0; i < 50; i++ {
		c.node(leader1).Start(rand.Int())
	}
	time.Sleep(electionTimeoutMin / 2)

	// Flip to the other three, who commit 50 entries of their own.
	c.disconnect(leader1)
	c.disconnect((leader1 + 1) % 5)
	c.connect((leader1 + 2) % 5)
	c.connect((leader1 + 3) % 5)
	c.connect((leader1 + 4) % 5)
	for i := 0; i < 50; i++ {
		c.one(rand.Int(), 3, true)
	}

	// The new leader loses a follower and piles up 50 uncommitted entries.
	leader2 := c.checkOneLeader()
	other := (leader1 + 2) % 5
	if leader2 == other {
		other = (leader2 + 1) % 5
	}
	c.disconnect(other)
	for i := 0; i < 50; i++ {
		c.node(leader2).Start(rand.Int())
	}
	time.Sleep(electionTimeoutMin / 2)

	// Bring back the original minority plus the dropped follower. Their
	// logs disagree with each other; the leader must back them up and fix them.
	for i := 0; i < 5; i++ {
		c.disconnect(i)
	}
	c.connect(leader1)
	c.connect((leader1 + 1) % 5)
	c.connect(other)
	for i := 0; i < 50; i++ {
		c.one(rand.Int(), 3, true)
	}

	// Everyone back.
	for i := 0; i < 5; i++ {
		c.connect(i)
	}
	c.one(rand.Int(), 5, true)
}

func TestReplicationUnderPartition(t *testing.T) {
	c := newCluster(t, 5, true)
	c.one(1, 5, false)

	// Put the leader in the minority. Writes there must not commit; the
	// majority side elects a new leader and keeps committing. After healing,
	// the apply checker would catch any divergence.
	leader := c.checkOneLeader()
	minority := []int{leader, (leader + 1) % 5}
	majority := []int{(leader + 2) % 5, (leader + 3) % 5, (leader + 4) % 5}
	c.net.Partition(minority, majority)

	stale, _, _ := c.node(leader).Start(-1)

	for i := 2; i <= 6; i++ {
		c.one(i, 3, true)
	}
	if n, cmd := c.nCommitted(stale); n > 0 && cmd == -1 {
		t.Fatalf("minority leader's entry at index %d committed", stale)
	}

	c.net.Heal()
	c.one(7, 5, true)
	if _, cmd := c.nCommitted(stale); cmd == -1 {
		t.Fatalf("minority leader's entry at index %d survived the heal", stale)
	}
}

func TestUnreliableAgree(t *testing.T) {
	c := newCluster(t, 5, false) // 10% drops plus reordering

	for i := 1; i <= 50; i++ {
		c.one(i, 5, true)
	}
}

func TestUnreliableChurn(t *testing.T) {
	c := newCluster(t, 5, false)

	// Keep a client writing while nodes drop in and out.
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for v := 1000; ; v++ {
			select {
			case <-stop:
				return
			default:
			}
			for i := 0; i < 5; i++ {
				if rf := c.node(i); rf != nil {
					rf.Start(v)
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	for iter := 0; iter < 10; iter++ {
		if rand.Intn(2) == 0 {
			c.disconnect(rand.Intn(5))
		} else {
			c.connect(rand.Intn(5))
		}
		time.Sleep(200 * time.Millisecond)
	}
	close(stop)
	wg.Wait()

	for i := 0; i < 5; i++ {
		c.connect(i)
	}
	c.net.SetDropRate(0)
	c.net.SetMaxDelay(0)
	c.one(-42, 5, true)
}
