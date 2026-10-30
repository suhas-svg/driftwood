# Driftwood

A Raft consensus implementation and replicated key-value store in Go, built from the paper and tested against a simulated network that drops, delays, reorders, and partitions messages.

## Layout

| Path | What it is |
|---|---|
| `sim/` | Simulated network. Every RPC goes through it, so tests control drops, delays, partitions, and crash/restart. |
| `raft/` | The Raft node. `raft.go` holds state, elections, and the applier. `replicate.go` holds the leader side of replication and commit. `rpc.go` holds the RPC handlers, `persist.go` holds stable storage, and `snapshot.go` holds log compaction and `InstallSnapshot`. |
| `raft/harness_test.go` | Test cluster: start, crash, and disconnect nodes, check leader and term invariants, and check that every node applies the same command at each index. |
| `raft/election_test.go` | Leader election tests (week 2). |
| `raft/replication_test.go` | Log replication and commit tests (week 3). |
| `raft/persistence_test.go` | Crash/restart and Figure 8 tests (week 4). |
| `raft/snapshot_test.go` | Snapshot and `InstallSnapshot` tests, including a follower 10,000 entries behind (week 6). |
| `kv/` | The key-value store on top of Raft. `server.go` is the replicated state machine with duplicate detection, `client.go` is the clerk that finds the leader and retries. |
| `kv/harness_test.go` | KV test cluster. Records every client operation with start and end times and checks the history with Porcupine. |
| `kv/kv_test.go` | KV tests under drops, partitions, crashes, and a stale leader (week 5). |
| `kv/snapshot_test.go` | KV tests with snapshots: bounded log size, restarts from snapshot, a lagging server (week 6). |

## How the KV store stays correct

- **Every operation goes through the Raft log, reads included.** A leader cut off by a partition still thinks it leads, but it can't commit anything, so it can't answer a read from stale data. (ReadIndex would make reads cheaper; it's a stretch goal.)
- **Exactly-once writes.** Each clerk has a random ID and numbers its requests. A retry reuses the number, and the server applies a Put or Append only if its number is higher than the last one applied for that clerk. The dedup table is rebuilt from the log, so it survives restarts.
- **No false successes.** A server answers a client only if the entry committed at the index it was given is that client's request. If a new leader overwrote it, the client is told to retry elsewhere.
- **Snapshots carry the dedup table.** When Raft's saved state passes `maxRaftState` bytes, the server snapshots its map and its per-client sequence numbers, and Raft discards the log the snapshot covers. Without the sequence numbers, a retry of a request the snapshot already includes would be applied twice.

## Testing notes

- **History timestamps are a counter, not a clock.** On Windows, Go's clock only advances about every 0.6ms. Fast operations from one client then share start and end times, and Porcupine has to treat them as concurrent, which turned easy histories into searches that never finished. A counter records the real order of calls and returns exactly. A Porcupine timeout now fails the test instead of passing silently.
- **Snapshots wait for the log to grow.** A follower that falls behind holds a backlog of unapplied entries that no snapshot can remove. A server snapshots only once its Raft state has grown by half the limit since its last snapshot. Otherwise it would snapshot on every applied entry, rewriting the whole backlog each time.

## Running tests

```bash
go test ./sim/                              # network layer
go test ./raft/ -run Election -count=100    # leader election
go test ./raft/ -count=20                   # all of Raft, about 3 minutes per run
go test ./kv/ -count=20                     # KV store with linearizability checks, about 1 minute per run
go test ./raft/ -short                      # skips the 10,000-entry snapshot test
```

When Porcupine finds a history that isn't linearizable, the test writes an HTML visualization to your temp directory and prints its path.

One passing run proves little. Raft bugs often show up about once in a few hundred runs, so loop the tests. On Linux/macOS add `-race`; on Windows `-race` needs a C compiler (gcc via MSYS2 or similar).

## Milestones

| Week | Goal | Done when |
|---|---|---|
| 1 | Read the paper, simulated network + RPC layer | `go test ./sim/` passes ✅ |
| 2 | Leader election | `go test ./raft -run Election -count=500` passes ✅ |
| 3 | Log replication + commit | Nodes agree on the log under drops and partitions ✅ |
| 4 | Persistence + crash recovery | Survives random crash/restart tests ✅ |
| 5 | KV service, client dedup, linearizable reads | Porcupine passes on thousands of randomized runs. Built ✅; 104 of 104 full-suite runs so far |
| 6 | Snapshots / log compaction | Lagging followers catch up via `InstallSnapshot` ✅ |
| 7+ | Real processes over TCP/gRPC, CLI, benchmarks | Kill a node live, writes keep working |

The `TODO(week N)` comments in `raft/` mark where each milestone's code goes, with the matching rules from Figure 2 of the paper.

## References

- Ongaro & Ousterhout, *In Search of an Understandable Consensus Algorithm* (2014). Figure 2 is the spec.
- [raft.github.io](https://raft.github.io): interactive visualization.
- Jon Gjengset, *Students' Guide to Raft*.
- [Porcupine](https://github.com/anishathalye/porcupine): linearizability checker.
