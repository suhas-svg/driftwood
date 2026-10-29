# Driftwood

A Raft consensus implementation and replicated key-value store in Go, built from the paper and tested against a simulated network that drops, delays, reorders, and partitions messages.

## Layout

| Path | What it is |
|---|---|
| `sim/` | Simulated network. Every RPC goes through it, so tests control drops, delays, partitions, and crash/restart. |
| `raft/` | The Raft node. `raft.go` holds state, elections, and the applier. `replicate.go` holds the leader side of replication and commit. `rpc.go` holds the RPC handlers, and `persist.go` holds stable storage. |
| `raft/harness_test.go` | Test cluster: start, crash, and disconnect nodes, check leader and term invariants, and check that every node applies the same command at each index. |
| `raft/election_test.go` | Leader election tests (week 2). |
| `raft/replication_test.go` | Log replication and commit tests (week 3). |
| `raft/persistence_test.go` | Crash/restart and Figure 8 tests (week 4). |

## Running tests

```bash
go test ./sim/                              # network layer, passes today
go test ./raft/ -run Election -count=100    # leader election
go test ./raft/ -count=20                   # everything, about 100s per run
```

One passing run proves little. Raft bugs often show up about once in a few hundred runs, so loop the tests. On Linux/macOS add `-race`; on Windows `-race` needs a C compiler (gcc via MSYS2 or similar).

## Milestones

| Week | Goal | Done when |
|---|---|---|
| 1 | Read the paper, simulated network + RPC layer | `go test ./sim/` passes ✅ |
| 2 | Leader election | `go test ./raft -run Election -count=500` passes ✅ |
| 3 | Log replication + commit | Nodes agree on the log under drops and partitions ✅ |
| 4 | Persistence + crash recovery | Survives random crash/restart tests ✅ |
| 5 | KV service, client dedup, linearizable reads | Porcupine passes on thousands of randomized runs |
| 6 | Snapshots / log compaction | Lagging followers catch up via `InstallSnapshot` |
| 7+ | Real processes over TCP/gRPC, CLI, benchmarks | Kill a node live, writes keep working |

The `TODO(week N)` comments in `raft/` mark where each milestone's code goes, with the matching rules from Figure 2 of the paper.

## References

- Ongaro & Ousterhout, *In Search of an Understandable Consensus Algorithm* (2014). Figure 2 is the spec.
- [raft.github.io](https://raft.github.io): interactive visualization.
- Jon Gjengset, *Students' Guide to Raft*.
- [Porcupine](https://github.com/anishathalye/porcupine): linearizability checker.
