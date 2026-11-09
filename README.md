# MC714 — Trabalho 2: Raft + KV-Raft

Raft (leader election, log replication, persistence, snapshots) and a linearizable
key/value service on top of it, packaged as independent containers.

Based on the MIT 6.824 labs 2A–3B skeleton (`labrpc`, `labgob`, `porcupine`).

## Layout

```
src/raft/       Raft implementation + labrpc test suite
src/kvraft/     linearizable KV service, Clerk, HTTP API, labrpc test suite
src/labrpc/     in-memory simulated RPC (tests) + TCP/HTTP clients (runtime)
src/labgob/     gob wrapper that rejects lowercase RPC fields
src/porcupine/  linearizability checker
cmd/kvnode/     container entrypoint (Raft peer RPC + KV HTTP API)
cmd/kvbench/    workload generator, latency/throughput + linearizability check
scripts/        experiments and result analysis (see scripts/README.md)
compose.yaml    3-node cluster
```

## Run the cluster

```bash
docker compose up --build -d
docker compose ps
```

Each node exposes:

* `:8000` — Raft peer-to-peer RPC (`RequestVote`, `AppendEntries`, `InstallSnapshot`), internal to the compose network.
* `:9000` — KV HTTP API, published on the host as `9000`, `9001`, `9002`.

## Talk to it

`kvraft.Clerk` is client-side: it sends the request to the server it currently
believes is the leader, and on `ErrWrongLeader` it moves to the next server and
retries. `kvbench` uses `Clerk`, so it finds the leader by itself.

Raw curl has no such retry loop, so it must be aimed at the current leader.
Followers answer `421` + `{"Err":"ErrWrongLeader"}`:

```bash
# which node is the leader?
for p in 9000 9001 9002; do echo "$p $(curl -s localhost:$p/status)"; done

# then, against that port (here 9002)
curl -s -X PUT localhost:9002/kv/course -d 'MC714'
curl -s -X POST localhost:9002/kv/course -d '-2s2026'   # append
curl -s localhost:9002/kv/course
```

Endpoints: `GET /status`, `GET|PUT|POST /kv/{key}`.
Optional `X-Client-Id` / `X-Seq` headers carry the duplicate-detection identity;
without them each request is treated as a fresh client.

## Benchmark

```bash
go run ./cmd/kvbench -peers localhost:9000,localhost:9001,localhost:9002 -clients 8 -ops 50
```

Flags: `-keys`, `-write-ratio`, `-seed`, `-linearizable=false`, `-csv`, `-dump`,
`-run-id`. Each run uses its own key namespace, so repeated runs against a
cluster with persistent volumes are still compared against an empty state.

## Experiments

Fault injection, the metrics that go with it, and the CSV layout are documented
in [`scripts/README.md`](scripts/README.md). Results are summarised and plotted
in [`scripts/analysis.ipynb`](scripts/analysis.ipynb).

## Tests (no Docker required)

```bash
go test ./src/raft           # 2A/2B/2C, simulated labrpc network
go test ./src/kvraft         # 3A/3B, plus TestHTTPAndTCPCluster (real sockets)
```

## Run a node without Docker

`kvnode` is configured by environment variables:

| var | default | meaning |
| --- | --- | --- |
| `NODE_ID` | `0` | this node's index into `PEERS` |
| `PEERS` | `localhost:8000` | comma-separated Raft addresses of every node, in index order |
| `RAFT_ADDR` | `:8000` | Raft peer RPC listen address |
| `HTTP_ADDR` | `:9000` | KV HTTP API listen address |
| `DATA_DIR` | `/tmp/opencode/kvraft-node-<id>` | where `raft.state` / `kv.snap` are written |
| `MAX_RAFT_STATE` | `4096` | snapshot threshold in bytes (`-1` disables snapshots) |

## Reset state

```bash
docker compose down -v
```
