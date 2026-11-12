# Experiments

`experiment.py` drives the cluster, injects the fault, polls `/status` and writes
one directory of CSV per scenario under `scripts/experiments/`. `analysis.ipynb` loads
those CSVs for tables and plots.

```bash
docker compose up -d
scripts/experiment.py baseline     --seeds 10
scripts/experiment.py netem        --delay 50ms --loss 2% --seeds 10
scripts/experiment.py kill-leader  --seeds 10
scripts/experiment.py partition    --seeds 10
scripts/experiment.py sweep        --sizes 3 5 7 --seeds 10
```

Common options: `--clients`, `--ops` (per client), `--poll` (status sampling
period, default 50 ms), `--out` (default `scripts/experiments`), `--probe-ops`,
`--probe-interval`, `--bench-timeout` (a run that cannot reach quorum is aborted
instead of hanging), `--network`, `--n` (bring up and tear down an N-node
cluster, like sweep; default: the `compose.yaml` cluster). Only the Python
standard library is used.

What each scenario does:

* **baseline** — no fault. Measures throughput/latency and counts leader changes
  and term growth over the run.
* **netem** — `tc netem delay/loss` on every node's `eth0` (egress), then the
  same workload; the qdisc is always removed on exit. Each seed also records a
  timeline and a `summary.csv` row with its leader churn.
* **kill-leader** — stops the leader container, benchmarks the surviving
  majority, restarts it and measures replica catch-up.
* **partition** — `docker network disconnect` on the leader, same measurements,
  then reconnects it.
* **sweep** — generates `scripts/experiments/compose-n<N>.yaml` for N = 3/5/7,
  brings each cluster up, discards one warm-up run (cold start), runs the
  workload and tears it down. Run `docker compose down` first: the generated
  clusters use the same host ports (`9000..9000+N-1`).

## Files per scenario

| file | contents |
| --- | --- |
| `runs.csv` | one row per bench run (the `kvbench -csv` columns, prefixed by a tag such as `kill-during`) |
| `timeline-<seed>.csv` | `/status` of every node every `--poll` seconds (baseline uses `timeline.csv`): term, leader, commit/last-log/last-applied, `raft.state` and snapshot bytes |
| `probe-<seed>.csv` | probe writes: `start_ns,end_ns,lat_ms` (kill/partition only) |
| `summary.csv` | derived metrics; one row per seed for kill/partition, aggregates for baseline |
| `compose-n<N>.yaml` | generated compose file (sweep and `--n`) |

`runs.csv` columns:

```
tag,nodes,clients,ops,seed,duration_s,throughput_ops_s,p50_ms,p95_ms,p99_ms,
retry_pct,attempts_per_op,attempts_p99,wrong_leader_pct,linearizable
```

`retry_pct` is the share of operations that needed more than one RPC attempt
(leader moved, node down), `attempts_per_op` the average number of endpoints
tried, `wrong_leader_pct` the share of attempts answered with `421`.

## Derived metrics

* **fault_unix** (ms) — the fault timestamp, taken right before `docker stop` /
  `docker network disconnect`, since the fault is already in effect while the
  command runs.
* **time-to-elect** (ms) — fault timestamp → first `/status` sample where some
  node reports `is_leader` at a term greater than the pre-fault term.
* **TTR-service** (ms) — fault timestamp → first probe write that completes.
  Because the probe is sequential, the write in flight at fault time is the one
  that spans the outage; its completion is when the cluster served again. In
  `kill-leader` this includes the client's HTTP timeout (~2 s), which is the
  honest client-visible recovery time, not just the election time.
* **TTR-replica** (ms) — container restart/reconnect → first poll where the
  returning node's `last_applied >= leader.commit_index`. Compares
  `last_applied` against the leader's `commit_index` (not `last_log_index`),
  because the leader usually has an uncommitted tail.
* **fault_cmd** (ms) — how long `docker stop` / `docker network disconnect`
  took to return. TTR-service only counts probe writes that end after that,
  since writes ending while the command runs were served before the fault.
* **heal_unix** (ms) — when `docker start` / `docker network connect`
  returned; the TTR-replica interval starts at this instant.
* **leader churn** — number of leader changes and `max(term) - min(term)` over
  the baseline run, plus changes/minute.
* **retry cost** — from `runs.csv`: `retry_pct`, `attempts_per_op`,
  `wrong_leader_pct`. These come from counters inside `HTTPEnd`, so they count
  exactly the RPC attempts the client made.

`/status` returns the fields these are computed from:

```json
{"id":1,"term":24,"is_leader":true,"leader_id":1,"commit_index":570,
 "last_log_index":572,"last_applied":570,"raft_state_bytes":2515,"snapshot_bytes":0}
```

## Probe mode

`kvbench -probe` writes one key at a time and logs absolute timestamps, so the
samples can be joined with the fault time. The experiment script uses it to
measure TTR-service.

```bash
go run ./cmd/kvbench -probe -probe-log /tmp/probe.csv \
  -peers localhost:9000,localhost:9001,localhost:9002 -ops 500 -probe-interval 20ms
```

## Reading the results

* Repeat each configuration over several seeds (`--seeds 10`) and report mean ±
  standard deviation for throughput plus median + IQR for p99; a single p99
  sample is noise.
* Throughput excludes the linearizability check (it is measured before the
  checker runs), so it is comparable across runs.
* `linearizable=ok` on every run is the correctness claim; a violation aborts
  `kvbench` with a non-zero exit code.

`analysis.ipynb` loads everything under `scripts/experiments/` and produces:

* a mean/std table per scenario and tag (`runs.csv`);
* throughput with error bars and latency percentiles;
* the TTR table and bars (`summary.csv`);
* term/leader timeline and probe latency with the fault marked.

It needs `pandas` and `matplotlib` (`pip install pandas matplotlib`), plus
Jupyter to run the notebook.

## Known limitation seen by `partition`

A node that stays isolated keeps calling elections, so its term grows without
bound; when it heals it rejects the healthy leader's `AppendEntries` (stale
term) and forces repeated re-elections — the *disruptive server* problem from
the Raft thesis (§4.2.3). This implementation has no PreVote, so the healed node
can stay out and the cluster keeps churning. `kill-leader` does not have this
behaviour because a stopped process does not campaign. If `partition` leaves the
cluster unstable, `docker compose restart` heals it; the scenario prints a
warning when that happens and restarts the cluster by itself before the next
seed, so each TTR sample starts from a converged cluster. Adding PreVote (a vote phase that does not increment
the term) is the standard fix and a good "future work" item for the report.

## Fault injection by hand

```bash
# find the leader, then kill that container; another node takes over
for p in 9000 9001 9002; do echo "$p $(curl -s localhost:$p/status)"; done
docker stop kvraft-node2

# bring it back; it catches up from the persisted log/snapshot
docker start kvraft-node2

# network delay / loss inside a node (needs iproute2, already in the image)
docker exec kvraft-node0 tc qdisc add dev eth0 root netem delay 50ms loss 5%
docker exec kvraft-node0 tc qdisc del dev eth0 root
```
