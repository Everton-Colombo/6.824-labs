#!/usr/bin/env python3
"""Reproducible experiments against the docker cluster.

Scenarios
    baseline                 no faults; leader churn + throughput/latency
    netem                    delay/loss on every node
    kill-leader              stop the leader, bench the majority, restart, wait catch-up
    partition                disconnect the leader, bench the majority, reconnect, wait catch-up
    sweep                    repeat baseline for N = 3, 5, 7 nodes

Environment must already be up (`docker compose up -d`) except for `sweep`,
which builds and tears down its own clusters.

Output per scenario, under scripts/experiments/<scenario>/:
    runs.csv       one row per bench run (throughput, percentiles, retries, linearizable)
    timeline.csv   /status polled every --poll seconds (leader churn, indexes, sizes)
    probe.csv      per-write timestamps from the availability probe (kill/partition only)
    summary.csv    derived metrics: time-to-elect, TTR-service, TTR-replica, churn

Only the Python standard library is used.
"""

import argparse
import csv
import json
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

TIMELINE_COLS = [
    "t_ms", "node", "term", "is_leader", "leader_id",
    "commit_index", "last_log_index", "last_applied",
    "raft_state_bytes", "snapshot_bytes",
]
RUNS_COLS = [
    "tag", "nodes", "clients", "ops", "seed", "duration_s", "throughput_ops_s",
    "p50_ms", "p95_ms", "p99_ms", "retry_pct", "attempts_per_op", "attempts_p99",
    "wrong_leader_pct", "linearizable",
]


def now_ms() -> int:
    return int(time.time() * 1000)


class Cluster:
    def __init__(self, ports, nodes, host="localhost", compose_file=None):
        self.ports = list(ports)
        self.nodes = list(nodes)
        self.host = host
        self.compose_file = compose_file

    @property
    def peers(self) -> str:
        return ",".join(f"{self.host}:{p}" for p in self.ports)

    def status(self, port: int, timeout: float = 0.5) -> dict | None:
        try:
            with urllib.request.urlopen(f"http://localhost:{port}/status", timeout=timeout) as r:
                return json.load(r)
        except (urllib.error.URLError, OSError, json.JSONDecodeError):
            return None

    def leader(self) -> int | None:
        for port in self.ports:
            st = self.status(port)
            if st and st.get("is_leader"):
                return port
        return None

    def wait_leader(self, timeout: float = 20.0) -> int | None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            port = self.leader()
            if port:
                return port
            time.sleep(0.1)
        return None

    def wait_stable(self, timeout: float = 15.0) -> bool:
        """All nodes reachable, same term, same (real) leader."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            states = [self.status(p) for p in self.ports]
            if all(states):
                terms = {st["term"] for st in states}
                leaders = {st["leader_id"] for st in states}
                if len(terms) == 1 and len(leaders) == 1 and -1 not in leaders:
                    return True
            time.sleep(0.1)
        return False

    def majority(self, exclude: int) -> str:
        return ",".join(f"{self.host}:{p}" for p in self.ports if p != exclude)

    def compose(self, *args, check: bool = True):
        cmd = ["docker", "compose"]
        if self.compose_file:
            cmd += ["-f", str(self.compose_file)]
        cmd += list(args)
        return subprocess.run(cmd, cwd=ROOT, check=check,
                              stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)


def node_index(port: int) -> int:
    return port - 9000


class Experiment:
    def __init__(self, args, cluster: Cluster, name: str):
        self.args = args
        self.c = cluster
        self.name = name
        self.dir = ROOT / args.out / name
        self.dir.mkdir(parents=True, exist_ok=True)
        self.runs_path = self.dir / "runs.csv"
        with self.runs_path.open("w", newline="") as f:
            csv.writer(f).writerow(RUNS_COLS)

    # ---- load -------------------------------------------------------------

    def bench(self, tag: str, peers: str, seed: int, record: bool = True) -> None:
        cmd = [
            "go", "run", "./cmd/kvbench", "-csv",
            "-peers", peers,
            "-clients", str(self.args.clients),
            "-ops", str(self.args.ops),
            "-seed", str(seed),
        ]
        out = subprocess.run(cmd, cwd=ROOT, check=True, capture_output=True, text=True,
                             timeout=self.args.bench_timeout).stdout.strip()
        if record:
            with self.runs_path.open("a", newline="") as f:
                csv.writer(f).writerow([tag] + out.split(","))
            print(f"  {tag} seed={seed}: {out}")

    def seeds(self, tag: str, peers: str) -> None:
        for seed in range(1, self.args.seeds + 1):
            self.bench(tag, peers, seed)

    # ---- state polling ----------------------------------------------------

    def start_timeline(self, path: Path) -> tuple[threading.Event, threading.Thread]:
        stop = threading.Event()

        def loop():
            with path.open("w", newline="") as f:
                w = csv.writer(f)
                w.writerow(TIMELINE_COLS)
                while not stop.is_set():
                    t = now_ms()
                    for i, port in enumerate(self.c.ports):
                        st = self.c.status(port)
                        if not st:
                            continue
                        w.writerow([
                            t, i, st.get("term"),
                            str(bool(st.get("is_leader"))).lower(),
                            st.get("leader_id"),
                            st.get("commit_index"), st.get("last_log_index"),
                            st.get("last_applied"), st.get("raft_state_bytes"),
                            st.get("snapshot_bytes"),
                        ])
                    f.flush()
                    stop.wait(self.args.poll)

        thread = threading.Thread(target=loop, daemon=True)
        thread.start()
        return stop, thread

    # ---- summary helpers --------------------------------------------------

    def timeline_rows(self, path: Path) -> list[dict]:
        with path.open(newline="") as f:
            return list(csv.DictReader(f))

    def leader_changes(self, path: Path) -> int:
        changes, last = 0, None
        for row in self.timeline_rows(path):
            if row["is_leader"] != "true":
                continue
            if last is not None and row["node"] != last:
                changes += 1
            last = row["node"]
        return changes

    def duration_minutes(self, path: Path) -> float:
        rows = self.timeline_rows(path)
        if len(rows) < 2:
            return 0.0
        return (int(rows[-1]["t_ms"]) - int(rows[0]["t_ms"])) / 60000

    def term_growth(self, path: Path) -> int:
        terms = [int(r["term"]) for r in self.timeline_rows(path) if r["term"]]
        return max(terms) - min(terms) if terms else 0

    def time_to_elect(self, path: Path, fault_ms: int, pre_term: int) -> int | None:
        for row in self.timeline_rows(path):
            if int(row["t_ms"]) > fault_ms and int(row["term"]) > pre_term \
                    and row["is_leader"] == "true":
                return int(row["t_ms"]) - fault_ms
        return None

    # ---- scenarios --------------------------------------------------------

    def baseline(self) -> None:
        timeline = self.dir / "timeline.csv"
        stop, thread = self.start_timeline(timeline)
        time.sleep(0.3)
        try:
            self.seeds("baseline", self.c.peers)
        finally:
            stop.set()
            thread.join()
        changes = self.leader_changes(timeline)
        minutes = self.duration_minutes(timeline)
        per_min = changes / minutes if minutes else 0.0
        growth = self.term_growth(timeline)
        with (self.dir / "summary.csv").open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["leader_changes", "leader_changes_per_min", "term_growth"])
            w.writerow([changes, f"{per_min:.2f}", growth])
        print(f"  leader changes: {changes} ({per_min:.2f}/min), term growth: {growth}")

    def netem(self) -> None:
        delay, loss = self.args.delay, self.args.loss

        def tc(*tc_args):
            for node in self.c.nodes:
                subprocess.run(["docker", "exec", node, "tc", *tc_args],
                               check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        tc("qdisc", "add", "dev", "eth0", "root", "netem", "delay", delay, "loss", loss)
        try:
            self.seeds(f"netem-{delay}-{loss}", self.c.peers)
        finally:
            tc("qdisc", "del", "dev", "eth0", "root")

    def leader_fault(self, kind: str) -> None:
        if self.args.probe_ops <= 0:
            sys.exit("--probe-ops must be > 0")
        rows = []
        for seed in range(1, self.args.seeds + 1):
            rows.append(self.one_fault(kind, seed))
        with (self.dir / "summary.csv").open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["seed", "fault_unix_ms", "time_to_elect_ms", "ttr_service_ms",
                        "ttr_replica_ms", "pre_term", "new_term", "converged"])
            w.writerows(rows)
        print("  " + ",".join(["seed", "t_elect", "ttr_service", "ttr_replica", "converged"]))
        for r in rows:
            print(f"  {r[0]},{r[2]},{r[3]},{r[4]},{r[7]}")

    def heal(self, timeout: float = 30.0) -> bool:
        """Restart the cluster if the previous fault left it churning."""
        if self.c.wait_stable(timeout=5):
            return True
        print("  restarting cluster to clear election churn", file=sys.stderr)
        self.c.compose("restart")
        if self.c.wait_stable(timeout=15):
            return True
        print("  still churning: recreating the cluster", file=sys.stderr)
        self.c.compose("down", "-v")
        self.c.compose("up", "-d")
        return self.c.wait_stable(timeout=timeout)

    def one_fault(self, kind: str, seed: int) -> list:
        if not self.heal():
            sys.exit("cluster did not converge before the fault")
        subprocess.run(["pkill", "-f", "kvbench -probe"], check=False,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        leader = self.c.wait_leader()
        if leader is None:
            sys.exit("no leader")
        idx = node_index(leader)
        pre_term = (self.c.status(leader) or {}).get("term", -1)

        probe_path = self.dir / f"probe-{seed}.csv"
        timeline = self.dir / f"timeline-{seed}.csv"
        probe = subprocess.Popen(
            ["go", "run", "./cmd/kvbench", "-probe", "-probe-log", str(probe_path),
             "-peers", self.c.peers, "-ops", str(self.args.probe_ops),
             "-probe-interval", self.args.probe_interval],
            cwd=ROOT, stdout=subprocess.DEVNULL, start_new_session=True,
        )
        stop, thread = self.start_timeline(timeline)
        time.sleep(1.0)

        if kind == "kill":
            subprocess.run(["docker", "stop", self.c.nodes[idx]], check=True,
                           stdout=subprocess.DEVNULL)
        else:
            subprocess.run(["docker", "network", "disconnect", self.args.network,
                            self.c.nodes[idx]], check=True, stdout=subprocess.DEVNULL)
        fault_ms = now_ms()

        if self.c.wait_leader() is None:
            print("  warning: no leader within 20s", file=sys.stderr)
        self.bench(f"{kind}-during", self.c.majority(leader), seed)

        # the probe's in-flight write only completes once the cluster serves
        # again; give it time to log that completion before killing it
        self.wait_probe_recovery(probe_path, fault_ms, timeout=15)
        os.killpg(os.getpgid(probe.pid), signal.SIGTERM)
        try:
            probe.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(os.getpgid(probe.pid), signal.SIGKILL)
            probe.wait(timeout=5)
        stop.set()
        thread.join()

        t_elect = self.time_to_elect(timeline, fault_ms, pre_term)
        t_service = self.time_to_service(probe_path, fault_ms)

        if kind == "kill":
            subprocess.run(["docker", "start", self.c.nodes[idx]], check=True,
                           stdout=subprocess.DEVNULL)
        else:
            subprocess.run(["docker", "network", "connect", self.args.network,
                            self.c.nodes[idx]], check=True, stdout=subprocess.DEVNULL)
        restart_ms = now_ms()
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline and self.c.status(leader) is None:
            time.sleep(0.1)

        t_replica = self.wait_catchup(leader, restart_ms, timeout=60)
        converged = self.c.wait_stable(timeout=5)
        self.bench(f"{kind}-after", self.c.peers, seed)

        leader_now = self.c.wait_leader(timeout=5) or leader
        new_term = (self.c.status(leader_now) or {}).get("term", -1)
        print(f"  seed={seed}: t_elect={t_elect}ms ttr_service={t_service}ms "
              f"ttr_replica={t_replica}ms term {pre_term}->{new_term}")
        if not converged and kind == "partition":
            print("  warning: cluster did not converge after healing.\n"
                  "  A node isolated long enough keeps campaigning and returns with an\n"
                  "  inflated term (no PreVote in this Raft), so it exiles itself and the\n"
                  "  remaining nodes keep re-electing. `docker compose restart` heals it.\n"
                  "  Use `kill-leader` if you need a scenario that always converges.",
                  file=sys.stderr)
        return [seed, fault_ms,
                t_elect if t_elect is not None else "na",
                t_service if t_service is not None else "na",
                t_replica, pre_term, new_term,
                str(converged).lower()]

    def wait_probe_recovery(self, probe_path: Path, fault_ms: int, timeout: float) -> bool:
        """Wait until the probe logged a write that finished after the fault."""
        fault_ns = fault_ms * 1_000_000
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                with probe_path.open(newline="") as f:
                    for row in csv.DictReader(f):
                        if int(row["end_ns"]) > fault_ns:
                            return True
            except FileNotFoundError:
                pass
            time.sleep(0.05)
        return False

    def time_to_service(self, probe_path: Path, fault_ms: int) -> int | None:
        """When the probe next completed a write after the fault.

        The probe is sequential, so the first completion after the fault is the
        moment the cluster was serving writes again (the op in flight at fault
        time is the one that spans the outage).
        """
        fault_ns = fault_ms * 1_000_000
        first_end = None
        with probe_path.open(newline="") as f:
            for row in csv.DictReader(f):
                end = int(row["end_ns"])
                if end > fault_ns and (first_end is None or end < first_end):
                    first_end = end
        return (first_end - fault_ns) // 1_000_000 if first_end else None

    def wait_catchup(self, node_port: int, restart_ms: int, timeout: float) -> int:
        """ms until the restarted node has applied everything the leader committed."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            leader = self.c.leader()
            if leader:
                commit = (self.c.status(leader) or {}).get("commit_index")
                applied = (self.c.status(node_port) or {}).get("last_applied")
                if commit is not None and applied is not None and applied >= commit:
                    return now_ms() - restart_ms
            time.sleep(0.05)
        return -1


def compose_yaml(n: int) -> str:
    peers = ",".join(f"kvraft-n{n}-node{i}:8000" for i in range(n))
    out = [f"name: mc714-n{n}", "", "services:"]
    for i in range(n):
        out += [
            f"  node{i}:",
            "    build:",
            f"      context: {ROOT}",
            f"    container_name: kvraft-n{n}-node{i}",
            "    environment:",
            f'      NODE_ID: "{i}"',
            f'      PEERS: "{peers}"',
            '      RAFT_ADDR: ":8000"',
            '      HTTP_ADDR: ":9000"',
            '      DATA_DIR: "/data"',
            '      MAX_RAFT_STATE: "4096"',
            "    ports:",
            f'      - "{9000 + i}:9000"',
            "    volumes:",
            f"      - node{i}-data:/data",
            "    cap_add:",
            "      - NET_ADMIN",
            "    networks:",
            "      - raftnet",
            "",
        ]
    out += ["volumes:"]
    out += [f"  node{i}-data:" for i in range(n)]
    out += ["", "networks:", "  raftnet:", "    driver: bridge", ""]
    return "\n".join(out)


def run_sweep(args) -> None:
    for n in args.sizes:
        print(f"== N={n}")
        compose_path = ROOT / args.out / f"compose-n{n}.yaml"
        compose_path.parent.mkdir(parents=True, exist_ok=True)
        compose_path.write_text(compose_yaml(n))
        c = Cluster(ports=[9000 + i for i in range(n)],
                    nodes=[f"kvraft-n{n}-node{i}" for i in range(n)],
                    compose_file=compose_path)
        exp = Experiment(args, c, f"sweep-n{n}")
        c.compose("up", "-d", "--build")
        try:
            if not c.wait_stable(timeout=30):
                sys.exit(f"cluster did not converge for N={n}")
            exp.bench("warmup", c.peers, 0, record=False)  # discard cold-start run
            exp.seeds(f"n{n}", c.peers)
        finally:
            c.compose("down", "-v", check=False)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("scenario", choices=["baseline", "netem", "kill-leader", "partition", "sweep"])
    p.add_argument("--seeds", type=int, default=3)
    p.add_argument("--clients", type=int, default=8)
    p.add_argument("--ops", type=int, default=40)
    p.add_argument("--poll", type=float, default=0.05, help="status poll interval (s)")
    p.add_argument("--out", default="scripts/experiments")
    p.add_argument("--n", type=int, default=3, help="cluster size for non-sweep runs")
    p.add_argument("--compose-file", default="compose.yaml")
    p.add_argument("--network", default="mc714_raftnet")
    p.add_argument("--delay", default="50ms")
    p.add_argument("--loss", default="2%")
    p.add_argument("--probe-ops", type=int, default=800, help="probe writes per fault run")
    p.add_argument("--probe-interval", default="20ms")
    p.add_argument("--bench-timeout", type=float, default=120.0,
                   help="abort a bench run that cannot reach quorum (s)")
    p.add_argument("--sizes", type=int, nargs="+", default=[3, 5, 7])
    args = p.parse_args()

    if args.scenario == "sweep":
        run_sweep(args)
        return

    c = Cluster(ports=[9000 + i for i in range(args.n)],
                nodes=[f"kvraft-node{i}" for i in range(args.n)],
                compose_file=args.compose_file)
    if c.wait_leader(timeout=5) is None:
        sys.exit("cluster not reachable (docker compose up -d)")
    if not c.wait_stable():
        sys.exit("cluster did not converge (all nodes, same term/leader)")

    exp = Experiment(args, c, args.scenario)
    if args.scenario == "baseline":
        exp.baseline()
    elif args.scenario == "netem":
        exp.netem()
    elif args.scenario == "kill-leader":
        exp.leader_fault("kill")
    elif args.scenario == "partition":
        exp.leader_fault("partition")


if __name__ == "__main__":
    main()
