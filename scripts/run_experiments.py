#!/usr/bin/env python3
"""Run every experiment configuration used in the report and check the timelines.

Replaces the results under scripts/experiments/.
"""

import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "scripts" / "experiments"


def quiet_execute(*cmd: str) -> str:
    """Return the command's output, ignoring failures."""
    r = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    return r.stdout.strip()


def cpu_model() -> str:
    for line in Path("/proc/cpuinfo").read_text().splitlines():
        if line.startswith("model name"):
            return line.split(":", 1)[1].strip()
    return "unknown"


def reset_cluster() -> None:
    quiet_execute("docker", "compose", "down", "-v")
    quiet_execute("docker", "compose", "up", "-d")
    quiet_execute("go", "run", "./cmd/kvbench")  # discard cold-start run


def rename_netem(name: str) -> None:
    if (OUT / "netem").exists():
        (OUT / "netem").rename(OUT / name)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--seeds", default="10")
    p.add_argument("--ops", default="200", help="operations per client")
    p.add_argument("--pause", type=float, default=60, help="seconds between scenarios")
    args = p.parse_args()

    if sys.platform != "linux":
        sys.exit("run under Linux or WSL (the driver needs POSIX signals)")
    if subprocess.run(["docker", "info"], capture_output=True).returncode != 0:
        sys.exit("docker is not reachable (is Docker Desktop running?)")

    experiment: list[str] = [sys.executable, "scripts/experiment.py", "--seeds", args.seeds,
                             "--ops", args.ops, "--bench-timeout", "300"]
    failed: list[str] = []

    def run(label: str, *scenario: str) -> None:
        print(f"== {label}", flush=True)
        if subprocess.run(experiment + list(scenario), cwd=ROOT).returncode != 0:
            failed.append(label)
        time.sleep(args.pause)

    OUT.mkdir(parents=True, exist_ok=True)
    for entry in OUT.iterdir():
        if entry.is_dir():
            shutil.rmtree(entry)
    (OUT / "environment.txt").write_text("\n".join([
        quiet_execute("date", "-Iseconds"),
        f"CPU {cpu_model()}, {os.cpu_count()} threads",
        quiet_execute("free", "-h"),
        quiet_execute("uname", "-sr"),
        quiet_execute("docker", "version", "--format", "Docker {{.Server.Version}}"),
        quiet_execute("docker", "compose", "version"),
        quiet_execute("go", "version"),
        f"Python {sys.version.split()[0]}",
        f"seeds={args.seeds} ops/client={args.ops} pause={args.pause:g}s",
    ]) + "\n")

    # the baseline is level 0 of both netem series
    reset_cluster()
    run("baseline", "baseline")
    for d in ("50ms", "100ms"):
        reset_cluster()
        run(f"netem delay {d}", "netem", "--delay", d, "--loss", "0%")
        rename_netem(f"netem-d{d}")
    for loss in ("2", "5"):
        reset_cluster()
        run(f"netem loss {loss}%", "netem", "--delay", "0ms", "--loss", f"{loss}%")
        rename_netem(f"netem-l{loss}pct")

    # --n and sweep bring up their own clusters
    quiet_execute("docker", "compose", "down", "-v")
    for n in ("3", "5", "7"):
        run(f"kill-leader N={n}", "kill-leader", "--n", n)
        run(f"partition N={n}", "partition", "--n", n)
    run("sweep", "sweep", "--sizes", "3", "5", "7")

    print("== safety check", flush=True)
    if subprocess.run([sys.executable, "scripts/verify_timelines.py", str(OUT)], cwd=ROOT).returncode != 0:
        failed.append("verify_timelines")

    if failed:
        sys.exit("FAILED: " + ", ".join(failed))
    print("all scenarios finished")


if __name__ == "__main__":
    main()
