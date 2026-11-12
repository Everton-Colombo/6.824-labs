#!/usr/bin/env python3
"""Check Raft safety properties on the timelines written by experiment.py.

Reads every timeline*.csv under the given directories and checks:
    election safety     at most one node reports is_leader for each term
    term monotonicity   a node's term never decreases, even across restarts
                        (currentTerm is persisted)

Prints one line per file and exits with status 1 if any property is violated.
"""

import argparse
import csv
import sys
from pathlib import Path


def check(path: Path) -> list[str]:
    leaders: dict[int, set[str]] = {}
    last_term: dict[str, int] = {}
    errors: list[str] = []
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            node, term = row["node"], int(row["term"])
            if row["is_leader"] == "true":
                leaders.setdefault(term, set()).add(node)
            if term < last_term.get(node, term):
                errors.append(f"node {node} term went back {last_term[node]} -> {term} "
                              f"at t_ms={row['t_ms']}")
            last_term[node] = term
    for term, nodes in sorted(leaders.items()):
        if len(nodes) > 1:
            errors.append(f"term {term} has {len(nodes)} leaders: nodes {sorted(nodes)}")
    return errors


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("dirs", nargs="*", type=Path,
                   default=[Path(__file__).parent / "experiments"],
                   help="directories to search (default: scripts/experiments)")
    args = p.parse_args()
    files: list[Path] = []
    for d in args.dirs:
        files += d.rglob("timeline*.csv")
    files.sort()
    if not files:
        sys.exit("no timeline*.csv found")
    failed = 0
    for path in files:
        errors = check(path)
        if not errors:
            print(f"ok   {path}")
            continue
        failed += 1
        print(f"FAIL {path}")
        for e in errors:
            print(f"     {e}")
    print(f"{len(files) - failed}/{len(files)} timelines ok")
    if failed:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
