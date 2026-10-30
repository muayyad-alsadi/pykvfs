"""Prints the Go-vs-Python comparison as a markdown table from bench/results/*.jsonl.

One column per implementation in IMPLS, for each of round trips, commands and median µs.
Run: python3 bench/report.py > bench/results/report.md
"""

import json
import sys
from pathlib import Path

RESULTS = Path(__file__).resolve().parent / "results"
ORDER = [
    "stat", "readdir", "readdirplus", "create", "mkdir", "write64", "read64",
    "rename_file", "rename_dir", "unlink", "rmdir_empty", "truncate",
]
# (column label, results file)
IMPLS = [("Go", "go.jsonl"), ("Py lua", "python-lua.jsonl"), ("Py multi-exec", "python-multi-exec.jsonl")]
METRICS = [("round trips", "rt", "{:g}"), ("commands", "cmds", "{:g}"), ("median µs", "median_us", "{:.0f}")]


def load(name: str) -> dict:
    rows = {}
    for line in (RESULTS / name).read_text().splitlines():
        row = json.loads(line)
        rows[(row["op"], row["n"])] = row
    return rows


def main() -> None:
    runs = [load(name) for _, name in IMPLS]
    header = [f"{metric} {label}" for metric, _, _ in METRICS for label, _ in IMPLS]
    print("| operation | N | " + " | ".join(header) + " |")
    print("|---|---:|" + "---:|" * len(header))
    for op in ORDER:
        for n in (10, 1000, 10000):
            cells = [fmt.format(run[(op, n)][field]) for _, field, fmt in METRICS for run in runs]
            print(f"| {op} | {n} | " + " | ".join(cells) + " |")
    return None


if __name__ == "__main__":
    sys.exit(main())
