#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a small ShardLoom local benchmark smoke.")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--rows", type=int, default=256)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument(
        "--formats",
        default="csv",
        help="Comma-separated formats for the smoke. Defaults to csv so no optional Parquet dependency is required.",
    )
    parser.add_argument(
        "--engines",
        default="shardloom,shardloom-prepared-vortex",
        help=(
            "Comma-separated ShardLoom lanes for the smoke. Defaults to compatibility import plus "
            "prepared Vortex so users see the current runtime-development lane without external baselines."
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/shardloom-local-vortex-benchmark-smoke.json"),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output = args.output if args.output.is_absolute() else repo_root / args.output
    command = [
        sys.executable,
        str(repo_root / "benchmarks" / "traditional_analytics" / "run.py"),
        "--engines",
        args.engines,
        "--formats",
        args.formats,
        "--scenario",
        "selective filter",
        "--dataset-profile",
        "tiny_smoke",
        "--rows",
        str(args.rows),
        "--iterations",
        str(args.iterations),
        "--shardloom-build-profile",
        "debug",
        "--shardloom-result-sink",
        "--skip-shardloom-native",
        "--no-markdown",
        "--output",
        str(output),
        "--regenerate",
    ]
    return subprocess.run(command, cwd=repo_root, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
