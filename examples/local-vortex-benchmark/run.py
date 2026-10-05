#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Run the selective-filter comparison through the guarded native benchmark harness."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--shardloom-binary", type=Path, required=True)
    result.add_argument("--workspace", type=Path, required=True)
    result.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    result.add_argument("--rows", type=int, default=64)
    result.add_argument("--dim-rows", type=int, default=8)
    result.add_argument("--repeats", type=int, default=1)
    result.add_argument(
        "--formats",
        nargs="+",
        choices=("csv", "jsonl", "parquet", "arrow-ipc", "avro", "orc"),
        default=["csv"],
    )
    result.add_argument("--input-state", choices=("raw", "prepared"), default="raw")
    result.add_argument(
        "--output-format",
        choices=("collect", "vortex", "csv", "json", "jsonl", "parquet", "arrow_ipc", "avro", "orc"),
        default="collect",
    )
    result.add_argument("--reference-engine", default="pandas")
    return result


def build_command(args: argparse.Namespace) -> tuple[list[str], Path]:
    repo_root = args.repo_root.resolve()
    command = [
        sys.executable,
        str(repo_root / "benchmarks" / "traditional_analytics" / "run.py"),
        "--shardloom-binary",
        str(args.shardloom_binary),
        "--workspace",
        str(args.workspace),
        "--engines",
        "shardloom",
        "--reference-engine",
        args.reference_engine,
        "--rows",
        str(args.rows),
        "--dim-rows",
        str(args.dim_rows),
        "--repeats",
        str(args.repeats),
        "--formats",
        *args.formats,
        "--scenarios",
        "selective filter",
        "--input-state",
        args.input_state,
        "--output-format",
        args.output_format,
    ]
    return command, repo_root


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    command, repo_root = build_command(args)
    return subprocess.run(command, cwd=repo_root, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
