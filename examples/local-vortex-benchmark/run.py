#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_ENGINES = "shardloom,shardloom-prepared-vortex"
DEFAULT_FORMATS = "csv"
DEFAULT_RUN_ROOT = Path("target/local-vortex-benchmark")


def default_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}-pid{os.getpid()}"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a small ShardLoom local benchmark smoke.")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--rows", type=int, default=256)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument(
        "--run-id",
        help="Run identifier used for isolated output/data paths. Defaults to UTC timestamp plus pid.",
    )
    parser.add_argument(
        "--run-root",
        type=Path,
        default=DEFAULT_RUN_ROOT,
        help="Directory that receives per-run benchmark smoke artifacts.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Optional generated data directory. Defaults to <run-root>/<run-id>/data.",
    )
    parser.add_argument(
        "--formats",
        default=DEFAULT_FORMATS,
        help="Comma-separated formats for the smoke. Defaults to csv so no optional Parquet dependency is required.",
    )
    parser.add_argument(
        "--engines",
        default=DEFAULT_ENGINES,
        help=(
            "Comma-separated ShardLoom lanes for the smoke. Defaults to compatibility import plus "
            "prepared Vortex so users see the current runtime-development lane without external baselines."
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output JSON path. Defaults to <run-root>/<run-id>/smoke.json.",
    )
    parser.add_argument(
        "--no-regenerate",
        action="store_false",
        dest="regenerate",
        help="Reuse an existing generated data directory instead of rebuilding this run's data.",
    )
    parser.set_defaults(regenerate=True)
    return parser.parse_args(argv)


def resolve_under_repo(repo_root: Path, path: Path) -> Path:
    candidate = path if path.is_absolute() else repo_root / path
    return candidate.resolve()


def validate_run_id(run_id: str) -> None:
    if not run_id or Path(run_id).name != run_id or run_id in {".", ".."}:
        raise ValueError("--run-id must be a single path segment")


def build_run_context(args: argparse.Namespace) -> dict[str, object]:
    repo_root = args.repo_root.resolve()
    run_id = args.run_id or default_run_id()
    validate_run_id(run_id)
    run_root = resolve_under_repo(repo_root, args.run_root)
    run_dir = run_root / run_id
    data_dir = resolve_under_repo(repo_root, args.data_dir) if args.data_dir else run_dir / "data"
    output = resolve_under_repo(repo_root, args.output) if args.output else run_dir / "smoke.json"
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
        "--data-dir",
        str(data_dir),
        "--output",
        str(output),
    ]
    if args.regenerate:
        command.append("--regenerate")
    return {
        "repo_root": repo_root,
        "run_id": run_id,
        "run_dir": run_dir,
        "data_dir": data_dir,
        "output": output,
        "command": command,
    }


class RunLock:
    def __init__(self, run_dir: Path) -> None:
        self.run_dir = run_dir
        self.lock_path = run_dir / ".shardloom-local-vortex-benchmark.lock"
        self.fd: int | None = None

    def __enter__(self) -> "RunLock":
        self.run_dir.mkdir(parents=True, exist_ok=True)
        try:
            self.fd = os.open(
                self.lock_path,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            )
        except FileExistsError as exc:
            raise RuntimeError(
                f"local benchmark run directory is already locked: {self.lock_path}"
            ) from exc
        os.write(self.fd, f"pid={os.getpid()}\n".encode("utf-8"))
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
        try:
            self.lock_path.unlink()
        except FileNotFoundError:
            pass


def main() -> int:
    args = parse_args()
    try:
        context = build_run_context(args)
        with RunLock(context["run_dir"]):
            return subprocess.run(
                context["command"],
                cwd=context["repo_root"],
                check=False,
            ).returncode
    except (RuntimeError, ValueError) as exc:
        print(f"local-vortex-benchmark safety error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
