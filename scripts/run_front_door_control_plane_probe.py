#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Measure ShardLoom front-door/control-plane overhead.

This probe is intentionally small and local. It does not run benchmarks or make
performance claims; it separates import, binary resolution, transport startup,
dispatch, envelope parsing, and metadata-first fingerprint timings so regressions
do not get mistaken for Vortex operator cost.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "target" / "front-door-control-plane-probe.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--shardloom-bin")
    return parser.parse_args()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def timed(call: Callable[[], Any]) -> tuple[Any, float]:
    started = time.perf_counter()
    result = call()
    return result, (time.perf_counter() - started) * 1000.0


def stats(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {"count": 0, "min_ms": None, "median_ms": None, "max_ms": None}
    return {
        "count": len(values),
        "min_ms": round(min(values), 6),
        "median_ms": round(statistics.median(values), 6),
        "max_ms": round(max(values), 6),
    }


def run_import_probe(repo_root: Path, iterations: int) -> dict[str, Any]:
    command = [
        sys.executable,
        "-c",
        "import time; t=time.perf_counter(); import shardloom as sl; print((time.perf_counter()-t)*1000)",
    ]
    env = dict(os.environ)
    env["PYTHONPATH"] = str(repo_root / "python" / "src")
    values: list[float] = []
    for _ in range(iterations):
        completed = subprocess.run(
            command,
            cwd=repo_root,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
        values.append(float(completed.stdout.strip()))
    return {
        "status": "measured",
        "iterations": iterations,
        "timing": stats(values),
    }


def run_client_probe(repo_root: Path, iterations: int, shardloom_bin: str | None) -> dict[str, Any]:
    sys.path.insert(0, str(repo_root / "python" / "src"))
    import shardloom as sl  # noqa: PLC0415
    from shardloom.client import ShardLoomClient  # noqa: PLC0415
    from shardloom.session import _fingerprint_file  # noqa: PLC0415

    source = repo_root / "target" / "front-door-control-plane-probe-source.csv"
    source.parent.mkdir(parents=True, exist_ok=True)
    if not source.exists():
        source.write_text("id,label\n1,alpha\n2,beta\n", encoding="utf-8")

    binary_resolution: list[float] = []
    metadata_fingerprint: list[float] = []
    subprocess_status: list[float] = []
    worker_status: list[float] = []
    worker_fields: dict[str, str | None] = {}
    subprocess_fields: dict[str, str | None] = {}
    binary = shardloom_bin

    client = ShardLoomClient(binary=binary)
    for _ in range(iterations):
        _, elapsed = timed(lambda: client.binary_command())
        binary_resolution.append(elapsed)
    client.close()

    for _ in range(iterations):
        fingerprint, elapsed = timed(lambda: _fingerprint_file(source))
        metadata_fingerprint.append(elapsed)
        if fingerprint.content_digest is not None:
            raise AssertionError("metadata-first fingerprint unexpectedly computed content_digest")

    one_shot = ShardLoomClient(binary=binary, use_persistent_worker=False)
    try:
        for _ in range(iterations):
            envelope, elapsed = timed(lambda: one_shot.status())
            subprocess_status.append(elapsed)
            subprocess_fields = {
                "command": envelope.command,
                "fallback_attempted": str(envelope.fallback.attempted).lower(),
                "external_engine_invoked": envelope.field("external_engine_invoked"),
            }
    finally:
        one_shot.close()

    worker = ShardLoomClient(binary=binary, use_persistent_worker=True)
    try:
        for _ in range(iterations):
            envelope, elapsed = timed(lambda: worker.status())
            worker_status.append(elapsed)
            worker_fields = {
                "command": envelope.command,
                "fallback_attempted": str(envelope.fallback.attempted).lower(),
                "external_engine_invoked": envelope.field("external_engine_invoked"),
            }
    finally:
        worker.close()

    context, context_ms = timed(lambda: sl.context(repo_root=repo_root))
    context_client = getattr(context, "client", None)
    if context_client is not None:
        context_client.close()

    return {
        "status": "measured",
        "binary_command": list(client.binary_command()),
        "context_construct_ms": round(context_ms, 6),
        "binary_resolution": stats(binary_resolution),
        "metadata_fingerprint": stats(metadata_fingerprint),
        "fresh_subprocess_status": stats(subprocess_status),
        "persistent_worker_status": stats(worker_status),
        "subprocess_fields": subprocess_fields,
        "worker_fields": worker_fields,
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    iterations = max(1, args.iterations)
    payload = {
        "schema_version": "shardloom.front_door_control_plane_probe.v1",
        "generated_at_utc": now_iso(),
        "repo_root": str(repo_root),
        "iterations": iterations,
        "benchmark_run_performed": False,
        "performance_claim_allowed": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "import_probe": run_import_probe(repo_root, iterations),
        "client_probe": run_client_probe(repo_root, iterations, args.shardloom_bin),
        "claim_boundary": (
            "Local control-plane overhead probe only; timings separate Python/CLI transport "
            "costs from Vortex runtime and do not authorize benchmark or superiority claims."
        ),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
