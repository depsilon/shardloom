#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Run one CI Python unittest shard and emit machine-readable evidence."""

from __future__ import annotations

import argparse
import json
import sys
import time
import unittest
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
PYTHON_SRC = ROOT / "python" / "src"
TEST_ROOT = ROOT / "python" / "tests"
SCHEMA_VERSION = "shardloom.python_test_shard_evidence.v1"
DEFAULT_OUTPUT_DIR = ROOT / "target" / "python-test-shards"
SHARDS: dict[str, tuple[str, ...]] = {
    "release_scripts": ("test_release_scripts",),
    "front_door_benchmark_publication": ("test_front_door_benchmark_publication",),
}
SHARD_ORDER = ("core", "front_door_benchmark_publication", "release_scripts")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--shard",
        required=True,
        choices=SHARD_ORDER,
        help="Shard id to run.",
    )
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def discover_test_modules(repo_root: Path) -> list[str]:
    test_root = repo_root / "python" / "tests"
    return [path.stem for path in sorted(test_root.glob("test_*.py"))]


def module_stems_for_shard(shard: str, repo_root: Path = ROOT) -> list[str]:
    if shard == "core":
        slow_module_stems = {stem for stems in SHARDS.values() for stem in stems}
        return [
            stem
            for stem in discover_test_modules(repo_root)
            if stem not in slow_module_stems
        ]
    return list(SHARDS[shard])


def module_names_for_shard(shard: str, repo_root: Path = ROOT) -> list[str]:
    return [f"python.tests.{stem}" for stem in module_stems_for_shard(shard, repo_root)]


def ensure_import_paths(repo_root: Path) -> None:
    for path in (repo_root / "python" / "src", repo_root):
        path_text = str(path)
        if path_text not in sys.path:
            sys.path.insert(0, path_text)


def run_unittest_modules(modules: list[str]) -> tuple[unittest.TestResult, float, int]:
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromNames(modules)
    test_count = suite.countTestCases()
    started = time.perf_counter()
    result = unittest.TextTestRunner(verbosity=1).run(suite)
    elapsed = time.perf_counter() - started
    return result, elapsed, test_count


def write_evidence(
    *,
    output_path: Path,
    shard: str,
    modules: list[str],
    test_count: int,
    elapsed_seconds: float,
    result: unittest.TestResult,
) -> dict[str, Any]:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if result.wasSuccessful() else "failed",
        "shard_id": shard,
        "module_count": len(modules),
        "test_count": test_count,
        "modules": modules,
        "command": f"python scripts/run_python_test_shard.py --shard {shard}",
        "elapsed_seconds": round(elapsed_seconds, 3),
        "failure_count": len(result.failures),
        "error_count": len(result.errors),
        "skipped_count": len(result.skipped),
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    ensure_import_paths(repo_root)
    modules = module_names_for_shard(args.shard, repo_root)
    result, elapsed_seconds, test_count = run_unittest_modules(modules)
    output_dir = args.output_dir if args.output_dir.is_absolute() else repo_root / args.output_dir
    payload = write_evidence(
        output_path=output_dir / f"{args.shard}.json",
        shard=args.shard,
        modules=modules,
        test_count=test_count,
        elapsed_seconds=elapsed_seconds,
        result=result,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
