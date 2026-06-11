#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Merge Python unittest shard evidence into the stable CI evidence contract."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from run_python_test_shard import (  # noqa: E402
    SCHEMA_VERSION as SHARD_SCHEMA_VERSION,
)
from run_python_test_shard import (  # noqa: E402
    SHARD_ORDER,
    discover_test_modules,
)


SCHEMA_VERSION = "shardloom.python_test_evidence.v2"
DEFAULT_SHARD_ROOT = ROOT / "target" / "python-test-shards"
DEFAULT_OUTPUT = ROOT / "target" / "python-test-evidence.json"
COMPILE_COMMAND = (
    "python -m compileall -q python/src python/tests scripts examples "
    "benchmarks/traditional_analytics"
)
REPLACED_LOGICAL_COMMAND = "python -m unittest discover -s python/tests"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--shard-root", type=Path, default=DEFAULT_SHARD_ROOT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def shard_evidence_paths(shard_root: Path) -> list[Path]:
    return sorted(shard_root.rglob("*.json"))


def build_report(repo_root: Path, shard_root: Path) -> dict[str, Any]:
    blockers: list[str] = []
    shard_payloads: list[dict[str, Any]] = []
    by_shard: dict[str, dict[str, Any]] = {}
    for path in shard_evidence_paths(shard_root):
        payload = load_json(path)
        shard_payloads.append(payload)
        shard_id = str(payload.get("shard_id") or "")
        if shard_id in by_shard:
            blockers.append(f"duplicate Python test shard evidence for {shard_id}")
        if shard_id:
            by_shard[shard_id] = payload

    for shard in SHARD_ORDER:
        if shard not in by_shard:
            blockers.append(f"missing Python test shard evidence for {shard}")

    observed_modules: list[str] = []
    total_tests = 0
    total_skipped = 0
    total_elapsed = 0.0
    for shard in SHARD_ORDER:
        payload = by_shard.get(shard)
        if payload is None:
            continue
        if payload.get("schema_version") != SHARD_SCHEMA_VERSION:
            blockers.append(f"{shard}: schema_version mismatch")
        if payload.get("status") != "passed":
            blockers.append(f"{shard}: status={payload.get('status', 'missing')}")
        for field in ("fallback_attempted", "external_engine_invoked"):
            if payload.get(field) is not False:
                blockers.append(f"{shard}: {field} must be false")
        modules = payload.get("modules")
        if not isinstance(modules, list) or not modules:
            blockers.append(f"{shard}: modules must be a non-empty list")
            modules = []
        observed_modules.extend(str(module) for module in modules)
        total_tests += int(payload.get("test_count") or 0)
        total_skipped += int(payload.get("skipped_count") or 0)
        total_elapsed += float(payload.get("elapsed_seconds") or 0.0)

    duplicated_modules = sorted(
        module for module in set(observed_modules) if observed_modules.count(module) > 1
    )
    if duplicated_modules:
        blockers.append(
            "Python test shard modules duplicated: " + ", ".join(duplicated_modules)
        )

    expected_modules = {
        f"python.tests.{stem}" for stem in discover_test_modules(repo_root)
    }
    observed_module_set = set(observed_modules)
    missing_modules = sorted(expected_modules - observed_module_set)
    unexpected_modules = sorted(observed_module_set - expected_modules)
    if missing_modules:
        blockers.append("Python test shard modules missing: " + ", ".join(missing_modules))
    if unexpected_modules:
        blockers.append(
            "Python test shard modules unexpected: " + ", ".join(unexpected_modules)
        )

    passed = not blockers
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if passed else "failed",
        "commands": [
            f"python scripts/run_python_test_shard.py --shard {shard}"
            for shard in SHARD_ORDER
        ]
        + [COMPILE_COMMAND],
        "replaced_logical_command": REPLACED_LOGICAL_COMMAND,
        "coverage_equivalent_to_discover": not missing_modules and not unexpected_modules,
        "shard_count": len(SHARD_ORDER),
        "shards": [
            {
                "shard_id": shard,
                "status": by_shard.get(shard, {}).get("status", "missing"),
                "module_count": by_shard.get(shard, {}).get("module_count", 0),
                "test_count": by_shard.get(shard, {}).get("test_count", 0),
                "elapsed_seconds": by_shard.get(shard, {}).get("elapsed_seconds", 0),
            }
            for shard in SHARD_ORDER
        ],
        "module_count": len(observed_module_set),
        "test_count": total_tests,
        "skipped_count": total_skipped,
        "shard_elapsed_seconds_sum": round(total_elapsed, 3),
        "blockers": blockers,
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output = resolve(repo_root, args.output)
    report = build_report(repo_root, resolve(repo_root, args.shard_root))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(output)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
