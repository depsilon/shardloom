#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Summarize the public benchmark surface check for maintainers and agents."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from check_benchmark_publication_claim_gate import (  # noqa: E402
    DEFAULT_MANIFEST,
    SCHEMA_VERSION as CLAIM_GATE_SCHEMA_VERSION,
    validate_publication_claim_gate,
)


SCHEMA_VERSION = "shardloom.benchmark_publish_doctor.v1"
ROUTE_PACKET_SCHEMA_VERSION = "shardloom.benchmark_route_packet.v1"
DEFAULT_OUTPUT = Path("target/benchmark-publish-doctor.json")
NEXT_CHECK_COMMAND = (
    "python3 scripts/check_benchmark_publication_claim_gate.py "
    "--manifest website/assets/benchmarks/latest/manifest.json"
)


def resolve(repo_root: Path, path: Path) -> Path:
    path = Path(path)
    return path if path.is_absolute() else repo_root / path


def build_report(
    repo_root: Path = ROOT,
    *,
    manifest_path: Path = DEFAULT_MANIFEST,
) -> tuple[dict[str, Any], dict[str, Any]]:
    repo_root = Path(repo_root).resolve()
    requested_manifest = resolve(repo_root, manifest_path)
    gate = validate_publication_claim_gate(
        requested_manifest,
        repo_root=repo_root,
    )
    packet = {
        "schema_version": ROUTE_PACKET_SCHEMA_VERSION,
        "status": gate["status"],
        "evidence_class": gate["evidence_class"],
        "manifest": gate["manifest"],
        "retired_mirror_absence_status": gate["retired_mirror_absence_check"]["status"],
        "present_retired_refs": gate["retired_mirror_absence_check"][
            "present_retired_refs"
        ],
        "publication_allowed": False,
        "next_check_command": NEXT_CHECK_COMMAND,
        "blockers": list(gate["blockers"]),
    }
    report = {
        "schema_version": SCHEMA_VERSION,
        "status": gate["status"],
        "manifest": gate["manifest"],
        "public_benchmark_surface": gate["public_benchmark_surface"],
        "public_benchmark_url": gate["public_benchmark_url"],
        "evidence_class": "public_surface_absence_check",
        "retired_mirror_absence_check": gate["retired_mirror_absence_check"],
        "local_manifest_completeness_explanation": gate[
            "local_manifest_completeness_explanation"
        ],
        "claim_gate_status": "not_claim_grade",
        "runtime_execution_performed": False,
        "benchmark_run_performed": False,
        "performance_claim_allowed": False,
        "performance_equivalence_claim_allowed": False,
        "production_claim_allowed": False,
        "superiority_claim_allowed": False,
        "parity_claim_allowed": False,
        "publication_allowed": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "publication_claim_gate_schema_version": CLAIM_GATE_SCHEMA_VERSION,
        "next_check_command": NEXT_CHECK_COMMAND,
        "blockers": list(gate["blockers"]),
    }
    return report, packet


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    report, packet = build_report(
        repo_root,
        manifest_path=args.manifest,
    )
    report["route_packet"] = packet
    serialized = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output is None:
        print(serialized, end="")
    else:
        output = resolve(repo_root, args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(serialized, encoding="utf-8")
        print(output)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
