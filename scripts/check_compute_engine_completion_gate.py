#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Check repository scope and current public-native evidence before completion claims.

This read-only gate does not infer support from published rows or status strings.
It verifies retained benchmark values and requires every current workload/format.
Passing does not authorize publication or a performance superiority claim.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from check_benchmark_artifact_completeness import result_rows, validate_manifest
from benchmarks.traditional_analytics.benchmark_models import FORMAT_ORDER
from benchmarks.traditional_analytics.workloads import WORKLOADS
from check_release_architecture_tracker import runtime_gap_family_burn_down_blockers
from check_runtime_gap_family_burn_down import build_report as build_runtime_gap_report

SCHEMA_VERSION = "shardloom.compute_engine_completion_gate.v2"


def unchecked_markdown_items(text: str) -> list[dict[str, Any]]:
    items = []
    for number, line in enumerate(text.splitlines(), 1):
        match = re.match(r"^\s*-\s+\[\s\]\s+(.+?)\s*$", line)
        if match:
            items.append({"line": number, "title": match.group(1)})
    return items


def benchmark_completion_report(path: Path | None) -> dict[str, Any]:
    payload = {}
    blockers = []
    if path is None:
        blockers.append("a current public-native benchmark report must be supplied")
    else:
        try:
            blockers, payload = validate_manifest(path)
        except (OSError, ValueError) as error:
            blockers = [str(error)]
    configuration = payload.get("configuration", {}) if isinstance(payload, dict) else {}
    configuration = configuration if isinstance(configuration, dict) else {}
    scenarios = configuration.get("scenarios", [])
    formats = configuration.get("formats", [])
    # Coverage is separate from artifact validity: a valid small probe cannot
    # establish completion of the repository's declared workload scope.
    if not isinstance(scenarios, list) or any(not isinstance(item, str) for item in scenarios):
        scenarios = []
    if not isinstance(formats, list) or any(not isinstance(item, str) for item in formats):
        formats = []
    missing_scenarios = sorted(set(WORKLOADS) - set(scenarios))
    missing_formats = sorted(set(FORMAT_ORDER) - set(formats))
    if missing_scenarios:
        blockers.append(f"benchmark omits {len(missing_scenarios)} required workloads")
    if missing_formats:
        blockers.append(f"benchmark omits required formats: {', '.join(missing_formats)}")
    rows = result_rows(payload)
    return {
        "status": "blocked" if blockers else "passed",
        "path": str(path) if path is not None else None,
        "recorded_case_count": len(rows),
        "native_case_count": sum(isinstance(row, dict) and row.get("engine") == "shardloom" for row in rows),
        "missing_workloads": missing_scenarios,
        "missing_formats": missing_formats,
        "blockers": blockers,
        "benchmark_run_performed": False,
        "performance_claim_allowed": False,
    }


def build_report(
    *,
    benchmark_results: Path | None,
    phase_plan: Path,
    global_review: Path,
    repo_root: Path | None = None,
    runtime_gap_family_burn_down_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    benchmark = benchmark_completion_report(benchmark_results)
    phase_unchecked = unchecked_markdown_items(phase_plan.read_text(encoding="utf-8"))
    review_unchecked = unchecked_markdown_items(global_review.read_text(encoding="utf-8"))
    mapping_blockers = []
    mapping_status = "no_unchecked_global_review_rows"
    if review_unchecked:
        if runtime_gap_family_burn_down_report is None and repo_root is not None:
            runtime_gap_family_burn_down_report = build_runtime_gap_report(repo_root)
        if runtime_gap_family_burn_down_report is None:
            mapping_blockers.append("missing runtime gap family burn-down report")
        else:
            mapping_blockers = runtime_gap_family_burn_down_blockers(
                runtime_gap_family_burn_down_report,
                expected_global_unchecked_count=len(review_unchecked),
            )
        mapping_status = ("blocked_unmapped_or_invalid" if mapping_blockers
                          else "mapped_to_runtime_gap_family_claim_boundaries")
    blockers = list(benchmark["blockers"])
    if phase_unchecked:
        blockers.append(f"phase plan still has unchecked items: {len(phase_unchecked)}")
    blockers.extend(mapping_blockers)
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "blocked" if blockers else "passed",
        "blockers": blockers,
        "phase_plan_unchecked_count": len(phase_unchecked),
        "phase_plan_unchecked_items": phase_unchecked,
        "global_review_unchecked_count": len(review_unchecked),
        "global_review_unchecked_items": review_unchecked,
        "global_review_mapping_status": mapping_status,
        "global_review_unchecked_rows_block_completion": bool(review_unchecked and mapping_blockers),
        "runtime_gap_family_burn_down_blocker_count": len(mapping_blockers),
        "runtime_gap_family_burn_down_blockers": mapping_blockers,
        "benchmark_evidence": benchmark,
        "completion_claim_allowed": not blockers,
        "claim_boundary": "declared repository scope and verified workload comparison only",
        "performance_claim_allowed": False,
        "publication_allowed": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--benchmark-results", type=Path, help="Current public-native runner report")
    parser.add_argument("--phase-plan", type=Path, default=Path("docs/architecture/phased-execution-plan.md"))
    parser.add_argument("--global-review", type=Path, default=Path("docs/architecture/global-architecture-review.md"))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--allow-incomplete", action="store_true", help="Inspect blockers with exit zero; never changes claim eligibility")
    args = parser.parse_args()
    root = args.repo_root.resolve()

    def resolve(path):
        return path if path is None or path.is_absolute() else root / path

    report = build_report(benchmark_results=resolve(args.benchmark_results),
                          phase_plan=resolve(args.phase_plan), global_review=resolve(args.global_review),
                          repo_root=root)
    text = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        target = resolve(args.output)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text + "\n", encoding="utf-8")
    else:
        print(text)
    return int(report["status"] != "passed" and not args.allow_incomplete)


if __name__ == "__main__":
    raise SystemExit(main())
