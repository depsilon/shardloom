#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Block full compute-engine completion claims while current evidence still has gaps."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from check_benchmark_artifact_completeness import result_rows as benchmark_result_rows
from check_release_architecture_tracker import runtime_gap_family_burn_down_blockers
from check_runtime_gap_family_burn_down import (
    build_report as build_runtime_gap_family_burn_down_report,
)


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.compute_engine_completion_gate.v1"

BLOCKING_STATUS_MARKERS = (
    "blocked",
    "unsupported",
    "not_claim_grade",
    "fixture_smoke_only",
    "report_only",
)
NON_BLOCKING_STATUS_VALUES = {
    "not_required_not_claim_grade",
}
IGNORED_STATUS_FIELD_SUFFIXES = (
    "_status_vocabulary",
    "_claim_boundary",
    "_boundary",
)
SHARDLOOM_READY_STATUS_FIELDS = {
    "status": "success",
    "claim_gate_status": "claim_grade",
    "runtime_execution_validation_status": "passed",
}
OPTIMIZATION_ONLY_STATUS_FIELDS = {
    "compressed_kernel_registry_claim_gate_status",
    "fused_pipeline_claim_gate_status",
    "fused_pipeline_correctness_digest_status",
    "fused_pipeline_selection_vector_status",
    "operator_hot_path_candidate_status",
    "source_read_scout_reuse_status",
    "source_read_scout_timing_split_status",
    "vortex_reopen_verify_split_status",
}
EXTERNAL_UNSUPPORTED_ROW_STATUSES = {"unsupported", "unsupported_format"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--benchmark-results",
        type=Path,
        default=Path("website/assets/benchmarks/latest/benchmark-results.json"),
    )
    parser.add_argument(
        "--phase-plan",
        type=Path,
        default=Path("docs/architecture/phased-execution-plan.md"),
    )
    parser.add_argument(
        "--global-review",
        type=Path,
        default=Path("docs/architecture/global-architecture-review.md"),
    )
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument(
        "--allow-incomplete",
        action="store_true",
        help="Write the gap report but return success while the engine is still incomplete.",
    )
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def result_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return benchmark_result_rows(payload)


def unchecked_markdown_items(text: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        match = re.match(r"^(?P<indent>\s*)-\s+\[\s\]\s+(?P<title>.+?)\s*$", line)
        if not match:
            continue
        items.append(
            {
                "line": line_number,
                "title": match.group("title").strip(),
            }
        )
    return items


def is_status_like_field(key: str) -> bool:
    if key in SHARDLOOM_READY_STATUS_FIELDS:
        return True
    if key.endswith(IGNORED_STATUS_FIELD_SUFFIXES):
        return False
    return key.endswith("_status") or key.endswith("_claim_gate_status") or key.endswith("_gate_status")


def blocking_status(value: Any) -> str | None:
    if isinstance(value, bool) or value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    lowered = text.lower()
    if lowered in {
        "passed",
        "success",
        "claim_grade",
        "certified",
        "false",
        "0",
        "none",
        *NON_BLOCKING_STATUS_VALUES,
    }:
        return None
    if lowered in {"true", "1"}:
        return None
    for marker in BLOCKING_STATUS_MARKERS:
        if marker in lowered:
            return text
    return None


def claim_gate_required(row: dict[str, Any]) -> bool:
    """Only proof/publication timing surfaces carry public claim-grade row obligations."""
    timing_surface = str(row.get("timing_surface") or "").strip()
    evidence_tier = str(row.get("actual_evidence_tier") or "").strip()
    if timing_surface == "hot_runtime" or evidence_tier == "metadata_sink":
        return False
    return True


def row_ready_field_expectations(row: dict[str, Any]) -> dict[str, str]:
    expectations = dict(SHARDLOOM_READY_STATUS_FIELDS)
    if not claim_gate_required(row):
        expectations.pop("claim_gate_status", None)
    return expectations


def row_identity(row: dict[str, Any], index: int) -> str:
    engine = row.get("engine", "unknown-engine")
    fmt = row.get("storage_format") or row.get("format") or "unknown-format"
    scenario = row.get("scenario_id") or row.get("scenario_name") or row.get("scenario") or index
    return f"{engine}:{fmt}:{scenario}"


def unsupported_reason_present(row: dict[str, Any]) -> bool:
    for field in (
        "reason",
        "error",
        "human_text",
        "unsupported_reason",
        "unsupported_diagnostic_message",
    ):
        if row.get(field):
            return True
    missing_evidence = row.get("claim_grade_missing_evidence")
    if isinstance(missing_evidence, list) and missing_evidence:
        return True
    if isinstance(missing_evidence, str) and missing_evidence.strip():
        return True
    return False


def external_baseline_unsupported_report(rows: list[dict[str, Any]]) -> dict[str, Any]:
    unsupported_rows: list[dict[str, Any]] = []
    blockers: list[dict[str, Any]] = []
    field_counts: Counter[str] = Counter()

    for index, row in enumerate(rows):
        engine = str(row.get("engine", ""))
        if engine.startswith("shardloom"):
            continue
        status = str(row.get("status", ""))
        if status not in EXTERNAL_UNSUPPORTED_ROW_STATUSES:
            continue
        identity = row_identity(row, index)
        unsupported_rows.append(
            {
                "row": identity,
                "engine": engine,
                "storage_format": row.get("storage_format") or row.get("format"),
                "scenario_id": row.get("scenario_id") or row.get("scenario"),
                "status": status,
            }
        )
        expected_fields = {
            "external_baseline_only": True,
            "fallback_attempted": False,
            "external_engine_invoked": False,
        }
        for field, expected in expected_fields.items():
            if row.get(field) != expected:
                field_counts[field] += 1
                blockers.append(
                    {
                        "row": identity,
                        "field": field,
                        "expected": expected,
                        "actual": row.get(field),
                    }
                )
        if not unsupported_reason_present(row):
            field_counts["unsupported_reason"] += 1
            blockers.append(
                {
                    "row": identity,
                    "field": "unsupported_reason",
                    "expected": "non-empty reason or claim_grade_missing_evidence",
                    "actual": None,
                }
            )

    return {
        "unsupported_row_count": len(unsupported_rows),
        "unsupported_row_examples": unsupported_rows[:50],
        "classification_blocker_count": len(blockers),
        "classification_blocker_field_counts": dict(sorted(field_counts.items())),
        "classification_blocker_examples": blockers[:50],
    }


def benchmark_gap_report(payload: dict[str, Any]) -> dict[str, Any]:
    rows = result_rows(payload)
    shardloom_rows = [
        (index, row)
        for index, row in enumerate(rows)
        if str(row.get("engine", "")).startswith("shardloom")
    ]
    top_level_blockers: list[dict[str, Any]] = []
    residual_blockers: list[dict[str, Any]] = []
    residual_field_counts: Counter[str] = Counter()
    optimization_claim_blockers: list[dict[str, Any]] = []
    optimization_claim_field_counts: Counter[str] = Counter()
    external_invocation_blockers: list[dict[str, Any]] = []

    for index, row in shardloom_rows:
        identity = row_identity(row, index)
        for field, expected in row_ready_field_expectations(row).items():
            actual = row.get(field)
            if actual != expected:
                top_level_blockers.append(
                    {
                        "row": identity,
                        "field": field,
                        "expected": expected,
                        "actual": actual,
                    }
                )
        for field in ("fallback_attempted", "external_engine_invoked"):
            if row.get(field) is not False:
                external_invocation_blockers.append(
                    {
                        "row": identity,
                        "field": field,
                        "actual": row.get(field),
                    }
                )
        for field in ("runtime_fallback_attempted", "runtime_external_query_engine_invoked"):
            if field in row and row.get(field) is not False:
                external_invocation_blockers.append(
                    {
                        "row": identity,
                        "field": field,
                        "actual": row.get(field),
                    }
                )

        unsupported_count = row.get("optimizer_rule_unsupported_count")
        if isinstance(unsupported_count, int) and unsupported_count > 0:
            residual_field_counts["optimizer_rule_unsupported_count"] += 1
            residual_blockers.append(
                {
                    "row": identity,
                    "field": "optimizer_rule_unsupported_count",
                    "actual": unsupported_count,
                }
            )
        for field, value in row.items():
            if not is_status_like_field(field):
                continue
            if field in SHARDLOOM_READY_STATUS_FIELDS:
                continue
            status = blocking_status(value)
            if status is None:
                continue
            if field in OPTIMIZATION_ONLY_STATUS_FIELDS:
                optimization_claim_field_counts[field] += 1
                if len(optimization_claim_blockers) < 200:
                    optimization_claim_blockers.append(
                        {
                            "row": identity,
                            "field": field,
                            "actual": status,
                            "claim_surface": "optimization_or_encoded_native_promotion_only",
                        }
                    )
                continue
            residual_field_counts[field] += 1
            if len(residual_blockers) < 200:
                residual_blockers.append(
                    {
                        "row": identity,
                        "field": field,
                        "actual": status,
                    }
                )

    return {
        "published_row_count": len(rows),
        "shardloom_row_count": len(shardloom_rows),
        "top_level_blocker_count": len(top_level_blockers),
        "top_level_blocker_examples": top_level_blockers[:50],
        "external_invocation_blocker_count": len(external_invocation_blockers),
        "external_invocation_blocker_examples": external_invocation_blockers[:50],
        "residual_blocker_count": sum(residual_field_counts.values()),
        "residual_blocker_field_counts": dict(sorted(residual_field_counts.items())),
        "residual_blocker_examples": residual_blockers[:50],
        "optimization_claim_blocker_count": sum(optimization_claim_field_counts.values()),
        "optimization_claim_blocker_field_counts": dict(
            sorted(optimization_claim_field_counts.items())
        ),
        "optimization_claim_blocker_examples": optimization_claim_blockers[:50],
        "external_baseline_unsupported_report": external_baseline_unsupported_report(rows),
    }


def build_report(
    *,
    benchmark_results: Path,
    phase_plan: Path,
    global_review: Path,
    repo_root: Path | None = None,
    runtime_gap_family_burn_down_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    benchmark_payload = load_json(benchmark_results)
    benchmark_report = benchmark_gap_report(benchmark_payload)
    phase_unchecked = unchecked_markdown_items(read_text(phase_plan))
    review_unchecked = unchecked_markdown_items(read_text(global_review))
    runtime_gap_family_blockers: list[str] = []
    global_review_mapping_status = "no_unchecked_global_review_rows"
    if review_unchecked:
        if runtime_gap_family_burn_down_report is None and repo_root is not None:
            runtime_gap_family_burn_down_report = build_runtime_gap_family_burn_down_report(
                repo_root
            )
        if runtime_gap_family_burn_down_report is None:
            runtime_gap_family_blockers.append(
                "missing runtime gap family burn-down report"
            )
        else:
            runtime_gap_family_blockers = runtime_gap_family_burn_down_blockers(
                runtime_gap_family_burn_down_report,
                expected_global_unchecked_count=len(review_unchecked),
            )
        global_review_mapping_status = (
            "blocked_unmapped_or_invalid"
            if runtime_gap_family_blockers
            else "mapped_to_runtime_gap_family_claim_boundaries"
        )

    blockers: list[str] = []
    if phase_unchecked:
        blockers.append(f"phase plan still has unchecked items: {len(phase_unchecked)}")
    if review_unchecked and runtime_gap_family_blockers:
        blockers.append(f"global architecture review still has unchecked items: {len(review_unchecked)}")
        blockers.extend(runtime_gap_family_blockers)
    if benchmark_report["top_level_blocker_count"]:
        blockers.append(
            "published ShardLoom rows still have top-level runtime/claim blockers: "
            f"{benchmark_report['top_level_blocker_count']}"
        )
    if benchmark_report["external_invocation_blocker_count"]:
        blockers.append(
            "published ShardLoom rows still show fallback/external invocation blockers: "
            f"{benchmark_report['external_invocation_blocker_count']}"
        )
    if benchmark_report["residual_blocker_count"]:
        blockers.append(
            "published ShardLoom rows still expose residual engine substatus blockers: "
            f"{benchmark_report['residual_blocker_count']}"
        )
    external_unsupported = benchmark_report["external_baseline_unsupported_report"]
    if external_unsupported["classification_blocker_count"]:
        blockers.append(
            "published non-ShardLoom unsupported rows are missing external-baseline limitation "
            "classification: "
            f"{external_unsupported['classification_blocker_count']}"
        )

    return {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if not blockers else "blocked",
        "blockers": blockers,
        "benchmark_results": str(benchmark_results),
        "phase_plan": str(phase_plan),
        "global_review": str(global_review),
        "phase_plan_unchecked_count": len(phase_unchecked),
        "phase_plan_unchecked_examples": phase_unchecked[:50],
        "global_review_unchecked_count": len(review_unchecked),
        "global_review_unchecked_examples": review_unchecked[:100],
        "global_review_mapping_status": global_review_mapping_status,
        "global_review_unchecked_rows_block_completion": bool(runtime_gap_family_blockers),
        "runtime_gap_family_burn_down_status": (
            runtime_gap_family_burn_down_report or {}
        ).get("status"),
        "runtime_gap_family_burn_down_blocker_count": len(runtime_gap_family_blockers),
        "runtime_gap_family_burn_down_blockers": runtime_gap_family_blockers,
        "benchmark_gap_report": benchmark_report,
        "completion_claim_allowed": not blockers,
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    report = build_report(
        benchmark_results=resolve(repo_root, args.benchmark_results),
        phase_plan=resolve(repo_root, args.phase_plan),
        global_review=resolve(repo_root, args.global_review),
        repo_root=repo_root,
    )
    text = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        output = resolve(repo_root, args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text + "\n", encoding="utf-8")
    else:
        print(text)
    if report["status"] != "passed" and not args.allow_incomplete:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
