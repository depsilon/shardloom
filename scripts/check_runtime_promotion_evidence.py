#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate claim-safe evidence metadata for publicly promoted runtime paths."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from check_use_case_index import INDEX_PATH, REPO_ROOT, load_index, validate_index


STATUS_ROWS_PATH = REPO_ROOT / "website-src" / "src" / "data" / "status-rows.json"
BENCHMARK_MANIFEST_PATH = REPO_ROOT / "website" / "assets" / "benchmarks" / "latest" / "manifest.json"
BENCHMARK_RESULTS_PATH = (
    REPO_ROOT / "website" / "assets" / "benchmarks" / "latest" / "benchmark-results.json"
)
SUPPORTED_USE_CASE_STATUSES = {"ready_local", "smoke_supported"}
SUPPORTED_STATUS_ROW_STATUSES = {
    "ready_local",
    "runtime_supported",
    "smoke_supported",
    "fixture_smoke_only",
}
REQUIRED_PUBLIC_EVIDENCE_TOKENS = {
    "fallback_attempted=false",
    "external_engine_invoked=false",
    "claim_gate_status",
}
REQUIRED_BENCHMARK_MANIFEST_FIELDS = {
    "schema_version",
    "generated_at_utc",
    "benchmark_profile",
    "benchmark_git_sha",
    "shardloom_git_sha",
    "expected_lanes",
    "available_lanes",
    "missing_lanes",
    "missing_required_lanes",
    "lane_versions",
    "lane_availability_reasons",
    "environment",
    "claim_boundary",
    "performance_claim_allowed",
    "artifact_paths",
    "artifact_status",
}
REQUIRED_COMPATIBILITY_TIMING_FIELDS = {
    "source_read_millis",
    "compatibility_parse_millis",
    "compatibility_to_vortex_import_millis",
    "vortex_write_millis",
    "vortex_reopen_millis",
    "vortex_scan_millis",
    "operator_compute_millis",
    "result_sink_write_millis",
    "evidence_render_millis",
    "total_runtime_millis",
}
REQUIRED_PREPARED_TIMING_FIELDS = {
    "selected_execution_mode",
    "query_runtime_millis",
    "total_runtime_millis",
    "fallback_attempted",
    "external_engine_invoked",
    "claim_gate_status",
}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _evidence_text(values: Any) -> str:
    return " ".join(str(value) for value in _as_list(values))


def _has_token(evidence: str, token: str) -> bool:
    if token.endswith("="):
        return token in evidence
    return token in evidence


def _load_json(path: Path, blockers: list[str], label: str) -> Any:
    if not path.exists():
        blockers.append(f"missing {label}: {path.relative_to(REPO_ROOT).as_posix()}")
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        blockers.append(f"{label} is not valid JSON: {error}")
        return None


def _validate_supported_evidence(
    *,
    label: str,
    evidence_values: Any,
    blockers: list[str],
) -> None:
    evidence = _evidence_text(evidence_values)
    for token in REQUIRED_PUBLIC_EVIDENCE_TOKENS:
        if not _has_token(evidence, token):
            blockers.append(f"{label} is publicly supported but missing evidence token {token}")


def _validate_session_reuse_evidence(
    *,
    label: str,
    evidence_values: Any,
    blockers: list[str],
) -> None:
    fields = {str(value) for value in _as_list(evidence_values)}
    text = " ".join(sorted(fields))
    if "session_id" not in text:
        return
    for token in ("session_state_scope", "reuse_reason"):
        if token not in text:
            blockers.append(f"{label} has session_id evidence but omits {token}")
    if "prepared_state_reuse_hit" in text:
        for token in ("prepared_state_id", "prepared_state_digest"):
            if token not in text:
                blockers.append(f"{label} has prepared-state reuse but omits {token}")
    if "output_plan_reuse_hit" in text:
        for token in ("result_replay_reuse_hit", "reuse_reason"):
            if token not in text:
                blockers.append(f"{label} has output-plan reuse but omits {token}")


def _validate_use_cases(repo_root: Path, index_path: Path, blockers: list[str]) -> None:
    data = load_index(index_path)
    blockers.extend(validate_index(data, repo_root))
    for use_case in _as_list(data.get("use_cases")):
        if not isinstance(use_case, dict):
            continue
        status = use_case.get("status")
        if status not in SUPPORTED_USE_CASE_STATUSES:
            continue
        label = f"use case {use_case.get('id', '<missing-id>')}"
        _validate_supported_evidence(
            label=label,
            evidence_values=use_case.get("evidence_fields"),
            blockers=blockers,
        )
        _validate_session_reuse_evidence(
            label=label,
            evidence_values=use_case.get("evidence_fields"),
            blockers=blockers,
        )
        if not str(use_case.get("claim_boundary", "")).strip():
            blockers.append(f"{label} is publicly supported but has no claim boundary")
        if not str(use_case.get("expected_output_evidence", "")).strip():
            blockers.append(f"{label} is publicly supported but has no expected evidence summary")
        if not str(use_case.get("runnable_example", "")).strip():
            blockers.append(f"{label} is publicly supported but has no runnable example")


def _validate_status_rows(path: Path, blockers: list[str]) -> None:
    rows = _load_json(path, blockers, "website status rows")
    if rows is None:
        return
    if not isinstance(rows, list):
        blockers.append("website status rows must be a JSON list")
        return
    for row in rows:
        if not isinstance(row, dict):
            blockers.append("website status row must be a JSON object")
            continue
        status = row.get("status")
        if status not in SUPPORTED_STATUS_ROW_STATUSES:
            continue
        label = f"status row {row.get('capability', '<missing-capability>')}"
        _validate_supported_evidence(
            label=label,
            evidence_values=row.get("evidence"),
            blockers=blockers,
        )
        _validate_session_reuse_evidence(
            label=label,
            evidence_values=row.get("evidence"),
            blockers=blockers,
        )
        if not _as_list(row.get("references")):
            blockers.append(f"{label} is publicly supported but has no reference files")
        if not str(row.get("blocked", "")).strip():
            blockers.append(f"{label} is publicly supported but has no blocked/non-goal text")


def _validate_benchmark_manifest(path: Path, blockers: list[str]) -> None:
    manifest = _load_json(path, blockers, "benchmark manifest")
    if not isinstance(manifest, dict):
        if manifest is not None:
            blockers.append("benchmark manifest must be a JSON object")
        return
    missing = REQUIRED_BENCHMARK_MANIFEST_FIELDS - manifest.keys()
    if missing:
        blockers.append(f"benchmark manifest missing fields: {sorted(missing)}")
    if manifest.get("schema_version") != "shardloom.website_benchmark_manifest.v1":
        blockers.append("benchmark manifest has unexpected schema_version")
    if manifest.get("artifact_status") != "complete":
        blockers.append("benchmark manifest artifact_status must be complete for public support")
    if manifest.get("performance_claim_allowed") is not False:
        blockers.append("benchmark manifest must keep performance_claim_allowed=false")
    for field in ("expected_lanes", "available_lanes", "missing_lanes", "missing_required_lanes"):
        if not isinstance(manifest.get(field), list):
            blockers.append(f"benchmark manifest {field} must be a list")
    if manifest.get("missing_required_lanes"):
        blockers.append("benchmark manifest has missing required lanes")
    expected = set(_as_list(manifest.get("expected_lanes")))
    available = set(_as_list(manifest.get("available_lanes")))
    unavailable_expected = expected - available - set(_as_list(manifest.get("missing_lanes")))
    if unavailable_expected:
        blockers.append(
            "benchmark manifest expected lanes are neither available nor listed missing: "
            f"{sorted(unavailable_expected)}"
        )
    versions = manifest.get("lane_versions")
    reasons = manifest.get("lane_availability_reasons")
    if not isinstance(versions, dict) or not isinstance(reasons, dict):
        blockers.append("benchmark manifest must include lane_versions and lane_availability_reasons")
    else:
        for lane in sorted(expected):
            if lane not in versions:
                blockers.append(f"benchmark manifest missing version for expected lane {lane}")
            if lane not in reasons:
                blockers.append(f"benchmark manifest missing availability reason for expected lane {lane}")


def _string_value(row: dict[str, Any], key: str) -> str:
    value = row.get(key)
    return "" if value is None else str(value)


def _validate_benchmark_results(path: Path, blockers: list[str]) -> None:
    results = _load_json(path, blockers, "benchmark results")
    if not isinstance(results, dict):
        if results is not None:
            blockers.append("benchmark results must be a JSON object")
        return
    rows = _as_list(results.get("rows"))
    if not rows:
        blockers.append("benchmark results must include promoted rows")
        return
    seen_compatibility = False
    seen_prepared = False
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            blockers.append(f"benchmark result row {index} must be a JSON object")
            continue
        label = f"benchmark row {index} ({row.get('engine', '<engine>')}/{row.get('scenario', '<scenario>')})"
        for key, expected in (
            ("fallback_attempted", "false"),
            ("external_engine_invoked", "false"),
        ):
            if _string_value(row, key).lower() != expected:
                blockers.append(f"{label} must report {key}=false")
        if not _string_value(row, "claim_gate_status"):
            blockers.append(f"{label} missing claim_gate_status")
        mode = _string_value(row, "selected_execution_mode")
        if mode == "compatibility_import_certified":
            seen_compatibility = True
            missing = {field for field in REQUIRED_COMPATIBILITY_TIMING_FIELDS if field not in row}
            if missing:
                blockers.append(f"{label} compatibility cold route missing timings {sorted(missing)}")
        if mode == "prepared_vortex":
            seen_prepared = True
            missing = {field for field in REQUIRED_PREPARED_TIMING_FIELDS if field not in row}
            if missing:
                blockers.append(f"{label} prepared warm route missing fields {sorted(missing)}")
    if not seen_compatibility:
        blockers.append("benchmark results must include compatibility_import_certified rows")
    if not seen_prepared:
        blockers.append("benchmark results must include prepared_vortex rows")


def validate_runtime_promotion_evidence(
    *,
    repo_root: Path = REPO_ROOT,
    index_path: Path = INDEX_PATH,
    status_rows_path: Path = STATUS_ROWS_PATH,
    benchmark_manifest_path: Path = BENCHMARK_MANIFEST_PATH,
    benchmark_results_path: Path = BENCHMARK_RESULTS_PATH,
) -> list[str]:
    repo_root = repo_root.resolve()
    blockers: list[str] = []
    _validate_use_cases(repo_root, index_path, blockers)
    _validate_status_rows(status_rows_path, blockers)
    _validate_benchmark_manifest(benchmark_manifest_path, blockers)
    _validate_benchmark_results(benchmark_results_path, blockers)
    return blockers


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--index", type=Path, default=INDEX_PATH)
    parser.add_argument("--status-rows", type=Path, default=STATUS_ROWS_PATH)
    parser.add_argument("--benchmark-manifest", type=Path, default=BENCHMARK_MANIFEST_PATH)
    parser.add_argument("--benchmark-results", type=Path, default=BENCHMARK_RESULTS_PATH)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    blockers = validate_runtime_promotion_evidence(
        repo_root=repo_root,
        index_path=args.index if args.index.is_absolute() else repo_root / args.index,
        status_rows_path=(
            args.status_rows if args.status_rows.is_absolute() else repo_root / args.status_rows
        ),
        benchmark_manifest_path=(
            args.benchmark_manifest
            if args.benchmark_manifest.is_absolute()
            else repo_root / args.benchmark_manifest
        ),
        benchmark_results_path=(
            args.benchmark_results
            if args.benchmark_results.is_absolute()
            else repo_root / args.benchmark_results
        ),
    )
    if blockers:
        print("runtime promotion evidence validation failed:", file=sys.stderr)
        for blocker in blockers:
            print(f"- {blocker}", file=sys.stderr)
        return 1
    print("runtime promotion evidence ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
