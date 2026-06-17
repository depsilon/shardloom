#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Validate the v1 SourceState and VortexPreparedState scope contract."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Iterable

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
ROOT = Path(__file__).resolve().parents[1]
PYTHON_SRC = ROOT / "python" / "src"
if str(PYTHON_SRC) not in sys.path:
    sys.path.insert(0, str(PYTHON_SRC))

from release_report_utils import (
    fail_closed_fields,
    load_json,
    read_text,
    require_markers,
    resolve_path,
    write_json,
)


SCHEMA_VERSION = "shardloom.v1_source_prepared_state_scope_report.v1"
DOC_PATH = Path("docs/architecture/v1-source-prepared-state-scope.md")
LATEST_BENCHMARK_ARTIFACT = Path(
    "website/assets/benchmarks/latest/benchmark-results.json"
)

DOC_MARKERS = (
    "shardloom.v1_source_prepared_state_scope.v1",
    "ShardLoomContext.source_prepared_state_scope_report()",
    "UniversalIngress -> SourceState -> vortex_ingest -> VortexPreparedState -> prepared_vortex",
    "UniversalIngress -> SourceState -> direct_compatibility_transient",
    "prepared_state_reuse_scope=not_applicable_no_prepared_state",
    "workspace_manifest_local_vortex_artifacts",
    "explicit_prepared_state_input",
    "artifact_adjacent_manifest_local_vortex_artifacts",
    "cold_prepare_no_manifest",
    "warm_reuse_manifest_match",
    "source_changed",
    "artifact_changed",
    "schema_changed",
    "policy_changed",
    "version_changed",
    "missing_artifact",
    "corrupted_manifest",
    "Vortex-first provider check",
    "use_vortex_native_provider",
    "wrap_vortex_concept",
    "blocked_until_vortex_or_shardloom_evidence",
    "global_hidden_cache",
    "object_store_prepared_state_reuse",
    "table_catalog_prepared_state_reuse",
    "claim_gate_status",
    "not_claim_grade",
    "fallback_attempted=false",
    "external_engine_invoked=false",
)

PUBLIC_DOC_MARKERS = {
    "README.md": (
        DOC_PATH.as_posix(),
        "SourceState",
        "VortexPreparedState",
    ),
    "python/README.md": (
        DOC_PATH.as_posix(),
        "source_prepared_state_scope_report",
        "UniversalIngress -> SourceState -> vortex_ingest -> VortexPreparedState",
    ),
    "docs/release/public-status-matrix.md": (
        DOC_PATH.as_posix(),
        "SourceState and prepared-state reuse",
        "scoped local prepared-state reuse",
    ),
    "docs/release/v1-inclusion-scope-matrix.md": (
        "`PROD-V1-1C`",
        "closed_source_prepared_state_scope",
        DOC_PATH.as_posix(),
    ),
    "website-src/src/components/BenchmarkDashboard.astro": (
        DOC_PATH.as_posix(),
        "v1 SourceState/prepared-state scope",
        "source_prepared_state_scope_report",
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--benchmark-artifact",
        type=Path,
        default=LATEST_BENCHMARK_ARTIFACT,
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/v1-source-prepared-state-scope-report.json"),
    )
    return parser.parse_args()


def load_context_report(repo_root: Path) -> Any:
    src = repo_root / "python" / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
    from shardloom import ShardLoomContext

    ctx = ShardLoomContext(client=None)
    return ctx.source_prepared_state_scope_report()


def _value(row: Any, field: str) -> Any:
    if isinstance(row, dict):
        return row.get(field)
    return getattr(row, field, None)


def _sequence(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


def _bool_false(value: Any) -> bool:
    if value is False:
        return True
    if isinstance(value, str):
        return value.strip().lower() == "false"
    return False


def _present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def route_row_payload(row: Any) -> dict[str, Any]:
    return {
        "route_id": _value(row, "route_id"),
        "route_display_name": _value(row, "route_display_name"),
        "start_state": _value(row, "start_state"),
        "vortex_normalization_point": _value(row, "vortex_normalization_point"),
        "preparation_route": _value(row, "preparation_route"),
        "execution_mode": _value(row, "execution_mode")
        or _value(row, "selected_execution_mode"),
        "output_route": _value(row, "output_route"),
        "evidence_route": _value(row, "evidence_route"),
        "materialization_decode_boundary": _value(
            row,
            "materialization_decode_boundary",
        ),
        "source_state_fingerprint": _value(row, "source_state_fingerprint"),
        "source_schema_fingerprint": _value(row, "source_schema_fingerprint"),
        "source_parse_plan_id": _value(row, "source_parse_plan_id"),
        "source_split_manifest_id": _value(row, "source_split_manifest_id"),
        "prepared_state_fingerprint": _value(row, "prepared_state_fingerprint"),
        "prepared_state_reuse_scope": _value(row, "prepared_state_reuse_scope"),
        "prepared_state_reuse_manifest_path": _value(
            row,
            "prepared_state_reuse_manifest_path",
        ),
        "prepared_state_reuse_policy": _value(row, "prepared_state_reuse_policy"),
        "prepared_state_reuse_hit": _value(row, "prepared_state_reuse_hit"),
        "prepared_state_reuse_reason": _value(row, "prepared_state_reuse_reason"),
        "prepared_state_reuse_manifest_digest": _value(
            row,
            "prepared_state_reuse_manifest_digest",
        ),
        "prepared_state_invalidation_reason": _value(
            row,
            "prepared_state_invalidation_reason",
        ),
        "route_runtime_status": _value(row, "route_runtime_status"),
        "fallback_attempted": _value(row, "fallback_attempted"),
        "external_engine_invoked": _value(row, "external_engine_invoked"),
        "required_evidence": _sequence(_value(row, "required_evidence")),
        "claim_gate_status": _value(row, "claim_gate_status"),
        "claim_boundary": _value(row, "claim_boundary"),
    }


def local_route_payload(row: Any) -> dict[str, Any]:
    payload = route_row_payload(row)
    payload.update(
        {
            "scenario_id": _value(row, "scenario_id"),
            "scenario_name": _value(row, "scenario_name"),
            "selected_execution_mode": _value(row, "selected_execution_mode"),
        }
    )
    return payload


def validate_context_report(report: Any) -> list[str]:
    blockers: list[str] = []
    if report.schema_version != "shardloom.v1_source_prepared_state_scope.v1":
        blockers.append("source/prepared scope report schema mismatch")
    if report.scope_document != DOC_PATH.as_posix():
        blockers.append("source/prepared scope document mismatch")
    if report.report_id != "prod-v1-1c.source_prepared_state_scope":
        blockers.append("source/prepared scope report id mismatch")
    if report.v1_scope_ready is not True:
        blockers.append("source/prepared v1_scope_ready must be true")
    if report.all_no_fallback_no_external_engine is not True:
        blockers.append("source/prepared routes must preserve no fallback")
    if report.all_prepared_routes_expose_reuse_contract is not True:
        blockers.append("prepared routes must expose reuse contracts")
    if report.all_generated_routes_expose_artifact_adjacent_reuse is not True:
        blockers.append("generated routes must expose artifact-adjacent reuse")
    if report.all_direct_transient_routes_are_labeled_non_persistent is not True:
        blockers.append("direct transient routes must be labeled non-persistent")
    if report.all_local_file_prepared_rows_expose_source_and_reuse_evidence is not True:
        blockers.append("local file prepared rows must expose source/reuse evidence")
    if len(report.supported_input_formats) != 6:
        blockers.append("supported input format coverage must contain 6 formats")
    if len(report.prepared_route_ids) != 4:
        blockers.append("prepared route coverage must contain 4 route ids")
    if len(report.direct_transient_route_ids) != 1:
        blockers.append("direct transient route coverage must contain 1 route id")
    if len(report.generated_route_ids) != 1:
        blockers.append("generated route coverage must contain 1 route id")
    if len(report.invalidation_case_ids) != 9:
        blockers.append("invalidation matrix must contain 9 cases")
    if len(report.golden_fixture_paths) != 3:
        blockers.append("golden fixture coverage must contain 3 fixtures")
    if "global_hidden_cache" not in report.unsupported_boundary_ids:
        blockers.append("unsupported boundaries must include global_hidden_cache")
    if "object_store_prepared_state_reuse" not in report.unsupported_boundary_ids:
        blockers.append("unsupported boundaries must include object_store_prepared_state_reuse")
    for field in (
        "performance_claim_allowed",
        "production_claim_allowed",
        "spark_replacement_claim_allowed",
    ):
        if getattr(report, field) is not False:
            blockers.append(f"{field} must be false")
    if report.claim_gate_status != "not_claim_grade":
        blockers.append("claim_gate_status must remain not_claim_grade")
    for row in (
        *report.prepared_user_route_rows,
        *report.direct_transient_user_route_rows,
        *report.generated_user_route_rows,
        *report.prepared_local_file_rows,
        *report.direct_transient_local_file_rows,
    ):
        route_id = _value(row, "route_id")
        if not _bool_false(_value(row, "fallback_attempted")):
            blockers.append(f"{route_id}: fallback_attempted must be false")
        if not _bool_false(_value(row, "external_engine_invoked")):
            blockers.append(f"{route_id}: external_engine_invoked must be false")
        if _value(row, "claim_gate_status") != "not_claim_grade":
            blockers.append(f"{route_id}: claim_gate_status must remain not_claim_grade")
    return blockers


def validate_docs(repo_root: Path) -> list[str]:
    blockers: list[str] = []
    blockers.extend(
        require_markers(
            DOC_PATH.as_posix(),
            read_text(resolve_path(repo_root, DOC_PATH)),
            DOC_MARKERS,
        )
    )
    for rel_path, markers in PUBLIC_DOC_MARKERS.items():
        blockers.extend(
            require_markers(rel_path, read_text(resolve_path(repo_root, rel_path)), markers)
        )
    return blockers


def validate_fixtures(repo_root: Path, report: Any) -> tuple[list[str], list[dict[str, Any]]]:
    blockers: list[str] = []
    fixtures: list[dict[str, Any]] = []
    expected_cases = set(report.invalidation_case_ids)
    for rel_path in report.golden_fixture_paths:
        path = resolve_path(repo_root, rel_path)
        try:
            payload = load_json(path)
        except (OSError, ValueError) as exc:
            blockers.append(f"{rel_path}: fixture unreadable: {exc.__class__.__name__}")
            continue
        fixtures.append({"path": rel_path, "schema_version": payload.get("schema_version")})
        if payload.get("scope_document") != DOC_PATH.as_posix():
            blockers.append(f"{rel_path}: scope_document mismatch")
        if payload.get("claim_gate_status") != "not_claim_grade":
            blockers.append(f"{rel_path}: claim_gate_status must remain not_claim_grade")
        if payload.get("fallback_attempted") is not False:
            blockers.append(f"{rel_path}: fallback_attempted must be false")
        if payload.get("external_engine_invoked") is not False:
            blockers.append(f"{rel_path}: external_engine_invoked must be false")
        if rel_path.endswith("reuse-invalidation-matrix.json"):
            cases = payload.get("cases", [])
            if not isinstance(cases, list):
                blockers.append(f"{rel_path}: cases must be a list")
                continue
            case_ids = {str(case.get("case_id")) for case in cases if isinstance(case, dict)}
            if case_ids != expected_cases:
                blockers.append(
                    f"{rel_path}: case ids mismatch: {','.join(sorted(case_ids ^ expected_cases))}"
                )
            for case in cases:
                if not isinstance(case, dict):
                    blockers.append(f"{rel_path}: case must be object")
                    continue
                if "reuse_hit" not in case:
                    blockers.append(f"{rel_path}: {case.get('case_id')}: missing reuse_hit")
                if not _present(case.get("reuse_reason")):
                    blockers.append(f"{rel_path}: {case.get('case_id')}: missing reuse_reason")
                if not _present(case.get("invalidation_reason")):
                    blockers.append(
                        f"{rel_path}: {case.get('case_id')}: missing invalidation_reason"
                    )
    return blockers, fixtures


def _candidate_chunk_paths(repo_root: Path, benchmark_path: Path, raw_path: str) -> Iterable[Path]:
    raw = Path(raw_path)
    if raw.is_absolute():
        yield raw
    yield repo_root / raw
    yield benchmark_path.parent / raw
    yield benchmark_path.parent / raw.name


def load_benchmark_rows(repo_root: Path, benchmark_path: Path) -> tuple[list[dict[str, Any]], str]:
    payload = load_json(benchmark_path, missing_ok=True)
    if not isinstance(payload, dict):
        return [], "missing_or_invalid"
    rows: list[dict[str, Any]] = []
    refs = payload.get("published_benchmark_row_chunks", [])
    if isinstance(refs, list) and refs:
        seen: set[Path] = set()
        for ref in refs:
            if not isinstance(ref, dict) or not ref.get("path"):
                continue
            chunk_path = None
            for candidate in _candidate_chunk_paths(repo_root, benchmark_path, str(ref["path"])):
                resolved = candidate.resolve(strict=False)
                if resolved in seen or not candidate.exists():
                    continue
                chunk_path = candidate
                seen.add(resolved)
                break
            if chunk_path is None:
                return rows, "missing_chunk"
            chunk_payload = load_json(chunk_path)
            chunk_rows = chunk_payload.get("rows") if isinstance(chunk_payload, dict) else None
            if isinstance(chunk_rows, list):
                rows.extend(row for row in chunk_rows if isinstance(row, dict))
        return rows, "chunked"
    inline_rows = payload.get("published_benchmark_rows") or payload.get("rows")
    if isinstance(inline_rows, list):
        rows.extend(row for row in inline_rows if isinstance(row, dict))
        return rows, "inline"
    return rows, "empty"


def validate_benchmark_rows(
    repo_root: Path,
    benchmark_artifact: Path,
    report: Any,
) -> tuple[list[str], dict[str, Any]]:
    blockers: list[str] = []
    benchmark_path = resolve_path(repo_root, benchmark_artifact)
    rows, row_source = load_benchmark_rows(repo_root, benchmark_path)
    shardloom_rows = [
        row
        for row in rows
        if str(row.get("engine", "")).startswith("shardloom")
        or str(row.get("route_lane_id", "")).startswith("shardloom")
    ]
    required = tuple(report.required_runtime_fields)
    missing_rows: list[dict[str, Any]] = []
    fallback_rows: list[dict[str, Any]] = []
    scenario_ids = set()
    for row in shardloom_rows:
        if row.get("scenario_id"):
            scenario_ids.add(str(row["scenario_id"]))
        missing = [field for field in required if not _present(row.get(field))]
        if missing:
            missing_rows.append(
                {
                    "engine": row.get("engine"),
                    "scenario_id": row.get("scenario_id"),
                    "route_lane_id": row.get("route_lane_id"),
                    "missing_fields": missing,
                }
            )
        if row.get("fallback_attempted") is not False or row.get("external_engine_invoked") is not False:
            fallback_rows.append(
                {
                    "engine": row.get("engine"),
                    "scenario_id": row.get("scenario_id"),
                    "route_lane_id": row.get("route_lane_id"),
                    "fallback_attempted": row.get("fallback_attempted"),
                    "external_engine_invoked": row.get("external_engine_invoked"),
                }
            )
    if not shardloom_rows:
        blockers.append("benchmark artifact must contain ShardLoom rows")
    if missing_rows:
        blockers.append(
            f"benchmark ShardLoom rows missing required fields: {len(missing_rows)}"
        )
    if fallback_rows:
        blockers.append(f"benchmark ShardLoom rows reported fallback/external use: {len(fallback_rows)}")
    expected_scenarios = set(report.local_file_routes.scenario_ids)
    if expected_scenarios and not expected_scenarios.issubset(scenario_ids):
        blockers.append(
            "benchmark ShardLoom rows missing local scenario ids: "
            + ",".join(sorted(expected_scenarios - scenario_ids))
        )
    summary = {
        "benchmark_artifact": str(benchmark_artifact).replace("\\", "/"),
        "row_source": row_source,
        "total_rows": len(rows),
        "shardloom_row_count": len(shardloom_rows),
        "required_runtime_fields": list(required),
        "rows_missing_required_fields": missing_rows[:20],
        "rows_with_fallback_or_external_engine": fallback_rows[:20],
        "local_scenario_count": len(scenario_ids & expected_scenarios),
        "all_required_fields_present": not missing_rows,
        "all_no_fallback_no_external_engine": not fallback_rows,
    }
    return blockers, summary


def build_report(repo_root: Path, *, benchmark_artifact: Path = LATEST_BENCHMARK_ARTIFACT) -> dict[str, Any]:
    scope_report = load_context_report(repo_root)
    prepared_user_rows = [route_row_payload(row) for row in scope_report.prepared_user_route_rows]
    direct_user_rows = [
        route_row_payload(row) for row in scope_report.direct_transient_user_route_rows
    ]
    generated_user_rows = [
        route_row_payload(row) for row in scope_report.generated_user_route_rows
    ]
    prepared_local_rows = [
        local_route_payload(row) for row in scope_report.prepared_local_file_rows
    ]
    direct_local_rows = [
        local_route_payload(row) for row in scope_report.direct_transient_local_file_rows
    ]

    blockers: list[str] = []
    blockers.extend(validate_context_report(scope_report))
    blockers.extend(validate_docs(repo_root))
    fixture_blockers, fixtures = validate_fixtures(repo_root, scope_report)
    blockers.extend(fixture_blockers)
    benchmark_blockers, benchmark_summary = validate_benchmark_rows(
        repo_root,
        benchmark_artifact,
        scope_report,
    )
    blockers.extend(benchmark_blockers)

    passed = not blockers
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if passed else "failed",
        "report_id": scope_report.report_id,
        "v1_scope_document": scope_report.scope_document,
        "canonical_route": scope_report.canonical_route,
        "direct_transient_route": scope_report.direct_transient_route,
        "supported_input_formats": list(scope_report.supported_input_formats),
        "prepared_route_ids": list(scope_report.prepared_route_ids),
        "direct_transient_route_ids": list(scope_report.direct_transient_route_ids),
        "generated_route_ids": list(scope_report.generated_route_ids),
        "invalidation_case_ids": list(scope_report.invalidation_case_ids),
        "golden_fixture_paths": list(scope_report.golden_fixture_paths),
        "required_runtime_fields": list(scope_report.required_runtime_fields),
        "unsupported_boundary_ids": list(scope_report.unsupported_boundary_ids),
        "prepared_user_route_rows": prepared_user_rows,
        "direct_transient_user_route_rows": direct_user_rows,
        "generated_user_route_rows": generated_user_rows,
        "prepared_local_file_rows": prepared_local_rows,
        "direct_transient_local_file_rows": direct_local_rows,
        "fixture_rows": fixtures,
        "benchmark_artifact_summary": benchmark_summary,
        "source_prepared_benchmark_rows_with_required_fields": (
            benchmark_summary["shardloom_row_count"]
            if benchmark_summary["all_required_fields_present"]
            else 0
        ),
        "source_prepared_benchmark_required_fields_ready": benchmark_summary[
            "all_required_fields_present"
        ],
        "all_no_fallback_no_external_engine": (
            scope_report.all_no_fallback_no_external_engine
            and benchmark_summary["all_no_fallback_no_external_engine"]
        ),
        "all_prepared_routes_expose_reuse_contract": (
            scope_report.all_prepared_routes_expose_reuse_contract
        ),
        "all_generated_routes_expose_artifact_adjacent_reuse": (
            scope_report.all_generated_routes_expose_artifact_adjacent_reuse
        ),
        "all_direct_transient_routes_are_labeled_non_persistent": (
            scope_report.all_direct_transient_routes_are_labeled_non_persistent
        ),
        "all_local_file_prepared_rows_expose_source_and_reuse_evidence": (
            scope_report.all_local_file_prepared_rows_expose_source_and_reuse_evidence
        ),
        "v1_scope_ready": scope_report.v1_scope_ready and not benchmark_blockers,
        "claim_gate_status": scope_report.claim_gate_status,
        "blockers": blockers,
        **fail_closed_fields(),
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output = resolve_path(repo_root, args.output)
    report = build_report(repo_root, benchmark_artifact=args.benchmark_artifact)
    write_json(output, report)
    print(output)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
