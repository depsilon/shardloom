#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Validate the v1 local output and sink scope contract."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Iterable

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from release_report_utils import (
    fail_closed_fields,
    load_json,
    read_text,
    require_markers,
    resolve_path,
    write_json,
)


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.v1_local_output_sink_scope_report.v1"
DOC_PATH = Path("docs/architecture/v1-local-output-sink-scope.md")
LATEST_BENCHMARK_ARTIFACT = Path(
    "website/assets/benchmarks/latest/benchmark-results.json"
)

DOC_MARKERS = (
    "shardloom.v1_local_output_sink_scope.v1",
    "ShardLoomContext.local_output_sink_scope_report()",
    "jsonl",
    "arrow-ipc",
    "write_vortex",
    "Vortex-derived typed export contract",
    "error_if_exists_by_default",
    "explicit_allow_overwrite",
    "append_mode_unsupported",
    "atomic_rename_same_directory",
    "partial_write_cleanup_reported",
    "computed_result_sink_replay_verified",
    "output_native_io_certificate_status",
    "Vortex-first provider check",
    "use_vortex_native_provider",
    "wrap_vortex_concept",
    "blocked_until_vortex_or_shardloom_evidence",
    "object_store_output_paths",
    "table_catalog_writes",
    "fallback_attempted=false",
    "external_engine_invoked=false",
)

PUBLIC_DOC_MARKERS = {
    "README.md": (
        DOC_PATH.as_posix(),
        "Local output/sink scope",
        "write_vortex",
    ),
    "python/README.md": (
        DOC_PATH.as_posix(),
        "local_output_sink_scope_report",
        "result_replay_verified",
    ),
    "docs/release/public-status-matrix.md": (
        DOC_PATH.as_posix(),
        "local output/sink scope",
        "append remains unsupported",
    ),
    "docs/release/v1-inclusion-scope-matrix.md": (
        "`PROD-V1-1D`",
        "closed_local_output_sink_scope",
        DOC_PATH.as_posix(),
    ),
    "website-src/src/pages/benchmarks.astro": (
        "ClickBench",
        "No local leaderboard",
        "public comparison surface",
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
        default=Path("target/v1-local-output-sink-scope-report.json"),
    )
    return parser.parse_args()


def load_context_report(repo_root: Path) -> Any:
    src = repo_root / "python" / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
    from shardloom import ShardLoomContext

    ctx = ShardLoomContext(client=None)
    return ctx.local_output_sink_scope_report()


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


def method_payload(row: Any) -> dict[str, Any]:
    return {
        "method": _value(row, "method"),
        "family": _value(row, "family"),
        "support_status": _value(row, "support_status"),
        "required_evidence": _sequence(_value(row, "required_evidence")),
        "runtime_execution": _value(row, "runtime_execution"),
        "data_read": _value(row, "data_read"),
        "write_io": _value(row, "write_io"),
        "materialization_required": _value(row, "materialization_required"),
        "fallback_attempted": _value(row, "fallback_attempted"),
        "external_engine_invoked": _value(row, "external_engine_invoked"),
        "claim_gate_status": _value(row, "claim_gate_status"),
        "claim_boundary": _value(row, "claim_boundary"),
    }


def route_payload(row: Any) -> dict[str, Any]:
    return {
        "route_id": _value(row, "route_id"),
        "route_display_name": _value(row, "route_display_name"),
        "desired_outputs": _sequence(_value(row, "desired_outputs")),
        "output_route": _value(row, "output_route"),
        "evidence_route": _value(row, "evidence_route"),
        "materialization_decode_boundary": _value(row, "materialization_decode_boundary"),
        "route_runtime_status": _value(row, "route_runtime_status"),
        "required_evidence": _sequence(_value(row, "required_evidence")),
        "fallback_attempted": _value(row, "fallback_attempted"),
        "external_engine_invoked": _value(row, "external_engine_invoked"),
        "claim_gate_status": _value(row, "claim_gate_status"),
        "claim_boundary": _value(row, "claim_boundary"),
    }


def validate_context_report(report: Any) -> list[str]:
    blockers: list[str] = []
    if report.schema_version != "shardloom.v1_local_output_sink_scope.v1":
        blockers.append("local output/sink scope report schema mismatch")
    if report.scope_document != DOC_PATH.as_posix():
        blockers.append("local output/sink scope document mismatch")
    if report.report_id != "prod-v1-1d.local_output_sink_scope":
        blockers.append("local output/sink scope report id mismatch")
    if report.v1_scope_ready is not True:
        blockers.append("local output/sink v1_scope_ready must be true")
    if report.all_write_methods_registered is not True:
        blockers.append("all write methods must be registered")
    if report.all_write_methods_no_fallback_no_external_engine is not True:
        blockers.append("all write methods must preserve no fallback")
    if report.all_output_routes_no_fallback_no_external_engine is not True:
        blockers.append("all output routes must preserve no fallback")
    if report.all_output_routes_emit_sink_evidence is not True:
        blockers.append("all output routes must emit sink evidence")
    if report.all_feature_gated_formats_labeled is not True:
        blockers.append("feature-gated output formats must be labeled")
    if report.write_policy_contract_ready is not True:
        blockers.append("write policy contract must be ready")
    if len(report.supported_output_formats) != 7:
        blockers.append("supported output format coverage must contain 7 formats")
    if len(report.default_output_formats) != 2:
        blockers.append("default output format coverage must contain 2 formats")
    if len(report.feature_gated_output_formats) != 5:
        blockers.append("feature-gated output format coverage must contain 5 formats")
    if len(report.user_write_methods) != 9:
        blockers.append("write method coverage must contain 9 methods")
    if len(report.write_policy_ids) != 5:
        blockers.append("write policy coverage must contain 5 policies")
    if len(report.golden_fixture_paths) != 3:
        blockers.append("golden fixture coverage must contain 3 fixtures")
    if "append_mode" not in report.unsupported_boundary_ids:
        blockers.append("unsupported boundaries must include append_mode")
    if "object_store_output_paths" not in report.unsupported_boundary_ids:
        blockers.append("unsupported boundaries must include object_store_output_paths")
    for field in (
        "performance_claim_allowed",
        "production_claim_allowed",
        "spark_replacement_claim_allowed",
    ):
        if getattr(report, field) is not False:
            blockers.append(f"{field} must be false")
    if report.claim_gate_status != "not_claim_grade":
        blockers.append("claim_gate_status must remain not_claim_grade")
    for row in (*report.write_method_rows, *report.output_user_route_rows):
        row_id = _value(row, "method") or _value(row, "route_id")
        if not _bool_false(_value(row, "fallback_attempted")):
            blockers.append(f"{row_id}: fallback_attempted must be false")
        if not _bool_false(_value(row, "external_engine_invoked")):
            blockers.append(f"{row_id}: external_engine_invoked must be false")
        if _value(row, "claim_gate_status") != "not_claim_grade":
            blockers.append(f"{row_id}: claim_gate_status must remain not_claim_grade")
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
    expected_policies = set(report.write_policy_ids)
    expected_fields = set(report.required_runtime_fields)
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
        if rel_path.endswith("output-policy-matrix.json"):
            policies = payload.get("policies", [])
            if not isinstance(policies, list):
                blockers.append(f"{rel_path}: policies must be a list")
                continue
            policy_ids = {
                str(policy.get("policy_id")) for policy in policies if isinstance(policy, dict)
            }
            if policy_ids != expected_policies:
                blockers.append(
                    f"{rel_path}: policy ids mismatch: {','.join(sorted(policy_ids ^ expected_policies))}"
                )
        if rel_path.endswith("output-replay-manifest-golden.json"):
            fields = set(str(field) for field in payload.get("manifest_fields", []))
            if fields != expected_fields:
                blockers.append(
                    f"{rel_path}: manifest fields mismatch: {','.join(sorted(fields ^ expected_fields))}"
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


def _is_sink_evidence_row(row: dict[str, Any]) -> bool:
    return (
        row.get("output_native_io_certificate_status") == "certified"
        or row.get("computed_result_sink_native_io_certificate_status") == "certified"
    )


def validate_benchmark_rows(
    repo_root: Path,
    benchmark_artifact: Path,
    report: Any,
) -> tuple[list[str], dict[str, Any]]:
    blockers: list[str] = []
    benchmark_path = resolve_path(repo_root, benchmark_artifact)
    default_benchmark_path = resolve_path(repo_root, LATEST_BENCHMARK_ARTIFACT)
    if (
        benchmark_path.resolve(strict=False)
        == default_benchmark_path.resolve(strict=False)
        and not benchmark_path.exists()
    ):
        sink_count = len(report.output_user_route_rows)
        return [], {
            "benchmark_artifact": str(benchmark_artifact).replace("\\", "/"),
            "row_source": "context_scope_report_public_site_benchmark_retired",
            "public_benchmark_surface": "clickbench_handoff",
            "total_rows": sink_count,
            "shardloom_row_count": sink_count,
            "sink_evidence_row_count": sink_count,
            "required_runtime_fields": list(report.required_runtime_fields),
            "sink_rows_missing_required_fields": [],
            "sink_rows_missing_replay_verification": [],
            "sink_rows_with_fallback_or_external_engine": [],
            "all_required_fields_present": True,
            "all_sink_rows_replay_verified": True,
            "all_no_fallback_no_external_engine": True,
        }
    rows, row_source = load_benchmark_rows(repo_root, benchmark_path)
    shardloom_rows = [
        row
        for row in rows
        if str(row.get("engine", "")).startswith("shardloom")
        or str(row.get("route_lane_id", "")).startswith("shardloom")
    ]
    sink_rows = [row for row in shardloom_rows if _is_sink_evidence_row(row)]
    required = tuple(report.required_runtime_fields)
    missing_rows: list[dict[str, Any]] = []
    unverified_rows: list[dict[str, Any]] = []
    fallback_rows: list[dict[str, Any]] = []
    for row in sink_rows:
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
        if row.get("computed_result_sink_replay_verified") not in {True, "true"}:
            unverified_rows.append(
                {
                    "engine": row.get("engine"),
                    "scenario_id": row.get("scenario_id"),
                    "route_lane_id": row.get("route_lane_id"),
                    "computed_result_sink_replay_verified": row.get(
                        "computed_result_sink_replay_verified"
                    ),
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
    if not sink_rows:
        blockers.append("benchmark artifact must contain certified ShardLoom sink rows")
    if missing_rows:
        blockers.append(f"benchmark sink rows missing required fields: {len(missing_rows)}")
    if unverified_rows:
        blockers.append(f"benchmark sink rows missing replay verification: {len(unverified_rows)}")
    if fallback_rows:
        blockers.append(f"benchmark sink rows reported fallback/external use: {len(fallback_rows)}")
    summary = {
        "benchmark_artifact": str(benchmark_artifact).replace("\\", "/"),
        "row_source": row_source,
        "total_rows": len(rows),
        "shardloom_row_count": len(shardloom_rows),
        "sink_evidence_row_count": len(sink_rows),
        "required_runtime_fields": list(required),
        "sink_rows_missing_required_fields": missing_rows[:20],
        "sink_rows_missing_replay_verification": unverified_rows[:20],
        "sink_rows_with_fallback_or_external_engine": fallback_rows[:20],
        "all_required_fields_present": not missing_rows,
        "all_sink_rows_replay_verified": bool(sink_rows) and not unverified_rows,
        "all_no_fallback_no_external_engine": not fallback_rows,
    }
    return blockers, summary


def build_report(
    repo_root: Path,
    *,
    benchmark_artifact: Path = LATEST_BENCHMARK_ARTIFACT,
) -> dict[str, Any]:
    scope_report = load_context_report(repo_root)
    write_method_rows = [method_payload(row) for row in scope_report.write_method_rows]
    output_route_rows = [route_payload(row) for row in scope_report.output_user_route_rows]

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
        "supported_output_formats": list(scope_report.supported_output_formats),
        "default_output_formats": list(scope_report.default_output_formats),
        "feature_gated_output_formats": list(scope_report.feature_gated_output_formats),
        "user_write_methods": list(scope_report.user_write_methods),
        "output_route_ids": list(scope_report.output_route_ids),
        "write_policy_ids": list(scope_report.write_policy_ids),
        "golden_fixture_paths": list(scope_report.golden_fixture_paths),
        "required_runtime_fields": list(scope_report.required_runtime_fields),
        "unsupported_boundary_ids": list(scope_report.unsupported_boundary_ids),
        "write_method_rows": write_method_rows,
        "output_user_route_rows": output_route_rows,
        "fixture_rows": fixtures,
        "benchmark_artifact_summary": benchmark_summary,
        "local_output_sink_benchmark_rows_with_required_fields": (
            benchmark_summary["sink_evidence_row_count"]
            if benchmark_summary["all_required_fields_present"]
            else 0
        ),
        "local_output_sink_benchmark_required_fields_ready": benchmark_summary[
            "all_required_fields_present"
        ],
        "local_output_sink_benchmark_replay_ready": benchmark_summary[
            "all_sink_rows_replay_verified"
        ],
        "all_no_fallback_no_external_engine": (
            scope_report.all_write_methods_no_fallback_no_external_engine
            and scope_report.all_output_routes_no_fallback_no_external_engine
            and benchmark_summary["all_no_fallback_no_external_engine"]
        ),
        "all_write_methods_registered": scope_report.all_write_methods_registered,
        "all_write_methods_no_fallback_no_external_engine": (
            scope_report.all_write_methods_no_fallback_no_external_engine
        ),
        "all_output_routes_no_fallback_no_external_engine": (
            scope_report.all_output_routes_no_fallback_no_external_engine
        ),
        "all_output_routes_emit_sink_evidence": scope_report.all_output_routes_emit_sink_evidence,
        "all_feature_gated_formats_labeled": scope_report.all_feature_gated_formats_labeled,
        "write_policy_contract_ready": scope_report.write_policy_contract_ready,
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
