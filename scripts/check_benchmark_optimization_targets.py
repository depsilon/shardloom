#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Extract benchmark-driven runtime optimization targets from promoted artifacts.

This validator reads committed benchmark artifacts only. It does not execute benchmarks, import
external engines, mutate benchmark rows, or make a performance claim.
"""

from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from release_report_utils import fail_closed_fields, load_json, resolve_path, write_json


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.benchmark_optimization_targets_report.v1"
DEFAULT_ARTIFACT = Path("website/assets/benchmarks/latest/benchmark-results.json")
DEFAULT_OUTPUT = Path("target/benchmark-optimization-targets-report.json")
DEFAULT_TOP_N = 8
NEXT_IMPLEMENTATION_SLICE = "none"
ROUTE_SHARE_ADDITIVE_TOLERANCE = 1.000001
ROUTE_STAGE_FIELD_TO_ID = {
    "source_admission_ms": "source_admission",
    "source_read_ms": "source_read",
    "source_parse_or_columnar_decode_ms": "source_parse_or_decode",
    "source_to_vortex_array_ms": "source_to_vortex_array",
    "vortex_write_ms": "vortex_write",
    "exclusive_vortex_digest_ms": "vortex_digest",
    "vortex_reopen_or_verify_ms": "vortex_reopen_verify",
    "prepared_state_lookup_or_create_ms": "prepared_state_lookup_or_create",
    "vortex_scan_ms": "vortex_scan",
    "operator_compute_ms": "operator_compute",
    "result_sink_write_ms": "result_sink_write",
    "evidence_render_ms": "evidence_render",
    "cli_process_wall_millis": "cli_process_wall",
}
INCLUDED_HOT_RUNTIME_STAGE_CLASS = "included_hot_runtime"


RowPredicate = Callable[[dict[str, Any]], bool]


@dataclass(frozen=True)
class OptimizationTarget:
    target_id: str
    stage_field: str
    route_metric_field: str
    predicate: RowPredicate
    rationale: str
    next_slice: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--artifact", type=Path, default=DEFAULT_ARTIFACT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)
    return parser.parse_args()


def boolish_false(value: Any) -> bool:
    if value is False:
        return True
    if isinstance(value, str):
        return value.strip().lower() == "false"
    return False


def numeric(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = round((len(ordered) - 1) * q)
    return ordered[index]


def is_shardloom_row(row: dict[str, Any]) -> bool:
    return str(row.get("engine", "")).startswith("shardloom")


def is_hot_runtime_row(row: dict[str, Any]) -> bool:
    return row.get("timing_surface") == "hot_runtime"


def is_publication_proof_row(row: dict[str, Any]) -> bool:
    return row.get("timing_surface") == "publication_proof"


def is_materialized_temporary_operator(row: dict[str, Any]) -> bool:
    if row.get("operator_temporary_materialization_used") is True:
        return True
    materialized = str(row.get("materialized_temporary_operators", "")).strip().lower()
    return bool(materialized and materialized != "none")


def unpack_stage_map(value: Any) -> dict[str, str]:
    if not isinstance(value, str):
        return {}
    result: dict[str, str] = {}
    for token in value.split(";"):
        if ":" not in token:
            continue
        key, stage_value = token.split(":", 1)
        if key.strip():
            result[key.strip()] = stage_value.strip()
    return result


def stage_included_in_hot_runtime(row: dict[str, Any], stage_field: str) -> bool:
    stage_id = ROUTE_STAGE_FIELD_TO_ID.get(stage_field)
    if stage_id is None:
        return False
    classes = unpack_stage_map(row.get("route_timing_stage_inclusion_classes"))
    if not classes:
        return False
    return classes.get(stage_id) in {"included", INCLUDED_HOT_RUNTIME_STAGE_CLASS}


def stage_contract_status(row: dict[str, Any], target: OptimizationTarget) -> str:
    stage_value = numeric(row.get(target.stage_field))
    if stage_value is None or stage_value <= 0.0:
        return "stage_zero_or_missing"
    if not stage_included_in_hot_runtime(row, target.stage_field):
        return "stage_not_included_in_hot_runtime"
    route_value = numeric(row.get(target.route_metric_field))
    if route_value is None or route_value <= 0.0:
        return "route_metric_missing"
    if stage_value > route_value * ROUTE_SHARE_ADDITIVE_TOLERANCE:
        return "non_additive_stage_exceeds_route_total"
    return "included_additive"


def stage_route_share(row: dict[str, Any], target: OptimizationTarget) -> float | None:
    if stage_contract_status(row, target) != "included_additive":
        return None
    route_value = numeric(row.get(target.route_metric_field))
    stage_value = numeric(row.get(target.stage_field))
    if route_value is None or route_value <= 0.0 or stage_value is None:
        return None
    return stage_value / route_value


def hot_shardloom_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in rows if is_shardloom_row(row) and is_hot_runtime_row(row)]


def chunked_published_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    chunks = payload.get("published_benchmark_row_chunks")
    if not isinstance(chunks, list):
        return []
    rows: list[dict[str, Any]] = []
    for chunk in chunks:
        if not isinstance(chunk, dict):
            continue
        path_text = chunk.get("path")
        if not isinstance(path_text, str) or not path_text:
            continue
        path = ROOT / path_text
        if not path.exists():
            continue
        chunk_payload = load_json(path)
        chunk_rows = (
            chunk_payload.get("rows") if isinstance(chunk_payload, dict) else chunk_payload
        )
        if isinstance(chunk_rows, list):
            rows.extend(row for row in chunk_rows if isinstance(row, dict))
    return rows


def published_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if str(payload.get("published_benchmark_rows_inlined") or "") == "summary_only":
        rows = chunked_published_rows(payload)
        if rows:
            return rows
    rows = payload.get("published_benchmark_rows") or payload.get("rows") or []
    return [row for row in rows if isinstance(row, dict)]


def row_identity(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "engine": row.get("engine"),
        "storage_format": row.get("storage_format"),
        "scenario_name": row.get("scenario_name"),
        "route_lane_id": row.get("route_lane_id"),
        "timing_surface": row.get("timing_surface"),
        "actual_evidence_tier": row.get("actual_evidence_tier"),
        "claim_gate_status": row.get("claim_gate_status"),
    }


def top_rows(
    rows: list[dict[str, Any]],
    *,
    route_metric_field: str,
    stage_field: str,
    top_n: int,
) -> list[dict[str, Any]]:
    projection_target = OptimizationTarget(
        target_id="_row_contract",
        stage_field=stage_field,
        route_metric_field=route_metric_field,
        predicate=lambda candidate: True,
        rationale="row contract projection",
        next_slice="row contract projection",
    )
    ranked = sorted(
        rows,
        key=lambda row: numeric(row.get(route_metric_field)) or 0.0,
        reverse=True,
    )
    result: list[dict[str, Any]] = []
    for row in ranked[:top_n]:
        result.append(
            {
                **row_identity(row),
                route_metric_field: numeric(row.get(route_metric_field)),
                stage_field: numeric(row.get(stage_field)),
                "query_runtime_millis": numeric(row.get("query_runtime_millis")),
                "source_read_ms": numeric(row.get("source_read_ms")),
                "source_parse_or_columnar_decode_ms": numeric(
                    row.get("source_parse_or_columnar_decode_ms")
                ),
                "vortex_write_ms": numeric(row.get("vortex_write_ms")),
                "prepared_state_lookup_or_create_ms": numeric(
                    row.get("prepared_state_lookup_or_create_ms")
                ),
                "operator_compute_ms": numeric(row.get("operator_compute_ms")),
                "stage_contract_status": stage_contract_status(
                    row, projection_target
                ),
                "stage_route_share": stage_route_share(row, projection_target),
            }
        )
    return result


def target_summary(
    target: OptimizationTarget,
    rows: list[dict[str, Any]],
    *,
    top_n: int,
) -> dict[str, Any]:
    target_rows = [row for row in rows if target.predicate(row)]
    contract_statuses = {
        status: sum(
            1 for row in target_rows if stage_contract_status(row, target) == status
        )
        for status in (
            "included_additive",
            "non_additive_stage_exceeds_route_total",
            "stage_not_included_in_hot_runtime",
            "route_metric_missing",
            "stage_zero_or_missing",
        )
    }
    included_additive_rows = [
        row for row in target_rows if stage_contract_status(row, target) == "included_additive"
    ]
    stage_values = [
        value
        for value in (numeric(row.get(target.stage_field)) for row in included_additive_rows)
        if value is not None
    ]
    nonzero_stage_values = [value for value in stage_values if value > 0]
    route_values = [
        value
        for value in (numeric(row.get(target.route_metric_field)) for row in target_rows)
        if value is not None
    ]
    evidence_present = bool(target_rows and nonzero_stage_values)
    if evidence_present:
        target_status = "evidence_present"
        target_evidence_class = "measured_hotspot"
        diagnostic_reason = "included_additive_stage_timing_present_in_hot_runtime_rows"
    elif target_rows and any(
        status
        in {
            "non_additive_stage_exceeds_route_total",
            "stage_not_included_in_hot_runtime",
            "route_metric_missing",
        }
        and count
        for status, count in contract_statuses.items()
    ):
        target_status = "diagnostic_stage_excluded_or_non_additive"
        target_evidence_class = "timing_contract_blocked"
        diagnostic_reason = "stage_timing_not_included_or_non_additive_for_hot_runtime"
    elif target_rows:
        target_status = "diagnostic_stage_zero_or_retired"
        target_evidence_class = "diagnostic_absent_or_retired"
        diagnostic_reason = "matching_hot_runtime_rows_have_zero_or_missing_stage_timing"
    else:
        target_status = "diagnostic_absent_or_retired"
        target_evidence_class = "diagnostic_absent_or_retired"
        diagnostic_reason = "no_matching_hot_runtime_rows_in_promoted_artifact"
    return {
        "target_id": target.target_id,
        "status": target_status,
        "target_evidence_class": target_evidence_class,
        "evidence_present": evidence_present,
        "release_blocker": False,
        "diagnostic_only": True,
        "diagnostic_reason": diagnostic_reason,
        "target_disappearance_policy": "diagnostic_absent_or_retired_not_release_blocker",
        "stage_field": target.stage_field,
        "route_metric_field": target.route_metric_field,
        "row_count": len(target_rows),
        "included_additive_stage_row_count": len(included_additive_rows),
        "non_additive_stage_row_count": contract_statuses[
            "non_additive_stage_exceeds_route_total"
        ],
        "excluded_stage_row_count": contract_statuses[
            "stage_not_included_in_hot_runtime"
        ],
        "stage_contract_status_counts": contract_statuses,
        "nonzero_stage_row_count": len(nonzero_stage_values),
        "stage_avg_ms": round(sum(stage_values) / len(stage_values), 6) if stage_values else None,
        "stage_p95_ms": percentile(stage_values, 0.95),
        "stage_max_ms": max(stage_values) if stage_values else None,
        "route_metric_p95_ms": percentile(route_values, 0.95),
        "route_metric_max_ms": max(route_values) if route_values else None,
        "top_rows": top_rows(
            target_rows,
            route_metric_field=target.route_metric_field,
            stage_field=target.stage_field,
            top_n=top_n,
        ),
        "rationale": target.rationale,
        "next_slice": target.next_slice,
        "claim_boundary": "diagnostic_only_no_performance_claim",
    }


def targets() -> tuple[OptimizationTarget, ...]:
    return (
        OptimizationTarget(
            target_id="jsonl_parse_decode_hot_runtime",
            stage_field="source_parse_or_columnar_decode_ms",
            route_metric_field="hot_route_total_ms",
            predicate=lambda row: row.get("storage_format") == "jsonl",
            rationale="JSONL rows show parse/decode as a hot-runtime contributor.",
            next_slice=(
                "optimize remaining JSONL typed text decode now that projected reads "
                "measure row assembly at zero."
            ),
        ),
        OptimizationTarget(
            target_id="avro_hot_runtime_outliers",
            stage_field="source_parse_or_columnar_decode_ms",
            route_metric_field="hot_route_total_ms",
            predicate=lambda row: row.get("storage_format") == "avro",
            rationale="AVRO rows contain the largest current hot-runtime outliers.",
            next_slice="profile AVRO import/decode and Vortex handoff before optimizing unrelated code.",
        ),
        OptimizationTarget(
            target_id="prepared_state_lookup_or_create",
            stage_field="prepared_state_lookup_or_create_ms",
            route_metric_field="hot_route_total_ms",
            predicate=lambda row: row.get("route_lane_id")
            in {"prepare_once_first_query", "prepare_once_batch"},
            rationale="Prepare-once rows expose state lookup/create cost separately from query runtime.",
            next_slice="separate create, lookup, digest, and manifest reuse work in prepared-state timing.",
        ),
        OptimizationTarget(
            target_id="vortex_write_and_reopen_verify",
            stage_field="vortex_write_ms",
            route_metric_field="hot_route_total_ms",
            predicate=lambda row: numeric(row.get("vortex_write_ms")) is not None,
            rationale="Cold and prepare routes still spend material time writing Vortex artifacts.",
            next_slice="continue writer context reuse, batching, digest, and reopen/verify attribution.",
        ),
        OptimizationTarget(
            target_id="source_read_scout_timing",
            stage_field="source_read_ms",
            route_metric_field="hot_route_total_ms",
            predicate=lambda row: numeric(row.get("source_read_ms")) is not None,
            rationale="Source-read timing is split into scout substages for optimization targeting.",
            next_slice=(
                "use populated scout-stage fields to target byte acquisition, typed decode, "
                "and columnar handoff in phase-specific work."
            ),
        ),
        OptimizationTarget(
            target_id="operator_materialization",
            stage_field="operator_compute_ms",
            route_metric_field="hot_route_total_ms",
            predicate=is_materialized_temporary_operator,
            rationale="Temporary materialized operators identify residual native gaps before hot claims.",
            next_slice="prioritize encoded-native kernels or explicit residual boundaries by operator family.",
        ),
    )


def validate_rows(rows: list[dict[str, Any]]) -> list[str]:
    blockers: list[str] = []
    shardloom_rows = [row for row in rows if is_shardloom_row(row)]
    if not shardloom_rows:
        blockers.append("no ShardLoom benchmark rows found")
    hot_rows = [row for row in shardloom_rows if is_hot_runtime_row(row)]
    publication_rows = [row for row in shardloom_rows if is_publication_proof_row(row)]
    if not publication_rows:
        blockers.append("no ShardLoom publication_proof rows found")
    for index, row in enumerate(shardloom_rows):
        if not boolish_false(row.get("fallback_attempted")):
            blockers.append(f"ShardLoom row {index} must preserve fallback_attempted=false")
        if not boolish_false(row.get("external_engine_invoked")):
            blockers.append(f"ShardLoom row {index} must preserve external_engine_invoked=false")
    return blockers


def build_report(artifact_path: Path, *, top_n: int = DEFAULT_TOP_N) -> dict[str, Any]:
    payload = load_json(artifact_path)
    rows = published_rows(payload if isinstance(payload, dict) else {})
    blockers = validate_rows(rows)
    hot_rows = hot_shardloom_rows(rows)
    publication_rows = [
        row
        for row in rows
        if is_shardloom_row(row) and is_publication_proof_row(row)
    ]
    summaries = [target_summary(target, hot_rows, top_n=top_n) for target in targets()]
    evidence_present_targets = [
        summary["target_id"]
        for summary in summaries
        if summary["evidence_present"]
    ]
    timing_contract_blocked_targets = [
        summary["target_id"]
        for summary in summaries
        if summary["target_evidence_class"] == "timing_contract_blocked"
    ]
    diagnostic_absent_or_retired_targets = [
        summary["target_id"]
        for summary in summaries
        if summary["target_evidence_class"] == "diagnostic_absent_or_retired"
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if not blockers else "failed",
        "artifact_path": artifact_path.as_posix(),
        "benchmark_schema_version": payload.get("schema_version")
        if isinstance(payload, dict)
        else None,
        "benchmark_profile": payload.get("benchmark_profile") if isinstance(payload, dict) else None,
        "published_benchmark_row_count": len(rows),
        "shardloom_hot_runtime_row_count": len(hot_rows),
        "shardloom_publication_proof_row_count": len(publication_rows),
        "target_count": len(summaries),
        "evidence_present_target_count": len(evidence_present_targets),
        "evidence_present_targets": evidence_present_targets,
        "timing_contract_blocked_target_count": len(timing_contract_blocked_targets),
        "timing_contract_blocked_targets": timing_contract_blocked_targets,
        "diagnostic_absent_or_retired_target_count": len(
            diagnostic_absent_or_retired_targets
        ),
        "diagnostic_absent_or_retired_targets": diagnostic_absent_or_retired_targets,
        "release_blocking_target_count": 0,
        "release_blocking_targets": [],
        "target_disappearance_policy": "diagnostic_absent_or_retired_not_release_blocker",
        "targets": summaries,
        "next_implementation_slice": NEXT_IMPLEMENTATION_SLICE,
        "claim_boundary": "diagnostic_only_no_performance_claim",
        "blockers": blockers,
        **fail_closed_fields(),
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    artifact = resolve_path(repo_root, args.artifact)
    output = resolve_path(repo_root, args.output)
    report = build_report(artifact, top_n=args.top_n)
    write_json(output, report)
    print(output)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
