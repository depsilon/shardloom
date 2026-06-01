#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Build and validate the user/agent route capability report.

This GAR-RUNTIME-IMPL-6D gate is the direct answer to "given input X and desired
output Y, which ShardLoom route should I use?" It is side-effect-free: it reads
the Python route metadata and verifies that benchmark-range route guidance names
the Vortex normalization point, execution mode, output/evidence path,
materialization/decode boundary, claim boundary, and no-fallback status.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.user_route_capability_report.v1"
GATE_ID = "gar-runtime-impl-6d.user_route_capability_report"

ROUTE_RUNTIME_STATUSES = {
    "scoped_runtime_supported",
    "output_route_pending",
    "runtime_expansion_pending",
    "claim_evidence_pending",
}

REQUIRED_ROUTE_IDS = {
    "local_file_direct_transient_route",
    "local_file_cold_certified_route",
    "local_file_prepare_once_first_query",
    "local_file_prepare_once_batch",
    "prepared_vortex_warm_query",
    "native_vortex_query",
    "local_vortex_primitive_report",
    "generated_rows_local_output",
    "materialized_python_snapshot_reentry",
    "bounded_decoded_preview",
    "schema_quality_preview",
    "quarantine_output_route",
    "broad_sql_python_dataframe_runtime",
    "object_store_lakehouse_runtime",
    "performance_equivalence_evidence",
}

REQUIRED_LOCAL_BENCHMARK_ROUTE_IDS = {
    "local_file_direct_transient_route",
    "local_file_cold_certified_route",
    "local_file_prepare_once_first_query",
    "local_file_prepare_once_batch",
    "prepared_vortex_warm_query",
    "native_vortex_query",
    "local_vortex_primitive_report",
    "generated_rows_local_output",
    "materialized_python_snapshot_reentry",
    "bounded_decoded_preview",
    "schema_quality_preview",
    "broad_sql_python_dataframe_runtime",
    "performance_equivalence_evidence",
}

REQUIRED_OUTPUT_TOKENS = {
    "machine_readable_report",
    "bounded_preview",
    "local_compat_output",
    "prepared_query_result",
    "amortized_prepared_queries",
    "feature_gated_local_vortex_output",
    "local_jsonl",
    "schema_report",
    "benchmark_evidence",
}

REQUIRED_LOCAL_VORTEX_PRIMITIVE_ROUTE_IDS = {
    "vortex_count_all",
    "vortex_count_where",
    "vortex_filter_collect",
    "vortex_filter_limit_collect",
    "vortex_project_collect",
    "vortex_project_limit_collect",
    "vortex_select_star_limit_collect",
    "vortex_filter_project_collect",
    "vortex_filter_project_limit_collect",
}

REQUIRED_LOCAL_VORTEX_PRIMITIVE_COMMANDS = {
    "vortex-run",
    "vortex-count-where",
    "vortex-filter",
    "vortex-project",
    "vortex-filter-project",
}

REQUIRED_LOCAL_VORTEX_LIMIT_ROUTE_IDS = {
    "vortex_filter_limit_collect",
    "vortex_project_limit_collect",
    "vortex_select_star_limit_collect",
    "vortex_filter_project_limit_collect",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/user-route-capability-report.json"),
    )
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def load_report(repo_root: Path) -> Any:
    src = repo_root / "python" / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
    from shardloom import ShardLoomContext

    return ShardLoomContext(client=None).user_route_capability_report()


def row_payload(row: Any) -> dict[str, Any]:
    return {
        "route_id": row.route_id,
        "route_display_name": row.route_display_name,
        "input_family": row.input_family,
        "input_examples": list(row.input_examples),
        "front_doors": list(row.front_doors),
        "desired_outputs": list(row.desired_outputs),
        "recommended_user_surface": row.recommended_user_surface,
        "start_state": row.start_state,
        "vortex_normalization_point": row.vortex_normalization_point,
        "source_route": row.source_route,
        "preparation_route": row.preparation_route,
        "execution_mode": row.execution_mode,
        "execution_route": row.execution_route,
        "output_route": row.output_route,
        "evidence_route": row.evidence_route,
        "materialization_decode_boundary": row.materialization_decode_boundary,
        "route_runtime_status": row.route_runtime_status,
        "benchmark_range": row.benchmark_range,
        "route_comparable_to_external_end_to_end": row.route_comparable_to_external_end_to_end,
        "fallback_attempted": row.fallback_attempted,
        "external_engine_invoked": row.external_engine_invoked,
        "blocker_id": row.blocker_id or "none",
        "owner": row.owner,
        "required_evidence": list(row.required_evidence),
        "claim_gate_status": row.claim_gate_status,
        "performance_claim_allowed": row.performance_claim_allowed,
        "production_claim_allowed": row.production_claim_allowed,
        "spark_replacement_claim_allowed": row.spark_replacement_claim_allowed,
        "claim_boundary": row.claim_boundary,
    }


def primitive_row_payload(row: Any) -> dict[str, Any]:
    return {
        "route_id": row.route_id,
        "primitive": row.primitive,
        "sql_surface": row.sql_surface,
        "python_surface": row.python_surface,
        "dataframe_surface": row.dataframe_surface,
        "context_surface": row.context_surface,
        "session_surface": row.session_surface,
        "cli_command": row.cli_command,
        "cli_args_template": row.cli_args_template,
        "start_state": row.start_state,
        "vortex_normalization_point": row.vortex_normalization_point,
        "execution_mode": row.execution_mode,
        "output_route": row.output_route,
        "evidence_route": row.evidence_route,
        "materialization_decode_boundary": row.materialization_decode_boundary,
        "supports_source_order_limit": row.supports_source_order_limit,
        "route_runtime_status": row.route_runtime_status,
        "fallback_attempted": row.fallback_attempted,
        "external_engine_invoked": row.external_engine_invoked,
        "required_evidence": list(row.required_evidence),
        "claim_gate_status": row.claim_gate_status,
        "claim_boundary": row.claim_boundary,
    }


def validate_local_vortex_primitives(
    report: Any,
    primitive_rows: list[dict[str, Any]],
) -> list[str]:
    blockers: list[str] = []
    by_id = {str(row["route_id"]): row for row in primitive_rows}

    missing = sorted(REQUIRED_LOCAL_VORTEX_PRIMITIVE_ROUTE_IDS - by_id.keys())
    if missing:
        blockers.append("local Vortex primitive route report missing rows: " + ",".join(missing))

    extra = sorted(by_id.keys() - REQUIRED_LOCAL_VORTEX_PRIMITIVE_ROUTE_IDS)
    if extra:
        blockers.append(
            "local Vortex primitive route report has unclassified extra rows: " + ",".join(extra)
        )

    if len(primitive_rows) != len(by_id):
        blockers.append(
            "local Vortex primitive route report has duplicate route ids: "
            f"{len(primitive_rows) - len(by_id)}"
        )

    for row in primitive_rows:
        route_id = str(row["route_id"])
        for field in (
            "primitive",
            "sql_surface",
            "python_surface",
            "dataframe_surface",
            "context_surface",
            "session_surface",
            "cli_command",
            "cli_args_template",
            "start_state",
            "vortex_normalization_point",
            "execution_mode",
            "output_route",
            "evidence_route",
            "materialization_decode_boundary",
            "claim_boundary",
        ):
            value = row.get(field)
            if not isinstance(value, str) or not value.strip():
                blockers.append(f"{route_id}: missing {field}")
        if row.get("cli_command") not in REQUIRED_LOCAL_VORTEX_PRIMITIVE_COMMANDS:
            blockers.append(f"{route_id}: unrecognized cli_command={row.get('cli_command')!r}")
        if row.get("start_state") != "native_vortex_file":
            blockers.append(f"{route_id}: start_state must be native_vortex_file")
        if row.get("vortex_normalization_point") != "native_vortex_boundary":
            blockers.append(f"{route_id}: vortex_normalization_point must be native_vortex_boundary")
        if row.get("execution_mode") != "native_vortex":
            blockers.append(f"{route_id}: execution_mode must be native_vortex")
        if row.get("route_runtime_status") != "scoped_runtime_supported":
            blockers.append(f"{route_id}: route_runtime_status must be scoped_runtime_supported")
        if row.get("fallback_attempted") is not False:
            blockers.append(f"{route_id}: fallback_attempted must be false")
        if row.get("external_engine_invoked") is not False:
            blockers.append(f"{route_id}: external_engine_invoked must be false")
        if row.get("claim_gate_status") != "not_claim_grade":
            blockers.append(f"{route_id}: claim_gate_status must be not_claim_grade")
        required_evidence = row.get("required_evidence")
        if not isinstance(required_evidence, list) or not required_evidence:
            blockers.append(f"{route_id}: missing required_evidence")
        for surface in ("sql_surface", "python_surface", "dataframe_surface", "context_surface", "session_surface"):
            text = str(row.get(surface, ""))
            if "write_vortex" in text:
                blockers.append(f"{route_id}: primitive surface must not advertise write_vortex")

    commands = set(report.command_coverage)
    missing_commands = sorted(REQUIRED_LOCAL_VORTEX_PRIMITIVE_COMMANDS - commands)
    if missing_commands:
        blockers.append(
            "local Vortex primitive route report missing command coverage: "
            + ",".join(missing_commands)
        )

    limit_ids = set(report.source_order_limit_route_ids)
    missing_limit_ids = sorted(REQUIRED_LOCAL_VORTEX_LIMIT_ROUTE_IDS - limit_ids)
    if missing_limit_ids:
        blockers.append(
            "local Vortex primitive route report missing source-order limit routes: "
            + ",".join(missing_limit_ids)
        )

    if report.all_runtime_supported is not True:
        blockers.append("local Vortex primitive route report all_runtime_supported must be true")
    if report.all_no_fallback_no_external_engine is not True:
        blockers.append(
            "local Vortex primitive route report all_no_fallback_no_external_engine must be true"
        )
    return blockers


def validate_rows(report: Any, rows: list[dict[str, Any]]) -> list[str]:
    blockers: list[str] = []
    by_id = {str(row["route_id"]): row for row in rows}

    missing = sorted(REQUIRED_ROUTE_IDS - by_id.keys())
    if missing:
        blockers.append("user route capability report missing rows: " + ",".join(missing))

    extra = sorted(by_id.keys() - REQUIRED_ROUTE_IDS)
    if extra:
        blockers.append("user route capability report has unclassified extra rows: " + ",".join(extra))

    duplicate_count = len(rows) - len(by_id)
    if duplicate_count:
        blockers.append(f"user route capability report has duplicate route ids: {duplicate_count}")

    for row in rows:
        route_id = str(row["route_id"])
        status = str(row["route_runtime_status"])
        if status not in ROUTE_RUNTIME_STATUSES:
            blockers.append(f"{route_id}: invalid route_runtime_status={status!r}")
        for field in (
            "route_display_name",
            "input_family",
            "recommended_user_surface",
            "start_state",
            "vortex_normalization_point",
            "source_route",
            "preparation_route",
            "execution_mode",
            "execution_route",
            "output_route",
            "evidence_route",
            "materialization_decode_boundary",
            "owner",
            "claim_boundary",
        ):
            value = row.get(field)
            if not isinstance(value, str) or not value.strip():
                blockers.append(f"{route_id}: missing {field}")
        for field in ("input_examples", "front_doors", "desired_outputs", "required_evidence"):
            value = row.get(field)
            if not isinstance(value, list) or not value:
                blockers.append(f"{route_id}: missing non-empty {field}")
        if row.get("fallback_attempted") is not False:
            blockers.append(f"{route_id}: fallback_attempted must be false")
        if row.get("external_engine_invoked") is not False:
            blockers.append(f"{route_id}: external_engine_invoked must be false")
        for field in (
            "performance_claim_allowed",
            "production_claim_allowed",
            "spark_replacement_claim_allowed",
        ):
            if row.get(field) is not False:
                blockers.append(f"{route_id}: {field} must be false")
        if row.get("claim_gate_status") != "not_claim_grade":
            blockers.append(f"{route_id}: claim_gate_status must be not_claim_grade")
        if row.get("benchmark_range") is True and status == "unsupported":
            blockers.append(f"{route_id}: benchmark-range ShardLoom route must not be unsupported")

    benchmark_ids = {str(row["route_id"]) for row in rows if row.get("benchmark_range") is True}
    missing_benchmark = sorted(REQUIRED_LOCAL_BENCHMARK_ROUTE_IDS - benchmark_ids)
    if missing_benchmark:
        blockers.append(
            "local benchmark-range route report missing benchmark_range rows: "
            + ",".join(missing_benchmark)
        )

    output_tokens = {
        str(token)
        for row in rows
        for token in row.get("desired_outputs", [])
    }
    missing_outputs = sorted(REQUIRED_OUTPUT_TOKENS - output_tokens)
    if missing_outputs:
        blockers.append("user route report missing desired output tokens: " + ",".join(missing_outputs))

    native_rows = [
        row
        for row in rows
        if row["route_id"] in {"native_vortex_query", "local_vortex_primitive_report"}
    ]
    for row in native_rows:
        if row.get("vortex_normalization_point") != "native_vortex_boundary":
            blockers.append(f"{row['route_id']}: native Vortex rows must start at native_vortex_boundary")
        if "write_vortex" in str(row.get("recommended_user_surface", "")):
            blockers.append(
                f"{row['route_id']}: scoped native Vortex primitive route must not advertise write_vortex"
            )

    prepared_rows = [
        row
        for row in rows
        if row["route_id"]
        in {
            "local_file_cold_certified_route",
            "local_file_prepare_once_first_query",
            "local_file_prepare_once_batch",
        }
    ]
    for row in prepared_rows:
        normalization = str(row.get("vortex_normalization_point", ""))
        if "SourceState" not in normalization or "VortexPreparedState" not in normalization:
            blockers.append(
                f"{row['route_id']}: prepared compatibility route must name SourceState and VortexPreparedState"
            )

    materialized = by_id.get("materialized_python_snapshot_reentry")
    if materialized is not None:
        normalization = str(materialized.get("vortex_normalization_point", ""))
        if "materialized snapshot" not in normalization or "Vortex-preparable" not in normalization:
            blockers.append(
                "materialized_python_snapshot_reentry: must name materialized snapshot re-entry "
                "through a Vortex-preparable route"
            )

    if report.all_no_fallback_no_external_engine is not True:
        blockers.append("user route report all_no_fallback_no_external_engine must be true")
    if report.flexible_anything_claim_allowed is not False:
        blockers.append("user route report flexible_anything_claim_allowed must be false")
    if report.performance_equivalence_claim_allowed is not False:
        blockers.append("user route report performance_equivalence_claim_allowed must be false")
    if report.production_claim_allowed is not False:
        blockers.append("user route report production_claim_allowed must be false")
    if report.spark_replacement_claim_allowed is not False:
        blockers.append("user route report spark_replacement_claim_allowed must be false")
    if report.claim_gate_status != "not_claim_grade":
        blockers.append("user route report claim_gate_status must be not_claim_grade")
    if report.unsupported_local_benchmark_route_ids:
        blockers.append(
            "benchmark-range ShardLoom route ids must not be generically unsupported: "
            + ",".join(report.unsupported_local_benchmark_route_ids)
        )
    return blockers


def build_report(repo_root: Path) -> dict[str, Any]:
    route_report = load_report(repo_root)
    from shardloom import ShardLoomContext

    local_vortex_report = ShardLoomContext(client=None).local_vortex_primitive_route_report()
    rows = [row_payload(row) for row in route_report.rows]
    local_vortex_primitive_rows = [
        primitive_row_payload(row) for row in local_vortex_report.rows
    ]
    blockers = validate_rows(route_report, rows)
    blockers.extend(
        validate_local_vortex_primitives(
            local_vortex_report,
            local_vortex_primitive_rows,
        )
    )
    runtime_status_counts = dict(route_report.route_runtime_status_counts)
    local_benchmark_route_ids = [
        row["route_id"] for row in rows if row["benchmark_range"] is True
    ]

    return {
        "schema_version": SCHEMA_VERSION,
        "gate_id": GATE_ID,
        "status": "passed" if not blockers else "blocked",
        "covered_phase_items": [
            "GAR-RUNTIME-IMPL-6D",
            "GAR-RUNTIME-IMPL-6D-1",
            "GAR-RUNTIME-IMPL-6D-2",
            "CG-20",
            "CG-21",
        ],
        "route_runtime_status_vocabulary": sorted(ROUTE_RUNTIME_STATUSES),
        "route_count": len(rows),
        "route_order": list(route_report.route_order),
        "route_runtime_status_counts": runtime_status_counts,
        "local_benchmark_range_route_ids": local_benchmark_route_ids,
        "local_benchmark_range_route_count": len(local_benchmark_route_ids),
        "unsupported_local_benchmark_route_ids": list(route_report.unsupported_local_benchmark_route_ids),
        "local_vortex_primitive_schema_version": local_vortex_report.schema_version,
        "local_vortex_primitive_route_count": len(local_vortex_primitive_rows),
        "local_vortex_primitive_route_order": list(local_vortex_report.route_order),
        "local_vortex_primitive_command_coverage": list(local_vortex_report.command_coverage),
        "local_vortex_primitive_source_order_limit_route_ids": list(
            local_vortex_report.source_order_limit_route_ids
        ),
        "local_vortex_primitive_all_runtime_supported": local_vortex_report.all_runtime_supported,
        "local_vortex_primitive_all_no_fallback_no_external_engine": (
            local_vortex_report.all_no_fallback_no_external_engine
        ),
        "all_no_fallback_no_external_engine": route_report.all_no_fallback_no_external_engine,
        "flexible_anything_claim_allowed": route_report.flexible_anything_claim_allowed,
        "performance_equivalence_claim_allowed": route_report.performance_equivalence_claim_allowed,
        "production_claim_allowed": route_report.production_claim_allowed,
        "spark_replacement_claim_allowed": route_report.spark_replacement_claim_allowed,
        "claim_gate_status": route_report.claim_gate_status,
        "vortex_normalization_contract": route_report.vortex_normalization_contract,
        "rows": rows,
        "local_vortex_primitive_rows": local_vortex_primitive_rows,
        "acceptance_summary": {
            "all_routes_have_vortex_normalization": all(
                bool(str(row["vortex_normalization_point"]).strip()) for row in rows
            ),
            "all_routes_have_output_and_evidence": all(
                bool(str(row["output_route"]).strip()) and bool(str(row["evidence_route"]).strip())
                for row in rows
            ),
            "all_routes_have_materialization_decode_boundary": all(
                bool(str(row["materialization_decode_boundary"]).strip()) for row in rows
            ),
            "no_generic_unsupported_local_benchmark_route": not route_report.unsupported_local_benchmark_route_ids,
            "all_local_vortex_primitive_routes_supported": (
                local_vortex_report.all_runtime_supported
            ),
            "all_local_vortex_primitive_routes_start_at_native_boundary": all(
                row["vortex_normalization_point"] == "native_vortex_boundary"
                for row in local_vortex_primitive_rows
            ),
            "all_local_vortex_primitive_commands_covered": (
                set(local_vortex_report.command_coverage)
                == REQUIRED_LOCAL_VORTEX_PRIMITIVE_COMMANDS
            ),
            "all_no_fallback_no_external_engine": route_report.all_no_fallback_no_external_engine,
            "claim_gate_status": route_report.claim_gate_status,
            "performance_claim_allowed": False,
            "production_claim_allowed": False,
            "spark_replacement_claim_allowed": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
        },
        "claim_boundary": (
            "This report provides route-selection and runtime-readiness guidance for scoped local "
            "benchmark-range ShardLoom workflows. It does not authorize broad arbitrary "
            "SQL/Python/DataFrame support, production readiness, package publication, performance "
            "equivalence, Spark replacement, or fallback execution."
        ),
        "blockers": blockers,
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output = resolve(repo_root, args.output)
    report = build_report(repo_root)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if report["blockers"]:
        for blocker in report["blockers"]:
            print(f"user route capability blocker: {blocker}")
        return 1
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
