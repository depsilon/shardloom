#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Validate the v1 Vortex runtime scope contract."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from release_report_utils import (
    fail_closed_fields,
    read_text,
    require_markers,
    resolve_path,
    write_json,
)


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.v1_vortex_runtime_scope_report.v1"
DOC_PATH = Path("docs/architecture/v1-vortex-runtime-scope.md")

DOC_MARKERS = (
    "shardloom.v1_vortex_runtime_scope.v1",
    "ShardLoomContext.local_vortex_primitive_route_report()",
    "ShardLoomContext.user_route_capability_report()",
    "native_local_vortex_file",
    "prepared_local_vortex_state",
    "prepared_compatibility_artifact",
    "generated_local_vortex_artifact",
    "vortex_count_all",
    "vortex_count_where",
    "vortex_filter_collect",
    "vortex_project_collect",
    "vortex_filter_project_collect",
    "source-order limit",
    "group_by_aggregation",
    "top_n_per_group",
    "feature_gated_local_vortex_runtime",
    "object_store_vortex_io",
    "table_catalog_vortex_io",
    "generalized_source_sink_api",
    "broad_vortex_sql_dataframe_parity",
    "nested_complex_dtype_general_vortex",
    "vector_device_gpu_vortex_runtime",
    "Vortex-first provider check",
    "use_vortex_native_provider",
    "wrap_vortex_concept",
    "blocked_until_vortex_or_shardloom_evidence",
    "timing_surface",
    "claim_gate_status",
    "not_claim_grade",
    "fallback_attempted=false",
    "external_engine_invoked=false",
)

PUBLIC_DOC_MARKERS = {
    "README.md": (
        "https://shardloom.io/field-guide/runtime-and-io/",
    ),
    "python/README.md": (
        DOC_PATH.as_posix(),
        "local_vortex_primitive_route_report",
        "v1 Vortex runtime scope",
    ),
    "docs/release/public-status-matrix.md": (
        DOC_PATH.as_posix(),
        "Scoped local Vortex primitives",
        "feature-gated local Vortex runtime scope",
    ),
    "docs/release/v1-inclusion-scope-matrix.md": (
        "`PROD-V1-1B`",
        "closed_vortex_runtime_scope",
        DOC_PATH.as_posix(),
    ),
    "website-src/src/content/docs/field-guide/benchmark-methodology.mdx": (
        "ClickBench",
        "does not present a public ranking.",
        "no-fallback evidence",
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/v1-vortex-runtime-scope-report.json"),
    )
    return parser.parse_args()


def load_context_reports(repo_root: Path) -> tuple[Any, Any, dict[str, tuple[str, ...]]]:
    src = repo_root / "python" / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
    from shardloom import ShardLoomContext
    from shardloom import V1_VORTEX_SUPPORTED_PRIMITIVE_ROUTE_IDS
    from shardloom import V1_VORTEX_SUPPORTED_STARTING_STATES
    from shardloom import V1_VORTEX_UNSUPPORTED_BOUNDARY_IDS

    ctx = ShardLoomContext(client=None)
    constants = {
        "supported_primitive_route_ids": tuple(V1_VORTEX_SUPPORTED_PRIMITIVE_ROUTE_IDS),
        "supported_starting_states": tuple(V1_VORTEX_SUPPORTED_STARTING_STATES),
        "unsupported_boundary_ids": tuple(V1_VORTEX_UNSUPPORTED_BOUNDARY_IDS),
    }
    return (
        ctx.local_vortex_primitive_route_report(),
        ctx.user_route_capability_report(),
        constants,
    )


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


def validate_primitive_report(report: Any, constants: dict[str, tuple[str, ...]]) -> list[str]:
    blockers: list[str] = []
    expected_route_ids = set(constants["supported_primitive_route_ids"])
    route_ids = set(report.route_order)

    if report.schema_version != "shardloom.local_vortex_primitive_route_report.v1":
        blockers.append("local Vortex primitive report schema mismatch")
    if report.v1_scope_document != DOC_PATH.as_posix():
        blockers.append("local Vortex primitive report v1 scope document mismatch")
    if tuple(report.v1_supported_route_ids) != constants["supported_primitive_route_ids"]:
        blockers.append("local Vortex primitive v1 route id contract mismatch")
    if tuple(report.v1_supported_starting_states) != constants["supported_starting_states"]:
        blockers.append("local Vortex primitive v1 starting-state contract mismatch")
    if tuple(report.v1_unsupported_boundary_ids) != constants["unsupported_boundary_ids"]:
        blockers.append("local Vortex primitive unsupported-boundary contract mismatch")
    if "feature_gated_local_vortex_runtime" not in report.v1_feature_profile_decision:
        blockers.append("local Vortex primitive feature-profile decision must be feature gated")
    if route_ids != expected_route_ids:
        blockers.append(
            "local Vortex primitive route ids mismatch: "
            + ",".join(sorted(route_ids ^ expected_route_ids))
        )
    if report.v1_scope_ready is not True:
        blockers.append("local Vortex primitive v1_scope_ready must be true")
    if report.all_runtime_supported is not True:
        blockers.append("local Vortex primitive routes must all be runtime-supported")
    if report.all_no_fallback_no_external_engine is not True:
        blockers.append("local Vortex primitive routes must preserve no fallback")

    for row in report.rows:
        route_id = row.route_id
        if row.start_state != "native_vortex_file":
            blockers.append(f"{route_id}: start_state must be native_vortex_file")
        if row.vortex_normalization_point != "native_vortex_boundary":
            blockers.append(f"{route_id}: must start at native_vortex_boundary")
        if row.execution_mode != "native_vortex":
            blockers.append(f"{route_id}: execution_mode must be native_vortex")
        if row.route_runtime_status != "global_runtime_supported":
            blockers.append(f"{route_id}: route_runtime_status must be global_runtime_supported")
        if row.fallback_attempted is not False:
            blockers.append(f"{route_id}: fallback_attempted must be false")
        if row.external_engine_invoked is not False:
            blockers.append(f"{route_id}: external_engine_invoked must be false")
        if row.claim_gate_status != "not_claim_grade":
            blockers.append(f"{route_id}: claim_gate_status must remain not_claim_grade")
        evidence = set(row.required_evidence)
        if "execution_certificate" not in evidence:
            blockers.append(f"{route_id}: required_evidence must include execution_certificate")
        if "native_io_certificate" not in evidence:
            blockers.append(f"{route_id}: required_evidence must include native_io_certificate")
        for text_field in (
            "output_route",
            "evidence_route",
            "materialization_decode_boundary",
            "claim_boundary",
        ):
            if not str(getattr(row, text_field)).strip():
                blockers.append(f"{route_id}: missing {text_field}")
    return blockers


def validate_user_routes(
    report: Any,
    constants: dict[str, tuple[str, ...]],
) -> list[str]:
    blockers: list[str] = []
    if report.v1_vortex_scope_document != DOC_PATH.as_posix():
        blockers.append("user route report v1 Vortex scope document mismatch")
    if tuple(report.v1_vortex_supported_starting_states) != constants["supported_starting_states"]:
        blockers.append("user route report v1 Vortex starting-state contract mismatch")
    if (
        tuple(report.v1_vortex_supported_primitive_route_ids)
        != constants["supported_primitive_route_ids"]
    ):
        blockers.append("user route report v1 Vortex primitive route id mismatch")
    if tuple(report.v1_vortex_unsupported_boundary_ids) != constants["unsupported_boundary_ids"]:
        blockers.append("user route report v1 Vortex unsupported-boundary mismatch")
    if "feature_gated_local_vortex_runtime" not in report.v1_vortex_feature_profile_decision:
        blockers.append("user route report feature-profile decision must be feature gated")
    if report.v1_vortex_scope_ready is not True:
        blockers.append("user route report v1_vortex_scope_ready must be true")
    if report.all_no_fallback_no_external_engine is not True:
        blockers.append("user route report must preserve no fallback")
    expected_ids = {
        "native_vortex_query",
        "object_store_lakehouse_runtime",
    }
    route_ids = set(report.route_order)
    if route_ids != expected_ids or len(report.rows) != len(expected_ids):
        blockers.append("user route ids must contain the native route and external-environment boundary exactly once")

    native = next((row for row in report.rows if row.route_id == "native_vortex_query"), None)
    external = next((row for row in report.rows if row.route_id == "object_store_lakehouse_runtime"), None)
    if native is not None:
        if native.route_runtime_status != "global_runtime_supported":
            blockers.append("native_vortex_query: route_runtime_status must be global_runtime_supported")
        if native.owner != "shared_native_workflow":
            blockers.append("native_vortex_query: owner must be shared_native_workflow")
        if native.fallback_attempted is not False:
            blockers.append("native_vortex_query: fallback_attempted must be false")
        if native.external_engine_invoked is not False:
            blockers.append("native_vortex_query: external_engine_invoked must be false")
        if "source_free" not in native.input_family or "source_free" not in native.start_state:
            blockers.append("native_vortex_query: input contract must include source-free declarations")
        native_inputs = " ".join(native.input_examples)
        for example in (".vortex", "from_rows(", "SELECT 1 AS id"):
            if example not in native_inputs:
                blockers.append(f"native_vortex_query: input examples are missing {example!r}")
        if "native Vortex admission" not in native.vortex_normalization_point or "native_vortex_unified_plan" not in native.vortex_normalization_point:
            blockers.append("native_vortex_query: normalization must name the native unified plan boundary")
        if "complete typed result" not in native.output_route or "committed requested output" not in native.output_route:
            blockers.append("native_vortex_query: output contract must cover complete results and declared outputs")
        for claim in ("performance_claim_allowed", "production_claim_allowed", "spark_replacement_claim_allowed"):
            if getattr(native, claim) is not False:
                blockers.append(f"native_vortex_query: {claim} must remain false")
    if external is not None:
        if external.route_runtime_status != "external_environment_gate_pending":
            blockers.append("object_store_lakehouse_runtime must remain explicitly gated")
        if external.execution_mode != "external_environment_gate_pending":
            blockers.append("external environment route must use the gated execution mode")
        if external.blocker_id != "cg9.cg10.cg21.production_io_front_door_missing":
            blockers.append("external environment route must retain its production I/O blocker")
        if external.fallback_attempted is not False:
            blockers.append("external environment route: fallback_attempted must be false")
        if external.external_engine_invoked is not False:
            blockers.append("external environment route: external_engine_invoked must be false")

    expected_front_doors = {
        "local_source_vortex_middle_front_door": ("local_compat_file", "ctx.read_csv("),
        "native_vortex_front_door": ("native_vortex_file", "ctx.read_vortex("),
        "declared_memory_front_door": ("declared_memory", "ctx.from_rows("),
        "source_free_sql_front_door": ("source_free", "ctx.sql("),
    }
    front_doors = tuple(report.public_front_door_route_rows)
    doors_by_id = {row.front_door_id: row for row in front_doors}
    if set(doors_by_id) != set(expected_front_doors) or len(front_doors) != 4:
        blockers.append("user route report must declare the four public input examples exactly once")
    for door_id, (family, surface_marker) in expected_front_doors.items():
        row = doors_by_id.get(door_id)
        if row is None:
            continue
        if row.input_family != family:
            blockers.append(f"{door_id}: input_family must be {family}")
        if surface_marker not in row.public_user_surface:
            blockers.append(f"{door_id}: public input example is missing {surface_marker}")
        if row.owning_route_id != "native_vortex_query":
            blockers.append(f"{door_id}: must use native_vortex_query")
        if row.execution_mode != "native_vortex":
            blockers.append(f"{door_id}: execution_mode must be native_vortex")
        if row.fallback_attempted is not False:
            blockers.append(f"{door_id}: fallback_attempted must be false")
        if row.external_engine_invoked is not False:
            blockers.append(f"{door_id}: external_engine_invoked must be false")
        if "native_vortex_unified_plan" not in row.vortex_normalization_point or not row.output_route:
            blockers.append(f"{door_id}: normalization and output contracts are required")

    for route_id in expected_ids:
        row = next((item for item in report.rows if item.route_id == route_id), None)
        if row is None:
            continue
        if row.fallback_attempted is not False:
            blockers.append(f"{route_id}: fallback_attempted must be false")
        if row.external_engine_invoked is not False:
            blockers.append(f"{route_id}: external_engine_invoked must be false")
        if row.claim_gate_status != "not_claim_grade":
            blockers.append(f"{route_id}: claim_gate_status must remain not_claim_grade")
        if not row.vortex_normalization_point or not row.materialization_decode_boundary:
            blockers.append(f"{route_id}: normalization and materialization boundaries are required")
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


def build_report(repo_root: Path) -> dict[str, Any]:
    (
        primitive_report,
        user_route_report,
        constants,
    ) = load_context_reports(repo_root)
    primitive_rows = [primitive_row_payload(row) for row in primitive_report.rows]
    blockers = []
    blockers.extend(validate_primitive_report(primitive_report, constants))
    blockers.extend(validate_user_routes(user_route_report, constants))
    blockers.extend(validate_docs(repo_root))

    passed = not blockers
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if passed else "failed",
        "v1_scope_document": DOC_PATH.as_posix(),
        "supported_starting_states": list(constants["supported_starting_states"]),
        "supported_primitive_route_ids": list(constants["supported_primitive_route_ids"]),
        "unsupported_boundary_ids": list(constants["unsupported_boundary_ids"]),
        "evidence_class": "declarative_specification",
        "runtime_execution_performed": False,
        "performance_evidence_produced": False,
        "feature_profile_decision": primitive_report.v1_feature_profile_decision,
        "local_vortex_primitive_schema_version": primitive_report.schema_version,
        "local_vortex_primitive_route_count": len(primitive_rows),
        "local_vortex_primitive_rows": primitive_rows,
        "local_vortex_primitive_all_runtime_supported": (
            primitive_report.all_runtime_supported
        ),
        "local_vortex_primitive_all_no_fallback_no_external_engine": (
            primitive_report.all_no_fallback_no_external_engine
        ),
        "local_vortex_primitive_v1_scope_ready": primitive_report.v1_scope_ready,
        "user_route_v1_vortex_scope_ready": user_route_report.v1_vortex_scope_ready,
        "user_route_ids": list(user_route_report.route_order),
        "user_route_rows": [
            {
                "route_id": row.route_id,
                "owner": row.owner,
                "input_family": row.input_family,
                "input_examples": list(row.input_examples),
                "start_state": row.start_state,
                "vortex_normalization_point": row.vortex_normalization_point,
                "execution_mode": row.execution_mode,
                "output_route": row.output_route,
                "route_runtime_status": row.route_runtime_status,
                "blocker_id": row.blocker_id or "none",
                "fallback_attempted": row.fallback_attempted,
                "external_engine_invoked": row.external_engine_invoked,
            }
            for row in user_route_report.rows
        ],
        "public_input_examples": [
            {
                "front_door_id": row.front_door_id,
                "owning_route_id": row.owning_route_id,
                "input_family": row.input_family,
                "public_user_surface": row.public_user_surface,
                "vortex_normalization_point": row.vortex_normalization_point,
                "execution_mode": row.execution_mode,
                "output_route": row.output_route,
                "fallback_attempted": row.fallback_attempted,
                "external_engine_invoked": row.external_engine_invoked,
            }
            for row in user_route_report.public_front_door_route_rows
        ],
        "all_no_fallback_no_external_engine": (
            primitive_report.all_no_fallback_no_external_engine
            and user_route_report.all_no_fallback_no_external_engine
        ),
        "claim_gate_status": "not_claim_grade",
        "blockers": blockers,
        **fail_closed_fields(),
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output = resolve_path(repo_root, args.output)
    report = build_report(repo_root)
    write_json(output, report)
    print(output)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
