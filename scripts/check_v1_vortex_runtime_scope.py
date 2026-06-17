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
    "ShardLoomContext.native_vortex_provider_route_certificate_report()",
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
    "native_vortex_user_aggregate",
    "native_vortex_user_join",
    "native_vortex_user_top_n",
    "native_vortex_user_cast",
    "native_vortex_user_contains",
    "native_vortex_user_sink",
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
        DOC_PATH.as_posix(),
        "v1 Vortex runtime scope",
        "feature-gated local Vortex",
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
    "website-src/src/components/BenchmarkDashboard.astro": (
        DOC_PATH.as_posix(),
        "v1 Vortex runtime scope",
        "local_vortex_primitive_route_report",
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


def load_context_reports(repo_root: Path) -> tuple[Any, Any, Any, Any, Any]:
    src = repo_root / "python" / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
    from shardloom import ShardLoomContext
    from shardloom import V1_VORTEX_PROVIDER_ROUTE_IDS
    from shardloom import V1_VORTEX_PROVIDER_SCENARIO_IDS
    from shardloom import V1_VORTEX_SUPPORTED_BENCHMARK_SCENARIO_IDS
    from shardloom import V1_VORTEX_SUPPORTED_PRIMITIVE_ROUTE_IDS
    from shardloom import V1_VORTEX_SUPPORTED_STARTING_STATES
    from shardloom import V1_VORTEX_UNSUPPORTED_BOUNDARY_IDS

    ctx = ShardLoomContext(client=None)
    constants = {
        "supported_benchmark_scenario_ids": tuple(V1_VORTEX_SUPPORTED_BENCHMARK_SCENARIO_IDS),
        "provider_route_ids": tuple(V1_VORTEX_PROVIDER_ROUTE_IDS),
        "provider_scenario_ids": tuple(V1_VORTEX_PROVIDER_SCENARIO_IDS),
        "supported_primitive_route_ids": tuple(V1_VORTEX_SUPPORTED_PRIMITIVE_ROUTE_IDS),
        "supported_starting_states": tuple(V1_VORTEX_SUPPORTED_STARTING_STATES),
        "unsupported_boundary_ids": tuple(V1_VORTEX_UNSUPPORTED_BOUNDARY_IDS),
    }
    return (
        ctx.local_vortex_primitive_route_report(),
        ctx.native_vortex_provider_route_certificate_report(),
        ctx.user_route_capability_report(),
        ctx.local_file_benchmark_route_report(),
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


def local_file_benchmark_row_payload(row: Any) -> dict[str, Any]:
    return {
        "scenario_id": row.scenario_id,
        "route_id": row.route_id,
        "start_state": row.start_state,
        "vortex_normalization_point": row.vortex_normalization_point,
        "preparation_route": row.preparation_route,
        "selected_execution_mode": row.selected_execution_mode,
        "output_route": row.output_route,
        "evidence_route": row.evidence_route,
        "materialization_decode_boundary": row.materialization_decode_boundary,
        "route_runtime_status": row.route_runtime_status,
        "fallback_attempted": row.fallback_attempted,
        "external_engine_invoked": row.external_engine_invoked,
        "required_evidence": list(row.required_evidence),
        "claim_gate_status": row.claim_gate_status,
        "claim_boundary": row.claim_boundary,
    }


def provider_route_row_payload(row: Any) -> dict[str, Any]:
    return {
        "route_id": row.route_id,
        "operation_family": row.operation_family,
        "provider_scenario": row.provider_scenario,
        "benchmark_scenario_id": row.benchmark_scenario_id,
        "python_surface": row.python_surface,
        "sql_surface": row.sql_surface,
        "required_right_input": row.required_right_input,
        "right_input_contract": row.right_input_contract,
        "resolved_internal_command": row.resolved_internal_command,
        "feature_gate": row.feature_gate,
        "start_state": row.start_state,
        "vortex_normalization_point": row.vortex_normalization_point,
        "execution_policy": row.execution_policy,
        "typed_result_contract": row.typed_result_contract,
        "typed_sink_contract": row.typed_sink_contract,
        "decode_materialization_boundary": row.decode_materialization_boundary,
        "output_route": row.output_route,
        "evidence_route": row.evidence_route,
        "route_certificate_status": row.route_certificate_status,
        "route_certificate_source": row.route_certificate_source,
        "benchmark_route_equivalence": row.benchmark_route_equivalence,
        "route_runtime_status": row.route_runtime_status,
        "fallback_attempted": row.fallback_attempted,
        "external_engine_invoked": row.external_engine_invoked,
        "required_evidence": list(row.required_evidence),
        "claim_gate_status": row.claim_gate_status,
        "performance_claim_allowed": row.performance_claim_allowed,
        "production_claim_allowed": row.production_claim_allowed,
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
        if row.route_runtime_status != "scoped_runtime_supported":
            blockers.append(f"{route_id}: route_runtime_status must be scoped_runtime_supported")
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


def validate_provider_route_report(
    report: Any,
    constants: dict[str, tuple[str, ...]],
) -> list[str]:
    blockers: list[str] = []
    if (
        report.schema_version
        != "shardloom.native_vortex_provider_route_certificate_report.v1"
    ):
        blockers.append("native Vortex provider certificate report schema mismatch")
    if report.v1_scope_document != DOC_PATH.as_posix():
        blockers.append("native Vortex provider report v1 scope document mismatch")
    if tuple(report.v1_provider_route_ids) != constants["provider_route_ids"]:
        blockers.append("native Vortex provider route id contract mismatch")
    if tuple(report.v1_provider_scenario_ids) != constants["provider_scenario_ids"]:
        blockers.append("native Vortex provider scenario id contract mismatch")
    if report.feature_gate != "vortex-production-runtime":
        blockers.append("native Vortex provider feature gate mismatch")
    if report.v1_scope_ready is not True:
        blockers.append("native Vortex provider v1_scope_ready must be true")
    if report.all_runtime_supported is not True:
        blockers.append("native Vortex provider routes must all be runtime-supported")
    if report.all_route_certificates_current is not True:
        blockers.append("native Vortex provider route certificates must be current")
    if report.all_no_fallback_no_external_engine is not True:
        blockers.append("native Vortex provider routes must preserve no fallback")
    if report.general_multi_input_join_claim_allowed is not False:
        blockers.append("native Vortex provider report must not claim arbitrary joins")
    if report.performance_claim_allowed is not False:
        blockers.append("native Vortex provider report must not permit performance claims")
    if report.production_claim_allowed is not False:
        blockers.append("native Vortex provider report must not permit production claims")

    expected_unique_routes = set(constants["provider_route_ids"])
    actual_unique_routes = set(dict.fromkeys(report.route_order))
    if actual_unique_routes != expected_unique_routes:
        blockers.append(
            "native Vortex provider route ids mismatch: "
            + ",".join(sorted(actual_unique_routes ^ expected_unique_routes))
        )
    expected_scenarios = set(constants["provider_scenario_ids"])
    actual_scenarios = set(dict.fromkeys(report.scenario_order))
    if actual_scenarios != expected_scenarios:
        blockers.append(
            "native Vortex provider scenarios mismatch: "
            + ",".join(sorted(actual_scenarios ^ expected_scenarios))
        )

    right_input_scenarios = {"hash-join"}
    sink_routes = {"native_vortex_user_sink"}
    for row in report.rows:
        row_id = f"{row.route_id}/{row.provider_scenario}"
        if row.start_state != "native_vortex_file":
            blockers.append(f"{row_id}: start_state must be native_vortex_file")
        if row.vortex_normalization_point != "native_vortex_boundary":
            blockers.append(f"{row_id}: must start at native_vortex_boundary")
        if row.execution_policy != "native_vortex":
            blockers.append(f"{row_id}: execution_policy must be native_vortex")
        if row.resolved_internal_command != "vortex-production-runtime-run":
            blockers.append(f"{row_id}: resolved_internal_command mismatch")
        if row.route_runtime_status != "production_admitted_local_workflow":
            blockers.append(
                f"{row_id}: route_runtime_status must be production_admitted_local_workflow"
            )
        if row.route_certificate_status != "current":
            blockers.append(f"{row_id}: route_certificate_status must be current")
        if row.fallback_attempted is not False:
            blockers.append(f"{row_id}: fallback_attempted must be false")
        if row.external_engine_invoked is not False:
            blockers.append(f"{row_id}: external_engine_invoked must be false")
        if row.claim_gate_status != "not_claim_grade":
            blockers.append(f"{row_id}: claim_gate_status must remain not_claim_grade")
        if row.performance_claim_allowed is not False:
            blockers.append(f"{row_id}: performance_claim_allowed must be false")
        if row.production_claim_allowed is not False:
            blockers.append(f"{row_id}: production_claim_allowed must be false")
        evidence = set(row.required_evidence)
        for required in (
            "execution_certificate",
            "native_io_certificate",
            "provider_route_certificate",
            "fallback_disabled",
        ):
            if required not in evidence:
                blockers.append(f"{row_id}: required_evidence must include {required}")
        if row.provider_scenario in right_input_scenarios and not row.required_right_input:
            blockers.append(f"{row_id}: required_right_input must be true")
        if row.provider_scenario not in right_input_scenarios and row.required_right_input:
            blockers.append(f"{row_id}: required_right_input must be false")
        if row.route_id in sink_routes:
            if row.typed_sink_contract != "native_vortex_result_sink_with_replay_verified_artifact":
                blockers.append(f"{row_id}: sink row must expose native Vortex sink contract")
        elif row.typed_sink_contract != "not_applicable_collect":
            blockers.append(f"{row_id}: collect row must not expose a sink contract")
    return blockers


def validate_user_routes(
    report: Any,
    local_file_report: Any,
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
    if (
        tuple(report.v1_vortex_supported_benchmark_scenario_ids)
        != constants["supported_benchmark_scenario_ids"]
    ):
        blockers.append("user route report v1 Vortex benchmark scenario id mismatch")
    if tuple(report.v1_vortex_unsupported_boundary_ids) != constants["unsupported_boundary_ids"]:
        blockers.append("user route report v1 Vortex unsupported-boundary mismatch")
    if "feature_gated_local_vortex_runtime" not in report.v1_vortex_feature_profile_decision:
        blockers.append("user route report feature-profile decision must be feature gated")
    if report.v1_vortex_scope_ready is not True:
        blockers.append("user route report v1_vortex_scope_ready must be true")
    if report.all_no_fallback_no_external_engine is not True:
        blockers.append("user route report must preserve no fallback")
    if report.unsupported_local_benchmark_route_ids:
        blockers.append("user route report must have zero unsupported local benchmark routes")

    required_user_routes = {
        "local_file_prepare_once_first_query": "scoped_runtime_supported",
        "local_file_prepare_once_batch": "scoped_runtime_supported",
        "prepared_vortex_warm_query": "scoped_runtime_supported",
        "native_vortex_query": "scoped_runtime_supported",
        "local_vortex_primitive_report": "scoped_runtime_supported",
        "generated_rows_local_output": "scoped_runtime_supported",
    }
    for route_id, status in required_user_routes.items():
        row = report.route(route_id)
        if row.route_runtime_status != status:
            blockers.append(f"{route_id}: route_runtime_status must be {status}")
        if row.fallback_attempted is not False:
            blockers.append(f"{route_id}: fallback_attempted must be false")
        if row.external_engine_invoked is not False:
            blockers.append(f"{route_id}: external_engine_invoked must be false")
        if not row.vortex_normalization_point:
            blockers.append(f"{route_id}: must name vortex_normalization_point")
        if not row.materialization_decode_boundary:
            blockers.append(f"{route_id}: must name materialization_decode_boundary")

    scenario_ids = tuple(local_file_report.scenario_ids)
    if scenario_ids != constants["supported_benchmark_scenario_ids"]:
        blockers.append("local file benchmark v1 Vortex scenario order mismatch")
    if local_file_report.unsupported_scenario_ids:
        blockers.append("local file benchmark scenarios must not contain unsupported rows")
    if local_file_report.all_no_fallback_no_external_engine is not True:
        blockers.append("local file benchmark routes must preserve no fallback")
    for row in local_file_report.rows:
        if row.fallback_attempted is not False:
            blockers.append(f"{row.scenario_id}: fallback_attempted must be false")
        if row.external_engine_invoked is not False:
            blockers.append(f"{row.scenario_id}: external_engine_invoked must be false")
        if row.claim_gate_status != "not_claim_grade":
            blockers.append(f"{row.scenario_id}: claim_gate_status must remain not_claim_grade")
        if not row.required_evidence:
            blockers.append(f"{row.scenario_id}: missing required_evidence")
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
        provider_route_report,
        user_route_report,
        local_file_report,
        constants,
    ) = load_context_reports(repo_root)
    primitive_rows = [primitive_row_payload(row) for row in primitive_report.rows]
    provider_rows = [provider_route_row_payload(row) for row in provider_route_report.rows]
    local_file_rows = [local_file_benchmark_row_payload(row) for row in local_file_report.rows]
    blockers = []
    blockers.extend(validate_primitive_report(primitive_report, constants))
    blockers.extend(validate_provider_route_report(provider_route_report, constants))
    blockers.extend(validate_user_routes(user_route_report, local_file_report, constants))
    blockers.extend(validate_docs(repo_root))

    passed = not blockers
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if passed else "failed",
        "v1_scope_document": DOC_PATH.as_posix(),
        "supported_starting_states": list(constants["supported_starting_states"]),
        "supported_primitive_route_ids": list(constants["supported_primitive_route_ids"]),
        "supported_benchmark_scenario_ids": list(constants["supported_benchmark_scenario_ids"]),
        "unsupported_boundary_ids": list(constants["unsupported_boundary_ids"]),
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
        "native_vortex_provider_route_schema_version": provider_route_report.schema_version,
        "native_vortex_provider_route_count": len(provider_rows),
        "native_vortex_provider_route_rows": provider_rows,
        "native_vortex_provider_route_all_runtime_supported": (
            provider_route_report.all_runtime_supported
        ),
        "native_vortex_provider_route_all_certificates_current": (
            provider_route_report.all_route_certificates_current
        ),
        "native_vortex_provider_route_all_no_fallback_no_external_engine": (
            provider_route_report.all_no_fallback_no_external_engine
        ),
        "native_vortex_provider_route_v1_scope_ready": (
            provider_route_report.v1_scope_ready
        ),
        "native_vortex_provider_general_multi_input_join_claim_allowed": (
            provider_route_report.general_multi_input_join_claim_allowed
        ),
        "local_file_benchmark_schema_version": local_file_report.schema_version,
        "local_file_benchmark_route_count": len(local_file_rows),
        "local_file_benchmark_rows": local_file_rows,
        "local_file_benchmark_all_no_fallback_no_external_engine": (
            local_file_report.all_no_fallback_no_external_engine
        ),
        "user_route_v1_vortex_scope_ready": user_route_report.v1_vortex_scope_ready,
        "all_no_fallback_no_external_engine": (
            primitive_report.all_no_fallback_no_external_engine
            and provider_route_report.all_no_fallback_no_external_engine
            and user_route_report.all_no_fallback_no_external_engine
            and local_file_report.all_no_fallback_no_external_engine
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
