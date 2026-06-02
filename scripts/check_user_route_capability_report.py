#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Build and validate the user/agent route capability report.

This GAR-RUNTIME-IMPL-6D/GAR-RUNTIME-IMPL-6E gate is the direct answer to
"given input X and desired output Y, which ShardLoom route should I use?" It is
side-effect-free: it reads the Python route metadata and verifies that
benchmark-range route guidance names the Vortex normalization point, execution
mode, prepared-state reuse contract, output/evidence path,
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
PUBLIC_FRONT_DOOR_ROUTE_SCHEMA_VERSION = "shardloom.public_front_door_route_rows.v1"

ROUTE_RUNTIME_STATUSES = {
    "scoped_runtime_supported",
    "output_route_pending",
    "runtime_expansion_pending",
    "claim_evidence_pending",
    "benchmark_publication_pending",
}

LOCAL_FILE_BENCHMARK_ROUTE_RUNTIME_STATUSES = {
    "scoped_runtime_supported",
    "prepared_route_supported",
    "front_door_connection_pending",
    "output_route_pending",
    "claim_evidence_pending",
    "benchmark_publication_pending",
    "runtime_expansion_pending",
}

REQUIRED_LOCAL_FILE_BENCHMARK_SCENARIO_IDS = {
    "selective_filter",
    "filter_projection_limit",
    "group_by_aggregation",
    "multi_key_group_by",
    "join_aggregate",
    "sort_top_k",
    "row_number_window",
    "top_n_per_group",
    "clean_cast_filter_write",
    "partition_pruning",
    "many_small_files_scan",
    "null_heavy_aggregate",
    "high_cardinality_string_group_distinct",
    "nested_json_field_scan",
    "small_change_over_large_base",
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

REQUIRED_PUBLIC_FRONT_DOOR_ROUTE_IDS = {
    "local_source_auto_prepare_vortex_front_door",
    "generated_source_prepare_vortex_front_door",
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

ADMITTED_ROUTE_RUNTIME_STATUSES = {
    "scoped_runtime_supported",
    "prepared_route_supported",
}

OUTPUT_OPTION_PATTERNS = {
    "machine_readable_report": (
        "machine_readable_report",
        "machine-readable",
        "machine readable",
        "report",
    ),
    "bounded_preview": ("bounded_preview", "bounded preview", "bounded scoped collect"),
    "local_compat_output": (
        "local_compat_output",
        "local compatibility output",
        "local jsonl/csv",
        "jsonl/csv",
    ),
    "native_vortex_output": (
        "native_vortex_output",
        "feature_gated_local_vortex_output",
        "vortex sink",
        "vortex result sink",
        "vortex output",
    ),
    "result_sink_replay": (
        "result_sink",
        "result sink",
        "result-sink",
        "replay proof",
        "replay",
    ),
    "fanout": ("fanout",),
}

REQUIRED_ROUTE_DIAGNOSTIC_FIELDS = {
    "source_state_fingerprint",
    "source_schema_fingerprint",
    "source_parse_plan_id",
    "source_split_manifest_id",
    "source_anomaly_count",
    "source_quarantine_required",
    "prepared_state_fingerprint",
    "prepared_state_reuse_scope",
    "prepared_state_reuse_manifest_path",
    "prepared_state_reuse_policy",
    "prepared_state_reuse_hit",
    "prepared_state_reuse_reason",
    "prepared_state_reuse_manifest_digest",
    "prepared_state_invalidation_reason",
    "nearest_runnable_route",
    "required_feature_gate",
    "runtime_blocker_code",
}

PREPARED_STATE_REUSE_MANIFEST_SCOPE = "workspace_manifest_local_vortex_artifacts"
PREPARED_STATE_REUSE_MANIFEST_PATH = (
    "<workspace>/.shardloom/prepared-vortex-reuse-manifest.json"
)
PREPARED_STATE_REUSE_MANIFEST_POLICY = (
    "shardloom.python.prepared_vortex_reuse_manifest.v1"
)
PREPARED_STATE_NOT_APPLICABLE = "not_applicable_no_prepared_state"
GENERATED_PREPARED_STATE_REUSE_MANIFEST_SCOPE = (
    "artifact_adjacent_manifest_local_vortex_artifacts"
)
GENERATED_PREPARED_STATE_REUSE_MANIFEST_PATH = (
    "<target-dir>/.shardloom/<target-name>.prepared-state-reuse.manifest"
)
GENERATED_PREPARED_STATE_REUSE_MANIFEST_POLICY = (
    "artifact_adjacent_local_prepared_state_reuse.v1"
)
GENERATED_PREPARED_STATE_REUSE_REASON = (
    "runtime_evaluated_artifact_adjacent_manifest_lookup"
)
GENERATED_PREPARED_STATE_INVALIDATION_REASON = (
    "runtime_evaluated_on_source_schema_plan_policy_or_artifact_drift"
)
GENERATED_SOURCE_SPLIT_MANIFEST_ID = (
    "not_applicable_generated_source_no_source_splits"
)

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


def load_scenario_catalog(repo_root: Path) -> dict[str, dict[str, Any]]:
    catalog_path = repo_root / "benchmarks" / "common" / "scenario_catalog.json"
    payload = json.loads(catalog_path.read_text(encoding="utf-8"))
    scenarios = payload.get("scenarios")
    if not isinstance(scenarios, list):
        return {}
    return {
        str(row.get("id")): row
        for row in scenarios
        if isinstance(row, dict) and row.get("id")
    }


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
        "source_state_fingerprint": row.source_state_fingerprint,
        "source_schema_fingerprint": row.source_schema_fingerprint,
        "source_parse_plan_id": row.source_parse_plan_id,
        "source_split_manifest_id": row.source_split_manifest_id,
        "source_anomaly_count": row.source_anomaly_count,
        "source_quarantine_required": row.source_quarantine_required,
        "prepared_state_fingerprint": row.prepared_state_fingerprint,
        "prepared_state_reuse_scope": row.prepared_state_reuse_scope,
        "prepared_state_reuse_manifest_path": row.prepared_state_reuse_manifest_path,
        "prepared_state_reuse_policy": row.prepared_state_reuse_policy,
        "prepared_state_reuse_hit": row.prepared_state_reuse_hit,
        "prepared_state_reuse_reason": row.prepared_state_reuse_reason,
        "prepared_state_reuse_manifest_digest": row.prepared_state_reuse_manifest_digest,
        "prepared_state_invalidation_reason": row.prepared_state_invalidation_reason,
        "nearest_runnable_route": row.nearest_runnable_route,
        "required_feature_gate": row.required_feature_gate,
        "runtime_blocker_code": row.runtime_blocker_code,
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


def public_front_door_row_payload(row: Any) -> dict[str, Any]:
    return {
        "front_door_id": row.front_door_id,
        "owning_route_id": row.owning_route_id,
        "route_lane_id": row.route_lane_id,
        "route_display_name": row.route_display_name,
        "input_family": row.input_family,
        "public_user_surface": row.public_user_surface,
        "benchmark_public_surface": row.benchmark_public_surface,
        "front_door_start_state": row.front_door_start_state,
        "front_door_end_state": row.front_door_end_state,
        "route_lane_start_state": row.route_lane_start_state,
        "route_lane_end_state": row.route_lane_end_state,
        "vortex_normalization_point": row.vortex_normalization_point,
        "source_route": row.source_route,
        "preparation_route": row.preparation_route,
        "execution_mode": row.execution_mode,
        "includes_preparation": row.includes_preparation,
        "includes_query": row.includes_query,
        "includes_output": row.includes_output,
        "includes_evidence": row.includes_evidence,
        "preparation_included": row.preparation_included,
        "query_timing_starts_after_preparation": (
            row.query_timing_starts_after_preparation
        ),
        "owning_route_comparable_to_external_end_to_end": (
            row.owning_route_comparable_to_external_end_to_end
        ),
        "prepared_state_reused": row.prepared_state_reused,
        "prepared_state_reuse_scope": row.prepared_state_reuse_scope,
        "prepared_state_reuse_manifest_path": row.prepared_state_reuse_manifest_path,
        "prepared_state_reuse_policy": row.prepared_state_reuse_policy,
        "prepared_state_reuse_hit": row.prepared_state_reuse_hit,
        "prepared_state_reuse_reason": row.prepared_state_reuse_reason,
        "prepared_state_reuse_manifest_digest": row.prepared_state_reuse_manifest_digest,
        "prepared_state_invalidation_reason": row.prepared_state_invalidation_reason,
        "route_runtime_status": row.route_runtime_status,
        "fallback_attempted": row.fallback_attempted,
        "external_engine_invoked": row.external_engine_invoked,
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


def local_file_benchmark_row_payload(row: Any) -> dict[str, Any]:
    return {
        "scenario_id": row.scenario_id,
        "scenario_name": row.scenario_name,
        "scenario_suite": row.scenario_suite,
        "scenario_category": row.scenario_category,
        "dataset_profiles": list(row.dataset_profiles),
        "route_id": row.route_id,
        "route_display_name": row.route_display_name,
        "alternate_route_ids": list(row.alternate_route_ids),
        "front_doors": list(row.front_doors),
        "sql_surface": row.sql_surface,
        "python_surface": row.python_surface,
        "dataframe_surface": row.dataframe_surface,
        "context_surface": row.context_surface,
        "session_surface": row.session_surface,
        "cli_surface": row.cli_surface,
        "start_state": row.start_state,
        "vortex_normalization_point": row.vortex_normalization_point,
        "source_route": row.source_route,
        "preparation_route": row.preparation_route,
        "selected_execution_mode": row.selected_execution_mode,
        "output_route": row.output_route,
        "evidence_route": row.evidence_route,
        "materialization_decode_boundary": row.materialization_decode_boundary,
        "source_state_fingerprint": row.source_state_fingerprint,
        "source_schema_fingerprint": row.source_schema_fingerprint,
        "source_parse_plan_id": row.source_parse_plan_id,
        "source_split_manifest_id": row.source_split_manifest_id,
        "source_anomaly_count": row.source_anomaly_count,
        "source_quarantine_required": row.source_quarantine_required,
        "prepared_state_fingerprint": row.prepared_state_fingerprint,
        "prepared_state_reuse_scope": row.prepared_state_reuse_scope,
        "prepared_state_reuse_manifest_path": row.prepared_state_reuse_manifest_path,
        "prepared_state_reuse_policy": row.prepared_state_reuse_policy,
        "prepared_state_reuse_hit": row.prepared_state_reuse_hit,
        "prepared_state_reuse_reason": row.prepared_state_reuse_reason,
        "prepared_state_reuse_manifest_digest": row.prepared_state_reuse_manifest_digest,
        "prepared_state_invalidation_reason": row.prepared_state_invalidation_reason,
        "nearest_runnable_route": row.nearest_runnable_route,
        "required_feature_gate": row.required_feature_gate,
        "runtime_blocker_code": row.runtime_blocker_code,
        "route_runtime_status": row.route_runtime_status,
        "fallback_attempted": row.fallback_attempted,
        "external_engine_invoked": row.external_engine_invoked,
        "blocker_id": row.blocker_id or "none",
        "owner": row.owner,
        "required_evidence": list(row.required_evidence),
        "next_verifier": row.next_verifier,
        "claim_gate_status": row.claim_gate_status,
        "performance_claim_allowed": row.performance_claim_allowed,
        "production_claim_allowed": row.production_claim_allowed,
        "spark_replacement_claim_allowed": row.spark_replacement_claim_allowed,
        "claim_boundary": row.claim_boundary,
    }


def output_options_for_row(row: dict[str, Any]) -> list[str]:
    """Classify the explicit output options advertised by one route row."""

    values: list[str] = []
    for field in (
        "desired_outputs",
        "output_route",
        "evidence_route",
        "recommended_user_surface",
    ):
        value = row.get(field)
        if isinstance(value, list):
            values.extend(str(item) for item in value)
        else:
            values.append(str(value or ""))
    text = " ".join(values).lower().replace("_", "-")
    options: list[str] = []
    for option, patterns in OUTPUT_OPTION_PATTERNS.items():
        if any(pattern.replace("_", "-") in text for pattern in patterns):
            options.append(option)
    return options


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


def validate_local_file_benchmark_routes(
    report: Any,
    rows: list[dict[str, Any]],
    scenario_catalog: dict[str, dict[str, Any]],
    user_route_ids: set[str],
) -> list[str]:
    blockers: list[str] = []
    by_id = {str(row["scenario_id"]): row for row in rows}

    missing = sorted(REQUIRED_LOCAL_FILE_BENCHMARK_SCENARIO_IDS - by_id.keys())
    if missing:
        blockers.append(
            "local file benchmark route report missing scenarios: " + ",".join(missing)
        )

    extra = sorted(by_id.keys() - REQUIRED_LOCAL_FILE_BENCHMARK_SCENARIO_IDS)
    if extra:
        blockers.append(
            "local file benchmark route report has unclassified extra scenarios: "
            + ",".join(extra)
        )

    duplicate_count = len(rows) - len(by_id)
    if duplicate_count:
        blockers.append(
            f"local file benchmark route report has duplicate scenario ids: {duplicate_count}"
        )

    for scenario_id in sorted(REQUIRED_LOCAL_FILE_BENCHMARK_SCENARIO_IDS):
        if scenario_id not in scenario_catalog:
            blockers.append(f"scenario catalog is missing required scenario {scenario_id}")

    for row in rows:
        scenario_id = str(row["scenario_id"])
        catalog_row = scenario_catalog.get(scenario_id)
        if catalog_row is None:
            blockers.append(f"{scenario_id}: not present in benchmark scenario catalog")
        else:
            if row.get("scenario_name") != catalog_row.get("name"):
                blockers.append(f"{scenario_id}: scenario_name does not match scenario catalog")
            if row.get("scenario_suite") != catalog_row.get("suite"):
                blockers.append(f"{scenario_id}: scenario_suite does not match scenario catalog")
            if row.get("scenario_category") != catalog_row.get("category"):
                blockers.append(f"{scenario_id}: scenario_category does not match scenario catalog")
            catalog_profiles = set(catalog_row.get("dataset_profiles", []))
            row_profiles = set(row.get("dataset_profiles", []))
            if not row_profiles:
                blockers.append(f"{scenario_id}: missing dataset_profiles")
            if not row_profiles.issubset(catalog_profiles):
                blockers.append(
                    f"{scenario_id}: dataset_profiles include values not in scenario catalog"
                )

        status = str(row.get("route_runtime_status", ""))
        if status not in LOCAL_FILE_BENCHMARK_ROUTE_RUNTIME_STATUSES:
            blockers.append(f"{scenario_id}: invalid route_runtime_status={status!r}")
        if status == "unsupported":
            blockers.append(f"{scenario_id}: must not be generically unsupported")

        route_id = str(row.get("route_id", ""))
        if route_id not in user_route_ids:
            blockers.append(f"{scenario_id}: route_id {route_id!r} is not in user route report")
        for alternate in row.get("alternate_route_ids", []):
            if str(alternate) not in user_route_ids:
                blockers.append(
                    f"{scenario_id}: alternate_route_id {alternate!r} is not in user route report"
                )

        for field in (
            "scenario_name",
            "scenario_suite",
            "scenario_category",
            "route_display_name",
            "sql_surface",
            "python_surface",
            "dataframe_surface",
            "context_surface",
            "session_surface",
            "cli_surface",
            "start_state",
            "vortex_normalization_point",
            "source_route",
            "preparation_route",
            "selected_execution_mode",
            "output_route",
            "evidence_route",
            "materialization_decode_boundary",
            "owner",
            "next_verifier",
            "claim_boundary",
        ):
            value = row.get(field)
            if not isinstance(value, str) or not value.strip():
                blockers.append(f"{scenario_id}: missing {field}")

        for field in ("front_doors", "dataset_profiles", "required_evidence"):
            value = row.get(field)
            if not isinstance(value, list) or not value:
                blockers.append(f"{scenario_id}: missing non-empty {field}")

        for field in REQUIRED_ROUTE_DIAGNOSTIC_FIELDS:
            value = row.get(field)
            if value is None or (isinstance(value, str) and not value.strip()):
                blockers.append(f"{scenario_id}: missing route diagnostic field {field}")
        nearest = str(row.get("nearest_runnable_route") or "")
        if nearest and nearest not in user_route_ids:
            blockers.append(
                f"{scenario_id}: nearest_runnable_route {nearest!r} is not in user route report"
            )
        blocker = str(row.get("blocker_id") or "none")
        runtime_blocker = str(row.get("runtime_blocker_code") or "")
        if blocker != "none" and runtime_blocker != blocker:
            blockers.append(f"{scenario_id}: runtime_blocker_code must mirror blocker_id")

        if "SourceState" not in str(row.get("vortex_normalization_point", "")):
            blockers.append(f"{scenario_id}: must name SourceState normalization")
        if status == "prepared_route_supported" and "VortexPreparedState" not in str(
            row.get("vortex_normalization_point", "")
        ):
            blockers.append(
                f"{scenario_id}: prepared routes must name VortexPreparedState"
            )
        if status == "prepared_route_supported":
            if row.get("prepared_state_reuse_scope") != PREPARED_STATE_REUSE_MANIFEST_SCOPE:
                blockers.append(
                    f"{scenario_id}: prepared routes must use workspace manifest reuse scope"
                )
            if (
                row.get("prepared_state_reuse_manifest_path")
                != PREPARED_STATE_REUSE_MANIFEST_PATH
            ):
                blockers.append(
                    f"{scenario_id}: prepared routes must expose workspace manifest path"
                )
            if (
                row.get("prepared_state_reuse_policy")
                != PREPARED_STATE_REUSE_MANIFEST_POLICY
            ):
                blockers.append(
                    f"{scenario_id}: prepared routes must expose reuse manifest policy"
                )
        if row.get("fallback_attempted") is not False:
            blockers.append(f"{scenario_id}: fallback_attempted must be false")
        if row.get("external_engine_invoked") is not False:
            blockers.append(f"{scenario_id}: external_engine_invoked must be false")
        for field in (
            "performance_claim_allowed",
            "production_claim_allowed",
            "spark_replacement_claim_allowed",
        ):
            if row.get(field) is not False:
                blockers.append(f"{scenario_id}: {field} must be false")
        if row.get("claim_gate_status") != "not_claim_grade":
            blockers.append(f"{scenario_id}: claim_gate_status must be not_claim_grade")
        if status in ADMITTED_ROUTE_RUNTIME_STATUSES and not output_options_for_row(row):
            blockers.append(
                f"{scenario_id}: admitted local-file benchmark route must advertise at least "
                "one clear output option"
            )

    status_counts = dict(getattr(report, "route_runtime_status_counts", {}))
    if status_counts != {
        status: list(row["route_runtime_status"] for row in rows).count(status)
        for status in status_counts
    }:
        blockers.append("local file benchmark route status counts are inconsistent")
    if getattr(report, "unsupported_scenario_ids", ()):
        blockers.append(
            "local file benchmark scenarios must not be generically unsupported: "
            + ",".join(report.unsupported_scenario_ids)
        )
    if report.all_no_fallback_no_external_engine is not True:
        blockers.append(
            "local file benchmark route report all_no_fallback_no_external_engine must be true"
        )
    if report.all_mapped_without_generic_unsupported is not True:
        blockers.append(
            "local file benchmark route report all_mapped_without_generic_unsupported must be true"
        )
    if report.claim_gate_status != "not_claim_grade":
        blockers.append("local file benchmark route report claim_gate_status must be not_claim_grade")
    for field in (
        "performance_claim_allowed",
        "production_claim_allowed",
        "spark_replacement_claim_allowed",
    ):
        if getattr(report, field) is not False:
            blockers.append(f"local file benchmark route report {field} must be false")

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
        for field in REQUIRED_ROUTE_DIAGNOSTIC_FIELDS:
            value = row.get(field)
            if value is None or (isinstance(value, str) and not value.strip()):
                blockers.append(f"{route_id}: missing route diagnostic field {field}")
        nearest = str(row.get("nearest_runnable_route") or "")
        if nearest and nearest not in REQUIRED_ROUTE_IDS:
            blockers.append(f"{route_id}: nearest_runnable_route {nearest!r} is unknown")
        blocker = str(row.get("blocker_id") or "none")
        runtime_blocker = str(row.get("runtime_blocker_code") or "")
        if blocker != "none" and runtime_blocker != blocker:
            blockers.append(f"{route_id}: runtime_blocker_code must mirror blocker_id")
        if status == "scoped_runtime_supported" and runtime_blocker != "none":
            blockers.append(f"{route_id}: supported route must have runtime_blocker_code=none")
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
        if (
            row.get("benchmark_range") is True
            and status in ADMITTED_ROUTE_RUNTIME_STATUSES
            and not output_options_for_row(row)
        ):
            blockers.append(
                f"{route_id}: admitted benchmark-range route must advertise at least one "
                "clear output option"
            )

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
        surface = str(row.get("recommended_user_surface", ""))
        if row["route_id"] == "local_vortex_primitive_report" and "write_vortex" in surface:
            blockers.append(
                f"{row['route_id']}: scoped native Vortex primitive route must not advertise write_vortex"
            )
        if row["route_id"] == "native_vortex_query":
            for token in ("native_vortex_route", "execution_mode", "memory_gb", "max_parallelism"):
                if token not in surface:
                    blockers.append(
                        f"{row['route_id']}: native route surface must name {token}"
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
        if row.get("prepared_state_reuse_scope") != PREPARED_STATE_REUSE_MANIFEST_SCOPE:
            blockers.append(
                f"{row['route_id']}: prepared compatibility route must use workspace manifest reuse scope"
            )
        if (
            row.get("prepared_state_reuse_manifest_path")
            != PREPARED_STATE_REUSE_MANIFEST_PATH
        ):
            blockers.append(
                f"{row['route_id']}: prepared compatibility route must expose workspace manifest path"
            )
        if row.get("prepared_state_reuse_policy") != PREPARED_STATE_REUSE_MANIFEST_POLICY:
            blockers.append(
                f"{row['route_id']}: prepared compatibility route must expose reuse manifest policy"
            )

    generated_route = by_id.get("generated_rows_local_output")
    if generated_route is not None:
        normalization = str(generated_route.get("vortex_normalization_point", ""))
        if (
            "GeneratedSourceState" not in normalization
            or "VortexPreparedState" not in normalization
        ):
            blockers.append(
                "generated_rows_local_output: generated-source Vortex output route must name "
                "GeneratedSourceState and VortexPreparedState"
            )
        if (
            generated_route.get("prepared_state_reuse_scope")
            != GENERATED_PREPARED_STATE_REUSE_MANIFEST_SCOPE
        ):
            blockers.append(
                "generated_rows_local_output: generated-source route must use "
                "artifact-adjacent manifest reuse scope"
            )
        if (
            generated_route.get("prepared_state_reuse_manifest_path")
            != GENERATED_PREPARED_STATE_REUSE_MANIFEST_PATH
        ):
            blockers.append(
                "generated_rows_local_output: generated-source route must expose "
                "artifact-adjacent reuse manifest path"
            )
        if (
            generated_route.get("prepared_state_reuse_policy")
            != GENERATED_PREPARED_STATE_REUSE_MANIFEST_POLICY
        ):
            blockers.append(
                "generated_rows_local_output: generated-source route must expose "
                "artifact-adjacent reuse manifest policy"
            )
        if generated_route.get("prepared_state_reuse_hit") != "runtime_evaluated":
            blockers.append(
                "generated_rows_local_output: generated-source route must evaluate reuse at runtime"
            )
        if (
            generated_route.get("prepared_state_reuse_reason")
            != GENERATED_PREPARED_STATE_REUSE_REASON
        ):
            blockers.append(
                "generated_rows_local_output: generated-source route must expose "
                "artifact-adjacent reuse reason"
            )
        if (
            generated_route.get("prepared_state_invalidation_reason")
            != GENERATED_PREPARED_STATE_INVALIDATION_REASON
        ):
            blockers.append(
                "generated_rows_local_output: generated-source route must expose "
                "source/schema/plan/policy/artifact invalidation reason"
            )
        if (
            generated_route.get("source_split_manifest_id")
            != GENERATED_SOURCE_SPLIT_MANIFEST_ID
        ):
            blockers.append(
                "generated_rows_local_output: generated-source route must not claim "
                "source split manifest evidence"
            )
        if (
            generated_route.get("prepared_state_reuse_manifest_digest")
            != "runtime_prepared_state_reuse_manifest_digest_pending"
        ):
            blockers.append(
                "generated_rows_local_output: generated-source route must expose "
                "runtime manifest digest status"
            )
        if (
            generated_route.get("prepared_state_fingerprint")
            != "runtime_prepared_state_fingerprint_pending"
        ):
            blockers.append(
                "generated_rows_local_output: generated-source route must expose "
                "runtime prepared-state fingerprint status"
            )
        required_evidence = generated_route.get("required_evidence")
        if (
            not isinstance(required_evidence, list)
            or "prepared_state_reuse_manifest" not in required_evidence
        ):
            blockers.append(
                "generated_rows_local_output: generated-source route must require "
                "prepared_state_reuse_manifest evidence"
            )
        desired_outputs = generated_route.get("desired_outputs")
        if (
            not isinstance(desired_outputs, list)
            or "feature_gated_local_vortex_output" not in desired_outputs
        ):
            blockers.append(
                "generated_rows_local_output: generated-source route must advertise "
                "feature_gated_local_vortex_output"
            )

    for row in rows:
        if row["route_id"] in {
            "local_file_cold_certified_route",
            "local_file_prepare_once_first_query",
            "local_file_prepare_once_batch",
            "prepared_vortex_warm_query",
            "generated_rows_local_output",
        }:
            continue
        if row.get("prepared_state_fingerprint") == PREPARED_STATE_NOT_APPLICABLE and (
            row.get("prepared_state_reuse_scope") != PREPARED_STATE_NOT_APPLICABLE
        ):
            blockers.append(
                f"{row['route_id']}: non-prepared route must mark prepared-state reuse not applicable"
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


def validate_public_front_door_routes(
    rows: list[dict[str, Any]],
    user_rows: list[dict[str, Any]],
) -> list[str]:
    blockers: list[str] = []
    by_id = {str(row["front_door_id"]): row for row in rows}
    user_by_id = {str(row["route_id"]): row for row in user_rows}

    missing = sorted(REQUIRED_PUBLIC_FRONT_DOOR_ROUTE_IDS - by_id.keys())
    if missing:
        blockers.append(
            "public front-door route report missing rows: " + ",".join(missing)
        )
    extra = sorted(by_id.keys() - REQUIRED_PUBLIC_FRONT_DOOR_ROUTE_IDS)
    if extra:
        blockers.append(
            "public front-door route report has unclassified extra rows: "
            + ",".join(extra)
        )
    duplicate_count = len(rows) - len(by_id)
    if duplicate_count:
        blockers.append(
            f"public front-door route report has duplicate ids: {duplicate_count}"
        )

    for row in rows:
        front_door_id = str(row.get("front_door_id") or "")
        owning_route_id = str(row.get("owning_route_id") or "")
        owning = user_by_id.get(owning_route_id)
        if owning is None:
            blockers.append(
                f"{front_door_id}: owning_route_id {owning_route_id!r} is not in user route report"
            )
        else:
            for field in (
                "route_display_name",
                "input_family",
                "vortex_normalization_point",
                "source_route",
                "preparation_route",
                "execution_mode",
                "route_runtime_status",
                "prepared_state_reuse_scope",
                "prepared_state_reuse_manifest_path",
                "prepared_state_reuse_policy",
                "prepared_state_reuse_hit",
                "prepared_state_reuse_reason",
                "prepared_state_reuse_manifest_digest",
                "prepared_state_invalidation_reason",
                "claim_gate_status",
                "claim_boundary",
            ):
                if row.get(field) != owning.get(field):
                    blockers.append(
                        f"{front_door_id}: {field} must match owning route {owning_route_id}"
                    )

        for field in (
            "front_door_id",
            "owning_route_id",
            "route_lane_id",
            "route_display_name",
            "input_family",
            "public_user_surface",
            "benchmark_public_surface",
            "front_door_start_state",
            "front_door_end_state",
            "route_lane_start_state",
            "route_lane_end_state",
            "vortex_normalization_point",
            "source_route",
            "preparation_route",
            "execution_mode",
            "claim_boundary",
        ):
            value = row.get(field)
            if not isinstance(value, str) or not value.strip():
                blockers.append(f"{front_door_id}: missing {field}")

        if row.get("route_runtime_status") != "scoped_runtime_supported":
            blockers.append(
                f"{front_door_id}: route_runtime_status must be scoped_runtime_supported"
            )
        if row.get("front_door_end_state") != "VortexPreparedState":
            blockers.append(f"{front_door_id}: front_door_end_state must be VortexPreparedState")
        if "VortexPreparedState" not in str(row.get("vortex_normalization_point", "")):
            blockers.append(f"{front_door_id}: must name VortexPreparedState normalization")
        for field in (
            "includes_preparation",
            "includes_output",
            "includes_evidence",
            "preparation_included",
            "owning_route_comparable_to_external_end_to_end",
        ):
            if row.get(field) is not True:
                blockers.append(f"{front_door_id}: {field} must be true")
        if row.get("includes_query") is not False:
            blockers.append(
                f"{front_door_id}: prepare front-door row must set includes_query=false"
            )
        if row.get("fallback_attempted") is not False:
            blockers.append(f"{front_door_id}: fallback_attempted must be false")
        if row.get("external_engine_invoked") is not False:
            blockers.append(f"{front_door_id}: external_engine_invoked must be false")
        for field in (
            "performance_claim_allowed",
            "production_claim_allowed",
            "spark_replacement_claim_allowed",
        ):
            if row.get(field) is not False:
                blockers.append(f"{front_door_id}: {field} must be false")
        if row.get("claim_gate_status") != "not_claim_grade":
            blockers.append(f"{front_door_id}: claim_gate_status must be not_claim_grade")
        required_evidence = row.get("required_evidence")
        if not isinstance(required_evidence, list) or not required_evidence:
            blockers.append(f"{front_door_id}: missing required_evidence")
        elif "prepared_state_reuse_manifest" not in required_evidence:
            blockers.append(
                f"{front_door_id}: required_evidence must include prepared_state_reuse_manifest"
            )

        surface = str(row.get("public_user_surface") or "")
        if front_door_id == "local_source_auto_prepare_vortex_front_door":
            if owning_route_id != "local_file_prepare_once_first_query":
                blockers.append(
                    f"{front_door_id}: must own local_file_prepare_once_first_query"
                )
            if row.get("route_lane_id") != "prepare_once_first_query":
                blockers.append(f"{front_door_id}: route_lane_id must be prepare_once_first_query")
            for token in ("ctx.read_csv", ".prepare_vortex", "workspace="):
                if token not in surface:
                    blockers.append(f"{front_door_id}: public_user_surface must include {token}")
            if "SourceState" not in str(row.get("vortex_normalization_point", "")):
                blockers.append(f"{front_door_id}: must name SourceState normalization")
            if row.get("prepared_state_reuse_scope") != PREPARED_STATE_REUSE_MANIFEST_SCOPE:
                blockers.append(f"{front_door_id}: must use workspace manifest reuse scope")
            if (
                row.get("prepared_state_reuse_manifest_path")
                != PREPARED_STATE_REUSE_MANIFEST_PATH
            ):
                blockers.append(f"{front_door_id}: must expose workspace manifest path")
            if row.get("prepared_state_reuse_policy") != PREPARED_STATE_REUSE_MANIFEST_POLICY:
                blockers.append(f"{front_door_id}: must expose workspace manifest policy")

        if front_door_id == "generated_source_prepare_vortex_front_door":
            if owning_route_id != "generated_rows_local_output":
                blockers.append(f"{front_door_id}: must own generated_rows_local_output")
            if row.get("route_lane_id") != "generated_rows_local_output":
                blockers.append(f"{front_door_id}: route_lane_id must be generated_rows_local_output")
            for token in ("ctx.from_rows", ".prepare_vortex", "workspace="):
                if token not in surface:
                    blockers.append(f"{front_door_id}: public_user_surface must include {token}")
            if "GeneratedSourceState" not in str(row.get("vortex_normalization_point", "")):
                blockers.append(f"{front_door_id}: must name GeneratedSourceState normalization")
            if (
                row.get("prepared_state_reuse_scope")
                != GENERATED_PREPARED_STATE_REUSE_MANIFEST_SCOPE
            ):
                blockers.append(f"{front_door_id}: must use artifact-adjacent manifest reuse scope")
            if (
                row.get("prepared_state_reuse_manifest_path")
                != GENERATED_PREPARED_STATE_REUSE_MANIFEST_PATH
            ):
                blockers.append(f"{front_door_id}: must expose artifact-adjacent manifest path")
            if (
                row.get("prepared_state_reuse_policy")
                != GENERATED_PREPARED_STATE_REUSE_MANIFEST_POLICY
            ):
                blockers.append(f"{front_door_id}: must expose artifact-adjacent manifest policy")

    return blockers


def build_report(repo_root: Path) -> dict[str, Any]:
    route_report = load_report(repo_root)
    from shardloom import ShardLoomContext

    local_vortex_report = ShardLoomContext(client=None).local_vortex_primitive_route_report()
    local_file_benchmark_report = (
        ShardLoomContext(client=None).local_file_benchmark_route_report()
    )
    rows = [row_payload(row) for row in route_report.rows]
    public_front_door_rows = [
        public_front_door_row_payload(row)
        for row in route_report.public_front_door_route_rows
    ]
    local_vortex_primitive_rows = [
        primitive_row_payload(row) for row in local_vortex_report.rows
    ]
    local_file_benchmark_rows = [
        local_file_benchmark_row_payload(row)
        for row in local_file_benchmark_report.rows
    ]
    user_route_ids = {str(row["route_id"]) for row in rows}
    scenario_catalog = load_scenario_catalog(repo_root)
    blockers = validate_rows(route_report, rows)
    blockers.extend(validate_public_front_door_routes(public_front_door_rows, rows))
    blockers.extend(
        validate_local_vortex_primitives(
            local_vortex_report,
            local_vortex_primitive_rows,
        )
    )
    blockers.extend(
        validate_local_file_benchmark_routes(
            local_file_benchmark_report,
            local_file_benchmark_rows,
            scenario_catalog,
            user_route_ids,
        )
    )
    runtime_status_counts = dict(route_report.route_runtime_status_counts)
    local_benchmark_route_ids = [
        row["route_id"] for row in rows if row["benchmark_range"] is True
    ]
    admitted_route_output_options = {
        str(row["route_id"]): output_options_for_row(row)
        for row in rows
        if row.get("benchmark_range") is True
        and row.get("route_runtime_status") in ADMITTED_ROUTE_RUNTIME_STATUSES
    }
    admitted_local_file_output_options = {
        str(row["scenario_id"]): output_options_for_row(row)
        for row in local_file_benchmark_rows
        if row.get("route_runtime_status") in ADMITTED_ROUTE_RUNTIME_STATUSES
    }
    prepared_route_reuse_rows = [
        row
        for row in rows
        if row["route_id"]
        in {
            "local_file_cold_certified_route",
            "local_file_prepare_once_first_query",
            "local_file_prepare_once_batch",
        }
    ]
    prepared_local_file_reuse_rows = [
        row
        for row in local_file_benchmark_rows
        if row.get("route_runtime_status") == "prepared_route_supported"
    ]
    generated_reuse_row = next(
        (row for row in rows if row.get("route_id") == "generated_rows_local_output"),
        None,
    )
    public_front_door_by_id = {
        str(row["front_door_id"]): row for row in public_front_door_rows
    }
    local_auto_front_door = public_front_door_by_id.get(
        "local_source_auto_prepare_vortex_front_door"
    )
    generated_front_door = public_front_door_by_id.get(
        "generated_source_prepare_vortex_front_door"
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "gate_id": GATE_ID,
        "status": "passed" if not blockers else "blocked",
        "covered_phase_items": [
            "GAR-RUNTIME-IMPL-6D",
            "GAR-RUNTIME-IMPL-6D-1",
            "GAR-RUNTIME-IMPL-6D-2",
            "GAR-RUNTIME-IMPL-6D-3",
            "GAR-RUNTIME-IMPL-6E",
            "GAR-RUNTIME-IMPL-6E-1",
            "CG-20",
            "CG-21",
        ],
        "route_runtime_status_vocabulary": sorted(ROUTE_RUNTIME_STATUSES),
        "local_file_benchmark_route_runtime_status_vocabulary": sorted(
            LOCAL_FILE_BENCHMARK_ROUTE_RUNTIME_STATUSES
        ),
        "route_count": len(rows),
        "route_order": list(route_report.route_order),
        "route_runtime_status_counts": runtime_status_counts,
        "local_benchmark_range_route_ids": local_benchmark_route_ids,
        "local_benchmark_range_route_count": len(local_benchmark_route_ids),
        "public_front_door_route_schema_version": PUBLIC_FRONT_DOOR_ROUTE_SCHEMA_VERSION,
        "public_front_door_route_count": len(public_front_door_rows),
        "public_front_door_route_ids": [
            str(row["front_door_id"]) for row in public_front_door_rows
        ],
        "admitted_route_output_options": admitted_route_output_options,
        "admitted_local_file_benchmark_output_options": admitted_local_file_output_options,
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
        "local_file_benchmark_schema_version": (
            local_file_benchmark_report.schema_version
        ),
        "local_file_benchmark_route_count": len(local_file_benchmark_rows),
        "local_file_benchmark_scenario_ids": list(
            local_file_benchmark_report.scenario_ids
        ),
        "local_file_benchmark_route_runtime_status_counts": dict(
            local_file_benchmark_report.route_runtime_status_counts
        ),
        "local_file_benchmark_unsupported_scenario_ids": list(
            local_file_benchmark_report.unsupported_scenario_ids
        ),
        "local_file_benchmark_all_no_fallback_no_external_engine": (
            local_file_benchmark_report.all_no_fallback_no_external_engine
        ),
        "local_file_benchmark_all_mapped_without_generic_unsupported": (
            local_file_benchmark_report.all_mapped_without_generic_unsupported
        ),
        "all_no_fallback_no_external_engine": route_report.all_no_fallback_no_external_engine,
        "flexible_anything_claim_allowed": route_report.flexible_anything_claim_allowed,
        "performance_equivalence_claim_allowed": route_report.performance_equivalence_claim_allowed,
        "production_claim_allowed": route_report.production_claim_allowed,
        "spark_replacement_claim_allowed": route_report.spark_replacement_claim_allowed,
        "claim_gate_status": route_report.claim_gate_status,
        "vortex_normalization_contract": route_report.vortex_normalization_contract,
        "rows": rows,
        "public_front_door_route_rows": public_front_door_rows,
        "local_vortex_primitive_rows": local_vortex_primitive_rows,
        "local_file_benchmark_rows": local_file_benchmark_rows,
        "acceptance_summary": {
            "all_routes_have_vortex_normalization": all(
                bool(str(row["vortex_normalization_point"]).strip()) for row in rows
            ),
            "all_routes_have_output_and_evidence": all(
                bool(str(row["output_route"]).strip()) and bool(str(row["evidence_route"]).strip())
                for row in rows
            ),
            "all_admitted_benchmark_routes_have_clear_output_options": all(
                bool(options) for options in admitted_route_output_options.values()
            ),
            "all_routes_have_materialization_decode_boundary": all(
                bool(str(row["materialization_decode_boundary"]).strip()) for row in rows
            ),
            "all_routes_have_source_prepared_diagnostics": all(
                all(row.get(field) is not None for field in REQUIRED_ROUTE_DIAGNOSTIC_FIELDS)
                for row in rows
            ),
            "all_prepared_routes_expose_workspace_manifest_reuse_contract": all(
                row.get("prepared_state_reuse_scope")
                == PREPARED_STATE_REUSE_MANIFEST_SCOPE
                and row.get("prepared_state_reuse_manifest_path")
                == PREPARED_STATE_REUSE_MANIFEST_PATH
                and row.get("prepared_state_reuse_policy")
                == PREPARED_STATE_REUSE_MANIFEST_POLICY
                for row in prepared_route_reuse_rows
            ),
            "generated_source_route_exposes_artifact_adjacent_manifest_reuse_contract": (
                generated_reuse_row is not None
                and "GeneratedSourceState"
                in str(generated_reuse_row.get("vortex_normalization_point"))
                and "VortexPreparedState"
                in str(generated_reuse_row.get("vortex_normalization_point"))
                and generated_reuse_row.get("prepared_state_reuse_scope")
                == GENERATED_PREPARED_STATE_REUSE_MANIFEST_SCOPE
                and generated_reuse_row.get("prepared_state_reuse_manifest_path")
                == GENERATED_PREPARED_STATE_REUSE_MANIFEST_PATH
                and generated_reuse_row.get("prepared_state_reuse_policy")
                == GENERATED_PREPARED_STATE_REUSE_MANIFEST_POLICY
                and generated_reuse_row.get("prepared_state_reuse_reason")
                == GENERATED_PREPARED_STATE_REUSE_REASON
                and generated_reuse_row.get("prepared_state_invalidation_reason")
                == GENERATED_PREPARED_STATE_INVALIDATION_REASON
                and generated_reuse_row.get("source_split_manifest_id")
                == GENERATED_SOURCE_SPLIT_MANIFEST_ID
                and "prepared_state_reuse_manifest"
                in generated_reuse_row.get("required_evidence", [])
                and "feature_gated_local_vortex_output"
                in generated_reuse_row.get("desired_outputs", [])
            ),
            "public_front_door_routes_expose_auto_and_generated_prepared_surfaces": (
                local_auto_front_door is not None
                and "ctx.read_csv"
                in str(local_auto_front_door.get("public_user_surface"))
                and ".prepare_vortex"
                in str(local_auto_front_door.get("public_user_surface"))
                and "SourceState"
                in str(local_auto_front_door.get("vortex_normalization_point"))
                and "VortexPreparedState"
                in str(local_auto_front_door.get("vortex_normalization_point"))
                and generated_front_door is not None
                and "ctx.from_rows"
                in str(generated_front_door.get("public_user_surface"))
                and ".prepare_vortex"
                in str(generated_front_door.get("public_user_surface"))
                and "GeneratedSourceState"
                in str(generated_front_door.get("vortex_normalization_point"))
                and "VortexPreparedState"
                in str(generated_front_door.get("vortex_normalization_point"))
            ),
            "public_front_door_routes_expose_prepared_state_reuse_contracts": all(
                row.get("prepared_state_reuse_scope")
                in {
                    PREPARED_STATE_REUSE_MANIFEST_SCOPE,
                    GENERATED_PREPARED_STATE_REUSE_MANIFEST_SCOPE,
                }
                and row.get("prepared_state_reuse_manifest_path")
                in {
                    PREPARED_STATE_REUSE_MANIFEST_PATH,
                    GENERATED_PREPARED_STATE_REUSE_MANIFEST_PATH,
                }
                and row.get("prepared_state_reuse_policy")
                in {
                    PREPARED_STATE_REUSE_MANIFEST_POLICY,
                    GENERATED_PREPARED_STATE_REUSE_MANIFEST_POLICY,
                }
                and "prepared_state_reuse_manifest"
                in row.get("required_evidence", [])
                for row in public_front_door_rows
            ),
            "public_front_door_routes_preserve_no_fallback": all(
                row.get("fallback_attempted") is False
                and row.get("external_engine_invoked") is False
                for row in public_front_door_rows
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
            "all_required_local_file_benchmark_scenarios_mapped": (
                set(local_file_benchmark_report.scenario_ids)
                == REQUIRED_LOCAL_FILE_BENCHMARK_SCENARIO_IDS
            ),
            "no_generic_unsupported_local_file_benchmark_scenario": (
                not local_file_benchmark_report.unsupported_scenario_ids
            ),
            "all_local_file_benchmark_routes_have_vortex_normalization": all(
                bool(str(row["vortex_normalization_point"]).strip())
                and "SourceState" in str(row["vortex_normalization_point"])
                for row in local_file_benchmark_rows
            ),
            "all_local_file_benchmark_routes_have_output_and_evidence": all(
                bool(str(row["output_route"]).strip())
                and bool(str(row["evidence_route"]).strip())
                for row in local_file_benchmark_rows
            ),
            "all_admitted_local_file_benchmark_routes_have_clear_output_options": all(
                bool(options) for options in admitted_local_file_output_options.values()
            ),
            "all_local_file_benchmark_routes_have_source_prepared_diagnostics": all(
                all(row.get(field) is not None for field in REQUIRED_ROUTE_DIAGNOSTIC_FIELDS)
                for row in local_file_benchmark_rows
            ),
            "all_prepared_local_file_benchmark_routes_expose_workspace_manifest_reuse_contract": all(
                row.get("prepared_state_reuse_scope")
                == PREPARED_STATE_REUSE_MANIFEST_SCOPE
                and row.get("prepared_state_reuse_manifest_path")
                == PREPARED_STATE_REUSE_MANIFEST_PATH
                and row.get("prepared_state_reuse_policy")
                == PREPARED_STATE_REUSE_MANIFEST_POLICY
                for row in prepared_local_file_reuse_rows
            ),
            "all_local_file_benchmark_routes_preserve_no_fallback": (
                local_file_benchmark_report.all_no_fallback_no_external_engine
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
