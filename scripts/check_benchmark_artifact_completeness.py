#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate ShardLoom website benchmark artifact completeness manifests."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "python" / "src"))

from benchmarks.traditional_analytics.benchmark_registry import (  # noqa: E402
    MANIFEST_SCHEMA_VERSION,
    PROFILES,
    expected_lanes_for_profile,
    lane_required_for_profile,
)
from shardloom import validate_runtime_execution_fields  # noqa: E402


REQUIRED_MANIFEST_FIELDS = {
    "schema_version",
    "generated_at_utc",
    "benchmark_profile",
    "expected_lanes",
    "available_lanes",
    "missing_lanes",
    "lane_versions",
    "lane_availability_reasons",
    "environment",
    "claim_boundary",
    "performance_claim_allowed",
    "route_runtime_status_schema_version",
    "route_runtime_status_vocabulary",
    "benchmark_constitution_schema_version",
    "benchmark_constitution_validator",
    "benchmark_constitution_required_field_order",
    "benchmark_constitution_claim_gate_status",
    "benchmark_constitution_performance_claim_allowed",
    "public_front_door_benchmark_schema_version",
    "public_front_door_benchmark_row_count",
    "public_front_door_benchmark_row_ids",
    "artifact_paths",
}
ROUTE_RUNTIME_STATUS_SCHEMA_VERSION = "shardloom.website.route_runtime_status.v1"
ROUTE_RUNTIME_STATUSES = {
    "scoped_runtime_supported",
    "feature_gated",
    "fixture_smoke_only",
    "unsupported",
    "external_baseline_only",
}
ROUTE_TIMING_LEDGER_SCHEMA_VERSION = "shardloom.route_timing_ledger.v1"
EXCLUSIVE_STAGE_TIMING_SCHEMA_VERSION = (
    "shardloom.traditional_analytics.exclusive_stage_timing.v1"
)
TIMING_NORMALIZATION_SCHEMA_VERSION = (
    "shardloom.traditional_analytics.timing_normalization.v1"
)
SOURCE_ADMISSION_DIGEST_POLICY_SCHEMA_VERSION = (
    "shardloom.traditional_analytics.source_admission_digest_policy.v1"
)
ROUTE_TIMING_STAGE_INCLUSION_SCHEMA_VERSION = (
    "shardloom.route_timing_stage_inclusion.v1"
)
FAST_PATH_ATTRIBUTION_SCHEMA_VERSION = "shardloom.route_fast_path_attribution.v1"
OPERATOR_MODE_INVENTORY_SCHEMA_VERSION = "shardloom.operator_mode_inventory.v1"
OPERATOR_EXECUTION_MODES = {
    "encoded_native",
    "residual_native",
    "materialized_temporary",
    "unsupported",
    "external_baseline_only",
}
PREPARED_ROUTE_AMORTIZATION_COUNTS = {1, 5, 10, 50, 100}
DERIVED_PREPARE_ONCE_FIRST_QUERY_STATUS = "derived_from_prepare_once_batch_route_timing"
COLD_BOTTLENECK_SCHEMA_VERSION = "shardloom.traditional_analytics.cold_bottleneck.v1"
COLD_BOTTLENECK_ROUTE_LANES = {
    "cold_certified_route",
    "prepare_once_first_query",
    "prepare_once_batch",
}
COLD_BOTTLENECK_STAGES = {
    "source_admission",
    "source_read",
    "source_parse_or_decode",
    "source_state_build",
    "vortex_array_build",
    "vortex_write",
    "vortex_digest",
    "vortex_reopen_verify",
    "prepared_query",
    "sink_output",
    "evidence_render",
}
COLD_BOTTLENECK_REQUIRED_FIELDS = {
    "cold_bottleneck_schema_version",
    "cold_bottleneck_status",
    "cold_bottleneck_stage_labels",
    "cold_bottleneck_primary_stage",
    "cold_bottleneck_secondary_stage",
    "cold_route_optimization_hint",
    "cold_route_optimization_hint_scope",
    "cold_route_bottleneck_claim_boundary",
    "source_split_count",
    "source_open_count",
    "source_bytes_read",
    "source_columns_requested",
    "source_projection_applied",
    "source_pressure_profile",
    "vortex_prepared_state_reusable",
    "vortex_prepared_state_fingerprint",
    "vortex_prepared_state_fingerprint_status",
}
ROUTE_DIAGNOSTIC_REQUIRED_FIELDS = {
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
EXCLUSIVE_STAGE_TIMING_REQUIRED_FIELDS = {
    "exclusive_stage_timing_schema_version",
    "exclusive_stage_timing_status",
    "exclusive_stage_timing_scope",
    "exclusive_stage_included_stage_ids",
    "route_timing_exclusive_stage_ids",
    "route_timing_exclusive_stage_sum_ms",
    "route_timing_exclusive_residual_ms",
    "route_timing_exclusive_total_delta_ms",
    "route_timing_exclusive_residual_status",
    "inclusive_compatibility_to_vortex_import_ms",
    "inclusive_compatibility_to_vortex_import_timing_scope",
    "exclusive_stage_timing_claim_boundary",
}
TIMING_NORMALIZATION_REQUIRED_FIELDS = {
    "timing_normalization_schema_version",
    "timing_normalization_status",
    "source_admission_policy_micros",
    "source_admission_digest_policy_schema_version",
    "source_admission_digest_policy_status",
    "source_admission_full_content_digest_requested",
    "source_admission_full_content_digest_micros",
    "source_stat_micros",
    "source_state_open_micros",
    "source_state_metadata_snapshot_micros",
    "source_state_manifest_validation_micros",
    "source_state_row_count_metadata_micros",
    "source_state_family_build_micros",
    "source_state_lazy_family_construction",
    "source_state_family_build_timing_scope",
    "source_state_family_build_count",
    "source_state_family_reuse_hit_count",
    "source_state_family_reuse_hit",
    "source_state_family_recompute_avoided",
    "source_state_digest_micros",
    "prepared_manifest_read_micros",
    "prepared_manifest_match_micros",
    "vortex_open_footer_micros",
    "scan_open_micros",
    "scan_chunk_iter_micros",
    "operator_kernel_micros",
    "operator_finalize_micros",
    "result_sink_plan_micros",
    "result_sink_write_micros",
    "result_sink_replay_micros",
    "human_evidence_render_micros",
    "json_envelope_emit_micros",
    "report_fields_build_micros",
    "cli_process_wall_micros",
}
ROUTE_TIMING_STAGE_INCLUSION_REQUIRED_FIELDS = {
    "route_timing_stage_inclusion_schema_version",
    "route_timing_stage_inclusion_status",
    "route_timing_stage_inclusion_stage_ids",
    "route_timing_stage_inclusion_classes",
    "route_timing_stage_inclusion_stage_owners",
    "route_timing_stage_inclusion_timing_scopes",
    "route_timing_stage_inclusion_skip_reasons",
    "route_timing_stage_inclusion_claim_boundary",
}
CANONICAL_ROUTE_TIMING_STAGE_IDS = {
    "source_admission",
    "source_read",
    "source_parse_or_decode",
    "source_to_vortex_array",
    "vortex_write",
    "vortex_digest",
    "vortex_reopen_verify",
    "prepared_state_lookup_or_create",
    "vortex_scan",
    "operator_compute",
    "result_sink_write",
    "evidence_render",
    "cli_process_wall",
}
PREPARED_STATE_REUSE_WORKSPACE_SCOPE = "workspace_manifest_local_vortex_artifacts"
PREPARED_STATE_REUSE_WORKSPACE_MANIFEST_PATH = (
    "<workspace>/.shardloom/prepared-vortex-reuse-manifest.json"
)
PREPARED_STATE_REUSE_WORKSPACE_POLICY = (
    "shardloom.python.prepared_vortex_reuse_manifest.v1"
)
PUBLIC_FRONT_DOOR_BENCHMARK_SCHEMA_VERSION = (
    "shardloom.public_front_door_benchmark_rows.v1"
)
PUBLIC_FRONT_DOOR_BENCHMARK_ROW_KIND = "public_front_door_route_evidence"
PUBLIC_FRONT_DOOR_BENCHMARK_TIMING_STATUS = (
    "not_timing_row_route_identity_only"
)
REQUIRED_PUBLIC_FRONT_DOOR_BENCHMARK_IDS = {
    "local_source_auto_prepare_vortex_front_door",
    "generated_source_prepare_vortex_front_door",
}
FAST_PATH_REQUIRED_FIELDS = {
    "fast_path_attribution_schema_version",
    "runtime_execution_ms",
    "output_delivery_ms",
    "evidence_capture_ms",
    "evidence_render_ms",
    "certificate_link_ms",
    "runtime_execution_timing_scope",
    "output_delivery_timing_scope",
    "evidence_capture_timing_status",
    "certificate_link_timing_status",
    "runtime_execution_certificate_id",
    "runtime_execution_certificate_status",
    "runtime_execution_certificate_plan_ref",
    "certificate_link_status",
    "evidence_required_for_claim",
    "evidence_render_included_in_route_total",
    "fast_path_claim_boundary",
}
OPERATOR_MODE_REQUIRED_FIELDS = {
    "operator_mode_inventory_schema_version",
    "operator_execution_class",
    "operator_admission_status",
    "operator_encoded_native_claim_allowed",
    "operator_residual_native_used",
    "operator_temporary_materialization_used",
    "operator_blocker_matrix_ref",
    "operator_execution_mode",
    "encoded_native_operators",
    "residual_native_operators",
    "materialized_temporary_operators",
    "operator_blocker_code",
    "operator_hot_path_candidate",
    "operator_hot_path_candidate_status",
    "operator_hot_path_next_step",
    "operator_mode_claim_boundary",
}
SOURCE_READ_SCOUT_REQUIRED_FIELDS = {
    "source_read_scout_schema_version",
    "source_read_scout_status",
    "source_read_scout_timing_split_status",
    "source_read_header_scout_ms",
    "source_read_byte_acquisition_ms",
    "source_read_full_body_ms",
    "source_read_scout_residual_ms",
    "source_read_scout_reuse_status",
    "source_read_decode_status",
    "source_read_projected_field_mask",
    "source_read_filter_field_mask",
    "source_read_decoded_columns",
    "source_read_skipped_columns",
    "source_read_decoded_column_count",
    "source_read_skipped_column_count",
    "source_read_row_materialization_status",
    "source_read_unsupported_shape_diagnostic",
    "source_read_scout_claim_boundary",
}
VORTEX_WRITER_CONTEXT_REQUIRED_FIELDS = {
    "vortex_writer_context_schema_version",
    "vortex_writer_context_status",
    "vortex_writer_context_open_ms",
    "vortex_writer_context_write_count",
    "vortex_writer_context_reuse_hit_count",
    "vortex_writer_context_reuse_status",
    "vortex_segment_write_ms",
    "vortex_workspace_stage_ms",
    "vortex_write_coalescing_status",
    "vortex_write_coalescing_reason",
}
REQUIRED_ROUTE_FIELDS = {
    "route_lane_id",
    "route_display_name",
    "route_runtime_status",
    "start_state",
    "end_state",
    "includes_preparation",
    "includes_query",
    "includes_output",
    "includes_evidence",
    "route_comparable_to_external_end_to_end",
    "preparation_included",
    "query_timing_starts_after_preparation",
    "prepared_state_reused",
    "route_timing_ledger_schema_version",
    "route_timing_ledger_status",
    "route_total_formula",
    "route_timing_scope",
    "stage_parent_id",
    "route_timing_included_stage_ids",
    "route_timing_excluded_stage_ids",
    "route_timing_included_stage_total_ms",
    "route_timing_total_delta_ms",
    *TIMING_NORMALIZATION_REQUIRED_FIELDS,
    *ROUTE_TIMING_STAGE_INCLUSION_REQUIRED_FIELDS,
    *EXCLUSIVE_STAGE_TIMING_REQUIRED_FIELDS,
    "preparation_timing_included_in_total",
    "query_timing_included_in_total",
    "output_timing_included_in_total",
    "evidence_timing_included_in_total",
    "performance_claim_allowed",
    "production_claim_allowed",
    "spark_replacement_claim_allowed",
    *ROUTE_DIAGNOSTIC_REQUIRED_FIELDS,
    *FAST_PATH_REQUIRED_FIELDS,
    *OPERATOR_MODE_REQUIRED_FIELDS,
    *SOURCE_READ_SCOUT_REQUIRED_FIELDS,
    *VORTEX_WRITER_CONTEXT_REQUIRED_FIELDS,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument(
        "--allow-incomplete",
        action="store_true",
        help="Allow missing required lanes if the manifest is explicitly marked incomplete.",
    )
    return parser.parse_args()


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def repo_path(path_text: str, manifest_path: Path) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        return path
    root_candidate = ROOT / path
    if root_candidate.exists():
        return root_candidate
    return manifest_path.parent / path


def chunked_result_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
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
        path = repo_path(path_text, ROOT / "website/assets/benchmarks/latest/manifest.json")
        if not path.exists():
            continue
        chunk_payload = load_json(path)
        chunk_rows = (
            chunk_payload.get("rows")
            if isinstance(chunk_payload, dict)
            else chunk_payload
        )
        if isinstance(chunk_rows, list):
            rows.extend(row for row in chunk_rows if isinstance(row, dict))
    return rows


def result_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if isinstance(payload.get("published_benchmark_row_chunks"), list):
        return chunked_result_rows(payload)
    rows = payload.get("results")
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    rows = payload.get("published_benchmark_rows")
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    rows = payload.get("rows")
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    return []


def lane_evidence_counts(payload: dict[str, Any]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for row in result_rows(payload):
        engine = row.get("engine")
        if engine:
            lane = str(engine)
            counts[lane] += 1
            if lane == "shardloom-vortex":
                counts["native-vortex"] += 1
    for row in payload.get("batch_rows", []):
        if not isinstance(row, dict):
            continue
        requested = str(row.get("requested_execution_mode") or "")
        selected = str(row.get("selected_execution_modes") or "")
        if requested == "prepared_vortex" or "prepared_vortex" in selected:
            counts["shardloom-prepared-vortex"] += 1
        if requested == "native_vortex" or "native_vortex" in selected:
            counts["shardloom-vortex"] += 1
            counts["native-vortex"] += 1
    return counts


def promoted_artifact_metadata(payload: dict[str, Any]) -> dict[str, Any]:
    metadata = payload.get("published_benchmark_artifact")
    return metadata if isinstance(metadata, dict) else payload


def validate_profile_scope(
    payload: dict[str, Any],
    profile: str,
    blockers: list[str],
) -> None:
    metadata = promoted_artifact_metadata(payload)
    profile_def = PROFILES[profile]
    format_order = {
        str(item)
        for item in metadata.get("format_order", [])
        if isinstance(item, str)
    }
    if not format_order:
        format_order = {
            str(row.get("storage_format"))
            for row in result_rows(payload)
            if isinstance(row.get("storage_format"), str) and row.get("storage_format")
        }
    scenario_order: set[str] = set()
    for item in metadata.get("scenario_order", []):
        if not isinstance(item, str):
            continue
        scenario_order.add(item)
        if ": " in item:
            scenario_order.add(item.split(": ", 1)[1])
    if not scenario_order:
        for row in result_rows(payload):
            scenario = row.get("scenario_name")
            if not isinstance(scenario, str) or not scenario:
                continue
            scenario_order.add(scenario)
            if ": " in scenario:
                scenario_order.add(scenario.split(": ", 1)[1])
    missing_formats = sorted(set(profile_def.required_formats) - format_order)
    missing_scenarios = sorted(set(profile_def.required_scenarios) - scenario_order)
    if missing_formats:
        blockers.append(
            f"published artifact is missing profile-required formats: {missing_formats}"
        )
    if missing_scenarios:
        blockers.append(
            f"published artifact is missing profile-required scenarios: {missing_scenarios}"
        )


def recursive_text_contains(value: Any, needle: str) -> bool:
    if isinstance(value, str):
        return needle in value
    if isinstance(value, list):
        return any(recursive_text_contains(item, needle) for item in value)
    if isinstance(value, dict):
        return any(recursive_text_contains(item, needle) for item in value.values())
    return False


def runtime_validation_field_map(row: dict[str, Any]) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    evidence = row.get("shardloom_evidence")
    if isinstance(evidence, dict):
        fields.update(evidence)
    metrics = row.get("metrics")
    if isinstance(metrics, dict):
        fields.update(metrics)
    for key, value in row.items():
        if key in {
            "benchmark_constitution",
            "iteration_wall_time_millis",
            "metrics",
            "output_preview",
            "runtime_execution_validation",
            "shardloom_evidence",
        }:
            continue
        fields[key] = value
    if row.get("selected_execution_mode") == "compatibility_import_certified":
        fields["preparation_included"] = (
            row.get("compatibility_import_included") is True
            or fields.get("preparation_included_in_timing") is True
        )
    return fields


def _numeric_value(value: Any) -> float | None:
    if isinstance(value, bool) or value in (None, ""):
        return None
    try:
        return float(str(value).strip())
    except ValueError:
        return None


def _certified_status(value: Any) -> bool:
    text = str(value or "").lower()
    return "certified" in text or text == "passed"


def _meaningful_operator_blocker(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return text not in {
        "",
        "none",
        "missing",
        "not_reported",
        "not_applicable",
        "external_baseline_only",
    }


def _meaningful_reuse_evidence(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return text not in {
        "",
        "none",
        "missing",
        "not_reported",
        "not_requested",
        "not_available",
        "not_applicable",
        "not_applicable_no_prepared_state",
        "not_applicable_no_reuse_manifest_for_route",
    }


def _boolish_true(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() == "true"


def _csv_set(value: Any) -> set[str]:
    if not isinstance(value, str):
        return set()
    return {part.strip() for part in value.split(",") if part.strip()}


def _packed_stage_keys(value: Any) -> set[str]:
    if not isinstance(value, str):
        return set()
    keys: set[str] = set()
    for token in value.split(";"):
        if ":" not in token:
            continue
        key, _ = token.split(":", 1)
        if key.strip():
            keys.add(key.strip())
    return keys


def validate_rows(payload: dict[str, Any], blockers: list[str]) -> None:
    route_lane_counts: Counter[str] = Counter()
    for index, row in enumerate(result_rows(payload)):
        engine = str(row.get("engine", ""))
        route_lane_id = str(row.get("route_lane_id") or "")
        route_lane_counts[route_lane_id] += 1
        missing_route_fields = sorted(REQUIRED_ROUTE_FIELDS - set(row))
        if missing_route_fields:
            blockers.append(
                f"benchmark row {index} is missing route fields: {missing_route_fields}"
            )
        route_status = str(row.get("route_runtime_status") or "")
        if route_status not in ROUTE_RUNTIME_STATUSES:
            blockers.append(
                f"benchmark row {index} has invalid route_runtime_status={route_status!r}"
            )
        if row.get("route_timing_ledger_schema_version") != ROUTE_TIMING_LEDGER_SCHEMA_VERSION:
            blockers.append(f"benchmark row {index} has invalid route timing ledger schema")
        if row.get("fast_path_attribution_schema_version") != FAST_PATH_ATTRIBUTION_SCHEMA_VERSION:
            blockers.append(f"benchmark row {index} has invalid fast-path attribution schema")
        if (
            row.get("operator_mode_inventory_schema_version")
            != OPERATOR_MODE_INVENTORY_SCHEMA_VERSION
        ):
            blockers.append(f"benchmark row {index} has invalid operator-mode inventory schema")
        operator_mode = str(row.get("operator_execution_mode") or "")
        if operator_mode not in OPERATOR_EXECUTION_MODES:
            blockers.append(
                f"benchmark row {index} has invalid operator_execution_mode={operator_mode!r}"
            )
        if row.get("route_timing_ledger_status") != "valid":
            blockers.append(f"benchmark row {index} route timing ledger is not valid")
        if row.get("timing_normalization_schema_version") != TIMING_NORMALIZATION_SCHEMA_VERSION:
            blockers.append(f"benchmark row {index} has invalid timing normalization schema")
        if (
            row.get("source_admission_digest_policy_schema_version")
            != SOURCE_ADMISSION_DIGEST_POLICY_SCHEMA_VERSION
        ):
            blockers.append(
                f"benchmark row {index} has invalid source admission digest policy schema"
            )
        if not str(row.get("source_admission_digest_policy_status") or "").strip():
            blockers.append(
                f"benchmark row {index} is missing source admission digest policy status"
            )
        if (
            row.get("route_timing_stage_inclusion_schema_version")
            != ROUTE_TIMING_STAGE_INCLUSION_SCHEMA_VERSION
        ):
            blockers.append(f"benchmark row {index} has invalid stage inclusion schema")
        if not str(row.get("route_total_formula") or "").strip():
            blockers.append(f"benchmark row {index} is missing route_total_formula")
        if not str(row.get("route_timing_scope") or "").strip():
            blockers.append(f"benchmark row {index} is missing route_timing_scope")
        included_total = _numeric_value(row.get("route_timing_included_stage_total_ms"))
        total_route = _numeric_value(row.get("total_route_ms"))
        delta = _numeric_value(row.get("route_timing_total_delta_ms"))
        if included_total is None or total_route is None or delta is None:
            blockers.append(f"benchmark row {index} route timing ledger has non-numeric totals")
        elif abs(included_total - total_route) > 0.001 or delta > 0.001:
            blockers.append(
                f"benchmark row {index} route timing ledger does not reproduce total_route_ms"
            )
        missing_exclusive_fields = sorted(EXCLUSIVE_STAGE_TIMING_REQUIRED_FIELDS - set(row))
        if missing_exclusive_fields:
            blockers.append(
                f"benchmark row {index} is missing exclusive stage timing fields: "
                f"{missing_exclusive_fields}"
            )
        else:
            if (
                row.get("exclusive_stage_timing_schema_version")
                != EXCLUSIVE_STAGE_TIMING_SCHEMA_VERSION
            ):
                blockers.append(
                    f"benchmark row {index} has invalid exclusive stage timing schema"
                )
            exclusive_sum = _numeric_value(row.get("route_timing_exclusive_stage_sum_ms"))
            exclusive_delta = _numeric_value(
                row.get("route_timing_exclusive_total_delta_ms")
            )
            exclusive_residual = _numeric_value(
                row.get("route_timing_exclusive_residual_ms")
            )
            if engine.startswith("shardloom"):
                if row.get("exclusive_stage_timing_status") != "complete":
                    blockers.append(
                        f"ShardLoom row {index} exclusive stage timing is not complete"
                    )
                if (
                    exclusive_sum is None
                    or exclusive_delta is None
                    or exclusive_residual is None
                ):
                    blockers.append(
                        f"ShardLoom row {index} exclusive stage timing has non-numeric totals"
                    )
                if row.get("route_timing_exclusive_residual_status") not in {
                    "auditable_residual",
                    "zero_residual",
                }:
                    blockers.append(
                        f"ShardLoom row {index} has invalid exclusive residual status"
                    )
            elif row.get("exclusive_stage_timing_status") != "external_baseline_only":
                blockers.append(
                    f"external row {index} must not report complete ShardLoom exclusive timing"
                )
        for timing_field in (
            "runtime_execution_ms",
            "output_delivery_ms",
            "evidence_capture_ms",
            "evidence_render_ms",
            "certificate_link_ms",
        ):
            value = _numeric_value(row.get(timing_field))
            if value is None or value < 0:
                blockers.append(
                    f"benchmark row {index} has invalid fast-path timing field {timing_field}"
                )
        if row.get("evidence_render_included_in_route_total") != row.get(
            "evidence_timing_included_in_total"
        ):
            blockers.append(
                f"benchmark row {index} evidence render inclusion disagrees with route ledger"
            )
        if engine.startswith("shardloom"):
            if row.get("timing_normalization_status") not in {
                "complete_with_unmeasured_optional_fields",
                "not_executed",
            }:
                blockers.append(
                    f"ShardLoom row {index} has invalid timing_normalization_status"
                )
            if row.get("route_timing_stage_inclusion_status") not in {
                "complete",
                "not_executed",
            }:
                blockers.append(
                    f"ShardLoom row {index} has invalid stage inclusion status"
                )
            stage_ids = _csv_set(row.get("route_timing_stage_inclusion_stage_ids"))
            if stage_ids != CANONICAL_ROUTE_TIMING_STAGE_IDS:
                blockers.append(
                    f"ShardLoom row {index} stage inclusion ids are incomplete"
                )
            for field in (
                "route_timing_stage_inclusion_classes",
                "route_timing_stage_inclusion_stage_owners",
                "route_timing_stage_inclusion_timing_scopes",
                "route_timing_stage_inclusion_skip_reasons",
            ):
                if _packed_stage_keys(row.get(field)) != CANONICAL_ROUTE_TIMING_STAGE_IDS:
                    blockers.append(
                        f"ShardLoom row {index} stage inclusion field {field} "
                        "does not cover every canonical stage"
                    )
            for field in (
                "source_read_projected_field_mask",
                "source_read_filter_field_mask",
            ):
                if not str(row.get(field) or "").startswith("0x"):
                    blockers.append(
                        f"ShardLoom row {index} has invalid source-read mask field {field}"
                    )
            for field in (
                "source_read_decoded_column_count",
                "source_read_skipped_column_count",
            ):
                value = _numeric_value(row.get(field))
                if value is None or value < 0:
                    blockers.append(
                        f"ShardLoom row {index} has invalid source-read count field {field}"
                    )
            for field in (
                "source_read_decode_status",
                "source_read_row_materialization_status",
                "source_read_unsupported_shape_diagnostic",
            ):
                if not str(row.get(field) or "").strip():
                    blockers.append(
                        f"ShardLoom row {index} is missing source-read field {field}"
                    )
            for field in (
                "vortex_writer_context_write_count",
                "vortex_writer_context_reuse_hit_count",
            ):
                value = _numeric_value(row.get(field))
                if value is None or value < 0:
                    blockers.append(
                        f"ShardLoom row {index} has invalid Vortex writer context count field {field}"
                    )
            for field in (
                "vortex_writer_context_status",
                "vortex_writer_context_reuse_status",
                "vortex_write_coalescing_status",
                "vortex_write_coalescing_reason",
            ):
                if not str(row.get(field) or "").strip():
                    blockers.append(
                        f"ShardLoom row {index} is missing Vortex writer context field {field}"
                    )
            source_state_prepare = _numeric_value(row.get("source_state_prepare_micros"))
            source_admission = _numeric_value(row.get("source_admission_ms"))
            direct_source_admission = _numeric_value(
                row.get("source_admission_policy_micros")
            )
            if (
                source_state_prepare is not None
                and direct_source_admission is None
                and source_admission is not None
                and abs(source_admission - source_state_prepare / 1000.0) <= 0.001
            ):
                blockers.append(
                    f"ShardLoom row {index} maps broad source_state_prepare_micros "
                    "to source_admission_ms without a direct admission timing field"
                )
            if _boolish_true(row.get("includes_output")) and row.get(
                "output_timing_included_in_total"
            ) is not True:
                blockers.append(
                    f"benchmark row {index} includes output but excludes output timing"
                )
            if _boolish_true(row.get("includes_evidence")) and row.get(
                "evidence_timing_included_in_total"
            ) is not True:
                blockers.append(
                    f"benchmark row {index} includes evidence but excludes evidence timing"
                )
        elif row.get("route_timing_stage_inclusion_status") != "external_baseline_only":
            blockers.append(
                f"external row {index} must keep stage inclusion external-baseline-only"
            )
        for claim_field in (
            "performance_claim_allowed",
            "production_claim_allowed",
            "spark_replacement_claim_allowed",
        ):
            if row.get(claim_field) is not False:
                blockers.append(f"benchmark row {index} must set {claim_field}=false")
        for diagnostic_field in ROUTE_DIAGNOSTIC_REQUIRED_FIELDS:
            value = row.get(diagnostic_field)
            if value is None or (isinstance(value, str) and not value.strip()):
                blockers.append(
                    f"benchmark row {index} is missing route diagnostic field {diagnostic_field}"
                )
        if engine.startswith("shardloom"):
            reuse_hit = _boolish_true(row.get("prepared_state_reuse_hit"))
            reused = _boolish_true(row.get("prepared_state_reused"))
            reuse_scope = str(row.get("prepared_state_reuse_scope") or "")
            if reuse_hit or reused:
                for reuse_field in (
                    "prepared_state_reuse_scope",
                    "prepared_state_reuse_reason",
                    "prepared_state_reuse_manifest_digest",
                    "prepared_state_invalidation_reason",
                ):
                    if not _meaningful_reuse_evidence(row.get(reuse_field)):
                        blockers.append(
                            f"ShardLoom reuse row {index} is missing {reuse_field}"
                        )
            if reuse_scope == PREPARED_STATE_REUSE_WORKSPACE_SCOPE:
                if (
                    row.get("prepared_state_reuse_manifest_path")
                    != PREPARED_STATE_REUSE_WORKSPACE_MANIFEST_PATH
                ):
                    blockers.append(
                        f"ShardLoom workspace reuse row {index} has invalid manifest path"
                    )
                if (
                    row.get("prepared_state_reuse_policy")
                    != PREPARED_STATE_REUSE_WORKSPACE_POLICY
                ):
                    blockers.append(
                        f"ShardLoom workspace reuse row {index} has invalid reuse policy"
                    )
            if row.get("status") == "success" and row.get("runtime_blocker_code") != "none":
                blockers.append(
                    f"successful ShardLoom row {index} must set runtime_blocker_code=none"
                )
            if row.get("claim_gate_status") == "claim_grade":
                if row.get("evidence_required_for_claim") is not True:
                    blockers.append(
                        f"ShardLoom claim-grade row {index} must require evidence for claim"
                    )
                if row.get("certificate_link_status") != "linked_certified_runtime_execution":
                    blockers.append(
                        f"ShardLoom claim-grade row {index} must link certified runtime execution"
                    )
                if not _certified_status(row.get("runtime_execution_certificate_status")):
                    blockers.append(
                        f"ShardLoom claim-grade row {index} missing certified runtime certificate"
                    )
            if not str(row.get("nearest_runnable_route") or "").strip():
                blockers.append(f"ShardLoom row {index} is missing nearest_runnable_route")
            if route_status == "external_baseline_only":
                blockers.append(
                    f"ShardLoom row {index} must not use external_baseline_only route status"
                )
            if row.get("status") == "success" and route_status == "unsupported":
                blockers.append(
                    f"successful ShardLoom row {index} must not report route_runtime_status=unsupported"
                )
            if operator_mode == "external_baseline_only":
                blockers.append(
                    f"ShardLoom row {index} must not use external_baseline_only operator mode"
                )
            if row.get("status") == "success" and operator_mode == "unsupported":
                blockers.append(
                    f"successful ShardLoom row {index} must not report operator_execution_mode=unsupported"
                )
            encoded_claim = row.get("operator_encoded_native_claim_allowed")
            residual_used = row.get("operator_residual_native_used")
            temporary_used = row.get("operator_temporary_materialization_used")
            blocker_code = row.get("operator_blocker_code")
            if operator_mode == "encoded_native":
                if encoded_claim is not True:
                    blockers.append(
                        f"encoded-native ShardLoom row {index} must set "
                        "operator_encoded_native_claim_allowed=true"
                    )
                if residual_used is True or temporary_used is True:
                    blockers.append(
                        f"encoded-native ShardLoom row {index} must not report residual/materialized operators"
                    )
                if str(blocker_code or "") != "none":
                    blockers.append(
                        f"encoded-native ShardLoom row {index} must set operator_blocker_code=none"
                    )
            elif operator_mode in {"residual_native", "materialized_temporary", "unsupported"}:
                if encoded_claim is not False:
                    blockers.append(
                        f"non-encoded ShardLoom row {index} must set "
                        "operator_encoded_native_claim_allowed=false"
                    )
                if not _meaningful_operator_blocker(blocker_code):
                    blockers.append(
                        f"non-encoded ShardLoom row {index} must publish a deterministic "
                        "operator_blocker_code"
                    )
                if row.get("encoded_native_operators") != "none":
                    blockers.append(
                        f"non-encoded ShardLoom row {index} must set encoded_native_operators=none"
                    )
            if (
                route_lane_id == "prepare_once_first_query"
                and row.get("route_row_derivation_status")
                != DERIVED_PREPARE_ONCE_FIRST_QUERY_STATUS
            ):
                blockers.append(
                    f"ShardLoom prepare-once first-query row {index} must declare "
                    f"route_row_derivation_status={DERIVED_PREPARE_ONCE_FIRST_QUERY_STATUS}"
                )
            missing_cold_fields = sorted(COLD_BOTTLENECK_REQUIRED_FIELDS - set(row))
            if missing_cold_fields:
                blockers.append(
                    f"ShardLoom row {index} is missing cold bottleneck fields: {missing_cold_fields}"
                )
            elif route_lane_id in COLD_BOTTLENECK_ROUTE_LANES:
                if row.get("cold_bottleneck_schema_version") != COLD_BOTTLENECK_SCHEMA_VERSION:
                    blockers.append(
                        f"ShardLoom cold row {index} has invalid cold bottleneck schema"
                    )
                if row.get("cold_bottleneck_status") != "complete":
                    blockers.append(
                        f"ShardLoom cold row {index} has incomplete cold bottleneck status: "
                        f"{row.get('cold_bottleneck_status')}"
                    )
                primary_stage = str(row.get("cold_bottleneck_primary_stage") or "")
                if primary_stage not in COLD_BOTTLENECK_STAGES:
                    blockers.append(
                        f"ShardLoom cold row {index} has invalid primary bottleneck stage: "
                        f"{primary_stage!r}"
                    )
                if not str(row.get("cold_route_optimization_hint") or "").strip():
                    blockers.append(
                        f"ShardLoom cold row {index} is missing cold_route_optimization_hint"
                    )
                for pressure_field in (
                    "source_split_count",
                    "source_open_count",
                    "source_bytes_read",
                    "source_columns_requested",
                ):
                    if _numeric_value(row.get(pressure_field)) is None:
                        blockers.append(
                            f"ShardLoom cold row {index} has non-numeric {pressure_field}"
                        )
                if row.get("source_projection_applied") not in {True, False}:
                    blockers.append(
                        f"ShardLoom cold row {index} must set source_projection_applied boolean"
                    )
            elif not str(row.get("cold_bottleneck_status") or "").startswith(
                "not_applicable"
            ):
                blockers.append(
                    f"ShardLoom non-cold row {index} must not inherit cold bottleneck labels"
                )
            if engine == "shardloom" and row.get("route_display_name") == "shardloom":
                blockers.append(
                    "internal shardloom lane must be publicly labeled as "
                    "ShardLoom Cold Certified Route"
                )
            if "fallback_attempted" not in row:
                blockers.append(f"ShardLoom row {index} is missing fallback_attempted")
            elif row.get("fallback_attempted") is not False:
                blockers.append(
                    f"ShardLoom row {index} must set fallback_attempted=false"
                )
            if "external_engine_invoked" not in row:
                blockers.append(f"ShardLoom row {index} is missing external_engine_invoked")
            elif row.get("external_engine_invoked") is not False:
                blockers.append(
                    f"ShardLoom row {index} must set external_engine_invoked=false"
                )
            validation = validate_runtime_execution_fields(
                runtime_validation_field_map(row),
                command="benchmark-artifact-completeness-row",
                status=str(row.get("status", "unknown")),
                surface_id=f"benchmark_artifact_row_{index}",
                runtime_expected=str(row.get("status", "unknown")) == "success",
                execution_mode=str(row.get("selected_execution_mode") or "") or None,
            )
            if validation.status != "passed":
                blockers.append(
                    f"ShardLoom row {index} runtime envelope blocked: "
                    + "; ".join(validation.blockers)
                )
        elif engine:
            if route_status != "external_baseline_only":
                blockers.append(
                    f"external row {index} ({engine}) must set route_runtime_status=external_baseline_only"
                )
            if operator_mode != "external_baseline_only":
                blockers.append(
                    f"external row {index} ({engine}) must set "
                    "operator_execution_mode=external_baseline_only"
                )
            if row.get("operator_encoded_native_claim_allowed") is not False:
                blockers.append(
                    f"external row {index} ({engine}) must not allow encoded-native operator claims"
                )
            if (
                row.get("external_baseline_only") is not True
                and row.get("row_classification") != "external_baseline_only"
            ):
                blockers.append(
                    f"external row {index} ({engine}) is missing external_baseline_only marker"
                )
    for required_lane in (
        "cold_certified_route",
        "prepare_once_first_query",
        "prepare_once_batch",
        "warm_prepared_query",
        "native_vortex_query",
    ):
        if route_lane_counts[required_lane] == 0:
            blockers.append(
                f"published benchmark artifact missing ShardLoom route lane: {required_lane}"
            )


def validate_prepared_route_amortization(
    payload: dict[str, Any],
    blockers: list[str],
) -> None:
    dashboard = payload.get("comparative_dashboard")
    table = dashboard.get("prepared_route_amortization") if isinstance(dashboard, dict) else None
    if not isinstance(table, dict):
        blockers.append("comparative_dashboard missing prepared_route_amortization table")
        return
    if table.get("schema_version") != "shardloom.website.prepared_route_amortization.v1":
        blockers.append("prepared_route_amortization schema_version mismatch")
    counts = {
        int(row[0])
        for row in table.get("rows", [])
        if isinstance(row, list) and row and _numeric_value(row[0]) is not None
    }
    missing_counts = sorted(PREPARED_ROUTE_AMORTIZATION_COUNTS - counts)
    if missing_counts:
        blockers.append(
            f"prepared_route_amortization missing query-count rows: {missing_counts}"
        )
    for row in table.get("rows", []):
        if not isinstance(row, list) or len(row) < 3:
            blockers.append("prepared_route_amortization contains malformed row")
            continue
        row_count = _numeric_value(row[1])
        if row_count is None or row_count <= 0:
            blockers.append(
                f"prepared_route_amortization query-count {row[0]} has no route rows"
            )


def validate_cold_lane_attribution(
    payload: dict[str, Any],
    blockers: list[str],
) -> None:
    dashboard = payload.get("comparative_dashboard")
    table = dashboard.get("cold_lane_attribution") if isinstance(dashboard, dict) else None
    if not isinstance(table, dict):
        blockers.append("comparative_dashboard missing cold_lane_attribution table")
        return
    if table.get("cold_bottleneck_schema_version") != COLD_BOTTLENECK_SCHEMA_VERSION:
        blockers.append("cold_lane_attribution cold_bottleneck_schema_version mismatch")
    if table.get("status") != "passed":
        blockers.append("cold_lane_attribution table is blocked")
    headers = table.get("headers")
    if not isinstance(headers, list) or "Primary bottleneck" not in headers:
        blockers.append("cold_lane_attribution table must include Primary bottleneck")
    primary_index = headers.index("Primary bottleneck") if isinstance(headers, list) and "Primary bottleneck" in headers else -1
    cold_primary_counts: Counter[str] = Counter()
    for row in table.get("rows", []):
        if not isinstance(row, list) or len(row) <= primary_index:
            blockers.append("cold_lane_attribution contains malformed row")
            continue
        if primary_index >= 0:
            primary = str(row[primary_index])
            if primary in COLD_BOTTLENECK_STAGES:
                cold_primary_counts[primary] += int(_numeric_value(row[4]) or 0)
    if not cold_primary_counts:
        blockers.append("cold_lane_attribution has no cold rows with primary bottleneck stages")


def validate_source_state_lazy_family_table(
    payload: dict[str, Any],
    blockers: list[str],
) -> None:
    dashboard = payload.get("comparative_dashboard")
    table = dashboard.get("source_state_lazy_family") if isinstance(dashboard, dict) else None
    if not isinstance(table, dict):
        blockers.append("comparative_dashboard missing source_state_lazy_family table")
        return
    if table.get("schema_version") != "shardloom.website.source_state_lazy_family.v1":
        blockers.append("source_state_lazy_family schema_version mismatch")
    headers = table.get("headers")
    if not isinstance(headers, list) or "Family builds" not in headers:
        blockers.append("source_state_lazy_family table must include Family builds")
    if not isinstance(table.get("rows"), list):
        blockers.append("source_state_lazy_family rows must be a list")


def validate_public_front_door_rows(
    payload: dict[str, Any],
    manifest: dict[str, Any],
    blockers: list[str],
) -> None:
    if payload.get("public_front_door_benchmark_schema_version") != (
        PUBLIC_FRONT_DOOR_BENCHMARK_SCHEMA_VERSION
    ):
        blockers.append("public front-door benchmark schema mismatch")
    if manifest.get("public_front_door_benchmark_schema_version") != (
        PUBLIC_FRONT_DOOR_BENCHMARK_SCHEMA_VERSION
    ):
        blockers.append("manifest public front-door benchmark schema mismatch")
    rows = payload.get("public_front_door_benchmark_rows")
    if not isinstance(rows, list):
        blockers.append("benchmark payload missing public_front_door_benchmark_rows")
        rows = []
    row_ids = {
        str(row.get("front_door_id"))
        for row in rows
        if isinstance(row, dict) and row.get("front_door_id")
    }
    missing = sorted(REQUIRED_PUBLIC_FRONT_DOOR_BENCHMARK_IDS - row_ids)
    extra = sorted(row_ids - REQUIRED_PUBLIC_FRONT_DOOR_BENCHMARK_IDS)
    if missing:
        blockers.append(
            "benchmark payload missing public front-door rows: " + ",".join(missing)
        )
    if extra:
        blockers.append(
            "benchmark payload has unclassified public front-door rows: " + ",".join(extra)
        )
    if payload.get("public_front_door_benchmark_row_count") != len(rows):
        blockers.append("public front-door benchmark row count mismatch")
    if manifest.get("public_front_door_benchmark_row_count") != len(rows):
        blockers.append("manifest public front-door benchmark row count mismatch")
    payload_ids = {
        str(item)
        for item in payload.get("public_front_door_benchmark_row_ids", [])
        if isinstance(item, str)
    }
    if payload_ids != row_ids:
        blockers.append("payload public front-door benchmark row ids mismatch")
    manifest_ids = {
        str(item)
        for item in manifest.get("public_front_door_benchmark_row_ids", [])
        if isinstance(item, str)
    }
    if manifest_ids != row_ids:
        blockers.append("manifest public front-door benchmark row ids mismatch")
    dashboard = payload.get("comparative_dashboard")
    public_table = (
        dashboard.get("public_front_door_routes")
        if isinstance(dashboard, dict)
        else None
    )
    if not isinstance(public_table, dict):
        blockers.append("comparative dashboard missing public_front_door_routes table")
    elif public_table.get("schema_version") != PUBLIC_FRONT_DOOR_BENCHMARK_SCHEMA_VERSION:
        blockers.append("public front-door route table schema mismatch")
    for row in rows:
        if not isinstance(row, dict):
            blockers.append("public front-door benchmark row is not an object")
            continue
        front_door_id = str(row.get("front_door_id") or "missing")
        if row.get("benchmark_row_kind") != PUBLIC_FRONT_DOOR_BENCHMARK_ROW_KIND:
            blockers.append(f"{front_door_id}: invalid public front-door row kind")
        if row.get("benchmark_timing_status") != PUBLIC_FRONT_DOOR_BENCHMARK_TIMING_STATUS:
            blockers.append(f"{front_door_id}: invalid public front-door timing status")
        if row.get("benchmark_timing_row") is not False:
            blockers.append(f"{front_door_id}: public front-door row must not be timing")
        if row.get("benchmark_route_publication_status") != "published_static_route_identity":
            blockers.append(f"{front_door_id}: missing public front-door publication status")
        if row.get("benchmark_route_publication_source") != "user_route_capability_report":
            blockers.append(f"{front_door_id}: missing public front-door publication source")
        if row.get("route_runtime_status") != "scoped_runtime_supported":
            blockers.append(f"{front_door_id}: route_runtime_status must be scoped_runtime_supported")
        if front_door_id == "local_source_auto_prepare_vortex_front_door":
            if row.get("front_door_end_state") != "result_sink":
                blockers.append(f"{front_door_id}: front door must end at result_sink")
            if row.get("includes_query") is not True:
                blockers.append(f"{front_door_id}: first-query row must include query")
            for token in (".query", ".collect"):
                if token not in str(row.get("public_user_surface") or ""):
                    blockers.append(f"{front_door_id}: public surface must show {token}")
        elif row.get("front_door_end_state") != "VortexPreparedState":
            blockers.append(f"{front_door_id}: front door must end at VortexPreparedState")
        elif row.get("includes_query") is not False:
            blockers.append(f"{front_door_id}: prepared-output row must not include query")
        if row.get("fallback_attempted") is not False:
            blockers.append(f"{front_door_id}: fallback_attempted must be false")
        if row.get("external_engine_invoked") is not False:
            blockers.append(f"{front_door_id}: external_engine_invoked must be false")
        if row.get("claim_gate_status") != "not_claim_grade":
            blockers.append(f"{front_door_id}: claim_gate_status must be not_claim_grade")
        public_surface = str(row.get("public_user_surface") or "")
        if ".prepare_vortex" not in public_surface or "workspace=" not in public_surface:
            blockers.append(f"{front_door_id}: public surface must show prepare_vortex workspace")


def validate_manifest(manifest_path: Path, allow_incomplete: bool) -> tuple[list[str], dict[str, Any]]:
    blockers: list[str] = []
    manifest = load_json(manifest_path)
    missing_fields = REQUIRED_MANIFEST_FIELDS - set(manifest)
    if missing_fields:
        blockers.append(f"manifest missing fields: {sorted(missing_fields)}")

    if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        blockers.append(
            f"manifest schema_version must be {MANIFEST_SCHEMA_VERSION}, got {manifest.get('schema_version')}"
        )
    if manifest.get("performance_claim_allowed") is not False:
        blockers.append("performance_claim_allowed must be false")
    if manifest.get("route_runtime_status_schema_version") != ROUTE_RUNTIME_STATUS_SCHEMA_VERSION:
        blockers.append("route_runtime_status_schema_version mismatch")
    route_vocab = set(manifest.get("route_runtime_status_vocabulary") or [])
    if not ROUTE_RUNTIME_STATUSES.issubset(route_vocab):
        blockers.append(
            "route_runtime_status_vocabulary missing values: "
            f"{sorted(ROUTE_RUNTIME_STATUSES - route_vocab)}"
        )
    if (
        manifest.get("benchmark_constitution_schema_version")
        != "shardloom.benchmark_constitution_validation.v1"
    ):
        blockers.append("benchmark_constitution_schema_version mismatch")
    if manifest.get("benchmark_constitution_performance_claim_allowed") is not False:
        blockers.append("benchmark_constitution_performance_claim_allowed must be false")
    profile = manifest.get("benchmark_profile")
    if profile not in PROFILES:
        blockers.append(f"unknown benchmark_profile: {profile}")
        return blockers, manifest

    expected = set(manifest.get("expected_lanes") or [])
    available = set(manifest.get("available_lanes") or [])
    missing = set(manifest.get("missing_lanes") or [])
    required_expected = set(expected_lanes_for_profile(profile))
    if not required_expected.issubset(expected):
        blockers.append(
            f"expected_lanes missing profile lanes: {sorted(required_expected - expected)}"
        )
    unresolved = expected - available - missing
    if unresolved:
        blockers.append(f"expected lanes with no availability status: {sorted(unresolved)}")
    overlap = available & missing
    if overlap:
        blockers.append(f"lanes marked both available and missing: {sorted(overlap)}")

    reasons = manifest.get("lane_availability_reasons") or {}
    for lane in missing:
        if not reasons.get(lane):
            blockers.append(f"missing lane lacks availability reason: {lane}")
    versions = manifest.get("lane_versions") or {}
    for lane in available:
        if not versions.get(lane):
            blockers.append(f"available lane lacks version metadata: {lane}")

    missing_required = [
        lane for lane in missing if lane_required_for_profile(profile, lane)
    ]
    artifact_status = str(manifest.get("artifact_status", "complete"))
    if missing_required and not (allow_incomplete and artifact_status == "incomplete"):
        blockers.append(
            "required lanes missing for profile "
            f"{profile}: {sorted(missing_required)}"
        )

    artifact_paths = manifest.get("artifact_paths") or {}
    json_path_text = artifact_paths.get("json")
    if not json_path_text:
        blockers.append("artifact_paths.json is required")
    else:
        json_path = repo_path(str(json_path_text), manifest_path)
        if not json_path.exists():
            blockers.append(f"artifact_paths.json does not exist: {json_path_text}")
        else:
            payload = load_json(json_path)
            if isinstance(payload, dict):
                validate_rows(payload, blockers)
                validate_prepared_route_amortization(payload, blockers)
                validate_source_state_lazy_family_table(payload, blockers)
                validate_cold_lane_attribution(payload, blockers)
                validate_public_front_door_rows(payload, manifest, blockers)
                validate_profile_scope(payload, profile, blockers)
                if recursive_text_contains(payload, "spark-retire"):
                    blockers.append(
                        "published benchmark artifact must not reference spark-retire"
                    )
                lane_counts = lane_evidence_counts(payload)
                for lane in sorted(expected & available):
                    if lane_counts[lane] == 0:
                        blockers.append(
                            f"available expected lane has no published row evidence: {lane}"
                        )
                if profile in {"full_local", "full_local_plus_spark"}:
                    if "polars" in expected or "polars" in available:
                        blockers.append(
                            "full benchmark profiles must use polars-eager and "
                            "polars-lazy, not collapsed polars"
                        )
                    for lane in ("polars-eager", "polars-lazy"):
                        if lane not in expected:
                            blockers.append(f"full benchmark profile missing {lane}")
            else:
                blockers.append("artifact_paths.json must contain an object")

    return blockers, manifest


def main() -> int:
    args = parse_args()
    blockers, manifest = validate_manifest(args.manifest, args.allow_incomplete)
    report = {
        "manifest": str(args.manifest),
        "benchmark_profile": manifest.get("benchmark_profile"),
        "artifact_status": manifest.get("artifact_status"),
        "blockers": blockers,
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 1 if blockers else 0


if __name__ == "__main__":
    raise SystemExit(main())
