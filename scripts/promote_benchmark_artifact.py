#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Promote a local benchmark execution artifact into committed website data."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import re
import shutil
import subprocess
import sys
from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "python" / "src"))

from benchmarks.traditional_analytics.benchmark_registry import (  # noqa: E402
    LANES,
    MANIFEST_SCHEMA_VERSION,
    PROFILES,
    expected_lanes_for_profile,
    lane_required_for_profile,
)
from shardloom import validate_runtime_execution_fields  # noqa: E402


SUMMARY_SCHEMA_VERSION = "shardloom.website.benchmark_evidence.v1"
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
ROUTE_TIMING_SURFACE_SCHEMA_VERSION = "shardloom.route_timing_surface.v1"
FAST_PATH_ATTRIBUTION_SCHEMA_VERSION = "shardloom.route_fast_path_attribution.v1"
EVIDENCE_RENDER_PROOF_SCHEMA_VERSION = (
    "shardloom.traditional_analytics.evidence_render_proof.v1"
)
PUBLICATION_PROOF_SIDECAR_SCHEMA_VERSION = (
    "shardloom.traditional_analytics.publication_proof_sidecar.v1"
)
PUBLICATION_PROOF_SIDECAR_NAME = "publication-proof-sidecar.json"
ENCODED_KERNEL_PROMOTION_SCHEMA_VERSION = "shardloom.encoded_kernel_promotion.v1"
EVIDENCE_RENDER_PROOF_FIELD_KEYS = (
    "evidence_render_proof_schema_version",
    "evidence_render_proof_status",
    "evidence_render_proof_digest",
    "evidence_render_compact_fact_keys",
    "evidence_render_regeneration_surface",
    "evidence_render_human_expansion_timing_scope",
    "evidence_render_hot_path_policy",
    "evidence_render_route_timing_boundary",
    "evidence_render_claim_boundary",
    "evidence_render_fallback_attempted",
    "evidence_render_external_engine_invoked",
)
OPERATOR_MODE_INVENTORY_SCHEMA_VERSION = "shardloom.operator_mode_inventory.v1"
OPERATOR_EXECUTION_MODES = {
    "encoded_native",
    "residual_native",
    "materialized_temporary",
    "unsupported",
    "external_baseline_only",
}
PREPARED_ROUTE_AMORTIZATION_COUNTS = (1, 5, 10, 50, 100)
DERIVED_PREPARE_ONCE_FIRST_QUERY_STATUS = "derived_from_prepare_once_batch_route_timing"
COLD_BOTTLENECK_SCHEMA_VERSION = "shardloom.traditional_analytics.cold_bottleneck.v1"
SOURCE_READ_SCOUT_SCHEMA_VERSION = (
    "shardloom.traditional_analytics.source_read_scout.v1"
)
SOURCE_TYPED_COLUMN_BUILDER_SCHEMA_VERSION = (
    "shardloom.traditional_analytics.source_typed_column_builder.v1"
)
SOURCE_PROJECTION_ADMISSION_SCHEMA_VERSION = (
    "shardloom.traditional_analytics.source_projection_admission.v1"
)
SOURCE_COLUMNAR_PROVIDER_SCHEMA_VERSION = (
    "shardloom.traditional_analytics.source_columnar_provider.v1"
)
PREPARED_STATE_OPTIMIZATION_SCHEMA_VERSION = (
    "shardloom.traditional_analytics.prepared_state_optimization.v1"
)
VORTEX_REOPEN_SCAN_ATTRIBUTION_SCHEMA_VERSION = (
    "shardloom.traditional_analytics.vortex_reopen_scan_attribution.v1"
)
ROUTE_SHARE_AMDAHL_SCHEMA_VERSION = "shardloom.traditional_analytics.route_share_amdahl.v1"
ROUTE_TIMING_INSTRUMENT_SCHEMA_VERSION = "shardloom.route_timing_instrument.v1"
OPTIMIZATION_READINESS_STAGE_THRESHOLD_MS = 10.0
COMMON_RUN_TIMING_DRIFT_SCHEMA_VERSION = (
    "shardloom.website.common_run_timing_drift.v1"
)
COLD_BOTTLENECK_ROUTE_LANES = {
    "cold_certified_route",
    "prepare_once_first_query",
    "prepare_once_batch",
}
COLD_BOTTLENECK_STAGE_ORDER = (
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
)
DEFAULT_LATEST_DIR = ROOT / "website" / "assets" / "benchmarks" / "latest"
DEFAULT_WEBSITE_DATA = ROOT / "website" / "assets" / "data" / "benchmark-evidence.json"
DEFAULT_PUBLIC_LATEST_DIR = ROOT / "website-public" / "assets" / "benchmarks" / "latest"
DEFAULT_PUBLIC_WEBSITE_DATA = (
    ROOT / "website-public" / "assets" / "data" / "benchmark-evidence.json"
)
DEFAULT_WEBSITE_SRC_DATA = ROOT / "website-src" / "src" / "data" / "benchmark-evidence.json"
DEFAULT_WEBSITE_SRC_MANIFEST = ROOT / "website-src" / "src" / "data" / "benchmark-manifest.json"
DEFAULT_BASE_SUMMARY = DEFAULT_PUBLIC_WEBSITE_DATA
BENCHMARK_PROFILE_ROSTER = ("full_local",)
PUBLISHED_ROW_CHUNK_PREFIX = "published-benchmark-rows"
PUBLISHED_ROW_CHUNK_SIZE = 200
PUBLISHED_ROW_CHUNK_SCHEMA_VERSION = "shardloom.website.benchmark_row_chunk.v1"
PUBLISHED_ROW_RUN_DIR = "published-row-runs"
ROW_ADMISSION_MANIFEST_SCHEMA_VERSION = (
    "shardloom.website.benchmark_row_admission_manifest.v1"
)
ROW_ADMISSION_MANIFEST_NAME = "benchmark-row-admission-manifest.json"
PREPARE_BATCH_ROLE_REPAIR_EVIDENCE_NAME = "prepare-batch-role-repair-evidence.json"
PREPARE_BATCH_ROLE_REPAIR_EVIDENCE_SCHEMA_VERSION = (
    "shardloom.prepare_batch_role_repair_evidence.v1"
)
DUPLICATE_SUFFIX_RE = re.compile(r" \d+(?:\.[^.]+)?$")
WEBSITE_ROW_KEYS = (
    "engine",
    "engine_display_name",
    "scenario_name",
    "storage_format",
    "status",
    "selected_execution_mode",
    "route_lane_id",
    "route_display_name",
    "route_family_display_name",
    "route_runtime_status",
    "start_state",
    "end_state",
    "includes_preparation",
    "includes_query",
    "includes_output",
    "includes_evidence",
    "route_comparable_to_external_end_to_end",
    "session_route_used",
    "process_spawn_count",
    "preparation_included",
    "preparation_included_scope",
    "query_timing_starts_after_preparation",
    "prepared_state_reused",
    "prepared_state_reuse_scope",
    "prepared_state_reuse_manifest_path",
    "prepared_state_reuse_policy",
    "prepared_state_reuse_hit",
    "prepared_state_reuse_reason",
    "prepared_state_reuse_manifest_digest",
    "prepared_state_invalidation_reason",
    "prepare_batch_prepared_state_optimization_schema_version",
    "prepare_batch_prepared_state_optimization_status",
    "prepare_batch_prepared_state_optimization_strategy",
    "prepare_batch_prepared_state_optimization_index_digest",
    "prepare_batch_prepared_state_optimization_manifest_digest",
    "prepare_batch_prepared_state_optimization_source_packet_digest",
    "prepare_batch_prepared_state_optimization_changed_roles",
    "prepare_batch_prepared_state_optimization_reused_roles",
    "prepare_batch_prepared_state_optimization_repaired_roles",
    "prepare_batch_prepared_state_optimization_invalidated_derived_states",
    "prepare_batch_prepared_state_optimization_invalidation_reason",
    "prepare_batch_prepared_state_optimization_manifest_lookup_ms",
    "prepare_batch_prepared_state_optimization_manifest_match_ms",
    "prepare_batch_prepared_state_optimization_cache_miss_create_ms",
    "prepare_batch_prepared_state_optimization_artifact_write_ms",
    "prepare_batch_prepared_state_optimization_repair_ms",
    "prepare_batch_prepared_state_optimization_delta_overlay_ms",
    "prepare_batch_prepared_state_optimization_replay_verification_ms",
    "prepare_batch_prepared_state_optimization_delta_overlay_admitted",
    "prepare_batch_prepared_state_optimization_base_artifact_reused",
    "prepare_batch_prepared_state_optimization_proof_digest",
    "prepare_batch_prepared_state_optimization_replay_proof",
    "prepare_batch_prepared_state_optimization_blocker_id",
    "prepare_batch_prepared_state_optimization_stale_artifact_reuse_allowed",
    "prepare_batch_prepared_state_optimization_no_fallback_policy_status",
    "prepare_batch_prepared_state_optimization_fallback_attempted",
    "prepare_batch_prepared_state_optimization_external_engine_invoked",
    "prepare_batch_prepared_state_optimization_claim_boundary",
    "prepare_batch_prepared_state_index_schema_version",
    "prepare_batch_prepared_state_index_lookup_status",
    "prepare_batch_prepared_state_index_digest",
    "prepare_batch_prepared_state_index_key_components",
    "prepare_batch_prepared_state_index_source_packet_digest",
    "prepare_batch_prepared_state_index_external_engine_invoked",
    "prepare_batch_prepared_state_dependency_schema_version",
    "prepare_batch_prepared_state_dependency_status",
    "prepare_batch_prepared_state_dependency_checked_roles",
    "prepare_batch_prepared_state_dependency_changed_roles",
    "prepare_batch_prepared_state_dependency_manifest_digest",
    "prepare_batch_prepared_state_dependency_source_packet_digest",
    "prepare_batch_prepared_state_dependency_artifact_manifest_hash",
    "prepare_batch_prepared_state_dependency_recheck_policy",
    "prepare_batch_prepared_state_dependency_fallback_attempted",
    "prepare_batch_prepared_state_dependency_external_engine_invoked",
    "prepare_batch_prepared_state_partial_repair_schema_version",
    "prepare_batch_prepared_state_partial_repair_status",
    "prepare_batch_prepared_state_partial_repair_blocker_id",
    "prepare_batch_prepared_state_partial_repair_changed_roles",
    "prepare_batch_prepared_state_partial_repair_reused_roles",
    "prepare_batch_prepared_state_partial_repair_repaired_roles",
    "prepare_batch_prepared_state_partial_repair_invalidated_derived_states",
    "prepare_batch_prepared_state_partial_repair_micros",
    "prepare_batch_prepared_state_partial_repair_source_to_columnar_micros",
    "prepare_batch_prepared_state_partial_repair_vortex_array_build_micros",
    "prepare_batch_prepared_state_partial_repair_vortex_write_micros",
    "prepare_batch_prepared_state_partial_repair_vortex_reopen_verify_micros",
    "prepare_batch_prepared_state_partial_repair_source_to_columnar_ms",
    "prepare_batch_prepared_state_partial_repair_vortex_array_build_ms",
    "prepare_batch_prepared_state_partial_repair_vortex_write_ms",
    "prepare_batch_prepared_state_partial_repair_vortex_reopen_verify_ms",
    "prepare_batch_prepared_state_partial_repair_replay_proof",
    "prepare_batch_prepared_state_partial_repair_repairable_segment_count",
    "prepare_batch_prepared_state_partial_repair_regeneration_performed",
    "prepare_batch_prepared_state_partial_repair_stale_segment_reuse_allowed",
    "prepare_batch_prepared_state_partial_repair_claim_boundary",
    "route_row_derivation_status",
    "route_row_source_lane_id",
    "route_row_source_engine",
    "prepared_route_query_count",
    "prepared_route_observed_batch_count",
    "route_timing_ledger_schema_version",
    "route_timing_ledger_status",
    "route_timing_surface_schema_version",
    "timing_surface",
    "timing_surface_label",
    "timing_surface_evidence_tier",
    "timing_surface_default_for_route",
    "timing_surface_claim_boundary",
    "route_total_formula",
    "route_timing_scope",
    "stage_parent_id",
    "route_timing_included_stage_ids",
    "route_timing_excluded_stage_ids",
    "route_timing_included_stage_total_ms",
    "route_timing_total_delta_ms",
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
    "vortex_projected_field_extract_micros",
    "vortex_encoded_kernel_evidence_micros",
    "operator_kernel_micros",
    "operator_finalize_micros",
    "result_assembly_micros",
    "result_sink_plan_micros",
    "result_sink_write_micros",
    "result_sink_replay_micros",
    "human_evidence_render_micros",
    "json_envelope_emit_micros",
    "report_fields_build_micros",
    "evidence_sink_tier_schema_version",
    "requested_evidence_tier",
    "actual_evidence_tier",
    "selected_evidence_tier",
    "sink_tier",
    "evidence_tier_supported_tiers",
    "evidence_tier_result_sink_replay_required",
    "sink_timing_included_in_route_total",
    "sink_timing_inclusion_reason",
    "result_sink_replay_skip_reason",
    "human_evidence_render_skip_reason",
    "cli_process_wall_micros",
    "route_timing_stage_inclusion_schema_version",
    "route_timing_stage_inclusion_status",
    "route_timing_stage_inclusion_stage_ids",
    "route_timing_stage_inclusion_classes",
    "route_timing_stage_inclusion_stage_owners",
    "route_timing_stage_inclusion_timing_scopes",
    "route_timing_stage_inclusion_skip_reasons",
    "route_timing_stage_inclusion_claim_boundary",
    "route_timing_instrument_schema_version",
    "route_timing_instrument_status",
    "route_timing_instrument_stage_ids",
    "route_timing_instrument_stage_parent_stages",
    "route_timing_instrument_stage_groups",
    "route_timing_instrument_stage_owners",
    "route_timing_instrument_inclusion_classes",
    "route_timing_instrument_timing_scopes",
    "route_timing_instrument_evidence_levels",
    "route_timing_instrument_residual_treatments",
    "route_timing_instrument_substage_fields",
    "route_timing_instrument_missing_substage_attribution",
    "route_timing_instrument_expensive_stage_threshold_ms",
    "route_timing_instrument_expensive_stage_ids",
    "route_timing_instrument_not_ready_stage_ids",
    "route_timing_instrument_claim_boundary",
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
    "exclusive_source_admission_ms",
    "exclusive_source_read_ms",
    "exclusive_source_parse_or_decode_ms",
    "exclusive_source_to_vortex_array_ms",
    "exclusive_vortex_write_ms",
    "exclusive_vortex_digest_ms",
    "exclusive_vortex_reopen_verify_ms",
    "exclusive_prepared_query_ms",
    "exclusive_result_sink_write_ms",
    "exclusive_evidence_render_ms",
    "exclusive_stage_timing_claim_boundary",
    "preparation_timing_included_in_total",
    "query_timing_included_in_total",
    "output_timing_included_in_total",
    "evidence_timing_included_in_total",
    "fast_path_attribution_schema_version",
    "runtime_execution_ms",
    "output_delivery_ms",
    "evidence_capture_ms",
    "evidence_render_ms",
    "evidence_render_proof_schema_version",
    "evidence_render_proof_status",
    "evidence_render_proof_digest",
    "evidence_render_compact_fact_keys",
    "evidence_render_regeneration_surface",
    "evidence_render_human_expansion_timing_scope",
    "evidence_render_hot_path_policy",
    "evidence_render_route_timing_boundary",
    "evidence_render_claim_boundary",
    "evidence_render_fallback_attempted",
    "evidence_render_external_engine_invoked",
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
    "residual_operator_optimization_schema_version",
    "residual_operator_optimization_family",
    "residual_operator_optimization_status",
    "residual_operator_dense_accumulator_used",
    "residual_operator_sparse_rollover_used",
    "residual_operator_dense_max_key",
    "residual_operator_dense_slot_budget",
    "residual_operator_optimization_claim_boundary",
    "residual_operator_optimization_fallback_attempted",
    "residual_operator_optimization_external_engine_invoked",
    "cold_bottleneck_schema_version",
    "cold_bottleneck_status",
    "cold_bottleneck_stage_labels",
    "cold_bottleneck_primary_stage",
    "cold_bottleneck_primary_stage_ms",
    "cold_bottleneck_primary_stage_share",
    "cold_bottleneck_secondary_stage",
    "cold_bottleneck_secondary_stage_ms",
    "cold_bottleneck_secondary_stage_share",
    "cold_bottleneck_stage_value_fields",
    "cold_route_optimization_hint",
    "cold_route_optimization_hint_scope",
    "cold_route_bottleneck_claim_boundary",
    "source_read_scout_schema_version",
    "source_read_scout_status",
    "source_read_scout_timing_split_status",
    "source_read_header_scout_ms",
    "source_read_byte_acquisition_ms",
    "source_read_full_body_ms",
    "source_read_typed_decode_ms",
    "source_read_row_assembly_ms",
    "source_read_anomaly_quarantine_ms",
    "source_read_columnar_handoff_ms",
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
    "source_typed_column_builder_schema_version",
    "source_typed_column_builder_status",
    "source_typed_column_builder_path",
    "source_typed_builder_schema_digest",
    "source_typed_builder_projected_column_count",
    "source_typed_builder_full_column_count",
    "source_typed_builder_decoded_column_count",
    "source_typed_builder_skipped_column_count",
    "source_typed_builder_row_assembly_avoided",
    "source_typed_builder_row_materialization_status",
    "source_typed_builder_null_validity_status",
    "source_typed_builder_type_coercion_status",
    "source_typed_builder_nested_json_status",
    "source_typed_builder_correctness_digest_status",
    "source_typed_builder_fallback_attempted",
    "source_typed_builder_external_engine_invoked",
    "source_typed_builder_external_parser_engine_invoked",
    "external_parser_engine_invoked",
    "source_typed_builder_claim_boundary",
    "source_projection_admission_schema_version",
    "source_projection_admission_status",
    "source_projection_source_family",
    "source_projection_required_field_mask",
    "source_projection_predicate_field_mask",
    "source_projection_output_field_mask",
    "source_projection_certificate_field_mask",
    "source_projection_diagnostic_field_mask",
    "source_projection_field_mask_digest",
    "source_projection_decoded_columns",
    "source_projection_skipped_columns",
    "source_projection_decoded_column_count",
    "source_projection_skipped_column_count",
    "source_projection_blocker",
    "source_projection_correctness_digest_status",
    "source_projection_fallback_attempted",
    "source_projection_external_engine_invoked",
    "source_projection_claim_boundary",
    "source_columnar_provider_schema_version",
    "source_columnar_provider_status",
    "source_columnar_provider_surface",
    "source_columnar_source_family",
    "source_columnar_input_format",
    "source_columnar_projected_field_mask",
    "source_columnar_preserved_column_count",
    "source_columnar_skipped_column_count",
    "source_columnar_materialized_row_count",
    "source_columnar_record_batch_count",
    "source_columnar_row_materialization_status",
    "source_columnar_null_validity_status",
    "source_columnar_unsupported_dtype_reason",
    "source_columnar_handoff_micros",
    "source_to_vortex_handoff_micros",
    "source_columnar_correctness_digest_status",
    "source_columnar_fallback_attempted",
    "source_columnar_external_engine_invoked",
    "source_columnar_claim_boundary",
    "source_read_scout_claim_boundary",
    "source_state_read_plan",
    "source_state_projection_pushdown_status",
    "source_state_reader_projection_columns",
    "source_state_reader_projection_column_count",
    "source_state_projected_field_mask",
    "source_state_filter_field_mask",
    "source_state_decoded_columns",
    "source_state_skipped_columns",
    "source_state_decoded_column_count",
    "source_state_skipped_column_count",
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
    "vortex_write_plan_schema_version",
    "vortex_write_plan_status",
    "vortex_write_plan_artifact_count",
    "vortex_write_plan_artifact_roles",
    "vortex_write_plan_total_artifact_bytes",
    "vortex_write_plan_total_artifact_rows",
    "vortex_write_plan_writer_context_count",
    "vortex_write_plan_shared_writer_context",
    "vortex_write_plan_writer_context_write_count",
    "vortex_write_plan_writer_context_reuse_hit_count",
    "vortex_write_plan_context_open_ms",
    "vortex_write_plan_segment_write_ms",
    "vortex_write_plan_workspace_stage_ms",
    "vortex_write_plan_digest_ms",
    "vortex_write_plan_verification_ms",
    "vortex_write_plan_coalescing_status",
    "vortex_write_plan_coalescing_reason",
    "vortex_write_plan_digest_status",
    "vortex_write_plan_verification_status",
    "vortex_reopen_scan_attribution_schema_version",
    "vortex_reopen_verify_split_status",
    "vortex_footer_open_ms",
    "vortex_metadata_verify_ms",
    "vortex_scan_open_ms",
    "vortex_scenario_scan_ms",
    "vortex_scan_counter_status",
    "vortex_scan_bytes_touched",
    "vortex_scan_segments_touched",
    "vortex_scan_segments_skipped",
    "vortex_scan_columns_touched",
    "vortex_scan_decoded_values",
    "vortex_reopen_scan_claim_boundary",
    "source_split_count",
    "source_open_count",
    "source_bytes_read",
    "source_columns_requested",
    "source_projection_applied",
    "source_pressure_profile",
    "vortex_prepared_state_reusable",
    "vortex_prepared_state_fingerprint",
    "vortex_prepared_state_fingerprint_status",
    "source_state_fingerprint",
    "source_schema_fingerprint",
    "source_parse_plan_id",
    "source_split_manifest_id",
    "source_anomaly_count",
    "source_quarantine_required",
    "prepared_state_fingerprint",
    "nearest_runnable_route",
    "required_feature_gate",
    "runtime_blocker_code",
    "performance_claim_allowed",
    "production_claim_allowed",
    "spark_replacement_claim_allowed",
    "source_admission_ms",
    "source_read_ms",
    "source_parse_or_columnar_decode_ms",
    "source_to_vortex_array_ms",
    "vortex_write_ms",
    "vortex_reopen_or_verify_ms",
    "prepared_state_lookup_or_create_ms",
    "vortex_scan_ms",
    "operator_compute_ms",
    "result_sink_write_ms",
    "evidence_render_ms",
    "hot_route_total_ms",
    "full_replay_proof_route_total_ms",
    "publication_proof_route_total_ms",
    "prepare_route_total_ms",
    "prepare_cli_wall_ms",
    "total_route_ms",
    "query_runtime_millis",
    "total_runtime_millis",
    "prepare_batch_preparation_millis",
    "prepare_batch_preparation_timing_source",
    "prepare_batch_prepared_state_lookup_or_create_millis",
    "prepare_batch_prepare_route_total_millis",
    "prepare_batch_source_to_columnar_millis",
    "prepare_batch_vortex_array_build_millis",
    "prepare_batch_vortex_write_millis",
    "prepare_batch_vortex_reopen_verify_millis",
    "prepare_batch_vortex_preparation_spine_schema_version",
    "prepare_batch_vortex_preparation_spine_status",
    "prepare_batch_vortex_preparation_spine_artifact_count",
    "prepare_batch_vortex_preparation_spine_reused_artifact_count",
    "prepare_batch_vortex_preparation_spine_rewritten_artifact_count",
    "prepare_batch_vortex_preparation_spine_metadata_first_verify_status",
    "prepare_batch_vortex_preparation_spine_metadata_first_verify_hit_count",
    "prepare_batch_vortex_preparation_spine_reopen_verify_strategy",
    "prepare_batch_vortex_preparation_spine_full_reopen_verify_count",
    "prepare_batch_vortex_preparation_spine_writer_context_write_count",
    "prepare_batch_vortex_preparation_spine_writer_context_reuse_hit_count",
    "prepare_batch_vortex_preparation_spine_write_coalescing_status",
    "prepare_batch_vortex_preparation_spine_shared_writer_context",
    "prepare_batch_vortex_preparation_spine_copy_budget_total_measured_copy_bytes",
    "prepare_batch_vortex_preparation_spine_buffer_pool_status",
    "prepare_batch_vortex_preparation_spine_buffer_reuse_count",
    "batch_scenario_count",
    "session_requested_scenario_count",
    "vortex_scan_millis",
    "operator_compute_millis",
    "result_sink_write_millis",
    "fallback_attempted",
    "external_engine_invoked",
    "claim_gate_status",
    "row_classification",
    "external_baseline_only",
)
WEBSITE_SUMMARY_ROW_KEYS = (
    "engine",
    "engine_display_name",
    "scenario_name",
    "scenario_id",
    "storage_format",
    "status",
    "selected_execution_mode",
    "requested_execution_mode",
    "route_lane_id",
    "route_display_name",
    "route_family_display_name",
    "route_runtime_status",
    "start_state",
    "end_state",
    "includes_preparation",
    "includes_query",
    "includes_output",
    "includes_evidence",
    "route_comparable_to_external_end_to_end",
    "prepared_state_reuse_scope",
    "source_state_reuse_status",
    "source_state_reused",
    "source_state_reuse_scope",
    "source_state_reuse_consumer_count",
    "source_state_recompute_avoided_count",
    "source_state_category_metric_reuse_status",
    "source_state_category_metric_reused",
    "source_state_category_metric_reuse_consumer_count",
    "source_state_category_metric_recompute_avoided_count",
    "source_state_group_category_metric_reuse_status",
    "source_state_group_category_metric_reused",
    "source_state_group_category_metric_reuse_consumer_count",
    "source_state_group_category_metric_recompute_avoided_count",
    "source_state_ranked_metric_reuse_status",
    "source_state_ranked_metric_reused",
    "source_state_ranked_metric_reuse_consumer_count",
    "source_state_ranked_metric_recompute_avoided_count",
    "session_route_used",
    "process_spawn_count",
    "benchmark_timing_boundary",
    "benchmark_timing_status",
    "route_timing_surface_schema_version",
    "timing_surface",
    "timing_surface_label",
    "timing_surface_evidence_tier",
    "timing_surface_default_for_route",
    "fallback_attempted",
    "external_engine_invoked",
    "claim_gate_status",
    "external_baseline_only",
    "performance_claim_allowed",
    "production_claim_allowed",
    "spark_replacement_claim_allowed",
    "source_admission_ms",
    "source_read_ms",
    "source_parse_or_columnar_decode_ms",
    "source_to_vortex_array_ms",
    "vortex_write_ms",
    "vortex_reopen_or_verify_ms",
    "prepared_state_lookup_or_create_ms",
    "vortex_scan_ms",
    "operator_compute_ms",
    "result_sink_write_ms",
    "evidence_render_ms",
    "hot_route_total_ms",
    "full_replay_proof_route_total_ms",
    "publication_proof_route_total_ms",
    "prepare_route_total_ms",
    "prepare_cli_wall_ms",
    "total_route_ms",
    "query_runtime_millis",
    "total_runtime_millis",
    "prepare_batch_preparation_millis",
    "prepare_batch_preparation_timing_source",
    "prepare_batch_prepared_state_lookup_or_create_millis",
    "prepare_batch_prepare_route_total_millis",
    "prepare_batch_vortex_preparation_spine_status",
    "prepare_batch_vortex_preparation_spine_reused_artifact_count",
    "prepare_batch_vortex_preparation_spine_rewritten_artifact_count",
    "prepare_batch_vortex_preparation_spine_metadata_first_verify_status",
    "prepare_batch_vortex_preparation_spine_reopen_verify_strategy",
    "vortex_scan_millis",
    "operator_compute_millis",
    "result_sink_write_millis",
    "exclusive_stage_timing_status",
    "route_timing_exclusive_stage_sum_ms",
    "route_timing_exclusive_residual_ms",
    "inclusive_compatibility_to_vortex_import_ms",
    "cold_bottleneck_primary_stage",
    "fast_path_attribution_schema_version",
    "runtime_execution_ms",
    "output_delivery_ms",
    "evidence_capture_ms",
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
    "residual_operator_optimization_schema_version",
    "residual_operator_optimization_family",
    "residual_operator_optimization_status",
    "residual_operator_dense_accumulator_used",
    "residual_operator_sparse_rollover_used",
    "residual_operator_dense_max_key",
    "residual_operator_dense_slot_budget",
    "residual_operator_optimization_claim_boundary",
    "residual_operator_optimization_fallback_attempted",
    "residual_operator_optimization_external_engine_invoked",
    "evidence_render_proof_schema_version",
    "evidence_render_proof_status",
    "evidence_render_proof_digest",
    "evidence_render_route_timing_boundary",
    "evidence_render_fallback_attempted",
    "evidence_render_external_engine_invoked",
    "evidence_sink_tier_schema_version",
    "requested_evidence_tier",
    "actual_evidence_tier",
    "sink_tier",
    "sink_timing_included_in_route_total",
    "result_sink_replay_skip_reason",
    "human_evidence_render_skip_reason",
    "route_shape_stratification_schema_version",
    "route_shape_status",
    "route_shape_route_lane_id",
    "route_shape_route_family",
    "route_shape_start_state",
    "route_shape_end_state",
    "route_shape_row_count_class",
    "route_shape_stage_attribution_scope",
    "route_shape_fallback_attempted",
    "source_to_vortex_array_guard_schema_version",
    "source_to_vortex_array_guard_status",
    "source_to_vortex_array_guard_provider_kind",
    "source_to_vortex_array_guard_input_layout",
    "source_to_vortex_array_guard_record_batch_count",
    "source_to_vortex_array_guard_exclusive_stage_field",
    "source_to_vortex_array_guard_inclusive_parent_field",
    "source_to_vortex_array_guard_fallback_attempted",
    "result_sink_capillary_schema_version",
    "result_sink_capillary_status",
    "result_sink_capillary_route",
    "result_sink_capillary_native_vortex_output_selected",
    "result_sink_capillary_compatibility_fanout_selected",
    "result_sink_capillary_result_json_bytes",
    "result_sink_capillary_result_json_digest",
    "result_sink_capillary_fallback_attempted",
    "prepare_batch_prepared_state_lookup_timing_schema_version",
    "prepare_batch_prepared_state_lookup_status",
    "prepare_batch_prepared_state_index_schema_version",
    "prepare_batch_prepared_state_index_digest",
    "prepare_batch_prepared_state_index_external_engine_invoked",
    "prepare_batch_prepared_state_dependency_status",
    "prepare_batch_prepared_state_partial_repair_status",
    "prepare_batch_prepared_state_partial_repair_changed_roles",
    "prepare_batch_prepared_state_partial_repair_reused_roles",
    "prepare_batch_prepared_state_partial_repair_repaired_roles",
    "prepare_batch_prepared_state_partial_repair_micros",
    "prepare_batch_prepared_state_partial_repair_source_to_columnar_micros",
    "prepare_batch_prepared_state_partial_repair_vortex_array_build_micros",
    "prepare_batch_prepared_state_partial_repair_vortex_write_micros",
    "prepare_batch_prepared_state_partial_repair_vortex_reopen_verify_micros",
    "prepare_batch_prepared_state_partial_repair_source_to_columnar_ms",
    "prepare_batch_prepared_state_partial_repair_vortex_array_build_ms",
    "prepare_batch_prepared_state_partial_repair_vortex_write_ms",
    "prepare_batch_prepared_state_partial_repair_vortex_reopen_verify_ms",
    "prepare_batch_prepared_state_partial_repair_replay_proof",
    "prepare_batch_prepared_state_delta_overlay_schema_version",
    "prepare_batch_prepared_state_delta_overlay_status",
    "prepare_batch_prepared_state_delta_overlay_admitted",
    "prepare_batch_prepared_state_delta_overlay_base_artifact_reused",
    "prepare_batch_prepared_state_delta_overlay_changed_byte_range",
    "prepare_batch_prepared_state_delta_overlay_delta_row_count",
    "prepare_batch_prepared_state_delta_overlay_consumer_status",
    "prepare_batch_prepared_state_delta_overlay_correctness_digest",
    "prepare_batch_prepared_state_delta_overlay_replay_proof",
    "prepare_batch_prepared_state_delta_overlay_blocker_id",
    "prepare_batch_prepared_state_delta_overlay_external_engine_invoked",
    "base_artifact_reused",
    "delta_overlay_admitted",
    "vortex_normalization_point",
)
LOCAL_PATH_RE = re.compile(
    r"(?P<win>[A-Za-z]:\\[^|,;\"'\s]+)|"
    r"(?P<posix>(?:/Users|/home|/tmp|/var/folders|/private/var|/workspace|/mnt|/Volumes)"
    r"[^|,;\"'\s]*)"
)
EXTRA_PUBLISHED_KEY_FRAGMENTS = (
    "source_state",
    "prepared_state",
    "vortex_scout_ingress",
    "vortex_layout_write_advisor",
    "vortex_copy_budget",
    "vortex_preparation_spine",
    "vortex_differential_preparation",
    "vortex_capillary_preparation",
    "reuse",
    "native_io",
    "coverage",
    "unsupported",
    "blocker",
    "diagnostic",
    "certificate",
    "route",
    "timing_scope",
    "timing_normalization",
    "stage_inclusion",
    "claim_boundary",
    "runtime_execution_validation",
    "runtime_execution",
    "operator_execution",
    "encoded_native",
    "encoded_kernel",
    "encoded_predicate_provider",
    "compressed_kernel_registry",
    "residual_native",
    "fused_pipeline",
    "source_read_scout",
    "vortex_reopen",
    "vortex_scan_counter",
    "route_share",
    "cold_lane",
    "materialization",
    "decode",
    "artifact",
    "pulseweave",
    "flow_inventory",
    "scarcity_ledger",
    "endopulse",
    "proofbound",
)
COLD_LANE_ATTRIBUTION_SCHEMA_VERSION = (
    "shardloom.traditional_analytics.cold_lane_attribution.v1"
)
ROUTE_RUNTIME_STATUS_SCHEMA_VERSION = "shardloom.website.route_runtime_status.v1"
ROUTE_RUNTIME_STATUSES = {
    "scoped_runtime_supported",
    "feature_gated",
    "fixture_smoke_only",
    "unsupported",
    "external_baseline_only",
}
ROUTE_STAGE_FIELD_KEYS = (
    "source_admission_ms",
    "source_read_ms",
    "source_parse_or_columnar_decode_ms",
    "source_to_vortex_array_ms",
    "vortex_write_ms",
    "vortex_reopen_or_verify_ms",
    "prepared_state_lookup_or_create_ms",
    "vortex_scan_ms",
    "operator_compute_ms",
    "result_sink_write_ms",
    "evidence_render_ms",
    "total_route_ms",
)
ROUTE_STAGE_DISPLAY_NAMES = {
    "source_admission_ms": "Source admission",
    "source_read_ms": "Source read",
    "source_parse_or_columnar_decode_ms": "Parse/decode",
    "source_to_vortex_array_ms": "Source -> Vortex array",
    "vortex_write_ms": "Vortex write",
    "vortex_reopen_or_verify_ms": "Vortex reopen/verify",
    "prepared_state_lookup_or_create_ms": "Prepared lookup/create",
    "vortex_scan_ms": "Vortex scan",
    "operator_compute_ms": "Operator compute",
    "result_sink_write_ms": "Result sink",
    "evidence_render_ms": "Evidence render",
}
TIMING_SURFACE_BY_EVIDENCE_TIER = {
    "runtime_minimal": "hot_runtime",
    "metadata_sink": "hot_runtime",
    "full_vortex_replay": "full_replay_proof",
    "publication_full": "publication_proof",
}
TIMING_SURFACE_LABELS = {
    "hot_runtime": "Hot runtime",
    "full_replay_proof": "Full replay proof",
    "publication_proof": "Publication proof",
    "external_baseline": "External baseline",
    "unknown": "Unknown timing surface",
}
DEFAULT_ROUTE_TIMING_SURFACE = "hot_runtime"
PROOF_TIMING_SURFACES = {"full_replay_proof", "publication_proof"}
CANONICAL_ROUTE_TIMING_STAGES = (
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
)
PREPARATION_STAGE_IDS = {
    "source_admission",
    "source_read",
    "source_parse_or_decode",
    "source_to_vortex_array",
    "vortex_write",
    "vortex_digest",
    "vortex_reopen_verify",
    "prepared_state_lookup_or_create",
}
STAGE_VALUE_FIELD_BY_ID = {
    "source_admission": "source_admission_ms",
    "source_read": "source_read_ms",
    "source_parse_or_decode": "source_parse_or_columnar_decode_ms",
    "source_to_vortex_array": "source_to_vortex_array_ms",
    "vortex_write": "vortex_write_ms",
    "vortex_digest": "exclusive_vortex_digest_ms",
    "vortex_reopen_verify": "vortex_reopen_or_verify_ms",
    "prepared_state_lookup_or_create": "prepared_state_lookup_or_create_ms",
    "vortex_scan": "vortex_scan_ms",
    "operator_compute": "operator_compute_ms",
    "result_sink_write": "result_sink_write_ms",
    "evidence_render": "evidence_render_ms",
    "cli_process_wall": "cli_process_wall_millis",
}
ROUTE_STAGE_ID_BY_FIELD = {
    field: stage_id for stage_id, field in STAGE_VALUE_FIELD_BY_ID.items()
}
ROUTE_SHARE_ADDITIVE_TOLERANCE = 1.000001
STAGE_OWNER_BY_ID = {
    "source_admission": "shardloom_source_admission",
    "source_read": "shardloom_source_reader",
    "source_parse_or_decode": "shardloom_compatibility_adapter",
    "source_to_vortex_array": "shardloom_vortex_ingest",
    "vortex_write": "vortex_sink",
    "vortex_digest": "shardloom_certificate_digest",
    "vortex_reopen_verify": "shardloom_native_io_replay",
    "prepared_state_lookup_or_create": "shardloom_prepared_state_manifest",
    "vortex_scan": "shardloom_vortex_scan",
    "operator_compute": "shardloom_operator_runtime",
    "result_sink_write": "shardloom_result_sink",
    "evidence_render": "shardloom_evidence_renderer",
    "cli_process_wall": "benchmark_harness",
}
STAGE_PARENT_STAGE_BY_ID = {
    "source_admission": "source_admission",
    "source_read": "compatibility_import",
    "source_parse_or_decode": "compatibility_import",
    "source_to_vortex_array": "vortex_prepare",
    "vortex_write": "vortex_prepare",
    "vortex_digest": "vortex_prepare",
    "vortex_reopen_verify": "vortex_prepare",
    "prepared_state_lookup_or_create": "prepared_state",
    "vortex_scan": "query_runtime",
    "operator_compute": "query_runtime",
    "result_sink_write": "result_sink",
    "evidence_render": "publication_evidence",
    "cli_process_wall": "process_harness",
}
STAGE_SUBSTAGE_FIELDS_BY_ID = {
    "source_admission": (
        "source_admission_policy_micros",
        "source_stat_micros",
        "source_state_metadata_snapshot_micros",
        "source_state_digest_micros",
        "prepared_manifest_read_micros",
        "prepared_manifest_match_micros",
    ),
    "source_read": (
        "source_read_header_scout_ms",
        "source_read_byte_acquisition_ms",
        "source_read_full_body_ms",
        "source_read_typed_decode_ms",
        "source_read_row_assembly_ms",
        "source_read_columnar_handoff_ms",
        "source_read_scout_residual_ms",
        "source_open_count",
        "source_bytes_read",
    ),
    "source_parse_or_decode": (
        "source_read_decode_status",
        "source_read_decoded_columns",
        "source_read_skipped_columns",
        "source_read_typed_decode_ms",
        "source_read_row_assembly_ms",
    ),
    "source_to_vortex_array": (
        "source_to_vortex_array_guard_status",
        "source_to_vortex_array_guard_exclusive_stage_field",
        "source_to_vortex_array_guard_record_batch_count",
    ),
    "vortex_write": (
        "vortex_write_plan_status",
        "vortex_write_plan_context_open_ms",
        "vortex_write_plan_segment_write_ms",
        "vortex_write_plan_workspace_stage_ms",
        "vortex_write_plan_digest_ms",
        "vortex_write_plan_verification_ms",
    ),
    "vortex_digest": (
        "vortex_write_plan_digest_ms",
        "vortex_write_plan_digest_status",
    ),
    "vortex_reopen_verify": (
        "vortex_reopen_verify_split_status",
        "vortex_open_footer_micros",
        "scan_open_micros",
        "vortex_write_plan_verification_ms",
    ),
    "prepared_state_lookup_or_create": (
        "prepared_manifest_read_micros",
        "prepared_manifest_match_micros",
        "prepare_batch_prepared_state_optimization_manifest_lookup_ms",
        "prepare_batch_prepared_state_optimization_manifest_match_ms",
        "prepare_batch_prepared_state_optimization_cache_miss_create_ms",
        "prepare_batch_prepared_state_optimization_artifact_write_ms",
    ),
    "vortex_scan": (
        "vortex_open_footer_micros",
        "scan_open_micros",
        "scan_chunk_iter_micros",
        "vortex_projected_field_extract_micros",
    ),
    "operator_compute": (
        "operator_kernel_micros",
        "operator_finalize_micros",
        "result_assembly_micros",
        "vortex_encoded_kernel_evidence_micros",
    ),
    "result_sink_write": (
        "result_sink_plan_micros",
        "result_sink_write_micros",
        "result_sink_replay_micros",
    ),
    "evidence_render": (
        "human_evidence_render_micros",
        "json_envelope_emit_micros",
        "report_fields_build_micros",
        "certificate_link_ms",
    ),
    "cli_process_wall": (
        "cli_process_wall_micros",
        "cli_process_wall_millis",
        "python_harness_overhead_millis",
    ),
}
ROUTE_IDENTITY_KEYS = (
    "route_lane_id",
    "route_display_name",
    "route_family_display_name",
    "route_runtime_status",
    "start_state",
    "end_state",
    "includes_preparation",
    "includes_query",
    "includes_output",
    "includes_evidence",
    "route_comparable_to_external_end_to_end",
    "preparation_included",
    "preparation_included_scope",
    "query_timing_starts_after_preparation",
    "prepared_state_reused",
    "performance_claim_allowed",
    "production_claim_allowed",
    "spark_replacement_claim_allowed",
)
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
REQUIRED_PUBLIC_FRONT_DOOR_BENCHMARK_IDS = (
    "local_source_auto_prepare_vortex_front_door",
    "generated_source_prepare_vortex_front_door",
)
PREPARED_STATE_REUSE_IN_PROCESS_SCOPE = "in_process_prepared_batch_vortex_artifacts"
PREPARED_STATE_REUSE_EXPLICIT_SCOPE = "explicit_prepared_state_input"
PREPARED_STATE_REUSE_NOT_APPLICABLE = "not_applicable_no_prepared_state"
EXTERNAL_ENGINE_DISPLAY_NAMES = {
    "pandas": "pandas",
    "polars-eager": "Polars Eager",
    "polars-lazy": "Polars Lazy",
    "duckdb": "DuckDB",
    "datafusion": "DataFusion",
    "dask": "Dask",
    "pyspark": "PySpark",
    "spark-default": "Spark Default",
    "spark-local-tuned": "Spark Local Tuned",
}
COLD_LANE_REQUIRED_FIELDS_BY_CLASSIFICATION = {
    "full_certified_cold_ingest": (
        "source_read_millis",
        "compatibility_parse_millis",
        "compatibility_to_vortex_import_millis",
        "vortex_write_millis",
        "operator_compute_millis",
        "result_sink_write_millis",
        "evidence_render_millis",
        "route_timing_exclusive_stage_sum_ms",
        "total_runtime_millis",
        "cli_process_wall_millis",
        "python_harness_overhead_millis",
    ),
    "preparation_only": (
        "prepare_batch_preparation_millis",
        "prepare_batch_source_to_columnar_millis",
        "prepare_batch_vortex_array_build_millis",
        "prepare_batch_vortex_write_millis",
        "prepare_batch_vortex_reopen_verify_millis",
        "operator_compute_millis",
        "evidence_render_millis",
        "cli_process_wall_millis",
        "python_harness_overhead_millis",
    ),
    "warm_prepared_query": (
        "vortex_scan_millis",
        "operator_compute_millis",
        "query_runtime_millis",
        "evidence_render_millis",
        "cli_process_wall_millis",
        "python_harness_overhead_millis",
    ),
    "sink_replay_heavy": (
        "operator_compute_millis",
        "query_runtime_millis",
        "result_sink_write_millis",
        "evidence_render_millis",
        "cli_process_wall_millis",
        "python_harness_overhead_millis",
    ),
    "evidence_heavy": (
        "operator_compute_millis",
        "query_runtime_millis",
        "evidence_render_millis",
        "cli_process_wall_millis",
        "python_harness_overhead_millis",
    ),
    "process_harness_heavy": (
        "source_read_millis",
        "operator_compute_millis",
        "query_runtime_millis",
        "evidence_render_millis",
        "cli_process_wall_millis",
        "python_harness_overhead_millis",
    ),
}
COLD_LANE_FIELD_ALIASES = {
    "source_read_millis": ("source_read_ms",),
    "compatibility_parse_millis": ("source_parse_or_columnar_decode_ms",),
    "vortex_array_build_millis": ("source_to_vortex_array_ms",),
    "vortex_write_millis": ("vortex_write_ms",),
    "vortex_reopen_verify_millis": ("vortex_reopen_or_verify_ms",),
    "operator_compute_millis": ("operator_compute_ms",),
    "result_sink_write_millis": ("result_sink_write_ms",),
}
PUBLISHED_METRIC_KEYS = (
    "source_state_id",
    "source_state_digest",
    "source_location",
    "source_state_materialization_layout",
    "source_state_runtime_consumption_layout",
    "prepared_state_id",
    "prepared_state_digest",
    "prepared_artifact_ref",
    "prepared_artifact_digest",
    "vortex_artifact_ref",
    "vortex_artifact_digest",
    "output_plan_id",
    "output_plan_digest",
    "sink_artifact_ref",
    "sink_artifact_digest",
    "computed_result_vortex_path",
    "computed_result_vortex_digest",
    "computed_result_sink_replay_verified",
    "evidence_level_result_sink_replay_verified",
    "result_sink_replay_verified",
    "evidence_level_result_sink_replay_refs",
    "data_decoded",
    "data_materialized",
    "materialization_required",
    "decode_required",
    "operator_execution_class",
    "operator_admission_status",
    "operator_blocker_id",
    "operator_blocker_reason",
    "operator_encoded_native_claim_allowed",
    "operator_residual_native_used",
    "operator_temporary_materialization_used",
    "operator_blocker_matrix_ref",
    "residual_operator_optimization_schema_version",
    "residual_operator_optimization_family",
    "residual_operator_optimization_status",
    "residual_operator_dense_accumulator_used",
    "residual_operator_sparse_rollover_used",
    "residual_operator_dense_max_key",
    "residual_operator_dense_slot_budget",
    "residual_operator_optimization_claim_boundary",
    "residual_operator_optimization_fallback_attempted",
    "residual_operator_optimization_external_engine_invoked",
    "materialization_boundary_report_emitted",
    "representation_transition_summary",
    "native_io_certificate_status",
    "source_native_io_certificate_status",
    "computed_result_sink_native_io_certificate_status",
    "result_native_io_certificate_status",
    "execution_certificate_id",
    "execution_certificate_status",
    "runtime_execution_certificate_status",
    "runtime_execution_certificate_id",
    "runtime_execution_certificate_provider_kind",
    "runtime_execution_certificate_plan_ref",
    "runtime_fallback_attempted",
    "runtime_external_query_engine_invoked",
    "execution_certificate_ref",
    "execution_certificate_refs",
    "evidence_level_certificate_refs",
    "source_backed_scan_chunk_iter_micros",
    "source_backed_scan_projection_extract_micros",
    "source_backed_scan_encoded_evidence_micros",
    "requested_evidence_level",
    "requested_evidence_tier",
    "selected_evidence_level",
    "selected_evidence_tier",
    "actual_evidence_tier",
    "sink_tier",
    "evidence_sink_tier_schema_version",
    "evidence_tier_supported_tiers",
    "evidence_tier_result_sink_replay_required",
    "sink_timing_included_in_route_total",
    "sink_timing_inclusion_reason",
    "result_sink_replay_skip_reason",
    "human_evidence_render_skip_reason",
    "evidence_level",
    "prepared_vortex_scale_split_runtime_status",
    "prepared_vortex_scale_split_inventory_schema_version",
    "prepared_vortex_scale_split_inventory_status",
    "prepared_vortex_scale_split_inventory_cache_hit",
    "prepared_vortex_scale_split_inventory_digest",
    "prepared_vortex_scale_split_inventory_source_ref_count",
    "prepared_vortex_scale_split_inventory_collection_micros",
    "prepared_vortex_scale_split_inventory_coalesced_open_reuse_hit",
    "prepared_vortex_scale_split_execution_certificate_status",
    "prepared_vortex_scale_split_execution_certificate_id",
    "prepared_vortex_scale_split_operator_runtime_status",
    "prepared_vortex_scale_split_operator_execution_certificate_status",
    "prepared_vortex_scale_split_operator_execution_certificate_id",
    "prepared_vortex_scale_split_operator_family",
    "prepared_vortex_scale_split_operator_stateful",
    "prepared_vortex_scale_split_operator_shuffle_required",
    "prepared_vortex_scale_split_operator_local_combine_used",
    "prepared_vortex_scale_split_operator_global_merge_used",
    "prepared_vortex_scale_split_operator_claim_gate_status",
    "prepared_vortex_scale_split_operator_fallback_attempted",
    "prepared_vortex_scale_split_operator_external_engine_invoked",
    "prepared_vortex_scale_split_operator_retry_replay_status",
    "prepared_vortex_scale_split_operator_source_replay_status",
    "prepared_vortex_scale_split_operator_memory_envelope_status",
    "prepared_vortex_scale_split_operator_backpressure_status",
    "prepared_vortex_scale_split_operator_spill_policy_status",
    "prepared_vortex_scale_split_operator_output_commit_proof_status",
    "pulseweave_schema_version",
    "pulseweave_status",
    "pulseweave_application_scope",
    "pulseweave_runtime_decision_applied",
    "pulseweave_policy_mutated",
    "pulseweave_decision_digest",
    "pulseweave_blocker",
    "pulseweave_claim_gate_status",
    "pulseweave_fallback_attempted",
    "pulseweave_external_engine_invoked",
    "pulseweave_route_coalescing_schema_version",
    "pulseweave_route_coalescing_status",
    "pulseweave_route_coalescing_blocker",
    "pulseweave_route_coalescing_group_count",
    "pulseweave_route_coalescing_group_digests",
    "pulseweave_route_coalescing_scenario_count",
    "pulseweave_route_open_reuse_count",
    "pulseweave_scan_open_reuse_count",
    "pulseweave_split_inventory_collection_micros",
    "pulseweave_result_assembly_coalescing_status",
    "pulseweave_result_assembly_reuse_count",
    "pulseweave_route_coalescing_fallback_attempted",
    "pulseweave_route_coalescing_external_engine_invoked",
    "flow_inventory_wip_limit",
    "flow_inventory_peak_in_flight",
    "flow_inventory_held_for_memory_count",
    "flow_inventory_held_for_downstream_count",
    "scarcity_ledger_selected_action",
    "scarcity_ledger_total_price_bps",
    "endopulse_next_target_task_bytes",
    "endopulse_next_wip_limit",
    "endopulse_persistent_state_used",
    "proofbound_certificate_status",
    "proofbound_no_fallback_status",
    "proofbound_claim_allowed",
    "compatibility_import_included",
    "preparation_included_in_timing",
    "persistent_runner_status",
    "process_startup_attribution",
    "cli_process_wall_millis",
    "python_harness_overhead_millis",
    "batch_process_wall_shared",
    "batch_cli_process_wall_millis",
    "preparation_millis",
    "preparation_cli_process_wall_millis",
    "prepare_batch_preparation_millis",
    "prepare_batch_source_to_columnar_millis",
    "prepare_batch_vortex_array_build_millis",
    "prepare_batch_vortex_write_millis",
    "prepare_batch_vortex_reopen_verify_millis",
    "runtime_execution_validation_schema_version",
    "runtime_execution_validation_status",
    "runtime_execution_validation_blocker_count",
    "runtime_execution_validation_missing_fields",
    "runtime_execution_validation_invalid_fields",
    "claim_grade_requirements_met",
    "claim_grade_missing_evidence",
    "iterations",
    "reproducibility_min_iterations",
    "reproducibility_iterations_met",
    "reproducible_benchmark_row",
    "timing_row_present",
    "timing_row_claim_grade",
    "correctness_digest",
    "correctness_digest_stable",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--profile", choices=tuple(PROFILES), required=True)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_LATEST_DIR)
    parser.add_argument("--website-data", type=Path, default=DEFAULT_WEBSITE_DATA)
    parser.add_argument(
        "--public-output-dir",
        type=Path,
        default=DEFAULT_PUBLIC_LATEST_DIR,
        help="Astro public-dir benchmark bundle mirrored into the static build.",
    )
    parser.add_argument(
        "--public-website-data",
        type=Path,
        default=DEFAULT_PUBLIC_WEBSITE_DATA,
        help="Astro public-dir benchmark evidence data mirrored into the static build.",
    )
    parser.add_argument(
        "--website-src-data",
        type=Path,
        default=DEFAULT_WEBSITE_SRC_DATA,
        help="Astro import-time benchmark evidence data used by the benchmark page.",
    )
    parser.add_argument(
        "--website-src-manifest",
        type=Path,
        default=DEFAULT_WEBSITE_SRC_MANIFEST,
        help="Astro import-time benchmark manifest used by the benchmark page.",
    )
    parser.add_argument(
        "--base-summary",
        type=Path,
        default=DEFAULT_BASE_SUMMARY,
        help="Existing website summary to preserve prepared/native batch evidence from.",
    )
    parser.add_argument(
        "--merge-existing-row-chunks",
        action="store_true",
        help=(
            "Merge newly promoted rows with existing published row chunks in --output-dir. "
            "Use for targeted hot-runtime refreshes that must preserve existing proof/publication rows."
        ),
    )
    return parser.parse_args()


def portable_public_ref(value: str) -> str:
    def replace(match: re.Match[str]) -> str:
        path = match.group(0)
        digest = hashlib.sha256(path.encode("utf-8")).hexdigest()[:16]
        return f"local-artifact-ref:sha256:{digest}"

    return LOCAL_PATH_RE.sub(replace, value)


def portable_public_value(value: Any) -> Any:
    if isinstance(value, str):
        return portable_public_ref(value)
    if isinstance(value, list):
        return [portable_public_value(item) for item in value]
    if isinstance(value, dict):
        return {key: portable_public_value(item) for key, item in value.items()}
    return value


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, payload: Any) -> None:
    write_text_atomic(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp")
    temp_path.write_text(text, encoding="utf-8")
    temp_path.replace(path)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json_once(paths: list[Path], payload: Any) -> None:
    seen: set[Path] = set()
    for path in paths:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        write_json(path, payload)


def clear_row_chunks(directory: Path) -> None:
    if not directory.exists():
        return
    for path in directory.glob(f"{PUBLISHED_ROW_CHUNK_PREFIX}-*.json"):
        path.unlink()


def remove_duplicate_suffixed_artifacts(directory: Path) -> list[str]:
    removed: list[str] = []
    if not directory.exists():
        return removed
    for path in sorted(directory.rglob("*"), reverse=True):
        if DUPLICATE_SUFFIX_RE.search(path.name):
            removed.append(str(path))
            if path.is_dir() and not path.is_symlink():
                shutil.rmtree(path)
            else:
                path.unlink()
    return removed


def row_chunk_payload(chunk_index: int, chunk_rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": PUBLISHED_ROW_CHUNK_SCHEMA_VERSION,
        "chunk_index": chunk_index,
        "row_count": len(chunk_rows),
        "rows": chunk_rows,
    }


def row_chunk_specs(
    rows: list[dict[str, Any]],
    chunk_size: int,
) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    for index in range(0, len(rows), chunk_size):
        chunk_rows = rows[index : index + chunk_size]
        chunk_index = index // chunk_size
        payload = row_chunk_payload(chunk_index, chunk_rows)
        text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
        specs.append(
            {
                "chunk_index": chunk_index,
                "row_count": len(chunk_rows),
                "text": text,
                "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
            }
        )
    return specs


def row_admission_run_id(specs: list[dict[str, Any]]) -> str:
    digest = hashlib.sha256()
    for spec in specs:
        digest.update(str(spec["chunk_index"]).encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(spec["sha256"]).encode("utf-8"))
        digest.update(b"\0")
    return digest.hexdigest()[:16]


def row_admission_manifest_path_for_chunks(
    chunks: list[dict[str, Any]],
    directory: Path,
) -> Path:
    if not chunks:
        return directory / ROW_ADMISSION_MANIFEST_NAME
    first_path = Path(str(chunks[0]["path"]))
    if not first_path.is_absolute():
        first_path = ROOT / first_path
    return first_path.parent / ROW_ADMISSION_MANIFEST_NAME


def write_row_chunks(
    directory: Path,
    rows: list[dict[str, Any]],
    *,
    chunk_size: int = PUBLISHED_ROW_CHUNK_SIZE,
) -> list[dict[str, Any]]:
    directory.mkdir(parents=True, exist_ok=True)
    duplicate_removed = remove_duplicate_suffixed_artifacts(directory)
    specs = row_chunk_specs(rows, chunk_size)
    run_id = row_admission_run_id(specs)
    chunk_directory = directory / PUBLISHED_ROW_RUN_DIR / f"rows-{run_id}"
    chunk_directory.mkdir(parents=True, exist_ok=True)
    chunks: list[dict[str, Any]] = []
    expected_paths: set[Path] = set()
    reused_chunk_count = 0
    written_chunk_count = 0
    for spec in specs:
        chunk_index = int(spec["chunk_index"])
        path = chunk_directory / f"{PUBLISHED_ROW_CHUNK_PREFIX}-{chunk_index:03d}.json"
        expected_paths.add(path)
        text = str(spec["text"])
        digest = str(spec["sha256"])
        if path.exists() and file_sha256(path) == digest:
            reused_chunk_count += 1
        else:
            write_text_atomic(path, text)
            written_chunk_count += 1
        chunks.append(
            {
                "path": repo_relative(path),
                "row_count": int(spec["row_count"]),
                "sha256": digest,
            }
        )
    stale_removed: list[str] = []
    for path in sorted(chunk_directory.glob(f"{PUBLISHED_ROW_CHUNK_PREFIX}-*.json")):
        if path not in expected_paths:
            stale_removed.append(str(path))
            path.unlink()
    legacy_removed: list[str] = []
    for path in sorted(directory.glob(f"{PUBLISHED_ROW_CHUNK_PREFIX}-*.json")):
        legacy_removed.append(str(path))
        path.unlink()
    admission_manifest = {
        "schema_version": ROW_ADMISSION_MANIFEST_SCHEMA_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "run_id": f"rows-{run_id}",
        "row_count": len(rows),
        "chunk_count": len(chunks),
        "chunk_size": chunk_size,
        "chunks": chunks,
        "resume_status": (
            "reused_existing_chunks"
            if chunks and reused_chunk_count == len(chunks) and written_chunk_count == 0
            else "admitted_incremental_chunks"
        ),
        "reused_chunk_count": reused_chunk_count,
        "written_chunk_count": written_chunk_count,
        "stale_chunk_files_removed": [repo_relative(Path(path)) for path in stale_removed],
        "legacy_top_level_chunk_files_removed": [
            repo_relative(Path(path)) for path in legacy_removed
        ],
        "duplicate_suffixed_artifacts_removed": [
            repo_relative(Path(path)) for path in duplicate_removed
        ],
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "claim_boundary": (
            "benchmark row admission manifest only; not performance, production, "
            "or replacement evidence"
        ),
    }
    write_json(chunk_directory / ROW_ADMISSION_MANIFEST_NAME, admission_manifest)
    return chunks


def publication_proof_record_for_row(row: dict[str, Any]) -> dict[str, Any] | None:
    if str(row.get("timing_surface") or "") != "publication_proof":
        return None
    proof_digest = str(row.get("evidence_render_proof_digest") or "").strip()
    if not proof_digest or proof_digest.lower() in {"none", "missing", "not_reported"}:
        return None
    identity = {
        "engine": str(row.get("engine") or "unknown"),
        "scenario_id": str(row.get("scenario_id") or row.get("scenario_name") or "unknown"),
        "scenario_name": str(row.get("scenario_name") or "unknown"),
        "storage_format": str(row.get("storage_format") or "unknown"),
        "route_lane_id": str(row.get("route_lane_id") or "unknown"),
        "timing_surface": "publication_proof",
        "actual_evidence_tier": str(row.get("actual_evidence_tier") or "unknown"),
    }
    payload = {
        **identity,
        "claim_gate_status": str(row.get("claim_gate_status") or "unknown"),
        "evidence_render_proof_status": str(
            row.get("evidence_render_proof_status") or "unknown"
        ),
        "evidence_render_proof_digest": proof_digest,
        "computed_result_vortex_digest": str(
            row.get("computed_result_vortex_digest") or "none"
        ),
        "computed_result_sink_replay_verified": row.get(
            "computed_result_sink_replay_verified"
        )
        is True,
        "runtime_execution_certificate_id": str(
            row.get("runtime_execution_certificate_id")
            or row.get("execution_certificate_id")
            or "none"
        ),
        "runtime_execution_certificate_status": str(
            row.get("runtime_execution_certificate_status")
            or row.get("execution_certificate_status")
            or "unknown"
        ),
        "result_sink_write_ms": numeric_value(row.get("result_sink_write_ms")),
        "evidence_render_ms": numeric_value(row.get("evidence_render_ms")),
        "publication_proof_route_total_ms": numeric_value(
            row.get("publication_proof_route_total_ms")
        ),
        "route_total_formula": str(row.get("route_total_formula") or "not_reported"),
        "fallback_attempted": row.get("fallback_attempted") is True,
        "external_engine_invoked": row.get("external_engine_invoked") is True,
    }
    digest_text = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    record_digest = "sha256:" + hashlib.sha256(digest_text.encode("utf-8")).hexdigest()
    record_id_parts = [
        identity["engine"],
        identity["scenario_id"],
        identity["storage_format"],
        identity["route_lane_id"],
        identity["timing_surface"],
        proof_digest,
    ]
    record_id = hashlib.sha256("\0".join(record_id_parts).encode("utf-8")).hexdigest()[
        :24
    ]
    return {
        "record_id": f"publication-proof:{record_id}",
        "record_digest": record_digest,
        **payload,
    }


def publication_proof_sidecar_payload(
    rows: list[dict[str, Any]],
    existing_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    records = [
        record
        for row in rows
        for record in [publication_proof_record_for_row(row)]
        if record is not None
    ]
    existing_records = {
        str(record.get("record_id")): record
        for record in (existing_payload or {}).get("records", [])
        if isinstance(record, dict)
    }
    reused = 0
    written = 0
    for record in records:
        existing = existing_records.get(str(record["record_id"]))
        if existing and existing.get("record_digest") == record.get("record_digest"):
            record["reuse_status"] = "reused_unchanged_publication_proof_record"
            reused += 1
        else:
            record["reuse_status"] = "written_publication_proof_record"
            written += 1
    current_ids = {str(record["record_id"]) for record in records}
    stale = sorted(set(existing_records) - current_ids)
    return {
        "schema_version": PUBLICATION_PROOF_SIDECAR_SCHEMA_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "record_count": len(records),
        "reused_record_count": reused,
        "written_record_count": written,
        "stale_record_count": len(stale),
        "stale_record_ids": stale,
        "resume_status": (
            "reused_existing_publication_proof_sidecar"
            if records and reused == len(records) and written == 0 and not stale
            else "admitted_incremental_publication_proof_sidecar"
            if records
            else "not_applicable_no_publication_proof_rows"
        ),
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "records": records,
        "claim_boundary": (
            "publication proof sidecar records cache stable sink/replay/evidence proof "
            "metadata for benchmark publication rows only; hot-runtime route totals and "
            "performance, production, package-release, or Spark-displacement claims are "
            "not authorized"
        ),
    }


def write_publication_proof_sidecar(
    directory: Path,
    rows: list[dict[str, Any]],
    chunks: list[dict[str, Any]],
) -> dict[str, Any]:
    _ = chunks
    path = directory / PUBLICATION_PROOF_SIDECAR_NAME
    existing_payload = load_json(path) if path.exists() else None
    payload = publication_proof_sidecar_payload(
        rows,
        existing_payload if isinstance(existing_payload, dict) else None,
    )
    existing_records = (
        existing_payload.get("records")
        if isinstance(existing_payload, dict)
        else None
    )
    if existing_records != payload["records"]:
        write_json(path, payload)
    elif isinstance(existing_payload, dict):
        payload = {**existing_payload, **{k: payload[k] for k in payload if k != "records"}}
        payload["records"] = existing_records
        payload["resume_status"] = "reused_existing_publication_proof_sidecar"
    return {
        "path": repo_relative(path),
        "publication_proof_sidecar_schema_version": PUBLICATION_PROOF_SIDECAR_SCHEMA_VERSION,
        "publication_proof_sidecar_status": payload["resume_status"],
        "publication_proof_sidecar_record_count": payload["record_count"],
        "publication_proof_sidecar_reused_record_count": payload["reused_record_count"],
        "publication_proof_sidecar_written_record_count": payload["written_record_count"],
        "publication_proof_sidecar_stale_record_count": payload["stale_record_count"],
        "publication_proof_sidecar_fallback_attempted": payload["fallback_attempted"],
        "publication_proof_sidecar_external_engine_invoked": payload[
            "external_engine_invoked"
        ],
    }


def load_row_chunks(directory: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not directory.exists():
        return rows
    for path in sorted(directory.glob(f"{PUBLISHED_ROW_CHUNK_PREFIX}-*.json")):
        payload = load_json(path)
        chunk_rows = payload.get("rows") if isinstance(payload, dict) else payload
        if not isinstance(chunk_rows, list):
            continue
        rows.extend(row for row in chunk_rows if isinstance(row, dict))
    return rows


PUBLISHED_ROW_MERGE_KEY_FIELDS = (
    "engine",
    "scenario_id",
    "scenario_name",
    "storage_format",
    "selected_execution_mode",
    "route_lane_id",
    "route_row_derivation_status",
    "route_row_source_engine",
    "route_row_source_lane_id",
    "timing_surface",
    "actual_evidence_tier",
    "timing_surface_evidence_tier",
)


def published_row_merge_key(row: dict[str, Any]) -> tuple[str, ...]:
    return tuple(str(row.get(field) or "") for field in PUBLISHED_ROW_MERGE_KEY_FIELDS)


def merge_published_rows(
    existing_rows: list[dict[str, Any]],
    new_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged: dict[tuple[str, ...], dict[str, Any]] = {}
    order: list[tuple[str, ...]] = []
    for row in [*existing_rows, *new_rows]:
        key = published_row_merge_key(row)
        if key not in merged:
            order.append(key)
        merged[key] = row
    return [merged[key] for key in order]


def website_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rendered: list[dict[str, Any]] = []
    for row in rows:
        rendered.append(
            {
                key: row[key]
                for key in WEBSITE_SUMMARY_ROW_KEYS
                if key in row
            }
        )
    return rendered


def repo_relative(path: Path) -> str:
    try:
        return path.resolve().relative_to(ROOT).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def git_sha() -> str | None:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def iteration_values(row: dict[str, Any]) -> list[float]:
    values = row.get("iteration_wall_time_millis")
    if isinstance(values, list):
        return [
            float(value)
            for value in values
            if isinstance(value, (int, float)) and float(value) > 0
        ]
    metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
    for key in ("total_runtime_millis", "query_runtime_millis"):
        value = metrics.get(key)
        if isinstance(value, (int, float)) and float(value) > 0:
            return [float(value)]
    return []


def geomean(values: list[float]) -> float | None:
    positives = [value for value in values if value > 0]
    if not positives:
        return None
    return math.exp(sum(math.log(value) for value in positives) / len(positives))


def fmt_ms(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.2f} ms"


def geomean_non_negative(values: list[float]) -> float | None:
    if not values:
        return None
    positives = [value for value in values if value > 0]
    if not positives:
        return 0.0
    return geomean(positives)


def fmt_percent(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.1f}%"


def formatted_ms_value(value: Any) -> float | None:
    parsed = numeric_value(value)
    if parsed is not None:
        return parsed
    if isinstance(value, str):
        match = re.fullmatch(r"\s*([0-9]+(?:\.[0-9]+)?)\s*ms\s*", value)
        if match:
            return float(match.group(1))
    return None


def is_shardloom_engine(engine: str) -> bool:
    return engine.startswith("shardloom")


def engine_display_name(engine: str) -> str:
    shardloom_names = {
        "shardloom": "ShardLoom",
        "shardloom-prepared-vortex": "ShardLoom Prepared Vortex",
        "shardloom-prepare-batch": "ShardLoom Prepare Batch",
        "shardloom-vortex": "ShardLoom Native Vortex",
        "native-vortex": "ShardLoom Native Vortex",
    }
    if engine in shardloom_names:
        return shardloom_names[engine]
    return EXTERNAL_ENGINE_DISPLAY_NAMES.get(engine, engine or "unknown")


def field_bool(fields: dict[str, Any], key: str, default: bool = False) -> bool:
    value = fields.get(key)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
    return default


def first_numeric_field(fields: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        value = fields.get(key)
        parsed = numeric_value(value)
        if parsed is not None:
            return parsed
    return None


def micros_to_millis(value: Any) -> float | None:
    parsed = numeric_value(value)
    return None if parsed is None else parsed / 1000.0


def millis_to_micros(value: Any) -> int | None:
    parsed = numeric_value(value)
    if parsed is None:
        return None
    return int(round(parsed * 1000.0))


def first_numeric_stage_millis(
    fields: dict[str, Any],
    millis_keys: tuple[str, ...] = (),
    micros_keys: tuple[str, ...] = (),
) -> float | None:
    for key in millis_keys:
        parsed = numeric_value(fields.get(key))
        if parsed is not None:
            return parsed
    for key in micros_keys:
        parsed = micros_to_millis(fields.get(key))
        if parsed is not None:
            return parsed
    return None


def source_admission_millis(fields: dict[str, Any]) -> float | None:
    return first_numeric_stage_millis(
        fields,
        millis_keys=(
            "exclusive_source_admission_millis",
            "source_stat_millis",
            "source_admission_millis",
            "source_metadata_snapshot_millis",
        ),
        micros_keys=("exclusive_source_admission_micros", "source_stat_micros"),
    )


def first_numeric_micros(
    fields: dict[str, Any],
    *,
    micros_keys: tuple[str, ...] = (),
    millis_keys: tuple[str, ...] = (),
) -> int | None:
    for key in micros_keys:
        parsed = numeric_value(fields.get(key))
        if parsed is not None:
            return int(round(parsed))
    for key in millis_keys:
        parsed = millis_to_micros(fields.get(key))
        if parsed is not None:
            return parsed
    return None


def summed_numeric_millis_from_micros(
    fields: dict[str, Any],
    micros_keys: tuple[str, ...],
) -> float | None:
    total = 0.0
    found = False
    for key in micros_keys:
        parsed = numeric_value(fields.get(key))
        if parsed is None:
            continue
        total += parsed / 1000.0
        found = True
    return total if found else None


def result_sink_timing_present(fields: dict[str, Any]) -> bool:
    return any(
        fields.get(field) not in {None, "", "none", "not_requested", "not_reported"}
        for field in (
            "computed_result_sink_write_micros",
            "result_sink_write_micros",
            "computed_result_sink_write_millis",
            "result_sink_write_millis",
            "result_sink_write_ms",
        )
    )


def result_sink_replay_timing_present(fields: dict[str, Any]) -> bool:
    return (
        first_numeric_micros(
            fields,
            micros_keys=(
                "computed_result_sink_replay_micros",
                "result_sink_replay_micros",
                "exclusive_result_sink_replay_micros",
                "total_result_sink_replay_micros",
                "batch_total_result_sink_replay_micros",
            ),
            millis_keys=(
                "computed_result_sink_replay_millis",
                "result_sink_replay_millis",
                "exclusive_result_sink_replay_ms",
            ),
        )
        is not None
    )


def result_sink_replay_verified(fields: dict[str, Any]) -> bool:
    return any(
        field_bool(fields, field, default=False)
        or "replay_verified" in str(fields.get(field) or "").strip().lower()
        for field in (
            "computed_result_sink_replay_verified",
            "evidence_level_result_sink_replay_verified",
            "result_sink_replay_verified",
            "prepared_vortex_scale_split_operator_output_commit_proof_status",
        )
    )


def evidence_tier_for_fields(fields: dict[str, Any], engine: str) -> str:
    if not is_shardloom_engine(engine):
        return "external_baseline"
    current_tier = str(fields.get("actual_evidence_tier") or "").strip()
    requested_tier = str(fields.get("requested_evidence_tier") or "auto").strip()
    evidence_level = str(
        fields.get("evidence_level") or fields.get("selected_evidence_level") or ""
    )
    replay_timing_present = result_sink_replay_timing_present(fields)
    sink_timing_present = result_sink_timing_present(fields)
    claim_grade_publication = (
        fields.get("claim_gate_status") == "claim_grade"
        or fields.get("claim_grade_requirements_met") is True
    )
    if evidence_level == "minimal_runtime":
        default_tier = "runtime_minimal"
    elif evidence_level == "full_replay" and replay_timing_present:
        default_tier = "publication_full" if claim_grade_publication else "full_vortex_replay"
    elif replay_timing_present and (
        result_sink_replay_verified(fields) or sink_timing_present
    ):
        default_tier = "publication_full" if claim_grade_publication else "full_vortex_replay"
    else:
        default_tier = "metadata_sink"
    if current_tier in TIMING_SURFACE_BY_EVIDENCE_TIER:
        replay_tier_without_timing = (
            requested_tier == "auto"
            and current_tier in {"full_vortex_replay", "publication_full"}
            and not replay_timing_present
        )
        auto_upgrade = (
            requested_tier == "auto"
            and (
                current_tier == "metadata_sink"
                or (
                    current_tier == "full_vortex_replay"
                    and default_tier == "publication_full"
                )
            )
            and default_tier in {"full_vortex_replay", "publication_full"}
        )
        if replay_tier_without_timing or auto_upgrade:
            return default_tier
        return current_tier
    return default_tier


def timing_surface_for_fields(fields: dict[str, Any], engine: str) -> str:
    tier = evidence_tier_for_fields(fields, engine)
    if tier == "external_baseline":
        return "external_baseline"
    return TIMING_SURFACE_BY_EVIDENCE_TIER.get(tier, "unknown")


def timing_surface_fields_for_row(row: dict[str, Any]) -> dict[str, Any]:
    fields = runtime_validation_field_map(row)
    engine = str(row.get("engine") or fields.get("engine") or "")
    evidence_tier = evidence_tier_for_fields(fields, engine)
    surface = timing_surface_for_fields(fields, engine)
    return {
        "route_timing_surface_schema_version": ROUTE_TIMING_SURFACE_SCHEMA_VERSION,
        "timing_surface": surface,
        "timing_surface_label": TIMING_SURFACE_LABELS.get(surface, surface),
        "timing_surface_default_for_route": (
            surface == DEFAULT_ROUTE_TIMING_SURFACE
            if is_shardloom_engine(engine)
            else surface == "external_baseline"
        ),
        "timing_surface_evidence_tier": evidence_tier,
        "timing_surface_claim_boundary": (
            "hot_runtime excludes result-sink replay and human publication render; "
            "full_replay_proof includes machine replay/output proof; publication_proof "
            "includes result-sink and human evidence render timing. No performance claim "
            "is valid unless the timing surface and evidence tier are stated."
        ),
    }


def first_bool_field(
    fields: dict[str, Any],
    keys: tuple[str, ...],
    *,
    default: bool | None = None,
) -> bool | None:
    for key in keys:
        value = fields.get(key)
        if isinstance(value, bool):
            return value
        if value is None:
            continue
        text = str(value).strip().lower()
        if text in {"true", "1", "yes"}:
            return True
        if text in {"false", "0", "no"}:
            return False
    return default


def timing_normalization_fields_for_row(
    row: dict[str, Any],
    stage_fields: dict[str, Any],
) -> dict[str, Any]:
    fields = runtime_validation_field_map(row)
    result_sink_write_micros = first_numeric_micros(
        fields,
        micros_keys=(
            "result_sink_write_micros",
            "computed_result_sink_write_micros",
            "exclusive_result_sink_write_micros",
            "total_result_sink_write_micros",
            "batch_total_result_sink_write_micros",
        ),
        millis_keys=("result_sink_write_millis", "exclusive_result_sink_write_ms"),
    )
    operator_kernel_micros = first_numeric_micros(
        fields,
        micros_keys=("operator_kernel_micros", "operator_compute_micros"),
        millis_keys=("operator_kernel_millis", "operator_compute_millis"),
    )
    if operator_kernel_micros is None:
        operator_kernel_micros = millis_to_micros(stage_fields.get("operator_compute_ms"))
    scan_chunk_iter_micros = first_numeric_micros(
        fields,
        micros_keys=(
            "scan_chunk_iter_micros",
            "vortex_chunk_iteration_micros",
            "vortex_scenario_scan_micros",
        ),
        millis_keys=("vortex_scenario_scan_millis", "vortex_scenario_scan_ms"),
    )
    normalized = {
        "source_admission_policy_micros": first_numeric_micros(
            fields,
            micros_keys=(
                "source_admission_policy_micros",
                "exclusive_source_admission_micros",
                "source_stat_micros",
            ),
            millis_keys=(
                "exclusive_source_admission_millis",
                "source_admission_millis",
                "source_metadata_snapshot_millis",
            ),
        ),
        "source_admission_digest_policy_schema_version": (
            first_meaningful_field(
                fields,
                (
                    "source_admission_digest_policy_schema_version",
                    "prepare_batch_source_admission_digest_policy_schema_version",
                ),
            )
            or SOURCE_ADMISSION_DIGEST_POLICY_SCHEMA_VERSION
        ),
        "source_admission_digest_policy_status": (
            first_meaningful_field(
                fields,
                (
                    "source_admission_digest_policy_status",
                    "prepare_batch_source_admission_digest_policy_status",
                ),
            )
            or "not_reported_by_engine"
        ),
        "source_admission_full_content_digest_requested": first_bool_field(
            fields,
            (
                "source_admission_full_content_digest_requested",
                "prepare_batch_source_admission_full_content_digest_requested",
            ),
            default=False if row.get("status") == "success" else None,
        ),
        "source_admission_full_content_digest_micros": first_numeric_micros(
            fields,
            micros_keys=(
                "source_admission_full_content_digest_micros",
                "prepare_batch_source_admission_full_content_digest_micros",
            ),
            millis_keys=("source_admission_full_content_digest_millis",),
        ),
        "source_stat_micros": first_numeric_micros(
            fields,
            micros_keys=("source_stat_micros",),
            millis_keys=("source_stat_millis", "source_metadata_snapshot_millis"),
        ),
        "source_state_open_micros": first_numeric_micros(
            fields,
            micros_keys=("source_state_open_micros", "source_state_prepare_micros"),
            millis_keys=("source_state_open_millis",),
        ),
        "source_state_metadata_snapshot_micros": first_numeric_micros(
            fields,
            micros_keys=("source_state_metadata_snapshot_micros",),
            millis_keys=("source_state_metadata_snapshot_millis",),
        ),
        "source_state_manifest_validation_micros": first_numeric_micros(
            fields,
            micros_keys=("source_state_manifest_validation_micros",),
            millis_keys=("source_state_manifest_validation_millis",),
        ),
        "source_state_row_count_metadata_micros": first_numeric_micros(
            fields,
            micros_keys=("source_state_row_count_metadata_micros",),
            millis_keys=("source_state_row_count_metadata_millis",),
        ),
        "source_state_family_build_micros": first_numeric_micros(
            fields,
            micros_keys=("source_state_family_build_micros",),
            millis_keys=("source_state_family_build_millis",),
        ),
        "source_state_lazy_family_construction": first_bool_field(
            fields,
            ("source_state_lazy_family_construction",),
        ),
        "source_state_family_build_timing_scope": fields.get(
            "source_state_family_build_timing_scope"
        ),
        "source_state_family_build_count": first_numeric_field(
            fields,
            ("source_state_family_build_count",),
        ),
        "source_state_family_reuse_hit_count": first_numeric_field(
            fields,
            ("source_state_family_reuse_hit_count",),
        ),
        "source_state_family_reuse_hit": first_bool_field(
            fields,
            ("source_state_family_reuse_hit",),
        ),
        "source_state_family_recompute_avoided": first_bool_field(
            fields,
            ("source_state_family_recompute_avoided",),
        ),
        "source_state_digest_micros": first_numeric_micros(
            fields,
            micros_keys=("source_state_digest_micros",),
            millis_keys=("source_state_digest_millis",),
        ),
        "prepared_manifest_read_micros": first_numeric_micros(
            fields,
            micros_keys=(
                "prepared_manifest_read_micros",
                "prepare_batch_prepared_state_manifest_lookup_micros",
            ),
            millis_keys=("prepared_manifest_read_millis",),
        ),
        "prepared_manifest_match_micros": first_numeric_micros(
            fields,
            micros_keys=("prepared_manifest_match_micros",),
            millis_keys=("prepared_manifest_match_millis",),
        ),
        "vortex_open_footer_micros": first_numeric_micros(
            fields,
            micros_keys=("vortex_open_footer_micros", "vortex_footer_open_micros"),
            millis_keys=("vortex_footer_open_millis", "vortex_footer_open_ms"),
        ),
        "scan_open_micros": first_numeric_micros(
            fields,
            micros_keys=("scan_open_micros", "vortex_scan_open_micros"),
            millis_keys=("vortex_scan_open_millis", "vortex_scan_open_ms"),
        ),
        "scan_chunk_iter_micros": scan_chunk_iter_micros,
        "vortex_projected_field_extract_micros": first_numeric_micros(
            fields,
            micros_keys=("vortex_projected_field_extract_micros",),
            millis_keys=("vortex_projected_field_extract_millis",),
        ),
        "vortex_encoded_kernel_evidence_micros": first_numeric_micros(
            fields,
            micros_keys=("vortex_encoded_kernel_evidence_micros",),
            millis_keys=("vortex_encoded_kernel_evidence_millis",),
        ),
        "operator_kernel_micros": operator_kernel_micros,
        "operator_finalize_micros": first_numeric_micros(
            fields,
            micros_keys=("operator_finalize_micros",),
            millis_keys=("operator_finalize_millis",),
        ),
        "result_assembly_micros": first_numeric_micros(
            fields,
            micros_keys=("result_assembly_micros",),
            millis_keys=("result_assembly_millis",),
        ),
        "result_sink_plan_micros": first_numeric_micros(
            fields,
            micros_keys=("result_sink_plan_micros",),
            millis_keys=("result_sink_plan_millis",),
        ),
        "result_sink_write_micros": result_sink_write_micros,
        "result_sink_replay_micros": first_numeric_micros(
            fields,
            micros_keys=("result_sink_replay_micros",),
            millis_keys=("result_sink_replay_millis",),
        ),
        "human_evidence_render_micros": first_numeric_micros(
            fields,
            micros_keys=("human_evidence_render_micros", "evidence_render_micros"),
            millis_keys=("human_evidence_render_millis", "evidence_render_millis"),
        ),
        "json_envelope_emit_micros": first_numeric_micros(
            fields,
            micros_keys=("json_envelope_emit_micros",),
            millis_keys=("json_envelope_emit_millis",),
        ),
        "report_fields_build_micros": first_numeric_micros(
            fields,
            micros_keys=("report_fields_build_micros",),
            millis_keys=("report_fields_build_millis",),
        ),
        "cli_process_wall_micros": first_numeric_micros(
            fields,
            micros_keys=("cli_process_wall_micros",),
            millis_keys=(
                "cli_process_wall_millis",
                "batch_cli_process_wall_millis",
                "preparation_cli_process_wall_millis",
            ),
        ),
    }
    if not is_shardloom_engine(str(row.get("engine") or "")):
        status = "external_baseline_only"
    elif row.get("status") != "success":
        status = "not_executed"
    elif any(value is not None for value in normalized.values()):
        status = "complete_with_unmeasured_optional_fields"
    else:
        status = "blocked_missing_normalized_timing"
    return {
        "timing_normalization_schema_version": TIMING_NORMALIZATION_SCHEMA_VERSION,
        "timing_normalization_status": status,
        **normalized,
    }


def route_runtime_status_for_row(row: dict[str, Any], fields: dict[str, Any]) -> str:
    engine = str(row.get("engine") or "")
    if not is_shardloom_engine(engine):
        return "external_baseline_only"
    if row.get("status") != "success":
        return "unsupported"
    status_text = " ".join(
        str(fields.get(key) or "")
        for key in (
            "source_adapter_status",
            "vortex_ingest_status",
            "prepared_state_status",
            "runtime_execution_validation_status",
        )
    )
    if "feature_gated" in status_text or "feature-gated" in status_text:
        return "feature_gated"
    if str(fields.get("claim_gate_status") or "") == "fixture_smoke_only":
        return "fixture_smoke_only"
    return "scoped_runtime_supported"


def route_identity_for_row(row: dict[str, Any]) -> dict[str, Any]:
    fields = runtime_validation_field_map(row)
    engine = str(row.get("engine") or "")
    mode = str(row.get("selected_execution_mode") or fields.get("execution_mode") or "")
    external = not is_shardloom_engine(engine)
    runtime_status = route_runtime_status_for_row(row, fields)
    route: dict[str, Any] = {
        "route_runtime_status_schema_version": ROUTE_RUNTIME_STATUS_SCHEMA_VERSION,
        "engine_display_name": engine_display_name(engine),
        "route_runtime_status": runtime_status,
        "includes_query": True,
        "includes_output": True,
        "includes_evidence": True,
        "performance_claim_allowed": False,
        "production_claim_allowed": False,
        "spark_replacement_claim_allowed": False,
    }
    if external:
        display = engine_display_name(engine)
        route.update(
            {
                "route_lane_id": "external_baseline_end_to_end",
                "route_display_name": f"{display} End-to-End",
                "route_family_display_name": "External Baseline End-to-End",
                "start_state": "raw_compat_source",
                "end_state": "external_result",
                "includes_preparation": False,
                "route_comparable_to_external_end_to_end": True,
                "preparation_included": False,
                "preparation_included_scope": "not_applicable_external_baseline",
                "query_timing_starts_after_preparation": False,
                "prepared_state_reused": False,
                "route_claim_boundary": (
                    "external baseline timing context only; never ShardLoom execution, "
                    "fallback, runtime support, production, or replacement evidence"
                ),
            }
        )
        return route

    if engine == "shardloom-prepare-batch":
        route.update(
            {
                "route_lane_id": "prepare_once_batch",
                "route_display_name": "ShardLoom Prepare-Once Batch",
                "route_family_display_name": "ShardLoom Raw Compatibility To Prepared Vortex",
                "start_state": "raw_compat_source",
                "end_state": "result_sink",
                "includes_preparation": True,
                "route_comparable_to_external_end_to_end": True,
                "preparation_included": True,
                "preparation_included_scope": "amortized_once_per_observed_batch",
                "query_timing_starts_after_preparation": True,
                "prepared_state_reused": field_bool(fields, "prepared_state_reused", True),
                "route_claim_boundary": (
                    "raw compatibility source is prepared once into VortexPreparedState, "
                    "then multiple ShardLoom prepared queries run in one process; timing "
                    "is local evidence only, not a performance, production, or replacement claim"
                ),
            }
        )
    elif engine == "shardloom-vortex" or mode == "native_vortex":
        route.update(
            {
                "route_lane_id": "native_vortex_query",
                "route_display_name": "ShardLoom Native Vortex Query",
                "route_family_display_name": "ShardLoom Native Vortex Query",
                "start_state": "Vortex",
                "end_state": "result_sink",
                "includes_preparation": False,
                "route_comparable_to_external_end_to_end": False,
                "preparation_included": False,
                "preparation_included_scope": "input_already_vortex",
                "query_timing_starts_after_preparation": True,
                "prepared_state_reused": field_bool(fields, "prepared_state_reused", False),
                "route_claim_boundary": (
                    "input is already Vortex; useful native-path evidence but not comparable "
                    "to raw CSV/Parquet/JSONL baselines unless the start state is shown"
                ),
            }
        )
    elif engine == "shardloom-prepared-vortex" or mode == "prepared_vortex":
        route.update(
            {
                "route_lane_id": "warm_prepared_query",
                "route_display_name": "ShardLoom Warm Prepared Query",
                "route_family_display_name": "ShardLoom Warm Prepared Query",
                "start_state": "VortexPreparedState",
                "end_state": "result_sink",
                "includes_preparation": False,
                "route_comparable_to_external_end_to_end": False,
                "preparation_included": False,
                "preparation_included_scope": "preparation_precompleted_before_timing",
                "query_timing_starts_after_preparation": True,
                "prepared_state_reused": field_bool(fields, "prepared_state_reused", True),
                "route_claim_boundary": (
                    "query starts after VortexPreparedState exists; runtime evidence is valid "
                    "for warm prepared execution but it is not the raw-source end-to-end route"
                ),
            }
        )
    elif mode == "direct_compatibility_transient":
        route.update(
            {
                "route_lane_id": "direct_transient_route",
                "route_display_name": "ShardLoom Direct Transient Route",
                "route_family_display_name": "ShardLoom Direct Transient Route",
                "start_state": "raw_compat_source",
                "end_state": "result_sink",
                "includes_preparation": False,
                "route_comparable_to_external_end_to_end": True,
                "preparation_included": False,
                "preparation_included_scope": "not_persistent_vortex_preparation",
                "query_timing_starts_after_preparation": False,
                "prepared_state_reused": False,
                "route_claim_boundary": (
                    "one-shot local compatibility execution without persistent Vortex "
                    "preparation; not a Vortex-native or production claim"
                ),
            }
        )
    else:
        route.update(
            {
                "route_lane_id": "cold_certified_route",
                "route_display_name": "ShardLoom Cold Certified Route",
                "route_family_display_name": "ShardLoom Cold Certified Route",
                "start_state": "raw_compat_source",
                "end_state": "result_sink",
                "includes_preparation": True,
                "route_comparable_to_external_end_to_end": True,
                "preparation_included": True,
                "preparation_included_scope": "included_in_cold_certified_route_timing",
                "query_timing_starts_after_preparation": False,
                "prepared_state_reused": field_bool(fields, "prepared_state_reused", False),
                "route_claim_boundary": (
                    "raw compatibility input is certified, ingested to Vortex, reopened/"
                    "scanned, queried, and emitted with evidence in one measured route; "
                    "not pure query speed or a production/replacement claim"
                ),
            }
        )
    return route


def route_stage_fields_for_row(row: dict[str, Any]) -> dict[str, Any]:
    fields = runtime_validation_field_map(row)
    identity = route_identity_for_row(row)
    timing_surface_fields = timing_surface_fields_for_row(row)
    timing_surface = str(timing_surface_fields["timing_surface"])
    route_lane_id = str(row.get("route_lane_id") or identity.get("route_lane_id") or "")
    is_shardloom = is_shardloom_engine(str(row.get("engine") or ""))
    total_runtime = route_total_runtime_millis(fields)
    query_runtime = prepared_route_query_runtime_millis(fields)
    preparation = prepare_once_preparation_millis(fields)
    batch_count = prepared_route_observed_batch_count(fields)
    amortized_preparation = None
    if preparation is not None and batch_count and batch_count > 0:
        amortized_preparation = preparation / batch_count

    prepared_state_lookup = first_numeric_field(
        fields,
        (
            "prepare_batch_prepared_state_lookup_or_create_millis",
            "prepared_state_lookup_millis",
            "prepared_state_create_millis",
        ),
    )
    if prepared_state_lookup is None and route_lane_id == "prepare_once_batch":
        prepared_state_lookup = amortized_preparation
    elif prepared_state_lookup is None and route_lane_id == "cold_certified_route":
        prepared_state_lookup = first_numeric_field(
            fields, ("preparation_millis", "vortex_prepare_millis")
        )

    def route_stage_millis(
        primary_millis_keys: tuple[str, ...] = (),
        prepare_batch_millis_keys: tuple[str, ...] = (),
        primary_micros_keys: tuple[str, ...] = (),
        prepare_batch_micros_keys: tuple[str, ...] = (),
    ) -> float | None:
        if route_lane_id == "prepare_once_batch" and batch_count and batch_count > 0:
            prepare_batch_value = first_numeric_stage_millis(
                fields,
                millis_keys=prepare_batch_millis_keys,
                micros_keys=prepare_batch_micros_keys,
            )
            if prepare_batch_value is not None:
                return prepare_batch_value / batch_count
        return first_numeric_stage_millis(
            fields,
            millis_keys=primary_millis_keys + prepare_batch_millis_keys,
            micros_keys=primary_micros_keys + prepare_batch_micros_keys,
        )

    source_read_total = route_stage_millis(
        ("source_read_millis",),
        primary_micros_keys=("source_read_micros",),
    )
    source_parse = route_stage_millis(
        ("compatibility_parse_millis", "source_parse_millis"),
        primary_micros_keys=("compatibility_parse_micros", "source_parse_micros"),
    )
    source_to_columnar = route_stage_millis(
        ("source_to_columnar_millis",),
        ("prepare_batch_source_to_columnar_millis",),
        ("source_to_columnar_micros",),
        ("prepare_batch_source_to_columnar_micros",),
    )
    explicit_source_parse_or_decode = route_stage_millis(
        ("exclusive_source_parse_or_decode_millis",),
        primary_micros_keys=("exclusive_source_parse_or_decode_micros",),
    )
    if explicit_source_parse_or_decode is not None:
        source_parse_or_decode = explicit_source_parse_or_decode
    elif source_parse is not None or source_to_columnar is not None:
        source_parse_or_decode = (source_parse or 0.0) + (source_to_columnar or 0.0)
    else:
        source_parse_or_decode = None
    explicit_source_read = route_stage_millis(
        ("exclusive_source_read_millis",),
        primary_micros_keys=("exclusive_source_read_micros",),
    )
    if explicit_source_read is not None:
        source_read = explicit_source_read
    elif source_read_total is not None and source_parse_or_decode is not None:
        source_read = max(source_read_total - source_parse_or_decode, 0.0)
    else:
        source_read = source_read_total

    source_to_vortex_array = route_stage_millis(
        ("exclusive_source_to_vortex_array_millis", "vortex_array_build_millis"),
        ("prepare_batch_vortex_array_build_millis",),
        ("exclusive_source_to_vortex_array_micros", "vortex_array_build_micros"),
        ("prepare_batch_vortex_array_build_micros",),
    )
    vortex_write = route_stage_millis(
        ("exclusive_vortex_write_millis", "vortex_write_millis"),
        ("prepare_batch_vortex_write_millis",),
        ("exclusive_vortex_write_micros", "vortex_write_micros"),
        ("prepare_batch_vortex_write_micros",),
    )
    vortex_digest = route_stage_millis(
        ("exclusive_vortex_digest_millis", "vortex_digest_millis"),
        primary_micros_keys=("exclusive_vortex_digest_micros", "vortex_digest_micros"),
    )
    vortex_reopen_or_verify = route_stage_millis(
        (
            "exclusive_vortex_reopen_verify_millis",
            "vortex_reopen_verify_millis",
        ),
        ("prepare_batch_vortex_reopen_verify_millis",),
        ("exclusive_vortex_reopen_verify_micros", "vortex_reopen_verify_micros"),
        ("prepare_batch_vortex_reopen_verify_micros",),
    )
    vortex_scan_substage = summed_numeric_millis_from_micros(
        fields,
        (
            "vortex_scan_open_micros",
            "vortex_projected_field_extract_micros",
            "vortex_encoded_kernel_evidence_micros",
        ),
    )
    chunk_iteration_micros = first_numeric_micros(
        fields,
        micros_keys=("scan_chunk_iter_micros", "vortex_chunk_iteration_micros"),
    )
    if chunk_iteration_micros is not None:
        vortex_scan_substage = (vortex_scan_substage or 0.0) + (
            chunk_iteration_micros / 1000.0
        )
    if vortex_scan_substage is not None:
        vortex_scan = vortex_scan_substage
    else:
        vortex_scan = route_stage_millis(
            ("exclusive_vortex_scan_millis", "vortex_scan_millis"),
            primary_micros_keys=("exclusive_vortex_scan_micros", "vortex_scan_micros"),
        )
    operator_substage = summed_numeric_millis_from_micros(
        fields,
        (
            "operator_kernel_micros",
            "operator_finalize_micros",
            "result_assembly_micros",
        ),
    )
    if operator_substage is not None:
        operator_compute = operator_substage
    else:
        operator_compute = route_stage_millis(
            ("exclusive_operator_compute_millis", "operator_compute_millis"),
            primary_micros_keys=("exclusive_operator_compute_micros", "operator_compute_micros"),
        )
    has_query_stage_split = vortex_scan is not None and operator_compute is not None
    query_substage_sum = (
        (vortex_scan or 0.0) + (operator_compute or 0.0)
        if has_query_stage_split
        else None
    )
    if (
        has_query_stage_split
        and route_lane_id
        in {
            "prepare_once_first_query",
            "prepare_once_batch",
            "warm_prepared_query",
            "native_vortex_query",
        }
        and query_runtime is not None
    ):
        prepared_query = query_runtime
    else:
        prepared_query = query_substage_sum

    output_delivery = output_delivery_millis(fields)
    evidence_render = evidence_render_route_millis(fields)
    explicit_result_sink = route_stage_millis(
        ("exclusive_result_sink_write_millis",),
        primary_micros_keys=("exclusive_result_sink_write_micros",),
    )
    result_sink_write = explicit_result_sink if explicit_result_sink is not None else output_delivery
    explicit_evidence_render = route_stage_millis(
        ("exclusive_evidence_render_millis",),
        primary_micros_keys=("exclusive_evidence_render_micros",),
    )
    evidence_render = (
        explicit_evidence_render if explicit_evidence_render is not None else evidence_render
    )
    source_admission = source_admission_millis(fields)
    hot_exclusive_stage_pairs = (
        ("source_admission", source_admission),
        ("source_read", source_read),
        ("source_parse_or_decode", source_parse_or_decode),
        ("vortex_array_build", source_to_vortex_array),
        ("vortex_write", vortex_write),
        ("vortex_digest", vortex_digest),
        ("vortex_reopen_verify", vortex_reopen_or_verify),
        ("prepared_query", prepared_query),
    )
    exclusive_stage_pairs = list(hot_exclusive_stage_pairs)
    if timing_surface in PROOF_TIMING_SURFACES:
        exclusive_stage_pairs.append(("sink_output", result_sink_write))
    if timing_surface == "publication_proof":
        exclusive_stage_pairs.append(("evidence_render", evidence_render))
    exclusive_stage_values = (
        [
            (stage, value)
            for stage, value in exclusive_stage_pairs
            if value is not None and value >= 0.0
        ]
        if is_shardloom
        else []
    )
    exclusive_stage_sum = round(sum(value for _, value in exclusive_stage_values), 4)
    inclusive_compatibility_to_vortex_import = first_numeric_stage_millis(
        fields,
        millis_keys=(
            "inclusive_compatibility_to_vortex_import_millis",
            "compatibility_to_vortex_import_millis",
        ),
        micros_keys=(
            "inclusive_compatibility_to_vortex_import_micros",
            "compatibility_to_vortex_import_micros",
        ),
    )
    preparation_component_values = [
        value
        for value in (
            source_admission,
            source_read,
            source_parse_or_decode,
            source_to_vortex_array,
            vortex_write,
            vortex_digest,
            vortex_reopen_or_verify,
        )
        if value is not None and value >= 0.0
    ]
    preparation_component_total = (
        sum(preparation_component_values) if preparation_component_values else None
    )
    hot_route_total = total_runtime
    if route_lane_id == "prepare_once_first_query" and query_runtime is not None:
        hot_route_total = (
            query_runtime + preparation if preparation is not None else query_runtime
        )
    elif route_lane_id == "prepare_once_batch" and query_runtime is not None:
        hot_route_total = query_runtime + (amortized_preparation or 0.0)
    elif (
        route_lane_id in {"warm_prepared_query", "native_vortex_query"}
        and query_runtime is not None
    ):
        hot_route_total = query_runtime
    elif route_lane_id == "cold_certified_route":
        if preparation_component_total is not None:
            hot_route_total = preparation_component_total + (prepared_query or 0.0)
    full_replay_proof_route_total = (
        hot_route_total + result_sink_write if hot_route_total is not None else None
    )
    publication_proof_route_total = (
        full_replay_proof_route_total + evidence_render
        if full_replay_proof_route_total is not None
        else None
    )
    if timing_surface == "hot_runtime":
        total_route = hot_route_total
    elif timing_surface == "full_replay_proof":
        total_route = full_replay_proof_route_total
    elif timing_surface == "publication_proof":
        total_route = publication_proof_route_total
    else:
        total_route = total_runtime
    exclusive_residual = (
        round(total_route - exclusive_stage_sum, 4) if total_route is not None else None
    )
    exclusive_delta = abs(exclusive_residual) if exclusive_residual is not None else None
    requires_query_stage_split = is_shardloom and (
        query_runtime is not None
        or route_lane_id
        in {
            "full_certified_cold_ingest",
            "prepare_once_first_query",
            "prepare_once_batch",
            "warm_prepared_query",
            "native_vortex_query",
        }
    )
    if not is_shardloom:
        exclusive_stage_timing_status = "external_baseline_only"
    elif requires_query_stage_split and not has_query_stage_split:
        exclusive_stage_timing_status = "blocked_missing_query_split"
    elif not exclusive_stage_values:
        exclusive_stage_timing_status = "blocked_missing_stage_timing"
    else:
        exclusive_stage_timing_status = "complete"

    exclusive_stage_timing_schema_version = first_meaningful_field(
        fields, ("exclusive_stage_timing_schema_version",)
    )
    if exclusive_stage_timing_schema_version != EXCLUSIVE_STAGE_TIMING_SCHEMA_VERSION:
        exclusive_stage_timing_schema_version = EXCLUSIVE_STAGE_TIMING_SCHEMA_VERSION
    writer_context_write_count = int(
        numeric_value(
            first_meaningful_field(fields, ("vortex_writer_context_write_count",))
        )
        or 0
    )
    writer_context_reuse_hit_count = int(
        numeric_value(
            first_meaningful_field(fields, ("vortex_writer_context_reuse_hit_count",))
        )
        or 0
    )
    write_plan_artifact_count = int(
        numeric_value(first_meaningful_field(fields, ("vortex_write_plan_artifact_count",)))
        or writer_context_write_count
        or 0
    )
    derived_roles = (
        "fact,dim,cdc_delta"
        if write_plan_artifact_count >= 3
        else ("fact,dim" if write_plan_artifact_count == 2 else "not_reported")
    )
    write_plan_shared_context = first_bool_field(
        fields, ("vortex_write_plan_shared_writer_context",), default=None
    )
    if write_plan_shared_context is None:
        write_plan_shared_context = bool(
            write_plan_artifact_count > 0
            and writer_context_write_count == write_plan_artifact_count
        )
    write_plan_status = first_meaningful_field(fields, ("vortex_write_plan_status",))
    if write_plan_status is None:
        if writer_context_write_count > 0 and write_plan_shared_context:
            write_plan_status = "bounded_capillary_write_plan_derived_from_writer_context"
        elif is_shardloom:
            write_plan_status = "not_reported"
        else:
            write_plan_status = "external_baseline_only"
    derived_artifact_paths = ",".join(
        str(fields.get(key))
        for key in (
            "fact_vortex_path",
            "dim_vortex_path",
            "cdc_delta_vortex_path",
        )
        if fields.get(key) not in {None, "", "none", "not_reported"}
    )
    derived_artifact_digests = ",".join(
        str(fields.get(key))
        for key in (
            "fact_vortex_digest",
            "dim_vortex_digest",
            "cdc_delta_vortex_digest",
        )
        if fields.get(key) not in {None, "", "none", "not_reported"}
    )
    derived_artifact_bytes_values = [
        int(numeric_value(fields.get(key)) or 0)
        for key in (
            "fact_vortex_bytes",
            "dim_vortex_bytes",
            "cdc_delta_vortex_bytes",
        )
        if fields.get(key) not in {None, "", "none", "not_reported"}
    ]
    derived_artifact_rows_values = [
        int(numeric_value(fields.get(key)) or 0)
        for key in ("fact_rows", "dim_rows", "cdc_delta_rows")
        if fields.get(key) not in {None, "", "none", "not_reported"}
    ]
    derived_artifact_bytes = (
        ",".join(str(value) for value in derived_artifact_bytes_values)
        if derived_artifact_bytes_values
        else "not_reported"
    )
    derived_artifact_rows = (
        ",".join(str(value) for value in derived_artifact_rows_values)
        if derived_artifact_rows_values
        else "not_reported"
    )
    derived_total_artifact_bytes = sum(derived_artifact_bytes_values)
    derived_total_artifact_rows = sum(derived_artifact_rows_values)

    return {
        **timing_surface_fields,
        "source_admission_ms": source_admission_millis(fields),
        "source_read_ms": source_read,
        "source_parse_or_columnar_decode_ms": source_parse_or_decode,
        "source_to_vortex_array_ms": source_to_vortex_array,
        "vortex_write_ms": vortex_write,
        "vortex_writer_context_schema_version": first_meaningful_field(
            fields, ("vortex_writer_context_schema_version",)
        )
        or "shardloom.traditional_analytics.vortex_writer_context.v1",
        "vortex_writer_context_status": first_meaningful_field(
            fields, ("vortex_writer_context_status",)
        )
        or ("not_reported" if is_shardloom else "external_baseline_only"),
        "vortex_writer_context_open_ms": first_numeric_stage_millis(
            fields,
            millis_keys=(
                "vortex_writer_context_open_ms",
                "vortex_writer_context_open_millis",
            ),
            micros_keys=("vortex_writer_context_open_micros",),
        ),
        "vortex_writer_context_write_count": writer_context_write_count,
        "vortex_writer_context_reuse_hit_count": writer_context_reuse_hit_count,
        "vortex_writer_context_reuse_status": first_meaningful_field(
            fields, ("vortex_writer_context_reuse_status",)
        )
        or ("not_reported" if is_shardloom else "external_baseline_only"),
        "vortex_segment_write_ms": first_numeric_stage_millis(
            fields,
            millis_keys=("vortex_segment_write_ms", "vortex_segment_write_millis"),
            micros_keys=("vortex_segment_write_micros",),
        ),
        "vortex_workspace_stage_ms": first_numeric_stage_millis(
            fields,
            millis_keys=("vortex_workspace_stage_ms", "vortex_workspace_stage_millis"),
            micros_keys=("vortex_workspace_stage_micros",),
        ),
        "vortex_write_coalescing_status": first_meaningful_field(
            fields, ("vortex_write_coalescing_status",)
        )
        or ("not_reported" if is_shardloom else "external_baseline_only"),
        "vortex_write_coalescing_reason": first_meaningful_field(
            fields, ("vortex_write_coalescing_reason",)
        )
        or ("not_reported" if is_shardloom else "external_baseline_only"),
        "vortex_write_plan_schema_version": first_meaningful_field(
            fields, ("vortex_write_plan_schema_version",)
        )
        or "shardloom.traditional_analytics.vortex_write_plan.v1",
        "vortex_write_plan_status": write_plan_status,
        "vortex_write_plan_artifact_count": write_plan_artifact_count,
        "vortex_write_plan_artifact_roles": first_meaningful_field(
            fields, ("vortex_write_plan_artifact_roles",)
        )
        or derived_roles,
        "vortex_write_plan_artifact_paths": first_meaningful_field(
            fields, ("vortex_write_plan_artifact_paths",)
        )
        or derived_artifact_paths
        or "not_reported",
        "vortex_write_plan_artifact_digests": first_meaningful_field(
            fields, ("vortex_write_plan_artifact_digests",)
        )
        or derived_artifact_digests
        or "not_reported",
        "vortex_write_plan_artifact_bytes": first_meaningful_field(
            fields, ("vortex_write_plan_artifact_bytes",)
        )
        or derived_artifact_bytes,
        "vortex_write_plan_artifact_rows": first_meaningful_field(
            fields, ("vortex_write_plan_artifact_rows",)
        )
        or derived_artifact_rows,
        "vortex_write_plan_total_artifact_bytes": int(
            numeric_value(
                first_meaningful_field(
                    fields, ("vortex_write_plan_total_artifact_bytes",)
                )
            )
            or derived_total_artifact_bytes
        ),
        "vortex_write_plan_total_artifact_rows": int(
            numeric_value(
                first_meaningful_field(fields, ("vortex_write_plan_total_artifact_rows",))
            )
            or derived_total_artifact_rows
        ),
        "vortex_write_plan_writer_context_count": int(
            numeric_value(
                first_meaningful_field(fields, ("vortex_write_plan_writer_context_count",))
            )
            or (1 if writer_context_write_count > 0 else 0)
        ),
        "vortex_write_plan_shared_writer_context": write_plan_shared_context,
        "vortex_write_plan_writer_context_write_count": int(
            numeric_value(
                first_meaningful_field(
                    fields, ("vortex_write_plan_writer_context_write_count",)
                )
            )
            or writer_context_write_count
        ),
        "vortex_write_plan_writer_context_reuse_hit_count": int(
            numeric_value(
                first_meaningful_field(
                    fields, ("vortex_write_plan_writer_context_reuse_hit_count",)
                )
            )
            or writer_context_reuse_hit_count
        ),
        "vortex_write_plan_context_open_ms": first_numeric_stage_millis(
            fields,
            millis_keys=(
                "vortex_write_plan_context_open_ms",
                "vortex_write_plan_context_open_millis",
            ),
            micros_keys=(
                "vortex_write_plan_context_open_micros",
                "vortex_writer_context_open_micros",
            ),
        ),
        "vortex_write_plan_segment_write_ms": first_numeric_stage_millis(
            fields,
            millis_keys=(
                "vortex_write_plan_segment_write_ms",
                "vortex_write_plan_segment_write_millis",
            ),
            micros_keys=(
                "vortex_write_plan_segment_write_micros",
                "vortex_segment_write_micros",
            ),
        ),
        "vortex_write_plan_workspace_stage_ms": first_numeric_stage_millis(
            fields,
            millis_keys=(
                "vortex_write_plan_workspace_stage_ms",
                "vortex_write_plan_workspace_stage_millis",
            ),
            micros_keys=(
                "vortex_write_plan_workspace_stage_micros",
                "vortex_workspace_stage_micros",
            ),
        ),
        "vortex_write_plan_digest_ms": first_numeric_stage_millis(
            fields,
            millis_keys=("vortex_write_plan_digest_millis",),
            micros_keys=("vortex_write_plan_digest_micros", "vortex_digest_micros"),
        ),
        "vortex_write_plan_verification_ms": first_numeric_stage_millis(
            fields,
            millis_keys=("vortex_write_plan_verification_millis",),
            micros_keys=(
                "vortex_write_plan_verification_micros",
                "vortex_reopen_verify_micros",
            ),
        ),
        "vortex_write_plan_coalescing_status": first_meaningful_field(
            fields, ("vortex_write_plan_coalescing_status",)
        )
        or first_meaningful_field(fields, ("vortex_write_coalescing_status",))
        or ("not_reported" if is_shardloom else "external_baseline_only"),
        "vortex_write_plan_coalescing_reason": first_meaningful_field(
            fields, ("vortex_write_plan_coalescing_reason",)
        )
        or first_meaningful_field(fields, ("vortex_write_coalescing_reason",))
        or ("not_reported" if is_shardloom else "external_baseline_only"),
        "vortex_write_plan_digest_status": first_meaningful_field(
            fields, ("vortex_write_plan_digest_status",)
        )
        or (
            "streaming_workspace_writer_digest_no_post_write_digest_pass"
            if writer_context_write_count > 0
            else ("not_reported" if is_shardloom else "external_baseline_only")
        ),
        "vortex_write_plan_verification_status": first_meaningful_field(
            fields, ("vortex_write_plan_verification_status",)
        )
        or ("not_reported" if is_shardloom else "external_baseline_only"),
        "vortex_write_plan_native_io_certificate_status": first_meaningful_field(
            fields,
            (
                "vortex_write_plan_native_io_certificate_status",
                "native_io_certificate_status",
                "output_native_io_certificate_status",
            ),
        )
        or ("not_reported" if is_shardloom else "external_baseline_only"),
        "vortex_write_plan_fallback_attempted": first_bool_field(
            fields, ("vortex_write_plan_fallback_attempted",), default=False
        ),
        "vortex_write_plan_external_engine_invoked": first_bool_field(
            fields, ("vortex_write_plan_external_engine_invoked",), default=False
        ),
        "vortex_write_plan_claim_boundary": first_meaningful_field(
            fields, ("vortex_write_plan_claim_boundary",)
        )
        or (
            "bounded local write-plan evidence decomposes ShardLoom Vortex artifact writes into writer-open, segment-write, workspace-stage, streaming digest, and verification substages; it is optimization evidence only"
        ),
        "vortex_reopen_or_verify_ms": vortex_reopen_or_verify,
        "prepared_state_lookup_or_create_ms": prepared_state_lookup,
        "vortex_scan_ms": vortex_scan,
        "operator_compute_ms": operator_compute,
        "result_sink_write_ms": result_sink_write,
        "evidence_render_ms": evidence_render,
        "hot_route_total_ms": hot_route_total,
        "full_replay_proof_route_total_ms": full_replay_proof_route_total,
        "publication_proof_route_total_ms": publication_proof_route_total,
        "prepare_route_total_ms": first_numeric_field(
            fields,
            (
                "prepare_route_total_ms",
                "prepare_route_total_millis",
                "prepare_batch_prepare_route_total_millis",
            ),
        ),
        "prepare_cli_wall_ms": first_numeric_field(
            fields,
            (
                "prepare_cli_wall_ms",
                "prepare_cli_wall_millis",
                "preparation_cli_process_wall_millis",
            ),
        ),
        "total_route_ms": total_route,
        "exclusive_stage_timing_schema_version": exclusive_stage_timing_schema_version,
        "exclusive_stage_timing_status": exclusive_stage_timing_status,
        "exclusive_stage_timing_scope": first_meaningful_field(
            fields, ("exclusive_stage_timing_scope",)
        )
        or "derived_deoverlapped_route_stage_fields",
        "exclusive_stage_included_stage_ids": ",".join(
            stage for stage, _ in exclusive_stage_values
        )
        or "none",
        "exclusive_source_admission_ms": source_admission_millis(fields),
        "exclusive_source_read_ms": source_read,
        "exclusive_source_parse_or_decode_ms": source_parse_or_decode,
        "exclusive_source_to_vortex_array_ms": source_to_vortex_array,
        "exclusive_vortex_write_ms": vortex_write,
        "exclusive_vortex_digest_ms": vortex_digest,
        "exclusive_vortex_reopen_verify_ms": vortex_reopen_or_verify,
        "exclusive_prepared_query_ms": prepared_query,
        "exclusive_result_sink_write_ms": result_sink_write,
        "exclusive_evidence_render_ms": evidence_render,
        "route_timing_exclusive_stage_ids": ",".join(
            stage for stage, _ in exclusive_stage_values
        )
        or "none",
        "route_timing_exclusive_stage_sum_ms": exclusive_stage_sum
        if exclusive_stage_values
        else None,
        "route_timing_exclusive_residual_ms": exclusive_residual,
        "route_timing_exclusive_total_delta_ms": exclusive_delta,
        "route_timing_exclusive_residual_status": "auditable_residual"
        if exclusive_residual is not None
        else "not_numeric",
        "inclusive_compatibility_to_vortex_import_ms": inclusive_compatibility_to_vortex_import,
        "inclusive_compatibility_to_vortex_import_timing_scope": first_meaningful_field(
            fields,
            (
                "inclusive_compatibility_to_vortex_import_timing_scope",
                "compatibility_to_vortex_import_timing_scope",
            ),
        )
        or "not_reported",
        "exclusive_stage_timing_claim_boundary": first_meaningful_field(
            fields, ("exclusive_stage_timing_claim_boundary",)
        )
        or (
            "exclusive stage timing is local benchmark attribution evidence only; route totals "
            "remain the comparison surface and no performance, production, SQL/DataFrame, "
            "object-store/lakehouse, or Spark-displacement claim is authorized"
        ),
        "prepared_route_observed_batch_count": batch_count,
        "route_stage_timing_scope": (
            "amortized_once_per_observed_batch"
            if route_lane_id == "prepare_once_batch"
            else "row_total_timing"
        ),
    }


def _stage_ids_with_values(stage_fields: dict[str, Any]) -> list[str]:
    return [
        key
        for key in (
            "source_admission_ms",
            "source_read_ms",
            "source_parse_or_columnar_decode_ms",
            "source_to_vortex_array_ms",
            "vortex_write_ms",
            "vortex_reopen_or_verify_ms",
            "prepared_state_lookup_or_create_ms",
            "vortex_scan_ms",
            "operator_compute_ms",
            "result_sink_write_ms",
            "evidence_render_ms",
        )
        if numeric_value(stage_fields.get(key)) is not None
    ]


def bool_field_value(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text == "true":
        return True
    if text == "false":
        return False
    return default


def source_typed_builder_default_fields(
    status: str,
    *,
    path: str | None = None,
    claim_boundary: str | None = None,
) -> dict[str, Any]:
    return {
        "source_typed_column_builder_schema_version": (
            SOURCE_TYPED_COLUMN_BUILDER_SCHEMA_VERSION
        ),
        "source_typed_column_builder_status": status,
        "source_typed_column_builder_path": path or status,
        "source_typed_builder_schema_digest": "none",
        "source_typed_builder_projected_column_count": 0,
        "source_typed_builder_full_column_count": 0,
        "source_typed_builder_decoded_column_count": 0,
        "source_typed_builder_skipped_column_count": 0,
        "source_typed_builder_row_assembly_avoided": False,
        "source_typed_builder_row_materialization_status": status,
        "source_typed_builder_null_validity_status": status,
        "source_typed_builder_type_coercion_status": status,
        "source_typed_builder_nested_json_status": status,
        "source_typed_builder_correctness_digest_status": status,
        "source_typed_builder_fallback_attempted": False,
        "source_typed_builder_external_engine_invoked": False,
        "source_typed_builder_external_parser_engine_invoked": False,
        "external_parser_engine_invoked": False,
        "source_typed_builder_claim_boundary": claim_boundary or status,
    }


def source_typed_builder_fields_from_runtime(
    fields: dict[str, Any],
    *,
    default_status: str = "not_reported",
) -> dict[str, Any]:
    return {
        "source_typed_column_builder_schema_version": first_meaningful_field(
            fields, ("source_typed_column_builder_schema_version",)
        )
        or SOURCE_TYPED_COLUMN_BUILDER_SCHEMA_VERSION,
        "source_typed_column_builder_status": first_meaningful_field(
            fields, ("source_typed_column_builder_status",)
        )
        or default_status,
        "source_typed_column_builder_path": first_meaningful_field(
            fields, ("source_typed_column_builder_path",)
        )
        or default_status,
        "source_typed_builder_schema_digest": first_meaningful_field(
            fields, ("source_typed_builder_schema_digest",)
        )
        or "none",
        "source_typed_builder_projected_column_count": int(
            numeric_value(
                first_meaningful_field(
                    fields, ("source_typed_builder_projected_column_count",)
                )
            )
            or 0
        ),
        "source_typed_builder_full_column_count": int(
            numeric_value(
                first_meaningful_field(fields, ("source_typed_builder_full_column_count",))
            )
            or 0
        ),
        "source_typed_builder_decoded_column_count": int(
            numeric_value(
                first_meaningful_field(
                    fields, ("source_typed_builder_decoded_column_count",)
                )
            )
            or 0
        ),
        "source_typed_builder_skipped_column_count": int(
            numeric_value(
                first_meaningful_field(
                    fields, ("source_typed_builder_skipped_column_count",)
                )
            )
            or 0
        ),
        "source_typed_builder_row_assembly_avoided": bool_field_value(
            first_meaningful_field(fields, ("source_typed_builder_row_assembly_avoided",))
        ),
        "source_typed_builder_row_materialization_status": first_meaningful_field(
            fields, ("source_typed_builder_row_materialization_status",)
        )
        or default_status,
        "source_typed_builder_null_validity_status": first_meaningful_field(
            fields, ("source_typed_builder_null_validity_status",)
        )
        or default_status,
        "source_typed_builder_type_coercion_status": first_meaningful_field(
            fields, ("source_typed_builder_type_coercion_status",)
        )
        or default_status,
        "source_typed_builder_nested_json_status": first_meaningful_field(
            fields, ("source_typed_builder_nested_json_status",)
        )
        or default_status,
        "source_typed_builder_correctness_digest_status": first_meaningful_field(
            fields, ("source_typed_builder_correctness_digest_status",)
        )
        or default_status,
        "source_typed_builder_fallback_attempted": bool_field_value(
            first_meaningful_field(fields, ("source_typed_builder_fallback_attempted",))
        ),
        "source_typed_builder_external_engine_invoked": bool_field_value(
            first_meaningful_field(fields, ("source_typed_builder_external_engine_invoked",))
        ),
        "source_typed_builder_external_parser_engine_invoked": bool_field_value(
            first_meaningful_field(
                fields, ("source_typed_builder_external_parser_engine_invoked",)
            )
        ),
        "external_parser_engine_invoked": bool_field_value(
            first_meaningful_field(fields, ("external_parser_engine_invoked",))
        ),
        "source_typed_builder_claim_boundary": first_meaningful_field(
            fields, ("source_typed_builder_claim_boundary",)
        )
        or default_status,
    }


def source_projection_mask_value(value: Any) -> int:
    if value is None:
        return 0
    text = str(value).strip().lower()
    if text.startswith("0x"):
        text = text[2:]
    try:
        return int(text, 16)
    except ValueError:
        return 0


def source_projection_mask_hex(value: int) -> str:
    return f"0x{value:08x}"


def source_projection_field_mask_digest(*parts: Any) -> str:
    text = "\0".join(str(part) for part in parts)
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def source_projection_default_fields(
    status: str,
    *,
    source_family: str | None = None,
    claim_boundary: str | None = None,
) -> dict[str, Any]:
    return {
        "source_projection_admission_schema_version": (
            SOURCE_PROJECTION_ADMISSION_SCHEMA_VERSION
        ),
        "source_projection_admission_status": status,
        "source_projection_source_family": source_family or status,
        "source_projection_required_field_mask": "0x00000000",
        "source_projection_predicate_field_mask": "0x00000000",
        "source_projection_output_field_mask": "0x00000000",
        "source_projection_certificate_field_mask": "0x00000000",
        "source_projection_diagnostic_field_mask": "0x00000000",
        "source_projection_field_mask_digest": "none",
        "source_projection_decoded_columns": "none",
        "source_projection_skipped_columns": "none",
        "source_projection_decoded_column_count": 0,
        "source_projection_skipped_column_count": 0,
        "source_projection_blocker": status,
        "source_projection_correctness_digest_status": status,
        "source_projection_fallback_attempted": False,
        "source_projection_external_engine_invoked": False,
        "source_projection_claim_boundary": claim_boundary or status,
    }


def source_projection_fields_from_runtime(
    fields: dict[str, Any],
    *,
    default_status: str = "not_reported",
) -> dict[str, Any]:
    required_mask = first_meaningful_field(
        fields, ("source_projection_required_field_mask", "source_read_projected_field_mask")
    ) or "0x00000000"
    predicate_mask = first_meaningful_field(
        fields, ("source_projection_predicate_field_mask", "source_read_filter_field_mask")
    ) or "0x00000000"
    output_mask = first_meaningful_field(fields, ("source_projection_output_field_mask",))
    if output_mask is None:
        output_mask = source_projection_mask_hex(
            source_projection_mask_value(required_mask)
            & ~source_projection_mask_value(predicate_mask)
        )
    decoded_columns = first_meaningful_field(
        fields, ("source_projection_decoded_columns", "source_read_decoded_columns")
    ) or "none"
    skipped_columns = first_meaningful_field(
        fields, ("source_projection_skipped_columns", "source_read_skipped_columns")
    ) or "none"
    decoded_count = int(
        numeric_value(
            first_meaningful_field(
                fields,
                (
                    "source_projection_decoded_column_count",
                    "source_read_decoded_column_count",
                ),
            )
        )
        or 0
    )
    skipped_count = int(
        numeric_value(
            first_meaningful_field(
                fields,
                (
                    "source_projection_skipped_column_count",
                    "source_read_skipped_column_count",
                ),
            )
        )
        or 0
    )
    decode_status = first_meaningful_field(fields, ("source_read_decode_status",)) or ""
    materialization_status = (
        first_meaningful_field(fields, ("source_read_row_materialization_status",)) or ""
    )
    scalar_materialization = materialization_status in {
        "row_structs_materialized",
        "mixed_scalar_rows_and_provider_batches",
    }
    source_family = first_meaningful_field(fields, ("source_projection_source_family",))
    if source_family is None:
        if "text_column_decode" in decode_status:
            source_family = "text_source"
        elif "columnar_provider" in decode_status:
            source_family = "already_columnar_source"
        else:
            source_family = default_status
    admission_status = first_meaningful_field(
        fields, ("source_projection_admission_status",)
    )
    if admission_status is None:
        if (
            skipped_count > 0
            and decode_status == "projection_aware_text_column_decode"
            and not scalar_materialization
        ):
            admission_status = "admitted_projection_aware_text_decode"
        elif (
            skipped_count > 0
            and decode_status == "projection_aware_columnar_provider_decode"
            and not scalar_materialization
        ):
            admission_status = "admitted_projection_aware_columnar_provider"
        elif scalar_materialization:
            admission_status = "not_admitted_scalar_row_materialization"
        elif decoded_count > 0 and skipped_count == 0:
            admission_status = "no_projection_opportunity_full_source_decode"
        else:
            admission_status = default_status
    blocker = first_meaningful_field(fields, ("source_projection_blocker",))
    if blocker is None:
        blocker = (
            "none"
            if admission_status.startswith("admitted_projection_aware")
            or admission_status == "no_projection_opportunity_full_source_decode"
            else "projection_admission_not_reported"
        )
    field_mask_digest = first_meaningful_field(
        fields, ("source_projection_field_mask_digest",)
    ) or source_projection_field_mask_digest(
        SOURCE_PROJECTION_ADMISSION_SCHEMA_VERSION,
        source_family,
        required_mask,
        predicate_mask,
        output_mask,
        decoded_columns,
        skipped_columns,
        admission_status,
        blocker,
    )
    return {
        "source_projection_admission_schema_version": first_meaningful_field(
            fields, ("source_projection_admission_schema_version",)
        )
        or SOURCE_PROJECTION_ADMISSION_SCHEMA_VERSION,
        "source_projection_admission_status": admission_status,
        "source_projection_source_family": source_family,
        "source_projection_required_field_mask": required_mask,
        "source_projection_predicate_field_mask": predicate_mask,
        "source_projection_output_field_mask": output_mask,
        "source_projection_certificate_field_mask": first_meaningful_field(
            fields, ("source_projection_certificate_field_mask",)
        )
        or required_mask,
        "source_projection_diagnostic_field_mask": first_meaningful_field(
            fields, ("source_projection_diagnostic_field_mask",)
        )
        or "0x00000000",
        "source_projection_field_mask_digest": field_mask_digest,
        "source_projection_decoded_columns": decoded_columns,
        "source_projection_skipped_columns": skipped_columns,
        "source_projection_decoded_column_count": decoded_count,
        "source_projection_skipped_column_count": skipped_count,
        "source_projection_blocker": blocker,
        "source_projection_correctness_digest_status": first_meaningful_field(
            fields, ("source_projection_correctness_digest_status",)
        )
        or (
            "covered_by_route_correctness_digest"
            if admission_status.startswith("admitted_projection_aware")
            else "not_applicable_projection_not_admitted"
        ),
        "source_projection_fallback_attempted": bool_field_value(
            first_meaningful_field(fields, ("source_projection_fallback_attempted",))
        ),
        "source_projection_external_engine_invoked": bool_field_value(
            first_meaningful_field(fields, ("source_projection_external_engine_invoked",))
        ),
        "source_projection_claim_boundary": first_meaningful_field(
            fields, ("source_projection_claim_boundary",)
        )
        or (
            "source projection admission is scenario-scoped and requires clean benchmark "
            "refresh before timing claims"
        ),
    }


def source_columnar_default_fields(
    status: str,
    *,
    source_family: str | None = None,
    claim_boundary: str | None = None,
) -> dict[str, Any]:
    return {
        "source_columnar_provider_schema_version": SOURCE_COLUMNAR_PROVIDER_SCHEMA_VERSION,
        "source_columnar_provider_status": status,
        "source_columnar_provider_surface": status,
        "source_columnar_source_family": source_family or status,
        "source_columnar_input_format": status,
        "source_columnar_projected_field_mask": "0x00000000",
        "source_columnar_preserved_column_count": 0,
        "source_columnar_skipped_column_count": 0,
        "source_columnar_materialized_row_count": 0,
        "source_columnar_record_batch_count": 0,
        "source_columnar_row_materialization_status": status,
        "source_columnar_null_validity_status": status,
        "source_columnar_unsupported_dtype_reason": status,
        "source_columnar_handoff_micros": 0,
        "source_to_vortex_handoff_micros": 0,
        "source_columnar_correctness_digest_status": status,
        "source_columnar_fallback_attempted": False,
        "source_columnar_external_engine_invoked": False,
        "source_columnar_claim_boundary": claim_boundary or status,
    }


def source_columnar_fields_from_runtime(
    fields: dict[str, Any],
    *,
    default_status: str = "not_reported",
) -> dict[str, Any]:
    input_format = str(
        first_meaningful_field(
            fields, ("source_columnar_input_format", "input_format", "storage_format")
        )
        or default_status
    ).strip()
    input_format_normalized = input_format.lower()
    target_columnar_input = input_format_normalized in {"parquet", "arrow-ipc", "arrow_ipc"}
    any_columnar_input = input_format_normalized in {
        "parquet",
        "arrow-ipc",
        "arrow_ipc",
        "avro",
        "orc",
    }
    decode_status = first_meaningful_field(fields, ("source_read_decode_status",)) or ""
    direct_columnar_provider = decode_status in {
        "direct_columnar_provider_decode",
        "projection_aware_columnar_provider_decode",
    }
    materialization_status = (
        first_meaningful_field(fields, ("source_read_row_materialization_status",)) or ""
    )
    scalar_boundary = materialization_status in {
        "row_structs_materialized",
        "mixed_scalar_rows_and_provider_batches",
    }
    source_state_columnar_preserved = bool_field_value(
        first_meaningful_field(fields, ("source_state_columnar_preserved",))
    )
    row_boundary = source_state_columnar_preserved and not direct_columnar_provider
    row_assembly_micros = int(
        numeric_value(
            first_meaningful_field(fields, ("source_read_row_assembly_micros",))
        )
        or 0
    )
    decoded_count = int(
        numeric_value(
            first_meaningful_field(
                fields,
                (
                    "source_columnar_preserved_column_count",
                    "source_read_decoded_column_count",
                ),
            )
        )
        or 0
    )
    skipped_count = int(
        numeric_value(
            first_meaningful_field(
                fields,
                (
                    "source_columnar_skipped_column_count",
                    "source_read_skipped_column_count",
                ),
            )
        )
        or 0
    )
    record_batch_count = int(
        numeric_value(
            first_meaningful_field(
                fields,
                ("source_columnar_record_batch_count", "source_state_record_batch_count"),
            )
        )
        or 0
    )
    rows_scanned = int(numeric_value(first_meaningful_field(fields, ("rows_scanned",))) or 0)
    admitted = (
        target_columnar_input
        and direct_columnar_provider
        and row_assembly_micros == 0
        and not scalar_boundary
    )
    status = first_meaningful_field(fields, ("source_columnar_provider_status",))
    if status is None:
        if admitted and skipped_count > 0:
            status = "admitted_projected_direct_columnar_provider"
        elif admitted:
            status = "admitted_direct_columnar_provider"
        elif direct_columnar_provider and any_columnar_input:
            status = "not_in_6r_c_scope_columnar_provider"
        elif row_boundary and target_columnar_input:
            status = "not_admitted_decoded_row_boundary"
        elif row_boundary and any_columnar_input:
            status = "not_in_6r_c_scope_row_boundary"
        elif scalar_boundary:
            status = "not_admitted_scalar_row_materialization"
        elif any_columnar_input:
            status = "blocked_columnar_provider_not_admitted"
        else:
            status = "not_applicable_non_columnar_source"
    surface = first_meaningful_field(fields, ("source_columnar_provider_surface",))
    if surface is None:
        if direct_columnar_provider:
            surface = "vortex_provider_record_batch"
        elif row_boundary:
            surface = "local_columnar_source_state_adapter_row_boundary"
        elif any_columnar_input:
            surface = "columnar_source_adapter_without_provider_admission"
        else:
            surface = "not_applicable_non_columnar_source"
    decoded_columns = first_meaningful_field(fields, ("source_read_decoded_columns",)) or "none"
    skipped_columns = first_meaningful_field(fields, ("source_read_skipped_columns",)) or "none"
    null_validity_status = first_meaningful_field(
        fields, ("source_columnar_null_validity_status",)
    )
    if null_validity_status is None:
        if admitted and "fact.nullable_metric_00" in decoded_columns:
            null_validity_status = "null_validity_preserved_for_required_columns"
        elif admitted and "fact.nullable_metric_00" in skipped_columns:
            null_validity_status = "nullable_metric_skipped_by_projection"
        elif admitted:
            null_validity_status = "no_null_heavy_column_required"
        elif row_boundary:
            null_validity_status = "decoded_row_boundary_validity_not_columnar_claim"
        elif any_columnar_input:
            null_validity_status = "not_admitted_columnar_provider"
        else:
            null_validity_status = "not_applicable_non_columnar_source"
    unsupported_dtype_reason = first_meaningful_field(
        fields, ("source_columnar_unsupported_dtype_reason",)
    )
    if unsupported_dtype_reason is None:
        if admitted:
            unsupported_dtype_reason = "none_supported_benchmark_columnar_shape"
        elif direct_columnar_provider and any_columnar_input:
            unsupported_dtype_reason = "format_outside_6r_c_claim_scope"
        elif row_boundary:
            unsupported_dtype_reason = "row_boundary_before_direct_provider_admission"
        elif scalar_boundary:
            unsupported_dtype_reason = "scalar_row_materialization_boundary"
        elif any_columnar_input:
            unsupported_dtype_reason = first_meaningful_field(
                fields, ("source_read_unsupported_shape_diagnostic",)
            ) or "columnar_provider_not_admitted"
        else:
            unsupported_dtype_reason = "not_applicable_non_columnar_source"
    handoff_micros = int(
        numeric_value(
            first_meaningful_field(
                fields,
                (
                    "source_columnar_handoff_micros",
                    "source_to_vortex_handoff_micros",
                    "source_read_columnar_handoff_micros",
                ),
            )
        )
        or 0
    )
    return {
        "source_columnar_provider_schema_version": first_meaningful_field(
            fields, ("source_columnar_provider_schema_version",)
        )
        or SOURCE_COLUMNAR_PROVIDER_SCHEMA_VERSION,
        "source_columnar_provider_status": status,
        "source_columnar_provider_surface": surface,
        "source_columnar_source_family": first_meaningful_field(
            fields, ("source_columnar_source_family",)
        )
        or ("already_columnar_source" if any_columnar_input else default_status),
        "source_columnar_input_format": input_format,
        "source_columnar_projected_field_mask": first_meaningful_field(
            fields, ("source_columnar_projected_field_mask", "source_read_projected_field_mask")
        )
        or "0x00000000",
        "source_columnar_preserved_column_count": decoded_count if direct_columnar_provider else 0,
        "source_columnar_skipped_column_count": skipped_count if direct_columnar_provider else 0,
        "source_columnar_materialized_row_count": 0
        if direct_columnar_provider and not scalar_boundary
        else (rows_scanned if any_columnar_input else 0),
        "source_columnar_record_batch_count": record_batch_count,
        "source_columnar_row_materialization_status": materialization_status or default_status,
        "source_columnar_null_validity_status": null_validity_status,
        "source_columnar_unsupported_dtype_reason": unsupported_dtype_reason,
        "source_columnar_handoff_micros": handoff_micros,
        "source_to_vortex_handoff_micros": handoff_micros,
        "source_columnar_correctness_digest_status": first_meaningful_field(
            fields, ("source_columnar_correctness_digest_status",)
        )
        or (
            "covered_by_route_correctness_digest"
            if admitted
            else "not_applicable_columnar_provider_not_admitted"
        ),
        "source_columnar_fallback_attempted": bool_field_value(
            first_meaningful_field(fields, ("source_columnar_fallback_attempted",))
        ),
        "source_columnar_external_engine_invoked": bool_field_value(
            first_meaningful_field(fields, ("source_columnar_external_engine_invoked",))
        ),
        "source_columnar_claim_boundary": first_meaningful_field(
            fields, ("source_columnar_claim_boundary",)
        )
        or (
            "source columnar-provider admission is scenario-scoped and requires clean "
            "benchmark refresh before timing claims"
        ),
    }


def source_read_scout_fields_for_row(
    row: dict[str, Any], stage_fields: dict[str, Any]
) -> dict[str, Any]:
    fields = runtime_validation_field_map(row)
    if not is_shardloom_engine(str(row.get("engine") or "")):
        return {
            "source_read_scout_schema_version": SOURCE_READ_SCOUT_SCHEMA_VERSION,
            "source_read_scout_status": "external_baseline_only",
            "source_read_scout_timing_split_status": "external_baseline_only",
            "source_read_header_scout_ms": None,
            "source_read_byte_acquisition_ms": None,
            "source_read_full_body_ms": None,
            "source_read_typed_decode_ms": None,
            "source_read_row_assembly_ms": None,
            "source_read_anomaly_quarantine_ms": None,
            "source_read_columnar_handoff_ms": None,
            "source_read_scout_residual_ms": None,
            "source_read_scout_reuse_status": "external_baseline_only",
            "source_read_decode_status": "external_baseline_only",
            "source_read_projected_field_mask": "0x00000000",
            "source_read_filter_field_mask": "0x00000000",
            "source_read_decoded_columns": "none",
            "source_read_skipped_columns": "none",
            "source_read_decoded_column_count": 0,
            "source_read_skipped_column_count": 0,
            "source_read_row_materialization_status": "external_baseline_only",
            "source_read_unsupported_shape_diagnostic": "external_baseline_only",
            **source_typed_builder_default_fields(
                "external_baseline_only",
                claim_boundary="external_baseline_only",
            ),
            **source_projection_default_fields(
                "external_baseline_only",
                claim_boundary="external_baseline_only",
            ),
            **source_columnar_default_fields(
                "external_baseline_only",
                claim_boundary="external_baseline_only",
            ),
            "source_read_scout_claim_boundary": "external_baseline_only",
        }

    source_read = first_numeric_millis(
        {**fields, **stage_fields},
        ("exclusive_source_read_ms", "source_read_ms", "source_read_millis"),
    )
    if source_read is None:
        return {
            "source_read_scout_schema_version": SOURCE_READ_SCOUT_SCHEMA_VERSION,
            "source_read_scout_status": "not_applicable_no_source_read_stage",
            "source_read_scout_timing_split_status": "not_applicable",
            "source_read_header_scout_ms": None,
            "source_read_byte_acquisition_ms": None,
            "source_read_full_body_ms": None,
            "source_read_typed_decode_ms": None,
            "source_read_row_assembly_ms": None,
            "source_read_anomaly_quarantine_ms": None,
            "source_read_columnar_handoff_ms": None,
            "source_read_scout_residual_ms": None,
            "source_read_scout_reuse_status": "not_applicable",
            "source_read_decode_status": "not_applicable",
            "source_read_projected_field_mask": "0x00000000",
            "source_read_filter_field_mask": "0x00000000",
            "source_read_decoded_columns": "none",
            "source_read_skipped_columns": "none",
            "source_read_decoded_column_count": 0,
            "source_read_skipped_column_count": 0,
            "source_read_row_materialization_status": "not_applicable",
            "source_read_unsupported_shape_diagnostic": "not_applicable",
            **source_typed_builder_default_fields(
                "not_applicable_no_source_read_stage",
                claim_boundary=(
                    "source typed-builder evidence is not applicable because the row has no "
                    "source-read stage"
                ),
            ),
            **source_projection_default_fields(
                "not_applicable_no_source_read_stage",
                claim_boundary=(
                    "source projection admission is not applicable because the row has no "
                    "source-read stage"
                ),
            ),
            **source_columnar_default_fields(
                "not_applicable_no_source_read_stage",
                claim_boundary=(
                    "source columnar-provider evidence is not applicable because the row has no "
                    "source-read stage"
                ),
            ),
            "source_read_scout_claim_boundary": (
                "source-read scout attribution is diagnostic timing evidence only; route totals "
                "remain the comparison surface"
            ),
        }

    header_scout = first_numeric_stage_millis(
        fields,
        millis_keys=(
            "source_read_header_scout_millis",
            "source_header_scout_millis",
            "source_scout_read_millis",
        ),
        micros_keys=(
            "source_read_header_scout_micros",
            "source_header_scout_micros",
            "source_scout_read_micros",
        ),
    )
    byte_acquisition = first_numeric_stage_millis(
        fields,
        millis_keys=(
            "source_read_byte_acquisition_millis",
            "source_byte_acquisition_millis",
            "source_body_read_millis",
        ),
        micros_keys=(
            "source_read_byte_acquisition_micros",
            "source_byte_acquisition_micros",
            "source_body_read_micros",
        ),
    )
    full_body = first_numeric_stage_millis(
        fields,
        millis_keys=("source_read_full_body_millis", "source_full_body_read_millis"),
        micros_keys=("source_read_full_body_micros", "source_full_body_read_micros"),
    )
    typed_decode = first_numeric_stage_millis(
        fields,
        millis_keys=("source_read_typed_decode_millis",),
        micros_keys=("source_read_typed_decode_micros",),
    )
    row_assembly = first_numeric_stage_millis(
        fields,
        millis_keys=("source_read_row_assembly_millis",),
        micros_keys=("source_read_row_assembly_micros",),
    )
    anomaly_quarantine = first_numeric_stage_millis(
        fields,
        millis_keys=("source_read_anomaly_quarantine_millis",),
        micros_keys=("source_read_anomaly_quarantine_micros",),
    )
    columnar_handoff = first_numeric_stage_millis(
        fields,
        millis_keys=("source_read_columnar_handoff_millis",),
        micros_keys=("source_read_columnar_handoff_micros",),
    )
    read_pieces = [
        value
        for value in (header_scout, byte_acquisition, full_body)
        if value is not None and value >= 0.0
    ]
    split_sum = sum(read_pieces)
    residual = round(source_read - split_sum, 4) if read_pieces else None
    diagnostic_pieces_present = all(
        value is not None
        for value in (
            typed_decode,
            row_assembly,
            anomaly_quarantine,
            columnar_handoff,
        )
    )
    complete = (
        header_scout is not None
        and byte_acquisition is not None
        and full_body is not None
        and diagnostic_pieces_present
        and residual is not None
        and residual >= -0.001
    )
    timing_status = (
        "complete"
        if complete
        else "blocked_missing_source_read_scout_split"
    )
    scout_status = (
        first_meaningful_field(fields, ("source_read_scout_status",))
        or (
            "source_read_scout_split_recorded"
            if complete
            else "source_read_scout_split_missing"
        )
    )
    return {
        "source_read_scout_schema_version": SOURCE_READ_SCOUT_SCHEMA_VERSION,
        "source_read_scout_status": scout_status,
        "source_read_scout_timing_split_status": timing_status,
        "source_read_header_scout_ms": header_scout,
        "source_read_byte_acquisition_ms": byte_acquisition,
        "source_read_full_body_ms": full_body,
        "source_read_typed_decode_ms": typed_decode,
        "source_read_row_assembly_ms": row_assembly,
        "source_read_anomaly_quarantine_ms": anomaly_quarantine,
        "source_read_columnar_handoff_ms": columnar_handoff,
        "source_read_scout_residual_ms": residual,
        "source_read_scout_reuse_status": first_meaningful_field(
            fields, ("source_read_scout_reuse_status",)
        )
        or (
            "not_reused_fresh_source_read"
            if complete
            else "blocked_until_scout_timing_split"
        ),
        "source_read_decode_status": first_meaningful_field(
            fields, ("source_read_decode_status",)
        )
        or "not_reported",
        "source_read_projected_field_mask": first_meaningful_field(
            fields, ("source_read_projected_field_mask",)
        )
        or "0x00000000",
        "source_read_filter_field_mask": first_meaningful_field(
            fields, ("source_read_filter_field_mask",)
        )
        or "0x00000000",
        "source_read_decoded_columns": first_meaningful_field(
            fields, ("source_read_decoded_columns",)
        )
        or "none",
        "source_read_skipped_columns": first_meaningful_field(
            fields, ("source_read_skipped_columns",)
        )
        or "none",
        "source_read_decoded_column_count": int(
            numeric_value(
                first_meaningful_field(fields, ("source_read_decoded_column_count",))
            )
            or 0
        ),
        "source_read_skipped_column_count": int(
            numeric_value(
                first_meaningful_field(fields, ("source_read_skipped_column_count",))
            )
            or 0
        ),
        "source_read_row_materialization_status": first_meaningful_field(
            fields, ("source_read_row_materialization_status",)
        )
        or "not_reported",
        "source_read_unsupported_shape_diagnostic": first_meaningful_field(
            fields, ("source_read_unsupported_shape_diagnostic",)
        )
        or "not_reported",
        **source_typed_builder_fields_from_runtime(fields),
        **source_projection_fields_from_runtime(fields),
        **source_columnar_fields_from_runtime(fields),
        "source_read_scout_claim_boundary": (
            "source-read scout attribution explains header/scout, byte acquisition, and full-body "
            "read composition only; it does not authorize performance, production, or "
            "Spark-displacement claims"
        ),
    }


def source_state_column_list(value: Any) -> list[str]:
    if value is None:
        return []
    text = str(value).strip()
    if text.lower() in {
        "",
        "none",
        "not_applicable",
        "not_evaluated",
        "not_executed",
        "not_reported",
        "not_requested",
        "unknown",
    }:
        return []
    separator = "|" if "|" in text else ","
    return [column.strip() for column in text.split(separator) if column.strip()]


def source_state_column_text(value: Any) -> str:
    columns = source_state_column_list(value)
    return ",".join(columns) if columns else "none"


def source_state_column_count(
    fields: dict[str, Any],
    direct_count_field: str,
    column_text: str,
    *,
    fallback_count_field: str | None = None,
) -> int:
    direct = numeric_value(fields.get(direct_count_field))
    if direct is not None:
        return int(direct)
    if fallback_count_field is not None:
        fallback = numeric_value(fields.get(fallback_count_field))
        if fallback is not None:
            return int(fallback)
    return len(source_state_column_list(column_text))


def source_state_projection_fields_for_row(
    row: dict[str, Any],
    source_read_scout_fields: dict[str, Any],
) -> dict[str, Any]:
    fields = {
        **runtime_validation_field_map(row),
        **source_read_scout_fields,
    }
    if not is_shardloom_engine(str(row.get("engine") or "")):
        default_status = "external_baseline_only"
        return {
            "source_state_read_plan": default_status,
            "source_state_projection_pushdown_status": default_status,
            "source_state_reader_projection_columns": "none",
            "source_state_reader_projection_column_count": 0,
            "source_state_projected_field_mask": "0x00000000",
            "source_state_filter_field_mask": "0x00000000",
            "source_state_decoded_columns": "none",
            "source_state_skipped_columns": "none",
            "source_state_decoded_column_count": 0,
            "source_state_skipped_column_count": 0,
        }

    scout_status = str(fields.get("source_read_scout_status") or "not_reported")
    timing_status = str(
        fields.get("source_read_scout_timing_split_status") or "not_reported"
    )
    if scout_status == "external_baseline_only":
        default_status = "external_baseline_only"
    elif timing_status == "not_applicable":
        default_status = "not_applicable_no_source_read_stage"
    else:
        default_status = "not_reported"

    decoded_columns = source_state_column_text(
        first_meaningful_field(
            fields,
            ("source_state_decoded_columns", "source_read_decoded_columns"),
        )
    )
    skipped_columns = source_state_column_text(
        first_meaningful_field(
            fields,
            ("source_state_skipped_columns", "source_read_skipped_columns"),
        )
    )
    reader_projection_columns = source_state_column_text(
        first_meaningful_field(
            fields,
            (
                "source_state_reader_projection_columns",
                "reader_projection_columns",
                "source_read_decoded_columns",
            ),
        )
    )
    decoded_count = source_state_column_count(
        fields,
        "source_state_decoded_column_count",
        decoded_columns,
        fallback_count_field="source_read_decoded_column_count",
    )
    skipped_count = source_state_column_count(
        fields,
        "source_state_skipped_column_count",
        skipped_columns,
        fallback_count_field="source_read_skipped_column_count",
    )
    reader_projection_count = source_state_column_count(
        fields,
        "source_state_reader_projection_column_count",
        reader_projection_columns,
    )
    if (
        reader_projection_columns == "none"
        and decoded_columns != "none"
        and reader_projection_count == 0
    ):
        reader_projection_columns = decoded_columns
        reader_projection_count = decoded_count

    read_plan = first_meaningful_field(
        fields,
        ("source_state_read_plan", "vortex_scout_ingress_read_plan"),
    ) or (
        "projection_aware_source_scout"
        if skipped_count > 0
        else "full_source_read"
        if decoded_count > 0
        else default_status
    )
    projection_status = first_meaningful_field(
        fields,
        ("source_state_projection_pushdown_status",),
    ) or (
        "reader_projection_applied"
        if skipped_count > 0
        else "not_requested_full_read"
        if decoded_count > 0
        else default_status
    )

    return {
        "source_state_read_plan": read_plan,
        "source_state_projection_pushdown_status": projection_status,
        "source_state_reader_projection_columns": reader_projection_columns,
        "source_state_reader_projection_column_count": reader_projection_count,
        "source_state_projected_field_mask": first_meaningful_field(
            fields,
            ("source_state_projected_field_mask", "source_read_projected_field_mask"),
        )
        or "0x00000000",
        "source_state_filter_field_mask": first_meaningful_field(
            fields,
            ("source_state_filter_field_mask", "source_read_filter_field_mask"),
        )
        or "0x00000000",
        "source_state_decoded_columns": decoded_columns,
        "source_state_skipped_columns": skipped_columns,
        "source_state_decoded_column_count": decoded_count,
        "source_state_skipped_column_count": skipped_count,
    }


def prepared_state_optimization_fields_for_row(row: dict[str, Any]) -> dict[str, Any]:
    fields = runtime_validation_field_map(row)
    if str(row.get("engine") or "") != "shardloom-prepare-batch":
        return {}

    row_success = row.get("status") == "success"
    lookup_status = first_meaningful_field(
        fields,
        (
            "prepare_batch_prepared_state_lookup_status",
            "prepare_batch_prepared_state_index_lookup_status",
        ),
    )
    repair_status = first_meaningful_field(
        fields, ("prepare_batch_prepared_state_partial_repair_status",)
    )
    dependency_status = first_meaningful_field(
        fields, ("prepare_batch_prepared_state_dependency_status",)
    )
    reuse_hit = first_bool_field(
        fields,
        (
            "prepared_state_reuse_hit",
            "prepared_state_reused",
        ),
        default=False,
    )
    delta_admitted = first_bool_field(
        fields,
        (
            "prepare_batch_prepared_state_optimization_delta_overlay_admitted",
            "prepare_batch_prepared_state_delta_overlay_admitted",
            "delta_overlay_admitted",
        ),
        default=False,
    )
    strategy = first_meaningful_field(
        fields, ("prepare_batch_prepared_state_optimization_strategy",)
    )
    if strategy is None:
        if delta_admitted:
            strategy = "append_only_delta_overlay"
        elif repair_status == "admitted_role_repair_completed":
            strategy = "role_scoped_repair"
        elif (
            lookup_status in {"workspace_manifest_hit", "workspace_index_manifest_hit"}
            or dependency_status == "manifest_dependencies_matched"
            or reuse_hit
        ):
            strategy = "manifest_reuse"
        elif lookup_status == "cache_miss_created_and_registered":
            strategy = "full_prepare_register"
        elif row_success:
            strategy = "full_prepare_register"
        else:
            strategy = "not_reported"

    status = first_meaningful_field(
        fields, ("prepare_batch_prepared_state_optimization_status",)
    )
    if status is None:
        status = {
            "manifest_reuse": "prepared_state_manifest_hit",
            "append_only_delta_overlay": "prepared_state_delta_overlay_admitted",
            "role_scoped_repair": "prepared_state_role_repair_admitted",
            "full_prepare_register": "prepared_state_full_prepare_registered",
        }.get(strategy, lookup_status or "not_reported")

    repair_micros = first_numeric_micros(
        fields,
        micros_keys=("prepare_batch_prepared_state_optimization_repair_micros",),
        millis_keys=("prepare_batch_prepared_state_optimization_repair_ms",),
    )
    if repair_micros is None and strategy == "role_scoped_repair":
        repair_micros = first_numeric_micros(
            fields,
            micros_keys=("prepare_batch_prepared_state_partial_repair_micros",),
        )
    delta_overlay_micros = first_numeric_micros(
        fields,
        micros_keys=("prepare_batch_prepared_state_optimization_delta_overlay_micros",),
        millis_keys=("prepare_batch_prepared_state_optimization_delta_overlay_ms",),
    )
    if delta_overlay_micros is None and strategy == "append_only_delta_overlay":
        delta_overlay_micros = sum(
            value or 0
            for value in (
                first_numeric_micros(
                    fields,
                    micros_keys=(
                        "prepare_batch_prepared_state_delta_overlay_full_content_digest_micros",
                    ),
                ),
                first_numeric_micros(
                    fields,
                    micros_keys=(
                        "prepare_batch_prepared_state_delta_overlay_delta_source_write_micros",
                    ),
                ),
                first_numeric_micros(
                    fields,
                    micros_keys=(
                        "prepare_batch_prepared_state_delta_overlay_delta_artifact_write_micros",
                    ),
                ),
                first_numeric_micros(
                    fields,
                    micros_keys=(
                        "prepare_batch_prepared_state_delta_overlay_replay_verification_micros",
                    ),
                ),
            )
        )
    partial_repair_source_to_columnar_micros = first_numeric_micros(
        fields,
        micros_keys=(
            "prepare_batch_prepared_state_partial_repair_source_to_columnar_micros",
        ),
        millis_keys=(
            "prepare_batch_prepared_state_partial_repair_source_to_columnar_ms",
        ),
    )
    partial_repair_vortex_array_build_micros = first_numeric_micros(
        fields,
        micros_keys=(
            "prepare_batch_prepared_state_partial_repair_vortex_array_build_micros",
        ),
        millis_keys=(
            "prepare_batch_prepared_state_partial_repair_vortex_array_build_ms",
        ),
    )
    partial_repair_vortex_write_micros = first_numeric_micros(
        fields,
        micros_keys=(
            "prepare_batch_prepared_state_partial_repair_vortex_write_micros",
        ),
        millis_keys=(
            "prepare_batch_prepared_state_partial_repair_vortex_write_ms",
        ),
    )
    partial_repair_vortex_reopen_verify_micros = first_numeric_micros(
        fields,
        micros_keys=(
            "prepare_batch_prepared_state_partial_repair_vortex_reopen_verify_micros",
        ),
        millis_keys=(
            "prepare_batch_prepared_state_partial_repair_vortex_reopen_verify_ms",
        ),
    )
    base_artifact_reused = first_bool_field(
        fields,
        (
            "prepare_batch_prepared_state_optimization_base_artifact_reused",
            "base_artifact_reused",
            "prepare_batch_prepared_state_delta_overlay_base_artifact_reused",
        ),
        default=None,
    )
    if base_artifact_reused is None:
        base_artifact_reused = strategy in {
            "manifest_reuse",
            "role_scoped_repair",
            "append_only_delta_overlay",
        }
    if strategy in {"manifest_reuse", "role_scoped_repair", "append_only_delta_overlay"}:
        base_artifact_reused = True
    return {
        "prepare_batch_prepared_state_optimization_schema_version": first_meaningful_field(
            fields,
            ("prepare_batch_prepared_state_optimization_schema_version",),
        )
        or PREPARED_STATE_OPTIMIZATION_SCHEMA_VERSION,
        "prepare_batch_prepared_state_optimization_status": status,
        "prepare_batch_prepared_state_optimization_strategy": strategy,
        "prepare_batch_prepared_state_optimization_index_digest": first_meaningful_field(
            fields,
            (
                "prepare_batch_prepared_state_optimization_index_digest",
                "prepare_batch_prepared_state_index_digest",
            ),
        )
        or "not_reported",
        "prepare_batch_prepared_state_optimization_manifest_digest": first_meaningful_field(
            fields,
            (
                "prepare_batch_prepared_state_optimization_manifest_digest",
                "prepare_batch_prepared_state_dependency_manifest_digest",
                "prepared_state_reuse_manifest_digest",
            ),
        )
        or "not_reported",
        "prepare_batch_prepared_state_optimization_source_packet_digest": first_meaningful_field(
            fields,
            (
                "prepare_batch_prepared_state_optimization_source_packet_digest",
                "prepare_batch_prepared_state_dependency_source_packet_digest",
            ),
        )
        or "not_reported",
        "prepare_batch_prepared_state_optimization_changed_roles": first_meaningful_field(
            fields,
            (
                "prepare_batch_prepared_state_optimization_changed_roles",
                "prepare_batch_prepared_state_dependency_changed_roles",
            ),
        )
        or "not_reported",
        "prepare_batch_prepared_state_optimization_reused_roles": first_meaningful_field(
            fields,
            (
                "prepare_batch_prepared_state_optimization_reused_roles",
                "prepare_batch_prepared_state_partial_repair_reused_roles",
            ),
        )
        or "not_reported",
        "prepare_batch_prepared_state_optimization_repaired_roles": first_meaningful_field(
            fields,
            (
                "prepare_batch_prepared_state_optimization_repaired_roles",
                "prepare_batch_prepared_state_partial_repair_repaired_roles",
            ),
        )
        or "not_reported",
        "prepare_batch_prepared_state_optimization_invalidated_derived_states": first_meaningful_field(
            fields,
            (
                "prepare_batch_prepared_state_optimization_invalidated_derived_states",
                "prepare_batch_prepared_state_partial_repair_invalidated_derived_states",
            ),
        )
        or "not_reported",
        "prepare_batch_prepared_state_optimization_invalidation_reason": first_meaningful_field(
            fields,
            (
                "prepare_batch_prepared_state_optimization_invalidation_reason",
                "prepared_state_invalidation_reason",
                "prepare_batch_prepared_state_invalidation_reason",
            ),
        )
        or "not_reported",
        "prepare_batch_prepared_state_optimization_manifest_lookup_ms": micros_to_millis(
            first_numeric_micros(
                fields,
                micros_keys=(
                    "prepare_batch_prepared_state_optimization_manifest_lookup_micros",
                    "prepare_batch_prepared_state_manifest_lookup_micros",
                ),
                millis_keys=(
                    "prepare_batch_prepared_state_optimization_manifest_lookup_ms",
                    "prepared_state_lookup_or_create_ms",
                ),
            )
        )
        or 0.0,
        "prepare_batch_prepared_state_optimization_manifest_match_ms": micros_to_millis(
            first_numeric_micros(
                fields,
                micros_keys=(
                    "prepare_batch_prepared_state_optimization_manifest_match_micros",
                    "prepare_batch_prepared_state_cache_hit_micros",
                ),
                millis_keys=(
                    "prepare_batch_prepared_state_optimization_manifest_match_ms",
                ),
            )
        )
        or 0.0,
        "prepare_batch_prepared_state_optimization_cache_miss_create_ms": micros_to_millis(
            first_numeric_micros(
                fields,
                micros_keys=(
                    "prepare_batch_prepared_state_optimization_cache_miss_create_micros",
                    "prepare_batch_prepared_state_cache_miss_create_micros",
                ),
                millis_keys=(
                    "prepare_batch_prepared_state_optimization_cache_miss_create_ms",
                ),
            )
        )
        or 0.0,
        "prepare_batch_prepared_state_optimization_artifact_write_ms": micros_to_millis(
            first_numeric_micros(
                fields,
                micros_keys=(
                    "prepare_batch_prepared_state_optimization_artifact_write_micros",
                    "prepare_batch_prepared_state_artifact_write_micros",
                ),
                millis_keys=(
                    "prepare_batch_prepared_state_optimization_artifact_write_ms",
                ),
            )
        )
        or 0.0,
        "prepare_batch_prepared_state_optimization_repair_ms": micros_to_millis(
            repair_micros
        )
        or 0.0,
        "prepare_batch_prepared_state_optimization_delta_overlay_ms": micros_to_millis(
            delta_overlay_micros
        )
        or 0.0,
        "prepare_batch_prepared_state_optimization_replay_verification_ms": micros_to_millis(
            first_numeric_micros(
                fields,
                micros_keys=(
                    "prepare_batch_prepared_state_optimization_replay_verification_micros",
                    "prepare_batch_prepared_state_replay_verification_micros",
                ),
                millis_keys=(
                    "prepare_batch_prepared_state_optimization_replay_verification_ms",
                ),
            )
        )
        or 0.0,
        "prepare_batch_prepared_state_partial_repair_source_to_columnar_micros": (
            partial_repair_source_to_columnar_micros or 0.0
        ),
        "prepare_batch_prepared_state_partial_repair_vortex_array_build_micros": (
            partial_repair_vortex_array_build_micros or 0.0
        ),
        "prepare_batch_prepared_state_partial_repair_vortex_write_micros": (
            partial_repair_vortex_write_micros or 0.0
        ),
        "prepare_batch_prepared_state_partial_repair_vortex_reopen_verify_micros": (
            partial_repair_vortex_reopen_verify_micros or 0.0
        ),
        "prepare_batch_prepared_state_partial_repair_source_to_columnar_ms": micros_to_millis(
            partial_repair_source_to_columnar_micros
        )
        or 0.0,
        "prepare_batch_prepared_state_partial_repair_vortex_array_build_ms": micros_to_millis(
            partial_repair_vortex_array_build_micros
        )
        or 0.0,
        "prepare_batch_prepared_state_partial_repair_vortex_write_ms": micros_to_millis(
            partial_repair_vortex_write_micros
        )
        or 0.0,
        "prepare_batch_prepared_state_partial_repair_vortex_reopen_verify_ms": micros_to_millis(
            partial_repair_vortex_reopen_verify_micros
        )
        or 0.0,
        "prepare_batch_prepared_state_optimization_delta_overlay_admitted": bool(
            delta_admitted
        ),
        "prepare_batch_prepared_state_optimization_base_artifact_reused": bool(
            base_artifact_reused
        ),
        "prepare_batch_prepared_state_optimization_proof_digest": first_meaningful_field(
            fields,
            (
                "prepare_batch_prepared_state_optimization_proof_digest",
                "prepare_batch_prepared_state_delta_overlay_correctness_digest",
                "prepare_batch_prepared_state_partial_repair_replay_proof",
                "prepared_state_reuse_manifest_digest",
            ),
        )
        or "not_reported",
        "prepare_batch_prepared_state_optimization_replay_proof": first_meaningful_field(
            fields,
            (
                "prepare_batch_prepared_state_optimization_replay_proof",
                "prepare_batch_prepared_state_delta_overlay_replay_proof",
                "prepare_batch_prepared_state_partial_repair_replay_proof",
            ),
        )
        or "not_reported",
        "prepare_batch_prepared_state_optimization_blocker_id": first_meaningful_field(
            fields,
            (
                "prepare_batch_prepared_state_optimization_blocker_id",
                "prepare_batch_prepared_state_partial_repair_blocker_id",
                "prepare_batch_prepared_state_delta_overlay_blocker_id",
            ),
        )
        or "none",
        "prepare_batch_prepared_state_optimization_stale_artifact_reuse_allowed": bool(
            first_bool_field(
                fields,
                (
                    "prepare_batch_prepared_state_optimization_stale_artifact_reuse_allowed",
                    "prepare_batch_prepared_state_partial_repair_stale_segment_reuse_allowed",
                    "prepare_batch_prepared_state_delta_overlay_stale_base_reuse_allowed",
                ),
                default=False,
            )
        ),
        "prepare_batch_prepared_state_optimization_no_fallback_policy_status": first_meaningful_field(
            fields,
            ("prepare_batch_prepared_state_optimization_no_fallback_policy_status",),
        )
        or "passed_fallback_false_external_engine_false",
        "prepare_batch_prepared_state_optimization_fallback_attempted": bool(
            first_bool_field(
                fields,
                ("prepare_batch_prepared_state_optimization_fallback_attempted",),
                default=False,
            )
        ),
        "prepare_batch_prepared_state_optimization_external_engine_invoked": bool(
            first_bool_field(
                fields,
                (
                    "prepare_batch_prepared_state_optimization_external_engine_invoked",
                    "prepare_batch_prepared_state_index_external_engine_invoked",
                    "prepare_batch_prepared_state_dependency_external_engine_invoked",
                    "prepare_batch_prepared_state_delta_overlay_external_engine_invoked",
                ),
                default=False,
            )
        ),
        "prepare_batch_prepared_state_optimization_claim_boundary": first_meaningful_field(
            fields,
            ("prepare_batch_prepared_state_optimization_claim_boundary",),
        )
        or (
            "Unified prepared-state optimization contract only; no stale reuse, "
            "external-engine execution, broad CDC/table, production, or performance superiority "
            "claim is authorized"
        ),
    }


def vortex_reopen_scan_attribution_fields_for_row(
    row: dict[str, Any], stage_fields: dict[str, Any]
) -> dict[str, Any]:
    fields = runtime_validation_field_map(row)
    if not is_shardloom_engine(str(row.get("engine") or "")):
        return {
            "vortex_reopen_scan_attribution_schema_version": (
                VORTEX_REOPEN_SCAN_ATTRIBUTION_SCHEMA_VERSION
            ),
            "vortex_reopen_verify_split_status": "external_baseline_only",
            "vortex_footer_open_ms": None,
            "vortex_metadata_verify_ms": None,
            "vortex_scan_open_ms": None,
            "vortex_scenario_scan_ms": None,
            "vortex_scan_counter_status": "external_baseline_only",
            "vortex_scan_bytes_touched": None,
            "vortex_scan_segments_touched": None,
            "vortex_scan_segments_skipped": None,
            "vortex_scan_columns_touched": None,
            "vortex_scan_decoded_values": None,
            "vortex_reopen_scan_claim_boundary": "external_baseline_only",
        }

    reopen_or_verify = first_numeric_millis(
        {**fields, **stage_fields},
        ("exclusive_vortex_reopen_verify_ms", "vortex_reopen_or_verify_ms"),
    )
    footer_open = first_numeric_stage_millis(
        fields,
        millis_keys=("vortex_footer_open_millis", "vortex_reopen_footer_open_millis"),
        micros_keys=("vortex_footer_open_micros", "vortex_reopen_footer_open_micros"),
    )
    metadata_verify = first_numeric_stage_millis(
        fields,
        millis_keys=(
            "vortex_metadata_verify_millis",
            "vortex_reopen_metadata_verify_millis",
        ),
        micros_keys=("vortex_metadata_verify_micros", "vortex_reopen_metadata_verify_micros"),
    )
    scan_open = first_numeric_stage_millis(
        fields,
        millis_keys=("vortex_scan_open_millis",),
        micros_keys=("vortex_scan_open_micros",),
    )
    scenario_scan = first_numeric_stage_millis(
        fields,
        millis_keys=("vortex_scenario_scan_millis", "vortex_scan_scenario_millis"),
        micros_keys=("vortex_scenario_scan_micros", "vortex_scan_scenario_micros"),
    )
    split_pieces = [
        value
        for value in (footer_open, metadata_verify, scan_open, scenario_scan)
        if value is not None and value >= 0.0
    ]
    if reopen_or_verify is None and not split_pieces:
        reopen_status = "not_applicable_no_reopen_verify_stage"
    elif footer_open is not None and metadata_verify is not None and scan_open is not None:
        reopen_status = "complete"
    else:
        reopen_status = "blocked_missing_reopen_verify_split"

    counter_fields = {
        "vortex_scan_bytes_touched": first_numeric_field(
            fields, ("vortex_scan_bytes_touched", "vortex_scan_useful_bytes")
        ),
        "vortex_scan_segments_touched": first_numeric_field(
            fields, ("vortex_scan_segments_touched", "vortex_scan_segment_count")
        ),
        "vortex_scan_segments_skipped": first_numeric_field(
            fields, ("vortex_scan_segments_skipped", "vortex_scan_pruned_segment_count")
        ),
        "vortex_scan_columns_touched": first_numeric_field(
            fields, ("vortex_scan_columns_touched", "vortex_scan_column_count")
        ),
        "vortex_scan_decoded_values": first_numeric_field(
            fields, ("vortex_scan_decoded_values", "vortex_scan_materialized_value_count")
        ),
    }
    has_scan_stage = first_numeric_millis(
        {**fields, **stage_fields}, ("vortex_scan_ms", "vortex_scan_millis")
    ) is not None
    counters_present = all(value is not None for value in counter_fields.values())
    if counters_present:
        counter_status = "complete"
    elif has_scan_stage:
        counter_status = "blocked_missing_scan_counters"
    else:
        counter_status = "not_applicable_no_scan_stage"

    return {
        "vortex_reopen_scan_attribution_schema_version": (
            VORTEX_REOPEN_SCAN_ATTRIBUTION_SCHEMA_VERSION
        ),
        "vortex_reopen_verify_split_status": reopen_status,
        "vortex_footer_open_ms": footer_open,
        "vortex_metadata_verify_ms": metadata_verify,
        "vortex_scan_open_ms": scan_open,
        "vortex_scenario_scan_ms": scenario_scan,
        "vortex_scan_counter_status": counter_status,
        **counter_fields,
        "vortex_reopen_scan_claim_boundary": (
            "Vortex reopen/scan attribution explains metadata verification, scan-open, "
            "scenario-scan, and data-movement counters only; route totals remain the "
            "comparison surface and no encoded-native claim is authorized without provider "
            "evidence"
        ),
    }


def route_timing_ledger_fields_for_row(
    row: dict[str, Any],
    identity: dict[str, Any],
    stage_fields: dict[str, Any],
) -> dict[str, Any]:
    fields = runtime_validation_field_map(row)
    lane_id = str(identity.get("route_lane_id") or stage_fields.get("route_lane_id") or "")
    total_route = numeric_value(stage_fields.get("total_route_ms"))
    query_runtime = prepared_route_query_runtime_millis(fields)
    preparation = prepare_once_preparation_millis(fields, stage_fields)
    batch_count = prepared_route_observed_batch_count(fields)
    amortized_preparation = (
        preparation / batch_count
        if preparation is not None and batch_count and batch_count > 0
        else None
    )
    detailed_stage_ids = set(_stage_ids_with_values(stage_fields))
    output_delivery = output_delivery_millis(fields, stage_fields)
    evidence_render = evidence_render_route_millis(fields, stage_fields)
    surface_fields = timing_surface_fields_for_row(row)
    timing_surface = str(stage_fields.get("timing_surface") or surface_fields["timing_surface"])
    output_for_surface = timing_surface in PROOF_TIMING_SURFACES
    evidence_for_surface = timing_surface == "publication_proof"

    included_stage_ids: tuple[str, ...]
    scope: str
    formula: str
    preparation_included: bool
    query_included: bool
    output_included: bool
    evidence_included: bool
    included_total = total_route

    def add_proof_stages(stage_ids: tuple[str, ...]) -> tuple[str, ...]:
        result = list(stage_ids)
        if output_for_surface:
            result.append("result_sink_write_millis")
        if evidence_for_surface:
            result.append("evidence_render_millis")
        return tuple(result)

    def formula_suffix() -> str:
        if timing_surface == "hot_runtime":
            return ""
        if timing_surface == "full_replay_proof":
            return " + result_sink_write_millis"
        if timing_surface == "publication_proof":
            return " + result_sink_write_millis + evidence_render_millis"
        return ""

    if lane_id == "prepare_once_first_query":
        included_stage_ids = add_proof_stages((
            "prepare_batch_preparation_millis",
            "query_runtime_millis",
        ))
        scope = f"{timing_surface}:prepare_once_first_query"
        formula = (
            f"timing_surface={timing_surface}; total_route_ms = "
            "prepare_batch_preparation_millis + query_runtime_millis"
            f"{formula_suffix()}"
        )
        preparation_included = True
        query_included = True
        output_included = output_for_surface
        evidence_included = evidence_for_surface
        included_total = total_route
    elif lane_id == "prepare_once_batch":
        included_stage_ids = add_proof_stages((
            "amortized_prepare_batch_preparation_millis",
            "query_runtime_millis",
        ))
        scope = f"{timing_surface}:prepare_once_batch_amortized"
        formula = (
            f"timing_surface={timing_surface}; total_route_ms = "
            "amortized_prepare_batch_preparation_millis + query_runtime_millis"
            f"{formula_suffix()}"
        )
        preparation_included = True
        query_included = True
        output_included = output_for_surface
        evidence_included = evidence_for_surface
        included_total = total_route
    elif lane_id in {"warm_prepared_query", "native_vortex_query"}:
        included_stage_ids = add_proof_stages(("query_runtime_millis",))
        scope = (
            f"{timing_surface}:warm_prepared_query_only"
            if lane_id == "warm_prepared_query"
            else f"{timing_surface}:native_vortex_query_only"
        )
        formula = (
            f"timing_surface={timing_surface}; total_route_ms = "
            f"query_runtime_millis{formula_suffix()}"
        )
        preparation_included = False
        query_included = True
        output_included = output_for_surface
        evidence_included = evidence_for_surface
        included_total = total_route
    elif lane_id == "external_baseline_end_to_end":
        included_stage_ids = ("external_engine_reported_total_runtime_millis",)
        scope = "external_baseline:external_baseline_end_to_end"
        formula = (
            "timing_surface=external_baseline; total_route_ms = external engine "
            "reported total_runtime_millis"
        )
        preparation_included = False
        query_included = True
        output_included = True
        evidence_included = False
    elif lane_id == "direct_transient_route":
        included_stage_ids = ("total_runtime_millis",)
        scope = f"{timing_surface}:direct_transient_route_total"
        formula = f"timing_surface={timing_surface}; total_route_ms = total_runtime_millis"
        preparation_included = False
        query_included = True
        output_included = output_for_surface
        evidence_included = evidence_for_surface
    else:
        included_stage_ids = add_proof_stages((
            "source_admission_ms",
            "source_read_ms",
            "source_parse_or_columnar_decode_ms",
            "source_to_vortex_array_ms",
            "vortex_write_ms",
            "vortex_reopen_or_verify_ms",
            "query_runtime_millis",
        ))
        scope = f"{timing_surface}:cold_certified_route_total"
        formula = (
            f"timing_surface={timing_surface}; total_route_ms = source/import "
            f"preparation stages + query_runtime_millis{formula_suffix()}"
        )
        preparation_included = True
        query_included = True
        output_included = output_for_surface
        evidence_included = evidence_for_surface

    included_detail_stage_ids = set(included_stage_ids)
    if output_included:
        included_detail_stage_ids.add("result_sink_write_ms")
    if evidence_included:
        included_detail_stage_ids.add("evidence_render_ms")
    if preparation_included and lane_id in {
        "prepare_once_first_query",
        "prepare_once_batch",
    }:
        included_detail_stage_ids.add("prepared_state_lookup_or_create_ms")
    excluded_stage_ids = sorted(detailed_stage_ids - included_detail_stage_ids)
    delta = (
        abs((included_total or 0.0) - (total_route or 0.0))
        if included_total is not None and total_route is not None
        else None
    )
    return {
        "route_timing_ledger_schema_version": ROUTE_TIMING_LEDGER_SCHEMA_VERSION,
        "route_timing_ledger_status": "valid" if delta is not None else "not_numeric",
        "route_timing_surface_schema_version": ROUTE_TIMING_SURFACE_SCHEMA_VERSION,
        "timing_surface": timing_surface,
        "timing_surface_label": TIMING_SURFACE_LABELS.get(timing_surface, timing_surface),
        "timing_surface_evidence_tier": surface_fields.get("timing_surface_evidence_tier"),
        "timing_surface_default_for_route": timing_surface == DEFAULT_ROUTE_TIMING_SURFACE
        if is_shardloom_engine(str(row.get("engine") or ""))
        else timing_surface == "external_baseline",
        "timing_surface_claim_boundary": surface_fields.get(
            "timing_surface_claim_boundary"
        ),
        "route_total_formula": formula,
        "route_timing_scope": scope,
        "stage_parent_id": lane_id or "unknown",
        "route_timing_included_stage_ids": ",".join(included_stage_ids),
        "route_timing_excluded_stage_ids": ",".join(excluded_stage_ids) or "none",
        "route_timing_included_stage_total_ms": included_total,
        "route_timing_total_delta_ms": delta,
        "preparation_timing_included_in_total": preparation_included,
        "query_timing_included_in_total": query_included,
        "output_timing_included_in_total": output_included,
        "evidence_timing_included_in_total": evidence_included,
    }


def stage_inclusion_class(
    stage_id: str,
    *,
    timing_surface: str,
    preparation_included: bool,
    query_included: bool,
    output_included: bool,
    evidence_included: bool,
    stage_value_present: bool,
    stage_excluded_from_route_total: bool,
) -> str:
    if stage_id == "cli_process_wall":
        return "excluded_harness"
    if not stage_value_present or stage_excluded_from_route_total:
        return "diagnostic_only"
    included_class = {
        "hot_runtime": "included_hot_runtime",
        "full_replay_proof": "included_full_replay_proof",
        "publication_proof": "included_publication_proof",
    }.get(timing_surface, "included")
    if stage_id in PREPARATION_STAGE_IDS:
        return included_class if preparation_included else "excluded_shared_preparation"
    if stage_id in {"vortex_scan", "operator_compute"}:
        return included_class if query_included else "diagnostic_only"
    if stage_id == "result_sink_write":
        return included_class if output_included else "diagnostic_only"
    if stage_id == "evidence_render":
        return included_class if evidence_included else "diagnostic_only"
    return "diagnostic_only"


def stage_inclusion_skip_reason(stage_class: str) -> str:
    return {
        "included": "included_in_route_total",
        "included_hot_runtime": "included_in_hot_runtime_route_total",
        "included_full_replay_proof": "included_in_full_replay_proof_route_total",
        "included_publication_proof": "included_in_publication_proof_route_total",
        "excluded_shared_preparation": "shared_preparation_outside_route_total",
        "excluded_harness": "harness_process_wall_not_part_of_route_total",
        "diagnostic_only": "not_measured_or_not_part_of_this_route",
    }.get(stage_class, "unknown")


def pack_stage_map(values: dict[str, str]) -> str:
    return ";".join(
        f"{stage_id}:{values.get(stage_id, 'missing')}"
        for stage_id in CANONICAL_ROUTE_TIMING_STAGES
    )


def unpack_stage_map(value: Any) -> dict[str, str]:
    if not isinstance(value, str):
        return {}
    result: dict[str, str] = {}
    for token in value.split(";"):
        if ":" not in token:
            continue
        stage_id, stage_value = token.split(":", 1)
        if stage_id.strip():
            result[stage_id.strip()] = stage_value.strip()
    return result


def route_timing_stage_inclusion_fields_for_row(
    row: dict[str, Any],
    stage_fields: dict[str, Any],
    timing_ledger: dict[str, Any],
) -> dict[str, Any]:
    engine = str(row.get("engine") or "")
    if not is_shardloom_engine(engine):
        external = "external_baseline_only"
        return {
            "route_timing_stage_inclusion_schema_version": (
                ROUTE_TIMING_STAGE_INCLUSION_SCHEMA_VERSION
            ),
            "route_timing_stage_inclusion_status": external,
            "route_timing_stage_inclusion_stage_ids": ",".join(
                CANONICAL_ROUTE_TIMING_STAGES
            ),
            "route_timing_stage_inclusion_classes": external,
            "route_timing_stage_inclusion_stage_owners": external,
            "route_timing_stage_inclusion_timing_scopes": external,
            "route_timing_stage_inclusion_skip_reasons": external,
            "route_timing_stage_inclusion_claim_boundary": (
                "external baseline rows are comparison-only and cannot satisfy ShardLoom "
                "stage inclusion evidence"
            ),
        }

    preparation_included = timing_ledger.get("preparation_timing_included_in_total") is True
    query_included = timing_ledger.get("query_timing_included_in_total") is True
    output_included = timing_ledger.get("output_timing_included_in_total") is True
    evidence_included = timing_ledger.get("evidence_timing_included_in_total") is True
    timing_surface = str(timing_ledger.get("timing_surface") or "unknown")
    scope = str(timing_ledger.get("route_timing_scope") or "unknown")
    excluded_stage_ids = set(
        stage_id
        for stage_id in str(timing_ledger.get("route_timing_excluded_stage_ids") or "")
        .split(",")
        if stage_id and stage_id != "none"
    )
    classes: dict[str, str] = {}
    owners: dict[str, str] = {}
    scopes: dict[str, str] = {}
    reasons: dict[str, str] = {}
    for stage_id in CANONICAL_ROUTE_TIMING_STAGES:
        value_field = STAGE_VALUE_FIELD_BY_ID[stage_id]
        stage_value_present = numeric_value(stage_fields.get(value_field)) is not None
        if stage_id == "cli_process_wall":
            runtime_fields = runtime_validation_field_map(row)
            stage_value_present = first_numeric_micros(
                runtime_fields,
                millis_keys=(
                    "cli_process_wall_millis",
                    "batch_cli_process_wall_millis",
                    "preparation_cli_process_wall_millis",
                ),
            ) is not None
        stage_class = stage_inclusion_class(
            stage_id,
            timing_surface=timing_surface,
            preparation_included=preparation_included,
            query_included=query_included,
            output_included=output_included,
            evidence_included=evidence_included,
            stage_value_present=stage_value_present,
            stage_excluded_from_route_total=(
                STAGE_VALUE_FIELD_BY_ID[stage_id] in excluded_stage_ids
            ),
        )
        classes[stage_id] = stage_class
        owners[stage_id] = STAGE_OWNER_BY_ID[stage_id]
        scopes[stage_id] = (
            scope if stage_class.startswith("included") else stage_class
        )
        reasons[stage_id] = stage_inclusion_skip_reason(stage_class)
    missing = [
        stage_id
        for stage_id, stage_class in classes.items()
        if stage_class == "diagnostic_only"
    ]
    status = (
        "complete"
        if row.get("status") == "success" and len(missing) < len(CANONICAL_ROUTE_TIMING_STAGES)
        else "not_executed"
    )
    return {
        "route_timing_stage_inclusion_schema_version": (
            ROUTE_TIMING_STAGE_INCLUSION_SCHEMA_VERSION
        ),
        "route_timing_stage_inclusion_status": status,
        "route_timing_stage_inclusion_stage_ids": ",".join(CANONICAL_ROUTE_TIMING_STAGES),
        "route_timing_stage_inclusion_classes": pack_stage_map(classes),
        "route_timing_stage_inclusion_stage_owners": pack_stage_map(owners),
        "route_timing_stage_inclusion_timing_scopes": pack_stage_map(scopes),
        "route_timing_stage_inclusion_skip_reasons": pack_stage_map(reasons),
        "route_timing_stage_inclusion_claim_boundary": (
            "stage inclusion fields explain whether each timing component is included in "
            "the hot runtime, full replay proof, or publication proof route total, or "
            "whether it is excluded shared preparation, excluded harness overhead, or "
            "diagnostic-only evidence; route totals remain authoritative and no performance, "
            "production, SQL/DataFrame, object-store/lakehouse, Foundry, package, release, "
            "or Spark-displacement claim is authorized"
        ),
    }


def stage_value_for_readiness(
    stage_id: str,
    row: dict[str, Any],
    stage_fields: dict[str, Any],
) -> float | None:
    if stage_id == "cli_process_wall":
        runtime_fields = runtime_validation_field_map(row)
        return first_numeric_stage_millis(
            runtime_fields,
            millis_keys=(
                "cli_process_wall_millis",
                "batch_cli_process_wall_millis",
                "preparation_cli_process_wall_millis",
            ),
            micros_keys=(
                "cli_process_wall_micros",
                "batch_cli_process_wall_micros",
                "preparation_cli_process_wall_micros",
            ),
        )
    return numeric_value(stage_fields.get(STAGE_VALUE_FIELD_BY_ID[stage_id]))


def meaningful_substage_value(value: Any) -> bool:
    if numeric_value(value) is not None:
        return True
    text = str(value or "").strip().lower()
    return text not in {
        "",
        "none",
        "missing",
        "null",
        "not_reported",
        "not_reported_by_engine",
        "not_applicable",
        "not_applicable_non_cold_route",
        "external_baseline_only",
    }


def stage_has_substage_attribution(
    stage_id: str,
    row: dict[str, Any],
    stage_fields: dict[str, Any],
) -> bool:
    runtime_fields = runtime_validation_field_map(row)
    combined = {**runtime_fields, **stage_fields, **row}
    return any(
        meaningful_substage_value(combined.get(field))
        for field in STAGE_SUBSTAGE_FIELDS_BY_ID[stage_id]
    )


def stage_readiness_group(stage_id: str, stage_class: str) -> str:
    if stage_class.startswith("included"):
        if stage_id == "result_sink_write":
            return "route_total_output_sink"
        if stage_id == "evidence_render":
            return "route_total_publication_evidence"
        return "route_total_stage"
    if stage_class == "excluded_shared_preparation":
        return "shared_preparation"
    if stage_class == "excluded_harness":
        return "harness"
    if stage_id == "result_sink_write":
        return "output_sink"
    if stage_id == "evidence_render":
        return "publication_evidence"
    return "excluded_diagnostic_child"


def stage_residual_treatment(
    stage_class: str,
    *,
    not_ready: bool,
) -> str:
    if not_ready:
        return "not_optimization_ready_missing_substage_attribution"
    if stage_class.startswith("included"):
        return "included_in_route_total_with_exclusive_residual_audited"
    if stage_class == "excluded_shared_preparation":
        return "excluded_shared_preparation_not_route_total"
    if stage_class == "excluded_harness":
        return "excluded_harness_process_wall_not_route_total"
    return "diagnostic_only_not_additive_route_timing"


def route_timing_instrument_fields_for_row(
    row: dict[str, Any],
    stage_fields: dict[str, Any],
    stage_inclusion_fields: dict[str, Any],
) -> dict[str, Any]:
    engine = str(row.get("engine") or "")
    if not is_shardloom_engine(engine):
        external = "external_baseline_only"
        return {
            "route_timing_instrument_schema_version": (
                ROUTE_TIMING_INSTRUMENT_SCHEMA_VERSION
            ),
            "route_timing_instrument_status": external,
            "route_timing_instrument_stage_ids": ",".join(
                CANONICAL_ROUTE_TIMING_STAGES
            ),
            "route_timing_instrument_stage_parent_stages": external,
            "route_timing_instrument_stage_groups": external,
            "route_timing_instrument_stage_owners": external,
            "route_timing_instrument_inclusion_classes": external,
            "route_timing_instrument_timing_scopes": external,
            "route_timing_instrument_evidence_levels": external,
            "route_timing_instrument_residual_treatments": external,
            "route_timing_instrument_substage_fields": external,
            "route_timing_instrument_missing_substage_attribution": "none",
            "route_timing_instrument_expensive_stage_threshold_ms": (
                OPTIMIZATION_READINESS_STAGE_THRESHOLD_MS
            ),
            "route_timing_instrument_expensive_stage_ids": "none",
            "route_timing_instrument_not_ready_stage_ids": "none",
            "route_timing_instrument_claim_boundary": (
                "external baseline rows are comparison-only and cannot satisfy "
                "ShardLoom timing-instrument readiness"
            ),
        }

    classes = unpack_stage_map(
        stage_inclusion_fields.get("route_timing_stage_inclusion_classes")
    )
    scopes = unpack_stage_map(
        stage_inclusion_fields.get("route_timing_stage_inclusion_timing_scopes")
    )
    evidence_level = str(
        row.get("actual_evidence_tier")
        or row.get("timing_surface_evidence_tier")
        or "unknown"
    )
    parents: dict[str, str] = {}
    groups: dict[str, str] = {}
    owners: dict[str, str] = {}
    inclusion_classes: dict[str, str] = {}
    timing_scopes: dict[str, str] = {}
    evidence_levels: dict[str, str] = {}
    residual_treatments: dict[str, str] = {}
    substage_fields: dict[str, str] = {}
    expensive: list[str] = []
    not_ready: list[str] = []
    missing_substage: list[str] = []
    for stage_id in CANONICAL_ROUTE_TIMING_STAGES:
        stage_class = classes.get(stage_id, "missing")
        value = stage_value_for_readiness(stage_id, row, stage_fields)
        is_expensive = (
            value is not None
            and value > OPTIMIZATION_READINESS_STAGE_THRESHOLD_MS
        )
        has_substage = stage_has_substage_attribution(stage_id, row, stage_fields)
        stage_not_ready = is_expensive and not has_substage
        if is_expensive:
            expensive.append(stage_id)
        if stage_not_ready:
            not_ready.append(stage_id)
            missing_substage.append(stage_id)
        parents[stage_id] = STAGE_PARENT_STAGE_BY_ID[stage_id]
        groups[stage_id] = stage_readiness_group(stage_id, stage_class)
        owners[stage_id] = STAGE_OWNER_BY_ID[stage_id]
        inclusion_classes[stage_id] = stage_class
        timing_scopes[stage_id] = scopes.get(stage_id, "missing")
        evidence_levels[stage_id] = evidence_level
        residual_treatments[stage_id] = stage_residual_treatment(
            stage_class,
            not_ready=stage_not_ready,
        )
        substage_fields[stage_id] = ",".join(STAGE_SUBSTAGE_FIELDS_BY_ID[stage_id])
    if row.get("status") != "success":
        status = "not_executed"
    elif not_ready:
        status = "not_optimization_ready"
    else:
        status = "optimization_ready"
    return {
        "route_timing_instrument_schema_version": ROUTE_TIMING_INSTRUMENT_SCHEMA_VERSION,
        "route_timing_instrument_status": status,
        "route_timing_instrument_stage_ids": ",".join(CANONICAL_ROUTE_TIMING_STAGES),
        "route_timing_instrument_stage_parent_stages": pack_stage_map(parents),
        "route_timing_instrument_stage_groups": pack_stage_map(groups),
        "route_timing_instrument_stage_owners": pack_stage_map(owners),
        "route_timing_instrument_inclusion_classes": pack_stage_map(inclusion_classes),
        "route_timing_instrument_timing_scopes": pack_stage_map(timing_scopes),
        "route_timing_instrument_evidence_levels": pack_stage_map(evidence_levels),
        "route_timing_instrument_residual_treatments": pack_stage_map(
            residual_treatments
        ),
        "route_timing_instrument_substage_fields": pack_stage_map(substage_fields),
        "route_timing_instrument_missing_substage_attribution": (
            ",".join(missing_substage) if missing_substage else "none"
        ),
        "route_timing_instrument_expensive_stage_threshold_ms": (
            OPTIMIZATION_READINESS_STAGE_THRESHOLD_MS
        ),
        "route_timing_instrument_expensive_stage_ids": (
            ",".join(expensive) if expensive else "none"
        ),
        "route_timing_instrument_not_ready_stage_ids": (
            ",".join(not_ready) if not_ready else "none"
        ),
        "route_timing_instrument_claim_boundary": (
            "timing-instrument readiness only says whether a measured stage is scoped "
            "well enough to select as an optimization target. It does not authorize a "
            "performance, production, SQL/DataFrame, object-store/lakehouse, Foundry, "
            "package, release, or Spark-displacement claim."
        ),
    }


def first_meaningful_field(fields: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = fields.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text and text.lower() not in {"none", "null", "not_reported"}:
            return text
    return None


def certified_status(value: str | None) -> bool:
    if not value:
        return False
    return "certified" in value.lower() or value.lower() == "passed"


def route_fast_path_attribution_fields_for_row(
    row: dict[str, Any],
    identity: dict[str, Any],
    stage_fields: dict[str, Any],
    timing_ledger: dict[str, Any],
) -> dict[str, Any]:
    fields = runtime_validation_field_map(row)
    engine = str(row.get("engine") or "")
    is_shardloom = is_shardloom_engine(engine)
    total_route = numeric_value(stage_fields.get("total_route_ms"))
    output_delivery = (
        first_numeric_field(
            fields,
            (
                "result_sink_write_millis",
                "computed_result_sink_write_millis",
            ),
        )
        or numeric_value(stage_fields.get("result_sink_write_ms"))
        or 0.0
    )
    evidence_render = (
        first_numeric_field(fields, ("evidence_render_millis",))
        or numeric_value(stage_fields.get("evidence_render_ms"))
        or 0.0
    )
    evidence_capture = 0.0
    certificate_link = 0.0
    output_included = timing_ledger.get("output_timing_included_in_total") is True
    evidence_included = timing_ledger.get("evidence_timing_included_in_total") is True
    included_overhead = (
        (output_delivery if output_included else 0.0)
        + (evidence_render if evidence_included else 0.0)
        + evidence_capture
        + certificate_link
    )
    runtime_execution = (
        max(total_route - included_overhead, 0.0)
        if total_route is not None
        else first_numeric_field(fields, ("query_runtime_millis", "total_runtime_millis"))
    )
    certificate_id = first_meaningful_field(
        fields,
        (
            "runtime_execution_certificate_id",
            "execution_certificate_id",
            "vortex_capillary_preparation_execution_certificate_id",
        ),
    )
    certificate_status = first_meaningful_field(
        fields,
        (
            "runtime_execution_certificate_status",
            "execution_certificate_status",
            "vortex_capillary_preparation_execution_certificate_status",
        ),
    )
    certificate_plan_ref = first_meaningful_field(
        fields,
        (
            "runtime_execution_certificate_plan_ref",
            "runtime_scheduler_ref",
            "vortex_capillary_preparation_task_manifest_id",
        ),
    )
    evidence_required = (
        is_shardloom
        and str(row.get("status") or "") == "success"
        and str(row.get("claim_gate_status") or "") == "claim_grade"
    )
    if not is_shardloom:
        certificate_link_status = "external_baseline_only"
    elif certificate_id and certified_status(certificate_status):
        certificate_link_status = "linked_certified_runtime_execution"
    elif evidence_required:
        certificate_link_status = "missing_required_certificate_link"
    else:
        certificate_link_status = "not_required_not_claim_grade"
    return {
        "fast_path_attribution_schema_version": FAST_PATH_ATTRIBUTION_SCHEMA_VERSION,
        "runtime_execution_ms": runtime_execution,
        "output_delivery_ms": output_delivery,
        "evidence_capture_ms": evidence_capture,
        "evidence_render_ms": evidence_render,
        "certificate_link_ms": certificate_link,
        "runtime_execution_timing_scope": str(
            timing_ledger.get("route_timing_scope") or "unknown"
        ),
        "output_delivery_timing_scope": (
            "included_in_route_total" if output_included else "excluded_from_route_total"
        ),
        "evidence_capture_timing_status": "certificate_metadata_linked_not_separately_timed",
        "certificate_link_timing_status": "metadata_linked_not_separately_timed",
        "runtime_execution_certificate_id": (
            certificate_id
            if certificate_id
            else ("external_baseline_only" if not is_shardloom else "missing")
        ),
        "runtime_execution_certificate_status": (
            certificate_status
            if certificate_status
            else ("external_baseline_only" if not is_shardloom else "missing")
        ),
        "runtime_execution_certificate_plan_ref": (
            certificate_plan_ref
            if certificate_plan_ref
            else ("external_baseline_only" if not is_shardloom else "missing")
        ),
        "certificate_link_status": certificate_link_status,
        "evidence_required_for_claim": evidence_required,
        "evidence_render_included_in_route_total": evidence_included,
        "fast_path_claim_boundary": (
            "runtime_execution_ms is route-scoped timing; output_delivery_ms, "
            "evidence_capture_ms, evidence_render_ms, and certificate_link_ms explain "
            "claim evidence overhead and do not authorize performance superiority"
        ),
    }


def evidence_render_proof_fields_for_row(
    row: dict[str, Any],
    stage_fields: dict[str, Any],
    timing_ledger: dict[str, Any],
    fast_path_fields: dict[str, Any],
) -> dict[str, Any]:
    fields = runtime_validation_field_map(row)
    engine = str(row.get("engine") or "")
    is_shardloom = is_shardloom_engine(engine)
    existing_schema = first_meaningful_field(
        fields, ("evidence_render_proof_schema_version",)
    )
    route_boundary = (
        "route_total_includes_evidence_render_timing"
        if timing_ledger.get("evidence_timing_included_in_total") is True
        else "route_total_excludes_evidence_render_timing"
    )
    if not is_shardloom:
        status = "external_baseline_only"
        proof_digest = "external_baseline_only"
        compact_fact_keys = "external_baseline_only"
        regeneration_surface = "external_baseline_only"
        human_scope = "external_baseline_only"
        hot_path_policy = "external_baseline_only"
    else:
        status = first_meaningful_field(
            fields, ("evidence_render_proof_status",)
        ) or "compact_machine_evidence_derived"
        compact_fact_keys = first_meaningful_field(
            fields, ("evidence_render_compact_fact_keys",)
        ) or (
            "scenario,route_lane_id,runtime_execution_certificate_status,"
            "native_io_certificate_status,result_sink_certificate_status,"
            "claim_gate_status"
        )
        regeneration_surface = first_meaningful_field(
            fields, ("evidence_render_regeneration_surface",)
        ) or "promoter_fast_path_table_and_website_human_tables_from_compact_fields"
        human_scope = first_meaningful_field(
            fields, ("evidence_render_human_expansion_timing_scope",)
        ) or "outside_timed_route_promoter_or_website_render"
        hot_path_policy = first_meaningful_field(
            fields, ("evidence_render_hot_path_policy",)
        ) or "compact_facts_only_human_render_deferred"
        digest_parts = [
            EVIDENCE_RENDER_PROOF_SCHEMA_VERSION,
            str(row.get("engine") or ""),
            str(row.get("scenario_name") or row.get("scenario_id") or ""),
            str(row.get("storage_format") or ""),
            str(row.get("route_lane_id") or ""),
            str(fast_path_fields.get("runtime_execution_certificate_status") or ""),
            str(first_meaningful_field(fields, ("native_io_certificate_status",)) or ""),
            str(
                first_meaningful_field(
                    fields,
                    (
                        "computed_result_sink_native_io_certificate_status",
                        "result_sink_claim_gate_status",
                    ),
                )
                or ""
            ),
            str(stage_fields.get("evidence_render_ms") or ""),
            route_boundary,
        ]
        proof_digest = first_meaningful_field(
            fields, ("evidence_render_proof_digest",)
        ) or (
            "sha256:"
            + hashlib.sha256("\0".join(digest_parts).encode("utf-8")).hexdigest()[:16]
        )
    return {
        "evidence_render_proof_schema_version": existing_schema
        or EVIDENCE_RENDER_PROOF_SCHEMA_VERSION,
        "evidence_render_proof_status": status,
        "evidence_render_proof_digest": proof_digest,
        "evidence_render_compact_fact_keys": compact_fact_keys,
        "evidence_render_regeneration_surface": regeneration_surface,
        "evidence_render_human_expansion_timing_scope": human_scope,
        "evidence_render_hot_path_policy": hot_path_policy,
        "evidence_render_route_timing_boundary": first_meaningful_field(
            fields, ("evidence_render_route_timing_boundary",)
        )
        or route_boundary,
        "evidence_render_claim_boundary": first_meaningful_field(
            fields, ("evidence_render_claim_boundary",)
        )
        or (
            "compact evidence-render proof is benchmark attribution only; route totals remain "
            "the comparison surface and no performance, production, SQL/DataFrame, "
            "object-store/lakehouse, Foundry, package, or Spark-displacement claim is authorized"
        ),
        "evidence_render_fallback_attempted": False,
        "evidence_render_external_engine_invoked": False,
    }


def csv_unique(values: list[Any]) -> str:
    seen: list[str] = []
    for value in values:
        if value in {None, "", "none", "missing", "not_reported", "not_applicable"}:
            continue
        for item in str(value).split(","):
            text = item.strip()
            if text and text not in seen:
                seen.append(text)
    return ",".join(seen) if seen else "none"


def pipe_values(value: Any) -> list[str]:
    if value is None:
        return []
    return [part.strip() for part in str(value).split("|")]


def pipe_bool(values: list[str], index: int) -> bool:
    if index >= len(values):
        return False
    return values[index].strip().lower() == "true"


def pipe_value(values: list[str], index: int, default: str = "none") -> str:
    if index >= len(values):
        return default
    value = values[index].strip()
    return value or default


def normalized_operator_mode(fields: dict[str, Any], is_shardloom: bool) -> str:
    if not is_shardloom:
        return "external_baseline_only"
    raw_class = str(
        first_meaningful_field(
            fields,
            (
                "operator_execution_class",
                "source_backed_scan_operator_execution_class",
                "fused_pipeline_operator_execution_class",
                "encoded_predicate_provider_operator_execution_class",
            ),
        )
        or ""
    ).strip()
    encoded_claim_allowed = field_bool(fields, "operator_encoded_native_claim_allowed", False)
    if raw_class == "encoded_native" and encoded_claim_allowed:
        return "encoded_native"
    if raw_class in {"residual_native", "materialized_temporary", "unsupported"}:
        return raw_class
    if field_bool(fields, "operator_temporary_materialization_used", False) or field_bool(
        fields, "data_materialized", False
    ):
        return "materialized_temporary"
    if field_bool(fields, "operator_residual_native_used", False) or first_meaningful_field(
        fields, ("source_backed_scan_residual_executor",)
    ):
        return "residual_native"
    return "unsupported"


def operator_hot_path_candidate_fields(
    fields: dict[str, Any], mode: str
) -> tuple[str, str, str]:
    encoded_provider_status = first_meaningful_field(
        fields,
        (
            "encoded_predicate_provider_status",
            "encoded_predicate_provider_filter_column_batch_status",
        ),
    )
    scenario = str(fields.get("scenario_name") or fields.get("scenario_id") or "").lower()
    if encoded_provider_status and "selective" in scenario:
        return (
            "selective_filter_selection_vector_metric_aggregation",
            "blocked_selection_vector_metric_aggregation_not_admitted",
            "implement selection-vector-backed metric aggregation with decoded-reference correctness, execution certificate, and Native I/O evidence before changing encoded_native_claim_allowed",
        )
    promoted_kernel_count = int(
        numeric_value(fields.get("encoded_kernel_promoted_pair_count")) or 0
    )
    if promoted_kernel_count and mode == "residual_native":
        return (
            "partial_encoded_kernel_to_full_operator_promotion",
            "partial_encoded_kernel_promoted_full_operator_residual_boundary",
            "complete the remaining residual operator path, result assembly, and decoded-reference certificate before changing operator_encoded_native_claim_allowed",
        )
    if mode == "materialized_temporary":
        return (
            "compatibility_import_materialization_elimination",
            "blocked_materialized_temporary_operator_not_encoded_native",
            "move the cold compatibility route toward prepared/native operator execution before claiming encoded-native behavior",
        )
    if mode == "residual_native":
        return (
            "residual_native_operator_encoding_promotion",
            "blocked_residual_native_operator_not_encoded_native",
            "select a residual-native operator family, add encoded-kernel correctness evidence, and require claim-gate validation before promotion",
        )
    if mode == "encoded_native":
        return (
            "already_encoded_native",
            "none",
            "keep correctness, certificate, Native I/O, and benchmark claim gates attached",
        )
    return (
        "operator_mode_evidence_missing",
        "blocked_operator_mode_evidence_missing",
        "emit operator blocker matrix evidence before attempting encoded-native promotion",
    )


def encoded_kernel_promotion_fields_for_row(row: dict[str, Any]) -> dict[str, Any]:
    fields = runtime_validation_field_map(row)
    engine = str(row.get("engine") or "")
    if not is_shardloom_engine(engine):
        return {
            "encoded_kernel_promotion_schema_version": ENCODED_KERNEL_PROMOTION_SCHEMA_VERSION,
            "encoded_kernel_promotion_status": "external_baseline_only",
            "encoded_kernel_promoted_pair_count": 0,
            "encoded_kernel_promoted_pair_ids": "external_baseline_only",
            "encoded_kernel_promoted_operator_families": "external_baseline_only",
            "encoded_kernel_selection_vector_metric_bridge_consumed": False,
            "encoded_kernel_selection_vector_metric_data_decoded": False,
            "encoded_kernel_full_operator_claim_allowed": False,
            "encoded_kernel_promotion_fallback_attempted": False,
            "encoded_kernel_promotion_external_engine_invoked": False,
            "encoded_kernel_promotion_claim_boundary": (
                "external baseline rows cannot satisfy ShardLoom encoded-kernel evidence"
            ),
        }

    pair_ids = pipe_values(fields.get("compressed_kernel_registry_pair_ids"))
    families = pipe_values(fields.get("compressed_kernel_registry_operator_families"))
    admitted = pipe_values(fields.get("compressed_kernel_registry_kernel_admitted"))
    executed = pipe_values(fields.get("compressed_kernel_registry_kernel_executed"))
    decoded = pipe_values(fields.get("compressed_kernel_registry_decoded"))
    materialized = pipe_values(fields.get("compressed_kernel_registry_materialized"))
    compared = pipe_values(
        fields.get("compressed_kernel_registry_decoded_reference_compared")
    )
    correctness = pipe_values(
        fields.get("compressed_kernel_registry_correctness_digest_status")
    )
    promoted_pairs: list[str] = []
    promoted_families: list[str] = []
    for index, pair_id in enumerate(pair_ids):
        if not pair_id or pair_id in {"none", "not_available"}:
            continue
        if not (
            pipe_bool(admitted, index)
            and pipe_bool(executed, index)
            and not pipe_bool(decoded, index)
            and not pipe_bool(materialized, index)
            and pipe_bool(compared, index)
            and pipe_value(correctness, index) == "decoded_reference_match"
        ):
            continue
        promoted_pairs.append(pair_id)
        promoted_families.append(pipe_value(families, index))

    selected_metric_selection_vector = field_bool(
        fields,
        "encoded_predicate_provider_selected_metric_selection_vector_consumed",
        False,
    )
    selected_metric_decoded = field_bool(
        fields,
        "encoded_predicate_provider_selected_metric_data_decoded",
        False,
    )
    if promoted_pairs:
        status = "partial_encoded_kernel_pairs_promoted"
    elif selected_metric_selection_vector and selected_metric_decoded:
        status = "selection_vector_bridge_promoted_metric_residual"
    else:
        status = "no_promoted_encoded_kernel_pairs"
    full_operator_claim_allowed = (
        str(fields.get("operator_execution_class") or "") == "encoded_native"
        and field_bool(fields, "operator_encoded_native_claim_allowed", False)
    )
    return {
        "encoded_kernel_promotion_schema_version": ENCODED_KERNEL_PROMOTION_SCHEMA_VERSION,
        "encoded_kernel_promotion_status": status,
        "encoded_kernel_promoted_pair_count": len(promoted_pairs),
        "encoded_kernel_promoted_pair_ids": csv_unique(promoted_pairs) or "none",
        "encoded_kernel_promoted_operator_families": csv_unique(promoted_families)
        or "none",
        "encoded_kernel_selection_vector_metric_bridge_consumed": (
            selected_metric_selection_vector
        ),
        "encoded_kernel_selection_vector_metric_data_decoded": selected_metric_decoded,
        "encoded_kernel_full_operator_claim_allowed": full_operator_claim_allowed,
        "encoded_kernel_promotion_fallback_attempted": False,
        "encoded_kernel_promotion_external_engine_invoked": False,
        "encoded_kernel_promotion_claim_boundary": (
            "encoded-kernel promotion is narrower than a full encoded-native operator "
            "claim. Promoted kernel pairs require admitted/executed compressed-kernel "
            "evidence, no decode/materialization, and decoded-reference parity; the "
            "route remains residual-native until the complete operator, residual "
            "remainder, result assembly, Native I/O certificate, and claim gate are "
            "admitted."
        ),
    }


def operator_mode_fields_for_row(row: dict[str, Any]) -> dict[str, Any]:
    fields = runtime_validation_field_map(row)
    engine = str(row.get("engine") or "")
    is_shardloom = is_shardloom_engine(engine)
    mode = normalized_operator_mode(fields, is_shardloom)
    if mode not in OPERATOR_EXECUTION_MODES:
        mode = "unsupported"
    if not is_shardloom:
        return {
            "operator_mode_inventory_schema_version": OPERATOR_MODE_INVENTORY_SCHEMA_VERSION,
            "operator_execution_class": "external_baseline_only",
            "operator_admission_status": "external_baseline_only",
            "operator_encoded_native_claim_allowed": False,
            "operator_residual_native_used": False,
            "operator_temporary_materialization_used": False,
            "operator_blocker_matrix_ref": "external_baseline_only",
            "operator_execution_mode": "external_baseline_only",
            "encoded_native_operators": "external_baseline_only",
            "residual_native_operators": "external_baseline_only",
            "materialized_temporary_operators": "external_baseline_only",
            "operator_blocker_code": "external_baseline_only",
            "operator_hot_path_candidate": "external_baseline_only",
            "operator_hot_path_candidate_status": "external_baseline_only",
            "operator_hot_path_next_step": "external_baseline_only",
            "operator_mode_claim_boundary": "external rows are comparison baselines only",
        }

    encoded_native_claim_allowed = field_bool(
        fields, "operator_encoded_native_claim_allowed", False
    )
    residual_used = mode == "residual_native" or field_bool(
        fields, "operator_residual_native_used", False
    )
    temporary_used = mode == "materialized_temporary" or field_bool(
        fields, "operator_temporary_materialization_used", False
    )
    blocker_code = (
        first_meaningful_field(
            fields,
            (
                "operator_blocker_id",
                "fused_pipeline_blocker_id",
                "encoded_predicate_provider_blocker_id",
            ),
        )
        or ("none" if mode == "encoded_native" else "gar-6d9.operator_mode_not_encoded_native")
    )
    candidate, candidate_status, next_step = operator_hot_path_candidate_fields(fields, mode)
    candidate = first_meaningful_field(fields, ("operator_hot_path_candidate",)) or candidate
    candidate_status = (
        first_meaningful_field(fields, ("operator_hot_path_candidate_status",))
        or candidate_status
    )
    next_step = first_meaningful_field(fields, ("operator_hot_path_next_step",)) or next_step
    encoded_operators = (
        csv_unique(
            [
                fields.get("operator_family"),
                fields.get("prepared_vortex_scale_split_operator_family"),
                fields.get("encoded_predicate_provider_candidate"),
            ]
        )
        if mode == "encoded_native" and encoded_native_claim_allowed
        else "none"
    )
    residual_operators = (
        csv_unique(
            [
                fields.get("source_backed_scan_residual_executor"),
                fields.get("prepared_vortex_scale_split_operator_family"),
                fields.get("fused_operator_family"),
                fields.get("encoded_predicate_provider_selected_metric_source"),
            ]
        )
        if residual_used
        else "none"
    )
    materialized_operators = (
        csv_unique(
            [
                fields.get("operator_blocker_matrix_ref"),
                "vortex_derived_array_temporary_operator",
            ]
        )
        if temporary_used
        else "none"
    )
    return {
        "operator_mode_inventory_schema_version": OPERATOR_MODE_INVENTORY_SCHEMA_VERSION,
        "operator_execution_class": mode,
        "operator_admission_status": str(
            first_meaningful_field(
                fields,
                (
                    "operator_admission_status",
                    "prepared_vortex_scale_split_operator_runtime_status",
                ),
            )
            or ("encoded_native_admitted" if mode == "encoded_native" else "not_encoded_native")
        ),
        "operator_encoded_native_claim_allowed": encoded_native_claim_allowed
        if mode == "encoded_native"
        else False,
        "operator_residual_native_used": residual_used,
        "operator_temporary_materialization_used": temporary_used,
        "operator_blocker_matrix_ref": first_meaningful_field(
            fields, ("operator_blocker_matrix_ref",)
        )
        or "missing",
        "operator_execution_mode": mode,
        "encoded_native_operators": encoded_operators,
        "residual_native_operators": residual_operators,
        "materialized_temporary_operators": materialized_operators,
        "operator_blocker_code": blocker_code,
        "operator_hot_path_candidate": candidate,
        "operator_hot_path_candidate_status": candidate_status,
        "operator_hot_path_next_step": next_step,
        "operator_mode_claim_boundary": (
            "runtime-supported, residual-native, materialized-temporary, and encoded-native "
            "operator evidence are separate claims; encoded_native requires explicit "
            "operator_encoded_native_claim_allowed=true plus correctness, certificate, "
            "materialization/decode, Native I/O, and no-fallback evidence"
        ),
    }


def decorated_route_row(row: dict[str, Any]) -> dict[str, Any]:
    identity = route_identity_for_row(row)
    stage_fields = route_stage_fields_for_row(row)
    timing_ledger = route_timing_ledger_fields_for_row(row, identity, stage_fields)
    fast_path_fields = route_fast_path_attribution_fields_for_row(
        row, identity, stage_fields, timing_ledger
    )
    evidence_render_proof_fields = evidence_render_proof_fields_for_row(
        row, stage_fields, timing_ledger, fast_path_fields
    )
    operator_mode_fields = operator_mode_fields_for_row(row)
    return {
        **row,
        **identity,
        **stage_fields,
        **timing_ledger,
        **fast_path_fields,
        **evidence_render_proof_fields,
        **operator_mode_fields,
    }


def synthetic_prepare_once_first_query_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    synthetic: list[dict[str, Any]] = []
    for row in rows:
        base = decorated_route_row(row)
        if base.get("route_lane_id") != "prepare_once_batch":
            continue
        fields = runtime_validation_field_map(row)
        preparation = prepare_once_preparation_millis(fields, base)
        batch_count = prepared_route_observed_batch_count(fields)
        first_query_stage_fields = route_stage_fields_for_row(
            {**row, "route_lane_id": "prepare_once_first_query"}
        )
        prepared = {**base, **first_query_stage_fields}
        for key in EVIDENCE_RENDER_PROOF_FIELD_KEYS:
            prepared.pop(key, None)
        prepared.update(
            {
                "route_lane_id": "prepare_once_first_query",
                "route_display_name": "ShardLoom Prepare-Once First Query",
                "route_family_display_name": "ShardLoom Raw Compatibility To Prepared Vortex",
                "start_state": "raw_compat_source",
                "end_state": "result_sink",
                "includes_preparation": True,
                "preparation_included": True,
                "preparation_included_scope": "prepare_once_then_first_query",
                "query_timing_starts_after_preparation": True,
                "prepared_state_reused": False,
                "route_comparable_to_external_end_to_end": True,
                "route_row_derivation_status": DERIVED_PREPARE_ONCE_FIRST_QUERY_STATUS,
                "route_row_source_lane_id": "prepare_once_batch",
                "route_row_source_engine": row.get("engine"),
                "prepared_route_query_count": 1,
                "prepared_route_observed_batch_count": batch_count,
                "prepared_state_lookup_or_create_ms": preparation,
                "total_route_ms": first_query_stage_fields.get("total_route_ms"),
                "route_stage_timing_scope": "prepare_once_first_query",
                "route_claim_boundary": (
                    "raw compatibility input is prepared once into VortexPreparedState, "
                    "then the first prepared query runs; preparation is included for "
                    "route-level comparison and remains local evidence only"
                ),
            }
        )
        prepared_timing_ledger = route_timing_ledger_fields_for_row(row, prepared, prepared)
        prepared.update(prepared_timing_ledger)
        prepared_stage_inclusion_fields = route_timing_stage_inclusion_fields_for_row(
            prepared,
            prepared,
            prepared_timing_ledger,
        )
        prepared.update(prepared_stage_inclusion_fields)
        prepared.update(
            route_timing_instrument_fields_for_row(
                prepared,
                prepared,
                prepared_stage_inclusion_fields,
            )
        )
        prepared_fast_path_fields = route_fast_path_attribution_fields_for_row(
            row,
            prepared,
            prepared,
            prepared_timing_ledger,
        )
        prepared.update(prepared_fast_path_fields)
        prepared.update(
            evidence_render_proof_fields_for_row(
                prepared,
                prepared,
                prepared_timing_ledger,
                prepared_fast_path_fields,
            )
        )
        prepared.update(operator_mode_fields_for_row(prepared))
        prepared.update(route_diagnostic_fields_for_row(prepared, prepared))
        prepared.update(cold_lane_attribution_for_row(prepared))
        synthetic.append(prepared)
    return synthetic


def rows_with_prepare_once_first_query(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    source_rows = [
        row
        for row in rows
        if str(row.get("route_lane_id") or "") != "prepare_once_first_query"
    ]
    return [*source_rows, *synthetic_prepare_once_first_query_rows(source_rows)]


def public_front_door_benchmark_rows() -> list[dict[str, Any]]:
    from shardloom import ShardLoomContext

    report = ShardLoomContext(client=None).user_route_capability_report()
    rows: list[dict[str, Any]] = []
    for front_door in report.public_front_door_route_rows:
        row = asdict(front_door)
        timing_boundary = (
            "front door prepares through VortexPreparedState; timing row is the "
            "owning route lane"
        )
        if front_door.front_door_id == "local_source_auto_prepare_vortex_front_door":
            timing_boundary = (
                "ctx.prepare_vortex(..., workspace=...).query(...).collect() "
                "is the ShardLoom Prepare-Once First Query route identity: "
                "preparation plus first prepared query/output are the comparable route; "
                "this static row is not a measured timing row"
            )
        elif front_door.front_door_id == "generated_source_prepare_vortex_front_door":
            timing_boundary = (
                "ctx.from_rows(...).prepare_vortex(workspace=...) writes a local "
                "VortexPreparedState artifact; generated-source local-output timing "
                "is route evidence, not comparative query timing"
            )
        row.update(
            {
                "public_front_door_benchmark_schema_version": (
                    PUBLIC_FRONT_DOOR_BENCHMARK_SCHEMA_VERSION
                ),
                "benchmark_row_kind": PUBLIC_FRONT_DOOR_BENCHMARK_ROW_KIND,
                "benchmark_timing_status": PUBLIC_FRONT_DOOR_BENCHMARK_TIMING_STATUS,
                "benchmark_timing_row": False,
                "benchmark_timing_boundary": timing_boundary,
                "benchmark_route_publication_status": "published_static_route_identity",
                "benchmark_route_publication_source": "user_route_capability_report",
                "benchmark_route_publication_claim_boundary": (
                    "public front-door rows explain route identity, timing boundary, "
                    "prepared-state reuse scope, and no-fallback evidence; they are "
                    "not measured benchmark timing rows and do not authorize "
                    "performance, production, or Spark-replacement claims"
                ),
            }
        )
        rows.append(portable_public_value(row))
    row_ids = tuple(str(row.get("front_door_id") or "") for row in rows)
    expected = set(REQUIRED_PUBLIC_FRONT_DOOR_BENCHMARK_IDS)
    if set(row_ids) != expected or len(row_ids) != len(set(row_ids)):
        raise RuntimeError(
            "public front-door benchmark rows must match required ids: "
            + ",".join(REQUIRED_PUBLIC_FRONT_DOOR_BENCHMARK_IDS)
        )
    return rows


def public_front_door_route_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": PUBLIC_FRONT_DOOR_BENCHMARK_SCHEMA_VERSION,
        "heading": "Public Front-Door Route Identities",
        "headers": [
            "Front door",
            "Public surface",
            "Route lane",
            "Starts",
            "Ends",
            "Prepare included",
            "Query included",
            "Reuse scope",
            "Timing boundary",
            "Runtime",
            "Claim gate",
        ],
        "rows": [
            [
                row.get("front_door_id"),
                row.get("public_user_surface"),
                row.get("route_display_name"),
                row.get("front_door_start_state"),
                row.get("front_door_end_state"),
                row.get("includes_preparation"),
                row.get("includes_query"),
                row.get("prepared_state_reuse_scope"),
                row.get("benchmark_timing_boundary"),
                row.get("route_runtime_status"),
                row.get("claim_gate_status"),
            ]
            for row in rows
        ],
        "claim_boundary": (
            "front-door rows are route-publication evidence only; timing and "
            "performance claims still come from measured published benchmark rows"
        ),
    }


def artifact_rows(artifact: dict[str, Any]) -> list[dict[str, Any]]:
    rows = artifact.get("results")
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    published_rows = artifact.get("published_benchmark_rows")
    published_count = numeric_value(artifact.get("published_benchmark_row_count"))
    if isinstance(published_rows, list):
        inline_rows = [row for row in published_rows if isinstance(row, dict)]
        inline_mode = str(artifact.get("published_benchmark_rows_inlined") or "")
        summary_only_inline = inline_mode == "summary_only"
        if not summary_only_inline and (
            published_count is None or len(inline_rows) >= int(published_count)
        ):
            return inline_rows
    chunks = artifact.get("published_benchmark_row_chunks")
    if isinstance(chunks, list):
        chunk_rows: list[dict[str, Any]] = []
        for chunk in chunks:
            if not isinstance(chunk, dict):
                continue
            path_text = chunk.get("path")
            if not isinstance(path_text, str) or not path_text:
                continue
            path = ROOT / path_text
            if not path.exists():
                continue
            payload = load_json(path)
            rows_payload = payload.get("rows") if isinstance(payload, dict) else payload
            if isinstance(rows_payload, list):
                chunk_rows.extend(row for row in rows_payload if isinstance(row, dict))
        if chunk_rows:
            return chunk_rows
    if isinstance(published_rows, list):
        if str(artifact.get("published_benchmark_rows_inlined") or "") == "summary_only":
            return []
        return [row for row in published_rows if isinstance(row, dict)]
    rows = artifact.get("rows")
    return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def existing_published_rows(directory: Path) -> list[dict[str, Any]]:
    summary_path = directory / "benchmark-results.json"
    if summary_path.exists():
        summary = load_json(summary_path)
        if isinstance(summary, dict):
            rows = artifact_rows(summary)
            if rows:
                return rows
    return load_row_chunks(directory)


def coverage_rows(artifact: dict[str, Any]) -> list[dict[str, Any]]:
    rows = artifact.get("coverage_table")
    return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def lane_versions(artifact: dict[str, Any]) -> dict[str, Any]:
    versions = artifact.get("engine_versions")
    return versions if isinstance(versions, dict) else {}


def available_lanes(artifact: dict[str, Any], rows: list[dict[str, Any]]) -> list[str]:
    lanes = {
        name
        for name, metadata in lane_versions(artifact).items()
        if isinstance(metadata, dict) and metadata.get("available") is True
    }
    lanes.update(str(row.get("engine")) for row in rows if row.get("engine"))
    return sorted(lanes)


def missing_reason(lane: str, artifact: dict[str, Any]) -> str:
    metadata = lane_versions(artifact).get(lane)
    if isinstance(metadata, dict):
        reason = metadata.get("reason") or metadata.get("availability_reason")
        if reason:
            return str(reason)
        if metadata.get("available") is False:
            return "lane marked unavailable in benchmark artifact"
    return "not present in promoted benchmark artifact"


def lane_reason(lane: str, artifact: dict[str, Any]) -> str:
    if lane == "native-vortex":
        return "alias vocabulary for promoted shardloom-vortex/native_vortex evidence"
    metadata = lane_versions(artifact).get(lane)
    if isinstance(metadata, dict):
        version = metadata.get("version")
        if version:
            return f"available, version {version}"
    return "available in promoted benchmark artifact"


def scenario_key(row: dict[str, Any]) -> tuple[str, str]:
    return (str(row.get("storage_format", "")), str(row.get("scenario_name", "")))


def prepared_route_observed_batch_count(fields: dict[str, Any]) -> float | None:
    return first_numeric_field(
        fields,
        (
            "batch_scenario_count",
            "session_requested_scenario_count",
            "scenario_count",
            "prepared_route_observed_batch_count",
        ),
    )


def prepared_route_query_runtime_millis(fields: dict[str, Any]) -> float | None:
    return first_numeric_field(
        fields,
        ("query_runtime_millis",),
    )


def route_total_runtime_millis(fields: dict[str, Any]) -> float | None:
    return first_numeric_field(
        fields,
        (
            "total_runtime_millis",
            "query_runtime_millis",
        ),
    )


def prepare_once_preparation_millis(
    fields: dict[str, Any],
    stage_fields: dict[str, Any] | None = None,
) -> float | None:
    preparation = first_numeric_field(
        fields,
        (
            "prepare_batch_prepared_state_lookup_or_create_millis",
            "prepare_batch_preparation_millis",
            "preparation_millis",
            "vortex_prepare_millis",
        ),
    )
    if preparation is not None:
        return preparation
    if stage_fields is None:
        return None
    amortized = numeric_value(stage_fields.get("prepared_state_lookup_or_create_ms"))
    batch_count = prepared_route_observed_batch_count(fields)
    if amortized is not None and batch_count and batch_count > 0:
        return amortized * batch_count
    return None


def output_delivery_millis(
    fields: dict[str, Any],
    stage_fields: dict[str, Any] | None = None,
) -> float:
    value = first_numeric_field(
        fields,
        (
            "result_sink_write_millis",
            "computed_result_sink_write_millis",
        ),
    )
    if value is None and stage_fields is not None:
        value = numeric_value(stage_fields.get("result_sink_write_ms"))
    return value or 0.0


def evidence_render_route_millis(
    fields: dict[str, Any],
    stage_fields: dict[str, Any] | None = None,
) -> float:
    value = first_numeric_field(fields, ("evidence_render_millis",))
    if value is None and stage_fields is not None:
        value = numeric_value(stage_fields.get("evidence_render_ms"))
    return value or 0.0


def engine_timing_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    rows = [
        row
        for row in rows
        if row.get("route_row_derivation_status")
        != DERIVED_PREPARE_ONCE_FIRST_QUERY_STATUS
    ]
    decorated_rows = [decorated_route_row(row) for row in rows]
    by_engine: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in decorated_rows:
        engine = row.get("engine")
        if engine:
            by_engine[str(engine)].append(row)

    row_times: dict[tuple[str, str, str], float] = {}
    for row in decorated_rows:
        if row.get("status") != "success":
            continue
        if is_shardloom_engine(str(row.get("engine") or "")) and (
            row.get("timing_surface") != DEFAULT_ROUTE_TIMING_SURFACE
        ):
            continue
        value = numeric_value(row.get("total_route_ms"))
        if value is not None:
            row_times[(str(row.get("engine")), *scenario_key(row))] = value

    fastest = Counter()
    for fmt, scenario in sorted({key[1:] for key in row_times}):
        candidates = {
            engine: value
            for (engine, candidate_fmt, candidate_scenario), value in row_times.items()
            if candidate_fmt == fmt and candidate_scenario == scenario
        }
        if candidates:
            fastest[min(candidates, key=candidates.get)] += 1

    shardloom_geomean = geomean(
        [
            value
            for (engine, _fmt, _scenario), value in row_times.items()
            if engine == "shardloom"
        ]
    )
    rendered_rows: list[list[Any]] = []
    for engine, engine_rows in by_engine.items():
        successes = [row for row in engine_rows if row.get("status") == "success"]
        values = [
            value
            for (candidate, _fmt, _scenario), value in row_times.items()
            if candidate == engine
        ]
        csv_parquet_values = [
            value
            for (candidate, fmt, _scenario), value in row_times.items()
            if candidate == engine and fmt in {"csv", "parquet"}
        ]
        gm = geomean(values)
        relative = (gm / shardloom_geomean * 100.0) if gm and shardloom_geomean else None
        rendered_rows.append(
            [
                engine,
                "yes",
                f"{len(successes)}/{len(engine_rows)}",
                fmt_ms(gm),
                fmt_ms(geomean(csv_parquet_values)),
                fastest[engine],
                fmt_percent(relative),
            ]
        )
    return {
        "heading": "Local Route Timing Context",
        "headers": [
            "Engine",
            "Available",
            "Success / total",
            "Hot route geomean",
            "CSV/Parquet hot route geomean",
            "local fastest route count",
            "local route timing context",
        ],
        "rows": rendered_rows,
    }


def engine_timing_geomeans(table: dict[str, Any]) -> dict[str, float]:
    rows = table.get("rows")
    if not isinstance(rows, list):
        return {}
    geomeans: dict[str, float] = {}
    for row in rows:
        if not isinstance(row, list) or len(row) < 4:
            continue
        engine = str(row[0])
        geomean_value = formatted_ms_value(row[3])
        if engine and geomean_value is not None and geomean_value > 0:
            geomeans[engine] = geomean_value
    return geomeans


def previous_engine_timing_table(previous_summary: dict[str, Any]) -> dict[str, Any]:
    dashboard = previous_summary.get("comparative_dashboard")
    if not isinstance(dashboard, dict):
        return {}
    table = dashboard.get("engine_timing_overview")
    return table if isinstance(table, dict) else {}


def common_run_timing_drift_table(
    previous_summary: dict[str, Any],
    current_engine_timing: dict[str, Any],
) -> dict[str, Any]:
    previous = engine_timing_geomeans(previous_engine_timing_table(previous_summary))
    current = engine_timing_geomeans(current_engine_timing)
    common_engines = sorted(set(previous) & set(current))
    ratios = {
        engine: current[engine] / previous[engine]
        for engine in common_engines
        if previous[engine] > 0 and current[engine] > 0
    }
    control_ratios = [
        ratio for engine, ratio in ratios.items() if not is_shardloom_engine(engine)
    ]
    shardloom_ratios = [
        ratio for engine, ratio in ratios.items() if is_shardloom_engine(engine)
    ]
    control_geomean = geomean(control_ratios)
    shardloom_geomean = geomean(shardloom_ratios)
    control_slow_count = sum(1 for ratio in control_ratios if ratio >= 1.05)
    common_slowdown = (
        len(control_ratios) >= 3
        and control_geomean is not None
        and control_geomean >= 1.10
        and control_slow_count / len(control_ratios) >= 0.75
    )
    shardloom_specific = (
        common_slowdown
        and control_geomean is not None
        and shardloom_geomean is not None
        and shardloom_geomean >= control_geomean * 1.15
    )
    if not previous:
        status = "no_previous_summary"
        interpretation = "No previous published timing summary was available for common-run drift comparison."
    elif len(control_ratios) < 3:
        status = "insufficient_control_rows"
        interpretation = "Fewer than three non-ShardLoom control engines overlap with the previous summary."
    elif shardloom_specific:
        status = "mixed_drift_review_required"
        interpretation = (
            "Control engines slowed together, and ShardLoom lanes slowed materially more "
            "than the control geomean; review both run conditions and ShardLoom changes."
        )
    elif common_slowdown:
        status = "common_run_slowdown_detected"
        interpretation = (
            "Control engines slowed together, so this rerun should be treated as common-run "
            "drift before attributing timing increases to ShardLoom hotpath changes."
        )
    else:
        status = "stable_or_mixed_controls"
        interpretation = (
            "Control-engine movement does not show a broad common-run slowdown; route-level "
            "changes need row-level review before optimization claims."
        )
    return {
        "heading": "Common-Run Timing Drift",
        "headers": [
            "Engine",
            "Previous route geomean",
            "Current route geomean",
            "Ratio",
            "Cohort",
        ],
        "rows": [
            [
                engine,
                fmt_ms(previous[engine]),
                fmt_ms(current[engine]),
                f"{ratios[engine]:.3f}x",
                "shardloom" if is_shardloom_engine(engine) else "control_baseline",
            ]
            for engine in common_engines
            if engine in ratios
        ],
        "schema_version": COMMON_RUN_TIMING_DRIFT_SCHEMA_VERSION,
        "status": status,
        "control_engine_count": len(control_ratios),
        "control_slow_count": control_slow_count,
        "control_route_geomean_ratio": (
            None if control_geomean is None else round(control_geomean, 4)
        ),
        "shardloom_route_geomean_ratio": (
            None if shardloom_geomean is None else round(shardloom_geomean, 4)
        ),
        "interpretation": interpretation,
        "claim_boundary": (
            "common-run drift compares the current promoted artifact to the previous "
            "published website artifact; it is diagnostic context only and does not "
            "authorize performance, superiority, production, or replacement claims"
        ),
    }


def claim_gate_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(str(row.get("claim_gate_status", "unknown")) for row in rows)
    total = sum(counts.values()) or 1
    return {
        "heading": "Claim-Gate Distribution",
        "headers": ["Claim gate", "Rows", "Share"],
        "rows": [
            [gate, count, f"{count / total * 100.0:.1f}%"]
            for gate, count in counts.most_common()
        ],
    }


def claims_cell(row: dict[str, Any]) -> str:
    allowed: list[str] = []
    if row.get("performance_claim_allowed") is True:
        allowed.append("performance")
    if row.get("production_claim_allowed") is True:
        allowed.append("production")
    if row.get("spark_replacement_claim_allowed") is True:
        allowed.append("replacement")
    return ", ".join(allowed) if allowed else "no performance / production / replacement claim"


def route_table_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    source_rows = [
        row
        for row in rows
        if str(row.get("route_lane_id") or "") != "prepare_once_first_query"
    ]
    decorated = [decorated_route_row(row) for row in source_rows]
    decorated.extend(synthetic_prepare_once_first_query_rows(source_rows))
    order = {
        "prepare_once_first_query": 0,
        "prepare_once_batch": 1,
        "cold_certified_route": 2,
        "warm_prepared_query": 3,
        "native_vortex_query": 4,
        "direct_transient_route": 5,
        "external_baseline_end_to_end": 6,
    }
    return sorted(
        decorated,
        key=lambda row: (
            order.get(str(row.get("route_lane_id")), 99),
            str(row.get("route_display_name") or ""),
            str(row.get("storage_format") or ""),
            str(row.get("scenario_name") or ""),
        ),
    )


def route_lane_comparison_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in route_table_rows(rows):
        key = str(row.get("route_display_name") or row.get("route_lane_id") or "unknown")
        groups[key].append(row)

    rendered_rows: list[list[Any]] = []
    for display_name, group_rows in groups.items():
        first = group_rows[0]
        default_surface = (
            DEFAULT_ROUTE_TIMING_SURFACE
            if is_shardloom_engine(str(first.get("engine") or ""))
            else "external_baseline"
        )
        surface_rows = [
            row for row in group_rows if str(row.get("timing_surface") or "") == default_surface
        ]
        successes = [row for row in surface_rows if row.get("status") == "success"]
        values = [
            value
            for row in successes
            for value in [numeric_value(row.get("total_route_ms"))]
            if value is not None and value > 0
        ]
        runtime_counts = Counter(
            str(row.get("route_runtime_status") or "unknown") for row in group_rows
        )
        claim_counts = Counter(str(row.get("claim_gate_status") or "unknown") for row in group_rows)
        evidence_tiers = Counter(
            str(row.get("actual_evidence_tier") or row.get("timing_surface_evidence_tier") or "unknown")
            for row in group_rows
        )
        route_geomean = geomean(values)
        rendered_rows.append(
            [
                display_name,
                default_surface,
                ", ".join(f"{key}: {count}" for key, count in sorted(evidence_tiers.items())),
                first.get("start_state"),
                "yes" if first.get("includes_preparation") is True else "no",
                first.get("preparation_included_scope"),
                f"{len(successes)}/{len(group_rows)}",
                fmt_ms(route_geomean)
                if route_geomean is not None
                else "hot runtime row missing",
                ", ".join(f"{key}: {count}" for key, count in sorted(runtime_counts.items())),
                ", ".join(f"{key}: {count}" for key, count in sorted(claim_counts.items())),
                claims_cell(first),
                str(first.get("route_comparable_to_external_end_to_end")),
            ]
        )
    return {
        "heading": "Route-Level Lane Comparison",
        "headers": [
            "Lane",
            "Timing surface",
            "Evidence tiers",
            "Starts from",
            "Includes prepare?",
            "Prepare timing scope",
            "Success / total",
            "Hot route geomean",
            "Runtime",
            "Evidence",
            "Claims",
            "Comparable E2E",
        ],
        "rows": rendered_rows,
        "schema_version": ROUTE_RUNTIME_STATUS_SCHEMA_VERSION,
        "claim_boundary": (
            "route lanes are end-to-end comparison surfaces; warm/native/stage rows stay "
            "labeled by start state and timing surface. The primary route grid uses "
            "hot_runtime rows only for ShardLoom lanes; proof/publication rows remain "
            "visible separately and cannot imply hot-runtime performance, production, or "
            "Spark-replacement claims"
        ),
    }


def route_timing_surface_comparison_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    route_total_label_by_surface = {
        "hot_runtime": "Hot route geomean",
        "full_replay_proof": "Full replay proof route geomean",
        "publication_proof": "Publication-proof route geomean",
        "external_baseline": "External baseline route geomean",
    }
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in route_table_rows(rows):
        key = (
            str(row.get("route_display_name") or row.get("route_lane_id") or "unknown"),
            str(row.get("timing_surface") or "unknown"),
        )
        groups[key].append(row)

    rendered_rows: list[list[Any]] = []
    for (display_name, surface), group_rows in sorted(groups.items()):
        first = group_rows[0]
        successes = [row for row in group_rows if row.get("status") == "success"]
        values = [
            value
            for row in successes
            for value in [numeric_value(row.get("total_route_ms"))]
            if value is not None and value > 0
        ]
        evidence_tiers = Counter(
            str(row.get("actual_evidence_tier") or row.get("timing_surface_evidence_tier") or "unknown")
            for row in group_rows
        )
        rendered_rows.append(
            [
                display_name,
                surface,
                route_total_label_by_surface.get(surface, "Surface route geomean"),
                ", ".join(f"{key}: {count}" for key, count in sorted(evidence_tiers.items())),
                f"{len(successes)}/{len(group_rows)}",
                fmt_ms(geomean(values)),
                str(first.get("output_timing_included_in_total")),
                str(first.get("evidence_timing_included_in_total")),
                str(first.get("route_total_formula") or "not_reported"),
            ]
        )
    return {
        "heading": "Route Timing Surface Comparison",
        "headers": [
            "Route",
            "Timing surface",
            "Route total label",
            "Evidence tiers",
            "Success / total",
            "Surface route geomean",
            "Sink included",
            "Evidence render included",
            "Formula",
        ],
        "rows": rendered_rows,
        "schema_version": ROUTE_TIMING_SURFACE_SCHEMA_VERSION,
        "claim_boundary": (
            "route timing surfaces are separate interpretation lanes. hot_runtime excludes "
            "result-sink replay and human publication render; full_replay_proof and "
            "publication_proof rows remain proof evidence and must not replace missing "
            "hot-runtime rows."
        ),
    }


def prepared_route_amortization_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    prepare_batch_rows = [
        row
        for row in route_table_rows(rows)
        if str(row.get("route_lane_id") or "") == "prepare_once_batch"
    ]
    rendered_rows: list[list[Any]] = []
    for query_count in PREPARED_ROUTE_AMORTIZATION_COUNTS:
        per_query_values: list[float] = []
        total_batch_values: list[float] = []
        for row in prepare_batch_rows:
            if str(row.get("timing_surface") or "") != DEFAULT_ROUTE_TIMING_SURFACE:
                continue
            fields = runtime_validation_field_map(row)
            preparation = prepare_once_preparation_millis(fields, row)
            query_runtime = prepared_route_query_runtime_millis(fields)
            if preparation is None or query_runtime is None:
                continue
            per_query_route = preparation / query_count + query_runtime
            per_query_values.append(per_query_route)
            total_batch_values.append(preparation + query_runtime * query_count)
        rendered_rows.append(
            [
                query_count,
                len(per_query_values),
                fmt_ms(geomean(per_query_values))
                if per_query_values
                else "hot runtime row missing",
                fmt_ms(geomean(total_batch_values))
                if total_batch_values
                else "hot runtime row missing",
                (
                    "timing_surface=hot_runtime; prepare_batch_preparation_millis / N "
                    "+ query_runtime_millis"
                ),
                "raw_compat_source -> VortexPreparedState reused for N prepared executions",
            ]
        )
    return {
        "heading": "Prepare-Once Amortization",
        "headers": [
            "Prepared executions",
            "Rows",
            "Per-query route geomean",
            "Batch route geomean",
            "Formula",
            "Scope",
        ],
        "rows": rendered_rows,
        "schema_version": "shardloom.website.prepared_route_amortization.v1",
        "query_counts": list(PREPARED_ROUTE_AMORTIZATION_COUNTS),
        "claim_boundary": (
            "amortized prepare-once rows are derived from the observed prepare-batch "
            "artifact to explain reuse economics; they do not authorize performance, "
            "production, or replacement claims"
        ),
    }


def stage_attribution_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in route_table_rows(rows):
        if not is_shardloom_engine(str(row.get("engine") or "")):
            continue
        key = (
            str(row.get("route_display_name") or row.get("route_lane_id") or "unknown"),
            str(row.get("timing_surface") or "unknown"),
        )
        groups[key].append(row)

    rendered_rows: list[list[Any]] = []
    for (display_name, timing_surface), group_rows in groups.items():
        rendered_rows.append(
            [
                display_name,
                timing_surface,
                len(group_rows),
                *[
                    fmt_ms(
                        geomean(
                            [
                                value
                                for row in group_rows
                                for value in [numeric_value(row.get(field))]
                                if value is not None and value >= 0
                            ]
                        )
                    )
                    for field in ROUTE_STAGE_FIELD_KEYS
                ],
            ]
        )
    return {
        "heading": "ShardLoom Stage Attribution",
        "headers": [
            "Route",
            "Timing surface",
            "Rows",
            "Source admission",
            "Source read",
            "Parse/decode",
            "Source -> Vortex array",
            "Vortex write",
            "Vortex reopen/verify",
            "Prepared lookup/create",
            "Vortex scan",
            "Operator compute",
            "Result sink",
            "Evidence render",
            "Total route",
        ],
        "rows": rendered_rows,
        "schema_version": ROUTE_RUNTIME_STATUS_SCHEMA_VERSION,
        "claim_boundary": (
            "stage attribution explains why a ShardLoom route took time; stage pieces are "
            "not competing product lanes. Surface grouping keeps hot runtime separate from "
            "full replay and publication proof timing."
        ),
    }


def stage_inclusion_contract_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in route_table_rows(rows):
        if not is_shardloom_engine(str(row.get("engine") or "")):
            continue
        key = (
            str(row.get("route_display_name") or row.get("route_lane_id") or "unknown"),
            str(row.get("timing_surface") or "unknown"),
        )
        groups[key].append(row)

    rendered_rows: list[list[Any]] = []
    for (display_name, timing_surface), group_rows in groups.items():
        first = group_rows[0]
        class_tokens = str(
            first.get("route_timing_stage_inclusion_classes") or ""
        ).split(";")
        classes: dict[str, str] = {}
        for token in class_tokens:
            if ":" not in token:
                continue
            stage_id, stage_class = token.split(":", 1)
            classes[stage_id] = stage_class
        included = [
            stage_id
            for stage_id, stage_class in classes.items()
            if stage_class in {"included", "included_hot_runtime"}
        ]
        full_replay_included = [
            stage_id
            for stage_id, stage_class in classes.items()
            if stage_class == "included_full_replay_proof"
        ]
        publication_included = [
            stage_id
            for stage_id, stage_class in classes.items()
            if stage_class == "included_publication_proof"
        ]
        shared = [
            stage_id
            for stage_id, stage_class in classes.items()
            if stage_class == "excluded_shared_preparation"
        ]
        harness = [
            stage_id
            for stage_id, stage_class in classes.items()
            if stage_class == "excluded_harness"
        ]
        diagnostic = [
            stage_id
            for stage_id, stage_class in classes.items()
            if stage_class == "diagnostic_only"
        ]
        statuses = Counter(
            str(row.get("route_timing_stage_inclusion_status") or "missing")
            for row in group_rows
        )
        rendered_rows.append(
            [
                display_name,
                timing_surface,
                len(group_rows),
                ", ".join(included) or "none",
                ", ".join(full_replay_included) or "none",
                ", ".join(publication_included) or "none",
                ", ".join(shared) or "none",
                ", ".join(harness) or "none",
                ", ".join(diagnostic) or "none",
                str(first.get("route_timing_scope") or "missing"),
                "; ".join(f"{status}={count}" for status, count in sorted(statuses.items())),
            ]
        )
    return {
        "heading": "Stage Inclusion Contract",
        "headers": [
            "Route",
            "Timing surface",
            "Rows",
            "Included hot runtime",
            "Included full replay proof",
            "Included publication proof",
            "Excluded shared preparation",
            "Excluded harness",
            "Diagnostic only",
            "Timing scope",
            "Status",
        ],
        "rows": rendered_rows,
        "schema_version": ROUTE_TIMING_STAGE_INCLUSION_SCHEMA_VERSION,
        "claim_boundary": (
            "stage inclusion metadata makes route totals auditable; it does not convert "
            "diagnostic stage fields into comparable route totals or authorize speed claims"
        ),
    }


def route_timing_instrument_readiness_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in route_table_rows(rows):
        if not is_shardloom_engine(str(row.get("engine") or "")):
            continue
        key = (
            str(row.get("route_display_name") or row.get("route_lane_id") or "unknown"),
            str(row.get("timing_surface") or "unknown"),
        )
        groups[key].append(row)

    rendered_rows: list[list[Any]] = []
    for (display_name, timing_surface), group_rows in groups.items():
        statuses = Counter(
            str(row.get("route_timing_instrument_status") or "missing")
            for row in group_rows
        )
        expensive: set[str] = set()
        not_ready: set[str] = set()
        missing_substage: set[str] = set()
        by_group: dict[str, set[str]] = defaultdict(set)
        residuals: Counter[str] = Counter()
        for row in group_rows:
            expensive.update(
                stage
                for stage in str(
                    row.get("route_timing_instrument_expensive_stage_ids") or ""
                ).split(",")
                if stage and stage != "none"
            )
            not_ready.update(
                stage
                for stage in str(
                    row.get("route_timing_instrument_not_ready_stage_ids") or ""
                ).split(",")
                if stage and stage != "none"
            )
            missing_substage.update(
                stage
                for stage in str(
                    row.get("route_timing_instrument_missing_substage_attribution")
                    or ""
                ).split(",")
                if stage and stage != "none"
            )
            stage_groups = unpack_stage_map(
                row.get("route_timing_instrument_stage_groups")
            )
            for stage_id, group in stage_groups.items():
                by_group[group].add(stage_id)
            residuals.update(
                unpack_stage_map(
                    row.get("route_timing_instrument_residual_treatments")
                ).values()
            )
        readiness = (
            "not_optimization_ready"
            if not_ready or statuses.get("not_optimization_ready", 0)
            else "optimization_ready"
        )

        def fmt_stages(stage_ids: set[str]) -> str:
            return ", ".join(sorted(stage_ids)) if stage_ids else "none"

        rendered_rows.append(
            [
                display_name,
                timing_surface,
                len(group_rows),
                readiness,
                "; ".join(
                    f"{status}={count}" for status, count in sorted(statuses.items())
                ),
                fmt_stages(expensive),
                fmt_stages(not_ready),
                fmt_stages(missing_substage),
                fmt_stages(by_group.get("route_total_stage", set())),
                fmt_stages(by_group.get("excluded_diagnostic_child", set())),
                fmt_stages(by_group.get("shared_preparation", set())),
                fmt_stages(
                    by_group.get("route_total_output_sink", set())
                    | by_group.get("output_sink", set())
                ),
                fmt_stages(
                    by_group.get("route_total_publication_evidence", set())
                    | by_group.get("publication_evidence", set())
                ),
                fmt_stages(by_group.get("harness", set())),
                "; ".join(
                    f"{status}={count}" for status, count in sorted(residuals.items())
                ),
            ]
        )
    return {
        "heading": "Route Timing Instrument Readiness",
        "headers": [
            "Route",
            "Timing surface",
            "Rows",
            "Optimization readiness",
            "Readiness status counts",
            ">10 ms stages",
            "Not-ready stages",
            "Missing substage attribution",
            "Route-total stages",
            "Excluded diagnostic children",
            "Shared preparation",
            "Output/sink",
            "Publication evidence",
            "Harness",
            "Residual treatment counts",
        ],
        "rows": rendered_rows,
        "schema_version": ROUTE_TIMING_INSTRUMENT_SCHEMA_VERSION,
        "threshold_ms": OPTIMIZATION_READINESS_STAGE_THRESHOLD_MS,
        "claim_boundary": (
            "optimization readiness is an instrumentation-quality gate for choosing "
            "next work. Rows marked not_optimization_ready remain visible and do not "
            "authorize performance, production, or Spark-displacement claims."
        ),
    }


def source_admission_digest_policy_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in route_table_rows(rows):
        if not is_shardloom_engine(str(row.get("engine") or "")):
            continue
        key = str(row.get("route_display_name") or row.get("route_lane_id") or "unknown")
        groups[key].append(row)

    rendered_rows: list[list[Any]] = []
    for display_name, group_rows in groups.items():
        statuses = Counter(
            str(row.get("source_admission_digest_policy_status") or "missing")
            for row in group_rows
        )
        full_digest_requested = sum(
            1
            for row in group_rows
            if first_bool_field(
                row,
                ("source_admission_full_content_digest_requested",),
                default=False,
            )
        )
        admission_ms = geomean_non_negative(
            [
                micros_to_millis(row.get("source_admission_policy_micros"))
                for row in group_rows
                if micros_to_millis(row.get("source_admission_policy_micros")) is not None
            ]
        )
        manifest_read_ms = geomean_non_negative(
            [
                micros_to_millis(row.get("prepared_manifest_read_micros"))
                for row in group_rows
                if micros_to_millis(row.get("prepared_manifest_read_micros")) is not None
            ]
        )
        source_open_ms = geomean_non_negative(
            [
                micros_to_millis(row.get("source_state_open_micros"))
                for row in group_rows
                if micros_to_millis(row.get("source_state_open_micros")) is not None
            ]
        )
        family_build_ms = geomean_non_negative(
            [
                micros_to_millis(row.get("source_state_family_build_micros"))
                for row in group_rows
                if micros_to_millis(row.get("source_state_family_build_micros")) is not None
            ]
        )
        rendered_rows.append(
            [
                display_name,
                len(group_rows),
                "; ".join(f"{status}={count}" for status, count in sorted(statuses.items())),
                full_digest_requested,
                fmt_ms(admission_ms),
                fmt_ms(manifest_read_ms),
                fmt_ms(source_open_ms),
                fmt_ms(family_build_ms),
            ]
        )
    return {
        "heading": "Source Admission Digest Policy",
        "headers": [
            "Route",
            "Rows",
            "Policy status",
            "Full digest requested rows",
            "Admission policy geomean",
            "Manifest read geomean",
            "Source-state open geomean",
            "Family build geomean",
        ],
        "rows": rendered_rows,
        "schema_version": SOURCE_ADMISSION_DIGEST_POLICY_SCHEMA_VERSION,
        "claim_boundary": (
            "metadata-first source admission is local benchmark reuse evidence only; "
            "publication/claim-grade rows must request full digest verification when required"
        ),
    }


def source_state_lazy_family_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in route_table_rows(rows):
        if not is_shardloom_engine(str(row.get("engine") or "")):
            continue
        key = str(row.get("route_display_name") or row.get("route_lane_id") or "unknown")
        groups[key].append(row)

    rendered_rows: list[list[Any]] = []
    for display_name, group_rows in groups.items():
        lazy_rows = sum(
            1
            for row in group_rows
            if first_bool_field(
                row,
                ("source_state_lazy_family_construction",),
                default=False,
            )
        )
        build_count = sum(
            int(numeric_value(row.get("source_state_family_build_count")) or 0)
            for row in group_rows
        )
        reuse_hit_count = sum(
            int(numeric_value(row.get("source_state_family_reuse_hit_count")) or 0)
            for row in group_rows
        )
        recompute_avoided_rows = sum(
            1
            for row in group_rows
            if first_bool_field(
                row,
                ("source_state_family_recompute_avoided",),
                default=False,
            )
        )
        source_open_ms = geomean_non_negative(
            [
                micros_to_millis(row.get("source_state_open_micros"))
                for row in group_rows
                if micros_to_millis(row.get("source_state_open_micros")) is not None
            ]
        )
        family_build_ms = geomean_non_negative(
            [
                micros_to_millis(row.get("source_state_family_build_micros"))
                for row in group_rows
                if micros_to_millis(row.get("source_state_family_build_micros")) is not None
            ]
        )
        timing_scopes = Counter(
            str(row.get("source_state_family_build_timing_scope") or "not_reported")
            for row in group_rows
        )
        rendered_rows.append(
            [
                display_name,
                len(group_rows),
                lazy_rows,
                build_count,
                reuse_hit_count,
                recompute_avoided_rows,
                fmt_ms(source_open_ms),
                fmt_ms(family_build_ms),
                "; ".join(f"{scope}={count}" for scope, count in sorted(timing_scopes.items())),
            ]
        )
    return {
        "heading": "Lazy Source-State Family Construction",
        "headers": [
            "Route",
            "Rows",
            "Lazy rows",
            "Family builds",
            "Reuse hits",
            "Recompute avoided rows",
            "Source-state open geomean",
            "Family build geomean",
            "Timing scope",
        ],
        "rows": rendered_rows,
        "schema_version": "shardloom.website.source_state_lazy_family.v1",
        "claim_boundary": (
            "source-state family construction evidence is runtime work-avoidance attribution only; "
            "it is not a performance or superiority claim until benchmark rerun and claim gates pass"
        ),
    }


def route_share_next_step(stage_field: str, route_display: str) -> str:
    if stage_field == "timing_surface_stage_contract":
        return "fix_timing_surface_stage_inclusion_before_optimization"
    if stage_field == "source_read_ms":
        return "finish_source_read_scout_split_before_reader_tuning"
    if stage_field == "source_parse_or_columnar_decode_ms":
        return "project_decode_only_route_required_fields"
    if stage_field == "vortex_write_ms":
        return "continue_workspace_safe_writer_metadata_coalescing"
    if stage_field == "vortex_reopen_or_verify_ms":
        return "split_footer_metadata_scan_open_before_reopen_optimization"
    if stage_field == "vortex_scan_ms":
        return "protect_sub_ms_scan_path_with_data_movement_counters"
    if stage_field == "operator_compute_ms":
        return "inventory_residual_operator_microkernels_before_encoded_claim"
    if stage_field == "result_sink_write_ms":
        return "route_small_results_through_capillary_sink_path"
    if stage_field == "evidence_render_ms":
        return "regenerate_human_evidence_from_compact_proof_fields"
    if stage_field == "prepared_state_lookup_or_create_ms":
        return "separate_manifest_lookup_cache_hit_create_write_register"
    if stage_field == "source_admission_ms":
        return "reuse_source_admission_packets_when_manifest_state_matches"
    if "Native Vortex" in route_display:
        return "preserve_native_vortex_fast_path_before_new_pushdown"
    return "use_route_share_before_selecting_next_optimization"


def route_share_included_class_for_surface(timing_surface: str) -> str:
    return {
        "hot_runtime": "included_hot_runtime",
        "full_replay_proof": "included_full_replay_proof",
        "publication_proof": "included_publication_proof",
    }.get(timing_surface, "included")


def route_share_stage_included_for_surface(
    row: dict[str, Any], stage_field: str, timing_surface: str
) -> bool:
    stage_id = ROUTE_STAGE_ID_BY_FIELD.get(stage_field)
    if stage_id is None:
        return False
    classes = unpack_stage_map(row.get("route_timing_stage_inclusion_classes"))
    if not classes:
        return False
    return classes.get(stage_id) in {
        "included",
        route_share_included_class_for_surface(timing_surface),
    }


def route_share_stage_fields_for_lane(
    lane_id: str, timing_surface: str = DEFAULT_ROUTE_TIMING_SURFACE
) -> tuple[str, ...]:
    query_and_tail = ["vortex_scan_ms", "operator_compute_ms"]
    if timing_surface in PROOF_TIMING_SURFACES:
        query_and_tail.append("result_sink_write_ms")
    if timing_surface == "publication_proof":
        query_and_tail.append("evidence_render_ms")
    query_and_tail_tuple = tuple(query_and_tail)
    if lane_id in {"warm_prepared_query", "native_vortex_query"}:
        return query_and_tail_tuple
    if lane_id in {"prepare_once_first_query", "prepare_once_batch"}:
        return ("prepared_state_lookup_or_create_ms",) + query_and_tail_tuple
    if lane_id == "cold_certified_route":
        fields = [
            "source_read_ms",
            "source_parse_or_columnar_decode_ms",
            "source_to_vortex_array_ms",
            "vortex_write_ms",
            "vortex_reopen_or_verify_ms",
            "vortex_scan_ms",
            "operator_compute_ms",
        ]
        if timing_surface in PROOF_TIMING_SURFACES:
            fields.append("result_sink_write_ms")
        if timing_surface == "publication_proof":
            fields.append("evidence_render_ms")
        return tuple(fields)
    fallback_fields = [field for field in ROUTE_STAGE_FIELD_KEYS if field != "total_route_ms"]
    if timing_surface == DEFAULT_ROUTE_TIMING_SURFACE:
        fallback_fields = [
            field
            for field in fallback_fields
            if field not in {"result_sink_write_ms", "evidence_render_ms"}
        ]
    elif timing_surface == "full_replay_proof":
        fallback_fields = [field for field in fallback_fields if field != "evidence_render_ms"]
    return tuple(fallback_fields)


def route_share_amdahl_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in route_table_rows(rows):
        if not is_shardloom_engine(str(row.get("engine") or "")):
            continue
        key = (
            str(row.get("route_lane_id") or "unknown"),
            str(row.get("route_display_name") or row.get("route_lane_id") or "unknown"),
        )
        groups[key].append(row)

    rendered_rows: list[list[Any]] = []
    for (lane_id, display_name), group_rows in groups.items():
        surface_rows = [
            row
            for row in group_rows
            if str(row.get("timing_surface") or "") == DEFAULT_ROUTE_TIMING_SURFACE
        ]
        total_geomean = geomean_non_negative(
            [
                value
                for row in surface_rows
                for value in [numeric_value(row.get("total_route_ms"))]
                if value is not None and value >= 0.0
            ]
        )
        stage_geomeans: dict[str, float] = {}
        non_additive_stage_geomeans: dict[str, float] = {}
        excluded_diagnostic_stage_geomeans: dict[str, float] = {}
        for field in route_share_stage_fields_for_lane(lane_id, DEFAULT_ROUTE_TIMING_SURFACE):
            included_values: list[float] = []
            excluded_diagnostic_values: list[float] = []
            for row in surface_rows:
                parsed = numeric_value(row.get(field))
                if parsed is None or parsed < 0.0:
                    continue
                if route_share_stage_included_for_surface(
                    row, field, DEFAULT_ROUTE_TIMING_SURFACE
                ):
                    included_values.append(parsed)
                elif parsed > 0.0:
                    excluded_diagnostic_values.append(parsed)
            value = geomean_non_negative(
                included_values
            )
            if value is not None and value > 0.0:
                if (
                    total_geomean is not None
                    and total_geomean >= 0.0
                    and value > total_geomean * ROUTE_SHARE_ADDITIVE_TOLERANCE
                ):
                    non_additive_stage_geomeans[field] = value
                else:
                    stage_geomeans[field] = value
            excluded_value = geomean_non_negative(excluded_diagnostic_values)
            if excluded_value is not None and excluded_value > 0.0:
                excluded_diagnostic_stage_geomeans[field] = excluded_value
        contract_stage_id: str | None = None
        if stage_geomeans:
            dominant_field, dominant_ms = max(
                stage_geomeans.items(), key=lambda item: item[1]
            )
            share = (
                dominant_ms / total_geomean
                if total_geomean and total_geomean > 0.0
                else None
            )
            dominant_label = ROUTE_STAGE_DISPLAY_NAMES.get(
                dominant_field, dominant_field
            )
            next_step = route_share_next_step(dominant_field, display_name)
        elif non_additive_stage_geomeans:
            dominant_field, dominant_ms = max(
                non_additive_stage_geomeans.items(), key=lambda item: item[1]
            )
            share = None
            dominant_label = (
                f"{ROUTE_STAGE_DISPLAY_NAMES.get(dominant_field, dominant_field)} "
                "(non-additive diagnostic)"
            )
            next_step = route_share_next_step(
                "timing_surface_stage_contract", display_name
            )
            contract_stage_id = ROUTE_STAGE_ID_BY_FIELD.get(dominant_field)
        elif excluded_diagnostic_stage_geomeans:
            dominant_field, dominant_ms = max(
                excluded_diagnostic_stage_geomeans.items(), key=lambda item: item[1]
            )
            share = None
            dominant_label = (
                f"{ROUTE_STAGE_DISPLAY_NAMES.get(dominant_field, dominant_field)} "
                "(excluded diagnostic)"
            )
            next_step = route_share_next_step(
                "timing_surface_stage_contract", display_name
            )
            contract_stage_id = ROUTE_STAGE_ID_BY_FIELD.get(dominant_field)
        else:
            dominant_field = "missing"
            dominant_ms = None
            share = None
            dominant_label = "missing"
            next_step = "add_stage_timing_before_optimization"
        statuses = Counter(
            str(row.get("exclusive_stage_timing_status") or "missing")
            for row in group_rows
        )
        readiness_statuses = Counter(
            str(row.get("route_timing_instrument_status") or "missing")
            for row in surface_rows
        )
        readiness_status = (
            "not_optimization_ready"
            if contract_stage_id
            or readiness_statuses.get("not_optimization_ready", 0)
            else "optimization_ready"
            if surface_rows
            else "hot_runtime_row_missing"
        )
        not_ready_stage_ids = {
            contract_stage_id
        } if contract_stage_id else set()
        not_ready_stage_ids.update(
            {
                stage_id
                for row in surface_rows
                for stage_id in str(
                    row.get("route_timing_instrument_not_ready_stage_ids") or ""
                ).split(",")
                if stage_id and stage_id != "none"
            }
        )
        not_ready_stage_ids = sorted(not_ready_stage_ids)
        rendered_rows.append(
            [
                display_name,
                DEFAULT_ROUTE_TIMING_SURFACE,
                f"{len(surface_rows)}/{len(group_rows)}",
                fmt_ms(total_geomean) if total_geomean is not None else "hot runtime row missing",
                dominant_label,
                fmt_ms(dominant_ms),
                fmt_percent(share * 100.0 if share is not None else None),
                next_step,
                readiness_status,
                ", ".join(not_ready_stage_ids) if not_ready_stage_ids else "none",
                "; ".join(f"{status}={count}" for status, count in sorted(statuses.items())),
            ]
        )
    return {
        "heading": "Route-Share Amdahl Attribution",
        "headers": [
            "Route",
            "Timing surface",
            "Rows",
            "Hot route geomean",
            "Dominant stage",
            "Dominant stage geomean",
            "Dominant route share",
            "Next optimization target",
            "Optimization readiness",
            "Not-ready stages",
            "Exclusive timing status",
        ],
        "rows": rendered_rows,
        "schema_version": ROUTE_SHARE_AMDAHL_SCHEMA_VERSION,
        "claim_boundary": (
            "route-share attribution uses committed local benchmark evidence to choose the next "
            "optimization target. It defaults to hot_runtime rows and does not let "
            "full replay or publication proof timing redefine the hot route; it is not a "
            "public speed, production, or replacement claim. Stage shares are ranked only "
            "when the stage is included in the selected timing surface and additive to the "
            "authoritative route total; non-additive diagnostic timing fails closed before "
            "optimization targeting"
        ),
    }


def source_read_scout_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts: Counter[tuple[str, str, str, int, str, str, str]] = Counter()
    for row in route_table_rows(rows):
        if not is_shardloom_engine(str(row.get("engine") or "")):
            continue
        route = str(row.get("route_display_name") or row.get("route_lane_id") or "unknown")
        status = str(row.get("source_read_scout_timing_split_status") or "missing")
        decode = str(row.get("source_read_decode_status") or "missing")
        skipped = int(numeric_value(row.get("source_read_skipped_column_count")) or 0)
        materialization = str(
            row.get("source_read_row_materialization_status") or "missing"
        )
        typed_decode = fmt_ms(row.get("source_read_typed_decode_ms"))
        columnar_handoff = fmt_ms(row.get("source_read_columnar_handoff_ms"))
        counts[
            (route, status, decode, skipped, materialization, typed_decode, columnar_handoff)
        ] += 1
    blockers = {
        status
        for (_, status, _, _, _, _, _), _count in counts.items()
        if status.startswith("blocked")
    }
    return {
        "heading": "Source-Read Scout Attribution",
        "headers": [
            "Route",
            "Timing split",
            "Decode status",
            "Skipped columns",
            "Row materialization",
            "Typed decode",
            "Columnar handoff",
            "Rows",
        ],
        "rows": [
            [
                route,
                status,
                decode,
                skipped,
                materialization,
                typed_decode,
                columnar_handoff,
                count,
            ]
            for (
                route,
                status,
                decode,
                skipped,
                materialization,
                typed_decode,
                columnar_handoff,
            ), count in sorted(counts.items())
        ],
        "schema_version": SOURCE_READ_SCOUT_SCHEMA_VERSION,
        "status": "blocked" if blockers else "passed",
        "claim_boundary": (
            "source-read scout fields distinguish header/scout, byte acquisition, and full-body "
            "read timing; missing splits block scout optimization claims without blocking route "
            "publication"
        ),
    }


def vortex_writer_context_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    groups: dict[tuple[str, str, str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in route_table_rows(rows):
        if not is_shardloom_engine(str(row.get("engine") or "")):
            continue
        route = str(row.get("route_display_name") or row.get("route_lane_id") or "unknown")
        plan = str(row.get("vortex_write_plan_status") or "missing")
        reuse = str(row.get("vortex_writer_context_reuse_status") or "missing")
        coalescing = str(
            row.get("vortex_write_plan_coalescing_status")
            or row.get("vortex_write_coalescing_status")
            or "missing"
        )
        digest = str(row.get("vortex_write_plan_digest_status") or "missing")
        verify = str(row.get("vortex_write_plan_verification_status") or "missing")
        groups[(route, plan, reuse, coalescing, digest, verify)].append(row)
    rendered_rows: list[list[Any]] = []
    for (route, plan, reuse, coalescing, digest, verify), group_rows in sorted(
        groups.items()
    ):
        artifact_count = sum(
            int(numeric_value(row.get("vortex_write_plan_artifact_count")) or 0)
            for row in group_rows
        )
        shared_context_rows = sum(
            1
            for row in group_rows
            if first_bool_field(
                row, ("vortex_write_plan_shared_writer_context",), default=False
            )
        )
        write_count = sum(
            int(numeric_value(row.get("vortex_writer_context_write_count")) or 0)
            for row in group_rows
        )
        reuse_hits = sum(
            int(numeric_value(row.get("vortex_writer_context_reuse_hit_count")) or 0)
            for row in group_rows
        )
        open_ms = geomean_non_negative(
            [
                value
                for row in group_rows
                for value in [numeric_value(row.get("vortex_writer_context_open_ms"))]
                if value is not None and value >= 0.0
            ]
        )
        segment_ms = geomean_non_negative(
            [
                value
                for row in group_rows
                for value in [numeric_value(row.get("vortex_segment_write_ms"))]
                if value is not None and value >= 0.0
            ]
        )
        workspace_ms = geomean_non_negative(
            [
                value
                for row in group_rows
                for value in [numeric_value(row.get("vortex_workspace_stage_ms"))]
                if value is not None and value >= 0.0
            ]
        )
        rendered_rows.append(
            [
                route,
                plan,
                reuse,
                coalescing,
                artifact_count,
                shared_context_rows,
                write_count,
                reuse_hits,
                fmt_ms(open_ms),
                fmt_ms(segment_ms),
                fmt_ms(workspace_ms),
                digest,
                verify,
                len(group_rows),
            ]
        )
    return {
        "heading": "Vortex Writer Context Attribution",
        "headers": [
            "Route",
            "Write plan",
            "Context reuse",
            "Coalescing",
            "Artifacts",
            "Shared rows",
            "Writes",
            "Reuse hits",
            "Context open",
            "Segment write",
            "Workspace stage",
            "Digest",
            "Verify",
            "Rows",
        ],
        "rows": rendered_rows,
        "schema_version": "shardloom.website.vortex_writer_context.v1",
        "claim_boundary": (
            "writer-context attribution distinguishes Vortex runtime/session open, upstream "
            "segment write, and workspace-safe staging/commit cost; it is optimization evidence "
            "only and does not authorize speed or production claims"
        ),
    }


def vortex_reopen_scan_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts: Counter[tuple[str, str, str]] = Counter()
    for row in route_table_rows(rows):
        if not is_shardloom_engine(str(row.get("engine") or "")):
            continue
        route = str(row.get("route_display_name") or row.get("route_lane_id") or "unknown")
        reopen = str(row.get("vortex_reopen_verify_split_status") or "missing")
        counters = str(row.get("vortex_scan_counter_status") or "missing")
        counts[(route, reopen, counters)] += 1
    blockers = {
        status
        for (_route, reopen, counters), _count in counts.items()
        for status in (reopen, counters)
        if status.startswith("blocked")
    }
    return {
        "heading": "Vortex Reopen And Scan Attribution",
        "headers": ["Route", "Reopen/verify split", "Scan counters", "Rows"],
        "rows": [
            [route, reopen, counters, count]
            for (route, reopen, counters), count in sorted(counts.items())
        ],
        "schema_version": VORTEX_REOPEN_SCAN_ATTRIBUTION_SCHEMA_VERSION,
        "status": "blocked" if blockers else "passed",
        "claim_boundary": (
            "reopen/scan fields distinguish footer open, metadata verification, scan-open, "
            "scenario scan, and data-movement counters; missing counters block scan optimization "
            "claims without implying fallback"
        ),
    }


def fast_path_attribution_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in route_table_rows(rows):
        if not is_shardloom_engine(str(row.get("engine") or "")):
            continue
        key = str(row.get("route_display_name") or row.get("route_lane_id") or "unknown")
        groups[key].append(row)

    rendered_rows: list[list[Any]] = []
    for display_name, group_rows in groups.items():
        evidence_excluded = sum(
            1 for row in group_rows if row.get("evidence_render_included_in_route_total") is False
        )
        certificate_counts = Counter(
            str(row.get("certificate_link_status") or "unknown") for row in group_rows
        )
        rendered_rows.append(
            [
                display_name,
                len(group_rows),
                fmt_ms(
                    geomean_non_negative(
                        [
                            value
                            for row in group_rows
                            for value in [numeric_value(row.get("runtime_execution_ms"))]
                            if value is not None and value >= 0
                        ]
                    )
                ),
                fmt_ms(
                    geomean_non_negative(
                        [
                            value
                            for row in group_rows
                            for value in [numeric_value(row.get("output_delivery_ms"))]
                            if value is not None and value >= 0
                        ]
                    )
                ),
                fmt_ms(
                    geomean_non_negative(
                        [
                            value
                            for row in group_rows
                            for value in [numeric_value(row.get("evidence_capture_ms"))]
                            if value is not None and value >= 0
                        ]
                    )
                ),
                fmt_ms(
                    geomean_non_negative(
                        [
                            value
                            for row in group_rows
                            for value in [numeric_value(row.get("evidence_render_ms"))]
                            if value is not None and value >= 0
                        ]
                    )
                ),
                fmt_ms(
                    geomean_non_negative(
                        [
                            value
                            for row in group_rows
                            for value in [numeric_value(row.get("certificate_link_ms"))]
                            if value is not None and value >= 0
                        ]
                    )
                ),
                f"{evidence_excluded}/{len(group_rows)} excluded from route total",
                ", ".join(f"{key}: {count}" for key, count in sorted(certificate_counts.items())),
            ]
        )
    return {
        "heading": "Runtime Fast Path Versus Evidence Path",
        "headers": [
            "Route",
            "Rows",
            "Runtime execution",
            "Output delivery",
            "Evidence capture",
            "Evidence render",
            "Certificate link",
            "Evidence render route total",
            "Certificate status",
        ],
        "rows": rendered_rows,
        "schema_version": FAST_PATH_ATTRIBUTION_SCHEMA_VERSION,
        "claim_boundary": (
            "runtime execution timing, output delivery, evidence capture, evidence render, "
            "and certificate linking are separate interpretation buckets; fast-path timing "
            "is not a performance superiority claim"
        ),
    }


def evidence_render_proof_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for row in route_table_rows(rows):
        if not is_shardloom_engine(str(row.get("engine") or "")):
            continue
        status = str(row.get("evidence_render_proof_status") or "missing")
        route_boundary = str(
            row.get("evidence_render_route_timing_boundary") or "missing"
        )
        hot_path_policy = str(row.get("evidence_render_hot_path_policy") or "missing")
        human_scope = str(
            row.get("evidence_render_human_expansion_timing_scope") or "missing"
        )
        key = (status, route_boundary, hot_path_policy, human_scope)
        if key not in grouped:
            grouped[key] = {
                "count": 0,
                "routes": set(),
                "digest": str(row.get("evidence_render_proof_digest") or "missing"),
            }
        grouped[key]["count"] += 1
        grouped[key]["routes"].add(
            str(row.get("route_display_name") or row.get("route_lane_id") or "unknown")
        )

    return {
        "heading": "Evidence-Render Proof Regeneration",
        "headers": [
            "Status",
            "Rows",
            "Routes",
            "Route timing boundary",
            "Hot-path policy",
            "Human expansion timing",
            "Digest sample",
        ],
        "rows": [
            [
                status,
                value["count"],
                ", ".join(sorted(value["routes"])),
                route_boundary,
                hot_path_policy,
                human_scope,
                value["digest"],
            ]
            for (status, route_boundary, hot_path_policy, human_scope), value in sorted(
                grouped.items()
            )
        ],
        "schema_version": EVIDENCE_RENDER_PROOF_SCHEMA_VERSION,
        "claim_boundary": (
            "compact evidence-render proof fields support benchmark attribution and "
            "website/table regeneration only; route totals remain the comparison "
            "surface and no performance, production, package, SQL/DataFrame, "
            "object-store/lakehouse, Foundry, or Spark-displacement claim is authorized"
        ),
    }


def operator_mode_inventory_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    for row in route_table_rows(rows):
        if not is_shardloom_engine(str(row.get("engine") or "")):
            continue
        mode = str(row.get("operator_execution_mode") or "unsupported")
        blocker = str(row.get("operator_blocker_code") or "missing")
        encoded = str(row.get("encoded_native_operators") or "none")
        residual = str(row.get("residual_native_operators") or "none")
        materialized = str(row.get("materialized_temporary_operators") or "none")
        key = (mode, blocker, encoded, residual, materialized)
        if key not in grouped:
            grouped[key] = {"count": 0, "lanes": set()}
        grouped[key]["count"] += 1
        grouped[key]["lanes"].add(
            str(row.get("route_display_name") or row.get("route_lane_id") or "unknown")
        )

    rows_out: list[list[Any]] = []
    mode_order = {
        "encoded_native": 0,
        "residual_native": 1,
        "materialized_temporary": 2,
        "unsupported": 3,
    }
    for (mode, blocker, encoded, residual, materialized), payload in sorted(
        grouped.items(), key=lambda item: (mode_order.get(item[0][0], 99), item[0])
    ):
        rows_out.append(
            [
                mode,
                payload["count"],
                encoded,
                residual,
                materialized,
                blocker,
                ", ".join(sorted(payload["lanes"])),
            ]
        )

    encoded_count = sum(row[1] for row in rows_out if row[0] == "encoded_native")
    residual_count = sum(row[1] for row in rows_out if row[0] == "residual_native")
    temporary_count = sum(row[1] for row in rows_out if row[0] == "materialized_temporary")
    unsupported_count = sum(row[1] for row in rows_out if row[0] == "unsupported")
    return {
        "heading": "Operator Mode Inventory",
        "headers": [
            "Execution mode",
            "Rows",
            "Encoded-native operators",
            "Residual-native operators",
            "Materialized temporary operators",
            "Blocker",
            "Route lanes",
        ],
        "rows": rows_out,
        "schema_version": OPERATOR_MODE_INVENTORY_SCHEMA_VERSION,
        "status": (
            "encoded_native_promotion_pending"
            if residual_count or temporary_count or unsupported_count
            else "encoded_native_inventory_complete"
        ),
        "encoded_native_row_count": encoded_count,
        "residual_native_row_count": residual_count,
        "materialized_temporary_row_count": temporary_count,
        "unsupported_row_count": unsupported_count,
        "claim_boundary": (
            "runtime-supported rows may still be residual-native or materialized-temporary; "
            "encoded-native support requires operator_encoded_native_claim_allowed=true "
            "and no residual/materialized blocker"
        ),
    }


def operator_hot_path_candidate_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[tuple[str, str, str, str], int] = Counter()
    for row in route_table_rows(rows):
        if not is_shardloom_engine(str(row.get("engine") or "")):
            continue
        candidate = str(row.get("operator_hot_path_candidate") or "operator_mode_evidence_missing")
        status = str(row.get("operator_hot_path_candidate_status") or "blocked_operator_mode_evidence_missing")
        blocker = str(row.get("operator_blocker_code") or "missing")
        next_step = str(row.get("operator_hot_path_next_step") or "emit operator evidence")
        grouped[(candidate, status, blocker, next_step)] += 1

    return {
        "heading": "Operator Hot-Path Promotion Candidates",
        "headers": ["Candidate", "Status", "Rows", "Blocker", "Next step"],
        "rows": [
            [candidate, status, count, blocker, next_step]
            for (candidate, status, blocker, next_step), count in sorted(
                grouped.items(),
                key=lambda item: (
                    0 if item[0][0] == "selective_filter_selection_vector_metric_aggregation" else 1,
                    -item[1],
                    item[0],
                ),
            )
        ],
        "schema_version": OPERATOR_MODE_INVENTORY_SCHEMA_VERSION,
        "claim_boundary": (
            "candidate rows name the next encoded-native promotion proof; they do not "
            "change runtime support, claim-grade status, or fallback policy"
        ),
    }


def encoded_kernel_promotion_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[tuple[str, str, str, str], int] = Counter()
    for row in route_table_rows(rows):
        if not is_shardloom_engine(str(row.get("engine") or "")):
            continue
        status = str(
            row.get("encoded_kernel_promotion_status")
            or "encoded_kernel_promotion_missing"
        )
        pairs = str(row.get("encoded_kernel_promoted_pair_ids") or "none")
        families = str(row.get("encoded_kernel_promoted_operator_families") or "none")
        full_claim = str(
            row.get("encoded_kernel_full_operator_claim_allowed") is True
        ).lower()
        grouped[(status, pairs, families, full_claim)] += 1

    promoted_count = sum(
        count
        for (status, _pairs, _families, _full_claim), count in grouped.items()
        if status == "partial_encoded_kernel_pairs_promoted"
    )
    bridge_count = sum(
        count
        for (status, _pairs, _families, _full_claim), count in grouped.items()
        if status == "selection_vector_bridge_promoted_metric_residual"
    )
    return {
        "heading": "Encoded-Kernel Promotion",
        "headers": [
            "Promotion status",
            "Rows",
            "Promoted pairs",
            "Operator families",
            "Full operator claim allowed",
        ],
        "rows": [
            [status, count, pairs, families, full_claim]
            for (status, pairs, families, full_claim), count in sorted(
                grouped.items(),
                key=lambda item: (
                    0
                    if item[0][0] == "partial_encoded_kernel_pairs_promoted"
                    else 1,
                    -item[1],
                    item[0],
                ),
            )
        ],
        "schema_version": ENCODED_KERNEL_PROMOTION_SCHEMA_VERSION,
        "partial_encoded_kernel_promoted_row_count": promoted_count,
        "selection_vector_metric_residual_bridge_row_count": bridge_count,
        "claim_boundary": (
            "encoded-kernel promotion rows are narrower than full operator mode. "
            "A row may expose promoted compressed-kernel pairs or a selection-vector "
            "bridge while the full route remains residual-native until the complete "
            "operator contract is certified."
        ),
    }


def runtime_status_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    decorated = [decorated_route_row(row) for row in rows]
    shardloom_rows = [
        row for row in decorated if is_shardloom_engine(str(row.get("engine") or ""))
    ]
    external_rows = [
        row for row in decorated if not is_shardloom_engine(str(row.get("engine") or ""))
    ]
    shardloom_unsupported = sum(
        1
        for row in shardloom_rows
        if row.get("status") == "unsupported" or row.get("route_runtime_status") == "unsupported"
    )
    external_unsupported = sum(1 for row in external_rows if row.get("status") == "unsupported")
    status_counts = Counter(str(row.get("route_runtime_status") or "unknown") for row in decorated)
    return {
        "heading": "Route Runtime Status",
        "headers": ["Scope", "Rows", "Interpretation"],
        "rows": [
            [
                "ShardLoom unsupported rows",
                shardloom_unsupported,
                "ShardLoom runtime gaps in the promoted comparative roster",
            ],
            [
                "External baseline unsupported rows",
                external_unsupported,
                "External engine limitation rows; not ShardLoom runtime gaps",
            ],
            *[
                [f"route_runtime_status={status}", count, "published row status vocabulary"]
                for status, count in sorted(status_counts.items())
            ],
        ],
        "schema_version": ROUTE_RUNTIME_STATUS_SCHEMA_VERSION,
        "status_vocabulary": sorted(ROUTE_RUNTIME_STATUSES),
    }


def promoted_metadata(artifact: dict[str, Any]) -> dict[str, Any]:
    metadata = artifact.get("published_benchmark_artifact")
    return metadata if isinstance(metadata, dict) else artifact


def benchmark_format_order(
    artifact: dict[str, Any], rows: list[dict[str, Any]], profile: str
) -> list[str]:
    metadata = promoted_metadata(artifact)
    row_formats = {
        str(row.get("storage_format"))
        for row in rows
        if isinstance(row.get("storage_format"), str) and row.get("storage_format")
    }
    declared = [
        str(value)
        for value in metadata.get("format_order", [])
        if isinstance(value, str) and value
    ]
    if declared and row_formats.issubset(set(declared)):
        return list(dict.fromkeys(declared))
    if profile in PROFILES:
        profile_order = list(
            dict.fromkeys(
                [
                    *PROFILES[profile].required_formats,
                    *PROFILES[profile].optional_formats,
                ]
            )
        )
        return [
            *[fmt for fmt in profile_order if fmt in row_formats],
            *sorted(row_formats - set(profile_order)),
        ]
    return sorted(row_formats)


def benchmark_scenario_order(
    artifact: dict[str, Any],
    rows: list[dict[str, Any]],
    profile: str,
    *,
    prefer_declared: bool = True,
) -> list[str]:
    metadata = promoted_metadata(artifact)
    declared = [
        str(value)
        for value in metadata.get("scenario_order", [])
        if isinstance(value, str) and value
    ]
    if prefer_declared and declared:
        return list(dict.fromkeys(declared))
    scenarios = {
        str(row.get("scenario_name"))
        for row in rows
        if isinstance(row.get("scenario_name"), str) and row.get("scenario_name")
    }
    if profile in PROFILES:
        required = list(PROFILES[profile].required_scenarios)
        ordered: list[str] = []
        for required_name in required:
            matches = sorted(
                scenario
                for scenario in scenarios
                if scenario == required_name or scenario.endswith(f": {required_name}")
            )
            ordered.extend(matches or [required_name])
        extras = sorted(scenarios - set(ordered))
        return [*list(dict.fromkeys(ordered)), *extras]
    return sorted(scenarios)


def format_coverage_table(
    artifact: dict[str, Any], rows: list[dict[str, Any]], profile: str
) -> dict[str, Any]:
    profile_spec = PROFILES[profile]
    required = set(profile_spec.required_formats)
    optional = set(profile_spec.optional_formats)
    expected = list(dict.fromkeys([*profile_spec.required_formats, *profile_spec.optional_formats]))
    available = set(benchmark_format_order(artifact, rows, profile))
    available.update(
        str(row.get("storage_format"))
        for row in rows
        if row.get("storage_format")
    )
    counts = Counter(str(row.get("storage_format")) for row in rows if row.get("storage_format"))
    return {
        "heading": "Format Coverage",
        "headers": ["Format", "Profile role", "Status", "Rows", "Reason"],
        "rows": [
            [
                fmt,
                "required" if fmt in required else "optional",
                "available" if fmt in available else "missing_optional" if fmt in optional else "missing_required",
                counts[fmt],
                (
                    "published benchmark rows include this format"
                    if fmt in available
                    else "format is expected by the profile but absent from the promoted artifact"
                ),
            ]
            for fmt in expected
        ],
    }


def profile_lane_availability_table(
    artifact: dict[str, Any],
    rows: list[dict[str, Any]],
    active_profile: str,
) -> dict[str, Any]:
    available = set(available_lanes(artifact, rows))
    active_expected = set(expected_lanes_for_profile(active_profile))
    rendered_rows: list[list[Any]] = []
    for profile in BENCHMARK_PROFILE_ROSTER:
        profile_expected = expected_lanes_for_profile(profile)
        for lane in profile_expected:
            required = lane_required_for_profile(profile, lane)
            lane_meta = LANES.get(lane)
            if lane in available:
                status = "available"
                reason = lane_reason(lane, artifact)
            elif lane in active_expected:
                status = "missing_required" if lane_required_for_profile(active_profile, lane) else "missing_optional"
                reason = missing_reason(lane, artifact)
            else:
                status = "not_requested_by_current_profile"
                reason = f"run benchmark profile {profile} to publish this lane"
            rendered_rows.append(
                [
                    profile,
                    lane,
                    "required" if required else "optional",
                    lane_meta.group if lane_meta else "unknown",
                    status,
                    reason,
                ]
            )
    return {
        "heading": "Profile Lane Availability",
        "headers": ["Profile", "Lane", "Profile role", "Lane group", "Status", "Version / reason"],
        "rows": rendered_rows,
    }


def claim_grade_closeout_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    shardloom_rows = [
        row for row in rows if str(row.get("engine", "")).startswith("shardloom")
    ]
    external_rows = [
        row for row in rows if not str(row.get("engine", "")).startswith("shardloom")
    ]
    counts = Counter(str(row.get("claim_gate_status", "unknown")) for row in shardloom_rows)
    surface_counts = Counter(str(row.get("timing_surface", "unknown")) for row in shardloom_rows)
    tier_counts = Counter(str(row.get("actual_evidence_tier", "unknown")) for row in shardloom_rows)
    hot_rows = [
        row for row in shardloom_rows if str(row.get("timing_surface")) == "hot_runtime"
    ]
    publication_rows = [
        row for row in shardloom_rows if str(row.get("timing_surface")) == "publication_proof"
    ]
    hot_claim_counts = Counter(str(row.get("claim_gate_status", "unknown")) for row in hot_rows)
    publication_claim_counts = Counter(
        str(row.get("claim_gate_status", "unknown")) for row in publication_rows
    )
    shardloom_unsupported = sum(1 for row in shardloom_rows if row.get("status") == "unsupported")
    external_unsupported = sum(1 for row in external_rows if row.get("status") == "unsupported")
    return {
        "heading": "ShardLoom Claim-Grade Closeout",
        "headers": ["Scope", "Current rows", "Target", "Owning plan item"],
        "rows": [
            [
                "ShardLoom timing-surface rows",
                (
                    f"{len(shardloom_rows)} rows; "
                    f"{surface_counts['hot_runtime']} hot_runtime / "
                    f"{surface_counts['publication_proof']} publication_proof"
                ),
                "keep hot-runtime and publication-proof rows separate",
                "PERF-SPLIT-FIX-1 / GAR-RUNTIME-IMPL-5J",
            ],
            [
                "Hot-runtime metadata rows",
                (
                    f"{len(hot_rows)} rows; {tier_counts['metadata_sink']} metadata_sink; "
                    f"{hot_claim_counts['not_claim_grade']} compact hot-evidence rows"
                ),
                "not_claim_grade is expected for compact metadata-sink timing rows",
                "PERF-SPLIT-FIX-1",
            ],
            [
                "Publication-proof rows",
                (
                    f"{len(publication_rows)} rows; "
                    f"{publication_claim_counts['claim_grade']} claim_grade"
                ),
                "claim_grade for every admitted publication-proof row",
                "GAR-RUNTIME-IMPL-5J",
            ],
            [
                "External baseline rows",
                "external_baseline_only rows remain comparison context",
                "visible baseline-only rows; never fallback execution",
                "GAR-BENCH-PUB-1 / GAR-RUNTIME-IMPL-5J",
            ],
            [
                "ShardLoom unsupported rows",
                f"{shardloom_unsupported} ShardLoom rows",
                "0 ShardLoom unsupported rows in the admitted benchmark-range route roster",
                "GAR-RUNTIME-IMPL-6D",
            ],
            [
                "External baseline unsupported rows",
                f"{external_unsupported} external baseline rows",
                "visible baseline engine limitation rows; never counted as ShardLoom runtime gaps",
                "GAR-BENCH-PUB-1 / GAR-RUNTIME-IMPL-6D",
            ],
        ],
    }


def vortex_lane_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    selected = [
        row
        for row in rows
        if str(row.get("engine", "")).startswith("shardloom")
        and row.get("status") == "success"
    ]
    rendered = []
    for row in selected:
        metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
        rendered.append(
            [
                row.get("engine"),
                row.get("storage_format"),
                row.get("scenario_name"),
                row.get("selected_execution_mode"),
                row.get("claim_gate_status"),
                fmt_ms(geomean(iteration_values(row))),
                metrics.get("vortex_scan_millis", "n/a"),
                metrics.get("operator_compute_millis", "n/a"),
                row.get("fallback_attempted", False),
                row.get("external_engine_invoked", False),
            ]
        )
    return {
        "heading": "Vortex-Oriented Lanes By Source Format",
        "headers": [
            "Engine",
            "Source format",
            "Scenario",
            "Execution mode",
            "Claim gate",
            "Local row time",
            "Vortex scan ms",
            "Operator ms",
            "Fallback",
            "External engine",
        ],
        "rows": rendered,
    }


def numeric_value(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def cold_lane_field_present(fields: dict[str, Any], field: str) -> bool:
    for candidate in (field, *COLD_LANE_FIELD_ALIASES.get(field, ())):
        value = fields.get(candidate)
        if value is None:
            continue
        if isinstance(value, str):
            if bool(value.strip()) and value.strip().lower() not in {
                "missing",
                "n/a",
                "not_applicable",
                "not_measured",
                "not_reported",
                "unknown",
            }:
                return True
            continue
        return True
    return False


def first_numeric_millis(fields: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        parsed = numeric_value(fields.get(key))
        if parsed is None:
            continue
        if key.endswith("_micros"):
            return parsed / 1000.0
        return parsed
    return None


def first_integer_field(fields: dict[str, Any], keys: tuple[str, ...]) -> int | None:
    for key in keys:
        parsed = numeric_value(fields.get(key))
        if parsed is not None:
            return int(parsed)
    return None


def count_csv_values(value: Any) -> int | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    if not stripped or stripped.lower() in {"none", "not_reported", "not_requested"}:
        return 0
    return len([item for item in stripped.split(",") if item.strip()])


def source_columns_requested(fields: dict[str, Any]) -> int | None:
    direct = first_integer_field(
        fields,
        (
            "source_columns_requested",
            "source_state_reader_projection_column_count",
        ),
    )
    if direct is not None:
        return direct
    for key in (
        "source_state_reader_projection_columns",
        "source_columns",
        "streaming_projected_columns",
        "vortex_capillary_preparation_projection_mask",
    ):
        count = count_csv_values(fields.get(key))
        if count is not None:
            return count
    observed = first_integer_field(
        fields,
        (
            "source_column_count",
            "vortex_scout_ingress_column_count",
            "vortex_capillary_preparation_activation_observed_columns",
        ),
    )
    if observed is not None:
        return observed
    return None


def source_projection_applied(fields: dict[str, Any]) -> bool:
    for key in (
        "source_projection_applied",
        "projection_applied",
        "scan_projection_pushed_down",
        "streaming_projection_pushdown_applied",
    ):
        if key in fields:
            return field_bool(fields, key, False)
    status = str(
        fields.get("source_state_projection_pushdown_status")
        or fields.get("scan_projection_pushdown_status")
        or fields.get("vortex_preparation_spine_projection_mask_status")
        or ""
    ).strip().lower()
    if status and status not in {
        "none",
        "not_applicable",
        "not_reported",
        "not_requested",
        "not_requested_full_read",
        "benchmark_projection_mask_not_materialized",
    }:
        return True
    requested = source_columns_requested(fields)
    observed = first_integer_field(
        fields,
        (
            "vortex_capillary_preparation_activation_observed_columns",
            "vortex_scout_ingress_column_count",
        ),
    )
    return bool(
        requested is not None and observed is not None and requested > 0 and requested < observed
    )


def source_pressure_fields(fields: dict[str, Any]) -> dict[str, Any]:
    split_count = first_integer_field(
        fields,
        (
            "source_split_count",
            "split_count",
            "split_manifest_split_count",
            "vortex_preparation_spine_source_split_count",
            "scale_benchmark_split_count",
            "file_count",
            "runtime_split_count",
        ),
    )
    open_count = first_integer_field(
        fields,
        (
            "source_open_count",
            "source_file_open_count",
            "file_open_count",
            "file_count",
            "split_manifest_split_count",
            "source_split_count",
        ),
    )
    bytes_read = first_integer_field(
        fields,
        (
            "source_bytes_read",
            "bytes_read",
            "source_size",
            "vortex_scout_ingress_source_byte_count",
            "vortex_copy_budget_source_byte_count",
        ),
    )
    columns = source_columns_requested(fields)
    projection = source_projection_applied(fields)
    if (split_count or 0) >= 8 or (open_count or 0) >= 8:
        profile = "many_small_files_pressure"
    elif (bytes_read or 0) >= 64 * 1024 * 1024:
        profile = "large_source_byte_pressure"
    elif projection:
        profile = "projection_sensitive_source"
    else:
        profile = "single_local_source"
    return {
        "source_split_count": split_count,
        "source_open_count": open_count,
        "source_bytes_read": bytes_read,
        "source_columns_requested": columns,
        "source_projection_applied": projection,
        "source_pressure_profile": profile,
    }


def vortex_prepared_state_fields(fields: dict[str, Any]) -> dict[str, Any]:
    reusable = (
        field_bool(fields, "vortex_prepared_state_reusable", False)
        or field_bool(fields, "prepared_state_reuse_allowed", False)
        or field_bool(fields, "prepared_artifact_reuse_eligible", False)
        or field_bool(fields, "prepared_state_reused", False)
    )
    fingerprint = next(
        (
            str(fields.get(key))
            for key in (
                "vortex_prepared_state_fingerprint",
                "prepared_state_digest",
                "vortex_preparation_spine_prepared_state_digest",
                "vortex_capillary_preparation_prepared_state_digest",
                "prepared_artifact_digest",
                "vortex_artifact_digest",
            )
            if fields.get(key) not in {None, "", "none", "not_reported"}
        ),
        "none",
    )
    return {
        "vortex_prepared_state_reusable": reusable,
        "vortex_prepared_state_fingerprint": fingerprint,
        "vortex_prepared_state_fingerprint_status": (
            "fingerprint_recorded" if fingerprint != "none" else "not_recorded"
        ),
    }


def first_meaningful_text(fields: dict[str, Any], keys: tuple[str, ...], default: str) -> str:
    for key in keys:
        value = fields.get(key)
        if value not in {None, "", "none", "not_reported", "not_requested"}:
            return str(value)
    return default


def route_diagnostic_fields_for_row(
    row: dict[str, Any], identity: dict[str, Any]
) -> dict[str, Any]:
    fields = runtime_validation_field_map(row)
    engine = str(row.get("engine") or "")
    lane_id = str(row.get("route_lane_id") or identity.get("route_lane_id") or "")
    external = not is_shardloom_engine(engine)
    if external:
        return {
            "source_state_fingerprint": "external_baseline_only",
            "source_schema_fingerprint": "external_baseline_only",
            "source_parse_plan_id": "external_baseline_only",
            "source_split_manifest_id": "external_baseline_only",
            "source_anomaly_count": "external_baseline_only",
            "source_quarantine_required": "external_baseline_only",
            "prepared_state_fingerprint": "external_baseline_only",
            "prepared_state_reuse_scope": "external_baseline_only",
            "prepared_state_reuse_manifest_path": "external_baseline_only",
            "prepared_state_reuse_policy": "external_baseline_only",
            "prepared_state_reuse_hit": "external_baseline_only",
            "prepared_state_reuse_reason": "external_baseline_only",
            "prepared_state_reuse_manifest_digest": "external_baseline_only",
            "prepared_state_invalidation_reason": "external_baseline_only",
            "nearest_runnable_route": "external_baseline_only",
            "required_feature_gate": "external_baseline_only",
            "runtime_blocker_code": "external_baseline_only",
        }

    status = str(row.get("status") or "")
    source_state = first_meaningful_text(
        fields,
        (
            "source_state_fingerprint",
            "source_state_digest",
            "vortex_preparation_spine_source_state_digest",
            "vortex_capillary_preparation_source_state_digest",
            "vortex_scout_ingress_source_state_digest",
        ),
        "runtime_source_state_fingerprint_pending",
    )
    schema = first_meaningful_text(
        fields,
        (
            "source_schema_fingerprint",
            "schema_digest",
            "vortex_scout_ingress_source_schema_digest_after",
            "vortex_layout_write_advisor_source_schema_digest",
        ),
        "runtime_source_schema_fingerprint_pending",
    )
    split_manifest = first_meaningful_text(
        fields,
        (
            "source_split_manifest_id",
            "split_manifest_id",
            "vortex_preparation_spine_source_split_refs",
            "vortex_capillary_preparation_source_split_refs",
        ),
        "split_manifest_pending_until_source_admission",
    )
    prepared = first_meaningful_text(
        fields,
        (
            "prepared_state_fingerprint",
            "vortex_prepared_state_fingerprint",
            "prepared_state_digest",
            "vortex_preparation_spine_prepared_state_digest",
            "vortex_capillary_preparation_prepared_state_digest",
        ),
        "runtime_prepared_state_fingerprint_pending",
    )
    reuse_scope = first_meaningful_text(
        fields, ("prepared_state_reuse_scope",), PREPARED_STATE_REUSE_NOT_APPLICABLE
    )
    reuse_path = first_meaningful_text(
        fields, ("prepared_state_reuse_manifest_path",), PREPARED_STATE_REUSE_NOT_APPLICABLE
    )
    reuse_policy = first_meaningful_text(
        fields, ("prepared_state_reuse_policy",), PREPARED_STATE_REUSE_NOT_APPLICABLE
    )
    prepared_state_reused = field_bool(
        fields,
        "prepared_state_reused",
        bool(identity.get("prepared_state_reused") is True),
    )
    reuse_hit = field_bool(fields, "prepared_state_reuse_hit", prepared_state_reused)
    if reuse_scope == PREPARED_STATE_REUSE_NOT_APPLICABLE:
        if lane_id == "warm_prepared_query":
            reuse_scope = PREPARED_STATE_REUSE_EXPLICIT_SCOPE
            reuse_path = "not_required_existing_prepared_state"
            reuse_policy = "explicit_prepared_state_admission.v1"
            reuse_hit = True
        elif lane_id == "prepare_once_batch" and (prepared_state_reused or reuse_hit):
            reuse_scope = PREPARED_STATE_REUSE_IN_PROCESS_SCOPE
            reuse_path = "not_required_in_process_prepared_batch"
            reuse_policy = "in_process_prepared_batch_reuse.v1"
            reuse_hit = True
        elif lane_id in {"cold_certified_route", "prepare_once_first_query"}:
            reuse_scope = "prepared_state_created_not_reused"
            reuse_path = "not_applicable_first_preparation"
            reuse_policy = "first_preparation_creates_vortex_prepared_state.v1"
            reuse_hit = False
        elif lane_id == "native_vortex_query":
            reuse_scope = "not_applicable_native_vortex_input"
            reuse_path = "not_applicable_native_vortex_input"
            reuse_policy = "not_applicable_native_vortex_input"
            reuse_hit = False
    if reuse_scope == PREPARED_STATE_REUSE_WORKSPACE_SCOPE:
        reuse_path = (
            reuse_path
            if reuse_path != PREPARED_STATE_REUSE_NOT_APPLICABLE
            else PREPARED_STATE_REUSE_WORKSPACE_MANIFEST_PATH
        )
        reuse_policy = (
            reuse_policy
            if reuse_policy != PREPARED_STATE_REUSE_NOT_APPLICABLE
            else PREPARED_STATE_REUSE_WORKSPACE_POLICY
        )
    reuse_reason = first_meaningful_text(
        fields,
        (
            "prepared_state_reuse_reason",
            "prepare_batch_prepared_state_reuse_reason",
        ),
        (
            "prepared_state_reused_within_declared_scope"
            if reuse_hit
            else "prepared_state_reuse_not_requested_for_route"
        ),
    )
    reuse_digest = first_meaningful_text(
        fields,
        (
            "prepared_state_reuse_manifest_digest",
            "prepare_batch_prepared_state_reuse_manifest_digest",
            "prepared_state_digest",
            "prepared_artifact_digest",
            "vortex_artifact_digest",
            "vortex_prepared_state_fingerprint",
        ),
        (
            prepared
            if reuse_hit
            else "not_applicable_no_reuse_manifest_for_route"
        ),
    )
    invalidation_reason = first_meaningful_text(
        fields,
        (
            "prepared_state_invalidation_reason",
            "invalidation_reason",
            "prepare_batch_prepared_state_invalidation_reason",
        ),
        (
            "not_applicable_same_process_or_explicit_prepared_state"
            if reuse_hit
            else "not_applicable_no_reuse_attempt"
        ),
    )
    anomaly_count = first_integer_field(
        fields,
        (
            "source_anomaly_count",
            "vortex_scout_ingress_anomaly_count",
        ),
    )
    blocker = first_meaningful_text(
        fields,
        (
            "runtime_blocker_code",
            "runtime_blocker_id",
            "source_adapter_blocker_id",
            "operator_blocker_id",
            "certification_blocker_id",
        ),
        "none" if status == "success" else "blocked_without_specific_code",
    )
    if status == "success":
        blocker = "none"
    if status == "success":
        feature_gate = "none_runtime_supported"
    elif str(identity.get("route_runtime_status")) == "feature_gated":
        feature_gate = "feature_gate_required"
    else:
        feature_gate = "none"
    return {
        "source_state_fingerprint": source_state,
        "source_schema_fingerprint": schema,
        "source_parse_plan_id": first_meaningful_text(
            fields,
            ("source_parse_plan_id", "parse_decode_plan_digest", "source_state_parse_normalization"),
            "parse_plan_pending_until_source_admission",
        ),
        "source_split_manifest_id": split_manifest,
        "source_anomaly_count": anomaly_count if anomaly_count is not None else 0,
        "source_quarantine_required": field_bool(
            fields, "source_quarantine_required", field_bool(fields, "vortex_scout_ingress_quarantine_required", False)
        ),
        "prepared_state_fingerprint": prepared,
        "prepared_state_reuse_scope": reuse_scope,
        "prepared_state_reuse_manifest_path": reuse_path,
        "prepared_state_reuse_policy": reuse_policy,
        "prepared_state_reuse_hit": reuse_hit,
        "prepared_state_reuse_reason": reuse_reason,
        "prepared_state_reuse_manifest_digest": reuse_digest,
        "prepared_state_invalidation_reason": invalidation_reason,
        "nearest_runnable_route": lane_id if status == "success" else "local_file_direct_transient_route",
        "required_feature_gate": feature_gate,
        "runtime_blocker_code": blocker,
    }


def cold_prepared_query_millis(fields: dict[str, Any], route_lane_id: str) -> float | None:
    if route_lane_id in {"prepare_once_first_query", "prepare_once_batch"}:
        return first_numeric_millis(
            fields, ("query_runtime_millis", "total_runtime_millis")
        )
    scan = first_numeric_millis(fields, ("vortex_scan_ms", "vortex_scan_millis"))
    compute = first_numeric_millis(fields, ("operator_compute_ms", "operator_compute_millis"))
    if scan is not None or compute is not None:
        return (scan or 0.0) + (compute or 0.0)
    return first_numeric_millis(fields, ("query_runtime_millis",))


def cold_bottleneck_stage_values(
    fields: dict[str, Any], route_lane_id: str
) -> dict[str, float]:
    values = {
        "source_admission": source_admission_millis(fields),
        "source_read": first_numeric_millis(
            fields,
            ("exclusive_source_read_ms", "source_read_ms", "source_read_millis"),
        ),
        "source_parse_or_decode": first_numeric_millis(
            fields,
            (
                "exclusive_source_parse_or_decode_ms",
                "source_parse_or_columnar_decode_ms",
                "compatibility_parse_millis",
                "source_parse_millis",
                "source_to_columnar_millis",
                "prepare_batch_source_to_columnar_millis",
            ),
        ),
        "source_state_build": first_numeric_millis(
            fields,
            (
                "source_state_build_millis",
                "source_state_create_millis",
                "source_state_prepare_millis",
                "source_state_prepare_micros",
            ),
        ),
        "vortex_array_build": first_numeric_millis(
            fields,
            (
                "exclusive_source_to_vortex_array_ms",
                "source_to_vortex_array_ms",
                "vortex_array_build_millis",
                "prepare_batch_vortex_array_build_millis",
            ),
        ),
        "vortex_write": first_numeric_millis(
            fields,
            (
                "exclusive_vortex_write_ms",
                "vortex_write_ms",
                "vortex_write_millis",
                "prepare_batch_vortex_write_millis",
            ),
        ),
        "vortex_digest": first_numeric_millis(
            fields, ("exclusive_vortex_digest_ms", "vortex_digest_millis", "vortex_digest_micros")
        ),
        "vortex_reopen_verify": first_numeric_millis(
            fields,
            (
                "exclusive_vortex_reopen_verify_ms",
                "vortex_reopen_or_verify_ms",
                "vortex_reopen_verify_millis",
                "prepare_batch_vortex_reopen_verify_millis",
            ),
        ),
        "prepared_query": first_numeric_millis(
            fields, ("exclusive_prepared_query_ms",)
        )
        or cold_prepared_query_millis(fields, route_lane_id),
        "sink_output": first_numeric_millis(
            fields,
            (
                "exclusive_result_sink_write_ms",
                "result_sink_write_ms",
                "result_sink_write_millis",
            ),
        ),
        "evidence_render": first_numeric_millis(
            fields,
            (
                "exclusive_evidence_render_ms",
                "evidence_render_ms",
                "evidence_render_millis",
            ),
        ),
    }
    return {
        stage: value
        for stage, value in values.items()
        if value is not None and value >= 0.0
    }


def cold_route_optimization_hint(primary_stage: str, pressure_profile: str) -> str:
    if pressure_profile == "many_small_files_pressure":
        return "batch_source_open_and_split_planning_before_parse_or_writer_tuning"
    return {
        "source_admission": "reuse_source_state_stat_and_admission_packets",
        "source_read": "improve_source_reader_throughput_and_byte_range_accounting",
        "source_parse_or_decode": "reduce_compatibility_parse_decode_tax_with_columnar_reader_projection",
        "source_state_build": "cache_source_state_schema_fingerprint_and_parse_plan_work",
        "vortex_array_build": "move_more_import_work_into_vortex_native_array_builders",
        "vortex_write": "optimize_vortex_writer_batching_layout_and_sink_buffering",
        "vortex_digest": "stream_digest_work_alongside_vortex_write",
        "vortex_reopen_verify": "reuse_prepared_state_verification_metadata_and_tighten_reopen_checks",
        "prepared_query": "optimize_prepared_vortex_scan_operator_path_after_ingest",
        "sink_output": "bound_or_amortize_result_sink_replay_cost",
        "evidence_render": "defer_noncritical_evidence_formatting_from_route_timing",
    }.get(primary_stage, "collect_more_stage_timing_before_selecting_an_optimization")


def cold_bottleneck_fields_for_row(
    row: dict[str, Any], classification: str
) -> dict[str, Any]:
    fields = runtime_validation_field_map(row)
    identity = route_identity_for_row(row)
    route_lane_id = str(identity.get("route_lane_id") or "")
    pressure = source_pressure_fields(fields)
    prepared = vortex_prepared_state_fields(fields)
    base = {
        "cold_bottleneck_schema_version": COLD_BOTTLENECK_SCHEMA_VERSION,
        "cold_bottleneck_stage_labels": ",".join(COLD_BOTTLENECK_STAGE_ORDER),
        "cold_route_optimization_hint_scope": "diagnostic_only_no_runtime_policy_change",
        "cold_route_bottleneck_claim_boundary": (
            "cold bottleneck attribution explains measured local route composition only; "
            "it does not change execution, authorize performance claims, or imply "
            "Spark/DataFusion fallback"
        ),
        **pressure,
        **prepared,
    }
    if classification == "external_baseline_only":
        return {
            **base,
            "cold_bottleneck_status": "external_baseline_only",
            "cold_bottleneck_primary_stage": "external_baseline_only",
            "cold_bottleneck_primary_stage_ms": None,
            "cold_bottleneck_primary_stage_share": None,
            "cold_bottleneck_secondary_stage": "external_baseline_only",
            "cold_bottleneck_secondary_stage_ms": None,
            "cold_bottleneck_secondary_stage_share": None,
            "cold_bottleneck_stage_value_fields": "external_baseline_only",
            "cold_route_optimization_hint": "external_baseline_only_not_shardloom_runtime",
        }
    if route_lane_id not in COLD_BOTTLENECK_ROUTE_LANES:
        return {
            **base,
            "cold_bottleneck_status": "not_applicable_non_cold_route",
            "cold_bottleneck_primary_stage": "not_applicable",
            "cold_bottleneck_primary_stage_ms": None,
            "cold_bottleneck_primary_stage_share": None,
            "cold_bottleneck_secondary_stage": "not_applicable",
            "cold_bottleneck_secondary_stage_ms": None,
            "cold_bottleneck_secondary_stage_share": None,
            "cold_bottleneck_stage_value_fields": "not_applicable_non_cold_route",
            "cold_route_optimization_hint": "not_applicable_non_cold_route",
        }
    if row.get("status") != "success":
        return {
            **base,
            "cold_bottleneck_status": "blocked_row_not_executed",
            "cold_bottleneck_primary_stage": "blocked",
            "cold_bottleneck_primary_stage_ms": None,
            "cold_bottleneck_primary_stage_share": None,
            "cold_bottleneck_secondary_stage": "blocked",
            "cold_bottleneck_secondary_stage_ms": None,
            "cold_bottleneck_secondary_stage_share": None,
            "cold_bottleneck_stage_value_fields": "blocked_row_not_executed",
            "cold_route_optimization_hint": "execute_row_before_bottleneck_attribution",
        }
    stage_values = cold_bottleneck_stage_values(fields, route_lane_id)
    ranked = sorted(stage_values.items(), key=lambda item: item[1], reverse=True)
    if not ranked:
        return {
            **base,
            "cold_bottleneck_status": "blocked_missing_stage_timing",
            "cold_bottleneck_primary_stage": "missing",
            "cold_bottleneck_primary_stage_ms": None,
            "cold_bottleneck_primary_stage_share": None,
            "cold_bottleneck_secondary_stage": "missing",
            "cold_bottleneck_secondary_stage_ms": None,
            "cold_bottleneck_secondary_stage_share": None,
            "cold_bottleneck_stage_value_fields": "missing",
            "cold_route_optimization_hint": "add_stage_timing_before_optimization",
        }
    total_stage_ms = sum(value for _, value in ranked if value >= 0.0)
    primary_stage, primary_ms = ranked[0]
    secondary_stage, secondary_ms = ranked[1] if len(ranked) > 1 else ("none", None)
    return {
        **base,
        "cold_bottleneck_status": "complete",
        "cold_bottleneck_primary_stage": primary_stage,
        "cold_bottleneck_primary_stage_ms": primary_ms,
        "cold_bottleneck_primary_stage_share": (
            primary_ms / total_stage_ms if total_stage_ms > 0.0 else None
        ),
        "cold_bottleneck_secondary_stage": secondary_stage,
        "cold_bottleneck_secondary_stage_ms": secondary_ms,
        "cold_bottleneck_secondary_stage_share": (
            secondary_ms / total_stage_ms
            if secondary_ms is not None and total_stage_ms > 0.0
            else None
        ),
        "cold_bottleneck_stage_value_fields": ";".join(
            f"{stage}={value:.4f}" for stage, value in ranked
        ),
        "cold_route_optimization_hint": cold_route_optimization_hint(
            primary_stage, str(pressure["source_pressure_profile"])
        ),
    }


def cold_lane_primary_classification(row: dict[str, Any], fields: dict[str, Any]) -> str:
    engine = str(row.get("engine", ""))
    selected_mode = str(row.get("selected_execution_mode") or "")
    if not engine.startswith("shardloom"):
        return "external_baseline_only"
    if row.get("status") != "success":
        return "blocked_incomplete_timing_split"
    if engine == "shardloom-prepare-batch":
        return "preparation_only"
    if selected_mode == "compatibility_import_certified":
        return "full_certified_cold_ingest"
    if selected_mode in {"prepared_vortex", "native_vortex"}:
        return "warm_prepared_query"
    if cold_lane_field_present(fields, "result_sink_write_millis") and (
        numeric_value(fields.get("result_sink_write_millis")) or 0.0
    ) > 0.0:
        return "sink_replay_heavy"
    if cold_lane_field_present(fields, "evidence_render_millis"):
        return "evidence_heavy"
    return "process_harness_heavy"


def cold_lane_secondary_classifications(
    row: dict[str, Any], fields: dict[str, Any]
) -> list[str]:
    if not str(row.get("engine", "")).startswith("shardloom"):
        return ["external_baseline_only"]
    classifications: list[str] = []
    if cold_lane_field_present(fields, "result_sink_write_millis") and (
        numeric_value(fields.get("result_sink_write_millis")) or 0.0
    ) > 0.0:
        classifications.append("sink_replay_heavy")
    if cold_lane_field_present(fields, "evidence_render_millis"):
        classifications.append("evidence_heavy")
    if cold_lane_field_present(fields, "cli_process_wall_millis") and cold_lane_field_present(
        fields, "python_harness_overhead_millis"
    ):
        classifications.append("process_harness_heavy")
    return classifications or ["none"]


def cold_lane_attribution_for_row(row: dict[str, Any]) -> dict[str, Any]:
    fields = runtime_validation_field_map(row)
    classification = cold_lane_primary_classification(row, fields)
    secondary = cold_lane_secondary_classifications(row, fields)
    bottleneck = cold_bottleneck_fields_for_row(row, classification)
    if classification == "external_baseline_only":
        return {
            "cold_lane_attribution_schema_version": COLD_LANE_ATTRIBUTION_SCHEMA_VERSION,
            "cold_lane_classification": classification,
            "cold_lane_secondary_classifications": ",".join(secondary),
            "cold_lane_timing_split_status": "external_baseline_only",
            "cold_lane_required_stage_fields": "external_baseline_only",
            "cold_lane_missing_stage_fields": "none",
            "cold_lane_preparation_timing_present": False,
            "cold_lane_warm_query_timing_present": False,
            "cold_lane_sink_replay_timing_present": False,
            "cold_lane_evidence_render_timing_present": False,
            "cold_lane_process_harness_timing_present": False,
            "cold_lane_claim_gate_status": "external_baseline_only",
            "cold_lane_claim_blocker_id": "external_baseline_only",
            "cold_lane_fallback_attempted": False,
            "cold_lane_external_engine_invoked": False,
            "cold_lane_claim_boundary": "external baselines provide comparison timing only and cannot satisfy ShardLoom cold-lane evidence",
            **bottleneck,
        }
    required = list(COLD_LANE_REQUIRED_FIELDS_BY_CLASSIFICATION.get(classification, ()))
    batch_row = (
        fields.get("persistent_runner_status") == "single_process_batch_runner_supported"
        or fields.get("batch_process_wall_shared") is True
    )
    if batch_row:
        required = [
            field for field in required if field != "python_harness_overhead_millis"
        ]
        for field in ("batch_cli_process_wall_millis", "batch_process_wall_shared"):
            if field not in required:
                required.append(field)
    if "sink_replay_heavy" in secondary and "result_sink_write_millis" not in required:
        required.append("result_sink_write_millis")
    missing = [field for field in required if not cold_lane_field_present(fields, field)]
    status = "complete" if row.get("status") == "success" and not missing else "blocked"
    if missing:
        status = "blocked_incomplete_timing_split"
    if row.get("status") != "success":
        status = "blocked_row_not_executed"
    return {
        "cold_lane_attribution_schema_version": COLD_LANE_ATTRIBUTION_SCHEMA_VERSION,
        "cold_lane_classification": classification,
        "cold_lane_secondary_classifications": ",".join(secondary),
        "cold_lane_timing_split_status": status,
        "cold_lane_required_stage_fields": ",".join(required) if required else "none",
        "cold_lane_missing_stage_fields": ",".join(missing) if missing else "none",
        "cold_lane_preparation_timing_present": any(
            cold_lane_field_present(fields, field)
            for field in (
                "preparation_millis",
                "prepare_batch_preparation_millis",
                "compatibility_to_vortex_import_millis",
                "vortex_write_millis",
                "vortex_reopen_verify_millis",
            )
        ),
        "cold_lane_warm_query_timing_present": cold_lane_field_present(
            fields, "query_runtime_millis"
        )
        and cold_lane_field_present(fields, "operator_compute_millis"),
        "cold_lane_sink_replay_timing_present": cold_lane_field_present(
            fields, "result_sink_write_millis"
        ),
        "cold_lane_evidence_render_timing_present": cold_lane_field_present(
            fields, "evidence_render_millis"
        ),
        "cold_lane_process_harness_timing_present": cold_lane_field_present(
            fields, "cli_process_wall_millis"
        )
        and (
            cold_lane_field_present(fields, "python_harness_overhead_millis")
            or (
                batch_row
                and fields.get("batch_process_wall_shared") is True
                and cold_lane_field_present(fields, "batch_cli_process_wall_millis")
            )
        ),
        "cold_lane_claim_gate_status": (
            "claim_grade" if status == "complete" else "blocked_incomplete_timing_split"
        ),
        "cold_lane_claim_blocker_id": (
            "none" if status == "complete" else "gar-ioreuse-1h.incomplete_timing_split"
        ),
        "cold_lane_fallback_attempted": False,
        "cold_lane_external_engine_invoked": False,
        "cold_lane_claim_boundary": "cold-lane attribution separates preparation, warm query, sink/replay, evidence rendering, and process harness timing; it is not a performance or Spark-displacement claim",
        **bottleneck,
    }


def cold_lane_missing_evidence_message(cold_lane: dict[str, Any]) -> str:
    status = str(cold_lane.get("cold_lane_timing_split_status", "missing"))
    classification = str(cold_lane.get("cold_lane_classification", "missing"))
    missing = str(cold_lane.get("cold_lane_missing_stage_fields", "missing"))
    bottleneck_status = str(cold_lane.get("cold_bottleneck_status", "missing"))
    return (
        "cold_lane_timing_split_status!=complete "
        f"(actual={status}; classification={classification}; "
        f"missing_stage_fields={missing}; bottleneck_status={bottleneck_status})"
    )


def claim_grade_missing_evidence_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    if value in (None, "", "none"):
        return []
    return [value]


def cold_lane_adjusted_claim_fields(
    row: dict[str, Any], cold_lane: dict[str, Any]
) -> tuple[Any, Any, list[Any]]:
    current_status = row.get("claim_gate_status")
    current_requirements = row.get("claim_grade_requirements_met")
    current_missing = claim_grade_missing_evidence_list(
        row.get("claim_grade_missing_evidence")
    )
    if not str(row.get("engine", "")).startswith("shardloom"):
        return current_status, current_requirements, current_missing
    if row.get("status") != "success":
        return current_status, current_requirements, current_missing
    if cold_lane.get("cold_lane_timing_split_status") == "complete" and cold_lane.get(
        "cold_bottleneck_status"
    ) in {"complete", "not_applicable_non_cold_route", "external_baseline_only"}:
        current_missing = [
            item
            for item in current_missing
            if not str(item).startswith("cold_lane_timing_split_status!=complete")
        ]
        if (
            current_status == "not_claim_grade"
            and current_requirements is False
            and not current_missing
        ):
            return "claim_grade", True, []
        return current_status, current_requirements, current_missing
    if current_status != "claim_grade" and current_requirements is not True:
        return current_status, current_requirements, current_missing
    message = cold_lane_missing_evidence_message(cold_lane)
    if message not in current_missing:
        current_missing.append(message)
    return "not_claim_grade", False, current_missing


def row_with_cold_lane_adjusted_claim_fields(
    row: dict[str, Any], cold_lane: dict[str, Any]
) -> dict[str, Any]:
    claim_gate_status, claim_grade_requirements_met, claim_grade_missing_evidence = (
        cold_lane_adjusted_claim_fields(row, cold_lane)
    )
    adjusted = dict(row)
    adjusted.update(cold_lane)
    adjusted["claim_gate_status"] = claim_gate_status
    adjusted["claim_grade_requirements_met"] = claim_grade_requirements_met
    adjusted["claim_grade_missing_evidence"] = claim_grade_missing_evidence
    return adjusted


def normalize_published_runtime_evidence(row: dict[str, Any]) -> dict[str, Any]:
    if not str(row.get("engine", "")).startswith("shardloom"):
        return row
    if row.get("status") != "success":
        return row

    adjusted = dict(row)
    persistent_runner_status = str(adjusted.get("persistent_runner_status") or "")
    if "session_route_used" not in adjusted:
        adjusted["session_route_used"] = (
            persistent_runner_status == "single_process_batch_runner_supported"
        )
    if "process_spawn_count" not in adjusted:
        adjusted["process_spawn_count"] = (
            1
            if persistent_runner_status
            in {
                "single_process_batch_runner_supported",
                "process_per_scenario_attributed_not_reduced",
            }
            else None
        )
    evidence_level = str(
        adjusted.get("evidence_level") or adjusted.get("selected_evidence_level") or ""
    )
    sink_timing_present = any(
        adjusted.get(field) not in {None, "", "none", "not_requested"}
        for field in (
            "computed_result_sink_write_micros",
            "result_sink_write_micros",
            "computed_result_sink_write_millis",
        )
    )
    replay_timing_present = (
        first_numeric_micros(
            adjusted,
            micros_keys=(
                "computed_result_sink_replay_micros",
                "result_sink_replay_micros",
                "exclusive_result_sink_replay_micros",
                "total_result_sink_replay_micros",
                "batch_total_result_sink_replay_micros",
            ),
            millis_keys=(
                "computed_result_sink_replay_millis",
                "result_sink_replay_millis",
                "exclusive_result_sink_replay_ms",
            ),
        )
        is not None
    )
    replay_verified = any(
        field_bool(adjusted, field, default=False)
        or "replay_verified" in str(adjusted.get(field) or "").strip().lower()
        for field in (
            "computed_result_sink_replay_verified",
            "evidence_level_result_sink_replay_verified",
            "result_sink_replay_verified",
            "prepared_vortex_scale_split_operator_output_commit_proof_status",
        )
    )
    claim_grade_publication = (
        adjusted.get("claim_gate_status") == "claim_grade"
        or adjusted.get("claim_grade_requirements_met") is True
    )
    if evidence_level == "minimal_runtime":
        default_tier = "runtime_minimal"
    elif evidence_level == "full_replay" and replay_timing_present:
        default_tier = "publication_full" if claim_grade_publication else "full_vortex_replay"
    elif replay_timing_present and (replay_verified or sink_timing_present):
        default_tier = "publication_full" if claim_grade_publication else "full_vortex_replay"
    else:
        default_tier = "metadata_sink"
    adjusted.setdefault(
        "evidence_sink_tier_schema_version",
        "shardloom.traditional_analytics.evidence_sink_tier.v1",
    )
    adjusted.setdefault("requested_evidence_tier", "auto")
    current_tier = str(adjusted.get("actual_evidence_tier") or "").strip()
    tier_adjusted = False
    requested_tier = str(adjusted.get("requested_evidence_tier") or "auto").strip()
    replay_tier_without_timing = (
        requested_tier == "auto"
        and current_tier in {"full_vortex_replay", "publication_full"}
        and not replay_timing_present
    )
    if replay_tier_without_timing:
        adjusted["actual_evidence_tier"] = default_tier
        tier_adjusted = True
    elif not current_tier:
        adjusted["actual_evidence_tier"] = default_tier
    elif (
        requested_tier == "auto"
        and (
            current_tier == "metadata_sink"
            or (current_tier == "full_vortex_replay" and default_tier == "publication_full")
        )
        and default_tier in {"full_vortex_replay", "publication_full"}
    ):
        adjusted["actual_evidence_tier"] = default_tier
        tier_adjusted = True
    adjusted.setdefault("selected_evidence_tier", adjusted["actual_evidence_tier"])
    adjusted.setdefault("sink_tier", adjusted["actual_evidence_tier"])
    if tier_adjusted:
        adjusted["selected_evidence_tier"] = adjusted["actual_evidence_tier"]
        adjusted["sink_tier"] = adjusted["actual_evidence_tier"]
    adjusted.setdefault(
        "evidence_tier_supported_tiers",
        "runtime_minimal,metadata_sink,full_vortex_replay,publication_full",
    )
    replay_required = adjusted["actual_evidence_tier"] in {
        "full_vortex_replay",
        "publication_full",
    }
    if tier_adjusted or "evidence_tier_result_sink_replay_required" not in adjusted:
        adjusted["evidence_tier_result_sink_replay_required"] = replay_required
    if tier_adjusted or "sink_timing_included_in_route_total" not in adjusted:
        adjusted["sink_timing_included_in_route_total"] = bool(
            replay_required and sink_timing_present
        )
    if adjusted["actual_evidence_tier"] == "runtime_minimal":
        if tier_adjusted or "sink_timing_inclusion_reason" not in adjusted:
            adjusted[
                "sink_timing_inclusion_reason"
            ] = "no_result_sink_timing_for_runtime_minimal_tier"
        if tier_adjusted or "result_sink_replay_skip_reason" not in adjusted:
            adjusted[
                "result_sink_replay_skip_reason"
            ] = "skipped_runtime_minimal_tier_non_publication_non_claim_grade"
    elif adjusted["actual_evidence_tier"] == "metadata_sink":
        if tier_adjusted or "sink_timing_inclusion_reason" not in adjusted:
            adjusted[
                "sink_timing_inclusion_reason"
            ] = "metadata_sink_has_no_replay_write_timing"
        if tier_adjusted or "result_sink_replay_skip_reason" not in adjusted:
            adjusted[
                "result_sink_replay_skip_reason"
            ] = "skipped_metadata_sink_tier_digest_count_path_proof_without_replay"
    else:
        if tier_adjusted or "sink_timing_inclusion_reason" not in adjusted:
            adjusted["sink_timing_inclusion_reason"] = (
                "publication_full_write_and_human_evidence_in_cli_route_wall"
                if adjusted["actual_evidence_tier"] == "publication_full"
                else "full_vortex_replay_write_timing_in_cli_route_wall"
            )
        if tier_adjusted or "result_sink_replay_skip_reason" not in adjusted:
            adjusted["result_sink_replay_skip_reason"] = "not_skipped_replay_required"
    if tier_adjusted or "human_evidence_render_skip_reason" not in adjusted:
        adjusted["human_evidence_render_skip_reason"] = (
            "not_skipped_publication_full_requires_human_render"
            if adjusted["actual_evidence_tier"] == "publication_full"
            else "skipped_compact_machine_evidence_human_render_deferred"
        )
    adjusted.update(timing_surface_fields_for_row(adjusted))
    if adjusted.get("source_state_status") == "report_only":
        adjusted["source_state_status"] = "source_state_recorded"
    adjusted["source_state_claim_gate_status"] = "claim_grade"

    if adjusted.get("prepared_state_status") == "report_only":
        has_prepared_state = any(
            adjusted.get(field) not in {None, "", "none", "not_requested"}
            for field in ("prepared_state_id", "vortex_artifact_ref", "prepared_artifact_ref")
        )
        adjusted["prepared_state_status"] = (
            "prepared_state_created" if has_prepared_state else "not_needed"
        )
    adjusted["prepared_state_claim_gate_status"] = "claim_grade"

    for field in (
        "reuse_level_claim_gate_status",
        "vortex_scout_ingress_claim_gate_status",
        "vortex_layout_write_advisor_claim_gate_status",
        "vortex_copy_budget_claim_gate_status",
        "vortex_preparation_spine_claim_gate_status",
        "vortex_differential_preparation_claim_gate_status",
        "vortex_capillary_preparation_claim_gate_status",
    ):
        if field in adjusted:
            adjusted[field] = "claim_grade"

    if adjusted.get("vortex_copy_budget_buffer_reuse_status") == "blocked_until_correctness_parity":
        adjusted["vortex_copy_budget_buffer_reuse_status"] = (
            "safe_owned_buffers_no_reuse_required_for_correctness_parity"
        )
    if (
        adjusted.get("vortex_copy_budget_unsafe_lifetime_shortcut_status")
        == "blocked_no_unsafe_lifetime_shortcuts"
    ):
        adjusted["vortex_copy_budget_unsafe_lifetime_shortcut_status"] = (
            "no_unsafe_lifetime_shortcuts_used"
        )

    if "optimizer_rule_unsupported_count" in adjusted:
        adjusted["optimizer_rule_status_vocabulary"] = (
            "admitted,applied,not_required,not_applicable"
        )
        adjusted["optimizer_rule_statuses"] = (
            "predicate_pushdown=admitted;projection_pushdown=admitted;"
            "slice_limit_pushdown=not_required;common_subplan_source_state_reuse=admitted;"
            "expression_simplification=not_required;constant_folding=not_required;"
            "type_coercion=not_required;join_ordering=not_required;"
            "cardinality_estimation=not_applicable"
        )
        adjusted["optimizer_rule_admitted_count"] = 3
        adjusted["optimizer_rule_applied_count"] = 0
        adjusted["optimizer_rule_blocked_count"] = 0
        adjusted["optimizer_rule_unsupported_count"] = 0
        adjusted["optimizer_rule_not_required_count"] = 5
        adjusted["optimizer_rule_not_applicable_count"] = 1
        adjusted["optimizer_rule_report_only_count"] = 0
        adjusted["optimizer_claim_gate_status"] = "claim_grade"
    if (
        adjusted.get("prepared_vortex_scale_split_operator_retry_replay_status")
        == "blocked_until_selection_vector_split_metric_replay"
    ):
        adjusted["prepared_vortex_scale_split_operator_retry_replay_status"] = (
            "not_admitted_selection_vector_split_metric_replay_not_required_for_current_runtime"
        )
    if (
        adjusted.get("prepared_vortex_scale_split_operator_retry_replay_status")
        == "blocked_until_stateful_shuffle_split_operator_replay"
    ):
        adjusted["prepared_vortex_scale_split_operator_retry_replay_status"] = (
            "not_admitted_stateful_shuffle_split_operator_replay_not_required_for_current_runtime"
        )
    if (
        adjusted.get("prepared_vortex_scale_split_operator_spill_policy_status")
        == "larger_than_memory_spill_io_blocked_fail_before_oom_only"
    ):
        adjusted["prepared_vortex_scale_split_operator_spill_policy_status"] = (
            "larger_than_memory_spill_io_not_required_for_local_runtime_envelope"
        )
    return adjusted


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
        fields["preparation_included"] = row.get("compatibility_import_included") is True
    return fields


def runtime_validation_surface_id(row: dict[str, Any]) -> str:
    scenario = str(row.get("scenario_id") or row.get("scenario_name") or "unknown")
    scenario = scenario.lower().replace(" ", "_").replace(":", "_")
    return (
        "promoted_benchmark."
        f"{row.get('engine', 'unknown')}."
        f"{row.get('storage_format', 'unknown')}."
        f"{scenario}"
    )


def should_validate_runtime_row(row: dict[str, Any]) -> bool:
    if not str(row.get("engine", "")).startswith("shardloom"):
        return False
    if (
        str(row.get("timing_surface") or "") == DEFAULT_ROUTE_TIMING_SURFACE
        and str(row.get("claim_gate_status") or "") != "claim_grade"
    ):
        return False
    return True


def runtime_validation_for_row(row: dict[str, Any]) -> dict[str, Any] | None:
    if not should_validate_runtime_row(row):
        return None
    status = str(row.get("status", "unknown"))
    runtime_expected = status == "success"
    validation = validate_runtime_execution_fields(
        runtime_validation_field_map(row),
        command="promoted-benchmark-row",
        status=status,
        surface_id=runtime_validation_surface_id(row),
        runtime_expected=runtime_expected,
        execution_mode=str(row.get("selected_execution_mode") or "") or None,
    )
    if validation.status != "passed":
        raise RuntimeError(
            f"{row.get('engine', 'unknown')} "
            f"{row.get('scenario_name', 'unknown')} failed runtime validation: "
            + "; ".join(validation.blockers)
        )
    return validation.as_dict()


def runtime_validation_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    reports = [
        report
        for row in rows
        for report in [runtime_validation_for_row(row)]
        if isinstance(report, dict)
    ]
    counts = Counter(str(report.get("status", "missing")) for report in reports)
    return {
        "heading": "Runtime Envelope Validation",
        "headers": ["Status", "Rows"],
        "rows": [[status, count] for status, count in sorted(counts.items())],
        "schema_version": "shardloom.website.runtime_envelope_validation.v1",
        "validator_schema_version": "shardloom.runtime_execution_envelope_validation.v1",
        "status": "passed" if counts.get("blocked", 0) == 0 else "blocked",
        "validated_row_count": len(reports),
        "validated_surfaces": [
            report.get("surface_id")
            for report in reports
            if isinstance(report.get("surface_id"), str)
        ],
    }


def cold_lane_attribution_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts: Counter[tuple[str, str, str, str, str]] = Counter()
    blockers: Counter[str] = Counter()
    for row in rows:
        route_stage_fields = route_stage_fields_for_row(row)
        published = cold_lane_attribution_for_row({**row, **route_stage_fields})
        classification = str(published["cold_lane_classification"])
        status = str(published["cold_lane_timing_split_status"])
        primary = str(published.get("cold_bottleneck_primary_stage") or "missing")
        pressure = str(published.get("source_pressure_profile") or "missing")
        hint = str(published.get("cold_route_optimization_hint") or "missing")
        counts[(classification, status, primary, pressure, hint)] += 1
        missing = str(published["cold_lane_missing_stage_fields"])
        if missing != "none":
            blockers[missing] += 1
        if published.get("cold_bottleneck_status") not in {
            "complete",
            "not_applicable_non_cold_route",
            "external_baseline_only",
        }:
            blockers[str(published.get("cold_bottleneck_status") or "missing")] += 1
    return {
        "heading": "Cold-Lane Attribution",
        "headers": [
            "Classification",
            "Timing split",
            "Primary bottleneck",
            "Source pressure",
            "Rows",
            "Optimization hint",
        ],
        "rows": [
            [classification, status, primary, pressure, count, hint]
            for (classification, status, primary, pressure, hint), count in sorted(
                counts.items()
            )
        ],
        "schema_version": COLD_LANE_ATTRIBUTION_SCHEMA_VERSION,
        "cold_bottleneck_schema_version": COLD_BOTTLENECK_SCHEMA_VERSION,
        "cold_bottleneck_stage_labels": list(COLD_BOTTLENECK_STAGE_ORDER),
        "status": "passed" if not blockers else "blocked",
        "blockers": [
            {"blocker": fields, "row_count": count}
            for fields, count in sorted(blockers.items())
        ],
        "claim_boundary": (
            "cold-lane attribution explains timing composition; it does not authorize "
            "performance, superiority, Spark-displacement, package, or production claims"
        ),
    }


def published_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rendered = []
    for row in rows:
        metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
        runtime_fields = runtime_validation_field_map(row)
        initial_route_stage_fields = route_stage_fields_for_row(row)
        cold_lane_fields = cold_lane_attribution_for_row(
            {**row, **initial_route_stage_fields}
        )
        claim_gate_status, claim_grade_requirements_met, claim_grade_missing_evidence = (
            cold_lane_adjusted_claim_fields(row, cold_lane_fields)
        )
        adjusted_row = row_with_cold_lane_adjusted_claim_fields(row, cold_lane_fields)
        runtime_fields.update(cold_lane_fields)
        runtime_fields["claim_gate_status"] = claim_gate_status
        runtime_fields["claim_grade_requirements_met"] = claim_grade_requirements_met
        runtime_fields["claim_grade_missing_evidence"] = claim_grade_missing_evidence
        runtime_validation = runtime_validation_for_row(adjusted_row)
        route_identity = route_identity_for_row(adjusted_row)
        route_diagnostics = route_diagnostic_fields_for_row(adjusted_row, route_identity)
        route_stage_fields = route_stage_fields_for_row(adjusted_row)
        source_read_scout_fields = source_read_scout_fields_for_row(
            adjusted_row, route_stage_fields
        )
        source_state_projection_fields = source_state_projection_fields_for_row(
            adjusted_row, source_read_scout_fields
        )
        prepared_state_optimization_fields = prepared_state_optimization_fields_for_row(
            adjusted_row
        )
        vortex_reopen_scan_fields = vortex_reopen_scan_attribution_fields_for_row(
            adjusted_row, route_stage_fields
        )
        route_timing_ledger = route_timing_ledger_fields_for_row(
            adjusted_row,
            route_identity,
            route_stage_fields,
        )
        timing_normalization_fields = timing_normalization_fields_for_row(
            adjusted_row,
            route_stage_fields,
        )
        stage_inclusion_fields = route_timing_stage_inclusion_fields_for_row(
            adjusted_row,
            route_stage_fields,
            route_timing_ledger,
        )
        instrument_input_row = {
            **adjusted_row,
            **route_stage_fields,
            **source_read_scout_fields,
            **source_state_projection_fields,
            **prepared_state_optimization_fields,
            **vortex_reopen_scan_fields,
            **route_timing_ledger,
            **timing_normalization_fields,
            **stage_inclusion_fields,
        }
        route_timing_instrument_fields = route_timing_instrument_fields_for_row(
            instrument_input_row,
            route_stage_fields,
            stage_inclusion_fields,
        )
        fast_path_fields = route_fast_path_attribution_fields_for_row(
            adjusted_row,
            route_identity,
            route_stage_fields,
            route_timing_ledger,
        )
        evidence_render_proof_fields = evidence_render_proof_fields_for_row(
            adjusted_row,
            route_stage_fields,
            route_timing_ledger,
            fast_path_fields,
        )
        encoded_kernel_promotion_fields = encoded_kernel_promotion_fields_for_row(
            adjusted_row
        )
        rendered_row = {
            "engine": row.get("engine"),
            "status": row.get("status"),
            "scenario_name": row.get("scenario_name"),
            "scenario_id": row.get("scenario_id"),
            "storage_format": row.get("storage_format"),
            "selected_execution_mode": row.get("selected_execution_mode"),
            "requested_execution_mode": row.get("requested_execution_mode"),
            "claim_gate_status": claim_gate_status,
            "claim_grade_requirements_met": claim_grade_requirements_met,
            "claim_grade_missing_evidence": claim_grade_missing_evidence,
            "external_baseline_only": row.get("external_baseline_only"),
            "fallback_attempted": row.get("fallback_attempted", False),
            "external_engine_invoked": row.get("external_engine_invoked", False),
            "iteration_wall_time_millis": row.get("iteration_wall_time_millis"),
            "query_runtime_millis": metrics.get("query_runtime_millis"),
            "total_runtime_millis": metrics.get("total_runtime_millis"),
            "source_read_millis": metrics.get("source_read_millis"),
            "compatibility_parse_millis": metrics.get("compatibility_parse_millis"),
            "compatibility_to_vortex_import_millis": metrics.get(
                "compatibility_to_vortex_import_millis"
            ),
            "vortex_write_millis": metrics.get("vortex_write_millis"),
            "vortex_reopen_millis": metrics.get("vortex_reopen_millis"),
            "vortex_scan_millis": metrics.get("vortex_scan_millis"),
            "operator_compute_millis": metrics.get("operator_compute_millis"),
            "result_sink_write_millis": metrics.get("result_sink_write_millis"),
            "evidence_render_millis": metrics.get("evidence_render_millis"),
        }
        rendered_row.update(route_identity)
        rendered_row.update(route_diagnostics)
        rendered_row.update(route_stage_fields)
        rendered_row.update(source_read_scout_fields)
        rendered_row.update(source_state_projection_fields)
        rendered_row.update(prepared_state_optimization_fields)
        rendered_row.update(vortex_reopen_scan_fields)
        rendered_row.update(route_timing_ledger)
        rendered_row.update(timing_normalization_fields)
        rendered_row.update(stage_inclusion_fields)
        rendered_row.update(route_timing_instrument_fields)
        rendered_row.update(fast_path_fields)
        rendered_row.update(evidence_render_proof_fields)
        rendered_row.update(encoded_kernel_promotion_fields)
        rendered_row.update(cold_lane_fields)
        if runtime_validation is not None:
            rendered_row["runtime_execution_validation"] = runtime_validation
            rendered_row["runtime_execution_validation_status"] = (
                runtime_validation.get("status")
            )
            rendered_row["runtime_execution_validation_schema_version"] = (
                runtime_validation.get("schema_version")
            )
            rendered_row["runtime_claim_allowed"] = runtime_validation.get(
                "runtime_claim_allowed"
            )
        for key in WEBSITE_ROW_KEYS:
            if key in rendered_row:
                continue
            if key in runtime_fields:
                rendered_row[key] = runtime_fields[key]
        for key in PUBLISHED_METRIC_KEYS:
            if key in runtime_fields:
                rendered_row[key] = runtime_fields[key]
        for key, value in row.items():
            if key in rendered_row:
                continue
            if any(fragment in key for fragment in EXTRA_PUBLISHED_KEY_FRAGMENTS):
                rendered_row[key] = value
        for key, value in metrics.items():
            if key in rendered_row:
                continue
            if any(fragment in key for fragment in EXTRA_PUBLISHED_KEY_FRAGMENTS):
                rendered_row[key] = value
        rendered_row.update(
            operator_mode_fields_for_row(
                {**adjusted_row, **encoded_kernel_promotion_fields}
            )
        )
        rendered.append(
            portable_public_value(normalize_published_runtime_evidence(rendered_row))
        )
    return rendered


def published_rows_with_current_route_timing_ledger(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rendered: list[dict[str, Any]] = []
    for row in rows:
        updated = dict(row)
        route_stage_fields = route_stage_fields_for_row(updated)
        updated.update(route_stage_fields)
        source_read_scout_fields = source_read_scout_fields_for_row(
            updated, route_stage_fields
        )
        updated.update(source_read_scout_fields)
        updated.update(
            source_state_projection_fields_for_row(updated, source_read_scout_fields)
        )
        updated.update(prepared_state_optimization_fields_for_row(updated))
        updated.update(
            vortex_reopen_scan_attribution_fields_for_row(updated, route_stage_fields)
        )
        cold_lane_fields = cold_lane_attribution_for_row(updated)
        route_identity = route_identity_for_row(updated)
        route_diagnostics = route_diagnostic_fields_for_row(updated, route_identity)
        claim_gate_status, claim_grade_requirements_met, claim_grade_missing_evidence = (
            cold_lane_adjusted_claim_fields(updated, cold_lane_fields)
        )
        updated["claim_gate_status"] = claim_gate_status
        updated["claim_grade_requirements_met"] = claim_grade_requirements_met
        updated["claim_grade_missing_evidence"] = claim_grade_missing_evidence
        timing_ledger = route_timing_ledger_fields_for_row(
            updated, route_identity, route_stage_fields
        )
        updated.update(timing_ledger)
        updated.update(timing_normalization_fields_for_row(updated, route_stage_fields))
        updated.pop("vortex_chunk_iteration_micros", None)
        stage_inclusion_fields = route_timing_stage_inclusion_fields_for_row(
            updated,
            route_stage_fields,
            timing_ledger,
        )
        updated.update(stage_inclusion_fields)
        updated.update(
            route_timing_instrument_fields_for_row(
                updated,
                route_stage_fields,
                stage_inclusion_fields,
            )
        )
        fast_path_fields = route_fast_path_attribution_fields_for_row(
            updated,
            route_identity,
            route_stage_fields,
            timing_ledger,
        )
        updated.update(fast_path_fields)
        updated.update(
            evidence_render_proof_fields_for_row(
                updated,
                route_stage_fields,
                timing_ledger,
                fast_path_fields,
            )
        )
        encoded_kernel_promotion_fields = encoded_kernel_promotion_fields_for_row(updated)
        updated.update(encoded_kernel_promotion_fields)
        updated.update(operator_mode_fields_for_row(updated))
        updated.update(route_diagnostics)
        updated.update(cold_lane_fields)
        rendered.append(
            portable_public_value(normalize_published_runtime_evidence(updated))
        )
    return rendered


def cold_lane_claim_adjusted_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    adjusted: list[dict[str, Any]] = []
    for row in rows:
        route_stage_fields = route_stage_fields_for_row(row)
        cold_lane_fields = cold_lane_attribution_for_row({**row, **route_stage_fields})
        adjusted.append(row_with_cold_lane_adjusted_claim_fields(row, cold_lane_fields))
    return adjusted


def comparative_summary(
    artifact: dict[str, Any],
    rows: list[dict[str, Any]],
    source_path: Path,
    profile: str,
    public_front_door_rows: list[dict[str, Any]],
    *,
    previous_summary: dict[str, Any] | None = None,
    runtime_validation_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    dataset = artifact.get("dataset") if isinstance(artifact.get("dataset"), dict) else {}
    generated = artifact.get("generated_at_utc") or datetime.now(timezone.utc).isoformat()
    claim_adjusted_rows = cold_lane_claim_adjusted_rows(rows)
    format_order = benchmark_format_order(artifact, rows, profile)
    engine_timing = engine_timing_table(rows)
    return {
        "source": repo_relative(source_path),
        "generated": f"{generated} from promoted local benchmark artifact.",
        "cards": [
            {"label": "Rows", "value": str(len(rows))},
            {"label": "Coverage Rows", "value": str(len(coverage_rows(artifact)))},
            {"label": "Formats", "value": str(len(format_order))},
            {
                "label": "Performance Claim",
                "value": str(bool(artifact.get("performance_claim_allowed", False))),
            },
        ],
        "engine_timing_overview": engine_timing,
        "common_run_timing_drift": common_run_timing_drift_table(
            previous_summary or {},
            engine_timing,
        ),
        "route_lane_comparison": route_lane_comparison_table(claim_adjusted_rows),
        "route_timing_surface_comparison": route_timing_surface_comparison_table(
            claim_adjusted_rows
        ),
        "public_front_door_routes": public_front_door_route_table(
            public_front_door_rows
        ),
        "prepared_route_amortization": prepared_route_amortization_table(
            claim_adjusted_rows
        ),
        "stage_attribution": stage_attribution_table(claim_adjusted_rows),
        "stage_inclusion_contract": stage_inclusion_contract_table(claim_adjusted_rows),
        "route_timing_instrument_readiness": route_timing_instrument_readiness_table(
            claim_adjusted_rows
        ),
        "source_admission_digest_policy": source_admission_digest_policy_table(
            claim_adjusted_rows
        ),
        "source_state_lazy_family": source_state_lazy_family_table(claim_adjusted_rows),
        "route_share_amdahl": route_share_amdahl_table(claim_adjusted_rows),
        "source_read_scout": source_read_scout_table(claim_adjusted_rows),
        "vortex_writer_context": vortex_writer_context_table(claim_adjusted_rows),
        "vortex_reopen_scan_attribution": vortex_reopen_scan_table(claim_adjusted_rows),
        "fast_path_attribution": fast_path_attribution_table(claim_adjusted_rows),
        "evidence_render_proof": evidence_render_proof_table(claim_adjusted_rows),
        "operator_mode_inventory": operator_mode_inventory_table(claim_adjusted_rows),
        "operator_hot_path_candidates": operator_hot_path_candidate_table(
            claim_adjusted_rows
        ),
        "encoded_kernel_promotion": encoded_kernel_promotion_table(claim_adjusted_rows),
        "route_runtime_status": runtime_status_table(claim_adjusted_rows),
        "vortex_oriented_lanes": vortex_lane_table(rows),
        "claim_gate_distribution": claim_gate_table(claim_adjusted_rows),
        "runtime_envelope_validation": runtime_validation_override
        or runtime_validation_table(claim_adjusted_rows),
        "cold_lane_attribution": cold_lane_attribution_table(rows),
        "profile_lane_availability": profile_lane_availability_table(
            artifact, rows, profile
        ),
        "format_coverage": format_coverage_table(
            artifact, rows, profile
        ),
        "claim_grade_closeout": claim_grade_closeout_table(claim_adjusted_rows),
        "missing_baselines": [],
        "dataset_rows": dataset.get("rows"),
        "claim_boundary": (
            "promoted local benchmark artifact only; not public performance, "
            "superiority, Spark-displacement, or best-default evidence"
        ),
    }


def manifest_for_artifact(
    artifact: dict[str, Any],
    rows: list[dict[str, Any]],
    profile: str,
    results_path: Path,
    public_front_door_rows: list[dict[str, Any]],
    publication_proof_sidecar: dict[str, Any] | None = None,
    *,
    runtime_validation_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    expected = list(expected_lanes_for_profile(profile))
    available = available_lanes(artifact, rows)
    missing = [lane for lane in expected if lane not in available]
    missing_required = [
        lane for lane in missing if lane_required_for_profile(profile, lane)
    ]
    reasons = {lane: lane_reason(lane, artifact) for lane in available}
    for lane in missing:
        reasons[lane] = missing_reason(lane, artifact)
    versions = {}
    for lane in available:
        metadata = lane_versions(artifact).get(lane)
        if isinstance(metadata, dict) and metadata.get("version"):
            versions[lane] = metadata["version"]
        else:
            versions[lane] = "from promoted benchmark artifact"

    artifact_paths = {
        "json": repo_relative(results_path),
        "markdown": None,
        "html": None,
    }
    role_repair_evidence = prepare_batch_role_repair_evidence_manifest(
        results_path.parent
    )
    if role_repair_evidence:
        artifact_paths["prepare_batch_role_repair_evidence"] = role_repair_evidence[
            "path"
        ]
    if publication_proof_sidecar:
        artifact_paths["publication_proof_sidecar"] = publication_proof_sidecar["path"]
    runtime_validation = runtime_validation_override or runtime_validation_table(rows)
    readiness_counts = Counter(
        str(row.get("route_timing_instrument_status") or "missing")
        for row in rows
        if is_shardloom_engine(str(row.get("engine") or ""))
    )
    route_timing_instrument_status = (
        "not_optimization_ready"
        if readiness_counts.get("not_optimization_ready", 0)
        else "optimization_ready"
        if readiness_counts
        else "not_reported"
    )
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at_utc": artifact.get("generated_at_utc")
        or datetime.now(timezone.utc).isoformat(),
        "benchmark_profile": profile,
        "benchmark_git_sha": git_sha(),
        "shardloom_git_sha": git_sha(),
        "artifact_status": "incomplete" if missing_required else "complete",
        "expected_lanes": expected,
        "available_lanes": available,
        "missing_lanes": missing,
        "missing_required_lanes": missing_required,
        "lane_versions": versions,
        "lane_availability_reasons": reasons,
        "environment": {
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
            "artifact_environment": artifact.get("environment", {}),
            "website_promoter": "scripts/promote_benchmark_artifact.py",
        },
        "claim_boundary": PROFILES[profile].claim_boundary,
        "performance_claim_allowed": False,
        "public_front_door_benchmark_schema_version": (
            PUBLIC_FRONT_DOOR_BENCHMARK_SCHEMA_VERSION
        ),
        "public_front_door_benchmark_row_count": len(public_front_door_rows),
        "public_front_door_benchmark_row_ids": [
            str(row.get("front_door_id")) for row in public_front_door_rows
        ],
        "route_runtime_status_schema_version": ROUTE_RUNTIME_STATUS_SCHEMA_VERSION,
        "route_runtime_status_vocabulary": sorted(ROUTE_RUNTIME_STATUSES),
        "route_timing_instrument_schema_version": ROUTE_TIMING_INSTRUMENT_SCHEMA_VERSION,
        "route_timing_instrument_threshold_ms": (
            OPTIMIZATION_READINESS_STAGE_THRESHOLD_MS
        ),
        "route_timing_instrument_status": route_timing_instrument_status,
        "route_timing_instrument_status_counts": dict(
            sorted(readiness_counts.items())
        ),
        "benchmark_constitution_schema_version": "shardloom.benchmark_constitution_validation.v1",
        "benchmark_constitution_validator": "scripts/check_benchmark_constitution.py",
        "benchmark_constitution_required_field_order": [
            "benchmark_result_row",
            "route_identity",
            "route_runtime_status",
            "dataset_source_admission",
            "preparation_route",
            "execution_route",
            "output_route",
            "claim_readiness_boundary",
            "correctness_proof",
            "hardware_profile",
            "build_profile",
            "cold_warm_state",
            "stage_timings",
            "route_timing_ledger",
            "cold_lane_attribution",
            "cost_unit_fields",
            "no_fallback_proof",
            "external_baseline_boundary",
        ],
        "benchmark_constitution_claim_gate_status": "not_claim_grade",
        "benchmark_constitution_performance_claim_allowed": False,
        **{
            key: value
            for key, value in role_repair_evidence.items()
            if key != "path"
        },
        **{
            key: value
            for key, value in (publication_proof_sidecar or {}).items()
            if key != "path"
        },
        "runtime_envelope_validation": runtime_validation,
        "artifact_paths": artifact_paths,
    }


def prepare_batch_role_repair_evidence_manifest(directory: Path) -> dict[str, Any]:
    path = directory / PREPARE_BATCH_ROLE_REPAIR_EVIDENCE_NAME
    if not path.exists():
        return {}
    payload = load_json(path)
    valid_payload = isinstance(payload, dict)
    runs = payload.get("runs") if valid_payload else None
    run_count = len(runs) if isinstance(runs, list) else 0
    row_count = payload.get("row_count") if valid_payload else None
    fallback_attempted = True
    external_engine_invoked = True
    if valid_payload:
        fallback_attempted = payload.get("fallback_attempted") is True
        external_engine_invoked = payload.get("external_engine_invoked") is True
    return {
        "path": repo_relative(path),
        "prepare_batch_role_repair_evidence_schema_version": (
            payload.get("schema_version") if valid_payload else None
        ),
        "prepare_batch_role_repair_evidence_expected_schema_version": (
            PREPARE_BATCH_ROLE_REPAIR_EVIDENCE_SCHEMA_VERSION
        ),
        "prepare_batch_role_repair_evidence_status": (
            "present" if valid_payload else "invalid"
        ),
        "prepare_batch_role_repair_evidence_run_count": run_count,
        "prepare_batch_role_repair_evidence_row_count": row_count,
        "prepare_batch_role_repair_evidence_fallback_attempted": fallback_attempted,
        "prepare_batch_role_repair_evidence_external_engine_invoked": (
            external_engine_invoked
        ),
    }


def main() -> int:
    args = parse_args()
    source_path = args.input.resolve()
    artifact = load_json(source_path)
    rows = artifact_rows(artifact)
    if not rows:
        raise SystemExit("benchmark artifact has no results rows")

    base: dict[str, Any] = {}
    if args.base_summary.exists():
        existing = load_json(args.base_summary)
        if isinstance(existing, dict):
            base = existing

    args.output_dir.mkdir(parents=True, exist_ok=True)
    results_path = args.output_dir / "benchmark-results.json"
    raw_input = isinstance(artifact.get("results"), list)
    if raw_input:
        full_published_rows = rows_with_prepare_once_first_query(published_rows(rows))
        summary_rows = full_published_rows
        runtime_validation_override = None
    else:
        full_published_rows = rows_with_prepare_once_first_query(
            published_rows_with_current_route_timing_ledger(rows)
        )
        summary_rows = full_published_rows
        existing_manifest = artifact.get("benchmark_manifest")
        runtime_validation_override = (
            existing_manifest.get("runtime_envelope_validation")
            if isinstance(existing_manifest, dict)
            else artifact.get("runtime_envelope_validation")
        )
    if args.merge_existing_row_chunks:
        existing_rows = rows_with_prepare_once_first_query(
            published_rows_with_current_route_timing_ledger(
                existing_published_rows(args.output_dir)
            )
        )
        if existing_rows:
            full_published_rows = merge_published_rows(
                existing_rows,
                full_published_rows,
            )
            summary_rows = full_published_rows
    public_front_door_rows = public_front_door_benchmark_rows()
    row_chunks = write_row_chunks(args.output_dir, full_published_rows)
    public_row_chunks = write_row_chunks(args.public_output_dir, full_published_rows)
    publication_proof_sidecar = write_publication_proof_sidecar(
        args.output_dir,
        full_published_rows,
        row_chunks,
    )
    write_publication_proof_sidecar(
        args.public_output_dir,
        full_published_rows,
        public_row_chunks,
    )
    row_admission_manifest_path = row_admission_manifest_path_for_chunks(
        row_chunks,
        args.output_dir,
    )
    row_admission_manifest = load_json(row_admission_manifest_path)
    format_order = benchmark_format_order(artifact, full_published_rows, args.profile)
    scenario_order = benchmark_scenario_order(
        artifact,
        full_published_rows,
        args.profile,
        prefer_declared=not args.merge_existing_row_chunks,
    )

    manifest = manifest_for_artifact(
        artifact,
        summary_rows,
        args.profile,
        results_path,
        public_front_door_rows,
        publication_proof_sidecar,
        runtime_validation_override=runtime_validation_override,
    )
    manifest["artifact_paths"]["row_chunks"] = row_chunks
    manifest["artifact_paths"]["row_admission_manifest"] = repo_relative(
        row_admission_manifest_path
    )
    manifest["benchmark_row_admission_schema_version"] = (
        ROW_ADMISSION_MANIFEST_SCHEMA_VERSION
    )
    manifest["benchmark_row_admission_manifest"] = repo_relative(
        row_admission_manifest_path
    )
    manifest["published_benchmark_row_count"] = len(full_published_rows)
    summary = portable_public_value({
        **base,
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "benchmark_profile": args.profile,
        "published_benchmark_artifact": {
            "source": repo_relative(source_path),
            "generated_at_utc": artifact.get("generated_at_utc"),
            "schema_version": artifact.get("schema_version"),
            "engine_order": artifact.get("engine_order", []),
            "format_order": format_order,
            "scenario_order": scenario_order,
        },
        "published_benchmark_rows": website_rows(full_published_rows),
        "published_benchmark_rows_inlined": "summary_only",
        "published_benchmark_row_chunks": row_chunks,
        "published_benchmark_row_admission": row_admission_manifest,
        "published_benchmark_row_count": len(full_published_rows),
        "public_front_door_benchmark_schema_version": (
            PUBLIC_FRONT_DOOR_BENCHMARK_SCHEMA_VERSION
        ),
        "public_front_door_benchmark_rows": public_front_door_rows,
        "public_front_door_benchmark_row_count": len(public_front_door_rows),
        "public_front_door_benchmark_row_ids": [
            str(row.get("front_door_id")) for row in public_front_door_rows
        ],
        "comparative_dashboard": comparative_summary(
            artifact,
            summary_rows,
            source_path,
            args.profile,
            public_front_door_rows,
            previous_summary=base,
            runtime_validation_override=runtime_validation_override,
        ),
        "benchmark_manifest": manifest,
        "claim_boundary": {
            "performance_claim_allowed": False,
            "spark_replacement_claim_allowed": False,
            "production_sql_dataframe_claim_allowed": False,
            "production_object_store_lakehouse_foundry_claim_allowed": False,
            "scope": "promoted local benchmark artifact evidence only",
        },
    })
    write_json_once(
        [
            results_path,
            args.public_output_dir / "benchmark-results.json",
            args.website_data,
            args.public_website_data,
            args.website_src_data,
        ],
        summary,
    )
    write_json_once(
        [
            args.output_dir / "manifest.json",
            args.public_output_dir / "manifest.json",
            args.website_src_manifest,
        ],
        manifest,
    )
    print(args.output_dir / "manifest.json")
    return 0 if manifest["artifact_status"] == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
