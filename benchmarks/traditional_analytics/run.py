#!/usr/bin/env python
"""Run traditional analytics benchmarks against external baseline engines.

This script is benchmark tooling only. It must not be imported by ShardLoom
runtime code and must not be used as fallback execution for unsupported
ShardLoom plans.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib
import json
import math
import os
import platform
import shutil
import statistics
import sys
import threading
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


ENGINE_ORDER = (
    "shardloom",
    "shardloom-vortex",
    "pandas",
    "polars",
    "duckdb",
    "spark-default",
    "spark-local-tuned",
    "datafusion",
    "dask",
)
ENGINE_CHOICES = ENGINE_ORDER + (
    "shardloom-prepared-vortex",
    "shardloom-direct-transient",
)
ENGINE_ALIASES = {"spark": ("spark-default", "spark-local-tuned")}
BENCHMARK_SUITE = "local_analytics"
SHARDLOOM_EXECUTION_MODE_VOCABULARY = (
    "auto",
    "compatibility_import_certified",
    "prepared_vortex",
    "native_vortex",
    "direct_compatibility_transient",
)
EXTERNAL_BASELINE_EXECUTION_MODE = "external_baseline_only"
NATIVE_UNSUPPORTED_COVERAGE_REF = (
    "compute-capability-matrix://native_unsupported_coverage.v1"
)
NATIVE_IO_SOURCE_SINK_COVERAGE_REF = (
    "native-io-envelope-plan://source_sink_coverage.v1"
)
VORTEX_SOURCE_SPLIT_ADMISSION_REF = (
    "vortex-api-inventory://source_split_admission.v1"
)
VORTEX_SEGMENT_EXTRACTION_ADMISSION_REF = (
    "vortex-api-inventory://segment_extraction_admission.v1"
)
VORTEX_LAYOUT_DEVICE_MANAGED_BOUNDARY_REF = (
    "vortex-runtime-utilization-audit://layout_device_managed_boundary.v1"
)
NATIVE_MICROBENCHMARK_SCHEMA_VERSION = (
    "shardloom.traditional_analytics.native_microbenchmark.v1"
)
NATIVE_MICROBENCHMARK_CLAIM_BOUNDARY = (
    "native microbenchmark rows are subsystem optimization evidence only; they are not "
    "end-to-end performance, superiority, Spark-replacement, production, SQL/DataFrame, "
    "object-store/lakehouse, Foundry, or package-publication claims"
)
NATIVE_MICROBENCHMARK_REQUIRED_FAMILIES: dict[str, dict[str, str]] = {
    "vortex_scan_only": {
        "name": "Vortex scan-only primitive",
        "primitive": "vortex_scan_only",
        "subsystem": "vortex_scan_source",
        "optimization_question": (
            "What is the local Vortex scan/source overhead without operator, sink, or evidence "
            "rendering work?"
        ),
        "support_status": "blocked",
        "reason": (
            "no isolated scan-only CLI primitive exists yet; current vortex-run count/projection "
            "rows are scan-plus-operator proxies"
        ),
    },
    "filter_predicate_only": {
        "name": "filter predicate only",
        "primitive": "count-where",
        "subsystem": "predicate_admission_and_selection",
        "optimization_question": (
            "What is the local predicate admission/selection cost without treating it as an "
            "end-to-end query benchmark?"
        ),
        "support_status": "smoke_supported",
        "reason": "covered by current vortex-run count-where native microbenchmark rows",
    },
    "projection_only": {
        "name": "projection only",
        "primitive": "project",
        "subsystem": "projection_pushdown",
        "optimization_question": (
            "What is the local projection path cost and materialization/decode posture?"
        ),
        "support_status": "smoke_supported",
        "reason": "covered by the current vortex-run project native microbenchmark row",
    },
    "group_by_kernel": {
        "name": "group-by kernel",
        "primitive": "group_by_kernel",
        "subsystem": "group_by_kernel",
        "optimization_question": (
            "What is the local group-by kernel cost isolated from source preparation and output?"
        ),
        "support_status": "blocked",
        "reason": "no isolated native group-by kernel microbenchmark primitive exists yet",
    },
    "hash_join_kernel": {
        "name": "hash join kernel",
        "primitive": "hash_join_kernel",
        "subsystem": "hash_join_kernel",
        "optimization_question": (
            "What is the local hash-join kernel build/probe cost isolated from source preparation "
            "and output?"
        ),
        "support_status": "blocked",
        "reason": "no isolated native hash-join kernel microbenchmark primitive exists yet",
    },
    "top_k": {
        "name": "top-k kernel",
        "primitive": "top_k",
        "subsystem": "top_k_kernel",
        "optimization_question": (
            "What is the local top-k selection cost isolated from full scenario execution?"
        ),
        "support_status": "blocked",
        "reason": "no isolated native top-k microbenchmark primitive exists yet",
    },
    "result_sink_write": {
        "name": "result-sink write",
        "primitive": "result_sink_write",
        "subsystem": "result_sink_writer",
        "optimization_question": (
            "What is the local result-sink write overhead without full scenario compute?"
        ),
        "support_status": "blocked",
        "reason": (
            "result-sink write evidence exists in full_replay traditional rows, but no isolated "
            "native result-sink microbenchmark primitive exists yet"
        ),
    },
    "evidence_render": {
        "name": "evidence render",
        "primitive": "evidence_render",
        "subsystem": "benchmark_evidence_renderer",
        "optimization_question": (
            "What is the benchmark evidence serialization/rendering cost for native rows?"
        ),
        "support_status": "smoke_supported",
        "reason": "covered by the benchmark harness evidence-render microbenchmark row",
    },
}
MATERIALIZATION_POLICY_REF = (
    "compute-capability-matrix://materialization_policy.v1"
)
EXECUTION_MODE_CONTRACT_FIELDS = (
    "requested_execution_mode",
    "selected_execution_mode",
    "mode_selection_reason",
    "execution_mode_family",
    "vortex_native_claim_allowed",
    "compatibility_import_included",
    "vortex_prepare_included",
    "vortex_write_reopen_included",
    "direct_transient_execution",
    "claim_gate_status",
)
STAGE_TIMING_CONTRACT_FIELDS = (
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
)
OPERATOR_BLOCKER_MATRIX_FIELDS = (
    "operator_execution_class",
    "operator_admission_status",
    "operator_blocker_id",
    "operator_blocker_reason",
    "operator_encoded_native_claim_allowed",
)
FUSED_PIPELINE_FIELDS = (
    "fused_pipeline_schema_version",
    "fused_pipeline_report_id",
    "fused_pipeline_scope",
    "fused_pipeline_planned_family_count",
    "fused_pipeline_family_statuses",
    "fused_pipeline_used",
    "fused_operator_family",
    "intermediate_materialization_avoided",
    "fused_pipeline_rows_scanned",
    "fused_pipeline_rows_selected",
    "fused_pipeline_rows_output",
    "fused_pipeline_filter_columns",
    "fused_pipeline_projection_columns",
    "fused_pipeline_selection_vector_consumed",
    "fused_pipeline_selection_vector_status",
    "fused_pipeline_correctness_digest_status",
    "fused_pipeline_unfused_correctness_digest",
    "fused_pipeline_fused_correctness_digest",
    "fused_pipeline_correctness_digest_match",
    "fused_pipeline_unfused_reference_status",
    "fused_pipeline_data_decoded",
    "fused_pipeline_data_materialized",
    "fused_pipeline_operator_execution_class",
    "fused_pipeline_encoded_native_claim_allowed",
    "fused_pipeline_claim_gate_status",
    "fused_pipeline_blocker_id",
    "fused_pipeline_blocker_reason",
    "fused_pipeline_claim_boundary",
    "fused_pipeline_fallback_attempted",
    "fused_pipeline_external_engine_invoked",
)
COMPRESSED_KERNEL_REGISTRY_FIELDS = (
    "compressed_kernel_registry_schema_version",
    "compressed_kernel_registry_scope",
    "compressed_kernel_registry_current_surface",
    "compressed_kernel_registry_vortex_first_decision",
    "compressed_kernel_registry_initial_pair_count",
    "compressed_kernel_registry_pairs_classified",
    "compressed_kernel_registry_pair_ids",
    "compressed_kernel_registry_pair_statuses",
    "compressed_kernel_registry_encoding_ids",
    "compressed_kernel_registry_logical_dtypes",
    "compressed_kernel_registry_physical_encodings",
    "compressed_kernel_registry_operator_families",
    "compressed_kernel_registry_kernel_admitted",
    "compressed_kernel_registry_kernel_executed",
    "compressed_kernel_registry_canonicalization_required",
    "compressed_kernel_registry_decoded",
    "compressed_kernel_registry_materialized",
    "compressed_kernel_registry_selection_vector_emitted",
    "compressed_kernel_registry_validity_semantics",
    "compressed_kernel_registry_unsupported_kernel_reasons",
    "compressed_kernel_registry_encoded_native_claim_allowed",
    "compressed_kernel_registry_admitted_pair_count",
    "compressed_kernel_registry_executed_pair_count",
    "compressed_kernel_registry_blocked_pair_count",
    "compressed_kernel_registry_not_available_pair_count",
    "compressed_kernel_registry_claim_gate_status",
    "compressed_kernel_registry_fallback_attempted",
    "compressed_kernel_registry_external_engine_invoked",
)
SCAN_PUSHDOWN_FIELDS = (
    "scan_pushdown_schema_version",
    "scan_pushdown_status",
    "scan_filter_pushed_down",
    "scan_projection_pushed_down",
    "scan_limit_pushed_down",
    "scan_filter_pushdown_status",
    "scan_projection_pushdown_status",
    "scan_limit_pushdown_status",
    "scan_filter_columns_read",
    "scan_output_columns_read",
    "scan_filter_only_columns_read",
    "scan_data_materialized",
    "scan_data_decoded",
    "scan_pushdown_blocker_id",
    "scan_pushdown_claim_gate_status",
    "scan_pushdown_fallback_attempted",
    "scan_pushdown_external_engine_invoked",
)
PERSISTENT_RUNNER_STATUS = "process_per_scenario_attributed_not_reduced"
BATCH_RUNNER_STATUS = "single_process_batch_runner_supported"
BATCH_PROCESS_STARTUP_ATTRIBUTION = "single_process_batch_cli_wall_shared_across_scenarios"
BATCH_HARNESS_OVERHEAD_STATUS = "batch_outer_harness_wall_minus_cli_process_wall_not_row_allocated"
PERSISTENT_RUNNER_ADMISSION_FIELDS = (
    "persistent_runner_status",
    "process_startup_attribution",
    "python_harness_overhead_status",
    "cli_process_wall_millis",
    "python_harness_overhead_millis",
    "startup_warmup_millis",
    "build_time_millis",
    "build_time_excluded",
    "preparation_millis",
    "preparation_cli_process_wall_millis",
    "preparation_included_in_timing",
)
BATCH_RUNNER_ADMISSION_FIELDS = (
    "batch_runner_kind",
    "batch_scenario_count",
    "batch_cli_process_wall_millis",
    "batch_process_wall_shared",
    "batch_process_startup_attribution",
    "source_state_reuse_status",
    "source_state_coverage_schema_version",
    "source_state_coverage_matrix_ref",
    "source_state_coverage_status_vocabulary",
    "source_state_coverage_all_requested_scenarios_classified",
    "source_state_coverage_matrix",
    "source_state_coverage_reused_scenario_count",
    "source_state_coverage_not_needed_scenario_count",
    "source_state_coverage_blocked_scenario_count",
    "source_state_coverage_unsupported_scenario_count",
    "source_state_digest_status",
    "source_state_digest_reason",
    "source_state_prepare_micros",
    "source_state_prepare_timing_scope",
    "source_state_family_count",
    "source_state_dimension_label_reuse_status",
    "source_state_category_metric_reuse_status",
    "source_state_group_category_metric_reuse_status",
    "source_state_ranked_metric_reuse_status",
    "source_state_selective_filter_reuse_status",
    "source_state_dirty_input_reuse_status",
    "source_state_date_null_metric_reuse_status",
)
SOURCE_STATE_CONTRACT_SCHEMA_VERSION = "shardloom.traditional_analytics.source_state.v1"
SOURCE_STATE_CONTRACT_STATUS_VOCABULARY = (
    "source_state_reuse_supported",
    "not_needed",
    "blocked",
    "unsupported",
    "report_only",
    "external_baseline_only",
)
SOURCE_STATE_CONTRACT_FIELDS = (
    "source_state_contract_schema_version",
    "source_state_status_vocabulary",
    "source_state_status",
    "source_state_id",
    "source_state_digest",
    "source_format",
    "source_location",
    "source_fingerprint_kind",
    "schema_digest",
    "row_count_known",
    "file_count",
    "byte_size",
    "partition_columns",
    "compression",
    "source_state_reuse_allowed",
    "source_discovery_millis",
    "schema_inference_millis",
    "source_parse_millis",
    "parse_decode_plan_digest",
    "source_state_reuse_hit",
    "source_state_reuse_reason",
    "source_state_materialization_decode_boundary_ref",
    "source_state_fallback_attempted",
    "source_state_external_engine_invoked",
    "source_state_claim_gate_status",
    "source_state_claim_boundary",
)
PREPARED_STATE_CONTRACT_SCHEMA_VERSION = (
    "shardloom.traditional_analytics.vortex_prepared_state.v1"
)
PREPARED_STATE_CONTRACT_STATUS_VOCABULARY = (
    "prepared_state_reuse_supported",
    "not_needed",
    "blocked",
    "unsupported",
    "report_only",
    "external_baseline_only",
)
PREPARED_STATE_CONTRACT_FIELDS = (
    "prepared_state_contract_schema_version",
    "prepared_state_status_vocabulary",
    "prepared_state_status",
    "prepared_state_id",
    "prepared_state_digest",
    "prepared_state_source_state_id",
    "vortex_artifact_ref",
    "vortex_artifact_digest",
    "layout_summary",
    "encoding_summary",
    "statistics_summary",
    "prepared_state_reuse_allowed",
    "prepared_state_reuse_hit",
    "prepared_state_reuse_reason",
    "preparation_included_in_timing",
    "vortex_prepare_millis",
    "prepared_state_materialization_decode_boundary_ref",
    "prepared_state_native_io_certificate_status",
    "prepared_state_fallback_attempted",
    "prepared_state_external_engine_invoked",
    "prepared_state_claim_gate_status",
    "prepared_state_claim_boundary",
)
OUTPUT_PLAN_CONTRACT_SCHEMA_VERSION = "shardloom.traditional_analytics.output_plan.v1"
OUTPUT_PLAN_CONTRACT_STATUS_VOCABULARY = (
    "output_plan_supported",
    "not_needed",
    "blocked",
    "unsupported",
    "report_only",
    "external_baseline_only",
)
OUTPUT_PLAN_CONTRACT_FIELDS = (
    "output_plan_contract_schema_version",
    "output_plan_status_vocabulary",
    "output_plan_status",
    "output_plan_id",
    "output_plan_digest",
    "output_format",
    "output_location",
    "output_schema_digest",
    "output_partitioning",
    "output_compression",
    "output_encoding",
    "output_write_mode",
    "output_plan_reuse_allowed",
    "output_metadata_preservation_status",
    "output_materialization_required",
    "output_plan_reuse_hit",
    "output_plan_reuse_reason",
    "output_plan_millis",
    "output_write_millis",
    "result_replay_verified",
    "output_native_io_certificate_status",
    "sink_artifact_ref",
    "sink_artifact_digest",
    "output_plan_fallback_attempted",
    "output_plan_external_engine_invoked",
    "output_plan_claim_gate_status",
    "output_plan_claim_boundary",
)
FANOUT_BENCHMARK_SCHEMA_VERSION = (
    "shardloom.traditional_analytics.io_reuse_and_fanout.v1"
)
FANOUT_BENCHMARK_STATUS_VOCABULARY = (
    "runtime_supported",
    "fixture_smoke",
    "report_only",
    "blocked",
    "unsupported",
)
FANOUT_BENCHMARK_FIELDS = (
    "benchmark_family",
    "fanout_case_id",
    "source_format",
    "prepared_state_required",
    "generated_source_required",
    "requested_output_formats",
    "currently_proven_output_formats",
    "blocked_output_formats",
    "fanout_status",
    "fanout_blocker_id",
    "fanout_blocker_reason",
    "source_discovery_millis",
    "schema_inference_millis",
    "source_parse_millis",
    "vortex_prepare_millis",
    "operator_compute_millis",
    "output_plan_millis",
    "output_write_millis",
    "output_replay_millis",
    "total_runtime_millis",
    "source_state_reuse_hit",
    "prepared_state_reuse_hit",
    "output_plan_reuse_hit",
    "fanout_output_count",
    "fallback_attempted",
    "external_engine_invoked",
    "claim_gate_status",
    "claim_boundary",
)
CACHE_INVALIDATION_SCHEMA_VERSION = (
    "shardloom.traditional_analytics.cache_invalidation.v1"
)
CACHE_INVALIDATION_STATUS_VOCABULARY = (
    "cache_valid",
    "invalidated",
    "not_needed",
    "blocked",
    "unsupported",
    "report_only",
    "external_baseline_only",
)
CACHE_INVALIDATION_CONTRACT_FIELDS = (
    "cache_invalidation_contract_schema_version",
    "cache_invalidation_status_vocabulary",
    "cache_invalidation_status",
    "cache_invalidation_layer_scope",
    "source_fingerprint_kind",
    "source_content_digest",
    "source_mtime",
    "source_size",
    "object_etag",
    "manifest_version",
    "schema_digest",
    "plan_digest",
    "output_plan_digest",
    "cache_valid",
    "invalidation_reason",
    "cache_invalidation_fallback_attempted",
    "cache_invalidation_external_engine_invoked",
    "cache_invalidation_claim_gate_status",
    "cache_invalidation_redaction_status",
    "cache_invalidation_claim_boundary",
)
REUSE_LEVEL_SCHEMA_VERSION = (
    "shardloom.traditional_analytics.evidence_safe_reuse_levels.v1"
)
REUSE_LEVEL_SUPPORTED_LEVELS = (
    "discovery_reuse",
    "schema_reuse",
    "parse_plan_reuse",
    "prepared_vortex_reuse",
    "operator_source_state_reuse",
    "output_plan_reuse",
    "result_replay_reuse",
)
REUSE_LEVEL_STATUS_VOCABULARY = (
    "reuse_hit",
    "reuse_miss",
    "not_needed",
    "blocked",
    "unsupported",
    "invalidated",
    "report_only",
)
REUSE_LEVEL_CONTRACT_FIELDS = (
    "reuse_level_contract_schema_version",
    "reuse_level_status_vocabulary",
    "reuse_level_supported_levels",
    "reuse_level_matrix_ref",
    "reuse_level_summary_digest",
    "reuse_level_hit_count",
    "reuse_level_allowed_count",
    "reuse_level_claim_gate_status",
    "claim_grade_requirements_met",
    "reuse_level_fallback_attempted",
    "reuse_level_external_engine_invoked",
    "reuse_level_claim_boundary",
)
FANOUT_BENCHMARK_CASES = (
    {
        "fanout_case_id": "csv_to_parquet_jsonl_vortex_outputs",
        "source_format": "csv",
        "prepared_state_required": True,
        "generated_source_required": False,
        "requested_output_formats": ("parquet", "jsonl", "vortex"),
        "blocker_id": "gar-ioreuse-1d.cross_format_output_runtime_not_implemented",
    },
    {
        "fanout_case_id": "parquet_to_csv_vortex_outputs",
        "source_format": "parquet",
        "prepared_state_required": True,
        "generated_source_required": False,
        "requested_output_formats": ("csv", "vortex"),
        "blocker_id": "gar-ioreuse-1d.cross_format_output_runtime_not_implemented",
    },
    {
        "fanout_case_id": "jsonl_to_parquet_vortex_outputs",
        "source_format": "jsonl",
        "prepared_state_required": True,
        "generated_source_required": False,
        "requested_output_formats": ("parquet", "vortex"),
        "blocker_id": "gar-ioreuse-1d.cross_format_output_runtime_not_implemented",
    },
    {
        "fanout_case_id": "generated_source_to_csv_parquet_vortex_outputs",
        "source_format": "generated_source",
        "prepared_state_required": True,
        "generated_source_required": True,
        "requested_output_formats": ("csv", "parquet", "vortex"),
        "blocker_id": "gar-ioreuse-1d.generated_source_output_runtime_not_implemented",
    },
    {
        "fanout_case_id": "prepared_vortex_to_multiple_output_formats",
        "source_format": "prepared_vortex",
        "prepared_state_required": True,
        "generated_source_required": False,
        "requested_output_formats": ("csv", "jsonl", "parquet", "vortex"),
        "blocker_id": "gar-ioreuse-1d.multi_output_fanout_runtime_not_implemented",
    },
)
SESSION_RUNTIME_FIELDS = (
    "session_schema_version",
    "session_id",
    "session_runtime_status",
    "session_state_scope",
    "session_owner",
    "session_open_status",
    "session_close_status",
    "session_drop_status",
    "session_lifecycle_explicit_close_required",
    "session_hidden_global_cache",
    "session_daemon_or_service",
    "session_requested_scenario_count",
    "session_prepared_artifact_registry_status",
    "session_prepared_artifact_registry_entry_count",
    "session_prepared_artifact_cache_hit_count",
    "session_prepared_artifact_cache_miss_count",
    "session_prepared_artifact_reuse_count",
    "session_source_metadata_cache_hit_count",
    "session_source_metadata_cache_miss_count",
    "session_source_state_cache_hit_count",
    "session_source_state_cache_miss_count",
    "session_source_state_reuse_count",
    "session_schema_cache_status",
    "session_schema_cache_hit_count",
    "session_schema_cache_miss_count",
    "session_dictionary_cache_status",
    "session_dictionary_cache_hit_count",
    "session_dictionary_cache_miss_count",
    "session_buffer_pool_status",
    "session_buffer_pool_reuse_count",
    "session_kernel_registry_ref",
    "session_evidence_recorder_ref",
    "session_fallback_attempted",
    "session_external_engine_invoked",
    "session_claim_gate_status",
    "session_claim_boundary",
)
ALLOCATION_RESOURCE_PROFILE_FIELDS = (
    "allocation_profile_schema_version",
    "allocation_profile_status",
    "allocation_profile_scope",
    "allocation_profile_family_status",
    "allocation_count",
    "allocation_count_status",
    "allocation_bytes",
    "allocation_bytes_status",
    "buffer_pool_enabled",
    "buffer_pool_scope",
    "buffer_reuse_count",
    "buffer_reuse_family",
    "buffer_reuse_blocker",
    "peak_rss_delta",
    "peak_rss_delta_status",
    "source_state_digest",
    "output_digest",
    "correctness_digest",
    "evidence_regression_status",
    "unsafe_lifetime_shortcut_used",
    "allocation_fallback_attempted",
    "allocation_external_engine_invoked",
    "allocation_claim_gate_status",
    "allocation_claim_boundary",
)
RUNTIME_EVIDENCE_LEVEL_FIELDS = (
    "runtime_evidence_level_schema_version",
    "requested_evidence_level",
    "selected_evidence_level",
    "evidence_level",
    "evidence_level_supported_levels",
    "evidence_level_claim_gate_status",
    "evidence_level_result_sink_replay_required",
    "evidence_level_result_sink_replay_requested",
    "evidence_level_result_sink_replay_verified",
    "evidence_level_native_io_certificate_required",
    "evidence_level_certificate_refs",
    "evidence_level_result_sink_replay_refs",
    "evidence_level_source_state_digest",
    "evidence_level_output_digest",
    "evidence_level_fallback_attempted",
    "evidence_level_external_engine_invoked",
    "evidence_level_claim_boundary",
)
OPTIMIZER_TRACE_SCHEMA_VERSION = "shardloom.evidence_aware_optimizer_trace.v1"
OPTIMIZER_TRACE_FIELDS = (
    "optimizer_trace_schema_version",
    "optimizer_trace_id",
    "optimizer_registry_version",
    "optimizer_phase",
    "optimizer_rule_status_vocabulary",
    "optimizer_rule_order",
    "optimizer_rule_statuses",
    "optimizer_rule_admitted_count",
    "optimizer_rule_applied_count",
    "optimizer_rule_blocked_count",
    "optimizer_rule_unsupported_count",
    "optimizer_rule_not_applicable_count",
    "optimizer_rule_report_only_count",
    "optimizer_before_plan_digest_status",
    "optimizer_after_plan_digest_status",
    "optimizer_rewrite_safety_status",
    "optimizer_evidence_preserved",
    "optimizer_no_fallback_preserved",
    "optimizer_claim_boundary_preserved",
    "optimizer_materialization_boundary_preserved",
    "optimizer_source_state_reuse_admitted",
    "optimizer_cardinality_estimation_status",
    "optimizer_correctness_smoke_ref",
    "optimizer_fallback_attempted",
    "optimizer_external_engine_invoked",
    "optimizer_claim_gate_status",
    "optimizer_benchmark_trace_ref",
    "optimizer_claim_boundary",
)
BUILD_PROFILE_SCHEMA_VERSION = "shardloom.traditional_analytics.build_profile.v1"
BUILD_PROFILE_STATUS_VOCABULARY = (
    "portable_release_baseline",
    "portable_lto",
    "pgo_report_only",
    "host_native_benchmark_only",
    "development",
    "external_baseline_only",
)
BUILD_PROFILE_FIELDS = (
    "build_profile_schema_version",
    "build_profile",
    "build_profile_kind",
    "rustc_version",
    "cargo_version",
    "target_triple",
    "target_cpu_policy",
    "target_cpu_native_enabled",
    "lto_enabled",
    "lto_mode",
    "codegen_units",
    "pgo_status",
    "pgo_profile_generate_status",
    "pgo_profile_use_status",
    "pgo_profile_artifact_ref",
    "pgo_training_workload_ref",
    "pgo_training_workload_digest",
    "build_reproducibility_status",
    "portable_release_artifact",
    "benchmark_only_build",
    "build_profile_correctness_digest",
    "build_profile_fallback_attempted",
    "build_profile_external_engine_invoked",
    "build_profile_claim_gate_status",
    "build_profile_claim_boundary",
)
WORK_AVOIDANCE_STATUS_VOCABULARY = (
    "measured",
    "not_available",
    "unsupported",
    "not_applicable",
)
WORK_AVOIDANCE_METRICS = (
    "rows_avoided",
    "segments_pruned",
    "bytes_avoided",
    "encoded_vector_reuse",
    "pushdown_proof",
)
WORK_AVOIDANCE_EVIDENCE_FIELDS = (
    "work_avoidance_schema_ref",
    "work_avoidance_status_vocabulary",
    "work_avoidance_rows_avoided_status",
    "work_avoidance_rows_avoided_value",
    "work_avoidance_rows_avoided_reason",
    "work_avoidance_segments_pruned_status",
    "work_avoidance_segments_pruned_value",
    "work_avoidance_segments_pruned_reason",
    "work_avoidance_bytes_avoided_status",
    "work_avoidance_bytes_avoided_value",
    "work_avoidance_bytes_avoided_reason",
    "work_avoidance_encoded_vector_reuse_status",
    "work_avoidance_encoded_vector_reuse_value",
    "work_avoidance_encoded_vector_reuse_reason",
    "work_avoidance_pushdown_proof_status",
    "work_avoidance_pushdown_proof_value",
    "work_avoidance_pushdown_proof_reason",
    "work_avoidance_claim_allowed",
    "work_avoidance_claim_boundary",
)
DEFAULT_DATASET_PROFILE = "narrow_fact_dim"
GENERATED_DATASET_PROFILES = (
    "tiny_smoke",
    "narrow_fact_dim",
    "skewed_keys",
    "high_cardinality_strings",
    "wide_table",
    "very_wide_table",
    "null_heavy",
    "many_small_files",
    "few_large_files",
    "partitioned_by_date",
    "poorly_clustered",
    "well_clustered",
    "schema_drift",
    "dirty_csv",
    "nested_json",
    "cdc_delta_overlay",
)
SCENARIO_ORDER = (
    "csv/file ingest",
    "selective filter",
    "group by aggregation",
    "sort and top-k",
    "hash join",
    "wide projection",
    "distinct count",
)
TAXONOMY_EXTRA_SCENARIO_ORDER = (
    "filter + projection + limit",
    "multi-key group by",
    "join + aggregate",
    "row number window",
    "partition pruning",
    "many-small-files scan",
    "null-heavy aggregate",
    "high-cardinality string group/distinct",
    "top-N per group",
    "clean/cast/filter/write",
    "malformed timestamp / dirty CSV",
    "small change over large base",
    "nested JSON field scan",
)
FORMAT_ORDER = ("csv", "jsonl", "parquet", "arrow-ipc", "avro", "orc")
DEFAULT_FORMAT_ORDER = ("csv", "parquet")
SHARDLOOM_VORTEX_FORMAT = "vortex"
SHARDLOOM_BUILD_TIMINGS: dict[str, float] = {}
SHARDLOOM_BUILD_PROFILE_EVIDENCE_CACHE: dict[str, dict[str, Any]] = {}
STRESS_SCENARIO_ORDER = (
    "scale stress skewed join aggregation",
    "scale stress multi-stage etl",
)
SHARDLOOM_TRADITIONAL_SCENARIOS = SCENARIO_ORDER + STRESS_SCENARIO_ORDER
SHARDLOOM_TAXONOMY_EXTRA_SCENARIOS = (
    "filter + projection + limit",
    "multi-key group by",
    "join + aggregate",
    "row number window",
    "partition pruning",
    "many-small-files scan",
    "null-heavy aggregate",
    "high-cardinality string group/distinct",
    "top-N per group",
    "clean/cast/filter/write",
    "malformed timestamp / dirty CSV",
    "small change over large base",
    "nested JSON field scan",
)
SHARDLOOM_EXECUTABLE_SCENARIOS = (
    SCENARIO_ORDER + SHARDLOOM_TAXONOMY_EXTRA_SCENARIOS + STRESS_SCENARIO_ORDER
)
SCENARIO_BYTES = {
    "csv/file ingest": ("fact",),
    "selective filter": ("fact",),
    "group by aggregation": ("fact",),
    "sort and top-k": ("fact",),
    "hash join": ("fact", "dim"),
    "wide projection": ("fact",),
    "distinct count": ("fact",),
    "filter + projection + limit": ("fact",),
    "multi-key group by": ("fact",),
    "join + aggregate": ("fact", "dim"),
    "row number window": ("fact",),
    "partition pruning": ("fact",),
    "many-small-files scan": ("fact",),
    "null-heavy aggregate": ("fact",),
    "high-cardinality string group/distinct": ("fact",),
    "top-N per group": ("fact",),
    "clean/cast/filter/write": ("fact",),
    "malformed timestamp / dirty CSV": ("fact",),
    "small change over large base": ("fact",),
    "nested JSON field scan": ("fact",),
    "scale stress skewed join aggregation": ("fact", "dim"),
    "scale stress multi-stage etl": ("fact", "dim"),
}
DASK_BLOCKSIZE = "16MB"
DASK_SCHEDULER = "threads"
SHARDLOOM_BUILD_PROFILE = "release"
SHARDLOOM_BUILD_PROFILES = (
    "debug",
    "release",
    "release-lto",
    "release-pgo",
    "release-native-benchmark",
)
SHARDLOOM_RESULT_SINK = False
SHARDLOOM_EVIDENCE_LEVEL = "certified"
MIN_CLAIM_GRADE_ITERATIONS = 3
CLAIM_READINESS_RERUN_ENGINES = (
    "shardloom",
    "shardloom-vortex",
    "pandas",
    "polars",
    "duckdb",
    "datafusion",
)
CLAIM_READINESS_RERUN_FORMATS = ("csv", "parquet")
SHARDLOOM_CLAIM_GRADE_REQUIRED_EVIDENCE = (
    ("workload_scorecard_status", "workload_certified"),
    ("native_io_certificate_status", "certified"),
    ("output_replay_verified", "true"),
    ("computed_result_sink_replay_verified", "true"),
    ("computed_result_sink_native_io_certificate_status", "certified"),
    ("runtime_execution_certificate_status", "certified"),
    ("runtime_fallback_attempted", "false"),
    ("runtime_external_query_engine_invoked", "false"),
    ("layout_advisor_fallback_attempted", "false"),
    ("layout_advisor_external_engine_invoked", "false"),
    ("materialization_boundary_report_emitted", "true"),
    ("native_io_materializing_transitions_have_boundaries", "true"),
)
ROW_CLASSIFICATIONS = (
    "claim_grade",
    "not_claim_grade",
    "fixture_smoke_only",
    "supported",
    "unsupported",
    "blocked",
    "external_baseline_only",
)
CORRECTNESS_FLOAT_DIGITS = 4


@dataclass(frozen=True)
class DatasetPaths:
    root: Path
    fact_csv: Path
    dim_csv: Path
    fact_jsonl: Path
    dim_jsonl: Path
    fact_parquet: Path
    dim_parquet: Path
    fact_arrow_ipc: Path
    dim_arrow_ipc: Path
    fact_avro: Path
    dim_avro: Path
    fact_orc: Path
    dim_orc: Path
    rows: int
    dim_rows: int
    dataset_profile: str = DEFAULT_DATASET_PROFILE
    fact_extra_columns: tuple[str, ...] = ()
    fact_csv_parts_dir: Path | None = None
    fact_jsonl_parts_dir: Path | None = None
    cdc_delta_csv: Path | None = None
    nested_jsonl: Path | None = None


@dataclass(frozen=True)
class EngineRunner:
    name: str
    version: str
    scenarios: dict[str, Callable[[DatasetPaths, str], Any]]
    batch_scenarios: (
        Callable[[DatasetPaths, str, tuple[str, ...]], dict[str, Any]] | None
    ) = None
    formats: tuple[str, ...] = ("csv",)
    prepare: Callable[[DatasetPaths, tuple[str, ...]], None] | None = None
    warmup: Callable[[], None] | None = None
    close: Callable[[], None] | None = None
    startup_time_millis: float | None = None
    preparation_time_millis: float | None = None
    build_time_millis: float | None = None


@dataclass(frozen=True)
class ScenarioMetadata:
    scenario_id: str
    name: str
    suite: str
    category: str
    default: bool
    stress: bool
    executable: bool
    dataset_profiles: tuple[str, ...]
    description: str


class BenchmarkUnsupported(RuntimeError):
    """Raised when an engine cannot execute a benchmark scenario yet."""


def scenario_catalog_path() -> Path:
    return Path(__file__).resolve().parents[1] / "common" / "scenario_catalog.json"


def load_scenario_catalog() -> dict[str, Any]:
    with scenario_catalog_path().open("r", encoding="utf-8") as handle:
        return json.load(handle)


def scenario_metadata_from_catalog(catalog: dict[str, Any]) -> dict[str, ScenarioMetadata]:
    metadata = {}
    for row in catalog["scenarios"]:
        metadata[row["name"]] = ScenarioMetadata(
            scenario_id=row["id"],
            name=row["name"],
            suite=row["suite"],
            category=row["category"],
            default=bool(row["default"]),
            stress=bool(row["stress"]),
            executable=bool(row["executable"]),
            dataset_profiles=tuple(row.get("dataset_profiles", [])),
            description=row.get("description", ""),
        )
    return metadata


SCENARIO_CATALOG = load_scenario_catalog()
SCENARIO_METADATA = scenario_metadata_from_catalog(SCENARIO_CATALOG)
EXECUTABLE_SCENARIO_ORDER = tuple(
    scenario["name"] for scenario in SCENARIO_CATALOG["scenarios"] if scenario["executable"]
)


def scenario_metadata(scenario: str) -> ScenarioMetadata:
    return SCENARIO_METADATA.get(
        scenario,
        ScenarioMetadata(
            scenario_id=scenario_slug(scenario),
            name=scenario,
            suite=BENCHMARK_SUITE,
            category="unknown",
            default=False,
            stress=False,
            executable=False,
            dataset_profiles=(DEFAULT_DATASET_PROFILE,),
            description="scenario is not present in the benchmark catalog",
        ),
    )


def taxonomy_default_scenarios(include_extra: bool, include_stress: bool) -> tuple[str, ...]:
    scenarios = list(SCENARIO_ORDER)
    if include_extra:
        scenarios.extend(TAXONOMY_EXTRA_SCENARIO_ORDER)
    if include_stress:
        scenarios.extend(STRESS_SCENARIO_ORDER)
    return tuple(scenario for scenario in scenarios if scenario in EXECUTABLE_SCENARIO_ORDER)


def scenario_dataset_profile_block_reason(scenario: str, dataset_profile: str) -> str | None:
    metadata = scenario_metadata(scenario)
    if metadata.dataset_profiles and dataset_profile not in metadata.dataset_profiles:
        allowed = ",".join(metadata.dataset_profiles)
        return (
            f"scenario '{scenario}' requires dataset_profile in [{allowed}], "
            f"but current dataset_profile is '{dataset_profile}'"
        )
    return None


def engine_role(engine: str) -> str:
    if engine.startswith("shardloom"):
        return "shardloom_native"
    return "local_baseline"


def is_shardloom_engine(engine: str) -> bool:
    return engine.startswith("shardloom")


def expand_engine_aliases(engine_names: tuple[str, ...]) -> tuple[str, ...]:
    expanded: list[str] = []
    for engine in engine_names:
        for name in ENGINE_ALIASES.get(engine, (engine,)):
            if name not in expanded:
                expanded.append(name)
    return tuple(expanded)


def option_was_provided(option: str, argv: list[str]) -> bool:
    prefix = f"{option}="
    return option in argv or any(arg.startswith(prefix) for arg in argv)


class MemorySampler:
    def __init__(self) -> None:
        self._running = False
        self._thread: threading.Thread | None = None
        self.peak_bytes: int | None = None
        try:
            import psutil  # type: ignore
        except ImportError:
            self._psutil = None
            self._process = None
        else:
            self._psutil = psutil
            self._process = psutil.Process(os.getpid())

    def __enter__(self) -> "MemorySampler":
        if self._process is None:
            return self
        self._running = True
        self._sample()
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self._running = False
        if self._thread is not None:
            self._thread.join(timeout=1.0)
        self._sample()

    def _sample_loop(self) -> None:
        while self._running:
            self._sample()
            time.sleep(0.01)

    def _sample(self) -> None:
        if self._process is None:
            return
        try:
            rss = self._process.memory_info().rss
            for child in self._process.children(recursive=True):
                try:
                    rss += child.memory_info().rss
                except Exception:
                    continue
        except Exception:
            return
        self.peak_bytes = rss if self.peak_bytes is None else max(self.peak_bytes, rss)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--dim-rows", type=int, default=1_000)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument(
        "--engines",
        default=",".join(ENGINE_ORDER),
        help="Comma-separated engines: shardloom,shardloom-vortex,shardloom-prepared-vortex,shardloom-direct-transient,pandas,polars,duckdb,spark-default,spark-local-tuned,datafusion,dask. Alias: spark expands to both Spark profiles.",
    )
    parser.add_argument(
        "--formats",
        default=",".join(DEFAULT_FORMAT_ORDER),
        help="Comma-separated external storage formats to run where supported: csv,jsonl,parquet,arrow-ipc,avro,orc. ShardLoom native/prepared Vortex rows are reported under the requested source formats with separate preparation metadata.",
    )
    parser.add_argument(
        "--scenario",
        action="append",
        choices=EXECUTABLE_SCENARIO_ORDER,
        help="Run one scenario. Repeat to run multiple scenarios.",
    )
    parser.add_argument(
        "--dataset-profile",
        default=DEFAULT_DATASET_PROFILE,
        choices=GENERATED_DATASET_PROFILES,
        help="Generated local dataset profile. Some advanced profiles emit fixture sidecars and remain claim-blocked until comparative coverage is promoted.",
    )
    parser.add_argument(
        "--include-taxonomy-extra",
        action="store_true",
        help="Include opt-in taxonomy scenarios beyond the default local analytics suite.",
    )
    parser.add_argument(
        "--include-stress",
        action="store_true",
        help="Include opt-in scale/shuffle stress scenarios. These are intended for Spark/Dask-style scale testing and may be inappropriate for small local runs.",
    )
    parser.add_argument(
        "--claim-readiness-rerun",
        action="store_true",
        help="Use the P7.4.4 selected local comparative rerun preset: ShardLoom plus local optional baselines, csv/parquet, taxonomy extras, result-sink evidence, no managed platforms, and at least three iterations.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(__file__).resolve().parent / ".generated" / "data",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output JSON path. Defaults to benchmarks/traditional_analytics/results/<timestamp>.json.",
    )
    parser.add_argument(
        "--markdown-output",
        type=Path,
        default=None,
        help="Output Markdown report path. Defaults to the JSON path with .md extension.",
    )
    parser.add_argument("--no-markdown", action="store_true")
    parser.add_argument("--regenerate", action="store_true")
    parser.add_argument(
        "--dask-blocksize",
        default=DASK_BLOCKSIZE,
        help="Dask CSV blocksize, for example 16MB or 64MB. Use 'default' for Dask defaults.",
    )
    parser.add_argument(
        "--dask-scheduler",
        default=DASK_SCHEDULER,
        choices=("threads", "processes", "synchronous"),
        help="Dask scheduler used for compute calls.",
    )
    parser.add_argument(
        "--skip-shardloom-native",
        action="store_true",
        help="Skip ShardLoom native encoded microbenchmarks in the report.",
    )
    parser.add_argument(
        "--shardloom-build-profile",
        default=SHARDLOOM_BUILD_PROFILE,
        choices=SHARDLOOM_BUILD_PROFILES,
        help=(
            "Build profile for the ShardLoom CLI used by benchmark rows. Build time is "
            "excluded from per-scenario timing. release-native-benchmark is host-native "
            "and benchmark-only; release-pgo requires a separately generated profile to "
            "be claim-relevant."
        ),
    )
    parser.add_argument(
        "--cache-mode",
        default="warm-ish-local-filesystem",
        help="Declared cache mode for the report. The harness does not clear OS file cache.",
    )
    parser.add_argument(
        "--timing-scope",
        default="per-scenario operation only; engine initialization excluded",
        help="Human-readable timing scope recorded in the report.",
    )
    parser.add_argument(
        "--shardloom-native-iterations",
        type=int,
        default=None,
        help="Iterations for ShardLoom native microbenchmarks. Defaults to --iterations.",
    )
    parser.add_argument(
        "--shardloom-result-sink",
        action="store_true",
        help="For shardloom rows, also write and replay the computed result as a native Vortex result artifact.",
    )
    parser.add_argument(
        "--shardloom-evidence-level",
        choices=("minimal_runtime", "certified", "full_replay"),
        default=None,
        help="Evidence depth for prepared/native batch rows. Defaults to certified, or full_replay when --shardloom-result-sink is enabled.",
    )
    parser.add_argument(
        "--require-all-engines",
        action="store_true",
        help="Return nonzero after writing artifacts if any selected engine dependency is missing.",
    )
    argv = sys.argv[1:]
    args = parser.parse_args()
    if args.rows <= 0:
        parser.error("--rows must be greater than zero")
    if args.dim_rows <= 0:
        parser.error("--dim-rows must be greater than zero")
    if args.iterations <= 0:
        parser.error("--iterations must be greater than zero")
    explicit_engines = option_was_provided("--engines", argv)
    explicit_formats = option_was_provided("--formats", argv)
    explicit_scenario = option_was_provided("--scenario", argv)
    explicit_skip_native = option_was_provided("--skip-shardloom-native", argv)
    if args.claim_readiness_rerun and args.iterations < MIN_CLAIM_GRADE_ITERATIONS:
        parser.error(
            f"--claim-readiness-rerun requires --iterations >= {MIN_CLAIM_GRADE_ITERATIONS}"
        )
    requested_engine_source = (
        ",".join(CLAIM_READINESS_RERUN_ENGINES)
        if args.claim_readiness_rerun and not explicit_engines
        else args.engines
    )
    requested_engines = tuple(
        engine.strip().lower()
        for engine in requested_engine_source.split(",")
        if engine.strip()
    )
    engines = expand_engine_aliases(requested_engines)
    unknown = sorted(set(engines) - set(ENGINE_CHOICES))
    if unknown:
        parser.error(f"unknown engines: {','.join(unknown)}")
    args.engine_list = engines
    requested_format_source = (
        ",".join(CLAIM_READINESS_RERUN_FORMATS)
        if args.claim_readiness_rerun and not explicit_formats
        else args.formats
    )
    requested_formats = tuple(
        data_format.strip().lower()
        for data_format in requested_format_source.split(",")
        if data_format.strip()
    )
    unknown_formats = sorted(set(requested_formats) - set(FORMAT_ORDER))
    if unknown_formats:
        parser.error(f"unknown formats: {','.join(unknown_formats)}")
    if not requested_formats:
        parser.error("--formats must include at least one format")
    args.format_list = requested_formats
    if args.claim_readiness_rerun and not explicit_scenario:
        args.include_taxonomy_extra = True
    if args.claim_readiness_rerun:
        args.shardloom_result_sink = True
        if not explicit_skip_native:
            args.skip_shardloom_native = True
    if args.shardloom_evidence_level is None:
        args.shardloom_evidence_level = (
            "full_replay" if args.shardloom_result_sink else "certified"
        )
    elif args.shardloom_evidence_level == "full_replay":
        args.shardloom_result_sink = True
    elif args.shardloom_result_sink:
        parser.error(
            "--shardloom-result-sink requires --shardloom-evidence-level full_replay"
        )
    if args.scenario:
        args.scenario_list = tuple(args.scenario)
    else:
        args.scenario_list = taxonomy_default_scenarios(
            args.include_taxonomy_extra, args.include_stress
        )
    args.shardloom_native_iterations = args.shardloom_native_iterations or args.iterations
    if args.shardloom_native_iterations <= 0:
        parser.error("--shardloom-native-iterations must be greater than zero")
    return args


def ensure_dataset(
    root: Path,
    rows: int,
    dim_rows: int,
    regenerate: bool,
    requested_formats: tuple[str, ...],
    dataset_profile: str,
) -> DatasetPaths:
    fact_csv = root / "fact.csv"
    dim_csv = root / "dim.csv"
    fact_jsonl = root / "fact.jsonl"
    dim_jsonl = root / "dim.jsonl"
    fact_parquet = root / "fact.parquet"
    dim_parquet = root / "dim.parquet"
    fact_arrow_ipc = root / "fact.arrow"
    dim_arrow_ipc = root / "dim.arrow"
    fact_avro = root / "fact.avro"
    dim_avro = root / "dim.avro"
    fact_orc = root / "fact.orc"
    dim_orc = root / "dim.orc"
    fact_csv_parts_dir = root / "fact_csv_parts"
    fact_jsonl_parts_dir = root / "fact_jsonl_parts"
    cdc_delta_csv = root / "cdc_delta.csv"
    nested_jsonl = root / "nested_fact.jsonl"
    metadata_json = root / "dataset.json"
    if regenerate and root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    fact_extra_columns = generated_fact_extra_columns(dataset_profile)
    expected_metadata = {
        "rows": rows,
        "dim_rows": dim_rows,
        "schema_version": 6,
        "dataset_profile": dataset_profile,
        "dataset_file_shape": dataset_file_shape(dataset_profile),
        "fact_extra_columns": list(fact_extra_columns),
        "fact_file_part_count": fact_file_part_count(dataset_profile, rows),
        "formats": sorted(requested_formats),
    }
    required_paths = [fact_csv, dim_csv]
    if "jsonl" in requested_formats:
        required_paths.extend([fact_jsonl, dim_jsonl])
    if "parquet" in requested_formats:
        required_paths.extend([fact_parquet, dim_parquet])
    if "arrow-ipc" in requested_formats:
        required_paths.extend([fact_arrow_ipc, dim_arrow_ipc])
    if "avro" in requested_formats:
        required_paths.extend([fact_avro, dim_avro])
    if "orc" in requested_formats:
        required_paths.extend([fact_orc, dim_orc])
    if fact_file_part_count(dataset_profile, rows) > 0:
        required_paths.append(fact_csv_parts_dir)
        if "jsonl" in requested_formats:
            required_paths.append(fact_jsonl_parts_dir)
    if dataset_profile == "cdc_delta_overlay":
        required_paths.append(cdc_delta_csv)
    if dataset_profile == "nested_json":
        required_paths.append(nested_jsonl)
    if (
        all(path.exists() for path in required_paths)
        and metadata_json.exists()
    ):
        with metadata_json.open("r", encoding="utf-8") as handle:
            if json.load(handle) == expected_metadata:
                return DatasetPaths(
                    root,
                    fact_csv,
                    dim_csv,
                    fact_jsonl,
                    dim_jsonl,
                    fact_parquet,
                    dim_parquet,
                    fact_arrow_ipc,
                    dim_arrow_ipc,
                    fact_avro,
                    dim_avro,
                    fact_orc,
                    dim_orc,
                    rows,
                    dim_rows,
                    dataset_profile,
                    fact_extra_columns,
                    fact_csv_parts_dir,
                    fact_jsonl_parts_dir,
                    cdc_delta_csv,
                    nested_jsonl,
                )

    with fact_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        fact_columns = [
            "id",
            "group_key",
            "dim_key",
            "value",
            "metric",
            "flag",
            "category",
            *fact_extra_columns,
        ]
        writer.writerow(fact_columns)
        for idx in range(rows):
            group_key = generated_group_key(idx, dataset_profile)
            dim_key = generated_dim_key(idx, dim_rows, dataset_profile)
            value = (idx * 17) % 10_000
            metric = ((idx * 13) % 100_000) / 100.0
            flag = 1 if idx % 7 == 0 else 0
            category = generated_category(idx, group_key, dataset_profile)
            writer.writerow(
                [
                    idx,
                    group_key,
                    dim_key,
                    value,
                    f"{metric:.2f}",
                    flag,
                    category,
                    *generated_extra_fact_values(
                        idx,
                        group_key,
                        dim_key,
                        value,
                        metric,
                        flag,
                        category,
                        dataset_profile,
                        fact_extra_columns,
                    ),
                ]
            )

    with dim_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["dim_key", "dim_label", "weight"])
        for idx in range(dim_rows):
            writer.writerow([idx, f"d{idx % 50}", (idx * 3) % 100])

    if "jsonl" in requested_formats:
        write_jsonl_copies(fact_csv, dim_csv, fact_jsonl, dim_jsonl)

    write_profile_sidecars(
        fact_csv,
        dataset_profile,
        rows,
        requested_formats,
        fact_csv_parts_dir,
        fact_jsonl_parts_dir,
        cdc_delta_csv,
        nested_jsonl,
    )

    with metadata_json.open("w", encoding="utf-8") as handle:
        json.dump(expected_metadata, handle, indent=2, sort_keys=True)
        handle.write("\n")

    if {"parquet", "arrow-ipc", "orc"} & set(requested_formats):
        write_arrow_family_copies(
            fact_csv,
            dim_csv,
            fact_parquet if "parquet" in requested_formats else None,
            dim_parquet if "parquet" in requested_formats else None,
            fact_arrow_ipc if "arrow-ipc" in requested_formats else None,
            dim_arrow_ipc if "arrow-ipc" in requested_formats else None,
            fact_orc if "orc" in requested_formats else None,
            dim_orc if "orc" in requested_formats else None,
        )
    if "avro" in requested_formats:
        write_avro_copies(fact_csv, dim_csv, fact_avro, dim_avro)

    return DatasetPaths(
        root,
        fact_csv,
        dim_csv,
        fact_jsonl,
        dim_jsonl,
        fact_parquet,
        dim_parquet,
        fact_arrow_ipc,
        dim_arrow_ipc,
        fact_avro,
        dim_avro,
        fact_orc,
        dim_orc,
        rows,
        dim_rows,
        dataset_profile,
        fact_extra_columns,
        fact_csv_parts_dir,
        fact_jsonl_parts_dir,
        cdc_delta_csv,
        nested_jsonl,
    )


def dataset_file_shape(dataset_profile: str) -> str:
    if dataset_profile == "many_small_files":
        return "many_small_csv_parts"
    if dataset_profile == "few_large_files":
        return "few_large_csv_parts"
    if dataset_profile == "cdc_delta_overlay":
        return "base_plus_small_change_overlay"
    if dataset_profile in {"schema_drift", "dirty_csv", "nested_json"}:
        return dataset_profile
    return "single_local_files"


def fact_file_part_count(dataset_profile: str, rows: int) -> int:
    if dataset_profile == "many_small_files":
        return max(4, min(rows, 32))
    if dataset_profile == "few_large_files":
        return max(1, min(rows, 2))
    return 0


def write_profile_sidecars(
    fact_csv: Path,
    dataset_profile: str,
    rows: int,
    requested_formats: tuple[str, ...],
    fact_csv_parts_dir: Path,
    fact_jsonl_parts_dir: Path,
    cdc_delta_csv: Path,
    nested_jsonl: Path,
) -> None:
    part_count = fact_file_part_count(dataset_profile, rows)
    if part_count > 0:
        write_csv_parts(fact_csv, fact_csv_parts_dir, part_count)
        if "jsonl" in requested_formats:
            write_jsonl_part_copies(fact_csv_parts_dir, fact_jsonl_parts_dir)
    if dataset_profile == "cdc_delta_overlay":
        write_cdc_delta_overlay(fact_csv, cdc_delta_csv)
    if dataset_profile == "nested_json":
        write_nested_json_fixture(fact_csv, nested_jsonl)


def write_csv_parts(source_csv: Path, target_dir: Path, part_count: int) -> None:
    if target_dir.exists():
        shutil.rmtree(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    with source_csv.open("r", newline="", encoding="utf-8") as source:
        reader = csv.reader(source)
        header = next(reader)
        writers: list[tuple[Any, Any]] = []
        try:
            for index in range(part_count):
                target = (target_dir / f"part-{index:05d}.csv").open(
                    "w", newline="", encoding="utf-8"
                )
                writer = csv.writer(target)
                writer.writerow(header)
                writers.append((target, writer))
            for row_index, row in enumerate(reader):
                writers[row_index % part_count][1].writerow(row)
        finally:
            for handle, _writer in writers:
                handle.close()


def write_jsonl_part_copies(source_dir: Path, target_dir: Path) -> None:
    if target_dir.exists():
        shutil.rmtree(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    for source_csv in sorted(source_dir.glob("part-*.csv")):
        write_jsonl_copy(
            source_csv,
            target_dir / f"{source_csv.stem}.jsonl",
            {
                "id": int,
                "group_key": int,
                "dim_key": int,
                "value": int,
                "metric": float,
                "flag": int,
                "category": str,
            },
        )


def write_cdc_delta_overlay(source_csv: Path, target_csv: Path) -> None:
    with source_csv.open("r", newline="", encoding="utf-8") as source:
        rows = list(csv.DictReader(source))
    overlay_size = max(1, min(len(rows), 24))
    with target_csv.open("w", newline="", encoding="utf-8") as target:
        fieldnames = ["id", "op", "value", "metric", "effective_ts"]
        writer = csv.DictWriter(target, fieldnames=fieldnames)
        writer.writeheader()
        for index, row in enumerate(rows[:overlay_size]):
            op = "delete" if index % 7 == 0 else "update"
            writer.writerow(
                {
                    "id": row["id"],
                    "op": op,
                    "value": "" if op == "delete" else str(int(row["value"]) + 101),
                    "metric": "" if op == "delete" else f"{float(row['metric']) + 1.25:.2f}",
                    "effective_ts": f"2024-12-{(index % 28) + 1:02d}T00:00:00Z",
                }
            )
        for offset in range(max(1, overlay_size // 4)):
            writer.writerow(
                {
                    "id": len(rows) + offset,
                    "op": "insert",
                    "value": 9000 + offset,
                    "metric": f"{250.0 + offset:.2f}",
                    "effective_ts": f"2024-12-{(offset % 28) + 1:02d}T12:00:00Z",
                }
            )


def write_nested_json_fixture(source_csv: Path, target_jsonl: Path) -> None:
    with source_csv.open("r", newline="", encoding="utf-8") as source:
        with target_jsonl.open("w", encoding="utf-8") as target:
            for row in csv.DictReader(source):
                payload = json.loads(row["nested_payload"])
                target.write(
                    json.dumps(
                        {
                            "id": int(row["id"]),
                            "group_key": int(row["group_key"]),
                            "metric": float(row["metric"]),
                            "nested_payload": payload,
                        },
                        separators=(",", ":"),
                    )
                )
                target.write("\n")


def generated_group_key(idx: int, dataset_profile: str) -> int:
    if dataset_profile == "skewed_keys":
        return 0 if idx % 10 < 7 else idx % 100
    if dataset_profile == "well_clustered":
        return (idx // 32) % 100
    if dataset_profile == "poorly_clustered":
        return (idx * 37) % 100
    return idx % 100


def generated_dim_key(idx: int, dim_rows: int, dataset_profile: str) -> int:
    if dataset_profile == "skewed_keys":
        return 0 if idx % 10 < 6 else idx % dim_rows
    if dataset_profile == "well_clustered":
        return (idx // 32) % dim_rows
    if dataset_profile == "poorly_clustered":
        return (idx * 7919) % dim_rows
    return idx % dim_rows


def generated_category(idx: int, group_key: int, dataset_profile: str) -> str:
    if dataset_profile == "high_cardinality_strings":
        return f"c{idx % 10_000}"
    if dataset_profile == "schema_drift":
        return f"c{group_key % 10}_v{1 + (idx % 3)}"
    return f"c{group_key % 10}"


def generated_fact_extra_columns(dataset_profile: str) -> tuple[str, ...]:
    if dataset_profile == "wide_table":
        return tuple(f"extra_metric_{index:02d}" for index in range(16))
    if dataset_profile == "very_wide_table":
        return tuple(f"extra_metric_{index:02d}" for index in range(64))
    if dataset_profile == "null_heavy":
        return tuple(f"nullable_metric_{index:02d}" for index in range(16)) + tuple(
            f"nullable_category_{index:02d}" for index in range(4)
        )
    if dataset_profile in {"many_small_files", "few_large_files"}:
        return ("file_bucket", "event_date")
    if dataset_profile == "partitioned_by_date":
        return ("event_date", "partition_year", "partition_month")
    if dataset_profile in {"poorly_clustered", "well_clustered"}:
        return ("cluster_bucket", "event_date")
    if dataset_profile == "schema_drift":
        return ("schema_version_tag", "optional_metric_v2", "renamed_metric_candidate")
    if dataset_profile == "dirty_csv":
        return ("raw_event_time", "dirty_numeric", "dirty_flag")
    if dataset_profile == "nested_json":
        return ("nested_payload", "nested_group", "nested_score")
    if dataset_profile == "cdc_delta_overlay":
        return ("cdc_op", "cdc_sequence", "effective_ts", "is_deleted")
    return ()


def generated_extra_fact_values(
    idx: int,
    group_key: int,
    dim_key: int,
    value: int,
    metric: float,
    flag: int,
    category: str,
    dataset_profile: str,
    fact_extra_columns: tuple[str, ...],
) -> list[str]:
    values = []
    for column in fact_extra_columns:
        if column.startswith("extra_metric_"):
            column_index = int(column.rsplit("_", 1)[1])
            values.append(f"{((idx + 1) * (column_index + 3)) % 100_000 / 100.0:.2f}")
        elif column.startswith("nullable_metric_"):
            column_index = int(column.rsplit("_", 1)[1])
            if (idx + column_index) % 3 == 0:
                values.append("")
            else:
                values.append(f"{(metric + column_index + (value % 17)):.2f}")
        elif column.startswith("nullable_category_"):
            column_index = int(column.rsplit("_", 1)[1])
            values.append("" if (idx + column_index) % 4 == 0 else category)
        elif column == "event_date":
            values.append(generated_event_date(idx))
        elif column == "partition_year":
            values.append(generated_event_date(idx)[:4])
        elif column == "partition_month":
            values.append(generated_event_date(idx)[5:7])
        elif column == "cluster_bucket":
            cluster_source = group_key if dataset_profile == "well_clustered" else dim_key
            values.append(str(cluster_source % 16))
        elif column == "file_bucket":
            values.append(str(idx % (32 if dataset_profile == "many_small_files" else 2)))
        elif column == "schema_version_tag":
            values.append(f"schema_v{1 + (idx % 3)}")
        elif column == "optional_metric_v2":
            values.append("" if idx % 5 == 0 else f"{metric * 1.1:.2f}")
        elif column == "renamed_metric_candidate":
            values.append(f"{metric:.2f}")
        elif column == "raw_event_time":
            values.append(
                "not-a-timestamp" if idx % 11 == 0 else f"{generated_event_date(idx)}T00:00:00Z"
            )
        elif column == "dirty_numeric":
            values.append("bad-number" if idx % 13 == 0 else str(value))
        elif column == "dirty_flag":
            values.append("Y" if flag else ("?" if idx % 17 == 0 else "N"))
        elif column == "nested_payload":
            values.append(
                json.dumps(
                    {
                        "event": {"date": generated_event_date(idx), "flag": bool(flag)},
                        "metrics": {"value": value, "score": round(metric / 10.0, 4)},
                        "labels": [category, f"g{group_key % 5}"],
                    },
                    separators=(",", ":"),
                )
            )
        elif column == "nested_group":
            values.append(f"g{group_key % 5}")
        elif column == "nested_score":
            values.append(f"{metric / 10.0:.4f}")
        elif column == "cdc_op":
            values.append("base")
        elif column == "cdc_sequence":
            values.append(str(idx))
        elif column == "effective_ts":
            values.append(f"{generated_event_date(idx)}T00:00:00Z")
        elif column == "is_deleted":
            values.append("false")
        else:
            values.append("" if flag else str(value))
    return values


def generated_event_date(idx: int) -> str:
    month = ((idx // 28) % 12) + 1
    day = (idx % 28) + 1
    return f"2024-{month:02d}-{day:02d}"


def write_jsonl_copies(fact_csv: Path, dim_csv: Path, fact_jsonl: Path, dim_jsonl: Path) -> None:
    write_jsonl_copy(
        fact_csv,
        fact_jsonl,
        {
            "id": int,
            "group_key": int,
            "dim_key": int,
            "value": int,
            "metric": float,
            "flag": int,
            "category": str,
        },
    )
    write_jsonl_copy(
        dim_csv,
        dim_jsonl,
        {"dim_key": int, "dim_label": str, "weight": float},
    )


def write_jsonl_copy(source_csv: Path, target_jsonl: Path, converters: dict[str, Callable[[str], Any]]) -> None:
    with source_csv.open("r", newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        with target_jsonl.open("w", encoding="utf-8") as target:
            for row in reader:
                typed = {}
                for key, value in row.items():
                    if key is None or value is None:
                        continue
                    converter = converters.get(key)
                    if converter is None:
                        typed[key] = None if value == "" else value
                    elif value == "":
                        typed[key] = None
                    else:
                        typed[key] = converter(value)
                target.write(json.dumps(typed, separators=(",", ":")))
                target.write("\n")


def write_arrow_family_copies(
    fact_csv: Path,
    dim_csv: Path,
    fact_parquet: Path | None,
    dim_parquet: Path | None,
    fact_arrow_ipc: Path | None,
    dim_arrow_ipc: Path | None,
    fact_orc: Path | None,
    dim_orc: Path | None,
) -> None:
    try:
        import pyarrow as pa  # type: ignore
        import pyarrow.csv as arrow_csv  # type: ignore
        import pyarrow.ipc as ipc  # type: ignore
        import pyarrow.orc as orc  # type: ignore
        import pyarrow.parquet as pq  # type: ignore
    except ImportError as exc:
        raise BenchmarkUnsupported(
            "pyarrow is required to generate Arrow-family benchmark inputs"
        ) from exc

    fact_table = arrow_csv.read_csv(fact_csv)
    dim_table = arrow_csv.read_csv(dim_csv)
    if fact_parquet is not None and dim_parquet is not None:
        pq.write_table(fact_table, fact_parquet)
        pq.write_table(dim_table, dim_parquet)
    if fact_arrow_ipc is not None and dim_arrow_ipc is not None:
        write_arrow_ipc_table(ipc, fact_table, fact_arrow_ipc)
        write_arrow_ipc_table(ipc, dim_table, dim_arrow_ipc)
    if fact_orc is not None and dim_orc is not None:
        orc.write_table(fact_table, fact_orc)
        orc.write_table(dim_table, dim_orc)
    _ = pa


def write_arrow_ipc_table(ipc: Any, table: Any, path: Path) -> None:
    with path.open("wb") as handle:
        with ipc.new_file(handle, table.schema) as writer:
            writer.write_table(table)


def write_avro_copies(fact_csv: Path, dim_csv: Path, fact_avro: Path, dim_avro: Path) -> None:
    try:
        import fastavro  # type: ignore
    except ImportError as exc:
        raise BenchmarkUnsupported(
            "fastavro is required to generate Avro benchmark inputs"
        ) from exc

    fact_schema = fastavro.parse_schema(
        {
            "type": "record",
            "name": "fact",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "group_key", "type": "int"},
                {"name": "dim_key", "type": "int"},
                {"name": "value", "type": "int"},
                {"name": "metric", "type": "double"},
                {"name": "flag", "type": "int"},
                {"name": "category", "type": "string"},
            ],
        }
    )
    dim_schema = fastavro.parse_schema(
        {
            "type": "record",
            "name": "dim",
            "fields": [
                {"name": "dim_key", "type": "int"},
                {"name": "dim_label", "type": "string"},
                {"name": "weight", "type": "double"},
            ],
        }
    )
    write_avro_copy(
        fastavro,
        fact_csv,
        fact_avro,
        fact_schema,
        {
            "id": int,
            "group_key": int,
            "dim_key": int,
            "value": int,
            "metric": float,
            "flag": int,
            "category": str,
        },
    )
    write_avro_copy(
        fastavro,
        dim_csv,
        dim_avro,
        dim_schema,
        {"dim_key": int, "dim_label": str, "weight": float},
    )


def write_avro_copy(
    fastavro: Any,
    source_csv: Path,
    target_avro: Path,
    schema: dict[str, Any],
    converters: dict[str, Callable[[str], Any]],
) -> None:
    schema_fields = {field["name"] for field in schema["fields"]}
    with source_csv.open("r", newline="", encoding="utf-8") as source:
        records = [
            {
                key: converters[key](value)
                for key, value in row.items()
                if key in schema_fields and value is not None
            }
            for row in csv.DictReader(source)
        ]
    with target_avro.open("wb") as target:
        fastavro.writer(target, schema, records)


def module_version(name: str) -> str:
    module = importlib.import_module(name)
    return str(getattr(module, "__version__", "unknown"))


def shardloom_runner() -> EngineRunner:
    root = workspace_root()
    binary = build_shardloom_cli(
        root,
        "vortex-traditional-analytics-benchmark",
        SHARDLOOM_BUILD_PROFILE,
    )
    env = os.environ.copy()
    env["RUSTUP_TOOLCHAIN"] = env.get("RUSTUP_TOOLCHAIN", "1.91.1")

    def shardloom_fact_source_path(
        paths: DatasetPaths, scenario: str, data_format: str
    ) -> Path:
        if scenario == "many-small-files scan":
            parts = fact_part_paths(paths, data_format)
            if not parts:
                raise BenchmarkUnsupported(
                    "many-small-files scan requires split CSV or JSONL fact parts"
                )
            return parts[0].parent
        if scenario == "nested JSON field scan" and data_format not in {
            "jsonl",
            "parquet",
            "arrow-ipc",
        }:
            raise BenchmarkUnsupported(
                "nested JSON field scan requires JSONL or Arrow-family fixture input for ShardLoom"
            )
        return fact_path(paths, data_format)

    def run_scenario(scenario: str, paths: DatasetPaths, data_format: str) -> Any:
        workspace = (
            paths.root / "shardloom_universal_io" / data_format / scenario_slug(scenario)
        )
        command = [
            str(binary),
            "traditional-analytics-run",
            scenario,
            str(shardloom_fact_source_path(paths, scenario, data_format)),
            str(dim_path(paths, data_format)),
            "--workspace",
            str(workspace),
            "--input-format",
            data_format,
            "--format",
            "json",
        ]
        if scenario == "small change over large base":
            if paths.cdc_delta_csv is None or not paths.cdc_delta_csv.exists():
                raise BenchmarkUnsupported("CDC overlay scenario requires cdc_delta.csv")
            command.extend(["--cdc-delta", str(paths.cdc_delta_csv)])
        if SHARDLOOM_RESULT_SINK:
            command.extend(["--verify-native-replay", "--write-result-vortex"])
        completed = subprocess_run(command, root, env)
        try:
            payload = json.loads(completed["stdout"].splitlines()[0])
        except (json.JSONDecodeError, IndexError) as exc:
            if completed["returncode"] != 0:
                raise RuntimeError(
                    completed["stderr"] or completed["stdout"] or "unknown failure"
                ) from exc
            raise RuntimeError(f"ShardLoom emitted invalid JSON: {exc}") from exc
        if completed["returncode"] != 0:
            raise RuntimeError(completed["stderr"] or completed["stdout"] or "unknown failure")
        fields = parse_output_fields(payload)
        fields["cli_process_wall_millis"] = str(
            completed.get("process_wall_millis", "not_measured")
        )
        fields["process_startup_attribution"] = "per_scenario_cli_process_wall_measured"
        fields["python_harness_overhead_status"] = (
            "outer_harness_wall_minus_cli_process_wall"
        )
        fields["build_time_excluded"] = "true"
        fields["persistent_runner_status"] = PERSISTENT_RUNNER_STATUS
        if payload.get("status") != "success":
            reason = fields.get("reason") or payload.get("human_text") or "unsupported"
            raise BenchmarkUnsupported(str(reason))
        required_true_fields = [
            "native_work_envelope_created",
            "native_work_stream_created",
            "native_result_stream_created",
            "native_io_certificate_emitted",
            "compatibility_source_adapter_used",
            "compatibility_to_vortex_import_performed",
            "resource_auto_sizing_enabled",
            "dynamic_sizing_applied",
            "partitioning_auto_derived",
            "vortex_file_written",
            "vortex_file_read",
            "upstream_vortex_scan_called",
            "materialization_boundary_report_emitted",
            "native_io_per_path_certificate_emitted",
            "native_io_materializing_transitions_have_boundaries",
            "runtime_task_graph_created",
            "runtime_task_graph_executed",
            "runtime_queue_limit_enforced",
            "runtime_backpressure_bounded",
            "runtime_cancellation_testable",
            "runtime_retry_testable",
            "runtime_fail_before_oom_enforced",
            "layout_advisor_report_emitted",
        ]
        missing_evidence = [
            field for field in required_true_fields if fields.get(field) != "true"
        ]
        if missing_evidence:
            raise RuntimeError(
                "ShardLoom universal I/O evidence was missing: "
                + ", ".join(missing_evidence)
            )
        if fields.get("native_io_certificate_status") != "certified":
            raise RuntimeError(
                "ShardLoom NativeIoCertificate was not certified: "
                + str(fields.get("native_io_certificate_status", "missing"))
            )
        if SHARDLOOM_RESULT_SINK:
            for field in (
                "computed_result_sink_requested",
                "computed_result_sink_written",
                "computed_result_sink_replay_verified",
            ):
                if fields.get(field) != "true":
                    raise RuntimeError(
                        "ShardLoom result sink evidence was missing: " + field
                    )
            if (
                fields.get("computed_result_sink_native_io_certificate_status")
                != "certified"
            ):
                raise RuntimeError(
                    "ShardLoom result sink NativeIoCertificate was not certified: "
                    + str(
                        fields.get(
                            "computed_result_sink_native_io_certificate_status", "missing"
                        )
                    )
                )
            if fields.get("runtime_execution_certificate_status") != "certified":
                raise RuntimeError(
                    "ShardLoom runtime execution certificate was not certified: "
                    + str(fields.get("runtime_execution_certificate_status", "missing"))
                )
            if fields.get("runtime_fallback_attempted") != "false":
                raise RuntimeError("ShardLoom runtime fallback_attempted was not false")
            if fields.get("runtime_external_query_engine_invoked") != "false":
                raise RuntimeError(
                    "ShardLoom runtime external query engine invocation was not false"
                )
            if fields.get("runtime_memory_reservations_requested") != fields.get(
                "runtime_memory_reservations_released"
            ):
                raise RuntimeError(
                    "ShardLoom runtime memory reservations were not released"
                )
            if fields.get("layout_advisor_status") != "report_only":
                raise RuntimeError(
                    "ShardLoom layout advisor status was not report_only: "
                    + str(fields.get("layout_advisor_status", "missing"))
                )
            if fields.get("layout_advisor_improvement_claim_allowed") != "false":
                raise RuntimeError("ShardLoom layout advisor allowed an improvement claim")
            if fields.get("layout_advisor_write_layout_execution_allowed") != "false":
                raise RuntimeError(
                    "ShardLoom layout advisor allowed write-layout execution"
                )
            if fields.get("layout_advisor_fallback_attempted") != "false":
                raise RuntimeError("ShardLoom layout advisor fallback_attempted was not false")
            if fields.get("layout_advisor_external_engine_invoked") != "false":
                raise RuntimeError(
                    "ShardLoom layout advisor external engine invocation was not false"
                )
        if (
            fields.get("native_io_certificate_path_id")
            != "compatibility_source_to_native_vortex_sink"
        ):
            raise RuntimeError(
                "ShardLoom NativeIoCertificate path was unexpected: "
                + str(fields.get("native_io_certificate_path_id", "missing"))
            )
        if fields.get("source_format") != shardloom_source_format(data_format):
            raise RuntimeError(
                "ShardLoom source format was unexpected: "
                + str(fields.get("source_format", "missing"))
            )
        result_json = fields.get("result_json")
        if result_json is None:
            raise RuntimeError("ShardLoom result_json field was missing")
        return {
            "__benchmark_result": json.loads(result_json),
            "__shardloom_evidence": fields,
        }

    return EngineRunner(
        "shardloom",
        shardloom_version(root, SHARDLOOM_BUILD_PROFILE),
        {
            scenario: (
                lambda paths, data_format, scenario=scenario: run_scenario(
                    scenario, paths, data_format
                )
            )
            for scenario in SHARDLOOM_EXECUTABLE_SCENARIOS
        },
        formats=FORMAT_ORDER,
        build_time_millis=SHARDLOOM_BUILD_TIMINGS.get(str(binary)),
    )


def shardloom_direct_transient_runner() -> EngineRunner:
    root = workspace_root()
    binary = build_shardloom_cli(
        root,
        "vortex-traditional-analytics-benchmark",
        SHARDLOOM_BUILD_PROFILE,
    )
    env = os.environ.copy()
    env["RUSTUP_TOOLCHAIN"] = env.get("RUSTUP_TOOLCHAIN", "1.91.1")

    def run_scenario(scenario: str, paths: DatasetPaths, data_format: str) -> Any:
        if data_format != "csv":
            raise BenchmarkUnsupported(
                "direct transient smoke currently supports CSV input only"
            )
        if scenario not in {"selective filter", "filter + projection + limit"}:
            raise BenchmarkUnsupported(
                "direct transient smoke currently supports only selective filter or filter + projection + limit"
            )
        command = [
            str(binary),
            "traditional-analytics-run",
            scenario,
            str(paths.fact_csv),
            str(paths.dim_csv),
            "--input-format",
            "csv",
            "--execution-mode",
            "direct_compatibility_transient",
            "--format",
            "json",
        ]
        completed = subprocess_run(command, root, env)
        try:
            payload = json.loads(completed["stdout"].splitlines()[0])
        except (json.JSONDecodeError, IndexError) as exc:
            if completed["returncode"] != 0:
                raise RuntimeError(
                    completed["stderr"] or completed["stdout"] or "unknown failure"
                ) from exc
            raise RuntimeError(f"ShardLoom emitted invalid JSON: {exc}") from exc
        if completed["returncode"] != 0:
            raise RuntimeError(completed["stderr"] or completed["stdout"] or "unknown failure")
        fields = parse_output_fields(payload)
        fields["cli_process_wall_millis"] = str(
            completed.get("process_wall_millis", "not_measured")
        )
        fields["process_startup_attribution"] = "per_scenario_cli_process_wall_measured"
        fields["python_harness_overhead_status"] = (
            "outer_harness_wall_minus_cli_process_wall"
        )
        fields["build_time_excluded"] = "true"
        fields["preparation_included_in_timing"] = "false"
        fields["preparation_millis"] = "none"
        fields["persistent_runner_status"] = PERSISTENT_RUNNER_STATUS
        if payload.get("status") != "success":
            reason = fields.get("reason") or payload.get("human_text") or "unsupported"
            raise BenchmarkUnsupported(str(reason))
        required_true_fields = [
            "mode_supported",
            "direct_transient_execution",
            "compatibility_source_adapter_used",
            "csv_source_adapter_used",
            "materialization_boundary_report_emitted",
            "native_io_materializing_transitions_have_boundaries",
            "runtime_task_graph_created",
            "runtime_task_graph_executed",
            "execution_certificate_emitted",
        ]
        missing_true = [
            field for field in required_true_fields if fields.get(field) != "true"
        ]
        if missing_true:
            raise RuntimeError(
                "ShardLoom direct transient evidence was missing: "
                + ", ".join(missing_true)
            )
        required_false_fields = [
            "vortex_native_claim_allowed",
            "compatibility_import_included",
            "vortex_prepare_included",
            "vortex_write_reopen_included",
            "compatibility_to_vortex_import_performed",
            "vortex_file_written",
            "vortex_file_read",
            "upstream_vortex_scan_called",
            "write_io",
            "fallback_attempted",
            "external_engine_invoked",
            "runtime_fallback_attempted",
            "runtime_external_query_engine_invoked",
        ]
        unexpected_true = [
            field for field in required_false_fields if fields.get(field) != "false"
        ]
        if unexpected_true:
            raise RuntimeError(
                "ShardLoom direct transient false evidence was unexpected: "
                + ", ".join(unexpected_true)
            )
        if fields.get("support_status") != "supported":
            raise RuntimeError(
                "ShardLoom direct transient support status was unexpected: "
                + str(fields.get("support_status", "missing"))
            )
        if fields.get("runtime_execution_certificate_status") != "certified":
            raise RuntimeError(
                "ShardLoom direct transient execution certificate was not certified: "
                + str(fields.get("runtime_execution_certificate_status", "missing"))
            )
        if fields.get("native_io_certificate_status") != "not_vortex_native":
            raise RuntimeError(
                "ShardLoom direct transient Native I/O status was unexpected: "
                + str(fields.get("native_io_certificate_status", "missing"))
            )
        expected_refs = {
            "selective filter": (
                "benchmark://local_vortex_analytics_v1/direct_transient_csv_selective_filter",
                "coverage.direct_compatibility_transient.local_csv_selective_filter",
            ),
            "filter + projection + limit": (
                "benchmark://local_vortex_analytics_v1/direct_transient_csv_filter_projection_limit",
                "coverage.direct_compatibility_transient.local_csv_filter_projection_limit",
            ),
        }
        expected_benchmark_ref, expected_coverage_ref = expected_refs[scenario]
        if fields.get("benchmark_row_ref") != expected_benchmark_ref:
            raise RuntimeError(
                "ShardLoom direct transient benchmark row ref was unexpected: "
                + str(fields.get("benchmark_row_ref", "missing"))
            )
        if fields.get("coverage_row_ref") != expected_coverage_ref:
            raise RuntimeError(
                "ShardLoom direct transient coverage row ref was unexpected: "
                + str(fields.get("coverage_row_ref", "missing"))
            )
        result_json = fields.get("result_json")
        if result_json is None:
            raise RuntimeError("ShardLoom result_json field was missing")
        return {
            "__benchmark_result": json.loads(result_json),
            "__shardloom_evidence": fields,
        }

    return EngineRunner(
        "shardloom-direct-transient",
        shardloom_version(root, SHARDLOOM_BUILD_PROFILE),
        {
            "selective filter": lambda paths, data_format: run_scenario(
                "selective filter", paths, data_format
            ),
            "filter + projection + limit": lambda paths, data_format: run_scenario(
                "filter + projection + limit", paths, data_format
            ),
        },
        formats=("csv",),
        build_time_millis=SHARDLOOM_BUILD_TIMINGS.get(str(binary)),
    )


def shardloom_vortex_runner(engine_name: str = "shardloom-vortex") -> EngineRunner:
    root = workspace_root()
    binary = build_shardloom_cli(
        root,
        "vortex-traditional-analytics-benchmark",
        SHARDLOOM_BUILD_PROFILE,
    )
    env = os.environ.copy()
    env["RUSTUP_TOOLCHAIN"] = env.get("RUSTUP_TOOLCHAIN", "1.91.1")
    prepared_paths: dict[str, dict[str, Path | float | str]] = {}

    def prepare_format(paths: DatasetPaths, data_format: str) -> None:
        if data_format in prepared_paths:
            return
        workspace = paths.root / "shardloom_native_vortex_inputs" / data_format
        command = [
            str(binary),
            "traditional-analytics-run",
            "csv/file ingest",
            str(fact_path(paths, data_format)),
            str(dim_path(paths, data_format)),
            "--workspace",
            str(workspace),
            "--input-format",
            data_format,
            "--execution-mode",
            "compatibility_import_certified",
            "--format",
            "json",
        ]
        started = time.perf_counter()
        completed = subprocess_run(command, root, env)
        preparation_millis = (time.perf_counter() - started) * 1000.0
        if completed["returncode"] != 0:
            raise BenchmarkUnsupported(
                completed["stderr"] or completed["stdout"] or "native Vortex input setup failed"
            )
        try:
            payload = json.loads(completed["stdout"].splitlines()[0])
        except (json.JSONDecodeError, IndexError) as exc:
            raise BenchmarkUnsupported(
                f"ShardLoom native Vortex setup emitted invalid JSON: {exc}"
            ) from exc
        fields = parse_output_fields(payload)
        fact_vortex = Path(fields.get("fact_vortex_path", ""))
        dim_vortex = Path(fields.get("dim_vortex_path", ""))
        if not fact_vortex.exists() or not dim_vortex.exists():
            raise BenchmarkUnsupported(
                "ShardLoom native Vortex setup did not produce fact/dim .vortex files"
            )
        prepared_paths[data_format] = {
            "fact": fact_vortex,
            "dim": dim_vortex,
            "preparation_millis": round(preparation_millis, 4),
            "preparation_cli_process_wall_millis": completed.get(
                "process_wall_millis", preparation_millis
            ),
            "fact_digest": fields.get("fact_vortex_digest", ""),
            "dim_digest": fields.get("dim_vortex_digest", ""),
            "source_native_io_certificate_status": fields.get(
                "native_io_certificate_status", ""
            ),
            "source_native_io_certificate_id": fields.get(
                "native_io_certificate_id", ""
            ),
        }

    def prepare_cdc_delta_format(paths: DatasetPaths, data_format: str) -> None:
        prepare_format(paths, data_format)
        prepared = prepared_paths[data_format]
        if "cdc_delta" in prepared:
            return
        if paths.cdc_delta_csv is None or not paths.cdc_delta_csv.exists():
            raise BenchmarkUnsupported("CDC overlay scenario requires cdc_delta.csv")
        workspace = (
            paths.root
            / "shardloom_native_vortex_inputs"
            / data_format
            / "cdc_delta_overlay"
        )
        command = [
            str(binary),
            "traditional-analytics-run",
            "small change over large base",
            str(fact_path(paths, data_format)),
            str(dim_path(paths, data_format)),
            "--workspace",
            str(workspace),
            "--input-format",
            data_format,
            "--cdc-delta",
            str(paths.cdc_delta_csv),
            "--execution-mode",
            "compatibility_import_certified",
            "--format",
            "json",
        ]
        started = time.perf_counter()
        completed = subprocess_run(command, root, env)
        cdc_preparation_millis = (time.perf_counter() - started) * 1000.0
        if completed["returncode"] != 0:
            raise BenchmarkUnsupported(
                completed["stderr"] or completed["stdout"] or "CDC Vortex input setup failed"
            )
        try:
            payload = json.loads(completed["stdout"].splitlines()[0])
        except (json.JSONDecodeError, IndexError) as exc:
            raise BenchmarkUnsupported(
                f"ShardLoom CDC Vortex setup emitted invalid JSON: {exc}"
            ) from exc
        fields = parse_output_fields(payload)
        cdc_delta_vortex = Path(fields.get("cdc_delta_vortex_path", ""))
        if not cdc_delta_vortex.exists():
            raise BenchmarkUnsupported(
                "ShardLoom CDC Vortex setup did not produce cdc_delta.vortex"
            )
        prepared.update(
            {
                "cdc_delta": cdc_delta_vortex,
                "cdc_delta_digest": fields.get("cdc_delta_vortex_digest", ""),
                "cdc_delta_preparation_millis": round(cdc_preparation_millis, 4),
                "cdc_delta_preparation_cli_process_wall_millis": completed.get(
                    "process_wall_millis", cdc_preparation_millis
                ),
            }
        )

    def prepare(paths: DatasetPaths, report_formats: tuple[str, ...]) -> None:
        for data_format in report_formats:
            if data_format == SHARDLOOM_VORTEX_FORMAT:
                continue
            prepare_format(paths, data_format)

    def run_scenario(scenario: str, paths: DatasetPaths, data_format: str) -> Any:
        if data_format == SHARDLOOM_VORTEX_FORMAT:
            raise BenchmarkUnsupported(
                "shardloom-vortex reports prepared/native results under the requested source format rows"
            )
        if scenario == "small change over large base":
            prepare_cdc_delta_format(paths, data_format)
        else:
            prepare_format(paths, data_format)
        prepared = prepared_paths[data_format]
        command = [
            str(binary),
            "traditional-analytics-vortex-run",
            scenario,
            str(prepared["fact"]),
            str(prepared["dim"]),
            "--execution-mode",
            "prepared_vortex",
        ]
        if scenario == "small change over large base":
            command.extend(["--cdc-delta-vortex", str(prepared["cdc_delta"])])
        command.extend(["--format", "json"])
        if SHARDLOOM_RESULT_SINK:
            result_workspace = (
                paths.root
                / "shardloom_prepared_vortex_result_sinks"
                / data_format
                / scenario_slug(scenario)
            )
            command.extend(
                ["--workspace", str(result_workspace), "--write-result-vortex"]
            )
        completed = subprocess_run(command, root, env)
        try:
            payload = json.loads(completed["stdout"].splitlines()[0])
        except (json.JSONDecodeError, IndexError) as exc:
            if completed["returncode"] != 0:
                raise RuntimeError(
                    completed["stderr"] or completed["stdout"] or "unknown failure"
                ) from exc
            raise RuntimeError(f"ShardLoom emitted invalid JSON: {exc}") from exc
        if completed["returncode"] != 0:
            raise RuntimeError(completed["stderr"] or completed["stdout"] or "unknown failure")
        fields = parse_output_fields(payload)
        fields["cli_process_wall_millis"] = str(
            completed.get("process_wall_millis", "not_measured")
        )
        fields["process_startup_attribution"] = "per_scenario_cli_process_wall_measured"
        fields["python_harness_overhead_status"] = (
            "outer_harness_wall_minus_cli_process_wall"
        )
        fields["build_time_excluded"] = "true"
        if payload.get("status") != "success":
            reason = fields.get("reason") or payload.get("human_text") or "unsupported"
            raise BenchmarkUnsupported(str(reason))
        required_true_fields = [
            "native_work_envelope_created",
            "native_work_stream_created",
            "native_result_stream_created",
            "native_io_certificate_emitted",
            "vortex_source_adapter_used",
            "vortex_file_read",
            "upstream_vortex_scan_called",
            "materialization_boundary_report_emitted",
            "native_io_per_path_certificate_emitted",
            "native_io_materializing_transitions_have_boundaries",
        ]
        missing_evidence = [
            field for field in required_true_fields if fields.get(field) != "true"
        ]
        if missing_evidence:
            raise RuntimeError(
                "ShardLoom native Vortex evidence was missing: "
                + ", ".join(missing_evidence)
            )
        if fields.get("native_io_certificate_status") != "certified":
            raise RuntimeError(
                "ShardLoom NativeIoCertificate was not certified: "
                + str(fields.get("native_io_certificate_status", "missing"))
            )
        if (
            fields.get("native_io_certificate_path_id")
            != "native_vortex_source_to_native_runtime_result"
        ):
            raise RuntimeError(
                "ShardLoom NativeIoCertificate path was unexpected: "
                + str(fields.get("native_io_certificate_path_id", "missing"))
            )
        if SHARDLOOM_RESULT_SINK:
            for field in (
                "computed_result_sink_requested",
                "computed_result_sink_written",
                "computed_result_sink_replay_verified",
            ):
                if fields.get(field) != "true":
                    raise RuntimeError(
                        "ShardLoom prepared/native result sink evidence was missing: "
                        + field
                    )
            if (
                fields.get("computed_result_sink_native_io_certificate_status")
                != "certified"
            ):
                raise RuntimeError(
                    "ShardLoom prepared/native result sink NativeIoCertificate was not certified: "
                    + str(
                        fields.get(
                            "computed_result_sink_native_io_certificate_status",
                            "missing",
                        )
                    )
                )
            if fields.get("result_sink_claim_gate_status") != "result_sink_replay_certified":
                raise RuntimeError(
                    "ShardLoom prepared/native result sink claim gate was not certified: "
                    + str(fields.get("result_sink_claim_gate_status", "missing"))
                )
        result_json = fields.get("result_json")
        if result_json is None:
            raise RuntimeError("ShardLoom result_json field was missing")
        prepared_refs = [str(prepared["fact"]), str(prepared["dim"])]
        prepared_digests = [str(prepared["fact_digest"]), str(prepared["dim_digest"])]
        if "cdc_delta" in prepared:
            prepared_refs.append(str(prepared["cdc_delta"]))
            prepared_digests.append(str(prepared["cdc_delta_digest"]))
        fields.update(
            {
                "requested_execution_mode": "prepared_vortex",
                "selected_execution_mode": "prepared_vortex",
                "execution_mode": "prepared_vortex",
                "mode_selection_reason": (
                    "compatibility input was prepared into Vortex once before scenario timing"
                ),
                "execution_mode_family": "native_vortex",
                "preparation_millis": str(prepared["preparation_millis"]),
                "preparation_cli_process_wall_millis": str(
                    prepared["preparation_cli_process_wall_millis"]
                ),
                "cdc_delta_preparation_millis": str(
                    prepared.get("cdc_delta_preparation_millis", "0")
                ),
                "cdc_delta_preparation_cli_process_wall_millis": str(
                    prepared.get("cdc_delta_preparation_cli_process_wall_millis", "0")
                ),
                "preparation_included_in_timing": "false",
                "prepared_artifact_ref": "|".join(prepared_refs),
                "prepared_artifact_digest": "|".join(prepared_digests),
                "source_native_io_certificate_status": str(
                    prepared["source_native_io_certificate_status"]
                ),
                "source_native_io_certificate_id": str(
                    prepared["source_native_io_certificate_id"]
                ),
                "compatibility_import_included": "false",
                "compatibility_to_vortex_included": "false",
                "vortex_prepare_included": "false",
                "vortex_write_reopen_included": "false",
                "direct_transient_execution": "false",
                "persistent_runner_status": PERSISTENT_RUNNER_STATUS,
            }
        )
        return {
            "__benchmark_result": json.loads(result_json),
            "__shardloom_evidence": fields,
        }

    def prepared_refs_for_batch(prepared: dict[str, Path | float | str]) -> tuple[str, str]:
        prepared_refs = [str(prepared["fact"]), str(prepared["dim"])]
        prepared_digests = [str(prepared["fact_digest"]), str(prepared["dim_digest"])]
        if "cdc_delta" in prepared:
            prepared_refs.append(str(prepared["cdc_delta"]))
            prepared_digests.append(str(prepared["cdc_delta_digest"]))
        return "|".join(prepared_refs), "|".join(prepared_digests)

    def extract_batch_scenario_output(
        scenario: str,
        batch_fields: dict[str, str],
        prepared: dict[str, Path | float | str],
        completed: dict[str, Any],
    ) -> dict[str, Any]:
        prefix = f"scenario_{vortex_batch_scenario_slug(scenario)}_"
        fields = {
            key[len(prefix) :]: value
            for key, value in batch_fields.items()
            if key.startswith(prefix)
        }
        if not fields:
            raise BenchmarkUnsupported(
                f"ShardLoom batch runner did not emit evidence for scenario: {scenario}"
            )
        required_true_fields = [
            "native_work_envelope_created",
            "native_work_stream_created",
            "native_result_stream_created",
            "native_io_certificate_emitted",
            "vortex_source_adapter_used",
            "vortex_file_read",
            "upstream_vortex_scan_called",
            "materialization_boundary_report_emitted",
            "native_io_per_path_certificate_emitted",
            "native_io_materializing_transitions_have_boundaries",
        ]
        missing_evidence = [
            field for field in required_true_fields if fields.get(field) != "true"
        ]
        if missing_evidence:
            raise RuntimeError(
                "ShardLoom batch native Vortex evidence was missing for "
                + scenario
                + ": "
                + ", ".join(missing_evidence)
            )
        if fields.get("native_io_certificate_status") != "certified":
            raise RuntimeError(
                "ShardLoom batch NativeIoCertificate was not certified for "
                + scenario
                + ": "
                + str(fields.get("native_io_certificate_status", "missing"))
            )
        if (
            fields.get("native_io_certificate_path_id")
            != "native_vortex_source_to_native_runtime_result"
        ):
            raise RuntimeError(
                "ShardLoom batch NativeIoCertificate path was unexpected for "
                + scenario
                + ": "
                + str(fields.get("native_io_certificate_path_id", "missing"))
            )
        if SHARDLOOM_RESULT_SINK:
            for field in (
                "computed_result_sink_requested",
                "computed_result_sink_written",
                "computed_result_sink_replay_verified",
            ):
                if fields.get(field) != "true":
                    raise RuntimeError(
                        "ShardLoom batch result sink evidence was missing for "
                        + scenario
                        + ": "
                        + field
                    )
            if (
                fields.get("computed_result_sink_native_io_certificate_status")
                != "certified"
            ):
                raise RuntimeError(
                    "ShardLoom batch result sink NativeIoCertificate was not certified for "
                    + scenario
                    + ": "
                    + str(
                        fields.get(
                            "computed_result_sink_native_io_certificate_status",
                            "missing",
                        )
                    )
                )
        result_json = fields.get("result_json")
        if result_json is None:
            raise RuntimeError(
                "ShardLoom batch result_json field was missing for " + scenario
            )
        prepared_ref, prepared_digest = prepared_refs_for_batch(prepared)
        batch_wall = completed.get("process_wall_millis", "not_measured")
        fields.update(
            {
                "requested_execution_mode": "prepared_vortex",
                "selected_execution_mode": fields.get(
                    "selected_execution_mode", "prepared_vortex"
                ),
                "execution_mode": fields.get("selected_execution_mode", "prepared_vortex"),
                "mode_selection_reason": fields.get(
                    "mode_selection_reason",
                    "compatibility input was prepared into Vortex once before scenario timing",
                ),
                "execution_mode_family": "native_vortex",
                "preparation_millis": str(prepared["preparation_millis"]),
                "preparation_cli_process_wall_millis": str(
                    prepared["preparation_cli_process_wall_millis"]
                ),
                "cdc_delta_preparation_millis": str(
                    prepared.get("cdc_delta_preparation_millis", "0")
                ),
                "cdc_delta_preparation_cli_process_wall_millis": str(
                    prepared.get("cdc_delta_preparation_cli_process_wall_millis", "0")
                ),
                "preparation_included_in_timing": "false",
                "prepared_artifact_ref": prepared_ref,
                "prepared_artifact_digest": prepared_digest,
                "source_native_io_certificate_status": str(
                    prepared["source_native_io_certificate_status"]
                ),
                "source_native_io_certificate_id": str(
                    prepared["source_native_io_certificate_id"]
                ),
                "compatibility_import_included": "false",
                "compatibility_to_vortex_included": "false",
                "vortex_prepare_included": "false",
                "vortex_write_reopen_included": "false",
                "direct_transient_execution": "false",
                "persistent_runner_status": BATCH_RUNNER_STATUS,
                "process_startup_attribution": BATCH_PROCESS_STARTUP_ATTRIBUTION,
                "python_harness_overhead_status": BATCH_HARNESS_OVERHEAD_STATUS,
                "cli_process_wall_millis": str(batch_wall),
                "batch_cli_process_wall_millis": str(batch_wall),
                "batch_process_wall_shared": "true",
                "batch_process_startup_attribution": BATCH_PROCESS_STARTUP_ATTRIBUTION,
                "batch_runner_kind": batch_fields.get(
                    "runner_kind", "single_process_prepared_native_batch"
                ),
                "source_metadata_snapshot_status": batch_fields.get(
                    "source_metadata_snapshot_status", "unknown"
                ),
                "source_metadata_snapshot_reused": batch_fields.get(
                    "source_metadata_snapshot_reused", "unknown"
                ),
                "source_metadata_snapshot_reuse_count": batch_fields.get(
                    "source_metadata_snapshot_reuse_count", "unknown"
                ),
                "source_metadata_digest_recompute_avoided_count": batch_fields.get(
                    "source_metadata_digest_recompute_avoided_count", "unknown"
                ),
                "source_metadata_snapshot_claim_boundary": batch_fields.get(
                    "source_metadata_snapshot_claim_boundary", "unknown"
                ),
                "source_state_reuse_status": batch_fields.get(
                    "source_state_reuse_status", "unknown"
                ),
                "source_state_coverage_schema_version": batch_fields.get(
                    "source_state_coverage_schema_version", "unknown"
                ),
                "source_state_coverage_matrix_ref": batch_fields.get(
                    "source_state_coverage_matrix_ref", "unknown"
                ),
                "source_state_coverage_status_vocabulary": batch_fields.get(
                    "source_state_coverage_status_vocabulary", "unknown"
                ),
                "source_state_coverage_all_requested_scenarios_classified": batch_fields.get(
                    "source_state_coverage_all_requested_scenarios_classified", "unknown"
                ),
                "source_state_coverage_matrix": batch_fields.get(
                    "source_state_coverage_matrix", "unknown"
                ),
                "source_state_coverage_reused_scenario_count": batch_fields.get(
                    "source_state_coverage_reused_scenario_count", "unknown"
                ),
                "source_state_coverage_not_needed_scenario_count": batch_fields.get(
                    "source_state_coverage_not_needed_scenario_count", "unknown"
                ),
                "source_state_coverage_blocked_scenario_count": batch_fields.get(
                    "source_state_coverage_blocked_scenario_count", "unknown"
                ),
                "source_state_coverage_unsupported_scenario_count": batch_fields.get(
                    "source_state_coverage_unsupported_scenario_count", "unknown"
                ),
                "source_state_digest_status": batch_fields.get(
                    "source_state_digest_status", "unknown"
                ),
                "source_state_digest_reason": batch_fields.get(
                    "source_state_digest_reason", "unknown"
                ),
                "source_state_reused": batch_fields.get(
                    "source_state_reused", "unknown"
                ),
                "source_state_reuse_consumer_count": batch_fields.get(
                    "source_state_reuse_consumer_count", "unknown"
                ),
                "source_state_recompute_avoided_count": batch_fields.get(
                    "source_state_recompute_avoided_count", "unknown"
                ),
                "source_state_family_count": batch_fields.get(
                    "source_state_family_count", "unknown"
                ),
                "source_state_dimension_label_reuse_status": batch_fields.get(
                    "source_state_dimension_label_reuse_status", "unknown"
                ),
                "source_state_dimension_label_reused": batch_fields.get(
                    "source_state_dimension_label_reused", "unknown"
                ),
                "source_state_dimension_label_reuse_consumer_count": batch_fields.get(
                    "source_state_dimension_label_reuse_consumer_count", "unknown"
                ),
                "source_state_dimension_label_recompute_avoided_count": batch_fields.get(
                    "source_state_dimension_label_recompute_avoided_count", "unknown"
                ),
                "source_state_category_metric_reuse_status": batch_fields.get(
                    "source_state_category_metric_reuse_status", "unknown"
                ),
                "source_state_category_metric_reused": batch_fields.get(
                    "source_state_category_metric_reused", "unknown"
                ),
                "source_state_category_metric_reuse_consumer_count": batch_fields.get(
                    "source_state_category_metric_reuse_consumer_count", "unknown"
                ),
                "source_state_category_metric_recompute_avoided_count": batch_fields.get(
                    "source_state_category_metric_recompute_avoided_count", "unknown"
                ),
                "source_state_group_category_metric_reuse_status": batch_fields.get(
                    "source_state_group_category_metric_reuse_status", "unknown"
                ),
                "source_state_group_category_metric_reused": batch_fields.get(
                    "source_state_group_category_metric_reused", "unknown"
                ),
                "source_state_group_category_metric_reuse_consumer_count": batch_fields.get(
                    "source_state_group_category_metric_reuse_consumer_count", "unknown"
                ),
                "source_state_group_category_metric_recompute_avoided_count": batch_fields.get(
                    "source_state_group_category_metric_recompute_avoided_count", "unknown"
                ),
                "source_state_ranked_metric_reuse_status": batch_fields.get(
                    "source_state_ranked_metric_reuse_status", "unknown"
                ),
                "source_state_ranked_metric_reused": batch_fields.get(
                    "source_state_ranked_metric_reused", "unknown"
                ),
                "source_state_ranked_metric_reuse_consumer_count": batch_fields.get(
                    "source_state_ranked_metric_reuse_consumer_count", "unknown"
                ),
                "source_state_ranked_metric_recompute_avoided_count": batch_fields.get(
                    "source_state_ranked_metric_recompute_avoided_count", "unknown"
                ),
                "source_state_selective_filter_reuse_status": batch_fields.get(
                    "source_state_selective_filter_reuse_status", "unknown"
                ),
                "source_state_selective_filter_reused": batch_fields.get(
                    "source_state_selective_filter_reused", "unknown"
                ),
                "source_state_selective_filter_reuse_consumer_count": batch_fields.get(
                    "source_state_selective_filter_reuse_consumer_count", "unknown"
                ),
                "source_state_selective_filter_recompute_avoided_count": batch_fields.get(
                    "source_state_selective_filter_recompute_avoided_count", "unknown"
                ),
                "source_state_dirty_input_reuse_status": batch_fields.get(
                    "source_state_dirty_input_reuse_status", "unknown"
                ),
                "source_state_dirty_input_reused": batch_fields.get(
                    "source_state_dirty_input_reused", "unknown"
                ),
                "source_state_dirty_input_reuse_consumer_count": batch_fields.get(
                    "source_state_dirty_input_reuse_consumer_count", "unknown"
                ),
                "source_state_dirty_input_recompute_avoided_count": batch_fields.get(
                    "source_state_dirty_input_recompute_avoided_count", "unknown"
                ),
                "source_state_date_null_metric_reuse_status": batch_fields.get(
                    "source_state_date_null_metric_reuse_status", "unknown"
                ),
                "source_state_date_null_metric_reused": batch_fields.get(
                    "source_state_date_null_metric_reused", "unknown"
                ),
                "source_state_date_null_metric_reuse_consumer_count": batch_fields.get(
                    "source_state_date_null_metric_reuse_consumer_count", "unknown"
                ),
                "source_state_date_null_metric_recompute_avoided_count": batch_fields.get(
                    "source_state_date_null_metric_recompute_avoided_count", "unknown"
                ),
                "source_state_prepare_micros": batch_fields.get(
                    "source_state_prepare_micros", "unknown"
                ),
                "source_state_prepare_timing_scope": batch_fields.get(
                    "source_state_prepare_timing_scope", "unknown"
                ),
                "source_state_claim_boundary": batch_fields.get(
                    "source_state_claim_boundary", "unknown"
                ),
                "runtime_evidence_level_schema_version": batch_fields.get(
                    "runtime_evidence_level_schema_version", "unknown"
                ),
                "requested_evidence_level": batch_fields.get(
                    "requested_evidence_level", "unknown"
                ),
                "selected_evidence_level": batch_fields.get(
                    "selected_evidence_level", "unknown"
                ),
                "evidence_level": batch_fields.get("evidence_level", "unknown"),
                "evidence_level_supported_levels": batch_fields.get(
                    "evidence_level_supported_levels", "unknown"
                ),
                "evidence_level_claim_gate_status": batch_fields.get(
                    "evidence_level_claim_gate_status", "unknown"
                ),
                "evidence_level_result_sink_replay_required": batch_fields.get(
                    "evidence_level_result_sink_replay_required", "unknown"
                ),
                "evidence_level_result_sink_replay_requested": batch_fields.get(
                    "evidence_level_result_sink_replay_requested", "unknown"
                ),
                "evidence_level_result_sink_replay_verified": batch_fields.get(
                    "evidence_level_result_sink_replay_verified", "unknown"
                ),
                "evidence_level_native_io_certificate_required": batch_fields.get(
                    "evidence_level_native_io_certificate_required", "unknown"
                ),
                "evidence_level_certificate_refs": batch_fields.get(
                    "evidence_level_certificate_refs", "unknown"
                ),
                "evidence_level_result_sink_replay_refs": batch_fields.get(
                    "evidence_level_result_sink_replay_refs", "unknown"
                ),
                "evidence_level_source_state_digest": batch_fields.get(
                    "evidence_level_source_state_digest", "unknown"
                ),
                "evidence_level_output_digest": batch_fields.get(
                    "evidence_level_output_digest", "unknown"
                ),
                "evidence_level_fallback_attempted": batch_fields.get(
                    "evidence_level_fallback_attempted", "unknown"
                ),
                "evidence_level_external_engine_invoked": batch_fields.get(
                    "evidence_level_external_engine_invoked", "unknown"
                ),
                "evidence_level_claim_boundary": batch_fields.get(
                    "evidence_level_claim_boundary", "unknown"
                ),
                "batch_scenario_count": batch_fields.get("scenario_count", "unknown"),
                "batch_scenario_order": batch_fields.get("scenario_order", ""),
                "batch_total_scenario_compute_micros": batch_fields.get(
                    "total_scenario_compute_micros", "unknown"
                ),
                "batch_total_vortex_scan_micros": batch_fields.get(
                    "total_vortex_scan_micros", "unknown"
                ),
                "batch_total_result_sink_write_micros": batch_fields.get(
                    "total_result_sink_write_micros", "none"
                ),
            }
        )
        for field in SESSION_RUNTIME_FIELDS:
            fields.setdefault(field, batch_fields.get(field, "unknown"))
        for field in ALLOCATION_RESOURCE_PROFILE_FIELDS:
            fields.setdefault(field, batch_fields.get(field, "unknown"))
        for field in RUNTIME_EVIDENCE_LEVEL_FIELDS:
            fields.setdefault(field, batch_fields.get(field, "unknown"))
        return {
            "__benchmark_result": json.loads(result_json),
            "__shardloom_evidence": fields,
        }

    def run_batch_scenarios(
        paths: DatasetPaths, data_format: str, scenarios: tuple[str, ...]
    ) -> dict[str, Any]:
        if data_format == SHARDLOOM_VORTEX_FORMAT:
            raise BenchmarkUnsupported(
                "shardloom-vortex reports prepared/native results under the requested source format rows"
            )
        if any(scenario == "small change over large base" for scenario in scenarios):
            prepare_cdc_delta_format(paths, data_format)
        else:
            prepare_format(paths, data_format)
        prepared = prepared_paths[data_format]
        scenario_csv = ",".join(scenarios)
        command = [
            str(binary),
            "traditional-analytics-vortex-batch-run",
            scenario_csv,
            str(prepared["fact"]),
            str(prepared["dim"]),
            "--execution-mode",
            "prepared_vortex",
            "--evidence-level",
            SHARDLOOM_EVIDENCE_LEVEL,
            "--format",
            "json",
        ]
        if "cdc_delta" in prepared:
            command.extend(["--cdc-delta-vortex", str(prepared["cdc_delta"])])
        if SHARDLOOM_RESULT_SINK:
            result_workspace = (
                paths.root
                / "shardloom_prepared_vortex_batch_result_sinks"
                / data_format
                / hashlib.sha256(scenario_csv.encode("utf-8")).hexdigest()[:12]
            )
            command.extend(
                ["--workspace", str(result_workspace), "--write-result-vortex"]
            )
        completed = subprocess_run(command, root, env)
        try:
            payload = json.loads(completed["stdout"].splitlines()[0])
        except (json.JSONDecodeError, IndexError) as exc:
            if completed["returncode"] != 0:
                raise RuntimeError(
                    completed["stderr"] or completed["stdout"] or "unknown failure"
                ) from exc
            raise RuntimeError(f"ShardLoom batch emitted invalid JSON: {exc}") from exc
        if completed["returncode"] != 0:
            raise RuntimeError(completed["stderr"] or completed["stdout"] or "unknown failure")
        fields = parse_output_fields(payload)
        if payload.get("status") != "success":
            reason = fields.get("reason") or payload.get("human_text") or "unsupported"
            raise BenchmarkUnsupported(str(reason))
        if fields.get("schema_version") != "shardloom.traditional_analytics.vortex_batch.v1":
            raise RuntimeError(
                "ShardLoom batch schema was unexpected: "
                + str(fields.get("schema_version", "missing"))
            )
        if fields.get("persistent_runner_status") != BATCH_RUNNER_STATUS:
            raise RuntimeError(
                "ShardLoom batch runner status was unexpected: "
                + str(fields.get("persistent_runner_status", "missing"))
            )
        if fields.get("source_metadata_snapshot_status") != "per_batch_source_metadata_reused":
            raise RuntimeError(
                "ShardLoom batch source metadata snapshot status was unexpected: "
                + str(fields.get("source_metadata_snapshot_status", "missing"))
            )
        if fields.get("source_metadata_snapshot_reused") != "true":
            raise RuntimeError("ShardLoom batch did not report source metadata reuse")
        if fields.get("source_state_prepare_timing_scope") != "batch_shared_pre_scenario":
            raise RuntimeError(
                "ShardLoom batch source-state timing scope was unexpected: "
                + str(fields.get("source_state_prepare_timing_scope", "missing"))
            )
        if (
            fields.get("source_state_coverage_schema_version")
            != "shardloom.traditional_analytics.source_state_coverage.v1"
        ):
            raise RuntimeError(
                "ShardLoom batch source-state coverage schema was unexpected: "
                + str(fields.get("source_state_coverage_schema_version", "missing"))
            )
        if fields.get("source_state_coverage_all_requested_scenarios_classified") != "true":
            raise RuntimeError("ShardLoom batch did not classify all requested source-state rows")
        if fields.get("source_state_digest_status") != "not_emitted_scoped_in_memory_source_state":
            raise RuntimeError(
                "ShardLoom batch source-state digest boundary was unexpected: "
                + str(fields.get("source_state_digest_status", "missing"))
            )
        if fields.get("source_state_fallback_attempted") != "false":
            raise RuntimeError("ShardLoom batch source-state reported fallback attempts")
        if fields.get("source_state_external_engine_invoked") != "false":
            raise RuntimeError("ShardLoom batch source-state reported external engine invocation")
        if fields.get("all_fallback_attempted_false") != "true":
            raise RuntimeError("ShardLoom batch reported fallback attempts")
        if fields.get("all_external_engine_invoked_false") != "true":
            raise RuntimeError("ShardLoom batch reported external engine invocation")
        if fields.get("all_native_io_certificates_certified") != "true":
            raise RuntimeError("ShardLoom batch Native I/O certificates were not certified")
        return {
            scenario: extract_batch_scenario_output(
                scenario, fields, prepared, completed
            )
            for scenario in scenarios
        }

    return EngineRunner(
        engine_name,
        shardloom_version(root, SHARDLOOM_BUILD_PROFILE),
        {
            scenario: (
                lambda paths, data_format, scenario=scenario: run_scenario(
                    scenario, paths, data_format
                )
            )
            for scenario in SHARDLOOM_EXECUTABLE_SCENARIOS
        },
        formats=FORMAT_ORDER,
        batch_scenarios=run_batch_scenarios,
        prepare=prepare,
        build_time_millis=SHARDLOOM_BUILD_TIMINGS.get(str(binary)),
    )


def shardloom_prepared_vortex_runner() -> EngineRunner:
    return shardloom_vortex_runner("shardloom-prepared-vortex")


def available_runners(engine_names: tuple[str, ...]) -> tuple[dict[str, EngineRunner], dict[str, str]]:
    runners: dict[str, EngineRunner] = {}
    missing: dict[str, str] = {}
    for engine in engine_names:
        try:
            started = time.perf_counter()
            runner = ENGINE_FACTORIES[engine]()
            startup_time = (time.perf_counter() - started) * 1000.0
            build_time = runner.build_time_millis or 0.0
            startup_without_build = max(0.0, startup_time - build_time)
            runners[engine] = replace(
                runner, startup_time_millis=round(startup_without_build, 4)
            )
        except Exception as exc:
            missing[engine] = f"{type(exc).__name__}: {exc}"
    return runners, missing


def warmup_runner(runner: EngineRunner) -> EngineRunner:
    if runner.warmup is None:
        return runner
    started = time.perf_counter()
    runner.warmup()
    warmup_time = (time.perf_counter() - started) * 1000.0
    startup_time = (runner.startup_time_millis or 0.0) + warmup_time
    return replace(runner, startup_time_millis=round(startup_time, 4))


def prepare_runner(
    runner: EngineRunner, paths: DatasetPaths, report_formats: tuple[str, ...]
) -> EngineRunner:
    if runner.prepare is None:
        return runner
    started = time.perf_counter()
    runner.prepare(paths, report_formats)
    prepare_time = (time.perf_counter() - started) * 1000.0
    return replace(
        runner,
        startup_time_millis=runner.startup_time_millis,
        preparation_time_millis=round(prepare_time, 4),
    )


def round_float(value: Any) -> float:
    if value is None:
        return 0.0
    number = float(value)
    if math.isnan(number):
        return 0.0
    return round(number, CORRECTNESS_FLOAT_DIGITS)


def normalize_scalar_result(row_count: Any, metric_sum: Any) -> dict[str, Any]:
    return {"row_count": int(row_count), "metric_sum": round_float(metric_sum)}


def parse_output_fields(payload: dict[str, Any]) -> dict[str, str]:
    return {
        str(field.get("key")): str(field.get("value"))
        for field in payload.get("fields", [])
        if isinstance(field, dict) and "key" in field
    }


def parse_optional_int(value: Any) -> int | None:
    if value is None or value == "none" or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_optional_float(value: Any) -> float | None:
    if value is None or value == "none" or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_optional_bool(value: Any) -> bool | None:
    if value is None or value == "none" or value == "":
        return None
    text = str(value).strip().lower()
    if text == "true":
        return True
    if text == "false":
        return False
    return None


def first_meaningful_field(*values: Any) -> Any:
    for value in values:
        if value is not None and value != "" and value != "none":
            return value
    return None


def unwrap_engine_value(value: Any) -> tuple[Any, dict[str, str]]:
    if (
        isinstance(value, dict)
        and "__benchmark_result" in value
        and isinstance(value.get("__shardloom_evidence"), dict)
    ):
        return value["__benchmark_result"], {
            str(key): str(field_value)
            for key, field_value in value["__shardloom_evidence"].items()
        }
    return value, {}


def workspace_root() -> Path:
    return Path(__file__).resolve().parents[2]


def shardloom_binary_path(root: Path, profile: str) -> Path:
    binary_name = "shardloom.exe" if os.name == "nt" else "shardloom"
    target_profile = profile if profile in SHARDLOOM_BUILD_PROFILES else "debug"
    target_root = Path(os.environ.get("CARGO_TARGET_DIR", root / "target"))
    if not target_root.is_absolute():
        target_root = root / target_root
    return target_root / target_profile / binary_name


def append_rustflag(existing: str | None, flag: str) -> str:
    if existing and flag in existing.split():
        return existing
    return f"{existing} {flag}".strip() if existing else flag


def build_shardloom_cli(root: Path, features: str, profile: str) -> Path:
    cargo = shutil.which("cargo")
    if cargo is None:
        raise BenchmarkUnsupported("cargo was not found on PATH, so ShardLoom could not be built")
    build_command = [
        cargo,
        "build",
        "-q",
        "-p",
        "shardloom-cli",
        "--features",
        features,
    ]
    if profile == "release":
        build_command.append("--release")
    elif profile != "debug":
        build_command.extend(["--profile", profile])
    env = os.environ.copy()
    env["RUSTUP_TOOLCHAIN"] = env.get("RUSTUP_TOOLCHAIN", "1.91.1")
    if profile == "release-native-benchmark":
        env["RUSTFLAGS"] = append_rustflag(env.get("RUSTFLAGS"), "-Ctarget-cpu=native")
    if profile == "release-pgo" and env.get("SHARDLOOM_PGO_PROFILE"):
        env["RUSTFLAGS"] = append_rustflag(
            env.get("RUSTFLAGS"), f"-Cprofile-use={env['SHARDLOOM_PGO_PROFILE']}"
        )
    completed = subprocess_run(build_command, root, env)
    if completed["returncode"] != 0:
        raise BenchmarkUnsupported(
            "ShardLoom CLI build failed before benchmark timing began: "
            + (completed["stderr"] or completed["stdout"] or "unknown failure")
        )
    binary = shardloom_binary_path(root, profile)
    if not binary.exists():
        raise BenchmarkUnsupported(f"ShardLoom binary was not found after build: {binary}")
    SHARDLOOM_BUILD_TIMINGS[str(binary)] = round(
        float(completed.get("process_wall_millis") or 0.0), 4
    )
    return binary


def shardloom_version(root: Path, profile: str) -> str:
    git = shutil.which("git")
    if git is None:
        return f"workspace-local-{profile}"
    completed = subprocess_run([git, "rev-parse", "--short", "HEAD"], root, os.environ.copy())
    if completed["returncode"] != 0:
        return f"workspace-local-{profile}"
    version = f"workspace-local-{profile}-{completed['stdout'].strip()}"
    dirty = subprocess_run(
        [git, "status", "--short", "--untracked-files=no"], root, os.environ.copy()
    )
    if dirty["returncode"] == 0 and dirty["stdout"].strip():
        version += "-dirty"
    return version


def scenario_slug(scenario: str) -> str:
    return (
        scenario.lower()
        .replace("/", "-")
        .replace(" ", "-")
        .replace("_", "-")
    )


def vortex_batch_scenario_slug(scenario: str) -> str:
    return scenario.replace("/", "-").replace(" ", "-").replace("+", "-")


def normalize_group_rows(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    normalized = []
    for row in rows:
        normalized.append(
            {
                key: str(row[key]) if key == "dim_label" else int(row[key]),
                "row_count": int(row["row_count"]),
                "metric_sum": round_float(row["metric_sum"]),
            }
        )
    return sorted(normalized, key=lambda row: row[key])


def normalize_top_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = [
        {"id": int(row["id"]), "metric": round_float(row["metric"])} for row in rows
    ]
    return sorted(normalized, key=lambda row: (-row["metric"], row["id"]))[:10]


def normalize_multi_group_rows(rows: list[dict[str, Any]], keys: tuple[str, ...]) -> list[dict[str, Any]]:
    normalized = []
    for row in rows:
        normalized_row = {
            key: str(row[key]) if key in {"category", "dim_label"} else int(row[key])
            for key in keys
        }
        normalized_row["row_count"] = int(row["row_count"])
        normalized_row["metric_sum"] = round_float(row["metric_sum"])
        normalized.append(normalized_row)
    return sorted(normalized, key=lambda row: tuple(row[key] for key in keys))


def normalize_rank_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = [
        {
            "group_key": int(row["group_key"]),
            "id": int(row["id"]),
            "metric": round_float(row["metric"]),
            "rank": int(row.get("rank", row.get("row_number", 1))),
        }
        for row in rows
    ]
    return sorted(normalized, key=lambda row: (row["group_key"], row["rank"], row["id"]))


def normalize_top_group_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = [
        {
            "group_key": int(row["group_key"]),
            "id": int(row["id"]),
            "metric": round_float(row["metric"]),
            "rank": int(row["rank"]),
        }
        for row in rows
    ]
    return sorted(normalized, key=lambda row: (row["group_key"], row["rank"], row["id"]))


def normalize_complex_etl_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = [
        {
            "dim_label": str(row["dim_label"]),
            "bucket": int(row["bucket"]),
            "row_count": int(row["row_count"]),
            "metric_sum": round_float(row["metric_sum"]),
            "weighted_sum": round_float(row["weighted_sum"]),
        }
        for row in rows
    ]
    return sorted(
        normalized,
        key=lambda row: (-row["weighted_sum"], row["dim_label"], row["bucket"]),
    )[:20]


def canonical_digest(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def sql_literal(path: Path) -> str:
    return "'" + str(path).replace("\\", "/").replace("'", "''") + "'"


def fact_path(paths: DatasetPaths, data_format: str) -> Path:
    if data_format == "csv":
        return paths.fact_csv
    if data_format == "jsonl":
        return paths.fact_jsonl
    if data_format == "parquet":
        return paths.fact_parquet
    if data_format == "arrow-ipc":
        return paths.fact_arrow_ipc
    if data_format == "avro":
        return paths.fact_avro
    if data_format == "orc":
        return paths.fact_orc
    raise BenchmarkUnsupported(f"unsupported fact storage format: {data_format}")


def dim_path(paths: DatasetPaths, data_format: str) -> Path:
    if data_format == "csv":
        return paths.dim_csv
    if data_format == "jsonl":
        return paths.dim_jsonl
    if data_format == "parquet":
        return paths.dim_parquet
    if data_format == "arrow-ipc":
        return paths.dim_arrow_ipc
    if data_format == "avro":
        return paths.dim_avro
    if data_format == "orc":
        return paths.dim_orc
    raise BenchmarkUnsupported(f"unsupported dimension storage format: {data_format}")


def fact_part_paths(paths: DatasetPaths, data_format: str) -> tuple[Path, ...]:
    if data_format == "csv" and paths.fact_csv_parts_dir is not None:
        return tuple(sorted(paths.fact_csv_parts_dir.glob("part-*.csv")))
    if data_format == "jsonl" and paths.fact_jsonl_parts_dir is not None:
        return tuple(sorted(paths.fact_jsonl_parts_dir.glob("part-*.jsonl")))
    return ()


def scenario_output_path(
    paths: DatasetPaths, engine: str, data_format: str, scenario: str, extension: str
) -> Path:
    output_dir = paths.root / "scenario_outputs" / engine / data_format / scenario_slug(scenario)
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir / f"part-00000.{extension}"


def scenario_display_name(data_format: str, scenario: str) -> str:
    return f"{data_format}: {scenario}"


def shardloom_source_format(data_format: str) -> str:
    return "arrow_ipc" if data_format == "arrow-ipc" else data_format


def pyarrow_rows(batches: list[Any]) -> list[dict[str, Any]]:
    import pyarrow as pa  # type: ignore

    if not batches:
        return []
    return pa.Table.from_batches(batches).to_pylist()


def configure_java_home() -> None:
    if shutil.which("java") is not None and os.environ.get("JAVA_HOME"):
        return
    candidates = []
    env_java_home = os.environ.get("JAVA_HOME")
    if env_java_home:
        candidates.append(Path(env_java_home))
    if os.name == "nt":
        adoptium_root = Path("C:/Program Files/Eclipse Adoptium")
        if adoptium_root.exists():
            candidates.extend(sorted(adoptium_root.glob("jdk-*-hotspot"), reverse=True))
        java_root = Path("C:/Program Files/Java")
        if java_root.exists():
            candidates.extend(sorted(java_root.glob("jdk-*"), reverse=True))
    for candidate in candidates:
        java_exe = candidate / "bin" / ("java.exe" if os.name == "nt" else "java")
        if java_exe.exists():
            os.environ["JAVA_HOME"] = str(candidate)
            os.environ["PATH"] = str(candidate / "bin") + os.pathsep + os.environ.get("PATH", "")
            return


def pandas_runner() -> EngineRunner:
    import pandas as pd  # type: ignore

    def read_fact(paths: DatasetPaths, data_format: str) -> Any:
        path = fact_path(paths, data_format)
        if data_format == "parquet":
            return pd.read_parquet(path)
        if data_format == "jsonl":
            return pd.read_json(path, lines=True)
        if data_format == "arrow-ipc":
            return pd.read_feather(path)
        if data_format == "orc":
            return pd.read_orc(path)
        return pd.read_csv(path)

    def read_dim(paths: DatasetPaths, data_format: str) -> Any:
        path = dim_path(paths, data_format)
        if data_format == "parquet":
            return pd.read_parquet(path)
        if data_format == "jsonl":
            return pd.read_json(path, lines=True)
        if data_format == "arrow-ipc":
            return pd.read_feather(path)
        if data_format == "orc":
            return pd.read_orc(path)
        return pd.read_csv(path)

    def read_fact_parts(paths: DatasetPaths, data_format: str) -> Any:
        parts = fact_part_paths(paths, data_format)
        if not parts:
            raise BenchmarkUnsupported(
                f"{paths.dataset_profile} does not have {data_format} fact parts"
            )
        frames = []
        for part in parts:
            if data_format == "jsonl":
                frames.append(pd.read_json(part, lines=True))
            else:
                frames.append(pd.read_csv(part))
        return pd.concat(frames, ignore_index=True)

    def ingest(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        return normalize_scalar_result(len(frame), frame["metric"].sum())

    def selective_filter(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        filtered = frame[(frame["flag"] == 1) & (frame["value"] >= 5000)]
        return normalize_scalar_result(len(filtered), filtered["metric"].sum())

    def group_by(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        grouped = (
            frame.groupby("group_key", as_index=False)
            .agg(row_count=("id", "count"), metric_sum=("metric", "sum"))
            .to_dict("records")
        )
        return normalize_group_rows(grouped, "group_key")

    def top_k(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        rows = (
            frame.sort_values(["metric", "id"], ascending=[False, True])
            .head(10)[["id", "metric"]]
            .to_dict("records")
        )
        return normalize_top_rows(rows)

    def hash_join(paths: DatasetPaths, data_format: str) -> Any:
        fact = read_fact(paths, data_format)
        dim = read_dim(paths, data_format)
        joined = fact.merge(dim, on="dim_key", how="inner")
        grouped = (
            joined.groupby("dim_label", as_index=False)
            .agg(row_count=("id", "count"), metric_sum=("metric", "sum"))
            .to_dict("records")
        )
        return normalize_group_rows(grouped, "dim_label")

    def wide_projection(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        projected = frame[["id", "group_key", "category"]]
        return normalize_scalar_result(len(projected), projected["group_key"].sum())

    def distinct_count(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        return {"distinct_category_count": int(frame["category"].nunique())}

    def filter_projection_limit(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        limited = (
            frame[(frame["flag"] == 1) & (frame["value"] >= 5000)][["id", "value", "category"]]
            .sort_values(["id"])
            .head(100)
        )
        return normalize_scalar_result(len(limited), limited["value"].sum())

    def multi_key_group_by(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        rows = (
            frame.groupby(["group_key", "category"], as_index=False)
            .agg(row_count=("id", "count"), metric_sum=("metric", "sum"))
            .to_dict("records")
        )
        return normalize_multi_group_rows(rows, ("group_key", "category"))

    def join_aggregate(paths: DatasetPaths, data_format: str) -> Any:
        fact = read_fact(paths, data_format)
        dim = read_dim(paths, data_format)
        rows = (
            fact[fact["value"] >= 2500]
            .merge(dim, on="dim_key", how="inner")
            .groupby(["dim_label", "category"], as_index=False)
            .agg(row_count=("id", "count"), metric_sum=("metric", "sum"))
            .to_dict("records")
        )
        return normalize_multi_group_rows(rows, ("dim_label", "category"))

    def row_number_window(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        ranked = frame.sort_values(["group_key", "metric", "id"], ascending=[True, False, True])
        ranked["rank"] = ranked.groupby("group_key").cumcount() + 1
        rows = ranked[ranked["rank"] == 1][["group_key", "id", "metric", "rank"]].to_dict(
            "records"
        )
        return normalize_rank_rows(rows)

    def partition_pruning(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        if "event_date" not in frame.columns:
            raise BenchmarkUnsupported("partition pruning requires an event_date fixture column")
        filtered = frame[(frame["event_date"] >= "2024-03-01") & (frame["event_date"] < "2024-06-01")]
        return normalize_scalar_result(len(filtered), filtered["metric"].sum())

    def many_small_files_scan(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact_parts(paths, data_format)
        return normalize_scalar_result(len(frame), frame["metric"].sum())

    def null_heavy_aggregate(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        if "nullable_metric_00" not in frame.columns:
            raise BenchmarkUnsupported("null-heavy aggregate requires nullable_metric_00")
        series = pd.to_numeric(frame["nullable_metric_00"], errors="coerce")
        return {
            "row_count": int(series.notna().sum()),
            "metric_sum": round_float(series.sum(skipna=True)),
        }

    def high_cardinality_string_group_distinct(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        rows = (
            frame.groupby("category", as_index=False)
            .agg(row_count=("id", "count"), metric_sum=("metric", "sum"))
            .to_dict("records")
        )
        return {
            "distinct_category_count": int(frame["category"].nunique()),
            "groups": normalize_multi_group_rows(rows, ("category",))[:100],
        }

    def top_n_per_group(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        ranked = frame.sort_values(["group_key", "metric", "id"], ascending=[True, False, True])
        ranked["rank"] = ranked.groupby("group_key").cumcount() + 1
        rows = ranked[ranked["rank"] <= 3][["group_key", "id", "metric", "rank"]].to_dict(
            "records"
        )
        return normalize_top_group_rows(rows)

    def clean_cast_filter_write(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        required = {"raw_event_time", "dirty_numeric", "dirty_flag"}
        missing = sorted(required - set(frame.columns))
        if missing:
            raise BenchmarkUnsupported(
                "clean/cast/filter/write requires dirty fixture columns: "
                + ",".join(missing)
            )
        parsed = pd.to_datetime(
            frame["raw_event_time"],
            format="%Y-%m-%dT%H:%M:%SZ",
            errors="coerce",
            utc=True,
        )
        numeric = pd.to_numeric(frame["dirty_numeric"], errors="coerce")
        valid = parsed.notna() & numeric.notna() & (frame["dirty_flag"].astype(str) == "Y")
        filtered = frame[valid & (numeric >= 500)].copy()
        filtered["clean_numeric"] = numeric[filtered.index]
        output_path = scenario_output_path(
            paths, "pandas", data_format, "clean/cast/filter/write", "csv"
        )
        filtered[["id", "raw_event_time", "clean_numeric", "category"]].to_csv(
            output_path, index=False
        )
        return normalize_scalar_result(len(filtered), filtered["clean_numeric"].sum())

    def malformed_timestamp_dirty_csv(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        if "raw_event_time" not in frame.columns:
            raise BenchmarkUnsupported("dirty CSV scenario requires raw_event_time")
        parsed = pd.to_datetime(
            frame["raw_event_time"],
            format="%Y-%m-%dT%H:%M:%SZ",
            errors="coerce",
            utc=True,
        )
        numeric = pd.to_numeric(frame["dirty_numeric"], errors="coerce")
        valid = parsed.notna() & numeric.notna()
        return normalize_scalar_result(int(valid.sum()), numeric[valid].sum())

    def small_change_over_large_base(paths: DatasetPaths, data_format: str) -> Any:
        if paths.cdc_delta_csv is None or not paths.cdc_delta_csv.exists():
            raise BenchmarkUnsupported("CDC overlay scenario requires cdc_delta.csv")
        frame = read_fact(paths, data_format).set_index("id", drop=False)
        overlay = pd.read_csv(paths.cdc_delta_csv)
        for row in overlay.to_dict("records"):
            row_id = int(row["id"])
            op = str(row["op"])
            if op == "delete":
                frame = frame.drop(index=row_id, errors="ignore")
            else:
                frame.loc[row_id, "id"] = row_id
                frame.loc[row_id, "value"] = int(row["value"])
                frame.loc[row_id, "metric"] = float(row["metric"])
                frame.loc[row_id, "flag"] = 1
                frame.loc[row_id, "category"] = f"cdc_{op}"
        return normalize_scalar_result(len(frame), frame["metric"].sum())

    def nested_json_field_scan(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        if "nested_payload" not in frame.columns:
            raise BenchmarkUnsupported("nested JSON scenario requires nested_payload")
        scores = []
        flagged = 0
        for value in frame["nested_payload"]:
            payload = json.loads(value) if isinstance(value, str) else value
            scores.append(float(payload["metrics"]["score"]))
            flagged += 1 if payload["event"]["flag"] else 0
        return {"row_count": len(scores), "metric_sum": round_float(sum(scores)), "flagged": flagged}

    def scale_stress(paths: DatasetPaths, data_format: str) -> Any:
        fact = read_fact(paths, data_format)
        dim = read_dim(paths, data_format)
        expanded = fact.merge(dim, on="dim_key", how="inner")
        expanded["skew_key"] = expanded["group_key"] % 10
        grouped = (
            expanded.groupby("skew_key", as_index=False)
            .agg(row_count=("id", "count"), metric_sum=("metric", "sum"))
            .to_dict("records")
        )
        return normalize_group_rows(grouped, "skew_key")

    def complex_etl(paths: DatasetPaths, data_format: str) -> Any:
        fact = read_fact(paths, data_format)
        dim = read_dim(paths, data_format)
        joined = fact[fact["value"] >= 2500].merge(dim, on="dim_key", how="inner")
        joined["bucket"] = joined["group_key"] % 10
        joined["weighted_metric"] = joined["metric"] * (joined["weight"] + 1)
        rows = (
            joined.groupby(["dim_label", "bucket"], as_index=False)
            .agg(
                row_count=("id", "count"),
                metric_sum=("metric", "sum"),
                weighted_sum=("weighted_metric", "sum"),
            )
            .sort_values(["weighted_sum", "dim_label", "bucket"], ascending=[False, True, True])
            .head(20)
            .to_dict("records")
        )
        return normalize_complex_etl_rows(rows)

    return EngineRunner(
        "pandas",
        module_version("pandas"),
        {
            "csv/file ingest": ingest,
            "selective filter": selective_filter,
            "group by aggregation": group_by,
            "sort and top-k": top_k,
            "hash join": hash_join,
            "wide projection": wide_projection,
            "distinct count": distinct_count,
            "filter + projection + limit": filter_projection_limit,
            "multi-key group by": multi_key_group_by,
            "join + aggregate": join_aggregate,
            "row number window": row_number_window,
            "partition pruning": partition_pruning,
            "many-small-files scan": many_small_files_scan,
            "null-heavy aggregate": null_heavy_aggregate,
            "high-cardinality string group/distinct": high_cardinality_string_group_distinct,
            "top-N per group": top_n_per_group,
            "clean/cast/filter/write": clean_cast_filter_write,
            "malformed timestamp / dirty CSV": malformed_timestamp_dirty_csv,
            "small change over large base": small_change_over_large_base,
            "nested JSON field scan": nested_json_field_scan,
            "scale stress skewed join aggregation": scale_stress,
            "scale stress multi-stage etl": complex_etl,
        },
        formats=("csv", "jsonl", "parquet", "arrow-ipc", "orc"),
    )


def polars_runner() -> EngineRunner:
    import polars as pl  # type: ignore

    def read_fact(paths: DatasetPaths, data_format: str) -> Any:
        path = fact_path(paths, data_format)
        if data_format == "parquet":
            return pl.read_parquet(path)
        if data_format == "jsonl":
            return pl.read_ndjson(path)
        if data_format == "arrow-ipc":
            return pl.read_ipc(path)
        if data_format == "avro":
            return pl.read_avro(path)
        return pl.read_csv(path)

    def read_dim(paths: DatasetPaths, data_format: str) -> Any:
        path = dim_path(paths, data_format)
        if data_format == "parquet":
            return pl.read_parquet(path)
        if data_format == "jsonl":
            return pl.read_ndjson(path)
        if data_format == "arrow-ipc":
            return pl.read_ipc(path)
        if data_format == "avro":
            return pl.read_avro(path)
        return pl.read_csv(path)

    def ingest(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        return normalize_scalar_result(frame.height, frame["metric"].sum())

    def selective_filter(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        filtered = frame.filter((pl.col("flag") == 1) & (pl.col("value") >= 5000))
        return normalize_scalar_result(filtered.height, filtered["metric"].sum())

    def group_by(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        rows = (
            frame.group_by("group_key")
            .agg(
                [
                    pl.len().alias("row_count"),
                    pl.col("metric").sum().alias("metric_sum"),
                ]
            )
            .to_dicts()
        )
        return normalize_group_rows(rows, "group_key")

    def top_k(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        rows = (
            frame.sort(["metric", "id"], descending=[True, False])
            .head(10)
            .select(["id", "metric"])
            .to_dicts()
        )
        return normalize_top_rows(rows)

    def hash_join(paths: DatasetPaths, data_format: str) -> Any:
        fact = read_fact(paths, data_format)
        dim = read_dim(paths, data_format)
        rows = (
            fact.join(dim, on="dim_key", how="inner")
            .group_by("dim_label")
            .agg(
                [
                    pl.len().alias("row_count"),
                    pl.col("metric").sum().alias("metric_sum"),
                ]
            )
            .to_dicts()
        )
        return normalize_group_rows(rows, "dim_label")

    def wide_projection(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        projected = frame.select(["id", "group_key", "category"])
        return normalize_scalar_result(projected.height, projected["group_key"].sum())

    def distinct_count(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        return {"distinct_category_count": int(frame["category"].n_unique())}

    def filter_projection_limit(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        limited = (
            frame.filter((pl.col("flag") == 1) & (pl.col("value") >= 5000))
            .select(["id", "value", "category"])
            .sort("id")
            .head(100)
        )
        return normalize_scalar_result(limited.height, limited["value"].sum())

    def multi_key_group_by(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        rows = (
            frame.group_by(["group_key", "category"])
            .agg(
                [
                    pl.len().alias("row_count"),
                    pl.col("metric").sum().alias("metric_sum"),
                ]
            )
            .to_dicts()
        )
        return normalize_multi_group_rows(rows, ("group_key", "category"))

    def join_aggregate(paths: DatasetPaths, data_format: str) -> Any:
        fact = read_fact(paths, data_format)
        dim = read_dim(paths, data_format)
        rows = (
            fact.filter(pl.col("value") >= 2500)
            .join(dim, on="dim_key", how="inner")
            .group_by(["dim_label", "category"])
            .agg(
                [
                    pl.len().alias("row_count"),
                    pl.col("metric").sum().alias("metric_sum"),
                ]
            )
            .to_dicts()
        )
        return normalize_multi_group_rows(rows, ("dim_label", "category"))

    def row_number_window(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        rows = (
            frame.sort(["group_key", "metric", "id"], descending=[False, True, False])
            .with_columns((pl.col("id").cum_count().over("group_key")).alias("rank"))
            .filter(pl.col("rank") == 1)
            .select(["group_key", "id", "metric", "rank"])
            .to_dicts()
        )
        return normalize_rank_rows(rows)

    def scale_stress(paths: DatasetPaths, data_format: str) -> Any:
        fact = read_fact(paths, data_format)
        dim = read_dim(paths, data_format)
        rows = (
            fact.join(dim, on="dim_key", how="inner")
            .with_columns((pl.col("group_key") % 10).alias("skew_key"))
            .group_by("skew_key")
            .agg(
                [
                    pl.len().alias("row_count"),
                    pl.col("metric").sum().alias("metric_sum"),
                ]
            )
            .to_dicts()
        )
        return normalize_group_rows(rows, "skew_key")

    def complex_etl(paths: DatasetPaths, data_format: str) -> Any:
        fact = read_fact(paths, data_format)
        dim = read_dim(paths, data_format)
        rows = (
            fact.filter(pl.col("value") >= 2500)
            .join(dim, on="dim_key", how="inner")
            .with_columns(
                [
                    (pl.col("group_key") % 10).alias("bucket"),
                    (pl.col("metric") * (pl.col("weight") + 1)).alias("weighted_metric"),
                ]
            )
            .group_by(["dim_label", "bucket"])
            .agg(
                [
                    pl.len().alias("row_count"),
                    pl.col("metric").sum().alias("metric_sum"),
                    pl.col("weighted_metric").sum().alias("weighted_sum"),
                ]
            )
            .sort(["weighted_sum", "dim_label", "bucket"], descending=[True, False, False])
            .head(20)
            .to_dicts()
        )
        return normalize_complex_etl_rows(rows)

    return EngineRunner(
        "polars",
        module_version("polars"),
        {
            "csv/file ingest": ingest,
            "selective filter": selective_filter,
            "group by aggregation": group_by,
            "sort and top-k": top_k,
            "hash join": hash_join,
            "wide projection": wide_projection,
            "distinct count": distinct_count,
            "filter + projection + limit": filter_projection_limit,
            "multi-key group by": multi_key_group_by,
            "join + aggregate": join_aggregate,
            "row number window": row_number_window,
            "scale stress skewed join aggregation": scale_stress,
            "scale stress multi-stage etl": complex_etl,
        },
        formats=("csv", "jsonl", "parquet", "arrow-ipc", "avro"),
    )


def duckdb_runner() -> EngineRunner:
    import duckdb  # type: ignore

    con = duckdb.connect(database=":memory:")

    def table_expr(paths: DatasetPaths, table: str, data_format: str) -> str:
        path = fact_path(paths, data_format) if table == "fact" else dim_path(paths, data_format)
        if data_format == "parquet":
            function = "read_parquet"
        elif data_format == "jsonl":
            function = "read_json_auto"
        else:
            function = "read_csv_auto"
        return f"{function}({sql_literal(path)})"

    def query(paths: DatasetPaths, data_format: str, sql: str) -> list[dict[str, Any]]:
        sql = sql.replace("{fact}", table_expr(paths, "fact", data_format)).replace(
            "{dim}", table_expr(paths, "dim", data_format)
        )
        columns = [column[0] for column in con.execute(sql).description]
        return [dict(zip(columns, row)) for row in con.fetchall()]

    def ingest(paths: DatasetPaths, data_format: str) -> Any:
        rows = query(
            paths,
            data_format,
            "select count(*) as row_count, sum(metric) as metric_sum from {fact}",
        )
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def selective_filter(paths: DatasetPaths, data_format: str) -> Any:
        rows = query(
            paths,
            data_format,
            "select count(*) as row_count, sum(metric) as metric_sum "
            "from {fact} where flag = 1 and value >= 5000",
        )
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def group_by(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_group_rows(
            query(
                paths,
                data_format,
                "select group_key, count(*) as row_count, sum(metric) as metric_sum "
                "from {fact} group by group_key",
            ),
            "group_key",
        )

    def top_k(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_top_rows(
            query(
                paths,
                data_format,
                "select id, metric from {fact} "
                "order by metric desc, id asc limit 10",
            )
        )

    def hash_join(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_group_rows(
            query(
                paths,
                data_format,
                "select d.dim_label, count(*) as row_count, sum(f.metric) as metric_sum "
                "from {fact} f join {dim} d "
                "on f.dim_key = d.dim_key group by d.dim_label",
            ),
            "dim_label",
        )

    def wide_projection(paths: DatasetPaths, data_format: str) -> Any:
        rows = query(
            paths,
            data_format,
            "select count(*) as row_count, sum(group_key) as metric_sum "
            "from (select id, group_key, category from {fact})",
        )
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def distinct_count(paths: DatasetPaths, data_format: str) -> Any:
        rows = query(
            paths,
            data_format,
            "select count(distinct category) as distinct_category_count from {fact}",
        )
        return {"distinct_category_count": int(rows[0]["distinct_category_count"])}

    def filter_projection_limit(paths: DatasetPaths, data_format: str) -> Any:
        rows = query(
            paths,
            data_format,
            "select count(*) as row_count, sum(value) as metric_sum "
            "from (select id, value, category from {fact} "
            "where flag = 1 and value >= 5000 order by id asc limit 100)",
        )
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def multi_key_group_by(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_multi_group_rows(
            query(
                paths,
                data_format,
                "select group_key, category, count(*) as row_count, sum(metric) as metric_sum "
                "from {fact} group by group_key, category",
            ),
            ("group_key", "category"),
        )

    def join_aggregate(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_multi_group_rows(
            query(
                paths,
                data_format,
                "select d.dim_label, f.category, count(*) as row_count, sum(f.metric) as metric_sum "
                "from {fact} f join {dim} d on f.dim_key = d.dim_key "
                "where f.value >= 2500 group by d.dim_label, f.category",
            ),
            ("dim_label", "category"),
        )

    def row_number_window(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_rank_rows(
            query(
                paths,
                data_format,
                "select group_key, id, metric, rank from ("
                "select group_key, id, metric, "
                "row_number() over (partition by group_key order by metric desc, id asc) as rank "
                "from {fact}) where rank = 1",
            )
        )

    def scale_stress(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_group_rows(
            query(
                paths,
                data_format,
                "select f.group_key % 10 as skew_key, count(*) as row_count, sum(f.metric) as metric_sum "
                "from {fact} f join {dim} d "
                "on f.dim_key = d.dim_key group by skew_key",
            ),
            "skew_key",
        )

    def complex_etl(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_complex_etl_rows(
            query(
                paths,
                data_format,
                "select d.dim_label, f.group_key % 10 as bucket, count(*) as row_count, "
                "sum(f.metric) as metric_sum, sum(f.metric * (d.weight + 1)) as weighted_sum "
                "from {fact} f join {dim} d "
                "on f.dim_key = d.dim_key where f.value >= 2500 "
                "group by d.dim_label, bucket "
                "order by weighted_sum desc, d.dim_label asc, bucket asc limit 20",
            )
        )

    return EngineRunner(
        "duckdb",
        module_version("duckdb"),
        {
            "csv/file ingest": ingest,
            "selective filter": selective_filter,
            "group by aggregation": group_by,
            "sort and top-k": top_k,
            "hash join": hash_join,
            "wide projection": wide_projection,
            "distinct count": distinct_count,
            "filter + projection + limit": filter_projection_limit,
            "multi-key group by": multi_key_group_by,
            "join + aggregate": join_aggregate,
            "row number window": row_number_window,
            "scale stress skewed join aggregation": scale_stress,
            "scale stress multi-stage etl": complex_etl,
        },
        formats=("csv", "jsonl", "parquet"),
        close=con.close,
    )


def spark_runner(profile: str) -> EngineRunner:
    if shutil.which("java") is None and not os.environ.get("JAVA_HOME"):
        raise BenchmarkUnsupported(
            "Spark/PySpark requires a local JDK. Install JDK 17 or newer, set JAVA_HOME, "
            "and ensure java is on PATH before running Spark benchmark rows."
        )
    import pyspark  # type: ignore
    from pyspark.sql import SparkSession, functions as F  # type: ignore
    from pyspark.sql.window import Window  # type: ignore

    builder = SparkSession.builder.master("local[*]").appName(
        f"shardloom-traditional-analytics-benchmark-{profile}"
    )
    builder = builder.config("spark.ui.enabled", "false")
    profile_notes = ["master=local[*]", "spark.ui.enabled=false"]
    if profile == "local-tuned":
        local_threads = os.cpu_count() or 1
        shuffle_partitions = max(1, min(local_threads, 8))
        builder = (
            builder.config("spark.sql.shuffle.partitions", str(shuffle_partitions))
            .config("spark.default.parallelism", str(shuffle_partitions))
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        )
        profile_notes.extend(
            [
                f"spark.sql.shuffle.partitions={shuffle_partitions}",
                f"spark.default.parallelism={shuffle_partitions}",
                "spark.sql.adaptive.enabled=true",
                "spark.sql.adaptive.coalescePartitions.enabled=true",
            ]
        )
    elif profile != "default":
        raise BenchmarkUnsupported(f"unknown Spark benchmark profile: {profile}")

    spark_session: Any | None = None

    def spark_instance() -> Any:
        nonlocal spark_session
        if spark_session is None:
            spark_session = builder.getOrCreate()
            spark_session.sparkContext.setLogLevel("ERROR")
        return spark_session

    def close_spark() -> None:
        nonlocal spark_session
        if spark_session is not None:
            spark_session.stop()
            spark_session = None

    def warmup_spark() -> None:
        spark_instance()

    def read_fact(paths: DatasetPaths, data_format: str) -> Any:
        if data_format == "parquet":
            return spark_instance().read.parquet(str(paths.fact_parquet))
        if data_format == "jsonl":
            return spark_instance().read.json(str(paths.fact_jsonl))
        if data_format == "orc":
            return spark_instance().read.orc(str(paths.fact_orc))
        return spark_instance().read.option("header", True).option("inferSchema", True).csv(
            str(paths.fact_csv)
        )

    def read_dim(paths: DatasetPaths, data_format: str) -> Any:
        if data_format == "parquet":
            return spark_instance().read.parquet(str(paths.dim_parquet))
        if data_format == "jsonl":
            return spark_instance().read.json(str(paths.dim_jsonl))
        if data_format == "orc":
            return spark_instance().read.orc(str(paths.dim_orc))
        return spark_instance().read.option("header", True).option("inferSchema", True).csv(
            str(paths.dim_csv)
        )

    def ingest(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        row = frame.agg(F.count("*").alias("row_count"), F.sum("metric").alias("metric_sum")).first()
        return normalize_scalar_result(row["row_count"], row["metric_sum"])

    def selective_filter(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format).where((F.col("flag") == 1) & (F.col("value") >= 5000))
        row = frame.agg(F.count("*").alias("row_count"), F.sum("metric").alias("metric_sum")).first()
        return normalize_scalar_result(row["row_count"], row["metric_sum"])

    def group_by(paths: DatasetPaths, data_format: str) -> Any:
        rows = [
            row.asDict()
            for row in read_fact(paths, data_format)
            .groupBy("group_key")
            .agg(F.count("*").alias("row_count"), F.sum("metric").alias("metric_sum"))
            .collect()
        ]
        return normalize_group_rows(rows, "group_key")

    def top_k(paths: DatasetPaths, data_format: str) -> Any:
        rows = [
            row.asDict()
            for row in read_fact(paths, data_format)
            .orderBy(F.col("metric").desc(), F.col("id").asc())
            .select("id", "metric")
            .limit(10)
            .collect()
        ]
        return normalize_top_rows(rows)

    def hash_join(paths: DatasetPaths, data_format: str) -> Any:
        rows = [
            row.asDict()
            for row in read_fact(paths, data_format)
            .join(read_dim(paths, data_format), on="dim_key", how="inner")
            .groupBy("dim_label")
            .agg(F.count("*").alias("row_count"), F.sum("metric").alias("metric_sum"))
            .collect()
        ]
        return normalize_group_rows(rows, "dim_label")

    def wide_projection(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format).select("id", "group_key", "category")
        row = frame.agg(
            F.count("*").alias("row_count"), F.sum("group_key").alias("metric_sum")
        ).first()
        return normalize_scalar_result(row["row_count"], row["metric_sum"])

    def distinct_count(paths: DatasetPaths, data_format: str) -> Any:
        row = read_fact(paths, data_format).agg(F.countDistinct("category").alias("distinct_category_count")).first()
        return {"distinct_category_count": int(row["distinct_category_count"])}

    def filter_projection_limit(paths: DatasetPaths, data_format: str) -> Any:
        frame = (
            read_fact(paths, data_format)
            .where((F.col("flag") == 1) & (F.col("value") >= 5000))
            .select("id", "value", "category")
            .orderBy(F.col("id").asc())
            .limit(100)
        )
        row = frame.agg(
            F.count("*").alias("row_count"), F.sum("value").alias("metric_sum")
        ).first()
        return normalize_scalar_result(row["row_count"], row["metric_sum"])

    def multi_key_group_by(paths: DatasetPaths, data_format: str) -> Any:
        rows = [
            row.asDict()
            for row in read_fact(paths, data_format)
            .groupBy("group_key", "category")
            .agg(F.count("*").alias("row_count"), F.sum("metric").alias("metric_sum"))
            .collect()
        ]
        return normalize_multi_group_rows(rows, ("group_key", "category"))

    def join_aggregate(paths: DatasetPaths, data_format: str) -> Any:
        rows = [
            row.asDict()
            for row in read_fact(paths, data_format)
            .where(F.col("value") >= 2500)
            .join(read_dim(paths, data_format), on="dim_key", how="inner")
            .groupBy("dim_label", "category")
            .agg(F.count("*").alias("row_count"), F.sum("metric").alias("metric_sum"))
            .collect()
        ]
        return normalize_multi_group_rows(rows, ("dim_label", "category"))

    def row_number_window(paths: DatasetPaths, data_format: str) -> Any:
        window = Window.partitionBy("group_key").orderBy(F.col("metric").desc(), F.col("id").asc())
        rows = [
            row.asDict()
            for row in read_fact(paths, data_format)
            .withColumn("rank", F.row_number().over(window))
            .where(F.col("rank") == 1)
            .select("group_key", "id", "metric", "rank")
            .collect()
        ]
        return normalize_rank_rows(rows)

    def scale_stress(paths: DatasetPaths, data_format: str) -> Any:
        rows = [
            row.asDict()
            for row in read_fact(paths, data_format)
            .join(read_dim(paths, data_format), on="dim_key", how="inner")
            .withColumn("skew_key", F.col("group_key") % F.lit(10))
            .groupBy("skew_key")
            .agg(F.count("*").alias("row_count"), F.sum("metric").alias("metric_sum"))
            .collect()
        ]
        return normalize_group_rows(rows, "skew_key")

    def complex_etl(paths: DatasetPaths, data_format: str) -> Any:
        joined = (
            read_fact(paths, data_format)
            .where(F.col("value") >= 2500)
            .join(read_dim(paths, data_format), on="dim_key", how="inner")
            .withColumn("bucket", F.col("group_key") % F.lit(10))
            .withColumn("weighted_metric", F.col("metric") * (F.col("weight") + F.lit(1)))
        )
        rows = [
            row.asDict()
            for row in joined.groupBy("dim_label", "bucket")
            .agg(
                F.count("*").alias("row_count"),
                F.sum("metric").alias("metric_sum"),
                F.sum("weighted_metric").alias("weighted_sum"),
            )
            .orderBy(F.col("weighted_sum").desc(), F.col("dim_label").asc(), F.col("bucket").asc())
            .limit(20)
            .collect()
        ]
        return normalize_complex_etl_rows(rows)

    return EngineRunner(
        "spark-default" if profile == "default" else "spark-local-tuned",
        f"{module_version('pyspark')} ({'; '.join(profile_notes)})",
        {
            "csv/file ingest": ingest,
            "selective filter": selective_filter,
            "group by aggregation": group_by,
            "sort and top-k": top_k,
            "hash join": hash_join,
            "wide projection": wide_projection,
            "distinct count": distinct_count,
            "filter + projection + limit": filter_projection_limit,
            "multi-key group by": multi_key_group_by,
            "join + aggregate": join_aggregate,
            "row number window": row_number_window,
            "scale stress skewed join aggregation": scale_stress,
            "scale stress multi-stage etl": complex_etl,
        },
        formats=("csv", "jsonl", "parquet", "orc"),
        warmup=warmup_spark,
        close=close_spark,
    )


def spark_default_runner() -> EngineRunner:
    return spark_runner("default")


def spark_local_tuned_runner() -> EngineRunner:
    return spark_runner("local-tuned")


def datafusion_runner() -> EngineRunner:
    import datafusion  # type: ignore

    def query(paths: DatasetPaths, data_format: str, sql: str) -> list[dict[str, Any]]:
        ctx = datafusion.SessionContext()
        if data_format == "parquet":
            ctx.register_parquet("fact", paths.fact_parquet)
            ctx.register_parquet("dim", paths.dim_parquet)
        else:
            ctx.register_csv("fact", paths.fact_csv, has_header=True)
            ctx.register_csv("dim", paths.dim_csv, has_header=True)
        return pyarrow_rows(ctx.sql(sql).collect())

    def ingest(paths: DatasetPaths, data_format: str) -> Any:
        rows = query(paths, data_format, "select count(*) as row_count, sum(metric) as metric_sum from fact")
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def selective_filter(paths: DatasetPaths, data_format: str) -> Any:
        rows = query(
            paths,
            data_format,
            "select count(*) as row_count, sum(metric) as metric_sum "
            "from fact where flag = 1 and value >= 5000",
        )
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def group_by(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_group_rows(
            query(
                paths,
                data_format,
                "select group_key, count(*) as row_count, sum(metric) as metric_sum "
                "from fact group by group_key",
            ),
            "group_key",
        )

    def top_k(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_top_rows(
            query(paths, data_format, "select id, metric from fact order by metric desc, id asc limit 10")
        )

    def hash_join(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_group_rows(
            query(
                paths,
                data_format,
                "select d.dim_label, count(*) as row_count, sum(f.metric) as metric_sum "
                "from fact f join dim d on f.dim_key = d.dim_key group by d.dim_label",
            ),
            "dim_label",
        )

    def wide_projection(paths: DatasetPaths, data_format: str) -> Any:
        rows = query(
            paths,
            data_format,
            "select count(*) as row_count, sum(group_key) as metric_sum "
            "from (select id, group_key, category from fact)",
        )
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def distinct_count(paths: DatasetPaths, data_format: str) -> Any:
        rows = query(paths, data_format, "select count(distinct category) as distinct_category_count from fact")
        return {"distinct_category_count": int(rows[0]["distinct_category_count"])}

    def filter_projection_limit(paths: DatasetPaths, data_format: str) -> Any:
        rows = query(
            paths,
            data_format,
            "select count(*) as row_count, sum(value) as metric_sum "
            "from (select id, value, category from fact "
            "where flag = 1 and value >= 5000 order by id asc limit 100)",
        )
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def multi_key_group_by(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_multi_group_rows(
            query(
                paths,
                data_format,
                "select group_key, category, count(*) as row_count, sum(metric) as metric_sum "
                "from fact group by group_key, category",
            ),
            ("group_key", "category"),
        )

    def join_aggregate(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_multi_group_rows(
            query(
                paths,
                data_format,
                "select d.dim_label, f.category, count(*) as row_count, sum(f.metric) as metric_sum "
                "from fact f join dim d on f.dim_key = d.dim_key "
                "where f.value >= 2500 group by d.dim_label, f.category",
            ),
            ("dim_label", "category"),
        )

    def row_number_window(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_rank_rows(
            query(
                paths,
                data_format,
                "select group_key, id, metric, rank from ("
                "select group_key, id, metric, "
                "row_number() over (partition by group_key order by metric desc, id asc) as rank "
                "from fact) where rank = 1",
            )
        )

    def scale_stress(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_group_rows(
            query(
                paths,
                data_format,
                "select f.group_key % 10 as skew_key, count(*) as row_count, sum(f.metric) as metric_sum "
                "from fact f join dim d on f.dim_key = d.dim_key group by skew_key",
            ),
            "skew_key",
        )

    def complex_etl(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_complex_etl_rows(
            query(
                paths,
                data_format,
                "select d.dim_label, f.group_key % 10 as bucket, count(*) as row_count, "
                "sum(f.metric) as metric_sum, sum(f.metric * (d.weight + 1)) as weighted_sum "
                "from fact f join dim d on f.dim_key = d.dim_key "
                "where f.value >= 2500 group by d.dim_label, bucket "
                "order by weighted_sum desc, d.dim_label asc, bucket asc limit 20",
            )
        )

    return EngineRunner(
        "datafusion",
        module_version("datafusion"),
        {
            "csv/file ingest": ingest,
            "selective filter": selective_filter,
            "group by aggregation": group_by,
            "sort and top-k": top_k,
            "hash join": hash_join,
            "wide projection": wide_projection,
            "distinct count": distinct_count,
            "filter + projection + limit": filter_projection_limit,
            "multi-key group by": multi_key_group_by,
            "join + aggregate": join_aggregate,
            "row number window": row_number_window,
            "scale stress skewed join aggregation": scale_stress,
            "scale stress multi-stage etl": complex_etl,
        },
        formats=("csv", "parquet"),
    )


def dask_runner() -> EngineRunner:
    import dask  # type: ignore
    import dask.dataframe as dd  # type: ignore

    blocksize = None if DASK_BLOCKSIZE == "default" else DASK_BLOCKSIZE

    def read_fact(paths: DatasetPaths, data_format: str) -> Any:
        if data_format == "parquet":
            return dd.read_parquet(paths.fact_parquet)
        if data_format == "jsonl":
            return dd.read_json(paths.fact_jsonl, lines=True, blocksize=blocksize)
        return dd.read_csv(paths.fact_csv, blocksize=blocksize)

    def read_dim(paths: DatasetPaths, data_format: str) -> Any:
        if data_format == "parquet":
            return dd.read_parquet(paths.dim_parquet)
        if data_format == "jsonl":
            return dd.read_json(paths.dim_jsonl, lines=True, blocksize=blocksize)
        return dd.read_csv(paths.dim_csv, blocksize=blocksize)

    def compute_one(*values: Any) -> tuple[Any, ...]:
        return dask.compute(*values, scheduler=DASK_SCHEDULER)

    def compute_frame(value: Any) -> Any:
        return value.compute(scheduler=DASK_SCHEDULER)

    def ingest(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        row_count, metric_sum = compute_one(frame.id.count(), frame.metric.sum())
        return normalize_scalar_result(row_count, metric_sum)

    def selective_filter(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        filtered = frame[(frame.flag == 1) & (frame.value >= 5000)]
        row_count, metric_sum = compute_one(filtered.id.count(), filtered.metric.sum())
        return normalize_scalar_result(row_count, metric_sum)

    def group_by(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        counts = frame.groupby("group_key").id.count().rename("row_count")
        sums = frame.groupby("group_key").metric.sum().rename("metric_sum")
        rows = compute_frame(dd.concat([counts, sums], axis=1).reset_index()).to_dict("records")
        return normalize_group_rows(rows, "group_key")

    def top_k(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        rows = (
            compute_frame(frame.nlargest(10, "metric")[["id", "metric"]])
            .sort_values(["metric", "id"], ascending=[False, True])
            .to_dict("records")
        )
        return normalize_top_rows(rows)

    def hash_join(paths: DatasetPaths, data_format: str) -> Any:
        fact = read_fact(paths, data_format)
        dim = read_dim(paths, data_format)
        joined = fact.merge(dim, on="dim_key", how="inner")
        counts = joined.groupby("dim_label").id.count().rename("row_count")
        sums = joined.groupby("dim_label").metric.sum().rename("metric_sum")
        rows = compute_frame(dd.concat([counts, sums], axis=1).reset_index()).to_dict("records")
        return normalize_group_rows(rows, "dim_label")

    def wide_projection(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)[["id", "group_key", "category"]]
        row_count, metric_sum = compute_one(frame.id.count(), frame.group_key.sum())
        return normalize_scalar_result(row_count, metric_sum)

    def distinct_count(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        distinct = compute_frame(frame.category.nunique())
        return {"distinct_category_count": int(distinct)}

    def filter_projection_limit(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        limited = compute_frame(
            frame[(frame.flag == 1) & (frame.value >= 5000)][["id", "value", "category"]]
        ).sort_values("id").head(100)
        return normalize_scalar_result(len(limited), limited["value"].sum())

    def multi_key_group_by(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        groups = frame.groupby(["group_key", "category"])
        counts = groups.id.count().rename("row_count")
        sums = groups.metric.sum().rename("metric_sum")
        rows = compute_frame(dd.concat([counts, sums], axis=1).reset_index()).to_dict("records")
        return normalize_multi_group_rows(rows, ("group_key", "category"))

    def join_aggregate(paths: DatasetPaths, data_format: str) -> Any:
        fact = read_fact(paths, data_format)
        dim = read_dim(paths, data_format)
        joined = fact[fact.value >= 2500].merge(dim, on="dim_key", how="inner")
        groups = joined.groupby(["dim_label", "category"])
        counts = groups.id.count().rename("row_count")
        sums = groups.metric.sum().rename("metric_sum")
        rows = compute_frame(dd.concat([counts, sums], axis=1).reset_index()).to_dict("records")
        return normalize_multi_group_rows(rows, ("dim_label", "category"))

    def row_number_window(paths: DatasetPaths, data_format: str) -> Any:
        frame = compute_frame(read_fact(paths, data_format))
        ranked = frame.sort_values(["group_key", "metric", "id"], ascending=[True, False, True])
        ranked["rank"] = ranked.groupby("group_key").cumcount() + 1
        rows = ranked[ranked["rank"] == 1][["group_key", "id", "metric", "rank"]].to_dict(
            "records"
        )
        return normalize_rank_rows(rows)

    def scale_stress(paths: DatasetPaths, data_format: str) -> Any:
        fact = read_fact(paths, data_format)
        dim = read_dim(paths, data_format)
        joined = fact.merge(dim, on="dim_key", how="inner")
        joined = joined.assign(skew_key=joined.group_key % 10)
        counts = joined.groupby("skew_key").id.count().rename("row_count")
        sums = joined.groupby("skew_key").metric.sum().rename("metric_sum")
        rows = compute_frame(dd.concat([counts, sums], axis=1).reset_index()).to_dict("records")
        return normalize_group_rows(rows, "skew_key")

    def complex_etl(paths: DatasetPaths, data_format: str) -> Any:
        fact = read_fact(paths, data_format)
        dim = read_dim(paths, data_format)
        joined = fact[fact.value >= 2500].merge(dim, on="dim_key", how="inner")
        joined = joined.assign(
            bucket=joined.group_key % 10,
            weighted_metric=joined.metric * (joined["weight"] + 1),
        )
        groups = joined.groupby(["dim_label", "bucket"])
        counts = groups.id.count().rename("row_count")
        sums = groups.metric.sum().rename("metric_sum")
        weighted_sums = groups.weighted_metric.sum().rename("weighted_sum")
        rows = (
            compute_frame(dd.concat([counts, sums, weighted_sums], axis=1).reset_index())
            .sort_values(["weighted_sum", "dim_label", "bucket"], ascending=[False, True, True])
            .head(20)
            .to_dict("records")
        )
        return normalize_complex_etl_rows(rows)

    return EngineRunner(
        "dask",
        module_version("dask"),
        {
            "csv/file ingest": ingest,
            "selective filter": selective_filter,
            "group by aggregation": group_by,
            "sort and top-k": top_k,
            "hash join": hash_join,
            "wide projection": wide_projection,
            "distinct count": distinct_count,
            "filter + projection + limit": filter_projection_limit,
            "multi-key group by": multi_key_group_by,
            "join + aggregate": join_aggregate,
            "row number window": row_number_window,
            "scale stress skewed join aggregation": scale_stress,
            "scale stress multi-stage etl": complex_etl,
        },
        formats=("csv", "jsonl", "parquet"),
    )


ENGINE_FACTORIES: dict[str, Callable[[], EngineRunner]] = {
    "shardloom": shardloom_runner,
    "shardloom-vortex": shardloom_vortex_runner,
    "shardloom-prepared-vortex": shardloom_prepared_vortex_runner,
    "shardloom-direct-transient": shardloom_direct_transient_runner,
    "pandas": pandas_runner,
    "polars": polars_runner,
    "duckdb": duckdb_runner,
    "spark-default": spark_default_runner,
    "spark-local-tuned": spark_local_tuned_runner,
    "datafusion": datafusion_runner,
    "dask": dask_runner,
}


def maybe_path_size(path: Path) -> int | None:
    return path.stat().st_size if path.exists() else None


def scenario_bytes(paths: DatasetPaths, scenario: str, data_format: str) -> int:
    if data_format == SHARDLOOM_VORTEX_FORMAT:
        return 0
    if scenario == "many-small-files scan":
        parts = fact_part_paths(paths, data_format)
        if parts:
            return sum(path.stat().st_size for path in parts)
    total = 0
    for name in SCENARIO_BYTES[scenario]:
        path = fact_path(paths, data_format) if name == "fact" else dim_path(paths, data_format)
        total += path.stat().st_size
    if (
        scenario == "small change over large base"
        and paths.cdc_delta_csv is not None
        and paths.cdc_delta_csv.exists()
    ):
        total += paths.cdc_delta_csv.stat().st_size
    return total


def source_state_path_text(path: Path) -> str:
    try:
        return path.resolve().relative_to(workspace_root()).as_posix()
    except ValueError:
        return path.as_posix()


def source_state_paths(paths: DatasetPaths, scenario: str, data_format: str) -> tuple[Path, ...]:
    if data_format == SHARDLOOM_VORTEX_FORMAT:
        return ()
    if scenario == "many-small-files scan":
        parts = fact_part_paths(paths, data_format)
        if parts:
            return parts
    source_paths: list[Path] = []
    for role in SCENARIO_BYTES.get(scenario, ("fact",)):
        source_paths.append(
            fact_path(paths, data_format) if role == "fact" else dim_path(paths, data_format)
        )
    if (
        scenario == "small change over large base"
        and paths.cdc_delta_csv is not None
        and paths.cdc_delta_csv.exists()
    ):
        source_paths.append(paths.cdc_delta_csv)
    return tuple(source_paths)


def source_state_compression(data_format: str) -> str:
    return {
        "csv": "none",
        "jsonl": "none",
        "parquet": "pyarrow_default",
        "arrow-ipc": "none",
        "avro": "fastavro_default",
        "orc": "pyarrow_default",
    }.get(data_format, "unsupported")


def source_state_reuse_reason(engine: str, evidence: dict[str, Any], status: str) -> str:
    if not is_shardloom_engine(engine):
        return "external baseline rows do not create or reuse ShardLoom SourceState"
    if status in {"unsupported", "unsupported_format", "execution_error"}:
        return "row did not execute; SourceState posture is deterministic but not reusable"
    if evidence.get("persistent_runner_status") == BATCH_RUNNER_STATUS:
        return str(
            evidence.get(
                "source_state_reuse_status",
                "prepared/native batch row did not report source-state reuse status",
            )
        )
    if engine == "shardloom-direct-transient":
        return "direct transient smoke reads one local compatibility source without reusable SourceState"
    return "one-shot compatibility import records SourceState identity but does not reuse it"


def source_state_contract_metadata(
    engine: str,
    paths: DatasetPaths,
    scenario: str,
    data_format: str,
    *,
    status: str,
    evidence: dict[str, Any] | None = None,
    source_parse_millis: float | None = None,
) -> dict[str, Any]:
    evidence = evidence or {}
    is_shardloom = is_shardloom_engine(engine)
    supported_format = data_format in FORMAT_ORDER
    source_paths = source_state_paths(paths, scenario, data_format) if supported_format else ()
    source_locations = [source_state_path_text(path) for path in source_paths]
    source_sizes = [path.stat().st_size for path in source_paths if path.exists()]
    byte_size = sum(source_sizes) if source_sizes else 0
    schema_digest = canonical_digest(
        {
            "dataset_profile": paths.dataset_profile,
            "dim_rows": paths.dim_rows,
            "fact_extra_columns": paths.fact_extra_columns,
            "format": data_format,
            "roles": SCENARIO_BYTES.get(scenario, ("fact",)),
            "rows": paths.rows,
        }
    )
    source_state_digest = canonical_digest(
        {
            "byte_size": byte_size,
            "file_count": len(source_locations),
            "locations": source_locations,
            "schema_digest": schema_digest,
            "source_format": data_format,
        }
    )
    parse_decode_plan_digest = canonical_digest(
        {
            "format": data_format,
            "roles": SCENARIO_BYTES.get(scenario, ("fact",)),
            "scenario": scenario,
            "source_state_schema": SOURCE_STATE_CONTRACT_SCHEMA_VERSION,
        }
    )
    batch_row = evidence.get("persistent_runner_status") == BATCH_RUNNER_STATUS
    batch_reuse_allowed = is_shardloom and batch_row and status == "success"
    source_state_reused = parse_optional_bool(evidence.get("source_state_reused")) is True
    if not is_shardloom:
        source_state_status = "external_baseline_only"
    elif not supported_format:
        source_state_status = "unsupported"
    elif status in {"unsupported", "unsupported_format"}:
        source_state_status = "unsupported"
    elif status == "execution_error":
        source_state_status = "blocked"
    elif batch_reuse_allowed:
        source_state_status = "source_state_reuse_supported"
    elif engine == "shardloom-direct-transient":
        source_state_status = "not_needed"
    else:
        source_state_status = "report_only"
    source_state_id = (
        f"source-state:{paths.dataset_profile}:{data_format}:{source_state_digest[:12]}"
        if is_shardloom
        else "none"
    )
    return {
        "source_state_contract_schema_version": SOURCE_STATE_CONTRACT_SCHEMA_VERSION,
        "source_state_status_vocabulary": ",".join(SOURCE_STATE_CONTRACT_STATUS_VOCABULARY),
        "source_state_status": source_state_status,
        "source_state_id": source_state_id,
        "source_state_digest": source_state_digest if is_shardloom else "none",
        "source_format": data_format,
        "source_location": ";".join(source_locations) if source_locations else "none",
        "source_fingerprint_kind": "local_file_size_schema_digest",
        "schema_digest": schema_digest,
        "row_count_known": supported_format,
        "file_count": len(source_locations),
        "byte_size": byte_size,
        "partition_columns": "event_date" if scenario in {"partition pruning", "null-heavy aggregate"} else "none",
        "compression": source_state_compression(data_format),
        "source_state_reuse_allowed": batch_reuse_allowed,
        "source_discovery_millis": None,
        "schema_inference_millis": None,
        "source_parse_millis": source_parse_millis,
        "parse_decode_plan_digest": parse_decode_plan_digest,
        "source_state_reuse_hit": source_state_reused,
        "source_state_reuse_reason": source_state_reuse_reason(engine, evidence, status),
        "source_state_materialization_decode_boundary_ref": MATERIALIZATION_POLICY_REF,
        "source_state_fallback_attempted": False,
        "source_state_external_engine_invoked": False,
        "source_state_claim_gate_status": "not_claim_grade",
        "source_state_claim_boundary": (
            "SourceState evidence covers local source discovery, schema identity, parse/decode "
            "planning, fingerprinting, and scoped reuse posture only; it is not output support, "
            "Vortex-native execution, performance, production, SQL/DataFrame, object-store/"
            "lakehouse, Foundry, package, release, or Spark-replacement evidence"
        ),
    }


def prepared_state_reuse_reason(
    engine: str,
    evidence: dict[str, Any],
    status: str,
    selected_mode: str | None,
    reuse_allowed: bool,
    reuse_hit: bool,
) -> str:
    if not is_shardloom_engine(engine):
        return "external baseline rows do not create or reuse ShardLoom VortexPreparedState"
    if status in {"unsupported", "unsupported_format", "execution_error"}:
        return "row did not execute; prepared-state posture is deterministic but not reusable"
    if engine == "shardloom-direct-transient":
        return "direct transient smoke does not create or reuse VortexPreparedState"
    if selected_mode == "compatibility_import_certified":
        return "compatibility import prepares Vortex inside the certified row but is not reusable prepared-state support"
    if reuse_hit:
        return "caller-supplied prepared Vortex artifacts were reused across prepared/native batch scenarios"
    if reuse_allowed:
        return (
            evidence.get("prepared_artifact_reuse_scope")
            or "prepared Vortex artifacts are reusable, but this row did not observe a reuse hit"
        )
    return "prepared-state reuse is report-only for this row"


def prepared_state_contract_metadata(
    engine: str,
    scenario: str,
    data_format: str,
    *,
    status: str,
    metrics: dict[str, Any],
    evidence: dict[str, Any] | None = None,
    selected_mode: str | None = None,
) -> dict[str, Any]:
    evidence = evidence or {}
    is_shardloom = is_shardloom_engine(engine)
    artifact_ref = first_meaningful_field(
        metrics.get("prepared_artifact_ref"),
        evidence.get("prepared_artifact_ref"),
        "none",
    )
    artifact_digest = first_meaningful_field(
        metrics.get("prepared_artifact_digest"),
        evidence.get("prepared_artifact_digest"),
        "none",
    )
    source_state_id = str(metrics.get("source_state_id", "none"))
    selected_mode = selected_mode or str(evidence.get("selected_execution_mode") or "unknown")
    prepared_mode = selected_mode in {"prepared_vortex", "native_vortex"}
    executed = status == "success"
    reuse_count = parse_optional_int(evidence.get("session_prepared_artifact_reuse_count"))
    reuse_hit = bool(reuse_count and reuse_count > 0)
    reuse_allowed = (
        is_shardloom
        and executed
        and prepared_mode
        and artifact_ref != "none"
        and artifact_digest != "none"
    )
    if not is_shardloom:
        prepared_state_status = "external_baseline_only"
    elif status in {"unsupported", "unsupported_format"}:
        prepared_state_status = "unsupported"
    elif status == "execution_error":
        prepared_state_status = "blocked"
    elif engine == "shardloom-direct-transient":
        prepared_state_status = "not_needed"
    elif reuse_allowed:
        prepared_state_status = "prepared_state_reuse_supported"
    else:
        prepared_state_status = "report_only"
    prepared_state_digest = canonical_digest(
        {
            "artifact_digest": artifact_digest,
            "artifact_ref": artifact_ref,
            "data_format": data_format,
            "scenario": scenario,
            "source_state_id": source_state_id,
            "vortex_prepared_state_schema": PREPARED_STATE_CONTRACT_SCHEMA_VERSION,
        }
    )
    prepared_state_id = (
        f"vortex-prepared-state:{data_format}:{prepared_state_digest[:12]}"
        if is_shardloom and artifact_ref != "none"
        else "none"
    )
    return {
        "prepared_state_contract_schema_version": PREPARED_STATE_CONTRACT_SCHEMA_VERSION,
        "prepared_state_status_vocabulary": ",".join(
            PREPARED_STATE_CONTRACT_STATUS_VOCABULARY
        ),
        "prepared_state_status": prepared_state_status,
        "prepared_state_id": prepared_state_id,
        "prepared_state_digest": prepared_state_digest if prepared_state_id != "none" else "none",
        "prepared_state_source_state_id": source_state_id,
        "vortex_artifact_ref": artifact_ref if is_shardloom else "none",
        "vortex_artifact_digest": artifact_digest if is_shardloom else "none",
        "layout_summary": (
            "local_vortex_artifact_layout_not_summarized"
            if prepared_state_id != "none"
            else "none"
        ),
        "encoding_summary": (
            evidence.get("compressed_kernel_registry_encoding_ids")
            or "encoding_summary_not_materialized_in_benchmark_row"
            if prepared_state_id != "none"
            else "none"
        ),
        "statistics_summary": (
            evidence.get("source_capability_statistics_availability")
            or "vortex_metadata_available"
            if prepared_state_id != "none"
            else "none"
        ),
        "prepared_state_reuse_allowed": reuse_allowed,
        "prepared_state_reuse_hit": reuse_hit,
        "prepared_state_reuse_reason": prepared_state_reuse_reason(
            engine, evidence, status, selected_mode, reuse_allowed, reuse_hit
        ),
        "preparation_included_in_timing": metrics.get(
            "preparation_included_in_timing", False
        ),
        "vortex_prepare_millis": metrics.get("preparation_millis"),
        "prepared_state_materialization_decode_boundary_ref": MATERIALIZATION_POLICY_REF,
        "prepared_state_native_io_certificate_status": first_meaningful_field(
            evidence.get("source_native_io_certificate_status"),
            evidence.get("native_io_certificate_status"),
            metrics.get("source_native_io_certificate_status"),
            "not_applicable",
        ),
        "prepared_state_fallback_attempted": False,
        "prepared_state_external_engine_invoked": False,
        "prepared_state_claim_gate_status": "not_claim_grade",
        "prepared_state_claim_boundary": (
            "VortexPreparedState evidence covers scoped local prepared Vortex artifact identity, "
            "digest, preparation timing separation, and reuse posture only; it is not encoded-native "
            "operator coverage, output support, performance, production, SQL/DataFrame, object-store/"
            "lakehouse, Foundry, package, release, or Spark-replacement evidence"
        ),
    }


def output_plan_reuse_reason(
    engine: str,
    status: str,
    output_plan_status: str,
    output_written: bool,
    output_plan_reuse_hit: bool,
) -> str:
    if not is_shardloom_engine(engine):
        return "external baseline rows do not create or reuse ShardLoom OutputPlan"
    if status in {"unsupported", "unsupported_format", "execution_error"}:
        return "row did not execute; output-plan posture is deterministic but not reusable"
    if output_plan_reuse_hit:
        return "output plan was reused by the benchmark row"
    if output_written:
        return "local Vortex result-sink output plan executed, but no output-plan reuse hit was observed"
    if output_plan_status == "not_needed":
        return "no output sink was requested for this row"
    return "output-plan reuse is report-only for this row"


def output_plan_contract_metadata(
    engine: str,
    scenario: str,
    data_format: str,
    *,
    status: str,
    metrics: dict[str, Any],
    evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    evidence = evidence or {}
    is_shardloom = is_shardloom_engine(engine)
    output_written = parse_optional_bool(
        evidence.get("computed_result_sink_written")
    ) is True or metrics.get("result_sink_included") is True
    result_replay_verified = (
        parse_optional_bool(evidence.get("computed_result_sink_replay_verified")) is True
    )
    sink_artifact_ref = first_meaningful_field(
        evidence.get("computed_result_vortex_path"),
        "none",
    )
    sink_artifact_digest = first_meaningful_field(
        evidence.get("computed_result_vortex_digest"),
        "none",
    )
    schema_summary = first_meaningful_field(
        evidence.get("computed_result_sink_schema_summary"),
        "computed_result_schema_not_emitted",
    )
    output_format = "vortex" if output_written else "not_requested"
    output_location = sink_artifact_ref if output_written else "not_requested"
    output_schema_digest = (
        canonical_digest(schema_summary) if output_written else "none"
    )
    output_plan_reuse_hit = False
    if not is_shardloom:
        output_plan_status = "external_baseline_only"
    elif status in {"unsupported", "unsupported_format"}:
        output_plan_status = "unsupported"
    elif status == "execution_error":
        output_plan_status = "blocked"
    elif output_written:
        output_plan_status = "output_plan_supported"
    else:
        output_plan_status = "not_needed"
    output_plan_digest = canonical_digest(
        {
            "data_format": data_format,
            "output_format": output_format,
            "output_location": output_location,
            "output_plan_schema": OUTPUT_PLAN_CONTRACT_SCHEMA_VERSION,
            "scenario": scenario,
            "schema_digest": output_schema_digest,
            "write_mode": "local_vortex_result_sink" if output_written else "none",
        }
    )
    output_plan_id = (
        f"output-plan:{output_format}:{output_plan_digest[:12]}"
        if is_shardloom and output_written
        else "none"
    )
    return {
        "output_plan_contract_schema_version": OUTPUT_PLAN_CONTRACT_SCHEMA_VERSION,
        "output_plan_status_vocabulary": ",".join(
            OUTPUT_PLAN_CONTRACT_STATUS_VOCABULARY
        ),
        "output_plan_status": output_plan_status,
        "output_plan_id": output_plan_id,
        "output_plan_digest": output_plan_digest if output_plan_id != "none" else "none",
        "output_format": output_format if is_shardloom else "none",
        "output_location": output_location if is_shardloom else "none",
        "output_schema_digest": output_schema_digest,
        "output_partitioning": "none",
        "output_compression": "vortex_default" if output_written else "none",
        "output_encoding": "vortex_native" if output_written else "none",
        "output_write_mode": (
            "local_vortex_result_sink" if output_written else "none"
        ),
        "output_plan_reuse_allowed": False,
        "output_metadata_preservation_status": (
            "native_vortex_highest_fidelity" if output_written else "not_applicable"
        ),
        "output_materialization_required": (
            "result_sink_materializes_computed_result" if output_written else "not_applicable"
        ),
        "output_plan_reuse_hit": output_plan_reuse_hit,
        "output_plan_reuse_reason": output_plan_reuse_reason(
            engine, status, output_plan_status, output_written, output_plan_reuse_hit
        ),
        "output_plan_millis": None,
        "output_write_millis": metrics.get("result_sink_write_millis"),
        "result_replay_verified": result_replay_verified,
        "output_native_io_certificate_status": first_meaningful_field(
            evidence.get("computed_result_sink_native_io_certificate_status"),
            "not_applicable" if not output_written else "missing",
        ),
        "sink_artifact_ref": sink_artifact_ref if output_written else "none",
        "sink_artifact_digest": sink_artifact_digest if output_written else "none",
        "output_plan_fallback_attempted": False,
        "output_plan_external_engine_invoked": False,
        "output_plan_claim_gate_status": "not_claim_grade",
        "output_plan_claim_boundary": (
            "OutputPlan evidence covers scoped local Vortex result-sink planning, metadata "
            "preservation posture, write/replay refs, and no-fallback policy only; it is not "
            "cross-format fanout, object-store/lakehouse/table commit, production, performance, "
            "SQL/DataFrame, Foundry, package, release, or Spark-replacement evidence"
        ),
    }


def cache_invalidation_status_reason(
    engine: str,
    status: str,
    cache_status: str,
    source_path_count: int,
) -> str:
    if not is_shardloom_engine(engine):
        return "external baseline rows do not create or validate ShardLoom reusable state"
    if status in {"unsupported", "unsupported_format"}:
        return "unsupported row cannot validate reusable state"
    if status == "execution_error":
        return "execution error blocks reusable state validation"
    if cache_status == "not_needed":
        return "no reusable source/prepared/output state is needed for this row"
    if source_path_count == 0:
        return (
            "source file fingerprint is not needed for this row; plan, prepared, and "
            "output fingerprints remain explicit"
        )
    return (
        "current row fingerprints are internally consistent; this is reuse eligibility "
        "evidence, not a cache hit or performance claim"
    )


def cache_invalidation_contract_metadata(
    engine: str,
    paths: DatasetPaths,
    scenario: str,
    data_format: str,
    *,
    status: str,
    metrics: dict[str, Any],
    evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    evidence = evidence or {}
    is_shardloom = is_shardloom_engine(engine)
    supported_format = data_format in FORMAT_ORDER
    source_paths = source_state_paths(paths, scenario, data_format) if supported_format else ()
    existing_sources = [path for path in source_paths if path.exists()]
    source_sizes = [path.stat().st_size for path in existing_sources]
    source_size = sum(source_sizes)
    source_mtimes = [path.stat().st_mtime_ns for path in existing_sources]
    source_mtime: int | str = max(source_mtimes) if source_mtimes else "not_applicable"
    source_content_digest = canonical_digest(
        {
            "fingerprint_kind": "local_file_size_mtime_schema_plan_digest",
            "schema_digest": metrics.get("schema_digest", "none"),
            "source_locations": [
                source_state_path_text(path) for path in existing_sources
            ],
            "source_mtime": source_mtime,
            "source_size": source_size,
        }
    )
    plan_digest = canonical_digest(
        {
            "data_format": data_format,
            "execution_mode": evidence.get("selected_execution_mode")
            or metrics.get("selected_execution_mode")
            or "unknown",
            "output_plan_digest": metrics.get("output_plan_digest", "none"),
            "parse_decode_plan_digest": metrics.get("parse_decode_plan_digest", "none"),
            "prepared_state_digest": metrics.get("prepared_state_digest", "none"),
            "scenario": scenario,
            "schema_digest": metrics.get("schema_digest", "none"),
        }
    )
    if not is_shardloom:
        cache_status = "external_baseline_only"
        cache_valid = False
    elif status in {"unsupported", "unsupported_format"}:
        cache_status = "unsupported"
        cache_valid = False
    elif status == "execution_error":
        cache_status = "blocked"
        cache_valid = False
    elif not existing_sources and metrics.get("source_state_status") == "not_needed":
        cache_status = "not_needed"
        cache_valid = True
    elif status == "success":
        cache_status = "cache_valid"
        cache_valid = True
    else:
        cache_status = "report_only"
        cache_valid = False
    return {
        "cache_invalidation_contract_schema_version": CACHE_INVALIDATION_SCHEMA_VERSION,
        "cache_invalidation_status_vocabulary": ",".join(
            CACHE_INVALIDATION_STATUS_VOCABULARY
        ),
        "cache_invalidation_status": cache_status,
        "cache_invalidation_layer_scope": (
            "SourceState,VortexPreparedState,ExecutionPlan,OutputPlan,SinkArtifact"
        ),
        "source_fingerprint_kind": "local_file_size_mtime_schema_plan_digest",
        "source_content_digest": source_content_digest if is_shardloom else "none",
        "source_mtime": source_mtime if is_shardloom else "not_applicable",
        "source_size": source_size if is_shardloom else 0,
        "object_etag": "not_applicable_local_filesystem",
        "manifest_version": CACHE_INVALIDATION_SCHEMA_VERSION,
        "schema_digest": metrics.get("schema_digest", "none"),
        "plan_digest": plan_digest if is_shardloom else "none",
        "output_plan_digest": metrics.get("output_plan_digest", "none"),
        "cache_valid": cache_valid,
        "invalidation_reason": cache_invalidation_status_reason(
            engine, status, cache_status, len(existing_sources)
        ),
        "cache_invalidation_fallback_attempted": False,
        "cache_invalidation_external_engine_invoked": False,
        "cache_invalidation_claim_gate_status": "not_claim_grade",
        "cache_invalidation_redaction_status": (
            "no_credentials_or_tokens_in_fingerprint_fields"
        ),
        "cache_invalidation_claim_boundary": (
            "Cache invalidation evidence covers current-row local fingerprint and reuse "
            "eligibility posture only; it is not a persistent cache, cache hit, performance, "
            "production, object-store/lakehouse, Foundry, package, release, or "
            "Spark-replacement claim"
        ),
    }


def reuse_layer_status_from_contract(
    *,
    is_shardloom: bool,
    row_status: str,
    layer_status: str | None,
    allowed: bool,
    hit: bool,
    not_needed: bool = False,
    invalidated: bool = False,
) -> tuple[str, bool, bool]:
    if not is_shardloom:
        return "report_only", False, False
    if invalidated:
        return "invalidated", False, False
    if row_status == "execution_error" or layer_status == "blocked":
        return "blocked", False, False
    if row_status in {"unsupported", "unsupported_format"} or layer_status == "unsupported":
        return "unsupported", False, False
    if not_needed or layer_status == "not_needed":
        return "not_needed", False, False
    if hit:
        return "reuse_hit", True, True
    if allowed:
        return "reuse_miss", True, False
    return "report_only", False, False


def reuse_level_rows_from_metrics(
    *,
    scenario: str,
    engine: str,
    row_status: str,
    metrics: dict[str, Any],
) -> list[dict[str, Any]]:
    is_shardloom = is_shardloom_engine(engine)
    execution_mode = first_meaningful_field(
        metrics.get("selected_execution_mode"),
        metrics.get("execution_mode"),
        "external_baseline_only" if not is_shardloom else "unknown",
    )
    evidence_level = first_meaningful_field(
        metrics.get("evidence_level"),
        "not_applicable" if not is_shardloom else "unknown",
    )
    output_format = first_meaningful_field(metrics.get("output_format"), "none")
    cache_invalidated = metrics.get("cache_invalidation_status") == "invalidated"
    invalidation_reason = str(metrics.get("invalidation_reason", "none"))
    source_state_allowed = metrics.get("source_state_reuse_allowed") is True
    source_state_hit = metrics.get("source_state_reuse_hit") is True
    prepared_state_allowed = metrics.get("prepared_state_reuse_allowed") is True
    prepared_state_hit = metrics.get("prepared_state_reuse_hit") is True
    output_plan_allowed = metrics.get("output_plan_reuse_allowed") is True
    output_plan_hit = metrics.get("output_plan_reuse_hit") is True
    batch_row = metrics.get("persistent_runner_status") == BATCH_RUNNER_STATUS
    output_plan_status = str(metrics.get("output_plan_status", "report_only"))
    output_written = output_format not in {"none", "not_requested"}
    result_replay_verified = metrics.get("result_replay_verified") is True

    level_specs = (
        (
            "discovery_reuse",
            metrics.get("source_state_status"),
            source_state_allowed,
            source_state_hit,
            metrics.get("source_state_status") == "not_needed",
            metrics.get("source_state_digest", "none"),
            metrics.get("source_state_reuse_reason", "none"),
        ),
        (
            "schema_reuse",
            metrics.get("source_state_status"),
            source_state_allowed,
            source_state_hit,
            metrics.get("source_state_status") == "not_needed",
            metrics.get("schema_digest", "none"),
            metrics.get("source_state_reuse_reason", "none"),
        ),
        (
            "parse_plan_reuse",
            metrics.get("source_state_status"),
            source_state_allowed,
            source_state_hit,
            metrics.get("source_state_status") == "not_needed",
            metrics.get("parse_decode_plan_digest", "none"),
            metrics.get("source_state_reuse_reason", "none"),
        ),
        (
            "prepared_vortex_reuse",
            metrics.get("prepared_state_status"),
            prepared_state_allowed,
            prepared_state_hit,
            metrics.get("prepared_state_status") == "not_needed",
            metrics.get("prepared_state_digest", "none"),
            metrics.get("prepared_state_reuse_reason", "none"),
        ),
        (
            "operator_source_state_reuse",
            metrics.get("source_state_status"),
            batch_row and row_status == "success",
            source_state_hit,
            metrics.get("source_state_status") == "not_needed",
            metrics.get("source_state_digest", "none"),
            metrics.get("source_state_reuse_status")
            or metrics.get("source_state_reuse_reason", "none"),
        ),
        (
            "output_plan_reuse",
            output_plan_status,
            output_plan_allowed,
            output_plan_hit,
            output_plan_status == "not_needed",
            metrics.get("output_plan_digest", "none"),
            metrics.get("output_plan_reuse_reason", "none"),
        ),
        (
            "result_replay_reuse",
            output_plan_status,
            False,
            False,
            not output_written,
            metrics.get("sink_artifact_digest", "none"),
            "result replay was verified but replay reuse is not implemented"
            if result_replay_verified
            else "result replay reuse is not implemented for this row",
        ),
    )
    rows: list[dict[str, Any]] = []
    for reuse_level, layer_status, allowed, hit, not_needed, digest_ref, blocker in level_specs:
        reuse_status, reuse_allowed, reuse_hit = reuse_layer_status_from_contract(
            is_shardloom=is_shardloom,
            row_status=row_status,
            layer_status=str(layer_status or "report_only"),
            allowed=bool(allowed),
            hit=bool(hit),
            not_needed=bool(not_needed),
            invalidated=cache_invalidated,
        )
        reuse_digest = canonical_digest(
            {
                "digest_ref": digest_ref,
                "evidence_level": evidence_level,
                "execution_mode": execution_mode,
                "output_format": output_format,
                "reuse_level": reuse_level,
                "reuse_schema": REUSE_LEVEL_SCHEMA_VERSION,
                "reuse_status": reuse_status,
                "scenario": scenario,
            }
        )
        rows.append(
            {
                "scenario": scenario,
                "engine": engine,
                "execution_mode": execution_mode,
                "evidence_level": evidence_level,
                "output_format": output_format,
                "reuse_level": reuse_level,
                "reuse_status": reuse_status,
                "reuse_hit": reuse_hit,
                "reuse_digest": reuse_digest if is_shardloom else "none",
                "reuse_allowed": reuse_allowed,
                "reuse_blocker": str(blocker or "none"),
                "layer_invalidation_reason": invalidation_reason,
                "claim_gate_status": "not_claim_grade",
                "claim_grade_requirements_met": False,
                "fallback_attempted": False,
                "external_engine_invoked": False,
            }
        )
    return rows


def reuse_level_contract_metadata(
    *,
    scenario: str,
    engine: str,
    status: str,
    metrics: dict[str, Any],
) -> dict[str, Any]:
    rows = reuse_level_rows_from_metrics(
        scenario=scenario,
        engine=engine,
        row_status=status,
        metrics=metrics,
    )
    return {
        "reuse_level_contract_schema_version": REUSE_LEVEL_SCHEMA_VERSION,
        "reuse_level_status_vocabulary": ",".join(REUSE_LEVEL_STATUS_VOCABULARY),
        "reuse_level_supported_levels": ",".join(REUSE_LEVEL_SUPPORTED_LEVELS),
        "reuse_level_matrix_ref": "artifact.reuse_level_matrix",
        "reuse_level_summary_digest": canonical_digest(rows),
        "reuse_level_hit_count": sum(1 for row in rows if row["reuse_hit"] is True),
        "reuse_level_allowed_count": sum(
            1 for row in rows if row["reuse_allowed"] is True
        ),
        "reuse_level_claim_gate_status": "not_claim_grade",
        "claim_grade_requirements_met": False,
        "reuse_level_fallback_attempted": False,
        "reuse_level_external_engine_invoked": False,
        "reuse_level_claim_boundary": (
            "Evidence-safe reuse levels expose layer-specific reuse posture only; reuse hits, "
            "misses, blockers, and invalidation reasons do not prove correctness, output "
            "fidelity, performance, production readiness, SQL/DataFrame support, object-store/"
            "lakehouse support, Foundry support, package readiness, release readiness, or "
            "Spark-replacement status"
        ),
    }


def tool_version_text(binary_name: str, args: tuple[str, ...] = ("--version",)) -> str:
    binary = shutil.which(binary_name)
    if binary is None:
        return "missing"
    env = os.environ.copy()
    env["RUSTUP_TOOLCHAIN"] = env.get("RUSTUP_TOOLCHAIN", "1.91.1")
    completed = subprocess_run([binary, *args], workspace_root(), env)
    if completed["returncode"] != 0:
        return "unavailable"
    return completed["stdout"].strip().splitlines()[0] or "unknown"


def rustc_target_triple() -> str:
    rustc = shutil.which("rustc")
    if rustc is None:
        return "missing"
    env = os.environ.copy()
    env["RUSTUP_TOOLCHAIN"] = env.get("RUSTUP_TOOLCHAIN", "1.91.1")
    completed = subprocess_run([rustc, "-vV"], workspace_root(), env)
    if completed["returncode"] != 0:
        return "unavailable"
    for line in completed["stdout"].splitlines():
        if line.startswith("host:"):
            return line.split(":", 1)[1].strip()
    return "unknown"


def pgo_training_workload_ref(profile: str) -> str:
    if profile != "release-pgo":
        return "not_applicable"
    return (
        "benchmarks/traditional_analytics/run.py --engines shardloom-prepared-vortex "
        "--formats csv,parquet --include-taxonomy-extra --rows 10000 --iterations 1"
    )


def build_profile_template(profile: str) -> dict[str, Any]:
    if profile == "debug":
        return {
            "build_profile_kind": "development",
            "target_cpu_policy": "portable_default",
            "target_cpu_native_enabled": False,
            "lto_enabled": False,
            "lto_mode": "none",
            "codegen_units": "cargo_default_debug",
            "pgo_status": "not_configured",
            "pgo_profile_generate_status": "not_requested",
            "pgo_profile_use_status": "not_requested",
            "pgo_profile_artifact_ref": "none",
            "build_reproducibility_status": "development_not_release_evidence",
            "portable_release_artifact": False,
            "benchmark_only_build": False,
        }
    if profile == "release":
        return {
            "build_profile_kind": "portable_release_baseline",
            "target_cpu_policy": "portable_default",
            "target_cpu_native_enabled": False,
            "lto_enabled": False,
            "lto_mode": "none",
            "codegen_units": "cargo_default_release",
            "pgo_status": "not_configured",
            "pgo_profile_generate_status": "not_requested",
            "pgo_profile_use_status": "not_requested",
            "pgo_profile_artifact_ref": "none",
            "build_reproducibility_status": "portable_baseline",
            "portable_release_artifact": True,
            "benchmark_only_build": False,
        }
    if profile == "release-lto":
        return {
            "build_profile_kind": "portable_lto",
            "target_cpu_policy": "portable_default",
            "target_cpu_native_enabled": False,
            "lto_enabled": True,
            "lto_mode": "thin",
            "codegen_units": 1,
            "pgo_status": "not_configured",
            "pgo_profile_generate_status": "not_requested",
            "pgo_profile_use_status": "not_requested",
            "pgo_profile_artifact_ref": "none",
            "build_reproducibility_status": "portable_optimized_local_artifact",
            "portable_release_artifact": True,
            "benchmark_only_build": False,
        }
    if profile == "release-pgo":
        profile_ref = os.environ.get("SHARDLOOM_PGO_PROFILE", "")
        pgo_use_status = "profile_use_artifact_configured" if profile_ref else "not_configured"
        return {
            "build_profile_kind": "pgo_report_only",
            "target_cpu_policy": "portable_default",
            "target_cpu_native_enabled": False,
            "lto_enabled": True,
            "lto_mode": "thin",
            "codegen_units": 1,
            "pgo_status": (
                "profile_use_artifact_configured"
                if profile_ref
                else "report_only_missing_profile_use_artifact"
            ),
            "pgo_profile_generate_status": "documented_not_run_by_harness",
            "pgo_profile_use_status": pgo_use_status,
            "pgo_profile_artifact_ref": profile_ref or "none",
            "build_reproducibility_status": "pgo_requires_recorded_training_workload",
            "portable_release_artifact": False,
            "benchmark_only_build": True,
        }
    if profile == "release-native-benchmark":
        return {
            "build_profile_kind": "host_native_benchmark_only",
            "target_cpu_policy": "native_benchmark_only",
            "target_cpu_native_enabled": True,
            "lto_enabled": True,
            "lto_mode": "thin",
            "codegen_units": 1,
            "pgo_status": "not_configured",
            "pgo_profile_generate_status": "not_requested",
            "pgo_profile_use_status": "not_requested",
            "pgo_profile_artifact_ref": "none",
            "build_reproducibility_status": "host_native_nonportable_benchmark_only",
            "portable_release_artifact": False,
            "benchmark_only_build": True,
        }
    return {
        "build_profile_kind": "external_baseline_only",
        "target_cpu_policy": "external_baseline_only",
        "target_cpu_native_enabled": False,
        "lto_enabled": False,
        "lto_mode": "external_baseline_only",
        "codegen_units": "external_baseline_only",
        "pgo_status": "external_baseline_only",
        "pgo_profile_generate_status": "external_baseline_only",
        "pgo_profile_use_status": "external_baseline_only",
        "pgo_profile_artifact_ref": "none",
        "build_reproducibility_status": "external_baseline_only",
        "portable_release_artifact": False,
        "benchmark_only_build": False,
    }


def build_profile_toolchain_evidence(profile: str) -> dict[str, Any]:
    cached = SHARDLOOM_BUILD_PROFILE_EVIDENCE_CACHE.get(profile)
    if cached is not None:
        return dict(cached)
    evidence = {
        "build_profile_schema_version": BUILD_PROFILE_SCHEMA_VERSION,
        "build_profile": profile,
        "rustc_version": tool_version_text("rustc"),
        "cargo_version": tool_version_text("cargo"),
        "target_triple": rustc_target_triple(),
        **build_profile_template(profile),
    }
    training_ref = pgo_training_workload_ref(profile)
    evidence["pgo_training_workload_ref"] = training_ref
    evidence["pgo_training_workload_digest"] = (
        canonical_digest(training_ref) if profile == "release-pgo" else "not_applicable"
    )
    SHARDLOOM_BUILD_PROFILE_EVIDENCE_CACHE[profile] = dict(evidence)
    return evidence


def build_profile_contract_metadata(
    engine: str, correctness_digest: str | None = None
) -> dict[str, Any]:
    profile = SHARDLOOM_BUILD_PROFILE if is_shardloom_engine(engine) else "external_baseline_only"
    evidence = build_profile_toolchain_evidence(profile)
    evidence.update(
        {
            "build_profile_correctness_digest": correctness_digest or "not_available",
            "build_profile_fallback_attempted": False,
            "build_profile_external_engine_invoked": False,
            "build_profile_claim_gate_status": (
                "not_claim_grade"
                if is_shardloom_engine(engine)
                else "external_baseline_only"
            ),
            "build_profile_claim_boundary": (
                "Build-profile evidence records compiler/configuration posture only; it does "
                "not create performance, superiority, Spark-replacement, production, package, "
                "release, SQL/DataFrame, object-store/lakehouse, or Foundry claims. "
                "release-native-benchmark is host-native and benchmark-only."
            ),
        }
    )
    return evidence


def optimizer_trace_contract_metadata(engine: str) -> dict[str, Any]:
    is_shardloom = is_shardloom_engine(engine)
    return {
        "optimizer_trace_schema_version": OPTIMIZER_TRACE_SCHEMA_VERSION,
        "optimizer_trace_id": (
            "optimizer_trace.gar_perf_2b.report_only_registry"
            if is_shardloom
            else "not_applicable_external_baseline"
        ),
        "optimizer_registry_version": "gar-perf-2b.optimizer_registry.v1",
        "optimizer_phase": "logical" if is_shardloom else "external_baseline_only",
        "optimizer_rule_status_vocabulary": (
            "admitted,applied,blocked,unsupported,not_applicable,report_only"
        ),
        "optimizer_rule_order": (
            "predicate_pushdown,projection_pushdown,slice_limit_pushdown,"
            "common_subplan_source_state_reuse,expression_simplification,"
            "constant_folding,type_coercion,join_ordering,cardinality_estimation"
        ),
        "optimizer_rule_statuses": (
            "predicate_pushdown=report_only;projection_pushdown=report_only;"
            "slice_limit_pushdown=blocked;common_subplan_source_state_reuse=admitted;"
            "expression_simplification=unsupported;constant_folding=unsupported;"
            "type_coercion=blocked;join_ordering=blocked;cardinality_estimation=not_applicable"
            if is_shardloom
            else "external_baseline_only"
        ),
        "optimizer_rule_admitted_count": 1 if is_shardloom else 0,
        "optimizer_rule_applied_count": 0,
        "optimizer_rule_blocked_count": 3 if is_shardloom else 0,
        "optimizer_rule_unsupported_count": 2 if is_shardloom else 0,
        "optimizer_rule_not_applicable_count": 1 if is_shardloom else 0,
        "optimizer_rule_report_only_count": 2 if is_shardloom else 0,
        "optimizer_before_plan_digest_status": "not_emitted_report_only",
        "optimizer_after_plan_digest_status": "not_emitted_report_only",
        "optimizer_rewrite_safety_status": "report_only_no_rewrite",
        "optimizer_evidence_preserved": True,
        "optimizer_no_fallback_preserved": True,
        "optimizer_claim_boundary_preserved": True,
        "optimizer_materialization_boundary_preserved": True,
        "optimizer_source_state_reuse_admitted": is_shardloom,
        "optimizer_cardinality_estimation_status": (
            "not_applicable_no_input_plan" if is_shardloom else "external_baseline_only"
        ),
        "optimizer_correctness_smoke_ref": "not_required_no_rewrite_applied",
        "optimizer_fallback_attempted": False,
        "optimizer_external_engine_invoked": False,
        "optimizer_claim_gate_status": "not_claim_grade"
        if is_shardloom
        else "external_baseline_only",
        "optimizer_benchmark_trace_ref": (
            "optimizer-trace://gar-perf-2b.report-only-registry"
            if is_shardloom
            else "none"
        ),
        "optimizer_claim_boundary": (
            "Optimizer trace fields classify report-only rewrite posture and source-state reuse "
            "admission only; no optimizer rewrite was applied, no performance claim is allowed, "
            "and this is not lazy optimizer parity, SQL/DataFrame runtime, object-store/lakehouse "
            "runtime, Foundry support, production readiness, package readiness, or Spark "
            "replacement evidence"
        ),
    }


def rows_scanned(paths: DatasetPaths, scenario: str) -> int:
    if scenario in {
        "hash join",
        "join + aggregate",
        "scale stress skewed join aggregation",
        "scale stress multi-stage etl",
    }:
        return paths.rows + paths.dim_rows
    if (
        scenario == "small change over large base"
        and paths.cdc_delta_csv is not None
        and paths.cdc_delta_csv.exists()
    ):
        return paths.rows + cdc_delta_row_count(paths.cdc_delta_csv)
    return paths.rows


def cdc_delta_row_count(path: Path) -> int:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return max(0, sum(1 for _ in handle) - 1)


def rows_materialized(value: Any) -> int:
    if isinstance(value, list):
        return len(value)
    if isinstance(value, dict):
        return int(value.get("row_count", 1))
    return 1


def materialization_policy(engine: str, data_format: str) -> str:
    if engine == "shardloom-direct-transient":
        return "direct_transient_local_csv_no_vortex_persistence"
    if engine in ("shardloom-vortex", "shardloom-prepared-vortex"):
        return "prepared_vortex_input_before_scenario_timing"
    if data_format == SHARDLOOM_VORTEX_FORMAT:
        return "native_vortex_input"
    if engine == "shardloom":
        return "compatibility_source_to_local_vortex_import_included"
    return "engine_local_compatibility_reader_policy"


def native_vortex_or_compatibility_import(engine: str, data_format: str) -> str:
    if engine == "shardloom-direct-transient":
        return "direct_compatibility_transient_no_vortex_persistence"
    if engine in ("shardloom-vortex", "shardloom-prepared-vortex"):
        return "prepared_vortex"
    if data_format == SHARDLOOM_VORTEX_FORMAT:
        return "native_vortex"
    if engine == "shardloom":
        return "compatibility_import_to_vortex"
    return "compatibility_format_baseline"


def shardloom_claim_grade_missing_evidence(result: dict[str, Any]) -> list[str]:
    evidence = result.get("shardloom_evidence", {})
    missing: list[str] = []
    for field, expected in SHARDLOOM_CLAIM_GRADE_REQUIRED_EVIDENCE:
        actual = str(evidence.get(field, "missing")).lower()
        if actual != expected:
            missing.append(f"{field}!={expected} (actual={actual})")
    if not evidence.get("benchmark_row_ref"):
        missing.append("benchmark_row_ref missing")
    if not evidence.get("coverage_row_ref"):
        missing.append("coverage_row_ref missing")
    if result.get("fallback_attempted", False):
        missing.append("result fallback_attempted was true")
    if result.get("iterations", 0) < MIN_CLAIM_GRADE_ITERATIONS:
        missing.append(
            f"iterations<{MIN_CLAIM_GRADE_ITERATIONS} "
            f"(actual={result.get('iterations', 'missing')})"
        )
    if result.get("correctness_digest_stable") is not True:
        missing.append("correctness_digest_stable!=true")
    if result["metrics"].get("query_runtime_millis") is None:
        missing.append("timing row missing")
    return missing


def reproducible_benchmark_row(result: dict[str, Any]) -> bool:
    return (
        result["status"] == "success"
        and result.get("iterations", 0) >= MIN_CLAIM_GRADE_ITERATIONS
        and result.get("correctness_digest_stable") is True
        and result["metrics"].get("query_runtime_millis") is not None
    )


def claim_grade_readiness(result: dict[str, Any]) -> dict[str, Any]:
    engine = result["engine"]
    status = result["status"]
    if status != "success":
        status_classification = (
            "unsupported"
            if status in ("unsupported", "unsupported_format")
            else "blocked"
        )
        return {
            "claim_gate_status": status_classification,
            "claim_grade_requirements_met": False,
            "claim_grade_missing_evidence": [result.get("reason", status)],
        }
    if not is_shardloom_engine(engine):
        return {
            "claim_gate_status": "external_baseline_only",
            "claim_grade_requirements_met": False,
            "claim_grade_missing_evidence": [
                "external baseline rows are comparison-only"
            ],
        }
    if engine == "shardloom-direct-transient":
        return {
            "claim_gate_status": "fixture_smoke_only",
            "claim_grade_requirements_met": False,
            "claim_grade_missing_evidence": [
                "direct transient local CSV smoke is scoped and not Vortex-native"
            ],
        }
    if engine in ("shardloom-vortex", "shardloom-prepared-vortex"):
        return {
            "claim_gate_status": "fixture_smoke_only",
            "claim_grade_requirements_met": False,
            "claim_grade_missing_evidence": [
                "native Vortex lane lacks workload scorecard/result-sink replay evidence"
            ],
        }
    missing = shardloom_claim_grade_missing_evidence(result)
    claim_grade = not missing
    return {
        "claim_gate_status": "claim_grade" if claim_grade else "not_claim_grade",
        "claim_grade_requirements_met": claim_grade,
        "claim_grade_missing_evidence": missing,
    }


def benchmark_constitution(
    result: dict[str, Any],
    cache_mode: str,
    dataset_profile: str,
) -> dict[str, Any]:
    metadata = scenario_metadata(result["scenario_base"])
    engine = result["engine"]
    data_format = result["storage_format"]
    return {
        "constitution_id": (
            f"{metadata.scenario_id}:{engine}:{data_format}:{dataset_profile}"
        ),
        "scenario_id": metadata.scenario_id,
        "scenario_category": metadata.category,
        "dataset_profile": dataset_profile,
        "engine_role": engine_role(engine),
        "input_format": data_format,
        "table_format": "none",
        "storage_mode": "local_filesystem",
        "native_vortex_or_compatibility_import": native_vortex_or_compatibility_import(
            engine, data_format
        ),
        "startup_included": False,
        "conversion_included": engine == "shardloom" and data_format != SHARDLOOM_VORTEX_FORMAT,
        "staging_included": engine.startswith("shardloom")
        and engine != "shardloom-direct-transient",
        "result_delivery_included": True,
        "write_included": engine == "shardloom" and SHARDLOOM_RESULT_SINK,
        "cache_mode": cache_mode,
        "iterations": result["iterations"],
        "reproducibility_min_iterations": MIN_CLAIM_GRADE_ITERATIONS,
        "correctness_digest_stable": result.get("correctness_digest_stable"),
        "warmup_policy": "engine startup/warmup recorded separately",
        "correctness_oracle": "first successful digest per formatted scenario",
        "materialization_policy": materialization_policy(engine, data_format),
        "requested_execution_mode": result.get("requested_execution_mode"),
        "selected_execution_mode": result.get("selected_execution_mode"),
        "execution_mode_family": result.get("execution_mode_family"),
        "compatibility_import_included": result.get("compatibility_import_included"),
        "vortex_prepare_included": result.get("vortex_prepare_included"),
        "vortex_write_reopen_included": result.get("vortex_write_reopen_included"),
        "direct_transient_execution": result.get("direct_transient_execution"),
        "resource_policy": "engine defaults; ShardLoom auto sizing recorded in evidence",
        "claim_level": result.get("claim_gate_status", "not_claim_grade"),
    }


def validate_result_attribution_contract(result: dict[str, Any]) -> None:
    metrics = result.get("metrics")
    if not isinstance(metrics, dict):
        raise RuntimeError(
            f"{result.get('engine', 'unknown')} {result.get('scenario_name', 'unknown')} "
            "benchmark row is missing metrics"
        )
    missing_stage_fields = [
        field for field in STAGE_TIMING_CONTRACT_FIELDS if field not in metrics
    ]
    if missing_stage_fields:
        raise RuntimeError(
            f"{result.get('engine', 'unknown')} {result.get('scenario_name', 'unknown')} "
            "benchmark row omitted stage timing fields: "
            + ", ".join(missing_stage_fields)
        )
    missing_mode_fields = [
        field for field in EXECUTION_MODE_CONTRACT_FIELDS if field not in result
    ]
    if missing_mode_fields:
        raise RuntimeError(
            f"{result.get('engine', 'unknown')} {result.get('scenario_name', 'unknown')} "
            "benchmark row omitted execution-mode fields: "
            + ", ".join(missing_mode_fields)
        )
    missing_operator_fields = [
        field for field in OPERATOR_BLOCKER_MATRIX_FIELDS if field not in result
    ]
    if missing_operator_fields:
        raise RuntimeError(
            f"{result.get('engine', 'unknown')} {result.get('scenario_name', 'unknown')} "
            "benchmark row omitted operator blocker fields: "
            + ", ".join(missing_operator_fields)
        )
    missing_fused_pipeline_fields = [
        field for field in FUSED_PIPELINE_FIELDS if field not in metrics
    ]
    if missing_fused_pipeline_fields:
        raise RuntimeError(
            f"{result.get('engine', 'unknown')} {result.get('scenario_name', 'unknown')} "
            "benchmark row omitted fused pipeline fields: "
            + ", ".join(missing_fused_pipeline_fields)
        )
    missing_compressed_kernel_registry_fields = [
        field for field in COMPRESSED_KERNEL_REGISTRY_FIELDS if field not in metrics
    ]
    if missing_compressed_kernel_registry_fields:
        raise RuntimeError(
            f"{result.get('engine', 'unknown')} {result.get('scenario_name', 'unknown')} "
            "benchmark row omitted compressed kernel registry fields: "
            + ", ".join(missing_compressed_kernel_registry_fields)
        )
    missing_scan_pushdown_fields = [
        field for field in SCAN_PUSHDOWN_FIELDS if field not in metrics
    ]
    if missing_scan_pushdown_fields:
        raise RuntimeError(
            f"{result.get('engine', 'unknown')} {result.get('scenario_name', 'unknown')} "
            "benchmark row omitted scan pushdown fields: "
            + ", ".join(missing_scan_pushdown_fields)
        )
    missing_persistent_runner_fields = [
        field for field in PERSISTENT_RUNNER_ADMISSION_FIELDS if field not in metrics
    ]
    if missing_persistent_runner_fields:
        raise RuntimeError(
            f"{result.get('engine', 'unknown')} {result.get('scenario_name', 'unknown')} "
            "benchmark row omitted persistent-runner admission fields: "
            + ", ".join(missing_persistent_runner_fields)
        )
    missing_work_avoidance_fields = [
        field for field in WORK_AVOIDANCE_EVIDENCE_FIELDS if field not in result
    ]
    if missing_work_avoidance_fields:
        raise RuntimeError(
            f"{result.get('engine', 'unknown')} {result.get('scenario_name', 'unknown')} "
            "benchmark row omitted work-avoidance evidence fields: "
            + ", ".join(missing_work_avoidance_fields)
        )
    for metric in WORK_AVOIDANCE_METRICS:
        status = str(result.get(f"work_avoidance_{metric}_status") or "")
        if status not in WORK_AVOIDANCE_STATUS_VOCABULARY:
            raise RuntimeError(
                f"{result.get('engine', 'unknown')} {result.get('scenario_name', 'unknown')} "
                f"used invalid work-avoidance status for {metric}: {status}"
            )
        if status in ("not_available", "unsupported") and result.get(
            f"work_avoidance_{metric}_value"
        ) in (0, "0"):
            raise RuntimeError(
                "missing work-avoidance metrics must not be converted to zero"
            )

    selected_mode = str(result.get("selected_execution_mode") or "")
    requested_mode = str(result.get("requested_execution_mode") or "")
    if is_shardloom_engine(str(result.get("engine"))):
        if selected_mode not in SHARDLOOM_EXECUTION_MODE_VOCABULARY:
            raise RuntimeError(f"unrecognized ShardLoom execution mode: {selected_mode}")
    elif selected_mode != EXTERNAL_BASELINE_EXECUTION_MODE:
        raise RuntimeError(f"external baseline row used unexpected mode: {selected_mode}")

    if requested_mode == "auto":
        if selected_mode == "auto" or not result.get("mode_selection_reason"):
            raise RuntimeError("auto execution mode must report selected mode and reason")

    if (
        result.get("engine") == "shardloom"
        and result.get("status") == "success"
        and selected_mode == "compatibility_import_certified"
    ):
        if result.get("compatibility_import_included") is not True:
            raise RuntimeError("compatibility_import_certified row must mark import included")
        if result.get("vortex_write_reopen_included") is not True:
            raise RuntimeError(
                "compatibility_import_certified row must mark Vortex write/reopen included"
            )
    if (
        selected_mode in ("prepared_vortex", "native_vortex")
        and result.get("status") == "success"
        and result.get("operator_execution_class")
        in ("materialized_temporary", "residual_native")
        and result.get("operator_encoded_native_claim_allowed") is True
    ):
        raise RuntimeError(
            "temporary or residual prepared/native operators cannot be encoded-native claims"
        )
    if (
        selected_mode in ("prepared_vortex", "native_vortex")
        and result.get("status") == "success"
        and metrics.get("fused_pipeline_used") is True
        and metrics.get("fused_pipeline_encoded_native_claim_allowed") is True
    ):
        raise RuntimeError(
            "fused residual prepared/native paths cannot be encoded-native claims"
        )
    if (
        is_shardloom_engine(str(result.get("engine") or ""))
        and metrics.get("fused_pipeline_fallback_attempted") is True
    ):
        raise RuntimeError("fused pipeline evidence cannot report fallback attempts")
    if (
        is_shardloom_engine(str(result.get("engine") or ""))
        and metrics.get("fused_pipeline_external_engine_invoked") is True
    ):
        raise RuntimeError("fused pipeline evidence cannot report external engine execution")
    if (
        is_shardloom_engine(str(result.get("engine") or ""))
        and metrics.get("fused_pipeline_used") is True
        and metrics.get("fused_pipeline_correctness_digest_match") is not True
    ):
        raise RuntimeError("fused pipeline rows must report matching correctness digests")
    if (
        is_shardloom_engine(str(result.get("engine") or ""))
        and metrics.get("fused_pipeline_used") is True
        and metrics.get("fused_pipeline_blocker_id") not in (None, "none")
    ):
        raise RuntimeError("fused pipeline rows cannot carry an active blocker")
    if (
        is_shardloom_engine(str(result.get("engine") or ""))
        and metrics.get("compressed_kernel_registry_encoded_native_claim_allowed") is True
    ):
        raise RuntimeError(
            "compressed kernel registry evidence cannot upgrade encoded-native claims"
        )
    if (
        is_shardloom_engine(str(result.get("engine") or ""))
        and metrics.get("compressed_kernel_registry_fallback_attempted") is True
    ):
        raise RuntimeError("compressed kernel registry evidence cannot report fallback attempts")
    if (
        is_shardloom_engine(str(result.get("engine") or ""))
        and metrics.get("compressed_kernel_registry_external_engine_invoked") is True
    ):
        raise RuntimeError(
            "compressed kernel registry evidence cannot report external engine execution"
        )
    if (
        is_shardloom_engine(str(result.get("engine") or ""))
        and metrics.get("scan_pushdown_fallback_attempted") is True
    ):
        raise RuntimeError("scan pushdown evidence cannot report fallback attempts")
    if (
        is_shardloom_engine(str(result.get("engine") or ""))
        and metrics.get("scan_pushdown_external_engine_invoked") is True
    ):
        raise RuntimeError("scan pushdown evidence cannot report external engine execution")
    if is_shardloom_engine(str(result.get("engine") or "")):
        missing_source_state_fields = [
            field for field in SOURCE_STATE_CONTRACT_FIELDS if field not in metrics
        ]
        if missing_source_state_fields:
            raise RuntimeError(
                "ShardLoom row omitted SourceState contract fields: "
                + ", ".join(missing_source_state_fields)
            )
        if metrics.get("source_state_fallback_attempted") is not False:
            raise RuntimeError("SourceState evidence cannot report fallback attempts")
        if metrics.get("source_state_external_engine_invoked") is not False:
            raise RuntimeError(
                "SourceState evidence cannot report external engine execution"
            )
        if metrics.get("source_state_claim_gate_status") != "not_claim_grade":
            raise RuntimeError("SourceState evidence cannot upgrade claim status")
        if (
            metrics.get("source_state_status") == "source_state_reuse_supported"
            and metrics.get("source_state_reuse_allowed") is not True
        ):
            raise RuntimeError("SourceState reuse-supported rows must allow reuse explicitly")
        if (
            metrics.get("source_state_status") == "source_state_reuse_supported"
            and result.get("selected_execution_mode") == "compatibility_import_certified"
        ):
            raise RuntimeError(
                "SourceState reuse cannot turn compatibility-import rows into prepared/native rows"
            )
        missing_prepared_state_fields = [
            field for field in PREPARED_STATE_CONTRACT_FIELDS if field not in metrics
        ]
        if missing_prepared_state_fields:
            raise RuntimeError(
                "ShardLoom row omitted VortexPreparedState contract fields: "
                + ", ".join(missing_prepared_state_fields)
            )
        if metrics.get("prepared_state_fallback_attempted") is not False:
            raise RuntimeError("VortexPreparedState evidence cannot report fallback attempts")
        if metrics.get("prepared_state_external_engine_invoked") is not False:
            raise RuntimeError(
                "VortexPreparedState evidence cannot report external engine execution"
            )
        if metrics.get("prepared_state_claim_gate_status") != "not_claim_grade":
            raise RuntimeError("VortexPreparedState evidence cannot upgrade claim status")
        if (
            result.get("selected_execution_mode") == "direct_compatibility_transient"
            and metrics.get("prepared_state_reuse_allowed") is True
        ):
            raise RuntimeError("direct transient rows cannot report prepared-state reuse")
        missing_output_plan_fields = [
            field for field in OUTPUT_PLAN_CONTRACT_FIELDS if field not in metrics
        ]
        if missing_output_plan_fields:
            raise RuntimeError(
                "ShardLoom row omitted OutputPlan contract fields: "
                + ", ".join(missing_output_plan_fields)
            )
        if metrics.get("output_plan_fallback_attempted") is not False:
            raise RuntimeError("OutputPlan evidence cannot report fallback attempts")
        if metrics.get("output_plan_external_engine_invoked") is not False:
            raise RuntimeError(
                "OutputPlan evidence cannot report external engine execution"
            )
        if metrics.get("output_plan_claim_gate_status") != "not_claim_grade":
            raise RuntimeError("OutputPlan evidence cannot upgrade claim status")
        if (
            str(metrics.get("output_format")) in {"s3", "gcs", "adls", "object_store"}
            or "://" in str(metrics.get("output_location"))
        ):
            raise RuntimeError(
                "OutputPlan evidence cannot report object-store output in this slice"
            )
        missing_cache_invalidation_fields = [
            field for field in CACHE_INVALIDATION_CONTRACT_FIELDS if field not in metrics
        ]
        if missing_cache_invalidation_fields:
            raise RuntimeError(
                "ShardLoom row omitted cache invalidation contract fields: "
                + ", ".join(missing_cache_invalidation_fields)
            )
        if metrics.get("cache_invalidation_fallback_attempted") is not False:
            raise RuntimeError("cache invalidation evidence cannot report fallback attempts")
        if metrics.get("cache_invalidation_external_engine_invoked") is not False:
            raise RuntimeError(
                "cache invalidation evidence cannot report external engine execution"
            )
        if metrics.get("cache_invalidation_claim_gate_status") != "not_claim_grade":
            raise RuntimeError("cache invalidation evidence cannot upgrade claim status")
        if metrics.get("cache_invalidation_redaction_status") != (
            "no_credentials_or_tokens_in_fingerprint_fields"
        ):
            raise RuntimeError(
                "cache invalidation evidence must preserve credential redaction status"
            )
        if (
            metrics.get("cache_invalidation_status") == "cache_valid"
            and metrics.get("cache_valid") is not True
        ):
            raise RuntimeError("cache-valid rows must set cache_valid=true")
        missing_reuse_level_fields = [
            field for field in REUSE_LEVEL_CONTRACT_FIELDS if field not in metrics
        ]
        if missing_reuse_level_fields:
            raise RuntimeError(
                "ShardLoom row omitted evidence-safe reuse-level fields: "
                + ", ".join(missing_reuse_level_fields)
            )
        if metrics.get("reuse_level_fallback_attempted") is not False:
            raise RuntimeError("reuse-level evidence cannot report fallback attempts")
        if metrics.get("reuse_level_external_engine_invoked") is not False:
            raise RuntimeError(
                "reuse-level evidence cannot report external engine execution"
            )
        if metrics.get("reuse_level_claim_gate_status") != "not_claim_grade":
            raise RuntimeError("reuse-level evidence cannot upgrade claim status")
        if metrics.get("claim_grade_requirements_met") is not False:
            raise RuntimeError(
                "reuse-level evidence cannot mark claim-grade requirements met"
            )
        missing_optimizer_trace_fields = [
            field for field in OPTIMIZER_TRACE_FIELDS if field not in metrics
        ]
        if missing_optimizer_trace_fields:
            raise RuntimeError(
                "ShardLoom row omitted optimizer trace fields: "
                + ", ".join(missing_optimizer_trace_fields)
            )
        if metrics.get("optimizer_trace_schema_version") != OPTIMIZER_TRACE_SCHEMA_VERSION:
            raise RuntimeError("optimizer trace rows must preserve schema version")
        if metrics.get("optimizer_rule_applied_count") != 0:
            raise RuntimeError("report-only optimizer trace cannot apply rewrites")
        if metrics.get("optimizer_fallback_attempted") is not False:
            raise RuntimeError("optimizer trace evidence cannot report fallback attempts")
        if metrics.get("optimizer_external_engine_invoked") is not False:
            raise RuntimeError(
                "optimizer trace evidence cannot report external engine execution"
            )
        if metrics.get("optimizer_claim_gate_status") != "not_claim_grade":
            raise RuntimeError("optimizer trace evidence cannot upgrade claim status")
        if metrics.get("optimizer_source_state_reuse_admitted") is not True:
            raise RuntimeError(
                "optimizer trace should surface scoped source-state reuse admission"
            )
        missing_build_profile_fields = [
            field for field in BUILD_PROFILE_FIELDS if field not in metrics
        ]
        if missing_build_profile_fields:
            raise RuntimeError(
                "ShardLoom row omitted build-profile evidence fields: "
                + ", ".join(missing_build_profile_fields)
            )
        if metrics.get("build_profile_schema_version") != BUILD_PROFILE_SCHEMA_VERSION:
            raise RuntimeError("build-profile rows must preserve schema version")
        if metrics.get("build_profile_fallback_attempted") is not False:
            raise RuntimeError("build-profile evidence cannot report fallback attempts")
        if metrics.get("build_profile_external_engine_invoked") is not False:
            raise RuntimeError(
                "build-profile evidence cannot report external engine execution"
            )
        if metrics.get("build_profile_claim_gate_status") != "not_claim_grade":
            raise RuntimeError("build-profile evidence cannot upgrade claim status")
        build_profile = str(metrics.get("build_profile") or "")
        if build_profile not in SHARDLOOM_BUILD_PROFILES:
            raise RuntimeError(f"ShardLoom row reported unknown build profile: {build_profile}")
        if build_profile == "release-native-benchmark":
            if metrics.get("target_cpu_native_enabled") is not True:
                raise RuntimeError(
                    "release-native-benchmark rows must expose target-cpu=native"
                )
            if metrics.get("benchmark_only_build") is not True:
                raise RuntimeError("release-native-benchmark must be benchmark-only")
            if metrics.get("portable_release_artifact") is not False:
                raise RuntimeError(
                    "release-native-benchmark cannot satisfy portable release evidence"
                )
        elif metrics.get("target_cpu_native_enabled") is not False:
            raise RuntimeError(
                "portable ShardLoom build profiles cannot enable target-cpu=native"
            )
    if (
        is_shardloom_engine(str(result.get("engine") or ""))
        and result.get("status") == "success"
    ):
        persistent_runner_status = metrics.get("persistent_runner_status")
        if persistent_runner_status == BATCH_RUNNER_STATUS:
            missing_batch_fields = [
                field for field in BATCH_RUNNER_ADMISSION_FIELDS if field not in metrics
            ]
            if missing_batch_fields:
                raise RuntimeError(
                    "ShardLoom batch row omitted batch-runner fields: "
                    + ", ".join(missing_batch_fields)
                )
            if metrics.get("process_startup_attribution") != BATCH_PROCESS_STARTUP_ATTRIBUTION:
                raise RuntimeError("ShardLoom batch row must attribute shared batch CLI wall time")
            if metrics.get("python_harness_overhead_status") != BATCH_HARNESS_OVERHEAD_STATUS:
                raise RuntimeError("ShardLoom batch row must explain shared batch overhead")
            if metrics.get("batch_process_wall_shared") is not True:
                raise RuntimeError("ShardLoom batch row must mark shared process wall timing")
            missing_session_fields = [
                field for field in SESSION_RUNTIME_FIELDS if field not in metrics
            ]
            if missing_session_fields:
                raise RuntimeError(
                    "ShardLoom batch row omitted session runtime fields: "
                    + ", ".join(missing_session_fields)
                )
            if metrics.get("session_hidden_global_cache") is not False:
                raise RuntimeError("ShardLoom session rows must forbid hidden global cache")
            if metrics.get("session_daemon_or_service") is not False:
                raise RuntimeError("ShardLoom session rows must not imply daemon/service runtime")
            if metrics.get("session_fallback_attempted") is not False:
                raise RuntimeError("ShardLoom session rows cannot report fallback attempts")
            if metrics.get("session_external_engine_invoked") is not False:
                raise RuntimeError(
                    "ShardLoom session rows cannot report external engine execution"
                )
            missing_allocation_fields = [
                field for field in ALLOCATION_RESOURCE_PROFILE_FIELDS if field not in metrics
            ]
            if missing_allocation_fields:
                raise RuntimeError(
                    "ShardLoom batch row omitted allocation/resource profile fields: "
                    + ", ".join(missing_allocation_fields)
                )
            if metrics.get("buffer_pool_enabled") is not False:
                raise RuntimeError("ShardLoom allocation rows must not hide a buffer pool")
            if metrics.get("unsafe_lifetime_shortcut_used") is not False:
                raise RuntimeError("ShardLoom allocation rows cannot use unsafe lifetime shortcuts")
            if metrics.get("allocation_fallback_attempted") is not False:
                raise RuntimeError("ShardLoom allocation rows cannot report fallback attempts")
            if metrics.get("allocation_external_engine_invoked") is not False:
                raise RuntimeError(
                    "ShardLoom allocation rows cannot report external engine execution"
                )
            missing_evidence_level_fields = [
                field for field in RUNTIME_EVIDENCE_LEVEL_FIELDS if field not in metrics
            ]
            if missing_evidence_level_fields:
                raise RuntimeError(
                    "ShardLoom batch row omitted runtime evidence-level fields: "
                    + ", ".join(missing_evidence_level_fields)
                )
            if metrics.get("evidence_level") not in (
                "minimal_runtime",
                "certified",
                "full_replay",
            ):
                raise RuntimeError("ShardLoom batch row reported unknown evidence_level")
            if metrics.get("evidence_level_fallback_attempted") is not False:
                raise RuntimeError("ShardLoom evidence-level rows cannot report fallback attempts")
            if metrics.get("evidence_level_external_engine_invoked") is not False:
                raise RuntimeError(
                    "ShardLoom evidence-level rows cannot report external engine execution"
                )
            if (
                metrics.get("evidence_level") == "minimal_runtime"
                and metrics.get("claim_gate_status") != "not_claim_grade"
            ):
                raise RuntimeError(
                    "ShardLoom minimal_runtime rows must remain not_claim_grade"
                )
            if (
                metrics.get("evidence_level") == "full_replay"
                and metrics.get("evidence_level_result_sink_replay_verified") is not True
            ):
                raise RuntimeError(
                    "ShardLoom full_replay rows must verify result-sink replay"
                )
        else:
            if persistent_runner_status != PERSISTENT_RUNNER_STATUS:
                raise RuntimeError("ShardLoom row hid or altered persistent runner status")
            if (
                metrics.get("process_startup_attribution")
                != "per_scenario_cli_process_wall_measured"
            ):
                raise RuntimeError("ShardLoom row must attribute per-scenario CLI process startup")
            if (
                metrics.get("python_harness_overhead_status")
                != "outer_harness_wall_minus_cli_process_wall"
            ):
                raise RuntimeError("ShardLoom row must explain Python harness overhead attribution")
        if metrics.get("cli_process_wall_millis") is None:
            raise RuntimeError("ShardLoom row must preserve CLI process wall timing")


def annotate_result(
    result: dict[str, Any],
    cache_mode: str,
    dataset_profile: str,
) -> dict[str, Any]:
    metadata = scenario_metadata(result["scenario_base"])
    readiness = claim_grade_readiness(result)
    result["benchmark_suite"] = metadata.suite
    result["scenario_id"] = metadata.scenario_id
    result["scenario_category"] = metadata.category
    result["dataset_profile"] = dataset_profile
    result["engine_role"] = engine_role(result["engine"])
    result["claim_gate_status"] = readiness["claim_gate_status"]
    result["claim_grade_requirements_met"] = readiness["claim_grade_requirements_met"]
    result["claim_grade_missing_evidence"] = readiness["claim_grade_missing_evidence"]
    result["benchmark_constitution"] = benchmark_constitution(
        result, cache_mode, dataset_profile
    )
    validate_result_attribution_contract(result)
    return result


def coverage_status(result: dict[str, Any]) -> str:
    if result["status"] != "success":
        if result["status"] in ("unsupported", "unsupported_format"):
            return "unsupported"
        if result["status"] == "missing_dependency":
            return "blocked"
        return "blocked"
    if result["engine"] in (
        "shardloom-vortex",
        "shardloom-prepared-vortex",
        "shardloom-direct-transient",
    ):
        return "fixture_smoke_only"
    if result["engine"] == "shardloom":
        return str(result.get("claim_gate_status", "not_claim_grade"))
    return "external_baseline_only"


def support_status(result: dict[str, Any]) -> str:
    if result["status"] == "success":
        if not is_shardloom_engine(result["engine"]):
            return "external_baseline_only"
        return "supported"
    if result["status"] in ("unsupported", "unsupported_format"):
        return "unsupported"
    return "blocked"


def materialization_decode_evidence_present(evidence: dict[str, Any]) -> bool:
    return (
        evidence.get("materialization_boundary_report_emitted") == "true"
        and evidence.get("native_io_materializing_transitions_have_boundaries") == "true"
    )


def native_unsupported_coverage_ref(result: dict[str, Any]) -> str | None:
    if not is_shardloom_engine(result["engine"]):
        return None
    if support_status(result) == "unsupported":
        return NATIVE_UNSUPPORTED_COVERAGE_REF
    return None


def unsupported_diagnostic_code(result: dict[str, Any], evidence: dict[str, Any]) -> str | None:
    if result["status"] not in ("unsupported", "unsupported_format"):
        return None
    return (
        result.get("unsupported_diagnostic_code")
        or evidence.get("unsupported_diagnostic_code")
        or result.get("mode_selection_reason")
        or "unsupported_without_fallback"
    )


def unsupported_blocker_id(result: dict[str, Any], evidence: dict[str, Any]) -> str | None:
    if result["status"] not in ("unsupported", "unsupported_format"):
        return None
    return result.get("blocker_id") or evidence.get("blocker_id") or result.get(
        "operator_blocker_id"
    )


def unsupported_required_future_evidence(
    result: dict[str, Any], evidence: dict[str, Any]
) -> str | None:
    if result["status"] not in ("unsupported", "unsupported_format"):
        return None
    return result.get("required_future_evidence") or evidence.get(
        "required_future_evidence"
    )


def direct_transient_admission_coverage_row(result: dict[str, Any]) -> dict[str, Any]:
    constitution = result["benchmark_constitution"]
    return {
        "scenario_name": result["scenario_name"],
        "scenario_id": result["scenario_id"],
        "scenario_category": result["scenario_category"],
        "dataset_profile": result["dataset_profile"],
        "engine": result["engine"],
        "engine_role": result["engine_role"],
        "status": "unsupported",
        "row_classification": "unsupported",
        "support_status": "unsupported",
        "supported_status": "unsupported",
        "timing_row_present": False,
        "claim_gate_status": "not_claim_grade",
        "claim_grade_requirements_met": False,
        "claim_grade_missing_evidence": [
            "direct compatibility transient runtime is unsupported",
            "direct mode certificate missing",
        ],
        "correctness_digest_stable": False,
        "reproducibility_min_iterations": MIN_CLAIM_GRADE_ITERATIONS,
        "reproducibility_iterations_met": False,
        "reproducible_benchmark_row": False,
        "timing_row_claim_grade": False,
        "write_timing_present": False,
        "computed_result_sink_write_millis": None,
        "execution_mode": "direct_compatibility_transient",
        "requested_execution_mode": "direct_compatibility_transient",
        "selected_execution_mode": "direct_compatibility_transient",
        "mode_selection_reason": "direct_compatibility_transient_not_implemented",
        "execution_mode_family": "compatibility",
        "vortex_native_claim_allowed": False,
        "compatibility_import_included": False,
        "vortex_prepare_included": False,
        "vortex_write_reopen_included": False,
        "direct_transient_execution": False,
        "operator_execution_class": "unsupported",
        "operator_admission_status": "unsupported",
        "operator_blocker_id": "gar-flow-2b.direct_transient_admission_only",
        "operator_blocker_reason": "direct transient admission row has no prepared/native operator execution",
        "operator_encoded_native_claim_allowed": False,
        "operator_residual_native_used": False,
        "operator_temporary_materialization_used": False,
        "operator_blocker_matrix_ref": "operator-blocker://traditional_analytics/direct_transient_admission",
        "preparation_millis": None,
        "preparation_included_in_timing": False,
        "benchmark_constitution_id": constitution["constitution_id"],
        "benchmark_row_ref": None,
        "coverage_row_ref": "coverage.direct_compatibility_transient.admission",
        "certificate_status": "unsupported",
        "execution_certificate_status": "unsupported",
        "source_native_io_certificate_status": "unsupported",
        "result_native_io_certificate_status": "unsupported",
        "materialization_decode_evidence_present": False,
        "native_io_status_required": False,
        "materialization_policy": constitution["materialization_policy"],
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "native_unsupported_coverage_ref": NATIVE_UNSUPPORTED_COVERAGE_REF,
        "unsupported_diagnostic_code": "direct_compatibility_transient_not_implemented",
        "blocker_id": "P7.5.4",
        "required_future_evidence": "shardloom_native_transient_executor,direct_mode_certificate",
        "native_io_source_sink_coverage_ref": NATIVE_IO_SOURCE_SINK_COVERAGE_REF,
        "vortex_source_split_admission_ref": VORTEX_SOURCE_SPLIT_ADMISSION_REF,
        "vortex_segment_extraction_admission_ref": VORTEX_SEGMENT_EXTRACTION_ADMISSION_REF,
        "vortex_layout_device_managed_boundary_ref": VORTEX_LAYOUT_DEVICE_MANAGED_BOUNDARY_REF,
        "materialization_policy_ref": MATERIALIZATION_POLICY_REF,
    }


def coverage_table(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for result in results:
        constitution = result["benchmark_constitution"]
        reproducible_row = reproducible_benchmark_row(result)
        evidence = result.get("shardloom_evidence", {})
        row_classification = coverage_status(result)
        if row_classification not in ROW_CLASSIFICATIONS:
            raise RuntimeError(f"unrecognized coverage row classification: {row_classification}")
        rows.append(
            {
                "scenario_name": result["scenario_name"],
                "scenario_id": result["scenario_id"],
                "scenario_category": result["scenario_category"],
                "dataset_profile": result["dataset_profile"],
                "engine": result["engine"],
                "engine_role": result["engine_role"],
                "status": row_classification,
                "row_classification": row_classification,
                "support_status": support_status(result),
                "supported_status": support_status(result),
                "timing_row_present": result["metrics"]["query_runtime_millis"] is not None,
                "claim_gate_status": result["claim_gate_status"],
                "claim_grade_requirements_met": result["claim_grade_requirements_met"],
                "claim_grade_missing_evidence": result["claim_grade_missing_evidence"],
                "correctness_digest_stable": result.get("correctness_digest_stable"),
                "reproducibility_min_iterations": MIN_CLAIM_GRADE_ITERATIONS,
                "reproducibility_iterations_met": result.get("iterations", 0)
                >= MIN_CLAIM_GRADE_ITERATIONS,
                "reproducible_benchmark_row": reproducible_row,
                "timing_row_claim_grade": (
                    reproducible_row and result["claim_grade_requirements_met"]
                ),
                "write_timing_present": result["metrics"].get(
                    "computed_result_sink_write_millis"
                )
                is not None,
                "computed_result_sink_write_millis": result["metrics"].get(
                    "computed_result_sink_write_millis"
                ),
                "execution_mode": result.get("execution_mode"),
                "requested_execution_mode": result.get("requested_execution_mode"),
                "selected_execution_mode": result.get("selected_execution_mode"),
                "mode_selection_reason": result.get("mode_selection_reason"),
                "execution_mode_family": result.get("execution_mode_family"),
                "vortex_native_claim_allowed": result.get(
                    "vortex_native_claim_allowed"
                ),
                "compatibility_import_included": result.get(
                    "compatibility_import_included"
                ),
                "vortex_prepare_included": result.get("vortex_prepare_included"),
                "vortex_write_reopen_included": result.get(
                    "vortex_write_reopen_included"
                ),
                "direct_transient_execution": result.get("direct_transient_execution"),
                "operator_execution_class": result.get("operator_execution_class"),
                "operator_admission_status": result.get("operator_admission_status"),
                "operator_blocker_id": result.get("operator_blocker_id"),
                "operator_blocker_reason": result.get("operator_blocker_reason"),
                "operator_encoded_native_claim_allowed": result.get(
                    "operator_encoded_native_claim_allowed"
                ),
                "operator_residual_native_used": result.get("operator_residual_native_used"),
                "operator_temporary_materialization_used": result.get(
                    "operator_temporary_materialization_used"
                ),
                "operator_blocker_matrix_ref": result.get("operator_blocker_matrix_ref"),
                "preparation_millis": result["metrics"].get("preparation_millis"),
                "preparation_included_in_timing": result["metrics"].get(
                    "preparation_included_in_timing"
                ),
                "benchmark_constitution_id": constitution["constitution_id"],
                "benchmark_row_ref": evidence.get("benchmark_row_ref"),
                "coverage_row_ref": evidence.get("coverage_row_ref"),
                "certificate_status": evidence.get("native_io_certificate_status"),
                "execution_certificate_status": evidence.get(
                    "runtime_execution_certificate_status"
                ),
                "source_native_io_certificate_status": evidence.get(
                    "native_io_certificate_status"
                ),
                "result_native_io_certificate_status": evidence.get(
                    "computed_result_sink_native_io_certificate_status"
                ),
                "materialization_decode_evidence_present": (
                    materialization_decode_evidence_present(evidence)
                ),
                "native_io_status_required": is_shardloom_engine(result["engine"]),
                "materialization_policy": constitution["materialization_policy"],
                "fallback_attempted": result.get("fallback_attempted", False),
                "external_engine_invoked": (
                    not is_shardloom_engine(result["engine"])
                    and result["status"] == "success"
                ),
                "native_unsupported_coverage_ref": native_unsupported_coverage_ref(
                    result
                ),
                "unsupported_diagnostic_code": unsupported_diagnostic_code(
                    result, evidence
                ),
                "blocker_id": unsupported_blocker_id(result, evidence),
                "required_future_evidence": unsupported_required_future_evidence(
                    result, evidence
                ),
                "native_io_source_sink_coverage_ref": (
                    NATIVE_IO_SOURCE_SINK_COVERAGE_REF
                    if is_shardloom_engine(result["engine"])
                    else None
                ),
                "vortex_source_split_admission_ref": (
                    VORTEX_SOURCE_SPLIT_ADMISSION_REF
                    if is_shardloom_engine(result["engine"])
                    else None
                ),
                "vortex_segment_extraction_admission_ref": (
                    VORTEX_SEGMENT_EXTRACTION_ADMISSION_REF
                    if is_shardloom_engine(result["engine"])
                    else None
                ),
                "vortex_layout_device_managed_boundary_ref": (
                    VORTEX_LAYOUT_DEVICE_MANAGED_BOUNDARY_REF
                    if is_shardloom_engine(result["engine"])
                    else None
                ),
                "materialization_policy_ref": (
                    MATERIALIZATION_POLICY_REF
                    if is_shardloom_engine(result["engine"])
                    else None
                ),
            }
        )
        if result["engine"] == "shardloom":
            rows.append(direct_transient_admission_coverage_row(result))
    return rows


def format_preparation_matrix(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for result in results:
        if not is_shardloom_engine(result["engine"]):
            continue
        metrics = result["metrics"]
        data_format = result["storage_format"]
        selected_mode = result.get("selected_execution_mode") or result.get("execution_mode")
        if result["engine"] == "shardloom-direct-transient":
            row_scope = "direct_transient_local_csv_smoke"
        elif result["engine"] == "shardloom":
            row_scope = "compatibility_preparation_and_query"
        else:
            row_scope = "prepared_vortex_query_from_prepared_artifact"
        rows.append(
            {
                "storage_format": data_format,
                "scenario_name": result["scenario_name"],
                "engine": result["engine"],
                "status": result["status"],
                "execution_mode": selected_mode,
                "row_scope": row_scope,
                "native_execution_format": "vortex",
                "operator_execution_class": result.get("operator_execution_class"),
                "operator_admission_status": result.get("operator_admission_status"),
                "operator_blocker_id": result.get("operator_blocker_id"),
                "operator_encoded_native_claim_allowed": result.get(
                    "operator_encoded_native_claim_allowed"
                ),
                "compatibility_preparation_input": data_format
                != SHARDLOOM_VORTEX_FORMAT,
                "preparation_included_in_timing": metrics.get(
                    "preparation_included_in_timing"
                ),
                "preparation_millis": metrics.get("preparation_millis"),
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
                "total_runtime_millis": metrics.get("total_runtime_millis"),
                "persistent_runner_status": metrics.get("persistent_runner_status"),
                "claim_gate_status": result.get("claim_gate_status"),
            }
        )
    return rows


def source_state_contract() -> dict[str, Any]:
    return {
        "contract_id": SOURCE_STATE_CONTRACT_SCHEMA_VERSION,
        "canonical_reference": "docs/architecture/io-reuse-and-fanout-architecture.md",
        "companion_reference": "docs/architecture/compute-engine-flow-reference.md",
        "status_vocabulary": list(SOURCE_STATE_CONTRACT_STATUS_VOCABULARY),
        "row_fields": list(SOURCE_STATE_CONTRACT_FIELDS),
        "supported_local_formats": list(FORMAT_ORDER),
        "stable_path": (
            "InputAdapter -> SourceState -> VortexPreparedState -> ExecutionPlan -> "
            "OutputPlan -> SinkArtifact"
        ),
        "current_scope": (
            "local benchmark SourceState identity, schema/fingerprint posture, "
            "parse/decode plan digest, and scoped prepared/native reuse visibility"
        ),
        "non_goals": [
            "object-store runtime",
            "lakehouse/table commit support",
            "production SQL/DataFrame runtime",
            "performance or superiority claims",
            "implicit Vortex-native execution claims from SourceState alone",
        ],
        "no_fallback_rule": (
            "SourceState rows must preserve source_state_fallback_attempted=false "
            "and source_state_external_engine_invoked=false for ShardLoom rows."
        ),
        "claim_boundary": (
            "SourceState evidence is input preparation evidence only. It may prove local "
            "source discovery, schema identity, parse/decode planning, and scoped reuse "
            "posture; it does not prove output support, Vortex-native execution, "
            "performance, production, SQL/DataFrame, object-store/lakehouse, Foundry, "
            "package, release, or Spark-replacement readiness."
        ),
    }


def prepared_state_contract() -> dict[str, Any]:
    return {
        "contract_id": PREPARED_STATE_CONTRACT_SCHEMA_VERSION,
        "canonical_reference": "docs/architecture/io-reuse-and-fanout-architecture.md",
        "companion_reference": "docs/architecture/compute-engine-flow-reference.md",
        "status_vocabulary": list(PREPARED_STATE_CONTRACT_STATUS_VOCABULARY),
        "row_fields": list(PREPARED_STATE_CONTRACT_FIELDS),
        "current_scope": (
            "local prepared Vortex artifact identity, digest, preparation timing "
            "separation, source-state linkage, and scoped reuse visibility"
        ),
        "stable_path": (
            "InputAdapter -> SourceState -> VortexPreparedState -> ExecutionPlan -> "
            "OutputPlan -> SinkArtifact"
        ),
        "non_goals": [
            "new preparation runtime",
            "object-store Vortex artifact runtime",
            "lakehouse/table commit support",
            "production SQL/DataFrame runtime",
            "encoded-native operator claim",
            "performance or superiority claims",
        ],
        "no_fallback_rule": (
            "Prepared-state rows must preserve prepared_state_fallback_attempted=false "
            "and prepared_state_external_engine_invoked=false for ShardLoom rows."
        ),
        "claim_boundary": (
            "VortexPreparedState evidence is scoped prepared-artifact reuse evidence only. "
            "It does not prove encoded-native operator coverage, output support, performance, "
            "production, SQL/DataFrame, object-store/lakehouse, Foundry, package, release, "
            "or Spark-replacement readiness."
        ),
    }


def output_plan_contract() -> dict[str, Any]:
    return {
        "contract_id": OUTPUT_PLAN_CONTRACT_SCHEMA_VERSION,
        "canonical_reference": "docs/architecture/io-reuse-and-fanout-architecture.md",
        "companion_reference": "docs/architecture/compute-engine-flow-reference.md",
        "status_vocabulary": list(OUTPUT_PLAN_CONTRACT_STATUS_VOCABULARY),
        "row_fields": list(OUTPUT_PLAN_CONTRACT_FIELDS),
        "current_scope": (
            "scoped local Vortex result-sink planning, metadata preservation posture, "
            "write/replay refs, sink artifact identity, and no-fallback visibility"
        ),
        "stable_path": (
            "InputAdapter -> SourceState -> VortexPreparedState -> ExecutionPlan -> "
            "OutputPlan -> SinkArtifact"
        ),
        "non_goals": [
            "cross-format output fanout benchmark",
            "object-store output runtime",
            "lakehouse/table commit support",
            "Foundry production output",
            "production SQL/DataFrame runtime",
            "performance or superiority claims",
        ],
        "no_fallback_rule": (
            "Output-plan rows must preserve output_plan_fallback_attempted=false "
            "and output_plan_external_engine_invoked=false for ShardLoom rows."
        ),
        "claim_boundary": (
            "OutputPlan evidence is scoped local output-planning evidence only. It does "
            "not prove cross-format fanout, object-store/lakehouse/table commit, production, "
            "performance, SQL/DataFrame, Foundry, package, release, or Spark-replacement readiness."
        ),
    }


def fanout_benchmark_contract() -> dict[str, Any]:
    return {
        "contract_id": FANOUT_BENCHMARK_SCHEMA_VERSION,
        "canonical_reference": "docs/architecture/io-reuse-and-fanout-architecture.md",
        "companion_reference": "docs/architecture/compute-engine-flow-reference.md",
        "status_vocabulary": list(FANOUT_BENCHMARK_STATUS_VOCABULARY),
        "row_fields": list(FANOUT_BENCHMARK_FIELDS),
        "current_scope": (
            "deterministic report-only benchmark matrix for required cross-format "
            "fanout cases, using SourceState, VortexPreparedState, and OutputPlan "
            "contract vocabulary without adding fanout runtime"
        ),
        "stable_path": (
            "InputAdapter -> SourceState -> VortexPreparedState -> ExecutionPlan -> "
            "OutputPlan -> SinkArtifact"
        ),
        "non_goals": [
            "multi-output runtime execution",
            "object-store output runtime",
            "lakehouse/table commit support",
            "generated-source runtime execution",
            "performance or superiority claims",
        ],
        "no_fallback_rule": (
            "Fanout benchmark rows must preserve fallback_attempted=false and "
            "external_engine_invoked=false for every supported, blocked, unsupported, "
            "or report-only case."
        ),
        "claim_boundary": (
            "The fanout benchmark matrix is local pre-release workflow/evidence posture. "
            "It does not prove cross-format fanout runtime, object-store/lakehouse/table "
            "commit, production, performance, SQL/DataFrame, Foundry, package, release, "
            "or Spark-replacement readiness."
        ),
    }


def cache_invalidation_contract() -> dict[str, Any]:
    return {
        "contract_id": CACHE_INVALIDATION_SCHEMA_VERSION,
        "canonical_reference": "docs/architecture/io-reuse-and-fanout-architecture.md",
        "companion_reference": "docs/architecture/compute-engine-flow-reference.md",
        "status_vocabulary": list(CACHE_INVALIDATION_STATUS_VOCABULARY),
        "row_fields": list(CACHE_INVALIDATION_CONTRACT_FIELDS),
        "current_scope": (
            "deterministic local fingerprint and invalidation posture for "
            "SourceState, VortexPreparedState, ExecutionPlan, OutputPlan, and "
            "SinkArtifact benchmark rows without adding a persistent cache"
        ),
        "stable_path": (
            "InputAdapter -> SourceState -> VortexPreparedState -> ExecutionPlan -> "
            "OutputPlan -> SinkArtifact"
        ),
        "invalidation_rules": [
            "source fingerprint changes block reuse",
            "schema digest changes block reuse",
            "plan digest changes block reuse",
            "output plan digest changes block reuse",
            "policy or evidence-level mismatch blocks reuse",
            "object-store ETag/version handling is planned but not runtime-claimed",
        ],
        "non_goals": [
            "persistent disk cache",
            "daemon or service cache",
            "distributed cache",
            "object-store cache",
            "performance or superiority claims",
        ],
        "redaction_boundary": (
            "Cache keys, fingerprints, digests, explain output, and benchmark evidence "
            "must not contain credentials, tokens, or private values."
        ),
        "no_fallback_rule": (
            "Cache invalidation rows must preserve cache_invalidation_fallback_attempted=false "
            "and cache_invalidation_external_engine_invoked=false."
        ),
        "claim_boundary": (
            "Cache invalidation evidence may claim only deterministic reuse eligibility, "
            "reuse rejection, or invalidation reason for scoped local state. It does not "
            "authorize cache performance, production cache correctness, remote cache, "
            "object-store/lakehouse, Foundry, package, release, or Spark-replacement claims."
        ),
    }


def reuse_level_contract() -> dict[str, Any]:
    return {
        "contract_id": REUSE_LEVEL_SCHEMA_VERSION,
        "canonical_reference": "docs/architecture/io-reuse-and-fanout-architecture.md",
        "companion_reference": "docs/architecture/runtime-evidence-level-tiering.md",
        "status_vocabulary": list(REUSE_LEVEL_STATUS_VOCABULARY),
        "supported_levels": list(REUSE_LEVEL_SUPPORTED_LEVELS),
        "row_fields": list(REUSE_LEVEL_CONTRACT_FIELDS),
        "matrix_fields": [
            "scenario",
            "engine",
            "execution_mode",
            "evidence_level",
            "output_format",
            "reuse_level",
            "reuse_status",
            "reuse_hit",
            "reuse_digest",
            "reuse_allowed",
            "reuse_blocker",
            "layer_invalidation_reason",
            "claim_gate_status",
            "claim_grade_requirements_met",
            "fallback_attempted",
            "external_engine_invoked",
        ],
        "current_scope": (
            "report-level reuse status normalization across source discovery, schema, "
            "parse plan, prepared Vortex state, operator source-state, output plan, "
            "and result-replay layers without adding runtime cache behavior"
        ),
        "stable_path": (
            "InputAdapter -> SourceState -> VortexPreparedState -> ExecutionPlan -> "
            "OutputPlan -> SinkArtifact"
        ),
        "non_goals": [
            "new runtime speed mode",
            "hidden global cache",
            "persistent cache",
            "claim-grade promotion from reuse evidence",
            "object-store/lakehouse runtime",
            "Foundry production runtime",
            "performance or superiority claims",
        ],
        "no_fallback_rule": (
            "Reuse-level rows must preserve fallback_attempted=false and "
            "external_engine_invoked=false for every level and every evidence level."
        ),
        "claim_boundary": (
            "Evidence-safe reuse levels may claim only that reuse status and evidence "
            "completeness are visible and machine-readable. They do not prove correctness, "
            "output fidelity, performance, production readiness, SQL/DataFrame runtime, "
            "object-store/lakehouse support, Foundry support, package readiness, release "
            "readiness, or Spark-replacement status."
        ),
    }


def build_profile_contract() -> dict[str, Any]:
    return {
        "contract_id": BUILD_PROFILE_SCHEMA_VERSION,
        "canonical_reference": (
            "docs/architecture/optimized-build-profiles-pgo-benchmark-lane.md"
        ),
        "companion_reference": "docs/release/hard-release-readiness-gate.md",
        "status_vocabulary": list(BUILD_PROFILE_STATUS_VOCABULARY),
        "supported_profiles": list(SHARDLOOM_BUILD_PROFILES),
        "row_fields": list(BUILD_PROFILE_FIELDS),
        "current_scope": (
            "compiler/toolchain/build-profile evidence for ShardLoom benchmark binaries, "
            "including portable release, portable LTO, report-only PGO, and host-native "
            "benchmark-only lanes"
        ),
        "profile_boundary": {
            "release": "portable baseline; remains the default release-style benchmark build",
            "release-lto": "portable ThinLTO optimized local artifact lane",
            "release-pgo": (
                "report-only PGO lane unless SHARDLOOM_PGO_PROFILE points to a merged "
                "profile artifact"
            ),
            "release-native-benchmark": "host-native benchmark-only lane using target-cpu=native",
        },
        "pgo_workflow": [
            "instrumented build with -Cprofile-generate",
            "representative training workload run",
            "llvm-profdata merge",
            "profile-use rebuild with -Cprofile-use",
        ],
        "non_goals": [
            "changing default cargo build --release",
            "publishing package artifacts",
            "using target-cpu=native for portable release evidence",
            "performance or superiority claims",
            "hidden fast mode",
            "fallback execution",
        ],
        "no_fallback_rule": (
            "Build-profile rows must preserve build_profile_fallback_attempted=false and "
            "build_profile_external_engine_invoked=false for every ShardLoom row."
        ),
        "claim_boundary": (
            "Build-profile evidence is configuration and reproducibility evidence only. "
            "It cannot upgrade claim_gate_status, prove performance, authorize public "
            "package/release claims, or imply Spark replacement, SQL/DataFrame, object-store/"
            "lakehouse, Foundry, or production support."
        ),
    }


def source_state_matrix(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for result in results:
        metrics = result["metrics"]
        if "source_state_contract_schema_version" not in metrics:
            continue
        rows.append(
            {
                "scenario_name": result["scenario_name"],
                "engine": result["engine"],
                "status": result["status"],
                "execution_mode": result.get("selected_execution_mode")
                or result.get("execution_mode"),
                "source_state_status": metrics.get("source_state_status"),
                "source_state_id": metrics.get("source_state_id"),
                "source_state_digest": metrics.get("source_state_digest"),
                "source_format": metrics.get("source_format"),
                "source_location": metrics.get("source_location"),
                "source_fingerprint_kind": metrics.get("source_fingerprint_kind"),
                "schema_digest": metrics.get("schema_digest"),
                "row_count_known": metrics.get("row_count_known"),
                "file_count": metrics.get("file_count"),
                "byte_size": metrics.get("byte_size"),
                "partition_columns": metrics.get("partition_columns"),
                "compression": metrics.get("compression"),
                "source_state_reuse_allowed": metrics.get("source_state_reuse_allowed"),
                "source_state_reuse_hit": metrics.get("source_state_reuse_hit"),
                "source_state_reuse_reason": metrics.get("source_state_reuse_reason"),
                "source_discovery_millis": metrics.get("source_discovery_millis"),
                "schema_inference_millis": metrics.get("schema_inference_millis"),
                "source_parse_millis": metrics.get("source_parse_millis"),
                "parse_decode_plan_digest": metrics.get("parse_decode_plan_digest"),
                "source_state_materialization_decode_boundary_ref": metrics.get(
                    "source_state_materialization_decode_boundary_ref"
                ),
                "source_state_claim_gate_status": metrics.get(
                    "source_state_claim_gate_status"
                ),
                "source_state_fallback_attempted": metrics.get(
                    "source_state_fallback_attempted"
                ),
                "source_state_external_engine_invoked": metrics.get(
                    "source_state_external_engine_invoked"
                ),
                "source_state_claim_boundary": metrics.get("source_state_claim_boundary"),
            }
        )
    return rows


def prepared_state_matrix(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for result in results:
        metrics = result["metrics"]
        if "prepared_state_contract_schema_version" not in metrics:
            continue
        rows.append(
            {
                "scenario_name": result["scenario_name"],
                "engine": result["engine"],
                "status": result["status"],
                "execution_mode": result.get("selected_execution_mode")
                or result.get("execution_mode"),
                "prepared_state_status": metrics.get("prepared_state_status"),
                "prepared_state_id": metrics.get("prepared_state_id"),
                "prepared_state_digest": metrics.get("prepared_state_digest"),
                "source_state_id": metrics.get("prepared_state_source_state_id"),
                "vortex_artifact_ref": metrics.get("vortex_artifact_ref"),
                "vortex_artifact_digest": metrics.get("vortex_artifact_digest"),
                "layout_summary": metrics.get("layout_summary"),
                "encoding_summary": metrics.get("encoding_summary"),
                "statistics_summary": metrics.get("statistics_summary"),
                "prepared_state_reuse_allowed": metrics.get(
                    "prepared_state_reuse_allowed"
                ),
                "prepared_state_reuse_hit": metrics.get("prepared_state_reuse_hit"),
                "prepared_state_reuse_reason": metrics.get(
                    "prepared_state_reuse_reason"
                ),
                "preparation_included_in_timing": metrics.get(
                    "preparation_included_in_timing"
                ),
                "vortex_prepare_millis": metrics.get("vortex_prepare_millis"),
                "prepared_state_native_io_certificate_status": metrics.get(
                    "prepared_state_native_io_certificate_status"
                ),
                "prepared_state_materialization_decode_boundary_ref": metrics.get(
                    "prepared_state_materialization_decode_boundary_ref"
                ),
                "prepared_state_claim_gate_status": metrics.get(
                    "prepared_state_claim_gate_status"
                ),
                "prepared_state_fallback_attempted": metrics.get(
                    "prepared_state_fallback_attempted"
                ),
                "prepared_state_external_engine_invoked": metrics.get(
                    "prepared_state_external_engine_invoked"
                ),
                "prepared_state_claim_boundary": metrics.get(
                    "prepared_state_claim_boundary"
                ),
            }
        )
    return rows


def output_plan_matrix(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for result in results:
        metrics = result["metrics"]
        if "output_plan_contract_schema_version" not in metrics:
            continue
        rows.append(
            {
                "scenario_name": result["scenario_name"],
                "engine": result["engine"],
                "status": result["status"],
                "execution_mode": result.get("selected_execution_mode")
                or result.get("execution_mode"),
                "output_plan_status": metrics.get("output_plan_status"),
                "output_plan_id": metrics.get("output_plan_id"),
                "output_plan_digest": metrics.get("output_plan_digest"),
                "output_format": metrics.get("output_format"),
                "output_location": metrics.get("output_location"),
                "output_schema_digest": metrics.get("output_schema_digest"),
                "output_partitioning": metrics.get("output_partitioning"),
                "output_compression": metrics.get("output_compression"),
                "output_encoding": metrics.get("output_encoding"),
                "output_write_mode": metrics.get("output_write_mode"),
                "output_plan_reuse_allowed": metrics.get("output_plan_reuse_allowed"),
                "output_metadata_preservation_status": metrics.get(
                    "output_metadata_preservation_status"
                ),
                "output_materialization_required": metrics.get(
                    "output_materialization_required"
                ),
                "output_plan_reuse_hit": metrics.get("output_plan_reuse_hit"),
                "output_plan_reuse_reason": metrics.get("output_plan_reuse_reason"),
                "output_plan_millis": metrics.get("output_plan_millis"),
                "output_write_millis": metrics.get("output_write_millis"),
                "result_replay_verified": metrics.get("result_replay_verified"),
                "output_native_io_certificate_status": metrics.get(
                    "output_native_io_certificate_status"
                ),
                "sink_artifact_ref": metrics.get("sink_artifact_ref"),
                "sink_artifact_digest": metrics.get("sink_artifact_digest"),
                "output_plan_claim_gate_status": metrics.get(
                    "output_plan_claim_gate_status"
                ),
                "output_plan_fallback_attempted": metrics.get(
                    "output_plan_fallback_attempted"
                ),
                "output_plan_external_engine_invoked": metrics.get(
                    "output_plan_external_engine_invoked"
                ),
                "output_plan_claim_boundary": metrics.get("output_plan_claim_boundary"),
            }
        )
    return rows


def fanout_benchmark_matrix(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output_plan_rows = output_plan_matrix(results)
    local_vortex_result_sink_available = any(
        row.get("output_plan_status") == "output_plan_supported"
        and row.get("output_format") == "vortex"
        and row.get("result_replay_verified") is True
        for row in output_plan_rows
    )
    currently_proven_output_formats = (
        "vortex_result_sink_when_requested"
        if local_vortex_result_sink_available
        else "none_in_this_artifact"
    )
    rows: list[dict[str, Any]] = []
    for case in FANOUT_BENCHMARK_CASES:
        requested_outputs = tuple(case["requested_output_formats"])
        blocked_outputs = ",".join(requested_outputs)
        rows.append(
            {
                "benchmark_family": "io_reuse_and_fanout",
                "fanout_case_id": case["fanout_case_id"],
                "source_format": case["source_format"],
                "prepared_state_required": case["prepared_state_required"],
                "generated_source_required": case["generated_source_required"],
                "requested_output_formats": ",".join(requested_outputs),
                "currently_proven_output_formats": currently_proven_output_formats,
                "blocked_output_formats": blocked_outputs,
                "fanout_status": "report_only",
                "fanout_blocker_id": case["blocker_id"],
                "fanout_blocker_reason": (
                    "first-class multi-output fanout benchmark runtime is not implemented; "
                    "local Vortex result-sink OutputPlan evidence remains separate from "
                    "cross-format fanout support"
                ),
                "source_discovery_millis": None,
                "schema_inference_millis": None,
                "source_parse_millis": None,
                "vortex_prepare_millis": None,
                "operator_compute_millis": None,
                "output_plan_millis": None,
                "output_write_millis": None,
                "output_replay_millis": None,
                "total_runtime_millis": None,
                "source_state_reuse_hit": False,
                "prepared_state_reuse_hit": False,
                "output_plan_reuse_hit": False,
                "fanout_output_count": 0,
                "fallback_attempted": False,
                "external_engine_invoked": False,
                "claim_gate_status": "not_claim_grade",
                "claim_boundary": (
                    "report-only fanout benchmark posture; not runtime support, not a "
                    "performance claim, and not object-store/lakehouse/table/Foundry support"
                ),
            }
        )
    return rows


def cache_invalidation_matrix(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for result in results:
        metrics = result["metrics"]
        if "cache_invalidation_contract_schema_version" not in metrics:
            continue
        rows.append(
            {
                "scenario_name": result["scenario_name"],
                "engine": result["engine"],
                "status": result["status"],
                "execution_mode": result.get("selected_execution_mode")
                or result.get("execution_mode"),
                "cache_invalidation_status": metrics.get("cache_invalidation_status"),
                "cache_invalidation_layer_scope": metrics.get(
                    "cache_invalidation_layer_scope"
                ),
                "source_fingerprint_kind": metrics.get("source_fingerprint_kind"),
                "source_content_digest": metrics.get("source_content_digest"),
                "source_mtime": metrics.get("source_mtime"),
                "source_size": metrics.get("source_size"),
                "object_etag": metrics.get("object_etag"),
                "manifest_version": metrics.get("manifest_version"),
                "schema_digest": metrics.get("schema_digest"),
                "plan_digest": metrics.get("plan_digest"),
                "output_plan_digest": metrics.get("output_plan_digest"),
                "cache_valid": metrics.get("cache_valid"),
                "invalidation_reason": metrics.get("invalidation_reason"),
                "cache_invalidation_fallback_attempted": metrics.get(
                    "cache_invalidation_fallback_attempted"
                ),
                "cache_invalidation_external_engine_invoked": metrics.get(
                    "cache_invalidation_external_engine_invoked"
                ),
                "cache_invalidation_claim_gate_status": metrics.get(
                    "cache_invalidation_claim_gate_status"
                ),
                "cache_invalidation_redaction_status": metrics.get(
                    "cache_invalidation_redaction_status"
                ),
                "cache_invalidation_claim_boundary": metrics.get(
                    "cache_invalidation_claim_boundary"
                ),
            }
        )
    return rows


def reuse_level_matrix(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for result in results:
        metrics = result["metrics"]
        if "reuse_level_contract_schema_version" not in metrics:
            continue
        rows.extend(
            reuse_level_rows_from_metrics(
                scenario=result["scenario_name"],
                engine=result["engine"],
                row_status=result["status"],
                metrics=metrics,
            )
        )
    return rows


def catalog_coverage_summary(catalog: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "scenario_id": scenario["id"],
            "scenario_name": scenario["name"],
            "suite": scenario["suite"],
            "scenario_category": scenario["category"],
            "executable_in_local_runner": bool(scenario["executable"]),
            "default": bool(scenario["default"]),
            "stress": bool(scenario["stress"]),
            "dataset_profiles": scenario.get("dataset_profiles", []),
        }
        for scenario in catalog["scenarios"]
    ]


def execution_mode_metadata(
    engine: str, data_format: str, evidence: dict[str, Any] | None = None
) -> dict[str, Any]:
    evidence = evidence or {}
    if engine == "shardloom":
        selected = "compatibility_import_certified"
        reason = "certified compatibility import/stage workflow"
        family = "compatibility"
        vortex_native_claim_allowed = False
        compatibility_import_included = True
        vortex_prepare_included = True
        vortex_write_reopen_included = True
        direct_transient_execution = False
    elif engine == "shardloom-direct-transient":
        selected = str(
            evidence.get("selected_execution_mode") or "direct_compatibility_transient"
        )
        reason = str(
            evidence.get("mode_selection_reason")
            or "direct transient local CSV smoke without Vortex persistence"
        )
        family = "compatibility"
        vortex_native_claim_allowed = False
        compatibility_import_included = False
        vortex_prepare_included = False
        vortex_write_reopen_included = False
        direct_transient_execution = True
    elif engine in ("shardloom-vortex", "shardloom-prepared-vortex"):
        selected = str(evidence.get("selected_execution_mode") or "prepared_vortex")
        reason = str(
            evidence.get("mode_selection_reason")
            or "prepared Vortex artifacts were created before scenario timing"
        )
        family = "native_vortex"
        vortex_native_claim_allowed = True
        compatibility_import_included = False
        vortex_prepare_included = False
        vortex_write_reopen_included = False
        direct_transient_execution = False
    elif data_format == SHARDLOOM_VORTEX_FORMAT:
        selected = "native_vortex"
        reason = "native Vortex format selected"
        family = "native_vortex"
        vortex_native_claim_allowed = True
        compatibility_import_included = False
        vortex_prepare_included = False
        vortex_write_reopen_included = False
        direct_transient_execution = False
    else:
        selected = "external_baseline_only"
        reason = "local comparison baseline; never ShardLoom runtime fallback"
        family = "external_baseline"
        vortex_native_claim_allowed = False
        compatibility_import_included = False
        vortex_prepare_included = False
        vortex_write_reopen_included = False
        direct_transient_execution = False

    requested = str(evidence.get("requested_execution_mode") or selected)
    return {
        "requested_execution_mode": requested,
        "selected_execution_mode": selected,
        "execution_mode": selected,
        "mode_selection_reason": reason,
        "execution_mode_family": family,
        "vortex_native_claim_allowed": vortex_native_claim_allowed,
        "compatibility_import_included": compatibility_import_included,
        "vortex_prepare_included": vortex_prepare_included,
        "vortex_write_reopen_included": vortex_write_reopen_included,
        "direct_transient_execution": direct_transient_execution,
        "claim_gate_status": str(evidence.get("claim_gate_status") or ""),
    }


def operator_blocker_metadata(
    engine: str, evidence: dict[str, Any] | None = None
) -> dict[str, Any]:
    evidence = evidence or {}
    if any(field in evidence for field in OPERATOR_BLOCKER_MATRIX_FIELDS):
        return {
            "operator_execution_class": str(
                evidence.get("operator_execution_class") or "unsupported"
            ),
            "operator_admission_status": str(
                evidence.get("operator_admission_status") or "unsupported"
            ),
            "operator_blocker_id": str(evidence.get("operator_blocker_id") or "missing"),
            "operator_blocker_reason": str(
                evidence.get("operator_blocker_reason") or "missing"
            ),
            "operator_encoded_native_claim_allowed": (
                parse_optional_bool(evidence.get("operator_encoded_native_claim_allowed"))
                is True
            ),
            "operator_residual_native_used": (
                parse_optional_bool(evidence.get("operator_residual_native_used")) is True
            ),
            "operator_temporary_materialization_used": (
                parse_optional_bool(evidence.get("operator_temporary_materialization_used"))
                is True
            ),
            "operator_blocker_matrix_ref": evidence.get("operator_blocker_matrix_ref"),
        }
    if engine == "shardloom-direct-transient":
        return {
            "operator_execution_class": "residual_native",
            "operator_admission_status": "direct_transient_scoped",
            "operator_blocker_id": "gar-flow-2b.not_prepared_native",
            "operator_blocker_reason": "direct transient is scoped ShardLoom-native compute, not prepared/native Vortex operator evidence",
            "operator_encoded_native_claim_allowed": False,
            "operator_residual_native_used": True,
            "operator_temporary_materialization_used": False,
            "operator_blocker_matrix_ref": "operator-blocker://traditional_analytics/direct_transient",
        }
    if is_shardloom_engine(engine):
        return {
            "operator_execution_class": "unsupported",
            "operator_admission_status": "not_executed",
            "operator_blocker_id": "gar-flow-2b.operator_evidence_missing",
            "operator_blocker_reason": "operator blocker evidence was not emitted",
            "operator_encoded_native_claim_allowed": False,
            "operator_residual_native_used": False,
            "operator_temporary_materialization_used": False,
            "operator_blocker_matrix_ref": None,
        }
    return {
        "operator_execution_class": "external_baseline_only",
        "operator_admission_status": "external_baseline_only",
        "operator_blocker_id": "external_baseline_only",
        "operator_blocker_reason": "external rows are comparison baselines, not ShardLoom operator evidence",
        "operator_encoded_native_claim_allowed": False,
        "operator_residual_native_used": False,
        "operator_temporary_materialization_used": False,
        "operator_blocker_matrix_ref": None,
    }


def work_avoidance_metric_field(metric: str, status: str, value: Any, reason: str) -> dict[str, Any]:
    if status not in WORK_AVOIDANCE_STATUS_VOCABULARY:
        raise ValueError(f"unknown work-avoidance status: {status}")
    return {
        f"work_avoidance_{metric}_status": status,
        f"work_avoidance_{metric}_value": value,
        f"work_avoidance_{metric}_reason": reason,
    }


def work_avoidance_metadata(
    engine: str,
    evidence: dict[str, Any] | None = None,
    row_status: str = "success",
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "work_avoidance_schema_ref": "gar-flow-2d.work_avoidance_evidence.v1",
        "work_avoidance_status_vocabulary": ",".join(WORK_AVOIDANCE_STATUS_VOCABULARY),
        "work_avoidance_claim_allowed": False,
        "work_avoidance_claim_boundary": (
            "missing or not_available work-avoidance metrics cannot support performance, "
            "superiority, Spark-displacement, or best-default claims"
        ),
    }
    evidence = evidence or {}
    if not is_shardloom_engine(engine):
        reason = "external baseline rows are comparison-only, not ShardLoom work-avoidance evidence"
        for metric in WORK_AVOIDANCE_METRICS:
            metadata.update(work_avoidance_metric_field(metric, "not_applicable", None, reason))
        return metadata
    if row_status != "success":
        reason = f"row status is {row_status}; no ShardLoom work-avoidance runtime evidence emitted"
        for metric in WORK_AVOIDANCE_METRICS:
            metadata.update(work_avoidance_metric_field(metric, "unsupported", None, reason))
        return metadata
    if engine == "shardloom-direct-transient":
        metadata.update(
            work_avoidance_metric_field(
                "rows_avoided",
                "not_available",
                None,
                "direct transient smoke does not count skipped rows",
            )
        )
        metadata.update(
            work_avoidance_metric_field(
                "segments_pruned",
                "not_applicable",
                None,
                "direct transient smoke does not scan Vortex segments",
            )
        )
        metadata.update(
            work_avoidance_metric_field(
                "bytes_avoided",
                "not_available",
                None,
                "direct transient smoke does not measure avoided bytes",
            )
        )
        metadata.update(
            work_avoidance_metric_field(
                "encoded_vector_reuse",
                "not_applicable",
                None,
                "direct transient smoke is not a Vortex encoded-vector path",
            )
        )
        metadata.update(
            work_avoidance_metric_field(
                "pushdown_proof",
                "not_applicable",
                None,
                "direct transient smoke has no Vortex Scan pushdown proof",
            )
        )
        return metadata

    filter_pushdown = evidence.get("streaming_filter_pushdown_applied")
    projection_pushdown = evidence.get("streaming_projection_pushdown_applied")
    pushdown_measured = filter_pushdown is not None or projection_pushdown is not None
    metadata.update(
        work_avoidance_metric_field(
            "rows_avoided",
            "not_available",
            None,
            "current traditional rows do not count skipped rows separately from rows scanned/materialized",
        )
    )
    metadata.update(
        work_avoidance_metric_field(
            "segments_pruned",
            "not_available",
            None,
            "current traditional rows do not emit pruned segment counts",
        )
    )
    metadata.update(
        work_avoidance_metric_field(
            "bytes_avoided",
            "not_available",
            None,
            "current traditional rows do not emit avoided byte counts",
        )
    )
    metadata.update(
        work_avoidance_metric_field(
            "encoded_vector_reuse",
            "not_available",
            None,
            "current traditional rows do not measure encoded-vector reuse as a standalone counter",
        )
    )
    metadata.update(
        work_avoidance_metric_field(
            "pushdown_proof",
            "measured" if pushdown_measured else "not_available",
            f"filter={filter_pushdown};projection={projection_pushdown}"
            if pushdown_measured
            else None,
            "streaming filter/projection pushdown fields are emitted by ShardLoom evidence"
            if pushdown_measured
            else "pushdown proof fields were not emitted for this row",
        )
    )
    return metadata


def native_work_avoidance_status(
    value: Any,
    known: Any,
    measured_reason: str,
    unknown_reason: str,
) -> tuple[str, Any, str]:
    if str(known).lower() == "true":
        return "measured", value, measured_reason
    if str(known).lower() == "false":
        return "not_available", value, unknown_reason
    if value not in (None, "", "n/a"):
        return "measured", value, measured_reason
    return "not_available", None, unknown_reason


def add_native_work_avoidance_schema(row: dict[str, Any]) -> dict[str, Any]:
    rows_status, rows_value, rows_reason = native_work_avoidance_status(
        row.get("work_avoided_rows_not_scanned"),
        row.get("work_avoided_rows_not_scanned_known"),
        "vortex-run emitted rows-not-scanned work-avoidance evidence",
        "vortex-run did not emit a rows-not-scanned count for this primitive",
    )
    segments_status, segments_value, segments_reason = native_work_avoidance_status(
        row.get("work_avoided_segments_pruned"),
        row.get("work_avoided_segments_pruned_known"),
        "vortex-run emitted segment-prune work-avoidance evidence",
        "vortex-run did not emit a pruned-segment count for this primitive",
    )
    bytes_status, bytes_value, bytes_reason = native_work_avoidance_status(
        row.get("work_avoided_bytes_not_read"),
        row.get("work_avoided_bytes_not_read_known"),
        "vortex-run emitted bytes-not-read work-avoidance evidence",
        "vortex-run did not emit an avoided-byte count for this primitive",
    )
    decode_value = row.get("work_avoided_decode_avoided")
    materialization_value = row.get("work_avoided_materialization_avoided")
    encoded_status = (
        "measured"
        if decode_value not in (None, "", "n/a") or materialization_value not in (None, "", "n/a")
        else "not_available"
    )
    pushdown_value = (
        f"filter={row.get('filter_pushdown_applied')};"
        f"projection={row.get('projection_pushdown_applied')}"
    )
    pushdown_status = (
        "measured"
        if row.get("filter_pushdown_applied") not in (None, "", "n/a")
        or row.get("projection_pushdown_applied") not in (None, "", "n/a")
        else "not_available"
    )
    row.update(
        {
            "work_avoidance_schema_ref": "gar-flow-2d.work_avoidance_evidence.v1",
            "work_avoidance_status_vocabulary": ",".join(WORK_AVOIDANCE_STATUS_VOCABULARY),
            "work_avoidance_claim_allowed": False,
            "work_avoidance_claim_boundary": (
                "native microbenchmark work-avoidance counters are scoped evidence, not "
                "performance or superiority claims"
            ),
        }
    )
    row.update(work_avoidance_metric_field("rows_avoided", rows_status, rows_value, rows_reason))
    row.update(
        work_avoidance_metric_field(
            "segments_pruned", segments_status, segments_value, segments_reason
        )
    )
    row.update(work_avoidance_metric_field("bytes_avoided", bytes_status, bytes_value, bytes_reason))
    row.update(
        work_avoidance_metric_field(
            "encoded_vector_reuse",
            encoded_status,
            f"decode_avoided={decode_value};materialization_avoided={materialization_value}"
            if encoded_status == "measured"
            else None,
            "decode/materialization avoidance fields emitted by vortex-run"
            if encoded_status == "measured"
            else "encoded-vector reuse was not measured for this native row",
        )
    )
    row.update(
        work_avoidance_metric_field(
            "pushdown_proof",
            pushdown_status,
            pushdown_value if pushdown_status == "measured" else None,
            "filter/projection pushdown fields emitted by vortex-run"
            if pushdown_status == "measured"
            else "pushdown evidence was not emitted for this native row",
        )
    )
    return row


def micros_to_millis_field(value: Any) -> float | None:
    micros = parse_optional_int(value)
    return None if micros is None else round(micros / 1000.0, 4)


def failed_result(
    engine: str,
    scenario: str,
    data_format: str,
    status: str,
    reason: str,
    paths: DatasetPaths,
    iterations: int,
    elapsed_millis: float | None = None,
) -> dict[str, Any]:
    execution_mode = execution_mode_metadata(engine, data_format)
    operator_metadata = operator_blocker_metadata(engine)
    work_avoidance = work_avoidance_metadata(engine, row_status=status)
    metrics = {
        "wall_time_millis": round(elapsed_millis, 4) if elapsed_millis is not None else None,
        "query_runtime_millis": round(elapsed_millis, 4) if elapsed_millis is not None else None,
        "total_runtime_millis": round(elapsed_millis, 4) if elapsed_millis is not None else None,
        "peak_memory_bytes": None,
        "bytes_read": scenario_bytes(paths, scenario, data_format),
        "bytes_written": None,
        "rows_scanned": rows_scanned(paths, scenario),
        "rows_materialized": 0,
        "data_decoded": None,
        "data_materialized": None,
        "row_read": None,
        "arrow_converted": None,
        "object_store_io": None,
        "write_io": None,
        "spill_io_performed": None,
        "object_store_requests": 0,
        "spill_required_bytes": None,
        "scenario_compute_millis": None,
        "computed_result_sink_write_millis": None,
        "result_sink_write_millis": None,
        "computed_result_sink_bytes": None,
        "operator_compute_millis": None,
        "cli_process_wall_millis": None,
        "python_harness_overhead_millis": None,
        "startup_warmup_millis": None,
        "build_time_millis": None,
        "preparation_millis": None,
        "preparation_cli_process_wall_millis": None,
        "preparation_included_in_timing": False,
        "prepared_artifact_ref": None,
        "prepared_artifact_digest": None,
        "source_read_millis": None,
        "compatibility_parse_millis": None,
        "compatibility_to_vortex_import_millis": None,
        "vortex_write_millis": None,
        "vortex_reopen_millis": None,
        "vortex_scan_millis": None,
        "evidence_render_millis": None,
        "build_time_excluded": True,
        "process_startup_attribution": "not_executed",
        "python_harness_overhead_status": "not_executed",
        "compatibility_to_vortex_included": execution_mode["compatibility_import_included"],
        "vortex_reopen_scan_included": execution_mode["vortex_write_reopen_included"],
        "result_sink_included": False,
        "representation_transition_summary": "not_executed",
        "encoded_native_execution_status": "not_executed",
        "fusion_status": "not_executed",
        "filter_project_limit_fused": False,
        "fusion_blocker": "not_executed",
        "fused_pipeline_schema_version": "not_executed",
        "fused_pipeline_report_id": "not_executed",
        "fused_pipeline_scope": "not_executed",
        "fused_pipeline_planned_family_count": 4,
        "fused_pipeline_family_statuses": "not_executed",
        "fused_pipeline_used": False,
        "fused_operator_family": "not_executed",
        "intermediate_materialization_avoided": False,
        "fused_pipeline_rows_scanned": None,
        "fused_pipeline_rows_selected": None,
        "fused_pipeline_rows_output": None,
        "fused_pipeline_filter_columns": "not_executed",
        "fused_pipeline_projection_columns": "not_executed",
        "fused_pipeline_selection_vector_consumed": "not_executed",
        "fused_pipeline_selection_vector_status": "not_executed",
        "fused_pipeline_correctness_digest_status": "not_executed",
        "fused_pipeline_unfused_correctness_digest": "not_available",
        "fused_pipeline_fused_correctness_digest": "not_available",
        "fused_pipeline_correctness_digest_match": False,
        "fused_pipeline_unfused_reference_status": "not_executed",
        "fused_pipeline_data_decoded": None,
        "fused_pipeline_data_materialized": None,
        "fused_pipeline_operator_execution_class": "not_executed",
        "fused_pipeline_encoded_native_claim_allowed": False,
        "fused_pipeline_claim_gate_status": "not_executed",
        "fused_pipeline_blocker_id": "not_executed",
        "fused_pipeline_blocker_reason": "not_executed",
        "fused_pipeline_claim_boundary": "not_executed",
        "fused_pipeline_fallback_attempted": False,
        "fused_pipeline_external_engine_invoked": False,
        "scan_pushdown_schema_version": "not_executed",
        "scan_pushdown_status": "not_executed",
        "scan_filter_pushed_down": False,
        "scan_projection_pushed_down": False,
        "scan_limit_pushed_down": False,
        "scan_filter_pushdown_status": "not_executed",
        "scan_projection_pushdown_status": "not_executed",
        "scan_limit_pushdown_status": "not_executed",
        "scan_filter_columns_read": "none",
        "scan_output_columns_read": "none",
        "scan_filter_only_columns_read": "none",
        "scan_data_materialized": None,
        "scan_data_decoded": None,
        "scan_pushdown_blocker_id": "not_executed",
        "scan_pushdown_claim_gate_status": "not_executed",
        "scan_pushdown_fallback_attempted": False,
        "scan_pushdown_external_engine_invoked": False,
        "materialization_required": None,
        "decode_required": None,
        "scan_api_status": "not_executed",
        "persistent_runner_status": "not_executed",
    }
    metrics.update(
        source_state_contract_metadata(
            engine,
            paths,
            scenario,
            data_format,
            status=status,
        )
    )
    metrics.update(
        prepared_state_contract_metadata(
            engine,
            scenario,
            data_format,
            status=status,
            metrics=metrics,
            selected_mode=execution_mode["selected_execution_mode"],
        )
    )
    metrics.update(
        output_plan_contract_metadata(
            engine,
            scenario,
            data_format,
            status=status,
            metrics=metrics,
        )
    )
    metrics.update(
        cache_invalidation_contract_metadata(
            engine,
            paths,
            scenario,
            data_format,
            status=status,
            metrics=metrics,
        )
    )
    metrics.update(
        reuse_level_contract_metadata(
            scenario=scenario,
            engine=engine,
            status=status,
            metrics=metrics,
        )
    )
    metrics.update(optimizer_trace_contract_metadata(engine))
    metrics.update(build_profile_contract_metadata(engine))
    return {
        "scenario_name": scenario_display_name(data_format, scenario),
        "scenario_base": scenario,
        "storage_format": data_format,
        "engine": engine,
        "status": status,
        "reason": reason,
        "iterations": iterations,
        "iteration_wall_time_millis": [] if elapsed_millis is None else [round(elapsed_millis, 4)],
        "metrics": metrics,
        "correctness_digest": None,
        "correctness_digest_stable": False,
        "output_preview": None,
        "shardloom_evidence": {},
        "fallback_attempted": False,
        "external_baseline_only": not is_shardloom_engine(engine),
        **work_avoidance,
        **operator_metadata,
        **execution_mode,
    }


def successful_result_from_iterations(
    runner: EngineRunner,
    paths: DatasetPaths,
    scenario: str,
    data_format: str,
    iterations: int,
    values: list[Any],
    evidence_rows: list[dict[str, str]],
    timings: list[float],
    peak_memory: list[int],
) -> dict[str, Any]:
    digest = canonical_digest(values[-1])
    stable = all(canonical_digest(value) == digest for value in values)
    evidence = evidence_rows[-1] if evidence_rows else {}

    def mean_evidence_micros(field: str) -> float | None:
        values = [
            parsed
            for row in evidence_rows
            if row
            for parsed in [parse_optional_int(row.get(field))]
            if parsed is not None
        ]
        return None if not values else round(statistics.mean(values) / 1000.0, 4)

    def mean_evidence_float(field: str) -> float | None:
        values = [
            parsed
            for row in evidence_rows
            if row
            for parsed in [parse_optional_float(row.get(field))]
            if parsed is not None
        ]
        return None if not values else round(statistics.mean(values), 4)

    bytes_written = None
    computed_result_sink_bytes = None
    scenario_compute_millis = None
    computed_result_sink_write_millis = None
    source_read_millis = None
    compatibility_parse_millis = None
    compatibility_to_vortex_import_millis = None
    vortex_write_millis = None
    vortex_reopen_millis = None
    vortex_scan_millis = None
    operator_compute_millis = None
    evidence_render_millis = None
    preparation_millis = None
    preparation_cli_process_wall_millis = None
    preparation_included_in_timing = False
    prepared_artifact_ref = None
    prepared_artifact_digest = None
    cli_process_wall_millis = None
    if evidence:
        fact_vortex_bytes = parse_optional_int(evidence.get("fact_vortex_bytes"))
        dim_vortex_bytes = parse_optional_int(evidence.get("dim_vortex_bytes"))
        cdc_delta_vortex_bytes = parse_optional_int(evidence.get("cdc_delta_vortex_bytes"))
        computed_result_sink_bytes = parse_optional_int(
            evidence.get("computed_result_vortex_bytes")
        )
        if (
            fact_vortex_bytes is not None
            or dim_vortex_bytes is not None
            or cdc_delta_vortex_bytes is not None
            or computed_result_sink_bytes is not None
        ):
            bytes_written = (
                (fact_vortex_bytes or 0)
                + (dim_vortex_bytes or 0)
                + (cdc_delta_vortex_bytes or 0)
                + (computed_result_sink_bytes or 0)
            )
        scenario_compute_millis = mean_evidence_micros("scenario_compute_micros")
        computed_result_sink_write_millis = mean_evidence_micros(
            "computed_result_sink_write_micros"
        )
        if scenario_compute_millis is not None:
            operator_compute_millis = scenario_compute_millis
        source_read_millis = mean_evidence_micros("source_read_micros")
        compatibility_parse_millis = mean_evidence_micros("compatibility_parse_micros")
        compatibility_to_vortex_import_millis = mean_evidence_micros(
            "compatibility_to_vortex_import_micros"
        )
        vortex_write_millis = mean_evidence_micros("vortex_write_micros")
        vortex_reopen_millis = mean_evidence_micros("vortex_reopen_micros")
        vortex_scan_millis = mean_evidence_micros("vortex_scan_micros")
        evidence_render_millis = mean_evidence_micros("evidence_render_micros")
        preparation_millis = parse_optional_float(evidence.get("preparation_millis"))
        preparation_cli_process_wall_millis = parse_optional_float(
            evidence.get("preparation_cli_process_wall_millis")
        )
        preparation_included_in_timing = (
            parse_optional_bool(evidence.get("preparation_included_in_timing")) is True
        )
        prepared_artifact_ref = evidence.get("prepared_artifact_ref")
        prepared_artifact_digest = evidence.get("prepared_artifact_digest")
        cli_process_wall_millis = mean_evidence_float("cli_process_wall_millis")
    bytes_read = parse_optional_int(evidence.get("source_bytes_read")) if evidence else None
    execution_mode = execution_mode_metadata(runner.name, data_format, evidence)
    operator_metadata = operator_blocker_metadata(runner.name, evidence)
    work_avoidance = work_avoidance_metadata(runner.name, evidence)
    result_sink_included = computed_result_sink_write_millis is not None
    query_runtime_millis = round(statistics.mean(timings), 4)
    if evidence.get("persistent_runner_status") == BATCH_RUNNER_STATUS:
        python_harness_overhead_millis = None
    else:
        python_harness_overhead_millis = (
            round(max(0.0, query_runtime_millis - cli_process_wall_millis), 4)
            if cli_process_wall_millis is not None
            else None
        )
    filter_project_limit_fused = (
        scenario == "filter + projection + limit"
        and parse_optional_bool(evidence.get("streaming_filter_pushdown_applied")) is True
        and parse_optional_bool(evidence.get("streaming_projection_pushdown_applied")) is True
        and parse_optional_bool(evidence.get("data_materialized")) is False
    )
    fusion_blocker = str(
        evidence.get(
            "fusion_blocker",
            "none"
            if filter_project_limit_fused
            else "temporary benchmark operator materializes Vortex-derived arrays after scan",
        )
    )
    encoded_native_execution_status = str(
        evidence.get(
            "encoded_native_execution_status",
            "materialized_vortex_derived_arrays"
            if parse_optional_bool(evidence.get("data_materialized")) is True
            else "streaming_or_native_vortex_evidence_present",
        )
    )
    scan_api_status = str(
        evidence.get(
            "scan_api_status",
            "direct_transient_no_vortex_scan"
            if runner.name == "shardloom-direct-transient"
            else "local_file_reopen_scan_path",
        )
    )
    metrics = {
        "wall_time_millis": round(sum(timings), 4),
        "query_runtime_millis": query_runtime_millis,
        "total_runtime_millis": query_runtime_millis,
        "peak_memory_bytes": max(peak_memory) if peak_memory else None,
        "bytes_read": bytes_read
        if bytes_read is not None
        else scenario_bytes(paths, scenario, data_format),
        "bytes_written": bytes_written,
        "rows_scanned": rows_scanned(paths, scenario),
        "rows_materialized": parse_optional_int(evidence.get("rows_materialized"))
        if evidence
        else rows_materialized(values[-1]),
        "data_decoded": parse_optional_bool(evidence.get("data_decoded")),
        "data_materialized": parse_optional_bool(evidence.get("data_materialized")),
        "row_read": parse_optional_bool(evidence.get("row_read")),
        "arrow_converted": parse_optional_bool(evidence.get("arrow_converted")),
        "object_store_io": parse_optional_bool(evidence.get("object_store_io")),
        "write_io": parse_optional_bool(evidence.get("write_io")),
        "spill_io_performed": parse_optional_bool(evidence.get("spill_io_performed")),
        "object_store_requests": 0,
        "spill_required_bytes": None,
        "scenario_compute_millis": scenario_compute_millis,
        "operator_compute_millis": operator_compute_millis,
        "cli_process_wall_millis": cli_process_wall_millis,
        "python_harness_overhead_millis": python_harness_overhead_millis,
        "computed_result_sink_write_millis": computed_result_sink_write_millis,
        "result_sink_write_millis": computed_result_sink_write_millis,
        "computed_result_sink_bytes": computed_result_sink_bytes,
        "startup_warmup_millis": runner.startup_time_millis,
        "build_time_millis": runner.build_time_millis,
        "preparation_millis": preparation_millis
        if preparation_millis is not None
        else runner.preparation_time_millis,
        "preparation_cli_process_wall_millis": preparation_cli_process_wall_millis,
        "preparation_included_in_timing": preparation_included_in_timing,
        "prepared_artifact_ref": prepared_artifact_ref,
        "prepared_artifact_digest": prepared_artifact_digest,
        "source_read_millis": source_read_millis,
        "compatibility_parse_millis": compatibility_parse_millis,
        "compatibility_to_vortex_import_millis": compatibility_to_vortex_import_millis,
        "vortex_write_millis": vortex_write_millis,
        "vortex_reopen_millis": vortex_reopen_millis,
        "vortex_scan_millis": vortex_scan_millis,
        "evidence_render_millis": evidence_render_millis,
        "build_time_excluded": True,
        "process_startup_attribution": evidence.get(
            "process_startup_attribution", "not_measured"
        ),
        "python_harness_overhead_status": evidence.get(
            "python_harness_overhead_status", "not_measured"
        ),
        "compatibility_to_vortex_included": execution_mode[
            "compatibility_import_included"
        ],
        "vortex_reopen_scan_included": (
            execution_mode["vortex_write_reopen_included"]
            or vortex_scan_millis is not None
        ),
        "result_sink_included": result_sink_included,
        "representation_transition_summary": evidence.get(
            "native_io_representation_transitions", "not_reported"
        ),
        "encoded_native_execution_status": encoded_native_execution_status,
        "fusion_status": (
            "filter_project_limit_fused=true"
            if filter_project_limit_fused
            else "not_fused_or_not_applicable"
        ),
        "filter_project_limit_fused": filter_project_limit_fused,
        "fusion_blocker": fusion_blocker,
        "fused_pipeline_schema_version": evidence.get(
            "fused_pipeline_schema_version", "not_reported"
        ),
        "fused_pipeline_report_id": evidence.get(
            "fused_pipeline_report_id", "not_reported"
        ),
        "fused_pipeline_scope": evidence.get(
            "fused_pipeline_scope", "not_reported"
        ),
        "fused_pipeline_planned_family_count": parse_optional_int(
            evidence.get("fused_pipeline_planned_family_count")
        ),
        "fused_pipeline_family_statuses": evidence.get(
            "fused_pipeline_family_statuses", "not_reported"
        ),
        "fused_pipeline_used": (
            parse_optional_bool(evidence.get("fused_pipeline_used")) is True
        ),
        "fused_operator_family": evidence.get("fused_operator_family", "not_reported"),
        "intermediate_materialization_avoided": (
            parse_optional_bool(evidence.get("intermediate_materialization_avoided")) is True
        ),
        "fused_pipeline_rows_scanned": parse_optional_int(
            evidence.get("fused_pipeline_rows_scanned")
        ),
        "fused_pipeline_rows_selected": parse_optional_int(
            evidence.get("fused_pipeline_rows_selected")
        ),
        "fused_pipeline_rows_output": parse_optional_int(
            evidence.get("fused_pipeline_rows_output")
        ),
        "fused_pipeline_filter_columns": evidence.get(
            "fused_pipeline_filter_columns", "not_reported"
        ),
        "fused_pipeline_projection_columns": evidence.get(
            "fused_pipeline_projection_columns", "not_reported"
        ),
        "fused_pipeline_selection_vector_consumed": evidence.get(
            "fused_pipeline_selection_vector_consumed", "not_reported"
        ),
        "fused_pipeline_selection_vector_status": evidence.get(
            "fused_pipeline_selection_vector_status", "not_reported"
        ),
        "fused_pipeline_correctness_digest_status": evidence.get(
            "fused_pipeline_correctness_digest_status", "not_reported"
        ),
        "fused_pipeline_unfused_correctness_digest": evidence.get(
            "fused_pipeline_unfused_correctness_digest", "not_reported"
        ),
        "fused_pipeline_fused_correctness_digest": evidence.get(
            "fused_pipeline_fused_correctness_digest", "not_reported"
        ),
        "fused_pipeline_correctness_digest_match": (
            parse_optional_bool(evidence.get("fused_pipeline_correctness_digest_match"))
            is True
        ),
        "fused_pipeline_unfused_reference_status": evidence.get(
            "fused_pipeline_unfused_reference_status", "not_reported"
        ),
        "fused_pipeline_data_decoded": parse_optional_bool(
            evidence.get("fused_pipeline_data_decoded")
        ),
        "fused_pipeline_data_materialized": parse_optional_bool(
            evidence.get("fused_pipeline_data_materialized")
        ),
        "fused_pipeline_operator_execution_class": evidence.get(
            "fused_pipeline_operator_execution_class", "not_reported"
        ),
        "fused_pipeline_encoded_native_claim_allowed": (
            parse_optional_bool(evidence.get("fused_pipeline_encoded_native_claim_allowed"))
            is True
        ),
        "fused_pipeline_claim_gate_status": evidence.get(
            "fused_pipeline_claim_gate_status", "not_reported"
        ),
        "fused_pipeline_blocker_id": evidence.get(
            "fused_pipeline_blocker_id", "not_reported"
        ),
        "fused_pipeline_blocker_reason": evidence.get(
            "fused_pipeline_blocker_reason", "not_reported"
        ),
        "fused_pipeline_claim_boundary": evidence.get(
            "fused_pipeline_claim_boundary", "not_reported"
        ),
        "fused_pipeline_fallback_attempted": (
            parse_optional_bool(evidence.get("fused_pipeline_fallback_attempted")) is True
        ),
        "fused_pipeline_external_engine_invoked": (
            parse_optional_bool(evidence.get("fused_pipeline_external_engine_invoked")) is True
        ),
        "compressed_kernel_registry_schema_version": evidence.get(
            "compressed_kernel_registry_schema_version", "not_reported"
        ),
        "compressed_kernel_registry_scope": evidence.get(
            "compressed_kernel_registry_scope", "not_reported"
        ),
        "compressed_kernel_registry_current_surface": evidence.get(
            "compressed_kernel_registry_current_surface", "not_reported"
        ),
        "compressed_kernel_registry_vortex_first_decision": evidence.get(
            "compressed_kernel_registry_vortex_first_decision", "not_reported"
        ),
        "compressed_kernel_registry_initial_pair_count": parse_optional_int(
            evidence.get("compressed_kernel_registry_initial_pair_count")
        ),
        "compressed_kernel_registry_pairs_classified": (
            parse_optional_bool(evidence.get("compressed_kernel_registry_pairs_classified"))
            is True
        ),
        "compressed_kernel_registry_pair_ids": evidence.get(
            "compressed_kernel_registry_pair_ids", "none"
        ),
        "compressed_kernel_registry_pair_statuses": evidence.get(
            "compressed_kernel_registry_pair_statuses", "none"
        ),
        "compressed_kernel_registry_encoding_ids": evidence.get(
            "compressed_kernel_registry_encoding_ids", "none"
        ),
        "compressed_kernel_registry_logical_dtypes": evidence.get(
            "compressed_kernel_registry_logical_dtypes", "none"
        ),
        "compressed_kernel_registry_physical_encodings": evidence.get(
            "compressed_kernel_registry_physical_encodings", "none"
        ),
        "compressed_kernel_registry_operator_families": evidence.get(
            "compressed_kernel_registry_operator_families", "none"
        ),
        "compressed_kernel_registry_kernel_admitted": evidence.get(
            "compressed_kernel_registry_kernel_admitted", "none"
        ),
        "compressed_kernel_registry_kernel_executed": evidence.get(
            "compressed_kernel_registry_kernel_executed", "none"
        ),
        "compressed_kernel_registry_canonicalization_required": evidence.get(
            "compressed_kernel_registry_canonicalization_required", "none"
        ),
        "compressed_kernel_registry_decoded": evidence.get(
            "compressed_kernel_registry_decoded", "none"
        ),
        "compressed_kernel_registry_materialized": evidence.get(
            "compressed_kernel_registry_materialized", "none"
        ),
        "compressed_kernel_registry_selection_vector_emitted": evidence.get(
            "compressed_kernel_registry_selection_vector_emitted", "none"
        ),
        "compressed_kernel_registry_validity_semantics": evidence.get(
            "compressed_kernel_registry_validity_semantics", "none"
        ),
        "compressed_kernel_registry_unsupported_kernel_reasons": evidence.get(
            "compressed_kernel_registry_unsupported_kernel_reasons", "none"
        ),
        "compressed_kernel_registry_encoded_native_claim_allowed": (
            parse_optional_bool(
                evidence.get("compressed_kernel_registry_encoded_native_claim_allowed")
            )
            is True
        ),
        "compressed_kernel_registry_admitted_pair_count": parse_optional_int(
            evidence.get("compressed_kernel_registry_admitted_pair_count")
        ),
        "compressed_kernel_registry_executed_pair_count": parse_optional_int(
            evidence.get("compressed_kernel_registry_executed_pair_count")
        ),
        "compressed_kernel_registry_blocked_pair_count": parse_optional_int(
            evidence.get("compressed_kernel_registry_blocked_pair_count")
        ),
        "compressed_kernel_registry_not_available_pair_count": parse_optional_int(
            evidence.get("compressed_kernel_registry_not_available_pair_count")
        ),
        "compressed_kernel_registry_claim_gate_status": evidence.get(
            "compressed_kernel_registry_claim_gate_status", "not_reported"
        ),
        "compressed_kernel_registry_fallback_attempted": (
            parse_optional_bool(evidence.get("compressed_kernel_registry_fallback_attempted"))
            is True
        ),
        "compressed_kernel_registry_external_engine_invoked": (
            parse_optional_bool(
                evidence.get("compressed_kernel_registry_external_engine_invoked")
            )
            is True
        ),
        "scan_pushdown_schema_version": evidence.get(
            "scan_pushdown_schema_version", "not_reported"
        ),
        "scan_pushdown_status": evidence.get("scan_pushdown_status", "not_reported"),
        "scan_filter_pushed_down": (
            parse_optional_bool(evidence.get("scan_filter_pushed_down")) is True
        ),
        "scan_projection_pushed_down": (
            parse_optional_bool(evidence.get("scan_projection_pushed_down")) is True
        ),
        "scan_limit_pushed_down": (
            parse_optional_bool(evidence.get("scan_limit_pushed_down")) is True
        ),
        "scan_filter_pushdown_status": evidence.get(
            "scan_filter_pushdown_status", "not_reported"
        ),
        "scan_projection_pushdown_status": evidence.get(
            "scan_projection_pushdown_status", "not_reported"
        ),
        "scan_limit_pushdown_status": evidence.get(
            "scan_limit_pushdown_status", "not_reported"
        ),
        "scan_filter_columns_read": evidence.get("scan_filter_columns_read", "none"),
        "scan_output_columns_read": evidence.get("scan_output_columns_read", "none"),
        "scan_filter_only_columns_read": evidence.get(
            "scan_filter_only_columns_read", "none"
        ),
        "scan_data_materialized": parse_optional_bool(
            evidence.get("scan_data_materialized")
        ),
        "scan_data_decoded": parse_optional_bool(evidence.get("scan_data_decoded")),
        "scan_pushdown_blocker_id": evidence.get(
            "scan_pushdown_blocker_id", "not_reported"
        ),
        "scan_pushdown_claim_gate_status": evidence.get(
            "scan_pushdown_claim_gate_status", "not_reported"
        ),
        "scan_pushdown_fallback_attempted": (
            parse_optional_bool(evidence.get("scan_pushdown_fallback_attempted")) is True
        ),
        "scan_pushdown_external_engine_invoked": (
            parse_optional_bool(evidence.get("scan_pushdown_external_engine_invoked")) is True
        ),
        "materialization_required": parse_optional_bool(
            evidence.get("data_materialized")
        ),
        "decode_required": parse_optional_bool(evidence.get("data_decoded")),
        "scan_api_status": scan_api_status,
        "persistent_runner_status": evidence.get(
            "persistent_runner_status", PERSISTENT_RUNNER_STATUS
        ),
    }
    metrics.update(
        source_state_contract_metadata(
            runner.name,
            paths,
            scenario,
            data_format,
            status="success" if stable else "unstable_output",
            evidence=evidence,
            source_parse_millis=compatibility_parse_millis,
        )
    )
    metrics.update(
        prepared_state_contract_metadata(
            runner.name,
            scenario,
            data_format,
            status="success" if stable else "unstable_output",
            metrics=metrics,
            evidence=evidence,
            selected_mode=execution_mode["selected_execution_mode"],
        )
    )
    metrics.update(
        output_plan_contract_metadata(
            runner.name,
            scenario,
            data_format,
            status="success" if stable else "unstable_output",
            metrics=metrics,
            evidence=evidence,
        )
    )
    metrics.update(
        cache_invalidation_contract_metadata(
            runner.name,
            paths,
            scenario,
            data_format,
            status="success" if stable else "unstable_output",
            metrics=metrics,
            evidence=evidence,
        )
    )
    metrics.update(
        reuse_level_contract_metadata(
            scenario=scenario,
            engine=runner.name,
            status="success" if stable else "unstable_output",
            metrics=metrics,
        )
    )
    metrics.update(optimizer_trace_contract_metadata(runner.name))
    metrics.update(build_profile_contract_metadata(runner.name, digest if stable else None))
    if evidence.get("persistent_runner_status") == BATCH_RUNNER_STATUS:
        metrics.update(
            {
                "batch_runner_kind": evidence.get("batch_runner_kind"),
                "batch_scenario_count": parse_optional_int(
                    evidence.get("batch_scenario_count")
                ),
                "batch_cli_process_wall_millis": parse_optional_float(
                    evidence.get("batch_cli_process_wall_millis")
                ),
                "batch_process_wall_shared": parse_optional_bool(
                    evidence.get("batch_process_wall_shared")
                ),
                "batch_process_startup_attribution": evidence.get(
                    "batch_process_startup_attribution"
                ),
            }
        )
        for field in BATCH_RUNNER_ADMISSION_FIELDS:
            metrics.setdefault(field, evidence.get(field))
        for field in SESSION_RUNTIME_FIELDS:
            value = evidence.get(field)
            if field.endswith("_count") or field.endswith("_entry_count"):
                metrics.setdefault(field, parse_optional_int(value))
            elif field in (
                "session_lifecycle_explicit_close_required",
                "session_hidden_global_cache",
                "session_daemon_or_service",
                "session_fallback_attempted",
                "session_external_engine_invoked",
            ):
                metrics.setdefault(field, parse_optional_bool(value))
            else:
                metrics.setdefault(field, value)
        for field in ALLOCATION_RESOURCE_PROFILE_FIELDS:
            value = evidence.get(field)
            if field in ("allocation_count", "buffer_reuse_count"):
                metrics.setdefault(field, parse_optional_int(value))
            elif field in (
                "buffer_pool_enabled",
                "unsafe_lifetime_shortcut_used",
                "allocation_fallback_attempted",
                "allocation_external_engine_invoked",
            ):
                metrics.setdefault(field, parse_optional_bool(value))
            else:
                metrics.setdefault(field, value)
        for field in RUNTIME_EVIDENCE_LEVEL_FIELDS:
            value = evidence.get(field)
            if field in (
                "evidence_level_result_sink_replay_required",
                "evidence_level_result_sink_replay_requested",
                "evidence_level_result_sink_replay_verified",
                "evidence_level_native_io_certificate_required",
                "evidence_level_fallback_attempted",
                "evidence_level_external_engine_invoked",
            ):
                metrics.setdefault(field, parse_optional_bool(value))
            else:
                metrics.setdefault(field, value)
    return {
        "scenario_name": scenario_display_name(data_format, scenario),
        "scenario_base": scenario,
        "storage_format": data_format,
        "engine": runner.name,
        "status": "success" if stable else "unstable_output",
        "iterations": iterations,
        "iteration_wall_time_millis": [round(value, 4) for value in timings],
        "metrics": metrics,
        "correctness_digest": digest,
        "correctness_digest_stable": stable,
        "output_preview": values[-1] if not isinstance(values[-1], list) else values[-1][:5],
        "shardloom_evidence": evidence,
        "fallback_attempted": False,
        "external_baseline_only": not is_shardloom_engine(runner.name),
        **work_avoidance,
        **operator_metadata,
        **execution_mode,
    }


def run_one(
    runner: EngineRunner,
    paths: DatasetPaths,
    scenario: str,
    data_format: str,
    iterations: int,
) -> dict[str, Any]:
    scenario_fn = runner.scenarios.get(scenario)
    if scenario_fn is None:
        return failed_result(
            runner.name,
            scenario,
            data_format,
            "unsupported",
            f"{runner.name} does not implement benchmark scenario: {scenario}",
            paths,
            iterations,
        )
    values = []
    evidence_rows = []
    timings = []
    peak_memory = []
    for _ in range(iterations):
        started = time.perf_counter()
        with MemorySampler() as sampler:
            try:
                value, evidence = unwrap_engine_value(scenario_fn(paths, data_format))
            except BenchmarkUnsupported as exc:
                elapsed = (time.perf_counter() - started) * 1000.0
                return failed_result(
                    runner.name,
                    scenario,
                    data_format,
                    "unsupported",
                    str(exc),
                    paths,
                    iterations,
                    elapsed,
                )
            except Exception as exc:
                elapsed = (time.perf_counter() - started) * 1000.0
                return failed_result(
                    runner.name,
                    scenario,
                    data_format,
                    "execution_error",
                    f"{type(exc).__name__}: {exc}",
                    paths,
                    iterations,
                    elapsed,
                )
            else:
                elapsed = time.perf_counter() - started
        values.append(value)
        evidence_rows.append(evidence)
        timings.append(elapsed * 1000.0)
        if sampler.peak_bytes is not None:
            peak_memory.append(sampler.peak_bytes)

    return successful_result_from_iterations(
        runner,
        paths,
        scenario,
        data_format,
        iterations,
        values,
        evidence_rows,
        timings,
        peak_memory,
    )


def run_batch(
    runner: EngineRunner,
    paths: DatasetPaths,
    scenarios: tuple[str, ...],
    data_format: str,
    iterations: int,
) -> list[dict[str, Any]]:
    if runner.batch_scenarios is None:
        raise BenchmarkUnsupported(f"{runner.name} does not expose batch execution")
    values_by_scenario: dict[str, list[Any]] = {scenario: [] for scenario in scenarios}
    evidence_by_scenario: dict[str, list[dict[str, str]]] = {
        scenario: [] for scenario in scenarios
    }
    timings_by_scenario: dict[str, list[float]] = {scenario: [] for scenario in scenarios}
    peak_memory_by_scenario: dict[str, list[int]] = {scenario: [] for scenario in scenarios}
    for _ in range(iterations):
        with MemorySampler() as sampler:
            outputs = runner.batch_scenarios(paths, data_format, scenarios)
        for scenario in scenarios:
            if scenario not in outputs:
                raise BenchmarkUnsupported(
                    f"{runner.name} batch output omitted scenario: {scenario}"
                )
            value, evidence = unwrap_engine_value(outputs[scenario])
            values_by_scenario[scenario].append(value)
            evidence_by_scenario[scenario].append(evidence)
            scenario_compute_millis = micros_to_millis_field(
                evidence.get("scenario_compute_micros")
            )
            timings_by_scenario[scenario].append(
                scenario_compute_millis
                if scenario_compute_millis is not None
                else parse_optional_float(evidence.get("batch_cli_process_wall_millis"))
                or 0.0
            )
            if sampler.peak_bytes is not None:
                peak_memory_by_scenario[scenario].append(sampler.peak_bytes)
    return [
        successful_result_from_iterations(
            runner,
            paths,
            scenario,
            data_format,
            iterations,
            values_by_scenario[scenario],
            evidence_by_scenario[scenario],
            timings_by_scenario[scenario],
            peak_memory_by_scenario[scenario],
        )
        for scenario in scenarios
    ]


def native_microbenchmark_spec(primitive_family: str) -> dict[str, str]:
    return NATIVE_MICROBENCHMARK_REQUIRED_FAMILIES.get(
        primitive_family,
        {
            "name": primitive_family.replace("_", " "),
            "primitive": primitive_family,
            "subsystem": "native_microbenchmark",
            "optimization_question": "What subsystem work is isolated by this native row?",
            "support_status": "smoke_supported",
            "reason": "non-required native microbenchmark row",
        },
    )


def add_native_microbenchmark_contract(
    row: dict[str, Any],
    primitive_family: str,
    *,
    support_status: str | None = None,
    unsupported_reason: str | None = None,
) -> dict[str, Any]:
    spec = native_microbenchmark_spec(primitive_family)
    status = str(row.get("status", "unknown"))
    support = support_status or spec["support_status"]
    reason = unsupported_reason or row.get("reason") or spec["reason"]
    row.update(
        {
            "native_microbenchmark_schema_version": NATIVE_MICROBENCHMARK_SCHEMA_VERSION,
            "benchmark_category": "native_microbenchmark",
            "native_microbenchmark_primitive_family": primitive_family,
            "native_microbenchmark_subsystem": spec["subsystem"],
            "native_microbenchmark_optimization_question": spec["optimization_question"],
            "native_microbenchmark_support_status": support,
            "native_microbenchmark_claim_boundary": NATIVE_MICROBENCHMARK_CLAIM_BOUNDARY,
            "external_engine_invoked": "false",
            "performance_claim_allowed": "false",
        }
    )
    row.setdefault("primitive", spec["primitive"])
    row.setdefault("claim_gate_status", "not_claim_grade")
    row.setdefault("fallback_attempted", "false")
    if status != "success" or support in {"blocked", "unsupported"}:
        row["unsupported_reason"] = str(reason)
    else:
        row.setdefault("unsupported_reason", "none")
    row.setdefault("rows_scanned", row.get("rows"))
    row.setdefault("rows_selected", row.get("rows"))
    if str(row.get("data_materialized", "")).lower() == "true":
        row.setdefault("rows_materialized", row.get("rows"))
    else:
        row.setdefault("rows_materialized", "0")
    row = add_native_work_avoidance_schema(row)
    if status != "success" or support in {"blocked", "unsupported"}:
        for metric in WORK_AVOIDANCE_METRICS:
            row.update(
                work_avoidance_metric_field(
                    metric,
                    "unsupported",
                    None,
                    f"native microbenchmark status is {status}: {reason}",
                )
            )
    return row


def native_microbenchmark_blocked_row(
    primitive_family: str,
    *,
    status: str = "blocked",
    reason: str | None = None,
    command: list[str] | None = None,
) -> dict[str, Any]:
    spec = native_microbenchmark_spec(primitive_family)
    return native_microbenchmark_error(
        spec["name"],
        status,
        reason or spec["reason"],
        command=command,
        primitive_family=primitive_family,
        primitive=spec["primitive"],
        support_status=spec["support_status"],
    )


def native_microbenchmark_suite_unavailable_rows(
    status: str,
    reason: str,
) -> list[dict[str, Any]]:
    return [
        native_microbenchmark_blocked_row(
            primitive_family,
            status=status,
            reason=reason,
        )
        for primitive_family in NATIVE_MICROBENCHMARK_REQUIRED_FAMILIES
    ]


def append_required_native_microbenchmark_blockers(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    covered = {
        str(row.get("native_microbenchmark_primitive_family"))
        for row in rows
        if str(row.get("native_microbenchmark_support_status")) == "smoke_supported"
    }
    for primitive_family, spec in NATIVE_MICROBENCHMARK_REQUIRED_FAMILIES.items():
        if primitive_family not in covered and spec["support_status"] != "smoke_supported":
            rows.append(native_microbenchmark_blocked_row(primitive_family))
    return rows


def run_native_evidence_render_microbenchmark(
    rows: list[dict[str, Any]],
    iterations: int,
) -> dict[str, Any]:
    timings: list[float] = []
    sample_rows = [dict(row) for row in rows]
    sample_artifact = {"shardloom_native_microbenchmarks": sample_rows}
    rendered = ""
    for _ in range(iterations):
        started = time.perf_counter()
        json.dumps(sample_artifact, sort_keys=True)
        rendered = render_shardloom_native_table(sample_artifact)
        timings.append((time.perf_counter() - started) * 1000.0)
    return add_native_microbenchmark_contract(
        {
            "name": "evidence render",
            "status": "success",
            "dataset": "native microbenchmark row set",
            "primitive": "evidence_render",
            "rows": str(len(sample_rows)),
            "iterations": str(iterations),
            "query_runtime_millis": round(statistics.mean(timings), 4),
            "timing_scope": "average benchmark harness JSON serialization plus Markdown render",
            "comparison_status": "not_applicable",
            "claim_gate_status": "not_claim_grade",
            "rendered_markdown_bytes": str(len(rendered.encode("utf-8"))),
            "data_read": "false",
            "data_decoded": "false",
            "data_materialized": "false",
            "row_read": "false",
            "arrow_converted": "false",
            "materialization_boundary_reported": "false",
            "fallback_attempted": "false",
        },
        "evidence_render",
    )


def run_shardloom_native_microbenchmarks(iterations: int) -> list[dict[str, Any]]:
    root = workspace_root()
    fixture = root / "shardloom-vortex" / "tests" / "fixtures" / "metadata_footer_u64_20000.vortex"
    if not fixture.exists():
        return native_microbenchmark_suite_unavailable_rows(
            "missing_fixture",
            f"Vortex fixture was not found at {fixture}",
        )
    try:
        binary = build_shardloom_cli(
            root,
            "vortex-traditional-analytics-benchmark",
            SHARDLOOM_BUILD_PROFILE,
        )
    except BenchmarkUnsupported as exc:
        return native_microbenchmark_suite_unavailable_rows("build_error", str(exc))
    env = os.environ.copy()
    env["RUSTUP_TOOLCHAIN"] = env.get("RUSTUP_TOOLCHAIN", "1.91.1")
    rows = [
        run_shardloom_count_microbenchmark(root, env, binary, fixture, iterations),
        run_shardloom_vortex_run_microbenchmark(
            root,
            env,
            binary,
            fixture,
            iterations,
            "local primitive count",
            "count",
            "vortex_count_proxy",
        ),
        run_shardloom_vortex_run_microbenchmark(
            root,
            env,
            binary,
            fixture,
            iterations,
            "local primitive projection",
            "project:value",
            "projection_only",
        ),
        run_shardloom_vortex_run_microbenchmark(
            root,
            env,
            binary,
            fixture,
            iterations,
            "local primitive validity count",
            "count-where:is_not_null:value",
            "filter_predicate_only",
        ),
        run_shardloom_vortex_run_microbenchmark(
            root,
            env,
            binary,
            fixture,
            iterations,
            "local primitive comparison count",
            "count-where:gte:value:10000",
            "filter_predicate_only",
        ),
        run_shardloom_vortex_run_microbenchmark(
            root,
            env,
            binary,
            fixture,
            iterations,
            "local primitive filter projection",
            "filter-project:gte:value:10000|value",
            "filter_projection",
        ),
        run_shardloom_commit_microbenchmark(root, env, binary, iterations),
    ]
    rows = append_required_native_microbenchmark_blockers(rows)
    rows.append(run_native_evidence_render_microbenchmark(rows, iterations))
    return rows


def run_shardloom_count_microbenchmark(
    root: Path,
    env: dict[str, str],
    binary: Path,
    fixture: Path,
    iterations: int,
) -> dict[str, Any]:
    command = [
        str(binary),
        "vortex-count-benchmark",
        str(fixture),
        "1",
        "1",
        "--iterations",
        str(iterations),
        "--format",
        "json",
    ]
    started = time.perf_counter()
    completed = subprocess_run(command, root, env)
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    if completed["returncode"] != 0:
        return native_microbenchmark_error(
            "local encoded CountAll",
            "execution_error",
            completed["stderr"] or completed["stdout"] or "unknown failure",
            command,
            elapsed_ms,
            primitive_family="encoded_count_all",
            primitive="count",
        )
    try:
        payload = json.loads(completed["stdout"].splitlines()[0])
    except (json.JSONDecodeError, IndexError) as exc:
        return native_microbenchmark_error(
            "local encoded CountAll",
            "invalid_output",
            f"{type(exc).__name__}: {exc}",
            command,
            elapsed_ms,
            primitive_family="encoded_count_all",
            primitive="count",
        )
    fields = parse_output_fields(payload)
    return add_native_microbenchmark_contract({
        "name": "local encoded CountAll",
        "status": payload.get("status", "unknown"),
        "dataset": str(fixture),
        "primitive": "count",
        "rows": fields.get("count"),
        "iterations": fields.get("iterations_completed"),
        "query_runtime_millis": fields.get("avg_query_runtime_millis"),
        "query_runtime_micros": fields.get("avg_query_runtime_micros"),
        "timing_scope": "in-command repeated local encoded count",
        "comparison_status": fields.get("comparison_status"),
        "claim_gate_status": fields.get("claim_gate_status"),
        "native_vortex_admission_lane_ref": fields.get(
            "native_vortex_admission_lane_ref"
        ),
        "native_vortex_admission_status": fields.get("native_vortex_admission_status"),
        "native_vortex_admission_support_status": fields.get(
            "native_vortex_admission_support_status"
        ),
        "native_vortex_admission_provider_kind": fields.get(
            "native_vortex_admission_provider_kind"
        ),
        "native_vortex_admission_claim_boundary": fields.get(
            "native_vortex_admission_claim_boundary"
        ),
        "native_vortex_admission_lane_claim_allowed": fields.get(
            "native_vortex_admission_lane_claim_allowed"
        ),
        "native_vortex_admission_execution_certificate_refs": fields.get(
            "native_vortex_admission_execution_certificate_refs"
        ),
        "native_vortex_admission_native_io_refs": fields.get(
            "native_vortex_admission_native_io_refs"
        ),
        "native_vortex_admission_materialization_decode_refs": fields.get(
            "native_vortex_admission_materialization_decode_refs"
        ),
        "native_vortex_admission_fallback_attempted": fields.get(
            "native_vortex_admission_fallback_attempted"
        ),
        "native_vortex_admission_external_engine_invoked": fields.get(
            "native_vortex_admission_external_engine_invoked"
        ),
        "data_read": fields.get("data_read"),
        "data_decoded": fields.get("data_decoded"),
        "data_materialized": fields.get("data_materialized"),
        "row_read": fields.get("row_read"),
        "arrow_converted": fields.get("arrow_converted"),
        "materialization_boundary_reported": fields.get(
            "materialization_boundary_reported", "false"
        ),
        "fallback_attempted": fields.get("fallback_attempted"),
        "performance_claim_allowed": fields.get("performance_claim_allowed"),
        "command": command,
    }, "encoded_count_all")


def run_shardloom_vortex_run_microbenchmark(
    root: Path,
    env: dict[str, str],
    binary: Path,
    fixture: Path,
    iterations: int,
    name: str,
    primitive: str,
    primitive_family: str,
) -> dict[str, Any]:
    command = [
        str(binary),
        "vortex-run",
        str(fixture),
        primitive,
        "1",
        "1",
        "--format",
        "json",
    ]
    timings: list[float] = []
    payload: dict[str, Any] | None = None
    for _ in range(iterations):
        started = time.perf_counter()
        completed = subprocess_run(command, root, env)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        timings.append(elapsed_ms)
        if completed["returncode"] != 0:
            return native_microbenchmark_error(
                name,
                "execution_error",
                completed["stderr"] or completed["stdout"] or "unknown failure",
                command,
                elapsed_ms,
                primitive_family=primitive_family,
                primitive=primitive,
            )
        try:
            payload = json.loads(completed["stdout"].splitlines()[0])
        except (json.JSONDecodeError, IndexError) as exc:
            return native_microbenchmark_error(
                name,
                "invalid_output",
                f"{type(exc).__name__}: {exc}",
                command,
                elapsed_ms,
                primitive_family=primitive_family,
                primitive=primitive,
            )
        if payload.get("status") != "success":
            return native_microbenchmark_error(
                name,
                str(payload.get("status", "unsupported")),
                payload.get("human_text") or "ShardLoom native primitive did not succeed",
                command,
                elapsed_ms,
                primitive_family=primitive_family,
                primitive=primitive,
            )
    fields = parse_output_fields(payload or {})
    return add_native_microbenchmark_contract({
        "name": name,
        "status": (payload or {}).get("status", "unknown"),
        "dataset": str(fixture),
        "primitive": primitive,
        "rows": first_meaningful_field(
            fields.get("local_primitive_rows_selected"),
            fields.get("local_primitive_rows_scanned"),
        ),
        "iterations": str(iterations),
        "query_runtime_millis": round(statistics.mean(timings), 4),
        "timing_scope": "average CLI process wall time",
        "comparison_status": "not_applicable",
        "claim_gate_status": "not_claim_grade",
        "result_known": fields.get("result_known"),
        "projected_columns": fields.get("local_primitive_projected_columns"),
        "filter_pushdown_applied": fields.get("local_primitive_filter_pushdown_applied"),
        "projection_pushdown_applied": fields.get(
            "local_primitive_projection_pushdown_applied"
        ),
        "upstream_filter_expression_used": fields.get(
            "local_primitive_upstream_filter_expression_used"
        ),
        "upstream_projection_expression_used": fields.get(
            "local_primitive_upstream_projection_expression_used"
        ),
        "data_read": fields.get("data_read"),
        "data_decoded": fields.get("data_decoded"),
        "data_materialized": fields.get("data_materialized"),
        "row_read": fields.get("row_read"),
        "arrow_converted": fields.get("arrow_converted"),
        "materialization_boundary_reported": fields.get(
            "local_primitive_materialization_boundary_reported"
        ),
        "work_avoided_metrics": fields.get("work_avoided_metrics"),
        "work_avoided_known_metrics": fields.get("work_avoided_known_metrics"),
        "work_avoided_unknown_metrics": fields.get("work_avoided_unknown_metrics"),
        "work_avoided_decode_avoided": fields.get("work_avoided_decode_avoided"),
        "work_avoided_materialization_avoided": fields.get(
            "work_avoided_materialization_avoided"
        ),
        "work_avoided_rows_not_scanned": fields.get("work_avoided_rows_not_scanned"),
        "work_avoided_rows_not_scanned_known": fields.get(
            "work_avoided_rows_not_scanned_known"
        ),
        "work_avoided_segments_pruned": fields.get("work_avoided_segments_pruned"),
        "work_avoided_segments_pruned_known": fields.get(
            "work_avoided_segments_pruned_known"
        ),
        "work_avoided_bytes_not_read": fields.get("work_avoided_bytes_not_read"),
        "work_avoided_bytes_not_read_known": fields.get("work_avoided_bytes_not_read_known"),
        "work_avoided_spill_avoided": fields.get("work_avoided_spill_avoided"),
        "work_avoided_fallback_blocked": fields.get("work_avoided_fallback_blocked"),
        "decision_trace_entries": fields.get("decision_trace_entries"),
        "why_claim_gate_status": fields.get("why_claim_gate_status"),
        "why_primary_reason": fields.get("why_primary_reason"),
        "why_blocker_count": fields.get("why_blocker_count"),
        "why_blockers": fields.get("why_blockers"),
        "why_next_actions": fields.get("why_next_actions"),
        "fallback_attempted": str(
            (payload or {}).get("fallback", {}).get("attempted", False)
        ).lower(),
        "performance_claim_allowed": "false",
        "command": command,
    }, primitive_family)


def native_microbenchmark_error(
    name: str,
    status: str,
    reason: str,
    command: list[str] | None = None,
    elapsed_millis: float | None = None,
    *,
    primitive_family: str = "native_microbenchmark",
    primitive: str | None = None,
    support_status: str = "unsupported",
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "name": name,
        "status": status,
        "reason": reason,
    }
    if command is not None:
        result["command"] = command
    if elapsed_millis is not None:
        result["elapsed_millis"] = round(elapsed_millis, 4)
    if primitive is not None:
        result["primitive"] = primitive
    result.update(
        {
            "dataset": "not_executed",
            "rows": "n/a",
            "iterations": "0",
            "query_runtime_millis": None,
            "timing_scope": "not_executed",
            "comparison_status": "not_applicable",
            "claim_gate_status": "not_claim_grade",
            "data_read": "false",
            "data_decoded": "false",
            "data_materialized": "false",
            "row_read": "false",
            "arrow_converted": "false",
            "materialization_boundary_reported": "false",
            "fallback_attempted": "false",
            "performance_claim_allowed": "false",
        }
    )
    for metric in WORK_AVOIDANCE_METRICS:
        result.update(
            work_avoidance_metric_field(
                metric,
                "unsupported",
                None,
                f"native microbenchmark status is {status}: {reason}",
            )
        )
    return add_native_microbenchmark_contract(
        result,
        primitive_family,
        support_status=support_status,
        unsupported_reason=reason,
    )


def run_shardloom_commit_microbenchmark(
    root: Path,
    env: dict[str, str],
    binary: Path,
    iterations: int,
) -> dict[str, Any]:
    command_template = [
        str(binary),
        "vortex-local-commit-execute",
        "<target-uri>",
        "<workspace>",
        "commit-protocol-ready,finalized-manifest-written,commit-marker-written,output-payload-written,local-workspace,feature-gate-enabled",
        "--format",
        "json",
    ]
    timings: list[float] = []
    bytes_written: list[int] = []
    commit_latencies: list[int] = []
    payload: dict[str, Any] | None = None
    generated_root = root / "benchmarks" / "traditional_analytics" / ".generated"
    generated_root.mkdir(parents=True, exist_ok=True)
    for iteration in range(iterations):
        workspace = generated_root / f"commit-{os.getpid()}-{time.time_ns()}-{iteration}"
        workspace.mkdir(parents=True, exist_ok=False)
        try:
            prepare_shardloom_commit_workspace(workspace, iteration)
            target_uri = (workspace / "target.vortex").resolve().as_uri()
            command = [
                str(binary),
                "vortex-local-commit-execute",
                target_uri,
                str(workspace),
                command_template[4],
                "--format",
                "json",
            ]
            started = time.perf_counter()
            completed = subprocess_run(command, root, env)
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            timings.append(elapsed_ms)
            if completed["returncode"] != 0:
                return native_microbenchmark_error(
                    "local commit manifest",
                    "execution_error",
                    completed["stderr"] or completed["stdout"] or "unknown failure",
                    command,
                    elapsed_ms,
                    primitive_family="local_commit_manifest",
                    primitive="local_commit",
                )
            try:
                payload = json.loads(completed["stdout"].splitlines()[0])
            except (json.JSONDecodeError, IndexError) as exc:
                return native_microbenchmark_error(
                    "local commit manifest",
                    "invalid_output",
                    f"{type(exc).__name__}: {exc}",
                    command,
                    elapsed_ms,
                    primitive_family="local_commit_manifest",
                    primitive="local_commit",
                )
            if payload.get("status") != "success":
                return native_microbenchmark_error(
                    "local commit manifest",
                    str(payload.get("status", "unsupported")),
                    payload.get("human_text") or "ShardLoom local commit did not succeed",
                    command,
                    elapsed_ms,
                    primitive_family="local_commit_manifest",
                    primitive="local_commit",
                )
            fields = parse_output_fields(payload)
            bytes_written_value = parse_optional_int(fields.get("bytes_written"))
            latency_value = parse_optional_int(fields.get("write_commit_latency_micros"))
            if bytes_written_value is not None:
                bytes_written.append(bytes_written_value)
            if latency_value is not None:
                commit_latencies.append(latency_value)
        finally:
            shutil.rmtree(workspace, ignore_errors=True)

    fields = parse_output_fields(payload or {})
    avg_commit_latency_micros = (
        int(round(statistics.mean(commit_latencies))) if commit_latencies else None
    )
    return add_native_microbenchmark_contract({
        "name": "local commit manifest",
        "status": (payload or {}).get("status", "unknown"),
        "dataset": "synthetic local staged workspace",
        "primitive": "local_commit",
        "rows": "n/a",
        "iterations": str(iterations),
        "query_runtime_millis": round(statistics.mean(timings), 4),
        "timing_scope": "average CLI process wall time",
        "comparison_status": "not_applicable",
        "claim_gate_status": "not_claim_grade",
        "commit_executed": fields.get("commit_executed"),
        "manifest_committed": fields.get("manifest_committed"),
        "bytes_written": str(sum(bytes_written)) if bytes_written else fields.get("bytes_written"),
        "write_commit_latency_micros": str(avg_commit_latency_micros)
        if avg_commit_latency_micros is not None
        else fields.get("write_commit_latency_micros"),
        "write_commit_latency_millis": str(round(avg_commit_latency_micros / 1000.0, 4))
        if avg_commit_latency_micros is not None
        else fields.get("write_commit_latency_millis"),
        "data_read": "false",
        "data_decoded": "false",
        "data_materialized": "false",
        "row_read": "false",
        "arrow_converted": "false",
        "materialization_boundary_reported": "false",
        "fallback_attempted": str((payload or {}).get("fallback", {}).get("attempted", False)).lower(),
        "performance_claim_allowed": "false",
        "command": command_template,
    }, "local_commit_manifest")


def prepare_shardloom_commit_workspace(workspace: Path, iteration: int) -> None:
    (workspace / "_shardloom_finalized_manifest.json").write_text(
        json.dumps({"finalized": True, "iteration": iteration}, sort_keys=True),
        encoding="utf-8",
    )
    (workspace / ".shardloom-commit-marker").write_text("marker=true\n", encoding="utf-8")
    (workspace / "_shardloom_output_payload.vortex").write_bytes(b"payload")


def subprocess_run(command: list[str], cwd: Path, env: dict[str, str]) -> dict[str, Any]:
    import subprocess

    started = time.perf_counter()
    completed = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    process_wall_millis = (time.perf_counter() - started) * 1000.0
    return {
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
        "process_wall_millis": round(process_wall_millis, 4),
    }


def universal_io_lanes() -> list[dict[str, Any]]:
    return [
        {
            "name": "CSV/JSONL/Parquet/Arrow IPC/Avro/ORC -> NativeWorkStream -> Vortex",
            "status": "smoke_supported",
            "reason": "ShardLoom benchmark rows use deterministic local compatibility source adapters, emit native work/native result evidence fields, write local Vortex files, reopen them through Vortex, and scan Vortex arrays. The path still materializes Vortex-derived arrays for the temporary operators.",
            "expected_report": "per-path NativeIoCertificate with SourceCapabilityReport, SourcePushdownReport, SinkRequirementReport, AdapterFidelityReport, MaterializationBoundaryReport, and side-effect evidence",
        },
        {
            "name": "Compatibility source -> Vortex import -> encoded CountAll",
            "status": "partial_smoke_supported",
            "reason": "Compatibility-to-Vortex import and Vortex scan are exercised by ShardLoom traditional rows. The native microbenchmark lane separately exercises local Vortex scan filter/projection pushdown. Fully integrated compatibility-to-Vortex encoded operator execution over imported artifacts remains a CG-2/CG-13/CG-19 follow-up.",
            "expected_report": "NativeIoCertificate plus encoded-count execution certificate",
        },
        {
            "name": "Local CSV -> direct transient ShardLoom compute",
            "status": "fixture_smoke_supported",
            "reason": "The shardloom-direct-transient lane covers scoped local CSV selective-filter and filter + projection + limit smoke paths without Vortex write/reopen. It emits an execution certificate, materialization/decode evidence, and no-fallback fields, but it is not Vortex-native.",
            "expected_report": "execution certificate, direct-transient coverage row, materialization/decode fields, and fallback_attempted=false",
        },
    ]


def correctness_summary(
    results: list[dict[str, Any]], scenarios: tuple[str, ...]
) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for scenario in scenarios:
        successful = [
            result
            for result in results
            if result["scenario_name"] == scenario and result["status"] == "success"
        ]
        if not successful:
            summary[scenario] = {
                "status": "missing",
                "reference_engine": None,
                "matching_engines": [],
                "mismatching_engines": [],
            }
            continue
        reference = successful[0]
        matching = [
            result["engine"]
            for result in successful
            if result["correctness_digest"] == reference["correctness_digest"]
        ]
        mismatching = [
            result["engine"]
            for result in successful
            if result["correctness_digest"] != reference["correctness_digest"]
        ]
        summary[scenario] = {
            "status": "passed" if not mismatching else "mismatch",
            "reference_engine": reference["engine"],
            "reference_digest": reference["correctness_digest"],
            "matching_engines": matching,
            "mismatching_engines": mismatching,
        }
    return summary


def environment_report() -> dict[str, Any]:
    total_memory = None
    try:
        import psutil  # type: ignore
    except ImportError:
        pass
    else:
        total_memory = psutil.virtual_memory().total
    return {
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "cpu_count": os.cpu_count(),
        "total_memory_bytes": total_memory,
    }


def fairness_parameters(args: argparse.Namespace, paths: DatasetPaths) -> dict[str, Any]:
    build_evidence = build_profile_toolchain_evidence(args.shardloom_build_profile)
    return {
        "status": "local_smoke_not_claim_grade",
        "rows": paths.rows,
        "dim_rows": paths.dim_rows,
        "storage_format": "CSV, JSONL, Parquet, Arrow IPC, Avro, and ORC where supported; ShardLoom compatibility rows import into local Vortex files; shardloom-vortex rows report prepared/native Vortex execution under the requested source-format rows; shardloom-direct-transient is a scoped local CSV smoke lane without Vortex persistence",
        "benchmark_suite": BENCHMARK_SUITE,
        "scenario_catalog_schema": SCENARIO_CATALOG["schema_version"],
        "dataset_profile": args.dataset_profile,
        "generated_dataset_profiles": list(GENERATED_DATASET_PROFILES),
        "formats_requested": list(args.format_list),
        "formats_reported": list(report_format_order(args)),
        "compression": "engine defaults; Parquet uses pyarrow defaults; ShardLoom uses upstream Vortex writer defaults",
        "iterations": args.iterations,
        "stress_lane_included": any(
            scenario in STRESS_SCENARIO_ORDER for scenario in args.scenario_list
        ),
        "cache_mode": args.cache_mode,
        "timing_scope": args.timing_scope,
        "engines_requested": list(args.engine_list),
        "scenarios_requested": list(args.scenario_list),
        "taxonomy_extra_included": args.include_taxonomy_extra,
        "shardloom_build_profile": args.shardloom_build_profile,
        "shardloom_build_profile_kind": build_evidence["build_profile_kind"],
        "shardloom_target_cpu_policy": build_evidence["target_cpu_policy"],
        "shardloom_target_cpu_native_enabled": build_evidence[
            "target_cpu_native_enabled"
        ],
        "shardloom_lto_enabled": build_evidence["lto_enabled"],
        "shardloom_lto_mode": build_evidence["lto_mode"],
        "shardloom_codegen_units": build_evidence["codegen_units"],
        "shardloom_pgo_status": build_evidence["pgo_status"],
        "shardloom_portable_release_artifact": build_evidence[
            "portable_release_artifact"
        ],
        "shardloom_benchmark_only_build": build_evidence["benchmark_only_build"],
        "shardloom_build_time_excluded": True,
        "shardloom_feature_gate": "vortex-traditional-analytics-benchmark",
        "shardloom_result_sink_enabled": args.shardloom_result_sink,
        "shardloom_evidence_level": args.shardloom_evidence_level,
        "claim_readiness_rerun_profile": args.claim_readiness_rerun,
        "claim_grade_min_iterations": MIN_CLAIM_GRADE_ITERATIONS,
        "dask_blocksize": args.dask_blocksize,
        "dask_scheduler": args.dask_scheduler,
        "spark_requires_java": True,
        "spark_profiles": "spark-default local[*] with Spark defaults; spark-local-tuned local[*] with shuffle/default parallelism capped to local CPU count and AQE enabled",
        "java_on_path": shutil.which("java") is not None,
        "java_home_set": bool(os.environ.get("JAVA_HOME")),
        "object_store_included": False,
        "compatibility_to_vortex_included": True,
        "csv_to_vortex_included": "csv" in args.format_list,
        "parquet_included": "parquet" in args.format_list,
        "jsonl_included": "jsonl" in args.format_list,
        "arrow_ipc_included": "arrow-ipc" in args.format_list,
        "avro_included": "avro" in args.format_list,
        "orc_included": "orc" in args.format_list,
        "shardloom_resource_sizing": "auto by default; optional --memory-gb and --max-parallelism caps are reflected in ShardLoom evidence fields",
        "native_vortex_included": any(
            engine in ("shardloom-vortex", "shardloom-prepared-vortex")
            for engine in args.engine_list
        ),
        "direct_transient_included": "shardloom-direct-transient" in args.engine_list,
        "shardloom_universal_io_smoke_included": True,
        "shardloom_native_microbenchmarks_included": not args.skip_shardloom_native,
        "claim_grade_requirements": [
            "pin engine versions",
            "declare hardware profile",
            "separate cold-cache and warm-cache runs",
            "use larger-than-memory and object-store datasets where relevant",
            "record ShardLoom native and universal-I/O rows separately from external compatibility-file baselines",
            "run multiple repetitions under the same process isolation policy",
        ],
    }


def execution_mode_attribution_contract() -> dict[str, Any]:
    return {
        "contract_id": "shardloom.execution_mode_benchmark_attribution.v1",
        "canonical_reference": "docs/architecture/compute-engine-flow-reference.md",
        "companion_reference": (
            "docs/architecture/performance-attribution-and-execution-structure.md"
        ),
        "mode_vocabulary": list(SHARDLOOM_EXECUTION_MODE_VOCABULARY),
        "execution_mode_fields": list(EXECUTION_MODE_CONTRACT_FIELDS),
        "stage_timing_fields": list(STAGE_TIMING_CONTRACT_FIELDS),
        "operator_blocker_matrix_fields": list(OPERATOR_BLOCKER_MATRIX_FIELDS),
        "fused_pipeline_fields": list(FUSED_PIPELINE_FIELDS),
        "compressed_kernel_registry_fields": list(COMPRESSED_KERNEL_REGISTRY_FIELDS),
        "scan_pushdown_fields": list(SCAN_PUSHDOWN_FIELDS),
        "optimizer_trace_fields": list(OPTIMIZER_TRACE_FIELDS),
        "build_profile_fields": list(BUILD_PROFILE_FIELDS),
        "unknown_stage_value_policy": "field_present_with_null_or_explicit_not_measured",
        "mode_interpretation": {
            "compatibility_import_certified": (
                "Times compatibility source read/parse, Vortex import, local Vortex "
                "write/reopen/scan, temporary operator compute, optional result-sink "
                "work, and evidence rendering; it is ingest/stage/certification "
                "evidence, not pure query-speed evidence."
            ),
            "prepared_vortex": (
                "Measures scenario timing from prepared Vortex artifacts; preparation "
                "is recorded separately unless preparation_included_in_timing=true."
            ),
            "native_vortex": (
                "Measures execution over existing Vortex input with provider and "
                "materialization/decode evidence."
            ),
            "direct_compatibility_transient": (
                "Measures the scoped one-shot direct compatibility path when admitted; "
                "it is ShardLoom-native but not Vortex-native."
            ),
            "auto": (
                "Transparent selection only; rows must preserve requested_execution_mode, "
                "selected_execution_mode, and mode_selection_reason."
            ),
            EXTERNAL_BASELINE_EXECUTION_MODE: (
                "External engines are comparison baselines/oracles only and are never "
                "ShardLoom fallback execution."
            ),
        },
        "claim_boundary": (
            "claim_gate_status remains not_claim_grade, fixture_smoke_only, "
            "claim_grade, or external_baseline_only; unsupported rows keep "
            "support_status=unsupported plus a deterministic diagnostic until "
            "workload-scoped claim-grade evidence is attached."
        ),
        "operator_claim_boundary": (
            "operator_execution_class=materialized_temporary or residual_native never "
            "permits an encoded-native operator claim."
        ),
        "no_fallback_rule": "fallback_attempted=false and external_engine_invoked=false for every ShardLoom row",
    }


def persistent_runner_admission_gate() -> dict[str, Any]:
    return {
        "gate_id": "gar-flow-2c.persistent_runner_admission.v1",
        "support_status": "report_only",
        "persistent_runner_admitted": False,
        "scoped_batch_runner_supported": True,
        "current_status": (
            f"{BATCH_RUNNER_STATUS} for eligible prepared/native Vortex groups; "
            f"{PERSISTENT_RUNNER_STATUS} otherwise"
        ),
        "hidden_fast_mode_allowed": False,
        "performance_claim_allowed": False,
        "claim_gate_status": "not_claim_grade",
        "row_fields": list(PERSISTENT_RUNNER_ADMISSION_FIELDS)
        + list(BATCH_RUNNER_ADMISSION_FIELDS)
        + list(SESSION_RUNTIME_FIELDS)
        + list(ALLOCATION_RESOURCE_PROFILE_FIELDS)
        + list(RUNTIME_EVIDENCE_LEVEL_FIELDS)
        + list(OPTIMIZER_TRACE_FIELDS)
        + list(BUILD_PROFILE_FIELDS),
        "must_preserve": [
            "shardloom.output.v2 typed envelopes per run",
            "execution-mode selection fields",
            "Native I/O certificate refs and inline artifacts",
            "runtime evidence-level fields",
            "optimizer trace refs without applied rewrites until correctness proof exists",
            "build-profile evidence including LTO, PGO, and native benchmark-only boundaries",
            "operator blocker matrix fields",
            "materialization/decode boundary fields",
            "result-sink replay evidence when result sinks are enabled",
            "fallback_attempted=false and external_engine_invoked=false",
            "build, startup, preparation, scenario, and evidence-render timing split",
            "deterministic unsupported diagnostics",
        ],
        "admission_requirements": [
            "no row may skip typed envelope rendering or policy evidence",
            "startup/warmup amortization must be a visible field, not hidden in query timing",
            "prepared Vortex artifact setup must remain separate from scenario timing",
            "external engines remain comparison baselines and never fallback execution",
            "persistent worker lifecycle must expose start, stop, failure, and cleanup status",
            "claim-grade reruns must pass before any process-overhead or performance claim",
        ],
        "blocked_until": [
            "worker lifecycle contract exists",
            "IPC or in-process protocol preserves typed artifacts",
            "per-run no-fallback and external-engine flags are validated",
            "benchmark rows prove timing equivalence against process-per-scenario mode before any claim",
            "release claim gate consumes persistent-runner status",
        ],
        "source_grounded_rationale": [
            {
                "reference": "Apache Arrow columnar format",
                "url": "https://arrow.apache.org/docs/format/Columnar.html",
                "relevance": "Columnar, vectorization-friendly data representation is a data-plane property; benchmark process startup must stay separate from operator/data-plane work.",
            },
            {
                "reference": "DuckDB execution format",
                "url": "https://duckdb.org/docs/current/internals/vector",
                "relevance": "Vectorized execution operates on vectors/data chunks, so benchmark reports should not mix process lifecycle overhead with vector operator work.",
            },
            {
                "reference": "Spark SQL performance tuning",
                "url": "https://spark.apache.org/docs/3.5.6/sql-performance-tuning.html",
                "relevance": "Startup, caching, batch size, and file-partition tuning affect reported times; ShardLoom must keep process and preparation timing explicit.",
            },
            {
                "reference": "Vortex Scan API",
                "url": "https://docs.vortex.dev/concepts/scanning",
                "relevance": "Source, split, sink, pushdown, and compressed-array evidence must remain visible if benchmark execution moves into a persistent worker.",
            },
        ],
        "no_fallback_rule": "fallback_attempted=false and external_engine_invoked=false for every ShardLoom row",
        "claim_boundary": "A future persistent runner may reduce measured process overhead only after this gate is implemented; current rows remain not claim-grade.",
    }


def work_avoidance_evidence_schema() -> dict[str, Any]:
    return {
        "schema_id": "gar-flow-2d.work_avoidance_evidence.v1",
        "support_status": "report_only",
        "status_vocabulary": list(WORK_AVOIDANCE_STATUS_VOCABULARY),
        "metrics": list(WORK_AVOIDANCE_METRICS),
        "row_fields": list(WORK_AVOIDANCE_EVIDENCE_FIELDS),
        "status_meaning": {
            "measured": "the row emitted a concrete value or proof field for this metric",
            "not_available": "the metric is meaningful for the row but is not yet measured",
            "unsupported": "the row did not execute a supported ShardLoom path",
            "not_applicable": "the metric does not apply to the row or engine role",
        },
        "unknown_value_policy": "not_available metrics must keep null/n/a values and a reason; they must not be converted to zero",
        "claim_boundary": (
            "missing, unsupported, or not_available work-avoidance metrics cannot support "
            "performance, superiority, Spark-displacement, production, or best-default claims"
        ),
        "source_grounded_rationale": [
            {
                "reference": "Apache Arrow columnar format",
                "url": "https://arrow.apache.org/docs/format/Columnar.html",
                "relevance": "columnar data-plane claims need explicit decode/materialization evidence rather than omitted counters",
            },
            {
                "reference": "Spark Parquet configuration",
                "url": "https://spark.apache.org/docs/latest/sql-data-sources-parquet.html",
                "relevance": "partition discovery, schema merging, metadata refresh, and file configuration affect what work can be avoided",
            },
            {
                "reference": "DuckDB execution format",
                "url": "https://duckdb.org/docs/current/internals/vector",
                "relevance": "vectorized execution needs separate evidence for vector work versus unmeasured setup or scan work",
            },
            {
                "reference": "Vortex Scan API",
                "url": "https://docs.vortex.dev/concepts/scanning",
                "relevance": "filter/projection pushdown, split scheduling, and pruning require explicit proof fields before optimization claims",
            },
        ],
    }


def default_output_path() -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Path(__file__).resolve().parent / "results" / f"traditional_analytics_{timestamp}.json"


def report_format_order(args: argparse.Namespace) -> tuple[str, ...]:
    return tuple(args.format_list)


def formats_for_engine_report(
    engine: str, runner: EngineRunner | None, report_formats: tuple[str, ...]
) -> tuple[str, ...]:
    supported_formats = runner.formats if runner is not None else FORMAT_ORDER
    return tuple(data_format for data_format in report_formats if data_format in supported_formats)


def expanded_scenario_order(
    formats: tuple[str, ...], scenarios: tuple[str, ...]
) -> list[str]:
    return [
        scenario_display_name(data_format, scenario)
        for data_format in formats
        for scenario in scenarios
    ]


def markdown_output_path(json_path: Path, requested: Path | None) -> Path:
    if requested is not None:
        return requested
    return json_path.with_suffix(".md")


def format_metric(value: Any, suffix: str = "") -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.2f}{suffix}"
    return f"{value}{suffix}"


def format_bytes(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    unit = units[0]
    for unit in units:
        if abs(number) < 1024.0 or unit == units[-1]:
            break
        number /= 1024.0
    return f"{number:.2f} {unit}"


def format_bool(value: Any) -> str:
    if value is None:
        return "n/a"
    return str(value).lower()


def result_lookup(results: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    return {(result["scenario_name"], result["engine"]): result for result in results}


def markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    output = ["| " + " | ".join(headers) + " |"]
    output.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        output.append("| " + " | ".join(row) + " |")
    return "\n".join(output)


def render_engine_overview(artifact: dict[str, Any]) -> str:
    results = artifact["results"]
    rows = []
    for engine in artifact["engine_order"]:
        engine_results = [result for result in results if result["engine"] == engine]
        version_info = artifact["engine_versions"].get(engine, {})
        rows.append(
            [
                engine,
                "yes" if version_info.get("available") else "no",
                str(version_info.get("version") or version_info.get("reason") or "n/a"),
                format_metric(version_info.get("startup_time_millis"), " ms"),
                format_metric(version_info.get("build_time_millis"), " ms"),
                str(sum(1 for result in engine_results if result["status"] == "success")),
                str(sum(1 for result in engine_results if result["status"] != "success")),
            ]
        )
    return markdown_table(
        [
            "Engine",
            "Available",
            "Version / reason",
            "Startup / warmup",
            "Build time (excluded)",
            "Successful scenarios",
            "Failed scenarios",
        ],
        rows,
    )


def render_fairness_parameters(artifact: dict[str, Any]) -> str:
    params = artifact["fairness_parameters"]
    rows = [
        ["Status", str(params["status"])],
        ["Rows", f"{params['rows']} fact / {params['dim_rows']} dimension"],
        ["Storage", f"{params['storage_format']} ({params['compression']})"],
        ["Benchmark suite", str(params["benchmark_suite"])],
        ["Scenario catalog", str(params["scenario_catalog_schema"])],
        ["Dataset profile", str(params["dataset_profile"])],
        ["Generated profiles", ", ".join(params["generated_dataset_profiles"])],
        ["Formats requested", ", ".join(params["formats_requested"])],
        ["Formats reported", ", ".join(params["formats_reported"])],
        ["Iterations", str(params["iterations"])],
        ["Stress lane included", str(params["stress_lane_included"])],
        ["Taxonomy extras included", str(params["taxonomy_extra_included"])],
        [
            "ShardLoom build",
            (
                f"profile={params['shardloom_build_profile']}, "
                f"kind={params['shardloom_build_profile_kind']}, "
                f"target_cpu={params['shardloom_target_cpu_policy']}, "
                f"native={params['shardloom_target_cpu_native_enabled']}, "
                f"lto={params['shardloom_lto_enabled']}:{params['shardloom_lto_mode']}, "
                f"pgo={params['shardloom_pgo_status']}, "
                f"benchmark_only={params['shardloom_benchmark_only_build']}, "
                f"feature={params['shardloom_feature_gate']}, "
                f"build_time_excluded={params['shardloom_build_time_excluded']}"
            ),
        ],
        ["Cache mode", str(params["cache_mode"])],
        ["Timing scope", str(params["timing_scope"])],
        ["Dask mode", f"blocksize={params['dask_blocksize']}, scheduler={params['dask_scheduler']}"],
        [
            "Spark prerequisite",
            f"requires Java; java_on_path={params['java_on_path']}, JAVA_HOME={params['java_home_set']}",
        ],
        ["Spark profiles", str(params["spark_profiles"])],
        ["Object store included", str(params["object_store_included"])],
        ["Compatibility to Vortex included", str(params["compatibility_to_vortex_included"])],
        ["CSV to Vortex included", str(params["csv_to_vortex_included"])],
        ["Parquet included", str(params["parquet_included"])],
        ["JSONL included", str(params["jsonl_included"])],
        ["Arrow IPC included", str(params["arrow_ipc_included"])],
        ["Avro included", str(params["avro_included"])],
        ["ORC included", str(params["orc_included"])],
        ["ShardLoom resource sizing", str(params["shardloom_resource_sizing"])],
        ["Native Vortex included", str(params["native_vortex_included"])],
        ["Direct transient included", str(params["direct_transient_included"])],
        [
            "ShardLoom universal I/O smoke",
            str(params["shardloom_universal_io_smoke_included"]),
        ],
        [
            "ShardLoom native microbenchmarks",
            str(params["shardloom_native_microbenchmarks_included"]),
        ],
    ]
    return markdown_table(["Parameter", "Value"], rows)


def render_execution_mode_attribution_contract(artifact: dict[str, Any]) -> str:
    contract = artifact["execution_mode_attribution_contract"]
    rows = [
        ["Contract", str(contract["contract_id"])],
        ["Canonical reference", str(contract["canonical_reference"])],
        ["Companion reference", str(contract["companion_reference"])],
        ["Mode vocabulary", ", ".join(contract["mode_vocabulary"])],
        ["Execution-mode fields", ", ".join(contract["execution_mode_fields"])],
        ["Stage timing fields", ", ".join(contract["stage_timing_fields"])],
        ["Operator blocker fields", ", ".join(contract["operator_blocker_matrix_fields"])],
        ["Fused pipeline fields", ", ".join(contract["fused_pipeline_fields"])],
        [
            "Compressed kernel registry fields",
            ", ".join(contract["compressed_kernel_registry_fields"]),
        ],
        ["Scan pushdown fields", ", ".join(contract["scan_pushdown_fields"])],
        ["Build-profile fields", ", ".join(contract["build_profile_fields"])],
        ["Unknown stage values", str(contract["unknown_stage_value_policy"])],
        ["Claim boundary", str(contract["claim_boundary"])],
        ["Operator claim boundary", str(contract["operator_claim_boundary"])],
        ["No-fallback rule", str(contract["no_fallback_rule"])],
    ]
    mode_rows = [
        [mode, str(description)]
        for mode, description in contract["mode_interpretation"].items()
    ]
    return (
        markdown_table(["Field", "Value"], rows)
        + "\n\n"
        + markdown_table(["Mode", "Interpretation"], mode_rows)
    )


def render_persistent_runner_admission_gate(artifact: dict[str, Any]) -> str:
    gate = artifact["persistent_runner_admission_gate"]
    rows = [
        ["Gate", str(gate["gate_id"])],
        ["Support status", str(gate["support_status"])],
        ["Persistent runner admitted", str(gate["persistent_runner_admitted"])],
        ["Scoped batch runner supported", str(gate["scoped_batch_runner_supported"])],
        ["Current status", str(gate["current_status"])],
        ["Hidden fast mode allowed", str(gate["hidden_fast_mode_allowed"])],
        ["Performance claim allowed", str(gate["performance_claim_allowed"])],
        ["Claim gate", str(gate["claim_gate_status"])],
        ["Row fields", ", ".join(gate["row_fields"])],
        ["No-fallback rule", str(gate["no_fallback_rule"])],
        ["Claim boundary", str(gate["claim_boundary"])],
    ]
    requirement_rows = [
        ["Must preserve", value] for value in gate["must_preserve"]
    ] + [["Admission requirement", value] for value in gate["admission_requirements"]]
    blocker_rows = [["Blocked until", value] for value in gate["blocked_until"]]
    rationale_rows = [
        [item["reference"], item["url"], item["relevance"]]
        for item in gate["source_grounded_rationale"]
    ]
    return (
        markdown_table(["Field", "Value"], rows)
        + "\n\n"
        + markdown_table(["Type", "Requirement"], requirement_rows)
        + "\n\n"
        + markdown_table(["Status", "Blocker"], blocker_rows)
        + "\n\n"
        + markdown_table(["Reference", "URL", "Relevance"], rationale_rows)
    )


def render_source_state_contract(artifact: dict[str, Any]) -> str:
    contract = artifact["source_state_contract"]
    rows = [
        ["Contract", str(contract["contract_id"])],
        ["Canonical reference", str(contract["canonical_reference"])],
        ["Companion reference", str(contract["companion_reference"])],
        ["Status vocabulary", ", ".join(contract["status_vocabulary"])],
        ["Row fields", ", ".join(contract["row_fields"])],
        ["Supported local formats", ", ".join(contract["supported_local_formats"])],
        ["Stable path", str(contract["stable_path"])],
        ["Current scope", str(contract["current_scope"])],
        ["No-fallback rule", str(contract["no_fallback_rule"])],
        ["Claim boundary", str(contract["claim_boundary"])],
    ]
    non_goal_rows = [["Non-goal", value] for value in contract["non_goals"]]
    return (
        markdown_table(["Field", "Value"], rows)
        + "\n\n"
        + markdown_table(["Type", "Boundary"], non_goal_rows)
    )


def render_prepared_state_contract(artifact: dict[str, Any]) -> str:
    contract = artifact["prepared_state_contract"]
    rows = [
        ["Contract", str(contract["contract_id"])],
        ["Canonical reference", str(contract["canonical_reference"])],
        ["Companion reference", str(contract["companion_reference"])],
        ["Status vocabulary", ", ".join(contract["status_vocabulary"])],
        ["Row fields", ", ".join(contract["row_fields"])],
        ["Stable path", str(contract["stable_path"])],
        ["Current scope", str(contract["current_scope"])],
        ["No-fallback rule", str(contract["no_fallback_rule"])],
        ["Claim boundary", str(contract["claim_boundary"])],
    ]
    non_goal_rows = [["Non-goal", value] for value in contract["non_goals"]]
    return (
        markdown_table(["Field", "Value"], rows)
        + "\n\n"
        + markdown_table(["Type", "Boundary"], non_goal_rows)
    )


def render_output_plan_contract(artifact: dict[str, Any]) -> str:
    contract = artifact["output_plan_contract"]
    rows = [
        ["Contract", str(contract["contract_id"])],
        ["Canonical reference", str(contract["canonical_reference"])],
        ["Companion reference", str(contract["companion_reference"])],
        ["Status vocabulary", ", ".join(contract["status_vocabulary"])],
        ["Row fields", ", ".join(contract["row_fields"])],
        ["Stable path", str(contract["stable_path"])],
        ["Current scope", str(contract["current_scope"])],
        ["No-fallback rule", str(contract["no_fallback_rule"])],
        ["Claim boundary", str(contract["claim_boundary"])],
    ]
    non_goal_rows = [["Non-goal", value] for value in contract["non_goals"]]
    return (
        markdown_table(["Field", "Value"], rows)
        + "\n\n"
        + markdown_table(["Type", "Boundary"], non_goal_rows)
    )


def render_fanout_benchmark_contract(artifact: dict[str, Any]) -> str:
    contract = artifact["fanout_benchmark_contract"]
    rows = [
        ["Contract", str(contract["contract_id"])],
        ["Canonical reference", str(contract["canonical_reference"])],
        ["Companion reference", str(contract["companion_reference"])],
        ["Status vocabulary", ", ".join(contract["status_vocabulary"])],
        ["Row fields", ", ".join(contract["row_fields"])],
        ["Stable path", str(contract["stable_path"])],
        ["Current scope", str(contract["current_scope"])],
        ["No-fallback rule", str(contract["no_fallback_rule"])],
        ["Claim boundary", str(contract["claim_boundary"])],
    ]
    non_goal_rows = [["Non-goal", value] for value in contract["non_goals"]]
    return (
        markdown_table(["Field", "Value"], rows)
        + "\n\n"
        + markdown_table(["Type", "Boundary"], non_goal_rows)
    )


def render_cache_invalidation_contract(artifact: dict[str, Any]) -> str:
    contract = artifact["cache_invalidation_contract"]
    rows = [
        ["Contract", str(contract["contract_id"])],
        ["Canonical reference", str(contract["canonical_reference"])],
        ["Companion reference", str(contract["companion_reference"])],
        ["Status vocabulary", ", ".join(contract["status_vocabulary"])],
        ["Row fields", ", ".join(contract["row_fields"])],
        ["Stable path", str(contract["stable_path"])],
        ["Current scope", str(contract["current_scope"])],
        ["Redaction boundary", str(contract["redaction_boundary"])],
        ["No-fallback rule", str(contract["no_fallback_rule"])],
        ["Claim boundary", str(contract["claim_boundary"])],
    ]
    invalidation_rows = [
        ["Invalidation rule", value] for value in contract["invalidation_rules"]
    ]
    non_goal_rows = [["Non-goal", value] for value in contract["non_goals"]]
    return (
        markdown_table(["Field", "Value"], rows)
        + "\n\n"
        + markdown_table(["Type", "Boundary"], invalidation_rows + non_goal_rows)
    )


def render_reuse_level_contract(artifact: dict[str, Any]) -> str:
    contract = artifact["reuse_level_contract"]
    rows = [
        ["Contract", str(contract["contract_id"])],
        ["Canonical reference", str(contract["canonical_reference"])],
        ["Companion reference", str(contract["companion_reference"])],
        ["Status vocabulary", ", ".join(contract["status_vocabulary"])],
        ["Supported levels", ", ".join(contract["supported_levels"])],
        ["Row fields", ", ".join(contract["row_fields"])],
        ["Matrix fields", ", ".join(contract["matrix_fields"])],
        ["Stable path", str(contract["stable_path"])],
        ["Current scope", str(contract["current_scope"])],
        ["No-fallback rule", str(contract["no_fallback_rule"])],
        ["Claim boundary", str(contract["claim_boundary"])],
    ]
    non_goal_rows = [["Non-goal", value] for value in contract["non_goals"]]
    return (
        markdown_table(["Field", "Value"], rows)
        + "\n\n"
        + markdown_table(["Type", "Boundary"], non_goal_rows)
    )


def render_build_profile_contract(artifact: dict[str, Any]) -> str:
    contract = artifact["build_profile_contract"]
    rows = [
        ["Contract", str(contract["contract_id"])],
        ["Canonical reference", str(contract["canonical_reference"])],
        ["Companion reference", str(contract["companion_reference"])],
        ["Status vocabulary", ", ".join(contract["status_vocabulary"])],
        ["Supported profiles", ", ".join(contract["supported_profiles"])],
        ["Row fields", ", ".join(contract["row_fields"])],
        ["Current scope", str(contract["current_scope"])],
        ["No-fallback rule", str(contract["no_fallback_rule"])],
        ["Claim boundary", str(contract["claim_boundary"])],
    ]
    profile_rows = [
        [profile, boundary]
        for profile, boundary in contract["profile_boundary"].items()
    ]
    workflow_rows = [["PGO workflow", value] for value in contract["pgo_workflow"]]
    non_goal_rows = [["Non-goal", value] for value in contract["non_goals"]]
    return (
        markdown_table(["Field", "Value"], rows)
        + "\n\n"
        + markdown_table(["Profile", "Boundary"], profile_rows)
        + "\n\n"
        + markdown_table(["Type", "Boundary"], workflow_rows + non_goal_rows)
    )


def render_read_this_first(artifact: dict[str, Any]) -> str:
    notes = [
        "This is a local smoke/bring-up report, not a claim-grade benchmark.",
        "External baseline rows measure each engine's local compatibility-file paths where supported. Unsupported format rows are captured explicitly instead of blocking the report.",
        "ShardLoom rows use compatibility source adapters into local Vortex files, reopen those files through Vortex, scan Vortex arrays, and then run the temporary benchmark operators over Vortex-derived arrays.",
        "ShardLoom native/prepared Vortex rows are reported under the requested source-format rows, such as CSV or Parquet, with preparation metadata rather than standalone `.vortex` report rows.",
        "ShardLoom direct-transient rows, when requested with `shardloom-direct-transient`, are scoped local CSV smoke rows without Vortex persistence and are never Vortex-native or performance-claim rows.",
        "ShardLoom prepared Vortex rows start timing from prepared Vortex artifacts; they still use temporary benchmark operators and are not mature SQL/DataFrame/API evidence.",
        "Prepared/native rows carry operator_execution_class and operator_blocker_id so residual-native and materialized-temporary operators are not counted as encoded-native.",
        "Prepared/native selective-filter rows carry encoded_predicate_provider_* v4 blocker fields; Vortex scan filter pushdown and real flag,value reader chunks are not counted as an admitted encoded predicate provider until encoding-specific kernel-input lowering and certificates exist.",
        "ShardLoom coverage rows carry materialization_policy_ref, which points to the GAR-0003-B shared materialization/decode policy in compute-capability-matrix; materialized-temporary rows cannot satisfy encoded-native claims.",
        "ShardLoom's current traditional rows report a concrete per-path NativeIoCertificate and a compatibility-format materialization boundary; they prove universal I/O viability, not mature encoded-native SQL/operator coverage.",
        "Coverage rows now carry support_status, claim_gate_status, native_unsupported_coverage_ref, and unsupported_diagnostic_code so unsupported capability rows stay distinct from timing rows.",
        "ShardLoom coverage rows also carry native_io_source_sink_coverage_ref, which points to the RFC 0031 source/sink matrix in native-io-envelope-plan.",
        "ShardLoom coverage rows carry vortex_source_split_admission_ref, which points to the GAR-0042A source/split admission proof in vortex-api-inventory and does not upgrade generalized Source/Split runtime claims.",
        "ShardLoom coverage rows carry vortex_segment_extraction_admission_ref, which points to the GAR-0003-A sparse segment extraction admission report in vortex-api-inventory; sparse patch/fill extraction is deterministically blocked until correctness, execution, Native I/O, materialization/decode, and no-fallback evidence exists.",
        "ShardLoom coverage rows carry vortex_layout_device_managed_boundary_ref, which points to the GAR-0042B layout/write/device/object-store/managed-platform claim boundary matrix; every row there is not-claim-grade until evidence exists.",
        "Claim-grade ShardLoom timing rows require at least three iterations, stable correctness digests, and the full evidence set; one-iteration smoke rows remain not-claim-grade.",
        "When result-sink proof is enabled, ShardLoom rows expose scenario_compute_millis and computed_result_sink_write_millis separately.",
        "ShardLoom rows expose cli_process_wall_millis and python_harness_overhead_millis where the Python harness can measure them. Build time is reported separately and excluded from per-scenario timing.",
        "Eligible prepared/native ShardLoom rows may use persistent_runner_status=single_process_batch_runner_supported; per-scenario CLI rows keep persistent_runner_status=process_per_scenario_attributed_not_reduced, and neither status permits a hidden fast mode or performance claim.",
        "ShardLoom rows carry SourceState contract fields for local source discovery, schema identity, parse/decode planning, fingerprinting, and scoped reuse posture; SourceState is input preparation evidence and never upgrades Vortex-native, output, object-store, SQL/DataFrame, production, or performance claims.",
        "ShardLoom prepared/native rows carry VortexPreparedState contract fields for prepared artifact refs, digests, preparation timing separation, source-state linkage, and scoped reuse posture; prepared-state evidence is not output support, encoded-native operator coverage, object-store/lakehouse support, or a performance claim.",
        "ShardLoom rows carry OutputPlan contract fields for local output planning posture; only result-sink rows with explicit local Vortex write/replay evidence can report output_plan_supported, and this is not cross-format fanout, object-store/lakehouse support, or a production sink claim.",
        "ShardLoom artifacts include an io_reuse_and_fanout matrix for required cross-format fanout cases; current rows are report-only blockers until a first-class fanout benchmark runtime writes and replays multiple local outputs.",
        "ShardLoom rows carry cache invalidation contract fields for local source/prepared/plan/output fingerprint posture; cache_valid means current-row fingerprints are internally consistent, not that a persistent cache hit or performance improvement occurred.",
        "ShardLoom rows carry evidence-safe reuse-level fields and a reuse_level_matrix so discovery, schema, parse-plan, prepared-state, operator-source-state, output-plan, and replay reuse statuses remain visible without upgrading claim gates.",
        "ShardLoom rows carry build-profile evidence for release, release-lto, release-pgo, and release-native-benchmark posture. release-native-benchmark is host-native and benchmark-only; PGO rows are report-only unless a merged profile artifact and training workload are recorded.",
        "Work-avoidance evidence uses measured/not_available/unsupported/not_applicable statuses; missing rows skipped, segments pruned, bytes avoided, encoded-vector reuse, or pushdown proof values are never interpreted as zero.",
        "ShardLoom derives resource sizing automatically by default. Evidence fields show policy mode, detected/applied parallelism, batch rows, target partition bytes, and target partition count.",
        "Dask results depend heavily on partitioning, scheduler, file count, and dataset size; small single-file CSV tests can make scheduler overhead dominate.",
        "Spark rows are split into spark-default and spark-local-tuned so default behavior is not mixed with local tuning; each Spark profile starts and warms its own session immediately before its scenario rows.",
        "Spark rows require Java/JDK. Missing Spark rows mean local setup is incomplete, not that Spark failed the workload.",
        "Stress rows are opt-in; they become meaningful Spark-style scale tests only with larger-than-memory data, stable cache policy, and explicit hardware/runtime settings.",
        "ShardLoom benchmark build time is excluded from per-scenario timing. Rows expose execution_mode, preparation_millis, total_runtime_millis, operator_compute_millis, source/import/write/reopen/scan fields, and whether preparation is included in timing.",
    ]
    return "\n".join(f"- {note}" for note in notes)


def render_scenario_matrix(artifact: dict[str, Any]) -> str:
    lookup = result_lookup(artifact["results"])
    headers = ["Scenario", *artifact["engine_order"]]
    rows = []
    for scenario in artifact["scenario_order"]:
        row = [scenario]
        for engine in artifact["engine_order"]:
            result = lookup.get((scenario, engine))
            if result is None:
                row.append("missing")
                continue
            if result["status"] == "success":
                millis = result["metrics"]["query_runtime_millis"]
                row.append(f"{format_metric(millis, ' ms')}")
            else:
                row.append(result["status"])
        rows.append(row)
    return markdown_table(headers, rows)


def render_coverage_table(artifact: dict[str, Any]) -> str:
    rows = []
    for row in artifact["coverage_table"]:
        rows.append(
            [
                row["scenario_name"],
                row["engine"],
                row["scenario_category"],
                row["engine_role"],
                row["status"],
                row["support_status"],
                str(row["timing_row_present"]),
                row["claim_gate_status"],
                str(row["selected_execution_mode"]),
                str(row["operator_execution_class"]),
                str(row["operator_admission_status"]),
                str(row["operator_blocker_id"]),
                str(row["operator_encoded_native_claim_allowed"]),
                str(row["preparation_millis"]),
                str(row["preparation_included_in_timing"]),
                str(row["claim_grade_requirements_met"]),
                str(row["reproducible_benchmark_row"]),
                str(row["correctness_digest_stable"]),
                str(row["timing_row_claim_grade"]),
                str(row["write_timing_present"]),
                format_metric(row["computed_result_sink_write_millis"], " ms"),
                "; ".join(row["claim_grade_missing_evidence"][:2])
                if row["claim_grade_missing_evidence"]
                else "none",
                str(row["native_io_status_required"]),
                str(row["certificate_status"] or "n/a"),
                str(row["execution_certificate_status"] or "n/a"),
                str(row["result_native_io_certificate_status"] or "n/a"),
                str(row["materialization_decode_evidence_present"]),
                str(row["native_unsupported_coverage_ref"] or "n/a"),
                str(row["unsupported_diagnostic_code"] or "n/a"),
                str(row["required_future_evidence"] or "n/a"),
                str(row["native_io_source_sink_coverage_ref"] or "n/a"),
                str(row["vortex_source_split_admission_ref"] or "n/a"),
                str(row["vortex_segment_extraction_admission_ref"] or "n/a"),
                str(row["vortex_layout_device_managed_boundary_ref"] or "n/a"),
                str(row["materialization_policy_ref"] or "n/a"),
                str(row["fallback_attempted"]),
                str(row["external_engine_invoked"]),
            ]
        )
    return markdown_table(
        [
            "Scenario",
            "Engine",
            "Category",
            "Role",
            "Coverage",
            "Support",
            "Timing row",
            "Claim gate",
            "Mode",
            "Operator class",
            "Operator admission",
            "Operator blocker",
            "Encoded-native op claim",
            "Prep ms",
            "Prep in timing",
            "Claim-grade",
            "Repro row",
            "Stable digest",
            "Timing claim-grade",
            "Write timing",
            "Result write",
            "Missing claim evidence",
            "Native I/O req",
            "Source Native I/O",
            "Exec cert",
            "Result Native I/O",
            "Materialization evidence",
            "Native unsupported ref",
            "Unsupported diagnostic",
            "Required future evidence",
            "Native I/O source/sink ref",
            "Vortex source/split ref",
            "Vortex segment extraction ref",
            "Vortex boundary ref",
            "Materialization policy ref",
            "Fallback",
            "External engine invoked",
        ],
        rows,
    )


def render_format_preparation_matrix(artifact: dict[str, Any]) -> str:
    rows = []
    for row in artifact["format_preparation_matrix"]:
        rows.append(
            [
                row["storage_format"],
                row["scenario_name"],
                row["engine"],
                row["status"],
                row["execution_mode"],
                row["row_scope"],
                row["native_execution_format"],
                str(row["operator_execution_class"]),
                str(row["operator_blocker_id"]),
                str(row["operator_encoded_native_claim_allowed"]),
                str(row["preparation_included_in_timing"]),
                format_metric(row["preparation_millis"], " ms"),
                format_metric(row["source_read_millis"], " ms"),
                format_metric(row["compatibility_parse_millis"], " ms"),
                format_metric(row["compatibility_to_vortex_import_millis"], " ms"),
                format_metric(row["vortex_write_millis"], " ms"),
                format_metric(row["vortex_reopen_millis"], " ms"),
                format_metric(row["vortex_scan_millis"], " ms"),
                format_metric(row["operator_compute_millis"], " ms"),
                format_metric(row["result_sink_write_millis"], " ms"),
                format_metric(row["total_runtime_millis"], " ms"),
                str(row["persistent_runner_status"]),
                str(row["claim_gate_status"]),
            ]
        )
    if not rows:
        rows.append(
            [
                "none",
                "none",
                "none",
                "missing",
                "none",
                "none",
                "vortex",
                "none",
                "none",
                "false",
                "false",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "not_executed",
                "blocked",
            ]
        )
    return markdown_table(
        [
            "Format",
            "Scenario",
            "Engine",
            "Status",
            "Mode",
            "Scope",
            "Native exec format",
            "Operator class",
            "Operator blocker",
            "Encoded-native op claim",
            "Prep in timing",
            "Prep",
            "Source read",
            "Parse",
            "Import",
            "Vortex write",
            "Vortex reopen",
            "Vortex scan",
            "Operator",
            "Result sink",
            "Total runtime",
            "Runner",
            "Claim gate",
        ],
        rows,
    )


def render_source_state_matrix(artifact: dict[str, Any]) -> str:
    rows = []
    for row in artifact["source_state_matrix"]:
        rows.append(
            [
                row["scenario_name"],
                row["engine"],
                row["status"],
                str(row["execution_mode"]),
                str(row["source_state_status"]),
                str(row["source_state_id"]),
                str(row["source_state_digest"]),
                str(row["source_format"]),
                str(row["source_fingerprint_kind"]),
                str(row["schema_digest"]),
                str(row["row_count_known"]),
                str(row["file_count"]),
                format_bytes(row["byte_size"]),
                str(row["partition_columns"]),
                str(row["compression"]),
                str(row["source_state_reuse_allowed"]),
                str(row["source_state_reuse_hit"]),
                str(row["source_state_reuse_reason"]).replace("|", "\\|"),
                format_metric(row["source_discovery_millis"], " ms"),
                format_metric(row["schema_inference_millis"], " ms"),
                format_metric(row["source_parse_millis"], " ms"),
                str(row["parse_decode_plan_digest"]),
                str(row["source_state_materialization_decode_boundary_ref"]),
                str(row["source_state_claim_gate_status"]),
                str(row["source_state_fallback_attempted"]),
                str(row["source_state_external_engine_invoked"]),
            ]
        )
    if not rows:
        rows.append(
            [
                "none",
                "none",
                "missing",
                "none",
                "blocked",
                "none",
                "none",
                "none",
                "none",
                "none",
                "false",
                "0",
                "n/a",
                "none",
                "none",
                "false",
                "false",
                "no SourceState rows were emitted",
                "n/a",
                "n/a",
                "n/a",
                "none",
                "none",
                "not_claim_grade",
                "false",
                "false",
            ]
        )
    return markdown_table(
        [
            "Scenario",
            "Engine",
            "Status",
            "Mode",
            "SourceState status",
            "SourceState id",
            "SourceState digest",
            "Format",
            "Fingerprint",
            "Schema digest",
            "Row count known",
            "Files",
            "Bytes",
            "Partitions",
            "Compression",
            "Reuse allowed",
            "Reuse hit",
            "Reuse reason",
            "Discovery",
            "Schema inference",
            "Parse/decode",
            "Parse plan digest",
            "Materialization policy",
            "Claim gate",
            "Fallback",
            "External engine",
        ],
        rows,
    )


def render_prepared_state_matrix(artifact: dict[str, Any]) -> str:
    rows = []
    for row in artifact["prepared_state_matrix"]:
        rows.append(
            [
                row["scenario_name"],
                row["engine"],
                row["status"],
                str(row["execution_mode"]),
                str(row["prepared_state_status"]),
                str(row["prepared_state_id"]),
                str(row["prepared_state_digest"]),
                str(row["source_state_id"]),
                str(row["vortex_artifact_ref"]).replace("|", "\\|"),
                str(row["vortex_artifact_digest"]).replace("|", "\\|"),
                str(row["layout_summary"]),
                str(row["encoding_summary"]).replace("|", "\\|"),
                str(row["statistics_summary"]),
                str(row["prepared_state_reuse_allowed"]),
                str(row["prepared_state_reuse_hit"]),
                str(row["prepared_state_reuse_reason"]).replace("|", "\\|"),
                str(row["preparation_included_in_timing"]),
                format_metric(row["vortex_prepare_millis"], " ms"),
                str(row["prepared_state_native_io_certificate_status"]),
                str(row["prepared_state_materialization_decode_boundary_ref"]),
                str(row["prepared_state_claim_gate_status"]),
                str(row["prepared_state_fallback_attempted"]),
                str(row["prepared_state_external_engine_invoked"]),
            ]
        )
    if not rows:
        rows.append(
            [
                "none",
                "none",
                "missing",
                "none",
                "blocked",
                "none",
                "none",
                "none",
                "none",
                "none",
                "none",
                "none",
                "none",
                "false",
                "false",
                "no VortexPreparedState rows were emitted",
                "false",
                "n/a",
                "none",
                "none",
                "not_claim_grade",
                "false",
                "false",
            ]
        )
    return markdown_table(
        [
            "Scenario",
            "Engine",
            "Status",
            "Mode",
            "PreparedState status",
            "PreparedState id",
            "PreparedState digest",
            "SourceState id",
            "Vortex artifact ref",
            "Vortex artifact digest",
            "Layout",
            "Encoding",
            "Statistics",
            "Reuse allowed",
            "Reuse hit",
            "Reuse reason",
            "Prep in timing",
            "Vortex prepare",
            "Source Native I/O",
            "Materialization policy",
            "Claim gate",
            "Fallback",
            "External engine",
        ],
        rows,
    )


def render_output_plan_matrix(artifact: dict[str, Any]) -> str:
    rows = []
    for row in artifact["output_plan_matrix"]:
        rows.append(
            [
                row["scenario_name"],
                row["engine"],
                row["status"],
                str(row["execution_mode"]),
                str(row["output_plan_status"]),
                str(row["output_plan_id"]),
                str(row["output_plan_digest"]),
                str(row["output_format"]),
                str(row["output_location"]).replace("|", "\\|"),
                str(row["output_schema_digest"]),
                str(row["output_partitioning"]),
                str(row["output_compression"]),
                str(row["output_encoding"]),
                str(row["output_write_mode"]),
                str(row["output_plan_reuse_allowed"]),
                str(row["output_metadata_preservation_status"]),
                str(row["output_materialization_required"]),
                str(row["output_plan_reuse_hit"]),
                str(row["output_plan_reuse_reason"]).replace("|", "\\|"),
                format_metric(row["output_plan_millis"], " ms"),
                format_metric(row["output_write_millis"], " ms"),
                str(row["result_replay_verified"]),
                str(row["output_native_io_certificate_status"]),
                str(row["sink_artifact_ref"]).replace("|", "\\|"),
                str(row["sink_artifact_digest"]),
                str(row["output_plan_claim_gate_status"]),
                str(row["output_plan_fallback_attempted"]),
                str(row["output_plan_external_engine_invoked"]),
            ]
        )
    if not rows:
        rows.append(
            [
                "none",
                "none",
                "missing",
                "none",
                "blocked",
                "none",
                "none",
                "none",
                "none",
                "none",
                "none",
                "none",
                "none",
                "none",
                "false",
                "none",
                "none",
                "false",
                "no OutputPlan rows were emitted",
                "n/a",
                "n/a",
                "false",
                "none",
                "none",
                "none",
                "not_claim_grade",
                "false",
                "false",
            ]
        )
    return markdown_table(
        [
            "Scenario",
            "Engine",
            "Status",
            "Mode",
            "OutputPlan status",
            "OutputPlan id",
            "OutputPlan digest",
            "Output format",
            "Output location",
            "Output schema digest",
            "Partitioning",
            "Compression",
            "Encoding",
            "Write mode",
            "Reuse allowed",
            "Metadata preservation",
            "Materialization",
            "Reuse hit",
            "Reuse reason",
            "Output plan",
            "Output write",
            "Replay verified",
            "Output Native I/O",
            "Sink ref",
            "Sink digest",
            "Claim gate",
            "Fallback",
            "External engine",
        ],
        rows,
    )


def render_fanout_benchmark_matrix(artifact: dict[str, Any]) -> str:
    rows = []
    for row in artifact["fanout_benchmark_matrix"]:
        rows.append(
            [
                str(row["benchmark_family"]),
                str(row["fanout_case_id"]),
                str(row["source_format"]),
                str(row["requested_output_formats"]),
                str(row["currently_proven_output_formats"]),
                str(row["blocked_output_formats"]),
                str(row["fanout_status"]),
                str(row["fanout_blocker_id"]),
                str(row["fanout_blocker_reason"]),
                format_metric(row["source_discovery_millis"], " ms"),
                format_metric(row["schema_inference_millis"], " ms"),
                format_metric(row["source_parse_millis"], " ms"),
                format_metric(row["vortex_prepare_millis"], " ms"),
                format_metric(row["operator_compute_millis"], " ms"),
                format_metric(row["output_plan_millis"], " ms"),
                format_metric(row["output_write_millis"], " ms"),
                format_metric(row["output_replay_millis"], " ms"),
                format_metric(row["total_runtime_millis"], " ms"),
                str(row["source_state_reuse_hit"]),
                str(row["prepared_state_reuse_hit"]),
                str(row["output_plan_reuse_hit"]),
                str(row["fanout_output_count"]),
                str(row["fallback_attempted"]),
                str(row["external_engine_invoked"]),
                str(row["claim_gate_status"]),
            ]
        )
    return markdown_table(
        [
            "Family",
            "Case",
            "Source",
            "Requested outputs",
            "Currently proven outputs",
            "Blocked outputs",
            "Status",
            "Blocker",
            "Reason",
            "Source discovery",
            "Schema inference",
            "Source parse",
            "Vortex prepare",
            "Operator compute",
            "Output plan",
            "Output write",
            "Output replay",
            "Total runtime",
            "SourceState reuse",
            "PreparedState reuse",
            "OutputPlan reuse",
            "Fanout outputs",
            "Fallback",
            "External engine",
            "Claim gate",
        ],
        rows,
    )


def render_cache_invalidation_matrix(artifact: dict[str, Any]) -> str:
    rows = []
    for row in artifact["cache_invalidation_matrix"]:
        rows.append(
            [
                row["scenario_name"],
                row["engine"],
                row["status"],
                str(row["execution_mode"]),
                str(row["cache_invalidation_status"]),
                str(row["cache_invalidation_layer_scope"]),
                str(row["source_fingerprint_kind"]),
                str(row["source_content_digest"]),
                str(row["source_mtime"]),
                format_bytes(row["source_size"]),
                str(row["object_etag"]),
                str(row["manifest_version"]),
                str(row["schema_digest"]),
                str(row["plan_digest"]),
                str(row["output_plan_digest"]),
                str(row["cache_valid"]),
                str(row["invalidation_reason"]).replace("|", "\\|"),
                str(row["cache_invalidation_fallback_attempted"]),
                str(row["cache_invalidation_external_engine_invoked"]),
                str(row["cache_invalidation_claim_gate_status"]),
                str(row["cache_invalidation_redaction_status"]),
            ]
        )
    if not rows:
        rows.append(
            [
                "none",
                "none",
                "missing",
                "none",
                "blocked",
                "none",
                "none",
                "none",
                "not_applicable",
                "n/a",
                "not_applicable",
                "none",
                "none",
                "none",
                "none",
                "false",
                "no cache invalidation rows were emitted",
                "false",
                "false",
                "not_claim_grade",
                "no_credentials_or_tokens_in_fingerprint_fields",
            ]
        )
    return markdown_table(
        [
            "Scenario",
            "Engine",
            "Status",
            "Mode",
            "Cache status",
            "Layer scope",
            "Source fingerprint",
            "Source content digest",
            "Source mtime",
            "Source size",
            "Object ETag",
            "Manifest version",
            "Schema digest",
            "Plan digest",
            "Output plan digest",
            "Cache valid",
            "Invalidation reason",
            "Fallback",
            "External engine",
            "Claim gate",
            "Credential redaction",
        ],
        rows,
    )


def render_reuse_level_matrix(artifact: dict[str, Any]) -> str:
    rows = []
    for row in artifact["reuse_level_matrix"]:
        rows.append(
            [
                str(row["scenario"]),
                str(row["engine"]),
                str(row["execution_mode"]),
                str(row["evidence_level"]),
                str(row["output_format"]),
                str(row["reuse_level"]),
                str(row["reuse_status"]),
                str(row["reuse_hit"]),
                str(row["reuse_digest"]),
                str(row["reuse_allowed"]),
                str(row["reuse_blocker"]).replace("|", "\\|"),
                str(row["layer_invalidation_reason"]).replace("|", "\\|"),
                str(row["claim_gate_status"]),
                str(row["claim_grade_requirements_met"]),
                str(row["fallback_attempted"]),
                str(row["external_engine_invoked"]),
            ]
        )
    if not rows:
        rows.append(
            [
                "none",
                "none",
                "none",
                "none",
                "none",
                "none",
                "blocked",
                "false",
                "none",
                "false",
                "no reuse-level rows were emitted",
                "none",
                "not_claim_grade",
                "false",
                "false",
                "false",
            ]
        )
    return markdown_table(
        [
            "Scenario",
            "Engine",
            "Mode",
            "Evidence level",
            "Output format",
            "Reuse level",
            "Reuse status",
            "Reuse hit",
            "Reuse digest",
            "Reuse allowed",
            "Reuse blocker",
            "Invalidation reason",
            "Claim gate",
            "Claim-grade requirements",
            "Fallback",
            "External engine",
        ],
        rows,
    )


def render_resource_metrics_table(artifact: dict[str, Any]) -> str:
    rows = []
    for result in artifact["results"]:
        metrics = result["metrics"]
        rows.append(
            [
                result["scenario_name"],
                result["engine"],
                result["status"],
                format_metric(metrics.get("query_runtime_millis"), " ms"),
                format_metric(metrics.get("scenario_compute_millis"), " ms"),
                format_metric(metrics.get("operator_compute_millis"), " ms"),
                format_metric(metrics.get("cli_process_wall_millis"), " ms"),
                format_metric(metrics.get("python_harness_overhead_millis"), " ms"),
                format_metric(metrics.get("preparation_millis"), " ms"),
                format_metric(metrics.get("preparation_cli_process_wall_millis"), " ms"),
                str(metrics.get("preparation_included_in_timing")),
                format_metric(metrics.get("source_read_millis"), " ms"),
                format_metric(metrics.get("compatibility_to_vortex_import_millis"), " ms"),
                format_metric(metrics.get("vortex_write_millis"), " ms"),
                format_metric(metrics.get("vortex_scan_millis"), " ms"),
                format_metric(metrics.get("computed_result_sink_write_millis"), " ms"),
                format_bytes(metrics.get("peak_memory_bytes")),
                format_bytes(metrics.get("bytes_read")),
                format_bytes(metrics.get("bytes_written")),
                format_bytes(metrics.get("computed_result_sink_bytes")),
                str(metrics.get("allocation_profile_status", "n/a")),
                str(metrics.get("allocation_profile_scope", "n/a")),
                str(metrics.get("allocation_count", "n/a")),
                str(metrics.get("allocation_count_status", "n/a")),
                str(metrics.get("allocation_bytes", "n/a")),
                str(metrics.get("allocation_bytes_status", "n/a")),
                str(metrics.get("buffer_pool_enabled", "n/a")),
                str(metrics.get("buffer_pool_scope", "n/a")),
                str(metrics.get("buffer_reuse_count", "n/a")),
                str(metrics.get("buffer_reuse_family", "n/a")),
                str(metrics.get("buffer_reuse_blocker", "n/a")),
                str(metrics.get("peak_rss_delta", "n/a")),
                str(metrics.get("peak_rss_delta_status", "n/a")),
                str(metrics.get("evidence_regression_status", "n/a")),
                str(metrics.get("unsafe_lifetime_shortcut_used", "n/a")),
                format_metric(metrics.get("rows_scanned")),
                format_metric(metrics.get("rows_materialized")),
            ]
        )
    return markdown_table(
        [
            "Scenario",
            "Engine",
            "Status",
            "Runtime",
            "Scenario compute",
            "Operator compute",
            "CLI process wall",
            "Python harness overhead",
            "Preparation",
            "Prep CLI wall",
            "Prep in timing",
            "Source read",
            "Compat import",
            "Vortex write",
            "Vortex scan",
            "Result write",
            "Peak RSS",
            "Bytes read",
            "Bytes written",
            "Result bytes",
            "Allocation profile",
            "Allocation scope",
            "Allocation count",
            "Allocation count status",
            "Allocation bytes",
            "Allocation bytes status",
            "Buffer pool",
            "Buffer scope",
            "Buffer reuse count",
            "Buffer reuse family",
            "Buffer blocker",
            "Peak RSS delta",
            "Peak RSS status",
            "Evidence regression",
            "Unsafe shortcut",
            "Rows scanned",
            "Rows materialized",
        ],
        rows,
    )


def render_shardloom_effects_table(artifact: dict[str, Any]) -> str:
    rows = []
    for result in artifact["results"]:
        if not str(result["engine"]).startswith("shardloom"):
            continue
        metrics = result["metrics"]
        evidence = result.get("shardloom_evidence", {})
        rows.append(
            [
                result["scenario_name"],
                result["status"],
                format_bool(metrics.get("data_decoded")),
                format_bool(metrics.get("data_materialized")),
                format_bool(metrics.get("row_read")),
                format_bool(metrics.get("arrow_converted")),
                format_bool(metrics.get("object_store_io")),
                format_bool(metrics.get("write_io")),
                format_bool(metrics.get("spill_io_performed")),
                str(evidence.get("native_io_certificate_path_id", "n/a")),
                str(evidence.get("native_io_certificate_emitted", "n/a")),
                str(evidence.get("native_io_certificate_status", "n/a")),
                str(evidence.get("resource_policy_mode", "n/a")),
                str(evidence.get("detected_parallelism", "n/a")),
                str(evidence.get("applied_max_parallelism", "n/a")),
                str(evidence.get("applied_batch_rows", "n/a")),
                format_bytes(parse_optional_int(evidence.get("target_partition_bytes"))),
                str(evidence.get("target_partition_count", "n/a")),
                str(evidence.get("materialization_boundary_rows", "n/a")),
                format_bytes(parse_optional_int(evidence.get("source_bytes_read"))),
                str(evidence.get("allocation_profile_status", "n/a")),
                str(evidence.get("allocation_profile_family_status", "n/a")),
                str(evidence.get("buffer_pool_enabled", "n/a")),
                str(evidence.get("buffer_reuse_count", "n/a")),
                str(evidence.get("buffer_reuse_blocker", "n/a")),
                str(evidence.get("peak_rss_delta", "n/a")),
                str(evidence.get("unsafe_lifetime_shortcut_used", "n/a")),
                str(evidence.get("allocation_claim_gate_status", "n/a")),
            ]
        )
    if not rows:
        rows.append(
            [
                "not run",
                "missing",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
            ]
        )
    return markdown_table(
        [
            "Scenario",
            "Status",
            "Decoded",
            "Materialized",
            "Row read",
            "Arrow",
            "Object store",
            "Write IO",
            "Spill IO",
            "Native I/O path",
            "Native I/O cert",
            "Cert status",
            "Sizing",
            "Detected par",
            "Applied par",
            "Batch rows",
            "Target part bytes",
            "Target parts",
            "Boundary rows",
            "Source bytes",
            "Allocation profile",
            "Allocation families",
            "Buffer pool",
            "Buffer reuse",
            "Buffer blocker",
            "Peak RSS delta",
            "Unsafe shortcut",
            "Allocation claim gate",
        ],
        rows,
    )


def render_correctness_table(artifact: dict[str, Any]) -> str:
    rows = []
    for scenario, summary in artifact["correctness"].items():
        rows.append(
            [
                scenario,
                summary["status"],
                str(summary.get("reference_engine") or "n/a"),
                ", ".join(summary.get("matching_engines", [])) or "n/a",
                ", ".join(summary.get("mismatching_engines", [])) or "n/a",
            ]
        )
    return markdown_table(
        ["Scenario", "Status", "Reference", "Matching engines", "Mismatching engines"],
        rows,
    )


def render_shardloom_native_table(artifact: dict[str, Any]) -> str:
    rows = []
    for result in artifact.get("shardloom_native_microbenchmarks", []):
        rows.append(
            [
                result.get("name", "n/a"),
                str(result.get("status", "n/a")),
                str(result.get("benchmark_category", "n/a")),
                str(result.get("native_microbenchmark_primitive_family", "n/a")),
                str(result.get("native_microbenchmark_support_status", "n/a")),
                str(result.get("primitive", "n/a")),
                str(result.get("native_microbenchmark_subsystem", "n/a")),
                str(result.get("native_microbenchmark_optimization_question", "n/a")),
                str(result.get("rows", "n/a")),
                str(result.get("rows_scanned", "n/a")),
                str(result.get("rows_selected", "n/a")),
                str(result.get("rows_materialized", "n/a")),
                format_metric(result.get("query_runtime_millis"), " ms"),
                str(result.get("timing_scope", "n/a")),
                str(result.get("data_decoded", "n/a")),
                str(result.get("data_materialized", "n/a")),
                str(result.get("filter_pushdown_applied", "n/a")),
                str(result.get("projection_pushdown_applied", "n/a")),
                str(result.get("materialization_boundary_reported", "n/a")),
                str(result.get("native_vortex_admission_lane_ref", "n/a")),
                str(result.get("native_vortex_admission_status", "n/a")),
                str(result.get("native_vortex_admission_provider_kind", "n/a")),
                str(result.get("native_vortex_admission_claim_boundary", "n/a")),
                str(result.get("fallback_attempted", "n/a")),
                str(result.get("external_engine_invoked", "n/a")),
                str(result.get("claim_gate_status", "n/a")),
                str(result.get("native_microbenchmark_claim_boundary", "n/a")),
                str(result.get("unsupported_reason", "none")),
            ]
        )
    if not rows:
        rows.append(["not run", "skipped", *(["n/a"] * 26)])
    return markdown_table(
        [
            "Microbenchmark",
            "Status",
            "Category",
            "Family",
            "Support",
            "Primitive",
            "Subsystem",
            "Optimization question",
            "Rows",
            "Rows scanned",
            "Rows selected",
            "Rows materialized",
            "Avg runtime",
            "Timing scope",
            "Decoded",
            "Materialized",
            "Filter pushdown",
            "Projection pushdown",
            "Boundary",
            "Native lane",
            "Admission",
            "Provider",
            "Native claim boundary",
            "Fallback",
            "External engine",
            "Claim gate",
            "Microbenchmark claim boundary",
            "Unsupported reason",
        ],
        rows,
    )


def render_shardloom_work_avoidance_table(artifact: dict[str, Any]) -> str:
    schema = artifact["work_avoidance_evidence_schema"]
    schema_rows = [
        ["Schema", str(schema["schema_id"])],
        ["Support status", str(schema["support_status"])],
        ["Status vocabulary", ", ".join(schema["status_vocabulary"])],
        ["Metrics", ", ".join(schema["metrics"])],
        ["Unknown value policy", str(schema["unknown_value_policy"])],
        ["Claim boundary", str(schema["claim_boundary"])],
    ]
    rows = []
    for result in artifact["results"]:
        if not is_shardloom_engine(str(result.get("engine", ""))):
            continue
        rows.append(
            [
                str(result.get("scenario_name", "n/a")),
                str(result.get("engine", "n/a")),
                str(result.get("status", "n/a")),
                "scenario",
                work_avoidance_cell(result, "rows_avoided"),
                work_avoidance_cell(result, "segments_pruned"),
                work_avoidance_cell(result, "bytes_avoided"),
                work_avoidance_cell(result, "encoded_vector_reuse"),
                work_avoidance_cell(result, "pushdown_proof"),
                str(result.get("work_avoidance_claim_allowed", "false")),
            ]
        )
    for result in artifact.get("shardloom_native_microbenchmarks", []):
        rows.append(
            [
                result.get("name", "n/a"),
                "shardloom-native-microbenchmark",
                str(result.get("status", "n/a")),
                str(result.get("primitive", "n/a")),
                work_avoidance_cell(result, "rows_avoided"),
                work_avoidance_cell(result, "segments_pruned"),
                work_avoidance_cell(result, "bytes_avoided"),
                work_avoidance_cell(result, "encoded_vector_reuse"),
                work_avoidance_cell(result, "pushdown_proof"),
                str(result.get("work_avoidance_claim_allowed", "false")),
            ]
        )
    if not rows:
        rows.append(
            [
                "not run",
                "skipped",
                "skipped",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
            ]
        )
    return (
        markdown_table(["Field", "Value"], schema_rows)
        + "\n\n"
        + markdown_table(
            [
                "Row",
                "Engine/scope",
                "Status",
                "Primitive/scope",
                "Rows avoided",
                "Segments pruned",
                "Bytes avoided",
                "Encoded-vector reuse",
                "Pushdown proof",
                "Claim allowed",
            ],
            rows,
        )
    )


def work_avoidance_cell(row: dict[str, Any], metric: str) -> str:
    status = str(row.get(f"work_avoidance_{metric}_status", "not_available"))
    value = row.get(f"work_avoidance_{metric}_value")
    reason = str(row.get(f"work_avoidance_{metric}_reason", "missing reason"))
    value_text = "n/a" if value in (None, "") else str(value)
    return f"{status}; value={value_text}; reason={reason}".replace("|", "\\|")


def render_shardloom_why_table(artifact: dict[str, Any]) -> str:
    rows = []
    details = []
    for result in artifact.get("shardloom_native_microbenchmarks", []):
        if result.get("why_claim_gate_status") is None:
            continue
        name = str(result.get("name", "n/a"))
        rows.append(
            [
                name,
                str(result.get("status", "n/a")),
                str(result.get("why_claim_gate_status", "n/a")),
                str(result.get("decision_trace_entries", "n/a")),
                str(result.get("why_blocker_count", "n/a")),
                str(result.get("why_primary_reason", "n/a")).replace("|", "\\|"),
                final_summary_item(result.get("why_next_actions")),
            ]
        )
        details.append(
            f"- **{name}** blockers: {summary_list_text(result.get('why_blockers'))}"
        )
        details.append(
            f"  next: {summary_list_text(result.get('why_next_actions'))}"
        )
    if not rows:
        rows.append(["not run", "skipped", "n/a", "n/a", "n/a", "n/a", "n/a"])
    table = markdown_table(
        [
            "Microbenchmark",
            "Status",
            "Claim gate",
            "Trace entries",
            "Blockers",
            "Primary reason",
            "Next focus",
        ],
        rows,
    )
    if details:
        return table + "\n\n" + "\n".join(details)
    return table


def final_summary_item(value: Any) -> str:
    text = str(value or "n/a")
    return text.rsplit(" | ", maxsplit=1)[-1].replace("|", "\\|")


def summary_list_text(value: Any) -> str:
    text = str(value or "n/a")
    return text.replace(" | ", "; ").replace("|", "\\|")


def render_shardloom_commit_table(artifact: dict[str, Any]) -> str:
    rows = []
    for result in artifact.get("shardloom_native_microbenchmarks", []):
        if result.get("primitive") != "local_commit":
            continue
        rows.append(
            [
                result.get("name", "n/a"),
                str(result.get("status", "n/a")),
                str(result.get("iterations", "n/a")),
                str(result.get("commit_executed", "n/a")),
                str(result.get("manifest_committed", "n/a")),
                format_bytes(parse_optional_int(result.get("bytes_written"))),
                str(result.get("write_commit_latency_micros", "n/a")),
                str(result.get("fallback_attempted", "n/a")),
            ]
        )
    if not rows:
        rows.append(["not run", "skipped", "n/a", "n/a", "n/a", "n/a", "n/a", "n/a"])
    return markdown_table(
        [
            "Microbenchmark",
            "Status",
            "Iterations",
            "Commit",
            "Manifest committed",
            "Bytes written",
            "Avg commit us",
            "Fallback",
        ],
        rows,
    )


def render_shardloom_result_sink_table(artifact: dict[str, Any]) -> str:
    rows = []
    for result in artifact["results"]:
        if result["engine"] != "shardloom":
            continue
        metrics = result["metrics"]
        evidence = result.get("shardloom_evidence", {})
        if metrics.get("computed_result_sink_write_millis") is None:
            continue
        rows.append(
            [
                result["scenario_name"],
                result["status"],
                result["claim_gate_status"],
                format_metric(metrics.get("scenario_compute_millis"), " ms"),
                format_metric(metrics.get("computed_result_sink_write_millis"), " ms"),
                format_bytes(metrics.get("computed_result_sink_bytes")),
                str(evidence.get("computed_result_sink_native_io_certificate_status", "n/a")),
                str(evidence.get("computed_result_sink_replay_verified", "n/a")),
                str(evidence.get("fallback_attempted", result.get("fallback_attempted", "n/a"))),
            ]
        )
    if not rows:
        rows.append(["not run", "skipped", "n/a", "n/a", "n/a", "n/a", "n/a", "n/a", "n/a"])
    return markdown_table(
        [
            "Scenario",
            "Status",
            "Claim gate",
            "Scenario compute",
            "Result write",
            "Result bytes",
            "Result Native I/O",
            "Replay verified",
            "Fallback",
        ],
        rows,
    )


def render_universal_io_table(artifact: dict[str, Any]) -> str:
    rows = []
    for lane in artifact.get("universal_io_lanes", []):
        rows.append(
            [
                lane.get("name", "n/a"),
                lane.get("status", "n/a"),
                str(lane.get("expected_report", "n/a")),
                str(lane.get("reason", "n/a")).replace("|", "\\|"),
            ]
        )
    return markdown_table(["Lane", "Status", "Expected evidence", "Reason"], rows)


def render_fastest_table(artifact: dict[str, Any]) -> str:
    lookup = result_lookup(artifact["results"])
    rows = []
    for scenario in artifact["scenario_order"]:
        successful = [
            lookup[(scenario, engine)]
            for engine in artifact["engine_order"]
            if (scenario, engine) in lookup and lookup[(scenario, engine)]["status"] == "success"
        ]
        if not successful:
            rows.append([scenario, "n/a", "n/a", "n/a", "n/a"])
            continue
        ordered = sorted(successful, key=lambda result: result["metrics"]["query_runtime_millis"])
        fastest = ordered[0]
        slowest = ordered[-1]
        fastest_ms = fastest["metrics"]["query_runtime_millis"]
        slowest_ms = slowest["metrics"]["query_runtime_millis"]
        rows.append(
            [
                scenario,
                fastest["engine"],
                format_metric(fastest_ms, " ms"),
                slowest["engine"],
                f"{slowest_ms / fastest_ms:.2f}x" if fastest_ms else "n/a",
            ]
        )
    return markdown_table(
        ["Scenario", "Fastest engine", "Fastest time", "Slowest engine", "Slowest / fastest"],
        rows,
    )


def render_timing_bars(artifact: dict[str, Any]) -> str:
    sections = []
    lookup = result_lookup(artifact["results"])
    for scenario in artifact["scenario_order"]:
        successful = [
            lookup[(scenario, engine)]
            for engine in artifact["engine_order"]
            if (scenario, engine) in lookup and lookup[(scenario, engine)]["status"] == "success"
        ]
        if not successful:
            sections.append(f"### {scenario}\n\nNo successful timing rows.")
            continue
        max_ms = max(result["metrics"]["query_runtime_millis"] for result in successful) or 1.0
        lines = [f"### {scenario}", "", "```text"]
        for engine in artifact["engine_order"]:
            result = lookup.get((scenario, engine))
            if result is None:
                lines.append(f"{engine:<12} missing")
                continue
            if result["status"] != "success":
                lines.append(f"{engine:<12} {result['status']}")
                continue
            millis = result["metrics"]["query_runtime_millis"]
            width = max(1, int((millis / max_ms) * 40))
            lines.append(f"{engine:<12} {millis:>10.2f} ms | {'#' * width}")
        lines.append("```")
        sections.append("\n".join(lines))
    return "\n\n".join(sections)


def render_errors_table(artifact: dict[str, Any]) -> str:
    rows = []
    for error in artifact["errors"]:
        rows.append(
            [
                error.get("engine", "n/a"),
                error.get("scenario", "n/a"),
                error.get("status", "n/a"),
                str(error.get("reason", "n/a")).replace("|", "\\|"),
            ]
        )
    if not rows:
        rows.append(["none", "none", "none", "none"])
    return markdown_table(["Engine", "Scenario", "Status", "Reason"], rows)


def render_markdown_report(artifact: dict[str, Any]) -> str:
    dataset = artifact["dataset"]
    env = artifact["environment"]
    lines = [
        "# Traditional Analytics Benchmark Results",
        "",
        "These are raw local benchmark measurements. They are not ShardLoom performance, superiority, or best-choice claims.",
        "",
        f"- Generated: `{artifact['generated_at_utc']}`",
        f"- Scope: `{artifact['benchmark_scope']}`",
        f"- Dataset profile: `{dataset['dataset_profile']}`",
        f"- Rows: `{dataset['rows']}` fact rows, `{dataset['dim_rows']}` dimension rows",
        f"- CSV files: `{dataset['fact_csv_bytes']}` fact bytes, `{dataset['dim_csv_bytes']}` dimension bytes",
        f"- Parquet files: `{dataset['fact_parquet_bytes']}` fact bytes, `{dataset['dim_parquet_bytes']}` dimension bytes",
        f"- JSONL files: `{dataset['fact_jsonl_bytes']}` fact bytes, `{dataset['dim_jsonl_bytes']}` dimension bytes",
        f"- Arrow IPC files: `{dataset['fact_arrow_ipc_bytes']}` fact bytes, `{dataset['dim_arrow_ipc_bytes']}` dimension bytes",
        f"- Avro files: `{dataset['fact_avro_bytes']}` fact bytes, `{dataset['dim_avro_bytes']}` dimension bytes",
        f"- ORC files: `{dataset['fact_orc_bytes']}` fact bytes, `{dataset['dim_orc_bytes']}` dimension bytes",
        f"- Python: `{env['python_version']}`",
        f"- Platform: `{env['platform']}`",
        f"- CPU count: `{env['cpu_count']}`",
        f"- Fallback execution allowed: `{artifact['fallback_execution_allowed']}`",
        f"- Performance claim allowed: `{artifact['performance_claim_allowed']}`",
        "",
        "## Read This First",
        "",
        render_read_this_first(artifact),
        "",
        "## Fairness Parameters",
        "",
        render_fairness_parameters(artifact),
        "",
        "## Execution Mode Attribution Contract",
        "",
        "Every benchmark row must carry these mode and stage fields so timing is read as the correct workflow, not as a hidden performance claim.",
        "",
        render_execution_mode_attribution_contract(artifact),
        "",
        "## Persistent Runner Admission Gate",
        "",
        "This report-only gate records why process-per-scenario execution remains visible and what a future persistent runner must preserve before it can be admitted.",
        "",
        render_persistent_runner_admission_gate(artifact),
        "",
        "## SourceState Contract",
        "",
        "This contract makes source discovery, schema identity, parse/decode planning, fingerprinting, and reuse posture visible without implying Vortex-native execution, output support, or performance claims.",
        "",
        render_source_state_contract(artifact),
        "",
        "## VortexPreparedState Contract",
        "",
        "This contract makes prepared Vortex artifact identity, preparation timing separation, source-state linkage, and scoped reuse posture visible without implying output support or performance claims.",
        "",
        render_prepared_state_contract(artifact),
        "",
        "## OutputPlan Contract",
        "",
        "This contract makes local output-plan posture, target format/schema, metadata preservation, write/replay refs, and sink artifact identity visible without implying fanout, object-store, table commit, or production sink support.",
        "",
        render_output_plan_contract(artifact),
        "",
        "## I/O Reuse And Fanout Benchmark Contract",
        "",
        "This contract makes the required cross-format fanout benchmark cases visible as deterministic report-only rows until multi-output local fanout runtime and replay evidence exist.",
        "",
        render_fanout_benchmark_contract(artifact),
        "",
        "## Cache Invalidation And Fingerprint Contract",
        "",
        "This contract makes local source/prepared/plan/output fingerprints and invalidation posture visible without adding a persistent cache, hidden fast mode, object-store cache, or performance claim.",
        "",
        render_cache_invalidation_contract(artifact),
        "",
        "## Evidence-Safe Reuse Level Contract",
        "",
        "This contract normalizes per-layer reuse status across source discovery, schema, parse planning, prepared Vortex state, operator source-state, output planning, and result replay without adding a hidden cache or claim-grade promotion.",
        "",
        render_reuse_level_contract(artifact),
        "",
        "## Build Profile Contract",
        "",
        "This contract records compiler/toolchain posture, LTO, PGO, and host-native benchmark-only boundaries without turning build settings into performance or release claims.",
        "",
        render_build_profile_contract(artifact),
        "",
        "## Engine Overview",
        "",
        render_engine_overview(artifact),
        "",
        "## Scenario Timing Matrix",
        "",
        "Values are mean per-iteration query/runtime milliseconds for successful rows. Failed rows show their status.",
        "",
        render_scenario_matrix(artifact),
        "",
        "## Support And Coverage Matrix",
        "",
        "Coverage is separate from timing. External engines are comparison baselines only, and ShardLoom rows must keep certificate, Native I/O, materialization, and no-fallback evidence visible.",
        "",
        render_coverage_table(artifact),
        "",
        "## ShardLoom Format Preparation Matrix",
        "",
        "This table separates compatibility source preparation from prepared/native Vortex query timing. CSV, JSONL, Parquet, Arrow IPC, Avro, and ORC remain compatibility preparation inputs; Vortex is the native execution format.",
        "",
        render_format_preparation_matrix(artifact),
        "",
        "## ShardLoom SourceState Evidence Matrix",
        "",
        "SourceState is input preparation evidence. Reuse fields show whether local source preparation state was reusable for this row; they do not upgrade claim_gate_status or create object-store/lakehouse/SQL/DataFrame support.",
        "",
        render_source_state_matrix(artifact),
        "",
        "## ShardLoom VortexPreparedState Evidence Matrix",
        "",
        "VortexPreparedState is prepared-artifact evidence. Reuse fields show whether prepared Vortex artifacts were eligible for reuse or reused; they do not upgrade claim_gate_status or create output/object-store/lakehouse/SQL/DataFrame support.",
        "",
        render_prepared_state_matrix(artifact),
        "",
        "## ShardLoom OutputPlan Evidence Matrix",
        "",
        "OutputPlan is output-planning evidence. Local Vortex result-sink rows can report scoped output-plan support only when write/replay evidence is present; other rows remain explicit `not_needed`, `report_only`, unsupported, or external-baseline posture.",
        "",
        render_output_plan_matrix(artifact),
        "",
        "## ShardLoom I/O Reuse And Fanout Benchmark Matrix",
        "",
        "Fanout rows list the required benchmark cases and current blockers. They are local workflow/evidence posture only, not runtime fanout support or speed rankings.",
        "",
        render_fanout_benchmark_matrix(artifact),
        "",
        "## ShardLoom Cache Invalidation And Fingerprint Matrix",
        "",
        "Cache invalidation rows expose current local fingerprint posture and deterministic reuse eligibility. They are not persistent cache hits, performance claims, or object-store cache support.",
        "",
        render_cache_invalidation_matrix(artifact),
        "",
        "## ShardLoom Evidence-Safe Reuse Level Matrix",
        "",
        "Reuse-level rows keep execution mode, evidence level, output format, reuse status, and claim gate separate. Reuse hits or misses are visibility evidence only; they do not prove correctness, output fidelity, performance, or production support.",
        "",
        render_reuse_level_matrix(artifact),
        "",
        "## Resource Metrics",
        "",
        "Memory is sampled process RSS when `psutil` is available. Bytes read and written are declared local file bytes for the scenario; ShardLoom bytes written include temporary Vortex artifacts from the universal-I/O smoke path.",
        "",
        render_resource_metrics_table(artifact),
        "",
        "## ShardLoom Runtime Effects",
        "",
        "These fields come from ShardLoom's CLI evidence and make decode, materialization, row-read, Arrow, object-store, write, spill, and native-I/O-certificate status explicit.",
        "",
        render_shardloom_effects_table(artifact),
        "",
        "## Fastest Successful Rows",
        "",
        render_fastest_table(artifact),
        "",
        "## ShardLoom Native Microbenchmarks",
        "",
        "These rows are not directly comparable to compatibility-file engine rows. They show the current native encoded/Vortex path that ShardLoom can execute today.",
        "",
        render_shardloom_native_table(artifact),
        "",
        "## ShardLoom Decision / Why Evidence",
        "",
        "These fields explain why each native runtime row is or is not claim-grade. They are derived from `vortex-run` DecisionTrace/WhyReport evidence.",
        "",
        render_shardloom_why_table(artifact),
        "",
        "## ShardLoom Work-Avoidance Evidence",
        "",
        "These fields come from `vortex-run` runtime effects. Unknown segment-prune and bytes-not-read values stay explicit until the runtime can measure them safely.",
        "",
        render_shardloom_work_avoidance_table(artifact),
        "",
        "## ShardLoom Write/Commit Evidence",
        "",
        "This local-only smoke row measures the current committed-manifest step. It is not an object-store or table-format commit benchmark.",
        "",
        render_shardloom_commit_table(artifact),
        "",
        "## ShardLoom Result-Sink Write Timing",
        "",
        "These rows separate scenario compute timing from the certified local Vortex result-sink write/replay timing when `--shardloom-result-sink` is enabled.",
        "",
        render_shardloom_result_sink_table(artifact),
        "",
        "## Universal I/O And Compatibility-To-Vortex Lanes",
        "",
        "These lanes make the ShardLoom universal-I/O boundary explicit instead of hiding compatibility-format import behind external baseline rows.",
        "",
        render_universal_io_table(artifact),
        "",
        "## Timing Bars",
        "",
        render_timing_bars(artifact),
        "",
        "## Correctness",
        "",
        render_correctness_table(artifact),
        "",
        "## Errors And Unsupported Rows",
        "",
        render_errors_table(artifact),
        "",
        "## Limitations",
        "",
    ]
    for limitation in artifact["limitations"]:
        lines.append(f"- {limitation}")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    global DASK_BLOCKSIZE, DASK_SCHEDULER, SHARDLOOM_BUILD_PROFILE, SHARDLOOM_RESULT_SINK
    global SHARDLOOM_EVIDENCE_LEVEL
    args = parse_args()
    DASK_BLOCKSIZE = args.dask_blocksize
    DASK_SCHEDULER = args.dask_scheduler
    SHARDLOOM_BUILD_PROFILE = args.shardloom_build_profile
    SHARDLOOM_RESULT_SINK = args.shardloom_result_sink
    SHARDLOOM_EVIDENCE_LEVEL = args.shardloom_evidence_level
    configure_java_home()
    paths = ensure_dataset(
        args.data_dir,
        args.rows,
        args.dim_rows,
        args.regenerate,
        args.format_list,
        args.dataset_profile,
    )
    report_formats = report_format_order(args)
    scenario_order = expanded_scenario_order(report_formats, args.scenario_list)
    runners, missing = available_runners(args.engine_list)

    results: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    def record_result(result: dict[str, Any]) -> None:
        annotate_result(result, args.cache_mode, args.dataset_profile)
        results.append(result)
        if result["status"] != "success":
            errors.append(
                {
                    "engine": result["engine"],
                    "scenario": result["scenario_name"],
                    "status": result["status"],
                    "reason": result.get("reason", "scenario did not complete"),
                }
            )

    for engine in args.engine_list:
        runner = runners.get(engine)
        engine_formats = formats_for_engine_report(engine, runner, report_formats)
        if runner is None:
            reason = missing.get(engine, "engine was not initialized")
            for data_format in engine_formats:
                for scenario in args.scenario_list:
                    profile_block = scenario_dataset_profile_block_reason(
                        scenario, args.dataset_profile
                    )
                    result = failed_result(
                        engine,
                        scenario,
                        data_format,
                        "missing_dependency",
                        reason if not profile_block else f"{reason}; {profile_block}",
                        paths,
                        args.iterations,
                    )
                    record_result(result)
            continue
        try:
            try:
                runner = warmup_runner(runner)
                runner = prepare_runner(runner, paths, engine_formats)
                runners[engine] = runner
            except Exception as exc:
                reason = f"{type(exc).__name__}: {exc}"
                for data_format in engine_formats:
                    for scenario in args.scenario_list:
                        result = failed_result(
                            engine,
                            scenario,
                            data_format,
                            "engine_startup_error",
                            reason,
                            paths,
                            args.iterations,
                        )
                        record_result(result)
                continue
            for data_format in engine_formats:
                runnable_scenarios = []
                for scenario in args.scenario_list:
                    profile_block = scenario_dataset_profile_block_reason(
                        scenario, args.dataset_profile
                    )
                    if profile_block:
                        result = failed_result(
                            engine,
                            scenario,
                            data_format,
                            "blocked",
                            profile_block,
                            paths,
                            args.iterations,
                        )
                        record_result(result)
                    elif data_format not in runner.formats:
                        result = failed_result(
                            engine,
                            scenario,
                            data_format,
                            "unsupported_format",
                            f"{engine} does not support {data_format} in this harness",
                            paths,
                            args.iterations,
                        )
                        record_result(result)
                    else:
                        runnable_scenarios.append(scenario)
                if not runnable_scenarios:
                    continue
                if runner.batch_scenarios is not None:
                    try:
                        for result in run_batch(
                            runner,
                            paths,
                            tuple(runnable_scenarios),
                            data_format,
                            args.iterations,
                        ):
                            record_result(result)
                        continue
                    except BenchmarkUnsupported:
                        # The batch command is an optimization of the benchmark harness process
                        # boundary, not a runtime fallback. Keep a deterministic report by
                        # running the existing single-scenario CLI path explicitly.
                        pass
                    except RuntimeError as exc:
                        for scenario in runnable_scenarios:
                            result = failed_result(
                                engine,
                                scenario,
                                data_format,
                                "execution_error",
                                f"{type(exc).__name__}: {exc}",
                                paths,
                                args.iterations,
                            )
                            record_result(result)
                        continue
                for scenario in runnable_scenarios:
                    result = run_one(runner, paths, scenario, data_format, args.iterations)
                    record_result(result)
        finally:
            if runner.close is not None:
                runner.close()

    engine_versions = {
        engine: {
            "available": engine in runners,
            "version": runners[engine].version,
            "startup_time_millis": runners[engine].startup_time_millis,
            "preparation_time_millis": runners[engine].preparation_time_millis,
            "build_time_millis": runners[engine].build_time_millis,
            "build_time_excluded": True,
        }
        for engine in runners
    }
    for engine, reason in missing.items():
        engine_versions[engine] = {
            "available": False,
            "version": None,
            "reason": reason,
            "startup_time_millis": None,
            "build_time_millis": None,
            "build_time_excluded": True,
        }

    artifact = {
        "schema_version": "shardloom.traditional_analytics_benchmark.v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "benchmark_scope": "traditional_analytics_comparative_harness",
        "fallback_execution_allowed": False,
        "external_engines_are_fallback": False,
        "performance_claim_allowed": False,
        "claim_readiness_rerun_profile": args.claim_readiness_rerun,
        "claim_grade_min_iterations": MIN_CLAIM_GRADE_ITERATIONS,
        "dataset": {
            "rows": paths.rows,
            "dim_rows": paths.dim_rows,
            "dataset_profile": args.dataset_profile,
            "dataset_file_shape": dataset_file_shape(args.dataset_profile),
            "fact_extra_columns": list(paths.fact_extra_columns),
            "fact_csv": str(paths.fact_csv),
            "dim_csv": str(paths.dim_csv),
            "fact_jsonl": str(paths.fact_jsonl),
            "dim_jsonl": str(paths.dim_jsonl),
            "fact_parquet": str(paths.fact_parquet),
            "dim_parquet": str(paths.dim_parquet),
            "fact_arrow_ipc": str(paths.fact_arrow_ipc),
            "dim_arrow_ipc": str(paths.dim_arrow_ipc),
            "fact_avro": str(paths.fact_avro),
            "dim_avro": str(paths.dim_avro),
            "fact_orc": str(paths.fact_orc),
            "dim_orc": str(paths.dim_orc),
            "fact_csv_parts_dir": str(paths.fact_csv_parts_dir)
            if paths.fact_csv_parts_dir is not None
            else None,
            "fact_jsonl_parts_dir": str(paths.fact_jsonl_parts_dir)
            if paths.fact_jsonl_parts_dir is not None
            else None,
            "fact_csv_part_count": len(fact_part_paths(paths, "csv")),
            "fact_jsonl_part_count": len(fact_part_paths(paths, "jsonl")),
            "cdc_delta_csv": str(paths.cdc_delta_csv)
            if paths.cdc_delta_csv is not None and paths.cdc_delta_csv.exists()
            else None,
            "nested_jsonl": str(paths.nested_jsonl)
            if paths.nested_jsonl is not None and paths.nested_jsonl.exists()
            else None,
            "fact_csv_bytes": paths.fact_csv.stat().st_size,
            "dim_csv_bytes": paths.dim_csv.stat().st_size,
            "fact_jsonl_bytes": maybe_path_size(paths.fact_jsonl),
            "dim_jsonl_bytes": maybe_path_size(paths.dim_jsonl),
            "fact_parquet_bytes": maybe_path_size(paths.fact_parquet),
            "dim_parquet_bytes": maybe_path_size(paths.dim_parquet),
            "fact_arrow_ipc_bytes": maybe_path_size(paths.fact_arrow_ipc),
            "dim_arrow_ipc_bytes": maybe_path_size(paths.dim_arrow_ipc),
            "fact_avro_bytes": maybe_path_size(paths.fact_avro),
            "dim_avro_bytes": maybe_path_size(paths.dim_avro),
            "fact_orc_bytes": maybe_path_size(paths.fact_orc),
            "dim_orc_bytes": maybe_path_size(paths.dim_orc),
            "deterministic_generator": "benchmarks/traditional_analytics/run.py",
        },
        "environment": environment_report(),
        "fairness_parameters": fairness_parameters(args, paths),
        "execution_mode_attribution_contract": execution_mode_attribution_contract(),
        "persistent_runner_admission_gate": persistent_runner_admission_gate(),
        "source_state_contract": source_state_contract(),
        "prepared_state_contract": prepared_state_contract(),
        "output_plan_contract": output_plan_contract(),
        "fanout_benchmark_contract": fanout_benchmark_contract(),
        "cache_invalidation_contract": cache_invalidation_contract(),
        "reuse_level_contract": reuse_level_contract(),
        "build_profile_contract": build_profile_contract(),
        "work_avoidance_evidence_schema": work_avoidance_evidence_schema(),
        "engine_order": list(args.engine_list),
        "engine_versions": engine_versions,
        "format_order": list(report_formats),
        "scenario_order": scenario_order,
        "scenario_catalog_path": str(scenario_catalog_path()),
        "scenario_catalog": catalog_coverage_summary(SCENARIO_CATALOG),
        "coverage_table": coverage_table(results),
        "format_preparation_matrix": format_preparation_matrix(results),
        "source_state_matrix": source_state_matrix(results),
        "prepared_state_matrix": prepared_state_matrix(results),
        "output_plan_matrix": output_plan_matrix(results),
        "fanout_benchmark_matrix": fanout_benchmark_matrix(results),
        "cache_invalidation_matrix": cache_invalidation_matrix(results),
        "reuse_level_matrix": reuse_level_matrix(results),
        "results": results,
        "shardloom_native_microbenchmarks": []
        if args.skip_shardloom_native
        else run_shardloom_native_microbenchmarks(args.shardloom_native_iterations),
        "universal_io_lanes": universal_io_lanes(),
        "correctness": correctness_summary(results, tuple(scenario_order)),
        "errors": errors,
        "limitations": [
            "Compatibility-file workloads include local file read cost and do not represent object-store behavior.",
            "Parquet, Arrow IPC, Avro, and ORC workloads use generated local files with engine-default read settings; they do not represent tuned lakehouse/table-format layouts.",
            "ShardLoom traditional rows include local compatibility-to-Vortex import and Vortex scan, but current temporary operators materialize Vortex-derived arrays instead of executing the full mature encoded SQL/operator surface.",
            "ShardLoom native/prepared Vortex rows are reported under requested source-format rows and exclude compatibility-to-Vortex setup from scenario timing; preparation timing and artifact refs are recorded separately.",
            "ShardLoom direct-transient rows currently cover only scoped local CSV selective-filter and filter + projection + limit smoke paths and do not permit Vortex-native, SQL/DataFrame, or performance-superiority claims.",
            "ShardLoom native microbenchmark rows separately expose local Vortex scan filter/projection pushdown evidence; those rows are not a mature SQL/DataFrame/API benchmark surface.",
            "Dask performance is sensitive to partitioning and scheduler settings; this report records the selected blocksize and scheduler.",
            "Engine startup/warmup time is recorded separately from per-scenario timing. Spark profiles warm an isolated Spark session before their scenario rows and are closed before the next engine runs.",
            "Peak memory is sampled process RSS when psutil is available and may miss short-lived spikes.",
            "ShardLoom traditional rows use the native Rust benchmark command, not the future SQL parser/dataframe API.",
            "This artifact is benchmark evidence only and does not permit performance or superiority claims by itself.",
        ],
    }

    output_path = args.output or default_output_path()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(artifact, handle, indent=2, sort_keys=True)
        handle.write("\n")
    print(output_path)
    if not args.no_markdown:
        report_path = markdown_output_path(output_path, args.markdown_output)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(render_markdown_report(artifact), encoding="utf-8")
        print(report_path)

    if args.require_all_engines and any(
        error["status"] == "missing_dependency" for error in errors
    ):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
