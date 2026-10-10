from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import os
import importlib.util
import re
import subprocess
import sys
import tempfile
import textwrap
import unittest
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from release_report_utils import (
    python_package_version,
    upstream_vortex_lock_version,
    upstream_vortex_manifest_version,
    upstream_vortex_provider_version,
    workspace_package_version,
    workspace_rust_version,
    workspace_version_env,
)
from release_channel_contract import (
    PUBLISHED_REGISTRY_BUILD_IDENTITIES,
    PUBLISHED_REGISTRY_DISTRIBUTIONS,
    SELECTED_PACKAGE_CHANNEL_STATUS_MARKER,
    SELECTED_PACKAGE_RELEASE_TAG,
    SELECTED_PACKAGE_RELEASE_VERSION,
    SELECTED_V0_1_0_FEASIBILITY_STATUS,
    SELECTED_V0_1_0_PUBLICATION_AUTHORIZATION_STATUS,
    SELECTED_V0_1_0_RELEASE_CHANNEL_IDS,
)
from release_feature_contract import RELEASE_USER_SURFACE_EXAMPLE_FEATURES

CURRENT_RUST_VERSION = workspace_rust_version(REPO_ROOT)
CURRENT_WORKSPACE_PACKAGE_VERSION = workspace_package_version(REPO_ROOT)
CURRENT_PYTHON_PACKAGE_VERSION = python_package_version(REPO_ROOT)
CURRENT_VORTEX_MANIFEST_VERSION = upstream_vortex_manifest_version(REPO_ROOT)
CURRENT_VORTEX_LOCK_VERSION = upstream_vortex_lock_version(REPO_ROOT)
UPSTREAM_VORTEX_PROVIDER_VERSION = upstream_vortex_provider_version(REPO_ROOT)
WORKSPACE_VERSION_ENV = workspace_version_env(REPO_ROOT)


class ReleaseScriptTests(unittest.TestCase):
    def _load_script_module(self, script_name: str, module_name: str) -> object:
        module_path = REPO_ROOT / "scripts" / script_name
        return self._load_module_from_path(module_path, module_name)

    def _load_module_from_path(self, module_path: Path, module_name: str) -> object:
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        self.assertIsNotNone(spec)
        self.assertIsNotNone(spec.loader)
        module = importlib.util.module_from_spec(spec)
        script_dir = str(module_path.parent)
        inserted = False
        if script_dir not in sys.path:
            sys.path.insert(0, script_dir)
            inserted = True
        previous_module = sys.modules.get(module_name)
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)
        finally:
            if previous_module is None:
                sys.modules.pop(module_name, None)
            else:
                sys.modules[module_name] = previous_module
        if inserted:
            sys.path.remove(script_dir)
        return module

    def test_front_door_control_plane_probe_reports_first_and_steady_stats(self) -> None:
        module = self._load_script_module(
            "run_front_door_control_plane_probe.py",
            "front_door_control_plane_probe_for_test",
        )

        split = module.first_and_steady_stats([5.0, 2.0, 4.0])

        self.assertEqual(split["first_request_ms"], 5.0)
        self.assertEqual(split["steady_state"]["count"], 2)
        self.assertEqual(split["steady_state"]["median_ms"], 3.0)
        self.assertEqual(split["all_requests"]["count"], 3)
        self.assertEqual(split["all_requests"]["max_ms"], 5.0)

    @contextlib.contextmanager
    def _temporary_env(self, **updates: str):
        previous = {key: os.environ.get(key) for key in updates}
        os.environ.update(updates)
        try:
            yield
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def _canonical_route_timing_stage_ids(self) -> tuple[str, ...]:
        return (
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

    def _packed_route_stage_map(self, value: str) -> str:
        return ";".join(
            f"{stage_id}:{value}" for stage_id in self._canonical_route_timing_stage_ids()
        )

    def test_runtime_envelope_validator_includes_hot_runtime_non_claim_grade_rows(self) -> None:
        module = self._load_script_module(
            "check_runtime_execution_envelopes.py",
            "check_runtime_execution_envelopes_hot_runtime_test",
        )

        self.assertTrue(
            module.should_validate_benchmark_row(
                {
                    "engine": "shardloom-vortex",
                    "timing_surface": "hot_runtime",
                    "claim_gate_status": "not_claim_grade",
                }
            )
        )
        self.assertFalse(
            module.should_validate_benchmark_row(
                {
                    "engine": "duckdb",
                    "timing_surface": "hot_runtime",
                    "claim_gate_status": "claim_grade",
                }
            )
        )

    def _shardloom_benchmark_route_fields(
        self,
        engine: str = "shardloom-prepare-batch",
    ) -> dict[str, object]:
        lane_by_engine = {
            "shardloom": (
                "cold_certified_route",
                "ShardLoom Cold Certified Route",
                "raw_compat_source",
                True,
                "total_route_ms = total_runtime_millis",
                "cold_certified_route_total",
                "total_runtime_millis",
            ),
            "shardloom-prepared-vortex": (
                "warm_prepared_query",
                "ShardLoom Warm Prepared Query",
                "VortexPreparedState",
                False,
                (
                    "total_route_ms = query_runtime_millis + "
                    "result_sink_write_millis + evidence_render_millis"
                ),
                "warm_prepared_query_only",
                "query_runtime_millis,result_sink_write_millis,evidence_render_millis",
            ),
            "shardloom-prepare-batch": (
                "prepare_once_batch",
                "ShardLoom Prepare-Once Batch",
                "raw_compat_source",
                True,
                (
                    "total_route_ms = amortized_prepare_batch_preparation_millis + "
                    "query_runtime_millis + result_sink_write_millis + "
                    "evidence_render_millis"
                ),
                "prepare_once_batch_amortized",
                (
                    "amortized_prepare_batch_preparation_millis,query_runtime_millis,"
                    "result_sink_write_millis,evidence_render_millis"
                ),
            ),
            "shardloom-vortex": (
                "native_vortex_query",
                "ShardLoom Native Vortex Query",
                "Vortex",
                False,
                (
                    "total_route_ms = query_runtime_millis + "
                    "result_sink_write_millis + evidence_render_millis"
                ),
                "native_vortex_query_only",
                "query_runtime_millis,result_sink_write_millis,evidence_render_millis",
            ),
        }
        (
            lane_id,
            display_name,
            start_state,
            preparation_included,
            formula,
            timing_scope,
            included_stage_ids,
        ) = lane_by_engine[engine]
        cold_route = lane_id in {
            "cold_certified_route",
            "prepare_once_first_query",
            "prepare_once_batch",
        }
        if lane_id == "warm_prepared_query":
            reuse_fields = {
                "prepared_state_reuse_scope": "explicit_prepared_state_input",
                "prepared_state_reuse_manifest_path": "not_required_existing_prepared_state",
                "prepared_state_reuse_policy": "explicit_prepared_state_admission.v1",
                "prepared_state_reuse_hit": True,
                "prepared_state_reuse_reason": "explicit_prepared_state_input",
                "prepared_state_reuse_manifest_digest": "fnv64:prepared",
                "prepared_state_invalidation_reason": (
                    "artifact_admission_failure_or_policy_mismatch"
                ),
            }
        elif lane_id == "prepare_once_batch":
            reuse_fields = {
                "prepared_state_reuse_scope": "in_process_prepared_batch_vortex_artifacts",
                "prepared_state_reuse_manifest_path": "not_required_in_process_prepared_batch",
                "prepared_state_reuse_policy": "in_process_prepared_batch_reuse.v1",
                "prepared_state_reuse_hit": True,
                "prepared_state_reuse_reason": "prepared_state_reused_inside_batch",
                "prepared_state_reuse_manifest_digest": "fnv64:prepared",
                "prepared_state_invalidation_reason": (
                    "not_applicable_same_process_or_explicit_prepared_state"
                ),
            }
        else:
            reuse_fields = {
                "prepared_state_reuse_scope": "prepared_state_created_not_reused"
                if cold_route
                else "not_applicable_native_vortex_input",
                "prepared_state_reuse_manifest_path": "not_applicable_first_preparation"
                if cold_route
                else "not_applicable_native_vortex_input",
                "prepared_state_reuse_policy": (
                    "first_preparation_creates_vortex_prepared_state.v1"
                    if cold_route
                    else "not_applicable_native_vortex_input"
                ),
                "prepared_state_reuse_hit": False,
                "prepared_state_reuse_reason": "prepared_state_reuse_not_requested_for_route",
                "prepared_state_reuse_manifest_digest": (
                    "not_applicable_no_reuse_manifest_for_route"
                ),
                "prepared_state_invalidation_reason": "not_applicable_no_reuse_attempt",
            }
        canonical_stage_ids = ",".join(self._canonical_route_timing_stage_ids())
        return {
            "route_lane_id": lane_id,
            "route_display_name": display_name,
            "route_runtime_status": "global_runtime_supported",
            "start_state": start_state,
            "end_state": "result_sink",
            "includes_preparation": preparation_included,
            "includes_query": True,
            "includes_output": True,
            "includes_evidence": True,
            "route_comparable_to_external_end_to_end": True,
            "preparation_included": preparation_included,
            "query_timing_starts_after_preparation": lane_id != "cold_certified_route",
            "prepared_state_reused": lane_id in {"prepare_once_batch", "warm_prepared_query"},
            "route_timing_ledger_schema_version": "shardloom.route_timing_ledger.v1",
            "route_timing_ledger_status": "valid",
            "route_timing_surface_schema_version": "shardloom.route_timing_surface.v1",
            "timing_surface": "publication_proof",
            "timing_surface_label": "Publication proof",
            "timing_surface_evidence_tier": "publication_full",
            "timing_surface_default_for_route": False,
            "timing_surface_claim_boundary": "fixture_publication_proof_no_claim",
            "route_total_formula": formula,
            "route_timing_scope": timing_scope,
            "stage_parent_id": lane_id,
            "route_timing_included_stage_ids": included_stage_ids,
            "route_timing_excluded_stage_ids": "none",
            "route_timing_included_stage_total_ms": 1.0,
            "route_timing_total_delta_ms": 0.0,
            "timing_normalization_schema_version": (
                "shardloom.traditional_analytics.timing_normalization.v1"
            ),
            "timing_normalization_status": "complete_with_unmeasured_optional_fields",
            "source_admission_policy_micros": 0,
            "source_admission_digest_policy_schema_version": (
                "shardloom.traditional_analytics.source_admission_digest_policy.v1"
            ),
            "source_admission_digest_policy_status": (
                "metadata_fingerprint_fast_path"
            ),
            "source_admission_full_content_digest_requested": False,
            "source_admission_full_content_digest_micros": 0,
            "source_stat_micros": 0,
            "source_state_open_micros": None,
            "source_state_metadata_snapshot_micros": None,
            "source_state_manifest_validation_micros": None,
            "source_state_row_count_metadata_micros": None,
            "source_state_family_build_micros": None,
            "source_state_lazy_family_construction": None,
            "source_state_family_build_timing_scope": "not_reported_by_engine",
            "source_state_family_build_count": None,
            "source_state_family_prewarm_status": "not_reported_by_engine",
            "source_state_family_prewarm_eligible_count": None,
            "source_state_family_prewarm_count": None,
            "source_state_family_prewarm_already_prepared_count": None,
            "source_state_family_prewarm_prepared_before_child_route_count": None,
            "source_state_family_prewarm_micros": None,
            "source_state_family_prewarm_scope": "not_reported_by_engine",
            "source_state_family_reuse_hit_count": None,
            "source_state_family_reuse_hit": None,
            "source_state_family_recompute_avoided": None,
            "source_state_digest_micros": None,
            "prepared_manifest_read_micros": None,
            "prepared_manifest_match_micros": None,
            "vortex_open_footer_micros": None,
            "scan_open_micros": None,
            "scan_chunk_iter_micros": None,
            "operator_kernel_micros": 100,
            "operator_finalize_micros": None,
            "result_sink_plan_micros": None,
            "result_sink_write_micros": 100,
            "result_sink_replay_micros": 100,
            "human_evidence_render_micros": 100,
            "json_envelope_emit_micros": None,
            "report_fields_build_micros": None,
            "cli_process_wall_micros": None,
            "route_timing_stage_inclusion_schema_version": (
                "shardloom.route_timing_stage_inclusion.v1"
            ),
            "route_timing_stage_inclusion_status": "complete",
            "route_timing_stage_inclusion_stage_ids": canonical_stage_ids,
            "route_timing_stage_inclusion_classes": self._packed_route_stage_map(
                "included"
            ),
            "route_timing_stage_inclusion_stage_owners": self._packed_route_stage_map(
                "fixture"
            ),
            "route_timing_stage_inclusion_timing_scopes": self._packed_route_stage_map(
                "fixture_route_total"
            ),
            "route_timing_stage_inclusion_skip_reasons": self._packed_route_stage_map(
                "included_in_route_total"
            ),
            "route_timing_stage_inclusion_claim_boundary": "fixture_no_claim",
            "route_timing_instrument_schema_version": "shardloom.route_timing_instrument.v1",
            "route_timing_instrument_status": "optimization_ready",
            "route_timing_instrument_stage_ids": canonical_stage_ids,
            "route_timing_instrument_stage_parent_stages": self._packed_route_stage_map(
                "fixture_parent"
            ),
            "route_timing_instrument_stage_groups": self._packed_route_stage_map(
                "route_total_stage"
            ),
            "route_timing_instrument_stage_owners": self._packed_route_stage_map(
                "fixture"
            ),
            "route_timing_instrument_inclusion_classes": self._packed_route_stage_map(
                "included"
            ),
            "route_timing_instrument_timing_scopes": self._packed_route_stage_map(
                "fixture_route_total"
            ),
            "route_timing_instrument_evidence_levels": self._packed_route_stage_map(
                "publication_full"
            ),
            "route_timing_instrument_residual_treatments": self._packed_route_stage_map(
                "included_in_route_total_with_exclusive_residual_audited"
            ),
            "route_timing_instrument_substage_fields": self._packed_route_stage_map(
                "fixture_substage_field"
            ),
            "route_timing_instrument_missing_substage_attribution": "none",
            "route_timing_instrument_expensive_stage_threshold_ms": 10.0,
            "route_timing_instrument_expensive_stage_ids": "none",
            "route_timing_instrument_not_ready_stage_ids": "none",
            "route_timing_instrument_claim_boundary": "fixture_no_claim",
            "exclusive_stage_timing_schema_version": (
                "shardloom.traditional_analytics.exclusive_stage_timing.v1"
            ),
            "exclusive_stage_timing_status": "complete",
            "exclusive_stage_timing_scope": "fixture_deoverlapped_route_stage_fields",
            "exclusive_stage_included_stage_ids": (
                "source_admission,source_read,source_parse_or_decode,"
                "vortex_array_build,vortex_write,prepared_query,sink_output,"
                "evidence_render"
            ),
            "route_timing_exclusive_stage_ids": (
                "source_admission,source_read,source_parse_or_decode,"
                "vortex_array_build,vortex_write,prepared_query,sink_output,"
                "evidence_render"
            ),
            "route_timing_exclusive_stage_sum_ms": 0.9,
            "route_timing_exclusive_residual_ms": 0.1,
            "route_timing_exclusive_total_delta_ms": 0.1,
            "route_timing_exclusive_residual_status": "auditable_residual",
            "inclusive_compatibility_to_vortex_import_ms": 0.5 if cold_route else None,
            "inclusive_compatibility_to_vortex_import_timing_scope": (
                "source_read_parse_including_columnar_decode_plus_vortex_array_build_plus_vortex_write"
                if cold_route
                else "not_applicable_non_cold_route"
            ),
            "exclusive_source_admission_ms": 0.0,
            "exclusive_source_read_ms": 0.0,
            "exclusive_source_parse_or_decode_ms": 0.1,
            "exclusive_source_to_vortex_array_ms": 0.2,
            "exclusive_vortex_write_ms": 0.3,
            "exclusive_vortex_digest_ms": 0.0,
            "exclusive_vortex_reopen_verify_ms": 0.0,
            "exclusive_prepared_query_ms": 0.1,
            "exclusive_result_sink_write_ms": 0.1,
            "exclusive_evidence_render_ms": 0.1,
            "exclusive_stage_timing_claim_boundary": "fixture_no_claim",
            "preparation_timing_included_in_total": preparation_included,
            "query_timing_included_in_total": True,
            "output_timing_included_in_total": True,
            "evidence_timing_included_in_total": True,
            "fast_path_attribution_schema_version": "shardloom.route_fast_path_attribution.v1",
            "runtime_execution_ms": 0.8,
            "output_delivery_ms": 0.1,
            "evidence_capture_ms": 0.0,
            "evidence_render_ms": 0.1,
            "certificate_link_ms": 0.0,
            "runtime_execution_timing_scope": timing_scope,
            "output_delivery_timing_scope": (
                "included_in_route_total"
            ),
            "evidence_capture_timing_status": "certificate_metadata_linked_not_separately_timed",
            "certificate_link_timing_status": "metadata_linked_not_separately_timed",
            "runtime_execution_certificate_id": "execution://fixture",
            "runtime_execution_certificate_status": "certified",
            "runtime_execution_certificate_plan_ref": "scheduler://fixture",
            "certificate_link_status": "linked_certified_runtime_execution",
            "evidence_required_for_claim": True,
            "evidence_render_included_in_route_total": True,
            "evidence_sink_tier_schema_version": (
                "shardloom.traditional_analytics.evidence_sink_tier.v1"
            ),
            "requested_evidence_tier": "publication_full",
            "actual_evidence_tier": "publication_full",
            "selected_evidence_tier": "publication_full",
            "sink_tier": "publication_full",
            "evidence_tier_supported_tiers": (
                "runtime_minimal,metadata_sink,full_vortex_replay,publication_full"
            ),
            "evidence_tier_result_sink_replay_required": True,
            "sink_timing_included_in_route_total": True,
            "sink_timing_inclusion_reason": (
                "publication_full_write_and_human_evidence_in_cli_route_wall"
            ),
            "result_sink_replay_skip_reason": "not_skipped_replay_required",
            "human_evidence_render_skip_reason": (
                "not_skipped_publication_full_requires_human_render"
            ),
            "computed_result_sink_replay_verified": "true",
            "fast_path_claim_boundary": "runtime fast path fixture",
            "operator_mode_inventory_schema_version": "shardloom.operator_mode_inventory.v1",
            "operator_execution_class": "residual_native",
            "operator_admission_status": "residual_native_supported",
            "operator_encoded_native_claim_allowed": False,
            "operator_residual_native_used": True,
            "operator_temporary_materialization_used": False,
            "operator_blocker_matrix_ref": "operator-blocker://fixture",
            "operator_execution_mode": "residual_native",
            "encoded_native_operators": "none",
            "residual_native_operators": "shardloom_native_residual_operator",
            "materialized_temporary_operators": "none",
            "operator_blocker_code": "gar-flow-2b.residual_native_operator_not_encoded_native",
            "operator_hot_path_candidate": "residual_native_operator_encoding_promotion",
            "operator_hot_path_candidate_status": "blocked_residual_native_operator_not_encoded_native",
            "operator_hot_path_next_step": (
                "add decoded-reference correctness and encoded kernel evidence before "
                "encoded-native promotion"
            ),
            "operator_mode_claim_boundary": (
                "runtime supported is not encoded-native support"
            ),
            "total_route_ms": 1.0,
            "cold_bottleneck_schema_version": "shardloom.traditional_analytics.cold_bottleneck.v1",
            "cold_bottleneck_status": (
                "complete" if cold_route else "not_applicable_non_cold_route"
            ),
            "cold_bottleneck_stage_labels": (
                "source_admission,source_read,source_parse_or_decode,source_state_build,"
                "vortex_array_build,vortex_write,vortex_digest,vortex_reopen_verify,"
                "prepared_query,sink_output,evidence_render"
            ),
            "cold_bottleneck_primary_stage": "vortex_write" if cold_route else "not_applicable",
            "cold_bottleneck_primary_stage_ms": 1.0 if cold_route else None,
            "cold_bottleneck_primary_stage_share": 1.0 if cold_route else None,
            "cold_bottleneck_secondary_stage": (
                "vortex_array_build" if cold_route else "not_applicable"
            ),
            "cold_bottleneck_secondary_stage_ms": 0.5 if cold_route else None,
            "cold_bottleneck_secondary_stage_share": 0.5 if cold_route else None,
            "cold_bottleneck_stage_value_fields": (
                "vortex_write=1.0000;vortex_array_build=0.5000"
                if cold_route
                else "not_applicable_non_cold_route"
            ),
            "cold_route_optimization_hint": (
                "optimize_vortex_writer_batching_layout_and_sink_buffering"
                if cold_route
                else "not_applicable_non_cold_route"
            ),
            "cold_route_optimization_hint_scope": "diagnostic_only_no_runtime_policy_change",
            "cold_route_bottleneck_claim_boundary": "diagnostic_only_no_claim",
            "source_read_scout_schema_version": (
                "shardloom.traditional_analytics.source_read_scout.v1"
            ),
            "source_read_scout_status": (
                "source_read_scout_split_recorded"
                if cold_route
                else "not_applicable_no_source_read_stage"
            ),
            "source_read_scout_timing_split_status": (
                "complete" if cold_route else "not_applicable"
            ),
            "source_read_header_scout_ms": 0.0 if cold_route else None,
            "source_read_byte_acquisition_ms": 0.0 if cold_route else None,
            "source_read_full_body_ms": 0.0 if cold_route else None,
            "source_read_typed_decode_ms": 0.0 if cold_route else None,
            "source_read_row_assembly_ms": 0.0 if cold_route else None,
            "source_read_anomaly_quarantine_ms": 0.0 if cold_route else None,
            "source_read_columnar_handoff_ms": 0.0 if cold_route else None,
            "source_read_scout_residual_ms": 0.0 if cold_route else None,
            "source_read_scout_reuse_status": (
                "not_reused_fresh_source_read" if cold_route else "not_applicable"
            ),
            "source_read_decode_status": (
                "projection_aware_text_column_decode"
                if cold_route
                else "not_applicable"
            ),
            "source_read_projected_field_mask": (
                "0x0000e07f" if cold_route else "0x00000000"
            ),
            "source_read_filter_field_mask": (
                "0x00000028" if cold_route else "0x00000000"
            ),
            "source_read_decoded_columns": (
                "fact.id|fact.group_key|fact.dim_key|fact.value|fact.metric|fact.flag|"
                "fact.category|dim.dim_key|dim.dim_label|dim.weight"
                if cold_route
                else "none"
            ),
            "source_read_skipped_columns": (
                "fact.event_date|fact.nullable_metric_00|fact.nested_payload|"
                "fact.raw_event_time|fact.dirty_numeric|fact.dirty_flag"
                if cold_route
                else "none"
            ),
            "source_read_decoded_column_count": 10 if cold_route else 0,
            "source_read_skipped_column_count": 6 if cold_route else 0,
            "source_read_row_materialization_status": (
                "typed_text_column_builders_without_row_structs"
                if cold_route
                else "not_applicable"
            ),
            "source_read_unsupported_shape_diagnostic": (
                "none_admitted_text_shape" if cold_route else "not_applicable"
            ),
            "source_state_read_plan": (
                "projection_aware_source_scout"
                if cold_route
                else "not_applicable_no_source_read_stage"
            ),
            "source_state_projection_pushdown_status": (
                "reader_projection_applied"
                if cold_route
                else "not_applicable_no_source_read_stage"
            ),
            "source_state_reader_projection_columns": (
                "fact.id|fact.group_key|fact.dim_key|fact.value|fact.metric|fact.flag|"
                "fact.category|dim.dim_key|dim.dim_label|dim.weight"
                if cold_route
                else "none"
            ),
            "source_state_reader_projection_column_count": 10 if cold_route else 0,
            "source_state_projected_field_mask": (
                "0x0000e07f" if cold_route else "0x00000000"
            ),
            "source_state_filter_field_mask": (
                "0x00000028" if cold_route else "0x00000000"
            ),
            "source_state_decoded_columns": (
                "fact.id|fact.group_key|fact.dim_key|fact.value|fact.metric|fact.flag|"
                "fact.category|dim.dim_key|dim.dim_label|dim.weight"
                if cold_route
                else "none"
            ),
            "source_state_skipped_columns": (
                "fact.event_date|fact.nullable_metric_00|fact.nested_payload|"
                "fact.raw_event_time|fact.dirty_numeric|fact.dirty_flag"
                if cold_route
                else "none"
            ),
            "source_state_decoded_column_count": 10 if cold_route else 0,
            "source_state_skipped_column_count": 6 if cold_route else 0,
            "source_read_scout_claim_boundary": "fixture_no_claim",
            "vortex_writer_context_schema_version": (
                "shardloom.traditional_analytics.vortex_writer_context.v1"
            ),
            "vortex_writer_context_status": "reported" if cold_route else "not_applicable",
            "vortex_writer_context_open_ms": 0.0 if cold_route else None,
            "vortex_writer_context_write_count": 2 if cold_route else 0,
            "vortex_writer_context_reuse_hit_count": 1 if cold_route else 0,
            "vortex_writer_context_reuse_status": (
                "single_vortex_runtime_session_reused_across_artifacts"
                if cold_route
                else "not_applicable"
            ),
            "vortex_segment_write_ms": 0.0 if cold_route else None,
            "vortex_workspace_stage_ms": 0.0 if cold_route else None,
            "vortex_write_coalescing_status": (
                "scheduled_multi_artifact_writes_on_shared_context"
                if cold_route
                else "not_applicable"
            ),
            "vortex_write_coalescing_reason": (
                "distinct_fact_dim_cdc_artifact_contract_preserved_while_reusing_vortex_runtime_session"
                if cold_route
                else "not_applicable"
            ),
            "vortex_write_plan_schema_version": (
                "shardloom.traditional_analytics.vortex_write_plan.v1"
            ),
            "vortex_write_plan_status": (
                "bounded_capillary_write_plan_derived_from_writer_context"
                if cold_route
                else "not_applicable_non_cold_route"
            ),
            "vortex_write_plan_artifact_count": 2 if cold_route else 0,
            "vortex_write_plan_artifact_roles": (
                "fact,dim" if cold_route else "not_applicable_non_cold_route"
            ),
            "vortex_write_plan_total_artifact_bytes": 1024 if cold_route else 0,
            "vortex_write_plan_total_artifact_rows": 100 if cold_route else 0,
            "vortex_write_plan_writer_context_count": 1 if cold_route else 0,
            "vortex_write_plan_shared_writer_context": bool(cold_route),
            "vortex_write_plan_writer_context_write_count": 2 if cold_route else 0,
            "vortex_write_plan_writer_context_reuse_hit_count": 1 if cold_route else 0,
            "vortex_write_plan_context_open_ms": 0.0 if cold_route else None,
            "vortex_write_plan_segment_write_ms": 0.0 if cold_route else None,
            "vortex_write_plan_workspace_stage_ms": 0.0 if cold_route else None,
            "vortex_write_plan_digest_ms": 0.0 if cold_route else None,
            "vortex_write_plan_verification_ms": 0.0 if cold_route else None,
            "vortex_write_plan_coalescing_status": (
                "scheduled_multi_artifact_writes_on_shared_context"
                if cold_route
                else "not_applicable_non_cold_route"
            ),
            "vortex_write_plan_coalescing_reason": (
                "distinct_fact_dim_cdc_artifact_contract_preserved_while_reusing_vortex_runtime_session"
                if cold_route
                else "not_applicable_non_cold_route"
            ),
            "vortex_write_plan_digest_status": (
                "streaming_workspace_writer_digest_no_post_write_digest_pass"
                if cold_route
                else "not_applicable_non_cold_route"
            ),
            "vortex_write_plan_verification_status": (
                "local_reopen_verification_completed"
                if cold_route
                else "not_applicable_non_cold_route"
            ),
            "source_split_count": 1,
            "source_open_count": 1,
            "source_bytes_read": 1024,
            "source_columns_requested": 2,
            "source_projection_applied": False,
            "source_pressure_profile": "single_local_source",
            "vortex_prepared_state_reusable": cold_route,
            "vortex_prepared_state_fingerprint": "fnv64:prepared",
            "vortex_prepared_state_fingerprint_status": "fingerprint_recorded",
            "source_state_fingerprint": "fnv64:source",
            "source_schema_fingerprint": "fnv64:schema",
            "source_parse_plan_id": "parse-plan://fixture",
            "source_split_manifest_id": "split-manifest://fixture",
            "source_anomaly_count": 0,
            "source_quarantine_required": False,
            "prepared_state_fingerprint": "fnv64:prepared",
            **reuse_fields,
            "nearest_runnable_route": lane_id,
            "required_feature_gate": "none_runtime_supported",
            "runtime_blocker_code": "none",
            "performance_claim_allowed": False,
            "production_claim_allowed": False,
            "spark_replacement_claim_allowed": False,
        }

    def _external_benchmark_route_fields(self, engine: str) -> dict[str, object]:
        canonical_stage_ids = ",".join(self._canonical_route_timing_stage_ids())
        return {
            "route_lane_id": "external_baseline_end_to_end",
            "route_display_name": f"{engine} End-to-End",
            "route_runtime_status": "external_baseline_only",
            "start_state": "raw_compat_source",
            "end_state": "result_sink",
            "includes_preparation": False,
            "includes_query": True,
            "includes_output": True,
            "includes_evidence": True,
            "route_comparable_to_external_end_to_end": True,
            "preparation_included": False,
            "query_timing_starts_after_preparation": False,
            "prepared_state_reused": False,
            "route_timing_ledger_schema_version": "shardloom.route_timing_ledger.v1",
            "route_timing_ledger_status": "valid",
            "route_timing_surface_schema_version": "shardloom.route_timing_surface.v1",
            "timing_surface": "external_baseline",
            "timing_surface_label": "External baseline",
            "timing_surface_evidence_tier": "external_baseline",
            "timing_surface_default_for_route": True,
            "timing_surface_claim_boundary": "external_baseline_fixture_no_claim",
            "route_total_formula": "total_route_ms = external engine reported total_runtime_millis",
            "route_timing_scope": "external_baseline_end_to_end",
            "stage_parent_id": "external_baseline_end_to_end",
            "route_timing_included_stage_ids": "external_engine_reported_total_runtime_millis",
            "route_timing_excluded_stage_ids": "none",
            "route_timing_included_stage_total_ms": 1.0,
            "route_timing_total_delta_ms": 0.0,
            "timing_normalization_schema_version": (
                "shardloom.traditional_analytics.timing_normalization.v1"
            ),
            "timing_normalization_status": "external_baseline_only",
            "source_admission_policy_micros": None,
            "source_admission_digest_policy_schema_version": (
                "shardloom.traditional_analytics.source_admission_digest_policy.v1"
            ),
            "source_admission_digest_policy_status": "external_baseline_only",
            "source_admission_full_content_digest_requested": False,
            "source_admission_full_content_digest_micros": None,
            "source_stat_micros": None,
            "source_state_open_micros": None,
            "source_state_metadata_snapshot_micros": None,
            "source_state_manifest_validation_micros": None,
            "source_state_row_count_metadata_micros": None,
            "source_state_family_build_micros": None,
            "source_state_lazy_family_construction": None,
            "source_state_family_build_timing_scope": "not_applicable_external_baseline",
            "source_state_family_build_count": None,
            "source_state_family_prewarm_status": "not_applicable_external_baseline",
            "source_state_family_prewarm_eligible_count": None,
            "source_state_family_prewarm_count": None,
            "source_state_family_prewarm_already_prepared_count": None,
            "source_state_family_prewarm_prepared_before_child_route_count": None,
            "source_state_family_prewarm_micros": None,
            "source_state_family_prewarm_scope": "not_applicable_external_baseline",
            "source_state_family_reuse_hit_count": None,
            "source_state_family_reuse_hit": None,
            "source_state_family_recompute_avoided": None,
            "source_state_digest_micros": None,
            "prepared_manifest_read_micros": None,
            "prepared_manifest_match_micros": None,
            "vortex_open_footer_micros": None,
            "scan_open_micros": None,
            "scan_chunk_iter_micros": None,
            "operator_kernel_micros": None,
            "operator_finalize_micros": None,
            "result_sink_plan_micros": None,
            "result_sink_write_micros": None,
            "result_sink_replay_micros": None,
            "human_evidence_render_micros": None,
            "json_envelope_emit_micros": None,
            "report_fields_build_micros": None,
            "cli_process_wall_micros": None,
            "route_timing_stage_inclusion_schema_version": (
                "shardloom.route_timing_stage_inclusion.v1"
            ),
            "route_timing_stage_inclusion_status": "external_baseline_only",
            "route_timing_stage_inclusion_stage_ids": canonical_stage_ids,
            "route_timing_stage_inclusion_classes": "external_baseline_only",
            "route_timing_stage_inclusion_stage_owners": "external_baseline_only",
            "route_timing_stage_inclusion_timing_scopes": "external_baseline_only",
            "route_timing_stage_inclusion_skip_reasons": "external_baseline_only",
            "route_timing_stage_inclusion_claim_boundary": "external_baseline_only",
            "route_timing_instrument_schema_version": "shardloom.route_timing_instrument.v1",
            "route_timing_instrument_status": "external_baseline_only",
            "route_timing_instrument_stage_ids": canonical_stage_ids,
            "route_timing_instrument_stage_parent_stages": "external_baseline_only",
            "route_timing_instrument_stage_groups": "external_baseline_only",
            "route_timing_instrument_stage_owners": "external_baseline_only",
            "route_timing_instrument_inclusion_classes": "external_baseline_only",
            "route_timing_instrument_timing_scopes": "external_baseline_only",
            "route_timing_instrument_evidence_levels": "external_baseline_only",
            "route_timing_instrument_residual_treatments": "external_baseline_only",
            "route_timing_instrument_substage_fields": "external_baseline_only",
            "route_timing_instrument_missing_substage_attribution": "none",
            "route_timing_instrument_expensive_stage_threshold_ms": 10.0,
            "route_timing_instrument_expensive_stage_ids": "none",
            "route_timing_instrument_not_ready_stage_ids": "none",
            "route_timing_instrument_claim_boundary": "external_baseline_only",
            "exclusive_stage_timing_schema_version": (
                "shardloom.traditional_analytics.exclusive_stage_timing.v1"
            ),
            "exclusive_stage_timing_status": "external_baseline_only",
            "exclusive_stage_timing_scope": "external_baseline_only",
            "exclusive_stage_included_stage_ids": "none",
            "route_timing_exclusive_stage_ids": "none",
            "route_timing_exclusive_stage_sum_ms": None,
            "route_timing_exclusive_residual_ms": None,
            "route_timing_exclusive_total_delta_ms": None,
            "route_timing_exclusive_residual_status": "not_numeric",
            "inclusive_compatibility_to_vortex_import_ms": None,
            "inclusive_compatibility_to_vortex_import_timing_scope": "external_baseline_only",
            "exclusive_stage_timing_claim_boundary": "external_baseline_only",
            "preparation_timing_included_in_total": False,
            "query_timing_included_in_total": True,
            "output_timing_included_in_total": True,
            "evidence_timing_included_in_total": False,
            "fast_path_attribution_schema_version": "shardloom.route_fast_path_attribution.v1",
            "runtime_execution_ms": 1.0,
            "output_delivery_ms": 0.0,
            "evidence_capture_ms": 0.0,
            "evidence_render_ms": 0.0,
            "certificate_link_ms": 0.0,
            "runtime_execution_timing_scope": "external_baseline_end_to_end",
            "output_delivery_timing_scope": "included_in_route_total",
            "evidence_capture_timing_status": "certificate_metadata_linked_not_separately_timed",
            "certificate_link_timing_status": "metadata_linked_not_separately_timed",
            "runtime_execution_certificate_id": "external_baseline_only",
            "runtime_execution_certificate_status": "external_baseline_only",
            "runtime_execution_certificate_plan_ref": "external_baseline_only",
            "certificate_link_status": "external_baseline_only",
            "evidence_required_for_claim": False,
            "evidence_render_included_in_route_total": False,
            "fast_path_claim_boundary": "external baseline fixture",
            "operator_mode_inventory_schema_version": "shardloom.operator_mode_inventory.v1",
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
            "total_route_ms": 1.0,
            "source_read_scout_schema_version": (
                "shardloom.traditional_analytics.source_read_scout.v1"
            ),
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
            "source_state_read_plan": "external_baseline_only",
            "source_state_projection_pushdown_status": "external_baseline_only",
            "source_state_reader_projection_columns": "none",
            "source_state_reader_projection_column_count": 0,
            "source_state_projected_field_mask": "0x00000000",
            "source_state_filter_field_mask": "0x00000000",
            "source_state_decoded_columns": "none",
            "source_state_skipped_columns": "none",
            "source_state_decoded_column_count": 0,
            "source_state_skipped_column_count": 0,
            "source_read_scout_claim_boundary": "external_baseline_only",
            "vortex_writer_context_schema_version": (
                "shardloom.traditional_analytics.vortex_writer_context.v1"
            ),
            "vortex_writer_context_status": "external_baseline_only",
            "vortex_writer_context_open_ms": None,
            "vortex_writer_context_write_count": 0,
            "vortex_writer_context_reuse_hit_count": 0,
            "vortex_writer_context_reuse_status": "external_baseline_only",
            "vortex_segment_write_ms": None,
            "vortex_workspace_stage_ms": None,
            "vortex_write_coalescing_status": "external_baseline_only",
            "vortex_write_coalescing_reason": "external_baseline_only",
            "vortex_write_plan_schema_version": "external_baseline_only",
            "vortex_write_plan_status": "external_baseline_only",
            "vortex_write_plan_artifact_count": 0,
            "vortex_write_plan_artifact_roles": "external_baseline_only",
            "vortex_write_plan_total_artifact_bytes": 0,
            "vortex_write_plan_total_artifact_rows": 0,
            "vortex_write_plan_writer_context_count": 0,
            "vortex_write_plan_shared_writer_context": False,
            "vortex_write_plan_writer_context_write_count": 0,
            "vortex_write_plan_writer_context_reuse_hit_count": 0,
            "vortex_write_plan_context_open_ms": None,
            "vortex_write_plan_segment_write_ms": None,
            "vortex_write_plan_workspace_stage_ms": None,
            "vortex_write_plan_digest_ms": None,
            "vortex_write_plan_verification_ms": None,
            "vortex_write_plan_coalescing_status": "external_baseline_only",
            "vortex_write_plan_coalescing_reason": "external_baseline_only",
            "vortex_write_plan_digest_status": "external_baseline_only",
            "vortex_write_plan_verification_status": "external_baseline_only",
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
            "performance_claim_allowed": False,
            "production_claim_allowed": False,
            "spark_replacement_claim_allowed": False,
        }

    def _public_front_door_benchmark_rows(self, module: object) -> list[dict[str, object]]:
        schema = getattr(
            module,
            "PUBLIC_FRONT_DOOR_BENCHMARK_SCHEMA_VERSION",
            "shardloom.public_front_door_benchmark_rows.v1",
        )
        row_kind = getattr(
            module,
            "PUBLIC_FRONT_DOOR_BENCHMARK_ROW_KIND",
            "public_front_door_route_evidence",
        )
        timing_status = getattr(
            module,
            "PUBLIC_FRONT_DOOR_BENCHMARK_TIMING_STATUS",
            "not_timing_row_route_identity_only",
        )
        claim_boundary = (
            "public front-door rows explain route identity, timing boundary, "
            "prepared-state reuse scope, and no-fallback evidence; they are not "
            "measured benchmark timing rows and do not authorize performance, "
            "production, or Spark-replacement claims"
        )
        shared = {
            "public_front_door_benchmark_schema_version": schema,
            "benchmark_row_kind": row_kind,
            "benchmark_timing_status": timing_status,
            "benchmark_timing_row": False,
            "benchmark_route_publication_status": "published_static_route_identity",
            "benchmark_route_publication_source": "user_route_capability_report",
            "benchmark_route_publication_claim_boundary": claim_boundary,
            "route_runtime_status": "global_runtime_supported",
            "includes_preparation": True,
            "includes_output": True,
            "includes_evidence": True,
            "preparation_included": True,
            "owning_route_comparable_to_external_end_to_end": True,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "performance_claim_allowed": False,
            "production_claim_allowed": False,
            "spark_replacement_claim_allowed": False,
            "claim_gate_status": "not_claim_grade",
            "required_evidence": [
                "prepared_state_reuse_manifest",
                "route_runtime_status",
                "no_fallback_evidence",
            ],
            "claim_boundary": "route identity and prepared-state reuse evidence only",
            "prepared_state_reuse_scope": "workspace_prepared_state_artifact",
            "prepared_state_reuse_manifest_path": (
                "target/shardloom-prepared/prepared-state-manifest.json"
            ),
            "prepared_state_reuse_policy": "workspace_prepared_state_reuse.v1",
            "prepared_state_reuse_reason": (
                "public_front_door_prepares_reusable_vortex_state"
            ),
            "prepared_state_reuse_manifest_digest": "fnv64:prepared-front-door",
            "prepared_state_invalidation_reason": (
                "workspace_manifest_or_input_fingerprint_mismatch"
            ),
        }
        return [
            {
                **shared,
                "front_door_id": "local_source_vortex_middle_front_door",
                "owning_route_id": "local_file_prepare_once_first_query",
                "route_lane_id": "prepare_once_first_query",
                "route_display_name": "ShardLoom Prepare-Once First Query",
                "front_door_start_state": "SourceState",
                "front_door_end_state": "result_sink",
                "includes_query": True,
                "public_user_surface": (
                    "ctx.prepare_vortex('fact.csv', "
                    "workspace='target/shardloom-prepared').query('selective filter').collect()"
                ),
                "benchmark_public_surface": (
                    "ctx.prepare_vortex('fact.csv', "
                    "workspace='target/shardloom-prepared').query('selective filter').collect()"
                ),
                "benchmark_timing_boundary": (
                    "ctx.prepare_vortex(..., workspace=...).query(...).collect() "
                    "is the ShardLoom Prepare-Once First Query route identity: "
                    "preparation plus first prepared query/output are the comparable route; "
                    "this static row is not a measured timing row"
                ),
                "vortex_normalization_point": "SourceState -> VortexPreparedState",
            },
            {
                **shared,
                "front_door_id": "generated_source_prepare_vortex_front_door",
                "owning_route_id": "generated_rows_local_output",
                "route_lane_id": "generated_rows_local_output",
                "route_display_name": "Generated Rows Local Output",
                "front_door_start_state": "GeneratedSourceState",
                "front_door_end_state": "VortexPreparedState",
                "includes_query": False,
                "public_user_surface": (
                    "ctx.from_rows([{'id': 1, 'label': 'alpha'}]).prepare_vortex("
                    "workspace='target/shardloom-prepared')"
                ),
                "benchmark_public_surface": (
                    "ctx.from_rows([{'id': 1, 'label': 'alpha'}]).prepare_vortex("
                    "workspace='target/shardloom-prepared')"
                ),
                "benchmark_timing_boundary": (
                    "ctx.from_rows(...).prepare_vortex(workspace=...) writes a "
                    "local VortexPreparedState artifact; generated-source "
                    "local-output timing is route evidence, not comparative "
                    "query timing"
                ),
                "required_evidence": [
                    "single_vortex_artifact_output_for_feature_gated_local_vortex_output",
                    "route_runtime_status",
                    "no_fallback_evidence",
                ],
                "vortex_normalization_point": (
                    "GeneratedSourceState -> VortexPreparedState"
                ),
            },
        ]

    def test_foundry_style_dataset_rewrite_removes_stale_parts(self) -> None:
        module = self._load_module_from_path(
            REPO_ROOT / "examples" / "foundry-lightweight-transform" / "run.py",
            "foundry_lightweight_transform_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            dataset_path = Path(tempdir) / "dataset"
            dataset_path.mkdir()
            (dataset_path / "part-00000.jsonl").write_text('{"old": 1}\n', encoding="utf-8")
            (dataset_path / "part-00001.jsonl").write_text('{"stale": 1}\n', encoding="utf-8")

            report = module.write_foundry_style_dataset(
                dataset_path,
                [{"id": 1}],
                dataset_role="result_dataset",
                metadata={"source": "test"},
            )

            self.assertEqual(report["row_count"], 1)
            self.assertEqual(report["stale_part_files_removed"], 2)
            self.assertEqual(
                sorted(path.name for path in dataset_path.glob("part-*.jsonl")),
                ["part-00000.jsonl"],
            )
            metadata = json.loads(
                (dataset_path / "_dataset_metadata.json").read_text(encoding="utf-8")
            )
            self.assertEqual(metadata["row_count"], 1)
            self.assertEqual(metadata["stale_part_files_removed"], 2)

    def test_architecture_tracker_missing_inputs_fail_even_when_blocked_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            output = repo_root / "target" / "tracker.json"

            completed = subprocess.run(
                [
                    sys.executable,
                    str(REPO_ROOT / "scripts" / "check_release_architecture_tracker.py"),
                    "--repo-root",
                    str(repo_root),
                    "--output",
                    "target/tracker.json",
                    "--allow-blocked",
                ],
                text=True,
                capture_output=True,
                check=False,
            )

            self.assertNotEqual(completed.returncode, 0, completed.stdout + completed.stderr)
            report = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "blocked")
            self.assertGreater(report["missing_required_input_count"], 0)
            self.assertTrue(report["missing_required_inputs"])
            self.assertTrue(
                any(
                    "missing required architecture tracker input" in blocker
                    for blocker in report["blockers"]
                )
            )
            self.assertFalse(report["fallback_attempted"])
            self.assertFalse(report["external_engine_invoked"])

    def test_architecture_tracker_accepts_mapped_global_review_burn_down(self) -> None:
        module = self._load_script_module(
            "check_release_architecture_tracker.py",
            "check_release_architecture_tracker_burn_down_for_test",
        )

        report = {
            "schema_version": "shardloom.runtime_gap_family_burn_down.v1",
            "status": "passed",
            "blockers": [],
            "global_review_unchecked_count": 36,
            "mapped_gap_count": 36,
            "acceptance_summary": {
                "all_unchecked_global_review_rows_mapped": True,
                "all_families_have_phase_items": True,
                "all_families_have_active_phase_owner": True,
                "all_families_have_evidence_and_validators": True,
                "all_no_fallback_invariants_named": True,
                "all_claim_boundaries_named": True,
            },
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "runtime_support_claim_allowed": False,
            "performance_claim_allowed": False,
            "production_claim_allowed": False,
            "claim_gate_status": "not_claim_grade",
        }

        blockers = module.runtime_gap_family_burn_down_blockers(
            report,
            expected_global_unchecked_count=36,
        )

        self.assertEqual(blockers, [])

    def test_architecture_tracker_rejects_stale_global_review_burn_down(self) -> None:
        module = self._load_script_module(
            "check_release_architecture_tracker.py",
            "check_release_architecture_tracker_stale_burn_down_for_test",
        )

        report = {
            "schema_version": "shardloom.runtime_gap_family_burn_down.v1",
            "status": "passed",
            "blockers": [],
            "global_review_unchecked_count": 35,
            "mapped_gap_count": 35,
            "acceptance_summary": {
                "all_unchecked_global_review_rows_mapped": True,
                "all_families_have_phase_items": True,
                "all_families_have_active_phase_owner": True,
                "all_families_have_evidence_and_validators": True,
                "all_no_fallback_invariants_named": True,
                "all_claim_boundaries_named": True,
            },
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "runtime_support_claim_allowed": False,
            "performance_claim_allowed": False,
            "production_claim_allowed": False,
            "claim_gate_status": "not_claim_grade",
        }

        blockers = module.runtime_gap_family_burn_down_blockers(
            report,
            expected_global_unchecked_count=36,
        )

        self.assertIn(
            "runtime gap family burn-down global_review_unchecked_count mismatch: 35 != 36",
            blockers,
        )
        self.assertIn(
            "runtime gap family burn-down mapped_gap_count mismatch: 35 != 36",
            blockers,
        )




































    def test_website_readiness_flags_duplicate_suffixed_artifacts(self) -> None:
        module = self._load_script_module(
            "check_website_readiness.py",
            "website_readiness_duplicate_suffix_for_test",
        )

        target = REPO_ROOT / "target"
        target.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=target) as tempdir:
            duplicate = Path(tempdir) / "benchmarks 2.json"
            duplicate.write_text("{}", encoding="utf-8")
            blockers: list[str] = []

            module.check_duplicate_suffixed_artifacts(
                [Path(tempdir)],
                REPO_ROOT,
                blockers,
            )

        self.assertEqual(len(blockers), 1)
        self.assertIn("duplicate suffixed generated artifact remains", blockers[0])
































    def test_release_readiness_accepts_burned_down_runtime_gap_count(self) -> None:
        module = self._load_script_module(
            "check_release_readiness.py",
            "check_release_readiness_runtime_gap_for_test",
        )
        report = {
            "schema_version": "shardloom.runtime_gap_family_burn_down.v1",
            "status": "passed",
            "global_review_unchecked_count": 36,
            "mapped_gap_count": 36,
            "acceptance_summary": {
                "all_unchecked_global_review_rows_mapped": True,
                "all_families_have_phase_items": True,
                "all_families_have_active_phase_owner": True,
                "all_families_have_evidence_and_validators": True,
                "all_no_fallback_invariants_named": True,
                "all_claim_boundaries_named": True,
            },
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "runtime_support_claim_allowed": False,
            "performance_claim_allowed": False,
            "production_claim_allowed": False,
            "claim_gate_status": "not_claim_grade",
        }

        self.assertEqual(module.runtime_gap_family_burn_down_blockers(report), [])
        mismatched = dict(report, mapped_gap_count=37)
        self.assertIn(
            "runtime gap family burn-down mapped_gap_count does not match global_review_unchecked_count: 37 != 36",
            module.runtime_gap_family_burn_down_blockers(mismatched),
        )


















    def test_clickbench_olap_coverage_accepts_relative_output_path(self) -> None:
        output = Path("target/clickbench-olap-runtime-coverage-test-relative.json")
        result = subprocess.run(
            [
                sys.executable,
                "scripts/check_clickbench_olap_runtime_coverage.py",
                "--output",
                output.as_posix(),
            ],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn(f"wrote {output.as_posix()}", result.stdout)
        report_path = REPO_ROOT / output
        self.assertTrue(report_path.exists())
        report = json.loads(report_path.read_text(encoding="utf-8"))
        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(report["admitted_query_count"], 43)
        self.assertEqual(report["implementation_required_count"], 0)
        self.assertEqual(report["feature_gated_query_count"], 0)
        self.assertTrue(report["route_family_counts"])
        self.assertEqual(
            report["clickbench_olap_readiness_status"],
            "all_queries_admitted_route_readiness",
        )
        self.assertEqual(
            report["memory_spill_diagnostic_status"],
            "state_budget_declared_spill_fail_closed_no_spill_io",
        )
        self.assertFalse(report["performance_claim_allowed"])
        self.assertIn("route readiness only", report["site_readiness_claim_boundary"])

    def test_clickbench_olap_statement_splitter_strips_sql_comments(self) -> None:
        module = self._load_script_module(
            "check_clickbench_olap_runtime_coverage.py",
            "clickbench_olap_splitter_comments_for_test",
        )

        queries = module.split_sql_statements(
            """
            -- leading source comment
            SELECT COUNT(*) FROM hits;
            /* block comment with ; semicolon */
            SELECT '-- not a comment' AS literal FROM hits;
            """
        )

        self.assertEqual(
            queries,
            [
                "SELECT COUNT(*) FROM hits",
                "SELECT '-- not a comment' AS literal FROM hits",
            ],
        )

    def _optimization_target_rows(self) -> list[dict[str, object]]:
        base = {
            "status": "success",
            "claim_gate_status": "not_claim_grade",
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "actual_evidence_tier": "metadata_sink",
            "timing_surface": "hot_runtime",
            "route_lane_id": "cold_certified_route",
            "hot_route_total_ms": 10.0,
            "query_runtime_millis": 8.0,
            "source_read_ms": 1.0,
            "source_parse_or_columnar_decode_ms": 2.0,
            "vortex_write_ms": 3.0,
            "prepared_state_lookup_or_create_ms": None,
            "operator_compute_ms": 0.5,
            "materialized_temporary_operators": "none",
            "operator_temporary_materialization_used": False,
            "route_timing_stage_inclusion_classes": self._packed_route_stage_map(
                "included_hot_runtime"
            ),
        }
        rows = [
            {
                **base,
                "engine": "shardloom",
                "storage_format": "jsonl",
                "scenario_name": "nested JSON field scan",
                "hot_route_total_ms": 110.0,
                "source_parse_or_columnar_decode_ms": 80.0,
            },
            {
                **base,
                "engine": "shardloom",
                "storage_format": "avro",
                "scenario_name": "high-cardinality string group/distinct",
                "hot_route_total_ms": 130.0,
                "source_parse_or_columnar_decode_ms": 70.0,
            },
            {
                **base,
                "engine": "shardloom-prepare-batch",
                "storage_format": "jsonl",
                "scenario_name": "prepare once",
                "route_lane_id": "prepare_once_first_query",
                "hot_route_total_ms": 90.0,
                "prepared_state_lookup_or_create_ms": 50.0,
            },
            {
                **base,
                "engine": "shardloom",
                "storage_format": "csv",
                "scenario_name": "group by aggregation",
                "hot_route_total_ms": 60.0,
                "operator_compute_ms": 12.0,
                "materialized_temporary_operators": "operator-blocker://csv/group_by",
                "operator_temporary_materialization_used": True,
            },
            {
                **base,
                "engine": "shardloom",
                "storage_format": "csv",
                "scenario_name": "publication proof row",
                "timing_surface": "publication_proof",
                "actual_evidence_tier": "publication_full",
            },
        ]
        return rows

    def test_benchmark_optimization_targets_extracts_current_hot_targets(self) -> None:
        module = self._load_script_module(
            "check_benchmark_optimization_targets.py",
            "check_benchmark_optimization_targets_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            artifact = Path(tempdir) / "benchmark-results.json"
            artifact.write_text(
                json.dumps(
                    {
                        "schema_version": "shardloom.website.benchmark_evidence.v1",
                        "benchmark_profile": "fixture",
                        "published_benchmark_rows": self._optimization_target_rows(),
                    }
                ),
                encoding="utf-8",
            )
            report = module.build_report(artifact, top_n=2)

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertFalse(report["performance_claim_allowed"])
        self.assertFalse(report["fallback_attempted"])
        self.assertFalse(report["external_engine_invoked"])
        self.assertEqual(report["next_implementation_slice"], "none")
        self.assertEqual(report["evidence_present_target_count"], 6)
        self.assertEqual(report["diagnostic_absent_or_retired_target_count"], 0)
        self.assertEqual(report["release_blocking_target_count"], 0)
        self.assertEqual(report["release_blocking_targets"], [])
        self.assertEqual(
            report["target_disappearance_policy"],
            "diagnostic_absent_or_retired_not_release_blocker",
        )
        target_ids = {target["target_id"] for target in report["targets"]}
        self.assertEqual(
            target_ids,
            {
                "jsonl_parse_decode_hot_runtime",
                "avro_hot_runtime_outliers",
                "prepared_state_lookup_or_create",
                "vortex_write_and_reopen_verify",
                "source_read_scout_timing",
                "operator_materialization",
            },
        )
        by_target = {target["target_id"]: target for target in report["targets"]}
        self.assertEqual(
            by_target["operator_materialization"]["top_rows"][0]["scenario_name"],
            "group by aggregation",
        )
        self.assertEqual(
            by_target["operator_materialization"][
                "included_additive_stage_row_count"
            ],
            1,
        )
        self.assertEqual(
            by_target["operator_materialization"]["top_rows"][0][
                "stage_contract_status"
            ],
            "included_additive",
        )

    def test_benchmark_optimization_targets_load_summary_only_row_chunks(self) -> None:
        module = self._load_script_module(
            "check_benchmark_optimization_targets.py",
            "check_benchmark_optimization_targets_row_chunks_for_test",
        )

        (REPO_ROOT / "target").mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=REPO_ROOT / "target") as tempdir:
            temp_path = Path(tempdir)
            chunk_path = temp_path / "published-benchmark-rows-000.json"
            chunk_path.write_text(
                json.dumps(
                    {
                        "schema_version": "shardloom.website.benchmark_row_chunk.v1",
                        "rows": self._optimization_target_rows(),
                    }
                ),
                encoding="utf-8",
            )
            artifact = temp_path / "benchmark-results.json"
            artifact.write_text(
                json.dumps(
                    {
                        "schema_version": "shardloom.website.benchmark_evidence.v1",
                        "benchmark_profile": "fixture",
                        "published_benchmark_rows": [],
                        "published_benchmark_rows_inlined": "summary_only",
                        "published_benchmark_row_chunks": [
                            {"path": str(chunk_path.relative_to(REPO_ROOT))}
                        ],
                    }
                ),
                encoding="utf-8",
            )

            report = module.build_report(artifact, top_n=2)

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(report["published_benchmark_row_count"], 5)
        self.assertEqual(report["evidence_present_target_count"], 6)
        self.assertEqual(report["timing_contract_blocked_target_count"], 0)

    def test_benchmark_optimization_targets_resolve_chunks_under_repo_root(self) -> None:
        module = self._load_script_module(
            "check_benchmark_optimization_targets.py",
            "check_benchmark_optimization_targets_repo_root_chunks_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            chunk_dir = repo_root / "website" / "assets" / "benchmarks" / "latest"
            chunk_dir.mkdir(parents=True)
            chunk_path = chunk_dir / "published-benchmark-rows-000.json"
            chunk_path.write_text(
                json.dumps(
                    {
                        "schema_version": "shardloom.website.benchmark_row_chunk.v1",
                        "rows": self._optimization_target_rows(),
                    }
                ),
                encoding="utf-8",
            )
            artifact = chunk_dir / "benchmark-results.json"
            artifact.write_text(
                json.dumps(
                    {
                        "schema_version": "shardloom.website.benchmark_evidence.v1",
                        "benchmark_profile": "fixture",
                        "published_benchmark_rows": [],
                        "published_benchmark_rows_inlined": "summary_only",
                        "published_benchmark_row_count": 5,
                        "published_benchmark_row_chunks": [
                            {
                                "path": (
                                    "website/assets/benchmarks/latest/"
                                    "published-benchmark-rows-000.json"
                                ),
                                "row_count": 5,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            report = module.build_report(artifact, top_n=2, repo_root=repo_root)

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(report["published_benchmark_row_count"], 5)

    def test_benchmark_optimization_targets_fail_closed_for_missing_chunks(self) -> None:
        module = self._load_script_module(
            "check_benchmark_optimization_targets.py",
            "check_benchmark_optimization_targets_missing_chunks_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            artifact = repo_root / "benchmark-results.json"
            artifact.write_text(
                json.dumps(
                    {
                        "schema_version": "shardloom.website.benchmark_evidence.v1",
                        "benchmark_profile": "fixture",
                        "published_benchmark_rows": [],
                        "published_benchmark_rows_inlined": "summary_only",
                        "published_benchmark_row_count": 5,
                        "published_benchmark_row_chunks": [
                            {
                                "path": "website/assets/benchmarks/latest/missing.json",
                                "row_count": 5,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            report = module.build_report(artifact, top_n=2, repo_root=repo_root)

        self.assertEqual(report["status"], "failed")
        self.assertTrue(
            any(
                "declared benchmark row chunk missing" in blocker
                for blocker in report["blockers"]
            )
        )

    def test_benchmark_optimization_targets_fail_closed_on_non_additive_stage(
        self,
    ) -> None:
        module = self._load_script_module(
            "check_benchmark_optimization_targets.py",
            "check_benchmark_optimization_targets_non_additive_for_test",
        )

        rows = self._optimization_target_rows()
        for row in rows:
            if row["scenario_name"] == "group by aggregation":
                row["hot_route_total_ms"] = 0.12
                row["operator_compute_ms"] = 1.4

        with tempfile.TemporaryDirectory() as tempdir:
            artifact = Path(tempdir) / "benchmark-results.json"
            artifact.write_text(
                json.dumps(
                    {
                        "schema_version": "shardloom.website.benchmark_evidence.v1",
                        "benchmark_profile": "fixture",
                        "published_benchmark_rows": rows,
                    }
                ),
                encoding="utf-8",
            )
            report = module.build_report(artifact, top_n=2)

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(
            report["timing_contract_blocked_targets"], ["operator_materialization"]
        )
        by_target = {target["target_id"]: target for target in report["targets"]}
        operator_target = by_target["operator_materialization"]
        self.assertEqual(
            operator_target["status"], "diagnostic_stage_excluded_or_non_additive"
        )
        self.assertEqual(operator_target["target_evidence_class"], "timing_contract_blocked")
        self.assertEqual(operator_target["included_additive_stage_row_count"], 0)
        self.assertEqual(operator_target["non_additive_stage_row_count"], 1)
        self.assertEqual(
            operator_target["top_rows"][0]["stage_contract_status"],
            "non_additive_stage_exceeds_route_total",
        )
        self.assertIsNone(operator_target["top_rows"][0]["stage_route_share"])

    def test_benchmark_optimization_targets_do_not_block_retired_hotspots(self) -> None:
        module = self._load_script_module(
            "check_benchmark_optimization_targets.py",
            "check_benchmark_optimization_targets_retired_for_test",
        )

        rows = [
            row
            for row in self._optimization_target_rows()
            if row["storage_format"] != "avro"
        ]
        for row in rows:
            if row.get("vortex_write_ms") is not None:
                row["vortex_write_ms"] = 0.0
        with tempfile.TemporaryDirectory() as tempdir:
            artifact = Path(tempdir) / "benchmark-results.json"
            artifact.write_text(
                json.dumps(
                    {
                        "schema_version": "shardloom.website.benchmark_evidence.v1",
                        "benchmark_profile": "fixture",
                        "published_benchmark_rows": rows,
                    }
                ),
                encoding="utf-8",
            )
            report = module.build_report(artifact)

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(report["release_blocking_target_count"], 0)
        self.assertEqual(report["release_blocking_targets"], [])
        self.assertIn(
            "avro_hot_runtime_outliers",
            report["diagnostic_absent_or_retired_targets"],
        )
        self.assertIn(
            "vortex_write_and_reopen_verify",
            report["diagnostic_absent_or_retired_targets"],
        )
        by_target = {target["target_id"]: target for target in report["targets"]}
        self.assertEqual(
            by_target["avro_hot_runtime_outliers"]["status"],
            "diagnostic_absent_or_retired",
        )
        self.assertEqual(by_target["avro_hot_runtime_outliers"]["row_count"], 0)
        self.assertEqual(
            by_target["vortex_write_and_reopen_verify"]["status"],
            "diagnostic_stage_zero_or_retired",
        )
        self.assertGreater(by_target["vortex_write_and_reopen_verify"]["row_count"], 0)
        self.assertFalse(by_target["vortex_write_and_reopen_verify"]["release_blocker"])
        self.assertTrue(by_target["vortex_write_and_reopen_verify"]["diagnostic_only"])
        self.assertEqual(
            by_target["vortex_write_and_reopen_verify"]["target_disappearance_policy"],
            "diagnostic_absent_or_retired_not_release_blocker",
        )

    def test_benchmark_optimization_targets_do_not_block_publication_only_bundle(
        self,
    ) -> None:
        module = self._load_script_module(
            "check_benchmark_optimization_targets.py",
            "check_benchmark_optimization_targets_publication_only_for_test",
        )

        rows = [
            {
                "engine": "shardloom",
                "storage_format": "csv",
                "scenario_name": "publication proof row",
                "timing_surface": "publication_proof",
                "actual_evidence_tier": "publication_full",
                "fallback_attempted": False,
                "external_engine_invoked": False,
            }
        ]
        with tempfile.TemporaryDirectory() as tempdir:
            artifact = Path(tempdir) / "benchmark-results.json"
            artifact.write_text(
                json.dumps(
                    {
                        "schema_version": "shardloom.website.benchmark_evidence.v1",
                        "benchmark_profile": "fixture",
                        "published_benchmark_rows": rows,
                    }
                ),
                encoding="utf-8",
            )
            report = module.build_report(artifact)

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(report["shardloom_hot_runtime_row_count"], 0)
        self.assertEqual(report["shardloom_publication_proof_row_count"], 1)
        self.assertEqual(
            report["diagnostic_absent_or_retired_target_count"],
            report["target_count"],
        )
        self.assertEqual(report["release_blocking_target_count"], 0)

    def test_benchmark_optimization_targets_fail_closed_on_fallback_row(self) -> None:
        module = self._load_script_module(
            "check_benchmark_optimization_targets.py",
            "check_benchmark_optimization_targets_fallback_for_test",
        )

        rows = self._optimization_target_rows()
        rows[0]["fallback_attempted"] = True
        with tempfile.TemporaryDirectory() as tempdir:
            artifact = Path(tempdir) / "benchmark-results.json"
            artifact.write_text(
                json.dumps(
                    {
                        "schema_version": "shardloom.website.benchmark_evidence.v1",
                        "benchmark_profile": "fixture",
                        "published_benchmark_rows": rows,
                    }
                ),
                encoding="utf-8",
            )
            report = module.build_report(artifact)

        self.assertEqual(report["status"], "failed")
        self.assertTrue(
            any("fallback_attempted=false" in blocker for blocker in report["blockers"])
        )









    def _dependabot_pr(self, number: int, title: str) -> dict[str, object]:
        return {
            "number": number,
            "title": title,
            "html_url": f"https://github.com/depsilon/shardloom/pull/{number}",
            "user": {"login": "dependabot[bot]"},
        }

    def _write_passing_pre_5j_dependency_report(self, path: Path) -> None:
        payload = {
            "schema_version": "shardloom.pre_5j_dependency_freshness_gate.v1",
            "status": "passed",
            "gate_id": "gar-runtime-impl-5j.pre_5j_dependency_freshness",
            "require_live_github": True,
            "open_dependabot_check_status": "loaded_from_file",
            "open_dependabot_check_error": None,
            "open_dependabot_prs": [
                self._dependabot_pr(1149, "Bump actions/download-artifact from 7 to 8"),
                self._dependabot_pr(
                    1223,
                    "Bump vortex from 0.74.0 to 0.75.0 in the vortex-upstream group",
                ),
                self._dependabot_pr(1151, "Bump serde_json from 1.0.149 to 1.0.150"),
                self._dependabot_pr(1152, "Bump sha2 from 0.10.9 to 0.11.0"),
                self._dependabot_pr(1153, "Bump rusqlite from 0.40.0 to 0.40.1"),
                self._dependabot_pr(1392, "Bump regex from 1.12.4 to 1.13.1"),
            ],
            "open_dependabot_pr_count": 6,
            "admitted_open_dependabot_prs": [
                1149,
                1151,
                1152,
                1153,
                1223,
                1392,
            ],
            "unknown_open_dependabot_prs": [],
            "benchmark_refresh_dependency_gate_status": "passed",
            "benchmark_refresh_allowed": True,
            "benchmark_run_performed": False,
            "publication_attempted": False,
            "tag_created": False,
            "secrets_required": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "blockers": [],
        }
        path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    def _write_workspace_version_source_fixture(
        self,
        root: Path,
        *,
        stale: bool = False,
    ) -> None:
        (root / "Cargo.toml").write_text(
            textwrap.dedent(
                f"""
                [workspace]
                members = ["shardloom-core", "shardloom-vortex"]

                [workspace.package]
                version = "{CURRENT_WORKSPACE_PACKAGE_VERSION}"
                rust-version = "{CURRENT_RUST_VERSION}"

                [workspace.dependencies]
                vortex = "{CURRENT_VORTEX_MANIFEST_VERSION}"
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )
        (root / "Cargo.lock").write_text(
            "[[package]]\n"
            'name = "vortex"\n'
            f'version = "{CURRENT_VORTEX_LOCK_VERSION}"\n'
            "\n"
            "[[package]]\n"
            'name = "shardloom-core"\n'
            f'version = "{CURRENT_WORKSPACE_PACKAGE_VERSION}"\n'
            "\n"
            "[[package]]\n"
            'name = "shardloom-vortex"\n'
            f'version = "{CURRENT_WORKSPACE_PACKAGE_VERSION}"\n',
            encoding="utf-8",
        )
        core_manifest = root / "shardloom-core" / "Cargo.toml"
        core_manifest.parent.mkdir(parents=True)
        core_manifest.write_text(
            "[package]\n"
            'name = "shardloom-core"\n'
            + (
                "rust-version.workspace = true\n"
                if stale
                else "version.workspace = true\nrust-version.workspace = true\n"
            ),
            encoding="utf-8",
        )
        vortex_manifest = root / "shardloom-vortex" / "Cargo.toml"
        vortex_manifest.parent.mkdir(parents=True)
        vortex_manifest.write_text(
            "[package]\n"
            'name = "shardloom-vortex"\n'
            "version.workspace = true\n"
            "rust-version.workspace = true\n"
            "\n"
            "[dependencies]\n"
            + (
                f'shardloom-core = {{ version = "0.1.0", path = "../shardloom-core" }}\n'
                f'vortex = "{CURRENT_VORTEX_MANIFEST_VERSION}"\n'
                if stale
                else "vortex = { workspace = true, optional = true }\n"
            ),
            encoding="utf-8",
        )
        build_rs = root / "shardloom-vortex" / "build.rs"
        build_rs.write_text(
            (
                f'const VORTEX_VERSION: &str = "{CURRENT_VORTEX_MANIFEST_VERSION}";\n'
                if stale
                else 'workspace_dependency_version(&workspace_manifest_text, "vortex")\n'
                'cargo:rustc-env=SHARDLOOM_UPSTREAM_VORTEX_PROVIDER_VERSION={vortex_version}\n'
            ),
            encoding="utf-8",
        )
        release_utils = root / "scripts" / "release_report_utils.py"
        release_utils.parent.mkdir(parents=True)
        release_utils.write_text(
            "def workspace_package_version(): pass\n"
            "def workspace_rust_version(): pass\n"
            "def upstream_vortex_manifest_version(): pass\n"
            "def upstream_vortex_lock_version(): pass\n"
            "def upstream_vortex_provider_version(): pass\n"
            "def workspace_version_env(): pass\n",
            encoding="utf-8",
        )
        sync_versions = root / "scripts" / "sync_workspace_package_versions.py"
        sync_versions.write_text(
            "workspace_package_version\n"
            "DERIVED_VERSION_SOURCES\n"
            "python/src/shardloom/_version.py\n"
            "website-src/package.json\n"
            "website-src/package-lock.json\n"
            "Cargo.lock\n"
            "--check\n",
            encoding="utf-8",
        )
        release_channel_contract = root / "scripts" / "release_channel_contract.py"
        release_channel_contract.write_text(
            f"SELECTED_PACKAGE_RELEASE_VERSION = {SELECTED_PACKAGE_RELEASE_VERSION!r}\n"
            f"SELECTED_PACKAGE_RELEASE_TAG = {SELECTED_PACKAGE_RELEASE_TAG!r}\n"
            "SELECTED_PACKAGE_CHANNEL_STATUS_MARKER = "
            f"{SELECTED_PACKAGE_CHANNEL_STATUS_MARKER!r}\n",
            encoding="utf-8",
        )
        (root / "scripts" / "write_ci_version_env.py").write_text(
            (
                "from release_report_utils import rust_toolchain_version\n"
                if stale
                else "from release_report_utils import workspace_version_env\n"
            ),
            encoding="utf-8",
        )
        python_version = root / "python" / "src" / "shardloom" / "_version.py"
        python_version.parent.mkdir(parents=True)
        python_version.write_text(
            '"""Package version for the ShardLoom Python client.\n\n'
            "This file is derived from root Cargo.toml by "
            "scripts/sync_workspace_package_versions.py.\n"
            '"""\n\n'
            + (
                '__version__ = "0.0.0"\n'
                if stale
                else f'__version__ = "{CURRENT_WORKSPACE_PACKAGE_VERSION}"\n'
            ),
            encoding="utf-8",
        )
        website_package = root / "website-src" / "package.json"
        website_package.parent.mkdir(parents=True)
        website_version = "0.0.0" if stale else CURRENT_WORKSPACE_PACKAGE_VERSION
        website_package.write_text(
            json.dumps(
                {"name": "shardloom-website", "version": website_version, "private": True},
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        (root / "website-src" / "package-lock.json").write_text(
            json.dumps(
                {
                    "name": "shardloom-website",
                    "version": website_version,
                    "lockfileVersion": 3,
                    "packages": {
                        "": {
                            "name": "shardloom-website",
                            "version": website_version,
                        }
                    },
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        workflow = root / ".github" / "workflows" / "ci.yml"
        workflow.parent.mkdir(parents=True)
        workflow.write_text(
            'python scripts/write_ci_version_env.py --github-env "$GITHUB_ENV"\n'
            'rustup toolchain install "$SHARDLOOM_RUST_MSRV_TOOLCHAIN"\n'
            'python scripts/write_release_compatibility_lane_report.py --lane "$SHARDLOOM_RUST_MSRV_LANE"\n',
            encoding="utf-8",
        )
        active_doc = (
            root
            / "docs"
            / "architecture"
            / "effectful-operation-admission-matrix.md"
        )
        active_doc.parent.mkdir(parents=True)
        active_doc.write_text(
            (
                "```powershell\n"
                "$env:RUSTUP_TOOLCHAIN='1.91.1'; cargo test -p shardloom-core --lib\n"
                "```\n"
                f"Vortex API/provider surface: upstream Vortex `{CURRENT_VORTEX_MANIFEST_VERSION}`\n"
                if stale
                else "```powershell\n"
                "python scripts\\write_ci_version_env.py --format powershell | Invoke-Expression\n"
                "$env:RUSTUP_TOOLCHAIN=$env:SHARDLOOM_RUST_MSRV_TOOLCHAIN\n"
                "cargo test -p shardloom-core --lib\n"
                "```\n"
            ),
            encoding="utf-8",
        )
        package_channel_gate = root / "scripts" / "check_package_channel_readiness.py"
        package_channel_gate.write_text("PACKAGE_CHANNEL_VERSION_SOURCE = 'manifest-derived'\n")
        registry_proof = root / "scripts" / "python_registry_package_proof.py"
        registry_proof.write_text("REGISTRY_PROOF_VERSION_SOURCE = 'package-version-arg'\n")
        publish_workflow = root / ".github" / "workflows" / "pypi-publish-draft.yml"
        publish_workflow.write_text(
            "on:\n"
            "  workflow_dispatch:\n"
            "jobs:\n"
            "  publish-testpypi:\n"
            "    steps: []\n"
            "  publish:\n"
            "    steps: []\n",
            encoding="utf-8",
        )
        benchmark = root / "benchmarks" / "traditional_analytics" / "run.py"
        benchmark.parent.mkdir(parents=True)
        benchmark.write_text(
            "from release_report_utils import upstream_vortex_provider_version\n"
            "UPSTREAM_VORTEX_PROVIDER_VERSION = upstream_vortex_provider_version(REPO_ROOT)\n",
            encoding="utf-8",
        )
        (root / "scripts" / "check_pre_5j_dependency_freshness.py").write_text(
            "$CURRENT_VORTEX_MANIFEST_VERSION\n"
            "$CURRENT_VORTEX_LOCK_VERSION\n"
            "$CURRENT_VORTEX_PROVIDER_VERSION\n"
            "upstream_vortex_provider_version(repo_root)\n",
            encoding="utf-8",
        )

    def _write_finished_product_readiness_fixture(
        self,
        module: object,
        root: Path,
        *,
        local_blocked: bool = False,
        public_ready: bool = False,
    ) -> Path:
        safety_fields = getattr(module, "FALSE_SAFETY_FIELDS")
        for requirement in getattr(module, "LOCAL_PRODUCT_REPORTS"):
            path = root / requirement.path
            path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "schema_version": requirement.schema_version,
                "status": "passed",
                "blockers": [],
                "claim_gate_status": "not_claim_grade",
                **{field: False for field in safety_fields},
            }
            if requirement.status_field is None:
                payload.pop("status", None)
            if requirement.name == "package_channel_readiness":
                payload["local_gate_evidence_status"] = "passed"
                payload["package_identity_contract_status"] = "passed"
            if local_blocked and requirement.name == "v1_api_schema_stability":
                payload["status"] = "blocked"
                payload["blockers"] = ["schema fixture drift"]
            path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

        for requirement in getattr(module, "PUBLICATION_REPORTS"):
            path = root / requirement.path
            path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "schema_version": requirement.schema_version,
                "status": "passed" if public_ready else "blocked",
                "blockers": [] if public_ready else ["human approval required"],
                "claim_gate_status": "not_claim_grade",
                **{field: False for field in safety_fields},
            }
            if requirement.name == "final_release_approval_post_release_verification":
                payload["public_release_ready"] = public_ready
                payload["post_release_verification_ready"] = public_ready
                payload["public_release_blockers"] = (
                    [] if public_ready else ["post-release verification required"]
                )
            path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

        matrix_path = root / "docs" / "release" / "package-channel-readiness-matrix.json"
        matrix_path.parent.mkdir(parents=True, exist_ok=True)
        matrix = {
            "schema_version": "shardloom.package_channel_readiness_matrix.v1",
            "status": "ready" if public_ready else "blocked",
            "public_package_release_claim_allowed": public_ready,
            "publication_authorization_state": "approved"
            if public_ready
            else "human_approval_required",
            "channels": [
                {
                    "channel_id": "pypi",
                    "status": "ready" if public_ready else "blocked",
                    "ready": public_ready,
                }
            ],
            **{field: False for field in safety_fields},
        }
        matrix_path.write_text(json.dumps(matrix, indent=2) + "\n", encoding="utf-8")
        return matrix_path

    def _write_package_identity_contract_fixture(
        self,
        module: object,
        repo_root: Path,
        *,
        publish_internal_crate: bool = False,
    ) -> dict[str, object]:
        pyproject = repo_root / module.PYPROJECT
        pyproject.parent.mkdir(parents=True, exist_ok=True)
        pyproject.write_text(
            "\n".join(
                [
                    "[project]",
                    'name = "shardloom"',
                    'requires-python = ">=3.10"',
                    'license = "Apache-2.0"',
                    "dependencies = []",
                    "classifiers = [",
                    '    "Development Status :: 2 - Pre-Alpha",',
                    "]",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        readiness_doc = repo_root / module.PACKAGE_NAME_READINESS_DOC
        readiness_doc.parent.mkdir(parents=True, exist_ok=True)
        readiness_doc.write_text(
            "\n".join(
                [
                    "PyPI: `shardloom`",
                    "Internal crates remain unpublished.",
                    "`shardloom-protocol`",
                    "`shardloom-client`",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        for index, manifest in enumerate(module.INTERNAL_CRATE_MANIFESTS):
            path = repo_root / manifest
            path.parent.mkdir(parents=True, exist_ok=True)
            publish_line = (
                "publish = true"
                if index == 0 and publish_internal_crate
                else "publish = false"
            )
            path.write_text(
                "\n".join(
                    [
                        "[package]",
                        f'name = "{Path(manifest).parent.name}"',
                        'version = "0.1.0"',
                        publish_line,
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

        return {
            "publication_authorization_state": "human_approval_required",
            "channels": [
                {
                    "channel_id": "github_prerelease",
                    "internal_crates_publish_allowed": False,
                },
                {
                    "channel_id": "crates_io_future",
                    "workspace_crate_publish_status": (
                        "all_current_workspace_crates_publish_false"
                    ),
                    "internal_crates_publish_allowed": False,
                    "prepared_local_workspace_refs": module.PACKAGE_WORKSPACE_REF_MANIFESTS,
                },
            ],
        }

    def test_package_identity_contract_accepts_current_unpublished_workspace_crates(self) -> None:
        module = self._load_script_module(
            "check_package_channel_readiness.py",
            "check_package_channel_readiness_identity_for_test",
        )

        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            matrix = self._write_package_identity_contract_fixture(module, repo_root)
            report = module.validate_package_identity_contract(repo_root, matrix)

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(report["python_package_identity"], "shardloom")
        self.assertEqual(report["internal_crate_publish_status"], "all_publish_false")
        self.assertFalse(report["fallback_attempted"])
        self.assertFalse(report["external_engine_invoked"])

    def test_package_identity_contract_blocks_publishable_internal_crates(self) -> None:
        module = self._load_script_module(
            "check_package_channel_readiness.py",
            "check_package_channel_readiness_identity_blocker_for_test",
        )

        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            matrix = self._write_package_identity_contract_fixture(
                module,
                repo_root,
                publish_internal_crate=True,
            )
            report = module.validate_package_identity_contract(repo_root, matrix)

        self.assertEqual(report["status"], "blocked")
        self.assertTrue(
            any("publish = false" in blocker for blocker in report["blockers"])
        )

    def test_package_channel_matrix_records_v1_feasibility_review(self) -> None:
        module = self._load_script_module(
            "check_package_channel_readiness.py",
            "check_package_channel_readiness_v1_feasibility_for_test",
        )
        matrix = json.loads(
            (REPO_ROOT / "docs/release/package-channel-readiness-matrix.json").read_text(
                encoding="utf-8"
            )
        )

        blockers = module.validate_matrix(matrix)
        summary = module.v1_feasibility_summary(matrix)

        self.assertEqual(blockers, [])
        self.assertEqual(summary["status"], "passed")
        self.assertEqual(summary["review_status"], "reviewed")
        self.assertEqual(summary["status_counts"][SELECTED_V0_1_0_FEASIBILITY_STATUS], 4)
        self.assertEqual(
            summary["status_counts"]["feasible_pending_channel_proof"], 3
        )
        self.assertEqual(summary["status_counts"]["not_in_v1_scope_recorded"], 2)
        selected_rows = [
            row
            for row in summary["rows"]
            if row["v1_feasibility_status"] == SELECTED_V0_1_0_FEASIBILITY_STATUS
        ]
        self.assertEqual(len(selected_rows), 4)
        self.assertTrue(all(row["ready"] for row in selected_rows))

    def _package_channel_dependency_audit_fixture(self) -> dict[str, object]:
        return {
            "schema_version": "shardloom.dependency_audit_report.v1",
            "cargo_deny_status": "passed",
            "cargo_audit_status": "passed",
            "pip_audit_status": "passed",
            "license_policy_status": "passed",
            "advisory_status": "passed",
            "fallback_dependency_absent": True,
        }

    def _package_channel_release_dry_run_fixture(self) -> dict[str, object]:
        return {
            "schema_version": "shardloom.release_dry_run_proof.v1",
            "proof_status": "passed",
            "clean_venv_install_status": "passed",
            "wheel_import_and_client_smoke_performed": True,
            "cli_status_smoke_performed": True,
            "cli_capabilities_smoke_performed": True,
            "local_python_example_smoke_performed": True,
            "local_python_user_surface_quickstart_performed": True,
            "local_python_result_and_evidence_printed": True,
            "local_python_unsupported_path_evidence_printed": True,
            "generated_output_proof_distinct_from_no_dataset_smoke": True,
            "generated_source_user_rows_runtime_performed": True,
            "generated_source_range_runtime_performed": True,
            "benchmark_smoke_required_for_package_release": False,
            "benchmark_smoke_status": "skipped_not_required_for_package_release",
            "provenance_dry_run_performed": True,
            "sbom_checksum_manifest_generated": True,
            "publication_attempted": False,
            "tag_created": False,
            "secrets_required": False,
            "external_runtime_dependencies_added": False,
            "fallback_engine_dependency_added": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "public_package_release_claim_allowed": False,
        }

    def _package_channel_provenance_fixture(
        self,
        module: object,
        repo_root: Path,
        *,
        omit_bundle_kind: str | None = None,
    ) -> dict[str, object]:
        target = repo_root / "target" / "release-provenance-dry-run"
        asset_dir = target / "github-prerelease-assets"
        asset_dir.mkdir(parents=True)

        def write_ref(kind: str, filename: str) -> dict[str, object]:
            path = target / filename
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f"{kind}\n", encoding="utf-8")
            return {
                "kind": kind,
                "path": path.relative_to(repo_root).as_posix(),
                "exists": True,
                "sha256": "0" * 64,
            }

        artifact_refs = [
            write_ref("source_archive", "shardloom-source.tar.gz"),
            write_ref("release_notes", "github-prerelease-release-notes.md"),
            write_ref("release_binary", "shardloom"),
            write_ref("python_wheel", "shardloom-0.1.0-py3-none-any.whl"),
            write_ref("python_sdist", "shardloom-0.1.0.tar.gz"),
        ]
        sbom_refs = [
            write_ref("rust_workspace_sbom", "shardloom-rust-workspace.cdx.json"),
            write_ref("python_artifact_sbom", "shardloom-python-artifacts.cdx.json"),
            write_ref("cli_binary_sbom", "shardloom-cli-binary.cdx.json"),
        ]
        checksum_refs = [write_ref("checksum_manifest", "checksums.sha256")]
        provenance_ref = write_ref(
            "supply_chain_provenance", "supply-chain-release-evidence.json"
        )

        staged_refs = []
        for row in [*artifact_refs, *sbom_refs, *checksum_refs, provenance_ref]:
            if row["kind"] == omit_bundle_kind:
                continue
            staged_path = asset_dir / Path(str(row["path"])).name
            staged_path.write_text(f"{row['kind']}\n", encoding="utf-8")
            staged_refs.append(
                {
                    "kind": row["kind"],
                    "path": staged_path.relative_to(repo_root).as_posix(),
                    "exists": True,
                    "sha256": "1" * 64,
                    "source_path": row["path"],
                }
            )

        present_kinds = sorted(str(row["kind"]) for row in staged_refs)
        missing_kinds = [
            kind
            for kind in module.GITHUB_PRERELEASE_REQUIRED_ASSET_KINDS
            if kind not in present_kinds
        ]
        manifest = {
            "schema_version": module.GITHUB_PRERELEASE_BUNDLE_SCHEMA_VERSION,
            "status": "prepared_local_no_publication"
            if not missing_kinds
            else "blocked",
            "required_asset_kinds": module.GITHUB_PRERELEASE_REQUIRED_ASSET_KINDS,
            "present_asset_kinds": present_kinds,
            "missing_asset_kinds": missing_kinds,
            "staged_asset_refs": staged_refs,
            "publication_attempted": False,
            "tag_created": False,
            "secrets_required": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
        }
        manifest_path = asset_dir / "asset-manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

        return {
            "schema_version": "shardloom.supply_chain_release_evidence.v1",
            "provenance_status": "dry_run_unsigned_local_evidence",
            "fallback_dependency_absent": True,
            "artifact_refs": artifact_refs,
            "sbom_refs": sbom_refs,
            "checksum_refs": checksum_refs,
            "github_prerelease_asset_bundle_status": "prepared_local_no_publication",
            "github_prerelease_asset_manifest_ref": manifest_path.relative_to(
                repo_root
            ).as_posix(),
            "publication_attempted": False,
            "tag_created": False,
            "secrets_required": False,
            "external_runtime_dependencies_added": False,
            "fallback_engine_dependency_added": False,
        }

    def test_package_channel_local_gate_accepts_github_prerelease_asset_bundle(self) -> None:
        module = self._load_script_module(
            "check_package_channel_readiness.py",
            "check_package_channel_readiness_bundle_for_test",
        )
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            report = module.validate_local_gate_evidence(
                repo_root=repo_root,
                dependency_audit_report=self._package_channel_dependency_audit_fixture(),
                release_dry_run_transcript=self._package_channel_release_dry_run_fixture(),
                provenance_report=self._package_channel_provenance_fixture(
                    module, repo_root
                ),
            )

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(
            report["github_prerelease_asset_bundle"]["status"],
            "prepared_local_no_publication",
        )
        self.assertEqual(report["github_prerelease_asset_bundle"]["missing_asset_kinds"], [])

    def test_package_channel_local_gate_blocks_incomplete_github_prerelease_bundle(self) -> None:
        module = self._load_script_module(
            "check_package_channel_readiness.py",
            "check_package_channel_readiness_bundle_blocker_for_test",
        )
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            report = module.validate_local_gate_evidence(
                repo_root=repo_root,
                dependency_audit_report=self._package_channel_dependency_audit_fixture(),
                release_dry_run_transcript=self._package_channel_release_dry_run_fixture(),
                provenance_report=self._package_channel_provenance_fixture(
                    module,
                    repo_root,
                    omit_bundle_kind="python_sdist",
                ),
            )

        self.assertEqual(report["status"], "blocked")
        self.assertTrue(
            any("python_sdist" in blocker for blocker in report["blockers"]),
            report["blockers"],
        )

    def _python_registry_proof_fixture(
        self,
        *,
        channel_id: str = "testpypi",
        testpypi_proof_ref: str | None = None,
    ) -> dict[str, object]:
        artifact_filename = (
            f"shardloom-{SELECTED_PACKAGE_RELEASE_VERSION}-py3-none-any.whl"
        )
        return {
            "schema_version": "shardloom.python_registry_package_proof.v1",
            "proof_status": "passed",
            "channel_id": channel_id,
            "package_name": "shardloom",
            "package_version": SELECTED_PACKAGE_RELEASE_VERSION,
            "download_transcript_status": "passed",
            "install_transcript_status": "passed",
            "smoke_check_status": "passed",
            "uninstall_transcript_status": "passed",
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "tag_created": False,
            "secrets_required": False,
            "registry_upload_attempted_by_this_tool": False,
            "publication_attempted_by_this_tool": False,
            "package_channel_submission_attempted_by_this_tool": False,
            "testpypi_proof_ref": testpypi_proof_ref,
            "cli_binary_required_for_clean_registry_smoke": True,
            "cli_binary_available": True,
            "cli_binary_ref": "target/release/shardloom",
            "cli_binary_smoke_source": "approved_release_or_local_artifact",
            "registry_artifact_digest_binding_status": "passed",
            "downloaded_registry_artifact_ref": (
                "target/python-registry-package-proof/downloads/"
                f"{artifact_filename}"
            ),
            "downloaded_registry_artifact_filename": artifact_filename,
            "downloaded_registry_artifact_sha256": "a" * 64,
            "registry_download_isolated": True,
            "registry_download_cache_disabled": True,
            "registry_install_from_downloaded_artifact": True,
            "registry_install_cache_disabled": True,
            "registry_install_cache_hit_detected": False,
            "installed_registry_artifact": {
                "filename": artifact_filename,
                "sha256": "a" * 64,
                "url": f"https://example.invalid/{artifact_filename}",
            },
            "installed_registry_artifact_filename": artifact_filename,
            "installed_registry_artifact_sha256": "a" * 64,
            "registry_release_artifact_count": 1,
            "registry_release_artifacts": [
                {
                    "filename": artifact_filename,
                    "packagetype": "bdist_wheel",
                    "python_version": "py3",
                    "sha256": "a" * 64,
                    "size": 268000,
                    "url": f"https://example.invalid/{artifact_filename}",
                }
            ],
        }

    def _python_registry_matrix_row_fixture(
        self,
        *,
        channel_id: str = "testpypi",
        proof: dict[str, object] | None = None,
    ) -> dict[str, object]:
        proof = proof or self._python_registry_proof_fixture(channel_id=channel_id)
        installed_artifact = proof["installed_registry_artifact"]
        self.assertIsInstance(installed_artifact, dict)
        return {
            "channel_id": channel_id,
            "ready": True,
            "downloaded_registry_artifact_ref": proof["downloaded_registry_artifact_ref"],
            "downloaded_registry_artifact_filename": proof[
                "downloaded_registry_artifact_filename"
            ],
            "downloaded_registry_artifact_sha256": proof[
                "downloaded_registry_artifact_sha256"
            ],
            "installed_registry_artifact_ref": installed_artifact["url"],
            "installed_registry_artifact_filename": proof[
                "installed_registry_artifact_filename"
            ],
            "installed_registry_artifact_sha256": proof[
                "installed_registry_artifact_sha256"
            ],
        }

    def _registry_supply_chain_fixture(self, root: Path) -> tuple[dict, dict, dict]:
        matrix = json.loads((REPO_ROOT / "docs/release/package-channel-readiness-matrix.json").read_text())
        row = next(item for item in matrix["channels"] if item["channel_id"] == "testpypi")
        for field in ("registry_release_artifacts_ref", "sbom_ref", "checksum_ref", "provenance_ref"):
            source = REPO_ROOT / row[field]
            target = root / row[field]
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(source.read_bytes())
        stdout_ref = ("docs/release/channel-proofs/"
                      f"testpypi-v{SELECTED_PACKAGE_RELEASE_VERSION}-bundled-smoke.stdout.json")
        (root / stdout_ref).parent.mkdir(parents=True, exist_ok=True)
        (root / stdout_ref).write_bytes((REPO_ROOT / stdout_ref).read_bytes())
        transcript_path = root / row["registry_release_artifacts_ref"]
        proof = json.loads(transcript_path.read_text())
        provenance = json.loads((root / row["provenance_ref"]).read_text())
        return {"channels": [row]}, {"testpypi": proof}, provenance

    def test_registry_supply_chain_accepts_complete_channel_artifact_binding(self) -> None:
        module = self._load_script_module("check_package_channel_readiness.py", "registry_supply_pass")
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            matrix, proofs, _ = self._registry_supply_chain_fixture(root)
            report = module.validate_registry_supply_chain_evidence(root, matrix, proofs)
        self.assertEqual(report["blockers"], [])
        self.assertEqual(report["verified_channels"], ["testpypi"])

    def test_registry_supply_chain_rejects_wrong_or_incomplete_channel_evidence(self) -> None:
        module = self._load_script_module("check_package_channel_readiness.py", "registry_supply_bad")
        for mutation, expected in (
            ("github_refs", "must be a checked-in relative path"),
            ("other_channel", "channel_id must be testpypi"),
            ("uninstalled_artifact_digest", "must match every registry artifact"),
            ("missing_artifact", "must match every registry artifact"),
            ("tampered_sbom", "must bind sbom_ref path and SHA256"),
            ("incomplete_checksum", "must cover every registry artifact"),
            ("wrong_sbom_digest", "SBOM must bind artifact SHA256"),
            ("malformed_sbom_hashes", "SBOM must bind artifact SHA256"),
            ("invalid_path", "is not readable"),
            ("invalid_size", "requires a positive byte size"),
            ("boolean_run_id", "requires its publishing workflow run"),
            ("unrelated_source", "source_commit must match the approved channel build"),
            ("unrelated_workflow", "workflow_run_id must match the approved channel build"),
            ("consistent_omission", "must cover the exact approved distribution filenames"),
            ("wrong_declared_count", "artifact count must match the approved distribution inventory"),
            ("untrusted_host", "on test-files.pythonhosted.org"),
            ("wrong_url_filename", "on test-files.pythonhosted.org"),
            ("unbound_transcript", "must bind the complete channel proof"),
            ("wrong_smoke_ref", "smoke_transcript_ref must reference the bound channel proof"),
            ("missing_supplement", "bundled CLI proof is required"),
            ("failed_supplement", "proof_status must be passed"),
            ("fallback_supplement", "fallback_attempted must be false"),
            ("empty_supplement_steps", "requires all six ordered"),
            ("provenance_fallback", "registry provenance fallback_attempted must be false"),
            ("failed_digest_match", "must record a passed digest match"),
            ("untrusted_installed_url", "installed artifact and matrix URL must match"),
            ("missing_smoke_capture", "captured smoke stdout must match"),
            ("changed_smoke_capture", "result must equal the captured smoke stdout"),
            ("omitted_cli_inventory", "requires its approved platform CLI record"),
            ("missing_cli_component", "exact distribution and bundled CLI components"),
            ("wrong_cli_component_digest", "exact distribution and bundled CLI components"),
            ("wrong_cli_dependency", "bind each wheel to its bundled CLI dependency"),
            ("missing_cli_dependency", "bind each wheel to its bundled CLI dependency"),
            ("wrong_cli_member", "valid member, platform, digest and size"),
            ("wrong_cli_platform", "valid member, platform, digest and size"),
            ("invalid_cli_size", "valid member, platform, digest and size"),
            ("invalid_cli_digest", "valid member, platform, digest and size"),
            ("consistent_cli_digest", "registry provenance SHA256 must match the approved observation"),
            ("consistent_cli_size", "registry provenance SHA256 must match the approved observation"),
            ("sdist_cli", "source distribution must record no bundled CLI"),
        ):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                matrix, proofs, provenance = self._registry_supply_chain_fixture(root)
                row = matrix["channels"][0]
                transcript_path = root / row["registry_release_artifacts_ref"]
                sbom_path = root / row["sbom_ref"]
                checksum_path = root / row["checksum_ref"]
                provenance_path = root / row["provenance_ref"]
                if mutation == "github_refs":
                    row["provenance_ref"] = "https://github.com/depsilon/shardloom/releases/download/v0.2.4/supply-chain-release-evidence.json"
                elif mutation == "other_channel":
                    provenance["channel_id"] = "pypi"
                elif mutation == "uninstalled_artifact_digest":
                    provenance["artifact_refs"][1]["sha256"] = "c" * 64
                elif mutation == "missing_artifact":
                    provenance["artifact_refs"].pop()
                elif mutation == "invalid_path":
                    row["sbom_ref"] = "invalid\x00.json"
                elif mutation == "invalid_size":
                    provenance["artifact_refs"][0]["size_bytes"] = True
                    proofs["testpypi"]["registry_release_artifacts"][0]["size"] = True
                elif mutation == "boolean_run_id":
                    provenance["workflow_run_id"] = True
                    provenance["workflow_url"] = "https://github.com/depsilon/shardloom/actions/runs/True"
                elif mutation == "unrelated_source":
                    provenance["source_commit"] = "f" * 40
                elif mutation == "unrelated_workflow":
                    provenance["workflow_run_id"] = 123
                    provenance["workflow_url"] = "https://github.com/depsilon/shardloom/actions/runs/123"
                elif mutation == "wrong_declared_count":
                    proofs["testpypi"]["registry_release_artifact_count"] = 3
                    row["registry_release_artifact_count"] = 3
                elif mutation in {"untrusted_host", "wrong_url_filename"}:
                    for rows in (proofs["testpypi"]["registry_release_artifacts"], provenance["artifact_refs"]):
                        for artifact in rows:
                            host = "attacker.invalid" if mutation == "untrusted_host" else "test-files.pythonhosted.org"
                            filename = artifact["filename"] if mutation == "untrusted_host" else "wrong.whl"
                            artifact["url"] = f"https://{host}/packages/{filename}"
                elif mutation == "wrong_smoke_ref":
                    row["smoke_transcript_ref"] = "other.json"
                elif mutation == "missing_supplement":
                    proofs["testpypi"].pop("bundled_cli_supplemental_proof")
                elif mutation == "failed_supplement":
                    proofs["testpypi"]["bundled_cli_supplemental_proof"]["proof_status"] = "failed"
                elif mutation == "fallback_supplement":
                    proofs["testpypi"]["bundled_cli_supplemental_proof"]["fallback_attempted"] = True
                elif mutation == "empty_supplement_steps":
                    proofs["testpypi"]["bundled_cli_supplemental_proof"]["steps"] = []
                elif mutation == "provenance_fallback":
                    provenance["fallback_attempted"] = True
                elif mutation == "failed_digest_match":
                    provenance["artifact_refs"][0]["registry_digest_match"] = False
                elif mutation == "untrusted_installed_url":
                    proofs["testpypi"]["installed_registry_artifact"]["url"] = "https://attacker.invalid/file.whl"
                    row["installed_registry_artifact_ref"] = "https://attacker.invalid/file.whl"
                elif mutation in {"missing_smoke_capture", "changed_smoke_capture"}:
                    stdout_path = root / ("docs/release/channel-proofs/"
                        f"testpypi-v{SELECTED_PACKAGE_RELEASE_VERSION}-bundled-smoke.stdout.json")
                    if mutation == "missing_smoke_capture":
                        stdout_path.unlink()
                    else:
                        stdout_path.write_bytes(b"{}\n")
                        proofs["testpypi"]["bundled_cli_supplemental_proof"]["steps"][3]["stdout_sha256"] = hashlib.sha256(b"{}\n").hexdigest()
                elif mutation in {"omitted_cli_inventory", "missing_cli_component", "wrong_cli_component_digest",
                                  "wrong_cli_dependency", "missing_cli_dependency", "wrong_cli_member",
                                  "wrong_cli_platform", "invalid_cli_size", "invalid_cli_digest",
                                  "consistent_cli_digest", "consistent_cli_size", "sdist_cli"}:
                    sbom = json.loads(sbom_path.read_text())
                    wheel = next(item for item in provenance["artifact_refs"] if "bundled_cli" in item)
                    child_ref = "sha256:" + wheel["sha256"] + ":bundled-cli"
                    if mutation == "omitted_cli_inventory":
                        for item in provenance["artifact_refs"]:
                            item.pop("bundled_cli", None)
                        sbom["components"] = [item for item in sbom["components"] if not item["bom-ref"].endswith(":bundled-cli")]
                        sbom["dependencies"] = []
                    elif mutation == "missing_cli_component":
                        sbom["components"] = [item for item in sbom["components"] if item["bom-ref"] != child_ref]
                    elif mutation == "wrong_cli_component_digest":
                        next(item for item in sbom["components"] if item["bom-ref"] == child_ref)["hashes"][0]["content"] = "f" * 64
                    elif mutation == "wrong_cli_dependency":
                        sbom["dependencies"][0]["dependsOn"] = ["sha256:unrelated"]
                    elif mutation == "missing_cli_dependency":
                        sbom["dependencies"] = []
                    elif mutation == "sdist_cli":
                        next(item for item in provenance["artifact_refs"] if item["filename"].endswith(".tar.gz"))["bundled_cli"] = wheel["bundled_cli"]
                    elif mutation == "consistent_cli_digest":
                        wheel["bundled_cli"]["sha256"] = "f" * 64
                        next(item for item in sbom["components"] if item["bom-ref"] == child_ref)["hashes"][0]["content"] = "f" * 64
                    elif mutation == "consistent_cli_size":
                        wheel["bundled_cli"]["size_bytes"] = 1
                    else:
                        field, value = {"wrong_cli_member": ("member", "shardloom/bin/other/shardloom"),
                                        "wrong_cli_platform": ("platform", "other"),
                                        "invalid_cli_size": ("size_bytes", False),
                                        "invalid_cli_digest": ("sha256", "invalid")}[mutation]
                        wheel["bundled_cli"][field] = value
                    sbom_path.write_text(json.dumps(sbom))
                    provenance["sbom_ref"]["sha256"] = hashlib.sha256(sbom_path.read_bytes()).hexdigest()
                elif mutation == "consistent_omission":
                    removed = provenance["artifact_refs"].pop(1)
                    omitted = removed["filename"]
                    proofs["testpypi"]["registry_release_artifacts"] = [item for item in
                        proofs["testpypi"]["registry_release_artifacts"] if item["filename"] != omitted]
                    proofs["testpypi"]["registry_release_artifact_count"] = 3
                    row["registry_release_artifact_count"] = 3
                    sbom = json.loads(sbom_path.read_text())
                    sbom["components"] = [item for item in sbom["components"] if item["name"] != omitted
                                          and not item["name"].startswith(omitted + "!/")]
                    sbom["dependencies"] = [item for item in sbom["dependencies"]
                                            if item["ref"] != "sha256:" + removed["sha256"]]
                    sbom_path.write_text(json.dumps(sbom))
                    checksum_path.write_text("".join(
                        f"{item['sha256']}  {item['filename']}\n" for item in provenance["artifact_refs"]
                    ))
                    for field in ("sbom_ref", "checksum_ref"):
                        provenance[field]["sha256"] = hashlib.sha256((root / row[field]).read_bytes()).hexdigest()
                elif mutation == "tampered_sbom":
                    with sbom_path.open("a") as handle:
                        handle.write("\n")
                elif mutation == "incomplete_checksum":
                    checksum_path.write_text("")
                    provenance["checksum_ref"]["sha256"] = hashlib.sha256(b"").hexdigest()
                elif mutation in {"wrong_sbom_digest", "malformed_sbom_hashes"}:
                    sbom = json.loads(sbom_path.read_text())
                    component = next(item for item in sbom["components"] if "!/" not in item["name"])
                    if mutation == "wrong_sbom_digest":
                        component["hashes"][0]["content"] = "c" * 64
                    else:
                        component["hashes"] = None
                    sbom_path.write_text(json.dumps(sbom))
                    provenance["sbom_ref"]["sha256"] = hashlib.sha256(
                        sbom_path.read_bytes()).hexdigest()
                transcript_path.write_text(json.dumps(proofs["testpypi"]))
                provenance["channel_proof_ref"]["sha256"] = hashlib.sha256(transcript_path.read_bytes()).hexdigest()
                if mutation == "unbound_transcript":
                    with transcript_path.open("a") as handle:
                        handle.write("\n")
                provenance_path.write_text(json.dumps(provenance))
                report = module.validate_registry_supply_chain_evidence(root, matrix, proofs)
                self.assertEqual(report["status"], "blocked")
                self.assertIn(expected, "; ".join(report["blockers"]))

    def test_registry_supply_chain_evidence_required_only_for_ready_channels(self) -> None:
        module = self._load_script_module("check_package_channel_readiness.py", "registry_supply_missing")
        with tempfile.TemporaryDirectory() as temp:
            matrix = {"channels": [{"channel_id": "testpypi", "ready": False}]}
            self.assertEqual(module.validate_registry_supply_chain_evidence(
                Path(temp), matrix, {})["status"], "passed")
            matrix["channels"][0]["ready"] = True
            self.assertEqual(module.validate_registry_supply_chain_evidence(
                Path(temp), matrix, {})["status"], "blocked")
            matrix, proofs, _ = self._registry_supply_chain_fixture(Path(temp))
            module.PUBLISHED_REGISTRY_BUILD_IDENTITIES = {}
            report = module.validate_registry_supply_chain_evidence(Path(temp), matrix, proofs)
            self.assertIn("no approved registry build identity", "; ".join(report["blockers"]))
            module.PUBLISHED_REGISTRY_DISTRIBUTIONS = {}
            report = module.validate_registry_supply_chain_evidence(Path(temp), matrix, proofs)
            self.assertIn("no approved registry distribution inventory", "; ".join(report["blockers"]))

    def test_python_registry_package_proof_commands_are_channel_specific(self) -> None:
        module = self._load_script_module(
            "python_registry_package_proof.py",
            "python_registry_package_proof_commands_for_test",
        )

        download_dir = Path("/tmp/shardloom-downloads")
        testpypi_command = module.download_command(
            Path("/clean/bin/python"),
            module.REGISTRY_CHANNELS["testpypi"],
            "0.1.0",
            download_dir,
        )
        pypi_command = module.download_command(
            Path("/clean/bin/python"),
            module.REGISTRY_CHANNELS["pypi"],
            "0.1.0",
            download_dir,
        )
        local_install_command = module.install_downloaded_artifact_command(
            Path("/clean/bin/python"),
            download_dir / "shardloom-0.1.0-py3-none-any.whl",
        )
        smoke_command = " ".join(module.smoke_command(Path("/clean/bin/python")))

        self.assertIn("--no-deps", testpypi_command)
        self.assertIn("--isolated", testpypi_command)
        self.assertIn("--no-cache-dir", testpypi_command)
        self.assertIn("--only-binary", testpypi_command)
        self.assertIn("--dest", testpypi_command)
        self.assertIn(str(download_dir), testpypi_command)
        self.assertIn("--index-url", testpypi_command)
        self.assertIn("https://test.pypi.org/simple/", testpypi_command)
        self.assertEqual(testpypi_command[-1], "shardloom==0.1.0")
        self.assertIn("--isolated", pypi_command)
        self.assertIn("--no-cache-dir", pypi_command)
        self.assertIn("--index-url", pypi_command)
        self.assertIn("https://pypi.org/simple/", pypi_command)
        self.assertEqual(pypi_command[-1], "shardloom==0.1.0")
        self.assertIn("--no-index", local_install_command)
        self.assertIn(str(download_dir / "shardloom-0.1.0-py3-none-any.whl"), local_install_command)
        self.assertIn("smoke.fallback_attempted", smoke_command)
        self.assertIn("external_engine_invoked", smoke_command)
        proof_env = module.smoke_env(Path("/release/shardloom"))
        self.assertEqual(proof_env["SHARDLOOM_BIN"], "/release/shardloom")

    def test_python_registry_package_proof_blocks_pypi_without_testpypi_ref(self) -> None:
        module = self._load_script_module(
            "python_registry_package_proof.py",
            "python_registry_package_proof_pypi_ref_for_test",
        )

        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            output = repo_root / "target" / "registry-proof.json"
            status = module.write_transcript(
                repo_root=repo_root,
                output=output,
                channel=module.REGISTRY_CHANNELS["pypi"],
                version="0.1.0",
                venv_dir=repo_root / "target" / "proof-venv",
                steps=[],
                testpypi_proof_ref=None,
                shardloom_bin=repo_root / "target" / "release" / "shardloom",
            )
            report = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(status, 1)
        self.assertEqual(report["proof_status"], "failed")
        self.assertIn("prior TestPyPI proof", "\n".join(report["blockers"]))
        self.assertFalse(report["registry_upload_attempted_by_this_tool"])
        self.assertFalse(report["publication_attempted_by_this_tool"])
        self.assertFalse(report["package_channel_submission_attempted_by_this_tool"])

    def test_python_registry_package_proof_requires_cli_binary_for_smoke(self) -> None:
        module = self._load_script_module(
            "python_registry_package_proof.py",
            "python_registry_package_proof_cli_binary_for_test",
        )

        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            output = repo_root / "target" / "registry-proof.json"
            status = module.write_transcript(
                repo_root=repo_root,
                output=output,
                channel=module.REGISTRY_CHANNELS["testpypi"],
                version="0.1.0",
                venv_dir=repo_root / "target" / "proof-venv",
                steps=[],
                testpypi_proof_ref=None,
                shardloom_bin=None,
                setup_blockers=["registry proof requires --shardloom-bin or SHARDLOOM_BIN"],
            )
            report = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(status, 1)
        self.assertEqual(report["proof_status"], "failed")
        self.assertFalse(report["cli_binary_available"])
        self.assertIn("SHARDLOOM_BIN", "\n".join(report["blockers"]))

    def test_python_registry_package_proof_fails_when_smoke_flags_fire(self) -> None:
        module = self._load_script_module(
            "python_registry_package_proof.py",
            "python_registry_package_proof_smoke_flags_for_test",
        )

        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            output = repo_root / "target" / "registry-proof.json"
            status = module.write_transcript(
                repo_root=repo_root,
                output=output,
                channel=module.REGISTRY_CHANNELS["testpypi"],
                version="0.1.0",
                venv_dir=repo_root / "target" / "proof-venv",
                steps=[
                    {"name": "install_registry_package", "returncode": 0},
                    {
                        "name": "registry_package_client_smoke",
                        "returncode": 0,
                        "stdout": (
                            "fallback_attempted=True\n"
                            "external_engine_invoked=True\n"
                        ),
                    },
                    {"name": "uninstall_registry_package", "returncode": 0},
                ],
                testpypi_proof_ref=None,
                shardloom_bin=repo_root / "target" / "release" / "shardloom",
            )
            report = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(status, 1)
        self.assertEqual(report["proof_status"], "failed")
        self.assertTrue(report["fallback_attempted"])
        self.assertTrue(report["external_engine_invoked"])
        self.assertIn("fallback_attempted=True", "\n".join(report["blockers"]))
        self.assertIn("external_engine_invoked=True", "\n".join(report["blockers"]))

    def test_package_channel_registry_proofs_are_optional_until_channel_ready(self) -> None:
        module = self._load_script_module(
            "check_package_channel_readiness.py",
            "check_package_channel_readiness_registry_optional_for_test",
        )
        matrix = {
            "channels": [
                {"channel_id": "testpypi", "ready": False},
                {"channel_id": "pypi", "ready": False},
            ]
        }

        report = module.validate_python_registry_package_proofs(
            matrix,
            testpypi_proof=None,
            pypi_proof=None,
        )

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertFalse(report["publication_attempted"])
        self.assertFalse(report["external_engine_invoked"])

    def test_package_channel_registry_proofs_require_testpypi_before_pypi(self) -> None:
        module = self._load_script_module(
            "check_package_channel_readiness.py",
            "check_package_channel_readiness_registry_sequence_for_test",
        )
        matrix = {
            "channels": [
                {"channel_id": "testpypi", "ready": False},
                {"channel_id": "pypi", "ready": True},
            ]
        }

        report = module.validate_python_registry_package_proofs(
            matrix,
            testpypi_proof=None,
            pypi_proof=self._python_registry_proof_fixture(channel_id="pypi"),
        )

        blockers = "\n".join(report["blockers"])
        self.assertEqual(report["status"], "blocked")
        self.assertIn("requires testpypi_proof_ref", blockers)
        self.assertIn("requires prior TestPyPI proof", blockers)
        self.assertIn("requires testpypi ready first", blockers)

    def test_package_channel_registry_proofs_accept_ordered_public_proofs(self) -> None:
        module = self._load_script_module(
            "check_package_channel_readiness.py",
            "check_package_channel_readiness_registry_pass_for_test",
        )
        testpypi_proof = self._python_registry_proof_fixture(channel_id="testpypi")
        pypi_proof = self._python_registry_proof_fixture(
            channel_id="pypi",
            testpypi_proof_ref=(
                "docs/release/channel-proofs/"
                f"testpypi-v{SELECTED_PACKAGE_RELEASE_VERSION}-transcript.json"
            ),
        )
        matrix = {
            "channels": [
                self._python_registry_matrix_row_fixture(
                    channel_id="testpypi",
                    proof=testpypi_proof,
                ),
                self._python_registry_matrix_row_fixture(
                    channel_id="pypi",
                    proof=pypi_proof,
                ),
            ]
        }

        report = module.validate_python_registry_package_proofs(
            matrix,
            testpypi_proof=testpypi_proof,
            pypi_proof=pypi_proof,
        )

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertTrue(report["pypi_requires_prior_testpypi"])
        self.assertEqual(
            report["pypi"]["registry_artifact_digest_binding_status"],
            "passed",
        )
        self.assertTrue(report["pypi"]["registry_download_isolated"])
        self.assertTrue(report["pypi"]["registry_download_cache_disabled"])
        self.assertTrue(report["pypi"]["registry_install_from_downloaded_artifact"])
        self.assertTrue(report["pypi"]["registry_install_cache_disabled"])
        self.assertFalse(report["pypi"]["registry_install_cache_hit_detected"])
        self.assertEqual(report["pypi"]["downloaded_registry_artifact_sha256"], "a" * 64)
        self.assertEqual(report["pypi"]["installed_registry_artifact_sha256"], "a" * 64)
        self.assertFalse(report["publication_attempted"])

    def test_package_channel_registry_proofs_reject_stale_matrix_artifact_fields(
        self,
    ) -> None:
        module = self._load_script_module(
            "check_package_channel_readiness.py",
            "check_package_channel_readiness_registry_matrix_stale_for_test",
        )
        testpypi_proof = self._python_registry_proof_fixture(channel_id="testpypi")
        matrix_row = self._python_registry_matrix_row_fixture(
            channel_id="testpypi",
            proof=testpypi_proof,
        )
        matrix_row["downloaded_registry_artifact_sha256"] = "b" * 64
        matrix = {"channels": [matrix_row]}

        report = module.validate_python_registry_package_proofs(
            matrix,
            testpypi_proof=testpypi_proof,
            pypi_proof=None,
        )

        blockers = "\n".join(report["blockers"])
        self.assertEqual(report["status"], "blocked")
        self.assertIn(
            "matrix downloaded_registry_artifact_sha256 must match registry proof transcript",
            blockers,
        )

    def test_package_channel_registry_proofs_reject_stale_release_version(self) -> None:
        module = self._load_script_module(
            "check_package_channel_readiness.py",
            "check_package_channel_readiness_registry_stale_version_for_test",
        )
        stale_proof = self._python_registry_proof_fixture(channel_id="pypi")
        stale_proof["package_version"] = "0.2.0"
        stale_proof["downloaded_registry_artifact_filename"] = (
            "shardloom-0.2.0-py3-none-any.whl"
        )
        stale_proof["installed_registry_artifact_filename"] = (
            "shardloom-0.2.0-py3-none-any.whl"
        )
        matrix = {"channels": [{"channel_id": "pypi", "ready": True}]}

        report = module.validate_python_registry_package_proofs(
            matrix,
            testpypi_proof=None,
            pypi_proof=stale_proof,
        )

        blockers = "\n".join(report["blockers"])
        self.assertEqual(report["status"], "blocked")
        self.assertIn(
            f"package_version must be {SELECTED_PACKAGE_RELEASE_VERSION}", blockers
        )
        self.assertIn(
            "downloaded registry artifact must match "
            f"{SELECTED_PACKAGE_RELEASE_VERSION}",
            blockers,
        )
        self.assertIn(
            "installed registry artifact must match "
            f"{SELECTED_PACKAGE_RELEASE_VERSION}",
            blockers,
        )

    def test_package_channel_registry_proofs_reject_stale_testpypi_ref(self) -> None:
        module = self._load_script_module(
            "check_package_channel_readiness.py",
            "check_package_channel_readiness_registry_stale_testpypi_ref_for_test",
        )
        pypi_proof = self._python_registry_proof_fixture(
            channel_id="pypi",
            testpypi_proof_ref=(
                "docs/release/channel-proofs/testpypi-v0.2.0-transcript.json"
            ),
        )
        matrix = {
            "channels": [
                {"channel_id": "testpypi", "ready": True},
                {"channel_id": "pypi", "ready": True},
            ]
        }

        report = module.validate_python_registry_package_proofs(
            matrix,
            testpypi_proof=self._python_registry_proof_fixture(channel_id="testpypi"),
            pypi_proof=pypi_proof,
        )

        blockers = "\n".join(report["blockers"])
        self.assertEqual(report["status"], "blocked")
        self.assertIn(
            "testpypi_proof_ref must be "
            "docs/release/channel-proofs/"
            f"testpypi-v{SELECTED_PACKAGE_RELEASE_VERSION}-transcript.json",
            blockers,
        )

    def test_package_channel_registry_proofs_reject_downloaded_digest_mismatch(self) -> None:
        module = self._load_script_module(
            "check_package_channel_readiness.py",
            "check_package_channel_readiness_registry_download_digest_for_test",
        )
        bad_proof = self._python_registry_proof_fixture(channel_id="pypi")
        bad_proof["downloaded_registry_artifact_sha256"] = "b" * 64
        matrix = {"channels": [{"channel_id": "pypi", "ready": True}]}

        report = module.validate_python_registry_package_proofs(
            matrix,
            testpypi_proof=None,
            pypi_proof=bad_proof,
        )

        blockers = "\n".join(report["blockers"])
        self.assertEqual(report["status"], "blocked")
        self.assertIn("installed SHA256 must match downloaded SHA256", blockers)
        self.assertIn("downloaded registry artifact SHA256 must match registry JSON", blockers)

    def test_package_channel_readiness_excludes_not_in_v1_rows_from_ready_claim(self) -> None:
        module = self._load_script_module(
            "check_package_channel_readiness.py",
            "check_package_channel_readiness_v1_scope_ready_for_test",
        )
        matrix = json.loads(
            (REPO_ROOT / "docs/release/package-channel-readiness-matrix.json").read_text(
                encoding="utf-8"
            )
        )
        matrix["status"] = "ready"
        matrix["public_package_release_claim_allowed"] = True
        for row in matrix["channels"]:
            if row["v1_feasibility_status"] == "not_in_v1_scope_recorded":
                row["ready"] = False
                row["status"] = "not_in_v1_scope_recorded"
                row["current_blockers"] = ["not in v1 scope"]
                continue
            row["ready"] = True
            row["status"] = "ready"
            row["current_blockers"] = []
            for field in module.READY_PROOF_FIELDS:
                row[field] = "passed"
            for field in module.READY_REFERENCE_FIELDS:
                row[field] = f"target/package-channel-readiness/{row['channel_id']}/{field}.json"

        blockers = module.validate_matrix(matrix)

        self.assertNotIn(
            "public_package_release_claim_allowed=true requires every channel ready",
            blockers,
        )
        self.assertNotIn("top-level status=ready requires every channel ready", blockers)
        self.assertFalse(
            [
                blocker
                for blocker in blockers
                if "requires every v1-scope channel ready" in blocker
            ],
            blockers,
        )

    def test_dependency_audit_resolves_configured_pip_audit_python(self) -> None:
        module = self._load_script_module(
            "check_dependency_audit.py", "check_dependency_audit_pip_audit_for_test"
        )

        with self._temporary_env(SHARDLOOM_PIP_AUDIT_PYTHON="/tool/python"):
            prefix = module.resolve_pip_audit_command(
                module_available=lambda candidate: candidate == "/tool/python",
                executable_lookup=lambda _name: None,
                home=Path("/missing-home"),
            )

        self.assertEqual(prefix, ["/tool/python", "-m", "pip_audit"])

    def test_dependency_audit_resolves_path_pip_audit_when_current_python_lacks_module(self) -> None:
        module = self._load_script_module(
            "check_dependency_audit.py",
            "check_dependency_audit_pip_audit_path_for_test",
        )

        prefix = module.resolve_pip_audit_command(
            module_available=lambda _candidate: False,
            executable_lookup=lambda name: "/usr/local/bin/pip-audit" if name == "pip-audit" else None,
            home=Path("/missing-home"),
        )

        self.assertEqual(prefix, ["/usr/local/bin/pip-audit"])

    def test_dependency_audit_resolves_target_local_pip_audit_venv(self) -> None:
        module = self._load_script_module(
            "check_dependency_audit.py",
            "check_dependency_audit_pip_audit_target_venv_for_test",
        )
        target_python = (
            REPO_ROOT
            / "target"
            / "release-readiness-audit"
            / "pip-audit-venv"
            / "bin"
            / "python"
        )

        prefix = module.resolve_pip_audit_command(
            module_available=lambda candidate: candidate == str(target_python),
            executable_lookup=lambda _name: None,
            home=Path("/missing-home"),
        )

        self.assertEqual(prefix, [str(target_python), "-m", "pip_audit"])

    def test_dependency_audit_probes_symlinked_python_by_executing_it(self) -> None:
        module = self._load_script_module(
            "check_dependency_audit.py",
            "check_dependency_audit_symlinked_python_for_test",
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package = root / "fake-site" / "pip_audit"
            package.mkdir(parents=True)
            (package / "__init__.py").write_text("", encoding="utf-8")
            symlinked_python = root / "venv-python"
            symlinked_python.symlink_to(Path(sys.executable))

            with self._temporary_env(PYTHONPATH=str(root / "fake-site")):
                self.assertTrue(module.pip_audit_module_available(str(symlinked_python)))

    def test_dependency_audit_reports_all_benchmark_requirement_profiles(self) -> None:
        module = self._load_script_module(
            "check_dependency_audit.py",
            "check_dependency_audit_benchmark_profiles_for_test",
        )

        report = module.check_benchmark_dependency_scope()
        profile_files = {
            row["requirements_file"]
            for row in report["profiles"]
        }

        self.assertEqual(report["profile_count"], 4)
        self.assertIn("benchmarks/traditional_analytics/requirements.txt", profile_files)
        self.assertIn(
            "benchmarks/traditional_analytics/requirements-extended-local.txt",
            profile_files,
        )
        self.assertIn("benchmarks/traditional_analytics/requirements-spark.txt", profile_files)
        self.assertIn(
            "benchmarks/traditional_analytics/requirements-gpu-optional.txt",
            profile_files,
        )
        self.assertIn("pandas", report["external_baseline_dependencies"])
        self.assertIn("pyspark", report["external_baseline_dependencies"])
        self.assertIn("ray", report["external_baseline_dependencies"])

    def test_release_validation_evidence_uses_configured_python_and_conda(self) -> None:
        module = self._load_script_module(
            "run_release_validation_evidence.py",
            "run_release_validation_evidence_python_for_test",
        )
        args = type(
            "Args",
            (),
            {
                "require_clean_conda": True,
                "conda_executable": Path("/opt/homebrew/bin/micromamba"),
            },
        )()

        commands = dict(module.required_validation_commands("/tool/python3.12", args))

        self.assertEqual(
            commands["python_unittest"],
            ["/tool/python3.12", "-m", "unittest", "discover", "python/tests"],
        )
        self.assertEqual(commands["python_build"], ["/tool/python3.12", "-m", "build", "python"])
        self.assertEqual(commands["release_security_gate"][0], "/tool/python3.12")
        self.assertEqual(
            commands["workspace_version_source_contract"],
            ["/tool/python3.12", "scripts/check_workspace_version_sources.py"],
        )
        self.assertEqual(commands["package_channel_readiness"][0], "/tool/python3.12")
        self.assertEqual(
            commands["v1_source_prepared_state_scope_gate"],
            ["/tool/python3.12", "scripts/check_v1_source_prepared_state_scope.py"],
        )
        self.assertEqual(
            commands["v1_local_output_sink_scope_gate"],
            ["/tool/python3.12", "scripts/check_v1_local_output_sink_scope.py"],
        )
        self.assertEqual(
            commands["v1_local_resource_safety_gate"],
            ["/tool/python3.12", "scripts/check_v1_local_resource_safety.py"],
        )
        self.assertEqual(
            commands["v1_observability_support_gate"],
            ["/tool/python3.12", "scripts/check_v1_observability_support.py"],
        )
        self.assertEqual(
            commands["v1_api_schema_stability_gate"],
            ["/tool/python3.12", "scripts/check_v1_api_schema_stability.py"],
        )
        self.assertEqual(
            commands["v1_example_replay_gate"],
            ["/tool/python3.12", "scripts/check_v1_example_replay.py"],
        )
        self.assertEqual(
            commands["v1_correctness_conformance_gate"],
            ["/tool/python3.12", "scripts/check_v1_correctness_conformance.py"],
        )
        self.assertEqual(
            commands["v1_security_ci_hardening_gate"],
            ["/tool/python3.12", "scripts/check_v1_security_ci_hardening.py"],
        )
        self.assertEqual(
            commands["v1_release_boundary_firewall"],
            ["/tool/python3.12", "scripts/check_v1_release_boundary.py"],
        )
        self.assertEqual(
            commands["final_release_approval_post_release_verification"],
            ["/tool/python3.12", "scripts/check_final_release_approval.py"],
        )
        self.assertNotIn("benchmark_constitution", commands)
        self.assertNotIn("benchmark_artifact_completeness", commands)
        self.assertNotIn("benchmark_publication_claim_gate", commands)
        self.assertNotIn("front_door_benchmark_publication_gate", commands)
        self.assertNotIn("pre_5j_dependency_freshness_gate", commands)
        self.assertEqual(
            commands["v1_front_door_runtime_scope_gate"],
            ["/tool/python3.12", "scripts/check_v1_front_door_runtime_scope.py"],
        )
        self.assertEqual(
            commands["release_dry_run_proof"],
            [
                "/tool/python3.12",
                "scripts/release_dry_run_proof.py",
                "--rows",
                "64",
                "--iterations",
                "1",
                "--require-clean-conda",
                "--conda-executable",
                "/opt/homebrew/bin/micromamba",
            ],
        )

    def test_final_release_approval_contract_blocks_pending_website_verification(self) -> None:
        module = self._load_script_module(
            "check_final_release_approval.py",
            "check_final_release_approval_for_test",
        )

        contract = json.loads((REPO_ROOT / module.DEFAULT_CONTRACT).read_text())
        contract.update(public_release_ready=False, post_release_verification_ready=False)
        for row in contract["verification_rows"]:
            row["verification_status"] = (
                "pending_deployment_verification" if row["row_id"] in
                {"docs_links_public_smoke", "website_support_matrix_public_smoke"} else "passed"
            )
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            contract_path = repo_root / "pending-website-contract.json"
            contract_path.write_text(json.dumps(contract))
            report = module.build_report(repo_root, contract_path=contract_path)
            public_report = module.build_report(
                repo_root, contract_path=contract_path, require_public_release_ready=True,
            )
            # Passed top-level flags cannot hide incomplete public website rows.
            contract.update(public_release_ready=True, post_release_verification_ready=True)
            contract_path.write_text(json.dumps(contract))
            premature_report = module.build_report(
                repo_root, contract_path=contract_path, require_public_release_ready=True,
            )

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(report["contract_validation_status"], "passed")
        self.assertFalse(report["public_release_ready"])
        self.assertFalse(report["post_release_verification_ready"])
        self.assertEqual(report["publication_authorization_state"], "approved")
        self.assertEqual(public_report["status"], "failed")
        self.assertEqual(public_report["public_release_blockers"], [
            "public_release_ready must be true",
            "post_release_verification_ready must be true",
            "docs_links_public_smoke: verification_status=pending_deployment_verification",
            "website_support_matrix_public_smoke: verification_status=pending_deployment_verification",
        ])
        self.assertEqual(report["public_release_blockers"], public_report["public_release_blockers"])
        self.assertEqual(premature_report["status"], "failed")
        self.assertEqual(premature_report["public_release_blockers"], public_report["public_release_blockers"][2:])
        self.assertFalse(public_report["fallback_attempted"])
        self.assertFalse(public_report["external_engine_invoked"])

    def test_final_release_approval_contract_accepts_approved_verified_contract(self) -> None:
        module = self._load_script_module(
            "check_final_release_approval.py",
            "check_final_release_approval_approved_for_test",
        )
        contract = json.loads(
            (REPO_ROOT / module.DEFAULT_CONTRACT).read_text(encoding="utf-8")
        )
        contract.update(
            {
                "status": "approved",
                "publication_authorization_state": "approved",
                "public_release_ready": True,
                "post_release_verification_ready": True,
                "approved_release_tag": "v0.1.0",
                "approved_release_commit": "abc123",
                "approved_package_channels": ["github_prerelease", "testpypi", "pypi"],
            }
        )
        for row in contract["verification_rows"]:
            row["verification_status"] = "passed"

        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            contract_path = repo_root / "approved-contract.json"
            output_path = repo_root / "report.json"
            contract_path.write_text(json.dumps(contract), encoding="utf-8")
            report = module.build_report(
                repo_root,
                contract_path=contract_path,
                output_path=output_path,
                require_public_release_ready=True,
            )

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(report["contract_validation_status"], "passed")
        self.assertTrue(report["public_release_ready"])
        self.assertTrue(report["post_release_verification_ready"])

    def _write_v1_correctness_conformance_fixture_reports(
        self,
        module: object,
        repo_root: Path,
    ) -> None:
        paths = module.ReportPaths()
        false_fields = {field: False for field in module.FALSE_REPORT_FIELDS}

        def write(path: Path, payload: dict[str, object]) -> None:
            resolved = repo_root / path
            resolved.parent.mkdir(parents=True, exist_ok=True)
            resolved.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

        write(
            module.DEFAULT_MATRIX,
            {
                "schema_version": module.MATRIX_SCHEMA_VERSION,
                "matrix_id": module.GATE_ID,
                "status": "v1_correctness_scope_declared",
                "correctness_claim_requires_report": True,
                "external_engines_allowed_as_oracles_only": True,
                "external_oracle_required_for_v1": False,
                "public_release_claim_allowed": False,
                "public_package_claim_allowed": False,
                "performance_claim_allowed": False,
                "production_claim_allowed": False,
                "spark_replacement_claim_allowed": False,
                "runtime_execution": False,
                "publication_attempted": False,
                "tag_created": False,
                "package_upload_attempted": False,
                "fallback_attempted": False,
                "external_engine_invoked": False,
                "report_inputs": [
                    {
                        "report_id": "golden_workflow",
                        "path": "target/golden-workflow-report.json",
                        "schema_version": "shardloom.golden_workflow_validation_report.v1",
                        "required_status": "passed",
                    },
                    {
                        "report_id": "admitted_semantics",
                        "path": "target/admitted-semantics-matrix-report.json",
                        "schema_version": "shardloom.admitted_semantics_matrix_report.v1",
                        "required_status": "passed",
                    },
                    {
                        "report_id": "front_door",
                        "path": "target/v1-front-door-runtime-scope-report.json",
                        "schema_version": (
                            "shardloom.v1_front_door_runtime_scope_report.v1"
                        ),
                        "required_status": "passed",
                    },
                    {
                        "report_id": "vortex_runtime",
                        "path": "target/v1-vortex-runtime-scope-report.json",
                        "schema_version": "shardloom.v1_vortex_runtime_scope_report.v1",
                        "required_status": "passed",
                    },
                    {
                        "report_id": "source_prepared_state",
                        "path": "target/v1-source-prepared-state-scope-report.json",
                        "schema_version": (
                            "shardloom.v1_source_prepared_state_scope_report.v1"
                        ),
                        "required_status": "passed",
                    },
                    {
                        "report_id": "local_output_sink",
                        "path": "target/v1-local-output-sink-scope-report.json",
                        "schema_version": (
                            "shardloom.v1_local_output_sink_scope_report.v1"
                        ),
                        "required_status": "passed",
                    },
                    {
                        "report_id": "python_user_surface",
                        "path": "target/python-user-surface-completion-gate.json",
                        "schema_version": (
                            "shardloom.python_user_surface_completion_gate.v1"
                        ),
                        "required_status": "passed",
                    },
                    {
                        "report_id": "example_replay",
                        "path": "target/v1-example-replay-report.json",
                        "schema_version": "shardloom.v1_example_replay_report.v1",
                        "required_status": "passed",
                    },
                ],
                "expected_counts": {
                    "front_door_supported_rows": (
                        module.EXPECTED_FRONT_DOOR_SUPPORTED_ROWS
                    ),
                    "front_door_pending_rows": module.EXPECTED_FRONT_DOOR_PENDING_ROWS,
                    "front_door_example_scenarios": len(
                        module.EXPECTED_EXAMPLE_SCENARIOS
                    ),
                    "front_door_expected_error_scenarios": len(
                        module.EXPECTED_ERROR_SCENARIOS
                    ),
                    "vortex_primitive_routes": module.EXPECTED_VORTEX_PRIMITIVE_ROUTES,
                    "source_input_formats": module.EXPECTED_SOURCE_INPUT_FORMATS,
                    "source_prepared_routes": len(module.EXPECTED_SOURCE_ROUTE_IDS),
                    "source_invalidation_cases": (
                        module.EXPECTED_SOURCE_INVALIDATION_CASES
                    ),
                    "output_formats": module.EXPECTED_OUTPUT_FORMATS,
                    "output_write_methods": module.EXPECTED_OUTPUT_WRITE_METHODS,
                    "output_routes": module.EXPECTED_OUTPUT_ROUTE_IDS,
                    "python_user_surface_method_rows": (
                        module.EXPECTED_PYTHON_USER_SURFACE_METHOD_ROWS
                    ),
                    "example_replay_doc_sources": (
                        module.EXPECTED_EXAMPLE_REPLAY_DOC_SOURCES
                    ),
                    "example_replay_runtime_commands": (
                        module.EXPECTED_EXAMPLE_REPLAY_RUNTIME_COMMANDS
                    ),
                    "example_replay_scenarios": (
                        module.EXPECTED_EXAMPLE_REPLAY_SCENARIOS
                    ),
                    "example_replay_expected_error_scenarios": (
                        module.EXPECTED_EXAMPLE_REPLAY_ERROR_SCENARIOS
                    ),
                    "example_replay_unsupported_failure_fixtures": (
                        module.EXPECTED_EXAMPLE_REPLAY_UNSUPPORTED_FIXTURES
                    ),
                    "golden_workflows": len(module.EXPECTED_GOLDEN_WORKFLOWS),
                    "golden_stage_count_min": module.EXPECTED_GOLDEN_STAGE_COUNT_MIN,
                    "executable_fixtures": module.EXPECTED_EXECUTABLE_FIXTURES,
                    "diagnostic_cases": module.EXPECTED_DIAGNOSTIC_CASES,
                    "unsupported_diagnostics": module.EXPECTED_UNSUPPORTED_DIAGNOSTICS,
                    "runtime_error_diagnostics": (
                        module.EXPECTED_RUNTIME_ERROR_DIAGNOSTICS
                    ),
                    "invalid_shape_diagnostics": (
                        module.EXPECTED_INVALID_SHAPE_DIAGNOSTICS
                    ),
                    "property_lanes": module.EXPECTED_PROPERTY_LANE_COUNT,
                    "deterministic_fuzz_cases": (
                        module.EXPECTED_DETERMINISTIC_FUZZ_CASES
                    ),
                    "admitted_stage_count_min": module.EXPECTED_ADMITTED_STAGE_COUNT_MIN,
                    "admitted_validator_cases": module.EXPECTED_ADMITTED_VALIDATOR_CASES,
                    "admitted_required_runtime_rows": (
                        module.EXPECTED_ADMITTED_REQUIRED_RUNTIME_ROWS
                    ),
                    "admitted_support_report_rows": (
                        module.EXPECTED_ADMITTED_SUPPORT_REPORT_ROWS
                    ),
                    "admitted_deterministic_unsupported_rows": (
                        module.EXPECTED_DETERMINISTIC_UNSUPPORTED_ROWS
                    ),
                },
                "front_door_example_scenario_ids": sorted(
                    module.EXPECTED_EXAMPLE_SCENARIOS
                ),
                "front_door_expected_error_scenario_ids": sorted(
                    module.EXPECTED_ERROR_SCENARIOS
                ),
                "golden_workflow_ids": sorted(module.EXPECTED_GOLDEN_WORKFLOWS),
                "required_semantic_case_ids": sorted(module.REQUIRED_SEMANTIC_CASE_IDS),
                "required_unsupported_case_ids": sorted(
                    module.REQUIRED_UNSUPPORTED_CASE_IDS
                ),
                "residual_gap_dispositions": [
                    {
                        "gap_id": "broad_ansi_subquery_parity_beyond_admitted_v1_scope",
                        "v1_closeout_status": "outside_declared_v1_scope",
                        "reason": "fixture",
                    },
                    {
                        "gap_id": "external_oracle_result_artifact_population",
                        "v1_closeout_status": (
                            "not_required_for_current_v1_correctness_claim"
                        ),
                        "reason": "fixture",
                    },
                    {
                        "gap_id": (
                            "general_fuzz_beyond_deterministic_v1_property_fuzz_lanes"
                        ),
                        "v1_closeout_status": (
                            "not_required_for_current_v1_correctness_claim"
                        ),
                        "reason": "fixture",
                    },
                ],
            },
        )

        unsupported_cases = sorted(
            case
            for case in module.REQUIRED_UNSUPPORTED_CASE_IDS
            if case.startswith("unsupported_")
        )
        runtime_error_cases = sorted(
            case
            for case in module.REQUIRED_UNSUPPORTED_CASE_IDS
            if case.startswith("runtime_error_")
        )
        invalid_shape_cases = sorted(
            case
            for case in module.REQUIRED_UNSUPPORTED_CASE_IDS
            if case.startswith("invalid_shape_")
        )
        semantic_stage_rows = [
            {
                "case_id": case_id,
                "kind": "sql_native_decoded_reference",
                "status": "passed",
                "artifact_ref": (
                    "target/admitted-semantics-matrix/artifacts/"
                    + f"{case_id}.json"
                ),
                "decoded_reference_digest": f"sha256:{index + 1:064x}",
                "expected_output_digest": f"sha256:{index + 1:064x}",
                "expected_output_digest_source": "canonical_decoded_reference_rows",
                "observed_output_digest": f"sha256:{index + 1:064x}",
                "observed_output_digest_source": "complete_native_result_rows",
                "fallback_attempted": False,
                "external_engine_invoked": False,
                "blockers": [],
            }
            for index, case_id in enumerate(sorted(module.REQUIRED_SEMANTIC_CASE_IDS))
        ]

        def diagnostic_kind(case_id: str) -> str:
            if case_id.startswith("runtime_error_"):
                return "runtime_error_diagnostic"
            if case_id.startswith("invalid_shape_"):
                return "invalid_shape_diagnostic"
            return "unsupported_diagnostic"

        unsupported_stage_rows = [
            {
                "case_id": case_id,
                "status": "passed",
                "artifact_ref": (
                    "target/admitted-semantics-matrix/artifacts/"
                    + f"{case_id}.json"
                ),
                "kind": diagnostic_kind(case_id),
                "diagnostic_code": "SL_INVALID_INPUT",
                "diagnostic_fragment": f"fixture diagnostic fragment for {case_id}",
                "fallback_attempted": False,
                "external_engine_invoked": False,
                "blockers": [],
            }
            for case_id in sorted(module.REQUIRED_UNSUPPORTED_CASE_IDS)
        ]
        required_operation_methods = sorted(
            {
                method
                for row in module.REQUIRED_OPERATION_COVERAGE_ROWS.values()
                for method in row["python_methods"]
            }
        )
        method_rows = [
            {
                "method": method,
                "support_status": "fixture_smoke_supported",
                "claim_gate_status": "not_claim_grade",
                "fallback_attempted": False,
                "external_engine_invoked": False,
            }
            for method in required_operation_methods
        ]
        method_rows.extend(
            {
                "method": f"fixture_extra_{index}",
                "support_status": "fixture_auxiliary_supported",
                "claim_gate_status": "not_claim_grade",
                "fallback_attempted": False,
                "external_engine_invoked": False,
            }
            for index in range(
                module.EXPECTED_PYTHON_USER_SURFACE_METHOD_ROWS - len(method_rows)
            )
        )
        write(
            paths.golden_workflow,
            {
                "schema_version": "shardloom.golden_workflow_validation_report.v1",
                "status": "passed",
                "blockers": [],
                "workflow_count": len(module.EXPECTED_GOLDEN_WORKFLOWS),
                "stage_count": module.EXPECTED_GOLDEN_STAGE_COUNT_MIN,
                "workflow_ids": sorted(module.EXPECTED_GOLDEN_WORKFLOWS),
                "support_matrix_status": "passed",
                **false_fields,
            },
        )
        write(
            paths.admitted_semantics,
            {
                "schema_version": "shardloom.admitted_semantics_matrix_report.v1",
                "status": "passed",
                "blockers": [],
                "executable_fixture_count": module.EXPECTED_EXECUTABLE_FIXTURES,
                "diagnostic_case_count": module.EXPECTED_DIAGNOSTIC_CASES,
                "unsupported_diagnostic_count": module.EXPECTED_UNSUPPORTED_DIAGNOSTICS,
                "runtime_error_diagnostic_count": (
                    module.EXPECTED_RUNTIME_ERROR_DIAGNOSTICS
                ),
                "invalid_shape_diagnostic_count": (
                    module.EXPECTED_INVALID_SHAPE_DIAGNOSTICS
                ),
                "stage_count": module.EXPECTED_ADMITTED_STAGE_COUNT_MIN,
                "remaining_matrix_gap_status": "passed",
                "v1_runtime_scope_status": "passed",
                "v1_expected_validator_case_count": (
                    module.EXPECTED_ADMITTED_VALIDATOR_CASES
                ),
                "v1_required_runtime_row_count": (
                    module.EXPECTED_ADMITTED_REQUIRED_RUNTIME_ROWS
                ),
                "v1_missing_validator_case_count": 0,
                "v1_unexpected_required_runtime_row_count": 0,
                "v1_support_report_row_count": (
                    module.EXPECTED_ADMITTED_SUPPORT_REPORT_ROWS
                ),
                "deterministic_unsupported_scope_status": "passed",
                "deterministic_unsupported_row_count": (
                    module.EXPECTED_DETERMINISTIC_UNSUPPORTED_ROWS
                ),
                "deterministic_unsupported_oracle_row_count": (
                    module.EXPECTED_DETERMINISTIC_UNSUPPORTED_ROWS
                ),
                "property_execution_performed": True,
                "property_lane_count": module.EXPECTED_PROPERTY_LANE_COUNT,
                "property_case_ids": sorted(module.REQUIRED_PROPERTY_CASE_IDS),
                "deterministic_fuzz_execution_performed": True,
                "deterministic_fuzz_case_count": (
                    module.EXPECTED_DETERMINISTIC_FUZZ_CASES
                ),
                "fuzz_case_ids": sorted(module.REQUIRED_FUZZ_CASE_IDS),
                "decoded_reference_differential_execution_performed": True,
                "semantic_conformance_suite_status": "passed",
                "correctness_harness_boundary_status": "passed",
                "executable_case_ids": sorted(module.REQUIRED_SEMANTIC_CASE_IDS),
                "unsupported_case_ids": unsupported_cases,
                "runtime_error_case_ids": runtime_error_cases,
                "invalid_shape_case_ids": invalid_shape_cases,
                "remaining_matrix_gaps": [],
                "stages": semantic_stage_rows + unsupported_stage_rows,
                **false_fields,
            },
        )
        write(
            paths.front_door,
            {
                "schema_version": "shardloom.v1_front_door_runtime_scope_report.v1",
                "status": "passed",
                "blockers": [],
                "scoped_local_front_door_parity_supported": True,
                "supported_parity_row_ids": [
                    f"supported-{index}"
                    for index in range(module.EXPECTED_FRONT_DOOR_SUPPORTED_ROWS)
                ],
                "broad_pending_parity_row_ids": [
                    f"pending-{index}"
                    for index in range(module.EXPECTED_FRONT_DOOR_PENDING_ROWS)
                ],
                "example_scenario_ids": sorted(module.EXPECTED_EXAMPLE_SCENARIOS),
                "expected_error_scenario_ids": sorted(module.EXPECTED_ERROR_SCENARIOS),
                "all_no_fallback_no_external_engine": True,
                "performance_equivalence_claim_allowed": False,
                **false_fields,
            },
        )
        write(
            paths.vortex_runtime,
            {
                "schema_version": "shardloom.v1_vortex_runtime_scope_report.v1",
                "status": "passed",
                "blockers": [],
                "evidence_class": "declarative_specification",
                "supported_primitive_route_ids": [
                    f"primitive-{index}"
                    for index in range(module.EXPECTED_VORTEX_PRIMITIVE_ROUTES)
                ],
                "runtime_execution_performed": False,
                "performance_evidence_produced": False,
                "user_route_ids": sorted(module.EXPECTED_VORTEX_USER_ROUTE_IDS),
                "user_route_rows": [
                    {
                        "route_id": "native_vortex_query",
                        "owner": "shared_native_workflow",
                        "route_runtime_status": "global_runtime_supported",
                        "fallback_attempted": False,
                        "external_engine_invoked": False,
                    },
                    {
                        "route_id": "object_store_lakehouse_runtime",
                        "owner": "GAR-RUNTIME-IMPL-6D:last_order.object_store_lakehouse_catalog",
                        "route_runtime_status": "external_environment_gate_pending",
                        "fallback_attempted": False,
                        "external_engine_invoked": False,
                    },
                ],
                "local_vortex_primitive_v1_scope_ready": True,
                "user_route_v1_vortex_scope_ready": True,
                "all_no_fallback_no_external_engine": True,
                "local_vortex_primitive_all_runtime_supported": True,
                "local_vortex_primitive_all_no_fallback_no_external_engine": True,
                "claim_gate_status": "not_claim_grade",
                "performance_claim_allowed": False,
                "production_claim_allowed": False,
                "spark_replacement_claim_allowed": False,
                **false_fields,
            },
        )
        write(
            paths.python_user_surface,
            {
                "schema_version": "shardloom.python_user_surface_completion_gate.v1",
                "status": "passed",
                "blockers": [],
                "scoped_python_front_door_claim_allowed": True,
                "method_matrix_row_count": (
                    module.EXPECTED_PYTHON_USER_SURFACE_METHOD_ROWS
                ),
                "method_matrix_rows": method_rows,
                "claim_gate_status": "not_claim_grade",
                **false_fields,
            },
        )
        write(
            paths.example_replay,
            {
                "schema_version": "shardloom.v1_example_replay_report.v1",
                "status": "passed",
                "blockers": [],
                "docs_marker_source_count": (
                    module.EXPECTED_EXAMPLE_REPLAY_DOC_SOURCES
                ),
                "docs_marker_count": 36,
                "docs_marker_pass_count": 36,
                "docs_marker_status": "passed",
                "runtime_command_count": (
                    module.EXPECTED_EXAMPLE_REPLAY_RUNTIME_COMMANDS
                ),
                "runtime_command_status": "passed",
                "golden_workflow_replay_status": "passed",
                "golden_workflow_replay_verified_count": len(
                    module.EXPECTED_GOLDEN_WORKFLOWS
                ),
                "golden_workflow_stage_count": module.EXPECTED_GOLDEN_STAGE_COUNT_MIN,
                "docs_example_execution_status": "passed",
                "python_readme_example_execution_status": "passed",
                "website_example_execution_status": "passed",
                "quickstart_smoke_status": "passed",
                "benchmark_scenario_execution_status": "passed",
                "timing_review_status": "passed",
                "benchmark_scenario_count": module.EXPECTED_EXAMPLE_REPLAY_SCENARIOS,
                "benchmark_expected_error_scenario_count": (
                    module.EXPECTED_EXAMPLE_REPLAY_ERROR_SCENARIOS
                ),
                "expected_error_scenario_ids": sorted(module.EXPECTED_ERROR_SCENARIOS),
                "unsupported_failure_fixture_count": (
                    module.EXPECTED_EXAMPLE_REPLAY_UNSUPPORTED_FIXTURES
                ),
                "unsupported_failure_fixture_status": "passed",
                "all_no_fallback_no_external_engine": True,
                "claim_gate_status": "not_claim_grade",
                "runtime_support_claim_allowed": False,
                "correctness_claim_allowed": True,
                **false_fields,
            },
        )
        write(
            paths.source_prepared_state,
            {
                "schema_version": "shardloom.v1_source_prepared_state_scope_report.v1",
                "status": "passed",
                "blockers": [],
                "supported_input_formats": sorted(module.EXPECTED_SOURCE_FORMAT_IDS),
                "route_ids": sorted(module.EXPECTED_SOURCE_ROUTE_IDS),
                "invalidation_case_ids": sorted(
                    module.REQUIRED_SOURCE_INVALIDATION_CASE_IDS
                ),
                "report_id": "prod-v1-1c.source_prepared_state_scope",
                "evidence_class": "declarative_specification",
                "canonical_route": module.EXPECTED_SOURCE_CANONICAL_ROUTE,
                "state_owner": module.EXPECTED_SOURCE_STATE_OWNER,
                "reuse_scope": module.EXPECTED_SOURCE_REUSE_SCOPE,
                "reuse_policy": module.EXPECTED_SOURCE_REUSE_POLICY,
                "query_answers_cached": False,
                "runtime_execution_performed": False,
                "performance_evidence_produced": False,
                "v1_scope_ready": True,
                "claim_gate_status": "not_claim_grade",
                "golden_fixture_paths": [
                    "docs/architecture/fixtures/v1-source-prepared-state/source-state-golden.json",
                    "docs/architecture/fixtures/v1-source-prepared-state/vortex-prepared-state-golden.json",
                    "docs/architecture/fixtures/v1-source-prepared-state/reuse-invalidation-matrix.json",
                ],
                "required_runtime_fields": [
                    "source_state_id",
                    "source_state_digest",
                    "prepared_state_id",
                    "prepared_state_digest",
                ],
                "unsupported_boundary_ids": ["global_hidden_cache"],
                "all_no_fallback_no_external_engine": True,
                **false_fields,
            },
        )
        write(
            paths.local_output_sink,
            {
                "schema_version": "shardloom.v1_local_output_sink_scope_report.v1",
                "status": "passed",
                "blockers": [],
                "supported_output_formats": [
                    f"format-{index}" for index in range(module.EXPECTED_OUTPUT_FORMATS)
                ],
                "user_write_methods": [
                    f"method-{index}"
                    for index in range(module.EXPECTED_OUTPUT_WRITE_METHODS)
                ],
                "output_route_ids": ["native_vortex_query"],
                "evidence_class": "declarative_contract",
                "declarative_contract_ready": True,
                "runtime_evidence_verified": False,
                **false_fields,
            },
        )

    def test_v1_correctness_conformance_gate_passes_complete_fixture(self) -> None:
        module = self._load_script_module(
            "check_v1_correctness_conformance.py",
            "check_v1_correctness_conformance_for_test",
        )

        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            self._write_v1_correctness_conformance_fixture_reports(module, repo_root)
            report = module.build_report(repo_root, module.ReportPaths())

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(report["input_report_count"], 8)
        self.assertTrue(report["correctness_claim_allowed"])
        self.assertTrue(report["decoded_reference_differential_execution_performed"])
        self.assertTrue(report["property_execution_performed"])
        self.assertTrue(report["deterministic_fuzz_execution_performed"])
        self.assertFalse(report["fallback_attempted"])
        self.assertFalse(report["external_engine_invoked"])
        self.assertEqual(report["docs_example_execution_status"], "passed")
        self.assertEqual(report["unsupported_path_test_status"], "passed")
        self.assertEqual(report["example_replay_validator_status"], "passed")
        self.assertEqual(
            report["summaries"]["admitted_semantics"]["required_semantic_case_count"],
            len(module.REQUIRED_SEMANTIC_CASE_IDS),
        )
        self.assertEqual(
            report["summaries"]["admitted_semantics"][
                "semantic_fixture_evidence_status"
            ],
            "passed",
        )
        self.assertEqual(
            report["summaries"]["admitted_semantics"][
                "required_stage_artifact_ref_count"
            ],
            (
                len(module.REQUIRED_SEMANTIC_CASE_IDS)
                + len(module.REQUIRED_UNSUPPORTED_CASE_IDS)
            ),
        )
        self.assertEqual(
            report["summaries"]["admitted_semantics"][
                "required_stage_decoded_reference_digest_count"
            ],
            len(module.REQUIRED_SEMANTIC_CASE_IDS),
        )
        self.assertEqual(
            report["summaries"]["admitted_semantics"][
                "required_stage_expected_output_digest_count"
            ],
            len(module.REQUIRED_SEMANTIC_CASE_IDS),
        )
        self.assertEqual(
            report["summaries"]["admitted_semantics"][
                "required_stage_observed_output_digest_count"
            ],
            len(module.REQUIRED_SEMANTIC_CASE_IDS),
        )
        self.assertEqual(
            report["summaries"]["admitted_semantics"][
                "required_stage_output_digest_match_count"
            ],
            len(module.REQUIRED_SEMANTIC_CASE_IDS),
        )
        self.assertEqual(
            report["summaries"]["admitted_semantics"][
                "required_unsupported_stage_diagnostic_field_count"
            ],
            len(module.REQUIRED_UNSUPPORTED_CASE_IDS),
        )
        self.assertEqual(
            report["summaries"]["admitted_semantics"]["v1_runtime_scope_status"],
            "passed",
        )
        self.assertEqual(
            report["summaries"]["admitted_semantics"][
                "v1_unexpected_required_runtime_row_count"
            ],
            0,
        )
        self.assertEqual(
            report["summaries"]["admitted_semantics"][
                "deterministic_unsupported_oracle_row_count"
            ],
            module.EXPECTED_DETERMINISTIC_UNSUPPORTED_ROWS,
        )
        self.assertEqual(
            report["summaries"]["admitted_semantics"][
                "deterministic_fuzz_case_count"
            ],
            module.EXPECTED_DETERMINISTIC_FUZZ_CASES,
        )
        self.assertEqual(
            set(report["summaries"]["admitted_semantics"]["property_case_ids"]),
            module.REQUIRED_PROPERTY_CASE_IDS,
        )
        self.assertEqual(
            set(report["summaries"]["source_prepared_state"]["invalidation_case_ids"]),
            module.REQUIRED_SOURCE_INVALIDATION_CASE_IDS,
        )
        self.assertEqual(
            report["summaries"]["python_user_surface"][
                "method_matrix_row_list_count"
            ],
            module.EXPECTED_PYTHON_USER_SURFACE_METHOD_ROWS,
        )
        self.assertEqual(
            report["summaries"]["python_user_surface"][
                "required_operation_method_count"
            ],
            module.REQUIRED_OPERATION_UNIQUE_PYTHON_METHOD_COUNT,
        )
        self.assertEqual(
            report["summaries"]["example_replay"]["docs_marker_source_count"],
            module.EXPECTED_EXAMPLE_REPLAY_DOC_SOURCES,
        )
        self.assertEqual(
            report["summaries"]["example_replay"]["runtime_command_count"],
            module.EXPECTED_EXAMPLE_REPLAY_RUNTIME_COMMANDS,
        )
        self.assertEqual(
            report["summaries"]["example_replay"]["benchmark_scenario_count"],
            module.EXPECTED_EXAMPLE_REPLAY_SCENARIOS,
        )
        self.assertTrue(
            report["summaries"]["example_replay"]["all_no_fallback_no_external_engine"]
        )
        self.assertEqual(
            report["summaries"]["operation_coverage"][
                "operation_coverage_row_count"
            ],
            module.REQUIRED_OPERATION_COVERAGE_ROW_COUNT,
        )
        self.assertEqual(
            report["summaries"]["operation_coverage"][
                "operation_coverage_python_method_link_count"
            ],
            module.REQUIRED_OPERATION_PYTHON_METHOD_LINK_COUNT,
        )
        self.assertTrue(
            all(row["status"] == "passed" for row in report["operation_coverage_rows"])
        )

    def test_v1_correctness_conformance_gate_fails_missing_semantic_case(self) -> None:
        module = self._load_script_module(
            "check_v1_correctness_conformance.py",
            "check_v1_correctness_conformance_missing_case_for_test",
        )

        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            self._write_v1_correctness_conformance_fixture_reports(module, repo_root)
            admitted_path = repo_root / module.ReportPaths().admitted_semantics
            admitted = json.loads(admitted_path.read_text(encoding="utf-8"))
            admitted["executable_case_ids"].remove("decimal_arithmetic_projection")
            admitted_path.write_text(json.dumps(admitted), encoding="utf-8")
            report = module.build_report(repo_root, module.ReportPaths())

        self.assertEqual(report["status"], "failed")
        self.assertFalse(report["correctness_claim_allowed"])
        self.assertTrue(
            any(
                "missing required executable cases decimal_arithmetic_projection" in blocker
                for blocker in report["blockers"]
            ),
            report["blockers"],
        )

    def test_v1_correctness_conformance_gate_fails_missing_stage_digest(self) -> None:
        module = self._load_script_module(
            "check_v1_correctness_conformance.py",
            "check_v1_correctness_conformance_missing_stage_digest_for_test",
        )

        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            self._write_v1_correctness_conformance_fixture_reports(module, repo_root)
            admitted_path = repo_root / module.ReportPaths().admitted_semantics
            admitted = json.loads(admitted_path.read_text(encoding="utf-8"))
            for stage in admitted["stages"]:
                if stage["case_id"] == "decimal_arithmetic_projection":
                    stage["decoded_reference_digest"] = ""
                    break
            admitted_path.write_text(json.dumps(admitted), encoding="utf-8")
            report = module.build_report(repo_root, module.ReportPaths())

        self.assertEqual(report["status"], "failed")
        self.assertEqual(
            report["summaries"]["admitted_semantics"][
                "semantic_fixture_evidence_status"
            ],
            "failed",
        )
        self.assertTrue(
            any(
                "decimal_arithmetic_projection decoded_reference_digest must be sha256-prefixed"
                in blocker
                for blocker in report["blockers"]
            ),
            report["blockers"],
        )

    def test_v1_correctness_conformance_gate_fails_missing_fuzz_case(self) -> None:
        module = self._load_script_module(
            "check_v1_correctness_conformance.py",
            "check_v1_correctness_conformance_missing_fuzz_case_for_test",
        )

        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            self._write_v1_correctness_conformance_fixture_reports(module, repo_root)
            admitted_path = repo_root / module.ReportPaths().admitted_semantics
            admitted = json.loads(admitted_path.read_text(encoding="utf-8"))
            admitted["fuzz_case_ids"].remove("route_selection_join_fuzz_seed_20260615")
            admitted_path.write_text(json.dumps(admitted), encoding="utf-8")
            report = module.build_report(repo_root, module.ReportPaths())

        self.assertEqual(report["status"], "failed")
        self.assertTrue(
            any(
                "missing deterministic fuzz cases route_selection_join_fuzz_seed_20260615"
                in blocker
                for blocker in report["blockers"]
            ),
            report["blockers"],
        )

    def test_v1_correctness_conformance_gate_fails_missing_property_case(self) -> None:
        module = self._load_script_module(
            "check_v1_correctness_conformance.py",
            "check_v1_correctness_conformance_missing_property_case_for_test",
        )

        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            self._write_v1_correctness_conformance_fixture_reports(module, repo_root)
            admitted_path = repo_root / module.ReportPaths().admitted_semantics
            admitted = json.loads(admitted_path.read_text(encoding="utf-8"))
            admitted["property_case_ids"].remove(
                "filter_project_limit_property_seed_20260618"
            )
            admitted_path.write_text(json.dumps(admitted), encoding="utf-8")
            report = module.build_report(repo_root, module.ReportPaths())

        self.assertEqual(report["status"], "failed")
        self.assertTrue(
            any(
                "missing deterministic property cases "
                "filter_project_limit_property_seed_20260618" in blocker
                for blocker in report["blockers"]
            ),
            report["blockers"],
        )

    def test_v1_correctness_conformance_gate_fails_missing_stage_artifact(
        self,
    ) -> None:
        module = self._load_script_module(
            "check_v1_correctness_conformance.py",
            "check_v1_correctness_conformance_missing_stage_artifact_for_test",
        )

        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            self._write_v1_correctness_conformance_fixture_reports(module, repo_root)
            admitted_path = repo_root / module.ReportPaths().admitted_semantics
            admitted = json.loads(admitted_path.read_text(encoding="utf-8"))
            for stage in admitted["stages"]:
                if stage["case_id"] == "exists_subquery_semantics":
                    stage["artifact_ref"] = ""
                    break
            admitted_path.write_text(json.dumps(admitted), encoding="utf-8")
            report = module.build_report(repo_root, module.ReportPaths())

        self.assertEqual(report["status"], "failed")
        self.assertTrue(
            any(
                "exists_subquery_semantics missing artifact_ref" in blocker
                for blocker in report["blockers"]
            ),
            report["blockers"],
        )

    def test_v1_correctness_conformance_gate_fails_output_digest_mismatch(
        self,
    ) -> None:
        module = self._load_script_module(
            "check_v1_correctness_conformance.py",
            "check_v1_correctness_conformance_output_digest_mismatch_for_test",
        )

        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            self._write_v1_correctness_conformance_fixture_reports(module, repo_root)
            admitted_path = repo_root / module.ReportPaths().admitted_semantics
            admitted = json.loads(admitted_path.read_text(encoding="utf-8"))
            for stage in admitted["stages"]:
                if stage["case_id"] == "select_distinct_projection":
                    stage["observed_output_digest"] = "sha256:" + ("f" * 64)
                    break
            admitted_path.write_text(json.dumps(admitted), encoding="utf-8")
            report = module.build_report(repo_root, module.ReportPaths())

        self.assertEqual(report["status"], "failed")
        self.assertTrue(
            any(
                "select_distinct_projection expected/observed output digests must match"
                in blocker
                for blocker in report["blockers"]
            ),
            report["blockers"],
        )

    def test_v1_correctness_conformance_gate_fails_missing_diagnostic_field(
        self,
    ) -> None:
        module = self._load_script_module(
            "check_v1_correctness_conformance.py",
            "check_v1_correctness_conformance_missing_diagnostic_field_for_test",
        )

        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            self._write_v1_correctness_conformance_fixture_reports(module, repo_root)
            admitted_path = repo_root / module.ReportPaths().admitted_semantics
            admitted = json.loads(admitted_path.read_text(encoding="utf-8"))
            for stage in admitted["stages"]:
                if stage["case_id"] == "unsupported_variant_access":
                    stage["diagnostic_code"] = ""
                    break
            admitted_path.write_text(json.dumps(admitted), encoding="utf-8")
            report = module.build_report(repo_root, module.ReportPaths())

        self.assertEqual(report["status"], "failed")
        self.assertTrue(
            any(
                "unsupported_variant_access diagnostic_code is required" in blocker
                for blocker in report["blockers"]
            ),
            report["blockers"],
        )

    def test_v1_correctness_conformance_gate_fails_stage_fallback_marker(
        self,
    ) -> None:
        module = self._load_script_module(
            "check_v1_correctness_conformance.py",
            "check_v1_correctness_conformance_stage_fallback_for_test",
        )

        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            self._write_v1_correctness_conformance_fixture_reports(module, repo_root)
            admitted_path = repo_root / module.ReportPaths().admitted_semantics
            admitted = json.loads(admitted_path.read_text(encoding="utf-8"))
            for stage in admitted["stages"]:
                if stage["case_id"] == "runtime_error_numeric_division_by_zero":
                    stage["fallback_attempted"] = True
                    break
            admitted_path.write_text(json.dumps(admitted), encoding="utf-8")
            report = module.build_report(repo_root, module.ReportPaths())

        self.assertEqual(report["status"], "failed")
        self.assertTrue(
            any(
                "runtime_error_numeric_division_by_zero fallback_attempted must be false"
                in blocker
                for blocker in report["blockers"]
            ),
            report["blockers"],
        )

    def test_v1_correctness_conformance_gate_fails_missing_python_accessor(
        self,
    ) -> None:
        module = self._load_script_module(
            "check_v1_correctness_conformance.py",
            "check_v1_correctness_conformance_missing_python_method_for_test",
        )

        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            self._write_v1_correctness_conformance_fixture_reports(module, repo_root)
            python_path = repo_root / module.ReportPaths().python_user_surface
            python_surface = json.loads(python_path.read_text(encoding="utf-8"))
            python_surface["method_matrix_rows"] = [
                row
                for row in python_surface["method_matrix_rows"]
                if row["method"] != "nlargest"
            ]
            python_surface["method_matrix_rows"].append(
                {
                    "method": "fixture_replacement_for_nlargest",
                    "support_status": "fixture_auxiliary_supported",
                    "claim_gate_status": "not_claim_grade",
                    "fallback_attempted": False,
                    "external_engine_invoked": False,
                }
            )
            python_path.write_text(json.dumps(python_surface), encoding="utf-8")
            report = module.build_report(repo_root, module.ReportPaths())

        self.assertEqual(report["status"], "failed")
        self.assertTrue(
            any(
                "python_user_surface: missing required operation methods nlargest"
                in blocker
                for blocker in report["blockers"]
            ),
            report["blockers"],
        )
        self.assertTrue(
            any(
                "operation_coverage: global_top_n: missing Python methods nlargest"
                in blocker
                for blocker in report["blockers"]
            ),
            report["blockers"],
        )

    def test_v1_correctness_conformance_gate_fails_missing_front_door_scenario(
        self,
    ) -> None:
        module = self._load_script_module(
            "check_v1_correctness_conformance.py",
            "check_v1_correctness_conformance_missing_front_door_scenario_for_test",
        )

        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            self._write_v1_correctness_conformance_fixture_reports(module, repo_root)
            front_door_path = repo_root / module.ReportPaths().front_door
            front_door = json.loads(front_door_path.read_text(encoding="utf-8"))
            front_door["example_scenario_ids"].remove("hash_join")
            front_door_path.write_text(json.dumps(front_door), encoding="utf-8")
            report = module.build_report(repo_root, module.ReportPaths())

        self.assertEqual(report["status"], "failed")
        self.assertTrue(
            any(
                "operation_coverage: hash_join: missing front-door example scenario"
                in blocker
                for blocker in report["blockers"]
            ),
            report["blockers"],
        )

    def test_v1_correctness_conformance_gate_fails_missing_example_replay_report(
        self,
    ) -> None:
        module = self._load_script_module(
            "check_v1_correctness_conformance.py",
            "check_v1_correctness_conformance_missing_example_replay_for_test",
        )

        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            self._write_v1_correctness_conformance_fixture_reports(module, repo_root)
            (repo_root / module.ReportPaths().example_replay).unlink()
            report = module.build_report(repo_root, module.ReportPaths())

        self.assertEqual(report["status"], "failed")
        self.assertFalse(report["correctness_claim_allowed"])
        self.assertEqual(report["docs_example_execution_status"], "blocked")
        self.assertTrue(
            any(
                "example_replay: missing report" in blocker
                for blocker in report["blockers"]
            ),
            report["blockers"],
        )

    def test_v1_correctness_conformance_gate_fails_example_replay_drift(
        self,
    ) -> None:
        module = self._load_script_module(
            "check_v1_correctness_conformance.py",
            "check_v1_correctness_conformance_example_replay_drift_for_test",
        )

        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            self._write_v1_correctness_conformance_fixture_reports(module, repo_root)
            example_path = repo_root / module.ReportPaths().example_replay
            example = json.loads(example_path.read_text(encoding="utf-8"))
            example["website_example_execution_status"] = "blocked"
            example["benchmark_scenario_count"] -= 1
            example_path.write_text(json.dumps(example), encoding="utf-8")
            report = module.build_report(repo_root, module.ReportPaths())

        self.assertEqual(report["status"], "failed")
        self.assertTrue(
            any(
                "example_replay: website_example_execution_status=blocked" in blocker
                for blocker in report["blockers"]
            ),
            report["blockers"],
        )
        self.assertTrue(
            any(
                "example_replay: benchmark_scenario_count=8" in blocker
                for blocker in report["blockers"]
            ),
            report["blockers"],
        )

    def test_v1_correctness_conformance_gate_fails_invalidation_case_drift(
        self,
    ) -> None:
        module = self._load_script_module(
            "check_v1_correctness_conformance.py",
            "check_v1_correctness_conformance_invalidation_drift_for_test",
        )

        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            self._write_v1_correctness_conformance_fixture_reports(module, repo_root)
            source_path = repo_root / module.ReportPaths().source_prepared_state
            source = json.loads(source_path.read_text(encoding="utf-8"))
            source["invalidation_case_ids"].remove("missing_artifact")
            source["invalidation_case_ids"].append("fixture_replacement_case")
            source_path.write_text(json.dumps(source), encoding="utf-8")
            report = module.build_report(repo_root, module.ReportPaths())

        self.assertEqual(report["status"], "failed")
        self.assertTrue(
            any(
                "source_prepared_state: invalidation_case_ids mismatch" in blocker
                for blocker in report["blockers"]
            ),
            report["blockers"],
        )

    def test_v1_correctness_conformance_gate_fails_closed_when_report_missing(self) -> None:
        module = self._load_script_module(
            "check_v1_correctness_conformance.py",
            "check_v1_correctness_conformance_missing_report_for_test",
        )

        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            self._write_v1_correctness_conformance_fixture_reports(module, repo_root)
            (repo_root / module.ReportPaths().admitted_semantics).unlink()
            report = module.build_report(repo_root, module.ReportPaths())

        self.assertEqual(report["status"], "failed")
        self.assertFalse(report["correctness_claim_allowed"])
        self.assertFalse(report["decoded_reference_differential_execution_performed"])
        self.assertFalse(report["property_execution_performed"])
        self.assertTrue(
            any(
                "admitted_semantics: missing report" in blocker
                for blocker in report["blockers"]
            ),
            report["blockers"],
        )

    def test_v1_correctness_conformance_gate_fails_matrix_drift(self) -> None:
        module = self._load_script_module(
            "check_v1_correctness_conformance.py",
            "check_v1_correctness_conformance_matrix_drift_for_test",
        )

        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            self._write_v1_correctness_conformance_fixture_reports(module, repo_root)
            matrix_path = repo_root / module.DEFAULT_MATRIX
            matrix = json.loads(matrix_path.read_text(encoding="utf-8"))
            matrix["required_semantic_case_ids"].remove("decimal_arithmetic_projection")
            matrix_path.write_text(json.dumps(matrix), encoding="utf-8")
            report = module.build_report(repo_root, module.ReportPaths())

        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["matrix_status"], "failed")
        self.assertTrue(
            any(
                "matrix required_semantic_case_ids mismatch" in blocker
                for blocker in report["blockers"]
            ),
            report["blockers"],
        )

    def test_v1_api_schema_stability_validator_passes_current_contracts(self) -> None:
        module = self._load_script_module(
            "check_v1_api_schema_stability.py",
            "check_v1_api_schema_stability_current_for_test",
        )

        report = module.build_report(
            REPO_ROOT,
            REPO_ROOT / "docs/release/v1-api-schema-stability-matrix.json",
        )

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(report["stable_surface_count"], 12)
        self.assertEqual(report["diagnostic_code_count"], 22)
        self.assertEqual(
            report["diagnostic_code_doc_ref"],
            "docs/release/diagnostic-code-stability.md",
        )
        self.assertIn("SL_NO_FALLBACK_EXECUTION", report["diagnostic_code_order"])
        self.assertIn("output_envelope", report["stable_surfaces"])
        self.assertFalse(report["public_release_claim_allowed"])
        self.assertFalse(report["public_package_claim_allowed"])
        self.assertFalse(report["fallback_attempted"])
        self.assertFalse(report["external_engine_invoked"])

    def test_v1_api_schema_stability_validator_fails_on_missing_stable_field(self) -> None:
        module = self._load_script_module(
            "check_v1_api_schema_stability.py",
            "check_v1_api_schema_stability_missing_field_for_test",
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fixture_path = root / "fixtures.json"
            matrix_path = root / "matrix.json"
            fixture = json.loads(
                (
                    REPO_ROOT
                    / "docs/release/fixtures/v1-api-schema-stability/golden-fixtures.json"
                ).read_text(encoding="utf-8")
            )
            del fixture["fixtures"]["output_envelope"]["command"]
            fixture_path.write_text(json.dumps(fixture), encoding="utf-8")
            matrix = json.loads(
                (REPO_ROOT / "docs/release/v1-api-schema-stability-matrix.json").read_text(
                    encoding="utf-8"
                )
            )
            matrix["fixture_path"] = str(fixture_path)
            matrix_path.write_text(json.dumps(matrix), encoding="utf-8")

            report = module.validate_matrix(REPO_ROOT, matrix_path)

        self.assertEqual(report["status"], "blocked")
        self.assertTrue(
            any(
                "output_envelope: fixture missing required field command" in blocker
                for blocker in report["blockers"]
            ),
            report["blockers"],
        )

    def test_v1_observability_support_validator_redacts_command_evidence(self) -> None:
        module = self._load_script_module(
            "check_v1_observability_support.py",
            "check_v1_observability_support_redaction_for_test",
        )

        command = [
            "shardloom",
            "support-bundle",
            "--note",
            "token=abc123 Authorization: Bearer secret-value",
            "--format",
            "json",
        ]

        text = module.command_text(command)
        argv = [module.redact_report_text(part) for part in command]

        self.assertIn("token=<redacted>", text)
        self.assertIn("Bearer <redacted>", text)
        self.assertNotIn("abc123", text)
        self.assertNotIn("secret-value", text)
        self.assertNotIn("abc123", " ".join(argv))
        self.assertNotIn("secret-value", " ".join(argv))

    def test_release_validation_evidence_records_security_posture_and_pip_audit_env(self) -> None:
        module = self._load_script_module(
            "run_release_validation_evidence.py",
            "run_release_validation_evidence_supporting_for_test",
        )

        commands = module.supporting_commands(
            "/tool/python3.12",
            Path("target/release-audit-venv/bin/python"),
        )
        by_name = {name: (command, group, env) for name, command, group, env in commands}

        dependency_command, dependency_group, dependency_env = by_name[
            "dependency_audit_release_gate"
        ]
        self.assertEqual(dependency_command[0], "/tool/python3.12")
        self.assertEqual(dependency_group, "security_dependency_provenance")
        self.assertEqual(
            dependency_env,
            {"SHARDLOOM_PIP_AUDIT_PYTHON": "target/release-audit-venv/bin/python"},
        )
        security_command, security_group, security_env = by_name["security_posture"]
        self.assertEqual(
            security_command,
            [
                "/tool/python3.12",
                "scripts/check_security_posture.py",
                "--json-output",
                "target/security-posture-report.json",
            ],
        )
        self.assertEqual(security_group, "security_dependency_provenance")
        self.assertEqual(security_env, {})

    def test_release_validation_evidence_skip_slow_plans_no_commands(self) -> None:
        module = self._load_script_module(
            "run_release_validation_evidence.py",
            "run_release_validation_evidence_skip_slow_for_test",
        )
        args = type(
            "Args",
            (),
            {
                "skip_slow": True,
                "pip_audit_python": Path("target/release-audit-venv/bin/python"),
                "require_clean_conda": False,
                "conda_executable": None,
            },
        )()

        planned, required = module.planned_release_validation_commands(
            "/tool/python3.12",
            args,
        )

        self.assertEqual(planned, [])
        self.assertTrue(required)
        self.assertTrue(
            any(command[0] == "cargo_test_workspace" for command in required),
            required,
        )

    def test_release_validation_evidence_skip_slow_status_is_not_passed(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            output = Path(tempdir) / "release-validation-evidence.json"
            result = subprocess.run(
                [
                    sys.executable,
                    str(REPO_ROOT / "scripts" / "run_release_validation_evidence.py"),
                    "--skip-slow",
                    "--output",
                    str(output),
                ],
                cwd=REPO_ROOT,
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            report = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "skipped_slow")
            self.assertEqual(report["feature_build_matrix_status"], "skipped_slow")
            self.assertEqual(report["required_validation_status"], "skipped_slow")
            self.assertEqual(
                report["supporting_security_dependency_status"],
                "skipped_slow",
            )
            self.assertEqual(report["command_results"], [])

    def test_v1_security_ci_hardening_blocks_missing_pip_audit_and_requires_matrix_lanes(self) -> None:
        module = self._load_script_module(
            "check_v1_security_ci_hardening.py",
            "check_v1_security_ci_hardening_for_test",
        )

        dependency_check = module.check_dependency_audit(
            {
                "schema_version": "shardloom.dependency_audit_report.v1",
                "cargo_deny_status": "passed",
                "cargo_audit_status": "passed",
                "pip_audit_status": "missing",
                "license_policy_status": "passed",
                "advisory_status": "failed",
                "fallback_dependency_absent": True,
            }
        )
        self.assertEqual(dependency_check["status"], "blocked")
        self.assertIn(
            "dependency audit pip_audit_status=missing",
            dependency_check["blockers"],
        )

        ci_check = module.check_ci_matrix(
            {
                "schema_version": "shardloom.ci_gate_matrix_report.v1",
                "status": "passed",
                "lanes": [{"lane_id": "rust_baseline"}],
                "publication_attempted": False,
                "tag_created": False,
                "secrets_required": False,
                "fallback_attempted": False,
                "external_engine_invoked": False,
            }
        )
        self.assertEqual(ci_check["status"], "blocked")
        self.assertTrue(
            any(
                "python_compatibility_matrix" in blocker
                and "rust_msrv_validation" in blocker
                for blocker in ci_check["blockers"]
            )
        )

    def test_security_posture_requires_sha_pinned_privileged_actions(self) -> None:
        module = self._load_script_module(
            "check_security_posture.py", "check_security_posture_pinning_for_test"
        )

        pinned = module.action_pin_check(
            "steps:\n"
            "  - uses: actions/checkout@df4cb1c069e1874edd31b4311f1884172cec0e10\n"
            "  - uses: github/codeql-action/analyze@8aad20d150bbac5944a9f9d289da16a4b0d87c1e\n"
        )
        mutable = module.action_pin_check("steps:\n  - uses: actions/checkout@v6\n")

        self.assertEqual(pinned["status"], "passed")
        self.assertEqual(pinned["pinned_ref_count"], 2)
        self.assertEqual(mutable["status"], "failed")
        self.assertEqual(mutable["mutable_refs"], ["actions/checkout@v6"])

    def test_security_posture_accepts_current_privileged_workflows(self) -> None:
        module = self._load_script_module(
            "check_security_posture.py", "check_security_posture_current_for_test"
        )

        report = module.build_report(REPO_ROOT)

        self.assertEqual(report["status"], "passed", report["checks"])
        self.assertEqual(report["checks"]["codeql_action_pinning"]["status"], "passed")
        self.assertEqual(report["checks"]["scorecard_action_pinning"]["status"], "passed")
        self.assertEqual(
            report["checks"]["pypi_trusted_publisher_action_pinning"]["status"],
            "passed",
        )
        self.assertEqual(
            report["checks"]["pypi_trusted_publisher_oidc_boundary"]["status"],
            "passed",
        )

    def test_security_posture_rejects_pypi_build_inside_oidc_publish_job(self) -> None:
        module = self._load_script_module(
            "check_security_posture.py", "check_security_posture_pypi_oidc_for_test"
        )

        check = module.pypi_trusted_publisher_boundary_check(
            "jobs:\n"
            "  publish:\n"
            "    permissions:\n"
            "      contents: read\n"
            "      id-token: write\n"
            "    environment: pypi\n"
            "    steps:\n"
            "      - run: python -m build python\n"
        )

        self.assertEqual(check["status"], "failed")
        self.assertIn("publish job must not build the package", check["missing"])

    def test_security_posture_rejects_pypi_without_testpypi_proof_guard(self) -> None:
        module = self._load_script_module(
            "check_security_posture.py",
            "check_security_posture_pypi_prior_proof_for_test",
        )
        workflow = (
            REPO_ROOT
            / ".github"
            / "workflows"
            / "pypi-publish-draft.yml"
        ).read_text(encoding="utf-8")
        workflow = workflow.replace(" && inputs.testpypi_proof_ref != ''", "")

        check = module.pypi_trusted_publisher_boundary_check(workflow)

        self.assertEqual(check["status"], "failed")
        self.assertIn(
            "publish job must require prior TestPyPI proof ref",
            check["missing"],
        )
        current_workflow = (
            REPO_ROOT / ".github" / "workflows" / "pypi-publish-draft.yml"
        ).read_text(encoding="utf-8")
        self.assertIn("validate-pypi-prior-proof", current_workflow)
        self.assertIn("Validate prior TestPyPI proof transcript", current_workflow)
        self.assertIn('"proof_status": "passed"', current_workflow)
        self.assertIn('"cli_binary_required_for_clean_registry_smoke": True', current_workflow)
        self.assertIn('"cli_binary_available": True', current_workflow)

    def test_pypi_workflow_uses_dynamic_python_package_version_for_prior_proof(self) -> None:
        workflow = (
            REPO_ROOT / ".github" / "workflows" / "pypi-publish-draft.yml"
        ).read_text(encoding="utf-8")

        self.assertIn("python scripts/sync_workspace_package_versions.py --check", workflow)
        self.assertIn("python scripts/check_workspace_version_sources.py", workflow)
        self.assertIn("stage_python_package_with_bundled_cli", workflow)
        self.assertIn("build_python_artifacts(repo_root, stage_dir, dist_dir)", workflow)
        self.assertIn("artifact_suffix: linux", workflow)
        self.assertIn("artifact_suffix: macos", workflow)
        self.assertIn("artifact_suffix: windows", workflow)
        self.assertIn("python-dist-${{ matrix.artifact_suffix }}", workflow)
        self.assertIn("python-dist-sdist", workflow)
        self.assertIn("Build clean Python sdist", workflow)
        self.assertIn("target/pypi-python-package-sdist/dist/*.tar.gz", workflow)
        self.assertIn("pattern: python-dist-*", workflow)
        self.assertIn("merge-multiple: true", workflow)
        self.assertIn("bundled CLI wheel must not be universal", workflow)
        self.assertNotIn("python -m build python", workflow)
        self.assertIn("from release_report_utils import workspace_package_version", workflow)
        self.assertIn("expected_version = resolve_python_package_version()", workflow)
        self.assertNotIn('pyproject["project"]["version"]', workflow)
        self.assertEqual(CURRENT_PYTHON_PACKAGE_VERSION, CURRENT_WORKSPACE_PACKAGE_VERSION)

    def test_v1_local_source_package_release_track_gate_passes_current_contract(self) -> None:
        module = self._load_script_module(
            "check_v1_local_source_package_release.py",
            "check_v1_local_source_package_release_for_test",
        )

        report = module.build_report(REPO_ROOT)

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(
            report["release_track_status"],
            "local_source_package_v1_selected_channels_published",
        )
        self.assertEqual(
            report["selected_publication_channels"],
            SELECTED_V0_1_0_RELEASE_CHANNEL_IDS,
        )
        self.assertFalse(report["publication_attempted"])
        self.assertFalse(report["tag_created"])
        self.assertFalse(report["package_upload_attempted"])
        self.assertFalse(report["fallback_attempted"])
        self.assertFalse(report["external_engine_invoked"])

    def test_v1_local_source_package_release_rejects_public_package_claim_drift(self) -> None:
        module = self._load_script_module(
            "check_v1_local_source_package_release.py",
            "check_v1_local_source_package_release_claim_drift_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            temp_root = Path(tempdir)
            contract_path = temp_root / "v1-local-source-package-release.json"
            contract = json.loads(
                (REPO_ROOT / "docs/release/v1-local-source-package-release.json").read_text(
                    encoding="utf-8"
                )
            )
            contract["public_package_release_claim_allowed"] = False
            contract_path.write_text(json.dumps(contract), encoding="utf-8")

            blockers = module.validate_contract(json.loads(contract_path.read_text(encoding="utf-8")))

        self.assertIn(
            "contract public_package_release_claim_allowed must be True",
            blockers,
        )

    def test_release_readiness_accepts_configured_dry_run_command_evidence(self) -> None:
        module = self._load_script_module(
            "check_release_readiness.py",
            "check_release_readiness_validation_command_for_test",
        )
        expected = "python scripts/release_dry_run_proof.py --rows 64 --iterations 1"

        self.assertTrue(
            module.validation_command_passed(
                {
                    expected
                    + " --require-clean-conda --conda-executable /opt/homebrew/bin/micromamba": "passed"
                },
                expected,
            )
        )
        self.assertFalse(
            module.validation_command_passed(
                {expected + " --require-clean-conda": "failed"},
                expected,
            )
        )
        self.assertFalse(
            module.validation_command_passed(
                {expected + "0": "passed"},
                expected,
            )
        )
    def test_pre_5j_dependency_freshness_accepts_current_dependabot_prs(self) -> None:
        module = self._load_script_module(
            "check_pre_5j_dependency_freshness.py",
            "check_pre_5j_dependency_freshness_for_test",
        )
        report = module.build_report(
            repo_root=REPO_ROOT,
            open_prs=[
                self._dependabot_pr(1149, "Bump actions/download-artifact from 7 to 8"),
                self._dependabot_pr(
                    1223,
                    "Bump vortex from 0.74.0 to 0.75.0 in the vortex-upstream group",
                ),
                self._dependabot_pr(1151, "Bump serde_json from 1.0.149 to 1.0.150"),
                self._dependabot_pr(1152, "Bump sha2 from 0.10.9 to 0.11.0"),
                self._dependabot_pr(1153, "Bump rusqlite from 0.40.0 to 0.40.1"),
                self._dependabot_pr(1392, "Bump regex from 1.12.4 to 1.13.1"),
            ],
            open_prs_status="loaded_from_file",
            require_live_github=True,
        )

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(
            report["admitted_open_dependabot_prs"],
            [1149, 1151, 1152, 1153, 1223, 1392],
        )
        self.assertTrue(report["benchmark_refresh_allowed"])
        self.assertFalse(report["benchmark_run_performed"])
        self.assertFalse(report["fallback_attempted"])
        self.assertFalse(report["external_engine_invoked"])

    def test_pre_5j_dependency_freshness_uses_github_token_header(self) -> None:
        module = self._load_script_module(
            "check_pre_5j_dependency_freshness.py",
            "check_pre_5j_dependency_freshness_auth_for_test",
        )

        token = module.github_token_from_env(
            {"GITHUB_TOKEN": "ghs_token", "GH_TOKEN": "gh_token"}
        )
        headers = module.github_request_headers(token)

        self.assertEqual(token, "ghs_token")
        self.assertEqual(headers["Authorization"], "Bearer ghs_token")
        self.assertEqual(headers["Accept"], "application/vnd.github+json")
        self.assertNotIn("Authorization", module.github_request_headers(None))
        self.assertIsNone(module.validate_live_github_pulls_url(module.GITHUB_PULLS_URL))

    def test_pre_5j_dependency_freshness_rejects_unadmitted_live_github_url(
        self,
    ) -> None:
        module = self._load_script_module(
            "check_pre_5j_dependency_freshness.py",
            "check_pre_5j_dependency_freshness_live_url_policy_for_test",
        )

        def fail_if_called(*_args: object, **_kwargs: object) -> object:
            raise AssertionError("unsafe live GitHub URL reached urlopen")

        original_urlopen = module.urllib.request.urlopen
        module.urllib.request.urlopen = fail_if_called
        try:
            with self._temporary_env(GITHUB_TOKEN="ghs_secret"):
                open_prs, status, error = module.load_open_prs(
                    repo_root=REPO_ROOT,
                    open_prs_json=None,
                    require_live_github=True,
                    github_url="https://attacker.example/repos/depsilon/shardloom/pulls",
                    timeout_seconds=0.01,
                    github_token_env=None,
                )
        finally:
            module.urllib.request.urlopen = original_urlopen

        self.assertIsNone(open_prs)
        self.assertEqual(status, "failed")
        self.assertIsNotNone(error)
        self.assertIn("refusing live GitHub dependency check URL host", error)

    def test_pre_5j_dependency_freshness_rejects_github_url_userinfo(
        self,
    ) -> None:
        module = self._load_script_module(
            "check_pre_5j_dependency_freshness.py",
            "check_pre_5j_dependency_freshness_userinfo_policy_for_test",
        )

        error = module.validate_live_github_pulls_url(
            "https://token@api.github.com/repos/depsilon/shardloom/pulls"
        )

        self.assertEqual(
            error,
            "live GitHub dependency check URL must not include userinfo",
        )

    def test_pre_5j_dependency_freshness_parses_cargo_files_without_tomllib(self) -> None:
        module = self._load_script_module(
            "check_pre_5j_dependency_freshness.py",
            "check_pre_5j_dependency_freshness_no_tomllib_for_test",
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "Cargo.toml").write_text(
                "[workspace.dependencies]\n"
                f'vortex = "{CURRENT_VORTEX_MANIFEST_VERSION}"\n',
                encoding="utf-8",
            )
            cli_manifest = root / "shardloom-cli" / "Cargo.toml"
            cli_manifest.parent.mkdir(parents=True)
            cli_manifest.write_text(
                "[dependencies]\n"
                'rusqlite = { version = "0.40.2", default-features = false, features = ["bundled"] }\n',
                encoding="utf-8",
            )
            vortex_manifest = root / "shardloom-vortex" / "Cargo.toml"
            vortex_manifest.parent.mkdir(parents=True)
            vortex_manifest.write_text(
                "[dependencies]\n"
                "vortex = { workspace = true, optional = true }\n",
                encoding="utf-8",
            )
            (root / "Cargo.lock").write_text(
                "[[package]]\n"
                'name = "vortex"\n'
                f'version = "{CURRENT_VORTEX_LOCK_VERSION}"\n'
                "\n"
                "[[package]]\n"
                'name = "rusqlite"\n'
                'version = "0.40.2"\n'
                "\n"
                "[[package]]\n"
                'name = "libsqlite3-sys"\n'
                'version = "0.38.2"\n',
                encoding="utf-8",
            )

            original_tomllib = module.tomllib
            module.tomllib = None
            try:
                rusqlite = module.manifest_dependency(
                    root, "shardloom-cli/Cargo.toml", "rusqlite"
                )
                vortex = module.manifest_dependency(
                    root, "shardloom-vortex/Cargo.toml", "vortex"
                )
                lock_versions = module.cargo_lock_versions(root)
            finally:
                module.tomllib = original_tomllib

        self.assertEqual(
            rusqlite,
            {"version": "0.40.2", "default-features": False, "features": ["bundled"]},
        )
        self.assertEqual(
            vortex,
            {"version": CURRENT_VORTEX_MANIFEST_VERSION, "optional": True},
        )
        self.assertEqual(lock_versions["vortex"], CURRENT_VORTEX_LOCK_VERSION)
        self.assertEqual(lock_versions["rusqlite"], "0.40.2")
        self.assertEqual(lock_versions["libsqlite3-sys"], "0.38.2")

    def test_workspace_version_source_contract_accepts_manifest_derived_versions(self) -> None:
        module = self._load_script_module(
            "check_workspace_version_sources.py",
            "check_workspace_version_sources_for_test",
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_workspace_version_source_fixture(root)

            report = module.build_report(root)

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(
            report["version_env"]["SHARDLOOM_RUST_MSRV_TOOLCHAIN"],
            WORKSPACE_VERSION_ENV["SHARDLOOM_RUST_MSRV_TOOLCHAIN"],
        )
        self.assertEqual(
            report["version_env"]["SHARDLOOM_UPSTREAM_VORTEX_MANIFEST_VERSION"],
            CURRENT_VORTEX_MANIFEST_VERSION,
        )
        self.assertEqual(
            report["version_env"]["SHARDLOOM_UPSTREAM_VORTEX_LOCK_VERSION"],
            CURRENT_VORTEX_LOCK_VERSION,
        )
        self.assertIn(
            "docs/architecture/effectful-operation-admission-matrix.md",
            report["active_doc_version_literal_audit_paths"],
        )
        self.assertEqual(
            report["selected_package_release_version_source"],
            "scripts/release_channel_contract.py#SELECTED_PACKAGE_RELEASE_VERSION",
        )
        self.assertEqual(
            report["selected_package_release_version"],
            SELECTED_PACKAGE_RELEASE_VERSION,
        )
        self.assertEqual(report["selected_package_release_tag"], SELECTED_PACKAGE_RELEASE_TAG)
        self.assertEqual(
            report["selected_package_channel_status_marker"],
            SELECTED_PACKAGE_CHANNEL_STATUS_MARKER,
        )
        self.assertFalse(report["fallback_attempted"])
        self.assertFalse(report["external_engine_invoked"])

    def test_workspace_version_source_contract_blocks_duplicate_version_sources(self) -> None:
        module = self._load_script_module(
            "check_workspace_version_sources.py",
            "check_workspace_version_sources_blocker_for_test",
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_workspace_version_source_fixture(root, stale=True)

            report = module.build_report(root)

        blockers = "\n".join(report["blockers"])
        self.assertEqual(report["status"], "blocked")
        self.assertIn("shardloom-core: package version must inherit", blockers)
        self.assertIn("internal dependency shardloom-core must inherit", blockers)
        self.assertIn("shardloom-vortex/Cargo.toml", blockers)
        self.assertIn("forbidden marker", blockers)
        self.assertIn("scripts/write_ci_version_env.py", blockers)
        self.assertIn("pinned Rust toolchain command", blockers)
        self.assertIn("pinned active upstream Vortex provider prose", blockers)

    def test_write_ci_version_env_formats_reusable_shell_assignments(self) -> None:
        module = self._load_script_module(
            "write_ci_version_env.py",
            "write_ci_version_env_formats_for_test",
        )
        env = {
            "SHARDLOOM_RUST_MSRV_TOOLCHAIN": "fixture-rust-msrv",
            "SHARDLOOM_UPSTREAM_VORTEX_PROVIDER_VERSION": "fixture-vortex",
        }

        self.assertIn(
            '$env:SHARDLOOM_RUST_MSRV_TOOLCHAIN = "fixture-rust-msrv"',
            module.format_env(env, "powershell"),
        )
        self.assertIn(
            "export SHARDLOOM_UPSTREAM_VORTEX_PROVIDER_VERSION='fixture-vortex'",
            module.format_env(env, "posix"),
        )
        self.assertEqual(
            json.loads(module.format_env(env, "json"))[
                "SHARDLOOM_RUST_MSRV_TOOLCHAIN"
            ],
            "fixture-rust-msrv",
        )
        self.assertIn(
            "SHARDLOOM_UPSTREAM_VORTEX_PROVIDER_VERSION=fixture-vortex",
            module.format_env(env, "env"),
        )

    def test_finished_product_readiness_allows_local_ready_publication_blocked(self) -> None:
        module = self._load_script_module(
            "check_finished_product_readiness.py",
            "check_finished_product_readiness_local_ready_for_test",
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            matrix_path = self._write_finished_product_readiness_fixture(module, root)

            report = module.build_report(
                root,
                package_channel_matrix=matrix_path,
                require_public_release_ready=False,
            )

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(
            report["finished_product_readiness_status"],
            "local_v1_ready_publication_blocked",
        )
        self.assertTrue(report["local_evidence_ready"])
        self.assertFalse(report["public_release_ready"])
        self.assertFalse(report["public_release_claim_allowed"])
        self.assertFalse(report["public_package_claim_allowed"])
        self.assertFalse(report["fallback_attempted"])
        self.assertFalse(report["external_engine_invoked"])
        self.assertEqual(report["local_evidence_blockers"], [])
        self.assertTrue(report["public_release_blockers"])

    def test_finished_product_readiness_public_mode_requires_public_evidence(self) -> None:
        module = self._load_script_module(
            "check_finished_product_readiness.py",
            "check_finished_product_readiness_public_mode_for_test",
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            matrix_path = self._write_finished_product_readiness_fixture(module, root)

            report = module.build_report(
                root,
                package_channel_matrix=matrix_path,
                require_public_release_ready=True,
            )

        blockers = "\n".join(report["blockers"])
        self.assertEqual(report["status"], "blocked")
        self.assertTrue(report["local_evidence_ready"])
        self.assertIn("package channels not ready", blockers)
        self.assertFalse(report["public_release_claim_allowed"])

    def test_finished_product_readiness_public_mode_passes_with_public_evidence(self) -> None:
        module = self._load_script_module(
            "check_finished_product_readiness.py",
            "check_finished_product_readiness_public_ready_for_test",
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            matrix_path = self._write_finished_product_readiness_fixture(
                module,
                root,
                public_ready=True,
            )

            report = module.build_report(
                root,
                package_channel_matrix=matrix_path,
                require_public_release_ready=True,
            )

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(
            report["finished_product_readiness_status"],
            "public_release_ready",
        )
        self.assertTrue(report["public_release_ready"])
        self.assertTrue(report["public_release_claim_allowed"])
        self.assertTrue(report["public_package_claim_allowed"])
        self.assertFalse(report["publication_attempted"])
        self.assertFalse(report["tag_created"])

    def test_finished_product_readiness_blocks_local_evidence_drift(self) -> None:
        module = self._load_script_module(
            "check_finished_product_readiness.py",
            "check_finished_product_readiness_local_blocker_for_test",
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            matrix_path = self._write_finished_product_readiness_fixture(
                module,
                root,
                local_blocked=True,
            )

            report = module.build_report(
                root,
                package_channel_matrix=matrix_path,
                require_public_release_ready=False,
            )

        blockers = "\n".join(report["local_evidence_blockers"])
        self.assertEqual(report["status"], "blocked")
        self.assertFalse(report["local_evidence_ready"])
        self.assertIn("v1_api_schema_stability", blockers)
        self.assertIn("schema fixture drift", blockers)

    def _write_production_certification_fixture(
        self,
        module: object,
        root: Path,
        *,
        production_ready: bool = False,
    ) -> Path:
        def write_json(path: Path, payload: dict[str, object]) -> None:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

        safety_false = {
            "production_claim_allowed": False,
            "performance_claim_allowed": False,
            "public_release_claim_allowed": False,
            "public_package_claim_allowed": False,
            "publication_attempted": False,
            "tag_created": False,
            "secrets_required": False,
            "package_upload_attempted": False,
            "package_channel_submission_attempted": False,
            "oci_push_attempted": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "fallback_engine_dependency_added": False,
            "external_engine_runtime_dependency_added": False,
        }
        evidence_status = "passed" if production_ready else "blocked_missing_fixture_proof"
        matrix_path = root / "docs" / "release" / "production-certification-workloads.json"
        write_json(
            matrix_path,
            {
                "schema_version": module.SCHEMA_VERSION,
                "status": "ready" if production_ready else "blocked",
                "claim_gate_status": "not_claim_grade",
                "required_evidence_keys": list(module.REQUIRED_EVIDENCE_KEYS),
                "technique_review_keys": list(module.TECHNIQUE_REVIEW_KEYS),
                **safety_false,
                "workloads": [
                    {
                        "workload_id": "fixture_local_workload",
                        "workload_name": "Fixture local workload",
                        "v1_scope_classification": "required_for_v1",
                        "readiness_status": "production_ready"
                        if production_ready
                        else "blocked_not_production_ready",
                        "production_ready": production_ready,
                        "claim_gate_status": "claim_grade"
                        if production_ready
                        else "not_claim_grade",
                        "production_claim_allowed": False,
                        "performance_claim_allowed": False,
                        "fallback_attempted": False,
                        "external_engine_invoked": False,
                        "environment": "fixture_local",
                        "data_scale": "fixture",
                        "input_formats": ["csv"],
                        "output_formats": ["jsonl"],
                        "statefulness": "stateless_batch",
                        "effect_permissions": ["no_network", "no_secrets"],
                        "security_posture": "fixture_no_effects",
                        "unsupported_edge_boundary": "fixture unsupported paths stay blocked",
                        "technique_review": {
                            key: {"decision": "applied", "reason": "fixture"}
                            for key in module.TECHNIQUE_REVIEW_KEYS
                        },
                        "evidence": {
                            key: {
                                "status": evidence_status,
                                "evidence_refs": [f"target/{key}.json"],
                            }
                            for key in module.REQUIRED_EVIDENCE_KEYS
                        },
                        "unsupported_diagnostics": [
                            {
                                "operation": "fixture_object_store",
                                "diagnostic_code": "SL_PROD_UNSUPPORTED_FIXTURE_OBJECT_STORE",
                                "status": module.UNSUPPORTED_STATUS,
                                "fallback_attempted": False,
                                "external_engine_invoked": False,
                            }
                        ],
                        "production_blockers": []
                        if production_ready
                        else ["fixture production proof missing"],
                    }
                ],
            },
        )

        (root / "README.md").write_text(
            "docs/release/public-status-matrix.md\n"
            "performance superiority are not claimed.\n",
            encoding="utf-8",
        )
        (root / "docs" / "release").mkdir(parents=True, exist_ok=True)
        (root / "docs" / "release" / "public-status-matrix.md").write_text(
            "production_claim_allowed=false\nperformance_claim_allowed=false\n",
            encoding="utf-8",
        )
        write_json(
            root / "docs" / "status" / "runs-today-support-matrix.json",
            {
                "performance_claim_allowed": False,
                "package_publication_allowed": True,
                "row_order": ["claim_production_readiness"],
            },
        )
        (root / "python").mkdir(parents=True, exist_ok=True)
        (root / "python" / "pyproject.toml").write_text(
            'name = "shardloom"\n'
            'classifiers = ["Development Status :: 2 - Pre-Alpha"]\n',
            encoding="utf-8",
        )
        benchmark_methodology = (
            root
            / "website-src"
            / "src"
            / "content"
            / "docs"
            / "field-guide"
            / "benchmark-methodology.mdx"
        )
        benchmark_methodology.parent.mkdir(parents=True, exist_ok=True)
        benchmark_methodology.write_text(
            'const clickBenchUrl = "https://benchmark.clickhouse.com/";\n'
            "does not present a public ranking.\n"
            "performance_claim_allowed=false\n"
            "production readiness\n",
            encoding="utf-8",
        )
        for name, path, schema in [
            (
                "user_route_capability",
                "target/user-route-capability-report.json",
                "shardloom.user_route_capability_report.v1",
            ),
            (
                "runtime_gap_family_burn_down",
                "target/runtime-gap-family-burn-down.json",
                "shardloom.runtime_gap_family_burn_down.v1",
            ),
            (
                "v1_correctness_conformance",
                "target/v1-correctness-conformance-report.json",
                "shardloom.v1_correctness_conformance_report.v1",
            ),
            (
                "v1_local_resource_safety",
                "target/v1-local-resource-safety-report.json",
                "shardloom.v1_local_resource_safety_report.v1",
            ),
            (
                "v1_release_boundary",
                "target/v1-release-boundary-report.json",
                "shardloom.v1_release_boundary_report.v1",
            ),
        ]:
            write_json(
                root / path,
                {
                    "name": name,
                    "schema_version": schema,
                    "status": "passed",
                    "fallback_attempted": False,
                    "external_engine_invoked": False,
                },
            )
        return matrix_path

    def _object_store_local_emulator_workload(self, module: object) -> dict[str, object]:
        evidence_statuses = {
            "runtime_execution": "passed_scoped_local_emulator",
            "correctness": "passed_scoped_local_emulator",
            "native_io_certificate": "passed_fixture_smoke_only",
            "execution_certificate": "passed_fixture_smoke_only",
            "fault_tolerance": "passed_scoped_local_recovery_smoke",
            "memory_backpressure": "blocked_missing_bounded_streaming_and_backpressure_profile",
            "benchmark": "blocked_not_claim_grade",
            "security_governance": "passed_scoped_local_emulator",
            "release_api_stability": "passed_scoped_local_emulator",
            "unsupported_diagnostics": "passed_scoped_local_emulator",
        }
        unsupported_rows = []
        for operation in module.OBJECT_STORE_REQUIRED_UNSUPPORTED_OPERATIONS:
            diagnostic_code = "SL_PROD_UNSUPPORTED_OBJECT_STORE_RUNTIME"
            if operation == "object_store_table_commit":
                diagnostic_code = "SL_PROD_UNSUPPORTED_TABLE_RUNTIME"
            elif operation == "object_store_distributed_runtime":
                diagnostic_code = "SL_PROD_UNSUPPORTED_DISTRIBUTED_RUNTIME"
            unsupported_rows.append(
                {
                    "operation": operation,
                    "diagnostic_code": diagnostic_code,
                    "status": module.UNSUPPORTED_STATUS,
                    "fallback_attempted": False,
                    "external_engine_invoked": False,
                }
            )

        return {
            "workload_id": module.OBJECT_STORE_LOCAL_EMULATOR_WORKLOAD_ID,
            "workload_name": "Object-store local-emulator runtime v1 candidate",
            "v1_scope_classification": "v1_candidate_pending_feasibility",
            "readiness_status": "blocked_real_backend_and_claim_grade_evidence_missing",
            "production_ready": False,
            "claim_gate_status": "not_claim_grade",
            "production_claim_allowed": False,
            "performance_claim_allowed": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "environment": "single_process_local_emulator_fixture_object_store",
            "data_scale": "bounded local-emulator fixture objects",
            "input_formats": ["local_emulator_object"],
            "output_formats": ["local_emulator_object"],
            "statefulness": "single_object_read_write_recovery_with_idempotency_key",
            "effect_permissions": sorted(module.OBJECT_STORE_REQUIRED_EFFECT_PERMISSIONS),
            "security_posture": "local_emulator_no_secrets_no_network",
            "unsupported_edge_boundary": "live providers and table commits remain blocked",
            "technique_review": {
                key: {"decision": "applied_scoped", "reason": "fixture"}
                for key in module.TECHNIQUE_REVIEW_KEYS
            },
            "evidence": {
                key: {
                    "status": evidence_statuses[key],
                    "evidence_refs": (
                        sorted(module.OBJECT_STORE_REQUIRED_SECURITY_EVIDENCE_REFS)
                        if key == "security_governance"
                        else [f"target/object-store/{key}.json"]
                    ),
                }
                for key in module.REQUIRED_EVIDENCE_KEYS
            },
            "unsupported_diagnostics": unsupported_rows,
            "production_blockers": [
                "memory_backpressure=blocked_missing_bounded_streaming_and_backpressure_profile",
                "benchmark=blocked_not_claim_grade",
                "approved real backend proof missing",
            ],
        }

    def test_production_certification_gate_passes_with_blocked_workload(self) -> None:
        module = self._load_script_module(
            "check_production_certification_gate.py",
            "check_production_certification_gate_blocked_for_test",
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            matrix_path = self._write_production_certification_fixture(module, root)

            report = module.build_report(root, matrix=matrix_path)

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(
            report["production_certification_status"],
            "blocked_not_production_ready",
        )
        self.assertEqual(report["production_ready_workload_count"], 0)
        self.assertFalse(report["production_claim_allowed"])
        self.assertFalse(report["fallback_attempted"])
        self.assertFalse(report["external_engine_invoked"])
        self.assertTrue(report["production_evidence_blockers"])

    def test_production_certification_gate_strict_mode_requires_ready_workload(self) -> None:
        module = self._load_script_module(
            "check_production_certification_gate.py",
            "check_production_certification_gate_strict_for_test",
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            matrix_path = self._write_production_certification_fixture(module, root)

            report = module.build_report(
                root,
                matrix=matrix_path,
                require_production_ready_workload=True,
            )

        self.assertEqual(report["status"], "blocked")
        self.assertIn(
            "strict production mode requires a production-ready workload",
            report["blockers"],
        )

    def test_production_certification_gate_accepts_ready_fixture(self) -> None:
        module = self._load_script_module(
            "check_production_certification_gate.py",
            "check_production_certification_gate_ready_for_test",
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            matrix_path = self._write_production_certification_fixture(
                module,
                root,
                production_ready=True,
            )

            report = module.build_report(
                root,
                matrix=matrix_path,
                require_production_ready_workload=True,
            )

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(report["production_certification_status"], "production_ready")
        self.assertEqual(report["production_ready_workload_count"], 1)
        self.assertTrue(report["production_claim_allowed"])

    def test_production_certification_gate_accepts_object_store_local_emulator_profile(self) -> None:
        module = self._load_script_module(
            "check_production_certification_gate.py",
            "check_production_certification_gate_object_store_for_test",
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            matrix_path = self._write_production_certification_fixture(module, root)
            payload = json.loads(matrix_path.read_text(encoding="utf-8"))
            payload["workloads"].append(self._object_store_local_emulator_workload(module))
            matrix_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

            report = module.build_report(root, matrix=matrix_path)

        self.assertEqual(report["status"], "passed", report["blockers"])
        rows = {row["workload_id"]: row for row in report["workloads"]}
        object_store_row = rows[module.OBJECT_STORE_LOCAL_EMULATOR_WORKLOAD_ID]
        profile = object_store_row["object_store_local_emulator_profile"]
        self.assertEqual(object_store_row["status"], "passed")
        self.assertEqual(profile["status"], "passed")
        self.assertFalse(profile["production_claim_allowed"])
        self.assertFalse(profile["performance_claim_allowed"])
        self.assertEqual(
            profile["effect_permissions_checked"],
            sorted(module.OBJECT_STORE_REQUIRED_EFFECT_PERMISSIONS),
        )
        self.assertEqual(
            profile["unsupported_operations_checked"],
            sorted(module.OBJECT_STORE_REQUIRED_UNSUPPORTED_OPERATIONS),
        )

    def test_production_certification_gate_blocks_object_store_claim_safety_drift(self) -> None:
        module = self._load_script_module(
            "check_production_certification_gate.py",
            "check_production_certification_gate_object_store_blocked_for_test",
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            matrix_path = self._write_production_certification_fixture(module, root)
            payload = json.loads(matrix_path.read_text(encoding="utf-8"))
            workload = self._object_store_local_emulator_workload(module)
            workload["effect_permissions"] = [
                item
                for item in workload["effect_permissions"]
                if item != "no_network"
            ]
            payload["workloads"].append(workload)
            matrix_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

            report = module.build_report(root, matrix=matrix_path)

        self.assertEqual(report["status"], "blocked")
        blockers = "\n".join(report["blockers"])
        self.assertIn("effect_permissions missing no_network", blockers)
        rows = {row["workload_id"]: row for row in report["workloads"]}
        object_store_row = rows[module.OBJECT_STORE_LOCAL_EMULATOR_WORKLOAD_ID]
        self.assertEqual(object_store_row["status"], "blocked")
        self.assertEqual(
            object_store_row["object_store_local_emulator_profile"]["status"],
            "blocked",
        )

    def test_pre_5j_dependency_freshness_blocks_stale_vortex_provider_surfaces(self) -> None:
        module = self._load_script_module(
            "check_pre_5j_dependency_freshness.py",
            "check_pre_5j_dependency_freshness_provider_surface_for_test",
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "Cargo.toml").write_text(
                "[workspace.dependencies]\n"
                f'vortex = "{CURRENT_VORTEX_MANIFEST_VERSION}"\n',
                encoding="utf-8",
            )
            lib_rs = root / "shardloom-vortex" / "src" / "lib.rs"
            lib_rs.parent.mkdir(parents=True)
            lib_rs.write_text(
                'pub const UPSTREAM_VORTEX_PROVIDER_VERSION: &str =\n'
                '    env!("SHARDLOOM_UPSTREAM_VORTEX_PROVIDER_VERSION");\n',
                encoding="utf-8",
            )
            (root / "shardloom-vortex" / "Cargo.toml").write_text(
                "[dependencies]\n"
                "vortex = { workspace = true, optional = true }\n",
                encoding="utf-8",
            )
            build_rs = root / "shardloom-vortex" / "build.rs"
            build_rs.write_text(
                'workspace_dependency_version(&workspace_manifest_text, "vortex")\n'
                'cargo:rustc-env=SHARDLOOM_UPSTREAM_VORTEX_PROVIDER_VERSION={vortex_version}\n',
                encoding="utf-8",
            )
            client_tests = root / "python" / "tests" / "test_cli_client.py"
            client_tests.parent.mkdir(parents=True)
            client_tests.write_text(
                "from release_report_utils import upstream_vortex_provider_version\n"
                "UPSTREAM_VORTEX_PROVIDER_VERSION = upstream_vortex_provider_version(REPO_ROOT)\n"
                '{"key": "provider_version", "value": "0.73"}\n'
                'self.assertEqual(result.provider_version, "0.73")\n',
                encoding="utf-8",
            )

            rows = module.validate_vortex_provider_version_surfaces(root)

        blockers = [blocker for row in rows for blocker in row["blockers"]]
        self.assertTrue(
            any('provider_version, "0.73"' in blocker for blocker in blockers),
            blockers,
        )
        self.assertTrue(
            any('"value": UPSTREAM_VORTEX_PROVIDER_VERSION' in blocker for blocker in blockers),
            blockers,
        )

    def test_pre_5j_dependency_freshness_blocks_unknown_dependabot_pr(self) -> None:
        module = self._load_script_module(
            "check_pre_5j_dependency_freshness.py",
            "check_pre_5j_dependency_freshness_blocker_for_test",
        )
        report = module.build_report(
            repo_root=REPO_ROOT,
            open_prs=[
                self._dependabot_pr(1150, "Bump vortex"),
                self._dependabot_pr(981, "Bump unexpected-package from 1.0.0 to 2.0.0"),
            ],
            open_prs_status="loaded_from_file",
            require_live_github=True,
        )

        self.assertEqual(report["status"], "blocked")
        self.assertFalse(report["benchmark_refresh_allowed"])
        self.assertTrue(
            any("unincorporated open Dependabot PR before 5J: #981" in blocker for blocker in report["blockers"])
        )

    def test_pre_5j_dependency_freshness_without_live_check_keeps_benchmark_blocked(self) -> None:
        module = self._load_script_module(
            "check_pre_5j_dependency_freshness.py",
            "check_pre_5j_dependency_freshness_offline_for_test",
        )
        report = module.build_report(
            repo_root=REPO_ROOT,
            open_prs=None,
            open_prs_status="skipped_not_requested",
            require_live_github=False,
        )

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(
            report["benchmark_refresh_dependency_gate_status"],
            "blocked_live_github_check_required",
        )
        self.assertFalse(report["benchmark_refresh_allowed"])

    def test_golden_workflow_gate_requires_external_engine_marker(self) -> None:
        module = self._load_script_module(
            "check_golden_workflows.py", "check_golden_workflows_for_test"
        )

        blockers = module.no_fallback_blockers(
            {
                "schema_version": "shardloom.output.v2",
                "fallback": {"attempted": False, "allowed": False},
                "fields": [{"key": "fallback_attempted", "value": "false"}],
            },
            "fixture",
        )

        self.assertIn("fixture: external engine marker is missing", blockers)

    def test_ci_gate_matrix_scopes_commands_to_declared_job(self) -> None:
        module = self._load_script_module(
            "check_ci_gate_matrix.py", "check_ci_gate_matrix_for_test"
        )

        workflow = """
name: ci
jobs:
  release-readiness:
    steps:
      - run: python scripts/check_release_readiness.py
  ci-gate-matrix:
    steps:
      - run: python scripts/check_ci_gate_matrix.py
"""
        doc = (
            "ci_gate_matrix_contract\n"
            "python scripts/check_ci_gate_matrix.py\n"
            "target/ci-gate-matrix-report.json\n"
            "CI matrix drift contract\n"
        )

        status = module.lane_status(module.REQUIRED_LANES[-1], workflow, doc)

        self.assertEqual(status["status"], "failed")
        self.assertIn(
            "workflow job ci-gate-matrix missing artifact ref: target/ci-gate-matrix-report.json",
            status["blockers"],
        )

    def test_ci_gate_matrix_requires_hard_release_without_allow_blocked(self) -> None:
        module = self._load_script_module(
            "check_ci_gate_matrix.py", "check_ci_gate_matrix_readiness_for_test"
        )

        release_lane = next(
            lane
            for lane in module.REQUIRED_LANES
            if lane.lane_id == "release_readiness_reports"
        )

        self.assertIn("python scripts/check_release_readiness.py", release_lane.commands)
        self.assertIn("python scripts/check_v1_security_ci_hardening.py", release_lane.commands)
        self.assertIn("python scripts/check_v1_release_boundary.py", release_lane.commands)
        self.assertIn("python scripts/check_production_certification_gate.py", release_lane.commands)
        self.assertIn("python scripts/check_finished_product_readiness.py", release_lane.commands)
        self.assertIn(
            "target/finished-product-readiness-report.json",
            release_lane.artifact_refs,
        )
        self.assertIn(
            "target/v1-release-boundary-report.json",
            release_lane.artifact_refs,
        )
        self.assertIn(
            "target/production-certification-gate.json",
            release_lane.artifact_refs,
        )
        self.assertNotIn(
            "python scripts/check_release_readiness.py --allow-blocked",
            release_lane.commands,
        )
        self.assertIn("continue-on-error: true", release_lane.workflow_markers)

        package_lane = next(
            lane
            for lane in module.REQUIRED_LANES
            if lane.lane_id == "release_package_governance_evidence"
        )
        self.assertIn("python scripts/check_workspace_version_sources.py", package_lane.commands)
        self.assertIn(
            "target/workspace-version-source-report.json",
            package_lane.artifact_refs,
        )

        lane_ids = {lane.lane_id for lane in module.REQUIRED_LANES}
        self.assertIn("python_compatibility_matrix", lane_ids)
        self.assertIn("rust_msrv_validation", lane_ids)

    def test_ci_gate_matrix_requires_windows_native_release_build(self) -> None:
        module = self._load_script_module(
            "check_ci_gate_matrix.py", "check_ci_gate_matrix_windows_native_for_test"
        )
        lane = next(
            lane for lane in module.REQUIRED_LANES
            if lane.lane_id == "python_compatibility_matrix"
        )
        workflow = (REPO_ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
        doc = (REPO_ROOT / "docs/release/ci-gate-matrix.md").read_text(encoding="utf-8")
        accepted = module.lane_status(lane, workflow, doc)
        self.assertEqual(accepted["status"], "passed", accepted["blockers"])

        for command in (
            "cargo build --release -p shardloom-cli --bin shardloom --features release-user-surfaces",
            "./target/release/shardloom.exe --version",
        ):
            with self.subTest(command=command):
                # A Python-only package lane must not receive native build credit.
                old_lane = module.workflow_job_section(workflow, lane.job_id)
                changed = workflow.replace(old_lane, old_lane.replace(command, "echo omitted"))
                rejected = module.lane_status(lane, changed, doc)
                self.assertEqual(rejected["status"], "failed")
                self.assertIn(
                    f"workflow job {lane.job_id} missing command: {command}",
                    rejected["blockers"],
                )

    def test_focused_check_runner_scopes_rust_filters_to_exact_targets(self) -> None:
        module = self._load_script_module(
            "run_focused_checks.py", "run_focused_checks_for_test"
        )

        cli_unit_args = argparse.Namespace(
            repo_root=REPO_ROOT,
            profile="rust-cli-bin",
            filter="route_infers_vortex_manifest_as_native_vortex_input",
            target=None,
            nocapture=True,
        )
        cli_unit = module.commands_for_profile(cli_unit_args)[0].command

        self.assertEqual(
            cli_unit,
            (
                "cargo",
                "test",
                "-p",
                "shardloom-cli",
                "--features",
                "release-user-surfaces",
                "--bin",
                "shardloom",
                "route_infers_vortex_manifest_as_native_vortex_input",
                "--",
                "--nocapture",
            ),
        )

        cli_integration_args = argparse.Namespace(
            repo_root=REPO_ROOT,
            profile="rust-cli-test",
            filter="partitioned",
            target="public_workflow_route",
            nocapture=True,
        )
        cli_integration = module.commands_for_profile(cli_integration_args)[0].command

        self.assertEqual(
            cli_integration,
            (
                "cargo",
                "test",
                "-p",
                "shardloom-cli",
                "--features",
                "release-user-surfaces",
                "--test",
                "public_workflow_route",
                "partitioned",
                "--",
                "--nocapture",
            ),
        )

        current_args = argparse.Namespace(
            repo_root=REPO_ROOT,
            profile="current-native-vortex",
            filter=None,
            target=None,
            nocapture=True,
        )
        current_commands = [
            module.command_text(command.command)
            for command in module.commands_for_profile(current_args)
        ]

        self.assertIn(
            "cargo test -p shardloom-cli --features release-user-surfaces --bin shardloom "
            "route_infers_vortex_manifest_as_native_vortex_input -- --nocapture",
            current_commands,
        )
        self.assertIn(
            "cargo test -p shardloom-vortex --features vortex-local-primitives --lib "
            "partitioned_local_primitive -- --nocapture",
            current_commands,
        )
        self.assertIn(
            "cargo test -p shardloom-cli --features release-user-surfaces --test "
            "public_workflow_route partitioned -- --nocapture",
            current_commands,
        )
        self.assertIn(
            f"{sys.executable} -m unittest "
            "python.tests.test_query_builder.LazyWorkflowBuilderTests."
            "test_context_sql_vortex_manifest_source_binds_native_vortex_collect",
            current_commands,
        )
        self.assertIn(
            f"{sys.executable} -m unittest "
            "python.tests.test_query_builder.LazyWorkflowBuilderTests."
            "test_context_sql_embedded_vortex_manifest_broad_query_uses_native_input_binding",
            current_commands,
        )

    def test_release_readiness_job_runs_after_failed_dependencies(self) -> None:
        workflow = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(
            encoding="utf-8"
        )
        release_job = workflow.split("  release-readiness:", maxsplit=1)[1].split(
            "  website-docs:", maxsplit=1
        )[0]

        self.assertIn("if: ${{ always() }}", release_job)
        self.assertIn("python scripts/check_release_readiness.py", release_job)

    def _write_public_status_docs_fixture(self, module: object, repo_root: Path) -> None:
        path_markers: dict[str, tuple[str, ...]] = {
            module.PUBLIC_STATUS_REF.as_posix(): module.CANONICAL_PUBLIC_STATUS_MARKERS,
            **module.PUBLIC_DOC_MARKERS,
            **module.COMPUTE_FLOW_MARKERS,
        }
        for rel_path, markers in path_markers.items():
            path = repo_root / rel_path
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("\n".join(markers) + "\n", encoding="utf-8")
        v1_docs_module = self._load_script_module(
            "check_v1_docs_productization.py",
            "check_v1_docs_productization_public_status_fixture",
        )
        for rel_path in v1_docs_module.DOC_MARKERS:
            path = repo_root / rel_path
            path.parent.mkdir(parents=True, exist_ok=True)
            existing = path.read_text(encoding="utf-8") if path.exists() else ""
            source_text = (REPO_ROOT / rel_path).read_text(encoding="utf-8")
            if rel_path == v1_docs_module.SUPPORTED_UNSUPPORTED_DOC.as_posix():
                path.write_text(source_text, encoding="utf-8")
            else:
                path.write_text(existing + source_text + "\n", encoding="utf-8")
        for rel_path in (
            "docs/status/runs-today-support-matrix.json",
            "docs/release/package-channel-readiness-matrix.json",
        ):
            path = repo_root / rel_path
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                (REPO_ROOT / rel_path).read_text(encoding="utf-8"),
                encoding="utf-8",
            )
        claim_module = self._load_script_module(
            "check_public_claim_language.py",
            "check_public_claim_language_public_status_fixture",
        )
        self._write_public_claim_language_fixture(claim_module, repo_root)
        v1_module = self._load_script_module(
            "check_v1_inclusion_scope.py",
            "check_v1_inclusion_scope_public_status_fixture",
        )
        self._write_v1_inclusion_scope_fixture(v1_module, repo_root)
        v1_front_door_module = self._load_script_module(
            "check_v1_front_door_runtime_scope.py",
            "check_v1_front_door_runtime_scope_public_status_fixture",
        )
        self._write_v1_front_door_runtime_scope_fixture(
            v1_front_door_module,
            repo_root,
        )
        v1_vortex_module = self._load_script_module(
            "check_v1_vortex_runtime_scope.py",
            "check_v1_vortex_runtime_scope_public_status_fixture",
        )
        self._write_v1_vortex_runtime_scope_fixture(v1_vortex_module, repo_root)
        v1_source_prepared_module = self._load_script_module(
            "check_v1_source_prepared_state_scope.py",
            "check_v1_source_prepared_state_scope_public_status_fixture",
        )
        self._write_v1_source_prepared_state_scope_fixture(
            v1_source_prepared_module,
            repo_root,
        )
        v1_local_output_sink_module = self._load_script_module(
            "check_v1_local_output_sink_scope.py",
            "check_v1_local_output_sink_scope_public_status_fixture",
        )
        self._write_v1_local_output_sink_scope_fixture(
            v1_local_output_sink_module,
            repo_root,
        )

    def _write_public_claim_language_fixture(
        self,
        module: object,
        repo_root: Path,
        *,
        omit_v1_row: str | None = None,
    ) -> None:
        v1_rows = [
            row for row in module.REQUIRED_V1_CLAIM_ROWS if row != omit_v1_row
        ]
        release_rows = list(module.REQUIRED_RELEASE_CLAIM_ROWS)
        out_of_v1_rows = list(module.OUT_OF_V1_CLAIM_ROWS)
        finished_scope = "\n".join(
            [
                "shardloom.finished_product_scope.v1",
                "Vortex-first",
                "no-fallback",
                "Required V1 Claim Rows",
                *v1_rows,
                *release_rows,
                "Out-of-V1 Claim Rows",
                *out_of_v1_rows,
                "Allowed External Engine Contexts",
                "PulseWeave",
                "capillary",
                "dynamic admission",
                "timing-surface",
                "evidence-tier",
            ]
        )
        per_claim = "\n".join(
            [
                "shardloom.per_claim_evidence_attachment_matrix.v1",
                "per_claim_evidence_attachment_matrix_required_v1_row_count=7",
                "per_claim_evidence_attachment_matrix_out_of_v1_row_count=6",
                "per_claim_evidence_attachment_matrix_external_baseline_context_allowed=true",
                "per_claim_evidence_attachment_matrix_performance_superiority_claim_allowed=false",
                "per_claim_evidence_attachment_matrix_spark_displacement_claim_allowed=false",
                "per_claim_evidence_attachment_matrix_engine_replacement_claim_allowed=false",
                *v1_rows,
                *release_rows,
                *out_of_v1_rows,
            ]
        )
        public_status = "\n".join(module.PUBLIC_STATUS_MARKERS)
        unsupported = "\n".join(module.KNOWN_UNSUPPORTED_MARKERS)
        for rel_path, text in {
            module.FINISHED_PRODUCT_SCOPE.as_posix(): finished_scope,
            module.PER_CLAIM_MATRIX.as_posix(): per_claim,
            module.PUBLIC_STATUS_MATRIX.as_posix(): public_status,
            module.KNOWN_UNSUPPORTED_PATHS.as_posix(): unsupported,
        }.items():
            path = repo_root / rel_path
            path.parent.mkdir(parents=True, exist_ok=True)
            existing = path.read_text(encoding="utf-8") if path.exists() else ""
            path.write_text(existing + text + "\n", encoding="utf-8")

    def _write_v1_inclusion_scope_fixture(
        self,
        module: object,
        repo_root: Path,
        *,
        item_id: str = "PROD-V1-1A",
        classification: str = "required_for_v1",
        support_gate_posture: str = "implementation_required",
        feasibility_status: str = "required_fixture_scope",
        unsupported_boundary: str = "not_deferred",
        include_phase_classification: bool = True,
        technique_review: str = (
            "dynamic; capillary; PulseWeave; metadata-first; timing-surface; evidence-tier"
        ),
    ) -> None:
        classification_line = (
            f"  - V1 scope classification: `{classification}`.\n"
            if include_phase_classification
            else ""
        )
        phase_plan = (
            "## Planned\n"
            f"- [ ] `{item_id}` Fixture row.\n"
            f"{classification_line}"
        )
        matrix = (
            "shardloom.v1_inclusion_scope_matrix.v1\n"
            "v1_inclusion_scope_allowed_classifications=required_for_v1,"
            "v1_candidate_pending_feasibility,deferred_out_of_v1,documentation_only,"
            "unsupported_boundary\n"
            "v1_inclusion_scope_required_rows_cannot_be_report_only=true\n"
            "v1_inclusion_scope_deferred_rows_require_unsupported_diagnostics=true\n"
            "v1_inclusion_scope_external_engine_fallback_allowed=false\n\n"
            "| Phase item | Classification | Support gate posture | Feasibility status | "
            "Unsupported boundary | Technique review |\n"
            "| --- | --- | --- | --- | --- | --- |\n"
            f"| `{item_id}` | `{classification}` | `{support_gate_posture}` | "
            f"`{feasibility_status}` | {unsupported_boundary} | {technique_review} |\n"
        )
        unsupported = (
            "docs/release/v1-inclusion-scope-matrix.md\n"
            "v1 candidates pending feasibility are not outside v1 by default\n"
            "deferred rows require deterministic unsupported diagnostics\n"
            "shardloom.production_unsupported_diagnostics.v1\n"
            "fallback_attempted=false\n"
            "external_engine_invoked=false\n"
            "side_effects_performed=false\n"
            "claim_gate_status=not_claim_grade\n"
            + "\n".join(
                f"diagnostic_row_id={row_id}"
                for row_id in module.REQUIRED_PRODUCTION_UNSUPPORTED_DIAGNOSTIC_ROWS
            )
            + "\n"
        )
        for rel_path, text in {
            module.PHASE_PLAN.as_posix(): phase_plan,
            module.MATRIX_DOC.as_posix(): matrix,
            module.KNOWN_UNSUPPORTED_PATHS.as_posix(): unsupported,
        }.items():
            path = repo_root / rel_path
            path.parent.mkdir(parents=True, exist_ok=True)
            existing = path.read_text(encoding="utf-8") if path.exists() else ""
            path.write_text(existing + text + "\n", encoding="utf-8")

    def _write_v1_release_boundary_fixture(
        self,
        module: object,
        repo_root: Path,
    ) -> None:
        public_status_module = self._load_script_module(
            "check_public_status_docs.py",
            "check_public_status_docs_v1_release_boundary_fixture",
        )
        self._write_public_status_docs_fixture(public_status_module, repo_root)

        for rel_path in (
            module.RUNS_TODAY_MATRIX,
            module.PACKAGE_CHANNEL_MATRIX,
            module.V1_SUPPORTED_DOC,
            module.PACKAGE_USER_INSTALL_DOC,
            module.PYPROJECT,
        ):
            source = REPO_ROOT / rel_path
            target = repo_root / rel_path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")

        pkg_info = repo_root / module.PKG_INFO
        pkg_info.parent.mkdir(parents=True, exist_ok=True)
        pkg_info.write_text(
            "\n".join(module.PKG_INFO_REQUIRED_MARKERS) + "\n",
            encoding="utf-8",
        )

        false_fields = {field: False for field in module.FALSE_SAFETY_FIELDS}
        dry_run = {
            "schema_version": "shardloom.release_dry_run_proof.v1",
            "proof_status": "passed",
            "clean_venv_install_status": "passed",
            "local_wheel": "python/dist/shardloom-0.1.0-py3-none-any.whl",
            "local_cli_binary": "target/debug/shardloom",
            "external_runtime_dependencies_added": False,
            "fallback_engine_dependency_added": False,
            "benchmark_smoke_required_for_package_release": False,
            "benchmark_smoke_status": "skipped_not_required_for_package_release",
            **false_fields,
        }
        dry_run.update({field: True for field in module.REQUIRED_DRY_RUN_TRUE_FIELDS})
        dry_run_path = repo_root / module.RELEASE_DRY_RUN_TRANSCRIPT
        dry_run_path.parent.mkdir(parents=True, exist_ok=True)
        dry_run_path.write_text(json.dumps(dry_run, indent=2) + "\n", encoding="utf-8")

        package_report = {
            "schema_version": "shardloom.package_channel_readiness_report.v1",
            "status": "passed",
            "local_gate_evidence_status": "passed",
            "package_identity_contract_status": "passed",
            "ready_channel_count": 4,
            "expected_channel_count": 9,
            "blockers": [],
            "public_package_release_claim_allowed": True,
            **false_fields,
        }
        package_report_path = repo_root / module.PACKAGE_CHANNEL_REPORT
        package_report_path.parent.mkdir(parents=True, exist_ok=True)
        package_report_path.write_text(
            json.dumps(package_report, indent=2) + "\n",
            encoding="utf-8",
        )

    def _write_v1_front_door_runtime_scope_fixture(
        self,
        module: object,
        repo_root: Path,
    ) -> None:
        scenario_names = sorted(module.EXPECTED_EXAMPLE_SCENARIOS)

        # Copy only the deterministic source declarations consumed by this validator.
        # These report methods are side-effect-free; no runtime receipt is synthesized.
        import dataclasses

        package_src = str(REPO_ROOT / "python" / "src")
        previous_sys_path = list(sys.path)
        saved_shardloom_modules = {
            name: loaded
            for name, loaded in sys.modules.items()
            if name == "shardloom" or name.startswith("shardloom.")
        }
        try:
            for name in saved_shardloom_modules:
                sys.modules.pop(name, None)
            if package_src in sys.path:
                sys.path.remove(package_src)
            sys.path.insert(0, package_src)
            from shardloom import ShardLoomContext

            context = ShardLoomContext(client=None)
            parity = context.front_door_parity_matrix()
            semantic = context.front_door_semantic_surface_matrix()
            routes = context.user_route_capability_report()

            def report_rows(rows: object) -> list[dict[str, object]]:
                return [dataclasses.asdict(row) for row in rows]

            parity_rows = report_rows(parity.rows)
            semantic_rows = report_rows(semantic.rows)
            public_route_rows = report_rows(routes.public_front_door_route_rows)
            route_capability_rows = report_rows(routes.rows)
            parity_flags = {
                "scoped_local_front_door_parity_supported": (
                    parity.scoped_local_front_door_parity_supported
                ),
                "flexible_anything_claim_allowed": parity.flexible_anything_claim_allowed,
                "performance_equivalence_claim_allowed": (
                    parity.performance_equivalence_claim_allowed
                ),
                "all_no_fallback_no_external_engine": (
                    parity.all_no_fallback_no_external_engine
                ),
            }
            semantic_fields = {
                "schema_version": semantic.schema_version,
                "row_order": semantic.row_order,
                "dataframe_claim_statement": semantic.dataframe_claim_statement,
                "dataframe_subset_claim_statement": (
                    semantic.dataframe_subset_claim_statement
                ),
                "sql_claim_statement": semantic.sql_claim_statement,
                "pandas_compatible_claim_allowed": (
                    semantic.pandas_compatible_claim_allowed
                ),
                "polars_compatible_claim_allowed": semantic.polars_compatible_claim_allowed,
                "broad_dataframe_compatible_claim_allowed": (
                    semantic.broad_dataframe_compatible_claim_allowed
                ),
                "ansi_sql_compliant_claim_allowed": semantic.ansi_sql_compliant_claim_allowed,
                "all_no_fallback_no_external_engine": (
                    semantic.all_no_fallback_no_external_engine
                ),
                "all_deterministic_blockers": semantic.all_deterministic_blockers,
            }
            route_flags = {
                "all_no_fallback_no_external_engine": (
                    routes.all_no_fallback_no_external_engine
                ),
            }
        finally:
            for name in tuple(sys.modules):
                if name == "shardloom" or name.startswith("shardloom."):
                    sys.modules.pop(name, None)
            sys.modules.update(saved_shardloom_modules)
            sys.path[:] = previous_sys_path

        for rel_path, markers in {
            module.DOC_PATH.as_posix(): module.DOC_MARKERS,
            **module.PUBLIC_DOC_MARKERS,
        }.items():
            path = repo_root / rel_path
            path.parent.mkdir(parents=True, exist_ok=True)
            existing = path.read_text(encoding="utf-8") if path.exists() else ""
            path.write_text(existing + "\n".join(markers) + "\n", encoding="utf-8")

        scenario_path = repo_root / module.SCENARIO_SUPPORT_PATH
        scenario_path.parent.mkdir(parents=True, exist_ok=True)
        scenario_path.write_text(
            textwrap.dedent(
                f'''
                from __future__ import annotations

                from typing import Sequence

                EXPECTED_ERROR_SCENARIOS = frozenset()
                SCENARIO_ROUTES = {tuple((name,) for name in scenario_names)!r}
                profile_order: Sequence[str] = ("release", "debug")
                fallback_attempted = False
                external_engine_invoked = False
                timing_components = {{}}
                python_wall_millis = 0.0
                '''
            ),
            encoding="utf-8",
        )

        package_dir = repo_root / "python" / "src" / "shardloom"
        package_dir.mkdir(parents=True, exist_ok=True)
        package_dir.joinpath("__init__.py").write_text(
            textwrap.dedent(
                f'''
                from types import SimpleNamespace


                _PARITY_ROWS = {parity_rows!r}
                _SEMANTIC_ROWS = {semantic_rows!r}
                _PUBLIC_ROUTE_ROWS = {public_route_rows!r}
                _USER_ROUTE_ROWS = {route_capability_rows!r}
                _PARITY_FLAGS = {parity_flags!r}
                _SEMANTIC_FIELDS = {semantic_fields!r}
                _ROUTE_FLAGS = {route_flags!r}


                class ShardLoomContext:
                    def __init__(self, client=None):
                        self.client = client

                    def front_door_parity_matrix(self):
                        return SimpleNamespace(
                            rows=tuple(SimpleNamespace(**row) for row in _PARITY_ROWS),
                            **_PARITY_FLAGS,
                        )

                    def front_door_semantic_surface_matrix(self):
                        return SimpleNamespace(
                            rows=tuple(SimpleNamespace(**row) for row in _SEMANTIC_ROWS),
                            **_SEMANTIC_FIELDS,
                        )

                    def user_route_capability_report(self):
                        rows = tuple(
                            SimpleNamespace(**row) for row in _USER_ROUTE_ROWS
                        )

                        def route(route_id):
                            return next(row for row in rows if row.route_id == route_id)

                        return SimpleNamespace(
                            rows=rows,
                            route_order=tuple(row.route_id for row in rows),
                            public_front_door_route_rows=tuple(
                                SimpleNamespace(**row) for row in _PUBLIC_ROUTE_ROWS
                            ),
                            **_ROUTE_FLAGS,
                            route=route,
                        )
                '''
            ),
            encoding="utf-8",
        )

    def _write_v1_vortex_runtime_scope_fixture(
        self,
        module: object,
        repo_root: Path,
    ) -> None:
        primitive_ids = [
            "vortex_count_all",
            "vortex_count_where",
            "vortex_filter_collect",
            "vortex_filter_limit_collect",
            "vortex_project_collect",
            "vortex_project_limit_collect",
            "vortex_select_star_limit_collect",
            "vortex_filter_project_collect",
            "vortex_filter_project_limit_collect",
            "vortex_tail_collect",
        ]
        scenario_ids = [
            "selective_filter",
            "filter_projection_limit",
            "group_by_aggregation",
            "multi_key_group_by",
            "join_aggregate",
            "sort_top_k",
            "row_number_window",
            "top_n_per_group",
            "clean_cast_filter_write",
            "malformed_timestamp_cast",
            "partition_pruning",
            "many_small_files_scan",
            "null_heavy_aggregate",
            "high_cardinality_string_group_distinct",
            "nested_json_field_scan",
            "small_change_over_large_base",
        ]
        starting_states = [
            "native_local_vortex_file",
            "prepared_local_vortex_state",
            "prepared_compatibility_artifact",
            "generated_local_vortex_artifact",
        ]
        unsupported_boundaries = [
            "object_store_vortex_io",
            "table_catalog_vortex_io",
            "generalized_source_sink_api",
            "broad_vortex_sql_dataframe_parity",
            "nested_complex_dtype_general_vortex",
            "vector_device_gpu_vortex_runtime",
        ]
        for rel_path, markers in {
            module.DOC_PATH.as_posix(): module.DOC_MARKERS,
            **module.PUBLIC_DOC_MARKERS,
        }.items():
            path = repo_root / rel_path
            path.parent.mkdir(parents=True, exist_ok=True)
            existing = path.read_text(encoding="utf-8") if path.exists() else ""
            path.write_text(existing + "\n".join(markers) + "\n", encoding="utf-8")

        package_init = repo_root / "python" / "src" / "shardloom" / "__init__.py"
        existing = package_init.read_text(encoding="utf-8")
        package_init.write_text(
            existing
            + textwrap.dedent(
                f'''

                V1_VORTEX_SUPPORTED_PRIMITIVE_ROUTE_IDS = {tuple(primitive_ids)!r}
                V1_VORTEX_SUPPORTED_BENCHMARK_SCENARIO_IDS = {tuple(scenario_ids)!r}
                V1_VORTEX_SUPPORTED_STARTING_STATES = {tuple(starting_states)!r}
                V1_VORTEX_UNSUPPORTED_BOUNDARY_IDS = {tuple(unsupported_boundaries)!r}


                def _v1_vortex_primitive_rows():
                    rows = []
                    for route_id in V1_VORTEX_SUPPORTED_PRIMITIVE_ROUTE_IDS:
                        rows.append(SimpleNamespace(
                            route_id=route_id,
                            primitive=route_id,
                            sql_surface="ctx.sql",
                            python_surface="ctx.read_vortex",
                            dataframe_surface="read_vortex",
                            context_surface="ctx.read_vortex",
                            session_surface="session.read_vortex",
                            cli_command="vortex-run",
                            start_state="native_vortex_file",
                            vortex_normalization_point="native_vortex_boundary",
                            execution_mode="native_vortex",
                            output_route="report",
                            evidence_route="execution and Native I/O evidence",
                            materialization_decode_boundary="bounded report",
                            supports_source_order_limit=route_id.endswith("_limit_collect"),
                            route_runtime_status="global_runtime_supported",
                            fallback_attempted=False,
                            external_engine_invoked=False,
                            required_evidence=("execution_certificate", "native_io_certificate"),
                            claim_gate_status="not_claim_grade",
                            claim_boundary="scoped local Vortex primitive only",
                        ))
                    return tuple(rows)


                class _V1VortexPrimitiveReport:
                    rows = _v1_vortex_primitive_rows()
                    schema_version = "shardloom.local_vortex_primitive_route_report.v1"
                    route_order = tuple(row.route_id for row in rows)
                    v1_scope_document = "docs/architecture/v1-vortex-runtime-scope.md"
                    v1_supported_route_ids = V1_VORTEX_SUPPORTED_PRIMITIVE_ROUTE_IDS
                    v1_supported_starting_states = V1_VORTEX_SUPPORTED_STARTING_STATES
                    v1_unsupported_boundary_ids = V1_VORTEX_UNSUPPORTED_BOUNDARY_IDS
                    v1_feature_profile_decision = "feature_gated_local_vortex_runtime"
                    v1_scope_ready = True
                    all_runtime_supported = True
                    all_no_fallback_no_external_engine = True


                _base_user_route_capability_report = (
                    ShardLoomContext.user_route_capability_report
                )


                def _v1_vortex_user_report(self):
                    base = _base_user_route_capability_report(self)

                    return SimpleNamespace(
                        rows=base.rows,
                        public_front_door_route_rows=base.public_front_door_route_rows,
                        all_no_fallback_no_external_engine=(
                            base.all_no_fallback_no_external_engine
                        ),
                        route_order=base.route_order,
                        v1_vortex_scope_document="docs/architecture/v1-vortex-runtime-scope.md",
                        v1_vortex_supported_starting_states=V1_VORTEX_SUPPORTED_STARTING_STATES,
                        v1_vortex_supported_primitive_route_ids=V1_VORTEX_SUPPORTED_PRIMITIVE_ROUTE_IDS,
                        v1_vortex_supported_benchmark_scenario_ids=V1_VORTEX_SUPPORTED_BENCHMARK_SCENARIO_IDS,
                        v1_vortex_unsupported_boundary_ids=V1_VORTEX_UNSUPPORTED_BOUNDARY_IDS,
                        v1_vortex_feature_profile_decision="feature_gated_local_vortex_runtime",
                        v1_vortex_scope_ready=True,
                        route=base.route,
                    )

                ShardLoomContext.local_vortex_primitive_route_report = lambda self: _V1VortexPrimitiveReport()
                ShardLoomContext.user_route_capability_report = _v1_vortex_user_report
                '''
            ),
            encoding="utf-8",
        )

    def _write_v1_source_prepared_state_scope_fixture(
        self,
        module: object,
        repo_root: Path,
    ) -> None:
        for rel_path in (module.DOC_PATH, *module.FIXTURE_PATHS):
            source_path = REPO_ROOT / rel_path
            target_path = repo_root / rel_path
            target_path.parent.mkdir(parents=True, exist_ok=True)
            existing_text = (
                target_path.read_text(encoding="utf-8") if target_path.exists() else ""
            )
            target_path.write_bytes(source_path.read_bytes())
            if rel_path == module.DOC_PATH and existing_text:
                target_path.write_text(
                    target_path.read_text(encoding="utf-8")
                    + "\n"
                    + existing_text,
                    encoding="utf-8",
                )

        package_init = repo_root / "python" / "src" / "shardloom" / "__init__.py"
        existing = package_init.read_text(encoding="utf-8")
        package_init.write_text(
            existing
            + textwrap.dedent(
                f"""

                from dataclasses import dataclass

                @dataclass(frozen=True)
                class _PreparedRouteRow:
                    route_id: str = {module.ROUTE_IDS[0]!r}
                    fallback_attempted: bool = False
                    external_engine_invoked: bool = False
                    claim_gate_status: str = "not_claim_grade"

                @dataclass(frozen=True)
                class SourcePreparedStateScopeReport:
                    schema_version: str = "shardloom.v1_source_prepared_state_scope.v1"
                    report_id: str = "prod-v1-1c.source_prepared_state_scope"
                    scope_document: str = {module.DOC_PATH.as_posix()!r}
                    canonical_route: str = {module.CANONICAL_ROUTE!r}
                    prepared_route_ids: tuple[str, ...] = {module.ROUTE_IDS!r}
                    supported_input_formats: tuple[str, ...] = {module.SUPPORTED_FORMATS!r}
                    invalidation_case_ids: tuple[str, ...] = {module.INVALIDATION_CASE_IDS!r}
                    golden_fixture_paths: tuple[str, ...] = tuple(
                        {tuple(path.as_posix() for path in module.FIXTURE_PATHS)!r}
                    )
                    required_runtime_fields: tuple[str, ...] = {module.REQUIRED_RUNTIME_FIELDS!r}
                    unsupported_boundary_ids: tuple[str, ...] = {module.UNSUPPORTED_BOUNDARIES!r}
                    state_owner: str = {module.STATE_OWNER!r}
                    reuse_scope: str = {module.REUSE_SCOPE!r}
                    reuse_policy: str = {module.REUSE_POLICY!r}
                    query_answers_cached: bool = False
                    prepared_user_route_rows: tuple[_PreparedRouteRow, ...] = (
                        _PreparedRouteRow(),
                    )
                    all_no_fallback_no_external_engine: bool = True
                    v1_scope_ready: bool = True
                    claim_gate_status: str = "not_claim_grade"
                    performance_claim_allowed: bool = False
                    production_claim_allowed: bool = False
                    spark_replacement_claim_allowed: bool = False

                def _source_prepared_state_scope_report(self):
                    return SourcePreparedStateScopeReport()

                ShardLoomContext.source_prepared_state_scope_report = (
                    _source_prepared_state_scope_report
                )
                """
            ),
            encoding="utf-8",
        )

    def _write_v1_local_output_sink_scope_fixture(
        self,
        module: object,
        repo_root: Path,
    ) -> None:
        for rel_path in (module.DOC_PATH, *module.PUBLIC_DOC_MARKERS):
            source = REPO_ROOT / rel_path
            target = repo_root / rel_path
            target.parent.mkdir(parents=True, exist_ok=True)
            existing = target.read_text(encoding="utf-8") if target.exists() else ""
            source_text = source.read_text(encoding="utf-8")
            target.write_text(
                (
                    source_text
                    if rel_path == module.DOC_PATH
                    else existing + source_text + "\n"
                ),
                encoding="utf-8",
            )

        source_path = str(REPO_ROOT / "python" / "src")
        inserted_source_path = source_path not in sys.path
        previous_shardloom_modules = {
            name: value
            for name, value in sys.modules.items()
            if name == "shardloom" or name.startswith("shardloom.")
        }
        if inserted_source_path:
            sys.path.insert(0, source_path)
        try:
            from shardloom import ShardLoomContext

            production_report = ShardLoomContext(
                client=None
            ).local_output_sink_scope_report()
            report_fields = (
                "schema_version",
                "report_id",
                "scope_document",
                "supported_output_formats",
                "default_output_formats",
                "feature_gated_output_formats",
                "user_write_methods",
                "output_route_ids",
                "write_policy_ids",
                "golden_fixture_paths",
                "required_runtime_fields",
                "unsupported_boundary_ids",
                "all_write_methods_registered",
                "all_write_methods_no_fallback_no_external_engine",
                "all_output_routes_no_fallback_no_external_engine",
                "all_output_routes_emit_sink_evidence",
                "all_feature_gated_formats_labeled",
                "write_policy_contract_ready",
                "v1_scope_ready",
                "claim_gate_status",
                "performance_claim_allowed",
                "production_claim_allowed",
                "spark_replacement_claim_allowed",
            )
            report_values = {
                field: getattr(production_report, field) for field in report_fields
            }

            def row_values(row: object) -> dict[str, object]:
                row_fields = getattr(type(row), "__dataclass_fields__", {})
                return {name: getattr(row, name) for name in row_fields}

            report_values["write_method_rows"] = tuple(
                row_values(row) for row in production_report.write_method_rows
            )
            report_values["output_user_route_rows"] = tuple(
                row_values(row) for row in production_report.output_user_route_rows
            )
        finally:
            for name in tuple(sys.modules):
                if name == "shardloom" or name.startswith("shardloom."):
                    sys.modules.pop(name, None)
            sys.modules.update(previous_shardloom_modules)
            if inserted_source_path:
                sys.path.remove(source_path)

        for rel_path in report_values["golden_fixture_paths"]:
            source = REPO_ROOT / rel_path
            target = repo_root / rel_path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(source.read_bytes())

        package_init = repo_root / "python" / "src" / "shardloom" / "__init__.py"
        existing = package_init.read_text(encoding="utf-8")
        package_init.write_text(
            existing
            + textwrap.dedent(
                f"""

                _V1_LOCAL_OUTPUT_REPORT = {report_values!r}

                def _v1_local_output_sink_scope_report(self):
                    report = _V1_LOCAL_OUTPUT_REPORT.copy()
                    report["write_method_rows"] = tuple(
                        SimpleNamespace(**row) for row in report["write_method_rows"]
                    )
                    report["output_user_route_rows"] = tuple(
                        SimpleNamespace(**row) for row in report["output_user_route_rows"]
                    )
                    return SimpleNamespace(**report)

                ShardLoomContext.local_output_sink_scope_report = (
                    _v1_local_output_sink_scope_report
                )
                """
            ),
            encoding="utf-8",
        )

    def test_public_status_docs_validator_accepts_required_markers(self) -> None:
        module = self._load_script_module(
            "check_public_status_docs.py",
            "check_public_status_docs_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_public_status_docs_fixture(module, repo_root)
            report = module.build_report(repo_root)

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(
            report["canonical_public_status_matrix"],
            "docs/release/public-status-matrix.md",
        )
        self.assertFalse(report["public_release_claim_allowed"])
        self.assertFalse(report["public_package_claim_allowed"])
        self.assertFalse(report["performance_claim_allowed"])
        self.assertFalse(report["fallback_attempted"])
        self.assertFalse(report["external_engine_invoked"])
        self.assertEqual(report["public_claim_language_status"], "passed")
        self.assertEqual(report["v1_inclusion_scope_status"], "passed")
        self.assertEqual(report["v1_front_door_runtime_scope_status"], "passed")
        self.assertEqual(report["v1_vortex_runtime_scope_status"], "passed")
        self.assertEqual(report["v1_source_prepared_state_scope_status"], "passed")

    def test_public_status_docs_validator_blocks_missing_marker(self) -> None:
        module = self._load_script_module(
            "check_public_status_docs.py",
            "check_public_status_docs_blocker_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_public_status_docs_fixture(module, repo_root)
            (repo_root / "README.md").write_text(
                "docs/release/public-status-matrix.md\nCurrent Support Posture\n",
                encoding="utf-8",
            )
            report = module.build_report(repo_root)

        self.assertEqual(report["status"], "failed")
        self.assertTrue(
            any(
                "README.md: missing marker" in blocker
                for blocker in report["blockers"]
            )
        )

    def test_public_claim_language_accepts_allowed_external_engine_contexts(self) -> None:
        module = self._load_script_module(
            "check_public_claim_language.py",
            "check_public_claim_language_allowed_contexts_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_public_claim_language_fixture(module, repo_root)
            fixtures = {
                "README.md": (
                    "ShardLoom does not claim Spark displacement. External engines are "
                    "baseline labels only.\n"
                ),
                "docs/getting-started/no-fallback.md": (
                    "Spark and DuckDB names appear in no-fallback policy and unsupported "
                    "diagnostics only.\n"
                ),
                "docs/use-cases/oracle.md": (
                    "Polars may be a test oracle; no fallback execution is allowed.\n"
                ),
                "docs/rfcs/0001-historical.md": (
                    "Historical RFC text says ShardLoom is a Spark replacement target.\n"
                ),
            }
            for rel_path, text in fixtures.items():
                path = repo_root / rel_path
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(text, encoding="utf-8")
            report = module.build_report(
                repo_root,
                scan_paths=tuple(fixtures.keys()),
            )

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertFalse(report["fallback_attempted"])
        self.assertFalse(report["external_engine_invoked"])

    def test_public_claim_language_blocks_positive_replacement_wording(self) -> None:
        module = self._load_script_module(
            "check_public_claim_language.py",
            "check_public_claim_language_replacement_blocker_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_public_claim_language_fixture(module, repo_root)
            (repo_root / "README.md").write_text(
                "ShardLoom is a Spark replacement for local analytics.\n",
                encoding="utf-8",
            )
            report = module.build_report(repo_root, scan_paths=("README.md",))

        self.assertEqual(report["status"], "failed")
        self.assertTrue(
            any("external_engine_replacement" in blocker for blocker in report["blockers"])
        )

    def test_public_claim_language_requires_v1_claim_rows(self) -> None:
        module = self._load_script_module(
            "check_public_claim_language.py",
            "check_public_claim_language_missing_row_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_public_claim_language_fixture(
                module,
                repo_root,
                omit_v1_row="supported_output_sink_claim",
            )
            report = module.build_report(repo_root, scan_paths=())

        self.assertEqual(report["status"], "failed")
        self.assertTrue(
            any("supported_output_sink_claim" in blocker for blocker in report["blockers"])
        )

    def test_v1_inclusion_scope_accepts_required_and_candidate_rows(self) -> None:
        module = self._load_script_module(
            "check_v1_inclusion_scope.py",
            "check_v1_inclusion_scope_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_v1_inclusion_scope_fixture(module, repo_root)
            self._write_v1_inclusion_scope_fixture(
                module,
                repo_root,
                item_id="PROD-READY-1B",
                classification="v1_candidate_pending_feasibility",
                support_gate_posture="feasibility_required",
                feasibility_status="pending_object_store_runtime_feasibility",
                unsupported_boundary="candidate_not_deferred",
            )
            report = module.build_report(repo_root)

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(report["classification_counts"]["required_for_v1"], 1)
        self.assertEqual(
            report["classification_counts"]["v1_candidate_pending_feasibility"],
            1,
        )

    def test_v1_inclusion_scope_blocks_missing_phase_classification(self) -> None:
        module = self._load_script_module(
            "check_v1_inclusion_scope.py",
            "check_v1_inclusion_scope_missing_phase_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_v1_inclusion_scope_fixture(
                module,
                repo_root,
                include_phase_classification=False,
            )
            report = module.build_report(repo_root)

        self.assertEqual(report["status"], "failed")
        self.assertTrue(
            any("missing V1 scope classification" in blocker for blocker in report["blockers"])
        )

    def test_v1_inclusion_scope_blocks_required_report_only_posture(self) -> None:
        module = self._load_script_module(
            "check_v1_inclusion_scope.py",
            "check_v1_inclusion_scope_required_posture_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_v1_inclusion_scope_fixture(
                module,
                repo_root,
                support_gate_posture="report_only",
            )
            report = module.build_report(repo_root)

        self.assertEqual(report["status"], "failed")
        self.assertTrue(
            any("forbidden support gate posture report_only" in blocker for blocker in report["blockers"])
        )

    def test_v1_inclusion_scope_blocks_deferred_without_diagnostics(self) -> None:
        module = self._load_script_module(
            "check_v1_inclusion_scope.py",
            "check_v1_inclusion_scope_deferred_boundary_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_v1_inclusion_scope_fixture(
                module,
                repo_root,
                classification="deferred_out_of_v1",
                support_gate_posture="deferred_with_reason",
                feasibility_status="deferred_infeasible_for_v1",
                unsupported_boundary="deferred_without_required_markers",
            )
            report = module.build_report(repo_root)

        self.assertEqual(report["status"], "failed")
        self.assertTrue(
            any("missing diagnostic boundary" in blocker for blocker in report["blockers"])
        )

    def test_v1_inclusion_scope_blocks_missing_production_diagnostic_row(self) -> None:
        module = self._load_script_module(
            "check_v1_inclusion_scope.py",
            "check_v1_inclusion_scope_production_diagnostic_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_v1_inclusion_scope_fixture(module, repo_root)
            unsupported_path = repo_root / module.KNOWN_UNSUPPORTED_PATHS
            unsupported_path.write_text(
                unsupported_path.read_text(encoding="utf-8").replace(
                    "diagnostic_row_id=object_store_runtime\n",
                    "",
                ),
                encoding="utf-8",
            )
            report = module.build_report(repo_root)

        self.assertEqual(report["status"], "failed")
        self.assertIn(
            "object_store_runtime",
            report["production_unsupported_diagnostic_missing_rows"],
        )
        self.assertTrue(
            any(
                "production unsupported diagnostic row missing: object_store_runtime" in blocker
                for blocker in report["blockers"]
            )
        )

    def test_v1_supported_doc_generator_inverts_no_fallback_aggregate(self) -> None:
        module = self._load_script_module(
            "write_v1_supported_unsupported_docs.py",
            "write_v1_supported_unsupported_docs_no_fallback_for_test",
        )

        rendered = module.render(
            {
                "schema_version": "shardloom.runs_today_support_matrix.v1",
                "row_count": 0,
                "family_order": [],
                "rows": [],
                "all_rows_fallback_attempted_false": True,
                "all_rows_external_engine_invoked_false": True,
                "performance_claim_allowed": False,
                "package_publication_allowed": False,
                "production_unsupported_diagnostic_schema_version": (
                    "shardloom.production_unsupported_diagnostics.v1"
                ),
                "production_unsupported_diagnostic_row_count": 1,
                "production_unsupported_diagnostic_all_rows_fallback_attempted_false": True,
                "production_unsupported_diagnostic_all_rows_external_engine_invoked_false": True,
                "production_unsupported_diagnostic_all_rows_side_effects_performed_false": True,
                "production_unsupported_diagnostics": [
                    {
                        "id": "object_store_runtime",
                        "production_family": "object_store",
                        "user_surface": ["object_store_read", "s3://"],
                        "support_status": "unsupported_boundary",
                        "diagnostic_code": "SL_UNSUPPORTED_PRODUCTION_OBJECT_STORE",
                        "blocker_id": (
                            "review-p0-3.object_store_runtime_and_path_safety_required"
                        ),
                        "next_action": "Use object-store capability reports.",
                    }
                ],
            },
            {
                "schema_version": "shardloom.package_channel_readiness_matrix.v1",
                "channels": [],
                "public_package_release_claim_allowed": False,
                "publication_attempted": False,
                "tag_created": False,
                "package_channel_submission_attempted": False,
            },
        )

        header = rendered.split("```text", maxsplit=1)[1].split("```", maxsplit=1)[0]
        self.assertIn("fallback_attempted=false", header)
        self.assertIn("external_engine_invoked=false", header)
        self.assertIn("## Production Unsupported Diagnostics", rendered)
        self.assertIn("SL_UNSUPPORTED_PRODUCTION_OBJECT_STORE", rendered)
        self.assertIn("production_unsupported_diagnostic_side_effects_performed=false", rendered)

    def test_local_format_production_profiles_validator_accepts_current_matrix(self) -> None:
        module = self._load_script_module(
            "check_local_format_production_profiles.py",
            "check_local_format_production_profiles_for_test",
        )

        report = module.build_report(REPO_ROOT)

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(report["covered_profile_count"], 5)
        self.assertFalse(report["fallback_attempted"])
        self.assertFalse(report["external_engine_invoked"])

    def test_local_format_production_profiles_validator_blocks_missing_format(self) -> None:
        module = self._load_script_module(
            "check_local_format_production_profiles.py",
            "check_local_format_production_profiles_missing_format_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            source = REPO_ROOT / module.DEFAULT_MATRIX
            target = repo_root / module.DEFAULT_MATRIX
            target.parent.mkdir(parents=True, exist_ok=True)
            payload = json.loads(source.read_text(encoding="utf-8"))
            row = next(
                profile
                for profile in payload["profiles"]
                if profile["profile_id"] == "parquet_arrow_ipc_columnar_source"
            )
            row["formats"] = ["parquet"]
            target.write_text(json.dumps(payload), encoding="utf-8")

            report = module.build_report(repo_root)

        self.assertEqual(report["status"], "failed")
        self.assertTrue(
            any(
                "parquet_arrow_ipc_columnar_source: missing formats ['arrow-ipc']"
                in blocker
                for blocker in report["blockers"]
            )
        )

    def test_local_format_pushdown_fidelity_validator_accepts_current_report(self) -> None:
        module = self._load_script_module(
            "check_local_format_pushdown_fidelity.py",
            "check_local_format_pushdown_fidelity_for_test",
        )

        report = module.build_report(REPO_ROOT)

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(report["covered_row_count"], 5)
        self.assertFalse(report["fallback_attempted"])
        self.assertFalse(report["external_engine_invoked"])

    def test_local_format_pushdown_fidelity_validator_blocks_missing_row(self) -> None:
        module = self._load_script_module(
            "check_local_format_pushdown_fidelity.py",
            "check_local_format_pushdown_fidelity_missing_row_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            source = REPO_ROOT / module.DEFAULT_REPORT
            target = repo_root / module.DEFAULT_REPORT
            target.parent.mkdir(parents=True, exist_ok=True)
            payload = json.loads(source.read_text(encoding="utf-8"))
            payload["rows"] = [
                row for row in payload["rows"] if row["row_id"] != "arrow_ipc_columnar"
            ]
            target.write_text(json.dumps(payload), encoding="utf-8")

            report = module.build_report(repo_root)

        self.assertEqual(report["status"], "failed")
        self.assertIn("arrow_ipc_columnar", report["missing_row_ids"])
        self.assertTrue(
            any(
                "missing row arrow_ipc_columnar" in blocker
                for blocker in report["blockers"]
            )
        )

    def test_compatibility_output_translation_report_validator_accepts_current_report(
        self,
    ) -> None:
        module = self._load_script_module(
            "check_compatibility_output_translation_reports.py",
            "check_compatibility_output_translation_reports_for_test",
        )

        report = module.build_report(REPO_ROOT)

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(report["covered_row_count"], 9)
        self.assertFalse(report["fallback_attempted"])
        self.assertFalse(report["external_engine_invoked"])

    def test_compatibility_output_translation_report_validator_blocks_lossless_compat(
        self,
    ) -> None:
        module = self._load_script_module(
            "check_compatibility_output_translation_reports.py",
            "check_compatibility_output_translation_reports_lossless_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            source = REPO_ROOT / module.DEFAULT_REPORT
            target = repo_root / module.DEFAULT_REPORT
            target.parent.mkdir(parents=True, exist_ok=True)
            payload = json.loads(source.read_text(encoding="utf-8"))
            row = next(
                row
                for row in payload["rows"]
                if row["row_id"] == "parquet_compatibility_output"
            )
            row["metadata_lost_or_partial"] = []
            target.write_text(json.dumps(payload), encoding="utf-8")

            report = module.build_report(repo_root)

        self.assertEqual(report["status"], "failed")
        self.assertTrue(
            any(
                "parquet_compatibility_output: compatibility outputs must list metadata_lost_or_partial"
                in blocker
                for blocker in report["blockers"]
            )
        )

    def test_local_format_edge_case_fixture_validator_accepts_current_matrix(self) -> None:
        module = self._load_script_module(
            "check_local_format_edge_case_fixtures.py",
            "check_local_format_edge_case_fixtures_for_test",
        )

        report = module.build_report(REPO_ROOT)

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(report["covered_row_count"], 7)
        self.assertEqual(report["missing_profile_refs"], [])
        self.assertEqual(report["missing_edge_families"], [])
        self.assertFalse(report["fallback_attempted"])
        self.assertFalse(report["external_engine_invoked"])

    def test_local_format_edge_case_fixture_validator_blocks_unknown_property_ref(
        self,
    ) -> None:
        module = self._load_script_module(
            "check_local_format_edge_case_fixtures.py",
            "check_local_format_edge_case_fixtures_unknown_property_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            source = REPO_ROOT / module.DEFAULT_REPORT
            target = repo_root / module.DEFAULT_REPORT
            target.parent.mkdir(parents=True, exist_ok=True)
            payload = json.loads(source.read_text(encoding="utf-8"))
            row = next(
                row
                for row in payload["rows"]
                if row["row_id"] == "csv_null_heavy_aggregate"
            )
            row["property_or_fuzz_case_refs"] = ["not_a_known_property_case"]
            target.write_text(json.dumps(payload), encoding="utf-8")

            correctness_source = REPO_ROOT / module.DEFAULT_CORRECTNESS_MATRIX
            correctness_target = repo_root / module.DEFAULT_CORRECTNESS_MATRIX
            correctness_target.parent.mkdir(parents=True, exist_ok=True)
            correctness_target.write_text(
                correctness_source.read_text(encoding="utf-8"),
                encoding="utf-8",
            )

            for ref in {
                Path(item.split("::", maxsplit=1)[0])
                for item in source.read_text(encoding="utf-8").split('"')
                if "/" in item and "." in item and not item.startswith("docs/release")
            }:
                source_path = REPO_ROOT / ref
                if not source_path.exists():
                    continue
                target_path = repo_root / ref
                target_path.parent.mkdir(parents=True, exist_ok=True)
                target_path.write_text("", encoding="utf-8")

            report = module.build_report(repo_root)

        self.assertEqual(report["status"], "failed")
        self.assertTrue(
            any(
                "csv_null_heavy_aggregate: unknown property_or_fuzz_case_ref not_a_known_property_case"
                in blocker
                for blocker in report["blockers"]
            )
        )

    def test_v1_release_boundary_accepts_claim_safe_fixture(self) -> None:
        module = self._load_script_module(
            "check_v1_release_boundary.py",
            "check_v1_release_boundary_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_v1_release_boundary_fixture(module, repo_root)
            report = module.build_report(repo_root)

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(report["claim_gate_status"], "not_claim_grade")
        self.assertFalse(report["public_package_claim_allowed"])
        self.assertFalse(report["fallback_attempted"])
        self.assertEqual(report["support_doc"]["header"]["fallback_attempted"], "false")
        self.assertIn("object-store runtime", report["unsupported_production_families"])

    def test_v1_release_boundary_blocks_stale_support_doc_no_fallback_header(self) -> None:
        module = self._load_script_module(
            "check_v1_release_boundary.py",
            "check_v1_release_boundary_support_doc_blocker_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_v1_release_boundary_fixture(module, repo_root)
            support_doc = repo_root / module.V1_SUPPORTED_DOC
            support_doc.write_text(
                support_doc.read_text(encoding="utf-8").replace(
                    "fallback_attempted=false",
                    "fallback_attempted=true",
                    1,
                ),
                encoding="utf-8",
            )
            report = module.build_report(repo_root)

        self.assertEqual(report["status"], "failed")
        self.assertTrue(
            any(
                "v1 supported doc header fallback_attempted=true" in blocker
                for blocker in report["blockers"]
            )
        )

    def test_v1_release_boundary_blocks_production_package_classifier(self) -> None:
        module = self._load_script_module(
            "check_v1_release_boundary.py",
            "check_v1_release_boundary_package_classifier_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_v1_release_boundary_fixture(module, repo_root)
            pyproject = repo_root / module.PYPROJECT
            pyproject.write_text(
                pyproject.read_text(encoding="utf-8")
                + '\n"Development Status :: 5 - Production/Stable"\n',
                encoding="utf-8",
            )
            report = module.build_report(repo_root)

        self.assertEqual(report["status"], "failed")
        self.assertTrue(
            any("forbidden marker" in blocker for blocker in report["blockers"])
        )

    def test_release_evidence_artifact_merge_restores_repo_relative_refs(self) -> None:
        module = self._load_script_module(
            "merge_release_evidence_artifacts.py",
            "merge_release_evidence_artifacts_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            artifact = repo_root / "target" / "downloads" / "release-local-smoke-evidence"
            (artifact / "release-dry-run-proof").mkdir(parents=True)
            (artifact / "release-dry-run-proof" / "transcript.json").write_text(
                "{}\n", encoding="utf-8"
            )
            (artifact / "release-provenance-dry-run").mkdir()
            provenance = (
                artifact
                / "release-provenance-dry-run"
                / "supply-chain-release-evidence.json"
            )
            provenance.write_text("{}\n", encoding="utf-8")
            (artifact / "debug").mkdir()
            (artifact / "debug" / "shardloom").write_text("binary\n", encoding="utf-8")
            (artifact / "dist").mkdir()
            (artifact / "dist" / "shardloom-0.1.0-py3-none-any.whl").write_text(
                "wheel\n", encoding="utf-8"
            )
            (artifact / "dist" / "shardloom-0.1.0.tar.gz").write_text(
                "sdist\n", encoding="utf-8"
            )

            report = module.merge_artifact(repo_root, artifact)

            self.assertEqual(report["status"], "passed", report["blockers"])
            self.assertEqual(
                report["producer_artifact_name"], "release-local-smoke-evidence"
            )
            self.assertTrue(report["downloaded_artifact_digest_bound"])
            self.assertTrue(report["artifact_tree_digest"].startswith("sha256:"))
            self.assertEqual(report["artifact_file_count"], 5)
            self.assertEqual(
                sorted(file["path"] for file in report["artifact_files"]),
                [
                    "debug/shardloom",
                    "dist/shardloom-0.1.0-py3-none-any.whl",
                    "dist/shardloom-0.1.0.tar.gz",
                    "release-dry-run-proof/transcript.json",
                    "release-provenance-dry-run/supply-chain-release-evidence.json",
                ],
            )
            self.assertIn("target/release-dry-run-proof", report["copied_paths"])
            self.assertIn("target/release-provenance-dry-run", report["copied_paths"])
            self.assertIn("target/debug", report["copied_paths"])
            self.assertIn("python/dist", report["copied_paths"])
            self.assertFalse(any(str(repo_root) in path for path in report["copied_paths"]))
            transcript = repo_root / "target" / "release-dry-run-proof" / "transcript.json"
            self.assertTrue(transcript.is_file())
            self.assertTrue(
                (
                    repo_root
                    / "target"
                    / "release-provenance-dry-run"
                    / "supply-chain-release-evidence.json"
                ).is_file()
            )
            self.assertTrue((repo_root / "target" / "debug" / "shardloom").is_file())
            self.assertTrue(os.access(repo_root / "target" / "debug" / "shardloom", os.X_OK))
            self.assertEqual(
                report["normalized_executable_paths"],
                [
                    {
                        "path": "target/debug/shardloom",
                        "before_mode": "0o644",
                        "after_mode": "0o744",
                        "permission_repair_attempted": True,
                        "owner_executable": True,
                    }
                ],
            )
            self.assertTrue(
                (
                    repo_root
                    / "python"
                    / "dist"
                    / "shardloom-0.1.0-py3-none-any.whl"
                ).is_file()
            )
            sdist = repo_root / "python" / "dist" / "shardloom-0.1.0.tar.gz"
            self.assertTrue(sdist.is_file())

    def test_release_evidence_artifact_merge_restores_compact_transcript(self) -> None:
        module = self._load_script_module(
            "merge_release_evidence_artifacts.py",
            "merge_release_evidence_artifacts_compact_transcript_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            artifact = repo_root / "target" / "downloads" / "release-local-smoke-evidence"
            artifact.mkdir(parents=True)
            (artifact / "transcript.json").write_text('{"proof_status":"passed"}\n', encoding="utf-8")

            report = module.merge_artifact(repo_root, artifact)

            self.assertEqual(report["status"], "passed", report["blockers"])
            self.assertEqual(report["artifact_file_count"], 1)
            self.assertEqual(
                report["copied_paths"],
                ["target/release-dry-run-proof/transcript.json"],
            )
            transcript = repo_root / "target" / "release-dry-run-proof" / "transcript.json"
            self.assertEqual(
                json.loads(transcript.read_text(encoding="utf-8"))["proof_status"],
                "passed",
            )

    def test_release_evidence_artifact_merge_rejects_symlinked_entries(self) -> None:
        module = self._load_script_module(
            "merge_release_evidence_artifacts.py",
            "merge_release_evidence_artifacts_symlink_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            artifact = repo_root / "target" / "downloads" / "release-local-smoke-evidence"
            artifact.mkdir(parents=True)
            (artifact / "outside").symlink_to(Path("/tmp"))

            report = module.merge_artifact(repo_root, artifact)

            self.assertEqual(report["status"], "failed")
            self.assertFalse(report["downloaded_artifact_digest_bound"])
            self.assertEqual(report["copied_paths"], [])
            self.assertIn("artifact contains unsupported symlink", report["blockers"][0])

    def test_release_example_proof_uses_benchmark_runtime_features(self) -> None:
        example_module = self._load_script_module(
            "check_v1_example_replay.py",
            "check_v1_example_replay_features_for_test",
        )
        dry_run_module = self._load_script_module(
            "release_dry_run_proof.py",
            "release_dry_run_proof_features_for_test",
        )

        self.assertEqual(example_module.DEFAULT_FEATURES, RELEASE_USER_SURFACE_EXAMPLE_FEATURES)
        self.assertEqual(
            dry_run_module.RELEASE_USER_SURFACE_EXAMPLE_FEATURES,
            RELEASE_USER_SURFACE_EXAMPLE_FEATURES,
        )
        self.assertIn("release-user-surfaces", RELEASE_USER_SURFACE_EXAMPLE_FEATURES)

    def test_v1_example_replay_accepts_metadata_reopen_proof(self) -> None:
        module = self._load_script_module(
            "check_v1_example_replay.py",
            "check_v1_example_replay_metadata_reopen_for_test",
        )

        workflow = {
            "workflow_id": "local_csv_jsonl_to_vortex_ingest_prepared_query_jsonl_csv_output",
            "stages": [
                {
                    "stage_id": "local_csv_vortex_ingest",
                    "selected_fields": {
                        "reopen_verification_status": "reopen_metadata_row_count_verified"
                    },
                }
            ],
        }

        self.assertTrue(module.workflow_replay_verified(workflow))

    def test_v1_example_replay_validates_current_documentation(self) -> None:
        module = self._load_script_module(
            "check_v1_example_replay.py", "example_replay_current_docs_for_test"
        )
        summary, blockers = module.validate_doc_markers(REPO_ROOT)
        self.assertEqual(blockers, [])
        self.assertEqual(summary["status"], "passed")

    def test_v1_local_resource_safety_normalizes_downloaded_binary_permissions(
        self,
    ) -> None:
        module = self._load_script_module(
            "check_v1_local_resource_safety.py",
            "check_v1_local_resource_safety_permissions_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            binary = repo_root / "target" / "debug" / "shardloom"
            binary.parent.mkdir(parents=True)
            binary.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            binary.chmod(0o644)

            report, blockers = module.ensure_binary(
                repo_root,
                binary=binary,
                features=module.DEFAULT_FEATURES,
                skip_build=True,
                explicit_binary=False,
            )

            self.assertEqual(blockers, [])
            self.assertEqual(report["status"], "passed")
            self.assertTrue(report["binary_executable_permission_normalized"])
            self.assertTrue(os.access(binary, os.X_OK))

    def test_local_python_smoke_runs_user_surface_quickstart(self) -> None:
        module = self._load_module_from_path(
            REPO_ROOT / "examples" / "local-python-smoke" / "run.py",
            "local_python_smoke_for_test",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            python_src = repo_root / "python" / "src"
            python_src.mkdir(parents=True)
            (python_src / "shardloom").symlink_to(
                REPO_ROOT / "python" / "src" / "shardloom",
                target_is_directory=True,
            )
            fake_cli = repo_root / "fake_shardloom.py"
            fake_cli.write_text(
                "#!/usr/bin/env python3\n"
                + textwrap.dedent(
                    """
                    import json, sys
                    from pathlib import Path

                    args = sys.argv[1:]

                    def emit(command, fields, *, status="success", diagnostics=None, returncode=0):
                        print(json.dumps({
                            "schema_version": "shardloom.output.v2",
                            "command": command,
                            "status": status,
                            "summary": "ok",
                            "human_text": "ok",
                            "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                            "diagnostics": diagnostics or [],
                            "fields": fields + [
                                {"key": "fallback_attempted", "value": "false"},
                                {"key": "external_engine_invoked", "value": "false"},
                            ],
                            "result": {"fields": fields},
                            "result_refs": [],
                            "artifacts": [],
                            "artifact_refs": [],
                            "certificates": [],
                            "policy": {"fields": []},
                            "lifecycle": {"fields": []},
                            "capability_snapshot": {"fields": []},
                        }))
                        sys.exit(returncode)

                    if args == ["status", "--format", "json"]:
                        emit("status", [{"key": "engine", "value": "shardloom"}])
                    if args == ["capabilities", "--format", "json"]:
                        emit("capabilities", [{"key": "scope", "value": "default"}])
                    if args == ["capabilities", "python", "--format", "json"]:
                        emit("capabilities", [{"key": "scope", "value": "python"}])
                    if args == ["capabilities", "deployment", "--format", "json"]:
                        emit("capabilities", [{"key": "scope", "value": "deployment"}])
                    if args == ["input-adapters", "--format", "json"]:
                        emit("input-adapters", [{"key": "plan_only", "value": "true"}])
                    if args[0] == "run":
                        assert args[1] == "dataframe", args
                        def value(flag):
                            return args[args.index(flag) + 1]
                        if value("--request") == "collect":
                            source = Path(value("--input"))
                            assert source.parent.parent == Path(__file__).resolve().parent / "target/local-python-smoke", args
                            assert source.parent.name.startswith("run-"), args
                            assert source.name == "orders.csv" and source.is_file(), args
                            assert value("--input-format") == "csv", args
                            bindings = json.loads(value("--source-bindings"))
                            assert bindings == {str(source): {"input_format": "csv"}}, args
                            assert value("--sql") == (
                                f"SELECT id,label,amount FROM '{source}' "
                                "WHERE amount >= 10 LIMIT 2"
                            ), args
                            assert args[args.index("--request") + 1] == "collect", args
                            result_rows = [
                                {"id": 2, "label": "beta", "amount": 15},
                                {"id": 3, "label": "gamma", "amount": 27},
                            ]
                            result_schema = {"Struct": [{"names": ["id", "label", "amount"], "dtypes": [
                                {"Primitive": ["i64", False]}, {"Utf8": False}, {"Primitive": ["i64", False]},
                            ]}, False]}
                            emit("run", [
                                {"key": "result_schema_json", "value": json.dumps(result_schema)},
                                {"key": "result_schema_format", "value": "vortex.dtype.serde.v1"},
                                {"key": "result_values_json", "value": json.dumps(result_rows)},
                                {"key": "runtime_execution", "value": "true"},
                                {"key": "data_read", "value": "true"},
                                {"key": "output_row_count", "value": "2"},
                                {"key": "public_workflow_native_vortex_plan_route_family", "value": "native_vortex_unified_plan"},
                                {"key": "resident_source_opens", "value": "1"},
                                {"key": "claim_gate_status", "value": "not_claim_grade"},
                            ])
                        assert value("--request") == "write_jsonl", args
                        output_path = Path(value("--output"))
                        assert output_path.parent.parent == Path(__file__).resolve().parent / "target/local-python-smoke", args
                        assert output_path.parent.name.startswith("run-"), args
                        assert output_path.name == "generated-reference.jsonl", args
                        assert not output_path.exists(), args
                        assert "--allow-overwrite" not in args, args
                        bindings = json.loads(value("--source-bindings"))
                        assert len(bindings) == 1, args
                        source_uri, declaration = next(iter(bindings.items()))
                        assert declaration == {
                            "input_format": "memory",
                            "memory_input": {
                                "kind": "rows",
                                "schema": [["id", "int64"], ["label", "utf8"]],
                                "rows": [["1", "alpha"]],
                            },
                        }, args
                        assert value("--sql") == (
                            f"SELECT id,label,1 AS batch_id FROM "
                            f"(SELECT * FROM '{source_uri}') AS _sl_stage_0"
                        ), args
                        output_path.parent.mkdir(parents=True, exist_ok=True)
                        output_path.write_text('{"id":1,"label":"alpha","batch_id":1}\\n', encoding="utf-8")
                        output_schema = {"Struct": [{"names": ["id", "label", "batch_id"], "dtypes": [
                            {"Primitive": ["i64", False]}, {"Utf8": False}, {"Primitive": ["i64", False]},
                        ]}, False]}
                        output_rows = [{"id": 1, "label": "alpha", "batch_id": 1}]
                        emit("run", [
                            {"key": "result_schema_json", "value": json.dumps(output_schema)},
                            {"key": "result_schema_format", "value": "vortex.dtype.serde.v1"},
                            {"key": "result_values_json", "value": json.dumps(output_rows)},
                            {"key": "native_vortex_result_export_path", "value": str(output_path)},
                            {"key": "native_vortex_result_export_format", "value": "jsonl"},
                            {"key": "native_vortex_result_export_rows_written", "value": "1"},
                            {"key": "native_vortex_result_export_all_targets_committed", "value": "true"},
                            {"key": "public_workflow_native_vortex_plan_route_family", "value": "native_vortex_unified_plan"},
                            {"key": "resident_source_opens", "value": "0"},
                            {"key": "output_row_count", "value": "1"},
                            {"key": "output_io_performed", "value": "true"},
                            {"key": "runtime_execution", "value": "true"},
                            {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                        ])
                    if args[0] == "workflow-unsupported-plan":
                        assert args[1] == "apply", args
                        assert args[2].startswith("read_csv(") and args[2].endswith(") -> select(id)"), args
                        source = Path(args[2][len("read_csv("):-len(") -> select(id)")])
                        assert source.parent.parent == Path(__file__).resolve().parent / "target/local-python-smoke", args
                        assert source.name == "orders.csv" and source.is_file(), args
                        assert args[3] == "callable=row_udf", args
                        emit("workflow-unsupported-plan", [
                            {"key": "blocker_id", "value": "cg21.workflow.apply.python_callable_unsupported"},
                            {"key": "runtime_execution", "value": "false"},
                            {"key": "data_read", "value": "false"},
                            {"key": "write_io", "value": "false"},
                            {"key": "claim_gate_status", "value": "not_claim_grade"},
                        ], status="unsupported", diagnostics=[{
                            "code": "SL_UNSUPPORTED_WORKFLOW_OPERATION",
                            "severity": "error",
                            "category": "unsupported_feature",
                            "message": "unsupported",
                            "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                        }], returncode=1)
                    raise AssertionError(args)
                    """
                ),
                encoding="utf-8",
            )
            fake_cli.chmod(0o755)

            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                returncode = module.main(
                    ["--repo-root", str(repo_root), "--shardloom-bin", str(fake_cli)]
                )

            output = stdout.getvalue()
            self.assertEqual(returncode, 0, output)
            self.assertIn("quickstart_user_surface_status=passed", output)
            self.assertIn("quickstart_local_file_blocker_id=none", output)
            self.assertIn("quickstart_local_file_route_status=passed", output)
            self.assertIn("quickstart_local_file_runtime_execution=true", output)
            self.assertIn("quickstart_local_file_native_plan_family=native_vortex_unified_plan", output)
            self.assertIn("quickstart_local_file_source_opens=1", output)
            self.assertIn("quickstart_local_file_output_row_count=2", output)
            self.assertIn("quickstart_local_file_fallback_attempted=false", output)
            self.assertIn("quickstart_local_file_external_engine_invoked=false", output)
            self.assertIn(
                "quickstart_local_file_result_rows=({'id': 2, 'label': 'beta', 'amount': 15}, {'id': 3, 'label': 'gamma', 'amount': 27})",
                output,
            )
            self.assertIn("quickstart_generated_input_row_count=1", output)
            self.assertIn("quickstart_generated_native_plan_family=native_vortex_unified_plan", output)
            self.assertIn("quickstart_generated_source_opens=0", output)
            self.assertIn("quickstart_generated_result_verified=true", output)
            self.assertIn("quickstart_generated_rows_written=1", output)
            generated_path = Path(next(
                line.split("=", 1)[1] for line in output.splitlines()
                if line.startswith("quickstart_generated_output_path=")
            ))
            self.assertEqual(generated_path.parent.parent, repo_root.resolve() / "target/local-python-smoke")
            self.assertEqual(generated_path.name, "generated-reference.jsonl")
            prior_output = generated_path.read_bytes()
            repeated_stdout = io.StringIO()
            with contextlib.redirect_stdout(repeated_stdout):
                self.assertEqual(module.main(
                    ["--repo-root", str(repo_root), "--shardloom-bin", str(fake_cli)]
                ), 0)
            repeated_path = Path(next(
                line.split("=", 1)[1] for line in repeated_stdout.getvalue().splitlines()
                if line.startswith("quickstart_generated_output_path=")
            ))
            self.assertNotEqual(generated_path, repeated_path)
            self.assertEqual(generated_path.read_bytes(), prior_output)
            self.assertEqual(repeated_path.read_bytes(), prior_output)
            self.assertIn("quickstart_generated_output_row_count=1", output)
            self.assertIn("quickstart_generated_output_commit_status=committed", output)
            self.assertIn("quickstart_generated_fallback_attempted=false", output)
            self.assertIn("quickstart_generated_external_engine_invoked=false", output)
            self.assertIn(
                "quickstart_generated_claim_gate_status=fixture_smoke_only", output
            )
            self.assertIn(
                "quickstart_unsupported_blocker_id=cg21.workflow.apply.python_callable_unsupported",
                output,
            )
            self.assertIn("quickstart_unsupported_external_engine_invoked=false", output)
            self.assertIn("quickstart_unsupported_runtime_execution=false", output)
            self.assertIn("quickstart_unsupported_data_read=false", output)
            self.assertIn("quickstart_unsupported_write_io=false", output)
            self.assertIn("quickstart_unsupported_fallback_attempted=false", output)
            replay = self._load_script_module(
                "check_v1_example_replay.py", "example_replay_actual_quickstart_for_test"
            )
            summary, blockers = replay.validate_quickstart(
                {"stdout_tail": output, "returncode": returncode}
            )
            self.assertEqual(blockers, [])
            self.assertTrue(summary["local_file_vortex_collect_present"])
            self.assertTrue(summary["unsupported_fixture_present"])
            for line in output.splitlines():
                if not line.startswith("quickstart_"):
                    continue
                with self.subTest(missing_quickstart_line=line):
                    _, blockers = replay.validate_quickstart(
                        {"stdout_tail": output.replace(line + "\n", ""), "returncode": 0}
                    )
                    self.assertTrue(blockers)
            for old, new in (
                ("quickstart_local_file_source_opens=1", "quickstart_local_file_source_opens=10"),
                ("quickstart_generated_source_opens=0", "quickstart_generated_source_opens=01"),
                ("quickstart_generated_result_verified=true", "quickstart_generated_result_verified=trueish"),
                ("quickstart_generated_claim_gate_status=fixture_smoke_only", "quickstart_generated_claim_gate_status="),
            ):
                with self.subTest(invalid_quickstart_line=new):
                    _, blockers = replay.validate_quickstart(
                        {"stdout_tail": output.replace(old, new), "returncode": 0}
                    )
                    self.assertTrue(blockers)
            _, duplicate_blockers = replay.validate_quickstart(
                {"stdout_tail": output + "quickstart_local_file_source_opens=2\n", "returncode": 0}
            )
            self.assertTrue(duplicate_blockers)
            self.assertFalse(
                (repo_root / "target" / "local-python-smoke" / "orders-out.jsonl").exists()
            )
            self.assertEqual(
                [json.loads(line) for line in generated_path.read_text(encoding="utf-8").splitlines()],
                [{"id": 1, "label": "alpha", "batch_id": 1}],
            )
            original_cli = fake_cli.read_text(encoding="utf-8")
            for original, replacement in (
                ('"public_workflow_native_vortex_plan_route_family"', '"missing_plan_family"'),
                ('"native_vortex_unified_plan"', '"unadmitted_plan_family"'),
                ('"resident_source_opens", "value": "1"', '"resident_source_opens", "value": "2"'),
                ('"resident_source_opens", "value": "0"', '"resident_source_opens", "value": "1"'),
            ):
                with self.subTest(missing_or_invalid_evidence=original):
                    self.assertIn(original, original_cli)
                    fake_cli.write_text(original_cli.replace(original, replacement), encoding="utf-8")
                    stdout = io.StringIO()
                    with contextlib.redirect_stdout(stdout):
                        returncode = module.main(
                            ["--repo-root", str(repo_root), "--shardloom-bin", str(fake_cli)]
                        )
                    self.assertEqual(returncode, 1, stdout.getvalue())
                    self.assertIn("quickstart_user_surface_status=failed", stdout.getvalue())

    def test_release_dry_run_transcript_records_user_surface_quickstart_markers(self) -> None:
        module = self._load_script_module(
            "release_dry_run_proof.py", "release_dry_run_proof_for_test"
        )
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            transcript = repo_root / "target" / "release-dry-run-proof" / "transcript.json"
            steps = [
                {
                    "name": "example_local_python_smoke",
                    "returncode": 0,
                    "stdout": "\n".join(
                        [
                            "quickstart_user_surface_status=passed",
                            "quickstart_local_file_blocker_id=none",
                            "quickstart_local_file_route_status=passed",
                            "quickstart_local_file_runtime_execution=true",
                            "quickstart_local_file_native_plan_family=native_vortex_unified_plan",
                            "quickstart_local_file_source_opens=1",
                            "quickstart_local_file_output_row_count=2",
                            "quickstart_local_file_fallback_attempted=false",
                            "quickstart_local_file_external_engine_invoked=false",
                            "quickstart_local_file_result_rows=({'id': 2, 'label': 'beta', 'amount': 15}, {'id': 3, 'label': 'gamma', 'amount': 27})",
                            "quickstart_generated_input_row_count=1",
                            "quickstart_generated_native_plan_family=native_vortex_unified_plan",
                            "quickstart_generated_source_opens=0",
                            "quickstart_generated_result_verified=true",
                            "quickstart_generated_rows_written=1",
                            "quickstart_generated_output_path=target/local-python-smoke/generated-reference.jsonl",
                            "quickstart_generated_output_row_count=1",
                            "quickstart_generated_output_commit_status=committed",
                            "quickstart_generated_fallback_attempted=false",
                            "quickstart_generated_external_engine_invoked=false",
                            "quickstart_generated_claim_gate_status=fixture_smoke_only",
                            "quickstart_unsupported_blocker_id=cg21.workflow.apply.python_callable_unsupported",
                            "quickstart_unsupported_runtime_execution=false",
                            "quickstart_unsupported_data_read=false",
                            "quickstart_unsupported_write_io=false",
                            "quickstart_unsupported_fallback_attempted=false",
                            "quickstart_unsupported_external_engine_invoked=false",
                        ]
                    ),
                    "stderr": "",
                }
            ]

            module.write_transcript(
                repo_root=repo_root,
                output=transcript,
                venv_dir=repo_root / "venv",
                conda_env_dir=repo_root / "conda",
                binary=repo_root / "target" / "debug" / "shardloom",
                wheel=repo_root / "python" / "dist" / "shardloom.whl",
                steps=steps,
                passed=True,
                clean_conda_status="skipped_tool_missing",
                clean_conda_tool=None,
                clean_conda_required=False,
                package_python=repo_root / "tools" / "python3.12",
                package_python_version="3.12.13",
                clean_conda_python_version="3.12",
            )

            report = json.loads(transcript.read_text(encoding="utf-8"))
            self.assertTrue(report["local_python_user_surface_quickstart_performed"])
            self.assertTrue(report["local_python_result_and_evidence_printed"])
            self.assertTrue(report["local_python_unsupported_path_evidence_printed"])
            self.assertEqual(report["repo_root"], "repo")
            self.assertEqual(report["clean_venv"], "venv")
            self.assertEqual(report["clean_conda_env"], "conda")
            self.assertEqual(report["local_cli_binary"], "target/debug/shardloom")
            self.assertEqual(report["local_wheel"], "python/dist/shardloom.whl")
            self.assertEqual(report["package_python"], "tools/python3.12")
            self.assertEqual(report["package_python_version"], "3.12.13")
            self.assertEqual(report["package_python_min_version"], "3.10")
            self.assertEqual(report["clean_conda_env_python_version_requested"], "3.12")
            self.assertNotIn(str(repo_root), json.dumps(report, sort_keys=True))

    @unittest.skipIf(sys.platform == "win32", "symlink creation requires Windows privileges")
    def test_release_dry_run_preserves_repo_build_symlink_reference(self) -> None:
        module = self._load_script_module(
            "release_dry_run_proof.py", "release_dry_run_symlink_reference_for_test"
        )
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir).resolve()
            repo = root / "repo"
            cache = root / "local-cache"
            (repo / "target").mkdir(parents=True)
            cache.mkdir()
            (cache / "shardloom").write_bytes(b"native-binary-fixture")
            (repo / "target" / "debug").symlink_to(cache, target_is_directory=True)
            ref = module.transcript_path_ref(repo, repo / "target/debug/shardloom")
            self.assertEqual(ref, "target/debug/shardloom")
            self.assertEqual((repo / ref).read_bytes(), b"native-binary-fixture")
            self.assertEqual(
                module.transcript_path_ref(repo, cache / "shardloom"),
                "external-path:shardloom",
            )

    def test_release_dry_run_transcript_redacts_command_paths(self) -> None:
        module = self._load_script_module(
            "release_dry_run_proof.py", "release_dry_run_proof_command_redaction_for_test"
        )

        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            command = [
                str(repo_root / "target" / "debug" / "shardloom"),
                "status",
                str(repo_root / "target" / "release-dry-run-proof" / "venv"),
                str(Path("/usr/local/bin/python3")),
            ]

            redacted = module.redact_command_for_transcript(repo_root, command)

            self.assertEqual(redacted[0], "target/debug/shardloom")
            self.assertEqual(redacted[2], "target/release-dry-run-proof/venv")
            self.assertEqual(redacted[3], "external-path:python3")
            self.assertNotIn(str(repo_root), " ".join(redacted))

    def test_release_dry_run_selects_package_python_satisfying_requires_python(self) -> None:
        module = self._load_script_module(
            "release_dry_run_proof.py", "release_dry_run_proof_python_selection_for_test"
        )
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            py39 = root / "python3.9"
            py312 = root / "python3.12"
            py39.write_text("#!/bin/sh\n", encoding="utf-8")
            py312.write_text("#!/bin/sh\n", encoding="utf-8")

            def fake_runner(command, **_kwargs):  # type: ignore[no-untyped-def]
                executable = Path(command[0])
                version = "Python 3.9.6"
                if executable == py312.resolve():
                    version = "Python 3.12.13"
                return subprocess.CompletedProcess(command, 0, stdout=version, stderr="")

            selected, version = module.select_package_python(
                [py39, py312],
                runner=fake_runner,
            )

            self.assertEqual(selected, py312.resolve())
            self.assertEqual(version, "3.12.13")

    def test_release_dry_run_clean_conda_python_matches_package_wheel_by_default(self) -> None:
        module = self._load_script_module(
            "release_dry_run_proof.py",
            "release_dry_run_proof_conda_python_selection_for_test",
        )

        self.assertEqual(
            module.conda_python_version_for_package_wheel("match-package", "3.12.13"),
            "3.12",
        )
        self.assertEqual(
            module.conda_python_version_for_package_wheel("auto", "3.13.1"),
            "3.13",
        )
        self.assertEqual(
            module.conda_python_version_for_package_wheel("3.11", "3.12.13"),
            "3.11",
        )

    def test_release_dry_run_python_artifact_build_falls_back_to_wheel_and_sdist(self) -> None:
        module = self._load_script_module(
            "release_dry_run_proof.py", "release_dry_run_proof_build_fallback_for_test"
        )
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            package_dir = repo_root / "python"
            package_dir.mkdir()
            dist_dir = repo_root / "python" / "dist"
            dist_dir.mkdir()
            stale_wheel = dist_dir / "shardloom-0.0.0-py3-none-any.whl"
            stale_wheel.write_text("stale", encoding="utf-8")
            (package_dir / "setup.cfg").write_text(
                "[bdist_wheel]\nplat_name = macosx_15_0_arm64\n", encoding="utf-8"
            )
            commands: list[list[str]] = []

            def fake_run_step(*, name, command, cwd, env=None):  # type: ignore[no-untyped-def]
                commands.append(command)
                self.assertEqual(env["SHARDLOOM_WHEEL_PLAT_NAME"], "macosx_15_0_arm64")
                if command[:3] == [sys.executable, "-m", "build"]:
                    return {
                        "name": name,
                        "command": command,
                        "returncode": 1,
                        "stdout": "",
                        "stderr": f"{sys.executable}: No module named build\n",
                    }
                if name == "build_python_artifacts":
                    (dist_dir / "shardloom-0.1.0-py3-none-any.whl").write_text(
                        "wheel", encoding="utf-8"
                    )
                if name == "build_python_artifacts_sdist":
                    (dist_dir / "shardloom-0.1.0.tar.gz").write_text(
                        "sdist", encoding="utf-8"
                    )
                return {
                    "name": name,
                    "command": command,
                    "returncode": 0,
                    "stdout": "wheel built",
                    "stderr": "",
                }

            original_run_step = module.run_step
            module.run_step = fake_run_step
            try:
                step = module.build_python_artifacts(repo_root, package_dir, dist_dir)
            finally:
                module.run_step = original_run_step

            self.assertEqual(step["returncode"], 0)
            self.assertEqual(
                step["build_backend"],
                "pip_wheel_and_setuptools_sdist_no_build_isolation",
            )
            self.assertEqual(step["fallback_reason"], "python_build_frontend_missing")
            self.assertFalse(stale_wheel.exists())
            self.assertEqual(
                commands[0], [sys.executable, "-m", "build", str(package_dir)]
            )
            self.assertEqual(commands[1][:4], [sys.executable, "-m", "pip", "wheel"])
            self.assertIn("--no-build-isolation", commands[1])
            self.assertIn("--no-deps", commands[1])
            self.assertEqual(commands[2][:2], [sys.executable, "-c"])
            self.assertIn("build_sdist", commands[2][2])
            self.assertEqual(step["python_artifact_blockers"], [])
            self.assertEqual(len(step["fallback_steps"]), 2)

    def test_bundled_macos_wheel_pins_architecture_in_build_options(self) -> None:
        import configparser
        from unittest.mock import patch

        module = self._load_script_module(
            "release_dry_run_proof.py", "release_dry_run_macos_wheel_platform_for_test"
        )
        with tempfile.TemporaryDirectory() as tempdir, patch.dict(
            module.os.environ, {"MACOSX_DEPLOYMENT_TARGET": ""}
        ):
            stage = Path(tempdir)
            config_path = stage / "setup.cfg"
            config_path.write_text("[metadata]\nname = retained\n[bdist_wheel]\nuniversal = 0\n")
            for platform_tag, os_version, expected in [
                ("macos-aarch64", "15.7.1", "macosx_15_0_arm64"),
                ("macos-x86_64", "10.15.7", "macosx_10_15_x86_64"),
            ]:
                with patch.object(module.platform, "mac_ver", return_value=(os_version, (), "")):
                    self.assertEqual(module.pin_bundled_macos_wheel_platform(stage, platform_tag), expected)
                config = configparser.ConfigParser()
                config.read(config_path)
                self.assertEqual(config["bdist_wheel"]["plat_name"], expected)
                self.assertEqual(config["bdist_wheel"]["universal"], "0")
                self.assertEqual(config["metadata"]["name"], "retained")
            with patch.object(module.platform, "mac_ver", return_value=("26.5.1", (), "")):
                for target, expected in [
                    ("11", "macosx_11_0_arm64"),
                    ("11.0", "macosx_11_0_arm64"),
                    ("15.4", "macosx_15_0_arm64"),
                ]:
                    with patch.dict(module.os.environ, {"MACOSX_DEPLOYMENT_TARGET": target}):
                        self.assertEqual(
                            module.pin_bundled_macos_wheel_platform(stage, "macos-aarch64"),
                            expected,
                        )
                with patch.dict(module.os.environ, {"MACOSX_DEPLOYMENT_TARGET": "invalid"}):
                    with self.assertRaises(OSError):
                        module.pin_bundled_macos_wheel_platform(stage, "macos-aarch64")
            before = config_path.read_bytes()
            self.assertIsNone(module.pin_bundled_macos_wheel_platform(stage, "linux-x86_64"))
            self.assertEqual(config_path.read_bytes(), before)
            with patch.object(module.platform, "mac_ver", return_value=("", (), "")):
                with self.assertRaises(OSError):
                    module.pin_bundled_macos_wheel_platform(stage, "macos-aarch64")
            with self.assertRaises(OSError):
                module.pin_bundled_macos_wheel_platform(stage, "macos-unknown")

    def test_release_dry_run_stages_built_artifacts_for_provenance(self) -> None:
        module = self._load_script_module(
            "release_dry_run_proof.py",
            "release_dry_run_proof_provenance_artifact_staging_for_test",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            built_dist = repo_root / "target" / "release-dry-run-proof" / "python-package-stage" / "dist"
            built_dist.mkdir(parents=True)
            wheel = built_dist / "shardloom-0.2.0-py3-none-any.whl"
            sdist = built_dist / "shardloom-0.2.0.tar.gz"
            wheel.write_text("wheel", encoding="utf-8")
            sdist.write_text("sdist", encoding="utf-8")
            target_dist = repo_root / "python" / "dist"
            target_dist.mkdir(parents=True)
            stale = target_dist / "shardloom-0.1.10-py3-none-any.whl"
            stale.write_text("stale", encoding="utf-8")

            step = module.stage_python_artifacts_for_provenance(repo_root, built_dist)

            self.assertEqual(step["returncode"], 0)
            self.assertEqual(step["python_artifact_blockers"], [])
            self.assertFalse(stale.exists())
            self.assertTrue((target_dist / wheel.name).exists())
            self.assertTrue((target_dist / sdist.name).exists())
            self.assertEqual(
                step["copied_artifacts"],
                [f"python/dist/{wheel.name}", f"python/dist/{sdist.name}"],
            )

    def test_release_dry_run_cleanup_rejects_repo_root_and_top_level_targets(self) -> None:
        module = self._load_script_module(
            "release_dry_run_proof.py", "release_dry_run_proof_cleanup_guard_for_test"
        )
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            sentinel = repo_root / "sentinel.txt"
            sentinel.write_text("keep", encoding="utf-8")
            target_dir = repo_root / "target"
            target_dir.mkdir()
            nested_env = target_dir / "release-dry-run-proof" / "venv"
            nested_env.mkdir(parents=True)
            (nested_env / "pyvenv.cfg").write_text("home = test\n", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "repository root"):
                module.remove_tree_under_repo(repo_root, repo_root)
            with self.assertRaisesRegex(ValueError, "protected repository directory"):
                module.remove_tree_under_repo(repo_root, target_dir)

            self.assertTrue(sentinel.exists())
            self.assertTrue(target_dir.exists())

            module.remove_tree_under_repo(repo_root, nested_env)

            self.assertFalse(nested_env.exists())
            self.assertTrue(target_dir.exists())

    def _write_production_usability_docs(self, repo_root: Path) -> None:
        docs = {
            "README.md": (
                "https://shardloom.io/field-guide/start-local-proof/\n"
                "https://shardloom.io/field-guide/python-surface/\n"
                "docs/release/public-status-matrix.md\n"
            ),
            "docs/getting-started/install.md": (
                "python scripts\\release_dry_run_proof.py --rows 64 --iterations 1\n"
                "pip --no-index\n"
                "SHARDLOOM_BIN\n"
            ),
            "docs/getting-started/first-10-minutes.md": (
                "python scripts\\release_dry_run_proof.py --rows 64 --iterations 1\n"
                "ctx.from_rows\nctx.read\nquickstart_local_file_blocker_id\n"
                "quickstart_generated_output_row_count\nctx.range\n"
                "public package release\n"
            ),
            "docs/release/release-dry-run-proof.md": (
                "clean virtual environment\n"
                "local_python_user_surface_quickstart_performed=true\n"
                "generated_source_user_rows_runtime_performed=true\n"
                "benchmark_smoke_required_for_package_release=false\n"
            ),
            "docs/release/production-usability-gate.md": (
                "shardloom.production_usability_gate.v1\n"
                "python scripts\\check_production_usability_gate.py\n"
                "public_release_claim_allowed=false\n"
            ),
            "docs/release/package-channel-readiness-matrix.md": (
                "Package Channel Readiness Matrix\nscripts/release_dry_run_proof.py\n"
            ),
            "docs/release/hard-release-readiness-gate.md": (
                "public_release_claim_allowed=false\nclean_conda_env_install_status=passed\n"
            ),
            "docs/release/known-unsupported-paths.md": (
                "fallback_attempted=false\nexternal_engine_invoked=false\n"
            ),
            "website-src/src/pages/start.astro": (
                "release_dry_run_proof.py\ncheck_production_usability_gate.py\n"
            ),
            "SECURITY.md": "security policy\n",
            "LICENSE": "Apache-2.0\n",
            "NOTICE": "ShardLoom\n",
            "python/pyproject.toml": 'license-files = ["LICENSE", "NOTICE"]\n',
        }
        for relative, content in docs.items():
            path = repo_root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")

    def _production_usability_payloads(self, module: object, repo_root: Path) -> dict[str, object]:
        wheel = repo_root / "python" / "dist" / "shardloom-0.1.0-py3-none-any.whl"
        binary = repo_root / "target" / "debug" / "shardloom.exe"
        wheel.parent.mkdir(parents=True, exist_ok=True)
        binary.parent.mkdir(parents=True, exist_ok=True)
        wheel.write_text("wheel", encoding="utf-8")
        binary.write_text("binary", encoding="utf-8")
        false_fields = {
            "publication_attempted": False,
            "tag_created": False,
            "secrets_required": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
        }
        rows = [
            {
                "id": row_id,
                "support_state": "executable",
                "claim_gate_status": "claim_safe_discovery",
                "fallback_attempted": False,
                "external_engine_invoked": False,
            }
            for row_id in [
                "cli_status_capability_reports",
                "python_status_capabilities",
                "python_generated_source_helpers",
                "cli_public_native_workflow",
            ]
        ]
        rows.extend(
            [
                {
                    "id": row_id,
                    "support_state": "blocked",
                    "claim_gate_status": "not_claim_grade",
                    "fallback_attempted": False,
                    "external_engine_invoked": False,
                }
                for row_id in [
                    "claim_production_readiness",
                    "claim_object_store_lakehouse_foundry_production",
                ]
            ]
        )
        rows.append(
            {
                "id": "claim_package_publication",
                "support_state": "executable",
                "claim_gate_status": "package_access_only",
                "fallback_attempted": False,
                "external_engine_invoked": False,
            }
        )
        rows.extend(
            {
                "id": f"dummy_{index}",
                "support_state": "report_only",
                "claim_gate_status": "not_claim_grade",
                "fallback_attempted": False,
                "external_engine_invoked": False,
            }
            for index in range(14)
        )
        return {
            "dry_run": {
                "schema_version": "shardloom.release_dry_run_proof.v1",
                "proof_status": "passed",
                "clean_venv_install_status": "passed",
                "clean_conda_env_install_status": "skipped_tool_missing",
                "clean_conda_env_install_required": False,
                "local_wheel": str(wheel),
                "local_cli_binary": str(binary),
                "publication_attempted": False,
                "tag_created": False,
                "secrets_required": False,
                "external_runtime_dependencies_added": False,
                "fallback_engine_dependency_added": False,
                "fallback_attempted": False,
                "external_engine_invoked": False,
                "public_package_release_claim_allowed": False,
                "wheel_import_and_client_smoke_performed": True,
                "cli_status_smoke_performed": True,
                "cli_capabilities_smoke_performed": True,
                "local_python_example_smoke_performed": True,
                "local_python_user_surface_quickstart_performed": True,
                "local_python_result_and_evidence_printed": True,
                "local_python_unsupported_path_evidence_printed": True,
                "generated_output_proof_distinct_from_no_dataset_smoke": True,
                "generated_source_user_rows_runtime_performed": True,
                "generated_source_range_runtime_performed": True,
                "benchmark_smoke_required_for_package_release": False,
                "benchmark_smoke_status": "skipped_not_required_for_package_release",
                "provenance_dry_run_performed": True,
                "sbom_checksum_manifest_generated": True,
                "steps": [
                    {"name": name, "returncode": 0}
                    for name in module.DRY_RUN_REQUIRED_STEPS
                ],
            },
            "package_report": {
                "schema_version": "shardloom.package_channel_readiness_report.v1",
                "status": "passed",
                "local_gate_evidence_required": True,
                "local_gate_evidence_status": "passed",
                "package_identity_contract_status": "passed",
                "public_package_release_claim_allowed": True,
                "ready_channel_count": 4,
                "expected_channel_count": 9,
                **false_fields,
            },
            "release_security": {
                "schema_version": "shardloom.release_security_gate_report.v1",
                "status": "passed",
                "blockers": [],
                **false_fields,
            },
            "contribution_governance": {
                "schema_version": "shardloom.contribution_governance_report.v1",
                "status": "passed",
                "blockers": [],
                **false_fields,
            },
            "final_rehearsal": {
                "schema_version": "shardloom.final_release_rehearsal_report.v1",
                "status": "blocked",
                "rehearsal_status": "blocked",
                "claim_gate_status": "not_claim_grade",
                "local_artifacts_only": True,
                "public_release_claim_allowed": False,
                "public_package_claim_allowed": False,
                "publication_authorization_status": (
                    SELECTED_V0_1_0_PUBLICATION_AUTHORIZATION_STATUS
                ),
                "publication_human_approved": True,
                "signing_key_used": False,
                "blockers": ["hard release claim still blocked"],
                **false_fields,
            },
            "website_report": {
                "schema_version": "shardloom.website_readiness.v3",
                "checked_pages": ["start.html"],
                "checked_assets": ["assets/site.css"],
                "blockers": [],
            },
            "runs_today": {
                "schema_version": "shardloom.runs_today_support_matrix.v1",
                "all_rows_no_fallback_no_external_engine": True,
                "performance_claim_allowed": False,
                "support_state_counts": {"blocked": 2},
                "rows": rows,
            },
        }

    def test_production_usability_gate_accepts_local_no_publication_evidence(self) -> None:
        module = self._load_script_module(
            "check_production_usability_gate.py", "check_production_usability_gate_for_test"
        )
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_production_usability_docs(repo_root)
            payloads = self._production_usability_payloads(module, repo_root)

            report = module.build_report(
                repo_root=repo_root,
                release_dry_run_ref="target/release-dry-run-proof/transcript.json",
                package_channel_report_ref="target/package-channel-readiness-report.json",
                release_security_report_ref="target/release-security-gate-report.json",
                contribution_governance_report_ref="target/contribution-governance-report.json",
                final_release_rehearsal_report_ref="target/final-release-rehearsal/final-release-rehearsal-report.json",
                website_readiness_report_ref="target/website-readiness-report.json",
                benchmark_manifest_ref="website/assets/benchmarks/latest/manifest.json",
                benchmark_completeness_report_ref="target/benchmark-artifact-completeness-report.json",
                runs_today_matrix_ref="docs/status/runs-today-support-matrix.json",
                dry_run=payloads["dry_run"],
                package_report=payloads["package_report"],
                release_security=payloads["release_security"],
                contribution_governance=payloads["contribution_governance"],
                final_rehearsal=payloads["final_rehearsal"],
                website_report=payloads["website_report"],
                benchmark_manifest_path=REPO_ROOT / "website" / "assets" / "benchmarks" / "latest" / "manifest.json",
                benchmark_completeness_report=None,
                runs_today=payloads["runs_today"],
            )

            self.assertEqual(report["status"], "passed", report["blockers"])
            self.assertEqual(report["claim_gate_status"], "not_claim_grade")
            self.assertFalse(report["public_release_claim_allowed"])
            self.assertFalse(report["public_package_claim_allowed"])
            self.assertIn("GAR-RUNTIME-IMPL-4S", report["covered_phase_items"])

    def test_production_usability_gate_accepts_compact_ci_package_artifacts(self) -> None:
        module = self._load_script_module(
            "check_production_usability_gate.py",
            "check_production_usability_gate_compact_ci_artifacts_for_test",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            payloads = self._production_usability_payloads(module, repo_root)
            wheel_name = "shardloom-0.1.0-py3-none-any.whl"
            payloads["dry_run"][
                "local_wheel"
            ] = f"target/release-dry-run-proof/python-package-stage/dist/{wheel_name}"
            nested_wheel = (
                repo_root
                / "target"
                / "release-dry-run-proof"
                / "python-package-stage"
                / "dist"
                / wheel_name
            )
            self.assertFalse(nested_wheel.exists())

            summary, blockers = module.validate_release_dry_run(repo_root, payloads["dry_run"])

            self.assertEqual(blockers, [])
            self.assertEqual(
                summary["resolved_artifacts"]["local_wheel"],
                f"python/dist/{wheel_name}",
            )

    def test_production_usability_gate_accepts_precomputed_benchmark_report(self) -> None:
        module = self._load_script_module(
            "check_production_usability_gate.py",
            "check_production_usability_gate_benchmark_report_for_test",
        )
        manifest_ref = "website/assets/benchmarks/latest/manifest.json"
        summary, blockers = module.validate_benchmark_completeness_report(
            {
                "schema_version": module.BENCHMARK_COMPLETENESS_REPORT_SCHEMA_VERSION,
                "status": "passed",
                "manifest": manifest_ref,
                "benchmark_profile": "full_local",
                "artifact_status": "complete",
                "available_lane_count": 12,
                "missing_lane_count": 0,
                "performance_claim_allowed": False,
                "benchmark_run_performed": False,
                "fallback_attempted": False,
                "external_engine_invoked": False,
                "blockers": [],
            },
            manifest_ref=manifest_ref,
        )

        self.assertEqual(blockers, [])
        self.assertEqual(summary["source"], "precomputed_report")
        self.assertEqual(summary["available_lane_count"], 12)

    def test_production_usability_gate_rejects_fallback_or_publication_drift(self) -> None:
        module = self._load_script_module(
            "check_production_usability_gate.py", "check_production_usability_gate_blocker_for_test"
        )
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_production_usability_docs(repo_root)
            payloads = self._production_usability_payloads(module, repo_root)
            payloads["dry_run"]["fallback_attempted"] = True

            _, blockers = module.validate_release_dry_run(repo_root, payloads["dry_run"])

            self.assertIn("release dry-run fallback_attempted must be false", blockers)

    def _python_user_surface_dry_run_payload(self, module: object) -> dict[str, object]:
        return {
            "schema_version": "shardloom.release_dry_run_proof.v1",
            "publication_attempted": False,
            "tag_created": False,
            "secrets_required": False,
            "external_runtime_dependencies_added": False,
            "fallback_engine_dependency_added": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "public_package_release_claim_allowed": False,
            "wheel_import_and_client_smoke_performed": True,
            "local_python_example_smoke_performed": True,
            "local_python_user_surface_quickstart_performed": True,
            "local_python_result_and_evidence_printed": True,
            "local_python_unsupported_path_evidence_printed": True,
            "generated_output_proof_distinct_from_no_dataset_smoke": True,
            "generated_source_user_rows_runtime_performed": True,
            "generated_source_range_runtime_performed": True,
            "steps": [
                {"name": name, "returncode": 0}
                for name in module.REQUIRED_DRY_RUN_STEPS
            ],
        }

    def test_python_user_surface_completion_gate_accepts_admitted_runtime_evidence(self) -> None:
        module = self._load_script_module(
            "check_python_user_surface_completion.py",
            "check_python_user_surface_completion_for_test",
        )
        runs_today = json.loads(
            (REPO_ROOT / "docs" / "status" / "runs-today-support-matrix.json").read_text(
                encoding="utf-8"
            )
        )
        report = module.build_report(
            repo_root=REPO_ROOT,
            release_dry_run_ref="target/release-dry-run-proof/transcript.json",
            runs_today_matrix_ref="docs/status/runs-today-support-matrix.json",
            production_usability_ref="target/production-usability-gate.json",
            dry_run=self._python_user_surface_dry_run_payload(module),
            runs_today=runs_today,
            production_usability=None,
        )

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertTrue(report["scoped_python_front_door_claim_allowed"])
        self.assertFalse(report["spark_compatibility_claim_allowed"])
        self.assertFalse(report["production_sql_dataframe_claim_allowed"])
        self.assertFalse(report["fallback_attempted"])
        self.assertFalse(report["external_engine_invoked"])
        self.assertIn("GAR-USER-SURFACE-1D", report["covered_phase_items"])
        by_id = {row["row_id"]: row for row in report["completion_matrix"]}
        self.assertEqual(by_id["ctx_sql"]["status"], "admitted_runtime_row_present")
        self.assertEqual(
            by_id["unsupported_paths"]["status"],
            "deterministic_blockers_present",
        )

    def test_python_user_surface_completion_can_read_method_rows_statically(self) -> None:
        module = self._load_script_module(
            "check_python_user_surface_completion.py",
            "check_python_user_surface_completion_static_rows_for_test",
        )

        rows = module._load_dataframe_method_rows_from_source(
            REPO_ROOT / "python" / "src" / "shardloom" / "context.py"
        )
        by_method = {row["method"]: row for row in rows}

        self.assertIn("filter", by_method)
        self.assertIn("from_rows", by_method)
        self.assertIn("sql", by_method)
        self.assertIn("to_pandas", by_method)
        self.assertIn("rename", by_method)
        self.assertIn("drop", by_method)
        self.assertIn("sample", by_method)
        self.assertIn("explode", by_method)
        self.assertIn("merge", by_method)
        self.assertIn("concat", by_method)
        self.assertIn("nunique", by_method)
        self.assertIn("value_counts", by_method)
        self.assertIn("fillna", by_method)
        self.assertIn("fill_null", by_method)
        self.assertIn("isna", by_method)
        self.assertIn("isnull", by_method)
        self.assertIn("notna", by_method)
        self.assertIn("notnull", by_method)
        self.assertIn("pivot", by_method)
        self.assertIn("pivot_table", by_method)
        self.assertIn("melt", by_method)
        self.assertIn("rolling", by_method)
        self.assertEqual(by_method["filter"]["support_status"], "lazy_plan_supported")
        self.assertEqual(by_method["from_rows"]["support_status"], "runtime-supported")
        self.assertEqual(
            by_method["to_pandas"]["support_status"],
            "optional_dependency_container_supported",
        )
        self.assertIn(
            "optional_dependency_policy",
            by_method["to_pandas"]["required_evidence"],
        )
        self.assertEqual(
            by_method["rename"]["support_status"],
            "production_admitted_local_workflow",
        )
        self.assertIsNone(by_method["rename"]["diagnostic_operation"])
        self.assertIn(
            "declared_schema_projection_rewrite",
            by_method["rename"]["required_evidence"],
        )
        self.assertEqual(
            by_method["drop"]["support_status"],
            "production_admitted_local_workflow",
        )
        self.assertIn(
            "projection_rewrite_semantics",
            by_method["drop"]["required_evidence"],
        )
        self.assertIn(
            "deterministic_seed_policy",
            by_method["sample"]["required_evidence"],
        )
        self.assertEqual(
            by_method["sample"]["support_status"],
            "production_admitted_local_workflow",
        )
        self.assertIn(
            "native_vortex_sample_primitive",
            by_method["sample"]["required_evidence"],
        )
        self.assertTrue(by_method["sample"]["runtime_execution"])
        self.assertTrue(by_method["sample"]["materialization_required"])
        self.assertIsNone(by_method["sample"]["blocker_id"])
        self.assertEqual(
            by_method["explode"]["support_status"],
            "production_admitted_local_workflow",
        )
        self.assertTrue(by_method["explode"]["runtime_execution"])
        self.assertTrue(by_method["explode"]["materialization_required"])
        self.assertIsNone(by_method["explode"]["blocker_id"])
        self.assertIn(
            "native_vortex_explode_primitive",
            by_method["explode"]["required_evidence"],
        )
        self.assertEqual(
            by_method["merge"]["support_status"],
            "production_admitted_local_workflow",
        )
        self.assertIn("join_operator_capability", by_method["merge"]["required_evidence"])
        self.assertEqual(
            by_method["concat"]["support_status"],
            "production_admitted_local_workflow",
        )
        self.assertIn(
            "schema_alignment_contract",
            by_method["concat"]["required_evidence"],
        )
        self.assertEqual(
            by_method["nunique"]["support_status"],
            "production_admitted_local_workflow",
        )
        self.assertIn("distinct_count_semantics", by_method["nunique"]["required_evidence"])
        self.assertEqual(
            by_method["value_counts"]["support_status"],
            "production_admitted_local_workflow",
        )
        self.assertIn(
            "grouped_count_semantics",
            by_method["value_counts"]["required_evidence"],
        )
        self.assertEqual(
            by_method["fillna"]["support_status"],
            "production_admitted_local_workflow",
        )
        self.assertIn("null_fill_semantics", by_method["fillna"]["required_evidence"])
        self.assertEqual(
            by_method["fill_null"]["support_status"],
            "production_admitted_local_workflow",
        )
        self.assertEqual(
            by_method["isna"]["support_status"],
            "production_admitted_local_workflow",
        )
        self.assertIn("null_mask_semantics", by_method["isna"]["required_evidence"])
        self.assertEqual(
            by_method["isnull"]["support_status"],
            "production_admitted_local_workflow",
        )
        self.assertEqual(
            by_method["notna"]["support_status"],
            "production_admitted_local_workflow",
        )
        self.assertIn("not_null_mask_semantics", by_method["notna"]["required_evidence"])
        self.assertEqual(
            by_method["notnull"]["support_status"],
            "production_admitted_local_workflow",
        )
        self.assertEqual(
            by_method["pivot"]["support_status"],
            "production_admitted_local_workflow",
        )
        self.assertIn(
            "native_vortex_pivot_primitive",
            by_method["pivot"]["required_evidence"],
        )
        self.assertIsNone(by_method["pivot"]["blocker_id"])
        self.assertEqual(
            by_method["pivot_table"]["support_status"],
            "production_admitted_local_workflow",
        )
        self.assertIn(
            "native_vortex_pivot_primitive",
            by_method["pivot_table"]["required_evidence"],
        )
        self.assertIn(
            "explicit_aggregate_kernel",
            by_method["pivot_table"]["required_evidence"],
        )
        self.assertIsNone(by_method["pivot_table"]["blocker_id"])
        self.assertEqual(
            by_method["melt"]["support_status"],
            "production_admitted_local_workflow",
        )
        self.assertIn(
            "native_vortex_melt_primitive",
            by_method["melt"]["required_evidence"],
        )
        self.assertEqual(
            by_method["rolling"]["support_status"],
            "production_admitted_local_workflow",
        )
        self.assertIn(
            "native_vortex_rolling_window_primitive",
            by_method["rolling"]["required_evidence"],
        )
        self.assertFalse(by_method["rolling"]["write_io"])
        self.assertEqual(
            by_method["map_rows"]["support_status"],
            "production_admitted_local_workflow",
        )
        self.assertIn(
            "declarative_row_transform_contract",
            by_method["map_rows"]["required_evidence"],
        )
        self.assertTrue(by_method["to_pandas"]["materialization_required"])
        self.assertIsNone(by_method["to_pandas"]["blocker_id"])
        self.assertEqual(
            by_method["display"]["support_status"],
            "production_admitted_local_workflow",
        )
        self.assertIn("notebook_display_contract", by_method["display"]["required_evidence"])
        self.assertIsNone(by_method["display"]["blocker_id"])
        self.assertFalse(any(row["fallback_attempted"] for row in rows))
        self.assertFalse(any(row["external_engine_invoked"] for row in rows))

    def test_python_user_surface_completion_gate_blocks_missing_unsupported_proof(self) -> None:
        module = self._load_script_module(
            "check_python_user_surface_completion.py",
            "check_python_user_surface_completion_blocker_for_test",
        )
        dry_run = self._python_user_surface_dry_run_payload(module)
        dry_run["local_python_unsupported_path_evidence_printed"] = False

        _, blockers = module.validate_release_dry_run(dry_run)

        self.assertIn(
            "release dry-run local_python_unsupported_path_evidence_printed must be true",
            blockers,
        )

    def test_benchmark_constitution_rejects_null_stage_timings(self) -> None:
        module = self._load_script_module(
            "check_benchmark_constitution.py", "check_benchmark_constitution_for_test"
        )

        missing = module.row_missing_fields(
            {
                "engine": "shardloom-native-vortex",
                "scenario_name": "null timing",
                "source_state_id": "source-state://null-timing",
                "selected_execution_mode": "native_vortex",
                "output_format": "inline_jsonl",
                "correctness_digest": "fnv1a64:abc",
                "cache_mode": "cold",
                "scenario_compute_millis": None,
                "cost_unit": "local_wall_time",
                "fallback_attempted": False,
                "external_engine_invoked": False,
            },
            environment={"cpu": "test"},
            build_profile={"build_profile": "debug"},
            claim_bearing=False,
        )

        self.assertIn("stage_timings", missing)
        self.assertIn("cold_lane_attribution", missing)

    def test_benchmark_constitution_accepts_complete_cold_lane_split(self) -> None:
        module = self._load_script_module(
            "check_benchmark_constitution.py",
            "check_benchmark_constitution_cold_lane_for_test",
        )

        missing = module.row_missing_fields(
            {
                "engine": "shardloom-prepared-vortex",
                "scenario_name": "warm prepared query",
                "source_format": "vortex",
                "selected_execution_mode": "prepared_vortex",
                "output_format": "inline_jsonl",
                "correctness_digest": "fnv1a64:abc",
                "cache_mode": "warm",
                "query_runtime_millis": 1.0,
                "vortex_scan_millis": 0.2,
                "operator_compute_millis": 0.5,
                "evidence_render_millis": 0.1,
                "cli_process_wall_millis": 2.0,
                "python_harness_overhead_millis": 0.3,
                "cold_lane_timing_split_status": "complete",
                "cost_unit": "local_wall_time",
                "fallback_attempted": False,
                "external_engine_invoked": False,
            },
            environment={"cpu": "test"},
            build_profile={"build_profile": "debug"},
            claim_bearing=True,
        )

        self.assertNotIn("stage_timings", missing)
        self.assertNotIn("cold_lane_attribution", missing)

    def test_admitted_semantics_missing_matrix_reports_remaining_gaps(self) -> None:
        module = self._load_script_module(
            "check_admitted_semantics_matrix.py",
            "check_admitted_semantics_matrix_for_test",
        )

        _rows, summary = module.validate_matrix_manifest(None, {"case_b", "case_a"})

        self.assertEqual(summary["status"], "failed")
        self.assertEqual(summary["remaining_matrix_gaps"], ["case_a", "case_b"])
        self.assertEqual(summary["v1_runtime_scope_status"], "failed")

    def test_admitted_semantics_matrix_blocks_extra_required_runtime_row(self) -> None:
        module = self._load_script_module(
            "check_admitted_semantics_matrix.py",
            "check_admitted_semantics_matrix_extra_required_for_test",
        )

        def row(row_id: str) -> dict[str, object]:
            return {
                "id": row_id,
                "operator_family": "fixture",
                "support_state": "executable",
                "runtime_validation": "required",
                "source_format": "csv",
                "input_dtype": "int64",
                "output_dtype": "int64",
                "null_policy": "fixture",
                "coercion_policy": "fixture",
                "invalid_input_behavior": "fixture",
                "unsupported_diagnostic_code": "not_applicable_executable",
                "unsupported_diagnostic_message": "not_applicable_executable",
                "decoded_reference_kind": "jsonl_inline_reference",
                "oracle_boundary": "decoded_reference_only",
                "property_seed": "not_applicable_fixed_fixture",
                "claim_boundary": "fixture",
                "fallback_attempted": False,
                "external_engine_invoked": False,
            }

        payload = {
            "schema_version": module.MATRIX_SCHEMA_VERSION,
            "row_order": ["case_a", "case_extra"],
            "remaining_matrix_gaps": list(module.EXPECTED_REMAINING_MATRIX_GAPS),
            "rows": [row("case_a"), row("case_extra")],
        }

        _rows, summary = module.validate_matrix_manifest(payload, {"case_a"})

        self.assertEqual(summary["status"], "failed")
        self.assertEqual(summary["v1_runtime_scope_status"], "failed")
        self.assertEqual(summary["v1_unexpected_required_runtime_row_count"], 1)
        self.assertTrue(
            any(
                "matrix required runtime rows without validator cases: case_extra"
                in blocker
                for blocker in summary["blockers"]
            ),
            summary["blockers"],
        )

    def test_admitted_semantics_matrix_blocks_changed_remaining_gap_list(self) -> None:
        module = self._load_script_module(
            "check_admitted_semantics_matrix.py",
            "check_admitted_semantics_matrix_gap_drift_for_test",
        )
        row = {
            "id": "case_a",
            "operator_family": "fixture",
            "support_state": "executable",
            "runtime_validation": "required",
            "source_format": "csv",
            "input_dtype": "int64",
            "output_dtype": "int64",
            "null_policy": "fixture",
            "coercion_policy": "fixture",
            "invalid_input_behavior": "fixture",
            "unsupported_diagnostic_code": "not_applicable_executable",
            "unsupported_diagnostic_message": "not_applicable_executable",
            "decoded_reference_kind": "jsonl_inline_reference",
            "oracle_boundary": "decoded_reference_only",
            "property_seed": "not_applicable_fixed_fixture",
            "claim_boundary": "fixture",
            "fallback_attempted": False,
            "external_engine_invoked": False,
        }
        payload = {
            "schema_version": module.MATRIX_SCHEMA_VERSION,
            "row_order": ["case_a"],
            "remaining_matrix_gaps": ["new silent v1 gap"],
            "rows": [row],
        }

        _rows, summary = module.validate_matrix_manifest(payload, {"case_a"})

        self.assertEqual(summary["status"], "failed")
        self.assertEqual(summary["remaining_matrix_gap_status"], "failed")
        self.assertTrue(
            any(
                "matrix remaining_matrix_gaps changed" in blocker
                for blocker in summary["blockers"]
            ),
            summary["blockers"],
        )

    def test_website_readiness_mirror_diagnostics_use_repo_root(self) -> None:
        module = self._load_script_module(
            "check_website_readiness.py", "check_website_readiness_for_test"
        )

        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir) / "checkout"
            source = repo_root / "docs" / "architecture" / "flow.md"
            mirror = repo_root / "website" / "assets" / "data" / "flow.md"
            source.parent.mkdir(parents=True)
            mirror.parent.mkdir(parents=True)
            source.write_text("canonical\n", encoding="utf-8")
            mirror.write_text("stale\n", encoding="utf-8")

            blockers: list[str] = []
            module.check_mirrored_file(
                source=source,
                mirror=mirror,
                label="flow snapshot",
                repo_root=repo_root,
                blockers=blockers,
            )

        self.assertEqual(
            blockers,
            [
                "flow snapshot drift: website/assets/data/flow.md does not match "
                "docs/architecture/flow.md"
            ],
        )

    def test_website_readiness_validates_benchmark_clickbench_handoff(self) -> None:
        module = self._load_script_module(
            "check_website_readiness.py", "check_website_clickbench_handoff_for_test"
        )

        with tempfile.TemporaryDirectory() as tempdir:
            website = Path(tempdir) / "website"
            website.mkdir()
            (website / "benchmarks.html").write_text(
                f"""
                <main>
                  <h1>Benchmarks</h1>
                  <p>ClickBench is the public comparison surface.</p>
                  <a href="{module.CLICKBENCH_URL}">Open ClickBench</a>
                </main>
                """,
                encoding="utf-8",
            )

            blockers: list[str] = []
            module.check_benchmark_clickbench_handoff(website, blockers)

        self.assertEqual(blockers, [])

    def test_website_readiness_validates_field_guide_route_pair(self) -> None:
        module = self._load_script_module(
            "check_website_readiness.py", "check_website_field_guide_alias_for_test"
        )

        with tempfile.TemporaryDirectory() as tempdir:
            website = Path(tempdir) / "website"
            alias = website / "field-guide.html" / "index.html"
            canonical = website / "field-guide" / "index.html"
            alias.parent.mkdir(parents=True)
            canonical.parent.mkdir(parents=True)
            alias.write_text(
                "<!doctype html><html><head><link rel=\"canonical\" "
                "href=\"https://shardloom.io/field-guide\"></head>"
                "<body><a href=\"/field-guide\">Open Field Guide</a></body></html>",
                encoding="utf-8",
            )
            canonical.write_text(
                "<!doctype html><html><head><meta name=\"generator\" "
                "content=\"Starlight v0.39.2\"><link rel=\"canonical\" "
                "href=\"https://shardloom.io/field-guide\"></head>"
                "<body><nav id=\"starlight__sidebar\"></nav></body></html>",
                encoding="utf-8",
            )

            blockers: list[str] = []
            module.check_field_guide_route_pair(website, blockers)

        self.assertEqual(blockers, [])

    def test_foundry_dev_stack_starter_accepts_local_runtime_proof(self) -> None:
        module = self._load_script_module(
            "check_foundry_dev_stack_starter.py",
            "check_foundry_dev_stack_starter_for_test",
        )

        manifest = json.loads(
            (REPO_ROOT / "docs" / "foundry" / "dev-stack-starter-kit.json").read_text(
                encoding="utf-8"
            )
        )
        doc_text = (
            REPO_ROOT / "docs" / "foundry" / "dev-stack-starter-kit.md"
        ).read_text(encoding="utf-8")

        blockers = module.validate_manifest(manifest)
        blockers.extend(module.validate_doc(doc_text))
        blockers.extend(module.validate_example_files(REPO_ROOT))

        self.assertEqual(blockers, [])

    def test_foundry_proof_posture_promotes_local_style_generated_and_staged_proof(self) -> None:
        module = self._load_script_module(
            "foundry_proof_of_use.py",
            "foundry_proof_of_use_for_test",
        )
        transform = {
            "generated_output_execution_performed": True,
            "generated_source_created": True,
            "generated_source_kind": "user_rows",
            "generated_source_row_count": 2,
            "generated_source_certificate_status": "present",
            "output_native_io_certificate_status": "certified_local_file_sink",
            "generated_output_fanout_output_count": 1,
            "generated_output_fanout_result_reuse_hit": True,
            "foundry_style_output_api_invoked": True,
            "foundry_style_result_dataset_written": True,
            "foundry_style_evidence_dataset_written": True,
            "staged_input_transform_execution_performed": True,
            "staged_input_transform_output_row_count": 3,
            "output_evidence_dataset_written": True,
        }

        fanout = module.foundry_generated_output_fanout_posture(transform)
        boundary = module.foundry_generated_output_boundary(transform)
        scale = module.foundry_scale_proof_boundary(27, transform)

        self.assertEqual(fanout["support_status"], "local_style_smoke_supported")
        self.assertEqual(fanout["claim_gate_status"], "fixture_smoke_only")
        self.assertEqual(fanout["blockers"], [])
        self.assertFalse(fanout["foundry_output_api_invoked"])
        self.assertTrue(fanout["foundry_style_output_api_invoked"])
        self.assertEqual(
            boundary["boundary_status"],
            "local_style_dataset_output_written_real_foundry_blocked",
        )
        self.assertFalse(boundary["public_foundry_generated_output_claim_allowed"])
        self.assertEqual(
            scale["proof_boundary_status"],
            "local_style_staged_transform_and_evidence_dataset_written_real_foundry_blocked",
        )
        self.assertEqual(scale["foundry_style_input_dataset_count"], 1)
        self.assertEqual(scale["foundry_style_output_dataset_count"], 2)


if __name__ == "__main__":
    unittest.main()
