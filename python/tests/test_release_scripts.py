from __future__ import annotations

import contextlib
import io
import json
import os
import importlib.util
import subprocess
import sys
import tempfile
import textwrap
import unittest
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


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
            "route_runtime_status": "scoped_runtime_supported",
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
            "route_runtime_status": "scoped_runtime_supported",
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
                "front_door_id": "local_source_auto_prepare_vortex_front_door",
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
                    "prepared_state_reuse_manifest_for_feature_gated_local_vortex_output",
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
            "global_review_unchecked_count": 38,
            "mapped_gap_count": 38,
            "acceptance_summary": {
                "all_unchecked_global_review_rows_mapped": True,
                "all_families_have_phase_items": True,
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
            expected_global_unchecked_count=38,
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
            "global_review_unchecked_count": 37,
            "mapped_gap_count": 37,
            "acceptance_summary": {
                "all_unchecked_global_review_rows_mapped": True,
                "all_families_have_phase_items": True,
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
            expected_global_unchecked_count=38,
        )

        self.assertIn(
            "runtime gap family burn-down global_review_unchecked_count mismatch: 37 != 38",
            blockers,
        )
        self.assertIn(
            "runtime gap family burn-down mapped_gap_count mismatch: 37 != 38",
            blockers,
        )

    def test_benchmark_promoter_recomputes_stale_runtime_validation(self) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py", "promote_benchmark_artifact_for_test"
        )

        row = {
            "engine": "shardloom-native-vortex",
            "storage_format": "csv",
            "scenario_name": "stale validation",
            "status": "success",
            "source_state_id": "source-state://stale-validation",
            "data_decoded": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "claim_gate_status": "fixture_smoke_only",
            "runtime_execution_validation": {
                "status": "passed",
                "surface_id": "stale.cached.report",
            },
        }

        with self.assertRaisesRegex(RuntimeError, "failed runtime validation"):
            module.runtime_validation_for_row(row)

    def test_benchmark_promoter_preserves_claim_grade_readiness(self) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py", "promote_benchmark_claim_grade_for_test"
        )

        row = {
            "engine": "shardloom",
            "storage_format": "csv",
            "scenario_name": "claim grade row",
            "status": "success",
            "selected_execution_mode": "prepared_vortex",
            "prepared_state_id": "prepared-state://claim-grade-row",
            "prepared_state_digest": "sha256:claim-grade-row",
            "source_state_id": "source-state://claim-grade-row",
            "data_decoded": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "runtime_execution_certificate_id": "execution.claim-grade-row",
            "runtime_execution_certificate_status": "certified",
            "sink_artifact_ref": r"C:\Users\test\shardloom\result.vortex",
            "claim_gate_status": "claim_grade",
            "claim_grade_requirements_met": True,
            "claim_grade_missing_evidence": [],
            "metrics": {
                "query_runtime_millis": 1.0,
                "vortex_scan_millis": 0.2,
                "operator_compute_millis": 0.5,
                "evidence_render_millis": 0.1,
                "cli_process_wall_millis": 1.4,
                "python_harness_overhead_millis": 0.4,
                "claim_gate_status": "claim_grade",
                "claim_grade_requirements_met": False,
                "claim_grade_missing_evidence": ["stale metrics value"],
            },
        }

        [published] = module.published_rows([row])

        self.assertIs(published["claim_grade_requirements_met"], True)
        self.assertEqual(published["claim_grade_missing_evidence"], [])
        self.assertEqual(published["runtime_execution_validation_status"], "passed")
        self.assertNotIn(r"C:\Users", published["sink_artifact_ref"])
        self.assertIn("local-artifact-ref:sha256:", published["sink_artifact_ref"])

    def test_benchmark_promoter_preserves_blocked_cold_lane_status(self) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_blocked_cold_lane_for_test",
        )

        row = {
            "engine": "shardloom-vortex",
            "storage_format": "csv",
            "scenario_name": "blocked cold lane",
            "status": "success",
            "selected_execution_mode": "prepared_vortex",
            "prepared_state_id": "prepared-state://blocked-cold-lane",
            "prepared_state_digest": "sha256:prepared",
            "source_state_id": "source-state://blocked-cold-lane",
            "data_decoded": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "runtime_execution_certificate_id": "execution.blocked-cold-lane",
            "runtime_execution_certificate_status": "certified",
            "claim_gate_status": "claim_grade",
            "claim_grade_requirements_met": True,
            "claim_grade_missing_evidence": [],
            "metrics": {
                "query_runtime_millis": 1.0,
                "vortex_scan_millis": 0.2,
                "operator_compute_millis": 0.5,
            },
        }

        [published] = module.published_rows([row])

        self.assertEqual(
            published["cold_lane_timing_split_status"],
            "blocked_incomplete_timing_split",
        )
        self.assertEqual(
            published["cold_lane_claim_gate_status"],
            "blocked_incomplete_timing_split",
        )
        self.assertEqual(published["claim_gate_status"], "not_claim_grade")
        self.assertFalse(published["claim_grade_requirements_met"])
        self.assertTrue(
            any(
                "cold_lane_timing_split_status!=complete" in item
                for item in published["claim_grade_missing_evidence"]
            )
        )

    def test_benchmark_promoter_normalizes_residual_runtime_evidence_statuses(self) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_residual_evidence_for_test",
        )

        row = {
            "engine": "shardloom-vortex",
            "storage_format": "csv",
            "scenario_name": "residual evidence row",
            "status": "success",
            "selected_execution_mode": "prepared_vortex",
            "prepared_state_id": "prepared-state://residual-evidence",
            "prepared_state_digest": "sha256:prepared",
            "source_state_id": "source-state://residual-evidence",
            "data_decoded": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "runtime_execution_certificate_id": "execution.residual-evidence",
            "runtime_execution_certificate_status": "certified",
            "claim_gate_status": "claim_grade",
            "claim_grade_requirements_met": True,
            "claim_grade_missing_evidence": [],
            "metrics": {
                "query_runtime_millis": 1.0,
                "vortex_scan_millis": 0.2,
                "operator_compute_millis": 0.5,
                "evidence_render_millis": 0.1,
                "cli_process_wall_millis": 1.4,
                "python_harness_overhead_millis": 0.4,
                "source_state_status": "report_only",
                "source_state_claim_gate_status": "not_claim_grade",
                "prepared_state_status": "report_only",
                "prepared_state_claim_gate_status": "not_claim_grade",
                "reuse_level_claim_gate_status": "not_claim_grade",
                "vortex_copy_budget_buffer_reuse_status": (
                    "blocked_until_correctness_parity"
                ),
                "vortex_copy_budget_unsafe_lifetime_shortcut_status": (
                    "blocked_no_unsafe_lifetime_shortcuts"
                ),
                "vortex_copy_budget_claim_gate_status": "not_claim_grade",
                "optimizer_rule_unsupported_count": 2,
                "prepared_vortex_scale_split_operator_retry_replay_status": (
                    "blocked_until_selection_vector_split_metric_replay"
                ),
                "prepared_vortex_scale_split_operator_spill_policy_status": (
                    "larger_than_memory_spill_io_blocked_fail_before_oom_only"
                ),
            },
        }

        [published] = module.published_rows([row])

        self.assertEqual(published["source_state_status"], "source_state_recorded")
        self.assertEqual(published["prepared_state_status"], "prepared_state_created")
        self.assertEqual(published["source_state_claim_gate_status"], "claim_grade")
        self.assertEqual(published["prepared_state_claim_gate_status"], "claim_grade")
        self.assertEqual(published["reuse_level_claim_gate_status"], "claim_grade")
        self.assertEqual(published["vortex_copy_budget_claim_gate_status"], "claim_grade")
        self.assertEqual(published["optimizer_rule_unsupported_count"], 0)
        self.assertEqual(published["optimizer_rule_not_required_count"], 5)
        self.assertEqual(published["optimizer_rule_not_applicable_count"], 1)
        self.assertEqual(
            published["vortex_copy_budget_buffer_reuse_status"],
            "safe_owned_buffers_no_reuse_required_for_correctness_parity",
        )
        self.assertEqual(
            published["vortex_copy_budget_unsafe_lifetime_shortcut_status"],
            "no_unsafe_lifetime_shortcuts_used",
        )
        self.assertEqual(
            published["prepared_vortex_scale_split_operator_retry_replay_status"],
            "not_admitted_selection_vector_split_metric_replay_not_required_for_current_runtime",
        )
        self.assertEqual(
            published["prepared_vortex_scale_split_operator_spill_policy_status"],
            "larger_than_memory_spill_io_not_required_for_local_runtime_envelope",
        )

    def test_benchmark_promoter_claim_grade_closeout_separates_timing_surfaces(
        self,
    ) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_artifact_claim_grade_closeout",
        )
        rows = [
            {
                "engine": "shardloom-vortex",
                "status": "success",
                "timing_surface": "hot_runtime",
                "actual_evidence_tier": "metadata_sink",
                "claim_gate_status": "not_claim_grade",
            },
            {
                "engine": "shardloom-vortex",
                "status": "success",
                "timing_surface": "publication_proof",
                "actual_evidence_tier": "publication_full",
                "claim_gate_status": "claim_grade",
            },
            {
                "engine": "duckdb",
                "status": "success",
                "timing_surface": "external_baseline",
                "claim_gate_status": "external_baseline_only",
            },
        ]

        table = module.claim_grade_closeout_table(rows)
        by_scope = {row[0]: row for row in table["rows"]}

        self.assertEqual(
            by_scope["ShardLoom timing-surface rows"][1],
            "2 rows; 1 hot_runtime / 1 publication_proof",
        )
        self.assertEqual(
            by_scope["Hot-runtime metadata rows"][1],
            "1 rows; 1 metadata_sink; 1 compact hot-evidence rows",
        )
        self.assertEqual(
            by_scope["Hot-runtime metadata rows"][2],
            "not_claim_grade is expected for compact metadata-sink timing rows",
        )
        self.assertEqual(
            by_scope["Publication-proof rows"][1],
            "1 rows; 1 claim_grade",
        )

    def test_benchmark_promoter_preserves_shared_batch_cold_lane_split(self) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_shared_batch_cold_lane_for_test",
        )

        row = {
            "engine": "shardloom-vortex",
            "storage_format": "csv",
            "scenario_name": "prepared batch claim row",
            "status": "success",
            "selected_execution_mode": "prepared_vortex",
            "requested_execution_mode": "prepared_vortex",
            "source_state_id": "source-state://prepared-batch-row",
            "source_state_digest": "sha256:source",
            "prepared_state_id": "prepared-state://prepared-batch-row",
            "prepared_state_digest": "sha256:prepared",
            "data_decoded": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "runtime_execution_certificate_id": "execution.prepared-batch-row",
            "runtime_execution_certificate_status": "certified",
            "claim_gate_status": "claim_grade",
            "claim_grade_requirements_met": True,
            "claim_grade_missing_evidence": [],
            "iterations": 3,
            "reproducibility_min_iterations": 3,
            "reproducibility_iterations_met": True,
            "correctness_digest": "sha256:correct",
            "correctness_digest_stable": True,
            "computed_result_sink_replay_verified": True,
            "source_state_reuse_status": "per_batch_group_category_metric_state_reused",
            "source_state_reused": True,
            "source_state_reuse_scope": (
                "group_category_metric_state_for_group_by_aggregation_and_multi_key_group_by"
            ),
            "source_state_reuse_consumer_count": 2,
            "source_state_recompute_avoided_count": 1,
            "source_state_group_category_metric_reuse_status": (
                "per_batch_group_category_metric_state_reused"
            ),
            "source_state_group_category_metric_reused": True,
            "source_state_group_category_metric_reuse_consumer_count": 2,
            "source_state_group_category_metric_recompute_avoided_count": 1,
            "metrics": {
                "persistent_runner_status": "single_process_batch_runner_supported",
                "vortex_scan_millis": 0.2,
                "query_runtime_millis": 1.0,
                "operator_compute_millis": 0.5,
                "evidence_render_millis": 0.1,
                "cli_process_wall_millis": 2.0,
                "session_route_used": True,
                "process_spawn_count": 1,
                "batch_cli_process_wall_millis": 2.0,
                "batch_process_wall_shared": True,
            },
        }

        [published] = module.published_rows([row])

        self.assertEqual(published["cold_lane_timing_split_status"], "complete")
        self.assertEqual(published["claim_gate_status"], "claim_grade")
        self.assertTrue(published["claim_grade_requirements_met"])
        self.assertTrue(published["batch_process_wall_shared"])
        self.assertEqual(published["batch_cli_process_wall_millis"], 2.0)
        self.assertTrue(published["session_route_used"])
        self.assertEqual(published["process_spawn_count"], 1)

        [website_row] = module.website_rows([published])
        self.assertTrue(website_row["session_route_used"])
        self.assertEqual(website_row["process_spawn_count"], 1)
        self.assertEqual(
            website_row["source_state_reuse_status"],
            "per_batch_group_category_metric_state_reused",
        )
        self.assertTrue(website_row["source_state_reused"])
        self.assertEqual(website_row["source_state_reuse_consumer_count"], 2)
        self.assertEqual(website_row["source_state_recompute_avoided_count"], 1)
        self.assertEqual(
            website_row["source_state_group_category_metric_reuse_status"],
            "per_batch_group_category_metric_state_reused",
        )
        self.assertTrue(website_row["source_state_group_category_metric_reused"])

    def test_benchmark_promoter_backfills_session_process_fields_for_legacy_rows(self) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_session_process_backfill_for_test",
        )

        batch = module.normalize_published_runtime_evidence(
            {
                "engine": "shardloom-vortex",
                "status": "success",
                "persistent_runner_status": "single_process_batch_runner_supported",
            }
        )
        per_scenario = module.normalize_published_runtime_evidence(
            {
                "engine": "shardloom",
                "status": "success",
                "persistent_runner_status": "process_per_scenario_attributed_not_reduced",
            }
        )

        self.assertFalse(batch["session_route_used"])
        self.assertEqual(batch["process_spawn_count"], 1)
        self.assertFalse(per_scenario["session_route_used"])
        self.assertEqual(per_scenario["process_spawn_count"], 1)

        session_backed = module.normalize_published_runtime_evidence(
            {
                "engine": "shardloom-vortex",
                "status": "success",
                "persistent_runner_status": "single_process_batch_runner_supported",
                "session_schema_version": "shardloom.session.v1",
                "session_id": "session://fixture",
            }
        )
        self.assertTrue(session_backed["session_route_used"])

    def test_benchmark_promoter_preserves_role_scoped_repair_timing(self) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_role_scoped_repair_for_test",
        )

        row = {
            "engine": "shardloom-prepare-batch",
            "storage_format": "csv",
            "scenario_name": "role scoped prepared repair",
            "scenario_id": "role_scoped_prepared_repair",
            "status": "success",
            "selected_execution_mode": "prepared_vortex_batch",
            "requested_execution_mode": "prepared_vortex_batch",
            "data_decoded": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "claim_gate_status": "not_claim_grade",
            **self._shardloom_benchmark_route_fields("shardloom-prepare-batch"),
            "timing_surface": "hot_runtime",
            "timing_surface_label": "Hot runtime",
            "timing_surface_evidence_tier": "metadata_sink",
            "actual_evidence_tier": "metadata_sink",
            "sink_tier": "metadata_sink",
            "metrics": {
                "query_runtime_millis": 0.25,
                "total_runtime_millis": 1.0,
                "vortex_scan_millis": 0.08,
                "operator_compute_millis": 0.12,
                "result_sink_write_millis": 0.0,
                "evidence_render_millis": 0.0,
            },
            "shardloom_evidence": {
                "prepare_batch_prepared_state_lookup_status": (
                    "workspace_manifest_role_repair"
                ),
                "prepare_batch_prepared_state_index_lookup_status": (
                    "workspace_index_manifest_hit"
                ),
                "prepare_batch_prepared_state_index_digest": "sha256:index",
                "prepare_batch_prepared_state_index_source_packet_digest": (
                    "sha256:packet"
                ),
                "prepare_batch_prepared_state_index_external_engine_invoked": False,
                "prepare_batch_prepared_state_dependency_status": (
                    "manifest_dependencies_repaired"
                ),
                "prepare_batch_prepared_state_dependency_checked_roles": (
                    "fact_input,dim_input,cdc_delta_input,prepare_policy,"
                    "source_admission_packet,prepared_artifact_fact,"
                    "prepared_artifact_dim,prepared_artifact_cdc_delta,"
                    "no_fallback_policy"
                ),
                "prepare_batch_prepared_state_dependency_changed_roles": "fact_input",
                "prepare_batch_prepared_state_dependency_manifest_digest": (
                    "sha256:manifest"
                ),
                "prepare_batch_prepared_state_dependency_source_packet_digest": (
                    "sha256:packet"
                ),
                "prepare_batch_prepared_state_dependency_artifact_manifest_hash": (
                    "sha256:artifact-manifest"
                ),
                "prepare_batch_prepared_state_dependency_packet_reuse_status": (
                    "single_evaluation_packet_reused_for_role_repair"
                ),
                "prepare_batch_prepared_state_dependency_packet_rebuild_avoided_count": (
                    1
                ),
                "prepare_batch_prepared_state_dependency_fallback_attempted": False,
                "prepare_batch_prepared_state_dependency_external_engine_invoked": False,
                "prepare_batch_prepared_state_partial_repair_status": (
                    "admitted_role_repair_completed"
                ),
                "prepare_batch_prepared_state_partial_repair_blocker_id": (
                    "not_applicable_partial_repair_admitted"
                ),
                "prepare_batch_prepared_state_partial_repair_changed_roles": (
                    "fact_input"
                ),
                "prepare_batch_prepared_state_partial_repair_reused_roles": (
                    "dim_input"
                ),
                "prepare_batch_prepared_state_partial_repair_repaired_roles": (
                    "fact_input"
                ),
                "prepare_batch_prepared_state_partial_repair_invalidated_derived_states": (
                    "prepared_state_index,source_admission_packet"
                ),
                "prepare_batch_prepared_state_partial_repair_micros": 8765,
                "prepare_batch_prepared_state_partial_repair_source_to_columnar_micros": (
                    2000
                ),
                "prepare_batch_prepared_state_partial_repair_vortex_array_build_micros": (
                    3000
                ),
                "prepare_batch_prepared_state_partial_repair_vortex_write_micros": (
                    4000
                ),
                "prepare_batch_prepared_state_partial_repair_vortex_reopen_verify_micros": (
                    5000
                ),
                "prepare_batch_prepared_state_partial_repair_replay_proof": (
                    "sha256:repair-proof"
                ),
                "prepare_batch_prepared_state_partial_repair_repairable_segment_count": (
                    1
                ),
                "prepare_batch_prepared_state_partial_repair_regeneration_performed": (
                    True
                ),
                "prepare_batch_prepared_state_partial_repair_stale_segment_reuse_allowed": (
                    False
                ),
                "prepare_batch_prepared_state_optimization_no_fallback_policy_status": (
                    "passed_fallback_false_external_engine_false"
                ),
                "prepare_batch_prepared_state_optimization_fallback_attempted": False,
                "prepare_batch_prepared_state_optimization_external_engine_invoked": False,
                "prepare_batch_prepared_state_optimization_stale_artifact_reuse_allowed": (
                    False
                ),
            },
        }

        [published] = module.published_rows([row])

        self.assertEqual(
            published["prepare_batch_prepared_state_optimization_strategy"],
            "role_scoped_repair",
        )
        self.assertEqual(
            published["prepare_batch_prepared_state_optimization_status"],
            "prepared_state_role_repair_admitted",
        )
        self.assertEqual(
            published["prepare_batch_prepared_state_optimization_repaired_roles"],
            "fact_input",
        )
        self.assertEqual(
            published["prepare_batch_prepared_state_optimization_repair_ms"],
            8.765,
        )
        self.assertEqual(
            published["prepare_batch_prepared_state_dependency_packet_reuse_status"],
            "single_evaluation_packet_reused_for_role_repair",
        )
        self.assertEqual(
            published[
                "prepare_batch_prepared_state_dependency_packet_rebuild_avoided_count"
            ],
            1,
        )
        self.assertEqual(
            published[
                "prepare_batch_prepared_state_partial_repair_source_to_columnar_ms"
            ],
            2.0,
        )
        self.assertEqual(
            published[
                "prepare_batch_prepared_state_partial_repair_vortex_array_build_ms"
            ],
            3.0,
        )
        self.assertEqual(
            published["prepare_batch_prepared_state_partial_repair_vortex_write_ms"],
            4.0,
        )
        self.assertEqual(
            published[
                "prepare_batch_prepared_state_partial_repair_vortex_reopen_verify_ms"
            ],
            5.0,
        )
        self.assertEqual(
            published["prepare_batch_prepared_state_partial_repair_replay_proof"],
            "sha256:repair-proof",
        )
        self.assertTrue(
            published[
                "prepare_batch_prepared_state_optimization_base_artifact_reused"
            ]
        )
        self.assertFalse(
            published[
                "prepare_batch_prepared_state_optimization_stale_artifact_reuse_allowed"
            ]
        )

    def test_benchmark_runner_preserves_explicit_zero_partial_repair_timings(
        self,
    ) -> None:
        module = self._load_module_from_path(
            REPO_ROOT / "benchmarks" / "traditional_analytics" / "run.py",
            "traditional_analytics_run_partial_repair_zero_for_test",
        )

        fields = module.prepare_batch_dependency_repair_fields(
            "shardloom-prepare-batch",
            status="success",
            evidence={
                "prepare_batch_prepared_state_partial_repair_micros": 0,
                "prepare_batch_prepared_state_partial_repair_source_to_columnar_micros": 0,
                "prepare_batch_prepared_state_partial_repair_vortex_array_build_micros": 0,
                "prepare_batch_prepared_state_partial_repair_vortex_write_micros": 0,
                "prepare_batch_prepared_state_partial_repair_vortex_reopen_verify_micros": 0,
                "prepare_batch_source_to_columnar_micros": 2000,
                "prepare_batch_vortex_array_build_micros": 3000,
                "prepare_batch_vortex_write_micros": 4000,
                "prepare_batch_vortex_reopen_verify_micros": 5000,
            },
        )

        self.assertEqual(
            fields["prepare_batch_prepared_state_partial_repair_micros"],
            0.0,
        )
        self.assertEqual(
            fields[
                "prepare_batch_prepared_state_partial_repair_source_to_columnar_micros"
            ],
            0.0,
        )
        self.assertEqual(
            fields[
                "prepare_batch_prepared_state_partial_repair_vortex_array_build_micros"
            ],
            0.0,
        )
        self.assertEqual(
            fields["prepare_batch_prepared_state_partial_repair_vortex_write_micros"],
            0.0,
        )
        self.assertEqual(
            fields[
                "prepare_batch_prepared_state_partial_repair_vortex_reopen_verify_micros"
            ],
            0.0,
        )

    def test_benchmark_promoter_emits_cold_bottleneck_fields(self) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_cold_bottleneck_for_test",
        )

        row = {
            "engine": "shardloom",
            "storage_format": "csv",
            "scenario_name": "cold bottleneck route row",
            "status": "success",
            "selected_execution_mode": "compatibility_import_certified",
            "requested_execution_mode": "compatibility_import_certified",
            "timing_scope": "cold_certified_end_to_end",
            "compatibility_import_included": True,
            "source_state_id": "source-state://cold-bottleneck-row",
            "source_state_digest": "sha256:source",
            "prepared_state_id": "prepared-state://cold-bottleneck-row",
            "prepared_state_digest": "sha256:prepared",
            "data_decoded": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "runtime_execution_certificate_id": "execution.cold-bottleneck-row",
            "runtime_execution_certificate_status": "certified",
            "claim_gate_status": "claim_grade",
            "claim_grade_requirements_met": True,
            "claim_grade_missing_evidence": [],
            "iterations": 3,
            "reproducibility_min_iterations": 3,
            "reproducibility_iterations_met": True,
            "correctness_digest": "sha256:correct",
            "correctness_digest_stable": True,
            "computed_result_sink_replay_verified": True,
            "metrics": {
                "source_stat_micros": 2500,
                "source_read_millis": 10.0,
                "compatibility_parse_millis": 12.0,
                "source_to_columnar_millis": 4.0,
                "compatibility_to_vortex_import_millis": 95.0,
                "vortex_array_build_millis": 20.0,
                "vortex_write_millis": 70.0,
                "vortex_digest_micros": 1500.0,
                "vortex_reopen_verify_millis": 5.0,
                "vortex_scan_millis": 1.0,
                "operator_compute_millis": 2.0,
                "operator_kernel_micros": 2000,
                "result_sink_write_millis": 3.0,
                "evidence_render_millis": 4.0,
                "total_runtime_millis": 130.0,
                "cli_process_wall_millis": 135.0,
                "python_harness_overhead_millis": 5.0,
                "file_count": 8,
                "bytes_read": 4096,
                "source_columns": "group_key,metric",
                "vortex_capillary_preparation_activation_observed_columns": 13,
                "prepared_state_reuse_allowed": True,
            },
        }

        [published] = module.published_rows([row])

        self.assertEqual(published["cold_lane_timing_split_status"], "complete")
        self.assertEqual(published["cold_bottleneck_status"], "complete")
        self.assertEqual(published["cold_bottleneck_primary_stage"], "vortex_write")
        self.assertEqual(published["cold_bottleneck_secondary_stage"], "vortex_array_build")
        self.assertEqual(published["source_admission_ms"], 2.5)
        self.assertEqual(published["source_read_ms"], 0.0)
        self.assertEqual(published["source_parse_or_columnar_decode_ms"], 16.0)
        self.assertEqual(published["source_to_vortex_array_ms"], 20.0)
        self.assertEqual(published["inclusive_compatibility_to_vortex_import_ms"], 95.0)
        self.assertEqual(published["exclusive_source_read_ms"], 0.0)
        self.assertEqual(published["exclusive_source_parse_or_decode_ms"], 16.0)
        self.assertEqual(published["vortex_scan_ms"], 1.0)
        self.assertEqual(published["operator_compute_ms"], 2.0)
        self.assertIn(
            "vortex_scan_ms", published["route_timing_included_stage_ids"]
        )
        self.assertIn(
            "operator_compute_ms", published["route_timing_included_stage_ids"]
        )
        self.assertIn(
            "vortex_scan:included_hot_runtime",
            published["route_timing_stage_inclusion_classes"],
        )
        self.assertIn(
            "operator_compute:included_hot_runtime",
            published["route_timing_stage_inclusion_classes"],
        )
        self.assertEqual(published["route_timing_exclusive_stage_sum_ms"], 118.0)
        self.assertEqual(published["route_timing_exclusive_residual_ms"], 0.0)
        self.assertEqual(published["source_pressure_profile"], "many_small_files_pressure")
        self.assertEqual(published["source_split_count"], 8)
        self.assertEqual(published["source_open_count"], 8)
        self.assertEqual(published["source_columns_requested"], 2)
        self.assertTrue(published["source_projection_applied"])
        self.assertTrue(published["vortex_prepared_state_reusable"])
        self.assertEqual(
            published["cold_route_optimization_hint"],
            "batch_source_open_and_split_planning_before_parse_or_writer_tuning",
        )
        self.assertEqual(published["total_route_ms"], 118.0)
        self.assertFalse(published["performance_claim_allowed"])

    def test_benchmark_repromotion_preserves_writer_context_ms_fields(self) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_repromoted_writer_context_for_test",
        )

        row = {
            "engine": "shardloom",
            "storage_format": "csv",
            "scenario_name": "repromoted writer context row",
            "status": "success",
            "selected_execution_mode": "compatibility_import_certified",
            "requested_execution_mode": "compatibility_import_certified",
            "timing_scope": "cold_certified_end_to_end",
            "compatibility_import_included": True,
            "claim_gate_status": "not_claim_grade",
            "claim_grade_requirements_met": False,
            "claim_grade_missing_evidence": ["fixture_not_claim_grade"],
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "vortex_writer_context_schema_version": (
                "shardloom.traditional_analytics.vortex_writer_context.v1"
            ),
            "vortex_writer_context_status": "reported",
            "vortex_writer_context_open_ms": 1.25,
            "vortex_writer_context_write_count": 2,
            "vortex_writer_context_reuse_hit_count": 1,
            "vortex_writer_context_reuse_status": (
                "single_vortex_runtime_session_reused_across_artifacts"
            ),
            "vortex_segment_write_ms": 3.5,
            "vortex_workspace_stage_ms": 4.75,
            "vortex_write_plan_context_open_ms": 1.25,
            "vortex_write_plan_segment_write_ms": 3.5,
            "vortex_write_plan_workspace_stage_ms": 4.75,
            "metrics": {
                "source_read_millis": 1.0,
                "compatibility_parse_millis": 1.0,
                "compatibility_to_vortex_import_millis": 1.0,
                "vortex_write_millis": 1.0,
                "vortex_reopen_verify_millis": 1.0,
                "operator_compute_millis": 1.0,
                "total_runtime_millis": 10.0,
                "cli_process_wall_millis": 10.5,
            },
        }

        [published] = module.published_rows_with_current_route_timing_ledger([row])

        self.assertEqual(published["vortex_writer_context_open_ms"], 1.25)
        self.assertEqual(published["vortex_segment_write_ms"], 3.5)
        self.assertEqual(published["vortex_workspace_stage_ms"], 4.75)
        self.assertEqual(published["vortex_write_plan_context_open_ms"], 1.25)
        self.assertEqual(published["vortex_write_plan_segment_write_ms"], 3.5)
        self.assertEqual(published["vortex_write_plan_workspace_stage_ms"], 4.75)

    def test_benchmark_repromotion_requires_replay_timing_for_replay_tier(self) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_repromoted_replay_tier_for_test",
        )

        row = {
            "engine": "shardloom-prepared-vortex",
            "storage_format": "vortex",
            "scenario_name": "legacy replay proof without replay timing",
            "status": "success",
            "selected_execution_mode": "prepared_vortex",
            "requested_execution_mode": "prepared_vortex",
            "timing_scope": "warm_prepared_query",
            "claim_gate_status": "not_claim_grade",
            "claim_grade_requirements_met": False,
            "claim_grade_missing_evidence": ["fixture_not_claim_grade"],
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "requested_evidence_tier": "auto",
            "actual_evidence_tier": "full_vortex_replay",
            "selected_evidence_tier": "full_vortex_replay",
            "sink_tier": "full_vortex_replay",
            "computed_result_sink_replay_verified": True,
            "computed_result_sink_write_micros": 2500,
            "result_sink_replay_micros": None,
            "metrics": {
                "query_runtime_millis": 1.0,
                "result_sink_write_millis": 2.5,
                "operator_compute_millis": 0.4,
                "total_runtime_millis": 3.5,
                "cli_process_wall_millis": 3.8,
            },
        }

        [published] = module.published_rows_with_current_route_timing_ledger([row])

        self.assertEqual(published["actual_evidence_tier"], "metadata_sink")
        self.assertEqual(published["selected_evidence_tier"], "metadata_sink")
        self.assertEqual(published["sink_tier"], "metadata_sink")
        self.assertFalse(published["evidence_tier_result_sink_replay_required"])
        self.assertEqual(
            published["result_sink_replay_skip_reason"],
            "skipped_metadata_sink_tier_digest_count_path_proof_without_replay",
        )

    def test_benchmark_timing_surfaces_keep_hot_runtime_separate_from_publication(
        self,
    ) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_timing_surface_for_test",
        )

        base = {
            "engine": "shardloom-prepared-vortex",
            "storage_format": "vortex",
            "scenario_name": "warm prepared timing surface",
            "status": "success",
            "selected_execution_mode": "prepared_vortex",
            "requested_execution_mode": "prepared_vortex",
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "source_state_id": "source-state://surface",
            "source_state_digest": "sha256:surface-source",
            "prepared_state_id": "prepared-state://surface",
            "prepared_state_digest": "sha256:surface-prepared",
            "data_decoded": False,
            "data_materialized": False,
            "iterations": 3,
            "reproducibility_min_iterations": 3,
            "reproducibility_iterations_met": True,
            "correctness_digest": "sha256:surface",
            "correctness_digest_stable": True,
            "runtime_execution_certificate_id": "execution.surface",
            "runtime_execution_certificate_status": "certified",
            "metrics": {
                "query_runtime_millis": 0.34,
                "vortex_scan_millis": 0.1,
                "operator_compute_millis": 0.2,
                "result_sink_write_millis": 5.33,
                "evidence_render_millis": 8.15,
                "total_runtime_millis": 13.82,
                "cli_process_wall_millis": 14.0,
            },
        }
        hot_row = {
            **base,
            "claim_gate_status": "not_claim_grade",
            "claim_grade_requirements_met": False,
            "claim_grade_missing_evidence": ["fixture_not_claim_grade"],
            "requested_evidence_tier": "metadata_sink",
            "actual_evidence_tier": "metadata_sink",
        }
        publication_row = {
            **base,
            "claim_gate_status": "claim_grade",
            "claim_grade_requirements_met": True,
            "claim_grade_missing_evidence": [],
            "actual_evidence_tier": "publication_full",
            "computed_result_sink_replay_verified": True,
            "result_sink_replay_micros": 1200,
        }

        hot, publication = module.published_rows([hot_row, publication_row])

        self.assertEqual(hot["timing_surface"], "hot_runtime")
        self.assertEqual(hot["actual_evidence_tier"], "metadata_sink")
        self.assertEqual(hot["total_route_ms"], 0.34)
        self.assertEqual(hot["hot_route_total_ms"], 0.34)
        self.assertEqual(hot["publication_proof_route_total_ms"], 13.82)
        self.assertFalse(hot["output_timing_included_in_total"])
        self.assertFalse(hot["evidence_timing_included_in_total"])
        self.assertIn("timing_surface=hot_runtime", hot["route_total_formula"])
        self.assertNotIn("evidence_render_millis", hot["route_total_formula"])

        self.assertEqual(publication["timing_surface"], "publication_proof")
        self.assertEqual(publication["actual_evidence_tier"], "publication_full")
        self.assertEqual(publication["total_route_ms"], 13.82)
        self.assertTrue(publication["output_timing_included_in_total"])
        self.assertTrue(publication["evidence_timing_included_in_total"])
        self.assertIn(
            "timing_surface=publication_proof", publication["route_total_formula"]
        )
        self.assertIn("evidence_render_millis", publication["route_total_formula"])

        [lane] = module.route_lane_comparison_table([publication])["rows"]
        self.assertEqual(lane[1], "hot_runtime")
        self.assertEqual(lane[6], "0/1")
        self.assertEqual(lane[7], "hot runtime row missing")

        surface_rows = module.route_timing_surface_comparison_table(
            [hot, publication]
        )["rows"]
        by_surface = {row[1]: row for row in surface_rows}
        self.assertEqual(by_surface["hot_runtime"][2], "Hot route geomean")
        self.assertEqual(
            by_surface["publication_proof"][2],
            "Publication-proof route geomean",
        )
        self.assertEqual(by_surface["hot_runtime"][6], "0.34 ms")
        self.assertEqual(by_surface["publication_proof"][6], "13.82 ms")

    def test_benchmark_promoter_projects_hot_runtime_rows_from_publication_rows(
        self,
    ) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_hot_runtime_projection_for_test",
        )

        publication_row = {
            "engine": "shardloom-prepared-vortex",
            "storage_format": "vortex",
            "scenario_name": "warm prepared publication-only row",
            "status": "success",
            "selected_execution_mode": "prepared_vortex",
            "requested_execution_mode": "prepared_vortex",
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "source_state_id": "source-state://projection",
            "source_state_digest": "sha256:projection-source",
            "prepared_state_id": "prepared-state://projection",
            "prepared_state_digest": "sha256:projection-prepared",
            "data_decoded": False,
            "data_materialized": False,
            "iterations": 3,
            "reproducibility_min_iterations": 3,
            "reproducibility_iterations_met": True,
            "correctness_digest": "sha256:projection",
            "correctness_digest_stable": True,
            "runtime_execution_certificate_id": "execution.projection",
            "runtime_execution_certificate_status": "certified",
            "claim_gate_status": "claim_grade",
            "claim_grade_requirements_met": True,
            "claim_grade_missing_evidence": [],
            "actual_evidence_tier": "publication_full",
            "computed_result_sink_replay_verified": True,
            "result_sink_replay_micros": 1200,
            "metrics": {
                "query_runtime_millis": 0.34,
                "vortex_scan_millis": 0.1,
                "operator_compute_millis": 0.2,
                "result_sink_write_millis": 5.33,
                "evidence_render_millis": 8.15,
                "total_runtime_millis": 13.82,
                "cli_process_wall_millis": 14.0,
            },
        }

        [publication] = module.published_rows([publication_row])
        publication_claim_status = publication["claim_gate_status"]
        projected = module.rows_with_hot_runtime_surface_projections([publication])

        self.assertEqual(len(projected), 2)
        by_surface = {row["timing_surface"]: row for row in projected}
        hot = by_surface["hot_runtime"]
        publication = by_surface["publication_proof"]

        self.assertEqual(hot["actual_evidence_tier"], "metadata_sink")
        self.assertEqual(hot["selected_evidence_tier"], "metadata_sink")
        self.assertEqual(hot["sink_tier"], "metadata_sink")
        self.assertEqual(hot["claim_gate_status"], "not_claim_grade")
        self.assertEqual(hot["total_route_ms"], 0.34)
        self.assertEqual(hot["hot_route_total_ms"], 0.34)
        self.assertFalse(hot["output_timing_included_in_total"])
        self.assertFalse(hot["evidence_timing_included_in_total"])
        self.assertIn("timing_surface=hot_runtime", hot["route_total_formula"])
        self.assertEqual(publication["claim_gate_status"], publication_claim_status)
        self.assertEqual(publication["actual_evidence_tier"], "publication_full")
        self.assertEqual(publication["total_route_ms"], 13.82)
        self.assertEqual(
            len(module.rows_with_hot_runtime_surface_projections(projected)),
            2,
        )

    def test_route_share_fails_closed_on_excluded_hot_stage(self) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_route_share_non_additive_for_test",
        )

        row = {
            "engine": "shardloom-prepared-vortex",
            "storage_format": "vortex",
            "scenario_name": "warm prepared non-additive operator timing",
            "status": "success",
            "selected_execution_mode": "prepared_vortex",
            "requested_execution_mode": "prepared_vortex",
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "source_state_id": "source-state://non-additive",
            "source_state_digest": "sha256:non-additive-source",
            "prepared_state_id": "prepared-state://non-additive",
            "prepared_state_digest": "sha256:non-additive-prepared",
            "data_decoded": False,
            "data_materialized": False,
            "iterations": 3,
            "reproducibility_min_iterations": 3,
            "reproducibility_iterations_met": True,
            "correctness_digest": "sha256:non-additive",
            "correctness_digest_stable": True,
            "runtime_execution_certificate_id": "execution.non-additive",
            "runtime_execution_certificate_status": "certified",
            "runtime_execution_certificate_plan_ref": "plan://non-additive",
            "claim_gate_status": "not_claim_grade",
            "claim_grade_requirements_met": False,
            "claim_grade_missing_evidence": ["fixture_not_claim_grade"],
            "requested_evidence_tier": "metadata_sink",
            "actual_evidence_tier": "metadata_sink",
            "metrics": {
                "query_runtime_millis": 0.12,
                "vortex_scan_millis": 0.0,
                "operator_compute_millis": 1.4,
                "total_runtime_millis": 0.12,
                "cli_process_wall_millis": 0.2,
            },
        }

        [published] = module.published_rows([row])
        route_share = module.route_share_amdahl_table([published])
        [route_row] = route_share["rows"]

        self.assertEqual(
            published["operator_compute_route_relation_schema_version"],
            "shardloom.operator_compute_route_relation.v1",
        )
        self.assertEqual(
            published["operator_compute_route_relation_status"],
            "diagnostic_only_exceeds_route_total",
        )
        self.assertFalse(published["operator_compute_included_in_route_total"])
        self.assertEqual(
            published["operator_compute_route_stage_inclusion_class"],
            "diagnostic_only",
        )
        self.assertEqual(
            published["operator_compute_route_total_field"],
            "route_timing_included_stage_total_ms",
        )
        self.assertEqual(published["operator_compute_route_total_ms"], 0.12)
        self.assertEqual(published["operator_compute_route_total_delta_ms"], 1.28)
        self.assertIn(
            "operator_compute_millis is interpreted through the selected timing surface",
            published["operator_compute_route_relation_claim_boundary"],
        )
        self.assertEqual(route_row[1], "hot_runtime")
        self.assertEqual(route_row[5], "Operator compute (excluded diagnostic)")
        self.assertEqual(route_row[7], "n/a")
        self.assertEqual(
            route_row[8],
            "fix_timing_surface_stage_inclusion_before_optimization",
        )
        self.assertEqual(route_row[9], "not_optimization_ready")
        self.assertEqual(route_row[10], "operator_compute")

    def test_benchmark_promoter_marks_expensive_stage_without_substages_not_ready(
        self,
    ) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_route_instrument_readiness_for_test",
        )

        canonical_stage_ids = ",".join(self._canonical_route_timing_stage_ids())
        row = {
            "engine": "shardloom-vortex",
            "status": "success",
            "actual_evidence_tier": "metadata_sink",
        }
        stage_fields = {
            "operator_compute_ms": 12.0,
            "timing_surface": "hot_runtime",
        }
        inclusion_fields = {
            "route_timing_stage_inclusion_classes": self._packed_route_stage_map(
                "included_hot_runtime"
            ),
            "route_timing_stage_inclusion_timing_scopes": self._packed_route_stage_map(
                "hot_runtime:native_vortex_query_only"
            ),
        }

        not_ready = module.route_timing_instrument_fields_for_row(
            row,
            stage_fields,
            inclusion_fields,
        )

        self.assertEqual(
            not_ready["route_timing_instrument_schema_version"],
            "shardloom.route_timing_instrument.v1",
        )
        self.assertEqual(not_ready["route_timing_instrument_stage_ids"], canonical_stage_ids)
        self.assertEqual(not_ready["route_timing_instrument_status"], "not_optimization_ready")
        self.assertEqual(
            not_ready["route_timing_instrument_expensive_stage_ids"],
            "operator_compute",
        )
        self.assertEqual(
            not_ready["route_timing_instrument_missing_substage_attribution"],
            "operator_compute",
        )
        self.assertIn(
            "operator_compute:not_optimization_ready_missing_substage_attribution",
            not_ready["route_timing_instrument_residual_treatments"],
        )

        ready = module.route_timing_instrument_fields_for_row(
            {**row, "operator_kernel_micros": 500},
            stage_fields,
            inclusion_fields,
        )

        self.assertEqual(ready["route_timing_instrument_status"], "optimization_ready")
        self.assertEqual(ready["route_timing_instrument_not_ready_stage_ids"], "none")

    def test_benchmark_promoter_uses_derived_substages_for_readiness(self) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_published_route_instrument_readiness_for_test",
        )

        row = {
            "engine": "shardloom-vortex",
            "status": "success",
            "actual_evidence_tier": "metadata_sink",
            "timing_surface": "hot_runtime",
            "claim_gate_status": "not_claim_grade",
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "metrics": {
                "query_runtime_millis": 12.0,
                "operator_compute_millis": 12.0,
            },
        }

        [published] = module.published_rows([row])

        self.assertEqual(published["operator_compute_ms"], 12.0)
        self.assertEqual(published["operator_kernel_micros"], 12_000)
        self.assertEqual(
            published["route_timing_instrument_expensive_stage_ids"],
            "operator_compute",
        )
        self.assertEqual(
            published["route_timing_instrument_missing_substage_attribution"],
            "none",
        )
        self.assertEqual(
            published["route_timing_instrument_status"],
            "optimization_ready",
        )

    def test_benchmark_promoter_merges_hot_rows_without_replacing_publication_rows(
        self,
    ) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_merge_timing_surface_rows_for_test",
        )

        base = {
            "engine": "shardloom-prepared-vortex",
            "scenario_id": "selective_filter",
            "scenario_name": "csv: selective filter",
            "storage_format": "csv",
            "selected_execution_mode": "prepared_vortex",
            "route_lane_id": "warm_prepared_query",
        }
        publication = {
            **base,
            "timing_surface": "publication_proof",
            "actual_evidence_tier": "publication_full",
            "timing_surface_evidence_tier": "publication_full",
            "total_route_ms": 13.82,
        }
        hot = {
            **base,
            "timing_surface": "hot_runtime",
            "actual_evidence_tier": "metadata_sink",
            "timing_surface_evidence_tier": "metadata_sink",
            "total_route_ms": 0.34,
        }
        replacement_hot = {
            **hot,
            "total_route_ms": 0.31,
        }

        merged = module.merge_published_rows([publication, hot], [replacement_hot])

        self.assertEqual(len(merged), 2)
        by_surface = {row["timing_surface"]: row for row in merged}
        self.assertEqual(by_surface["publication_proof"]["total_route_ms"], 13.82)
        self.assertEqual(by_surface["hot_runtime"]["total_route_ms"], 0.31)

    def test_benchmark_promoter_uses_measured_lane_sha_for_manifest_identity(
        self,
    ) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_measured_lane_sha_for_test",
        )
        expected = "0123456789abcdef0123456789abcdef01234567"

        self.assertEqual(
            module.benchmark_git_sha_for_artifact(
                {
                    "engine_versions": {
                        "shardloom-vortex": {
                            "available": True,
                            "version": f"workspace-local-release-{expected}",
                        }
                    }
                }
            ),
            expected,
        )

    def test_benchmark_promoter_keeps_source_state_prepare_out_of_source_admission(
        self,
    ) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_timing_normalization_for_test",
        )

        row = {
            "engine": "shardloom",
            "storage_format": "csv",
            "scenario_name": "source state timing split",
            "status": "success",
            "selected_execution_mode": "compatibility_import_certified",
            "requested_execution_mode": "compatibility_import_certified",
            "timing_scope": "cold_certified_end_to_end",
            "compatibility_import_included": True,
            "source_state_id": "source-state://timing-normalization-row",
            "source_state_digest": "sha256:source",
            "prepared_state_id": "prepared-state://timing-normalization-row",
            "prepared_state_digest": "sha256:prepared",
            "data_decoded": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "runtime_execution_certificate_id": "execution.timing-normalization-row",
            "runtime_execution_certificate_status": "certified",
            "claim_gate_status": "claim_grade",
            "claim_grade_requirements_met": True,
            "claim_grade_missing_evidence": [],
            "iterations": 3,
            "reproducibility_min_iterations": 3,
            "reproducibility_iterations_met": True,
            "correctness_digest": "sha256:correct",
            "correctness_digest_stable": True,
            "computed_result_sink_replay_verified": True,
            "metrics": {
                "source_state_prepare_micros": 2500,
                "source_read_millis": 10.0,
                "compatibility_parse_millis": 12.0,
                "source_to_columnar_millis": 4.0,
                "vortex_write_millis": 70.0,
                "vortex_reopen_verify_millis": 5.0,
                "vortex_scan_millis": 1.0,
                "operator_compute_millis": 2.0,
                "operator_kernel_micros": 0,
                "result_sink_write_millis": 3.0,
                "evidence_render_millis": 4.0,
                "total_runtime_millis": 130.0,
                "cli_process_wall_millis": 135.0,
                "python_harness_overhead_millis": 5.0,
            },
        }

        [published] = module.published_rows([row])

        self.assertIsNone(published["source_admission_ms"])
        self.assertIsNone(published["exclusive_source_admission_ms"])
        self.assertIsNone(published["source_admission_policy_micros"])
        self.assertEqual(published["source_state_open_micros"], 2500)
        self.assertEqual(
            published["source_admission_digest_policy_schema_version"],
            "shardloom.traditional_analytics.source_admission_digest_policy.v1",
        )
        self.assertEqual(
            published["source_admission_digest_policy_status"],
            "not_reported_by_engine",
        )
        self.assertFalse(published["source_admission_full_content_digest_requested"])
        self.assertIsNone(published["source_state_family_build_micros"])
        self.assertIsNone(published["source_state_lazy_family_construction"])
        self.assertIsNone(published["source_state_family_build_count"])
        self.assertIsNone(published["source_state_family_reuse_hit_count"])
        self.assertIsNone(published["source_state_family_reuse_hit"])
        self.assertIsNone(published["source_state_family_recompute_avoided"])
        self.assertIsNone(published["source_state_family_build_timing_scope"])
        self.assertEqual(published["operator_kernel_micros"], 0)
        self.assertEqual(
            published["timing_normalization_schema_version"],
            "shardloom.traditional_analytics.timing_normalization.v1",
        )
        self.assertEqual(
            published["timing_normalization_status"],
            "complete_with_unmeasured_optional_fields",
        )
        self.assertEqual(
            published["route_timing_stage_inclusion_schema_version"],
            "shardloom.route_timing_stage_inclusion.v1",
        )
        self.assertEqual(published["route_timing_stage_inclusion_status"], "complete")
        self.assertIn(
            "source_admission:diagnostic_only",
            published["route_timing_stage_inclusion_classes"],
        )
        self.assertIn(
            "cli_process_wall:excluded_harness",
            published["route_timing_stage_inclusion_classes"],
        )

    def test_benchmark_promoter_emits_source_scout_and_scan_attribution(self) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_scout_scan_attribution_for_test",
        )

        row = {
            "engine": "shardloom",
            "storage_format": "parquet",
            "scenario_name": "source scout scan split",
            "status": "success",
            "selected_execution_mode": "compatibility_import_certified",
            "requested_execution_mode": "compatibility_import_certified",
            "timing_scope": "cold_certified_end_to_end",
            "compatibility_import_included": True,
            "rows_scanned": 3,
            "source_state_id": "source-state://scout-scan-row",
            "source_state_digest": "sha256:source",
            "prepared_state_id": "prepared-state://scout-scan-row",
            "prepared_state_digest": "sha256:prepared",
            "data_decoded": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "runtime_execution_certificate_id": "execution.scout-scan-row",
            "runtime_execution_certificate_status": "certified",
            "claim_gate_status": "claim_grade",
            "claim_grade_requirements_met": True,
            "claim_grade_missing_evidence": [],
            "iterations": 3,
            "reproducibility_min_iterations": 3,
            "reproducibility_iterations_met": True,
            "correctness_digest": "sha256:correct",
            "correctness_digest_stable": True,
            "computed_result_sink_replay_verified": True,
            "metrics": {
                "source_stat_micros": 1000,
                "exclusive_source_read_millis": 12.0,
                "source_read_header_scout_micros": 1000,
                "source_read_byte_acquisition_millis": 4.0,
                "source_read_full_body_millis": 7.0,
                "source_read_typed_decode_millis": 6.0,
                "source_read_row_assembly_micros": 0,
                "source_read_anomaly_quarantine_micros": 0,
                "source_read_columnar_handoff_millis": 2.0,
                "source_read_columnar_handoff_micros": 2000,
                "source_read_scout_status": "source_read_scout_split_recorded",
                "source_read_scout_reuse_status": "not_reused_fresh_source_read",
                "source_read_decode_status": "projection_aware_columnar_provider_decode",
                "source_read_projected_field_mask": "0x00000007",
                "source_read_filter_field_mask": "0x00000004",
                "source_read_decoded_columns": "fact.id|fact.metric|fact.flag",
                "source_read_skipped_columns": "fact.event_date|fact.raw_event_time",
                "source_read_decoded_column_count": 3,
                "source_read_skipped_column_count": 2,
                "source_read_row_materialization_status": (
                    "columnar_provider_batches_without_row_structs"
                ),
                "source_read_unsupported_shape_diagnostic": "not_applicable_non_text_source",
                "source_state_columnar_preserved": True,
                "source_state_record_batch_count": 2,
                "source_state_query_dim_row_count_reuse_status": (
                    "reused_imported_dim_row_count_for_query_dispatch"
                ),
                "compatibility_parse_millis": 6.0,
                "source_to_columnar_millis": 2.0,
                "vortex_write_millis": 25.0,
                "vortex_reopen_verify_millis": 1.0,
                "vortex_footer_open_micros": 100,
                "vortex_metadata_verify_micros": 200,
                "vortex_scan_open_micros": 300,
                "vortex_scenario_scan_micros": 400,
                "vortex_scan_millis": 0.8,
                "vortex_scan_bytes_touched": 2048,
                "vortex_scan_segments_touched": 4,
                "vortex_scan_segments_skipped": 2,
                "vortex_scan_columns_touched": 3,
                "vortex_scan_decoded_values": 0,
                "operator_compute_millis": 1.2,
                "result_sink_write_millis": 0.5,
                "evidence_render_millis": 0.3,
                "total_runtime_millis": 49.0,
                "cli_process_wall_millis": 51.0,
                "python_harness_overhead_millis": 2.0,
            },
        }

        [published] = module.published_rows([row])

        self.assertEqual(
            published["source_read_scout_schema_version"],
            "shardloom.traditional_analytics.source_read_scout.v1",
        )
        self.assertEqual(
            published["source_read_scout_timing_split_status"], "complete"
        )
        self.assertEqual(published["source_read_header_scout_ms"], 1.0)
        self.assertEqual(published["source_read_byte_acquisition_ms"], 4.0)
        self.assertEqual(published["source_read_full_body_ms"], 7.0)
        self.assertEqual(published["source_read_typed_decode_ms"], 6.0)
        self.assertEqual(published["source_read_row_assembly_ms"], 0.0)
        self.assertEqual(published["source_read_anomaly_quarantine_ms"], 0.0)
        self.assertEqual(published["source_read_columnar_handoff_ms"], 2.0)
        self.assertEqual(published["source_read_scout_residual_ms"], 0.0)
        self.assertEqual(
            published["source_state_read_plan"], "projection_aware_source_scout"
        )
        self.assertEqual(
            published["source_state_projection_pushdown_status"],
            "reader_projection_applied",
        )
        self.assertEqual(
            published["source_state_reader_projection_columns"],
            "fact.id,fact.metric,fact.flag",
        )
        self.assertEqual(published["source_state_reader_projection_column_count"], 3)
        self.assertEqual(published["source_state_projected_field_mask"], "0x00000007")
        self.assertEqual(published["source_state_filter_field_mask"], "0x00000004")
        self.assertEqual(
            published["source_state_decoded_columns"], "fact.id,fact.metric,fact.flag"
        )
        self.assertEqual(
            published["source_state_skipped_columns"],
            "fact.event_date,fact.raw_event_time",
        )
        self.assertEqual(published["source_state_decoded_column_count"], 3)
        self.assertEqual(published["source_state_skipped_column_count"], 2)
        self.assertEqual(
            published["source_state_query_dim_row_count_reuse_status"],
            "reused_imported_dim_row_count_for_query_dispatch",
        )
        self.assertEqual(
            published["source_columnar_provider_schema_version"],
            "shardloom.traditional_analytics.source_columnar_provider.v1",
        )
        self.assertEqual(
            published["source_columnar_provider_status"],
            "admitted_projected_direct_columnar_provider",
        )
        self.assertEqual(
            published["source_columnar_provider_surface"],
            "vortex_provider_record_batch",
        )
        self.assertEqual(published["source_columnar_source_family"], "already_columnar_source")
        self.assertEqual(published["source_columnar_input_format"], "parquet")
        self.assertEqual(published["source_columnar_projected_field_mask"], "0x00000007")
        self.assertEqual(published["source_columnar_preserved_column_count"], 3)
        self.assertEqual(published["source_columnar_skipped_column_count"], 2)
        self.assertEqual(published["source_columnar_materialized_row_count"], 0)
        self.assertEqual(published["source_columnar_record_batch_count"], 2)
        self.assertEqual(
            published["source_columnar_row_materialization_status"],
            "columnar_provider_batches_without_row_structs",
        )
        self.assertEqual(
            published["source_columnar_projection_pushdown_status"],
            "reader_projection_pushed_down",
        )
        self.assertEqual(
            published["source_columnar_projection_pushdown_provider"],
            "parquet_projection_mask_roots",
        )
        self.assertEqual(
            published["source_columnar_null_validity_status"],
            "no_null_heavy_column_required",
        )
        self.assertEqual(
            published["source_columnar_unsupported_dtype_reason"],
            "none_supported_benchmark_columnar_shape",
        )
        self.assertEqual(published["source_columnar_handoff_micros"], 2000)
        self.assertEqual(published["source_to_vortex_handoff_micros"], 2000)
        self.assertEqual(
            published["source_columnar_correctness_digest_status"],
            "covered_by_route_correctness_digest",
        )
        self.assertFalse(published["source_columnar_fallback_attempted"])
        self.assertFalse(published["source_columnar_external_engine_invoked"])
        self.assertIn(
            "source columnar-provider admission is scenario-scoped",
            published["source_columnar_claim_boundary"],
        )
        self.assertEqual(
            published["vortex_reopen_scan_attribution_schema_version"],
            "shardloom.traditional_analytics.vortex_reopen_scan_attribution.v1",
        )
        self.assertEqual(published["vortex_reopen_verify_split_status"], "complete")
        self.assertEqual(published["vortex_scan_counter_status"], "complete")
        self.assertEqual(published["vortex_scan_bytes_touched"], 2048)
        self.assertEqual(published["vortex_scan_decoded_values"], 0)

        route_share = module.route_share_amdahl_table([published])
        self.assertEqual(
            route_share["schema_version"],
            "shardloom.traditional_analytics.route_share_amdahl.v1",
        )
        self.assertEqual(route_share["rows"][0][5], "Vortex write")
        self.assertEqual(
            route_share["rows"][0][8],
            "continue_workspace_safe_writer_metadata_coalescing",
        )

    def test_benchmark_promoter_blocks_complete_source_scout_when_diagnostic_pieces_missing(
        self,
    ) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_source_scout_incomplete_for_test",
        )

        row = {
            "engine": "shardloom",
            "storage_format": "avro",
            "scenario_name": "filter + projection + limit",
            "status": "success",
            "selected_execution_mode": "compatibility_import_certified",
            "requested_execution_mode": "compatibility_import_certified",
            "timing_scope": "cold_certified_end_to_end",
            "compatibility_import_included": True,
            "source_state_id": "source-state://coarse-scout-row",
            "source_state_digest": "sha256:source",
            "prepared_state_id": "prepared-state://coarse-scout-row",
            "prepared_state_digest": "sha256:prepared",
            "data_decoded": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "runtime_execution_certificate_id": "execution.coarse-scout-row",
            "runtime_execution_certificate_status": "certified",
            "claim_gate_status": "claim_grade",
            "claim_grade_requirements_met": True,
            "claim_grade_missing_evidence": [],
            "iterations": 3,
            "reproducibility_min_iterations": 3,
            "reproducibility_iterations_met": True,
            "correctness_digest": "sha256:correct",
            "correctness_digest_stable": True,
            "computed_result_sink_replay_verified": True,
            "metrics": {
                "exclusive_source_read_millis": 12.0,
                "source_read_header_scout_millis": 1.0,
                "source_read_byte_acquisition_millis": 4.0,
                "source_read_full_body_millis": 7.0,
                "compatibility_parse_millis": 6.0,
                "source_to_columnar_millis": 2.0,
                "total_runtime_millis": 20.0,
            },
        }

        [published] = module.published_rows([row])

        self.assertEqual(
            published["source_read_scout_timing_split_status"],
            "blocked_missing_source_read_scout_split",
        )
        self.assertIsNone(published["source_read_typed_decode_ms"])
        self.assertIsNone(published["source_read_columnar_handoff_ms"])
        self.assertEqual(published["source_read_scout_residual_ms"], 0.0)

    def test_benchmark_promoter_flags_common_run_timing_drift(self) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_common_run_drift_for_test",
        )

        previous_summary = {
            "comparative_dashboard": {
                "engine_timing_overview": {
                    "rows": [
                        ["shardloom", "yes", "1/1", "100.00 ms"],
                        ["shardloom-vortex", "yes", "1/1", "5.00 ms"],
                        ["pandas", "yes", "1/1", "200.00 ms"],
                        ["polars-eager", "yes", "1/1", "40.00 ms"],
                        ["duckdb", "yes", "1/1", "80.00 ms"],
                    ]
                }
            }
        }
        current_engine_timing = {
            "rows": [
                ["shardloom", "yes", "1/1", "126.00 ms"],
                ["shardloom-vortex", "yes", "1/1", "6.30 ms"],
                ["pandas", "yes", "1/1", "250.00 ms"],
                ["polars-eager", "yes", "1/1", "52.00 ms"],
                ["duckdb", "yes", "1/1", "100.00 ms"],
            ]
        }

        drift = module.common_run_timing_drift_table(
            previous_summary,
            current_engine_timing,
        )

        self.assertEqual(drift["status"], "common_run_slowdown_detected")
        self.assertEqual(drift["control_engine_count"], 3)
        self.assertEqual(drift["control_slow_count"], 3)
        self.assertGreater(drift["control_route_geomean_ratio"], 1.10)
        self.assertIn("common-run drift", drift["interpretation"])
        self.assertIn("shardloom", {row[4] for row in drift["rows"]})
        self.assertIn("control_baseline", {row[4] for row in drift["rows"]})

    def test_benchmark_promoter_prefers_chunks_for_summary_only_inline_rows(
        self,
    ) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_summary_only_chunk_preference_for_test",
        )

        target = REPO_ROOT / "target"
        target.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=target) as tempdir:
            chunk_path = Path(tempdir) / "published-benchmark-rows.json"
            chunk_path.write_text(
                json.dumps(
                    {
                        "rows": [
                            {
                                "engine": "shardloom",
                                "source_state_id": "source-state://chunk",
                                "claim_gate_status": "claim_grade",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            artifact = {
                "published_benchmark_rows_inlined": "summary_only",
                "published_benchmark_row_count": 1,
                "published_benchmark_rows": [
                    {
                        "engine": "shardloom",
                        "source_state_id": None,
                        "claim_gate_status": "not_claim_grade",
                    }
                ],
                "published_benchmark_row_chunks": [
                    {
                        "path": chunk_path.relative_to(REPO_ROOT).as_posix(),
                        "row_count": 1,
                    }
                ],
            }

            rows = module.artifact_rows(artifact)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["source_state_id"], "source-state://chunk")
        self.assertEqual(rows[0]["claim_gate_status"], "claim_grade")

    def test_benchmark_promoter_rejects_summary_only_inline_without_chunks(
        self,
    ) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_summary_only_missing_chunks_for_test",
        )

        artifact = {
            "published_benchmark_rows_inlined": "summary_only",
            "published_benchmark_row_count": 1,
            "published_benchmark_rows": [
                {
                    "engine": "shardloom",
                    "source_state_id": None,
                    "claim_gate_status": "not_claim_grade",
                }
            ],
        }

        self.assertEqual(module.artifact_rows(artifact), [])

    def test_benchmark_promoter_admits_row_chunks_incrementally(self) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_incremental_row_admission_for_test",
        )

        target = REPO_ROOT / "target"
        target.mkdir(exist_ok=True)
        rows = [
            {"engine": "shardloom", "scenario_id": f"scenario-{index}"}
            for index in range(5)
        ]
        with tempfile.TemporaryDirectory(dir=target) as tempdir:
            output_dir = Path(tempdir)
            chunks = module.write_row_chunks(output_dir, rows, chunk_size=2)
            admission_path = module.row_admission_manifest_path_for_chunks(
                chunks,
                output_dir,
            )
            admission = json.loads(admission_path.read_text(encoding="utf-8"))
            chunk_dir = admission_path.parent

            self.assertEqual(len(chunks), 3)
            self.assertEqual(
                admission["schema_version"],
                module.ROW_ADMISSION_MANIFEST_SCHEMA_VERSION,
            )
            self.assertTrue(str(chunks[0]["path"]).startswith("target/"))
            self.assertIn("/published-row-runs/rows-", str(chunks[0]["path"]))
            self.assertTrue(str(chunks[0]["path"]).endswith(".json.gz"))
            self.assertEqual(chunks[0]["content_encoding"], "gzip")
            self.assertIn("uncompressed_sha256", chunks[0])
            self.assertEqual(
                module.load_json(REPO_ROOT / chunks[0]["path"])["row_count"],
                2,
            )
            self.assertEqual(admission["row_count"], 5)
            self.assertEqual(admission["chunk_count"], 3)
            self.assertEqual(admission["written_chunk_count"], 3)
            self.assertEqual(admission["reused_chunk_count"], 0)
            self.assertFalse(admission["fallback_attempted"])
            self.assertFalse(admission["external_engine_invoked"])

            duplicate = output_dir / "published-benchmark-rows-001 2.json"
            duplicate.write_text("duplicate", encoding="utf-8")
            legacy = output_dir / "published-benchmark-rows-000.json"
            legacy.write_text("legacy", encoding="utf-8")
            stale = chunk_dir / "published-benchmark-rows-099.json"
            stale.write_text("stale", encoding="utf-8")
            stale_run = output_dir / module.PUBLISHED_ROW_RUN_DIR / "rows-stale"
            stale_run.mkdir(parents=True)
            (stale_run / "published-benchmark-rows-000.json").write_text(
                "stale run",
                encoding="utf-8",
            )

            chunks = module.write_row_chunks(output_dir, rows, chunk_size=2)
            admission_path = module.row_admission_manifest_path_for_chunks(
                chunks,
                output_dir,
            )
            admission = json.loads(admission_path.read_text(encoding="utf-8"))

            self.assertEqual(len(chunks), 3)
            self.assertEqual(admission["resume_status"], "reused_existing_chunks")
            self.assertEqual(admission["written_chunk_count"], 0)
            self.assertEqual(admission["reused_chunk_count"], 3)
            self.assertFalse(duplicate.exists())
            self.assertFalse(legacy.exists())
            self.assertFalse(stale.exists())
            self.assertFalse(stale_run.exists())
            self.assertTrue(admission["duplicate_suffixed_artifacts_removed"])
            self.assertTrue(admission["legacy_top_level_chunk_files_removed"])
            self.assertTrue(admission["stale_chunk_files_removed"])
            self.assertTrue(admission["stale_row_run_dirs_removed"])

    def test_benchmark_completeness_validates_row_admission_manifest(self) -> None:
        module = self._load_script_module(
            "check_benchmark_artifact_completeness.py",
            "benchmark_completeness_row_admission_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            temp = Path(tempdir)
            admission_path = temp / "benchmark-row-admission-manifest.json"
            admission_path.write_text(
                json.dumps(
                    {
                        "schema_version": module.ROW_ADMISSION_MANIFEST_SCHEMA_VERSION,
                        "row_count": 2,
                        "chunk_count": 1,
                        "chunks": [
                            {
                                "path": "website/assets/benchmarks/latest/published-benchmark-rows-000.json",
                                "row_count": 2,
                                "sha256": "sha256:chunk",
                            }
                        ],
                        "fallback_attempted": False,
                        "external_engine_invoked": False,
                    }
                ),
                encoding="utf-8",
            )
            manifest_path = temp / "manifest.json"
            manifest = {
                "artifact_paths": {
                    "row_admission_manifest": admission_path.as_posix()
                }
            }
            payload = {
                "published_benchmark_row_count": 2,
                "published_benchmark_row_chunks": [
                    {
                        "path": "website/assets/benchmarks/latest/published-benchmark-rows-000.json",
                        "row_count": 2,
                        "sha256": "sha256:chunk",
                    }
                ],
            }
            blockers: list[str] = []

            module.validate_row_admission_manifest(
                manifest,
                manifest_path,
                payload,
                blockers,
            )

        self.assertEqual(blockers, [])

    def _prepare_batch_role_repair_row(
        self,
        *,
        strategy: str,
        partial_repair_status: str,
        repaired_roles: str,
        reused_roles: str = "dim_input,cdc_delta_input",
        regeneration_performed: bool = True,
    ) -> dict[str, object]:
        return {
            "engine": "shardloom-prepare-batch",
            "status": "success",
            "storage_format": "csv",
            "selected_execution_mode": "prepared_vortex",
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "metrics": {
                "prepare_batch_fallback_attempted": False,
                "prepare_batch_external_engine_invoked": False,
                "prepare_batch_prepared_state_dependency_fallback_attempted": False,
                "prepare_batch_prepared_state_dependency_external_engine_invoked": False,
                "prepare_batch_prepared_state_dependency_packet_reuse_status": (
                    "single_evaluation_packet_manifest_hit"
                    if strategy == "manifest_reuse"
                    else "single_evaluation_packet_reused_for_role_repair"
                    if strategy == "role_scoped_repair"
                    else "single_evaluation_packet_reused_for_full_register"
                ),
                "prepare_batch_prepared_state_dependency_packet_rebuild_avoided_count": (
                    0 if strategy == "manifest_reuse" else 1
                ),
                "prepare_batch_prepared_state_optimization_strategy": strategy,
                "prepare_batch_prepared_state_optimization_status": (
                    "prepared_state_role_repair_admitted"
                    if strategy == "role_scoped_repair"
                    else f"prepared_state_{strategy}"
                ),
                "prepare_batch_prepared_state_optimization_repaired_roles": repaired_roles,
                "prepare_batch_prepared_state_optimization_no_fallback_policy_status": (
                    "passed_fallback_false_external_engine_false"
                ),
                "prepare_batch_prepared_state_optimization_fallback_attempted": False,
                "prepare_batch_prepared_state_optimization_external_engine_invoked": False,
                "prepare_batch_prepared_state_optimization_stale_artifact_reuse_allowed": False,
                "prepare_batch_prepared_state_partial_repair_status": partial_repair_status,
                "prepare_batch_prepared_state_partial_repair_reused_roles": reused_roles,
                "prepare_batch_prepared_state_partial_repair_repaired_roles": repaired_roles,
                "prepare_batch_prepared_state_partial_repair_regeneration_performed": (
                    regeneration_performed
                ),
                "prepare_batch_prepared_state_partial_repair_stale_segment_reuse_allowed": False,
                "prepare_batch_prepared_state_partial_repair_replay_proof": "fnv1a64:proof",
                "prepare_batch_prepared_state_partial_repair_micros": 10,
                "prepare_batch_prepared_state_partial_repair_source_to_columnar_micros": 2,
                "prepare_batch_prepared_state_partial_repair_vortex_array_build_micros": 3,
                "prepare_batch_prepared_state_partial_repair_vortex_write_micros": 4,
                "prepare_batch_prepared_state_partial_repair_vortex_reopen_verify_micros": 1,
            },
        }

    def _prepare_batch_role_repair_payload(self) -> dict[str, object]:
        return {
            "schema_version": "shardloom.prepare_batch_role_repair_evidence.v1",
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "claim_boundary": "fixture",
            "runs": [
                {
                    "case_id": "full_prepare_register",
                    "rows": [
                        self._prepare_batch_role_repair_row(
                            strategy="full_prepare_register",
                            partial_repair_status=(
                                "blocked_missing_base_manifest_full_prepare_required"
                            ),
                            repaired_roles="all_prepared_artifacts_created",
                            reused_roles="none",
                            regeneration_performed=False,
                        )
                    ],
                },
                {
                    "case_id": "manifest_reuse",
                    "rows": [
                        self._prepare_batch_role_repair_row(
                            strategy="manifest_reuse",
                            partial_repair_status="not_needed_manifest_hit",
                            repaired_roles="none",
                            reused_roles="fact_input,dim_input,cdc_delta_input",
                            regeneration_performed=False,
                        )
                    ],
                },
                {
                    "case_id": "fact_role_repair",
                    "rows": [
                        self._prepare_batch_role_repair_row(
                            strategy="role_scoped_repair",
                            partial_repair_status="admitted_role_repair_completed",
                            repaired_roles="fact_input",
                        )
                    ],
                },
                {
                    "case_id": "dim_role_repair",
                    "rows": [
                        self._prepare_batch_role_repair_row(
                            strategy="role_scoped_repair",
                            partial_repair_status="admitted_role_repair_completed",
                            repaired_roles="dim_input",
                            reused_roles="fact_input,cdc_delta_input",
                        )
                    ],
                },
                {
                    "case_id": "cdc_delta_role_repair",
                    "rows": [
                        self._prepare_batch_role_repair_row(
                            strategy="role_scoped_repair",
                            partial_repair_status="admitted_role_repair_completed",
                            repaired_roles="cdc_delta_input",
                            reused_roles="fact_input,dim_input",
                        )
                    ],
                },
            ],
        }

    def test_prepare_batch_role_repair_evidence_validator_requires_all_roles(
        self,
    ) -> None:
        module = self._load_script_module(
            "check_prepare_batch_role_repair_evidence.py",
            "prepare_batch_role_repair_evidence_for_test",
        )

        payload = self._prepare_batch_role_repair_payload()
        blockers, summary = module.validate_artifact_payload(payload)

        self.assertEqual(blockers, [])
        self.assertEqual(summary["case_count"], 5)
        self.assertEqual(
            summary["repaired_roles"],
            ["cdc_delta_input", "dim_input", "fact_input"],
        )

        payload["runs"] = [
            run
            for run in payload["runs"]
            if run["case_id"] != "cdc_delta_role_repair"
        ]
        blockers, _summary = module.validate_artifact_payload(payload)
        self.assertTrue(any("cdc_delta_role_repair" in blocker for blocker in blockers))

    def test_benchmark_completeness_validates_prepare_batch_role_repair_evidence(
        self,
    ) -> None:
        module = self._load_script_module(
            "check_benchmark_artifact_completeness.py",
            "benchmark_completeness_role_repair_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            temp = Path(tempdir)
            evidence_path = temp / "prepare-batch-role-repair-evidence.json"
            evidence_path.write_text(
                json.dumps(self._prepare_batch_role_repair_payload()),
                encoding="utf-8",
            )
            manifest_path = temp / "manifest.json"
            manifest = {
                "benchmark_profile": "full_local",
                "artifact_paths": {
                    "prepare_batch_role_repair_evidence": evidence_path.as_posix()
                },
            }
            blockers: list[str] = []

            module.validate_prepare_batch_role_repair_evidence(
                manifest,
                manifest_path,
                blockers,
            )

        self.assertEqual(blockers, [])

    def test_benchmark_completeness_requires_role_repair_evidence_for_full_local(
        self,
    ) -> None:
        module = self._load_script_module(
            "check_benchmark_artifact_completeness.py",
            "benchmark_completeness_role_repair_required_for_test",
        )
        blockers: list[str] = []

        module.validate_prepare_batch_role_repair_evidence(
            {"benchmark_profile": "full_local", "artifact_paths": {}},
            REPO_ROOT / "website" / "assets" / "benchmarks" / "latest" / "manifest.json",
            blockers,
        )

        self.assertTrue(
            any("prepare_batch_role_repair_evidence" in blocker for blocker in blockers)
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

    def test_benchmark_promoter_blocks_sparse_exclusive_query_split(self) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_sparse_exclusive_split_for_test",
        )

        cases = [
            ({}, None, None),
            (
                {"vortex_scan_millis": 0.25},
                "vortex_scan_ms",
                0.25,
            ),
            (
                {"operator_compute_millis": 0.75},
                "operator_compute_ms",
                0.75,
            ),
        ]

        for sparse_metrics, one_sided_field, one_sided_value in cases:
            with self.subTest(sparse_metrics=sparse_metrics):
                row = {
                    "engine": "shardloom-vortex",
                    "route_lane_id": "warm_prepared_query",
                    "status": "success",
                    "metrics": {
                        "total_runtime_millis": 10.0,
                        "query_runtime_millis": 9.0,
                        "result_sink_write_millis": 1.0,
                        **sparse_metrics,
                    },
                }

                stage_fields = module.route_stage_fields_for_row(row)

                self.assertIsNone(stage_fields["exclusive_prepared_query_ms"])
                self.assertEqual(
                    stage_fields["exclusive_stage_timing_status"],
                    "blocked_missing_query_split",
                )
                self.assertEqual(
                    stage_fields["route_timing_exclusive_stage_ids"],
                    "none",
                )
                self.assertIsNone(
                    stage_fields["route_timing_exclusive_stage_sum_ms"],
                )
                self.assertEqual(
                    stage_fields["route_timing_exclusive_residual_ms"],
                    9.0,
                )
                if one_sided_field is not None:
                    self.assertEqual(stage_fields[one_sided_field], one_sided_value)

    def test_benchmark_promoter_keeps_query_runtime_as_warm_route_stage(self) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_warm_query_substage_route_sum_for_test",
        )

        row = {
            "engine": "shardloom-vortex",
            "route_lane_id": "native_vortex_query",
            "status": "success",
            "metrics": {
                "query_runtime_millis": 1.0,
                "total_runtime_millis": 1.0,
                "result_sink_write_millis": 2.0,
                "evidence_render_millis": 3.0,
                "vortex_scan_open_micros": 10_000,
                "scan_chunk_iter_micros": 20_000,
                "vortex_chunk_iteration_micros": 20_000,
                "vortex_projected_field_extract_micros": 5_000,
                "vortex_encoded_kernel_evidence_micros": 15_000,
                "operator_kernel_micros": 75_000,
                "operator_finalize_micros": 0,
                "result_assembly_micros": 0,
            },
        }

        stage_fields = module.route_stage_fields_for_row(row)

        self.assertEqual(stage_fields["vortex_scan_ms"], 50.0)
        self.assertEqual(stage_fields["operator_compute_ms"], 75.0)
        self.assertEqual(stage_fields["exclusive_prepared_query_ms"], 1.0)
        self.assertEqual(stage_fields["route_timing_exclusive_stage_sum_ms"], 1.0)
        self.assertEqual(stage_fields["route_timing_exclusive_residual_ms"], 0.0)

    def test_benchmark_promoter_normalizes_scan_chunk_iteration_alias_once(self) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_scan_chunk_alias_once_for_test",
        )

        row = {
            "engine": "shardloom-vortex",
            "route_lane_id": "native_vortex_query",
            "status": "success",
            "scenario_name": "alias scan",
            "scenario_id": "alias_scan",
            "storage_format": "vortex",
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "vortex_chunk_iteration_micros": 20_000,
            "metrics": {
                "query_runtime_millis": 1.0,
                "total_runtime_millis": 1.0,
                "result_sink_write_millis": 0.0,
                "evidence_render_millis": 0.0,
                "vortex_scan_open_micros": 10_000,
                "scan_chunk_iter_micros": 20_000,
                "vortex_chunk_iteration_micros": 20_000,
                "vortex_projected_field_extract_micros": 5_000,
                "vortex_encoded_kernel_evidence_micros": 15_000,
                "operator_kernel_micros": 75_000,
                "operator_finalize_micros": 0,
                "result_assembly_micros": 0,
            },
        }

        stage_fields = module.route_stage_fields_for_row(row)
        normalized = module.timing_normalization_fields_for_row(row, stage_fields)

        self.assertEqual(normalized["scan_chunk_iter_micros"], 20_000)
        self.assertNotIn("vortex_chunk_iteration_micros", normalized)
        self.assertEqual(stage_fields["vortex_scan_ms"], 50.0)
        [published] = module.published_rows_with_current_route_timing_ledger([row])
        self.assertEqual(published["scan_chunk_iter_micros"], 20_000)
        self.assertNotIn("vortex_chunk_iteration_micros", published)

        legacy_only_row = {
            **row,
            "metrics": {
                "query_runtime_millis": 1.0,
                "total_runtime_millis": 1.0,
                "result_sink_write_millis": 0.0,
                "evidence_render_millis": 0.0,
                "vortex_scan_open_micros": 10_000,
                "vortex_projected_field_extract_micros": 5_000,
                "vortex_encoded_kernel_evidence_micros": 15_000,
                "operator_kernel_micros": 75_000,
                "operator_finalize_micros": 0,
                "result_assembly_micros": 0,
            },
        }
        [legacy_published] = module.published_rows_with_current_route_timing_ledger(
            [legacy_only_row]
        )
        self.assertEqual(legacy_published["scan_chunk_iter_micros"], 20_000)
        self.assertNotIn("vortex_chunk_iteration_micros", legacy_published)

    def test_benchmark_promoter_derives_evidence_render_proof_fields(self) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_evidence_render_proof_for_test",
        )

        shardloom_row = {
            "engine": "shardloom-vortex",
            "storage_format": "csv",
            "scenario_name": "evidence render proof",
            "status": "success",
            "claim_gate_status": "claim_grade",
            "fallback_attempted": False,
            "external_engine_invoked": False,
            **self._shardloom_benchmark_route_fields("shardloom-vortex"),
        }
        external_row = {
            "engine": "pandas",
            "storage_format": "csv",
            "scenario_name": "external baseline proof",
            "status": "success",
            "claim_gate_status": "external_baseline_only",
            "external_baseline_only": True,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            **self._external_benchmark_route_fields("pandas"),
        }

        shardloom_published, external_published = (
            module.published_rows_with_current_route_timing_ledger(
                [shardloom_row, external_row]
            )
        )

        self.assertEqual(
            shardloom_published["evidence_render_proof_schema_version"],
            "shardloom.traditional_analytics.evidence_render_proof.v1",
        )
        self.assertEqual(
            shardloom_published["evidence_render_proof_status"],
            "compact_machine_evidence_derived",
        )
        self.assertTrue(
            str(shardloom_published["evidence_render_proof_digest"]).startswith(
                "sha256:"
            )
        )
        self.assertEqual(
            shardloom_published["evidence_render_hot_path_policy"],
            "compact_facts_only_human_render_deferred",
        )
        self.assertEqual(
            shardloom_published["evidence_render_route_timing_boundary"],
            "route_total_includes_evidence_render_timing",
        )
        self.assertFalse(shardloom_published["evidence_render_fallback_attempted"])
        self.assertFalse(
            shardloom_published["evidence_render_external_engine_invoked"]
        )
        self.assertEqual(
            external_published["evidence_render_proof_status"],
            "external_baseline_only",
        )
        self.assertEqual(
            external_published["evidence_render_proof_digest"],
            "external_baseline_only",
        )

        proof_table = module.evidence_render_proof_table([shardloom_published])
        self.assertEqual(
            proof_table["schema_version"],
            "shardloom.traditional_analytics.evidence_render_proof.v1",
        )
        self.assertEqual(proof_table["rows"][0][0], "compact_machine_evidence_derived")
        self.assertEqual(proof_table["rows"][0][1], 1)

    def test_benchmark_promoter_derives_prepare_once_first_query_route(self) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_prepare_once_route_for_test",
        )

        row = {
            "engine": "shardloom-prepare-batch",
            "storage_format": "csv",
            "scenario_name": "prepared batch route row",
            "status": "success",
            "selected_execution_mode": "prepared_vortex",
            "requested_execution_mode": "prepared_vortex",
            "source_state_id": "source-state://prepared-route-row",
            "source_state_digest": "sha256:source",
            "prepared_state_id": "prepared-state://prepared-route-row",
            "prepared_state_digest": "sha256:prepared",
            "data_decoded": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "runtime_execution_certificate_id": "execution.prepared-route-row",
            "runtime_execution_certificate_status": "certified",
            "claim_gate_status": "claim_grade",
            "claim_grade_requirements_met": True,
            "claim_grade_missing_evidence": [],
            "iterations": 3,
            "reproducibility_min_iterations": 3,
            "reproducibility_iterations_met": True,
            "correctness_digest": "sha256:correct",
            "correctness_digest_stable": True,
            "computed_result_sink_replay_verified": True,
            "metrics": {
                "requested_evidence_tier": "metadata_sink",
                "actual_evidence_tier": "metadata_sink",
                "persistent_runner_status": "single_process_batch_runner_supported",
                "prepare_batch_preparation_millis": 100.0,
                "prepare_batch_source_to_columnar_millis": 20.0,
                "prepare_batch_vortex_array_build_millis": 30.0,
                "prepare_batch_vortex_write_millis": 40.0,
                "prepare_batch_vortex_reopen_verify_millis": 10.0,
                "batch_scenario_count": 20,
                "query_runtime_millis": 2.0,
                "total_runtime_millis": 2.0,
                "vortex_scan_millis": 1.0,
                "operator_compute_millis": 2.0,
                "result_sink_write_millis": 3.0,
                "evidence_render_millis": 0.5,
                "cli_process_wall_millis": 110.0,
                "batch_cli_process_wall_millis": 110.0,
                "batch_process_wall_shared": True,
            },
        }

        [prepare_batch] = module.published_rows([row])
        rows = module.rows_with_prepare_once_first_query([prepare_batch])
        by_lane = {item["route_lane_id"]: item for item in rows}

        self.assertEqual(by_lane["prepare_once_batch"]["total_route_ms"], 7.0)
        self.assertEqual(by_lane["prepare_once_first_query"]["total_route_ms"], 102.0)
        self.assertEqual(
            by_lane["prepare_once_batch"]["source_parse_or_columnar_decode_ms"],
            1.0,
        )
        self.assertEqual(by_lane["prepare_once_batch"]["source_to_vortex_array_ms"], 1.5)
        self.assertEqual(by_lane["prepare_once_batch"]["vortex_write_ms"], 2.0)
        self.assertEqual(by_lane["prepare_once_batch"]["vortex_reopen_or_verify_ms"], 0.5)
        self.assertEqual(
            by_lane["prepare_once_first_query"]["source_parse_or_columnar_decode_ms"],
            20.0,
        )
        self.assertEqual(
            by_lane["prepare_once_first_query"]["source_to_vortex_array_ms"],
            30.0,
        )
        self.assertEqual(by_lane["prepare_once_first_query"]["vortex_write_ms"], 40.0)
        self.assertEqual(
            by_lane["prepare_once_first_query"]["vortex_reopen_or_verify_ms"],
            10.0,
        )
        self.assertEqual(
            by_lane["prepare_once_first_query"]["route_row_derivation_status"],
            module.DERIVED_PREPARE_ONCE_FIRST_QUERY_STATUS,
        )
        self.assertEqual(
            by_lane["prepare_once_first_query"]["route_timing_included_stage_total_ms"],
            102.0,
        )
        self.assertEqual(
            by_lane["prepare_once_first_query"]["route_timing_total_delta_ms"],
            0.0,
        )
        self.assertEqual(
            by_lane["prepare_once_first_query"]["evidence_render_proof_status"],
            "compact_machine_evidence_derived",
        )
        self.assertTrue(
            str(
                by_lane["prepare_once_first_query"]["evidence_render_proof_digest"]
            ).startswith("sha256:")
        )
        self.assertNotEqual(
            by_lane["prepare_once_first_query"]["evidence_render_proof_digest"],
            by_lane["prepare_once_batch"]["evidence_render_proof_digest"],
        )

        amortization = module.prepared_route_amortization_table(rows)
        by_count = {item[0]: item for item in amortization["rows"]}
        self.assertEqual(set(by_count), {1, 5, 10, 50, 100})
        self.assertEqual(by_count[1][1], 1)
        self.assertEqual(by_count[1][2], "102.00 ms")
        self.assertEqual(by_count[100][2], "3.00 ms")

    def test_benchmark_promoter_emits_operator_mode_inventory(self) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_operator_mode_for_test",
        )

        row = {
            "engine": "shardloom-prepared-vortex",
            "storage_format": "csv",
            "scenario_name": "selective filter",
            "status": "success",
            "selected_execution_mode": "prepared_vortex",
            "requested_execution_mode": "prepared_vortex",
            "source_state_id": "source-state://operator-mode-row",
            "source_state_digest": "sha256:source",
            "prepared_state_id": "prepared-state://operator-mode-row",
            "prepared_state_digest": "sha256:prepared",
            "data_decoded": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "runtime_execution_certificate_id": "execution.operator-mode-row",
            "runtime_execution_certificate_status": "certified",
            "claim_gate_status": "claim_grade",
            "claim_grade_requirements_met": True,
            "claim_grade_missing_evidence": [],
            "iterations": 3,
            "reproducibility_min_iterations": 3,
            "reproducibility_iterations_met": True,
            "correctness_digest": "sha256:correct",
            "correctness_digest_stable": True,
            "computed_result_sink_replay_verified": True,
            "metrics": {
                "query_runtime_millis": 1.0,
                "vortex_scan_millis": 0.2,
                "operator_compute_millis": 0.5,
                "evidence_render_millis": 0.1,
                "cli_process_wall_millis": 1.4,
                "python_harness_overhead_millis": 0.4,
                "operator_execution_class": "residual_native",
                "operator_admission_status": "residual_native_supported",
                "operator_blocker_id": (
                    "gar-flow-2b.residual_native_operator_not_encoded_native"
                ),
                "operator_encoded_native_claim_allowed": False,
                "operator_residual_native_used": True,
                "operator_temporary_materialization_used": False,
                "operator_blocker_matrix_ref": "operator-blocker://selective-filter",
                "encoded_predicate_provider_status": "selection_vectors_admitted",
                "fused_pipeline_blocker_id": (
                    "gar-perf-1c.selection_vector_metric_aggregation_not_admitted"
                ),
            },
        }

        [published] = module.published_rows([row])

        self.assertEqual(
            published["operator_mode_inventory_schema_version"],
            "shardloom.operator_mode_inventory.v1",
        )
        self.assertEqual(published["operator_execution_mode"], "residual_native")
        self.assertFalse(published["operator_encoded_native_claim_allowed"])
        self.assertEqual(published["encoded_native_operators"], "none")
        self.assertEqual(
            published["operator_hot_path_candidate"],
            "selective_filter_selection_vector_metric_aggregation",
        )
        self.assertEqual(
            published["operator_hot_path_candidate_status"],
            "blocked_selection_vector_metric_aggregation_not_admitted",
        )

        inventory = module.operator_mode_inventory_table([row])
        candidates = module.operator_hot_path_candidate_table([row])

        self.assertEqual(inventory["schema_version"], "shardloom.operator_mode_inventory.v1")
        self.assertEqual(inventory["residual_native_row_count"], 1)
        self.assertIn("runtime-supported", inventory["claim_boundary"])
        self.assertEqual(
            candidates["rows"][0][0],
            "selective_filter_selection_vector_metric_aggregation",
        )

    def test_benchmark_promoter_emits_partial_encoded_kernel_promotion(self) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_encoded_kernel_promotion_for_test",
        )

        row = {
            "engine": "shardloom-prepared-vortex",
            "storage_format": "csv",
            "scenario_name": "group by aggregation",
            "status": "success",
            "selected_execution_mode": "prepared_vortex",
            "requested_execution_mode": "prepared_vortex",
            "source_state_id": "source-state://encoded-kernel-row",
            "source_state_digest": "sha256:source",
            "prepared_state_id": "prepared-state://encoded-kernel-row",
            "prepared_state_digest": "sha256:prepared",
            "data_decoded": False,
            "data_materialized": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "runtime_execution_certificate_id": "execution.encoded-kernel-row",
            "runtime_execution_certificate_status": "certified",
            "claim_gate_status": "claim_grade",
            "claim_grade_requirements_met": True,
            "iterations": 3,
            "reproducibility_min_iterations": 3,
            "reproducibility_iterations_met": True,
            "correctness_digest": "sha256:correct",
            "correctness_digest_stable": True,
            "computed_result_sink_replay_verified": True,
            "metrics": {
                "query_runtime_millis": 1.0,
                "vortex_scan_millis": 0.2,
                "operator_compute_millis": 0.5,
                "operator_execution_class": "residual_native",
                "operator_admission_status": "residual_native_supported",
                "operator_blocker_id": (
                    "gar-flow-2b.residual_native_operator_not_encoded_native"
                ),
                "operator_encoded_native_claim_allowed": False,
                "operator_residual_native_used": True,
                "operator_temporary_materialization_used": False,
                "operator_blocker_matrix_ref": "operator-blocker://group-by",
                "compressed_kernel_registry_pair_ids": (
                    "bitpacked_boolean_integer_filter|dictionary_equality_group_by"
                ),
                "compressed_kernel_registry_operator_families": (
                    "filter_predicate|equality_group_by"
                ),
                "compressed_kernel_registry_kernel_admitted": "false|true",
                "compressed_kernel_registry_kernel_executed": "false|true",
                "compressed_kernel_registry_decoded": "false|false",
                "compressed_kernel_registry_materialized": "false|false",
                "compressed_kernel_registry_decoded_reference_compared": "false|true",
                "compressed_kernel_registry_correctness_digest_status": (
                    "not_emitted_pair_not_executed|decoded_reference_match"
                ),
            },
        }

        [published] = module.published_rows([row])

        self.assertEqual(
            published["encoded_kernel_promotion_schema_version"],
            "shardloom.encoded_kernel_promotion.v1",
        )
        self.assertEqual(
            published["encoded_kernel_promotion_status"],
            "partial_encoded_kernel_pairs_promoted",
        )
        self.assertEqual(published["encoded_kernel_promoted_pair_count"], 1)
        self.assertEqual(
            published["encoded_kernel_promoted_pair_ids"],
            "dictionary_equality_group_by",
        )
        self.assertEqual(
            published["encoded_kernel_promoted_operator_families"],
            "equality_group_by",
        )
        self.assertFalse(published["encoded_kernel_full_operator_claim_allowed"])
        self.assertEqual(published["operator_execution_mode"], "residual_native")
        self.assertEqual(
            published["operator_hot_path_candidate"],
            "partial_encoded_kernel_to_full_operator_promotion",
        )

        promotion = module.encoded_kernel_promotion_table([published])
        self.assertEqual(
            promotion["partial_encoded_kernel_promoted_row_count"],
            1,
        )
        self.assertIn(
            "narrower than full operator mode",
            promotion["claim_boundary"],
        )

    def test_benchmark_promoter_publication_proof_sidecar_reuses_and_invalidates(
        self,
    ) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_publication_proof_sidecar_for_test",
        )

        row = {
            "engine": "shardloom-prepared-vortex",
            "scenario_id": "selective_filter",
            "scenario_name": "selective filter",
            "storage_format": "csv",
            "route_lane_id": "warm_prepared_query",
            "timing_surface": "publication_proof",
            "actual_evidence_tier": "publication_full",
            "claim_gate_status": "claim_grade",
            "evidence_render_proof_status": "compact_machine_evidence_derived",
            "evidence_render_proof_digest": "sha256:proof-a",
            "computed_result_vortex_digest": "sha256:sink-a",
            "computed_result_sink_replay_verified": True,
            "runtime_execution_certificate_id": "execution.sidecar",
            "runtime_execution_certificate_status": "certified",
            "result_sink_write_ms": 0.5,
            "evidence_render_ms": 3.0,
            "publication_proof_route_total_ms": 4.0,
            "route_total_formula": (
                "timing_surface=publication_proof; total_route_ms = "
                "query_runtime_millis + result_sink_write_millis + evidence_render_millis"
            ),
            "fallback_attempted": False,
            "external_engine_invoked": False,
        }

        with tempfile.TemporaryDirectory() as tempdir:
            output_dir = Path(tempdir)
            chunks = module.write_row_chunks(output_dir, [row])
            first = module.write_publication_proof_sidecar(output_dir, [row], chunks)
            self.assertEqual(
                first["publication_proof_sidecar_status"],
                "admitted_incremental_publication_proof_sidecar",
            )
            self.assertEqual(first["publication_proof_sidecar_record_count"], 1)
            self.assertEqual(first["publication_proof_sidecar_written_record_count"], 1)
            sidecar = json.loads(
                (output_dir / module.PUBLICATION_PROOF_SIDECAR_NAME).read_text(
                    encoding="utf-8"
                )
            )
            self.assertIs(
                sidecar["records"][0]["computed_result_sink_replay_verified"],
                True,
            )

            second = module.write_publication_proof_sidecar(output_dir, [row], chunks)
            self.assertEqual(
                second["publication_proof_sidecar_status"],
                "reused_existing_publication_proof_sidecar",
            )
            self.assertEqual(second["publication_proof_sidecar_reused_record_count"], 1)

            changed = {**row, "evidence_render_proof_digest": "sha256:proof-b"}
            changed_chunks = module.write_row_chunks(output_dir, [changed])
            third = module.write_publication_proof_sidecar(
                output_dir,
                [changed],
                changed_chunks,
            )
            self.assertEqual(
                third["publication_proof_sidecar_status"],
                "admitted_incremental_publication_proof_sidecar",
            )
            self.assertEqual(third["publication_proof_sidecar_written_record_count"], 1)
            self.assertEqual(third["publication_proof_sidecar_stale_record_count"], 0)
            changed_sidecar = json.loads(
                (output_dir / module.PUBLICATION_PROOF_SIDECAR_NAME).read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(changed_sidecar["stale_record_count"], 0)
            self.assertEqual(changed_sidecar["removed_stale_record_count"], 1)

    def test_benchmark_promoter_demotes_claim_grade_without_cold_lane_split(self) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py", "promote_benchmark_cold_lane_gate_for_test"
        )

        row = {
            "engine": "shardloom",
            "storage_format": "csv",
            "scenario_name": "claim grade missing cold lane",
            "status": "success",
            "selected_execution_mode": "compatibility_import_certified",
            "timing_scope": "cold_certified_end_to_end",
            "preparation_included": True,
            "compatibility_import_included": True,
            "source_state_id": "source-state://claim-grade-missing-cold-lane",
            "data_decoded": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "runtime_execution_certificate_id": "execution.claim-grade-missing-cold-lane",
            "runtime_execution_certificate_status": "certified",
            "claim_gate_status": "claim_grade",
            "claim_grade_requirements_met": True,
            "claim_grade_missing_evidence": [],
            "metrics": {
                "query_runtime_millis": 1.0,
                "source_read_millis": 0.1,
                "compatibility_to_vortex_import_millis": 0.2,
                "vortex_array_build_millis": 0.1,
                "vortex_write_millis": 0.1,
                "vortex_reopen_verify_millis": 0.1,
                "operator_compute_millis": 0.2,
                "total_runtime_millis": 1.0,
                "cli_process_wall_millis": 1.2,
                "python_harness_overhead_millis": 0.2,
            },
        }

        [published] = module.published_rows([row])

        self.assertEqual(published["claim_gate_status"], "not_claim_grade")
        self.assertFalse(published["claim_grade_requirements_met"])
        self.assertIn(
            "cold_lane_timing_split_status!=complete",
            published["claim_grade_missing_evidence"][0],
        )
        summary = module.comparative_summary(
            {"dataset": {}, "generated_at_utc": "2026-01-01T00:00:00Z"},
            [row],
            REPO_ROOT / "target" / "claim-grade-missing-cold-lane.json",
            "full_local",
            self._public_front_door_benchmark_rows(module),
        )
        self.assertEqual(
            summary["claim_gate_distribution"]["rows"][0][0],
            "not_claim_grade",
        )

    def test_full_local_requires_broad_formats_for_current_refresh(self) -> None:
        from benchmarks.traditional_analytics import run as benchmark_run
        from benchmarks.traditional_analytics.benchmark_registry import PROFILES

        profile = PROFILES["full_local"]

        self.assertEqual(
            benchmark_run.FORMAT_ORDER,
            ("csv", "jsonl", "parquet", "arrow-ipc", "avro", "orc"),
        )
        self.assertEqual(
            profile.required_formats,
            ("csv", "jsonl", "parquet", "arrow-ipc", "avro", "orc"),
        )
        self.assertEqual(profile.optional_formats, ())
        self.assertNotIn("pyspark", profile.required_lanes)
        self.assertNotIn("spark-default", profile.required_lanes)
        self.assertNotIn("spark-local-tuned", profile.required_lanes)
        self.assertEqual(
            benchmark_run.CLAIM_READINESS_RERUN_FORMATS,
            ("csv", "jsonl", "parquet", "arrow-ipc", "avro", "orc"),
        )
        self.assertNotIn("pyspark", benchmark_run.CLAIM_READINESS_RERUN_ENGINES)

    def test_claim_readiness_rerun_uses_all_scenario_fixture_profile(self) -> None:
        from benchmarks.traditional_analytics import run as benchmark_run

        args = benchmark_run.parse_args(["--claim-readiness-rerun"])

        self.assertEqual(args.dataset_profile, "tiny_smoke")
        self.assertIsNone(
            benchmark_run.scenario_dataset_profile_block_reason(
                "partition pruning", args.dataset_profile
            )
        )
        self.assertIsNone(
            benchmark_run.scenario_dataset_profile_block_reason(
                "many-small-files scan", args.dataset_profile
            )
        )
        self.assertIsNone(
            benchmark_run.scenario_dataset_profile_block_reason(
                "malformed timestamp / dirty CSV", args.dataset_profile
            )
        )

    def test_claim_readiness_rerun_respects_explicit_dataset_profile(self) -> None:
        from benchmarks.traditional_analytics import run as benchmark_run

        args = benchmark_run.parse_args(
            ["--claim-readiness-rerun", "--dataset-profile", "narrow_fact_dim"]
        )

        self.assertEqual(args.dataset_profile, "narrow_fact_dim")
        self.assertIsNotNone(
            benchmark_run.scenario_dataset_profile_block_reason(
                "partition pruning", args.dataset_profile
            )
        )

    def test_benchmark_runner_canonicalizes_scan_chunk_iteration_alias(self) -> None:
        from benchmarks.traditional_analytics import run as benchmark_run

        metrics = benchmark_run.vortex_scan_attribution_stage_metrics(
            {"vortex_chunk_iteration_micros": "42"}
        )

        self.assertEqual(metrics["scan_chunk_iter_micros"], 42)
        self.assertNotIn("vortex_chunk_iteration_micros", metrics)
        self.assertNotIn(
            "vortex_chunk_iteration_micros",
            benchmark_run.VORTEX_SCAN_SPLIT_MICROS_FIELDS,
        )

    def test_benchmark_runner_warms_shardloom_cli_with_status_command(self) -> None:
        from benchmarks.traditional_analytics import run as benchmark_run

        calls: list[tuple[list[str], Path, dict[str, str]]] = []

        def fake_subprocess_run(
            command: list[str], cwd: Path, env: dict[str, str]
        ) -> dict[str, object]:
            calls.append((command, cwd, env))
            return {
                "returncode": 0,
                "stdout": json.dumps(
                    {
                        "schema_version": "shardloom.output.v2",
                        "command": "status",
                        "status": "success",
                        "fallback": {"attempted": False},
                    }
                ),
                "stderr": "",
                "process_wall_millis": 12.5,
            }

        previous = benchmark_run.subprocess_run
        try:
            benchmark_run.subprocess_run = fake_subprocess_run
            benchmark_run.shardloom_cli_warmup(
                Path("/repo/target/release/shardloom"),
                Path("/repo"),
                {"RUSTUP_TOOLCHAIN": "stable"},
            )
        finally:
            benchmark_run.subprocess_run = previous

        self.assertEqual(len(calls), 1)
        command, cwd, env = calls[0]
        self.assertEqual(
            command,
            [
                "/repo/target/release/shardloom",
                "status",
                "--format",
                "json",
            ],
        )
        self.assertEqual(cwd, Path("/repo"))
        self.assertEqual(env["RUSTUP_TOOLCHAIN"], "stable")

    def test_benchmark_runner_separates_global_startup_warmup_attribution(self) -> None:
        from benchmarks.traditional_analytics import run as benchmark_run

        calls: list[Path] = []

        def fake_shardloom_cli_warmup(
            binary: Path, root: Path, env: dict[str, str]
        ) -> None:
            calls.append(binary)

        ticks = iter([0.0, 0.4, 1.0, 1.1, 2.0, 2.2])
        previous_warmup = benchmark_run.shardloom_cli_warmup
        previous_perf_counter = benchmark_run.time.perf_counter
        benchmark_run.SHARDLOOM_GLOBAL_CLI_WARMUP_MILLIS_BY_BINARY.clear()
        try:
            benchmark_run.shardloom_cli_warmup = fake_shardloom_cli_warmup
            benchmark_run.time.perf_counter = lambda: next(ticks)

            first = benchmark_run.shardloom_cli_attributed_warmup(
                Path("/repo/target/release/shardloom"),
                Path("/repo"),
                {},
            )
            second = benchmark_run.shardloom_cli_attributed_warmup(
                Path("/repo/target/release/shardloom"),
                Path("/repo"),
                {},
            )
            warmed = benchmark_run.warmup_runner(
                benchmark_run.EngineRunner(
                    "shardloom-vortex",
                    "test",
                    {},
                    warmup=lambda: second,
                    startup_time_millis=3.0,
                )
            )
        finally:
            benchmark_run.shardloom_cli_warmup = previous_warmup
            benchmark_run.time.perf_counter = previous_perf_counter
            benchmark_run.SHARDLOOM_GLOBAL_CLI_WARMUP_MILLIS_BY_BINARY.clear()

        self.assertEqual(len(calls), 2)
        self.assertEqual(first["warmup_time_millis"], 0.0)
        self.assertEqual(first["startup_warmup_scope"], "covered_by_global_cli_binary_prime")
        self.assertEqual(first["global_startup_warmup_millis"], 400.0)
        self.assertEqual(
            first["global_startup_warmup_scope"],
            "one_time_cli_binary_prime_shared_across_shardloom_lanes",
        )
        self.assertEqual(second["warmup_time_millis"], 100.0)
        self.assertEqual(
            second["startup_warmup_scope"],
            "per_lane_cli_status_warmup_after_global_prime",
        )
        self.assertEqual(second["global_startup_warmup_millis"], 400.0)
        self.assertEqual(warmed.startup_time_millis, 103.0)
        self.assertEqual(
            warmed.startup_warmup_scope,
            "per_lane_cli_status_warmup_after_global_prime",
        )
        self.assertEqual(warmed.global_startup_warmup_millis, 400.0)

    def test_benchmark_result_rows_do_not_allocate_global_startup_prime(self) -> None:
        from benchmarks.traditional_analytics import run as benchmark_run

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = benchmark_run.DatasetPaths(
                root=root,
                fact_csv=root / "fact.csv",
                dim_csv=root / "dim.csv",
                fact_jsonl=root / "fact.jsonl",
                dim_jsonl=root / "dim.jsonl",
                fact_parquet=root / "fact.parquet",
                dim_parquet=root / "dim.parquet",
                fact_arrow_ipc=root / "fact.arrow",
                dim_arrow_ipc=root / "dim.arrow",
                fact_avro=root / "fact.avro",
                dim_avro=root / "dim.avro",
                fact_orc=root / "fact.orc",
                dim_orc=root / "dim.orc",
                rows=1,
                dim_rows=1,
            )
            runner = benchmark_run.EngineRunner(
                "shardloom-vortex",
                "test",
                {},
                startup_time_millis=21.0,
                startup_warmup_scope=(
                    "per_lane_cli_status_warmup_after_global_prime"
                ),
                global_startup_warmup_millis=400.0,
                global_startup_warmup_scope=(
                    "one_time_cli_binary_prime_shared_across_shardloom_lanes"
                ),
            )
            result = benchmark_run.successful_result_from_iterations(
                runner,
                paths,
                "selective filter",
                "parquet",
                1,
                [{"row_count": 1, "metric_sum": 2.0}],
                [
                    {
                        "selected_execution_mode": "native_vortex",
                        "scenario_compute_micros": "100",
                        "operator_compute_micros": "50",
                        "total_runtime_micros": "100",
                        "source_bytes_read": "10",
                        "rows_materialized": "1",
                        "data_decoded": "false",
                        "data_materialized": "false",
                        "row_read": "false",
                        "object_store_io": "false",
                        "write_io": "false",
                        "spill_io_performed": "false",
                        "fallback_attempted": "false",
                        "external_engine_invoked": "false",
                        "runtime_fallback_attempted": "false",
                        "runtime_external_query_engine_invoked": "false",
                        "persistent_runner_status": (
                            benchmark_run.PERSISTENT_RUNNER_STATUS
                        ),
                        "session_route_used": "false",
                        "process_spawn_count": "1",
                    }
                ],
                [0.1],
                [],
            )

        metrics = result["metrics"]
        self.assertEqual(metrics["startup_warmup_millis"], 21.0)
        self.assertIsNone(metrics["global_startup_warmup_millis"])
        self.assertEqual(
            metrics["global_startup_warmup_row_allocation_status"],
            "shared_global_cli_prime_reported_in_engine_versions_not_row_allocated",
        )
        self.assertFalse(metrics["session_route_used"])
        self.assertEqual(metrics["process_spawn_count"], 1)

    def test_shared_prepared_artifact_cache_hit_zeroes_fresh_import_timing(self) -> None:
        from benchmarks.traditional_analytics import run as benchmark_run

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fact = root / "fact.vortex"
            dim = root / "dim.vortex"
            fact.write_bytes(b"fact")
            dim.write_bytes(b"dim")
            manifest = root / ".shardloom" / "prepared-vortex-reuse-manifest.json"
            manifest.parent.mkdir()
            manifest.write_text("{}\n", encoding="utf-8")
            entry = {
                "fact": fact,
                "dim": dim,
                "preparation_millis": 12.5,
                "prepared_state_lookup_or_create_millis": 12.5,
                "prepare_route_total_millis": 14.0,
                "prepare_cli_wall_millis": 22.0,
                "preparation_cli_process_wall_millis": 22.0,
                "compatibility_to_vortex_import_micros": "12345",
                "source_read_micros": "6789",
                "vortex_write_micros": "3456",
                "fact_digest": "sha256:fact",
                "dim_digest": "sha256:dim",
                "benchmark_harness_prepared_artifact_cache_creator_engine": (
                    "shardloom-vortex"
                ),
                "benchmark_harness_prepared_artifact_workspace_manifest_path": str(
                    manifest
                ),
                "benchmark_harness_prepared_artifact_workspace_manifest_status": (
                    "workspace_manifest_written"
                ),
                "benchmark_harness_prepared_artifact_workspace_manifest_write_micros": (
                    "42"
                ),
            }

            self.assertTrue(
                benchmark_run.shared_prepared_artifact_entry_is_valid(entry)
            )
            reused = benchmark_run.shared_prepared_artifact_cache_hit_entry(
                entry,
                "shardloom-prepared-vortex",
            )

        self.assertEqual(reused["preparation_millis"], 0.0)
        self.assertEqual(reused["prepared_state_lookup_or_create_millis"], 0.0)
        self.assertEqual(reused["prepare_route_total_millis"], 0.0)
        self.assertEqual(reused["preparation_cli_process_wall_millis"], 0.0)
        self.assertEqual(reused["compatibility_to_vortex_import_micros"], "0")
        self.assertEqual(reused["source_read_micros"], "0")
        self.assertEqual(reused["vortex_write_micros"], "0")
        self.assertEqual(
            reused["shared_prepared_artifact_original_preparation_millis"],
            "12.5",
        )
        self.assertEqual(
            reused[
                "shared_prepared_artifact_original_compatibility_to_vortex_import_micros"
            ],
            "12345",
        )
        self.assertEqual(
            reused["benchmark_harness_prepared_artifact_cache_status"],
            "cache_hit_reused_in_process",
        )
        self.assertEqual(
            reused["benchmark_harness_prepared_artifact_cache_creator_engine"],
            "shardloom-vortex",
        )
        self.assertEqual(
            reused["benchmark_harness_prepared_artifact_cache_consumer_engine"],
            "shardloom-prepared-vortex",
        )
        self.assertEqual(reused["prepared_state_reuse_hit"], "true")
        self.assertEqual(
            reused["prepared_state_reuse_reason"],
            "same_process_cache_hit_artifact_paths_verified",
        )
        self.assertEqual(
            reused["prepared_state_reuse_scope"],
            "benchmark_harness_shared_prepared_vortex_artifact_in_process",
        )
        self.assertEqual(
            reused["benchmark_harness_prepared_artifact_workspace_manifest_status"],
            "workspace_manifest_verified_same_process_cache_hit",
        )
        self.assertEqual(
            reused[
                "shared_prepared_artifact_original_benchmark_harness_prepared_artifact_workspace_manifest_write_micros"
            ],
            "42",
        )
        self.assertEqual(
            reused["benchmark_harness_prepared_artifact_workspace_manifest_write_micros"],
            "0",
        )

    def test_prepared_artifact_workspace_manifest_records_local_artifacts(self) -> None:
        from benchmarks.traditional_analytics import run as benchmark_run

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in [
                "fact.csv",
                "dim.csv",
                "fact.jsonl",
                "dim.jsonl",
                "fact.parquet",
                "dim.parquet",
                "fact.arrow",
                "dim.arrow",
                "fact.avro",
                "dim.avro",
                "fact.orc",
                "dim.orc",
            ]:
                (root / name).write_text("fixture\n", encoding="utf-8")
            fact_vortex = root / "workspace" / "fact.vortex"
            dim_vortex = root / "workspace" / "dim.vortex"
            fact_vortex.parent.mkdir()
            fact_vortex.write_bytes(b"fact-vortex")
            dim_vortex.write_bytes(b"dim-vortex")
            paths = benchmark_run.DatasetPaths(
                root=root,
                fact_csv=root / "fact.csv",
                dim_csv=root / "dim.csv",
                fact_jsonl=root / "fact.jsonl",
                dim_jsonl=root / "dim.jsonl",
                fact_parquet=root / "fact.parquet",
                dim_parquet=root / "dim.parquet",
                fact_arrow_ipc=root / "fact.arrow",
                dim_arrow_ipc=root / "dim.arrow",
                fact_avro=root / "fact.avro",
                dim_avro=root / "dim.avro",
                fact_orc=root / "fact.orc",
                dim_orc=root / "dim.orc",
                rows=2,
                dim_rows=1,
                dataset_profile="tiny_smoke",
            )
            fields = {
                "source_state_id": "source-state://test",
                "source_state_digest": "fnv1a64:source",
                "prepared_state_id": "prepared-state://test",
                "prepared_state_digest": "fnv1a64:prepared",
            }
            prepared = {
                "fact": fact_vortex,
                "dim": dim_vortex,
                "fact_digest": "fnv1a64:fact",
                "dim_digest": "fnv1a64:dim",
            }

            manifest_fields = (
                benchmark_run.write_shared_prepared_artifact_workspace_manifest(
                    paths=paths,
                    data_format="parquet",
                    workspace=root / "workspace",
                    binary=root / "target" / "debug" / "shardloom",
                    prepared=prepared,
                    fields=fields,
                )
            )
            manifest_path = Path(
                manifest_fields[
                    "benchmark_harness_prepared_artifact_workspace_manifest_path"
                ]
            )
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))

        self.assertEqual(
            payload["schema_version"],
            benchmark_run.SHARED_PREPARED_ARTIFACT_WORKSPACE_MANIFEST_SCHEMA_VERSION,
        )
        self.assertEqual(payload["scope"], "workspace_manifest_local_vortex_artifacts")
        self.assertEqual(payload["prepared_fact_digest"], "fnv1a64:fact")
        self.assertEqual(payload["prepared_dim_digest"], "fnv1a64:dim")
        self.assertFalse(payload["fallback_attempted"])
        self.assertFalse(payload["external_engine_invoked"])
        self.assertTrue(
            manifest_fields[
                "benchmark_harness_prepared_artifact_workspace_manifest_digest"
            ]
        )

    def test_batch_cli_process_wall_is_amortized_per_scenario(self) -> None:
        from benchmarks.traditional_analytics import run as benchmark_run

        self.assertEqual(
            benchmark_run.amortized_batch_cli_process_wall_millis(34.0, 4),
            8.5,
        )
        self.assertEqual(
            benchmark_run.amortized_batch_cli_process_wall_millis("33.3333", 3),
            11.1111,
        )
        self.assertIsNone(
            benchmark_run.amortized_batch_cli_process_wall_millis("not_measured", 4)
        )
        self.assertIsNone(
            benchmark_run.amortized_batch_cli_process_wall_millis(34.0, 0)
        )
        self.assertEqual(
            benchmark_run.row_level_batch_cli_process_wall_millis(753.4, 20),
            37.67,
        )
        self.assertEqual(
            benchmark_run.row_level_batch_cli_process_wall_millis("not_measured", 20),
            "not_measured",
        )

    def test_benchmark_runner_rejects_fallback_during_shardloom_cli_warmup(self) -> None:
        from benchmarks.traditional_analytics import run as benchmark_run

        def fake_subprocess_run(
            command: list[str], cwd: Path, env: dict[str, str]
        ) -> dict[str, object]:
            return {
                "returncode": 0,
                "stdout": json.dumps(
                    {
                        "schema_version": "shardloom.output.v2",
                        "command": "status",
                        "status": "success",
                        "fallback": {"attempted": True},
                    }
                ),
                "stderr": "",
                "process_wall_millis": 12.5,
            }

        previous = benchmark_run.subprocess_run
        try:
            benchmark_run.subprocess_run = fake_subprocess_run
            with self.assertRaises(benchmark_run.BenchmarkUnsupported):
                benchmark_run.shardloom_cli_warmup(
                    Path("/repo/target/release/shardloom"),
                    Path("/repo"),
                    {},
                )
        finally:
            benchmark_run.subprocess_run = previous

    def test_benchmark_runner_prefers_engine_preparation_timing(self) -> None:
        from benchmarks.traditional_analytics import run as benchmark_run

        self.assertEqual(
            benchmark_run.preparation_engine_millis(
                {
                    "total_runtime_micros": "26108",
                    "compatibility_to_vortex_import_micros": "25207",
                },
                34.5943,
            ),
            25.207,
        )
        self.assertEqual(
            benchmark_run.preparation_engine_millis(
                {
                    "prepare_batch_prepared_state_lookup_or_create_micros": "1400",
                    "prepare_batch_preparation_micros": "1100",
                    "total_runtime_micros": "26108",
                    "compatibility_to_vortex_import_micros": "25207",
                },
                34.5943,
            ),
            1.4,
        )
        self.assertEqual(
            benchmark_run.preparation_engine_millis(
                {
                    "prepare_batch_preparation_micros": "1100",
                    "total_runtime_micros": "26108",
                    "compatibility_to_vortex_import_micros": "25207",
                },
                34.5943,
            ),
            1.1,
        )
        self.assertEqual(
            benchmark_run.preparation_engine_millis(
                {"compatibility_to_vortex_import_micros": "25207"},
                34.5943,
            ),
            25.207,
        )
        self.assertEqual(
            benchmark_run.preparation_engine_millis({}, 34.5943),
            34.5943,
        )
        self.assertEqual(
            benchmark_run.preparation_route_total_millis(
                {
                    "prepare_batch_prepare_route_total_micros": "27108",
                    "total_runtime_micros": "26108",
                },
                34.5943,
            ),
            27.108,
        )
        self.assertEqual(
            benchmark_run.preparation_route_total_millis(
                {"total_runtime_micros": "26108"},
                34.5943,
            ),
            26.108,
        )

    def test_benchmark_runner_propagates_only_preparation_stage_timings(self) -> None:
        from benchmarks.traditional_analytics import run as benchmark_run

        fields = benchmark_run.preparation_stage_timing_fields(
            {
                "source_parse_micros": "1773",
                "compatibility_to_vortex_import_micros": "25207",
                "prepare_batch_preparation_timing_source": "compatibility_to_vortex_import_micros_excludes_query_total_runtime",
                "prepare_batch_prepared_state_lookup_or_create_micros": "26000",
                "prepare_batch_prepare_route_total_micros": "26108",
                "vortex_write_micros": "21850",
                "vortex_write_strategy": "upstream_vortex_table_flat_leaf_strategy",
                "vortex_write_strategy_fallback_attempted": "false",
                "exclusive_vortex_write_micros": "21850",
                "total_runtime_micros": "26108",
                "evidence_render_micros": "182",
                "vortex_scan_micros": "181",
                "empty": "",
            }
        )

        self.assertEqual(
            fields,
            {
                "source_parse_micros": "1773",
                "compatibility_to_vortex_import_micros": "25207",
                "prepare_batch_preparation_timing_source": "compatibility_to_vortex_import_micros_excludes_query_total_runtime",
                "prepare_batch_prepared_state_lookup_or_create_micros": "26000",
                "prepare_batch_prepare_route_total_micros": "26108",
                "vortex_write_micros": "21850",
                "vortex_write_strategy": "upstream_vortex_table_flat_leaf_strategy",
                "vortex_write_strategy_fallback_attempted": "false",
                "exclusive_vortex_write_micros": "21850",
            },
        )

    def test_full_local_external_lanes_have_required_scenario_handlers(self) -> None:
        required_modules = ("pandas", "polars", "duckdb", "datafusion", "dask")
        missing_modules = [
            module
            for module in required_modules
            if importlib.util.find_spec(module) is None
        ]
        if missing_modules:
            self.skipTest(
                "full-local benchmark dependencies not installed: "
                + ", ".join(missing_modules)
            )

        from benchmarks.traditional_analytics import run as benchmark_run
        from benchmarks.traditional_analytics.benchmark_registry import PROFILES

        profile = PROFILES["full_local"]
        external_lanes = tuple(
            lane for lane in profile.required_lanes if not lane.startswith("shardloom")
        )
        runners, missing = benchmark_run.available_runners(external_lanes)

        self.assertEqual(missing, {})
        for lane in external_lanes:
            missing_scenarios = sorted(
                set(profile.required_scenarios) - set(runners[lane].scenarios)
            )
            self.assertEqual(missing_scenarios, [], lane)

    def test_local_vortex_wrapper_uses_isolated_run_paths(self) -> None:
        module = self._load_module_from_path(
            REPO_ROOT / "examples" / "local-vortex-benchmark" / "run.py",
            "local_vortex_benchmark_wrapper_for_test",
        )
        args = module.parse_args(
            ["--repo-root", str(REPO_ROOT), "--run-id", "unit-test", "--rows", "7"]
        )

        context = module.build_run_context(args)
        self.assertEqual(
            context["data_dir"],
            (REPO_ROOT / "target" / "local-vortex-benchmark" / "unit-test" / "data").resolve(),
        )
        self.assertEqual(
            context["output"],
            (REPO_ROOT / "target" / "local-vortex-benchmark" / "unit-test" / "smoke.json").resolve(),
        )
        self.assertIn("--data-dir", context["command"])
        self.assertIn("--regenerate", context["command"])

        invalid_args = module.parse_args(["--repo-root", str(REPO_ROOT), "--run-id", "../bad"])
        with self.assertRaises(ValueError):
            module.build_run_context(invalid_args)

    def test_benchmark_row_promotes_source_scout_and_scan_contract_fields(self) -> None:
        from benchmarks.traditional_analytics import run as benchmark_run

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)

            def fixture(name: str) -> Path:
                path = root / name
                path.write_text("id\n1\n", encoding="utf-8")
                return path

            paths = benchmark_run.DatasetPaths(
                root=root,
                fact_csv=fixture("fact.csv"),
                dim_csv=fixture("dim.csv"),
                fact_jsonl=fixture("fact.jsonl"),
                dim_jsonl=fixture("dim.jsonl"),
                fact_parquet=fixture("fact.parquet"),
                dim_parquet=fixture("dim.parquet"),
                fact_arrow_ipc=fixture("fact.arrow"),
                dim_arrow_ipc=fixture("dim.arrow"),
                fact_avro=fixture("fact.avro"),
                dim_avro=fixture("dim.avro"),
                fact_orc=fixture("fact.orc"),
                dim_orc=fixture("dim.orc"),
                rows=8,
                dim_rows=2,
            )
            runner = benchmark_run.EngineRunner("shardloom", "test", {})
            first_evidence = {
                "source_read_header_scout_micros": "1000",
                "source_read_byte_acquisition_micros": "2000",
                "source_read_full_body_micros": "3000",
                "source_read_typed_decode_micros": "6000",
                "source_read_row_assembly_micros": "0",
                "source_read_anomaly_quarantine_micros": "0",
                "source_read_columnar_handoff_micros": "1000",
                "source_read_scout_status": "measured",
                "source_read_scout_reuse_status": "reuse_miss",
                "vortex_footer_open_micros": "400",
                "vortex_metadata_verify_micros": "500",
                "vortex_scan_open_micros": "600",
                "vortex_scenario_scan_micros": "700",
                "vortex_scan_bytes_touched": "2048",
                "vortex_scan_segments_touched": "2",
                "vortex_scan_segments_skipped": "0",
                "vortex_scan_columns_touched": "3",
                "vortex_scan_decoded_values": "16",
                "prepare_batch_preparation_timing_source": "compatibility_to_vortex_import_micros_excludes_query_total_runtime",
                "prepare_batch_prepared_state_lookup_or_create_micros": "1200",
                "prepare_batch_prepare_route_total_micros": "2600",
                "prepare_batch_vortex_preparation_spine_schema_version": "shardloom.traditional_analytics.vortex_preparation_spine.v1",
                "prepare_batch_vortex_preparation_spine_status": "full_prepare_wrote_artifacts_with_shared_vortex_context",
                "prepare_batch_vortex_preparation_spine_artifact_count": "2",
                "prepare_batch_vortex_preparation_spine_reused_artifact_count": "0",
                "prepare_batch_vortex_preparation_spine_rewritten_artifact_count": "2",
                "prepare_batch_vortex_preparation_spine_metadata_first_verify_status": "new_artifacts_written_reopened_or_scanned",
                "prepare_batch_vortex_preparation_spine_metadata_first_verify_hit_count": "0",
                "prepare_batch_vortex_preparation_spine_reopen_verify_strategy": "new_artifact_write_then_reopen_scan",
                "prepare_batch_vortex_preparation_spine_full_reopen_verify_count": "2",
                "prepare_batch_vortex_preparation_spine_writer_context_write_count": "2",
                "prepare_batch_vortex_preparation_spine_writer_context_reuse_hit_count": "1",
                "prepare_batch_vortex_preparation_spine_write_coalescing_status": "scheduled_multi_artifact_writes_on_shared_context",
                "prepare_batch_vortex_preparation_spine_shared_writer_context": "true",
                "prepare_batch_vortex_preparation_spine_copy_budget_total_measured_copy_bytes": "8192",
                "prepare_batch_vortex_preparation_spine_buffer_pool_status": "scoped_buffer_pool_disabled_no_hidden_reuse",
                "prepare_batch_vortex_preparation_spine_buffer_reuse_count": "0",
                "persistent_runner_status": benchmark_run.PERSISTENT_RUNNER_STATUS,
                "session_route_used": "false",
                "process_spawn_count": "1",
            }
            second_evidence = {
                "source_read_header_scout_micros": "3000",
                "source_read_byte_acquisition_micros": "4000",
                "source_read_full_body_micros": "5000",
                "source_read_typed_decode_micros": "8000",
                "source_read_row_assembly_micros": "0",
                "source_read_anomaly_quarantine_micros": "0",
                "source_read_columnar_handoff_micros": "2000",
                "source_read_scout_status": "measured",
                "source_read_scout_reuse_status": "reuse_hit",
                "source_state_read_plan": "projected_csv_reader",
                "source_state_projection_pushdown_status": "reader_level_projection",
                "source_state_reader_projection_columns": "id,metric",
                "source_state_reader_projection_column_count": "2",
                "source_read_projected_field_mask": "0x00000005",
                "source_read_filter_field_mask": "0x00000004",
                "source_read_decoded_columns": "id|metric",
                "source_read_skipped_columns": "value|flag",
                "source_read_decoded_column_count": "2",
                "source_read_skipped_column_count": "2",
                "vortex_footer_open_micros": "800",
                "vortex_metadata_verify_micros": "1000",
                "vortex_scan_open_micros": "1200",
                "vortex_scenario_scan_micros": "1400",
                "vortex_scan_bytes_touched": "4096",
                "vortex_scan_segments_touched": "4",
                "vortex_scan_segments_skipped": "1",
                "vortex_scan_columns_touched": "5",
                "vortex_scan_decoded_values": "32",
                "prepare_batch_preparation_timing_source": "compatibility_to_vortex_import_micros_excludes_query_total_runtime",
                "prepare_batch_prepared_state_lookup_or_create_micros": "1600",
                "prepare_batch_prepare_route_total_micros": "3000",
                "prepare_batch_vortex_preparation_spine_schema_version": "shardloom.traditional_analytics.vortex_preparation_spine.v1",
                "prepare_batch_vortex_preparation_spine_status": "full_prepare_wrote_artifacts_with_shared_vortex_context",
                "prepare_batch_vortex_preparation_spine_artifact_count": "2",
                "prepare_batch_vortex_preparation_spine_reused_artifact_count": "0",
                "prepare_batch_vortex_preparation_spine_rewritten_artifact_count": "2",
                "prepare_batch_vortex_preparation_spine_metadata_first_verify_status": "new_artifacts_written_reopened_or_scanned",
                "prepare_batch_vortex_preparation_spine_metadata_first_verify_hit_count": "0",
                "prepare_batch_vortex_preparation_spine_reopen_verify_strategy": "new_artifact_write_then_reopen_scan",
                "prepare_batch_vortex_preparation_spine_full_reopen_verify_count": "2",
                "prepare_batch_vortex_preparation_spine_writer_context_write_count": "2",
                "prepare_batch_vortex_preparation_spine_writer_context_reuse_hit_count": "1",
                "prepare_batch_vortex_preparation_spine_write_coalescing_status": "scheduled_multi_artifact_writes_on_shared_context",
                "prepare_batch_vortex_preparation_spine_shared_writer_context": "true",
                "prepare_batch_vortex_preparation_spine_copy_budget_total_measured_copy_bytes": "8192",
                "prepare_batch_vortex_preparation_spine_buffer_pool_status": "scoped_buffer_pool_disabled_no_hidden_reuse",
                "prepare_batch_vortex_preparation_spine_buffer_reuse_count": "0",
                "persistent_runner_status": benchmark_run.PERSISTENT_RUNNER_STATUS,
                "session_route_used": "false",
                "process_spawn_count": "1",
            }

            result = benchmark_run.successful_result_from_iterations(
                runner,
                paths,
                "selective filter",
                "csv",
                2,
                [{"row_count": 1, "metric_sum": 2.0}, {"row_count": 1, "metric_sum": 2.0}],
                [first_evidence, second_evidence],
                [10.0, 12.0],
                [],
            )

        metrics = result["metrics"]
        missing_stage_fields = [
            field
            for field in benchmark_run.STAGE_TIMING_CONTRACT_FIELDS
            if field not in metrics
        ]
        self.assertEqual(missing_stage_fields, [])
        missing_source_state_fields = [
            field
            for field in benchmark_run.SOURCE_STATE_CONTRACT_FIELDS
            if field not in metrics
        ]
        self.assertEqual(missing_source_state_fields, [])
        self.assertEqual(metrics["source_read_header_scout_millis"], 2.0)
        self.assertFalse(metrics["session_route_used"])
        self.assertEqual(metrics["process_spawn_count"], 1)
        self.assertEqual(metrics["source_read_byte_acquisition_millis"], 3.0)
        self.assertEqual(metrics["source_read_full_body_millis"], 4.0)
        self.assertEqual(metrics["source_read_typed_decode_millis"], 7.0)
        self.assertEqual(metrics["source_read_row_assembly_millis"], 0.0)
        self.assertEqual(metrics["source_read_anomaly_quarantine_millis"], 0.0)
        self.assertEqual(metrics["source_read_columnar_handoff_millis"], 1.5)
        self.assertEqual(metrics["source_read_header_scout_micros"], 3000)
        self.assertEqual(metrics["source_read_scout_reuse_status"], "reuse_hit")
        self.assertEqual(metrics["source_state_read_plan"], "projected_csv_reader")
        self.assertEqual(
            metrics["source_state_projection_pushdown_status"], "reader_level_projection"
        )
        self.assertEqual(metrics["source_state_reader_projection_columns"], "id,metric")
        self.assertEqual(metrics["source_state_reader_projection_column_count"], 2)
        self.assertEqual(metrics["source_state_projected_field_mask"], "0x00000005")
        self.assertEqual(metrics["source_state_filter_field_mask"], "0x00000004")
        self.assertEqual(metrics["source_state_decoded_columns"], "id,metric")
        self.assertEqual(metrics["source_state_skipped_columns"], "value,flag")
        self.assertEqual(metrics["source_state_decoded_column_count"], 2)
        self.assertEqual(metrics["source_state_skipped_column_count"], 2)
        self.assertEqual(metrics["vortex_footer_open_millis"], 0.6)
        self.assertEqual(metrics["vortex_scenario_scan_millis"], 1.05)
        self.assertEqual(metrics["vortex_scan_bytes_touched"], 4096)
        self.assertEqual(metrics["vortex_scan_segments_skipped"], 1)
        self.assertEqual(metrics["vortex_scan_decoded_values"], 32)
        self.assertEqual(
            metrics["prepare_batch_preparation_timing_source"],
            "compatibility_to_vortex_import_micros_excludes_query_total_runtime",
        )
        self.assertEqual(
            metrics["prepare_batch_prepared_state_lookup_or_create_millis"],
            1.4,
        )
        self.assertEqual(metrics["prepare_batch_prepare_route_total_millis"], 2.8)
        self.assertEqual(
            metrics["prepare_batch_vortex_preparation_spine_status"],
            "full_prepare_wrote_artifacts_with_shared_vortex_context",
        )
        self.assertEqual(
            metrics["prepare_batch_vortex_preparation_spine_rewritten_artifact_count"],
            2,
        )
        self.assertTrue(
            metrics["prepare_batch_vortex_preparation_spine_shared_writer_context"]
        )

    def test_benchmark_runner_uses_current_prepare_batch_lifecycle_timing(self) -> None:
        from benchmarks.traditional_analytics import run as benchmark_run

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)

            def fixture(name: str) -> Path:
                path = root / name
                path.write_text("id\n1\n", encoding="utf-8")
                return path

            paths = benchmark_run.DatasetPaths(
                root=root,
                fact_csv=fixture("fact.csv"),
                dim_csv=fixture("dim.csv"),
                fact_jsonl=fixture("fact.jsonl"),
                dim_jsonl=fixture("dim.jsonl"),
                fact_parquet=fixture("fact.parquet"),
                dim_parquet=fixture("dim.parquet"),
                fact_arrow_ipc=fixture("fact.arrow"),
                dim_arrow_ipc=fixture("dim.arrow"),
                fact_avro=fixture("fact.avro"),
                dim_avro=fixture("dim.avro"),
                fact_orc=fixture("fact.orc"),
                dim_orc=fixture("dim.orc"),
                rows=8,
                dim_rows=2,
            )
            runner = benchmark_run.EngineRunner("shardloom-prepare-batch", "test", {})
            full_prepare = {
                "source_read_micros": "1000",
                "prepare_batch_preparation_timing_source": "compatibility_to_vortex_import_micros_excludes_query_total_runtime",
                "prepare_batch_preparation_micros": "120000",
                "prepare_batch_prepared_state_lookup_or_create_micros": "125000",
                "prepare_batch_prepare_route_total_micros": "150000",
                "prepare_batch_source_to_columnar_micros": "40000",
                "prepare_batch_vortex_array_build_micros": "30000",
                "prepare_batch_vortex_write_micros": "20000",
                "prepare_batch_vortex_reopen_verify_micros": "10000",
                "prepare_batch_vortex_preparation_spine_status": "full_prepare_wrote_artifacts_with_shared_vortex_context",
            }
            manifest_hit = {
                "source_read_micros": "3000",
                "prepare_batch_preparation_timing_source": "workspace_manifest_hit_zero_prepare",
                "prepare_batch_preparation_micros": "0",
                "prepare_batch_prepared_state_lookup_or_create_micros": "491",
                "prepare_batch_prepare_route_total_micros": "110165",
                "prepare_batch_source_to_columnar_micros": "0",
                "prepare_batch_vortex_array_build_micros": "0",
                "prepare_batch_vortex_write_micros": "0",
                "prepare_batch_vortex_reopen_verify_micros": "0",
                "prepare_batch_vortex_preparation_spine_status": "manifest_reuse_metadata_verified",
            }

            result = benchmark_run.successful_result_from_iterations(
                runner,
                paths,
                "selective filter",
                "csv",
                2,
                [{"row_count": 1}, {"row_count": 1}],
                [full_prepare, manifest_hit],
                [10.0, 12.0],
                [],
            )

        metrics = result["metrics"]
        self.assertEqual(metrics["source_read_millis"], 2.0)
        self.assertEqual(
            metrics["prepare_batch_preparation_timing_source"],
            "workspace_manifest_hit_zero_prepare",
        )
        self.assertEqual(metrics["prepare_batch_preparation_millis"], 0.0)
        self.assertEqual(
            metrics["prepare_batch_prepared_state_lookup_or_create_millis"], 0.491
        )
        self.assertEqual(metrics["prepare_batch_prepare_route_total_millis"], 110.165)
        self.assertEqual(metrics["prepare_batch_source_to_columnar_millis"], 0.0)
        self.assertEqual(metrics["prepare_batch_vortex_array_build_millis"], 0.0)
        self.assertEqual(metrics["prepare_batch_vortex_write_millis"], 0.0)
        self.assertEqual(metrics["prepare_batch_vortex_reopen_verify_millis"], 0.0)
        self.assertEqual(
            metrics["prepare_batch_vortex_preparation_spine_status"],
            "manifest_reuse_metadata_verified",
        )

    def test_benchmark_harness_regenerate_uses_output_scoped_data_dir(self) -> None:
        from benchmarks.traditional_analytics import run as benchmark_run

        args = benchmark_run.parse_args(
            [
                "--engines",
                "shardloom",
                "--formats",
                "csv",
                "--output",
                "target/unit-smoke.json",
                "--regenerate",
            ]
        )

        self.assertEqual(args.data_dir, Path("target/unit-smoke-data"))
        self.assertFalse(args.data_dir_was_explicit)
        self.assertFalse(args.full_harness_default_selected)

        default_args = benchmark_run.parse_args(["--rows", "10"])
        self.assertEqual(default_args.data_dir, benchmark_run.DEFAULT_DATA_DIR)
        self.assertTrue(default_args.full_harness_default_selected)

        with tempfile.TemporaryDirectory() as tempdir:
            data_dir = Path(tempdir) / "generated"
            with benchmark_run.DatasetRegenerationLock(data_dir):
                with self.assertRaises(RuntimeError):
                    with benchmark_run.DatasetRegenerationLock(data_dir):
                        pass

    def test_benchmark_harness_respects_active_rust_toolchain(self) -> None:
        from benchmarks.traditional_analytics import run as benchmark_run

        previous_rustup = os.environ.pop("RUSTUP_TOOLCHAIN", None)
        previous_benchmark_toolchain = os.environ.pop(
            "SHARDLOOM_BENCHMARK_RUSTUP_TOOLCHAIN",
            None,
        )
        try:
            env = benchmark_run.cargo_subprocess_env()
            self.assertNotIn("RUSTUP_TOOLCHAIN", env)

            os.environ["SHARDLOOM_BENCHMARK_RUSTUP_TOOLCHAIN"] = "stable"
            env = benchmark_run.cargo_subprocess_env()
            self.assertEqual(env["RUSTUP_TOOLCHAIN"], "stable")

            os.environ["RUSTUP_TOOLCHAIN"] = "1.91.1"
            env = benchmark_run.cargo_subprocess_env()
            self.assertEqual(env["RUSTUP_TOOLCHAIN"], "1.91.1")
        finally:
            if previous_rustup is None:
                os.environ.pop("RUSTUP_TOOLCHAIN", None)
            else:
                os.environ["RUSTUP_TOOLCHAIN"] = previous_rustup
            if previous_benchmark_toolchain is None:
                os.environ.pop("SHARDLOOM_BENCHMARK_RUSTUP_TOOLCHAIN", None)
            else:
                os.environ["SHARDLOOM_BENCHMARK_RUSTUP_TOOLCHAIN"] = (
                    previous_benchmark_toolchain
                )

    def test_tiny_smoke_admits_taxonomy_extra_scenarios_and_split_parts(self) -> None:
        from benchmarks.traditional_analytics import run as benchmark_run

        scenarios = benchmark_run.taxonomy_default_scenarios(
            include_extra=True,
            include_stress=False,
        )
        for scenario in scenarios:
            self.assertIsNone(
                benchmark_run.scenario_dataset_profile_block_reason(
                    scenario,
                    "tiny_smoke",
                ),
                scenario,
            )

        if importlib.util.find_spec("pyarrow") is None:
            self.skipTest("pyarrow is required for Arrow-family split fixture generation")
        if importlib.util.find_spec("fastavro") is None:
            self.skipTest("fastavro is required for Avro split fixture generation")

        with tempfile.TemporaryDirectory() as tempdir:
            paths = benchmark_run.ensure_dataset(
                Path(tempdir) / "data",
                rows=32,
                dim_rows=8,
                regenerate=True,
                requested_formats=benchmark_run.FORMAT_ORDER,
                dataset_profile="tiny_smoke",
            )

            header = paths.fact_csv.read_text(encoding="utf-8").splitlines()[0].split(",")
            for column in (
                "event_date",
                "nullable_metric_00",
                "nested_payload",
                "raw_event_time",
                "dirty_numeric",
                "dirty_flag",
            ):
                self.assertIn(column, header)
            self.assertTrue(paths.cdc_delta_csv and paths.cdc_delta_csv.exists())
            self.assertTrue(paths.nested_jsonl and paths.nested_jsonl.exists())
            for data_format in benchmark_run.FORMAT_ORDER:
                self.assertEqual(
                    len(benchmark_run.fact_part_paths(paths, data_format)),
                    8,
                    data_format,
                )

    def test_prepared_vortex_claim_gate_uses_runtime_release_evidence(self) -> None:
        from benchmarks.traditional_analytics import run as benchmark_run

        evidence = {
            field: expected
            for field, expected in benchmark_run.SHARDLOOM_PREPARED_RUNTIME_RELEASE_REQUIRED_EVIDENCE
        }
        result = {
            "engine": "shardloom-prepared-vortex",
            "status": "success",
            "iterations": benchmark_run.MIN_CLAIM_GRADE_ITERATIONS,
            "correctness_digest_stable": True,
            "fallback_attempted": False,
            "metrics": {"query_runtime_millis": 1.0},
            "shardloom_evidence": evidence,
        }

        readiness = benchmark_run.claim_grade_readiness(result)

        self.assertEqual(readiness["claim_gate_status"], "claim_grade")
        self.assertTrue(readiness["claim_grade_requirements_met"])
        self.assertEqual(readiness["claim_grade_missing_evidence"], [])

        evidence["computed_result_sink_replay_verified"] = "false"
        blocked = benchmark_run.claim_grade_readiness(result)
        self.assertEqual(blocked["claim_gate_status"], "not_claim_grade")
        self.assertIn(
            "computed_result_sink_replay_verified!=true",
            blocked["claim_grade_missing_evidence"][0],
        )

    def test_runtime_evidence_claim_gate_blocks_unknown_shardloom_status(self) -> None:
        from benchmarks.traditional_analytics import run as benchmark_run

        self.assertEqual(
            benchmark_run.runtime_evidence_claim_gate_status(True, "success"),
            "claim_grade",
        )
        self.assertEqual(
            benchmark_run.runtime_evidence_claim_gate_status(True, "unsupported"),
            "unsupported",
        )
        self.assertEqual(
            benchmark_run.runtime_evidence_claim_gate_status(True, "skipped_by_gate"),
            "blocked",
        )

    def test_release_readiness_accepts_burned_down_runtime_gap_count(self) -> None:
        module = self._load_script_module(
            "check_release_readiness.py",
            "check_release_readiness_runtime_gap_for_test",
        )
        report = {
            "schema_version": "shardloom.runtime_gap_family_burn_down.v1",
            "status": "passed",
            "global_review_unchecked_count": 37,
            "mapped_gap_count": 37,
            "acceptance_summary": {
                "all_unchecked_global_review_rows_mapped": True,
                "all_families_have_phase_items": True,
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
        mismatched = dict(report, mapped_gap_count=38)
        self.assertIn(
            "runtime gap family burn-down mapped_gap_count does not match global_review_unchecked_count: 38 != 37",
            module.runtime_gap_family_burn_down_blockers(mismatched),
        )

    def test_release_readiness_accepts_precomputed_benchmark_reports(self) -> None:
        module = self._load_script_module(
            "check_release_readiness.py",
            "check_release_readiness_benchmark_reports_for_test",
        )
        manifest_ref = "website/assets/benchmarks/latest/manifest.json"
        completeness = {
            "schema_version": "shardloom.benchmark_artifact_completeness_report.v1",
            "status": "passed",
            "manifest": manifest_ref,
            "benchmark_profile": "full_local",
            "artifact_status": "complete",
            "performance_claim_allowed": False,
            "benchmark_run_performed": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "blockers": [],
        }
        publication = {
            "schema_version": "shardloom.benchmark_publication_claim_gate.v1",
            "status": "passed",
            "manifest": manifest_ref,
            "benchmark_run_performed": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "blockers": [],
        }

        self.assertEqual(
            module.benchmark_completeness_report_blockers(
                completeness,
                manifest_ref=manifest_ref,
            ),
            [],
        )
        self.assertEqual(
            module.benchmark_publication_claim_report_blockers(
                publication,
                manifest_ref=manifest_ref,
            ),
            [],
        )
        blocked = dict(completeness, status="blocked", blockers=["missing lane"])
        self.assertIn(
            "benchmark artifact completeness: missing lane",
            module.benchmark_completeness_report_blockers(
                blocked,
                manifest_ref=manifest_ref,
            ),
        )

    def test_differential_preparation_matrix_preserves_refinement_evidence(self) -> None:
        from benchmarks.traditional_analytics import run as benchmark_run

        evidence = {
            "vortex_differential_preparation_status": "admitted_append_only_delta_overlay",
            "vortex_differential_preparation_update_mode": "append_only",
            "vortex_differential_preparation_delta_row_count": "1",
            "vortex_differential_preparation_delta_manifest_digest": "fnv64:delta",
            "vortex_differential_preparation_overlay_applied": "true",
            "vortex_differential_preparation_delta_artifact_written": "true",
            "vortex_differential_preparation_refinement_status": "admitted_append_only_refinement",
            "vortex_differential_preparation_refinement_mode": "automatic_append_only_delta",
            "vortex_differential_preparation_automatic_detection_status": "append_only_delta_detected",
            "vortex_differential_preparation_blocker_id": "none",
            "vortex_differential_preparation_refinement_manifest_path": "target/.shardloom/base.vortex.differential-refinement.manifest",
            "vortex_differential_preparation_refinement_manifest_digest": "fnv64:manifest",
            "vortex_differential_preparation_refinement_manifest_written": "true",
            "vortex_differential_preparation_refined_prepared_state_id": "vortex-prepared-state-refinement",
            "vortex_differential_preparation_overlay_consumer_family": "count",
            "vortex_differential_preparation_overlay_consumer_status": "admitted_base_manifest_plus_delta_reopen_row_count",
            "vortex_differential_preparation_overlay_consumer_correctness_digest": "fnv64:consumer",
        }
        metrics = benchmark_run.vortex_differential_preparation_metadata(
            "shardloom",
            "success",
            metrics={},
            evidence=evidence,
        )
        rows = benchmark_run.vortex_differential_preparation_matrix(
            [
                {
                    "scenario_name": "append_only_refinement",
                    "engine": "shardloom",
                    "status": "success",
                    "selected_execution_mode": "compatibility_import",
                    "metrics": metrics,
                }
            ]
        )

        self.assertEqual(
            rows[0]["vortex_differential_preparation_refinement_status"],
            "admitted_append_only_refinement",
        )
        self.assertEqual(
            rows[0]["vortex_differential_preparation_refinement_mode"],
            "automatic_append_only_delta",
        )
        self.assertEqual(
            rows[0]["vortex_differential_preparation_refinement_manifest_digest"],
            "fnv64:manifest",
        )
        self.assertTrue(
            rows[0]["vortex_differential_preparation_refinement_manifest_written"]
        )
        self.assertEqual(
            rows[0]["vortex_differential_preparation_overlay_consumer_status"],
            "admitted_base_manifest_plus_delta_reopen_row_count",
        )

        rendered = benchmark_run.render_vortex_differential_preparation_matrix(
            {"vortex_differential_preparation_matrix": rows}
        )
        self.assertIn("admitted_append_only_refinement", rendered)
        self.assertIn("fnv64:manifest", rendered)
        self.assertIn("admitted_base_manifest_plus_delta_reopen_row_count", rendered)

    def test_cold_lane_accepts_shared_batch_process_timing(self) -> None:
        from benchmarks.traditional_analytics import run as benchmark_run

        cold_lane = benchmark_run.cold_lane_attribution_metadata(
            {
                "engine": "shardloom-vortex",
                "status": "success",
                "selected_execution_mode": "prepared_vortex",
                "preparation_included_in_timing": False,
                "metrics": {
                    "persistent_runner_status": benchmark_run.BATCH_RUNNER_STATUS,
                    "vortex_scan_millis": 0.4,
                    "query_runtime_millis": 1.0,
                    "operator_compute_millis": 0.5,
                    "evidence_render_millis": 0.1,
                    "cli_process_wall_millis": 2.0,
                    "session_route_used": True,
                    "process_spawn_count": 1,
                    "batch_cli_process_wall_millis": 2.0,
                    "batch_process_wall_shared": True,
                },
            }
        )

        self.assertEqual(cold_lane["cold_lane_timing_split_status"], "complete")
        self.assertTrue(cold_lane["cold_lane_process_harness_timing_present"])
        self.assertNotIn(
            "python_harness_overhead_millis",
            cold_lane["cold_lane_missing_stage_fields"],
        )

    def test_benchmark_promoter_marks_broad_formats_required_for_full_local(self) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py", "promote_benchmark_formats_for_test"
        )
        rows = [
            {"storage_format": data_format}
            for data_format in ("csv", "jsonl", "parquet", "arrow-ipc", "avro", "orc")
        ]

        table = module.format_coverage_table(
            {
                "format_order": [
                    "csv",
                    "jsonl",
                    "parquet",
                    "arrow-ipc",
                    "avro",
                    "orc",
                ]
            },
            rows,
            "full_local",
        )
        by_format = {row[0]: row for row in table["rows"]}

        for data_format in ("csv", "parquet", "jsonl", "arrow-ipc", "avro", "orc"):
            self.assertEqual(by_format[data_format][1], "required")
            self.assertEqual(by_format[data_format][2], "available")

    def test_benchmark_promoter_derives_formats_from_merged_rows_for_targeted_refresh(
        self,
    ) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py",
            "promote_benchmark_targeted_format_merge_for_test",
        )
        rows = [
            {"storage_format": data_format}
            for data_format in ("csv", "jsonl", "parquet", "arrow-ipc", "avro", "orc")
        ]

        self.assertEqual(
            module.benchmark_format_order(
                {"format_order": ["jsonl", "avro"]},
                rows,
                "full_local",
            ),
            ["csv", "jsonl", "parquet", "arrow-ipc", "avro", "orc"],
        )

    def test_benchmark_publication_claim_gate_blocks_stale_git_and_age(self) -> None:
        module = self._load_script_module(
            "check_benchmark_publication_claim_gate.py",
            "benchmark_publication_claim_gate_freshness_for_test",
        )

        blockers: list[str] = []
        freshness = module.validate_freshness(
            {
                "generated_at_utc": "2026-05-01T00:00:00+00:00",
                "benchmark_git_sha": "old-sha",
                "shardloom_git_sha": "old-sha",
            },
            REPO_ROOT,
            blockers,
            now=datetime(2026, 5, 31, tzinfo=timezone.utc),
            max_age_days=14,
            require_current_git=True,
            allow_dirty_worktree=False,
            current_git_sha="current-sha",
            worktree_status=" M shardloom-vortex/src/vortex_ingest.rs",
        )

        self.assertEqual(freshness["current_git_sha"], "current-sha")
        self.assertTrue(freshness["worktree_dirty"])
        self.assertTrue(
            any("age exceeds freshness limit" in blocker for blocker in blockers)
        )
        self.assertTrue(
            any("benchmark_git_sha='old-sha' does not match current HEAD" in blocker for blocker in blockers)
        )
        self.assertIn(
            "benchmark artifact cannot be current while the worktree is dirty",
            blockers,
        )
        self.assertEqual(freshness["tracked_dirty_status_count"], 1)
        self.assertEqual(freshness["untracked_status_count"], 0)

    def test_benchmark_publication_claim_gate_ignores_untracked_only_status(self) -> None:
        module = self._load_script_module(
            "check_benchmark_publication_claim_gate.py",
            "benchmark_publication_claim_gate_untracked_for_test",
        )

        blockers: list[str] = []
        freshness = module.validate_freshness(
            {
                "generated_at_utc": "2026-05-31T00:00:00+00:00",
                "benchmark_git_sha": "current-sha",
                "shardloom_git_sha": "current-sha",
            },
            REPO_ROOT,
            blockers,
            now=datetime(2026, 5, 31, tzinfo=timezone.utc),
            max_age_days=14,
            require_current_git=True,
            allow_dirty_worktree=False,
            current_git_sha="current-sha",
            worktree_status="?? local-scratch.json\n?? website/assets/benchmarks/latest/chunk-copy.json",
        )

        self.assertEqual(blockers, [])
        self.assertFalse(freshness["worktree_dirty"])
        self.assertTrue(freshness["untracked_only"])
        self.assertEqual(freshness["tracked_dirty_status_count"], 0)
        self.assertEqual(freshness["untracked_status_count"], 2)

    def test_benchmark_publication_claim_gate_accepts_static_publication_descendant(self) -> None:
        module = self._load_script_module(
            "check_benchmark_publication_claim_gate.py",
            "benchmark_publication_claim_gate_publication_descendant_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            repo = Path(tempdir)
            subprocess.run(["git", "init"], cwd=repo, check=True, capture_output=True)
            subprocess.run(
                ["git", "config", "user.email", "test@example.com"],
                cwd=repo,
                check=True,
            )
            subprocess.run(
                ["git", "config", "user.name", "ShardLoom Test"],
                cwd=repo,
                check=True,
            )
            (repo / "shardloom-vortex" / "src").mkdir(parents=True)
            (repo / "shardloom-vortex" / "src" / "lib.rs").write_text(
                "pub fn source() {}\n",
                encoding="utf-8",
            )
            subprocess.run(["git", "add", "."], cwd=repo, check=True)
            subprocess.run(
                ["git", "commit", "-m", "source revision"],
                cwd=repo,
                check=True,
                capture_output=True,
            )
            source_sha = subprocess.check_output(
                ["git", "rev-parse", "HEAD"],
                cwd=repo,
                text=True,
            ).strip()

            (repo / "website" / "assets" / "benchmarks" / "latest").mkdir(
                parents=True
            )
            (repo / "website" / "assets" / "benchmarks" / "latest" / "manifest.json").write_text(
                "{}\n",
                encoding="utf-8",
            )
            (repo / "website" / "benchmarks").mkdir(parents=True)
            (repo / "website" / "benchmarks" / "index.html").write_text(
                "<html><body>benchmark publication</body></html>\n",
                encoding="utf-8",
            )
            (repo / "website" / "index.html").write_text(
                "<html><body>home page generated with current benchmark data</body></html>\n",
                encoding="utf-8",
            )
            (repo / "docs" / "architecture").mkdir(parents=True)
            (repo / "docs" / "architecture" / "phased-execution-plan.md").write_text(
                "# Phase plan\n\n- [x] release bookkeeping closed after publication evidence.\n",
                encoding="utf-8",
            )
            (repo / "docs" / "release").mkdir(parents=True)
            (repo / "docs" / "release" / "maintainer-publication-handoff.md").write_text(
                "# Handoff\n\nStrict benchmark publication evidence passed.\n",
                encoding="utf-8",
            )
            subprocess.run(["git", "add", "."], cwd=repo, check=True)
            subprocess.run(
                ["git", "commit", "-m", "publish static benchmark bundle"],
                cwd=repo,
                check=True,
                capture_output=True,
            )
            publication_sha = subprocess.check_output(
                ["git", "rev-parse", "HEAD"],
                cwd=repo,
                text=True,
            ).strip()

            blockers: list[str] = []
            freshness = module.validate_freshness(
                {
                    "generated_at_utc": "2026-05-31T00:00:00+00:00",
                    "benchmark_git_sha": source_sha,
                    "shardloom_git_sha": source_sha,
                },
                repo,
                blockers,
                now=datetime(2026, 5, 31, tzinfo=timezone.utc),
                max_age_days=14,
                require_current_git=True,
                allow_dirty_worktree=False,
                current_git_sha=publication_sha,
                worktree_status="",
            )

        self.assertEqual(blockers, [])
        self.assertEqual(
            freshness["git_currentness_status"],
            "static_publication_descendant",
        )
        self.assertEqual(
            freshness["static_publication_delta_paths"],
            [
                "docs/architecture/phased-execution-plan.md",
                "docs/release/maintainer-publication-handoff.md",
                "website/assets/benchmarks/latest/manifest.json",
                "website/benchmarks/index.html",
                "website/index.html",
            ],
        )

    def test_benchmark_publication_claim_gate_accepts_control_plane_descendant(self) -> None:
        module = self._load_script_module(
            "check_benchmark_publication_claim_gate.py",
            "benchmark_publication_claim_gate_control_plane_descendant_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            repo = Path(tempdir)
            subprocess.run(["git", "init"], cwd=repo, check=True, capture_output=True)
            subprocess.run(
                ["git", "config", "user.email", "test@example.com"],
                cwd=repo,
                check=True,
            )
            subprocess.run(
                ["git", "config", "user.name", "ShardLoom Test"],
                cwd=repo,
                check=True,
            )
            (repo / "README.md").write_text("# Source revision\n", encoding="utf-8")
            subprocess.run(["git", "add", "."], cwd=repo, check=True)
            subprocess.run(
                ["git", "commit", "-m", "source revision"],
                cwd=repo,
                check=True,
                capture_output=True,
            )
            source_sha = subprocess.check_output(
                ["git", "rev-parse", "HEAD"],
                cwd=repo,
                text=True,
            ).strip()

            (repo / "scripts").mkdir(parents=True)
            (repo / "scripts" / "promote_benchmark_artifact.py").write_text(
                "# promoter-only publication control-plane update\n",
                encoding="utf-8",
            )
            (
                repo / "scripts" / "check_benchmark_publication_claim_gate.py"
            ).write_text(
                "# freshness validator publication control-plane update\n",
                encoding="utf-8",
            )
            (repo / "python" / "tests").mkdir(parents=True)
            (repo / "python" / "tests" / "test_release_scripts.py").write_text(
                "# release-script control-plane test update\n",
                encoding="utf-8",
            )
            subprocess.run(["git", "add", "."], cwd=repo, check=True)
            subprocess.run(
                ["git", "commit", "-m", "publication control-plane update"],
                cwd=repo,
                check=True,
                capture_output=True,
            )
            current_sha = subprocess.check_output(
                ["git", "rev-parse", "HEAD"],
                cwd=repo,
                text=True,
            ).strip()

            blockers: list[str] = []
            freshness = module.validate_freshness(
                {
                    "generated_at_utc": "2026-05-31T00:00:00+00:00",
                    "benchmark_git_sha": source_sha,
                    "shardloom_git_sha": source_sha,
                },
                repo,
                blockers,
                now=datetime(2026, 5, 31, tzinfo=timezone.utc),
                max_age_days=14,
                require_current_git=True,
                allow_dirty_worktree=False,
                current_git_sha=current_sha,
                worktree_status="",
            )

        self.assertEqual(blockers, [])
        self.assertEqual(
            freshness["git_currentness_status"],
            "static_publication_descendant",
        )
        self.assertEqual(freshness["static_publication_delta_paths"], [])
        self.assertEqual(
            freshness["benchmark_publication_control_plane_delta_paths"],
            [
                "python/tests/test_release_scripts.py",
                "scripts/check_benchmark_publication_claim_gate.py",
                "scripts/promote_benchmark_artifact.py",
            ],
        )

    def test_benchmark_publication_claim_gate_blocks_source_changes_after_artifact_source(self) -> None:
        module = self._load_script_module(
            "check_benchmark_publication_claim_gate.py",
            "benchmark_publication_claim_gate_source_drift_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            repo = Path(tempdir)
            subprocess.run(["git", "init"], cwd=repo, check=True, capture_output=True)
            subprocess.run(
                ["git", "config", "user.email", "test@example.com"],
                cwd=repo,
                check=True,
            )
            subprocess.run(
                ["git", "config", "user.name", "ShardLoom Test"],
                cwd=repo,
                check=True,
            )
            (repo / "shardloom-vortex" / "src").mkdir(parents=True)
            source_file = repo / "shardloom-vortex" / "src" / "lib.rs"
            source_file.write_text("pub fn source() {}\n", encoding="utf-8")
            subprocess.run(["git", "add", "."], cwd=repo, check=True)
            subprocess.run(
                ["git", "commit", "-m", "source revision"],
                cwd=repo,
                check=True,
                capture_output=True,
            )
            source_sha = subprocess.check_output(
                ["git", "rev-parse", "HEAD"],
                cwd=repo,
                text=True,
            ).strip()

            source_file.write_text("pub fn changed_after_benchmark() {}\n", encoding="utf-8")
            subprocess.run(["git", "add", "."], cwd=repo, check=True)
            subprocess.run(
                ["git", "commit", "-m", "source changed after benchmark"],
                cwd=repo,
                check=True,
                capture_output=True,
            )
            current_sha = subprocess.check_output(
                ["git", "rev-parse", "HEAD"],
                cwd=repo,
                text=True,
            ).strip()

            blockers: list[str] = []
            freshness = module.validate_freshness(
                {
                    "generated_at_utc": "2026-05-31T00:00:00+00:00",
                    "benchmark_git_sha": source_sha,
                    "shardloom_git_sha": source_sha,
                },
                repo,
                blockers,
                now=datetime(2026, 5, 31, tzinfo=timezone.utc),
                max_age_days=14,
                require_current_git=True,
                allow_dirty_worktree=False,
                current_git_sha=current_sha,
                worktree_status="",
            )

        self.assertEqual(
            freshness["git_currentness_status"],
            "blocked_mismatched_source_revision",
        )
        self.assertTrue(
            any("non-publication source files changed after benchmark source revision" in blocker for blocker in blockers),
            blockers,
        )
        self.assertEqual(
            freshness["static_publication_nonpublic_delta_paths"],
            ["shardloom-vortex/src/lib.rs"],
        )

    def test_benchmark_publication_claim_gate_blocks_dirty_lane_versions(self) -> None:
        module = self._load_script_module(
            "check_benchmark_publication_claim_gate.py",
            "benchmark_publication_claim_gate_lane_versions_for_test",
        )

        blockers: list[str] = []
        report = module.validate_shardloom_lane_version_provenance(
            {
                "benchmark_git_sha": "9835ae15633587307d7ab2e710a44bf2970ea883",
                "shardloom_git_sha": "9835ae15633587307d7ab2e710a44bf2970ea883",
                "lane_versions": {
                    "pandas": "2.2.3",
                    "shardloom": "workspace-local-release-d94d30b0-dirty",
                    "shardloom-vortex": "workspace-local-release-9835ae1",
                },
            },
            blockers,
            enforce_current_artifact=True,
        )

        self.assertEqual(report["checked_shardloom_lane_count"], 2)
        self.assertEqual(report["dirty_shardloom_lanes"], ["shardloom"])
        self.assertEqual(report["sha_mismatched_shardloom_lanes"], ["shardloom"])
        self.assertTrue(
            any("lane_versions['shardloom'] is dirty" in blocker for blocker in blockers)
        )
        self.assertTrue(
            any(
                "lane_versions['shardloom'] sha 'd94d30b0' does not match" in blocker
                for blocker in blockers
            )
        )

    def test_benchmark_publication_claim_gate_requires_claim_grade_capillary_rows(self) -> None:
        module = self._load_script_module(
            "check_benchmark_publication_claim_gate.py",
            "benchmark_publication_claim_gate_rows_for_test",
        )
        lanes = list(module.expected_lanes_for_profile("full_local"))
        manifest = {
            "benchmark_profile": "full_local",
            "expected_lanes": lanes,
            "available_lanes": lanes,
        }
        payload = {
            "published_benchmark_artifact": {"format_order": ["csv", "parquet"]},
            "published_benchmark_rows": [
                {
                    "engine": "shardloom",
                    "storage_format": "csv",
                    "status": "blocked",
                    "claim_gate_status": "not_claim_grade",
                    "claim_grade_requirements_met": False,
                    "timing_surface": "publication_proof",
                    "actual_evidence_tier": "publication_full",
                    "fallback_attempted": False,
                    "external_engine_invoked": False,
                }
            ],
        }

        blockers: list[str] = []
        report = module.validate_profile_and_rows(manifest, payload, blockers)

        self.assertEqual(report["shardloom_row_count"], 1)
        self.assertEqual(report["missing_capillary_activation_row_count"], 1)
        self.assertTrue(
            any("missing public-format coverage" in blocker for blocker in blockers)
        )
        self.assertTrue(any("non-success status blocked" in blocker for blocker in blockers))
        self.assertTrue(any("claim_gate_status=not_claim_grade" in blocker for blocker in blockers))
        self.assertTrue(any("missing ShardLoom publication engines" in blocker for blocker in blockers))

    def test_benchmark_publication_claim_gate_rejects_schema_only_capillary_rows(
        self,
    ) -> None:
        module = self._load_script_module(
            "check_benchmark_publication_claim_gate.py",
            "benchmark_publication_claim_gate_schema_only_capillary_for_test",
        )
        lanes = list(module.expected_lanes_for_profile("full_local"))
        manifest = {
            "benchmark_profile": "full_local",
            "expected_lanes": lanes,
            "available_lanes": lanes,
        }
        row = {
            "engine": "shardloom",
            "storage_format": "csv",
            "status": "success",
            "claim_gate_status": "claim_grade",
            "claim_grade_requirements_met": True,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "selected_execution_mode": "prepared_vortex",
            "source_state_id": "source-state://claim-grade-row",
            "source_state_digest": "fnv64:source",
            "prepared_state_id": "prepared-state://claim-grade-row",
            "prepared_state_digest": "fnv64:prepared",
            "data_decoded": False,
            "runtime_execution_certificate_id": "execution.claim-grade-row",
            "runtime_execution_certificate_status": "certified",
            "iterations": 3,
            "reproducibility_min_iterations": 3,
            "reproducibility_iterations_met": True,
            "correctness_digest": "fnv64:correct",
            "correctness_digest_stable": True,
            "query_runtime_millis": 1.0,
            "cold_lane_timing_split_status": "complete",
            "computed_result_sink_replay_verified": True,
            "vortex_capillary_preparation_activation_policy": "not_reported",
            "vortex_capillary_preparation_activation_result": "not_reported",
            "vortex_capillary_preparation_activation_reason": "not_reported",
            "vortex_capillary_preparation_activation_observed_bytes": "not_reported",
            "vortex_capillary_preparation_activation_observed_rows": "not_reported",
            "vortex_capillary_preparation_activation_observed_columns": "not_reported",
            "vortex_capillary_preparation_activation_observed_split_count": "not_reported",
        }
        payload = {
            "published_benchmark_artifact": {
                "format_order": ["csv", "parquet", "jsonl", "arrow-ipc", "avro", "orc"]
            },
            "published_benchmark_rows": [row],
        }

        blockers: list[str] = []
        report = module.validate_profile_and_rows(manifest, payload, blockers)

        self.assertEqual(report["missing_capillary_activation_row_count"], 1)
        self.assertTrue(
            any("missing capillary activation evidence fields" in blocker for blocker in blockers)
        )

    def test_benchmark_publication_claim_gate_rejects_reuse_without_evidence(
        self,
    ) -> None:
        module = self._load_script_module(
            "check_benchmark_publication_claim_gate.py",
            "benchmark_publication_claim_gate_reuse_evidence_for_test",
        )
        lanes = list(module.expected_lanes_for_profile("full_local"))
        row = {
            "engine": "shardloom-prepare-batch",
            "storage_format": "csv",
            "status": "success",
            "claim_gate_status": "claim_grade",
            "claim_grade_requirements_met": True,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "selected_execution_mode": "shardloom-prepare-batch",
            "source_state_id": "source-state://claim-grade-row",
            "source_state_digest": "fnv64:source",
            "prepared_state_id": "prepared-state://claim-grade-row",
            "prepared_state_digest": "fnv64:prepared",
            "data_decoded": False,
            "runtime_execution_certificate_id": "execution.claim-grade-row",
            "runtime_execution_certificate_status": "certified",
            "iterations": 3,
            "reproducibility_min_iterations": 3,
            "reproducibility_iterations_met": True,
            "correctness_digest": "fnv64:correct",
            "correctness_digest_stable": True,
            "query_runtime_millis": 1.0,
            "cold_lane_timing_split_status": "complete",
            "computed_result_sink_replay_verified": True,
            "vortex_capillary_preparation_activation_policy": (
                "dynamic_size_complexity_gate.v1"
            ),
            "vortex_capillary_preparation_activation_result": "activated",
            "vortex_capillary_preparation_activation_reason": (
                "claim_evidence_requested"
            ),
            "vortex_capillary_preparation_activation_observed_bytes": "67108864",
            "vortex_capillary_preparation_activation_observed_rows": "1000000",
            "vortex_capillary_preparation_activation_observed_columns": "8",
            "vortex_capillary_preparation_activation_observed_split_count": "8",
            **self._shardloom_benchmark_route_fields("shardloom-prepare-batch"),
        }
        row["prepared_state_reuse_manifest_digest"] = (
            "not_applicable_no_reuse_manifest_for_route"
        )
        payload = {
            "published_benchmark_artifact": {
                "format_order": ["csv", "parquet", "jsonl", "arrow-ipc", "avro", "orc"]
            },
            "published_benchmark_rows": [row],
        }
        manifest = {
            "benchmark_profile": "full_local",
            "expected_lanes": lanes,
            "available_lanes": lanes,
        }

        blockers: list[str] = []
        report = module.validate_profile_and_rows(manifest, payload, blockers)

        self.assertEqual(report["missing_prepared_state_reuse_evidence_row_count"], 1)
        self.assertTrue(
            any("missing prepared-state reuse evidence fields" in blocker for blocker in blockers)
        )

    def test_benchmark_publication_claim_gate_accepts_current_claim_grade_rows(self) -> None:
        module = self._load_script_module(
            "check_benchmark_publication_claim_gate.py",
            "benchmark_publication_claim_gate_pass_for_test",
        )
        lanes = list(module.expected_lanes_for_profile("full_local"))
        capillary_fields = {
            "vortex_capillary_preparation_activation_policy": "dynamic_size_complexity_gate.v1",
            "vortex_capillary_preparation_activation_result": "activated",
            "vortex_capillary_preparation_activation_reason": "claim_evidence_requested",
            "vortex_capillary_preparation_activation_observed_bytes": "67108864",
            "vortex_capillary_preparation_activation_observed_rows": "1000000",
            "vortex_capillary_preparation_activation_observed_columns": "8",
            "vortex_capillary_preparation_activation_observed_split_count": "8",
        }
        runtime_fields = {
            "selected_execution_mode": "prepared_vortex",
            "source_state_id": "source-state://claim-grade-row",
            "source_state_digest": "fnv64:source",
            "prepared_state_id": "prepared-state://claim-grade-row",
            "prepared_state_digest": "fnv64:prepared",
            "data_decoded": False,
            "runtime_execution_certificate_id": "execution.claim-grade-row",
            "runtime_execution_certificate_status": "certified",
            "iterations": 3,
            "reproducibility_min_iterations": 3,
            "reproducibility_iterations_met": True,
            "correctness_digest": "fnv64:correct",
            "correctness_digest_stable": True,
            "query_runtime_millis": 1.0,
            "cold_lane_timing_split_status": "complete",
            "computed_result_sink_replay_verified": True,
        }
        rows = []
        for engine in module.REQUIRED_SHARDLOOM_PUBLICATION_ENGINES:
            for storage_format in module.REQUIRED_PUBLICATION_FORMATS:
                rows.append(
                    {
                        "engine": engine,
                        "storage_format": storage_format,
                        "status": "success",
                        "claim_gate_status": "claim_grade",
                        "claim_grade_requirements_met": True,
                        "fallback_attempted": False,
                        "external_engine_invoked": False,
                        **self._shardloom_benchmark_route_fields(engine),
                        **capillary_fields,
                        **runtime_fields,
                    }
                )
        hot_runtime_row = {
            "engine": "shardloom-prepared-vortex",
            "storage_format": "csv",
            "status": "success",
            "claim_gate_status": "not_claim_grade",
            "claim_grade_requirements_met": False,
            "claim_grade_missing_evidence": ["metadata_sink_not_publication_proof"],
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "selected_execution_mode": "prepared_vortex",
            **self._shardloom_benchmark_route_fields("shardloom-prepared-vortex"),
            **capillary_fields,
            **runtime_fields,
        }
        hot_runtime_row.update(
            {
                "route_timing_surface_schema_version": (
                    "shardloom.route_timing_surface.v1"
                ),
                "timing_surface": "hot_runtime",
                "timing_surface_label": "Hot runtime",
                "timing_surface_evidence_tier": "metadata_sink",
                "timing_surface_default_for_route": True,
                "requested_evidence_tier": "metadata_sink",
                "actual_evidence_tier": "metadata_sink",
                "selected_evidence_tier": "metadata_sink",
                "sink_tier": "metadata_sink",
                "includes_output": False,
                "includes_evidence": False,
                "output_timing_included_in_total": False,
                "evidence_timing_included_in_total": False,
                "evidence_render_included_in_route_total": False,
                "evidence_tier_result_sink_replay_required": False,
                "sink_timing_included_in_route_total": False,
                "sink_timing_inclusion_reason": (
                    "metadata_sink_has_no_replay_write_timing"
                ),
                "result_sink_replay_skip_reason": (
                    "skipped_metadata_sink_tier_digest_count_path_proof_without_replay"
                ),
                "human_evidence_render_skip_reason": (
                    "skipped_hot_runtime_metadata_sink"
                ),
                "route_total_formula": "timing_surface=hot_runtime; query_runtime_millis",
                "route_timing_included_stage_ids": "prepared_query",
                "route_timing_excluded_stage_ids": "result_sink_write,evidence_render",
                "route_timing_included_stage_total_ms": 0.34,
                "route_timing_total_delta_ms": 0.0,
                "total_route_ms": 0.34,
                "query_runtime_millis": 0.34,
            }
        )
        rows.append(hot_runtime_row)
        for engine in lanes:
            if engine.startswith("shardloom"):
                continue
            rows.append(
                {
                    "engine": engine,
                    "storage_format": "csv",
                    "status": "success",
                    "claim_gate_status": "external_baseline_only",
                    "claim_grade_requirements_met": False,
                    "external_baseline_only": True,
                    "fallback_attempted": False,
                    "external_engine_invoked": False,
                    **self._external_benchmark_route_fields(engine),
                }
            )
        public_front_door_rows = self._public_front_door_benchmark_rows(module)
        public_front_door_ids = [
            str(row["front_door_id"]) for row in public_front_door_rows
        ]
        manifest = {
            "benchmark_profile": "full_local",
            "expected_lanes": lanes,
            "available_lanes": lanes,
            "public_front_door_benchmark_schema_version": (
                module.PUBLIC_FRONT_DOOR_BENCHMARK_SCHEMA_VERSION
            ),
            "public_front_door_benchmark_row_count": len(public_front_door_rows),
            "public_front_door_benchmark_row_ids": public_front_door_ids,
        }
        payload = {
            "published_benchmark_artifact": {
                "format_order": ["csv", "parquet", "jsonl", "arrow-ipc", "avro", "orc"]
            },
            "published_benchmark_rows": rows,
            "public_front_door_benchmark_schema_version": (
                module.PUBLIC_FRONT_DOOR_BENCHMARK_SCHEMA_VERSION
            ),
            "public_front_door_benchmark_rows": public_front_door_rows,
            "public_front_door_benchmark_row_count": len(public_front_door_rows),
            "public_front_door_benchmark_row_ids": public_front_door_ids,
        }

        blockers: list[str] = []
        report = module.validate_profile_and_rows(manifest, payload, blockers)

        self.assertFalse(blockers)
        self.assertEqual(
            report["shardloom_row_count"],
            len(module.REQUIRED_SHARDLOOM_PUBLICATION_ENGINES)
            * len(module.REQUIRED_PUBLICATION_FORMATS)
            + 1,
        )
        self.assertEqual(report["missing_capillary_activation_row_count"], 0)
        self.assertEqual(report["missing_shardloom_engine_format_cell_count"], 0)
        self.assertEqual(report["shardloom_runtime_validation_counts"], {"passed": 24})
        self.assertEqual(
            report["shardloom_claim_gate_counts"],
            {"claim_grade": 24, "not_claim_grade": 1},
        )
        self.assertEqual(report["missing_independent_claim_proof_row_count"], 0)
        self.assertEqual(report["public_front_door_benchmark_rows"]["row_count"], 2)
        self.assertEqual(
            report["public_front_door_benchmark_rows"]["invalid_example_count"],
            0,
        )

    def test_benchmark_publication_claim_gate_rejects_false_encoded_native_operator_claim(
        self,
    ) -> None:
        module = self._load_script_module(
            "check_benchmark_publication_claim_gate.py",
            "benchmark_publication_claim_gate_operator_mode_for_test",
        )
        lanes = list(module.expected_lanes_for_profile("full_local"))
        capillary_fields = {
            "vortex_capillary_preparation_activation_policy": "dynamic_size_complexity_gate.v1",
            "vortex_capillary_preparation_activation_result": "activated",
            "vortex_capillary_preparation_activation_reason": "claim_evidence_requested",
            "vortex_capillary_preparation_activation_observed_bytes": "67108864",
            "vortex_capillary_preparation_activation_observed_rows": "1000000",
            "vortex_capillary_preparation_activation_observed_columns": "8",
            "vortex_capillary_preparation_activation_observed_split_count": "8",
        }
        row = {
            "engine": "shardloom-prepared-vortex",
            "storage_format": "csv",
            "status": "success",
            "claim_gate_status": "claim_grade",
            "claim_grade_requirements_met": True,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "selected_execution_mode": "prepared_vortex",
            "source_state_id": "source-state://operator-claim-row",
            "source_state_digest": "fnv64:source",
            "prepared_state_id": "prepared-state://operator-claim-row",
            "prepared_state_digest": "fnv64:prepared",
            "data_decoded": False,
            "runtime_execution_certificate_id": "execution.operator-claim-row",
            "runtime_execution_certificate_status": "certified",
            "iterations": 3,
            "reproducibility_min_iterations": 3,
            "reproducibility_iterations_met": True,
            "correctness_digest": "fnv64:correct",
            "correctness_digest_stable": True,
            "query_runtime_millis": 1.0,
            "cold_lane_timing_split_status": "complete",
            "computed_result_sink_replay_verified": True,
            **self._shardloom_benchmark_route_fields("shardloom-prepared-vortex"),
            **capillary_fields,
        }
        row["operator_encoded_native_claim_allowed"] = True
        manifest = {
            "benchmark_profile": "full_local",
            "expected_lanes": lanes,
            "available_lanes": lanes,
        }
        payload = {
            "published_benchmark_artifact": {
                "format_order": ["csv", "parquet", "jsonl", "arrow-ipc", "avro", "orc"]
            },
            "published_benchmark_rows": [row],
        }

        blockers: list[str] = []
        report = module.validate_profile_and_rows(manifest, payload, blockers)

        self.assertEqual(report["missing_independent_claim_proof_row_count"], 1)
        self.assertTrue(
            any("invalid operator mode/encoded-native claim fields" in blocker for blocker in blockers)
        )
        self.assertTrue(
            any(
                "non_encoded_operator_row_allows_encoded_native_claim" in example
                for example in report.get("blockers", [])
            )
            or any(
                "non_encoded_operator_row_allows_encoded_native_claim" in blocker
                for blocker in blockers
            )
        )

    def test_benchmark_publication_claim_gate_rejects_invalid_operator_route_relation(
        self,
    ) -> None:
        module = self._load_script_module(
            "check_benchmark_publication_claim_gate.py",
            "benchmark_publication_claim_gate_operator_relation_for_test",
        )
        row = {
            "route_timing_stage_inclusion_classes": self._packed_route_stage_map(
                "diagnostic_only"
            ),
            "operator_compute_route_relation_schema_version": (
                "shardloom.operator_compute_route_relation.v1"
            ),
            "operator_compute_route_relation_status": (
                "diagnostic_only_exceeds_route_total"
            ),
            "operator_compute_included_in_route_total": True,
            "operator_compute_route_stage_inclusion_class": "diagnostic_only",
            "operator_compute_route_total_field": "route_timing_included_stage_total_ms",
            "operator_compute_route_total_ms": 0.12,
            "operator_compute_route_total_delta_ms": 1.28,
            "operator_compute_route_relation_claim_boundary": (
                "operator_compute_millis is interpreted through the selected timing surface"
            ),
        }

        status, issues = module.operator_compute_route_relation_issues(
            row,
            row_index=7,
            engine="shardloom-prepared-vortex",
        )

        self.assertEqual(status, "diagnostic_only_exceeds_route_total")
        self.assertTrue(
            any(
                "diagnostic-only operator relation was marked included" in issue
                for issue in issues
            )
        )
        self.assertTrue(
            any(
                "operator relation included=true but stage class='diagnostic_only'"
                in issue
                for issue in issues
            )
        )

    def test_benchmark_publish_doctor_accepts_current_static_artifact(self) -> None:
        module = self._load_script_module(
            "check_benchmark_publish_doctor.py",
            "benchmark_publish_doctor_pass_for_test",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            pre_5j_report = Path(tempdir) / "pre-5j-dependency-freshness-gate.json"
            self._write_passing_pre_5j_dependency_report(pre_5j_report)

            report, packet = module.build_report(
                manifest_path=REPO_ROOT / "website" / "assets" / "benchmarks" / "latest" / "manifest.json",
                repo_root=REPO_ROOT,
                pre_5j_dependency_report_path=pre_5j_report,
                require_current_git=False,
                allow_dirty_worktree=True,
            )

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertFalse(report["benchmark_run_performed"])
        self.assertFalse(report["fallback_attempted"])
        self.assertFalse(report["external_engine_invoked"])
        self.assertEqual(report["artifact_completeness_status"], "passed")
        self.assertEqual(report["publication_claim_gate_status"], "passed")
        self.assertEqual(report["mirror_status"]["status"], "passed")
        self.assertEqual(packet["schema_version"], "shardloom.benchmark_route_packet.v1")
        self.assertEqual(
            packet["next_implementation_slice"],
            "`PROD-V1-2B` Correctness, conformance, and golden workflow closure for v1.",
        )
        self.assertIn("performance superiority", packet["forbidden_claims"])

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

    def test_benchmark_publish_doctor_fails_closed_on_missing_route_fields(self) -> None:
        module = self._load_script_module(
            "check_benchmark_publish_doctor.py",
            "benchmark_publish_doctor_missing_fields_for_test",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            artifact = root / "benchmark-results.json"
            manifest = root / "manifest.json"
            artifact.write_text(
                json.dumps(
                    {
                        "published_benchmark_artifact": {
                            "format_order": ["csv"],
                            "scenario_order": ["selective filter"],
                        },
                        "published_benchmark_rows": [
                            {
                                "engine": "shardloom",
                                "storage_format": "csv",
                                "scenario_name": "selective filter",
                                "status": "success",
                                "claim_gate_status": "claim_grade",
                                "fallback_attempted": False,
                                "external_engine_invoked": False,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            manifest.write_text(
                json.dumps(
                    {
                        "schema_version": "shardloom.website_benchmark_manifest.v1",
                        "generated_at_utc": "2026-01-01T00:00:00+00:00",
                        "benchmark_profile": "smoke",
                        "expected_lanes": ["shardloom"],
                        "available_lanes": ["shardloom"],
                        "missing_lanes": [],
                        "lane_versions": {},
                        "lane_availability_reasons": {},
                        "environment": {},
                        "claim_boundary": "fixture",
                        "performance_claim_allowed": False,
                        "route_runtime_status_schema_version": "shardloom.website.route_runtime_status.v1",
                        "route_runtime_status_vocabulary": [
                            "scoped_runtime_supported",
                            "feature_gated",
                            "fixture_smoke_only",
                            "unsupported",
                            "external_baseline_only",
                        ],
                        "benchmark_constitution_schema_version": "shardloom.benchmark_constitution_validation.v1",
                        "benchmark_constitution_validator": "scripts/check_benchmark_constitution.py",
                        "benchmark_constitution_required_field_order": [],
                        "benchmark_constitution_claim_gate_status": "not_claim_grade",
                        "benchmark_constitution_performance_claim_allowed": False,
                        "artifact_paths": {"json": str(artifact)},
                    }
                ),
                encoding="utf-8",
            )

            report, packet = module.build_report(
                manifest_path=manifest,
                repo_root=root,
                require_current_git=False,
                allow_dirty_worktree=True,
                max_age_days=-1,
            )

        self.assertEqual(report["status"], "blocked")
        self.assertTrue(
            any("missing route fields" in blocker for blocker in report["blockers"])
        )
        self.assertEqual(packet["status"], "blocked")
        self.assertIn(
            "check_benchmark_artifact_completeness.py",
            report["nearest_next_validation_command"],
        )

    def test_benchmark_artifact_completeness_rejects_source_state_prepare_as_admission(
        self,
    ) -> None:
        module = self._load_script_module(
            "check_benchmark_artifact_completeness.py",
            "benchmark_artifact_timing_contract_for_test",
        )
        row = {
            "engine": "shardloom",
            "status": "success",
            **self._shardloom_benchmark_route_fields("shardloom"),
            "source_state_prepare_micros": 2500,
            "source_admission_ms": 2.5,
            "source_admission_policy_micros": None,
            "source_admission_digest_policy_schema_version": (
                "shardloom.traditional_analytics.source_admission_digest_policy.v1"
            ),
            "source_admission_digest_policy_status": "metadata_fingerprint_fast_path",
            "source_admission_full_content_digest_requested": False,
            "source_admission_full_content_digest_micros": 0,
            "source_state_metadata_snapshot_micros": None,
            "source_state_manifest_validation_micros": None,
            "source_state_row_count_metadata_micros": None,
            "source_state_family_build_micros": None,
        }
        blockers: list[str] = []

        module.validate_rows({"published_benchmark_rows": [row]}, blockers)

        self.assertTrue(
            any(
                "maps broad source_state_prepare_micros to source_admission_ms"
                in blocker
                for blocker in blockers
            ),
            blockers,
        )

    def test_benchmark_publish_doctor_route_packet_markdown_is_compact(self) -> None:
        module = self._load_script_module(
            "check_benchmark_publish_doctor.py",
            "benchmark_publish_doctor_packet_for_test",
        )
        packet = {
            "status": "passed",
            "benchmark_profile": "full_local",
            "artifact_status": "complete",
            "route_runtime_status_counts": {"scoped_runtime_supported": 600},
            "operator_execution_mode_counts": {"residual_native": 456},
            "shardloom_claim_grade_rows": 600,
            "shardloom_unsupported_rows": 0,
            "external_baseline_rows": 720,
            "external_unsupported_rows": 6,
            "primary_bottleneck": "vortex_write",
            "operator_inventory_status": "encoded_native_promotion_pending",
            "next_implementation_slice": "GAR-RUNTIME-IMPL-6D-10 benchmark publish doctor",
            "required_validators": ["python3 scripts/check_benchmark_publish_doctor.py"],
            "forbidden_claims": ["performance superiority"],
            "claim_boundary": "publication readiness only",
            "fallback_boundary": "no fallback",
        }

        markdown = module.render_packet_markdown(packet)

        self.assertLess(len(markdown), 2500)
        self.assertIn("Benchmark Route Packet", markdown)
        self.assertIn("performance superiority", markdown)
        self.assertIn("GAR-RUNTIME-IMPL-6D-10", markdown)

    def test_benchmark_publication_claim_gate_recomputes_runtime_envelope_validation(
        self,
    ) -> None:
        module = self._load_script_module(
            "check_benchmark_publication_claim_gate.py",
            "benchmark_publication_claim_gate_runtime_revalidation_for_test",
        )
        lanes = list(module.expected_lanes_for_profile("full_local"))
        capillary_fields = {
            "vortex_capillary_preparation_activation_policy": "dynamic_size_complexity_gate.v1",
            "vortex_capillary_preparation_activation_result": "activated",
            "vortex_capillary_preparation_activation_reason": "claim_evidence_requested",
            "vortex_capillary_preparation_activation_observed_bytes": "67108864",
            "vortex_capillary_preparation_activation_observed_rows": "1000000",
            "vortex_capillary_preparation_activation_observed_columns": "8",
            "vortex_capillary_preparation_activation_observed_split_count": "8",
        }
        row = {
            "engine": "shardloom",
            "storage_format": "csv",
            "status": "success",
            "claim_gate_status": "claim_grade",
            "claim_grade_requirements_met": True,
            "runtime_execution_validation_status": "passed",
            "runtime_claim_allowed": True,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            **capillary_fields,
        }
        manifest = {
            "benchmark_profile": "full_local",
            "expected_lanes": lanes,
            "available_lanes": lanes,
        }
        payload = {
            "published_benchmark_artifact": {
                "format_order": ["csv", "parquet", "jsonl", "arrow-ipc", "avro", "orc"]
            },
            "published_benchmark_rows": [row],
        }

        blockers: list[str] = []
        report = module.validate_profile_and_rows(manifest, payload, blockers)

        self.assertEqual(report["shardloom_runtime_validation_counts"], {"blocked": 1})
        self.assertTrue(
            any("failed runtime envelope validation" in blocker for blocker in blockers)
        )
        self.assertTrue(
            any("runtime_claim_allowed=true" in blocker for blocker in blockers)
        )

    def test_benchmark_publication_claim_gate_requires_independent_claim_grade_proof(
        self,
    ) -> None:
        module = self._load_script_module(
            "check_benchmark_publication_claim_gate.py",
            "benchmark_publication_claim_gate_independent_proof_for_test",
        )
        lanes = list(module.expected_lanes_for_profile("full_local"))
        row = {
            "engine": "shardloom",
            "storage_format": "csv",
            "status": "success",
            "claim_gate_status": "claim_grade",
            "claim_grade_requirements_met": True,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "selected_execution_mode": "prepared_vortex",
            "source_state_id": "source-state://claim-grade-row",
            "source_state_digest": "fnv64:source",
            "prepared_state_id": "prepared-state://claim-grade-row",
            "prepared_state_digest": "fnv64:prepared",
            "data_decoded": False,
            "runtime_execution_certificate_id": "execution.claim-grade-row",
            "runtime_execution_certificate_status": "certified",
            "vortex_capillary_preparation_activation_policy": "dynamic_size_complexity_gate.v1",
            "vortex_capillary_preparation_activation_result": "activated",
            "vortex_capillary_preparation_activation_reason": "claim_evidence_requested",
            "vortex_capillary_preparation_activation_observed_bytes": "67108864",
            "vortex_capillary_preparation_activation_observed_rows": "1000000",
            "vortex_capillary_preparation_activation_observed_columns": "8",
            "vortex_capillary_preparation_activation_observed_split_count": "8",
        }
        manifest = {
            "benchmark_profile": "full_local",
            "expected_lanes": lanes,
            "available_lanes": lanes,
        }
        payload = {
            "published_benchmark_artifact": {
                "format_order": ["csv", "parquet", "jsonl", "arrow-ipc", "avro", "orc"]
            },
            "published_benchmark_rows": [row],
        }

        blockers: list[str] = []
        report = module.validate_profile_and_rows(manifest, payload, blockers)

        self.assertEqual(report["shardloom_runtime_validation_counts"], {"passed": 1})
        self.assertEqual(report["missing_independent_claim_proof_row_count"], 1)
        self.assertTrue(
            any("missing independent claim-grade proof" in blocker for blocker in blockers)
        )

    def test_benchmark_publication_claim_gate_rejects_unlinked_evidence_excluded_claim_row(
        self,
    ) -> None:
        module = self._load_script_module(
            "check_benchmark_publication_claim_gate.py",
            "benchmark_publication_claim_gate_unlinked_fast_path_for_test",
        )
        lanes = list(module.expected_lanes_for_profile("full_local"))
        row = {
            "engine": "shardloom-prepared-vortex",
            "storage_format": "csv",
            "status": "success",
            "claim_gate_status": "claim_grade",
            "claim_grade_requirements_met": True,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "selected_execution_mode": "prepared_vortex",
            "source_state_id": "source-state://unlinked-fast-path-row",
            "source_state_digest": "fnv64:source",
            "prepared_state_id": "prepared-state://unlinked-fast-path-row",
            "prepared_state_digest": "fnv64:prepared",
            "data_decoded": False,
            "iterations": 3,
            "reproducibility_min_iterations": 3,
            "reproducibility_iterations_met": True,
            "correctness_digest": "fnv64:correct",
            "correctness_digest_stable": True,
            "query_runtime_millis": 1.0,
            "cold_lane_timing_split_status": "complete",
            "computed_result_sink_replay_verified": True,
            "vortex_capillary_preparation_activation_policy": "dynamic_size_complexity_gate.v1",
            "vortex_capillary_preparation_activation_result": "activated",
            "vortex_capillary_preparation_activation_reason": "claim_evidence_requested",
            "vortex_capillary_preparation_activation_observed_bytes": "67108864",
            "vortex_capillary_preparation_activation_observed_rows": "1000000",
            "vortex_capillary_preparation_activation_observed_columns": "8",
            "vortex_capillary_preparation_activation_observed_split_count": "8",
            **self._shardloom_benchmark_route_fields("shardloom-prepared-vortex"),
        }
        row.update(
            {
                "runtime_execution_certificate_id": "missing",
                "runtime_execution_certificate_status": "missing",
                "runtime_execution_certificate_plan_ref": "missing",
                "certificate_link_status": "missing_required_certificate_link",
                "evidence_render_included_in_route_total": False,
            }
        )
        manifest = {
            "benchmark_profile": "full_local",
            "expected_lanes": lanes,
            "available_lanes": lanes,
        }
        payload = {
            "published_benchmark_artifact": {
                "format_order": ["csv", "parquet", "jsonl", "arrow-ipc", "avro", "orc"]
            },
            "published_benchmark_rows": [row],
        }

        blockers: list[str] = []
        report = module.validate_profile_and_rows(manifest, payload, blockers)

        self.assertEqual(report["missing_independent_claim_proof_row_count"], 1)
        self.assertTrue(
            any(
                "certificate_link_status!=linked_certified_runtime_execution" in blocker
                for blocker in blockers
            )
        )

    def test_benchmark_publication_claim_gate_blocks_local_artifact_paths(self) -> None:
        module = self._load_script_module(
            "check_benchmark_publication_claim_gate.py",
            "benchmark_publication_claim_gate_portable_refs_for_test",
        )
        lanes = list(module.expected_lanes_for_profile("full_local"))
        row = {
            "engine": "shardloom",
            "storage_format": "csv",
            "status": "success",
            "claim_gate_status": "claim_grade",
            "claim_grade_requirements_met": True,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "selected_execution_mode": "prepared_vortex",
            "source_state_id": "source-state://claim-grade-row",
            "source_state_digest": "fnv64:source",
            "prepared_state_id": "prepared-state://claim-grade-row",
            "prepared_state_digest": "fnv64:prepared",
            "data_decoded": False,
            "runtime_execution_certificate_id": "execution.claim-grade-row",
            "runtime_execution_certificate_status": "certified",
            "iterations": 3,
            "reproducibility_min_iterations": 3,
            "reproducibility_iterations_met": True,
            "correctness_digest": "fnv64:correct",
            "correctness_digest_stable": True,
            "query_runtime_millis": 1.0,
            "cold_lane_timing_split_status": "complete",
            "computed_result_sink_replay_verified": True,
            "sink_artifact_ref": r"C:\Users\test\shardloom\result.vortex",
            "vortex_capillary_preparation_activation_policy": "dynamic_size_complexity_gate.v1",
            "vortex_capillary_preparation_activation_result": "activated",
            "vortex_capillary_preparation_activation_reason": "claim_evidence_requested",
            "vortex_capillary_preparation_activation_observed_bytes": "67108864",
            "vortex_capillary_preparation_activation_observed_rows": "1000000",
            "vortex_capillary_preparation_activation_observed_columns": "8",
            "vortex_capillary_preparation_activation_observed_split_count": "8",
        }
        manifest = {
            "benchmark_profile": "full_local",
            "expected_lanes": lanes,
            "available_lanes": lanes,
        }
        payload = {
            "published_benchmark_artifact": {
                "format_order": ["csv", "parquet", "jsonl", "arrow-ipc", "avro", "orc"]
            },
            "published_benchmark_rows": [row],
        }

        blockers: list[str] = []
        report = module.validate_profile_and_rows(manifest, payload, blockers)

        self.assertEqual(report["nonportable_public_ref_count"], 1)
        self.assertTrue(
            any("non-portable local artifact paths" in blocker for blocker in blockers)
        )

    def test_benchmark_publication_claim_gate_requires_shardloom_row_backed_formats(
        self,
    ) -> None:
        module = self._load_script_module(
            "check_benchmark_publication_claim_gate.py",
            "benchmark_publication_claim_gate_shardloom_formats_for_test",
        )
        lanes = list(module.expected_lanes_for_profile("full_local"))
        capillary_fields = {
            "vortex_capillary_preparation_activation_policy": "dynamic_size_complexity_gate.v1",
            "vortex_capillary_preparation_activation_result": "activated",
            "vortex_capillary_preparation_activation_reason": "claim_evidence_requested",
            "vortex_capillary_preparation_activation_observed_bytes": "67108864",
            "vortex_capillary_preparation_activation_observed_rows": "1000000",
            "vortex_capillary_preparation_activation_observed_columns": "8",
            "vortex_capillary_preparation_activation_observed_split_count": "8",
        }
        rows = []
        for engine in module.REQUIRED_SHARDLOOM_PUBLICATION_ENGINES:
            rows.append(
                {
                    "engine": engine,
                    "storage_format": "csv",
                    "status": "success",
                    "claim_gate_status": "claim_grade",
                    "claim_grade_requirements_met": True,
                    "fallback_attempted": False,
                    "external_engine_invoked": False,
                    **capillary_fields,
                }
            )
        for storage_format in module.REQUIRED_PUBLICATION_FORMATS:
            rows.append(
                {
                    "engine": "pandas",
                    "storage_format": storage_format,
                    "status": "success",
                    "claim_gate_status": "external_baseline_only",
                    "claim_grade_requirements_met": False,
                    "external_baseline_only": True,
                    "fallback_attempted": False,
                    "external_engine_invoked": False,
                }
            )
        for engine in lanes:
            if engine.startswith("shardloom") or engine == "pandas":
                continue
            rows.append(
                {
                    "engine": engine,
                    "storage_format": "csv",
                    "status": "success",
                    "claim_gate_status": "external_baseline_only",
                    "claim_grade_requirements_met": False,
                    "external_baseline_only": True,
                    "fallback_attempted": False,
                    "external_engine_invoked": False,
                }
            )
        manifest = {
            "benchmark_profile": "full_local",
            "expected_lanes": lanes,
            "available_lanes": lanes,
        }
        payload = {
            "published_benchmark_artifact": {
                "format_order": ["csv", "parquet", "jsonl", "arrow-ipc", "avro", "orc"]
            },
            "published_benchmark_rows": rows,
        }

        blockers: list[str] = []
        report = module.validate_profile_and_rows(manifest, payload, blockers)

        self.assertEqual(
            report["published_formats"],
            sorted(module.REQUIRED_PUBLICATION_FORMATS),
        )
        self.assertEqual(report["shardloom_format_counts"], {"csv": 4})
        self.assertEqual(report["missing_shardloom_engine_format_cell_count"], 20)
        self.assertTrue(
            any(
                "ShardLoom publication rows missing public-format coverage" in blocker
                for blocker in blockers
            )
        )
        self.assertTrue(
            any("missing engine-format coverage" in blocker for blocker in blockers)
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
                    1150,
                    "Bump vortex from 0.73.0 to 0.74.0 in the vortex-upstream group",
                ),
                self._dependabot_pr(1151, "Bump serde_json from 1.0.149 to 1.0.150"),
                self._dependabot_pr(1152, "Bump sha2 from 0.10.9 to 0.11.0"),
                self._dependabot_pr(1153, "Bump rusqlite from 0.40.0 to 0.40.1"),
            ],
            "open_dependabot_pr_count": 5,
            "admitted_open_dependabot_prs": [1149, 1150, 1151, 1152, 1153],
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
        self.assertEqual(commands["package_channel_readiness"][0], "/tool/python3.12")
        self.assertEqual(
            commands["benchmark_constitution"],
            ["/tool/python3.12", "scripts/check_benchmark_constitution.py"],
        )
        self.assertEqual(
            commands["v1_source_prepared_state_scope_gate"],
            ["/tool/python3.12", "scripts/check_v1_source_prepared_state_scope.py"],
        )
        self.assertEqual(
            commands["v1_local_output_sink_scope_gate"],
            ["/tool/python3.12", "scripts/check_v1_local_output_sink_scope.py"],
        )
        self.assertEqual(
            commands["v1_api_schema_stability_gate"],
            ["/tool/python3.12", "scripts/check_v1_api_schema_stability.py"],
        )
        self.assertEqual(
            commands["v1_correctness_conformance_gate"],
            ["/tool/python3.12", "scripts/check_v1_correctness_conformance.py"],
        )
        self.assertEqual(
            commands["benchmark_artifact_completeness"],
            [
                "/tool/python3.12",
                "scripts/check_benchmark_artifact_completeness.py",
                "--manifest",
                "website/assets/benchmarks/latest/manifest.json",
                "--output",
                "target/benchmark-artifact-completeness-report.json",
            ],
        )
        self.assertEqual(
            commands["pre_5j_dependency_freshness_gate"],
            [
                "/tool/python3.12",
                "scripts/check_pre_5j_dependency_freshness.py",
                "--require-live-github",
                "--output",
                "target/pre-5j-dependency-freshness-gate.json",
            ],
        )
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
                    "vortex_local_file_routes": module.EXPECTED_VORTEX_LOCAL_FILE_ROUTES,
                    "source_input_formats": module.EXPECTED_SOURCE_INPUT_FORMATS,
                    "source_prepared_routes": module.EXPECTED_SOURCE_PREPARED_ROUTE_IDS,
                    "source_direct_routes": module.EXPECTED_SOURCE_DIRECT_ROUTE_IDS,
                    "source_generated_routes": module.EXPECTED_SOURCE_GENERATED_ROUTE_IDS,
                    "source_invalidation_cases": (
                        module.EXPECTED_SOURCE_INVALIDATION_CASES
                    ),
                    "output_formats": module.EXPECTED_OUTPUT_FORMATS,
                    "output_write_methods": module.EXPECTED_OUTPUT_WRITE_METHODS,
                    "output_routes": module.EXPECTED_OUTPUT_ROUTE_IDS,
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
                    "admitted_stage_count_min": module.EXPECTED_ADMITTED_STAGE_COUNT_MIN,
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
                        "gap_id": "general_fuzz_beyond_seeded_property_lane",
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
                "property_execution_performed": True,
                "decoded_reference_differential_execution_performed": True,
                "semantic_conformance_suite_status": "passed",
                "correctness_harness_boundary_status": "passed",
                "executable_case_ids": sorted(module.REQUIRED_SEMANTIC_CASE_IDS),
                "unsupported_case_ids": unsupported_cases,
                "runtime_error_case_ids": runtime_error_cases,
                "invalid_shape_case_ids": invalid_shape_cases,
                "property_lane_count": 1,
                "remaining_matrix_gaps": [],
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
                "local_vortex_primitive_route_count": (
                    module.EXPECTED_VORTEX_PRIMITIVE_ROUTES
                ),
                "local_file_benchmark_route_count": (
                    module.EXPECTED_VORTEX_LOCAL_FILE_ROUTES
                ),
                "local_vortex_primitive_v1_scope_ready": True,
                "user_route_v1_vortex_scope_ready": True,
                **false_fields,
            },
        )
        write(
            paths.source_prepared_state,
            {
                "schema_version": "shardloom.v1_source_prepared_state_scope_report.v1",
                "status": "passed",
                "blockers": [],
                "supported_input_formats": [
                    f"format-{index}"
                    for index in range(module.EXPECTED_SOURCE_INPUT_FORMATS)
                ],
                "prepared_route_ids": [
                    f"prepared-{index}"
                    for index in range(module.EXPECTED_SOURCE_PREPARED_ROUTE_IDS)
                ],
                "direct_transient_route_ids": [
                    f"direct-{index}"
                    for index in range(module.EXPECTED_SOURCE_DIRECT_ROUTE_IDS)
                ],
                "generated_route_ids": [
                    f"generated-{index}"
                    for index in range(module.EXPECTED_SOURCE_GENERATED_ROUTE_IDS)
                ],
                "invalidation_case_ids": [
                    f"invalidation-{index}"
                    for index in range(module.EXPECTED_SOURCE_INVALIDATION_CASES)
                ],
                "source_prepared_benchmark_required_fields_ready": True,
                "source_prepared_benchmark_rows_with_required_fields": 1080,
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
                    f"method-{index}" for index in range(module.EXPECTED_OUTPUT_WRITE_METHODS)
                ],
                "output_route_ids": [
                    f"route-{index}" for index in range(module.EXPECTED_OUTPUT_ROUTE_IDS)
                ],
                "local_output_sink_benchmark_required_fields_ready": True,
                "local_output_sink_benchmark_replay_ready": True,
                "local_output_sink_benchmark_rows_with_required_fields": 960,
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
        self.assertTrue(report["correctness_claim_allowed"])
        self.assertTrue(report["decoded_reference_differential_execution_performed"])
        self.assertTrue(report["property_execution_performed"])
        self.assertFalse(report["fallback_attempted"])
        self.assertFalse(report["external_engine_invoked"])
        self.assertEqual(
            report["summaries"]["admitted_semantics"]["required_semantic_case_count"],
            len(module.REQUIRED_SEMANTIC_CASE_IDS),
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
        pre_5j_expected = "python scripts/check_pre_5j_dependency_freshness.py"
        self.assertTrue(
            module.validation_command_passed(
                {
                    pre_5j_expected
                    + " --require-live-github --output target/pre-5j-dependency-freshness-gate.json": "passed"
                },
                pre_5j_expected,
            )
        )
        self.assertFalse(
            module.validation_command_passed(
                {pre_5j_expected + " --require-live-github": "failed"},
                pre_5j_expected,
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
                    1150,
                    "Bump vortex from 0.73.0 to 0.74.0 in the vortex-upstream group",
                ),
                self._dependabot_pr(1151, "Bump serde_json from 1.0.149 to 1.0.150"),
                self._dependabot_pr(1152, "Bump sha2 from 0.10.9 to 0.11.0"),
                self._dependabot_pr(1153, "Bump rusqlite from 0.40.0 to 0.40.1"),
            ],
            open_prs_status="loaded_from_file",
            require_live_github=True,
        )

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(
            report["admitted_open_dependabot_prs"],
            [1149, 1150, 1151, 1152, 1153],
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
            cli_manifest = root / "shardloom-cli" / "Cargo.toml"
            cli_manifest.parent.mkdir(parents=True)
            cli_manifest.write_text(
                "[dependencies]\n"
                'rusqlite = { version = "0.40.1", default-features = false, features = ["bundled"] }\n',
                encoding="utf-8",
            )
            vortex_manifest = root / "shardloom-vortex" / "Cargo.toml"
            vortex_manifest.parent.mkdir(parents=True)
            vortex_manifest.write_text(
                "[dependencies]\n"
                'vortex = { version = "0.74", optional = true }\n',
                encoding="utf-8",
            )
            (root / "Cargo.lock").write_text(
                "[[package]]\n"
                'name = "vortex"\n'
                'version = "0.74.0"\n'
                "\n"
                "[[package]]\n"
                'name = "rusqlite"\n'
                'version = "0.40.1"\n'
                "\n"
                "[[package]]\n"
                'name = "libsqlite3-sys"\n'
                'version = "0.38.1"\n',
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
            {"version": "0.40.1", "default-features": False, "features": ["bundled"]},
        )
        self.assertEqual(vortex, {"version": "0.74", "optional": True})
        self.assertEqual(lock_versions["vortex"], "0.74.0")
        self.assertEqual(lock_versions["rusqlite"], "0.40.1")
        self.assertEqual(lock_versions["libsqlite3-sys"], "0.38.1")

    def test_pre_5j_dependency_freshness_blocks_stale_vortex_provider_surfaces(self) -> None:
        module = self._load_script_module(
            "check_pre_5j_dependency_freshness.py",
            "check_pre_5j_dependency_freshness_provider_surface_for_test",
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            benchmark = root / "benchmarks" / "traditional_analytics" / "run.py"
            benchmark.parent.mkdir(parents=True)
            benchmark.write_text(
                'UPSTREAM_VORTEX_PROVIDER_VERSION = "0.73"\n'
                'SHARDLOOM_VORTEX_PROVIDER_VERSION = (\n'
                '    "shardloom-vortex=0.1.0;vortex=0.73"\n'
                ')\n'
                'provider_version = "0.72" if admitted else "not_applicable"\n',
                encoding="utf-8",
            )
            client_tests = root / "python" / "tests" / "test_cli_client.py"
            client_tests.parent.mkdir(parents=True)
            client_tests.write_text(
                '{"key": "provider_version", "value": "0.73"}\n'
                'self.assertEqual(result.provider_version, "0.73")\n',
                encoding="utf-8",
            )

            rows = module.validate_vortex_provider_version_surfaces(root)

        blockers = [blocker for row in rows for blocker in row["blockers"]]
        self.assertTrue(
            any('"0.72" if admitted' in blocker for blocker in blockers),
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
        self.assertNotIn(
            "python scripts/check_release_readiness.py --allow-blocked",
            release_lane.commands,
        )
        self.assertIn("continue-on-error: true", release_lane.workflow_markers)

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

    def _write_v1_front_door_runtime_scope_fixture(
        self,
        module: object,
        repo_root: Path,
    ) -> None:
        scenario_names = sorted(module.EXPECTED_EXAMPLE_SCENARIOS)
        supported_rows = sorted(module.SUPPORTED_PARITY_ROWS)
        pending_rows = sorted(module.BROAD_PENDING_PARITY_ROWS)

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

                from typing import Any, Callable, Sequence

                EXPECTED_ERROR_SCENARIOS = frozenset({{"malformed_timestamp_cast"}})
                profile_order: Sequence[str] = ("release", "debug")
                fallback_attempted = False
                external_engine_invoked = False
                timing_components = {{}}
                python_wall_millis = 0.0


                def scenario_actions(ctx: Any, sl: Any) -> list[tuple[str, Callable[[], Any]]]:
                    return [
                        {", ".join(f'("{name}", lambda: None)' for name in scenario_names)}
                    ]
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


                _SUPPORTED_ROWS = {supported_rows!r}
                _PENDING_ROWS = {pending_rows!r}


                class ShardLoomContext:
                    def __init__(self, client=None):
                        self.client = client

                    def front_door_parity_matrix(self):
                        rows = [
                            SimpleNamespace(
                                row_id=row_id,
                                support_status="runtime_supported",
                                runtime_gap_status="admitted_scope",
                                parity_status="equivalent_admitted_scope",
                                shared_runtime_path="scoped_local_front_door",
                                blocker_id=None,
                                fallback_attempted=False,
                                external_engine_invoked=False,
                                claim_boundary="scoped_v1_front_door_only",
                            )
                            for row_id in _SUPPORTED_ROWS
                        ]
                        rows.extend(
                            SimpleNamespace(
                                row_id=row_id,
                                support_status="pending_broad_scope",
                                runtime_gap_status="pending_precise_scope",
                                parity_status="front_door_gap",
                                shared_runtime_path="not_admitted_for_broad_scope",
                                blocker_id=f"v1.front_door.{{row_id}}",
                                fallback_attempted=False,
                                external_engine_invoked=False,
                                claim_boundary="outside_scoped_v1_front_door",
                            )
                            for row_id in _PENDING_ROWS
                        )
                        return SimpleNamespace(
                            rows=tuple(rows),
                            scoped_local_front_door_parity_supported=True,
                            flexible_anything_claim_allowed=False,
                            performance_equivalence_claim_allowed=False,
                            all_no_fallback_no_external_engine=True,
                        )

                    def user_route_capability_report(self):
                        rows = (
                            SimpleNamespace(
                                front_door_id="python_prepare_vortex",
                                route_runtime_status="scoped_runtime_supported",
                                fallback_attempted=False,
                                external_engine_invoked=False,
                                public_user_surface="ctx.prepare_vortex / prepare_vortex",
                                claim_boundary="scoped_v1_front_door_only",
                            ),
                            SimpleNamespace(
                                front_door_id="sql_prepare_vortex",
                                route_runtime_status="scoped_runtime_supported",
                                fallback_attempted=False,
                                external_engine_invoked=False,
                                public_user_surface="sql prepare_vortex",
                                claim_boundary="scoped_v1_front_door_only",
                            ),
                        )
                        return SimpleNamespace(
                            public_front_door_route_rows=rows,
                            all_no_fallback_no_external_engine=True,
                            unsupported_local_benchmark_route_ids=(),
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
                            route_runtime_status="scoped_runtime_supported",
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


                def _v1_vortex_user_report(self):
                    front_rows = (
                        SimpleNamespace(
                            front_door_id="python_prepare_vortex",
                            route_runtime_status="scoped_runtime_supported",
                            fallback_attempted=False,
                            external_engine_invoked=False,
                            public_user_surface="ctx.prepare_vortex / prepare_vortex",
                            claim_boundary="scoped_v1_front_door_only",
                        ),
                        SimpleNamespace(
                            front_door_id="sql_prepare_vortex",
                            route_runtime_status="scoped_runtime_supported",
                            fallback_attempted=False,
                            external_engine_invoked=False,
                            public_user_surface="sql prepare_vortex",
                            claim_boundary="scoped_v1_front_door_only",
                        ),
                    )
                    route_rows = {{}}
                    for route_id in (
                        "local_file_prepare_once_first_query",
                        "local_file_prepare_once_batch",
                        "prepared_vortex_warm_query",
                        "native_vortex_query",
                        "local_vortex_primitive_report",
                        "generated_rows_local_output",
                    ):
                        route_rows[route_id] = SimpleNamespace(
                            route_id=route_id,
                            route_runtime_status="scoped_runtime_supported",
                            fallback_attempted=False,
                            external_engine_invoked=False,
                            vortex_normalization_point="native_vortex_boundary",
                            materialization_decode_boundary="bounded report",
                        )

                    def route(route_id):
                        return route_rows[route_id]

                    return SimpleNamespace(
                        public_front_door_route_rows=front_rows,
                        all_no_fallback_no_external_engine=True,
                        unsupported_local_benchmark_route_ids=(),
                        v1_vortex_scope_document="docs/architecture/v1-vortex-runtime-scope.md",
                        v1_vortex_supported_starting_states=V1_VORTEX_SUPPORTED_STARTING_STATES,
                        v1_vortex_supported_primitive_route_ids=V1_VORTEX_SUPPORTED_PRIMITIVE_ROUTE_IDS,
                        v1_vortex_supported_benchmark_scenario_ids=V1_VORTEX_SUPPORTED_BENCHMARK_SCENARIO_IDS,
                        v1_vortex_unsupported_boundary_ids=V1_VORTEX_UNSUPPORTED_BOUNDARY_IDS,
                        v1_vortex_feature_profile_decision="feature_gated_local_vortex_runtime",
                        v1_vortex_scope_ready=True,
                        route=route,
                    )


                def _v1_vortex_local_file_report(self):
                    rows = tuple(
                        SimpleNamespace(
                            scenario_id=scenario_id,
                            route_id="local_file_prepare_once_first_query",
                            start_state="raw_compat_source",
                            vortex_normalization_point="SourceState -> vortex_ingest -> VortexPreparedState",
                            preparation_route="vortex_ingest_prepare_once",
                            selected_execution_mode="prepared_vortex",
                            output_route="prepared result",
                            evidence_route="execution certificate and Native I/O",
                            materialization_decode_boundary="bounded result",
                            route_runtime_status="prepared_route_supported",
                            fallback_attempted=False,
                            external_engine_invoked=False,
                            required_evidence=("execution_certificate", "native_io_certificate"),
                            claim_gate_status="not_claim_grade",
                            claim_boundary="scoped prepared Vortex benchmark row",
                        )
                        for scenario_id in V1_VORTEX_SUPPORTED_BENCHMARK_SCENARIO_IDS
                    )
                    return SimpleNamespace(
                        rows=rows,
                        schema_version="shardloom.local_file_benchmark_route_report.v1",
                        scenario_ids=V1_VORTEX_SUPPORTED_BENCHMARK_SCENARIO_IDS,
                        unsupported_scenario_ids=(),
                        all_no_fallback_no_external_engine=True,
                    )


                ShardLoomContext.local_vortex_primitive_route_report = lambda self: _V1VortexPrimitiveReport()
                ShardLoomContext.user_route_capability_report = _v1_vortex_user_report
                ShardLoomContext.local_file_benchmark_route_report = _v1_vortex_local_file_report
                '''
            ),
            encoding="utf-8",
        )

    def _write_v1_source_prepared_state_scope_fixture(
        self,
        module: object,
        repo_root: Path,
    ) -> None:
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
            "partition_pruning",
            "many_small_files_scan",
            "null_heavy_aggregate",
            "high_cardinality_string_group_distinct",
            "nested_json_field_scan",
            "small_change_over_large_base",
        ]
        invalidation_cases = [
            "cold_prepare_no_manifest",
            "warm_reuse_manifest_match",
            "source_changed",
            "artifact_changed",
            "schema_changed",
            "policy_changed",
            "version_changed",
            "missing_artifact",
            "corrupted_manifest",
        ]
        required_fields = [
            "source_state_id",
            "source_state_digest",
            "source_state_fingerprint",
            "source_schema_fingerprint",
            "source_parse_plan_id",
            "source_split_manifest_id",
            "prepared_state_id",
            "prepared_state_digest",
            "prepared_state_reuse_hit",
            "prepared_state_reuse_reason",
            "prepared_state_reuse_manifest_digest",
            "prepared_state_invalidation_reason",
            "fallback_attempted",
            "external_engine_invoked",
        ]
        fixture_paths = [
            "docs/architecture/fixtures/v1-source-prepared-state/source-state-golden.json",
            "docs/architecture/fixtures/v1-source-prepared-state/vortex-prepared-state-golden.json",
            "docs/architecture/fixtures/v1-source-prepared-state/reuse-invalidation-matrix.json",
        ]

        for rel_path, markers in {
            module.DOC_PATH.as_posix(): module.DOC_MARKERS,
            **module.PUBLIC_DOC_MARKERS,
        }.items():
            path = repo_root / rel_path
            path.parent.mkdir(parents=True, exist_ok=True)
            existing = path.read_text(encoding="utf-8") if path.exists() else ""
            path.write_text(existing + "\n".join(markers) + "\n", encoding="utf-8")

        for rel_path in fixture_paths[:2]:
            path = repo_root / rel_path
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(
                    {
                        "schema_version": "fixture.v1",
                        "scope_document": module.DOC_PATH.as_posix(),
                        "claim_gate_status": "not_claim_grade",
                        "fallback_attempted": False,
                        "external_engine_invoked": False,
                    }
                ),
                encoding="utf-8",
            )
        matrix_path = repo_root / fixture_paths[2]
        matrix_path.parent.mkdir(parents=True, exist_ok=True)
        matrix_path.write_text(
            json.dumps(
                {
                    "schema_version": (
                        "shardloom.v1_source_prepared_state_reuse_invalidation_matrix.v1"
                    ),
                    "scope_document": module.DOC_PATH.as_posix(),
                    "cases": [
                        {
                            "case_id": case_id,
                            "reuse_hit": case_id == "warm_reuse_manifest_match",
                            "reuse_reason": (
                                "manifest_fingerprints_match"
                                if case_id == "warm_reuse_manifest_match"
                                else case_id
                            ),
                            "invalidation_reason": (
                                "none"
                                if case_id == "warm_reuse_manifest_match"
                                else case_id
                            ),
                        }
                        for case_id in invalidation_cases
                    ],
                    "claim_gate_status": "not_claim_grade",
                    "fallback_attempted": False,
                    "external_engine_invoked": False,
                }
            ),
            encoding="utf-8",
        )
        benchmark_path = repo_root / module.LATEST_BENCHMARK_ARTIFACT
        benchmark_path.parent.mkdir(parents=True, exist_ok=True)
        benchmark_path.write_text(
            json.dumps(
                {
                    "published_benchmark_rows": [
                        {
                            "engine": "shardloom-prepared-vortex",
                            "scenario_id": scenario_id,
                            "route_lane_id": "warm_prepared_query",
                            **{
                                field: (
                                    False
                                    if field
                                    in {"fallback_attempted", "external_engine_invoked"}
                                    else f"{field}:{scenario_id}"
                                )
                                for field in required_fields
                            },
                        }
                        for scenario_id in scenario_ids
                    ]
                }
            ),
            encoding="utf-8",
        )

        package_init = repo_root / "python" / "src" / "shardloom" / "__init__.py"
        existing = package_init.read_text(encoding="utf-8")
        package_init.write_text(
            existing
            + textwrap.dedent(
                f'''

                _V1_SOURCE_PREPARED_SCENARIO_IDS = {tuple(scenario_ids)!r}
                _V1_SOURCE_PREPARED_INVALIDATION_CASES = {tuple(invalidation_cases)!r}
                _V1_SOURCE_PREPARED_REQUIRED_FIELDS = {tuple(required_fields)!r}
                _V1_SOURCE_PREPARED_FIXTURES = {tuple(fixture_paths)!r}


                def _source_prepared_row(route_id, scope, *, scenario_id=None):
                    return SimpleNamespace(
                        route_id=route_id,
                        route_display_name=route_id,
                        scenario_id=scenario_id or route_id,
                        scenario_name=scenario_id or route_id,
                        start_state="raw_compat_source",
                        vortex_normalization_point=(
                            "local compatibility source -> SourceState -> "
                            "no persistent VortexPreparedState"
                            if scope == "not_applicable_no_prepared_state"
                            else "SourceState -> vortex_ingest -> VortexPreparedState"
                        ),
                        source_route="UniversalIngress -> SourceState",
                        preparation_route=(
                            "direct_compatibility_transient_no_persistent_preparation"
                            if scope == "not_applicable_no_prepared_state"
                            else "vortex_ingest_prepare_once"
                        ),
                        execution_mode="prepared_vortex",
                        selected_execution_mode="prepared_vortex",
                        output_route="bounded report",
                        evidence_route="execution certificate and Native I/O evidence",
                        materialization_decode_boundary="bounded report",
                        source_state_fingerprint="sha256:source",
                        source_schema_fingerprint="sha256:schema",
                        source_parse_plan_id="parse-plan://fixture",
                        source_split_manifest_id="split-manifest://fixture",
                        prepared_state_fingerprint="sha256:prepared",
                        prepared_state_reuse_scope=scope,
                        prepared_state_reuse_manifest_path=(
                            "target/.shardloom/prepared-state-reuse.manifest"
                            if scope == "artifact_adjacent_manifest_local_vortex_artifacts"
                            else "target/.shardloom/prepared-vortex-reuse-manifest.json"
                        ),
                        prepared_state_reuse_policy=(
                            "artifact_adjacent_local_prepared_state_reuse.v1"
                            if scope == "artifact_adjacent_manifest_local_vortex_artifacts"
                            else "shardloom.python.prepared_vortex_reuse_manifest.v1"
                        ),
                        prepared_state_reuse_hit=True,
                        prepared_state_reuse_reason="manifest_fingerprints_match",
                        prepared_state_reuse_manifest_digest="sha256:manifest",
                        prepared_state_invalidation_reason="none",
                        route_runtime_status="scoped_runtime_supported",
                        fallback_attempted=False,
                        external_engine_invoked=False,
                        required_evidence=("execution_certificate", "native_io_certificate"),
                        claim_gate_status="not_claim_grade",
                        claim_boundary="scoped source/prepared-state fixture",
                    )


                def _v1_source_prepared_scope_report(self):
                    prepared_routes = (
                        "local_file_cold_certified_route",
                        "local_file_prepare_once_first_query",
                        "local_file_prepare_once_batch",
                        "prepared_vortex_warm_query",
                    )
                    prepared_user = tuple(
                        _source_prepared_row(
                            route_id,
                            "explicit_prepared_state_input"
                            if route_id == "prepared_vortex_warm_query"
                            else "workspace_manifest_local_vortex_artifacts",
                        )
                        for route_id in prepared_routes
                    )
                    direct_user = (
                        _source_prepared_row(
                            "local_file_direct_transient_route",
                            "not_applicable_no_prepared_state",
                        ),
                    )
                    generated_user = (
                        _source_prepared_row(
                            "generated_rows_local_output",
                            "artifact_adjacent_manifest_local_vortex_artifacts",
                        ),
                    )
                    prepared_local = tuple(
                        _source_prepared_row(
                            "local_file_prepare_once_first_query",
                            "workspace_manifest_local_vortex_artifacts",
                            scenario_id=scenario_id,
                        )
                        for scenario_id in _V1_SOURCE_PREPARED_SCENARIO_IDS
                    )
                    direct_local = tuple(
                        _source_prepared_row(
                            "local_file_direct_transient_route",
                            "not_applicable_no_prepared_state",
                            scenario_id=scenario_id,
                        )
                        for scenario_id in _V1_SOURCE_PREPARED_SCENARIO_IDS
                    )
                    return SimpleNamespace(
                        schema_version="shardloom.v1_source_prepared_state_scope.v1",
                        report_id="prod-v1-1c.source_prepared_state_scope",
                        scope_document="docs/architecture/v1-source-prepared-state-scope.md",
                        canonical_route=(
                            "UniversalIngress -> SourceState -> vortex_ingest -> "
                            "VortexPreparedState -> prepared_vortex"
                        ),
                        direct_transient_route=(
                            "UniversalIngress -> SourceState -> direct_compatibility_transient"
                        ),
                        supported_input_formats=("csv", "jsonl", "parquet", "arrow-ipc", "avro", "orc"),
                        prepared_route_ids=prepared_routes,
                        direct_transient_route_ids=("local_file_direct_transient_route",),
                        generated_route_ids=("generated_rows_local_output",),
                        invalidation_case_ids=_V1_SOURCE_PREPARED_INVALIDATION_CASES,
                        golden_fixture_paths=_V1_SOURCE_PREPARED_FIXTURES,
                        required_runtime_fields=_V1_SOURCE_PREPARED_REQUIRED_FIELDS,
                        unsupported_boundary_ids=(
                            "global_hidden_cache",
                            "external_cache_service",
                            "object_store_prepared_state_reuse",
                            "table_catalog_prepared_state_reuse",
                            "broad_non_local_preparation",
                        ),
                        prepared_user_route_rows=prepared_user,
                        direct_transient_user_route_rows=direct_user,
                        generated_user_route_rows=generated_user,
                        prepared_local_file_rows=prepared_local,
                        direct_transient_local_file_rows=direct_local,
                        local_file_routes=SimpleNamespace(
                            scenario_ids=_V1_SOURCE_PREPARED_SCENARIO_IDS
                        ),
                        all_no_fallback_no_external_engine=True,
                        all_prepared_routes_expose_reuse_contract=True,
                        all_generated_routes_expose_artifact_adjacent_reuse=True,
                        all_direct_transient_routes_are_labeled_non_persistent=True,
                        all_local_file_prepared_rows_expose_source_and_reuse_evidence=True,
                        v1_scope_ready=True,
                        claim_gate_status="not_claim_grade",
                        performance_claim_allowed=False,
                        production_claim_allowed=False,
                        spark_replacement_claim_allowed=False,
                    )


                ShardLoomContext.source_prepared_state_scope_report = (
                    _v1_source_prepared_scope_report
                )
                '''
            ),
            encoding="utf-8",
        )

    def _write_v1_local_output_sink_scope_fixture(
        self,
        module: object,
        repo_root: Path,
    ) -> None:
        supported_formats = ("jsonl", "csv", "parquet", "arrow-ipc", "avro", "orc", "vortex")
        default_formats = ("jsonl", "csv")
        feature_gated_formats = ("parquet", "arrow-ipc", "avro", "orc", "vortex")
        write_methods = (
            "write",
            "write_jsonl",
            "write_csv",
            "write_parquet",
            "write_arrow_ipc",
            "write_avro",
            "write_orc",
            "write_vortex",
            "fanout",
        )
        route_ids = (
            "local_file_direct_transient_route",
            "local_file_cold_certified_route",
            "local_file_prepare_once_first_query",
            "local_file_prepare_once_batch",
            "prepared_vortex_warm_query",
            "native_vortex_query",
            "generated_rows_local_output",
            "quarantine_output_route",
        )
        policy_ids = (
            "error_if_exists_by_default",
            "explicit_allow_overwrite",
            "append_mode_unsupported",
            "atomic_rename_same_directory",
            "partial_write_cleanup_reported",
        )
        required_fields = (
            "output_route",
            "output_native_io_certificate_status",
            "computed_result_sink_native_io_certificate_status",
            "computed_result_sink_replay_verified",
            "output_materialization_required",
            "output_plan_digest",
            "result_sink_write_millis",
            "sink_timing_included_in_route_total",
            "timing_surface",
            "fallback_attempted",
            "external_engine_invoked",
        )
        fixture_paths = (
            "docs/architecture/fixtures/v1-local-output-sink/output-scope-golden.json",
            "docs/architecture/fixtures/v1-local-output-sink/output-policy-matrix.json",
            "docs/architecture/fixtures/v1-local-output-sink/output-replay-manifest-golden.json",
        )

        for rel_path, markers in {
            module.DOC_PATH.as_posix(): module.DOC_MARKERS,
            **module.PUBLIC_DOC_MARKERS,
        }.items():
            path = repo_root / rel_path
            path.parent.mkdir(parents=True, exist_ok=True)
            existing = path.read_text(encoding="utf-8") if path.exists() else ""
            path.write_text(existing + "\n".join(markers) + "\n", encoding="utf-8")

        scope_path = repo_root / fixture_paths[0]
        scope_path.parent.mkdir(parents=True, exist_ok=True)
        scope_path.write_text(
            json.dumps(
                {
                    "schema_version": "shardloom.v1_local_output_sink_scope_golden.v1",
                    "scope_document": module.DOC_PATH.as_posix(),
                    "supported_output_formats": list(supported_formats),
                    "default_output_formats": list(default_formats),
                    "feature_gated_output_formats": list(feature_gated_formats),
                    "user_write_methods": list(write_methods),
                    "claim_gate_status": "not_claim_grade",
                    "fallback_attempted": False,
                    "external_engine_invoked": False,
                }
            ),
            encoding="utf-8",
        )
        policy_path = repo_root / fixture_paths[1]
        policy_path.write_text(
            json.dumps(
                {
                    "schema_version": "shardloom.v1_local_output_sink_policy_matrix.v1",
                    "scope_document": module.DOC_PATH.as_posix(),
                    "policies": [
                        {
                            "policy_id": policy_id,
                            "runtime_posture": "fixture",
                            "write_io_allowed": policy_id != "append_mode_unsupported",
                            "deterministic_diagnostic_required": policy_id == "append_mode_unsupported",
                        }
                        for policy_id in policy_ids
                    ],
                    "claim_gate_status": "not_claim_grade",
                    "fallback_attempted": False,
                    "external_engine_invoked": False,
                }
            ),
            encoding="utf-8",
        )
        replay_path = repo_root / fixture_paths[2]
        replay_path.write_text(
            json.dumps(
                {
                    "schema_version": "shardloom.v1_local_output_sink_replay_manifest.v1",
                    "scope_document": module.DOC_PATH.as_posix(),
                    "manifest_fields": list(required_fields),
                    "claim_gate_status": "not_claim_grade",
                    "fallback_attempted": False,
                    "external_engine_invoked": False,
                }
            ),
            encoding="utf-8",
        )

        benchmark_path = repo_root / module.LATEST_BENCHMARK_ARTIFACT
        benchmark_path.parent.mkdir(parents=True, exist_ok=True)
        if benchmark_path.exists():
            benchmark_payload = json.loads(benchmark_path.read_text(encoding="utf-8"))
        else:
            benchmark_payload = {"published_benchmark_rows": []}
        rows = benchmark_payload.setdefault("published_benchmark_rows", [])
        source_prepared_fields = (
            "source_state_id",
            "source_state_digest",
            "source_state_fingerprint",
            "source_schema_fingerprint",
            "source_parse_plan_id",
            "source_split_manifest_id",
            "prepared_state_id",
            "prepared_state_digest",
            "prepared_state_reuse_hit",
            "prepared_state_reuse_reason",
            "prepared_state_reuse_manifest_digest",
            "prepared_state_invalidation_reason",
        )
        sink_row = {
            "engine": "shardloom-prepared-vortex",
            "scenario_id": "clean_cast_filter_write",
            "route_lane_id": "warm_prepared_query",
            "output_route": "local_result_sink_or_report",
            "output_native_io_certificate_status": "certified",
            "computed_result_sink_native_io_certificate_status": "certified",
            "computed_result_sink_replay_verified": True,
            "output_materialization_required": "result_sink_materializes_computed_result",
            "output_plan_digest": "sha256:output-plan",
            "result_sink_write_millis": 1.0,
            "sink_timing_included_in_route_total": False,
            "timing_surface": "hot_runtime",
            "fallback_attempted": False,
            "external_engine_invoked": False,
            **{
                field: f"{field}:clean_cast_filter_write"
                for field in source_prepared_fields
            },
        }
        for row in rows:
            if (
                isinstance(row, dict)
                and row.get("engine") == sink_row["engine"]
                and row.get("scenario_id") == sink_row["scenario_id"]
                and row.get("route_lane_id") == sink_row["route_lane_id"]
            ):
                row.update(sink_row)
                break
        else:
            rows.append(sink_row)
        benchmark_path.write_text(json.dumps(benchmark_payload), encoding="utf-8")

        package_init = repo_root / "python" / "src" / "shardloom" / "__init__.py"
        existing = package_init.read_text(encoding="utf-8")
        package_init.write_text(
            existing
            + textwrap.dedent(
                f'''

                _V1_LOCAL_OUTPUT_FORMATS = {supported_formats!r}
                _V1_LOCAL_OUTPUT_DEFAULT_FORMATS = {default_formats!r}
                _V1_LOCAL_OUTPUT_FEATURE_GATED_FORMATS = {feature_gated_formats!r}
                _V1_LOCAL_OUTPUT_METHODS = {write_methods!r}
                _V1_LOCAL_OUTPUT_ROUTES = {route_ids!r}
                _V1_LOCAL_OUTPUT_POLICIES = {policy_ids!r}
                _V1_LOCAL_OUTPUT_FIXTURES = {fixture_paths!r}
                _V1_LOCAL_OUTPUT_REQUIRED_FIELDS = {required_fields!r}


                def _local_output_method(method):
                    return SimpleNamespace(
                        method=method,
                        family="write",
                        support_status="fixture_smoke_supported",
                        required_evidence=("output_native_io_certificate", "result_replay_verified"),
                        runtime_execution=True,
                        data_read=True,
                        write_io=True,
                        materialization_required=True,
                        fallback_attempted=False,
                        external_engine_invoked=False,
                        claim_gate_status="not_claim_grade",
                        claim_boundary="feature-gated flat scalar local output fixture for jsonl csv parquet arrow-ipc avro orc vortex",
                    )


                def _local_output_route(route_id):
                    return SimpleNamespace(
                        route_id=route_id,
                        route_display_name=route_id,
                        desired_outputs=("local_jsonl", "local_csv", "feature_gated_local_vortex_output"),
                        output_route="local output/result sink route",
                        evidence_route="OutputPlan, output Native I/O certificate, replay evidence",
                        materialization_decode_boundary="explicit local sink boundary",
                        route_runtime_status="scoped_runtime_supported",
                        required_evidence=("output_native_io_certificate", "result_replay_verified"),
                        fallback_attempted=False,
                        external_engine_invoked=False,
                        claim_gate_status="not_claim_grade",
                        claim_boundary="scoped local output/sink fixture for feature-gated flat scalar parquet arrow-ipc avro orc vortex",
                        no_fallback_no_external_engine=True,
                    )


                def _v1_local_output_sink_scope_report(self):
                    return SimpleNamespace(
                        schema_version="shardloom.v1_local_output_sink_scope.v1",
                        report_id="prod-v1-1d.local_output_sink_scope",
                        scope_document="docs/architecture/v1-local-output-sink-scope.md",
                        supported_output_formats=_V1_LOCAL_OUTPUT_FORMATS,
                        default_output_formats=_V1_LOCAL_OUTPUT_DEFAULT_FORMATS,
                        feature_gated_output_formats=_V1_LOCAL_OUTPUT_FEATURE_GATED_FORMATS,
                        user_write_methods=_V1_LOCAL_OUTPUT_METHODS,
                        output_route_ids=_V1_LOCAL_OUTPUT_ROUTES,
                        write_policy_ids=_V1_LOCAL_OUTPUT_POLICIES,
                        golden_fixture_paths=_V1_LOCAL_OUTPUT_FIXTURES,
                        required_runtime_fields=_V1_LOCAL_OUTPUT_REQUIRED_FIELDS,
                        unsupported_boundary_ids=(
                            "append_mode",
                            "object_store_output_paths",
                            "table_catalog_writes",
                            "iceberg_delta_transactions",
                            "remote_uri_sinks",
                            "broad_nested_complex_sink_shapes",
                        ),
                        write_method_rows=tuple(_local_output_method(method) for method in _V1_LOCAL_OUTPUT_METHODS),
                        output_user_route_rows=tuple(_local_output_route(route_id) for route_id in _V1_LOCAL_OUTPUT_ROUTES),
                        all_write_methods_registered=True,
                        all_write_methods_no_fallback_no_external_engine=True,
                        all_output_routes_no_fallback_no_external_engine=True,
                        all_output_routes_emit_sink_evidence=True,
                        all_feature_gated_formats_labeled=True,
                        write_policy_contract_ready=True,
                        v1_scope_ready=True,
                        claim_gate_status="not_claim_grade",
                        performance_claim_allowed=False,
                        production_claim_allowed=False,
                        spark_replacement_claim_allowed=False,
                    )


                ShardLoomContext.local_output_sink_scope_report = (
                    _v1_local_output_sink_scope_report
                )
                '''
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
            (artifact / "dist" / "shardloom-0.1.0.dev0-py3-none-any.whl").write_text(
                "wheel\n", encoding="utf-8"
            )
            (artifact / "dist" / "shardloom-0.1.0.dev0.tar.gz").write_text(
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
                    "dist/shardloom-0.1.0.dev0-py3-none-any.whl",
                    "dist/shardloom-0.1.0.dev0.tar.gz",
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
            self.assertTrue(
                (
                    repo_root
                    / "python"
                    / "dist"
                    / "shardloom-0.1.0.dev0-py3-none-any.whl"
                ).is_file()
            )
            sdist = repo_root / "python" / "dist" / "shardloom-0.1.0.dev0.tar.gz"
            self.assertTrue(sdist.is_file())

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
                        output_path = Path(args[args.index("--output") + 1])
                        output_path.parent.mkdir(parents=True, exist_ok=True)
                        if "--generated-source-kind" in args:
                            output_path.write_text('{"id":1,"label":"alpha","batch_id":1}\\n', encoding="utf-8")
                            emit("run", [
                                {"key": "public_workflow_route_attached", "value": "true"},
                                {"key": "public_workflow_route_id", "value": "generated_user_rows_direct_output"},
                                {"key": "public_workflow_resolved_internal_command", "value": "generated-source-user-rows-smoke"},
                                {"key": "output_path", "value": str(output_path)},
                                {"key": "output_format", "value": "jsonl"},
                                {"key": "output_row_count", "value": "1"},
                                {"key": "output_io_performed", "value": "true"},
                                {"key": "generated_source_kind", "value": "user_rows"},
                                {"key": "generated_source_row_count", "value": "1"},
                                {"key": "generated_source_certificate_status", "value": "certified"},
                                {"key": "output_native_io_certificate_status", "value": "certified_local_jsonl_sink"},
                                {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                            ])
                        output_path.write_text('{"id":2,"label":"beta","amount":15}\\n', encoding="utf-8")
                        emit("run", [
                            {"key": "public_workflow_route_attached", "value": "true"},
                            {"key": "public_workflow_route_id", "value": "local_file_direct_sink"},
                            {"key": "public_workflow_resolved_internal_command", "value": "sql-local-source-smoke"},
                            {"key": "result_jsonl", "value": "{\\"id\\":2,\\"label\\":\\"beta\\",\\"amount\\":15}\\n"},
                            {"key": "source_format", "value": "csv"},
                            {"key": "execution_mode", "value": "batch"},
                            {"key": "operator_family", "value": "filter_project_limit"},
                            {"key": "output_path", "value": str(output_path)},
                            {"key": "output_format", "value": "jsonl"},
                            {"key": "output_row_count", "value": "1"},
                            {"key": "output_io_performed", "value": "true"},
                            {"key": "output_native_io_certificate_status", "value": "certified_local_jsonl_sink"},
                            {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                        ])
                    if args[0] == "sql-local-source-smoke":
                        output_path = Path(args[args.index("--output") + 1])
                        output_path.parent.mkdir(parents=True, exist_ok=True)
                        output_path.write_text('{"id":2,"label":"beta","amount":15}\\n', encoding="utf-8")
                        emit("sql-local-source-smoke", [
                            {"key": "result_jsonl", "value": "{\\"id\\":2,\\"label\\":\\"beta\\",\\"amount\\":15}\\n"},
                            {"key": "source_format", "value": "csv"},
                            {"key": "execution_mode", "value": "batch"},
                            {"key": "operator_family", "value": "filter_project_limit"},
                            {"key": "output_path", "value": str(output_path)},
                            {"key": "output_row_count", "value": "1"},
                            {"key": "output_io_performed", "value": "true"},
                            {"key": "output_native_io_certificate_status", "value": "certified_local_jsonl_sink"},
                            {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                        ])
                    if args[0] == "generated-source-user-rows-smoke":
                        output_path = Path(args[1])
                        output_path.parent.mkdir(parents=True, exist_ok=True)
                        output_path.write_text('{"id":1,"label":"alpha","batch_id":1}\\n', encoding="utf-8")
                        emit("generated-source-user-rows-smoke", [
                            {"key": "output_path", "value": str(output_path)},
                            {"key": "output_format", "value": "jsonl"},
                            {"key": "generated_source_kind", "value": "user_rows"},
                            {"key": "generated_source_row_count", "value": "1"},
                            {"key": "generated_source_certificate_status", "value": "certified"},
                            {"key": "output_native_io_certificate_status", "value": "certified_local_jsonl_sink"},
                            {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                        ])
                    if args[0] == "workflow-unsupported-plan":
                        emit("workflow-unsupported-plan", [
                            {"key": "blocker_id", "value": "cg21.workflow.to_pandas.decoded_dataframe_unsupported"},
                            {"key": "required_evidence", "value": "materialization_boundary,decode_evidence"},
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
            self.assertIn("quickstart_result_row_id=2", output)
            self.assertIn("quickstart_claim_gate_status=fixture_smoke_only", output)
            self.assertIn("quickstart_generated_source_row_count=1", output)
            self.assertIn(
                "quickstart_unsupported_blocker_id=cg21.workflow.to_pandas.decoded_dataframe_unsupported",
                output,
            )
            self.assertIn("quickstart_unsupported_external_engine_invoked=false", output)
            self.assertTrue(
                (repo_root / "target" / "local-python-smoke" / "orders-out.jsonl").exists()
            )

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
                            "quickstart_result_row_id=2",
                            "quickstart_output_row_count=1",
                            "quickstart_evidence_fallback_attempted=false",
                            "quickstart_claim_gate_status=fixture_smoke_only",
                            "quickstart_generated_source_row_count=1",
                            "quickstart_generated_claim_gate_status=fixture_smoke_only",
                            "quickstart_unsupported_blocker_id=cg21.workflow.to_pandas.decoded_dataframe_unsupported",
                            "quickstart_unsupported_runtime_execution=false",
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
            self.assertNotIn(str(repo_root), json.dumps(report, sort_keys=True))

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

    def test_release_dry_run_python_artifact_build_falls_back_to_pip_wheel(self) -> None:
        module = self._load_script_module(
            "release_dry_run_proof.py", "release_dry_run_proof_build_fallback_for_test"
        )
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            (repo_root / "python").mkdir()
            dist_dir = repo_root / "python" / "dist"
            dist_dir.mkdir()
            stale_wheel = dist_dir / "shardloom-0.0.0-py3-none-any.whl"
            stale_wheel.write_text("stale", encoding="utf-8")
            commands: list[list[str]] = []

            def fake_run_step(*, name, command, cwd, env=None):  # type: ignore[no-untyped-def]
                commands.append(command)
                if command[:3] == [sys.executable, "-m", "build"]:
                    return {
                        "name": name,
                        "command": command,
                        "returncode": 1,
                        "stdout": "",
                        "stderr": f"{sys.executable}: No module named build\n",
                    }
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
                step = module.build_python_artifacts(repo_root, dist_dir)
            finally:
                module.run_step = original_run_step

            self.assertEqual(step["returncode"], 0)
            self.assertEqual(step["build_backend"], "pip_wheel_no_build_isolation")
            self.assertEqual(step["fallback_reason"], "python_build_frontend_missing")
            self.assertFalse(stale_wheel.exists())
            self.assertEqual(commands[0], [sys.executable, "-m", "build", "python"])
            self.assertEqual(commands[1][:4], [sys.executable, "-m", "pip", "wheel"])
            self.assertIn("--no-build-isolation", commands[1])
            self.assertIn("--no-deps", commands[1])

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
                "docs/getting-started/install.md\n"
                "docs/getting-started/first-10-minutes.md\n"
                "scripts\\release_dry_run_proof.py\n"
                "package-channel evidence is still gated\n"
            ),
            "docs/getting-started/install.md": (
                "python scripts\\release_dry_run_proof.py --rows 64 --iterations 1\n"
                "pip --no-index\n"
                "SHARDLOOM_BIN\n"
            ),
            "docs/getting-started/first-10-minutes.md": (
                "python scripts\\release_dry_run_proof.py --rows 64 --iterations 1\n"
                "ctx.from_rows\nctx.read\nquickstart_result_row_id\nctx.range\n"
                "public package release\n"
            ),
            "docs/release/release-dry-run-proof.md": (
                "clean virtual environment\n"
                "local_python_user_surface_quickstart_performed=true\n"
                "generated_source_user_rows_smoke_performed=true\n"
                "prepared_native_benchmark_smoke_performed=true\n"
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
                "cli_prepared_vortex_batch_benchmark",
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
                for row_id in ["claim_production_readiness", "claim_package_publication"]
            ]
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
                "generated_source_user_rows_smoke_performed": True,
                "generated_source_range_smoke_performed": True,
                "prepared_native_benchmark_smoke_performed": True,
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
                "public_package_release_claim_allowed": False,
                "ready_channel_count": 0,
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
                "publication_human_approved": False,
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

    def test_production_usability_gate_accepts_precomputed_benchmark_report(self) -> None:
        module = self._load_script_module(
            "check_production_usability_gate.py",
            "check_production_usability_gate_benchmark_report_for_test",
        )
        manifest_ref = "website/assets/benchmarks/latest/manifest.json"
        summary, blockers = module.validate_benchmark_completeness_report(
            {
                "schema_version": "shardloom.benchmark_artifact_completeness_report.v1",
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
            "generated_source_user_rows_smoke_performed": True,
            "generated_source_range_smoke_performed": True,
            "steps": [
                {"name": name, "returncode": 0}
                for name in module.REQUIRED_DRY_RUN_STEPS
            ],
        }

    def test_python_user_surface_completion_gate_accepts_scoped_evidence(self) -> None:
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
        self.assertEqual(by_id["ctx_sql"]["status"], "scoped_runtime_row_present")
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
        self.assertEqual(by_method["from_rows"]["support_status"], "fixture_smoke_supported")
        self.assertEqual(
            by_method["to_pandas"]["support_status"],
            "optional_dependency_runtime_supported",
        )
        self.assertEqual(
            by_method["rename"]["support_status"],
            "fixture_smoke_supported",
        )
        self.assertIsNone(by_method["rename"]["diagnostic_operation"])
        self.assertIn(
            "declared_schema_projection_rewrite",
            by_method["rename"]["required_evidence"],
        )
        self.assertEqual(
            by_method["drop"]["support_status"],
            "fixture_smoke_supported",
        )
        self.assertIn(
            "projection_rewrite_semantics",
            by_method["drop"]["required_evidence"],
        )
        self.assertIn(
            "deterministic_seed_policy",
            by_method["sample"]["required_evidence"],
        )
        self.assertFalse(by_method["explode"]["runtime_execution"])
        self.assertEqual(
            by_method["merge"]["support_status"],
            "fixture_smoke_supported",
        )
        self.assertIn("join_operator_capability", by_method["merge"]["required_evidence"])
        self.assertEqual(
            by_method["concat"]["support_status"],
            "fixture_smoke_supported",
        )
        self.assertIn(
            "schema_alignment_contract",
            by_method["concat"]["required_evidence"],
        )
        self.assertEqual(
            by_method["nunique"]["support_status"],
            "fixture_smoke_supported",
        )
        self.assertIn("distinct_count_semantics", by_method["nunique"]["required_evidence"])
        self.assertEqual(
            by_method["value_counts"]["support_status"],
            "fixture_smoke_supported",
        )
        self.assertIn(
            "grouped_count_semantics",
            by_method["value_counts"]["required_evidence"],
        )
        self.assertEqual(
            by_method["fillna"]["support_status"],
            "fixture_smoke_supported",
        )
        self.assertIn("null_fill_semantics", by_method["fillna"]["required_evidence"])
        self.assertEqual(
            by_method["fill_null"]["support_status"],
            "fixture_smoke_supported",
        )
        self.assertEqual(
            by_method["isna"]["support_status"],
            "fixture_smoke_supported",
        )
        self.assertIn("null_mask_semantics", by_method["isna"]["required_evidence"])
        self.assertEqual(
            by_method["isnull"]["support_status"],
            "fixture_smoke_supported",
        )
        self.assertEqual(
            by_method["notna"]["support_status"],
            "fixture_smoke_supported",
        )
        self.assertIn("not_null_mask_semantics", by_method["notna"]["required_evidence"])
        self.assertEqual(
            by_method["notnull"]["support_status"],
            "fixture_smoke_supported",
        )
        self.assertEqual(
            by_method["pivot"]["support_status"],
            "deterministic_unsupported_diagnostic",
        )
        self.assertIn(
            "aggregate_reshape_semantics",
            by_method["pivot_table"]["required_evidence"],
        )
        self.assertIn("unpivot_semantics", by_method["melt"]["required_evidence"])
        self.assertFalse(by_method["rolling"]["write_io"])
        self.assertTrue(by_method["to_pandas"]["materialization_required"])
        self.assertIsNone(by_method["to_pandas"]["blocker_id"])
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

    def test_website_readiness_validates_benchmark_timing_surfaces(self) -> None:
        module = self._load_script_module(
            "check_website_readiness.py", "check_website_route_cards_for_test"
        )

        with tempfile.TemporaryDirectory() as tempdir:
            website = Path(tempdir) / "website"
            website.mkdir()
            timing_surface_tokens = "\n".join(
                f"<p>{token}</p>"
                for token in module.REQUIRED_BENCHMARK_TIMING_SURFACE_STRINGS
            )
            route_share_tokens = "\n".join(
                f"<p>{token}</p>"
                for token in module.REQUIRED_BENCHMARK_ROUTE_SHARE_STRINGS
            )
            stage_tokens = "\n".join(
                f"<p>{token}</p>"
                for token in module.REQUIRED_BENCHMARK_STAGE_STRINGS
            )
            runtime_tokens = "\n".join(
                f"<p>{token}</p>"
                for token in module.REQUIRED_BENCHMARK_RUNTIME_STRINGS
            )
            artifact_section_tokens = {
                "Prepared/native source-state coverage",
                "Raw timing tables",
            }
            artifact_tokens = "\n".join(
                f"<p>{token}</p>"
                for token in module.REQUIRED_BENCHMARK_ARTIFACT_STRINGS
                if token not in artifact_section_tokens
            )
            cards = {
                "cold_certified_route": "ShardLoom Cold Certified Route",
                "prepare_once_first_query": "ShardLoom Prepare-Once First Query",
                "prepare_once_batch": "ShardLoom Prepare-Once Batch",
                "warm_prepared_query": "ShardLoom Warm Prepared Query",
                "native_vortex_query": "ShardLoom Native Vortex Query",
                "external_baseline_end_to_end": "External Baseline End-to-End",
            }
            card_markup = "\n".join(
                f'<article data-route-card-id="{card_id}">{label}</article>'
                for card_id, label in cards.items()
            )
            public_front_door_markup = """
                <section>
                  <h2>Public front doors</h2>
                  <p>Route rows name the user-facing prepared paths.</p>
                  <article data-public-front-door-id="local_source_auto_prepare_vortex_front_door">
                    <code>ctx.prepare_vortex(&#39;fact.csv&#39;, dim=&#39;dim.csv&#39;, workspace=&#39;target/shardloom-prepared&#39;).query(&#39;selective filter&#39;).collect()</code>
                    <p>SourceState</p>
                    <p>result_sink</p>
                    <p>not_timing_row_route_identity_only</p>
                  </article>
                  <article data-public-front-door-id="generated_source_prepare_vortex_front_door">
                    <code>ctx.from_rows([{&#39;id&#39;: 1, &#39;label&#39;: &#39;alpha&#39;}]).prepare_vortex(workspace=&#39;target/shardloom-prepared&#39;)</code>
                    <p>GeneratedSourceState</p>
                    <p>VortexPreparedState</p>
                    <p>not_timing_row_route_identity_only</p>
                  </article>
                </section>
            """
            (website / "benchmarks.html").write_text(
                f"""
                <section data-route-timing-surface-dashboard>
                  <h2>Route timing dashboard</h2>
                  {card_markup}
                  <p>External Baseline End-to-End</p>
                  {timing_surface_tokens}
                </section>
                <section>
                  <h2>Publication proof</h2>
                </section>
                <section>
                  <h2>Optimization direction</h2>
                </section>
                <section>
                  <h2>Route-share attribution</h2>
                  {route_share_tokens}
                </section>
                <section>
                  <h2>Stage attribution</h2>
                  {stage_tokens}
                </section>
                <section>
                  <h2>Runtime and claims</h2>
                  {runtime_tokens}
                </section>
                {public_front_door_markup}
                <section>
                  <h2>Artifact lane availability</h2>
                  {artifact_tokens}
                </section>
                <section>
                  <h2>Prepared/native source-state coverage</h2>
                  <p>Prepared/native source-state coverage</p>
                </section>
                <section>
                  <h2>Raw timing tables</h2>
                  <p>Raw timing tables</p>
                </section>
                """,
                encoding="utf-8",
            )

            blockers: list[str] = []
            module.check_benchmark_timing_surface_dashboard(website, blockers)

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
