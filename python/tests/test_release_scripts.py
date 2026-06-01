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
                "total_route_ms = query_runtime_millis",
                "warm_prepared_query_only",
                "query_runtime_millis",
            ),
            "shardloom-prepare-batch": (
                "prepare_once_batch",
                "ShardLoom Prepare-Once Batch",
                "raw_compat_source",
                True,
                "total_route_ms = amortized_prepare_batch_preparation_millis + query_runtime_millis",
                "prepare_once_batch_amortized",
                "amortized_prepare_batch_preparation_millis,query_runtime_millis",
            ),
            "shardloom-vortex": (
                "native_vortex_query",
                "ShardLoom Native Vortex Query",
                "Vortex",
                False,
                "total_route_ms = query_runtime_millis",
                "native_vortex_query_only",
                "query_runtime_millis",
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
            "route_total_formula": formula,
            "route_timing_scope": timing_scope,
            "stage_parent_id": lane_id,
            "route_timing_included_stage_ids": included_stage_ids,
            "route_timing_excluded_stage_ids": "none",
            "route_timing_included_stage_total_ms": 1.0,
            "route_timing_total_delta_ms": 0.0,
            "preparation_timing_included_in_total": preparation_included,
            "query_timing_included_in_total": True,
            "output_timing_included_in_total": lane_id in {"cold_certified_route"},
            "evidence_timing_included_in_total": lane_id in {"cold_certified_route"},
            "fast_path_attribution_schema_version": "shardloom.route_fast_path_attribution.v1",
            "runtime_execution_ms": 0.8,
            "output_delivery_ms": 0.1,
            "evidence_capture_ms": 0.0,
            "evidence_render_ms": 0.1,
            "certificate_link_ms": 0.0,
            "runtime_execution_timing_scope": timing_scope,
            "output_delivery_timing_scope": (
                "included_in_route_total"
                if lane_id in {"cold_certified_route"}
                else "excluded_from_route_total"
            ),
            "evidence_capture_timing_status": "certificate_metadata_linked_not_separately_timed",
            "certificate_link_timing_status": "metadata_linked_not_separately_timed",
            "runtime_execution_certificate_id": "execution://fixture",
            "runtime_execution_certificate_status": "certified",
            "runtime_execution_certificate_plan_ref": "scheduler://fixture",
            "certificate_link_status": "linked_certified_runtime_execution",
            "evidence_required_for_claim": True,
            "evidence_render_included_in_route_total": lane_id in {"cold_certified_route"},
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
            "route_total_formula": "total_route_ms = external engine reported total_runtime_millis",
            "route_timing_scope": "external_baseline_end_to_end",
            "stage_parent_id": "external_baseline_end_to_end",
            "route_timing_included_stage_ids": "external_engine_reported_total_runtime_millis",
            "route_timing_excluded_stage_ids": "none",
            "route_timing_included_stage_total_ms": 1.0,
            "route_timing_total_delta_ms": 0.0,
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
            "metrics": {
                "persistent_runner_status": "single_process_batch_runner_supported",
                "vortex_scan_millis": 0.2,
                "query_runtime_millis": 1.0,
                "operator_compute_millis": 0.5,
                "evidence_render_millis": 0.1,
                "cli_process_wall_millis": 2.0,
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
                "source_read_millis": 10.0,
                "compatibility_parse_millis": 12.0,
                "compatibility_to_vortex_import_millis": 95.0,
                "vortex_array_build_millis": 20.0,
                "vortex_write_millis": 70.0,
                "vortex_digest_micros": 1500.0,
                "vortex_reopen_verify_millis": 5.0,
                "vortex_scan_millis": 1.0,
                "operator_compute_millis": 2.0,
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
        self.assertEqual(published["total_route_ms"], 130.0)
        self.assertFalse(published["performance_claim_allowed"])

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
                "persistent_runner_status": "single_process_batch_runner_supported",
                "prepare_batch_preparation_millis": 100.0,
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

        self.assertFalse(blockers)
        self.assertEqual(
            report["shardloom_row_count"],
            len(module.REQUIRED_SHARDLOOM_PUBLICATION_ENGINES)
            * len(module.REQUIRED_PUBLICATION_FORMATS),
        )
        self.assertEqual(report["missing_capillary_activation_row_count"], 0)
        self.assertEqual(report["missing_shardloom_engine_format_cell_count"], 0)
        self.assertEqual(report["shardloom_runtime_validation_counts"], {"passed": 24})
        self.assertEqual(report["missing_independent_claim_proof_row_count"], 0)

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
        self.assertIn("GAR-RUNTIME-IMPL", packet["next_implementation_slice"])
        self.assertIn("performance superiority", packet["forbidden_claims"])

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
                self._dependabot_pr(
                    979,
                    "Bump vortex from 0.72.0 to 0.73.0 in the vortex-upstream group",
                ),
                self._dependabot_pr(980, "Bump rusqlite from 0.37.0 to 0.40.0"),
            ],
            "open_dependabot_pr_count": 2,
            "admitted_open_dependabot_prs": [979, 980],
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

    def test_pre_5j_dependency_freshness_accepts_current_dependabot_prs(self) -> None:
        module = self._load_script_module(
            "check_pre_5j_dependency_freshness.py",
            "check_pre_5j_dependency_freshness_for_test",
        )
        report = module.build_report(
            repo_root=REPO_ROOT,
            open_prs=[
                self._dependabot_pr(
                    979,
                    "Bump vortex from 0.72.0 to 0.73.0 in the vortex-upstream group",
                ),
                self._dependabot_pr(980, "Bump rusqlite from 0.37.0 to 0.40.0"),
            ],
            open_prs_status="loaded_from_file",
            require_live_github=True,
        )

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(report["admitted_open_dependabot_prs"], [979, 980])
        self.assertTrue(report["benchmark_refresh_allowed"])
        self.assertFalse(report["benchmark_run_performed"])
        self.assertFalse(report["fallback_attempted"])
        self.assertFalse(report["external_engine_invoked"])

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
                'rusqlite = { version = "0.40.0", default-features = false, features = ["bundled"] }\n',
                encoding="utf-8",
            )
            vortex_manifest = root / "shardloom-vortex" / "Cargo.toml"
            vortex_manifest.parent.mkdir(parents=True)
            vortex_manifest.write_text(
                "[dependencies]\n"
                'vortex = { version = "0.73", optional = true }\n',
                encoding="utf-8",
            )
            (root / "Cargo.lock").write_text(
                "[[package]]\n"
                'name = "vortex"\n'
                'version = "0.73.0"\n'
                "\n"
                "[[package]]\n"
                'name = "rusqlite"\n'
                'version = "0.40.0"\n'
                "\n"
                "[[package]]\n"
                'name = "libsqlite3-sys"\n'
                'version = "0.38.0"\n',
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
            {"version": "0.40.0", "default-features": False, "features": ["bundled"]},
        )
        self.assertEqual(vortex, {"version": "0.73", "optional": True})
        self.assertEqual(lock_versions["vortex"], "0.73.0")
        self.assertEqual(lock_versions["rusqlite"], "0.40.0")
        self.assertEqual(lock_versions["libsqlite3-sys"], "0.38.0")

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
                self._dependabot_pr(979, "Bump vortex"),
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
            )

            report = json.loads(transcript.read_text(encoding="utf-8"))
            self.assertTrue(report["local_python_user_surface_quickstart_performed"])
            self.assertTrue(report["local_python_result_and_evidence_printed"])
            self.assertTrue(report["local_python_unsupported_path_evidence_printed"])

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
                runs_today_matrix_ref="docs/status/runs-today-support-matrix.json",
                dry_run=payloads["dry_run"],
                package_report=payloads["package_report"],
                release_security=payloads["release_security"],
                contribution_governance=payloads["contribution_governance"],
                final_rehearsal=payloads["final_rehearsal"],
                website_report=payloads["website_report"],
                benchmark_manifest_path=REPO_ROOT / "website" / "assets" / "benchmarks" / "latest" / "manifest.json",
                runs_today=payloads["runs_today"],
            )

            self.assertEqual(report["status"], "passed", report["blockers"])
            self.assertEqual(report["claim_gate_status"], "not_claim_grade")
            self.assertFalse(report["public_release_claim_allowed"])
            self.assertFalse(report["public_package_claim_allowed"])
            self.assertIn("GAR-RUNTIME-IMPL-4S", report["covered_phase_items"])

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
        self.assertEqual(by_method["filter"]["support_status"], "lazy_plan_supported")
        self.assertEqual(by_method["from_rows"]["support_status"], "fixture_smoke_supported")
        self.assertEqual(
            by_method["to_pandas"]["support_status"],
            "optional_dependency_runtime_supported",
        )
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

    def test_website_readiness_validates_benchmark_route_cards(self) -> None:
        module = self._load_script_module(
            "check_website_readiness.py", "check_website_route_cards_for_test"
        )

        with tempfile.TemporaryDirectory() as tempdir:
            website = Path(tempdir) / "website"
            website.mkdir()
            runtime_badges = "".join(
                f'<span class="status-chip">{status}</span>'
                for status in module.REQUIRED_BENCHMARK_RUNTIME_BADGES
            )
            evidence_badges = "".join(
                f'<span class="status-chip">{status}</span>'
                for status in module.REQUIRED_BENCHMARK_EVIDENCE_BADGES
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
                f'<article data-route-card-id="{card_id}" data-route-view="end-to-end prepared-state native-vortex diagnostic-stage" data-route-card-e2e-comparable="{str(card_id != "warm_prepared_query").lower()}">{label}</article>'
                for card_id, label in cards.items()
            )
            (website / "benchmarks.html").write_text(
                f"""
                <section data-route-card-dashboard>
                  {card_markup}
                  <p>Not comparable to raw-source external end-to-end baselines.</p>
                  <p>External rows are baseline context only.</p>
                  <div data-route-badge-fixture>{runtime_badges}{evidence_badges}</div>
                </section>
                <h2>Stage attribution</h2>
                <section>
                  <h2>Runtime fast path</h2>
                  <p>Runtime timing is separate from output and evidence rendering.</p>
                  <table>
                    <caption>Runtime Fast Path Versus Evidence Path</caption>
                    <tr><th>Certificate status</th></tr>
                    <tr><td>linked_certified_runtime_execution</td></tr>
                  </table>
                  <p>shardloom.route_fast_path_attribution.v1</p>
                </section>
                <section>
                  <h2>Operator mode inventory</h2>
                  <p>Runtime support is not encoded-native support.</p>
                  <table>
                    <caption>Operator Mode Inventory</caption>
                    <tr><th>Execution mode</th><th>Rows</th></tr>
                    <tr><td>residual_native</td><td>1</td></tr>
                  </table>
                  <table>
                    <caption>Operator Hot-Path Promotion Candidates</caption>
                    <tr><th>Candidate</th><th>Status</th></tr>
                    <tr>
                      <td>selective_filter_selection_vector_metric_aggregation</td>
                      <td>blocked_selection_vector_metric_aggregation_not_admitted</td>
                    </tr>
                  </table>
                  <p>shardloom.operator_mode_inventory.v1</p>
                </section>
                <h2>Raw timing tables</h2>
                """,
                encoding="utf-8",
            )

            blockers: list[str] = []
            module.check_benchmark_route_card_dashboard(website, blockers)

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
