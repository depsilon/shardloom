from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def load_completion_gate_module():
    module_path = REPO_ROOT / "scripts" / "check_compute_engine_completion_gate.py"
    spec = importlib.util.spec_from_file_location(
        "check_compute_engine_completion_gate_for_test",
        module_path,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    script_dir = str(module_path.parent)
    original_path = list(sys.path)
    sys.path[:] = [entry for entry in sys.path if entry != script_dir]
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path[:] = original_path
        sys.modules.pop(spec.name, None)
    return module


class ComputeEngineCompletionGateTests(unittest.TestCase):
    def test_completion_gate_passes_clean_evidence(self) -> None:
        module = load_completion_gate_module()

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            benchmark = root / "benchmark-results.json"
            phase_plan = root / "phased-execution-plan.md"
            global_review = root / "global-architecture-review.md"
            benchmark.write_text(
                json.dumps(
                    {
                        "published_benchmark_rows": [
                            {
                                "engine": "shardloom",
                                "storage_format": "csv",
                                "scenario_id": "selective_filter",
                                "status": "success",
                                "claim_gate_status": "claim_grade",
                                "runtime_execution_validation_status": "passed",
                                "fallback_attempted": False,
                                "external_engine_invoked": False,
                                "runtime_fallback_attempted": False,
                                "runtime_external_query_engine_invoked": False,
                                "optimizer_rule_unsupported_count": 0,
                                "source_state_status": "source_state_reuse_supported",
                                "prepared_state_status": "prepared_state_reuse_supported",
                            },
                            {
                                "engine": "datafusion",
                                "storage_format": "jsonl",
                                "scenario_id": "nested_json_field_scan",
                                "status": "unsupported",
                                "claim_gate_status": "unsupported",
                                "external_baseline_only": True,
                                "fallback_attempted": False,
                                "external_engine_invoked": False,
                                "claim_grade_missing_evidence": [
                                    "DataFusion Python SQL has no JSON extraction function in this profile"
                                ],
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            phase_plan.write_text("- [x] completed item\n", encoding="utf-8")
            global_review.write_text("- [x] completed review item\n", encoding="utf-8")

            report = module.build_report(
                benchmark_results=benchmark,
                phase_plan=phase_plan,
                global_review=global_review,
            )

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertTrue(report["completion_claim_allowed"])
        self.assertEqual(report["benchmark_gap_report"]["residual_blocker_count"], 0)
        external_report = report["benchmark_gap_report"]["external_baseline_unsupported_report"]
        self.assertEqual(external_report["unsupported_row_count"], 1)
        self.assertEqual(external_report["classification_blocker_count"], 0)

    def test_completion_gate_allows_hot_runtime_non_claim_grade_rows(self) -> None:
        module = load_completion_gate_module()

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            benchmark = root / "benchmark-results.json"
            phase_plan = root / "phased-execution-plan.md"
            global_review = root / "global-architecture-review.md"
            benchmark.write_text(
                json.dumps(
                    {
                        "published_benchmark_rows": [
                            {
                                "engine": "shardloom-vortex",
                                "storage_format": "csv",
                                "scenario_id": "selective_filter",
                                "timing_surface": "hot_runtime",
                                "actual_evidence_tier": "metadata_sink",
                                "status": "success",
                                "claim_gate_status": "not_claim_grade",
                                "runtime_execution_validation_status": "passed",
                                "fallback_attempted": False,
                                "external_engine_invoked": False,
                                "certificate_link_status": "not_required_not_claim_grade",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            phase_plan.write_text("- [x] completed item\n", encoding="utf-8")
            global_review.write_text("- [x] completed review item\n", encoding="utf-8")

            report = module.build_report(
                benchmark_results=benchmark,
                phase_plan=phase_plan,
                global_review=global_review,
            )

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(report["benchmark_gap_report"]["top_level_blocker_count"], 0)
        self.assertEqual(report["benchmark_gap_report"]["residual_blocker_count"], 0)

    def test_completion_gate_still_requires_publication_proof_claim_grade(self) -> None:
        module = load_completion_gate_module()

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            benchmark = root / "benchmark-results.json"
            phase_plan = root / "phased-execution-plan.md"
            global_review = root / "global-architecture-review.md"
            benchmark.write_text(
                json.dumps(
                    {
                        "published_benchmark_rows": [
                            {
                                "engine": "shardloom-vortex",
                                "storage_format": "csv",
                                "scenario_id": "selective_filter",
                                "timing_surface": "publication_proof",
                                "actual_evidence_tier": "publication_full",
                                "status": "success",
                                "claim_gate_status": "not_claim_grade",
                                "runtime_execution_validation_status": "passed",
                                "fallback_attempted": False,
                                "external_engine_invoked": False,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            phase_plan.write_text("- [x] completed item\n", encoding="utf-8")
            global_review.write_text("- [x] completed review item\n", encoding="utf-8")

            report = module.build_report(
                benchmark_results=benchmark,
                phase_plan=phase_plan,
                global_review=global_review,
            )

        self.assertEqual(report["status"], "blocked")
        self.assertEqual(report["benchmark_gap_report"]["top_level_blocker_count"], 1)
        self.assertEqual(
            report["benchmark_gap_report"]["top_level_blocker_examples"][0]["field"],
            "claim_gate_status",
        )

    def test_completion_gate_classifies_optimization_statuses_separately(self) -> None:
        module = load_completion_gate_module()

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            benchmark = root / "benchmark-results.json"
            phase_plan = root / "phased-execution-plan.md"
            global_review = root / "global-architecture-review.md"
            benchmark.write_text(
                json.dumps(
                    {
                        "published_benchmark_rows": [
                            {
                                "engine": "shardloom-vortex",
                                "storage_format": "csv",
                                "scenario_id": "selective_filter",
                                "timing_surface": "publication_proof",
                                "actual_evidence_tier": "publication_full",
                                "status": "success",
                                "claim_gate_status": "claim_grade",
                                "runtime_execution_validation_status": "passed",
                                "fallback_attempted": False,
                                "external_engine_invoked": False,
                                "operator_hot_path_candidate_status": (
                                    "blocked_selection_vector_metric_aggregation_not_admitted"
                                ),
                                "source_read_scout_reuse_status": (
                                    "blocked_until_scout_timing_split"
                                ),
                                "source_read_scout_timing_split_status": (
                                    "blocked_missing_source_read_scout_split"
                                ),
                                "vortex_reopen_verify_split_status": (
                                    "blocked_missing_reopen_verify_split"
                                ),
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            phase_plan.write_text("- [x] completed item\n", encoding="utf-8")
            global_review.write_text("- [x] completed review item\n", encoding="utf-8")

            report = module.build_report(
                benchmark_results=benchmark,
                phase_plan=phase_plan,
                global_review=global_review,
            )

        self.assertEqual(report["status"], "passed", report["blockers"])
        benchmark_report = report["benchmark_gap_report"]
        self.assertEqual(benchmark_report["residual_blocker_count"], 0)
        self.assertEqual(benchmark_report["optimization_claim_blocker_count"], 4)
        self.assertEqual(
            benchmark_report["optimization_claim_blocker_field_counts"],
            {
                "operator_hot_path_candidate_status": 1,
                "source_read_scout_reuse_status": 1,
                "source_read_scout_timing_split_status": 1,
                "vortex_reopen_verify_split_status": 1,
            },
        )

    def test_completion_gate_accepts_mapped_global_review_claim_boundaries(self) -> None:
        module = load_completion_gate_module()

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            benchmark = root / "benchmark-results.json"
            phase_plan = root / "phased-execution-plan.md"
            global_review = root / "global-architecture-review.md"
            benchmark.write_text(
                json.dumps(
                    {
                        "published_benchmark_rows": [
                            {
                                "engine": "shardloom-vortex",
                                "storage_format": "csv",
                                "scenario_id": "selective_filter",
                                "timing_surface": "publication_proof",
                                "actual_evidence_tier": "publication_full",
                                "status": "success",
                                "claim_gate_status": "claim_grade",
                                "runtime_execution_validation_status": "passed",
                                "fallback_attempted": False,
                                "external_engine_invoked": False,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            phase_plan.write_text("- [x] completed item\n", encoding="utf-8")
            global_review.write_text("- [ ] broad claim boundary row\n", encoding="utf-8")

            report = module.build_report(
                benchmark_results=benchmark,
                phase_plan=phase_plan,
                global_review=global_review,
                runtime_gap_family_burn_down_report={
                    "schema_version": "shardloom.runtime_gap_family_burn_down.v1",
                    "status": "passed",
                    "blockers": [],
                    "global_review_unchecked_count": 1,
                    "mapped_gap_count": 1,
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
                },
            )

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(
            report["global_review_mapping_status"],
            "mapped_to_runtime_gap_family_claim_boundaries",
        )
        self.assertFalse(report["global_review_unchecked_rows_block_completion"])

    def test_completion_gate_blocks_unchecked_items_and_residual_engine_status(self) -> None:
        module = load_completion_gate_module()

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            benchmark = root / "benchmark-results.json"
            phase_plan = root / "phased-execution-plan.md"
            global_review = root / "global-architecture-review.md"
            benchmark.write_text(
                json.dumps(
                    {
                        "published_benchmark_rows": [
                            {
                                "engine": "shardloom-prepared-vortex",
                                "storage_format": "parquet",
                                "scenario_id": "group_by_aggregation",
                                "status": "success",
                                "claim_gate_status": "claim_grade",
                                "runtime_execution_validation_status": "passed",
                                "fallback_attempted": False,
                                "external_engine_invoked": False,
                                "optimizer_rule_unsupported_count": 2,
                                "source_state_status": "report_only",
                                "vortex_copy_budget_buffer_reuse_status": (
                                    "blocked_until_correctness_parity"
                                ),
                            },
                            {
                                "engine": "datafusion",
                                "storage_format": "jsonl",
                                "scenario_id": "nested_json_field_scan",
                                "status": "unsupported",
                                "external_baseline_only": False,
                                "fallback_attempted": False,
                                "external_engine_invoked": True,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            phase_plan.write_text("- [ ] runtime item\n", encoding="utf-8")
            global_review.write_text("- [ ] broad engine item\n", encoding="utf-8")

            report = module.build_report(
                benchmark_results=benchmark,
                phase_plan=phase_plan,
                global_review=global_review,
            )

        self.assertEqual(report["status"], "blocked")
        self.assertFalse(report["completion_claim_allowed"])
        self.assertEqual(report["phase_plan_unchecked_count"], 1)
        self.assertEqual(report["global_review_unchecked_count"], 1)
        field_counts = report["benchmark_gap_report"]["residual_blocker_field_counts"]
        self.assertEqual(field_counts["optimizer_rule_unsupported_count"], 1)
        self.assertEqual(field_counts["source_state_status"], 1)
        self.assertEqual(field_counts["vortex_copy_budget_buffer_reuse_status"], 1)
        external_report = report["benchmark_gap_report"]["external_baseline_unsupported_report"]
        self.assertEqual(external_report["unsupported_row_count"], 1)
        external_fields = external_report["classification_blocker_field_counts"]
        self.assertEqual(external_fields["external_baseline_only"], 1)
        self.assertEqual(external_fields["external_engine_invoked"], 1)
        self.assertEqual(external_fields["unsupported_reason"], 1)


if __name__ == "__main__":
    unittest.main()
