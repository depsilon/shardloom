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
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    finally:
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
