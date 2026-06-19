from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def load_gate_module():
    module_path = REPO_ROOT / "scripts" / "check_front_door_benchmark_publication.py"
    spec = importlib.util.spec_from_file_location(
        "check_front_door_benchmark_publication_for_test",
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


class FrontDoorBenchmarkPublicationTests(unittest.TestCase):
    def test_current_repo_gate_is_fail_closed_without_running_benchmarks(self) -> None:
        module = load_gate_module()

        report = module.build_report(
            REPO_ROOT,
            require_current_git=False,
            allow_dirty_worktree=True,
        )

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(
            report["front_door_performance_publication_status"],
            "blocked_pending_measured_equivalence_artifact",
        )
        self.assertEqual(report["claim_gate_status"], "not_claim_grade")
        self.assertFalse(report["front_door_performance_equivalence_claim_allowed"])
        self.assertFalse(report["performance_claim_allowed"])
        self.assertFalse(report["benchmark_run_performed"])
        self.assertFalse(report["benchmark_rerun_approved"])
        self.assertFalse(report["publication_attempted"])
        self.assertFalse(report["fallback_attempted"])
        self.assertFalse(report["external_engine_invoked"])
        self.assertTrue(report["scoped_local_front_door_parity_supported"])
        self.assertEqual(
            report["front_door_equivalence_constitution_status"],
            "local_constitution_ready",
        )
        self.assertEqual(report["front_door_equivalence_constitution_workload_count"], 9)
        self.assertIn(
            "front_door_lowering_overhead_millis",
            report["front_door_equivalence_constitution_timing_fields"],
        )
        self.assertIn(
            "native_vortex_unified_plan_contract",
            report["front_door_equivalence_constitution_evidence_fields"],
        )
        self.assertIn("performance_equivalence", report["parity_remaining_gap_row_ids"])
        self.assertEqual(report["public_front_door_benchmark_row_count"], 2)
        self.assertIn(
            "measured_sql_python_dataframe_front_door_rows",
            report["missing_claim_grade_evidence"],
        )
        self.assertTrue(report["publication_admission_blockers"])

    def test_gate_rejects_overclaimed_front_door_performance_equivalence(self) -> None:
        module = load_gate_module()
        parity_report = {
            "schema_version": "shardloom.sql_python_dataframe_parity_gate.v1",
            "status": "passed",
            "claim_gate_status": "claim_grade",
            "scoped_local_front_door_parity_supported": True,
            "all_no_fallback_no_external_engine": True,
            "flexible_anything_claim_allowed": False,
            "performance_equivalence_claim_allowed": True,
            "remaining_gap_row_ids": [],
            "rows": [
                {
                    "row_id": "performance_equivalence",
                    "runtime_gap_status": "admitted_scope",
                    "parity_status": "equivalent_admitted_scope",
                    "blocker_id": None,
                    "fallback_attempted": False,
                    "external_engine_invoked": False,
                }
            ],
            "blockers": [],
        }
        publication_claim_gate = {
            "schema_version": "shardloom.benchmark_publication_claim_gate.v1",
            "status": "passed",
            "public_front_door_benchmark_rows": {
                "schema_version": "shardloom.public_front_door_benchmark_rows.v1",
                "row_count": 2,
                "front_door_ids": [
                    "local_source_auto_prepare_vortex_front_door",
                    "generated_source_prepare_vortex_front_door",
                ],
                "missing_front_door_ids": [],
                "invalid_example_count": 0,
            },
            "blockers": [],
        }

        report = module.build_report(
            REPO_ROOT,
            parity_report=parity_report,
            publication_claim_gate=publication_claim_gate,
        )

        self.assertEqual(report["status"], "blocked")
        self.assertTrue(
            any(
                "performance_equivalence_claim_allowed" in blocker
                for blocker in report["blockers"]
            )
        )
        self.assertTrue(
            any("claim_gate_status" in blocker for blocker in report["blockers"])
        )
        self.assertTrue(
            any("benchmark_publication_pending" in blocker for blocker in report["blockers"])
        )


if __name__ == "__main__":
    unittest.main()
