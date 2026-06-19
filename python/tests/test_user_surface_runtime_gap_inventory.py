from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def load_inventory_module():
    module_path = REPO_ROOT / "scripts" / "check_user_surface_runtime_gap_inventory.py"
    spec = importlib.util.spec_from_file_location(
        "check_user_surface_runtime_gap_inventory_for_test",
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


class UserSurfaceRuntimeGapInventoryTests(unittest.TestCase):
    def test_current_repo_inventory_classifies_gaps_and_external_baselines(self) -> None:
        module = load_inventory_module()

        report = module.build_report(
            repo_root=REPO_ROOT,
            benchmark_results=REPO_ROOT
            / "website"
            / "assets"
            / "benchmarks"
            / "latest"
            / "benchmark-results.json",
            runs_today_matrix=REPO_ROOT / "docs" / "status" / "runs-today-support-matrix.json",
            website_status_dir=REPO_ROOT / "website-src" / "src" / "content" / "status",
        )

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertGreaterEqual(report["inventory_row_count"], 20)
        self.assertTrue(report["acceptance_summary"]["all_inventory_rows_classified"])
        self.assertTrue(
            report["acceptance_summary"]["all_inventory_rows_no_fallback_no_external_engine"]
        )
        self.assertEqual(
            report["benchmark_support_summary"]["shardloom_unsupported_row_count"],
            0,
        )
        self.assertEqual(
            report["benchmark_support_summary"]["external_baseline_unsupported_row_count"],
            6,
        )
        self.assertEqual(
            report["benchmark_support_summary"]["external_baseline_classification_blockers"],
            [],
        )

        by_id = {
            (row["source"], row["row_id"]): row
            for row in report["inventory_rows"]
        }
        self.assertNotIn(
            ("front_door_parity_matrix", "native_vortex_general_runtime"),
            by_id,
        )

        performance = by_id[("front_door_parity_matrix", "performance_equivalence")]
        self.assertEqual(
            performance["classification"],
            "runtime_available_needs_claim_evidence",
        )
        self.assertEqual(performance["observed_status"], "benchmark_publication_pending")
        self.assertIn("benchmark", performance["output_or_evidence_route"])

    def test_inventory_validator_rejects_unclassified_and_fallback_rows(self) -> None:
        module = load_inventory_module()
        bad_row = module.common_row(
            source="unit",
            row_id="bad",
            surface="bad unsupported route",
            observed_status="unsupported",
            classification="unknown",
            vortex_normalization_point="",
            runtime_route="",
            output_or_evidence_route="",
            owner="",
            blocker_id="bad.blocker",
            claim_gate_status="claim_grade",
            fallback_attempted=True,
            external_engine_invoked=True,
            runtime_execution=False,
            benchmark_range=True,
            user_visible_refs=["unit"],
            required_evidence=[],
            claim_boundary="",
        )
        benchmark = {
            "shardloom_unsupported_row_count": 1,
            "external_baseline_classification_blockers": [{"row": "external"}],
        }

        blockers = module.validate_inventory([bad_row], benchmark)

        self.assertTrue(any("invalid classification" in blocker for blocker in blockers))
        self.assertTrue(any("missing vortex_normalization_point" in blocker for blocker in blockers))
        self.assertTrue(any("fallback_attempted=false" in blocker for blocker in blockers))
        self.assertTrue(any("external_engine_invoked=false" in blocker for blocker in blockers))
        self.assertTrue(any("must not claim claim_grade" in blocker for blocker in blockers))
        self.assertTrue(any("zero ShardLoom unsupported rows" in blocker for blocker in blockers))
        self.assertTrue(
            any("external benchmark unsupported rows" in blocker for blocker in blockers)
        )


if __name__ == "__main__":
    unittest.main()
