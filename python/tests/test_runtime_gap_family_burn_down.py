from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def load_burn_down_module():
    module_path = REPO_ROOT / "scripts" / "check_runtime_gap_family_burn_down.py"
    spec = importlib.util.spec_from_file_location(
        "check_runtime_gap_family_burn_down_for_test",
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


class RuntimeGapFamilyBurnDownTests(unittest.TestCase):
    def test_current_repo_maps_every_unchecked_global_review_gap(self) -> None:
        module = load_burn_down_module()

        report = module.build_report(REPO_ROOT)

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertGreater(report["global_review_unchecked_count"], 0)
        self.assertEqual(
            report["mapped_gap_count"],
            report["global_review_unchecked_count"],
        )
        self.assertGreaterEqual(report["runtime_gap_family_count"], 10)
        self.assertTrue(
            report["acceptance_summary"]["all_unchecked_global_review_rows_mapped"]
        )
        self.assertTrue(report["acceptance_summary"]["all_families_have_phase_items"])
        self.assertTrue(
            report["acceptance_summary"]["all_families_have_active_phase_owner"]
        )
        self.assertTrue(
            report["acceptance_summary"]["all_families_have_evidence_and_validators"]
        )
        self.assertFalse(report["fallback_attempted"])
        self.assertFalse(report["external_engine_invoked"])
        self.assertEqual(report["claim_gate_status"], "not_claim_grade")

    def test_validator_rejects_unmapped_global_review_gap(self) -> None:
        module = load_burn_down_module()
        mappings = module.GLOBAL_REVIEW_MAPPINGS[:-1]

        report = module.build_report(REPO_ROOT, mappings=mappings)

        self.assertEqual(report["status"], "blocked")
        self.assertTrue(
            any(
                "unchecked global-review rows lack burn-down family" in blocker
                for blocker in report["blockers"]
            )
        )

    def test_validator_rejects_completed_ledger_only_phase_ownership(self) -> None:
        module = load_burn_down_module()
        original_read_text = module.read_text

        def read_text_without_active_owner(path: Path) -> str:
            text = original_read_text(path)
            if path.name == "phased-execution-plan.md":
                return text.replace(
                    module.ACTIVE_GLOBAL_GAP_PHASE_OWNER,
                    "REMOVED-ACTIVE-GAP-OWNER",
                )
            return text

        module.read_text = read_text_without_active_owner
        try:
            report = module.build_report(REPO_ROOT)
        finally:
            module.read_text = original_read_text

        self.assertEqual(report["status"], "blocked")
        self.assertFalse(
            report["acceptance_summary"]["all_families_have_active_phase_owner"]
        )
        self.assertTrue(
            any(
                "unchecked gap family lacks active phase owner" in blocker
                for blocker in report["blockers"]
            )
        )


if __name__ == "__main__":
    unittest.main()
