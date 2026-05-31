from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace


REPO_ROOT = Path(__file__).resolve().parents[2]


def load_parity_module():
    module_path = REPO_ROOT / "scripts" / "check_sql_python_dataframe_parity.py"
    spec = importlib.util.spec_from_file_location(
        "check_sql_python_dataframe_parity_for_test",
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


class SqlPythonDataFrameParityTests(unittest.TestCase):
    def test_current_repo_parity_gate_is_honest_about_scope_and_gaps(self) -> None:
        module = load_parity_module()

        report = module.build_report(REPO_ROOT)

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertTrue(report["scoped_local_front_door_parity_supported"])
        self.assertFalse(report["flexible_anything_claim_allowed"])
        self.assertFalse(report["performance_equivalence_claim_allowed"])
        self.assertEqual(report["admitted_row_count"], 4)
        self.assertGreaterEqual(report["remaining_gap_count"], 5)
        self.assertIn(
            "arbitrary_sql_python_dataframe_breadth",
            report["remaining_gap_row_ids"],
        )
        self.assertIn("performance_equivalence", report["remaining_gap_row_ids"])
        local = next(
            row
            for row in report["rows"]
            if row["row_id"] == "local_file_filter_project_limit"
        )
        self.assertEqual(local["shared_runtime_path"], "sql-local-source-smoke")
        self.assertIn("no_benchmark_claim", local["performance_equivalence_status"])
        schema_quality = next(
            row
            for row in report["rows"]
            if row["row_id"] == "schema_quality_preview"
        )
        self.assertEqual(schema_quality["parity_status"], "equivalent_admitted_scope")
        self.assertIn("ctx.sql", schema_quality["sql_surface"])
        self.assertIsNone(schema_quality["blocker_id"])

    def test_parity_validator_rejects_overclaimed_or_fallback_rows(self) -> None:
        module = load_parity_module()
        rows = [
            SimpleNamespace(
                row_id=row_id,
                workflow=row_id,
                support_status="scoped_runtime_supported",
                sql_surface="sql",
                python_surface="python",
                dataframe_surface="dataframe",
                shared_runtime_path="sql-local-source-smoke",
                parity_status="equivalent_admitted_scope",
                performance_equivalence_status="same_runtime_path_no_benchmark_claim",
                runtime_execution=True,
                data_read=True,
                write_io=True,
                materialization_required=False,
                fallback_attempted=False,
                external_engine_invoked=False,
                blocker_id=None,
                required_evidence=("evidence",),
                claim_boundary="bounded",
            )
            for row_id in module.REQUIRED_ADMITTED_ROWS
        ]
        rows.append(
            SimpleNamespace(
                row_id="arbitrary_sql_python_dataframe_breadth",
                workflow="broad",
                support_status="blocked",
                sql_surface="sql",
                python_surface="python",
                dataframe_surface="dataframe",
                shared_runtime_path="unsupported",
                parity_status="equivalent_admitted_scope",
                performance_equivalence_status="claim_grade",
                runtime_execution=True,
                data_read=False,
                write_io=False,
                materialization_required=False,
                fallback_attempted=True,
                external_engine_invoked=False,
                blocker_id=None,
                required_evidence=("evidence",),
                claim_boundary="bad",
            )
        )
        matrix = SimpleNamespace(
            rows=tuple(rows),
            flexible_anything_claim_allowed=True,
            performance_equivalence_claim_allowed=True,
            scoped_local_front_door_parity_supported=True,
            all_no_fallback_no_external_engine=False,
        )

        _, blockers = module.validate_matrix(matrix)

        self.assertTrue(any("fallback_attempted must be false" in b for b in blockers))
        self.assertTrue(any("gap row must be front_door_gap" in b for b in blockers))
        self.assertTrue(any("flexible_anything_claim_allowed" in b for b in blockers))
        self.assertTrue(any("performance_equivalence_claim_allowed" in b for b in blockers))


if __name__ == "__main__":
    unittest.main()
