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
        self.assertTrue(report["v1_scope_ready"])
        self.assertEqual(
            report["v1_scope_document"],
            "docs/architecture/v1-front-door-runtime-scope.md",
        )
        self.assertIn("local_file_filter_project_limit", report["v1_supported_row_ids"])
        self.assertIn("performance_equivalence", report["v1_pending_row_ids"])
        self.assertIn("selective_filter", report["v1_example_scenario_ids"])
        self.assertEqual(report["v1_expected_error_scenario_ids"], [])
        self.assertFalse(report["flexible_anything_claim_allowed"])
        self.assertFalse(report["performance_equivalence_claim_allowed"])
        self.assertEqual(report["admitted_row_count"], 6)
        self.assertGreaterEqual(report["remaining_gap_count"], 4)
        self.assertTrue(report["all_broad_gaps_have_precise_runtime_status"])
        self.assertIn(
            "benchmark_publication_pending",
            report["runtime_gap_status_vocabulary"],
        )
        self.assertIn(
            "Vortex-backed runtime path",
            report["vortex_normalization_contract"],
        )
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
        self.assertEqual(
            local["shared_runtime_path"],
            "vortex-ingest-smoke->native_vortex_primitive_or_provider",
        )
        self.assertIn("no_benchmark_claim", local["performance_equivalence_status"])
        schema_quality = next(
            row
            for row in report["rows"]
            if row["row_id"] == "schema_quality_preview"
        )
        self.assertEqual(schema_quality["parity_status"], "equivalent_admitted_scope")
        self.assertIn("ctx.sql", schema_quality["sql_surface"])
        self.assertIsNone(schema_quality["blocker_id"])
        materialization = next(
            row
            for row in report["rows"]
            if row["row_id"] == "decoded_materialization_interop"
        )
        self.assertEqual(materialization["parity_status"], "equivalent_admitted_scope")
        self.assertIn("to_pandas", materialization["sql_surface"])
        self.assertIsNone(materialization["blocker_id"])
        vortex = next(
            row
            for row in report["rows"]
            if row["row_id"] == "local_vortex_primitive_runtime"
        )
        self.assertEqual(vortex["parity_status"], "equivalent_admitted_scope")
        self.assertEqual(vortex["runtime_gap_status"], "admitted_scope")
        self.assertIn("Vortex-normalized", vortex["claim_boundary"])
        nested_sink = next(
            row
            for row in report["rows"]
            if row["row_id"] == "typed_nested_compatibility_sink"
        )
        self.assertEqual(
            nested_sink["parity_status"],
            "deterministic_blocker_until_native_export_contract",
        )
        self.assertEqual(
            nested_sink["runtime_gap_status"],
            "native_compatibility_export_contract_missing",
        )
        self.assertIn(
            "certified native Vortex result/export contract",
            nested_sink["claim_boundary"],
        )
        native = next(
            row
            for row in report["rows"]
            if row["row_id"] == "native_vortex_general_runtime"
        )
        self.assertEqual(native["runtime_gap_status"], "front_door_connection_pending")
        performance = next(
            row
            for row in report["rows"]
            if row["row_id"] == "performance_equivalence"
        )
        self.assertEqual(performance["runtime_gap_status"], "benchmark_publication_pending")

    def test_parity_validator_rejects_overclaimed_or_fallback_rows(self) -> None:
        module = load_parity_module()
        rows = [
            SimpleNamespace(
                row_id=row_id,
                workflow=row_id,
                support_status="scoped_runtime_supported",
                runtime_gap_status="admitted_scope",
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
                runtime_gap_status="unsupported",
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
            all_broad_gaps_have_precise_runtime_status=False,
        )

        _, blockers = module.validate_matrix(matrix)

        self.assertTrue(any("fallback_attempted must be false" in b for b in blockers))
        self.assertTrue(any("gap row must be front_door_gap" in b for b in blockers))
        self.assertTrue(any("precise runtime_gap_status" in b for b in blockers))
        self.assertTrue(any("concrete pending label" in b for b in blockers))
        self.assertTrue(any("shared_runtime_path must not be generic" in b for b in blockers))
        self.assertTrue(any("flexible_anything_claim_allowed" in b for b in blockers))
        self.assertTrue(any("performance_equivalence_claim_allowed" in b for b in blockers))


if __name__ == "__main__":
    unittest.main()
