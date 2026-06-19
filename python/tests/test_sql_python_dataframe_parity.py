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
        self.assertIn(
            "arbitrary_sql_python_dataframe_breadth",
            report["v1_supported_row_ids"],
        )
        self.assertIn("performance_equivalence", report["v1_pending_row_ids"])
        self.assertIn("selective_filter", report["v1_example_scenario_ids"])
        self.assertEqual(report["v1_expected_error_scenario_ids"], [])
        self.assertFalse(report["flexible_anything_claim_allowed"])
        self.assertFalse(report["performance_equivalence_claim_allowed"])
        self.assertEqual(report["admitted_row_count"], 9)
        self.assertEqual(report["remaining_gap_count"], 2)
        self.assertEqual(report["dataframe_method_blocker_count"], 0)
        self.assertEqual(report["dataframe_method_pending_or_unsupported_count"], 0)
        self.assertEqual(report["dataframe_named_runtime_surface_status"], "passed")
        self.assertEqual(report["semantic_surface_status"], "passed")
        self.assertEqual(
            report["front_door_semantic_surface_schema_version"],
            "shardloom.front_door_semantic_surface_matrix.v1",
        )
        self.assertGreaterEqual(report["semantic_surface_row_count"], 31)
        self.assertFalse(report["pandas_compatible_claim_allowed"])
        self.assertFalse(report["polars_compatible_claim_allowed"])
        self.assertFalse(report["broad_dataframe_compatible_claim_allowed"])
        self.assertFalse(report["ansi_sql_compliant_claim_allowed"])
        self.assertTrue(report["semantic_surface_all_no_fallback_no_external_engine"])
        self.assertTrue(report["semantic_surface_all_deterministic_blockers"])
        self.assertIn(
            "ShardLoom-native/Vortex-native routes",
            report["dataframe_claim_statement"],
        )
        self.assertIn(
            "documented subset of pandas/Polars-style DataFrame operations",
            report["dataframe_subset_claim_statement"],
        )
        self.assertIn(
            "documented SQL-standard-inspired SELECT-query subset",
            report["sql_claim_statement"],
        )
        self.assertIn(
            "dataframe_materialization",
            report["dataframe_semantic_surface_row_ids"],
        )
        self.assertIn(
            "dataframe_expression_callable_apis",
            report["dataframe_semantic_surface_row_ids"],
        )
        self.assertIn("sql_null_semantics", report["sql_semantic_surface_row_ids"])
        self.assertIn("sql_subqueries", report["sql_semantic_surface_row_ids"])
        self.assertIn("sql_fallback_boundary", report["sql_semantic_surface_row_ids"])
        self.assertIn(
            "shared_claim_vocabulary",
            report["shared_semantic_surface_row_ids"],
        )
        semantic_by_id = {
            row["row_id"]: row for row in report["semantic_surface_rows"]
        }
        self.assertIn(
            "No hidden pandas/Polars construction backend",
            semantic_by_id["dataframe_construction_read_apis"]["unsupported_scope"],
        )
        self.assertIn(
            "not broad SQL-standard/ANSI-style compliance",
            semantic_by_id["sql_parser_grammar_scope"]["claim_boundary"],
        )
        self.assertIn(
            "Do not claim broad pandas compatibility",
            semantic_by_id["shared_claim_vocabulary"]["unsupported_scope"],
        )
        self.assertIn("sample", report["dataframe_named_runtime_surface_ids"])
        self.assertIn("pivot_table", report["dataframe_named_runtime_surface_ids"])
        self.assertIn("rolling", report["dataframe_named_runtime_surface_ids"])
        self.assertIn("fillna", report["dataframe_named_runtime_surface_ids"])
        self.assertIn("map_rows", report["dataframe_named_runtime_surface_ids"])
        self.assertIn("apply", report["dataframe_plan_transform_only_method_ids"])
        self.assertIn(
            "cg21.workflow.sample.rng_object_contract_missing",
            report["dataframe_future_contract_blocker_ids"],
        )
        self.assertIn(
            "cg21.workflow.map_rows.python_callable_or_row_udf_unsupported",
            report["dataframe_future_contract_blocker_ids"],
        )
        self.assertEqual(report["dataframe_future_contract_classification_count"], 27)
        self.assertEqual(report["dataframe_future_contract_repo_feasible_count"], 18)
        self.assertEqual(report["dataframe_future_contract_unsafe_callable_count"], 6)
        self.assertEqual(
            report["dataframe_future_contract_classification_counts"],
            {
                "repo_feasible_contract_needed": 18,
                "scoped_product_boundary": 3,
                "unsafe_callable_boundary": 6,
            },
        )
        future_contract_by_id = {
            row["blocker_id"]: row
            for row in report["dataframe_future_contract_classification_rows"]
        }
        self.assertEqual(
            future_contract_by_id[
                "cg21.workflow.fanout.multi_sink_atomicity_contract_missing"
            ]["classification"],
            "repo_feasible_contract_needed",
        )
        self.assertIn(
            "typed UDF",
            future_contract_by_id[
                "cg21.workflow.apply.python_callable_unsupported"
            ]["v1_resolution"],
        )
        fillna = next(
            row
            for row in report["dataframe_named_runtime_surface_rows"]
            if row["method"] == "fillna"
        )
        self.assertEqual(fillna["support_status"], "production_admitted_local_workflow")
        self.assertTrue(fillna["runtime_execution"])
        self.assertEqual(
            fillna["future_contract_blocker_ids"],
            ["cg21.workflow.fillna.null_fill_semantics_unsupported"],
        )
        apply = next(
            row
            for row in report["dataframe_named_runtime_surface_rows"]
            if row["method"] == "apply"
        )
        self.assertEqual(apply["support_status"], "lazy_plan_supported")
        self.assertFalse(apply["runtime_execution"])
        self.assertEqual(
            apply["future_contract_blocker_ids"],
            ["cg21.workflow.apply.python_callable_unsupported"],
        )
        self.assertTrue(report["all_broad_gaps_have_precise_runtime_status"])
        self.assertIn(
            "benchmark_publication_pending",
            report["runtime_gap_status_vocabulary"],
        )
        self.assertIn(
            "Vortex-backed runtime path",
            report["vortex_normalization_contract"],
        )
        self.assertNotIn(
            "arbitrary_sql_python_dataframe_breadth",
            report["remaining_gap_row_ids"],
        )
        self.assertIn("object_store_lakehouse_catalog", report["remaining_gap_row_ids"])
        self.assertIn("performance_equivalence", report["remaining_gap_row_ids"])
        object_store = next(
            row
            for row in report["rows"]
            if row["row_id"] == "object_store_lakehouse_catalog"
        )
        self.assertEqual(
            object_store["runtime_gap_status"],
            "external_environment_gate_pending",
        )
        self.assertEqual(
            object_store["support_status"],
            "external_production_io_gate_pending",
        )
        self.assertIn("Local object-store/table", object_store["claim_boundary"])
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
        self.assertEqual(nested_sink["parity_status"], "equivalent_admitted_scope")
        self.assertEqual(nested_sink["runtime_gap_status"], "admitted_scope")
        self.assertIn("native Vortex structured row stream", nested_sink["claim_boundary"])
        native = next(
            row
            for row in report["rows"]
            if row["row_id"] == "native_vortex_general_runtime"
        )
        self.assertEqual(native["runtime_gap_status"], "admitted_scope")
        self.assertEqual(native["parity_status"], "equivalent_admitted_scope")
        self.assertIn("native_vortex_unified_plan", native["shared_runtime_path"])
        language = next(
            row
            for row in report["rows"]
            if row["row_id"] == "arbitrary_sql_python_dataframe_breadth"
        )
        self.assertEqual(language["runtime_gap_status"], "admitted_scope")
        self.assertEqual(language["parity_status"], "equivalent_admitted_scope")
        self.assertIsNone(language["blocker_id"])
        self.assertIn(
            "documented local SQL/Python/DataFrame-style subset",
            language["claim_boundary"],
        )
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
                row_id="performance_equivalence",
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
