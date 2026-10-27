# SPDX-License-Identifier: Apache-2.0
"""Optional external-baseline adapter checks, never an engine dependency."""
from __future__ import annotations

import os
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "python" / "src"))
sys.path.insert(0, str(ROOT / "benchmarks" / "traditional_analytics"))
sys.path.insert(0, str(ROOT / "scripts"))

from query_surface.corpus import corpus
from query_surface.datasets import DatasetSpec, write_dataset
from query_surface.polars_adapter import compare_result, declaration, expression
from query_surface.polars_run import comparison_record
from query_surface.reference import expected
from query_surface.semantics import ResultContract, ResultMismatch, ResultType, TypedResult, arrow_schema
from query_surface.specification import Expression


class PolarsQuerySurfaceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            import polars as pl
            import pyarrow
        except ImportError as error:
            if os.environ.get("SHARDLOOM_REQUIRE_POLARS_BASELINE") == "1":
                raise AssertionError("required Polars/Arrow baseline adapter is unavailable") from error
            raise unittest.SkipTest("optional Polars baseline is not installed") from error
        cls.pl = pl

    def test_all_development_declarations_match_independent_tiny_values(self):
        spec = DatasetSpec(fact_rows=127, entity_rows=19, product_rows=7, relationship_rows=37, calendar_rows=31)
        with tempfile.TemporaryDirectory(prefix="shardloom-polars-adapter-") as directory:
            root = Path(directory) / "dataset"
            manifest = write_dataset(root, spec, "shuffled", guard=lambda: None)
            sources = {name: [str(root / part["path"]) for part in record["files"]]
                       for name, record in manifest["tables"].items()}
            cases = [case for case in corpus() if case.relation is not None and case.partition == "development"]
            for case in cases:
                with self.subTest(case=case.id):
                    output = declaration(case.relation, sources, self.pl).collect().to_arrow()
                    compare_result(expected(case, spec), TypedResult(arrow_schema(output.schema), output.to_pylist()), case.contract)

    def test_signed_remainder_uses_integer_precision_for_all_sign_pairs(self):
        pl = self.pl
        schema = (("a", ResultType("int64", True)), ("b", ResultType("int64", False)))
        expr = Expression.column("a", schema).binary("mod", Expression.column("b", schema))
        values = [(5, 3), (-5, 3), (5, -3), (-5, -3), (2**63 - 1, 7), (-2**63, 7), (None, 7)]
        frame = pl.DataFrame({"a": [a for a, _ in values], "b": [b for _, b in values]}, schema={"a": pl.Int64, "b": pl.Int64})
        from query_surface.reference import scalar
        self.assertEqual(frame.select(expression(expr, pl)).to_series().to_list(),
                         [scalar(expr, {"a": a, "b": b}) for a, b in values])

    def test_parquet_projection_predicates_and_rich_types_use_the_original_source(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        spec = DatasetSpec(fact_rows=127, entity_rows=19, product_rows=7, relationship_rows=37, calendar_rows=31)
        with tempfile.TemporaryDirectory(prefix="shardloom-polars-parquet-") as directory:
            root = Path(directory) / "dataset"
            manifest = write_dataset(root, spec, "sorted", guard=lambda: None)
            member = manifest["tables"]["fact"]["files"][0]
            with pa.memory_map(str(root / member["path"]), "r") as source:
                table = pa.ipc.open_file(source).read_all()
            path = root / "fact.parquet"
            pq.write_table(table, path, row_group_size=17)
            names = {"predicate_one_row", "projection_rich", "projection_wide", "selectivity_10000", "set_union_all"}
            cases = [case for case in corpus(interactions="all") if case.id in names]
            self.assertEqual(len(cases), len(names))
            for case in cases:
                with self.subTest(case=case.id):
                    output = declaration(case.relation, {"fact": [str(path)]}, self.pl).collect().to_arrow()
                    compare_result(expected(case, spec), TypedResult(arrow_schema(output.schema), output.to_pylist()), case.contract)

    def test_unexposed_nullability_is_not_permission_to_change_types_or_values(self):
        schema = (("v", ResultType("int64", False)),)
        wanted, contract = TypedResult(schema, [{"v": 1}]), ResultContract(schema)
        compare_result(wanted, TypedResult((("v", ResultType("int64", True)),), [{"v": 1}]), contract)
        for actual in (TypedResult((("v", ResultType("int32", True)),), [{"v": 1}]),
                       TypedResult((("alias", ResultType("int64", True)),), [{"alias": 1}]),
                       TypedResult((("v", ResultType("int64", True)),), [{"v": None}])):
            with self.assertRaises(ResultMismatch):
                compare_result(wanted, actual, contract)

    def test_comparison_requires_correct_results_and_a_common_complete_clock(self):
        record = comparison_record("case", "numeric", 2.0, 0.5, correct=True)
        self.assertEqual(record["clock"], "complete_workflow_seconds")
        self.assertEqual(record["native_over_polars"], 4.0)
        self.assertEqual(record["native_minus_polars_seconds"], 1.5)
        self.assertIsNone(comparison_record("case", "numeric", 2.0, None, correct=False)["native_over_polars"])
        for clock in (None, True, 0, -1, float("nan")):
            with self.assertRaises(ValueError):
                comparison_record("case", "numeric", 2.0, clock, correct=True)


if __name__ == "__main__":
    unittest.main()
