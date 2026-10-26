# SPDX-License-Identifier: Apache-2.0
"""Legal compositions, measured coverage and independent reference controls."""
from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "benchmarks" / "traditional_analytics"))
sys.path.insert(0, str(ROOT / "scripts"))

from query_surface.adapters import expression_sql, relation_sql
from query_surface.corpus import aggregate_cells, composition_neighbors, corpus, measure
from query_surface.coverage import coverage_record, covering_selection, feasible_cells
from query_surface.datasets import DatasetSpec, RowGenerator
from query_surface.reference import expected, records, scalar
from query_surface.semantics import ResultType
from query_surface.specification import Expression as E, FAMILIES, Order, Relation


class QueryCorpusTests(unittest.TestCase):
    def test_constrained_pairwise_coverage_measures_only_feasible_interactions(self):
        factors = {"type": ("integer", "text"), "operator": ("sum", "count"), "nulls": ("none", "some")}
        cells = feasible_cells(factors, allowed=lambda cell: cell["operator"] != "sum" or cell["type"] != "text")
        selected = covering_selection(cells)
        record = coverage_record(cells, selected)
        self.assertTrue(record["complete"])
        self.assertEqual(record["missing_interactions"], [])
        self.assertEqual(record["feasible_cells"], 6)
        self.assertFalse(coverage_record(cells, selected[:1])["complete"])
        with self.assertRaises(ValueError):
            coverage_record(cells, [{"type": "text", "operator": "sum", "nulls": "none"}])
        self.assertEqual(selected, covering_selection(cells))

    def test_aggregate_factor_selection_and_higher_order_coverage_are_distinct(self):
        cells = aggregate_cells()
        pairwise = covering_selection(cells)
        self.assertTrue(coverage_record(cells, pairwise)["complete"])
        self.assertLess(len(pairwise), len(cells))
        self.assertFalse(coverage_record(cells, pairwise, strength=3)["complete"])
        self.assertTrue(coverage_record(cells, covering_selection(cells, strength=3), strength=3)["complete"])

    def test_corpus_keeps_all_families_pending_cases_and_stable_identities(self):
        cases = corpus()
        self.assertEqual({item.family for item in cases}, set(FAMILIES))
        self.assertEqual([item.identity for item in cases], [item.identity for item in corpus()])
        self.assertTrue(any(item.partition == "held_out" for item in cases if item.relation))
        pending = [item for item in cases if item.relation is None]
        self.assertTrue(pending)
        self.assertTrue(all(item.pending_reason and item.desired_sql and item.contract is None for item in pending))
        self.assertGreater(len(corpus(interactions="all")), len(cases))

    def test_every_executable_case_has_complete_tiny_expectations_and_rendered_sql(self):
        spec = DatasetSpec(fact_rows=27, entity_rows=7, product_rows=3, relationship_rows=19, calendar_rows=5)
        sources = {name: (Path("/declared") / f"{name}.vortex",) for name in spec.counts()}
        for item in corpus(interactions="all", include_pending=False):
            with self.subTest(case=item.id):
                result = expected(item, spec)
                self.assertEqual(result.schema, item.contract.schema)
                self.assertTrue(relation_sql(item.relation, sources).startswith("SELECT"))
                self.assertTrue(all(set(row) == set(dict(item.contract.schema)) for row in result.rows))

    def test_type_constraints_reject_invalid_compositions_before_execution(self):
        source = Relation.scan("fact")
        for operation in (
            lambda: source.col("missing"),
            lambda: source.col("category").binary("add", 1),
            lambda: source.where(source.col("signed_value")),
            lambda: source.order(Order("missing")),
            lambda: source.take(-1),
            lambda: source.project(("x", source.col("row_id")), ("x", source.col("signed_value"))),
            lambda: source.project(("x", replace(source.col("row_id"), dtype=ResultType("utf8", False)))),
            lambda: source.select("row_id").set(source.select("category"), "union_all"),
        ):
            with self.assertRaises((ValueError, KeyError)):
                operation()

    def test_independent_reference_null_logic_and_negative_remainder(self):
        schema = (("a", ResultType("bool", True)), ("b", ResultType("bool", True)))
        a, b = E.column("a", schema), E.column("b", schema)
        self.assertIs(scalar(a.binary("and", b), {"a": None, "b": False}), False)
        self.assertIs(scalar(a.binary("or", b), {"a": None, "b": True}), True)
        self.assertIsNone(scalar(a.binary("and", b), {"a": None, "b": True}))
        self.assertEqual(scalar(E.literal(-8).binary("mod", 3), {}), -2)
        self.assertEqual(expression_sql(E.literal("a'b")), "'a''b'")

    def test_count_star_generates_no_unused_column_values(self):
        spec = DatasetSpec(fact_rows=31, entity_rows=3, product_rows=2, relationship_rows=4, calendar_rows=1)
        self.assertEqual(RowGenerator(spec, "fact").row(0, ()), {})
        source = Relation.scan("fact")
        result = list(records(source.aggregate((), (measure(source, "count", alias="rows"),)), spec))
        self.assertEqual(result, [{"rows": 31}])

    def test_empty_and_all_null_aggregates_distinguish_count_and_sum(self):
        spec = DatasetSpec(fact_rows=0, entity_rows=3, product_rows=2, relationship_rows=4, calendar_rows=1)
        source = Relation.scan("fact")
        aggregate = source.aggregate((), [measure(source, op, "all_null", op) for op in ("count", "sum", "avg", "min", "max")])
        expected_row = {"count": 0, "sum": None, "avg": None, "min": None, "max": None}
        self.assertEqual(list(records(aggregate, spec)), [expected_row])
        self.assertEqual(list(records(aggregate, replace(spec, fact_rows=11))), [expected_row])

    def test_projection_and_join_neighbors_have_independent_equivalence_controls(self):
        spec = DatasetSpec(fact_rows=173, entity_rows=31, product_rows=7, relationship_rows=67, calendar_rows=11)
        cases = {case.id: case for case in composition_neighbors()}
        for label in ("direct", "arithmetic", "coalesce", "string", "temporal"):
            self.assertEqual(expected(cases[f"projection_limit_{label}_31"], spec),
                             expected(cases[f"projection_limit_{label}_prefiltered_31"], spec))
        self.assertEqual(expected(cases["star_join"], spec), expected(cases["star_join_products_first"], spec))
        self.assertEqual(expected(cases["join_population_left_count"], spec).rows, [{"rows": 173}])


if __name__ == "__main__":
    unittest.main()
