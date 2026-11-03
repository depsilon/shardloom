# SPDX-License-Identifier: Apache-2.0
"""Tiny synthetic contracts for nested explode and IN-subquery corpus adapters."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock
from pathlib import Path
import sys
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "benchmarks/traditional_analytics"))

from shardloom._result_schema import ResultType
from query_surface.expanded_nested import (
    dataframe,
    explode,
    membership,
    native_plan,
    polars_plan,
    reference_rows,
    sql,
)
from query_surface.specification import Relation


def explode_source():
    return Relation(
        "scan",
        (
            ("row_id", ResultType("int64", False)),
            ("label", ResultType("utf8", True)),
            ("items", ResultType("list", True, (ResultType("int64", True),))),
        ),
        args=("nested",),
    )


def membership_sources():
    left = Relation(
        "scan",
        (
            ("row_id", ResultType("int64", False)),
            ("key", ResultType("int64", True)),
            ("label", ResultType("utf8", True)),
        ),
        args=("left",),
    )
    right = Relation("scan", (("candidate", ResultType("int64", True)),), args=("right",))
    return left, right


class ExpandedNestedTests(unittest.TestCase):
    def test_explode_oracle_null_list_empty_list_and_null_elements(self):
        node = explode(explode_source())
        self.assertEqual(node.op, "explode_case")
        self.assertEqual(node.args, ())
        self.assertEqual(node.inputs, (explode_source(),))
        self.assertEqual(dict(node.schema)["items"], ResultType("int64", True))
        rows = [
            {"row_id": 1, "label": "a", "items": None},
            {"row_id": 2, "label": "b", "items": []},
            {"row_id": 3, "label": "c", "items": [7, None, 8]},
        ]
        self.assertEqual(
            reference_rows(node, iter(rows)),
            [
                {"row_id": 1, "label": "a", "items": None},
                {"row_id": 3, "label": "c", "items": 7},
                {"row_id": 3, "label": "c", "items": None},
                {"row_id": 3, "label": "c", "items": 8},
            ],
        )
        self.assertEqual(reference_rows(node, iter([])), [])

    def test_membership_where_oracle_in_not_in_null_and_empty_rhs(self):
        left, right = membership_sources()
        lhs = [
            {"row_id": 1, "key": None, "label": "null"},
            {"row_id": 2, "key": 1, "label": "one"},
            {"row_id": 3, "key": 2, "label": "two"},
            {"row_id": 4, "key": 3, "label": "three"},
        ]
        for negated, rhs, row_ids in (
            (False, [{"candidate": 2}, {"candidate": None}], [3]),
            (True, [{"candidate": 2}, {"candidate": None}], []),
            (False, [], []),
            (True, [], [1, 2, 3, 4]),
            (False, [{"candidate": 2}], [3]),
            (True, [{"candidate": 2}], [2, 4]),
        ):
            node = membership(left, right, left_key="key", right_key="candidate", negated=negated)
            self.assertEqual(node.op, "membership_case")
            self.assertEqual(node.inputs, (left, right))
            self.assertEqual(node.args, ("key", "candidate", negated))
            self.assertEqual([row["row_id"] for row in reference_rows(node, iter(lhs), iter(rhs))], row_ids)

    def test_declarations_emit_real_explode_sql_and_membership_subquery(self):
        node = explode(explode_source())
        statement = sql(node, ("SELECT row_id,label,items FROM nested",))
        self.assertEqual(
            statement,
            'SELECT * FROM EXPLODE((SELECT row_id,label,items FROM nested), \'{"column":"items"}\') AS explode_case',
        )
        left, right = membership_sources()
        inclusion = membership(left, right, left_key="key", right_key="candidate", negated=False)
        exclusion = membership(left, right, left_key="key", right_key="candidate", negated=True)
        self.assertIn(
            "WHERE l.key IN (SELECT r.candidate FROM (SELECT * FROM right) AS r)",
            sql(inclusion, ("SELECT * FROM left", "SELECT * FROM right")),
        )
        self.assertIn(
            "WHERE l.key NOT IN (SELECT r.candidate FROM (SELECT * FROM right) AS r)",
            sql(exclusion, ("SELECT * FROM left", "SELECT * FROM right")),
        )

    def test_native_adapter_uses_unary_payload_and_typed_subquery_arguments(self):
        calls = {}

        class Expression:
            @staticmethod
            def column(name):
                return ("column", name)

        class Child:
            def __init__(self, name):
                self.name = name

            def unary(self, operation, options):
                calls["unary"] = (self.name, operation, options)
                return "exploded-plan"

            def subquery(self, relation, output_column, **kwargs):
                calls["subquery"] = (self.name, relation.name, output_column, kwargs)
                return self

            def filter(self, predicate):
                calls["filter"] = (self.name, predicate)
                return self

            def project(self, projections):
                calls["project"] = (self.name, projections)
                return "membership-plan"

        self.assertEqual(
            native_plan(explode(explode_source()), (Child("input"),), SimpleNamespace(Expression=Expression)),
            "exploded-plan",
        )
        self.assertEqual(calls["unary"], ("input", "explode", '{"column":"items"}'))
        left, right = membership_sources()
        node = membership(left, right, left_key="key", right_key="candidate", negated=True)
        self.assertEqual(
            native_plan(node, (Child("left"), Child("right")), SimpleNamespace(Expression=Expression)),
            "membership-plan",
        )
        self.assertEqual(
            calls["subquery"],
            (
                "left",
                "right",
                "__membership_match",
                {
                    "kind": "in",
                    "keys": [("key", "candidate")],
                    "negated": True,
                },
            ),
        )
        self.assertEqual(calls["filter"], ("left", ("column", "__membership_match")))
        self.assertEqual(
            calls["project"],
            (
                "left",
                [
                    ("row_id", ("column", "row_id")),
                    ("key", ("column", "key")),
                    ("label", ("column", "label")),
                ],
            ),
        )

    def test_dataframe_adapters_use_native_explode_and_source_membership(self):
        calls = {}

        class Predicate:
            def __init__(self, value):
                self.value = value

            def __invert__(self):
                return Predicate(("not", self.value))

        class Column:
            def __init__(self, name):
                self.name = name

            def isin_source(self, source, column):
                return Predicate(("in_source", self.name, source.name, column))

        class Frame:
            def __init__(self, name):
                self.name = name

            def explode(self, column):
                calls["explode"] = (self.name, column)
                return "explode-frame"

            def filter(self, predicate):
                calls["filter"] = (self.name, predicate.value)
                return self

            def select(self, *columns):
                calls["select"] = (self.name, columns)
                return "membership-frame"

        sl = SimpleNamespace(col=Column)
        self.assertEqual(dataframe(explode(explode_source()), (Frame("nested"),), sl), "explode-frame")
        self.assertEqual(calls["explode"], ("nested", "items"))
        left, right = membership_sources()
        node = membership(left, right, left_key="key", right_key="candidate", negated=True)
        self.assertEqual(dataframe(node, (Frame("left"), Frame("right")), sl), "membership-frame")
        self.assertEqual(calls["filter"], ("left", ("not", ("in_source", "key", "right", "candidate"))))
        self.assertEqual(calls["select"], ("left", ("row_id", "key", "label")))

    def test_positive_membership_uses_only_the_deduplicated_right_keys(self):
        outer, inner = membership_sources()
        node = membership(outer, inner, left_key="key", right_key="candidate", negated=False)
        left, right, pl = MagicMock(), MagicMock(), MagicMock()
        pl.len.side_effect = AssertionError("positive membership must not summarize the right input")
        polars_plan(node, (left, right), pl)
        right.select.assert_called_once_with(pl.col.return_value)
        keys = right.select.return_value.unique.return_value.with_columns.return_value
        left.join.assert_called_once_with(keys, left_on="key", right_on="candidate", how="left")
        left.join.return_value.join.assert_not_called()

    def test_polars_explode_and_membership_match_independent_sql_where_oracle(self):
        try:
            import polars as pl
        except ImportError:
            self.skipTest("Polars is an optional comparison dependency")

        explode_node = explode(explode_source())
        source_rows = [
            {"row_id": 1, "label": "null", "items": None},
            {"row_id": 2, "label": "empty", "items": []},
            {"row_id": 3, "label": "values", "items": [7, None, 8]},
        ]
        actual_explode = (
            polars_plan(
                explode_node,
                (pl.DataFrame(source_rows).lazy(),),
                pl,
            )
            .collect()
            .to_dicts()
        )
        self.assertEqual(actual_explode, reference_rows(explode_node, iter(source_rows)))

        left, right = membership_sources()
        lhs = [
            {"row_id": 1, "key": None, "label": "null"},
            {"row_id": 2, "key": 1, "label": "one"},
            {"row_id": 3, "key": 2, "label": "two"},
            {"row_id": 4, "key": 3, "label": "three"},
        ]
        for negated, rhs in (
            (False, [{"candidate": 2}, {"candidate": None}]),
            (True, [{"candidate": 2}, {"candidate": None}]),
            (False, []),
            (True, []),
            (False, [{"candidate": 2}]),
            (True, [{"candidate": 2}]),
        ):
            with self.subTest(negated=negated, rhs=rhs):
                node = membership(left, right, left_key="key", right_key="candidate", negated=negated)
                actual = (
                    polars_plan(
                        node,
                        (
                            pl.DataFrame(lhs).lazy(),
                            pl.DataFrame(
                                rhs,
                                schema={"candidate": pl.Int64},
                            ).lazy(),
                        ),
                        pl,
                    )
                    .collect()
                    .to_dicts()
                )
                self.assertEqual(
                    sorted(actual, key=lambda row: row["row_id"]),
                    sorted(reference_rows(node, iter(lhs), iter(rhs)), key=lambda row: row["row_id"]),
                )


if __name__ == "__main__":
    unittest.main()
