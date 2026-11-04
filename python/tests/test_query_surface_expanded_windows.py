# SPDX-License-Identifier: Apache-2.0
"""Unit contracts for expanded analytic window adapters."""

from pathlib import Path
import sys
import unittest
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "benchmarks/traditional_analytics"))
sys.path.insert(0, str(ROOT / "python/src"))

from query_surface.expanded_windows import (  # noqa: E402
    dataframe,
    native_plan,
    polars_plan,
    reference_rows,
    sql,
    window,
)
from query_surface.specification import Relation  # noqa: E402
from shardloom._result_schema import ResultType  # noqa: E402


def input_relation():
    return Relation.scan("fact")


class ExpandedWindowTests(unittest.TestCase):
    def test_reference_partitions_nulls_directions_and_shift_values(self):
        source = input_relation()
        rows = [
            {"row_id": 1, "group_key": None, "signed_value": 10},
            {"row_id": 2, "group_key": 1, "signed_value": 20},
            {"row_id": 3, "group_key": None, "signed_value": None},
            {"row_id": 4, "group_key": 1, "signed_value": 40},
        ]
        lag = window(
            source,
            function="lag",
            partition="group_key",
            argument="signed_value",
            descending=False,
            distance=1,
        )
        lead_desc = window(
            source,
            function="lead",
            partition="group_key",
            argument="signed_value",
            descending=True,
            distance=1,
        )
        self.assertEqual(
            reference_rows(lag, rows),
            [
                {"row_id": 1, "value": None},
                {"row_id": 3, "value": 10},
                {"row_id": 2, "value": None},
                {"row_id": 4, "value": 20},
            ],
        )
        self.assertEqual(
            reference_rows(lead_desc, rows),
            [
                {"row_id": 3, "value": 10},
                {"row_id": 1, "value": None},
                {"row_id": 4, "value": 20},
                {"row_id": 2, "value": None},
            ],
        )

    def test_reference_row_number_and_all_null_rolling_sum(self):
        source = input_relation()
        rows = [
            {"row_id": 2, "category": "a", "mostly_null": None},
            {"row_id": 1, "category": "a", "mostly_null": None},
            {"row_id": 3, "category": None, "mostly_null": 5},
            {"row_id": 4, "category": None, "mostly_null": 5},
        ]
        ranked = window(
            source,
            function="row_number",
            partition="category",
            argument="mostly_null",
            descending=True,
            distance=0,
        )
        rolling = window(
            source,
            function="rolling_sum",
            partition="category",
            argument="mostly_null",
            descending=False,
            distance=1,
        )
        self.assertEqual(
            reference_rows(ranked, rows),
            [
                {"row_id": 2, "value": 1},
                {"row_id": 1, "value": 2},
                {"row_id": 4, "value": 1},
                {"row_id": 3, "value": 2},
            ],
        )
        self.assertEqual(
            reference_rows(rolling, rows),
            [
                {"row_id": 1, "value": None},
                {"row_id": 2, "value": None},
                {"row_id": 3, "value": 5.0},
                {"row_id": 4, "value": 10.0},
            ],
        )

    def test_contract_rejects_invalid_parameters_and_source_schema(self):
        source = input_relation()
        values = dict(
            function="lag", partition="group_key", argument="signed_value", descending=False, distance=1
        )
        for change in (
            {"function": "rank"},
            {"partition": "row_id"},
            {"argument": "amount"},
            {"distance": 0},
            {"distance": 17},
            {"descending": 1},
        ):
            with self.subTest(change=change), self.assertRaises((ValueError, TypeError)):
                window(source, **(values | change))
        malformed = Relation(
            "scan",
            (
                ("row_id", ResultType("int64", True)),
                ("group_key", source.col("group_key").dtype),
                ("signed_value", source.col("signed_value").dtype),
            ),
            args=("malformed",),
        )
        with self.assertRaisesRegex(ValueError, "non-null int64 row_id"):
            window(malformed, **values)

    def test_sql_adapter_uses_explicit_partition_order_and_frame(self):
        node = window(
            input_relation(),
            function="rolling_sum",
            partition="nullable_flag",
            argument="mostly_null",
            descending=True,
            distance=3,
        )
        rendered = sql(node, "SELECT row_id, nullable_flag, mostly_null FROM fact")
        self.assertEqual(
            rendered,
            "SELECT q.row_id AS row_id, SUM(q.mostly_null) OVER (PARTITION BY q.nullable_flag ORDER BY q.row_id DESC NULLS LAST ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS value FROM (SELECT row_id, nullable_flag, mostly_null FROM fact) AS q",
        )

    def test_native_adapter_constructs_public_window_expression(self):
        calls = {}

        class Expr:
            @staticmethod
            def literal(value):
                return ("literal", value)

            @staticmethod
            def column(name):
                return ("column", name)

        class FrameOffset:
            @staticmethod
            def number(value):
                return ("number", value)

        class WindowExpression:
            def __init__(self, function, alias, **kwargs):
                self.function, self.alias, self.kwargs = function, alias, kwargs

        class WindowFrame:
            def __init__(self, unit, start, end):
                self.unit, self.start, self.end = unit, start, end

        class Child:
            def window(self, projection, expressions):
                calls["window"] = (projection, expressions)
                return self

            def project(self, projections):
                calls["project"] = projections
                return "native-plan"

        native = SimpleNamespace(
            Expression=Expr,
            FrameOffset=FrameOffset,
            WindowExpression=WindowExpression,
            WindowFrame=WindowFrame,
        )
        node = window(
            input_relation(),
            function="rolling_sum",
            partition="group_key",
            argument="signed_value",
            descending=False,
            distance=2,
        )
        self.assertEqual(native_plan(node, Child(), native), "native-plan")
        projection, (measure,) = calls["window"]
        self.assertEqual(projection, ["row_id", "group_key", "signed_value"])
        self.assertEqual((measure.function, measure.alias), ("sum", "value"))
        self.assertEqual(measure.kwargs["order_by"], [("row_id", False, "last")])
        self.assertEqual(measure.kwargs["frame"].start, ("preceding", ("number", ("literal", 2))))
        self.assertEqual(calls["project"], [("row_id", ("column", "row_id")), ("value", ("column", "value"))])

    def test_dataframe_adapter_uses_public_expression_objects(self):
        from shardloom import Window, row_number

        class Child:
            def select(self, *columns):
                self.columns = columns
                return self

        child = Child()
        source = input_relation()
        node = window(
            source,
            function="row_number",
            partition="group_key",
            argument="signed_value",
            descending=True,
            distance=0,
        )
        self.assertIs(dataframe(node, child, SimpleNamespace(Window=Window, row_number=row_number)), child)
        self.assertEqual(child.columns[0], "row_id")
        self.assertIn(
            "ROW_NUMBER() OVER (PARTITION BY group_key ORDER BY row_id DESC NULLS LAST) AS value",
            str(child.columns[1]),
        )
        shifted = window(
            source,
            function="lag",
            partition="group_key",
            argument="signed_value",
            descending=False,
            distance=2,
        )
        dataframe(shifted, child, SimpleNamespace(Window=Window, row_number=row_number))
        self.assertIn("LAG(signed_value,2)", str(child.columns[1]))

    def test_polars_plan_orders_rows_and_preserves_null_partition(self):
        try:
            import polars as pl
        except ImportError:
            self.skipTest("Polars is an optional comparison dependency")
        node = window(
            input_relation(),
            function="rolling_sum",
            partition="nullable_flag",
            argument="mostly_null",
            descending=False,
            distance=1,
        )
        frame = pl.DataFrame(
            {
                "row_id": [2, 1, 3, 4],
                "nullable_flag": [None, None, True, True],
                "mostly_null": [None, None, 7, None],
            }
        ).lazy()
        output = polars_plan(node, frame, pl).collect().sort("row_id")
        self.assertEqual(
            output.to_dicts(),
            [
                {"row_id": 1, "value": None},
                {"row_id": 2, "value": None},
                {"row_id": 3, "value": 7.0},
                {"row_id": 4, "value": 7.0},
            ],
        )


if __name__ == "__main__":
    unittest.main()
