# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "benchmarks/traditional_analytics"))

from shardloom._result_schema import ResultType
from query_surface.expanded_asof import (
    asof,
    dataframe,
    native_plan,
    polars_plan,
    reference_rows,
    sql,
)
from query_surface.specification import Relation


def inputs():
    left = Relation(
        "scan", (("row_id", ResultType("int64", False)), ("day", ResultType("int64", True))), args=("fact",)
    )
    right = Relation(
        "scan",
        (("day_index", ResultType("int64", False)), ("week_index", ResultType("int64", False))),
        args=("calendar",),
    )
    return left, right


class ExpandedAsOfTests(unittest.TestCase):
    def test_oracle_direction_exact_tolerance_null_empty_and_gaps(self):
        left, right = inputs()
        rows = [
            {"row_id": 4, "day": 6},
            {"row_id": 1, "day": None},
            {"row_id": 5, "day": 4},
            {"row_id": 9, "day": 8},
        ]
        calendar = [{"day_index": day, "week_index": day // 7} for day in (2, 6, 9)]
        backward = asof(
            left,
            right,
            left_key="day",
            right_key="day_index",
            direction="backward",
            exact=True,
            tolerance=None,
        )
        self.assertEqual(
            reference_rows(backward, rows, calendar),
            [
                {"row_id": 4, "matched_day_index": 6, "week_index": 0},
                {"row_id": 1, "matched_day_index": None, "week_index": None},
                {"row_id": 5, "matched_day_index": 2, "week_index": 0},
                {"row_id": 9, "matched_day_index": 6, "week_index": 0},
            ],
        )
        forward = asof(
            left, right, left_key="day", right_key="day_index", direction="forward", exact=False, tolerance=2
        )
        self.assertEqual(
            reference_rows(forward, rows, calendar),
            [
                {"row_id": 4, "matched_day_index": None, "week_index": None},
                {"row_id": 1, "matched_day_index": None, "week_index": None},
                {"row_id": 5, "matched_day_index": 6, "week_index": 0},
                {"row_id": 9, "matched_day_index": 9, "week_index": 1},
            ],
        )
        empty = asof(
            left, right, left_key="day", right_key="day_index", direction="backward", exact=False, tolerance=0
        )
        self.assertEqual(
            reference_rows(empty, rows, []),
            [{"row_id": row["row_id"], "matched_day_index": None, "week_index": None} for row in rows],
        )

    def test_oracle_accepts_signed_left_keys_outside_calendar_and_bounds_tolerance(self):
        left, right = inputs()
        rows = [{"row_id": 1, "day": -1}, {"row_id": 2, "day": 731}]
        calendar = [{"day_index": day, "week_index": day // 7} for day in (2, 6, 9)]
        forward = asof(
            left, right, left_key="day", right_key="day_index", direction="forward", exact=True, tolerance=3
        )
        self.assertEqual(
            reference_rows(forward, rows, calendar),
            [
                {"row_id": 1, "matched_day_index": 2, "week_index": 0},
                {"row_id": 2, "matched_day_index": None, "week_index": None},
            ],
        )
        backward = asof(
            left,
            right,
            left_key="day",
            right_key="day_index",
            direction="backward",
            exact=True,
            tolerance=722,
        )
        self.assertEqual(
            reference_rows(backward, rows, calendar),
            [
                {"row_id": 1, "matched_day_index": None, "week_index": None},
                {"row_id": 2, "matched_day_index": 9, "week_index": 1},
            ],
        )

    def test_declaration_schema_and_surface_renderers(self):
        left, right = inputs()
        node = asof(
            left, right, left_key="day", right_key="day_index", direction="backward", exact=True, tolerance=5
        )
        self.assertEqual(node.op, "asof_case")
        self.assertEqual(node.args, ("day", "day_index", "backward", True, 5))
        self.assertEqual([name for name, _ in node.schema], ["row_id", "matched_day_index", "week_index"])
        statement = sql(node, ("SELECT * FROM fact", "SELECT * FROM calendar"))
        self.assertIn("ASOF LEFT JOIN", statement)
        self.assertIn("MATCH_CONDITION (f.day >= d.day_index)", statement)
        self.assertNotIn(" ON ", statement)
        self.assertTrue(statement.endswith("TOLERANCE 5"))

        calls = {}

        class NativeLeft:
            def join_asof(self, other, keys, columns, **kwargs):
                calls["native"] = (other, keys, columns, kwargs)
                return "native-plan"

        self.assertEqual(native_plan(node, (NativeLeft(), "native-right"), object()), "native-plan")
        self.assertEqual(calls["native"][1], ("day", "day_index"))
        self.assertEqual(
            calls["native"][2],
            [
                ("left", "row_id", "row_id"),
                ("right", "day_index", "matched_day_index"),
                ("right", "week_index", "week_index"),
            ],
        )
        self.assertEqual(
            calls["native"][3], {"strategy": "backward", "tolerance": 5, "allow_exact_matches": True}
        )

        class Frame:
            def join_asof(self, other, **kwargs):
                calls["frame_join"] = (other, kwargs)
                return self

            def select(self, *columns):
                calls["frame_select"] = columns
                return "dataframe-plan"

        self.assertEqual(dataframe(node, (Frame(), "frame-right"), object()), "dataframe-plan")
        self.assertEqual(
            calls["frame_join"][1],
            {
                "left_on": "day",
                "right_on": "day_index",
                "strategy": "backward",
                "tolerance": 5,
                "allow_exact_matches": True,
            },
        )
        self.assertEqual(
            calls["frame_select"],
            ("f.row_id AS row_id", "d.day_index AS matched_day_index", "d.week_index AS week_index"),
        )

        class Expr:
            def __init__(self, value):
                self.value = value

            def is_null(self):
                return Expr(("is_null", self.value))

            def is_not_null(self):
                return Expr(("is_not_null", self.value))

            def alias(self, name):
                return Expr(("alias", self.value, name))

        class LazyFrame:
            def __init__(self, label):
                self.label, self.steps = label, []

            def with_step(self, step):
                frame = LazyFrame(self.label)
                frame.steps = [*self.steps, step]
                return frame

            def filter(self, predicate):
                return self.with_step(("filter", predicate.value))

            def sort(self, key):
                return self.with_step(("sort", key))

            def join_asof(self, other, **kwargs):
                return self.with_step(("join_asof", other.label, kwargs))

            def select(self, *columns):
                return self.with_step(("select", columns))

        class Polars:
            Int64 = "Int64"

            @staticmethod
            def col(name):
                return Expr(("col", name))

            @staticmethod
            def lit(value, dtype=None):
                return Expr(("lit", value, dtype))

            @staticmethod
            def concat(frames):
                return ("concat", frames)

        pl_left, pl_right = LazyFrame("left"), LazyFrame("right")
        plan = polars_plan(node, (pl_left, pl_right), Polars)
        self.assertEqual(plan[0], "concat")
        matched, null_left = plan[1]
        self.assertEqual(matched.steps[0], ("filter", ("is_not_null", ("col", "day"))))
        self.assertEqual(matched.steps[1], ("sort", "day"))
        self.assertEqual(matched.steps[2][0], "join_asof")
        self.assertEqual(
            matched.steps[2][2],
            {
                "left_on": "day",
                "right_on": "matched_day_index",
                "strategy": "backward",
                "tolerance": 5,
                "allow_exact_matches": True,
            },
        )
        self.assertEqual(null_left.steps[0], ("filter", ("is_null", ("col", "day"))))

    def test_rejects_ambiguous_or_invalid_contracts_and_duplicate_right_key(self):
        left, right = inputs()
        for direction, exact, tolerance in (
            ("nearest", True, None),
            ("backward", 1, None),
            ("forward", True, -1),
            ("forward", True, True),
        ):
            with self.subTest(direction=direction, exact=exact, tolerance=tolerance):
                with self.assertRaises((TypeError, ValueError)):
                    asof(
                        left,
                        right,
                        left_key="day",
                        right_key="day_index",
                        direction=direction,
                        exact=exact,
                        tolerance=tolerance,
                    )
        node = asof(
            left,
            right,
            left_key="day",
            right_key="day_index",
            direction="backward",
            exact=True,
            tolerance=None,
        )
        with self.assertRaisesRegex(ValueError, "unique"):
            reference_rows(
                node,
                [{"row_id": 1, "day": 1}],
                [{"day_index": 1, "week_index": 0}, {"day_index": 1, "week_index": 0}],
            )


if __name__ == "__main__":
    unittest.main()
