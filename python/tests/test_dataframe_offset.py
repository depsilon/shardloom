from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import shardloom as sl
from shardloom.query import WorkflowOperation


class DataFrameLimitOffsetTests(unittest.TestCase):
    def setUp(self) -> None:
        # Rendering a declaration must not need a CLI or execution backend.
        self.source = sl.read_vortex("events.vortex", client=SimpleNamespace())

    def test_limit_validates_types_nonnegative_values_and_native_usize_range(self) -> None:
        for count in (True, 1.5, "1"):
            with self.subTest(count=count), self.assertRaises(TypeError):
                self.source.limit(count)  # type: ignore[arg-type]
        for offset in (True, 1.5, "1"):
            with self.subTest(offset=offset), self.assertRaises(TypeError):
                self.source.limit(1, offset=offset)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            self.source.limit(-1)
        with self.assertRaises(ValueError):
            self.source.limit(1, offset=-1)

        native_usize_max = (1 << 64) - 1
        self.assertEqual(
            self.source.limit(native_usize_max, offset=0).operations[-1].values,
            (str(native_usize_max),),
        )
        self.assertEqual(
            self.source.limit(1, offset=native_usize_max - 1).operations[-1].values,
            ("1", str(native_usize_max - 1)),
        )
        for count, offset in (
            (native_usize_max + 1, 0),
            (1, native_usize_max),
            (native_usize_max, 1),
        ):
            with self.subTest(count=count, offset=offset), self.assertRaises(ValueError):
                self.source.limit(count, offset=offset)

    def test_zero_offset_keeps_the_existing_one_value_operation_and_summary(self) -> None:
        self.assertEqual(
            self.source.limit(5).operations[-1],
            WorkflowOperation("limit", ("5",)),
        )
        explicit_zero = self.source.limit(5, offset=0)
        self.assertEqual(explicit_zero.operations[-1], WorkflowOperation("limit", ("5",)))
        self.assertEqual(explicit_zero.operations[-1].to_summary(), "limit(5)")
        statement = explicit_zero._native_relational_statement()
        self.assertEqual(statement, "SELECT * FROM 'events.vortex' LIMIT 5")
        self.assertNotIn("OFFSET", statement or "")

    def test_flat_sort_then_limit_renders_offset_and_summary(self) -> None:
        frame = self.source.sort("id").limit(3, offset=4)
        self.assertEqual(
            frame.operations[-1].to_summary(),
            "limit(3, offset=4)",
        )
        self.assertEqual(
            frame._native_relational_statement(),
            "SELECT * FROM 'events.vortex' ORDER BY id ASC LIMIT 3 OFFSET 4",
        )

    def test_aggregate_sort_then_offset_uses_flat_sql_with_ordered_stages(self) -> None:
        frame = (
            self.source.group_by("category")
            .agg(total="sum(sample)")
            .sort("total", descending=True)
            .limit(4, offset=7)
        )
        self.assertEqual(
            frame._native_relational_statement(),
            "SELECT category,sum(sample) AS total FROM 'events.vortex' "
            "GROUP BY category ORDER BY total DESC LIMIT 4 OFFSET 7",
        )

    def test_repeated_limits_keep_each_range_as_a_nested_stage(self) -> None:
        frame = self.source.limit(10, offset=3).limit(4, offset=2)
        self.assertEqual(
            frame._native_relational_statement(),
            "SELECT * FROM (SELECT * FROM (SELECT * FROM 'events.vortex') "
            "AS _sl_stage_0 LIMIT 10 OFFSET 3) AS _sl_stage_1 LIMIT 4 OFFSET 2",
        )

    def test_filter_and_sort_after_range_remain_after_the_offset_stage(self) -> None:
        frame = (
            self.source.limit(4, offset=2)
            .filter(sl.col("id") > 3)
            .sort("id", descending=True)
        )
        self.assertEqual(
            frame._native_relational_statement(),
            "SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM "
            "'events.vortex') AS _sl_stage_0 LIMIT 4 OFFSET 2) AS _sl_stage_1 "
            "WHERE id > 3) AS _sl_stage_2 ORDER BY id DESC",
        )

    def test_zero_count_with_nonzero_offset_is_not_elided(self) -> None:
        frame = self.source.limit(0, offset=5)
        self.assertEqual(
            frame._native_relational_statement(),
            "SELECT * FROM 'events.vortex' LIMIT 0 OFFSET 5",
        )

    def test_aggregate_after_offset_keeps_input_range_and_declines_unrenderable_legacy_route(self) -> None:
        frame = self.source.limit(4, offset=2)
        grouped = frame.group_by("category").agg(rows="count(*)")
        statement = grouped._native_relational_statement()
        self.assertIn("LIMIT 4 OFFSET 2)", statement or "")
        self.assertIn("GROUP BY category", statement or "")
        # An older command payload cannot represent this input range. If the
        # complete relation cannot be rendered, neither aggregate may drop it.
        with patch.object(type(frame), "_relation_statement", return_value=None):
            self.assertFalse(frame._can_append_group_by_aggregate(("category",)))
            self.assertFalse(frame._can_append_scalar_aggregate())


if __name__ == "__main__":
    unittest.main()
