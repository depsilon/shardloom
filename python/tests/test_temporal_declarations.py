from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from shardloom import ExecutionResourceLimits
from shardloom.query import ColumnExpression, WorkflowSource


class TemporalDeclarationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.source_a = WorkflowSource("vortex", "events.vortex")
        self.source_b = WorkflowSource("csv", "offsets.csv")
        self.limits_a = ExecutionResourceLimits(memory_bytes=96 << 20, max_parallelism=2)
        self.limits_b = ExecutionResourceLimits(memory_bytes=64 << 20, max_parallelism=4)
        self.value = ColumnExpression("event_time", (self.source_a,), self.limits_a)
        self.other = ColumnExpression("delta", (self.source_b,), self.limits_b)

    def test_methods_lower_to_shared_temporal_registry_calls(self) -> None:
        cases = (
            (self.value.temporal_extract("hour"), "TEMPORAL_EXTRACT(event_time, 'hour')"),
            (self.value.temporal_trunc("day"), "TEMPORAL_TRUNC(event_time, 'day', 'reject')"),
            (self.value.temporal_trunc("minute", ambiguity="earlier"),
             "TEMPORAL_TRUNC(event_time, 'minute', 'earlier')"),
            (self.value.calendar_add_days(self.other),
             "CALENDAR_ADD_DAYS(event_time, delta, 'reject')"),
            (self.value.calendar_add_months(2),
             "CALENDAR_ADD_MONTHS(event_time, 2, 'reject', 'reject')"),
            (self.value.calendar_add_months(-1, month_end="clamp", ambiguity="later"),
             "CALENDAR_ADD_MONTHS(event_time, -1, 'clamp', 'later')"),
            (self.value.temporal_add_duration(self.other),
             "TEMPORAL_ADD_DURATION(event_time, delta)"),
            (self.value.temporal_sub_duration(1_000_000),
             "TEMPORAL_SUB_DURATION(event_time, 1000000)"),
            (self.value.timestamp_difference(self.other),
             "TIMESTAMP_DIFFERENCE(event_time, delta)"),
            (self.value.timestamp_assign_timezone("America/O'Brien"),
             "TIMESTAMP_ASSIGN_TIMEZONE(event_time, 'America/O''Brien', 'reject')"),
            (self.value.timestamp_assign_timezone("UTC", ambiguity="earlier"),
             "TIMESTAMP_ASSIGN_TIMEZONE(event_time, 'UTC', 'earlier')"),
            (self.value.timestamp_convert_timezone("America/Chicago"),
             "TIMESTAMP_CONVERT_TIMEZONE(event_time, 'America/Chicago')"),
            (self.value.timestamp_remove_timezone(), "TIMESTAMP_REMOVE_TIMEZONE(event_time)"),
        )
        for expression, expected in cases:
            with self.subTest(sql=expected):
                self.assertEqual(expression.sql, expected)

    def test_column_operands_preserve_source_bindings_and_resource_limits(self) -> None:
        expressions = (
            self.value.calendar_add_days(self.other),
            self.value.calendar_add_months(self.other),
            self.value.temporal_add_duration(self.other),
            self.value.temporal_sub_duration(self.other),
            self.value.timestamp_difference(self.other),
        )
        for expression in expressions:
            with self.subTest(sql=expression.sql):
                self.assertEqual(set(expression.source_bindings), {self.source_a, self.source_b})
                self.assertEqual(expression.resource_limits,
                                 ExecutionResourceLimits(memory_bytes=64 << 20, max_parallelism=2))

    def test_control_arguments_require_strings_but_values_are_not_domain_checked(self) -> None:
        for build in (
            lambda: self.value.temporal_extract(1),
            lambda: self.value.temporal_trunc("day", ambiguity=None),
            lambda: self.value.calendar_add_days(1, ambiguity=2),
            lambda: self.value.calendar_add_months(1, month_end=False),
            lambda: self.value.timestamp_assign_timezone(7),
            lambda: self.value.timestamp_convert_timezone(None),
        ):
            with self.subTest(build=build), self.assertRaises(TypeError):
                build()

        # Policy and part value admission is owned by the native expression binder.
        self.assertEqual(self.value.temporal_trunc("made-up", ambiguity="no-such-policy").sql,
                         "TEMPORAL_TRUNC(event_time, 'made-up', 'no-such-policy')")

    def test_non_column_values_use_existing_sql_literal_rules(self) -> None:
        self.assertEqual(self.value.calendar_add_days(4).sql,
                         "CALENDAR_ADD_DAYS(event_time, 4, 'reject')")
        self.assertEqual(self.value.temporal_add_duration("not validated here").sql,
                         "TEMPORAL_ADD_DURATION(event_time, 'not validated here')")
        with self.assertRaises(TypeError):
            self.value.temporal_add_duration(object())


if __name__ == "__main__":
    unittest.main()
