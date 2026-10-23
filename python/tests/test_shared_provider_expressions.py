from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import shardloom as sl
from shardloom.query import ColumnExpression, WorkflowSource


class SharedProviderExpressionTests(unittest.TestCase):
    def test_temporal_and_domain_wrappers_preserve_expression_scope(self) -> None:
        source = WorkflowSource("vortex", "events.vortex")
        limits = sl.ExecutionResourceLimits(memory_bytes=1 << 20, max_parallelism=2)
        expression = ColumnExpression("event_time", (source,), limits)
        cases = (
            ("extract_minute", "EXTRACT_MINUTE(event_time)"),
            ("date_trunc_minute", "DATE_TRUNC_MINUTE(event_time)"),
            ("url_domain", "URL_DOMAIN(event_time)"),
            ("regex_domain", "REGEX_DOMAIN(event_time)"),
        )

        for method_name, expected_sql in cases:
            with self.subTest(method=method_name):
                result = getattr(expression, method_name)()
                self.assertEqual(result.sql, expected_sql)
                self.assertEqual(result.source_bindings, (source,))
                self.assertIs(result.resource_limits, limits)


if __name__ == "__main__":
    unittest.main()
