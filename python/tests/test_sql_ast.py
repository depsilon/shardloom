# SPDX-License-Identifier: Apache-2.0
"""Tests for the unified Python SQL lexer and optional native AST bridge."""

from __future__ import annotations

import json
import sys
import types
import unittest
from unittest import mock

from shardloom import _sql_ast


class SqlAstHelpersTest(unittest.TestCase):
    def test_ignores_comments_and_escaped_quotes(self) -> None:
        sql = (
            "-- FROM 'commented.csv'\n"
            "SELECT 'FROM ''fake.csv''' AS txt, EXTRACT(YEAR FROM ts) AS yr, "
            "a IS NOT DISTINCT FROM 'other.csv' /* JOIN 'block.csv' */\n"
            "FROM 'real_data.vortex' AS t\n"
            "WHERE a BETWEEN 1 AND 5 AND b = 'x)y' LIMIT 10"
        )
        self.assertEqual(_sql_ast.extract_source_refs(sql), ("real_data.vortex",))
        self.assertTrue(_sql_ast.starts_with_keyword(sql, "select"))
        self.assertEqual(
            _sql_ast.find_top_level_phrase_span(sql, "FROM"),
            (sql.index("FROM 'real_data.vortex'"), sql.index("FROM 'real_data.vortex'") + 4),
        )
        self.assertIsNotNone(_sql_ast.find_top_level_keyword(sql, "limit"))
        self.assertEqual(
            _sql_ast.find_top_level_keyword("a BETWEEN 1 AND 5 AND b = 2", "and"),
            len("a BETWEEN 1 AND 5 "),
        )

    def test_handles_ctes_values_and_generators(self) -> None:
        cte_sql = (
            "WITH prep AS (SELECT id FROM 'left.vortex') "
            "SELECT * FROM prep JOIN 'right.vortex' ON prep.id = 'right.vortex'.id"
        )
        self.assertEqual(_sql_ast.extract_source_refs(cte_sql), ("left.vortex", "right.vortex"))
        self.assertEqual(_sql_ast.extract_source_refs("VALUES (1, 'FROM ''x''') LIMIT 5"), ())
        self.assertEqual(
            _sql_ast.extract_source_refs("SELECT value FROM generate_series(1, 5)"),
            (),
        )

    def test_splits_cast_and_function_csv_arguments(self) -> None:
        args = _sql_ast.split_top_level_csv(
            "a, CONCAT(b, ',', 'x''y'), arr[1, 2], CAST(z AS DECIMAL(10, 2))"
        )
        self.assertEqual(
            args,
            ("a", "CONCAT(b, ',', 'x''y')", "arr[1, 2]", "CAST(z AS DECIMAL(10, 2))"),
        )
        _sql_ast.validate_balanced_parentheses("CAST('(' || ')' AS UTF8)")

    def test_rejects_unbalanced_or_unclosed_inputs(self) -> None:
        with self.assertRaisesRegex(ValueError, "unbalanced parentheses"):
            _sql_ast.validate_balanced_parentheses("SUM(x))")
        with self.assertRaisesRegex(ValueError, "unclosed string literal"):
            _sql_ast.validate_balanced_parentheses("'unclosed")
        with self.assertRaisesRegex(ValueError, "empty argument"):
            _sql_ast.split_top_level_csv("a, ")

    def test_uses_loaded_native_inspect_sql_json_when_available(self) -> None:
        fake_module = types.ModuleType("shardloom._native")
        fake_module.inspect_sql_json = lambda sql: json.dumps(
            {
                "statement_kind": "select",
                "source_refs": [
                    {"reference": "orders.vortex", "quoted_path": True},
                    {"reference": "cte_alias", "quoted_path": False},
                ],
                "is_relational_candidate": False,
                "has_top_level_limit": True,
                "top_level_limit": "LIMIT 5",
                "is_source_free": False,
                "is_generator": False,
            }
        )
        with mock.patch.dict(sys.modules, {"shardloom._native": fake_module}):
            self.assertEqual(
                _sql_ast.extract_source_refs("SELECT * FROM 'ignored_when_mocked.vortex'"),
                ("orders.vortex",),
            )
        try:
            native_mod = __import__("shardloom._native", fromlist=["inspect_sql_json"])
        except ImportError:
            native_mod = None
        if native_mod is not None and hasattr(native_mod, "inspect_sql_json"):
            payload = _sql_ast.inspect_sql_native("SELECT 1 LIMIT 2")
            self.assertIsNotNone(payload)
            assert payload is not None
            self.assertEqual(payload["statement_kind"], "select")
            self.assertTrue(payload["is_source_free"])
            self.assertTrue(payload["has_top_level_limit"])


if __name__ == "__main__":
    unittest.main()
