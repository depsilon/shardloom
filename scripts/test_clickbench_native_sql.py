# SPDX-License-Identifier: Apache-2.0
"""The native benchmark dialect preserves the independent workload contract."""

import hashlib
from pathlib import Path
import unittest

from clickbench_native_sql import PINNED_QUERIES_SHA256, native_query_statements
from clickbench_reference_packet import query_statements


class NativeClickBenchSqlTests(unittest.TestCase):
    def setUp(self):
        raw, self.original = query_statements(
            Path(__file__).resolve().parents[1] / "benchmarks/clickbench/queries.sql"
        )
        self.source_hash = hashlib.sha256(raw).hexdigest()
        self.assertEqual(self.source_hash, PINNED_QUERIES_SHA256)

    def test_only_byte_length_spelling_and_implicit_output_alias_change(self):
        submitted, receipt = native_query_statements(self.original, self.source_hash)
        self.assertEqual(receipt["changed_query_ids"], [28, 29])
        self.assertEqual(receipt["translation"], "explicit_utf8_byte_length")
        self.assertEqual(len(submitted), 43)
        for index, (before, after) in enumerate(zip(self.original, submitted), 1):
            with self.subTest(query=index):
                if index in (28, 29):
                    restored = after.replace("octet_length(", "length(")
                    if index == 29:
                        restored = restored.replace("MIN(Referer) AS min_Referer_3", "MIN(Referer)")
                    self.assertEqual(restored, before)
                else:
                    self.assertEqual(after, before)
        # The byte-sensitive independent result keeps its exact field name.
        self.assertIn("MIN(Referer) AS min_Referer_3", submitted[28])
        self.assertIn(r"'^https?://(?:www\.)?([^/]+)/.*$', '\1'", submitted[28])
        self.assertEqual(self.original[27].count("AVG(length(URL))"), 1)

    def test_custom_sql_is_unchanged_including_literal_and_identifier_spelling(self):
        queries = ["SELECT length('é☃') AS length", "SELECT 'length(URL)' AS text"] * 21
        queries.append("SELECT byte_length(Referer) FROM hits")
        submitted, receipt = native_query_statements(queries, "custom-query-file")
        self.assertEqual(submitted, queries)
        self.assertIsNot(submitted, queries)
        self.assertEqual(receipt["translation"], "as_supplied")
        self.assertEqual(receipt["changed_query_ids"], [])

    def test_mismatched_pinned_statement_or_count_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "43 statements"):
            native_query_statements(self.original[:-1], self.source_hash)
        for query in (28, 29):
            changed = list(self.original)
            changed[query - 1] += " "
            with self.subTest(query=query), self.assertRaisesRegex(ValueError, f"statement {query}"):
                native_query_statements(changed, self.source_hash)

    def test_receipt_binds_the_exact_submitted_statements(self):
        first, receipt = native_query_statements(self.original, self.source_hash)
        second, repeated = native_query_statements(self.original, self.source_hash)
        self.assertEqual((first, receipt), (second, repeated))
        _, unchanged = native_query_statements(self.original, "different-query-file")
        self.assertNotEqual(receipt["submitted_statements_sha256"], unchanged["submitted_statements_sha256"])


if __name__ == "__main__":
    unittest.main()
