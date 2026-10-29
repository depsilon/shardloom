# SPDX-License-Identifier: Apache-2.0
"""The native benchmark dialect preserves the independent workload contract."""

import hashlib
from datetime import date
from pathlib import Path
import re
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
        submitted, receipt = native_query_statements(self.original, self.source_hash, input_format="parquet")
        self.assertEqual(receipt["changed_query_ids"],
                         [query for query in range(1, 44) if query not in (1, 2, 20, 21, 24, 25, 26, 27)])
        self.assertEqual(receipt["byte_length_query_ids"], [28, 29])
        self.assertEqual(receipt["translation"], "explicit_byte_length_epoch_types_output_names_and_group_ties")
        self.assertEqual(receipt["integer_epoch_query_ids"], [19, 37, 38, 39, 40, 41, 42, 43])
        self.assertEqual(len(submitted), 43)
        for index, (before, after) in enumerate(zip(self.original, submitted), 1):
            with self.subTest(query=index):
                restored = re.sub(r", [A-Za-z_0-9]+ ASC NULLS FIRST", "", after)
                restored = restored.replace("octet_length(", "length(")
                restored = re.sub(
                    r" AS (?:(?:sum|avg|min|max|count)_[A-Za-z_]+_[0-9]+|literal_1|ClientIP_minus[123])\b",
                    "", restored)
                if index == 19:
                    restored = restored.replace("extract_minute(EventTime)", "extract(minute FROM EventTime)")
                if index == 43:
                    restored = restored.replace("GROUP BY M ORDER BY M",
                        "GROUP BY date_trunc_minute(EventTime) ORDER BY date_trunc_minute(EventTime)")
                    restored = restored.replace("date_trunc_minute(EventTime)", "DATE_TRUNC('minute', EventTime)")
                if 37 <= index <= 43:
                    for literal in ("2013-07-01", "2013-07-31", "2013-07-14", "2013-07-15"):
                        days = (date.fromisoformat(literal) - date(1970, 1, 1)).days
                        restored = restored.replace(f"EventDate >= {days}", f"EventDate >= '{literal}'")
                        restored = restored.replace(f"EventDate <= {days}", f"EventDate <= '{literal}'")
                self.assertEqual(restored, before)
        # The byte-sensitive independent result keeps its exact field name.
        self.assertIn("MIN(Referer) AS min_Referer_3", submitted[28])
        self.assertIn(r"'^https?://(?:www\.)?([^/]+)/.*$', '\1'", submitted[28])
        self.assertEqual(self.original[27].count("AVG(length(URL))"), 1)
        self.assertEqual(submitted[2], "SELECT SUM(AdvEngineID) AS sum_AdvEngineID_0, "
                         "COUNT(*) AS count_all_1, AVG(ResolutionWidth) AS avg_ResolutionWidth_2 FROM hits")
        self.assertEqual(submitted[29].count(" AS sum_ResolutionWidth_"), 90)

    def test_group_ties_match_independent_contract_without_changing_source_order_queries(self):
        from clickbench_independent_reference import GROUP_KEYS
        submitted, receipt = native_query_statements(self.original, self.source_hash, input_format="parquet")
        self.assertEqual(receipt["group_tie_query_ids"], sorted(set(GROUP_KEYS) - {18, 43}))
        for query, keys in GROUP_KEYS.items():
            if query in (18, 43):
                continue
            ordering = submitted[query - 1].partition(" LIMIT ")[0]
            self.assertTrue(ordering.endswith(", " + ", ".join(key + " ASC NULLS FIRST" for key in keys)))
        self.assertNotIn("ORDER BY", submitted[17])
        for query in (20, 24, 25, 26, 27):
            self.assertEqual(submitted[query - 1], self.original[query - 1])

    def test_custom_sql_is_unchanged_including_literal_and_identifier_spelling(self):
        queries = ["SELECT length('é☃') AS length", "SELECT 'length(URL)' AS text"] * 21
        queries.append("SELECT byte_length(Referer) FROM hits")
        submitted, receipt = native_query_statements(queries, "custom-query-file", input_format="parquet")
        self.assertEqual(submitted, queries)
        self.assertIsNot(submitted, queries)
        self.assertEqual(receipt["translation"], "as_supplied")
        self.assertEqual(receipt["changed_query_ids"], [])

    def test_vortex_keeps_existing_submission_and_unsupported_formats_fail(self):
        submitted, receipt = native_query_statements(self.original, self.source_hash)
        self.assertEqual(receipt["changed_query_ids"], [28, 29])
        self.assertEqual(receipt["translation"], "explicit_utf8_byte_length")
        for query in set(range(1, 44)) - {28, 29}:
            self.assertEqual(submitted[query - 1], self.original[query - 1])
        self.assertEqual(receipt["group_tie_query_ids"], [])
        with self.assertRaisesRegex(ValueError, "input format"):
            native_query_statements(self.original, self.source_hash, input_format="csv")

    def test_mismatched_pinned_statement_or_count_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "43 statements"):
            native_query_statements(self.original[:-1], self.source_hash)
        for query in (28, 29):
            changed = list(self.original)
            changed[query - 1] += " "
            with self.subTest(query=query), self.assertRaisesRegex(ValueError, f"statement {query}"):
                native_query_statements(changed, self.source_hash)
        for query in (3, 20, 30, 36, 43):
            changed = list(self.original)
            changed[query - 1] += " "
            with self.subTest(query=query), self.assertRaisesRegex(ValueError, "statement vector"):
                native_query_statements(changed, self.source_hash)

    def test_receipt_binds_the_exact_submitted_statements(self):
        first, receipt = native_query_statements(self.original, self.source_hash)
        second, repeated = native_query_statements(self.original, self.source_hash)
        self.assertEqual((first, receipt), (second, repeated))
        _, unchanged = native_query_statements(self.original, "different-query-file")
        self.assertNotEqual(receipt["submitted_statements_sha256"], unchanged["submitted_statements_sha256"])


if __name__ == "__main__":
    unittest.main()
