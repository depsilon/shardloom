#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Reference dialect checks; explicit opt-in runs real independent SQL fixtures."""

import json
import os
from pathlib import Path
import tempfile
import unittest

from clickbench_independent_reference import (
    INTEGER_AGGREGATE_COLUMNS, ORDINAL, dialect_expression, execute_plan, query_plan,
    split_projection, validate_arithmetic_schema,
)

QUERIES = Path(__file__).resolve().parents[1] / "benchmarks/clickbench/queries.sql"


class IndependentPlanTests(unittest.TestCase):
    def test_integer_arithmetic_rejects_fractional_or_missing_source_types(self):
        columns = dict.fromkeys(INTEGER_AGGREGATE_COLUMNS, "BIGINT")
        validate_arithmetic_schema(columns)
        for invalid in ("DOUBLE", "FLOAT", "DECIMAL(18,2)", "VARCHAR", None):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(ValueError, "aggregate schema"):
                validate_arithmetic_schema(dict(columns, UserID=invalid))

    def test_exact_query_identity_and_complete_projection_manifest(self):
        plans = query_plan(QUERIES)
        self.assertEqual([p["query_id"] for p in plans], list(range(1, 44)))
        self.assertEqual(plans[2]["output_names"],
                         ["sum_AdvEngineID_0", "count_all_1", "avg_ResolutionWidth_2"])
        self.assertEqual(plans[29]["output_names"], [f"sum_ResolutionWidth_{i}" for i in range(90)])
        self.assertEqual(plans[34]["output_names"], ["literal_1", "URL", "c"])
        self.assertEqual(plans[35]["output_names"],
                         ["ClientIP", "ClientIP_minus1", "ClientIP_minus2", "ClientIP_minus3", "c"])
        for q in (1, 2, 21):
            self.assertEqual(plans[q - 1]["output_names"], ["count_all"])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "queries.sql"
            path.write_bytes(QUERIES.read_bytes() + b"\n")
            with self.assertRaisesRegex(ValueError, "exact pinned"):
                query_plan(path)

    def test_projection_quotes_and_nested_commas_are_not_rewritten(self):
        raw = "regexp_replace(x, 'a,''b', '\\1') AS k, COUNT(*), x"
        self.assertEqual(split_projection(raw),
                         ["regexp_replace(x, 'a,''b', '\\1') AS k", "COUNT(*)", "x"])
        for raw in ("x,", "sum(x", "sum(x))", "'x"):
            with self.assertRaises(ValueError):
                split_projection(raw)

    def test_group_and_absolute_source_order_are_frozen_before_results(self):
        plans = query_plan(QUERIES)
        self.assertIn('ORDER BY MIN("' + ORDINAL + '") LIMIT 10', plans[17]["equivalent_sql"])
        self.assertIn('ORDER BY COUNT(*) DESC, "UserID" ASC NULLS FIRST, "m" ASC NULLS FIRST, '
                      '"SearchPhrase" ASC NULLS FIRST LIMIT 10', plans[18]["equivalent_sql"])
        self.assertIn('ORDER BY EventTime, "' + ORDINAL + '" ASC LIMIT 10', plans[23]["equivalent_sql"])
        self.assertIn('* EXCLUDE ("' + ORDINAL + '")', plans[23]["equivalent_sql"])
        self.assertIn("EventDate >= 15887", plans[36]["equivalent_sql"])
        self.assertIn("octet_length(encode(URL))", plans[27]["equivalent_sql"])

    def test_schema_changes_and_nonfinite_values_fail_without_repair(self):
        class Result:
            def __init__(self, names, rows):
                self.description = [(name, "DOUBLE") for name in names]
                self.rows = rows

            def execute(self, _sql):
                return self

            def fetchall(self):
                return self.rows

        plan = dict(query_id=1, statement="pinned", equivalent_sql="SELECT", output_names=["value"])
        for result in (Result(["renamed"], [(1.0,)]), Result(["value", "value"], [(1.0, 2.0)]),
                       Result(["value"], [(float("nan"),)]), Result(["value"], [(float("inf"),)])):
            with self.assertRaises(ValueError):
                execute_plan(result, plan)
        record = execute_plan(Result(["value"], [(None,), (1.0,), (-0.0,)]), plan)
        self.assertEqual(json.dumps(record["values"]), '[{"value": null}, {"value": 1.0}, {"value": -0.0}]')


@unittest.skipUnless(os.environ.get("SHARDLOOM_TEST_INDEPENDENT_SQL") == "1",
                     "explicit independent DuckDB fixture environment required")
class IndependentSqlTests(unittest.TestCase):
    def setUp(self):
        # An explicitly requested lane without the oracle is a failure, not a skip.
        import duckdb
        self.connection = duckdb.connect(config={"threads": 1, "memory_limit": "256MiB",
            "autoinstall_known_extensions": False, "autoload_known_extensions": False,
            "default_null_order": "NULLS_FIRST"})
        self.plans = query_plan(QUERIES)

    def tearDown(self):
        self.connection.close()

    def values(self, query):
        return execute_plan(self.connection, self.plans[query - 1])["values"]

    def test_empty_null_signed_values_and_widened_arithmetic(self):
        self.connection.execute("CREATE TABLE hits (AdvEngineID SMALLINT, ResolutionWidth SMALLINT, UserID BIGINT)")
        self.assertEqual(self.values(3), [{"sum_AdvEngineID_0": None, "count_all_1": 0,
                                          "avg_ResolutionWidth_2": None}])
        self.connection.execute("INSERT INTO hits VALUES (NULL,NULL,NULL), (-3,32767,-7), (7,1,3)")
        self.assertEqual(self.values(3), [{"sum_AdvEngineID_0": 4.0, "count_all_1": 3,
                                          "avg_ResolutionWidth_2": 16384.0}])
        self.assertEqual(self.values(4), [{"avg_UserID_0": -2.0}])
        sums = self.values(30)[0]
        self.assertEqual([sums[f"sum_ResolutionWidth_{i}"] for i in range(90)],
                         [32768.0 + 2 * i for i in range(90)])

    def test_large_integer_average_uses_exact_total_of_converted_inputs(self):
        self.connection.execute("CREATE TABLE hits (UserID BIGINT)")
        values = [10**18, 1, -(10**18), None]
        self.connection.executemany("INSERT INTO hits VALUES (?)", [(value,) for value in values])
        expected = float(sum(int(float(value)) for value in values if value is not None)) / 3
        self.assertEqual(self.values(4), [{"avg_UserID_0": expected}])
        ordinary = self.connection.execute("SELECT AVG(CAST(UserID AS DOUBLE)) FROM hits").fetchone()[0]
        self.assertNotEqual(ordinary, expected)
        self.connection.execute("DELETE FROM hits")
        self.connection.execute("INSERT INTO hits VALUES (NULL),(NULL)")
        self.assertEqual(self.values(4), [{"avg_UserID_0": None}])

    def test_integer_oracle_preserves_initial_conversion_and_final_f64_rounding(self):
        self.connection.execute("CREATE TABLE hits (UserID UBIGINT)")
        values = [2**64 - 1, 2**53 + 3, 7, None]
        self.connection.executemany("INSERT INTO hits VALUES (?)", [(value,) for value in values])
        converted = [int(float(value)) for value in values if value is not None]
        expected = float(sum(converted)) / len(converted)
        self.assertEqual(self.values(4), [{"avg_UserID_0": expected}])
        self.assertNotEqual(sum(converted), sum(value for value in values if value is not None))

    def test_raw_sort_source_ties_and_group_key_ties_are_distinct(self):
        self.connection.execute(f"CREATE TABLE hits (UserID BIGINT, SearchPhrase VARCHAR, "
                                f"EventTime BIGINT, URL VARCHAR, {ORDINAL} BIGINT)")
        self.connection.execute("INSERT INTO hits VALUES (9,'z',60,'google-z',0), "
                                "(2,'a',60,'google-a',1), (9,'z',120,'google-zz',2), "
                                "(2,'a',120,'google-aa',3)")
        self.assertEqual(self.values(18), [{"UserID": 9, "SearchPhrase": "z", "count_all_2": 2},
                                         {"UserID": 2, "SearchPhrase": "a", "count_all_2": 2}])
        self.assertEqual(self.values(17), [{"UserID": 2, "SearchPhrase": "a", "count_all_2": 2},
                                         {"UserID": 9, "SearchPhrase": "z", "count_all_2": 2}])
        self.assertEqual([r["URL"] for r in self.values(24)], ["google-z", "google-a", "google-zz", "google-aa"])
        self.assertTrue(all(ORDINAL not in row for row in self.values(24)))
        self.assertEqual(self.values(25), [{"SearchPhrase": x} for x in ("z", "a", "z", "a")])

    def test_byte_length_regex_and_negative_integer_epochs(self):
        self.assertEqual(self.connection.execute("SELECT " + dialect_expression("length(value)")
                         + " FROM (VALUES ('é☃')) AS text(value)").fetchone(), (5,))
        expression = dialect_expression("extract(minute FROM EventTime)")
        truncation = dialect_expression("DATE_TRUNC('minute', EventTime)")
        self.connection.execute("CREATE TABLE epochs(EventTime BIGINT)")
        self.connection.execute("INSERT INTO epochs VALUES (-61),(-60),(-1),(0),(59),(60),(9007199254740991)")
        actual = self.connection.execute(f"SELECT EventTime, {expression}, {truncation} FROM epochs").fetchall()
        for stamp, minute, truncated in actual:
            self.assertEqual((minute, truncated), ((stamp // 60) % 60, (stamp // 60) * 60))
        self.connection.execute("CREATE TABLE hits AS SELECT 1 AS CounterID, 'https://www.é.fr/a' AS Referer, "
                                "'é☃' AS URL FROM range(100001)")
        self.assertEqual(self.values(28), [{"CounterID": 1, "l": 5.0, "c": 100001}])
        self.assertEqual(self.values(29), [{"k": "é.fr", "l": float(len('https://www.é.fr/a'.encode())),
                                          "c": 100001, "min_Referer_3": "https://www.é.fr/a"}])

    def test_all_43_equivalents_bind_on_an_independent_empty_schema(self):
        strings = {"SearchPhrase", "MobilePhoneModel", "URL", "Title", "Referer"}
        columns = ("AdvEngineID ResolutionWidth UserID SearchPhrase EventDate RegionID MobilePhoneModel "
                   "MobilePhone SearchEngineID EventTime URL Title Referer CounterID ClientIP WatchID "
                   "IsRefresh DontCountHits IsLink IsDownload TraficSourceID URLHash RefererHash "
                   "WindowClientWidth WindowClientHeight").split()
        fields = [f'"{name}" {"VARCHAR" if name in strings else "BIGINT"}' for name in columns]
        fields.append(f'"{ORDINAL}" BIGINT')
        self.connection.execute("CREATE TABLE hits (" + ", ".join(fields) + ")")
        scalar = {1, 2, 3, 4, 5, 6, 7, 21, 30}
        for query in range(1, 44):
            with self.subTest(query=query):
                values = self.values(query)
                self.assertEqual(len(values), int(query in scalar))
        self.connection.execute("INSERT INTO hits (ClientIP) VALUES (-2147483648),(2147483647)")
        self.assertEqual(self.values(36), [
            {"ClientIP": -2147483648, "ClientIP_minus1": -2147483649,
             "ClientIP_minus2": -2147483650, "ClientIP_minus3": -2147483651, "c": 1},
            {"ClientIP": 2147483647, "ClientIP_minus1": 2147483646,
             "ClientIP_minus2": 2147483645, "ClientIP_minus3": 2147483644, "c": 1},
        ])


if __name__ == "__main__":
    unittest.main()
