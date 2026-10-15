# SPDX-License-Identifier: Apache-2.0
import csv
import tempfile
import unittest
from pathlib import Path

from query_timing_report import (
    TIMING_BOUNDARY, TIMING_CLOCK, query_phase_timings, timing_totals, write_timing_csv,
)


def measured_fields():
    return dict(native_query_timing_reported="true", native_query_execution_nanos="600000000",
                native_result_serialization_nanos="20000000", native_query_timing_clock=TIMING_CLOCK,
                native_query_timing_boundary=TIMING_BOUNDARY)


class QueryTimingTests(unittest.TestCase):
    def test_disjoint_engine_sink_and_process_overhead_reconcile(self):
        values = query_phase_timings(measured_fields(), 0.8)
        self.assertEqual(values["engine_execution_seconds"], 0.6)
        self.assertEqual(values["result_serialization_seconds"], 0.02)
        self.assertAlmostEqual(values["process_other_seconds"], 0.18)
        self.assertAlmostEqual(sum(values[key] for key in (
            "engine_execution_seconds", "result_serialization_seconds", "process_other_seconds")), 0.8)

    def test_missing_clocks_are_refused_unless_explicit_historical_comparison(self):
        for fields in ({}, {"native_query_timing_reported": "false"}):
            with self.subTest(fields=fields):
                with self.assertRaisesRegex(ValueError, "timing is missing"):
                    query_phase_timings(fields, 0.8)
                values = query_phase_timings(fields, 0.8, allow_missing=True)
                self.assertFalse(values["engine_timing_reported"])
                self.assertIsNone(values["engine_execution_seconds"])
                self.assertIsNone(values["process_other_seconds"])

    def test_malformed_or_inconsistent_clocks_never_become_engine_evidence(self):
        invalid = [None, True, -1, 4.5, "-1", "1.0", "NaN", "inf", "１２", str(1 << 64)]
        for value in invalid:
            with self.subTest(value=value), self.assertRaises(ValueError):
                query_phase_timings(measured_fields() | {"native_query_execution_nanos": value}, 0.8,
                                    allow_missing=True)
        for value in (float("nan"), float("inf"), -1, True):
            with self.subTest(value=value), self.assertRaises(ValueError):
                query_phase_timings(measured_fields(), value)
        with self.assertRaisesRegex(ValueError, "exceed"):
            query_phase_timings(measured_fields(), 0.5)
        with self.assertRaisesRegex(ValueError, "boundary"):
            query_phase_timings(measured_fields() | {"native_query_timing_clock": "cpu"}, 0.8)

    def test_totals_preserve_missing_metrics_and_all_raw_samples(self):
        values = query_phase_timings(measured_fields(), 0.8)
        totals = timing_totals([values, values])
        self.assertEqual(totals["raw_calls"], 2)
        self.assertEqual(totals["calls_with_engine_timing"], 2)
        self.assertEqual(totals["totals"]["engine_execution_seconds"], 1.2)
        self.assertIsNone(totals["totals"]["user_cpu_seconds"])
        mixed = timing_totals([values, query_phase_timings({}, 1, allow_missing=True)])
        self.assertIsNone(mixed["totals"]["engine_execution_seconds"])
        self.assertEqual(mixed["totals"]["process_wall_seconds"], 1.8)

    def test_csv_preserves_every_query_and_quoted_sql(self):
        records = [dict(query=index, run=1, passed=True,
                        query_statement='SELECT "a,b" FROM hits\nLIMIT 1',
                        **query_phase_timings(measured_fields(), 0.8))
                   for index in range(1, 44)]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "query-timings.csv"
            write_timing_csv(path, records)
            with path.open(newline="") as stream:
                actual = list(csv.DictReader(stream))
        self.assertEqual([int(row["query"]) for row in actual], list(range(1, 44)))
        self.assertEqual(actual[0]["query_statement"], records[0]["query_statement"])
        self.assertEqual(actual[0]["user_cpu_seconds"], "")


if __name__ == "__main__":
    unittest.main()
