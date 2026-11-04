# SPDX-License-Identifier: Apache-2.0
"""The updated cohort preserves complete and common-correct timing denominators."""

import math
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "benchmarks/traditional_analytics"))
from query_surface.expanded_updated_replay import summarize


def observation(status, engine, workflow=4.0):
    return {"status": status, "engine_seconds": engine, "workflow_seconds": workflow}


class UpdatedCohortReplayTests(unittest.TestCase):
    def test_recovered_requests_contribute_only_to_the_complete_updated_total(self):
        rows = [
            {"id": "common", "family": "group", "original": observation("passed", 3.0),
             "updated": observation("passed", 2.0), "polars": observation("passed", None, 1.0)},
            {"id": "recovered", "family": "membership", "original": observation("engine_error", None),
             "updated": observation("passed", 5.0), "polars": observation("passed", None, 2.0)},
        ]
        result = summarize(rows)
        self.assertEqual(result["complete_engine_seconds"], 7.0)
        self.assertEqual(result["common_correct"]["cases"], 1)
        self.assertEqual(result["common_correct"]["original_engine_seconds"], 3.0)
        self.assertEqual(result["common_correct"]["updated_engine_seconds"], 2.0)
        self.assertEqual(result["recovered"], [{"id": "recovered", "engine_seconds": 5.0}])
        self.assertEqual(result["public_workflow"]["polars_seconds"], 3.0)

    def test_failed_updated_request_withholds_the_complete_total(self):
        rows = [
            {"id": "passed", "family": "group", "original": observation("passed", 3.0),
             "updated": observation("passed", 2.0), "polars": observation("passed", None)},
            {"id": "failed", "family": "group", "original": observation("passed", 1.0),
             "updated": observation("engine_error", None), "polars": observation("passed", None)},
        ]
        result = summarize(rows)
        self.assertIsNone(result["complete_engine_seconds"])
        self.assertEqual(result["new_failures"], ["failed"])
        self.assertEqual(result["common_correct"]["cases"], 1)

    def test_nonfinite_or_missing_completed_engine_clock_is_rejected(self):
        for value in (None, math.nan, math.inf, -1.0, True):
            with self.subTest(value=value):
                rows = [{"id": "case", "family": "group", "original": observation("passed", 3.0),
                         "updated": observation("passed", value), "polars": observation("passed", None)}]
                with self.assertRaisesRegex(ValueError, "engine clock"):
                    summarize(rows)


if __name__ == "__main__":
    unittest.main()
