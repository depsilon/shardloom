# SPDX-License-Identifier: Apache-2.0
"""Synthetic controls for expanded-query cohort metrics."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "benchmarks" / "traditional_analytics"))

from query_surface.expanded_metrics import summarize


def row(
    case_id,
    family="one",
    *,
    profile="primary",
    native_status="passed",
    polars_status="passed",
    engine=1.0,
    native_workflow=2.0,
    polars_workflow=4.0,
    native_format="parquet",
    polars_format="parquet",
    surface="python",
):
    return {
        "id": case_id,
        "family": family,
        "archetype": "aggregate",
        "profile": profile,
        "surface": surface,
        "native_format": native_format,
        "polars_format": polars_format,
        "native": {
            "status": native_status,
            "engine_seconds": engine,
            "workflow_seconds": native_workflow,
            "actual_rows": 1,
        },
        "polars": {
            "status": polars_status,
            "engine_seconds": None,
            "workflow_seconds": polars_workflow,
            "actual_rows": 1,
        },
    }


class ExpandedMetricsTests(unittest.TestCase):
    def test_denominators_status_histograms_and_missing_clocks_are_preserved(self):
        missing = row("missing", engine=None, native_workflow=None)
        failed = row("failed", native_status="engine_error", engine=9, native_workflow=8)
        result = summarize([row("ok"), missing, failed])
        self.assertEqual(result["denominator"], 3)
        self.assertEqual(result["native_status_counts"], {"engine_error": 1, "passed": 2})
        self.assertEqual(result["polars_status_counts"], {"passed": 3})
        self.assertEqual(
            result["native_engine_time_valid"],
            {"count": 1, "denominator": 3, "sum_seconds": 1.0, "mean_seconds": 1.0},
        )
        self.assertIsNone(result["complete_total_engine_seconds"])
        self.assertFalse(result["complete_total_available"])

    def test_complete_family_totals_and_family_balanced_mean_use_distinct_weighting(self):
        result = summarize(
            [
                row("a", "small", engine=2),
                row("b", "large", engine=10),
                row("c", "large", engine=14),
                row("d", "large", engine=18),
            ]
        )
        self.assertEqual(result["complete_total_engine_seconds"], 44.0)
        self.assertEqual(result["families"]["small"]["complete_total_engine_seconds"], 2.0)
        self.assertEqual(result["families"]["large"]["complete_total_engine_seconds"], 42.0)
        self.assertEqual(result["family_balanced_mean_native_engine_seconds"], 8.0)
        self.assertEqual(
            result["family_balanced_coverage"], {"families_with_valid_means": 2, "family_denominator": 2}
        )

    def test_prepared_vortex_is_separate_and_only_workflow_clocks_form_ratios(self):
        same = row("same", engine=0.1, native_workflow=2, polars_workflow=4)
        vortex = row(
            "vortex",
            profile="holdout",
            engine=100,
            native_workflow=8,
            polars_workflow=2,
            native_format="vortex",
        )
        result = summarize([same, vortex])
        self.assertEqual(
            result["paired_declarations_through_complete_delivery"]["native_workflow_seconds"], 10.0
        )
        self.assertEqual(
            result["paired_declarations_through_complete_delivery"]["polars_workflow_seconds"], 6.0
        )
        self.assertAlmostEqual(
            result["paired_declarations_through_complete_delivery"]["native_to_polars_workflow_ratio"], 10 / 6
        )
        self.assertEqual(result["paired_same_source"]["count"], 1)
        self.assertEqual(result["paired_native_prepared_vortex"]["count"], 1)
        self.assertEqual(result["paired_unmatched_format"]["count"], 0)
        self.assertIsNone(result["polars_status_counts"].get("engine_seconds"))
        self.assertEqual(result["native_top_20_engine_contributions"][0]["id"], "vortex")

    def test_only_declared_same_formats_and_prepared_vortex_enter_named_strata(self):
        parquet = row("parquet", native_format="parquet", polars_format="parquet")
        arrow = row("arrow", native_format="arrow-ipc", polars_format="arrow-ipc")
        vortex = row("vortex", native_format="vortex", polars_format="parquet")
        unmatched = row("mixed", native_format="vortex", polars_format="arrow-ipc")
        unknown = row("other", native_format="csv", polars_format="csv")
        result = summarize([parquet, arrow, vortex, unmatched, unknown])
        self.assertEqual(result["paired_same_source"]["count"], 2)
        self.assertEqual(result["paired_native_prepared_vortex"]["count"], 2)
        self.assertEqual(result["paired_unmatched_format"]["count"], 1)

    def test_invalid_negative_clock_unavailable_and_zero_denominator_has_no_ratio(self):
        invalid = row("negative", engine=-1, native_workflow=0, polars_workflow=0)
        result = summarize([invalid])
        self.assertEqual(result["native_engine_time_valid"]["count"], 0)
        self.assertIsNone(result["complete_total_engine_seconds"])
        paired = result["paired_declarations_through_complete_delivery"]
        self.assertEqual(paired["count"], 1)
        self.assertEqual(paired["native_workflow_seconds"], 0.0)
        self.assertIsNone(paired["native_to_polars_workflow_ratio"])
        self.assertFalse(paired["ratio_available"])

    def test_zero_native_engine_clock_is_valid_but_missing_family_member_breaks_complete_sum(self):
        result = summarize([row("zero", "family", engine=0), row("missing", "family", engine=None)])
        family = result["families"]["family"]
        self.assertEqual(family["native_engine_time_valid"]["count"], 1)
        self.assertEqual(family["native_engine_time_valid"]["sum_seconds"], 0.0)
        self.assertIsNone(family["complete_total_engine_seconds"])

    def test_engine_seconds_never_substitute_for_polars_workflow_clock(self):
        value = row("one", engine=12, native_workflow=3, polars_workflow=None)
        value["polars"]["engine_seconds"] = 0
        result = summarize([value])
        paired = result["paired_declarations_through_complete_delivery"]
        self.assertEqual(paired["count"], 0)
        self.assertIsNone(paired["native_to_polars_workflow_ratio"])

    def test_invalid_duplicate_and_empty_cohorts_are_rejected(self):
        value = row("one")
        with self.assertRaises(ValueError):
            summarize([])
        with self.assertRaises(ValueError):
            summarize([value, deepcopy(value)])


if __name__ == "__main__":
    unittest.main()
