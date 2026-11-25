# SPDX-License-Identifier: Apache-2.0
import unittest

from evaluate_perf_ship_drop import evaluate_ship_drop


def _make_summary(
    *,
    engine_times: dict[int, float],
    rss: int = 64_000_000,
    sys_cpu: float = 0.1,
    minor_faults: int = 1000,
    timing_surface: str = "cold_process_cli",
    input_format: str = "vortex",
    result_sha256: str = "abc",
    zero_cache: bool = True,
) -> dict:
    records = [
        {
            "query": q,
            "run": 1,
            "passed": True,
            "seconds": t + 0.05,
            "engine_execution_seconds": t,
            "native_peak_rss_bytes": rss,
            "system_cpu_seconds": sys_cpu,
            "minor_page_faults": minor_faults,
            "major_page_faults": 0,
            "result_sha256": f"{result_sha256}-{q}",
            "execution_report": {
                "fallback_attempted": False,
                "external_engine_invoked": False,
            },
        }
        for q, t in sorted(engine_times.items())
    ]
    return {
        "complete": True,
        "runs_completed": len(records),
        "runs_passed": len(records),
        "selected_query_ids": sorted(engine_times),
        "timing_metric": "engine_execution_seconds",
        "timing_surface": timing_surface,
        "input_format": input_format,
        "zero_answer_cache_verified": zero_cache,
        "records": records,
    }


def _make_repeated_summary(
    *,
    repeated_engine_times: dict[int, list[float]],
    rss: int = 64_000_000,
    sys_cpu: float = 0.1,
    minor_faults: int = 1000,
    timing_surface: str = "cold_process_cli",
    input_format: str = "vortex",
    result_sha256: str = "abc",
    zero_cache: bool = True,
) -> dict:
    records = [
        {
            "query": q,
            "run": run_idx,
            "passed": True,
            "seconds": t + 0.05,
            "engine_execution_seconds": t,
            "native_peak_rss_bytes": rss,
            "system_cpu_seconds": sys_cpu,
            "minor_page_faults": minor_faults,
            "major_page_faults": 0,
            "result_sha256": f"{result_sha256}-{q}-{run_idx}",
            "execution_report": {
                "fallback_attempted": False,
                "external_engine_invoked": False,
            },
        }
        for q, runs in sorted(repeated_engine_times.items())
        for run_idx, t in enumerate(runs, start=1)
    ]
    repeats = {len(runs) for runs in repeated_engine_times.values()}
    return {
        "complete": True,
        "repeats": repeats.pop() if len(repeats) == 1 else None,
        "runs_completed": len(records),
        "runs_passed": len(records),
        "selected_query_ids": sorted(repeated_engine_times),
        "timing_metric": "engine_execution_seconds",
        "timing_surface": timing_surface,
        "input_format": input_format,
        "zero_answer_cache_verified": zero_cache,
        "records": records,
    }


class EvaluatePerfShipDropTests(unittest.TestCase):
    def test_retains_when_total_improves_and_tail_and_os_budgets_hold(self):
        base = _make_summary(engine_times={1: 1.0, 2: 2.0})
        cand = _make_summary(engine_times={1: 0.90, 2: 1.90}, rss=66_000_000)
        decision = evaluate_ship_drop(base, cand)
        self.assertEqual(decision["decision"], "retain")
        self.assertLess(decision["total_delta_pct"], -2.0)
        self.assertFalse(decision["fallback_attempted"])

    def test_drops_on_tail_regression_rss_growth_or_result_mismatch(self):
        base = _make_summary(engine_times={1: 1.0, 2: 10.0})
        tail_bad = _make_summary(engine_times={1: 1.15, 2: 8.0})
        self.assertEqual(evaluate_ship_drop(base, tail_bad)["decision"], "drop")

        rss_bad = _make_summary(engine_times={1: 0.8, 2: 8.0}, rss=90_000_000)
        self.assertEqual(evaluate_ship_drop(base, rss_bad)["decision"], "drop")

        hash_bad = _make_summary(engine_times={1: 0.8, 2: 8.0}, result_sha256="changed")
        self.assertEqual(evaluate_ship_drop(base, hash_bad)["decision"], "drop")

    def test_blocks_mismatched_timing_surface_or_zero_cache_failure(self):
        base = _make_summary(engine_times={1: 1.0}, timing_surface="cold_process_cli")
        cand = _make_summary(engine_times={1: 0.5}, timing_surface="warm_resident_session")
        self.assertEqual(evaluate_ship_drop(base, cand)["decision"], "blocked")

        cached = _make_summary(engine_times={1: 0.5}, zero_cache=False)
        self.assertEqual(evaluate_ship_drop(base, cached)["decision"], "drop")

    def test_binary_complete_cohort_policy_retains_any_positive_gain_and_drops_flat_or_slower(self):
        base = _make_summary(engine_times={1: 1.0, 2: 9.0})
        # Total improves -0.80% (< 2% PERF-01 cutoff) while query 1 regresses +15% (> 5% PERF-01 cutoff).
        small_gain = _make_summary(engine_times={1: 1.15, 2: 8.77})
        legacy = evaluate_ship_drop(base, small_gain, policy="perf01_thresholds")
        self.assertEqual(legacy["decision"], "drop")
        self.assertEqual(legacy["policy"], "perf01_thresholds")

        binary = evaluate_ship_drop(base, small_gain, policy="binary_complete_cohort")
        self.assertEqual(binary["decision"], "retain")
        self.assertEqual(binary["policy"], "binary_complete_cohort")
        self.assertLess(binary["total_delta_pct"], 0.0)
        self.assertAlmostEqual(binary["max_query_regression_pct"], 15.0, places=6)

        flat = _make_summary(engine_times={1: 1.0, 2: 9.0})
        self.assertEqual(evaluate_ship_drop(base, flat, policy="binary_complete_cohort")["decision"], "drop")

        slower = _make_summary(engine_times={1: 1.0, 2: 9.01})
        self.assertEqual(evaluate_ship_drop(base, slower, policy="binary_complete_cohort")["decision"], "drop")

        rss_bad = _make_summary(engine_times={1: 0.8, 2: 8.0}, rss=90_000_000)
        self.assertEqual(evaluate_ship_drop(base, rss_bad, policy="binary_complete_cohort")["decision"], "drop")

    def test_binary_complete_cohort_scores_all_repeats_not_per_query_minima(self):
        base = _make_repeated_summary(repeated_engine_times={1: [1.0, 1.0]})
        cand_slower_overall = _make_repeated_summary(repeated_engine_times={1: [0.5, 2.0]})

        legacy = evaluate_ship_drop(base, cand_slower_overall, policy="perf01_thresholds")
        self.assertEqual(legacy["decision"], "retain")
        self.assertAlmostEqual(legacy["total_delta_pct"], -50.0, places=6)

        binary_drop = evaluate_ship_drop(base, cand_slower_overall, policy="binary_complete_cohort")
        self.assertEqual(binary_drop["decision"], "drop")
        self.assertAlmostEqual(binary_drop["total_delta_pct"], 25.0, places=6)

        cand_faster_overall = _make_repeated_summary(repeated_engine_times={1: [0.95, 1.02]})
        binary_retain = evaluate_ship_drop(base, cand_faster_overall, policy="binary_complete_cohort")
        self.assertEqual(binary_retain["decision"], "retain")
        self.assertAlmostEqual(binary_retain["total_delta_pct"], -1.5, places=6)

        cand_mismatched_repeats = _make_repeated_summary(repeated_engine_times={1: [0.5]})
        self.assertEqual(
            evaluate_ship_drop(base, cand_mismatched_repeats, policy="binary_complete_cohort")["decision"],
            "blocked",
        )

    def test_binary_complete_cohort_requires_engine_clock_observations(self):
        base = _make_summary(engine_times={1: 1.0})
        cand = _make_summary(engine_times={1: 2.0})
        # Reverse process-wall ranking (10s -> 5s) while engine time worsens (1s -> 2s).
        base["records"][0]["seconds"] = 10.0
        cand["records"][0]["seconds"] = 5.0

        omitted_base = dict(base)
        omitted_base.pop("timing_metric")
        omitted_decision = evaluate_ship_drop(omitted_base, cand, policy="binary_complete_cohort")
        self.assertEqual(omitted_decision["decision"], "blocked")
        self.assertEqual(omitted_decision["timing_key"], "engine_execution_seconds")

        wall_cand = dict(cand, timing_metric="process_wall_seconds")
        wall_decision = evaluate_ship_drop(base, wall_cand, policy="binary_complete_cohort")
        self.assertEqual(wall_decision["decision"], "blocked")
        self.assertEqual(wall_decision["timing_key"], "engine_execution_seconds")

        legacy_wall = evaluate_ship_drop(omitted_base, wall_cand, policy="perf01_thresholds")
        self.assertEqual(legacy_wall["decision"], "retain")
        self.assertEqual(legacy_wall["timing_key"], "seconds")

        missing_engine_cand = _make_summary(engine_times={1: 0.5})
        missing_engine_cand["records"][0].pop("engine_execution_seconds")
        self.assertEqual(
            evaluate_ship_drop(base, missing_engine_cand, policy="binary_complete_cohort")["decision"],
            "blocked",
        )

        unavailable_cand = dict(cand, timing_available=False)
        self.assertEqual(
            evaluate_ship_drop(base, unavailable_cand, policy="binary_complete_cohort")["decision"],
            "blocked",
        )

    def test_warm_summaries_block_without_resources_and_evaluate_worker_resource_usage(self):
        resource_keys = (
            "native_peak_rss_bytes",
            "system_cpu_seconds",
            "minor_page_faults",
            "major_page_faults",
        )
        base_warm = _make_summary(engine_times={1: 1.0}, timing_surface="warm_resident_session")
        cand_warm = _make_summary(engine_times={1: 0.8}, timing_surface="warm_resident_session")
        for record in (*base_warm["records"], *cand_warm["records"]):
            for key in resource_keys:
                record.pop(key, None)

        self.assertEqual(
            evaluate_ship_drop(base_warm, cand_warm, policy="binary_complete_cohort")["decision"],
            "blocked",
        )
        self.assertEqual(
            evaluate_ship_drop(base_warm, cand_warm, policy="perf01_thresholds")["decision"],
            "blocked",
        )

        usage_base = {
            "returncode": 0,
            "native_peak_rss_bytes": 64_000_000,
            "system_cpu_seconds": 0.1,
            "minor_page_faults": 1000,
            "major_page_faults": 0,
        }
        base_with_usage = {**base_warm, "worker_resource_usage": dict(usage_base)}
        cand_with_ok_usage = {
            **cand_warm,
            "worker_resource_usage": {**usage_base, "native_peak_rss_bytes": 66_000_000},
        }
        ok_decision = evaluate_ship_drop(base_with_usage, cand_with_ok_usage, policy="binary_complete_cohort")
        self.assertEqual(ok_decision["decision"], "retain")
        self.assertEqual(
            set(ok_decision["resource_deltas_pct"]),
            {"peak_rss", "system_cpu", "minor_page_faults", "major_page_faults"},
        )

        cand_with_high_rss = {
            **cand_warm,
            "worker_resource_usage": {**usage_base, "native_peak_rss_bytes": 90_000_000},
        }
        self.assertEqual(
            evaluate_ship_drop(base_with_usage, cand_with_high_rss, policy="binary_complete_cohort")["decision"],
            "drop",
        )

        mismatched_scope_base = _make_summary(engine_times={1: 1.0}, timing_surface="warm_resident_session")
        self.assertEqual(
            evaluate_ship_drop(mismatched_scope_base, cand_with_ok_usage, policy="binary_complete_cohort")["decision"],
            "blocked",
        )


if __name__ == "__main__":
    unittest.main()
