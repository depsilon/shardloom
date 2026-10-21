# SPDX-License-Identifier: Apache-2.0
"""Small deterministic tests for the frozen cost-gate arithmetic."""

from __future__ import annotations

import statistics
import unittest

from native_exchange_cost_gates import (
    TARGETS,
    bootstrap_upper_ratio,
    distinct_families,
    paired_metrics,
    within_allowance,
)


class CostGateArithmeticTests(unittest.TestCase):
    def test_paired_median_of_ratios_is_not_ratio_of_medians(self) -> None:
        candidate_values = [1, 20, 30, 40, 50]
        control_values = [1, 2, 100, 100, 100]
        candidate = [{"workflow": value, "engine": value, "wire": value, "peer_cpu": value}
                     for value in candidate_values]
        control = [{"workflow": value, "engine": value, "wire": value, "peer_cpu": value}
                   for value in control_values]
        result = paired_metrics(candidate, control)
        self.assertEqual(result["median_workflow_ratio"], 0.5)
        self.assertEqual(statistics.median(candidate_values) / statistics.median(control_values), 0.3)

    def test_absolute_allowance_can_pass_when_relative_allowance_fails(self) -> None:
        self.assertTrue(within_allowance(1.2, 0.004, 0.05, 0.005))

    def test_relative_allowance_can_pass_when_absolute_allowance_fails(self) -> None:
        self.assertTrue(within_allowance(1.04, 0.1, 0.05, 0.005))

    def test_exceeding_both_allowances_fails_and_exact_limits_pass(self) -> None:
        self.assertFalse(within_allowance(1.06, 0.006, 0.05, 0.005))
        self.assertTrue(within_allowance(1.05, 0.006, 0.05, 0.005))
        self.assertTrue(within_allowance(1.06, 0.005, 0.05, 0.005))

    def test_constant_ratio_bootstrap_is_constant_within_tolerance(self) -> None:
        ratios = {case: [0.8] * 5 for case in TARGETS}
        self.assertAlmostEqual(bootstrap_upper_ratio(ratios), 0.8, places=12)

    def test_bootstrap_repeats_with_fixed_seed(self) -> None:
        ratios = {case: [0.7 + (i + j) / 100 for i in range(5)] for j, case in enumerate(TARGETS)}
        self.assertEqual(bootstrap_upper_ratio(ratios), bootstrap_upper_ratio(ratios))

    def test_two_dictionary_winners_count_as_one_family(self) -> None:
        self.assertEqual(distinct_families({"dictionary_small", "nested"}), {"dictionaries"})
        self.assertEqual(len(distinct_families({"dictionary_small", "nested"})), 1)


if __name__ == "__main__":
    unittest.main()
