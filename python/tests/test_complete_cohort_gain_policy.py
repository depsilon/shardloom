# SPDX-License-Identifier: Apache-2.0
"""Small complete-cohort gains ship without rewriting historical decisions."""
import math
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
from parquet_demand_completion_replay import benefit_decision


class CompleteCohortGainPolicyTests(unittest.TestCase):
    def test_any_representable_complete_gain_passes_without_a_family_gain(self):
        before = 15.992156898
        policy = {'rule': 'strict_complete_cohort_gain', 'declared_families': ['projection']}
        families = {'projection': {'reduction_fraction': -0.01}}
        for after in (15.9, math.nextafter(before, -math.inf)):
            with self.subTest(after=after):
                self.assertEqual(benefit_decision(policy, before, after, families), (True, []))

    def test_equal_or_slower_complete_cohort_drops_despite_a_family_gain(self):
        before = 15.992156898
        policy = {'rule': 'strict_complete_cohort_gain', 'declared_families': ['projection']}
        families = {'projection': {'reduction_fraction': 0.5}}
        for after in (before, math.nextafter(before, math.inf), 16.1):
            with self.subTest(after=after):
                self.assertEqual(benefit_decision(policy, before, after, families), (False, []))

    def test_legacy_protocols_keep_their_original_gate_result(self):
        policy = {'minimum_complete_cohort_reduction_fraction': 0.1,
                  'alternative_minimum_declared_family_reduction_fraction': 0.1,
                  'declared_families': ['projection']}
        self.assertEqual(benefit_decision(policy, 100, 99, {
            'projection': {'reduction_fraction': 0.08}}), (False, []))
        self.assertEqual(benefit_decision(policy, 100, 80, {
            'projection': {'reduction_fraction': 0.08}}), (True, []))
        # The old field describes its percentage gate, not every acceptance
        # condition. Keep that historical result even when overall time rises.
        self.assertEqual(benefit_decision(policy, 100, 101, {
            'projection': {'reduction_fraction': 0.12}}), (True, ['projection']))

    def test_unknown_or_contradictory_rules_fail_explicitly(self):
        for policy in ({'rule': 'family_gain_only'},
                       {'rule': 'strict_complete_cohort_gain',
                        'minimum_complete_cohort_reduction_fraction': 0.1},
                       {'rule': 'strict_complete_cohort_gain',
                        'alternative_minimum_declared_family_reduction_fraction': 0.0}):
            with self.subTest(policy=policy), self.assertRaises(ValueError):
                benefit_decision(policy, 100, 90, {})


if __name__ == '__main__':
    unittest.main()
