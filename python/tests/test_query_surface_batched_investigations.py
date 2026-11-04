# SPDX-License-Identifier: Apache-2.0
"""Offline regressions for sampled stacks and the read-only declared-plan comparison."""

from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "benchmarks/traditional_analytics"))
from query_surface.expanded_batch_replay import main_stack_counts, selector


class BatchedInvestigationTests(unittest.TestCase):
    def test_branch_art_and_inclusive_paths_count_each_main_thread_sample_once(self):
        stack = b"""Call graph:
    9 Thread_123   DispatchQueue_1: com.apple.main-thread  (serial)
    + 7 _Rjoin  (in _native.abi3.so) + 4
    + ! 5 _Rtake  (in _native.abi3.so) + 8
    + ! 2 _Rtake  (in _native.abi3.so) + 12
    + 2 _Relse  (in _native.abi3.so) + 4
    9 Thread_124   worker
Total number in stack:
"""
        symbols = {
            "_Rjoin": "<shardloom_vortex::local_primitives::native_relational_join::Spec>::flush",
            "_Rtake": "shardloom_vortex::local_primitives::native_relational_batch::take_column_with_policy",
        }
        counts = main_stack_counts(stack, symbols)
        self.assertEqual(counts, {"all": 9, "join_pair_flush": 7, "gather_in_join_flush": 7})
        with self.assertRaisesRegex(ValueError, "exceeds parent"):
            main_stack_counts(stack.replace(b"! 5", b"! 6"), symbols)

    def test_selector_prioritizes_first_population_and_excludes_previously_observed_cases(self):
        rows = [
            {
                "id": str(index),
                "profile": "primary",
                "archetype": "nullable_expression_order",
                "native_format": "parquet",
                "cell": {"scope": scope, "function": "coalesce"},
            }
            for index, scope in enumerate(("rare", "population", "population", "population"))
        ]
        self.assertEqual(selector(rows, {"1"}), {"2": ("defer_nullable", "primary", "parquet", "coalesce")})
