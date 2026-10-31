# SPDX-License-Identifier: Apache-2.0
"""Reject changes to the reduced composition and its preserved comparison method."""
import copy
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'scripts'), str(ROOT / 'python/src'),
               str(ROOT / 'benchmarks/traditional_analytics')]
from native_workflow_protocol import strict_json
from native_performance_batch_subset_replay import (
    FUNCTIONS80, native_controls, retained_protocol, same_functions, source_reduction,
)
from shared_performance_bundle import Bundle
from shared_performance_replay import linked
from test_native_performance_batch_packet import ChangedRecords, raw


class NativePerformanceSubsetPacketTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bundle = Bundle(ROOT / 'docs/benchmarks/evidence/native-performance-batch-2026-10-31')
        cls.anchors = strict_json((ROOT / 'scripts/native_performance_batch_anchors.json').read_bytes())
        cls.subset = cls.anchors['retained_subset']
        cls.runtime = linked(cls.bundle, cls.subset['runtime'])
        cls.protocol = linked(cls.bundle, cls.subset['supplemental_cohort']['protocol'])

    def test_exact_reduction_is_reproducible_from_both_predecessors(self):
        result = source_reduction(self.bundle, self.anchors, self.subset, self.runtime)
        self.assertEqual(result, dict(production_inputs=2662, retained_owner_files=14,
            restored_files=28, removed_files=8, metadata_files_preserved=9))
        bad = copy.deepcopy(self.runtime)
        old = linked(self.bundle, self.anchors['runtime'])
        name = 'shardloom-vortex/src/local_primitives/native_relational_spill.rs'
        bad['source_snapshot'][name] = old['source_snapshot'][name]
        with self.assertRaises(ValueError):
            source_reduction(self.bundle, self.anchors, self.subset, bad)

    def test_shared_files_cannot_retain_extra_optimization_hunks(self):
        name = 'shardloom-vortex/src/local_primitives/native_relational_observation.rs'
        receipt = strict_json(self.bundle.raw(self.anchors['root'] + '/' + self.runtime['build_receipt']))
        path = receipt['cwd'] + '/' + name
        item = dict(path=path, **self.runtime['source_snapshot'][name])
        modified = self.bundle.identity(item).replace(b'    pub(crate) replay_captured_rows: u64,',
            b'    pub(crate) replay_captured_rows: u64,\n    pub(crate) unrelated_owner_hits: u64,')
        view = ChangedRecords(self.bundle, {path: modified})
        with self.assertRaises(ValueError):
            source_reduction(view, self.anchors, self.subset, self.runtime)

    def test_retained_owner_and_metadata_controls_cannot_be_removed(self):
        proof = linked(self.bundle, self.subset['native_controls'])
        self.assertEqual(native_controls(self.bundle, proof, self.runtime)['controls'], 16)
        for index in (0, 6, 8, 12, 15):
            bad = copy.deepcopy(proof)
            bad['tests'].pop(index)
            with self.subTest(index=index), self.assertRaises(ValueError):
                native_controls(self.bundle, bad, self.runtime)

    def test_subset_keeps_every_case_order_and_original_resource_grant(self):
        self.assertEqual(retained_protocol(self.bundle, self.protocol)['unchanged_cases'], 80)
        for mutation in ('case', 'order', 'grant'):
            bad = copy.deepcopy(self.protocol)
            if mutation == 'case':
                bad['cases'].pop()
            elif mutation == 'order':
                bad['cases'].reverse()
            else:
                bad['cases'][0]['resources']['memory_bytes'] *= 2
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                retained_protocol(self.bundle, bad)

    def test_reference_reexecution_and_relabeling_original_drop_are_rejected(self):
        for key, value in (('reference_reexecutions', 1),
                           ('retained_candidate_executions_before_declaration', 80),
                           ('original_ten_owner_disposition', 'retained')):
            bad = copy.deepcopy(self.protocol)
            bad[key] = value
            with self.subTest(field=key), self.assertRaises(ValueError):
                retained_protocol(self.bundle, bad)

    def test_observer_timing_or_execution_changes_are_rejected(self):
        old = linked(self.bundle, self.protocol['original_protocol'])
        source = self.bundle.identity(self.protocol['observer_driver'])
        previous = self.bundle.identity(old['observer_driver'])
        same_functions(previous, source, FUNCTIONS80)
        for before, after in ((b'started = time.perf_counter()', b'started = 0'),
                              (b'for index, case in enumerate(frozen[\'cases\']):',
                               b'for index, case in enumerate(frozen[\'cases\'][:16]):')):
            self.assertIn(before, source)
            with self.subTest(change=before), self.assertRaises(ValueError):
                same_functions(previous, source.replace(before, after), FUNCTIONS80)


if __name__ == '__main__':
    unittest.main()
