# SPDX-License-Identifier: Apache-2.0
"""Corruption controls for complete candidate, diagnostic and package evidence."""
import copy
import json
import os
from pathlib import Path
import unittest

from verify_shared_analytical_performance_packet import object_files, sha, strict_json, verify

DEFAULT_PACKET = Path(__file__).resolve().parents[1] / 'docs/benchmarks/evidence/native-shared-analytical-performance-2026-10-27'
PACKET = Path(os.environ.get('SHARDLOOM_SHARED_PERFORMANCE_PACKET', DEFAULT_PACKET))


class SharedAnalyticalPerformancePacketTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.manifest = strict_json((PACKET / 'manifest.json').read_bytes())
        cls.archive = (PACKET / 'packet.tar.xz').read_bytes()
        cls.files = object_files(cls.manifest, cls.archive)
        cls.root = cls.manifest['original_evidence_root']

    def content(self, files, path):
        return strict_json(files[self.manifest['path_map'][path]])

    def put(self, files, path, value):
        data = json.dumps(value, sort_keys=True, allow_nan=False).encode()
        files[self.manifest['path_map'][path]] = data
        return sha(data)

    def changed_outcome(self, mutate, *, clocks=False, diagnostic=False):
        files = dict(self.files)
        summary_path = self.root + ('/ordering-aggregate-diagnostic-r1/report.json' if diagnostic
                                    else '/broad-ordering-r1/native-observations.json')
        summary = self.content(files, summary_path)
        item = summary['observations'][0]
        receipt = self.content(files, item['receipt'])
        outcome = self.content(files, receipt['stdout'])
        mutate(outcome)
        digest = self.put(files, receipt['stdout'], outcome)
        receipt['stdout_sha256'] = item['stdout_sha256'] = digest
        self.put(files, item['receipt'], receipt)
        if clocks:
            item['clocks'] = outcome['observation']['clocks']
        self.put(files, summary_path, summary)
        return files

    def test_complete_packet_preserves_candidate_decisions_and_flags(self):
        report = verify(self.files, self.manifest)
        self.assertEqual(report['complete_comparisons_per_cohort'], 347)
        self.assertEqual(report['complete_rows_per_cohort'], 12565)
        self.assertEqual(report['dropped_candidates'], ['borrowed_variable_width_output'])
        self.assertEqual(len(report['failed_validation_attempts']), 5)
        self.assertEqual(report['original_material_case_regressions']['broad-ordering-r1'],
                         ['aggregate_metric_scalar_mixed_arithmetic_all'])
        self.assertEqual(report['package_tests'], 194)
        self.assertFalse(report['item_20_complete'])

    def test_changed_archive(self):
        data = bytearray(self.archive)
        data[len(data) // 2] ^= 1
        with self.assertRaisesRegex(ValueError, 'archive identity'):
            object_files(self.manifest, bytes(data))

    def test_changed_complete_value_with_relinked_digests(self):
        def mutate(outcome):
            outcome['actual']['rows'][0]['row_id'] = -12345
        with self.assertRaises((AssertionError, ValueError)):
            verify(self.changed_outcome(mutate), self.manifest)

    def test_changed_schema_with_relinked_digests(self):
        def mutate(outcome):
            outcome['actual']['schema'][0]['type']['nullable'] = True
        with self.assertRaises((AssertionError, ValueError)):
            verify(self.changed_outcome(mutate), self.manifest)

    def test_changed_engine_clock_with_relinked_digests(self):
        def mutate(outcome):
            outcome['observation']['clocks']['engine_execution_seconds'] += 1
        with self.assertRaisesRegex(ValueError, 'clock disagrees'):
            verify(self.changed_outcome(mutate, clocks=True), self.manifest)

    def test_diagnostic_changed_result_with_relinked_digests(self):
        def mutate(outcome):
            outcome['actual']['rows'][0]['value_count'] -= 1
        with self.assertRaises((AssertionError, ValueError)):
            verify(self.changed_outcome(mutate, diagnostic=True), self.manifest)

    def test_diagnostic_changed_clock_with_relinked_digests(self):
        def mutate(outcome):
            outcome['observation']['clocks']['engine_execution_seconds'] += 1
        with self.assertRaisesRegex(ValueError, 'diagnostic clock disagrees'):
            verify(self.changed_outcome(mutate, clocks=True, diagnostic=True), self.manifest)

    def test_diagnostic_missing_observation(self):
        files = dict(self.files)
        path = self.root + '/ordering-aggregate-diagnostic-r1/report.json'
        report = self.content(files, path)
        report['observations'].pop()
        self.put(files, path, report)
        with self.assertRaisesRegex(ValueError, 'diagnostic execution order'):
            verify(files, self.manifest)

    def test_diagnostic_median_changed(self):
        files = dict(self.files)
        path = self.root + '/ordering-aggregate-diagnostic-r1/report.json'
        report = self.content(files, path)
        report['cases']['aggregate_metric_scalar_mixed_arithmetic_all']['median_engine_seconds']['candidate'] = 0
        self.put(files, path, report)
        with self.assertRaisesRegex(ValueError, 'diagnostic summary'):
            verify(files, self.manifest)

    def test_dropped_candidate_cannot_be_promoted(self):
        files = dict(self.files)
        path = self.root + '/broad-variable-r1-comparison.json'
        report = self.content(files, path)
        report['benefit_gate_passed'] = True
        self.put(files, path, report)
        with self.assertRaises(ValueError):
            verify(files, self.manifest)

    def test_original_regression_cannot_be_removed(self):
        files = dict(self.files)
        path = self.root + '/broad-ordering-r1-comparison.json'
        report = self.content(files, path)
        report['material_case_regressions'] = []
        self.put(files, path, report)
        with self.assertRaises(ValueError):
            verify(files, self.manifest)

    def test_failed_worker_guard(self):
        files = dict(self.files)
        summary = self.content(files, self.root + '/broad-ordering-r1/native-observations.json')
        path = summary['observations'][0]['receipt']
        receipt = self.content(files, path)
        receipt['guard_failures'] = ['deliberate corruption control']
        self.put(files, path, receipt)
        with self.assertRaises(ValueError):
            verify(files, self.manifest)

    def test_missing_required_gate(self):
        files = dict(self.files)
        del files[self.manifest['path_map'][self.root + '/r23-ordering-workspace-tests.json']]
        with self.assertRaisesRegex(ValueError, 'missing linked evidence'):
            verify(files, self.manifest)

    def test_historical_failure_cannot_be_replaced(self):
        files = dict(self.files)
        path = self.root + '/r17-streaming-order-tests.json'
        receipt = self.content(files, path)
        receipt.update(status='passed', returncode=0)
        self.put(files, path, receipt)
        with self.assertRaisesRegex(ValueError, 'historical failure replaced'):
            verify(files, self.manifest)

    def test_changed_frozen_source(self):
        files = dict(self.files)
        frozen = self.content(files, self.root + '/broad-ordering-r1/inputs.json')
        item = next(item for item in frozen['frozen_files']
                    if item['path'].endswith('/python/src/shardloom/__init__.py'))
        files[self.manifest['path_map'][item['path']]] += b'\n# changed\n'
        with self.assertRaisesRegex(ValueError, 'frozen source or evidence changed'):
            verify(files, self.manifest)

    def test_missing_external_identity(self):
        manifest = copy.deepcopy(self.manifest)
        manifest['external_artifacts'] = []
        with self.assertRaisesRegex(ValueError, 'unrepresented frozen payload'):
            verify(self.files, manifest)

    def test_public_ordering_case_missing(self):
        files = dict(self.files)
        path = self.root + '/ordering-public-r1/summary.json'
        report = self.content(files, path)
        report['cases'].pop()
        self.put(files, path, report)
        with self.assertRaisesRegex(ValueError, 'streamed ordering coverage'):
            verify(files, self.manifest)

    def test_package_runtime_replaced(self):
        files = dict(self.files)
        path = self.root + '/installed-wheel-r1/report.json'
        report = self.content(files, path)
        report['integrity']['native_library_sha256'] = '0' * 64
        self.put(files, path, report)
        with self.assertRaisesRegex(ValueError, 'another runtime'):
            verify(files, self.manifest)

    def test_compiled_source_changed(self):
        files = dict(self.files)
        path = self.root + '/r21-ordering-native-tests.json'
        receipt = self.content(files, path)
        name = 'shardloom-vortex/src/local_primitives/native_relational_sort_streaming.rs'
        receipt['changed_source_inventory'][name]['sha256'] = '0' * 64
        self.put(files, path, receipt)
        with self.assertRaisesRegex(ValueError, 'compiled source differs'):
            verify(files, self.manifest)


if __name__ == '__main__':
    unittest.main()
