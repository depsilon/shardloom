# SPDX-License-Identifier: Apache-2.0
"""Adversarial controls for immutable aggregate and grouping acceptance."""
import copy
import json
from pathlib import Path
import unittest

from verify_analytical_aggregate_packet import object_files, sha, strict_json, verify

PACKET = Path(__file__).resolve().parents[1] / 'docs/benchmarks/evidence/native-analytical-aggregates-2026-10-27'


class AnalyticalPacketTests(unittest.TestCase):
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

    def changed_outcome(self, mutate, *, clocks=False):
        files = dict(self.files)
        summary_path = self.root + '/broad-analytical-r1/native-observations.json'
        summary = self.content(files, summary_path)
        item = summary['observations'][0]
        outcome = self.content(files, item['stdout'])
        mutate(outcome)
        digest = self.put(files, item['stdout'], outcome)
        receipt = self.content(files, item['receipt'])
        receipt['stdout_sha256'] = item['stdout_sha256'] = digest
        self.put(files, item['receipt'], receipt)
        if clocks:
            item['clocks'] = outcome['observation']['clocks']
        self.put(files, summary_path, summary)
        return files

    def test_complete_packet(self):
        report = verify(self.files, self.manifest)
        self.assertEqual(report['complete_comparisons'], 347)
        self.assertEqual(report['complete_rows'], 12565)
        self.assertEqual(report['public_feature_suites'], 9)
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

    def test_failed_worker_guard(self):
        files = dict(self.files)
        summary = self.content(files, self.root + '/broad-analytical-r1/native-observations.json')
        path = summary['observations'][0]['receipt']
        receipt = self.content(files, path)
        receipt['guard_failures'] = ['deliberate corruption control']
        self.put(files, path, receipt)
        with self.assertRaises(ValueError):
            verify(files, self.manifest)

    def test_missing_required_gate(self):
        files = dict(self.files)
        del files[self.manifest['path_map'][self.root + '/r141-workspace-tests.json']]
        with self.assertRaisesRegex(ValueError, 'missing linked evidence'):
            verify(files, self.manifest)

    def test_changed_frozen_source(self):
        files = dict(self.files)
        frozen = self.content(files, self.root + '/broad-analytical-r1/inputs.json')
        source = next(item for item in frozen['frozen_files'] if item['path'].endswith('/python/src/shardloom/__init__.py'))
        files[self.manifest['path_map'][source['path']]] += b'\n# altered\n'
        with self.assertRaisesRegex(ValueError, 'frozen source or evidence changed'):
            verify(files, self.manifest)

    def test_missing_external_identity(self):
        manifest = copy.deepcopy(self.manifest)
        manifest['external_artifacts'] = []
        with self.assertRaisesRegex(ValueError, 'unrepresented frozen payload'):
            verify(self.files, manifest)

    def test_public_suite_missing(self):
        files = dict(self.files)
        path = self.root + '/analytical-surfaces-r11.json'
        public = self.content(files, path)
        public['test_names'].pop()
        self.put(files, path, public)
        with self.assertRaisesRegex(ValueError, 'public matrix coverage'):
            verify(files, self.manifest)

    def test_package_runtime_replaced(self):
        files = dict(self.files)
        path = self.root + '/installed-wheel-r1/report.json'
        package = self.content(files, path)
        package['integrity']['native_library_sha256'] = '0' * 64
        self.put(files, path, package)
        with self.assertRaisesRegex(ValueError, 'another runtime'):
            verify(files, self.manifest)

    def test_compiled_source_changed(self):
        files = dict(self.files)
        path = self.root + '/r138-native-vortex-full-suite.json'
        gate = self.content(files, path)
        gate['changed_source_inventory']['shardloom-vortex/src/query_primitive.rs']['sha256'] = '0' * 64
        self.put(files, path, gate)
        with self.assertRaisesRegex(ValueError, 'compiled source differs'):
            verify(files, self.manifest)


if __name__ == '__main__':
    unittest.main()
