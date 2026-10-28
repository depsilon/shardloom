# SPDX-License-Identifier: Apache-2.0
"""Corruption controls for rich bounded-retention acceptance evidence."""
import copy
import gzip
import json
import os
from pathlib import Path
import re
import unittest

from rich_ordering_replay import replay_heldout
from verify_rich_bounded_retention_packet import (
    object_files, sha, strict_json, verify,
)
from query_surface.replay import MAX_FILE

DEFAULT_PACKET = (Path(__file__).resolve().parents[1]
                  / 'docs/benchmarks/evidence/native-rich-bounded-retention-2026-10-28')
PACKET = Path(os.environ.get('SHARDLOOM_RICH_RETENTION_PACKET', DEFAULT_PACKET))


class RichBoundedRetentionPacketTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.manifest = strict_json((PACKET / 'manifest.json').read_bytes())
        cls.archive = (PACKET / 'packet.tar.xz').read_bytes()
        cls.files = object_files(cls.manifest, cls.archive)
        cls.root = cls.manifest['original_evidence_root']

    def content(self, files, path):
        return strict_json(files[self.manifest['path_map'][path]])

    def put_bytes(self, files, path, data):
        files[self.manifest['path_map'][path]] = data
        return sha(data)

    def put(self, files, path, value):
        data = json.dumps(value, sort_keys=True, allow_nan=False).encode()
        return self.put_bytes(files, path, data)

    def gate_path(self, number):
        pattern = re.compile(rf'{re.escape(self.root)}/r{number:02d}-[^/]+\.json$')
        return next(path for path in self.manifest['path_map'] if pattern.fullmatch(path))

    def heldout_context(self, files):
        path_map = self.manifest['path_map']
        raw = lambda path: files[path_map[path]]
        protocol = strict_json(raw(self.root + '/protocol-r1.json'))
        candidate = strict_json(raw(self.root + '/broad-candidate-r1/inputs.json'))
        return raw, protocol, candidate['runtime']

    def replay_heldout(self, files):
        raw, protocol, runtime = self.heldout_context(files)
        return replay_heldout(raw, self.root + '/heldout-r1', protocol, runtime)

    def heldout_observation(self, files, case_id='filter_project_order_limit_k7_o3'):
        summary_path = self.root + '/heldout-r1/observations.json'
        summary = self.content(files, summary_path)
        row = next(item for item in summary['observations'] if item['case_id'] == case_id)
        receipt = self.content(files, row['receipt'])
        outcome = strict_json(files[self.manifest['path_map'][receipt['stdout']]])
        return summary_path, summary, row, receipt, outcome

    def relink_heldout_outcome(self, files, summary_path, summary, row, receipt, outcome):
        stdout = json.dumps(outcome, allow_nan=False, separators=(',', ':')).encode()
        receipt['stdout_sha256'] = self.put_bytes(files, receipt['stdout'], stdout)
        self.put(files, row['receipt'], receipt)
        self.put(files, summary_path, summary)

    def repack_actual(self, files, outcome, mutate):
        record = outcome['actual']
        packed_path = record['path']
        value = json.loads(gzip.decompress(files[self.manifest['path_map'][packed_path]]))
        mutate(value)
        body = json.dumps(value, sort_keys=True, allow_nan=False, separators=(',', ':')).encode()
        compressed = gzip.compress(body, mtime=0)
        record['sha256'] = self.put_bytes(files, packed_path, compressed)
        record['uncompressed_bytes'] = len(body)

    def changed_heldout_outcome(self, mutate):
        files = dict(self.files)
        summary_path, summary, row, receipt, outcome = self.heldout_observation(files)
        mutate(files, outcome)
        self.relink_heldout_outcome(files, summary_path, summary, row, receipt, outcome)
        return files

    def test_complete_packet_preserves_acceptance(self):
        report = verify(self.files, self.manifest)
        self.assertEqual(report['comparisons']['baseline']['complete_comparisons'], 347)
        self.assertEqual(report['comparisons']['candidate']['complete_comparisons'], 347)
        self.assertEqual(report['heldout']['observations'], 176)
        self.assertEqual(report['heldout']['complete_rows'], 624906)
        self.assertEqual(report['installed_package_tests'], 200)
        self.assertEqual(len(report['failed_validation_attempts']), 7)

    def test_changed_archive(self):
        data = bytearray(self.archive)
        data[len(data) // 2] ^= 1
        with self.assertRaisesRegex(ValueError, 'archive identity'):
            object_files(self.manifest, bytes(data))

    def test_missing_heldout_observation(self):
        files = dict(self.files)
        path, summary, *_ = self.heldout_observation(files)
        summary['observations'].pop()
        self.put(files, path, summary)
        with self.assertRaisesRegex(ValueError, 'held-out observation coverage'):
            self.replay_heldout(files)

    def test_duplicated_heldout_case(self):
        files = dict(self.files)
        path, summary, *_ = self.heldout_observation(files)
        summary['observations'][1]['case_id'] = summary['observations'][0]['case_id']
        self.put(files, path, summary)
        with self.assertRaises((AssertionError, ValueError)):
            self.replay_heldout(files)

    def test_changed_heldout_value_with_repacked_actual_and_relinked_stdout(self):
        def mutate(files, outcome):
            self.repack_actual(files, outcome, lambda actual: actual['rows'][0].update(row_id=-123456))
        with self.assertRaises((AssertionError, ValueError)):
            self.replay_heldout(self.changed_heldout_outcome(mutate))

    def test_changed_heldout_schema_with_repacked_actual_and_relinked_stdout(self):
        def mutate(files, outcome):
            self.repack_actual(files, outcome,
                lambda actual: actual['schema'][0]['type'].update(nullable=True))
        with self.assertRaises((AssertionError, ValueError)):
            self.replay_heldout(self.changed_heldout_outcome(mutate))

    def test_changed_actual_compressed_hash(self):
        def mutate(_files, outcome):
            outcome['actual']['sha256'] = '0' * 64
        with self.assertRaisesRegex(ValueError, 'packed evidence changed'):
            self.replay_heldout(self.changed_heldout_outcome(mutate))

    def test_excessive_actual_uncompressed_length(self):
        def mutate(_files, outcome):
            outcome['actual']['uncompressed_bytes'] = MAX_FILE + 1
        with self.assertRaisesRegex(ValueError, 'packed expansion exceeds bound'):
            self.replay_heldout(self.changed_heldout_outcome(mutate))

    def test_changed_declared_query_in_heldout_outcome(self):
        def mutate(_files, outcome):
            outcome['observation']['public_declaration'] = {'kind': 'changed'}
        with self.assertRaisesRegex(ValueError, 'public declaration changed'):
            self.replay_heldout(self.changed_heldout_outcome(mutate))

    def test_failed_worker_guard(self):
        files = dict(self.files)
        _, _, row, receipt, _ = self.heldout_observation(files)
        receipt['guard_failures'] = ['deliberate corruption control']
        self.put(files, row['receipt'], receipt)
        with self.assertRaisesRegex(ValueError, 'failed held-out worker accepted'):
            self.replay_heldout(files)

    def test_no_fallback_cannot_be_changed_to_true(self):
        def mutate(_files, outcome):
            fields = outcome['observation']['fields']
            key = next(name for name in fields if name.endswith('fallback_attempted'))
            fields[key] = 'true'
        with self.assertRaisesRegex(ValueError, 'unsafe native evidence'):
            self.replay_heldout(self.changed_heldout_outcome(mutate))

    def test_prefix_activation_cannot_be_erased(self):
        def mutate(_files, outcome):
            outcome['observation']['fields']['native_input_ordering_prefix_batches'] = '0'
        with self.assertRaisesRegex(ValueError, 'bounded ordering activation differs'):
            self.replay_heldout(self.changed_heldout_outcome(mutate))

    def test_copied_bytes_cannot_fall_below_retained_peak(self):
        def mutate(_files, outcome):
            fields = outcome['observation']['fields']
            peak = int(fields['native_input_ordering_prefix_peak_retained_buffer_bytes'])
            fields['native_input_ordering_payload_buffer_bytes_copied'] = str(peak - 1)
        with self.assertRaisesRegex(ValueError, 'retained payload exceeds all copied buffers'):
            self.replay_heldout(self.changed_heldout_outcome(mutate))

    def test_missing_frozen_external_identity(self):
        manifest = copy.deepcopy(self.manifest)
        manifest['external_artifacts'] = []
        with self.assertRaisesRegex(ValueError, 'unrepresented frozen payload'):
            verify(self.files, manifest)

    def test_missing_required_gate(self):
        files = dict(self.files)
        path = self.gate_path(36)
        del files[self.manifest['path_map'][path]]
        with self.assertRaisesRegex(ValueError, 'missing linked evidence'):
            verify(files, self.manifest)

    def test_historical_failed_gate_cannot_be_promoted(self):
        files = dict(self.files)
        path = self.gate_path(4)
        receipt = self.content(files, path)
        receipt.update(status='passed', returncode=0)
        self.put(files, path, receipt)
        with self.assertRaisesRegex(ValueError, 'historical failure replaced'):
            verify(files, self.manifest)

    def update_record(self, record, data):
        record['sha256'] = sha(data)
        record['generation']['bytes'] = len(data)

    def test_changed_candidate_source_snapshot(self):
        files = dict(self.files)
        runtime_path = self.root + '/candidate-release-r1/manifest.json'
        runtime = self.content(files, runtime_path)
        name = next(name for name in runtime['source_snapshot'] if name.endswith('.rs'))
        source_path = self.root + '/candidate-release-r1/source/' + name
        files[self.manifest['path_map'][source_path]] += b'\n'

        with self.assertRaisesRegex(ValueError, 'runtime source differs from build'):
            verify(files, self.manifest)

    def rewrite_runtime_manifest(self, files, runtime_path, mutate):
        runtime = self.content(files, runtime_path)
        mutate(runtime)
        data = json.dumps(runtime, sort_keys=True, allow_nan=False).encode()
        self.put_bytes(files, runtime_path, data)

        inputs_path = self.root + '/broad-candidate-r1/inputs.json'
        inputs = self.content(files, inputs_path)
        self.update_record(inputs['runtime_manifest'], data)
        for item in inputs['frozen_files']:
            if item['path'] == runtime_path:
                self.update_record(item, data)
        input_data = json.dumps(inputs, sort_keys=True, allow_nan=False).encode()
        self.put_bytes(files, inputs_path, input_data)

        broad_summary_path = self.root + '/broad-candidate-r1/native-observations.json'
        broad_summary = self.content(files, broad_summary_path)
        self.update_record(broad_summary['input_manifest'], input_data)
        self.put(files, broad_summary_path, broad_summary)

        bound_path = self.root + '/heldout-r1/runtime.json'
        bound = self.content(files, bound_path)
        self.update_record(bound['runtime_manifest'], data)
        bound_data = json.dumps(bound, sort_keys=True, allow_nan=False).encode()
        self.put_bytes(files, bound_path, bound_data)
        summary_path = self.root + '/heldout-r1/observations.json'
        summary = self.content(files, summary_path)
        self.update_record(summary['runtime_binding'], bound_data)
        self.put(files, summary_path, summary)

    def test_empty_public_runtime_test_hash_map_is_rejected(self):
        files = dict(self.files)
        path = self.root + '/public-prefix-r3.json'
        proof = self.content(files, path)
        proof['runtime_and_test_sha256'] = {}
        self.put(files, path, proof)
        with self.assertRaises(ValueError):
            verify(files, self.manifest)

    def test_candidate_source_snapshot_entry_cannot_be_removed(self):
        files = dict(self.files)
        runtime_path = self.root + '/candidate-release-r1/manifest.json'

        def remove_source(runtime):
            name = next(name for name in runtime['source_snapshot']
                        if name.endswith('.rs')
                        and name != 'python/tests/test_native_shared_provider_binding.py')
            del runtime['source_snapshot'][name]

        self.rewrite_runtime_manifest(files, runtime_path, remove_source)
        with self.assertRaises(ValueError):
            verify(files, self.manifest)

    def test_compiled_source_receipt_hash_mismatch(self):
        files = dict(self.files)
        runtime = self.content(files, self.root + '/candidate-release-r1/manifest.json')
        source = next(name for name in runtime['source_snapshot'] if name.endswith('.rs'))
        path = self.gate_path(20)
        receipt = self.content(files, path)
        receipt['changed_source_inventory'][source]['sha256'] = '0' * 64
        self.put(files, path, receipt)
        with self.assertRaisesRegex(ValueError, 'compiled source differs'):
            verify(files, self.manifest)

    def test_changed_public_proof_test_hash(self):
        files = dict(self.files)
        path = self.root + '/public-prefix-r3.json'
        proof = self.content(files, path)
        test_path = next(name for name in proof['runtime_and_test_sha256']
                         if name.endswith('test_native_shared_provider_binding.py'))
        proof['runtime_and_test_sha256'][test_path] = '0' * 64
        self.put(files, path, proof)
        with self.assertRaisesRegex(ValueError, 'public proof runtime or source changed'):
            verify(files, self.manifest)

    def test_package_runtime_mismatch(self):
        files = dict(self.files)
        path = self.root + '/installed-wheel-r1/report.json'
        report = self.content(files, path)
        report['integrity']['native_library_sha256'] = '0' * 64
        self.put(files, path, report)
        with self.assertRaisesRegex(ValueError, 'another runtime'):
            verify(files, self.manifest)

    def test_original_temporal_baseline_flags_cannot_be_erased(self):
        files = dict(self.files)
        path = self.root + '/broad-baseline-r1-comparison.json'
        report = self.content(files, path)
        report['material_case_regressions'] = []
        self.put(files, path, report)
        with self.assertRaisesRegex(ValueError, 'frozen source or evidence changed'):
            verify(files, self.manifest)


if __name__ == '__main__':
    unittest.main()
