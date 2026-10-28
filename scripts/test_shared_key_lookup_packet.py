# SPDX-License-Identifier: Apache-2.0
"""Corruption controls for shared key lookup acceptance evidence."""
import gzip
import json
import os
from pathlib import Path
import re
import unittest

from verify_shared_key_lookup_packet import MAX_ARCHIVE, MAX_FILE, object_files, sha, strict_json, verify
from query_surface.semantics import ResultMismatch

DEFAULT_PACKET = (Path(__file__).resolve().parents[1]
                  / 'docs/benchmarks/evidence/native-shared-key-lookup-2026-10-28')
PACKET = Path(os.environ.get('SHARDLOOM_SHARED_KEY_PACKET', DEFAULT_PACKET))


class SharedKeyLookupPacketTests(unittest.TestCase):
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

    def heldout_observation(self, files, case_id='group_dense_negative'):
        summary_path = self.root + '/heldout-r1/observations.json'
        summary = self.content(files, summary_path)
        row = next(item for item in summary['observations'] if item['case_id'] == case_id)
        receipt = self.content(files, row['receipt'])
        outcome = strict_json(files[self.manifest['path_map'][receipt['stdout']]])
        return summary_path, summary, row, receipt, outcome

    def relink_outcome(self, files, summary_path, summary, row, receipt, outcome):
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

    def changed_outcome(self, mutate):
        files = dict(self.files)
        summary_path, summary, row, receipt, outcome = self.heldout_observation(files)
        mutate(files, outcome)
        self.relink_outcome(files, summary_path, summary, row, receipt, outcome)
        return files

    def test_complete_packet_preserves_acceptance(self):
        report = verify(self.files, self.manifest)
        self.assertEqual(report['comparison']['complete_comparisons'], 347)
        self.assertEqual(report['heldout']['observations'], 160)
        self.assertEqual(report['heldout']['complete_rows'], 2345274)
        self.assertEqual(report['installed_package_tests'], 201)
        self.assertEqual(report['public_interface_cases'], 25)
        self.assertEqual(report['failed_validation_attempts'], [5])

    def test_changed_archive_bytes(self):
        data = bytearray(self.archive)
        data[len(data) // 2] ^= 1
        with self.assertRaises(ValueError):
            object_files(self.manifest, bytes(data))

    def test_previous_archive_chunk_order_is_verified(self):
        manifest = json.loads(json.dumps(self.manifest))
        parts = manifest['chunked_files'][manifest['previous_archive_path']]['parts']
        self.assertGreater(len(parts), 1)
        parts.reverse()
        with self.assertRaisesRegex(ValueError, 'chunked evidence digest changed'):
            verify(self.files, manifest)

    def test_chunked_evidence_keeps_the_archive_size_bound(self):
        manifest = json.loads(json.dumps(self.manifest))
        manifest['chunked_files'][manifest['previous_archive_path']]['bytes'] = MAX_ARCHIVE + 1
        with self.assertRaisesRegex(ValueError, 'chunked evidence exceeds bound'):
            verify(self.files, manifest)

    def test_missing_heldout_observation(self):
        files = dict(self.files)
        path, summary, *_ = self.heldout_observation(files)
        summary['observations'].pop()
        self.put(files, path, summary)
        with self.assertRaises(ValueError):
            verify(files, self.manifest)

    def test_duplicate_heldout_observation(self):
        files = dict(self.files)
        path, summary, *_ = self.heldout_observation(files)
        summary['observations'][1]['case_id'] = summary['observations'][0]['case_id']
        self.put(files, path, summary)
        with self.assertRaises(ValueError):
            verify(files, self.manifest)

    def test_missing_runtime_binding(self):
        files = dict(self.files)
        path = self.root + '/heldout-r1/runtime.json'
        del files[self.manifest['path_map'][path]]
        with self.assertRaises(ValueError):
            verify(files, self.manifest)

    def test_changed_runtime_binding(self):
        files = dict(self.files)
        path = self.root + '/heldout-r1/runtime.json'
        bound = self.content(files, path)
        bound['status'] = 'bound_after_candidate_execution'
        self.put(files, path, bound)
        with self.assertRaises(ValueError):
            verify(files, self.manifest)

    def test_worker_runtime_or_resources_cannot_be_relinked_to_a_different_job(self):
        files = dict(self.files)
        _, _, row, receipt, _ = self.heldout_observation(files)
        job_path = receipt['command'][-1]
        job = strict_json(files[self.manifest['path_map'][job_path]])
        job['binary'] += '.changed'
        job['memory_bytes'] += 1
        job_data = json.dumps(job, sort_keys=True, allow_nan=False).encode()
        receipt['job_sha256'] = self.put_bytes(files, job_path, job_data)
        self.put(files, row['receipt'], receipt)
        with self.assertRaises(ValueError):
            verify(files, self.manifest)

    def test_changed_actual_value_after_repacking_and_relinking(self):
        def mutate(files, outcome):
            self.repack_actual(files, outcome,
                lambda actual: actual['rows'][0].update(key=actual['rows'][0].get('key', 0) + 1))
        with self.assertRaisesRegex(ResultMismatch, 'exact fields or NULL multiplicities differ'):
            verify(self.changed_outcome(mutate), self.manifest)

    def test_changed_actual_schema_or_nullability_after_repacking(self):
        def mutate(files, outcome):
            self.repack_actual(files, outcome,
                lambda actual: actual['schema'][0]['type'].update(nullable=True))
        with self.assertRaisesRegex(ResultMismatch, 'schema differs'):
            verify(self.changed_outcome(mutate), self.manifest)

    def test_wrong_actual_row_count(self):
        def mutate(files, outcome):
            self.repack_actual(files, outcome, lambda actual: actual['rows'].pop())
        with self.assertRaisesRegex(ResultMismatch, 'row count differs'):
            verify(self.changed_outcome(mutate), self.manifest)

    def test_changed_actual_dtype_after_repacking(self):
        def mutate(files, outcome):
            self.repack_actual(files, outcome,
                lambda actual: actual['schema'][0]['type'].update(name='int32'))
        with self.assertRaisesRegex(ResultMismatch, 'schema differs'):
            verify(self.changed_outcome(mutate), self.manifest)

    def test_incomplete_source_end_and_false_detached_ownership(self):
        def mutate(_files, outcome):
            fields = outcome['observation']['fields']
            fields['native_input_end_observed'] = 'false'
            fields['native_input_output_ownership_detached'] = 'false'
        with self.assertRaisesRegex(ValueError, 'source completion or ownership proof absent'):
            verify(self.changed_outcome(mutate), self.manifest)

    def test_external_engine_cannot_be_claimed(self):
        def mutate(_files, outcome):
            outcome['comparison_engine_invoked'] = True
        with self.assertRaises(ValueError):
            verify(self.changed_outcome(mutate), self.manifest)

    def test_engine_clock_must_match_reported_fields(self):
        def mutate(_files, outcome):
            outcome['observation']['clocks']['engine_execution_seconds'] += 1
        with self.assertRaisesRegex(ValueError, 'clock disagrees with engine report'):
            verify(self.changed_outcome(mutate), self.manifest)

    def test_external_execution_field_cannot_be_relinked(self):
        def mutate(_files, outcome):
            outcome['observation']['fields']['external_engine_invoked'] = 'true'
        with self.assertRaisesRegex(ValueError, 'missing or unsafe native evidence'):
            verify(self.changed_outcome(mutate), self.manifest)

    def test_packed_expansion_above_limit_is_rejected(self):
        def mutate(_files, outcome):
            outcome['actual']['uncompressed_bytes'] = MAX_FILE + 1
        with self.assertRaises(ValueError):
            verify(self.changed_outcome(mutate), self.manifest)

    def test_empty_public_runtime_test_digest_map_is_rejected(self):
        files = dict(self.files)
        path = self.root + '/public-keys-r1.json'
        proof = self.content(files, path)
        proof['runtime_and_test_sha256'] = {}
        self.put(files, path, proof)
        with self.assertRaises(ValueError):
            verify(files, self.manifest)

    def test_missing_candidate_source_snapshot_entry(self):
        files = dict(self.files)
        path = self.root + '/candidate-key-release-r1/manifest.json'
        runtime = self.content(files, path)
        name = next(name for name in runtime['source_snapshot'] if name.endswith('.rs'))
        del runtime['source_snapshot'][name]
        self.put(files, path, runtime)
        with self.assertRaises(ValueError):
            verify(files, self.manifest)

    def test_required_gate_command_cannot_be_changed(self):
        files = dict(self.files)
        path = self.gate_path(6)
        receipt = self.content(files, path)
        receipt['command'][-1] = 'unexpected-option'
        self.put(files, path, receipt)
        with self.assertRaisesRegex(ValueError, 'wrong gate command'):
            verify(files, self.manifest)

    def test_historical_failed_attempt_cannot_be_deleted_or_marked_passed(self):
        files = dict(self.files)
        path = self.gate_path(5)
        del files[self.manifest['path_map'][path]]
        with self.assertRaises(ValueError):
            verify(files, self.manifest)

        files = dict(self.files)
        receipt = self.content(files, path)
        receipt.update(status='passed', returncode=0)
        self.put(files, path, receipt)
        with self.assertRaisesRegex(ValueError, 'historical failure replaced'):
            verify(files, self.manifest)

    def test_baseline_observation_digest_cannot_be_altered(self):
        files = dict(self.files)
        path = self.root + '/broad-key-r1/inputs.json'
        inputs = self.content(files, path)
        inputs['baseline_observations']['sha256'] = '0' * 64
        self.put(files, path, inputs)
        with self.assertRaises(ValueError):
            verify(files, self.manifest)


if __name__ == '__main__':
    unittest.main()
