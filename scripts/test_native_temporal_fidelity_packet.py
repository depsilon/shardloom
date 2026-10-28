# SPDX-License-Identifier: Apache-2.0
"""Refusal controls for saved temporal source, test and package acceptance."""
import copy
import json
import os
from pathlib import Path
import unittest

from verify_native_temporal_fidelity_packet import object_files, sha, strict_json, verify

DEFAULT_PACKET = Path(__file__).resolve().parents[1] / 'docs/benchmarks/evidence/native-temporal-fidelity-2026-10-27'
PACKET = Path(os.environ.get('SHARDLOOM_TEMPORAL_PACKET', DEFAULT_PACKET))
RECORD_CONTROLS = (
    ('failed_native_gate', 'r63-native-workspace-tests.json', ('status',), 'failed'),
    ('nonzero_exit', 'r63-native-workspace-tests.json', ('returncode',), 1),
    ('undrained_processes', 'r63-native-workspace-tests.json', ('process_group_drained',), False),
    ('timed_out_gate', 'r63-native-workspace-tests.json', ('timed_out',), True),
    ('another_build_commit', 'r64-final-temporal-build.json', ('head',), '0' * 40),
    ('dirty_final_build', 'r64-final-temporal-build.json', ('working_tree',), ' M Cargo.toml'),
    ('another_build_configuration', 'r64-final-temporal-build.json', ('command',), ['cargo', 'check']),
    ('another_runtime_profile', 'temporal-native-r5/manifest.json', ('profile',), 'release'),
    ('public_skip', 'temporal-public-r5.json', ('skipped',), 1),
    ('missing_public_test', 'temporal-public-r5.json', ('tests',), 21),
    ('wrong_public_runtime', 'temporal-public-r5.json', ('products', 'shardloom'), '0' * 64),
    ('package_test_failure', 'installed-wheel-r1/report.json', ('integration', 'failures'), 1),
    ('package_skip', 'installed-wheel-r1/report.json', ('integration', 'skipped'), 1),
    ('another_packaged_binary', 'installed-wheel-r1/report.json', ('integrity', 'cli_sha256'), '0' * 64),
    ('published_artifact', 'installed-wheel-r1/report.json', ('publication_attempted',), True),
    ('failed_uninstall', 'installed-wheel-r1/report.json', ('uninstall_status',), 'failed'),
    ('unbound_native_source', 'native-test-source-binding.json', ('gate_receipt_sha256',), '0' * 64),
    ('missing_final_source_inventory', 'native-test-source-binding.json', ('final_source_records',), {}),
    ('historical_failure_replaced', 'r48-native-workspace-tests.json', ('status',), 'passed'),
)


class NativeTemporalFidelityPacketTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.manifest = strict_json((PACKET / 'manifest.json').read_bytes())
        cls.archive = (PACKET / 'packet.tar.xz').read_bytes()
        cls.files = object_files(cls.manifest, cls.archive)

    def test_complete_packet_reproduces_original_acceptance(self):
        result = verify(self.files, self.manifest)
        self.assertEqual(result, strict_json((PACKET / 'acceptance.json').read_bytes()))
        self.assertEqual(len(result['observations']), 67)
        self.assertEqual(sum(row['status'] == 'failed' for row in result['observations'].values()), 28)
        self.assertEqual(result['installed_package_tests'], 199)
        self.assertFalse(result['performance_claim'])

    def test_changed_records_are_refused(self):
        for name, file, fields, replacement in RECORD_CONTROLS:
            with self.subTest(control=name):
                changed = dict(self.files)
                value = strict_json(changed['evidence/' + file])
                target = value
                for field in fields[:-1]:
                    target = target[field]
                target[fields[-1]] = replacement
                changed['evidence/' + file] = json.dumps(value).encode()
                with self.assertRaises(ValueError):
                    verify(changed, self.manifest)

    def test_changed_archive(self):
        archive = bytearray(self.archive)
        archive[len(archive) // 2] ^= 1
        with self.assertRaisesRegex(ValueError, 'archive identity'):
            object_files(self.manifest, bytes(archive))

    def test_changed_packet_head(self):
        manifest = copy.deepcopy(self.manifest)
        manifest['source_head'] = '0' * 40
        with self.assertRaisesRegex(ValueError, 'packet identity'):
            verify(self.files, manifest)

    def test_changed_native_log(self):
        changed = dict(self.files)
        changed['evidence/r63-native-workspace-tests.log'] += b'changed'
        with self.assertRaisesRegex(ValueError, 'gate log changed'):
            verify(changed, self.manifest)

    def test_missing_historical_failure(self):
        changed = dict(self.files)
        del changed['evidence/r48-native-workspace-tests.json']
        with self.assertRaisesRegex(ValueError, 'observations incomplete'):
            verify(changed, self.manifest)

    def test_changed_test_coverage_with_relinked_log_digest(self):
        changed = dict(self.files)
        name = 'evidence/r63-native-workspace-tests'
        log = changed[name + '.log']
        prefix = b'test result: ok. '
        offset = log.index(prefix) + len(prefix)
        end = log.index(b' passed;', offset)
        changed[name + '.log'] = log[:offset] + str(int(log[offset:end]) + 1).encode() + log[end:]
        gate = strict_json(changed[name + '.json'])
        gate['log_sha256'] = sha(changed[name + '.log'])
        changed[name + '.json'] = json.dumps(gate).encode()
        with self.assertRaisesRegex(ValueError, 'test coverage changed'):
            verify(changed, self.manifest)


if __name__ == '__main__':
    unittest.main()
