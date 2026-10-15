# SPDX-License-Identifier: Apache-2.0
"""Unit-only 0.6.0 smoke fixtures; no registry observation or package execution."""
import copy
import hashlib
import json
from pathlib import Path, PurePosixPath
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
import check_registry_bundled_proof as checker
from release_channel_contract import PUBLISHED_REGISTRY_BUILD_IDENTITIES

MODULE_SHA = 'a' * 64
SOURCE = 'b' * 40
recorded = json.loads((ROOT / 'docs/release/channel-proofs/testpypi-v0.6.0-transcript.json').read_text())
programs = [step['command'][3] for step in recorded['bundled_cli_supplemental_proof']['steps']
            if step['name'] == 'bundled_cli_complete_values']
assert len(programs) == 1
SMOKE = programs[0]
SMOKE_SHA = hashlib.sha256(SMOKE.encode()).hexdigest()
assert SMOKE_SHA == 'ff03c39a574abf01ed3e7315c9982fa48a0de006332dcda9d1214cc4dfa4ed3b'


class NativeBundledProofTests(unittest.TestCase):
    def fixture(self, channel='testpypi'):
        # The retained historical full-workflow fixture supplies the common
        # contract. Version, source and added native rows below are synthetic.
        base = ROOT / f'docs/release/channel-proofs/{channel}-v0.5.1-transcript.json'
        proof = json.loads(base.read_text().replace('0.5.1', '0.6.0'))
        supplement = proof['bundled_cli_supplemental_proof']
        supplement['release_source_commit'] = SOURCE
        supplement['shared_smoke_sha256'] = SMOKE_SHA
        supplement['steps'][3]['command'][3] = SMOKE
        result = supplement['result']
        result['native_module_proof'] = {
            'library': str(PurePosixPath(result['package_path']).parent / '_native.abi3.so'),
            'library_sha256': MODULE_SHA,
            'execution_provider': 'shared_shardloom_native',
            'retained_after_session_close': True,
            'explicit_row_export_complete': True,
            'rows': [{'value': number} for number in range(4)],
        }
        return proof

    def validate(self, proof, *, module_sha=MODULE_SHA, rebind_capture=True):
        supplement = proof['bundled_cli_supplemental_proof']
        capture = json.dumps(supplement['result']).encode()
        if rebind_capture:
            supplement['steps'][3]['stdout_sha256'] = hashlib.sha256(capture).hexdigest()
        with patch.dict(checker.PUBLISHED_REGISTRY_BUNDLED_SMOKE_SHA256, {'0.6.0': SMOKE_SHA}):
            return checker.bundled_registry_proof_blockers(
                proof, channel_id=proof['channel_id'], package_version='0.6.0',
                runtime_source_commit=SOURCE, smoke_stdout=capture,
                native_library_sha256=module_sha)

    def test_accepts_both_complete_native_fixtures(self):
        for channel in ('testpypi', 'pypi'):
            with self.subTest(channel=channel):
                self.assertEqual(self.validate(self.fixture(channel)), [])

    def test_requires_independently_observed_module_hash(self):
        for digest in (None, 'bad', 'f' * 64, True):
            with self.subTest(digest=digest):
                errors = self.validate(self.fixture(), module_sha=digest)
                self.assertIn('match the observed installed wheel payload', '; '.join(errors))

    def test_rejects_rebound_native_result_and_identity_changes(self):
        # Rebinding capture hashes cannot make changed semantics acceptable.
        changes = [
            (None, None, 'installed native module and retained-result proof'),
            (('library_sha256',), 'f' * 64, 'observed installed wheel payload'),
            (('library_sha256',), None, 'observed installed wheel payload'),
            (('execution_provider',), 'other_engine', 'shared ShardLoom provider'),
            (('retained_after_session_close',), False, 'complete after session closure'),
            (('retained_after_session_close',), 1, 'complete after session closure'),
            (('explicit_row_export_complete',), False, 'complete after session closure'),
            (('explicit_row_export_complete',), 1, 'complete after session closure'),
            (('rows',), [], 'four exact typed rows'),
            (('rows',), [{'value': n} for n in range(3)], 'four exact typed rows'),
            (('rows', 0, 'value'), False, 'four exact typed rows'),
            (('rows', 0, 'value'), 0.0, 'four exact typed rows'),
            (('rows', 0, 'value'), '0', 'four exact typed rows'),
            (('library',), '/tmp/source/shardloom/_native.abi3.so', 'same installed package'),
            (('library',), '/tmp/../source/shardloom/_native.abi3.so', 'same installed package'),
            (('library',), 'shardloom/_native.abi3.so', 'same installed package'),
            (('library',), None, 'same installed package'),
            (('unsupported_extra_claim',), True, 'complete approved field set'),
        ]
        for path, value, expected in changes:
            with self.subTest(path=path, value=value):
                proof = self.fixture()
                if path is None:
                    proof['bundled_cli_supplemental_proof']['result']['native_module_proof'] = value
                else:
                    target = proof['bundled_cli_supplemental_proof']['result']['native_module_proof']
                    for key in path[:-1]:
                        target = target[key]
                    target[path[-1]] = value
                errors = '; '.join(self.validate(proof))
                self.assertNotIn('result must equal the captured', errors)
                self.assertIn(expected, errors)
        proof = self.fixture()
        del proof['bundled_cli_supplemental_proof']['result']['native_module_proof']['rows']
        self.assertIn('complete approved field set', '; '.join(self.validate(proof)))

    def test_preserves_full_workflow_requirements_for_060(self):
        for path, value, expected in (
            (('exact_results',), [], 'DataFrame and two SQL results'),
            (('native_vortex_roundtrip_values',), [], 'native_vortex_roundtrip_values'),
            (('analytic_frame_values',), [], 'analytic_frame_values'),
            (('correlated_scalar_subquery_values',), [], 'correlated_scalar_subquery_values'),
            (('incremental_complete_values',), {}, 'five complete incremental workflows'),
            (('incremental_complete_values', 'streamed_left_join', 'final_report_success'), False, 'final_report_success'),
            (('incremental_complete_values', 'sparse_pivot', 'owned_workspace_empty'), False, 'owned_workspace_empty'),
        ):
            with self.subTest(path=path):
                proof = self.fixture()
                target = proof['bundled_cli_supplemental_proof']['result']
                for key in path[:-1]:
                    target = target[key]
                target[path[-1]] = value
                self.assertIn(expected, '; '.join(self.validate(proof)))

    def test_retains_program_and_cli_argument_binding(self):
        for change, expected in (
            ('short', 'approved complete-value smoke program'),
            ('extra', 'approved complete-value smoke program'),
            ('program', 'approved complete-value smoke program'),
            ('digest', 'bundled CLI SHA256 argument'),
            ('capture', 'captured smoke stdout must match'),
        ):
            with self.subTest(change=change):
                proof = self.fixture()
                supplement = proof['bundled_cli_supplemental_proof']
                command = supplement['steps'][3]['command']
                if change == 'short':
                    command.pop()
                elif change == 'extra':
                    command.append('/opt/homebrew/bin/shardloom')
                elif change == 'program':
                    command[3] = 'pass'
                elif change == 'digest':
                    command[4] = 'f' * 64
                elif change == 'capture':
                    supplement['steps'][3]['stdout_sha256'] = 'f' * 64
                self.assertIn(expected, '; '.join(self.validate(proof, rebind_capture=change != 'capture')))

    def test_preserves_all_actual_historical_proofs(self):
        base = ROOT / 'docs/release/channel-proofs'
        for version in ('0.2.4', '0.3.0', '0.3.1', '0.3.2', '0.3.3', '0.4.0', '0.5.1'):
            for channel in ('testpypi', 'pypi'):
                with self.subTest(version=version, channel=channel):
                    proof = json.loads((base / f'{channel}-v{version}-transcript.json').read_text())
                    capture = (base / f'{channel}-v{version}-bundled-smoke.stdout.json').read_bytes()
                    self.assertEqual(checker.bundled_registry_proof_blockers(
                        proof, channel_id=channel, package_version=version,
                        runtime_source_commit=PUBLISHED_REGISTRY_BUILD_IDENTITIES[version]['testpypi']['source_commit'],
                        smoke_stdout=capture), [])


if __name__ == '__main__':
    unittest.main()
