# SPDX-License-Identifier: Apache-2.0
"""Versioned native and context/result proof rejection checks; no package execution."""
import copy
import hashlib
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
import registry_release_evidence as evidence
import check_registry_bundled_proof as checker

if __package__:
    from . import test_registry_release_evidence as original_evidence
    from . import test_registry_native_bundled_proof as original_bundled
    from . import test_registry_native_consumer as original_consumer
else:
    import test_registry_release_evidence as original_evidence
    import test_registry_native_bundled_proof as original_bundled
    import test_registry_native_consumer as original_consumer

recorded = json.loads((ROOT / 'docs/release/channel-proofs/testpypi-v0.7.0-transcript.json').read_text())
smokes = [step['command'][3] for step in recorded['bundled_cli_supplemental_proof']['steps']
          if step['name'] == 'bundled_cli_complete_values']
assert len(smokes) == 1
SMOKE = smokes[0]
SMOKE_SHA = hashlib.sha256(SMOKE.encode()).hexdigest()
assert SMOKE_SHA == 'c16d31b93d6c53208b4a0c42ab9cf725f1867474c4ce08d417556ea17bc9e273'


def native_fixture(version, linux):
    return original_evidence.native_fixture(linux, package_version=version)


class CandidateNativeConsumerTests(unittest.TestCase):
    def test_both_release_contracts_and_platforms_are_accepted(self):
        for version, tests in (('0.6.0', 80), ('0.7.0', 121)):
            for linux in (False, True):
                with self.subTest(version=version, linux=linux):
                    files, distribution, programs = native_fixture(version, linux)
                    result = evidence.verify_native_proof(
                        files, distribution, programs, package_version=version)
                    self.assertEqual(result['integration']['tests'], tests)

    def test_rebound_other_release_counts_are_rejected(self):
        for version, wrong_count in (('0.6.0', 121), ('0.7.0', 80)):
            for linux in (False, True):
                with self.subTest(version=version, linux=linux):
                    files, distribution, programs = native_fixture(version, linux)
                    proof = json.loads(files['proof/report.json'])
                    proof['integration']['tests'] = wrong_count
                    files['proof/report.json'] = evidence.json_bytes(proof)
                    files['proof/integration.json'] = evidence.json_bytes(proof['integration'])
                    with self.assertRaisesRegex(ValueError, 'complete zero-skip'):
                        evidence.verify_native_proof(files, distribution, programs, package_version=version)

    def test_other_release_linux_payload_count_is_rejected(self):
        for version, wrong_count in (('0.6.0', 275), ('0.7.0', 232)):
            with self.subTest(version=version):
                files, distribution, programs = native_fixture(version, True)
                proof = json.loads(files['proof/report.json'])
                proof['linux_audit']['unchanged_payload_count'] = wrong_count
                files['proof/report.json'] = evidence.json_bytes(proof)
                with self.assertRaisesRegex(ValueError, 'Linux portability'):
                    evidence.verify_native_proof(files, distribution, programs, package_version=version)

    def test_unapproved_release_is_rejected(self):
        files, distribution, programs = native_fixture('0.7.0', False)
        for version in ('0.8.0', '', None, []):
            with self.subTest(version=version), self.assertRaisesRegex(ValueError, 'approved expectations'):
                evidence.verify_native_proof(files, distribution, programs, package_version=version)

    def test_selected_070_consumer_binds_the_actual_program_policy(self):
        artifacts, rows = [], []
        for index, linux in enumerate((False, True)):
            files, distribution, programs = native_fixture('0.7.0', linux)
            artifacts.append(distribution)
            row = evidence.verify_native_proof(files, distribution, programs, package_version='0.7.0')
            row.update(workflow_artifact_id=index + 1,
                workflow_artifact_name='native-registry-proof-' + ('linux' if linux else 'macos'),
                workflow_run_id=44, source_commit='d' * 40,
                workflow_artifact_archive_sha256='e' * 64, workflow_artifact_archive_size_bytes=100)
            rows.append(row)
        provenance = dict(workflow_run_id=44, source_commit='d' * 40,
                          native_installed_wheel_proofs=rows)
        policy = {'0.7.0': {name: hashlib.sha256(program.encode()).hexdigest()
                           for name, program in programs.items()}}
        consumer = original_consumer.consumer
        with patch.object(consumer, 'SELECTED_PACKAGE_RELEASE_VERSION', '0.7.0'), \
                patch.object(consumer, 'PUBLISHED_REGISTRY_NATIVE_PROOF_PROGRAM_SHA256', policy):
            self.assertEqual(consumer.registry_native_installation_blockers('testpypi', artifacts, provenance), [])
            changed = copy.deepcopy(provenance)
            changed['native_installed_wheel_proofs'][0]['integration']['tests'] = 80
            self.assertTrue(consumer.registry_native_installation_blockers('testpypi', artifacts, changed))


class CandidateBundledConsumerTests(unittest.TestCase):
    def fixture(self, channel='testpypi'):
        proof = original_bundled.NativeBundledProofTests().fixture(channel)
        proof = json.loads(json.dumps(proof).replace('0.6.0', '0.7.0'))
        supplement = proof['bundled_cli_supplemental_proof']
        supplement['shared_smoke_sha256'] = SMOKE_SHA
        supplement['steps'][3]['command'][3] = SMOKE
        result = supplement['result']
        result['typed_unsupported_error'] = True
        result['context_result_proof'] = {
            'default_transport': 'native', 'result_type': 'Result',
            'retained_after_context_close': True,
            'from_result_values': [{'value': 2}, {'value': 3}],
            'rows': [{'value': number} for number in range(4)],
            'typed_closed_error': True,
            'native_module_origin': 'verified_installed_python_wheel',
        }
        return proof

    def validate(self, proof):
        supplement = proof['bundled_cli_supplemental_proof']
        capture = json.dumps(supplement['result']).encode()
        supplement['steps'][3]['stdout_sha256'] = hashlib.sha256(capture).hexdigest()
        with patch.dict(checker.PUBLISHED_REGISTRY_BUNDLED_SMOKE_SHA256, {'0.7.0': SMOKE_SHA}):
            return checker.bundled_registry_proof_blockers(
                proof, channel_id=proof['channel_id'], package_version='0.7.0',
                runtime_source_commit=original_bundled.SOURCE, smoke_stdout=capture,
                native_library_sha256=original_bundled.MODULE_SHA)

    def test_both_new_bundled_contracts_pass(self):
        for channel in ('testpypi', 'pypi'):
            with self.subTest(channel=channel):
                self.assertEqual(self.validate(self.fixture(channel)), [])

    def test_rebound_context_and_typed_error_claims_fail(self):
        changes = [
            (('context_result_proof',), None),
            (('context_result_proof', 'default_transport'), 'process'),
            (('context_result_proof', 'result_type'), 'LegacyResult'),
            (('context_result_proof', 'retained_after_context_close'), 1),
            (('context_result_proof', 'from_result_values'), [{'value': 2}]),
            (('context_result_proof', 'rows', 0, 'value'), 0.0),
            (('context_result_proof', 'typed_closed_error'), False),
            (('context_result_proof', 'native_module_origin'), 'source_checkout'),
            (('context_result_proof', 'unproved_extra_claim'), True),
            (('typed_unsupported_error',), None),
            (('typed_unsupported_error',), 1),
        ]
        for path, value in changes:
            with self.subTest(path=path, value=value):
                proof = self.fixture()
                target = proof['bundled_cli_supplemental_proof']['result']
                for key in path[:-1]:
                    target = target[key]
                target[path[-1]] = value
                errors = self.validate(proof)
                self.assertTrue(errors)
                self.assertNotIn('result must equal the captured', '; '.join(errors))

    def test_new_contract_still_requires_old_native_and_workflow_proof(self):
        for field in ('native_module_proof', 'exact_results', 'incremental_complete_values'):
            with self.subTest(field=field):
                proof = self.fixture()
                del proof['bundled_cli_supplemental_proof']['result'][field]
                self.assertTrue(self.validate(proof))


if __name__ == '__main__':
    unittest.main()
