# SPDX-License-Identifier: Apache-2.0
"""Reject 1.0.0 proof downgrades while retaining historical release contracts."""
import copy
import hashlib
import json
import unittest
from unittest.mock import patch

import registry_release_evidence as evidence
import check_registry_bundled_proof as checker

if __package__:
    from . import test_registry_070_contract as previous
    from . import test_registry_release_evidence as fixtures
    from . import test_registry_native_consumer as native_consumer
    from . import test_registry_native_bundled_proof as bundled_fixtures
else:
    import test_registry_070_contract as previous
    import test_registry_release_evidence as fixtures
    import test_registry_native_consumer as native_consumer
    import test_registry_native_bundled_proof as bundled_fixtures

# The release keeps the established workflow proof and changes its exact
# package-version binding. This inert fixture executes no package program.
SMOKE = previous.SMOKE.replace('0.7.0', '1.0.0')
SMOKE_SHA = hashlib.sha256(SMOKE.encode()).hexdigest()
assert SMOKE_SHA == '9d2edc55b3e67886895155a6ac8abb99e13a4a3e7a1f39a32844b884e1578278'


class Release100NativeProofTests(unittest.TestCase):
    def test_all_versioned_suites_and_platforms_remain_accepted(self):
        expected = {'0.6.0': (80, 232), '0.7.0': (121, 275), '1.0.0': (140, 280)}
        for version, (tests, payloads) in expected.items():
            self.assertEqual(evidence.NATIVE_PROOF_EXPECTATIONS[version],
                             {'tests': tests, 'linux_payloads': payloads})
            for linux in (False, True):
                with self.subTest(version=version, linux=linux):
                    files, artifact, programs = fixtures.native_fixture(linux, package_version=version)
                    result = evidence.verify_native_proof(files, artifact, programs, package_version=version)
                    self.assertEqual(result['integration'],
                        {'tests': tests, 'failures': 0, 'errors': 0, 'skipped': 0, 'status': 'passed'})

    def test_rebound_older_suite_counts_are_rejected(self):
        for linux in (False, True):
            for count in (80, 121, 139):
                with self.subTest(linux=linux, count=count):
                    files, artifact, programs = fixtures.native_fixture(linux, package_version='1.0.0')
                    report = json.loads(files['proof/report.json'])
                    report['integration']['tests'] = count
                    files['proof/report.json'] = evidence.json_bytes(report)
                    files['proof/integration.json'] = evidence.json_bytes(report['integration'])
                    with self.assertRaisesRegex(ValueError, 'complete zero-skip'):
                        evidence.verify_native_proof(files, artifact, programs, package_version='1.0.0')

    def test_rebound_linux_payload_counts_are_rejected(self):
        for count in (232, 275, 279, 281):
            with self.subTest(count=count):
                files, artifact, programs = fixtures.native_fixture(True, package_version='1.0.0')
                report = json.loads(files['proof/report.json'])
                report['linux_audit']['unchanged_payload_count'] = count
                files['proof/report.json'] = evidence.json_bytes(report)
                with self.assertRaisesRegex(ValueError, 'Linux portability'):
                    evidence.verify_native_proof(files, artifact, programs, package_version='1.0.0')

    def test_selected_100_consumer_requires_the_actual_program_policy(self):
        artifacts, rows = [], []
        for index, linux in enumerate((False, True)):
            files, artifact, programs = fixtures.native_fixture(linux, package_version='1.0.0')
            artifacts.append(artifact)
            row = evidence.verify_native_proof(files, artifact, programs, package_version='1.0.0')
            row.update(workflow_artifact_id=index + 1,
                workflow_artifact_name='native-registry-proof-' + ('linux' if linux else 'macos'),
                workflow_run_id=44, source_commit='d' * 40,
                workflow_artifact_archive_sha256='e' * 64, workflow_artifact_archive_size_bytes=100)
            rows.append(row)
        provenance = dict(workflow_run_id=44, source_commit='d' * 40,
                          native_installed_wheel_proofs=rows)
        consumer = native_consumer.consumer
        policy = {'1.0.0': {name: hashlib.sha256(value.encode()).hexdigest()
                            for name, value in programs.items()}}
        with patch.object(consumer, 'SELECTED_PACKAGE_RELEASE_VERSION', '1.0.0'), \
                patch.object(consumer, 'PUBLISHED_REGISTRY_NATIVE_PROOF_PROGRAM_SHA256', policy):
            self.assertEqual(consumer.registry_native_installation_blockers('testpypi', artifacts, provenance), [])
            changed = copy.deepcopy(provenance)
            changed['native_installed_wheel_proofs'][0]['integration']['tests'] = 121
            self.assertTrue(consumer.registry_native_installation_blockers('testpypi', artifacts, changed))
        with patch.object(consumer, 'SELECTED_PACKAGE_RELEASE_VERSION', '1.0.0'), \
                patch.object(consumer, 'PUBLISHED_REGISTRY_NATIVE_PROOF_PROGRAM_SHA256', {}):
            self.assertTrue(consumer.registry_native_installation_blockers('testpypi', artifacts, provenance))


class Release100BundledProofTests(unittest.TestCase):
    def fixture(self, channel='testpypi'):
        proof = previous.CandidateBundledConsumerTests().fixture(channel)
        proof = json.loads(json.dumps(proof).replace('0.7.0', '1.0.0'))
        supplement = proof['bundled_cli_supplemental_proof']
        supplement['shared_smoke_sha256'] = SMOKE_SHA
        supplement['steps'][3]['command'][3] = SMOKE
        return proof

    def validate(self, proof):
        supplement = proof['bundled_cli_supplemental_proof']
        capture = json.dumps(supplement['result']).encode()
        supplement['steps'][3]['stdout_sha256'] = hashlib.sha256(capture).hexdigest()
        with patch.dict(checker.PUBLISHED_REGISTRY_BUNDLED_SMOKE_SHA256, {'1.0.0': SMOKE_SHA}):
            return checker.bundled_registry_proof_blockers(
                proof, channel_id=proof['channel_id'], package_version='1.0.0',
                runtime_source_commit=bundled_fixtures.SOURCE, smoke_stdout=capture,
                native_library_sha256=bundled_fixtures.MODULE_SHA)

    def test_both_channels_accept_the_complete_workflow_and_context_proof(self):
        for channel in ('testpypi', 'pypi'):
            with self.subTest(channel=channel):
                self.assertEqual(self.validate(self.fixture(channel)), [])

    def test_rebound_context_and_native_claims_are_rejected(self):
        changes = [
            (('context_result_proof', 'default_transport'), 'process'),
            (('context_result_proof', 'retained_after_context_close'), False),
            (('context_result_proof', 'rows', 0, 'value'), 0.0),
            (('context_result_proof', 'typed_closed_error'), False),
            (('context_result_proof', 'extra_claim'), True),
            (('native_module_proof', 'library_sha256'), 'f' * 64),
            (('native_module_proof', 'execution_provider'), 'other'),
            (('native_module_proof', 'retained_after_session_close'), False),
            (('typed_unsupported_error',), 1),
        ]
        for path, value in changes:
            with self.subTest(path=path):
                proof = self.fixture()
                target = proof['bundled_cli_supplemental_proof']['result']
                for key in path[:-1]:
                    target = target[key]
                target[path[-1]] = value
                errors = self.validate(proof)
                self.assertTrue(errors)
                self.assertNotIn('result must equal the captured', '; '.join(errors))

    def test_100_cannot_downgrade_to_the_legacy_proof_surface(self):
        for field in ('native_module_proof', 'context_result_proof', 'exact_results',
                      'incremental_complete_values', 'analytic_frame_values',
                      'native_vortex_roundtrip_values', 'correlated_scalar_subquery_values'):
            with self.subTest(field=field):
                proof = self.fixture()
                del proof['bundled_cli_supplemental_proof']['result'][field]
                self.assertTrue(self.validate(proof))


if __name__ == '__main__':
    unittest.main()
