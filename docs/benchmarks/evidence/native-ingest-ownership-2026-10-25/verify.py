#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Replay saved ownership/dependency evidence without executing engine code.

Historical absolute paths are archive keys only. This verifier reads the packet
directory and never opens those original files or reconstructs a dataset.
"""
from __future__ import annotations

import argparse
import copy
import gzip
import hashlib
import io
import json
import math
from pathlib import Path
import re
import tarfile
import tomllib

ROOT = Path('/Users/dylan/LocalData/shardloom/ingest-ownership-20261025')
REPO = Path('/Users/dylan/.codex/worktrees/public-docs-api/shardloom-local-repo')
SOURCE_SHA = 'a390f6cb782f6aaef278c72fc1dd86c4f30bc843ebab3c159e9bd4d45ddb079f'
OUTPUT_SHA = '95a393d153a8c2e1ffeb5084b2656ac61465c6ef9dad435d032365844f1ac057'
REFERENCE_SHA = 'fc5780da415dc33bd06ed0879db07fc040db5dd419e3c37ecde7cd40a315696b'
PROTOCOL_SHA = 'b49eb17934df933f6542afd242b99b72e096125ffaef7d0599acfd8aea58f716'
PREFIX = 'public_workflow_preparation_'
TESTS = re.compile(r'^test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;', re.M)
VALIDATION = ROOT / 'validation'


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def file_identity(reference):
    return {key: reference[key] for key in ('path', 'sha256')} | {
        'bytes': reference.get('bytes', reference.get('size_bytes'))}


def strict(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, 'duplicate JSON key')
            result[key] = value
        return result

    def finite(value):
        result = float(value)
        require(math.isfinite(result), 'nonfinite JSON number')
        return result

    def invalid(value):
        raise ValueError('nonfinite JSON constant: ' + value)

    return json.loads(raw, object_pairs_hook=unique, parse_float=finite, parse_constant=invalid)


class Packet:
    def __init__(self, directory):
        self.manifest = strict((directory / 'manifest.json').read_bytes())
        m = self.manifest
        require(m['schema_version'] == 'shardloom.ingest_ownership.packet.v1', 'packet schema')
        require(digest(Path(__file__).read_bytes()) == m['verifier_sha256'], 'verifier identity')
        self.files, expected = {}, {}
        for row in m['files']:
            require(row['path'] not in self.files, 'duplicate original path')
            require(re.fullmatch(r'[0-9a-f]{64}', row['sha256']), 'invalid object digest')
            require(row['member'] == 'objects/' + row['sha256'], 'unsafe object path')
            identity = (row['bytes'], row['sha256'])
            require(type(row['bytes']) is int and 0 <= row['bytes'] <= 64 * 2**20, 'object size cap')
            require(expected.setdefault(row['member'], identity) == identity, 'object identity collision')
            self.files[row['path']] = row
        require(sum(size for size, _ in expected.values()) <= 256 * 2**20, 'packet expansion cap')
        archive = m['archive']
        require(Path(archive['path']).name == archive['path'], 'unsafe archive filename')
        packed = (directory / archive['path']).read_bytes()
        require((len(packed), digest(packed)) == (archive['bytes'], archive['sha256']), 'archive identity')
        self.objects = {}
        with tarfile.open(fileobj=io.BytesIO(packed), mode='r:xz') as stream:
            for member in stream:
                require(member.name in expected and member.name not in self.objects, 'unexpected archive member')
                size, sha = expected[member.name]
                require(member.isfile() and member.size == size, 'archive member kind/size')
                raw = stream.extractfile(member).read()
                require((len(raw), digest(raw)) == (size, sha), 'archive member identity')
                self.objects[member.name] = raw
        require(set(self.objects) == set(expected), 'missing archive members')
        selection = self.read(ROOT / 'ownership-evidence-selection-r1.json')
        selected = {x['path']: (x['bytes'], x['sha256']) for x in selection['files']}
        additions = {x['path']: (x['bytes'], x['sha256']) for x in m['additional_files']}
        require(not selected.keys() & additions.keys(), 'duplicate selection additions')
        require({k: (v['bytes'], v['sha256']) for k, v in self.files.items()} == {**selected, **additions},
                'selected evidence coverage changed')

    def raw(self, path):
        return self.objects[self.files[str(path)]['member']]

    def read(self, path):
        raw = self.raw(path)
        return strict(gzip.decompress(raw) if str(path).endswith('.gz') else raw)

    def linked(self, reference):
        raw = self.raw(reference['path'])
        require(digest(raw) == reference['sha256'], 'linked digest: ' + reference['path'])
        size = reference.get('bytes', reference.get('size_bytes'))
        require(size is None or len(raw) == size, 'linked size: ' + reference['path'])
        return self.read(reference['path'])

    def verified_raw(self, reference, base=None):
        path = Path(reference['path'])
        path = path if path.is_absolute() else base / path
        raw = self.raw(path)
        require(digest(raw) == reference['sha256'], 'linked raw digest: ' + str(path))
        size = reference.get('bytes', reference.get('size_bytes'))
        require(size is None or size == len(raw), 'linked raw size: ' + str(path))
        return raw


def test_counts(raw):
    text = raw.decode()
    require('test result: FAILED.' not in text, 'failed Rust tests')
    found = [tuple(map(int, values)) for values in TESTS.findall(text)]
    return {key: sum(row[index] for row in found)
            for index, key in enumerate(('passed', 'failed', 'ignored'))}


def historical_validation(packet):
    receipt = packet.read(VALIDATION / 'ingest-transfer-broad-r12.json')
    require(receipt['status'] == 'COMPLETE', 'ownership validation completion')
    require([c['name'] for c in receipt['commands']] ==
            ['fmt', 'clippy-focused', 'clippy-workspace', 'test-workspace', 'test-vortex-lib'],
            'ownership validation coverage')
    totals = dict.fromkeys(('passed', 'failed', 'ignored'), 0)
    for command in receipt['commands']:
        require(command['exit_code'] == 0 and not command['bounded_helper_guard_failures'],
                'ownership validation command')
        for kind in ('stdout', 'stderr'):
            raw = packet.verified_raw({'path': command[kind + '_log'],
                                      'bytes': command[kind + '_bytes'],
                                      'sha256': command[kind + '_sha256']}, VALIDATION)
            if kind == 'stdout' and command['name'].startswith('test-'):
                counts = test_counts(raw)
                require(counts['passed'] > 0, 'empty ownership test run')
                totals = {key: totals[key] + counts[key] for key in totals}
    require(totals == {key: receipt['test_totals'][key] for key in totals}, 'ownership test totals')
    source = receipt['source_hashes']
    before = packet.read(VALIDATION / source['before_manifest'])
    after = packet.read(VALIDATION / source['after_manifest'])
    require(before == after and not source['changed_hash_paths'], 'ownership tested-source continuity')
    freeze = packet.read(VALIDATION / 'ingest-candidate-freeze-r13.json')
    build = packet.read(ROOT / 'candidate-build-r1.json')
    require(freeze['status'] == 'COMPLETE' and freeze['commit'] == build['source_revision']
            and freeze['tree'] == build['source_tree'], 'ownership release-build linkage')
    require(freeze['build_features'] == ['release-user-surfaces'], 'ownership release features')
    inventory = packet.read(VALIDATION / freeze['tracked_files']['before_manifest'])
    require(inventory == packet.read(VALIDATION / freeze['tracked_files']['after_manifest']),
            'ownership release-source continuity')
    for command in freeze['commands']:
        require(command['exit_code'] == 0 and not command['bounded_helper_guard_failures'], 'ownership release command')
        for kind in ('stdout', 'stderr'):
            packet.verified_raw(command[kind], VALIDATION)
    for artifact in freeze['frozen_executables']:
        ref = build['artifacts'][artifact['name']]
        require(artifact['source_sha256'] == artifact['copy_sha256'] == ref['sha256']
                and artifact['source_size_bytes'] == artifact['copy_size_bytes'] == ref['bytes'],
                'ownership frozen executable identity')
    return {'commands': len(receipt['commands']), 'tests': totals,
            'frozen_source_revision': build['source_revision']}


def combined_validation(packet):
    receipt = packet.read(VALIDATION / 'ingest-combined-selected-r15.json')
    require(receipt['status'] == 'COMPLETE' and len(receipt['commands']) == 9, 'combined gate coverage')
    expected = ['01-fmt', '02-clippy-workspace', '03-test-workspace',
                '04-clippy-focused-release', '05-test-workspace-release-feature',
                '06-check-all-features', '07-check-no-default-features',
                '08-check-rust-1-96-no-default', '09-check-rust-1-96-release-surfaces']
    require([c['name'] for c in receipt['commands']] == expected, 'combined gate identities')
    totals = dict.fromkeys(('passed', 'failed', 'ignored'), 0)
    for command in receipt['commands']:
        require(command['status'] == 'passed' and command['returncode'] == 0
                and not command['timed_out'] and command['interrupted_signal'] is None
                and command['process_group_drained'] and command['direct_child_reaped'], 'combined gate outcome')
        require(command['source_hashes_match_before_after_command']
                and not command['source_hash_changed_paths'] and not command['active_jobs_after_command'],
                'combined gate lifecycle')
        raw = packet.verified_raw(command['log'], VALIDATION)
        if 'test_counts' in command:
            counts = test_counts(raw)
            require(counts == {key: command['test_counts'][key] for key in totals}, 'combined test counts')
            require(counts['passed'] > 0 and counts['failed'] == 0, 'combined test outcomes')
            totals = {key: totals[key] + counts[key] for key in totals}
    require(totals == {'passed': 9364, 'failed': 0, 'ignored': 35}, 'combined aggregate counts')
    before = packet.read(VALIDATION / 'ingest-combined-source-before-r15.json')
    after = packet.read(VALIDATION / 'ingest-combined-source-after-r15.json')
    require(before['files'] == after['files'] and len(before['files']) == 2858, 'combined source continuity')
    return {'gates': 9, 'tests': totals, 'source_files': len(before['files']),
            'excluded_test_scope': 'platform-specific and explicit opt-in benchmarks or fixture helpers'}


def combined_runtime_validation(packet):
    build = packet.read(ROOT / 'combined-build-r1.json')
    primary = packet.linked(build['primary_verification'])
    receipt = packet.linked(build['executor_receipt'])
    require(build['status'] == primary['status'] == 'passed', 'combined build acceptance')
    require(receipt['status'] == 'NEEDS_REASONING' and receipt['failure']['type'] == 'EvidenceMismatch'
            and primary['original_receipt_status_preserved'] == receipt['status'], 'original runtime receipt preserved')
    require(primary['original_executor_receipt'] == build['executor_receipt'], 'runtime primary linkage')
    require(build['source_revision'] == primary['source_revision'] == receipt['source_commit']
            == '23807cb70e13ba2981140a895798e9aff99fb9da', 'combined runtime revision')
    require(build['source_tree'] == primary['source_tree'] == receipt['base_tree'], 'combined runtime tree')
    require(build['package_feature_union'] == 'release-user-surfaces', 'combined runtime features')
    expected = ['build-native-python-release', 'build-cli-release-user-surfaces',
                'otool_native_library', 'otool_cli_binary', 'installed-native-wheel-proof']
    require([c['name'] for c in receipt['commands']] == expected
            == primary['verified_commands'], 'combined runtime gate coverage')
    for command in receipt['commands']:
        require(command['status'] == 'passed' and command['returncode'] == 0
                and not command['timed_out'] and command['interrupted_signal'] is None
                and command['process_group_drained'] and command['direct_child_reaped'], 'combined runtime gate')
        raw = packet.verified_raw({'path': command['log_path'], 'bytes': command['log_bytes'],
                                  'sha256': command['log_sha256']})
        if command['name'].startswith('otool_'):
            require(all(value not in raw for value in (b'/opt/homebrew/', b'Python.framework', b'libpython')),
                    'native package Python linkage')
    manifests = []
    for key in ('source_before', 'source_after'):
        ref = receipt[key]
        path = VALIDATION / ref['manifest']
        manifests.append(packet.linked({'path': str(path), 'sha256': ref['manifest_sha256']}))
    require(manifests[0] == manifests[1], 'runtime source continuity')
    source = manifests[0]['files']
    tested = packet.read(VALIDATION / 'ingest-combined-source-after-r15.json')['files']
    keys = ('path', 'mode', 'size_bytes', 'working_sha256')
    require(len(source) == len(tested) == 2858, 'runtime source coverage')
    require([{k: row[k] for k in keys} for row in source] ==
            [{k: row[k] for k in keys} for row in tested], 'runtime tested-source binding')
    index_changes = [row['path'] for row, prior in zip(source, tested)
                     if row['index_blob'] != prior['index_blob']]
    require(sorted(index_changes) == sorted([
        'Cargo.toml', 'Cargo.lock', '.github/workflows/ci.yml',
        '.github/workflows/codeql-analysis.yml', '.github/workflows/pypi-publish-draft.yml',
        '.github/workflows/scorecard.yml', 'python/THIRD_PARTY_LICENSES/manifest.json']), 'historical index scope')
    binding = primary['tested_source_binding']
    require(binding['compared_fields'] == list(keys) and binding['working_source_bytes_identical']
            and binding['commit_updated_index_metadata_only'] == index_changes, 'primary source binding')
    packet.linked(binding['r15_manifest'])
    packet.linked(binding['r16_manifest'])
    manifest = packet.linked(build['tracked_source_manifest'])
    require(manifest['commit'] == build['source_revision'] and manifest['tree'] == build['source_tree'],
            'complete source manifest linkage')
    tracked = {row['path']: row for row in manifest['files']}
    require(len(tracked) == len(manifest['files']) == manifest['tracked_file_count'] == 4994,
            'complete source manifest coverage')
    require(all(row == tracked[row['path']] for row in source), 'release source membership')
    old, new = receipt['prior_receipts_before'], receipt['prior_receipts_after']
    removed, added = sorted(old.keys() - new.keys()), sorted(new.keys() - old.keys())
    require(removed == ['ingest-combined-source-before-r16.json'] and added == [
        'compact-structure-source-anchors-primary-r1.json', 'compact-structure-source-anchors-r1.json',
        'dependency-static-verification-r1.json'], 'preservation inventory explanation')
    require(all(old[key] == new[key] for key in old.keys() & new.keys()), 'prior receipt identities')
    for ref in old.values():
        packet.verified_raw(ref)
    require(receipt['r14_failure_receipt_unchanged'] and receipt['r15_receipt_unchanged']
            and receipt['prior_binary_roots_before'] == receipt['prior_binary_roots_after']
            and receipt['no_task_owned_native_processes_after'] and not receipt['active_native_processes_after'],
            'prior evidence and runtime cleanup')
    frozen = receipt['frozen_runtime']
    native, cli = frozen['native_library'], frozen['cli_binary']
    require(primary['native_library'] == build['artifacts']['native_library'] == file_identity(native)
            and primary['cli'] == build['artifacts']['shardloom'] == file_identity(cli), 'frozen runtime linkage')
    require(frozen['native_build_extension_module'] == '1' and frozen['cli_build_extension_module'] == 'UNSET',
            'separate native and CLI linkage flags')
    require(frozen['native_build_summary']['status'] == 'passed'
            and frozen['native_build_summary']['library_sha256'] == native['sha256'], 'native build identity')
    proof = receipt['wheel_proof']
    report = packet.linked(primary['wheel_report'])
    require(primary['wheel_report']['path'] == proof['report_path']
            and primary['wheel_report']['sha256'] == proof['report_sha256'], 'wheel report linkage')
    expected_integration = {'tests': 140, 'failures': 0, 'errors': 0, 'skipped': 0, 'status': 'passed'}
    require(report['status'] == 'passed' and report['integration'] == expected_integration
            == primary['integration'] == proof['integration'], 'installed native integration')
    require(not report['publication_attempted'] and report['uninstall_status'] == 'passed'
            and report['installed_load']['isolated_install'], 'installed package lifecycle')
    wheel = receipt['wheel_identity']
    require(primary['wheel'] == file_identity(wheel) and report['integrity']['wheel_sha256'] == wheel['sha256']
            and report['integrity']['native_library_sha256'] == native['sha256']
            and report['integrity']['cli_sha256'] == cli['sha256'], 'wheel embedded identities')
    require([step['name'] for step in report['steps']] == [
        'build', 'create_environment', 'install_optional_consumer', 'install', 'load',
        'integration', 'uninstall', 'verify_uninstalled'], 'installed proof coverage')
    wheel_root = Path(proof['report_path']).parent
    require(len(proof['steps']) == len(report['steps']), 'installed proof receipt steps')
    for step, observed in zip(report['steps'], proof['steps']):
        require(step['returncode'] == 0, 'installed proof step')
        require(all(step[key] == observed[key] for key in ('name', 'returncode', 'log', 'log_sha256')),
                'installed proof log linkage')
        raw = packet.verified_raw({'path': str(wheel_root / step['log']),
                                  'bytes': observed['log_bytes'], 'sha256': step['log_sha256']})
        if step['name'] == 'integration':
            require(re.search(rb'Ran 140 tests', raw) and re.search(rb'^OK$', raw, re.M), 'installed raw test totals')
    return {'source_revision': build['source_revision'], 'tested_source_files': len(source),
            'installed_native_tests': 140, 'native_library_sha256': native['sha256'],
            'cli_sha256': cli['sha256'], 'wheel_sha256': wheel['sha256'],
            'original_inventory_mismatch_preserved_and_resolved': True,
            'binary_payloads_reexecuted_or_archived': False, 'publication_attempted': False}


def dependency_validation(packet):
    delta = packet.read(VALIDATION / 'dependency-lock-delta-r1.json')
    require(delta['status'] == 'passed' and delta['before_packages'] == delta['after_packages'] == 452,
            'dependency graph size')
    expected = {'tokio': '1.54.1', 'prost': '0.14.4', 'prost-derive': '0.14.4'}
    require({x['name']: x['version'] for x in delta['added']} == expected
            and {x['name'] for x in delta['removed']} == set(expected), 'dependency scope')
    source = packet.read(VALIDATION / 'dependency-source-primary-verification-r1.json')
    require(source['status'] == 'passed' and source['exact_action_files'] == 14
            and source['action_bytes_verified_against_git_blob_sha'], 'dependency provenance verification')
    crates = packet.read(VALIDATION / 'dependency-crates-r1/registry-provenance.json')
    for crate in crates['crates']:
        require(crate['archive_sha256'] == crate['expected_sha256'], 'registry identity observation')
        new, = [x for x in delta['added'] if x['name'] == crate['name']]
        require(new['checksum'] == crate['archive_sha256'], 'registry checksum linkage')
        for file in crate['root_files']:
            packet.verified_raw({**file, 'path': file['evidence_path']})
        manifest = tomllib.loads(packet.raw(crate['manifest']['evidence_path']).decode())['package']
        require(manifest['name'] == crate['name'] and manifest['version'] == expected[crate['name']]
                and manifest['rust-version'] == '1.85' and manifest['license'] in ('MIT', 'Apache-2.0'),
                'registry metadata/license')
    audit = packet.read(VALIDATION / 'dependency-audit-r1.json')
    require(audit['cargo_deny_status'] == audit['cargo_audit_status'] == 'passed'
            and audit['fallback_dependency_absent'], 'dependency audit')
    audit_run = packet.read(VALIDATION / 'dependency-audit-supervision-r1.json')
    require(audit_run['status'] == 'passed' and audit_run['returncode'] == 0
            and audit_run['process_group_drained'] and audit_run['direct_child_reaped'], 'audit lifecycle')
    packet.verified_raw(audit_run['log'])
    scope = packet.read(VALIDATION / 'dependency-workflow-scope-verification-r1.json')
    require(scope['status'] == 'passed' and scope['only_pins_and_setup_python_version_comment_changed']
            and scope['workflow_triggers_permissions_inputs_and_publication_gates_unchanged'], 'workflow scope')
    ci = packet.read(VALIDATION / 'dependency-ci-gate-matrix-r1.json')
    require(not ci['blockers'] and ci['lane_count'] == 16
            and all(lane['status'] == 'passed' for lane in ci['lanes']), 'workflow gate matrix')
    return {'changed_package_identities': expected, 'rust_audits': 'passed',
            'workflow_pin_uses': sum(scope['counts'].values()), 'workflow_contract_lanes': 16,
            'audit_warning_scope': 'existing paste, anyhow and event-listener warnings retained in raw log; no new waiver'}


def fields(envelope):
    require(envelope['status'] == 'success', 'operation status')
    fallback = envelope['fallback']
    require(fallback['attempted'] is False and fallback['allowed'] is False
            and fallback['engine'] is None, 'fallback boundary')
    result = {}
    for item in envelope['fields']:
        require(item['key'] not in result, 'duplicate report field')
        result[item['key']] = item['value']
    for key, value in result.items():
        if key.endswith(('fallback_attempted', 'external_engine_invoked')):
            require(value is False or value == 'false', 'unsafe execution field: ' + key)
    return result


def rows(envelope):
    f = fields(envelope)
    require(f['public_workflow_external_engine_invoked'] == 'false', 'external execution')
    payloads = [key for key in ('result_jsonl', 'result_values_json') if key in f]
    require(len(payloads) == 1 and f['result_payload_complete'] in (True, 'true'), 'complete result payload')
    payload = f[payloads[0]]
    values = ([strict(line) for line in payload.splitlines()]
              if payloads[0] == 'result_jsonl' else strict(payload))
    require(type(values) is list and len(values) == int(f['output_row_count'])
            and all(type(row) is dict for row in values), 'complete result shape')
    require(isinstance(f['result_schema_format'], str), 'result schema format')
    strict(f['result_schema_json'])
    return f, values


def equivalent(a, b):
    if type(a) is not type(b):
        return False
    if isinstance(a, dict):
        return a.keys() == b.keys() and all(equivalent(a[k], b[k]) for k in a)
    if isinstance(a, list):
        return len(a) == len(b) and all(equivalent(x, y) for x, y in zip(a, b))
    if isinstance(a, float):
        return math.isfinite(a) and math.isfinite(b) and math.isclose(a, b, rel_tol=1e-12, abs_tol=1e-12)
    return a == b


def readback(report):
    for key, expected in {'compared_rows': 99997497, 'compared_columns': 112,
                          'compared_values': 11199719664, 'source_columns': 105,
                          'derived_columns': 7, 'native_completed_executions': 1,
                          'native_prepared_source_opens': 1, 'native_split_tasks_after_drain': 0}.items():
        require(report[key] == expected, 'readback coverage: ' + key)
    require(report['complete_value_equality'] and report['native_schema_equal']
            and report['source_generations_validated'], 'readback correctness')
    require(report['peak_native_split_tasks'] <= report['native_task_concurrency'] == 2, 'readback task bound')
    m = report['memory']
    require(m['native_final_reserved_bytes'] == m['arrow_final_reserved_bytes'] == 0, 'readback release')
    require(m['native_peak_reserved_bytes'] <= m['native_limit_bytes']
            and m['arrow_peak_reserved_bytes'] <= m['arrow_work_limit_bytes']
            and m['peak_arrow_batch_bytes'] <= m['arrow_batch_bytes'], 'readback memory bound')


def layout(report):
    require(report['complete_flat_inspection'] and report['all_referenced_segments_are_flat'], 'complete layout')
    require(report['non_flat_segment_reference_count'] == 0, 'flat layout scope')
    require(report['external_engine_invoked'] is False and report['fallback_attempted'] is False, 'layout boundary')
    require(report['provider_version'] == '=0.90.0', 'provider identity')
    for key in ('array_nodes', 'layout_nodes'):
        require(report[key] <= report['limits'][key], 'layout node limit')
    require(report['flat_reference_count'] <= report['limits']['flat_references'], 'flat-reference limit')
    require(report['segment_bytes_requested'] <= report['limits']['total_segment_bytes'], 'layout byte limit')
    require(report['artifact']['bytes'] == 15715228660
            and report['artifact']['sha256'] == 'sha256:' + OUTPUT_SHA, 'physical artifact identity')


def ingest(packet, role):
    d = packet.read(ROOT / f'{role}-screen-r1.driver.json')
    b = packet.linked(d['build'])
    require(d['status'] == 'completed' and d['role'] == role and d['returncode'] == 0, 'ingest completion')
    require(d['process_group_drained'] and d['direct_child_reaped'] and d['native_process_cleanup_verified'], 'ingest cleanup')
    require(not d['timed_out'] and d['interrupted_signal'] is None
            and not d['recursive_inventory_during_execution'], 'ingest supervision')
    require(d['input_before'] == d['input_after'] and d['input_before']['sha256'] == SOURCE_SHA, 'source continuity')
    require(d['output']['sha256'] == OUTPUT_SHA and d['output']['bytes'] == 15715228660, 'output identity')
    require(d['source_revision'] == b['source_revision'] and d['source_tree'] == b['source_tree'], 'ingest build linkage')
    require(b['status'] == 'passed' and b['package_feature_union'] == 'release-user-surfaces', 'native build')
    directory, = d['ingest_log_directories']
    summary = packet.read(Path(directory) / 'prepare_summary.json')
    f = fields(packet.read(Path(directory) / 'stdout.json'))
    require(summary['returncode'] == 0 and summary['stdout_json_ok'] and summary['target_exists'], 'ingest summary')
    require(summary['memory_gb'] == 24 and summary['max_parallelism'] == 12, 'ingest resources')
    require(f[PREFIX + 'vortex_ingest_status'] == 'prepared_state_created'
            and f[PREFIX + 'prepared_olap_state_status'] == 'prepared_olap_state_ready', 'prepared publication')
    require(f[PREFIX + 'prepared_artifact_ref'] == d['output']['path']
            and f[PREFIX + 'prepared_artifact_digest'] == 'sha256:' + OUTPUT_SHA, 'publication identity')
    require(int(f[PREFIX + 'source_state_record_batch_count']) == 3120, 'complete batches')
    for key in ('input_row_count', 'vortex_segment_metadata_row_count', 'prepared_olap_state_layout_footer_row_count',
                'vortex_ingest_stream_validation_rows', 'vortex_ingest_stream_projection_rows', 'vortex_ingest_stream_arrow_conversion_rows'):
        require(int(f[PREFIX + key]) == 99997497, 'complete ingest rows')
    require(int(f[PREFIX + 'vortex_shared_native_memory_denied_reservations']) == 0, 'ingest admission')
    require(int(f[PREFIX + 'vortex_shared_native_memory_peak_reserved_bytes']) <= 24 * 2**30, 'ingest memory')
    require(int(f[PREFIX + 'vortex_shared_native_memory_final_reserved_bytes']) == 4096, 'retained metadata snapshot')
    rb_dir = ROOT / f'logs/readonly-proof-{role}-screen-r1-readback'
    lo_dir = ROOT / ('logs/readonly-proof-control-screen-r1-layout-r2' if role == 'control'
                     else 'logs/readonly-proof-candidate-screen-r1-layout')
    rb, lo = packet.read(rb_dir / 'proof.stdout.json'), packet.read(lo_dir / 'proof.stdout.json')
    readback(rb)
    layout(lo)
    for directory, tool in ((rb_dir, 'compare_parquet_native_artifact'), (lo_dir, 'physical_encoding_inventory')):
        receipt = packet.read(directory / 'receipt.json')
        require(receipt['status'] == 'passed' and receipt['unchanged'], 'proof status')
        require(receipt['inputs_before'] == receipt['inputs_after'], 'proof generations')
        require(receipt['result']['returncode'] == 0 and receipt['result']['guard_failures'] == [], 'proof guards')
        require(receipt['binary_sha256'] == b['artifacts'][tool]['sha256'], 'proof binary identity')
    return {'driver': d, 'build': b, 'summary': summary, 'fields': f, 'readback': rb, 'layout': lo}


def screen(control, candidate, observed):
    old, new = control['fields'], candidate['fields']
    ownership = observed['ownership']
    for name, value in ownership.items():
        require(int(new[PREFIX + 'vortex_ingest_ownership_' + name]) == value, 'ownership counter linkage')
    copied = int(old[PREFIX + 'vortex_ingest_ownership_copied_buffer_bytes'])
    require(copied == 115652962945 and ownership['copied_buffer_bytes'] == 44831432362, 'copy observations')
    require(ownership['copied_buffer_bytes'] + ownership['transferred_buffer_bytes'] == copied, 'ownership conservation')
    require(ownership['transferred_allocation_bytes'] >= ownership['transferred_buffer_bytes'], 'retained allocation charge')
    require(ownership['repeated_buffer_bytes'] == ownership['repeated_buffer_regions'] == 0, 'duplicate observation')
    reduction = 1 - ownership['copied_buffer_bytes'] / copied
    improvement = 1 - candidate['summary']['native_process_seconds'] / control['summary']['native_process_seconds']
    size_ratio = candidate['driver']['output']['bytes'] / control['driver']['output']['bytes']
    rss_ratio = candidate['summary']['native_peak_rss_bytes'] / control['summary']['native_peak_rss_bytes']
    gates = {'copy_reduction_at_least_20_percent': reduction >= .20,
             'complete_ingest_improvement_at_least_3_percent': improvement >= .03,
             'complete_ingest_no_more_than_5_percent_regression': improvement >= -.05,
             'output_growth_no_more_than_0_5_percent': size_ratio <= 1.005,
             'peak_rss_growth_no_more_than_5_percent': rss_ratio <= 1.05}
    require(observed['gates'] == gates and observed['status'] == 'rejected_by_frozen_screen', 'unchanged screen decision')
    require(gates['complete_ingest_improvement_at_least_3_percent'] is False, 'failed speed screen preserved')
    require(all(v for k, v in gates.items() if k != 'complete_ingest_improvement_at_least_3_percent'), 'other screen gates')
    require(observed['reductions'] == {'copied_bytes': reduction, 'complete_ingest_process': improvement}, 'screen arithmetic')
    require(control['layout']['columns'] == candidate['layout']['columns'], 'complete physical column continuity')
    return reduction


def full43(packet, acceptance_path):
    a = packet.read(acceptance_path)
    require(a['status'] == 'passed' and a['performance_claim'] is False, 'consumer acceptance scope')
    outer, build, summary = packet.linked(a['outer']), packet.linked(a['build']), packet.linked(a['summary'])
    previous = packet.linked(a['previous_summary'])
    reference = packet.linked(a['reference'])
    require(a['reference']['sha256'] == REFERENCE_SHA and reference['reference_kind'] == 'independent_reference', 'independent reference')
    query_raw = packet.raw(REPO / 'benchmarks/clickbench/queries.sql')
    statements = [x.strip() for x in '\n'.join(x for x in query_raw.decode().splitlines()
                                              if not x.lstrip().startswith('--')).split(';') if x.strip()]
    require(len(statements) == 43 and digest(query_raw) == reference['queries_sha256'], 'query identity')
    expected = {r['query_id']: r for r in reference['records']}
    require(len(reference['records']) == 43 and set(expected) == set(range(1, 44)), 'reference coverage')
    for query, ref in expected.items():
        require(ref['statement'] == statements[query - 1], 'reference statement')
        require(digest(json.dumps(ref['values'], sort_keys=True, allow_nan=False).encode()) == ref['values_sha256'], 'reference values identity')
    require(outer['status'] == 'passed' and outer['returncode'] == 0
            and outer['native_process_cleanup_verified'] and outer['process_group_drained'], 'query supervision')
    require(outer['input_before'] == outer['input_after'] and outer['input_after']['sha256'] == OUTPUT_SHA, 'query input identity')
    require(not outer['recursive_inventory_during_execution'], 'query supervision scope')
    require(summary['complete'] and summary['full_result_validation'] and summary['repeats'] == 1, 'full single pass')
    require(summary['selected_query_ids'] == list(range(1, 44)) and len(summary['records']) == 43, 'query coverage')
    require({record['query'] for record in summary['records']} == set(range(1, 44)), 'unique query coverage')
    require(summary['completed_identity_check'] and summary['reference_packet_identity_verified']
            and summary['harness_identity_verified'], 'query completion identities')
    require(summary['binary_sha256'] == build['artifacts']['shardloom']['sha256'], 'query executable identity')
    require(summary['build_commit'] == build['source_revision'], 'query source revision')
    old_records = {r['query']: r for r in previous['records']}
    receipts = {r['query']: r for r in a['records']}
    require(len(a['records']) == 43 and set(receipts) == set(expected), 'acceptance row coverage')
    times, prior_times, count = [], [], 0
    for record in summary['records']:
        query = record['query']
        require(record['passed'] and record['returncode'] == 0 and record['run'] == 1
                and not record['guard_failures'] and record['validation'] == 'complete_values', 'query record')
        receipt = receipts[query]
        envelope = packet.linked(receipt['output'])
        f, values = rows(envelope)
        require(equivalent(values, expected[query]['values']), 'complete independent values')
        require(digest(json.dumps(values, sort_keys=True, allow_nan=False).encode()) == record['result_sha256'], 'query result digest')
        previous_f, previous_values = rows(packet.linked(receipt['previous_output']))
        require(equivalent(previous_values, expected[query]['values']), 'previous complete values')
        require(strict(f['result_schema_json']) == strict(previous_f['result_schema_json'])
                and f['result_schema_format'] == previous_f['result_schema_format'], 'schema continuity')
        seconds = int(f['native_query_execution_nanos']) / 1e9
        old_seconds = int(previous_f['native_query_execution_nanos']) / 1e9
        require(record['engine_timing_reported'] and seconds == record['engine_execution_seconds']
                == receipt['engine_seconds'], 'engine clock')
        require(old_seconds == old_records[query]['engine_execution_seconds'] == receipt['previous_engine_seconds'], 'previous engine clock')
        require(record['query_statement'] == statements[query - 1], 'query statement linkage')
        count += len(values)
        times.append(seconds)
        prior_times.append(old_seconds)
    total, prior_total = sum(times), sum(prior_times)
    require(count == a['complete_rows'] == 381 and a['complete_results'] == a['exact_schema_continuity'] == 43, 'complete result totals')
    require(total == a['engine_seconds'] == summary['query_total_seconds'], 'engine total')
    require(prior_total == a['previous_engine_seconds'] == previous['query_total_seconds'], 'previous engine total')
    require(not a['more_than_10_percent_regression'] and total <= prior_total * 1.1, 'consumer trigger')
    return {'queries': 43, 'complete_rows': count, 'engine_seconds': total,
            'previous_engine_seconds': prior_total, 'causal_speedup_claim': False}


def replay(packet):
    require(packet.manifest['scope'] in {'ownership_observations_only;combined_dependency_acceptance_pending',
                                        'ownership_and_combined_dependency_acceptance'}, 'packet acceptance scope')
    require(digest(packet.raw(ROOT / 'protocol-r1.md')) == PROTOCOL_SHA, 'frozen protocol')
    control, candidate = ingest(packet, 'control'), ingest(packet, 'candidate')
    accepted = packet.read(ROOT / 'control-screen-r1-acceptance.json')
    for key in accepted['unchanged_writer_recipe_fields'] + ['source_state_materialized_columns']:
        require(control['fields'][PREFIX + key] == candidate['fields'][PREFIX + key], 'writer recipe: ' + key)
    reduction = screen(control, candidate, packet.read(ROOT / 'candidate-screen-r1-preliminary.json'))
    decision = packet.read(ROOT / 'candidate-screen-r1-decision.json')
    retained = packet.read(ROOT / 'candidate-retention-decision-r1.json')
    packet.linked(retained['original_screen'])
    require(decision['status'] == 'rejected' and decision['correctness_status'] == 'passed'
            and decision['performance_candidate_accepted'] is False
            and decision['paired_confirmation_executed'] is False, 'original failed screen')
    require(retained['decision'] == 'retain_controlled_reader_owner_transfer'
            and retained['original_speed_gate_passed'] is False
            and retained['repeatable_ingest_speedup_claim'] is False
            and retained['causal_query_speedup_claim'] is False, 'explicit retention scope')
    query = full43(packet, ROOT / 'candidate-full43-r1-acceptance.json')
    checks = {'ownership_validation': historical_validation(packet)}
    if packet.manifest['scope'] == 'ownership_and_combined_dependency_acceptance':
        checks['combined_validation'] = combined_validation(packet)
        checks['dependency_validation'] = dependency_validation(packet)
        checks['combined_runtime'] = combined_runtime_validation(packet)
        checks['combined_full43'] = full43(packet, ROOT / 'combined-full43-r1-acceptance.json')
    return {'status': 'passed', 'complete_ingests': 2, 'readback_rows_per_artifact': 99997497,
            'readback_columns_per_artifact': 112, 'readback_values_per_artifact': 11199719664,
            'copy_reduction_fraction': reduction, 'frozen_speed_screen': 'failed',
            'implementation_retention': 'explicit_maintainer_decision', 'ownership_full43': query,
            'archived_files': len(packet.files), 'engine_execution_performed': False, **checks}


def corruption_controls(packet):
    rejected = []
    control, candidate = ingest(packet, 'control'), ingest(packet, 'candidate')
    observed = packet.read(ROOT / 'candidate-screen-r1-preliminary.json')
    cases = []
    bad = copy.deepcopy(observed)
    bad['gates']['complete_ingest_improvement_at_least_3_percent'] = True
    cases.append(('changed_speed_gate', lambda: screen(control, candidate, bad)))
    for name, key, value in [('truncated_readback', 'compared_rows', 99997496),
                             ('wrong_column_coverage', 'compared_columns', 111),
                             ('incomplete_value_comparison', 'complete_value_equality', False)]:
        row = copy.deepcopy(candidate['readback'])
        row[key] = value
        cases.append((name, lambda row=row: readback(row)))
    row = copy.deepcopy(candidate['readback'])
    row['memory']['native_final_reserved_bytes'] = 1
    cases.append(('retained_readback_credit', lambda row=row: readback(row)))
    row = copy.deepcopy(candidate['layout'])
    row['non_flat_segment_reference_count'] = 1
    cases.append(('uninspected_layout', lambda row=row: layout(row)))
    envelope = {'status': 'success', 'fallback': {'attempted': True, 'allowed': False, 'engine': None}, 'fields': []}
    cases.append(('fallback_execution', lambda: fields(envelope)))
    cases.append(('duplicate_json_key', lambda: strict('{"rows":1,"rows":2}')))
    cases.append(('nonfinite_result_value', lambda: strict('{"value":NaN}')))
    cases.append(('failed_test_log', lambda: test_counts(b'test result: FAILED. 1 passed; 1 failed; 0 ignored;')))
    # Override parsed evidence only after the ordinary linked-byte check. These
    # controls exercise semantic rejection beyond archive digest validation.
    class Changed:
        def __init__(self, path, value):
            self.path, self.value = str(path), value

        def read(self, path):
            return copy.deepcopy(self.value) if str(path) == self.path else packet.read(path)

        def raw(self, path):
            return packet.raw(path)

        def linked(self, reference):
            packet.linked(reference)
            return self.read(reference['path'])

        def verified_raw(self, reference, base=None):
            return packet.verified_raw(reference, base)

    acceptance_path = ROOT / 'candidate-full43-r1-acceptance.json'
    acceptance = packet.read(acceptance_path)
    for name, key, value in [('false_engine_total', 'engine_seconds', acceptance['engine_seconds'] + 1),
                             ('truncated_query_coverage', 'complete_results', 42),
                             ('false_speedup_claim', 'performance_claim', True)]:
        changed = copy.deepcopy(acceptance)
        changed[key] = value
        altered = Changed(acceptance_path, changed)
        cases.append((name, lambda altered=altered: full43(altered, acceptance_path)))
    record = acceptance['records'][0]
    changed = packet.linked(record['output'])
    field = next(field for field in changed['fields'] if field['key'] == 'native_query_execution_nanos')
    field['value'] = str(int(field['value']) + 1)
    altered = Changed(record['output']['path'], changed)
    cases.append(('changed_raw_engine_clock', lambda altered=altered: full43(altered, acceptance_path)))
    changed = packet.linked(acceptance['summary'])
    changed['records'][1] = copy.deepcopy(changed['records'][0])
    altered = Changed(acceptance['summary']['path'], changed)
    cases.append(('duplicate_query_record', lambda altered=altered: full43(altered, acceptance_path)))
    if packet.manifest['scope'] == 'ownership_and_combined_dependency_acceptance':
        path = VALIDATION / 'ingest-combined-selected-r15.json'
        changed = packet.read(path)
        changed['commands'][0]['returncode'] = 1
        altered = Changed(path, changed)
        cases.append(('failed_combined_gate', lambda altered=altered: combined_validation(altered)))
        path = VALIDATION / 'dependency-lock-delta-r1.json'
        changed = packet.read(path)
        changed['added'][0]['version'] = '60.0.0'
        altered = Changed(path, changed)
        cases.append(('unreviewed_dependency', lambda altered=altered: dependency_validation(altered)))
        path = VALIDATION / 'ingest-combined-source-after-r15.json'
        changed = packet.read(path)
        changed['files'][0]['working_sha256'] = '0' * 64
        altered = Changed(path, changed)
        cases.append(('different_tested_runtime_source', lambda altered=altered: combined_runtime_validation(altered)))
        path = ROOT / 'wheel-proof-combined-r1/report.json'
        changed = packet.read(path)
        changed['steps'][4]['returncode'] = 1
        altered = Changed(path, changed)
        cases.append(('failed_installed_load', lambda altered=altered: combined_runtime_validation(altered)))
        changed = packet.read(path)
        changed['integrity']['cli_sha256'] = '0' * 64
        altered = Changed(path, changed)
        cases.append(('wrong_embedded_cli', lambda altered=altered: combined_runtime_validation(altered)))
        path = ROOT / 'combined-full43-r1-acceptance.json'
        changed = packet.read(path)
        changed['engine_seconds'] += 1
        altered = Changed(path, changed)
        cases.append(('false_combined_engine_total', lambda altered=altered, path=path: full43(altered, path)))
    for name, operation in cases:
        try:
            operation()
        except (ValueError, KeyError, TypeError):
            rejected.append(name)
        else:
            raise ValueError('corruption control accepted: ' + name)
    return rejected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packet', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    packet = Packet(args.packet)
    result = replay(packet)
    result['corruption_controls_rejected'] = corruption_controls(packet)
    text = json.dumps(result, indent=2, allow_nan=False) + '\n'
    if args.output:
        args.output.write_text(text)
    print(text, end='')


if __name__ == '__main__':
    main()
