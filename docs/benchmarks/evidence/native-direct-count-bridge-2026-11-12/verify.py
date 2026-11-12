"""Verify the invalid-baseline COUNT checkpoint without executing queries."""
from __future__ import annotations

import argparse
import ast
from collections import Counter, defaultdict
from contextlib import ExitStack
import gzip
import hashlib
import json
import math
from pathlib import Path, PurePosixPath
import re
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import types

BASE = '3a0608b12e40435fab1adc4afefbaa4173932737'
BASE_TREE = 'ef1ebe017aaedcc858098106480f20c5ff82a102'
SOURCE_FINGERPRINT = 'c317190e05e07cf2b824d1fccf70f432a65b809b2ef15b8a1ad13697b52171bf'
ORIGINAL_ROOT = '/Users/dylan/LocalData/shardloom/direct-count-bridge-2026-11-12'
JOIN_ROOT = '/Users/dylan/LocalData/shardloom/join-mapping-2026-11-11'
WINDOW_ROOT = '/Users/dylan/LocalData/shardloom/shared-window-state-2026-11-11'
PREPARATION_ROOT = '/Users/dylan/LocalData/shardloom/shared-operator-state-2026-11-10'
ROOT = '/Users/dylan/.codex/worktrees/native-vector-representations/shardloom-local-repo'
PYTHON = '/opt/homebrew/opt/python@3.13/libexec/bin/python3'
FROZEN_SHA256 = '38c7a349ec04645e094248d20cfd3d8325e33a63ad76dd9bdc1543412bf7681d'
CLOSEOUT_SHA256 = 'e34f1128c33c78f634fdfd03e30cc1c5a53f328b060f011f1a53300bc2c64936'
FEATURES = 'shardloom-python/python-native,shardloom-vortex/native-encoded-exchange-experiment'
CI_POLICY_PATH = '.github/ci-native-baseline.json'
CI_DIAGNOSTIC = 'local_primitives::prepared_relational::tests::direct_count_tests::native_direct_count_attribution'
REPLAY_SUPPORT = ('scripts/native_workflow_protocol.py', 'scripts/clickbench_reference_packet.py', 'scripts/clickbench_native_sql.py')
CHECKS = {
    'count-partial-format-r1': ['cargo', 'fmt', '--all', '--', '--check'],
    'count-partial-default-clippy-r1': ['cargo', 'clippy', '--offline', '--locked', '--workspace', '--all-targets', '--', '-D', 'warnings'],
    'count-partial-default-tests-r1': ['cargo', 'test', '--offline', '--locked', '--workspace', '--all-targets'],
    'count-partial-workspace-native-clippy-r1': ['cargo', 'clippy', '--offline', '--locked', '--workspace', '--all-targets', '--features', FEATURES, '--', '-D', 'warnings'],
    'count-partial-native-tests-r2': ['env', 'RUST_TEST_THREADS=4', 'cargo', 'test', '--offline', '--locked', '--workspace', '--all-targets', '--features', FEATURES],
    'candidate-release-build-r1': ['env', 'PYO3_PYTHON=' + PYTHON, 'cargo', 'build', '--offline', '--locked', '--release', '--features', 'release-user-surfaces', '-p', 'shardloom-cli', '-p', 'shardloom-python'],
}
DOC_CHECKS = {
    'docs-checks-r1': [PYTHON, '-B', ORIGINAL_ROOT + '/tools/check_docs.py'],
    'doc-contracts-r1': ['env', 'RUST_TEST_THREADS=4', 'cargo', 'test', '--offline', '--locked', '-p', 'shardloom-contract-tests', '--all-targets'],
}
STATUS_PROSE = {
    'docs/architecture/phased-execution-plan.md',
    'docs/architecture/native-shared-pipeline-investigations-2026-11-10.md',
    'docs/architecture/native-structural-work-investigations-2026-11-11.md',
    'docs/architecture/native-join-payload-mapping-2026-11-12.md',
    'docs/architecture/native-direct-count-bridge-2026-11-12.md',
    'docs/architecture/local-development-storage.md',
}
ORIGINAL_FAILURES = {
    'count-partial-focused-r1.json', 'count-partial-focused-r2.json',
    'count-partial-native-clippy-r1.json', 'count-partial-native-clippy-r2.json',
    'count-partial-native-tests-r1.json', 'debug-attribution-r1-result.json',
    'debug-attribution-r1.json', 'debug-attribution-r2-result.json',
    'debug-attribution-r2.json', 'debug-attribution-r3-result.json',
    'debug-attribution-r3.json', 'debug-attribution-r4-result.json',
    'debug-attribution-r4.json', 'debug-diagnostic-build-r1.json',
    'declare-diagnostics-r1.json', 'guard-preparation-r1-setup-failure.json',
    'baseline-verified-r1.json', 'baseline-replay-r1.json',
}


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def identity(raw):
    return dict(bytes=len(raw), sha256=digest(raw))


def fingerprint(value):
    return digest(json.dumps(value, sort_keys=True, separators=(',', ':')).encode())


def rust_counts(raw):
    rows = re.findall(rb'test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;', raw)
    return dict(zip(('passed', 'failed', 'ignored'), (sum(int(row[i]) for row in rows) for i in range(3))))


def safe_name(name):
    path = PurePosixPath(name)
    assert name and not path.is_absolute() and str(path) == name
    assert all(part not in ('', '.', '..') for part in path.parts)


def read_archive(root):
    report = json.loads((root / 'acceptance.json').read_bytes())
    assert report['schema'] == 'shardloom.direct_count_checkpoint.v1'
    rows, stored = report['members'], report['stored_members']
    for items in (rows, stored):
        assert items == sorted(items, key=lambda row: row['path'])
        assert len({row['path'] for row in items}) == len(items)
        assert sum(row['bytes'] for row in items) < 512 << 20
        for row in items:
            safe_name(row['path'])
            assert type(row['bytes']) is int and row['bytes'] >= 0
    assert sum(row['bytes'] for row in rows) == sum(row['bytes'] for row in stored)
    by_name, data = {row['path']: row for row in stored}, {}
    for ordinal, record in enumerate(report['archives']):
        assert record['path'] == f'verification-{ordinal + 1:03d}.tar.xz'
        path = root / record['path']
        assert identity(path.read_bytes()) == {key:record[key] for key in ('bytes', 'sha256')}
        assert record['bytes'] < 64 << 20
        with tarfile.open(path, 'r:xz') as archive:
            members = archive.getmembers()
            assert [member.name for member in members] == record['members']
            assert sum(member.size for member in members) <= 48 << 20
            for member in members:
                safe_name(member.name)
                assert member.isfile() and member.name not in data
                wanted = by_name[member.name]
                assert member.size == wanted['bytes'] and member.size <= 24 << 20
                raw = archive.extractfile(member).read()
                assert identity(raw) == {key:wanted[key] for key in ('bytes', 'sha256')}
                data[member.name] = raw
    assert set(data) == set(by_name)
    for row in report['assemblies']:
        assert row['path'] not in data and len(row['parts']) > 1
        raw = b''.join(data.pop(name) for name in row['parts'])
        assert identity(raw) == {key:row[key] for key in ('bytes', 'sha256')}
        data[row['path']] = raw
    assert set(data) == {row['path'] for row in rows}
    assert all(identity(data[row['path']]) == {key:row[key] for key in ('bytes', 'sha256')} for row in rows)
    assert data['current/tools/verify_direct_count_packet.py'] == Path(__file__).read_bytes()
    return report, data


class Packet:
    def __init__(self, report, data):
        self.report, self.data = report, data

    def read(self, name):
        return json.loads(self.data[name])

    def bound(self, row):
        raw = self.data[self.report['locations'][row['path']]]
        size = row.get('bytes', row.get('generation', {}).get('bytes'))
        assert identity(raw) == dict(bytes=size, sha256=row['sha256']), row['path']
        return raw

    def proof(self, row):
        external = self.report['identity_only_proof']
        if row['path'] not in external:
            return self.bound(row)
        declared = self.read('current/baseline-protocol-r1.json')['runtime_binding']['proof_files']
        matching = [item for item in declared if item['path'] == row['path']]
        assert len(matching) == 1 and matching[0] == row
        item, path = external[row['path']], Path(row['path'])
        size = row.get('bytes', row.get('generation', {}).get('bytes'))
        assert item['identity'] == dict(bytes=size, sha256=row['sha256'])
        if item['role'] in ('accepted_runtime_identity', 'candidate_runtime_identity'):
            root = WINDOW_ROOT if item['role'] == 'accepted_runtime_identity' else ORIGINAL_ROOT
            assert path.parent == Path(root) / 'runtime-r1'
            assert path.name in ('shardloom', 'lib_native.dylib', '_native.abi3.so')
        elif item['role'] == 'candidate_installed_wheel_identity':
            assert path.parent == Path(ORIGINAL_ROOT) / 'installed-proof-r1/dist'
            installed = self.read('current/installed-proof-r1/report.json')['integrity']
            assert path.name == installed['wheel']
            assert item['identity'] == dict(bytes=installed['wheel_bytes'], sha256=installed['wheel_sha256'])
        else:
            assert item['role'] == 'historical_committed_archive_identity'
            root = Path(ROOT) / 'docs'
            assert path.is_relative_to(root / 'benchmarks/evidence') or path.is_relative_to(root / 'release/evidence')
            assert path.name.startswith('verification') and path.name.endswith('.tar.xz')

    def guard(self, prefix, command=None, source=None, *, expected_failure=False):
        record = self.read(prefix + '.json')
        if expected_failure:
            assert prefix == 'current/baseline-replay-r1'
            assert record['status'] == 'failed' and record['returncode'] == 1
            assert b'failed observations preserved; no complete performance score admitted' in self.data[prefix + '.log']
        else:
            assert record['status'] == 'passed' and record['returncode'] == 0
        assert record['process_group_drained'] and record['direct_child_reaped']
        assert record['fixed_source_validation'] and not record['allow_lockfile_update']
        assert not record['timed_out'] and not record['interrupted_signal']
        assert record['log_sha256'] == digest(self.data[prefix + '.log'])
        if command is not None:
            assert record['command'] == command
        sources = record['build_sources']
        if source is not None:
            assert sources == source
        assert fingerprint(sources) == record['build_source_fingerprint_sha256']
        for phase in ('pre_execution', 'post_execution'):
            check = record['build_source_verification'][phase]
            assert check['head_matches'] and check['path_set_matches'] and not check['changed_paths']
            assert check['fingerprint_sha256'] == fingerprint(sources)
        for item in (*sources.values(), *record['tool_source_inventory'].values()):
            if 'sha256' in item and 'symlink' not in item:
                assert identity(self.data['source-blobs/' + item['sha256']]) == item
        return record


def functions(raw, names, namespace):
    nodes = [node for node in ast.parse(raw).body if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names]
    assert {node.name for node in nodes} == names and len(nodes) == len(names)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), 'archived-proof-definitions', 'exec'), namespace)
    return namespace


def declaration(raw, name):
    nodes = [node for node in ast.parse(raw).body if isinstance(node, ast.Assign) and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name) and node.targets[0].id == name]
    assert len(nodes) == 1
    return ast.literal_eval(nodes[0].value)


def test_names(raw):
    names = re.findall(rb'^test (\S+) \.\.\. ok$', raw, re.M)
    assert names and not re.search(rb'^test .* \.\.\. FAILED$|^test result: FAILED\.', raw, re.M)
    return [name.decode() for name in names]


def verify_ci(packet, source):
    policy = json.loads(packet.data['source-blobs/' + source[CI_POLICY_PATH]['sha256']])
    baseline = packet.read('join/join-mapping-closeout-r1.json')['repository_source_snapshot']
    old = json.loads(packet.data['source-blobs/' + baseline[CI_POLICY_PATH]['sha256']])
    assert len(old['ignored_tests']) == 35 and CI_DIAGNOSTIC not in old['ignored_tests']
    assert policy == {**old, 'ignored_tests': sorted([*old['ignored_tests'], CI_DIAGNOSTIC])}
    record = packet.read('current/candidate-ci-inventory-r1.json')
    assert record['schema'] == 'shardloom.direct_count_ci_inventory.v1'
    assert record['status'] == 'passed_actual_native_inventory' and record['exact_ignored_ownership']
    assert record['query_executions'] == 0 and record['source_fingerprint_sha256'] == fingerprint(source)
    assert record['ignored'] == policy['ignored_tests'] and len(record['ignored']) == 36
    assert record['discovery_features'] == ['release-user-surfaces']
    assert record['discovery_profile'] == 'debug' and record['hosted_ci_profile'] == 'ci-native'
    assert record['both_profiles_have_debug_assertions']
    partitions = record['partitions']; names = [name for part in partitions for name in part['tests']]
    assert len(partitions) == 4 and len(names) == len(set(names)) == record['test_count'] == 4144
    assert names.count(CI_DIAGNOSTIC) == 1 and record['attribution_test'] == CI_DIAGNOSTIC
    for name, expected in record['files'].items():
        assert identity(packet.data[packet.report['locations'][name]]) == expected
    packet.guard('current/candidate-ci-verification-r1', [PYTHON, '-B', ORIGINAL_ROOT + '/tools/verify_direct_count_ci_inventory.py',
        '--build', ORIGINAL_ROOT + '/candidate-debug-build-r1.json', '--output', ORIGINAL_ROOT + '/candidate-ci-inventory-r1.json'], source)
    packet.guard('current/candidate-debug-build-r1', ['cargo', 'test', '--offline', '--locked', '-p', 'shardloom-vortex',
        '--lib', '--features', 'release-user-surfaces', '--no-run', '--message-format=json'], source)
    required = declaration(packet.data['current/tools/direct_count_proof.py'], 'REQUIRED_TESTS')
    assert len(required) == len(set(required)) == 9
    assert all(sum(value.endswith('::' + name) for value in names) == 1 for name in required)
    observed = test_names(packet.data['current/count-partial-native-tests-r2.log'])
    assert all(sum(value.endswith('::' + name) for value in observed) == 1 for name in required)
    return dict(tests=len(names), ignored=len(record['ignored']), partitions=len(partitions))


def verify_diagnostics(packet):
    feasibility = packet.read('current/feasibility-attribution-r1.json')
    assert feasibility['status'] == 'passed' and feasibility['queries_executed_by_verifier'] == 0
    assert not feasibility['production_performance_result']
    profiles = {}
    for profile in ('debug', 'release'):
        report = packet.read('current/' + profile + '-attribution-r5-result.json')
        assert report['status'] == 'passed' and report['profile'] == profile
        assert report['before_inputs_verified'] and report['after_inputs_verified']
        assert not report['performance_result'] and not report['complete_10k_claim'] and not report['full43_claim']
        protocol = json.loads(packet.bound(report['protocol']))
        assert protocol['profile'] == profile and not protocol['scored']
        assert protocol['iterations_per_request'] == 1 and protocol['warmups'] == protocol['repeats'] == 0
        declarations = json.loads(packet.bound(protocol['declarations']))
        frozen = json.loads(packet.bound(protocol['build_identity']))
        assert frozen['build_source_fingerprint_sha256'] == protocol['source_fingerprint_sha256'] == feasibility['source_fingerprint_sha256']
        assert frozen['binary'] == protocol['binary']
        receipt = json.loads(packet.bound(frozen['build_receipt']))
        assert receipt['status'] == 'passed' and receipt['returncode'] == 0 and receipt['fixed_source_validation']
        assert receipt['build_source_fingerprint_sha256'] == frozen['build_source_fingerprint_sha256']
        guard = packet.guard('current/' + profile + '-attribution-r5')
        assert guard['query_workload'] and guard['build_source_fingerprint_sha256'] == protocol['source_fingerprint_sha256']
        assert guard['build_inventory_policy'] == guard['evidence_inventory_policy'] == 'before_and_after_only'
        assert len(report['observations']) == declarations['total_requests'] == 19
        rows = []
        for entry, declared in zip(report['observations'], declarations['jobs'], strict=True):
            assert entry['case_id'] == declared['case_id'] and entry['job'] == declared
            assert entry['status'] == 'passed' and entry['returncode'] == 0 and entry['child_completed']
            observed, job = (json.loads(packet.bound(item)) for item in (entry['observation'], declared))
            assert observed['job'] == job and observed['actual_rows'] == job['expected_rows']
            assert observed['native_io_certified'] and not observed['fallback_attempted'] and not observed['external_engine_invoked']
            assert observed['final_reserved_bytes'] == observed['denied_reservations'] == 0
            assert observed['peak_reserved_bytes'] <= observed['memory_grant_bytes'] == 2 << 30
            assert 0 <= observed['after_prepared_reserved_bytes'] <= observed['after_result_reserved_bytes']
            if job['source']['format'] != 'vortex':
                assert observed['after_result_reserved_bytes'] == observed['initial_reserved_bytes']
                assert observed['retained_source_count'] == observed['after_prepared_reserved_bytes'] == 0
            else:
                assert observed['retained_source_count'] == 1
            log = packet.bound(dict(path=entry['log'], **entry['log_identity']))
            messages = [json.loads(line.split(b'DIRECT_COUNT ', 1)[1]) for line in log.splitlines() if b'DIRECT_COUNT ' in line]
            assert messages == [observed]
            phases = {value['phase']: value['exclusive_nanos'] for value in observed['phases']}
            assert len(phases) == len(observed['phases']) == 10
            assert all(type(value) is int and value >= 0 for value in phases.values())
            assert sum(value['exclusive_nanos'] for value in observed['phases'] if value['engine_classified']) == observed['classified_engine_ns']
            assert observed['classified_engine_ns'] + observed['unclassified_engine_ns'] == observed['engine_execution_ns']
            row = dict(case_id=entry['case_id'], origin=job['origin'], source=job['source'],
                shared_aggregate=observed['shared_aggregate'], provider_selection_reason=observed['provider_selection_reason'],
                observation=entry['observation'], complete_output_rows=len(observed['actual_rows']),
                instrumented_engine_execution_ns=observed['engine_execution_ns'], phases_exclusive_ns=phases,
                after_result_minus_initial_bytes=observed['after_result_reserved_bytes']-observed['initial_reserved_bytes'],
                after_prepared_reserved_bytes=observed['after_prepared_reserved_bytes'], final_reserved_bytes=0,
                interning_and_update_share=(phases['group_interning'] + phases['typed_updates'] + phases['other_updates']) / observed['engine_execution_ns'])
            if profile == 'debug':
                counts = observed['counts']
                assert observed['debug_work_observation']
                if not observed['shared_aggregate']:
                    assert job['source']['format'] == 'vortex' and observed['provider_selection_reason'] == 'equivalent_bound_native_file_provider_selected'
                    assert counts['group_ordinal_rows'] == counts['aggregate_count_contributions'] == 0
                elif job.get('expected_selected_rows') is not None:
                    assert counts['group_ordinal_rows'] == job['expected_selected_rows']
                    assert counts['aggregate_count_contributions'] == job['expected_count_contributions']
                else:
                    assert job['expected_count_contributions'] == 'all_selected_rows_nonnullable_argument'
                    assert counts['aggregate_count_contributions'] == counts['group_ordinal_rows']
                assert counts['aggregate_count_updates'] == counts['aggregate_count_contributions']
                keys = counts['membership_key_preparations']
                assert len(keys) == counts['group_input_batches']
                assert sum(item['rows'] for item in keys) == counts['group_ordinal_rows']
                assert all(item['numeric'] for item in keys)
                row['work'] = {key:counts[key] for key in ('group_ordinal_rows', 'group_input_batches', 'key_cache_batches',
                    'key_cache_hits', 'key_inline_cache_batches', 'key_inline_cache_hits', 'key_exact_lookups', 'row_index_lookups',
                    'row_index_equality_calls', 'aggregate_count_updates', 'aggregate_count_contributions')}
                row['key_input_encodings_by_batches'] = dict(Counter(item['input_encoding'] for item in keys))
                row['key_dtypes_by_batches'] = dict(Counter(item['dtype'] for item in keys))
            else:
                assert observed['counts'] is None and not observed['debug_work_observation']
            rows.append(row)
        expected = feasibility['profiles'][profile]
        assert packet.bound(expected['run']) == packet.data['current/' + profile + '-attribution-r5-result.json']
        assert expected['rows'] == rows and expected['passed'] == 19
        assert expected['source_fingerprint_sha256'] == protocol['source_fingerprint_sha256']
        profiles[profile] = rows
    for debug, release in zip(profiles['debug'], profiles['release'], strict=True):
        assert all(debug[key] == release[key] for key in ('case_id', 'source', 'origin', 'shared_aggregate', 'provider_selection_reason', 'complete_output_rows'))
    for item in feasibility['original_failed_debug_results']:
        assert json.loads(packet.bound(item))['status'] == 'failed'

    validation = packet.read('current/candidate-validation-r1.json')
    assert validation['status'] == 'passed_fixed_candidate_validation'
    assert validation['source_fingerprint_sha256'] == SOURCE_FINGERPRINT
    assert validation['diagnostic_requests'] == 19 and validation['native_test_count'] == 4144
    assert validation['ignored_tests'] == 36 and validation['native_query_executions_during_verification'] == 0
    assert not validation['whole_process_memory_claim'] and not validation['scored_performance_claim'] and not validation['candidate_acceptance_claimed']
    result = packet.read('current/candidate-debug-attribution-r1-result.json')
    baseline = packet.read('current/debug-attribution-r5-result.json')
    protocol = json.loads(packet.bound(result['protocol']))
    assert result['status'] == 'passed' and result['before_inputs_verified'] and result['after_inputs_verified']
    assert not result['performance_result'] and result['profile'] == 'debug'
    assert protocol['source_fingerprint_sha256'] == SOURCE_FINGERPRINT and protocol['candidate_selected'] and not protocol['scored']
    declared = json.loads(packet.bound(protocol['declarations']))
    assert [row['case_id'] for row in result['observations']] == [row['case_id'] for row in declared['jobs']]
    assert len(result['observations']) == len(baseline['observations']) == 19
    old_by_id = {row['case_id']: row for row in baseline['observations']}
    compare = functions(packet.data['current/tools/verify_direct_count_candidate.py'], {'validate_observation'}, {})['validate_observation']
    reductions = []
    for entry in result['observations']:
        assert entry['status'] == 'passed' and entry['returncode'] == 0 and entry['child_completed']
        observed = json.loads(packet.bound(entry['observation']))
        old = json.loads(packet.bound(old_by_id[entry['case_id']]['observation']))
        assert packet.bound(entry['baseline_observation']) == packet.bound(old_by_id[entry['case_id']]['observation'])
        removed = compare(observed, old)
        assert entry['removed_ordinal_rows_and_count_updates'] == removed
        if 'repeat-memory' in entry['case_id'] or 'constant-key-batch' in entry['case_id']:
            assert removed > 0 and observed['counts']['aggregate_count_partial_batches'] > 0
        log = packet.bound(dict(path=entry['log'], **entry['log_identity']))
        messages = [json.loads(line.split(b'DIRECT_COUNT ', 1)[1]) for line in log.splitlines() if b'DIRECT_COUNT ' in line]
        assert messages == [observed]
        reductions.append(dict(case_id=entry['case_id'], removed_updates=removed,
            input_rows=observed['counts']['aggregate_count_partial_input_rows'],
            partial_groups=observed['counts']['aggregate_count_partial_groups']))
    assert validation['reductions'] == reductions
    assert len(validation['rejected_corruptions']) == len(set(validation['rejected_corruptions'])) == 8
    guard = packet.guard('current/candidate-debug-attribution-r1', [PYTHON, '-B', ORIGINAL_ROOT + '/tools/run_candidate_diagnostics.py',
        '--protocol', 'diagnostic-protocol-candidate-debug-r1.json', '--name', 'candidate-debug-attribution-r1'], packet.report['candidate_source_snapshot'])
    assert guard['query_workload'] and guard['active_output'] == ORIGINAL_ROOT + '/candidate-debug-attribution-r1'
    assert guard['build_inventory_policy'] == guard['evidence_inventory_policy'] == 'before_and_after_only'
    return dict(feasibility_requests_per_profile=19, candidate_debug_requests=19, reductions=reductions,
        query_executions=0, performance_claim=False, whole_query_memory_claim=False)


def verify_installed(packet, source, runtime):
    root = ORIGINAL_ROOT + '/installed-proof-r1'
    python = ORIGINAL_ROOT + '/package-tools-r1/bin/python'
    commands = {
        'candidate-package-environment-r1': [PYTHON, '-m', 'venv', ORIGINAL_ROOT + '/package-tools-r1'],
        'candidate-package-tools-r1': [python, '-I', '-m', 'pip', 'install', '--only-binary=:all:', '--require-hashes', '-r', ROOT + '/.github/requirements/ci-native-python-build.txt'],
        'candidate-installed-r1': [python, '-B', ROOT + '/scripts/prove_native_python_wheel.py', '--repo-root', ROOT,
            '--output-dir', root, '--native-library', ORIGINAL_ROOT + '/runtime-r1/_native.abi3.so',
            '--cli-binary', ORIGINAL_ROOT + '/runtime-r1/shardloom', '--wheelhouse', JOIN_ROOT + '/wheelhouse-r1'],
    }
    for stem, command in commands.items():
        packet.guard('current/' + stem, command, source)
    report = packet.read('current/installed-proof-r1/report.json')
    assert report['schema_version'] == 'shardloom.native_python_wheel_proof.v1'
    assert report['status'] == 'passed' and report['installation_performed']
    assert report['scope'] == 'current_host_native_wheel_install_and_binding_integration'
    assert not report['publication_attempted'] and not report['performance_claim']
    assert report['uninstall_status'] == 'passed' and 'artifact_reuse' not in report
    assert report['build_tools']['setuptools'] == '84.0.0' and report['build_tools']['wheel'] == '0.48.0'
    assert report['build_tools']['pip']
    for key in ('stage', 'integrity'):
        item = report[key]
        assert item['status'] == 'passed'
        assert item['native_library_sha256'] == runtime['_native.abi3.so']['sha256']
        assert item['cli_sha256'] == runtime['shardloom']['sha256']
    integrity = report['integrity']
    assert integrity['schema_version'] == 'shardloom.native_python_wheel_integrity.v1'
    assert integrity['tag'].startswith('cp310-abi3-macosx_') and not integrity['published']
    wheel = root + '/dist/' + integrity['wheel']
    assert Path(wheel).parent == Path(root) / 'dist' and Path(wheel).suffix == '.whl'
    assert packet.report['identity_only_proof'][wheel] == dict(role='candidate_installed_wheel_identity',
        identity=dict(bytes=integrity['wheel_bytes'], sha256=integrity['wheel_sha256']))
    integration = report['integration']
    assert integration == packet.read('current/installed-proof-r1/integration.json')
    assert integration['status'] == 'passed' and type(integration['tests']) is int and integration['tests'] > 0
    assert integration['failures'] == integration['errors'] == integration['skipped'] == 0
    log = packet.data['current/installed-proof-r1/integration.log']
    assert re.search(rb'Ran ' + str(integration['tests']).encode() + rb' tests in [\d.]+s\n\nOK\n', log)
    assert not re.search(rb'^FAILED |^OK \(skipped=', log, re.M)
    loaded = report['installed_load']
    assert loaded == packet.read('current/installed-proof-r1/load.log') and loaded['isolated_install']
    assert all(Path(loaded[key]).is_relative_to(Path(root) / 'venv') for key in ('package', 'native_library', 'cli'))
    assert [step['name'] for step in report['steps']] == ['build', 'create_environment', 'install_optional_consumer', 'install', 'load', 'integration', 'uninstall', 'verify_uninstalled']
    for step in report['steps']:
        assert step['returncode'] == 0 and step['timeout_seconds'] == (1200 if step['name'] == 'integration' else 600)
        assert step['log'] == step['name'] + '.log'
        assert digest(packet.data['current/installed-proof-r1/' + step['log']]) == step['log_sha256']
    return dict(integration_tests=integration['tests'], skipped=0, isolated_install=True, uninstall_status='passed')


def integration_sources(packet):
    source = packet.report['candidate_source_snapshot']
    closeout = packet.read('join/join-mapping-closeout-r1.json')
    accepted = closeout['repository_source_snapshot']
    review = packet.read('current/pre-score-source-review-r1.json')
    assert review['schema'] == 'shardloom.direct_count_source_review.v1'
    assert review['status'] == 'completed_pre_score_source_review'
    assert review['source_head'] == BASE and review['source_fingerprint_sha256'] == SOURCE_FINGERPRINT
    assert review['reviewed_changed_files'] == {name:dict(before=accepted.get(name), after=source.get(name))
        for name in sorted(set(accepted) | set(source)) if accepted.get(name) != source.get(name)}
    assert review['accepted_repository'] == identity(packet.data['join/join-mapping-closeout-r1.json'])
    assert review['runtime_objections_reviewed'] == ['total_sort_and_source_order',
        'checked_count_and_producer_error_semantics', 'optional_capacity_admission_and_owner_lifetimes',
        'complete_request_and_no_fallback_proof']
    assert review['unresolved_in_scope_findings'] == [] and review['scored_query_executions'] == 0
    assert review['candidate_protocol_absent_at_review']
    assert review['review_document'] == identity(packet.data['current/adversarial-review-r1.md'])
    return dict(accepted)


def verify_sources(report, data, repo=None):
    packet = Packet(report, data)
    build = packet.read('current/candidate-release-build-r1.json')
    source = build['build_sources']
    assert report['source_base'] == BASE and report['candidate_source_snapshot'] == source
    assert report['candidate_source_fingerprint_sha256'] == fingerprint(source) == SOURCE_FINGERPRINT
    for name, command in CHECKS.items():
        guard = packet.guard('current/' + name, command, source)
        assert guard['base'] == guard['head'] == BASE and not guard['query_workload']
        assert report['checks'][name] == dict(command=command, status='passed', rust_tests=rust_counts(data['current/' + name + '.log']))
    assert rust_counts(data['current/count-partial-default-tests-r1.log']) == dict(passed=3323, failed=0, ignored=0)
    assert rust_counts(data['current/count-partial-native-tests-r2.log']) == dict(passed=7101, failed=0, ignored=40)
    assert report['ci_inventory'] == verify_ci(packet, source)
    closeout_raw = data['join/join-mapping-closeout-r1.json']
    assert digest(closeout_raw) == CLOSEOUT_SHA256
    closeout = json.loads(closeout_raw)
    assert closeout['status'] == 'complete' and closeout['merge_commit'] == BASE and closeout['source_tree'] == BASE_TREE
    assert closeout['pr_checks_passed'] == 58 and closeout['main_checks_passed'] == 55 and not closeout['review_findings']
    assert closeout['runtime_sources_match_build'] and closeout['current_inputs_unchanged']
    assert closeout['source_bound_replay'] == closeout['portable_replay_after_retirement'] == 'passed'
    assert closeout['disposition'] == 'drop_candidate_no_gain'
    accepted = closeout['repository_source_snapshot']
    assert fingerprint(accepted) == closeout['repository_source_fingerprint_sha256']
    assert closeout['complete_repository_source_equals_runtime_build'] is False
    previous = packet.read('window/candidate-release-build-r1.json')
    assert fingerprint(previous['build_sources']) == previous['build_source_fingerprint_sha256'] == closeout['source_fingerprint_sha256']
    differences = {name:dict(runtime=previous['build_sources'].get(name), repository=accepted.get(name))
        for name in sorted(set(accepted) | set(previous['build_sources'])) if accepted.get(name) != previous['build_sources'].get(name)}
    assert differences == closeout['repository_differences_from_runtime_build']
    assert len(differences) == 10
    assert {name for name in differences if not name.startswith('docs/')} == {'scripts/ci_native_tests.py', 'scripts/test_ci_native_tests.py'}
    for name, correction in closeout['independent_ci_cleanup_fix'].items():
        assert correction['before'] == differences[name]['runtime'] and correction['after'] == differences[name]['repository']
    assert report['status'] == 'local_invalid_baseline_checkpoint'
    assert report['disposition'] == 'not_admitted_invalid_baseline'
    assert report['candidate_admitted'] is False and report['investigation_b_complete'] is False
    assert report['scored_candidate_query_executions'] == 0
    assert report['engine_execution_seconds'] is None
    assert not any(name in data for name in ('current/candidate-protocol-r1.json',
        'current/candidate-verified-r1.json', 'current/candidate-r1/attempts.sqlite'))
    target = integration_sources(packet)
    for name, command in DOC_CHECKS.items():
        guard = packet.guard('current/' + name, command)
        assert guard['base'] == guard['head'] == BASE
        changed = {key for key in set(target) | set(guard['build_sources']) if target.get(key) != guard['build_sources'].get(key)}
        assert changed <= STATUS_PROSE
        counts = rust_counts(data['current/' + name + '.log'])
        assert report['documentation_checks'][name] == dict(command=command, status='passed', rust_tests=counts)
        if name == 'doc-contracts-r1':
            assert counts['passed'] > 0 and counts['failed'] == counts['ignored'] == 0
    runtime = packet.read('current/runtime-r1-identity.json')
    assert runtime['status'] == 'passed' and runtime['profile'] == 'release' and runtime['native_query_executions'] == 0
    assert runtime['build_receipt'] == 'candidate-release-build-r1.json'
    assert runtime['build_receipt_identity'] == identity(data['current/candidate-release-build-r1.json'])
    assert runtime['build_source_fingerprint_sha256'] == SOURCE_FINGERPRINT
    files = {Path(row['path']).name:row for row in runtime['files']}
    assert set(files) == {'shardloom', 'lib_native.dylib', '_native.abi3.so'}
    assert all(Path(row['path']).parent == Path(ORIGINAL_ROOT) / 'runtime-r1' for row in files.values())
    assert {key:files['lib_native.dylib'][key] for key in ('bytes', 'sha256')} == {key:files['_native.abi3.so'][key] for key in ('bytes', 'sha256')}
    packet.guard('current/candidate-freeze-runtime-r1', [PYTHON, '-B', ORIGINAL_ROOT + '/tools/freeze_runtime.py',
        '--build', 'candidate-release-build-r1.json', '--name', 'runtime-r1', '--profile', 'release'], source)
    for role in ('baseline',):
        packet.guard('current/' + role + '-plan-r1', [PYTHON, '-B', ORIGINAL_ROOT + '/tools/run_direct_count.py', '--role', role, '--plan'], source)
    protocol = packet.read('current/baseline-protocol-r1.json')
    binding = protocol['runtime_binding']
    assert binding['source_head'] == binding['accepted_merge'] == BASE
    assert binding['source_fingerprint_sha256'] == closeout['source_fingerprint_sha256']
    assert binding['candidate_source_fingerprint_sha256'] == SOURCE_FINGERPRINT
    assert binding['runtime'] == packet.read('current/baseline-reuse-r1.json')['runtime']
    for name, wanted in closeout['retained_runtime'].items():
        assert Path(name).parent == Path(WINDOW_ROOT) / 'runtime-r1'
        assert Path(name).name in ('shardloom', 'lib_native.dylib', '_native.abi3.so')
        assert report['identity_only_proof'][name] == dict(role='accepted_runtime_identity', identity=wanted)
    for row in binding['runtime'].values():
        assert closeout['retained_runtime'][row['path']] == dict(bytes=row['generation']['bytes'], sha256=row['sha256'])
    for role, name in (('binary', 'shardloom'), ('library', '_native.abi3.so')):
        row = binding['candidate_runtime'][role]
        assert dict(bytes=row['generation']['bytes'], sha256=row['sha256']) == {key:files[name][key] for key in ('bytes', 'sha256')}
    for row in binding['proof_files']:
        packet.proof(row)
    validation = packet.read('current/candidate-validation-r1.json')
    assert packet.bound(binding['candidate_validation']) == data['current/candidate-validation-r1.json']
    declared = {row['path']:row for row in binding['proof_files']}
    assert len(declared) == len(binding['proof_files'])
    for row in validation['proof_files']:
        assert declared[row['path']] == row
        packet.proof(row)
    packet.guard('current/candidate-proof-verification-r1', [PYTHON, '-B', ORIGINAL_ROOT + '/tools/verify_direct_count_candidate.py'], source)
    assert report['installed_package'] == verify_installed(packet, source, files)
    for field in ('complete_10k_performance_claim', 'full43_claim', 'whole_item_19_complete',
            'whole_item_20_complete', 'publication_attempted', 'whole_query_memory_claim'):
        assert report[field] is False
    dispositions = packet.read('current/failure-dispositions-r1.json')
    failures = []
    for name, raw in sorted(data.items()):
        if name.startswith('current/') and name.count('/') == 1 and name.endswith('.json'):
            record = json.loads(raw)
            if isinstance(record, dict) and (str(record.get('status')).startswith('failed') or record.get('status') == 'interrupted'):
                key = name.removeprefix('current/')
                assert dispositions[key]
                failures.append(dict(receipt=name, identity=identity(raw), disposition=dispositions[key]))
    assert report['original_failed_checks'] == failures
    assert ORIGINAL_FAILURES <= set(dispositions) == {row['receipt'].removeprefix('current/') for row in failures}
    clarification = packet.read('current/declaration-controls-r1-overwrite-clarification.json')
    assert clarification['native_query_executions'] == clarification['scored_query_executions'] == 0
    assert clarification['original_guard_and_log_preserved']
    assert clarification['original_detailed_checker_report_available'] is False
    verify_restoration(packet)
    original = packet.read('preparation/current-baseline-10k-r1/failed-observation-replay-r1.json')
    assert original['status'] == 'verified_failed_observation' and original['verification_passed']
    assert original['observed_attempts'] == 10000 and original['verified_correct_results'] == 9904
    assert original['original_outcomes'] == {'passed':9904, 'crashed':76, 'harness_error':20}
    assert original['engine_execution_seconds'] is None and original['complete_performance_result_valid'] is False
    if repo is not None:
        for name, item in target.items():
            if name in STATUS_PROSE:
                continue
            path = repo / name
            if 'symlink' in item:
                assert path.is_symlink() and str(path.readlink()) == item['symlink']
            elif item.get('absent'):
                assert not path.exists()
            else:
                assert path.is_file() and not path.is_symlink() and identity(path.read_bytes()) == item, name
        paths = set()
        excluded = ('docs/benchmarks/evidence/', 'docs/release/evidence/', 'docs/dependencies/evidence/')
        for command in (['git', 'ls-files', '--cached', '-z'], ['git', 'ls-files', '--others', '--exclude-standard', '-z']):
            paths.update(name for name in subprocess.check_output(command, cwd=repo).decode().split('\0')
                if name and not name.startswith(('.git/', *excluded)) and '.git' not in Path(name).parts)
        assert (paths ^ set(target)) <= STATUS_PROSE
    return packet


def replay_sources(packet):
    data = packet.data
    sources = {name.removeprefix('harness/'):raw for name, raw in data.items() if name.startswith('harness/')}
    frozen = packet.read('frozen/frozen.json')
    assert digest(data['frozen/frozen.json']) == FROZEN_SHA256
    manifest = packet.read('frozen/investigation-original-harness-source-r1.json')
    assert manifest['commit'] == '0155355e7927e6f8a4747daae5a6daf3438e822b'
    assert manifest['published_frozen_sha256'] == FROZEN_SHA256
    assert len(sources) == len(frozen['harness']) == len(manifest['harness_sha256']) == 65
    for row in frozen['harness']:
        raw = packet.bound(row)
        relative = packet.report['locations'][row['path']].removeprefix('harness/')
        assert manifest['harness_sha256'][relative] == digest(raw) == digest(sources[relative])
    source = packet.report['candidate_source_snapshot']
    for name in REPLAY_SUPPORT:
        assert name not in sources
        raw = data['source-blobs/' + source[name]['sha256']]
        assert identity(raw) == source[name]
        sources[name] = raw
    for row in frozen['references']:
        packet.bound(row)
    return sources


def replay_saved(report, data, temporary):
    temporary = temporary.resolve()
    packet = Packet(report, data)
    protocols = {'baseline':packet.read('current/baseline-protocol-r1.json')}
    results = {'baseline':packet.read('current/baseline-verified-r1.json')}
    old = protocol = protocols['baseline']
    old_result = result = results['baseline']
    selection = json.loads(packet.bound(old['selection']))
    reuse = json.loads(packet.bound(old['reuse']))
    assert result['selected_cases'] == 2543
    assert result['reused_saved_cases'] == 159 and result['new_attempts'] == 2384
    assert result['query_executions_during_replay'] == 0 and result['status'] == 'failed_baseline_cohort'
    assert old['candidate_contract']['source_fingerprint_sha256'] == SOURCE_FINGERPRINT
    assert old['candidate_contract']['stage'] == 'source_and_release_runtime_fixed_before_any_new_scored_query'
    assert old['cases'] == [row for row, used in zip(selection['cases'], reuse['requests'], strict=True) if not used['reused']]
    assert old['new_attempts'] == len(old['cases']) == 2384
    for item in (old, result):
        assert item['primary_clock'] == 'engine_execution_seconds'
        assert item['attempts_per_case'] == 1 and item['warmups'] == item['requested_repeats'] == 0
        assert not item['complete_10k_performance_claim'] and not item['full43_claim']
    assert old['selected_count'] == 2543 and old['original_controls'] == 2258
    assert old['saved_count_requests'] == 2098 and old['no_count_neighbors'] == 160 and old['new_compositions'] == 285
    assert old['selection_uses_timing'] is False
    assert packet.bound(result['protocol']) == data['current/baseline-protocol-r1.json']
    assert old['selection'] == result['selection'] and old['reuse'] == result['reuse']
    candidate_proof = {row['path']:row for row in old['runtime_binding']['proof_files']}
    assert len(candidate_proof) == len(old['runtime_binding']['proof_files'])
    assert result['runtime_binding'] == old['runtime_binding']
    design, inputs = (packet.read('frozen/' + name + '.json') for name in ('design', 'inputs'))
    prepared = packet.read('frozen/native-preparation/complete.json')
    assert len(design['cases']) == 10000 and old['execution'] == design['execution']
    for name in ('design', 'inputs', 'frozen'):
        assert packet.bound(old[name]) == data['frozen/' + name + '.json']
    assert selection['status'] == 'frozen_before_scoring' and selection['selected_requests'] == 2543
    assert selection['saved_count_requests'] == 2098 and selection['no_count_neighbors'] == 160 and selection['new_declarations'] == 285
    assert selection['query_executions'] == 0 and not selection['selection_uses_timing']
    assert Counter(row['kind'] for row in selection['cases']) == {'original':2258, 'composition':285}
    assert len({row['case']['id'] for row in selection['cases']}) == 2543
    assert len({row['case']['case_identity'] for row in selection['cases']}) == 2543
    for selected in selection['cases']:
        if selected['kind'] == 'original':
            assert selected['case'] == design['cases'][selected['original_seq']]
        else:
            assert selected['original_seq'] is None and 'direct_count_version' in selected['case']

    written = {}
    def write_source(path, raw):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        written[path.resolve()] = raw
    harness = temporary / 'expanded-query-2026-11-03/investigation-original-harness-r1'
    for name, raw in replay_sources(packet).items():
        write_source(harness / name, raw)
    preparation = temporary / 'shared-operator-state-2026-11-10'
    for name in ('window-tools-draft/window_replay.py', 'window-client-draft/shared_window_worker.py', 'window-client-draft/shared_window_cases.py'):
        write_source(preparation / name, packet.bound(candidate_proof[PREPARATION_ROOT + '/' + name]))
    for name, raw in data.items():
        if name.endswith('.sqlite'):
            safe_name(name)
            path = temporary / 'databases' / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
    current = temporary / 'direct-count-bridge-2026-11-12'
    for name in ('client/direct_count_worker.py', 'client/direct_count_cases.py'):
        write_source(current / name, packet.bound(candidate_proof[ORIGINAL_ROOT + '/' + name]))
    sys.path.insert(0, str(preparation / 'window-tools-draft'))
    import window_replay as old_replay
    sys.path.insert(0, str(current / 'client'))
    import direct_count_cases
    from query_surface import adapters
    replay_module = types.ModuleType('archived_direct_count_replay')
    replay_module.__dict__.update(old_proof=old_replay, worker=types.SimpleNamespace(declarations=direct_count_cases),
        SimpleNamespace=types.SimpleNamespace, json=json, adapters=adapters)
    functions(data['current/tools/direct_count_common.py'], {'Cases', 'replay_case'}, replay_module.__dict__)
    from query_surface.expanded_freeze import unpack
    from query_surface.expanded_report import raw_files
    from query_surface.expanded_run import request
    assert not any(name == 'shardloom._native' or name.startswith('shardloom._native.') for name in sys.modules)
    for name, module in list(sys.modules.items()):
        if name.startswith(('query_surface', 'shardloom', 'shared_window', 'direct_count_cases')) or name in (
                'window_replay', 'query_timing_report', 'native_workflow_protocol', 'local_uat_storage', 'resources',
                'run_clickbench_query_uat', 'timed_native_command', 'clickbench_reference_packet', 'clickbench_native_sql'):
            path = Path(module.__file__).resolve()
            assert path in written and path.read_bytes() == written[path], name
    cases = replay_module.Cases()
    archived_rows = {role:[json.loads(line) for line in gzip.decompress(packet.bound(item['case_rows'])).splitlines()]
        for role,item in results.items()}
    assert all(len(rows) == 2543 for rows in archived_rows.values())
    assert reuse['schema'] == 'shardloom.direct_count_reuse.v1' and reuse['status'] == 'passed_exact_complete_reuse_audit'
    assert reuse['selected_reused_cases'] == old['reused_count'] == 159 and reuse['missing_cases'] == 2384
    assert reuse['complete_requests_verified'] == len(reuse['requests']) == 2543
    assert reuse['query_executions'] == 0 and not reuse['original_ledgers_recreated'] and reuse['original_observations_preserved']
    assert reuse['runtime'] == old['runtime_binding']['runtime'] and reuse['selection'] == old['selection']
    assert reuse['source_fingerprint_sha256'] == old['runtime_binding']['source_fingerprint_sha256']
    target = {row['case']['id']:row for row in selection['cases']}
    available = {}
    assert len(reuse['inspected_pools']) == 3
    for row, count in zip(reuse['inspected_pools'], (1212, 1090, 940), strict=True):
        prior = json.loads(packet.bound(row['protocol']))
        saved = json.loads(packet.bound(row['result']))
        accepted = json.loads(packet.bound(row['packet_acceptance']))
        members = {item['path']:{key:item[key] for key in ('bytes', 'sha256')} for item in accepted['members']}
        for key in ('protocol', 'result', 'case_rows'):
            assert identity(packet.bound(row[key])) == members['current/' + Path(row[key]['path']).name]
        assert prior['runtime_binding']['runtime'] == reuse['runtime']
        assert prior['runtime_binding']['source_fingerprint_sha256'] == reuse['source_fingerprint_sha256']
        assert saved['selected_cases'] == row['observed_cases'] == count and saved['outcomes'] == {'passed':count}
        assert saved['complete_performance_result_valid'] and saved['case_rows'] == row['case_rows']
        rows = [json.loads(line) for line in gzip.decompress(packet.bound(row['case_rows'])).splitlines()]
        assert len(rows) == len({item['id'] for item in rows}) == count
        matches = []
        for ordinal, item in enumerate(rows):
            if item['id'] not in target:
                continue
            selected = target[item['id']]
            assert all(item[key] == selected['case'][key] for key in ('id', 'case_identity', 'profile', 'surface', 'native_format'))
            assert row['root'] == JOIN_ROOT and row['role'] == 'baseline'
            assert item['id'] not in available and item['result']['status'] == 'passed'
            assert prior['missing_cases'][item['provenance']['source_seq']]['case'] == selected['case']
            available[item['id']] = item
            matches.append(item['id'])
        assert matches == row['matching_ids']
    assert [len(row['matching_ids']) for row in reuse['inspected_pools']] == [0, 0, 159]
    assert len(available) == 159
    assert packet.bound(reuse['reused_database']) == data['current/baseline-reused-r1.sqlite']

    captures = {}
    for role, total in (('baseline', 2384),):
        prefix = 'current/' + ('baseline-missing-r1' if role == 'baseline' else 'candidate-r1')
        done, binding = packet.read(prefix + '/complete.json'), packet.read(prefix + '/binding.json')
        assert done['status'] == 'complete_observation' and done['stage'] == 'native'
        assert done['total'] == done['attempt_slots'] == total
        assert done['posthash_verified'] and done['attempts_per_case'] == 1 and done['requested_repeats'] == 0
        assert binding['contract'] == {key:done[key] for key in ('frozen', 'stage', 'prerequisites', 'updated')}
        assert packet.bound(done['ledger']) == data[prefix + '/attempts.sqlite']
        assert packet.bound(done['binding']) == data[prefix + '/binding.json']
        packet.bound(done['frozen'])
        guard = packet.guard(prefix, [PYTHON, '-B', ORIGINAL_ROOT + '/tools/run_direct_count.py', '--role', role], report['candidate_source_snapshot'])
        assert guard['query_workload'] and guard['build_inventory_policy'] == guard['evidence_inventory_policy'] == 'before_and_after_only'
        assert guard['base'] == guard['head'] == BASE
        assert guard['active_output'] == ORIGINAL_ROOT + '/' + prefix.removeprefix('current/')
        assert done['updated'] == protocols[role]['runtime_binding'] == results[role]['runtime_binding']
        captures[role] = dict(prefix=prefix, done=done, runtime=done['updated']['runtime'])
    outcomes, new_outcomes, families, totals = {}, {}, {}, {}
    failure_details = []
    with ExitStack() as stack:
        def database(name):
            connection = sqlite3.connect('file:' + str(temporary / 'databases' / name) + '?mode=ro', uri=True)
            connection.execute('PRAGMA query_only=ON')
            assert connection.execute('PRAGMA integrity_check').fetchone() == ('ok',)
            stack.callback(connection.close)
            return connection
        refs = {profile:database('frozen/reference-' + profile + '/expectations.sqlite')
            for profile in {row['case']['profile'] for row in selection['cases']}}
        refs['composition'] = database('current/references-r1/expectations.sqlite')
        dbs = {role:database(row['prefix'] + '/attempts.sqlite') for role,row in captures.items()}
        reused_db = database('current/baseline-reused-r1.sqlite')
        assert reused_db.execute('SELECT count(*),count(distinct id) FROM attempt').fetchone() == (159,159)
        for role,total in (('baseline', 2384),):
            assert dbs[role].execute('SELECT count(*),min(seq),max(seq),count(distinct id) FROM attempt').fetchone() == (total,0,total-1,total)
        for role, rows in archived_rows.items():
            counts, fresh, family_values, seconds = Counter(), Counter(), defaultdict(list), []
            capture, missing, used = captures[role], 0, 0
            for ordinal, (selected, archived, declared) in enumerate(zip(selection['cases'], rows, reuse['requests'], strict=True)):
                case = selected['case']
                assert archived['cohort_ordinal'] == selected['cohort_ordinal'] == declared['cohort_ordinal'] == ordinal
                assert archived['original_seq'] == selected['original_seq'] and archived['kind'] == selected['kind']
                assert all(archived[key] == case[key] for key in ('id', 'case_identity', 'family', 'profile', 'surface', 'native_format'))
                assert declared['id'] == case['id'] and declared['case_identity'] == case['case_identity']
                reused = role == 'baseline' and declared['reused']
                if reused:
                    seq, connection = ordinal, reused_db
                    assert declared['reused_seq'] == ordinal and declared['missing_seq'] is None
                    used += 1
                else:
                    seq, connection = missing if role == 'baseline' else ordinal, dbs[role]
                    assert protocols[role]['cases'][seq] == selected
                    if role == 'baseline':
                        assert declared['missing_seq'] == missing
                        missing += 1
                entry = connection.execute('SELECT seq,id,status,packet FROM attempt WHERE seq=?', (seq,)).fetchone()
                assert entry is not None and entry[:2] == (seq, case['id'])
                item = cases.item(case)
                reference = refs['composition' if selected['kind'] == 'composition' else case['profile']].execute(
                    'SELECT identity,value FROM expected WHERE id=?', (case['id'],)).fetchone()
                assert reference and reference[0] == item.identity == case['case_identity']
                assert declared['reference_blob_sha256'] == digest(reference[1])
                expected = unpack(reference[1])
                config = dict(inputs, runtime=capture['runtime'])
                observed = replay_module.replay_case(entry, case, expected, item, config, prepared, design['execution'])
                assert observed == archived['result'] and observed['status'] == entry[2]
                files = raw_files(unpack(entry[3])) if entry[3] is not None else {}
                if files:
                    job = json.loads(files['one.job.json'])
                    assert job == request(case, 'native', config, prepared, expected, design['execution'])
                    if role == 'baseline':
                        assert fingerprint(job) == declared['request_sha256']
                provenance = archived['provenance']
                assert provenance['ledger'] == (reuse['reused_database']['path'] if reused else capture['done']['ledger']['path'])
                assert provenance['source_seq'] == seq
                assert provenance['source'] == ('accepted_reused_baseline' if reused else 'new_' + role)
                assert provenance['packet'] == (identity(entry[3]) if entry[3] is not None else None)
                assert provenance['job_sha256'] == (digest(files['one.job.json']) if files else None)
                assert provenance['independent_expected_blob_sha256'] == digest(reference[1])
                if reused:
                    original = available[case['id']]
                    assert declared['source_role'] == 'accepted_join_mapping_baseline'
                    assert declared['source_cohort_ordinal'] == original['cohort_ordinal']
                    assert declared['source_seq'] == original['provenance']['source_seq']
                    assert declared['packet'] == original['provenance']['packet'] == identity(entry[3])
                    assert declared['result'] == original['result'] == observed
                    assert declared['original_provenance'] == original['provenance'] == provenance['original_provenance']
                    assert digest(files['one.job.json']) == original['provenance']['job_sha256']
                else:
                    fresh[observed['status']] += 1
                counts[observed['status']] += 1
                if observed['status'] != 'passed':
                    assert selected['kind'] == 'composition' and not reused
                    failure_details.append(replay_failure(case, seq, ordinal, observed, unpack(entry[3]), entry[3], files, cases, adapters))
                    continue
                value = observed['engine_seconds']
                assert type(value) in (int,float) and math.isfinite(value) and value >= 0
                seconds.append(value)
                family_values[case['family']].append(value)
            assert used == (159 if role == 'baseline' else 0)
            assert missing == (2384 if role == 'baseline' else 0)
            outcomes[role], new_outcomes[role], families[role] = dict(counts), dict(fresh), family_values
            totals[role] = math.fsum(seconds) if counts == {'passed':2543} else None
    wanted = {'passed':2483, 'engine_rejected':15, 'engine_error':30, 'incorrect':15}
    assert outcomes['baseline'] == result['outcomes'] == wanted
    assert new_outcomes['baseline'] == result['new_outcomes'] == captures['baseline']['done']['outcomes'] == {
        'passed':2324, 'engine_rejected':15, 'engine_error':30, 'incorrect':15}
    assert totals['baseline'] is None and result['engine_execution_seconds'] is None
    for field in ('complete_performance_result_valid', 'all_selected_results_correct', 'all_engine_clocks_valid', 'native_no_fallback'):
        assert result[field] is False
    assert result['performance_claim'] is False
    rows = archived_rows['baseline']
    assert Counter(row['result']['status'] for row in rows if row['kind'] == 'original') == {'passed':2258}
    assert Counter(row['result']['status'] for row in rows if row['kind'] == 'composition') == {
        'passed':225, 'engine_rejected':15, 'engine_error':30, 'incorrect':15}
    assert report['baseline'] == dict(cases=2543, reused_saved_cases=159, new_attempts=2384,
        outcomes=wanted, engine_execution_seconds=None, complete_performance_result_valid=False)
    assert report['candidate'] == dict(admitted=False, scored_query_executions=0,
        comparison_available=False, gain_loss_disposition=None)
    for name, values in families['baseline'].items():
        assert result['families'][name] == dict(cases=len(values), engine_execution_seconds=math.fsum(values))
    assert set(result['families']) == set(families['baseline'])
    assert result['failures'] == [dict(cohort_ordinal=row['cohort_ordinal'], id=row['id'],
        status=row['result']['status'], detail=row['result'].get('failure_detail')) for row in rows if row['result']['status'] != 'passed']
    verify_failure_evidence(packet, failure_details)
    packet.guard('current/baseline-replay-r1', [PYTHON, '-B', ORIGINAL_ROOT + '/tools/replay_direct_count.py',
        '--role', 'baseline'], report['candidate_source_snapshot'], expected_failure=True)
    return dict(baseline_cases=2543, reused_baseline_cases=159, new_baseline_cases=2384,
        candidate_cases=0, outcomes=outcomes, engine_execution_seconds=None,
        disposition='not_admitted_invalid_baseline', investigation_b_complete=False, query_executions=0,
        complete_10k_performance_claim=False, full43_claim=False)


def verify_restoration(packet):
    accepted = packet.read('join/join-mapping-closeout-r1.json')['repository_source_snapshot']
    candidate = packet.report['candidate_source_snapshot']
    plan = packet.read('current/source-restoration-r1/plan.json')
    receipt = packet.read('current/source-restoration-r1/receipt.json')
    changes = {name:dict(before=candidate.get(name), after=accepted.get(name))
        for name in sorted(set(candidate) | set(accepted)) if candidate.get(name) != accepted.get(name)}
    assert len(changes) == 19 and sum(row['after'] is None for row in changes.values()) == 4
    assert plan['changes'] == changes and receipt['restored'] == list(changes)
    assert all(name == CI_POLICY_PATH or name.startswith('shardloom-vortex/src/') for name in changes)
    assert plan['status'] == 'planned_no_restoration' and receipt['status'] == 'passed_exact_source_restoration'
    assert receipt['plan'] == identity(packet.data['current/source-restoration-r1/plan.json'])
    assert plan['helper'] == identity(packet.data['current/tools/restore_unadmitted_count.py'])
    assert receipt['accepted_repository'] == identity(packet.data['join/join-mapping-closeout-r1.json'])
    for record in (plan, receipt):
        assert record['source_before_sha256'] == fingerprint(candidate) == SOURCE_FINGERPRINT
        assert record['source_after_sha256'] == fingerprint(accepted) == 'f3b5df1f92c9948a1aa5a3c3ab7655370fec7b807d942c19596ae586a2bfbbd5'
        assert record['candidate_admitted'] is False and record['investigation_b_complete'] is False
        assert record['query_executions'] == 0
        assert record['candidate_patch'] == identity(packet.data['current/candidate-proposal-r1.patch'])
    assert receipt['protected_files_unchanged'] and receipt['current_inputs_unchanged']
    assert receipt['working_tree'] == '' and receipt['candidate_patch_reapplication_check'] == 'passed'
    assert len(plan['input_generations']) == 220 and len(plan['protected']) == 12
    assert plan['input_generations'] == packet.read('current/baseline-missing-r1.json')['input_generations']
    for name, expected in plan['protected'].items():
        if name in packet.report['identity_only_proof']:
            assert packet.report['identity_only_proof'][name]['identity'] == expected
        else:
            assert identity(packet.data[packet.report['locations'][name]]) == expected
    patch = packet.data['current/candidate-proposal-r1.patch']
    assert digest(patch) == 'e6b7672d775090b69c687606ced5a257c59b1bc81c048240af679fb7446e26b1'
    paths = re.findall(rb'^diff --git a/(\S+) b/(\S+)$', patch, re.M)
    assert len(paths) == 19 and all(left == right for left,right in paths)
    assert {left.decode() for left,_ in paths} == set(changes)
    with tempfile.TemporaryDirectory(prefix='count-proposal-check-') as directory:
        root = Path(directory)
        for name, row in changes.items():
            safe_name(name)
            path = root / name
            if row['after'] is not None:
                raw = packet.data['source-blobs/' + row['after']['sha256']]
                assert identity(raw) == row['after']
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(raw)
        for arguments in (['--check'], []):
            process = subprocess.run(['git', 'apply', *arguments, '-'], cwd=root, input=patch, capture_output=True)
            assert process.returncode == 0, process.stderr
        for name, row in changes.items():
            assert identity((root / name).read_bytes()) == row['before']


def replay_failure(case, seq, ordinal, observed, packed, blob, files, cases, adapters):
    job = json.loads(files['one.job.json'])
    status = observed['status']
    assert job['case'] == case and packed['classified_status'] == status
    row = dict(id=case['id'], case_identity=case['case_identity'], baseline_seq=seq,
        cohort_ordinal=ordinal, scenario=case['archetype'], surface=case['surface'],
        native_format=case['native_format'], profile=case['profile'], original_status=status,
        detail=packed['classification_detail'], packet=identity(blob))
    if case['archetype'].startswith('uint64_'):
        assert case['surface'] in ('cli_sql', 'embedded_workflow', 'python_sql')
        assert status == ('engine_rejected' if case['surface'] == 'cli_sql' else 'engine_error')
        assert 'CAST target dtype must be' in row['detail']
        assert 'no fallback execution was attempted' in row['detail']
        sql = adapters.relation_sql(cases.item(case).relation, job['sources'])
        assert ' AS UBIGINT)' in sql
        row.update(category='frozen_adapter_unsigned_sql_spelling', regenerated_sql=sql,
            admitted_engine_spelling='uint64', emitted_adapter_spelling='UBIGINT')
    else:
        assert case['archetype'] == 'mixed_measure' and status == 'incorrect'
        actual, expected = json.loads(files['one.stdout.json'])['actual'], job['expected']
        assert actual['schema'][:2] == expected['schema'][:2] and len(actual['schema']) == len(expected['schema']) == 3
        assert expected['schema'][2] == dict(name='total', type=dict(name='int64', nullable=True, parameters=[]))
        assert actual['schema'][2] == dict(name='total', type=dict(name='float64', nullable=True, parameters=[]))
        assert len(actual['rows']) == len(expected['rows'])
        for native, reference in zip(actual['rows'], expected['rows'], strict=True):
            assert native.keys() == reference.keys() == {'k', 'n', 'total'}
            assert native['k'] == reference['k'] and native['n'] == reference['n']
            assert abs(reference['total']) < 2**53
            assert float.fromhex(native['total']) == reference['total']
        row.update(category='new_mixed_sum_declared_result_dtype', expected_dtype='int64',
            native_dtype='float64', complete_numeric_values_match=True, original_schema_mismatch_preserved=True)
    return row


def verify_failure_evidence(packet, failures):
    report = packet.read('current/baseline-admission-failure-r1.json')
    assert report['schema'] == 'shardloom.direct_count_baseline_failure.v1'
    assert report['status'] == 'verified_invalid_baseline'
    assert report['source_fingerprint_sha256'] == SOURCE_FINGERPRINT
    assert report['failures'] == failures and len(failures) == 60
    categories = Counter(row['category'] for row in failures)
    assert categories == report['failure_categories'] == {
        'frozen_adapter_unsigned_sql_spelling':45, 'new_mixed_sum_declared_result_dtype':15}
    assert report['original_controls'] == report['original_controls_passed'] == 2258
    assert report['new_controls'] == 285 and report['new_controls_passed'] == 225
    assert report['outcomes'] == packet.read('current/baseline-verified-r1.json')['outcomes']
    assert report['engine_execution_seconds'] is None and report['complete_performance_result_valid'] is False
    assert report['scored_candidate_query_executions'] == report['native_queries_executed_by_audit'] == 0
    assert report['candidate_protocol_present'] is False and report['original_results_and_references_unchanged']
    for field, path in (
            ('accepted_repository', 'join/join-mapping-closeout-r1.json'),
            ('baseline_result', 'current/baseline-verified-r1.json'),
            ('baseline_protocol', 'current/baseline-protocol-r1.json'),
            ('baseline_ledger', 'current/baseline-missing-r1/attempts.sqlite'),
            ('case_rows', 'current/baseline-cases-r1.jsonl.gz'),
            ('candidate_validation', 'current/candidate-validation-r1.json')):
        assert packet.bound(report[field]) == packet.data[path]
    adapter = packet.bound(report['adapter'])
    assert b'"uint64": "UBIGINT"' in adapter
    helper = packet.data['current/tools/record_baseline_admission_failure.py']
    rust = functions(helper, {'rust_function'}, {})['rust_function']
    accepted = packet.read('join/join-mapping-closeout-r1.json')['repository_source_snapshot']
    candidate = packet.report['candidate_source_snapshot']
    paths = ('shardloom-cli/src/sql_local_source_runtime.rs',
        'shardloom-vortex/src/local_primitive_relational_aggregate_bind.rs')
    assert set(report['source_evidence']) == set(paths)
    for index, path in enumerate(paths):
        name = 'parse_cast_target_dtype' if index == 0 else 'reduction_dtype'
        marker = ('fn ' if index == 0 else 'pub(super) fn ') + name + '('
        before, after = [rust(packet.data['source-blobs/' + source[path]['sha256']], marker)
            for source in (accepted, candidate)]
        assert before == after
        if index == 0:
            assert '"uint64" => Ok(LogicalDType::UInt64)' in before and 'ubigint' not in before.lower()
        else:
            assert 'Function::Sum | Function::Avg | Function::SumDistinct | Function::AvgDistinct' in before
            assert 'Some(DType::Primitive(_, _)) => DType::Primitive(PType::F64, Nullability::Nullable)' in before
        assert report['source_evidence'][path] == dict(accepted=accepted[path], current=candidate[path],
            inspected_function=name, relevant_function_unchanged=True)
    packet.guard('current/baseline-failure-verification-r1', [PYTHON, '-B',
        ORIGINAL_ROOT + '/tools/record_baseline_admission_failure.py'], candidate)


CONTROL_NAMES = {
    'native_test_failure', 'undrained_native_process', 'mutable_source_validation',
    'count_source_changed', 'root_lock_changed', 'required_test_missing_with_rebound_log',
    'ignored_ci_policy_changed', 'actual_ci_inventory_changed', 'runtime_library_mismatch',
    'partial_work_corrupted', 'owner_release_denied', 'complete_attribution_corrupted',
    'baseline_engine_total_invented', 'baseline_case_omitted', 'baseline_reuse_corrupted',
    'failed_status_reclassified', 'candidate_execution_claim', 'candidate_admission_claim',
    'investigation_complete_claim', 'restored_source_corrupted', 'proposal_patch_corrupted',
    'original_failure_omitted', 'full10k_claim', 'full43_claim',
    'composition_identity_changed', 'composition_reference_changed', 'archive_byte_changed',
    'installed_skipped_test', 'installed_runtime_mismatch', 'installed_publication_claim',
    'baseline_launcher_changed', 'sum_schema_failure_erased', 'unsigned_sql_failure_erased',
}


def verify_controls(packet_path, report, data):
    controls = json.loads((packet_path / 'corruption-controls.json').read_bytes())
    assert controls['status'] == 'passed' and controls['query_executions'] == 0
    assert controls['acceptance_manifest'] == identity((packet_path / 'acceptance.json').read_bytes())
    assert controls['positive_controls'] == 2 and controls['rejected_corruptions'] == len(CONTROL_NAMES)
    assert len(controls['controls']) == len(CONTROL_NAMES) + 2
    assert {row['name'] for row in controls['controls'] if row['outcome'] == 'rejected'} == CONTROL_NAMES
    assert {row['name'] for row in controls['controls'] if row['outcome'] == 'passed'} == {'source_bound', 'relocated_source_only'}
    for name in ('verify_direct_count_packet.py', 'check_direct_count_packet_controls.py'):
        assert controls['sources'][name] == identity(data['current/tools/' + name])
    assert controls['archives'] == report['archives']
    expected = {name + suffix for name in CONTROL_NAMES | {'source-bound', 'relocated'}
        for suffix in ('.stdout.log', '.stderr.log')}
    assert {row['path'] for row in controls['observations']} == {'control-observations/' + name for name in expected}
    assert len(controls['observations']) == len(expected)
    for row in controls['observations']:
        safe_name(row['path'])
        assert identity((packet_path / row['path']).read_bytes()) == {key:row[key] for key in ('bytes','sha256')}
    return dict(positive_controls=2, rejected_corruptions=len(CONTROL_NAMES))


def verify(packet_path, repo=None, require_controls=False):
    if not __debug__:
        raise RuntimeError('evidence verification requires assertion checks')
    report, data = read_archive(packet_path)
    packet = verify_sources(report, data, repo)
    diagnostics = verify_diagnostics(packet)
    with tempfile.TemporaryDirectory(prefix='count-checkpoint-evidence-') as temporary:
        replay = replay_saved(report, data, Path(temporary))
    controls = verify_controls(packet_path, report, data) if require_controls else None
    return dict(status='passed_source_bound_replay', diagnostics=diagnostics, controls=controls, **replay)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo-root', type=Path)
    parser.add_argument('--require-controls', action='store_true')
    args = parser.parse_args()
    print(json.dumps(verify(Path(__file__).resolve().parent, args.repo_root, args.require_controls), indent=2, sort_keys=True))
