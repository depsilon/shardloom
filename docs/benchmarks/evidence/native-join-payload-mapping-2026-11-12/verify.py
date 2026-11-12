"""Replay native join payload mappings from archived source, requests and results.

No native library, query, compiler or external reference engine is executed.
"""
from __future__ import annotations
import argparse
import ast
from collections import Counter, defaultdict
from contextlib import ExitStack
import gzip
import hashlib
import itertools
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

BASE = 'e10fc31e4ada819038711dd3a4658c3ad10a91f0'
BASE_TREE = '0d8991d11b2decca87d60f7b25a3f06672cb341f'
ORIGINAL_ROOT = '/Users/dylan/LocalData/shardloom/join-mapping-2026-11-11'
WINDOW_ROOT = '/Users/dylan/LocalData/shardloom/shared-window-state-2026-11-11'
DISTINCT_ROOT = '/Users/dylan/LocalData/shardloom/rolling-distinct-2026-11-11'
PREPARATION_ROOT = '/Users/dylan/LocalData/shardloom/shared-operator-state-2026-11-10'
ROOT = '/Users/dylan/.codex/worktrees/native-vector-representations/shardloom-local-repo'
SOURCE_FINGERPRINT = '0304c24e95c4d1bc963a6ce1d16fff58b2bc9a7aa16890ef50483fd44c151024'
BASELINE_PROTOCOL_SHA256 = '29165122564ca6442f5ecb2caf76164c42307973063e7fbe3c0676bcf00a29c8'
BASELINE_RESULT_SHA256 = '9899c58304e985e6b467279f6b5d66bdd68ef4690225eb6c0e5cedd6e1a1bd8d'
CLARIFICATION_SHA256 = '7e44110c6fe280e22a9e384570dae154b8040778e339354219223161c1f34c36'
FROZEN_SHA256 = '38c7a349ec04645e094248d20cfd3d8325e33a63ad76dd9bdc1543412bf7681d'
FEATURES = 'shardloom-python/python-native,shardloom-vortex/native-encoded-exchange-experiment'
PYTHON = '/opt/homebrew/opt/python@3.13/libexec/bin/python3'
CLIENT_PYTHON = '/Users/dylan/.cache/shardloom/query-surface-polars-2.1.0/bin/python'
CI_POLICY_PATH = '.github/ci-native-baseline.json'
CI_DIAGNOSTIC = 'local_primitives::native_relational_join::tests::mapping_attribution_tests::successive_join_ranked_payload_work_attribution_matrix'
REPLAY_SUPPORT = ('scripts/native_workflow_protocol.py', 'scripts/clickbench_reference_packet.py', 'scripts/clickbench_native_sql.py')
CHECKS = {
    'candidate-format-r2': ['cargo', 'fmt', '--all', '--', '--check'],
    'candidate-default-clippy-r2': ['cargo', 'clippy', '--offline', '--locked', '--workspace', '--all-targets', '--', '-D', 'warnings'],
    'candidate-default-tests-r2': ['env', 'RUST_TEST_THREADS=4', 'cargo', 'test', '--offline', '--locked', '--workspace', '--all-targets'],
    'candidate-native-clippy-r3': ['cargo', 'clippy', '--offline', '--locked', '--workspace', '--all-targets', '--features', FEATURES, '--', '-D', 'warnings'],
    'candidate-native-tests-r2': ['env', 'RUST_TEST_THREADS=4', 'cargo', 'test', '--offline', '--locked', '--workspace', '--all-targets', '--features', FEATURES],
    'candidate-attribution-r4': ['cargo', 'test', '--offline', '--locked', '--workspace', '--lib', '--features', FEATURES,
        'successive_join_ranked_payload', '--', '--include-ignored', '--show-output', '--test-threads=1'],
    'candidate-ci-cleanup-tests-r1': [PYTHON, '-B', 'scripts/test_ci_native_tests.py'],
    'candidate-ci-cleanup-regression-r1': [PYTHON, '-B', ORIGINAL_ROOT + '/tools/check_cleanup_regression.py'],
    'candidate-release-build-r1': ['env', 'PYO3_PYTHON=' + PYTHON, 'cargo', 'build', '--offline', '--locked',
        '--release', '--features', 'release-user-surfaces', '-p', 'shardloom-cli', '-p', 'shardloom-python'],
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
    'docs/architecture/native-rolling-distinct-2026-11-11.md',
    'docs/architecture/local-development-storage.md',
}
ORIGINAL_FAILURES = {'baseline-attribution-r1.json', 'baseline-attribution-r2.json',
    'candidate-attribution-r1.json', 'candidate-ci-verification-r1.json', 'candidate-ci-verification-r2.json',
    'candidate-mapping-focused-r1.json', 'candidate-mapping-focused-r2.json',
    'candidate-mapping-prepared-r1.json', 'candidate-mapping-prepared-r3.json',
    'candidate-mapping-prepared-r4.json', 'candidate-native-clippy-r1.json'}
INDEPENDENT_CI_FIX_PATHS = {'scripts/ci_native_tests.py', 'scripts/test_ci_native_tests.py'}
CONTROL_NAMES = {
    'native_test_failure', 'undrained_native_process', 'mutable_source_validation',
    'mapping_source_changed', 'root_lock_changed', 'required_test_missing_with_rebound_log',
    'ignored_ci_policy_changed', 'actual_ci_inventory_changed', 'runtime_library_mismatch',
    'mapped_payload_count_corrupted', 'compact_payload_retained', 'owner_release_denied',
    'complete_attribution_corrupted', 'candidate_engine_total_corrupted', 'candidate_case_omitted',
    'baseline_reuse_corrupted', 'candidate_no_fallback_denied', 'gain_disposition_corrupted',
    'original_failure_omitted', 'full10k_claim', 'full43_claim', 'composition_identity_changed',
    'composition_reference_changed', 'archive_byte_changed', 'installed_skipped_test',
    'installed_runtime_mismatch', 'installed_publication_claim', 'baseline_launcher_changed',
    'cleanup_regression_denied', 'cleanup_fix_unbound',
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

def read_archive(packet):
    report = json.loads((packet / 'acceptance.json').read_bytes())
    assert report['schema'] == 'shardloom.join_payload_mapping_acceptance.v1'
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
        path = packet / record['path']
        assert identity(path.read_bytes()) == {key: record[key] for key in ('bytes', 'sha256')}
        assert record['bytes'] < 64 << 20
        with tarfile.open(path, 'r:xz') as archive:
            members = archive.getmembers()
            assert [member.name for member in members] == record['members']
            for member in members:
                safe_name(member.name)
                assert member.isfile() and member.name not in data
                wanted = by_name[member.name]
                assert member.size == wanted['bytes'] and member.size <= 24 << 20
                raw = archive.extractfile(member).read()
                assert identity(raw) == {key: wanted[key] for key in ('bytes', 'sha256')}
                data[member.name] = raw
    assert set(data) == set(by_name)
    for row in report['assemblies']:
        assert row['path'] not in data and len(row['parts']) > 1
        raw = b''.join(data.pop(name) for name in row['parts'])
        assert identity(raw) == {key: row[key] for key in ('bytes', 'sha256')}
        data[row['path']] = raw
    assert set(data) == {row['path'] for row in rows}
    for row in rows:
        assert identity(data[row['path']]) == {key: row[key] for key in ('bytes', 'sha256')}
    assert data['current/tools/verify_join_mapping_packet.py'] == Path(__file__).read_bytes()
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
        declared = self.read('current/candidate-protocol-r1.json')['runtime_binding']['proof_files']
        matching = [item for item in declared if item['path'] == row['path']]
        assert len(matching) == 1 and matching[0] == row
        record = external[row['path']]
        size = row.get('bytes', row.get('generation', {}).get('bytes'))
        assert record['identity'] == dict(bytes=size, sha256=row['sha256'])
        path = Path(row['path'])
        if record['role'] == 'accepted_runtime_identity':
            assert path.parent == Path(WINDOW_ROOT) / 'runtime-r1'
            assert path.name in ('shardloom', 'lib_native.dylib', '_native.abi3.so')
        elif record['role'] == 'candidate_installed_wheel_identity':
            assert path.parent == Path(ORIGINAL_ROOT) / 'installed-proof-r1/dist'
            installed = self.read('current/installed-proof-r1/report.json')['integrity']
            assert path.name == installed['wheel']
            assert record['identity'] == dict(bytes=installed['wheel_bytes'], sha256=installed['wheel_sha256'])
        else:
            assert record['role'] == 'historical_committed_archive_identity'
            root = Path('/Users/dylan/.codex/worktrees/native-vector-representations/shardloom-local-repo/docs')
            assert path.is_relative_to(root / 'benchmarks/evidence') or path.is_relative_to(root / 'release/evidence')
            assert path.name.startswith('verification') and path.name.endswith('.tar.xz')

    def guard(self, prefix, command=None, source=None):
        record = self.read(prefix + '.json')
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

def test_names(raw):
    names = re.findall(rb'^test (\S+) \.\.\. ok$', raw, re.M)
    assert names
    assert not re.search(rb'^test .* \.\.\. FAILED$|^test result: FAILED\.', raw, re.M)
    return [name.decode() for name in names]

def replay_sources(packet):
    data = packet.data
    sources = {name.removeprefix('harness/'): raw for name, raw in data.items() if name.startswith('harness/')}
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
    before = packet.read('current/baseline-missing-r1.json')['build_sources']
    after = packet.read('current/candidate-release-build-r1.json')['build_sources']
    for name in REPLAY_SUPPORT:
        assert name not in sources and before[name] == after[name]
        raw = data['source-blobs/' + before[name]['sha256']]
        assert identity(raw) == before[name]
        sources[name] = raw
    for row in frozen['references']:
        packet.bound(row)
    return sources

def verify_controls(packet_path, report, data):
    controls = json.loads((packet_path / 'corruption-controls.json').read_bytes())
    assert controls['status'] == 'passed' and controls['query_executions'] == 0
    assert controls['acceptance_manifest'] == identity((packet_path / 'acceptance.json').read_bytes())
    assert controls['positive_controls'] == 2 and controls['rejected_corruptions'] == len(CONTROL_NAMES)
    assert len(controls['controls']) == len(CONTROL_NAMES) + 2
    assert {row['name'] for row in controls['controls'] if row['outcome'] == 'rejected'} == CONTROL_NAMES
    assert {row['name'] for row in controls['controls'] if row['outcome'] == 'passed'} == {'source_bound', 'relocated_source_only'}
    for name in ('verify_join_mapping_packet.py', 'check_join_mapping_packet_controls.py'):
        assert controls['sources'][name] == identity(data['current/tools/' + name])
    assert controls['archives'] == report['archives']
    expected_logs = {name + suffix for name in CONTROL_NAMES | {'source-bound', 'relocated'}
        for suffix in ('.stdout.log', '.stderr.log')}
    assert {row['path'] for row in controls['observations']} == {
        'control-observations/' + name for name in expected_logs}
    assert len(controls['observations']) == len(expected_logs)
    for row in controls['observations']:
        safe_name(row['path'])
        assert identity((packet_path / row['path']).read_bytes()) == {
            key: row[key] for key in ('bytes', 'sha256')}
    return dict(positive_controls=2, rejected_corruptions=len(CONTROL_NAMES))

def verify(packet_path, repo=None, require_controls=False):
    if not __debug__:
        raise RuntimeError('evidence verification requires assertion checks')
    report, data = read_archive(packet_path)
    packet = verify_sources(report, data, repo)
    diagnostics = verify_diagnostics(packet)
    with tempfile.TemporaryDirectory(prefix='join-mapping-evidence-') as temporary:
        replay = replay_saved(report, data, Path(temporary))
    controls = verify_controls(packet_path, report, data) if require_controls else None
    return dict(status='passed_source_bound_replay', diagnostics=diagnostics, controls=controls, **replay)


def functions(raw, names, namespace):
    nodes = [node for node in ast.parse(raw).body
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names]
    assert {node.name for node in nodes} == names and len(nodes) == len(names)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), 'archived-proof-definitions', 'exec'), namespace)
    return namespace


def declaration(raw, name):
    nodes = [node for node in ast.parse(raw).body if isinstance(node, ast.Assign)
        and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id == name]
    assert len(nodes) == 1
    return ast.literal_eval(nodes[0].value)


def verify_ci(packet, source):
    policy = json.loads(packet.data['source-blobs/' + source[CI_POLICY_PATH]['sha256']])
    baseline = packet.read('current/baseline-missing-r1.json')['build_sources']
    old = json.loads(packet.data['source-blobs/' + baseline[CI_POLICY_PATH]['sha256']])
    assert len(old['ignored_tests']) == 35 and CI_DIAGNOSTIC not in old['ignored_tests']
    assert policy == {**old, 'ignored_tests': sorted([*old['ignored_tests'], CI_DIAGNOSTIC])}
    record = packet.read('current/candidate-ci-inventory-r1.json')
    assert record['schema'] == 'shardloom.join_mapping_ci_inventory.v1'
    assert record['status'] == 'passed_actual_native_inventory' and record['exact_ignored_ownership']
    assert record['query_executions'] == 0 and record['source_fingerprint_sha256'] == fingerprint(source)
    assert record['ignored'] == policy['ignored_tests'] and len(record['ignored']) == 36
    assert record['discovery_features'] == ['release-user-surfaces']
    assert record['discovery_profile'] == 'debug' and record['hosted_ci_profile'] == 'ci-native'
    assert record['both_profiles_have_debug_assertions']
    partitions = record['partitions']; names = [name for part in partitions for name in part['tests']]
    assert len(partitions) == 4 and len(names) == len(set(names)) == record['test_count']
    assert record['test_count'] == 4151
    assert names.count(CI_DIAGNOSTIC) == 1 and record['attribution_test'] == CI_DIAGNOSTIC
    for name, expected in record['files'].items():
        assert identity(packet.data[packet.report['locations'][name]]) == expected
    packet.guard('current/candidate-ci-verification-r3', [PYTHON, '-B',
        ORIGINAL_ROOT + '/tools/verify_join_mapping_ci_inventory.py', '--build',
        ORIGINAL_ROOT + '/candidate-ci-build-r2.json', '--output',
        ORIGINAL_ROOT + '/candidate-ci-inventory-r1.json'], source)
    packet.guard('current/candidate-ci-build-r2', ['cargo', 'test', '--offline', '--locked',
        '-p', 'shardloom-vortex', '--lib', '--features', 'release-user-surfaces', '--no-run'], source)
    assert re.search(rb'Ran 20 tests in [\d.]+s\n\nOK\n', packet.data['current/candidate-ci-verification-r3.log'])
    required = declaration(packet.data['current/tools/verify_join_mapping_diagnostics.py'], 'REQUIRED_TESTS')
    assert len(required) == len(set(required)) == 17
    assert all(sum(value.endswith('::' + name) for value in names) == 1 for name in required)
    return dict(tests=len(names), ignored=len(record['ignored']), partitions=len(partitions))


def verify_diagnostics(packet):
    data = packet.data
    current = packet.read('current/candidate-diagnostics-r1.json')
    old_raw = data['current/baseline-attribution-observations-r1.json']
    assert digest(old_raw) == 'd3ad390c2e99560f76aa717611480a42e1ae56af5a310224fb96baa9d68e54a1'
    old = json.loads(old_raw)
    assert current['schema'] == 'shardloom.join_mapping_candidate_diagnostics.v1'
    assert current['status'] == 'passed_candidate_diagnostics'
    assert current['diagnostic_source_fingerprint_sha256'] == SOURCE_FINGERPRINT
    assert current['native_query_executions_during_verification'] == 0
    assert not current['engine_timing_claimed'] and not current['candidate_acceptance_claimed']
    assert current['whole_process_memory_claim'] is False and current['ignored_ci_registered']
    for name, expected in current['files'].items():
        assert identity(data[packet.report['locations'][name]]) == expected
    namespace = functions(data['current/tools/verify_join_mapping_attribution.py'],
        {'expected_cases', 'reference', 'validate_observations'}, {})
    namespace['validate_observations'](old['observations'])
    comparator = functions(data['current/tools/verify_join_mapping_diagnostics.py'],
        {'validate_attribution'}, dict(baseline=types.SimpleNamespace(**namespace)))['validate_attribution']
    comparator(current['observations'], old)
    prefix = b'JOIN_RANKING_ATTRIBUTION '
    for stem, expected in (('baseline-attribution-r3', old['observations']),
            ('candidate-attribution-r4', current['observations'])):
        actual = [json.loads(line[len(prefix):]) for line in data['current/' + stem + '.log'].splitlines()
            if line.startswith(prefix)]
        assert actual == expected
        names = test_names(data['current/' + stem + '.log'])
        assert len(names) == 2 and {name.rsplit('::', 1)[-1] for name in names} == {
            'successive_join_ranked_payloads_release_sources_and_match_independent_rows',
            'successive_join_ranked_payload_work_attribution_matrix'}
        assert rust_counts(data['current/' + stem + '.log']) == dict(passed=2, failed=0, ignored=0)
    assert old['status'] == 'passed_baseline_attribution' and old['internal_native_fixtures'] == 102
    assert old['accepted_sources_restored'] and not old['candidate_runtime_enabled']
    assert old['complete_results_verified'] and old['complete_schema_verified']
    assert old['materialized_payload_cells'] == 1214090
    assert old['materialized_payload_buffer_bytes'] == 104059054
    for row in old['proof'].values():
        packet.bound(row)
    attribution = current['attribution']
    assert attribution['internal_native_fixtures'] == 102 and attribution['matrix_cases'] == 96
    assert attribution['ordinary_regression_cases'] == 6
    assert attribution['complete_results_verified'] and attribution['complete_schema_verified']
    assert attribution['all_owner_checks_passed']
    assert attribution['baseline_materialized_payload_cells'] == old['materialized_payload_cells']
    assert attribution['baseline_materialized_payload_buffer_bytes'] == old['materialized_payload_buffer_bytes']
    assert attribution['candidate_mapped_payload_cells'] == sum(stage['mapped_payload_cells']
        for row in current['observations'] for stage in row['stages']) == 1214090
    assert attribution['candidate_materialized_payload_cells'] == attribution['candidate_materialized_payload_buffer_bytes'] == 0
    assert packet.report['attribution'] == attribution
    required = declaration(data['current/tools/verify_join_mapping_diagnostics.py'], 'REQUIRED_TESTS')
    assert current['required_tests'] == required and len(required) == len(set(required)) == 17
    names = test_names(data['current/candidate-native-tests-r2.log'])
    assert all(sum(value.endswith('::' + name) for value in names) == 1 for name in required)
    assert len(current['rejected_corruptions']) == len(set(current['rejected_corruptions'])) == 17
    checks = dict(format='candidate-format-r2', default_clippy='candidate-default-clippy-r2',
        default_tests='candidate-default-tests-r2', native_clippy='candidate-native-clippy-r3',
        native_tests='candidate-native-tests-r2', attribution='candidate-attribution-r4',
        ci_cleanup_tests='candidate-ci-cleanup-tests-r1', ci_cleanup_regression='candidate-ci-cleanup-regression-r1')
    assert current['checks'] == {key: ORIGINAL_ROOT + '/' + value + '.json' for key, value in checks.items()}
    cleanup = packet.read('current/ci-cleanup-regression-r1.json')
    assert cleanup['status'] == 'passed_regression_control' and cleanup['original_runner_rejects_regression']
    assert cleanup['expected_test_errors'] == 1 and cleanup['native_query_executions'] == 0
    assert cleanup['original_runner'] == packet.read('current/baseline-missing-r1.json')['build_sources']['scripts/ci_native_tests.py']
    assert cleanup['regression_tests'] == packet.report['candidate_source_snapshot']['scripts/test_ci_native_tests.py']
    assert re.search(rb'Ran 20 tests in [\d.]+s\n\nOK\n', data['current/candidate-ci-cleanup-tests-r1.log'])
    for name, expected in current['original_failures_preserved'].items():
        assert identity(data['current/' + name]) == expected['receipt']
        assert identity(data['current/' + name.replace('.json', '.log')]) == expected['log']
    packet.guard('current/candidate-diagnostics-verification-r1', [PYTHON, '-B',
        ORIGINAL_ROOT + '/tools/verify_join_mapping_diagnostics.py', '--output',
        ORIGINAL_ROOT + '/candidate-diagnostics-r1.json'], packet.report['candidate_source_snapshot'])
    baseline = packet.guard('current/baseline-attribution-r3')
    assert not baseline['query_workload']
    return dict(internal_native_fixtures=102, matrix_cases=96, required_tests=17,
        diagnostic_corruptions_rejected=17, whole_query_memory_claim=False, engine_timing_claimed=False)


def verify_installed(packet, source, runtime):
    root = ORIGINAL_ROOT + '/installed-proof-r1'
    python = ORIGINAL_ROOT + '/package-tools-r1/bin/python'
    requirements = ROOT + '/.github/requirements'
    commands = {
        'candidate-package-environment-r1': [PYTHON, '-m', 'venv', ORIGINAL_ROOT + '/package-tools-r1'],
        'candidate-package-tools-r1': [python, '-I', '-m', 'pip', 'install', '--only-binary=:all:',
            '--require-hashes', '-r', requirements + '/ci-native-python-build.txt'],
        'candidate-wheelhouse-r1': [python, '-I', '-m', 'pip', 'download', '--only-binary=:all:',
            '--require-hashes', '-r', requirements + '/ci-native-python.txt', '--dest', ORIGINAL_ROOT + '/wheelhouse-r1'],
        'candidate-installed-r1': [python, '-B', ROOT + '/scripts/prove_native_python_wheel.py',
            '--repo-root', ROOT, '--output-dir', root, '--native-library', ORIGINAL_ROOT + '/runtime-r1/_native.abi3.so',
            '--cli-binary', ORIGINAL_ROOT + '/runtime-r1/shardloom', '--wheelhouse', ORIGINAL_ROOT + '/wheelhouse-r1'],
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
    assert integration['status'] == 'passed' and type(integration['tests']) is int and integration['tests'] == 441
    assert integration['failures'] == integration['errors'] == integration['skipped'] == 0
    integration_log = packet.data['current/installed-proof-r1/integration.log']
    assert re.search(rb'Ran 441 tests in [\d.]+s\n\nOK\n', integration_log)
    assert not re.search(rb'^FAILED |^OK \(skipped=', integration_log, re.M)
    loaded = report['installed_load']
    assert loaded == packet.read('current/installed-proof-r1/load.log') and loaded['isolated_install']
    assert all(Path(loaded[key]).is_relative_to(Path(root) / 'venv') for key in ('package', 'native_library', 'cli'))
    assert [step['name'] for step in report['steps']] == ['build', 'create_environment', 'install_optional_consumer',
        'install', 'load', 'integration', 'uninstall', 'verify_uninstalled']
    for step in report['steps']:
        assert step['returncode'] == 0
        assert step['timeout_seconds'] == (1200 if step['name'] == 'integration' else 600)
        assert step['log'] == step['name'] + '.log'
        assert digest(packet.data['current/installed-proof-r1/' + step['log']]) == step['log_sha256']
    return dict(integration_tests=integration['tests'], skipped=0, isolated_install=True, uninstall_status='passed')


def integration_sources(packet):
    source = packet.report['candidate_source_snapshot']
    baseline = packet.read('current/baseline-missing-r1.json')['build_sources']
    correction = packet.read('current/ci-cleanup-correction-r1.json')
    assert correction['status'] == 'verified_ci_evidence_fix_before_candidate_queries'
    assert correction['benchmark_queries_executed'] == 0 and correction['retain_independently_of_mapping_decision']
    assert correction['native_runtime_changed_by_fix'] is False and correction['benchmark_supervisor_changed'] is False
    assert set(correction['source_changes']) == INDEPENDENT_CI_FIX_PATHS
    assert correction['candidate_source_fingerprint_sha256'] == SOURCE_FINGERPRINT
    for name, change in correction['source_changes'].items():
        assert change == dict(before=baseline[name], after=source[name])
    assert correction['regression_control'] == identity(packet.data['current/ci-cleanup-regression-r1.json'])
    target = dict(source if packet.report['disposition'] == 'retain_candidate' else baseline)
    for name in INDEPENDENT_CI_FIX_PATHS:
        target[name] = source[name]
    assert packet.report['independent_ci_cleanup_fix'] == correction['source_changes']
    review = packet.read('current/pre-score-source-review-r1.json')
    assert review['schema'] == 'shardloom.join_mapping_source_review.v1'
    assert review['status'] == 'completed_pre_score_source_review'
    assert review['source_head'] == BASE and review['source_fingerprint_sha256'] == SOURCE_FINGERPRINT
    assert review['reviewed_changed_files'] == {name:item for name,item in source.items() if baseline.get(name) != item}
    assert review['runtime_objections_reviewed'] == ['eligibility_and_alias_semantics',
        'validation_and_publication_order', 'owner_and_reservation_lifetime']
    assert review['ci_cleanup_reviewed'] and review['unresolved_in_scope_findings'] == []
    assert review['benchmark_queries_executed'] == 0 and review['candidate_protocol_absent_at_review']
    assert review['review_document'] == identity(packet.data['current/adversarial-review-r1.md'])
    assert review['independent_ci_cleanup_correction'] == identity(packet.data['current/ci-cleanup-correction-r1.json'])
    return target


def verify_sources(report, data, repo=None):
    packet = Packet(report, data)
    assert digest(data['current/baseline-protocol-r1.json']) == BASELINE_PROTOCOL_SHA256
    assert digest(data['current/baseline-verified-r1.json']) == BASELINE_RESULT_SHA256
    build = packet.read('current/candidate-release-build-r1.json'); source = build['build_sources']
    assert report['source_base'] == BASE and report['candidate_source_snapshot'] == source
    assert report['candidate_source_fingerprint_sha256'] == fingerprint(source) == SOURCE_FINGERPRINT
    for name, command in CHECKS.items():
        record = packet.guard('current/' + name, command, source)
        assert record['base'] == record['head'] == BASE and not record['query_workload']
        assert report['checks'][name] == dict(command=command, status='passed',
            rust_tests=rust_counts(data['current/' + name + '.log']))
    assert rust_counts(data['current/candidate-default-tests-r2.log']) == dict(passed=3323, failed=0, ignored=0)
    assert rust_counts(data['current/candidate-native-tests-r2.log']) == dict(passed=7108, failed=0, ignored=40)
    assert rust_counts(data['current/candidate-attribution-r4.log']) == dict(passed=2, failed=0, ignored=0)
    assert report['ci_inventory'] == verify_ci(packet, source)
    baseline = packet.guard('current/baseline-missing-r1')
    assert baseline['base'] == baseline['head'] == BASE
    assert all(line[3:].startswith('docs/') for line in baseline['working_tree'].splitlines())
    closeout_raw = data['distinct/rolling-distinct-closeout-r1.json']
    assert digest(closeout_raw) == '47fc3ef54aeb45222f6e15be47a4deb0ce4bd81108de9157655b3b9da08a5b6b'
    closeout = json.loads(closeout_raw)
    assert closeout['status'] == 'complete' and closeout['merge_commit'] == BASE and closeout['source_tree'] == BASE_TREE
    assert closeout['pr_checks_passed'] == 58 and closeout['main_checks_passed'] == 55
    assert closeout['runtime_sources_match_build'] and closeout['source_bound_replay'] == 'passed'
    assert closeout['portable_replay_after_retirement'] == 'passed' and closeout['current_inputs_unchanged']
    assert closeout['disposition'] == 'drop_candidate_no_gain' and not closeout['review_findings']
    window_raw = data['window/shared-window-closeout-r1.json']
    assert digest(window_raw) == '97b80163c9c91d1186acc86522e808fa8563c8d9dd88be9a4ff1b71e316768a0'
    window = json.loads(window_raw)
    assert closeout['retained_runtime'] == window['retained_runtime']
    assert closeout['source_fingerprint_sha256'] == window['source_fingerprint_sha256']
    previous = packet.read('window/candidate-release-build-r1.json')
    assert previous['build_source_fingerprint_sha256'] == closeout['source_fingerprint_sha256']
    old = previous['build_sources']
    assert fingerprint(old) == previous['build_source_fingerprint_sha256']
    changed = {name for name in set(old) | set(baseline['build_sources'])
        if old.get(name) != baseline['build_sources'].get(name)}
    assert all(name.startswith('docs/') for name in changed)
    assert report['disposition'] in {'retain_candidate', 'drop_candidate_no_gain', 'drop_candidate_invalid_cohort'}
    target = integration_sources(packet)
    for name, command in DOC_CHECKS.items():
        record = packet.guard('current/' + name, command)
        assert record['base'] == record['head'] == BASE
        late = record['build_sources']
        changed = {key for key in set(late) | set(target) if late.get(key) != target.get(key)}
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
    files = {Path(row['path']).name: row for row in runtime['files']}
    assert set(files) == {'shardloom', 'lib_native.dylib', '_native.abi3.so'}
    assert all(Path(row['path']).parent == Path(ORIGINAL_ROOT) / 'runtime-r1' for row in files.values())
    assert {key: files['lib_native.dylib'][key] for key in ('bytes', 'sha256')} == {
        key: files['_native.abi3.so'][key] for key in ('bytes', 'sha256')}
    packet.guard('current/candidate-freeze-runtime-r1', [PYTHON, '-B',
        ORIGINAL_ROOT + '/tools/freeze_runtime.py', '--build', 'candidate-release-build-r1.json',
        '--name', 'runtime-r1', '--profile', 'release'], source)
    packet.guard('current/candidate-plan-r1', [PYTHON,
        ORIGINAL_ROOT + '/tools/run_join_mapping_candidate.py', '--plan'], source)
    protocol = packet.read('current/candidate-protocol-r1.json'); binding = protocol['runtime_binding']
    assert binding['source_head'] == binding['accepted_base'] == BASE
    assert binding['source_fingerprint_sha256'] == SOURCE_FINGERPRINT
    for role, name in (('binary', 'shardloom'), ('library', '_native.abi3.so')):
        row = binding['runtime'][role]
        assert dict(bytes=row['generation']['bytes'], sha256=row['sha256']) == {
            key: files[name][key] for key in ('bytes', 'sha256')}
    for row in binding['proof_files']:
        packet.proof(row)
    assert report['installed_package'] == verify_installed(packet, source, files)
    clarification = data['current/baseline-replay-command-clarification-r1.json']
    assert digest(clarification) == CLARIFICATION_SHA256
    clarified = json.loads(clarification)
    assert clarified['discovered_before_replay'] and clarified['candidate_requires_same_interpreter_path']
    assert clarified['observed_parent_command'] == [PYTHON, ORIGINAL_ROOT + '/tools/run_join_mapping_baseline.py']
    assert clarified['comparison_command_prefix'] == [PYTHON]
    assert clarified['queries_repeated'] == 0 and not clarified['native_runtime_changed'] and not clarified['query_results_changed']
    assert len(clarified['interpreters']) == 2
    assert clarified['interpreters'][0]['content'] == clarified['interpreters'][1]['content']
    assert {key:binding['parent_interpreter'][key] for key in ('path', 'sha256')} == dict(
        path=clarified['interpreters'][1]['resolved'], sha256=clarified['interpreters'][1]['content']['sha256'])
    assert packet.bound(binding['baseline_replay_command_clarification']) == clarification
    packet.bound(clarified['original_verifier']); packet.bound(clarified['baseline_guard'])
    packet.guard('current/baseline-replay-r2', [PYTHON, ORIGINAL_ROOT + '/tools/replay_join_mapping_baseline_r2.py'])
    for field in ('complete_10k_performance_claim', 'full43_claim', 'whole_item_19_complete',
            'whole_item_20_complete', 'publication_attempted', 'whole_query_memory_claim'):
        assert report[field] is False
    dispositions = packet.read('current/candidate-failure-dispositions-r1.json'); failures = []
    for name, raw in sorted(data.items()):
        if name.startswith('current/') and name.count('/') == 1 and name.endswith('.json'):
            record = json.loads(raw)
            if isinstance(record, dict) and (str(record.get('status')).startswith('failed') or record.get('status') == 'interrupted'):
                key = name.removeprefix('current/'); assert dispositions[key]
                failures.append(dict(receipt=name, identity=identity(raw), disposition=dispositions[key]))
    assert report['original_failed_checks'] == failures
    assert ORIGINAL_FAILURES <= set(dispositions) == {row['receipt'].removeprefix('current/') for row in failures}
    original = packet.read('preparation/current-baseline-10k-r1/failed-observation-replay-r1.json')
    assert original['status'] == 'verified_failed_observation' and original['verification_passed']
    assert original['observed_attempts'] == 10000 and original['verified_correct_results'] == 9904
    assert original['original_outcomes'] == {'passed': 9904, 'crashed': 76, 'harness_error': 20}
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


def replay_saved(report, data, temporary):
    temporary = temporary.resolve()
    packet = Packet(report, data)
    old_protocol = packet.read('current/baseline-protocol-r1.json')
    old_result = packet.read('current/baseline-verified-r1.json')
    assert digest(data['current/baseline-protocol-r1.json']) == BASELINE_PROTOCOL_SHA256
    assert digest(data['current/baseline-verified-r1.json']) == BASELINE_RESULT_SHA256
    protocol = packet.read('current/candidate-protocol-r1.json')
    result = packet.read('current/candidate-verified-r1.json')
    selection = json.loads(packet.bound(old_protocol['selection']))
    assert old_result['selected_cases'] == result['selected_cases'] == 940
    assert old_result['reused_saved_cases'] == 0 and old_result['new_attempts'] == 940
    assert result['new_attempts'] == 940 and result['query_executions_during_replay'] == 0
    assert protocol['cases'] == old_protocol['missing_cases'] == selection['cases']
    assert len(selection['cases']) == protocol['selected_count'] == old_protocol['selected_count'] == 940
    assert protocol['original_controls'] == old_protocol['original_controls'] == 580
    assert protocol['new_compositions'] == old_protocol['new_compositions'] == 360
    assert protocol['candidate_contract'] == old_protocol['candidate_contract']
    assert protocol['selection_uses_timing'] is False
    for record in (old_protocol, protocol, old_result, result):
        assert record['primary_clock'] == 'engine_execution_seconds'
        assert record['attempts_per_case'] == 1 and record['warmups'] == record['requested_repeats'] == 0
        assert record['complete_10k_performance_claim'] is False and record['full43_claim'] is False
    assert packet.bound(protocol['baseline_protocol']) == data['current/baseline-protocol-r1.json']
    assert packet.bound(protocol['baseline_result']) == data['current/baseline-verified-r1.json']
    assert protocol['selection'] == old_protocol['selection'] == result['selection'] == old_result['selection']
    candidate_proof = {row['path']: row for row in protocol['runtime_binding']['proof_files']}
    assert len(candidate_proof) == len(protocol['runtime_binding']['proof_files'])
    assert old_result['runtime_binding'] == old_protocol['runtime_binding']
    for row in old_protocol['runtime_binding']['proof_files']:
        assert candidate_proof[row['path']] == row
    assert set(report['identity_only_proof']) <= set(candidate_proof)
    for key in ('isolation', 'performance_decision'):
        assert protocol[key] == old_protocol[key]
    assert protocol['baseline_observations'] == dict(reused=0, newly_measured=940, result=protocol['baseline_result'])
    design, inputs = (packet.read('frozen/' + name + '.json') for name in ('design', 'inputs'))
    prepared = packet.read('frozen/native-preparation/complete.json')
    assert len(design['cases']) == 10000 and old_protocol['execution'] == protocol['execution'] == design['execution']
    for name in ('design', 'inputs', 'frozen'):
        assert packet.bound(protocol[name]) == packet.bound(old_protocol[name]) == data['frozen/' + name + '.json']
    assert Counter(row['kind'] for row in selection['cases']) == {'original': 580, 'composition': 360}
    assert len({row['case']['id'] for row in selection['cases']}) == 940
    assert len({row['case']['case_identity'] for row in selection['cases']}) == 940
    for selected in selection['cases']:
        if selected['kind'] == 'original':
            assert selected['case'] == design['cases'][selected['original_seq']]
        else:
            assert selected['original_seq'] is None and 'join_mapping_version' in selected['case']
    source_bytes = replay_sources(packet)
    harness = temporary / 'expanded-query-2026-11-03/investigation-original-harness-r1'
    written = {}
    def write_source(path, raw):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        written[path.resolve()] = raw
    for name, raw in source_bytes.items():
        write_source(harness / name, raw)
    preparation = temporary / 'shared-operator-state-2026-11-10'
    for name in ('window-tools-draft/window_replay.py', 'window-client-draft/shared_window_worker.py',
            'window-client-draft/shared_window_cases.py'):
        row = candidate_proof[PREPARATION_ROOT + '/' + name]
        write_source(preparation / name, packet.bound(row))
    for name, raw in data.items():
        if name.endswith('.sqlite'):
            safe_name(name)
            path = temporary / 'databases' / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
    current = temporary / 'join-mapping-2026-11-11'
    for name in ('client/join_mapping_worker.py', 'client/join_mapping_cases.py'):
        write_source(current / name, packet.bound(candidate_proof[ORIGINAL_ROOT + '/' + name]))
    sys.path.insert(0, str(preparation / 'window-tools-draft'))
    import window_replay as old_replay
    sys.path.insert(0, str(current / 'client'))
    import join_mapping_worker as join_worker
    from query_surface import adapters
    from query_surface.semantics import ResultContract
    replay_module = types.ModuleType('archived_join_mapping_replay')
    replay_module.__dict__.update(old_proof=old_replay, worker=join_worker,
        SimpleNamespace=types.SimpleNamespace, json=json, ResultContract=ResultContract, adapters=adapters)
    functions(data['current/tools/join_mapping_common.py'], {'Cases', 'replay_case'}, replay_module.__dict__)
    from query_surface.expanded_freeze import unpack
    from query_surface.expanded_report import raw_files
    from query_surface.expanded_run import request
    assert not any(name == 'shardloom._native' or name.startswith('shardloom._native.') for name in sys.modules)
    for name, module in list(sys.modules.items()):
        if name.startswith(('query_surface', 'shardloom', 'shared_window', 'join_mapping')) or name in (
                'window_replay', 'query_timing_report', 'native_workflow_protocol', 'local_uat_storage',
                'resources', 'run_clickbench_query_uat', 'timed_native_command',
                'clickbench_reference_packet', 'clickbench_native_sql'):
            path = Path(module.__file__).resolve()
            assert path in written and path.read_bytes() == written[path], name
    cases = replay_module.Cases()
    baseline_rows = [json.loads(line) for line in gzip.decompress(packet.bound(old_result['case_rows'])).splitlines()]
    candidate_rows = [json.loads(line) for line in gzip.decompress(packet.bound(result['case_rows'])).splitlines()]
    assert len(baseline_rows) == len(candidate_rows) == 940
    reused = json.loads(packet.bound(old_protocol['reuse']))
    assert reused['schema'] == 'shardloom.join_mapping_reuse.v1'
    assert reused['status'] == 'verified_no_matching_saved_cases'
    assert reused['selected_reused_cases'] == old_protocol['reused_count'] == 0
    assert reused['complete_requests_verified'] == len(reused['requests']) == 940
    assert reused['eligible_runtime_case_pool'] == 1648 and len(reused['inspected_pools']) == 2
    assert reused['query_executions'] == 0 and not reused['original_ledgers_recreated']
    assert reused['original_observations_preserved']
    assert reused['runtime'] == old_protocol['runtime_binding']['runtime']
    assert reused['selection'] == old_protocol['selection']
    pools = {}
    for row, count, key in zip(reused['inspected_pools'], (1212, 436), ('cases', 'missing_cases'), strict=True):
        prior = json.loads(packet.bound(row['protocol']))
        accepted = json.loads(packet.bound(row['packet_acceptance']))
        member = next(item for item in accepted['members'] if item['path'] == 'current/' + Path(row['protocol']['path']).name)
        assert identity(packet.bound(row['protocol'])) == {key:member[key] for key in ('bytes', 'sha256')}
        assert prior['runtime_binding']['runtime'] == reused['runtime']
        assert row['cases'] == len(prior[key]) == count and row['selected_key'] == key
        for item in prior[key]:
            case = item['case']; assert case['id'] not in pools
            pools[case['id']] = case['case_identity']
    assert len(pools) == 1648
    assert not pools.keys() & {row['case']['id'] for row in selection['cases']}
    assert not set(pools.values()) & {row['case']['case_identity'] for row in selection['cases']}
    captures = {}
    for role, prefix in (('baseline', 'current/baseline-missing-r1'), ('candidate', 'current/candidate-r1')):
        done, binding = packet.read(prefix + '/complete.json'), packet.read(prefix + '/binding.json')
        assert done['status'] == 'complete_observation' and done['stage'] == 'native'
        assert done['total'] == done['attempt_slots'] == 940
        assert done['posthash_verified'] and done['attempts_per_case'] == 1 and done['requested_repeats'] == 0
        assert binding['contract'] == {key: done[key] for key in ('frozen', 'stage', 'prerequisites', 'updated')}
        assert packet.bound(done['ledger']) == data[prefix + '/attempts.sqlite']
        assert packet.bound(done['binding']) == data[prefix + '/binding.json']
        packet.bound(done['frozen'])
        guard = packet.guard(prefix)
        assert guard['query_workload'] and guard['build_inventory_policy'] == guard['evidence_inventory_policy'] == 'before_and_after_only'
        assert guard['base'] == guard['head'] == BASE
        assert guard['active_output'] == ORIGINAL_ROOT + '/' + prefix.removeprefix('current/')
        script = 'run_join_mapping_candidate.py' if role == 'candidate' else 'run_join_mapping_baseline.py'
        assert guard['command'] == [PYTHON, ORIGINAL_ROOT + '/tools/' + script]
        if role == 'candidate':
            assert guard['build_sources'] == report['candidate_source_snapshot']
        else:
            assert all(line[3:].startswith('docs/') for line in guard['working_tree'].splitlines())
        captures[role] = dict(prefix=prefix, done=done, runtime=done['updated']['runtime'])
    assert captures['baseline']['done']['updated'] == old_protocol['runtime_binding']
    assert captures['candidate']['done']['updated'] == protocol['runtime_binding'] == result['runtime_binding']
    outcomes, families, totals = {}, {}, {}
    with ExitStack() as stack:
        def database(name):
            connection = sqlite3.connect('file:' + str(temporary / 'databases' / name) + '?mode=ro', uri=True)
            connection.execute('PRAGMA query_only=ON')
            assert connection.execute('PRAGMA integrity_check').fetchone() == ('ok',)
            stack.callback(connection.close)
            return connection
        refs = {profile: database('frozen/reference-' + profile + '/expectations.sqlite')
            for profile in {row['case']['profile'] for row in selection['cases']}}
        refs['composition'] = database('current/references-r1/expectations.sqlite')
        dbs = {role: database(row['prefix'] + '/attempts.sqlite') for role, row in captures.items()}
        for connection in dbs.values():
            assert connection.execute('SELECT count(*),min(seq),max(seq),count(distinct id) FROM attempt').fetchone() == (940, 0, 939, 940)
        for role, archived_rows in (('baseline', baseline_rows), ('candidate', candidate_rows)):
            counts, family_values, seconds = Counter(), defaultdict(list), []
            capture = captures[role]
            for ordinal, (selected, archived) in enumerate(zip(selection['cases'], archived_rows, strict=True)):
                case = selected['case']
                assert archived['cohort_ordinal'] == selected['cohort_ordinal'] == ordinal
                assert archived['original_seq'] == selected['original_seq'] and archived['kind'] == selected['kind']
                for key in ('id', 'case_identity', 'family', 'profile', 'surface', 'native_format'):
                    assert archived[key] == case[key]
                entry = dbs[role].execute('SELECT seq,id,status,packet FROM attempt WHERE seq=?', (ordinal,)).fetchone()
                assert entry is not None and entry[:2] == (ordinal, case['id'])
                item = cases.item(case)
                reference = refs['composition' if selected['kind'] == 'composition' else case['profile']].execute(
                    'SELECT identity,value FROM expected WHERE id=?', (case['id'],)).fetchone()
                assert reference and reference[0] == item.identity == case['case_identity']
                expected = unpack(reference[1])
                config = dict(inputs, runtime=capture['runtime'])
                observed = replay_module.replay_case(entry, case, expected, item, config, prepared, design['execution'])
                assert observed == archived['result'] and observed['status'] == entry[2]
                files = raw_files(unpack(entry[3])) if entry[3] is not None else {}
                if files:
                    assert json.loads(files['one.job.json']) == request(case, 'native', config, prepared, expected, design['execution'])
                provenance = archived['provenance']
                assert provenance['ledger'] == capture['done']['ledger']['path'] and provenance['source_seq'] == ordinal
                assert provenance['packet'] == (identity(entry[3]) if entry[3] is not None else None)
                assert provenance['job_sha256'] == (digest(files['one.job.json']) if files else None)
                assert provenance['independent_expected_blob_sha256'] == digest(reference[1])
                if role == 'baseline':
                    record = reused['requests'][ordinal]
                    assert record['cohort_ordinal'] == ordinal and record['id'] == case['id']
                    assert record['case_identity'] == case['case_identity']
                    assert record['reference_blob_sha256'] == digest(reference[1])
                    assert record['request_sha256'] == fingerprint(json.loads(files['one.job.json']))
                else:
                    assert archived['baseline_engine_seconds'] == baseline_rows[ordinal]['result']['engine_seconds']
                counts[observed['status']] += 1
                if observed['status'] != 'passed':
                    assert role == 'candidate'
                    continue
                value = observed['engine_seconds']
                assert type(value) in (int, float) and math.isfinite(value) and value >= 0
                seconds.append(value)
                family_values[case['family']].append(value)
            outcomes[role], families[role] = dict(counts), family_values
            totals[role] = math.fsum(seconds) if counts == {'passed': 940} else None
    assert outcomes['baseline'] == old_result['outcomes'] == captures['baseline']['done']['outcomes'] == {'passed': 940}
    assert totals['baseline'] == old_result['engine_execution_seconds'] == 653.057359901
    assert outcomes['candidate'] == result['outcomes'] == captures['candidate']['done']['outcomes']
    assert totals['candidate'] == result['engine_execution_seconds']
    passed = outcomes['candidate'] == {'passed': 940}
    disposition = ('retain_candidate' if totals['candidate'] < totals['baseline'] else 'drop_candidate_no_gain') if passed else 'drop_candidate_invalid_cohort'
    assert report['disposition'] == result['frozen_rule_disposition'] == disposition
    assert result['complete_performance_result_valid'] == result['all_selected_results_correct'] == result['all_engine_clocks_valid'] == result['native_no_fallback'] == passed
    assert result['baseline_engine_execution_seconds'] == totals['baseline']
    assert result['engine_seconds_saved'] == (totals['baseline'] - totals['candidate'] if passed else None)
    assert result['observed_engine_reduction_percent'] == (100 * (totals['baseline'] - totals['candidate']) / totals['baseline'] if passed else None)
    assert report['baseline'] == dict(cases=940, reused_saved_cases=0, new_attempts=940, engine_execution_seconds=totals['baseline'])
    assert report['candidate'] == {key: result[key] for key in ('selected_cases', 'outcomes', 'engine_execution_seconds',
        'engine_seconds_saved', 'observed_engine_reduction_percent', 'complete_performance_result_valid')}
    for name, values in families['baseline'].items():
        assert old_result['families'][name] == dict(cases=len(values), engine_execution_seconds=math.fsum(values))
    for name, values in families['candidate'].items():
        assert result['families'][name] == dict(cases=len(values), engine_execution_seconds=math.fsum(values),
            baseline_engine_execution_seconds=old_result['families'][name]['engine_execution_seconds'])
    return dict(baseline_cases=940, reused_baseline_cases=0, new_baseline_cases=940,
        candidate_cases=940, outcomes=outcomes, engine_execution_seconds=totals,
        disposition=disposition, query_executions=0, complete_10k_performance_claim=False, full43_claim=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo-root', type=Path)
    parser.add_argument('--require-controls', action='store_true')
    args = parser.parse_args()
    print(json.dumps(verify(Path(__file__).resolve().parent, args.repo_root, args.require_controls), indent=2, sort_keys=True))
