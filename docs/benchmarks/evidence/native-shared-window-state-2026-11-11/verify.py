"""Replay the fixed compatible-window experiment from archived source and result bytes.

This verifier never starts a query, native library, compiler or reference engine.
The original frozen comparator rechecks complete saved answers and engine clocks.
"""
from __future__ import annotations

import argparse
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

BASE = 'a640475ae66bc36cefcae42088dc23da6776e8ed'
BASE_TREE = 'c239675c12a25c04aa07a6dc45b8854d6b4718d2'
ORIGINAL_ROOT = '/Users/dylan/LocalData/shardloom/shared-window-state-2026-11-11'
INDEX_ROOT = '/Users/dylan/LocalData/shardloom/shared-index-metadata-2026-11-10'
PREPARATION_ROOT = '/Users/dylan/LocalData/shardloom/shared-operator-state-2026-11-10'
SOURCE_FINGERPRINT = '51e9a408564dd45e5507782727348da0204a05142a6cc1d28647abb83ea9285a'
BASELINE_PROTOCOL_SHA256 = 'b033be339cf37f9d1b8d04059933dc9420829a5173eb59b7a8589448b242b7ab'
BASELINE_RESULT_SHA256 = 'eda8443b6f68b9c19a3499088d55c625043846650071990a1a8e7264fe799884'
BASELINE_DIAGNOSTICS_SHA256 = '13e4d2798be38dab60b317966eea60a3d9121540af6af2759745ebcd3fff04d6'
FROZEN_SHA256 = '38c7a349ec04645e094248d20cfd3d8325e33a63ad76dd9bdc1543412bf7681d'
FEATURES = 'shardloom-python/python-native,shardloom-vortex/native-encoded-exchange-experiment'
CHECKS = {
    'candidate-format-r1': ['cargo', 'fmt', '--all', '--', '--check'],
    'candidate-default-clippy-r1': ['cargo', 'clippy', '--offline', '--locked', '--workspace', '--all-targets', '--', '-D', 'warnings'],
    'candidate-default-tests-r1': ['env', 'RUST_TEST_THREADS=4', 'cargo', 'test', '--offline', '--locked', '--workspace', '--all-targets'],
    'candidate-native-clippy-r2': ['cargo', 'clippy', '--offline', '--locked', '--workspace', '--all-targets', '--features', FEATURES, '--', '-D', 'warnings'],
    'candidate-native-tests-r1': ['env', 'RUST_TEST_THREADS=4', 'cargo', 'test', '--offline', '--locked', '--workspace', '--all-targets', '--features', FEATURES],
    'candidate-regressions-r2': ['cargo', 'test', '--offline', '--locked', '--workspace', '--lib', '--features', FEATURES, 'window', '--', '--show-output', '--test-threads=4'],
    'candidate-attribution-r1': ['env', 'SHARDLOOM_WINDOW_ATTRIBUTION_OUTPUT=' + ORIGINAL_ROOT + '/candidate-attribution-observations-r1.json',
        'cargo', 'test', '--offline', '--locked', '--workspace', '--lib', '--features', FEATURES,
        'native_compatible_window_complete_request_attribution', '--', '--ignored', '--show-output', '--test-threads=1'],
    'candidate-release-build-r1': ['env', 'PYO3_PYTHON=/opt/homebrew/opt/python@3.13/libexec/bin/python3',
        'cargo', 'build', '--offline', '--locked', '--release', '--features', 'release-user-surfaces',
        '-p', 'shardloom-cli', '-p', 'shardloom-python'],
}
REPLAY_SUPPORT = ('scripts/native_workflow_protocol.py', 'scripts/clickbench_reference_packet.py',
                  'scripts/clickbench_native_sql.py')
DOC_CHECKS = {
    'docs-checks-r1': ['/opt/homebrew/opt/python@3.13/libexec/bin/python3', '-B',
        ORIGINAL_ROOT + '/tools/check_docs.py'],
    'doc-contracts-r1': ['env', 'RUST_TEST_THREADS=4', 'cargo', 'test', '--offline', '--locked',
        '-p', 'shardloom-contract-tests', '--all-targets'],
}
STATUS_PROSE = {
    'docs/architecture/phased-execution-plan.md',
    'docs/architecture/native-shared-pipeline-investigations-2026-11-10.md',
    'docs/architecture/native-shared-index-metadata-2026-11-10.md',
    'docs/architecture/native-shared-window-state-2026-11-11.md',
    'docs/architecture/local-development-storage.md',
}
CI_POLICY_PATH = '.github/ci-native-baseline.json'
CI_DIAGNOSTIC = 'local_primitives::prepared_relational::tests::window_spill_tests::shared_tests::attribution_tests::native_compatible_window_complete_request_attribution'
ORIGINAL_FAILURES = {'candidate-native-clippy-r1.json', 'candidate-ci-verification-r1.json',
                     'candidate-ci-verification-r2.json'}
CONTROL_NAMES = {
    'native_test_failure', 'undrained_native_process', 'mutable_source_validation',
    'window_source_changed', 'root_lock_changed', 'required_test_missing_with_rebound_log',
    'ignored_ci_policy_changed', 'actual_ci_inventory_changed', 'runtime_library_mismatch',
    'diagnostic_output_corrupted', 'diagnostic_work_corrupted', 'candidate_engine_total_corrupted',
    'candidate_case_omitted', 'baseline_reuse_corrupted', 'candidate_no_fallback_denied',
    'gain_disposition_corrupted', 'original_failure_omitted', 'full10k_claim', 'full43_claim',
    'composition_identity_changed', 'composition_reference_changed', 'archive_byte_changed',
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
    assert report['schema'] == 'shardloom.compatible_window_acceptance.v1'
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
    assert data['current/tools/verify_window_packet.py'] == Path(__file__).read_bytes()
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
        record = external[row['path']]
        size = row.get('bytes', row.get('generation', {}).get('bytes'))
        assert record['identity'] == dict(bytes=size, sha256=row['sha256'])
        path = Path(row['path'])
        if record['role'] == 'accepted_runtime_identity':
            assert path.parent == Path(INDEX_ROOT) / 'runtime-r1'
            assert path.name in ('shardloom', 'lib_native.dylib', '_native.abi3.so')
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
    assert names and len(names) == len(set(names))
    assert not re.search(rb'^test .* \.\.\. FAILED$|^test result: FAILED\.', raw, re.M)
    return [name.decode() for name in names]


def verify_ci(packet, source):
    policy = json.loads(packet.data['source-blobs/' + source[CI_POLICY_PATH]['sha256']])
    baseline = packet.read('current/baseline-missing-r1.json')['build_sources']
    old = json.loads(packet.data['source-blobs/' + baseline[CI_POLICY_PATH]['sha256']])
    assert len(old['ignored_tests']) == 34 and CI_DIAGNOSTIC not in old['ignored_tests']
    assert policy == {**old, 'ignored_tests': sorted([*old['ignored_tests'], CI_DIAGNOSTIC])}
    record = packet.read('current/candidate-ci-inventory-r1.json')
    assert record['status'] == 'passed_actual_native_inventory' and record['exact_ignored_ownership']
    assert record['query_executions'] == 0 and record['source_fingerprint_sha256'] == fingerprint(source)
    assert record['ignored'] == policy['ignored_tests'] and len(record['ignored']) == 35
    assert record['discovery_features'] == ['release-user-surfaces']
    assert record['discovery_profile'] == 'debug' and record['hosted_ci_profile'] == 'ci-native'
    assert record['both_profiles_have_debug_assertions']
    partitions = record['partitions']
    names = [name for part in partitions for name in part['tests']]
    assert len(partitions) == 4 and len(names) == len(set(names)) == record['test_count'] == 4134
    assert names.count(CI_DIAGNOSTIC) == 1 and record['attribution_test'] == CI_DIAGNOSTIC
    for name, expected in record['files'].items():
        assert identity(packet.data[packet.report['locations'][name]]) == expected
    command = ['/opt/homebrew/opt/python@3.13/libexec/bin/python3', '-B',
        ORIGINAL_ROOT + '/tools/verify_window_ci_inventory.py', '--build',
        ORIGINAL_ROOT + '/candidate-ci-build-r1.json', '--output',
        ORIGINAL_ROOT + '/candidate-ci-inventory-r1.json']
    packet.guard('current/candidate-ci-verification-r3', command, source)
    packet.guard('current/candidate-ci-build-r1', ['cargo', 'test', '--offline', '--locked',
        '-p', 'shardloom-vortex', '--lib', '--features', 'release-user-surfaces', '--no-run'], source)
    assert re.search(rb'Ran 17 tests in [\d.]+s\n\nOK\n', packet.data['current/candidate-ci-verification-r3.log'])
    return dict(tests=len(names), ignored=len(record['ignored']), partitions=len(partitions))


def verify_diagnostics(packet):
    data = packet.data
    assert digest(data['current/baseline-diagnostics-r1.json']) == BASELINE_DIAGNOSTICS_SHA256
    baseline = packet.read('current/baseline-diagnostics-r1.json')
    current = packet.read('current/candidate-diagnostics-r1.json')
    assert baseline['status'] == 'passed_baseline_diagnostics'
    assert current['status'] == 'passed_candidate_diagnostics'
    assert current['diagnostic_source_fingerprint_sha256'] == SOURCE_FINGERPRINT
    assert current['native_query_executions_during_verification'] == 0
    assert not current['engine_timing_claimed'] and not current['candidate_acceptance_claimed']
    for report in (baseline, current):
        for name, expected in report['files'].items():
            assert identity(data[packet.report['locations'][name]]) == expected
    old = json.loads(packet.bound(baseline['attribution_observations']))
    old_log = packet.bound(baseline['regression_log']).decode()
    new = packet.read('current/candidate-attribution-observations-r1.json')
    new_log = data['current/candidate-regressions-r2.log'].decode()
    work_path = PREPARATION_ROOT + '/window-tools-draft/window_work_contract.py'
    raw = data[packet.report['locations'][work_path]]
    module = types.ModuleType('archived_window_work_contract')
    exec(compile(raw, work_path, 'exec'), module.__dict__)
    compared = module.compare(old, new, old_log, new_log)
    assert compared == current['work_contract']
    assert current['diagnostic_requests'] == 33 and current['calendar_controls'] == 16 and current['error_controls'] == 18
    assert len(current['rejected_corruptions']) == len(set(current['rejected_corruptions'])) == 20
    logged = [json.loads(line.split('WINDOW_SHARED_WORK ', 1)[1])
        for line in data['current/candidate-attribution-r1.log'].decode().splitlines()
        if 'WINDOW_SHARED_WORK ' in line]
    assert logged == new['observations'] and len(logged) == 33
    required = current['required_tests']
    assert len(required) == len(set(required)) == 29
    for check in ('candidate-regressions-r2', 'candidate-native-tests-r1'):
        names = test_names(data['current/' + check + '.log'])
        for name in required:
            assert sum(value.endswith('::' + name) for value in names) == 1, name
    names = test_names(data['current/candidate-attribution-r1.log'])
    assert names == [CI_DIAGNOSTIC] and current['ignored_ci_registered']
    source = packet.report['candidate_source_snapshot']
    for name, item in baseline['diagnostic_source_changes'].items():
        if name.endswith(('local_primitive_relational_window_shared_tests.rs',
                'local_primitive_relational_window_shared_attribution_tests.rs',
                'local_primitive_relational_calendar_shared_tests.rs')):
            assert source[name] == item['after']
    return dict(complete_requests=33, calendar_controls=16, error_controls=18,
        diagnostic_corruptions_rejected=20, engine_timing_claimed=False)


def verify_sources(report, data, repo=None):
    packet = Packet(report, data)
    assert digest(data['current/baseline-protocol-r1.json']) == BASELINE_PROTOCOL_SHA256
    assert digest(data['current/baseline-verified-r1.json']) == BASELINE_RESULT_SHA256
    build = packet.read('current/candidate-release-build-r1.json')
    source = build['build_sources']
    assert report['source_base'] == BASE and report['candidate_source_snapshot'] == source
    assert report['candidate_source_fingerprint_sha256'] == fingerprint(source) == SOURCE_FINGERPRINT
    for name, command in CHECKS.items():
        record = packet.guard('current/' + name, command, source)
        assert record['base'] == record['head'] == BASE and not record['query_workload']
        assert report['checks'][name] == dict(command=command, status='passed',
            rust_tests=rust_counts(data['current/' + name + '.log']))
    assert rust_counts(data['current/candidate-default-tests-r1.log']) == dict(passed=3323, failed=0, ignored=0)
    native = rust_counts(data['current/candidate-native-tests-r1.log'])
    assert native == dict(passed=7092, failed=0, ignored=39)
    assert rust_counts(data['current/candidate-regressions-r2.log']) == dict(passed=183, failed=0, ignored=1)
    assert rust_counts(data['current/candidate-attribution-r1.log']) == dict(passed=1, failed=0, ignored=0)
    check_index = packet.read('current/candidate-source-checks-r1.json')
    assert check_index['schema'] == 'shardloom.compatible_window_candidate_checks.v1'
    assert check_index['source_head'] == BASE and check_index['source_fingerprint_sha256'] == SOURCE_FINGERPRINT
    expected_checks = dict(format='candidate-format-r1', default_clippy='candidate-default-clippy-r1',
        default_tests='candidate-default-tests-r1', native_clippy='candidate-native-clippy-r2',
        native_tests='candidate-native-tests-r1', regression='candidate-regressions-r2',
        attribution='candidate-attribution-r1')
    assert check_index['checks'] == {key: ORIGINAL_ROOT + '/' + name + '.json'
        for key, name in expected_checks.items()}
    assert report['ci_inventory'] == verify_ci(packet, source)
    for name, command in DOC_CHECKS.items():
        record = packet.guard('current/' + name, command)
        assert record['base'] == record['head'] == BASE
        late = record['build_sources']
        changed = {key for key in set(late) | set(source) if late.get(key) != source.get(key)}
        assert changed <= STATUS_PROSE
        counts = rust_counts(data['current/' + name + '.log'])
        assert report['documentation_checks'][name] == dict(command=command, status='passed', rust_tests=counts)
        if name == 'doc-contracts-r1':
            assert counts['passed'] > 0 and counts['failed'] == counts['ignored'] == 0
    runtime = packet.read('current/runtime-r1-identity.json')
    assert runtime['status'] == 'passed' and runtime['profile'] == 'release'
    assert runtime['build_receipt'] == 'candidate-release-build-r1.json'
    assert runtime['build_receipt_identity'] == identity(data['current/candidate-release-build-r1.json'])
    assert runtime['build_source_fingerprint_sha256'] == SOURCE_FINGERPRINT
    files = {Path(row['path']).name: row for row in runtime['files']}
    assert set(files) == {'shardloom', 'lib_native.dylib'}
    assert all(Path(row['path']).parent == Path(ORIGINAL_ROOT) / 'runtime-r1' for row in files.values())
    copied = packet.read('current/runtime-library-copy-r1.json')
    assert copied['status'] == 'passed' and copied['byte_identical'] and copied['queries_executed'] == 0
    library = files['lib_native.dylib']
    assert copied['source']['path'] == library['path']
    assert copied['destination']['path'] == ORIGINAL_ROOT + '/runtime-r1/_native.abi3.so'
    for row in (copied['source'], copied['destination']):
        assert dict(bytes=row['generation']['bytes'], sha256=row['sha256']) == {
            key: library[key] for key in ('bytes', 'sha256')}
    python = '/opt/homebrew/opt/python@3.13/libexec/bin/python3'
    client_python = '/Users/dylan/.cache/shardloom/query-surface-polars-2.1.0/bin/python'
    packet.guard('current/candidate-freeze-runtime-r1', [python, '-B',
        ORIGINAL_ROOT + '/tools/freeze_runtime.py', '--build', 'candidate-release-build-r1.json',
        '--name', 'runtime-r1', '--profile', 'release'], source)
    packet.guard('current/candidate-library-r1', [client_python, '-B',
        ORIGINAL_ROOT + '/tools/run_window_candidate.py', '--prepare-library'], source)
    packet.guard('current/candidate-plan-r1', [client_python, '-B',
        ORIGINAL_ROOT + '/tools/run_window_candidate.py', '--plan'], source)
    protocol = packet.read('current/candidate-protocol-r1.json')
    binding = protocol['runtime_binding']
    assert binding['source_head'] == binding['accepted_base'] == BASE
    assert binding['source_fingerprint_sha256'] == SOURCE_FINGERPRINT
    for role, name in (('binary', 'shardloom'), ('library', 'lib_native.dylib')):
        row = binding['runtime'][role]
        assert dict(bytes=row['generation']['bytes'], sha256=row['sha256']) == {
            key: files[name][key] for key in ('bytes', 'sha256')}
    for row in binding['proof_files']:
        packet.proof(row)
    baseline = packet.guard('current/baseline-missing-r1')
    assert baseline['base'] == baseline['head'] == BASE and not baseline['working_tree']
    closeout = packet.read('index/shared-index-closeout-r1.json')
    assert closeout['status'] == 'complete' and closeout['merge_commit'] == BASE and closeout['source_tree'] == BASE_TREE
    assert closeout['pr_checks_passed'] == 58 and closeout['main_checks_passed'] == 55
    assert closeout['runtime_sources_match_build'] and closeout['source_bound_replay'] == 'passed'
    assert closeout['portable_replay_after_retirement'] == 'passed' and closeout['current_inputs_unchanged']
    for field in ('complete_10k_performance_claim', 'full43_claim', 'whole_item_19_complete',
            'whole_item_20_complete', 'publication_attempted'):
        assert report[field] is False
    dispositions = packet.read('current/candidate-failure-dispositions-r1.json')
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
    failure_name = 'preparation/window-baseline-failure-dispositions-r1.json'
    history = packet.read(failure_name)
    assert history['schema'] == 'shardloom.window_baseline_failure_dispositions.v1'
    assert history['status'] == 'recorded_original_failed_diagnostics'
    assert len(history['observations']) == 4
    assert {row['attempt'] for row in history['observations']} == {
        'window-baseline-clippy-r1', 'window-baseline-clippy-r2',
        'window-baseline-regressions-r1', 'window-baseline-attribution-r1'}
    for row in history['observations']:
        failed = json.loads(packet.bound(row['receipt']))
        log = packet.bound(row['log'])
        assert failed['status'] == 'failed' and failed['returncode'] == row['returncode'] == 101
        assert not failed['query_workload'] and failed['log_sha256'] == digest(log)
        assert failed['build_source_fingerprint_sha256'] == row['original_source_fingerprint_sha256']
        assert fingerprint(failed['build_sources']) == row['original_source_fingerprint_sha256']
        for item in failed['build_sources'].values():
            if 'sha256' in item and 'symlink' not in item:
                assert identity(data['source-blobs/' + item['sha256']]) == item
    assert not history['partial_attribution_accepted_as_complete'] and not history['release_runtime_changed']
    assert history['scored_query_executions'] == 0 and not history['storage_limits_changed'] and not history['deadline_changed']
    assert report['baseline_failure_history'] == dict(observations=4, source=identity(data[failure_name]))
    original = packet.read('preparation/current-baseline-10k-r1/failed-observation-replay-r1.json')
    assert original['status'] == 'verified_failed_observation' and original['verification_passed']
    assert original['observed_attempts'] == 10000 and original['verified_correct_results'] == 9904
    assert original['original_outcomes'] == {'passed': 9904, 'crashed': 76, 'harness_error': 20}
    assert original['engine_execution_seconds'] is None and original['complete_performance_result_valid'] is False
    assert report['disposition'] in {'retain_candidate', 'drop_candidate_no_gain', 'drop_candidate_invalid_cohort'}
    if repo is not None:
        target = source if report['disposition'] == 'retain_candidate' else baseline['build_sources']
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
    assert old_result['selected_cases'] == result['selected_cases'] == 1212
    assert old_result['reused_saved_cases'] == 188 and old_result['new_attempts'] == 1024
    assert result['new_attempts'] == 1212 and result['query_executions_during_replay'] == 0
    assert protocol['cases'] == selection['cases'] and len(selection['cases']) == 1212
    assert protocol['selected_count'] == 1212 and protocol['original_count'] == 776 and protocol['composed_count'] == 436
    assert protocol['candidate_contract'] == old_protocol['candidate_contract']
    assert protocol['selection_uses_timing'] is False and protocol['full43_claim'] is False
    for record in (old_protocol, protocol, old_result, result):
        assert record['primary_clock'] == 'engine_execution_seconds'
        assert record['attempts_per_case'] == 1 and record['warmups'] == record['requested_repeats'] == 0
        assert record['complete_10k_performance_claim'] is False
    assert packet.bound(protocol['baseline_protocol']) == data['current/baseline-protocol-r1.json']
    assert packet.bound(protocol['baseline_result']) == data['current/baseline-verified-r1.json']
    assert protocol['selection'] == old_protocol['selection'] == result['selection'] == old_result['selection']
    candidate_proof = {row['path']: row for row in protocol['runtime_binding']['proof_files']}
    assert len(candidate_proof) == len(protocol['runtime_binding']['proof_files'])
    assert old_result['runtime_binding'] == old_protocol['runtime_binding']
    for row in old_protocol['runtime_binding']['proof_files']:
        assert candidate_proof[row['path']] == row
    assert set(report['identity_only_proof']) <= set(candidate_proof)
    for key in ('isolation', 'performance_decision', 'baseline_observations'):
        assert protocol[key] == old_protocol[key]
    design, inputs = (packet.read('frozen/' + name + '.json') for name in ('design', 'inputs'))
    prepared = packet.read('frozen/native-preparation/complete.json')
    assert len(design['cases']) == 10000 and old_protocol['execution'] == protocol['execution'] == design['execution']
    for name in ('design', 'inputs', 'frozen'):
        assert packet.bound(protocol[name]) == packet.bound(old_protocol[name]) == data['frozen/' + name + '.json']
    assert Counter(row['kind'] for row in selection['cases']) == {'original': 776, 'composition': 436}
    for selected in selection['cases']:
        case = selected['case']
        original = case['original_case'] if selected['kind'] == 'composition' else case
        assert original == design['cases'][selected['original_seq']]
    source_bytes = replay_sources(packet)
    harness = temporary / 'expanded-query-2026-11-03/investigation-original-harness-r1'
    written = {}
    for name, raw in source_bytes.items():
        path = harness / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        written[path.resolve()] = raw
    preparation = temporary / 'shared-operator-state-2026-11-10'
    for name in ('window-tools-draft/window_replay.py', 'window-client-draft/shared_window_worker.py',
            'window-client-draft/shared_window_cases.py'):
        raw = data[packet.report['locations'][PREPARATION_ROOT + '/' + name]]
        path = preparation / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        written[path.resolve()] = raw
    for name, raw in data.items():
        if name.endswith('.sqlite'):
            safe_name(name)
            path = temporary / 'databases' / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
    sys.path.insert(0, str(preparation / 'window-tools-draft'))
    import window_replay as replay_module
    from query_surface.expanded_freeze import unpack
    from query_surface.expanded_report import raw_files
    from query_surface.expanded_run import request
    assert not any(name == 'shardloom._native' or name.startswith('shardloom._native.') for name in sys.modules)
    for name, module in list(sys.modules.items()):
        if name.startswith(('query_surface', 'shardloom', 'shared_window')) or name in (
                'window_replay', 'query_timing_report', 'native_workflow_protocol', 'local_uat_storage',
                'resources', 'run_clickbench_query_uat', 'timed_native_command',
                'clickbench_reference_packet', 'clickbench_native_sql'):
            path = Path(module.__file__).resolve()
            assert path in written and path.read_bytes() == written[path], name
    cases = replay_module.Cases()
    baseline_rows = [json.loads(line) for line in gzip.decompress(packet.bound(old_result['case_rows'])).splitlines()]
    candidate_rows = [json.loads(line) for line in gzip.decompress(packet.bound(result['case_rows'])).splitlines()]
    assert len(baseline_rows) == len(candidate_rows) == 1212
    reused = json.loads(packet.bound(old_protocol['reuse']))
    reused_by_ordinal = {row['cohort_ordinal']: row for row in reused['rows']}
    assert len(reused['rows']) == len(reused_by_ordinal) == old_protocol['reused_count'] == 188
    assert reused['all_complete_results_correct'] and reused['all_engine_clocks_valid']
    assert reused['no_external_fallback'] and reused['exact_requests_and_expected_results']
    assert reused['runtime'] == old_protocol['runtime_binding']['runtime']
    packet.bound(reused['retained_saved_attempts'])
    missing = old_protocol['missing_cases']
    assert len(missing) == old_protocol['missing_count'] == 1024
    assert [row for ordinal, row in enumerate(selection['cases']) if ordinal not in reused_by_ordinal] == missing
    captures = {}
    for role, prefix, count in (('missing', 'current/baseline-missing-r1', 1024),
            ('candidate', 'current/candidate-r1', 1212)):
        done, binding = packet.read(prefix + '/complete.json'), packet.read(prefix + '/binding.json')
        assert done['status'] == 'complete_observation' and done['stage'] == 'native'
        assert done['total'] == done['attempt_slots'] == count
        assert done['posthash_verified'] and done['attempts_per_case'] == 1 and done['requested_repeats'] == 0
        assert binding['contract'] == {key: done[key] for key in ('frozen', 'stage', 'prerequisites', 'updated')}
        assert packet.bound(done['ledger']) == data[prefix + '/attempts.sqlite']
        assert packet.bound(done['binding']) == data[prefix + '/binding.json']
        packet.bound(done['frozen'])
        guard = packet.guard(prefix)
        assert guard['query_workload'] and guard['build_inventory_policy'] == guard['evidence_inventory_policy'] == 'before_and_after_only'
        assert guard['base'] == guard['head'] == BASE
        assert guard['active_output'] == ORIGINAL_ROOT + '/' + prefix.removeprefix('current/')
        script = 'run_window_candidate.py' if role == 'candidate' else 'run_window_baseline.py'
        assert guard['command'] == ['/Users/dylan/.cache/shardloom/query-surface-polars-2.1.0/bin/python',
            '-B', ORIGINAL_ROOT + '/tools/' + script]
        if role == 'candidate':
            assert guard['build_sources'] == report['candidate_source_snapshot']
        else:
            assert not guard['working_tree']
        captures[role] = dict(prefix=prefix, done=done, runtime=done['updated']['runtime'])
    assert captures['missing']['done']['updated'] == old_protocol['runtime_binding']
    assert captures['candidate']['done']['updated'] == protocol['runtime_binding'] == result['runtime_binding']
    outcomes, families, totals = {}, {}, {}
    with ExitStack() as stack:
        def database(name):
            connection = sqlite3.connect('file:' + str(temporary / 'databases' / name) + '?mode=ro', uri=True)
            connection.execute('PRAGMA query_only=ON')
            stack.callback(connection.close)
            return connection
        refs = {profile: database('frozen/reference-' + profile + '/expectations.sqlite')
            for profile in {row['case']['profile'] for row in selection['cases']}}
        refs['composition'] = database('preparation/window-reference-r2/expectations.sqlite')
        dbs = {role: database(row['prefix'] + '/attempts.sqlite') for role, row in captures.items()}
        saved_path = packet.report['locations'][reused['retained_saved_attempts']['path']]
        dbs['saved'] = database(saved_path)
        for role, count in (('missing', 1024), ('candidate', 1212)):
            assert dbs[role].execute('SELECT count(*),min(seq),max(seq),count(distinct id) FROM attempt').fetchone() == (count, 0, count-1, count)
        assert dict(dbs['saved'].execute('SELECT status,count(*) FROM attempt GROUP BY status')) == {'passed': 188}
        assert {row[0] for row in dbs['saved'].execute('SELECT seq FROM attempt')} == {
            row['source_candidate_ordinal'] for row in reused['rows']}
        for role, archived_rows in (('baseline', baseline_rows), ('candidate', candidate_rows)):
            counts, family_values, seconds = Counter(), defaultdict(list), []
            missing_ordinal = 0
            for ordinal, (selected, archived) in enumerate(zip(selection['cases'], archived_rows, strict=True)):
                case = selected['case']
                assert archived['cohort_ordinal'] == selected['cohort_ordinal'] == ordinal
                assert archived['original_seq'] == selected['original_seq'] and archived['kind'] == selected['kind']
                assert archived['id'] == case['id'] and archived['case_identity'] == case['case_identity']
                if role == 'candidate':
                    capture_role, seq = 'candidate', ordinal
                elif ordinal in reused_by_ordinal:
                    capture_role, seq = 'saved', reused_by_ordinal[ordinal]['source_candidate_ordinal']
                    assert seq == selected['saved_candidate_ordinal']
                else:
                    capture_role, seq = 'missing', missing_ordinal
                    missing_ordinal += 1
                capture = captures['missing' if capture_role == 'saved' else capture_role]
                entry = dbs[capture_role].execute('SELECT seq,id,status,packet FROM attempt WHERE seq=?', (seq,)).fetchone()
                assert entry is not None and entry[:2] == (seq, case['id'])
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
                ledger = reused['retained_saved_attempts']['path'] if capture_role == 'saved' else capture['done']['ledger']['path']
                assert provenance['ledger'] == ledger and provenance['source_seq'] == seq
                assert provenance['packet'] == (identity(entry[3]) if entry[3] is not None else None)
                assert provenance['job_sha256'] == (digest(files['one.job.json']) if files else None)
                assert provenance['independent_expected_blob_sha256'] == digest(reference[1])
                if capture_role == 'saved':
                    previous = reused_by_ordinal[ordinal]
                    assert previous['result'] == observed and previous['packet'] == provenance['packet']
                    assert previous['raw_job_sha256'] == provenance['job_sha256']
                if role == 'candidate':
                    assert archived['baseline_engine_seconds'] == baseline_rows[ordinal]['result']['engine_seconds']
                counts[observed['status']] += 1
                if observed['status'] != 'passed':
                    assert role == 'candidate'
                    continue
                value = observed['engine_seconds']
                assert type(value) in (int, float) and math.isfinite(value) and value >= 0
                seconds.append(value)
                family_values[case['family']].append(value)
            assert role == 'candidate' or missing_ordinal == 1024
            outcomes[role], families[role] = dict(counts), family_values
            totals[role] = math.fsum(seconds) if counts == {'passed': 1212} else None
    assert outcomes['baseline'] == old_result['outcomes'] == {'passed': 1212}
    assert totals['baseline'] == old_result['engine_execution_seconds'] == 340.599417211
    assert outcomes['candidate'] == result['outcomes'] == captures['candidate']['done']['outcomes']
    assert totals['candidate'] == result['engine_execution_seconds']
    passed = outcomes['candidate'] == {'passed': 1212}
    disposition = ('retain_candidate' if totals['candidate'] < totals['baseline'] else 'drop_candidate_no_gain') if passed else 'drop_candidate_invalid_cohort'
    assert report['disposition'] == result['frozen_rule_disposition'] == disposition
    assert result['complete_performance_result_valid'] == result['all_selected_results_correct'] == result['all_engine_clocks_valid'] == result['native_no_fallback'] == passed
    assert result['baseline_engine_execution_seconds'] == totals['baseline']
    assert result['engine_seconds_saved'] == (totals['baseline'] - totals['candidate'] if passed else None)
    assert result['observed_engine_reduction_percent'] == (100 * (totals['baseline'] - totals['candidate']) / totals['baseline'] if passed else None)
    assert report['baseline'] == dict(cases=1212, reused_saved_cases=188, new_attempts=1024,
        engine_execution_seconds=totals['baseline'])
    assert report['candidate'] == {key: result[key] for key in ('selected_cases', 'outcomes',
        'engine_execution_seconds', 'engine_seconds_saved', 'observed_engine_reduction_percent',
        'complete_performance_result_valid')}
    for name, values in families['baseline'].items():
        assert old_result['families'][name] == dict(cases=len(values), engine_execution_seconds=math.fsum(values))
    for name, values in families['candidate'].items():
        assert result['families'][name] == dict(cases=len(values), engine_execution_seconds=math.fsum(values),
            baseline_engine_execution_seconds=old_result['families'][name]['engine_execution_seconds'])
    return dict(baseline_cases=1212, reused_baseline_cases=188, new_baseline_cases=1024,
        candidate_cases=1212, outcomes=outcomes, engine_execution_seconds=totals,
        disposition=disposition, query_executions=0, complete_10k_performance_claim=False, full43_claim=False)


def verify_controls(packet_path, report, data):
    controls = json.loads((packet_path / 'corruption-controls.json').read_bytes())
    assert controls['status'] == 'passed' and controls['query_executions'] == 0
    assert controls['acceptance_manifest'] == identity((packet_path / 'acceptance.json').read_bytes())
    assert controls['positive_controls'] == 2 and controls['rejected_corruptions'] == len(CONTROL_NAMES)
    assert len(controls['controls']) == len(CONTROL_NAMES) + 2
    assert {row['name'] for row in controls['controls'] if row['outcome'] == 'rejected'} == CONTROL_NAMES
    assert {row['name'] for row in controls['controls'] if row['outcome'] == 'passed'} == {'source_bound', 'relocated_source_only'}
    for name in ('verify_window_packet.py', 'check_window_packet_controls.py'):
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
    with tempfile.TemporaryDirectory(prefix='shared-window-evidence-') as temporary:
        replay = replay_saved(report, data, Path(temporary))
    controls = verify_controls(packet_path, report, data) if require_controls else None
    return dict(status='passed_source_bound_replay', diagnostics=diagnostics, controls=controls, **replay)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo-root', type=Path)
    parser.add_argument('--require-controls', action='store_true')
    args = parser.parse_args()
    print(json.dumps(verify(Path(__file__).resolve().parent, args.repo_root, args.require_controls), indent=2, sort_keys=True))

