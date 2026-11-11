"""Replay the fixed shared-index experiment from archived source and result bytes.

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

BASE = '037b6801320aa0bfe939ba10aed9304198b77a5a'
BASE_TREE = '7ea0196daa21254d4c1bf0953f20a57e1b4a14e1'
ORIGINAL_ROOT = '/Users/dylan/LocalData/shardloom/shared-index-metadata-2026-11-10'
BASELINE_PROTOCOL_SHA256 = '326742c88a96116aff6f4bd77d2222270268e864013c41b3dce537ea6ef91ab5'
BASELINE_RESULT_SHA256 = '6e7e36ebfb2d11a8fb650c6710e6c460d39fc9ef0c55e7e9aaf553f495668e36'
SELECTION_SHA256 = '5477c06868540fff63d418d6fd1a50ca8a1597627efb0c35131826efe13bde52'
FROZEN_SHA256 = '38c7a349ec04645e094248d20cfd3d8325e33a63ad76dd9bdc1543412bf7681d'
FEATURES = 'shardloom-python/python-native,shardloom-vortex/native-encoded-exchange-experiment'
CHECKS = {
    'format-r1': ['cargo', 'fmt', '--all', '--', '--check'],
    'default-clippy-r1': ['cargo', 'clippy', '--offline', '--locked', '--workspace', '--all-targets', '--', '-D', 'warnings'],
    'default-tests-r1': ['env', 'RUST_TEST_THREADS=4', 'cargo', 'test', '--offline', '--locked', '--workspace', '--all-targets'],
    'native-clippy-r2': ['cargo', 'clippy', '--offline', '--locked', '--workspace', '--all-targets', '--features', FEATURES, '--', '-D', 'warnings'],
    'native-tests-r1': ['env', 'RUST_TEST_THREADS=4', 'cargo', 'test', '--offline', '--locked', '--workspace', '--all-targets', '--features', FEATURES],
    'candidate-mechanism-r1': ['env', 'SHARDLOOM_PROBE_ATTRIBUTION_OUTPUT=' + ORIGINAL_ROOT + '/candidate-attribution-observations-r1.json',
        'cargo', 'test', '--offline', '--locked', '--workspace', '--lib', '--features', FEATURES, '--',
        'local_primitives::native_relational_index::', 'cyclic_candidate_counts_actual_work_for_all_four_frozen_targets',
        'native_row_index_complete_request_attribution', '--include-ignored', '--nocapture', '--test-threads=1'],
    'release-build-r1': ['env', 'PYO3_PYTHON=/opt/homebrew/opt/python@3.13/libexec/bin/python3',
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
    'docs/architecture/local-development-storage.md',
}
ORIGINAL_FAILURES = {'baseline-attribution-r1.json', 'baseline-attribution-r2.json',
                     'candidate-diagnostic-comparison-r1.json', 'native-clippy-r1.json'}
CONTROL_NAMES = {
    'native_test_failure', 'undrained_native_process', 'mutable_source_validation',
    'index_source_changed', 'root_lock_changed', 'consumer_test_missing_with_rebound_log',
    'metadata_test_missing_with_rebound_log', 'runtime_library_mismatch',
    'diagnostic_entry_count_corrupted', 'candidate_engine_total_corrupted',
    'candidate_case_omitted', 'baseline_reuse_corrupted', 'candidate_no_fallback_denied',
    'gain_disposition_corrupted', 'original_failure_omitted', 'full10k_claim',
    'archive_byte_changed',
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
    assert report['schema'] == 'shardloom.shared_index_acceptance.v1'
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
    assert data['current/tools/verify_index_packet.py'] == Path(__file__).read_bytes()
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
            self.bound(row)
            return
        record = external[row['path']]
        size = row.get('bytes', row.get('generation', {}).get('bytes'))
        assert record['identity'] == dict(bytes=size, sha256=row['sha256'])
        path = Path(row['path'])
        if record['role'] == 'accepted_runtime_identity':
            assert path.parent == Path(ORIGINAL_ROOT).parent / 'dictionary-execution-repair-2026-11-10/runtime-r1'
            assert path.name in ('shardloom', 'lib_native.dylib', '_native.abi3.so')
        else:
            assert record['role'] == 'historical_committed_archive_identity'
            assert path.parent.parent == Path('/Users/dylan/.codex/worktrees/native-vector-representations/shardloom-local-repo/docs/release/evidence')
            assert path.parent.name in ('native-dictionary-execution-repair-2026-11-10',
                'native-uuid-lifecycle-2026-11-10', 'native-variant-expressions-2026-11-10',
                'native-variant-semantics-repair-2026-11-10')
            assert path.name.startswith('verification') and path.name.endswith('.tar.xz')
            manifest = self.read('prior-packets/' + path.parent.name + '/acceptance.json')
            if 'archives' in manifest:
                declared = next(item for item in manifest['archives'] if item['path'] == path.name)
                assert record['identity'] == {key: declared[key] for key in ('bytes', 'sha256')}
            else:
                # Earlier packets store one archive; their byte identity is
                # already immutable in the accepted baseline protocol.
                baseline = self.read('current/baseline-protocol-r1.json')
                declared = next(item for item in baseline['runtime_binding']['proof_files'] if item['path'] == row['path'])
                assert record['identity'] == dict(bytes=declared['generation']['bytes'], sha256=declared['sha256'])

    def guard(self, prefix, command=None, source=None):
        record = self.read(prefix + '.json')
        assert record['status'] == 'passed' and record['returncode'] == 0
        assert record['process_group_drained'] and record['direct_child_reaped']
        assert record['fixed_source_validation'] and not record['allow_lockfile_update']
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


def verify_sources(report, data, repo=None):
    packet = Packet(report, data)
    build = packet.read('current/release-build-r1.json')
    source = build['build_sources']
    assert report['source_base'] == BASE and report['candidate_source_snapshot'] == source
    assert report['candidate_source_fingerprint_sha256'] == fingerprint(source)
    for name, command in CHECKS.items():
        record = packet.guard('current/' + name, command, source)
        assert record['base'] == record['head'] == BASE
        assert report['checks'][name] == dict(command=command, status='passed',
            rust_tests=rust_counts(data['current/' + name + '.log']))
    assert rust_counts(data['current/default-tests-r1.log']) == dict(passed=3323, failed=0, ignored=0)
    native = rust_counts(data['current/native-tests-r1.log'])
    assert native == dict(passed=7075, failed=0, ignored=38)
    assert rust_counts(data['current/candidate-mechanism-r1.log']) == dict(passed=16, failed=0, ignored=0)
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
    manifest = packet.read('preparation/reuse-metadata-r1/consumer-test-manifest-primary-r1.json')
    assert len(manifest['tests']) == 41
    for row in manifest['tests']:
        assert ('test ' + row['exact_test_name'] + ' ... ok\n').encode() in data['current/native-tests-r1.log']
    owner_tests = []
    for name in ('shardloom-vortex/src/local_primitives/native_relational_index.rs',
                 'shardloom-vortex/src/local_primitives/native_relational_index_metadata_tests.rs'):
        raw = data['source-blobs/' + source[name]['sha256']]
        owner_tests.extend(re.findall(rb'#\[test\](?:\s*#\[[^\]]+\])*\s*fn (\w+)\(', raw))
    assert len(owner_tests) == len(set(owner_tests)) == 14
    for name in owner_tests:
        for check in ('candidate-mechanism-r1', 'native-tests-r1'):
            assert re.search(rb'^test \S*::' + name + rb' \.\.\. ok$', data['current/' + check + '.log'], re.M), name
    runtime = packet.read('current/runtime-r1-identity.json')
    assert runtime['status'] == 'passed' and runtime['profile'] == 'release'
    assert runtime['build_receipt'] == 'release-build-r1.json'
    assert runtime['build_receipt_identity'] == identity(data['current/release-build-r1.json'])
    assert runtime['build_source_fingerprint_sha256'] == fingerprint(source)
    files = {Path(row['path']).name: row for row in runtime['files']}
    assert set(files) == {'shardloom', 'lib_native.dylib'}
    copied = packet.read('current/runtime-library-copy-r1.json')
    assert copied['status'] == 'passed' and copied['byte_identical'] and copied['queries_executed'] == 0
    library = files['lib_native.dylib']
    assert copied['source']['path'] == library['path']
    for row in (copied['source'], copied['destination']):
        assert dict(bytes=row['generation']['bytes'], sha256=row['sha256']) == {
            key: library[key] for key in ('bytes', 'sha256')}
    candidate = packet.read('current/candidate-protocol-r1.json')
    binding = candidate['runtime_binding']
    assert binding['source_head'] == binding['accepted_base'] == BASE
    assert binding['source_fingerprint_sha256'] == fingerprint(source)
    for role, name in (('binary', 'shardloom'), ('library', 'lib_native.dylib')):
        row = binding['runtime'][role]
        assert dict(bytes=row['generation']['bytes'], sha256=row['sha256']) == {
            key: files[name][key] for key in ('bytes', 'sha256')}
    for row in binding['proof_files']:
        packet.proof(row)
    baseline = packet.guard('current/baseline-missing-r1')
    assert baseline['base'] == baseline['head'] == BASE and not baseline['working_tree']
    closeout = packet.read('repair/dictionary-repair-closeout-r1.json')
    assert closeout['status'] == 'complete' and closeout['merge_commit'] == BASE and closeout['source_tree'] == BASE_TREE
    assert closeout['pr_checks_passed'] == 58 and closeout['main_checks_passed'] == 55
    assert closeout['runtime_sources_match_build'] and closeout['source_bound_replay'] == 'passed'
    assert closeout['portable_replay_after_retirement'] == 'passed' and closeout['current_inputs_unchanged']
    assert report['complete_10k_performance_claim'] is False
    assert report['whole_item_19_complete'] is False and report['whole_item_20_complete'] is False
    assert report['publication_attempted'] is False
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
    original = packet.read('preparation/current-baseline-10k-r1/failed-observation-replay-r1.json')
    assert original['status'] == 'verified_failed_observation' and original['verification_passed']
    assert original['observed_attempts'] == 10000 and original['verified_correct_results'] == 9904
    assert original['original_outcomes'] == {'passed': 9904, 'crashed': 76, 'harness_error': 20}
    assert original['engine_execution_seconds'] is None and original['complete_performance_result_valid'] is False
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
    before = packet.read('repair/release-build-r1.json')['build_sources']
    after = packet.read('current/release-build-r1.json')['build_sources']
    for name in REPLAY_SUPPORT:
        assert name not in sources and before[name] == after[name]
        raw = data['source-blobs/' + before[name]['sha256']]
        assert identity(raw) == before[name]
        sources[name] = raw
    for row in frozen['references']:
        packet.bound(row)
    return sources


def verify_diagnostics(packet):
    data = packet.data
    name = 'current/tools/verify_index_diagnostics.py'
    module = types.ModuleType('shared_index_archived_diagnostic_verifier')
    exec(compile(data[name], name, 'exec'), module.__dict__)
    before = packet.read('current/baseline-attribution-observations-r3.json')['observations']
    after = packet.read('current/candidate-attribution-observations-r1.json')['observations']

    def log_rows(name, marker):
        pattern = re.compile(r'(?:^|\s)' + re.escape(marker) + r' (\{.*\})$')
        return [json.loads(match[1]) for line in data[name].decode().splitlines()
            if (match := pattern.search(line)) is not None]

    assert before == log_rows('current/baseline-attribution-r3.log', 'ROW_INDEX_WORK')
    assert after == log_rows('current/candidate-mechanism-r1.log', 'ROW_INDEX_WORK')
    pairs = module.compare_requests(before, after)
    old_cyclic = log_rows('current/baseline-consumers-r1.log', 'CYCLIC_ROW_INDEX_WORK')
    new_cyclic = log_rows('current/candidate-mechanism-r1.log', 'CYCLIC_ROW_INDEX_WORK')
    expected = {(case, call) for case in ('abundant', 'rare', 'absent', 'skew_wide') for call in (0, 1)}
    assert len(old_cyclic) == len(new_cyclic) == 8
    old_map = {(row['case'], row['call']): row['observations'] for row in old_cyclic}
    new_map = {(row['case'], row['call']): row['observations'] for row in new_cyclic}
    assert set(old_map) == set(new_map) == expected
    for key in expected:
        module.compare_counts(old_map[key], new_map[key], True)
    result = packet.read('current/candidate-diagnostic-comparison-r2.json')
    assert result['status'] == 'passed_frozen_contract' and result['complete_requests'] == 22 and result['cyclic_calls'] == 8
    assert result['pairs'] == pairs
    assert {(row['case'], row['call']): row['baseline'] for row in result['cyclic']} == old_map
    assert {(row['case'], row['call']): row['candidate'] for row in result['cyclic']} == new_map
    for item in result['proof']:
        packet.bound(item)
    controls = packet.read('current/diagnostic-corruption-controls-r1.json')
    assert controls['status'] == 'passed' and controls['queries_executed'] == 0
    assert controls['positive_controls'] == 1 and controls['rejected_corruptions'] == 14
    assert len(controls['controls']) == 14 and all(row['outcome'] == 'rejected' for row in controls['controls'])
    for item in [*controls['sources'], *controls['input_files']]:
        packet.bound(item)
    return dict(complete_requests=22, cyclic_calls=8, engine_time_attribution=None,
        large_entry_reads_before=sum(row['baseline_large_entry_reads'] for row in pairs),
        large_entry_reads_after=sum(row['candidate_large_entry_reads'] for row in pairs),
        diagnostic_corruptions_rejected=14)


def replay_saved(report, data, temporary):
    # macOS exposes the default temporary root through /var -> /private/var.
    # Compare module origins against the same canonical root used by resolve().
    temporary = temporary.resolve()
    packet = Packet(report, data)
    selection = packet.read('preparation/index-cohort-selection-r1.json')
    assert digest(data['preparation/index-cohort-selection-r1.json']) == SELECTION_SHA256
    old_protocol = packet.read('current/baseline-protocol-r1.json')
    old_result = packet.read('current/baseline-verified-r1.json')
    assert digest(data['current/baseline-protocol-r1.json']) == BASELINE_PROTOCOL_SHA256
    assert digest(data['current/baseline-verified-r1.json']) == BASELINE_RESULT_SHA256
    protocol = packet.read('current/candidate-protocol-r1.json')
    result = packet.read('current/candidate-verified-r1.json')
    assert old_result['selected_cases'] == result['selected_cases'] == 2452
    assert old_result['reused_saved_cases'] == 433 and old_result['new_attempts'] == 2019
    assert result['new_attempts'] == 2452 and result['query_executions_during_replay'] == 0
    assert protocol['cases'] == selection['cases'] and len(selection['cases']) == 2452
    assert protocol['selected_count'] == 2452 and protocol['candidate_contract'] == old_protocol['candidate_contract']
    assert protocol['selection_uses_timing'] is False and protocol['complete_10k_performance_claim'] is False
    for record in (old_protocol, protocol, old_result, result):
        assert record['primary_clock'] == 'engine_execution_seconds'
        assert record['attempts_per_case'] == 1 and record['warmups'] == record['requested_repeats'] == 0
        assert record['complete_10k_performance_claim'] is False
    assert packet.bound(protocol['baseline_protocol']) == data['current/baseline-protocol-r1.json']
    assert packet.bound(protocol['baseline_result']) == data['current/baseline-verified-r1.json']
    assert packet.bound(protocol['selection']) == data['preparation/index-cohort-selection-r1.json']
    candidate_proof = {row['path']: row for row in protocol['runtime_binding']['proof_files']}
    assert len(candidate_proof) == len(protocol['runtime_binding']['proof_files'])
    assert old_result['runtime_binding'] == old_protocol['runtime_binding']
    for row in old_protocol['runtime_binding']['proof_files']:
        assert candidate_proof[row['path']] == row
    assert set(report['identity_only_proof']) <= set(candidate_proof)
    for key in ('isolation', 'performance_decision', 'baseline_observations'):
        assert protocol[key] == old_protocol[key]
    for row in protocol['candidate_contract']['sources']:
        packet.bound(row)
    design, inputs = (packet.read('frozen/' + name + '.json') for name in ('design', 'inputs'))
    prepared = packet.read('frozen/native-preparation/complete.json')
    assert len(design['cases']) == 10000 and old_protocol['execution'] == protocol['execution'] == design['execution']
    for name in ('design', 'inputs', 'frozen'):
        assert packet.bound(protocol[name]) == packet.bound(old_protocol[name]) == data['frozen/' + name + '.json']
    for selected in selection['cases']:
        assert selected['case'] == design['cases'][selected['original_seq']]
    assert len({row['original_seq'] for row in selection['cases']}) == 2452
    source_bytes = replay_sources(packet)
    harness = temporary / 'harness'
    for name, raw in source_bytes.items():
        path = harness / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
    for name, raw in data.items():
        if name.endswith('/attempts.sqlite') or (name.startswith('frozen/reference-') and name.endswith('.sqlite')):
            path = temporary / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
    sys.path[:0] = [str(harness / 'scripts'), str(harness / 'python/src'), str(harness / 'benchmarks/traditional_analytics')]
    from query_surface.corpus import corpus
    from query_surface.expanded_catalog import build, spaces
    from query_surface.expanded_freeze import unpack
    from query_surface.expanded_report import raw_files, replay
    from query_surface.expanded_run import request
    from query_timing_report import query_phase_timings
    assert not any(name == 'shardloom._native' or name.startswith('shardloom._native.') for name in sys.modules)
    for name, module in list(sys.modules.items()):
        if name.startswith(('query_surface', 'shardloom')) or name in (
                'query_timing_report', 'native_workflow_protocol', 'local_uat_storage', 'resources',
                'run_clickbench_query_uat', 'timed_native_command', 'clickbench_reference_packet', 'clickbench_native_sql'):
            path = Path(module.__file__).resolve()
            assert path.is_relative_to(harness)
            assert path.read_bytes() == source_bytes[path.relative_to(harness).as_posix()]
    specifications, originals = {spec.name: spec for spec in spaces()}, {item.id: item for item in corpus(interactions='all', include_pending=False)}
    baseline_rows = [json.loads(line) for line in gzip.decompress(packet.bound(old_result['case_rows'])).splitlines()]
    candidate_rows = [json.loads(line) for line in gzip.decompress(packet.bound(result['case_rows'])).splitlines()]
    assert len(baseline_rows) == len(candidate_rows) == 2452
    reused = packet.read('current/baseline-reuse-r1.json')
    reused_by_ordinal = {row['cohort_ordinal']: row for row in reused['rows']}
    assert len(reused['rows']) == len(reused_by_ordinal) == old_protocol['reused_count'] == 433
    missing = old_protocol['missing_cases']
    assert len(missing) == old_protocol['missing_count'] == 2019
    assert [dict(cohort_ordinal=ordinal, original_seq=row['original_seq'], case=row['case'])
        for ordinal, row in enumerate(selection['cases']) if ordinal not in reused_by_ordinal] == missing
    captures = {}
    for role, prefix, count in (
        ('repair', 'repair/correctness-cohort-r1', 517),
        ('missing', 'current/baseline-missing-r1', 2019),
        ('candidate', 'current/candidate-r1', 2452),
    ):
        done, binding = packet.read(prefix + '/complete.json'), packet.read(prefix + '/binding.json')
        assert done['status'] == 'complete_observation' and done['stage'] == 'native'
        assert done['total'] == done['attempt_slots'] == count
        assert done['posthash_verified'] and done['attempts_per_case'] == 1 and done['requested_repeats'] == 0
        assert binding['contract'] == {key: done[key] for key in ('frozen', 'stage', 'prerequisites', 'updated')}
        assert packet.bound(done['ledger']) == data[prefix + '/attempts.sqlite']
        assert packet.bound(done['binding']) == data[prefix + '/binding.json']
        guard = packet.guard(prefix)
        assert guard['query_workload'] and guard['build_inventory_policy'] == guard['evidence_inventory_policy'] == 'before_and_after_only'
        if role == 'candidate':
            assert guard['base'] == guard['head'] == BASE
            assert guard['build_sources'] == report['candidate_source_snapshot']
            assert guard['active_output'] == ORIGINAL_ROOT + '/candidate-r1'
            assert guard['command'] == ['/Users/dylan/.cache/shardloom/query-surface-polars-2.1.0/bin/python',
                '-B', ORIGINAL_ROOT + '/tools/run_index_candidate.py']
        captures[role] = dict(prefix=prefix, done=done, runtime=done['updated']['runtime'])
    assert captures['missing']['done']['updated'] == old_protocol['runtime_binding']
    assert captures['candidate']['done']['updated'] == protocol['runtime_binding'] == result['runtime_binding']
    assert captures['missing']['runtime'] == captures['repair']['runtime'] == old_result['runtime_binding']['runtime']
    outcomes, families, totals = {}, {}, {}
    with ExitStack() as stack:
        def database(name):
            connection = sqlite3.connect('file:' + str(temporary / name) + '?mode=ro', uri=True)
            stack.callback(connection.close)
            return connection
        refs = {profile: database('frozen/reference-' + profile + '/expectations.sqlite')
            for profile in {row['case']['profile'] for row in selection['cases']}}
        dbs = {role: database(row['prefix'] + '/attempts.sqlite') for role, row in captures.items()}
        for role, count in (('repair', 517), ('missing', 2019), ('candidate', 2452)):
            assert dbs[role].execute('SELECT count(*),min(seq),max(seq),count(distinct id) FROM attempt').fetchone() == (count, 0, count - 1, count)
        for role, saved in (('baseline', baseline_rows), ('candidate', candidate_rows)):
            counts, family_values, seconds = Counter(), defaultdict(list), []
            missing_ordinal = 0
            for ordinal, (selected, archived) in enumerate(zip(selection['cases'], saved)):
                case = selected['case']
                assert archived['cohort_ordinal'] == ordinal and archived['original_seq'] == selected['original_seq']
                assert archived['id'] == case['id'] and archived['case_identity'] == case['case_identity']
                if role == 'candidate':
                    capture_role, seq = 'candidate', ordinal
                elif ordinal in reused_by_ordinal:
                    capture_role, seq = 'repair', reused_by_ordinal[ordinal]['repair_ordinal']
                else:
                    capture_role, seq = 'missing', missing_ordinal
                    missing_ordinal += 1
                capture = captures[capture_role]
                entry = dbs[capture_role].execute('SELECT seq,id,status,packet FROM attempt WHERE seq=?', (seq,)).fetchone()
                assert entry is not None and entry[:2] == (seq, case['id'])
                item = build(case, specifications) if 'cell' in case else originals[case['id']]
                reference = refs[case['profile']].execute('SELECT identity,value FROM expected WHERE id=?', (case['id'],)).fetchone()
                assert reference and reference[0] == item.identity == case['case_identity']
                expected = unpack(reference[1])
                config = dict(inputs, runtime=capture['runtime'])
                observed = replay(entry, case, 'native', expected, item, config, prepared, design['execution'])
                assert observed == archived['result'] and observed['status'] == entry[2]
                files = raw_files(unpack(entry[3])) if entry[3] is not None else {}
                if files:
                    assert json.loads(files['one.job.json']) == request(case, 'native', config, prepared, expected, design['execution'])
                provenance = archived['provenance']
                assert provenance['ledger'] == capture['done']['ledger']['path'] and provenance['source_seq'] == seq
                assert provenance['packet'] == (identity(entry[3]) if entry[3] is not None else None)
                assert provenance['job_sha256'] == (digest(files['one.job.json']) if files else None)
                assert provenance['independent_expected_blob_sha256'] == digest(reference[1])
                if role == 'candidate':
                    assert archived['baseline_engine_seconds'] == baseline_rows[ordinal]['result']['engine_seconds']
                counts[observed['status']] += 1
                if observed['status'] != 'passed':
                    assert role == 'candidate'
                    continue
                observation = json.loads(files['one.stdout.json'])['observation']
                clocks = observation['clocks']
                fields = query_phase_timings(observation['fields'], clocks['complete_workflow_seconds'])
                assert clocks['engine_timing_reported'] is True
                assert fields['engine_execution_seconds'] == clocks['engine_execution_seconds'] == observed['engine_seconds']
                assert fields['result_serialization_seconds'] == clocks['result_serialization_seconds']
                value = observed['engine_seconds']
                assert type(value) in (int, float) and math.isfinite(value) and value >= 0
                seconds.append(value)
                family_values[case['family']].append(value)
            assert role == 'candidate' or missing_ordinal == 2019
            outcomes[role], families[role] = dict(counts), family_values
            totals[role] = math.fsum(seconds) if counts == {'passed': 2452} else None
    assert outcomes['baseline'] == old_result['outcomes'] == {'passed': 2452}
    assert totals['baseline'] == old_result['engine_execution_seconds'] == 320.508980218
    assert outcomes['candidate'] == result['outcomes'] == captures['candidate']['done']['outcomes']
    assert totals['candidate'] == result['engine_execution_seconds']
    passed = outcomes['candidate'] == {'passed': 2452}
    disposition = ('retain_candidate' if totals['candidate'] < totals['baseline'] else 'drop_candidate_no_gain') if passed else 'drop_candidate_invalid_cohort'
    assert report['disposition'] == result['frozen_rule_disposition'] == disposition
    assert result['complete_performance_result_valid'] == result['all_selected_results_correct'] == result['all_engine_clocks_valid'] == result['native_no_fallback'] == passed
    assert result['baseline_engine_execution_seconds'] == totals['baseline']
    assert result['engine_seconds_saved'] == (totals['baseline'] - totals['candidate'] if passed else None)
    assert result['observed_engine_reduction_percent'] == (100 * (totals['baseline'] - totals['candidate']) / totals['baseline'] if passed else None)
    assert report['baseline'] == dict(cases=2452, reused_saved_cases=433, new_attempts=2019,
        engine_execution_seconds=totals['baseline'])
    assert report['candidate'] == {key: result[key] for key in ('selected_cases', 'outcomes',
        'engine_execution_seconds', 'engine_seconds_saved', 'observed_engine_reduction_percent',
        'complete_performance_result_valid')}
    for name, values in families['baseline'].items():
        assert old_result['families'][name] == dict(cases=len(values), engine_execution_seconds=math.fsum(values))
    for name, values in families['candidate'].items():
        assert result['families'][name] == dict(cases=len(values), engine_execution_seconds=math.fsum(values),
            baseline_engine_execution_seconds=old_result['families'][name]['engine_execution_seconds'])
    return dict(baseline_cases=2452, reused_baseline_cases=433, new_baseline_cases=2019,
        candidate_cases=2452, outcomes=outcomes, engine_execution_seconds=totals,
        disposition=disposition, query_executions=0, complete_10k_performance_claim=False)


def verify_controls(packet_path, report, data):
    controls = json.loads((packet_path / 'corruption-controls.json').read_bytes())
    assert controls['status'] == 'passed' and controls['query_executions'] == 0
    assert controls['acceptance_manifest'] == identity((packet_path / 'acceptance.json').read_bytes())
    assert controls['positive_controls'] == 2 and controls['rejected_corruptions'] == len(CONTROL_NAMES)
    assert len(controls['controls']) == len(CONTROL_NAMES) + 2
    assert {row['name'] for row in controls['controls'] if row['outcome'] == 'rejected'} == CONTROL_NAMES
    assert {row['name'] for row in controls['controls'] if row['outcome'] == 'passed'} == {'source_bound', 'relocated_source_only'}
    for name in ('verify_index_packet.py', 'check_index_packet_controls.py'):
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
    with tempfile.TemporaryDirectory(prefix='shared-index-evidence-') as temporary:
        replay = replay_saved(report, data, Path(temporary))
    controls = verify_controls(packet_path, report, data) if require_controls else None
    return dict(status='passed_source_bound_replay', diagnostics=diagnostics, controls=controls, **replay)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo-root', type=Path)
    parser.add_argument('--require-controls', action='store_true')
    args = parser.parse_args()
    print(json.dumps(verify(Path(__file__).resolve().parent, args.repo_root, args.require_controls), indent=2, sort_keys=True))
