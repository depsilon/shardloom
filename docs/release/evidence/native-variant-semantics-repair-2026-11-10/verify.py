"""Replay the validation-only Variant repair and its immutable original evidence."""
import argparse
from collections import Counter
import copy
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import tarfile
import zipfile

HEAD = '847a80611703af5471402e436040424aa08c5f15'
ORIGINAL = 'native-variant-expressions-2026-11-10'
ROOT = '/Users/dylan/.codex/worktrees/native-vector-representations/shardloom-local-repo'
OUT = '/Users/dylan/LocalData/shardloom/variant-expressions-2026-11-10'
CHANGED = {
    'docs/release/v1-correctness-conformance-matrix.json',
    'docs/status/admitted-semantics-matrix.json',
    'docs/status/admitted-semantics-matrix.md',
    'python/tests/test_release_report_producer_contracts.py',
    'python/tests/test_release_scripts.py',
    'scripts/check_admitted_semantics_matrix.py',
    'scripts/check_release_readiness.py',
    'scripts/check_v1_correctness_conformance.py',
    'shardloom-contract-tests/tests/release_readiness_metadata.rs',
}
TEST_DELTA = {'shardloom-contract-tests/tests/release_readiness_metadata.rs'}
SEMANTICS_DELTA = TEST_DELTA | {
    'python/tests/test_release_report_producer_contracts.py',
    'python/tests/test_release_scripts.py', 'scripts/check_release_readiness.py',
}
ROLES = {
    'semantics': 'semantics-repair-r1', 'release_tests': 'release-script-repair-r3',
    'consumers': 'semantics-consumers-repair-r2', 'format': 'format-repair-r1',
    'contract_clippy': 'contract-clippy-repair-r1',
    'doc_contracts': 'doc-contracts-repair-r2', 'docs': 'docs-repair-r2',
}
FAILURES = ('release-script-repair-r2', 'semantics-consumers-repair-r1', 'doc-contracts-repair-r1')
CONTROLS = ('original', 'relocated', 'archive_byte', 'production_delta',
    'hosted_failure_missing', 'local_failure_missing', 'semantic_fallback',
    'failed_test_receipt', 'runtime_identity', 'original_packet_changed', 'source_scope_missing')
EXCLUDED = ('docs/benchmarks/evidence/', 'docs/release/evidence/', 'docs/dependencies/evidence/')


def identity(raw):
    return dict(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())


def fingerprint(value):
    return identity(json.dumps(value, sort_keys=True, separators=(',', ':')).encode())['sha256']


def deltas(before, after):
    return {name: dict(before=before.get(name), after=after.get(name))
        for name in sorted(set(before) | set(after)) if before.get(name) != after.get(name)}


def read_archive(packet, report):
    raw = (packet / 'verification.tar.xz').read_bytes()
    assert len(raw) < 16 << 20 and identity(raw) == report['archive']
    rows = report['members']
    assert len(rows) == len({row['path'] for row in rows}) and sum(row['bytes'] for row in rows) < 96 << 20
    data = {}
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:xz') as archive:
        entries = archive.getmembers()
        assert [row['path'] for row in rows] == [entry.name for entry in entries]
        for row, entry in zip(rows, entries):
            path = PurePosixPath(entry.name)
            assert entry.isfile() and not path.is_absolute() and '..' not in path.parts
            assert path.as_posix() == entry.name and entry.size == row['bytes']
            body = archive.extractfile(entry).read()
            assert identity(body) == {key: row[key] for key in ('bytes', 'sha256')}
            data[entry.name] = body
    return data


def verify(packet, repo=None, require_controls=False):
    report = json.loads((packet / 'acceptance.json').read_bytes())
    assert report['schema_version'] == 'shardloom.variant_semantics_repair.v1'
    assert report['status'] == 'local_acceptance_passed' and report['original_head'] == HEAD
    assert report['roles'] == ROLES and report['changed_paths'] == sorted(CHANGED)
    assert report['production_inputs_changed'] is report['broad_native_validation_repeated'] is False
    assert report['semantics_execution_performed'] is True
    assert report['performance_claim'] is report['publication_attempted'] is False
    original = packet.parent / ORIGINAL
    assert set(report['original_packet']) == {'README.md', 'acceptance.json', 'verification.tar.xz', 'verify.py', 'corruption-controls.json'}
    for name, info in report['original_packet'].items():
        assert identity((original / name).read_bytes()) == info, name
    command = [sys.executable, '-I', '-B', str(original / 'verify.py'), '--require-controls']
    prior = subprocess.run(command, check=True, capture_output=True, text=True, timeout=180)
    replay = json.loads(prior.stdout)
    assert replay['status'] == 'passed' and replay['controls_verified'] and not replay['repository_checked']
    assert replay['roles'] == 14 and replay['installed_tests'] == 441 and replay['original_failures'] == 8
    accepted = json.loads((original / 'acceptance.json').read_bytes())
    with tarfile.open(original / 'verification.tar.xz', mode='r:xz') as archive:
        original_guard = json.loads(archive.extractfile('current/' + accepted['roles']['docs']).read())
    source = report['final_source']
    assert set(source) == set(accepted['source_snapshot'])
    difference = deltas(accepted['source_snapshot'], source)
    assert set(difference) == CHANGED and report['source_deltas'] == difference
    assert report['source_fingerprint_sha256'] == fingerprint(source)
    assert report['runtime_and_wheel'] == accepted['runtime_and_wheel']
    observed_runtime = report['retained_runtime_observation']
    assert set(observed_runtime) == {'cli_sha256', 'native_library_sha256', 'wheel_sha256'}
    assert {name: row['sha256'] for name, row in observed_runtime.items()} == report['runtime_and_wheel']
    data = read_archive(packet, report)
    assert data['tools/verify_semantics_repair.py'] == Path(__file__).read_bytes()
    known = {(row['bytes'], row['sha256']) for row in accepted['members']}
    known.update((info['bytes'], info['sha256']) for info in report['original_packet'].values())
    known.update((len(raw), identity(raw)['sha256']) for raw in data.values())
    for name in CHANGED:
        assert identity(data['source-blobs/' + source[name]['sha256']]) == source[name]

    def read(name):
        return json.loads(data[name])

    def guard(stem, passed=True):
        receipt = read(stem + '.json')
        assert receipt['status'] == ('passed' if passed else 'failed')
        assert (receipt['returncode'] == 0) is passed
        assert receipt['head'] == HEAD and receipt['process_group_drained'] and receipt['direct_child_reaped']
        assert not receipt['timed_out'] and receipt['interrupted_signal'] is None
        for field in ('input_generations', 'retained_generations', 'prior_retained_runtime_and_wheel',
                      'accepted_uuid_closeout', 'driver_sha256', 'supervisor_sha256'):
            assert receipt[field] == original_guard[field], field
        assert receipt['fixed_source_validation'] and receipt['source_validation_mode'] == 'fixed_source_validation'
        assert not receipt['allow_lockfile_update'] and not receipt['root_cargo_lock_hashes']['changed']
        inventory = receipt['build_sources']
        assert fingerprint(inventory) == receipt['build_source_fingerprint_sha256']
        for phase in ('pre_execution', 'post_execution'):
            row = receipt['build_source_verification'][phase]
            assert row['head'] == HEAD and row['head_matches'] and row['path_set_matches']
            assert not row['changed_paths'] and row['path_count'] == len(inventory)
            assert row['fingerprint_sha256'] == fingerprint(inventory)
        for inventory in (inventory, receipt['tool_source_inventory'], receipt['changed_source_inventory']):
            for info in inventory.values():
                if 'bytes' in info:
                    assert (info['bytes'], info['sha256']) in known
        assert identity(data[stem + '.log'])['sha256'] == receipt['log_sha256']
        return receipt

    for role, stem in ROLES.items():
        receipt = guard(stem)
        allowed = SEMANTICS_DELTA if role == 'semantics' else TEST_DELTA if role == 'release_tests' else set()
        assert set(deltas(receipt['build_sources'], source)) == allowed
        assert not set(receipt['build_sources']).symmetric_difference(source)
    for stem in FAILURES:
        guard(stem, passed=False)
    assert set(report['failure_dispositions']) == set(FAILURES) | {'hosted_original'}
    assert all(isinstance(value, str) and value for value in report['failure_dispositions'].values())
    tests = data[ROLES['release_tests'] + '.log']
    assert b'Ran 184 tests in ' in tests and b'\nOK\n' in tests and b'... skipped' not in tests
    for test in ('test_conformance_counts_cover_actual_admitted_fixture_inventory',
                 'test_readiness_accepts_admitted_diagnostics_and_rejects_stale_counts'):
        assert re.search(rb'^' + test.encode() + rb' \([^\n]+\) \.\.\. ok$', tests, re.M)
    log = data[ROLES['doc_contracts'] + '.log']
    counts = re.findall(rb'test result: ok\. (\d+) passed; (\d+) failed;', log)
    assert counts and all(int(row[1]) == 0 for row in counts)
    assert b'admitted_semantics_matrix_validator_is_wired_into_release_readiness ... ok' in log
    commands = {role: read(stem + '.json')['command'] for role, stem in ROLES.items()}
    assert commands['format'] == ['cargo', 'fmt', '--all', '--', '--check']
    assert commands['doc_contracts'] == ['cargo', 'test', '--offline', '--locked', '-p', 'shardloom-contract-tests', '--all-targets']
    assert commands['contract_clippy'] == ['cargo', 'clippy', '--offline', '--locked', '-p', 'shardloom-contract-tests', '--all-targets', '--', '-D', 'warnings']
    assert commands['release_tests'][1:] == ['-B', '-m', 'unittest', 'python.tests.test_release_scripts', 'python.tests.test_release_report_producer_contracts', '-v']
    assert commands['consumers'][1:] == ['-B', OUT + '/tools/check_semantics_repair_consumers.py']
    assert commands['docs'][1:] == ['-B', OUT + '/tools/check_docs.py', '--tag', 'repair-r2', '--runtime', 'runtime-r2']
    assert commands['semantics'][2:4] == ['scripts/check_admitted_semantics_matrix.py', '--skip-build']
    assert commands['semantics'][commands['semantics'].index('--binary') + 1] == OUT + '/runtime-r2/shardloom'

    failed = read('hosted-failure-r1/admitted-semantics-matrix-report.json')
    assert failed['status'] == 'failed'
    assert [row['case_id'] for row in failed['stages'] if row['status'] != 'passed'] == ['unsupported_variant_access']
    zipped = data['hosted-failure-r1/release-runtime-core-evidence.zip']
    assert identity(zipped) == dict(bytes=2227225, sha256='f44261da8d1d2faeb0bce10494905706f5c50e92a4443df2d69cad807f84558f')
    with zipfile.ZipFile(io.BytesIO(zipped)) as archive:
        names = [name for name in archive.namelist() if name.endswith('admitted-semantics-matrix-report.json')]
        assert len(names) == 1 and archive.read(names[0]) == data['hosted-failure-r1/admitted-semantics-matrix-report.json']
    matrix = read('semantics-repair-r1/report.json')
    assert matrix['status'] == 'passed' and matrix['matrix_status'] == 'passed' and not matrix['blockers']
    expected = dict(executable_fixture_count=128, diagnostic_case_count=23, unsupported_diagnostic_count=20,
        runtime_error_diagnostic_count=1, invalid_shape_diagnostic_count=2, stage_count=153, matrix_row_count=153)
    assert all(matrix[name] == value for name, value in expected.items())
    assert matrix['fallback_attempted'] is matrix['external_engine_invoked'] is False
    stages = matrix['stages']
    assert len(stages) == len({row['case_id'] for row in stages}) == 153
    assert Counter(row['kind'] for row in stages) == dict(sql_native_decoded_reference=128,
        unsupported_diagnostic=20, invalid_shape_diagnostic=2, runtime_error_diagnostic=1, support_report=2)
    for row in stages:
        assert row['status'] == 'passed' and row['blockers'] == []
        assert row['fallback_attempted'] is row['external_engine_invoked'] is False
        assert row['artifact_ref'] == OUT + '/semantics-repair-r1/work/artifacts/' + row['case_id'] + '.json'
        assert row['artifact_ref'].removeprefix(OUT + '/') in data
        if row['kind'] == 'sql_native_decoded_reference':
            assert row['returncode'] == 0 and row['expected_output_digest'] == row['observed_output_digest']
            assert re.fullmatch(r'sha256:[a-f0-9]{64}', row['observed_output_digest'])
    variant = next(row for row in stages if row['case_id'] == 'unsupported_variant_access')
    assert variant['kind'] == 'invalid_shape_diagnostic' and variant['diagnostic_code'] == 'SL_INVALID_INPUT'
    assert variant['diagnostic_fragment'] == 'Variant functions require a native Variant operand'
    assert "'$.field'" in variant['command'] and variant['returncode'] == 2
    observed = read('semantics-repair-r1/work/artifacts/unsupported_variant_access.json')
    assert observed['fallback']['attempted'] is False and observed['fallback']['engine'] is None
    assert any(row['code'] == variant['diagnostic_code'] and variant['diagnostic_fragment'] in row['reason']
        for row in observed['diagnostics'])
    projection = read('semantics-repair-consumer-projection-r1.json')
    assert projection['original_sha256'] == identity(data['semantics-repair-r1/report.json'])['sha256']
    relocated = copy.deepcopy(matrix)
    expected_relocations = []
    for row in relocated['stages']:
        path = row['artifact_ref']
        target = 'target/admitted-semantics-matrix/artifacts/' + row['case_id'] + '.json'
        expected_relocations.append(dict(source=path, target=target, **identity(data[path.removeprefix(OUT + '/')])) )
        row['artifact_ref'] = target
    assert projection['relocations'] == expected_relocations and projection['projected_report'] == relocated
    readiness = read('semantics-repair-readiness-r1.json')
    gate = [row for row in readiness['checks'] if row['name'] == 'admitted_semantics_matrix_validator']
    assert len(gate) == 1 and not gate[0]['blockers'] and readiness['public_release_claim_allowed'] is False
    if repo:
        paths = set()
        for argv in (['git', 'ls-files', '--cached', '-z'], ['git', 'ls-files', '--others', '--exclude-standard', '-z']):
            paths.update(filter(None, subprocess.check_output(argv, cwd=repo).decode().split('\0')))
        paths = {name for name in paths if not name.startswith(EXCLUDED) and '.git' not in Path(name).parts}
        assert paths == set(source), 'source path inventory differs'
        for name, info in source.items():
            path = repo / name
            if 'symlink' in info:
                assert path.is_symlink() and str(path.readlink()) == info['symlink']
            elif info.get('absent'):
                assert not path.exists() and not path.is_symlink()
            else:
                assert not path.is_symlink() and identity(path.read_bytes()) == info, name
    if require_controls:
        controls = json.loads((packet / 'corruption-controls.json').read_bytes())
        assert controls['status'] == 'passed' and controls['verifier'] == identity(Path(__file__).read_bytes())
        assert controls['driver'] == identity(data['tools/check_semantics_repair_controls.py'])
        assert controls['original_packet'] == {name: identity((packet / name).read_bytes())
            for name in ('acceptance.json', 'verification.tar.xz', 'verify.py')}
        assert [row['case'] for row in controls['cases']] == list(CONTROLS)
        assert controls['positive_controls'] == 2 and controls['rejected_controls'] == len(CONTROLS) - 2
        for index, row in enumerate(controls['cases']):
            assert row['returncode'] == (0 if index < 2 else 1)
            assert identity(row['log'].encode()) == row['log_identity']
            if index < 2:
                assert json.loads(row['log'])['status'] == 'passed'
            else:
                assert any(marker in row['log'] for marker in ('AssertionError', 'KeyError', 'CalledProcessError'))
    return dict(status='passed', changed_paths=len(CHANGED), stages=153, release_tests=184,
        original_local_failures=8, repair_local_failures=len(FAILURES), hosted_failures=1,
        production_inputs_changed=False, repository_checked=repo is not None,
        controls_verified=require_controls, source_fingerprint_sha256=fingerprint(source))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packet', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--repo-root', type=Path)
    parser.add_argument('--require-controls', action='store_true')
    args = parser.parse_args()
    print(json.dumps(verify(args.packet, args.repo_root, args.require_controls), sort_keys=True))
