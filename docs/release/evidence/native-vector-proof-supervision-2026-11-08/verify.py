"""Replay the bounded proof-runner follow-up without executing native code."""
import argparse
import ast
import hashlib
import importlib.util
import io
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import tarfile

HEAD = 'c87ef29fcb528e47d8c41d84fc6cc4d105d00169'
CORE = 'native-vector-expressions-2026-11-08'
CORE_FILES = {
    'README.md': {'bytes': 5412, 'sha256': '2289e688aa3edcc4f5d34a88406d01f368a7db6ea99d3a007143191d26ef0447'},
    'acceptance.json': {'bytes': 2316095, 'sha256': '8961036b28b7e5dc07485d6c3118eee16c673affbc873ed90d6d88148039ddff'},
    'corruption-controls.json': {'bytes': 74284, 'sha256': 'f50674e1e07a62c2fe701150d5eafdec3c29d029396e4efc9d2d990f736bcc9d'},
    'verification.tar.xz': {'bytes': 70665220, 'sha256': '53cecdfdf3c50eac4644beddb99239546c30c9689f2906341c7e48a04b455088'},
    'verify.py': {'bytes': 28204, 'sha256': '1c8d367156cd4141dc0b053a79c61db5be837098fbb339a1ddba0468b53902f3'},
}
CHANGED_HARNESS = {
    'scripts/prove_native_python_wheel.py': '3517ba61a1ee21ab50079536fec9f0f0c54a1f213723e61b408933cde09628eb',
    'scripts/test_prove_native_python_wheel.py': '6f521eb796315b5ec81f7c1a4a60f623d80335da0f9c150419a1e47553f63c70',
}
ROLES = {
    'unit': 'vector-proof-budget-unit-r1.json',
    'installed': 'vector-installed-wheel-r3.json',
    'docs': 'vector-docs-r2.json',
    'doc_contracts': 'vector-doc-contracts-r2.json',
    'format': 'vector-proof-format-r1.json',
}
CASES = ('original', 'relocated', 'base_changed', 'compiled_source_changed',
         'unit_omitted', 'unit_failed', 'unit_test_omitted', 'installed_skipped',
         'installed_test_omitted', 'budget_source_changed', 'budget_log_changed',
         'original_failure_omitted', 'unsafe_archive_path', 'archive_checksum')
CI_FILES = {
    'attempt1-checks-final.json', 'attempt1-completion.json', 'attempt1-jobs-final.json',
    'attempt1-package-gate-log.json', 'attempt1-readiness-log.json',
    'attempt2-completion.json', 'attempt2-initial-jobs.json', 'attempt2-final-jobs.json',
    'attempt2-final-check-runs.json', 'attempt2-final-completion.json',
    'jobs-post-failure-r1.json', 'macos-py313-passed.zip', 'macos-py313-report.json',
    'prior-py313-artifact-response.json', 'prior-py313-passed.zip', 'prior-py313-report.json',
    'py310-integration.json', 'py310-integration.log', 'py310-passed.zip', 'py310-report.json',
    'py313-artifact-response.json', 'py313-failed.zip', 'py313-integration.log',
    'py313-job.json', 'py313-job.log', 'py313-report.json',
    'py313-retry-artifact-response.json', 'py313-retry-failed.zip',
    'py313-retry-integration.log', 'py313-retry-job-log.json', 'py313-retry-report.json',
    'py313-retry-request-r1.json', 'py313-retry-request-r2.json', 'triage-r1.json',
}


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def identity(raw):
    return dict(bytes=len(raw), sha256=sha(raw))


def encoded(value):
    return (json.dumps(value, indent=2) + '\n').encode()


def fingerprint(value):
    return sha(json.dumps(value, sort_keys=True, separators=(',', ':')).encode())


def read_archive(packet, report, limit):
    raw = (packet / 'verification.tar.xz').read_bytes()
    assert len(raw) < limit and sha(raw) == report['archive_sha256']
    rows = report['members']
    assert len(rows) == len({row['path'] for row in rows})
    assert all(isinstance(row['bytes'], int) and row['bytes'] >= 0 for row in rows)
    assert sum(row['bytes'] for row in rows) < 512 << 20
    result = {}
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:xz') as archive:
        members = archive.getmembers()
        assert [member.name for member in members] == [row['path'] for row in rows]
        for member, row in zip(members, rows, strict=True):
            path = PurePosixPath(member.name)
            assert member.isfile() and not path.is_absolute() and '..' not in path.parts
            assert path.as_posix() == member.name and member.size == row['bytes']
            content = archive.extractfile(member).read()
            assert identity(content) == {key: row[key] for key in ('bytes', 'sha256')}
            result[member.name] = content
    return result


def verify(packet, core_packet, repo_root=None, require_controls=False):
    assert {path.name for path in core_packet.iterdir()} == set(CORE_FILES)
    core_raw = {name: (core_packet / name).read_bytes() for name in CORE_FILES}
    assert {name: identity(raw) for name, raw in core_raw.items()} == CORE_FILES
    # Only import the byte-pinned original verifier, retaining its original policy.
    spec = importlib.util.spec_from_file_location('vector_core_verifier', core_packet / 'verify.py')
    core = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(core)
    original = core.verify(core_packet, require_controls=True)
    assert original['status'] == 'passed' and not original['repository_checked']
    base = json.loads(core_raw['acceptance.json'])
    old = read_archive(core_packet, base, 90 << 20)
    report = json.loads((packet / 'acceptance.json').read_bytes())
    assert report['schema_version'] == 'shardloom.vector_proof_followup.v1'
    assert report['status'] == 'local_acceptance_passed' and report['validated_parent'] == HEAD
    assert report['core_files'] == CORE_FILES and report['roles'] == ROLES
    data = read_archive(packet, report, 12 << 20)
    assert (packet / 'verify.py').read_bytes() == data['tools/verify_vector_proof_followup.py']
    lookup = {sha(raw): raw for raw in old.values()}
    lookup.update({sha(raw): raw for raw in core_raw.values()})
    lookup.update({sha(raw): raw for raw in data.values()})

    def blob(info):
        raw = lookup[info['sha256']]
        assert identity(raw) == info
        return raw

    def read(name):
        return json.loads(data[name])

    def inventory(value):
        for name, info in value.items():
            path = PurePosixPath(name)
            assert not path.is_absolute() and '..' not in path.parts
            if 'symlink' in info:
                assert sha(info['symlink'].encode()) == info['sha256']
            elif 'sha256' in info:
                blob(info)
            else:
                assert info == {'absent': True}

    source = report['source_snapshot']
    assert set(source) == set(base['source_snapshot'])
    inventory(source)
    assert fingerprint(source) == report['source_fingerprint_sha256']
    deltas = core.source_deltas(base['source_snapshot'], source)
    assert report['source_deltas'] == deltas
    assert set(CHANGED_HARNESS) <= set(deltas) <= set(CHANGED_HARNESS) | core.STATUS_PROSE
    for name, expected in CHANGED_HARNESS.items():
        assert source[name]['sha256'] == expected
    proof = blob(source['scripts/prove_native_python_wheel.py'])
    old_proof = blob(base['source_snapshot']['scripts/prove_native_python_wheel.py'])

    def constants(raw):
        return {node.targets[0].id: ast.literal_eval(node.value) for node in ast.parse(raw).body
                if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id in {'IMPORT_CHECK', 'RUN_TESTS'}}

    assert constants(proof) == constants(old_proof) and len(constants(proof)) == 2
    receipts = {role: read(name) for role, name in ROLES.items()}
    for role, receipt in receipts.items():
        assert receipt['status'] == 'passed' and receipt['returncode'] == 0, role
        assert receipt['head'] == HEAD and receipt['base'] == core.BASE
        assert receipt['fixed_source_validation'] and receipt['process_group_drained'] and receipt['direct_child_reaped']
        assert receipt['driver_sha256'] == sha(old['current/tools/guard_vector_expressions.py'])
        assert receipt['prior_closeout_sha256'] == sha(old['prior/provider-closeout-r1.json'])
        assert receipt['log_sha256'] == sha(data[ROLES[role].replace('.json', '.log')])
        inventory(receipt['build_sources'])
        assert set(receipt['build_sources']) == set(source)
        assert len(source) == receipt['build_source_path_count']
        assert fingerprint(receipt['build_sources']) == receipt['build_source_fingerprint_sha256']
        allowed = set() if role in {'docs', 'doc_contracts', 'format'} else core.STATUS_PROSE
        assert set(core.source_deltas(receipt['build_sources'], source)) <= allowed
        for phase in ('pre_execution', 'post_execution'):
            observed = receipt['build_source_verification'][phase]
            assert observed['head'] == HEAD and observed['head_matches'] and observed['path_set_matches']
            assert observed['changed_paths'] == [] and observed['path_count'] == len(source)
            assert observed['fingerprint_sha256'] == fingerprint(receipt['build_sources'])
        for name, info in receipt['tool_source_inventory'].items():
            raw = blob(info)
            if name.endswith('/scripts/prove_native_python_wheel.py'):
                assert raw == proof
        if role in {'format', 'doc_contracts'}:
            assert receipt['command'] == core.CARGO[role]
    assert receipts['unit']['command'][1:] == ['-B', '-m', 'unittest', '-v',
        'test_prove_native_python_wheel', 'test_ci_native_package']
    modules = ['test_prove_native_python_wheel', 'test_ci_native_package']
    unit_log = data[ROLES['unit'].replace('.json', '.log')]
    methods = sorted(method for module in modules for method in core.test_methods(blob(source['scripts/' + module + '.py']), module))
    assert len(methods) == 21 and core.successful_methods(unit_log, modules) == methods
    assert b'\nOK\n' in unit_log and b'... skipped' not in unit_log
    assert core.rust_counts(data[ROLES['doc_contracts'].replace('.json', '.log')]) == dict(passed=171, failed=0, ignored=0)
    docs_command = receipts['docs']['command']
    assert len(docs_command) == 7 and docs_command[1] == '-B'
    assert docs_command[3:6] == ['--tag', 'r2', '--runtime']
    assert docs_command[2].endswith('/tools/check_vector_docs.py') and docs_command[-1].endswith('/runtime-r2')
    assert receipts['docs']['tool_source_inventory'][docs_command[2]] == identity(old['current/tools/check_vector_docs.py'])
    assert read('public-status-docs-r2.json')['status'] == 'passed'
    assert read('contribution-governance-r2.json')['status'] == 'passed'
    assert receipts['installed']['command'][2] == 'scripts/prove_native_python_wheel.py'
    assert '--existing-wheel' in receipts['installed']['command'] and '--build-report' in receipts['installed']['command']
    installed = read('installed-proof-r3/report.json')
    assert installed['status'] == installed['uninstall_status'] == 'passed'
    assert installed['installed_load']['isolated_install'] and installed['installation_performed']
    assert installed['integration'] == read('installed-proof-r3/integration.json') == base['installed_package']
    assert installed['integration'] == dict(tests=397, failures=0, errors=0, skipped=0, status='passed')
    assert {key: installed['integrity'][key] for key in base['runtime_and_wheel']} == base['runtime_and_wheel']
    assert installed['artifact_reuse']['wheel_sha256'] == base['runtime_and_wheel']['wheel_sha256']
    assert installed['artifact_reuse']['build_report_sha256'] == sha(old['current/' + base['installed_report']])
    assert [step['name'] for step in installed['steps']] == [
        'create_environment', 'install_optional_consumer', 'install', 'load', 'integration', 'uninstall', 'verify_uninstalled']
    for step in installed['steps']:
        assert step['returncode'] == 0 and step['log_sha256'] == sha(data['installed-proof-r3/' + step['log']])
        assert step['timeout_seconds'] == (1200 if step['name'] == 'integration' else 600)
    integration = data['installed-proof-r3/integration.log']
    old_integration = old['current/' + str(PurePosixPath(base['installed_report']).parent) + '/integration.log']
    test_lines = lambda raw: sorted(re.findall(rb'^test_\S+ \([^\n]+\) \.\.\. ok$', raw, re.MULTILINE))
    assert len(test_lines(integration)) == 397 and test_lines(integration) == test_lines(old_integration)
    assert re.search(rb'Ran 397 tests in [0-9.]+s\n\nOK\n', integration)
    assert b'... skipped' not in integration
    assert {name.removeprefix('ci-failures-r1/') for name in data if name.startswith('ci-failures-r1/')} == CI_FILES
    assert report['ci_failure_files'] == {name: identity(data['ci-failures-r1/' + name]) for name in sorted(CI_FILES)}
    failed_wheels = []
    for prefix in ('py313', 'py313-retry'):
        failed = read('ci-failures-r1/' + prefix + '-report.json')
        assert failed['status'] == 'failed' and 'TimeoutExpired' in failed['error'] and '600' in failed['error']
        assert 'integration' not in failed and failed['steps'][-1]['name'] == 'integration'
        raw = data['ci-failures-r1/' + prefix + '-integration.log']
        assert failed['steps'][-1]['log_sha256'] == sha(raw)
        assert len(test_lines(raw)) == 321
        failed_wheels.append(failed['integrity']['wheel_sha256'])
    passed = read('ci-failures-r1/py310-report.json')
    assert failed_wheels == [passed['integrity']['wheel_sha256']] * 2
    assert passed['status'] == 'passed' and passed['integration'] == base['installed_package']
    final = read('ci-failures-r1/attempt2-final-completion.json')
    assert final['head_sha'] == HEAD and final['workflow_id'] == 42525861676 and final['attempt'] == 2
    assert final['workflow_status'] == 'completed' and final['workflow_conclusion'] == 'failure'
    for name, info in final['evidence'].items():
        assert identity(data['ci-failures-r1/' + name]) == info
    checks = read('ci-failures-r1/attempt2-final-check-runs.json')['check_runs']
    assert len(checks) == 58 and all(row['head_sha'] == HEAD and row['status'] == 'completed' for row in checks)
    assert sum(row['conclusion'] == 'success' for row in checks) == 55
    assert {row['name'] for row in checks if row['conclusion'] == 'failure'} == {
        'Native Python wheel (py313_ubuntu)', 'Native Python wheel acceptance', 'Release readiness reports'}
    launch = read('vector-installed-wheel-r3-launch.json')
    assert launch['status'] == 'failed_before_child_launch' and not launch['runtime_execution_performed']
    assert report['dispositions']['original_ci_attempts'] and report['dispositions']['local_launch']
    assert report['hosted_integration'] == 'pending' and not report['native_runtime_rebuilt']
    assert not report['publication_attempted'] and not report['performance_claim'] and not report['whole_item_19_complete']
    if repo_root:
        paths = set()
        for command in (['git', 'ls-files', '--cached', '-z'], ['git', 'ls-files', '--others', '--exclude-standard', '-z']):
            paths.update(filter(None, subprocess.check_output(command, cwd=repo_root).decode().split('\0')))
        paths = {name for name in paths if not name.startswith(core.EXCLUDED) and '.git' not in Path(name).parts}
        assert paths == set(source)
        for name, info in source.items():
            path = repo_root / name
            if 'symlink' in info:
                assert path.is_symlink() and str(path.readlink()) == info['symlink']
            elif info.get('absent'):
                assert not path.exists() and not path.is_symlink()
            else:
                assert path.is_file() and not path.is_symlink() and identity(path.read_bytes()) == info, name
    controls_path = packet / 'corruption-controls.json'
    if require_controls:
        assert controls_path.is_file()
    if controls_path.exists():
        controls = json.loads(controls_path.read_bytes())
        saved = controls['receipt']
        assert controls['status'] == saved['status'] == 'passed'
        assert saved['driver_sha256'] == sha(data['tools/check_vector_proof_controls.py'])
        expected = {name: identity((packet / name).read_bytes()) for name in ('acceptance.json', 'verification.tar.xz', 'verify.py')}
        assert saved['original_packet'] == expected and saved['core_files'] == CORE_FILES
        assert [row['case'] for row in saved['cases']] == list(CASES)
        assert saved['positive_controls'] == 2 and saved['rejected_controls'] == len(CASES) - 2
        for index, row in enumerate(saved['cases']):
            raw = controls['logs'][row['case']].encode()
            assert identity(raw) == row['log_identity'] and row['originals_unchanged']
            assert row['returncode'] == (0 if index < 2 else 1)
            if index < 2:
                observed = json.loads(raw)
                assert observed['status'] == 'passed' and not observed['repository_checked'] and not observed['native_execution_repeated']
            else:
                assert b'AssertionError' in raw or b'KeyError' in raw
    return dict(status='passed', core_replay=original, followup_members=len(data), harness_tests=21,
        installed_tests=397, source_fingerprint_sha256=fingerprint(source), repository_checked=repo_root is not None,
        controls_verified=controls_path.exists(), native_execution_repeated=False, hosted_integration='pending')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packet', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--core-packet', type=Path)
    parser.add_argument('--repo-root', type=Path)
    parser.add_argument('--require-controls', action='store_true')
    args = parser.parse_args()
    print(json.dumps(verify(args.packet, args.core_packet or args.packet.parent / CORE,
                            args.repo_root, args.require_controls), sort_keys=True))
