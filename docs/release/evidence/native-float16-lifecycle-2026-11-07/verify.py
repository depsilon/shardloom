"""Verify portable Float16 lifecycle evidence without rerunning native work."""
import argparse
import ast
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import re
import tarfile

NATIVE = 'shardloom-python/python-native,shardloom-vortex/native-encoded-exchange-experiment'
CARGO = {
    'format': ['cargo', 'fmt', '--all', '--', '--check'],
    'default_clippy': ['cargo', 'clippy', '--workspace', '--all-targets', '--locked', '--offline', '--', '-D', 'warnings'],
    'native_clippy': ['cargo', 'clippy', '--workspace', '--all-targets', '--features', NATIVE, '--locked', '--offline', '--', '-D', 'warnings'],
    'default_tests': ['cargo', 'test', '--workspace', '--all-targets', '--locked', '--offline'],
    'native_tests': ['cargo', 'test', '--workspace', '--all-targets', '--features', NATIVE, '--locked', '--offline'],
    'build': ['cargo', 'build', '--locked', '--offline', '--release', '--features', 'release-user-surfaces', '-p', 'shardloom-cli', '-p', 'shardloom-python'],
}
ROLES = set(CARGO) | {'features', 'freeze', 'float16_bindings', 'python_core', 'example', 'docs', 'installed'}
RUNNERS = {
    'features': 'check_float16_features.py', 'freeze': 'freeze_runtime.py',
    'python_core': 'scripts/run_python_test_shard.py', 'example': 'run_float16_example.py',
    'docs': 'check_float16_docs.py', 'installed': 'scripts/prove_native_python_wheel.py',
}
MODULES = ('test_native_float16_binding', 'test_native_float16_lifecycle_binding', 'test_native_float16_controls_binding')
LIMITATIONS = [
    'Scalar casts and optional legacy encoded reductions retain their previous admission limits.',
    'Text, Avro and ORC do not provide an exact Float16 persistence mapping.',
    'PyArrow 25.0.1 Parquet cannot independently read fixed-size lists beneath absent parents; complete native results and supported present-parent subsets are checked.',
    'Sparse and quantized representations and exact vector expressions remain separate work.',
]


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def identity(raw):
    return dict(bytes=len(raw), sha256=digest(raw))


def code_path(name):
    return not name.startswith('docs/') and Path(name).suffix in {'.rs', '.py', '.toml', '.lock'}


def rust_counts(raw):
    rows = re.findall(rb'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;', raw)
    return dict(zip(('passed', 'failed', 'ignored'),
        (sum(int(row[i]) for row in rows) for i in range(3)))) if rows else None


def test_methods(raw, module):
    return sorted(module + '.' + node.name + '.' + method.name
        for node in ast.parse(raw).body if isinstance(node, ast.ClassDef)
        for method in node.body if isinstance(method, ast.FunctionDef) and method.name.startswith('test_'))


def successful_methods(raw, modules):
    names = '|'.join(re.escape(name) for name in modules)
    matches = re.findall(r'^test_\S+ \((' + names + r')\.([^\)]+)\) \.\.\. ok$', raw.decode(), re.MULTILINE)
    return sorted(module + '.' + method for module, method in matches)


def verify(packet, repo_root=None):
    report = json.loads((packet / 'acceptance.json').read_bytes())
    assert report['schema_version'] == 'shardloom.float16_lifecycle_acceptance.v1'
    assert report['status'] == 'local_acceptance_passed'
    assert report['archive'] == 'verification.tar.xz'
    archive_raw = (packet / report['archive']).read_bytes()
    assert digest(archive_raw) == report['archive_sha256']
    members = report['members']
    assert len({row['path'] for row in members}) == len(members)
    assert sum(row['bytes'] for row in members) < 128 << 20
    data = {}
    with tarfile.open(fileobj=io.BytesIO(archive_raw), mode='r:xz') as archive:
        entries = archive.getmembers()
        assert [entry.name for entry in entries] == [row['path'] for row in members]
        for row, entry in zip(members, entries):
            path = PurePosixPath(row['path'])
            assert entry.isfile() and not path.is_absolute() and '..' not in path.parts
            assert entry.size == row['bytes']
            raw = archive.extractfile(entry).read()
            assert identity(raw) == {key: row[key] for key in ('bytes', 'sha256')}
            data[row['path']] = raw
    assert Path(__file__).read_bytes() == data['current/tools/verify_float16_packet.py']

    def read(name):
        return json.loads(data[name])

    def blob(info):
        raw = data['current/source-blobs/' + info['sha256']]
        assert identity(raw) == info
        return raw

    for name, raw in data.items():
        if name.startswith('current/source-blobs/'):
            assert name.rsplit('/', 1)[1] == digest(raw)
        if name.startswith('current/') and name.count('/') == 1 and name.endswith('.json'):
            value = json.loads(raw)
            assert value.get('status') != 'running', name
            if 'log_sha256' in value:
                assert digest(data[name.removesuffix('.json') + '.log']) == value['log_sha256']
            for info in value.get('changed_source_inventory', {}).values():
                if 'sha256' in info:
                    blob(info)

    roles = report['roles']
    assert set(roles) == ROLES and len(set(roles.values())) == len(ROLES)
    source = report['code_snapshot']
    assert source and all(code_path(name) for name in source)
    build = read('current/' + roles['build'])
    assert report['source_base'] == build['head']
    assert source == {name: info for name, info in build['changed_source_inventory'].items() if code_path(name)}
    assert not any(name.startswith('vendor/') or name.endswith(('Cargo.toml', 'Cargo.lock')) for name in source)
    for name, info in source.items():
        raw = blob(info)
        if repo_root:
            assert (repo_root / name).read_bytes() == raw, name
    assert set(report['checks']) == ROLES
    for role, filename in roles.items():
        result = read('current/' + filename)
        assert result['status'] == 'passed' and result['returncode'] == 0
        assert result['process_group_drained'] and result['direct_child_reaped']
        assert result['head'] == report['source_base']
        assert result['environment']['CARGO_BUILD_JOBS'] == '2'
        assert result['environment']['CARGO_INCREMENTAL'] == '0'
        assert {name: info for name, info in result['changed_source_inventory'].items() if code_path(name)} == source, role
        if role in CARGO:
            assert result['command'][result['command'].index('cargo'):] == CARGO[role]
        elif role in RUNNERS:
            assert any(arg.endswith(RUNNERS[role]) for arg in result['command'])
        elif role == 'float16_bindings':
            assert 'SHARDLOOM_REQUIRE_NATIVE_PYTHON=1' in result['command']
            assert result['command'][-9:] == ['-B', '-m', 'unittest', 'discover', '-s', 'python/tests', '-p', 'test_native_float16*binding.py', '-v']
        raw = data['current/' + filename.removesuffix('.json') + '.log']
        summary = dict(receipt=filename, command=result['command'], status='passed', rust_tests=rust_counts(raw))
        assert report['checks'][role] == summary
        if summary['rust_tests']:
            assert summary['rust_tests']['passed'] > 0 and summary['rust_tests']['failed'] == 0

    def log(role):
        return data['current/' + roles[role].removesuffix('.json') + '.log']

    for name in ('register_widening_preserves_every_binary16_representation',
                 'binary16_narrowing_rejects_rounding_and_unrepresentable_regions'):
        assert re.search(rb'::' + name.encode() + rb' \.\.\. ok', log('default_tests'))
    for name in ('float16_every_bit_pattern_survives_owned_register_copy_and_reordered_gather',
                 'float16_promotion_observes_only_selected_valid_values_under_strict_policy',
                 'native_nested_empty_intake_retains_nonnullable_lists_and_structs'):
        assert re.search(rb'::' + name.encode() + rb' \.\.\. ok', log('native_tests'))
    feature_tree = ast.parse(data['current/tools/check_float16_features.py'])
    commands = next(ast.literal_eval(node.value) for node in feature_tree.body
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'commands' for t in node.targets))
    assert len(commands) == 7
    assert [line.removeprefix('Checking ') for line in log('features').decode().splitlines() if line.startswith('Checking ')] == [
        ' '.join(command + ['--locked', '--offline']) for command in commands]

    core = read('current/python-core-r2/core.json')
    assert core['status'] == 'passed' and core['failure_count'] == core['error_count'] == 0
    count = re.search(rb'Ran (\d+) tests? in [0-9.]+s', log('python_core'))
    assert count
    assert report['python_core'] == dict(discovered_test_cases=core['test_count'],
        reported_test_runs=int(count.group(1)), skip_records=core['skipped_count'], failures=0, errors=0)
    installed = read('current/installed-proof-r2/report.json')
    assert installed['status'] == installed['uninstall_status'] == 'passed'
    assert installed['installed_load']['isolated_install']
    assert installed['integration'] == report['installed_package'] == read('current/installed-proof-r2/integration.json')
    assert installed['integration']['status'] == 'passed'
    assert all(installed['integration'][key] == 0 for key in ('failures', 'errors', 'skipped'))
    assert not installed['publication_attempted'] and not installed['performance_claim']
    proof_source = data['contracts/scripts/prove_native_python_wheel.py']
    if repo_root:
        assert (repo_root / 'scripts/prove_native_python_wheel.py').read_bytes() == proof_source
    assert b'"test_native*binding.py"' in proof_source
    assert b'"test_public_context.py"' in proof_source
    assert b'selected.countTestCases()' in proof_source
    for step in installed['steps']:
        assert step['returncode'] == 0
        assert digest(data['current/installed-proof-r2/' + step['log']]) == step['log_sha256']
    integration_log = data['current/installed-proof-r2/integration.log']
    count = re.search(rb'Ran (\d+) tests? in [0-9.]+s', integration_log)
    assert count and int(count.group(1)) == installed['integration']['tests']
    assert b'\nOK\n' in integration_log and b'... skipped' not in integration_log
    expected = sorted(name for module in MODULES for name in test_methods(blob(source['python/tests/' + module + '.py']), module))
    assert len(expected) == 14
    assert report['float16_tests'] == successful_methods(integration_log, MODULES) == successful_methods(log('float16_bindings'), MODULES) == expected
    context = data['contracts/python/tests/test_public_context.py']
    assert successful_methods(integration_log, ['test_public_context']) == test_methods(context, 'test_public_context')
    assert b'"complete_reopened_rows": 2' in log('example') and b'"signed_zero_preserved": true' in log('example')

    frozen = read('current/' + report['runtime_identity'])
    assert frozen['status'] == 'passed' and frozen['build_receipt'] == roles['build']
    assert frozen['build_receipt_sha256'] == digest(data['current/' + roles['build']])
    assert {Path(row['path']).name: row['sha256'] for row in frozen['files']} == {
        'shardloom': installed['integrity']['cli_sha256'], 'lib_native.dylib': installed['integrity']['native_library_sha256']}
    assert report['runtime_and_wheel'] == {key: installed['integrity'][key] for key in ('cli_sha256', 'native_library_sha256', 'wheel_sha256')}
    package_sources = read('current/wheel-source-identity.json')
    assert package_sources['status'] == 'passed' and package_sources['wheel_sha256'] == report['runtime_and_wheel']['wheel_sha256']
    assert package_sources['changed_python_sources'] == {name: info for name, info in source.items() if name.startswith('python/src/shardloom/')}
    assert report['dependency_change'] == 'none'
    for name, info in report['unchanged_dependency_sources'].items():
        assert identity(data['contracts/' + name]) == info
        if repo_root:
            assert identity((repo_root / name).read_bytes()) == info

    failures = sorted(name for name in data if name.startswith('current/') and name.count('/') == 1
        and name.endswith('.json') and read(name).get('status') == 'failed')
    index = read('current/' + report['failure_index'])
    assert failures == [entry['receipt'] for entry in index['failures']] == [entry['receipt'] for entry in report['original_failures']]
    for entry in index['failures']:
        assert identity(data[entry['receipt']]) == entry['identity'] and entry['disposition']
    assert index['failures'] == report['original_failures']
    assert len(failures) >= 8
    cleanup = read('current/debug-retirement-r1/receipt.json')
    assert cleanup['status'] == 'passed' and cleanup['regular_files'] == 14909
    assert cleanup['retained_runtimes_and_wheel_unchanged'] and cleanup['input_generations_unchanged']
    assert cleanup['release_cache_unchanged'] and not cleanup['storage_floor_changed'] and not cleanup['payload_backup_created']
    assert digest(data['current/debug-retirement-r1/manifest.json.xz']) == cleanup['manifest_sha256']

    closeout = read('prior/variable-closeout-r1.json')
    assert closeout['status'] == 'complete' and closeout['merge_commit'] == report['source_base']
    assert closeout['pr_checks_passed'] == 58 and closeout['main_checks_passed'] == 55
    assert not closeout['review_findings'] and closeout['no_open_prs_at_observation']
    assert closeout['portable_replay_after_retirement'] == 'passed' and closeout['current_inputs_unchanged']
    for name, info in closeout['receipts'].items():
        assert identity(data['prior/' + name]) == info
    main_names = [name for name in closeout['receipts'] if re.fullmatch(r'variable-main1662-r\d+\.json', name)
        and read('prior/' + name)['status'] == 'passed']
    assert len(main_names) == 1
    main = read('prior/' + main_names[0])
    assert main['source_commit'] == closeout['merge_commit']
    assert main['pr_head'] == closeout['pr_head'] and main['source_tree'] == closeout['source_tree']
    for name, checksum in main['raw_sources'].items():
        assert digest(data['prior/' + name]) == checksum
    checks = read('prior/' + main_names[0].removesuffix('.json') + '-raw/checks.json')
    assert checks['total_count'] == len(checks['check_runs'])
    required = set(main['required_hosted_check_names'])
    selected = [row for row in checks['check_runs'] if row['name'] in required]
    assert len(required) == len(selected) == 55 and {row['name'] for row in selected} == required
    assert all(row['head_sha'] == closeout['merge_commit'] and row['status'] == 'completed' and row['conclusion'] == 'success' for row in selected)
    assert report['limitations'] == LIMITATIONS and report['hosted_integration'] == 'pending'
    assert not any(report[key] for key in ('performance_claim', 'publication_attempted', 'whole_item_19_complete'))
    return dict(status='passed', archive_members=len(members), changed_sources=len(source),
        installed_tests=installed['integration']['tests'], float16_tests=len(expected), original_failures=len(failures))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packet', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--repo-root', type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(args.packet, args.repo_root)))
