"""Verify saved variable-tensor lifecycle evidence offline without query reruns."""
import argparse
import ast
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import re
import tarfile
import tomllib

REQUIRED = [
    'variable-format-r4', 'variable-default-clippy-r3', 'variable-default-tests-r3',
    'variable-native-clippy-r4', 'variable-native-tests-r2', 'variable-feature-matrix-r1',
    'variable-native-build-r3', 'variable-freeze-runtime-r3', 'variable-python-core-r1',
    'variable-example-r2', 'variable-static-docs-r1', 'variable-installed-wheel-r1',
    'variable-provider-acceptance-r1',
]
FORMAT_PROVIDERS = ('orc-rust', 'arrow-avro', 'arrow-ipc', 'arrow-data', 'arrow-array')
NATIVE_FEATURES = 'shardloom-python/python-native,shardloom-vortex/native-encoded-exchange-experiment'
CARGO_COMMANDS = {
    'variable-format-r4': ['cargo', 'fmt', '--all', '--', '--check'],
    'variable-default-clippy-r3': ['cargo', 'clippy', '--workspace', '--all-targets', '--locked', '--offline', '--', '-D', 'warnings'],
    'variable-default-tests-r3': ['cargo', 'test', '--workspace', '--all-targets', '--locked', '--offline'],
    'variable-native-clippy-r4': ['cargo', 'clippy', '--workspace', '--all-targets', '--features', NATIVE_FEATURES, '--locked', '--offline', '--', '-D', 'warnings'],
    'variable-native-tests-r2': ['cargo', 'test', '--workspace', '--all-targets', '--features', NATIVE_FEATURES, '--locked', '--offline'],
    'variable-native-build-r3': ['cargo', 'build', '--locked', '--offline', '--release', '--features', 'release-user-surfaces', '-p', 'shardloom-cli', '-p', 'shardloom-python'],
}


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def identity(raw):
    return dict(bytes=len(raw), sha256=digest(raw))


def code_path(name):
    return not name.startswith('docs/') and (Path(name).suffix in {'.rs', '.py', '.toml', '.lock'}
        or name.startswith(('vendor/', 'python/THIRD_PARTY_LICENSES/')))


def rust_counts(raw):
    matches = re.findall(rb'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;', raw)
    return dict(zip(('passed', 'failed', 'ignored'),
        (sum(int(row[i]) for row in matches) for i in range(3)))) if matches else None


def verify(packet, repo_root=None):
    report = json.loads((packet / 'acceptance.json').read_bytes())
    assert report['schema_version'] == 'shardloom.variable_shape_tensor_acceptance.v1'
    assert report['status'] == 'local_acceptance_passed'
    assert report['archive'] == 'verification.tar.xz'
    raw = (packet / report['archive']).read_bytes()
    assert digest(raw) == report['archive_sha256']
    members = report['members']
    assert len({row['path'] for row in members}) == len(members)
    assert sum(row['bytes'] for row in members) < 64 << 20
    data = {}
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:xz') as archive:
        entries = archive.getmembers()
        assert [entry.name for entry in entries] == [row['path'] for row in members]
        for row, entry in zip(members, entries):
            path = PurePosixPath(row['path'])
            assert entry.isfile() and not path.is_absolute() and '..' not in path.parts
            assert entry.size == row['bytes']
            content = archive.extractfile(entry).read()
            assert identity(content) == {key: row[key] for key in ('bytes', 'sha256')}
            data[row['path']] = content
    assert Path(__file__).read_bytes() == data['current/tools/verify_variable_packet.py']

    def read(name):
        return json.loads(data[name])

    def source_blob(info):
        content = data['current/source-blobs/' + info['sha256']]
        assert identity(content) == info
        return content

    for name, raw in data.items():
        if name.startswith('current/source-blobs/'):
            assert name.rsplit('/', 1)[1] == digest(raw)
        if name.startswith('current/') and name.count('/') == 1 and name.endswith('.json'):
            value = json.loads(raw)
            if 'log_sha256' in value:
                assert digest(data[name.removesuffix('.json') + '.log']) == value['log_sha256']
            for info in value.get('changed_source_inventory', {}).values():
                if 'sha256' in info:
                    source_blob(info)

    source = report['code_snapshot']
    assert source and all(code_path(name) for name in source)
    build = read('current/variable-native-build-r3.json')
    built_source = {name: info for name, info in build['changed_source_inventory'].items() if code_path(name)}
    final_source = read('current/variable-installed-wheel-r1.json')['changed_source_inventory']
    assert source == {name: info for name, info in final_source.items() if code_path(name)}
    correction = read('current/example-correction-r1.json')
    example = 'python/examples/variable_shape_tensor.py'
    assert correction['path'] == example
    assert correction['status'] == 'corrected_verification_pending'
    assert not correction['runtime_implementation_changed'] and not correction['native_rebuild_required']
    assert correction['before'] == built_source[example] and correction['after'] == source[example]
    assert correction['replacement'] == [
        'frame.filter("gte:id:2").sort("id")', 'frame.filter("id >= 2").sort("id")']
    before, after = source_blob(correction['before']), source_blob(correction['after'])
    old, new = [value.encode() for value in correction['replacement']]
    assert before.count(old) == 1 and before.replace(old, new) == after
    assert source == {**built_source, example: correction['after']}
    assert report['example_correction'] == dict(receipt='example-correction-r1.json',
        failed='variable-example-r1.json', passed='variable-example-r2.json',
        runtime_implementation_changed=False)
    assert report['source_base'] == build['head']
    for name, info in source.items():
        content = source_blob(info)
        if repo_root:
            assert (repo_root / name).read_bytes() == content, name
    assert [row['receipt'] for row in report['checks']] == [name + '.json' for name in REQUIRED]
    for entry in report['checks']:
        result = read('current/' + entry['receipt'])
        assert result['status'] == 'passed' and result['returncode'] == 0
        assert result['process_group_drained'] and result['direct_child_reaped']
        assert result['command'] == entry['command'] and result['head'] == report['source_base']
        name = entry['receipt'].removesuffix('.json')
        if name in CARGO_COMMANDS:
            assert result['command'][result['command'].index('cargo'):] == CARGO_COMMANDS[name]
        assert result['environment']['CARGO_BUILD_JOBS'] == '2'
        assert result['environment']['CARGO_INCREMENTAL'] == '0'
        log = data['current/' + entry['receipt'].removesuffix('.json') + '.log']
        assert digest(log) == result['log_sha256']
        assert entry.get('rust_tests') == rust_counts(log)
        if entry.get('rust_tests'):
            assert entry['rust_tests']['passed'] > 0 and entry['rust_tests']['failed'] == 0
        if entry['receipt'] == 'variable-provider-acceptance-r1.json':
            continue  # Exact provider commands and locks are checked independently below.
        for name, info in source.items():
            if entry['receipt'] in {x + '.json' for x in REQUIRED[:6]} and name.endswith('.py'):
                continue
            if name == example and entry['receipt'] not in {
                    'variable-example-r2.json', 'variable-static-docs-r1.json', 'variable-installed-wheel-r1.json'}:
                info = correction['before']
            assert result['changed_source_inventory'][name] == info, (entry['receipt'], name)

    native = read('current/native-sequence-r2.json')
    protocol = read('current/native-protocol-r2.json')
    assert native['status'] == 'passed' and native['active_stage'] is None
    assert native['protocol_sha256'] == digest(data['current/native-protocol-r2.json'])
    assert native['completed'] == [row['name'] for row in protocol['stages']]
    for row in protocol['stages']:
        assert read('current/' + row['name'] + '.json')['command'] == row['command']
    sequence = read('current/python-sequence-r1.json')
    protocol = read('current/python-protocol-r1.json')
    assert sequence['status'] == 'failed' and sequence['active_stage'] == 'variable-example-r1'
    assert sequence['completed'] == ['variable-python-core-r1']
    assert sequence['protocol_sha256'] == digest(data['current/python-protocol-r1.json'])
    assert [row['name'] for row in protocol['stages']] == [
        'variable-python-core-r1', 'variable-example-r1', 'variable-static-docs-r1', 'variable-installed-wheel-r1']
    for row in protocol['stages'][:2]:
        assert read('current/' + row['name'] + '.json')['command'] == row['command']
    failed_example = read('current/variable-example-r1.json')
    assert failed_example['status'] == 'failed' and failed_example['returncode'] != 0
    assert failed_example['changed_source_inventory'][example] == correction['before']
    assert failed_example['process_group_drained'] and failed_example['direct_child_reaped']
    sequence = read('current/python-sequence-r2.json')
    protocol = read('current/python-protocol-r2.json')
    assert sequence['status'] == 'passed' and sequence['active_stage'] is None
    assert sequence['protocol_sha256'] == digest(data['current/python-protocol-r2.json'])
    assert [row['name'] for row in protocol['stages']] == [
        'variable-example-r2', 'variable-static-docs-r1', 'variable-installed-wheel-r1']
    assert sequence['completed'] == [row['name'] for row in protocol['stages']]
    for row in protocol['stages']:
        assert read('current/' + row['name'] + '.json')['command'] == row['command']
    core = read('current/python-core-r1/core.json')
    assert core['status'] == 'passed' and core['failure_count'] == core['error_count'] == 0
    match = re.search(rb'Ran (\d+) tests? in [0-9.]+s', data['current/variable-python-core-r1.log'])
    assert match
    assert report['python_core'] == dict(status='passed', discovered_test_cases=core['test_count'],
        reported_test_runs=int(match.group(1)), skip_records=core['skipped_count'], failures=0, errors=0)

    installed = read('current/installed-proof-r1/report.json')
    assert installed['status'] == installed['uninstall_status'] == 'passed'
    assert installed['integration'] == report['installed_package'] == read('current/installed-proof-r1/integration.json')
    assert installed['integration']['status'] == 'passed' and installed['integration']['tests'] > 350
    assert all(installed['integration'][key] == 0 for key in ('failures', 'errors', 'skipped'))
    assert not installed['publication_attempted'] and not installed['performance_claim']
    for step in installed['steps']:
        assert step['returncode'] == 0
        assert digest(data['current/installed-proof-r1/' + step['log']]) == step['log_sha256']
    for key in ('cli_sha256', 'native_library_sha256', 'wheel_sha256'):
        assert report[key] == installed['integrity'][key]
    frozen = read('current/runtime-r3-identity.json')
    assert frozen['status'] == 'passed' and frozen['build_receipt'] == 'variable-native-build-r3.json'
    assert frozen['build_receipt_sha256'] == digest(data['current/variable-native-build-r3.json'])
    assert {Path(row['path']).name: row['sha256'] for row in frozen['files']} == {
        'shardloom': report['cli_sha256'], 'lib_native.dylib': report['native_library_sha256']}
    log = data['current/installed-proof-r1/integration.log'].decode()
    observed_count = re.search(r'Ran (\d+) tests? in [0-9.]+s', log)
    assert observed_count and int(observed_count.group(1)) == installed['integration']['tests']
    assert '\nOK\n' in log and '... skipped' not in log
    for label, names in [('variable_tensor', ['test_native_tensor_variable_binding', 'test_native_tensor_variable_controls_binding']),
                         ('context', ['test_public_context'])]:
        expected = []
        for module in names:
            path = 'python/tests/' + module + '.py'
            content = source_blob(source[path]) if path in source else data['contracts/' + path]
            if repo_root:
                assert (repo_root / path).read_bytes() == content
            expected += [module + '.' + node.name + '.' + method.name
                for node in ast.parse(content).body if isinstance(node, ast.ClassDef)
                for method in node.body if isinstance(method, ast.FunctionDef) and method.name.startswith('test_')]
        observed = sorted(re.findall(r'^test_\S+ \((' + '|'.join(re.escape(n) for n in names) + r')\.([^\)]+)\) \.\.\. ok$', log, re.MULTILINE))
        observed = sorted(module + '.' + name for module, name in observed)
        assert len(observed) == len(set(observed)) and observed == sorted(expected) == report[label + '_installed_tests']
    assert len(report['variable_tensor_installed_tests']) == 11
    assert len(report['context_installed_tests']) == 19
    assert b'"complete_reopened_rows": 2' in data['current/variable-example-r2.log']

    # Bind every failed top-level observation, including intentionally unsuccessful probes.
    failures = [name for name in sorted(data) if name.startswith('current/') and name.count('/') == 1
                and name.endswith('.json') and read(name).get('status') == 'failed']
    assert failures == [entry['receipt'] for entry in report['original_failures']]
    assert all(entry['disposition'] for entry in report['original_failures'])
    indexed = read('current/failure-index-r2.json')
    assert indexed['failed_receipt_count'] == len(indexed['failed_receipts']) == 21
    assert {'current/' + row['filename'] for row in indexed['failed_receipts']} <= set(failures)
    indexed = read('current/failure-index-r3.json')
    assert indexed['failed_receipt_count'] == len(indexed['failed_receipts']) == 23
    assert {'current/' + row['filename'] for row in indexed['failed_receipts']} == set(failures)
    for name in failures:
        value = read(name)
        if 'log_sha256' in value:
            assert digest(data[name.removesuffix('.json') + '.log']) == value['log_sha256']
    stopped = read('current/variable-native-tests-r1.json')
    assert stopped['status'] == 'failed' and stopped['process_group_drained'] and stopped['direct_child_reaped']
    assert 'minimum free storage reached' in stopped['error']
    cleanup = read('current/debug-retirement-r1/receipt.json')
    assert cleanup['status'] == 'passed' and cleanup['regular_files'] == 22392
    assert cleanup['retained_runtimes_and_wheel_unchanged'] and cleanup['input_generations_unchanged']
    assert cleanup['release_cache_unchanged'] and not cleanup['storage_floor_changed'] and not cleanup['payload_backup_created']
    assert digest(data['current/debug-retirement-r1/manifest.json.xz']) == cleanup['manifest_sha256']

    # The only dependency resolution change replaces two equal-version registry packages with paths.
    original = tomllib.loads(data['contracts/base-Cargo.lock'].decode())
    current = tomllib.loads(source_blob(source['Cargo.lock']).decode())
    for package in original['package']:
        if package['name'] in {'arrow-data', 'arrow-array'}:
            package.pop('source')
            package.pop('checksum')
    assert original == current
    assert report['dependency_change'] == 'same_version_arrow_data_and_arrow_array_path_patches'
    provider_report = read('current/provider-acceptance-r1.json')
    assert provider_report['status'] == 'passed' and len(provider_report['commands']) == 10
    assert {(row['crate'], row['command'][1]) for row in provider_report['commands']} == {
        (name, kind) for name in FORMAT_PROVIDERS for kind in ('test', 'clippy')}
    totals = dict(passed=0, failed=0, ignored=0)
    known = {(p['name'], p['version'], p.get('source'), p.get('checksum')) for p in current['package']}
    for provider in ('arrow-ipc', 'arrow-avro'):
        lock = tomllib.loads(data['contracts/.github/provider-locks/' + provider + '.lock'].decode())
        known.update((p['name'], p['version'], p.get('source'), p.get('checksum')) for p in lock['package'])
    for row in provider_report['commands']:
        assert row['returncode'] == 0
        raw = data['current/' + row['log']]
        assert digest(raw) == row['log_sha256']
        lockpath = '.github/provider-locks/' + row['crate'] + '.lock'
        lockraw = source_blob(source[lockpath]) if lockpath in source else data['contracts/' + lockpath]
        assert digest(lockraw) == row['lock_sha256']
        if repo_root:
            assert (repo_root / lockpath).read_bytes() == lockraw
        if row['crate'] in {'arrow-data', 'arrow-array'}:
            assert all((p['name'], p['version'], p.get('source'), p.get('checksum')) in known
                for p in tomllib.loads(lockraw.decode())['package'])
        if 'tests' in row:
            assert row['tests'] == rust_counts(raw) and row['tests']['failed'] == 0
            for key in totals:
                totals[key] += row['tests'][key]
    assert totals == report['provider_tests'] == dict(passed=1606, failed=0, ignored=1)
    for directory in ('provider-formats-r1', 'provider-new-arrow-r2'):
        failed_provider = read('current/' + directory + '/report.json')
        assert failed_provider['status'] == 'failed'
        for row in failed_provider['commands']:
            assert digest(data['current/' + directory + '/' + row['log']]) == row['log_sha256']
    for provider in ('arrow-data', 'arrow-array', 'parquet'):
        provenance = read('providers/' + provider + '/upstream-provenance.json')
        raw = data['providers/' + provider + '/original.crate']
        assert digest(raw) == provenance['registry_archive_sha256']
        with tarfile.open(fileobj=io.BytesIO(raw), mode='r:gz') as archive:
            originals = {}
            for entry in archive.getmembers():
                path = PurePosixPath(entry.name)
                assert entry.isfile() and not path.is_absolute() and '..' not in path.parts
                assert path.parts[0] == provider + '-' + provenance['version']
                name = PurePosixPath(*path.parts[1:]).as_posix()
                assert name not in originals
                originals[name] = digest(archive.extractfile(entry).read())
            assert originals == provenance['original_files_sha256']
        for field in ('modified_files_sha256', 'added_files_sha256'):
            for name, checksum in provenance[field].items():
                content = data['providers/' + provider + '/current/' + name]
                assert digest(content) == checksum
                if repo_root:
                    assert (repo_root / 'vendor' / provider / name).read_bytes() == content
        if repo_root:
            assert (repo_root / 'vendor' / provider / 'upstream-provenance.json').read_bytes() == data['providers/' + provider + '/upstream-provenance.json']
        if provider in {'arrow-data', 'arrow-array'}:
            for name, checksum in originals.items():
                if name in provenance['omitted_files']:
                    continue
                relative = 'vendor/' + provider + '/' + name
                assert digest(source_blob(source[relative])) == provenance['modified_files_sha256'].get(name, checksum)
    assert read('providers/arrow-array/upstream-provenance.json')['license'] == 'Apache-2.0 AND MIT'
    decision = read('provider-decision/receipt.json')
    raw = data['provider-decision/vortex-tensor-0.90.0.crate']
    assert decision['status'] == 'downloaded_verified_source_only'
    assert decision['version'] == '0.90.0' and decision['license'] == 'Apache-2.0'
    assert not decision['runtime_dependency_added']
    assert digest(raw) == decision['checksum'] and len(raw) == decision['archive_bytes']
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:gz') as archive:
        entries = {entry.name.removeprefix('vortex-tensor-0.90.0/'): entry
                   for entry in archive.getmembers() if entry.isfile()}
        assert set(entries) == set(decision['files'])
        for name, info in decision['files'].items():
            assert identity(archive.extractfile(entries[name]).read()) == info
    assert report['reader_limitations'] == [
        'PyArrow 25.0.1 default Parquet schema drops the variable tensor extension; typed present-parent reads use the stored ARROW:schema.',
        'PyArrow 25.0.1 rejects fixed-size lists beneath null parents; complete native Parquet equality passes independently.']

    # The preceding delivery must be merged, checked on that exact commit, and retired safely.
    closeout = read('fixed/tensor-closeout-r1.json')
    assert closeout['status'] == 'complete' and closeout['pr_checks_passed'] == 58 and closeout['main_checks_passed'] == 55
    assert not closeout['review_findings'] and closeout['no_open_prs_at_observation']
    assert report['source_base'] == closeout['merge_commit']
    assert report['preceding_integration'] == dict(pr=1661, merge_commit=closeout['merge_commit'], pr_checks_passed=58, main_checks_passed=55)
    for name, info in closeout['receipts'].items():
        assert identity(data['fixed/' + name]) == info
    if repo_root:
        for name, info in closeout['committed_evidence'].items():
            assert identity((repo_root / name).read_bytes()) == info
    main_names = [name for name in closeout['receipts']
                  if re.fullmatch(r'tensor-main1661-r\d+\.json', name)
                  and read('fixed/' + name)['status'] == 'passed']
    assert len(main_names) == 1
    main = read('fixed/' + main_names[0])
    assert main['status'] == 'passed' and main['source_commit'] == closeout['merge_commit']
    assert main['pr_head'] == closeout['pr_head'] and main['source_tree'] == closeout['source_tree']
    for name, checksum in main['raw_sources'].items():
        assert digest(data['fixed/' + name]) == checksum
    raw = read('fixed/' + main_names[0].removesuffix('.json') + '-raw/checks.json')
    assert raw['total_count'] == len(raw['check_runs'])
    expected = set(main['required_hosted_check_names'])
    checks = [row for row in raw['check_runs'] if row['name'] in expected]
    assert len(expected) == len(checks) == 55 and {row['name'] for row in checks} == expected
    assert all(row['head_sha'] == closeout['merge_commit'] and row['status'] == 'completed'
        and row['conclusion'] == 'success' for row in checks)
    assert not report['performance_claim'] and not report['publication_attempted'] and not report['whole_item_19_complete']
    assert report['hosted_integration'] == 'pending'
    return dict(status='passed', archive_members=len(members), changed_sources=len(source),
        installed_tests=installed['integration']['tests'], variable_tensor_tests=11, provider_tests=totals)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packet', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--repo-root', type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(args.packet, args.repo_root)))
