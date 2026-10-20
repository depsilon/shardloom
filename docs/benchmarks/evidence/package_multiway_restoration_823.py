"""Preserve and independently verify restored-source and regression evidence."""
import argparse
import base64
import hashlib
import json
import lzma
from pathlib import Path
import re


def sha(data):
    return hashlib.sha256(data).hexdigest()


def identity(path):
    path = Path(path)
    data = path.read_bytes()
    return dict(path=str(path), bytes=len(data), sha256=sha(data))


def save(path, value):
    with Path(path).open('x') as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write('\n')


def verify(packet_path, historical_archive, historical_manifest):
    packet = json.loads(lzma.decompress(Path(packet_path).read_bytes()))
    assert packet['status'] == 'restoration_verified' and packet['candidate_retained'] is False
    assert packet['historical_archive']['sha256'] == identity(historical_archive)['sha256']
    assert packet['historical_manifest']['sha256'] == identity(historical_manifest)['sha256']
    historical = {row['member']: row for row in json.loads(Path(historical_manifest).read_bytes())['members']}
    members = {}
    for row in packet['members']:
        assert row['name'] not in members and not Path(row['name']).is_absolute() and '..' not in Path(row['name']).parts
        data = base64.b64decode(row['base64'], validate=True)
        assert len(data) == row['bytes'] and sha(data) == row['sha256'], row['name']
        members[row['name']] = data
    def load(name):
        return json.loads(members[name])
    source = load('multiway-restored-source-823.json')
    mapping = load('multiway-restored-source-portability-823/manifest.json')
    assert source['files'].keys() == mapping['files'].keys() and len(source['files']) == 2222
    for name, value in mapping['files'].items():
        assert {key: value[key] for key in ('bytes', 'sha256')} == source['files'][name]
        if value['packet'] == 'historical':
            row = historical[value['member']]
            assert row['bytes'] == value['bytes'] and row['sha256'] == value['sha256']
        else:
            assert value['packet'] == 'restoration'
            data = members[value['member']]
            assert len(data) == value['bytes'] and sha(data) == value['sha256']
    compile_inputs = load('multiway-restored-build-822a-input.json')['files']
    assert len(compile_inputs) == 1901
    assert all(source['files'][name] == row for name, row in compile_inputs.items())
    restoration = load('multiway-runtime-restore-plan-822.json')
    assert len(restoration['files']) == 13
    for row in restoration['files']:
        if row['action'] == 'restore_exact_base':
            assert source['files'][row['path']] == dict(bytes=row['after_bytes'], sha256=row['after_sha256'])
        else:
            assert row['action'] == 'remove_archived_experiment' and row['path'] not in source['files']
    assert sha(members['multiway-runtime-restore-822.patch']) == restoration['patch_sha256']
    assert mapping['runtime_code_changed'] is False
    assert set(mapping['changed_tracked_source_files']) == {
        'shardloom-cli/tests/public_memory_inputs.rs', 'shardloom-vortex/src/local_primitive_relational_tests.rs'}
    assert set(mapping['added_source_files']) == {
        'python/tests/test_native_multiway_binding.py', 'shardloom-vortex/src/local_primitive_relational_multiway_tests.rs'}
    guards = [name for name in members if name.endswith('.json') and '.log' not in name]
    successful_guards = 0
    failed_guards = []
    for name in guards:
        record = load(name)
        if 'log_sha256' not in record or 'process_group_drained' not in record:
            continue
        if name == 'multiway-restored-wheel-proof-823.json':
            assert record['status'] == 'failed' and record['returncode'] == 1
            failed_guards.append(name)
        else:
            assert record['status'] == 'passed' and record['returncode'] == 0, name
            successful_guards += 1
        assert record['process_group_drained'] is True and record['direct_child_reaped'] is True
        assert record['head'] == source['source_commit']
        assert record['working_tree'].strip() == source['working_tree']
        assert record['log_sha256'] == sha(members[name.removesuffix('.json') + '.log'])
    assert failed_guards == ['multiway-restored-wheel-proof-823.json']
    assert successful_guards == 13
    counts = {}
    for name, expected in [('multiway-restored-focused-823', (1, 0)),
                           ('multiway-restored-native-all-823', (3041, 30)),
                           ('multiway-restored-workspace-tests-823', (3235, 0)),
                           ('multiway-restored-public-cli-823', (1, 0))]:
        text = members[name + '.log'].decode()
        rows = re.findall(r'test result: ok\. (\d+) passed; 0 failed; (\d+) ignored;', text)
        assert rows and 'test result: FAILED' not in text
        counts[name] = (sum(int(a) for a, _ in rows), sum(int(b) for _, b in rows))
        assert counts[name] == expected, (name, counts[name])
    failed = load('multiway-restored-installed-python-823/report.json')
    assert failed['status'] == 'failed' and 'integration' not in failed
    assert [step['name'] for step in failed['steps']] == ['build', 'create_environment', 'install_optional_consumer']
    assert [step['returncode'] for step in failed['steps']] == [0, 0, 1]
    for step in failed['steps']:
        assert step['log_sha256'] == sha(members['multiway-restored-installed-python-823/' + step['log']])
    failed_log = members['multiway-restored-installed-python-823/install_optional_consumer.log'].decode()
    assert 'Could not find a version that satisfies the requirement pyarrow==25.0.1' in failed_log
    dependency = load('multiway-restored-dependency-824.json')
    requirements = members['multiway-restored-dependency-requirements-824.txt']
    assert sha(requirements) == dependency['requirements']['sha256']
    assert dependency['wheel']['sha256'] in re.findall(r'--hash=sha256:([a-f0-9]{64})', requirements.decode())
    assert dependency['wheel']['path'].endswith('/pyarrow-25.0.1-cp312-cp312-macosx_12_0_arm64.whl')
    wheel = load('multiway-restored-installed-python-824/report.json')
    assert wheel['status'] == wheel['uninstall_status'] == 'passed'
    assert wheel['integration'] == dict(tests=113, failures=0, errors=0, skipped=0, status='passed')
    build = load('multiway-restored-public-build-823.json')
    assert wheel['integrity']['cli_sha256'] == build['binary']['sha256']
    assert wheel['integrity']['native_library_sha256'] == build['native_library']['sha256']
    assert wheel['installed_load']['isolated_install'] is True and wheel['publication_attempted'] is False
    for step in wheel['steps']:
        assert step['returncode'] == 0
        assert step['log_sha256'] == sha(members['multiway-restored-installed-python-824/' + step['log']])
    replayed = load('multiway-historical-replayed-823.json')
    assert replayed['status'] == 'passed' and replayed['outcome'] == 'not_retained'
    assert replayed['archive']['sha256'] == identity(historical_archive)['sha256']
    assert replayed['queries_replayed'] == 2080 and replayed['complete_output_rows_checked'] == 16160040
    assert replayed['production_accepted'] is False and replayed['timing_failure_repeated'] is False
    return dict(status='passed', packet=identity(packet_path), historical_archive=identity(historical_archive),
        members_checked=len(members), source_files_checked=2222, compile_files_checked=1901,
        production_files_restored=8, experimental_modules_removed=5, successful_guards=successful_guards,
        failed_attempts_preserved=failed_guards, repaired_test_dependency=dependency['wheel'],
        suite_counts=counts, installed_python_tests=113, installed_python_skips=0, uninstall_status='passed',
        candidate_retained=False, runtime_code_changed=False, publication_attempted=False,
        historical_replay_queries=2080, historical_output_rows_checked=16160040,
        verifier=identity(__file__), no_git_or_binaries_needed=True)


def package(root):
    import verify_multiway_restoration_823 as restoration
    restoration.verify()
    assert json.loads((root / 'multiway-restored-installed-python-824.json').read_bytes())['status'] == 'passed'
    paths = {p for p in root.glob('multiway-restored-*') if p.is_file() and p.suffix in ('.json', '.xz', '.log')}
    paths.update(p for p in (root / 'multiway-restored-source-portability-823').rglob('*') if p.is_file())
    for attempt in ('823', '824'):
        paths.update(p for p in (root / ('multiway-restored-installed-python-' + attempt)).iterdir()
                     if p.is_file() and p.suffix in ('.json', '.log'))
    extras = ['verify_multiway_restoration_823.py', 'map_multiway_restored_sources_823.py',
              'replay_multiway_historical_823.py', Path(__file__).name, 'multiway-runtime-restore-plan-822.json',
              'multiway-runtime-restore-822.patch', 'multiway-historical-replayed-823.json',
              'multiway-historical-replay-823.json', 'multiway-historical-replay-823.log',
              'multiway-historical-packet-823-readback.json', 'multiway-historical-source-closure-primary-823.json',
              'multiway_acceptance_common_820.py', 'retire_multiway_823.py',
              'cleanup_generated_storage_799.py', 'native_workload_guard_816.py',
              'verify_multiway_wheel_824.py', 'multiway-restored-dependency-requirements-824.txt']
    paths.update(root / name for name in extras)
    members = []
    for path in sorted(paths):
        assert path.is_file() and not path.is_symlink()
        data = path.read_bytes()
        members.append(dict(name=str(path.relative_to(root)), bytes=len(data), sha256=sha(data),
                            base64=base64.b64encode(data).decode()))
    archive = root / 'multiway-historical-packet-823.tar.xz'
    manifest = root / 'multiway-historical-packet-823-manifest.json'
    packet = dict(schema='shardloom.multiway_restoration_closeout.v1', status='restoration_verified',
        candidate_retained=False, runtime_code_changed=False, historical_archive=identity(archive),
        historical_manifest=identity(manifest), members=members)
    output = root / 'multiway-restoration-closeout-823.json.xz'
    with output.open('xb') as stream:
        stream.write(lzma.compress(json.dumps(packet, sort_keys=True, separators=(',', ':')).encode(), preset=6))
    assert json.loads(lzma.decompress(output.read_bytes())) == packet
    result = verify(output, archive, manifest)
    for path in paths:
        row = next(row for row in members if row['name'] == str(path.relative_to(root)))
        assert path.stat().st_size == row['bytes'] and sha(path.read_bytes()) == row['sha256']
    restoration.verify()
    save(root / 'multiway-restoration-closeout-823.json', result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--verify', nargs=4, metavar=('PACKET', 'HISTORICAL_ARCHIVE', 'HISTORICAL_MANIFEST', 'OUTPUT'))
    args = parser.parse_args()
    if args.verify:
        packet, archive, manifest, output = args.verify
        result = verify(packet, archive, manifest)
        save(output, result)
    else:
        result = package(Path(__file__).resolve().parent)
    print(json.dumps(result, sort_keys=True), flush=True)


if __name__ == '__main__':
    main()
