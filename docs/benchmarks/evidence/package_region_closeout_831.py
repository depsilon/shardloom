"""Replay region rejection, restoration, fixture repair and scoped retirement."""
import argparse
import hashlib
import importlib.util
import json
import lzma
from pathlib import Path
import re
import subprocess
import tempfile

E = Path(__file__).resolve().parent
W = Path('/Users/dylan/.codex/worktrees/public-docs-api/shardloom-local-repo')
BASE = '603e8ec163a0cbcb24bbf6712a9a4e311e46b187'
FIXTURE = 'shardloom-vortex/src/local_primitive_numeric_partition_native_tests.rs'


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def identity(path):
    raw = path.read_bytes()
    return dict(path=str(path), bytes=len(raw), sha256=digest(raw))


def member(path):
    raw = path.read_bytes()
    return dict(**identity(path), text=raw.decode())


def matches(raw, expected):
    assert len(raw) == expected['bytes'] and digest(raw) == expected['sha256']


def read(packet, name):
    row = packet['members'][name]
    raw = row['text'].encode()
    matches(raw, row)
    return raw


def parsed(packet, name):
    return json.loads(read(packet, name))


def guard(packet, name, passed=True):
    result = parsed(packet, name + '.json')
    assert result['head'] == BASE
    assert result['process_group_drained'] and result['direct_child_reaped']
    assert result['log_sha256'] == digest(read(packet, name + '.log'))
    assert result['status'] == ('passed' if passed else 'failed')
    assert (result['returncode'] == 0) is passed
    return result


def verify_git(packet, repository, sources, revised, scratch):
    if repository is None:
        return None
    process = subprocess.Popen(['git', 'cat-file', '--batch'], cwd=repository,
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    original_fixture = None
    try:
        for name, expected in sorted(sources.items()):
            assert not Path(name).is_absolute() and '..' not in Path(name).parts and '\n' not in name
            process.stdin.write((BASE + ':' + name + '\n').encode())
            process.stdin.flush()
            header = process.stdout.readline().decode().strip().split()
            assert len(header) == 3 and header[1] == 'blob', name
            raw = process.stdout.read(int(header[2]))
            assert process.stdout.read(1) == b'\n'
            matches(raw, expected)
            if name == FIXTURE:
                original_fixture = raw
    finally:
        process.stdin.close()
        process.stdout.close()
        assert process.wait(timeout=10) == 0
    assert original_fixture is not None
    with tempfile.TemporaryDirectory(prefix='region-fixture-replay-', dir=scratch) as temporary:
        root = Path(temporary)
        destination = root / FIXTURE
        destination.parent.mkdir(parents=True)
        destination.write_bytes(original_fixture)
        patch = revised['test_fixture_patch']
        assert re.findall(r'^diff --git a/(\S+) b/\1$', patch, re.M) == [FIXTURE]
        subprocess.run(['git', 'apply', '--check', '-'], cwd=root, input=patch.encode(), check=True)
        subprocess.run(['git', 'apply', '-'], cwd=root, input=patch.encode(), check=True)
        assert destination.read_bytes() == read(packet, 'source/test_fixture.rs')
    return len(sources)


def verify(path, comparison, scratch, repository=None):
    packet = json.loads(lzma.decompress(path.read_bytes()))
    assert packet['schema'] == 'shardloom.region_projection_closeout.v1'
    assert packet['base_commit'] == BASE
    for name in packet['members']:
        read(packet, name)
    matches(comparison.read_bytes(), packet['comparison_packet'])
    with tempfile.TemporaryDirectory(prefix='region-closeout-replay-', dir=scratch) as temporary:
        script = Path(temporary) / 'package_region_projection_831.py'
        script.write_bytes(read(packet, script.name))
        spec = importlib.util.spec_from_file_location('region_comparison_closeout', script)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        decision = module.verify(comparison, scratch, repository)
    assert decision['eligible_for_timings'] is False
    assert decision['comparison_calls'] == 64 and decision['intake_calls'] == 32
    original = parsed(packet, 'region-restoration-831-source.json')
    revised = parsed(packet, 'region-restoration-831b-source.json')
    assert original['base_commit'] == revised['base_commit'] == BASE
    assert original['experimental_modules_removed'] and revised['experimental_modules_removed']
    assert revised['runtime_sources_restored_to_base'] is True
    before, after = original['source_files'], revised['source_files']
    assert before.keys() == after.keys()
    assert [name for name in before if before[name] != after[name]] == [FIXTURE]
    matches(read(packet, 'source/test_fixture.rs'), revised['test_fixture_change'])
    matches(read(packet, 'source/test_fixture.rs'), after[FIXTURE])
    for suffix, source in [('831', original), ('831b', revised)]:
        matches(read(packet, f'run_region_restoration_{suffix}.py'), source['helper'])
    failure = parsed(packet, 'region-restoration-831-failed-native-qualified.json')
    assert failure['status'] == 'failed_fixture_creation'
    assert failure['runtime_source_exactly_restored'] and not failure['execution_reached_in_failed_test']
    matches(read(packet, 'region-restoration-831-source.json'), failure['source'])
    old_log = read(packet, 'region-restored-native-tests-831.log').decode()
    assert '3040 passed; 1 failed; 30 ignored' in old_log
    assert 'AlreadyExists' in old_log and 'File exists' in old_log
    assert failure['failure'] in old_log
    assert failure['fresh_native_artifact']['fresh'] is False
    assert failure['fresh_native_artifact']['profile']['opt_level'] == '3'
    assert failure['fresh_native_artifact'] in [json.loads(line) for line in old_log.splitlines() if line.startswith('{')]
    for phase in ('fmt', 'workspace-clippy', 'workspace-tests', 'native-clippy'):
        guard(packet, f'region-restored-{phase}-831')
    guard(packet, 'region-restored-native-tests-831', passed=False)
    result = parsed(packet, 'region-restoration-831b-verified.json')
    assert result['status'] == 'passed' and result['all_sources_unchanged']
    assert result['runtime_sources_restored_to_base'] is True
    matches(read(packet, 'region-restoration-831b-source.json'), result['source'])
    assert result['source_files'] == len(after)
    assert result['test_fixture_change'] == revised['test_fixture_change']
    assert [row['name'] for row in result['gates']] == [
        'fmt', 'workspace-clippy', 'workspace-tests', 'native-clippy', 'native-tests']
    counts = {}
    for row in result['gates']:
        name = 'region-restored-' + row['name'] + '-831b'
        receipt = guard(packet, name)
        assert receipt['command'] == row['command']
        matches(read(packet, name + '.json'), row['receipt'])
        log_raw = read(packet, name + '.log')
        matches(log_raw, row['log'])
        log = log_raw.decode()
        if row['name'].endswith('tests'):
            found = re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;', log)
            observed = dict(passed=sum(int(item[0]) for item in found),
                            failed=sum(int(item[1]) for item in found),
                            ignored=sum(int(item[2]) for item in found))
            assert found and observed['passed'] > 0 and observed['failed'] == 0
            assert row['test_counts'] == observed
            counts[row['name']] = observed
        if row['name'] == 'native-tests':
            artifact = row['fresh_native_artifact']
            assert artifact['fresh'] is False and artifact['profile']['opt_level'] == '3'
            assert 'release-user-surfaces' in artifact['features']
            assert artifact in [json.loads(line) for line in log.splitlines() if line.startswith('{')]
            assert artifact['executable'] == row['executable']['path']
            names = re.findall(r'^test local_primitives::numeric_partition_native_tests::(\S+) \.\.\. ok$', log, re.M)
            assert set(names) == {
                'native_numeric_partition_fixtures_with_equal_timestamps_are_independent',
                'native_numeric_partitions_reexecute_exact_topk_on_one_source',
                'native_numeric_partitions_preserve_derived_values_through_ordinary_execution',
                'native_numeric_partitions_fail_committed_source_faults_and_release_owners'}
    bridge = parsed(packet, 'region-restored-public-bridge-831b.json')
    public_source = parsed(packet, 'multiway-restored-source-823.json')
    public_build = parsed(packet, 'multiway-restored-public-build-823.json')
    assert bridge['status'] == 'passed' and bridge['public_package_tests_rerun'] is False
    assert bridge['all_non_test_source_bytes_unchanged'] is True
    assert set(bridge['test_only_changed_files']) == {FIXTURE}
    assert bridge['test_only_changed_files'][FIXTURE]['before'] == public_source['files'][FIXTURE]
    assert bridge['test_only_changed_files'][FIXTURE]['after'] == revised['test_fixture_change']
    assert bridge['matching_source_files'] == len(public_source['files']) - 1
    assert 'test,' in bridge['test_module_boundary'] and 'mod numeric_partition_native_tests;' in bridge['test_module_boundary']
    assert bridge['test_module_boundary'] in read(packet, 'source/test_module_container.rs').decode()
    matches(read(packet, 'source/test_module_container.rs'), bridge['test_module_container'])
    matches(read(packet, 'multiway-restored-source-823.json'), bridge['baseline_source'])
    matches(read(packet, 'multiway-restored-public-build-823.json'), bridge['baseline_build'])
    assert public_build['status'] == 'passed' and public_build['sources_unchanged']
    assert public_build['cli_artifact']['profile']['test'] is False
    assert public_build['native_artifact']['profile']['test'] is False
    assert bridge['artifacts'] == {key: public_build[key] for key in ('binary', 'native_library')}
    all_sources = dict(before)
    for name, expected in public_source['files'].items():
        if name in all_sources:
            assert all(all_sources[name][key] == expected[key] for key in ('bytes', 'sha256'))
        all_sources[name] = expected
    checked_sources = verify_git(packet, repository, all_sources, revised, scratch)
    retired = parsed(packet, 'region-retirement-831.json')
    assert retired['status'] == 'passed' and retired['retained_public_artifacts_unchanged']
    assert retired['packet'] == packet['comparison_packet']
    assert retired['files'] == len(retired['files_retired'])
    assert retired['logical_bytes'] == sum(row['identity']['bytes'] for row in retired['files_retired'])
    assert sum(row['kind'] == 'temporary_native_payload' for row in retired['files_retired']) == 8
    assert sum(row['kind'] == 'comparison_executable' for row in retired['files_retired']) == 2
    assert retired['retained_public_artifacts'] == bridge['artifacts']
    for field in ('complete_replay_before', 'complete_replay_after'):
        assert {key: value for key, value in retired[field].items() if key != 'source_verification'} == {
            key: value for key, value in decision.items() if key != 'source_verification'}
    matches(read(packet, 'retire_region_projection_831.py'), retired['helper'])
    guard(packet, 'region-projection-package-831-a1')
    guard(packet, 'region-projection-retire-831-a1')
    return dict(status='passed', members=len(packet['members']), eligible_for_timings=False,
        comparison_calls=64, intake_calls=32, original_native_failure_preserved=True,
        restoration_gates=5, tests=counts, runtime_source_files_checked=checked_sources,
        test_only_fixture_change=FIXTURE, public_source_bridge_unchanged_files=bridge['matching_source_files'],
        retired_files=retired['files'], retired_bytes=retired['logical_bytes'], all_checks_replayed=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--verify', type=Path)
    parser.add_argument('--comparison', type=Path, default=E / 'region-projection-packet-831-r1.json.xz')
    parser.add_argument('--scratch-root', type=Path, default=E)
    parser.add_argument('--repository', type=Path)
    args = parser.parse_args()
    if args.verify:
        print(json.dumps(verify(args.verify, args.comparison, args.scratch_root, args.repository)), flush=True)
        return
    names = ['package_region_closeout_831.py', 'package_region_projection_831.py',
        'run_region_restoration_831.py', 'run_region_restoration_831b.py', 'retire_region_projection_831.py',
        'region-restoration-831-source.json', 'region-restoration-831-failed-native-qualified.json',
        'region-restoration-831b-source.json', 'region-restoration-831b-verified.json',
        'region-restored-public-bridge-831.json', 'region-restored-public-bridge-831b.json',
        'multiway-restored-source-823.json', 'multiway-restored-public-build-823.json',
        'region-retirement-831-plan.json', 'region-retirement-831.json']
    stems = ['region-projection-package-831-a1', 'region-projection-retire-831-a1']
    stems += [f'region-restored-{kind}-{suffix}' for suffix in ('831', '831b')
              for kind in ('fmt', 'workspace-clippy', 'workspace-tests', 'native-clippy', 'native-tests')]
    names += [stem + extension for stem in stems for extension in ('.json', '.log')]
    members = {name: member(E / name) for name in names}
    members['source/test_fixture.rs'] = member(W / FIXTURE)
    members['source/test_module_container.rs'] = member(W / 'shardloom-vortex/src/local_primitives.rs')
    packet = dict(schema='shardloom.region_projection_closeout.v1', base_commit=BASE,
                  comparison_packet=identity(args.comparison), members=members)
    raw = json.dumps(packet, sort_keys=True, separators=(',', ':')).encode()
    output = E / 'region-closeout-831.json.xz'
    with output.open('xb') as stream:
        stream.write(lzma.compress(raw, preset=6))
    assert lzma.decompress(output.read_bytes()) == raw
    result = verify(output, args.comparison, E, W)
    result.update(packet=identity(output), packager=identity(Path(__file__)),
        members={name: {key: value for key, value in row.items() if key != 'text'} for name, row in members.items()})
    with (E / 'region-closeout-831.manifest.json').open('x') as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write('\n')
    print(json.dumps({key: value for key, value in result.items() if key != 'members'}), flush=True)


if __name__ == '__main__':
    main()
