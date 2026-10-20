"""Replay the learned-search decision, exact restoration, checks and retirement."""
import argparse
import hashlib
import importlib.util
import json
import lzma
from pathlib import Path
import re
import subprocess
import sys
import tempfile

E = Path(__file__).resolve().parent
W = Path('/Users/dylan/.codex/worktrees/public-docs-api/shardloom-local-repo')
BASE = '8721506c487b873cc0598ead0b78b4e22339ff99'


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def identity(path):
    raw = path.read_bytes()
    return dict(path=str(path), bytes=len(raw), sha256=digest(raw))


def member(path):
    return dict(**identity(path), text=path.read_text())


def matches(raw, expected):
    assert len(raw) == expected['bytes'] and digest(raw) == expected['sha256']


def read(packet, name):
    row = packet['members'][name]
    raw = row['text'].encode()
    matches(raw, row)
    return raw


def verify_git(repository, files):
    process = subprocess.Popen(['git', 'cat-file', '--batch'], cwd=repository,
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    try:
        for name, expected in sorted(files.items()):
            assert '\n' not in name and not Path(name).is_absolute() and '..' not in Path(name).parts
            process.stdin.write((BASE + ':' + name + '\n').encode())
            process.stdin.flush()
            header = process.stdout.readline().decode().strip().split()
            assert len(header) == 3 and header[1] == 'blob', (name, header)
            size = int(header[2])
            assert size == expected['bytes'], name
            raw = process.stdout.read(size)
            assert process.stdout.read(1) == b'\n'
            matches(raw, expected)
    finally:
        process.stdin.close()
        process.stdout.close()
        assert process.wait(timeout=10) == 0
    return len(files)


def verify(path, comparison, scratch, repository):
    packet = json.loads(lzma.decompress(path.read_bytes()))
    assert packet['schema'] == 'shardloom.learned_search_closeout.v1'
    assert packet['base_commit'] == BASE
    for name in packet['members']:
        read(packet, name)
    matches(comparison.read_bytes(), packet['comparison_packet'])
    with tempfile.TemporaryDirectory(prefix='learned-closeout-replay-', dir=scratch) as directory:
        script = Path(directory) / 'package_learned_cost_826.py'
        script.write_bytes(read(packet, script.name))
        spec = importlib.util.spec_from_file_location('learned_comparison_replay', script)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        decision = module.verify(comparison, scratch, repository)
    assert decision['retention_gate_passed'] is False and decision['calls'] == 544
    source = json.loads(read(packet, 'learned-restoration-827-source.json'))
    assert source['base_commit'] == BASE and source['experimental_modules_removed']
    matches(read(packet, 'run_learned_restoration_827.py'), source['helper'])
    result = json.loads(read(packet, 'learned-restoration-827-verified.json'))
    assert result['status'] == 'passed' and result['all_sources_unchanged']
    matches(read(packet, 'learned-restoration-827-source.json'), result['source'])
    assert result['source_files'] == len(source['source_files'])
    assert [row['name'] for row in result['gates']] == [
        'fmt', 'workspace-clippy', 'workspace-tests', 'native-clippy', 'native-tests']
    counts = {}
    for row in result['gates']:
        raw = read(packet, Path(row['receipt']['path']).name)
        matches(raw, row['receipt'])
        guard = json.loads(raw)
        assert guard['command'] == row['command'] and guard['head'] == BASE
        assert guard['status'] == 'passed' and guard['returncode'] == 0
        assert guard['process_group_drained'] and guard['direct_child_reaped']
        log = read(packet, Path(row['log']['path']).name)
        matches(log, row['log'])
        assert guard['log_sha256'] == digest(log)
        if row['name'].endswith('tests'):
            matches_ = re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;', log.decode())
            expected = dict(passed=sum(int(item[0]) for item in matches_),
                            failed=sum(int(item[1]) for item in matches_),
                            ignored=sum(int(item[2]) for item in matches_))
            assert row['test_counts'] == expected and expected['passed'] > 0 and expected['failed'] == 0
            counts[row['name']] = expected
        if row['name'] == 'native-tests':
            artifact = row['fresh_native_artifact']
            assert artifact['fresh'] is False and artifact['profile']['opt_level'] == '3'
            assert 'release-user-surfaces' in artifact['features']
            assert artifact['executable'] == row['executable']['path']
            assert artifact in [json.loads(line) for line in log.decode().splitlines() if line.startswith('{')]
    bridge = json.loads(read(packet, 'learned-restored-public-bridge-827.json'))
    assert bridge['status'] == 'passed' and bridge['public_package_tests_rerun'] is False
    public_source = json.loads(read(packet, 'multiway-restored-source-823.json'))
    public_build = json.loads(read(packet, 'multiway-restored-public-build-823.json'))
    assert public_build['status'] == 'passed' and public_build['sources_unchanged']
    matches(read(packet, 'multiway-restored-source-823.json'), public_build['source'])
    matches(read(packet, 'multiway-restored-source-823.json'), bridge['baseline_source'])
    matches(read(packet, 'multiway-restored-public-build-823.json'), bridge['baseline_build'])
    assert bridge['matching_source_files'] == len(public_source['files'])
    for key in ('binary', 'native_library'):
        assert bridge['artifacts'][key] == public_build[key]
    all_sources = dict(source['source_files'])
    for name, expected in public_source['files'].items():
        if name in all_sources:
            assert all(all_sources[name][key] == expected[key] for key in ('bytes', 'sha256'))
        all_sources[name] = expected
    checked_sources = verify_git(repository, all_sources) if repository else None
    retired = json.loads(read(packet, 'learned-cost-retirement-827.json'))
    assert retired['status'] == 'passed' and retired['retained_public_artifacts_unchanged']
    assert retired['files'] == len(retired['files_retired']) == 290
    assert retired['logical_bytes'] == sum(row['identity']['bytes'] for row in retired['files_retired'])
    # Source verification is optional in portable replays without Git. The
    # original retirement did verify both compiled source closures against Git.
    for key in ('complete_replay_before', 'complete_replay_after'):
        original = retired[key]
        assert all(original[name] == value for name, value in decision.items() if name != 'compiled_source_checks')
    assert retired['packet_sha256'] == packet['comparison_packet']['sha256']
    for expected in bridge['artifacts'].values():
        retained = retired['retained_public_artifacts'][expected['path']]
        assert retained['sha256'] == expected['sha256'] and retained['identity']['bytes'] == expected['bytes']
    for stem in ('learned-cost-package-826', 'learned-cost-retire-827'):
        guard = json.loads(read(packet, stem + '.json'))
        assert guard['status'] == 'passed' and guard['returncode'] == 0 and guard['process_group_drained']
        assert guard['log_sha256'] == digest(read(packet, stem + '.log'))
    return dict(status='passed', members=len(packet['members']), retention_gate_passed=False,
                comparison_calls=decision['calls'], restoration_gates=len(result['gates']),
                tests=counts, restored_source_files_checked=checked_sources,
                public_source_bridge_files=bridge['matching_source_files'], retired_files=retired['files'],
                retired_bytes=retired['logical_bytes'], all_checks_replayed=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--verify', type=Path)
    parser.add_argument('--comparison', type=Path, default=E / 'learned-cost-packet-826.json.xz')
    parser.add_argument('--scratch-root', type=Path, default=E)
    parser.add_argument('--repository', type=Path)
    args = parser.parse_args()
    if args.verify:
        print(json.dumps(verify(args.verify, args.comparison, args.scratch_root, args.repository)), flush=True)
        return
    names = [
        'package_learned_closeout_827.py', 'package_learned_cost_826.py',
        'run_learned_restoration_827.py', 'retire_learned_cost_827.py',
        'learned-restoration-827-source.json', 'learned-restoration-827-verified.json',
        'learned-restored-public-bridge-827.json', 'multiway-restored-source-823.json',
        'multiway-restored-public-build-823.json', 'learned-cost-retirement-827-plan.json',
        'learned-cost-retirement-827.json', 'learned-intake-retirement-825.json',
    ]
    stems = ['learned-cost-package-826', 'learned-cost-retire-827']
    stems += ['learned-restored-' + kind + '-827' for kind in
              ('fmt', 'workspace-clippy', 'workspace-tests', 'native-clippy', 'native-tests')]
    names += [stem + extension for stem in stems for extension in ('.json', '.log')]
    packet = dict(schema='shardloom.learned_search_closeout.v1', base_commit=BASE,
                  comparison_packet=identity(args.comparison),
                  members={name: member(E / name) for name in names})
    raw = json.dumps(packet, sort_keys=True, separators=(',', ':')).encode()
    path = E / 'learned-closeout-827.json.xz'
    with path.open('xb') as stream:
        stream.write(lzma.compress(raw, preset=6))
    assert lzma.decompress(path.read_bytes()) == raw
    result = verify(path, args.comparison, E, W)
    result.update(packet=identity(path), packager=identity(Path(__file__)),
                  members={name:{key:value for key,value in row.items() if key!='text'} for name,row in packet['members'].items()})
    with (E / 'learned-closeout-827.manifest.json').open('x') as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write('\n')
    print(json.dumps({key:value for key,value in result.items() if key!='members'}), flush=True)


if __name__ == '__main__':
    main()
