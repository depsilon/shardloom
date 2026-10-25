"""Retire exact completed experiment payloads after independent portable proof."""
import hashlib
import json
from pathlib import Path
import stat
import subprocess

P = Path(__file__).resolve().parent
E = Path('/Users/dylan/.codex/worktrees/public-docs-api/shardloom-local-repo/docs/benchmarks/evidence/native-fused-measure-2026-10-25')
RETAINED = Path('/Users/dylan/LocalData/shardloom/ingest-ownership-20261025/combined-runtime-r1')


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def generation(path):
    info = path.lstat()
    assert stat.S_ISREG(info.st_mode) and not path.is_symlink(), str(path)
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns


def main():
    destination = E / 'cleanup.json'
    assert not destination.exists()
    manifest = json.loads((E / 'manifest.json').read_text())
    validation = json.loads((E / 'portable-validation.json').read_text())
    packet_sha = digest(E / 'packet.tar.gz')
    assert packet_sha == manifest['archive']['sha256'] == validation['packet_sha256']
    assert validation['verifier_sha256'] == digest(E / 'verify.py')
    assert validation['status'] == 'passed' and validation['engine_executions'] == 0
    result = validation['portable_result']
    assert result['portable_replay_passed']
    assert result['comparison']['all_timing_gates_passed']
    assert result['observations']['work_elimination_observations_passed']
    assert RETAINED.is_dir() and not RETAINED.is_symlink()
    archived = {entry['path']: entry for entry in manifest['files']}
    expected = []

    def saved_json(relative):
        source = P / relative
        assert digest(source) == archived[relative]['sha256']
        return json.loads(source.read_text())

    variants = {
        ('r1', 'baseline'): ('release', 'observe'),
        ('r1', 'candidate'): ('release', 'observe'),
        ('r2', 'baseline'): ('release', 'observe'),
        ('r2', 'candidate'): ('observe',),
        ('r3', 'candidate'): ('release', 'observe'),
    }
    for (revision, role), labels in variants.items():
        for label in labels:
            prefix = f'fused-measure-{role}-runtime-{revision}'
            runtime = saved_json(f'{prefix}/{label}-manifest.json')
            path = P / prefix / f'shardloom-fused-measure-{label}'
            assert path == Path(runtime['binary'])
            expected.append((path, runtime['binary_sha256'], None, 'comparison_executable'))
    json_repro = saved_json('fused-measure-json-repro-identity-r1.json')['identities']['binary']
    expected.append((P / 'fused-measure-json-repro-r1', json_repro['sha256'],
                     json_repro['bytes'], 'diagnostic_executable'))
    encoding = saved_json('fused-measure-encoding-repro-identity-r1.json')['identities']
    for revision in ('r1', 'r2'):
        name = f'fused-measure-encoding-repro-{revision}'
        expected.append((P / name, encoding[name]['sha256'], encoding[name]['bytes'],
                         'diagnostic_executable'))
    inputs = saved_json('fused-measure-inputs-r1/manifest.json')
    for case, identity in inputs.items():
        expected.append((P / 'fused-measure-inputs-r1' / f'{case}.vortex',
                         identity['sha256'], identity['bytes'], 'finite_native_fixture'))
    assert len(expected) == 18 and len({path for path, *_ in expected}) == 18
    records = []
    generations = {}
    for path, identity, size, kind in expected:
        assert path.is_relative_to(P) and path.resolve() == path
        assert not path.is_relative_to(RETAINED)
        before = generation(path)
        assert digest(path) == identity and generation(path) == before
        assert size is None or before[2] == size
        generations[path] = before
        records.append({'path': str(path), 'sha256': identity, 'bytes': before[2],
                        'kind': kind, 'open_handles': False})
    handles = subprocess.run(['/usr/sbin/lsof', '-t', '--',
                              *(str(path) for path, *_ in expected)],
                             text=True, capture_output=True, check=False)
    assert handles.returncode == 1 and not handles.stdout.strip() and not handles.stderr.strip(), handles
    assert all(generation(path) == before for path, before in generations.items())
    retired = []
    report = {'status': 'running', 'retired_files': retired, 'packet_sha256': packet_sha,
              'driver_sha256': digest(Path(__file__)),
              'basis': 'completed finite comparison; independently replayed packet retains complete values, failures, clocks, native input witnesses, source overlays and regeneration harness',
              'accepted_runtime_preserved': str(RETAINED),
              'compiler_cache_retained_for_active_work': True,
              'accepted_benchmark_source_inputs_preserved': True, 'backups_created': False}
    try:
        for item in records:
            path = Path(item['path'])
            assert generation(path) == generations[path]
            path.unlink()
            retired.append(item)
        assert all(not Path(item['path']).exists() for item in retired)
        report['status'] = 'passed'
    finally:
        report['retired_bytes'] = sum(item['bytes'] for item in retired)
        with destination.open('x') as stream:
            json.dump(report, stream, indent=2)
            stream.write('\n')
    print(json.dumps({'status': report['status'], 'retired_files': len(retired),
                      'retired_bytes': report['retired_bytes']}))


if __name__ == '__main__':
    main()
