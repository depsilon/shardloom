"""Preserve exact finite admission evidence; never executes tests or benchmarks."""
import gzip
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import shutil
import subprocess
import tarfile

ROOT = Path('/Users/dylan/LocalData/shardloom/optimization-investigation-queue-worktree')
OUT = Path(__file__).resolve().parent
DESTINATION = OUT / 'packet-r1'
spec = importlib.util.spec_from_file_location('survivor_replay', OUT / 'verify_survivor_packet.py')
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)

def baseline_source_files():
    entries = subprocess.check_output(
        ['git', 'ls-tree', '-r', '-z', verifier.BASE], cwd=ROOT).split(b'\0')
    sources = {}
    process = subprocess.Popen(['git', 'cat-file', '--batch'], cwd=ROOT,
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    try:
        for entry in entries:
            if not entry:
                continue
            metadata, raw_name = entry.split(b'\t', 1)
            mode, kind, oid = metadata.split()
            name = raw_name.decode()
            if name.startswith(('docs/benchmarks/evidence/', 'docs/release/evidence/',
                                'docs/dependencies/evidence/')):
                continue
            assert kind == b'blob'
            process.stdin.write(oid + b'\n')
            process.stdin.flush()
            found, object_kind, size = process.stdout.readline().split()
            assert found == oid and object_kind == b'blob'
            size = int(size)
            digest = hashlib.sha256()
            remaining, link = size, b''
            while remaining:
                chunk = process.stdout.read(min(remaining, 1 << 20))
                assert chunk
                digest.update(chunk)
                if mode == b'120000':
                    link += chunk
                remaining -= len(chunk)
            assert process.stdout.read(1) == b'\n'
            sources[name] = ({'symlink': link.decode()} if mode == b'120000' else
                             {'bytes': size, 'sha256': digest.hexdigest()})
        process.stdin.close()
        assert process.wait(timeout=30) == 0
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=30)
    return sources

def main():
    assert __debug__ and not DESTINATION.exists()
    assert shutil.disk_usage(OUT).free >= 12 << 30
    files = {}
    raw = ['guard_checks.py', 'source-owner-inventory-r1.json', 'source-owner-inventory-r1.md',
           'source-owner-primary-r1.json', 'provider-inventory-r1.json', 'provider-inventory-r1.md',
           'provider-inventory-r2.json', 'provider-inventory-r2.md', 'provider-primary-r1.json']
    names = [f'admission-r{index}' for index in range(1, 5)] + [
        f'native-clippy-r{index}' for index in range(1, 4)] + [
        name for name in verifier.FINAL_CHECKS if name != 'native-clippy-r3']
    for name in names:
        raw.extend([name + '.json', 'logs/' + name + '.log'])
    raw.extend(f'admission-source-r{index}.tar' for index in range(1, 8))
    raw.extend(['survivor-package-r1.json', 'logs/survivor-package-r1.log'])
    raw.extend('packet-failure-r1/' + name for name in [
        'package_survivor.py', 'verify_survivor_packet.py', 'control_survivor_packet.py'])
    raw.extend(['survivor-package-r2.json', 'logs/survivor-package-r2.log'])
    raw.extend('packet-failure-r2/' + name for name in [
        'package_survivor.py', 'verify_survivor_packet.py', 'control_survivor_packet.py'])
    for name in raw:
        files['raw/' + name] = (OUT / name).read_bytes()
    files['baseline/source-files.json'] = (json.dumps(
        baseline_source_files(), sort_keys=True, indent=2) + '\n').encode()
    for path in verifier.REGISTRATIONS:
        files['baseline/' + path] = subprocess.check_output(
            ['git', 'show', verifier.BASE + ':' + path], cwd=ROOT)
    inventory = json.loads(files['raw/source-owner-inventory-r1.json'])
    provider = json.loads(files['raw/provider-inventory-r2.json'])
    requests = {(inventory['commit'], item['path']) for item in inventory['source_files']}
    requests |= {(provider['commit'], item['file']) for item in provider['citations']}
    for commit, path in sorted(requests):
        files[f'inventory-source/{commit}/{path}'] = subprocess.check_output(
            ['git', 'show', commit + ':' + path], cwd=ROOT)
    support = {
        'support/bounded_supervisor.py': Path('/Users/dylan/LocalData/shardloom/perf-all-20260906/bounded_build_process_resume_20260912.py'),
        'support/release_validation_guard.py': Path('/Users/dylan/LocalData/shardloom/release-0.3.3-20260930/release_validation_guard.py'),
        'support/local_uat_storage.py': ROOT / 'scripts/local_uat_storage.py',
    }
    for name, path in support.items():
        files[name] = path.read_bytes()
    controls = ['native_relational_output_buffer_tests.rs']
    for name in controls:
        path = 'shardloom-vortex/src/local_primitives/' + name
        files['control-source/' + path] = (ROOT / path).read_bytes()
    for name in ['local_primitive_relational_application_tests.rs',
                 'local_primitive_relational_batch_join_tests.rs',
                 'local_primitive_relational_batch_input_tests.rs',
                 'local_primitive_relational_aggregate_bind.rs',
                 'local_primitive_collect.rs']:
        path = 'shardloom-vortex/src/' + name
        files['control-source/' + path] = (ROOT / path).read_bytes()
    path = 'vendor/vortex-array/src/dtype/serde/serde.rs'
    files['control-source/' + path] = (ROOT / path).read_bytes()
    facts = verifier.verify_members(files)
    DESTINATION.mkdir()
    with (DESTINATION / 'observations.tar.gz').open('xb') as output:
        with gzip.GzipFile(filename='', fileobj=output, mode='wb', compresslevel=9, mtime=0) as packed:
            with tarfile.open(fileobj=packed, mode='w|', format=tarfile.PAX_FORMAT) as archive:
                for name, data in sorted(files.items()):
                    info = tarfile.TarInfo(name)
                    info.size, info.mode, info.mtime = len(data), 0o644, 0
                    archive.addfile(info, io.BytesIO(data))
    shutil.copyfile(OUT / 'verify_survivor_packet.py', DESTINATION / 'verify.py')
    shutil.copyfile(Path(__file__), DESTINATION / 'package.py')
    with (DESTINATION / 'decision.json').open('x') as stream:
        json.dump(facts, stream, sort_keys=True, indent=2)
        stream.write('\n')
    manifest = {'schema': 'shardloom.survivor_admission_packet.v1',
                'files': {path.name: verifier.shape(path.read_bytes())
                          for path in sorted(DESTINATION.iterdir()) if path.is_file()},
                'members': {name: verifier.shape(data) for name, data in sorted(files.items())},
                'input_payloads': 'identity only; deterministic fixture construction and source snapshots retained',
                'accepted_base_commit': verifier.BASE,
                'decision': 'decline_unconditional_callback_adapter'}
    with (DESTINATION / 'manifest.json').open('x') as stream:
        json.dump(manifest, stream, sort_keys=True, indent=2)
        stream.write('\n')
    replay = verifier.verify(DESTINATION)
    with (OUT / 'packet-replay-r1.json').open('x') as stream:
        json.dump(replay, stream, sort_keys=True, indent=2)
        stream.write('\n')
    print(json.dumps(replay, sort_keys=True))

if __name__ == '__main__':
    main()
