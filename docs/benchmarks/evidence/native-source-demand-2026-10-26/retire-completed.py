"""Retire exact completed source-demand artifacts after portable and clean-source proof."""
from pathlib import Path
import gzip
import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys

REPO = Path('/Users/dylan/.codex/worktrees/public-docs-api/shardloom-local-repo')
ROOT = Path(__file__).resolve().parent.parent
PACKET = REPO / 'docs/benchmarks/evidence/native-source-demand-2026-10-26'
sys.path[:0] = [str(REPO / 'scripts'), str(REPO / 'python/src'),
               str(REPO / 'benchmarks/traditional_analytics'),
               '/Users/dylan/LocalData/shardloom/release-0.3.3-20260930']
from query_surface.replay import object_files
from release_validation_guard import WorkloadGuard


def read(path):
    return json.loads(path.read_text())


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def identity(path):
    state = path.lstat()
    item = {'path': str(path), 'generation': [state.st_dev, state.st_ino, state.st_mode,
            state.st_size, state.st_mtime_ns, state.st_ctime_ns]}
    if stat.S_ISREG(state.st_mode):
        item.update(kind='regular', sha256=digest(path), allocated_bytes=state.st_blocks * 512)
    elif stat.S_ISLNK(state.st_mode):
        item.update(kind='symlink', target=os.readlink(path))
    elif stat.S_ISDIR(state.st_mode):
        item.update(kind='directory')
    else:
        raise ValueError('unexpected generated file type: ' + str(path))
    after = path.lstat()
    assert item['generation'] == [after.st_dev, after.st_ino, after.st_mode,
                                 after.st_size, after.st_mtime_ns, after.st_ctime_ns]
    return item


def inventory(root):
    assert root.is_relative_to(ROOT) and root.resolve() == root
    rows = [identity(root)]
    if rows[0]['kind'] == 'directory':
        assert not (root / '.git').exists()
        def visit(directory):
            for path in sorted(directory.iterdir()):
                item = identity(path)
                rows.append(item)
                if item['kind'] == 'directory':
                    visit(path)
        visit(root)
    return rows


def closed(path):
    command = ['/usr/sbin/lsof', '-nP', '-Fpn']
    command += ['+D', str(path)] if path.is_dir() else [str(path)]
    result = subprocess.run(command, capture_output=True, text=True, timeout=45)
    assert result.returncode == 1 and not result.stdout and not result.stderr, (str(path), result.stdout, result.stderr)
    return {'path': str(path), 'returncode': 1, 'open_handles': 0}


def main():
    assert ROOT == Path('/Users/dylan/LocalData/shardloom/source-demand-2026-10-26')
    assert not (PACKET / 'cleanup.json').exists()
    manifest = read(PACKET / 'manifest.json')
    archived = object_files(manifest, (PACKET / 'packet.tar.xz').read_bytes())
    clean = read(PACKET / 'clean-source-replay.json')
    assert clean['status'] == 'passed' and clean['complete_replay_matches']
    assert all(digest(PACKET / name) == value for name, value in clean['packet_sha256'].items())
    assert read(PACKET / 'replay.json')['status'] == 'passed'
    package = read(ROOT / 'installed-wheel-r3/report.json')
    assert package['status'] == package['uninstall_status'] == 'passed'
    assert package['integration'] == dict(tests=184, failures=0, errors=0, skipped=0, status='passed')
    retained = {
        ROOT / 'candidate-runtime-r2/shardloom': package['integrity']['cli_sha256'],
        ROOT / 'candidate-runtime-r2/_native.abi3.so': package['integrity']['native_library_sha256'],
        ROOT / 'installed-wheel-r3/dist' / package['integrity']['wheel']: package['integrity']['wheel_sha256'],
    }
    baseline = read(ROOT / 'baseline-inputs.json')
    heldout = read(ROOT / 'heldout-inputs.json')
    for item in [baseline['parquet'], heldout['original_inputs'][1]]:
        retained[Path(item['path'])] = item['sha256']
    broad = read(ROOT / 'broad-parquet-checkpoint-r2/inputs.json')
    for paths in broad['sources'].values():
        for value in paths:
            item = next(item for item in broad['frozen_files'] if item['path'] == value)
            retained[Path(value)] = item['sha256']
    assert all(digest(path) == value for path, value in retained.items())
    roots = []
    for number in (1, 2, 3):
        roots.extend((ROOT / f'installed-wheel-r{number}/{name}', False) for name in ('package', 'venv'))
        if number != 3:
            roots.append((ROOT / f'installed-wheel-r{number}/dist', False))
    roots.extend((ROOT / name, False) for name in (
        'wheel-build-tools', 'installed-proof-wheelhouse', 'candidate-inputs',
        'candidate2-inputs', 'heldout2-inputs', 'inputs/.shardloom',
        'packet-draft', 'packet-replay-clean'))
    roots.extend((ROOT / name, True) for name in (
        'candidate-r1-source', 'candidate-runtime-r2/source',
        'candidate-harness-r1', 'candidate-harness-r2', 'broad-parquet-checkpoint-r1/harness',
        'logs', 'broad-parquet-checkpoint-r1/logs', 'broad-parquet-checkpoint-r2/logs',
        'full43-r1', 'full43-r2'))
    singles = [Path(item['path']) for item in baseline['fresh_inputs']]
    singles.append(Path(baseline['sequence_input']['path']))
    singles += [Path(item['path']) for item in heldout['inputs'].values()]
    singles.append(Path(heldout['original_inputs'][0]['path']))
    singles.extend(ROOT / 'candidate-runtime-r1' / name for name in ('shardloom', '_native.abi3.so'))
    roots.extend((path, False) for path in singles)
    assert len({str(path) for path, _ in roots}) == len(roots)
    assert all(not (path == kept or path in kept.parents) for path, _ in roots for kept in retained)
    assert all(path.is_relative_to(ROOT) and path.exists() and not path.is_symlink() for path, _ in roots)
    rows = []
    for path, require_archive in roots:
        current = inventory(path)
        for item in current:
            if require_archive and item['kind'] == 'regular':
                key = manifest['path_map'].get(item['path'])
                assert key in archived and hashlib.sha256(archived[key]).hexdigest() == item['sha256'], item['path']
        rows.extend(current)
    assert len({item['path'] for item in rows}) == len(rows)
    proofs = {str(path): digest(path) for path in ROOT.glob('*.json') if path.is_file() and not path.is_symlink()}
    proofs.update({str(PACKET / name): digest(PACKET / name)
                   for name in ('manifest.json', 'packet.tar.xz', 'replay.json', 'clean-source-replay.json', 'corruption-checks.json')})
    record = {
        'schema_version': 'shardloom.generated_artifact_retirement.v1', 'status': 'running',
        'roots': [str(path) for path, _ in roots], 'retained_sha256': {str(path): value for path, value in retained.items()},
        'closed_handle_checks': [], 'removed_entries': 0,
        'source_inputs_retained': True, 'original_external_baselines_untouched': True,
        'user_files_and_item18_untouched': True, 'cargo_cache_untouched': True,
        'payload_backups_created': False, 'symlink_targets_followed': False,
    }
    raw = json.dumps({'entries': rows, 'retained': record['retained_sha256'], 'proof_sha256': proofs,
        'regeneration_basis': 'Retained original Parquet/seed, recorded source overlay and complete native/competitor evidence; latest tested runtime/wheel; locked package requirements. No bit-identical rebuild claim.'},
        sort_keys=True).encode()
    assert len(raw) < 32 << 20
    packed = gzip.compress(raw, mtime=0)
    assert gzip.decompress(packed) == raw
    inventory_path = PACKET / 'cleanup-inventory.json.gz'
    with inventory_path.open('xb') as stream:
        stream.write(packed)
    record.update(inventory=str(inventory_path), inventory_sha256=digest(inventory_path),
                  free_bytes_before=shutil.disk_usage(ROOT).free)
    try:
        WorkloadGuard().check()
        for path, _ in roots:
            record['closed_handle_checks'].append(closed(path))
        assert all(digest(Path(path)) == value for path, value in proofs.items())
        assert [item for path, _ in roots for item in inventory(path)] == rows
        WorkloadGuard().check()
        print(json.dumps({'stage': 'identities_and_closed_handles_verified', 'entries': len(rows)}), flush=True)
        for item in sorted(rows, key=lambda row: len(Path(row['path']).parts), reverse=True):
            path = Path(item['path'])
            if item['kind'] == 'directory':
                state = path.lstat()
                assert [state.st_dev, state.st_ino, state.st_mode] == item['generation'][:3]
                path.rmdir()
            else:
                assert identity(path) == item
                path.unlink()
            record['removed_entries'] += 1
        assert all(not path.exists() and not path.is_symlink() for path, _ in roots)
        assert all(digest(path) == value for path, value in retained.items())
        assert all(digest(Path(path)) == value for path, value in proofs.items())
        record.update(status='passed', regular_files=sum(item['kind'] == 'regular' for item in rows),
            symlinks=sum(item['kind'] == 'symlink' for item in rows),
            retired_regular_bytes=sum(item['generation'][3] for item in rows if item['kind'] == 'regular'),
            retired_allocated_bytes=sum(item.get('allocated_bytes', 0) for item in rows),
            all_proof_files_unchanged=True, free_bytes_after=shutil.disk_usage(ROOT).free)
    except BaseException as error:
        record.update(status='failed', error=repr(error))
        raise
    finally:
        (PACKET / 'cleanup.json').write_text(json.dumps(record, indent=2) + '\n')
        (ROOT / 'cleanup.json').write_text(json.dumps(record, indent=2) + '\n')
    print(json.dumps({key: record[key] for key in ('status', 'regular_files', 'symlinks', 'retired_regular_bytes')}), flush=True)


if __name__ == '__main__':
    main()
