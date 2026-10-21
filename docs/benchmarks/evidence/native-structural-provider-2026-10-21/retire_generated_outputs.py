# SPDX-License-Identifier: Apache-2.0
"""Retire only completed structural-acceptance artifacts after portable replay."""
import hashlib
import json
import lzma
import os
from pathlib import Path
import shutil
import stat
import subprocess

ROOT = Path('/Users/dylan/LocalData/shardloom/conditional-investigations-20261019')
REPO = Path('/Users/dylan/.codex/worktrees/public-docs-api/shardloom-local-repo')
DEST = REPO / 'docs/benchmarks/evidence/native-structural-provider-2026-10-21'
ROOT_NAMES = (
    'structural-public-883', 'structural-matrix-883',
    'structural-workflow-environment-883',
    'structural-pr-wheel-884/package', 'structural-pr-wheel-884/venv',
    'artifacts/prepared-sampling-pr-870',
)


def digest(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1 << 20), b''):
            result.update(block)
    return result.hexdigest()


def identity(path):
    return dict(path=str(path), bytes=path.stat().st_size, sha256=digest(path))


def write_json(path, value):
    with path.open('x') as stream:
        json.dump(value, stream, sort_keys=True, indent=2)
        stream.write('\n')


def main():
    assert not (DEST / 'cleanup.json').exists()
    manifest = json.loads((DEST / 'acceptance.json').read_bytes())
    replay = json.loads((DEST / 'replay.json').read_bytes())
    negative = json.loads((DEST / 'corruption-checks.json').read_bytes())
    assert replay == manifest['replay'] and replay['status'] == 'passed'
    assert negative['status'] == 'passed' and negative['rejected'] == 21
    assert negative['packet_sha256'] == manifest['packet']['sha256']
    packed_path = DEST / manifest['packet']['file']
    assert packed_path.stat().st_size == manifest['packet']['bytes']
    assert digest(packed_path) == manifest['packet']['sha256']
    packet = json.loads(lzma.decompress(packed_path.read_bytes()))
    build = json.loads(packet['members'][str(ROOT / 'structural-workflow-build-883.json')]['data'])
    wheel = json.loads((ROOT / 'structural-pr-wheel-884/report.json').read_bytes())['integrity']
    retained = [identity(Path(build['binary'])), identity(Path(build['native_library']['path'])),
                identity(ROOT / 'structural-pr-wheel-884/dist' / wheel['wheel'])]
    assert retained[0]['sha256'] == build['binary_sha256']
    assert retained[1]['sha256'] == build['native_library']['sha256']
    assert retained[2]['sha256'] == wheel['wheel_sha256']
    roots = [ROOT / name for name in ROOT_NAMES]
    for root in roots:
        assert root.is_dir() and not root.is_symlink() and root.resolve().is_relative_to(ROOT)
        assert all(not Path(row['path']).is_relative_to(root) for row in retained)
        opened = subprocess.run(['/usr/sbin/lsof', '-nP', '+D', str(root)],
                                capture_output=True, text=True)
        assert opened.returncode == 1 and not opened.stdout.strip(), (root, opened.stdout, opened.stderr)
        assert not opened.stderr.strip(), (root, opened.stderr)

    inventory, summaries = [], []
    for root in roots:
        rows = []
        for parent, directories, files in os.walk(root, followlinks=False):
            for name in sorted(directories + files):
                path = Path(parent) / name
                info = path.lstat()
                if stat.S_ISDIR(info.st_mode):
                    continue
                if stat.S_ISLNK(info.st_mode):
                    row = dict(path=str(path), kind='symlink', target=os.readlink(path))
                else:
                    assert stat.S_ISREG(info.st_mode), path
                    row = dict(identity(path), kind='file', device=info.st_dev,
                               inode=info.st_ino, mtime_ns=info.st_mtime_ns)
                    member = packet['members'].get(str(path))
                    if member:
                        assert row['sha256'] == member['sha256'] and row['bytes'] == member['bytes'], path
                        row['retention_basis'] = 'exact_record_in_verified_packet'
                    else:
                        row['retention_basis'] = 'completed_generated_payload_or_environment_regenerable_from_recorded_source_and_protocol'
                rows.append(row)
        inventory.extend(rows)
        summaries.append(dict(path=str(root), entries=len(rows),
                              bytes=sum(row.get('bytes', 0) for row in rows),
                              symlinks=sum(row['kind'] == 'symlink' for row in rows),
                              packet_records=sum(row.get('retention_basis') == 'exact_record_in_verified_packet' for row in rows)))

    compact = json.dumps(dict(schema='shardloom.structural_retirement_inventory.v1',
                              roots=summaries, entries=inventory), sort_keys=True).encode()
    compressed = lzma.compress(compact)
    assert lzma.decompress(compressed) == compact
    inventory_path = DEST / 'cleanup-inventory.json.xz'
    with inventory_path.open('xb') as stream:
        stream.write(compressed)
    write_json(ROOT / 'structural-retirement-plan-889.json', dict(
        schema='shardloom.structural_retirement_plan.v1', status='planned',
        roots=summaries, inventory=identity(inventory_path),
        retained=retained, packet=manifest['packet'], helper=identity(Path(__file__)),
        payload_backup_created=False, active_handles_found=False))
    for row in inventory:
        path = Path(row['path'])
        info = path.lstat()
        if row['kind'] == 'symlink':
            assert stat.S_ISLNK(info.st_mode) and os.readlink(path) == row['target']
        else:
            assert (info.st_dev, info.st_ino, info.st_mtime_ns, info.st_size) == (
                row['device'], row['inode'], row['mtime_ns'], row['bytes']), path
    for root in roots:
        shutil.rmtree(root)
        assert not root.exists()
    assert [identity(Path(row['path'])) for row in retained] == retained
    result = dict(schema='shardloom.structural_retirement.v1', status='passed',
                  roots=summaries, retired_entries=len(inventory),
                  retired_regular_bytes=sum(row.get('bytes', 0) for row in inventory),
                  inventory=identity(inventory_path), retained_runtime_and_wheel=retained,
                  verified_packet=manifest['packet'], copied_packet_replay=identity(DEST / 'replay.json'),
                  negative_controls=identity(DEST / 'corruption-checks.json'),
                  helper=identity(Path(__file__)), active_handles_found=False,
                  payload_backups_created=False, runtime_or_benchmark_executed=False,
                  original_parquet_and_current_vortex_input_untouched=True)
    write_json(DEST / 'cleanup.json', result)
    print(json.dumps(dict(status='passed', retired_entries=len(inventory),
                          retired_regular_bytes=result['retired_regular_bytes'])))


if __name__ == '__main__':
    main()
