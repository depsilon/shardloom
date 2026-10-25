#!/usr/bin/env python3
"""Execute the reviewed exact plan and retain a compact, checked outcome."""

import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

V = Path('/Users/dylan/LocalData/shardloom/vortex-provider-refresh-20261024')
W = Path('/Users/dylan/.codex/worktrees/public-docs-api/shardloom-local-repo')
PACKET = W / 'docs/benchmarks/evidence/native-vortex-provider-refresh-2026-10-24'
PLAN = V / 'cleanup-plan-r1.json.gz'
LOG = V / 'cleanup-execution-r1.log'
OUT = V / 'cleanup-execution-r1.json'


def digest(path):
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def generation(path):
    value = path.stat()
    return [value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns]


if LOG.exists() or OUT.exists():
    raise SystemExit('Refusing to overwrite a prior cleanup observation.')
with gzip.open(PLAN, 'rt') as source:
    plan = json.load(source)
assert plan['root'] == str(V) and not plan['unresolved']
assert plan['identity_summary']['candidate_file_count'] == 49955
assert plan['script']['sha256'] == digest(Path(plan['script']['path']))
fresh = json.loads((V / 'fresh-ingest-acceptance-r2.json').read_text())
input_generations = {}
for key in ('original_input', 'target'):
    entry = fresh[key]
    path = Path(entry['path'])
    assert generation(path) == entry['generation'], key
    input_generations[str(path)] = entry['generation']
retained = plan['retained_files'] + [
    entry for entry in plan['retained_invariants'] if 'sha256_basis' not in entry
]
archive_ids = plan['portable_packet']['archives']
before_free = shutil.disk_usage(V).free
argv = [sys.executable, '-B', str(V / 'cleanup-provider-r1.py'), '--delete',
        '--confirm-token', 'DELETE_CLEANUP_PROVIDER_R1']
with LOG.open('x') as log:
    result = subprocess.run(argv, stdout=log, stderr=subprocess.STDOUT, check=False)
missing = [entry for entry in plan['entries'] if not os.path.lexists(entry['path'])]
remaining = [entry['path'] for entry in plan['entries'] if os.path.lexists(entry['path'])]
errors = []
for entry in retained + archive_ids:
    path = Path(entry['path'])
    if not path.is_file() or path.stat().st_size != entry['bytes'] or digest(path) != entry['sha256']:
        errors.append(str(path))
for path, before in input_generations.items():
    if generation(Path(path)) != before:
        errors.append(path)
passed = result.returncode == 0 and not remaining and not errors
receipt = {
    'schema': 'shardloom.native_provider_refresh.cleanup.v1',
    'status': 'passed' if passed else 'failed',
    'argv': argv, 'returncode': result.returncode,
    'inventory': {'path': 'cleanup-inventory.json.gz', 'bytes': PLAN.stat().st_size, 'sha256': digest(PLAN)},
    'helper': {'path': 'retire_generated_outputs.py', 'bytes': Path(plan['script']['path']).stat().st_size,
               'sha256': digest(Path(plan['script']['path']))},
    'log': {'path': 'cleanup.log', 'bytes': LOG.stat().st_size, 'sha256': digest(LOG)},
    'retired_entries': len(missing),
    'retired_regular_bytes': sum(e['bytes'] for e in missing if e['kind'] == 'regular_file'),
    'retired_symlinks': sum(e['kind'] == 'symlink' for e in missing),
    'remaining_targets': remaining,
    'retained_identity_errors': errors,
    'retained_files_rehashed': len(retained),
    'retained_archive_identities_verified': len(archive_ids),
    'retained_unarchived_public_log_items': len(plan['retained_unarchived_public_logs']),
    'retained_input_generations': input_generations,
    'runtime_or_benchmark_executed': False,
    'payload_backups_created': False,
    'storage_limits_changed': False,
    'free_bytes_before': before_free,
    'free_bytes_after': shutil.disk_usage(V).free,
    'empty_directories_retained': True,
    'packet_manifest_sha256': digest(PACKET / 'manifest.json'),
    'packet_replay_sha256': digest(PACKET / 'replay.json'),
}
OUT.write_text(json.dumps(receipt, indent=2) + '\n')
for source, name in ((PLAN, 'cleanup-inventory.json.gz'),
                     (Path(plan['script']['path']), 'retire_generated_outputs.py'),
                     (LOG, 'cleanup.log'), (OUT, 'cleanup.json')):
    destination = PACKET / name
    if destination.exists():
        raise SystemExit('Refusing to overwrite existing portable cleanup evidence.')
    shutil.copyfile(source, destination)
    assert digest(source) == digest(destination)
print(json.dumps({key: receipt[key] for key in ('status', 'retired_entries', 'retired_regular_bytes',
      'retired_symlinks', 'retained_files_rehashed', 'retained_identity_errors', 'remaining_targets')}, indent=2))
raise SystemExit(0 if passed else 1)
