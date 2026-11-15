"""Freeze the bounded admission source review; never build or execute an engine."""
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import shutil
import subprocess
import tarfile

B = Path(__file__).resolve().parent
W = Path('/Users/dylan/LocalData/shardloom/optimization-investigation-queue-worktree')
OUT = W / 'docs/benchmarks/evidence/native-bloom-admission-2026-11-14'


def shape(data):
    return dict(bytes=len(data), sha256=hashlib.sha256(data).hexdigest())


def save(path, value):
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + '\n')


def main():
    inventory = json.loads((B / 'bloom-packet-inventory-r1.json').read_bytes())
    members = {}
    origins = {}
    for row in inventory['members']:
        if row['source'].startswith('git:'):
            _, commit, relative = row['source'].split(':', 2)
            assert commit == inventory['base'] and row['member'] == 'sources/' + relative
            data = subprocess.check_output(['git', 'show', commit + ':' + relative], cwd=W)
            assert data == (W / relative).read_bytes(), relative
        else:
            path = Path(row['source'])
            assert path.is_file() and not path.is_symlink(), row['source']
            data = path.read_bytes()
        assert shape(data) == {key: row[key] for key in ('bytes', 'sha256')}
        assert row['member'] not in members
        members[row['member']] = data
        origins[row['member']] = row
    assert len(members) == inventory['count'] == 63
    assert sum(map(len, members.values())) == inventory['total_bytes']
    additions = {}

    def add(name, path, kind, expected=None):
        assert name not in members and path.is_file() and not path.is_symlink(), name
        data = path.read_bytes()
        if expected is not None:
            assert shape(data) == expected, name
        members[name] = data
        additions[name] = str(path)
        origins[name] = dict(member=name, source=str(path), kind=kind, **shape(data))

    for name, keys in (('bloom-provider-inventory-r1.json', ('source_sha256', 'context_sha256')),
                       ('bloom-registration-inventory-r1.json', ('source_hashes_sha256',))):
        record = json.loads((B / name).read_bytes())
        for key in keys:
            entries = record[key]
            pairs = entries.items() if isinstance(entries, dict) else ((item['path'], item['sha256']) for item in entries)
            for path, digest in pairs:
                data = (W / path).read_bytes()
                assert shape(data)['sha256'] == digest
                if 'sources/' + path not in members:
                    add('sources/' + path, W / path, 'reviewed_source', shape(data))
                else:
                    assert members['sources/' + path] == data
    review = json.loads((B / 'bloom-resource-primary-review-r1.json').read_bytes())
    for path, expected in review['additional_sources'].items():
        add('sources/' + path, W / path, 'reviewed_source', expected)
    for path, expected in review['hash_dependency']['source_files_verified_against_archive'].items():
        add('dependencies/twox-hash-2.1.2/' + path,
            B / ('twox-hash-2.1.2-' + Path(path).name), 'locked_dependency_source', expected)
    add('evidence/guard_checks.py', B / 'guard_checks.py', 'supervision_source')
    add('supervision/guard_checks.py', Path('/Users/dylan/LocalData/shardloom/native-pipeline-sql-minute-2026-11-13/guard_checks.py'), 'supervision_source')
    add('supervision/release_validation_guard.py', Path('/Users/dylan/LocalData/shardloom/release-0.3.3-20260930/release_validation_guard.py'), 'supervision_source')
    add('supervision/bounded_build_process_resume_20260912.py', Path('/Users/dylan/LocalData/shardloom/perf-all-20260906/bounded_build_process_resume_20260912.py'), 'supervision_source')
    # The reused supervisor imports this module before the wrapper replaces ROOT.
    actual_storage = Path('/Users/dylan/.codex/worktrees/native-vector-representations/shardloom-local-repo/scripts/local_uat_storage.py')
    assert actual_storage.read_bytes() == (W / 'scripts/local_uat_storage.py').read_bytes()
    add('sources/scripts/local_uat_storage.py', actual_storage, 'supervision_source')
    add('evidence/bloom-packet-inventory-r1.json', B / 'bloom-packet-inventory-r1.json', 'original_inventory')
    for name in ('bloom-packet-build-r1-failure.json', 'build_bloom_packet_initial.py',
                 'bloom-packet-build-r2-failure.json', 'build_bloom_packet_before_complete_union.py'):
        add('evidence/' + name, B / name, 'original_packaging_failure')
    correction = dict(status='passed_primary_inventory_repair', original_members=63,
        original_sha256=shape((B / 'bloom-packet-inventory-r1.json').read_bytes())['sha256'],
        original_gap='Missing 14 registration sources, allocator and hash sources, and supervision wrapper; primary completes the exact union and adds supervision dependencies.',
        added_sources=additions, original_records_preserved=True)
    save(B / 'bloom-packet-inventory-primary-review-r1.json', correction)
    add('evidence/bloom-packet-inventory-primary-review-r1.json', B / 'bloom-packet-inventory-primary-review-r1.json', 'primary_inventory_review')
    spec = importlib.util.spec_from_file_location('bloom_packet_verify', B / 'verify_bloom_admission.py')
    verifier = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(verifier)
    result = verifier.verify_payload(members)
    save(B / 'bloom-packet-inventory-r2.json', dict(schema=inventory['schema'], base=inventory['base'], tree=inventory['tree'],
        count=len(members), total_bytes=sum(map(len, members.values())), members=[origins[k] for k in sorted(origins)]))
    OUT.mkdir(parents=True, exist_ok=True)
    archive = OUT / 'evidence.tar.xz'
    assert not archive.exists(), 'do not replace an existing frozen packet'
    with tarfile.open(archive, 'w:xz', preset=6, format=tarfile.PAX_FORMAT) as tar:
        for name, data in sorted(members.items()):
            entry = tarfile.TarInfo(name)
            entry.size = len(data)
            entry.mode = 0o644
            tar.addfile(entry, io.BytesIO(data))
    shutil.copyfile(B / 'verify_bloom_admission.py', OUT / 'verify.py')
    manifest = dict(schema='shardloom.bloom_admission_packet.v1', base=inventory['base'], tree=inventory['tree'],
        archive=dict(path=archive.name, **shape(archive.read_bytes())),
        verifier=shape((OUT / 'verify.py').read_bytes()),
        members={name:shape(data) for name,data in sorted(members.items())},
        claim_scope='Saved source-bound admission evidence, not an independent runtime rerun or a complete build-source bundle.')
    save(OUT / 'manifest.json', manifest)
    print(json.dumps(dict(status='passed_packet_construction', members=len(members), archive=manifest['archive'], verification=result), sort_keys=True))


if __name__ == '__main__':
    main()
