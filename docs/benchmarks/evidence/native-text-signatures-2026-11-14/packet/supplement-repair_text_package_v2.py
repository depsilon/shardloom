"""Supply the prior dictionary verifier without changing any measured evidence."""
import hashlib
import json
import os
from pathlib import Path
import shutil

import text_recovery_support as support
from text_runtime import checked_receipt, verify_runtime
from query_surface.evidence import identity
from query_surface.expanded_freeze import read


def shape(record):
    return dict(bytes=record.get('bytes', record.get('generation', {}).get('bytes')), sha256=record['sha256'])


def main():
    out, root, n = support.OUT, support.ROOT, support.N
    candidate, build = verify_runtime('runtime-r3')
    checked_receipt(out / 'text-package-repair-check-r1.json', build['source_files'])
    failed = read(out / 'text-packet-controls-check-r2.json')
    assert failed['status'] == 'failed' and failed['returncode'] == 1
    assert failed['sources_unchanged'] and failed['process_group_drained'] and failed['direct_child_reaped']
    failure = read(out / 'text-packet-relocated-r2.json')
    assert failure['returncode'] == 1 and failure['original_paths_forbidden']
    assert "No module named 'dictionary_observations'" in failure['stderr']
    original, destination = out / 'text-packet-r2', out / 'text-packet-r3'
    assert not destination.exists()
    old = read(original / 'manifest.json')
    before = {path.name: shape(identity(path)) for path in original.iterdir() if path.is_file()}
    assert old['files'] == {name: value for name, value in before.items() if name != 'manifest.json'}
    protocol = support.load_protocol(out / 'text-protocol-r3.json')
    frozen = {row['path']: row for row in protocol['frozen_files']}
    closeout_path = n / 'dictionary-closeout-r1.json'
    assert identity(closeout_path) == frozen[str(closeout_path)]
    closeout = read(closeout_path)
    control_path = n / 'dictionary-packet-controls-r1.json'
    controls = read(control_path)
    assert shape(identity(control_path)) == closeout['inputs'][control_path.name]
    prior_path = root / 'docs/benchmarks/evidence/native-dictionary-domain-2026-11-14/packet/manifest.json'
    assert shape(identity(prior_path)) == controls['manifest']
    prior = read(prior_path)
    missing = n / 'dictionary_observations.py'
    (record,) = (row['identity'] for row in prior['archive_members'].values() if row['origin'] == str(missing))
    assert identity(missing) == record
    existing = {row['origin'] for row in old['archive_members'].values()} | {row['origin'] for row in old['supplemental_members']}
    assert str(missing) not in existing and str(control_path) not in existing
    review = read(out / 'text-import-primary-r1.json')
    assert review['status'] == 'passed_primary_import_inventory_verification'
    assert review['missing_replay_helpers'] == [str(missing)]
    assert review['inventory'] == shape(identity(out / 'text-import-inventory-r1.json'))
    checked_receipt(out / 'text-import-closure-check-r2.json', build['source_files'])
    destination.mkdir()
    # Keep all earlier payloads byte-identical, including the first repair/failure.
    for name in old['files']:
        target = {'verify.py': 'r2-verify.py', 'README.md': 'r2-README.md'}.get(name, name)
        os.link(original / name, destination / target)
        assert shape(identity(destination / target)) == old['files'][name]
    supplemental = list(old['supplemental_members'])

    def add(path, transport):
        path = Path(path)
        origin = str(path)
        assert origin not in existing
        existing.add(origin)
        current = identity(path)
        archive_name = ('data/' + path.relative_to(out.parent).as_posix() if path.is_relative_to(out.parent)
            else 'extra/' + hashlib.sha256(origin.encode()).hexdigest() + '/' + path.name)
        assert not (destination / transport).exists()
        shutil.copyfile(path, destination / transport)
        assert shape(identity(destination / transport)) == shape(current)
        supplemental.append(dict(origin=origin, identity=current, archive_name=archive_name, transport=transport))

    add(missing, 'supplement-dictionary_observations.py')
    add(control_path, 'supplement-dictionary-packet-controls-r1.json')
    add(original / 'manifest.json', 'r2-manifest.json')
    # Previous verifier/README already have byte-identical transport copies.
    for name in ('verify.py', 'README.md'):
        path = original / name
        supplemental.append(dict(origin=str(path), identity=identity(path),
            archive_name='data/' + path.relative_to(out.parent).as_posix(), transport='r2-' + name))
    for name in ('portable_text_v3.py', 'test_text_packet_v3.py', 'repair_text_package_v2.py',
        'text-import-inventory-r1.json', 'text-import-inventory-r1.md', 'text-import-primary-r1.json',
        'check_text_import_closure.py', 'text-import-closure-execution-r1.json',
        'text-import-closure-check-r1.json', 'text-import-closure-check-r2.json',
        'text-packet-relocated-r2.json', 'text-packet-controls-check-r2.json',
        'text-package-repair-check-r1.json', 'text-package-repair-r1.json'):
        add(out / name, 'supplement-' + name)
    for name in ('text-packet-controls-check-r2.log', 'text-package-repair-check-r1.log',
                 'text-import-closure-check-r1.log', 'text-import-closure-check-r2.log'):
        add(out / 'logs' / name, 'supplement-' + name)
    shutil.copyfile(out / 'portable_text_v3.py', destination / 'verify.py')
    (destination / 'README.md').write_text('''# Complete offline helper closure

Run `python3 -I -B verify.py` here with Python 3.13 or newer. Both earlier
relocated-import failures remain preserved. The second repair supplies
`dictionary_observations.py`, bound to the frozen accepted dictionary closeout,
its packet-control receipt, and its original packet manifest. All original
archive parts, source, observations, references, clocks and acceptance rules
are unchanged. The measured decision remains **drop**. No queries are rerun.

The previous README and repair explanation follow:

''' + (original / 'README.md').read_text())
    manifest = dict(old)
    manifest.update(schema='shardloom.native_text_signature_portable.v3', supplemental_members=supplemental,
        archive_repair_v2=dict(original_packet_manifest=identity(original / 'manifest.json'),
            missing_shared_module=record, failure_record=identity(out / 'text-packet-relocated-r2.json'),
            original_archive_bytes_unchanged=True, original_protocol_results_and_acceptance_unchanged=True,
            query_executions=0, helper=identity(Path(__file__))))
    manifest['files'] = {path.name: shape(identity(path)) for path in sorted(destination.iterdir()) if path.is_file()}
    with (destination / 'manifest.json').open('x') as stream:
        json.dump(manifest, stream, indent=2)
        stream.write('\n')
    assert before == {path.name: shape(identity(path)) for path in original.iterdir() if path.is_file()}
    assert verify_runtime('runtime-r3')[0] == candidate
    result = dict(status='completed_offline_import_closure_pending_replay', query_executions=0,
        original_manifest=identity(original / 'manifest.json'), corrected_manifest=identity(destination / 'manifest.json'),
        original_archive_bytes_unchanged=True, transport_parts=len(old['archive']['parts']),
        added_members=len(supplemental) - len(old['supplemental_members']), missing_module=record,
        prior_closeout=identity(closeout_path), prior_controls=identity(control_path), prior_manifest=identity(prior_path))
    with (out / 'text-package-repair-r2.json').open('x') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(json.dumps(dict(status=result['status'], added_members=result['added_members'], query_executions=0)))


if __name__ == '__main__':
    main()
