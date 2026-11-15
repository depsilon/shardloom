"""Complete the offline import closure while preserving the original archive bytes."""
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
    out, root, c = support.OUT, support.ROOT, support.C
    candidate, build = verify_runtime('runtime-r3')
    checked_receipt(out / 'text-package-check-r1.json', build['source_files'])
    failed = read(out / 'text-packet-controls-check-r1.json')
    assert failed['status'] == 'failed' and failed['returncode'] == 1
    assert failed['sources_unchanged'] and failed['process_group_drained'] and failed['direct_child_reaped']
    failure = read(out / 'text-packet-relocated-r1.json')
    assert failure['returncode'] == 1 and failure['original_paths_forbidden']
    assert "No module named 'source_facts_observations'" in failure['stderr']
    original, destination = out / 'text-packet-r1', out / 'text-packet-r2'
    assert not destination.exists()
    old_manifest = read(original / 'manifest.json')
    before = {path.name: shape(identity(path)) for path in original.iterdir() if path.is_file()}
    assert old_manifest['files'] == {name: value for name, value in before.items() if name != 'manifest.json'}
    protocol = support.load_protocol(out / 'text-protocol-r3.json')
    frozen = {row['path']: row for row in protocol['frozen_files']}
    closeout_path = c / 'source-facts-closeout-r1.json'
    assert identity(closeout_path) == frozen[str(closeout_path)]
    closeout = read(closeout_path)
    control_path = c / 'source-facts-packet-controls-r1.json'
    controls = read(control_path)
    assert shape(identity(control_path)) == closeout['inputs'][control_path.name]
    prior_manifest_path = root / 'docs/benchmarks/evidence/native-source-domain-facts-2026-11-14/packet/manifest.json'
    assert shape(identity(prior_manifest_path)) == controls['manifest']
    prior = read(prior_manifest_path)
    missing = c / 'source_facts_observations.py'
    (record,) = (row['identity'] for row in prior['archive_members'].values() if row['origin'] == str(missing))
    assert identity(missing) == record
    assert str(missing) not in {row['origin'] for row in old_manifest['archive_members'].values()}
    destination.mkdir()
    # The large immutable transport parts are byte-identical links, not recompressed evidence.
    for name in old_manifest['archive']['parts']:
        os.link(original / name, destination / name)
        assert shape(identity(destination / name)) == old_manifest['files'][name]
    supplemental = []

    def add(path, transport):
        path = Path(path)
        origin = str(path)
        current = identity(path)
        assert origin not in {row['origin'] for row in supplemental}
        archive_name = ('data/' + path.relative_to(out.parent).as_posix() if path.is_relative_to(out.parent)
            else 'extra/' + hashlib.sha256(origin.encode()).hexdigest() + '/' + path.name)
        shutil.copyfile(path, destination / transport)
        assert shape(identity(destination / transport)) == shape(current)
        supplemental.append(dict(origin=origin, identity=current, archive_name=archive_name, transport=transport))

    add(missing, 'supplement-source_facts_observations.py')
    add(control_path, 'supplement-source-facts-packet-controls-r1.json')
    add(original / 'manifest.json', 'original-manifest.json')
    add(original / 'verify.py', 'original-verify.py')
    add(original / 'README.md', 'original-README.md')
    for name in ('portable_text_v2.py', 'test_text_packet_v2.py', 'repair_text_package.py',
        'text-packet-relocated-r1.json', 'text-packet-controls-check-r1.json', 'text-package-check-r1.json'):
        add(out / name, 'supplement-' + name)
    add(out / 'logs/text-packet-controls-check-r1.log', 'supplement-text-packet-controls-check-r1.log')
    shutil.copyfile(out / 'portable_text_v2.py', destination / 'verify.py')
    (destination / 'README.md').write_text('''# Text-summary evidence with repaired offline import closure

Run `python3 -I -B verify.py` here with Python 3.13 or newer. The complete decision remains
**drop**. The original thirteen archive parts are byte-identical; source, query observations,
reference values, protocols, clocks, acceptance rules and failed records are unchanged.

The first relocated verification failed because the shared `source_facts_observations.py`
module was absent from the archive. Its exact bytes are now supplied separately and bound
through the frozen prior source/domain closeout, that closeout's packet-control receipt, and
the accepted prior packet manifest. This repairs only offline import closure. The original
failed verifier, manifest, README, failure output and guard receipt are retained. All extra
files have explicit transport/member identities and cannot replace an archived member.

The following original README describes the unchanged measurements and their limits:

'''+(original / 'README.md').read_text())
    manifest = dict(old_manifest)
    manifest.update(schema='shardloom.native_text_signature_portable.v2', supplemental_members=supplemental,
        archive_repair=dict(original_packet_manifest=identity(original / 'manifest.json'),
            missing_shared_module=record, failure_record=identity(out / 'text-packet-relocated-r1.json'),
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
        original_archive_bytes_unchanged=True, transport_parts=len(old_manifest['archive']['parts']),
        added_members=len(supplemental), missing_module=record,
        prior_closeout=identity(closeout_path), prior_controls=identity(control_path), prior_manifest=identity(prior_manifest_path))
    with (out / 'text-package-repair-r1.json').open('x') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(json.dumps(dict(status=result['status'], added_members=len(supplemental), query_executions=0)))


if __name__ == '__main__':
    main()
