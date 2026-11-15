"""Install the unchanged observation packet and completed verification supplement."""
import hashlib
import json
from pathlib import Path
import shutil

from source_facts_runtime import OUT, ROOT, verify_runtime


def identity(path):
    with path.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    return dict(bytes=path.stat().st_size, sha256=digest)


def read(path):
    return json.loads(path.read_bytes())


def main():
    assert __debug__
    runtime, _ = verify_runtime('runtime-r4')
    inventory = read(OUT / 'source-facts-doc-inventory-r1.json')
    primary = read(OUT / 'source-facts-doc-primary-r1.json')
    assert primary['status'] == 'passed_primary_document_inventory_verification'
    assert primary['inventory'] == identity(OUT / 'source-facts-doc-inventory-r1.json')
    for name, digest in inventory['source_sha256'].items():
        assert identity(ROOT / name)['sha256'] == digest, name
    normalized = lambda text: ' '.join(text.split())
    for row in inventory['current_status_passages_to_change']:
        assert normalized(row['literal_stale_text']) in normalized((ROOT / row['path']).read_text())
    controls = read(OUT / 'source-facts-packet-controls-r1.json')
    assert controls['status'] == 'passed' and controls['controls_rejected'] == 50
    assert controls['original_packet_unchanged'] and controls['original_source_and_runtime_reads_forbidden']
    disposition = read(OUT / 'source-facts-disposition-r1.json')
    preserved = read(OUT / 'source-facts-source-r1.json')
    assert disposition['disposition'] in {'retain', 'drop'}
    assert disposition['candidate_source'] == preserved
    assert disposition['work_requirements_passed']
    fresh, lifecycle = disposition['fresh'], disposition['lifecycle']
    assert fresh['complete'] and lifecycle['complete']
    source = OUT / 'packet-r1'
    original = {p.name: identity(p) for p in source.iterdir() if p.is_file()}
    assert controls['manifest'] == original['manifest.json']
    destination = ROOT / 'docs/benchmarks/evidence/native-source-domain-facts-2026-11-14'
    assert not destination.exists()
    destination.mkdir()
    shutil.move(str(source), destination / 'packet')
    assert original == {p.name: identity(p) for p in (destination / 'packet').iterdir() if p.is_file()}
    supplement = destination / 'supplement'
    (supplement / 'logs').mkdir(parents=True)
    names = (
        'source-facts-source-r1.json', 'source-facts-disposition-r1.json', 'source-facts-source-preservation-r1.json',
        'source-facts-package-r1.json', 'source-facts-portable-verification-r1.json', 'source-facts-portable-replay-r1.json',
        'source-facts-packet-control-verification-r1.json', 'source-facts-packet-controls-r1.json',
        'source-facts-packet-relocated-r1.json', 'test_source_facts_packet.py',
        'install_source_facts_packet.py',
        'source-facts-doc-inventory-r1.json', 'source-facts-doc-inventory-r1.md', 'source-facts-doc-primary-r1.json',
    )
    for name in names:
        shutil.copyfile(OUT / name, supplement / name)
        assert identity(OUT / name) == identity(supplement / name)
    for name in ('source-facts-source-preservation-r1.log', 'source-facts-package-r1.log',
                 'source-facts-portable-verification-r1.log', 'source-facts-packet-control-verification-r1.log'):
        shutil.copyfile(OUT / 'logs' / name, supplement / 'logs' / name)
        assert identity(OUT / 'logs' / name) == identity(supplement / 'logs' / name)
    shutil.copyfile(OUT / 'verify_source_facts_closeout.py', destination / 'verify.py')
    (destination / 'README.md').write_text(f'''<!-- SPDX-License-Identifier: Apache-2.0 -->

# Exact source-domain predicate experiment evidence

The fixed experiment's disposition is **{disposition['disposition']}**. Its exact
measured source is preserved at `{preserved['head']}`. Run `python3 -I -B verify.py`
with Python 3.13 or newer from this directory. The verifier checks the packet
and supplement, replays all 5,572 complete results and 240 prepared lifecycle
operations, and recomputes the frozen decision without loading an engine or
executing queries.

The [comparison](../../native-source-domain-facts-2026-11-14.md) and
[architecture disposition](../../../architecture/native-source-domain-facts-2026-11-14.md)
record the contract and its limits. Complete fresh engine time is
{fresh['baseline_engine_seconds']:.9f} -> {fresh['candidate_engine_seconds']:.9f}
seconds. Separate complete lifecycle engine time is
{lifecycle['baseline_engine_seconds']:.9f} -> {lifecycle['candidate_engine_seconds']:.9f}
seconds. Both totals must decrease to retain the candidate. All
{fresh['regressions']} individual fresh regressions remain in the evidence.
These populations are not a Full43 or full10k observation.

Each runtime has 2,786 declarations: 746 original predicate and computed-float
barrier controls plus 2,040 versioned declarations across 12 source profiles,
two sizes, 17 consumer forms and all five public surfaces. The 336 accepted
dictionary-runtime baseline observations are reused byte-for-byte and remain
part of the complete baseline total; 2,450 new baseline and 2,786 candidate
requests run once. Sixty ordinary prepared plans per runtime execute two scored
calls. No warmups, successful-observation repeats or cache manipulation are added.

The finite-source requirements include 620 admitted declarations and 150 actual
compressed comparison dispatch witnesses. Exact held-footer finite-float facts
enable reviewed ALP predicates with signed-zero normalization; FastLanes integer
controls preserve existing semantics. Missing/inexact facts, lossy literals and
computed-result barriers keep their existing typed path. Counters distinguish
successful adaptor dispatches from fact admission. Repeated or nested logical
operand rows remain counted; baseline encoded counters are unavailable.
No decoded-byte, unique-row, physical-read or process-RSS reduction is inferred.

`packet/` preserves every original archive part and member identity, complete
raw ledgers and independent references, exact candidate source and patch,
validation logs, failed attempts, and 28 saved-evidence controls. All
{preserved['committed_blobs_verified']} captured source files were checked
against committed blobs. The build retains its actual `e5a3c3440` parent and
dirty-source identity; the preservation commit is not presented as its build
revision. Earlier memory-credit assertion failures and the provider lint
failure remain with exact source snapshots. The original provider archive and
every fixture readback are verified independently. Large generated input,
runtime and environment payloads remain explicit identity-only artifacts.
Checked-in source fixture bytes remain present.
The r3 package-notice failure is preserved; r4 corrects notice provenance only, with engine bytes identical to r3.

`supplement/` binds completed packaging, all 50 packet controls and relocated
verification with original source/runtime/evidence reads forbidden. Synthetic
control values never replace observed measurements. Cache and host state were
not controlled; a single comparison does not establish the cause of a regression.

The remaining Bloom/text-zone, pruning and batching, FSST, required spatial,
codec and CPU-fusion work, item 19/20 and CG-1 through CG-23 remain open.
This packet does not imply a merge, publication, release or competitive claim.
''')
    manifest = dict(schema='shardloom.native_source_facts_closeout_packet.v1',
        files={p.relative_to(destination).as_posix(): identity(p)
               for p in sorted(destination.rglob('*')) if p.is_file()},
        original_packet_moved_without_changes=True, query_executions=0, publication_attempted=False)
    with (destination / 'manifest.json').open('x') as stream:
        json.dump(manifest, stream, indent=2)
        stream.write('\n')
    assert verify_runtime('runtime-r4')[0] == runtime
    result = dict(status='installed_exact_packet', packet=str(destination), files=len(manifest['files']),
        manifest=identity(destination / 'manifest.json'), original_packet_unchanged=True,
        doc_source_hashes_verified=len(inventory['source_sha256']),
        doc_literals_verified=len(inventory['current_status_passages_to_change']),
        query_executions=0, publication_attempted=False)
    with (OUT / 'source-facts-packet-installation-r1.json').open('x') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
