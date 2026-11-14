"""Install the unchanged observation packet and completed verification supplement."""
import hashlib
import json
from pathlib import Path
import shutil

from dictionary_runtime import OUT, ROOT, verify_runtime


def identity(path):
    with path.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    return dict(bytes=path.stat().st_size, sha256=digest)


def read(path):
    return json.loads(path.read_bytes())


def main():
    assert __debug__
    runtime, _ = verify_runtime('runtime-r1')
    inventory = read(OUT / 'dictionary-doc-inventory-r1.json')
    primary = read(OUT / 'dictionary-doc-primary-r1.json')
    assert primary['status'] == 'passed_primary_document_inventory_verification'
    assert primary['inventory'] == identity(OUT / 'dictionary-doc-inventory-r1.json')
    for name, digest in inventory['source_sha256'].items():
        assert identity(ROOT / name)['sha256'] == digest, name
    normalized = lambda text: ' '.join(text.split())
    for row in inventory['current_status_passages_to_change']:
        assert normalized(row['literal_stale_text']) in normalized((ROOT / row['path']).read_text())
    controls = read(OUT / 'dictionary-packet-controls-r1.json')
    assert controls['status'] == 'passed' and controls['controls_rejected'] == 36
    assert controls['original_packet_unchanged'] and controls['original_source_and_runtime_reads_forbidden']
    disposition = read(OUT / 'dictionary-disposition-r1.json')
    preserved = read(OUT / 'dictionary-source-r1.json')
    assert disposition['disposition'] in {'retain', 'drop'}
    assert disposition['candidate_source'] == preserved
    assert disposition['work_requirements_passed']
    fresh, lifecycle = disposition['fresh'], disposition['lifecycle']
    assert fresh['complete'] and lifecycle['complete']
    source = OUT / 'packet-r1'
    original = {p.name: identity(p) for p in source.iterdir() if p.is_file()}
    assert controls['manifest'] == original['manifest.json']
    destination = ROOT / 'docs/benchmarks/evidence/native-dictionary-domain-2026-11-14'
    assert not destination.exists()
    destination.mkdir()
    shutil.move(str(source), destination / 'packet')
    assert original == {p.name: identity(p) for p in (destination / 'packet').iterdir() if p.is_file()}
    supplement = destination / 'supplement'
    (supplement / 'logs').mkdir(parents=True)
    names = (
        'dictionary-source-r1.json', 'dictionary-disposition-r1.json', 'dictionary-source-preservation-r1.json',
        'dictionary-package-r1.json', 'dictionary-portable-verification-r1.json', 'dictionary-portable-replay-r1.json',
        'dictionary-packet-control-verification-r1.json', 'dictionary-packet-controls-r1.json',
        'dictionary-packet-relocated-r1.json', 'test_dictionary_packet.py',
        'install_dictionary_packet.py',
        'dictionary-doc-inventory-r1.json', 'dictionary-doc-inventory-r1.md', 'dictionary-doc-primary-r1.json',
    )
    for name in names:
        shutil.copyfile(OUT / name, supplement / name)
        assert identity(OUT / name) == identity(supplement / name)
    for name in ('dictionary-source-preservation-r1.log', 'dictionary-package-r1.log',
                 'dictionary-portable-verification-r1.log', 'dictionary-packet-control-verification-r1.log'):
        shutil.copyfile(OUT / 'logs' / name, supplement / 'logs' / name)
        assert identity(OUT / 'logs' / name) == identity(supplement / 'logs' / name)
    shutil.copyfile(OUT / 'verify_dictionary_closeout.py', destination / 'verify.py')
    (destination / 'README.md').write_text(f'''<!-- SPDX-License-Identifier: Apache-2.0 -->

# Dictionary-domain text experiment evidence

The fixed experiment's disposition is **{disposition['disposition']}**. Its exact
measured source is preserved at `{preserved['head']}`. Run `python3 -I -B verify.py`
with Python 3.13 or newer from this directory. The verifier checks the packet and
supplement, replays all 11,060 complete results and 80 prepared lifecycle
operations, and recomputes the frozen decision without loading an engine or
executing queries.

The [comparison](../../native-dictionary-domain-2026-11-14.md) and
[architecture disposition](../../../architecture/native-dictionary-domain-2026-11-14.md)
record the contract and its limits. Complete fresh engine time is
{fresh['baseline_engine_seconds']:.9f} -> {fresh['candidate_engine_seconds']:.9f}
seconds. Separate complete lifecycle engine time is
{lifecycle['baseline_engine_seconds']:.9f} -> {lifecycle['candidate_engine_seconds']:.9f}
seconds. Both totals must decrease to retain the candidate. All
{fresh['regressions']} individual fresh regressions remain in the evidence. This
is neither a Full43 nor a full10k observation.

The 5,530-case cohort combines 4,150 unchanged saved declarations and 1,380
versioned dictionary consumers across all five public surfaces. Of the new
requests, 630 require real dictionary activation and 210 require fewer completed
text-kernel calls than logical expression rows. Those work witnesses preserve
codes, exact transformed equality and ordinary output/null semantics. They do
not measure baseline kernel-call counts, zero-decode execution, physical device
bytes or process RSS. Union, selected, flat, unique and empty controls retain
their existing paths. The baseline reuses 4,903 verified packets: 1,807 historical
observations and 3,096 successful observations from the stopped capture.
Recovery executes 627 requests: 599 never started, 27 regex-adapter failures
before collect, and one oversized evidence capture requiring a justified retry.
The original oversized output and clock remain preserved and unaccepted; no
successful observation is repeated or chosen by timing. All 5,530 candidate
requests run once. Twenty prepared plans per runtime each execute two scored calls.

The versioned harness corrects the public DataFrame regex argument form and
losslessly compresses oversized diagnostic records. Complete rows, schemas,
certificates and clocks survive exact decode. The original 8 MiB wire and 9 MiB
raw-member limits stay fixed; decoded artifact memory is bounded at 64 MiB.
All 48 regex-containing DataFrame declarations and 12 transport corruption
controls pass offline checks. These are harness changes, with no runtime code,
input, independent reference or acceptance-rule change.

`packet/` preserves every original archive part and member identity, a standalone
verifier, complete raw ledgers and independent references, candidate source and
patch, validation logs and failed attempts, and 28 saved-evidence controls.
All {preserved['committed_blobs_verified']} captured source files were checked
against committed blobs. The build retains its actual `c74d0ebb4` parent and
dirty-source identity; the preservation commit is not presented as the original
build revision. Large input, runtime and environment payloads remain explicit
identity-only artifacts. Portable replay does not reopen those payloads.

`supplement/` binds completed packaging, all 36 packet controls, and relocated
verification with original workspace/runtime/evidence reads forbidden. The
original native-suite supervision interruption is preserved alongside the
subsequent complete passing suite. No failed observation, acceptance threshold
or timing value is rewritten. Synthetic control values never replace observed
measurements. Cache and host state were not controlled; a single comparison does
not establish the cause of an individual regression.

The remaining source/domain, encoded predicate, batching, FSST, spatial and
codec/fusion work, item 19/20 and CG-1 through CG-23 remain open. No PR merge,
publication, release, writer-default change or competitive superiority is implied.
''')
    manifest = dict(schema='shardloom.native_dictionary_closeout_packet.v1',
        files={p.relative_to(destination).as_posix(): identity(p)
               for p in sorted(destination.rglob('*')) if p.is_file()},
        original_packet_moved_without_changes=True, query_executions=0, publication_attempted=False)
    with (destination / 'manifest.json').open('x') as stream:
        json.dump(manifest, stream, indent=2)
        stream.write('\n')
    assert verify_runtime('runtime-r1')[0] == runtime
    result = dict(status='installed_exact_packet', packet=str(destination), files=len(manifest['files']),
        manifest=identity(destination / 'manifest.json'), original_packet_unchanged=True,
        doc_source_hashes_verified=len(inventory['source_sha256']),
        doc_literals_verified=len(inventory['current_status_passages_to_change']),
        query_executions=0, publication_attempted=False)
    with (OUT / 'dictionary-packet-installation-r1.json').open('x') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
