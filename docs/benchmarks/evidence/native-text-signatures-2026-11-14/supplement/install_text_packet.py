"""Install the verified immutable text-summary archive and its final checks."""
import hashlib
import json
from pathlib import Path
import shutil

from text_runtime import OUT, ROOT, verify_runtime


def identity(path):
    with path.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    return dict(bytes=path.stat().st_size, sha256=digest)


def read(path):
    return json.loads(path.read_bytes())


def main():
    assert __debug__
    runtime, _ = verify_runtime('runtime-r3')
    inventory = read(OUT / 'text-doc-inventory-r1.json')
    primary = read(OUT / 'text-doc-primary-r1.json')
    assert primary['status'] == 'passed_primary_document_inventory_verification'
    assert primary['inventory'] == identity(OUT / 'text-doc-inventory-r1.json')
    for row in inventory['findings']:
        path = Path(row['path'])
        assert identity(path)['sha256'] == row['sha256']
        assert path.read_text().splitlines()[row['lines']['start']-1:row['lines']['end']] == row['literal_lines']
    controls = read(OUT / 'text-packet-controls-r3.json')
    assert controls['status'] == 'passed_packet_corruption_controls' and controls['count'] == 61
    assert controls['original_packet_unchanged'] and controls['original_source_runtime_evidence_reads_forbidden']
    assert controls['relocated_complete_replay']['disposition'] == 'drop'
    disposition = read(OUT / 'text-disposition-r1.json')
    preserved = read(OUT / 'text-source-r1.json')
    assert disposition['disposition'] == 'drop' and disposition['candidate_source'] == preserved
    assert disposition['work_requirements_passed']
    fresh, lifecycle = disposition['fresh'], disposition['lifecycle']
    assert fresh['complete'] and lifecycle['complete']
    source = OUT / 'text-packet-r3'
    original = {path.name: identity(path) for path in source.iterdir() if path.is_file()}
    assert controls['manifest'] == original['manifest.json']
    destination = ROOT / 'docs/benchmarks/evidence/native-text-signatures-2026-11-14'
    assert not destination.exists()
    destination.mkdir()
    shutil.move(str(source), destination / 'packet')
    assert original == {path.name: identity(path) for path in (destination / 'packet').iterdir() if path.is_file()}
    supplement = destination / 'supplement'
    (supplement / 'logs').mkdir(parents=True)
    names = (
        'text-source-r1.json', 'text-disposition-r1.json', 'text-source-preservation-r1.json',
        'text-package-check-r1.json', 'text-package-repair-r1.json', 'text-package-repair-r2.json',
        'text-package-repair-check-r1.json', 'text-package-repair-check-r2.json',
        'text-packet-controls-check-r1.json', 'text-packet-controls-check-r2.json', 'text-packet-controls-check-r3.json',
        'text-packet-controls-r3.json', 'text-packet-relocated-r1.json', 'text-packet-relocated-r2.json',
        'text-packet-relocated-r3.json', 'test_text_packet_v3.py', 'install_text_packet.py',
        'text-doc-inventory-r1.json', 'text-doc-inventory-r1.md', 'text-doc-primary-r1.json',
    )
    for name in names:
        shutil.copyfile(OUT / name, supplement / name)
        assert identity(OUT / name) == identity(supplement / name)
    for name in ('text-source-preservation-r1.log', 'text-package-check-r1.log',
                 'text-package-repair-check-r1.log', 'text-package-repair-check-r2.log',
                 'text-packet-controls-check-r1.log', 'text-packet-controls-check-r2.log', 'text-packet-controls-check-r3.log'):
        shutil.copyfile(OUT / 'logs' / name, supplement / 'logs' / name)
        assert identity(OUT / 'logs' / name) == identity(supplement / 'logs' / name)
    shutil.copyfile(OUT / 'verify_text_closeout.py', destination / 'verify.py')
    (destination / 'README.md').write_text(f'''<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native text-signature experiment evidence

Disposition: **drop**. The complete fresh engine total rises from
{fresh['baseline_engine_seconds']:.9f} to {fresh['candidate_engine_seconds']:.9f}
seconds. The separate prepared-lifecycle total falls from
{lifecycle['baseline_engine_seconds']:.9f} to {lifecycle['candidate_engine_seconds']:.9f}
seconds. The frozen rule requires both totals to decrease. All
{fresh['regressions']:,} individual fresh regressions remain in the saved evidence.

Run `python3 -I -B verify.py` here with Python 3.13 or newer. The verifier
checks every packet and supplement file, replays all 18,080 complete fresh
results and 1,080 prepared operations, and recomputes the decision without
loading an engine or executing queries. Relocated replay also forbids original
source/runtime/evidence reads, network access and subprocesses. There are 28
saved-evidence controls and 61 packet corruption controls.

The [comparison](../../native-text-signatures-2026-11-14.md) and
[architecture disposition](../../../architecture/native-text-signatures-2026-11-14.md)
define the populations and limits. Each runtime has 9,040 fresh declarations:
4,150 original cases, 1,380 dictionary cases and 3,510 prospective text cases.
Each also executes 270 ordinary prepared plans twice. The text matrix spans
nine profiles, three physical encodings, two sizes, thirteen consumer forms
and all five public surfaces. Each declared successful request executes once.
The final baseline retains 4,631 complete verified observations from the first
attempt. Sixteen pre-collection helper failures, one interrupted capture and
4,392 requests that had not started are accounted for explicitly; no successful
observation is rerun. Original failures, partial ledgers and cleanup records
remain present. These populations are not a Full43 or full10k result.

All 840 required logical exclusion witnesses pass. The separate direct-provider
diagnostic includes 648 scans, 108 complete file readbacks and 324 paired reads:
36 pairs read fewer completed positional bytes, 103 the same, and 185 more.
Logical zone exclusion is not physical-read or decode savings. Small public
fixtures are prefetched whole. Construction observations cover writer strategy
setup, push and finish inside an already registered session; session startup,
registration, input encoding, open/sync and validation are outside that clock.
No complete first-use construction or break-even claim is made.

`packet/` preserves the original thirteen transport parts byte-for-byte, complete
raw ledgers and independent references, 46 candidate source changes and their
patch, build/validation logs, every failed attempt and source revision. The
candidate is preserved at `{preserved['head']}`; all 5,823 captured source files
match its Git blobs. The build retains its actual accepted parent and dirty-source
identity. No rebuild occurred to manufacture a commit identity.
Large generated fixtures, native binaries and environment payloads remain explicit
identity-only artifacts; fixture readbacks and checked-in source fixtures are present.

Two relocated checks initially failed on missing shared Python verification
modules. Both failure records and original verifier/manifest/README bytes remain
bound. The added modules are byte-exact to the already frozen accepted source-domain
and dictionary packets, through their closeout and packet-control receipts.
The repairs change only offline verification packaging. Protocols, source, results,
clocks, thresholds and disposition are unchanged. No engine queries were rerun.

`supplement/` binds the completed package checks and source preservation. The
declined implementation remains on its experimental branch; subsequent source
restoration and the continuing queue are recorded in the architecture note.
Cache and host state were uncontrolled. A single comparison does not establish
the cause of an individual regression. I read/consumer attribution and survivor
batching are next, followed by FSST, required spatial execution, optional spatial
summaries, codec and CPU-fusion work. The parent queue, item 19/20, RFC 0044 and
CG-1 through CG-23 retain their obligations. No publication or release is implied.
''')
    manifest = dict(schema='shardloom.native_text_signature_closeout.v1',
        files={path.relative_to(destination).as_posix(): identity(path)
               for path in sorted(destination.rglob('*')) if path.is_file()},
        verified_packet_moved_without_changes=True, query_executions=0, publication_attempted=False)
    with (destination / 'manifest.json').open('x') as stream:
        json.dump(manifest, stream, indent=2)
        stream.write('\n')
    assert verify_runtime('runtime-r3')[0] == runtime
    result = dict(status='installed_exact_packet', packet=str(destination), files=len(manifest['files']),
        manifest=identity(destination / 'manifest.json'), original_packet_unchanged=True,
        doc_excerpts_verified=17, query_executions=0, publication_attempted=False)
    with (OUT / 'text-packet-installation-r1.json').open('x') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
