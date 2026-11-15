"""Install the unchanged FSST packet and completed control evidence."""
import hashlib
import json
from pathlib import Path
import shutil

ROOT = Path('/Users/dylan/LocalData/shardloom/optimization-investigation-queue-worktree')
OUT = Path(__file__).resolve().parent
DEST = ROOT / 'docs/benchmarks/evidence/native-fsst-consumer-2026-11-15'


def content(path):
    with path.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    return dict(bytes=path.stat().st_size, sha256=digest)


def read(path):
    return json.loads(path.read_bytes())


def main():
    assert __debug__ and not DEST.exists() and shutil.disk_usage(OUT).free >= 12 << 30
    controls = read(OUT / 'fsst-packet-controls-r2.json')
    repairs = read(OUT / 'fsst-repair-controls-r1.json')
    assert controls['status'] == 'passed_packet_corruption_controls' and controls['count'] == 57
    assert repairs['status'] == 'passed_repair_provenance_controls' and repairs['count'] == 8
    assert controls['original_packet_unchanged'] and repairs['original_packet_unchanged']
    source = OUT / 'fsst-packet-r2'
    original = {path.name: content(path) for path in source.iterdir() if path.is_file()}
    assert controls['manifest'] == repairs['manifest'] == original['manifest.json']
    DEST.mkdir()
    shutil.move(str(source), DEST / 'packet')
    assert original == {path.name: content(path) for path in (DEST / 'packet').iterdir() if path.is_file()}
    supplement = DEST / 'supplement'
    (supplement / 'logs').mkdir(parents=True)
    names = (
        'fsst-source-r1.json', 'fsst-disposition-r1.json', 'fsst-source-preservation-r1.json',
        'fsst-package-check-r2.json', 'fsst-package-repair-check-r1.json', 'fsst-package-repair-r1.json',
        'fsst-packet-controls-check-r2.json', 'fsst-packet-controls-r2.json', 'fsst-packet-relocated-r2.json',
        'fsst-repair-controls-check-r1.json', 'fsst-repair-controls-r1.json',
        'test_fsst_packet_v2.py', 'test_fsst_repair.py', 'repair_fsst_package.py',
        'install_fsst_packet.py', 'fsst-doc-inventory-r1.json', 'fsst-doc-inventory-r1.md', 'fsst-doc-primary-r1.json',
    )
    for name in names:
        shutil.copyfile(OUT / name, supplement / name)
        assert content(OUT / name) == content(supplement / name)
    for name in ('fsst-source-preservation-r1', 'fsst-package-check-r2', 'fsst-package-repair-check-r1',
                 'fsst-packet-controls-check-r2', 'fsst-repair-controls-check-r1'):
        shutil.copyfile(OUT / 'logs' / (name + '.log'), supplement / 'logs' / (name + '.log'))
    shutil.copyfile(OUT / 'verify_fsst_closeout.py', DEST / 'verify.py')
    notices = DEST / 'notices'
    notices.mkdir()
    for name, path in (
        ('shardloom-LICENSE', ROOT / 'LICENSE'), ('shardloom-NOTICE', ROOT / 'NOTICE'),
        ('vortex-APACHE-2.0.txt', ROOT / 'vendor/vortex-fsst/LICENSE'),
        ('memchr-MIT.txt', Path(read(OUT / 'memchr-dependency-r1.json')['license_file']))):
        shutil.copyfile(path, notices / name)
    assert content(notices / 'memchr-MIT.txt')['sha256'] == read(OUT / 'memchr-dependency-r1.json')['license_sha256']
    (notices / 'provenance.json').write_text(json.dumps(dict(
        source_revision='45be8e0bd1d9cef920c88fa9bf91811deade41c4',
        local='ShardLoom source, tests, tools and evidence; Apache-2.0.',
        upstream='Pinned Vortex 0.93.0 sources and local modifications; Apache-2.0. Original and modified provenance retained.',
        dependency='Existing memchr 2.8.3 direct edge to vortex-fsst; MIT license option. No new resolved package.',
        archive_scope='All 5,794 measured repository source files, complete raw results/references and explicit external-payload identities.',
        no_external_implementation_copied=True), indent=2) + '\n')
    (DEST / 'README.md').write_text('''<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native FSST prefilter evidence

The finite candidate is **retained locally** at
`45be8e0bd1d9cef920c88fa9bf91811deade41c4`. Run `python3 -I -B verify.py` with
Python 3.13 or newer. Complete portable replay checks all 18,080 saved fresh
results and 1,080 prepared operations, independent values/schemas, native
certificates, resource policy, exact source and the frozen decision. It executes
no engine or query. Native binaries, inputs and the original consumer environment
are identity-only exclusions; their payloads need not exist for replay.

The [comparison](../../native-fsst-consumer-2026-11-15.md) and
[architecture contract](../../../architecture/native-fsst-consumer-2026-11-15.md)
define the boundary. Complete fresh engine time is 682.167470629 -> 663.612327731
seconds; separate prepared engine time is 0.520543159 -> 0.511136028 seconds.
All 1,759 individual fresh regressions remain in the decision. A reused baseline
and uncontrolled host/cache state prevent causal attribution of the total gain.
This is the frozen 9,040-declaration population, not Full43 or a complete 10k pass.

Each candidate request executes once. All 9,040 baseline results and 540 baseline
prepared calls are reused with exact worker/request/input/reference/runtime
bindings. The candidate executes 270 prepared plans with two calls each. There
are no warmups, repeated successful observations, cache changes or timing-based
selection. All active query stages retain the original lightweight supervision.

The conservative necessary-byte filter stays inside the admitted positive FSST
source LIKE route. A rejection can skip matcher construction or exact row work;
every surviving candidate still uses the original exact matcher. Prefix, negated,
unsupported and shared projection routes preserve their existing behavior.
The separate untimed diagnostic replays 864 physical/shared declarations and
378 complete native-core requests against 288 nullable Boolean vectors. Its
session observer measures logical calls/rows, not decoded bytes, and compiles
out of release builds. Per-request release prefilter counters were not measured.
Existing provider scratch exclusions remain; full-provider allocation accounting
is not claimed. No writer default, new scalar bridge or fallback engine is added.

`packet/` preserves the nine original archive parts, all 6,363 archived members,
21 additive repair members, source/patch, validation and failed attempts. The
first relocated replay exposed two missing shared validators. Their exact bytes
are now bound through the original baseline protocol to the accepted dictionary
and source-domain closeouts, controls and manifests. All 62 local imports pass
with original paths inaccessible. Original archive bytes and measurements are
unchanged; the failed replay remains recorded.

`supplement/` binds the completed portable replay, 57 corruption controls and
eight additional repair-provenance controls. Twelve prospective synthetic
acceptance controls are in the original archive. Required checks passed: 3,344
default tests, 7,266 native tests, 113 provider tests, 455 installed-wheel tests,
strict lint, feature boundaries and supported-Rust checks. The 41 native and one
provider intentional ignores are explicit. The exact measured source was built
from parent `873e60bca` plus its recorded source changes; the later preservation
commit is not represented as the build revision.

The initial packaging storage refusal is preserved. Verified retirement removed
18,398 idle regenerable debug files after repeated identities and closed-handle
checks. Measured inputs, runtimes, tested wheel and release cache were unchanged.
The original inventory comparison stop is also retained. No storage limit changed.

Exact-table matcher lifetime, broader I attribution/batching, required spatial
execution, optional spatial summaries, codec/fusion work, item 20, RFC 0044 and
CG-1 through CG-23 remain open. This evidence implies no merge or release.
''')
    manifest = dict(schema='shardloom.native_fsst_closeout_packet.v1',
        closeout_state='pending_document_verification', original_packet_moved_without_changes=True,
        query_executions=0, publication_attempted=False, whole_investigation_queue_complete=False,
        files={path.relative_to(DEST).as_posix(): content(path) for path in sorted(DEST.rglob('*')) if path.is_file()})
    with (DEST / 'manifest.json').open('x') as stream:
        json.dump(manifest, stream, indent=2); stream.write('\n')
    with (OUT / 'fsst-packet-installation-r1.json').open('x') as stream:
        json.dump(dict(status='installed_exact_packet_pending_document_closeout', destination=str(DEST),
            manifest=content(DEST / 'manifest.json'), original_packet_unchanged=True,
            query_executions=0, files=len(manifest['files'])), stream, indent=2); stream.write('\n')
    print(json.dumps(dict(status='installed_exact_packet_pending_document_closeout', files=len(manifest['files']), query_executions=0)))


if __name__ == '__main__':
    main()
