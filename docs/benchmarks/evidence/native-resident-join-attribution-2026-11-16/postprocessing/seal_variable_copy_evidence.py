"""Seal the successful packet and subsequent verification without altering it."""
import gzip
import hashlib
import json
from pathlib import Path
import shutil
import tarfile

OUT = Path(__file__).resolve().parent.parent
PACKET = OUT / 'variable-copy-packet-r3'
DESTINATION = OUT / 'variable-copy-evidence-r2'


def content(path):
    with path.open('rb') as stream:
        return dict(bytes=path.stat().st_size, sha256=hashlib.file_digest(stream, 'sha256').hexdigest())


def main():
    assert not DESTINATION.exists()
    current = json.loads((PACKET / 'manifest.json').read_bytes())
    result = json.loads((OUT / 'variable-copy-portable-replay-r3.log').read_bytes())
    relocation = json.loads((OUT / 'variable-copy-relocation-verification-r1.json').read_bytes())
    controls = json.loads((OUT / 'variable-copy-transport-controls-r1.json').read_bytes())
    assert result['status'] == 'passed_complete_portable_replay'
    assert relocation['status'] == controls['status'] == 'passed'
    assert result['fresh_observations_replayed'] == 806 and controls['rejected_count'] == 19
    assert result['negative_controls']['rejected_count'] == 126
    assert result['negative_controls']['decision_boundary_controls'] == 7
    assert content(OUT / 'variable-copy-portable-replay-r3.log') == relocation['original_result']
    assert content(OUT / 'logs/variable-copy-portable-relocated-r1.log') == relocation['relocated_result']
    assert content(PACKET / 'manifest.json') == controls['packet_manifest']
    assert content(PACKET / 'manifest.json') == relocation['packet_files']['manifest.json']
    assert shutil.disk_usage(OUT).free >= (12 << 30) + (256 << 20)
    DESTINATION.mkdir()
    shutil.copytree(PACKET, DESTINATION / 'packet', copy_function=shutil.copyfile)
    mappings = {
        'variable-copy-portable-replay-r3.log': 'verification/original-replay.json',
        'logs/variable-copy-portable-relocated-r1.log': 'verification/relocated-replay.json',
        'variable-copy-relocation-verification-r1.json': 'verification/relocation.json',
        'variable-copy-portable-relocated-r1.json': 'verification/relocation-guard.json',
        'variable-copy-transport-controls-r1.json': 'verification/transport-controls.json',
        'portable-diagnostic-inventory-primary-verification-r1.json': 'verification/diagnostic-inventory-primary.json',
        'pr1693-primary-main-final-r1.json': 'verification/preceding-pr-main.json',
        'variable-copy-package-r1.log': 'failures/package-r1.log',
        'variable-copy-portable-replay-r2.log': 'failures/replay-r2.log',
        'variable-copy-seal-r1.log': 'failures/seal-r1.log',
        'postprocessing/check_variable_copy_transport.py': 'postprocessing/check_variable_copy_transport.py',
        'postprocessing/verify_variable_copy_relocation.py': 'postprocessing/verify_variable_copy_relocation.py',
        'postprocessing/seal_variable_copy_evidence.py': 'postprocessing/seal_variable_copy_evidence.py',
    }
    bindings = {}
    for source, target in mappings.items():
        destination = DESTINATION / target
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(OUT / source, destination)
        assert content(destination) == content(OUT / source)
        bindings[target] = dict(original_path=str(OUT / source), **content(destination))
    previous = OUT / 'variable-copy-packet-r2'
    old = json.loads((previous / 'manifest.json').read_bytes())
    for name in ('verify.py', 'propagation_portable_store.py', 'variable_copy_portable_store.py'):
        destination = DESTINATION / 'failures/packet-r2' / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(previous / name, destination)
    with gzip.GzipFile(filename=str(DESTINATION / 'failures/packet-r2/manifest.json.gz'), mode='wb', mtime=0) as stream:
        stream.write((previous / 'manifest.json').read_bytes())
    # Only the source members changed after the failed portable replay need a
    # second content copy. The original failed packet remains retained locally.
    old_changed = {n: r for n, r in old['archive_members'].items()
                   if n not in current['archive_members'] or r['identity']['sha256'] != current['archive_members'][n]['identity']['sha256']}
    with tempfile_archive(previous, old) as archive:
        for entry in archive:
            if entry.name in old_changed:
                raw = archive.extractfile(entry).read()
                assert hashlib.sha256(raw).hexdigest() == old_changed[entry.name]['identity']['sha256']
                target = DESTINATION / 'failures/packet-r2/changed-source' / Path(entry.name).name
                assert not target.exists()
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(raw)
    summary = dict(status='passed_local_and_portable_acceptance_hosted_integration_pending',
        candidate_commit='ffe0d077a676fd8b23cfebed9e11bf189e1ae459',
        baseline_commit='75596d0ee32e59ba77ea7cf4429166cb3052b3de',
        source_fingerprints=result['source_fingerprints'], fresh=result['fresh'],
        original_frozen_decision=result['decision'], query_executions_during_postprocessing=0,
        fresh_result_packets=806, diagnostic_requests=54, gather_requests=3,
        original_and_relocated_replay_passed=True, new_packet_corruptions_rejected=126,
        decision_boundary_controls=7, transport_corruptions_rejected=19,
        prior_source_and_guard_controls_retained=True,
        archive_members=len(current['archive_members']), external_identity_only_records=len(current['external_artifacts']),
        source_members={k:len(v) for k,v in current['source_snapshots'].items()},
        all_historical_build_source_bytes_reconstructed=True,
        historical_source_filesystem_generation_tuples_unavailable=24,
        absolute_gain_seconds=result['fresh']['engine_gain_seconds'],
        relative_gain_fraction=result['fresh']['engine_gain_seconds']/result['fresh']['baseline_engine_seconds'],
        material_performance_gain_claim=False, causal_speedup_claim=False,
        latest_maintainer_direction='Prioritize material performance gains, not further sliver gains; finish this already measured closeout and screen substantial work elimination before selecting another candidate.',
        new_numeric_material_threshold_not_specified=True,
        original_protocol_and_decision_unchanged=True, publication_attempted=False,
        bindings=bindings,
        failed_packet_r2_manifest=content(previous / 'manifest.json'),
        failed_packet_r2_retained_locally=str(previous), failed_packet_r2_changed_sources=old_changed)
    (DESTINATION / 'acceptance.json').write_text(json.dumps(summary, indent=2) + '\n')
    (DESTINATION / 'README.md').write_text('''# Resident join attribution and selected-variable copying

The measured candidate is `ffe0d077a676fd8b23cfebed9e11bf189e1ae459`.
Both complete 403-request runs pass independent result, schema, resource,
ownership, native-clock and no-fallback checks. The frozen decision retains the
candidate: 60.778284678 -> 60.597904154 engine seconds. All 186 individual
regressions remain in the complete result ledger. The gain is about 0.3%; this
is not a material-performance or repeatability claim.

The maintainer subsequently prioritizes material gains over further sliver
optimizations. That prospective direction is recorded separately; it changes
the next candidate selection, not this frozen observation or earlier decisions.

Run `python3 -I -B packet/verify.py` with Python 3.13 or newer. This reconstructs
all 806 complete packets, both complete source snapshots, the 54 corrected
instrumented diagnostics and three debug gather observations, checks the frozen
source/test/installed-package records, and rejects 126 fresh-packet corruptions
plus seven decision-boundary controls. It uses no execution library, native
binary, original data, subprocess or network request. Original and relocated
replay reports are under `verification/`.

Run the exact transport controls separately with:
`python3 -B postprocessing/check_variable_copy_transport.py --packet packet --output /tmp/variable-copy-transport-result.json`
The output must be a new path. Nineteen malformed archive/manifest controls are
rejected and one valid transport control passes. The controls use small
synthetic archives, not new query executions. Their source and report bind the
same transport bytes used by complete replay.

The inner packet manifest binds every part and extracted member. Large inputs,
native binaries, execution environments, wheel and original source archive
containers are identity-only exclusions. All source members and diagnostic
source variants are included. Complete historical build source bytes match;
24 preserved historical source records lack original filesystem generation
tuples, which are not reconstructed or claimed.

The original one-ULP diagnostic failure, source/test/build failures and retirement
inventories remain in the inner archive. Later packaging failures are recorded
under `failures/`, with their old manifest and changed source bytes; the complete
failed packet also remains retained at its recorded local path. The initial
packager compared access time; the corrected generation check excludes it.
The first portable replay lacked the inherited build guard and misresolved
relative cleanup reads; the final packet fixes both without changing any query.

`acceptance.json` and `files.json` bind subsequent verification and the preceding
PR's exact-main completion. They do not imply that this candidate is merged or
released. No full10k, full11k, Full43, competitor run or publication occurs here.
''')
    files = {p.relative_to(DESTINATION).as_posix():content(p) for p in DESTINATION.rglob('*') if p.is_file()}
    (DESTINATION / 'files.json').write_text(json.dumps(files, indent=2, sort_keys=True) + '\n')
    print(json.dumps(dict(status='sealed', path=str(DESTINATION), files=len(files)+1,
                         bytes=sum(r['bytes'] for r in files.values()), query_executions=0)))


from contextlib import contextmanager
import tempfile


@contextmanager
def tempfile_archive(packet, manifest):
    with tempfile.TemporaryFile() as joined:
        for name in manifest['archive']['parts']:
            assert content(packet / name) == manifest['files'][name]
            with (packet / name).open('rb') as source:
                shutil.copyfileobj(source, joined)
        joined.seek(0)
        with tarfile.open(fileobj=joined, mode='r:gz') as archive:
            yield archive


if __name__ == '__main__':
    main()
