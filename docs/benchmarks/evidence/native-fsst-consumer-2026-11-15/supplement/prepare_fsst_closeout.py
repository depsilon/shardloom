"""Bind the documentation-only closeout to the tested source and portable packet."""
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
from urllib.parse import unquote, urlsplit

import guard_checks as guard

OUT, ROOT = guard.OUT, guard.ROOT
PACKET = ROOT / 'docs/benchmarks/evidence/native-fsst-consumer-2026-11-15'
ALLOWED = {
    'docs/architecture/native-fsst-consumer-2026-11-15.md',
    'docs/architecture/native-metadata-encoded-consumer-investigations-2026-11-13.md',
    'docs/architecture/phased-execution-plan.md',
    'docs/architecture/native-shared-pipeline-investigations-2026-11-10.md',
    'docs/architecture/native-structural-work-investigations-2026-11-11.md',
    'docs/architecture/local-development-storage.md',
    'docs/benchmarks/native-fsst-consumer-2026-11-15.md',
}


def content(path):
    with path.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    return dict(bytes=path.stat().st_size, sha256=digest)


def read(path):
    return json.loads(path.read_bytes())


def write_new(path, value):
    with path.open('x') as stream:
        json.dump(value, stream, indent=2); stream.write('\n')


def main():
    assert __debug__
    assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip() == '45be8e0bd1d9cef920c88fa9bf91811deade41c4'
    source = read(OUT / 'fsst-source-r1.json')
    runtime = read(OUT / 'runtime-r1-identity.json')
    build = read(OUT / runtime['build_receipt'])
    before = guard.sources()
    differences = {name: dict(executed=build['source_files'].get(name), current=before.get(name))
        for name in sorted(set(build['source_files']) | set(before))
        if build['source_files'].get(name) != before.get(name)}
    assert set(differences) == ALLOWED, differences.keys()
    native = {name: content(Path(name)) for name in runtime['files']}
    assert native == runtime['files']
    fingerprint = hashlib.sha256(json.dumps(build['source_files'], sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    assert fingerprint == source['source_fingerprint_sha256'] == runtime['source_fingerprint_sha256']
    write_new(OUT / 'fsst-final-source-r1.json', dict(status='passed_exact_runtime_and_document_only_delta',
        runtime_source_fingerprint_sha256=fingerprint, candidate_source_commit=source['head'],
        all_non_document_sources_unchanged=True, native_artifacts_unchanged=True,
        changed_source_paths=sorted(differences), differences=differences, native_artifacts=native,
        runtime_manifest=content(OUT / 'runtime-r1-identity.json'),
        current_source_fingerprint_sha256=hashlib.sha256(json.dumps(before, sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
        current_source_files=len(before), query_executions=0))
    public = read(OUT / 'fsst-public-status-docs-r1.json')
    gaps = read(OUT / 'fsst-gap-burndown-r1.json')
    tracker = read(OUT / 'fsst-queue-tracker-r1.json')
    assert public['status'] == gaps['status'] == 'passed' and public['blockers'] == gaps['blockers'] == []
    assert tracker['status'] == 'blocked' and not tracker['missing_required_inputs']
    assert len(tracker['blockers']) == 1
    match = re.fullmatch(r'phase plan has unchecked items: (\d+)', tracker['blockers'][0])
    assert match and int(match[1]) > 0
    assert tracker['phase_plan_queue_status'] == 'open_development_queue_blocks_release_claims'
    assert tracker['runtime_gap_family_burn_down_status'] == 'passed'
    links, missing = [], []
    documents = [ROOT / name for name in sorted(ALLOWED)] + [PACKET / 'README.md']
    for path in documents:
        for target in re.findall(r'\[[^\]]*\]\(([^)]+)\)', path.read_text()):
            url = urlsplit(target)
            if url.scheme or not url.path:
                continue
            resolved = (path.parent / unquote(url.path)).resolve()
            row = dict(source=str(path.relative_to(ROOT)), target=target,
                resolved=str(resolved.relative_to(ROOT)), exists=resolved.exists())
            links.append(row)
            if not row['exists']:
                missing.append(row)
    assert links and not missing, missing
    write_new(OUT / 'fsst-local-links-r1.json', dict(status='passed_local_document_target_paths',
        documents={str(path.relative_to(ROOT)): content(path) for path in documents},
        links=links, links_checked=len(links), missing=missing,
        scope='Local target path existence; existing fragment labels are not independently revalidated.', query_executions=0))
    replay = read(OUT / 'fsst-replay-r1.json')
    report = (ROOT / 'docs/benchmarks/native-fsst-consumer-2026-11-15.md').read_text()
    for group in [replay['fresh'], replay['lifecycle'], *[replay['subgroups']['kind:' + name]
        for name in ('original', 'dictionary_v1', 'text_signature_v1')]]:
        for key in ('baseline_engine_seconds', 'candidate_engine_seconds'):
            assert f'{group[key]:.9f}' in report
    assert f"{replay['fresh']['regression_engine_seconds']:.9f}" in report
    assert str(replay['fresh']['regressions']) in report.replace(',', '')
    subprocess.run(['git', 'diff', '--check'], cwd=ROOT, check=True)
    reports = {name: content(OUT / name) for name in ('fsst-public-status-docs-r1.json',
        'fsst-gap-burndown-r1.json', 'fsst-queue-tracker-r1.json', 'fsst-local-links-r1.json')}
    write_new(OUT / 'fsst-docs-validation-r1.json', dict(status='passed_complete_closeout_documentation',
        query_executions=0, reports=reports, local_links_checked=len(links), missing_local_links=[],
        exact_report_engine_totals_verified=True, release_tracker_open_items=int(match[1]),
        release_claim_allowed=False, runtime_changes_after_measurement=False))
    old = read(PACKET / 'manifest.json')
    assert old['closeout_state'] == 'pending_document_verification'
    for name, expected in old['files'].items():
        assert content(PACKET / name) == expected
    supplement = PACKET / 'supplement'
    shutil.copyfile(PACKET / 'manifest.json', supplement / 'initial-closeout-manifest.json')
    for name in [*reports, 'fsst-final-source-r1.json', 'fsst-docs-validation-r1.json',
                 'fsst-packet-installation-r1.json', 'prepare_fsst_closeout.py']:
        shutil.copyfile(OUT / name, supplement / name)
        assert content(OUT / name) == content(supplement / name)
    shutil.copyfile(OUT / 'verify_fsst_closeout.py', PACKET / 'verify.py')
    manifest = dict(old, closeout_state='complete_local_evidence_and_docs')
    manifest['files'] = {path.relative_to(PACKET).as_posix(): content(path)
        for path in sorted(PACKET.rglob('*')) if path.is_file() and path != PACKET / 'manifest.json'}
    (PACKET / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    assert guard.sources() == before
    write_new(OUT / 'fsst-closeout-preparation-r1.json', dict(status='prepared_complete_closeout_pending_final_replay',
        packet_manifest=content(PACKET / 'manifest.json'), source=content(OUT / 'fsst-final-source-r1.json'),
        docs=content(OUT / 'fsst-docs-validation-r1.json'), source_unchanged=True, query_executions=0,
        packet_files=len(manifest['files'])))
    print(json.dumps(dict(status='prepared_complete_closeout_pending_final_replay',
        local_links=len(links), document_changes=len(differences), open_queue_items=int(match[1]), query_executions=0)))


if __name__ == '__main__':
    main()
