"""Preserve the verified FSST closeout and its final generated-reference refresh."""
import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

import guard_checks as guard

ROOT, OUT = guard.ROOT, guard.OUT
PACKET = ROOT / 'docs/benchmarks/evidence/native-fsst-consumer-2026-11-15'
SUPPLEMENT = ROOT / 'docs/benchmarks/evidence/native-fsst-publication-2026-11-15'
PARENT = '45be8e0bd1d9cef920c88fa9bf91811deade41c4'
REFERENCES = {'docs/reference/shardloom-user-surface-index.json',
              'docs/reference/shardloom-user-surface-index.md'}


def content(path):
    with path.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    return dict(bytes=path.stat().st_size, sha256=digest)


def read(path):
    return json.loads(path.read_bytes())


def git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT)


def write_new(path, value):
    with path.open('x') as stream:
        json.dump(value, stream, indent=2); stream.write('\n')


def fingerprint(files):
    return hashlib.sha256(json.dumps(files, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def main():
    assert __debug__ and not SUPPLEMENT.exists()
    assert shutil.disk_usage(ROOT).free >= 14 << 30
    assert git('rev-parse', 'HEAD').decode().strip() == PARENT
    assert git('branch', '--show-current').decode().strip() == 'codex/native-fsst-consumer'
    assert not git('diff', '--cached', '--name-only')
    old_source = read(OUT / 'fsst-final-source-r1.json')
    runtime = read(OUT / 'runtime-r1-identity.json')
    build = read(OUT / runtime['build_receipt'])
    before = guard.sources()
    delta = {name: dict(measured=build['source_files'].get(name), current=before.get(name))
             for name in sorted(set(before) | set(build['source_files']))
             if before.get(name) != build['source_files'].get(name)}
    assert set(delta) == set(old_source['changed_source_paths']) | REFERENCES
    for name in old_source['changed_source_paths']:
        assert delta[name]['current'] == old_source['differences'][name]['current']
    assert all(name.startswith('docs/') for name in delta)
    assert fingerprint(build['source_files']) == runtime['source_fingerprint_sha256'] == old_source['runtime_source_fingerprint_sha256']
    assert {name: content(Path(name)) for name in runtime['files']} == runtime['files']

    index_name = 'docs/reference/shardloom-user-surface-index.json'
    prior = json.loads(git('show', PARENT + ':' + index_name))
    current = read(ROOT / index_name)
    expected = copy.deepcopy(prior)
    expected['application']['source_inputs']['Cargo.lock'] = before['Cargo.lock']['sha256']
    expected['application']['source_identity'] = hashlib.sha256(json.dumps(
        expected['application']['source_inputs'], sort_keys=True).encode()).hexdigest()
    assert current == expected
    markdown_name = 'docs/reference/shardloom-user-surface-index.md'
    assert (ROOT / markdown_name).read_text() == git('show', PARENT + ':' + markdown_name).decode().replace(
        prior['application']['source_identity'], current['application']['source_identity'])
    checks = ['fsst-publication-api-check-r1', 'fsst-publication-governance-check-r1']
    for name in checks:
        receipt = read(OUT / (name + '.json'))
        assert receipt['status'] == 'passed' and receipt['returncode'] == 0
        assert receipt['sources_unchanged'] and receipt['process_group_drained'] and receipt['direct_child_reaped']
        assert receipt['source_files'] == before and receipt['head'] == PARENT
    api_log = (OUT / 'logs/fsst-publication-api-check-r1.log').read_text()
    assert 'Ran 15 tests' in api_log and api_log.rstrip().endswith('OK')
    closed = read(OUT / 'fsst-closeout-replay-r1.json')
    closed_guard = read(OUT / 'fsst-closeout-replay-check-r1.json')
    assert closed['status'] == closed_guard['status'] == 'passed'
    assert closed_guard['sources_unchanged'] and closed_guard['process_group_drained'] and closed_guard['direct_child_reaped']
    assert fingerprint(closed_guard['source_files']) == old_source['current_source_fingerprint_sha256']
    assert closed['original_paths_forbidden'] and closed['network_and_subprocess_forbidden'] and closed['installed_packet_unchanged']
    assert closed['query_executions'] == 0 and closed['result']['disposition'] == 'retain'
    assert closed['result']['complete_results'] == 18080 and closed['result']['lifecycle_operations'] == 1080
    assert content(PACKET / 'manifest.json') == closed['manifest']
    manifest = read(PACKET / 'manifest.json')
    packet_files = {path.relative_to(PACKET).as_posix(): content(path)
        for path in PACKET.rglob('*') if path.is_file() and path != PACKET / 'manifest.json'}
    assert packet_files == manifest['files']
    assert not any(path.is_symlink() for path in PACKET.rglob('*'))

    record = dict(status='passed_final_source_and_generated_reference_binding',
        parent=PARENT, runtime_source_fingerprint_sha256=runtime['source_fingerprint_sha256'],
        current_source_fingerprint_sha256=fingerprint(before), current_source_files=len(before),
        changes_from_measured_source=delta, all_runtime_sources_and_native_artifacts_unchanged=True,
        generated_reference_change='Only Cargo.lock identity and the derived application source identity.',
        generated_reference_semantics_unchanged=True, original_closed_packet=closed['manifest'],
        original_closed_packet_files_verified=len(packet_files), queries_executed=0,
        builds_executed=0, original_closeout_replay_reused=True,
        new_proof=dict(api_tests=15, api_failures=0, governance='passed'))
    write_new(OUT / 'fsst-publication-source-r1.json', record)
    SUPPLEMENT.mkdir()
    names = ['fsst-publication-source-r1.json', 'preserve_fsst_closeout.py',
             'fsst-closeout-replay-r1.json', 'fsst-closeout-replay-check-r1.json',
             'run_fsst_closeout_replay.py', *[name + '.json' for name in checks]]
    names += ['logs/' + name + '.log' for name in checks + ['fsst-closeout-replay-check-r1']]
    for name in names:
        dest = SUPPLEMENT / name
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(OUT / name, dest)
        assert content(dest) == content(OUT / name)
    (SUPPLEMENT / 'README.md').write_text('''<!-- SPDX-License-Identifier: Apache-2.0 -->

# FSST publication source binding

This additive receipt preserves the final relocated replay of the
[closed FSST packet](../native-fsst-consumer-2026-11-15/README.md), all 15 passing
API-reference tests and passing contribution-governance check. It binds the final
generated reference refresh to the measured source and already-verified docs.
Only the recorded Cargo.lock identity and derived application identity change
in the generated references; the measured runtime and original packet are
byte-identical. No query, build or performance observation is repeated.

`manifest.json` gives exact identities for every file here. The replay receipt
binds the original packet manifest, which binds every packet file. The final
source record compares all captured source files and all native artifacts with
the measured candidate. Existing guards record unchanged sources and drained
process groups. This is local evidence and implies no merge or release.
''')
    write_new(SUPPLEMENT / 'manifest.json', dict(schema='shardloom.native_fsst_publication_binding.v1',
        status='passed', query_executions=0, original_packet_manifest=closed['manifest'],
        files={path.relative_to(SUPPLEMENT).as_posix(): content(path)
               for path in sorted(SUPPLEMENT.rglob('*')) if path.is_file()}))
    assert guard.sources() == before

    paths = set(git('diff', '--name-only', '-z').decode().split('\0')) - {''}
    paths |= set(git('ls-files', '--others', '--exclude-standard', '-z').decode().split('\0')) - {''}
    allowed_docs = set(delta)
    allowed_packets = {str(path.relative_to(ROOT)) for folder in (PACKET, SUPPLEMENT)
                       for path in folder.rglob('*') if path.is_file()}
    assert paths == allowed_docs | allowed_packets
    assert all((ROOT / name).stat().st_size < 100 << 20 for name in paths)
    original_ids = {name: content(ROOT / name) for name in sorted(paths)}
    object_dir = Path('/Users/dylan/LocalData/shardloom/git-object-stores/native-optimization-2026-11-14/objects')
    assert object_dir.is_dir()
    common = Path(git('rev-parse', '--git-common-dir').decode().strip()).resolve()
    os.environ['GIT_OBJECT_DIRECTORY'] = str(object_dir)
    os.environ['GIT_ALTERNATE_OBJECT_DIRECTORIES'] = str(common / 'objects')
    subprocess.run(['git', 'add', '--', *sorted(paths)], cwd=ROOT, check=True)
    assert set(git('diff', '--cached', '--name-only', '-z').decode().split('\0')) - {''} == paths
    subprocess.run(['git', 'diff', '--cached', '--check'], cwd=ROOT, check=True)
    subprocess.run(['git', '-c', 'gc.auto=0', '-c', 'maintenance.auto=false', 'commit', '-m',
                    'Close FSST prefilter evidence and bind final generated references'], cwd=ROOT, check=True)
    head = git('rev-parse', 'HEAD').decode().strip()
    assert git('rev-parse', 'HEAD^').decode().strip() == PARENT
    assert not git('status', '--porcelain') and guard.sources() == before
    for name, expected_id in original_ids.items():
        data = git('show', head + ':' + name)
        assert dict(bytes=len(data), sha256=hashlib.sha256(data).hexdigest()) == expected_id
    write_new(OUT / 'fsst-closeout-commit-r1.json', dict(status='preserved_complete_closeout',
        head=head, parent=PARENT, tree=git('rev-parse', 'HEAD^{tree}').decode().strip(),
        files=original_ids, committed_blobs_verified=len(original_ids),
        current_source_fingerprint_sha256=fingerprint(before), queries_executed=0,
        original_packet_unchanged=True, runtime_unchanged=True, publication_attempted=False))
    print(json.dumps(dict(status='preserved_complete_closeout', head=head, committed_files=len(paths),
                         query_executions=0, original_packet_unchanged=True, runtime_unchanged=True)))


if __name__ == '__main__':
    main()
