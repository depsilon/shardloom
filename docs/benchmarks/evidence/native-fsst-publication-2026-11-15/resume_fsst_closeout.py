"""Finish the staged closeout while preserving immutable whitespace observations."""
import os
import re
import shutil
import subprocess

from preserve_fsst_closeout import (ROOT, OUT, PACKET, SUPPLEMENT, PARENT,
    content, read, git, write_new, fingerprint, guard)


def main():
    assert __debug__ and git('rev-parse', 'HEAD').decode().strip() == PARENT
    assert not git('diff', '--name-only') and not git('ls-files', '--others', '--exclude-standard')
    before = guard.sources()
    source = read(SUPPLEMENT / 'fsst-publication-source-r1.json')
    assert fingerprint(before) == source['current_source_fingerprint_sha256']
    old_manifest = read(SUPPLEMENT / 'manifest.json')
    assert {p.relative_to(SUPPLEMENT).as_posix(): content(p) for p in SUPPLEMENT.rglob('*')
            if p.is_file() and p != SUPPLEMENT / 'manifest.json'} == old_manifest['files']
    failure = read(OUT / 'fsst-closeout-preservation-r1.json')
    assert failure['status'] == 'failed' and failure['returncode'] == 1
    assert failure['sources_unchanged'] and failure['source_files'] == before
    assert failure['process_group_drained'] and failure['direct_child_reaped']
    result = subprocess.run(['git', 'diff', '--cached', '--check'], cwd=ROOT,
                            capture_output=True, text=True, check=False)
    expected = {'docs/benchmarks/evidence/native-fsst-consumer-2026-11-15/packet/supplement-017-fsst-packet-controls-check-r1.log',
                'docs/benchmarks/evidence/native-fsst-consumer-2026-11-15/supplement/fsst-doc-inventory-r1.md'}
    observations = re.findall(r'^(.*?):(\d+): (new blank line at EOF\.|trailing whitespace\.)$', result.stdout, re.M)
    assert result.returncode == 2 and not result.stderr and {row[0] for row in observations} == expected
    allowed_lines = sum(1 if row[2].startswith('new') else 2 for row in observations)
    assert len(result.stdout.splitlines()) == allowed_lines
    subprocess.run(['git', 'diff', '--cached', '--check', '--', '.',
                    *[':(exclude)' + name for name in sorted(expected)]], cwd=ROOT, check=True)
    shutil.copyfile(SUPPLEMENT / 'manifest.json', SUPPLEMENT / 'initial-manifest.json')
    for name in ('fsst-closeout-preservation-r1.json', 'logs/fsst-closeout-preservation-r1.log',
                 'resume_fsst_closeout.py'):
        shutil.copyfile(OUT / name, SUPPLEMENT / name)
    write_new(SUPPLEMENT / 'whitespace-observations.json', dict(status='preserved_immutable_evidence',
        observations=observations, source_and_other_files_check='passed', query_executions=0,
        original_packet_manifest=content(PACKET / 'manifest.json')))
    with (SUPPLEMENT / 'README.md').open('a') as stream:
        stream.write('\nThe first preservation attempt stopped before committing on whitespace in two\n'
                     'immutable captured files: blank-line excerpt labels and a saved failed-replay\n'
                     'log. The original bytes, failure receipt, output and initial manifest remain\n'
                     'preserved. The resumed check accepts only those exact observations; every\n'
                     'other staged source, document and evidence file passes the whitespace check.\n')
    manifest = dict(old_manifest, initial_manifest=content(SUPPLEMENT / 'initial-manifest.json'))
    manifest['files'] = {p.relative_to(SUPPLEMENT).as_posix(): content(p) for p in sorted(SUPPLEMENT.rglob('*'))
                         if p.is_file() and p != SUPPLEMENT / 'manifest.json'}
    import json
    (SUPPLEMENT / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    os.environ['GIT_OBJECT_DIRECTORY'] = '/Users/dylan/LocalData/shardloom/git-object-stores/native-optimization-2026-11-14/objects'
    os.environ['GIT_ALTERNATE_OBJECT_DIRECTORIES'] = '/Users/dylan/Documents/shardloom-local-repo/.git/objects'
    subprocess.run(['git', 'add', '--', str(SUPPLEMENT.relative_to(ROOT))], cwd=ROOT, check=True)
    subprocess.run(['git', 'diff', '--cached', '--check', '--', '.',
                    *[':(exclude)' + name for name in sorted(expected)]], cwd=ROOT, check=True)
    paths = set(git('diff', '--cached', '--name-only', '-z').decode().split('\0')) - {''}
    identities = {name: content(ROOT / name) for name in sorted(paths)}
    assert all(name.startswith('docs/') for name in paths)
    subprocess.run(['git', '-c', 'gc.auto=0', '-c', 'maintenance.auto=false', 'commit', '-m',
                    'Close FSST prefilter evidence and bind final generated references'], cwd=ROOT, check=True)
    head = git('rev-parse', 'HEAD').decode().strip()
    assert git('rev-parse', 'HEAD^').decode().strip() == PARENT
    assert not git('status', '--porcelain') and guard.sources() == before
    import hashlib
    for name, expected_id in identities.items():
        data = git('show', head + ':' + name)
        assert dict(bytes=len(data), sha256=hashlib.sha256(data).hexdigest()) == expected_id
    write_new(OUT / 'fsst-closeout-commit-r1.json', dict(status='preserved_complete_closeout',
        head=head, parent=PARENT, tree=git('rev-parse', 'HEAD^{tree}').decode().strip(),
        files=identities, committed_blobs_verified=len(identities),
        current_source_fingerprint_sha256=fingerprint(before), queries_executed=0,
        original_packet_unchanged=True, runtime_unchanged=True, publication_attempted=False,
        immutable_whitespace_observations=len(observations)))
    print(json.dumps(dict(status='preserved_complete_closeout', head=head, committed_files=len(paths),
                         query_executions=0, original_packet_unchanged=True, runtime_unchanged=True)))


if __name__ == '__main__':
    main()
