"""Commit checked source/docs while retaining captured whitespace as evidence."""
import gzip
import hashlib
import json
import os
import re
import shutil
import subprocess

from preserve_fsst_closeout import (ROOT, OUT, SUPPLEMENT, PARENT,
    content, read, git, write_new, fingerprint, guard)


def main():
    assert __debug__ and git('rev-parse', 'HEAD').decode().strip() == PARENT
    assert not git('diff', '--name-only') and not git('ls-files', '--others', '--exclude-standard')
    before = guard.sources()
    assert fingerprint(before) == read(SUPPLEMENT / 'fsst-publication-source-r1.json')['current_source_fingerprint_sha256']
    old = read(SUPPLEMENT / 'manifest.json')
    assert {p.relative_to(SUPPLEMENT).as_posix(): content(p) for p in SUPPLEMENT.rglob('*')
        if p.is_file() and p != SUPPLEMENT / 'manifest.json'} == old['files']
    receipt = read(OUT / 'fsst-closeout-preservation-r2.json')
    assert receipt['status'] == 'failed' and receipt['returncode'] == 1 and receipt['sources_unchanged']
    assert receipt['source_files'] == before and receipt['process_group_drained'] and receipt['direct_child_reaped']
    shutil.copyfile(SUPPLEMENT / 'manifest.json', SUPPLEMENT / 'preservation-r2-manifest.json')
    for name in ('fsst-closeout-preservation-r2.json', 'finalize_fsst_closeout.py'):
        shutil.copyfile(OUT / name, SUPPLEMENT / name)
    original_log = (OUT / 'logs/fsst-closeout-preservation-r2.log').read_bytes()
    compressed_log = SUPPLEMENT / 'logs/fsst-closeout-preservation-r2.log.gz'
    compressed_log.write_bytes(gzip.compress(original_log, mtime=0))
    assert gzip.decompress(compressed_log.read_bytes()) == original_log
    text = (SUPPLEMENT / 'README.md').read_text()
    text = text.replace('The resumed check accepts only those exact observations; every\nother staged source, document and evidence file passes the whitespace check.',
        'A second preservation stop caught the same whitespace copied into the\nsaved first-failure log. Both stops are preserved. The final check enumerates\nall observations in those three captured files and checks every other staged\nfile. Captured evidence bytes remain unchanged.')
    (SUPPLEMENT / 'README.md').write_text(text)
    manifest = dict(old, preservation_r2_manifest=content(SUPPLEMENT / 'preservation-r2-manifest.json'),
        compressed_log_original=content(OUT / 'logs/fsst-closeout-preservation-r2.log'))
    manifest['files'] = {p.relative_to(SUPPLEMENT).as_posix(): content(p)
        for p in sorted(SUPPLEMENT.rglob('*')) if p.is_file() and p != SUPPLEMENT / 'manifest.json'}
    (SUPPLEMENT / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    os.environ['GIT_OBJECT_DIRECTORY'] = '/Users/dylan/LocalData/shardloom/git-object-stores/native-optimization-2026-11-14/objects'
    os.environ['GIT_ALTERNATE_OBJECT_DIRECTORIES'] = '/Users/dylan/Documents/shardloom-local-repo/.git/objects'
    subprocess.run(['git', 'add', '--', str(SUPPLEMENT.relative_to(ROOT))], cwd=ROOT, check=True)
    result = subprocess.run(['git', 'diff', '--cached', '--check'], cwd=ROOT, capture_output=True, text=True)
    expected = {'docs/benchmarks/evidence/native-fsst-consumer-2026-11-15/packet/supplement-017-fsst-packet-controls-check-r1.log',
                'docs/benchmarks/evidence/native-fsst-consumer-2026-11-15/supplement/fsst-doc-inventory-r1.md',
                'docs/benchmarks/evidence/native-fsst-publication-2026-11-15/logs/fsst-closeout-preservation-r1.log'}
    observations = re.findall(r'^(.*?):(\d+): (new blank line at EOF\.|trailing whitespace\.)$', result.stdout, re.M)
    assert result.returncode == 2 and not result.stderr and {row[0] for row in observations} == expected
    assert len(result.stdout.splitlines()) == sum(1 if row[2].startswith('new') else 2 for row in observations)
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
    for name, expected_id in identities.items():
        data = git('show', head + ':' + name)
        assert dict(bytes=len(data), sha256=hashlib.sha256(data).hexdigest()) == expected_id
    write_new(OUT / 'fsst-closeout-commit-r1.json', dict(status='preserved_complete_closeout',
        head=head, parent=PARENT, tree=git('rev-parse', 'HEAD^{tree}').decode().strip(),
        files=identities, committed_blobs_verified=len(identities),
        current_source_fingerprint_sha256=fingerprint(before), queries_executed=0,
        original_packet_unchanged=True, runtime_unchanged=True, publication_attempted=False,
        immutable_whitespace_observations=observations))
    print(json.dumps(dict(status='preserved_complete_closeout', head=head,
        committed_files=len(paths), query_executions=0, original_packet_unchanged=True, runtime_unchanged=True)))


if __name__ == '__main__':
    main()
