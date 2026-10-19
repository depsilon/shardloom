"""Preserve historical patch bytes and verify the staged predicate closeout."""
from pathlib import Path
import argparse
import hashlib
import json
import lzma
import re
import subprocess
from urllib.parse import unquote

E = Path('/Users/dylan/LocalData/shardloom/conditional-investigations-20261019')
W = Path('/Users/dylan/.codex/worktrees/public-docs-api/shardloom-local-repo')
D = W / 'docs/benchmarks/evidence'
BASE = '896c71793a8859a6b4770bbc8a9fe0b85d755cf8'
PATCHES = ('predicate-restoration-rejected-770.patch', 'predicate-restoration-applied-770.patch')


def sha(data):
    return hashlib.sha256(data).hexdigest()


def identity(path):
    data = path.read_bytes()
    return dict(path=str(path), bytes=len(data), sha256=sha(data))


def git(*args):
    return subprocess.check_output(['git', *args], cwd=W, text=True)


def write_once(path, data):
    if path.exists():
        assert path.read_bytes() == data, str(path)
    else:
        with path.open('xb') as stream:
            stream.write(data)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--prepare', action='store_true')
    args = parser.parse_args()
    assert git('rev-parse', 'HEAD').strip() == BASE
    mappings = []
    for name in PATCHES:
        original = E / name
        raw = original.read_bytes()
        compressed = D / (name + '.xz')
        if args.prepare:
            assert (D / name).read_bytes() == raw
            write_once(E / compressed.name, lzma.compress(raw, format=lzma.FORMAT_XZ, preset=6))
            write_once(compressed, (E / compressed.name).read_bytes())
        assert compressed.read_bytes() == (E / compressed.name).read_bytes()
        assert lzma.decompress(compressed.read_bytes()) == raw
        mappings.append(dict(historical_repository_path=str((D / name).relative_to(W)),
                             original=identity(original), compressed=identity(compressed),
                             decompressed_bytes=len(raw), decompressed_sha256=sha(raw)))
    if args.prepare:
        print(json.dumps(dict(status='prepared_and_readback_verified', mappings=mappings)))
        return
    for name in PATCHES:
        assert not (D / name).exists()
    accepted_path = E / 'compact-index-acceptance-source-764.json'
    accepted = json.loads(accepted_path.read_text())
    assert len(accepted['source_hashes']) == 2217
    for path, digest in accepted['source_hashes'].items():
        assert sha((W / path).read_bytes()) == digest, path
    historical = []
    for name in ('predicate-exclusion-restoration-770.json', 'predicate-exclusion-closeout-770.json',
                 'finalize_predicate_restoration_770.py', 'verify_predicate_closeout_770.py'):
        assert (D / name).read_bytes() == (E / name).read_bytes()
        historical.append(identity(D / name))
    restore = json.loads((D / 'predicate-exclusion-restoration-770.json').read_text())
    assert restore['baseline'] == BASE and restore['runtime_input_count'] == 1899
    for row in restore['runtime_sources']:
        assert sha((W / row['path']).read_bytes()) == row['sha256'], row['path']
    mapped = {row['historical_repository_path'] for row in mappings}
    for row in restore['copied_evidence']:
        path = W / row['path']
        data = lzma.decompress(Path(str(path) + '.xz').read_bytes()) if row['path'] in mapped else path.read_bytes()
        assert (len(data), sha(data)) == (row['bytes'], row['sha256']), row['path']
    sequence_path = 'docs/architecture/native-conditional-investigations-2026-10-19.md'
    sequence = (W / sequence_path).read_text()
    ids = lambda text: re.findall(r'^\| (\d+) \| `([^`]+)` \|', text, re.M)
    assert ids(sequence) == ids(git('show', f'{BASE}:{sequence_path}')) and len(ids(sequence)) == 21
    assert 'six finite investigations completed' in sequence and '15 rows awaiting' in sequence
    assert len(re.findall(r'^\| `PERF-[^`]+` \|', sequence, re.M)) == 6
    phase_path = 'docs/architecture/phased-execution-plan.md'
    phase = (W / phase_path).read_text()
    substeps = lambda text: re.findall(r'^\s*- \[ \] `((?:PERF-\d+|NATIVE-ENCODED-EXCHANGE)\.[^`]+)`', text, re.M)
    assert substeps(phase) == substeps(git('show', f'{BASE}:{phase_path}')) and len(substeps(phase)) == 58
    for n in range(1, 24):
        assert re.search(rf'\bCG-{n}(?!\d)', phase)
    docs = ['native-predicate-exclusion-2026-10-19.md', 'native-predicate-exclusion-design-2026-10-19.md',
            'native-conditional-investigations-2026-10-19.md', 'phased-execution-plan.md', 'phased-execution-completed-ledger.md']
    links = 0
    for name in docs:
        path = W / 'docs/architecture' / name
        if name.startswith('native-predicate-'):
            content = path.read_text()
        else:
            content = '\n'.join(line[1:] for line in git('diff', BASE, '--unified=0', '--', str(path.relative_to(W))).splitlines()
                                if line.startswith('+') and not line.startswith('+++'))
        for target in re.findall(r'\]\(([^)]+)\)', content):
            if target.startswith(('https:', 'http:', '#')):
                continue
            linked = (path.parent / unquote(target.split('#', 1)[0])).resolve()
            assert linked.is_file() or linked == D / 'predicate-exclusion-packaging-771.json', (name, target)
            links += 1
    assert not git('diff', BASE, '--name-only', '--', '*.rs', '*Cargo.toml', '*Cargo.lock').strip()
    changed = git('diff', BASE, '--name-only').splitlines()
    assert changed and all(path.startswith('docs/') for path in changed)
    subprocess.run(['git', 'diff', '--check'], cwd=W, check=True)
    subprocess.run(['git', 'diff', '--cached', '--check'], cwd=W, check=True)
    helper = D / Path(__file__).name
    assert helper.read_bytes() == Path(__file__).read_bytes()
    receipt = dict(schema_version='shardloom.predicate_exclusion_packaging.v1', status='passed', baseline=BASE,
                   repair='Lossless compression of two saved patch texts; no runtime or measurement changes.',
                   historical_receipts=historical, historical_path_mappings=mappings,
                   historical_whitespace_scope='The 770 closeout ran git diff --check before staging. It did not check newly staged patch context lines. The staged check subsequently rejected their significant single-space context lines. This receipt supersedes that limited whitespace claim; original receipts and helpers remain unchanged.',
                   accepted_source=identity(accepted_path), accepted_source_files=2217,
                   all_accepted_source_contents_unchanged=True, runtime_inputs=1899,
                   conditional_sequence_preserved=21, required_and_conditional_substeps_preserved=58,
                   competitive_gates_preserved=list(range(1, 24)), finite_decisions=6,
                   finite_retained=1, finite_rejected=5, first_decisions_remaining=15,
                   relative_links_checked=links, unstaged_whitespace_check='passed',
                   staged_whitespace_check='passed', documentation=[identity(W / 'docs/architecture' / name) for name in docs],
                   verifier=identity(helper), native_runs_repeated=False)
    data = (json.dumps(receipt, indent=2) + '\n').encode()
    write_once(E / 'predicate-exclusion-packaging-771.json', data)
    write_once(D / 'predicate-exclusion-packaging-771.json', data)
    print(json.dumps(dict(status='passed', sources=2217, runtime_inputs=1899,
                         compressed_patches=2, finite_decisions=6, remaining=15, links_checked=links)))


if __name__ == '__main__':
    main()
