"""Verify the rejection record, copied evidence and unchanged accepted sources."""
from pathlib import Path
import hashlib
import json
import re
import subprocess
from urllib.parse import unquote

E = Path(__file__).resolve().parent
W = Path('/Users/dylan/.codex/worktrees/public-docs-api/shardloom-local-repo')
D = W / 'docs/benchmarks/evidence'
BASE = '896c71793a8859a6b4770bbc8a9fe0b85d755cf8'


def identity(path):
    path = Path(path)
    data = path.read_bytes()
    return dict(path=str(path), bytes=len(data), sha256=hashlib.sha256(data).hexdigest())


def git(*args):
    return subprocess.check_output(['git', *args], cwd=W, text=True)


def main():
    accepted_path = E / 'compact-index-acceptance-source-764.json'
    accepted = json.loads(accepted_path.read_text())
    assert len(accepted['source_hashes']) == 2217
    for path, digest in accepted['source_hashes'].items():
        assert identity(W / path)['sha256'] == digest, path
    sequence_path = W / 'docs/architecture/native-conditional-investigations-2026-10-19.md'
    sequence = sequence_path.read_text()
    old_sequence = git('show', f'{BASE}:docs/architecture/native-conditional-investigations-2026-10-19.md')
    ids = lambda text: re.findall(r'^\| (\d+) \| `([^`]+)` \|', text, re.M)
    assert ids(sequence) == ids(old_sequence) and len(ids(sequence)) == 21
    assert 'six finite investigations completed' in sequence and '15 rows awaiting' in sequence
    assert len(re.findall(r'^\| `PERF-[^`]+` \|', sequence, re.M)) == 6
    phase_path = W / 'docs/architecture/phased-execution-plan.md'
    phase = phase_path.read_text()
    old_phase = git('show', f'{BASE}:docs/architecture/phased-execution-plan.md')
    substeps = lambda text: re.findall(r'^\s*- \[ \] `((?:PERF-\d+|NATIVE-ENCODED-EXCHANGE)\.[^`]+)`', text, re.M)
    assert substeps(phase) == substeps(old_phase) and len(substeps(phase)) == 58
    for n in range(1, 24):
        assert re.search(rf'\bCG-{n}(?!\d)', phase)
    documents = ['native-predicate-exclusion-2026-10-19.md', 'native-predicate-exclusion-design-2026-10-19.md',
                 'native-conditional-investigations-2026-10-19.md', 'phased-execution-plan.md', 'phased-execution-completed-ledger.md']
    links = 0
    for name in documents:
        path = W / 'docs/architecture' / name
        if name.startswith('native-predicate-'):
            content = path.read_text()
        else:
            content = '\n'.join(line[1:] for line in git('diff', '--unified=0', '--', str(path.relative_to(W))).splitlines()
                                if line.startswith('+') and not line.startswith('+++'))
        for target in re.findall(r'\]\(([^)]+)\)', content):
            if target.startswith(('https:', 'http:', '#')):
                continue
            linked = (path.parent / unquote(target.split('#', 1)[0])).resolve()
            assert linked.is_file(), (name, target)
            links += 1
    for name in ('predicate-attribution-portable-767', 'predicate-candidate-portable-770'):
        index_path = D / f'{name}.json'
        assert index_path.read_bytes() == (E / name / index_path.name).read_bytes()
        index = json.loads(index_path.read_text())
        for key in ('archive', 'manifest'):
            row = index[key]
            got = identity(D / row['file'])
            assert (got['bytes'], got['sha256']) == (row['bytes'], row['sha256'])
    restore_path = D / 'predicate-exclusion-restoration-770.json'
    restore = json.loads(restore_path.read_text())
    assert restore['status'] == 'passed' and restore['baseline'] == BASE
    assert restore['runtime_input_count'] == 1899 and restore['prior_accepted_source_files'] == 2217
    for row in restore['runtime_sources']:
        assert identity(W / row['path'])['sha256'] == row['sha256']
    for row in restore['copied_evidence']:
        got = identity(W / row['path'])
        assert (got['bytes'], got['sha256']) == (row['bytes'], row['sha256'])
    assert not git('diff', '--name-only', '--', '*.rs', '*Cargo.toml', '*Cargo.lock').strip()
    subprocess.run(['git', 'diff', '--check'], cwd=W, check=True)
    helper_target = D / Path(__file__).name
    with helper_target.open('xb') as stream:
        stream.write(Path(__file__).read_bytes())
    result = dict(schema_version='shardloom.predicate_exclusion_closeout.v1', status='passed', baseline=BASE,
                  accepted_source=identity(accepted_path), accepted_source_files=2217,
                  all_accepted_source_contents_unchanged=True, runtime_inputs=1899,
                  restoration=identity(restore_path), conditional_sequence_preserved=21,
                  required_and_conditional_substeps_preserved=58, competitive_gates_preserved=list(range(1, 24)),
                  finite_decisions=6, finite_retained=1, finite_rejected=5, first_decisions_remaining=15,
                  new_relative_links_checked=links, whitespace_check='passed',
                  documentation=[identity(W / 'docs/architecture' / name) for name in documents],
                  verifier=identity(__file__), native_runs_repeated_for_closeout=False)
    for path in (E / 'predicate-exclusion-closeout-770.json', D / 'predicate-exclusion-closeout-770.json'):
        with path.open('x') as stream:
            json.dump(result, stream, indent=2)
            stream.write('\n')
    print(json.dumps(dict(status='passed', source_files=2217, conditional_rows=21, finite_decisions=6,
                          first_decisions_remaining=15, new_relative_links_checked=links)))


if __name__ == '__main__':
    main()
