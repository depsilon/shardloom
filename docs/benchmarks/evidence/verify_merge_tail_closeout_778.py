"""Verify the staged finite merge-tail record and unchanged production sources."""
from pathlib import Path
import hashlib
import json
import lzma
import re
import subprocess
from urllib.parse import unquote

E = Path(__file__).resolve().parent
W = Path('/Users/dylan/.codex/worktrees/public-docs-api/shardloom-local-repo')
D = W / 'docs/benchmarks/evidence'
BASE = 'd732d7889207e7d8fc0de8d746de1623174c1dcf'


def identity(path):
    path = Path(path)
    assert path.is_file() and not path.is_symlink(), path
    data = path.read_bytes()
    return dict(path=str(path), bytes=len(data), sha256=hashlib.sha256(data).hexdigest())


def git(*args):
    return subprocess.check_output(['git', *args], cwd=W, text=True)


def main():
    assert git('rev-parse', 'HEAD').strip() == BASE
    accepted_path = D / 'compact-index-acceptance-source-764.json'
    assert accepted_path.read_bytes() == (E / accepted_path.name).read_bytes()
    accepted = json.loads(accepted_path.read_text())
    assert len(accepted['source_hashes']) == 2217
    for path, digest in accepted['source_hashes'].items():
        assert identity(W / path)['sha256'] == digest, path
    restoration_path = D / 'merge-tail-restoration-778.json'
    restoration = json.loads(restoration_path.read_text())
    assert restoration_path.read_bytes() == (E / restoration_path.name).read_bytes()
    assert restoration['status'] == 'passed' and restoration['baseline'] == BASE
    assert restoration['runtime_input_count'] == 1899 and restoration['prior_accepted_source_files'] == 2217
    assert restoration['production_behavior_changed'] is restoration['candidate_schedule_implemented'] is False
    for row in restoration['runtime_sources']:
        actual = identity(W / row['path'])
        assert (actual['bytes'], actual['sha256']) == (row['bytes'], row['sha256'])
    for row in restoration['copied_evidence']:
        actual = identity(W / row['path'])
        assert (actual['bytes'], actual['sha256']) == (row['bytes'], row['sha256'])
    patch = lzma.decompress((D / 'merge-tail-observer-removal-778.patch.xz').read_bytes())
    assert hashlib.sha256(patch).hexdigest() == restoration['applied_removal_patch']['sha256']
    assert patch == (E / 'merge-tail-observer-removal-778.patch').read_bytes()
    for row in restoration['removed_observer_files']:
        path = W / row['path']
        assert identity(path)['sha256'] == row['baseline_sha256'] if row['tracked'] else not path.exists()
    index_path = D / 'merge-tail-portable-778.json'
    assert index_path.read_bytes() == (E / 'merge-tail-portable-778' / index_path.name).read_bytes()
    index = json.loads(index_path.read_text())
    assert index['status'] == 'passed' and index['scope_result'] == 'no_signal'
    assert index['member_count'] == 2247 and index['empty_directory_count'] == 2
    for field in ('archive', 'manifest'):
        row = index[field]
        actual = identity(D / row['file'])
        assert (actual['bytes'], actual['sha256']) == (row['bytes'], row['sha256'])
    packet_proof = json.loads((D / 'merge-tail-packet-verification-778.json').read_text())
    assert packet_proof['status'] == 'passed' and packet_proof['all_members_and_original_bytes_verified'] is True
    assert packet_proof['member_count'] == 2247 and packet_proof['empty_directories_verified'] == 2
    assert [r['source_files'] for r in packet_proof['reconstructed_versions']] == [1901] * 4
    assert [r['helpers'] for r in packet_proof['reconstructed_versions']] == [23, 28, 32, 39]
    assert packet_proof['release_workflows'] == 236 and packet_proof['debug_workflows_without_decision_credit'] == 40
    assert packet_proof['invalid_attempt_recorded_workflows'] == 0 and packet_proof['v3_partial_completed_public_operations'] == 1
    sequence_path = W / 'docs/architecture/native-conditional-investigations-2026-10-19.md'
    sequence = sequence_path.read_text()
    ids = lambda text: re.findall(r'^\| (\d+) \| `([^`]+)` \|', text, re.M)
    assert ids(sequence) == ids(git('show', f'{BASE}:docs/architecture/native-conditional-investigations-2026-10-19.md'))
    assert len(ids(sequence)) == 21
    assert 'seven finite investigations completed' in sequence and '14 rows awaiting' in sequence
    assert len(re.findall(r'^\| `PERF-[^`]+` \|', sequence, re.M)) == 7
    phase = (W / 'docs/architecture/phased-execution-plan.md').read_text()
    substeps = lambda text: re.findall(r'^\s*- \[ \] `((?:PERF-\d+|NATIVE-ENCODED-EXCHANGE)\.[^`]+)`', text, re.M)
    assert substeps(phase) == substeps(git('show', f'{BASE}:docs/architecture/phased-execution-plan.md'))
    assert len(substeps(phase)) == 58
    conditional = re.findall(r'^\s*- \[ \] `((?:PERF-\d+|NATIVE-ENCODED-EXCHANGE)\.[^`]+)` \*\*Conditional\.\*\*', phase, re.M)
    assert len(conditional) == 21
    for n in range(1, 24):
        assert re.search(rf'\bCG-{n}(?!\d)', phase)
    assert 'Fourteen rows await their first decision' in phase
    documents = ['native-merge-tail-2026-10-19.md', 'native-merge-tail-design-2026-10-19.md',
                 'native-conditional-investigations-2026-10-19.md', 'phased-execution-plan.md',
                 'phased-execution-completed-ledger.md']
    links = 0
    for name in documents:
        path = W / 'docs/architecture' / name
        if name.startswith('native-merge-tail'):
            content = path.read_text()
        else:
            content = '\n'.join(line[1:] for line in git('diff', BASE, '--unified=0', '--', str(path.relative_to(W))).splitlines()
                                if line.startswith('+') and not line.startswith('+++'))
        for target in re.findall(r'\]\(([^)]+)\)', content):
            if target.startswith(('https:', 'http:', '#')):
                continue
            linked = (path.parent / unquote(target.split('#', 1)[0])).resolve()
            assert linked.is_file(), (name, target)
            links += 1
    assert not git('diff', BASE, '--name-only', '--', '*.rs', '*Cargo.toml', '*Cargo.lock').strip()
    changed = git('diff', '--cached', '--name-only').splitlines()
    assert changed and all(p.startswith('docs/') for p in changed)
    assert not git('diff', '--name-only').strip(), 'all closeout changes must be staged before verification'
    subprocess.run(['git', 'diff', '--check'], cwd=W, check=True)
    subprocess.run(['git', 'diff', '--cached', '--check'], cwd=W, check=True)
    helper = D / Path(__file__).name
    assert helper.read_bytes() == Path(__file__).read_bytes()
    result = dict(schema_version='shardloom.merge_tail_closeout.v1', status='passed', baseline=BASE,
                  accepted_source=identity(accepted_path), accepted_source_files=2217,
                  all_accepted_source_contents_unchanged=True, runtime_inputs=1899,
                  restoration=identity(restoration_path), packet=identity(index_path),
                  conditional_sequence_preserved=21, conditional_substeps=21, required_substeps=37,
                  required_and_conditional_substeps_preserved=58, competitive_gates_preserved=list(range(1, 24)),
                  finite_decisions=7, finite_retained=1, finite_rejected_candidates=5,
                  finite_no_signal_attributions=1, first_decisions_remaining=14,
                  release_workflows=236, debug_workflows_without_decision_credit=40,
                  invalid_attempt_recorded_workflows=0, v3_partial_completed_public_operations=1,
                  new_relative_links_checked=links, unstaged_whitespace_check='passed',
                  staged_whitespace_check='passed', documentation=[identity(W / 'docs/architecture' / n) for n in documents],
                  staged_paths=changed, verifier=identity(helper), native_runs_repeated_for_closeout=False,
                  expert_review=dict(frontier_scope='One/two-run frontiers cannot establish general scheduling optimality; the report requires a new protocol for a real alternative.',
                                     failed_attempts='Partial public execution, invalid complete workflow records, debug checks and release measurements retain distinct counts and credit.',
                                     restoration='All production inputs match accepted bytes; prior acceptance is inherited without claiming fresh broad suite runs.'))
    for output in (E / 'merge-tail-closeout-778.json', D / 'merge-tail-closeout-778.json'):
        with output.open('x') as stream:
            json.dump(result, stream, indent=2)
            stream.write('\n')
    print(json.dumps(dict(status='passed', source_files=2217, conditional_rows=21,
                         finite_decisions=7, first_decisions_remaining=14, links_checked=links)))


if __name__ == '__main__':
    main()
