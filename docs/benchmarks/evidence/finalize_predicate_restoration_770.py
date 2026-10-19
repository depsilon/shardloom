"""Bind exact production restoration and copy the immutable rejection evidence."""
from pathlib import Path
import hashlib
import json
import shutil
import subprocess

E = Path(__file__).resolve().parent
W = Path('/Users/dylan/.codex/worktrees/public-docs-api/shardloom-local-repo')
D = W / 'docs/benchmarks/evidence'
BASE = '896c71793a8859a6b4770bbc8a9fe0b85d755cf8'


def sha(data):
    return hashlib.sha256(data).hexdigest()


def identity(path):
    path = Path(path)
    data = path.read_bytes()
    return dict(path=str(path), bytes=len(data), sha256=sha(data))


def runtime(path):
    return path.endswith('.rs') or Path(path).name in ('Cargo.toml', 'Cargo.toml.orig', 'Cargo.lock')


def main():
    assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=W, text=True).strip() == BASE
    proof_path = E / 'predicate-candidate-packet-verification-770.json'
    proof = json.loads(proof_path.read_text())
    plan_path = E / 'predicate-candidate-restoration-plan-770.json'
    plan = json.loads(plan_path.read_text())
    assert proof['status'] == 'passed' and proof['restoration_plan'] == identity(plan_path)
    for row in plan['files']:
        path = W / row['path']
        if row['tracked']:
            assert sha(path.read_bytes()) == row['baseline_sha256']
        else:
            assert not path.exists()
    tree = subprocess.check_output(['git', 'ls-tree', '-rz', '--full-tree', BASE], cwd=W)
    expected = {}
    for entry in tree.split(b'\0'):
        if not entry:
            continue
        descriptor, path = entry.split(b'\t')
        path = path.decode()
        if runtime(path):
            expected[path] = descriptor.decode().split()[2]
    current = subprocess.check_output(['git', 'ls-files', '-co', '--exclude-standard', '-z'], cwd=W, text=True).split('\0')
    assert {p for p in current if p and runtime(p)} == set(expected)
    assert len(expected) == 1899
    inventory = []
    for path, git_hash in sorted(expected.items()):
        data = (W / path).read_bytes()
        assert hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest() == git_hash, path
        inventory.append(dict(path=path, bytes=len(data), sha256=sha(data), baseline_git_blob=git_hash))
    accepted_path = E / 'compact-index-acceptance-source-764.json'
    accepted = json.loads(accepted_path.read_text())
    assert len(accepted['source_hashes']) == 2217
    for path, digest in accepted['source_hashes'].items():
        assert sha((W / path).read_bytes()) == digest, path
    fmt_path = E / 'predicate-restored-fmt-770.json'
    fmt = json.loads(fmt_path.read_text())
    assert fmt['status'] == 'passed' and fmt['returncode'] == 0
    copied = []

    def copy(source):
        source = Path(source)
        target = D / source.name
        if target.exists():
            assert target.read_bytes() == source.read_bytes(), str(target)
        else:
            with target.open('xb') as stream:
                stream.write(source.read_bytes())
        assert target.read_bytes() == source.read_bytes()
        copied.append(dict(path=str(target.relative_to(W)), bytes=target.stat().st_size, sha256=sha(target.read_bytes())))

    for name in ('predicate-attribution-portable-767', 'predicate-candidate-portable-770'):
        folder = E / name
        index = json.loads((folder / f'{name}.json').read_text())
        for kind in ('archive', 'manifest'):
            row = index[kind]
            data = (folder / row['file']).read_bytes()
            assert len(data) == row['bytes'] and sha(data) == row['sha256']
        for suffix in ('.json', '.tar.xz', '.manifest.json.xz'):
            copy(folder / f'{name}{suffix}')
    for name in ('predicate-exclusion-candidate-protocol-768.json', 'predicate-exclusion-attribution-protocol-766.json',
                 'predicate-attribution-packet-verification-768.json', 'predicate-candidate-verification-770.json',
                 'predicate-candidate-packet-verification-770.json', 'predicate-restored-fmt-770.json',
                 'verify_predicate_candidate_packet_770.py', 'finalize_predicate_restoration_770.py',
                 'predicate-candidate-restoration-plan-770.json', 'predicate-restoration-rejected-770.patch',
                 'predicate-restoration-applied-770.patch'):
        copy(E / name)
    receipt_path = E / 'predicate-exclusion-restoration-770.json'
    result = dict(schema_version='shardloom.predicate_exclusion_restoration.v1', status='passed', baseline=BASE,
                  runtime_input_count=1899, all_runtime_inputs_match_baseline=True, runtime_sources=inventory,
                  removed_candidate_files=plan['files'], accepted_source_manifest=identity(accepted_path),
                  prior_accepted_source_files=2217, all_prior_accepted_source_contents_unchanged=True,
                  candidate_packet_verification=identity(proof_path), copied_evidence=copied,
                  format_check=identity(fmt_path), finalizer=identity(__file__),
                  restoration_helper_correction='The first generated patch used a reversed git-diff header incorrectly. apply_patch refused the first filename before any file changed. Corrected path headers were applied; both patch texts are retained and every resulting source byte is verified against the accepted baseline.',
                  native_test_suites_or_benchmarks_repeated=False, candidate_integration_accepted=False,
                  production_behavior_changed=False, full43_performance_claim=False)
    with receipt_path.open('x') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    copy(receipt_path)
    print(json.dumps(dict(status='passed', runtime_inputs=1899, accepted_source_files=2217,
                          removed_candidate_files=14, copied_evidence_files=len(copied), restoration_receipt=str(D / receipt_path.name))))


if __name__ == '__main__':
    main()
