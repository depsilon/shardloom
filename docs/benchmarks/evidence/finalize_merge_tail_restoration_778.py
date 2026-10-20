"""Bind exact production restoration and copy the verified finite attribution evidence."""
from pathlib import Path
import hashlib
import json
import lzma
import subprocess

E = Path(__file__).resolve().parent
W = Path('/Users/dylan/.codex/worktrees/public-docs-api/shardloom-local-repo')
D = W / 'docs/benchmarks/evidence'
BASE = 'd732d7889207e7d8fc0de8d746de1623174c1dcf'
PACK = E / 'merge-tail-portable-778'


def sha(data):
    return hashlib.sha256(data).hexdigest()


def identity(path):
    path = Path(path)
    assert path.is_file() and not path.is_symlink(), path
    data = path.read_bytes()
    return dict(path=str(path), bytes=len(data), sha256=sha(data))


def runtime(path):
    return path.endswith('.rs') or Path(path).name in ('Cargo.toml', 'Cargo.toml.orig', 'Cargo.lock')


def main():
    assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=W, text=True).strip() == BASE
    proof_path = E / 'merge-tail-packet-verification-778.json'
    proof = json.loads(proof_path.read_text())
    plan_path = E / 'merge-tail-restoration-plan-778.json'
    plan = json.loads(plan_path.read_text())
    assert proof['status'] == 'passed' and proof['scope_result'] == 'no_signal'
    assert proof['restoration_plan'] == identity(plan_path)
    assert proof['release_workflows'] == 236 and proof['debug_workflows_without_decision_credit'] == 40
    assert len(proof['reconstructed_versions']) == 4
    assert len(plan['files']) == 5 and plan['baseline'] == BASE
    assert plan['all_removed_source_content_preserved'] is True
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
        descriptor, name = entry.split(b'\t')
        name = name.decode()
        if runtime(name):
            expected[name] = descriptor.decode().split()[2]
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
    fmt_path = E / 'merge-tail-restored-fmt-778.json'
    fmt = json.loads(fmt_path.read_text())
    assert fmt['status'] == 'passed' and fmt['returncode'] == 0 and fmt['process_group_drained'] is True
    assert fmt['command'] == ['/Users/dylan/.cargo/bin/rustup', 'run', '1.99.0', 'cargo', 'fmt', '--all', '--', '--check']
    assert fmt['log_sha256'] == sha((E / 'merge-tail-restored-fmt-778.log').read_bytes())
    removal_patch = E / 'merge-tail-observer-removal-778.patch'
    assert removal_patch.read_text().startswith('*** Begin Patch\n')
    compressed_patch = E / 'merge-tail-observer-removal-778.patch.xz'
    payload = lzma.compress(removal_patch.read_bytes(), preset=6)
    if compressed_patch.exists():
        assert compressed_patch.read_bytes() == payload
    else:
        with compressed_patch.open('xb') as stream:
            stream.write(payload)
    assert lzma.decompress(compressed_patch.read_bytes()) == removal_patch.read_bytes()
    copied = []

    def copy(source):
        source = Path(source)
        target = D / source.name
        data = source.read_bytes()
        if target.exists():
            assert target.read_bytes() == data, target
        else:
            with target.open('xb') as stream:
                stream.write(data)
        assert target.read_bytes() == data
        copied.append(dict(path=str(target.relative_to(W)), bytes=len(data), sha256=sha(data)))

    index_path = PACK / 'merge-tail-portable-778.json'
    index = json.loads(index_path.read_text())
    assert proof['packet'] == identity(index_path) and index['scope_result'] == 'no_signal'
    for kind in ('archive', 'manifest'):
        row = index[kind]
        actual = identity(PACK / row['file'])
        assert (actual['bytes'], actual['sha256']) == (row['bytes'], row['sha256'])
    for suffix in ('.json', '.tar.xz', '.manifest.json.xz'):
        copy(PACK / f'merge-tail-portable-778{suffix}')
    for name in ('merge-tail-attribution-protocol-776.json', 'merge-tail-verification-776.json',
                 'merge-tail-packet-verification-778.json', 'merge-tail-restoration-plan-778.json',
                 'merge-tail-restored-fmt-778.json', 'merge-tail-restored-fmt-778.log',
                 'merge-tail-package-failure-778.json', 'merge-tail-package-778.json',
                 'merge-tail-packet-verify-778.json', 'merge-tail-source-accounting-failure-775.json',
                 'merge-tail-storage-admission-failure-775.json',
                 'merge-tail-observer-removal-778.patch.xz', 'compact-index-acceptance-source-764.json',
                 'verify_merge_tail_packet_778.py', 'finalize_merge_tail_restoration_778.py'):
        copy(E / name)
    result = dict(schema_version='shardloom.merge_tail_restoration.v1', status='passed', baseline=BASE,
                  runtime_input_count=1899, all_runtime_inputs_match_baseline=True, runtime_sources=inventory,
                  removed_observer_files=plan['files'], accepted_source_manifest=identity(accepted_path),
                  prior_accepted_source_files=2217, all_prior_accepted_source_contents_unchanged=True,
                  packet_verification=identity(proof_path), copied_evidence=copied,
                  format_check=identity(fmt_path), finalizer=identity(__file__),
                  applied_removal_patch=identity(removal_patch), compressed_patch=identity(compressed_patch),
                  removal_patch_decompressed_bytes_verified=True, native_suites_or_benchmarks_repeated=False,
                  production_behavior_changed=False, candidate_schedule_implemented=False,
                  finite_scope_result='no_signal', release_workflows=236, debug_workflows_without_decision_credit=40,
                  invalid_attempt_recorded_workflows=0, v3_partial_completed_public_operations=1,
                  full43_performance_claim=False)
    receipt_path = E / 'merge-tail-restoration-778.json'
    with receipt_path.open('x') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    copy(receipt_path)
    print(json.dumps(dict(status='passed', runtime_inputs=1899, accepted_source_files=2217,
                          removed_observer_files=5, copied_evidence_files=len(copied),
                          restoration_receipt=str(D / receipt_path.name))))


if __name__ == '__main__':
    main()
