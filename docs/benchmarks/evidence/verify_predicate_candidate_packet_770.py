"""Reopen immutable candidate evidence and prepare an exact restoration plan."""
from pathlib import Path
import hashlib
import json
import lzma
import subprocess
import tarfile

E = Path(__file__).resolve().parent
W = Path('/Users/dylan/.codex/worktrees/public-docs-api/shardloom-local-repo')
PACK = E / 'predicate-candidate-portable-770'
BASE = '896c71793a8859a6b4770bbc8a9fe0b85d755cf8'


def sha(data):
    return hashlib.sha256(data).hexdigest()


def ident(path):
    path = Path(path)
    data = path.read_bytes()
    return dict(path=str(path), bytes=len(data), sha256=sha(data))


def save(path, data):
    with path.open('x') as stream:
        json.dump(data, stream, indent=2)
        stream.write('\n')


def reopen(folder, name):
    index_path = folder / f'{name}.json'
    index = json.loads(index_path.read_text())
    for key in ('archive', 'manifest'):
        row = index[key]
        data = (folder / row['file']).read_bytes()
        assert len(data) == row['bytes'] and sha(data) == row['sha256']
    manifest = json.loads(lzma.decompress((folder / index['manifest']['file']).read_bytes()))
    assert manifest['all_members_reopened'] and index['all_members_reopened']
    rows = manifest['members']
    assert len(rows) == index['member_count']
    members = {}
    with tarfile.open(folder / index['archive']['file'], 'r:xz') as archive:
        assert archive.getnames() == [r['path'] for r in rows]
        for row in rows:
            info = archive.getmember(row['path'])
            assert info.isfile() and not Path(info.name).is_absolute() and '..' not in Path(info.name).parts
            data = archive.extractfile(info).read()
            assert (len(data), sha(data)) == (row['bytes'], row['sha256'])
            original = Path(row['original_path'])
            assert original.is_file() and not original.is_symlink()
            assert original.read_bytes() == data, str(original)
            members[row['path']] = data
    assert sum(len(v) for v in members.values()) == index['uncompressed_bytes']
    return index, members, ident(index_path)


def main():
    index, members, index_identity = reopen(PACK, 'predicate-candidate-portable-770')
    assert len(members) == 211 and not index['production_accepted']
    assert index['scope_result'] == 'candidate_comparison_rejected'
    attr, _, attr_identity = reopen(E / 'predicate-attribution-portable-767', 'predicate-attribution-portable-767')
    assert attr['member_count'] == 123 and attr['scope_result'] == 'attribution_positive'
    frozen = json.loads(members['evidence/predicate-candidate-769-a2-source-freeze.json'])
    assert frozen['head'] == subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=W, text=True).strip() == BASE
    assert len(frozen['sources']) == 1904
    source_map = {r['path']: r for r in frozen['sources']}
    for path, row in source_map.items():
        current = (W / path).read_bytes()
        assert (len(current), sha(current)) == (row['bytes'], row['sha256'])
    patch = members['evidence/predicate-candidate-769-a2-tracked.patch']
    assert sha(patch) == frozen['patch_sha256']
    assert patch == subprocess.check_output(['git', 'diff', '--binary'], cwd=W)
    for path in frozen['extra_sources']:
        assert members[f'sources/final-a2-extra/{path}'] == (W / path).read_bytes()
    # Every guarded receipt must retain the exact log it authenticates.
    supervised = 0
    for name, data in members.items():
        if not name.startswith('evidence/predicate-candidate-') or not name.endswith('.json'):
            continue
        value = json.loads(data)
        if 'log_sha256' in value:
            log_name = name.removesuffix('.json') + '.log'
            assert log_name in members and sha(members[log_name]) == value['log_sha256']
            assert value['process_group_drained']
            supervised += 1
    decision = json.loads(members['evidence/predicate-candidate-769-a2-decision.json'])
    checked = json.loads(members['evidence/predicate-candidate-verification-770.json'])
    assert checked['decision']['sha256'] == sha(members['evidence/predicate-candidate-769-a2-decision.json'])
    assert checked['status'] == 'passed' and checked['all_gates_independently_recomputed']
    assert checked['workflows'] == 1828 and checked['operations'] == 4260
    witness_refs = set()
    for row in decision['streams'].values():
        data = members[f"evidence/predicate-candidate-769-a2/{row['stream']}"]
        assert sha(data) == row['sha256'] and len(data) == row['bytes']
        records = [json.loads(line) for line in data.splitlines()]
        assert len(records) == row['workflows']
        assert sum(len(r['operations']) for r in records) == row['operations']
        for record in records:
            witness_refs.update(record[k] for k in ('source_rows_witness', 'source_schema_witness'))
            for operation in record['operations']:
                witness_refs.update(operation[k] for k in ('actual_rows_witness', 'actual_schema_witness', 'expected_rows_witness', 'expected_schema_witness'))
    assert len(witness_refs) == 42
    for digest in witness_refs:
        data = members[f'evidence/predicate-candidate-769-a2/witnesses/{digest}.json']
        assert sha(data) == digest
    modified = subprocess.check_output(['git', 'diff', '--name-only'], cwd=W, text=True).splitlines()
    assert len(modified) == 9 and all(p.endswith('.rs') for p in modified)
    files = []
    for path in modified:
        current = (W / path).read_bytes()
        baseline = subprocess.check_output(['git', 'show', f'{BASE}:{path}'], cwd=W)
        files.append(dict(path=path, tracked=True, candidate_sha256=sha(current), baseline_sha256=sha(baseline)))
    for path in frozen['extra_sources']:
        assert path.endswith('.rs')
        files.append(dict(path=path, tracked=False, candidate_sha256=sha((W / path).read_bytes())))
    assert len(files) == 14
    plan = E / 'predicate-candidate-restoration-plan-770.json'
    save(plan, dict(status='verified_before_restoration', baseline=BASE, packet=index_identity,
                    source_freeze_sha256=sha(members['evidence/predicate-candidate-769-a2-source-freeze.json']),
                    tracked_patch_sha256=sha(patch), files=files, all_removed_source_content_preserved=True))
    receipt = E / 'predicate-candidate-packet-verification-770.json'
    save(receipt, dict(status='passed', scope_result='candidate_comparison_rejected', production_accepted=False,
                       packet=index_identity, archive_sha256=index['archive']['sha256'], manifest_sha256=index['manifest']['sha256'],
                       members=211, all_archive_members_and_original_bytes_verified=True,
                       guarded_receipts_with_exact_logs=supervised, candidate_source_inputs=1904,
                       workflows=1828, operations=4260, full_content_witnesses=42,
                       attribution_packet=attr_identity, attribution_members_reopened=123,
                       restoration_plan=ident(plan), verifier=ident(__file__),
                       package_review=ident(E / 'predicate-candidate-packager-review-770.json'),
                       comparison_or_native_source_changed=False))
    print(json.dumps(dict(status='passed', candidate_members=211, attribution_members=123,
                          restoration_files=len(files), verifier_receipt=str(receipt))))


if __name__ == '__main__':
    main()
