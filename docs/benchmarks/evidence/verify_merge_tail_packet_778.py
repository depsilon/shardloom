"""Independently reopen all packet members and reconstruct four frozen source sets."""
from __future__ import annotations

import hashlib
import json
import lzma
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parent
REPO = Path('/Users/dylan/.codex/worktrees/public-docs-api/shardloom-local-repo')
PACK = ROOT / 'merge-tail-portable-778'
HEAD = 'd732d7889207e7d8fc0de8d746de1623174c1dcf'
sys.path.insert(0, str(REPO / 'scripts'))
from local_uat_storage import GIB, MIB, check_budgets, require_local_path


def sha(data):
    return hashlib.sha256(data).hexdigest()


def identity(path):
    path = Path(path)
    assert path.is_file() and not path.is_symlink(), path
    data = path.read_bytes()
    return dict(path=str(path), bytes=len(data), sha256=sha(data))


def save(path, value):
    with path.open('x') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n')


def main():
    require_local_path(ROOT, Path.home(), sys.platform)
    check_budgets(ROOT, PACK, ROOT, min_free_bytes=12 * GIB, reserve_bytes=128 * MIB,
                  max_workspace_bytes=100 * GIB, max_log_bytes=192 * MIB)
    index_path = PACK / 'merge-tail-portable-778.json'
    index = json.loads(index_path.read_text())
    assert index['status'] == 'passed' and index['production_head'] == HEAD
    assert index['production_optimization'] is index['actual_schedule_speedup'] is False
    assert index['release_workflows'] == 236 and index['debug_workflows_without_decision_credit'] == 40
    for key in ('archive', 'manifest'):
        row = index[key]
        actual = identity(PACK / row['file'])
        assert (actual['bytes'], actual['sha256']) == (row['bytes'], row['sha256'])
    manifest = json.loads(lzma.decompress((PACK / index['manifest']['file']).read_bytes()))
    assert manifest['all_members_reopened'] is index['all_members_reopened'] is True
    rows = manifest['members']
    assert len(rows) == index['member_count']
    assert len({r['path'] for r in rows}) == len(rows)
    by_original = {r['original_path']: r for r in rows}
    assert len(by_original) == len(rows)
    directories = manifest['directories']
    assert len(directories) == index['empty_directory_count'] == 2
    assert {r['path'].split('/')[0] for r in directories} == {'failed-v1', 'failed-v3'}
    assert all(r['type'] == 'empty_directory' for r in directories)
    expected = sorted([*rows, *directories], key=lambda row: row['path'])
    kept = {}
    with tempfile.TemporaryDirectory(prefix='merge-tail-packet-reopen-778-', dir=ROOT) as tmp:
        temporary = Path(tmp)
        base = temporary / 'base'
        base.mkdir()
        with tarfile.open(PACK / index['archive']['file'], 'r:xz') as archive:
            assert [m.name.rstrip('/') + '/' if m.isdir() else m.name for m in archive.getmembers()] == [r['path'] for r in expected]
            for row in expected:
                member = archive.getmember(row['path'])
                path = Path(member.name)
                assert not path.is_absolute() and '..' not in path.parts
                if row.get('type') == 'empty_directory':
                    assert member.isdir() and member.size == 0
                    original = Path(row['original_path'])
                    assert original.is_dir() and not original.is_symlink() and not any(original.iterdir())
                    continue
                assert member.isfile() and member.size == row['bytes']
                data = archive.extractfile(member).read()
                assert (len(data), sha(data)) == (row['bytes'], row['sha256'])
                original = Path(row['original_path'])
                assert original.is_file() and not original.is_symlink()
                assert original.read_bytes() == data, original
                if '/witnesses/' in member.name:
                    assert sha(data) == path.stem
                if member.name.startswith('sources/accepted-base/'):
                    target = base / member.name.removeprefix('sources/accepted-base/')
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(data)
                elif member.name.startswith('sources/') or member.name.startswith('evidence/') and member.name.endswith(('.json', '.patch', '.log')) or member.name.endswith('.jsonl'):
                    kept[member.name] = data
        base_inventory = json.loads(kept['evidence/merge-tail-accepted-base-774.json'])
        assert len(base_inventory['sources']) == 1899 and base_inventory['head'] == HEAD
        actual_names = sorted(p.relative_to(base).as_posix() for p in base.rglob('*') if p.is_file())
        assert actual_names == [r['path'] for r in base_inventory['sources']]
        for row in base_inventory['sources']:
            value = identity(base / row['path'])
            assert (value['bytes'], value['sha256']) == (row['bytes'], row['sha256'])
        reconstructed = []
        for version, helpers in (('771', 23), ('773', 28), ('774', 32), ('776', 39)):
            label = f'merge-tail-attribution-{version}-a1'
            freeze = json.loads(kept[f'evidence/{label}-source-freeze.json'])
            assert freeze['head'] == HEAD and len(freeze['sources']) == 1901
            assert len(freeze['helpers']) == helpers
            patch = kept[f'evidence/{label}-tracked.patch']
            assert sha(patch) == freeze['patch_sha256']
            stage = temporary / f'stage-{version}'
            shutil.copytree(base, stage)
            patch_file = temporary / f'{version}.patch'
            patch_file.write_bytes(patch)
            subprocess.run(['git', 'apply', '--check', '--', str(patch_file)], cwd=stage, check=True, capture_output=True)
            subprocess.run(['git', 'apply', '--whitespace=nowarn', '--', str(patch_file)], cwd=stage, check=True, capture_output=True)
            for relative in freeze['extra_sources']:
                target = stage / relative
                assert not target.exists()
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(kept[f'sources/{label}-extra/{relative}'])
            names = sorted(p.relative_to(stage).as_posix() for p in stage.rglob('*') if p.is_file())
            assert names == [r['path'] for r in freeze['sources']]
            for row in freeze['sources']:
                actual = identity(stage / row['path'])
                assert (actual['bytes'], actual['sha256']) == (row['bytes'], row['sha256'])
            for helper in freeze['helpers']:
                original = helper['path']
                if original == str(REPO / 'docs/architecture/native-merge-tail-design-2026-10-19.md'):
                    if version == '773': original = str(ROOT / 'merge-tail-design-v2-preserved-773.md')
                    if version == '774': original = str(ROOT / 'merge-tail-design-v3-preserved-776.md')
                retained = by_original[original]
                assert (retained['bytes'], retained['sha256']) == (helper['bytes'], helper['sha256'])
            reconstructed.append(dict(label=label, source_files=len(names), helpers=helpers,
                                      all_source_and_helper_identities_match=True))
            shutil.rmtree(stage)
    supervised = 0
    for name, data in kept.items():
        if name.startswith('evidence/merge-tail-') and name.endswith('.json'):
            record = json.loads(data)
            if 'log_sha256' in record:
                log = name.removesuffix('.json') + '.log'
                assert sha(kept[log]) == record['log_sha256'] and record['process_group_drained'] is True
                supervised += 1
    for filename in ('merge-tail-protocol-admission-failure-772.json', 'merge-tail-collector-admission-failure-773.json'):
        failure = json.loads(kept['evidence/' + filename])
        assert failure['completed_workflows'] == failure['measured_workflows'] == 0
        assert failure['engine_measurement_available'] is False
    failure = json.loads(kept['evidence/merge-tail-source-accounting-failure-775.json'])
    assert failure['completed_recorded_workflows'] == failure['measured_workflows'] == failure['engine_samples_emitted'] == 0
    assert failure['public_native_operations_completed_before_harness_assertion'] == 1
    assert failure['final_cleanup_assertions_reached'] is False
    assert kept['failed-v1/preflight-preflight-off.jsonl'] == kept['failed-v3/preflight-preflight-off.jsonl'] == b''
    refused = json.loads(kept['evidence/merge-tail-focused-spill-774-a1.json'])
    assert refused['status'] == 'failed' and refused.get('pid') is refused.get('returncode') is None
    assert 'evidence/merge-tail-focused-spill-774-a1.log' not in kept
    checked = json.loads(kept['evidence/merge-tail-verification-776.json'])
    assert checked['status'] == 'passed' and checked['scope_result'] == index['scope_result']
    assert checked['total_workflows'] == checked['total_operations'] == 236
    assert checked['all_complete_values_types_traces_and_decisions_recomputed'] is True
    assert checked['debug_preflight_workflows_without_decision_credit'] == 40
    stream_counts = {}
    for prefix, expected_count, expected_streams in (('raw-v4', 236, 6), ('debug-v4', 40, 2)):
        streams = {name: data for name, data in kept.items() if name.startswith(prefix + '/') and name.endswith('.jsonl')}
        assert len(streams) == expected_streams
        records = [json.loads(line) for data in streams.values() for line in data.splitlines()]
        assert len(records) == expected_count
        refs = {f'{prefix}/witnesses/{r[key]}.json' for r in records for key in (
            'source_rows_witness', 'source_schema_witness', 'expected_rows_witness',
            'expected_schema_witness', 'actual_rows_witness', 'actual_schema_witness')}
        assert refs == {r['path'] for r in rows if r['path'].startswith(f'{prefix}/witnesses/')}
        if prefix == 'raw-v4':
            assert sum(r['warmup'] for r in records) == 4
            assert sum(r['cohort'] == 'preflight' for r in records) == 40
            assert sum(not r['warmup'] and r['cohort'] != 'preflight' for r in records) == 192
        else:
            assert all(r['cohort'] == 'preflight' and r['warmup'] is False for r in records)
        stream_counts[prefix] = dict(streams=len(streams), workflows=len(records), witnesses=len(refs))
    assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip() == HEAD
    freeze = json.loads(kept['evidence/merge-tail-attribution-776-a1-source-freeze.json'])
    patch = kept['evidence/merge-tail-attribution-776-a1-tracked.patch']
    assert subprocess.check_output(['git', 'diff', 'HEAD', '--binary'], cwd=REPO) == patch
    modified = subprocess.check_output(['git', 'diff', '--name-only'], cwd=REPO, text=True).splitlines()
    assert set(modified) == {'shardloom-vortex/src/local_primitive_prepared_relational.rs', 'shardloom-vortex/src/local_primitive_relational_tests.rs', 'shardloom-vortex/src/local_primitives/native_relational_spill.rs'}
    source_map = {r['path']: r for r in freeze['sources']}
    restoration = []
    for relative in [*modified, *freeze['extra_sources']]:
        current = identity(REPO / relative)
        assert (current['bytes'], current['sha256']) == (source_map[relative]['bytes'], source_map[relative]['sha256'])
        row = dict(path=relative, tracked=relative in modified, observed_sha256=current['sha256'])
        if row['tracked']:
            row['baseline_sha256'] = sha(subprocess.check_output(['git', 'show', f'{HEAD}:{relative}'], cwd=REPO))
        restoration.append(row)
    plan_path = ROOT / 'merge-tail-restoration-plan-778.json'
    save(plan_path, dict(status='verified_before_observer_removal', baseline=HEAD, packet=identity(index_path),
                         files=restoration, all_removed_source_content_preserved=True))
    output = ROOT / 'merge-tail-packet-verification-778.json'
    save(output, dict(status='passed', production_optimization=False, scope_result=index['scope_result'],
                      packet=identity(index_path), archive_sha256=index['archive']['sha256'], manifest_sha256=index['manifest']['sha256'],
                      member_count=len(rows), empty_directories_verified=2, all_members_and_original_bytes_verified=True,
                      reconstructed_versions=reconstructed, guarded_receipts_with_exact_logs=supervised,
                      release_workflows=236, debug_workflows_without_decision_credit=40,
                      invalid_attempt_recorded_workflows=0, v3_partial_completed_public_operations=1,
                      streams=stream_counts, restoration_plan=identity(plan_path), verifier=identity(__file__)))
    check_budgets(ROOT, PACK, ROOT, min_free_bytes=12 * GIB, reserve_bytes=0,
                  max_workspace_bytes=100 * GIB, max_log_bytes=192 * MIB)
    print(json.dumps(dict(status='passed', members=len(rows), reconstructed_versions=4,
                         restoration_files=len(restoration), output=str(output))))


if __name__ == '__main__':
    main()
