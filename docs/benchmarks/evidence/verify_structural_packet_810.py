"""Replay the frozen structural screen without live sources or native payloads.

Verify every archived member, reconstruct the exact experimental sources from
Git and the saved delta, and recompute all gates from complete recorded runs.
Only pure functions from the hash-pinned checker are loaded. Measurement drivers
are never executed. This does not reopen retired files or rerun benchmarks.
"""

import argparse
import ast
import base64
from fractions import Fraction
import hashlib
import io
import json
import lzma
from pathlib import Path
import re
import statistics
import subprocess
import tempfile

EXPECTED_PACKET = '9991f1cd88919533ca2f78260db2b93eeb51eec53e8b2f5f75ef0dee46339103'
BASE = 'ec2b27f121ff7e5e8f740c999dca7f433737fbc7'
ROOT = '/Users/dylan/LocalData/shardloom/conditional-investigations-20261019/'
ORIGINAL_REPO = '/Users/dylan/.codex/worktrees/public-docs-api/shardloom-local-repo/'
ATTEMPT = 'structural-screen-809-a1/'
CONSUMERS = ('equal', 'not_equal', 'contains', 'not_contains')
FAMILIES = ('original_write', 'rewrite', 'first_read', 'repeated_read', *CONSUMERS)
STAGES = [('producer', 'structural_payload_producer', 'producer.json'),
          ('verify', 'structural_payload_verify_and_rewrite', 'verification.json'),
          ('verify_rewrite', 'structural_payload_verify_rewrite', 'rewrite-verification.json')]


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def ordered_cases(preflight=False):
    result = []
    for cohort in (['preflight'] if preflight else ['A', 'B']):
        for region in [0, 113, 225]:
            for repeat in range(1 if preflight else 3):
                profiles = ['current', 'structural']
                if (repeat == 1) != (cohort == 'B'):
                    profiles.reverse()
                for profile in profiles:
                    result.append(dict(id=f'{cohort}-rg{region}-r{repeat}-{profile}',
                        cohort=cohort, row_group=region, repeat=repeat, profile=profile))
    return result


def reconstruct_sources(repository, scratch, snapshot, binding, raw_at):
    delta = raw_at('structural-source-809.patch')
    assert sha(delta) == binding['tracked_patch_sha256']
    with tempfile.TemporaryDirectory(prefix='structural-reopen-', dir=scratch) as temporary:
        work = Path(temporary)
        patch = work/'frozen.patch'
        patch.write_bytes(delta)
        stats = subprocess.check_output(['git', 'apply', '--numstat', '--', str(patch)], cwd=work, text=True)
        changed = []
        for line in stats.splitlines():
            name = line.split('\t', 2)[2]
            relative = Path(name)
            assert not relative.is_absolute() and '..' not in relative.parts
            target = work/relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(subprocess.check_output(['git', 'show', BASE+':'+name], cwd=repository))
            changed.append(name)
        assert len(changed) == 3
        subprocess.run(['git', 'apply', '--check', '--', str(patch)], cwd=work, check=True)
        subprocess.run(['git', 'apply', '--', str(patch)], cwd=work, check=True)
        for row in binding['extra_sources']:
            relative = Path(row['path'])
            assert not relative.is_absolute() and '..' not in relative.parts
            target = work/relative
            assert not target.exists()
            target.parent.mkdir(parents=True, exist_ok=True)
            raw = raw_at(row['saved_path'])
            assert sha(raw) == row['sha256']
            target.write_bytes(raw)
        assert len(binding['extra_sources']) == 6
        refs = snapshot['sources']
        assert len(refs) == len({row['path'] for row in refs}) == 1909
        base_rows = [row for row in refs if not (work/row['path']).exists()]
        queries = ''.join(BASE+':'+row['path']+'\n' for row in base_rows).encode()
        stream = io.BytesIO(subprocess.check_output(['git', 'cat-file', '--batch'], input=queries, cwd=repository))
        actual = {}
        for row in base_rows:
            object_id, kind, size = stream.readline().strip().split()
            assert kind == b'blob' and len(object_id) == 40
            raw = stream.read(int(size))
            assert len(raw) == int(size) and stream.read(1) == b'\n'
            actual[row['path']] = (len(raw), sha(raw))
        assert not stream.read()
        for row in refs:
            if row['path'] not in actual:
                raw = (work/row['path']).read_bytes()
                actual[row['path']] = (len(raw), sha(raw))
            assert actual[row['path']] == (row['bytes'], row['sha256']), row['path']
    return dict(total=1909, changed_tracked=3, extra=6, unchanged_git_blobs=len(base_rows))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packet', type=Path, required=True)
    parser.add_argument('--repository', type=Path, required=True)
    parser.add_argument('--scratch', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    assert not args.output.exists()
    scratch = args.scratch.resolve(strict=True)
    assert scratch.is_dir() and not any(part in ('Desktop', 'Documents', 'CloudDocs', 'Mobile Documents') for part in scratch.parts)
    compressed = args.packet.read_bytes()
    assert sha(compressed) == EXPECTED_PACKET and len(compressed) == 1295972
    packet = json.loads(lzma.decompress(compressed))
    assert packet['schema_version'] == 'shardloom.structural_payload_results.v1'
    assert packet['base_commit'] == BASE and packet['production_admitted'] is False
    members = {}
    for row in packet['members']:
        assert row['path'] not in members
        raw = base64.b64decode(row['base64'], validate=True)
        assert len(raw) == row['bytes'] and sha(raw) == row['sha256']
        members[row['path']] = raw
    assert len(members) == 424 and sum(map(len, members.values())) == 34168658

    def raw_at(relative):
        return members[relative if Path(relative).is_absolute() else ROOT+relative]

    def read_at(relative):
        return json.loads(raw_at(relative))

    spec = read_at('structural-payload-protocol-808.json')
    protocol_sha = sha(raw_at('structural-payload-protocol-808.json'))
    assert protocol_sha == packet['protocol_sha256'] == 'c533bdf5d4211c397ef7349975b2f0a2f99ff746e2b092af5f2532979efb1bbb'
    assert sha(members[ORIGINAL_REPO+spec['design_path']]) == spec['design_sha256']
    snapshot = read_at('structural-build-input-809.json')
    assert snapshot['head'] == spec['source_commit'] == BASE
    assert snapshot['protocol_sha256'] == protocol_sha
    assert snapshot['before_check'] == 'structural-screen-tests-809c'
    binding = read_at('structural-build-binding-809.json')
    binding_sha = sha(raw_at('structural-build-binding-809.json'))
    assert binding_sha == packet['binding_sha256']
    assert binding['prebuild_input_sha256'] == sha(raw_at('structural-build-input-809.json'))
    reconstruction = reconstruct_sources(args.repository, scratch, snapshot, binding, raw_at)
    assert '8 tests, 0 benchmarks' in binding['test_inventory']
    assert len(binding['checks']) == 4
    for check in binding['checks']:
        raw = members[check['path']]
        receipt = json.loads(raw)
        assert sha(raw) == check['sha256'] and receipt['status'] == 'passed'
        assert receipt['returncode'] == 0 and receipt['head'] == BASE
        assert receipt['process_group_drained'] and receipt['direct_child_reaped']
        assert receipt['log_sha256'] == sha(members[str(Path(check['path']).with_suffix('.log'))])
    setup_checks = []
    for path, raw in members.items():
        if Path(path).parent == Path(ROOT) and path.endswith('.json'):
            receipt = json.loads(raw)
            if 'command' not in receipt:
                continue
            assert receipt['process_group_drained'] and receipt['direct_child_reaped']
            assert receipt['timed_out'] is False and receipt['interrupted_signal'] is None
            assert receipt['log_sha256'] == sha(members[str(Path(path).with_suffix('.log'))])
            setup_checks.append(dict(name=Path(path).stem, status=receipt['status'], returncode=receipt['returncode']))
    assert sum(row['status'] == 'failed' for row in setup_checks) == 4
    native_log = raw_at('structural-screen-tests-809c.log').decode()
    assert 'Compiling shardloom-vortex ' in native_log
    assert 'test result: ok. 5 passed; 0 failed; 3 ignored;' in native_log
    assert re.search(r'Ran 7 tests', raw_at('structural-oracle-tests-810.log').decode())

    frozen_raw = raw_at(ATTEMPT+'measurement-freeze.json')
    frozen = json.loads(frozen_raw)
    frozen_sha = sha(frozen_raw)
    assert frozen_sha == packet['freeze_sha256']
    assert frozen['protocol_sha256'] == protocol_sha and frozen['binding_sha256'] == binding_sha
    assert frozen['source_pre_sha256'] == spec['source']['sha256']
    for helper in frozen['helpers']:
        raw = members[helper['path']]
        assert len(raw) == helper['bytes'] and sha(raw) == helper['sha256']
    selected = ordered_cases(True)+ordered_cases()
    assert len(selected) == len({case['id'] for case in selected}) == 42
    assert frozen['preflight_cases']+frozen['cases'] == spec['preflight_cases']+spec['cases'] == selected
    assert packet['case_ids'] == [case['id'] for case in selected]
    preflight = read_at(ATTEMPT+'preflight-complete.json')
    complete = read_at(ATTEMPT+'cohorts-complete.json')
    assert preflight['status'] == 'passed' and preflight['decision_credit'] is False and preflight['workflow_count'] == 6
    assert preflight['case_ids'] == [case['id'] for case in selected[:6]]
    assert complete['status'] == 'completed' and complete['cost_analysis_performed'] is False and complete['workflow_count'] == 36
    assert complete['case_ids'] == [case['id'] for case in selected[6:]]
    assert complete['source_post_sha256'] == spec['source']['sha256']
    assert complete['source_generation_after'] == frozen['source_generation']
    assert complete['freeze_sha256'] == preflight['freeze_sha256'] == frozen_sha

    names = {'integer', 'cost', 'file_record', 'native_record', 'validate_records', 'sample_metrics', 'evaluate'}
    tree = ast.parse(raw_at('verify_structural_screen_809.py'))
    nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
    assert {node.name for node in nodes} == names and len(nodes) == len(names)
    namespace = dict(statistics=statistics, CONSUMERS=CONSUMERS, FAMILIES=FAMILIES,
        protocol=lambda: spec, PROTOCOL_SHA=protocol_sha)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), 'frozen_structural_checker', 'exec'), namespace)
    raw_samples = {}
    stable_statistics = {}
    native_files = 0
    for case in selected:
        root = ATTEMPT+case['id']+'/'
        config = read_at(root+'config.json')
        assert config['case'] == case and config['freeze_sha256'] == frozen_sha and config['protocol_sha256'] == protocol_sha
        assert config['profile'] == case['profile'] and config['row_group'] == case['row_group']
        assert config['text_columns'] == [{k:v for k,v in row.items() if k != 'index'} for row in spec['source']['text_columns']]
        assert config['expected'] == [row for row in spec['expected_activation'] if row['row_group'] == case['row_group']]
        guard = read_at(ATTEMPT+'logs/'+case['id']+'.guard.json')
        assert guard['case'] == case and guard['status'] == 'passed' and guard['process_group_drained']
        assert guard['freeze_sha256'] == frozen_sha and guard['protocol_sha256'] == protocol_sha
        assert guard['binding_sha256'] == binding_sha and guard['binary_sha256'] == binding['binary']['sha256']
        assert guard['source_before'] == guard['source_after'] == frozen['source_generation']
        assert guard['limits'] == spec['limits']
        assert [child['kind'] for child in guard['children']] == [stage[0] for stage in STAGES]
        assert len({child['pid'] for child in guard['children']}) == 3
        records = []
        for child, (_, selector, result_name) in zip(guard['children'], STAGES, strict=True):
            assert child['returncode'] == 0 and child['process_group_drained'] and child['direct_child_reaped']
            assert child['source_before'] == child['source_after'] == frozen['source_generation']
            assert child['command'] == [binding['binary']['path'], 'vortex_ingest::structural_payload::screen::'+selector,
                '--exact', '--ignored', '--nocapture', '--test-threads=1']
            log = raw_at(ATTEMPT+'logs/'+child['log_name'])
            assert sha(log) == child['log_sha256'] and b'test result: ok. 1 passed; 0 failed; 0 ignored;' in log
            raw = raw_at(root+result_name)
            assert sha(raw) == guard[result_name+'_sha256']
            records.append(json.loads(raw))
        producer, verification, rewrite = records
        namespace['validate_records'](case, producer, verification, rewrite)
        expected_files = {}
        for directory, rows in [('moved', producer['samples']), ('rewritten_moved', [row['rewrite'] for row in verification['samples']])]:
            expected_files.update({directory+'/'+row['file_name']: row for row in rows})
        assert len(guard['moves']) == len(guard['artifact_retirement']['files']) == len(expected_files) == 56
        assert {row['to_path'] for row in guard['moves']} == {row['path'] for row in guard['artifact_retirement']['files']} == set(expected_files)
        for move in guard['moves']:
            row = expected_files[move['to_path']]
            for identity in [move['before'], move['after']]:
                assert identity['bytes'] == row['file_bytes'] and identity['sha256'] == row['file_sha256']
            for key in ('device', 'inode', 'bytes', 'mtime_ns', 'sha256'):
                assert move['before'][key] == move['after'][key]
        assert guard['artifact_retirement']['status'] == 'retired_after_both_independent_verifications'
        for retired in guard['artifact_retirement']['files']:
            row = expected_files[retired['path']]
            assert retired['bytes'] == row['file_bytes'] and retired['sha256'] == row['file_sha256']
        native_files += len(expected_files)
        for index, field in enumerate(spec['source']['text_columns']):
            key = (case['row_group'], field['name'])
            stats = verification['samples'][index]['original']['statistics']
            if key in stable_statistics:
                assert stable_statistics[key] == stats
            else:
                stable_statistics[key] = stats
            if case['cohort'] != 'preflight':
                key = (case['cohort'], case['row_group'], field['name'], case['profile'])
                raw_samples.setdefault(key, []).append(namespace['sample_metrics'](producer['samples'][index], verification['samples'][index]))
    medians = {}
    for key, samples in raw_samples.items():
        assert len(samples) == 3
        medians[key] = {metric:statistics.median(row[metric] for row in samples) for metric in samples[0]}
    accepted, controls, individual = namespace['evaluate'](medians, spec['source']['text_columns'])
    decision_raw = raw_at(ATTEMPT+'decision.json')
    decision = json.loads(decision_raw)
    assert sha(decision_raw) == packet['decision_sha256']
    assert not accepted and decision['decision'] == packet['outcome'] == 'reject_finite_profile'
    assert decision['controls'] == controls and decision['individual_failures'] == individual
    assert decision['medians'] == [dict(cohort=k[0], region=k[1], field=k[2], profile=k[3], metrics=v) for k,v in medians.items()]
    assert decision['completion_sha256'] == sha(raw_at(ATTEMPT+'cohorts-complete.json'))
    assert decision['preserved_original_text_fields'] == 28 and decision['verified_measured_workflows'] == 36
    assert decision['full_workflow_retention'] is False
    # Independently evaluate every threshold with exact rational arithmetic.
    for control in controls:
        failed = []
        for metric, limit in control['limits'].items():
            current = control['totals']['current'][metric]
            candidate = control['totals']['structural'][metric]
            if Fraction(candidate, current) > Fraction(str(limit)):
                failed.append(metric)
        assert set(failed) == set(control['failed']) == {'file_bytes', 'physical_column_bytes', 'original_write'}
    exact_individual = []
    for region in [0, 113, 225]:
        for field in spec['source']['text_columns']:
            for family in FAMILIES:
                failed = []
                for cohort in ['A', 'B']:
                    current = medians[(cohort, region, field['name'], 'current')][family]
                    candidate = medians[(cohort, region, field['name'], 'structural')][family]
                    failed.append(Fraction(candidate, current) > Fraction(11, 10) and candidate-current > 100_000)
                if all(failed):
                    exact_individual.append((region, field['name'], family))
    assert exact_individual == [(row['region'], row['field'], row['family']) for row in individual]
    assert len(individual) == 13 and all(row['family'] == 'original_write' for row in individual)
    result = dict(status='passed', packet_sha256=EXPECTED_PACKET, members_verified=len(members),
        source_reconstruction=reconstruction, setup_checks=setup_checks, preflight_workflows=6, measured_workflows=36,
        native_file_records_verified=native_files, fresh_native_processes=126,
        original_value_checks=28*32768*42, rewritten_value_checks=28*32768*42,
        current_outcome='rejected', production_admitted=False, original_member_files_required=False,
        original_working_tree_changes_required=False, physical_artifacts_reopened=False,
        old_executable_required=False, complete_decision_recomputed=True, exact_threshold_checks_passed=True)
    with args.output.open('x') as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write('\n')
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
