"""Verify the finite screen without live experimental sources or native payloads.

Reconstruct source deltas against the recorded Git base, check all producer and
fresh-process verification records, and recompute the frozen decision. This does
not rerun physical-file reads or benchmarks. Only pure checker functions from
the hash-pinned packet are loaded; measurement helpers are never executed.
"""
import argparse
import ast
import base64
from collections import defaultdict
from fractions import Fraction
import hashlib
import io
import json
import lzma
from pathlib import Path
import re
from statistics import median
import subprocess
import tempfile

EXPECTED_PACKET = '49b5834d666e8899b3d36f75b84466fa7bf428a4682ba260dbf6c02d64762ccf'
BASE = 'ca72dba5ed7bd8e5dc6ee38a4f3a59ed42bc014d'
ROOT = '/Users/dylan/LocalData/shardloom/conditional-investigations-20261019/'
ORIGINAL_REPO = '/Users/dylan/.codex/worktrees/public-docs-api/shardloom-local-repo/'
ATTEMPT = 'numeric-portfolio-806-a1/'
METRICS = ('numeric_cpu', 'write_cpu', 'column_bytes', 'file_bytes',
           'first_read', 'repeated_read', 'equal_zero', 'not_equal_zero')
CONSUMERS = METRICS[4:]


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def ordered_cases(spec, preflight=False):
    result = []
    for cohort in (['preflight'] if preflight else ['A', 'B']):
        for region in spec['source']['row_groups']:
            for repeat in range(1 if preflight else 3):
                profiles = list(spec['profiles'])
                if (repeat == 1) != (cohort == 'B'):
                    profiles.reverse()
                for profile in profiles:
                    result.append(dict(id=f'{cohort}-rg{region}-r{repeat}-{profile.replace("_", "-")}',
                        cohort=cohort, row_group=region, repeat=repeat, profile=profile))
    return result


def reconstruct_sources(repository, scratch, freeze, raw_at):
    patch = raw_at('numeric-portfolio-source-806.patch')
    assert sha(patch) == freeze['tracked_patch_sha256']
    with tempfile.TemporaryDirectory(prefix='numeric-portfolio-reopen-', dir=scratch) as tmp:
        work = Path(tmp)
        patch_path = work / 'frozen.patch'
        patch_path.write_bytes(patch)
        stats = subprocess.check_output(['git', 'apply', '--numstat', '--', str(patch_path)], cwd=work, text=True)
        changed = []
        for line in stats.splitlines():
            name = line.split('\t', 2)[2]
            relative = Path(name)
            assert not relative.is_absolute() and '..' not in relative.parts
            target = work / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(subprocess.check_output(['git', 'show', BASE + ':' + name], cwd=repository))
            changed.append(name)
        assert len(changed) == 4
        subprocess.run(['git', 'apply', '--check', '--', str(patch_path)], cwd=work, check=True)
        subprocess.run(['git', 'apply', '--', str(patch_path)], cwd=work, check=True)
        for row in freeze['extra_sources']:
            relative = Path(row['path'])
            assert not relative.is_absolute() and '..' not in relative.parts
            target = work / relative
            assert not target.exists()
            target.parent.mkdir(parents=True, exist_ok=True)
            raw = raw_at(row['saved_path'])
            assert sha(raw) == row['sha256']
            target.write_bytes(raw)
        assert len(freeze['extra_sources']) == 5
        base_rows = [row for row in freeze['sources'] if not (work / row['path']).exists()]
        queries = ''.join(BASE + ':' + row['path'] + '\n' for row in base_rows).encode()
        output = subprocess.check_output(['git', 'cat-file', '--batch'], input=queries, cwd=repository)
        stream = io.BytesIO(output)
        actual = {}
        for row in base_rows:
            object_id, kind, size = stream.readline().strip().split()
            assert kind == b'blob' and len(object_id) == 40
            raw = stream.read(int(size))
            assert len(raw) == int(size) and stream.read(1) == b'\n'
            actual[row['path']] = (len(raw), sha(raw))
        assert not stream.read()
        assert len(freeze['sources']) == len({row['path'] for row in freeze['sources']}) == 1908
        for row in freeze['sources']:
            if row['path'] not in actual:
                raw = (work / row['path']).read_bytes()
                actual[row['path']] = (len(raw), sha(raw))
            assert actual[row['path']] == (row['bytes'], row['sha256']), row['path']
    return dict(total=1908, changed_tracked=4, extra=5, unchanged_git_blobs=len(base_rows))


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
    assert sha(compressed) == EXPECTED_PACKET and len(compressed) == 1156636
    packet = json.loads(lzma.decompress(compressed))
    assert packet['schema_version'] == 'shardloom.numeric_portfolio_results.v1'
    assert packet['base_commit'] == BASE and packet['production_admitted'] is False
    members = {}
    for row in packet['members']:
        assert row['path'] not in members
        raw = base64.b64decode(row['base64'], validate=True)
        assert len(raw) == row['bytes'] and sha(raw) == row['sha256']
        members[row['path']] = raw
    assert len(members) == 323 and sum(map(len, members.values())) == 39116033

    def raw_at(relative):
        return members[ROOT + relative]

    def read_at(relative):
        return json.loads(raw_at(relative))

    spec = read_at('numeric-portfolio-protocol-805.json')
    protocol_sha = sha(raw_at('numeric-portfolio-protocol-805.json'))
    assert protocol_sha == packet['protocol_sha256'] == 'b277155df9cbb907e79ff10b1b53cb5aa0eb00463fe616416fb72226852434c4'
    assert sha(members[ORIGINAL_REPO + spec['design_path']]) == spec['design_sha256']
    source = read_at('numeric-portfolio-source-freeze-806.json')
    assert source['head'] == BASE and source['protocol_sha256'] == protocol_sha
    assert sha(raw_at('numeric-portfolio-source-freeze-806.json')) == packet['source_freeze_sha256']
    prebuild = members[source['prebuild_input_path']]
    assert sha(prebuild) == source['prebuild_input_sha256']
    assert json.loads(prebuild)['sources'] == source['sources']
    assert json.loads(prebuild)['before_check'] == source['prebuild_check'] == 'numeric-portfolio-tests-806d'
    reconstruction = reconstruct_sources(args.repository, scratch, source, raw_at)
    artifact = read_at('numeric-portfolio-artifact-806.json')
    artifact_sha = sha(raw_at('numeric-portfolio-artifact-806.json'))
    assert artifact_sha == packet['artifact_binding_sha256']
    assert artifact['source_freeze_sha256'] == packet['source_freeze_sha256']
    assert artifact['production_runtime_changed'] is False
    assert '11 tests, 0 benchmarks' in artifact['test_inventory']
    assert len(artifact['checks']) == 4
    for check in artifact['checks']:
        raw = members[check['receipt']]
        record = json.loads(raw)
        assert sha(raw) == check['sha256'] and record['status'] == 'passed'
        assert record['returncode'] == 0 and record['head'] == BASE
        assert record['log_sha256'] == check['log_sha256'] == sha(members[str(Path(check['receipt']).with_suffix('.log'))])
    setup_checks = []
    for path, raw in members.items():
        if Path(path).parent == Path(ROOT) and path.endswith('.json'):
            record = json.loads(raw)
            if 'command' not in record:
                continue
            assert record['process_group_drained'] and record['direct_child_reaped']
            assert record['timed_out'] is False and record['interrupted_signal'] is None
            assert sha(members[str(Path(path).with_suffix('.log'))]) == record['log_sha256']
            setup_checks.append(dict(name=Path(path).stem, status=record['status'], returncode=record['returncode']))
    assert len(setup_checks) == 13 and sum(row['status'] == 'failed' for row in setup_checks) == 5
    focused_log = raw_at('numeric-portfolio-tests-806d.log').decode()
    assert 'Compiling shardloom-vortex ' in focused_log
    assert 'test result: ok. 9 passed; 0 failed; 2 ignored;' in focused_log
    assert re.search(r'Ran 9 tests', raw_at('numeric-portfolio-oracle-806.log').decode())

    frozen_raw = raw_at(ATTEMPT + 'measurement-freeze.json')
    frozen = json.loads(frozen_raw)
    frozen_sha = sha(frozen_raw)
    assert frozen['protocol_sha256'] == protocol_sha
    assert frozen['artifact_binding_sha256'] == artifact_sha
    assert frozen['source_freeze_sha256'] == packet['source_freeze_sha256']
    assert frozen['source_pre_sha256'] == spec['source']['sha256']
    for helper in frozen['helpers']:
        raw = members[helper['path']]
        assert len(raw) == helper['bytes'] and sha(raw) == helper['sha256']
    selected = ordered_cases(spec, True) + ordered_cases(spec)
    assert len(selected) == len({case['id'] for case in selected}) == 42
    assert frozen['preflight_cases'] + frozen['cases'] == selected
    assert packet['case_ids'] == [case['id'] for case in selected]
    preflight = read_at(ATTEMPT + 'preflight-complete.json')
    assert preflight['status'] == 'passed' and preflight['decision_credit'] is False
    assert preflight['case_ids'] == [case['id'] for case in selected[:6]]
    complete = read_at(ATTEMPT + 'cohorts-complete.json')
    assert complete['status'] == 'completed' and complete['cost_analysis_performed'] is False
    assert complete['case_ids'] == [case['id'] for case in selected[6:]]
    assert complete['source_post_sha256'] == spec['source']['sha256']
    assert complete['source_generation_after'] == frozen['source_generation']
    assert complete['measurement_freeze_sha256'] == preflight['measurement_freeze_sha256'] == frozen_sha

    functions = {'integer', 'validate_records', 'score'}
    tree = ast.parse(raw_at('verify_numeric_portfolio_806.py'))
    nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in functions]
    assert {node.name for node in nodes} == functions and len(nodes) == len(functions)
    namespace = dict(defaultdict=defaultdict, median=median, METRICS=METRICS, CONSUMERS=CONSUMERS,
        protocol=lambda: spec, cases=lambda: ordered_cases(spec), PROTOCOL_SHA=protocol_sha, PAYLOAD_CAP=256 << 20)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), 'frozen_numeric_checker', 'exec'), namespace)
    samples = {}
    native_files = 0
    for case in selected:
        root = ATTEMPT + case['id'] + '/'
        config = read_at(root + 'config.json')
        assert config['case'] == case and config['measurement_freeze_sha256'] == frozen_sha
        assert config['protocol_sha256'] == protocol_sha and config['numeric_columns'] == spec['source']['numeric_columns']
        guard = read_at(ATTEMPT + 'logs/' + case['id'] + '.guard.json')
        assert guard['case'] == case and guard['status'] == 'passed' and guard['process_group_drained']
        assert guard['measurement_freeze_sha256'] == frozen_sha and guard['protocol_sha256'] == protocol_sha
        assert guard['artifact_binding_sha256'] == artifact_sha and guard['binary_sha256'] == artifact['sha256']
        assert guard['source_before'] == guard['source_after'] == frozen['source_generation']
        assert [child['kind'] for child in guard['children']] == ['producer', 'verify']
        for child in guard['children']:
            assert child['returncode'] == 0 and child['process_group_drained'] and child['direct_child_reaped']
            assert child['source_before'] == child['source_after'] == frozen['source_generation']
            assert child['command'][0] == artifact['path']
            log = raw_at(ATTEMPT + 'logs/' + child['log_name'])
            assert sha(log) == child['log_sha256']
            assert b'test result: ok. 1 passed; 0 failed; 0 ignored;' in log
        p_raw, v_raw = raw_at(root + 'producer.json'), raw_at(root + 'verification.json')
        assert sha(p_raw) == guard['producer_sha256'] and sha(v_raw) == guard['verification_sha256']
        producer, verified = json.loads(p_raw), json.loads(v_raw)
        metrics = namespace['validate_records'](case, producer, verified)
        retired = guard['artifact_retirement']
        assert retired['status'] == 'retired_after_independent_verification' and len(retired['files']) == 77
        for before, after in zip(producer['samples'], retired['files'], strict=True):
            assert (before['file_name'], before['file_bytes'], before['file_sha256']) == (after['name'], after['bytes'], after['sha256'])
        native_files += len(retired['files'])
        if case['cohort'] != 'preflight':
            samples[(case['cohort'], case['row_group'], case['repeat'], case['profile'])] = metrics
    decision = read_at(ATTEMPT + 'decision.json')
    assert sha(raw_at(ATTEMPT + 'decision.json')) == packet['decision_sha256']
    recomputed = namespace['score'](samples)
    assert all(decision[key] == value for key, value in recomputed.items())
    assert decision['measurement_freeze_sha256'] == frozen_sha
    assert decision['workflow_count'] == 36 and decision['file_count'] == 2772
    assert decision['status'] == packet['current_outcome'] == 'rejected'
    # Separately check all threshold comparisons with exact rational arithmetic.
    for cohort in decision['cohort_scores']:
        for aggregate in cohort['aggregates']:
            bound = aggregate['gate_ratio']
            if bound is not None:
                passed = Fraction(aggregate['reduced_numeric'], aggregate['current']) <= Fraction(str(bound))
                assert passed == aggregate['passed']
    assert not decision['failed_aggregate_gates']
    assert len(decision['repeated_individual_control_regressions']) == 1
    regression = decision['repeated_individual_control_regressions'][0]
    assert (regression['column'], regression['region'], regression['metric']) == ('DontCountHits', 113, 'write_cpu')
    for witness in regression['witnesses']:
        assert witness['fails'] and Fraction(witness['reduced_numeric'], witness['current']) > Fraction(11, 10)
        assert witness['reduced_numeric'] - witness['current'] > 100000
    result = dict(status='passed', packet_sha256=EXPECTED_PACKET, members_verified=len(members),
        source_reconstruction=reconstruction, setup_checks=setup_checks, preflight_workflows=6,
        measured_workflows=36, native_file_records_verified=native_files,
        source_values_compared_during_original_runs=77 * 32768 * 42,
        current_outcome='rejected', production_admitted=False,
        original_member_files_required=False, original_working_tree_changes_required=False,
        physical_artifacts_reopened=False, old_executable_required=False,
        complete_decision_recomputed=True, exact_threshold_checks_passed=True)
    with args.output.open('x') as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write('\n')
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
