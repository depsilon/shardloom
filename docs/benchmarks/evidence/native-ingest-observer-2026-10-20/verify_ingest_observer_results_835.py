"""Replay archived source identities, workflow checks and observer decisions.

This needs the recorded Git base, not original executables or source data.
Retired event traces and Vortex payloads are not reconstructed or revalidated.
"""

import argparse
import ast
import base64
from fractions import Fraction
import gzip
import hashlib
import json
import lzma
import os
from pathlib import Path
import shlex
import stat
import statistics
import subprocess
import tempfile
from types import SimpleNamespace

BASE = '24d11e58f1e789581979580d1f82586ee76057a8'
E = '/Users/dylan/LocalData/shardloom/conditional-investigations-20261019'
W = '/Users/dylan/.codex/worktrees/public-docs-api/shardloom-local-repo'
ATTEMPT = E + '/ingest-overlap-834-a1'
SOURCE_SHA = '544b9b87d03ea4c12cf5a99e5d0f21c4be4e294c77af03d795e55220aa6704a3'
BINARY_SHA = '8ace9f77a71d34e9aab4bda883763560565acbe6c4772753692ff0e4a882861e'
FREEZE_SHA = '8312de9a889fd9278aca013ae8fc2f6431cf5ec4732988b5ccfae75a847bfb34'
VERIFIER_SHA = '999e458cfa2fe7855132e7229f3b65a3c8bf0b6fab06dcb4945e30c39dd87ce4'
RETENTION_SHA = 'cbe750491b8353d5546ba2877efffcf3e7407cc2a2530d9da6b5d9c8fd47b231'
PROTOCOL_SHA = '83da704cd7852ba3d638052b0bebcd39512e116ec0c74b5fe86f6f7f1eaaa5cf'
MIB, GIB = 1 << 20, 1 << 30


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def safe_relative(name):
    path = Path(name)
    assert name and not path.is_absolute() and '..' not in path.parts
    return path


def pure_functions(raw, expected_sha, names, namespace):
    assert digest(raw) == expected_sha
    tree = ast.parse(raw)
    selected = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
    assert len(selected) == len(names) and {node.name for node in selected} == names
    exec(compile(ast.Module(body=selected, type_ignores=[]), 'frozen_pure_checker', 'exec'), namespace)


def source_replay(source, members, repository, scratch):
    binding = source['binding']
    assert binding['base_commit'] == BASE
    work = scratch / 'source'
    work.mkdir()
    patch = work / 'source.patch'
    patch.write_text(source['source_delta']['tracked_patch'])
    stats = subprocess.check_output(['git', 'apply', '--numstat', '--', str(patch)], cwd=work, text=True)
    changed = [line.split('\t', 2)[2] for line in stats.splitlines()]
    assert len(changed) == len(set(changed)) == 12
    for name in changed:
        target = work / safe_relative(name)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(subprocess.check_output(['git', 'show', BASE + ':' + name], cwd=repository))
    subprocess.run(['git', 'apply', '--check', '--', str(patch)], cwd=work, check=True)
    subprocess.run(['git', 'apply', '--', str(patch)], cwd=work, check=True)
    extras = source['source_delta']['extra_files']
    assert len(extras) == 5
    for name, content in extras.items():
        target = work / safe_relative(name)
        assert not target.exists()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)
    unchanged = 0
    process = subprocess.Popen(['git', 'cat-file', '--batch'], cwd=repository,
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        for name, expected in binding['files'].items():
            relative = safe_relative(name)
            target = work / relative
            archived = members.get(W + '/' + name)
            if target.is_file() or archived is not None:
                raw = target.read_bytes() if target.is_file() else archived
                actual = dict(bytes=len(raw), sha256=digest(raw))
            else:
                process.stdin.write((BASE + ':' + name + '\n').encode())
                process.stdin.flush()
                header = process.stdout.readline().decode().strip().split()
                assert len(header) == 3 and header[1] == 'blob', (name, header)
                size = int(header[2]); remaining = size; sha = hashlib.sha256()
                while remaining:
                    block = process.stdout.read(min(1 << 20, remaining))
                    assert block, name
                    sha.update(block); remaining -= len(block)
                assert process.stdout.read(1) == b'\n'
                actual = dict(bytes=size, sha256=sha.hexdigest())
                unchanged += 1
            assert actual == expected, name
        process.stdin.close()
        assert process.wait(timeout=30) == 0
        assert not process.stderr.read()
    finally:
        if process.poll() is None:
            process.kill(); process.wait(timeout=30)
    return dict(files=len(binding['files']), changed_tracked=len(changed), extras=len(extras),
                unchanged_git_files=unchanged)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('packet', 'index', 'repository', 'scratch', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    assert not args.output.exists()
    scratch = args.scratch.resolve(strict=True)
    assert scratch.is_dir() and not any(part in ('Desktop', 'Documents', 'CloudDocs', 'Mobile Documents') for part in scratch.parts)
    index = json.loads(args.index.read_text())
    compressed = args.packet.read_bytes()
    assert index['status'] == 'passed' and len(compressed) == index['bytes']
    assert digest(compressed) == index['sha256']
    raw_packet = lzma.decompress(compressed)
    assert len(raw_packet) == index['raw_bytes']
    payload = json.loads(raw_packet)
    assert payload['schema_version'] == 'shardloom.ingest_observer_results.v1'
    assert payload['base_commit'] == BASE and payload['attempt'] == ATTEMPT
    assert payload['repository_root'] == W and payload['evidence_root'] == E
    assert payload['scheduling_candidate_implemented'] is False and payload['scheduling_change_retained'] is False
    assert payload['local_cargo_configuration']['worktree_config_present'] is False
    members = {}
    for entry in payload['members']:
        assert entry['path'] not in members
        raw = base64.b64decode(entry['base64'], validate=True)
        assert len(raw) == entry['bytes'] and digest(raw) == entry['sha256']
        members[entry['path']] = raw
    assert len(members) == index['members']
    assert sum(map(len, members.values())) == index['input_bytes']
    get = lambda relative: members[E + '/' + relative]
    read = lambda relative: json.loads(get(relative))
    for stage in ('build', 'qualify', 'clippy'):
        check = read('ingest-observer-834-r1-' + stage + '.json')
        assert check['status'] == 'passed' and check['returncode'] == 0 and check['process_group_drained'] is True
        assert digest(get('ingest-observer-834-r1-' + stage + '.log')) == check['log_sha256']
        assert check['environment']['CARGO_TARGET_DIR'] == '/Users/dylan/.cache/shardloom/cargo-target'
    failure = read('ingest-observer-package-failure-835-r1.json')
    assert failure['status'] == 'failed_before_packet_creation' and failure['packet_created'] is False
    assert digest(failure['helper']['source'].encode()) == failure['helper']['sha256']
    assert digest(get('ingest-observer-package-835-r1.json')) == failure['failure_receipt_sha256']
    assert read('ingest-observer-package-835-r1.json')['status'] == 'failed'
    frozen_raw = get('ingest-overlap-834-a1/measurement-freeze.json')
    source_raw = get('ingest-observer-834-r1-source.json')
    binary_raw = get('ingest-observer-834-r1-binary.json')
    assert digest(frozen_raw) == FREEZE_SHA and digest(source_raw) == SOURCE_SHA and digest(binary_raw) == BINARY_SHA
    freeze, source, binary = map(json.loads, (frozen_raw, source_raw, binary_raw))
    assert freeze['source_freeze_sha256'] == SOURCE_SHA and freeze['artifact_binding_sha256'] == BINARY_SHA
    assert binary['source']['sha256'] == SOURCE_SHA and binary['status'] == 'passed'
    assert binary['executable']['sha256'] == freeze['binary_sha256']
    for reference in freeze['helpers']:
        assert digest(members[reference['path']]) == reference['sha256'], reference['path']
    for key in ('driver', 'guard', 'workload_guard', 'amendment', 'design'):
        reference = source['binding'][key]
        assert digest(members[reference['path']]) == reference['sha256']
    for key, expected in (('source_freeze', SOURCE_SHA), ('binary_binding', BINARY_SHA), ('measurement_freeze', FREEZE_SHA)):
        assert payload[key]['sha256'] == expected and digest(members[payload[key]['path']]) == expected
    for name, record in binary['compilation_inputs'].items():
        assert source['binding']['files'][name] == record
    compiled = set()
    for package in binary['packages']:
        reference = package['dependency_record']
        raw = members[reference['path']]
        assert len(raw) == reference['bytes'] and digest(raw) == reference['sha256']
        parsed = shlex.split(raw.decode().splitlines()[0].split(': ', 1)[1])
        paths = [os.path.normpath(name if name.startswith('/') else W + '/' + name) for name in parsed]
        local = [name.removeprefix(W + '/') for name in paths if name.startswith(W + '/')]
        assert local == package['inputs'] and len(local) == len(paths)
        assert not package['generated']
        compiled.update(local)
    assert compiled == set(binary['compilation_inputs']) and len(binary['packages']) == 11
    assert binary['artifact']['fresh'] is False and binary['artifact']['profile']['opt_level'] == '3'
    checks = []
    for reference in freeze['prerequisite_checks']:
        raw = members[reference['receipt']]
        assert digest(raw) == reference['sha256']
        check = json.loads(raw)
        log = members[str(Path(reference['receipt']).with_suffix('.log'))]
        assert check['status'] == 'passed' and check['returncode'] == 0 and check['process_group_drained'] is True
        assert digest(log) == check['log_sha256'] == reference['log_sha256']
        checks.append(Path(reference['receipt']).stem)
    assert len(checks) == 9
    protocol_raw = get('ingest-overlap-attribution-protocol-797.json')
    assert digest(protocol_raw) == PROTOCOL_SHA == freeze['protocol_sha256']
    protocol = json.loads(protocol_raw)
    cases = []
    for cohort in protocol['cohorts']:
        for cell in protocol['cells']:
            for repeat in range(3):
                roles = ['ordinary', 'observed'] if repeat != 1 else ['observed', 'ordinary']
                if cohort == 'B': roles.reverse()
                cases.extend(dict(id=f'{cohort}-{cell["id"]}-r{repeat}-{role}', cohort=cohort,
                                  repeat=repeat, role=role, cell=cell) for role in roles)
    assert len(cases) == 84 and cases == freeze['cases']
    preflight = [dict(id=f'preflight-{cell["id"]}-{role}', cohort='preflight', repeat=0, role=role, cell=cell)
                 for cell in protocol['cells'] for role in ('ordinary', 'observed')]
    assert len(preflight) == 14 and preflight == freeze['preflight_cases']
    barrier = read('ingest-overlap-834-a1/cohorts-complete.json')
    assert barrier['case_ids'] == [case['id'] for case in cases]
    assert barrier['workflow_count'] == 84 and barrier['cost_analysis_performed'] is False
    decision_raw = get('ingest-overlap-834-a1/decision.json')
    decision = json.loads(decision_raw)
    assert digest(decision_raw) == index['decision_sha256'] == payload['decision']['sha256']
    metrics = {row['case']['id']: row for row in decision['metrics']}
    assert len(metrics) == 84 and set(metrics) == {case['id'] for case in cases}
    trace_rows = {row['path']: row for row in payload['event_trace_identities']}
    assert len(trace_rows) == len(payload['event_trace_identities']) == index['traces_indexed'] == 49
    namespace = dict(Fraction=Fraction, statistics=statistics, GIB=GIB)
    pure_functions(get('verify_ingest_overlap_834.py'), VERIFIER_SHA,
                   {'natural', 'verify_admission', 'verify_activation', 'decide'}, namespace)
    observed = 0
    with tempfile.TemporaryDirectory(prefix='ingest-observer-replay-', dir=scratch) as tmp:
        root = Path(tmp)
        source_result = source_replay(source, members, args.repository, root)
        data = root / 'data'; data.mkdir()
        for name, raw in members.items():
            if name.startswith(ATTEMPT + '/'):
                target = data / safe_relative(name.removeprefix(ATTEMPT + '/'))
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(raw)
        c = SimpleNamespace(DATA=data, MIB=MIB, GIB=GIB, ARTIFACT_CAP=256*MIB,
                            STORAGE_AMENDMENT_SHA=freeze['storage_amendment_sha256'],
                            read=lambda p: json.loads(Path(p).read_text()),
                            sha=lambda p: digest(Path(p).read_bytes()))
        retention = dict(c=c, stat=stat, gzip=gzip, hashlib=hashlib, json=json)
        pure_functions(get('ingest_case_retention_834.py'), RETENTION_SHA,
                       {'identity', 'verify_compaction', 'read_result', 'verify_retired_case'}, retention)
        for case in [*preflight, *cases]:
            case_root = data / case['id']
            guard = c.read(data / 'logs' / (case['id'] + '.guard.json'))
            assert guard['status'] == 'passed' and guard['returncode'] == 0 and guard['process_group_drained'] is True
            assert c.sha(data / 'logs' / (case['id'] + '.log')) == guard['log_sha256']
            assert guard['source_pre_sha256'] == guard['source_post_sha256'] == freeze['source_sha256']
            assert guard['source_before'] == guard['source_after'] == freeze['source_generation']
            result = retention['read_result'](case_root, guard)
            summary = retention['verify_retired_case'](case_root, result, guard, freeze)
            config = c.read(case_root / 'config.json')
            assert result['status'] == 'passed' and result['config'] == config and config['case'] == case
            assert result['fallback_attempted'] is False and result['external_engine_invoked'] is False
            assert result['native_limit_bytes'] == 2*GIB and 0 < result['native_peak_bytes'] <= 2*GIB
            assert result['native_final_bytes'] == result['native_denied_reservations'] == result['owned_staging_remaining'] == 0
            geometry = config['expected_geometry']
            assert summary['complete_ordered_rows'] == geometry['rows'] and len(geometry['columns']) == 112
            assert {k: v for k, v in result['geometry'].items() if k not in ('parallelism', 'source_task_window', 'source_policy')} == geometry
            namespace['verify_activation'](result, case['cell']['parallelism'], len(geometry['batch_rows']))
            metric = metrics.get(case['id'])
            if metric is not None:
                assert metric['case'] == case and metric['activation_verified'] is True
                assert metric['complete_ingest_nanos'] == result['complete_ingest_nanos']
                assert metric['artifact_sha256'] == result['artifact_sha256']
            if case['role'] == 'observed':
                observed += 1
                row = trace_rows[ATTEMPT + '/' + case['id'] + '/trace.json.gz']
                trace = guard['trace_compaction']
                assert trace['verified'] is True and trace['retained_sha256'] == row['sha256'] and trace['retained_bytes'] == row['bytes']
                observation = result['observation']; drivers = observation['admitted_drivers']
                assert observation['observer_implementation'] == 'outermost_poll_ticketed_events_v3' and observation['drained'] is True
                assert drivers == case['cell']['parallelism'] and len(observation['children']) == len(geometry['batch_rows'])
                opportunity = sum((drivers - occupied)*duration for child in observation['children']
                                  for occupied, duration in enumerate(child['ready_successor_cpu_queue_empty_wall_nanos']))
                assert 0 <= opportunity <= drivers*result['complete_ingest_nanos']
                if drivers == 1 or len(geometry['batch_rows']) == 1: assert opportunity == 0
                if metric is not None:
                    assert metric['opportunity_driver_nanos'] == opportunity
                    assert metric['denominator_driver_nanos'] == drivers*result['complete_ingest_nanos']
                    assert observation['polls']['outer_pairs'] == metric['event_counts']['poll_enter'] == metric['event_counts']['poll_exit']
            else:
                assert result['observation'] is None and result['observer_start_nanos'] is None and result['observer_end_nanos'] is None
        assert observed == 49
    recomputed = namespace['decide'](decision['metrics'], protocol)
    assert all(decision[key] == value for key, value in recomputed.items())
    assert recomputed['outcome'] == payload['current_outcome'] == index['current_outcome']
    assert recomputed['candidate_design_admitted'] == payload['candidate_design_admitted'] == index['candidate_design_admitted']
    result = dict(status='passed', packet_sha256=index['sha256'], members_verified=len(members),
        source_reconstructed=source_result, compiled_inputs_verified=len(compiled), native_packages=11,
        prerequisite_receipts_verified=len(checks), workflow_records_verified=98,
        measured_records_recomputed=84, raw_event_trace_identities_indexed=49,
        raw_event_traces_replayed=False, physical_artifacts_reopened=False,
        original_member_files_required=False, original_working_tree_changes_required=False,
        temporary_sources_removed=True, current_outcome=recomputed['outcome'],
        candidate_design_admitted=recomputed['candidate_design_admitted'], scheduling_change_retained=False)
    with args.output.open('x') as stream:
        json.dump(result, stream, indent=2, sort_keys=True); stream.write('\n')
    print(json.dumps(result, sort_keys=True), flush=True)


if __name__ == '__main__':
    main()
