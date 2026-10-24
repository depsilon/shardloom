#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Replay G2, rejected G3 and final Full43; never execute archived helpers or an engine."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import importlib.util
import io
import json
import math
from pathlib import Path
import statistics
import tarfile


def open_packet(root):
    manifest = json.loads((root / 'manifest.json').read_bytes())
    shared = root.parent / 'verify_packet.py'
    assert hashlib.sha256(shared.read_bytes()).hexdigest() == manifest['shared_verifier']['sha256']
    spec = importlib.util.spec_from_file_location('grouped_preparation_shared_verifier', shared)
    core = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(core)
    packet = core.Packet(root)
    assert core.digest(Path(__file__).read_bytes()) == packet.manifest['verifier_sha256']
    assert packet.data(packet.manifest['shared_verifier']) == shared.read_bytes()
    return packet, core


def checked_build(packet, ref, revision, commit):
    build = packet.read(ref)
    assert build['status'] == 'passed' and build['returncode'] == 0
    assert build['process_group_drained'] and build['direct_child_reaped']
    assert build['source_before'] == build['source_after']
    assert build['command'] == ['cargo', 'build', '--release', '-p', 'shardloom-cli', '--bin', 'shardloom', '--features', 'release-user-surfaces']
    log = packet.data(dict(path=str(Path(ref['path']).with_suffix('.log')), sha256=build['log_sha256']))
    assert b'Compiling shardloom-vortex ' in log and b'Compiling shardloom-cli ' in log
    assert revision['commit'] == commit and revision['all_build_source_hashes_match']
    assert len(build['source_after']) == revision['tracked_runtime_files'] + len(revision['ignored_vendor_lockfiles'])
    assert all(build['source_after'][path] == sha for path, sha in revision['ignored_vendor_lockfiles'].items())
    return build


def cell_gates(rows, target):
    roles = {role: sorted([row for row in rows if row['role'] == role], key=lambda row: row['run'])
             for role in ('control', 'candidate')}
    def values(role, field):
        if field == 'cpu':
            result = [row['user_cpu_seconds'] + row['system_cpu_seconds'] for row in roles[role]]
        else:
            result = [row[field] for row in roles[role]]
        assert all(type(value) in (int, float) and math.isfinite(value) and value >= 0 for value in result)
        return result
    engine = {role: values(role, 'engine_execution_seconds') for role in roles}
    medians = {role: statistics.median(value) for role, value in engine.items()}
    cpu = {role: statistics.median(values(role, 'cpu')) for role in roles}
    rss = {role: statistics.median(values(role, 'native_peak_rss_bytes')) for role in roles}
    savings = [b - c for b, c in zip(engine['control'], engine['candidate'], strict=True)]
    benefit = (medians['candidate'] < medians['control'] and statistics.median(savings) > 0
               and len(savings) == 5 and sum(value > 0 for value in savings) >= 4)
    return dict(engine_gate=benefit if target else medians['candidate'] <= medians['control'] + max(.05 * medians['control'], .005),
                cpu_gate=cpu['candidate'] <= 1.05 * cpu['control'] if target else None,
                rss_gate=rss['candidate'] <= rss['control'] + max(.25 * rss['control'], 256 * (1 << 20)),
                engine_medians=medians, cpu_medians=cpu, rss_medians=rss,
                paired_savings_seconds=savings, positive_pairs=sum(value > 0 for value in savings))


def comparison(packet, core, item):
    candidate_id = item['id']
    protocols = {
        'g2': [('target', [28], 12, 5), ('single', [28], 1, 3), ('controls', [17, 19, 23, 29, 34, 35], 12, 3)],
        'g3': [('target', [23], 12, 5), ('single', [23], 1, 3), ('controls', [17, 19, 22, 28, 29, 34, 35], 12, 3)],
    }
    expected_calls = {'g2': 52, 'g3': 58}[candidate_id]
    activation_key = {'g2': 'wider_block_activation_tests', 'g3': 'projected_text_reuse_activation'}[candidate_id]
    plan, outcome, decision = [packet.read(item[key]) for key in ('plan', 'outcome', 'decision')]
    assert decision['source_commit'] == plan['source_commit'] and decision['source_tree'] == plan['source_tree']
    assert decision['baseline_commit'] == plan['baseline_commit'] and decision['binaries'] == plan['binary']
    assert decision['plan'] == outcome['plan'] == item['plan'] and decision['outcome'] == item['outcome']
    assert plan['expected_calls'] == outcome['calls'] == decision['calls'] == expected_calls
    assert outcome['status'] == 'passed' and outcome['source_unchanged'] and outcome['input_hashes_verified']
    assert outcome['frozen_identities_verified']
    assert plan['cells'] == [list(cell) for cell in protocols[candidate_id]]
    assert plan['gates'] == {'target_median_improves': True, 'target_paired_median_saving_positive': True,
        'target_positive_pairs_minimum': 4, 'control_relative_allowance': .05,
        'control_absolute_allowance_seconds': .005, 'target_cpu_relative_allowance': .05,
        'rss_relative_allowance': .25, 'rss_absolute_allowance_bytes': 256 * (1 << 20),
        'complete_values_schema_no_fallback': True, activation_key: True}
    builds = {role: checked_build(packet, plan['builds'][role], plan['candidate_revision' if role == 'candidate' else 'baseline_revision'],
                                 plan['source_commit' if role == 'candidate' else 'baseline_commit'])
              for role in ('control', 'candidate')}
    assert plan['binary']['control']['sha256'] != plan['binary']['candidate']['sha256']
    for ref in [*plan['helpers'].values(), *plan['harness'].values(), plan['protocol'], plan['queries'], decision['scorer']]:
        packet.data(ref)
    reference = packet.read(plan['reference'])
    assert reference['independent_correctness_oracle'] is True and reference['queries_sha256'] == plan['queries']['sha256']
    expected = {row['query_id']: row['values'] for row in reference['records']}
    assert len(reference['records']) == 43 and sorted(expected) == list(range(1, 44))
    assert all(row['native_runtime_invoked'] is False and row['baseline_only'] is True for row in reference['records'])
    oracle = packet.read(plan['oracle_acceptance'])
    assert oracle['status'] == 'passed' and oracle['independent_correctness_oracle'] is True
    assert oracle['native_runtime_invoked'] is False and oracle['reference'] == plan['reference']
    activation_test = packet.read(plan['activation_test'])
    assert activation_test['status'] == 'passed' and activation_test['returncode'] == 0
    assert activation_test['process_group_drained'] and activation_test['direct_child_reaped']
    assert activation_test['source_before'] == activation_test['source_after'] == builds['candidate']['source_after']
    activation_log = packet.data(plan['activation_test_log'])
    assert core.digest(activation_log) == activation_test['log_sha256']
    test_count, test_name = (6, b'compact_numeric_block_repeated_counts_keep_having_limits_and_chunk_rebinding') if candidate_id == 'g2' else (
        16, b'prepared_text_scan_reuse_uses_provider_session_memory_without_aggregate_workers')
    assert f'{test_count} passed; 0 failed; 0 ignored'.encode() in activation_log
    assert test_name + b' ... ok' in activation_log
    calls, calculated, target_activation = 0, [], []
    for operation, (label, selected, parallelism, repeats) in zip(outcome['operations'], protocols[candidate_id], strict=True):
        assert operation['label'] == label and operation['status'] == 'passed' and operation['returncode'] == 0
        assert operation['process_group_drained'] and operation['direct_child_reaped']
        summary = packet.read(operation['summary'])
        assert summary['completed_identity_check'] and summary['complete_result_validation']
        assert summary['reference_packet_identity_verified'] and summary['reference_kind'] == 'independent_reference'
        assert summary['reference_packet']['sha256'] == plan['reference']['sha256']
        assert summary['queries_sha256'] == plan['queries']['sha256'] and summary['source_generation'] == plan['input_generation']
        for role in ('control', 'candidate'):
            assert summary['binaries'][role]['sha256'] == plan['binary'][role]['sha256']
        assert summary['selected_query_ids'] == selected and summary['max_parallelism'] == parallelism
        assert summary['repeats'] == repeats and summary['memory_gb'] == 24
        expected_order = [(q, r, role) for q in selected for r in range(1, repeats + 1)
                          for role in (['control', 'candidate'] if (q + r) % 2 == 0 else ['candidate', 'control'])]
        assert [(row['query'], row['run'], row['role']) for row in summary['records']] == expected_order
        contents = {}
        for archived in summary['archives']:
            with tarfile.open(fileobj=io.BytesIO(packet.data(archived)), mode='r:xz') as archive:
                assert archive.getnames() == [member['name'] for member in archived['members']]
                for member in archived['members']:
                    data = archive.extractfile(member['name']).read()
                    assert core.digest(data) == member['sha256'] and len(data) == member['bytes']
                    assert member['name'] not in contents
                    contents[member['name']] = data
        for q in selected:
            rows, schemas = [], []
            for record in [row for row in summary['records'] if row['query'] == q]:
                prefix = f'q{q:02d}_run{record["run"]}_{record["role"]}'
                schema, _, envelope = core.result(contents[prefix + '.stdout.json'], expected[q], record)
                schemas.append(schema)
                assert record['command'][0] == plan['binary'][record['role']]['path']
                assert record['command'][record['command'].index('--sql') + 1] == record['executed_query_statement']
                timing = core.strict(contents[prefix + '.timing.json'])
                assert timing['returncode'] == 0 and timing['seconds'] == record['seconds']
                assert timing['peak_rss_bytes'] == record['native_peak_rss_bytes']
                for key in ('user_cpu_seconds', 'system_cpu_seconds'):
                    assert timing[key] == record[key]
                if record['role'] == 'candidate' and (q == 19 or (candidate_id == 'g3' and q == 23 and label == 'target')):
                    line, = [line for line in envelope['human_text'].splitlines() if line.startswith('result summary:')]
                    details = core.strict(line.split(' values=', 1)[1])
                    if q == 19:
                        assert details['aggregate_workers_triple_worker_routed_chunks'] == details['aggregate_workers_submitted_chunks'] > 0
                        assert details['aggregate_workers_triple_retained_input_peak_bytes'] > 0
                    else:
                        target_activation.append(all(type(details.get(key)) is int and details[key] > 0 for key in (
                            'aggregate_text_scan_decoded_chunks', 'aggregate_text_scan_projected_reuses')))
                rows.append(record)
                calls += 1
            assert all(schema == schemas[0] for schema in schemas)
            gates = cell_gates(rows, label == 'target')
            recorded, = [cell for cell in decision['cells'] if cell['case'] == label and cell['query'] == q]
            assert all(recorded[key] == value for key, value in gates.items())
            calculated.append((label, q, gates))
    assert calls == expected_calls and len(calculated) == len(decision['cells'])
    target, = [gates for label, _, gates in calculated if label == 'target']
    derived = dict(target_engine=target['engine_gate'], target_cpu=target['cpu_gate'],
        controls=all(gates['engine_gate'] for label, _, gates in calculated if label != 'target'),
        rss=all(gates['rss_gate'] for _, _, gates in calculated), complete_values_schemas_no_fallback=True,
        **{activation_key: True if candidate_id == 'g2' else len(target_activation) == 5 and all(target_activation)})
    assert decision['gates'] == derived and decision['retained'] is all(derived.values())
    assert decision['status'] == ('retained' if all(derived.values()) else 'dropped')
    assert decision['retained'] is (candidate_id == 'g2')
    return dict(plan=plan, decision=decision, builds=builds, expected=expected,
                summary=dict(id=candidate_id, calls=calls, status=decision['status'], gates=derived,
                             target_engine_medians=target['engine_medians']))


def verify(packet, core):
    manifest = packet.manifest
    assert manifest['schema'] == 'shardloom.grouped_preparation.followup.packet.v1'
    assert [item['id'] for item in manifest['comparisons']] == ['g2', 'g3']
    checked = {item['id']: comparison(packet, core, item) for item in manifest['comparisons']}
    assert checked['g3']['plan']['binary']['control'] == checked['g2']['plan']['binary']['candidate']
    assert checked['g3']['plan']['builds']['control'] == checked['g2']['plan']['builds']['candidate']
    retained = checked['g2']
    plan, build, expected = retained['plan'], retained['builds']['candidate'], retained['expected']
    restored = packet.read(manifest['restoration'])
    assert restored['status'] == 'passed' and restored['source_equals_retained_g2'] is True
    assert restored['retained_binary'] == plan['binary']['candidate'] and restored['build'] == plan['builds']['candidate']
    assert restored['decision'] == manifest['comparisons'][1]['decision']
    full_plan, full = [packet.read(manifest[key]) for key in ('full43_plan', 'full43_receipt')]
    assert full['status'] == 'passed' and full['process_group_drained'] and full['direct_child_reaped']
    assert full['plan'] == manifest['full43_plan']
    assert full['input_hashes_verified'] and full['source_unchanged'] and full['schemas_verified'] and full['uat_lock_released']
    assert full_plan['retained_candidate_id'] == 'g2' and full_plan['comparison_source_commit'] == plan['source_commit']
    assert full_plan['binary'] == plan['binary']['candidate'] and full_plan['build'] == plan['builds']['candidate']
    assert full_plan['decision'] == manifest['comparisons'][0]['decision']
    assert full_plan['source_commit'] == manifest['source_commit'] and full_plan['source_tree'] == manifest['source_tree']
    assert full_plan['source_revision'] == restored['source_revision']
    checked_build(packet, full_plan['build'], full_plan['source_revision'], full_plan['source_commit'])
    for key in ('input', 'input_generation', 'reference', 'queries', 'harness'):
        assert full_plan[key] == plan[key]
    assert full_plan['validation'] == manifest['validation']
    assert full_plan['query_ids'] == list(range(1, 44)) and full_plan['repeats'] == 1
    assert full_plan['expected_calls'] == full['queries'] == 43 and full['executions_per_query'] == 1
    for key in ('helper', 'common_helper', 'supervisor', 'workload_guard'):
        packet.data(full_plan[key])
    summary = packet.read(full['summary'])
    packet.data(full['query_timings'])
    assert summary['binary_sha256'] == plan['binary']['candidate']['sha256']
    assert summary['build_commit'] == full_plan['source_commit']
    assert summary['queries_sha256'] == plan['queries']['sha256'] and summary['reference_packet']['sha256'] == plan['reference']['sha256']
    assert summary['complete'] and summary['full_result_validation'] and summary['completed_identity_check']
    assert summary['harness_identity_verified'] and summary['reference_packet_identity_verified']
    assert summary['repeats'] == 1 and summary['score_scope'] == 'full_43'
    assert [(row['query'], row['run']) for row in summary['records']] == [(q, 1) for q in range(1, 44)]
    prior = packet.read(full_plan['prior_summary'])
    assert [(row['query'], row['run']) for row in prior['records']] == [(q, 1) for q in range(1, 44)]
    for record in prior['records']:
        archived = full_plan['schema_sources'][str(record['query'])]
        raw = gzip.decompress(packet.data(dict(path=archived['path'], sha256=archived['gzip_sha256'], bytes=archived['gzip_bytes'])))
        assert core.digest(raw) == archived['raw_sha256'] and len(raw) == archived['raw_bytes']
        schema, _, _ = core.result(raw, expected[record['query']], record)
        assert schema == full_plan['expected_schemas'][str(record['query'])]
    rows, total = 0, 0.0
    for record in summary['records']:
        archived, = record['completed_log_archives']
        raw = gzip.decompress(packet.data(dict(path=archived['path'], sha256=archived['gzip_sha256'], bytes=archived['gzip_bytes'])))
        assert core.digest(raw) == archived['raw_sha256'] and len(raw) == archived['raw_bytes']
        schema, count, _ = core.result(raw, expected[record['query']], record)
        assert schema == full_plan['expected_schemas'][str(record['query'])]
        rows += count
        total += record['engine_execution_seconds']
    assert rows == full['result_rows'] == 381
    assert total == full['engine_execution_seconds'] == summary['timing_totals']['totals']['engine_execution_seconds']
    validation = packet.read(manifest['validation'])
    assert validation['status'] == 'passed'
    commands = [
        ['cargo', 'fmt', '--all', '--', '--check'],
        ['cargo', 'clippy', '--workspace', '--all-targets', '--', '-D', 'warnings'],
        ['cargo', 'test', '--workspace', '--all-targets'],
        ['cargo', 'test', '--workspace', '--all-targets', '--features', 'shardloom-vortex/release-user-surfaces,shardloom-cli/release-user-surfaces'],
    ]
    for stage, command in zip(validation['stages'], commands, strict=True):
        receipt = packet.read(stage['receipt'])
        assert receipt['status'] == 'passed' and receipt['returncode'] == 0 and receipt['command'] == command
        assert receipt['process_group_drained'] and receipt['direct_child_reaped']
        assert receipt['source_before'] == receipt['source_after'] == build['source_after']
        packet.data(dict(path=str(Path(stage['receipt']['path']).with_suffix('.log')), sha256=receipt['log_sha256']))
    native_clippy = packet.read(full_plan['native_clippy'])
    assert native_clippy['status'] == 'passed' and native_clippy['returncode'] == 0
    assert native_clippy['process_group_drained'] and native_clippy['direct_child_reaped']
    assert native_clippy['source_before'] == native_clippy['source_after'] == build['source_after']
    assert native_clippy['command'] == ['cargo', 'clippy', '--workspace', '--all-targets', '--features',
        'shardloom-vortex/release-user-surfaces,shardloom-cli/release-user-surfaces', '--', '-D', 'warnings']
    packet.data(dict(path=str(Path(full_plan['native_clippy']['path']).with_suffix('.log')), sha256=native_clippy['log_sha256']))
    return dict(status='passed', comparisons=[checked[key]['summary'] for key in ('g2', 'g3')],
                full43_queries=43, full43_rows=rows, full43_engine_seconds=total,
                files_verified=len(packet.files), engine_executed=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parent)
    args = parser.parse_args()
    packet, core = open_packet(args.root)
    print(json.dumps(verify(packet, core), sort_keys=True))
