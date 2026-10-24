#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Replay compact G1 evidence without executing an engine or requiring local payloads."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import math
from pathlib import Path
import statistics
import tarfile


def digest(data):
    return hashlib.sha256(data).hexdigest()


def strict(data):
    def pairs(values):
        result = {}
        for key, value in values:
            if key in result:
                raise ValueError('duplicate JSON field: ' + key)
            result[key] = value
        return result
    def invalid(value):
        raise ValueError('nonfinite JSON number: ' + value)
    return json.loads(data, object_pairs_hook=pairs, parse_constant=invalid)


def equivalent(actual, expected):
    if type(actual) is not type(expected):
        return False
    if isinstance(actual, dict):
        return actual.keys() == expected.keys() and all(equivalent(actual[k], expected[k]) for k in actual)
    if isinstance(actual, list):
        return len(actual) == len(expected) and all(equivalent(a, b) for a, b in zip(actual, expected))
    if isinstance(actual, float):
        return math.isfinite(actual) and math.isfinite(expected) and math.isclose(actual, expected, rel_tol=1e-12, abs_tol=1e-12)
    return actual == expected


class Packet:
    def __init__(self, root):
        self.manifest = strict((root / 'manifest.json').read_bytes())
        packed = (root / self.manifest['archive']).read_bytes()
        assert len(packed) == self.manifest['archive_bytes']
        assert digest(packed) == self.manifest['archive_sha256']
        self.files = {}
        with tarfile.open(fileobj=io.BytesIO(packed), mode='r:xz') as archive:
            expected = self.manifest['files']
            assert archive.getnames() == [row['member'] for row in expected]
            for row in expected:
                member = archive.getmember(row['member'])
                assert member.isfile() and not member.issym() and not member.islnk()
                data = archive.extractfile(member).read()
                assert len(data) == row['bytes'] and digest(data) == row['sha256']
                key = (row['path'], row['sha256'])
                assert key not in self.files
                self.files[key] = data

    def data(self, ref):
        data = self.files[(ref['path'], ref['sha256'])]
        assert 'bytes' not in ref or len(data) == ref['bytes']
        return data

    def read(self, ref):
        return strict(self.data(ref))


def result(raw, expected, record):
    envelope = strict(raw)
    assert envelope['status'] == 'success'
    fallback = envelope['fallback']
    assert fallback['attempted'] is False and fallback['engine'] is None
    assert fallback.get('allowed', False) is False
    fields = {}
    for field in envelope['fields']:
        assert field['key'] not in fields
        fields[field['key']] = field['value']
    for key in ['public_workflow_fallback_attempted', 'public_workflow_external_engine_invoked']:
        assert fields[key] is False or fields[key] == 'false'
    for key, value in fields.items():
        if key.endswith(('fallback_attempted', 'external_engine_invoked')):
            assert value is False or value == 'false'
    assert fields['result_payload_complete'] is True or fields['result_payload_complete'] == 'true'
    keys = [k for k in ['result_jsonl', 'result_values_json'] if k in fields]
    assert len(keys) == 1
    rows = ([strict(line) for line in fields[keys[0]].splitlines()]
            if keys[0] == 'result_jsonl' else strict(fields[keys[0]]))
    assert isinstance(rows, list) and len(rows) == int(fields['output_row_count'])
    assert equivalent(rows, expected)
    schema = [fields['result_schema_format'], strict(fields['result_schema_json'])]
    assert fields['native_query_timing_reported'] == 'true'
    assert fields['native_query_timing_clock'] == 'monotonic_wall_not_cpu'
    assert fields['native_query_timing_boundary'] == 'prepared_native_execution_including_io_generation_validation_and_certification_excluding_result_serialization'
    nanos = fields['native_query_execution_nanos']
    assert isinstance(nanos, str) and nanos.isascii() and nanos.isdecimal() and len(nanos) <= 20
    assert int(nanos) <= (1 << 64) - 1
    assert record['engine_timing_reported'] and int(nanos) / 1e9 == record['engine_execution_seconds']
    assert int(nanos) / 1e9 + int(fields['native_result_serialization_nanos']) / 1e9 <= record['seconds'] + 1e-6
    assert record['passed'] and record['validation'] == 'complete_values'
    assert record['returncode'] == 0 and not record['guard_failures']
    return schema, len(rows), envelope


def check_cell(rows, target):
    by_role = {role: sorted([r for r in rows if r['role'] == role], key=lambda r: r['run'])
               for role in ['control', 'candidate']}
    med = lambda role, value: statistics.median(value(r) for r in by_role[role])
    engine = lambda r: r['engine_execution_seconds']
    cpu = lambda r: r['user_cpu_seconds'] + r['system_cpu_seconds']
    rss = lambda r: r['native_peak_rss_bytes']
    for row in rows:
        for value in [engine(row), cpu(row), rss(row)]:
            assert type(value) in (float, int) and math.isfinite(value) and value >= 0
    baseline, candidate = med('control', engine), med('candidate', engine)
    savings = [engine(b) - engine(c) for b, c in zip(by_role['control'], by_role['candidate'], strict=True)]
    if target:
        assert len(savings) == 5 and sum(value > 0 for value in savings) >= 4
        assert candidate < baseline and statistics.median(savings) > 0
        assert med('candidate', cpu) <= med('control', cpu) * 1.05
    else:
        assert candidate <= baseline + max(.05 * baseline, .005)
    assert med('candidate', rss) <= med('control', rss) + max(.25 * med('control', rss), 256 * (1 << 20))
    return baseline, candidate


def verify(packet):
    manifest = packet.manifest
    plan = packet.read(manifest['comparison_plan'])
    outcome = packet.read(manifest['comparison_outcome'])
    decision = packet.read(manifest['comparison_decision'])
    assert manifest['source_commit'] == plan['source_commit']
    assert manifest['source_tree'] == plan['source_tree']
    assert decision['retained'] and all(decision['gates'].values())
    assert decision['plan'] == outcome['plan'] == manifest['comparison_plan']
    assert decision['outcome'] == manifest['comparison_outcome']
    assert plan['expected_calls'] == outcome['calls'] == decision['calls'] == 52
    assert outcome['status'] == 'passed' and outcome['source_unchanged'] and outcome['input_hashes_verified']
    assert outcome['frozen_identities_verified']
    assert plan['gates'] == {'target_median_improves': True, 'target_paired_median_saving_positive': True,
        'target_positive_pairs_minimum': 4, 'control_relative_allowance': .05,
        'control_absolute_allowance_seconds': .005, 'target_cpu_relative_allowance': .05,
        'rss_relative_allowance': .25, 'rss_absolute_allowance_bytes': 256 * (1 << 20),
        'complete_values_schema_no_fallback': True, 'positive_target_activation': True}
    builds = {role: packet.read(ref) for role, ref in plan['builds'].items()}
    for role, build in builds.items():
        assert build['status'] == 'passed' and build['returncode'] == 0
        assert build['process_group_drained'] and build['direct_child_reaped']
        assert build['source_before'] == build['source_after']
        assert build['command'] == ['cargo', 'build', '--release', '-p', 'shardloom-cli', '--bin', 'shardloom', '--features', 'release-user-surfaces']
        log = packet.data(dict(path=str(Path(plan['builds'][role]['path']).with_suffix('.log')),
                               sha256=build['log_sha256']))
        assert b'Compiling shardloom-vortex ' in log and b'Compiling shardloom-cli ' in log
        revision = plan['candidate_revision' if role == 'candidate' else 'baseline_revision']
        assert revision['commit'] == plan['source_commit' if role == 'candidate' else 'baseline_commit']
        assert revision['all_build_source_hashes_match']
        assert len(build['source_after']) == revision['tracked_runtime_files'] + len(revision['ignored_vendor_lockfiles'])
        assert all(build['source_after'][path] == sha for path, sha in revision['ignored_vendor_lockfiles'].items())
    assert plan['binary']['control']['sha256'] != plan['binary']['candidate']['sha256']
    for ref in [*plan['helpers'].values(), *plan['harness'].values(), plan['protocol'], plan['queries']]:
        packet.data(ref)
    reference = packet.read(plan['reference'])
    assert reference['independent_correctness_oracle'] is True
    assert reference['queries_sha256'] == plan['queries']['sha256']
    expected = {r['query_id']: r['values'] for r in reference['records']}
    assert len(reference['records']) == 43 and sorted(expected) == list(range(1, 44))
    assert all(r['native_runtime_invoked'] is False and r['baseline_only'] is True for r in reference['records'])
    protocol = [('target', [19], 12, 5), ('single', [19], 1, 3),
                ('controls', [17, 23, 28, 29, 34, 35], 12, 3)]
    assert plan['cells'] == [list(cell) for cell in protocol]
    calls, target_medians = 0, None
    for operation, (label, selected, parallelism, repeats) in zip(outcome['operations'], protocol, strict=True):
        assert operation['status'] == 'passed' and operation['returncode'] == 0
        assert operation['process_group_drained'] and operation['direct_child_reaped']
        summary = packet.read(operation['summary'])
        assert summary['completed_identity_check'] and summary['complete_result_validation']
        assert summary['reference_packet_identity_verified'] and summary['reference_kind'] == 'independent_reference'
        assert summary['reference_packet']['sha256'] == plan['reference']['sha256']
        assert summary['queries_sha256'] == plan['queries']['sha256']
        assert summary['source_generation'] == plan['input_generation']
        for role in ['control', 'candidate']:
            assert summary['binaries'][role]['sha256'] == plan['binary'][role]['sha256']
        assert summary['selected_query_ids'] == selected and summary['max_parallelism'] == parallelism
        assert summary['repeats'] == repeats and summary['memory_gb'] == 24
        expected_order = [(q, r, role) for q in selected for r in range(1, repeats + 1)
                          for role in (['control', 'candidate'] if (q + r) % 2 == 0 else ['candidate', 'control'])]
        assert [(r['query'], r['run'], r['role']) for r in summary['records']] == expected_order
        contents = {}
        for archived in summary['archives']:
            with tarfile.open(fileobj=io.BytesIO(packet.data(archived)), mode='r:xz') as archive:
                assert archive.getnames() == [m['name'] for m in archived['members']]
                for member in archived['members']:
                    data = archive.extractfile(member['name']).read()
                    assert digest(data) == member['sha256'] and len(data) == member['bytes']
                    assert member['name'] not in contents
                    contents[member['name']] = data
        for q in selected:
            rows, schemas = [], []
            for record in [r for r in summary['records'] if r['query'] == q]:
                prefix = f'q{q:02d}_run{record["run"]}_{record["role"]}'
                schema, _, envelope = result(contents[prefix + '.stdout.json'], expected[q], record)
                schemas.append(schema)
                timing = strict(contents[prefix + '.timing.json'])
                assert timing['returncode'] == 0 and timing['seconds'] == record['seconds']
                assert timing['peak_rss_bytes'] == record['native_peak_rss_bytes']
                for key in ['user_cpu_seconds', 'system_cpu_seconds']:
                    assert timing[key] == record[key]
                if q == 19 and record['role'] == 'candidate':
                    line, = [s for s in envelope['human_text'].splitlines() if s.startswith('result summary:')]
                    details = strict(line.split(' values=', 1)[1])
                    assert details['aggregate_workers_triple_worker_routed_chunks'] == details['aggregate_workers_submitted_chunks'] > 0
                    assert details['aggregate_workers_triple_retained_input_peak_bytes'] > 0
                rows.append(record)
                calls += 1
            assert all(schema == schemas[0] for schema in schemas)
            medians = check_cell(rows, label == 'target')
            if label == 'target':
                target_medians = medians
    assert calls == 52
    full_plan = packet.read(manifest['full43_plan'])
    full = packet.read(manifest['full43_receipt'])
    assert full['status'] == 'passed' and full['process_group_drained'] and full['direct_child_reaped']
    assert full['input_hashes_verified'] and full['source_unchanged'] and full['schemas_verified'] and full['uat_lock_released']
    assert full_plan['binary'] == plan['binary']['candidate']
    assert full_plan['source_commit'] == plan['source_commit']
    assert full_plan['source_tree'] == plan['source_tree']
    assert full_plan['build'] == plan['builds']['candidate']
    for key in ['input', 'input_generation', 'reference', 'queries', 'harness']:
        assert full_plan[key] == plan[key]
    assert full_plan['validation'] == manifest['validation']
    assert full_plan['query_ids'] == list(range(1, 44)) and full_plan['repeats'] == 1
    assert full_plan['expected_calls'] == full['queries'] == 43 and full['executions_per_query'] == 1
    assert full_plan['decision'] == manifest['comparison_decision']
    summary = packet.read(full['summary'])
    assert summary['binary_sha256'] == plan['binary']['candidate']['sha256']
    assert summary['build_commit'] == plan['source_commit']
    assert summary['queries_sha256'] == plan['queries']['sha256']
    assert summary['reference_packet']['sha256'] == plan['reference']['sha256']
    assert summary['complete'] and summary['full_result_validation'] and summary['completed_identity_check']
    assert summary['harness_identity_verified'] and summary['reference_packet_identity_verified']
    assert summary['repeats'] == 1 and summary['score_scope'] == 'full_43'
    assert [(r['query'], r['run']) for r in summary['records']] == [(q, 1) for q in range(1, 44)]
    prior = packet.read(full_plan['prior_summary'])
    assert [(r['query'], r['run']) for r in prior['records']] == [(q, 1) for q in range(1, 44)]
    for record in prior['records']:
        archived = full_plan['schema_sources'][str(record['query'])]
        raw = gzip.decompress(packet.data(dict(path=archived['path'], sha256=archived['gzip_sha256'], bytes=archived['gzip_bytes'])))
        assert digest(raw) == archived['raw_sha256']
        schema, _, _ = result(raw, expected[record['query']], record)
        assert schema == full_plan['expected_schemas'][str(record['query'])]
    rows, total = 0, 0.0
    for record in summary['records']:
        archived, = record['completed_log_archives']
        packed = packet.data(dict(path=archived['path'], sha256=archived['gzip_sha256'], bytes=archived['gzip_bytes']))
        raw = gzip.decompress(packed)
        assert digest(raw) == archived['raw_sha256'] and len(raw) == archived['raw_bytes']
        schema, count, _ = result(raw, expected[record['query']], record)
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
        ['cargo', 'test', '--workspace', '--all-targets', '--features',
         'shardloom-vortex/release-user-surfaces,shardloom-cli/release-user-surfaces'],
    ]
    for stage, command in zip(validation['stages'], commands, strict=True):
        receipt = packet.read(stage['receipt'])
        assert receipt['status'] == 'passed' and receipt['returncode'] == 0
        assert receipt['command'] == command
        assert receipt['process_group_drained'] and receipt['source_before'] == receipt['source_after'] == builds['candidate']['source_after']
        packet.data(dict(path=str(Path(stage['receipt']['path']).with_suffix('.log')),
                         sha256=receipt['log_sha256']))
    native_clippy = packet.read(full_plan['native_clippy'])
    assert native_clippy['status'] == 'passed' and native_clippy['source_after'] == builds['candidate']['source_after']
    assert native_clippy['source_before'] == native_clippy['source_after']
    assert native_clippy['returncode'] == 0 and native_clippy['process_group_drained']
    packet.data(dict(path=str(Path(full_plan['native_clippy']['path']).with_suffix('.log')),
                     sha256=native_clippy['log_sha256']))
    return dict(status='passed', comparison_calls=calls, target_engine_medians=target_medians,
                full43_queries=43, full43_rows=rows, full43_engine_seconds=total,
                files_verified=len(packet.files), engine_executed=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parent)
    args = parser.parse_args()
    packet = Packet(args.root)
    assert digest(Path(__file__).read_bytes()) == packet.manifest['verifier_sha256']
    print(json.dumps(verify(packet), sort_keys=True))
