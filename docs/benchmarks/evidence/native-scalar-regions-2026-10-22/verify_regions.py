#!/usr/bin/env python3
"""Independently replay the frozen scalar-region experiment from saved bytes.

The packet contains raw outputs, receipts, frozen harness and source records.
No native binary or source dataset is executed or required for this replay.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
from pathlib import Path, PurePosixPath
import re
import sys
import tarfile
import tempfile

PROTOCOL_SHA = '5fdd1589bb3ec01799ba55b2a7f42c759ce84ac39d9a9cba82e1f0d2294507ab'
REFERENCE_SHA = 'fc5780da415dc33bd06ed0879db07fc040db5dd419e3c37ecde7cd40a315696b'
MIB = 1 << 20


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def authenticate_helpers(root):
    # Trust lives in this reviewed verifier, never in the packet's manifest.
    # Bind every file in both import roots, including transitive package imports
    # and unexpected files that could shadow a standard-library module.
    identities = {}
    for relative in ('repo/scripts', 'repo/python/src'):
        base = root / relative
        assert base.is_dir() and not base.is_symlink(), 'archived helper identity mismatch'
        for path in base.rglob('*'):
            assert not path.is_symlink(), 'archived helper identity mismatch'
            if path.is_file():
                identities[path.relative_to(root).as_posix()] = digest(path.read_bytes())
    encoded = json.dumps(identities, sort_keys=True, separators=(',', ':')).encode()
    assert digest(encoded) == 'd24ca1652f75b1f3585a22a5931c465eb83252aa7e472710470beb6b3a52d5c0', 'archived helper identity mismatch'

    # Keep repeated in-process replay from adding unauthenticated .pyc files.
    sys.dont_write_bytecode = True


def exact(actual, expected):
    if type(actual) is not type(expected):
        return False
    if isinstance(actual, dict):
        return actual.keys() == expected.keys() and all(exact(actual[k], expected[k]) for k in actual)
    if isinstance(actual, list):
        return len(actual) == len(expected) and all(exact(a, b) for a, b in zip(actual, expected))
    if isinstance(actual, float):
        return math.isfinite(actual) and actual.hex() == expected.hex()
    return actual == expected


def safe_member(name):
    path = PurePosixPath(name)
    assert not path.is_absolute() and '..' not in path.parts and name != ''
    return path


def verify(root):
    manifest = json.loads((root / 'manifest.json').read_text())
    authenticate_helpers(root)
    assert manifest['schema'] == 'shardloom.scalar_region_experiment_archive.v1'
    mapping = {}
    members = {'manifest.json'}
    for entry in manifest['files']:
        member = safe_member(entry['member']).as_posix()
        assert member not in members and entry['original_path'] not in mapping
        path = root / member
        raw = path.read_bytes()
        assert len(raw) == entry['bytes'] and digest(raw) == entry['sha256'], member
        members.add(member)
        mapping[entry['original_path']] = path
    assert {p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file()} == members

    def bound(reference):
        path = mapping[reference['path']]
        raw = path.read_bytes()
        assert digest(raw) == reference['sha256'], reference['path']
        if 'bytes' in reference:
            assert len(raw) == reference['bytes']
        return path

    # Only the frozen result/schema readers are shared with collection. All call
    # coverage, ordering, timing admission and retain/drop arithmetic below are
    # reconstructed here, independently of the comparison runner's score_stage.
    sys.path.insert(0, str(root / 'repo/scripts'))
    from clickbench_reference_packet import load_reference_packet
    from native_workflow_protocol import extract_result, report_fields, strict_json
    from query_timing_report import query_phase_timings
    from run_clickbench_query_uat import equivalent

    def read(reference):
        return strict_json(bound(reference).read_text())

    protocol_path = bound(manifest['protocol'])
    assert digest(protocol_path.read_bytes()) == PROTOCOL_SHA
    protocol = strict_json(protocol_path.read_text())
    assert protocol['status'] == 'frozen_before_runtime_implementation'
    assert protocol['candidate_observations_collected'] == 0
    for source in protocol['harness_sources'] + protocol['design_sources']:
        bound(source)
    amendment = read(manifest['collection_amendment'])
    assert amendment['protocol'] == manifest['protocol']
    assert amendment['original_driver'] in protocol['harness_sources']
    bound(amendment['original_driver'])
    bound(amendment['corrected_driver'])
    rejected_launch = read(amendment['failed_launch'])
    assert rejected_launch['status'] == 'failed' and rejected_launch['records'] == []
    assert rejected_launch['active_call']['returncode'] == 2 and rejected_launch['active_call']['process_group_drained']
    assert 'unrecognized arguments: --compress-logs' in bound(amendment['failed_driver_log']).read_text()
    assert amendment['native_query_calls_in_failed_launch'] == 0
    assert protocol['full43_reference']['sha256'] == REFERENCE_SHA
    reference = load_reference_packet(bound(protocol['full43_reference']), bound(protocol['full43_queries']))
    assert reference['packet']['reference_kind'] == 'independent_reference'
    heldout = read(protocol['heldout_reference'])
    expected_cases = {c['id']: c for c in heldout['cases']}
    cases = protocol['heldout']['cases']
    assert len(cases) == len(expected_cases) == 13
    runtime = read(manifest['runtime'])
    assert runtime['status'] == 'passed' and runtime['query_calls'] == 0
    assert runtime['protocol'] == manifest['protocol']
    build = read(runtime['build'])
    assert build['status'] == 'passed' and build['returncode'] == 0 and build['process_group_drained']
    assert build['source_before'] == build['source_after']
    assert build['command'] == protocol['candidate_build_command']
    build_plan = read(runtime['build_plan'])
    assert build_plan['source_commit'] == runtime['source_commit']
    assert build_plan['source_tree'] == runtime['source_tree']
    assert build_plan['source'] == build['source_after']
    assert build_plan['protocol'] == manifest['protocol']
    assert runtime['executable']['sha256'] != protocol['baseline']['executable']['sha256']
    assert len(runtime['checks']) == 6 and len(set(runtime['role_distinguishing_unit_tests'])) == 15
    check_logs = {}
    for check_ref in runtime['checks']:
        check = read(check_ref)
        assert check['status'] == 'passed' and check['returncode'] == 0 and check['process_group_drained']
        assert check['source_before'] == check['source_after']
        assert set(check['source_after']) == set(build['source_after'])
        differences = {p for p in build['source_after'] if check['source_after'][p] != build['source_after'][p]}
        if Path(check_ref['path']).name == 'region-native-tests-961.json':
            assert differences == {'shardloom-cli/src/public_workflow_route.rs'}
            assert runtime['validation_reuse']['changed_source_only'] == 'shardloom-cli/src/public_workflow_route.rs'
        else:
            assert differences == set()
        log_path = str(Path(check_ref['path']).with_suffix('.log'))
        assert digest(mapping[log_path].read_bytes()) == check['log_sha256']
        check_logs[Path(check_ref['path']).stem] = mapping[log_path].read_text()
    changed_sources = {p for p, value in build['source_after'].items()
                       if protocol['baseline_runtime_sources'].get(p) != value}
    assert len(changed_sources) == 10
    assert set(protocol['baseline_runtime_sources']) <= set(build['source_after'])
    assert {item['relative_path'] for item in manifest['candidate_sources']} == changed_sources
    test_names = []
    for item in manifest['candidate_sources']:
        path = bound(item['identity'])
        assert digest(path.read_bytes()) == build['source_after'][item['relative_path']]
        if path.name in ('scalar_region_workers_tests.rs', 'scalar_region_workers_lifecycle_tests.rs',
                         'scalar_region_workers_layout_tests.rs'):
            test_names.extend(re.findall(r'#\[test\]\s*fn ([A-Za-z0-9_]+)\(', path.read_text()))
    assert len(test_names) == 15 and set(test_names) == set(runtime['role_distinguishing_unit_tests'])
    for name in test_names:
        assert re.search(r'^test .*::' + re.escape(name) + r' \.\.\. ok$', check_logs['region-native-tests-961'], re.M)
    public_log = next(value for label, value in check_logs.items() if label.startswith('region-public-tests-'))
    assert 'source_region_observations_survive_public_summary_without_invented_counters ... ok' in public_log

    activated = []

    def activation(fields, role, label, requested_parallelism, case=None):
        prefix = 'local_primitive_aggregate_source_region_'
        active = fields.get(prefix + 'execution') in ('true', True)
        if role == 'control':
            assert not active, label
        if case is not None:
            if role == 'candidate' and case['role'] == 'target':
                assert active, label
            if case['id'] in ('c_small', 'c_metadata', 'c_large_integer', 'c_floating', 'c_dictionary', 'c_wide_text'):
                assert not active, label
        if not active:
            return
        def count(name):
            value = fields[prefix + name]
            assert str(value).isascii() and str(value).isdecimal()
            return int(value)
        assert fields[prefix + 'coverage_complete'] in ('true', True)
        assert count('submitted') == count('completed') >= 2
        assert count('outstanding') == 0
        assert int(fields['local_primitive_aggregate_provider_background_workers']) == 0
        parallelism = count('parallelism')
        assert 1 <= parallelism <= requested_parallelism
        assert count('peak_active_workers') <= max(0, parallelism - 1)
        assert count('peak_outstanding') <= 2 * parallelism
        assert count('directory_retained_bytes') <= count('directory_peak_bytes')
        if case is not None:
            dataset = next(d for d in protocol['heldout']['datasets'] if d['id'] == case['dataset'])
            assert count('source_rows') == dataset['rows']
        activated.append(dict(call=label, fields={k: v for k, v in fields.items() if k.startswith(prefix)}))

    def raw_call(row, envelope_raw, timing_raw, expected, role, label, requested_parallelism, case=None):
        envelope = strict_json(envelope_raw.decode())
        fields = report_fields(envelope)
        assert fields['fallback_attempted'] == 'false' and fields['external_engine_invoked'] == 'false'
        actual = extract_result(envelope)
        assert (exact(actual, expected) if case else equivalent(actual, expected)), label
        timing = strict_json(timing_raw.decode())
        assert timing['returncode'] == 0
        phase = query_phase_timings(fields, timing['seconds'])
        assert row['passed'] and row['engine_timing_reported']
        for key, value in phase.items():
            assert row[key] == value, (label, key)
        assert row['native_peak_rss_bytes'] == timing['peak_rss_bytes']
        assert type(timing['peak_rss_bytes']) is int and timing['peak_rss_bytes'] > 0
        if 'result_sha256' in row:
            assert digest(json.dumps(actual, sort_keys=True, allow_nan=False).encode()) == row['result_sha256']
        activation(fields, role, label, requested_parallelism, case)
        return dict(case=row.get('case', row.get('query')), role=role, rows=len(actual),
                    engine_execution_seconds=phase['engine_execution_seconds'],
                    native_peak_rss_bytes=timing['peak_rss_bytes'])

    standard = read(manifest['standard'])
    assert standard['status'] == 'passed' and standard['process_group_drained']
    assert standard['pre_input_hash_verified'] and standard['post_input_hash_verified']
    assert standard['source_unchanged'] and standard['uat_lock_released']
    assert standard['runtime'] == manifest['runtime'] and standard['binary'] == runtime['executable']
    single = read(standard['summary'])
    assert single['complete'] and single['full_result_validation'] and single['completed_identity_check']
    assert single['reference_packet_identity_verified'] and single['harness_identity_verified']
    assert single['selected_query_ids'] == list(range(1, 44)) and single['repeats'] == 1
    assert [(r['query'], r['run']) for r in single['records']] == [(q, 1) for q in range(1, 44)]
    singles = []
    for row in single['records']:
        assert row['returncode'] == 0 and row['guard_failures'] == []
        prefix = Path(standard['summary']['path']).parent / f"q{row['query']:02d}_run1"
        def standard_log(suffix):
            path = str(prefix) + suffix
            if path in mapping:
                return mapping[path].read_bytes()
            return gzip.decompress(mapping[path + '.gz'].read_bytes())
        singles.append(raw_call(row, standard_log('.stdout.json'), standard_log('.timing.json'),
            reference['values'][row['query']], 'candidate', 'standard-q' + str(row['query']), 12))
    assert sum(r['rows'] for r in singles) == standard['result_rows'] == 381
    # Built-in float sum changed in Python 3.12. Reconstruct the recorded
    # compensated totals identically on supported older replay interpreters.
    standard_engine = math.fsum(r['engine_execution_seconds'] for r in singles)
    assert standard_engine == standard['engine_execution_seconds'] == single['timing_totals']['totals']['engine_execution_seconds']

    stage_results = []
    stage_refs = manifest['stages']
    assert list(stage_refs) == ['full43-A', 'full43-B', 'heldout-A']
    closeout = read(manifest['failure_closeout'])
    assert closeout['status'] == 'passed' and closeout['decision'] == 'drop'
    assert closeout['runtime'] == manifest['runtime'] and closeout['protocol'] == manifest['protocol']
    for key in ('all_heldout_fixture_identities_and_generations_unchanged',
                'all_harness_identities_unchanged', 'all_candidate_rust_and_cargo_sources_unchanged',
                'both_executables_unchanged', 'all_launched_process_groups_drained',
                'workload_guard_passed', 'lock_released', 'heldout_b_not_started'):
        assert closeout[key] is True, key
    for name, stage_ref in stage_refs.items():
        stage = read(stage_ref)
        kind, cohort = name.split('-')
        assert stage['stage'] == name
        if kind == 'heldout':
            assert stage['status'] == 'failed' and stage['failure'] == 'AssertionError: excluded control started region execution'
            assert stage_ref == closeout['failed_stage']
            assert len(stage['records']) == 11 and not stage['records'][-1]['passed']
        else:
            assert stage['status'] == 'complete' and stage['identities_unchanged']
        assert stage['protocol'] == manifest['protocol'] and stage['runtime'] == manifest['runtime']
        assert stage['collection_amendment'] == manifest['collection_amendment']
        assert stage['binaries'] == {'control': protocol['baseline']['executable'], 'candidate': runtime['executable']}
        ids = list(range(1, 44)) if kind == 'full43' else [c['id'] for c in cases]
        expected_order = []
        for index, case_id in enumerate(ids, 1):
            roles = ['control', 'candidate'] if (index + 1 + int(cohort == 'B')) % 2 == 0 else ['candidate', 'control']
            expected_order.extend((case_id, role) for role in roles)
        if kind == 'heldout':
            expected_order = expected_order[:11]
        assert [(r['case'], r['role']) for r in stage['records']] == expected_order
        raw_rows = []
        if kind == 'full43':
            summary = read(stage['summary'])
            assert summary['completed_identity_check'] and summary['complete_result_validation'] and summary['reference_packet_identity_verified']
            assert summary['selected_query_ids'] == ids and summary['repeats'] == 1
            assert summary['reverse_order'] == (cohort == 'B')
            assert stage['records'] == [dict(r, case=r['query']) for r in summary['records']]
            assert len(summary['archives']) == 43
            for query, archive_ref in zip(ids, summary['archives']):
                archive_path = bound(archive_ref)
                with tarfile.open(archive_path, 'r:xz') as archive:
                    originals = archive_ref['members']
                    assert archive.getnames() == [m['name'] for m in originals]
                    raw = {}
                    for item in originals:
                        member = archive.getmember(item['name'])
                        assert member.isfile()
                        data = archive.extractfile(member).read()
                        assert len(data) == item['bytes'] and digest(data) == item['sha256']
                        raw[item['name']] = data
                    selected = [r for r in stage['records'] if r['case'] == query]
                    assert len(selected) == 2
                    for row in selected:
                        assert row['run'] == 1 and row['returncode'] == 0 and row['guard_failures'] == []
                        assert row['query_statement'] == reference['queries'][query - 1]
                        prefix = f"q{query:02d}_run1_{row['role']}"
                        raw_rows.append(raw_call(row, raw[prefix + '.stdout.json'], raw[prefix + '.timing.json'],
                            reference['values'][query], row['role'], name + '-' + prefix, 12))
            assert sum(r['rows'] for r in raw_rows) == 762
        else:
            for row in stage['records']:
                case = next(c for c in cases if c['id'] == row['case'])
                policy = case.get('policy', protocol['heldout']['default_policy'])
                assert row['returncode'] == 0 and row['process_group_drained']
                assert not row['timed_out'] and row['interrupted_signal'] is None
                prefix = str(Path(stage_ref['path']).parent / (row['case'] + '-' + row['role']))
                stdout = mapping[prefix + '.stdout.json'].read_bytes()
                timing_path = prefix + '.timing.json'
                timing = mapping[timing_path].read_bytes()
                if not row['passed']:
                    assert (row['case'], row['role']) == ('c_metadata', 'candidate')
                    envelope = strict_json(stdout.decode())
                    fields = report_fields(envelope)
                    assert fields['fallback_attempted'] == 'false' and fields['external_engine_invoked'] == 'false'
                    assert exact(extract_result(envelope), expected_cases['c_metadata']['values'])
                    native_timing = strict_json(timing.decode())
                    assert native_timing['returncode'] == 0
                    query_phase_timings(fields, native_timing['seconds'])
                    assert fields['local_primitive_aggregate_source_region_execution'] == 'true'
                    assert int(fields['local_primitive_aggregate_source_region_submitted']) == 64
                    assert int(fields['local_primitive_aggregate_source_region_completed']) == 64
                    assert int(fields['local_primitive_aggregate_source_region_outstanding']) == 0
                    try:
                        activation(fields, row['role'], name + '-c_metadata-candidate', 4, case)
                    except AssertionError:
                        pass
                    else:
                        raise AssertionError('The mandatory metadata control violation was not rejected')
                    raw_rows.append(dict(case='c_metadata', role='candidate', complete_values_match=True,
                        activation_contract_passed=False, engine_timing_admitted=False))
                    continue
                assert bound(row['stdout']).read_bytes() == stdout
                bound(row['stderr'])
                assert strict_json(timing.decode()) == row['native_timing']
                assert exact(extract_result(strict_json(stdout.decode())), row['actual'])
                assert report_fields(strict_json(stdout.decode())) == row['fields']
                expected = expected_cases[row['case']]
                assert expected['statement'] in row['command']
                raw_rows.append(raw_call(row, stdout, timing, expected['values'], row['role'],
                    name + '-' + row['case'] + '-' + row['role'], policy['requested_parallelism'], case))
        assert [(r['case'], r['role']) for r in raw_rows] == expected_order
        if kind == 'heldout':
            assert len(raw_rows) == 11 and raw_rows[-1]['activation_contract_passed'] is False
            assert 'score' not in stage and 'retention_gate_passed' not in stage
            stage_results.append(dict(stage=name, raw_calls=11, complete_values_checked=11,
                complete_cohort=False, activation_contract_passed=False, performance_score_admitted=False,
                failed_call=dict(case='c_metadata', role='candidate'), passed=False))
            continue
        lookup = {(r['case'], r['role']): r for r in raw_rows}
        targets = ids if kind == 'full43' else [c['id'] for c in cases if c['role'] == 'target']
        totals = {role: math.fsum(lookup[(key, role)]['engine_execution_seconds'] for key in targets) for role in ('control', 'candidate')}
        aggregate_passed = totals['candidate'] <= totals['control'] * (0.98 if kind == 'full43' else 0.80)
        pairs = []
        for key in ids:
            control, candidate = (lookup[(key, role)] for role in ('control', 'candidate'))
            factor, allowance = (1.10, 0.020) if kind == 'full43' else ((0.95, 0.002) if key in targets else (1.05, 0.002))
            pairs.append(dict(case=key, baseline_engine_seconds=control['engine_execution_seconds'],
                candidate_engine_seconds=candidate['engine_execution_seconds'],
                timing_passed=candidate['engine_execution_seconds'] <= control['engine_execution_seconds'] * factor + allowance,
                rss_passed=candidate['native_peak_rss_bytes'] <= control['native_peak_rss_bytes'] * 1.25 + 128 * MIB))
        passed = aggregate_passed and all(p['timing_passed'] and p['rss_passed'] for p in pairs)
        expected_score = dict(kind=kind, raw_calls=len(raw_rows), target_engine_sums=totals,
            aggregate_passed=aggregate_passed, pairs=pairs, passed=passed)
        assert stage['score'] == expected_score and stage['retention_gate_passed'] == passed
        stage_results.append(dict(stage=name, **expected_score))
    calls = len(singles) + sum(s['raw_calls'] for s in stage_results)
    assert calls == manifest['executed_calls'] == closeout['executed_calls'] == 226
    assert manifest['planned_calls'] == closeout['planned_calls'] == 267
    assert manifest['unexecuted_calls'] == closeout['unexecuted_calls'] == 41
    assert manifest['heldout_b_not_started'] and closeout['heldout_b_not_started']
    assert closeout['performance_comparison_admitted'] is False and closeout['activation_contract_passed'] is False
    assert all(not s['passed'] for s in stage_results)
    return dict(status='passed', calls=calls, standard_full43_engine_seconds=standard_engine,
        standard_result_rows=381, stages=stage_results, decision='drop',
        experiment_complete=False, candidate_activation_contract_passed=False,
        performance_comparison_admitted=False, planned_calls=267, unexecuted_calls=41,
        activation=activated, raw_values_and_engine_clocks_replayed=True,
        binary_and_input_bytes_in_archive=False, native_execution_during_replay=False,
        clock_scope='Engine elapsed time only for gates; region consumer nanos include provider/pruning/reduction and can overlap, not exclusive CPU.',
        fallback_attempted=False, external_engine_invoked=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('packet', type=Path)
    args = parser.parse_args()
    if args.packet.is_dir():
        result = verify(args.packet)
    else:
        with tempfile.TemporaryDirectory(prefix='shardloom-region-replay-') as temporary:
            root = Path(temporary)
            with tarfile.open(args.packet, 'r:gz') as archive:
                seen = set()
                for member in archive.getmembers():
                    name = safe_member(member.name).as_posix()
                    assert member.isfile() and name not in seen
                    assert member.size <= 32 * MIB
                    seen.add(name)
                    destination = root / name
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    destination.write_bytes(archive.extractfile(member).read())
            result = verify(root)
    print(json.dumps(result, indent=2, allow_nan=False))


if __name__ == '__main__':
    main()
