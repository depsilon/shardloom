#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Replay preserved parity evidence without executing an engine or archived code."""
from __future__ import annotations

import argparse
import copy
import gzip
import hashlib
import io
import json
import math
from pathlib import Path
import statistics
import sys
import tarfile

ROLES = ['cli_sql', 'embedded_workflow', 'python_sql', 'dataframe', 'native_expression']
FOCUSED = [1, 17, 19, 20, 23, 28, 29, 34, 35]
COMPARISONS = ['embedded_workflow_vs_python_sql', 'embedded_workflow_vs_dataframe',
               'embedded_workflow_vs_native_expression', 'cli_sql_vs_embedded_workflow',
               'accepted_cli_vs_candidate_cli']
REFERENCE_SHA = 'fc5780da415dc33bd06ed0879db07fc040db5dd419e3c37ecde7cd40a315696b'
GATES = ['parity-r4-workspace-native-tests', 'parity-r4-workspace-native-lint-reviewed',
         'parity-r5-format-corrected', 'parity-r5-native-cli-lint-corrected',
         'parity-r5-native-cli-tests', 'parity-r5-default-lint', 'parity-r5-default-tests',
         'parity-r5-matched-release-build', 'parity-r5-public-provider-tests',
         'parity-r5-small-fixtures', 'parity-r5-preparation-preflight', 'parity-r5-adapter-selftest',
         'parity-r5-scope-execution', 'parity-pr-runner-tests', 'parity-pr-user-reference', 'parity-pr-contribution']
# These standalone-test lockfiles were generated locally and were not part of
# the measured root workspace build or its Git source snapshot. Keep their
# exact recorded identities in the packet, but do not require local copies when
# checking committed runtime sources. In particular, the root lockfile is checked.
GENERATED_TEST_LOCKFILES = {
    'vendor/vortex-file/Cargo.lock': 'e4594b8110c13a4cbef53fb7634b166f551df33835a9b23b48fa8134f2eb5a27',
    'vendor/vortex-layout/Cargo.lock': 'ba66d545212ba1a9bb939a8f66ed05af11ed7ae1adf44ee32cdfacf491c7c1ef',
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


class Packet:
    def __init__(self, root, strict):
        self.strict = strict
        self.manifest = strict((root / 'manifest.json').read_text())
        m = self.manifest
        require(m['schema'] == 'shardloom.native_surface_parity.packet.v1', 'packet schema')
        require(m['archive'] == 'evidence.tar.xz', 'archive filename')
        require(digest(Path(__file__).read_bytes()) == m['verifier_sha256'], 'verifier identity')
        packed = (root / m['archive']).read_bytes()
        require(len(packed) == m['archive_bytes'] and digest(packed) == m['archive_sha256'], 'archive identity')
        self.files = {}
        with tarfile.open(fileobj=io.BytesIO(packed), mode='r:xz') as archive:
            require(archive.getnames() == [row['member'] for row in m['files']], 'archive coverage')
            for row in m['files']:
                member = archive.getmember(row['member'])
                require(member.isfile() and member.size == row['bytes'], 'archive member type/size')
                raw = archive.extractfile(member).read()
                require(digest(raw) == row['sha256'], 'archive member hash')
                key = row['path'], row['sha256']
                require(key not in self.files, 'duplicate member identity')
                self.files[key] = raw

    def data(self, ref):
        key = ref['path'], ref['sha256']
        require(key in self.files, 'missing exact evidence member: ' + ref['path'])
        raw = self.files[key]
        require('bytes' not in ref or len(raw) == ref['bytes'], 'referenced member size')
        return raw

    def read(self, ref):
        raw = self.data(ref)
        if ref['path'].endswith('.gz'):
            raw = gzip.decompress(raw)
        return self.strict(raw.decode())

    def by_path(self, path):
        matches = [dict(path=p, sha256=s) for p, s in self.files if p == str(path)]
        require(len(matches) == 1, 'ambiguous or missing evidence path: ' + str(path))
        return matches[0]


def result(envelope, expected, record, extract, fields_of, equivalent, strict):
    values, fields = extract(envelope), fields_of(envelope)
    require(equivalent(values, expected), 'result values differ from independent reference')
    require(record['passed'] and record['returncode'] == 0
            and record['validation'] == 'complete_values' and not record['guard_failures'], 'query completion')
    require(digest(json.dumps(values, sort_keys=True, allow_nan=False).encode())
            == record['result_sha256'], 'normalized result identity')
    require(fields['native_query_timing_reported'] == 'true'
            and fields['native_query_timing_clock'] == 'monotonic_wall_not_cpu', 'engine timing declaration')
    require(fields['native_query_timing_boundary'] ==
            'prepared_native_execution_including_io_generation_validation_and_certification_excluding_result_serialization',
            'engine timing boundary')
    nanos = fields['native_query_execution_nanos']
    require(isinstance(nanos, str) and nanos.isascii() and nanos.isdecimal()
            and int(nanos) <= (1 << 64) - 1, 'engine nanoseconds')
    seconds = int(nanos) / 1e9
    require(record['engine_timing_reported'] and seconds == record['engine_execution_seconds'], 'engine clock mismatch')
    require(seconds + int(fields['native_result_serialization_nanos']) / 1e9
            <= record['seconds'] + 1e-6, 'engine clock exceeds operation')
    for key, value in record.get('execution_report', {}).items():
        require(fields.get(key) == value, 'execution report mismatch: ' + key)
    return strict(fields['result_schema_json']), len(values)


def checked_members(packet, ref):
    packed = packet.data(ref)
    members = {}
    with tarfile.open(fileobj=io.BytesIO(packed), mode='r:xz') as archive:
        require(archive.getnames() == [row['name'] for row in ref['members']], 'paired archive coverage')
        for row in ref['members']:
            item = archive.getmember(row['name'])
            require(item.isfile() and item.size == row['bytes'], 'paired archive member type/size')
            raw = archive.extractfile(item).read()
            require(digest(raw) == row['sha256'], 'paired archive member hash')
            require(row['name'] not in members, 'duplicate paired archive member')
            members[row['name']] = raw
    return members


def median_screen(records, accepted_control):
    cells = []
    for query in FOCUSED:
        values = {role: [r['engine_execution_seconds'] for r in records
                         if r['query'] == query and r['role'] == role]
                  for role in ('control', 'candidate')}
        require(all(len(v) == 3 and all(type(x) in (int, float) and math.isfinite(x) and x >= 0
                                       for x in v) for v in values.values()), 'paired engine scope')
        control, candidate = (statistics.median(values[role]) for role in ('control', 'candidate'))
        allowance = max(.1 * (control if accepted_control else min(control, candidate)), .0001)
        difference = candidate - control
        cells.append(dict(query=query, control_median_seconds=control,
                          candidate_median_seconds=candidate, candidate_minus_control_seconds=difference,
                          allowance_seconds=allowance,
                          within_control_allowance=(difference <= allowance if accepted_control
                                                    else abs(difference) <= allowance)))
    return cells


def source_manifest(build):
    return digest(json.dumps({**build['source_after'], **build['runtime_python_sources']},
                             sort_keys=True, separators=(',', ':')).encode())


def check_harness(packet, summary):
    for path, sha in summary['harness_sha256'].items():
        packet.data(dict(path=path, sha256=sha))


def check_current_source(build, repository):
    sources = {**build['source_after'], **build['runtime_python_sources']}
    require(all(sources.get(path) == sha for path, sha in GENERATED_TEST_LOCKFILES.items()),
            'recorded generated-test lockfile identities differ')
    for path, sha in sources.items():
        if path in GENERATED_TEST_LOCKFILES:
            continue
        source = repository / path
        require(source.is_file(), 'current runtime source is missing: ' + path)
        require(digest(source.read_bytes()) == sha, 'current runtime source differs: ' + path)


def verify(packet, repository, current, api):
    extract, fields_of, equivalent, strict = api
    m = packet.manifest
    reference = packet.read(m['reference'])
    require(m['reference']['sha256'] == REFERENCE_SHA and reference['independent_correctness_oracle'], 'independent reference')
    require(reference['queries_sha256'] == digest(packet.data(m['queries'])), 'query identity')
    expected = {row['query_id']: row['values'] for row in reference['records']}
    require(sorted(expected) == list(range(1, 44)), 'reference scope')
    require(all(row['baseline_only'] and row['native_runtime_invoked'] is False
                for row in reference['records']), 'reference independence')
    build = packet.read(m['build'])
    require(build['status'] == 'passed' and build['source_before'] == build['source_after']
            and build['package_feature_union'] == 'release-user-surfaces', 'matched final build')
    packet.data(m['source_patch'])
    if current:
        check_current_source(build, repository)
    require([Path(g['receipt']['path']).stem for g in m['gates']] == GATES, 'required gate coverage')
    for item in m['gates']:
        gate = packet.read(item['receipt'])
        require(gate['status'] == 'passed' and gate['returncode'] == 0
                and gate['process_group_drained'], 'required validation gate')
        require(gate['source_before'] == gate['source_after'], 'validation source changed')
        require(digest(packet.data(item['log'])) == gate['log_sha256'], 'validation log identity')
        name = Path(item['receipt']['path']).stem
        if name == 'parity-r5-scope-execution':
            # Focused execution preceded two lint-only assertions in a separate
            # test module. The final native CLI suite re-executes this case.
            changed = sorted(p for p in build['source_after']
                             if gate['source_after'].get(p) != build['source_after'][p])
            require(changed == ['shardloom-cli/src/sql_aggregate_expression_tests.rs'], 'focused scope proof difference')
        elif not name.startswith('parity-r4-'):
            require(gate['source_after'] == build['source_after'], 'final validation source mismatch')
    native = packet.read(m['native_runtime_gate'])
    changed = sorted(path for path in build['source_after']
                     if native['source_after'].get(path) != build['source_after'][path])
    require(changed == m['native_runtime_scope_difference'], 'native proof scope changed')
    require(all(path.startswith('shardloom-cli/') for path in changed), 'unchanged native runtime proof invalid')

    fixture = packet.read(m['fixture'])
    require(fixture['status'] == 'passed' and fixture['oracle_only']
            and fixture['real_dataset_queries_executed'] == 0, 'fixture completion')
    require(fixture['binary'] == build['artifacts']['cli']
            and fixture['library'] == build['artifacts']['library'], 'fixture build identity')
    require(len(fixture['observations']) == 215, 'fixture scope')
    for role in ROLES:
        rows = [r for r in fixture['observations'] if r['role'] == role]
        require([r['query'] for r in rows] == list(range(1, 44)), 'fixture role coverage')
        require(all(r['status'] == 'passed' and equivalent(r['actual'], r['expected']) for r in rows), 'fixture values')
    for query in range(1, 44):
        schemas = [strict(r['schema']) for r in fixture['observations'] if r['query'] == query]
        require(all(s == schemas[0] for s in schemas), 'fixture schemas')
    preflight = packet.read(m['preflight'])
    require(preflight['native_queries_executed'] == 0 and len(preflight['records']) == 172, 'preparation scope')
    adapter = packet.read(m['adapter_selftest'])
    require(adapter['status'] == 'passed' and len(adapter['observations']) == 5
            and len(adapter['rejected_corruptions']) == 3, 'adapter validation')

    paired_calls, failures, q34_reports = 0, {}, []
    require([r['id'] for r in m['paired_cohorts']] == ['surfaces', 'surfaces-r3', 'surfaces-r4', 'surfaces-r5'], 'paired cohort coverage')
    for cohort in m['paired_cohorts']:
        receipt, plan = packet.read(cohort['receipt']), packet.read(cohort['plan'])
        require(receipt['plan'] == cohort['plan'] and receipt['status'] == 'completed_with_failures', 'preserved paired status')
        require(receipt['source_unchanged'] and receipt['input_hash_verified_before_and_after']
                and receipt['reference_unchanged'] and receipt['helper_identities_unchanged']
                and receipt['cohort_lock_released'], 'paired cohort identity/cleanup')
        require(plan['reference'] == m['reference'] and plan['queries'] == m['queries'], 'paired reference identity')
        require(plan['focused_query_ids'] == FOCUSED and plan['repeats'] == 3
                and plan['expected_calls_total'] == 270, 'paired protocol')
        cohort_build = packet.read(plan['build'])
        require(cohort_build['status'] == 'passed' and cohort_build['source_before'] == cohort_build['source_after']
                and source_manifest(cohort_build) == plan['source_manifest_sha256'], 'paired build source binding')
        for ref in plan['helpers'] + [plan['supervisor'], plan['workload_guard']]:
            packet.data(ref)
        require([c['name'] for c in receipt['comparisons']] == COMPARISONS, 'comparison coverage')
        failed = []
        for comparison, declared in zip(receipt['comparisons'], plan['comparisons'], strict=True):
            require(comparison['name'] == declared['name'], 'declared comparison identity')
            require(comparison['status'] == 'passed' and comparison['returncode'] == 0
                    and comparison['process_group_drained'] and comparison['direct_child_reaped']
                    and comparison['uat_lock_released'] and comparison['exact_alternating_schedule'], 'paired operation completion')
            summary = packet.read(comparison['summary'])
            require(summary['completed_identity_check'] and summary['complete_result_validation']
                    and summary['reference_packet_identity_verified'], 'paired result/identity checks')
            require(summary['reference_packet']['sha256'] == REFERENCE_SHA
                    and summary['queries_sha256'] == m['queries']['sha256'], 'paired input contract')
            require(summary['source_generation'] == plan['input_generation']
                    and summary['memory_gb'] == 24 and summary['max_parallelism'] == 12, 'paired source/resources')
            for role in ('control', 'candidate'):
                require(summary['binaries'][role]['sha256'] == declared[role + '_binary']['sha256'], 'paired executable binding')
            check_harness(packet, summary)
            schedule = [(query, run, role, position + 1) for query in FOCUSED
                        for run in range(1, 4)
                        for position, role in enumerate(('control', 'candidate') if (query + run) % 2 == 0
                                                        else ('candidate', 'control'))]
            records = summary['records']
            if cohort['id'] == 'surfaces-r5' and comparison['name'] == 'embedded_workflow_vs_dataframe':
                q34_reports = [r['execution_report'] for r in records if r['query'] == 34]
            require([(r['query'], r['run'], r['role'], r['pair_position']) for r in records] == schedule, 'paired schedule')
            require(len(summary['archives']) == 9, 'paired archive scope')
            raw_outputs = {}
            for archive in summary['archives']:
                raw_outputs.update(checked_members(packet, archive))
            pair_schemas = {query: [] for query in FOCUSED}
            for record in records:
                name = f"q{record['query']:02d}_run{record['run']}_{record['role']}.stdout.json"
                schema, _ = result(strict(raw_outputs[name].decode()), expected[record['query']], record, *api)
                pair_schemas[record['query']].append(schema)
                paired_calls += 1
            require(all(len(v) == 6 and all(s == v[0] for s in v) for v in pair_schemas.values()), 'paired schemas')
            cells = median_screen(records, comparison['name'] == 'accepted_cli_vs_candidate_cli')
            screen = comparison['tolerance_screen']
            require(screen['available'] and cells == screen['per_query'], 'recomputed paired timing screen')
            require(screen['all_within_allowance'] == all(c['within_control_allowance'] for c in cells), 'paired screen conclusion')
            failed.extend(dict(comparison=comparison['name'], **c) for c in cells if not c['within_control_allowance'])
        require(failed and all(c['comparison'] != 'accepted_cli_vs_candidate_cli' for c in failed), 'paired failure/control boundary')
        failures[cohort['id']] = failed
    require(paired_calls == 1080, 'complete paired result scope')

    decision = packet.read(m['attribution'])
    require(decision['paired_timing_screen_passed'] is False and decision['full43_cannot_override_failed_timing_screen']
            and decision['original_threshold_unchanged'] and decision['failed_samples_retained'], 'finite decision claim boundary')
    require(decision['failed_timing_cells'] == failures['surfaces-r5']
            and len(failures['surfaces-r5']) == 1 and failures['surfaces-r5'][0]['query'] == 34, 'final timing limitation')
    require(decision['pairs_receipt'] == m['paired_cohorts'][-1]['receipt'], 'decision cohort binding')
    require(len(q34_reports) == 6 and all(len(r) == 298 for r in q34_reports), 'Q34 report scope')
    keys = set.union(*(set(r) for r in q34_reports))
    varying = sorted(k for k in keys if any(r.get(k) != q34_reports[0].get(k) for r in q34_reports))
    require(len(keys) == 298 and len(keys) - len(varying) == 282
            and varying == decision['q34_work_reports']['varying_fields'], 'Q34 work attribution')
    for evidence in decision['source_evidence']:
        packet.data(evidence['file'])

    receipt, plan = packet.read(m['full43_receipt']), packet.read(m['full43_plan'])
    require(receipt['status'] == 'passed' and receipt['source_unchanged']
            and receipt['input_full_hash_verified_before_and_after'] and receipt['cohort_lock_released'], 'Full43 cohort completion')
    require(receipt['plan'] == m['full43_plan'] and plan['build'] == m['build'], 'Full43 build/plan identity')
    require(plan['paired_attribution'] == receipt['paired_attribution'] == m['attribution']
            and plan['paired_timing_screen_passed'] is False and receipt['paired_timing_screen_passed'] is False,
            'Full43 must preserve failed paired timing screen')
    require(plan['role_order'] == ROLES and plan['executions_per_query_per_role'] == 1
            and plan['no_parallel_task_work'] and plan['no_profiling']
            and plan['no_cache_manipulation'] and plan['no_artificial_pacing'], 'Full43 protocol')
    require(plan['reference'] == m['reference'] and plan['sources_verified'] == len(build['source_after']), 'Full43 source/reference')
    require(plan['source_manifest_sha256'] == source_manifest(build)
            and plan['source_worktree'] == receipt['source_worktree_before'] == receipt['source_worktree_after'], 'Full43 frozen source')
    for ref in plan['helpers'] + [plan['supervisor'], plan['workload_guard']]:
        packet.data(ref)
    require([r['role'] for r in receipt['roles']] == ROLES, 'Full43 role coverage')
    totals, schemas = {}, {q: [] for q in range(1, 44)}
    for role in receipt['roles']:
        require(role['status'] == 'passed' and role['full43_complete'] and role['process_group_drained']
                and role['direct_child_reaped'] and role['uat_lock_released'], 'Full43 role completion')
        summary = packet.read(role['summary'])
        require(summary['complete'] and summary['full_result_validation'] and summary['completed_identity_check']
                and summary['reference_packet_identity_verified'] and summary['harness_identity_verified'], 'Full43 checks')
        require(summary['reference_packet']['sha256'] == REFERENCE_SHA
                and summary['queries_sha256'] == m['queries']['sha256'], 'Full43 reference/query binding')
        require(summary['source_generation'] == plan['input_generation']
                and summary['memory_gb'] == 24 and summary['max_parallelism'] == 12, 'Full43 source/resources')
        executable = (build['artifacts']['cli'] if role['role'] == 'cli_sql' else
                      next(ref for ref in plan['helpers'] if Path(ref['path']).name == role['role']))
        require(summary['binary_sha256'] == executable['sha256'], 'Full43 executable binding')
        check_harness(packet, summary)
        require([(r['query'], r['run']) for r in summary['records']] == [(q, 1) for q in range(1, 44)], 'Full43 calls')
        output_refs = m['full43_outputs'][role['role']]
        require(len(output_refs) == 43, 'Full43 raw output scope')
        times, count = [], 0
        for record, output in zip(summary['records'], output_refs, strict=True):
            schema, rows = result(packet.read(output), expected[record['query']], record, *api)
            schemas[record['query']].append(schema)
            count += rows
            times.append(record['engine_execution_seconds'])
        require(count == 381 and math.isclose(math.fsum(times), role['full43_engine_seconds'], rel_tol=1e-14), 'Full43 result/engine total')
        totals[role['role']] = math.fsum(times)
    require(all(len(v) == 5 and all(s == v[0] for s in v) for v in schemas.values()), 'Full43 equal schemas')
    return dict(status='passed', members=len(packet.files), paired_complete_results=paired_calls,
                paired_failed_cells={k: len(v) for k, v in failures.items()}, paired_timing_screen_passed=False,
                full43_complete_results=215, full43_result_rows=1905, equal_schema_queries=43,
                engine_seconds=totals, final_small_fixture_results=215, engine_executed=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packet-root', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--repository', type=Path, default=Path(__file__).resolve().parents[4])
    parser.add_argument('--check-current-source', action='store_true')
    parser.add_argument('--self-test', action='store_true')
    args = parser.parse_args()
    sys.path.insert(0, str(args.repository / 'scripts'))
    from native_workflow_protocol import extract_result, report_fields, strict_json
    from run_clickbench_query_uat import equivalent
    api = extract_result, report_fields, equivalent, strict_json
    packet = Packet(args.packet_root, strict_json)
    outcome = verify(packet, args.repository, args.check_current_source, api)
    if args.self_test:
        tested = []
        for label, ref, mutate in [
            ('false_timing_pass', packet.manifest['full43_receipt'], lambda d: d.update(paired_timing_screen_passed=True)),
            ('missing_final_call', packet.manifest['full43_receipt'], lambda d: d['roles'].pop()),
            ('changed_fixture_value', packet.manifest['fixture'], lambda d: d['observations'][0].update(actual=[])),
        ]:
            key = ref['path'], ref['sha256']
            original = packet.files[key]
            data = copy.deepcopy(packet.read(ref))
            mutate(data)
            # Bypass byte-integrity checks only here to exercise semantic rejection too.
            packet.files[key] = json.dumps(data).encode()
            original_size = ref.pop('bytes', None)
            try:
                verify(packet, args.repository, False, api)
            except ValueError:
                tested.append(label)
            else:
                raise ValueError('corruption accepted: ' + label)
            finally:
                packet.files[key] = original
                if original_size is not None:
                    ref['bytes'] = original_size
        require(len(tested) == 3, 'corruption controls')
        outcome['rejected_semantic_corruptions'] = tested
    print(json.dumps(outcome, indent=2))


if __name__ == '__main__':
    main()
