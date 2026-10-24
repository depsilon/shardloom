"""Check retained surface results without running an engine or archived code."""
from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import json
import lzma
import math
from pathlib import Path
import sys


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def verify(review, packet_bytes, repository, current=False):
    sys.path.insert(0, str(repository / 'scripts'))
    from native_workflow_protocol import extract_result, report_fields, strict_json
    from run_clickbench_query_uat import equivalent

    require(review['schema'] == 'shardloom.shared_native_surfaces_acceptance.v1', 'review schema')
    require(sha(packet_bytes) == review['packet']['sha256'], 'packet hash')
    require(len(packet_bytes) == review['packet']['bytes'], 'packet size')
    packet = strict_json(lzma.decompress(packet_bytes).decode())
    require(packet['schema'] == 'shardloom.versioned_result_evidence.v1', 'packet schema')
    members = {}
    for member in packet['files']:
        key = member['path'], member['sha256']
        require(key not in members, 'duplicate member identity')
        require(member['encoding'] in ('utf-8', 'base64'), 'member encoding')
        raw = (member['data'].encode() if member['encoding'] == 'utf-8'
               else base64.b64decode(member['data'], validate=True))
        require(len(raw) == member['bytes'] and sha(raw) == member['sha256'], 'member changed')
        members[key] = raw
    require(len(members) == review['member_count'], 'member count')

    def raw(ref):
        content = members.get((ref['path'], ref['sha256']))
        require(content is not None and len(content) == ref['bytes'], 'missing exact evidence member')
        return content

    def read(ref):
        content = raw(ref)
        if ref['path'].endswith('.gz'):
            content = gzip.decompress(content)
        return strict_json(content.decode())

    reference = read(review['reference'])
    require(review['reference']['sha256'] == 'fc5780da415dc33bd06ed0879db07fc040db5dd419e3c37ecde7cd40a315696b', 'independent reference changed')
    require(reference['queries_sha256'] == sha(raw(review['queries'])), 'query identity')
    expected = {row['query_id']: row['values'] for row in reference['records']}
    require(sorted(expected) == list(range(1, 44)), 'reference scope')
    build = read(review['build'])
    require(build['status'] == 'passed' and build['source_before'] == build['source_after'], 'build source changed')
    require(build['package_feature_union'] == 'release-user-surfaces', 'build features')
    python_sources = read(review['python_sources'])
    require(python_sources['before'] == python_sources['after'], 'Python sources changed during acceptance')
    inventory = read(review['family_inventory'])
    accepted_sources = {**build['source_after'], **python_sources['after']}
    require(inventory['artifact_type'] == 'source inventory; not a test execution result', 'inventory claim boundary')
    for path, digest in inventory['source_hashes'].items():
        require(accepted_sources.get(path) == digest, 'inventory source mismatch: ' + path)
    if current:
        for path, digest in {**build['source_after'], **build['runtime_python_sources'], **python_sources['after']}.items():
            require(sha((repository / path).read_bytes()) == digest, 'current source differs: ' + path)
    expected_gates = ['fmt-1000b', 'clippy-native-1000b', 'clippy-default-1000',
                      'workspace-native-1000', 'workspace-default-1000',
                      'python-construction-1000', 'user-reference-1000b', 'release-build-1000',
                      'python-release-1000', 'fixture-release-1000', 'adapter-release-1000',
                      'preflight-release-1000']
    require([Path(gate['receipt']['path']).name for gate in review['gates']]
            == ['shared-provider-' + name + '.json' for name in expected_gates], 'validation gate scope')
    for gate in review['gates']:
        receipt = read(gate['receipt'])
        require(receipt['status'] == 'passed' and receipt['returncode'] == 0
                and receipt['process_group_drained'], 'validation gate failed')
        require(receipt['source_before'] == receipt['source_after'] == build['source_after'], 'gate source mismatch')
        require(receipt['log_sha256'] == sha(raw(gate['log'])), 'gate log mismatch')

    fixture = read(review['fixture'])
    require(fixture['status'] == 'passed' and fixture['oracle_only']
            and fixture['real_dataset_queries_executed'] == 0, 'small fixture status')
    require(fixture['binary'] == build['artifacts']['cli']
            and fixture['library'] == build['artifacts']['library'], 'fixture artifact mismatch')
    require(len(fixture['observations']) == 215, 'small fixture scope')
    roles = ['cli_sql', 'embedded_workflow', 'python_sql', 'dataframe', 'native_expression']
    for role in roles:
        observations = [row for row in fixture['observations'] if row['role'] == role]
        require([row['query'] for row in observations] == list(range(1, 44)), 'fixture query scope')
        for row in observations:
            require(row['status'] == 'passed' and equivalent(row['actual'], row['expected']), 'fixture values')
    for query in range(1, 44):
        schemas = [strict_json(row['schema']) for row in fixture['observations'] if row['query'] == query]
        require(all(schema == schemas[0] for schema in schemas), 'fixture cross-surface schemas')

    cohort = read(review['final_cohort'])
    plan = read(cohort['plan'])
    require(cohort['status'] == 'passed' and cohort['source_unchanged']
            and cohort['input_full_hash_verified_before_and_after'] and cohort['cohort_lock_released'], 'cohort completion')
    require(plan['source_commit'] == review['source_commit'] and plan['build'] == review['build'], 'cohort source/build link')
    require(plan['role_order'] == roles and plan['executions_per_query_per_role'] == 1
            and plan['no_parallel_task_work'] and plan['no_profiling']
            and plan['no_cache_manipulation'] and plan['no_artificial_pacing'], 'cohort protocol')
    require(plan['selected_query_ids'] == {role: list(range(1, 44)) for role in roles}, 'declared query scope')
    require(plan['reference'] == review['reference'], 'cohort reference link')
    for helper in plan['helpers'] + [plan['supervisor'], plan['workload_guard']]:
        raw(helper)
    require([row['role'] for row in cohort['roles']] == roles, 'cohort roles')
    totals, result_schemas = {}, {query: [] for query in range(1, 44)}
    for role in cohort['roles']:
        require(role['status'] == 'passed' and role['full43_complete'] and role['process_group_drained']
                and role['direct_child_reaped'] and role['uat_lock_released'], 'role completion')
        summary = read(role['summary'])
        require(summary['complete'] and summary['full_result_validation']
                and summary['completed_identity_check'] and summary['reference_packet_identity_verified']
                and summary['harness_identity_verified'], 'runner completion')
        require([(row['query'], row['run']) for row in summary['records']] == [(q, 1) for q in range(1, 44)], 'execution scope')
        outputs = review['final_outputs'][role['role']]
        require(len(outputs) == 43, 'result coverage')
        elapsed, row_count = [], 0
        for query, (record, output_ref) in enumerate(zip(summary['records'], outputs, strict=True), 1):
            require(record['passed'] and record['returncode'] == 0 and record['validation'] == 'complete_values', 'query failed')
            envelope = read(output_ref)
            values, fields = extract_result(envelope), report_fields(envelope)
            result_schemas[query].append(strict_json(fields['result_schema_json']))
            require(equivalent(values, expected[query]), 'result differs from independent reference')
            require(fields['fallback_attempted'] == fields['external_engine_invoked'] == 'false', 'external execution')
            seconds = int(fields['native_query_execution_nanos']) / 1e9
            require(record['engine_timing_reported'] and math.isfinite(seconds) and seconds >= 0
                    and seconds == record['engine_execution_seconds'], 'engine clock mismatch')
            if query in (28, 29):
                require(fields['local_primitive_report_present'] == 'true', 'grouped provider unreachable')
            if role['role'] == 'native_expression' and query == 20:
                require(int(fields['relational_scan_rows_delivered']) == len(values), 'typed scan filter not pushed')
            elapsed.append(seconds)
            row_count += len(values)
        require(row_count == 381 and math.isclose(sum(elapsed), role['full43_engine_seconds'], rel_tol=1e-14), 'role totals')
        totals[role['role']] = math.fsum(elapsed)
    for schemas in result_schemas.values():
        require(len(schemas) == 5 and all(schema == schemas[0] for schema in schemas), 'Full43 cross-surface schemas')
    historical_validated = 0
    for prior in review['historical_cohorts']:
        receipt = read(prior['receipt'])
        require([role['role'] for role in receipt['roles']] == roles, 'historical role scope')
        for role, evidence in zip(receipt['roles'], prior['roles'], strict=True):
            require(role['role'] == evidence['role'] and role['summary'] == evidence['summary'], 'historical role identity')
            summary = read(evidence['summary'])
            require(len(summary['records']) == len(evidence['outputs']), 'historical output coverage')
            for record, output in zip(summary['records'], evidence['outputs'], strict=True):
                if record['passed']:
                    require(equivalent(extract_result(read(output)), expected[record['query']]), 'historical accepted values')
                    historical_validated += 1
            if role['full43_complete']:
                require(len(summary['records']) == 43 and all(row['passed'] for row in summary['records']), 'historical Full43 scope')
            else:
                require(role['full43_engine_seconds'] is None, 'partial run received a Full43 clock')
    return dict(status='passed', members=len(members), validated_queries=215,
                historical_validated_queries=historical_validated,
                full43_equal_schema_queries=43,
                small_fixture_comparisons=215, engine_seconds=totals, engine_executed=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--review', type=Path, required=True)
    parser.add_argument('--packet', type=Path, required=True)
    parser.add_argument('--repository', type=Path, required=True)
    parser.add_argument('--check-current-source', action='store_true')
    args = parser.parse_args()
    result = verify(json.loads(args.review.read_text()), args.packet.read_bytes(),
                    args.repository.resolve(), args.check_current_source)
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
