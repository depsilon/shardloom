"""Replay complete retained results without executing an engine or archived code."""
from __future__ import annotations

import argparse
import base64
from fractions import Fraction
import gzip
import hashlib
import json
import lzma
import math
from pathlib import Path
import re
import sys


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, 'duplicate JSON key: ' + key)
            result[key] = value
        return result

    def invalid(value):
        raise ValueError('nonfinite JSON constant: ' + value)

    def finite_float(value):
        number = float(value)
        require(math.isfinite(number), 'nonfinite JSON number')
        return number

    return json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid, parse_float=finite_float)


def equivalent(actual, expected):
    if type(actual) is not type(expected):
        return False
    if isinstance(actual, dict):
        return actual.keys() == expected.keys() and all(
            equivalent(actual[key], expected[key]) for key in actual)
    if isinstance(actual, list):
        return len(actual) == len(expected) and all(
            equivalent(a, b) for a, b in zip(actual, expected))
    if isinstance(actual, float):
        return math.isfinite(actual) and math.isfinite(expected) and math.isclose(
            actual, expected, rel_tol=1e-12, abs_tol=1e-12)
    return actual == expected


def values_digest(values):
    return digest(json.dumps(values, sort_keys=True, allow_nan=False).encode())


class Evidence:
    """An original path may have multiple explicitly identified source versions."""

    def __init__(self, packet):
        require(packet['schema'] == 'shardloom.versioned_result_evidence.v1', 'packet schema')
        self.files = {}
        for row in packet['files']:
            key = row['path'], row['sha256']
            require(key not in self.files, 'duplicate evidence identity')
            require(row['encoding'] in ('utf-8', 'base64'), 'unknown member encoding')
            raw = (row['data'].encode() if row['encoding'] == 'utf-8'
                   else base64.b64decode(row['data'], validate=True))
            require(len(raw) == row['bytes'] and digest(raw) == row['sha256'], 'member bytes changed')
            self.files[key] = raw

    def raw(self, reference):
        key = reference['path'], reference['sha256']
        require(key in self.files, 'missing exact evidence version: ' + reference['path'])
        raw = self.files[key]
        require(len(raw) == reference['bytes'], 'reference size differs')
        return raw

    def read(self, reference):
        raw = self.raw(reference)
        if reference['path'].endswith('.gz'):
            raw = gzip.decompress(raw)
        return strict_json(raw)


def verify_oracle(evidence, item, statements, query_hash):
    protocol = evidence.read(item['protocol'])
    acceptance = evidence.read(item['acceptance'])
    reference = evidence.read(item['reference'])
    require(protocol['status'] == 'frozen_before_execution', 'oracle was not predeclared')
    require(protocol['query_ids'] == list(range(1, 44)) and protocol['total_calls'] == 43
            and protocol['executions_per_query'] == 1, 'oracle execution scope differs')
    require(protocol['independence']['native_binary_input'] is False
            and protocol['independence']['native_values_input'] is False
            and protocol['independence']['native_runtime_invoked'] is False
            and protocol['independence']['baseline_only'] is True, 'oracle independence differs')
    require(protocol['dependency']['duckdb'] == '2.0.0', 'oracle dependency differs')
    for program in protocol['programs']:
        evidence.raw(program)
    evidence.raw(protocol['install_receipt'])
    require(protocol['queries']['sha256'] == query_hash, 'oracle query bytes differ')
    require(acceptance['status'] == 'passed' and acceptance['calls'] == 43
            and acceptance['rows'] == 381 and acceptance['source_hash_before_after']
            and acceptance['program_and_package_unchanged'] and acceptance['shared_lock_released'],
            'oracle completion proof differs')
    require(acceptance['native_runtime_invoked'] is False
            and acceptance['independent_correctness_oracle'] is True
            and acceptance['performance_comparison'] is False, 'oracle acceptance scope differs')
    require(acceptance['reference'] == item['reference']
            and acceptance['protocol'] == item['protocol'] == reference['protocol'], 'oracle identity link differs')
    require(reference['source'] == protocol['source'] and reference['queries_sha256'] == query_hash,
            'oracle source/query binding differs')
    require(reference['reference_kind'] == 'independent_reference'
            and reference['independent_correctness_oracle'] is True, 'reference kind differs')
    require(reference['records'] == acceptance['records'] and len(reference['records']) == 43,
            'frozen reference differs from accepted oracle output')
    require(len(item['outputs']) == len(item['receipts']) == 43, 'oracle raw coverage differs')
    values = {}
    for query, (record, plan, output_ref, receipt_ref) in enumerate(zip(
            reference['records'], protocol['plans'], item['outputs'], item['receipts'], strict=True), 1):
        output = evidence.read(output_ref)
        receipt = evidence.read(receipt_ref)
        require(record['query_id'] == plan['query_id'] == output['query_id'] == receipt['query_id'] == query,
                'oracle query order differs')
        require(record['statement'] == plan['statement'] == statements[query - 1]
                and record['equivalent_sql'] == plan['equivalent_sql'], 'oracle SQL binding differs')
        require(record['baseline_only'] is True and record['native_runtime_invoked'] is False
                and record['oracle_version'] == '2.0.0' and record['engine_version'] == 'v2.0.0',
                'oracle output role differs')
        require(record == dict(output, values_sha256=values_digest(output['values'])),
                'oracle frozen values differ from raw output')
        require(receipt['returncode'] == 0 and receipt['process_group_drained']
                and receipt['direct_child_reaped'], 'oracle process did not complete cleanly')
        require(receipt['status'] == 'passed' and receipt['temporary_payloads_remaining'] == 0
                and output_ref in receipt['outputs'], 'oracle raw output/cleanup binding differs')
        command = receipt['command']
        require(command[command.index('--parquet') + 1] == protocol['source']['path']
                and command[command.index('--query-id') + 1] == str(query)
                and command[command.index('--queries') + 1] == protocol['queries']['path'],
                'oracle command input differs')
        require(record['values_sha256'] == values_digest(record['values']), 'oracle value digest differs')
        require(receipt['values_sha256'] == record['values_sha256']
                and receipt['rows'] == len(record['values']), 'oracle receipt values differ')
        values[query] = record['values']
    require(sum(map(len, values.values())) == 381, 'oracle complete row count differs')
    return protocol, values


def verify_runtime(evidence, item, extract_result):
    runtime = evidence.read(item['runtime'])
    evidence.raw(runtime['source'])
    evidence.raw(runtime['script'])
    build = evidence.read(runtime['build'])
    plan = evidence.read(runtime['build_plan'])
    require(runtime['status'] == 'passed' and build['status'] == 'passed'
            and build['returncode'] == 0 and build['process_group_drained'], 'runtime build did not pass')
    require(build['source_before'] == build['source_after'], 'build source changed')
    require(runtime['source_commit'] == plan['source_commit']
            and runtime['source_tree'] == plan['source_tree'], 'runtime source identity differs')
    require(build['command'] == plan['command'] and build['target'] == plan['target'], 'build recipe differs')
    log = evidence.raw(item['build_log'])
    require(digest(log) == build['log_sha256'], 'build log differs')
    for name in ('shardloom-vortex', 'shardloom-cli'):
        require(f'Compiling {name} v0.7.0 ({build["cwd"]}/{name})'.encode() in log,
                'changed runtime package was not rebuilt')
    require(len(runtime['cases']) == 3 and [row['name'] for row in runtime['cases']] == [
        'integer_average', 'fused_integer_totals', 'regex_grouping'], 'functional role coverage differs')
    for case in runtime['cases']:
        require(case['passed'] and case['returncode'] == 0 and case['process_group_drained'],
                'functional role process failed')
        actual = extract_result(evidence.read(case['stdout']))
        require(actual == case['actual'] and equivalent(actual, case['expected']), 'functional role result differs')
        evidence.raw(case['stderr'])
    return runtime, build


def verify_native(evidence, item, runtime, expected, statements, query_hash, extract_result, report_fields):
    receipt = evidence.read(item['receipt'])
    plan = evidence.read(receipt['plan'])
    for name in ('helper', 'supervisor', 'workload_guard'):
        evidence.raw(plan[name])
    summary = evidence.read(item['summary'])
    complete = item['complete']
    require(receipt['runtime'] == item['runtime'] and receipt['binary'] == runtime['executable'],
            'native runtime binding differs')
    require(plan['runtime'] == item['runtime'] and plan['reference'] == item['reference']
            and plan['input'] == receipt['input'], 'native plan identity differs')
    require(plan['parallel_work'] is False and plan['extra_profiling'] is False
            and plan['artificial_pacing'] is False, 'native isolated procedure differs')
    require(plan['query_ids'] == list(range(1, 44)) and plan['repeats'] == 1
            and plan['expected_calls'] == 43, 'native planned scope differs')
    require(summary['build_commit'] == runtime['source_commit']
            and summary['binary_sha256'] == runtime['executable']['sha256']
            and summary['queries_sha256'] == query_hash, 'native source/executable/query differs')
    require(summary['source'] == receipt['input']['path']
            and summary['source_bytes'] == receipt['input']['bytes']
            and summary['source_generation'] == receipt['input_generation'], 'native input binding differs')
    require(summary['reference_kind'] == 'independent_reference'
            and summary['reference_packet']['sha256'] == item['reference']['sha256'], 'native reference differs')
    require(summary['selected_query_ids'] == list(range(1, 44)) and summary['repeats'] == 1
            and summary['score_scope'] == 'full_43', 'native selected scope differs')
    require(summary['memory_gb'] == 24 and summary['max_parallelism'] == 12, 'native resources differ')
    require(receipt['process_group_drained'] and receipt['pre_input_hash_verified'], 'native preflight/drain differs')
    require(summary['complete'] is complete, 'native completeness differs')
    count = 43 if complete else 4
    require([(r['query'], r['run']) for r in summary['records']] == [(q, 1) for q in range(1, count + 1)],
            'native query executions differ')
    require(len(item['outputs']) == count, 'native output coverage differs')
    rows, engine_samples = 0, []
    for row, output_ref in zip(summary['records'], item['outputs'], strict=True):
        query = row['query']
        envelope = evidence.read(output_ref)
        fields = report_fields(envelope)
        require(envelope['fallback']['attempted'] is False and envelope['fallback']['allowed'] is False
                and fields['fallback_attempted'] == 'false' and fields['external_engine_invoked'] == 'false',
                'native no-fallback boundary differs')
        actual = extract_result(envelope)
        matched = equivalent(actual, expected[query])
        require(row['passed'] is matched and matched is (complete or query < 4), 'native result disposition differs')
        require(row['returncode'] == 0 and row['query_statement'] == statements[query - 1]
                and not row['guard_failures'], 'native query execution differs')
        seconds_value = row['engine_execution_seconds']
        require(row['engine_timing_reported'] and type(seconds_value) in (int, float)
                and math.isfinite(seconds_value) and seconds_value >= 0, 'native engine clock missing')
        engine_samples.append(seconds_value)
        rows += len(actual)
        if matched:
            require(row['validation'] == 'complete_values', 'native result validation differs')
    if complete:
        seconds = math.fsum(engine_samples)
        require(receipt['status'] == 'passed' and receipt['returncode'] == 0
                and receipt['post_input_hash_verified'] and receipt['source_unchanged']
                and receipt['uat_lock_released'], 'native completion proof differs')
        require(summary['full_result_validation'] and summary['completed_identity_check']
                and summary['reference_packet_identity_verified'] and summary['harness_identity_verified'],
                'native postflight validation differs')
        require(seconds == summary['timing_totals']['totals']['engine_execution_seconds']
                == receipt['engine_execution_seconds'] == item['engine_execution_seconds'], 'native engine total differs')
        require(rows == receipt['result_rows'] == 381, 'native result rows differ')
    else:
        require(receipt['status'] == 'failed' and receipt['returncode'] != 0
                and 'engine_execution_seconds' not in item, 'incomplete run treated as a Full43 result')
    return summary


def verify(review, packet, repository, check_current_source=False):
    sys.path[:0] = [str(repository / 'scripts'), str(repository / 'python/src')]
    from native_workflow_protocol import extract_result, report_fields
    evidence = Evidence(packet)
    require(review['schema'] == 'shardloom.independent_full43_correction.v1', 'review schema')
    require(review['claims'] == dict(independent_full43_parity=True, new_release=False,
            performance_superiority=False, competitive_gates_closed=False), 'review claim scope differs')
    queries = evidence.raw(review['queries'])
    query_hash = digest(queries)
    require(query_hash == '4afa04814edf3a4c52ff26fd87ea3b5dd92c7264b2d8d69ee718709f3df6f09b', 'pinned query file differs')
    statements = [s.strip() for s in '\n'.join(line for line in queries.decode().splitlines()
        if not line.lstrip().startswith('--')).split(';') if s.strip()]
    require(len(statements) == 43 and len(review['oracles']) == 2, 'query/oracle coverage differs')
    oracles = [verify_oracle(evidence, item, statements, query_hash) for item in review['oracles']]
    first, corrected = [values for _, values in oracles]
    require(oracles[0][0]['source'] == oracles[1][0]['source'], 'original Parquet identity differs')
    lineage = review['input_lineage']
    lineage_index = evidence.read(lineage['index'])
    ingestion = evidence.read(lineage['acceptance'])
    summary = evidence.read(lineage['summary'])
    ingestion_output = evidence.read(lineage['output'])
    ingestion_fields = report_fields(ingestion_output)
    ingestion_plan = evidence.read(lineage['plan'])
    require(lineage_index['acceptance'] == lineage['acceptance']
            and ingestion['ingest_summary'] == lineage['summary']
            and ingestion['ingest_stdout'] == lineage['output']
            and ingestion['plan'] == lineage['plan'], 'ingest record identities differ')
    require(ingestion['status'] == 'passed' and ingestion['fresh_ingest'] is True
            and ingestion['ingest_rows'] == 99997497 and ingestion['source_identity_preserved'] is True,
            'native input ingestion was not accepted')
    require(summary['returncode'] == 0 and summary['stop_reason'] == 'process_completed'
            and summary['target_exists'] and summary['stdout_json_ok'], 'native input ingestion failed')
    require(ingestion_fields['public_workflow_preparation_vortex_ingest_stream_validation_rows'] == '99997497'
            and ingestion_fields['public_workflow_preparation_vortex_ingest_stream_projection_rows'] == '99997497'
            and ingestion_fields['fallback_attempted'] == 'false'
            and ingestion_fields['external_engine_invoked'] == 'false', 'native input row/fallback proof differs')
    dataset_ids = [{key: row[key] for key in ('path', 'bytes', 'sha256')} for row in ingestion['datasets']]
    require(len(dataset_ids) == 2 and dataset_ids[0] == oracles[1][0]['source'], 'oracle is not the ingested source')
    require(summary['source'] == dataset_ids[0]['path'] and summary['source_bytes'] == dataset_ids[0]['bytes']
            and summary['target'] == dataset_ids[1]['path'] and summary['target_bytes'] == dataset_ids[1]['bytes'],
            'ingestion source/target differs')
    ingest_command = ingestion_plan['commands']['ingest']
    require(ingest_command[ingest_command.index('--source') + 1] == dataset_ids[0]['path']
            and ingest_command[ingest_command.index('--target') + 1] == dataset_ids[1]['path'], 'ingestion command differs')
    require([q for q in range(1, 44) if first[q] != corrected[q]] == [4], 'oracle revision changed unrelated results')
    adjudication = evidence.read(review['adjudication'])
    exact = evidence.read(adjudication['output'])
    require(adjudication['status'] == 'passed' and adjudication['returncode'] == 0
            and adjudication['process_group_drained'] and adjudication['source_hash_unchanged']
            and adjudication['dependency_hashes_unchanged'] and adjudication['shared_lock_released']
            and adjudication['temporary_payloads_remaining'] == 0, 'arithmetic adjudication failed')
    arithmetic_protocol = evidence.read(adjudication['protocol'])
    require(arithmetic_protocol['source'] == oracles[1][0]['source'], 'adjudication source differs')
    require(oracles[1][0]['arithmetic_adjudication'] == review['adjudication']
            and oracles[1][0]['supersedes'] == review['oracles'][0]['reference']
            and oracles[1][0]['native_result_values_used'] is False, 'oracle revision evidence differs')
    require(exact['native_runtime_invoked'] is False and exact['oracle_version'] == '2.0.0', 'adjudication oracle differs')
    values = exact['values']
    average = float(values['exact_converted_sum']) / values['count']
    require(average == float(Fraction(values['exact_converted_sum'], values['count']))
            == values['compensated_average'] == corrected[4][0]['avg_UserID_0'], 'exact integer adjudication differs')
    runtimes = {}
    for item in review['runtimes']:
        runtime, build = verify_runtime(evidence, item, extract_result)
        runtimes[item['runtime']['sha256']] = runtime, build
    require(len(runtimes) == 2 and len(review['native_runs']) == 4, 'native observation history differs')
    for index, item in enumerate(review['native_runs']):
        require(item['complete'] is (index > 0), 'stopped/complete run order differs')
        reference = review['oracles'][0 if index == 0 else 1]['reference']
        require(item['reference'] == reference, 'native oracle cohort differs')
        runtime, _ = runtimes[item['runtime']['sha256']]
        require(evidence.read(item['receipt'])['input'] == dataset_ids[1], 'native input is not the accepted Vortex generation')
        verify_native(evidence, item, runtime, first if index == 0 else corrected,
                      statements, query_hash, extract_result, report_fields)
    before_fields = report_fields(evidence.read(review['native_runs'][-2]['outputs'][28]))
    after_fields = report_fields(evidence.read(review['native_runs'][-1]['outputs'][28]))
    require(before_fields['local_primitive_aggregate_update_strategy'] == 'direct_accessor_general_group_update'
            and before_fields['local_primitive_embedded_derived_column_rewrite_status'] == 'applied', 'Q29 regression path differs')
    require(after_fields['local_primitive_aggregate_update_strategy'] == 'transformed_dictionary_dense_general_chunk_partial_group_update'
            and after_fields['local_primitive_aggregate_accessor_summary'] == 'Referer:chunk_utf8_dictionary'
            and after_fields['local_primitive_aggregate_first_pass_accessor_rows'] == '0'
            and after_fields['local_primitive_materialized_group_value_count'] == '0'
            and 'local_primitive_embedded_derived_column_rewrite_status' not in after_fields, 'Q29 corrected dictionary path differs')
    current, current_build = runtimes[review['native_runs'][-1]['runtime']['sha256']]
    require(current['source_commit'] == review['runtime_source_commit'], 'current runtime revision differs')
    source_names = [source['repository_path'] for source in review['current_sources']]
    require(len(source_names) == len(set(source_names)) == 21, 'changed runtime source coverage differs')
    require({'shardloom-vortex/src/local_primitives/numeric_aggregate_sum.rs',
             'shardloom-vortex/src/local_primitives.rs', 'shardloom-vortex/src/url_domain.rs',
             'shardloom-cli/src/public_workflow_route.rs'}.issubset(source_names),
            'corrected runtime sources are missing')
    for source in review['current_sources']:
        raw = evidence.raw(source['file'])
        require(current_build['source_after'][source['repository_path']] == digest(raw), 'current source binding differs')
        if check_current_source:
            require((repository / source['repository_path']).read_bytes() == raw, 'current repository source changed')
    validation_names = [Path(item['receipt']['path']).stem for item in review['validations']]
    require(len(validation_names) == 7 and set(validation_names) == {
        'q29-native-tests-950', 'q29-fmt-950', 'q29-workspace-clippy-950',
        'q29-workspace-tests-950', 'q29-native-clippy-950', 'q29-public-tests-950',
        'q29-oracle-tests-950'}, 'required validation coverage differs')
    for item in review['validations']:
        record = evidence.read(item['receipt'])
        log = evidence.raw(item['log'])
        require(record['status'] == 'passed' and record['returncode'] == 0
                and record['process_group_drained'], 'required validation failed')
        require(record['source_before'] == record['source_after'] == current_build['source_after'],
                'required validation source differs')
        require(digest(log) == record['log_sha256'], 'required validation log differs')
        if 'test' in record['command']:
            counts = re.findall(rb'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;', log)
            require(counts and sum(int(row[0]) for row in counts) > 0
                    and all(int(row[1]) == 0 for row in counts), 'required validation has no passing tests')
        if Path(item['receipt']['path']).stem == 'q29-oracle-tests-950':
            require(b'Ran 11 tests' in log and b'\nOK\n' in log, 'independent oracle tests did not pass')
    for ref in review['retained_failures'] + review['supporting_records']:
        evidence.raw(ref)
    return dict(status='passed', oracle_cohorts=2, complete_native_passes=3,
                stopped_native_passes=1, queries_per_complete_pass=43, rows_per_complete_pass=381,
                current_engine_execution_seconds=review['native_runs'][-1]['engine_execution_seconds'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--review', type=Path, required=True)
    parser.add_argument('--packet', type=Path, required=True)
    parser.add_argument('--repository', type=Path, required=True)
    parser.add_argument('--check-current-source', action='store_true')
    args = parser.parse_args()
    review = strict_json(args.review.read_bytes())
    packed = args.packet.read_bytes()
    require(digest(packed) == review['packet']['sha256'] and len(packed) == review['packet']['bytes'], 'packet bytes differ')
    result = verify(review, strict_json(lzma.decompress(packed)), args.repository.resolve(), args.check_current_source)
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
