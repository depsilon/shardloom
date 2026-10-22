"""Check original collected rows, declared ordering and logical types in retained evidence."""
import argparse
import copy
import json
import lzma
import math
from pathlib import Path
import sys

from verify_packet_929 import Evidence, digest, replay, require, strict_json


def fields(items):
    result = {}
    for item in items:
        require(item['key'] not in result, 'duplicate envelope field')
        result[item['key']] = item['value']
    return result


def expected_columns(workload, index):
    count = ('row_count', 'u64')
    total = ('metric_sum', 'f64')
    kind = workload.result_kind
    if kind == 'high_cardinality' and index == 1 or kind == 'distinct':
        return [('distinct_category_count', 'u64')]
    if kind in ('scalar', 'nested'):
        return [count, total] + ([('flagged', 'f64')] if kind == 'nested' else [])
    if kind in ('groups', 'high_cardinality'):
        return [(key, 'utf8' if key in ('dim_label', 'category') else 'i64')
                for key in workload.keys] + [count, total]
    if kind == 'top':
        return [('id', 'i64'), ('metric', 'f64')]
    if kind == 'rank':
        return [('group_key', 'i64'), ('id', 'i64'), ('metric', 'f64'), ('rank', 'i64')]
    if kind == 'etl':
        return [('dim_label', 'utf8'), ('bucket', 'i64'), count, total, ('weighted_sum', 'f64')]
    raise ValueError('undeclared result kind')


def check_payload(envelope, workload, index):
    original = fields(envelope['result']['fields'])
    flattened = fields(envelope['fields'])
    require(all(key in flattened and value == flattened[key] for key, value in original.items()),
            'duplicated result fields differ')
    require(original['result_payload_complete'] == 'true', 'original payload incomplete')
    require(original['result_schema_format'] == 'vortex.dtype.serde.v1', 'unknown logical schema format')
    schema = strict_json(original['result_schema_json'])
    require(set(schema) == {'Struct'} and len(schema['Struct']) == 2
            and schema['Struct'][1] is False, 'unexpected result structure')
    struct = schema['Struct'][0]
    expected = expected_columns(workload, index)
    require(struct['names'] == [name for name, _ in expected]
            and len(struct['dtypes']) == len(expected), 'logical columns differ')
    nullable = []
    for (_, kind), dtype in zip(expected, struct['dtypes']):
        if kind == 'utf8':
            require(set(dtype) == {'Utf8'} and type(dtype['Utf8']) is bool, 'logical type differs')
            nullable.append(dtype['Utf8'])
        else:
            require(set(dtype) == {'Primitive'} and len(dtype['Primitive']) == 2
                    and dtype['Primitive'][0] == kind and type(dtype['Primitive'][1]) is bool,
                    'logical type differs')
            nullable.append(dtype['Primitive'][1])
    rows = [strict_json(line) for line in original['result_jsonl'].splitlines() if line.strip()]
    nulls = 0
    for row in rows:
        require(set(row) == set(struct['names']), 'row fields differ')
        for (name, kind), allows_null in zip(expected, nullable):
            value = row[name]
            if value is None:
                require(allows_null, 'null in a nonnullable field')
                nulls += 1
            elif kind in ('i64', 'u64'):
                low, high = (-(1 << 63), (1 << 63) - 1) if kind == 'i64' else (0, (1 << 64) - 1)
                require(type(value) is int and low <= value <= high, 'integer value outside logical domain')
            elif kind == 'f64':
                require(type(value) in (int, float) and math.isfinite(value), 'non-finite or nonnumeric value')
            else:
                require(type(value) is str, 'non-string value')
    key = None
    if workload.result_kind == 'top':
        key = lambda row: (-row['metric'], row['id'])
    elif workload.result_kind == 'high_cardinality' and index == 0:
        key = lambda row: row['category']
    elif workload.result_kind == 'etl':
        key = lambda row: (-row['weighted_sum'], row['dim_label'], row['bucket'])
    if key is not None:
        require(rows == sorted(rows, key=key), 'declared terminal order differs')
    return {'rows': len(rows), 'null_values': nulls, 'terminal_order_checked': key is not None,
            'schema_sha256': digest(original['result_schema_json'].encode()),
            'jsonl_sha256': digest(original['result_jsonl'].encode())}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    manifest = strict_json(args.manifest.read_bytes())
    archive = args.manifest.parent / Path(manifest['archive']['path']).name
    raw = archive.read_bytes()
    require(len(raw) == manifest['archive']['bytes'] and digest(raw) == manifest['archive']['sha256'],
            'packet identity differs')
    packet = strict_json(lzma.decompress(raw))
    base_replay = replay(args.repository.resolve(), manifest, packet)
    evidence = Evidence(packet)
    accepted = evidence.read(evidence.check(manifest['acceptance']))
    sys.path.insert(0, str(args.repository.resolve()))
    from benchmarks.traditional_analytics.workloads import WORKLOADS
    records = []
    positive = None
    for cohort in accepted['cohorts']:
        if cohort['output_format'] != 'collect':
            continue
        report = evidence.read(evidence.check(cohort['report']))
        for record_index, record in enumerate(report['records']):
            if record['engine'] != 'shardloom':
                continue
            workload = WORKLOADS[record['scenario']]
            calls = strict_json(record['evidence']['benchmark_native_calls'])
            expected_count = len(workload.statements) + bool(workload.write_statement)
            require(len(calls) == expected_count, 'declared native call count differs')
            collected = [call for call in calls
                         if call['command'][call['command'].index('--request') + 1] == 'collect']
            require(len(collected) == len(workload.statements), 'declared collect call count differs')
            for index, call in enumerate(collected):
                stdout = evidence.raw(call['stdout'])
                require(digest(stdout) == call['stdout_sha256'], 'original stdout identity differs')
                envelope = strict_json(stdout)
                result = check_payload(envelope, workload, index)
                records.append({'task': record['scenario'], 'cohort': cohort['ordinal'],
                    'input_format': record['format'], 'record_index': record_index,
                    'statement_index': index, 'stdout_member': call['stdout'],
                    'stdout_sha256': call['stdout_sha256'], **result})
                if positive is None and workload.result_kind == 'top':
                    positive = (envelope, workload, index)
    require(len(records) == 368 and len({row['task'] for row in records}) == 22,
            'collected payload coverage differs')
    require(sum(row['terminal_order_checked'] for row in records) == 48,
            'ordered statement coverage differs')
    negatives = []
    for case in ('reversed original rows', 'changed logical type', 'missing row field',
                 'incomplete payload', 'duplicate envelope field'):
        envelope, workload, index = copy.deepcopy(positive)
        original = fields(envelope['result']['fields'])
        if case == 'reversed original rows':
            original['result_jsonl'] = '\n'.join(reversed(original['result_jsonl'].splitlines()))
        elif case == 'changed logical type':
            schema = strict_json(original['result_schema_json'])
            schema['Struct'][0]['dtypes'][0]['Primitive'][0] = 'f64'
            original['result_schema_json'] = json.dumps(schema)
        elif case == 'missing row field':
            rows = [strict_json(line) for line in original['result_jsonl'].splitlines()]
            del rows[0]['id']
            original['result_jsonl'] = '\n'.join(json.dumps(row) for row in rows)
        elif case == 'incomplete payload':
            original['result_payload_complete'] = 'false'
        items = [{'key': key, 'value': value} for key, value in original.items()]
        if case == 'duplicate envelope field':
            items.append(copy.deepcopy(items[0]))
        envelope['fields'] = copy.deepcopy(items)
        envelope['result']['fields'] = copy.deepcopy(items)
        try:
            check_payload(envelope, workload, index)
        except ValueError as error:
            negatives.append({'case': case, 'status': 'rejected', 'reason': str(error)})
        else:
            raise ValueError('invalid original payload accepted: ' + case)
    result = {'status': 'passed_original_collect_payload_contracts', 'base_replay': base_replay,
        'complete_native_statements': len(records), 'native_collect_records': 352,
        'terminal_order_statements': 48, 'logical_schema_statements': 368,
        'observed_null_values': sum(row['null_values'] for row in records),
        'negative_cases': negatives, 'records': records,
        'limits': ['Only the two collect cohorts and declared result schemas are checked here.',
            'The writer cohorts retain their separate original complete-value readback checks.',
            'No nullable value occurs in these outputs; empty/all-null behavior needs separate regression evidence.',
            'Pandas records do not preserve logical schema metadata; equal logical schemas across engines are not claimed.',
            'The four-decimal comparison and fixture scale remain unchanged. This is not precision, pressure, platform or performance certification.'],
        'runtime_executed': False, 'verifier_sha256': digest(Path(__file__).read_bytes())}
    with args.output.open('x') as stream:
        json.dump(result, stream, indent=2, allow_nan=False)
        stream.write('\n')
    print(json.dumps({key: value for key, value in result.items() if key != 'records'}))


if __name__ == '__main__':
    main()
