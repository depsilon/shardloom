"""Verify the finite survivor admission record without engines or original paths."""
import argparse
from collections import OrderedDict
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import re
import tarfile

BASE = '236dbccd590a301ede8f34cce09715be92f025e5'
FINAL_CHECKS = ('survivor-fmt-r1', 'survivor-default-clippy-r1',
                'survivor-default-tests-r1', 'native-clippy-r3',
                'survivor-native-tests-r1', 'survivor-final-screen-r1')
COMMANDS = {
    'survivor-fmt-r1': ['cargo', 'fmt', '--all', '--', '--check'],
    'survivor-default-clippy-r1': ['cargo', 'clippy', '--offline', '--locked',
        '--workspace', '--all-targets', '--', '-D', 'warnings'],
    'survivor-default-tests-r1': ['cargo', 'test', '--offline', '--locked',
        '--workspace', '--all-targets'],
    'native-clippy-r3': ['cargo', 'clippy', '--offline', '--locked', '--workspace',
        '--all-targets', '--features', 'release-user-surfaces', '--', '-D', 'warnings'],
    'survivor-native-tests-r1': ['cargo', 'test', '--offline', '--locked', '--workspace',
        '--all-targets', '--features', 'release-user-surfaces'],
    'survivor-final-screen-r1': ['cargo', 'test', '--offline', '--locked', '-p',
        'shardloom-vortex', '--features', 'release-user-surfaces', '--lib',
        'survivor_admission', '--', '--nocapture', '--test-threads=1'],
}
CASES = {'sparse_filter': (8192, 512, 1), 'zoned_sparse_filter': (8192, 512, 1),
         'dense_filter': (8192, 1, 1), 'tiny_filter': (7, 2, 1),
         'nullable_filter': (1024, 31, 1), 'wide_filter': (1024, 128, 1024),
         'empty_filter': (0, 1, 1)}
CONTROLS = (
    'output_compaction_applies_pressure_and_bounded_regrowth_with_retained_output',
    'output_compaction_declines_without_spending_existing_record_credits',
    'output_compaction_drains_owners_on_consumer_error_and_cancellation',
    'output_compaction_sizes_fixed_list_gather_scratch_before_selecting_rows',
    'application_late_failure_is_observed_before_downstream_limit_success',
    'streaming_join_zero_limit_still_drains_and_rejects_a_late_producer_error',
    'completion_input_late_expression_failure_cannot_certify_an_emitted_prefix',
    'completion_input_late_failure_cancellation_and_consumer_error_never_complete',
)
REGISTRATIONS = {
    'shardloom-vortex/src/local_primitive_relational_transform.rs':
        '#[cfg(all(test, debug_assertions, feature = "vortex-write"))]\n'
        '#[path = "local_primitive_relational_survivor_admission_tests.rs"]\n'
        'mod survivor_admission_tests;\n\n',
    'shardloom-vortex/src/local_primitive_relational_tests.rs':
        '#[cfg(debug_assertions)]\n'
        '#[path = "local_primitive_relational_survivor_work_tests.rs"]\n'
        'mod survivor_work_tests;\n',
}

def shape(data):
    return {'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest()}

def require(condition, reason):
    if not condition:
        raise ValueError(reason)

def json_bytes(data):
    return json.loads(data)

def read_tar(data):
    result = {}
    total = 0
    with tarfile.open(fileobj=io.BytesIO(data), mode='r:*') as archive:
        for member in archive:
            name = PurePosixPath(member.name)
            require(member.isfile() and not name.is_absolute() and '..' not in name.parts,
                    'archive member is not a relative regular file')
            require(str(name) == member.name and member.name not in result,
                    'duplicate or noncanonical archive member')
            total += member.size
            require(total <= 128 << 20, 'archive exceeds declared replay bound')
            result[member.name] = archive.extractfile(member).read()
            require(len(result[member.name]) == member.size, 'incomplete archive member')
    return result

def expected_rows(case):
    if case == 'sparse_join_chain':
        return [{'n': row * 512} for row in range(16)]
    rows, stride, width = CASES[case]
    groups = OrderedDict()
    for row in range(0, rows, stride):
        value = None if row % 17 == 0 else (
            [' AΣΟΣ東京', 'aΣΟΣ東京', ' Bİß '][row % 3] * width).lower()
        if value not in groups:
            groups[value] = {'key': value, 'rows': 0, 'first': row, 'last': row}
        groups[value]['rows'] += 1
        groups[value]['last'] = row
    return list(groups.values())

def records_from_log(log):
    output = []
    for line in log.decode().splitlines():
        marker = 'SURVIVOR_ADMISSION '
        if marker in line:
            output.append(json.loads(line.split(marker, 1)[1]))
    require(len(output) == 13, 'final admission log must contain all 13 records')
    result = {record['case']: record for record in output}
    require(len(result) == len(output), 'duplicate admission observation')
    return result

def expected_schema(case):
    # Native result DType serialization and the admitted COUNT/MIN/MAX contract.
    names = ['n'] if case == 'sparse_join_chain' else ['key', 'rows', 'first', 'last']
    types = [{'Primitive': ['i64', False]}] if case == 'sparse_join_chain' else [
        {'Utf8': True}, {'Primitive': ['u64', False]},
        {'Primitive': ['i64', True]}, {'Primitive': ['i64', True]}]
    return {'Struct': [{'names': names, 'dtypes': types}, False]}

def exact_json(value):
    # Python equality conflates bool/int and int/float; typed results must not.
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False,
                      separators=(',', ':'))

def verify_records(records):
    work = set(CASES) | {'sparse_join_chain'}
    require(set(records) == work | {'preparation_boundary', 'projection_failure_order',
            'source_callback_order', 'pinned_source_allocations', 'pending_failure_cleanup'},
            'admission cases changed')
    for case in sorted(work):
        record = records[case]
        expected = expected_rows(case)
        actual = [json.loads(line) for line in record['actual_jsonl'].splitlines()]
        require(exact_json(actual) == exact_json(expected) == exact_json(record['reference_rows']),
                f'{case}: complete values/order/types')
        require(record['complete_result_rows'] == record['output_rows'] == len(expected),
                f'{case}: complete result count')
        require(record['complete_values_match'] is True and record['fallback_attempted'] is False,
                f'{case}: completion/no-fallback')
        require(record['engine_timing'] is None and record['logical_zone_exclusions'] is None
                and record['pinned_backing_bytes_at_consumer'] is None,
                f'{case}: unobserved boundary was filled in')
        schema = json.loads(record['actual_schema'])
        require(exact_json(schema) == exact_json(expected_schema(case)),
                f'{case}: complete schema/field order/nullability/width')
        require(len(record['inputs']) == (2 if case == 'sparse_join_chain' else 1),
                f'{case}: input identities')
        for item in record['inputs']:
            require(item['bytes'] > 0 and re.fullmatch('[0-9a-f]{64}', item['sha256']),
                    f'{case}: invalid input identity')
        require(len(record['physical_reads']) == len(record['inputs']),
                f'{case}: source/read attribution')
        for source in record['physical_reads']:
            require(re.fullmatch('[0-9a-f]{64}', source['generation']) is not None,
                    f'{case}: generation identity')
            for key in ('segment_requests', 'completed_read_requests', 'completed_read_bytes',
                        'segment_reuse_hits'):
                require(type(source[key]) is int and source[key] >= 0, f'{case}: invalid read count')
        if case == 'zoned_sparse_filter':
            require(record['logical_layout_zones'] == [
                {'source': 0, 'rows': 8192, 'zone_rows': 256, 'zone_count': 32}],
                'logical zone geometry changed')
        else:
            require(record['logical_layout_zones'] == [], f'{case}: invented logical zones')
    sparse = records['sparse_filter']
    require(sparse['text_expression_rows'] == 16 and sparse['text_expression_preparations'] > 1
            and sparse['aggregate_value_preparations'] > 1, 'sparse preparation witness absent')
    joined = records['sparse_join_chain']
    require(joined['key_preparations'] > 16 and joined['output_batches'] == 16
            and joined['join_output_rows'] == 32, 'sparse join preparation witness absent')
    observations = records['preparation_boundary']['observations']
    require([row['strategy'] for row in observations] ==
            ['direct', 'list_forward', 'chunked', 'physical_compaction'], 'preparation controls changed')
    for row in observations:
        require(row['ordered_values'] == list(range(16))
                and row['complete_ordered_values_match'] is True, 'compaction changed row values/order')
        require(row['consumer_key_preparations'] == row['consumer_invocations']
                and row['compaction_key_preparations'] >= 0
                and row['all_key_preparations'] == row['consumer_key_preparations'] +
                row['compaction_key_preparations'], 'preparation scope mismatch')
        count = row['consumer_invocations']
        require(count == 16 if row['strategy'] in ('direct', 'list_forward') else
                count == 1 if row['strategy'] == 'chunked' else 1 <= count < 16,
                'actual consumer preparation reduction absent')
        if row['strategy'] in ('chunked', 'physical_compaction'):
            require(1 <= row['all_key_preparations'] < 16,
                    'compaction preparations erase the actual work reduction')
    error = records['projection_failure_order']
    require(error['candidate_preserves_error'] is False
            and 'signed integer arithmetic overflow' in error['direct']
            and 'division by zero' in error['compacted'] and error['direct'] != error['compacted'],
            'error-order counterexample absent')
    callback = records['source_callback_order']
    require(callback['direct_calls'] == 1 and callback['flush_on_error_calls'] == 2
            and callback['same_final_error'] is True and callback['candidate_preserves_callbacks'] is False
            and callback['direct_error'] == callback['flush_on_error_error'],
            'callback-order counterexample absent')
    pinned = records['pinned_source_allocations']
    require(pinned['visible_slice_bytes'] == 64 and pinned['original_payload_bytes'] == 2 << 20
            and pinned['live_credit_increase_bytes'] >= pinned['original_payload_bytes']
            and pinned['complete_rows'] == 8 and pinned['released_after_flush'] is True
            and pinned['generic_backing_size_api'] is None,
            'view/backing ownership distinction absent')
    cleanup = records['pending_failure_cleanup']
    require(cleanup['consumer_error'] is True and cleanup['cancellation'] is True
            and cleanup['remaining_credits_bytes'] == 0, 'pending owner cleanup absent')
    return {'work_cases': len(work), 'complete_values': sum(len(expected_rows(x)) for x in work),
            'admission_records': len(records), 'preparation_strategies': len(observations)}

def verify_members(members):
    sources = None
    totals = {}
    for name in FINAL_CHECKS:
        receipt = json_bytes(members[f'raw/{name}.json'])
        require(receipt['head'] == BASE and receipt['status'] == 'passed'
                and receipt['returncode'] == 0 and receipt['sources_unchanged'] is True
                and receipt['process_group_drained'] is True, f'{name}: incomplete validation')
        require(receipt['command'] == COMMANDS[name], f'{name}: wrong validation command')
        require(receipt['guard_sha256'] == shape(members['raw/guard_checks.py'])['sha256'],
                f'{name}: check guard identity')
        if sources is None:
            sources = receipt['source_files']
        require(sources == receipt['source_files'], f'{name}: different tested source')
        matches = re.findall(rb'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;',
                             members[f'raw/logs/{name}.log'])
        if 'tests' in name or 'screen' in name:
            require(matches and sum(int(x[0]) for x in matches) > 0
                    and not any(int(x[1]) for x in matches), f'{name}: no passing test proof')
            totals[name] = {'passed': sum(int(x[0]) for x in matches),
                            'ignored': sum(int(x[2]) for x in matches)}
    require(totals['survivor-final-screen-r1'] == {'passed': 7, 'ignored': 0},
            'seven admission tests must execute')
    history = {**{f'admission-r{index}': 'failed' for index in range(1, 4)},
               **{f'native-clippy-r{index}': 'failed' for index in range(1, 3)},
               'admission-r4': 'passed'}
    for name, status in history.items():
        receipt = json_bytes(members[f'raw/{name}.json'])
        require(receipt['status'] == status and receipt['sources_unchanged'] is True
                and receipt['process_group_drained'] is True, f'{name}: historical attempt changed')
        require((receipt['returncode'] == 0) == (status == 'passed'),
                f'{name}: historical outcome mismatch')
        require(members[f'raw/logs/{name}.log'], f'{name}: historical log absent')
    native = members['raw/logs/survivor-native-tests-r1.log'].decode()
    for name in CONTROLS:
        require(re.search(r'^test [^\n]*::' + re.escape(name) + r' \.\.\. ok$', native, re.M),
                f'existing boundary control did not pass: {name}')
    snapshot = read_tar(members['raw/admission-source-r7.tar'])
    for path, data in snapshot.items():
        name = PurePosixPath(path)
        if name.name.startswith('._'):
            payload = str(name.with_name(name.name[2:]))
            require(payload in snapshot and payload in sources
                    and len(data) == 163 and data[:8] == bytes.fromhex('0005160700020000'),
                    f'unrecognized archive metadata: {path}')
            continue
        require(shape(data) == sources[path], f'final source snapshot differs: {path}')
    for name, data in members.items():
        if name.startswith('control-source/'):
            require(shape(data) == sources[name[len('control-source/'):]],
                    f'control source differs from tested source: {name}')
    for path, insertion in REGISTRATIONS.items():
        final = snapshot[path].decode()
        require(final.count(insertion) == 1 and final.replace(insertion, '', 1).encode() ==
                members[f'baseline/{path}'], f'production code changed outside test registration: {path}')
    baseline = json_bytes(members['baseline/source-files.json'])
    additions = {
        'docs/architecture/native-survivor-admission-2026-11-15.md',
        'shardloom-vortex/src/local_primitive_relational_survivor_admission_tests.rs',
        'shardloom-vortex/src/local_primitive_relational_survivor_work_tests.rs',
    }
    require(set(sources) == set(baseline) | additions, 'tested source membership changed')
    for path, expected in baseline.items():
        if path in REGISTRATIONS:
            require(shape(members['baseline/' + path]) == expected,
                    f'baseline registration source changed: {path}')
        else:
            require(sources[path] == expected, f'non-admission source changed: {path}')
    inventory = json_bytes(members['raw/source-owner-inventory-r1.json'])
    for item in inventory['source_files']:
        data = members[f"inventory-source/{inventory['commit']}/{item['path']}"]
        require(shape(data)['sha256'] == item['sha256'], 'owner source hash mismatch')
    for item in inventory['excerpts']:
        data = members[f"inventory-source/{inventory['commit']}/{item['path']}"]
        actual = '\n'.join(data.decode().splitlines()[item['start_line'] - 1:item['end_line']])
        require(actual == item['literal_excerpt'], 'owner excerpt mismatch')
    provider = json_bytes(members['raw/provider-inventory-r2.json'])
    for item in provider['citations']:
        data = members[f"inventory-source/{provider['commit']}/{item['file']}"]
        require(shape(data)['sha256'] == item['sha256'], 'provider source hash mismatch')
        actual = '\n'.join(data.decode().splitlines()[item['start_line'] - 1:item['end_line']])
        require(actual == item['excerpt'], 'provider excerpt mismatch')
    records = records_from_log(members['raw/logs/survivor-final-screen-r1.log'])
    facts = verify_records(records)
    return dict(status='passed', decision='decline_unconditional_callback_adapter',
                production_runtime_change=False, performance_comparison=False,
                broader_I_complete=False, existing_batch_contract_retained=True,
                **facts, validation=totals, existing_boundary_controls=len(CONTROLS),
                preserved_historical_attempts=len(history),
                inventory_excerpts=len(inventory['excerpts']) + len(provider['citations']))

def verify(root):
    manifest = json_bytes((root / 'manifest.json').read_bytes())
    require(manifest['schema'] == 'shardloom.survivor_admission_packet.v1', 'manifest schema')
    for name, expected in manifest['files'].items():
        path = PurePosixPath(name)
        require(not path.is_absolute() and '..' not in path.parts, 'manifest traversal')
        require(shape((root / name).read_bytes()) == expected, f'packet file differs: {name}')
    members = read_tar((root / 'observations.tar.gz').read_bytes())
    require(set(members) == set(manifest['members']), 'archive membership differs')
    for name, data in members.items():
        require(shape(data) == manifest['members'][name], f'archive member differs: {name}')
    result = verify_members(members)
    require(result == json_bytes((root / 'decision.json').read_bytes()),
            'saved decision differs from independent replay')
    result['archive_members'] = len(members)
    return result

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('packet', type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(args.packet.resolve()), sort_keys=True, indent=2))

if __name__ == '__main__':
    main()
