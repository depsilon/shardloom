#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Replay provider acceptance from saved values, without running archived code."""
from __future__ import annotations

import argparse
from collections import Counter
import copy
from dataclasses import asdict
import gzip
import hashlib
import importlib.util
import io
import json
import math
from pathlib import Path
import re
import sys
import tarfile

from public_replay import verify as verify_public

ORIGINAL = Path('/Users/dylan/LocalData/shardloom/vortex-provider-refresh-20261024')
ORIGINAL_REPO = Path('/Users/dylan/.codex/worktrees/public-docs-api/shardloom-local-repo')
SOURCE_COMMIT = '4117846158edc7b1ab692ef90dd99ba686c5360c'
REFERENCE_SHA = 'fc5780da415dc33bd06ed0879db07fc040db5dd419e3c37ecde7cd40a315696b'
FINAL_GATES = {
    'clippy-workspace-native-090-r7': None,
    'test-workspace-native-090-r5': (6072, 0, 35),
    'clippy-workspace-default-090-r3': None,
    'test-workspace-default-090-r3': (3246, 0, 0),
    'fmt-workspace-090-r3': None,
    'check-msrv-native-090-r2': None,
    'check-msrv-no-default-090-r2': None,
    'check-workspace-all-features-090-r2': None,
    'test-provider-io-090-r2': (147, 0, 0),
    'test-provider-runend-090-r2': (133, 0, 0),
}
TEST_RESULT = re.compile(r'^test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out', re.M)


def require(value, message):
    if not value:
        raise ValueError(message)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(value):
    return digest(json.dumps(value, sort_keys=True, allow_nan=False).encode())


def check_current_source(tracked, repository):
    for name, sha in tracked.items():
        path = repository / name
        require(path.is_file(), 'current runtime source is missing: ' + name)
        require(digest(path.read_bytes()) == sha, 'current runtime source differs: ' + name)


def strict(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, 'duplicate JSON key: ' + key)
            result[key] = value
        return result

    def reject(value):
        raise ValueError('nonfinite JSON value: ' + value)

    def number(value):
        parsed = float(value)
        require(math.isfinite(parsed), 'nonfinite JSON number')
        return parsed
    return json.loads(raw, object_pairs_hook=unique, parse_constant=reject, parse_float=number)


class Packet:
    def __init__(self, root):
        self.manifest = strict((root / 'manifest.json').read_bytes())
        manifest = self.manifest
        require(manifest['schema'] == 'shardloom.native_provider_refresh.packet.v1', 'packet schema')
        require(manifest['status'] == 'complete' and set(manifest['verification_sources']) ==
                {'verify_packet.py', 'public_replay.py'}, 'verification source coverage')
        for name, expected in manifest['verification_sources'].items():
            require(Path(name).name == name, 'unsafe verifier path')
            require(digest((Path(__file__).parent / name).read_bytes()) == expected, 'verifier changed: ' + name)
        selection_ref = manifest['selection_manifest']
        require(Path(selection_ref['path']).name == selection_ref['path'], 'unsafe selection path')
        selection_packed = (root / selection_ref['path']).read_bytes()
        selection_raw = gzip.decompress(selection_packed)
        require((len(selection_packed), digest(selection_packed), len(selection_raw), digest(selection_raw)) ==
                (selection_ref['bytes'], selection_ref['sha256'], selection_ref['raw_bytes'], selection_ref['raw_sha256']),
                'original selection inventory identity')
        selection = strict(selection_raw)
        original = {(r['path'], r['sha256']): r for r in selection['files']}
        additions = manifest['primary_selection_additions']
        require(len(additions) == 81 and all('/public-r1/' in r['path'] for r in additions), 'historical case archive correction')
        for row in additions:
            key = row['path'], row['sha256']
            require(key not in original, 'duplicate selection correction')
            original[key] = row
        require(original == {(r['path'], r['sha256']): r for r in manifest['files']}, 'selected evidence coverage changed')
        self.selection = selection
        self.files, self.paths, self.objects = {}, {}, {}
        expected_objects = {}
        for row in manifest['files']:
            key = row['path'], row['sha256']
            require(key not in self.files and row['member'] == 'objects/' + row['sha256'], 'duplicate or invalid member identity')
            self.files[key] = row
            self.paths.setdefault(row['path'], []).append(key)
            previous = expected_objects.setdefault(row['member'], (row['bytes'], row['sha256']))
            require(previous == (row['bytes'], row['sha256']), 'inconsistent shared content')
        archive_names = set()
        for ref in manifest['archives']:
            require(Path(ref['path']).name == ref['path'] and ref['path'] not in archive_names, 'invalid archive name')
            archive_names.add(ref['path'])
            packed = (root / ref['path']).read_bytes()
            require(len(packed) == ref['bytes'] and digest(packed) == ref['sha256'], 'archive identity')
            with tarfile.open(fileobj=io.BytesIO(packed), mode='r:xz') as archive:
                for member in archive:
                    require(member.name in expected_objects and member.name not in self.objects, 'extra or duplicate archive member')
                    size, sha = expected_objects[member.name]
                    require(member.isfile() and member.size == size, 'archive member type/size')
                    raw = archive.extractfile(member).read()
                    require(len(raw) == size and digest(raw) == sha, 'archive member content')
                    self.objects[member.name] = raw
        require(set(self.objects) == set(expected_objects), 'archive omitted evidence')

    def raw(self, path):
        keys = self.paths.get(str(path), [])
        require(len(keys) == 1, 'missing or ambiguous evidence path: ' + str(path))
        return self.objects[self.files[keys[0]]['member']]

    def reference(self, ref):
        key = ref['path'], ref['sha256']
        require(key in self.files, 'missing exact reference: ' + ref['path'])
        row = self.files[key]
        require('bytes' not in ref or row['bytes'] == ref['bytes'], 'reference size')
        return self.objects[row['member']]

    def read(self, path):
        raw = self.raw(path)
        return strict(gzip.decompress(raw) if str(path).endswith('.gz') else raw)

    def linked(self, ref):
        raw = self.reference(ref)
        return strict(gzip.decompress(raw) if ref['path'].endswith('.gz') else raw)


def verify_sources(packet, repository, current):
    binding = packet.read(ORIGINAL / 'provider-source-binding-r1.json')
    build = packet.linked(binding['build'])
    require(binding['status'] == 'passed' and binding['source_commit'] == SOURCE_COMMIT, 'runtime source binding')
    require(build['status'] == 'passed' and build['source_before'] == build['source_after'], 'build source changed')
    require(binding['frozen_build_base'] == build['source_commit'], 'frozen build base')
    combined = {**build['source_after'], **build['runtime_python_sources']}
    tracked, generated = binding['tracked_source_files'], binding['generated_standalone_test_lockfiles']
    require(len(build['source_after']) == 2126 and len(build['runtime_python_sources']) == 21, 'build coverage')
    require(len(tracked) == 2139 and len(generated) == 8 and not tracked.keys() & generated.keys(), 'source ownership split')
    require(combined == {**tracked, **generated}, 'source binding omitted or changed files')
    for name, sha in generated.items():
        require(name.startswith('vendor/') and name.endswith('/Cargo.lock'), 'unexpected generated source')
        packet.reference({'path': str(ORIGINAL_REPO / name), 'sha256': sha})
    if current:
        check_current_source(tracked, repository)
    for name, sha in packet.manifest['helper_sources'].items():
        require(not Path(name).is_absolute() and '..' not in Path(name).parts, 'unsafe helper path')
        require(digest((repository / name).read_bytes()) == sha, 'trusted replay helper changed: ' + name)
        packet.reference({'path': str(ORIGINAL_REPO / name), 'sha256': sha})
    return build


def verify_validation(packet, build):
    catalog = packet.read(ORIGINAL / 'evidence/provider-validation-catalog-r5.json')
    require(catalog['status'] == 'COMPLETE' and not catalog['missing_logs'] and not catalog['log_identity_mismatches'], 'validation catalog incomplete')
    rows = catalog['receipts']
    require(len(rows) == 138 and sorted(row['receipt_path'] for row in rows) == catalog['eligible_receipt_paths'], 'validation receipt coverage')
    statuses, zero, gates = Counter(), [], {}
    for row in rows:
        ref = dict(path=row['receipt_path'], bytes=row['receipt_bytes'], sha256=row['receipt_sha256'])
        receipt = packet.linked(ref)
        require(receipt['status'] == row['status'] and receipt['command'] == row['command'], 'validation status/command')
        log = row['log_identity']
        raw = packet.reference(dict(path=log['resolved_path'], bytes=log['observed_bytes'], sha256=log['recorded_sha256']))
        require(digest(raw) == log['observed_sha256'], 'validation log identity')
        observed = TEST_RESULT.findall(raw.decode())
        expected = [tuple([line['result']] + [str(line[k]) for k in ('passed', 'failed', 'ignored', 'measured', 'filtered_out')]) for line in row['complete_test_result_lines']]
        require(observed == expected, 'test result transcript differs')
        if observed and sum(int(line[1]) + int(line[2]) for line in observed) == 0:
            require(row['test_result_classification'] == 'complete_zero_executed_test_result', 'zero executed tests disguised')
            zero.append(Path(ref['path']).name)
        statuses[receipt['status']] += 1
        label = Path(ref['path']).stem
        if label in FINAL_GATES:
            require(receipt['status'] == 'passed' and receipt['returncode'] == 0
                    and receipt['process_group_drained'] and receipt['direct_child_reaped'], 'final validation failed')
            require(receipt['source_before'] == receipt['source_after'] == build['source_after'], 'final gate source mismatch: ' + label)
            totals = tuple(sum(int(line[i]) for line in observed) for i in (1, 2, 3))
            require(FINAL_GATES[label] is None or totals == FINAL_GATES[label], 'final test counts')
            gates[label] = totals
    require(statuses == {'passed': 91, 'failed': 47} and set(gates) == set(FINAL_GATES), 'validation history or final gates omitted')
    require(sorted(zero) == ['test-fsst-activation-090-r1.json', 'test-writer-edition-090-r1.json'], 'filtered-test history omitted')
    return dict(receipts=len(rows), statuses=dict(statuses), final_source_gates=len(gates), zero_executed_historical_tests=zero)


def verify_outer(packet, name, build):
    receipt = packet.read(ORIGINAL / (name + '.json'))
    require(receipt['status'] == 'passed' and receipt['returncode'] == 0
            and receipt['process_group_drained'] and receipt['direct_child_reaped']
            and receipt['native_process_cleanup_verified'] and not receipt['timed_out'], 'acceptance process/cleanup')
    require(packet.linked(receipt['build']) == build and receipt['sources_verified'] == 2147, 'acceptance build binding')
    require(receipt['input_before'] == receipt['input_after'] and not receipt['publication_attempted'], 'acceptance input/publication')
    packet.reference(receipt['log'])
    for ref in receipt['helper_identities']:
        packet.reference(ref)
    return receipt


def verify_wheel(packet, build):
    verify_outer(packet, 'installed-native-wheel-090-r2', build)
    root = ORIGINAL / 'wheel-proof-r2'
    report = packet.read(root / 'report.json')
    require(report['status'] == 'passed' and not report['publication_attempted'] and not report['performance_claim'], 'wheel status')
    require(report['integration'] == packet.read(root / 'integration.json') == dict(tests=140, failures=0, errors=0, skipped=0, status='passed'), 'wheel complete integration')
    for section in ('stage', 'integrity'):
        require(report[section]['status'] == 'passed' and report[section]['published'] is False, 'wheel publication')
        require(report[section]['cli_sha256'] == build['artifacts']['cli']['sha256']
                and report[section]['native_library_sha256'] == build['artifacts']['library']['sha256'], 'wheel binary binding')
    require(report['installed_load']['isolated_install'] and report['uninstall_status'] == 'passed', 'wheel install/uninstall')
    require(len(report['steps']) == 8, 'wheel step omitted')
    for step in report['steps']:
        require(step['returncode'] == 0, 'wheel step failed')
        require(digest(packet.raw(root / step['log'])) == step['log_sha256'], 'wheel step log')
    return dict(tests=140, skipped=0, installed_and_removed=True)


def native_fields(envelope, report_fields, preparation=False):
    require(envelope['status'] == 'success' and envelope['fallback']['attempted'] is False
            and envelope['fallback']['engine'] is None, 'native envelope/fallback')
    if preparation:
        # The public vortex-prepare command has its own report namespace;
        # it does not emit the public SQL/workflow route prefix.
        rows = envelope['fields']
        require(isinstance(rows, list) and rows and all(isinstance(r['key'], str) for r in rows), 'preparation report fields')
        fields = {r['key']: r['value'] for r in rows}
        require(len(fields) == len(rows), 'duplicate preparation report fields')
    else:
        fields = report_fields(envelope)
    require(all(value in (False, 'false') for key, value in fields.items()
                if key.endswith(('fallback_attempted', 'external_engine_invoked'))), 'native fallback marker')
    return fields


def verify_ingest(packet, build, report_fields):
    report = packet.read(ORIGINAL / 'fresh-ingest-acceptance-r2.json')
    outer = verify_outer(packet, 'fresh-ingest-090-r2', build)
    require(report['status'] == 'passed' and packet.linked(report['build']) == build, 'ingest build')
    require(outer['input_before'] == [report['original_input']], 'ingest original identity')
    fields = native_fields(packet.linked(report['envelope']), report_fields)
    require(len(fields) == report['unique_report_fields'] == 554, 'ingest duplicate report fields')
    require(report['engine_reported_row_count'] == 99997497 and len(report['row_count_fields']) == 4, 'ingest row coverage')
    require(all(fields[key] == value == '99997497' for key, value in report['row_count_fields'].items()), 'ingest row count')
    require(fields['public_workflow_preparation_prepared_artifact_digest'] == 'sha256:' + report['target']['sha256'], 'ingest stream/file digest linkage')
    require(report['reopen_hot_path_status'] == 'skipped_minimal_ingest_digest_path'
            and report['independent_full_100m_row_readback_claimed'] is False, 'ingest readback scope')
    failed = packet.read(ORIGINAL / 'fresh-ingest-090-r1.json')
    require(failed['status'] == 'failed' and failed['returncode'] != 0, 'pre-engine failed ingest omitted')
    packet.linked(report['failed_pre_engine_attempt'])
    return report


def verify_full43(packet, build, ingest, helpers):
    extract_result, report_fields, equivalent, query_statements, native_query_statements = helpers
    report = packet.read(ORIGINAL / 'full43-acceptance-r1.json')
    require(report['status'] == 'passed' and report['complete_results'] == 43
            and report['complete_rows'] == 381 and report['exact_schema_continuity'] == 43
            and [r['query'] for r in report['records']] == list(range(1, 44)), 'Full43 acceptance coverage')
    outer = verify_outer(packet, 'full43-native-090-r1', build)
    require(outer['input_before'] == [ingest['target']], 'Full43 fresh input')
    current, previous = packet.linked(report['summary']), packet.linked(report['previous_summary'])
    reference = packet.linked(report['reference'])
    require(report['reference']['sha256'] == REFERENCE_SHA and reference['reference_kind'] == 'independent_reference', 'independent reference')
    query_bytes, statements = query_statements()
    submitted, dialect = native_query_statements(statements, digest(query_bytes))
    require(reference['queries_sha256'] == current['queries_sha256'] == digest(query_bytes) and current['query_dialect'] == dialect, 'Full43 workload identity')
    reference_rows = {r['query_id']: r for r in reference['records']}
    require(len(reference['records']) == 43 and set(reference_rows) == set(range(1, 44)), 'reference query coverage')
    for query, row in reference_rows.items():
        require(row['statement'] == statements[query - 1] and canonical(row['values']) == row['values_sha256'], 'reference statement/values')
    require(current['complete'] and current['full_result_validation'] and current['repeats'] == 1
            and current['selected_query_ids'] == list(range(1, 44)) and len(current['records']) == 43, 'Full43 complete isolated pass')
    require(current['completed_identity_check'] and current['reference_packet_identity_verified'] and current['harness_identity_verified'], 'Full43 identity completion')
    require(current['binary_sha256'] == build['artifacts']['cli']['sha256'] and current['source'] == ingest['target']['path']
            and current['source_bytes'] == ingest['target']['bytes'] and current['memory_gb'] == 24 and current['max_parallelism'] == 12, 'Full43 build/input/resources')
    require([current['source_generation'][k] for k in ('device', 'inode', 'size_bytes', 'mtime_ns', 'ctime_ns')]
            == ingest['target']['generation'], 'Full43 fresh source generation')
    require([r['query'] for r in current['records']] == list(range(1, 44)), 'Full43 duplicate/reordered query')
    old = {r['query']: r for r in previous['records']}
    recorded = {r['query']: r for r in report['records']}
    count, total = 0, 0.0
    for row in current['records']:
        query = row['query']
        require(row['passed'] and row['returncode'] == 0 and not row['guard_failures'] and row['run'] == 1
                and row['validation'] == 'complete_values', 'Full43 query completion')
        require(row['query_statement'] == statements[query - 1] and row['executed_query_statement'] == submitted[query - 1], 'Full43 submitted SQL')
        ref = recorded[query]['output']
        compressed = packet.reference(ref)
        raw = gzip.decompress(compressed)
        archive = next(x for x in row['completed_log_archives'] if x['path'] == ref['path'])
        require((len(compressed), digest(compressed), len(raw), digest(raw)) ==
                (archive['gzip_bytes'], archive['gzip_sha256'], archive['raw_bytes'], archive['raw_sha256']), 'Full43 compressed output')
        envelope = strict(raw)
        fields = native_fields(envelope, report_fields)
        values = extract_result(envelope)
        require(equivalent(values, reference_rows[query]['values']) and canonical(values) == row['result_sha256'], 'Full43 complete values')
        require(recorded[query]['complete_rows'] == len(values) and recorded[query]['result_sha256'] == row['result_sha256']
                and recorded[query]['schema_matches_accepted_runtime'] is True, 'Full43 per-query acceptance')
        old_envelope = packet.linked(recorded[query]['previous_output'])
        old_fields = native_fields(old_envelope, report_fields)
        require(strict(fields['result_schema_json']) == strict(old_fields['result_schema_json']), 'Full43 schema continuity')
        require(equivalent(extract_result(old_envelope), reference_rows[query]['values']), 'prior Full43 complete values')
        seconds = int(fields['native_query_execution_nanos']) / 1e9
        require(row['engine_timing_reported'] and seconds == row['engine_execution_seconds'] == recorded[query]['engine_seconds'], 'Full43 engine clock')
        require(fields['native_query_timing_clock'] == 'monotonic_wall_not_cpu'
                and fields['native_query_timing_reported'] == 'true', 'Full43 timing declaration')
        require(fields['native_query_timing_boundary'] ==
                'prepared_native_execution_including_io_generation_validation_and_certification_excluding_result_serialization',
                'Full43 engine timing boundary')
        require(old[query]['engine_execution_seconds'] == recorded[query]['previous_engine_seconds'], 'prior engine time')
        require(int(old_fields['native_query_execution_nanos']) / 1e9 == old[query]['engine_execution_seconds'], 'prior engine timing transcript')
        count += len(values)
        total += seconds
    require(count == 381 and total == current['query_total_seconds'] == report['engine_seconds'] == 49.625514041, 'Full43 totals')
    require(math.isclose(previous['query_total_seconds'], 48.701809680, rel_tol=0, abs_tol=1e-12)
            and report['performance_claim'] is False and report['additional_query_execution'] is False, 'Full43 claim scope')
    return dict(queries=43, rows=count, exact_schema_continuity=43, engine_seconds=total, previous_engine_seconds=previous['query_total_seconds'])


def option(command, name):
    require(command.count(name) == 1 and command.index(name) + 1 < len(command), 'command option: ' + name)
    return command[command.index(name) + 1]


def role_sources(paths, data_format, roles, inputs):
    result = {}
    for role in roles:
        if role == 'parts':
            parent = paths['fact_' + data_format.replace('-', '_') + '_parts_dir']
            extension = 'arrow' if data_format == 'arrow-ipc' else data_format
            members = sorted(p for p in inputs if Path(p).parent == Path(parent) and Path(p).match('part-*.' + extension))
        else:
            key = 'cdc_delta_csv' if role == 'delta' else role + '_' + data_format.replace('-', '_')
            members = [paths[key]]
        require(members and all(p in inputs for p in members), 'workload source outside retained input inventory')
        result[role] = [(p, 'csv' if role == 'delta' else data_format) for p in members]
    return result


def verify_modular_bindings(payload, packet, workloads, comparison_input_format, report_fields):
    config = payload['configuration']
    inputs = {x['path']: x for x in payload['inputs']}
    require(len(inputs) == len(payload['inputs']), 'duplicate modular input')
    jobs = {}
    for row in payload['records']:
        workload = workloads[row['scenario']]
        require(payload['workload_declarations'][row['scenario']] == json.loads(json.dumps(asdict(workload))), 'modular workload declaration')
        if row['engine'] == 'pandas':
            receipt = row['receipt']
            job = packet.linked(dict(path=receipt['job'], sha256=receipt['job_sha256']))
            fmt = comparison_input_format(row['format'])
            require(job['operation'] == 'baseline' and job['engine'] == 'pandas' and job['scenario'] == row['scenario']
                    and job['format'] == row['comparison_input_format'] == fmt, 'modular oracle job')
            role_sources(job['paths'], fmt, workload.source_roles, inputs)
            jobs[row['format'], row['scenario']] = job
            continue
        evidence = row['evidence']
        require(evidence['benchmark_request_protocol'] == 'public_native_workflow' and evidence['benchmark_query_answer_cached'] == 'false', 'modular execution policy')
        require(evidence['benchmark_input_state'] == config['input_state'] and evidence['benchmark_output_format'] == config['output_format'], 'modular modes')
        preparations = strict(evidence['benchmark_input_preparation_calls'])
        targets = {}
        for receipt in preparations:
            command = receipt['command']
            require(command[1] == 'vortex-prepare' and command[2] in inputs, 'modular preparation source')
            require(receipt['returncode'] == 0 and not receipt['guard_failures']
                    and receipt['binary_sha256'] == payload['binary']['sha256'], 'modular preparation execution')
            native_fields(packet.linked(dict(path=receipt['stdout'], sha256=receipt['stdout_sha256'])), report_fields, preparation=True)
            key = command[2], option(command, '--input-format')
            require(key not in targets, 'duplicate modular preparation')
            targets[key] = command[3]
        roles = role_sources(jobs[row['format'], row['scenario']]['paths'], row['format'], workload.source_roles, inputs)
        expressions, expected_bindings = {}, {}
        for role, members in roles.items():
            refs = []
            for source, fmt in members:
                if config['input_state'] == 'prepared':
                    source = source if fmt == 'vortex' else targets[source, fmt]
                    fmt = 'vortex'
                expected_bindings[source] = fmt
                refs.append("'" + source.replace("'", "''") + "'")
            expressions[role] = ('(' + ' UNION ALL '.join('SELECT * FROM ' + x for x in refs) + ') AS parts') if role == 'parts' else refs[0]
        statements, writer = workload.bind(expressions)
        declarations = ([writer] if writer is not None else []) + list(statements)
        calls, readbacks = strict(evidence['benchmark_native_calls']), strict(evidence['benchmark_output_validation_calls'])
        require(list(statements) == strict(evidence['benchmark_sql_declarations'])
                and declarations == [option(call['command'], '--sql') for call in calls], 'modular SQL composition')
        write_index = 0
        for i, call in enumerate(calls):
            command = call['command']
            require(command[1:3] == ['run', 'sql'], 'modular public entrypoint')
            bindings = strict(option(command, '--source-bindings'))
            require({p: v['input_format'] for p, v in bindings.items()} == expected_bindings, 'modular source bindings')
            fmt = 'csv' if writer is not None and i == 0 else config['output_format']
            require(option(command, '--request') == ('collect' if fmt == 'collect' else 'write_' + fmt), 'modular output request')
            if fmt == 'collect':
                require('output' not in call, 'collect secretly writes output')
                continue
            output = call['output']
            fields = native_fields(packet.linked(dict(path=call['stdout'], sha256=call['stdout_sha256'])), report_fields)
            require(fields['native_vortex_result_export_all_targets_committed'] == 'true'
                    and fields['native_vortex_result_export_path'] == output['path'] == option(command, '--output'), 'modular publication')
            readback = readbacks[write_index]
            write_index += 1
            if fmt in ('json', 'jsonl'):
                require(readback['kind'] == 'complete_json_file_readback' and readback['format'] == fmt
                        and all(readback[k] == value for k, value in output.items()), 'modular JSON readback identity')
            else:
                command = readback['command']
                bindings = strict(option(command, '--source-bindings'))
                require(set(bindings) == {output['path']} and bindings[output['path']]['input_format'] == fmt
                        and option(command, '--request') == 'collect', 'modular readback binds written output')
        require(write_index == len(readbacks), 'modular unexpected readbacks')


def verify_modular(packet, build, helpers, verifier, comparison_input_format):
    report = packet.read(ORIGINAL / 'modular-r1/provider-native-matrix-2-recovered-report.json')
    require(report['status'] == 'passed' and report['accepted_records'] == 1408 and report['native_records'] == report['independent_oracle_records'] == 704, 'modular totals')
    require(packet.linked(report['build']) == build and len(report['accepted_cohorts']) == 18, 'modular build/cohorts')
    frozen = dict(binary=build['artifacts']['cli']['path'], binary_sha256=build['artifacts']['cli']['sha256'])
    records, calls, modes = 0, 0, []
    for cohort in report['accepted_cohorts']:
        payload = packet.linked(cohort['report'])
        count, called = verifier.verify_record_values(payload, packet, frozen, helpers)
        require(count == cohort['records'] and sum(r['engine'] == 'shardloom' for r in payload['records']) == cohort['native_records'], 'modular cohort records')
        verify_modular_bindings(payload, packet, helpers[3], comparison_input_format, helpers[2])
        records += count
        calls += called
        modes.append((payload['configuration']['input_state'], payload['configuration']['output_format']))
    require(records == 1408 and len(set(modes)) == 18
            and set(modes) == {(state, fmt) for state in ('raw', 'prepared') for fmt in ('collect', 'vortex', 'json', 'jsonl', 'csv', 'parquet', 'arrow_ipc', 'avro', 'orc')}, 'modular mode coverage')
    original = packet.linked(report['original_interrupted_matrix'])
    compacted = packet.linked(report['log_compaction'])
    require(original['status'] == 'failed' and compacted['status'] == 'passed' and compacted['complete_member_readback'], 'modular interrupted history')
    require(compacted['log_limit_bytes'] == 192 * (1 << 20) and len(compacted['completed_cohorts_revalidated']) == 17, 'modular unchanged storage/reuse')
    require(report['prior_partial_records_preserved'] == 25 and report['accepted_cohorts'][-1]['records'] == 44, 'modular final cohort recovery')
    require(report['performance_claim'] is False and report['complete_cartesian_input_output_coverage_claimed'] is False, 'modular bounded claim')
    return dict(records=records, native_records=704, independent_records=704, cohorts=18, command_values_checked=calls)


def load_module(path):
    spec = importlib.util.spec_from_file_location('provider_replay_modular', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def corruption_controls(packet, build, ingest, full_helpers, modular_helpers, modular_verifier, comparison_input_format):
    """Bypass archive integrity deliberately to exercise semantic rejection."""
    outcomes = []

    def rejects(name, action, expected):
        try:
            action()
        except (ValueError, AssertionError, KeyError) as error:
            require(expected in str(error), 'wrong corruption rejection for ' + name + ': ' + str(error))
            outcomes.append(name)
        else:
            raise ValueError('corruption was accepted: ' + name)

    def changed(path, mutation):
        replica = copy.copy(packet)
        replica.objects = packet.objects.copy()
        value = copy.deepcopy(packet.read(path))
        mutation(value)
        key, = packet.paths[str(path)]
        replica.objects[packet.files[key]['member']] = json.dumps(value).encode()
        return replica

    path = ORIGINAL / 'test-workspace-native-090-r5.json'
    bad = changed(path, lambda value: value['source_after'].__setitem__('Cargo.lock', 'changed'))
    rejects('final_gate_source_mismatch', lambda: verify_validation(bad, build), 'final gate source mismatch')
    path = ORIGINAL / 'wheel-proof-r2/report.json'
    bad = changed(path, lambda value: value['integration'].__setitem__('skipped', 1))
    rejects('wheel_skipped_test', lambda: verify_wheel(bad, build), 'wheel complete integration')
    path = ORIGINAL / 'full43-acceptance-r1.json'
    bad = changed(path, lambda value: value['records'].pop())
    rejects('omitted_full43_result', lambda: verify_full43(bad, build, ingest, full_helpers), 'Full43 acceptance coverage')
    bad = changed(path, lambda value: value['records'][0].__setitem__('engine_seconds', 100))
    rejects('substituted_engine_clock', lambda: verify_full43(bad, build, ingest, full_helpers), 'Full43 engine clock')
    path = ORIGINAL / 'fresh-ingest-acceptance-r2.json'
    bad = changed(path, lambda value: value.__setitem__('independent_full_100m_row_readback_claimed', True))
    rejects('expanded_ingest_readback_claim', lambda: verify_ingest(bad, build, full_helpers[1]), 'ingest readback scope')
    report = packet.read(ORIGINAL / 'modular-r1/provider-native-matrix-2-recovered-report.json')
    payload = packet.linked(report['accepted_cohorts'][0]['report'])
    frozen = dict(binary=build['artifacts']['cli']['path'], binary_sha256=build['artifacts']['cli']['sha256'])
    bad_payload = copy.deepcopy(payload)
    row = next(r for r in bad_payload['records'] if r['engine'] == 'shardloom')
    row['result']['row_count'] += 1
    row['result_sha256'] = canonical(row['result'])
    rejects('modular_changed_complete_value', lambda: modular_verifier.verify_record_values(
        bad_payload, packet, frozen, modular_helpers), 'native/oracle values differ')
    bad_sql = copy.deepcopy(payload)
    row = next(r for r in bad_sql['records'] if r['engine'] == 'shardloom')
    declarations = strict(row['evidence']['benchmark_sql_declarations'])
    declarations[0] += ' LIMIT 0'
    row['evidence']['benchmark_sql_declarations'] = json.dumps(declarations)
    rejects('modular_changed_sql_declaration', lambda: verify_modular_bindings(
        bad_sql, packet, modular_helpers[3], comparison_input_format, modular_helpers[2]), 'modular SQL composition')
    public_path = ORIGINAL / 'public-r2/summary.json'
    summary = packet.read(public_path)
    case = next(c for c in summary['cases'] if isinstance(c.get('complete_rows'), dict))
    missing = copy.copy(packet)
    missing.objects = packet.objects.copy()
    key, = missing.paths[str(public_path.parent / case['complete_rows']['path'])]
    missing.objects.pop(missing.files[key]['member'])
    rejects('missing_case_level_rows_archive', lambda: verify_public(public_path, missing.raw), 'objects/')
    return outcomes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packet', type=Path, default=Path(__file__).parent)
    parser.add_argument('--repository', type=Path, default=Path(__file__).resolve().parents[4])
    parser.add_argument('--check-current-source', action='store_true')
    parser.add_argument('--self-test', action='store_true')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    require(not sys.flags.optimize, 'run verification without Python optimization')
    repository = args.repository.resolve()
    packet = Packet(args.packet.resolve())
    build = verify_sources(packet, repository, args.check_current_source)
    sys.path[:0] = [str(repository / 'scripts'), str(repository / 'python/src'), str(repository)]
    from native_workflow_protocol import extract_result, report_fields
    from run_clickbench_query_uat import equivalent
    from clickbench_reference_packet import query_statements
    from clickbench_native_sql import native_query_statements
    from benchmarks.traditional_analytics.workloads import WORKLOADS
    from benchmarks.traditional_analytics.comparison import round_float
    from benchmarks.traditional_analytics.benchmark_models import comparison_input_format
    modular_verifier = load_module(repository / 'docs/benchmarks/evidence/native-structural-provider-2026-10-21/verify_acceptance.py')
    validation = verify_validation(packet, build)
    wheel = verify_wheel(packet, build)
    publics = {}
    for name in ('public-r1', 'public-r2'):
        result = verify_public(ORIGINAL / name / 'summary.json', packet.raw)
        require(result['cases'] == 5930 and result['families'] == 39
                and result['counts']['compressed_json'] == 16389
                and result['counts']['complete_value_files'] == 5639
                and result['counts']['retained_value_archives'] == 56
                and result['counts']['case_value_archives'] == 81, 'public suite coverage')
        publics[name] = {k: result[k] for k in ('families', 'cases', 'counts')}
    verify_outer(packet, 'public-native-090-r2', build)
    modular_helpers = (equivalent, extract_result, report_fields, WORKLOADS, round_float)
    modular = verify_modular(packet, build, modular_helpers, modular_verifier, comparison_input_format)
    ingest = verify_ingest(packet, build, report_fields)
    full_helpers = (extract_result, report_fields, equivalent,
                    lambda: query_statements(repository / 'benchmarks/clickbench/queries.sql'), native_query_statements)
    full = verify_full43(packet, build, ingest, full_helpers)
    controls = corruption_controls(packet, build, ingest, full_helpers, modular_helpers,
                                   modular_verifier, comparison_input_format) if args.self_test else []
    result = dict(status='passed', source_commit=SOURCE_COMMIT, frozen_build_base=build['source_commit'],
                  verifier_sha256=digest(Path(__file__).read_bytes()),
                  verification_sources=packet.manifest['verification_sources'],
                  packet_manifest_sha256=digest((args.packet / 'manifest.json').read_bytes()),
                  source_files=2147, current_source_checked=args.check_current_source,
                  validation=validation, wheel=wheel, public=publics, modular=modular,
                  fresh_ingest_rows=99997497, full43=full,
                  corruption_controls_rejected=controls,
                  execution_scope='saved complete values and identity replay; no engine, build or archived code execution',
                  bulk_payloads_rehashed=False, historical_failed_runs_reclassified=False,
                  public_inventory_stale_coverage_annotations_used=False)
    if args.output:
        require(not args.output.exists(), 'preserve prior verification output')
        args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
