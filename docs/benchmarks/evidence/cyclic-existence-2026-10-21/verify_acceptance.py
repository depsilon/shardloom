# SPDX-License-Identifier: Apache-2.0
"""Replay compact cyclic integration results without native execution or resident datasets."""
import argparse
import base64
from collections import Counter
from decimal import Decimal
import gzip
import hashlib
import importlib.util
import json
import lzma
from pathlib import Path
import re
import subprocess
import sys


def digest(data):
    return hashlib.sha256(data).hexdigest()


def require(value, detail):
    if not value:
        raise ValueError(detail)


def canonical(value):
    return digest(json.dumps(value, sort_keys=True, allow_nan=False).encode())


class Evidence:
    def __init__(self, packet):
        require(packet['schema'] == 'shardloom.cyclic_existence_integration_packet.v1', 'changed packet schema')
        self.members = packet['members']
        self.omitted = {row['path']: row for row in packet['omitted_payloads']}
        require(len(self.omitted) == len(packet['omitted_payloads']), 'duplicate omitted identity')
        require(not (set(self.members) & set(self.omitted)), 'payload both archived and omitted')
        for name, row in self.members.items():
            raw = self.raw(name)
            require(len(raw) == row['bytes'] and digest(raw) == row['sha256'], 'changed member: ' + name)

    def raw(self, path):
        row = self.members[str(path)]
        require(row['encoding'] in ('utf8', 'base64'), 'unknown member encoding')
        return row['data'].encode() if row['encoding'] == 'utf8' else base64.b64decode(row['data'], validate=True)

    def read(self, path):
        raw = self.raw(path)
        return json.loads(gzip.decompress(raw) if str(path).endswith('.gz') else raw)

    def check(self, ref):
        row = self.members.get(ref['path'], self.omitted.get(ref['path']))
        require(row is not None, 'missing referenced identity: ' + ref['path'])
        require(all(row[key] == ref[key] for key in ('bytes', 'sha256')), 'substituted identity: ' + ref['path'])
        return ref['path']

    def references(self, value):
        if isinstance(value, dict):
            if set(value) == {'path', 'bytes', 'sha256'} and Path(value['path']).is_absolute():
                self.check(value)
            else:
                for child in value.values():
                    self.references(child)
        elif isinstance(value, list):
            for child in value:
                self.references(child)


def verify_git_sources(repository, revision, sources):
    names = sorted(sources)
    require(names and all('\n' not in name and not Path(name).is_absolute() and '..' not in Path(name).parts
                          for name in names), 'invalid source closure')
    process = subprocess.Popen(['git', 'cat-file', '--batch'], cwd=repository,
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    try:
        for name in names:
            process.stdin.write(f'{revision}:{name}\n'.encode())
            process.stdin.flush()
            header = process.stdout.readline().split()
            require(len(header) == 3 and header[1] == b'blob', 'missing Git source: ' + name)
            raw = process.stdout.read(int(header[2]))
            require(process.stdout.read(1) == b'\n' and digest(raw) == sources[name], 'changed Git source: ' + name)
    finally:
        process.stdin.close()
        process.stdout.close()
        require(process.wait(timeout=20) == 0, 'Git source verification failed')


def verify_record_values(payload, evidence, build, helpers):
    equivalent, extract_result, report_fields, workloads, round_float = helpers
    require(payload['status'] == 'passed' and payload['independent_reference'] is True
            and payload['performance_claim'] is False and not payload['query_answers_cached_by_native_adapter'],
            'invalid modular report')
    config = payload['configuration']
    require(config['repeats'] == 1 and payload['reference_engine'] == 'pandas'
            and set(payload['engines']) == {'pandas', 'shardloom'}, 'changed modular roles')
    require(config['scenarios'] == list(workloads) and config['rows'] == 1000
            and config['dim_rows'] == 20 and config['memory_gb'] == 1
            and config['max_parallelism'] == 2, 'changed modular workload or resources')
    expected = {(engine, data_format, scenario, 1) for engine in payload['engines']
                for data_format in config['formats'] for scenario in config['scenarios']}
    rows = payload['records']
    actual = [(row['engine'], row['format'], row['scenario'], row['repeat']) for row in rows]
    require(len(actual) == len(expected) and set(actual) == expected, 'modular coverage changed')
    reference = {(row['format'], row['scenario']): row['result'] for row in rows if row['engine'] == 'pandas'}
    calls_verified = 0

    def call(item, native):
        nonlocal calls_verified
        require(item['returncode'] == 0 and not item['guard_failures'], 'failed modular process')
        raw = evidence.raw(item['stdout'])
        require(digest(raw) == item['stdout_sha256'], 'changed modular process output')
        envelope = evidence.read(item['stdout'])
        if native:
            require(item['binary_sha256'] == build['binary_sha256'] and item['command'][0] == build['binary'],
                    'substituted modular native binary')
            require(envelope['status'] == 'success' and envelope['fallback']['attempted'] is False
                    and envelope['fallback']['engine'] is None, 'unsafe modular envelope')
            fields = report_fields(envelope)
            require(all(value in (False, 'false') for key, value in fields.items()
                        if key.endswith(('fallback_attempted', 'external_engine_invoked'))), 'unsafe modular fields')
        else:
            require(envelope['status'] == 'passed' and envelope['execution_role'] == 'independent_comparison_only',
                    'missing independent modular reference')
        calls_verified += 1
        return envelope

    for row in rows:
        require(row['status'] == 'passed' and row['matches_reference'] is True
                and canonical(row['result']) == row['result_sha256'], 'changed modular complete result')
        require(equivalent(row['result'], reference[row['format'], row['scenario']]), 'native/reference values differ')
        if row['engine'] == 'pandas':
            result = call(row['receipt'], False)
            require(canonical(result['result']) == row['result_sha256'], 'independent process result differs')
            continue
        fields = row['evidence']
        require(fields['public_workflow_fallback_attempted'] == fields['public_workflow_external_engine_invoked'] == 'false',
                'modular fallback marker changed')
        calls = json.loads(fields['benchmark_native_calls'])
        readbacks = json.loads(fields['benchmark_output_validation_calls'])
        workload = workloads[row['scenario']]
        first = int(workload.write_statement is not None)
        require(len(calls) == len(workload.statements) + first, 'missing complete native workflow calls')
        batches, writer = [], 0
        for index, item in enumerate(calls):
            envelope = call(item, True)
            if 'output' in item:
                require(writer < len(readbacks), 'missing output readback')
                readback = readbacks[writer]
                writer += 1
                if readback.get('kind') == 'complete_json_file_readback':
                    raw = evidence.raw(readback['path'])
                    require(digest(raw) == readback['sha256'] and len(raw) == readback['bytes'], 'changed JSON output')
                    result = json.loads(raw) if readback['format'] == 'json' else [json.loads(line) for line in raw.splitlines() if line]
                else:
                    result = extract_result(call(readback, True))
            else:
                result = extract_result(envelope)
            if index >= first:
                batches.append(result)
        require(writer == len(readbacks), 'extra output readback')
        require(equivalent(workload.result(batches, round_float), row['result']), 'native call payload differs from complete result')
    return len(rows), calls_verified


def replay(repository, manifest, packet):
    evidence = Evidence(packet)
    final = evidence.read(manifest['final_acceptance']['path'])
    evidence.check(manifest['final_acceptance'])
    require(final['status'] == 'passed' and final['finite_runtime_accepted']
            and final['outcome'] == 'finite_cyclic_existence_join_retained', 'finite acceptance missing')
    require(final['public_cases'] == 5930 and final['installed_binding_tests'] == 115
            and not final['publication_attempted'] and not final['competitive_claim']
            and not final['broader_owner_complete'] and not final['completed_roadmap_substeps'], 'scope changed')
    evidence.references(final)
    source = evidence.read(final['source']['path'])
    build = evidence.read(final['build']['path'])
    bridge = evidence.read(final['package_metadata_bridge']['path'])
    plan = evidence.read(bridge['original_protocol']['path'])
    evidence.references(bridge)
    require(final['source_commit'] == bridge['source_commit']
            and final['compiled_source_commit'] == bridge['compiled_source_commit'] == build['source_commit'] == source['source_commit'],
            'source bridge changed')
    require(build['source_hashes'] == source['source_hashes'] and build['cli_compiler_artifact']['fresh'] is False
            and build['compiled_runtime_unchanged_from_scored_candidate'], 'runtime provenance changed')
    require(bridge['runtime_and_lockfile_unchanged'] and bridge['scored_libraries_unchanged']
            and bridge['workloads_gates_and_resources_unchanged'], 'metadata bridge widened')
    require({name for name in source['source_hashes'] if source['source_hashes'][name] != bridge['source_hashes'][name]}
            == {'python/THIRD_PARTY_LICENSES/manifest.json'}, 'non-notice source drift')
    verify_git_sources(repository, source['source_commit'], source['source_hashes'])
    verify_git_sources(repository, bridge['source_commit'], bridge['source_hashes'])
    comparison_dir = repository / 'docs/benchmarks/evidence/cyclic-existence-2026-10-21'
    comparison_original = Path(final['comparison']['path']).parent
    for name in ('comparison.json', 'comparison.json.xz', 'intake.json', 'intake.json.xz',
                 'verify_comparison.py', 'verify.py'):
        require((comparison_dir / name).read_bytes() == evidence.raw(comparison_original / name), 'comparison dependency changed')
    spec = importlib.util.spec_from_file_location('cyclic_comparison_archive_replay', comparison_dir / 'verify_comparison.py')
    comparison_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(comparison_module)
    comparison = evidence.read(comparison_original / 'comparison.json')
    comparison_packet = json.loads(lzma.decompress(evidence.raw(comparison_original / 'comparison.json.xz')))
    require(comparison_module.replay(comparison, comparison_packet) == comparison['replay']
            and comparison['replay']['retention_gate_passed'], 'finite comparison failed replay')
    candidate = json.loads(comparison_packet['members']['cyclic-existence-candidate-build-852.json']['text'])
    require(build['native_library'] == candidate['library'], 'integrated native library differs from scored candidate')
    sys.path[:0] = [str(repository / 'scripts'), str(repository)]
    from native_workflow_protocol import extract_result, report_fields
    from run_clickbench_query_uat import equivalent, score
    from query_timing_report import query_phase_timings, timing_totals
    from benchmarks.traditional_analytics.workloads import WORKLOADS
    from benchmarks.traditional_analytics.comparison import round_float
    from benchmarks.traditional_analytics.benchmark_models import FORMAT_ORDER
    for name in ('scripts/native_workflow_protocol.py', 'scripts/run_clickbench_query_uat.py',
                 'scripts/query_timing_report.py', 'benchmarks/traditional_analytics/workloads.py',
                 'benchmarks/traditional_analytics/comparison.py', 'benchmarks/traditional_analytics/benchmark_models.py'):
        require(digest((repository / name).read_bytes()) == source['source_hashes'][name], 'replay helper changed: ' + name)
    owner10 = evidence.read(final['perf10']['path'])
    owner12 = evidence.read(final['perf12']['path'])
    evidence.references(owner10)
    evidence.references(owner12)
    require(owner10['status'] == owner12['status'] == 'passed', 'owner acceptance missing')
    correction = evidence.read(owner12['dependency_environment_correction']['path'])
    evidence.references(correction)
    require(correction['original_matrix_records'] == correction['original_matrix_queries_executed'] == 0
            and correction['runtime_workload_resources_reference_policy_and_gates_unchanged']
            and correction['original_protocol'] == bridge['original_protocol'], 'dependency correction widened')
    require({row['name']: row['version'] for row in correction['environment']['packages']}
            == {'pandas': '2.2.3', 'numpy': '2.3.5', 'pyarrow': '25.0.1', 'fastavro': '1.12.2'},
            'dependency identity changed')
    accepted = {phase: evidence.read(ref['path']) for phase, ref in owner10['acceptance'].items()}
    for phase, value in accepted.items():
        evidence.references(value)
        require(value['status'] == 'passed' and value['phase'] == phase and value['source_commit'] == build['source_commit']
                and value['package_source_commit'] == bridge['source_commit'] and not value['performance_claim'], 'invalid acceptance report')
    public = evidence.read(accepted['public']['summary']['path'])
    require(public['status'] == 'passed' and len(public['families']) == 39 and public['families'] == plan['families']
            and len(public['cases']) == 5930 and Counter(row['status'] for row in public['cases']) == {'passed': 5930},
            'incomplete public coverage')
    public_root = str(Path(accepted['public']['summary']['path']).parent)
    for row in public['compressed_json_files']:
        raw = evidence.raw(public_root + '/' + row['path'])
        require(len(raw) == row['bytes'] and digest(raw) == row['sha256'], 'changed compressed public evidence')
        data = gzip.decompress(raw)
        require(len(data) == row['raw_bytes'] and digest(data) == row['raw_sha256'], 'changed public payload')
        value = json.loads(data)
        if isinstance(value, dict) and 'fallback' in value:
            require(value['fallback']['attempted'] is False and value['fallback']['allowed'] is False
                    and value['fallback']['engine'] is None, 'unsafe public envelope')
    full = evidence.read(accepted['full43']['summary']['path'])
    require(full['complete'] and full['full_result_validation'] and full['repeats'] == 1
            and full['selected_query_ids'] == list(range(1, 44)) and len(full['records']) == 43
            and full['hot_total_seconds'] is None and full['reference_kind'] == 'retained_native_regression', 'incomplete Full43')
    require({(row['query'], row['run']) for row in full['records']} == {(query, 1) for query in range(1, 44)},
            'Full43 queries duplicated or omitted')
    require(full['binary_sha256'] == build['binary_sha256'] and full['build_commit'] == build['source_commit']
            and full['source'] == plan['input']['path'] and full['source_generation'] == plan['input']['generation']
            and full['completed_identity_check'] and full['reference_packet_identity_verified'] and full['harness_identity_verified'],
            'Full43 provenance changed')
    reference = evidence.read(plan['reference']['path'])
    require(reference['queries_sha256'] == digest(evidence.raw(plan['query_source']['path'])), 'changed pinned query set')
    references = {row['query_id']: row for row in reference['records']}
    require(len(references) == 43 and set(references) == set(range(1, 44)), 'reference coverage changed')
    schemas = {int(query): tuple(value) for ref in plan['schema_sources']
               for query, value in evidence.read(ref['path'])['expected_schemas'].items()}
    nanos, count = 0, 0
    for row in full['records']:
        require(row['passed'] and row['validation'] == 'complete_values' and row['run'] == 1
                and row['returncode'] == 0 and not row['guard_failures'], 'invalid Full43 row')
        items = [item for item in row['completed_log_archives'] if item['path'].endswith('.stdout.json.gz')]
        require(len(items) == 1, 'Full43 output missing')
        item = items[0]
        packed = evidence.raw(item['path'])
        raw = gzip.decompress(packed)
        require(len(packed) == item['gzip_bytes'] and digest(packed) == item['gzip_sha256']
                and len(raw) == item['raw_bytes'] and digest(raw) == item['raw_sha256'], 'changed Full43 archive')
        envelope = json.loads(raw)
        fields = report_fields(envelope)
        require(envelope['status'] == 'success' and not envelope['fallback']['attempted'], 'Full43 execution failed')
        require(all(value in (False, 'false') for key, value in fields.items()
                    if key.endswith(('fallback_attempted', 'external_engine_invoked'))), 'unsafe Full43 fields')
        actual = extract_result(envelope)
        expected = references[row['query']]
        require(canonical(expected['values']) == expected['values_sha256'] and equivalent(actual, expected['values'])
                and canonical(actual) == row['result_sha256'], 'Full43 values changed')
        require((fields['result_schema_format'], json.loads(fields['result_schema_json'])) == schemas[row['query']], 'Full43 schema changed')
        require(all(row[key] == value for key, value in query_phase_timings(fields, row['seconds']).items()), 'Full43 clocks changed')
        require(any(item['kind'] == 'native_io_certificate' and item['status'] == 'available' for item in envelope['certificates']),
                'Full43 native certificate absent')
        nanos += int(fields['native_query_execution_nanos'])
        count += len(actual)
    require(count == 381 and str(Decimal(nanos) / Decimal(1_000_000_000)) == final['full43_engine_execution_seconds'], 'Full43 total changed')
    require(all(full[key] == value for key, value in score(full['records'], 43, repeats=1, timing_key='engine_execution_seconds').items()),
            'Full43 score changed')
    require(full['timing_totals'] == timing_totals(full['records']), 'Full43 clock totals changed')
    matrix = evidence.read(accepted['modular']['matrix']['path'])
    require(matrix['status'] == 'passed' and len(matrix['cohorts']) == 18 and matrix['records'] == 1408
            and matrix['native_records'] == 704 and not matrix['complete_cartesian_input_output_coverage_claimed'], 'modular matrix incomplete')
    require(matrix['binary_sha256'] == build['binary_sha256'] and matrix['binary'] == build['binary']
            and matrix['runtime_commit'] == build['source_commit'] and matrix['harness_commit'] == bridge['source_commit'],
            'modular provenance changed')
    expected_cohorts = [(state, 'collect', list(FORMAT_ORDER)) for state in ('raw', 'prepared')]
    expected_cohorts += [(state, output, ['csv']) for state in ('raw', 'prepared')
                         for output in ('vortex', 'json', 'jsonl', 'csv', 'parquet', 'arrow_ipc', 'avro', 'orc')]
    require([(row['input_state'], row['output_format'], row['formats']) for row in matrix['cohorts']] == expected_cohorts,
            'modular format/lifecycle coverage changed')
    total, call_count = 0, 0
    for row in matrix['cohorts']:
        require(digest(evidence.raw(row['report'])) == row['report_sha256'] and row['independently_reopened'], 'changed modular report')
        payload = evidence.read(row['report'])
        require(all(payload['configuration'][key] == row[key]
                    for key in ('input_state', 'output_format', 'formats')), 'modular cohort/report mismatch')
        records, calls = verify_record_values(payload, evidence, build,
                                             (equivalent, extract_result, report_fields, WORKLOADS, round_float))
        require(records == row['observed_records'], 'modular cohort count changed')
        total += records
        call_count += calls
    require(total == 1408, 'modular record count changed')
    wheel = evidence.read(owner10['installed_python']['path'])
    require(wheel['status'] == wheel['uninstall_status'] == 'passed'
            and wheel['integration'] == dict(status='passed', tests=115, failures=0, errors=0, skipped=0)
            and wheel['installed_load']['isolated_install'] and not wheel['publication_attempted'], 'installed proof incomplete')
    require(wheel['integrity']['native_library_sha256'] == build['native_library']['sha256']
            and wheel['integrity']['cli_sha256'] == build['binary_sha256'], 'installed binaries changed')
    wheel_root = Path(owner10['installed_python']['path']).parent
    for row in wheel['steps']:
        require(row['returncode'] == 0 and digest(evidence.raw(wheel_root / row['log'])) == row['log_sha256'], 'package step failed')
    guards, failed = 0, 0
    for name in evidence.members:
        if not name.endswith('.json') or name.endswith('.stdout.json') or Path(name).parent != Path(manifest['final_acceptance']['path']).parent:
            continue
        value = evidence.read(name)
        if isinstance(value, dict) and 'driver_sha256' in value and 'process_group_drained' in value:
            require(value['status'] in ('passed', 'failed') and value['process_group_drained'] and value['direct_child_reaped'], 'undrained guard')
            require(digest(evidence.raw(name.removesuffix('.json') + '.log')) == value['log_sha256'], 'guard log changed')
            guards += 1
            failed += int(value['status'] == 'failed')
    require(failed >= 2, 'failed package or modular setup attempt omitted')
    return dict(status='passed', members=len(evidence.members), public_cases=5930, public_families=39,
                full43_queries=43, full43_complete_rows=count, full43_engine_execution_seconds=final['full43_engine_execution_seconds'],
                modular_records=total, modular_payload_calls_replayed=call_count, installed_tests=115,
                guarded_receipts=guards, preserved_failed_guards=failed, source_files=len(bridge['source_hashes']),
                native_execution_performed=False, payload_residency_revalidated=False, publication_attempted=False,
                scope='Archived complete outputs, reference comparisons, source and receipt integrity; original artifact-residency checks remain recorded.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_bytes())
    path = args.manifest.parent / manifest['packet']['file']
    data = path.read_bytes()
    require(len(data) == manifest['packet']['bytes'] and digest(data) == manifest['packet']['sha256'], 'packet identity changed')
    result = replay(args.repository.resolve(), manifest, json.loads(lzma.decompress(data)))
    require(result == manifest['replay'], 'replay summary changed')
    if args.output:
        with args.output.open('x') as stream:
            json.dump(result, stream, indent=2, sort_keys=True)
            stream.write('\n')
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
