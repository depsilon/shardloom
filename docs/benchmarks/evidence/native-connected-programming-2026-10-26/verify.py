#!/usr/bin/env python3
"""Offline connected-programming proof; never extracts or executes packet code."""
from __future__ import annotations

import hashlib
import io
import json
import lzma
import math
from pathlib import Path, PurePosixPath
import re
import sys
import tarfile
import zlib

MAX_FILE = 16 << 20
MAX_EXPANDED = 96 << 20
MAX_TAR = MAX_EXPANDED + (4 << 20)
MAX_OBJECTS = 2048
PREFIX = '/Users/dylan/LocalData/shardloom/connected-programming-2026-10-26/'
REFERENCE = '/Users/dylan/LocalData/shardloom/native-prerequisites-20261022/independent-full43-948/reference.json'
SCHEMAS = '/Users/dylan/LocalData/shardloom/workflow-work-elimination-20261025/checkpoint-full43-r1-baseline-schemas.json'
REFERENCE_SHA = 'fc5780da415dc33bd06ed0879db07fc040db5dd419e3c37ecde7cd40a315696b'
INPUT_SHA = '95a393d153a8c2e1ffeb5084b2656ac61465c6ef9dad435d032365844f1ac057'
BINARY_SHA = 'b309284cfa8c9ba746ab64782e5d2ab9ae5485145b636439a4ba7482cc1e1027'
LIBRARY_SHA = '5b708fa91b12b5951a22bf2b5c35aa7e8f5a5622279ea0d461ca3e7fe74fa76f'
WHEEL_SHA = '7b6b543d74583007b29306ead987cb99c97157045512a288d268e59c4f7f137f'
QUERY_SHA = '4afa04814edf3a4c52ff26fd87ea3b5dd92c7264b2d8d69ee718709f3df6f09b'
ENGINE_SECONDS = 47.935588458
BASELINE_SECONDS = 47.889862794


def need(condition, message):
    if not condition:
        raise ValueError(message)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def strict_json(raw):
    def unique(pairs):
        out = {}
        for key, value in pairs:
            need(key not in out, 'duplicate JSON key: ' + key)
            out[key] = value
        return out
    def reject(value):
        raise ValueError('nonfinite JSON constant: ' + value)
    def finite(value):
        number = float(value)
        need(math.isfinite(number), 'nonfinite JSON number')
        return number
    return json.loads(raw, object_pairs_hook=unique, parse_constant=reject, parse_float=finite)


def normalized(path):
    return path[len(PREFIX):] if path.startswith(PREFIX) else path


def object_files(manifest_raw, archive_raw, verifier_raw):
    manifest = strict_json(manifest_raw)
    need(manifest.get('schema_version') == 'shardloom.connected_programming.packet.v1', 'manifest schema mismatch')
    need(manifest.get('verifier') == {'path': 'verify.py', 'sha256': sha(verifier_raw)}, 'verifier identity mismatch')
    meta = manifest['archive']
    need(meta == {'path': 'packet.tar.xz', 'bytes': len(archive_raw), 'sha256': sha(archive_raw)}, 'archive identity mismatch')
    need(len(archive_raw) <= MAX_TAR, 'compressed archive too large')
    decoder = lzma.LZMADecompressor(memlimit=128 << 20)
    tar_bytes = decoder.decompress(archive_raw, max_length=MAX_TAR + 1)
    need(len(tar_bytes) <= MAX_TAR and decoder.eof and not decoder.unused_data, 'invalid or oversized xz stream')
    payloads = {}
    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode='r:') as tar:
        for entry in tar:
            need(len(payloads) < MAX_OBJECTS, 'too many archive objects')
            name, parts = entry.name, PurePosixPath(entry.name).parts
            need(entry.isfile() and not entry.issym() and not entry.islnk(), 'non-regular archive member')
            need(len(parts) == 3 and parts[:2] == ('objects', 'sha256') and
                 name == 'objects/sha256/' + parts[-1] and re.fullmatch('[a-f0-9]{64}', parts[-1]), 'unsafe object name')
            need(name not in payloads and 0 <= entry.size <= MAX_FILE, 'duplicate or oversized object')
            stream = tar.extractfile(entry)
            need(stream is not None, 'unreadable object')
            raw = stream.read(MAX_FILE + 1)
            need(len(raw) == entry.size and sha(raw) == parts[-1], 'object content mismatch')
            payloads[name] = raw
    files = manifest.get('files')
    need(isinstance(files, list) and 0 < len(files) <= MAX_OBJECTS, 'invalid file inventory')
    out, referenced, expanded = {}, set(), 0
    for row in files:
        path, size, digest, member = (row.get(k) for k in ('path', 'bytes', 'sha256', 'member'))
        need(isinstance(path, str) and path not in out, 'duplicate or invalid file path')
        need(type(size) is int and 0 <= size <= MAX_FILE, 'invalid file size')
        need(isinstance(digest, str) and re.fullmatch('[a-f0-9]{64}', digest), 'invalid file digest')
        need(member == 'objects/sha256/' + digest and member in payloads, 'missing or misaddressed object')
        raw = payloads[member]
        need(len(raw) == size and sha(raw) == digest, 'file content mismatch')
        out[path], expanded = raw, expanded + size
        referenced.add(member)
    need(set(payloads) == referenced, 'unreferenced or missing objects')
    need(expanded <= MAX_EXPANDED and manifest.get('expanded_file_bytes') == expanded, 'expanded-size mismatch')
    need(manifest.get('unique_objects') == len(referenced), 'object count mismatch')
    return out


def read_json(files, path):
    return strict_json(files[normalized(path)])


def bound_ref(files, record):
    raw = files[normalized(record['path'])]
    need(type(record['bytes']) is int and len(raw) == record['bytes'] and sha(raw) == record['sha256'], 'reference identity mismatch')
    return raw


def unsigned(value):
    need(type(value) is int or (type(value) is str and re.fullmatch('[0-9]+', value)), 'invalid unsigned integer')
    number = int(value)
    need(number >= 0, 'negative unsigned integer')
    return number


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


def gunzip(raw):
    decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
    decoded = decoder.decompress(raw, MAX_FILE + 1)
    need(len(decoded) <= MAX_FILE and not decoder.unconsumed_tail, 'gzip expands beyond limit')
    decoded += decoder.flush()
    need(len(decoded) <= MAX_FILE and decoder.eof and not decoder.unused_data, 'invalid gzip stream')
    return decoded


def result(envelope):
    need(envelope.get('status') == 'success', 'query failed')
    fallback = envelope['fallback']
    need(fallback.get('attempted') is False and fallback.get('allowed') is False and fallback.get('engine') is None, 'fallback evidence invalid')
    fields = {}
    for field in envelope['fields']:
        key = field['key']
        need(isinstance(key, str) and key not in fields, 'duplicate/malformed report field')
        fields[key] = field['value']
    need(fields.get('public_workflow_external_engine_invoked') == 'false', 'external engine invoked')
    need(fields.get('public_workflow_fallback_attempted') == 'false', 'fallback attempted')
    need(all(value in (False, 'false') for key, value in fields.items()
             if key.endswith(('fallback_attempted', 'external_engine_invoked'))), 'conflicting fallback field')
    keys = [key for key in ('result_jsonl', 'result_values_json') if key in fields]
    need(len(keys) == 1 and fields.get('result_payload_complete') in (True, 'true'), 'complete result missing')
    payload = fields[keys[0]]
    rows = [strict_json(line) for line in payload.splitlines()] if keys[0] == 'result_jsonl' else strict_json(payload)
    need(isinstance(rows, list) and all(isinstance(row, dict) for row in rows) and
         len(rows) == unsigned(fields['output_row_count']), 'result row inventory differs')
    return rows, strict_json(fields['result_schema_json']), unsigned(fields['native_query_execution_nanos'])


def validation(files):
    gates = ('r95-workspace-tests', 'r100-native-workspace-tests', 'r103-native-workspace-clippy',
             'r108-default-workspace-clippy', 'r105-release-runtime-build', 'r106-freeze-release-runtime', 'r109-installed-native-wheel',
             'r110-source-acceptance', 'r99-python-full-suite')
    for name in gates:
        receipt = read_json(files, name + '.json')
        need(receipt.get('status') == 'passed' and type(receipt.get('returncode')) is int and
             receipt['returncode'] == 0 and receipt.get('process_group_drained') is True, 'required validation failed: ' + name)
        need(sha(files[name + '.log']) == receipt['log_sha256'], 'validation log changed: ' + name)
    for name, expected in [('r95-workspace-tests', (3261, 0, 0)), ('r100-native-workspace-tests', (6128, 0, 35))]:
        totals = re.findall(r'^test result: \w+\. (\d+) passed; (\d+) failed; (\d+) ignored;', files[name + '.log'].decode(), re.M)
        need(tuple(sum(int(row[i]) for row in totals) for i in range(3)) == expected, 'workspace test coverage changed')
    source = read_json(files, 'source-acceptance-r1/report.json')
    need(source.get('status') == 'passed' and [row['label'] for row in source['checks']] ==
         ['format', 'diff', 'notices', 'packaging_tests', 'exports', 'reference', 'functions',
          'parity', 'front_door', 'api_schema', 'ci_gates', 'docs'], 'source gate coverage changed')
    for check in source['checks']:
        need(type(check['returncode']) is int and check['returncode'] == 0 and
             sha(files[normalized(check['log'])]) == check['log_sha256'], 'source check failed or changed')
    python = read_json(files, 'r99-python-result.json')
    need(python.get('status') == 'passed' and python.get('tests') == 997 and not python['failures'] and not python['errors']
         and len(python['skips']) == 8 and python.get('documented_example') == 'passed', 'Python coverage changed')
    wheel = read_json(files, 'installed-wheel-r2/report.json')
    need(wheel.get('status') == 'passed' and wheel.get('installation_performed') is True and
         wheel.get('uninstall_status') == 'passed' and wheel.get('publication_attempted') is False, 'installed wheel proof incomplete')
    expected = {'status': 'passed', 'tests': 179, 'failures': 0, 'errors': 0, 'skipped': 0}
    need(wheel['integration'] == expected and read_json(files, 'installed-wheel-r2/integration.json') == expected, 'installed integration coverage changed')
    need(wheel['integrity']['wheel_sha256'] == WHEEL_SHA and wheel['integrity']['native_library_sha256'] == LIBRARY_SHA
         and wheel['integrity']['cli_sha256'] == BINARY_SHA, 'installed artifact identity changed')
    need([step['name'] for step in wheel['steps']] == ['build', 'create_environment', 'install_optional_consumer',
         'install', 'load', 'integration', 'uninstall', 'verify_uninstalled'], 'wheel step coverage changed')
    for step in wheel['steps']:
        need(step.get('returncode') == 0 and sha(files['installed-wheel-r2/' + step['log']]) == step['log_sha256'], 'wheel step failed/changed')
    failed_install = read_json(files, 'r107-installed-native-wheel.json')
    need(failed_install.get('status') == 'failed' and failed_install.get('returncode') == 1, 'original packaging failure was rewritten')
    frozen = read_json(files, 'r106-freeze-release-runtime.log')
    need(frozen['shardloom']['sha256'] == BINARY_SHA and frozen['shardloom']['bytes'] == 78482704
         and frozen['_native.abi3.so']['sha256'] == LIBRARY_SHA and frozen['_native.abi3.so']['bytes'] == 78841440,
         'frozen artifact identity differs')
    return {'default_rust_tests': 3261, 'native_rust_tests': 6128, 'native_ignored': 35,
            'source_python_tests': 997, 'source_python_skips': 8, 'installed_native_tests': 179, 'installed_skips': 0}


def verify_evidence(files):
    checks = validation(files)
    need(sha(files[REFERENCE]) == REFERENCE_SHA, 'independent reference changed')
    reference = read_json(files, REFERENCE)['records']
    need(len(reference) == 43 and [row['query_id'] for row in reference] == list(range(1, 44)) and
         sum(len(row['values']) for row in reference) == 381, 'reference inventory invalid')
    schemas = {row['query']: row['schema'] for row in read_json(files, SCHEMAS)['records']}
    need(set(schemas) == set(range(1, 44)), 'schema inventory invalid')
    driver = read_json(files, 'final-full43-r1.driver.json')
    accept = read_json(files, 'final-full43-r1-acceptance.json')
    bound_ref(files, accept['outer'])
    build = strict_json(bound_ref(files, driver['build']))
    overlay = strict_json(bound_ref(files, driver['source_overlay']))
    need(driver['source_revision'] == overlay['head'] == build['head'], 'source base differs')
    need(build['command'] == ['cargo', 'build', '--locked', '--offline', '--release', '-p', 'shardloom-cli',
         '-p', 'shardloom-python', '--features', 'shardloom-cli/release-user-surfaces,shardloom-python/python-native'],
         'release build features differ')
    runtime_files = {path: row for path, row in build['changed_source_inventory'].items()
                     if path.endswith(('.rs', 'Cargo.toml', 'Cargo.lock')) and row.get('sha256')}
    need(bool(runtime_files), 'runtime source inventory absent')
    for path, row in runtime_files.items():
        raw = files['source/' + path]
        need(sha(raw) == row['sha256'] == overlay['files'][path]['sha256'] and len(raw) == row['bytes'],
             'runtime source changed after build: ' + path)
    for helper in driver['helper_identities']:
        if helper['path'] != driver['binary']['path']:
            bound_ref(files, helper)
    bound_ref(files, driver['log'])
    need(normalized(accept['outer']['path']) == 'final-full43-r1.driver.json', 'wrong driver')
    need(driver.get('status') == 'passed' and driver.get('returncode') == 0 and
         driver.get('process_group_drained') is True and driver.get('native_process_cleanup_verified') is True, 'driver/cleanup incomplete')
    need(driver.get('input_before') == driver.get('input_after') and driver['input_before'].get('sha256') == INPUT_SHA
         and driver['input_before'].get('bytes') == 15715228660, 'input identity changed')
    need(driver.get('recursive_inventory_during_execution') is False, 'execution supervision changed')
    need(driver['prospective_regression_screen']['baseline_engine_seconds'] == BASELINE_SECONDS and
         driver['prospective_regression_screen']['maximum_total_ratio'] == 1.05, 'prospective screen changed')
    summaries = driver['summaries']
    need(len(summaries) == 1 and accept['summary'] == summaries[0], 'expected one summary')
    summary = strict_json(bound_ref(files, summaries[0]))
    need(all(summary.get(k) is True for k in ('complete', 'full_result_validation', 'completed_identity_check',
         'reference_packet_identity_verified', 'harness_identity_verified', 'engine_timing_required', 'timing_available')), 'query proof incomplete')
    need(summary.get('timing_metric') == 'engine_execution_seconds' and summary.get('runs_completed') == 43 and
         summary.get('runs_passed') == 43 and summary.get('repeats') == 1, 'not one complete Full43')
    need(summary.get('selected_query_ids') == list(range(1, 44)) and len(summary['records']) == 43, 'wrong query coverage')
    need(summary.get('binary_sha256') == driver['binary']['sha256'] == BINARY_SHA, 'binary changed')
    need(summary.get('queries_sha256') == QUERY_SHA and summary['reference_packet']['sha256'] == REFERENCE_SHA, 'query/reference binding changed')
    total_nanos, rows_total = 0, 0
    for index, record in enumerate(summary['records'], 1):
        need(record.get('query') == index and record.get('run') == 1 and record.get('passed') is True and
             record.get('returncode') == 0 and not record.get('guard_failures') and record.get('validation') == 'complete_values', 'query failed or repeated')
        archives = record['completed_log_archives']
        need(len(archives) == 1, 'query output archive absent')
        arc = archives[0]
        gz = files[normalized(arc['path'])]
        need(sha(gz) == arc['gzip_sha256'] and len(gz) == arc['gzip_bytes'], 'gzip identity differs')
        raw = gunzip(gz)
        need(sha(raw) == arc['raw_sha256'] and len(raw) == arc['raw_bytes'], 'query output identity differs')
        rows, schema, nanos = result(strict_json(raw))
        seconds = nanos / 1e9
        need(equivalent(rows, reference[index - 1]['values']) and schema == schemas[index], 'values/schema differ')
        need(record.get('engine_timing_reported') is True and type(record['engine_execution_seconds']) in (int, float)
             and seconds == record['engine_execution_seconds'], 'engine time differs')
        need(sha(json.dumps(rows, sort_keys=True, allow_nan=False).encode()) == record['result_sha256'], 'result hash differs')
        total_nanos, rows_total = total_nanos + nanos, rows_total + len(rows)
    total = total_nanos / 1e9
    need(rows_total == 381 and total == summary['query_total_seconds'] == accept['engine_seconds'] == ENGINE_SECONDS, 'complete result/engine totals differ')
    need(accept['complete_results'] == 43 and accept['complete_rows'] == 381 and accept['exact_schemas'] == 43, 'acceptance coverage differs')
    need(accept['regression_screen_passed'] is True and total <= BASELINE_SECONDS * 1.05 and accept['causal_attribution'] is False, 'regression/claim boundary changed')
    return dict(status='passed', checks=checks, full43_queries=43, complete_rows=381,
                engine_seconds=total, baseline_engine_seconds=BASELINE_SECONDS,
                regression_screen_passed=True, fallback_observed=False)


def main(argv):
    if len(argv) != 3:
        print('usage: verify.py MANIFEST.json PACKET.tar.xz', file=sys.stderr)
        return 2
    try:
        with Path(argv[1]).open('rb') as stream:
            manifest = stream.read(MAX_FILE + 1)
        with Path(argv[2]).open('rb') as stream:
            archive = stream.read(MAX_TAR + 1)
        need(len(manifest) <= MAX_FILE, 'manifest too large')
        files = object_files(manifest, archive, Path(__file__).read_bytes())
        print(json.dumps(verify_evidence(files), sort_keys=True))
        return 0
    except (OSError, ValueError, KeyError, TypeError, IndexError, EOFError, lzma.LZMAError,
            tarfile.TarError, zlib.error, UnicodeError) as error:
        print(json.dumps({'status': 'failed', 'error': str(error)}), file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main(sys.argv))
