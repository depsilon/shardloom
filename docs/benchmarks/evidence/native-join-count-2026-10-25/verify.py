#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Replay the saved join-count decision without an engine or original files.

Historical paths are keys only. Archive scripts are never imported or executed.
This verifies the mechanism comparison, not the still-pending package acceptance.
"""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import io
import json
import math
from pathlib import Path, PurePosixPath
import re
import statistics
import tarfile

ORIGINAL = '/Users/dylan/LocalData/shardloom/workflow-work-elimination-20261025/'
HEAD = '167ac248ba662ae7967a7801995e9b160b17bae5'
HARNESS = 'shardloom-vortex/src/local_primitive_relational_join_count_cost_tests.rs'
HARNESS_SHA = 'f9b82bbe9d8f05042ca1df3c0ee48d9a3484127e2d10a72eacd98629c3af1c9e'
TEST = 'local_primitives::prepared_relational::tests::join_count_cost_tests::native_join_count_cost_screen'
ROWS = {'duplicate_numeric': (8192, 16384), 'composite_text': (4096, 8192),
        'skew': (1024, 2048), 'unique_match': (32768, 32768),
        'unique_no_match': (32768, 32768), 'tiny': (17, 23),
        'ineligible_count_column': (8192, 8192)}
TARGETS = ('duplicate_numeric', 'composite_text', 'skew')
SCHEMA = {'Struct': [{'names': ['rows'], 'dtypes': [{'Primitive': ['u64', False]}]}, False]}
COUNTS = ('counted_join_stages', 'counted_join_build_rows', 'counted_join_probe_rows',
          'counted_join_represented_pairs')
MIB = 1 << 20
AUXILIARY_FILES = {
    'build_join_count_packet_r1.py', 'run_join_count_comparison_r1.py',
    'score_join_count_comparison_r1.py', 'verify_join_count_observations_r1.py',
    'validate_join_count_scorer_r1.py', 'validate_join_count_observations_r1.py',
    'freeze_join_count_candidate_r1.py', 'run_guarded.py', 'scorer-validation-r1.json',
    'observation-verifier-validation-r1.json', 'join-count-correctness-r1.json',
    'baseline-runtime/release-manifest.json', 'baseline-runtime/observe-manifest.json',
    'baseline-runtime/source-overlay.tar.gz', 'candidate-runtime-r1/release-manifest.json',
    'candidate-runtime-r1/observe-manifest.json', 'candidate-runtime-r1/release-source-overlay.tar.gz',
    'candidate-runtime-r1/observe-source-overlay.tar.gz',
} | {name + suffix for name in (
    'join-count-baseline-release-build-r1', 'join-count-baseline-observe-build-r1',
    'join-count-focused-native-r1', 'join-count-focused-native-r2', 'join-count-focused-native-r3',
    'join-count-native-clippy-r1', 'join-count-native-suite-r1', 'join-count-native-suite-r2',
    'join-count-candidate-release-build-r1') for suffix in ('.json', '.log')}


def require(value, reason):
    if not value:
        raise ValueError(reason)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def strict(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, 'duplicate JSON key')
            result[key] = value
        return result

    def finite(value):
        number = float(value)
        require(math.isfinite(number), 'nonfinite JSON number')
        return number

    def invalid(value):
        raise ValueError('nonfinite JSON constant: ' + value)

    return json.loads(raw, object_pairs_hook=unique, parse_float=finite, parse_constant=invalid)


def safe(name):
    return (isinstance(name, str) and bool(name) and not name.startswith('/')
            and all(part not in ('', '.', '..') for part in name.split('/'))
            and str(PurePosixPath(name)) == name)


def unpack(raw, expected):
    require(sum(item['bytes'] for item in expected.values()) <= 16 * MIB, 'archive expansion bound')
    files = {}
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:gz') as stream:
        for entry in stream:
            require(safe(entry.name) and entry.name in expected and entry.name not in files,
                    'unexpected or duplicate archive member')
            identity = expected[entry.name]
            require(entry.isfile() and entry.size == identity['bytes'], 'archive member kind/size')
            data = stream.extractfile(entry).read()
            require(len(data) == identity['bytes'] and digest(data) == identity['sha256'],
                    'archive member digest: ' + entry.name)
            files[entry.name] = data
    require(files.keys() == expected.keys(), 'missing archive members')
    return files


class Packet:
    def __init__(self, directory):
        self.manifest = strict((directory / 'manifest.json').read_bytes())
        manifest = self.manifest
        require(manifest['schema_version'] == 'shardloom.native_join_count.packet.v1', 'packet schema')
        require(manifest['baseline_head'] == HEAD, 'baseline revision')
        require(manifest['status'] == 'mechanism_comparison_passed_package_acceptance_pending',
                'package status overclaim')
        expected = {}
        for identity in manifest['files']:
            name = identity['path']
            require(safe(name) and name not in expected, 'invalid or duplicate file identity')
            require(type(identity['bytes']) is int and 0 <= identity['bytes'] <= 16 * MIB,
                    'file size bound')
            require(re.fullmatch(r'[0-9a-f]{64}', identity['sha256']), 'invalid SHA256')
            expected[name] = identity
        require(len(expected) == 374, 'frozen packet member coverage')
        require(sum(item['bytes'] for item in expected.values()) == manifest['raw_member_bytes'],
                'raw byte total')
        archive = manifest['archive']
        require(archive['path'] == 'packet.tar.gz' and 0 < archive['bytes'] <= 16 * MIB, 'archive path/size')
        path = directory / archive['path']
        require(path.stat().st_size == archive['bytes'], 'archive size')
        raw = path.read_bytes()
        require(digest(raw) == archive['sha256'], 'archive SHA256')
        self.files = unpack(raw, expected)
        require({name for name in self.files if not name.startswith(
            ('join-count-comparison-r1/', 'join-count-observations-r1/'))} == AUXILIARY_FILES,
            'auxiliary evidence coverage')

    def raw(self, name):
        if name.startswith(ORIGINAL):
            name = name[len(ORIGINAL):]
        require(safe(name) and name in self.files, 'missing evidence: ' + name)
        return self.files[name]

    def read(self, name):
        return strict(self.raw(name))

    def linked(self, reference):
        raw = self.raw(reference['path'])
        require(digest(raw) == reference['sha256'], 'linked evidence digest')
        return raw


def schedule(mode):
    if mode == 'observe':
        return [(case, role, 0, 0) for case in ROWS for role in ('baseline', 'candidate')]
    result = []
    for cohort in (1, 2):
        for case in ROWS:
            for sample in (1, 2, 3):
                first = sample != 2 if cohort == 1 else sample == 2
                roles = ('baseline', 'candidate') if first else ('candidate', 'baseline')
                result.extend((case, role, cohort, sample) for role in roles)
    return result


def check_receipt(packet, name, status='passed'):
    receipt = packet.read(name + '.json')
    raw = packet.raw(name + '.log')
    require(receipt['status'] == status and receipt['returncode'] == (0 if status == 'passed' else 101),
            'build/test outcome: ' + name)
    require(receipt['head'] == HEAD and digest(raw) == receipt['log_sha256'], 'build/test identity')
    require(receipt['direct_child_reaped'] and receipt['process_group_drained']
            and not receipt['timed_out'] and receipt['interrupted_signal'] is None,
            'build/test supervision')
    return receipt, raw.decode()


def check_manifest(packet, manifest, role, mode):
    directory = 'baseline-runtime' if role == 'baseline' else 'candidate-runtime-r1'
    label = 'release' if mode == 'measure' else 'observe'
    require(packet.read(f'{directory}/{label}-manifest.json') == manifest, 'runtime manifest identity')
    require(manifest['revision'] == HEAD and manifest['features'] == ['release-user-surfaces'],
            'runtime build/features')
    require(manifest['profile'] == ('release' if mode == 'measure' else 'ci-native'), 'runtime profile')
    require(manifest['harnesses'] == {HARNESS: HARNESS_SHA}, 'shared harness identity')
    require(re.fullmatch(r'[0-9a-f]{64}', manifest['binary_sha256']), 'binary digest')
    require(manifest['binary'] == ORIGINAL + directory + '/shardloom-join-count-' + label,
            'runtime binary path')
    source = manifest['source_inventory']
    require(len(source) == (2126 if role == 'baseline' else 2129), 'runtime source coverage')
    require(source[HARNESS] == HARNESS_SHA, 'source harness identity')
    for name, sha in source.items():
        require(safe(name) and re.fullmatch(r'[0-9a-f]{64}', sha), 'source identity')
    overlay = manifest['source_overlay']
    raw = packet.linked(overlay)
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:gz') as stream:
        members = {}
        for entry in stream:
            require(entry.isfile() and safe(entry.name) and entry.name not in members
                    and 0 <= entry.size <= 2 * MIB, 'source overlay member')
            require(entry.name in overlay['members'], 'extra overlay source')
            data = stream.extractfile(entry).read()
            require(digest(data) == overlay['members'][entry.name] == source[entry.name], 'overlay source identity')
            members[entry.name] = digest(data)
    require(members == overlay['members'] and len(members) == (4 if role == 'baseline' else 16),
            'overlay coverage')
    build = strict(packet.linked(manifest['build_receipt']))
    receipt_name = manifest['build_receipt']['path'][len(ORIGINAL):-5]
    checked, _ = check_receipt(packet, receipt_name)
    require(build == checked, 'build receipt')
    require(build['command'][:4] == ['cargo', 'test', '--offline', '--locked'], 'build command')
    require(build['command'][build['command'].index('--features') + 1] == 'release-user-surfaces',
            'build feature selection')
    require('--lib' in build['command'] and build['command'][build['command'].index('-p') + 1] == 'shardloom-vortex',
            'build target')
    require(('--release' in build['command']) == (mode == 'measure'), 'build profile flags')
    for name, sha in members.items():
        require(build['changed_source_inventory'][name]['sha256'] == sha, 'compiled overlay identity')


def inputs_and_answer(packet, root, case, record):
    def witness(sha):
        require(re.fullmatch(r'[0-9a-f]{64}', sha), 'witness identity')
        raw = packet.raw(root + '/witnesses/' + sha + '.json')
        require(digest(raw) == sha, 'witness bytes')
        return strict(raw)

    require(len(record['input_witnesses']) == 2, 'two complete inputs')
    keys = []
    for side, (sha, count) in enumerate(zip(record['input_witnesses'], ROWS[case])):
        data = witness(sha)
        require(set(data) == {'n', 's', 'id'}, 'input fields')
        require(len(data['n']) == len(data['id']) == count, 'input cardinality')
        require(all(type(value) is int for value in data['id'] + data['n']), 'input integer type')
        require(data['id'] == list(range(count)), 'complete input IDs')
        numbers = []
        for row in range(count):
            if case == 'duplicate_numeric': value = row % 64
            elif case == 'composite_text': value = row % 32
            elif case == 'skew': value = row + 1 if row % 10 == 9 else 0
            elif case == 'tiny': value = row % 7
            elif case == 'unique_no_match' and side == 1: value = row + count
            else: value = row
            numbers.append(value)
        require(data['n'] == numbers, 'frozen numeric inputs')
        strings = [f'clé-東京-{value}' for value in numbers] if case == 'composite_text' else []
        require(data['s'] == strings, 'frozen text inputs')
        keys.append([(number, strings[row] if strings else None) for row, number in enumerate(numbers)])
    right = Counter(keys[1])
    answer = sum(right[key] for key in keys[0])
    require(witness(record['schema_witness']) == SCHEMA, 'authoritative output schema')
    for field in ('expected_rows_witness', 'actual_rows_witness'):
        value = witness(record[field])
        require(isinstance(value, list) and len(value) == 1 and set(value[0]) == {'rows'}
                and type(value[0]['rows']) is int and value == [{'rows': answer}], 'independent complete values')
    return answer, len(right)


def number(text, name, separator=':'):
    values = re.findall(r'\b' + re.escape(name + separator) + r'\s*(\d+)\b', text)
    require(len(values) == 1, 'unique reported counter: ' + name)
    return int(values[0])


def check_record(record, case, role, mode, answer, unique):
    require(record['protocol'] == 'native-join-count-v1', 'record protocol')
    require(record['complete_values_match'] is True and record['complete_schema_match'] is True,
            'record correctness status')
    require(record['expected_rows_witness'] == record['actual_rows_witness'], 'complete result identity')
    require((record['left_rows'], record['right_rows']) == ROWS[case], 'record input rows')
    require(record['reserved_after_cleanup'] == record['denied_reservations'] == 0, 'record cleanup')
    require(record['memory_grant_bytes'] == 128 * MIB and type(record['peak_reserved_bytes']) is int
            and 0 <= record['peak_reserved_bytes'] <= 128 * MIB, 'native reservation grant')
    for field in ('engine_execution_ns', 'preparation_ns', 'result_serialization_ns'):
        require(type(record[field]) is int and record[field] >= 0, 'record clock')
    runtime = record['runtime']
    for field, expected in [('completed_executions', 1), ('provider_background_workers', 0),
                            ('admitted_max_parallelism', 1), ('limit_bytes', 128 * MIB), ('denied_reservations', 0)]:
        require(number(runtime, field) == expected, 'reported runtime: ' + field)
    certificate = record['native_io_certificate']
    require('schema_version: "shardloom.native_io_certificate.v1"' in certificate
            and 'diagnostics: []' in certificate, 'native certificate')
    fallback = re.findall(r'\bfallback_attempted:\s*(true|false)', certificate)
    require(fallback and set(fallback) == {'false'} and 'fallback_execution_allowed: false' in certificate,
            'no-fallback certificate')
    require(number(certificate, 'scans_started', '=') == 2
            and number(certificate, 'delivered_scan_rows', '=') == sum(ROWS[case]), 'complete source delivery')
    activated = role == 'candidate' and case != 'ineligible_count_column'
    counts = dict(zip(COUNTS, (int(activated), ROWS[case][1] if activated else 0,
                              ROWS[case][0] if activated else 0, answer if activated else 0)))
    if role == 'candidate':
        for name, value in counts.items():
            require(number(certificate, name, '=') == value, 'certificate mechanism counter')
        require(number(certificate, 'counted_join_distinct_build_keys', '=') == (unique if activated else 0),
                'certificate unique key count')
    require(record['observation_enabled'] is (mode == 'observe'), 'instrumentation isolation')
    if mode == 'measure':
        require(record['observations'] is None, 'release observation contamination')
    else:
        observed = record['observations']
        require(observed['join_output_rows_constructed'] == (0 if activated else answer), 'constructed pair count')
        require(observed['join_retained_right_rows'] == (unique if activated else ROWS[case][1]),
                'retained build rows')
        for name, value in counts.items():
            actual = observed[name] if role == 'candidate' else observed.get(name, 0)
            require(type(actual) is int and actual == value, 'observed counted state')


def comparison(packet, mode):
    root = 'join-count-comparison-r1' if mode == 'measure' else 'join-count-observations-r1'
    outcome = packet.read(root + '/outcome.json')
    protocol = packet.read(root + '/protocol.json')
    expected = schedule(mode)
    require(outcome['complete'] is True and outcome['mode'] == protocol['mode'] == mode, 'comparison completion')
    require(outcome['protocol'] == protocol['protocol'] == 'native-join-count-v1', 'comparison protocol')
    require(outcome['schedule'] == protocol['schedule'] == [list(row) for row in expected]
            and len(outcome['calls']) == len(expected), 'complete fixed schedule')
    manifests = outcome['runtime_manifests']
    require(manifests == protocol['runtime_manifests'] and set(manifests) == {'baseline', 'candidate'},
            'runtime manifest selection')
    for role, manifest in manifests.items():
        check_manifest(packet, manifest, role, mode)
    require(manifests['baseline']['toolchain'] == manifests['candidate']['toolchain']
            and manifests['baseline']['toolchain'].startswith('rustc 1.99.0 '), 'identical compiler')
    require(outcome['source_inventory_before'] == outcome['source_inventory_after']
            == protocol['source_inventory_before'] == manifests['candidate']['source_inventory'], 'source freeze')
    binaries = {role: manifest['binary_sha256'] for role, manifest in manifests.items()}
    require(outcome['binary_sha256_before'] == outcome['binary_sha256_after'] == protocol['binary_sha256_before']
            == binaries, 'executable freeze')
    for field, name in [('driver_sha256', 'run_join_count_comparison_r1.py'),
                        ('scorer_sha256', 'score_join_count_comparison_r1.py')]:
        require(protocol[field] == outcome[field] == digest(packet.raw(name)), 'comparison tooling identity')
    frozen = protocol['frozen_protocol']
    require(frozen == outcome['frozen_protocol'] and digest(frozen['text'].encode()) == frozen['sha256'],
            'frozen prospective protocol')
    require(outcome['helpers'] == protocol['helpers'] and len(outcome['helpers']) == 2, 'supervisor identities')
    require(protocol['active_packet_limit_bytes'] == 192 * MIB, 'active output cap')
    require(sum(outcome['storage_before'].values()) <= 100 * (1 << 30)
            and sum(outcome['storage_after'].values()) <= 100 * (1 << 30), 'combined storage cap')
    records, witnesses, expected_files = {}, set(), {root + '/protocol.json', root + '/outcome.json'}
    oracles = {}
    for index, (call, expected_call) in enumerate(zip(outcome['calls'], expected), 1):
        case, role, cohort, sample = expected_call
        require((call['case'], call['role'], call['cohort'], call['sample']) == expected_call
                and call['mode'] == mode and call['sequence'] == index, 'call order')
        name = f'{mode}-{case}-{role}-{cohort}-{sample}'
        record_path, log_path, receipt_path = [root + '/' + value for value in
            (name + '.json', name + '.log', 'supervision-' + name + '.json')]
        expected_files.update((record_path, log_path, receipt_path))
        require(packet.read(receipt_path) == call, 'separate call receipt')
        require(digest(packet.raw(record_path)) == call['record_sha256']
                and digest(packet.raw(log_path)) == call['log_sha256'], 'call record/log identity')
        require(call['status'] == 'passed' and call['returncode'] == 0
                and call['direct_child_reaped'] and call['process_group_drained'], 'call completion')
        require(not call['timed_out'] and call['interrupted_signal'] is None
                and not call.get('error') and not call.get('cleanup_error')
                and not call.get('group_absence_unproved_at_cleanup'), 'call cleanup')
        require(call['timeout_seconds'] == 120 and call['output_limit_bytes'] == 8 * MIB, 'call safety bounds')
        require(call['command'] == [manifests[role]['binary'], TEST, '--exact', '--ignored',
                                    '--nocapture', '--test-threads=1'], 'actual engine command')
        record = packet.read(record_path)
        require((record['case'], record['role'], record['cohort'], record['sample']) == expected_call,
                'record call identity')
        identities = tuple(record['input_witnesses']) + (record['schema_witness'],
            record['expected_rows_witness'], record['actual_rows_witness'])
        if case not in oracles:
            oracles[case] = (identities, inputs_and_answer(packet, root, case, record))
        require(identities == oracles[case][0], 'fixed complete inputs and results across roles/samples')
        for sha in identities:
            path = root + '/witnesses/' + sha + '.json'
            require(digest(packet.raw(path)) == sha, 'complete witness content')
            witnesses.add(path)
        check_record(record, case, role, mode, *oracles[case][1])
        records[expected_call] = record
    expected_files.update(witnesses)
    expected_files.add(root + ('/decision.json' if mode == 'measure' else '/observation-verification.json'))
    require({name for name in packet.files if name.startswith(root + '/')} == expected_files, 'comparison file closure')
    return records, manifests, protocol


def score(packet, records):
    saved = packet.read('join-count-comparison-r1/decision.json')
    require(saved['calls'] == 84 and saved['cohorts'] == [1, 2]
            and saved['samples_per_case_role_cohort'] == 3, 'decision schedule')
    rows = []
    for cohort in (1, 2):
        for case in ROWS:
            engine, complete = {}, {}
            for role in ('baseline', 'candidate'):
                observations = [records[(case, role, cohort, sample)] for sample in (1, 2, 3)]
                engine[role] = [row['engine_execution_ns'] for row in observations]
                complete[role] = [row['preparation_ns'] + row['engine_execution_ns'] for row in observations]
            medians = {role: statistics.median(values) for role, values in engine.items()}
            whole = {role: statistics.median(values) for role, values in complete.items()}
            def within(values):
                return values['candidate'] * 100 <= values['baseline'] * 105 or (
                    case == 'tiny' and values['candidate'] <= values['baseline'] + 50_000)
            target = case not in TARGETS or medians['candidate'] * 100 <= medians['baseline'] * 98
            passed = within(medians) and within(whole) and target
            matching = [row for row in saved['cases'] if row['case'] == case and row['cohort'] == cohort]
            require(len(matching) == 1, 'one decision per case/cohort')
            row = matching[0]
            require(row['engine_execution_ns'] == engine and row['preparation_plus_engine_ns'] == complete
                    and row['engine_medians_ns'] == medians and row['preparation_plus_engine_medians_ns'] == whole,
                    'recomputed timing medians')
            require(row['engine_gate_passed'] is within(medians)
                    and row['preparation_plus_engine_gate_passed'] is within(whole)
                    and row['target_two_percent_gate_passed'] is target
                    and row['timing_gate_passed'] is passed, 'recomputed frozen timing gates')
            rows.append({'case': case, 'cohort': cohort, 'engine_medians_ns': medians, 'passed': passed})
    require(len(saved['cases']) == 14 and all(row['passed'] for row in rows), 'all-case two-cohort gates')
    require(saved['timing_gate_passed'] is True and saved['all_targets_passed_in_both_cohorts'] is True
            and saved['all_case_engine_and_complete_workload_gates_passed'] is True, 'saved retention decision')
    require(saved['outcome_sha256'] == digest(packet.raw('join-count-comparison-r1/outcome.json'))
            and saved['protocol_sha256'] == digest(packet.raw('join-count-comparison-r1/protocol.json'))
            and saved['scorer_sha256'] == digest(packet.raw('score_join_count_comparison_r1.py')), 'decision linkage')
    expected_records = {f'{case}-{role}-{cohort}-{sample}': digest(packet.raw(
        f'join-count-comparison-r1/measure-{case}-{role}-{cohort}-{sample}.json'))
        for case, role, cohort, sample in schedule('measure')}
    require(saved['record_sha256'] == expected_records, 'decision complete-record linkage')
    witness_hashes = {name.rsplit('/', 1)[1]: digest(raw) for name, raw in packet.files.items()
                     if name.startswith('join-count-comparison-r1/witnesses/')}
    require(saved['witness_sha256'] == witness_hashes, 'decision complete-witness linkage')
    return rows


def replay(packet):
    measured, release, protocol = comparison(packet, 'measure')
    observed, debug, observation_protocol = comparison(packet, 'observe')
    require(protocol['frozen_protocol'] == observation_protocol['frozen_protocol'], 'shared prospective protocol')
    for role in ('baseline', 'candidate'):
        require(release[role]['source_inventory'] == debug[role]['source_inventory'], 'profile source parity')
    for case in ROWS:
        for role in ('baseline', 'candidate'):
            for field in ('input_witnesses', 'expected_rows_witness', 'actual_rows_witness', 'schema_witness'):
                require(measured[(case, role, 1, 1)][field] == observed[(case, role, 0, 0)][field],
                        'observation/release semantic parity')
    observation = packet.read('join-count-observations-r1/observation-verification.json')
    require(observation['calls_verified'] == 14 and observation['timing_scored'] is False
            and observation['all_mechanism_assertions_passed'] is True
            and observation['all_complete_values_and_schemas_match'] is True
            and observation['all_resource_cleanup_checks_passed'] is True, 'saved observation decision')
    require(observation['verifier_sha256'] == digest(packet.raw('verify_join_count_observations_r1.py'))
            and observation['outcome_sha256'] == digest(packet.raw('join-count-observations-r1/outcome.json'))
            and observation['protocol_sha256'] == digest(packet.raw('join-count-observations-r1/protocol.json')),
            'observation linkage')
    require(observation['record_sha256'] == {f'{case}-{role}': digest(packet.raw(
        f'join-count-observations-r1/observe-{case}-{role}-0-0.json'))
        for case, role, _, _ in schedule('observe')}, 'observation complete-record linkage')
    require(observation['witness_sha256'] == {name.rsplit('/', 1)[1]: digest(raw)
        for name, raw in packet.files.items() if name.startswith('join-count-observations-r1/witnesses/')},
        'observation complete-witness linkage')
    require(len(observation['cases']) == len(ROWS), 'observation case coverage')
    for case, saved in zip(ROWS, observation['cases']):
        require(saved['case'] == case and saved['mechanism_gate_passed'] is True, 'observation case order/status')
        for role in ('baseline', 'candidate'):
            record = observed[(case, role, 0, 0)]
            facts = saved[role + '_observations']
            require(facts['counted_fields'] == {key: record['observations'].get(key, 0) for key in COUNTS}
                    and all(facts[key] == record['observations'][key] for key in
                            ('join_output_rows_constructed', 'join_retained_right_rows')),
                    'saved observed counters')
    for name, status in [('join-count-focused-native-r1', 'failed'), ('join-count-focused-native-r2', 'failed'),
                         ('join-count-focused-native-r3', 'passed'), ('join-count-native-clippy-r1', 'passed'),
                         ('join-count-native-suite-r1', 'failed'), ('join-count-native-suite-r2', 'passed')]:
        receipt, log = check_receipt(packet, name, status)
        if name == 'join-count-native-suite-r2':
            require('test result: ok. 3312 passed; 0 failed; 33 ignored;' in log, 'full native correctness result')
        if name == 'join-count-native-suite-r1':
            require('test result: FAILED. 3311 passed; 1 failed; 33 ignored;' in log
                    and 'kind: AlreadyExists' in log, 'preserved fixture collision failure')
        if name == 'join-count-focused-native-r3':
            require('test result: ok. 19 passed; 0 failed;' in log, 'focused counted-join correctness')
        if name in ('join-count-focused-native-r3', 'join-count-native-clippy-r1', 'join-count-native-suite-r2'):
            for source, sha in release['candidate']['source_overlay']['members'].items():
                if name != 'join-count-native-suite-r2' and source.endswith('/vortex_ingest_column_layout_tests.rs'):
                    continue  # The separately preserved test-directory repair follows these focused checks.
                require(receipt['changed_source_inventory'][source]['sha256'] == sha,
                        'candidate tested-source identity')
    for name, count, script_field, script in [
        ('scorer-validation-r1.json', 13, 'scorer_sha256', 'score_join_count_comparison_r1.py'),
        ('observation-verifier-validation-r1.json', 9, 'verifier_sha256', 'verify_join_count_observations_r1.py')]:
        validation = packet.read(name)
        require(validation['status'] == 'passed' and validation['engine_executions'] == 0
                and validation['synthetic_only'] is True and len(validation['checks']) == count
                and validation[script_field] == digest(packet.raw(script)), 'decision-checker validation')
    rows = score(packet, measured)
    return {'status': 'passed', 'scope': 'mechanism_comparison_only', 'release_calls': 84,
            'observation_calls': 14, 'native_tests_passed': 3312, 'native_tests_ignored': 33,
            'timing_gate_passed': True, 'package_acceptance_complete': False,
            'engine_comparisons': rows}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', nargs='?', type=Path, default=Path(__file__).resolve().parent)
    args = parser.parse_args()
    result = replay(Packet(args.directory))
    print(json.dumps({key: value for key, value in result.items() if key != 'engine_comparisons'}, sort_keys=True))


if __name__ == '__main__':
    main()
