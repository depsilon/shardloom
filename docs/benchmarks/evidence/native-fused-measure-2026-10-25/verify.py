#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Verify saved fused-measure evidence without the engine or original paths.

Historical paths are archive keys only. Archived scripts are never executed.
This checks a finite mechanism decision, not complete package acceptance.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import math
from pathlib import Path, PurePosixPath
import re
import statistics
import tarfile

if not __debug__:
    raise RuntimeError('verification requires Python assertions enabled')

ORIGINAL = '/Users/dylan/LocalData/shardloom/workflow-work-elimination-20261025/'
HEAD = '167ac248ba662ae7967a7801995e9b160b17bae5'
HARNESS = 'shardloom-vortex/src/local_primitive_relational_fused_measure_cost_tests.rs'
HARNESS_SHA = '245432bde7f85375126429d5ab55ac891648b221980d54b5c873f6560511e80f'
OLD_HARNESS_SHA = '29ee47c250571fd10806784fb44c31e52b93bca2bca7504ee7e140b2eb46eb02'
CASES = {'dictionary_dense': (262144, 64), 'dictionary_skew': (262144, 64),
         'dictionary_sparse': (262144, 64), 'plain_high_cardinality': (65536, 8192),
         'tiny': (17, 7), 'ineligible_i64': (65536, 64)}
TARGETS = tuple(CASES)[:3]
SCHEMA = {'Struct': [{'names': ['group', 'rows', 'values', 'sum', 'avg'],
    'dtypes': [{'Utf8': True}, {'Primitive': ['u64', False]},
               {'Primitive': ['u64', False]}, {'Primitive': ['f64', True]},
               {'Primitive': ['f64', True]}]}, False]}
TEST = 'local_primitives::prepared_relational::tests::fused_measure_cost_tests::native_fused_measure_cost_screen'
MIB = 1 << 20
AUXILIARY_FILES = {
    "build_fused_measure_packet_r1.py",
    "build_fused_measure_packet_r2.py",
    "build_fused_measure_packet_r3.py",
    "freeze_fused_measure_r1.py",
    "freeze_fused_measure_r2.py",
    "freeze_fused_measure_r3.py",
    "fused-measure-baseline-debug-build-r1.json",
    "fused-measure-baseline-debug-build-r1.log",
    "fused-measure-baseline-debug-build-r2.json",
    "fused-measure-baseline-debug-build-r2.log",
    "fused-measure-baseline-parser-r2.json",
    "fused-measure-baseline-parser-r2.log",
    "fused-measure-baseline-release-build-r1.json",
    "fused-measure-baseline-release-build-r1.log",
    "fused-measure-baseline-release-build-r2.json",
    "fused-measure-baseline-release-build-r2.log",
    "fused-measure-baseline-runtime-r1/observe-manifest.json",
    "fused-measure-baseline-runtime-r1/observe-source-overlay.tar.gz",
    "fused-measure-baseline-runtime-r1/release-manifest.json",
    "fused-measure-baseline-runtime-r1/release-source-overlay.tar.gz",
    "fused-measure-baseline-runtime-r2/observe-manifest.json",
    "fused-measure-baseline-runtime-r2/observe-source-overlay.tar.gz",
    "fused-measure-baseline-runtime-r2/release-manifest.json",
    "fused-measure-baseline-runtime-r2/release-source-overlay.tar.gz",
    "fused-measure-candidate-debug-build-r1.json",
    "fused-measure-candidate-debug-build-r1.log",
    "fused-measure-candidate-debug-build-r2.json",
    "fused-measure-candidate-debug-build-r2.log",
    "fused-measure-candidate-debug-build-r3.json",
    "fused-measure-candidate-debug-build-r3.log",
    "fused-measure-candidate-parser-r2.json",
    "fused-measure-candidate-parser-r2.log",
    "fused-measure-candidate-release-build-r1.json",
    "fused-measure-candidate-release-build-r1.log",
    "fused-measure-candidate-release-build-r3.json",
    "fused-measure-candidate-release-build-r3.log",
    "fused-measure-candidate-runtime-r1/observe-manifest.json",
    "fused-measure-candidate-runtime-r1/observe-source-overlay.tar.gz",
    "fused-measure-candidate-runtime-r1/release-manifest.json",
    "fused-measure-candidate-runtime-r1/release-source-overlay.tar.gz",
    "fused-measure-candidate-runtime-r2/observe-manifest.json",
    "fused-measure-candidate-runtime-r2/observe-source-overlay.tar.gz",
    "fused-measure-candidate-runtime-r3/observe-manifest.json",
    "fused-measure-candidate-runtime-r3/observe-source-overlay.tar.gz",
    "fused-measure-candidate-runtime-r3/release-manifest.json",
    "fused-measure-candidate-runtime-r3/release-source-overlay.tar.gz",
    "fused-measure-dictionary-verifier-validation-r1.json",
    "fused-measure-encoding-repro-build-r1.json",
    "fused-measure-encoding-repro-build-r1.log",
    "fused-measure-encoding-repro-build-r2.json",
    "fused-measure-encoding-repro-build-r2.log",
    "fused-measure-encoding-repro-identity-r1.json",
    "fused-measure-encoding-repro-r1.json",
    "fused-measure-encoding-repro-r1.log",
    "fused-measure-encoding-repro-r2.json",
    "fused-measure-encoding-repro-r2.log",
    "fused-measure-failed-observe-verification-r2.json",
    "fused-measure-failed-tests-source-r3.json",
    "fused-measure-failed-tests-source-r3.tar.gz",
    "fused-measure-focused-r1.json",
    "fused-measure-focused-r1.log",
    "fused-measure-focused-r2.json",
    "fused-measure-focused-r2.log",
    "fused-measure-json-repro-build-r1.json",
    "fused-measure-json-repro-build-r1.log",
    "fused-measure-json-repro-identity-r1.json",
    "fused-measure-json-repro-r1.json",
    "fused-measure-json-repro-r1.log",
    "fused-measure-measure-verification-r3.json",
    "fused-measure-native-clippy-r1.json",
    "fused-measure-native-clippy-r1.log",
    "fused-measure-native-clippy-r2.json",
    "fused-measure-native-clippy-r2.log",
    "fused-measure-native-clippy-r3.json",
    "fused-measure-native-clippy-r3.log",
    "fused-measure-native-clippy-r4.json",
    "fused-measure-native-clippy-r4.log",
    "fused-measure-native-clippy-r5.json",
    "fused-measure-native-clippy-r5.log",
    "fused-measure-native-suite-r1.json",
    "fused-measure-native-suite-r1.log",
    "fused-measure-native-suite-r3.json",
    "fused-measure-native-suite-r3.log",
    "fused-measure-native-tests-r3.json",
    "fused-measure-native-tests-r3.log",
    "fused-measure-observation-verifier-r2.json",
    "fused-measure-observation-verifier-r2.log",
    "fused-measure-observe-verification-r3.json",
    "fused-measure-prepare-inputs-r1.json",
    "fused-measure-prepare-inputs-r1.log",
    "fused-measure-runner-validation-r1.json",
    "fused-measure-runner-validation-r2.json",
    "fused-measure-runner-validation-r3.json",
    "fused-measure-source-baseline-r2.json",
    "fused-measure-source-candidate-r2.json",
    "fused-measure-support-r1/bounded_build_process_resume_20260912.py",
    "fused-measure-support-r1/manifest.json",
    "fused-measure-support-r1/release_validation_guard.py",
    "fused-measure-verifier-first-environment-failure.json",
    "fused-measure-verifier-validation-r1.json",
    "fused-measure-verifier-validation-r2.json",
    "fused-measure-verifier-validation-r3.json",
    "fused-measure-verifier-validation-r4.json",
    "fused-measure-verifier-validation-r5.json",
    "fused_measure_encoding_repro_r1.rs",
    "fused_measure_encoding_repro_r2.rs",
    "fused_measure_harness_r2.rs",
    "fused_measure_json_roundtrip_repro_r1.rs",
    "run_fused_measure_r1.py",
    "run_fused_measure_r2.py",
    "run_fused_measure_r3.py",
    "run_guarded.py",
    "switch_fused_measure_source_r2.py",
    "validate_fused_measure_verifier_first_r1.py",
    "validate_fused_measure_verifier_r1.py",
    "validate_fused_measure_verifier_r2.py",
    "validate_fused_measure_verifier_r3.py",
    "verify_fused_measure_before_dictionary_r1.py",
    "verify_fused_measure_packet_r1.py",
    "verify_fused_measure_packet_r2.py",
    "verify_fused_measure_r1.py",
    "verify_fused_measure_r2.py",
    "verify_fused_measure_r3.py",
}


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def strict(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            assert key not in result, 'duplicate JSON key'
            result[key] = value
        return result

    def finite(value):
        number = float(value)
        assert math.isfinite(number), 'nonfinite JSON number'
        return number

    def invalid(value):
        raise ValueError('nonfinite JSON constant: ' + value)

    return json.loads(raw, object_pairs_hook=unique, parse_float=finite, parse_constant=invalid)


def safe(name):
    return (isinstance(name, str) and bool(name) and not name.startswith('/')
            and all(part not in ('', '.', '..') for part in name.split('/'))
            and str(PurePosixPath(name)) == name)


class Packet:
    def __init__(self, directory):
        self.manifest = strict((directory / 'manifest.json').read_bytes())
        manifest = self.manifest
        assert manifest['schema_version'] == 'shardloom.native_fused_measure.packet.v3'
        assert manifest['baseline_head'] == HEAD
        assert manifest['status'] == 'finite_mechanism_evidence_captured_package_acceptance_pending'
        expected = {}
        for identity in manifest['files']:
            name = identity['path']
            assert safe(name) and name not in expected
            assert type(identity['bytes']) is int and 0 <= identity['bytes'] <= 64 * MIB
            assert re.fullmatch('[0-9a-f]{64}', identity['sha256'])
            expected[name] = identity
        assert sum(item['bytes'] for item in expected.values()) == manifest['raw_member_bytes']
        assert manifest['raw_member_bytes'] <= 64 * MIB
        archive = manifest['archive']
        assert archive['path'] == 'packet.tar.gz' and 0 < archive['bytes'] <= 64 * MIB
        path = directory / archive['path']
        assert not path.is_symlink() and path.stat().st_size == archive['bytes']
        raw = path.read_bytes()
        assert digest(raw) == archive['sha256']
        self.files = {}
        with tarfile.open(fileobj=io.BytesIO(raw), mode='r:gz') as stream:
            for entry in stream:
                assert safe(entry.name) and entry.name in expected and entry.name not in self.files
                identity = expected[entry.name]
                assert entry.isfile() and entry.size == identity['bytes']
                data = stream.extractfile(entry).read()
                assert len(data) == identity['bytes'] and digest(data) == identity['sha256']
                self.files[entry.name] = data
        assert self.files.keys() == expected.keys(), 'missing archive member'
        assert {name for name in self.files if not name.startswith((
            'fused-measure-comparison-r3/', 'fused-measure-observations-r3/',
            'fused-measure-observations-r2/',
            'fused-measure-observations-r1/',
            'fused-measure-inputs-r1/'))} == AUXILIARY_FILES, 'auxiliary evidence coverage'

    def raw(self, name):
        if name.startswith(ORIGINAL):
            name = name[len(ORIGINAL):]
        assert safe(name) and name in self.files, 'missing evidence: ' + name
        return self.files[name]

    def read(self, name):
        return strict(self.raw(name))

    def linked(self, reference):
        raw = self.raw(reference['path'])
        assert digest(raw) == reference['sha256']
        return raw


def schedule(mode):
    if mode == 'observe':
        return [(case, role, 0, 0) for case in CASES for role in ('baseline', 'candidate')]
    result = []
    for cohort in (1, 2):
        for case in CASES:
            for sample in (1, 2, 3):
                first = (sample != 2) if cohort == 1 else (sample == 2)
                roles = ('baseline', 'candidate') if first else ('candidate', 'baseline')
                result.extend((case, role, cohort, sample) for role in roles)
    return result


def independent_input(case):
    rows, keys = CASES[case]
    return {'domain': [f'clé-東京-{key}' for key in range(keys)] + [None],
            'codes': [keys if row % 97 == 0 else
                      0 if case == 'dictionary_skew' and row % 10 != 9 else row % keys
                      for row in range(rows)],
            'n': [None if row % 19 == 0 else (row % 1009) - 504 for row in range(rows)],
            'p': [row % (100 if case == 'dictionary_sparse' else 5) for row in range(rows)]}


def oracle(data):
    groups = {}
    selected = 0
    for code, number, predicate in zip(data['codes'], data['n'], data['p'], strict=True):
        if predicate < 1 or predicate > 3:
            continue
        selected += 1
        group = groups.setdefault(data['domain'][code], [0, 0, 0])
        group[0] += 1
        if number is not None:
            group[1] += 1
            group[2] += number * 3
    result = []
    for key, (rows, count, total) in groups.items():
        assert abs(total) < 2**53 and count < 2**53
        result.append({'group': key, 'rows': rows, 'values': count,
                       'sum': float(total) if count else None,
                       'avg': total / count if count else None})
    return result, selected


def number(text, name):
    found = re.findall(r'\b' + re.escape(name) + r':\s*(\d+)\b', text)
    assert len(found) == 1, name
    return int(found[0])


def values(record, witness, revision='r3'):
    case = record['case']
    data = witness(record['input_witness'])
    assert data == independent_input(case), 'frozen input: ' + case
    expected, selected = oracle(data)
    assert record['input_rows'] == CASES[case][0] and record['selected_rows'] == selected
    assert witness(record['schema_witness']) == SCHEMA
    assert record['expected_rows_witness'] == record['actual_rows_witness']
    actual = witness(record['actual_rows_witness'])
    assert actual == expected, 'full ordered values: ' + case
    raw = witness(record['raw_rows_witness'])
    assert isinstance(raw, str)
    assert [strict(line) for line in raw.splitlines()] == expected, 'raw emitted JSON'
    for row in actual:
        assert set(row) == {'group', 'rows', 'values', 'sum', 'avg'}
        assert type(row['rows']) is int and type(row['values']) is int
        assert row['sum'] is None or type(row['sum']) is float
        assert row['avg'] is None or type(row['avg']) is float
    for name in ('engine_execution_ns', 'preparation_ns', 'result_serialization_ns'):
        assert type(record[name]) is int and record[name] >= 0
    assert record['engine_execution_ns'] > 0
    assert record['complete_values_match'] is True and record['complete_schema_match'] is True
    assert record['memory_grant_bytes'] == 128 * MIB
    assert type(record['peak_reserved_bytes']) is int
    assert 0 <= record['peak_reserved_bytes'] <= record['memory_grant_bytes']
    assert record['denied_reservations'] == record['reserved_after_cleanup'] == 0
    certificate = record['native_io_certificate']
    assert 'schema_version: "shardloom.native_io_certificate.v1"' in certificate
    assert 'diagnostics: []' in certificate
    fallback = re.findall(r'\bfallback_attempted:\s*(true|false)', certificate)
    assert fallback and set(fallback) == {'false'} and 'fallback_execution_allowed: false' in certificate
    for name, expected_value in [('completed_executions', 1), ('admitted_max_parallelism', 1),
                                 ('provider_background_workers', 0), ('limit_bytes', 128 * MIB),
                                 ('denied_reservations', 0)]:
        assert number(record['runtime'], name) == expected_value, name
    active = record['role'] == 'candidate' and case != 'ineligible_i64'
    if record['role'] == 'candidate':
        for name, value in [('fused_measure_stages', int(active)),
                            ('fused_measure_rows', selected if active else 0)]:
            found = re.findall(r'\b' + name + r'=(\d+)\b', certificate)
            assert found == [str(value)], (case, name, found)
    delivered = re.findall(r'\bdelivered_scan_rows=(\d+)\b', certificate)
    batches = re.findall(r'\bnative_batches=(\d+)\b', certificate)
    assert delivered == [str(selected)] and len(batches) == 1 and int(batches[0]) > 0
    if record['observation_enabled']:
        counts = record['observations']
        assert isinstance(counts, dict) and all(type(value) is int and value >= 0 for value in counts.values())
        assert counts['fused_measure_stages'] == int(active)
        assert counts['fused_measure_rows'] == (selected if active else 0)
        assert counts['expression_rows_constructed'] == (0 if active else selected)
        assert counts['aggregate_row_updates'] == selected * 4
        assert counts['variable_domains'] > 0
        assert revision in ('r2', 'r3')
        encoded = revision == 'r3' and active and case != 'plain_high_cardinality'
        domains = int(batches[0]) if encoded else 0
        entries = domains * (CASES[case][1] + 1)
        if record['role'] == 'candidate':
            assert counts['variable_dictionary_domains'] == domains
            assert counts['variable_dictionary_domain_values'] == entries
        assert counts['variable_domain_values'] == (entries if encoded else selected) + len(expected)
    else:
        assert record['observations'] is None


def score(records):
    results, passed = {}, True
    for cohort in (1, 2):
        for case in CASES:
            medians = {}
            for role in ('baseline', 'candidate'):
                runs = [records[(case, role, cohort, sample)] for sample in (1, 2, 3)]
                medians[role] = {'engine_ns': statistics.median(row['engine_execution_ns'] for row in runs),
                    'preparation_engine_ns': statistics.median(row['engine_execution_ns'] + row['preparation_ns'] for row in runs)}
            allowance = 50000 if case == 'tiny' else 0
            control = all(medians['candidate'][clock] <= max(medians['baseline'][clock] * 1.05,
                medians['baseline'][clock] + allowance) for clock in ('engine_ns', 'preparation_engine_ns'))
            target = case not in TARGETS or medians['candidate']['engine_ns'] <= medians['baseline']['engine_ns'] * .98
            passed &= control and target
            results[f'{cohort}:{case}'] = {'medians': medians, 'control_passed': control, 'target_passed': target}
    return {'all_timing_gates_passed': passed, 'cells': results}


def checked_receipt(packet, name, passed=True):
    receipt, raw = packet.read(name + '.json'), packet.raw(name + '.log')
    assert receipt['head'] == HEAD and digest(raw) == receipt['log_sha256']
    assert receipt['status'] == ('passed' if passed else 'failed')
    assert (receipt['returncode'] == 0) is passed
    assert receipt['direct_child_reaped'] and receipt['process_group_drained']
    assert not receipt['timed_out'] and receipt['interrupted_signal'] is None
    return receipt, raw.decode()


def checked_manifest(packet, manifest, role, mode, revision='r2'):
    assert revision in ('r1', 'r2', 'r3')
    harness_sha = OLD_HARNESS_SHA if revision == 'r1' else HARNESS_SHA
    directory = f'fused-measure-{role}-runtime-{revision}'
    label = 'release' if mode == 'measure' else 'observe'
    assert packet.read(f'{directory}/{label}-manifest.json') == manifest
    assert manifest['revision'] == HEAD and manifest['features'] == ['release-user-surfaces']
    assert manifest['profile'] == ('release' if mode == 'measure' else 'ci-native')
    assert manifest['harnesses'] == {HARNESS: harness_sha}
    assert re.fullmatch('[0-9a-f]{64}', manifest['binary_sha256'])
    assert manifest['binary'] == ORIGINAL + directory + '/shardloom-fused-measure-' + label
    source = manifest['source_inventory']
    assert len(source) == (2130 if role == 'baseline' else 2134) and source[HARNESS] == harness_sha
    assert all(safe(name) and re.fullmatch('[0-9a-f]{64}', sha) for name, sha in source.items())
    overlay = manifest['source_overlay']
    members = {}
    with tarfile.open(fileobj=io.BytesIO(packet.linked(overlay)), mode='r:gz') as stream:
        for entry in stream:
            assert entry.isfile() and safe(entry.name) and entry.name not in members
            assert entry.name in overlay['members'] and 0 <= entry.size <= 2 * MIB
            raw = stream.extractfile(entry).read()
            assert digest(raw) == overlay['members'][entry.name] == source[entry.name]
            members[entry.name] = digest(raw)
    assert members == overlay['members'] and len(members) == (18 if role == 'baseline' else 24)
    receipt = strict(packet.linked(manifest['build_receipt']))
    name = manifest['build_receipt']['path'][len(ORIGINAL):-5]
    assert receipt == checked_receipt(packet, name)[0]
    command = receipt['command']
    assert command[:4] == ['cargo', 'test', '--offline', '--locked']
    assert command[command.index('--features') + 1] == 'release-user-surfaces'
    assert command[command.index('-p') + 1] == 'shardloom-vortex'
    assert '--lib' in command and '--no-run' in command
    assert ('--release' in command) is (mode == 'measure')
    if mode == 'observe': assert command[command.index('--profile') + 1] == 'ci-native'
    for path, sha in members.items():
        assert receipt['changed_source_inventory'][path]['sha256'] == sha


def verify_cohort(packet, mode, revision='r3', require_saved_summary=True):
    assert revision == 'r3' or (revision == 'r2' and mode == 'observe')
    root = 'fused-measure-' + ('comparison' if mode == 'measure' else 'observations') + '-' + revision
    outcome, protocol = packet.read(root + '/outcome.json'), packet.read(root + '/protocol.json')
    assert outcome['mode'] == mode and outcome['complete'] is True
    assert outcome['protocol'] == protocol['protocol'] == 'native-fused-measure-v2'
    for key in protocol:
        if key not in ('calls', 'complete'): assert protocol[key] == outcome[key], key
    assert outcome['driver_sha256'] == digest(packet.raw('run_fused_measure_' + revision + '.py'))
    assert outcome['scorer_sha256'] == digest(packet.raw('verify_fused_measure_' + revision + '.py'))
    if revision == 'r3':
        assert outcome['candidate_revision'] == 3
        original = outcome['original_frozen_protocol']
        assert digest(original['text'].encode()) == original['sha256']
    assert digest(outcome['frozen_protocol']['text'].encode()) == outcome['frozen_protocol']['sha256']
    for field in ('input', 'source'):
        assert outcome[field + '_inventory_before'] == outcome[field + '_inventory_after']
        assert outcome[field + '_unchanged'] is True
    assert outcome['immutable_runtime_files_unchanged'] and outcome['helpers_unchanged']
    assert outcome['helpers'] == outcome['helpers_after']
    support = packet.read('fused-measure-support-r1/manifest.json')
    assert set(support) == set(outcome['helpers'])
    for original, identity in support.items():
        raw = packet.raw('fused-measure-support-r1/' + identity['path'])
        assert len(raw) == identity['bytes']
        assert digest(raw) == identity['sha256'] == outcome['helpers'][original]
    assert outcome['immutable_runtime_files_before'] == outcome['immutable_runtime_files_after']
    for path, sha in outcome['immutable_runtime_files_before'].items():
        if not path.rsplit('/', 1)[-1].startswith('shardloom-fused-measure-'):
            assert digest(packet.raw(path)) == sha
    assert outcome['storage_within_budget'] is True
    assert all(sum(outcome['storage_' + phase].values()) <= 100 << 30 for phase in ('before', 'after'))
    assert outcome['active_packet_limit_bytes'] == 192 * MIB
    assert outcome['call_timeout_seconds'] == 120 and outcome['call_output_limit_bytes'] == 8 * MIB
    manifests = outcome['runtime_manifests']
    assert set(manifests) == {'baseline', 'candidate'}
    for role, manifest in manifests.items():
        runtime_revision = 'r2' if role == 'baseline' else revision
        checked_manifest(packet, manifest, role, mode, runtime_revision)
        assert outcome['reconstructed_source_inventory'][role] == manifest['source_inventory']
        assert outcome['binary_sha256_after'][role] == manifest['binary_sha256']
    assert manifests['baseline']['toolchain'] == manifests['candidate']['toolchain']
    assert 'rustc 1.99.0' in manifests['baseline']['toolchain']
    assert manifests['candidate']['source_inventory'] == outcome['source_inventory_before']
    planned = schedule(mode)
    assert outcome['schedule'] == [list(row) for row in planned]
    assert len(outcome['calls']) == len(planned)
    names = {root + '/protocol.json', root + '/outcome.json'}
    def witness(identity):
        assert isinstance(identity, str) and re.fullmatch('[0-9a-f]{64}', identity)
        name = root + '/witnesses/' + identity + '.json'
        names.add(name)
        raw = packet.raw(name)
        assert digest(raw) == identity
        return strict(raw)
    fixed_inputs = packet.read('fused-measure-inputs-r1/manifest.json')
    assert set(fixed_inputs) == set(CASES)
    input_names = {'manifest.json'}
    for case, item in fixed_inputs.items():
        assert (item['rows'], item['keys']) == CASES[case]
        assert item['writer'] == 'sequential_native_flat' and item['chunk_rows'] == 65536
        assert item['file_statistics'] is False
        raw = packet.raw('fused-measure-inputs-r1/witnesses/' + item['input_witness'] + '.json')
        assert digest(raw) == item['input_witness']
        assert strict(raw) == independent_input(case)
        name = case + '.vortex'
        assert item['schema'] == {'Struct': [{'names': ['g', 'n', 'p'],
            'dtypes': [{'Utf8': True}, {'Primitive': ['i64' if case == 'ineligible_i64' else 'i32', True]},
                       {'Primitive': ['i64', False]}]}, False]}
        assert outcome['input_inventory_before'][name] == {'sha256': item['sha256'], 'bytes': item['bytes']}
        input_names.update((name, 'witnesses/' + item['input_witness'] + '.json'))
    assert set(outcome['input_inventory_before']) == input_names
    assert {name for name in packet.files if name.startswith('fused-measure-inputs-r1/')} == {
        'fused-measure-inputs-r1/' + name for name in input_names if not name.endswith('.vortex')}
    for name, identity in outcome['input_inventory_before'].items():
        if not name.endswith('.vortex'):
            raw = packet.raw('fused-measure-inputs-r1/' + name)
            assert digest(raw) == identity['sha256'] and len(raw) == identity['bytes']
    records = {}
    for sequence, (call, expected) in enumerate(zip(outcome['calls'], planned, strict=True), 1):
        case, role, cohort, sample = expected
        label = f'{mode}-{case}-{role}-{cohort}-{sample}'
        assert (call['case'], call['role'], call['cohort'], call['sample']) == expected
        assert call['sequence'] == sequence and call['mode'] == mode
        assert call['profile'] == ('release' if mode == 'measure' else 'ci-native')
        assert call['status'] == 'passed' and call['returncode'] == 0
        assert call['direct_child_reaped'] and call['process_group_drained']
        assert call['timed_out'] is False and call['interrupted_signal'] is None
        assert not call.get('error') and not call.get('cleanup_error')
        assert call['command'] == [manifests[role]['binary'], TEST, '--exact', '--ignored', '--nocapture', '--test-threads=1']
        assert call['timeout_seconds'] == 120 and call['output_limit_bytes'] == 8 * MIB
        for file in (label + '.json', label + '.log', 'supervision-' + label + '.json'):
            names.add(root + '/' + file)
        raw = packet.raw(root + '/' + label + '.json')
        assert digest(raw) == call['record_sha256']
        assert digest(packet.raw(root + '/' + label + '.log')) == call['log_sha256']
        assert packet.read(root + '/supervision-' + label + '.json') == call
        record = strict(raw)
        assert record['protocol'] == 'native-fused-measure-v2'
        assert (record['case'], record['role'], record['cohort'], record['sample']) == expected
        assert record['observation_enabled'] is (mode == 'observe')
        assert record['input_file'] == fixed_inputs[case]
        assert record['input_witness'] == fixed_inputs[case]['input_witness']
        values(record, witness, revision)
        records[expected] = record
    assert {name for name in packet.files if name.startswith(root + '/')} == names
    result = {'protocol': outcome['protocol'], 'mode': mode, 'calls': len(records),
              'complete_values_schemas_resources_and_closure_passed': True}
    if mode == 'measure':
        result.update(score(records))
    else:
        for case in CASES:
            base = records[(case, 'baseline', 0, 0)]['observations']
            candidate = records[(case, 'candidate', 0, 0)]['observations']
            assert candidate['variable_domains'] == base['variable_domains']
        result['work_elimination_observations_passed'] = revision == 'r3'
        if revision == 'r2':
            result['failure'] = 'dictionary_activation_not_observed'
            result['failed_targets'] = list(TARGETS)
    summary = (f'fused-measure-{mode}-verification-r3.json' if revision == 'r3'
               else 'fused-measure-failed-observe-verification-r2.json')
    if require_saved_summary:
        assert packet.read(summary) == result
    return result, outcome


def verify_first_attempt(packet):
    root = 'fused-measure-observations-r1'
    outcome, protocol = packet.read(root + '/outcome.json'), packet.read(root + '/protocol.json')
    assert outcome['complete'] is False and outcome['mode'] == 'observe'
    assert outcome['protocol'] == protocol['protocol'] == 'native-fused-measure-v1'
    assert outcome['schedule'] == protocol['schedule'] == [list(row) for row in schedule('observe')]
    for key in protocol:
        if key not in ('calls', 'complete'): assert protocol[key] == outcome[key]
    assert outcome['driver_sha256'] == digest(packet.raw('run_fused_measure_r1.py'))
    assert outcome['scorer_sha256'] == digest(packet.raw('verify_fused_measure_r1.py'))
    assert len(outcome['calls']) == 1
    call = outcome['calls'][0]
    assert (call['case'], call['role'], call['cohort'], call['sample']) == ('dictionary_dense', 'baseline', 0, 0)
    assert call['status'] == 'failed' and call['returncode'] == 101 and call['sequence'] == 1
    assert call['direct_child_reaped'] and call['process_group_drained']
    assert call['timed_out'] is False and call['interrupted_signal'] is None
    label = 'observe-dictionary_dense-baseline-0-0'
    log = packet.raw(root + '/' + label + '.log')
    assert digest(log) == call['log_sha256']
    assert b'complete ordered values' in log and b'1 failed' in log
    assert packet.read(root + '/supervision-' + label + '.json') == call
    names = {root + '/' + name for name in ('protocol.json', 'outcome.json',
        label + '.log', 'supervision-' + label + '.json')}
    witnesses = {}
    for name, raw in packet.files.items():
        if name.startswith(root + '/witnesses/'):
            assert name.endswith('.json') and digest(raw) == name.rsplit('/', 1)[-1][:-5]
            names.add(name)
            witnesses[name] = strict(raw)
    assert len(witnesses) == 3
    expected, _ = oracle(independent_input('dictionary_dense'))
    assert independent_input('dictionary_dense') in witnesses.values()
    assert expected in witnesses.values() and SCHEMA in witnesses.values()
    assert {name for name in packet.files if name.startswith(root + '/')} == names
    for field in ('input', 'source'):
        assert outcome[field + '_inventory_before'] == outcome[field + '_inventory_after']
        assert outcome[field + '_unchanged'] is True
    assert outcome['immutable_runtime_files_unchanged'] and outcome['helpers_unchanged']
    assert outcome['helpers'] == outcome['helpers_after']
    assert outcome['immutable_runtime_files_before'] == outcome['immutable_runtime_files_after']
    assert outcome['storage_within_budget'] is True
    assert digest(outcome['frozen_protocol']['text'].encode()) == outcome['frozen_protocol']['sha256']
    for role in ('baseline', 'candidate'):
        manifest = outcome['runtime_manifests'][role]
        checked_manifest(packet, manifest, role, 'observe', 'r1')
        assert outcome['reconstructed_source_inventory'][role] == manifest['source_inventory']
        assert outcome['binary_sha256_after'][role] == manifest['binary_sha256']
        released = packet.read(f'fused-measure-{role}-runtime-r1/release-manifest.json')
        checked_manifest(packet, released, role, 'measure', 'r1')
        for field in ('source_inventory', 'harnesses', 'toolchain', 'features', 'revision'):
            assert released[field] == manifest[field]
    assert call['command'] == [outcome['runtime_manifests']['baseline']['binary'], TEST,
                              '--exact', '--ignored', '--nocapture', '--test-threads=1']
    assert not any(name.startswith('fused-measure-comparison-r1/') for name in packet.files)
    return outcome


def verify_harness_repair(packet, original, observation):
    assert digest(packet.raw('fused_measure_harness_r2.rs')) == HARNESS_SHA
    for role in ('baseline', 'candidate'):
        old = original['runtime_manifests'][role]['source_inventory']
        expected = dict(old, **{HARNESS: HARNESS_SHA})
        assert observation['runtime_manifests'][role]['source_inventory'] == expected
        change = packet.read(f'fused-measure-source-{role}-r2.json')
        prior = 'candidate' if role == 'baseline' else 'baseline'
        before = dict(original['runtime_manifests'][prior]['source_inventory'], **{HARNESS: HARNESS_SHA})
        assert change['status'] == 'passed' and change['revision'] == HEAD
        assert (change['from_role'], change['to_role']) == (prior, role)
        assert change['harness_only_change_to_frozen_role'] is True
        assert change['harness_sha256'] == HARNESS_SHA
        assert change['before_source_inventory'] == before and change['after_source_inventory'] == expected
        assert change['changed_paths'] == sorted(name for name in set(before) | set(expected)
                                               if before.get(name) != expected.get(name))
        assert change['driver_sha256'] == digest(packet.raw('switch_fused_measure_source_r2.py'))
        receipt, log = checked_receipt(packet, f'fused-measure-{role}-parser-r2')
        assert '1 passed; 0 failed;' in log
        parser_test = TEST.rsplit('::', 1)[0] + '::fused_measure_witness_parser_preserves_exact_emitted_average_bits'
        assert receipt['command'] == [observation['runtime_manifests'][role]['binary'], parser_test,
                                      '--exact', '--nocapture', '--test-threads=1']
        assert receipt['changed_source_inventory'][HARNESS]['sha256'] == HARNESS_SHA
    assert original['input_inventory_before'] == observation['input_inventory_before']
    checked_receipt(packet, 'fused-measure-json-repro-build-r1')
    _, repro_log = checked_receipt(packet, 'fused-measure-json-repro-r1')
    lines = repro_log.splitlines()
    assert len(lines) == 3
    for line in lines:
        match = re.fullmatch(r'([^:]+): expected_bits=(\d+), serde_bits=(\d+), std_bits=(\d+)', line)
        assert match and match[2] == match[4] and match[2] != match[3]
    identity = packet.read('fused-measure-json-repro-identity-r1.json')
    assert identity['engine_executions'] == 0
    source = identity['identities']['source']
    assert source['path'] == ORIGINAL + 'fused_measure_json_roundtrip_repro_r1.rs'
    assert digest(packet.raw(source['path'])) == source['sha256']
    assert identity['toolchain'] == observation['runtime_manifests']['baseline']['toolchain']
    validation = packet.read('fused-measure-verifier-validation-r4.json')
    assert validation['status'] == 'passed' and validation['engine_executions'] == 0
    assert len(validation['checks']) == len(set(validation['checks'])) == 22
    assert 'raw_output_disagrees' in validation['checks']
    assert validation['verifier_sha256'] == digest(packet.raw('verify_fused_measure_r2.py'))
    assert validation['driver_sha256'] == digest(packet.raw('validate_fused_measure_verifier_r2.py'))
    runner = packet.read('fused-measure-runner-validation-r2.json')
    assert runner['status'] == 'passed' and runner['engine_executions'] == 0
    for field, name in [('driver', 'run'), ('scorer', 'verify'), ('freezer', 'freeze')]:
        assert runner[field + '_sha256'] == digest(packet.raw(name + '_fused_measure_r2.py'))


def verify_encoding_correction(packet, failed, corrected):
    changed = {
        'shardloom-vortex/src/local_primitive_relational_fused_measure.rs',
        'shardloom-vortex/src/local_primitive_relational_fused_measure_tests.rs',
    }
    before = failed['runtime_manifests']['candidate']['source_inventory']
    after = corrected['runtime_manifests']['candidate']['source_inventory']
    assert set(before) == set(after)
    assert {name for name in before if before[name] != after[name]} == changed
    assert before[HARNESS] == after[HARNESS] == HARNESS_SHA
    assert corrected['runtime_manifests']['baseline'] == failed['runtime_manifests']['baseline']
    assert corrected['input_inventory_before'] == failed['input_inventory_before']
    assert corrected['original_frozen_protocol'] == failed['frozen_protocol']
    baseline_release = packet.read('fused-measure-baseline-runtime-r2/release-manifest.json')
    checked_manifest(packet, baseline_release, 'baseline', 'measure', 'r2')
    for name in ('source_inventory', 'harnesses', 'toolchain', 'features', 'revision'):
        assert baseline_release[name] == corrected['runtime_manifests']['baseline'][name]
    assert not any(name.startswith(('fused-measure-comparison-r1/',
                                   'fused-measure-comparison-r2/')) for name in packet.files)
    refusal, log = checked_receipt(packet, 'fused-measure-observation-verifier-r2', False)
    assert refusal['returncode'] == 1
    assert "assert counts['variable_dictionary_domains'] > 0" in log
    assert refusal['command'][-2:] == [ORIGINAL + 'verify_fused_measure_r2.py',
                                     ORIGINAL + 'fused-measure-observations-r2']

    # The original broad suite belongs to the pre-encoding candidate, including
    # its original harness. It cannot certify the later production correction.
    prior_suite, log = checked_receipt(packet, 'fused-measure-native-suite-r1')
    assert '3322 passed; 0 failed; 35 ignored;' in log
    for name, sha in failed['runtime_manifests']['candidate']['source_overlay']['members'].items():
        assert prior_suite['changed_source_inventory'][name]['sha256'] == (
            OLD_HARNESS_SHA if name == HARNESS else sha)

    failed_test, log = checked_receipt(packet, 'fused-measure-native-tests-r3', False)
    assert failed_test['returncode'] == 101 and '10 passed; 1 failed; 2 ignored;' in log
    assert 'counts.variable_dictionary_domains > 0' in log
    saved = packet.read('fused-measure-failed-tests-source-r3.json')
    assert saved['status'] == 'byte_exact_reconstruction_verified'
    assert saved['receipt'] == 'fused-measure-native-tests-r3.json'
    assert saved['receipt_sha256'] == digest(packet.raw(saved['receipt']))
    assert set(saved['members']) == changed
    members = {}
    with tarfile.open(fileobj=io.BytesIO(packet.linked(saved['archive'])), mode='r:gz') as stream:
        for entry in stream:
            assert entry.isfile() and entry.name in changed and entry.name not in members
            assert 0 <= entry.size <= MIB
            members[entry.name] = digest(stream.extractfile(entry).read())
    assert members == saved['members']
    for name, sha in members.items():
        assert failed_test['changed_source_inventory'][name]['sha256'] == sha

    identity = packet.read('fused-measure-encoding-repro-identity-r1.json')
    assert identity['shardloom_query_executions'] == 0 and identity['provider_probe_executions'] == 2
    assert identity['toolchain'] == corrected['runtime_manifests']['candidate']['toolchain']
    assert set(identity['identities']) == {
        'fused_measure_encoding_repro_r1.rs', 'fused_measure_encoding_repro_r2.rs',
        'fused-measure-encoding-repro-r1', 'fused-measure-encoding-repro-r2', 'vortex_rlib'}
    for name, item in identity['identities'].items():
        assert type(item['bytes']) is int and item['bytes'] > 0
        assert re.fullmatch('[0-9a-f]{64}', item['sha256'])
        if name.endswith('.rs'):
            assert item['path'] == ORIGINAL + name
            raw = packet.raw(name)
            assert len(raw) == item['bytes'] and digest(raw) == item['sha256']
    for revision in ('r1', 'r2'):
        build, _ = checked_receipt(packet, 'fused-measure-encoding-repro-build-' + revision)
        execution, log = checked_receipt(packet, 'fused-measure-encoding-repro-' + revision,
                                        passed=revision == 'r2')
        source = 'fused_measure_encoding_repro_' + revision + '.rs'
        binary = 'fused-measure-encoding-repro-' + revision
        assert build['command'][-3:] == [ORIGINAL + source, '-o', ORIGINAL + binary]
        assert execution['command'] == [ORIGINAL + binary,
            ORIGINAL + 'fused-measure-inputs-r1/dictionary_dense.vortex']
        assert identity['identities'][binary]['path'] == ORIGINAL + binary
        assert 'resolved_group:' in log and 'root=vortex.dict' in log
        if revision == 'r1':
            assert execution['returncode'] == 101 and 'field.as_opt::<Dict>().is_none()' in log
        else:
            assert 'selected_group: rows=2048 root=vortex.get_item' in log
            assert 'lazy_dictionary_resolution_and_complete_values_proved=true rows=2048' in log
    validation = packet.read('fused-measure-verifier-validation-r5.json')
    assert validation['status'] == 'passed' and validation['engine_executions'] == 0
    assert len(validation['checks']) == len(set(validation['checks'])) == 26
    assert {'dictionary_domain_missing', 'dictionary_entries_wrong', 'prepared_key_values_wrong',
            'delivered_source_rows_wrong'} <= set(validation['checks'])
    assert validation['verifier_sha256'] == digest(packet.raw('verify_fused_measure_r3.py'))
    assert validation['driver_sha256'] == digest(packet.raw('validate_fused_measure_verifier_r3.py'))
    runner = packet.read('fused-measure-runner-validation-r3.json')
    assert runner['status'] == 'passed' and runner['engine_executions'] == 0
    for field, name in [('driver', 'run'), ('scorer', 'verify'), ('freezer', 'freeze')]:
        assert runner[field + '_sha256'] == digest(packet.raw(name + '_fused_measure_r3.py'))


def verify(directory):
    packet = Packet(directory)
    original = verify_first_attempt(packet)
    observed, observation = verify_cohort(packet, 'observe')
    measured, measurement = verify_cohort(packet, 'measure')
    _, failed_observation = verify_cohort(packet, 'observe', 'r2')
    verify_harness_repair(packet, original, failed_observation)
    verify_encoding_correction(packet, failed_observation, observation)
    for role in ('baseline', 'candidate'):
        for key in ('source_inventory', 'harnesses', 'toolchain', 'features', 'revision'):
            assert observation['runtime_manifests'][role][key] == measurement['runtime_manifests'][role][key]
    assert observation['input_inventory_before'] == measurement['input_inventory_before']
    assert observation['frozen_protocol'] == measurement['frozen_protocol']
    candidate = measurement['runtime_manifests']['candidate']['source_overlay']['members']
    suite, log = checked_receipt(packet, 'fused-measure-native-suite-r3')
    assert '3324 passed; 0 failed; 35 ignored;' in log
    for name, sha in candidate.items():
        assert suite['changed_source_inventory'][name]['sha256'] == sha
    assert suite['command'] == [observation['runtime_manifests']['candidate']['binary'], '--test-threads=1']
    checked_receipt(packet, 'fused-measure-focused-r1', False)
    checked_receipt(packet, 'fused-measure-native-clippy-r1', False)
    checked_receipt(packet, 'fused-measure-focused-r2')
    checked_receipt(packet, 'fused-measure-native-clippy-r2')
    checked_receipt(packet, 'fused-measure-native-clippy-r3')
    checked_receipt(packet, 'fused-measure-native-clippy-r4')
    clippy, _ = checked_receipt(packet, 'fused-measure-native-clippy-r5')
    for name, sha in candidate.items():
        assert clippy['changed_source_inventory'][name]['sha256'] == sha
    return {'portable_replay_passed': True, 'engine_executions': 0,
            'failed_first_attempt_preserved': True, 'failed_dictionary_activation_preserved': True,
            'production_source_unchanged_by_harness_repair': True,
            'encoding_correction_limited_to_fused_consumer_and_regression': True,
            'observations': observed, 'comparison': measured,
            'package_acceptance': 'pending'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', nargs='?', type=Path, default=Path(__file__).resolve().parent)
    args = parser.parse_args()
    print(json.dumps(verify(args.directory), indent=2))


if __name__ == '__main__':
    main()
