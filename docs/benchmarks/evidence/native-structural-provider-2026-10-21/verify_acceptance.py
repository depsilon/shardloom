# SPDX-License-Identifier: Apache-2.0
"""Replay structural-provider acceptance from compact records and committed source."""
import argparse
import base64
from collections import Counter
import copy
import csv
from decimal import Decimal
import gzip
import hashlib
import io
import json
import lzma
import os
from pathlib import Path
import re
import shlex
import statistics
import subprocess
import sys

PREFIXES = ('shardloom-', 'vendor/', 'python/', 'scripts/', 'benchmarks/', '.github/', '.cargo/')
NAMES = {'Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml',
         'website-src/package.json', 'website-src/package-lock.json'}
CLI_TEST = 'shardloom-cli/src/sql_native_relational_tests.rs'
NOTICE = 'python/THIRD_PARTY_LICENSES/manifest.json'
TARGETS = {'shardloom', 'shardloom_runtime', 'shardloom_vortex', 'shardloom_plan',
           'shardloom_core', 'shardloom_exec', 'vortex_array', 'vortex_runend',
           'vortex_fsst', 'vortex_zstd', 'vortex_datetime_parts', 'vortex_layout', 'vortex_file'}
CORE_GATES = {'metadata-locators-native-release-880': (13, (3176, 0, 31)),
              'metadata-locators-workspace-tests-880': (107, (3235, 0, 0)),
              'metadata-locators-native-clippy-880': (0, (0, 0, 0)),
              'metadata-locators-workspace-clippy-880': (0, (0, 0, 0)),
              'metadata-locators-minimal-native-880': (0, (0, 0, 0)),
              'metadata-locators-vendor-clippy-880-r2': (0, (0, 0, 0)),
              'file-statistics-vendor-layout-tests-880': (1, (249, 0, 0)),
              'file-statistics-vendor-layout-clippy-880': (0, (0, 0, 0))}


def require(value, message):
    if not value:
        raise ValueError(message)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(value):
    return digest(json.dumps(value, sort_keys=True, allow_nan=False).encode())


def git(repository, *args):
    return subprocess.check_output(['git', *args], cwd=repository)


def source_blobs(repository, revision, names):
    require(names and all('\n' not in name and not Path(name).is_absolute()
                          and '..' not in Path(name).parts for name in names), 'invalid source path')
    requests = ''.join(revision + ':' + name + '\n' for name in names).encode()
    output = io.BytesIO(subprocess.check_output(['git', 'cat-file', '--batch'],
                                               cwd=repository, input=requests))
    result = {}
    for name in names:
        header = output.readline().split()
        require(len(header) == 3 and header[1] == b'blob', 'missing source: ' + name)
        result[name] = output.read(int(header[2]))
        require(len(result[name]) == int(header[2]) and output.read(1) == b'\n', 'truncated source')
    require(output.read() == b'', 'extra source data')
    return result


class Evidence:
    def __init__(self, packet):
        require(packet['schema'] == 'shardloom.structural_acceptance_packet.v1', 'packet schema')
        self.members = packet['members']
        self.supporting = packet['supporting_helpers']
        require(set(self.supporting) == {'process_supervisor', 'workload_guard', 'native_runner'}, 'guard helpers omitted')
        self.variants = {}
        for row in packet['historical_helper_variants']:
            before, archived = row['original'], row['archived']
            require(before['bytes'] == archived['bytes'] and before['sha256'] == archived['sha256'], 'helper variant identity')
            key = before['path'], before['sha256']
            require(key not in self.variants, 'duplicate helper variant')
            self.variants[key] = archived['path']
        for name, row in self.members.items():
            raw = self.raw(name)
            require(len(raw) == row['bytes'] and digest(raw) == row['sha256'], 'member changed: ' + name)
        for ref in self.supporting.values():
            self.reference(ref)

    def raw(self, name):
        row = self.members[str(name)]
        require(row['encoding'] in ('utf8', 'base64'), 'member encoding')
        return row['data'].encode() if row['encoding'] == 'utf8' else base64.b64decode(row['data'], validate=True)

    def read(self, name):
        raw = self.raw(name)
        return json.loads(gzip.decompress(raw) if str(name).endswith('.gz') else raw)

    def reference(self, ref):
        path = self.variants.get((ref['path'], ref['sha256']), ref['path'])
        raw = self.raw(path)
        require(len(raw) == ref['bytes'] and digest(raw) == ref['sha256'], 'reference changed: ' + ref['path'])
        return raw

    def linked(self, ref):
        return json.loads(self.reference(ref))

    def run(self, refs, source=None, expected_status='passed'):
        receipt = self.linked(refs['receipt'])
        raw = self.reference(refs['log'])
        require(receipt['status'] == expected_status and
                (receipt['returncode'] == 0) == (expected_status == 'passed'), 'guard status')
        require(receipt['process_group_drained'] and receipt['direct_child_reaped']
                and receipt['group_absence_unproved_at_cleanup'] is False
                and receipt['timed_out'] is False and receipt['interrupted_signal'] is None,
                'guard did not drain')
        require(receipt['log_sha256'] == digest(raw) and receipt['output_bytes'] == len(raw), 'guard log changed')
        require(receipt['runner_sha256'] == self.supporting['native_runner']['sha256']
                and receipt['supervisor_sha256'] == self.supporting['process_supervisor']['sha256']
                and receipt['guard_sha256'] == self.supporting['workload_guard']['sha256'], 'guard implementation changed')
        if source is not None:
            require(receipt['head'] == source['head'] and
                    receipt['working_tree'].strip() == source['working_tree'], 'guard source changed')
        return receipt, raw.decode()


def counts(text):
    return [tuple(map(int, row)) for row in re.findall(
        r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;', text)]


def verify_sources(packet, evidence, repository, blobs=None):
    source = evidence.linked(packet['source'])
    require(source['schema'] == 'shardloom.structural_pr_source.v2', 'source schema')
    revision = packet['code_commit']
    require(re.fullmatch(r'[a-f0-9]{40}', revision) is not None, 'invalid code revision')
    paths = git(repository, 'ls-tree', '-rz', '--name-only', revision).decode().rstrip('\0').split('\0')
    names = sorted(name for name in paths if name.startswith(PREFIXES) or name in NAMES)
    require(names == sorted(source['source_hashes']) and len(names) == 2594, 'source closure incomplete')
    blobs = source_blobs(repository, revision, names) if blobs is None else blobs
    require({name: digest(blobs[name]) for name in names} == source['source_hashes'], 'committed source changed')
    require(git(repository, 'merge-base', source['head'], revision).decode().strip() == source['head'], 'source base changed')
    prior = evidence.linked(source['previous_source'])
    changed = {name for name in source['source_hashes'].keys() | prior['source_hashes'].keys()
               if source['source_hashes'].get(name) != prior['source_hashes'].get(name)}
    require(changed == {NOTICE} and source['changed_notice_only'] == NOTICE
            and source['unchanged_all_compiled_and_test_inputs'], 'notice source bridge changed')
    correction = evidence.linked(source['notice_correction'])
    require(correction['source'] == source['previous_source'], 'notice correction source')
    failed, log = evidence.run(dict(receipt=correction['failed_wheel'], log=correction['failed_log']), prior, 'failed')
    require(failed['returncode'] == 1 and 'notice bundle does not match reviewed vendored dependencies' in log,
            'original notice failure missing')
    old_notice = json.loads(git(repository, 'show', source['head'] + ':' + NOTICE))
    require(digest(git(repository, 'show', source['head'] + ':' + NOTICE)) == prior['source_hashes'][NOTICE],
            'previous notice changed')
    new_notice = json.loads(blobs[NOTICE])
    changes = []
    for before, after in zip(old_notice['vendored_components'], new_notice['vendored_components'], strict=True):
        if before == after:
            continue
        require(before['name'] == after['name'], 'notice component changed')
        left, right = copy.deepcopy(before), copy.deepcopy(after)
        del left['provenance']['local_changes']
        del right['provenance']['local_changes']
        require(left == right, 'notice identity or licensing changed')
        require(after['provenance'] == json.loads(blobs['vendor/' + after['name'] + '/upstream-provenance.json']),
                'notice differs from reviewed provider')
        changes.append(before['name'])
    require(changes == ['vortex-file', 'vortex-layout'], 'notice delta changed')
    old_notice['vendored_components'] = new_notice['vendored_components']
    require(old_notice == new_notice, 'non-vendored notice content changed')
    native_source = evidence.linked(prior['prior_native_source'])
    changed = {name for name in prior['source_hashes'].keys() | native_source['source_hashes'].keys()
               if prior['source_hashes'].get(name) != native_source['source_hashes'].get(name)}
    require(changed == {CLI_TEST} and prior['unchanged_production_and_native_tests'], 'CLI test source bridge changed')
    cli_failure = evidence.linked(prior['cli_failure'])
    failed, log = evidence.run(dict(receipt=cli_failure['receipt'], log=cli_failure['log']), expected_status='failed')
    require(failed['returncode'] == 101 and 'native_relational_sql_derived_cancellation_consumer_failure_and_generation_cleanup' in log,
            'original CLI cleanup failure missing')
    text = blobs[CLI_TEST].decode().replace('use shardloom_vortex::resident_session::ResidentVortexSession;\n', '', 1)
    start = text.index('fn successful_derived_relational_retained_memory(')
    test_start = text.index('#[test]\nfn native_relational_sql_derived_cancellation_consumer_failure_and_generation_cleanup', start)
    end = text.index('\n#[test]', test_start + 1)
    require(digest((text[:start] + cli_failure['original_test'] + text[end:]).encode()) == native_source['source_hashes'][CLI_TEST],
            'unrelated CLI test changed')
    require(text[start:end].count('assert_eq!(prepared.snapshot().memory.reserved_bytes, reserved);') == 3
            and text[start:end].count('assert_eq!(prepared.snapshot().completed_executions, 0);') == 2
            and text[start:end].count('assert_eq!(memory.snapshot().reserved_bytes, 0);') == 2,
            'cleanup or zero-credit assertion removed')
    bridge = evidence.linked(prior['base_integration'])
    require(bridge['after_head'] == source['head'] and bridge['before_head'] == native_source['head']
            and bridge['tested_source_unchanged'] and bridge['working_changes_preserved'], 'base integration bridge changed')
    changed = git(repository, 'diff', '--name-only', bridge['before_head'], bridge['after_head']).decode().splitlines()
    require(changed == bridge['incoming_paths'] and all(name.startswith('docs/') for name in changed), 'base moved compiled source')
    return source, prior, native_source, blobs


def verify_gates(packet, evidence, source, prior, native_source, blobs):
    require(set(packet['core_gates']) == set(CORE_GATES), 'required gate missing')
    totals = {}
    native_log = None
    for name, (targets, expected) in CORE_GATES.items():
        result = evidence.linked(packet['core_gates'][name])
        checked_source = evidence.linked(result['source'])
        require(result['status'] == 'passed' and result['source_unchanged'], 'gate did not pass: ' + name)
        receipt, log = evidence.run(result, checked_source)
        rows = counts(log)
        require(len(rows) == targets and tuple(sum(row[i] for row in rows) for i in range(3)) == expected
                and result['test_summaries'] == [list(row) for row in rows], 'gate coverage changed: ' + name)
        if name.startswith('file-statistics-vendor-layout'):
            selected = {n: h for n, h in checked_source['source_hashes'].items()
                        if n.startswith(('vendor/vortex-layout/', 'vendor/vortex-array/')) or n in ('Cargo.toml', 'Cargo.lock')}
            require(all(source['source_hashes'].get(n) == h for n, h in selected.items()), 'layout suite source changed')
        else:
            require(checked_source['source_hashes'] == native_source['source_hashes'], 'core gate source mismatch: ' + name)
        if 'clippy' in name:
            require('clippy' in receipt['command'] and receipt['command'][-2:] == ['-D', 'warnings'], 'lint weakened')
        if name.endswith('workspace-tests-880'):
            require('--workspace' in receipt['command'] and '--all-targets' in receipt['command'], 'workspace narrowed')
        if name.endswith('native-release-880'):
            require('--release' in receipt['command'] and '--all-targets' in receipt['command']
                    and receipt['command'][receipt['command'].index('--features') + 1] == 'release-user-surfaces', 'native suite narrowed')
            native_log = log
        totals[name] = dict(targets=targets, passed=expected[0], failed=expected[1], ignored=expected[2])
    require(set(packet['phases']) == {'format', 'cli-lint', 'native-cli', 'python', 'build', 'wheel', 'references', 'vendor-file-tests'},
            'current phase missing')
    phases = {name: evidence.linked(ref) for name, ref in packet['phases'].items()}
    for name, phase in phases.items():
        require(phase['status'] == 'passed' and phase['phase'] == name, 'phase failed: ' + name)
        expected_source = packet['source'] if name in ('wheel', 'references', 'vendor-file-tests') else source['previous_source']
        require(phase['source'] == expected_source, 'phase source changed: ' + name)
    for name in ('format', 'cli-lint', 'native-cli', 'vendor-file-tests'):
        phase_source = source if name == 'vendor-file-tests' else prior
        receipt, log = evidence.run(phases[name]['run'], phase_source)
        if name == 'native-cli':
            require(len(counts(log)) == 6 and tuple(sum(row[i] for row in counts(log)) for i in range(3)) == (1091, 0, 0),
                    'CLI coverage changed')
            require('--features' in receipt['command'] and 'release-user-surfaces' in receipt['command'], 'CLI feature coverage')
        elif name == 'vendor-file-tests':
            require(counts(log) == [(145, 0, 0)] and 'vendor/vortex-file/Cargo.toml' in receipt['command'], 'final file suite coverage')
        elif name == 'format':
            require(receipt['command'][-4:] == ['fmt', '--all', '--', '--check'], 'format check weakened')
        else:
            require('clippy' in receipt['command'] and receipt['command'][-2:] == ['-D', 'warnings'], 'CLI lint weakened')
    test_inventory = evidence.linked(packet['test_inventory'])
    total = 0
    for module in test_inventory['modules']:
        raw = blobs[module['path']]
        require(digest(raw) == module['source_identity']['sha256'] and len(raw) == module['source_identity']['bytes'], 'test module changed')
        names = re.findall(r'(?m)^fn (native_\w+)\(', raw.decode())
        require(len(names) == module['test_count'], 'new test count changed')
        for name in names:
            matches = re.findall(r'(?m)^test [^\n]*::' + re.escape(name) + r' \.\.\. ok$', native_log)
            require(len(matches) == 1, 'new native test did not pass: ' + name)
        total += len(names)
    require(total == 69 and len(re.findall(r'(?m)^test .* \.\.\. ignored(?:,.*)?$', native_log)) == 31, 'native test inventory incomplete')
    require(set(phases['python']['shards']) == {'core', 'release_scripts'}, 'Python shard missing')
    for name, discovered, ran, skipped in (('core', 700, 559, 23), ('release_scripts', 168, 168, 0)):
        row = phases['python']['shards'][name]
        receipt, log = evidence.run(row['run'], prior)
        report = evidence.linked(row['report'])
        require(report['status'] == 'passed' and report['test_count'] == discovered
                and report['failure_count'] == report['error_count'] == 0 and report['skipped_count'] == skipped,
                'Python coverage changed')
        require(re.findall(r'(?m)^Ran (\d+) tests in ', log) == [str(ran)]
                and re.search(r'(?m)^OK' + (r' \(skipped=23\)' if skipped else '') + r'$', log), 'Python log mismatch')
    for refs in phases['references']['checks'].values():
        evidence.run(refs, source)
    return phases, totals


def verify_compilation(build, source, original_root):
    proof = build['compiler_input_proof']
    require(len(proof['packages']) == 13 and {row['target'] for row in proof['packages']} == TARGETS, 'compiler targets changed')
    require(set(proof['compilation_inputs']) == {name for row in proof['packages'] for name in row['local_inputs']}, 'compiler inputs omitted')
    for name, value in proof['compilation_inputs'].items():
        require(value == source['source_hashes'][name], 'compiler source changed: ' + name)
    for row in proof['packages']:
        raw = row['dependency_record_text'].encode()
        require(len(raw) == row['dependency_record']['bytes'] and digest(raw) == row['dependency_record']['sha256'], 'dependency snapshot changed')
        names = shlex.split(raw.decode().splitlines()[0].split(': ', 1)[1])
        local, generated = [], []
        for name in names:
            path = Path(name)
            path = Path(os.path.normpath(path if path.is_absolute() else original_root / path))
            if path.is_relative_to(original_root):
                local.append(str(path.relative_to(original_root)))
            else:
                require(path.is_relative_to('/Users/dylan/.cache/shardloom/cargo-target'), 'unknown generated source')
                generated.append(str(path))
        require(local == row['local_inputs'] and generated == [ref['path'] for ref in row['generated_inputs']], 'dependency paths changed')


def verify_wheel(phases, evidence, build, prior):
    compilation = evidence.linked(phases['build']['build'])
    require(compilation['status'] == 'passed' and compilation['source_commit'] == prior['head'], 'build failed')
    receipt, log = evidence.run(phases['build']['cli'], prior)
    evidence.run(phases['build']['extension'], prior)
    events = [json.loads(line) for line in log.splitlines() if line.startswith('{')]
    artifact = compilation['cli_compiler_artifact']
    require(artifact in events and {'reason': 'build-finished', 'success': True} in events
            and artifact['profile']['opt_level'] == '3' and not artifact['profile']['test']
            and 'release-user-surfaces' in artifact['features'], 'release artifact provenance')
    require(compilation['binary']['sha256'] == build['binary_sha256']
            and compilation['library'] == build['native_library'], 'packaged build changed')
    wheel = evidence.linked(phases['wheel']['report'])
    evidence.run(phases['wheel']['run'])
    require(wheel['status'] == wheel['uninstall_status'] == wheel['integration']['status'] == 'passed'
            and wheel['integration'] == {'tests': 118, 'failures': 0, 'errors': 0, 'skipped': 0, 'status': 'passed'}
            and wheel['installed_load']['isolated_install'] and not wheel['publication_attempted'], 'installed proof incomplete')
    require(wheel['integrity']['cli_sha256'] == build['binary_sha256']
            and wheel['integrity']['native_library_sha256'] == build['native_library']['sha256'], 'installed bytes changed')
    root = Path(phases['wheel']['report']['path']).parent
    require([row['name'] for row in wheel['steps']] == ['build', 'create_environment', 'install_optional_consumer',
            'install', 'load', 'integration', 'uninstall', 'verify_uninstalled'], 'package lifecycle incomplete')
    for row in wheel['steps']:
        require(row['returncode'] == 0 and digest(evidence.raw(root / row['log'])) == row['log_sha256'], 'wheel step failed')
        if row['name'] == 'integration':
            require(row['command'][-2] == str(Path(packet_root(build)) / 'python/tests'), 'wheel test overlay')
    require(re.findall(r'(?m)^Ran (\d+) tests in ', evidence.raw(root / 'integration.log').decode()) == ['118'], 'wheel log coverage')


def packet_root(build):
    return Path(build['cli_compiler_artifact']['manifest_path']).parent.parent


def verify_public(accepted, plan, build, evidence):
    summary = evidence.linked(accepted['summary'])
    require(summary['status'] == 'passed' and len(summary['families']) == 39
            and summary['families'] == plan['families'] and len(summary['cases']) == 5930
            and Counter(row['status'] for row in summary['cases']) == {'passed': 5930}, 'public coverage incomplete')
    require(summary['performance_claim'] is False and summary['whole_process_memory_ceiling'] is False
            and summary['sources'][build['binary']] == build['binary_sha256'], 'public scope changed')
    root = Path(accepted['summary']['path']).parent
    seen, envelopes = set(), 0
    for row in summary['compressed_json_files']:
        require(row['path'] not in seen, 'duplicate public log')
        seen.add(row['path'])
        packed = evidence.raw(root / row['path'])
        raw = gzip.decompress(packed)
        require(len(packed) == row['bytes'] and digest(packed) == row['sha256']
                and len(raw) == row['raw_bytes'] and digest(raw) == row['raw_sha256'], 'public archive changed')
        value = json.loads(raw)
        if isinstance(value, dict) and 'fallback' in value:
            require(value['fallback']['attempted'] is False and value['fallback']['allowed'] is False
                    and value['fallback']['engine'] is None, 'public fallback marker')
            fields = value['fields']
            for item in fields:
                if item['key'].endswith(('fallback_attempted', 'external_engine_invoked')):
                    require(item['value'] in ('false', False), 'public external execution')
            envelopes += 1
    require(envelopes == accepted['no_fallback_envelopes'] and len(seen) == accepted['compressed_log_members_verified'],
            'public archive inventory changed')
    return dict(public_cases=5930, public_families=39, public_envelopes=envelopes)


def verify_full43(accepted, plan, build, evidence, helpers, *, selected=None, input_binding=None):
    equivalent, extract_result, report_fields, query_phase_timings, timing_totals, score = helpers
    selected = list(range(1, 44)) if selected is None else selected
    is_full = selected == list(range(1, 44))
    full = evidence.linked(accepted['summary'])
    require(full['complete'] and full['full_result_validation'] == is_full and full['repeats'] == 1
            and full['selected_query_ids'] == selected and len(full['records']) == len(selected)
            and full['score_scope'] == ('full_43' if is_full else 'targeted_queries_only')
            and full['hot_total_seconds'] is None and full['reference_kind'] == 'retained_native_regression', 'Full43 incomplete')
    require([(row['query'], row['run']) for row in full['records']] == [(query, 1) for query in selected], 'Full43 query omitted or duplicated')
    require(full['binary_sha256'] == build['binary_sha256'] and full['build_commit'] == build['source_commit']
            and full['source'] == plan['input']['path'] and full['source_generation'] == plan['input']['generation']
            and full['completed_identity_check'] and full['reference_packet_identity_verified']
            and full['harness_identity_verified'] and full['engine_timing_required'], 'Full43 provenance changed')
    require(full['memory_gb'] == 24 and full['max_parallelism'] == 12
            and full['log_budget_bytes'] == {'retained': 1024 << 20, 'active_run': 256 << 20}, 'Full43 resources changed')
    binding = evidence.linked(accepted['pre_and_post_input_hash_binding']) if input_binding is None else input_binding
    require(binding['status'] == 'passed' and binding['full_input_pre_and_post_hash_verified'], 'full input identity proof absent')
    reference = evidence.linked(plan['reference'])
    require(reference['queries_sha256'] == plan['query_source']['sha256'] == full['queries_sha256'], 'query set changed')
    references = {row['query_id']: row for row in reference['records']}
    require(len(references) == len(reference['records']) == 43 and set(references) == set(range(1, 44)), 'reference coverage changed')
    schemas = {int(query): tuple(value) for ref in plan['schema_sources']
               for query, value in evidence.linked(ref)['expected_schemas'].items()}
    require(set(schemas) == set(range(1, 44)), 'schema coverage changed')
    nanos, rows, observations = 0, 0, []
    root = Path(accepted['summary']['path']).parent
    for row in full['records']:
        require(row['passed'] and row['validation'] == 'complete_values' and row['run'] == 1
                and row['returncode'] == 0 and not row['guard_failures'], 'Full43 row failed')
        require(row['command'][0] == build['binary']
                and row['command'][row['command'].index('--input') + 1] == plan['input']['path'], 'Full43 source substituted')
        envelope = None
        for item in row['completed_log_archives']:
            packed = evidence.raw(item['path'])
            raw = gzip.decompress(packed)
            require(len(packed) == item['gzip_bytes'] and digest(packed) == item['gzip_sha256']
                    and len(raw) == item['raw_bytes'] and digest(raw) == item['raw_sha256'], 'Full43 archive changed')
            if item['path'].endswith('.stdout.json.gz'):
                require(envelope is None, 'duplicate Full43 payload')
                envelope = json.loads(raw)
        require(envelope is not None and envelope['status'] == 'success' and not envelope['fallback']['attempted'], 'Full43 execution failed')
        fields = report_fields(envelope)
        require(all(value in (False, 'false') for key, value in fields.items()
                    if key.endswith(('fallback_attempted', 'external_engine_invoked'))), 'Full43 fallback marker')
        actual = extract_result(envelope)
        expected = references[row['query']]
        require(canonical(expected['values']) == expected['values_sha256']
                and equivalent(actual, expected['values']) and canonical(actual) == row['result_sha256'], 'Full43 values changed')
        require((fields['result_schema_format'], json.loads(fields['result_schema_json'])) == schemas[row['query']], 'Full43 types changed')
        timing = evidence.read(root / f"q{row['query']:02d}_run1.timing.json")
        require(timing['returncode'] == 0 and all(timing[key] == row[key] for key in ('seconds', 'user_cpu_seconds', 'system_cpu_seconds'))
                and timing['peak_rss_bytes'] == row['native_peak_rss_bytes'], 'Full43 process record changed')
        require(all(row[key] == value for key, value in query_phase_timings(fields, timing['seconds']).items()), 'Full43 clock changed')
        require(any(item['kind'] == 'native_io_certificate' and item['status'] == 'available' for item in envelope['certificates']),
                'Full43 native certificate absent')
        require(fields['execution_resource_declared_memory_bytes'] == str(24 << 30)
                and fields['execution_resource_declared_max_parallelism'] == '12'
                and fields['execution_resource_whole_process_memory_limit_enforced'] == 'false', 'Full43 resource certificate changed')
        nanos += int(fields['native_query_execution_nanos'])
        observations.append(dict(query=row['query'], engine_execution_nanos=int(fields['native_query_execution_nanos']),
                                 engine_execution_seconds=str(Decimal(fields['native_query_execution_nanos']) / Decimal(1_000_000_000))))
        rows += len(actual)
    seconds = str(Decimal(nanos) / Decimal(1_000_000_000))
    require(rows == sum(len(references[q]['values']) for q in selected) and seconds == accepted['engine_execution_seconds']
            and nanos == accepted['engine_execution_nanos'], 'Full43 engine total changed')
    if 'observations' in accepted:
        require(accepted['observations'] == observations, 'per-query observation changed')
    require(all(full[key] == value for key, value in score(full['records'], 43, selected, repeats=1, timing_key='engine_execution_seconds').items()), 'Full43 score changed')
    require(full['secondary_process_wall'] == score(full['records'], 43, selected, repeats=1)
            and full['timing_totals'] == timing_totals(full['records']), 'Full43 diagnostic totals changed')
    csv_rows = list(csv.DictReader(io.StringIO(evidence.reference(accepted['timing_csv']).decode())))
    require(len(csv_rows) == len(selected) and [int(row['query']) for row in csv_rows] == selected
            and all(int(row['run']) == 1 and row['passed'] == row['engine_timing_reported'] == 'True' for row in csv_rows), 'Full43 timing table changed')
    require(all(Decimal(row['engine_execution_seconds']) == Decimal(observed['engine_execution_seconds'])
                for row, observed in zip(csv_rows, observations, strict=True)), 'Full43 timing table clock changed')
    return dict(full43_queries=len(selected), full43_complete_rows=rows, full43_engine_execution_seconds=seconds)


def verify_timing_diagnosis(packet, evidence, source, plan, build, helpers):
    require(set(packet['timing_diagnosis']) == {'targeted', 'comparison', 'isolated'}, 'timing history omitted')
    records = {name: evidence.linked(ref) for name, ref in packet['timing_diagnosis'].items()}
    totals = {}

    def member_ref(path):
        row = evidence.members[str(path)]
        return dict(path=str(path), bytes=row['bytes'], sha256=row['sha256'])

    def accepted_group(group):
        observations = group['observations']
        nanos = sum(row['engine_execution_nanos'] for row in observations)
        summary = evidence.linked(group['summary'])
        return dict(summary=group['summary'], timing_csv=member_ref(summary['timing_csv']),
                    engine_execution_nanos=nanos, engine_execution_seconds=str(Decimal(nanos) / Decimal(1_000_000_000)),
                    observations=observations)

    for name, selected, order in (
            ('targeted', [2, 3, 21, 24, 34], [('control', 'a'), ('candidate', 'a'), ('candidate', 'b'), ('control', 'b')]),
            ('comparison', list(range(1, 44)), [('candidate', 'a'), ('control', 'a')])):
        result = records[name]
        protocol = evidence.linked(result['plan'])
        evidence.reference(protocol['helper'])
        require(result['status'] == 'passed' and result['source_unchanged'] and result['input_pre_and_post_hash_verified']
                and result['complete_values_and_schemas_verified'] and not result['performance_claim'], 'timing diagnosis incomplete')
        require(protocol['source'] == packet['source'] and protocol['workflow_plan'] == packet['plan']
                and protocol['input'] == plan['input'] and protocol['reference'] == plan['reference']
                and protocol['schema_sources'] == plan['schema_sources'] and protocol['query_ids'] == selected
                and protocol['expected_calls'] == result['calls'] == len(selected) * len(order), 'timing protocol changed')
        require([(row['role'], row['cohort']) for row in protocol['commands']] == order
                and [(row['role'], row['cohort']) for row in result['groups']] == order, 'timing group omitted or reordered')
        require(result['full43_result'] == (name == 'comparison'), 'targeted result mislabeled Full43')
        group_totals = {}
        query_times = {role: {q: [] for q in selected} for role in ('candidate', 'control')}
        for command, group in zip(protocol['commands'], result['groups'], strict=True):
            role = group['role']
            chosen = protocol['builds'][role]
            evidence.linked(chosen['manifest'])
            if role == 'candidate':
                require(chosen['binary']['path'] == build['binary'] and chosen['binary']['sha256'] == build['binary_sha256']
                        and chosen['source_commit'] == source['head'], 'diagnostic candidate changed')
            else:
                control = evidence.linked(chosen['manifest'])
                require(control['binary'] == chosen['binary'] and control['source_commit'] == chosen['source_commit']
                        and control['status'] == 'passed'
                        and control['cli_compiler_artifact']['features'] == build['cli_compiler_artifact']['features']
                        and control['cli_compiler_artifact']['profile'] == build['cli_compiler_artifact']['profile'], 'diagnostic control changed')
            expected = plan['commands']['full43'].copy()
            for flag, value in {'--binary': chosen['binary']['path'], '--build-commit': chosen['source_commit'],
                                '--uat-root': command['output']}.items():
                expected[expected.index(flag) + 1] = value
            if name == 'targeted':
                expected += ['--query-ids', ','.join(map(str, selected))]
            receipt, _ = evidence.run(group['run'], source)
            require(receipt['command'] == command['command'] == expected, 'diagnostic command changed')
            selected_build = dict(binary=chosen['binary']['path'], binary_sha256=chosen['binary']['sha256'], source_commit=chosen['source_commit'])
            accepted = accepted_group(group)
            verify_full43(accepted, plan, selected_build, evidence, helpers, selected=selected,
                          input_binding=dict(status=result['status'], full_input_pre_and_post_hash_verified=result['input_pre_and_post_hash_verified']))
            group_totals[role + '-' + group['cohort']] = accepted['engine_execution_seconds']
            for observation in group['observations']:
                query_times[role][observation['query']].append(observation['engine_execution_nanos'])
        expected_comparisons = [dict(query=q, control_engine_nanos=query_times['control'][q],
                                     candidate_engine_nanos=query_times['candidate'][q],
                                     candidate_to_control_median_ratio=statistics.median(query_times['candidate'][q]) /
                                     statistics.median(query_times['control'][q])) for q in selected]
        require(result['comparisons'] == expected_comparisons, 'diagnostic comparison arithmetic changed')
        if name == 'comparison':
            require(result['engine_totals_nanos'] == {group['role']: sum(row['engine_execution_nanos'] for row in group['observations'])
                                                     for group in result['groups']}, 'comparison totals changed')
            require(result['candidate_to_control_total_ratio'] == result['engine_totals_nanos']['candidate'] /
                    result['engine_totals_nanos']['control'], 'comparison ratio changed')
        else:
            correction = evidence.linked(result['verifier_correction'])
            require(correction['original_plan'] == result['plan'] and correction['source'] == packet['source']
                    and correction['completed_control'] == result['groups'][0]['run']
                    and correction['summary'] == result['groups'][0]['summary']
                    and correction['original_helper'] == protocol['helper']
                    and correction['original_exception'] == 'AssertionError at check_structural_timing_885.py:94'
                    and correction['status'] == 'verification_failed_after_completed_control'
                    and correction['measurements_repeated_or_dropped'] is False
                    and correction['query_order_and_limits_changed'] is False
                    and correction['original_scoring_decision_unchanged']
                    and result['original_control_preserved_without_rerun'], 'targeted verifier correction lost')
            evidence.reference(correction['continuation_helper'])
        totals[name] = group_totals

    single = records['isolated']
    protocol = evidence.linked(single['plan'])
    receipt = evidence.linked(single['run'])
    evidence.reference(protocol['helper'])
    require(protocol['source'] == packet['source'] and protocol['input'] == plan['input']
            and protocol['reference'] == plan['reference'] and protocol['schema_sources'] == plan['schema_sources']
            and protocol['build']['binary']['path'] == build['binary']
            and protocol['build']['binary']['sha256'] == build['binary_sha256']
            and protocol['build']['source_commit'] == source['head']
            and protocol['supervisor'] == evidence.supporting['process_supervisor']
            and protocol['workload_guard'] == evidence.supporting['workload_guard'], 'isolated protocol changed')
    require(protocol['expected_calls'] == 43 and protocol['repeats'] == 1
            and protocol['query_ids'] == list(range(1, 44)) and not protocol['parallel_work']
            and not protocol['extra_profiling'] and not protocol['artificial_pacing'], 'isolated run scope changed')
    expected = plan['commands']['full43'].copy()
    expected[expected.index('--uat-root') + 1] = protocol['command'][protocol['command'].index('--uat-root') + 1]
    require(receipt['command'] == protocol['command'] == expected and receipt['plan'] == single['plan']
            and receipt['source'] == packet['source'] and receipt['input'] == plan['input'], 'isolated command changed')
    require(receipt['status'] == 'passed' and receipt['returncode'] == 0
            and receipt['process_group_drained'] and receipt['direct_child_reaped'] and receipt['uat_lock_released']
            and receipt['timed_out'] is False and receipt['interrupted_signal'] is None
            and receipt['group_absence_unproved_at_cleanup'] is False
            and receipt['source_unchanged'] and receipt['pre_input_hash_verified'] and receipt['post_input_hash_verified'], 'isolated cleanup or identity failed')
    require(len(evidence.reference(receipt['log'])) == receipt['output_bytes'], 'isolated run log changed')
    require(single['status'] == 'passed' and single['queries'] == 43 and single['repeats'] == 1
            and single['result_rows_checked'] == 381 and single['complete_values_and_schemas_verified']
            and single['no_fallback_verified'] and single['source_unchanged'] and single['input_pre_and_post_hash_verified']
            and not single['parallel_task_work'], 'isolated result incomplete')
    accepted = dict(summary=single['summary'], timing_csv=single['timing_csv'],
                    engine_execution_nanos=single['engine_total_nanos'], engine_execution_seconds=single['engine_total_seconds'],
                    observations=single['observations'])
    replay = verify_full43(accepted, plan, build, evidence, helpers,
                          input_binding=dict(status=receipt['status'], full_input_pre_and_post_hash_verified=
                                             receipt['pre_input_hash_verified'] and receipt['post_input_hash_verified']))
    totals['isolated'] = replay['full43_engine_execution_seconds']
    return dict(diagnostic_targeted_calls=20, diagnostic_comparison_calls=86, isolated_full43_queries=43,
                isolated_full43_engine_seconds=totals['isolated'], preserved_timing_observations=totals,
                timing_variation_cause_established=False)


def verify_record_values(payload, evidence, build, helpers):
    equivalent, extract_result, report_fields, workloads, round_float = helpers
    require(payload['status'] == 'passed' and payload['independent_reference'] is True
            and payload['performance_claim'] is False and not payload['query_answers_cached_by_native_adapter'], 'invalid format report')
    config = payload['configuration']
    require(config['repeats'] == 1 and payload['reference_engine'] == 'pandas'
            and set(payload['engines']) == {'pandas', 'shardloom'}, 'format execution roles')
    require(config['scenarios'] == list(workloads) and config['rows'] == 1000
            and config['dim_rows'] == 20 and config['memory_gb'] == 1 and config['max_parallelism'] == 2, 'format workload changed')
    expected = {(engine, data_format, scenario, 1) for engine in payload['engines']
                for data_format in config['formats'] for scenario in config['scenarios']}
    rows = payload['records']
    actual = [(row['engine'], row['format'], row['scenario'], row['repeat']) for row in rows]
    require(len(actual) == len(expected) and set(actual) == expected, 'format coverage changed')
    reference = {(row['format'], row['scenario']): row['result'] for row in rows if row['engine'] == 'pandas'}
    calls_verified = 0

    def call(item, native):
        nonlocal calls_verified
        require(item['returncode'] == 0 and not item['guard_failures'], 'format process failed')
        raw = evidence.raw(item['stdout'])
        require(digest(raw) == item['stdout_sha256'], 'format process output changed')
        envelope = evidence.read(item['stdout'])
        if native:
            require(item['binary_sha256'] == build['binary_sha256'] and item['command'][0] == build['binary'], 'format native binary changed')
            require(envelope['status'] == 'success' and envelope['fallback']['attempted'] is False
                    and envelope['fallback']['engine'] is None, 'format native execution failed')
            fields = report_fields(envelope)
            require(all(value in (False, 'false') for key, value in fields.items()
                        if key.endswith(('fallback_attempted', 'external_engine_invoked'))), 'format fallback marker')
        else:
            require(envelope['status'] == 'passed' and envelope['execution_role'] == 'independent_comparison_only', 'format oracle role changed')
        calls_verified += 1
        return envelope

    for row in rows:
        require(row['status'] == 'passed' and row['matches_reference'] is True
                and canonical(row['result']) == row['result_sha256'], 'format complete result changed')
        require(equivalent(row['result'], reference[row['format'], row['scenario']]), 'native/oracle values differ')
        if row['engine'] == 'pandas':
            result = call(row['receipt'], False)
            require(canonical(result['result']) == row['result_sha256'], 'oracle process result changed')
            continue
        fields = row['evidence']
        require(fields['public_workflow_fallback_attempted'] == fields['public_workflow_external_engine_invoked'] == 'false', 'format fallback evidence changed')
        calls = json.loads(fields['benchmark_native_calls'])
        readbacks = json.loads(fields['benchmark_output_validation_calls'])
        workload = workloads[row['scenario']]
        first = int(workload.write_statement is not None)
        require(len(calls) == len(workload.statements) + first, 'format native calls omitted')
        batches, writer = [], 0
        for index, item in enumerate(calls):
            envelope = call(item, True)
            if 'output' in item:
                require(writer < len(readbacks), 'output readback omitted')
                readback = readbacks[writer]
                writer += 1
                if readback.get('kind') == 'complete_json_file_readback':
                    raw = evidence.raw(readback['path'])
                    require(digest(raw) == readback['sha256'] and len(raw) == readback['bytes'], 'written JSON changed')
                    result = json.loads(raw) if readback['format'] == 'json' else [json.loads(line) for line in raw.splitlines() if line]
                else:
                    result = extract_result(call(readback, True))
            else:
                result = extract_result(envelope)
            if index >= first:
                batches.append(result)
        require(writer == len(readbacks), 'extra output readback')
        require(equivalent(workload.result(batches, round_float), row['result']), 'native payload differs from reported format result')
    return len(rows), calls_verified


def verify_modular(accepted, plan, build, evidence, helpers, format_order):
    environment = evidence.linked(plan['dependency_environment'])
    binding = evidence.linked(accepted['dependency_binding'])
    require(binding['status'] == 'passed' and binding['private_dependency_files_verified_before_and_after']
            and binding['dependency_environment'] == plan['dependency_environment'], 'format environment binding absent')
    expected = {'pandas': '2.2.3', 'numpy': '2.3.5', 'python-dateutil': '2.9.0.post0',
                'pytz': '2026.5', 'tzdata': '2026.5', 'six': '1.17.0', 'fastavro': '1.12.2', 'pyarrow': '25.0.1'}
    require(environment['status'] == 'passed' and environment['private_environment']
            and environment['imports']['distributions'] == expected
            and environment['imports']['user_site'] is False, 'format dependency versions changed')
    site = Path(environment['imports']['prefix']) / 'lib/python3.12/site-packages'
    require(all(Path(path).is_relative_to(site) for path in environment['imports']['modules'].values())
            and not any('site-packages' in path and path != str(site) for path in environment['imports']['path']), 'shared format environment')
    require({row['name']: row['version'] for row in environment['origins']} == expected, 'format package origin omitted')
    copied = sorted([row for origin in environment['origins'] for row in origin['files']], key=lambda row: row['relative_path'])
    require(copied == sorted(environment['files'], key=lambda row: row['relative_path']), 'private package copy changed')
    require(plan['commands']['modular'][1:4] == ['PYTHONPATH=' + str(site), 'PYTHONNOUSERSITE=1', environment['python']['path']], 'format Python changed')
    matrix = evidence.linked(accepted['matrix'])
    require(matrix['status'] == 'passed' and len(matrix['cohorts']) == 18 and matrix['records'] == 1408
            and matrix['native_records'] == 704 and not matrix['complete_cartesian_input_output_coverage_claimed']
            and not matrix['performance_claim'], 'format matrix incomplete')
    require(matrix['binary_sha256'] == build['binary_sha256'] and matrix['binary'] == build['binary']
            and matrix['runtime_commit'] == matrix['harness_commit'] == build['source_commit'], 'format provenance changed')
    expected_cohorts = [(state, 'collect', list(format_order)) for state in ('raw', 'prepared')]
    expected_cohorts += [(state, output, ['csv']) for state in ('raw', 'prepared')
                         for output in ('vortex', 'json', 'jsonl', 'csv', 'parquet', 'arrow_ipc', 'avro', 'orc')]
    require([(row['input_state'], row['output_format'], row['formats']) for row in matrix['cohorts']] == expected_cohorts, 'format lifecycle coverage changed')
    total, calls = 0, 0
    for row in matrix['cohorts']:
        require(digest(evidence.raw(row['report'])) == row['report_sha256'] and row['independently_reopened']
                and row['command'][0] == environment['python']['path'], 'format report changed')
        payload = evidence.read(row['report'])
        require(all(payload['configuration'][key] == row[key] for key in ('input_state', 'output_format', 'formats')), 'format configuration mismatch')
        nrows, ncalls = verify_record_values(payload, evidence, build, helpers)
        require(nrows == row['observed_records'], 'format record count changed')
        total += nrows
        calls += ncalls
    require(total == 1408, 'format record omitted')
    return dict(modular_records=total, native_workflows=704, independent_oracle_workflows=704,
                modular_payload_calls_replayed=calls, private_package_files=len(environment['files']))


def verify(packet, repository, blobs=None):
    evidence = Evidence(packet)
    source, prior, native_source, blobs = verify_sources(packet, evidence, repository, blobs)
    completion = evidence.linked(packet['completion'])
    require(completion['status'] == 'passed' and completion['source'] == packet['source']
            and completion['source_file_count'] == 2594 and completion['final_source_verified']
            and completion['finite_structural_scope_accepted']
            and completion['broader_provider_acceptance_complete'] is False
            and completion['performance_claim'] is False, 'finite completion record changed')
    phases, totals = verify_gates(packet, evidence, source, prior, native_source, blobs)
    plan = evidence.linked(packet['plan'])
    build = evidence.linked(plan['build'])
    workflow_source = evidence.linked(plan['source'])
    require(workflow_source['source_manifest'] == packet['source'] and workflow_source['source_hashes'] == source['source_hashes']
            and build['source_hashes'] == source['source_hashes']
            and build['source_commit'] == workflow_source['source_commit'] == source['head'], 'workflow source changed')
    require(not plan['performance_claim'] and not plan['reingested'] and not plan['resource_limits_changed'], 'workflow scope changed')
    original_root = packet_root(build)
    verify_compilation(build, source, original_root)
    verify_wheel(phases, evidence, build, prior)
    require(set(packet['acceptance']) == {'public', 'full43', 'modular'}, 'workflow family omitted')
    accepted = {name: evidence.linked(ref) for name, ref in packet['acceptance'].items()}
    for name, row in accepted.items():
        require(row['status'] == 'passed' and row['phase'] == name and row['plan'] == packet['plan']
                and row['source_commit'] == row['package_source_commit'] == source['head']
                and not row['performance_claim'], 'workflow acceptance changed')
        receipt, _ = evidence.run(row['run'], source)
        require(receipt['command'] == plan['commands'][name], 'workflow command changed')
    helpers = ('scripts/native_workflow_protocol.py', 'scripts/run_clickbench_query_uat.py',
               'scripts/query_timing_report.py', 'benchmarks/traditional_analytics/workloads.py',
               'benchmarks/traditional_analytics/comparison.py', 'benchmarks/traditional_analytics/benchmark_models.py')
    for name in helpers:
        require(digest((repository / name).read_bytes()) == source['source_hashes'][name], 'replay helper changed: ' + name)
    sys.path[:0] = [str(repository / 'scripts'), str(repository)]
    from native_workflow_protocol import extract_result, report_fields
    from run_clickbench_query_uat import equivalent, score
    from query_timing_report import query_phase_timings, timing_totals
    from benchmarks.traditional_analytics.workloads import WORKLOADS
    from benchmarks.traditional_analytics.comparison import round_float
    from benchmarks.traditional_analytics.benchmark_models import FORMAT_ORDER
    public = verify_public(accepted['public'], plan, build, evidence)
    full = verify_full43(accepted['full43'], plan, build, evidence,
                        (equivalent, extract_result, report_fields, query_phase_timings, timing_totals, score))
    timing = verify_timing_diagnosis(packet, evidence, source, plan, build,
                                    (equivalent, extract_result, report_fields, query_phase_timings, timing_totals, score))
    modular = verify_modular(accepted['modular'], plan, build, evidence,
                            (equivalent, extract_result, report_fields, WORKLOADS, round_float), FORMAT_ORDER)
    history = evidence.linked(packet['history_inventory'])
    require(len(history['records']) == 228, 'historical record omitted')
    for row in history['complete_inspected_input_identities']:
        evidence.reference(row)
    failed, passed = 0, 0
    for name in evidence.members:
        # Root process stdout may contain several JSON progress objects; it is
        # a raw output member, not a guarded-run receipt document.
        if not name.endswith('.json') or name.endswith('.stdout.json') or Path(name).parent != Path(packet['source']['path']).parent:
            continue
        value = evidence.read(name)
        if isinstance(value, dict) and 'runner_sha256' in value and 'process_group_drained' in value:
            require(value['status'] in ('passed', 'failed'), 'unknown historical run status')
            raw = evidence.raw(name.removesuffix('.json') + '.log')
            require(digest(raw) == value['log_sha256'] and len(raw) == value['output_bytes'], 'historical run log changed')
            require(value['process_group_drained'] and value['direct_child_reaped'], 'historical run undrained')
            require((value['returncode'] == 0) == (value['status'] == 'passed'), 'historical outcome changed')
            failed += value['status'] == 'failed'
            passed += value['status'] == 'passed'
    require(failed >= 1 and passed >= 1, 'historical failures missing')
    return dict(status='passed', source_commit=packet['code_commit'], source_files=len(blobs),
                native_tests=3176, native_ignored=31, workspace_tests=3235, cli_tests=1091,
                new_native_tests=69, vendor_file_tests=145, vendor_layout_tests=249,
                installed_tests=118, core_gates=totals, preserved_failed_runs=failed,
                guarded_passed_runs=passed, members=len(evidence.members), **public, **full, **modular, **timing,
                broader_perf03_complete=False, performance_claim=False,
                native_execution_performed=False, payload_residency_revalidated=False,
                publication_attempted=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_bytes())
    require(manifest['schema'] == 'shardloom.structural_acceptance_evidence.v1', 'manifest schema')
    require(digest(Path(__file__).read_bytes()) == manifest['verifier']['sha256'], 'verifier identity changed')
    packed = (args.manifest.parent / manifest['packet']['file']).read_bytes()
    require(len(packed) == manifest['packet']['bytes'] and digest(packed) == manifest['packet']['sha256'], 'packet identity changed')
    packet = json.loads(lzma.decompress(packed))
    require(packet['code_commit'] == manifest['code_commit'], 'manifest source mismatch')
    result = verify(packet, args.repository)
    require(result == manifest['replay'], 'replay summary changed')
    if args.output:
        with args.output.open('x') as stream:
            json.dump(result, stream, indent=2, sort_keys=True)
            stream.write('\n')
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
