# SPDX-License-Identifier: Apache-2.0
"""Replay prepared-sampling acceptance without native execution or resident datasets."""
import argparse
import ast
import base64
from collections import Counter
from decimal import Decimal
import difflib
import gzip
import hashlib
import importlib.util
import json
import lzma
import os
import shlex
import tempfile
import tomllib
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
        require(packet['schema'] == 'shardloom.prepared_sampling_integration_packet.v1', 'changed packet schema')
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


def verify_compilation(proof, sources, original_root, expected_targets):
    require({row['target'] for row in proof['packages']} == expected_targets, 'compiler target coverage changed')
    require(len(proof['packages']) == len(expected_targets), 'duplicate compiler target')
    require(set(proof['compilation_inputs']) == {name for row in proof['packages'] for name in row['local_inputs']},
            'compiler input coverage changed')
    for name, value in proof['compilation_inputs'].items():
        require((value['sha256'] if isinstance(value, dict) else value) == sources[name], 'compiler source changed: ' + name)
    for row in proof['packages']:
        raw = row['dependency_record_text'].encode()
        require(len(raw) == row['dependency_record']['bytes'] and digest(raw) == row['dependency_record']['sha256'],
                'compiler dependency snapshot changed')
        names = shlex.split(raw.decode().splitlines()[0].split(': ', 1)[1])
        local, generated = [], []
        for name in names:
            path = Path(name)
            path = Path(os.path.normpath(path if path.is_absolute() else original_root / path))
            if path.is_relative_to(original_root):
                local.append(str(path.relative_to(original_root)))
            else:
                require(path.is_relative_to('/Users/dylan/.cache/shardloom/cargo-target'), 'unexpected generated source location')
                generated.append(str(path))
        require(local == row['local_inputs'] and generated == [ref['path'] for ref in row['generated_inputs']],
                'compiler dependency paths changed')


def verify_required_gates(source, final, evidence, repository):
    integration_phases = ('native-focused', 'native-all', 'cli-focused', 'cli-all', 'extension-build', 'python-focused')
    broad_phases = ('format', 'workspace-lint', 'workspace-tests', 'native-cli-lint', 'minimal-feature',
                    'cyclic-regression', 'optional-tests', 'python', 'references')
    expected_gates = {f'prepared-sampling-integration-861-r5-{phase}-verified.json' for phase in integration_phases}
    expected_gates.add('prepared-sampling-integration-861-r5-python-environment-binding.json')
    expected_gates.update(f'prepared-sampling-gates-864-{phase}-verified.json' for phase in broad_phases)
    expected_gates.update(f'prepared-sampling-856-candidate-r5-{phase}.{suffix}'
                          for phase in ('clippy', 'build', 'unit') for suffix in ('json', 'log'))
    require(len(source['required_gates']) == len(expected_gates)
            and {Path(ref['path']).name for ref in source['required_gates']} == expected_gates,
            'required gate coverage changed')
    gate_records = {}
    for ref in source['required_gates']:
        evidence.check(ref)
        if ref['path'].endswith('-verified.json'):
            record = evidence.read(ref['path'])
            require(record['status'] == 'passed' and record['source_commit'] == source['source_commit']
                    and record['source'] == source['source_manifest'],
                    'required gate failed or changed source')
            gate_records[Path(ref['path']).name] = record
    def rust_counts(log):
        return [tuple(map(int, row)) for row in re.findall(
            r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;', log)]
    def gate_log(run):
        receipt = evidence.read(evidence.check(run['receipt']))
        raw = evidence.raw(evidence.check(run['log']))
        require(receipt['status'] == 'passed' and receipt['returncode'] == 0
                and receipt['head'] == source['source_commit'] and receipt['working_tree'].strip() == ''
                and receipt['process_group_drained'] and receipt['direct_child_reaped']
                and digest(raw) == receipt['log_sha256'], 'required gate receipt changed')
        return raw.decode()
    unit = rust_counts(gate_log(final['verification']['unit']))
    require(unit == [(3, 0, 0), (10, 0, 0), (20, 0, 0)], 'unit coverage changed')
    python_gate = gate_records['prepared-sampling-integration-861-r5-python-focused-verified.json']
    require(python_gate['test_count'] == 3 and python_gate['skipped'] == 0, 'Python sampling coverage changed')
    environment_ref = next(ref for ref in source['required_gates']
                           if Path(ref['path']).name == 'prepared-sampling-integration-861-r5-python-environment-binding.json')
    environment = evidence.read(environment_ref['path'])
    evidence.references(environment)
    python_receipt = evidence.read(Path(environment_ref['path']).parent / 'prepared-sampling-integration-861-r5-python-focused-a1.json')
    require(environment['status'] == 'passed' and environment['dependency_files_unchanged']
            and environment['command'][:2] == python_receipt['command'][:2]
            and environment['command'][2:] == [python_gate['helper']['path'], 'python-focused', '--revision', '5']
            and python_receipt['command'][2:] == [environment['helper']['path'], 'focus', '--revision', '5']
            and python_receipt['status'] == 'passed' and python_receipt['returncode'] == 0
            and python_receipt['head'] == source['source_commit'] and python_receipt['working_tree'].strip() == ''
            and python_receipt['process_group_drained'] and python_receipt['direct_child_reaped'],
            'Python environment binding changed')
    for phase in broad_phases:
        record = gate_records[f'prepared-sampling-gates-864-{phase}-verified.json']
        evidence.references(record)
        require(record['phase'] == phase, 'broad gate phase changed')
        if phase in ('format', 'workspace-lint', 'native-cli-lint', 'minimal-feature'):
            gate_log(record['run'])
        elif phase == 'workspace-tests':
            counts = rust_counts(gate_log(record['run']))
            require(len(counts) == 107 and sum(row[0] for row in counts) >= 3235
                    and all(row[1:] == (0, 0) for row in counts)
                    and record['counts'] == [list(row) for row in counts], 'workspace coverage changed')
        elif phase == 'cyclic-regression':
            require(rust_counts(gate_log(record['run'])) == [(1, 0, 0)], 'corrected cyclic regression omitted')
        elif phase == 'optional-tests':
            optional = {'output_window_observation::tests': 16, 'credit_window::tests': 9, 'encoded_arrow_control_': 11}
            require(set(record['checks']) == set(optional), 'optional test family omitted')
            for name, count in optional.items():
                require(rust_counts(gate_log(record['checks'][name]['run'])) == [(count, 0, 0)],
                        'optional test coverage changed')
        elif phase == 'python':
            require(set(record['shards']) == {'core', 'release_scripts'}, 'Python shard omitted')
            correction = evidence.read(evidence.check(record['count_correction']))
            require(correction['status'] == 'validator_count_corrected'
                    and correction['source_commit'] == source['source_commit']
                    and correction['source'] == source['source_manifest']
                    and correction['original_helper'] == record['original_helper']
                    and correction['completion_helper'] == record['helper']
                    and correction['original_shards'] == record['shards']
                    and not correction['passing_shards_rerun'] and not correction['source_changed'],
                    'Python count correction provenance changed')
            require(correction['failed_postcondition'] == dict(phase='python',
                    expression="report['test_count'] >= minimum", expected_minimum=168,
                    observed_discovered=167, exception='AssertionError', test_process_status='passed',
                    test_process_returncode=0, tests_failed=0, tests_errored=0, tests_skipped=0),
                    'original Python validator failure erased')
            test_path = 'python/tests/test_release_scripts.py'
            require(correction['compared_commit'] == 'e07bf3418671c335dd6b90a1dd81d3553fecea15'
                    and correction['compared_path'] == test_path, 'comparison test source changed')
            inventories = []
            for revision, expected_count in ((source['source_commit'], 167), (correction['compared_commit'], 168)):
                raw = subprocess.check_output(['git', 'show', revision + ':' + test_path], cwd=repository)
                classes = [node for node in ast.parse(raw).body if isinstance(node, ast.ClassDef)]
                require(len(classes) == 1 and classes[0].name == 'ReleaseScriptTests', 'test discovery shape changed')
                names = sorted(node.name for node in classes[0].body if isinstance(node, ast.FunctionDef)
                               and node.name.startswith('test_'))
                require(len(names) == len(set(names)) == expected_count, 'release-script test inventory changed')
                inventories.append(names)
                if revision == source['source_commit']:
                    require(digest(raw) == source['source_hashes'][test_path]
                            == correction['release_test_source']['sha256']
                            and len(raw) == correction['release_test_source']['bytes'], 'frozen release-script source changed')
                else:
                    require(digest(raw) == correction['compared_source_sha256']
                            and len(raw) == correction['compared_source_bytes'], 'PR release-script source changed')
            require(inventories == [correction['release_test_names'], correction['compared_release_test_names']]
                    and set(inventories[1]) - set(inventories[0]) == {
                        'test_pre_5j_dependency_freshness_rejects_unreviewed_patch_versions'}
                    and set(inventories[0]) <= set(inventories[1]), 'test-count difference not explained')
            for name, minimum in (('core', 700), ('release_scripts', 167)):
                shard = record['shards'][name]
                log = gate_log(shard['run'])
                report = evidence.read(evidence.check(shard['report']))
                require(report['status'] == 'passed' and report['test_count'] == minimum
                        and report['failure_count'] == report['error_count'] == 0
                        and report['skipped_count'] == (23 if name == 'core' else 0), 'Python shard incomplete')
                require(re.search(r'Ran ' + ('559' if name == 'core' else '167') + r' tests in ', log)
                        and ('\nOK (skipped=23)\n' if name == 'core' else '\nOK\n') in log,
                        'Python execution count changed')
            log = gate_log(record['focused'])
            match = re.search(r'Ran (\d+) tests in .*\n\nOK\s*$', log)
            require(match and int(match[1]) == 18 and 'skipped=' not in log, 'focused Python checks incomplete')
        else:
            require(set(record['checks']) == {'python_license_bundle', 'user_surface_reference', 'golden_workflows',
                                             'public_status_docs', 'contribution_governance'}, 'reference check omitted')
            for run in record['checks'].values():
                gate_log(run)


def verify_wheel_test_correction(source, final, owner, build, evidence):
    correction = evidence.read(evidence.check(final['installed_wheel_test_correction']))
    binding = evidence.read(evidence.check(final['installed_wheel_binding']))
    evidence.references(correction)
    evidence.references(binding)
    require(correction['status'] == 'predeclared' and correction['source_commit'] == source['source_commit']
            and correction['source'] == final['source'] and correction['build'] == final['build']
            and correction['original_protocol'] == final['protocol']
            and correction['runtime_source_unchanged'] and correction['package_sources_unchanged'],
            'wheel correction source changed')
    require(owner['installed_wheel_test_correction'] == final['installed_wheel_test_correction']
            and owner['installed_wheel_binding'] == final['installed_wheel_binding']
            and binding['status'] == 'passed' and binding['sources_unchanged'] and binding['artifacts_unchanged']
            and binding['plan'] == final['installed_wheel_test_correction']
            and binding['build'] == final['build'] and binding['runtime_source_commit'] == source['source_commit']
            and binding['wheel_report'] == owner['installed_python'], 'wheel correction binding changed')
    original_root = Path(build['cli_compiler_artifact']['manifest_path']).parents[1]
    test_rows = correction['test_files']
    names = {name.removeprefix('python/tests/') for name in source['source_hashes'] if name.startswith('python/tests/')}
    require(len(test_rows) == len(names) == 70 and {row['name'] for row in test_rows} == names,
            'wheel test source coverage changed')
    fixture = 'test_native_output_window_binding.py'
    selected_root = Path(binding['corrected_test_fixture']['path']).parent
    original_tests, selected_tests = [], []
    for row in test_rows:
        name = row['name']
        before, after = (evidence.raw(row[key]['path']) for key in ('original', 'selected'))
        require(row['original']['path'] == str(original_root / 'python/tests' / name)
                and row['selected']['path'] == str(selected_root / name)
                and digest(before) == source['source_hashes']['python/tests/' + name], 'wheel original test source changed')
        require(row['changed'] == (name == fixture) and row['symlink'] == (name != fixture),
                'wheel test correction scope changed')
        if name == fixture:
            require(row['selected'] == binding['corrected_test_fixture']
                    and digest(after) == '3dfdc03fb74df626b1870426205f4894558045fbc4c12b72d24b4680744c436c',
                    'wheel corrected fixture changed')
            diff = ''.join(difflib.unified_diff(before.decode().splitlines(True), after.decode().splitlines(True),
                fromfile='a/python/tests/' + name, tofile='b/python/tests/' + name))
            require(diff == correction['test_diff'], 'wheel correction diff changed')
        else:
            require(before == after, 'wheel unrelated test changed')
        if name.startswith('test_native') and name.endswith('binding.py'):
            for content, tests in ((before, original_tests), (after, selected_tests)):
                tests.extend(f'{Path(name).stem}.{cls.name}.{method.name}'
                             for cls in ast.parse(content).body if isinstance(cls, ast.ClassDef)
                             for method in cls.body if isinstance(method, ast.FunctionDef)
                             and method.name.startswith('test_'))
    require(len(original_tests) == len(set(original_tests)) == 118
            and sorted(original_tests) == sorted(selected_tests) == correction['expected_test_ids']
            == binding['complete_test_ids'], 'wheel discovery coverage changed')
    helper_names = {'prove_native_python_wheel.py', 'stage_native_python_package.py',
                    'check_native_python_wheel.py', 'audit_native_python_wheel.py'}
    require(len(correction['proof_helpers']) == len(helper_names)
            and {Path(ref['path']).name for ref in correction['proof_helpers']} == helper_names,
            'wheel proof helper omitted')
    for ref in correction['proof_helpers']:
        name = Path(ref['path']).name
        require(ref['path'] == str(original_root / 'scripts' / name)
                and digest(evidence.raw(ref['path'])) == source['source_hashes']['scripts/' + name],
                'wheel proof helper changed')
    helper = evidence.raw(original_root / 'scripts/prove_native_python_wheel.py')
    constants = [node for node in ast.parse(helper).body if isinstance(node, ast.Assign)
                 and any(isinstance(target, ast.Name) and target.id == 'RUN_TESTS' for target in node.targets)]
    require(len(constants) == 1, 'wheel discovery helper missing')
    original_runner = ast.literal_eval(constants[0].value)
    token = 'discover(sys.argv[1], pattern="test_native*binding.py")'
    require(original_runner.count(token) == 1 and correction['original_runner'] == original_runner
            and correction['selected_runner'] == original_runner.replace(token,
                f'discover({str(selected_root)!r}, pattern="test_native*binding.py")'),
            'wheel discovery runner narrowed')
    wheel_root = Path(owner['installed_python']['path']).parent
    report = evidence.read(owner['installed_python']['path'])
    require(report['integration'] == dict(tests=118, failures=0, errors=0, skipped=0, status='passed'),
            'wheel corrected integration incomplete')
    require([step['name'] for step in report['steps']] == [
        'build', 'create_environment', 'install_optional_consumer', 'install', 'load',
        'integration', 'uninstall', 'verify_uninstalled'], 'wheel proof step omitted')
    integration = next(step for step in report['steps'] if step['name'] == 'integration')
    require(integration['command'][1:4] == ['-I', '-c', correction['selected_runner']],
            'wheel corrected tests not executed')
    lines = evidence.raw(wheel_root / 'integration.log').decode().splitlines()
    executed = sorted(match.group(1) for line in lines
                      if (match := re.fullmatch(r'test_\w+ \(([^)]+)\) \.\.\. ok', line)))
    require(executed == correction['expected_test_ids'], 'wheel executed test coverage changed')
    guard = evidence.read(binding['receipt']['receipt']['path'])
    require(guard['status'] == 'passed' and guard['returncode'] == 0
            and guard['process_group_drained'] and guard['direct_child_reaped']
            and guard['head'] == source['source_commit'] and guard['working_tree'].strip() == ''
            and guard['command'] == correction['command']
            and digest(evidence.raw(binding['receipt']['log']['path'])) == guard['log_sha256'],
            'wheel correction guard changed')
    original_failure = correction['original_failure']
    require(original_failure == binding['original_failure_preserved'], 'original wheel failure omitted')
    failed_reports = [ref for ref in original_failure if Path(ref['path']).name == 'report.json']
    require(len(failed_reports) == 1, 'original wheel report missing')
    failed_root = Path(failed_reports[0]['path']).parent
    failed = evidence.read(failed_reports[0]['path'])
    require(failed['status'] == 'failed' and failed['integrity']['native_library_sha256'] == build['native_library']['sha256']
            and failed['integrity']['cli_sha256'] == build['binary_sha256']
            and evidence.read(failed_root / 'integration.json') == dict(tests=118, failures=2, errors=0, skipped=0, status='failed'),
            'original wheel failure changed')
    failure_lines = [line for line in evidence.raw(failed_root / 'integration.log').decode().splitlines() if line.startswith('FAIL: ')]
    require(len(failure_lines) == 2
            and all('test_public_sql_and_dataframe_slicing_preserves_values_and_policy' in line
                    and "(surface='dataframe_projection', batch_rows=" in line for line in failure_lines)
            and 'batch_rows=257)' in failure_lines[0] and 'batch_rows=4096)' in failure_lines[1],
            'original wheel failure cause changed')
    for step in failed['steps']:
        require(step['returncode'] == (1 if step['name'] == 'integration' else 0)
                and digest(evidence.raw(failed_root / step['log'])) == step['log_sha256'],
                'original wheel failure log changed')
    failed_guard_refs = [ref for ref in original_failure
                         if Path(ref['path']).name == 'prepared-sampling-final-864-wheel-run.json']
    require(len(failed_guard_refs) == 1, 'original failed wheel guard missing')
    failed_guard = evidence.read(failed_guard_refs[0]['path'])
    original_protocol = evidence.read(correction['original_protocol']['path'])
    require(failed_guard['status'] == 'failed' and failed_guard['returncode'] == 1
            and failed_guard['process_group_drained'] and failed_guard['direct_child_reaped']
            and failed_guard['head'] == source['source_commit'] and failed_guard['working_tree'].strip() == ''
            and failed_guard['command'] == original_protocol['commands']['wheel']
            and digest(evidence.raw(failed_guard_refs[0]['path'].removesuffix('.json') + '.log')) == failed_guard['log_sha256'],
            'original failed wheel guard changed')
    return dict(tests=118, corrected_test_files=1, original_failed_subcases=2)


def verify_modular_environment(source, final, accepted, build, evidence):
    plan = evidence.read(evidence.check(final['modular_environment_correction']))
    binding = evidence.read(evidence.check(final['modular_environment_binding']))
    old = evidence.read(evidence.check(final['protocol']))
    dependencies = evidence.read(evidence.check(plan['dependencies']))
    for value in (plan, binding, dependencies):
        evidence.references(value)
    require(plan['status'] == 'predeclared' and plan['source'] == final['source']
            and plan['build'] == final['build'] and plan['original_protocol'] == final['protocol']
            and not plan['native_source_changed'] and not plan['resource_limits_changed']
            and not plan['performance_claim'] and not plan['original_matrix_accepted'],
            'modular correction changed scope')
    require(plan['runner'] == old['modular_runner'] and plan['expected'] == old['expected']
            and plan['original_command'] == old['commands']['modular'], 'modular original protocol changed')
    require(binding['status'] == 'passed' and binding['sources_unchanged'] and binding['artifacts_unchanged']
            and binding['private_environment'] and binding['dependency_files_unchanged']
            and not binding['original_matrix_accepted'] and binding['plan'] == final['modular_environment_correction']
            and binding['build'] == final['build']
            and binding['dependencies_before'] == binding['dependencies_after'] == plan['dependencies'],
            'modular dependency integrity failed')
    require(accepted['plan'] == final['modular_environment_correction']
            and accepted['dependency_binding'] == final['modular_environment_binding']
            and accepted['dependencies'] == plan['dependencies'] and accepted['run'] == binding['receipt'],
            'modular acceptance detached from correction')
    packages = {'pandas': '2.2.3', 'numpy': '2.3.5', 'python-dateutil': '2.9.0.post0',
                'pytz': '2026.5', 'tzdata': '2026.5', 'six': '1.17.0', 'fastavro': '1.12.2', 'pyarrow': '25.0.1'}
    require(dependencies['status'] == 'passed' and dependencies['private_environment']
            and dependencies['helper'] == plan['helper'], 'modular private environment missing')
    origins = dependencies['origins']
    require(len(origins) == len(packages) and {r['name']: r['version'] for r in origins} == packages,
            'modular dependency packages changed')
    files = dependencies['files']
    require(files and len({r['relative_path'] for r in files}) == len(files)
            and all(not Path(r['relative_path']).is_absolute() and '..' not in Path(r['relative_path']).parts
                    and r['bytes'] >= 0 and re.fullmatch('[0-9a-f]{64}', r['sha256']) for r in files),
            'modular dependency file inventory invalid')
    require(sorted((r for origin in origins for r in origin['files']), key=lambda r: r['relative_path'])
            == sorted(files, key=lambda r: r['relative_path']), 'modular private copy differs from source snapshot')
    top = {Path(r['relative_path']).parts[0] for r in files}
    require(len(dependencies['top_level_names']) == len(top) == 16 and top == set(dependencies['top_level_names']),
            'modular dependency top-level coverage changed')
    imported = dependencies['imports']
    private_root = Path(imported['prefix'])
    site = private_root / 'lib/python3.12/site-packages'
    python = str(private_root / 'bin/python3')
    require(dependencies['python']['path'] == python and imported['user_site'] is False
            and imported['distributions'] == packages
            and set(imported['modules']) == {'pandas', 'numpy', 'dateutil', 'pytz', 'tzdata', 'six', 'fastavro', 'pyarrow'}
            and all(Path(p).is_relative_to(site) for p in imported['modules'].values())
            and all('site-packages' not in p or p == str(site) for p in imported['path']),
            'modular reference import escaped private environment')
    require('include-system-site-packages = false' in evidence.raw(dependencies['venv_config']['path']).decode(),
            'modular environment permits shared site packages')
    command = list(plan['original_command'])
    require(command[1].startswith('PYTHONPATH=') and command[-1] == '864', 'modular original command malformed')
    command[1:3] = ['PYTHONPATH=' + str(site), 'PYTHONNOUSERSITE=1', python]
    command[-1] = '869'
    require(plan['command'] == command, 'modular correction changed workload command')
    run = evidence.read(binding['receipt']['receipt']['path'])
    require(run['status'] == 'passed' and run['returncode'] == 0 and run['command'] == command
            and run['process_group_drained'] and run['direct_child_reaped']
            and run['head'] == source['source_commit'] and run['working_tree'].strip() == ''
            and digest(evidence.raw(binding['receipt']['log']['path'])) == run['log_sha256'],
            'modular corrected run receipt changed')
    preserved = {Path(ref['path']).name: evidence.check(ref) for ref in plan['preserved_original_evidence']}
    require(set(preserved) == {'prepared-sampling-modular-environment-postcheck-868.json',
            'prepared-sampling-matrix-864-report.json', 'prepared-sampling-final-864-modular-run.json',
            'prepared-sampling-final-864-modular-run.log'}, 'modular failed environment evidence omitted')
    diagnostic = evidence.read(preserved['prepared-sampling-modular-environment-postcheck-868.json'])
    require(diagnostic['status'] == 'post_run_environment_changed' and diagnostic['source'] == final['source']
            and diagnostic['plan'] == final['protocol'] and diagnostic['before'] == old['dependency_environment']
            and diagnostic['before'] != diagnostic['after'] and diagnostic['other_changes'] == {},
            'modular original environment failure erased')
    before, after = diagnostic['before'], diagnostic['after']
    require({k: v for k, v in before.items() if k != 'packages'} == {k: v for k, v in after.items() if k != 'packages'},
            'modular original nonpackage environment changed')
    original_packages = {p['name']: p for p in before['packages']}
    after_packages = {p['name']: p for p in after['packages']}
    require(set(original_packages) == set(after_packages) == {'pandas', 'numpy', 'fastavro'},
            'modular original package set changed')
    differences = {}
    for name, old_package in original_packages.items():
        new_package = after_packages[name]
        require({k: v for k, v in old_package.items() if k != 'files'} ==
                {k: v for k, v in new_package.items() if k != 'files'}, 'modular original package metadata changed')
        old_files = {r['relative_path']: r for r in old_package['files']}
        new_files = {r['relative_path']: r for r in new_package['files']}
        require(old_files.keys() == new_files.keys(), 'modular original package file set changed')
        changed = [n for n in old_files if old_files[n] != new_files[n]]
        require(all(n.endswith('.so') and old_files[n]['bytes'] == new_files[n]['bytes'] for n in changed),
                'modular original change classification differs')
        differences[name] = len(changed)
        origin = next(row for row in origins if row['name'] == name)
        selected = [dict(r, relative_path=r['relative_path'][len(name)+1:]) for r in origin['files']
                    if r['relative_path'].startswith(name + '/')]
        require(origin['source_root'] == str(Path(new_package['directory']).parent)
                and selected == new_package['files'], 'modular private package snapshot detached from diagnosis')
    require(differences == {'pandas': 44, 'numpy': 19, 'fastavro': 0}, 'modular original changed-file count differs')
    old_run = evidence.read(preserved['prepared-sampling-final-864-modular-run.json'])
    old_matrix = evidence.read(preserved['prepared-sampling-matrix-864-report.json'])
    require(old_run['command'] == plan['original_command'] and old_run['status'] == 'passed' and old_run['returncode'] == 0
            and old_run['process_group_drained'] and old_run['direct_child_reaped']
            and old_run['head'] == source['source_commit'] and old_run['working_tree'].strip() == ''
            and digest(evidence.raw(preserved['prepared-sampling-final-864-modular-run.log'])) == old_run['log_sha256']
            and old_matrix['status'] == 'passed' and old_matrix['records'] == 1408
            and old_matrix['native_records'] == 704 and len(old_matrix['cohorts']) == 18
            and old_matrix['binary_sha256'] == build['binary_sha256'], 'modular original attempt changed')
    return dict(status='passed', packages=len(packages), files=len(files), original_changed_native_files=63,
                full_matrix_repeated=True, original_matrix_accepted=False, private_dependency_files_unchanged=True)


def verify_comparison_replay(comparison, observed, raw):
    members = json.loads(lzma.decompress(raw))['members']
    identities = {name: {key: value for key, value in row.items() if key != 'text'}
                  for name, row in members.items()}
    require(comparison['members'] == identities and observed['members'] == len(identities),
            'comparison member inventory changed')
    require(all(comparison[key] == value for key, value in observed.items() if key != 'members'),
            'comparison replay changed')


def replay(repository, manifest, packet, scratch):
    require(manifest['schema'] == 'shardloom.prepared_sampling_integration_evidence.v1', 'manifest schema changed')
    require(digest(Path(__file__).read_bytes()) == manifest['verifier']['sha256'], 'replay implementation changed')
    evidence = Evidence(packet)
    final = evidence.read(evidence.check(manifest['final_acceptance']))
    require(final['schema'] == 'shardloom.prepared_sampling_final_acceptance.v1' and final['status'] == 'passed'
            and final['finite_runtime_accepted'] and final['outcome'] == 'finite_prepared_weighted_sampling_retained',
            'finite acceptance missing')
    require(final['public_cases'] == 5930 and final['installed_binding_tests'] == 118
            and not final['publication_attempted'] and not final['competitive_claim']
            and not final['broader_owner_complete'] and not final['completed_roadmap_substeps']
            and not final['fallback_attempted'] and not final['external_engine_invoked']
            and not final['whole_process_memory_ceiling_claimed'], 'scope changed')
    evidence.references(final)
    source = evidence.read(final['source']['path'])
    build = evidence.read(final['build']['path'])
    plan = evidence.read(final['protocol']['path'])
    bridge = evidence.read(final['correction_bridge']['path'])
    require(final['source_commit'] == final['compiled_source_commit'] == build['source_commit'] == source['source_commit']
            == bridge['corrected_commit'] == '4cd3a9e125cad4bba082b6e80c7c600f71239891', 'accepted source changed')
    require(manifest['source_commit'] == source['source_commit'] and manifest['scored_commit'] == final['scored_commit'],
            'manifest source changed')
    require(plan['source'] == final['source'] and plan['build'] == final['build'], 'protocol binding changed')
    require(build['source_hashes'] == source['source_hashes'] and source['working_tree'] == build['working_tree'] == '',
            'source closure changed')
    verify_git_sources(repository, source['source_commit'], source['source_hashes'])
    require(source['correction_bridge'] == final['correction_bridge'], 'source correction missing')
    require(bridge['scored_commit'] == final['scored_commit'] == '95fbca52a752fabd9135921a47c7baae2f90cbb7'
            and bridge['status'] == 'source_bridge_passed_acceptance_pending'
            and bridge['new_cfg_true_for_scored_features'] and bridge['feature_method_body_unchanged']
            and bridge['python_test_bodies_unchanged'] and not bridge['performance_comparison_repeated'], 'correction widened')
    corrected = evidence.read(evidence.check(bridge['corrected_source']))
    scored = evidence.read(evidence.check(bridge['scored_source']))
    middle = evidence.read(evidence.check(bridge['test_corrected_source']))
    require(bridge['corrected_source'] == source['source_manifest'], 'corrected source detached')
    require({name: row['sha256'] for name, row in corrected['binding']['files'].items()} == source['source_hashes'],
            'frozen source differs from accepted build')
    old, intermediate, new = (value['binding']['files'] for value in (scored, middle, corrected))
    require(set(old) == set(intermediate) == set(new), 'source input set changed')
    test_module = 'shardloom-vortex/src/local_primitive_relational_cyclic_exists_tests.rs'
    feature_module = 'shardloom-vortex/src/local_primitive_relational_batch_input.rs'
    require({name for name in old if old[name] != intermediate[name]} == {test_module}, 'test correction changed runtime')
    require({name for name in old if old[name] != new[name]} == set(bridge['changed_source_inputs']),
            'feature source correction changed')
    require(bridge['changed_compiler_inputs'] == [test_module, feature_module]
            or bridge['changed_compiler_inputs'] == [feature_module, test_module], 'unexpected compiler source correction')
    for record in (scored, middle):
        verify_git_sources(repository, record['binding']['base_commit'],
                           {name: row['sha256'] for name, row in record['binding']['files'].items()})
    patch = subprocess.check_output(['git', 'diff', '--binary', bridge['scored_commit'],
                                     bridge['corrected_commit']], cwd=repository, text=True).strip()
    require(patch == bridge['source_diff'], 'feature correction patch changed')
    scored_binary = evidence.read(evidence.check(bridge['scored_binary_manifest']))
    compiled = scored_binary['compilation']['compilation_inputs']
    require(bridge['scored_binary'] == scored_binary['executable']
            and bridge['scored_features'] == scored_binary['compilation']['compiler_artifact']['features']
            and {'vortex-write', 'universal-format-io'} <= set(bridge['scored_features']), 'scored feature identity changed')
    require(all(new[name] == value for name, value in compiled.items() if name not in {test_module, feature_module})
            and bridge['unchanged_compiler_inputs'] == len(compiled) - 2
            and bridge['unchanged_source_inputs'] == len(old) - 4, 'unbridged compiler source change')
    def git_text(commit, name):
        return subprocess.check_output(['git', 'show', commit + ':' + name], cwd=repository, text=True)
    old_method = git_text(bridge['scored_commit'], feature_module)
    new_method = git_text(bridge['corrected_commit'], feature_module)
    marker = '    pub fn for_each_arrow_batch_with_output_window(\n'
    require(old_method.count(marker) == 1 and new_method == old_method.replace(marker,
            '    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]\n' + marker),
            'feature correction changed method body')
    import_line = 'from test_native_binding import _load_native\n'
    corrected_import = ('if __package__:\n    from .test_native_binding import _load_native\n'
                        'else:\n    from test_native_binding import _load_native\n')
    for name in ('python/tests/test_native_output_window_binding.py', 'python/tests/test_native_output_window_observation.py'):
        old_text = git_text(bridge['scored_commit'], name)
        require(old_text.count(import_line) == 1 and git_text(bridge['corrected_commit'], name)
                == old_text.replace(import_line, corrected_import), 'Python correction changed test body')
    previous = evidence.read(evidence.check(bridge['previous_test_only_bridge']))
    require(previous['failed_library_test_counts'] == dict(passed=3089, failed=1, ignored=31)
            and previous['status'] == 'source_bridge_passed_tests_pending'
            and not previous['performance_comparison_repeated'], 'failed historical suite erased')
    evidence.check(previous['failure_receipt'])
    failed = evidence.read(previous['failure_receipt']['path'])
    raw_failure = evidence.raw(evidence.check(previous['failure_log']))
    require(failed['status'] == 'failed' and failed['returncode'] != 0
            and digest(raw_failure) == failed['log_sha256']
            and re.findall(r'test result: FAILED\. (\d+) passed; (\d+) failed; (\d+) ignored;', raw_failure.decode())
                == [('3089', '1', '31')], 'historical failure changed')

    comparisons = {}
    for label in ('original', 'public_integration'):
        ref = final['comparisons'][label]
        comparison = evidence.read(evidence.check(ref))
        packet_ref = comparison['packet']
        evidence.check(packet_ref)
        raw = evidence.raw(packet_ref['path'])
        require(len(raw) == packet_ref['bytes'] and digest(raw) == packet_ref['sha256'], 'comparison packet changed')
        helper_ref = comparison['packager']
        helper = evidence.raw(evidence.check(helper_ref))
        with tempfile.TemporaryDirectory(prefix='prepared-acceptance-comparison-', dir=scratch) as directory:
            directory = Path(directory)
            helper_path = directory / Path(helper_ref['path']).name
            packet_path = directory / Path(packet_ref['path']).name
            helper_path.write_bytes(helper)
            packet_path.write_bytes(raw)
            spec = importlib.util.spec_from_file_location('sampling_' + label + '_archive_replay', helper_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            observed = module.verify(packet_path, directory, repository)
        verify_comparison_replay(comparison, observed, raw)
        require(observed['retention_gate_passed'] and observed['workflows'] == 816 and observed['calls'] == 5712,
                'comparison incomplete')
        if label == 'public_integration':
            require(observed['native_test_counts']['failed'] == 1
                    and observed['integration_acceptance_status'] == 'pending_corrected_native_and_remaining_gates',
                    'historical comparison relabelled')
        comparisons[label] = observed

    candidate = evidence.read(evidence.check(source['candidate_binary']))
    require(candidate['source'] == source['source_manifest'] and candidate['executable'] == source['native_test_executable'],
            'accepted test binary detached')
    targets = {'shardloom_core', 'shardloom_exec', 'shardloom_plan', 'shardloom_vortex',
               'vortex_array', 'vortex_runend', 'vortex_fsst', 'vortex_zstd', 'vortex_datetime_parts', 'vortex_layout', 'vortex_file'}
    original_root = Path('/Users/dylan/.codex/worktrees/public-docs-api/shardloom-local-repo')
    verify_compilation(candidate['compilation'], source['source_hashes'], original_root, targets)
    artifact = candidate['compilation']['compiler_artifact']
    require(artifact['fresh'] is False and artifact['profile']['test'] is True
            and artifact['profile']['opt_level'] == '3' and 'release-user-surfaces' in artifact['features'], 'test build not qualified')
    require(candidate['executable']['sha256'] == candidate['compiled']['sha256']
            and candidate['executable']['bytes'] == candidate['compiled']['bytes']
            and artifact['executable'] == candidate['compiled']['path'], 'copied test executable changed')
    evidence.references(candidate['checks'])
    events = [json.loads(line) for line in evidence.raw(candidate['checks']['build']['log']['path']).decode().splitlines()
              if line.startswith('{')]
    require(artifact in events and {'reason': 'build-finished', 'success': True} in events, 'test compiler event missing')
    verify_compilation(build['compiler_input_proof'], source['source_hashes'], original_root, targets | {'shardloom', 'shardloom_runtime'})
    correction = evidence.read(evidence.check(build['build_correction']))
    require(correction['status'] == 'validator_target_name_corrected'
            and correction['source'] == final['source'] and correction['source_commit'] == source['source_commit']
            and correction['original_helper'] == source['helper']
            and correction['completion_helper'] == build['completion_helper']
            and correction['original_build'] == build['cli_build']
            and not correction['build_repeated'] and not correction['source_changed'],
            'compiler target correction provenance changed')
    require(correction['failed_postcondition'] == dict(function='compiler_inputs', required_target='shardloom_cli',
            exception='AssertionError', compiler_status='passed', compiler_returncode=0),
            'original compiler target validator failure erased')
    cargo_name = 'shardloom-cli/Cargo.toml'
    raw_cargo = subprocess.check_output(['git', 'show', source['source_commit'] + ':' + cargo_name], cwd=repository)
    require(digest(raw_cargo) == source['source_hashes'][cargo_name] == correction['cargo_manifest']['sha256'],
            'Cargo target source changed')
    cargo = tomllib.loads(raw_cargo.decode())
    require(cargo['package']['name'] == correction['cargo_package'] == 'shardloom-cli'
            and cargo['lib']['name'] == correction['cargo_library_target'] == 'shardloom_runtime'
            and cargo['bin'] == [dict(name='shardloom', path='src/main.rs')], 'declared compiler target changed')
    functions = []
    for ref in (correction['original_helper'], correction['completion_helper']):
        text = evidence.raw(evidence.check(ref)).decode()
        nodes = [node for node in ast.parse(text).body if isinstance(node, ast.FunctionDef) and node.name == 'compiler_inputs']
        require(len(nodes) == 1, 'compiler input checker missing')
        functions.append(''.join(text.splitlines(keepends=True)[nodes[0].lineno - 1:nodes[0].end_lineno]))
    require(functions == [correction['original_compiler_input_function'], correction['corrected_compiler_input_function']]
            and functions[0].count("'shardloom_cli'") == 1
            and functions[1] == functions[0].replace("'shardloom_cli'", "'shardloom_runtime'")
            and set(correction['actual_local_targets']) == targets | {'shardloom', 'shardloom_runtime'},
            'compiler input correction changed other checks')
    artifact = build['cli_compiler_artifact']
    require(artifact['fresh'] is False and artifact['profile']['test'] is False
            and artifact['profile']['opt_level'] == '3' and 'release-user-surfaces' in artifact['features'], 'CLI build not qualified')
    evidence.references(build['cli_build'])
    events = [json.loads(line) for line in evidence.raw(build['cli_build']['log']['path']).decode().splitlines()
              if line.startswith('{')]
    require(artifact in events and {'reason': 'build-finished', 'success': True} in events, 'CLI compiler event missing')
    for phase, expected, targets_count in (
        ('native-focused', (4, 0, 0), 1), ('native-all', (3107, 0, 31), 13),
        ('cli-focused', (2, 0, 0), 1), ('cli-all', (1091, 0, 0), 6)):
        check = final['verification'][phase]
        log = evidence.raw(evidence.check(check['log'])).decode()
        counts = [tuple(map(int, row)) for row in re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;', log)]
        require(len(counts) == targets_count and tuple(sum(row[i] for row in counts) for i in range(3)) == expected,
                'accepted native or CLI test coverage changed')
        receipt = evidence.read(evidence.check(check['receipt']))
        require(receipt['head'] == source['source_commit'] and receipt['status'] == 'passed'
                and receipt['returncode'] == 0 and receipt['working_tree'].strip() == ''
                and digest(log.encode()) == receipt['log_sha256'], 'test receipt source changed')
    verify_required_gates(source, final, evidence, repository)
    owner10 = evidence.read(final['perf10']['path'])
    owner12 = evidence.read(final['perf12']['path'])
    evidence.references(owner10)
    evidence.references(owner12)
    require(owner10['status'] == owner12['status'] == 'passed' and owner10['comparisons'] == final['comparisons'],
            'owner acceptance missing')
    require(owner12['historical_comparisons_preserved'] and not owner12['scored_binary_relabelled']
            and not owner12['retained_input_reingested'], 'historical or input scope changed')
    accepted = {phase: evidence.read(ref['path']) for phase, ref in owner10['acceptance'].items()}
    require(set(accepted) == {'public', 'full43', 'modular'}, 'acceptance family omitted')
    for phase, value in accepted.items():
        evidence.references(value)
        require(value['status'] == 'passed' and value['phase'] == phase and value['source_commit'] == source['source_commit']
                and value['package_source_commit'] == source['source_commit'] and not value['performance_claim'],
                'invalid acceptance report')
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
    modular_environment = verify_modular_environment(source, final, accepted['modular'], build, evidence)
    require(matrix['status'] == 'passed' and len(matrix['cohorts']) == 18 and matrix['records'] == 1408
            and matrix['native_records'] == 704 and not matrix['complete_cartesian_input_output_coverage_claimed'], 'modular matrix incomplete')
    require(matrix['binary_sha256'] == build['binary_sha256'] and matrix['binary'] == build['binary']
            and matrix['runtime_commit'] == build['source_commit'] and matrix['harness_commit'] == source['source_commit'],
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
    wheel_correction = verify_wheel_test_correction(source, final, owner10, build, evidence)
    require(wheel['status'] == wheel['uninstall_status'] == wheel['integration']['status'] == 'passed'
            and wheel['integration']['tests'] == final['installed_binding_tests']
            and wheel['integration']['failures'] == wheel['integration']['errors'] == wheel['integration']['skipped'] == 0
            and wheel['installed_load']['isolated_install'] and not wheel['publication_attempted']
            and not wheel['performance_claim'], 'installed proof incomplete')
    require(wheel['integrity']['native_library_sha256'] == build['native_library']['sha256']
            and wheel['integrity']['cli_sha256'] == build['binary_sha256'], 'installed binaries changed')
    wheel_root = Path(owner10['installed_python']['path']).parent
    for row in wheel['steps']:
        require(row['returncode'] == 0 and digest(evidence.raw(wheel_root / row['log'])) == row['log_sha256'], 'package step failed')
    sampling = owner10['installed_sampling_tests']
    require(len(sampling) == len(set(sampling)) == 3, 'installed sampling coverage omitted')
    lines = evidence.raw(wheel_root / 'integration.log').decode().splitlines()
    for name in sampling:
        matching = [line for line in lines if line.startswith(name + ' (')]
        require(len(matching) == 1 and 'NativePreparedSamplingBindingTests.' in matching[0]
                and matching[0].endswith(' ... ok'), 'installed sampling test did not execute')
    guards, failed_guards = 0, 0
    for name in evidence.members:
        if not name.endswith('.json') or name.endswith('.stdout.json') or Path(name).parent != Path(manifest['final_acceptance']['path']).parent:
            continue
        value = evidence.read(name)
        if isinstance(value, dict) and 'runner_sha256' in value and 'process_group_drained' in value:
            require(value['status'] in ('passed', 'failed') and value['process_group_drained'] and value['direct_child_reaped'],
                    'undrained guard')
            require((value['returncode'] == 0) == (value['status'] == 'passed'), 'inconsistent guarded status')
            require(digest(evidence.raw(name.removesuffix('.json') + '.log')) == value['log_sha256'], 'guard log changed')
            guards += 1
            failed_guards += int(value['status'] == 'failed')
    require(failed_guards >= 1, 'historical failed native suite omitted')
    return dict(status='passed', members=len(evidence.members), public_cases=5930, public_families=39,
        full43_queries=43, full43_complete_rows=count, full43_engine_execution_seconds=final['full43_engine_execution_seconds'],
        modular_records=total, modular_payload_calls_replayed=call_count, installed_tests=final['installed_binding_tests'],
        modular_environment=modular_environment,
        installed_sampling_tests=3, wheel_test_correction=wheel_correction,
        comparisons=comparisons, guarded_receipts=guards,
        preserved_failed_guards=failed_guards, source_files=len(source['source_hashes']),
        native_execution_performed=False, payload_residency_revalidated=False, publication_attempted=False,
        scope='Complete archived values, reference comparisons, source and receipt integrity; original residency checks remain recorded.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--scratch', type=Path, required=True)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_bytes())
    path = args.manifest.parent / manifest['packet']['file']
    data = path.read_bytes()
    require(len(data) == manifest['packet']['bytes'] and digest(data) == manifest['packet']['sha256'], 'packet identity changed')
    result = replay(args.repository.resolve(), manifest, json.loads(lzma.decompress(data)), args.scratch)
    require(result == manifest['replay'], 'replay summary changed')
    if args.output:
        with args.output.open('x') as stream:
            json.dump(result, stream, indent=2, sort_keys=True)
            stream.write('\n')
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
