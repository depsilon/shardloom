"""Replay current-PR correctness and installed-wheel evidence without executing it."""
import argparse
import ast
import hashlib
import io
import json
import lzma
from pathlib import Path
import re
import subprocess
import tomllib

PHASES = ('format', 'workspace-lint', 'workspace-tests', 'native-lint', 'native-tests',
          'native-cli', 'minimal-feature', 'optional-tests', 'python', 'build', 'wheel', 'references')
PREFIXES = ('shardloom-', 'vendor/', 'python/', 'scripts/', 'benchmarks/', '.github/', '.cargo/')
NAMES = {'Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml',
         'website-src/package.json', 'website-src/package-lock.json'}
CHANGES = {'Cargo.lock', 'python/THIRD_PARTY_LICENSES/manifest.json',
           'python/tests/test_native_output_window_binding.py', 'python/tests/test_release_scripts.py',
           'scripts/check_pre_5j_dependency_freshness.py',
           'shardloom-vortex/tests/fixtures/native-exchange/README.md',
           'shardloom-vortex/tests/fixtures/native-exchange/producer/src/main.rs'}


def require(value, message):
    if not value:
        raise ValueError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


class Evidence:
    def __init__(self, members):
        self.members = members
        for name, member in members.items():
            raw = member['text'].encode()
            require(set(member) == {'text', 'bytes', 'sha256'} and
                    member['bytes'] == len(raw) and member['sha256'] == digest(raw),
                    'member identity changed: ' + name)

    def raw(self, name):
        return self.members[str(name)]['text'].encode()

    def read(self, name):
        return json.loads(self.raw(name))

    def reference(self, ref):
        raw = self.raw(ref['path'])
        require(ref['bytes'] == len(raw) and ref['sha256'] == digest(raw),
                'linked member identity changed: ' + ref['path'])
        return raw

    def linked(self, ref):
        return json.loads(self.reference(ref))


def git(repository, *args):
    return subprocess.check_output(['git', *args], cwd=repository)


def source_blobs(repository, revision, names):
    # A single bounded batch preserves exact bytes, including embedded newlines.
    requests = ''.join(revision + ':' + name + '\n' for name in names).encode()
    output = io.BytesIO(subprocess.check_output(['git', 'cat-file', '--batch'],
                                               cwd=repository, input=requests))
    result = {}
    for name in names:
        header = output.readline().split()
        require(len(header) == 3 and header[1] == b'blob', 'source blob missing: ' + name)
        size = int(header[2])
        result[name] = output.read(size)
        require(len(result[name]) == size and output.read(1) == b'\n', 'source blob truncated')
    require(output.read() == b'', 'extra source blob output')
    return result


def rust_counts(text):
    return [tuple(map(int, row)) for row in re.findall(
        r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;', text)]


def verify(packet, repository):
    require(packet['schema'] == 'shardloom.prepared_sampling_pr_packet.v1', 'packet schema changed')
    evidence = Evidence(packet['members'])
    source = evidence.linked(packet['source'])
    require(source['schema'] == 'shardloom.prepared_sampling_pr_source.v1', 'source schema changed')
    head = source['head']
    require(head == '9e51ffc1a490bc39d50d50acd53c56ec5d72e473', 'validated source changed')
    require(source['accepted_source'] == '4cd3a9e125cad4bba082b6e80c7c600f71239891',
            'historical acceptance source changed')
    parents = git(repository, 'show', '-s', '--format=%P', head).decode().strip().split()
    require(parents == [source['pr_parent'], source['fixture_and_documentation_parent']],
            'source parents changed')
    require(git(repository, 'merge-base', source['accepted_source'], head).decode().strip()
            == source['accepted_source'], 'accepted source is not an ancestor')
    require(source['comparison_source_relabelled'] is False and
            source['performance_comparison_repeated'] is False and
            source['full43_repeated'] is False and source['original_format_matrix_repeated'] is False,
            'historical measurement scope changed')
    paths = git(repository, 'ls-tree', '-rz', '--name-only', head).decode().rstrip('\0').split('\0')
    names = sorted(name for name in paths if name.startswith(PREFIXES) or name in NAMES)
    require(names == sorted(source['source_hashes']) and len(names) == 2588, 'source closure incomplete')
    blobs = source_blobs(repository, head, names)
    require({name: digest(value) for name, value in blobs.items()} == source['source_hashes'],
            'source closure hashes changed')
    changed = set(git(repository, 'diff', '--name-only', source['accepted_source'], head, '--',
        'shardloom-*', 'vendor', 'python', 'scripts', 'benchmarks', '.github', '.cargo',
        'Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml').decode().splitlines())
    require(changed == CHANGES == set(source['changed_source_inputs']), 'integration source delta changed')
    fixture = 'python/tests/test_native_output_window_binding.py'
    old_pr = source_blobs(repository, source['pr_parent'], sorted(CHANGES - {fixture}))
    require(all(blobs[name] == value for name, value in old_pr.items()), 'prior PR inputs changed')
    require(digest(blobs[fixture]) == '3dfdc03fb74df626b1870426205f4894558045fbc4c12b72d24b4680744c436c',
            'tested output-window fixture changed')
    old_lock = tomllib.loads(git(repository, 'show', source['accepted_source'] + ':Cargo.lock').decode())
    new_lock = tomllib.loads(blobs['Cargo.lock'].decode())
    require(old_lock.keys() == new_lock.keys() and old_lock['version'] == new_lock['version']
            and len(old_lock['package']) == len(new_lock['package']), 'dependency graph shape changed')
    changes = []
    for before, after in zip(old_lock['package'], new_lock['package']):
        if before == after:
            continue
        require(before['name'] == after['name'] and
                {k: v for k, v in before.items() if k not in ('version', 'checksum')} ==
                {k: v for k, v in after.items() if k not in ('version', 'checksum')},
                'dependency fields changed')
        changes.append(dict(name=before['name'], before=before, after=after))
    require(changes == source['dependency_changes'] and
            [(row['name'], row['before']['version'], row['after']['version']) for row in changes] ==
            [('serde_json', '1.0.151', '1.0.154'), ('sha2', '0.11.0', '0.11.1')],
            'dependency bridge changed')
    for key, name in (('accepted_packet', 'acceptance.json'),
                      ('accepted_packet_payload', 'acceptance.json.xz')):
        raw = (repository / 'docs/benchmarks/evidence/prepared-sampling-2026-10-21' / name).read_bytes()
        require(len(raw) == source[key]['bytes'] and digest(raw) == source[key]['sha256'],
                'historical acceptance packet changed')
    helper = evidence.reference(source['helper'])
    guard = evidence.reference(source['guard'])
    supporting = packet['supporting_helpers']
    require(set(supporting) == {'process_supervisor', 'workload_guard'}, 'guard helper omitted')
    supervisor = evidence.reference(supporting['process_supervisor'])
    workload_guard = evidence.reference(supporting['workload_guard'])
    require(packet['native_execution_performed_during_replay'] is False
            and packet['publication_attempted'] is False, 'replay effect scope changed')
    require(set(packet['phases']) == set(PHASES), 'required phase omitted')
    receipts = set()
    phases = {}

    def run(refs):
        receipt = evidence.linked(refs['receipt'])
        log = evidence.reference(refs['log']).decode()
        require(receipt['status'] == 'passed' and receipt['returncode'] == 0
                and receipt['head'] == head and receipt['working_tree'] == ''
                and receipt['cwd'] == '/Users/dylan/.codex/worktrees/native-result-composition/shardloom-local-repo'
                and receipt['process_group_drained'] is True and receipt['direct_child_reaped'] is True
                and receipt['timed_out'] is False and receipt['interrupted_signal'] is None
                and receipt['group_absence_unproved_at_cleanup'] is False,
                'guarded check did not finish cleanly')
        require(receipt['runner_sha256'] == digest(guard) and receipt['log_sha256'] == digest(log.encode()),
                'guard runner or output changed')
        require(receipt['output_bytes'] == len(log.encode()), 'guard output was truncated')
        require(receipt['supervisor_sha256'] == digest(supervisor)
                and receipt['guard_sha256'] == digest(workload_guard), 'guard implementation changed')
        require(receipt['environment'] == dict(CARGO_TARGET_DIR='/Users/dylan/.cache/shardloom/cargo-target',
                CARGO_BUILD_JOBS='2', CARGO_INCREMENTAL='0', CARGO_PROFILE_DEV_DEBUG='0',
                CARGO_PROFILE_TEST_DEBUG='0'), 'build guard environment changed')
        receipts.add(refs['receipt']['path'])
        return receipt, log

    for name in PHASES:
        record = evidence.linked(packet['phases'][name])
        require(record['status'] == 'passed' and record['phase'] == name
                and record['source'] == packet['source'] and record['helper'] == source['helper'],
                'phase source/status changed: ' + name)
        phases[name] = record

    cargo = ['/Users/dylan/.cargo/bin/rustup', 'run', '1.99.0', 'cargo']
    commands = {
        'format': [*cargo, 'fmt', '--all', '--', '--check'],
        'workspace-lint': [*cargo, 'clippy', '--offline', '--locked', '--workspace', '--all-targets', '--', '-D', 'warnings'],
        'workspace-tests': [*cargo, 'test', '--offline', '--locked', '--workspace', '--all-targets', '--', '--test-threads=2'],
        'native-lint': [*cargo, 'clippy', '--release', '--offline', '--locked', '-p', 'shardloom-vortex', '-p', 'shardloom-cli', '--all-targets', '--features', 'release-user-surfaces', '--', '-D', 'warnings'],
        'native-tests': [*cargo, 'test', '--release', '--offline', '--locked', '-p', 'shardloom-vortex', '--all-targets', '--features', 'release-user-surfaces', '--', '--test-threads=2'],
        'native-cli': [*cargo, 'test', '--release', '--offline', '--locked', '-p', 'shardloom-cli', '--lib', '--bin', 'shardloom', '--test', 'vortex_prepare', '--test', 'public_memory_inputs', '--test', 'public_workflow_route', '--test', 'resident_worker', '--features', 'release-user-surfaces', '--', '--test-threads=2'],
        'minimal-feature': [*cargo, 'check', '--offline', '--locked', '-p', 'shardloom-vortex', '--features', 'vortex-local-primitives'],
    }
    totals = {}
    for name, command in commands.items():
        receipt, log = run(phases[name]['run'])
        require(receipt['command'] == command, 'validation command narrowed: ' + name)
        if name in ('workspace-tests', 'native-tests', 'native-cli'):
            counts = rust_counts(log)
            expected_targets, expected = {'workspace-tests': (107, (3235, 0, 0)),
                'native-tests': (13, (3107, 0, 31)), 'native-cli': (6, (1091, 0, 0))}[name]
            total = tuple(sum(row[index] for row in counts) for index in range(3))
            require(len(counts) == expected_targets and total == expected and
                    [list(row) for row in counts] == phases[name]['counts'] and
                    phases[name]['targets'] == expected_targets and
                    [phases[name][key] for key in ('passed', 'failed', 'ignored')] == list(expected),
                    'test coverage changed: ' + name)
            totals[name] = dict(targets=len(counts), passed=total[0], failed=total[1], ignored=total[2])
    optional = {'output_window_observation::tests': ('release-user-surfaces,output-window-observation', 16),
                'credit_window::tests': ('release-user-surfaces,output-window-observation', 9),
                'encoded_arrow_control_': ('native-encoded-exchange-experiment', 11)}
    require(set(phases['optional-tests']['checks']) == set(optional), 'optional check omitted')
    for selector, (features, expected) in optional.items():
        record = phases['optional-tests']['checks'][selector]
        receipt, log = run(record['run'])
        require(receipt['command'] == [*cargo, 'test', '--offline', '--locked', '-p', 'shardloom-vortex',
                '--lib', '--features', features, selector, '--', '--test-threads=2'] and
                rust_counts(log) == [(expected, 0, 0)] and record['counts'] == [[expected, 0, 0]],
                'optional test coverage changed')
    python = '/Users/dylan/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3'
    require(set(phases['python']['shards']) == {'core', 'release_scripts'}, 'Python shard omitted')
    for name, count, ran, skipped in (('core', 700, 559, 23), ('release_scripts', 168, 168, 0)):
        record = phases['python']['shards'][name]
        receipt, log = run(record['run'])
        report = evidence.linked(record['report'])
        require(report['status'] == 'passed' and report['test_count'] == count and
                report['failure_count'] == report['error_count'] == 0 and report['skipped_count'] == skipped
                and report['fallback_attempted'] is False and report['external_engine_invoked'] is False,
                'Python shard coverage changed')
        require(record['discovered'] == count and record['skipped'] == skipped,
                'Python shard summary changed')
        require(re.findall(r'(?m)^Ran (\d+) tests in ', log) == [str(ran)] and
                re.search(r'(?m)^OK' + (r' \(skipped=23\)' if skipped else '') + r'$', log),
                'Python shard log disagrees with report')
        require(receipt['command'] == [python, '-B', 'scripts/run_python_test_shard.py', '--shard', name,
                '--output-dir', str(Path(record['report']['path']).parent)], 'Python shard command changed')
    receipt, log = run(phases['python']['focused'])
    require(receipt['command'] == [python, '-B', '-m', 'unittest', 'scripts.test_python_license_bundle',
            'scripts.test_native_cyclic_existence_compare', 'scripts.test_native_cyclic_existence_workload', '-v']
            and re.findall(r'Ran (\d+) tests in .*\n\nOK\s*$', log) == ['18']
            and phases['python']['focused_test_count'] == 18, 'focused Python tests changed')
    build = evidence.linked(phases['build']['build'])
    require(build['source'] == packet['source'] and build['source_commit'] == head and
            build['status'] == 'passed', 'build source changed')
    cli_receipt, cli_log = run(phases['build']['cli'])
    library_receipt, library_log = run(phases['build']['extension'])
    require(cli_receipt['command'] == [*cargo, 'build', '--release', '--offline', '--locked', '-p',
            'shardloom-cli', '--bin', 'shardloom', '--features', 'release-user-surfaces',
            '--message-format=json-render-diagnostics'], 'CLI build command changed')
    require(library_receipt['command'] == [python, '-B', 'scripts/build_native_python.py', '--release',
            '--offline', '--output-dir', str(Path(build['binary']['path']).parent / 'native')],
            'library build command changed')
    messages = [json.loads(line) for line in cli_log.splitlines() if line.startswith('{')]
    artifacts = [row for row in messages if row.get('reason') == 'compiler-artifact' and
                 row.get('target', {}).get('name') == 'shardloom' and row.get('executable')]
    require(artifacts == [build['cli_compiler_artifact']] and artifacts[0]['profile']['opt_level'] == '3',
            'release CLI compiler artifact changed')
    require(build['native_build']['status'] == 'passed' and
            build['native_build']['library_sha256'] == build['library']['sha256'], 'library identity changed')
    decoded_builds = []
    for match in re.finditer(r'(?m)^\{', library_log):
        try:
            candidate, _ = json.JSONDecoder().raw_decode(library_log[match.start():])
        except json.JSONDecodeError:
            continue
        if isinstance(candidate, dict) and candidate.get('schema_version') == 'shardloom.native_python_build.v1':
            decoded_builds.append(candidate)
    require(decoded_builds == [build['native_build']], 'library build output changed')
    wheel = phases['wheel']
    receipt, _ = run(wheel['run'])
    report = evidence.linked(wheel['report'])
    directory = Path(wheel['report']['path']).parent
    expected_integration = dict(tests=118, failures=0, errors=0, skipped=0, status='passed')
    require(report['status'] == report['uninstall_status'] == report['integrity']['status'] == 'passed'
            and report['integration'] == expected_integration == evidence.linked(wheel['integration'])
            and report['publication_attempted'] is False and report['performance_claim'] is False
            and report['installed_load']['isolated_install'] is True
            and wheel['complete_repository_tests_without_overlay'] is True, 'installed wheel proof incomplete')
    require(report['integrity']['cli_sha256'] == build['binary']['sha256'] and
            report['integrity']['native_library_sha256'] == build['library']['sha256'],
            'installed artifact differs from fresh build')
    require(receipt['command'][0] == '/Users/dylan/LocalData/shardloom/native-python-surface-20261010/release-v0.6.0/build-env/bin/python'
            and receipt['command'][1:5] == ['-B', 'scripts/prove_native_python_wheel.py', '--repo-root', receipt['cwd']]
            and receipt['command'][5:] == ['--output-dir', str(directory), '--native-library',
                build['library']['path'], '--cli-binary', build['binary']['path'], '--wheelhouse',
                str(directory.parent / 'pyarrow-wheelhouse-cp312')], 'normal wheel command changed')
    require([step['name'] for step in report['steps']] == ['build', 'create_environment',
        'install_optional_consumer', 'install', 'load', 'integration', 'uninstall', 'verify_uninstalled'],
        'installed package lifecycle step omitted')
    for step in report['steps']:
        require(step['returncode'] == 0 and digest(evidence.raw(directory / step['log'])) == step['log_sha256'],
                'installed package lifecycle failure')
    discovery = []
    for name, raw in blobs.items():
        if name.startswith('python/tests/test_native') and name.endswith('binding.py'):
            discovery.extend(f'{Path(name).stem}.{cls.name}.{method.name}'
                for cls in ast.parse(raw).body if isinstance(cls, ast.ClassDef)
                for method in cls.body if isinstance(method, ast.FunctionDef) and method.name.startswith('test_'))
    executed = sorted(match.group(1) for line in evidence.raw(directory / 'integration.log').decode().splitlines()
        if (match := re.fullmatch(r'test_\w+ \(([^)]+)\) \.\.\. ok', line)))
    require(len(discovery) == len(set(discovery)) == 118 and executed == sorted(discovery),
            'normal installed test discovery narrowed')
    nodes = [node for node in ast.parse(blobs['scripts/prove_native_python_wheel.py']).body
             if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == 'RUN_TESTS'
                                                    for target in node.targets)]
    require(len(nodes) == 1, 'normal wheel runner absent')
    selected = next(step for step in report['steps'] if step['name'] == 'integration')
    require(selected['command'][1:4] == ['-I', '-c', ast.literal_eval(nodes[0].value)]
            and selected['command'][4] == str(Path(receipt['cwd']) / 'python/tests'),
            'installed test runner used an overlay')
    checks = phases['references']['checks']
    require(set(checks) == {'python_license_bundle', 'user_surface_reference', 'public_status_docs',
            'contribution_governance', 'v1_inclusion_scope', 'golden_workflows', 'original_packet_replay'},
            'reference validation omitted')
    for name, refs in checks.items():
        receipt, log = run(refs)
        if name == 'golden_workflows':
            require(receipt['command'][2:6] == ['scripts/check_golden_workflows.py', '--skip-build',
                '--binary', build['binary']['path']], 'golden workflows did not use the verified CLI')
        elif name == 'original_packet_replay':
            require(receipt['command'][2].endswith('/prepared-sampling-2026-10-21/verify_acceptance.py')
                    and '--manifest' in receipt['command'] and '--scratch' in receipt['command'],
                    'original acceptance replay omitted')
            summary = json.loads(log)
            require(summary['status'] == 'passed' and summary['members'] == 28698
                    and summary['full43_queries'] == 43 and summary['modular_records'] == 1408,
                    'original acceptance replay incomplete')
        else:
            require(receipt['command'][:3] == [python, '-B', 'scripts/check_' + name + '.py'],
                    'reference command changed')
    require(len(receipts) == 23, 'guarded command coverage changed')
    require(len(packet['replay_corrections']) == 1, 'replay correction history omitted')
    correction = evidence.linked(packet['replay_corrections'][0])
    require(correction['status'] == 'replay_corrected'
            and correction['observed'] == dict(discovered=700, ran=559, skipped_entries=23)
            and correction['source_changed'] is False and correction['test_suite_repeated'] is False
            and correction['native_query_repeated'] is False
            and evidence.linked(correction['phase']) == phases['python'],
            'Python discovery/run correction changed')
    for key in ('original_verifier', 'original_packager'):
        evidence.reference(correction[key])
    require(b'Python shard log disagrees with report' in evidence.reference(correction['failed_replay_log']),
            'original replay failure omitted')
    return dict(status='passed', source_commit=head, source_files=len(names), phases=len(PHASES),
        guarded_commands=len(receipts), tests=totals, installed_tests=118, installed_skipped=0,
        optional_tests=36, python_core_discovered=700, python_core_ran=559,
        python_core_skipped_entries=23,
        python_release_tests=168, python_focused_tests=18, dependency_changes=2,
        historical_measurements_relabelled=False, native_execution_performed=False,
        payload_residency_revalidated=False, publication_attempted=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository', type=Path, default=Path.cwd())
    parser.add_argument('--manifest', type=Path, required=True)
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_bytes())
    require(manifest['schema'] == 'shardloom.prepared_sampling_pr_integration.v1', 'manifest schema changed')
    verifier = Path(__file__).read_bytes()
    require(len(verifier) == manifest['verifier']['bytes'] and digest(verifier) == manifest['verifier']['sha256'],
            'verifier identity changed')
    payload = (args.manifest.parent / manifest['packet']['file']).read_bytes()
    require(len(payload) == manifest['packet']['bytes'] and digest(payload) == manifest['packet']['sha256'],
            'compressed packet identity changed')
    packet = json.loads(lzma.decompress(payload))
    require(len(packet['members']) == manifest['packet']['members'], 'packet member count changed')
    result = verify(packet, args.repository.resolve())
    source = Evidence(packet['members']).linked(packet['source'])
    require(result == manifest['replay'] and manifest['source_commit'] == result['source_commit']
            and manifest['original_acceptance_source'] == source['accepted_source'], 'manifest replay differs')
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
