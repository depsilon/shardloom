"""Replay saved wide-schema acceptance without a checkout or native execution."""
import argparse
import ast
import hashlib
import io
import json
import lzma
from pathlib import Path, PurePosixPath
import re
import subprocess
import tarfile

BASE = '9c4ada03e1c30ed20fd2118c78a8be3fb69b0dd8'
HEAD = '12507b3b6f918a7e808564aca7e9ea3159b949b5'
NATIVE = 'shardloom-python/python-native,shardloom-vortex/native-encoded-exchange-experiment'
CARGO = {
    'format': ['cargo', 'fmt', '--all', '--', '--check'],
    'default_clippy': ['cargo', 'clippy', '--workspace', '--all-targets', '--locked', '--offline', '--', '-D', 'warnings'],
    'native_clippy': ['cargo', 'clippy', '--workspace', '--all-targets', '--features', NATIVE, '--locked', '--offline', '--', '-D', 'warnings'],
    'default_tests': ['cargo', 'test', '--workspace', '--all-targets', '--locked', '--offline'],
    'native_tests': ['cargo', 'test', '--workspace', '--all-targets', '--features', NATIVE, '--locked', '--offline'],
    'cold_file_native': ['cargo', 'test', '--locked', '--offline', '-p', 'shardloom-vortex', '--features',
        'vortex-local-primitives,vortex-write,universal-format-io', '--lib',
        'native_file_metadata_cold_wide_refusal_and_retry_cover_statistics_modes', '--', '--nocapture'],
    'doc_contracts': ['cargo', 'test', '-p', 'shardloom-contract-tests', '--all-targets', '--locked', '--offline'],
    'build': ['cargo', 'build', '--locked', '--offline', '--release', '--features', 'release-user-surfaces', '-p', 'shardloom-cli', '-p', 'shardloom-python'],
}
RUNNERS = {
    'features': 'check_vector_features.py', 'freeze': 'freeze_runtime.py',
    'python_core': 'scripts/run_python_test_shard.py',
    'docs': 'check_wide_schema_docs.py', 'installed': 'scripts/prove_native_python_wheel.py',
    'msrv': 'check_vector_msrv.py',
    'provider_arrays': 'scripts/ci_provider_tests.py',
    'provider_io': 'scripts/ci_provider_tests.py',
    'prior_refusals': 'python/tests/test_native_wide_schema_binding.py',
    'wheel_runner_tests': 'scripts/test_prove_native_python_wheel.py',
}
ROLES = set(CARGO) | set(RUNNERS) | {'public'}
MODULES = (
    'test_native_wide_schema_binding', 'test_wide_schema',
)
NATIVE_MODULES = MODULES[:1]
NATIVE_TEST_SOURCES = (
    'shardloom-vortex/src/native_payload_schema_tests.rs',
)
EXTRA_NATIVE_TESTS = {
    'shardloom-vortex/src/local_primitive_native_sink_tests.rs': (
        'wide_nested_footer_admission_fails_before_staging_and_releases_schema_credit',),
    'shardloom-vortex/src/native_provider_file_metadata_tests.rs': (
        'native_file_metadata_cold_wide_refusal_and_retry_cover_statistics_modes',),
}
PROVIDER_TEST_SOURCE = 'vendor/vortex-array/src/dtype/struct_memory_tests.rs'
# These are exact, reviewable exceptions, not a filename-extension filter.
# Cargo gates do not run the separately discovered Python unittest modules.
# Status prose may be completed after runtime validation; final docs validation
# and the complete contract-test crate must see it. Every before/after identity
# remains in the original receipts.
STATUS_PROSE = {
    'docs/architecture/phased-execution-plan.md',
    'docs/architecture/native-wide-schema-2026-11-09.md',
    'docs/architecture/local-development-storage.md',
    'docs/reference/rich-columns.md',
    'docs/architecture/native-input-growth-2026-10-09.md',
    'docs/architecture/native-dense-embedding-lifecycle-2026-11-05.md',
}
PYTHON_TEST_ONLY = {'python/tests/' + name + '.py' for name in NATIVE_MODULES} | {
    # The native Rust suite does not run the wheel-selection fixture. Its own
    # required gate below must see the final fixture and the final proof runner.
    'scripts/test_prove_native_python_wheel.py',
}
# The predecessor refusal check loads the retained accepted library, not the
# candidate's Rust/provider inputs. Keep its original inventories and exact
# later changes visible. The executed refusal function and imported Python
# helpers must remain identical below; added cold-reopen coverage is separate.
# Every candidate provider/engine/build/public gate sees final compiled inputs.
PRIOR_RUNTIME_ONLY_FOLLOWUPS = {
    'shardloom-vortex/src/local_primitive_native_sink.rs',
    'shardloom-vortex/src/local_primitive_native_sink_tests.rs',
    'shardloom-vortex/src/native_payload_schema.rs',
    'shardloom-vortex/src/local_primitives/native_input_schema.rs',
    'shardloom-vortex/src/resident_memory_arrow_encoded_tests.rs',
    'shardloom-cli/src/sql_native_relational_unary_tests.rs',
    'python/THIRD_PARTY_LICENSES/manifest.json',
    'python/tests/test_native_wide_schema_binding.py',
    'scripts/test_prove_native_python_wheel.py',
    'shardloom-vortex/src/binary_object.rs',
    'shardloom-vortex/src/binary_object_catalog.rs',
    'shardloom-vortex/src/binary_object_layout.rs',
    'shardloom-vortex/src/binary_object_writer.rs',
    'shardloom-vortex/src/local_primitive_columnar_compat_sink.rs',
    'shardloom-vortex/src/local_primitive_completed_result.rs',
    'shardloom-vortex/src/local_primitive_native_text_sink.rs',
    'shardloom-vortex/src/native_binary_type.rs',
    'shardloom-vortex/src/native_provider_file_metadata_tests.rs',
    'shardloom-vortex/src/resident_result_arrow.rs',
    'shardloom-vortex/src/vortex_ingest.rs',
    'vendor/vortex-array/README.shardloom.md',
    'vendor/vortex-array/src/dtype/struct_.rs',
    'vendor/vortex-array/src/dtype/struct_memory_tests.rs',
    'vendor/vortex-array/src/scalar_fn/fns/get_item.rs',
    'vendor/vortex-array/src/scalar_fn/fns/select.rs',
    'vendor/vortex-array/upstream-provenance.json',
    'vendor/vortex-file/README.shardloom.md',
    'vendor/vortex-file/src/footer/file_statistics.rs',
    'vendor/vortex-file/src/footer/file_statistics_storage.rs',
    'vendor/vortex-file/upstream-provenance.json',
    'vendor/vortex-layout/README.shardloom.md',
    'vendor/vortex-layout/src/layouts/struct_/mod.rs',
    'vendor/vortex-layout/src/layouts/struct_/reader.rs',
    'vendor/vortex-layout/upstream-provenance.json',
}
FROZEN = {
    'python/tests/wide_schema_fixture.py': 'c4a00c15212678912c1355c7f5a164607b878b57bcb395434f52e70f2d82c90f',
}
FROZEN_FIXTURE_REPORT_SHA256 = '5295aafabfc8e30cda83c8023469c1faa1d3180f7fa6dc1ef451e35c08605114'
EXCLUDED = ('docs/benchmarks/evidence/', 'docs/release/evidence/', 'docs/dependencies/evidence/')
REQUIRED_HISTORICAL_FAILURES = {
    'native-check-r1.json', 'native-check-r2.json', 'native-library-tests-r1.json',
    'native-failure-details-r1.json', 'native-focused-r1.json', 'provider-arrays-r1.json',
    'public-r1.json',
    'native-clippy-r1.json', 'native-tests-r1.json',
    'features-r1.json',
    'cold-file-denial-r1.json', 'cold-file-denial-r2.json', 'cold-file-native-r1.json',
}
LIMITATIONS = [
    'Schema depth 24 remains. Wider schema admission does not complete deep provider serialization, value traversal, destruction or recursive plan/expression traversal.',
    'Serialized declaration, JSON cell/result and physical IPC framing limits remain separate. A memory grant does not override a format or transport representation limit.',
    'Native schema and provider container credits are ownership accounting, not an RSS bound. Detached caller-owned names/DTypes, arbitrary extensions and unrelated provider scratch remain outside those container owners.',
    'Native installed-wheel proof is local macOS arm64. Hosted PR and exact-main acceptance are a separate closeout.',
    'Saved failures remain evidence; default/native opt-in ignores and Python optional/environment skips remain explicit. Installed-package and selected public wide-schema acceptance require zero skips.',
    'No benchmark, performance superiority, package publication, whole item 19 or CG-1 through CG-23 completion claim.',
]
PRIOR_RECEIPTS = (
    'vector-main1671-r1.json', 'vector-main1671-r1-raw/checks.json',
    'vector-main1671-r1-raw/commit.json', 'vector-main1671-r1-raw/workflows.json',
    'pr1671-final-checks-r1.json', 'pr1671-final-raw-r1/checks.json',
    'pr1671-final-raw-r1/comments.json', 'pr1671-final-raw-r1/pull.json',
    'pr1671-final-raw-r1/review.json', 'pr1671-final-review-r1.json',
    'vector-retirement-r2/receipt.json', 'vector-retirement-r2/plan.json',
    'vector-retirement-r2/manifest.json.xz', 'runtime-r2-identity.json',
)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def identity(raw):
    return dict(bytes=len(raw), sha256=digest(raw))


def fingerprint(value):
    return digest(json.dumps(value, sort_keys=True, separators=(',', ':')).encode())


def rust_counts(raw):
    rows = re.findall(rb'test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;', raw)
    return dict(zip(('passed', 'failed', 'ignored'),
        (sum(int(row[i]) for row in rows) for i in range(3)))) if rows else None


def test_methods(raw, module):
    return sorted(module + '.' + node.name + '.' + method.name
        for node in ast.parse(raw).body if isinstance(node, ast.ClassDef)
        for method in node.body if isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef))
        and method.name.startswith('test_'))


def successful_methods(raw, modules):
    pattern = '|'.join(re.escape(name) for name in modules)
    return sorted(module + '.' + method for module, method in re.findall(
        r'^test_\S+ \((' + pattern + r')\.([^\)]+)\) \.\.\. ok$', raw.decode(), re.MULTILINE))


def source_deltas(before, after):
    return {name: dict(before=before.get(name), after=after.get(name))
            for name in sorted(set(before) | set(after)) if before.get(name) != after.get(name)}


def verify(packet, repo_root=None, require_controls=False):
    report = json.loads((packet / 'acceptance.json').read_bytes())
    assert report['schema_version'] == 'shardloom.wide_schema_acceptance.v1'
    assert report['status'] == 'local_acceptance_passed' and report['source_base'] == BASE
    assert report['archive'] == 'verification.tar.xz'
    archive_raw = (packet / report['archive']).read_bytes()
    assert len(archive_raw) < 90 << 20 and digest(archive_raw) == report['archive_sha256']
    members = report['members']
    assert len(members) == len({row['path'] for row in members})
    assert all(isinstance(row['bytes'], int) and row['bytes'] >= 0 for row in members)
    assert sum(row['bytes'] for row in members) < 512 << 20
    data = {}
    with tarfile.open(fileobj=io.BytesIO(archive_raw), mode='r:xz') as archive:
        entries = archive.getmembers()
        assert [entry.name for entry in entries] == [row['path'] for row in members]
        for row, entry in zip(members, entries):
            path = PurePosixPath(row['path'])
            assert entry.isfile() and not path.is_absolute() and '..' not in path.parts
            assert path.as_posix() == row['path'] and entry.size == row['bytes']
            raw = archive.extractfile(entry).read()
            assert identity(raw) == {key: row[key] for key in ('bytes', 'sha256')}
            data[row['path']] = raw
    assert Path(__file__).read_bytes() == data['current/tools/verify_wide_schema_packet.py']

    def read(name):
        return json.loads(data[name])

    def blob(info):
        raw = data['current/source-blobs/' + info['sha256']]
        assert identity(raw) == info
        return raw

    def check_inventory(inventory):
        for name, info in inventory.items():
            assert not PurePosixPath(name).is_absolute() and '..' not in PurePosixPath(name).parts
            if 'symlink' in info:
                assert digest(info['symlink'].encode()) == info['sha256']
            elif 'sha256' in info:
                blob(info)
            else:
                assert info == {'absent': True}, (name, info)

    for name, raw in data.items():
        if name.startswith('current/source-blobs/'):
            assert name.rsplit('/', 1)[1] == digest(raw)
        if name.startswith('current/') and name.count('/') == 1 and name.endswith('.json'):
            value = json.loads(raw)
            if not isinstance(value, dict):
                continue
            assert value.get('status') != 'running', name
            if 'log_sha256' in value:
                assert digest(data[name.removesuffix('.json') + '.log']) == value['log_sha256']
            if 'build_sources' in value:
                check_inventory(value['build_sources'])
                assert fingerprint(value['build_sources']) == value['build_source_fingerprint_sha256']
                assert len(value['build_sources']) == value['build_source_path_count']
                for info in value.get('tool_source_inventory', {}).values():
                    blob(info)
    roles = report['roles']
    assert set(roles) == ROLES and len(set(roles.values())) == len(ROLES)
    receipts = {role: read('current/' + filename) for role, filename in roles.items()}
    source = receipts['docs']['build_sources']
    assert report['source_snapshot'] == source
    assert report['source_fingerprint_sha256'] == fingerprint(source)
    check_inventory(source)

    def source_raw(name):
        return blob(source[name])

    def log(role):
        return data['current/' + roles[role].removesuffix('.json') + '.log']

    for role, result in receipts.items():
        assert result['status'] == 'passed' and result['returncode'] == 0, role
        assert result['process_group_drained'] and result['direct_child_reaped'], role
        assert result['fixed_source_validation'] and result['head'] == HEAD and result['base'] == BASE
        assert result['prior_closeout_sha256'] == digest(data['prior/vector-closeout-r1.json'])
        assert result['driver_sha256'] == digest(data['current/tools/guard_wide_schema.py'])
        for phase in ('pre_execution', 'post_execution'):
            observed = result['build_source_verification'][phase]
            assert observed['head'] == HEAD and observed['head_matches'] and observed['path_set_matches']
            assert observed['changed_paths'] == []
            assert observed['path_count'] == len(result['build_sources'])
            assert observed['fingerprint_sha256'] == fingerprint(result['build_sources'])
        assert result['environment']['CARGO_BUILD_JOBS'] == '2'
        assert result['environment']['CARGO_INCREMENTAL'] == '0'
        deltas = source_deltas(result['build_sources'], source)
        allowed = set() if role in {'docs', 'doc_contracts'} else STATUS_PROSE
        if role in (set(CARGO) - {'doc_contracts'}) | {'features', 'msrv', 'freeze'}:
            allowed = allowed | PYTHON_TEST_ONLY
        if role == 'prior_refusals':
            allowed = allowed | PRIOR_RUNTIME_ONLY_FOLLOWUPS
        assert set(deltas) <= allowed, (role, set(deltas) - allowed)
        assert report['source_deltas'][role] == deltas
        if role in CARGO:
            assert result['command'][result['command'].index('cargo'):] == CARGO[role]
        elif role in RUNNERS:
            runner = RUNNERS[role]
            matches = [arg for arg in result['command'] if arg.endswith(runner)]
            assert len(matches) == 1, role
            recorded = [(name, info) for name, info in result['tool_source_inventory'].items()
                        if name.endswith('/' + runner)]
            assert len(recorded) == 1, role
            expected = (blob(result['build_sources'][runner]) if role == 'prior_refusals' else
                source_raw(runner) if runner.startswith(('scripts/', 'python/')) else data['current/tools/' + runner])
            assert identity(expected) == recorded[0][1], role
        elif role == 'public':
            assert 'SHARDLOOM_REQUIRE_NATIVE_PYTHON=1' in result['command']
            assert result['command'][-len(MODULES) - 4:] == ['-B', '-m', 'unittest', '-v', *MODULES]
        summary = dict(receipt=roles[role], status='passed', command=result['command'], rust_tests=rust_counts(log(role)))
        assert report['checks'][role] == summary
        if summary['rust_tests']:
            assert summary['rust_tests']['passed'] > 0 and summary['rust_tests']['failed'] == 0
    if repo_root:
        paths = set()
        for argv in (['git', 'ls-files', '--cached', '-z'], ['git', 'ls-files', '--others', '--exclude-standard', '-z']):
            paths.update(filter(None, subprocess.check_output(argv, cwd=repo_root).decode().split('\0')))
        paths = {name for name in paths if not name.startswith(EXCLUDED) and '.git' not in Path(name).parts}
        assert paths == set(source), 'current source path set differs from final docs validation'
        for name, info in source.items():
            path = repo_root / name
            if 'symlink' in info:
                assert path.is_symlink() and str(path.readlink()) == info['symlink']
            elif info.get('absent'):
                assert not path.exists() and not path.is_symlink()
            else:
                assert path.is_file() and not path.is_symlink() and identity(path.read_bytes()) == info, name

    required_native = []
    for name in NATIVE_TEST_SOURCES:
        methods = re.findall(r'#\[test\]\s*fn\s+(\w+)\s*\(', source_raw(name).decode())
        assert methods, name
        required_native.extend(methods)
    for path, methods in EXTRA_NATIVE_TESTS.items():
        defined = re.findall(r'#\[test\]\s*fn\s+(\w+)\s*\(', source_raw(path).decode())
        assert set(methods) <= set(defined), path
        required_native.extend(methods)
    assert len(required_native) == len(set(required_native))
    for name in required_native:
        assert re.search(rb'::' + re.escape(name.encode()) + rb' \.\.\. ok$', log('native_tests'), re.MULTILINE), name
    assert report['required_native_tests'] == sorted(required_native)
    assert b'::wide_nested_footer_admission_fails_before_staging_and_releases_schema_credit ... ok' in log('native_tests')
    cold_test = b'::native_file_metadata_cold_wide_refusal_and_retry_cover_statistics_modes ... ok'
    assert cold_test in log('native_tests') and cold_test in log('cold_file_native')
    for name, sha in FROZEN.items():
        assert digest(source_raw(name)) == sha, name
    assert report['frozen_references'] == FROZEN
    assert digest(data['current/preparation/frozen-public-fixture-r1.json']) == FROZEN_FIXTURE_REPORT_SHA256
    frozen = read('current/preparation/frozen-public-fixture-r1.json')
    assert frozen['schema_version'] == 'shardloom.wide_schema_fixture.v1'
    assert frozen['fixture_source'] == identity(source_raw('python/tests/wide_schema_fixture.py'))
    assert frozen['memory_bytes'] == 512 << 20 and not frozen['baseline_or_candidate_execution']
    assert set(frozen['cases']) == {'siblings', 'nodes', 'metadata'}
    for case in frozen['cases'].values():
        assert case['input_rows'] == 8 and case['joined_rows'] == 9
    assert frozen['spill'] == dict(kind='siblings', input_rows=257, buffer_bytes=1 << 20,
        quota_bytes=64 << 20, joined_rows=341,
        joined_digest='d762b8891c60aec187cd79286794439bc0aeb78d0134e1ec2c32a03f138ed635')
    # The exact original contract is retained even after its status prose is updated.
    blob(frozen['contract_source'])
    prior_lines = [line for line in log('prior_refusals').splitlines() if line.startswith(b'[')]
    assert len(prior_lines) == 1
    refused = json.loads(prior_lines[0])
    assert [item['kind'] for item in refused] == ['siblings', 'nodes', 'metadata']
    for item in refused:
        assert item['refused'] and item['diagnostic_codes'] == ['SL_INVALID_INPUT']
        expected = '1..=1024 fields' if item['kind'] == 'siblings' else '4096 nodes'
        assert item['expected_boundary'] == expected and expected in item['error']
    prior_command = receipts['prior_refusals']['command']
    assert prior_command[-2:] == ['python/tests/test_native_wide_schema_binding.py', '--prior-refusals']
    assert 'SHARDLOOM_REQUIRE_NATIVE_PYTHON=1' in prior_command
    prior_library = next(arg.split('=', 1)[1] for arg in prior_command
                         if arg.startswith('SHARDLOOM_NATIVE_TEST_LIBRARY='))
    assert prior_library in receipts['prior_refusals']['prior_retained_runtime_and_wheel']
    # The predecessor refusal code and every original public test are unchanged;
    # the candidate adds cold-read coverage without weakening the frozen lifecycle.
    public_source = 'python/tests/test_native_wide_schema_binding.py'
    previous_source = receipts['prior_refusals']['build_sources']
    before_tree = ast.parse(blob(previous_source[public_source]))
    after_tree = ast.parse(source_raw(public_source))
    def definitions(tree):
        return {node.name: ast.dump(node, include_attributes=False) for node in tree.body
                if isinstance(node, (ast.FunctionDef, ast.ClassDef))}
    before_definitions, after_definitions = definitions(before_tree), definitions(after_tree)
    for function in ('_prior_refusals', '_reopen'):
        assert before_definitions[function] == after_definitions[function]
    for dependency in ('python/tests/test_native_binding.py', 'python/tests/wide_schema_fixture.py'):
        assert previous_source[dependency] == source[dependency]
    before_class = next(node for node in before_tree.body if isinstance(node, ast.ClassDef)
                        and node.name == 'NativeWideSchemaBindingTests')
    after_class = next(node for node in after_tree.body if isinstance(node, ast.ClassDef)
                       and node.name == 'NativeWideSchemaBindingTests')
    before_methods, after_methods = definitions(before_class), definitions(after_class)
    assert all(after_methods[name] == body for name, body in before_methods.items())
    for tree in (before_tree, after_tree):
        grant = next(node.value for node in tree.body if isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == 'GRANT' for target in node.targets))
        assert ast.dump(grant) == ast.dump(ast.parse('512 << 20', mode='eval').body)
    assert report['public_tests'] == successful_methods(log('public'), MODULES) == sorted(
        name for module in MODULES for name in test_methods(source_raw('python/tests/' + module + '.py'), module))
    assert len(report['public_tests']) == 8 and b'\nOK\n' in log('public') and b'... skipped' not in log('public')
    wheel_runner_methods = test_methods(source_raw('scripts/test_prove_native_python_wheel.py'), '__main__')
    assert len(wheel_runner_methods) == 9
    assert successful_methods(log('wheel_runner_tests'), ['__main__']) == wheel_runner_methods
    assert b'\nOK\n' in log('wheel_runner_tests') and b'... skipped' not in log('wheel_runner_tests')
    feature_tree = ast.parse(data['current/tools/check_vector_features.py'])
    commands = next(ast.literal_eval(node.value) for node in feature_tree.body if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == 'commands' for target in node.targets))
    assert len(commands) == 7
    assert [line.removeprefix('Checking ') for line in log('features').decode().splitlines() if line.startswith('Checking ')] == [
        ' '.join(command + ['--locked', '--offline']) for command in commands]
    msrv = [
        ['rustup', 'run', '1.96.0', 'cargo', 'check', '--workspace', '--no-default-features'],
        ['rustup', 'run', '1.96.0', 'cargo', 'check', '--workspace', '--all-targets', '--features', 'release-user-surfaces'],
    ]
    assert [line.removeprefix('Checking ') for line in log('msrv').decode().splitlines() if line.startswith('Checking ')] == [
        ' '.join(command + ['--locked', '--offline']) for command in msrv]
    provider_checks = {}
    provider_tree = ast.parse(source_raw('scripts/ci_provider_tests.py'))
    def literal(name):
        return next(ast.literal_eval(node.value) for node in provider_tree.body
                    if isinstance(node, ast.Assign) and any(
                        isinstance(target, ast.Name) and target.id == name for target in node.targets))
    groups, configurations = literal('GROUPS'), literal('CONFIGURATIONS')
    for group in ('arrays', 'io'):
        role = 'provider_' + group
        command = receipts[role]['command']
        assert command[command.index('--group') + 1] == group
        provider_root = 'current/' + Path(command[command.index('--output') + 1]).name
        provider = read(provider_root + '/report.json')
        assert provider['status'] == 'passed' and provider['group'] == group
        assert len(provider['commands']) == len(groups[group]) * 2
        for index, entry in enumerate(provider['commands']):
            crate = groups[group][index // 2]
            operation = 'test' if index % 2 == 0 else 'clippy'
            config = configurations[crate]
            expected = ['cargo', operation, '--locked', '--manifest-path', f'vendor/{crate}/Cargo.toml']
            for patch in config['patches']:
                expected += ['--config', f'patch.crates-io.{patch}.path="vendor/{patch}"']
            expected += ['--features', config['features'], '--lib']
            if operation == 'clippy' and config.get('clippy_tests'):
                expected += ['--tests']
            expected += ['--', '--test-threads=2'] if operation == 'test' else ['--', '-D', 'warnings']
            assert entry['command'] == expected and entry['crate'] == crate and entry['returncode'] == 0
            assert entry['lock_sha256'] == digest(source_raw('.github/provider-locks/' + crate + '.lock'))
            provider_log = data[provider_root + '/' + entry['log']]
            assert digest(provider_log) == entry['log_sha256']
            if operation == 'test':
                counts = rust_counts(provider_log)
                assert counts and counts['passed'] > 0 and counts['failed'] == 0
                provider_checks[crate] = counts
                if crate == 'vortex-array':
                    methods = re.findall(r'#\[test\]\s*fn\s+(\w+)\s*\(', source_raw(PROVIDER_TEST_SOURCE).decode())
                    assert len(methods) == 3
                    for method in methods:
                        assert re.search(rb'::' + re.escape(method.encode()) + rb' \.\.\. ok$', provider_log, re.MULTILINE)
    core = read('current/' + report['python_core_receipt'])
    assert core['status'] == 'passed' and core['failure_count'] == core['error_count'] == 0
    assert core['shard_id'] == 'core' and core['test_count'] > 0
    assert {'python.tests.' + module for module in MODULES} <= set(core['modules'])
    count = re.search(rb'Ran (\d+) tests? in [0-9.]+s', log('python_core'))
    assert count
    assert report['python_core'] == dict(discovered_test_cases=core['test_count'], reported_test_runs=int(count[1]),
        skip_records=core['skipped_count'], failures=0, errors=0)

    installed = read('current/' + report['installed_report'])
    installed_root = str(PurePosixPath('current/' + report['installed_report']).parent)
    assert installed['status'] == installed['uninstall_status'] == 'passed'
    assert installed['installed_load']['isolated_install']
    assert installed['integration'] == report['installed_package'] == read(installed_root + '/integration.json')
    assert installed['integration']['status'] == 'passed'
    assert all(installed['integration'][key] == 0 for key in ('failures', 'errors', 'skipped'))
    assert not installed['publication_attempted'] and not installed['performance_claim']
    for step in installed['steps']:
        assert step['returncode'] == 0 and digest(data[installed_root + '/' + step['log']]) == step['log_sha256']
    integration = data[installed_root + '/integration.log']
    count = re.search(rb'Ran (\d+) tests? in [0-9.]+s', integration)
    assert count and int(count[1]) == installed['integration']['tests']
    assert b'\nOK\n' in integration and b'... skipped' not in integration
    expected_installed = sorted(name for module in MODULES
        for name in test_methods(source_raw('python/tests/' + module + '.py'), module))
    assert successful_methods(integration, MODULES) == expected_installed
    assert successful_methods(integration, ['test_public_context']) == test_methods(
        source_raw('python/tests/test_public_context.py'), 'test_public_context')
    proof = source_raw('scripts/prove_native_python_wheel.py')
    assert b'"test_native*binding.py"' in proof and b'"test_public_context.py"' in proof
    assert b'"test_wide_schema.py"' in proof
    frozen = read('current/' + report['runtime_identity'])
    assert frozen['status'] == 'passed' and frozen['build_receipt'] == roles['build']
    assert frozen['build_receipt_sha256'] == digest(data['current/' + roles['build']])
    runtime_paths = {Path(row['path']).name: row['path'] for row in frozen['files']}
    assert len(frozen['files']) == len(runtime_paths) == 2
    public_libraries = [argument.split('=', 1)[1] for argument in receipts['public']['command']
                        if argument.startswith('SHARDLOOM_NATIVE_TEST_LIBRARY=')]
    assert public_libraries == [runtime_paths['lib_native.dylib']]
    installed_command = receipts['installed']['command']
    assert installed_command[installed_command.index('--native-library') + 1] == runtime_paths['lib_native.dylib']
    assert installed_command[installed_command.index('--cli-binary') + 1] == runtime_paths['shardloom']
    freeze_command = receipts['freeze']['command']
    assert freeze_command[freeze_command.index('--build') + 1] == roles['build']
    assert all(Path(path).parent.name == freeze_command[freeze_command.index('--name') + 1]
               for path in runtime_paths.values())
    assert {Path(row['path']).name: row['sha256'] for row in frozen['files']} == {
        'shardloom': installed['integrity']['cli_sha256'], 'lib_native.dylib': installed['integrity']['native_library_sha256']}
    assert report['runtime_and_wheel'] == {key: installed['integrity'][key]
        for key in ('cli_sha256', 'native_library_sha256', 'wheel_sha256')}
    wheel = read('current/wheel-source-identity.json')
    assert wheel['status'] == 'passed' and wheel['wheel_sha256'] == report['runtime_and_wheel']['wheel_sha256']
    assert wheel['python_sources'] == {name: info for name, info in source.items()
        if name.startswith('python/src/shardloom/') and name.endswith('.py')}
    failures = []
    for name, raw in sorted(data.items()):
        if not (name.startswith('current/') and name.endswith('.json') and name.count('/') <= 2):
            continue
        try:
            value = json.loads(raw)
        except (ValueError, UnicodeDecodeError):
            continue
        if isinstance(value, dict) and (str(value.get('status')).startswith('failed') or value.get('status') == 'interrupted'):
            failures.append(dict(receipt=name, identity=identity(raw)))
    assert failures == [{key: row[key] for key in ('receipt', 'identity')} for row in report['original_failures']]
    assert all(row['disposition'] for row in report['original_failures'])
    assert {'current/' + name for name in REQUIRED_HISTORICAL_FAILURES} <= {row['receipt'] for row in failures}
    closeout = read('prior/vector-closeout-r1.json')
    assert closeout['status'] == 'complete' and closeout['merge_commit'] == BASE
    assert closeout['pr_checks_passed'] == 58 and closeout['main_checks_passed'] == 55
    assert closeout['portable_replay_after_retirement'] == 'passed' and closeout['current_inputs_unchanged']
    assert report['prior_included_receipts'] == list(PRIOR_RECEIPTS)
    for name in PRIOR_RECEIPTS:
        assert identity(data['prior/' + name]) == closeout['receipts'][name]
    assert all(receipt['prior_retained_runtime_and_wheel'] == closeout['retained_runtime_and_wheel']
               for receipt in receipts.values())
    for receipt_name, raw_name, key, count, head in (
        ('vector-main1671-r1.json', 'vector-main1671-r1-raw/checks.json', 'required_hosted_check_names', 55, BASE),
        ('pr1671-final-checks-r1.json', 'pr1671-final-raw-r1/checks.json', 'required_check_names', 58, closeout['pr_head']),
    ):
        hosted, raw = read('prior/' + receipt_name), read('prior/' + raw_name)
        assert hosted['status'] == 'passed' and hosted['successful_hosted_checks'] == count
        assert len(hosted[key]) == len(set(hosted[key])) == count
        for name in hosted[key]:
            matching = [row for row in raw['check_runs'] if row['name'] == name]
            assert len(matching) == 1 and matching[0]['head_sha'] == head
            assert matching[0]['status'] == 'completed' and matching[0]['conclusion'] == 'success'
        for name, info in hosted['raw_sources'].items():
            assert digest(data['prior/' + name]) == info
    review = read('prior/pr1671-final-review-r1.json')
    assert review['status'] == 'passed' and review['head'] == closeout['pr_head'] and review['unresolved_threads'] == 0
    assert not review['automated_review']['has_any_findings'] and not review['automated_review']['has_unresolved_findings']
    retired = read('prior/vector-retirement-r2/receipt.json')
    assert retired['status'] == 'passed' and retired['retained_files_unchanged'] and retired['input_generations_unchanged']
    assert retired['portable_replay_after_retirement'] == 'passed'
    assert not retired['storage_limits_changed'] and not retired['payload_backup_created']
    assert digest(data['prior/vector-retirement-r2/manifest.json.xz']) == retired['manifest_sha256']
    for name in sorted(data):
        if not re.fullmatch(r'current/debug-retirement-r\d+/receipt\.json', name):
            continue
        directory = name.removesuffix('/receipt.json')
        cleanup, plan = read(name), read(directory + '/plan.json')
        manifest_raw = data[directory + '/manifest.json.xz']
        assert cleanup['status'] == 'passed' and plan['status'] == 'planned_no_deletion'
        assert digest(manifest_raw) == cleanup['manifest_sha256'] == plan['manifest_sha256']
        cleanup_scripts = {
            'current/debug-retirement-r1': ('retire_wide_debug.py', {'native-clippy-r3.json', 'native-tests-r2.json'}),
            'current/debug-retirement-r2': ('retire_wide_debug_r2.py', {'cold-file-native-r2.json', 'provider-arrays-r3.json',
                'provider-io-r2.json', 'release-build-r4.json', 'public-r4.json'}),
            'current/debug-retirement-r3': ('retire_wide_debug_r3.py', {'native-clippy-r4.json', 'native-tests-r3.json',
                'format-r2.json', 'default-clippy-r2.json', 'default-tests-r2.json', 'wheel-runner-tests-r3.json'}),
        }
        cleanup_script, cleanup_checks = cleanup_scripts[directory]
        assert digest(data['current/tools/' + cleanup_script]) == plan['script_sha256']
        assert digest(data['current/source-blobs/' + plan['helper_sha256']]) == plan['helper_sha256']
        manifest = json.loads(lzma.decompress(manifest_raw))
        assert manifest['root'] == plan['root'] == '/Users/dylan/.cache/shardloom/cargo-target/debug'
        root = PurePosixPath(manifest['root'])
        entries = manifest['entries']
        assert len(entries) == plan['entries'] == cleanup['removed']
        assert all(PurePosixPath(row['path']).is_relative_to(root) for row in entries)
        assert plan['regular_files'] == cleanup['regular_files'] == sum(row['kind'] == 'regular' for row in entries)
        assert plan['regular_bytes'] == cleanup['regular_bytes'] == sum(row['generation'][3] for row in entries if row['kind'] == 'regular')
        assert manifest['source_head'] == HEAD
        # Bind the exact completed checks used for each retirement, not future
        # gate names or a retrospective claim that all final checks had run.
        assert set(manifest['native_checks']) == cleanup_checks
        for filename, sha in manifest['native_checks'].items():
            assert digest(data['current/' + filename]) == sha
            original_check = read('current/' + filename)
            assert original_check['status'] == 'passed' and original_check['returncode'] == 0
            assert original_check['process_group_drained'] and original_check['fixed_source_validation']
        assert all(manifest['retained'][path] == info for path, info in closeout['retained_runtime_and_wheel'].items())
        assert cleanup['retained_runtimes_and_wheel_unchanged'] and cleanup['input_generations_unchanged']
        assert cleanup['release_cache_unchanged'] and not cleanup['storage_floor_changed'] and not cleanup['payload_backup_created']
    assert report['limitations'] == LIMITATIONS and report['hosted_integration'] == 'pending'
    assert not report['performance_claim'] and not report['publication_attempted'] and not report['whole_item_19_complete']
    controls_path = packet / 'corruption-controls.json'
    if require_controls:
        assert controls_path.is_file(), 'saved corruption controls are required'
    controls_verified = False
    if controls_path.exists():
        controls = json.loads(controls_path.read_bytes())
        assert controls['status'] == 'passed'
        saved = controls['receipt']
        assert saved['status'] == 'passed' and saved['positive_controls'] == 2 and saved['rejected_controls'] == 17
        assert saved['driver_sha256'] == digest(data['current/tools/check_wide_schema_controls.py'])
        assert controls['primary_sealer_sha256'] == digest(data['current/tools/seal_wide_schema_controls.py'])
        expected_packet = {name: identity((packet / name).read_bytes())
                           for name in ('acceptance.json', 'verification.tar.xz', 'verify.py')}
        assert saved['original_packet'] == expected_packet
        tree = ast.parse(data['current/tools/check_wide_schema_controls.py'])
        cases = next(ast.literal_eval(node.value) for node in tree.body if isinstance(node, ast.Assign)
                     and any(isinstance(target, ast.Name) and target.id == 'CASES' for target in node.targets))
        assert len(cases) == len(set(cases)) == 19 and cases[:2] == ('original', 'relocated_source_only')
        assert [case['case'] for case in saved['cases']] == list(cases)
        assert set(controls['logs']) == {case['log'] for case in saved['cases']}
        for index, case in enumerate(saved['cases']):
            raw = controls['logs'][case['log']].encode()
            assert identity(raw) == case['log_identity']
            assert case['returncode'] == (0 if index < 2 else 1)
            assert case['expected'] == ('accepted' if index < 2 else 'rejected')
            assert case['original_packet_unchanged'] and case['report_before'] == expected_packet['acceptance.json']
            if index < 2:
                observed = json.loads(raw)
                assert observed['status'] == 'passed' and not observed['native_execution_repeated']
                assert not observed['repository_checked'] and case['tested_packet'] == expected_packet
            else:
                assert b'AssertionError' in raw or b'KeyError' in raw
        controls_verified = True
    return dict(status='passed', members=len(members), roles=len(roles), public_tests=len(report['public_tests']),
        native_tests=len(required_native), provider_tests=provider_checks, installed_tests=installed['integration']['tests'],
        original_failures=len(failures), source_fingerprint_sha256=fingerprint(source),
        native_execution_repeated=False, repository_checked=repo_root is not None,
        controls_verified=controls_verified)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packet', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--repo-root', type=Path)
    parser.add_argument('--require-controls', action='store_true')
    args = parser.parse_args()
    print(json.dumps(verify(args.packet, args.repo_root, args.require_controls), sort_keys=True))
