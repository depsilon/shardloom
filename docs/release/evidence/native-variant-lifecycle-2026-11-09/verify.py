"""Replay saved Variant lifecycle acceptance without a checkout or native execution."""
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
HEAD = 'e85e379588d8ea74a6ce6ee72e3e407ad15a3b49'
NATIVE = 'shardloom-python/python-native,shardloom-vortex/native-encoded-exchange-experiment'
CARGO = {
    'format': ['cargo', 'fmt', '--all', '--', '--check'],
    'default_clippy': ['cargo', 'clippy', '--offline', '--locked', '--workspace', '--all-targets', '--', '-D', 'warnings'],
    'native_clippy': ['cargo', 'clippy', '--offline', '--locked', '--workspace', '--all-targets', '--features', NATIVE, '--', '-D', 'warnings'],
    'default_tests': ['cargo', 'test', '--offline', '--locked', '--workspace', '--all-targets'],
    'native_tests': ['cargo', 'test', '--offline', '--locked', '--workspace', '--all-targets', '--features', NATIVE],
    'doc_contracts': ['cargo', 'test', '--offline', '--locked', '-p', 'shardloom-contract-tests', '--all-targets'],
    'build': ['cargo', 'build', '--offline', '--locked', '--release', '--features', 'release-user-surfaces', '-p', 'shardloom-cli', '-p', 'shardloom-python'],
}
RUNNERS = {
    'features': 'check_variant_features.py', 'freeze': 'freeze_runtime.py',
    'python_core': 'scripts/run_python_test_shard.py',
    'docs': 'check_variant_docs.py', 'installed': 'scripts/prove_native_python_wheel.py',
    'msrv': 'check_variant_msrv.py', 'provider_arrow': 'check_arrow_provider.py',
}
ROLES = set(CARGO) | set(RUNNERS) | {'public'}
MODULES = ('test_native_variant_binding', 'test_native_result_schema')
NATIVE_MODULES = MODULES[:1]
NATIVE_TEST_SOURCES = (
    'shardloom-vortex/src/native_variant.rs',
    'shardloom-vortex/src/native_variant_arrow.rs',
    'shardloom-vortex/src/local_primitives/native_payload_variant_tests.rs',
    'shardloom-vortex/src/resident_variant_lifecycle_tests.rs',
)
EXTRA_NATIVE_TESTS = {
    'shardloom-vortex/src/local_primitive_native_writer_tests.rs': (
        'native_writer_pins_frozen_core_edition_and_rejects_opt_in_buffer_encoding',),
    'shardloom-vortex/src/resident_result_arrow_tests.rs': (
        'native_variant_arrow_result_preserves_identity_encoded_values_and_final_child_owner',
        'native_variant_shredded_objects_keep_raw_residuals_and_floating_bits',
        'native_variant_dictionary_arrow_import_preserves_extension_nulls_and_order',),
    'shardloom-vortex/src/local_primitive_unary_melt_tests.rs': (
        'unary_melt_preserves_mixed_value_types_nulls_order_and_native_file_payload',
        'unary_melt_text_writers_consume_native_variants_and_preserve_complete_order',
        'unary_melt_preserves_uniform_schema_limits_and_typed_empty_variant_output',),
    'shardloom-vortex/src/local_primitive_unary_pivot_tests.rs': (
        'unary_pivot_fill_margins_and_limit_preserve_types_and_selected_margin_scope',),
    'shardloom-cli/src/sql_native_relational_unary_tests.rs': (
        'native_relational_sql_unary_mixed_melt_does_not_widen_scalar_admission',),
}
PROVIDER_TEST_SOURCE = 'vendor/vortex-arrow/src/variant_chunks.rs'
# Exact status documents may be finalized after compiled behavior checks.
STATUS_PROSE = {
    'docs/architecture/phased-execution-plan.md',
    'docs/architecture/native-variant-lifecycle-2026-11-09.md',
    'docs/architecture/native-rich-data-lifecycle-2026-10-24.md',
    'docs/architecture/local-development-storage.md',
}
GENERATED_REFERENCE_DOCS = {
    'docs/reference/shardloom-user-surface-index.json',
    'docs/reference/shardloom-user-surface-index.md',
}
GENERATED_REFERENCE_INPUTS = (
    'Cargo.lock', 'Cargo.toml', 'python/src/shardloom/_result_schema.py',
    'shardloom-vortex/Cargo.toml',
)
PYTHON_TEST_ONLY = {'python/tests/' + name + '.py' for name in MODULES}
# Rust-only gates do not compile the Python result converter. Its final bytes
# are required by fresh public/core/installed gates and the wheel-source binder.
CARGO_INDEPENDENT_PYTHON = PYTHON_TEST_ONLY | {'python/src/shardloom/_result_schema.py'}
FROZEN = {
    'python/tests/test_native_variant_binding.py': '2820a4ef6498d8b02034d37b813e246fa96d3a32b741020ae7701fcc067540a2',
}
EXCLUDED = ('docs/benchmarks/evidence/', 'docs/release/evidence/', 'docs/dependencies/evidence/')
REQUIRED_HISTORICAL_FAILURES = {
    'arrow-provider-r1.json', 'arrow-provider-r2.json', 'native-clippy-r1.json', 'native-clippy-r3.json',
    'native-tests-r1.json', 'native-tests-r2.json', 'native-tests-r3.json', 'native-tests-r4.json',
    'native-tests-r5.json', 'legacy-python-r1.json',
    'python-core-r1.json', 'python-core-r1/core.json',
    'provenance-refresh-original-failure.json',
    'public-r1.json', 'public-r3.json', 'public-r6.json', 'variant-compile-r1.json', 'variant-compile-r2.json',
    'variant-focus-r3.json', 'variant-focus-r5.json', 'variant-focus-r6.json',
    'variant-focus-r8.json', 'variant-focus-r9.json', 'variant-focus-r11.json',
    'package-r1.json',
    'controls-r1.json',
}
LIMITATIONS = [
    'Local finite correctness, typed persistence and ownership acceptance only; no performance comparison.',
    'Variant payload carriage does not enable Variant keys, field expressions, general JSON conversion, row-literal schema hints or unsupported physical shredding.',
    'Logical/raw/shredded depth remains 24 and storage wrappers remain bounded at 64; deep traversal, UUID/spatial and Variant expression obligations remain open.',
    'Saved failures and interrupted builds remain evidence. Default/native opt-in ignores and Python optional/environment skips remain explicit; selected public and installed Variant tests require zero skips.',
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


def embedded_evidence(data):
    """Identify earlier evidence payloads outside every build/tool input graph."""
    required, evidence = set(), {}
    for name, raw in data.items():
        if not (name.startswith('current/') and name.count('/') == 1 and name.endswith('.json')):
            continue
        value = json.loads(raw)
        if not isinstance(value, dict):
            continue
        for key in ('build_sources', 'tool_source_inventory', 'changed_source_inventory'):
            for path, info in value.get(key, {}).items():
                if 'sha256' not in info or 'symlink' in info:
                    continue
                sha = info['sha256']
                if key == 'changed_source_inventory' and path.startswith(EXCLUDED):
                    row = evidence.setdefault(sha, dict(bytes=info['bytes'], sha256=sha, paths=set()))
                    assert row['bytes'] == info['bytes']
                    row['paths'].add(path)
                else:
                    required.add(sha)
    return [dict(bytes=row['bytes'], sha256=sha, paths=sorted(row['paths']))
            for sha, row in sorted(evidence.items()) if sha not in required]


def verify(packet, repo_root=None, require_controls=False):
    report = json.loads((packet / 'acceptance.json').read_bytes())
    assert report['schema_version'] == 'shardloom.variant_lifecycle_acceptance.v1'
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
    assert Path(__file__).read_bytes() == data['current/tools/verify_variant_packet.py']
    omitted = embedded_evidence(data)
    assert report['omitted_embedded_evidence'] == omitted
    assert all('current/source-blobs/' + row['sha256'] not in data for row in omitted)

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
        assert result['driver_sha256'] == digest(data['current/tools/guard_variant.py'])
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
        if role not in {'docs', 'doc_contracts', 'python_core'}:
            allowed = allowed | GENERATED_REFERENCE_DOCS
            if role != 'provider_arrow':
                original_core = read('current/python-core-r1.json')
                for path in set(deltas) & GENERATED_REFERENCE_DOCS:
                    assert result['build_sources'][path] == original_core['build_sources'][path]
        if role in (set(CARGO) - {'doc_contracts'}) | {'features', 'msrv', 'freeze'}:
            allowed = allowed | CARGO_INDEPENDENT_PYTHON
        if role == 'provider_arrow':
            # This standalone provider gate links only its recorded vendor
            # graph; later engine/Python edits do not affect that graph.
            provider_inputs = {name for name in source if name.startswith('vendor/')} | {
                '.github/provider-locks/vortex-arrow.lock', 'scripts/ci_provider_tests.py'}
            provenance = 'vendor/vortex-arrow/upstream-provenance.json'
            assert set(deltas) & provider_inputs <= {provenance}
            if provenance in deltas:
                # The provider was tested after its source/README changes but
                # before their three recorded provenance hashes were refreshed.
                # Admit only that exact bookkeeping update, never a vendor code,
                # lockfile, runner or other provenance-field change.
                previous = json.loads(blob(result['build_sources'][provenance]))
                current = json.loads(source_raw(provenance))
                assert previous['local_changes_documentation']['path'] == 'README.shardloom.md'
                for name in ('README.shardloom.md', 'src/variant_chunks.rs'):
                    sha = digest(source_raw('vendor/vortex-arrow/' + name))
                    previous['added_files_sha256'][name] = sha
                    if name == 'README.shardloom.md':
                        previous['local_changes_documentation']['sha256'] = sha
                assert previous == current
            allowed = allowed | set(deltas)
        assert set(deltas) <= allowed, (role, set(deltas) - allowed)
        assert report['source_deltas'][role] == deltas
        if role in CARGO:
            assert result['command'][result['command'].index('cargo'):] == CARGO[role]
            if role == 'native_tests':
                assert result['command'][:2] == ['env', 'RUST_TEST_THREADS=4']
        elif role in RUNNERS:
            runner = RUNNERS[role]
            matches = [arg for arg in result['command'] if arg.endswith(runner)]
            assert len(matches) == 1, role
            recorded = [(name, info) for name, info in result['tool_source_inventory'].items()
                        if name.endswith('/' + runner)]
            assert len(recorded) == 1, role
            expected = (source_raw(runner) if runner.startswith(('scripts/', 'python/'))
                else data['current/tools/' + runner])
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
        methods = re.findall(r'\bfn\s+(native_variant_\w+)\s*\(', source_raw(name).decode())
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
    for name, sha in FROZEN.items():
        assert digest(source_raw(name)) == sha, name
    assert report['frozen_references'] == FROZEN
    correction = read('current/public-query-correction-r1.json')
    assert correction['status'] == 'source_correction_verified'
    assert correction['original_failure'] == 'public-r6.json'
    assert correction['completed_public_receipt'] == roles['public']
    fixture = 'python/tests/test_native_variant_binding.py'
    assert correction['source'] == fixture
    assert correction['before_sha256'] == '29626622029de4afafeab114046c5351818875ac9e115a561cf6d5558bca717e'
    assert correction['after_sha256'] == FROZEN[fixture]
    failed_public = read('current/public-r6.json')
    assert failed_public['status'] == 'failed' and failed_public['returncode'] == 1
    assert failed_public['process_group_drained'] and failed_public['direct_child_reaped']
    before = blob(failed_public['build_sources'][fixture])
    old_query = b'SELECT l.id FROM data l JOIN data r ON l.payload = r.payload'
    new_query = b'SELECT l.id FROM data AS l JOIN data AS r ON l.payload = r.payload'
    assert digest(before) == correction['before_sha256'] and before.count(old_query) == 1
    assert correction['before_query'].encode() == old_query and correction['after_query'].encode() == new_query
    assert before.replace(old_query, new_query) == source_raw(fixture)
    assert b'JOIN and derived sources require <source> AS <alias> syntax' in source_raw(correction['parser_source'])
    assert b'FAILED (failures=2)' in data['current/public-r6.log']
    refresh = read('current/public-reference-refresh-r1.json')
    assert refresh['status'] == 'generated_documentation_refreshed'
    assert refresh['original_failure'] == 'python-core-r1.json'
    assert refresh['original_core_report'] == 'python-core-r1/core.json'
    original_core = read('current/python-core-r1.json')
    original_report = read('current/python-core-r1/core.json')
    assert original_core['status'] == original_report['status'] == 'failed'
    assert original_core['returncode'] == 1 and original_core['process_group_drained']
    assert original_core['direct_child_reaped'] and original_report['failure_count'] == 3
    assert original_report['error_count'] == 0
    assert set(refresh['changes']) == GENERATED_REFERENCE_DOCS
    assert refresh['refreshed_source_inputs'] == list(GENERATED_REFERENCE_INPUTS)
    for path, change in refresh['changes'].items():
        assert change['before'] == original_core['build_sources'][path]
        assert change['after'] == source[path]
    index = 'docs/reference/shardloom-user-surface-index.json'
    previous_index = json.loads(blob(original_core['build_sources'][index]))
    previous_identity = previous_index['application']['source_identity']
    for path in GENERATED_REFERENCE_INPUTS:
        previous_index['application']['source_inputs'][path] = digest(source_raw(path))
    inputs = previous_index['application']['source_inputs']
    current_identity = digest(json.dumps(inputs, sort_keys=True).encode())
    previous_index['application']['source_identity'] = current_identity
    assert (json.dumps(previous_index, indent=2, ensure_ascii=False) + '\n').encode() == source_raw(index)
    markdown = 'docs/reference/shardloom-user-surface-index.md'
    previous_markdown = blob(original_core['build_sources'][markdown])
    assert previous_markdown.count(previous_identity.encode()) == 1
    assert previous_markdown.replace(previous_identity.encode(), current_identity.encode()) == source_raw(markdown)
    expected_public = sorted(name for module in MODULES
        for name in test_methods(source_raw('python/tests/' + module + '.py'), module))
    assert report['public_tests'] == expected_public
    assert successful_methods(log('public'), MODULES) == expected_public
    assert len(expected_public) == 15 and b'\nOK\n' in log('public') and b'... skipped' not in log('public')
    for role, filename, expected_count in (
        ('features', 'check_variant_features.py', 7), ('msrv', 'check_variant_msrv.py', 2)):
        tree = ast.parse(data['current/tools/' + filename])
        commands = next(ast.literal_eval(node.value) for node in tree.body if isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == 'commands' for target in node.targets))
        assert len(commands) == expected_count
        assert [line.removeprefix('Checking ') for line in log(role).decode().splitlines() if line.startswith('Checking ')] == [
            ' '.join(command + ['--locked', '--offline']) for command in commands]
    provider_tree = ast.parse(source_raw('scripts/ci_provider_tests.py'))
    configurations = next(ast.literal_eval(node.value) for node in provider_tree.body if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == 'CONFIGURATIONS' for target in node.targets))
    config = configurations['vortex-arrow']
    expected_commands = []
    for operation in ('test', 'clippy'):
        command = ['cargo', operation, '--offline', '--locked', '--manifest-path', 'vendor/vortex-arrow/Cargo.toml']
        for patch in config['patches']:
            command += ['--config', f'patch.crates-io.{patch}.path="vendor/{patch}"']
        command += ['--features', config['features'], '--lib']
        command += ['--', '--test-threads=2'] if operation == 'test' else ['--tests', '--', '-D', 'warnings']
        expected_commands.append(command)
    recorded_commands = [ast.literal_eval(line) for line in log('provider_arrow').decode().splitlines()
        if line.startswith("['cargo', ")]
    assert recorded_commands == expected_commands
    provider_counts = rust_counts(log('provider_arrow'))
    assert provider_counts and provider_counts['failed'] == 0 and provider_counts['passed'] >= 408
    provider_methods = re.findall(r'#\[test\]\s*fn\s+(\w+)\s*\(', source_raw(PROVIDER_TEST_SOURCE).decode())
    assert len(provider_methods) == 2
    for method in provider_methods:
        assert re.search(rb'::' + re.escape(method.encode()) + rb' \.\.\. ok$', log('provider_arrow'), re.MULTILINE), method
    provider_checks = {'vortex-arrow': provider_counts}
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
    assert all(pattern in proof for pattern in (
        b'"test_native*binding.py"', b'"test_public_context.py"', b'"test_native_result_schema.py"'))
    frozen = read('current/' + report['runtime_identity'])
    assert frozen['status'] == 'passed' and frozen['build_receipt'] == roles['build']
    assert frozen['build_receipt_sha256'] == digest(data['current/' + roles['build']])
    runtime_paths = {Path(row['path']).name: row['path'] for row in frozen['files']}
    assert len(frozen['files']) == len(runtime_paths) == 2
    assert refresh['binary'] == runtime_paths['shardloom']
    assert refresh['generator'] == 'scripts/write_public_api_reference.py'
    assert refresh['command'][-3:] == [refresh['generator'], '--binary', runtime_paths['shardloom']]
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
        cleanup_scripts = [raw for path, raw in data.items()
            if path.startswith('current/tools/retire_variant_debug') and digest(raw) == plan['script_sha256']]
        assert len(cleanup_scripts) == 1
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
        for filename, sha in manifest['native_checks'].items():
            assert digest(data['current/' + filename]) == sha
            original_check = read('current/' + filename)
            if directory == 'current/debug-retirement-r1':
                assert filename == 'native-tests-r2.json'
                assert original_check['status'] == 'failed' and original_check['returncode'] == -15
                assert original_check['error'] == 'AssertionError: minimum free storage reached'
            else:
                assert original_check['status'] == 'passed' and original_check['returncode'] == 0
            assert original_check['process_group_drained'] and original_check['fixed_source_validation']
        assert all(manifest['retained'][path] == info for path, info in closeout['retained_runtime_and_wheel'].items())
        assert cleanup['retained_runtimes_and_wheel_unchanged'] and cleanup['input_generations_unchanged']
        assert cleanup['release_cache_unchanged'] and not cleanup['storage_floor_changed'] and not cleanup['payload_backup_created']
    directory = 'current/temporary-retirement-r1'
    cleanup, plan = read(directory + '/receipt.json'), read(directory + '/plan.json')
    raw = data[directory + '/manifest.json.xz']
    assert cleanup['status'] == 'passed' and plan['status'] == 'planned_no_deletion'
    assert digest(raw) == cleanup['manifest_sha256'] == plan['manifest_sha256']
    assert digest(data['current/tools/retire_variant_temporary_r1.py']) == plan['script_sha256']
    assert digest(data['current/tools/retire_variant_debug_r1.py']) == plan['helper_sha256']
    manifest = json.loads(lzma.decompress(raw))
    roots = [PurePosixPath(name) for name in manifest['roots']]
    assert manifest['roots'] == plan['roots'] and len(roots) == len(set(roots)) == 6
    assert len({root.parent for root in roots}) == 1
    assert all(str(root).startswith('/private/var/folders/') and re.fullmatch(
        r'shardloom-(?:pivot|grid|calendar)-pressure-57363-\d+', root.name) for root in roots)
    entries = manifest['entries']
    assert len(entries) == plan['entries'] == cleanup['removed']
    assert all(any(PurePosixPath(row['path']).is_relative_to(root) for root in roots) for row in entries)
    assert all(row['kind'] in {'regular', 'directory'} for row in entries)
    assert plan['regular_files'] == cleanup['regular_files'] == sum(row['kind'] == 'regular' for row in entries)
    assert plan['regular_bytes'] == cleanup['regular_bytes'] == sum(
        row['generation'][3] for row in entries if row['kind'] == 'regular')
    failed_name = 'current/' + manifest['native_receipt']
    assert manifest['native_receipt'] == 'native-tests-r5.json'
    assert digest(data[failed_name]) == manifest['native_receipt_sha256']
    interrupted = read(failed_name)
    assert interrupted['status'] == 'failed' and interrupted['returncode'] == -15
    assert interrupted['error'] == 'AssertionError: minimum free storage reached'
    assert interrupted['process_group_drained'] and interrupted['direct_child_reaped']
    assert plan['regeneration_source_identities'] == {
        name: interrupted['build_sources'][name] for name in plan['regeneration_sources']}
    assert all(manifest['retained'][path] == info for path, info in closeout['retained_runtime_and_wheel'].items())
    assert cleanup['retained_runtimes_and_wheel_unchanged'] and cleanup['input_generations_unchanged']
    assert cleanup['release_cache_unchanged'] and not cleanup['storage_limits_changed'] and not cleanup['payload_backup_created']
    merged = read('wide/main-completed-observation-r1.json')
    assert merged['status'] == 'passed' and merged['commit'] == HEAD
    required_main = read('prior/vector-main1671-r1.json')['required_hosted_check_names']
    assert len(merged['check_runs']) == 55 and sorted(row['name'] for row in merged['check_runs']) == sorted(required_main)
    assert all(row['head_sha'] == HEAD and row['status'] == 'completed' and row['conclusion'] == 'success'
        for row in merged['check_runs'])
    assert len(merged['jobs']) == 54 and all(row['conclusion'] == 'success' and row['status'] == 'completed'
        for row in merged['jobs'])
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
        assert saved['status'] == 'passed' and saved['positive_controls'] == 2 and saved['rejected_controls'] == 18
        assert saved['driver_sha256'] == digest(data['current/tools/check_variant_controls.py'])
        assert controls['primary_sealer_sha256'] == digest(data['current/tools/seal_variant_controls.py'])
        expected_packet = {name: identity((packet / name).read_bytes())
                           for name in ('acceptance.json', 'verification.tar.xz', 'verify.py')}
        assert saved['original_packet'] == expected_packet
        tree = ast.parse(data['current/tools/check_variant_controls.py'])
        cases = next(ast.literal_eval(node.value) for node in tree.body if isinstance(node, ast.Assign)
                     and any(isinstance(target, ast.Name) and target.id == 'CASES' for target in node.targets))
        assert len(cases) == len(set(cases)) == 20 and cases[:2] == ('original', 'relocated_source_only')
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
                if case['case'] == 'prior_main_failed':
                    assert b"matching[0]['conclusion'] == 'success'" in raw
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
