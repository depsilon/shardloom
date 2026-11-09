"""Replay saved shared-vector acceptance without a checkout or native execution."""
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

BASE = 'd29bff42c5b3aacf7a61463bf2c8f9cec2bdf216'
NATIVE = 'shardloom-python/python-native,shardloom-vortex/native-encoded-exchange-experiment'
CARGO = {
    'format': ['cargo', 'fmt', '--all', '--', '--check'],
    'default_clippy': ['cargo', 'clippy', '--workspace', '--all-targets', '--locked', '--offline', '--', '-D', 'warnings'],
    'native_clippy': ['cargo', 'clippy', '--workspace', '--all-targets', '--features', NATIVE, '--locked', '--offline', '--', '-D', 'warnings'],
    'default_tests': ['cargo', 'test', '--workspace', '--all-targets', '--locked', '--offline'],
    'native_tests': ['cargo', 'test', '--workspace', '--all-targets', '--features', NATIVE, '--locked', '--offline'],
    'doc_contracts': ['cargo', 'test', '-p', 'shardloom-contract-tests', '--all-targets', '--locked', '--offline'],
    'build': ['cargo', 'build', '--locked', '--offline', '--release', '--features', 'release-user-surfaces', '-p', 'shardloom-cli', '-p', 'shardloom-python'],
}
RUNNERS = {
    'features': 'check_vector_features.py', 'freeze': 'freeze_runtime.py',
    'python_core': 'scripts/run_python_test_shard.py', 'example': 'run_vector_doc_examples.py',
    'docs': 'check_vector_docs.py', 'installed': 'scripts/prove_native_python_wheel.py',
    'msrv': 'check_vector_msrv.py',
}
ROLES = set(CARGO) | set(RUNNERS) | {'public', 'msrv'}
MODULES = (
    'test_native_vector_expression_binding',
    'test_native_vector_expression_semantics_binding',
    'test_native_vector_expression_workflow_binding',
    'test_native_function_catalog', 'test_vector_expression_declarations',
)
NATIVE_MODULES = MODULES[:3]
NATIVE_TEST_SOURCES = (
    'shardloom-vortex/src/local_primitive_relational_literal_ownership_tests.rs',
    'shardloom-vortex/src/local_primitive_relational_vector_delivery_tests.rs',
    'shardloom-vortex/src/local_primitive_relational_vector_expression_tests.rs',
    'shardloom-vortex/src/local_primitive_relational_vector_inspector_tests.rs',
    'shardloom-vortex/src/local_primitive_relational_vector_reference_tests.rs',
    'shardloom-vortex/src/local_primitive_relational_vector_resource_tests.rs',
    'shardloom-vortex/src/local_primitives/native_vector_math_tests.rs',
)
# These are exact, reviewable exceptions, not a filename-extension filter.
# Cargo gates do not run the separately discovered Python unittest modules.
# Status prose may be completed after runtime validation; final docs validation
# and the complete contract-test crate must see it. Every before/after identity
# remains in the original receipts.
STATUS_PROSE = {
    'docs/architecture/phased-execution-plan.md',
    'docs/architecture/native-vector-expressions-2026-11-08.md',
    'docs/architecture/local-development-storage.md',
}
PYTHON_TEST_ONLY = {'python/tests/' + name + '.py' for name in MODULES[:3]}
FROZEN = {
    'scripts/native_vector_reference.py': '8c9fc3b53792a5dd5de7541dbf3277a2f543907b7eb85b37d101b082e5af7be3',
    'scripts/native_vector_rounding_reference.py': '48619742e5233ed0185165afadcd1c1eae2470e0997b814b4b83d46105edec30',
    'shardloom-vortex/tests/fixtures/native_vector_numerical_reference_r2.json': '17ebc40f4497c52323b3bdedb60ffa8fd24c63f5242c5c091f0c75d9839a0260',
    'shardloom-vortex/tests/fixtures/native_vector_rounding_reference.json': 'e28a08ee9c08767571a5a10c58b8a88a5b00e4c81cbebb6bb98332382a979df9',
    'shardloom-vortex/tests/fixtures/native_vector_semantic_reference_r1.json': 'dbfec9726a41c31a627e7d296550f043c064f267ffac3a3680ff5c829a0c104a',
    'shardloom-vortex/tests/fixtures/native_vector_retrieval_reference_r2.json': '77f9a767108deeeed5de421a87c86a3aecf619b1a6fd0bda5662d403b33374ff',
}
EXCLUDED = ('docs/benchmarks/evidence/', 'docs/release/evidence/', 'docs/dependencies/evidence/')
REQUIRED_HISTORICAL_FAILURES = {
    'vector-literal-regression-r1-launch.json', 'vector-literal-regression-r2.json',
    'vector-literal-regression-r3.json', 'vector-focused-r1.json', 'vector-focused-r2.json',
    'vector-focused-r3.json', 'vector-native-clippy-r1.json', 'vector-native-clippy-r2.json',
    'vector-native-clippy-r3.json', 'vector-public-python-r1.json', 'vector-public-python-r2.json',
    'vector-public-python-r3.json', 'vector-delivery-r1.json',
}
LIMITATIONS = [
    'Shared exact arithmetic admits dense floating vectors and dense embeddings only; sparse/quantized arithmetic, densification, dequantization, ANN and implicit multi-vector reduction remain unadmitted.',
    'Strict numeric consumers and sort keys still observe nonfinite values. Raw vector selection policy follows explicit fields through the documented ordinary projection/filter/range/order paths; other operators retain their existing validation contracts.',
    'PyArrow 25.0.1 cannot reopen every nullable fixed-list Parquet parent. Complete native Parquet and independent Arrow readbacks are checked; present-parent Parquet subsets receive independent PyArrow readback.',
    'Native selected-buffer credits do not cover all upstream provider allocations and do not establish an RSS bound, zero-copy, zero-decode or file-I/O saving.',
    'Native installed-wheel proof is local macOS arm64. Hosted PR and exact-main acceptance are a separate closeout.',
    'Saved failures remain evidence; default/native opt-in ignores and Python optional/environment skips remain explicit. Installed-package and selected public-vector acceptance require zero skips.',
    'No benchmark, performance superiority, package publication, whole item 19/item 20, RFC 0044 or CG-1 through CG-23 completion claim.',
]


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
    assert report['schema_version'] == 'shardloom.vector_expressions_acceptance.v1'
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
    assert Path(__file__).read_bytes() == data['current/tools/verify_vector_expression_packet.py']

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
        assert result['fixed_source_validation'] and result['head'] == result['base'] == BASE
        assert result['prior_closeout_sha256'] == digest(data['prior/provider-closeout-r1.json'])
        assert result['driver_sha256'] == digest(data['current/tools/guard_vector_expressions.py'])
        for phase in ('pre_execution', 'post_execution'):
            observed = result['build_source_verification'][phase]
            assert observed['head'] == BASE and observed['head_matches'] and observed['path_set_matches']
            assert observed['changed_paths'] == []
            assert observed['path_count'] == len(result['build_sources'])
            assert observed['fingerprint_sha256'] == fingerprint(result['build_sources'])
        assert result['environment']['CARGO_BUILD_JOBS'] == '2'
        assert result['environment']['CARGO_INCREMENTAL'] == '0'
        deltas = source_deltas(result['build_sources'], source)
        allowed = set() if role in {'docs', 'doc_contracts'} else STATUS_PROSE
        if role in (set(CARGO) - {'doc_contracts'}) | {'features', 'msrv', 'freeze'}:
            allowed = allowed | PYTHON_TEST_ONLY
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
            expected = source_raw(runner) if runner.startswith('scripts/') else data['current/tools/' + runner]
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
    assert len(required_native) == len(set(required_native))
    for name in required_native:
        assert re.search(rb'::' + re.escape(name.encode()) + rb' \.\.\. ok$', log('native_tests'), re.MULTILINE), name
    assert report['required_native_tests'] == sorted(required_native)
    for name, sha in FROZEN.items():
        assert digest(source_raw(name)) == sha, name
    assert report['frozen_references'] == FROZEN
    numerical = json.loads(source_raw('shardloom-vortex/tests/fixtures/native_vector_numerical_reference_r2.json'))
    rounding = json.loads(source_raw('shardloom-vortex/tests/fixtures/native_vector_rounding_reference.json'))
    semantics = json.loads(source_raw('shardloom-vortex/tests/fixtures/native_vector_semantic_reference_r1.json'))
    retrieval = json.loads(source_raw('shardloom-vortex/tests/fixtures/native_vector_retrieval_reference_r2.json'))
    assert numerical['case_count'] == len(numerical['cases']) == 270
    assert rounding['case_count'] == len(rounding['cases']) == 4918
    assert len(semantics['cases']) == 85 and len(semantics['binding_cases']) == 19
    assert semantics['source_contract_sha256'] == digest(data['current/preparation/selected-contract-r1.md'])
    assert retrieval['dimension'] == len(retrieval['query_bits']) == 1536
    crosscheck = read('current/preparation/numerical-reference-crosscheck-r1.json')
    assert crosscheck['status'] == 'passed_independent_reference_crosscheck'
    assert crosscheck['reference'] == identity(source_raw('shardloom-vortex/tests/fixtures/native_vector_numerical_reference_r2.json'))
    assert crosscheck['generator'] == identity(source_raw('scripts/native_vector_reference.py'))
    assert crosscheck['decimal_precision'] == 6144 and crosscheck['decimal_rational_stage_had_no_rounding']
    assert report['public_tests'] == successful_methods(log('public'), MODULES) == sorted(
        name for module in MODULES for name in test_methods(source_raw('python/tests/' + module + '.py'), module))
    assert len(report['public_tests']) == 21 and b'\nOK\n' in log('public') and b'... skipped' not in log('public')
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
    assert b'"python_examples": 2' in log('example') and b'"sql_examples": 1' in log('example')
    assert b'"filtered_order_and_ties": [10, 40, 20, 2]' in log('example')
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
    expected_installed = sorted(name for module in NATIVE_MODULES
        for name in test_methods(source_raw('python/tests/' + module + '.py'), module))
    assert successful_methods(integration, NATIVE_MODULES) == expected_installed
    assert successful_methods(integration, ['test_public_context']) == test_methods(
        source_raw('python/tests/test_public_context.py'), 'test_public_context')
    proof = source_raw('scripts/prove_native_python_wheel.py')
    assert b'"test_native*binding.py"' in proof and b'"test_public_context.py"' in proof
    frozen = read('current/' + report['runtime_identity'])
    assert frozen['status'] == 'passed' and frozen['build_receipt'] == roles['build']
    assert frozen['build_receipt_sha256'] == digest(data['current/' + roles['build']])
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
    cleanup = read('current/debug-retirement-r1/receipt.json')
    cleanup_plan = read('current/debug-retirement-r1/plan.json')
    cleanup_raw = data['current/debug-retirement-r1/manifest.json.xz']
    assert cleanup['status'] == 'passed' and cleanup['regular_files'] > 0
    assert cleanup['retained_runtimes_and_wheel_unchanged'] and cleanup['input_generations_unchanged'] and cleanup['release_cache_unchanged']
    assert not cleanup['storage_floor_changed'] and not cleanup['payload_backup_created']
    assert digest(cleanup_raw) == cleanup['manifest_sha256'] == cleanup_plan['manifest_sha256']
    cleanup_manifest = json.loads(lzma.decompress(cleanup_raw))
    assert cleanup['removed'] == cleanup_plan['entries'] == len(cleanup_manifest['entries'])
    for name, sha in cleanup_manifest['native_checks'].items():
        assert digest(data['current/' + name]) == sha
    assert digest(data['current/tools/retire_vector_debug.py']) == cleanup_plan['script_sha256']
    assert digest(data['current/source-blobs/' + cleanup_plan['helper_sha256']]) == cleanup_plan['helper_sha256']
    closeout = read('prior/provider-closeout-r1.json')
    assert closeout['status'] == 'complete' and closeout['merge_commit'] == BASE
    assert closeout['pr_checks_passed'] == 58 and closeout['main_checks_passed'] == 55
    assert closeout['portable_replay_after_retirement'] == 'passed' and closeout['current_inputs_unchanged']
    for name, info in closeout['receipts'].items():
        assert identity(data['prior/' + name]) == info
    for receipt_name, raw_name, key, count, head in (
        ('provider-main1670-r1.json', 'provider-main1670-r1-raw/checks.json', 'required_hosted_check_names', 55, BASE),
        ('pr1670-final-checks-r1.json', 'pr1670-final-raw-r1/checks.json', 'required_check_names', 58, closeout['pr_head']),
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
    review = read('prior/pr1670-final-review-r1.json')
    assert review['status'] == 'passed' and review['head'] == closeout['pr_head'] and review['unresolved_threads'] == 0
    assert not review['automated_review']['has_any_findings'] and not review['automated_review']['has_unresolved_findings']
    retired = read('prior/provider-retirement-r1/receipt.json')
    assert retired['status'] == 'passed' and retired['retained_files_unchanged'] and retired['input_generations_unchanged']
    assert retired['portable_replay_after_retirement'] == 'passed'
    assert not retired['storage_limits_changed'] and not retired['payload_backup_created']
    assert digest(data['prior/provider-retirement-r1/manifest.json.xz']) == retired['manifest_sha256']
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
        assert saved['status'] == 'passed' and saved['positive_controls'] == 2 and saved['rejected_controls'] == 15
        assert saved['driver_sha256'] == digest(data['current/tools/check_vector_expression_controls.py'])
        assert controls['primary_sealer_sha256'] == digest(data['current/tools/seal_vector_controls.py'])
        expected_packet = {name: identity((packet / name).read_bytes())
                           for name in ('acceptance.json', 'verification.tar.xz', 'verify.py')}
        assert saved['original_packet'] == expected_packet
        tree = ast.parse(data['current/tools/check_vector_expression_controls.py'])
        cases = next(ast.literal_eval(node.value) for node in tree.body if isinstance(node, ast.Assign)
                     and any(isinstance(target, ast.Name) and target.id == 'CASES' for target in node.targets))
        assert len(cases) == len(set(cases)) == 17 and cases[:2] == ('original', 'relocated_source_only')
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
        native_tests=len(required_native), installed_tests=installed['integration']['tests'],
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
