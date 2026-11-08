"""Verify portable sparse and quantized vector lifecycle evidence without rerunning native work."""
import argparse
import ast
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import re
import tarfile

NATIVE = 'shardloom-python/python-native,shardloom-vortex/native-encoded-exchange-experiment'
CARGO = {
    'format': ['cargo', 'fmt', '--all', '--', '--check'],
    'default_clippy': ['cargo', 'clippy', '--workspace', '--all-targets', '--locked', '--offline', '--', '-D', 'warnings'],
    'native_clippy': ['cargo', 'clippy', '--workspace', '--all-targets', '--features', NATIVE, '--locked', '--offline', '--', '-D', 'warnings'],
    'default_tests': ['cargo', 'test', '--workspace', '--all-targets', '--locked', '--offline'],
    'native_tests': ['cargo', 'test', '--workspace', '--all-targets', '--features', NATIVE, '--locked', '--offline'],
    'build': ['cargo', 'build', '--locked', '--offline', '--release', '--features', 'release-user-surfaces', '-p', 'shardloom-cli', '-p', 'shardloom-python'],
}
ROLES = set(CARGO) | {'features', 'freeze', 'vector_bindings', 'python_core', 'example', 'docs', 'installed'}
RUNNERS = {
    'features': 'check_vector_features.py', 'freeze': 'freeze_runtime.py',
    'python_core': 'scripts/run_python_test_shard.py', 'example': 'run_vector_example.py',
    'docs': 'check_vector_docs.py', 'installed': 'scripts/prove_native_python_wheel.py',
}
MODULES = ('test_native_vector_representations_binding', 'test_native_vector_controls_binding')
REGRESSION_MODULES = (
    'test_native_tensor_binding', 'test_native_tensor_controls_binding',
    'test_native_tensor_variable_binding', 'test_native_tensor_variable_controls_binding',
    'test_native_float16_binding', 'test_native_float16_controls_binding',
    'test_native_float16_lifecycle_binding',
)
REQUIRED_NATIVE_TESTS = (
    'native_vector_metadata_is_versioned_canonical_and_preserves_identity_presence',
    'native_vector_sparse_relations_are_exact_without_dimension_sized_state',
    'native_vector_quantized_modes_keep_empty_parameters_and_partial_blocks',
    'native_vector_storage_retains_every_primitive_width_and_nullability',
    'native_vector_received_metadata_is_released_with_its_last_owner',
    'native_vector_arrow_slices_and_every_ancestor_mask_hide_only_absent_rows',
    'native_vector_arrow_required_scales_remain_finite_positive_in_extended_policy',
    'native_vector_arrow_metadata_budget_and_cancellation_release_before_publication',
    'native_vector_native_file_selection_collection_stream_and_writes_ignore_hidden_invalid_values',
    'native_vector_native_file_visible_invalid_values_fail_before_retention_and_publication',
    'native_vector_typed_json_preserves_wide_values_empty_rows_and_nested_nulls',
    'native_vector_typed_json_refuses_cross_field_errors_and_bad_metadata_before_source_publication',
    'variable_tensor_arrow_parent_masks_and_slices_control_shape_validation',
    'variable_tensor_arrow_rejects_visible_nulls_mismatch_and_uniform_violation',
    'variable_tensor_arrow_roundtrip_preserves_schema_slices_and_float_bits',
    'variable_tensor_arrow_metadata_workspace_and_cancellation_release',
    'variable_tensor_parquet_required_shape_inherits_null_struct_visibility',
    'variable_tensor_native_selection_validates_only_observed_rows_and_keeps_duplicates',
    'variable_tensor_native_parent_masks_and_complete_validation_agree',
    'variable_tensor_typed_json_checks_shape_before_source_publication',
    'float16_every_bit_pattern_survives_owned_register_copy_and_reordered_gather',
    'float16_promotion_observes_only_selected_valid_values_under_strict_policy',
    'native_nested_empty_intake_retains_nonnullable_lists_and_structs',
)
LIMITATIONS = [
    'Sparse and quantized vectors are payload-only; arithmetic, quantization, dequantization and ANN remain separate work.',
    'Text, Avro and ORC refuse these representations before producer or publication work.',
    'Rich dictionary ingestion remains unadmitted; List and LargeList map to native List, and export retains the existing checked Int32 offset limit.',
    'The native selected-validation grants do not account for all upstream provider allocations and do not establish an RSS bound or zero-decode claim.',
]


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def identity(raw):
    return dict(bytes=len(raw), sha256=digest(raw))


def code_path(name):
    return not name.startswith('docs/') and Path(name).suffix in {'.rs', '.py', '.toml', '.lock'}


def rust_counts(raw):
    rows = re.findall(rb'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;', raw)
    return dict(zip(('passed', 'failed', 'ignored'),
        (sum(int(row[i]) for row in rows) for i in range(3)))) if rows else None


def test_methods(raw, module):
    return sorted(module + '.' + node.name + '.' + method.name
        for node in ast.parse(raw).body if isinstance(node, ast.ClassDef)
        for method in node.body if isinstance(method, ast.FunctionDef) and method.name.startswith('test_'))


def successful_methods(raw, modules):
    names = '|'.join(re.escape(name) for name in modules)
    matches = re.findall(r'^test_\S+ \((' + names + r')\.([^\)]+)\) \.\.\. ok$', raw.decode(), re.MULTILINE)
    return sorted(module + '.' + method for module, method in matches)


def verify(packet, repo_root=None):
    report = json.loads((packet / 'acceptance.json').read_bytes())
    assert report['schema_version'] == 'shardloom.vector_representations_acceptance.v1'
    assert report['status'] == 'local_acceptance_passed'
    assert report['archive'] == 'verification.tar.xz'
    archive_raw = (packet / report['archive']).read_bytes()
    assert digest(archive_raw) == report['archive_sha256']
    members = report['members']
    assert len({row['path'] for row in members}) == len(members)
    assert sum(row['bytes'] for row in members) < 128 << 20
    data = {}
    with tarfile.open(fileobj=io.BytesIO(archive_raw), mode='r:xz') as archive:
        entries = archive.getmembers()
        assert [entry.name for entry in entries] == [row['path'] for row in members]
        for row, entry in zip(members, entries):
            path = PurePosixPath(row['path'])
            assert entry.isfile() and not path.is_absolute() and '..' not in path.parts
            assert entry.size == row['bytes']
            raw = archive.extractfile(entry).read()
            assert identity(raw) == {key: row[key] for key in ('bytes', 'sha256')}
            data[row['path']] = raw
    assert Path(__file__).read_bytes() == data['current/tools/verify_vector_packet.py']

    def read(name):
        return json.loads(data[name])

    def blob(info):
        raw = data['current/source-blobs/' + info['sha256']]
        assert identity(raw) == info
        return raw

    for name, raw in data.items():
        if name.startswith('current/source-blobs/'):
            assert name.rsplit('/', 1)[1] == digest(raw)
        if name.startswith('current/') and name.count('/') == 1 and name.endswith('.json'):
            value = json.loads(raw)
            assert value.get('status') != 'running', name
            if 'log_sha256' in value:
                assert digest(data[name.removesuffix('.json') + '.log']) == value['log_sha256']
            for info in value.get('changed_source_inventory', {}).values():
                if 'sha256' in info:
                    blob(info)

    roles = report['roles']
    assert set(roles) == ROLES and len(set(roles.values())) == len(ROLES)
    source = report['code_snapshot']
    assert source and all(code_path(name) for name in source)
    build = read('current/' + roles['build'])
    assert report['source_base'] == build['head']
    assert source == {name: info for name, info in build['changed_source_inventory'].items() if code_path(name)}
    assert not any(name.startswith('vendor/') or name.endswith(('Cargo.toml', 'Cargo.lock')) for name in source)
    for name, info in source.items():
        if info == {'absent': True}:
            if repo_root:
                assert not (repo_root / name).exists(), name
        else:
            raw = blob(info)
            if repo_root:
                assert (repo_root / name).read_bytes() == raw, name
    assert set(report['checks']) == ROLES
    for role, filename in roles.items():
        result = read('current/' + filename)
        assert result['status'] == 'passed' and result['returncode'] == 0
        assert result['process_group_drained'] and result['direct_child_reaped']
        assert result['head'] == report['source_base']
        assert result['environment']['CARGO_BUILD_JOBS'] == '2'
        assert result['environment']['CARGO_INCREMENTAL'] == '0'
        assert {name: info for name, info in result['changed_source_inventory'].items() if code_path(name)} == source, role
        if role in CARGO:
            assert result['command'][result['command'].index('cargo'):] == CARGO[role]
        elif role in RUNNERS:
            assert any(arg.endswith(RUNNERS[role]) for arg in result['command'])
        elif role == 'vector_bindings':
            assert 'SHARDLOOM_REQUIRE_NATIVE_PYTHON=1' in result['command']
            assert result['command'][-9:] == ['-B', '-m', 'unittest', 'discover', '-s', 'python/tests', '-p', 'test_native_vector*binding.py', '-v']
        raw = data['current/' + filename.removesuffix('.json') + '.log']
        summary = dict(receipt=filename, command=result['command'], status='passed', rust_tests=rust_counts(raw))
        assert report['checks'][role] == summary
        if summary['rust_tests']:
            assert summary['rust_tests']['passed'] > 0 and summary['rust_tests']['failed'] == 0

    def log(role):
        return data['current/' + roles[role].removesuffix('.json') + '.log']

    for name in REQUIRED_NATIVE_TESTS:
        assert re.search(rb'::' + re.escape(name.encode()) + rb' \.\.\. ok', log('native_tests')), name
    feature_tree = ast.parse(data['current/tools/check_vector_features.py'])
    commands = next(ast.literal_eval(node.value) for node in feature_tree.body
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'commands' for t in node.targets))
    assert len(commands) == 7
    assert [line.removeprefix('Checking ') for line in log('features').decode().splitlines() if line.startswith('Checking ')] == [
        ' '.join(command + ['--locked', '--offline']) for command in commands]

    core = read('current/python-core-r2/core.json')
    assert core['status'] == 'passed' and core['failure_count'] == core['error_count'] == 0
    count = re.search(rb'Ran (\d+) tests? in [0-9.]+s', log('python_core'))
    assert count
    assert report['python_core'] == dict(discovered_test_cases=core['test_count'],
        reported_test_runs=int(count.group(1)), skip_records=core['skipped_count'], failures=0, errors=0)
    installed = read('current/installed-proof-r1/report.json')
    assert installed['status'] == installed['uninstall_status'] == 'passed'
    assert installed['installed_load']['isolated_install']
    assert installed['integration'] == report['installed_package'] == read('current/installed-proof-r1/integration.json')
    assert installed['integration']['status'] == 'passed'
    assert all(installed['integration'][key] == 0 for key in ('failures', 'errors', 'skipped'))
    assert not installed['publication_attempted'] and not installed['performance_claim']
    proof_source = data['contracts/scripts/prove_native_python_wheel.py']
    if repo_root:
        assert (repo_root / 'scripts/prove_native_python_wheel.py').read_bytes() == proof_source
    assert b'"test_native*binding.py"' in proof_source
    assert b'"test_public_context.py"' in proof_source
    assert b'selected.countTestCases()' in proof_source
    for step in installed['steps']:
        assert step['returncode'] == 0
        assert digest(data['current/installed-proof-r1/' + step['log']]) == step['log_sha256']
    integration_log = data['current/installed-proof-r1/integration.log']
    count = re.search(rb'Ran (\d+) tests? in [0-9.]+s', integration_log)
    assert count and int(count.group(1)) == installed['integration']['tests']
    assert b'\nOK\n' in integration_log and b'... skipped' not in integration_log
    expected = sorted(name for module in MODULES for name in test_methods(blob(source['python/tests/' + module + '.py']), module))
    assert len(expected) == 11
    assert report['vector_tests'] == successful_methods(integration_log, MODULES) == successful_methods(log('vector_bindings'), MODULES) == expected
    context = data['contracts/python/tests/test_public_context.py']
    assert successful_methods(integration_log, ['test_public_context']) == test_methods(context, 'test_public_context')
    for module in REGRESSION_MODULES:
        name = 'python/tests/' + module + '.py'
        raw = data['contracts/' + name]
        if repo_root:
            assert (repo_root / name).read_bytes() == raw
        assert successful_methods(integration_log, [module]) == test_methods(raw, module)
    assert all(marker in log('example') for marker in (
        b'"complete_reopened_rows": 2', b'"signed_zero_preserved": true',
        b'"sparse_order_and_duplicates_preserved": true', b'"integer_code_extremes_preserved": true'))

    frozen = read('current/' + report['runtime_identity'])
    assert frozen['status'] == 'passed' and frozen['build_receipt'] == roles['build']
    assert frozen['build_receipt_sha256'] == digest(data['current/' + roles['build']])
    assert {Path(row['path']).name: row['sha256'] for row in frozen['files']} == {
        'shardloom': installed['integrity']['cli_sha256'], 'lib_native.dylib': installed['integrity']['native_library_sha256']}
    assert report['runtime_and_wheel'] == {key: installed['integrity'][key] for key in ('cli_sha256', 'native_library_sha256', 'wheel_sha256')}
    package_sources = read('current/wheel-source-identity.json')
    assert package_sources['status'] == 'passed' and package_sources['wheel_sha256'] == report['runtime_and_wheel']['wheel_sha256']
    assert package_sources['changed_python_sources'] == {name: info for name, info in source.items() if name.startswith('python/src/shardloom/')}
    assert report['dependency_change'] == 'none'
    for name, info in report['unchanged_dependency_sources'].items():
        assert identity(data['contracts/' + name]) == info
        if repo_root:
            assert identity((repo_root / name).read_bytes()) == info

    failures = sorted(name for name in data if (name.startswith('current/') and name.count('/') == 1
        or name.startswith('current/preparation/') and name.count('/') == 2)
        and name.endswith('.json') and read(name).get('status') == 'failed')
    index = read('current/' + report['failure_index'])
    assert failures == [entry['receipt'] for entry in index['failures']] == [entry['receipt'] for entry in report['original_failures']]
    for entry in index['failures']:
        assert identity(data[entry['receipt']]) == entry['identity'] and entry['disposition']
    assert index['failures'] == report['original_failures']
    assert len(failures) >= 10
    cleanup = read('current/debug-retirement-r1/receipt.json')
    assert cleanup['status'] == 'passed' and cleanup['regular_files'] > 0
    assert cleanup['retained_runtimes_and_wheel_unchanged'] and cleanup['input_generations_unchanged']
    assert cleanup['release_cache_unchanged'] and not cleanup['storage_floor_changed'] and not cleanup['payload_backup_created']
    assert digest(data['current/debug-retirement-r1/manifest.json.xz']) == cleanup['manifest_sha256']

    closeout = read('prior/float16-closeout-r1.json')
    assert closeout['status'] == 'complete' and closeout['merge_commit'] == report['source_base']
    assert closeout['pr_checks_passed'] == 58 and closeout['main_checks_passed'] == 55
    assert not closeout['review_findings']
    assert closeout['float16_pull_request_closed']
    assert closeout['repository_reconciliation_status'] == 'recorded_pending_separate_review'
    assert [row['number'] for row in closeout['pending_independent_dependency_proposals']] == [1664, 1665, 1666, 1667, 1668]
    assert closeout['portable_replay_after_retirement'] == 'passed' and closeout['current_inputs_unchanged']
    transfer = read('current/preparation-transfer-r1.json')
    assert transfer['status'] == 'passed'
    assert digest(data['prior/float16-closeout-r1.json']) == transfer['prior_float16_closeout_sha256']
    for relative, info in transfer['files'].items():
        assert identity(data['current/preparation/' + relative]) == info, relative
    fixture = read('current/preparation/next-vector-fixture-r2/report.json')
    assert fixture['status'] == 'passed' and fixture['rows'] == 6 and fixture['fields'] == 11
    assert fixture['complete_values_including_signed_zero'] and fixture['logical_schema_and_metadata_exact']
    assert fixture['fixture_source']['sha256'] == '27076e2f97d1c1246e09e1f7f65ec6c4eeb1ed7873c2a0d411fe4676e0f3ee69'
    assert source['python/tests/vector_fixture.py'] == fixture['fixture_source']
    assert identity(data['current/preparation/tools/next_vector_fixture.py']) == fixture['fixture_source']
    for relative, info in fixture['artifacts'].items():
        assert identity(data['current/preparation/next-vector-fixture-r2/' + relative]) == info
    formats = read('current/preparation/next-vector-format-probe-r2/report.json')
    assert formats['status'] == 'passed' and formats['cases'] == len(formats['observations']) == 192
    assert not formats['native_support_claim'] and not formats['performance_claim']
    for observation in formats['observations']:
        assert all(observation[key] for key in ('logical_schema_and_metadata_exact', 'complete_values_exact', 'signed_zero_exact'))
        assert identity(data['current/preparation/next-vector-format-probe-r2/' + observation['artifact']]) == {
            key: observation[key] for key in ('bytes', 'sha256')}
    for attempt in ('next-vector-fixture-r1', 'next-vector-fixture-r2', 'next-vector-format-probe-r1', 'next-vector-format-probe-r2'):
        observation = read('current/preparation/' + attempt + '.json')
        assert observation['status'] == ('failed' if attempt.endswith('r1') else 'passed')
        assert observation['process_group_drained']
        assert digest(data['current/preparation/' + attempt + '.log']) == observation['log_sha256']
    for name, info in closeout['receipts'].items():
        assert identity(data['prior/' + name]) == info
    main_names = [name for name in closeout['receipts'] if re.fullmatch(r'float16-main1663-r\d+\.json', name)
        and read('prior/' + name)['status'] == 'passed']
    assert len(main_names) == 1
    main = read('prior/' + main_names[0])
    assert main['source_commit'] == closeout['merge_commit']
    assert main['pr_head'] == closeout['pr_head'] and main['source_tree'] == closeout['source_tree']
    for name, checksum in main['raw_sources'].items():
        assert digest(data['prior/' + name]) == checksum
    checks = read('prior/' + main_names[0].removesuffix('.json') + '-raw/checks.json')
    assert checks['total_count'] == len(checks['check_runs'])
    required = set(main['required_hosted_check_names'])
    selected = [row for row in checks['check_runs'] if row['name'] in required]
    assert len(required) == len(selected) == 55 and {row['name'] for row in selected} == required
    assert all(row['head_sha'] == closeout['merge_commit'] and row['status'] == 'completed' and row['conclusion'] == 'success' for row in selected)
    assert report['limitations'] == LIMITATIONS and report['hosted_integration'] == 'pending'
    assert not any(report[key] for key in ('performance_claim', 'publication_attempted', 'whole_item_19_complete'))
    return dict(status='passed', archive_members=len(members), changed_sources=len(source),
        installed_tests=installed['integration']['tests'], vector_tests=len(expected), original_failures=len(failures))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packet', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--repo-root', type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(args.packet, args.repo_root)))
