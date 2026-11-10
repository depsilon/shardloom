"""Replay source-bound Variant expression acceptance without native execution or a checkout."""
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

BASE = '8df722749e3993bb27a9cdd573de8a2a3860bb34'
PREPARATION_BASE = 'd21f680f0fab0592d9fa0b649d505b36ecbd7e8b'
BASE_TREE = '2686fcc6b7120ca04d53e7ac4f0d1cc1847eb322'
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
    'features': 'check_features.py', 'freeze': 'freeze_runtime.py',
    'python_core': 'scripts/run_python_test_shard.py',
    'docs': 'check_docs.py', 'installed': 'scripts/prove_native_python_wheel.py',
    'msrv': 'check_msrv.py',
}
ROLES = set(CARGO) | set(RUNNERS) | {'public'}
REUSABLE_PREPARATION = {
    'format', 'native_clippy', 'native_tests',
    'default_clippy', 'default_tests', 'features', 'msrv',
    'build', 'freeze', 'public', 'installed', 'python_core',
}
MODULES = ('test_native_variant_expression_binding', 'test_native_function_catalog', 'test_native_result_schema')
CONTROL_CASES = (
    'original', 'relocated_source_only', 'archive_byte', 'preparation_relabelled',
    'adopted_tree_changed', 'adopted_sources_changed', 'native_source_changed',
    'provider_source_changed', 'provider_lock_changed', 'preparation_prior_changed',
    'unclosed_process', 'native_clippy_failed', 'native_test_missing',
    'public_test_missing', 'public_test_skipped', 'installed_skip',
    'installed_binary_changed', 'wheel_python_changed', 'frozen_fixture_changed',
    'path_test_missing', 'failure_missing', 'prior_main_failed',
    'prior_retirement_incomplete',
)
NATIVE_TEST_SOURCES = (
    'shardloom-vortex/src/local_primitive_relational_variant_expression_tests.rs',
    'shardloom-vortex/src/local_primitive_relational_variant_bind.rs',
    'shardloom-cli/src/sql_syntax.rs',
)
NATIVE_TEST_PATTERN = r'\bfn\s+((?:native_variant_expression_\w+|variant_path_literals_preserve_names_and_refuse_unsupported_steps|parser_variant_field_functions_reach_native_binding))\s*\('
STATUS_PROSE = {
    'docs/architecture/phased-execution-plan.md',
    'docs/architecture/native-variant-expressions-2026-11-10.md',
    'docs/architecture/native-rich-data-lifecycle-2026-10-24.md',
    'docs/architecture/local-development-storage.md',
}
# Complete independently specified fixtures captured before broad acceptance.
# Their expected values do not come from candidate execution.
FROZEN = {'python/tests/test_native_variant_expression_binding.py': ('public-release-preparation-r1.json', 'cddffdd3ec418b12737a84f849ffce108d89efb85514fe43f445402cbc351dd5'), 'python/tests/test_native_function_catalog.py': ('public-release-preparation-r1.json', 'f33a28f012fba406a3fb8ae1b2cdb43dfdee287c69f4b2c24b0852bf98720beb'), 'python/tests/test_native_result_schema.py': ('public-release-preparation-r1.json', '42dbfa36e89f02ae3de8d4df30c654f6fe6364e1560961d8b1a0fea4dac0069a'), 'shardloom-vortex/src/local_primitive_relational_variant_expression_tests.rs': ('public-release-preparation-r1.json', 'c319673e0d7b90fc783db24e7cec8ee46795746c05b42f27230cde0eaf41142c'), 'shardloom-vortex/src/local_primitive_relational_variant_bind.rs': ('public-release-preparation-r1.json', 'f1797da046f05ac53fdbdbbb6f73865da5b4e9d2cc13e534956d254b90a342ee'), 'shardloom-cli/src/sql_syntax.rs': ('public-release-preparation-r1.json', 'd8aabdec01243af278e855f33c1474f0e0ffebf4e8e557446d0bae995fdc4aaf')}
EXCLUDED = ('docs/benchmarks/evidence/', 'docs/release/evidence/', 'docs/dependencies/evidence/')
REQUIRED_HISTORICAL_FAILURES = {'variant-clippy-preparation-r1.json', 'native-clippy-preparation-r2.json', 'variant-focused-preparation-r5.json', 'public-preparation-r1.json', 'variant-focused-preparation-r3.json', 'variant-focused-preparation-r1.json', 'public-preparation-r2.json', 'native-clippy-preparation-r1.json'}
PRIOR_CORE_RECEIPTS = (
    'pr1675-premerge-r1.json', 'uuid-main-r1.json',
    'uuid-retirement-r1/receipt.json', 'uuid-retirement-r1/plan.json',
    'uuid-retirement-r1/manifest.json.xz', 'uuid-main-checkout-r1.json',
    'runtime-r1-identity.json', 'installed-proof-r1/report.json',
)
LIMITATIONS = [
    'Finite native Variant field/path extraction, typed conversion, persistence and ownership acceptance; no benchmark or performance comparison.',
    'Literal paths and targets only. Paths have at most 24 steps and 16384 UTF-8 bytes. SQL NULL and embedded Variant null remain distinct; unsupported traversal and failed typed conversions return SQL NULL.',
    'Pinned Vortex 0.93 native format conversion uses an admitted Arrow format bridge and conservative workspace reservations. No zero-copy, zero-decode or total-process RSS bound is claimed; temporal conversion pairs remain explicitly bounded.',
    'Logical nesting depth remains 24 and storage wrappers remain bounded at 64. Useful spatial support and resource-admitted deep traversal remain required.',
    'Original failed and interrupted observations remain evidence. Default/native opt-in ignores and Python optional/environment skips remain explicit; selected public and installed tests require zero skips.',
    'No package publication, performance superiority, whole item 19, item 20, RFC 0044 or CG-1 through CG-23 completion claim.',
]
SOURCE_POLICY = (
    'All original source inventories and preparation labels remain immutable. Reused preparation '
    'requires unchanged pre/post source inventories, drained/reaped processes, final production-source '
    'equality and the completed preceding UUID closeout. The UUID PR head and accepted merge have the '
    'same tree; base adoption preserves index, dirty file identities and working diff. Only named '
    'status prose may differ at engine/runtime gates. All vendor, provider-lock, Cargo/config and '
    'production inputs remain included. Fresh docs and contract checks run after identical-tree '
    'base adoption and completed UUID closeout. Final guards capture and reverify that closeout '
    'identity; all guards preserve the retained runtime/wheel and input generations. No preparation '
    'receipt is relabeled as fixed-source final acceptance.'
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


def failed_records(data):
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
    return failures


def embedded_evidence(data):
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
    assert report['schema_version'] == 'shardloom.variant_expression_acceptance.v1'
    assert report['status'] == 'local_acceptance_passed' and report['source_base'] == BASE
    assert report['archive'] == 'verification.tar.xz' and report['source_scope_policy'] == SOURCE_POLICY
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
    assert Path(__file__).read_bytes() == data['current/tools/verify_variant_expression_packet.py']
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
                # Guards retain unexpected filesystem modes diagnostically;
                # acceptance refuses source objects without portable identities.
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
    receipts = {role: read('current/' + name) for role, name in roles.items()}
    source = receipts['docs']['build_sources']
    assert report['source_snapshot'] == source and report['source_fingerprint_sha256'] == fingerprint(source)
    check_inventory(source)

    def source_raw(name):
        return blob(source[name])

    def log(role):
        return data['current/' + roles[role].removesuffix('.json') + '.log']

    closeout = verify_prior(data, read, report)
    adoption = read('current/variant-expression-base-adoption-r1.json')
    assert adoption['status'] == 'passed' and adoption['old_head'] == PREPARATION_BASE and adoption['new_head'] == BASE
    assert adoption['tree'] == BASE_TREE and adoption['accepted_base_tree_unchanged']
    assert adoption['index_unchanged'] and adoption['dirty_sources_unchanged'] and adoption['working_diff_unchanged']
    assert adoption['branch_compare_and_set'] and adoption['before'] == adoption['after']
    assert adoption['prior_closeout'] == identity(data['prior/uuid-closeout-r1.json'])
    assert adoption['script'] == identity(data['current/tools/adopt_uuid_base.py'])
    assert report['base_adoption'] == identity(data['current/variant-expression-base-adoption-r1.json'])
    retained = read('current/retained-baseline-r1.json')
    assert retained['status'] == 'local_dependency_proof_passed' and retained['source_head'] == PREPARATION_BASE
    assert retained['retained_runtime_and_wheel'] == closeout['retained_runtime_and_wheel']
    reused = {}
    for role, result in receipts.items():
        assert result['status'] == 'passed' and result['returncode'] == 0, role
        assert result['process_group_drained'] and result['direct_child_reaped'], role
        preparation = result['source_validation_mode'] == 'preparation_no_source_changes'
        if preparation:
            assert role in REUSABLE_PREPARATION and result['fixed_source_validation'] is False
            assert result['head'] == result['base'] == PREPARATION_BASE
            assert result['accepted_uuid_closeout'] is None
            reused[role] = dict(receipt=roles[role], original_mode=result['source_validation_mode'],
                original_head=result['head'], final_base=BASE, equal_base_tree=BASE_TREE,
                compared_scope='all_inputs_except_named_status_prose')
        else:
            assert result['fixed_source_validation'] and result['source_validation_mode'] == 'fixed_source_validation'
            assert result['head'] == BASE and result['base'] == PREPARATION_BASE
            assert result['accepted_uuid_closeout'] == dict(
                path='/Users/dylan/LocalData/shardloom/uuid-lifecycle-2026-11-10/uuid-closeout-r1.json',
                **identity(data['prior/uuid-closeout-r1.json']))
        assert result['prior_closeout_sha256'] == digest(data['current/retained-baseline-r1.json'])
        assert result['driver_sha256'] == digest(data['current/tools/guard_variant_expressions.py'])
        assert result['prior_retained_runtime_and_wheel'] == closeout['retained_runtime_and_wheel']
        assert not result['allow_lockfile_update'] and not result['root_cargo_lock_hashes']['changed']
        for phase in ('pre_execution', 'post_execution'):
            observed = result['build_source_verification'][phase]
            assert observed['head'] == result['head'] and observed['head_matches'] and observed['path_set_matches']
            assert observed['changed_paths'] == [] and observed['path_count'] == len(result['build_sources'])
            assert observed['fingerprint_sha256'] == fingerprint(result['build_sources'])
        assert result['environment']['CARGO_BUILD_JOBS'] == '2' and result['environment']['CARGO_INCREMENTAL'] == '0'
        deltas = source_deltas(result['build_sources'], source)
        assert set(deltas) <= (set() if role in {'docs', 'doc_contracts'} else STATUS_PROSE), (role, deltas)
        assert report['source_deltas'][role] == deltas
        if role in CARGO:
            assert result['command'][result['command'].index('cargo'):] == CARGO[role]
            if role == 'native_tests':
                assert result['command'][:2] == ['env', 'RUST_TEST_THREADS=4']
        elif role in RUNNERS:
            runner = RUNNERS[role]
            assert len([arg for arg in result['command'] if arg.endswith(runner)]) == 1
            records = [(name, info) for name, info in result['tool_source_inventory'].items() if name.endswith('/' + runner)]
            assert len(records) == 1
            expected = source_raw(runner) if runner.startswith('scripts/') else data['current/tools/' + runner]
            assert identity(expected) == records[0][1]
        else:
            assert 'SHARDLOOM_REQUIRE_NATIVE_PYTHON=1' in result['command']
            assert result['command'][-len(MODULES) - 4:] == ['-B', '-m', 'unittest', '-v', *MODULES]
        assert report['checks'][role] == dict(receipt=roles[role], status='passed', command=result['command'], rust_tests=rust_counts(log(role)))
        counts = rust_counts(log(role))
        if counts:
            assert counts['passed'] > 0 and counts['failed'] == 0
    assert report['preparation_reuse'] == reused
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
    native_tests, provider_checks = verify_tests(data, read, source_raw, log, report)
    installed = verify_runtime(data, read, source_raw, source, log, report, receipts)
    failures = failed_records(data)
    assert failures == [{key: row[key] for key in ('receipt', 'identity')} for row in report['original_failures']]
    assert all(row['disposition'] for row in report['original_failures'])
    assert re.fullmatch(r'failure-dispositions-final-r\d+\.json', report['failure_dispositions'])
    dispositions = read('current/' + report['failure_dispositions'])
    assert set(dispositions) == {row['receipt'].removeprefix('current/') for row in failures}
    assert all(row['disposition'] == dispositions[row['receipt'].removeprefix('current/')] for row in report['original_failures'])
    assert {'current/' + name for name in REQUIRED_HISTORICAL_FAILURES} <= {row['receipt'] for row in failures}
    verify_retirements(data, read, closeout)
    assert report['limitations'] == LIMITATIONS and report['hosted_integration'] == 'pending'
    assert not report['performance_claim'] and not report['publication_attempted'] and not report['whole_item_19_complete']
    controls_verified = verify_controls(packet, data, require_controls)
    return dict(status='passed', members=len(members), roles=len(roles), public_tests=len(report['public_tests']),
        native_tests=native_tests, provider_tests=provider_checks, installed_tests=installed['integration']['tests'],
        original_failures=len(failures), source_fingerprint_sha256=fingerprint(source),
        preparation_roles_reused=sorted(reused), native_execution_repeated=False,
        repository_checked=repo_root is not None, controls_verified=controls_verified)


def verify_prior(data, read, report):
    closeout = read('prior/uuid-closeout-r1.json')
    assert closeout['status'] == 'complete' and closeout['merge_commit'] == BASE
    assert closeout['pr_head'] == PREPARATION_BASE and closeout['source_tree'] == BASE_TREE
    assert closeout['pr_checks_passed'] == 58 and closeout['main_checks_passed'] == 55
    assert closeout['portable_replay_after_retirement'] == 'passed' and closeout['current_inputs_unchanged']
    accepted = read('prior/acceptance.json')
    accepted_name = 'docs/release/evidence/native-uuid-lifecycle-2026-11-10/acceptance.json'
    assert identity(data['prior/acceptance.json']) == closeout['committed_evidence'][accepted_name]
    assert accepted['status'] == 'local_acceptance_passed'
    assert accepted['installed_package'] == closeout['installed_package'] == dict(
        status='passed', tests=431, failures=0, errors=0, skipped=0)
    pre, main = read('prior/pr1675-premerge-r1.json'), read('prior/uuid-main-r1.json')
    required = set(PRIOR_CORE_RECEIPTS) | set(pre['raw_sources']) | set(main['raw_sources'])
    assert report['prior_included_receipts'] == sorted(required)
    for name in required:
        assert identity(data['prior/' + name]) == closeout['receipts'][name], name
    assert pre['status'] == 'passed_premerge' and pre['pr'] == main['pr'] == 1675
    assert pre['head'] == main['pr_head'] == PREPARATION_BASE
    assert pre['base'] == main['pr_base'] and pre['source_tree'] == main['source_tree'] == BASE_TREE
    assert pre['tree_matches_synthetic'] and pre['unresolved_threads'] == 0
    assert pre['review']['commit_sha'] == PREPARATION_BASE and pre['review']['status'] == 'completed'
    assert pre['review']['has_any_findings'] is pre['review']['has_unresolved_findings'] is False
    assert not any(row['state'] == 'CHANGES_REQUESTED' for row in pre['review_submissions'])
    assert main['status'] == 'passed' and main['source_commit'] == BASE
    assert main['premerge_sha256'] == digest(data['prior/pr1675-premerge-r1.json'])
    assert not main['failed'] and not main['pending'] and not main['missing'] and not main['duplicate_expected_checks']
    assert main['commit_verification']['verified']
    assert set(main['required_hosted_check_names']) == set(pre['required_check_names']) - {
        'CodeQL', 'CodeQL rust', 'CodeQL python'}
    for hosted, raw_name, key, count, head in (
        (pre, 'pr1675-final-snapshots-r1/checks.json', 'required_check_names', 58, PREPARATION_BASE),
        (main, 'uuid-main-r1-raw/checks.json', 'required_hosted_check_names', 55, BASE),
    ):
        raw = read('prior/' + raw_name)
        assert hosted['successful_hosted_checks'] == count
        assert len(hosted[key]) == len(set(hosted[key])) == count
        assert raw['total_count'] == len(raw['check_runs']) <= 100
        for name in hosted[key]:
            matching = [row for row in raw['check_runs'] if row['name'] == name]
            assert len(matching) == 1 and matching[0]['head_sha'] == head
            assert matching[0]['status'] == 'completed' and matching[0]['conclusion'] == 'success'
        assert all(row['head_sha'] == head and row['status'] == 'completed'
                   and row['conclusion'] == 'success' for row in raw['check_runs'])
        for name, info in hosted['raw_sources'].items():
            observed = identity(data['prior/' + name])
            assert observed == info if isinstance(info, dict) else observed['sha256'] == info
    commit = read('prior/uuid-main-r1-raw/commit.json')
    assert commit['sha'] == BASE and commit['commit']['tree']['sha'] == BASE_TREE
    assert [row['sha'] for row in commit['parents']] == [pre['base'], PREPARATION_BASE]
    assert commit['commit']['verification']['verified'] is True
    retired = read('prior/uuid-retirement-r1/receipt.json')
    plan = read('prior/uuid-retirement-r1/plan.json')
    assert retired['status'] == 'passed' and retired['head'] == PREPARATION_BASE
    assert retired['retained_files_unchanged'] and retired['input_generations_unchanged']
    assert retired['portable_replay_after_retirement'] == 'passed'
    assert retired['historical_member_identities_preserved'] and retired['original_failed_observations_preserved']
    assert not retired['storage_limits_changed'] and not retired['payload_backup_created']
    raw = data['prior/uuid-retirement-r1/manifest.json.xz']
    assert digest(raw) == retired['manifest_sha256'] == plan['manifest_sha256']
    manifest = json.loads(lzma.decompress(raw))
    assert len(manifest['entries']) == plan['entries'] == retired['removed']
    assert len(manifest['input_generations']) == 220
    assert manifest['hosted'] == retired['hosted'] == dict(
        premerge=identity(data['prior/pr1675-premerge-r1.json']),
        main=identity(data['prior/uuid-main-r1.json']), merge_commit=BASE)
    assert all(manifest['retained'][path] == info for path, info in closeout['retained_runtime_and_wheel'].items())
    installed = read('prior/installed-proof-r1/report.json')
    assert installed['status'] == installed['uninstall_status'] == 'passed'
    assert installed['integration'] == accepted['installed_package']
    frozen = read('prior/runtime-r1-identity.json')
    for filename, key in (('shardloom', 'cli_sha256'), ('lib_native.dylib', 'native_library_sha256')):
        rows = [row for row in frozen['files'] if Path(row['path']).name == filename]
        assert len(rows) == 1 and rows[0]['sha256'] == installed['integrity'][key] == accepted['runtime_and_wheel'][key]
        assert closeout['retained_runtime_and_wheel'][rows[0]['path']] == {key: rows[0][key] for key in ('bytes', 'sha256')}
    assert installed['integrity']['wheel_sha256'] == accepted['runtime_and_wheel']['wheel_sha256']
    return closeout


def verify_tests(data, read, source_raw, log, report):
    required_native = []
    for name in NATIVE_TEST_SOURCES:
        methods = re.findall(NATIVE_TEST_PATTERN, source_raw(name).decode())
        assert methods, name
        required_native.extend(methods)
    assert len(required_native) == len(set(required_native)) == 13
    for name in required_native:
        assert re.search(rb'::' + re.escape(name.encode()) + rb' \.\.\. ok$', log('native_tests'), re.MULTILINE), name
    assert report['required_native_tests'] == sorted(required_native)
    assert report['frozen_references'] == {name: list(value) for name, value in FROZEN.items()}
    for name, (receipt_name, sha) in FROZEN.items():
        original = read('current/' + receipt_name)
        assert original['status'] == 'passed' and original['returncode'] == 0
        assert original['build_sources'][name]['sha256'] == sha == digest(source_raw(name))
    expected_public = sorted(name for module in MODULES
        for name in test_methods(source_raw('python/tests/' + module + '.py'), module))
    assert len(expected_public) == 25 and report['public_tests'] == expected_public
    assert successful_methods(log('public'), MODULES) == expected_public
    assert b'\nOK\n' in log('public') and b'... skipped' not in log('public')
    for role, filename, expected_count in (
        ('features', 'check_features.py', 7), ('msrv', 'check_msrv.py', 2)):
        tree = ast.parse(data['current/tools/' + filename])
        commands = next(ast.literal_eval(node.value) for node in tree.body if isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == 'commands' for target in node.targets))
        assert len(commands) == expected_count
        assert [line.removeprefix('Checking ') for line in log(role).decode().splitlines() if line.startswith('Checking ')] == [
            ' '.join(command + ['--locked', '--offline']) for command in commands]
    core = read('current/' + report['python_core_receipt'])
    assert core['status'] == 'passed' and core['failure_count'] == core['error_count'] == 0
    assert core['shard_id'] == 'core' and core['test_count'] > 0
    assert {'python.tests.' + module for module in MODULES} <= set(core['modules'])
    count = re.search(rb'Ran (\d+) tests? in [0-9.]+s', log('python_core'))
    assert count
    assert report['python_core'] == dict(discovered_test_cases=core['test_count'], reported_test_runs=int(count[1]),
        skip_records=core['skipped_count'], failures=0, errors=0)
    return len(required_native), {}


def verify_runtime(data, read, source_raw, source, log, report, receipts):
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
    proof = source_raw('scripts/prove_native_python_wheel.py')
    proof_tree = ast.parse(proof)
    run_tests = next(ast.literal_eval(node.value) for node in proof_tree.body if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == 'RUN_TESTS' for target in node.targets))
    patterns = next(ast.literal_eval(node.value) for node in ast.parse(run_tests).body if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == 'patterns' for target in node.targets))
    assert patterns == ('test_native*binding.py', 'test_connected_programming.py',
        'test_native_function_catalog.py', 'test_native_observations.py', 'test_native_standard_adapters.py',
        'test_native_result_schema.py', 'test_query_surface_native.py', 'test_recursion_policy.py',
        'test_public_context.py', 'test_wide_schema.py')
    selected_modules = [PurePosixPath(name).stem for name in source
        if PurePosixPath(name).parent.as_posix() == 'python/tests'
        and any(PurePosixPath(name).match(pattern) for pattern in patterns)]
    assert set(MODULES) <= set(selected_modules)
    expected_installed = sorted(name for module in selected_modules
        for name in test_methods(source_raw('python/tests/' + module + '.py'), module))
    assert successful_methods(integration, selected_modules) == expected_installed
    assert installed['integration']['tests'] == len(expected_installed)
    integration_steps = [step for step in installed['steps'] if step['name'] == 'integration']
    assert len(integration_steps) == 1 and integration_steps[0]['command'][1:4] == ['-I', '-c', run_tests]
    assert all(pattern in proof for pattern in (
        b'"test_native*binding.py"', b'"test_public_context.py"', b'"test_native_result_schema.py"'))
    frozen = read('current/' + report['runtime_identity'])
    roles = report['roles']
    assert frozen['status'] == 'passed' and frozen['build_receipt'] == roles['build']
    assert frozen['build_receipt_identity'] == identity(data['current/' + roles['build']])
    assert frozen['profile'] == 'release'
    assert frozen['build_source_fingerprint_sha256'] == receipts['build']['build_source_fingerprint_sha256']
    runtime_paths = {Path(row['path']).name: row['path'] for row in frozen['files']}
    assert len(frozen['files']) == len(runtime_paths) == 2
    for variable, filename in (('SHARDLOOM_NATIVE_TEST_LIBRARY=', 'lib_native.dylib'), ('SHARDLOOM_BINARY=', 'shardloom')):
        selected = [argument.split('=', 1)[1] for argument in receipts['public']['command'] if argument.startswith(variable)]
        assert selected == [runtime_paths[filename]]
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
    return installed


def verify_retirements(data, read, closeout):
    directory = 'current/preparation-retirement-r1'
    receipt, plan = read(directory + '/receipt.json'), read(directory + '/plan.json')
    raw = data[directory + '/manifest.json.xz']
    assert receipt['status'] == 'passed' and plan['status'] == 'planned_no_deletion'
    assert digest(raw) == receipt['manifest_sha256'] == plan['manifest_sha256']
    assert digest(data['current/tools/retire_preparation.py']) == plan['script_sha256']
    assert digest(data['current/source-blobs/' + plan['helper_sha256']]) == plan['helper_sha256']
    manifest = json.loads(lzma.decompress(raw))
    roots = manifest['roots']
    assert roots == plan['roots'] and len(roots) == 2
    assert roots == ['/Users/dylan/.cache/shardloom/cargo-target/debug',
        '/Users/dylan/LocalData/shardloom/variant-expressions-2026-11-10/runtime-preparation-r1']
    entries = manifest['entries']
    assert len(entries) == plan['entries'] == receipt['removed'] == len({row['path'] for row in entries})
    assert all(any(PurePosixPath(row['path']).is_relative_to(root) for root in roots) for row in entries)
    assert plan['regular_files'] == receipt['regular_files'] == sum(row['kind'] == 'regular' for row in entries)
    assert plan['symlinks'] == receipt['symlinks'] == sum(row['kind'] == 'symlink' for row in entries)
    assert plan['regular_bytes'] == receipt['regular_bytes'] == sum(
        row['generation'][3] for row in entries if row['kind'] == 'regular')
    assert manifest['source_head'] == PREPARATION_BASE
    successful = {'variant-focused-preparation-r4.json', 'variant-path-preparation-r1.json',
        'variant-clippy-preparation-r2.json', 'public-build-preparation-r2.json'}
    assert set(manifest['observations']) == successful | {
        'public-preparation-r1.json', 'public-preparation-r2.json', 'variant-focused-preparation-r5.json'}
    for name, info in manifest['observations'].items():
        original = read('current/' + name)
        assert identity(data['current/' + name]) == info
        assert original['source_validation_mode'] == 'preparation_no_source_changes'
        assert original['process_group_drained'] and original['direct_child_reaped']
        if name in successful:
            assert original['status'] == 'passed' and original['returncode'] == 0
            for phase in ('pre_execution', 'post_execution'):
                assert original['build_source_verification'][phase]['changed_paths'] == []
    stopped = read('current/variant-focused-preparation-r5.json')
    assert stopped['status'] == 'failed' and 'insufficient free disk' in stopped['error']
    assert all(manifest['retained'][path] == info for path, info in closeout['retained_runtime_and_wheel'].items())
    assert len(manifest['retained']) == 8 and len(manifest['input_generations']) == 220
    assert receipt['retained_runtimes_and_wheels_unchanged'] and receipt['input_generations_unchanged']
    assert receipt['release_cache_unchanged'] and not receipt['storage_limits_changed'] and not receipt['payload_backup_created']
    runtime = manifest['superseded']
    assert runtime == read('current/runtime-preparation-r1-identity.json') and runtime['status'] == 'passed'
    assert runtime['build_receipt_identity'] == identity(data['current/' + runtime['build_receipt']])
    for row in runtime['files']:
        matching = [entry for entry in entries if entry['path'] == row['path']]
        assert len(matching) == 1 and matching[0]['sha256'] == row['sha256']
        assert matching[0]['generation'][3] == row['bytes']


def verify_controls(packet, data, required):
    path = packet / 'corruption-controls.json'
    if required:
        assert path.is_file(), 'saved corruption controls are required'
    if not path.exists():
        return False
    controls = json.loads(path.read_bytes())
    assert controls['status'] == 'passed'
    saved = controls['receipt']
    assert saved['status'] == 'passed' and saved['positive_controls'] == 2
    assert saved['driver_sha256'] == digest(data['current/tools/check_variant_expression_controls.py'])
    assert controls['primary_sealer_sha256'] == digest(data['current/tools/seal_variant_expression_controls.py'])
    expected_packet = {name: identity((packet / name).read_bytes())
        for name in ('acceptance.json', 'verification.tar.xz', 'verify.py')}
    assert saved['original_packet'] == expected_packet
    tree = ast.parse(data['current/tools/check_variant_expression_controls.py'])
    cases = next(ast.literal_eval(node.value) for node in tree.body if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == 'CASES' for target in node.targets))
    assert cases == CONTROL_CASES, 'the complete ordered control inventory is required'
    assert saved['rejected_controls'] == 21
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
    return True


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packet', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--repo-root', type=Path)
    parser.add_argument('--require-controls', action='store_true')
    args = parser.parse_args()
    print(json.dumps(verify(args.packet, args.repo_root, args.require_controls), sort_keys=True))
