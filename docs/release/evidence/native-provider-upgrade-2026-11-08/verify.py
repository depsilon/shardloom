"""Replay saved provider acceptance offline without running engine workloads."""
import argparse
import hashlib
import importlib.util
import io
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import tarfile
import tempfile
import tomllib

ROLES = {'format', 'generated-docs', 'temporal-focused', 'default-clippy',
         'default-tests', 'native-clippy', 'native-tests', 'feature-matrix',
         'msrv-default', 'msrv-native', 'audit', 'release-build', 'freeze-runtime',
         'python-core', 'installed-wheel'}
EXCLUSIONS = ('docs/benchmarks/evidence/', 'docs/release/evidence/', 'docs/dependencies/evidence/')
MAX_RAW = 768 << 20
DOCUMENTATION = {
    'docs/architecture/native-provider-upgrade-2026-11-08.md',
    'docs/architecture/phased-execution-plan.md',
    'docs/architecture/local-development-storage.md',
    'docs/dependencies/vortex-upstream-review.md',
    '.github/provider-locks/README.md',
}


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def identity(raw):
    return dict(bytes=len(raw), sha256=sha(raw))


def fingerprint(value):
    return sha(json.dumps(value, sort_keys=True, separators=(',', ':')).encode())


def counts(log):
    rows = re.findall(rb'test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;', log)
    return dict(zip(('passed', 'failed', 'ignored'),
                    (sum(int(row[i]) for row in rows) for i in range(3)))) if rows else None


def safe(path):
    value = PurePosixPath(path)
    assert path and not value.is_absolute() and '..' not in value.parts and str(value) == path
    return path


def verify_failure_inventory(read):
    prefix = 'observations/provider/'
    root = PurePosixPath('/Users/dylan/LocalData/shardloom/provider-upgrade-2026-11-08')
    relative = lambda path: safe(str(PurePosixPath(path).relative_to(root)))
    raw = read(prefix + 'failure-observation-inventory-r3.json')
    inventory = json.loads(raw)
    previous_raw = read(prefix + 'failure-observation-inventory-r2.json')
    previous = json.loads(previous_raw)
    preserved = inventory['r2_inventory_preserved_verbatim']
    assert preserved['content_utf8'].encode() == previous_raw
    assert identity(previous_raw) == {k: preserved[k] for k in ('bytes', 'sha256')}
    rows = {row['receipt_path']: row for row in inventory['observations']}
    assert len(rows) == len(inventory['observations']) == inventory['summary']['receipt_json_count']
    for row in previous['observations']:
        assert rows[row['receipt_path']] == row
    for row in inventory['observations']:
        content = read(prefix + relative(row['receipt_path']))
        assert identity(content) == dict(bytes=row['receipt_bytes'], sha256=row['receipt_sha256_computed'])
        assert json.loads(content).get('status') == row['status']
    logs = inventory['distinct_logs']
    assert len({row['path'] for row in logs}) == len(logs) == inventory['summary']['distinct_log_path_count']
    for row in logs:
        assert row['exists']
        content = read(prefix + relative(row['path']))
        assert identity(content) == dict(bytes=row['bytes'], sha256=row['sha256_computed'])
        assert all(digest == sha(content) for digest in row['receipt_claimed_sha256'])
    return dict(status='passed', inventory_sha256=sha(raw), retained_prior_receipts=len(previous['observations']),
                receipt_records=len(rows), distinct_linked_logs=len(logs), original_bytes_preserved=True)


def verify(packet, repo_root=None):
    report = json.loads((packet / 'acceptance.json').read_bytes())
    assert report['schema_version'] == 'shardloom.provider_upgrade_acceptance.v1'
    assert report['status'] == 'local_acceptance_passed'
    assert report['archive'] == 'verification.tar.xz'
    archive_raw = (packet / report['archive']).read_bytes()
    assert sha(archive_raw) == report['archive_sha256']
    expected = report['members']
    assert len({p['path'] for p in expected}) == len(expected)
    assert sum(p['bytes'] for p in expected) < MAX_RAW
    data = {}
    with tarfile.open(fileobj=io.BytesIO(archive_raw), mode='r:xz') as archive:
        assert archive.getnames() == [p['path'] for p in expected]
        for row, member in zip(expected, archive.getmembers(), strict=True):
            assert member.isfile() and safe(member.name) == row['path'] and member.size == row['bytes']
            content = archive.extractfile(member).read()
            assert identity(content) == {k: row[k] for k in ('bytes', 'sha256')}
            data[row['path']] = content
    assert Path(__file__).read_bytes() == data['observations/provider/tools/verify_provider_packet.py']
    source_tool = packet / 'verify_sources.py'
    assert source_tool.read_bytes() == data['observations/provider/tools/verify_provider_sources.py']
    obj = lambda path: json.loads(data[path])
    provider = lambda path: obj('observations/provider/' + path)
    sources = report['validation_source_snapshot']
    assert fingerprint(sources) == report['validation_source_fingerprint_sha256']
    assert len(sources) == report['validation_source_path_count']
    for path, entry in sources.items():
        safe(path)
        if 'bytes' in entry:
            assert identity(data['source-blobs/' + entry['sha256']]) == entry
        else:
            assert entry == {'absent': True} or set(entry) == {'symlink', 'sha256'}
            if 'symlink' in entry:
                assert sha(entry['symlink'].encode()) == entry['sha256']
    current = lambda path: data['source-blobs/' + sources[path]['sha256']]
    protocol = provider(report['validation_protocol'])
    sequence = provider(report['validation_sequence'])
    assert not protocol['prepared_only']
    assert protocol['head'] == report['source_head']
    assert protocol['build_source_fingerprint_sha256'] == report['validation_source_fingerprint_sha256']
    names = [p['name'] for p in protocol['stages']]
    assert len(names) == len(ROLES)
    assert {re.sub(r'-r\d+$', '', n.removeprefix('provider-')) for n in names} == ROLES
    assert sequence['status'] == 'passed' and sequence['completed'] == names and sequence['active_stage'] is None
    assert sequence['protocol_sha256'] == sha(data['observations/provider/' + report['validation_protocol']])
    assert [c['receipt'] for c in report['checks']] == [name + '.json' for name in names]
    for path, digest in protocol['requires'].items():
        assert sha(data['observations/provider/' + path]) == digest
    for check, stage in zip(report['checks'], protocol['stages'], strict=True):
        result = provider(check['receipt'])
        assert result['status'] == 'passed' and result['returncode'] == 0
        assert result['process_group_drained'] and result['direct_child_reaped']
        assert result['fixed_source_validation'] and not result['allow_lockfile_update']
        assert not result['query_workload'] and not result['timed_out'] and result['interrupted_signal'] is None
        assert result['head'] == report['source_head'] and result['base'] == report['accepted_base']
        assert result['command'] == stage['command'] == check['command']
        assert result['build_sources'] == sources
        assert result['build_source_fingerprint_sha256'] == fingerprint(sources)
        for point in ('pre_execution', 'post_execution'):
            validation = result['build_source_verification'][point]
            assert validation['head_matches'] and validation['path_set_matches'] and validation['changed_paths'] == []
            assert validation['fingerprint_sha256'] == fingerprint(sources)
        lock = result['root_cargo_lock_hashes']
        assert not lock['changed'] and not lock['allowed_to_change']
        assert lock['before_sha256'] == lock['after_sha256'] == sources['Cargo.lock']['sha256']
        assert sha(data['source-blobs/' + result['driver_sha256']]) == result['driver_sha256']
        for item in result['tool_source_inventory'].values():
            assert identity(data['source-blobs/' + item['sha256']]) == item
        log = data['observations/provider/' + check['receipt'].removesuffix('.json') + '.log']
        assert sha(log) == result['log_sha256']
        assert counts(log) == check['rust_tests']
        if check['rust_tests'] is not None:
            assert check['rust_tests']['failed'] == 0
    spec = importlib.util.spec_from_file_location('provider_source_replay', source_tool)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    source_result = module.verify(data.__getitem__, sources, report['baseline_provider_locks'])
    assert source_result == report['source_verification']
    saved_source = provider(report['source_replay'])
    assert saved_source['status'] == 'passed_actual_current_sources' and saved_source['simulation'] is False
    assert saved_source['build_source_fingerprint_sha256'] == fingerprint(sources)
    assert saved_source['verification'] == source_result
    assert saved_source['verifier_sha256'] == sha(source_tool.read_bytes())
    standalone_tool = packet / 'verify_standalone.py'
    assert standalone_tool.read_bytes() == data['observations/provider/tools/verify_standalone_acceptance.py']
    standalone_spec = importlib.util.spec_from_file_location('provider_standalone_replay', standalone_tool)
    standalone_module = importlib.util.module_from_spec(standalone_spec)
    standalone_spec.loader.exec_module(standalone_module)
    standalone_result = standalone_module.verify(data.__getitem__, sources)
    assert standalone_result == provider(report['standalone_acceptance'])
    notice_summary = source_result['dependency_notices']
    assert report['dependency_notices'] == notice_summary
    baseline_files = {p.removeprefix('baseline/provider/') for p in data if p.startswith('baseline/provider/')}
    assert set(report['baseline_provider_locks']) <= baseline_files
    assert all(p.startswith('.github/provider-locks/') and p.endswith('.lock') for p in report['baseline_provider_locks'])
    # The separately sealed dependency observation is earlier than this provider
    # migration. Replaying it proves its own source/checks, not the new runtime.
    nested_files = {p.removeprefix('dependency-baseline/'): raw for p, raw in data.items()
                    if p.startswith('dependency-baseline/')}
    assert set(nested_files) == {'acceptance.json', 'verification.tar.xz', 'verify.py', 'verify_sources.py'}
    nested = json.loads(nested_files['acceptance.json'])
    assert nested['source_base'] == report['accepted_base']
    assert identity(nested_files['acceptance.json']) == report['dependency_baseline_acceptance']
    with tempfile.TemporaryDirectory(prefix='shardloom-dependency-replay-') as directory:
        temporary = Path(directory)
        for path, raw in nested_files.items():
            (temporary / safe(path)).write_bytes(raw)
        result = subprocess.run([sys.executable, '-B', str(temporary / 'verify.py')],
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120, check=False)
        assert result.returncode == 0, result.stdout.decode(errors='replace')
        assert json.loads(result.stdout)['status'] == 'passed'
    before = {p['name']: p for p in tomllib.loads(data['baseline/provider/Cargo.lock'].decode())['package']
              if p['name'] in ('jiff', 'jiff-core', 'jiff-static', 'tonic-prost')}
    after = {p['name']: p for p in tomllib.loads(current('Cargo.lock').decode())['package']
             if p['name'] in before}
    assert before == after and {p['version'] for p in after.values()} == {'0.2.39', '0.1.3', '0.14.9'}
    for path in ('shardloom-core/src/temporal_timezone.rs', '.github/workflows/codeql-analysis.yml',
                 '.github/workflows/scorecard.yml'):
        assert identity(current(path)) == nested['code_snapshot'][path]
    # Canonical Python core allows recorded optional/environment skips; the
    # ordinary installed-wheel acceptance cohort requires zero skips.
    core = provider(report['python_core_receipt'])
    core_stage = next(c for c in report['checks'] if c['receipt'].startswith('provider-python-core-'))
    core_log = data['observations/provider/' + core_stage['receipt'].removesuffix('.json') + '.log']
    match = re.search(rb'Ran (\d+) tests? in [0-9.]+s', core_log)
    assert match and core['status'] == 'passed' and core['failure_count'] == core['error_count'] == 0
    assert report['python_core'] == dict(status='passed', discovered_test_cases=core['test_count'],
        reported_test_runs=int(match[1]), skip_records=core['skipped_count'], failures=0, errors=0)
    installed = provider(report['installed_report'])
    prefix = str(PurePosixPath(report['installed_report']).parent) + '/'
    assert installed['status'] == installed['uninstall_status'] == 'passed'
    assert installed['integration'] == report['installed_package'] == provider(prefix + 'integration.json')
    integration = installed['integration']
    assert integration['status'] == 'passed' and integration['tests'] >= 386
    assert integration['failures'] == integration['errors'] == integration['skipped'] == 0
    assert installed['integrity']['licenses'] == notice_summary and installed['installed_load']['isolated_install']
    assert not installed['publication_attempted'] and not installed['performance_claim']
    assert [p['name'] for p in installed['steps']] == ['build', 'create_environment', 'install_optional_consumer',
        'install', 'load', 'integration', 'uninstall', 'verify_uninstalled']
    for step in installed['steps']:
        assert step['returncode'] == 0
        assert sha(data['observations/provider/' + prefix + step['log']]) == step['log_sha256']
    log = data['observations/provider/' + prefix + 'integration.log']
    names = re.findall(rb'^(test_\S+ \([^\n]+\)) \.\.\. ok$', log, re.MULTILINE)
    assert len(names) == len(set(names)) == integration['tests']
    assert re.search(rb'Ran ' + str(integration['tests']).encode() + rb' tests in [0-9.]+s\s+OK\s*$', log)
    assert any(b'test_temporal_calendar_workflows_match_all_five_surfaces ' in name for name in names)
    runtime = provider(report['runtime_identity'])
    assert runtime['status'] == 'passed'
    assert runtime['build_receipt'] == next(c['receipt'] for c in report['checks'] if c['receipt'].startswith('provider-release-build-'))
    assert runtime['build_receipt_sha256'] == sha(data['observations/provider/' + runtime['build_receipt']])
    assert {Path(p['path']).name: p['sha256'] for p in runtime['files']} == {
        'shardloom': installed['integrity']['cli_sha256'], 'lib_native.dylib': installed['integrity']['native_library_sha256']}
    for key in ('cli_sha256', 'native_library_sha256', 'wheel_sha256'):
        assert report['runtime_and_wheel'][key] == installed['integrity'][key]
    audit = provider(report['audit_report'])
    assert audit['release_gate'] and audit['fallback_dependency_absent']
    for key in ('advisory_status', 'cargo_audit_status', 'cargo_deny_status', 'license_policy_status',
                'pip_audit_status'):
        assert audit[key] == 'passed'
    for key in ('unknown_source_status', 'yanked_dependency_status'):
        assert audit[key] == 'covered_by_cargo_deny'
    assert report['audit_report_sha256'] == sha(data['observations/provider/' + report['audit_report']])
    # Inventory is explicit and immutable; linked missing logs remain limitations.
    failure_inventory = verify_failure_inventory(data.__getitem__)
    assert failure_inventory == provider('failure-observation-primary-verification-r3.json')
    for failure in report['original_observations']:
        path = 'observations/provider/' + failure['path']
        assert identity(data[path]) == {k: failure[k] for k in ('bytes', 'sha256')}
        observed = obj(path)
        assert observed.get('status') == failure['status']
        for item in failure['logs']:
            assert identity(data['observations/provider/' + item['path']]) == {k: item[k] for k in ('bytes', 'sha256')}
    archived_nonacceptance = set()
    for path, raw in data.items():
        if path.startswith('observations/provider/') and path.endswith('.json'):
            try:
                value = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            status = value.get('status') if isinstance(value, dict) else None
            if isinstance(status, str) and (status.startswith('failed') or status in
                {'interrupted', 'drift_detected', 'installed_candidate_not_accepted', 'prepared_not_accepted',
                 'prepared_needs_primary_review', 'simulation_passed'}):
                archived_nonacceptance.add(path.removeprefix('observations/provider/'))
    assert {p['path'] for p in report['original_observations']} == archived_nonacceptance
    assert not report['publication_attempted'] and not report['performance_claim']
    controls_path = packet / 'corruption-controls.json'
    if controls_path.exists():
        controls = json.loads(controls_path.read_bytes())
        assert controls['status'] == 'passed'
        observed = controls['receipt']
        assert observed['status'] == 'passed' and observed['positive_controls'] == 2 and observed['rejected_controls'] == 13
        expected_cases = ['original', 'relocated_source_only', 'unsealed_archive_corruption',
            'native_failed', 'installed_skips', 'omitted_original_failure', 'compiled_source_changed',
            'provider_archive_changed', 'standalone_provider_omitted', 'standalone_child_failed',
            'source_drift', 'audit_policy_failed', 'dependency_baseline_changed', 'required_stage_omitted',
            'unsafe_archive_path']
        assert [c['case'] for c in observed['cases']] == expected_cases
        assert set(observed['original_packet']) == {'acceptance.json', 'verification.tar.xz',
            'verify.py', 'verify_sources.py', 'verify_standalone.py'}
        for name, entry in observed['original_packet'].items():
            assert identity((packet / name).read_bytes()) == entry
        driver = data['observations/provider/tools/check_provider_packet_controls.py']
        assert observed['driver_sha256'] == sha(driver)
        for index, case in enumerate(observed['cases']):
            raw = controls['logs'][case['log']].encode()
            assert identity(raw) == case['log_identity']
            assert case['original_packet_unchanged']
            assert case['returncode'] == (0 if index < 2 else 1)
            assert case['expected'] == ('accepted' if index < 2 else 'rejected')
            if index < 2:
                assert json.loads(raw)['status'] == 'passed'
            else:
                assert b'AssertionError' in raw or b'KeyError' in raw
                assert case['mutations'] or case['case'] == 'unsealed_archive_corruption'
    documentation = {}
    documentation_path = packet / 'documentation.json'
    if documentation_path.exists():
        delivery = json.loads(documentation_path.read_bytes())
        assert delivery['status'] == 'documentation_only'
        assert delivery['validation_source_fingerprint_sha256'] == fingerprint(sources)
        documentation = delivery['changes']
        assert documentation and set(documentation) <= DOCUMENTATION
        for path, change in documentation.items():
            assert change['before'] == sources[path]
            assert set(change['after']) == {'bytes', 'sha256'}
            assert change['after'] != change['before'] and change['after']['bytes'] > 0
    if repo_root:
        # Added runtime/configuration files must not evade the frozen snapshot.
        # Removed tracked paths may disappear from ls-files after commit but
        # retain their explicit absence assertions below.
        listed = subprocess.check_output(['git', 'ls-files', '--cached', '--others',
                                         '--exclude-standard', '-z'], cwd=repo_root).decode().split('\0')
        actual = {p for p in listed if p and not p.startswith(EXCLUSIONS) and '.git' not in PurePosixPath(p).parts
                  and ((repo_root / p).exists() or (repo_root / p).is_symlink())}
        expected_paths = {p for p, entry in sources.items() if not entry.get('absent')}
        assert actual == expected_paths, {'added': sorted(actual - expected_paths), 'missing': sorted(expected_paths - actual)}
        for path, entry in sources.items():
            target = repo_root / path
            entry = documentation[path]['after'] if path in documentation else entry
            if entry.get('absent'):
                assert not target.exists() and not target.is_symlink(), path
            elif 'symlink' in entry:
                assert target.is_symlink() and str(target.readlink()) == entry['symlink'], path
            else:
                assert target.is_file() and not target.is_symlink() and identity(target.read_bytes()) == entry, path
    return dict(status='passed', archive_members=len(data), source_verification=source_result,
                checks=len(report['checks']), standalone_providers=standalone_result['provider_count'], installed_package=integration,
                original_nonacceptance_records=len(report['original_observations']),
                documentation_bindings=len(documentation), repository_source_comparison=bool(repo_root),
                scope='Offline source/graph/license and saved-check replay; no runtime tests or benchmarks rerun.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packet', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--repo-root', type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(args.packet, args.repo_root)))
