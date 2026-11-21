"""Verify the Chrysalis partial-coverage acceptance packet without loading an engine or running queries."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import re


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


PASSING_CHECKS = (
    'coverage-format-r6',
    'coverage-native-cli-lint-r6',
    'coverage-default-lint-r5',
    'coverage-default-tests-r5',
    'coverage-native-full-r5',
    'coverage-native-inventory-r5',
    'coverage-public-cli-r5',
    'coverage-all-features-r5',
    'coverage-minimal-r5',
    'coverage-msrv-minimal-r5',
    'coverage-msrv-native-r5',
    'coverage-cli-build-r2',
    'coverage-extension-build-r2',
    'coverage-focused-python-r2',
    'coverage-installed-wheel-r2',
)

PRESERVED_FAILURES = (
    ('coverage-native-full-r1', 101),
    ('coverage-native-cli-lint-r3', 101),
    ('coverage-public-cli-r3', 101),
    ('coverage-installed-wheel-r1', 1),
    ('exposure-r1', 1),
)

CANDIDATE_TEST_AND_BASELINE = {
    'python/tests/test_native_columnar_coverage_binding.py',
    '.github/ci-native-baseline.json',
}


def code_path(name):
    return (
        name.endswith('.rs')
        or Path(name).name in {'Cargo.toml', 'Cargo.lock'}
        or name.startswith('python/src/')
        or name in CANDIDATE_TEST_AND_BASELINE
    )


def validate(payload):
    assert payload['schema_version'] == 'shardloom.chrysalis_acceptance_raw.v1'
    files, sources = payload['files'], payload['sources']
    assert {n: sha(t.encode()) for n, t in files.items()} == payload['file_sha256']
    assert {n: sha(t.encode()) for n, t in sources.items()} == payload['source_sha256']
    final = payload['final_source_inventory']
    for name, text in sources.items():
        assert final[name] == {'bytes': len(text.encode()), 'sha256': sha(text.encode())}, name

    checks = []
    passing_receipt_sha = {}
    for name in PASSING_CHECKS:
        raw_receipt = files[name + '.json']
        receipt = json.loads(raw_receipt)
        assert receipt['status'] == 'passed', name
        assert receipt['returncode'] == 0, name
        assert receipt['sources_unchanged'] and receipt['process_group_drained'], name
        differences = [
            n
            for n in sorted(set(final) | set(receipt['source_files']))
            if final.get(n) != receipt['source_files'].get(n)
        ]
        code_differences = [n for n in differences if code_path(n)]
        assert code_differences == [], (name, code_differences)
        log = files['logs/' + name + '.log']
        receipt_digest = sha(raw_receipt.encode())
        passing_receipt_sha[name + '.json'] = receipt_digest
        record = dict(
            name=name,
            status='passed',
            command=receipt['command'],
            receipt_sha256=receipt_digest,
            log_sha256=sha(log.encode()),
            sources_unchanged=True,
            process_group_drained=True,
            whole_repository_differences=differences,
            compiled_source_differences=code_differences,
        )
        summaries = re.findall(
            r'test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;',
            log,
        )
        if summaries:
            record['tests'] = dict(
                targets=len(summaries),
                passed=sum(int(x[0]) for x in summaries),
                failed=sum(int(x[1]) for x in summaries),
                ignored=sum(int(x[2]) for x in summaries),
            )
            assert record['tests']['failed'] == 0, name
        if '-msrv-' in name:
            assert receipt['rustc_version'].startswith('rustc 1.96.0 '), name
        checks.append(record)

    inventory = json.loads(files['coverage-native-inventory-r5.result.json'])
    assert inventory['status'] == 'passed'
    assert inventory['tests'] == 4474 and inventory['ignored'] == 42
    assert len(inventory['partitions']) == 4 and len(inventory['new_family']) == 17
    assert inventory['native_query_executions'] == 0
    native_summary = next(r['tests'] for r in checks if r['name'] == 'coverage-native-full-r5')
    assert native_summary == dict(targets=17, passed=4449, failed=0, ignored=42), native_summary
    default_summary = next(r['tests'] for r in checks if r['name'] == 'coverage-default-tests-r5')
    assert default_summary == dict(targets=108, passed=3351, failed=0, ignored=0), default_summary
    cli_summary = next(r['tests'] for r in checks if r['name'] == 'coverage-public-cli-r5')
    assert cli_summary == dict(targets=8, passed=1257, failed=0, ignored=0), cli_summary

    focused = json.loads(files['coverage-focused-python-r2.result.json'])
    assert {k: focused[k] for k in ('status', 'tests', 'errors', 'failures', 'skipped')} == {
        'status': 'passed',
        'tests': 3,
        'errors': 0,
        'failures': 0,
        'skipped': 0,
    }

    wheel = json.loads(files['installed-wheel-r2/report.json'])
    integration = json.loads(files['installed-wheel-r2/integration.json'])
    assert wheel['status'] == wheel['uninstall_status'] == 'passed'
    assert wheel['publication_attempted'] is False and wheel['installation_performed'] is True
    expected_integration = dict(tests=468, failures=0, errors=0, skipped=0, status='passed')
    assert wheel['integration'] == integration == expected_integration
    for step in wheel['steps']:
        assert step['returncode'] == 0, step
        assert sha(files['installed-wheel-r2/' + step['log']].encode()) == step['log_sha256'], step

    cli_manifest = json.loads(files['runtime-r2-cli/manifest.json'])
    ext_manifest = json.loads(files['runtime-r2-extension/manifest.json'])
    assert cli_manifest['product'] == 'cli-release' and cli_manifest['profile'] == 'release'
    assert ext_manifest['product'] == 'native-extension' and ext_manifest['profile'] == 'release'
    assert len(cli_manifest['files']) == 1 and len(ext_manifest['files']) == 1
    cli_info = next(iter(cli_manifest['files'].values()))
    ext_info = next(iter(ext_manifest['files'].values()))
    assert cli_info['sha256'] == wheel['integrity']['cli_sha256']
    assert ext_info['sha256'] == wheel['integrity']['native_library_sha256']
    assert focused['cli'] == cli_manifest['files']
    assert focused['extension'] == ext_manifest['files']

    preserved = []
    for name, expected_rc in PRESERVED_FAILURES:
        raw_receipt = files[name + '.json']
        receipt = json.loads(raw_receipt)
        assert receipt['status'] == 'failed', name
        assert receipt['returncode'] == expected_rc, (name, receipt['returncode'])
        assert receipt['sources_unchanged'] and receipt['process_group_drained'], name
        if name == 'exposure-r1':
            exp_manifest = files['exposure-manifest-r1.json']
            assert receipt['manifest_sha256'] == sha(exp_manifest.encode())
            snapshot_count = 0
        else:
            snapshots = payload['prior_source_revisions'][name]
            assert snapshots, name
            for rel_path, text in snapshots.items():
                assert sha(text.encode()) == receipt['source_files'][rel_path]['sha256'], (name, rel_path)
            snapshot_count = len(snapshots)
            if name == 'coverage-installed-wheel-r1':
                w1 = json.loads(files['installed-wheel-r1/report.json'])
                i1 = json.loads(files['installed-wheel-r1/integration.json'])
                assert w1['status'] == 'failed' and i1['status'] == 'failed'
                assert i1['failures'] == 29 and i1['tests'] == 468
        preserved.append(
            dict(
                name=name,
                status='failed',
                returncode=expected_rc,
                receipt_sha256=sha(raw_receipt.encode()),
                snapshot_files=snapshot_count,
            )
        )

    bindings = json.loads(files['comparison-runtime-bindings-r1.json'])
    manifest_raw = files['exposure-manifest-r2.json']
    manifest = json.loads(manifest_raw)
    protocol_raw = files['comparison-protocol-r1.md']
    assert bindings['manifest_sha256'] == sha(manifest_raw.encode())
    assert bindings['protocol_sha256'] == sha(protocol_raw.encode())
    for driver_name, expected_digest in bindings['drivers'].items():
        assert sha(files[driver_name].encode()) == expected_digest, driver_name
    bound_names = []
    for entry in bindings['validation_receipts']:
        receipt_name = Path(entry['path']).name
        assert receipt_name in passing_receipt_sha, receipt_name
        assert entry['sha256'] == passing_receipt_sha[receipt_name], receipt_name
        bound_names.append(receipt_name)
    assert bound_names == [name + '.json' for name in PASSING_CHECKS]

    base_ext_manifest = json.loads(files['base/runtime-r1-extension/manifest.json'])
    base_ext_build = json.loads(files['base/incorporation-extension-build-r1.json'])
    assert base_ext_build['status'] == 'passed'
    assert bindings['roles']['direct'] == bindings['roles']['full-preparation']
    assert (
        bindings['roles']['direct']['artifact_manifest_sha256']
        == sha(files['base/runtime-r1-extension/manifest.json'].encode())
    )
    assert (
        bindings['roles']['direct']['build_receipt_sha256']
        == sha(files['base/incorporation-extension-build-r1.json'].encode())
    )
    base_ext_info = next(iter(base_ext_manifest['files'].values()))
    assert bindings['roles']['direct']['binary_sha256'] == base_ext_info['sha256']

    assert (
        bindings['roles']['partial']['artifact_manifest_sha256']
        == sha(files['runtime-r2-extension/manifest.json'].encode())
    )
    assert (
        bindings['roles']['partial']['build_receipt_sha256']
        == passing_receipt_sha['coverage-extension-build-r2.json']
    )
    assert bindings['roles']['partial']['binary_sha256'] == ext_info['sha256']

    bindings_digest = sha(files['comparison-runtime-bindings-r1.json'].encode())
    role_summaries = {}
    for role in ('direct', 'full-preparation', 'partial'):
        raw_comp = files[f'comparison-{role}-r1.json']
        comp = json.loads(raw_comp)
        assert comp['status'] == 'passed' and comp['returncode'] == 0, role
        assert comp['process_group_drained'] is True, role
        assert (
            comp['sources_unchanged']
            and comp['binary_unchanged']
            and comp['bindings_unchanged']
            and comp['inputs_unchanged']
        ), role
        assert comp['bindings_sha256'] == bindings_digest, role
        assert comp['manifest_sha256'] == bindings['manifest_sha256'], role
        assert comp['protocol_sha256'] == bindings['protocol_sha256'], role
        assert comp['runtime'] == bindings['roles'][role], role
        assert comp['source_files'] == final, role
        assert [obs['id'] for obs in comp['observations']] == [
            req['id'] for req in manifest['requests']
        ]
        assert len(comp['groups']) == 10
        assert len(comp['artifacts']) == (10 if role == 'full-preparation' else 0)
        engine_sum = 0
        for obs, req in zip(comp['observations'], manifest['requests'], strict=True):
            assert obs['rows'] == req['expected_rows'], (role, req['id'])
            assert obs['schema'] == req['expected_schema'], (role, req['id'])
            fields = obs['fields']
            assert fields['fallback_attempted'] == 'false', (role, req['id'])
            assert fields['external_engine_invoked'] == 'false', (role, req['id'])
            engine_sum += int(fields['native_query_execution_nanos'])
            if role == 'partial':
                retained = int(fields['native_source_coverage_retained_bytes'])
                assert 0 <= retained <= min(32 << 20, manifest['memory_bytes'] // 16), req['id']
                assert 'prior_successful_operation_only' in fields['native_source_coverage_scope']
                if req['id'] == 'numeric-overlap-3':
                    assert fields['native_source_demand_rows_decoded'] == '0'
                    assert fields['native_source_demand_data_read_bytes'] == '0'
                    assert fields['native_source_coverage_field_rows'] == '262144'
        assert engine_sum == comp['engine_nanos'], role
        role_summaries[role] = dict(
            receipt_sha256=sha(raw_comp.encode()),
            engine_nanos=comp['engine_nanos'],
            public_query_terminal_nanos=sum(
                g['public_query_terminal_nanos'] for g in comp['groups']
            ),
            preparation_nanos=sum(g['preparation_nanos'] for g in comp['groups']),
            complete_public_workflow_nanos=sum(
                g['complete_public_workflow_nanos'] for g in comp['groups']
            ),
            queries=len(comp['observations']),
            groups=len(comp['groups']),
        )

    assert role_summaries['partial']['engine_nanos'] < role_summaries['direct']['engine_nanos']
    return dict(
        schema_version='shardloom.chrysalis_acceptance.v1',
        status='local_acceptance_passed',
        decision='locally_retained',
        checks=checks,
        preserved_failures=preserved,
        native_tests=native_summary,
        focused_python=focused,
        installed_tests=wheel['integration'],
        wheel_sha256=wheel['integrity']['wheel_sha256'],
        comparison=role_summaries,
        hosted_acceptance='pending_required_checks',
        publication_attempted=False,
        new_performance_claim=False,
        new_native_query_executions_by_replay=0,
        source_sha256=payload['source_sha256'],
        predecessor_packets=payload['predecessor_packets'],
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('packet', type=Path)
    parser.add_argument('--repository', type=Path)
    args = parser.parse_args()
    packed = (args.packet / 'observations.json.gz').read_bytes()
    expected = json.loads((args.packet / 'validation.json').read_text())
    assert expected['archive'] == dict(
        path='observations.json.gz', bytes=len(packed), sha256=sha(packed)
    )
    payload = json.loads(gzip.decompress(packed))
    observed = validate(payload)
    assert observed == {k: v for k, v in expected.items() if k != 'archive'}
    if args.repository:
        for name, digest in payload['source_sha256'].items():
            assert sha((args.repository / name).read_bytes()) == digest, name
        for ref in payload['predecessor_packets']:
            assert sha((args.repository / ref['path']).read_bytes()) == ref['sha256'], ref['path']
    print(
        json.dumps(
            dict(
                status='passed',
                decision=observed['decision'],
                checks=len(observed['checks']),
                preserved_failures=len(observed['preserved_failures']),
                installed_tests=observed['installed_tests']['tests'],
                direct_engine_nanos=observed['comparison']['direct']['engine_nanos'],
                partial_engine_nanos=observed['comparison']['partial']['engine_nanos'],
                source_files=len(payload['sources']),
                native_queries=0,
            )
        )
    )


if __name__ == '__main__':
    main()
