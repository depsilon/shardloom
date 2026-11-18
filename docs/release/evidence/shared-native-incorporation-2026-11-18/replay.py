"""Verify the enclosing acceptance packet without loading an engine or running queries."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import re


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


CHECKS = (
    'queue-default-fmt-r1', 'queue-default-lint-r1', 'queue-default-tests-r1',
    'string-delivery-native-lint-r1', 'incorporation-format-r1',
    'incorporation-native-checked-r1', 'incorporation-inventory-r1',
    'incorporation-cli-build-r1', 'incorporation-extension-build-r1',
    'incorporation-focused-environment-r1', 'incorporation-focused-python-r1',
    'incorporation-focused-python-r2', 'incorporation-installed-wheel-r1',
    'incorporation-public-cli-r1', 'incorporation-all-features-r1',
    'incorporation-minimal-r1', 'incorporation-msrv-minimal-r1',
    'incorporation-msrv-native-r1',
)
TEST_ONLY = {
    'shardloom-vortex/src/local_primitive_pair_wait_attribution_tests.rs',
    'shardloom-vortex/src/local_primitive_string_delivery_tests.rs',
    'shardloom-vortex/src/local_primitive_string_wait_attribution_tests.rs',
    'shardloom-vortex/src/local_primitives.rs',
    'shardloom-vortex/src/local_primitives/aggregate_chunk_job_observation.rs',
}
STRING_MODULE = '''#[cfg(all(
    test,
    feature = "vortex-local-primitives",
    feature = "vortex-write",
    unix
))]
#[path = "local_primitive_string_wait_attribution_tests.rs"]
mod string_wait_attribution_tests;
'''


def code_path(name):
    return (name.endswith('.rs') or Path(name).name in {'Cargo.toml', 'Cargo.lock'}
            or name.startswith('python/src/'))


def validate(payload):
    assert payload['schema_version'] == 'shardloom.incorporation_acceptance_raw.v1'
    files, sources = payload['files'], payload['sources']
    assert {n: sha(t.encode()) for n, t in files.items()} == payload['file_sha256']
    assert {n: sha(t.encode()) for n, t in sources.items()} == payload['source_sha256']
    final = payload['final_source_inventory']
    for name, text in sources.items():
        assert final[name] == {'bytes': len(text.encode()), 'sha256': sha(text.encode())}, name
    checks = []
    for name in CHECKS:
        receipt = json.loads(files[name + '.json'])
        assert receipt['sources_unchanged'] and receipt['process_group_drained'], name
        expected = 'failed' if name == 'incorporation-focused-python-r1' else 'passed'
        assert receipt['status'] == expected, name
        assert receipt['returncode'] == (1 if expected == 'failed' else 0), name
        differences = [n for n in sorted(set(final) | set(receipt['source_files']))
                       if final.get(n) != receipt['source_files'].get(n)]
        code_differences = [n for n in differences if code_path(n)]
        if name.startswith('queue-default-'):
            assert set(code_differences) == TEST_ONLY, (name, code_differences)
            parent = 'shardloom-vortex/src/local_primitives.rs'
            assert sources[parent].count(STRING_MODULE) == 1
            assert sha(sources[parent].replace(STRING_MODULE, '').encode()) == receipt['source_files'][parent]['sha256']
            jobs = sources['shardloom-vortex/src/local_primitives/aggregate_chunk_jobs.rs']
            assert '#[cfg(test)]\n#[path = "aggregate_chunk_job_observation.rs"]' in jobs
        else:
            assert code_differences == [], (name, code_differences)
        log_name = 'logs/' + name + '.log'
        log = files[log_name]
        record = dict(name=name, status=expected, command=receipt['command'],
                      receipt_sha256=sha(files[name + '.json'].encode()),
                      log_sha256=sha(log.encode()), sources_unchanged=True,
                      process_group_drained=True, whole_repository_differences=differences,
                      compiled_source_differences=code_differences)
        summaries = re.findall(r'test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;', log)
        if summaries:
            record['tests'] = dict(targets=len(summaries),
                passed=sum(int(x[0]) for x in summaries), failed=sum(int(x[1]) for x in summaries),
                ignored=sum(int(x[2]) for x in summaries))
            assert record['tests']['failed'] == 0
        if 'msrv' in name:
            assert receipt['rustc_version'].startswith('rustc 1.96.0 '), name
        checks.append(record)
    native = next(row['tests'] for row in checks if row['name'] == 'incorporation-native-checked-r1')
    assert native == dict(targets=17, passed=4432, failed=0, ignored=41), native
    default = next(row['tests'] for row in checks if row['name'] == 'queue-default-tests-r1')
    assert default == dict(targets=108, passed=3351, failed=0, ignored=0), default
    focused = json.loads(files['incorporation-focused-python-r2.result.json'])
    original = json.loads(files['incorporation-focused-python-r1.result.json'])
    assert {k: focused[k] for k in ('status', 'tests', 'errors', 'failures', 'skipped')} == {
        'status': 'passed', 'tests': 34, 'errors': 0, 'failures': 0, 'skipped': 0}
    assert {k: original[k] for k in ('status', 'tests', 'errors', 'failures', 'skipped')} == {
        'status': 'failed', 'tests': 34, 'errors': 0, 'failures': 2, 'skipped': 0}
    prior_name = 'python/tests/test_native_analytical_workflows_binding.py'
    prior = payload['prior_source_revisions'][prior_name]
    original_receipt = json.loads(files['incorporation-focused-python-r1.json'])
    assert sha(prior.encode()) == original_receipt['source_files'][prior_name]['sha256']
    wheel = json.loads(files['installed-wheel-r1/report.json'])
    assert wheel['status'] == wheel['uninstall_status'] == 'passed'
    assert wheel['publication_attempted'] is False and wheel['installation_performed'] is True
    assert wheel['integration'] == dict(tests=466, failures=0, errors=0, skipped=0, status='passed')
    for step in wheel['steps']:
        assert step['returncode'] == 0, step
        assert sha(files['installed-wheel-r1/' + step['log']].encode()) == step['log_sha256']
    for label, product, integrity in [('cli', 'cli-release', 'cli_sha256'),
                                       ('extension', 'native-extension', 'native_library_sha256')]:
        manifest = json.loads(files['runtime-r1-' + label + '/manifest.json'])
        assert manifest['product'] == product and manifest['profile'] == 'release'
        assert len(manifest['files']) == 1
        artifact = next(iter(manifest['files'].values()))
        assert artifact['sha256'] == wheel['integrity'][integrity]
        assert focused[label] == original[label] == manifest['files']
    matrix = json.loads(files['incorporation-propagation-evidence-r1.json'])
    assert matrix['status'] == 'passed' and matrix['unique_passing_tests'] == 107
    assert matrix['native_query_executions_by_reconciliation'] == 0 and len(matrix['families']) == 7
    native_log = files['logs/incorporation-native-checked-r1.log']
    assert sha(native_log.encode()) == matrix['native_log_sha256']
    assert sha(files['incorporation-native-checked-r1.json'].encode()) == matrix['native_receipt_sha256']
    seen = set()
    for family in matrix['families']:
        assert not family['blanket_coverage'] and not family['new_feedback_application']
        for source in family['source_checks']:
            assert source['sha256'] == final[source['path']]['sha256']
            for test in source['tests']:
                assert test['status'] in {'ok', 'ignored'}
                assert ('test ' + test['name'] + ' ... ' + test['status']) in native_log
                if test['status'] == 'ok':
                    seen.add(test['name'])
    assert len(seen) == 107
    return dict(schema_version='shardloom.incorporation_acceptance.v1',
        status='local_acceptance_passed', checks=checks, native_tests=native,
        default_tests=default, focused_python=focused, installed_tests=wheel['integration'],
        wheel_sha256=wheel['integrity']['wheel_sha256'],
        propagation_families=7, propagation_unique_tests=107,
        finite_feedback='screen_complete_no_candidate_admitted_implementation_deferred',
        remote_refresh='deferred_to_existing_io_foundry_owner',
        deep_traversal='deferred_after_foundry_public_limit_unchanged',
        hosted_acceptance='pending_required_checks', publication_attempted=False,
        new_performance_claim=False, new_native_query_executions_by_replay=0,
        source_sha256=payload['source_sha256'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('packet', type=Path)
    parser.add_argument('--repository', type=Path)
    args = parser.parse_args()
    packed = (args.packet / 'observations.json.gz').read_bytes()
    expected = json.loads((args.packet / 'validation.json').read_text())
    assert expected['archive'] == dict(path='observations.json.gz', bytes=len(packed), sha256=sha(packed))
    payload = json.loads(gzip.decompress(packed))
    observed = validate(payload)
    assert observed == {k: v for k, v in expected.items() if k != 'archive'}
    if args.repository:
        for name, digest in payload['source_sha256'].items():
            assert sha((args.repository / name).read_bytes()) == digest, name
        for ref in payload['predecessor_packets']:
            assert sha((args.repository / ref['path']).read_bytes()) == ref['sha256'], ref['path']
    print(json.dumps(dict(status='passed', checks=len(observed['checks']),
                         installed_tests=observed['installed_tests']['tests'],
                         families=7, unique_matrix_tests=107,
                         source_files=len(payload['sources']), native_queries=0)))


if __name__ == '__main__':
    main()
