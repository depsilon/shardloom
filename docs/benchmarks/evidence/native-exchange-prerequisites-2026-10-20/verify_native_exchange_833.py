"""Replay the native-exchange prerequisite packet without claiming transport execution."""
import argparse
import hashlib
import json
import lzma
from pathlib import Path
import re
import subprocess
import tomllib


def match(raw, row):
    assert len(raw) == row['bytes'] and hashlib.sha256(raw).hexdigest() == row['sha256']


def git_sources(repository, commit, sources):
    proc = subprocess.Popen(['git', 'cat-file', '--batch'], cwd=repository,
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    try:
        for name, row in sources.items():
            proc.stdin.write((commit + ':' + name + '\n').encode())
            proc.stdin.flush()
            header = proc.stdout.readline().split()
            assert len(header) == 3 and header[1] == b'blob', name
            raw = proc.stdout.read(int(header[2]))
            assert proc.stdout.read(1) == b'\n'
            match(raw, row)
    finally:
        proc.stdin.close()
        assert proc.wait() == 0


def verify(path, repository, current_protocol=False):
    record = json.loads(path.read_text())
    assert record['schema'] == 'shardloom.native_encoded_exchange_prerequisite_acceptance.v1'
    assert record['status'] == 'prerequisite_review_recorded'
    assert record['completed_substeps'] == []
    for field in ('runtime_changed', 'workspace_dependencies_changed', 'transport_implemented',
                  'two_process_experiment_executed', 'independent_flight_sql_client_executed',
                  'performance_claim_allowed', 'fallback_attempted', 'external_engine_invoked'):
        assert record[field] is False
    raw = (path.parent / record['packet']['relative_path']).read_bytes()
    match(raw, record['packet'])
    packet = json.loads(lzma.decompress(raw))
    assert packet['schema'] == 'shardloom.native_exchange_prerequisites_packet.v1'
    members = packet['members']
    assert len(members) == record['packet']['members']
    for row in members.values():
        match(row['text'].encode(), row)

    def text(name):
        return members[name]['text']

    def data(name):
        return json.loads(text(name))

    git_sources(repository, record['source_commit'], record['sources'])
    bridge = record['compiled_source_bridge']
    closeout_raw = (repository / bridge['repository_path']).read_bytes()
    match(closeout_raw, bridge)
    closeout = json.loads(lzma.decompress(closeout_raw))
    source_member = closeout['members'][bridge['source_member']]
    match(source_member['text'].encode(), source_member)
    compiled = json.loads(source_member['text'])['source_files']
    assert len(compiled) == bridge['source_files_verified'] == 3027
    git_sources(repository, record['source_commit'], compiled)
    build_member = closeout['members'][bridge['verification_member']]
    match(build_member['text'].encode(), build_member)
    build = json.loads(build_member['text'])
    native = next(g for g in build['gates'] if g['name'] == 'native-tests')
    assert native['executable'] == bridge['executable']
    assert native['test_counts'] == dict(passed=3042, failed=0, ignored=30)
    assert native['fresh_native_artifact']['fresh'] is False

    expected = [
        'native-exchange-foundation-833-a1', 'native-exchange-ipc-833-a1',
        'native-exchange-scope-833-a1', 'native-exchange-provider-lock-833-a1',
        'native-exchange-provider-fetch-833-a1', 'native-exchange-provider-license-833-a1',
        'native-exchange-provider-advisory-833-a1', 'native-exchange-provider-update-833-a2',
        'native-exchange-provider-fetch-833-a2', 'native-exchange-provider-audit-833-a2',
        'native-exchange-provider-policy-833-a2',
    ]
    assert [g['name'] for g in record['checks']] == expected
    for index, gate in enumerate(record['checks']):
        receipt, log = data(gate['receipt']), text(gate['log'])
        assert receipt['head'] == record['source_commit']
        assert receipt['command'] == gate['command']
        assert receipt['direct_child_reaped'] and receipt['process_group_drained']
        assert hashlib.sha256(log.encode()).hexdigest() == receipt['log_sha256']
        failed = gate['name'] == 'native-exchange-provider-advisory-833-a1'
        assert receipt['status'] == gate['status'] == ('failed' if failed else 'passed')
        assert receipt['returncode'] == (1 if failed else 0)
        if index < 2:
            assert gate['tests_passed'] == 1
            assert re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;', log) == [('1', '0', '0')]
            assert gate['compiler_artifact']['executable'] == bridge['executable']['path']
            assert gate['compiler_artifact']['profile']['opt_level'] == '3'
            artifacts = []
            for line in log.splitlines():
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if item.get('reason') == 'compiler-artifact' and item.get('executable'):
                    artifacts.append(item)
            assert artifacts == [gate['compiler_artifact']]
    assert 'encoded_transfer_preserves_complete_values_validity_and_physical_trees' in record['checks'][0]['command']
    assert 'independent_ipc_fixtures_preserve_all_values_encodings_and_reusable_sources' in record['checks'][1]['command']
    scope = data('native-exchange-scope-833-report.json')
    assert scope['status'] == 'passed' and scope['blockers'] == []
    assert scope['claim_gate_status'] == 'not_claim_grade'

    graph = record['provider_graph']
    assert not graph['compiled'] and not graph['integrated']
    lock = tomllib.loads(text(graph['lock_member']))
    lock_rows = {(r['name'], r['version']): r for r in lock['package']}
    metadata = data(graph['metadata_member'])
    meta_rows = {(r['name'], r['version']): r for r in metadata['packages']}
    nodes = {r['id']: r for r in metadata['resolve']['nodes']}
    packages = graph['packages']
    assert len(packages) == 160 and len(lock_rows) == len(meta_rows) == 161
    keys = {(r['name'], r['version']) for r in packages}
    expected_keys = {k for k in lock_rows if k[0] != 'shardloom-native-exchange-provider-review'}
    assert len(keys) == len(packages) and keys == expected_keys
    for package in packages:
        key = (package['name'], package['version'])
        declared = tomllib.loads(text(package['manifest_member']))['package']
        assert declared['name'] == package['name'] and declared['version'] == package['version']
        assert package['license'] == declared['license'] == meta_rows[key]['license']
        assert package['registry_archive']['sha256'] == lock_rows[key]['checksum']
        assert lock_rows[key]['source'] == 'registry+https://github.com/rust-lang/crates.io-index'
        assert package['features'] == nodes[meta_rows[key]['id']]['features']
        assert not any(x in package['name'] for x in ('datafusion', 'duckdb', 'spark', 'polars', 'velox'))
    actual = {p['name']: p for p in packages if p['name'] in {'arrow-flight', 'tonic', 'tonic-prost', 'tokio-rustls', 'rustls'}}
    assert {n: p['version'] for n, p in actual.items()} == {
        'arrow-flight': '58.3.0', 'tonic': '0.14.6', 'tonic-prost': '0.14.6',
        'tokio-rustls': '0.26.6', 'rustls': '0.23.46'}
    assert actual['arrow-flight']['features'] == ['flight-sql', 'tls-ring']
    assert 'ring' in actual['rustls']['features']
    assert not set(actual['rustls']['features']).intersection({'aws_lc_rs', 'brotli', 'zlib'})
    assert set(graph['source_files']) == {n for n in members if n.startswith('provider-source/')}
    assert 'client_ca_root' in text('provider-source/tonic-0.14.6/src/transport/server/tls.rs')
    assert 'WebPkiClientVerifier::builder' in text('provider-source/tonic-0.14.6/src/transport/server/service/tls.rs')
    assert 'pub fn peer_certs' in text('provider-source/tonic-0.14.6/src/request.rs')
    workspace_lock = subprocess.check_output(['git', 'show', record['source_commit'] + ':Cargo.lock'], cwd=repository)
    assert not {p['name'] for p in tomllib.loads(workspace_lock.decode())['package']}.intersection(actual)

    initial = data('native-exchange-provider-advisory-833-a1.log')
    final = data('native-exchange-provider-audit-833-a2.log')
    ids = [r['advisory']['id'] for r in initial['vulnerabilities']['list']]
    assert ids == record['rejected_provider']['advisory_ids'] == [
        'RUSTSEC-2026-0346', 'RUSTSEC-2026-0347', 'RUSTSEC-2026-0348', 'RUSTSEC-2026-0349']
    assert initial['vulnerabilities']['count'] == 4
    assert all(r['package']['version'] == '0.23.45' for r in initial['vulnerabilities']['list'])
    assert final['vulnerabilities']['count'] == 0 and final['vulnerabilities']['found'] is False
    assert [r['advisory']['id'] for r in final['warnings']['unmaintained']] == ['RUSTSEC-2024-0436']
    assert record['advisory_database']['commit'] == initial['database']['last-commit']
    policy = [json.loads(line) for line in text('native-exchange-provider-policy-833-a2.log').splitlines() if line.startswith('{')]
    summary = [row['fields'] for row in policy if row['type'] == 'summary']
    assert len(summary) == 1
    assert all(summary[0][key]['errors'] == 0 for key in ('advisories', 'bans', 'licenses', 'sources'))
    rejected = data(record['rejected_provider']['manifest_and_lock_member'])
    for row in rejected['files'].values():
        match(row['text'].encode(), row)
    assert rejected['audit'] == initial
    old_lock = tomllib.loads(rejected['files']['Cargo.lock']['text'])
    assert next(p['version'] for p in old_lock['package'] if p['name'] == 'rustls') == '0.23.45'

    protocol = text(record['protocol']['repository_path']).encode()
    match(protocol, record['protocol'])
    if current_protocol:
        assert (repository / record['protocol']['repository_path']).read_bytes() == protocol
    body = protocol.decode()
    for token in ('planned and\ncurrently absent', 'native_encoded_exchange_protocol',
                  'native_encoded_exchange_two_process', 'native_encoded_exchange_client_interop',
                  'native-encoded-exchange-two-process.json', 'native-encoded-exchange-client-interop.json'):
        assert token in body
    assert len(record['remaining_prerequisites']) == 4
    return dict(status='passed', source_files_verified=3027, focused_tests=2,
                provider_registry_packages=160, preserved_failed_audits=1,
                selected_graph_vulnerabilities=0, existing_unmaintained_warnings=1,
                members_verified=len(members), two_process_experiment_executed=False,
                independent_flight_sql_client_executed=False, substeps_completed=0,
                physical_binary_revalidated=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--acceptance', type=Path, required=True)
    parser.add_argument('--repository', type=Path, required=True)
    parser.add_argument('--current-protocol', action='store_true')
    args = parser.parse_args()
    print(json.dumps(verify(args.acceptance, args.repository, args.current_protocol)), flush=True)


if __name__ == '__main__':
    main()
