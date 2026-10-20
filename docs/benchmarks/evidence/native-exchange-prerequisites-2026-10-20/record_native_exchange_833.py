"""Bind the finite native-exchange prerequisite review to replayable evidence."""
import hashlib
import json
import lzma
from pathlib import Path
import re
import subprocess
import tomllib

E = Path(__file__).resolve().parent
W = Path('/Users/dylan/.codex/worktrees/public-docs-api/shardloom-local-repo')
R = Path('/Users/dylan/.cargo/registry')
PROTOCOL = 'docs/architecture/native-encoded-exchange-protocol.md'
DEST = W / 'docs/benchmarks/evidence/native-exchange-prerequisites-2026-10-20'
GATES = [
    'native-exchange-foundation-833-a1', 'native-exchange-ipc-833-a1',
    'native-exchange-scope-833-a1', 'native-exchange-provider-lock-833-a1',
    'native-exchange-provider-fetch-833-a1', 'native-exchange-provider-license-833-a1',
    'native-exchange-provider-advisory-833-a1', 'native-exchange-provider-update-833-a2',
    'native-exchange-provider-fetch-833-a2', 'native-exchange-provider-audit-833-a2',
    'native-exchange-provider-policy-833-a2',
]
SOURCES = [
    'Cargo.toml', 'Cargo.lock', 'deny.toml', 'shardloom-vortex/Cargo.toml',
    'shardloom-vortex/src/resident_memory_source.rs',
    'shardloom-vortex/src/resident_memory_ipc.rs',
    'shardloom-vortex/src/resident_memory_ipc_schema.rs',
    'shardloom-vortex/src/resident_memory_ipc_validation.rs',
    'shardloom-vortex/src/resident_memory_ipc_tests.rs',
    'shardloom-vortex/src/resident_memory_ipc_corruption_tests.rs',
    'shardloom-vortex/src/resident_memory_encoded_transfer.rs',
    'shardloom-vortex/src/resident_memory_encoded_transfer_tests.rs',
    'docs/rfcs/0035-rest-event-remote-api-surface.md',
    'docs/rfcs/0044-resident-runtime-resource-ownership.md',
    'docs/architecture/native-encoded-interchange-2026-10-12.md',
    'docs/architecture/native-roadmap-verification-contracts-2026-10-18.md',
    'scripts/check_v1_inclusion_scope.py',
]
PROVIDER_SOURCES = {
    'arrow-flight-58.3.0': ['Cargo.toml', 'Cargo.toml.orig', 'src/arrow.flight.protocol.rs',
                            'src/client.rs', 'src/encode.rs', 'src/sql/client.rs', 'src/sql/server.rs'],
    'tonic-0.14.6': ['Cargo.toml', 'src/request.rs', 'src/service/interceptor.rs',
                    'src/transport/server/tls.rs', 'src/transport/server/service/tls.rs',
                    'src/transport/channel/tls.rs', 'src/transport/server/mod.rs',
                    'src/transport/channel/endpoint.rs'],
    'tokio-rustls-0.26.6': ['Cargo.toml'],
    'rustls-0.23.46': ['Cargo.toml'],
    'vortex-ipc-0.85.0': ['Cargo.toml', 'src/messages/encoder.rs', 'src/messages/decoder.rs'],
}


def identity(raw):
    return dict(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())


def member(path):
    raw = path.read_bytes()
    return dict(identity(raw), path=str(path), text=raw.decode())


def check(raw, row):
    assert identity(raw) == {key: row[key] for key in ('bytes', 'sha256')}


def git_blobs(commit, expected):
    proc = subprocess.Popen(['git', 'cat-file', '--batch'], cwd=W,
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    try:
        for name, row in expected.items():
            proc.stdin.write((commit + ':' + name + '\n').encode())
            proc.stdin.flush()
            header = proc.stdout.readline().split()
            assert len(header) == 3 and header[1] == b'blob', (name, header)
            raw = proc.stdout.read(int(header[2]))
            assert proc.stdout.read(1) == b'\n'
            check(raw, row)
            check((W / name).read_bytes(), row)
    finally:
        proc.stdin.close()
        assert proc.wait() == 0


def main():
    commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=W, text=True).strip()
    assert commit.startswith('95a7b56f')
    assert not subprocess.check_output(['git', 'diff', '--name-only', '--', '.', ':!docs'], cwd=W)
    closeout_name = 'docs/benchmarks/evidence/region-closeout-831.json.xz'
    closeout_raw = (W / closeout_name).read_bytes()
    assert hashlib.sha256(closeout_raw).hexdigest() == '1f293cb96ade31d6c4c5b4fbcb38983cf59c1fe29038d12fdf516309ed8a0c26'
    closeout = json.loads(lzma.decompress(closeout_raw))
    source_member = closeout['members']['region-restoration-831b-source.json']
    check(source_member['text'].encode(), source_member)
    compiled = json.loads(source_member['text'])['source_files']
    assert len(compiled) == 3027
    git_blobs(commit, compiled)
    build_member = closeout['members']['region-restoration-831b-verified.json']
    check(build_member['text'].encode(), build_member)
    build = json.loads(build_member['text'])
    executable = next(g for g in build['gates'] if g['name'] == 'native-tests')['executable']
    check(Path(executable['path']).read_bytes(), executable)

    members = {}
    for name in [
        'record_native_exchange_833.py', 'verify_native_exchange_833.py',
        'run_guarded.py', 'native_workload_guard_816.py',
        'native-exchange-prerequisites-830.json',
        'native-exchange-inventory-source-verification-830.json',
        'native-exchange-official-contract-inventory-831.json',
        'native-exchange-transport-provider-inventory-833.json',
        'native-exchange-provider-rejected-833-a1.json',
        'native-exchange-provider-metadata-833-a2.json',
        'native-exchange-scope-833-report.json',
        'native-exchange-provider-833/Cargo.toml',
        'native-exchange-provider-833/Cargo.lock',
        'native-exchange-provider-833/review.rs',
    ]:
        members[name] = member(E / name)
    checks = []
    for name in GATES:
        for suffix in ('.json', '.log'):
            members[name + suffix] = member(E / (name + suffix))
        receipt = json.loads(members[name + '.json']['text'])
        assert receipt['head'] == commit
        assert receipt['direct_child_reaped'] and receipt['process_group_drained']
        assert receipt['log_sha256'] == members[name + '.log']['sha256']
        failed = name == 'native-exchange-provider-advisory-833-a1'
        assert receipt['returncode'] == (1 if failed else 0)
        assert receipt['status'] == ('failed' if failed else 'passed')
        row = dict(name=name, command=receipt['command'], status=receipt['status'],
                   receipt=name + '.json', log=name + '.log')
        if name in GATES[:2]:
            log = members[name + '.log']['text']
            counts = re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;', log)
            assert counts == [('1', '0', '0')]
            artifacts = []
            for line in log.splitlines():
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if item.get('reason') == 'compiler-artifact' and item.get('executable'):
                    artifacts.append(item)
            assert len(artifacts) == 1 and artifacts[0]['executable'] == executable['path']
            assert artifacts[0]['profile']['opt_level'] == '3'
            row.update(tests_passed=1, compiler_artifact=artifacts[0])
        checks.append(row)

    metadata = json.loads(members['native-exchange-provider-metadata-833-a2.json']['text'])
    lock = tomllib.loads(members['native-exchange-provider-833/Cargo.lock']['text'])
    nodes = {row['id']: row for row in metadata['resolve']['nodes']}
    packages = []
    lock_packages = {(p['name'], p['version']): p for p in lock['package']}
    for package in metadata['packages']:
        if package['source'] is None:
            assert package['name'] == 'shardloom-native-exchange-provider-review'
            continue
        key = (package['name'], package['version'])
        entry = lock_packages[key]
        assert entry['source'] == 'registry+https://github.com/rust-lang/crates.io-index'
        archives = list((R / 'cache').glob('*/' + '-'.join(key) + '.crate'))
        assert len(archives) == 1
        archive_identity = identity(archives[0].read_bytes())
        assert archive_identity['sha256'] == entry['checksum']
        manifest = Path(package['manifest_path'])
        manifest_key = 'provider-manifests/' + '-'.join(key) + '/Cargo.toml'
        members[manifest_key] = member(manifest)
        packages.append(dict(name=key[0], version=key[1], license=package['license'],
                             registry_archive=archive_identity, manifest_member=manifest_key,
                             features=nodes[package['id']]['features']))
    assert len(packages) == 160
    assert not any(any(x in row['name'] for x in ('datafusion', 'duckdb', 'spark', 'polars', 'velox')) for row in packages)
    for package, names in PROVIDER_SOURCES.items():
        roots = list((R / 'src').glob('*/' + package))
        assert len(roots) == 1
        for name in names:
            members['provider-source/' + package + '/' + name] = member(roots[0] / name)
    sources = {name: identity((W / name).read_bytes()) for name in SOURCES}
    git_blobs(commit, sources)
    protocol = member(W / PROTOCOL)
    members[PROTOCOL] = protocol
    initial = json.loads(members['native-exchange-provider-advisory-833-a1.log']['text'])
    final = json.loads(members['native-exchange-provider-audit-833-a2.log']['text'])
    assert initial['vulnerabilities']['count'] == 4 and final['vulnerabilities']['count'] == 0
    assert [r['advisory']['id'] for r in final['warnings']['unmaintained']] == ['RUSTSEC-2024-0436']
    database = Path('/Users/dylan/.cargo/advisory-db')
    database_head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=database, text=True).strip()
    assert database_head == initial['database']['last-commit']
    scope = json.loads(members['native-exchange-scope-833-report.json']['text'])
    assert scope['status'] == 'passed' and scope['blockers'] == []
    packet = dict(schema='shardloom.native_exchange_prerequisites_packet.v1', members=members)
    packet_raw = lzma.compress(json.dumps(packet, sort_keys=True, separators=(',', ':')).encode())
    packet_name = 'native-encoded-exchange-prerequisites.json.xz'
    record = dict(
        schema='shardloom.native_encoded_exchange_prerequisite_acceptance.v1',
        status='prerequisite_review_recorded', source_commit=commit, sources=sources,
        runtime_changed=False, workspace_dependencies_changed=False,
        completed_substeps=[], transport_implemented=False, two_process_experiment_executed=False,
        independent_flight_sql_client_executed=False, performance_claim_allowed=False,
        fallback_attempted=False, external_engine_invoked=False,
        protocol=dict(repository_path=PROTOCOL, **identity(protocol['text'].encode())),
        packet=dict(relative_path=packet_name, **identity(packet_raw), members=len(members)),
        compiled_source_bridge=dict(repository_path=closeout_name, **identity(closeout_raw),
            source_member='region-restoration-831b-source.json',
            verification_member='region-restoration-831b-verified.json',
            source_files_verified=3027, executable=executable),
        checks=checks, provider_graph=dict(packages=packages, compiled=False, integrated=False,
            metadata_member='native-exchange-provider-metadata-833-a2.json',
            manifest_member='native-exchange-provider-833/Cargo.toml',
            lock_member='native-exchange-provider-833/Cargo.lock',
            source_files=[name for name in members if name.startswith('provider-source/')]),
        advisory_database=dict(commit=database_head, initial_observation=initial['database'],
            note='Final --no-fetch audit omits DB metadata; verified unchanged local database HEAD.'),
        rejected_provider=dict(rustls='0.23.45', advisory_ids=[r['advisory']['id'] for r in initial['vulnerabilities']['list']],
            manifest_and_lock_member='native-exchange-provider-rejected-833-a1.json'),
        selected_provider=dict(rustls='0.23.46', vulnerabilities=0,
            existing_unmaintained_warning='RUSTSEC-2024-0436', new_policy_exceptions=0),
        corrections=[
            'Primary Cargo fetch obtained registry archives after the earlier web-source download limitation; source checksums are now verified.',
            'The isolated lock now resolves the candidate transport graph; this does not add it to the production workspace.',
            'Current Vortex documentation navigation shows 0.90 draft/0.89 released; pinned 0.85 codec source remains authoritative.',
        ],
        remaining_prerequisites=[
            'Freeze extension schema, complete independent fixtures, exact matched encoder controls, numerical gates and process/resource envelope before candidate implementation/timing.',
            'Compile/integrate the reviewed transport graph through an opt-in experiment boundary and recheck any resolved changes.',
            'Execute all registered malformed input, identity/auth/isolation, grants, cancellation/truncation/reconnect/lifetime and real downstream two-process tests.',
            'Run matched complete-workflow comparisons and independent ordinary Flight SQL/native-client interoperability before runtime/client acceptance.',
        ],
    )
    DEST.mkdir(parents=True, exist_ok=True)
    for path, raw in [(E / packet_name, packet_raw), (DEST / packet_name, packet_raw)]:
        with path.open('xb') as stream:
            stream.write(raw)
    record_raw = (json.dumps(record, indent=2, sort_keys=True) + '\n').encode()
    for path in (E / 'native-encoded-exchange-acceptance.json', DEST / 'native-encoded-exchange-acceptance.json'):
        with path.open('xb') as stream:
            stream.write(record_raw)
    for name in ('record_native_exchange_833.py', 'verify_native_exchange_833.py'):
        with (DEST / name).open('xb') as stream:
            stream.write((E / name).read_bytes())
    print(json.dumps(dict(status='recorded', source_files=3027, packages=len(packages),
                         checks=len(checks), members=len(members), packet=identity(packet_raw))))


if __name__ == '__main__':
    main()
