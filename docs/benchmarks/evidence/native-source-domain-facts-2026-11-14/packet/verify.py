"""Replay the complete source-fact comparison packet using only its saved sources and results."""
import argparse
import base64
from collections import Counter
from contextlib import contextmanager
import gzip
import hashlib
import importlib.abc
import io
import json
from pathlib import Path, PurePosixPath
import re
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import tempfile


def content(path):
    with path.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    return dict(bytes=path.stat().st_size, sha256=digest)


def shape(record):
    return dict(bytes=record.get('bytes', record.get('generation', {}).get('bytes')),
                sha256=record['sha256'])


def safe_name(name):
    path = PurePosixPath(name)
    assert name and str(path) == name and not path.is_absolute()
    assert '..' not in path.parts and '\\' not in name


def transport_parts(manifest):
    archive = manifest['archive']
    assert set(archive) == {'format', 'parts', 'bytes', 'sha256'}
    assert archive['format'] == 'concatenated_gzip_tar'
    parts = archive['parts']
    assert isinstance(parts, list) and 0 < len(parts) <= 128
    assert parts == ['evidence.tar.gz.part-' + str(i + 1).zfill(3) for i in range(len(parts))]
    assert set(parts) == {name for name in manifest['files'] if name.startswith('evidence.tar.gz.part-')}
    assert type(archive['bytes']) is int and 0 < archive['bytes'] <= 2 << 30
    assert isinstance(archive['sha256'], str) and re.fullmatch('[0-9a-f]{64}', archive['sha256'])
    assert all(type(manifest['files'][name]['bytes']) is int and
               0 < manifest['files'][name]['bytes'] <= 48 << 20 for name in parts)
    assert sum(manifest['files'][name]['bytes'] for name in parts) == archive['bytes']
    return parts


@contextmanager
def archive_stream(packet, manifest, directory):
    parts = transport_parts(manifest)
    expected = manifest['archive']
    assert shutil.disk_usage(directory).free >= (12 << 30) + expected['bytes']
    with tempfile.TemporaryFile(dir=directory) as joined:
        digest, count = hashlib.sha256(), 0
        for name in parts:
            with (packet / name).open('rb') as source:
                while block := source.read(1 << 20):
                    count += len(block)
                    assert count <= expected['bytes']
                    digest.update(block)
                    joined.write(block)
        assert count == expected['bytes'] and digest.hexdigest() == expected['sha256']
        joined.seek(0)
        yield joined


class Store:
    def __init__(self, packet, directory):
        self.packet, self.directory = packet, directory
        self.manifest = json.loads((packet / 'manifest.json').read_bytes())
        manifest = self.manifest
        assert manifest['schema'] == 'shardloom.native_source_facts_portable.v1'
        assert manifest['query_executions_during_packaging'] == 0 and not manifest['publication_attempted']
        assert manifest['complete_raw_ledgers_retained'] and manifest['original_references_retained']
        assert manifest['declared_fresh_results_per_runtime'] == 2786
        assert manifest['declared_lifecycle_operations_per_runtime'] == 120
        assert not any(path.is_symlink() for path in packet.rglob('*'))
        actual = {path.name for path in packet.iterdir() if path.is_file()}
        assert actual == set(manifest['files']) | {'manifest.json'}
        for name, expected in manifest['files'].items():
            safe_name(name)
            assert content(packet / name) == expected, name
        self.members = manifest['archive_members']
        self.origins = {row['origin']:name for name, row in self.members.items()}
        assert len(self.origins) == len(self.members)
        with archive_stream(packet, manifest, directory) as compressed, tarfile.open(fileobj=compressed, mode='r:gz') as archive:
            entries = archive.getmembers()
            assert len(entries) == len({entry.name for entry in entries})
            assert {entry.name for entry in entries} == set(self.members)
            unpacked_bytes = sum(entry.size for entry in entries)
            assert unpacked_bytes <= 10 << 30
            assert shutil.disk_usage(directory).free >= (12 << 30) + unpacked_bytes, 'portable replay storage headroom is insufficient'
            for entry in entries:
                safe_name(entry.name)
                assert entry.isfile() and not entry.issym() and not entry.islnk()
                expected = self.members[entry.name]['identity']
                assert entry.size == expected['generation']['bytes']
                target = directory / entry.name
                target.parent.mkdir(parents=True, exist_ok=True)
                digest = hashlib.sha256()
                with archive.extractfile(entry) as source, target.open('xb') as destination:
                    while block := source.read(1 << 20):
                        digest.update(block)
                        destination.write(block)
                assert digest.hexdigest() == expected['sha256'], entry.name
        self.external = manifest['external_artifacts']
        assert not set(self.external) & set(self.origins)

    def path(self, path):
        original = str(path)
        assert original in self.origins, 'portable replay tried an unbundled file: ' + original
        return self.directory / self.origins[original]

    def raw(self, path):
        return self.path(path).read_bytes()

    def read(self, path):
        return json.loads(self.raw(path))

    def identity(self, path):
        record = self.members[self.origins[str(path)]]['identity']
        assert content(self.path(path)) == shape(record), path
        return record

    def verify(self, records, *, allow_external=False):
        for record in records:
            origin = record['path']
            if origin in self.external:
                assert allow_external and self.external[origin]['identity'] == record, origin
            else:
                assert shape(self.identity(origin)) == shape(record), origin
                if 'generation' in record:
                    assert self.identity(origin) == {key:record[key] for key in ('path','generation','sha256')}

    @contextmanager
    def closing_db(self, path, readonly=False):
        assert readonly, 'portable replay may only read saved databases'
        connection = sqlite3.connect('file:' + str(self.path(path)) + '?mode=ro', uri=True)
        try:
            yield connection
        finally:
            connection.close()


class NoEngineImports(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'shardloom._native' or fullname.split('.')[0] in {
                'polars','pyarrow','duckdb','pyspark','datafusion','torch'}:
            raise RuntimeError('execution library import forbidden during portable replay: ' + fullname)
        return None


def no_process(*args, **kwargs):
    raise RuntimeError('subprocess execution forbidden during portable replay')


def environment_bindings(protocol, previous):
    environment = protocol['consumer_environment']
    assert protocol['consumer_python'] == environment['executable'] == previous['consumer_python']
    assert protocol['python_version'] == environment['version'] == previous['python_version']
    assert protocol['control_python_version'] == protocol['python_version']
    assert protocol['control_python_resolved'] == environment['resolved_executable']
    assert environment['physical_interpreter']['path'] == environment['resolved_executable']
    inherited = {row['path']: row for row in previous['frozen_files']}
    frozen = {row['path']: row for row in protocol['frozen_files']}
    assert len(frozen) == len(protocol['frozen_files'])
    prefix = Path(environment['prefix'])
    expected = sorted(name for name in inherited if Path(name).is_relative_to(prefix))
    assert len(expected) == 367 and environment['inherited_files'] == expected
    assert all(frozen[name] == inherited[name] for name in expected)
    interpreter = environment['resolved_executable']
    assert frozen[interpreter] == inherited[interpreter] == environment['physical_interpreter']


def validation(store, protocol):
    from activation_test_inventory import completed_tests

    manifest = store.manifest
    out, root = Path(manifest['out']), Path(manifest['worktree'])
    reuse = store.read(out / 'source-facts-baseline-reuse-r1.json')
    previous = store.read(reuse['protocol']['path'])
    environment_bindings(protocol, previous)
    proof = store.read(manifest['validation']['path'])
    assert proof['status'] == 'passed' and proof['complete_test_inventory_verified']
    candidate = store.read(protocol['runtimes']['candidate']['manifest']['path'])
    assert candidate['status'] == 'passed' and candidate['profile'] == 'release'
    assert proof['runtime'] == shape(protocol['runtimes']['candidate']['manifest'])
    build_path = out / candidate['build_receipt']
    build = store.read(build_path)
    assert shape(store.identity(build_path)) == candidate['build_receipt_identity']
    snapshot = build['source_files']
    fingerprint = hashlib.sha256(json.dumps(snapshot, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    assert fingerprint == candidate['source_fingerprint_sha256'] == proof['source_fingerprint_sha256']
    assert build['head'] == candidate['source_commit']
    assert build['source_tree'] == candidate['source_tree']
    assert build['working_tree'] == candidate['working_tree']
    assert build['command'] == ['cargo', 'build', '--offline', '--locked', '--release',
        '--features', 'release-user-surfaces', '-p', 'shardloom-cli', '-p', 'shardloom-python']
    assert build['status'] == 'passed' and build['returncode'] == 0
    assert build['sources_unchanged'] and build['process_group_drained'] and build['direct_child_reaped']
    for relative, expected in snapshot.items():
        assert shape(store.identity(root / relative)) == expected, relative
    from verify_packaging_revision import verify_revision
    compatibility_path = out / 'source-facts-packaging-compatibility-r4.json'
    compatibility = store.read(compatibility_path)
    assert compatibility == verify_revision(store.read, store.raw, store.identity, out, root)
    assert proof['packaging_compatibility'] == shape(store.identity(compatibility_path))
    assert protocol['packaging_compatibility'] == store.identity(compatibility_path)
    code_runtime = store.read(out / 'runtime-r3-identity.json')
    code_snapshot = store.read(out / code_runtime['build_receipt'])['source_files']
    assert proof['reused_code_validation_runtime'] == shape(store.identity(out / 'runtime-r3-identity.json'))
    cargo = ['cargo', 'test', '--offline', '--locked', '--workspace', '--all-targets']
    prefix = 'source-facts-final-r3'
    commands = {
        prefix + '-format': ['cargo', 'fmt', '--all', '--', '--check'],
        prefix + '-default-clippy': ['cargo', 'clippy', '--offline', '--locked', '--workspace', '--all-targets', '--', '-D', 'warnings'],
        prefix + '-native-clippy': ['cargo', 'clippy', '--offline', '--locked', '--workspace', '--all-targets', '--features', 'release-user-surfaces', '--', '-D', 'warnings'],
        prefix + '-file-only': ['cargo', 'check', '--offline', '--locked', '-p', 'shardloom-vortex', '--no-default-features', '--features', 'vortex-file-io'],
        prefix + '-local-only': ['cargo', 'check', '--offline', '--locked', '-p', 'shardloom-vortex', '--no-default-features', '--features', 'vortex-local-primitives'],
        prefix + '-default-inventory': cargo + ['--no-fail-fast', '--', '--list'],
        prefix + '-default-tests': cargo + ['--no-fail-fast'],
        prefix + '-native-inventory': cargo + ['--features', 'release-user-surfaces', '--no-fail-fast', '--', '--list'],
        prefix + '-native-tests': cargo + ['--features', 'release-user-surfaces', '--no-fail-fast'],
        prefix + '-array-provider': [protocol['control_python'], '-B', str(out / 'run_provider_checks_r3.py')],
        prefix + '-all-features': ['cargo', 'check', '--offline', '--locked', '--workspace', '--all-features'],
        prefix + '-minimal-workspace': ['cargo', 'check', '--offline', '--locked', '--workspace', '--no-default-features'],
        prefix + '-reader-boundary': ['cargo', 'check', '--offline', '--locked', '-p', 'shardloom-cli', '--no-default-features', '--features', 'vortex-local-primitives,universal-format-io'],
        prefix + '-msrv-minimal': ['/Users/dylan/.cargo/bin/rustup', 'run', '1.96.0', 'cargo', 'check', '--offline', '--locked', '--workspace', '--no-default-features'],
        prefix + '-msrv-native': ['/Users/dylan/.cargo/bin/rustup', 'run', '1.96.0', 'cargo', 'check', '--offline', '--locked', '--workspace', '--all-targets', '--features', 'release-user-surfaces'],
        'source-facts-final-r4-installed-wheel': [str(out.parent / 'native-pipeline-sql-minute-2026-11-13/wheel-tools-r1/venv/bin/python'),
            '-B', 'scripts/prove_native_python_wheel.py', '--repo-root', str(root),
            '--output-dir', str(out / 'source-facts-final-r4-installed-wheel'),
            '--native-library', str(out / 'runtime-r4/lib_native.dylib'), '--cli-binary', str(out / 'runtime-r4/shardloom'),
            '--wheelhouse', str(out.parent / 'native-pipeline-consolidation-2026-11-12/wheel-tools-r1/wheelhouse')],
        'source-facts-public-r4': ['/Users/dylan/.cache/shardloom/query-surface-polars-2.1.0/bin/python',
            '-B', str(out / 'run_public_checks.py'), '--runtime', 'runtime-r3', '--name', 'source-facts-public-r4'],
        'source-facts-provider-format-r3': ['rustfmt', '--check', '--edition', '2024',
            'vendor/vortex-array/src/scalar_fn/fns/binary/compare/mod.rs',
            'vendor/vortex-array/src/scalar_fn/fns/binary/compare/observation.rs'],
        'source-facts-provider-provenance-check-r4': [protocol['control_python'], '-B', str(out / 'verify_provider_provenance_r3.py')],
        'source-facts-license-bundle-r1': [protocol['control_python'], '-B', 'scripts/check_python_license_bundle.py'],
        'source-facts-license-tests-r1': [protocol['control_python'], '-B', '-m', 'unittest', 'discover', '-s', 'scripts', '-p', 'test_python_license_bundle.py', '-v'],
        'source-facts-packaging-compatibility-check-r4': [protocol['control_python'], '-B', str(out / 'verify_packaging_revision.py')],
    }
    fresh = {'source-facts-final-r4-installed-wheel', 'source-facts-license-bundle-r1',
        'source-facts-license-tests-r1', 'source-facts-packaging-compatibility-check-r4'}
    assert proof['reused_code_checks'] == sorted(name + '.json' for name in commands if name not in fresh)
    assert set(proof['checks']) == {name + '.json' for name in commands}
    for name, expected in proof['checks'].items():
        receipt = store.read(out / name)
        assert shape(store.identity(out / name)) == expected
        assert receipt['status'] == 'passed' and receipt['returncode'] == 0
        assert receipt['source_files'] == (snapshot if Path(name).stem in fresh else code_snapshot)
        assert receipt['sources_unchanged']
        assert receipt['process_group_drained'] and receipt['direct_child_reaped']
        assert receipt['cwd'] == str(root) and receipt['command'] == commands[Path(name).stem]
    for kind in ('default', 'native'):
        listed_path = out / 'logs' / (prefix + '-' + kind + '-inventory.log')
        result_path = out / 'logs' / (prefix + '-' + kind + '-tests.log')
        listed, results = store.raw(listed_path).decode(), store.raw(result_path).decode()
        wanted = Counter(re.findall(r'^(.+): test$', listed, re.MULTILINE))
        statuses = completed_tests(results)
        assert wanted and wanted == Counter(name for name, _ in statuses)
        totals = [tuple(map(int, row)) for row in re.findall(
            r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored', results)]
        passed = sum(status == 'ok' for _, status in statuses)
        ignored = sum(status.startswith('ignored') for _, status in statuses)
        assert tuple(sum(row[index] for row in totals) for index in range(3)) == (passed, 0, ignored)
        bins = re.findall(r'^\s+Running (.+)$', listed, re.MULTILINE)
        assert bins == re.findall(r'^\s+Running (.+)$', results, re.MULTILINE)
        assert len(totals) == len(bins)
        assert proof['test_counts'][kind] == dict(passed=passed, failed=0, ignored=ignored,
            test_binaries=len(totals), exact_listed_names_verified=sum(wanted.values()),
            inventory=shape(store.identity(listed_path)), results=shape(store.identity(result_path)))
        assert passed > 0
    public_path = out / 'source-facts-public-r4-result.json'
    public = store.read(public_path)
    assert proof['public_matrix'] == shape(store.identity(public_path))
    assert public['status'] == 'passed' and public['tests'] == 4
    assert public['failures'] == public['errors'] == public['skipped'] == 0
    assert public['source_fingerprint_sha256'] == code_runtime['source_fingerprint_sha256']
    for name, expected in proof['installed_checks'].items():
        assert shape(store.identity(out / name)) == expected
    installed = out / 'source-facts-final-r4-installed-wheel'
    report, integration = store.read(installed / 'report.json'), store.read(installed / 'integration.json')
    assert integration == proof['installed_python_tests'] == dict(tests=455, failures=0, errors=0, skipped=0, status='passed')
    assert report['status'] == 'passed' and not report['publication_attempted']
    artifacts = protocol['runtimes']['candidate']['runtime']
    assert report['integrity']['native_library_sha256'] == artifacts['library']['sha256']
    assert report['integrity']['cli_sha256'] == artifacts['binary']['sha256']
    assert report['installation_performed'] and report['uninstall_status'] == 'passed'
    assert report['installed_load']['isolated_install']
    for step in report['steps']:
        assert step['returncode'] == 0
        assert store.identity(installed / step['log'])['sha256'] == step['log_sha256']
    installed_text = store.raw(installed / 'integration.log').decode()
    for name in ('test_source_fact_predicates_match_all_five_surfaces',
                 'test_source_fact_predicates_own_results_and_reject_changed_sources'):
        assert name in installed_text
    native_list = store.raw(out / 'logs' / (prefix + '-native-inventory.log')).decode()
    native_results = store.raw(out / 'logs' / (prefix + '-native-tests.log')).decode()
    regressions = [name for name in re.findall(r'^(.+): test$', native_list, re.MULTILINE) if 'source_facts_' in name]
    assert len(regressions) == 10 and proof['explicit_source_fact_regressions'] == regressions
    assert all('test ' + name + ' ... ok' in native_results for name in regressions)
    for name in ('compact_migration_preserves_compensation_signed_zero_and_new_group_identity',
                 'replay_capture_releases_every_fragment_and_preserves_raw_float_bits_and_alias_credit'):
        assert re.search(r'test [^\n]*::' + name + r' \.\.\. ok', native_results), name
    provider_dir = out / 'source-facts-array-provider-r3'
    provider = store.read(provider_dir / 'report.json')
    assert provider['status'] == 'passed' and provider['group'] == 'source_facts_array'
    for name, expected in proof['provider_checks'].items():
        assert shape(store.identity(out / name)) == expected
    config = ['--manifest-path', 'vendor/vortex-array/Cargo.toml', '--features', '_test-harness,table-display']
    expected_commands = [['cargo', 'test', '--offline', '--locked', *config, '--lib', '--', '--test-threads=2'],
        ['cargo', 'clippy', '--offline', '--locked', *config, '--lib', '--', '-D', 'warnings']]
    assert [entry['command'] for entry in provider['commands']] == expected_commands
    lock = store.identity(root / '.github/provider-locks/vortex-array.lock')['sha256']
    for entry in provider['commands']:
        assert entry['returncode'] == 0 and entry['lock_sha256'] == lock
        assert store.identity(provider_dir / entry['log'])['sha256'] == entry['log_sha256']
    provider_text = store.raw(provider_dir / 'vortex-array-test.log').decode()
    counts = re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored', provider_text)
    assert len(counts) == 1 and int(counts[0][0]) > 0 and counts[0][1] == '0'
    assert proof['provider_test_counts'] == dict(zip(('passed', 'failed', 'ignored'), map(int, counts[0])))
    for name in ('compare_work_is_optional_shared_and_overflow_is_unavailable',
                 'compare_work_releases_ownership_and_poison_never_reports_partial_counts',
                 'compare_work_clears_registry_while_inflight_record_retains_credit'):
        assert re.search(r'test [^\n]*::' + name + r' \.\.\. ok', provider_text), name
    for role in ('baseline', 'candidate'):
        runtime = protocol['runtimes'][role]
        record = store.read(runtime['manifest']['path'])
        assert record['source_fingerprint_sha256'] == runtime['source_fingerprint_sha256']
        assert record['source_commit'] == runtime['source_commit']
        for item in runtime['runtime'].values():
            assert record['files'][item['path']] == shape(item)
    assert protocol['runtimes']['baseline']['source_fingerprint_sha256'] == 'c64f40da7b3693e3eff7e4eebeca2f64a171a7d02deba387f440d021f586b4ef'
    accepted = store.read(out.parent / 'native-dictionary-domain-2026-11-14/dictionary-closeout-r1.json')
    assert accepted['status'] == 'complete_local_experiment_closeout' and accepted['disposition'] == 'retain'
    assert accepted['source_fingerprint_sha256'] == protocol['runtimes']['baseline']['source_fingerprint_sha256']
    return fingerprint


def fixture_and_provider_provenance(store, protocol, support):
    out, root = support.OUT, support.ROOT
    compatibility = store.read(protocol['fixture_runtime_compatibility']['path'])
    assert compatibility['status'] == 'passed_unchanged_fixture_semantics'
    assert compatibility['queries_executed'] == 0 and compatibility['original_complete_readbacks'] == 24
    before_manifest = store.read(out / 'runtime-r1-identity.json')
    after_manifest = store.read(out / 'runtime-r4-identity.json')
    before_build = store.read(out / before_manifest['build_receipt'])
    after_build = store.read(out / after_manifest['build_receipt'])
    before, after = before_build['source_files'], after_build['source_files']
    expected_changes = {
        'python/THIRD_PARTY_LICENSES/manifest.json',
        'vendor/vortex-array/src/scalar_fn/fns/binary/compare/mod.rs',
        'vendor/vortex-array/src/scalar_fn/fns/binary/compare/observation.rs',
        'vendor/vortex-array/README.shardloom.md', 'vendor/vortex-array/upstream-provenance.json',
        'shardloom-vortex/src/local_primitives/native_relational_aggregate_compact_state_tests.rs',
        'shardloom-vortex/src/local_primitive_relational_replay_capture_tests.rs',
    }
    changed = {name for name in before.keys() | after.keys() if before.get(name) != after.get(name)}
    assert changed == expected_changes
    assert compatibility['changed_sources'] == {name: dict(before=before[name], after=after[name]) for name in sorted(changed)}
    assert compatibility['original_source_fingerprint_sha256'] == before_manifest['source_fingerprint_sha256']
    assert compatibility['current_source_fingerprint_sha256'] == after_manifest['source_fingerprint_sha256']
    assert compatibility['original_runtime'] == shape(store.identity(out / 'runtime-r1-identity.json'))
    assert compatibility['current_runtime'] == shape(store.identity(out / 'runtime-r4-identity.json'))
    saved = {}
    for line in gzip.decompress(store.raw(out / 'runtime-r1-source-and-harness-snapshot.jsonl.gz')).splitlines():
        row = json.loads(line)
        raw = base64.b64decode(row['base64'], validate=True)
        assert len(raw) == row['bytes'] and hashlib.sha256(raw).hexdigest() == row['sha256']
        if row['kind'] == 'source':
            assert row['path'] not in saved
            saved[row['path']] = raw
            assert before[row['path']] == dict(bytes=len(raw), sha256=row['sha256'])
    assert len(saved) == 35
    notice_name = 'python/THIRD_PARTY_LICENSES/manifest.json'
    third_snapshot = [json.loads(line) for line in gzip.decompress(
        store.raw(out / 'runtime-r3-source-and-harness-snapshot.jsonl.gz')).splitlines()]
    notice = next(row for row in third_snapshot if row['kind'] == 'source' and row['path'] == notice_name)
    notice_bytes = base64.b64decode(notice['base64'], validate=True)
    assert dict(bytes=len(notice_bytes), sha256=hashlib.sha256(notice_bytes).hexdigest()) == before[notice_name]
    saved[notice_name] = notice_bytes
    for name, expected in before.items():
        if name in changed:
            raw = saved[name]
            assert dict(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest()) == expected
        else:
            assert shape(store.identity(root / name)) == expected
    first_failure = store.read(out / 'source-facts-final-r1-native-tests.json')
    assert first_failure['status'] == 'failed' and first_failure['returncode'] == 101
    assert first_failure['source_files'] == before and first_failure['sources_unchanged']
    assert first_failure['process_group_drained'] and first_failure['direct_child_reaped']
    failed_log = store.raw(out / 'logs/source-facts-final-r1-native-tests.log').decode()
    failed = re.findall(r'^test (.+) \.\.\. FAILED$', failed_log, re.MULTILINE)
    assert {name.rsplit('::', 1)[-1] for name in failed} == {
        'compact_migration_preserves_compensation_signed_zero_and_new_group_identity',
        'replay_capture_releases_every_fragment_and_preserves_raw_float_bits_and_alias_credit'}
    assert len(failed) == 2
    second_manifest = store.read(out / 'runtime-r2-identity.json')
    second_build = store.read(out / second_manifest['build_receipt'])
    second_sources = second_build['source_files']
    second_saved = {}
    for line in gzip.decompress(store.raw(out / 'runtime-r2-source-and-harness-snapshot.jsonl.gz')).splitlines():
        row = json.loads(line)
        raw = base64.b64decode(row['base64'], validate=True)
        assert len(raw) == row['bytes'] and hashlib.sha256(raw).hexdigest() == row['sha256']
        if row['kind'] == 'source':
            assert row['path'] not in second_saved
            second_saved[row['path']] = raw
            assert second_sources[row['path']] == dict(bytes=len(raw), sha256=row['sha256'])
    assert len(second_saved) == 35
    assert second_sources[notice_name] == before[notice_name]
    second_saved[notice_name] = notice_bytes
    lint_changes = {name for name in second_sources.keys() | after.keys()
                    if second_sources.get(name) != after.get(name)}
    assert lint_changes == {notice_name, 'vendor/vortex-array/README.shardloom.md',
        'vendor/vortex-array/upstream-provenance.json',
        'vendor/vortex-array/src/scalar_fn/fns/binary/compare/observation.rs'}
    for name, expected in second_sources.items():
        if name in lint_changes:
            raw = second_saved[name]
            assert dict(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest()) == expected
        else:
            assert shape(store.identity(root / name)) == expected
    second_failure = store.read(out / 'source-facts-final-r2-array-provider.json')
    assert second_failure['status'] == 'failed' and second_failure['returncode'] == 1
    assert second_failure['source_files'] == second_sources and second_failure['sources_unchanged']
    assert second_failure['process_group_drained'] and second_failure['direct_child_reaped']
    second_log = store.raw(out / 'source-facts-array-provider-r2/vortex-array-clippy.log').decode()
    assert second_log.count('error: use of a disallowed type') == 3
    assert 'error: called `.ok().is_some_and(..)`' in second_log
    second_tests = store.raw(out / 'source-facts-array-provider-r2/vortex-array-test.log').decode()
    assert 'test result: ok. 5779 passed; 0 failed; 1 ignored' in second_tests
    fixtures = store.read(out / 'source-facts-inputs-r1/complete.json')
    preparation = store.read(out / 'source-facts-input-preparation-r1.json')
    assert preparation['status'] == 'passed' and preparation['returncode'] == 0
    assert preparation['source_files'] == before and preparation['sources_unchanged']
    assert preparation['process_group_drained'] and preparation['direct_child_reaped']
    assert preparation['command'] == protocol['preparation_command']
    assert fixtures['status'] == 'passed_preparation'
    assert fixtures['source_runtime_fingerprint'] == before_manifest['source_fingerprint_sha256']
    assert fixtures['declarations'] == support.source_facts_cases.declarations()
    store.verify([fixtures['references'], *fixtures['sources_used']])
    assert len(fixtures['readbacks']) == 24
    pairs = set()
    for row in fixtures['readbacks']:
        size, profile = row['size'], row['profile']
        assert (size, profile) not in pairs
        pairs.add((size, profile))
        store.verify([row['packet']])
        store.verify([row['input']], allow_external=True)
        raw = gzip.decompress(store.raw(row['packet']['path']))
        assert hashlib.sha256(raw).hexdigest() == row['actual_sha256'] == row['expected_sha256']
        expected = b''.join((json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n').encode()
            for value in support.source_facts_cases.literals.population(size, profile))
        assert raw == expected
        assert len(raw.splitlines()) == row['rows'] == support.source_facts_cases.row_count(size, profile)
        assert row['complete_row_export']['complete'] and not row['complete_row_export']['fallback_attempted']
    assert pairs == {(size, profile) for size in support.source_facts_cases.SIZES for profile in support.source_facts_cases.PROFILES}
    provenance_path = 'vendor/vortex-array/upstream-provenance.json'
    provenance = store.read(root / provenance_path)
    original = json.loads(saved[provenance_path])
    for field in ('provider', 'version', 'license', 'registry_archive_sha256', 'registry_archive_file_count',
                  'original_files_sha256', 'vcs_info', 'source_of_truth', 'license_url', 'license_sha256',
                  'license_git_blob_sha1', 'supplemental_test_files', 'omitted_files'):
        assert provenance[field] == original[field], field
    raw_archive = store.raw(out / 'vortex-array-0.93.0.crate')
    assert hashlib.sha256(raw_archive).hexdigest() == provenance['registry_archive_sha256']
    original_files = {}
    with tarfile.open(fileobj=io.BytesIO(raw_archive), mode='r:gz') as archive:
        for member in archive:
            if member.isdir():
                continue
            safe_name(member.name)
            assert member.isfile() and member.name.startswith('vortex-array-0.93.0/')
            name = member.name.removeprefix('vortex-array-0.93.0/')
            assert name not in original_files
            original_files[name] = hashlib.sha256(archive.extractfile(member).read()).hexdigest()
    assert original_files == provenance['original_files_sha256']
    assert len(original_files) == provenance['registry_archive_file_count']
    expected = {name: digest for name, digest in original_files.items() if name not in provenance['omitted_files']}
    expected.update(provenance['modified_files_sha256'])
    expected.update(provenance['added_files_sha256'])
    prefix = 'vendor/vortex-array/'
    assert {name.removeprefix(prefix) for name in after if name.startswith(prefix)} == set(expected) | {'upstream-provenance.json'}
    for name, digest in expected.items():
        assert store.identity(root / prefix / name)['sha256'] == digest, name
    assert provenance['local_changes_documentation'] == dict(path='README.shardloom.md', sha256=expected['README.shardloom.md'])


def verify(packet):
    if not __debug__:
        raise RuntimeError('assertions must remain enabled')
    assert sys.version_info >= (3, 13)
    import types
    with tempfile.TemporaryDirectory(prefix='shardloom-source-facts-replay-') as temporary:
        store = Store(packet, Path(temporary))
        manifest = store.manifest
        out, activation, root, original = (Path(manifest[key]) for key in ('out', 'activation', 'worktree', 'original'))
        protocol_path = Path(manifest['protocol']['path'])
        protocol = store.read(protocol_path)
        store.verify([manifest[key] for key in ('protocol', 'validation', 'replay', 'disposition')])
        store.verify(protocol['frozen_files'], allow_external=True)
        assert content(packet / 'verify.py') == shape(store.identity(out / 'portable_source_facts.py'))
        sys.meta_path.insert(0, NoEngineImports())
        subprocess.Popen = no_process
        runtime_stub = types.ModuleType('source_facts_runtime')
        runtime_stub.verify_runtime = no_process
        sys.modules['source_facts_runtime'] = runtime_stub
        sys.path.insert(0, str(store.path(out / 'source_facts_support.py').parent))
        import importlib.util
        literal_path = store.path(root / 'python/tests/native_source_fact_cases.py')
        literal_spec = importlib.util.spec_from_file_location('native_source_fact_cases', literal_path)
        literal = importlib.util.module_from_spec(literal_spec)
        sys.modules[literal_spec.name] = literal
        literal_spec.loader.exec_module(literal)
        import source_facts_support as support
        import source_facts_replay as replay
        assert Path(support.source_facts_cases.literals.__file__).resolve() == literal_path.resolve()
        assert Path(support.__file__).resolve() == store.path(out / 'source_facts_support.py').resolve()
        assert Path(replay.__file__).resolve() == store.path(out / 'source_facts_replay.py').resolve()
        assert Path(support.source_facts_cases.__file__).resolve() == store.path(out / 'source_facts_cases.py').resolve()
        assert Path(support.prior.__file__).resolve() == store.path(activation / 'activation-tools/activation_support.py').resolve()
        prior = support.prior
        support.OUT, support.ACTIVATION, support.ROOT, support.E = out, activation, root, original
        support.FROZEN = original / 'investigation-original-harness-r1'
        support.TOOLS = out
        support.PREVIOUS = out.parent / 'native-dictionary-domain-2026-11-14'
        support.source_facts_cases.OUT, support.source_facts_cases.ROOT, support.source_facts_cases.E = out, root, original
        support.source_facts_cases.FROZEN = support.FROZEN
        prior.OUT, prior.TOOLS, prior.ROOT, prior.E = activation, activation / 'activation-tools', root, original
        prior.FROZEN, prior.COUNT = support.FROZEN, activation / 'count-cohort-v2'
        support.read = prior.read = store.read
        replay.read, replay.identity, replay.verify = store.read, store.identity, store.verify
        replay.closing_db = store.closing_db
        assert support.load_protocol(protocol_path) == protocol
        fingerprint = validation(store, protocol)
        fixture_and_provider_provenance(store, protocol, support)
        with store.closing_db(Path(protocol['reused_ledger']['path']), readonly=True) as previous:
            assert len(protocol['baseline_reuse']) == 336
            for reused in protocol['baseline_reuse']:
                entry = previous.execute('SELECT seq,id,status,packet,reuse FROM attempt WHERE seq=?',
                    (reused['prefix_seq'],)).fetchone()
                assert entry[:3] == (reused['prefix_seq'], reused['id'], 'passed')
                assert hashlib.sha256(entry[3]).hexdigest() == reused['compressed_packet_sha256']
                assert reused['earlier_reuse'] is None and entry[4] is None
        fresh = {role: replay.fresh(protocol, protocol_path, role) for role in ('baseline', 'candidate')}
        lifecycles = {role: replay.lifecycle(protocol, protocol_path, role) for role in ('baseline', 'candidate')}
        recomputed = replay.evaluate(protocol, fresh, lifecycles)
        expected = store.read(manifest['replay']['path'])
        assert {key: value for key, value in expected.items() if key not in
            ('protocol', 'source_fingerprint_sha256', 'verifier', 'observations_replayed', 'lifecycle_operations_replayed')} == recomputed
        assert expected['source_fingerprint_sha256'] == fingerprint
        store.verify([expected['protocol'], expected['verifier']])
        assert expected['observations_replayed'] == 5572 and expected['lifecycle_operations_replayed'] == 240
        assert recomputed['decision'] in {'retain', 'drop'}
        assert recomputed['status'] == ('passed_complete_acceptance' if recomputed['decision'] == 'retain' else 'complete_decline')
        assert recomputed['fresh']['complete'] and recomputed['lifecycle']['complete']
        assert recomputed['work_requirements_passed'] and recomputed['measured_encoded_compare_dispatch']
        assert len(recomputed['work_witnesses']) == 150
        disposition = store.read(manifest['disposition']['path'])
        assert disposition['disposition'] == recomputed['decision']
        assert disposition['status'] == ('complete_experiment_acceptance' if recomputed['decision'] == 'retain' else 'complete_experiment_decline')
        assert disposition['original_replay_status'] == recomputed['status']
        assert disposition['original_replay_decision'] == recomputed['decision']
        assert disposition['fresh'] == recomputed['fresh'] and disposition['lifecycle'] == recomputed['lifecycle']
        assert not disposition['thresholds_changed'] and disposition['additional_query_executions'] == 0
        assert not disposition['whole_investigation_queue_complete'] and not disposition['publication_attempted']
        for name, digest in disposition['inputs'].items():
            assert store.identity(out / name)['sha256'] == digest
        controls = store.read(out / 'source-facts-negative-controls-r1.json')
        assert controls['status'] == 'passed_negative_controls'
        assert controls['count'] == len(controls['controls']) == 28
        assert all(row['status'] == 'rejected' for row in controls['controls'])
        assert len({row['name'] for row in controls['controls']}) == controls['count']
        assert controls['native_query_executions'] == 0 and controls['original_unchanged']
        assert controls['synthetic_acceptance_fixture_is_not_observed_evidence']
        store.verify([controls['replay'], controls['verifier']])
        preserved = store.read(out / 'source-facts-source-r1.json')
        assert preserved == disposition['candidate_source']
        assert preserved['source_bytes_equal_to_executed_candidate']
        assert preserved['source_fingerprint_sha256'] == fingerprint
        assert preserved['parent'] == disposition['predecessor_commit']
        assert shape(store.identity(out / 'candidate-proposal-r1.patch')) == preserved['patch']
        assert not any(name in sys.modules for name in ('shardloom._native', 'polars', 'pyarrow', 'duckdb'))
        return dict(status='passed_complete_portable_replay', query_executions=0,
            complete_fresh_results=5572, complete_lifecycle_operations=240, baseline_reuse=336,
            source_fingerprint_sha256=fingerprint, external_payloads_not_reopened=len(store.external),
            fresh=recomputed['fresh'], lifecycle=recomputed['lifecycle'],
            disposition=recomputed['decision'], original_replay_decision=recomputed['decision'],
            negative_controls_bound=controls['count'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    result = verify(args.root.resolve())
    if args.output:
        with args.output.open('x') as stream:
            json.dump(result, stream, indent=2)
            stream.write('\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
