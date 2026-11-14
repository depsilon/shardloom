"""Replay the complete activation packet using only its saved sources and results."""
import argparse
from collections import Counter
from contextlib import contextmanager
import gzip
import hashlib
import importlib.abc
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
        assert manifest['schema'] == 'shardloom.native_activation_portable.v1'
        assert manifest['query_executions_during_packaging'] == 0 and not manifest['publication_attempted']
        assert manifest['complete_raw_ledgers_retained'] and manifest['original_references_retained']
        assert manifest['declared_fresh_results_per_runtime'] == 7498
        assert manifest['declared_lifecycle_operations_per_runtime'] == 10
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


def isolation_correction(store, protocol):
    """Account for the complete disturbed capture without admitting its clocks."""
    amendment = protocol['prospective_isolation_amendment']
    assert amendment['prior_observations_retained']
    assert amendment['prior_baseline_admissible_for_performance'] is False
    assert amendment['timing_values_consulted_for_replacement'] is False
    assert amendment['unchanged_query_and_acceptance_policy']
    assert amendment['prior_fresh_query_executions'] == amendment['replacement_baseline_executions'] == 7120
    assert amendment['prior_reused_observations'] == 378
    assert store.manifest['prior_invalidated_fresh_executions'] == 7120
    assert store.manifest['prior_invalidated_observations_included']
    store.verify([amendment['original_protocol'], amendment['incident']])
    previous = store.read(amendment['original_protocol']['path'])
    incident = store.read(amendment['incident']['path'])
    assert incident['status'] == 'invalid_for_performance_isolation'
    assert incident['engine_times_inspected_for_disposition'] is False
    target = Path(amendment['invalidated_baseline_target'])
    assert str(target) == previous['targets']['baseline'] == incident['completed_capture']
    assert str(target) not in [*protocol['targets'].values(), *protocol['lifecycle_targets'].values()]
    permitted = {'targets', 'lifecycle_targets', 'frozen_at', 'frozen_files', 'prospective_isolation_amendment'}
    assert {k: v for k, v in protocol.items() if k not in permitted} == {
        k: v for k, v in previous.items() if k not in permitted}
    old_files = {row['path']: row for row in previous['frozen_files']}
    current_files = {row['path']: row for row in protocol['frozen_files']}
    assert all(current_files.get(path) == record for path, record in old_files.items())
    complete = store.read(target / 'complete.json')
    assert complete['status'] == 'complete_observation' and complete['posthash_verified']
    assert complete['protocol'] == amendment['original_protocol']
    assert complete['fresh_attempts'] == 7120 and complete['reused_observations'] == 378
    assert complete['outcomes'] == {'passed': 7498}
    store.verify([complete['ledger'], complete['binding']])
    with store.closing_db(target / 'attempts.sqlite', readonly=True) as connection:
        assert connection.execute('SELECT status, COUNT(*), SUM(reuse IS NOT NULL) FROM attempt GROUP BY status').fetchall() == [('passed', 7498, 378)]
    return dict(prior_fresh_executions=7120, prior_reused_observations=378,
                prior_capture_retained=True, prior_clocks_admitted=False)


def validation(store, protocol):
    from activation_validation_contract import check_commands

    manifest = store.manifest
    out, root = Path(manifest['out']), Path(manifest['worktree'])
    proof = store.read(manifest['validation']['path'])
    assert proof['status'] == 'passed' and proof['complete_test_inventory_verified']
    candidate = store.read(protocol['runtimes']['candidate']['manifest']['path'])
    assert candidate['status'] == 'passed' and candidate['profile'] == 'release'
    assert proof['runtime'] == shape(protocol['runtimes']['candidate']['manifest'])
    build = store.read(out / candidate['build_receipt'])
    assert candidate['build_receipt_identity'] == shape(store.identity(out / candidate['build_receipt']))
    snapshot = build['source_files']
    fingerprint = hashlib.sha256(json.dumps(snapshot, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    assert fingerprint == candidate['source_fingerprint_sha256'] == proof['source_fingerprint_sha256']
    assert build['head'] == candidate['source_commit'] == proof['source_commit']
    assert build['source_tree'] == candidate['source_tree'] == proof['source_tree']
    assert build['working_tree'] == candidate['working_tree'] == proof['working_tree']
    assert build['command'] == ['cargo','build','--offline','--locked','--release',
        '--features','release-user-surfaces','-p','shardloom-cli','-p','shardloom-python']
    assert build['status'] == 'passed' and build['returncode'] == 0
    assert build['sources_unchanged'] and build['process_group_drained'] and build['direct_child_reaped']
    for relative, expected in snapshot.items():
        assert shape(store.identity(root / relative)) == expected, relative
    receipts = {}
    for name, expected in proof['checks'].items():
        receipt = store.read(out / name)
        receipts[name] = receipt
        assert shape(store.identity(out / name)) == expected
        assert receipt['status'] == 'passed' and receipt['returncode'] == 0
        assert receipt['source_files'] == snapshot and receipt['sources_unchanged']
        assert receipt['process_group_drained'] and receipt['direct_child_reaped']
        assert receipt['cwd'] == str(root)
    check_commands(receipts, root=root, out=out,
        runtime=protocol['runtimes']['candidate']['name'], consumer_python=protocol['consumer_python'])
    for kind in ('default','native'):
        (test_name,) = (name for name in proof['checks'] if name.endswith('-' + kind + '-tests.json'))
        (inventory_name,) = (name for name in proof['checks'] if name.endswith('-' + kind + '-inventory.json'))
        listed_path = out / 'logs' / (Path(inventory_name).stem + '.log')
        result_path = out / 'logs' / (Path(test_name).stem + '.log')
        listed, result = store.raw(listed_path).decode(), store.raw(result_path).decode()
        wanted = Counter(re.findall(r'^(.+): test$', listed, re.MULTILINE))
        from activation_test_inventory import completed_tests
        statuses = completed_tests(result)
        assert wanted and wanted == Counter(name for name, _ in statuses)
        totals = [tuple(map(int, row)) for row in re.findall(
            r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored', result)]
        passed, ignored = sum(status == 'ok' for _,status in statuses), sum(status == 'ignored' for _,status in statuses)
        assert tuple(sum(row[index] for row in totals) for index in range(3)) == (passed,0,ignored)
        listed_bins = re.findall(r'^\s+Running (.+)$', listed, re.MULTILINE)
        assert listed_bins == re.findall(r'^\s+Running (.+)$', result, re.MULTILINE)
        assert len(totals) == len(listed_bins)
        assert proof['test_counts'][kind] == dict(passed=passed, failed=0, ignored=ignored,
            test_binaries=len(totals), exact_listed_names_verified=sum(wanted.values()),
            inventory=shape(store.identity(listed_path)), results=shape(store.identity(result_path)))
    public_path = next(name for name in proof['checks'] if name.startswith('activation-public-'))
    public = store.read(out / public_path.replace('.json','-result.json'))
    assert proof['public_matrix'] == shape(store.identity(out / public_path.replace('.json','-result.json')))
    assert public['status'] == 'passed' and public['tests'] == 7
    assert public['failures'] == public['errors'] == public['skipped'] == 0
    assert public['source_fingerprint_sha256'] == fingerprint
    installed_paths = {Path(name).name:name for name in proof['installed_checks']}
    for name, expected in proof['installed_checks'].items():
        assert shape(store.identity(out / name)) == expected
    report = store.read(out / installed_paths['report.json'])
    integrations = store.read(out / installed_paths['integration.json'])
    assert integrations == dict(tests=449, failures=0, errors=0, skipped=0, status='passed')
    assert proof['installed_python_tests'] == integrations
    assert report['status'] == 'passed' and not report['publication_attempted']
    artifacts = protocol['runtimes']['candidate']['runtime']
    assert report['integrity']['native_library_sha256'] == artifacts['library']['sha256']
    assert report['integrity']['cli_sha256'] == artifacts['binary']['sha256']
    for role in ('baseline','candidate'):
        runtime = protocol['runtimes'][role]
        record = store.read(runtime['manifest']['path'])
        assert record['source_fingerprint_sha256'] == runtime['source_fingerprint_sha256']
        assert record['source_commit'] == runtime['source_commit']
        for item in runtime['runtime'].values():
            assert record['files'][item['path']] == shape(item)
    baseline = store.read(protocol['runtimes']['baseline']['manifest']['path'])
    baseline_build = store.read(out / baseline['build_receipt'])
    assert shape(store.identity(out / baseline['build_receipt'])) == baseline['build_receipt_identity']
    old_snapshot = baseline_build['source_files']
    assert hashlib.sha256(json.dumps(old_snapshot, sort_keys=True, separators=(',', ':')).encode()).hexdigest() == baseline['source_fingerprint_sha256']
    historical = store.read(out / 'prefix-validation-r1.json')
    assert historical['status'] == 'passed' and historical['full_native_library_suite_completed']
    assert historical['runtime'] == shape(protocol['runtimes']['baseline']['manifest'])
    for name, expected in historical['checks'].items():
        receipt = store.read(out / name)
        assert shape(store.identity(out / name)) == expected
        assert receipt['status'] == 'passed' and receipt['returncode'] == 0
        assert receipt['source_files'] == old_snapshot and receipt['sources_unchanged']
        assert receipt['direct_child_reaped'] and receipt['process_group_drained']
    for name, expected in historical['installed_checks'].items():
        assert shape(store.identity(out / name)) == expected
    assert historical['installed_python_tests'] == dict(tests=442, failures=0, errors=0, skipped=0, status='passed')


def verify(packet):
    if not __debug__:
        raise RuntimeError('assertions must remain enabled')
    assert sys.version_info >= (3,13)
    with tempfile.TemporaryDirectory(prefix='shardloom-activation-replay-') as temporary:
        store = Store(packet, Path(temporary))
        manifest = store.manifest
        out = Path(manifest['out'])
        protocol_path = Path(manifest['protocol']['path'])
        protocol = store.read(protocol_path)
        store.verify([manifest['protocol'], manifest['summary'], manifest['validation']])
        store.verify(protocol['frozen_files'], allow_external=True)
        prior_capture = isolation_correction(store, protocol)
        helper = out / 'activation-tools/portable_activation.py'
        assert content(packet / 'verify.py') == shape(store.identity(helper))
        local_tools = store.path(out / 'activation-tools/activation_support.py').parent
        sys.path.insert(0, str(local_tools))
        sys.meta_path.insert(0, NoEngineImports())
        subprocess.Popen = no_process
        import activation_support as support
        import activation_replay as replay
        import lifecycle_capture
        # Only filesystem location changes. Requests keep their original paths,
        # identities, command strings, generation records and policies intact.
        support.OUT, support.ROOT, support.E = out, Path(manifest['worktree']), Path(manifest['original'])
        support.TOOLS = out / 'activation-tools'
        support.FROZEN = support.E / 'investigation-original-harness-r1'
        support.COUNT = out / 'count-cohort-v2'
        original_source = store.read(support.E / 'frozen.json')['source_revision']
        assert protocol['supplemental_harness_source_revision'] == original_source == '0155355e7927e6f8a4747daae5a6daf3438e822b'
        supplemental = {row['path']: row for row in protocol['supplemental_harness_sources']}
        assert set(supplemental) == {str(support.FROZEN / relative) for relative in support.SUPPLEMENTAL_HARNESS}
        for relative, expected_sha256 in support.SUPPLEMENTAL_HARNESS.items():
            record = supplemental[str(support.FROZEN / relative)]
            assert record['sha256'] == expected_sha256
            store.verify([record])
        support.read = store.read
        replay.read, replay.identity, replay.verify = store.read, store.identity, store.verify
        replay.closing_db = store.closing_db
        lifecycle_capture.read = store.read
        assert support.load_protocol(protocol_path) == protocol
        assert protocol['runtimes']['baseline']['source_commit'] == '788a8925e587316ed6f6c3d9ca114fef29937bd5'
        assert protocol['runtimes']['baseline']['source_fingerprint_sha256'] == 'e0192f0cdd42382038ef7aec913b3bd3c3d126159252ddd68d390481dd6387b6'
        validation(store, protocol)
        for role in ('baseline','candidate'):
            for key in ('targets','lifecycle_targets'):
                replay.stage_supervision(protocol, protocol_path, role, key)
        fresh = {role:replay.fresh(protocol,protocol_path,role) for role in ('baseline','candidate')}
        lifecycles = {role:replay.lifecycle(protocol,protocol_path,role) for role in ('baseline','candidate')}
        pools = {row['id']:row['pools'] for row in store.read(out / 'coverage-inventory-r1/activation-cohort-pools.json')['cases']}
        cases, lifecycle_rows, recomputed = replay.evaluate(protocol, fresh, lifecycles, pools)
        expected = store.read(manifest['summary']['path'])
        assert {key:value for key,value in expected.items() if key not in
            ('protocol','case_rows','lifecycle_rows')} == recomputed
        assert expected['status'] == 'passed_complete_replay'
        store.verify([expected['protocol'], expected['case_rows'], expected['lifecycle_rows']])
        stored_cases = [json.loads(line) for line in gzip.decompress(store.raw(expected['case_rows']['path'])).splitlines()]
        assert stored_cases == cases
        assert store.read(expected['lifecycle_rows']['path']) == lifecycle_rows
        assert not any(name in sys.modules for name in ('shardloom._native','polars','pyarrow','duckdb'))
        return dict(status='passed_complete_portable_replay', query_executions=0,
            complete_fresh_results=14996, complete_lifecycle_operations=20,
            baseline_reuse=378, candidate_source_files=len(store.read(out / store.read(
                protocol['runtimes']['candidate']['manifest']['path'])['build_receipt'])['source_files']),
            external_payloads_not_reopened=len(store.external),
            fresh_session=recomputed['fresh_session'], lifecycle=recomputed['lifecycle'],
            performance_disposition=recomputed['performance_disposition'],
            isolation_correction=prior_capture)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    result = verify(args.root.resolve())
    if args.output:
        with args.output.open('x') as stream:
            json.dump(result, stream, indent=2); stream.write('\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
