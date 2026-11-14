"""Replay the complete declined metadata packet using only its saved sources and results."""
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
        assert manifest['schema'] == 'shardloom.native_metadata_portable.v1'
        assert manifest['query_executions_during_packaging'] == 0 and not manifest['publication_attempted']
        assert manifest['complete_raw_ledgers_retained'] and manifest['original_references_retained']
        assert manifest['declared_fresh_results_per_runtime'] == 637
        assert manifest['declared_lifecycle_operations_per_runtime'] == 20
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


def validation(store, protocol):
    from activation_test_inventory import completed_tests

    manifest = store.manifest
    out, root = Path(manifest['out']), Path(manifest['worktree'])
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
    assert build['command'] == ['cargo','build','--offline','--locked','--release',
        '--features','release-user-surfaces','-p','shardloom-cli','-p','shardloom-python']
    assert build['status'] == 'passed' and build['returncode'] == 0
    assert build['sources_unchanged'] and build['process_group_drained'] and build['direct_child_reaped']
    for relative, expected in snapshot.items():
        assert shape(store.identity(root / relative)) == expected, relative
    cargo = ['cargo','test','--offline','--locked','--workspace','--all-targets']
    prefix = 'metadata-final-r1'
    commands = {
        prefix+'-format':['cargo','fmt','--all','--','--check'],
        prefix+'-default-clippy':['cargo','clippy','--offline','--locked','--workspace','--all-targets','--','-D','warnings'],
        'metadata-native-clippy-r3':['cargo','clippy','--offline','--locked','--workspace','--all-targets','--features','release-user-surfaces','--','-D','warnings'],
        prefix+'-file-only':['cargo','check','--offline','--locked','-p','shardloom-vortex','--no-default-features','--features','vortex-file-io'],
        prefix+'-local-only':['cargo','check','--offline','--locked','-p','shardloom-vortex','--no-default-features','--features','vortex-local-primitives'],
        prefix+'-default-inventory':cargo+['--no-fail-fast','--','--list'],
        prefix+'-default-tests':cargo+['--no-fail-fast'],
        prefix+'-native-inventory':cargo+['--features','release-user-surfaces','--no-fail-fast','--','--list'],
        prefix+'-native-tests':cargo+['--features','release-user-surfaces','--no-fail-fast'],
        'metadata-public-r1':[protocol['consumer_python'],'-B',str(out/'run_public_checks.py'),'--runtime','runtime-r1','--name','metadata-public-r1'],
        prefix+'-installed-wheel':[
            str(out.parent/'native-pipeline-sql-minute-2026-11-13/wheel-tools-r1/venv/bin/python'),
            '-B','scripts/prove_native_python_wheel.py','--repo-root',str(root),
            '--output-dir',str(out/(prefix+'-installed-wheel')),
            '--native-library',str(out/'runtime-r1/lib_native.dylib'),
            '--cli-binary',str(out/'runtime-r1/shardloom'),
            '--wheelhouse',str(out.parent/'native-pipeline-consolidation-2026-11-12/wheel-tools-r1/wheelhouse')],
    }
    assert set(proof['checks']) == {name+'.json' for name in commands}
    for name,expected in proof['checks'].items():
        receipt = store.read(out/name)
        assert shape(store.identity(out/name)) == expected
        assert receipt['status'] == 'passed' and receipt['returncode'] == 0
        assert receipt['source_files'] == snapshot and receipt['sources_unchanged']
        assert receipt['process_group_drained'] and receipt['direct_child_reaped']
        assert receipt['cwd'] == str(root) and receipt['command'] == commands[Path(name).stem]
    for kind in ('default','native'):
        listed_path = out/'logs'/(prefix+'-'+kind+'-inventory.log')
        result_path = out/'logs'/(prefix+'-'+kind+'-tests.log')
        listed,results = store.raw(listed_path).decode(),store.raw(result_path).decode()
        wanted = Counter(re.findall(r'^(.+): test$',listed,re.MULTILINE))
        statuses = completed_tests(results)
        assert wanted and wanted == Counter(name for name,_ in statuses)
        totals = [tuple(map(int,row)) for row in re.findall(
            r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored',results)]
        passed = sum(status=='ok' for _,status in statuses)
        ignored = sum(status.startswith('ignored') for _,status in statuses)
        assert tuple(sum(row[index] for row in totals) for index in range(3)) == (passed,0,ignored)
        bins = re.findall(r'^\s+Running (.+)$',listed,re.MULTILINE)
        assert bins == re.findall(r'^\s+Running (.+)$',results,re.MULTILINE)
        assert len(totals) == len(bins)
        assert proof['test_counts'][kind] == dict(passed=passed,failed=0,ignored=ignored,
            test_binaries=len(totals),exact_listed_names_verified=sum(wanted.values()),
            inventory=shape(store.identity(listed_path)),results=shape(store.identity(result_path)))
    assert proof['test_counts']['default']['passed'] == 3345
    assert proof['test_counts']['native']['passed'] == 7223 and proof['test_counts']['native']['ignored'] == 39
    public_path = out/'metadata-public-r1-result.json'
    public = store.read(public_path)
    assert proof['public_matrix'] == shape(store.identity(public_path))
    assert public['status']=='passed' and public['tests']==5
    assert public['failures']==public['errors']==public['skipped']==0
    assert public['source_fingerprint_sha256']==fingerprint
    for name,expected in proof['installed_checks'].items():
        assert shape(store.identity(out/name))==expected
    installed = out/(prefix+'-installed-wheel')
    report,integration = store.read(installed/'report.json'),store.read(installed/'integration.json')
    assert integration == proof['installed_python_tests'] == dict(tests=450,failures=0,errors=0,skipped=0,status='passed')
    assert report['status']=='passed' and not report['publication_attempted']
    artifacts=protocol['runtimes']['candidate']['runtime']
    assert report['integrity']['native_library_sha256']==artifacts['library']['sha256']
    assert report['integrity']['cli_sha256']==artifacts['binary']['sha256']
    for role in ('baseline','candidate'):
        runtime=protocol['runtimes'][role]
        record=store.read(runtime['manifest']['path'])
        assert record['source_fingerprint_sha256']==runtime['source_fingerprint_sha256']
        assert record['source_commit']==runtime['source_commit']
        for item in runtime['runtime'].values():
            assert record['files'][item['path']]==shape(item)
    assert protocol['runtimes']['baseline']['source_fingerprint_sha256']=='504910e4fff98357b1933726687692e1d15b72c250b95df3aadfe1d00404c026'
    accepted=store.read(Path(manifest['activation'])/'activation-final-acceptance-r1.json')
    assert accepted['status']=='passed_complete_acceptance'
    assert accepted['source_fingerprint_sha256']==protocol['runtimes']['baseline']['source_fingerprint_sha256']
    return fingerprint


def verify(packet):
    if not __debug__:
        raise RuntimeError('assertions must remain enabled')
    assert sys.version_info >= (3,13)
    import types
    with tempfile.TemporaryDirectory(prefix='shardloom-metadata-replay-') as temporary:
        store=Store(packet,Path(temporary))
        manifest=store.manifest
        out,activation,root,original=(Path(manifest[key]) for key in ('out','activation','worktree','original'))
        protocol_path=Path(manifest['protocol']['path'])
        protocol=store.read(protocol_path)
        store.verify([manifest[key] for key in ('protocol','validation','replay','disposition')])
        store.verify(protocol['frozen_files'],allow_external=True)
        assert content(packet/'verify.py')==shape(store.identity(out/'portable_metadata.py'))
        sys.meta_path.insert(0,NoEngineImports())
        subprocess.Popen=no_process
        # This import exists only for replay.main's live-source check, which is
        # intentionally not called. No original guard or local source is loaded.
        runtime_stub=types.ModuleType('metadata_runtime')
        runtime_stub.verify_runtime=no_process
        sys.modules['metadata_runtime']=runtime_stub
        sys.path.insert(0,str(store.path(out/'metadata_support.py').parent))
        import metadata_support as support
        import metadata_replay as replay
        # Use only the bundled modules; preserve original request path strings.
        assert Path(support.__file__).resolve()==store.path(out/'metadata_support.py').resolve()
        assert Path(replay.__file__).resolve()==store.path(out/'metadata_replay.py').resolve()
        prior=support.prior
        support.OUT,support.ACTIVATION,support.ROOT,support.E=out,activation,root,original
        support.FROZEN=original/'investigation-original-harness-r1'
        support.TOOLS=out
        support.metadata_cases.OUT,support.metadata_cases.ACTIVATION=out,activation
        prior.OUT,prior.TOOLS,prior.ROOT,prior.E=activation,activation/'activation-tools',root,original
        prior.FROZEN=support.FROZEN
        prior.COUNT=activation/'count-cohort-v2'
        support.read=prior.read=store.read
        replay.read,replay.identity,replay.verify=store.read,store.identity,store.verify
        replay.closing_db=store.closing_db
        assert support.load_protocol(protocol_path)==protocol
        fingerprint=validation(store,protocol)
        fresh={role:replay.fresh(protocol,protocol_path,role) for role in ('baseline','candidate')}
        lifecycles={role:replay.lifecycle(protocol,protocol_path,role) for role in ('baseline','candidate')}
        recomputed=replay.evaluate(protocol,fresh,lifecycles)
        expected=store.read(manifest['replay']['path'])
        assert {key:value for key,value in expected.items() if key not in
            ('protocol','source_fingerprint_sha256','verifier','observations_replayed','lifecycle_operations_replayed')}==recomputed
        assert expected['source_fingerprint_sha256']==fingerprint
        store.verify([expected['protocol'],expected['verifier']])
        assert expected['observations_replayed']==1274 and expected['lifecycle_operations_replayed']==40
        assert recomputed['decision']=='not_admitted' and recomputed['status']=='incomplete_or_invalid_evidence'
        assert recomputed['fresh']['complete'] and recomputed['lifecycle']['complete']
        assert recomputed['work_error'] is not None
        assert recomputed['fresh']['candidate_engine_seconds']>recomputed['fresh']['baseline_engine_seconds']
        disposition=store.read(manifest['disposition']['path'])
        assert disposition['status']=='complete_experiment_decline' and disposition['disposition']=='drop'
        assert disposition['original_replay_status']==recomputed['status']
        assert disposition['original_replay_decision']==recomputed['decision']
        assert disposition['fresh']==recomputed['fresh'] and disposition['lifecycle']==recomputed['lifecycle']
        assert not disposition['thresholds_changed'] and disposition['additional_query_executions']==0
        assert not disposition['whole_metadata_queue_complete'] and not disposition['publication_attempted']
        for name,digest in disposition['inputs'].items():
            assert store.identity(out/name)['sha256']==digest
        controls=store.read(out/'metadata-negative-controls-r1.json')
        assert controls['status']=='passed_negative_controls' and controls['count']==len(controls['controls'])==27
        assert all(row['status']=='rejected' for row in controls['controls'])
        assert len({row['name'] for row in controls['controls']})==27
        assert controls['native_query_executions']==0 and controls['original_unchanged']
        assert controls['synthetic_acceptance_fixture_is_not_observed_evidence']
        store.verify([controls['replay'],controls['verifier']])
        preserved=store.read(out/'metadata-experimental-source-r1.json')
        assert preserved==disposition['experimental_source']
        assert preserved['source_bytes_equal_to_executed_candidate']
        assert preserved['source_fingerprint_sha256']==fingerprint
        assert preserved['parent']==disposition['retained_source_commit']
        assert shape(store.identity(out/'candidate-proposal-r1.patch'))==preserved['patch']
        assert not any(name in sys.modules for name in ('shardloom._native','polars','pyarrow','duckdb'))
        return dict(status='passed_complete_portable_replay',query_executions=0,
            complete_fresh_results=1274,complete_lifecycle_operations=40,baseline_reuse=343,
            source_fingerprint_sha256=fingerprint,external_payloads_not_reopened=len(store.external),
            fresh=recomputed['fresh'],lifecycle=recomputed['lifecycle'],
            disposition='drop',original_replay_decision='not_admitted',negative_controls_bound=27)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,default=Path(__file__).resolve().parent)
    parser.add_argument('--output',type=Path)
    args=parser.parse_args()
    result=verify(args.root.resolve())
    if args.output:
        with args.output.open('x') as stream:
            json.dump(result,stream,indent=2);stream.write('\n')
    print(json.dumps(result))


if __name__=='__main__':
    main()

