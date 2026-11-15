"""Replay the complete FSST experiment from archived bytes, without an engine."""
import argparse
from contextlib import contextmanager
import hashlib
import importlib.abc
import importlib.util
import json
from pathlib import Path, PurePosixPath
import re
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import types


def content(path):
    with path.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    return dict(bytes=path.stat().st_size, sha256=digest)


def shape(record):
    return dict(bytes=record.get('bytes', record.get('generation', {}).get('bytes')), sha256=record['sha256'])


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
    assert all(type(manifest['files'][name]['bytes']) is int and 0 < manifest['files'][name]['bytes'] <= 48 << 20 for name in parts)
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
                    digest.update(block); joined.write(block)
        assert count == expected['bytes'] and digest.hexdigest() == expected['sha256']
        joined.seek(0)
        yield joined


class Store:
    def __init__(self, packet, directory):
        self.packet, self.directory = packet, directory
        self.manifest = json.loads((packet / 'manifest.json').read_bytes())
        manifest = self.manifest
        assert manifest['schema'] == 'shardloom.native_fsst_prefilter_portable.v2'
        assert manifest['query_executions_during_packaging'] == 0 and not manifest['publication_attempted']
        assert manifest['complete_raw_ledgers_retained'] and manifest['original_references_retained']
        assert manifest['declared_fresh_results_per_runtime'] == 9040
        assert manifest['declared_lifecycle_operations_per_runtime'] == 540
        assert not any(path.is_symlink() for path in packet.rglob('*'))
        assert {path.name for path in packet.iterdir() if path.is_file()} == set(manifest['files']) | {'manifest.json'}
        for name, expected in manifest['files'].items():
            safe_name(name)
            assert content(packet / name) == expected, name
        self.members = dict(manifest['archive_members'])
        self.origins = {row['origin']: name for name, row in self.members.items()}
        assert len(self.origins) == len(self.members)
        self.external = manifest['external_artifacts']
        assert not set(self.external) & set(self.origins)
        with archive_stream(packet, manifest, directory) as compressed, tarfile.open(fileobj=compressed, mode='r:gz') as archive:
            entries = archive.getmembers()
            assert len(entries) == len({entry.name for entry in entries})
            assert {entry.name for entry in entries} == set(self.members)
            unpacked = sum(entry.size for entry in entries)
            assert unpacked <= 10 << 30
            assert shutil.disk_usage(directory).free >= (12 << 30) + unpacked
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
                        digest.update(block); destination.write(block)
                assert digest.hexdigest() == expected['sha256'], entry.name
        for row in manifest['supplemental_members']:
            name, origin, transport = row['archive_name'], row['origin'], row['transport']
            safe_name(name); safe_name(transport)
            assert origin not in self.origins and name not in self.members and origin not in self.external
            assert content(packet / transport) == shape(row['identity'])
            target = directory / name
            target.parent.mkdir(parents=True, exist_ok=True)
            with (packet / transport).open('rb') as source, target.open('xb') as destination:
                shutil.copyfileobj(source, destination)
            assert content(target) == shape(row['identity'])
            self.members[name] = dict(origin=origin, identity=row['identity'])
            self.origins[origin] = name

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
                    assert self.identity(origin) == {key: record[key] for key in ('path', 'generation', 'sha256')}

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
        if fullname == 'shardloom._native' or fullname.split('.')[0] in {'polars', 'pyarrow', 'duckdb', 'pyspark', 'datafusion', 'torch'}:
            raise RuntimeError('execution library import forbidden during portable replay: ' + fullname)
        return None


def no_process(*args, **kwargs):
    raise RuntimeError('subprocess execution forbidden during portable replay')


def module(store, name, path):
    spec = importlib.util.spec_from_file_location(name, store.path(path))
    value = importlib.util.module_from_spec(spec)
    sys.modules[name] = value
    spec.loader.exec_module(value)
    return value


def validate_repair(store):
    manifest = store.manifest
    out, root = Path(manifest['out']), Path(manifest['worktree'])
    repair = manifest['archive_repair']
    store.verify([repair['original_packet_manifest'], repair['failure_record']])
    old = store.read(repair['original_packet_manifest']['path'])
    assert old['schema'] == 'shardloom.native_fsst_prefilter_portable.v1'
    for key, value in old.items():
        if key not in {'schema', 'files'}:
            assert manifest[key] == value, key
    for name, expected in old['files'].items():
        target = {'verify.py': 'original-verify.py', 'README.md': 'original-README.md'}.get(name, name)
        assert manifest['files'][target] == expected
    failed = store.read(repair['failure_record']['path'])
    assert failed['returncode'] == 1 and failed['original_paths_forbidden']
    assert "No module named 'source_facts_observations'" in failed['stderr']
    assert repair['original_archive_bytes_unchanged'] and repair['original_protocol_results_and_acceptance_unchanged']
    assert repair['query_executions'] == 0
    protocol = store.read(manifest['protocol']['path'])
    previous = store.read(protocol['original_baseline_protocol']['path'])
    frozen = {row['path']: row for row in previous['frozen_files']}
    expected_modules = {}
    for dirname, prefix, module_name in (
        ('native-source-domain-facts-2026-11-14', 'source-facts', 'source_facts_observations.py'),
        ('native-dictionary-domain-2026-11-14', 'dictionary', 'dictionary_observations.py')):
        directory = out.parent / dirname
        closeout_path = directory / (prefix + '-closeout-r1.json')
        assert store.identity(closeout_path) == frozen[str(closeout_path)]
        closeout = store.read(closeout_path)
        controls_path = directory / (prefix + '-packet-controls-r1.json')
        assert shape(store.identity(controls_path)) == closeout['inputs'][controls_path.name]
        controls = store.read(controls_path)
        assert controls['status'] == 'passed'
        prior_path = root / 'docs/benchmarks/evidence' / dirname / 'packet/manifest.json'
        assert shape(store.identity(prior_path)) == controls['manifest']
        prior = store.read(prior_path)
        origin = str(directory / module_name)
        (record,) = (row['identity'] for row in prior['archive_members'].values() if row['origin'] == origin)
        assert store.identity(origin) == record
        expected_modules[origin] = record
    assert repair['missing_shared_modules'] == expected_modules
    closure = store.read(out / 'fsst-import-closure-r1.json')
    assert closure['status'] == 'passed_relocated_actual_imports' and closure['query_executions'] == 0
    assert closure['original_reads_forbidden'] and closure['network_and_subprocess_forbidden']
    assert repair['import_closure'] == shape(store.identity(out / 'fsst-import-closure-r1.json'))
    assert {'dictionary_observations', 'source_facts_observations', 'fsst_replay'} <= {row['module'] for row in closure['loaded']}
    for row in closure['loaded']:
        assert shape(store.identity(row['origin'])) == row['identity']
    return dict(missing_shared_modules_bound_to_prior_closeouts=True,
        loaded_helpers=len(closure['loaded']), original_archive_bytes_unchanged=True,
        original_failed_packet_preserved=True, query_executions=0)


def bind(store):
    """Load byte-exact archived semantic helpers, then map only their I/O."""
    manifest = store.manifest
    out, root, original, activation = (Path(manifest[key]) for key in ('out', 'worktree', 'original', 'activation'))
    b, n, c = (out.parent / name for name in ('native-predicate-zones-2026-11-14',
        'native-dictionary-domain-2026-11-14', 'native-source-domain-facts-2026-11-14'))
    frozen = original / 'investigation-original-harness-r1'
    sys.meta_path.insert(0, NoEngineImports())
    subprocess.Popen = no_process
    runtime = types.ModuleType('fsst_runtime')
    runtime.OUT, runtime.ROOT = out, root
    runtime.read, runtime.identity = store.read, lambda path: shape(store.identity(path))
    runtime.verify_runtime = no_process
    sys.modules['fsst_runtime'] = runtime
    sys.path.insert(0, str(store.path(out / 'fsst_support.py').parent))
    literals = {}
    for name, path in (
        ('native_text_signature_cases', out / 'input-freeze-r1/native_text_signature_cases.py'),
        ('native_dictionary_text_cases', root / 'python/tests/native_dictionary_text_cases.py'),
        ('native_source_fact_cases', root / 'python/tests/native_source_fact_cases.py')):
        literals[name] = module(store, name, path)
    import fsst_support as support
    import fsst_replay as replay
    import text_recovery_support as recovery
    import text_support as text
    import dictionary_support as dictionary
    import source_facts_support as facts
    import activation_support
    modules = [(support, out / 'fsst_support.py'), (replay, out / 'fsst_replay.py'),
        (replay._reuse, out / 'verify_baseline_reuse.py'),
        (recovery, b / 'text_recovery_support.py'), (text, b / 'text_support.py'),
        (text.text_cases, b / 'text_cases.py'), (dictionary, n / 'dictionary_support.py'),
        (dictionary.dictionary_cases, n / 'dictionary_cases.py'), (facts, c / 'source_facts_support.py'),
        (facts.source_facts_cases, c / 'source_facts_cases.py'),
        (activation_support, activation / 'activation-tools/activation_support.py')]
    for imported, origin in modules:
        assert Path(imported.__file__).resolve() == store.path(origin).resolve(), imported.__name__
    assert text.text_cases.literals is literals['native_text_signature_cases']
    assert dictionary.dictionary_cases.literals is literals['native_dictionary_text_cases']
    assert facts.source_facts_cases.literals is literals['native_source_fact_cases']
    for imported, location in ((support, out), (recovery, b), (text, b), (dictionary, n), (facts, c)):
        imported.OUT, imported.ROOT, imported.E, imported.FROZEN = location, root, original, frozen
        imported.read = store.read
    support.B, support.LITERALS, support.BASELINE_PROTOCOL = b, out / 'input-freeze-r1', b / 'text-protocol-r3.json'
    support.N = recovery.N = text.N = n
    support.C = recovery.C = text.C = c
    dictionary.TOOLS = n
    facts.TOOLS, facts.PREVIOUS = c, n
    dictionary.ACTIVATION = facts.ACTIVATION = activation
    for imported, location in ((text.text_cases, b), (dictionary.dictionary_cases, n), (facts.source_facts_cases, c)):
        imported.OUT, imported.ROOT, imported.E, imported.FROZEN = location, root, original, frozen
    activation_support.OUT, activation_support.TOOLS = activation, activation / 'activation-tools'
    activation_support.ROOT, activation_support.E, activation_support.FROZEN = root, original, frozen
    activation_support.COUNT, activation_support.read = activation / 'count-cohort-v2', store.read
    for imported in (replay, replay._reuse):
        imported.read, imported.identity = store.read, store.identity
        imported.verify, imported.closing_db = store.verify, store.closing_db
    return support, replay


def verify(packet):
    if not __debug__:
        raise RuntimeError('assertions must remain enabled')
    assert sys.version_info >= (3, 13)
    with tempfile.TemporaryDirectory(prefix='shardloom-fsst-replay-') as temporary:
        store = Store(packet, Path(temporary))
        manifest = store.manifest
        out = Path(manifest['out'])
        store.verify([manifest[key] for key in ('protocol', 'validation', 'replay', 'disposition')])
        protocol_path = Path(manifest['protocol']['path'])
        protocol = store.read(protocol_path)
        store.verify(protocol['frozen_files'], allow_external=True)
        assert content(packet / 'verify.py') == shape(store.identity(out / 'portable_fsst_v2.py'))
        repair = validate_repair(store)
        sys.meta_path.insert(0, NoEngineImports())
        validation = module(store, 'fsst_portable_validation', out / 'fsst_portable_validation.py')
        diagnostic = validation.diagnostic(store)
        fingerprint = validation.validate(store, protocol)
        support, replay = bind(store)
        assert support.load_protocol(protocol_path) == protocol
        fresh = {role: replay.fresh(protocol, protocol_path, role) for role in ('baseline', 'candidate')}
        prepared = {role: replay.lifecycle(protocol, protocol_path, role) for role in ('baseline', 'candidate')}
        recomputed = replay.evaluate(protocol, fresh, prepared)
        expected = store.read(manifest['replay']['path'])
        assert {key: value for key, value in expected.items() if key not in
            ('protocol', 'source_fingerprint_sha256', 'verifier', 'observations_replayed', 'lifecycle_operations_replayed')} == recomputed
        assert expected['source_fingerprint_sha256'] == fingerprint
        store.verify([expected['protocol'], expected['verifier']])
        assert expected['observations_replayed'] == 18080 and expected['lifecycle_operations_replayed'] == 1080
        assert recomputed['decision'] == 'retain' and recomputed['status'] == 'passed_complete_acceptance'
        disposition = store.read(manifest['disposition']['path'])
        assert disposition['disposition'] == recomputed['decision']
        assert disposition['status'] == 'complete_experiment_retention'
        assert disposition['original_replay_status'] == recomputed['status']
        assert disposition['original_replay_decision'] == recomputed['decision']
        assert disposition['fresh'] == recomputed['fresh'] and disposition['lifecycle'] == recomputed['lifecycle']
        assert not disposition['thresholds_changed'] and disposition['additional_query_executions'] == 0
        assert not disposition['whole_investigation_queue_complete'] and not disposition['publication_attempted']
        assert not disposition['matcher_lifetime_experiment_complete'] and not disposition['required_spatial_execution_complete']
        for name, wanted in disposition['inputs'].items():
            assert shape(store.identity(out / name)) == wanted
        source = store.read(out / 'fsst-source-r1.json')
        assert source == disposition['candidate_source'] and source['source_bytes_equal_to_executed_candidate']
        assert source['source_fingerprint_sha256'] == fingerprint and source['parent'] == disposition['predecessor_commit']
        assert shape(store.identity(out / 'candidate-proposal-r1.patch')) == source['patch']
        assert not any(name in sys.modules for name in ('shardloom._native', 'polars', 'pyarrow', 'duckdb'))
        return dict(status='passed_complete_portable_replay', query_executions=0,
            complete_fresh_results=18080, complete_lifecycle_operations=1080, reused_baseline_results=9040,
            source_fingerprint_sha256=fingerprint, external_payloads_not_reopened=len(store.external),
            fresh=recomputed['fresh'], lifecycle=recomputed['lifecycle'], disposition=recomputed['decision'],
            untimed_diagnostic=diagnostic, offline_packaging_repair=repair, causal_speedup_claim=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    result = verify(args.root.resolve())
    if args.output:
        with args.output.open('x') as stream:
            json.dump(result, stream, indent=2); stream.write('\n')
    print(json.dumps(result, allow_nan=False))


if __name__ == '__main__':
    main()
