"""Replay the complete text-signature comparison packet using only its saved sources and results."""
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
        assert manifest['schema'] == 'shardloom.native_text_signature_portable.v3'
        assert manifest['query_executions_during_packaging'] == 0 and not manifest['publication_attempted']
        assert manifest['complete_raw_ledgers_retained'] and manifest['original_references_retained']
        assert manifest['declared_fresh_results_per_runtime'] == 9040
        assert manifest['declared_lifecycle_operations_per_runtime'] == 540
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
        self.members = dict(self.members)
        for row in manifest['supplemental_members']:
            origin, name, transport = row['origin'], row['archive_name'], row['transport']
            safe_name(name)
            safe_name(transport)
            assert origin not in self.origins and origin not in self.external
            assert name not in self.members and transport in manifest['files']
            assert manifest['files'][transport] == shape(row['identity'])
            target = directory / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(packet / transport, target)
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


def validate_first_repair(store):
    manifest = store.manifest
    repair = manifest['archive_repair']
    out = Path(manifest['out'])
    old = store.read(repair['original_packet_manifest']['path'])
    store.verify([repair['original_packet_manifest']])
    assert old['schema'] == 'shardloom.native_text_signature_portable.v1'
    for key, value in old.items():
        if key not in {'schema', 'files'}:
            assert manifest[key] == value, key
    assert manifest['archive'] == old['archive']
    for name, expected in old['files'].items():
        destination = {'verify.py': 'original-verify.py', 'README.md': 'original-README.md'}.get(name, name)
        assert manifest['files'][destination] == expected
    assert repair['query_executions'] == 0 and repair['original_archive_bytes_unchanged']
    assert repair['original_protocol_results_and_acceptance_unchanged']
    c = out.parent / 'native-source-domain-facts-2026-11-14'
    prior_closeout = store.read(c / 'source-facts-closeout-r1.json')
    prior_controls = store.read(c / 'source-facts-packet-controls-r1.json')
    assert shape(store.identity(c / 'source-facts-packet-controls-r1.json')) == prior_closeout['inputs']['source-facts-packet-controls-r1.json']
    assert prior_controls['status'] == 'passed' and prior_controls['controls_rejected'] == 50
    prior_manifest_path = Path(manifest['worktree']) / 'docs/benchmarks/evidence/native-source-domain-facts-2026-11-14/packet/manifest.json'
    prior_manifest = store.read(prior_manifest_path)
    assert prior_controls['manifest'] == shape(store.identity(prior_manifest_path))
    origin = str(c / 'source_facts_observations.py')
    earlier = [row for row in prior_manifest['archive_members'].values() if row['origin'] == origin]
    assert len(earlier) == 1 and store.identity(origin) == earlier[0]['identity']
    assert repair['missing_shared_module'] == store.identity(origin)
    failed = store.read(out / 'text-packet-relocated-r1.json')
    assert failed['returncode'] == 1 and failed['original_paths_forbidden']
    assert "No module named 'source_facts_observations'" in failed['stderr']
    assert repair['failure_record'] == store.identity(out / 'text-packet-relocated-r1.json')
    return dict(shared_module_bound_through_frozen_prior_closeout=True,
        original_archive_bytes_unchanged=True, query_executions=0,
        corrected_boundary='offline archive import closure only; original helpers, measurements and decision unchanged')


def validate_repair(store):
    result = validate_first_repair(store)
    manifest = store.manifest
    out = Path(manifest['out'])
    repair = manifest['archive_repair_v2']
    old = store.read(repair['original_packet_manifest']['path'])
    store.verify([repair['original_packet_manifest']])
    assert old['schema'] == 'shardloom.native_text_signature_portable.v2'
    for key, value in old.items():
        if key not in {'schema', 'files', 'supplemental_members'}:
            assert manifest[key] == value, key
    assert manifest['supplemental_members'][:len(old['supplemental_members'])] == old['supplemental_members']
    for name, expected in old['files'].items():
        destination = {'verify.py': 'r2-verify.py', 'README.md': 'r2-README.md'}.get(name, name)
        assert manifest['files'][destination] == expected
    assert repair['query_executions'] == 0 and repair['original_archive_bytes_unchanged']
    assert repair['original_protocol_results_and_acceptance_unchanged']
    n = out.parent / 'native-dictionary-domain-2026-11-14'
    closeout = store.read(n / 'dictionary-closeout-r1.json')
    controls = store.read(n / 'dictionary-packet-controls-r1.json')
    assert shape(store.identity(n / 'dictionary-packet-controls-r1.json')) == closeout['inputs']['dictionary-packet-controls-r1.json']
    assert controls['status'] == 'passed'
    prior_path = Path(manifest['worktree']) / 'docs/benchmarks/evidence/native-dictionary-domain-2026-11-14/packet/manifest.json'
    assert controls['manifest'] == shape(store.identity(prior_path))
    prior = store.read(prior_path)
    origin = str(n / 'dictionary_observations.py')
    earlier = [row for row in prior['archive_members'].values() if row['origin'] == origin]
    assert len(earlier) == 1 and store.identity(origin) == earlier[0]['identity']
    assert repair['missing_shared_module'] == store.identity(origin)
    failed = store.read(out / 'text-packet-relocated-r2.json')
    assert failed['returncode'] == 1 and failed['original_paths_forbidden']
    assert "No module named 'dictionary_observations'" in failed['stderr']
    assert repair['failure_record'] == store.identity(out / 'text-packet-relocated-r2.json')
    result['dictionary_module_bound_through_frozen_prior_closeout'] = True
    result['prior_failed_packets_retained'] = 2
    return result


def bind(store):
    """Import every semantic helper from this packet, then map historical paths."""
    import importlib.util
    import types
    manifest = store.manifest
    out, activation, root, original = (Path(manifest[key]) for key in ('out', 'activation', 'worktree', 'original'))
    n = out.parent / 'native-dictionary-domain-2026-11-14'
    c = out.parent / 'native-source-domain-facts-2026-11-14'
    sys.meta_path.insert(0, NoEngineImports())
    subprocess.Popen = no_process
    runtime_stub = types.ModuleType('text_runtime')
    runtime_stub.verify_runtime = no_process
    sys.modules['text_runtime'] = runtime_stub
    sys.path.insert(0, str(store.path(out / 'text_support.py').parent))
    literals = {}
    for name in ('native_text_signature_cases', 'native_dictionary_text_cases', 'native_source_fact_cases'):
        path = store.path(root / 'python/tests' / (name + '.py'))
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        literals[name] = module
    import text_recovery_support as support
    import text_replay_v3 as replay
    import text_support as text
    import dictionary_support as dictionary
    import source_facts_support as facts
    import activation_support as activation_support
    import text_portable_validation as validation
    import text_portable_provenance as provenance
    modules = [(support, out / 'text_recovery_support.py'), (text, out / 'text_support.py'),
        (replay, out / 'text_replay_v3.py'), (text.text_cases, out / 'text_cases.py'),
        (dictionary, n / 'dictionary_support.py'), (dictionary.dictionary_cases, n / 'dictionary_cases.py'),
        (facts, c / 'source_facts_support.py'), (facts.source_facts_cases, c / 'source_facts_cases.py'),
        (activation_support, activation / 'activation-tools/activation_support.py'),
        (validation, out / 'text_portable_validation.py'), (provenance, out / 'text_portable_provenance.py')]
    for module, origin in modules:
        assert Path(module.__file__).resolve() == store.path(origin).resolve(), module.__name__
    assert text.text_cases.literals is literals['native_text_signature_cases']
    assert dictionary.dictionary_cases.literals is literals['native_dictionary_text_cases']
    assert facts.source_facts_cases.literals is literals['native_source_fact_cases']
    frozen = original / 'investigation-original-harness-r1'
    for module, location in [(support, out), (text, out), (dictionary, n), (facts, c)]:
        module.OUT, module.ROOT, module.E, module.FROZEN = location, root, original, frozen
        module.read = store.read
    support.N = text.N = n
    support.C = text.C = c
    dictionary.TOOLS = n
    facts.TOOLS, facts.PREVIOUS = c, n
    dictionary.ACTIVATION = facts.ACTIVATION = activation
    for module, location in [(text.text_cases, out), (dictionary.dictionary_cases, n), (facts.source_facts_cases, c)]:
        module.OUT, module.ROOT, module.E, module.FROZEN = location, root, original, frozen
    activation_support.OUT, activation_support.TOOLS = activation, activation / 'activation-tools'
    activation_support.ROOT, activation_support.E, activation_support.FROZEN = root, original, frozen
    activation_support.COUNT, activation_support.read = activation / 'count-cohort-v2', store.read
    replay.read, replay.identity, replay.verify, replay.closing_db = store.read, store.identity, store.verify, store.closing_db
    return support, replay, validation, provenance


def verify(packet):
    if not __debug__:
        raise RuntimeError('assertions must remain enabled')
    assert sys.version_info >= (3, 13)
    with tempfile.TemporaryDirectory(prefix='shardloom-text-summary-replay-') as temporary:
        store = Store(packet, Path(temporary))
        manifest = store.manifest
        out = Path(manifest['out'])
        protocol_path = Path(manifest['protocol']['path'])
        protocol = store.read(protocol_path)
        store.verify([manifest[key] for key in ('protocol', 'validation', 'replay', 'disposition')])
        store.verify(protocol['frozen_files'], allow_external=True)
        assert content(packet / 'verify.py') == shape(store.identity(out / 'portable_text_v3.py'))
        repair = validate_repair(store)
        support, replay, validation, provenance = bind(store)
        assert support.load_protocol(protocol_path) == protocol
        fingerprint = validation.validate(store, protocol, environment_bindings)
        diagnostic = provenance.fixtures_and_reads(store, protocol, support)
        construction = provenance.construction(store, support)
        recovery = provenance.recovery(store, protocol, support, replay)
        fresh = {role: replay.fresh(protocol, protocol_path, role) for role in ('baseline', 'candidate')}
        lifecycles = {role: replay.lifecycle(protocol, protocol_path, role) for role in ('baseline', 'candidate')}
        recomputed = replay.evaluate(protocol, fresh, lifecycles)
        expected = store.read(manifest['replay']['path'])
        assert {key: value for key, value in expected.items() if key not in
            ('protocol', 'source_fingerprint_sha256', 'verifier', 'observations_replayed', 'lifecycle_operations_replayed')} == recomputed
        assert expected['source_fingerprint_sha256'] == fingerprint
        store.verify([expected['protocol'], expected['verifier']])
        assert expected['observations_replayed'] == 18080 and expected['lifecycle_operations_replayed'] == 1080
        assert recomputed['decision'] == 'drop' and recomputed['status'] == 'complete_decline'
        assert recomputed['fresh']['complete'] and recomputed['lifecycle']['complete']
        assert recomputed['work_requirements_passed'] and recomputed['measured_logical_summary_work']
        assert len(recomputed['work_witnesses']) == 840
        disposition = store.read(manifest['disposition']['path'])
        assert disposition['disposition'] == recomputed['decision']
        assert disposition['status'] == 'complete_experiment_decline'
        assert disposition['original_replay_status'] == recomputed['status']
        assert disposition['original_replay_decision'] == recomputed['decision']
        assert disposition['fresh'] == recomputed['fresh'] and disposition['lifecycle'] == recomputed['lifecycle']
        assert not disposition['thresholds_changed'] and disposition['additional_query_executions'] == 0
        assert not disposition['whole_investigation_queue_complete'] and not disposition['publication_attempted']
        for name, digest in disposition['inputs'].items():
            assert store.identity(out / name)['sha256'] == digest
        controls = store.read(out / 'text-negative-controls-r1.json')
        assert controls['status'] == 'passed_negative_controls'
        assert controls['count'] == len(controls['controls']) and controls['count'] >= 25
        assert all(row['status'] == 'rejected' for row in controls['controls'])
        assert len({row['name'] for row in controls['controls']}) == controls['count']
        assert controls['native_query_executions'] == 0 and controls['original_unchanged']
        assert controls['synthetic_acceptance_fixture_is_not_observed_evidence']
        store.verify([controls['replay'], controls['verifier']])
        preserved = store.read(out / 'text-source-r1.json')
        assert preserved == disposition['candidate_source']
        assert preserved['source_bytes_equal_to_executed_candidate']
        assert preserved['source_fingerprint_sha256'] == fingerprint
        assert preserved['parent'] == disposition['predecessor_commit']
        assert shape(store.identity(out / 'candidate-proposal-r1.patch')) == preserved['patch']
        assert not any(name in sys.modules for name in ('shardloom._native', 'polars', 'pyarrow', 'duckdb'))
        return dict(status='passed_complete_portable_replay', query_executions=0,
            complete_fresh_results=18080, complete_lifecycle_operations=1080, baseline_reuse=4631,
            source_fingerprint_sha256=fingerprint, external_payloads_not_reopened=len(store.external),
            fresh=recomputed['fresh'], lifecycle=recomputed['lifecycle'],
            disposition=recomputed['decision'], original_replay_decision=recomputed['decision'],
            negative_controls_bound=controls['count'], recovery=recovery,
            read_evidence=diagnostic, construction=construction, archive_repair=repair)


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


