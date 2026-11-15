"""Verify relocated source-facts replay and corruptions without original source/runtime reads."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


def identity(path):
    with path.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    return dict(bytes=path.stat().st_size, sha256=digest)


def main():
    if not __debug__:
        raise RuntimeError('negative controls require assertions')
    out = Path(__file__).resolve().parent
    packet = out / 'packet-r1'
    destination = out / 'source-facts-packet-controls-r1.json'
    assert not destination.exists()
    original = {path.name: identity(path) for path in packet.iterdir() if path.is_file()}
    controls = []

    def rejected(name, operation, boundary):
        try:
            operation()
        except (AssertionError, KeyError, ValueError, TypeError, RuntimeError) as error:
            controls.append(dict(name=name, status='rejected', boundary=boundary, error=type(error).__name__))
        else:
            raise AssertionError('corrupt evidence accepted: ' + name)

    disabled = subprocess.run([sys.executable, '-I', '-B', '-O', str(packet / 'verify.py')],
        capture_output=True, text=True, timeout=30)
    assert disabled.returncode != 0 and 'assertions must remain enabled' in disabled.stderr
    controls.append(dict(name='disabled_assertions', status='rejected', boundary='entrypoint', error='RuntimeError'))
    with tempfile.TemporaryDirectory(prefix='source-facts-relocated-controls-') as temporary:
        relocated = Path(temporary) / 'packet'
        shutil.copytree(packet, relocated)
        # At interpreter level, forbid reads from the source worktree, evidence directory,
        # and runtime cache. Only the relocated packet and its temporary extraction are allowed.
        runner = '''import os,runpy,sys
blocked=('/Users/dylan/LocalData/shardloom/','/Users/dylan/Documents/shardloom-local-repo/','/Users/dylan/.cache/shardloom/')
def audit(event,args):
 if event=='open' and isinstance(args[0],(str,bytes,os.PathLike)):
  path=os.fsdecode(os.fspath(args[0]))
  if os.path.abspath(path).startswith(blocked):
   raise RuntimeError('original source/runtime/evidence read forbidden: '+path)
 if event in ('socket.connect','subprocess.Popen','os.system'):
  raise RuntimeError('execution or network forbidden during portable replay')
sys.addaudithook(audit)
sys.argv=[sys.argv[1]]
runpy.run_path(sys.argv[0],run_name='__main__')
'''
        full = subprocess.run([sys.executable, '-I', '-B', '-c', runner, str(relocated / 'verify.py')],
            capture_output=True, text=True, timeout=1800, cwd=temporary)
        with (out / 'source-facts-packet-relocated-r1.json').open('x') as stream:
            json.dump(dict(returncode=full.returncode, stdout=full.stdout, stderr=full.stderr,
                original_paths_forbidden=True, working_directory='disposable directory outside original source'), stream, indent=2)
            stream.write('\n')
        assert full.returncode == 0, full.stderr[-6000:]
        positive = json.loads(full.stdout)
        assert positive['status'] == 'passed_complete_portable_replay' and positive['query_executions'] == 0
        spec = importlib.util.spec_from_file_location('source_facts_portable_verifier', packet / 'verify.py')
        verifier = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(verifier)
        extract = Path(temporary) / 'extracted'
        extract.mkdir()
        store = verifier.Store(packet, extract)
        manifest = store.manifest
        protocol = store.read(manifest['protocol']['path'])
        for name in ('../escape', '/escape', 'a//b', 'a\\b', ''):
            rejected('unsafe_archive_name_' + repr(name), lambda name=name: verifier.safe_name(name), 'archive_path')
        rejected('unbundled_read', lambda: store.path(out / 'not-bundled'), 'archive_identity')
        verifier.transport_parts(manifest)
        for label, mutation in (
            ('missing_part', lambda value: value['archive']['parts'].pop()),
            ('changed_transport_size', lambda value: value['archive'].__setitem__('bytes', value['archive']['bytes'] + 1)),
            ('missing_archive_digest', lambda value: value['archive'].pop('sha256')),
            ('wrong_archive_format', lambda value: value['archive'].__setitem__('format', 'unchecked')),
        ):
            changed = copy.deepcopy(manifest)
            mutation(changed)
            rejected(label, lambda changed=changed: verifier.transport_parts(changed), 'archive_transport')
        tiny = Path(temporary) / 'transport'
        tiny.mkdir()
        data = b'complete transport evidence\n'
        names = ['evidence.tar.gz.part-001', 'evidence.tar.gz.part-002']
        for name, value in zip(names, (data[:9], data[9:]), strict=True):
            (tiny / name).write_bytes(value)
        tiny_manifest = dict(files={name: identity(tiny / name) for name in names},
            archive=dict(format='concatenated_gzip_tar', parts=names, bytes=len(data), sha256=hashlib.sha256(data).hexdigest()))
        reordered = copy.deepcopy(tiny_manifest)
        reordered['archive']['parts'].reverse()
        rejected('reordered_parts', lambda: verifier.transport_parts(reordered), 'archive_transport')

        def open_transport(value):
            with verifier.archive_stream(tiny, value, tiny) as stream:
                assert stream.read() == data
        open_transport(tiny_manifest)
        changed = copy.deepcopy(tiny_manifest)
        changed['archive']['sha256'] = '0' * 64
        rejected('changed_complete_archive_digest', lambda: open_transport(changed), 'archive_transport')
        (tiny / names[1]).write_bytes(b'X' + data[10:])
        rejected('changed_archive_bytes', lambda: open_transport(tiny_manifest), 'archive_transport')
        external = next(iter(store.external.values()))['identity']
        rejected('external_payload_read', lambda: store.path(external['path']), 'external_identity_only')
        rejected('external_identity_not_explicitly_allowed', lambda: store.verify([external]), 'external_identity_only')
        changed = copy.deepcopy(external)
        changed['sha256'] = '0' * 64
        rejected('changed_external_identity', lambda: store.verify([changed], allow_external=True), 'external_identity_only')
        source = store.path(out / 'source_facts_support.py')
        before = source.read_bytes()
        source.write_bytes(before + b'\n# corruption\n')
        rejected('changed_source_member', lambda: store.identity(out / 'source_facts_support.py'), 'source_identity')
        source.write_bytes(before)
        sys.path.insert(0, str(source.parent))
        sys.meta_path.insert(0, verifier.NoEngineImports())
        subprocess.Popen = verifier.no_process
        literal_path = store.path(Path(manifest['worktree']) / 'python/tests/native_source_fact_cases.py')
        literal_spec = importlib.util.spec_from_file_location('native_source_fact_cases', literal_path)
        literal = importlib.util.module_from_spec(literal_spec)
        sys.modules[literal_spec.name] = literal
        literal_spec.loader.exec_module(literal)
        import source_facts_support as support
        assert Path(support.source_facts_cases.literals.__file__).resolve() == literal_path.resolve()
        import source_facts_observations as observations
        from query_surface.expanded_freeze import unpack
        from query_surface.expanded_report import raw_files
        from source_facts_transport import worker_outcome
        verifier.validation(store, protocol)

        class ChangedStore:
            def __init__(self, path, mutation):
                self.path, self.mutation = str(path), mutation
            def __getattr__(self, key):
                return getattr(store, key)
            def read(self, path):
                value = copy.deepcopy(store.read(path))
                if str(path) == self.path:
                    self.mutation(value)
                return value

        for label, path, mutation in (
            ('missing_required_test_receipt', manifest['validation']['path'],
                lambda value: value['checks'].pop('source-facts-final-r3-native-tests.json')),
            ('filtered_native_tests', out / 'source-facts-final-r3-native-tests.json',
                lambda value: value['command'].insert(2, 'one_test')),
            ('unreaped_validation_process', out / 'source-facts-final-r3-native-tests.json',
                lambda value: value.__setitem__('direct_child_reaped', False)),
            ('validation_source_changed', out / 'source-facts-final-r3-native-tests.json',
                lambda value: value.__setitem__('sources_unchanged', False)),
            ('wrong_installed_test_total', out / 'source-facts-final-r4-installed-wheel/integration.json',
                lambda value: value.__setitem__('tests', 454)),
            ('wrong_public_test_total', out / 'source-facts-public-r4-result.json',
                lambda value: value.__setitem__('tests', 3)),
            ('missing_provider_validation', manifest['validation']['path'],
                lambda value: value['checks'].pop('source-facts-final-r3-array-provider.json')),
            ('failed_provider_test', out / 'source-facts-array-provider-r3/report.json',
                lambda value: value['commands'][0].__setitem__('returncode', 1)),
            ('changed_provider_test_configuration', out / 'source-facts-array-provider-r3/report.json',
                lambda value: value['commands'][0]['command'].remove('--locked')),
            ('changed_runtime_fingerprint', protocol['runtimes']['candidate']['manifest']['path'],
                lambda value: value.__setitem__('source_fingerprint_sha256', '0' * 64)),
            ('unbound_reused_code_check', manifest['validation']['path'],
                lambda value: value['reused_code_checks'].pop()),
            ('changed_prior_native_bytes', out / 'runtime-r3-identity.json',
                lambda value: value['files'][str(out / 'runtime-r3/shardloom')].__setitem__('sha256', '0' * 64)),
            ('missing_packaging_compatibility', manifest['validation']['path'],
                lambda value: value.pop('packaging_compatibility')),
            ('discarded_original_package_failure', out / 'source-facts-final-r3-installed-wheel/report.json',
                lambda value: value.__setitem__('installation_performed', True)),
            ('reused_old_source_for_fresh_package', out / 'source-facts-final-r4-installed-wheel.json',
                lambda value: value.__setitem__('source_files', store.read(out / 'source-facts-release-build-r3.json')['source_files'])),
        ):
            changed = ChangedStore(path, mutation)
            rejected(label, lambda changed=changed: verifier.validation(changed, protocol), 'source_bound_validation')

        previous = store.read(store.read(out / 'source-facts-baseline-reuse-r1.json')['protocol']['path'])
        verifier.environment_bindings(protocol, previous)
        non_python = next(name for name in protocol['consumer_environment']['inherited_files']
                          if Path(name).suffix != '.py')
        for label, mutation in (
            ('omitted_consumer_native_dependency', lambda value: value.__setitem__('frozen_files',
                [row for row in value['frozen_files'] if row['path'] != non_python])),
            ('changed_consumer_python_version', lambda value: value.__setitem__('python_version', 'different')),
            ('wrong_consumer_environment_prefix', lambda value:
                value['consumer_environment'].__setitem__('prefix', '/different-environment')),
        ):
            changed = copy.deepcopy(protocol)
            mutation(changed)
            rejected(label, lambda changed=changed: verifier.environment_bindings(changed, previous),
                'unchanged_consumer_environment')

        support.OUT, support.ROOT = out, Path(manifest['worktree'])
        verifier.fixture_and_provider_provenance(store, protocol, support)
        for label, path, mutation in (
            ('missing_complete_fixture_readback', out / 'source-facts-inputs-r1/complete.json',
                lambda value: value['readbacks'].pop()),
            ('changed_fixture_reference_digest', out / 'source-facts-inputs-r1/complete.json',
                lambda value: value['readbacks'][0].__setitem__('expected_sha256', '0' * 64)),
            ('changed_preparation_source_fingerprint', out / 'runtime-r1-identity.json',
                lambda value: value.__setitem__('source_fingerprint_sha256', '0' * 64)),
            ('unbound_fixture_reuse_delta', protocol['fixture_runtime_compatibility']['path'],
                lambda value: value['changed_sources'].pop(next(iter(value['changed_sources'])))),
            ('changed_original_provider_license', support.ROOT / 'vendor/vortex-array/upstream-provenance.json',
                lambda value: value.__setitem__('license', 'unknown')),
            ('changed_original_provider_archive_map', support.ROOT / 'vendor/vortex-array/upstream-provenance.json',
                lambda value: value['original_files_sha256'].__setitem__('README.md', '0' * 64)),
        ):
            changed = ChangedStore(path, mutation)
            rejected(label, lambda changed=changed: verifier.fixture_and_provider_provenance(changed, protocol, support),
                'fixture_and_provider_provenance')

        target = Path(protocol['targets']['candidate']) / 'attempts.sqlite'
        selected = next(row['case'] for row in protocol['cases'] if row['kind'] == 'source_facts_v1'
            and row['case']['profile'] == 'source_facts_work_alp_f32'
            and row['case']['requirement'] == 'group' and row['case']['surface'] == 'python_sql')
        with store.closing_db(target, readonly=True) as database:
            blob = database.execute('SELECT packet FROM attempt WHERE id=?', (selected['id'],)).fetchone()[0]
        raw = unpack(blob)
        files = raw_files(raw)
        job, outcome = json.loads(files['one.job.json']), worker_outcome(raw['supervision'], files['one.stdout.json'])
        report = outcome['observation']
        observations.work(report, job)

        def field(value, key, text):
            value['fields'][key] = text
            underlying = value['raw_report']['fields']
            if isinstance(underlying, list):
                entry = next(item for item in underlying if item['key'] == key)
                entry['value'] = text
            else:
                underlying[key] = text

        for label, mutation in (
            ('incomplete_consumption', lambda value: value.__setitem__('complete_consumption', False)),
            ('negative_engine_clock', lambda value: value['clocks'].__setitem__('engine_execution_seconds', -1)),
            ('unreported_engine_clock', lambda value: value['clocks'].__setitem__('engine_timing_reported', False)),
            ('external_fallback', lambda value: field(value, 'external_engine_invoked', 'true')),
            ('denied_memory', lambda value: field(value, 'execution_resource_observed_native_denied_reservations', '1')),
            ('unexplained_spill', lambda value: field(value, 'execution_resource_observed_spill_bytes', '1')),
            ('memory_exceeds_grant', lambda value: field(value,
                'execution_resource_observed_native_peak_reserved_bytes', str(job['memory_bytes'] + 1))),
        ):
            changed = copy.deepcopy(report)
            mutation(changed)
            rejected(label, lambda changed=changed: observations.work(changed, job), 'completed_native_report')
        failed = copy.deepcopy(raw)
        failed['supervision']['returncode'] = 1
        from source_facts_observations import successful_packet
        rejected('successful_fields_from_failed_process', lambda: successful_packet(failed, files), 'process_completion')
        assert not any(name in sys.modules for name in ('shardloom._native', 'polars', 'pyarrow', 'duckdb'))
    assert original == {path.name: identity(path) for path in packet.iterdir() if path.is_file()}
    assert len(controls) == len({row['name'] for row in controls}) == 50
    result = dict(status='passed', controls_rejected=len(controls), controls=controls,
        relocated_replay=positive, original_source_and_runtime_reads_forbidden=True,
        query_executions=0, original_packet_unchanged=True, helper=identity(Path(__file__)), manifest=original['manifest.json'])
    with destination.open('x') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(json.dumps(dict(status='passed', controls_rejected=len(controls), query_executions=0)))


if __name__ == '__main__':
    main()
