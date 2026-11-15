"""Verify relocated text-summary replay and corruptions without original source/runtime reads."""
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
    packet = out / 'text-packet-r3'
    destination = out / 'text-packet-controls-r3.json'
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
    with tempfile.TemporaryDirectory(prefix='text-summary-relocated-controls-') as temporary:
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
        with (out / 'text-packet-relocated-r3.json').open('x') as stream:
            json.dump(dict(returncode=full.returncode, stdout=full.stdout, stderr=full.stderr,
                original_paths_forbidden=True, working_directory='disposable directory outside original source'), stream, indent=2)
            stream.write('\n')
        assert full.returncode == 0, full.stderr[-6000:]
        positive = json.loads(full.stdout)
        assert positive['status'] == 'passed_complete_portable_replay' and positive['query_executions'] == 0
        spec = importlib.util.spec_from_file_location('text_portable_verifier', packet / 'verify.py')
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
        source = store.path(out / 'text_support.py')
        before = source.read_bytes()
        source.write_bytes(before + b'\n# corruption\n')
        rejected('changed_source_member', lambda: store.identity(out / 'text_support.py'), 'source_identity')
        source.write_bytes(before)
        verifier.validate_repair(store)
        support, replay, validation, provenance = verifier.bind(store)
        validation.validate(store, protocol, verifier.environment_bindings)

        class ChangedStore:
            def __init__(self, path, mutation):
                self.changed_path, self.mutation = str(path), mutation
            def __getattr__(self, key):
                return getattr(store, key)
            def read(self, path):
                value = copy.deepcopy(store.read(path))
                if str(path) == self.changed_path:
                    self.mutation(value)
                return value

        n = out.parent / 'native-dictionary-domain-2026-11-14'
        prior_path = Path(manifest['worktree']) / 'docs/benchmarks/evidence/native-dictionary-domain-2026-11-14/packet/manifest.json'

        def change_module(value):
            rows = [row for row in value['archive_members'].values()
                    if row['origin'] == str(n / 'dictionary_observations.py')]
            assert len(rows) == 1
            rows[0]['identity']['sha256'] = '0' * 64

        for label, path, mutation in (
            ('unbound_dictionary_controls', n / 'dictionary-closeout-r1.json',
                lambda value: value['inputs']['dictionary-packet-controls-r1.json'].__setitem__('sha256', '0' * 64)),
            ('unbound_dictionary_manifest', n / 'dictionary-packet-controls-r1.json',
                lambda value: value['manifest'].__setitem__('sha256', '0' * 64)),
            ('failed_prior_dictionary_controls', n / 'dictionary-packet-controls-r1.json',
                lambda value: value.__setitem__('status', 'failed')),
            ('changed_prior_dictionary_module', prior_path, change_module),
            ('hidden_second_import_failure', out / 'text-packet-relocated-r2.json',
                lambda value: value.__setitem__('returncode', 0)),
            ('changed_prior_repair_archive', out / 'text-packet-r2/manifest.json',
                lambda value: value['archive'].__setitem__('sha256', '0' * 64)),
        ):
            changed = ChangedStore(path, mutation)
            rejected(label, lambda changed=changed: verifier.validate_repair(changed), 'repair_provenance')

        for label, path, mutation in (
            ('missing_required_test_receipt', manifest['validation']['path'],
                lambda value: value['checks'].pop('text-final-r1-native-tests.json')),
            ('filtered_native_tests', out / 'text-final-r1-native-tests.json',
                lambda value: value['command'].insert(2, 'one_test')),
            ('unreaped_validation_process', out / 'text-final-r1-native-tests.json',
                lambda value: value.__setitem__('direct_child_reaped', False)),
            ('validation_source_changed', out / 'text-final-r1-native-tests.json',
                lambda value: value.__setitem__('sources_unchanged', False)),
            ('wrong_installed_test_total', out / 'text-final-r1-installed-wheel/integration.json',
                lambda value: value.__setitem__('tests', 456)),
            ('wrong_public_test_total', out / 'text-public-r2-result.json',
                lambda value: value.__setitem__('tests', 1)),
            ('missing_provider_validation', manifest['validation']['path'],
                lambda value: value['checks'].pop('text-final-r1-array-provider.json')),
            ('failed_provider_test', out / 'text-array-provider-r1/report.json',
                lambda value: value['commands'][0].__setitem__('returncode', 1)),
            ('changed_provider_test_configuration', out / 'text-array-provider-r1/report.json',
                lambda value: value['commands'][0]['command'].remove('--locked')),
            ('changed_runtime_fingerprint', protocol['runtimes']['candidate']['manifest']['path'],
                lambda value: value.__setitem__('source_fingerprint_sha256', '0' * 64)),
            ('changed_prior_native_bytes', out / 'runtime-r2-identity.json',
                lambda value: value['files'][str(out / 'runtime-r2/shardloom')].__setitem__('sha256', '0' * 64)),
            ('unbound_example_only_source_delta', out / 'text-runtime-compatibility-r3.json',
                lambda value: value['changed_source_paths'].pop()),
            ('unbound_public_source_compatibility', manifest['validation']['path'],
                lambda value: value.pop('public_preparation_runtime_compatibility')),
        ):
            changed = ChangedStore(path, mutation)
            rejected(label, lambda changed=changed: validation.validate(changed, protocol, verifier.environment_bindings),
                'source_bound_validation')
        previous = store.read(store.read(out / 'text-baseline-reuse-r1.json')['protocol']['path'])
        verifier.environment_bindings(protocol, previous)
        non_python = next(name for name in protocol['consumer_environment']['inherited_files'] if Path(name).suffix != '.py')
        for label, mutation in (
            ('omitted_consumer_dependency', lambda value: value.__setitem__('frozen_files',
                [row for row in value['frozen_files'] if row['path'] != non_python])),
            ('changed_consumer_python_version', lambda value: value.__setitem__('python_version', 'different')),
            ('wrong_consumer_environment_prefix', lambda value: value['consumer_environment'].__setitem__('prefix', '/different')),
        ):
            changed = copy.deepcopy(protocol)
            mutation(changed)
            rejected(label, lambda changed=changed: verifier.environment_bindings(changed, previous), 'consumer_environment')
        for label, path, mutation in (
            ('missing_fixture_readback', out / 'text-inputs-r1/complete.json', lambda value: value['readbacks'].pop()),
            ('changed_fixture_reference', out / 'text-inputs-r1/complete.json',
                lambda value: value['readbacks'][0].__setitem__('expected_sha256', '0' * 64)),
            ('missing_diagnostic_scan_record', out / 'text-read-evidence-r2/complete.json', lambda value: value['records'].pop()),
            ('changed_diagnostic_read_count', out / 'text-read-evidence-r2/complete.json',
                lambda value: value['records'][0]['observations'][0].__setitem__('completed_positional_read_bytes', 0)),
            ('falsely_claimed_read_reduction', out / 'text-read-construction-analysis-r1.json',
                lambda value: value['completed_positional_read_changes'].__setitem__('fewer', 324)),
        ):
            changed = ChangedStore(path, mutation)
            rejected(label, lambda changed=changed: provenance.fixtures_and_reads(changed, protocol, support), 'read_attribution')
        for label, mutation in (
            ('missing_construction_record', lambda value: value['records'].pop()),
            ('changed_construction_payload', lambda value: value['records'][0]['output'].__setitem__('sha256', '0' * 64)),
            ('changed_construction_clock', lambda value: value['records'][0].__setitem__('native_writer_construction_seconds', -1)),
            ('false_first_use_claim', lambda value: value.__setitem__('first_use_break_even_claim', True)),
        ):
            changed = ChangedStore(out / 'text-construction-r1/complete.json', mutation)
            rejected(label, lambda changed=changed: provenance.construction(changed, support), 'construction_boundary')
        for label, path, mutation in (
            ('hidden_original_harness_failure', out / 'text-interruption-r1.json',
                lambda value: value['status_counts'].__setitem__('harness_error', 0)),
            ('interrupted_stage_given_accepted_total', out / 'text-interruption-r1.json',
                lambda value: value.__setitem__('original_stage_has_no_accepted_total', False)),
            ('successful_observations_repeated', out / 'text-recovery-prefix-r1.json',
                lambda value: value.__setitem__('complete_results_replayed', 4630)),
            ('lock_failure_reclassified_as_success', out / 'text-baseline-r2-supervision.json',
                lambda value: value.__setitem__('status', 'passed_capture')),
            ('missing_independent_protocol_admission', out / 'text-launch-recovery-validation-r1.json',
                lambda value: value.__setitem__('normalized_launch_record_exact', False)),
        ):
            changed = ChangedStore(path, mutation)
            rejected(label, lambda changed=changed: provenance.recovery(changed, protocol, support, replay), 'capture_recovery')
        for module in ('shardloom._native', 'polars', 'pyarrow', 'duckdb', 'pyspark', 'datafusion'):
            rejected('forbidden_import_' + module, lambda module=module: verifier.NoEngineImports().find_spec(module), 'no_engine')
        rejected('forbidden_subprocess', lambda: verifier.no_process(['false']), 'no_engine')
    assert {path.name: identity(path) for path in packet.iterdir() if path.is_file()} == original
    result = dict(status='passed_packet_corruption_controls', count=len(controls), controls=controls,
        relocated_complete_replay=positive, query_executions=0, original_packet_unchanged=True,
        original_source_runtime_evidence_reads_forbidden=True, helper=identity(Path(__file__)),
        manifest=identity(packet / 'manifest.json'))
    with destination.open('x') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(json.dumps(dict(status=result['status'], controls=len(controls), query_executions=0)))


if __name__ == '__main__':
    main()


