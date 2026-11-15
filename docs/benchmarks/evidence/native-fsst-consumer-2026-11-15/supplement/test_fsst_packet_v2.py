"""Relocate complete FSST replay and reject corrupt evidence; execute no queries."""
import argparse
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


def content(path):
    with path.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    return dict(bytes=path.stat().st_size, sha256=digest)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--revision', default='r2')
    args = parser.parse_args()
    assert __debug__ and args.revision.startswith('r') and args.revision[1:].isdigit()
    out = Path(__file__).resolve().parent
    packet = out / 'fsst-packet-r2'
    destination = out / ('fsst-packet-controls-' + args.revision + '.json')
    assert not destination.exists()
    original = {p.name: content(p) for p in packet.iterdir() if p.is_file()}
    controls = []

    def rejected(name, operation, boundary):
        try:
            operation()
        except (AssertionError, KeyError, ValueError, TypeError, RuntimeError) as error:
            controls.append(dict(name=name, status='rejected', boundary=boundary, error=type(error).__name__))
        else:
            raise AssertionError('corrupt evidence accepted: ' + name)

    def mutation(name, value, mutate, validate, boundary):
        changed = copy.deepcopy(value)
        mutate(changed)
        rejected(name, lambda: validate(changed), boundary)

    disabled = subprocess.run([sys.executable, '-I', '-B', '-O', str(packet / 'verify.py')],
        capture_output=True, text=True, timeout=30)
    assert disabled.returncode != 0 and 'assertions must remain enabled' in disabled.stderr
    controls.append(dict(name='disabled_assertions', status='rejected', boundary='entrypoint'))
    with tempfile.TemporaryDirectory(prefix='fsst-relocated-controls-') as temporary:
        directory = Path(temporary)
        relocated = directory / 'packet'
        shutil.copytree(packet, relocated)
        runner = '''import os,runpy,sys
blocked=('/Users/dylan/LocalData/shardloom/','/Users/dylan/Documents/shardloom-local-repo/','/Users/dylan/.cache/shardloom/')
def audit(event,args):
 if event=='open' and isinstance(args[0],(str,bytes,os.PathLike)):
  path=os.path.abspath(os.fsdecode(os.fspath(args[0])))
  if path.startswith(blocked):
   raise RuntimeError('original source/runtime/evidence read forbidden: '+path)
 if event in ('socket.connect','subprocess.Popen','os.system'):
  raise RuntimeError('execution or network forbidden during portable replay')
sys.addaudithook(audit)
sys.argv=[sys.argv[1]]
runpy.run_path(sys.argv[0],run_name='__main__')
'''
        full = subprocess.run([sys.executable, '-I', '-B', '-c', runner, str(relocated / 'verify.py')],
            capture_output=True, text=True, timeout=1800, cwd=directory)
        with (out / ('fsst-packet-relocated-' + args.revision + '.json')).open('x') as stream:
            json.dump(dict(returncode=full.returncode, stdout=full.stdout, stderr=full.stderr,
                original_paths_forbidden=True, network_and_subprocess_forbidden=True,
                working_directory='disposable directory outside original source'), stream, indent=2)
            stream.write('\n')
        assert full.returncode == 0, full.stderr[-6000:]
        positive = json.loads(full.stdout)
        assert positive['status'] == 'passed_complete_portable_replay' and positive['query_executions'] == 0
        assert positive['complete_fresh_results'] == 18080 and positive['complete_lifecycle_operations'] == 1080
        spec = importlib.util.spec_from_file_location('fsst_portable_controls', relocated / 'verify.py')
        verifier = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(verifier)
        extracted = directory / 'extracted'
        extracted.mkdir()
        store = verifier.Store(relocated, extracted)
        manifest = store.manifest
        protocol = store.read(manifest['protocol']['path'])
        verifier.validate_repair(store)
        for name in ('../escape', '/escape', 'a//b', 'a\\b', ''):
            rejected('unsafe_archive_name_' + repr(name), lambda name=name: verifier.safe_name(name), 'archive_path')
        rejected('unbundled_read', lambda: store.path(out / 'not-bundled'), 'archive_identity')
        for label, change in (
            ('missing_part', lambda value: value['archive']['parts'].pop()),
            ('changed_transport_size', lambda value: value['archive'].__setitem__('bytes', value['archive']['bytes'] + 1)),
            ('missing_archive_digest', lambda value: value['archive'].pop('sha256')),
            ('wrong_archive_format', lambda value: value['archive'].__setitem__('format', 'unchecked')),
        ):
            mutation(label, manifest, change, verifier.transport_parts, 'archive_transport')
        tiny = directory / 'transport'
        tiny.mkdir()
        data = b'complete archive transport\n'
        names = ['evidence.tar.gz.part-001', 'evidence.tar.gz.part-002']
        for name, value in zip(names, (data[:9], data[9:]), strict=True):
            (tiny / name).write_bytes(value)
        small = dict(files={name: content(tiny / name) for name in names}, archive=dict(
            format='concatenated_gzip_tar', parts=names, bytes=len(data), sha256=hashlib.sha256(data).hexdigest()))
        mutation('reordered_parts', small, lambda value: value['archive']['parts'].reverse(),
            verifier.transport_parts, 'archive_transport')

        def transport(value):
            with verifier.archive_stream(tiny, value, tiny) as stream:
                assert stream.read() == data

        transport(small)
        mutation('changed_complete_archive_digest', small,
            lambda value: value['archive'].__setitem__('sha256', '0' * 64), transport, 'archive_transport')
        (tiny / names[1]).write_bytes(b'X' + data[10:])
        rejected('changed_archive_bytes', lambda: transport(small), 'archive_transport')
        external = next(iter(store.external.values()))['identity']
        rejected('external_payload_read', lambda: store.path(external['path']), 'external_identity_only')
        rejected('external_not_explicitly_allowed', lambda: store.verify([external]), 'external_identity_only')
        mutation('changed_external_identity', external, lambda value: value.__setitem__('sha256', '0' * 64),
            lambda value: store.verify([value], allow_external=True), 'external_identity_only')
        for name in ('fsst_support.py', 'input-freeze-r1/boolean-references-v1.json.gz'):
            path = store.path(out / name)
            before = path.read_bytes()
            try:
                path.write_bytes(before + b'corruption')
                rejected('changed_member_' + name, lambda name=name: store.identity(out / name), 'source_or_reference_identity')
            finally:
                path.write_bytes(before)
        validation = verifier.module(store, 'fsst_portable_validation', out / 'fsst_portable_validation.py')
        validation.diagnostic(store)
        validation.validate(store, protocol)

        class ChangedStore:
            def __init__(self, path, change):
                self.changed_path, self.change = str(path), change
            def __getattr__(self, key):
                return getattr(store, key)
            def read(self, path):
                value = store.read(path)
                if str(path) == self.changed_path:
                    self.change(value)
                return value

        for label, path, change in (
            ('missing_test_receipt', out / 'fsst-runtime-validation-r1.json',
                lambda value: value['checks'].pop('fsst-prefilter-native-tests-r1.json')),
            ('filtered_native_tests', out / 'fsst-prefilter-native-tests-r1.json',
                lambda value: value['command'].insert(2, 'one_test')),
            ('unreaped_validation_process', out / 'fsst-prefilter-native-tests-r1.json',
                lambda value: value.__setitem__('direct_child_reaped', False)),
            ('validation_source_changed', out / 'fsst-prefilter-native-tests-r1.json',
                lambda value: value.__setitem__('sources_unchanged', False)),
            ('wrong_installed_total', out / 'installed-wheel-r1/integration.json',
                lambda value: value.__setitem__('tests', 454)),
            ('changed_runtime_fingerprint', out / 'runtime-r1-identity.json',
                lambda value: value.__setitem__('source_fingerprint_sha256', '0' * 64)),
            ('missing_required_dispatch', out / 'fsst-runtime-validation-r1.json',
                lambda value: value['untimed_actual_dispatch']['totals'].__setitem__('rejected_rows', 0)),
            ('changed_reuse_count', out / 'fsst-baseline-reuse-r1.json',
                lambda value: value.__setitem__('full_results_verified', 9039)),
            ('failed_acceptance_controls', out / 'fsst-acceptance-controls-r1.json',
                lambda value: value['checks'][0].__setitem__('passed', False)),
        ):
            changed = ChangedStore(path, change)
            rejected(label, lambda changed=changed: validation.validate(changed, protocol), 'source_bound_validation')
        changed = ChangedStore(out / 'validate-prefilter-existing-r1.json',
            lambda value: value['upstream_dispatch']['totals'].__setitem__('exact_rows', 0))
        rejected('changed_untimed_dispatch', lambda: validation.diagnostic(changed), 'complete_diagnostic')
        support, replay = verifier.bind(store)
        assert support.load_protocol(Path(manifest['protocol']['path'])) == protocol
        candidate_target = Path(protocol['targets']['candidate'])
        for label, path, change in (
            ('unclosed_query_process', out / (candidate_target.name + '-supervision.json'),
                lambda value: value.__setitem__('process_group_drained', False)),
            ('omitted_posthash', candidate_target / 'complete.json',
                lambda value: value.__setitem__('posthash_verified', False)),
            ('repeated_candidate_request', candidate_target / 'complete.json',
                lambda value: value.__setitem__('attempts_per_case', 2)),
        ):
            changed = ChangedStore(path, change)
            original_read = replay.read
            replay.read = changed.read
            try:
                rejected(label, lambda: replay.stage(protocol, Path(manifest['protocol']['path']), 'candidate'), 'capture_binding')
            finally:
                replay.read = original_read
        selected = next(row for row in protocol['cases'] if row['kind'] == 'text_signature_v1'
            and row['case']['profile'] == 'text_signature_work_selective_fsst'
            and row['case']['requirement'] == 'group' and row['case']['surface'] == 'python_sql')
        with store.closing_db(candidate_target / 'attempts.sqlite', readonly=True) as db:
            packet_row = db.execute('SELECT packet FROM attempt WHERE id=?', (selected['case']['id'],)).fetchone()
        raw = replay.unpack(packet_row[0])
        files = replay.raw_files(raw)
        job = json.loads(files['one.job.json'])
        outcome = replay.worker_outcome(raw['supervision'], files['one.stdout.json'])
        report = outcome['observation']
        replay.work(report, job)

        def field(value, key, item):
            value['fields'][key] = item
            native = value['raw_report']['fields']
            if isinstance(native, list):
                next(row for row in native if row['key'] == key)['value'] = item
            else:
                native[key] = item

        for label, change in (
            ('incomplete_delivery', lambda value: value.__setitem__('complete_consumption', False)),
            ('unbound_engine_clock', lambda value: value['clocks'].__setitem__('engine_execution_seconds', -1)),
            ('missing_native_certificate', lambda value: field(value, 'local_primitive_native_io_certificate_status', 'missing')),
            ('unbound_memory_grant', lambda value: value['fields'].__setitem__('execution_resource_declared_memory_bytes', '1')),
            ('reserved_exceeds_grant', lambda value: field(value, 'execution_resource_observed_native_reserved_bytes', str(job['memory_bytes'] + 1))),
            ('denied_memory_reservation', lambda value: field(value, 'execution_resource_observed_native_denied_reservations', '1')),
        ):
            mutation(label, report, change, lambda value: replay.work(value, job), 'native_resource_and_clock')
        mutation('raw_packet_corruption', raw,
            lambda value: value['files']['one.stdout.json'].__setitem__('sha256', '0' * 64), replay.raw_files, 'raw_result_identity')
        wanted = replay.result_from_record(job['expected'])
        contract = support.text_cases.validate(job['case']).contract
        for label, change in (
            ('missing_result_row', lambda value: value['rows'].pop()),
            ('extra_result_row', lambda value: value['rows'].append(copy.deepcopy(value['rows'][0]))),
            ('wrong_result_schema', lambda value: value['schema'].reverse()),
        ):
            mutation(label, outcome['actual'], change,
                lambda value: replay.compare(wanted, replay.result_from_record(value), contract), 'complete_result')
        original_outcome = replay.worker_outcome
        for label, key in (('prepared_owner_not_released', 'release_prepared_completed'),
                           ('prepared_session_not_closed', 'client_or_session_closed')):
            def changed_outcome(supervision, data, key=key):
                value = original_outcome(supervision, data)
                value['cleanup'][key] = False
                return value
            replay.worker_outcome = changed_outcome
            try:
                rejected(label, lambda: replay.lifecycle(protocol, Path(manifest['protocol']['path']), 'candidate'),
                    'prepared_owner_lifetime')
            finally:
                replay.worker_outcome = original_outcome
        saved = store.read(manifest['replay']['path'])
        fresh = {role: [pair[role] for pair in saved['pairs']] for role in ('baseline', 'candidate')}
        lifecycle = {role: [pair[role] for pair in saved['lifecycle_pairs']] for role in ('baseline', 'candidate')}
        evaluated = replay.evaluate(protocol, fresh, lifecycle)
        assert evaluated['decision'] == 'retain' and evaluated['fresh'] == saved['fresh']
        short = dict(fresh, candidate=fresh['candidate'][:-1])
        rejected('missing_frozen_case', lambda: replay.evaluate(protocol, short, lifecycle), 'complete_population')
        one = copy.deepcopy(lifecycle['candidate'][0])
        one['operations'].pop()
        missing = dict(lifecycle, candidate=[one, *lifecycle['candidate'][1:]])
        rejected('missing_prepared_operation', lambda: replay.evaluate(protocol, fresh, missing), 'prepared_lifecycle')
        for label, key, value in (('failed_case', 'status', 'incorrect'), ('invalid_resource_evidence', 'evidence_error', 'corrupt')):
            changed = dict(fresh, candidate=[dict(fresh['candidate'][0], **{key: value}), *fresh['candidate'][1:]])
            assert replay.evaluate(protocol, changed, lifecycle)['decision'] == 'not_admitted'
            controls.append(dict(name=label, status='rejected', boundary='retention_decision'))
        for label, changed in (
            ('wrong_saved_decision', dict(saved, decision='drop')),
            ('wrong_saved_engine_total', dict(saved, fresh=dict(saved['fresh'], candidate_engine_seconds=0))),
        ):
            rejected(label, lambda changed=changed: compare_saved(changed, evaluated), 'recomputed_decision')
        for module in ('shardloom._native', 'polars', 'pyarrow', 'duckdb', 'pyspark', 'datafusion'):
            rejected('forbidden_import_' + module, lambda module=module: verifier.NoEngineImports().find_spec(module), 'no_engine')
        rejected('forbidden_subprocess', lambda: verifier.no_process(['false']), 'no_engine')
    assert {p.name: content(p) for p in packet.iterdir() if p.is_file()} == original
    assert len(controls) == len({row['name'] for row in controls})
    result = dict(status='passed_packet_corruption_controls', controls=controls, count=len(controls),
        relocated_complete_replay=positive, query_executions=0, original_packet_unchanged=True,
        original_source_runtime_evidence_reads_forbidden=True, helper=content(Path(__file__)),
        manifest=content(packet / 'manifest.json'))
    with destination.open('x') as stream:
        json.dump(result, stream, indent=2); stream.write('\n')
    print(json.dumps(dict(status=result['status'], controls=len(controls), query_executions=0)))


def compare_saved(saved, evaluated):
    assert {key: value for key, value in saved.items() if key not in
        ('protocol', 'source_fingerprint_sha256', 'verifier', 'observations_replayed', 'lifecycle_operations_replayed')} == evaluated


if __name__ == '__main__':
    main()
