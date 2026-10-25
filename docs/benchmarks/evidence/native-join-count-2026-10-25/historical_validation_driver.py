"""Replay and corrupt the compact packet without starting an engine."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import sys
import tempfile

P = Path(__file__).resolve().parent
W = Path('/Users/dylan/.codex/worktrees/public-docs-api/shardloom-local-repo')
E = W / 'docs/benchmarks/evidence/native-join-count-2026-10-25'
spec = importlib.util.spec_from_file_location('join_count_packet_verifier', E / 'verify.py')
v = importlib.util.module_from_spec(spec)
spec.loader.exec_module(v)
sha = lambda raw: hashlib.sha256(raw).hexdigest()


def main():
    report_path = P / 'join-count-portable-validation-r1.json'
    assert not report_path.exists()
    verifier_sha = sha((E / 'verify.py').read_bytes())
    packet = v.Packet(E)
    measured, _, _ = v.comparison(packet, 'measure')
    observed, _, _ = v.comparison(packet, 'observe')
    checks = []

    def rejects(name, function, reason):
        try:
            function()
        except ValueError as error:
            assert reason in str(error), (name, reason, str(error))
            checks.append({'name': name, 'outcome': 'rejected', 'diagnostic': str(error)})
        else:
            raise AssertionError(name + ' unexpectedly accepted')

    with tempfile.TemporaryDirectory(prefix='join-count-portable-r1-', dir=P / 'tmp') as temporary:
        directory = Path(temporary)
        for name in ('verify.py', 'manifest.json', 'packet.tar.gz'):
            shutil.copy2(E / name, directory / name)
        active = True
        forbidden_reads = []

        def audit(event, args):
            if not active or event != 'open' or not isinstance(args[0], (str, bytes)):
                return
            path = str(Path(args[0]).absolute())
            if path.startswith((str(P) + '/', str(W) + '/')) and not path.startswith(str(directory) + '/'):
                forbidden_reads.append(path)
                raise ValueError('original source/input read during portable replay')

        sys.addaudithook(audit)
        result = v.replay(v.Packet(directory))
        active = False
        assert not forbidden_reads and result['status'] == 'passed'
        checks.append({'name': 'isolated_copy_with_original_reads_denied', 'outcome': 'passed'})
        manifest_bytes = (directory / 'manifest.json').read_bytes()
        manifest = json.loads(manifest_bytes)
        manifest['files'].pop()
        (directory / 'manifest.json').write_text(json.dumps(manifest))
        rejects('missing_archived_member', lambda: v.Packet(directory), 'packet member coverage')
        manifest = json.loads(manifest_bytes)
        manifest['files'][0]['path'] = '../outside'
        (directory / 'manifest.json').write_text(json.dumps(manifest))
        rejects('archive_traversal_name', lambda: v.Packet(directory), 'invalid or duplicate file identity')
        (directory / 'manifest.json').write_bytes(manifest_bytes)
        raw = bytearray((directory / 'packet.tar.gz').read_bytes())
        raw[-20] ^= 1
        (directory / 'packet.tar.gz').write_bytes(raw)
        rejects('corrupt_archive', lambda: v.Packet(directory), 'archive SHA256')

    def edited_packet(name, edit):
        changed = copy.copy(packet)
        changed.files = packet.files.copy()
        value = json.loads(changed.files[name])
        edit(value)
        changed.files[name] = json.dumps(value).encode()
        return changed

    missing = edited_packet('join-count-comparison-r1/outcome.json', lambda value: value['calls'].pop())
    rejects('missing_measurement', lambda: v.comparison(missing, 'measure'), 'complete fixed schedule')
    changed = copy.deepcopy(measured)
    changed[('skew', 'candidate', 2, 2)]['engine_execution_ns'] *= 1000
    rejects('changed_second_cohort_time', lambda: v.score(packet, changed), 'recomputed timing medians')

    root = 'join-count-comparison-r1'
    normal = measured[('duplicate_numeric', 'candidate', 1, 1)]
    for label, field, edit, reason in (
        ('wrong_complete_value', 'actual_rows_witness', lambda x: x[0].update(rows=1), 'independent complete values'),
        ('wrong_schema', 'schema_witness', lambda x: x['Struct'][0]['dtypes'][0]['Primitive'].__setitem__(0, 'u32'), 'authoritative output schema'),
        ('wrong_literal_input', 'input_witnesses', lambda x: x['n'].__setitem__(0, 999), 'frozen numeric inputs'),
    ):
        changed = copy.copy(packet)
        changed.files = packet.files.copy()
        record = copy.deepcopy(normal)
        old = record[field][0] if field == 'input_witnesses' else record[field]
        value = json.loads(changed.files[root + '/witnesses/' + old + '.json'])
        edit(value)
        raw = json.dumps(value).encode()
        new = sha(raw)
        changed.files[root + '/witnesses/' + new + '.json'] = raw
        if field == 'input_witnesses': record[field][0] = new
        else: record[field] = new
        rejects(label, lambda: v.inputs_and_answer(changed, root, 'duplicate_numeric', record), reason)

    base = observed[('duplicate_numeric', 'candidate', 0, 0)]
    for label, edit, reason in (
        ('materialized_pairs', lambda r: r['observations'].update(join_output_rows_constructed=1), 'constructed pair count'),
        ('wrong_key_retention', lambda r: r['observations'].update(join_retained_right_rows=65), 'retained build rows'),
        ('incomplete_build', lambda r: r['observations'].update(counted_join_build_rows=1), 'observed counted state'),
        ('wrong_multiplicity', lambda r: r['observations'].update(counted_join_represented_pairs=1), 'observed counted state'),
        ('remaining_owner', lambda r: r.update(reserved_after_cleanup=1), 'record cleanup'),
        ('oversized_memory', lambda r: r.update(peak_reserved_bytes=129 * v.MIB), 'native reservation grant'),
        ('external_fallback', lambda r: r.update(native_io_certificate=r['native_io_certificate'].replace('fallback_attempted: false', 'fallback_attempted: true', 1)), 'no-fallback certificate'),
        ('extra_workers', lambda r: r.update(runtime=r['runtime'].replace('admitted_max_parallelism: 1', 'admitted_max_parallelism: 2')), 'reported runtime'),
    ):
        record = copy.deepcopy(base)
        edit(record)
        rejects(label, lambda: v.check_record(record, 'duplicate_numeric', 'candidate', 'observe', 2097152, 64), reason)
    record = copy.deepcopy(measured[('duplicate_numeric', 'candidate', 1, 1)])
    record['observation_enabled'] = True
    rejects('instrumented_release_record', lambda: v.check_record(record, 'duplicate_numeric', 'candidate', 'measure', 2097152, 64), 'instrumentation isolation')
    certificate = copy.deepcopy(base)
    certificate['native_io_certificate'] = certificate['native_io_certificate'].replace(
        'counted_join_distinct_build_keys=64', 'counted_join_distinct_build_keys=65')
    rejects('false_public_counter', lambda: v.check_record(certificate, 'duplicate_numeric', 'candidate', 'observe', 2097152, 64), 'certificate unique key count')
    value = {'status': 'passed', 'engine_executions': 0, 'packet_sha256': packet.manifest['archive']['sha256'],
        'verifier_sha256': verifier_sha, 'validator_sha256': sha(Path(__file__).read_bytes()),
        'checks': checks, 'portable_result': result, 'temporary_fixture_payloads_retired': True}
    with report_path.open('x') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n')
    print(json.dumps({'status': 'passed', 'checks': len(checks), 'engine_executions': 0}))


if __name__ == '__main__':
    main()
