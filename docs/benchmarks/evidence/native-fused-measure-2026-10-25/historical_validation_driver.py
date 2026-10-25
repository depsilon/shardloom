"""Check isolated replay and reject corruption without starting the engine."""
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
E = W / 'docs/benchmarks/evidence/native-fused-measure-2026-10-25'


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def main():
    report_path = P / 'fused-measure-portable-validation-r3.json'
    assert not report_path.exists()
    spec = importlib.util.spec_from_file_location('fused_packet_verifier', E / 'verify.py')
    v = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(v)
    verifier_sha = sha((E / 'verify.py').read_bytes())
    packet = v.Packet(E)
    checks = []

    def rejects(name, function):
        try:
            function()
        except (AssertionError, KeyError, ValueError, TypeError, FileNotFoundError) as error:
            checks.append({'name': name, 'outcome': 'rejected',
                           'diagnostic': type(error).__name__ + ': ' + str(error)})
        else:
            raise AssertionError('corruption accepted: ' + name)

    with tempfile.TemporaryDirectory(prefix='fused-measure-portable-r3-', dir=P / 'tmp') as temporary:
        directory = Path(temporary)
        for name in ('verify.py', 'manifest.json', 'packet.tar.gz'):
            shutil.copy2(E / name, directory / name)
        active = True
        forbidden = []

        def audit(event, args):
            if not active or event != 'open' or not isinstance(args[0], (str, bytes)):
                return
            path = str(Path(args[0]).absolute())
            if path.startswith((str(P) + '/', str(W) + '/')) and not path.startswith(str(directory) + '/'):
                forbidden.append(path)
                raise ValueError('original file access during isolated replay')

        sys.addaudithook(audit)
        result = v.verify(directory)
        active = False
        assert result['portable_replay_passed'] and not forbidden
        checks.append({'name': 'isolated_copy_with_original_reads_denied', 'outcome': 'passed'})
        manifest_bytes = (directory / 'manifest.json').read_bytes()
        manifest = json.loads(manifest_bytes)
        lost = manifest['files'].pop()
        manifest['raw_member_bytes'] -= lost['bytes']
        (directory / 'manifest.json').write_text(json.dumps(manifest))
        rejects('missing_archive_member', lambda: v.Packet(directory))
        manifest = json.loads(manifest_bytes)
        manifest['files'][0]['path'] = '../outside'
        (directory / 'manifest.json').write_text(json.dumps(manifest))
        rejects('archive_path_traversal', lambda: v.Packet(directory))
        (directory / 'manifest.json').write_bytes(manifest_bytes)
        raw = bytearray((directory / 'packet.tar.gz').read_bytes())
        raw[-20] ^= 1
        (directory / 'packet.tar.gz').write_bytes(raw)
        rejects('corrupt_archive_bytes', lambda: v.Packet(directory))

    def edited_packet(name, edit):
        changed = copy.copy(packet)
        changed.files = packet.files.copy()
        value = json.loads(changed.files[name])
        edit(value)
        changed.files[name] = json.dumps(value).encode()
        return changed

    missing = edited_packet('fused-measure-comparison-r3/outcome.json', lambda x: x['calls'].pop())
    rejects('missing_timing_call', lambda: v.verify_cohort(missing, 'measure'))
    original_missing = edited_packet('fused-measure-observations-r1/outcome.json', lambda x: x['calls'].clear())
    rejects('missing_failed_first_attempt', lambda: v.verify_first_attempt(original_missing))
    changed = edited_packet('fused-measure-observations-r3/protocol.json',
                            lambda x: x['frozen_protocol'].__setitem__('text', 'altered'))
    rejects('changed_frozen_protocol', lambda: v.verify_cohort(changed, 'observe'))

    missing_activation = edited_packet('fused-measure-observations-r2/outcome.json', lambda x: x['calls'].pop())
    rejects('missing_failed_activation_call', lambda: v.verify_cohort(missing_activation, 'observe', 'r2'))
    claimed_activation = edited_packet('fused-measure-failed-observe-verification-r2.json',
        lambda x: x.__setitem__('work_elimination_observations_passed', True))
    rejects('failed_activation_relabelled_as_success', lambda: v.verify_cohort(claimed_activation, 'observe', 'r2'))

    root = 'fused-measure-observations-r3'
    normal = packet.read(root + '/observe-dictionary_dense-candidate-0-0.json')

    def witness(identity):
        return packet.read(root + '/witnesses/' + identity + '.json')

    v.values(normal, witness)
    for label, field, edit in (
        ('wrong_ordered_result', 'actual_rows_witness', lambda x: x.reverse()),
        ('wrong_sum', 'actual_rows_witness', lambda x: x[0].__setitem__('sum', 1.0)),
        ('wrong_count_type', 'actual_rows_witness', lambda x: x[0].__setitem__('rows', True)),
        ('wrong_schema', 'schema_witness', lambda x: x['Struct'][0]['dtypes'][3]['Primitive'].__setitem__(1, False)),
        ('wrong_complete_input', 'input_witness', lambda x: x['n'].__setitem__(1, 999)),
    ):
        value = witness(normal[field])
        edit(value)
        rejects(label, lambda: v.values(normal, lambda identity: value if identity == normal[field] else witness(identity)))
    rejects('wrong_original_json', lambda: v.values(normal, lambda identity:
            '{}' if identity == normal['raw_rows_witness'] else witness(identity)))
    for label, edit in (
        ('missing_actual_dictionary_domains', lambda x: x['observations'].__setitem__('variable_dictionary_domains', 0)),
        ('wrong_source_rows', lambda x: x.__setitem__('native_io_certificate', x['native_io_certificate'].replace('delivered_scan_rows=' + str(x['selected_rows']), 'delivered_scan_rows=0'))),
        ('extra_dictionary_domains', lambda x: x['observations'].__setitem__('variable_dictionary_domains', 1 + x['observations']['variable_dictionary_domains'])),
        ('missing_dictionary_values', lambda x: x['observations'].__setitem__('variable_dictionary_domain_values', 0)),
        ('intermediate_still_constructed', lambda x: x['observations'].__setitem__('expression_rows_constructed', 1)),
        ('wrong_mapped_rows', lambda x: x['observations'].__setitem__('fused_measure_rows', 0)),
        ('wrong_aggregate_updates', lambda x: x['observations'].__setitem__('aggregate_row_updates', 0)),
        ('false_public_activation', lambda x: x.__setitem__('native_io_certificate', x['native_io_certificate'].replace('fused_measure_stages=1', 'fused_measure_stages=0'))),
        ('owner_leak', lambda x: x.__setitem__('reserved_after_cleanup', 1)),
        ('over_grant', lambda x: x.__setitem__('peak_reserved_bytes', 129 * v.MIB)),
        ('fallback', lambda x: x.__setitem__('native_io_certificate', x['native_io_certificate'].replace('fallback_attempted: false', 'fallback_attempted: true'))),
        ('extra_cpu', lambda x: x.__setitem__('runtime', x['runtime'].replace('admitted_max_parallelism: 1', 'admitted_max_parallelism: 2'))),
        ('missing_clock', lambda x: x.pop('engine_execution_ns')),
    ):
        record = copy.deepcopy(normal)
        edit(record)
        rejects(label, lambda: v.values(record, witness))
    observed, observation = v.verify_cohort(packet, 'observe')
    measured, measurement = v.verify_cohort(packet, 'measure')
    original = v.verify_first_attempt(packet)
    _, failed_observation = v.verify_cohort(packet, 'observe', 'r2')
    changed = edited_packet('fused-measure-source-candidate-r2.json',
                            lambda x: x['after_source_inventory'].__setitem__('Cargo.toml', '0' * 64))
    rejects('production_change_hidden_as_harness_fix',
            lambda: v.verify_harness_repair(changed, original, failed_observation))
    hidden_runtime = copy.deepcopy(observation)
    hidden_runtime['runtime_manifests']['candidate']['source_inventory']['Cargo.toml'] = '0' * 64
    rejects('encoding_fix_changes_unrelated_runtime', lambda: v.verify_encoding_correction(packet, failed_observation, hidden_runtime))
    changed_baseline = copy.deepcopy(observation)
    changed_baseline['runtime_manifests']['baseline']['binary_sha256'] = '0' * 64
    rejects('reused_baseline_identity_changed', lambda: v.verify_encoding_correction(packet, failed_observation, changed_baseline))
    missing_source = copy.copy(packet)
    missing_source.files = packet.files.copy()
    del missing_source.files['fused-measure-failed-tests-source-r3.tar.gz']
    rejects('failed_test_source_missing', lambda: v.verify_encoding_correction(missing_source, failed_observation, observation))
    records = {}
    for case, role, cohort, sample in v.schedule('measure'):
        records[(case, role, cohort, sample)] = packet.read(
            f'fused-measure-comparison-r3/measure-{case}-{role}-{cohort}-{sample}.json')
    assert v.score(records) == {key: measured[key] for key in ('all_timing_gates_passed', 'cells')}
    changed = copy.deepcopy(records)
    for sample in (1, 2, 3):
        changed[('dictionary_sparse', 'candidate', 2, sample)]['engine_execution_ns'] *= 1000
    changed_score = v.score(changed)
    assert not changed_score['all_timing_gates_passed'] and changed_score != v.score(records)
    checks.append({'name': 'second_cohort_regression_changes_frozen_decision', 'outcome': 'passed'})
    result = {'status': 'passed', 'engine_executions': 0, 'checks': checks,
        'verifier_sha256': verifier_sha, 'validator_sha256': sha(Path(__file__).read_bytes()),
        'packet_sha256': packet.manifest['archive']['sha256'], 'portable_result': result,
        'temporary_fixtures_retired': True}
    with report_path.open('x') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(json.dumps({'status': 'passed', 'checks': len(checks), 'engine_executions': 0}))


if __name__ == '__main__':
    main()
