"""Relocate survivor evidence and check tamper rejection without executing an engine."""
import copy
import gzip  # Load the standard-library archive decoder before denying external reads.
import importlib.util
import json
from pathlib import Path
import shutil
import sys

ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / 'packet-r1'
DESTINATION = ROOT / 'relocated-packet-r2'

def main():
    assert not DESTINATION.exists()
    shutil.copytree(SOURCE, DESTINATION)
    spec = importlib.util.spec_from_file_location('survivor_replay', DESTINATION / 'verify.py')
    verifier = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(verifier)
    active = [True]
    attempts = []
    def audit(event, args):
        if not active[0]:
            return
        if event in ('subprocess.Popen', 'os.system', 'socket.connect', 'socket.getaddrinfo'):
            attempts.append(event)
            raise RuntimeError('offline evidence replay attempted an external operation')
        if event == 'open' and isinstance(args[0], (str, bytes)):
            path = Path(args[0]).resolve()
            if not path.is_relative_to(DESTINATION):
                attempts.append(str(path))
                raise RuntimeError('offline evidence replay read outside the relocated packet')
    sys.addaudithook(audit)
    replay = verifier.verify(DESTINATION)
    assert not attempts
    members = verifier.read_tar((DESTINATION / 'observations.tar.gz').read_bytes())
    original = verifier.records_from_log(members['raw/logs/survivor-final-screen-r1.log'])
    tested = []
    def reject(name, mutation):
        records = copy.deepcopy(original)
        mutation(records)
        try:
            verifier.verify_records(records)
        except (ValueError, KeyError, TypeError):
            tested.append(name)
        else:
            raise AssertionError('corruption not rejected: ' + name)
    reject('missing_case', lambda r: r.pop('dense_filter'))
    reject('changed_complete_value', lambda r: r['tiny_filter'].__setitem__('actual_jsonl', '{"wrong":1}'))
    reject('changed_reference_value', lambda r: r['sparse_filter']['reference_rows'][0].__setitem__('rows', 999))
    reject('boolean_for_integer_result', lambda r: r['tiny_filter'].__setitem__('actual_jsonl',
        r['tiny_filter']['actual_jsonl'].replace('"rows":1', '"rows":true')))
    reject('changed_field_order', lambda r: r['sparse_filter'].__setitem__('actual_schema',
        r['sparse_filter']['actual_schema'].replace('"key","rows"', '"rows","key"')))
    reject('changed_schema_width', lambda r: r['sparse_join_chain'].__setitem__('actual_schema',
        r['sparse_join_chain']['actual_schema'].replace('"i64"', '"i32"')))
    reject('changed_schema_nullability', lambda r: r['sparse_join_chain'].__setitem__('actual_schema',
        r['sparse_join_chain']['actual_schema'].replace('false', 'true', 1)))
    reject('fallback_claim', lambda r: r['sparse_filter'].__setitem__('fallback_attempted', True))
    reject('invented_engine_time', lambda r: r['sparse_filter'].__setitem__('engine_timing', 0))
    reject('invented_zone_exclusions', lambda r: r['sparse_filter'].__setitem__('logical_zone_exclusions', 0))
    reject('missing_input_identity', lambda r: r['sparse_join_chain'].__setitem__('inputs', []))
    reject('changed_logical_geometry', lambda r: r['zoned_sparse_filter']['logical_layout_zones'][0].__setitem__('zone_rows', 512))
    reject('changed_preparation_order', lambda r: r['preparation_boundary']['observations'][0].__setitem__('ordered_values', list(reversed(range(16)))))
    reject('invented_preparation_savings', lambda r: r['preparation_boundary']['observations'][1].__setitem__('consumer_invocations', 1))
    def hidden_compaction_work(records):
        row = records['preparation_boundary']['observations'][3]
        row['all_key_preparations'] = 16
        row['compaction_key_preparations'] = 16 - row['consumer_key_preparations']
    reject('hidden_compaction_preparations', hidden_compaction_work)
    reject('lost_projection_counterexample', lambda r: r['projection_failure_order'].__setitem__('compacted', r['projection_failure_order']['direct']))
    reject('lost_callback_counterexample', lambda r: r['source_callback_order'].__setitem__('direct_calls', 2))
    reject('view_bytes_as_backing_bytes', lambda r: r['pinned_source_allocations'].__setitem__('live_credit_increase_bytes', 64))
    reject('unreleased_pending_credit', lambda r: r['pending_failure_cleanup'].__setitem__('remaining_credits_bytes', 256))
    altered = dict(members)
    first = next(iter(verifier.REGISTRATIONS))
    altered['baseline/' + first] += b'\n'
    try:
        verifier.verify_members(altered)
    except ValueError:
        tested.append('changed_baseline_source')
    else:
        raise AssertionError('changed production baseline was accepted')
    altered = dict(members)
    baseline = json.loads(altered['baseline/source-files.json'])
    baseline['shardloom-vortex/Cargo.toml']['sha256'] = '0' * 64
    altered['baseline/source-files.json'] = json.dumps(baseline).encode()
    try:
        verifier.verify_members(altered)
    except ValueError:
        tested.append('changed_unrelated_production_source')
    else:
        raise AssertionError('changed production source identity was accepted')
    archive = DESTINATION / 'observations.tar.gz'
    saved = archive.read_bytes()
    archive.write_bytes(saved + b'corruption')
    try:
        verifier.verify(DESTINATION)
    except ValueError:
        tested.append('archive_integrity')
    else:
        raise AssertionError('changed archive was accepted')
    archive.write_bytes(saved)
    assert verifier.verify(DESTINATION) == replay and not attempts
    active[0] = False
    receipt = {'status': 'passed', 'relocated_replay': replay,
               'original_paths_engines_subprocesses_network_denied': True,
               'forbidden_operation_attempts': attempts, 'corruption_controls': tested,
               'corruption_control_count': len(tested)}
    with (ROOT / 'packet-controls-r2.json').open('x') as stream:
        json.dump(receipt, stream, sort_keys=True, indent=2)
        stream.write('\n')
    print(json.dumps({'status': 'passed', 'controls': len(tested), 'forbidden_attempts': len(attempts)}))

if __name__ == '__main__':
    main()
