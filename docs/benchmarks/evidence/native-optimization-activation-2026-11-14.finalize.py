"""Bind the completed source, saved-result replay and portable refusal checks."""
from collections import Counter, defaultdict
from datetime import datetime, timezone
import gzip
import hashlib
import json
import math
from pathlib import Path

from activation_runtime import verify_runtime

OUT = Path(__file__).resolve().parent


def read(name):
    return json.loads((OUT / name).read_bytes())


def identity(path):
    with path.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    return dict(bytes=path.stat().st_size, sha256=digest)


def main():
    assert __debug__
    destination = OUT / 'activation-final-acceptance-r1.json'
    assert not destination.exists()
    protocol = read('activation-protocol-r3.json')
    summary = read('activation-replay-r1/summary.json')
    controls = read('activation-packet-controls-r2.json')
    validation = read('activation-validation-r4.json')
    runtime, _ = verify_runtime('activation-runtime-r6')
    assert validation['status'] == 'passed' and validation['complete_test_inventory_verified']
    assert runtime['source_fingerprint_sha256'] == validation['source_fingerprint_sha256']
    assert controls['status'] == 'passed' and controls['controls_rejected'] == 50
    assert len({x['control'] for x in controls['controls']}) == 50
    assert all(x['status'] == 'rejected' for x in controls['controls'])
    assert controls['query_executions'] == 0 and controls['original_packet_unchanged']
    assert controls['manifest'] == identity(OUT / 'activation-packet-r2/manifest.json')
    assert controls['helper'] == identity(OUT / 'activation-tools/test_activation_packet.py')
    portable = controls['relocated_replay']
    assert portable['status'] == 'passed_complete_portable_replay'
    assert portable['complete_fresh_results'] == 14996 and portable['complete_lifecycle_operations'] == 20
    assert portable['fresh_session'] == summary['fresh_session'] and portable['lifecycle'] == summary['lifecycle']
    assert summary['status'] == 'passed_complete_replay'
    assert portable['performance_disposition'] == summary['performance_disposition'] == 'retain_candidate'
    for name in ('activation-complete-replay-r1.json', 'activation-package-r2.json', 'activation-portable-controls-r2.json'):
        receipt = read(name)
        assert receipt['status'] == 'passed' and receipt['returncode'] == 0
        assert receipt['sources_unchanged'] and receipt['direct_child_reaped'] and receipt['process_group_drained']
    for group in ('targets', 'lifecycle_targets'):
        for role, target in protocol[group].items():
            receipt = read(Path(target).name + '-supervision.json')
            assert receipt['status'] == 'passed_capture' and receipt['returncode'] == 0
            assert receipt['runtime_and_source_unchanged'] and receipt['direct_child_reaped'] and receipt['process_group_drained']
            assert receipt['protocol'] == identity(OUT / 'activation-protocol-r3.json')
    reviewed = read('activation-harness-review-r2.json')['files']
    changed = sorted(name for name, expected in reviewed.items() if identity(OUT / name) != expected)
    assert changed == ['activation-tools/package_activation.py', 'activation-tools/portable_activation.py', 'activation-tools/test_activation_packet.py']
    with gzip.open(OUT / 'activation-replay-r1/cases.jsonl.gz', 'rt') as stream:
        rows = [json.loads(line) for line in stream]
    assert len(rows) == len({x['id'] for x in rows}) == 7498
    assert [(x['id'], x['case_identity']) for x in rows] == [
        (x['case']['id'], x['case']['case_identity']) for x in protocol['cases']]
    for role in ('baseline', 'candidate'):
        assert all(x[role]['status'] == 'passed' for x in rows)
        total = math.fsum(x[role]['work']['engine_execution_seconds'] for x in rows)
        assert total == summary['fresh_session'][role + '_engine_seconds']
    regressions = [x for x in rows if x['candidate']['work']['engine_execution_seconds'] > x['baseline']['work']['engine_execution_seconds']]
    assert len(regressions) == summary['fresh_session']['regressions'] == 5189
    activation = defaultdict(Counter)
    for row in rows:
        work = row['candidate']['work']
        assert work['resources']['native_denied_reservations'] == work['resources']['spill_bytes'] == 0
        assert work['resources']['native_peak_reserved_bytes'] <= protocol['execution']['native_memory_bytes']
        tally = activation[row['native_format']]
        tally['cases'] += 1
        tally['scan_reports'] += work['file_scans'] is not None
        tally['runtime_filter_admitted_cases'] += work['join_work']['runtime_filter']['admitted'] > 0
        tally['skew_admitted_cases'] += work['join_work']['skew']['admitted_groups'] > 0
        tally['cyclic_executed_cases'] += work['join_work']['cyclic']['executions'] > 0
        tally['count_partial_admitted_cases'] += work['count_partial_work']['admitted_batches'] > 0
    lifecycle = read('activation-replay-r1/lifecycles.json')
    calls = []
    for item in lifecycle:
        observations = []
        for operation in item['candidate']['operations']:
            scans = operation['file_scans']
            observations.append(dict(engine_seconds=operation['engine_execution_seconds'],
                completed_read_bytes=sum(s['completed_read_bytes'] for s in scans['sources']),
                cross_call_hits=sum(s['segment_reuse']['cross_call_hits'] for s in scans['sources']),
                retained_entries=scans['session_segment_cache']['retained_entries']))
        assert len(observations) == 2 and observations[0]['cross_call_hits'] == 0
        assert observations[1]['cross_call_hits'] == 128
        assert observations[1]['completed_read_bytes'] < observations[0]['completed_read_bytes']
        calls.append(dict(id=item['id'], baseline_engine_seconds=item['baseline_engine_seconds'],
                         candidate_engine_seconds=item['candidate_engine_seconds'], calls=observations))
    for role in ('baseline', 'candidate'):
        assert math.fsum(x[role + '_engine_seconds'] for x in lifecycle) == summary['lifecycle'][role + '_engine_seconds']
    record = dict(schema='shardloom.native_optimization_activation_acceptance.v1',
        status='passed_complete_acceptance', disposition='retain_coherent_activation_candidate',
        created_at=datetime.now(timezone.utc).isoformat(), query_executions_during_acceptance=0,
        source_commit=runtime['source_commit'], source_fingerprint_sha256=runtime['source_fingerprint_sha256'],
        comparison_source=protocol['runtimes']['baseline']['source_commit'],
        fresh_session=summary['fresh_session'], lifecycle=summary['lifecycle'],
        groups=summary['groups'], activation_by_format=dict(activation), lifecycle_calls=calls,
        validation_counts=validation['test_counts'], installed_tests=validation['installed_python_tests'],
        public_tests=read('activation-public-r6-result.json'), portable=portable,
        negative_controls=controls['controls'], negative_controls_rejected=50,
        changed_reviewed_helpers=changed, prior_disturbed_capture=read('activation-isolation-incident-r1.json'),
        remote_engine_head=read('activation-pr-monitor-r7-primary.json'),
        packet='evidence/native-optimization-activation-2026-11-14',
        packet_files={x.name:identity(x) for x in sorted((OUT/'activation-packet-r2').iterdir()) if x.is_file()},
        evidence={name:identity(OUT/name) for name in (
            'activation-protocol-r3.json', 'activation-replay-r1/summary.json',
            'activation-validation-r4.json', 'activation-packet-controls-r2.json',
            'activation-complete-replay-r1.json', 'activation-package-r2.json', 'activation-portable-controls-r2.json',
            'activation-isolation-amendment-primary-r1.json', 'activation-packet-transport-primary-r1.json',
            'activation-final-review-r1.md', 'finalize_activation_acceptance.py')},
        limitations=[*summary['limitations'], '5,189 cases regress; the complete COUNT, Parquet, Arrow IPC, CLI SQL and typed-plan subgroups regress.',
            'The disturbed r2 baseline is retained but excluded from performance acceptance.',
            'Physical per-column reads, device reads and decoded-byte counts remain unavailable.',
            'Metadata follow-ons and broader coverage remain open; no competitive gate, merge or publication is authorized.'])
    with destination.open('x') as stream:
        json.dump(record, stream, indent=2, allow_nan=False)
        stream.write('\n')
    print(json.dumps({k:record[k] for k in ('status','disposition','source_commit','fresh_session','lifecycle','negative_controls_rejected')}))


if __name__ == '__main__':
    main()
