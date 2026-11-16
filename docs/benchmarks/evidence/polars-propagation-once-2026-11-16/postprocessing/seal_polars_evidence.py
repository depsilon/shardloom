"""Seal the accepted offline packet and its later verification receipts."""
import hashlib
import json
from pathlib import Path
import shutil

from verify_polars_relocation import content, final_result

P = Path(__file__).resolve().parents[1]
W = P.parent / 'optimization-investigation-queue-worktree'
PACKET = P / 'polars-packet-r4'
DESTINATION = W / 'docs/benchmarks/evidence/polars-propagation-once-2026-11-16'


def read(path):
    return json.loads(path.read_bytes())


def main():
    assert not DESTINATION.exists()
    manifest = read(PACKET / 'manifest.json')
    result = final_result(P / 'portable-check-r4.log')
    relocation = read(P / 'polars-relocation-verification-r1.json')
    controls = read(P / 'polars-transport-controls-r2.json')
    assert result == relocation['result'] == final_result(P / 'relocation-check-r1.log')
    assert result['status'] == 'passed_complete_portable_evidence_replay_with_retained_failures'
    assert relocation['status'] == controls['status'] == 'passed'
    assert result['queries_executed'] == 0 and result['fullset_competitive_total'] is None
    assert result['polars_attempts_replayed'] == result['historical_native_results_replayed'] == 11579
    assert result['outcomes'] == {'passed':11489,'incorrect':90}
    assert result['evidence_corruptions_rejected'] == 1120
    assert result['memory_peak_exceptions_retained'] == 69
    assert controls['control_count'] == 25 and controls['rejections'] == 24
    assert controls['positive_controls'] == 1 and controls['query_executions'] == 0
    assert controls['verifier'] == content(P / 'postprocessing/check_polars_transport.py')
    for name, wanted in controls['sources'].items():
        assert content(PACKET / name) == wanted
    files = {p.name:content(p) for p in PACKET.iterdir()}
    assert files == relocation['packet_files']
    assert content(P / 'portable-check-r4.log') == relocation['original_log']
    assert content(P / 'relocation-check-r1.log') == relocation['relocated_log']
    guards = ['packet-build-r4', 'portable-check-r4', 'relocation-check-r1',
              'relocation-wrapper-r1', 'transport-controls-portable-r2']
    for name in guards:
        record = read(P / (name + '.json'))
        assert record['status'] == 'passed' and record['returncode'] == 0, name
        assert record['source_unchanged'] and record['direct_child_reaped'] and record['process_group_drained'], name
        assert content(P / (name + '.log')) == {
            'bytes':record['log']['generation']['bytes'], 'sha256':record['log']['sha256']}, name
    assert shutil.disk_usage(W).free >= (12 << 30) + sum(row['bytes'] for row in files.values())
    DESTINATION.mkdir()
    shutil.copytree(PACKET, DESTINATION / 'packet', copy_function=shutil.copyfile)
    mappings = {
        'portable-check-r4.log':'verification/original-replay.log',
        'relocation-check-r1.log':'verification/relocated-replay.log',
        'polars-relocation-verification-r1.json':'verification/relocation.json',
        'polars-transport-controls-r2.json':'verification/transport-controls.json',
        'postprocessing/transport-controls-provenance-r1.json':'verification/transport-controls-provenance.json',
        'postprocessing/check_polars_transport.py':'postprocessing/check_polars_transport.py',
        'postprocessing/verify_polars_relocation.py':'postprocessing/verify_polars_relocation.py',
        'postprocessing/seal_polars_evidence.py':'postprocessing/seal_polars_evidence.py',
    }
    for name in guards:
        mappings[name + '.json'] = 'verification/' + name + '.json'
        mappings[name + '.log'] = 'verification/' + name + '.log'
    bindings = {}
    for source, target in mappings.items():
        destination = DESTINATION / target
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(P / source, destination)
        assert content(destination) == content(P / source)
        bindings[target] = dict(original_path=str(P / source), **content(destination))
    summary = dict(schema='shardloom.polars_once_acceptance.v1',
        status='passed_complete_observation_and_offline_proof_with_retained_failures',
        polars_attempts=11579, outcomes={'passed':11489,'incorrect':90},
        query_executions_during_postprocessing=0, scored_retries=0,
        new_native_observations=0, historical_native_observations_replayed=11579,
        complete_competitive_baseline=False, fullset_competitive_total=None,
        polars_engine_execution_seconds=None, comparable_engine_clock=False, speedup_ratio=None,
        passing_subset=result['passing_subset'],
        historical_native_disposition=result['historical_native_disposition'],
        latest_native_runtime_observed=False,
        original_and_relocated_replay_passed=True,
        evidence_corruptions_rejected=1120, transport_corruptions_rejected=24,
        positive_transport_controls=1, memory_peak_exceptions_retained=69,
        archive_members=len(manifest['archive_members']),
        identity_only_external_payloads=len(manifest['external_artifacts']),
        archive_bytes=manifest['archive']['bytes'], archive_parts=len(manifest['archive']['parts']),
        bulk_inputs_and_installed_engines_reopened_during_portable_replay=False,
        frozen_queries_references_outcomes_and_clocks_unchanged=True,
        failed_or_intermediate_transport_test_source_and_report_variants_unavailable=4,
        provenance_limitation_scope='Synthetic transport controls only; every scored query packet and its frozen source remains retained.',
        bindings=bindings, publication_attempted=False)
    (DESTINATION / 'acceptance.json').write_text(json.dumps(summary, indent=2, allow_nan=False) + '\n')
    (DESTINATION / 'verification/replay-summary.json').write_text(json.dumps(result, indent=2, allow_nan=False) + '\n')
    (DESTINATION / 'README.md').write_text('''# One Polars pass over the saved propagation requests

Exactly 11,579 requests ran once: 11,489 passed and 90 retained a constant-only
projection error in the benchmark adapter. No request was retried and no new
native execution was added. There is no accepted complete-set competitive total.

The passing subset uses 101.07273444440216 seconds in Polars collect calls.
The exact same historical native request IDs use 888.133524476 engine seconds.
These clocks differ; no speedup ratio is claimed. Polars engine time remains
unavailable. The historical native runtime and its original not-admitted
performance/work decision are unchanged; this is not a latest-runtime comparison.

Run `python3 -I -B packet/verify.py` with Python 3.13 or later and every packet
file present. The verifier reopens all 11,579 Polars packets and all 11,579 saved
native packets, reconstructs independent references, checks complete values,
schemas, ordering, clocks and source identities, and rejects 1,120 corruptions.
Original and relocated verification both pass. Neither executes query engines,
spawns processes, uses the network or reads original evidence/input locations.
Allow the unchanged 12 GiB free-space floor plus the unpacked evidence.

Run the small transport controls with:
`python3 -B postprocessing/check_polars_transport.py --packet packet --output /tmp/polars-transport-result.json`
Use a new output path. The exact Store sources reject 24 malformed packets and
pass the valid byte-readback and canonical-directory-alias control. These tests
use tiny synthetic archives, not another workload execution.

The archive contains complete raw ledgers, frozen jobs, original references,
source bytes, preparation/capture receipts, independent input-equivalence and
Vortex-readback proofs, failed attempts, all results and clocks. Bulk inputs
and installed engines are explicitly identity-only. Polars uses declared
IPC/Parquet counterparts for native Vortex slots; native interface and encoding
labels preserve their original slots, not additional Polars APIs or encodings.

All 90 incorrect results remain unchanged. The adapter's constant projection
collapses the input to one row, so COUNT returns 1 instead of the original
population. This is an adapter defect, not evidence of an engine defect.

The periodic memory guard passes, but 69 whole-worker OS high-water readings
exceed its sampled 2.5 GiB threshold. The maximum is 5,302,255,616 bytes. These
are not isolated engine-memory readings or proof of a hard peak cap.

Original offline replay/packaging failures remain archived. The corrected
portable wrapper fixes lazy import lookup and path aliases, then stops the
comparison helper only after its complete paired result exactly matches;
the following location-specific import-map export is replaced by checking and
loading its already frozen source map. Semantic helpers and results are unchanged.
The first synthetic transport test's missing-part exception expectation was
wrong. Its log and receipt remain, but four failed/intermediate source/report
variants were overwritten before preservation. The provenance report identifies
this gap explicitly. Final transport source/report identities are checked; this
limitation affects no scored query source, result, expectation or timing.

`acceptance.json`, `files.json` and `verification/` bind this local evidence.
They do not imply a PR merge, release, full-set baseline or competitive claim.
No repeat, fresh native full11k, Full43 or prepared-lifecycle pass is authorized.
''')
    all_files = {p.relative_to(DESTINATION).as_posix():content(p)
                 for p in DESTINATION.rglob('*') if p.is_file()}
    (DESTINATION / 'files.json').write_text(json.dumps(all_files, indent=2, sort_keys=True) + '\n')
    print(json.dumps(dict(status='sealed', files=len(all_files)+1,
        bytes=sum(row['bytes'] for row in all_files.values()), queries_executed=0,
        path=str(DESTINATION))), flush=True)


if __name__ == '__main__':
    main()
