"""Record a useful-behavior integration judgment separately from failed speed gates."""
import json
from pathlib import Path
import sys

OUT = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(OUT), str(OUT / 'postprocessing')]
import selection_support as support
import selection_runtime as runtime
import selection_transport_generations as generations


def main():
    destination = Path(__file__).parent / 'integration-disposition-r1.json'
    assert not destination.exists()
    manifest, build = runtime.verify_runtime()
    protocol = support.load_protocol(OUT / 'selection-protocol-r1.json')
    generations.verify_frozen(protocol)
    guard = runtime.checked_receipt(OUT / 'selection-portable-replay-r2.json', build['source_files'])
    proof = runtime.read(OUT / 'selection-portable-result-r2.json')
    replay = runtime.read(OUT / 'selection-replay-r1.json')
    original = runtime.read(runtime.PREVIOUS / 'propagation-disposition-r1.json')
    assert proof['status'] == 'passed_complete_portable_replay' and proof['query_executions'] == 0
    assert proof['decision'] == replay['decision'] == 'drop' and proof['required_work_complete']
    assert proof['fresh'] == replay['fresh'] and proof['lifecycle'] == replay['lifecycle']
    assert original['disposition'] == proof['original_decision'] == 'not_admitted'
    assert proof['original_gate_unchanged'] and not proof['causal_speedup_claim']
    assert original['maintainer_direction']['candidate_may_be_kept_for_additional_value']
    assert original['maintainer_direction']['preserve_candidate_and_fix_demonstrated_gap']
    assert original['maintainer_direction']['pr_when_coherent_and_verified_then_continue_development']
    packets = {}
    for name, packet in (('propagation', runtime.PREVIOUS / 'propagation-packet-r4'),
                         ('selection', OUT / 'selection-packet-r2')):
        data = runtime.read(packet / 'manifest.json')
        for file, wanted in data['files'].items():
            assert runtime.identity(packet / file) == wanted
        packets[name] = dict(manifest=runtime.identity(packet / 'manifest.json'),
            files=data['files'], archive_bytes=data['archive']['bytes'])
    result = dict(schema='shardloom.native_propagation_integration.v1',
        status='ready_for_pr_after_complete_source_and_portable_verification',
        integration_disposition='retain_useful_behavior_under_maintainer_direction',
        decision_owner='agent integration judgment within the explicit maintainer retention and PR instruction',
        source_commit=manifest['source_commit'], source_tree=manifest['source_tree'],
        source_fingerprint_sha256=manifest['source_fingerprint_sha256'],
        original_performance_decision=original['disposition'],
        focused_performance_decision=replay['decision'], thresholds_changed=False,
        original_fresh=original['fresh'], original_lifecycle=original['lifecycle'],
        focused_fresh=replay['fresh'], focused_lifecycle=replay['lifecycle'],
        authority=original['maintainer_direction'],
        useful_behavior=[
            'Detached compact dictionaries survive shared set alignment and noncontiguous selection.',
            'All 60 formerly missing selected-dictionary witnesses now pass, with all 120 retention and 80 kernel-reduction obligations satisfied.',
            'All 120 grouped COUNT projection witnesses passed in the original comparison and unchanged COUNT source remains covered by final native checks.',
            'One credited exact-table FSST matcher entry stays scoped to an admitted operation; per-backing rejection and results remain local.',
            'Opaque selected-source lifetime is retained without enlarging every shared key owner; the original same-budget temporal pressure regression is fixed.'
        ],
        limitations=[
            'The original 11579-request gate was not admitted; its 60 work failures and higher fresh/prepared totals remain historical facts.',
            'The focused 2190-request gate drops because the complete fresh total increases, despite the lower separate prepared total.',
            'Fewer kernel invocations are not a proven complete-query speedup.',
            'The observed UNION ALL and large all-null increases remain unresolved cost observations, without exclusive phase attribution.',
            'No repeated trial, independent matcher speedup, global eligibility denominator, or competitive claim is established.'
        ],
        next_development='Map resident join request inputs and attribute input, keys, index, payload and downstream consumption before choosing a mechanism.',
        whole_propagation_queue_complete=False, competitive_gates_complete=False,
        additional_query_executions=0, publication_attempted=False,
        proof={name:runtime.identity(path) for name,path in {
            'original_disposition':runtime.PREVIOUS / 'propagation-disposition-r1.json',
            'focused_replay':OUT / 'selection-replay-r1.json',
            'focused_disposition':OUT / 'selection-disposition-r1.json',
            'portable_replay':OUT / 'selection-portable-result-r2.json',
            'portable_guard':OUT / 'selection-portable-replay-r2.json',
            'runtime_validation':OUT / 'selection-runtime-validation-r1.json',
            'cost_verification':OUT / 'postprocessing/selection-cost-primary-verification-r1.json',
        }.items()}, packets=packets, recorder=runtime.identity(Path(__file__)))
    with destination.open('x') as stream:
        json.dump(result, stream, indent=2); stream.write('\n')
    print(json.dumps({key:result[key] for key in ('status','integration_disposition',
        'original_performance_decision','focused_performance_decision','additional_query_executions')}))


if __name__ == '__main__':
    main()
