"""Adversarial replay checks for the finite guarantee acceptance."""
import argparse
import copy
import json
from pathlib import Path

from verify_guarantees import CLAIMS, HERE, digest, load, strict_json, verify, verify_completion_link


def check_corruptions(review, packet):
    positive = verify(review, packet)
    mutations = [
        ('missing action', lambda d: d['completed_actions'].pop()),
        ('owner closed', lambda d: d['required_children_closed'].append('PERF-06.recovery')),
        ('optional scope promoted', lambda d: d['optional_rows_promoted'].append('PERF-12.topology')),
        ('competitive gate closed', lambda d: d['competitive_gates_closed'].append('CG-5')),
        ('missing Rust selector', lambda d: d['rust_selectors'].pop()),
        ('duplicate Rust selector', lambda d: d['rust_selectors'].append(d['rust_selectors'][0])),
        ('changed definition line', lambda d: d['rust_selectors'][0].update(definition_line=1)),
        ('changed source identity', lambda d: d['rust_selectors'][0].update(source_sha256='0' * 64)),
        ('missing terminal observation', lambda d: d['rust_selectors'][0]['observations'].clear()),
        ('changed terminal test', lambda d: d['rust_selectors'][0]['observations'][0].update(test_id='other::test')),
        ('invented passing line', lambda d: d['rust_selectors'][0]['observations'][0].update(text='invented ... ok')),
        ('child process promoted', lambda d: next(r for r in d['rust_selectors'] if r['role'] == 'process_child_support').update(role='regression')),
        ('diagnostic capture promoted', lambda d: next(r for r in d['rust_selectors'] if r['role'] == 'diagnostic_capture').update(role='regression')),
        ('different Rust job', lambda d: d['rust_logs'][0]['job'].update(id=1)),
        ('unfinished Rust job', lambda d: d['rust_logs'][0]['job'].update(status='in_progress')),
        ('changed raw Rust log', lambda d: d['rust_logs'][0].update(raw_sha256='0' * 64)),
        ('different checkout tree', lambda d: d['identical_git_trees'].update({d['test_checkout_commit']: '0' * 40})),
        ('missing source commit binding', lambda d: d['source_files'][0]['git_source_sha256'].pop(d['published_runtime_source_commit'])),
        ('missing Python method', lambda d: d['python_selectors'].pop()),
        ('changed Python method', lambda d: d['python_selectors'][0].update(method_sha256='0' * 64)),
        ('missing installed lane', lambda d: d['installed_lanes'].pop()),
        ('Windows lane invented', lambda d: d['installed_lanes'][0].update(lane='py313_windows')),
        ('development wheel relabelled', lambda d: d['installed_lanes'][0]['build_identity'].update(published=True)),
        ('installed skips hidden', lambda d: d['installed_lanes'][0]['proof_identity']['integration_summary'].update(skipped=1)),
        ('installed selector missing', lambda d: d['installed_lanes'][0]['selected_examples'].pop()),
        ('summary promoted to per-test proof', lambda d: d['installed_lanes'][0]['opt_in_output_window'].update(has_per_test_ids_or_log=True)),
        ('missing guarantee review', lambda d: d['guarantees'].pop()),
        ('support test added to guarantee', lambda d: d['guarantees'][0]['tests'].append('sampling_population_recovery_process_child')),
        ('semantic limit erased', lambda d: d['guarantees'][0].update(limit='')),
        ('independent native oracle asserted', lambda d: d['full43'].update(independent_correctness_oracle=True)),
        ('historical payload parity asserted', lambda d: d['full43'].update(historical_payload_parity_claimed=True)),
        ('exhaustive historical search asserted', lambda d: d['full43'].update(exhaustive_historical_search_claimed=True)),
        ('historical candidate missing', lambda d: d['full43']['historical_sources'].pop()),
        ('historical excerpt changed', lambda d: d['full43']['historical_sources'][0]['reference_excerpts'][0].update(text='independent oracle')),
        ('Full43 reference removed', lambda d: d['full43']['reference_members'].pop()),
    ]
    mutations += [(f'unearned {claim}', lambda d, key=claim: d['claims'].update({key: True})) for claim in CLAIMS]
    results = []
    for name, mutate in mutations:
        changed = copy.deepcopy(review)
        mutate(changed)
        try:
            verify(changed, packet)
        except ValueError as error:
            results.append(dict(case=name, status='rejected', reason=str(error)))
        else:
            raise AssertionError('corrupt review accepted: ' + name)
    for name, mutate in [
        ('changed stored member', lambda p: p['members'][0].update(data=p['members'][0]['data'] + 'x')),
        ('duplicate stored member', lambda p: p['members'].append(p['members'][0])),
    ]:
        changed = copy.deepcopy(packet)
        mutate(changed)
        try:
            verify(review, changed)
        except ValueError as error:
            results.append(dict(case=name, status='rejected', reason=str(error)))
        else:
            raise AssertionError('corrupt packet accepted: ' + name)
    for name, raw in [('duplicate JSON field', '{"status":false,"status":true}'),
                      ('nonfinite JSON value', '{"value":NaN}')]:
        try:
            strict_json(raw)
        except ValueError as error:
            results.append(dict(case=name, status='rejected', reason=str(error)))
        else:
            raise AssertionError('invalid JSON accepted: ' + name)
    return dict(status='passed_guarantee_corruption_checks', positive=positive,
                negative_cases=results, negative_case_count=len(results), runtime_executed=False)


def check_completion_link(document):
    positive = verify_completion_link(document)
    mutations = [
        ('missing follow-up review', lambda d: d.pop('followup_review')),
        ('retargeted follow-up review', lambda d: d['followup_review'].update(path='other/review.json')),
        ('absolute follow-up path', lambda d: d['followup_review'].update(path=str(HERE / 'review.json'))),
        ('traversing follow-up path', lambda d: d['followup_review'].update(path='../review.json')),
        ('changed review size', lambda d: d['followup_review'].update(bytes=0)),
        ('changed review digest', lambda d: d['followup_review'].update(sha256='0' * 64)),
        ('changed review status', lambda d: d['followup_review'].update(status='accepted_without_limits')),
        ('missing completed action', lambda d: d['followup_review']['completed_action_ids'].pop()),
        ('duplicate completed action', lambda d: d['followup_review']['completed_action_ids'].append(d['followup_review']['completed_action_ids'][0])),
        ('changed Full43 disposition', lambda d: d['followup_review'].update(independent_full43_reference='accepted_independent_oracle')),
        ('follow-up owner closure', lambda d: d['followup_review']['required_children_closed'].append('PERF-06.recovery')),
        ('follow-up optional promotion', lambda d: d['followup_review']['optional_rows_promoted'].append('PERF-12.topology')),
        ('follow-up competitive closure', lambda d: d['followup_review']['competitive_gates_closed'].append('CG-5')),
        ('pending reviewed action', lambda d: d['required_actions'].append(d['reviewed_action_contracts'][0])),
        ('missing original contracts', lambda d: d.pop('reviewed_action_contracts')),
        ('removed original action', lambda d: d['reviewed_action_contracts'].pop()),
        ('changed original owners', lambda d: d['reviewed_action_contracts'][0]['owners'].pop()),
        ('changed original scope', lambda d: d['reviewed_action_contracts'][0].update(scope='unbounded')),
        ('changed original evidence requirement', lambda d: d['reviewed_action_contracts'][0].update(next_evidence='none')),
    ]
    results = []
    for name, mutate in mutations:
        changed = copy.deepcopy(document)
        mutate(changed)
        try:
            verify_completion_link(changed)
        except ValueError as error:
            results.append(dict(case=name, status='rejected', reason=str(error)))
        else:
            raise AssertionError('corrupt completion link accepted: ' + name)
    return dict(status='passed_completion_link_corruption_checks', positive=positive,
                negative_cases=results, negative_case_count=len(results), runtime_executed=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    result = check_corruptions(*load())
    document = strict_json((HERE.parent / 'perf12-comparisons-2026-10-22/perf-12-acceptance.json').read_bytes())
    result['completion_link'] = check_completion_link(document)
    result['test_source_sha256'] = digest(Path(__file__).read_bytes())
    result['verifier_sha256'] = digest(Path(__file__).with_name('verify_guarantees.py').read_bytes())
    with args.output.open('x') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(json.dumps({key: value for key, value in result.items() if key != 'negative_cases'}))


if __name__ == '__main__':
    main()
