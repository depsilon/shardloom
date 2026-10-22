"""Verify the declared workload gap matrix against its portable source evidence."""
import argparse
import copy
import json
import lzma
from pathlib import Path
import re

from verify_packet_929 import Evidence, digest, replay, require, strict_json


def check_matrix(repository, document, accepted, protocol, payloads):
    require(document['status'] == 'current_declared_matrix_accepted_remaining_guarantees_explicit',
            'acceptance status differs')
    require(document['claim_gate_status'] == 'not_claim_grade'
            and document['required_children_completed_by_this_assessment'] == []
            and document['optional_rows_promoted'] == [], 'unearned closure or promotion')
    expected_claims = {'broad_engine_parity': False, 'competitive_superiority': False,
        'production_certification': False, 'new_runtime_implementation': False,
        'current_declared_matrix_executed': True, 'engine_performance_comparison': False,
        'package_publication_by_this_assessment': False, 'fallback_attempted': False,
        'external_engine_used_as_fallback': False}
    require(document['claims'] == expected_claims, 'unearned claim')
    require(document['competitive_gate_ids_preserved'] == ['CG-' + str(i) for i in range(1, 24)],
            'competitive gate inventory differs')
    for source in document['source_inputs']:
        raw = (repository / source['path']).read_bytes()
        require(len(raw) == source['bytes'] and digest(raw) == source['sha256'], 'source changed')
    plan = (repository / 'docs/architecture/phased-execution-plan.md').read_text()
    required = re.findall(r'^\s*- \[ \] `(PERF-\d+\.[a-z_]+)` \*\*Required for the selected workload contract\.\*\*',
                          plan, re.MULTILINE)
    require(len(required) == 15 and document['required_child_ids'] == required,
            'required contract inventory differs')
    require([row['child_id'] for row in document['protected_public_contracts']] == required,
            'protected contract inventory differs')
    for row in document['protected_public_contracts']:
        require(row['required_contract'] in plan, 'protected contract changed')
    require(document['as_of_runtime_source_commit'] == protocol['runtime_source_commit']
            and document['current_scope']['source_commit'] == protocol['source_commit']
            and document['current_scope']['runtime_sha256'] == protocol['runtime']['sha256'],
            'runtime or harness identity differs')
    for key in ('runtime_source_commit', 'host', 'rows', 'dim_rows', 'dataset_profile',
                'fact_parts', 'repeats_per_cell', 'resources', 'cache_context', 'baseline'):
        require(document['current_scope'][key] == protocol[key], 'declared scope differs: ' + key)
    require(document['current_scope']['native_records'] == 704
            and document['current_scope']['independent_reference_records'] == 704
            and document['current_scope']['accepted_cohorts'] == 18
            and document['current_scope']['complete_input_output_cartesian_product'] is False,
            'matrix extent differs')
    interruption = document['interruption']
    require(interruption['original_failure'] == accepted['original_failure']
            and interruption['failed_original_cohort'] == accepted['original_incomplete_cohort']
            and interruption['continuation'] == accepted['continuation_result']
            and interruption['incomplete_records_counted'] == 0
            and interruption['limits_changed'] is False
            and interruption['uninterrupted_single_run_claimed'] is False,
            'interruption accounting differs')
    require(len(document['workload_gap_matrix']) == 22, 'task count differs')
    require({row['task'] for row in document['workload_gap_matrix']} == set(protocol['workload_declarations']),
            'task set differs')
    for row in document['workload_gap_matrix']:
        name = row['task']
        require(row['declaration'] == protocol['workload_declarations'][name], 'task declaration differs')
        require(row['independent_baseline']['observed_version'] == protocol['baseline']['version']
                and row['scale_resources_ref'] == row['independent_baseline']['settings_ref'] == 'current_scope',
                'task reference differs')
        outcome = row['current_shardloom_outcome']
        require(outcome['status'] == 'passed_current_source_complete_normalized_results'
                and outcome['observations'] == accepted['tasks'][name]
                and outcome['complete_result_pairs'] == len(accepted['tasks'][name]) == 32,
                'task observations differ')
        original = [value for value in payloads['records'] if value['task'] == name]
        contract = outcome['original_collected_statement_contracts']
        require(contract['logical_schema_statements'] == len(original)
                and contract['terminal_order_statements'] == sum(r['terminal_order_checked'] for r in original)
                and contract['null_values_observed'] == sum(r['null_values'] for r in original)
                and contract['baseline_logical_type_equality_claimed'] is False
                and contract['all_null_empty_precision_or_pressure_coverage_claimed'] is False,
                'original payload scope differs')
    require(len(payloads['records']) == payloads['complete_native_statements'] == 368
            and payloads['terminal_order_statements'] == 48
            and payloads['observed_null_values'] == 0, 'original payload inventory differs')
    require(document['historical_comparison']['scope']['observed_baseline_version'] == '2.2.3',
            'historical reference relabelled')
    require(document['full43']['observed']['independent_correctness_oracle'] is False
            and document['full43']['observed']['reference_kind'] == 'retained_native_regression',
            'native regression relabelled as independent oracle')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository', type=Path, required=True)
    parser.add_argument('--acceptance', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    repository = args.repository.resolve()
    document = strict_json(args.acceptance.read_bytes())
    artifact_directory = args.acceptance.resolve().parent
    artifact_prefix = 'docs/benchmarks/evidence/perf12-comparisons-2026-10-22/'

    def artifact(reference):
        require(reference['path'].startswith(artifact_prefix), 'unexpected artifact location')
        name = reference['path'].removeprefix(artifact_prefix)
        require(name == Path(name).name, 'artifact path is not a direct child')
        return artifact_directory / name

    for reference in document['evidence'].values():
        if reference['path'].startswith('docs/'):
            raw = artifact(reference).read_bytes()
            require(len(raw) == reference['bytes'] and digest(raw) == reference['sha256'],
                    'acceptance evidence identity differs')
    manifest = strict_json(artifact(document['evidence']['manifest']).read_bytes())
    packet = strict_json(lzma.decompress(artifact(document['evidence']['packet']).read_bytes()))
    replay_result = replay(repository, manifest, packet)
    evidence = Evidence(packet)
    accepted = evidence.read(evidence.check(manifest['acceptance']))
    protocol = evidence.read(evidence.check(manifest['protocol']))
    payloads = strict_json(artifact(document['evidence']['original_payload_contracts']).read_bytes())
    check_matrix(repository, document, accepted, protocol, payloads)
    negatives = []
    mutations = [
        ('missing comparison', lambda d: d['workload_gap_matrix'][0]['current_shardloom_outcome']['observations'].pop()),
        ('promoted optional scope', lambda d: d['optional_rows_promoted'].append('PERF-03.codec_preparation')),
        ('unearned schema parity', lambda d: d['workload_gap_matrix'][0]['current_shardloom_outcome']['original_collected_statement_contracts'].update(baseline_logical_type_equality_claimed=True)),
        ('independent Full43 claim', lambda d: d['full43']['observed'].update(independent_correctness_oracle=True)),
        ('historical baseline relabelled', lambda d: d['historical_comparison']['scope'].update(observed_baseline_version='3.0.3')),
    ]
    for name, mutate in mutations:
        changed = copy.deepcopy(document)
        mutate(changed)
        try:
            check_matrix(repository, changed, accepted, protocol, payloads)
        except ValueError as error:
            negatives.append({'case': name, 'status': 'rejected', 'reason': str(error)})
        else:
            raise ValueError('altered acceptance accepted: ' + name)
    result = {'status': 'passed_declared_acceptance', 'acceptance_sha256': digest(args.acceptance.read_bytes()),
        'verifier_sha256': digest(Path(__file__).read_bytes()), 'portable_replay': replay_result,
        'tasks': 22, 'complete_result_pairs': 704, 'protected_required_contracts': 15,
        'negative_cases': negatives, 'runtime_executed': False}
    with args.output.open('x') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
