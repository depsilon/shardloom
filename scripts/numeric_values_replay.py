# SPDX-License-Identifier: Apache-2.0
"""Replay complete fresh numeric-value results without running native code."""
from itertools import product
import json
from pathlib import Path

from native_workflow_protocol import strict_json
from parquet_demand_completion_replay import need
from query_surface.evidence import no_fallback, worker_outcome
from query_surface.replay import clock, contract, sha
from query_surface.semantics import compare, result_from_record
from query_timing_report import query_phase_timings
from rich_ordering_replay import packed

CASE_IDS = [
    'numeric_global_all_widths', 'numeric_group_group_key', 'numeric_group_high_key',
    'numeric_group_skew_key', 'numeric_filtered_tiny', 'numeric_filtered_selected',
    'numeric_all_null_global', 'numeric_empty_global', 'numeric_negative_zero_global',
    'numeric_all_null_grouped', 'numeric_empty_grouped', 'numeric_negative_zero_grouped',
    'numeric_reordered_output', 'numeric_sorted_offset_output', 'numeric_duplicate_join_inner',
    'numeric_join_extrema_inner', 'numeric_duplicate_join_left', 'numeric_join_extrema_left',
    'numeric_duplicate_join_full', 'numeric_join_extrema_full',
]


def replay_heldout(raw, root, protocol, expected_runtime):
    def read(path):
        return strict_json(raw(path))

    def linked(record):
        data = raw(record['path'])
        need(sha(data) == record['sha256'] and len(data) == record['generation']['bytes'],
             'linked held-out evidence changed')
        return strict_json(data)

    frozen, bound, summary = (read(root + '/' + name + '.json')
                              for name in ('inputs', 'runtime', 'observations'))
    need(summary['status'] == 'passed' and summary['complete'] is True
         and summary['posthash_verified'] is True and summary['comparison_engine_invoked'] is False,
         'held-out run incomplete or failed')
    need(linked(summary['input_manifest']) == linked(bound['input_manifest']) == frozen
         and linked(summary['runtime_binding']) == bound, 'held-out manifests changed')
    need(bound['status'] == 'bound_before_candidate_execution' and bound['runtime'] == expected_runtime,
         'held-out runtime differs from measured candidate')
    need(frozen['status'] == 'frozen_before_candidate_execution'
         and frozen['surface'] == 'python_sql' and frozen['memory_bytes'] == 2 << 30
         and frozen['parallelism'] == 2 and frozen['comparison_engine_invoked'] is False,
         'held-out execution contract changed')
    declaration = protocol['held_out_correctness']
    cohorts, catalog = frozen['cohorts'], frozen['catalog']
    expected_cohorts = list(product(declaration['seeds'], declaration['layouts']))
    need([(item['dataset']['seed'], item['layout']) for item in cohorts]
         == expected_cohorts and len(cohorts) == 4, 'held-out seed/layout coverage changed')
    need([item['id'] for item in catalog] == CASE_IDS
         and all(item['partition'] == 'held_out' for item in catalog), 'held-out case catalog changed')
    declarations = {item['id']: item for item in catalog}
    execution_order = [(item['id'], case_id) for item in cohorts for case_id in CASE_IDS]
    need([(item['cohort'], item['case_id']) for item in summary['observations']] == execution_order
         and frozen['expected_observations'] == len(execution_order) == 80
         and frozen['cases_per_cohort'] == 20, 'held-out observation coverage changed')
    need(frozen['complete_input_readback_rows'] == 133216
         and all(item['complete_input_readback_rows'] == 33304
                 and item['dataset']['rows'] == declaration['rows'] == 32779
                 and item['row_group_rows'] == 257
                 and item['dictionary_encoding_requested'] is (item['layout'] == 'clustered') for item in cohorts),
         'held-out complete input readback changed')
    need(all(not item['contract']['floats'] for item in catalog),
         'numeric extrema/output comparisons must remain exact')
    runtime = {Path(item['path']).name: item['path'] for item in bound['runtime']}
    totals = dict(observations=0, complete_rows=0)
    references = {}
    for cohort in cohorts:
        need([item['case_id'] for item in cohort['cases']] == CASE_IDS, 'held-out cohort order changed')
        for item in cohort['cases']:
            record = declarations[item['case_id']]
            need(sha(json.dumps(record, sort_keys=True, allow_nan=False).encode()) == item['case_identity'],
                 'held-out declaration identity changed')
            row = summary['observations'][totals['observations']]
            receipt = read(row['receipt'])
            output = raw(receipt['stdout'])
            need(sha(output) == receipt['stdout_sha256']
                 and sha(raw(receipt['stderr'])) == receipt['stderr_sha256'], 'held-out worker output changed')
            outcome = worker_outcome(receipt, output)
            need(row['status'] == outcome['status'] == 'passed'
                 and outcome['comparison_engine_invoked'] is False, 'failed held-out worker accepted')
            need(receipt['command'][-3:-1] == ['worker', '--job'], 'held-out request absent')
            job_data = raw(receipt['command'][-1])
            need(sha(job_data) == receipt['job_sha256'], 'held-out request changed')
            job = strict_json(job_data)
            need(job['case_id'] == outcome['case_id'] == item['case_id']
                 and job['case_identity'] == outcome['case_identity'] == item['case_identity']
                 and job['reference'] == item['reference'] and job['dataset'] == cohort['dataset']
                 and job['sources'] == cohort['sources'], 'held-out worker declaration or source changed')
            need(job['binary'] == runtime['shardloom'] and job['library'] == runtime['_native.abi3.so']
                 and job['memory_bytes'] == frozen['memory_bytes'] and job['parallelism'] == frozen['parallelism'],
                 'held-out worker runtime or resources changed')
            expected = packed(raw, item['reference'])
            need(expected['status'] == 'passed' and expected['case_id'] == item['case_id']
                 and expected['case_identity'] == item['case_identity'] and expected['dataset'] == cohort['dataset']
                 and expected['comparison_engine_invoked'] is False, 'held-out reference changed')
            need(outcome['actual']['path'] == job['actual_path'], 'held-out actual result replaced')
            actual = packed(raw, outcome['actual'])
            compare(result_from_record(expected['reference']), result_from_record(actual), contract(record['contract']))
            count = len(actual['rows'])
            need(count == item['rows'] == outcome['complete_rows'], 'held-out complete row count changed')
            observation = outcome['observation']
            need(observation['surface'] == 'python_sql' and observation['complete_consumption'] is True
                 and observation['public_declaration'] == record['relation'], 'held-out public declaration changed')
            fields = observation['fields']
            no_fallback(fields)
            need(fields['native_input_end_observed'] == fields['native_input_output_ownership_detached'] == 'true',
                 'source completion or ownership proof absent')
            derived = query_phase_timings(fields, clock(observation['clocks']['complete_workflow_seconds']),
                                          allow_missing=False)
            for name in ('engine_execution_seconds', 'result_serialization_seconds'):
                need(derived[name] == clock(observation['clocks'][name]), 'clock disagrees with engine report')
            totals['observations'] += 1
            totals['complete_rows'] += count
            references[item['reference']['path']] = count
    need(len(references) == 80
         and sum(references.values()) == frozen['independently_generated_reference_rows']
         and totals['complete_rows'] == sum(references.values()),
         'held-out complete reference coverage changed')
    return dict(status='passed', **totals, complete_input_readback_rows=133216,
                independent_reference_rows=sum(references.values()), comparison_engine_invoked=False,
                scope='Complete fresh correctness; activation comes from native counter tests; no additional benefit cohort.')
