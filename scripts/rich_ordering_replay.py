# SPDX-License-Identifier: Apache-2.0
"""Replay complete held-out ordering results without native code or live inputs."""
from itertools import product
import json
from pathlib import Path
import zlib

from native_workflow_protocol import strict_json
from parquet_demand_completion_replay import need
from query_surface.evidence import no_fallback, worker_outcome
from query_surface.replay import MAX_FILE, clock, contract, sha
from query_surface.semantics import compare, result_from_record
from query_timing_report import query_phase_timings


def packed(raw, record):
    """Bound both compressed and expanded evidence before parsing any values."""
    data = raw(record['path'])
    size = record['uncompressed_bytes']
    need(len(data) <= MAX_FILE and sha(data) == record['sha256'], 'packed evidence changed')
    need(type(size) is int and 0 <= size <= MAX_FILE, 'packed expansion exceeds bound')
    decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
    value = decoder.decompress(data, size + 1)
    need(len(value) == size and decoder.eof and not decoder.unused_data
         and not decoder.unconsumed_tail, 'packed expansion is incomplete or changed')
    return strict_json(value)


def ordering_counts(observation, prefix, *, zero_demand=False, output_rows=0):
    need(observation['complete_consumption'] is True, 'complete result not consumed')
    fields = observation['fields']
    no_fallback(fields)
    need(fields['native_input_end_observed'] == fields['native_input_output_ownership_detached'] == 'true',
         'source completion or ownership proof absent')
    names = ('prefix_batches', 'prefix_rows_validated', 'payload_buffer_bytes_copied',
             'prefix_peak_retained_buffer_bytes', 'rows_detached')
    values = {name: fields['native_input_ordering_' + name] for name in names}
    need(all(isinstance(value, str) and value.isascii() and value.isdecimal()
             and int(value) <= (1 << 64) - 1 for value in values.values()), 'invalid ordering counter')
    values = {name: int(value) for name, value in values.items()}
    need((values['prefix_batches'] > 0) == prefix, 'bounded ordering activation differs')
    need(values['payload_buffer_bytes_copied'] >= values['prefix_peak_retained_buffer_bytes'],
         'retained payload exceeds all copied buffers')
    if prefix:
        need(values['prefix_rows_validated'] > 0, 'prefix skipped payload validation')
        if output_rows:
            need(values['prefix_peak_retained_buffer_bytes'] > 0
                 and values['rows_detached'] >= output_rows, 'nonempty prefix lacks owned payload')
    else:
        need(values['prefix_rows_validated'] == values['prefix_peak_retained_buffer_bytes'] == 0,
             'ordinary ordering claimed bounded retention')
    if zero_demand:
        need(values['rows_detached'] == values['payload_buffer_bytes_copied']
             == values['prefix_peak_retained_buffer_bytes'] == 0, 'zero demand retained payload')
    derived = query_phase_timings(fields, clock(observation['clocks']['complete_workflow_seconds']),
                                  allow_missing=False)
    for name in ('engine_execution_seconds', 'result_serialization_seconds'):
        need(derived[name] == clock(observation['clocks'][name]), 'clock disagrees with engine report')
    return values


def replay_heldout(raw, root, protocol, expected_runtime):
    def read(path):
        return strict_json(raw(path))

    def linked(record):
        data = raw(record['path'])
        need(sha(data) == record['sha256'] and len(data) == record['generation']['bytes'],
             'linked held-out evidence changed')
        return strict_json(data)

    frozen = read(root + '/inputs.json')
    bound = read(root + '/runtime.json')
    summary = read(root + '/observations.json')
    need(summary['status'] == 'passed' and summary['complete'] is True
         and summary['posthash_verified'] is True and summary['comparison_engine_invoked'] is False,
         'held-out run incomplete or failed')
    need(linked(summary['input_manifest']) == linked(bound['input_manifest']) == frozen
         and linked(summary['runtime_binding']) == bound, 'held-out manifests changed')
    need(bound['status'] == 'bound_before_candidate_execution' and bound['runtime'] == expected_runtime,
         'held-out runtime differs from timed candidate')
    need(frozen['status'] == 'frozen_before_candidate_execution'
         and frozen['surface'] == 'python_sql' and frozen['memory_bytes'] == 2 << 30
         and frozen['parallelism'] == 2 and frozen['comparison_engine_invoked'] is False,
         'held-out execution contract changed')
    declaration = protocol['held_out_correctness']
    expected_cohorts = list(product(declaration['seeds'], declaration['profiles'], declaration['layouts']))
    cohorts = frozen['cohorts']
    need([(item['dataset']['seed'], item['dataset']['profile'], item['layout']) for item in cohorts]
         == expected_cohorts and len(cohorts) == 8, 'held-out seed/profile/layout coverage changed')
    families = ('filter_project_order_limit', 'order_limit_project', 'nested_order_limit', 'decimal_order_limit')
    ids = [f'{family}_k{count}_o{offset}' for count, offset in declaration['limits_and_offsets']
           for family in families] + ['narrow_numeric_topk', 'full_sort']
    catalog = frozen['catalog']
    need([item['id'] for item in catalog] == ids and len(ids) == len(set(ids)) == 22,
         'held-out case catalog changed')
    declarations = {item['id']: item for item in catalog}
    need(all(item['partition'] == 'held_out' and item['contract']['ordered'] for item in catalog),
         'held-out case contract changed')
    execution_order = [(item['id'], case_id) for item in cohorts for case_id in ids]
    need([(item['cohort'], item['case_id']) for item in summary['observations']] == execution_order
         and frozen['expected_observations'] == len(execution_order) == 176
         and frozen['cases_per_cohort'] == 22, 'held-out observation coverage changed')
    need(frozen['complete_input_readback_rows'] == 262168
         and all(item['complete_input_readback_rows'] == item['dataset']['fact_rows'] == declaration['fact_rows']
                 and item['row_group_rows'] == 257
                 and item['dictionary_encoding_requested'] is (item['layout'] == 'clustered') for item in cohorts),
         'held-out complete input readback changed')
    runtime = {Path(item['path']).name: item['path'] for item in bound['runtime']}
    totals = dict(observations=0, complete_rows=0, bounded_observations=0,
                  ordinary_observations=0, zero_demand_observations=0)
    metrics, references = [], {}
    for cohort in cohorts:
        need([item['case_id'] for item in cohort['cases']] == ids, 'held-out cohort case order changed')
        for item in cohort['cases']:
            record = declarations[item['case_id']]
            need(sha(json.dumps(record, sort_keys=True, allow_nan=False).encode()) == item['case_identity'],
                 'held-out declaration identity changed')
            index = totals['observations']
            row = summary['observations'][index]
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
                 and job['reference'] == item['reference'] and job['prefix_expected'] == item['prefix_expected']
                 and job['dataset'] == cohort['dataset'] and job['sources'] == cohort['sources'],
                 'held-out worker declaration or source changed')
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
            need(observation['surface'] == 'python_sql'
                 and observation['public_declaration'] == record['relation'], 'held-out public declaration changed')
            zero = item['case_id'].endswith('_k0_o0') and not item['case_id'].startswith('nested_')
            counts = ordering_counts(observation, item['prefix_expected'], zero_demand=zero, output_rows=count)
            totals['observations'] += 1
            totals['complete_rows'] += count
            totals['bounded_observations' if item['prefix_expected'] else 'ordinary_observations'] += 1
            totals['zero_demand_observations'] += int(zero)
            references[item['reference']['path']] = count
            metrics.append(dict(cohort=cohort['id'], case_id=item['case_id'], **counts))
    need(totals['complete_rows'] == 624906 and len(references) == 88
         and sum(references.values()) == frozen['independently_generated_reference_rows'] == 312453,
         'held-out complete reference coverage changed')
    return dict(status='passed', **totals, complete_input_readback_rows=262168,
                independent_reference_rows=312453, metrics=metrics, comparison_engine_invoked=False,
                scope='Complete correctness and activation; no additional performance-benefit cohort.')
