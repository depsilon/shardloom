# SPDX-License-Identifier: Apache-2.0
"""Replay complete Parquet demand observations without executing an engine."""
from pathlib import Path
import argparse
import json
import math
import sys

REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(REPO / 'scripts'), str(REPO / 'python/src'), str(REPO / 'benchmarks/traditional_analytics')]
from native_workflow_protocol import strict_json
from query_surface.evidence import no_fallback, worker_outcome
from query_surface.replay import contract, sha
from query_surface.semantics import compare, result_from_record
from query_timing_report import query_phase_timings


def need(value, message):
    if not value:
        raise ValueError(message)


def replay(raw, root):
    def read(path):
        return strict_json(raw(path))
    def checked(path, digest):
        data = raw(path)
        need(sha(data) == digest, 'changed linked evidence: ' + path)
        return strict_json(data)
    frozen = read(root + '/inputs.json')
    summary = read(root + '/native-observations.json')
    need(summary['status'] == 'passed' and summary['complete'] and summary['posthash_verified'], 'incomplete candidate')
    need(summary['engine'] == 'native' and summary['comparison_engine_invoked'] is False, 'unexpected engine')
    need(summary['input_manifest']['sha256'] == sha(raw(root + '/inputs.json')), 'input manifest changed')
    baseline = checked(frozen['baseline_observations']['path'], frozen['baseline_observations']['sha256'])
    protocol = checked(frozen['protocol']['path'], frozen['protocol']['sha256'])
    previous_inputs = checked(frozen['baseline_inputs']['path'], frozen['baseline_inputs']['sha256'])
    cases = frozen['cases']
    need(cases == previous_inputs['cases'] and frozen['sources'] == previous_inputs['sources'], 'cohort or source change')
    need(len(cases) == len({item['case_id'] for item in cases}) == len(summary['observations']) == len(baseline['observations']) == 347, 'wrong case coverage')
    need(frozen['surface'] == 'python_sql' and frozen['repeats'] == 1 and baseline['complete'] and baseline['status'] == 'passed', 'wrong execution policy')
    catalog = {item['id']: item for item in checked(frozen['corpus']['path'], frozen['corpus']['sha256'])}
    need(len(catalog) == 347, 'duplicate corpus declarations')
    runtime = {Path(item['path']).name: item['path'] for item in frozen['runtime']}
    rows, families, complete_rows = [], {}, 0
    def clock(clocks, name):
        value = clocks[name]
        need(type(value) in (int, float) and math.isfinite(value) and value >= 0, 'invalid clock')
        return value
    for case, item, before in zip(cases, summary['observations'], baseline['observations']):
        case_id = case['case_id']
        need(case_id == item['case_id'] == before['case_id'], 'case order changed')
        record = catalog[case_id]
        need(sha(json.dumps(record, sort_keys=True, allow_nan=False).encode()) == case['case_identity'], 'corpus identity changed')
        receipt = read(item['receipt'])
        need(receipt['stdout'] == item['stdout'] and receipt['stdout_sha256'] == item['stdout_sha256'], 'receipt output changed')
        outcome = checked(item['stdout'], item['stdout_sha256'])
        need(worker_outcome(receipt, raw(item['stdout'])) == outcome, 'supervision failed')
        need(sha(raw(receipt['stderr'])) == receipt['stderr_sha256'], 'stderr changed')
        need(receipt['command'][-2] == '--job', 'missing request')
        job = checked(receipt['command'][-1], receipt['job_sha256'])
        need(job['kind'] == 'execute' and outcome['comparison_engine_invoked'] is False, 'external engine invoked')
        need(item['status'] == outcome['status'] == before['status'] == 'passed', 'failed case accepted')
        need(job['case_id'] == outcome['case_id'] == case_id and job['case_identity'] == outcome['case_identity'] == case['case_identity'], 'request identity changed')
        need(job['dataset'] == frozen['dataset'] and job['sources'] == frozen['sources'] and job['surface'] == 'python_sql', 'input binding changed')
        need(job['binary'] == runtime['shardloom'] and job['library'] == runtime['_native.abi3.so'], 'runtime changed')
        need(job['memory_bytes'] == frozen['memory_bytes'] == 2 << 30 and job['parallelism'] == frozen['parallelism'] == 2, 'resources changed')
        need(job['reference_path'] == case['reference_path'] and job['reference_sha256'] == case['reference_sha256'], 'reference replaced')
        expectation = checked(job['reference_path'], job['reference_sha256'])
        need(expectation['status'] == 'passed' and expectation['case_identity'] == case['case_identity'], 'reference identity changed')
        observed = outcome['observation']
        need(observed['complete_consumption'] and observed['public_declaration'] == case['relation'] == record['relation'], 'declaration or completion changed')
        compare(result_from_record(expectation['reference']), result_from_record(outcome['actual']), contract(record['contract']))
        complete_rows += len(outcome['actual']['rows'])
        fields = observed['fields']
        no_fallback(fields)
        need(item['physical'] == {key: value for key, value in fields.items() if key.startswith('native_source_demand_')}, 'physical summary changed')
        def number(name):
            value = fields['native_source_demand_' + name]
            need(isinstance(value, str) and value.isdecimal(), 'invalid physical counter: ' + name)
            return int(value)
        need(number('sources') > 0 and number('files') >= number('sources') and number('prepared_input_bytes_written') == 0, 'demand route absent')
        need(0 < number('projected_columns') <= number('available_columns'), 'invalid projection')
        need(0 <= number('row_groups_read') <= number('row_groups_total'), 'invalid row groups')
        need('not_full_ingest_certification' in fields['native_source_demand_validation_scope'], 'validation scope absent')
        need(fields['native_input_end_observed'] == fields['native_input_output_ownership_detached'] == 'true', 'input ownership incomplete')
        need(item['clocks'] == observed['clocks'], 'clock summary changed')
        derived = query_phase_timings(fields, observed['clocks']['complete_workflow_seconds'], allow_missing=False)
        for name in ('engine_execution_seconds', 'result_serialization_seconds'):
            need(derived[name] == observed['clocks'][name], 'clock disagrees with engine report')
        row = {'case_id': case_id, 'family': case['family'],
               'baseline_engine_seconds': clock(before['clocks'], 'engine_execution_seconds'),
               'candidate_engine_seconds': clock(item['clocks'], 'engine_execution_seconds'),
               'baseline_api_seconds': clock(before['clocks'], 'complete_workflow_seconds'),
               'candidate_api_seconds': clock(item['clocks'], 'complete_workflow_seconds'),
               'complete_rows': len(outcome['actual']['rows']),
               'baseline_read_requests': int(before['physical']['native_source_demand_data_read_requests']),
               'candidate_read_requests': number('data_read_requests')}
        threshold = protocol['retention_decision']['material_case_regression']
        row['material_regression'] = (row['candidate_engine_seconds'] > row['baseline_engine_seconds'] * (1 + threshold['fraction'])
            and row['candidate_engine_seconds'] - row['baseline_engine_seconds'] > threshold['absolute_seconds'])
        rows.append(row)
        family = families.setdefault(case['family'], {'cases': 0, 'baseline_engine_seconds': 0., 'candidate_engine_seconds': 0.})
        family['cases'] += 1
        for key in ('baseline_engine_seconds', 'candidate_engine_seconds'):
            family[key] += row[key]
    need(complete_rows == 12565, 'wrong complete row coverage')
    threshold = protocol['retention_decision']['material_family_regression']
    for family in families.values():
        before, after = family['baseline_engine_seconds'], family['candidate_engine_seconds']
        family['reduction_fraction'] = 1 - after / before
        family['material_regression'] = after > before * (1 + threshold['fraction']) and after - before > threshold['absolute_seconds']
    # Python 3.12 changed float sum(). Keep portable evidence replay stable
    # across supported Python versions without changing any measured value.
    before = math.fsum(row['baseline_engine_seconds'] for row in rows)
    after = math.fsum(row['candidate_engine_seconds'] for row in rows)
    need(math.isclose(before, frozen['baseline_complete_result_replay']['engine_seconds'], rel_tol=0, abs_tol=1e-9), 'baseline clock changed')
    reduction = 1 - after / before
    retention = protocol['retention_decision']
    improved_families = [name for name in retention['declared_families']
        if families[name]['reduction_fraction'] >= retention['alternative_minimum_declared_family_reduction_fraction']]
    return {'status': 'passed', 'complete_comparisons': 347, 'complete_rows': complete_rows,
        'baseline_engine_seconds': before, 'candidate_engine_seconds': after, 'reduction_fraction': reduction,
        'benefit_gate_passed': reduction >= retention['minimum_complete_cohort_reduction_fraction'] or bool(improved_families),
        'materially_improved_declared_families': improved_families,
        'material_case_regressions': [row['case_id'] for row in rows if row['material_regression']],
        'material_family_regressions': [name for name, row in families.items() if row['material_regression']],
        'families': families, 'cases': rows, 'comparison_engine_invoked': False,
        'comparison_scope': 'Same 347 cases, same five Parquet files, one native observation per case per candidate. Engine clocks only for retention; no statistical confidence claim.'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('candidate', type=Path)
    args = parser.parse_args()
    result = replay(lambda path: Path(path).read_bytes(), str(args.candidate.resolve()))
    print(json.dumps(result, indent=2))
