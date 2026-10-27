# SPDX-License-Identifier: Apache-2.0
"""Replay complete native checkpoint values, declarations, clocks and physical evidence."""
import json
import math
from pathlib import Path

from native_workflow_protocol import strict_json
from query_surface.evidence import no_fallback, worker_outcome
from query_surface.replay import contract, sha
from query_surface.semantics import compare, result_from_record


def need(value, message):
    if not value:
        raise ValueError(message)


def verify_broad(files, manifest, round_number):
    root = manifest['original_evidence_root']
    def raw(path):
        name = manifest['path_map'].get(path)
        need(name in files, 'missing broad linked evidence: ' + str(path))
        return files[name]
    def read(path):
        return strict_json(raw(path))
    def checked(path, digest):
        content = raw(path)
        need(sha(content) == digest, 'broad linked digest differs: ' + path)
        return strict_json(content)
    prefix = root + f'/broad-parquet-checkpoint-r{round_number}'
    frozen = read(prefix + '/inputs.json')
    summary = read(prefix + '/native-observations.json')
    need(summary['status'] == 'passed' and summary['complete'] is True and summary['posthash_verified'] is True, 'broad native checkpoint incomplete')
    need(summary['engine'] == 'native' and summary['input_manifest']['sha256'] == sha(raw(prefix + '/inputs.json')), 'broad manifest differs')
    cases = frozen['cases']
    need(len(cases) == len({case['case_id'] for case in cases}) == len(summary['observations']) == 347, 'broad query coverage differs')
    need(frozen['surface'] == 'python_sql' and frozen['repeats'] == 1, 'broad execution policy differs')
    external = {(item['path'], item['sha256']) for item in manifest['external_artifacts']}
    for item in frozen['frozen_files']:
        name = manifest['path_map'].get(item['path'])
        if name:
            need(sha(files[name]) == item['sha256'], 'broad frozen evidence changed')
        else:
            need((item['path'], item['sha256']) in external, 'broad external artifact missing')
    corpus = {item['id']: item for item in strict_json(files['corpus.json'])}
    runtime = {Path(item['path']).name: item['path'] for item in frozen['runtime']}
    engine = api = 0.0
    projected, pruned, reduced, result_rows, source_routed = [], [], [], 0, 0
    for case, item in zip(cases, summary['observations']):
        case_id = case['case_id']
        need(case_id == item['case_id'], 'broad order differs')
        record = corpus[case_id]
        need(case['case_identity'] == sha(json.dumps(record, sort_keys=True, allow_nan=False).encode()), 'broad case identity changed')
        receipt = read(item['receipt'])
        need(receipt['stdout'] == item['stdout'] and receipt['stdout_sha256'] == item['stdout_sha256'], 'broad receipt output differs')
        outcome = checked(item['stdout'], item['stdout_sha256'])
        need(worker_outcome(receipt, raw(item['stdout'])) == outcome, 'broad supervision failed')
        need(sha(raw(receipt['stderr'])) == receipt['stderr_sha256'], 'broad stderr changed')
        need(receipt['command'][-2] == '--job', 'broad receipt lacks request')
        job = checked(receipt['command'][-1], receipt['job_sha256'])
        need(job['kind'] == 'execute' and outcome['comparison_engine_invoked'] is False, 'broad external provider')
        need(item['status'] == outcome['status'] == 'passed', 'broad failed result accepted')
        need(job['case_id'] == outcome['case_id'] == case_id and job['case_identity'] == outcome['case_identity'] == case['case_identity'], 'broad result identity differs')
        need(job['dataset'] == frozen['dataset'] and job['sources'] == frozen['sources'] and job['surface'] == 'python_sql', 'broad input binding differs')
        need(job['binary'] == runtime['shardloom'] and job['library'] == runtime['_native.abi3.so'], 'broad runtime differs')
        need(job['memory_bytes'] == frozen['memory_bytes'] == 2 << 30 and job['parallelism'] == frozen['parallelism'] == 2, 'broad resource policy differs')
        expectation = checked(job['reference_path'], job['reference_sha256'])
        need(expectation['status'] == 'passed' and expectation['case_identity'] == case['case_identity'], 'broad reference differs')
        observed = outcome['observation']
        need(observed['complete_consumption'] is True and observed['public_declaration'] == case['relation'] == record['relation'], 'broad declaration or completion differs')
        compare(result_from_record(expectation['reference']), result_from_record(outcome['actual']), contract(record['contract']))
        need(item['clocks'] == observed['clocks'], 'broad clocks differ')
        for clock_name in ('engine_execution_seconds', 'complete_workflow_seconds'):
            value = observed['clocks'][clock_name]
            need(type(value) in (int, float) and math.isfinite(value) and value >= 0, 'invalid broad clock')
        engine += observed['clocks']['engine_execution_seconds']
        api += observed['clocks']['complete_workflow_seconds']
        result_rows += len(outcome['actual']['rows'])
        fields = observed['fields']
        no_fallback(fields)
        need(item['physical'] == {key: value for key, value in fields.items() if key.startswith('native_source_demand_')}, 'broad physical summary differs')
        def number(name):
            value = fields['native_source_demand_' + name]
            need(isinstance(value, str) and value.isdecimal(), 'invalid broad physical counter')
            return int(value)
        need(number('sources') > 0 and number('prepared_input_bytes_written') == 0, 'broad demand route absent')
        need(0 < number('projected_columns') <= number('available_columns'), 'broad projection counts invalid')
        need(0 <= number('row_groups_read') <= number('row_groups_total'), 'broad region counts invalid')
        need('not_full_ingest_certification' in fields['native_source_demand_validation_scope'], 'broad validation scope absent')
        need(fields['native_input_end_observed'] == fields['native_input_output_ownership_detached'] == 'true', 'broad completion ownership absent')
        source_routed += 1
        narrow = number('projected_columns') < number('available_columns')
        prune = number('row_groups_read') < number('row_groups_total')
        if narrow:
            projected.append(case_id)
        if prune:
            pruned.append(case_id)
        if narrow or prune:
            reduced.append(case_id)
    need(result_rows == 12565, 'broad complete result row count differs')
    amendment = read(root + '/checkpoint-scope-amendment.json')
    need(amendment['competitor_change']['new_broad_polars_queries'] == 0 and amendment['original_manifest_preserved'] is True, 'competitor cancellation missing')
    need(prefix + '/polars-observations.json' not in manifest['path_map'], 'unexpected repeated competitor pass')
    return dict(status='passed', candidate_round=round_number, complete_comparisons=347,
        complete_rows=result_rows, engine_seconds=engine, api_seconds=api,
        source_demand_queries=source_routed, projected_column_queries=len(projected),
        pruned_region_queries=len(pruned), any_physical_reduction_queries=len(reduced),
        projected_cases=projected, pruned_cases=pruned,
        comparison_scope='same logical cases and Parquet files across native checkpoints; one observation each; original competitor baseline reused separately')
