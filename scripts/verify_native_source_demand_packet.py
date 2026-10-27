# SPDX-License-Identifier: Apache-2.0
"""Offline source-demand evidence verification; never imports packet code."""
from pathlib import Path
import argparse
import gzip
import io
import json
import math
import sys

REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(REPO / 'scripts'), str(REPO / 'python/src'), str(REPO / 'benchmarks/traditional_analytics')]
from native_workflow_protocol import extract_result, report_fields, strict_json
from query_surface.evidence import no_fallback, worker_outcome
from query_surface.polars_adapter import compare_result as compare_polars
from query_surface.replay import bounded_bytes, contract, object_files, sha, MAX_ARCHIVE, MAX_FILE
from query_surface.semantics import compare, result_from_record
from run_clickbench_query_uat import equivalent

RULES_SHA = '5b3811e67a32a8ca83cce2bfd6019ca6a55a40671b5671aecbcba03d9ee2d7bc'


def need(value, message):
    if not value:
        raise ValueError(message)


def physical(fields, case_id, maximum=38):
    no_fallback(fields)
    def number(name):
        text = fields['native_source_demand_' + name]
        need(isinstance(text, str) and text.isdecimal(), 'invalid physical counter')
        return int(text)
    need(number('sources') == 1 and number('prepared_input_bytes_written') == 0, 'source strategy differs')
    need(number('available_columns') == 38 and 0 < number('projected_columns') <= maximum, 'projection differs')
    need(0 < number('physical_leaf_columns') <= 39, 'leaf projection differs')
    need(number('row_groups_total') == 123 and 0 <= number('row_groups_read') <= 123, 'region counts differ')
    if case_id == 'predicate_one_row':
        need(number('row_groups_read') == 1, 'mandatory region exclusion absent')
    if case_id in {'selectivity_10000', 'aggregate_signed_value_group_key_mixed_direct_all'}:
        need(number('row_groups_read') == 123, 'unprunable control lost regions')
    if case_id in {'predicate_one_row', 'projection_narrow', 'projection_alias_filter', 'set_union_all'}:
        need(number('projected_columns') < 38 and number('physical_leaf_columns') < 39, 'physical projection absent')
    need(number('data_read_bytes') > 0 and number('data_read_requests') > 0, 'observed reads absent')
    need('not_full_ingest_certification' in fields['native_source_demand_validation_scope'], 'validation scope absent')
    need(fields['native_input_end_observed'] == fields['native_input_output_ownership_detached'] == 'true', 'completion ownership absent')
    need(int(fields['native_input_max_batch_rows']) <= 8192, 'batch scope differs')


def verify_round(files, manifest, round_number):
    need(manifest['verification_kind'] == 'native_source_demand', 'wrong verification kind')
    root = manifest['original_evidence_root']
    def raw(path):
        name = manifest['path_map'].get(str(path))
        need(name in files, 'missing linked evidence: ' + str(path))
        return files[name]
    def read(path):
        return strict_json(raw(path))
    def local(name):
        if round_number == 2:
            for old, new in [
                ('candidate-', 'candidate2-'), ('heldout-inputs', 'heldout2-inputs'),
                ('heldout-observations', 'heldout2-observations'), ('full43-r1', 'full43-r2'),
                ('installed-wheel-r2', 'installed-wheel-r3'),
            ]:
                if name.startswith(old):
                    name = new + name[len(old):]
                    break
            if name == 'candidate2-runtime-r1/manifest.json':
                name = 'candidate-runtime-r2/manifest.json'
        return read(root + '/' + name)
    def checked(path, digest):
        value = raw(path)
        need(sha(value) == digest, 'linked digest differs: ' + str(path))
        return strict_json(value)
    def check_identity(item):
        if item['path'] in manifest['path_map']:
            need(sha(raw(item['path'])) == item['sha256'], 'frozen metadata changed')
        else:
            need((item['path'], item['sha256']) in external, 'unrepresented frozen artifact')

    external = {(item['path'], item['sha256']) for item in manifest['external_artifacts']}
    corpus = {item['id']: item for item in strict_json(files['corpus.json'])}
    baseline_inputs, candidate_inputs, heldout_inputs = [local(name + '-inputs.json') for name in ('baseline', 'candidate', 'heldout')]
    rules = local('baseline-decision.json')
    need(sha(raw(root + '/baseline-decision.json')) == RULES_SHA, 'prospective decision changed')
    need(sha(raw(root + '/baseline-observations.json')) == rules['baseline_sha256'], 'baseline changed')
    ids = [item['case_id'] for item in baseline_inputs['cases']]
    need(len(ids) == len(set(ids)) == 9, 'baseline case coverage differs')
    heldout = local('heldout-plan.json')
    heldout_ids = [item['case_id'] for item in heldout['cases']]
    need(len(heldout_ids) == len(set(heldout_ids)) == 6 and heldout['candidate_performance_observations_so_far'] == 0, 'held-out plan differs')
    need(heldout_inputs['complete_typed_readback_rows'] == 1000000 and heldout_inputs['row_groups'] == 123, 'held-out readback incomplete')
    for frozen in [baseline_inputs, candidate_inputs, heldout_inputs]:
        for item in frozen['cases']:
            record = corpus[item['case_id']]
            need(sha(json.dumps(record, sort_keys=True, allow_nan=False).encode()) == item['case_identity'], 'case contract changed')
            checked(item['reference_path'], item['reference_sha256'])
    for item in candidate_inputs['runtime'] + candidate_inputs['harness_sources'] + candidate_inputs['prepared_evidence']:
        check_identity(item)
    for item in [candidate_inputs['runtime_manifest'], candidate_inputs['runtime_build'], heldout_inputs['tool'], heldout_inputs['physical_check_tool']]:
        check_identity(item)

    completed, outputs = {}, {}
    for label, frozen in [('baseline', baseline_inputs), ('candidate', candidate_inputs), ('heldout', heldout_inputs)]:
        summary = local(label + '-observations.json')
        need(summary['status'] == 'passed', label + ' is incomplete')
        check_identity(summary['input_manifest'])
        observations = summary['observations']
        if label == 'baseline':
            wanted = {f'fresh-{run}-{case}' for run in (0, 1) for case in ids}
            wanted |= {f'{strategy}-{case}' for strategy in ('prepared', 'polars') for case in ids}
            wanted |= {f'sequence-{index}-{case}' for index, case in enumerate(frozen['sequence'])}
        elif label == 'candidate':
            wanted = {f'{strategy}-{case}' for strategy in ('fresh', 'prepared') for case in ids}
            wanted |= {f'sequence-{index}-{case}' for index, case in enumerate(frozen['sequence'])}
        else:
            wanted = set(heldout_ids)
        keys = [item.get('name', item['case_id']) for item in observations]
        need(set(keys) == wanted and len(keys) == len(wanted), label + ' observation coverage differs')
        for item in observations:
            name, case_id = item.get('name', item['case_id']), item['case_id']
            receipt = read(item['receipt'])
            need(receipt['stdout'] == item['stdout'] and receipt['stdout_sha256'] == item['stdout_sha256'], 'receipt output differs')
            outcome = checked(item['stdout'], item['stdout_sha256'])
            need(worker_outcome(receipt, raw(item['stdout'])) == outcome, 'supervision failed')
            need(sha(raw(receipt['stderr'])) == receipt['stderr_sha256'], 'stderr changed')
            need(receipt['command'][-2] == '--job', 'receipt lacks worker request')
            job = checked(receipt['command'][-1], receipt['job_sha256'])
            case = corpus[case_id]
            need(item['status'] == outcome['status'] == 'passed', 'failed result accepted')
            need(job['case_id'] == outcome['case_id'] == case_id and job['case_identity'] == outcome['case_identity'] == sha(json.dumps(case, sort_keys=True, allow_nan=False).encode()), 'request/result identity differs')
            need(job['dataset'] == frozen['dataset'] and job['memory_bytes'] == 2 << 30 and job['parallelism'] == 2, 'input/resource policy differs')
            strategy = item.get('strategy')
            if strategy == 'prepared':
                sources = frozen['prepared_sources']
            elif label == 'heldout':
                sources = {'fact': [frozen['inputs'][case_id]['path']]}
            elif label == 'candidate':
                sources = {'fact': [frozen['fresh_inputs']['sequence' if strategy == 'sequence' else case_id]['path']]}
            elif strategy in {'sequence', 'polars'}:
                sources = {'fact': [frozen['sequence_input' if strategy == 'sequence' else 'parquet']['path']]}
            else:
                source, = [entry['path'] for entry in frozen['fresh_inputs'] if Path(entry['path']).stem == name]
                sources = {'fact': [source]}
            need(job['sources'] == sources, 'query source binding differs')
            runtime_inputs = candidate_inputs if label == 'heldout' else frozen
            for field, filename in [('binary', 'shardloom'), ('library', '_native.abi3.so')]:
                expected_path, = [entry['path'] for entry in runtime_inputs['runtime'] if Path(entry['path']).name == filename]
                need(job[field] == expected_path, 'query runtime binding differs')
            expectation = checked(job['reference_path'], job['reference_sha256'])
            need(expectation['status'] == 'passed' and expectation['case_identity'] == job['case_identity'], 'independent reference differs')
            observed = outcome['observation']
            need(observed['complete_consumption'] is True and observed['public_declaration'] == case['relation'], 'incomplete or different declared result')
            need(item['clocks'] == observed['clocks'], 'observation clocks changed')
            for clock_name in ['complete_workflow_seconds', 'engine_execution_seconds']:
                value = observed['clocks'][clock_name]
                need(value is None or (type(value) in (float, int) and math.isfinite(value) and value >= 0), 'invalid clock')
            left, right = result_from_record(expectation['reference']), result_from_record(outcome['actual'])
            if item.get('strategy') == 'polars':
                need(job['kind'] == 'polars' and outcome['comparison_engine_invoked'] is True, 'wrong comparison provider')
                need(observed['clocks']['engine_execution_seconds'] is None, 'Polars engine clock fabricated')
                compare_polars(left, right, contract(case['contract']))
            else:
                need(job['kind'] == 'execute' and outcome['comparison_engine_invoked'] is False, 'external native execution')
                no_fallback(observed['fields'])
                compare(left, right, contract(case['contract']))
                if label != 'baseline':
                    if item.get('strategy') == 'prepared':
                        need(observed['fields'].get('native_source_demand_sources', '0') == '0', 'prepared control changed route')
                    else:
                        maximum = next(row['maximum_projected_columns'] for row in heldout['cases'] if row['case_id'] == case_id) if label == 'heldout' else 38
                        physical(observed['fields'], case_id, maximum)
                        expected_fields = {key: value for key, value in observed['fields'].items() if key.startswith('native_source_demand_')}
                        need(item['physical'] == {'status': 'passed', 'fields': expected_fields}, 'physical summary differs')
            outputs[(label, name)] = outcome
        completed[label] = len(observations)

    decision = local('candidate-decision.json')
    check_identity(decision['rules'])
    check_identity(decision['observations'])
    improvements = []
    for case_id in ids:
        values = [outputs[('baseline', f'fresh-{run}-{case_id}')]['observation']['clocks']['complete_workflow_seconds'] for run in (0, 1)]
        old = min(values)
        need(rules['noise'][case_id] == dict(minimum=old, maximum=max(values), spread=max(values)-old, spread_over_minimum=(max(values)-old)/old), 'noise screen changed')
        for strategy in ('fresh', 'prepared'):
            clock_name = 'complete_workflow_seconds' if strategy == 'fresh' else 'engine_execution_seconds'
            before = old if strategy == 'fresh' else outputs[('baseline', f'prepared-{case_id}')]['observation']['clocks'][clock_name]
            after = outputs[('candidate', f'{strategy}-{case_id}')]['observation']['clocks'][clock_name]
            allowance = max(old * .1, .1, max(values)-old) if strategy == 'fresh' else max(before * .1, .02)
            material = (before-after >= .25 and after <= before*.75) if strategy == 'fresh' else None
            row, = [item for item in decision['comparisons'] if item['case_id'] == case_id and item['strategy'] == strategy]
            need(row == dict(case_id=case_id, strategy=strategy, clock=clock_name, baseline=before, candidate=after, allowed_regression=allowance, within_regression_gate=after<=before+allowance, materially_improved=material), 'performance decision is not derived from observations')
            need(after <= before + allowance, 'control regression gate failed')
            if material:
                improvements.append(case_id)
    sequence = lambda label: sum(outputs[(label, f'sequence-{index}-{case}')]['observation']['clocks']['complete_workflow_seconds'] for index, case in enumerate(baseline_inputs['sequence']))
    need(sequence('baseline') == rules['sequence_gate']['baseline_seconds'], 'baseline sequence changed')
    need(sequence('candidate') == decision['sequence_seconds'] and sequence('candidate') <= sequence('baseline')*.9, 'sequence gate failed')
    need(decision['materially_improved_cases'] == improvements and len(improvements) >= 6 and 'predicate_one_row' in improvements and decision['performance_gate'] == 'passed' and decision['sequence_gate_passed'] is True, 'benefit decision changed')

    required = ['r13-relational-tests', 'r26-native-clippy', 'r27-workspace-clippy', 'r28-workspace-tests', 'r29-format-check', 'r30-all-features', 'r31-no-default-features', 'r33-reader-feature-boundary', 'r34-final-demand-tests', 'r35-final-public-workflow', 'r36-release-build', 'r37-runtime-freeze', 'r41-package-regressions', 'r42-installed-wheel']
    if round_number == 2:
        required = [
            'r55-validation-scope-regression', 'r56-final-relational', 'r57-final-public-workflow',
            'r58-final-format', 'r59-final-native-clippy', 'r60-final-workspace-clippy',
            'r61-final-workspace-tests', 'r62-final-all-features', 'r63-final-no-default-features',
            'r64-final-reader-boundary', 'r65-final-release-build', 'r66-final-runtime-freeze',
            'r67-final-installed-wheel',
        ]
    for name in required:
        record = local(name + '.json')
        need(record['status'] == 'passed' and record['returncode'] == 0 and record['process_group_drained'] is True, 'validation gate failed: ' + name)
        need(sha(raw(root + '/' + name + '.log')) == record['log_sha256'], 'validation log changed')
    package = local('installed-wheel-r2/report.json')
    need(package['status'] == package['uninstall_status'] == 'passed' and package['publication_attempted'] is False, 'package proof incomplete')
    need(package['integration'] == dict(tests=184, failures=0, errors=0, skipped=0, status='passed'), 'package integration differs')
    runtime = local('candidate-runtime-r1/manifest.json')
    build = local(runtime['build_receipt'])
    need(sha(raw(root + '/' + runtime['build_receipt'])) == runtime['build_receipt_sha256'], 'runtime build receipt changed')
    for name, expected in build['changed_source_inventory'].items():
        if name.endswith(('.rs', '.toml', '.lock')) and 'sha256' in expected:
            prefix = 'evidence/candidate-r1-source/' if round_number == 1 else 'evidence/candidate-runtime-r2/source/'
            need(sha(files[prefix + name]) == expected['sha256'], 'runtime source differs from tested build')
    need(package['integrity']['cli_sha256'] == runtime['files']['shardloom']['sha256'] and package['integrity']['native_library_sha256'] == runtime['files']['_native.abi3.so']['sha256'], 'tested package uses another runtime')

    full = local('full43-r1-acceptance.json')
    driver = checked(full['driver']['path'], full['driver']['sha256'])
    summary = checked(full['summary']['path'], full['summary']['sha256'])
    reference = checked(full['reference']['path'], full['reference']['sha256'])
    schemas = {item['query']: item['schema'] for item in checked(full['schemas']['path'], full['schemas']['sha256'])['records']}
    need(driver['status'] == 'passed' and driver['native_process_cleanup_verified'] and driver['process_group_drained'] and driver['input_before'] == driver['input_after'], 'Full43 identity/cleanup failed')
    need(summary['complete'] and summary['full_result_validation'] and summary['repeats'] == 1 and summary['selected_query_ids'] == list(range(1,44)), 'Full43 incomplete')
    need(summary['binary_sha256'] == runtime['files']['shardloom']['sha256'], 'Full43 runtime differs')
    need(len(full['records']) == len(summary['records']) == 43, 'Full43 coverage differs')
    for row, original in zip(full['records'], summary['records']):
        need(row['query'] == original['query'] and original['passed'] and original['run'] == 1 and not original['guard_failures'], 'Full43 outcome changed')
        packed = raw(row['output']['path'])
        need(sha(packed) == row['output']['sha256'], 'Full43 archived output changed')
        with gzip.GzipFile(fileobj=io.BytesIO(packed)) as stream:
            unpacked = stream.read(MAX_FILE + 1)
        need(len(unpacked) <= MAX_FILE, 'Full43 expanded output exceeds bound')
        data = strict_json(unpacked)
        need(data['status'] == 'success' and data['fallback']['attempted'] is False, 'Full43 failed native output')
        fields, values = report_fields(data), extract_result(data)
        wanted, = [item['values'] for item in reference['records'] if item['query_id'] == row['query']]
        need(equivalent(values, wanted) and len(values) == row['complete_rows'], 'Full43 complete values differ')
        no_fallback(fields)
        need(strict_json(fields['result_schema_json']) == schemas[row['query']] == row['schema'], 'Full43 schema changed')
        need(int(fields['native_query_execution_nanos']) / 1e9 == row['engine_seconds'] == original['engine_execution_seconds'], 'Full43 clock changed')
    need(sum(row['complete_rows'] for row in full['records']) == full['complete_rows'] == 381, 'Full43 row coverage differs')
    need(sum(row['engine_seconds'] for row in full['records']) == full['engine_seconds'] == summary['query_total_seconds'], 'Full43 engine sum changed')
    need(full['within_10_percent_regression_trigger'] is True and full['engine_seconds'] <= full['prior_engine_seconds']*1.1, 'Full43 regression trigger requires investigation')
    return dict(status='passed', candidate_round=round_number, complete_comparisons=completed, full43_queries=43, full43_rows=381,
        full43_engine_seconds=full['engine_seconds'], materially_improved_cases=len(improvements),
        package_tests=184, verification='offline complete-value, identity, physical-work and frozen-decision replay; no engine execution')


def verify(files, manifest):
    from native_source_demand_checkpoint_replay import verify_broad
    need(manifest['verification_kind'] == 'native_source_demand', 'wrong verification kind')
    rounds = [verify_round(files, manifest, value) for value in (1, 2)]
    broad = [verify_broad(files, manifest, value) for value in (1, 2)]
    return dict(status='passed', rounds=rounds, broad_checkpoints=broad,
        distinct_complete_result_observations=39 + 2 * (21 + 6 + 43 + 347),
        final_package_tests=184, new_broad_competitor_queries=0,
        limitations=['Content hashes prove internal consistency, not signed provenance.',
            'No query is executed by this replay; complete retained outputs are compared.',
            'Binary/data identities are retained, while large payloads are excluded.',
            'First-output latency, exact decoder allocations and exact process RSS remain unavailable.'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('packet', type=Path)
    args = parser.parse_args()
    manifest = strict_json(bounded_bytes(args.packet / 'manifest.json', MAX_FILE))
    files = object_files(manifest, bounded_bytes(args.packet / 'packet.tar.xz', MAX_ARCHIVE))
    print(json.dumps(verify(files, manifest), indent=2))


if __name__ == '__main__':
    main()
