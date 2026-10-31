# SPDX-License-Identifier: Apache-2.0
"""Replay saved grouped attribution using archive bytes and maintained validators."""
from pathlib import Path

from native_grouped_cost_diagnostic import TEST, PHASES, parse_observation, selected_cases, summarize, validate
from native_workflow_protocol import strict_json
from query_surface.corpus import corpus
from query_surface.replay import need, sha


def verify(bundle, anchors):
    evidence = bundle.index['evidence']
    need(evidence == anchors and evidence['kind'] == 'grouped_cost_diagnostic', 'attribution anchors differ')
    frozen = strict_json(bundle.identity(evidence['inputs']))
    run = strict_json(bundle.identity(evidence['observations']))
    baseline = strict_json(bundle.identity(evidence['baseline_inputs']))
    baseline_run = strict_json(bundle.identity(evidence['baseline_observations']))
    need(baseline_run['status'] == 'passed' and baseline_run['complete'] is True
         and baseline_run['posthash_verified'] is True, 'original cohort incomplete')
    baseline_rows = {row['case_id']: row for row in baseline_run['observations']}
    need(len(baseline_rows) == len(baseline_run['observations']) == 347, 'original cohort coverage changed')
    need(run['status'] == 'passed' and run['complete'] is True and run['posthash_verified'] is True,
         'diagnostic did not finish')
    need(run['input_manifest'] == evidence['inputs'], 'run refers to a different freeze')
    need(frozen['test'] == TEST and frozen['phase_order'] == list(PHASES)
         and frozen['repeats'] == 1 and frozen['memory_bytes'] == 2 << 30
         and frozen['max_parallelism'] == 2 and frozen['original_case_order'] is True
         and frozen['comparison_engine_invoked'] is False and frozen['full43_executed'] is False,
         'diagnostic method changed')
    selected = selected_cases(baseline)
    need([{key: value for key, value in case.items() if key != 'job'}
          for case in frozen['cases']] == selected, 'original cases or order changed')
    need(frozen['source'] == baseline['sources']['fact'][0], 'original source changed')
    catalog = {case.id: case for case in corpus(interactions='all', include_pending=False)}
    rows = run['observations']
    need(len(rows) == len(selected) == 48, 'incomplete diagnostic')
    for case, row in zip(frozen['cases'], rows):
        need(row['status'] == 'passed' and row['case_id'] == case['case_id'], 'case result/order changed')
        receipt = strict_json(bundle.identity(row['receipt']))
        log = bundle.identity(receipt['log']).decode()
        job = strict_json(bundle.identity(case['job']))
        reference = strict_json(bundle.raw(case['reference_path'], case['reference_sha256']))
        need(receipt['command'] == [frozen['binary'], TEST, '--exact', '--ignored', '--nocapture', '--test-threads=1']
             and receipt['job'] == case['job'] and receipt['timeout_seconds'] == 180
             and receipt['output_limit_bytes'] == 8 << 20, 'bounded query command changed')
        observation = parse_observation(log)
        need(observation == row['observation'], 'raw observation differs')
        need(validate(case, observation, receipt, log, catalog, job=job, reference=reference) == row['actual'],
             'complete result differs')
        prior = baseline_rows[case['case_id']]['physical']
        need(all(observation['source'][key] == int(prior['native_source_demand_' + key])
                 for key in ('projected_columns', 'rows_decoded', 'data_read_bytes', 'data_read_requests')),
             'diagnostic source demand differs from original execution')
    items = {item['path']: item for item in frozen['frozen_files']}
    external = {item['path']: item for item in evidence['external_artifacts']}
    need(set(external) == {frozen['source'], frozen['binary']}, 'external artifact inventory changed')
    for path, item in items.items():
        if path in external:
            need(item == external[path], 'large input/runtime identity changed')
        else:
            bundle.identity(item)
    build = strict_json(bundle.raw(frozen['build_receipt']))
    need(build['status'] == 'passed' and build['returncode'] == 0 and build['process_group_drained']
         and build['head'] == frozen['source_revision'] and '--release' in build['command']
         and '--no-run' in build['command'] and 'release-user-surfaces' in build['command'],
         'probe release build changed')
    build_log = bundle.raw(str(Path(frozen['build_receipt']).with_suffix('.log')), build['log_sha256']).decode()
    need(f"Executable unittests src/lib.rs ({frozen['binary']})" in build_log, 'probe executable not built')
    for relative, item in frozen['modified_build_sources'].items():
        if 'sha256' in item:
            raw = bundle.raw(build['cwd'] + '/' + relative, item['sha256'])
            need(len(raw) == item['bytes'], 'build source size differs')
    for item in evidence['gates']:
        gate = strict_json(bundle.identity(item['receipt']))
        log = bundle.identity(item['log'])
        need(gate['status'] == item['status'] and sha(log) == gate['log_sha256']
             and gate['process_group_drained'] and gate['direct_child_reaped'], 'gate receipt changed')
        need((gate['returncode'] == 0) is (item['status'] == 'passed'), 'gate exit status differs')
    need(any(item['status'] == 'failed' for item in evidence['gates']), 'initial lint failure omitted')
    assembly = strict_json(bundle.identity(evidence['assembly_manifest']))
    runtime = strict_json(bundle.identity(evidence['assembly_runtime']))
    review = strict_json(bundle.identity(evidence['assembly_review']))
    need(assembly['queries_executed'] is False and review['queries_executed'] is False
         and review['status'] == 'passed' and assembly['binary_sha256'] == review['runtime_product_sha256']
         == runtime['products']['_native.abi3.so'], 'assembly is not the accepted runtime')
    for item in assembly['records']:
        path = str(Path(evidence['assembly_manifest']['path']).parent / item['file'])
        raw = bundle.raw(path, item['sha256'])
        need(raw.count(b'\n') == item['lines'] and item['symbol'].encode() in raw, 'assembly record differs')
    result = summarize(rows)
    result['subsets'] = {}
    for factor in ('selection', 'measures', 'value', 'groups'):
        result['subsets'][factor] = {}
        for value in dict.fromkeys(row['diagnostic'][factor] for row in rows):
            result['subsets'][factor][value] = summarize([row for row in rows if row['diagnostic'][factor] == value])
    result['typed_update_calls'] = sum(row['observation']['phases'][5]['calls'] for row in rows)
    result['sample_average_typed_update_seconds_per_call'] = result['phases']['typed_updates']['engine_seconds'] / result['typed_update_calls']
    result['scope'] = frozen['scope']
    result['original_source_demand_matches'] = 48
    return result
