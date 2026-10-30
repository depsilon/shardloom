# SPDX-License-Identifier: Apache-2.0
"""Pure replay of explicit-materialization observations and semantic controls."""
from dataclasses import asdict, replace
from functools import lru_cache
import json
from pathlib import Path

from native_workflow_protocol import strict_json
from query_surface.evidence import no_fallback, worker_outcome
from query_surface.replay import need, sha
from query_surface.semantics import compare, result_from_record
from query_timing_report import query_phase_timings
import shared_followthrough_materialized_workload as workload

reference = lru_cache(maxsize=32)(workload.reference)
CONTROLS = ['changed_complete_value', 'missing_complete_row', 'changed_schema', 'changed_engine_clock',
    'hidden_external_execution', 'false_reuse', 'changed_role', 'changed_statement', 'unproved_cleanup',
    'zero_output_skips_capture', 'missing_second_action']


def validate_result(value, job, reference_record):
    case = workload.Case(**{**job['case'], 'consumers': tuple(job['case']['consumers'])})
    spec = workload.Fixture(**job['fixture'])
    need(job['case_identity'] == workload.identity(case, spec)
         and job['sql'] == workload.sql(case, job['sources'], job['materialized']),
         'materialization declaration differs')
    expected, normalized = reference(case, spec)
    need(reference_record['status'] == 'passed' and reference_record['case_identity'] == job['case_identity']
         and reference_record['normalized_rows'] == normalized
         and reference_record['comparison_engine_invoked'] is False, 'materialization reference changed')
    compare(expected, result_from_record(reference_record['reference']), workload.contract(case))
    need(value['status'] == 'passed' and value['case_id'] == case.name
         and value['case_identity'] == job['case_identity'] and value['materialized'] is job['materialized']
         and value['sql_declaration'] == job['sql'], 'materialization result declaration changed')
    need(value['retained_after_close'] is True and value['spill_workspace_clean'] is True
         and value['comparison_engine_invoked'] is False and value['complete_result_rows'] == len(expected.rows),
         'materialization completion or ownership changed')
    need(len(value['observations']) == value['session_snapshot']['completed_executions'] == job['actions'],
         'materialization action coverage changed')
    for action, observed in enumerate(value['observations']):
        need(observed['action'] == action and observed['complete_consumption'] is True, 'incomplete materialization action')
        compare(expected, result_from_record(observed['actual']), workload.contract(case))
        fields, clocks = observed['fields'], observed['clocks']
        no_fallback(fields)
        validated = query_phase_timings(fields, clocks['complete_workflow_seconds'], allow_missing=False)
        need(all(clocks[name] == value for name, value in validated.items()), 'materialization clock changed')
        need(fields['relational_materialized_relations'] == str(int(job['materialized']))
             and fields['relational_materialized_reuses'] == str(len(case.consumers) - 1 if job['materialized'] else 0)
             and fields['relational_materialized_rows'] == str(normalized if job['materialized'] else 0),
             'materialization capture/reuse changed')
        need(0 <= int(fields['execution_resource_observed_native_peak_reserved_bytes']) <= case.memory_bytes,
             'materialization exceeded memory grant')
        if job['require_spilled_relation'] and job['materialized']:
            need(int(fields['relational_spilled_relations']) > 0, 'pressure control did not spill')
        if case.spill:
            need(fields['relational_spill_owned_cleanup_completed'] == 'true'
                 and 0 <= int(fields['relational_spill_peak_disk_bytes']) <= 512 << 20,
                 'materialization spill cleanup/quota changed')
    return expected


def declared_jobs(root, prepared, semantic):
    """Regenerate the full prospectively selected case/role/format/action grid."""
    cases = [replace(case, fixture='empty' if case.fixture == 'empty' else 'semantic', memory_bytes=128 << 20)
             for case in workload.CASES] if semantic else workload.CASES
    specs = {spec.name: spec for spec in workload.FIXTURES}
    need(len(prepared['cases']) == len(cases), 'materialization case coverage changed')
    result = []
    for index, (case, record) in enumerate(zip(cases, prepared['cases'])):
        spec = specs[case.fixture]
        need(record['case'] == json.loads(json.dumps(asdict(case))) and record['fixture'] == asdict(spec)
             and record['case_identity'] == workload.identity(case, spec), 'materialization fixture changed')
        formats = prepared['sources'][case.fixture]
        need(set(formats) == ({'parquet', 'arrow', 'vortex'} if semantic else {'parquet'}),
             'materialization format coverage changed')
        for extension, sources in formats.items():
            for materialized in ((False, True) if index % 2 == 0 else (True, False)):
                role = 'materialized' if materialized else 'ordinary'
                name = f'{index:02d}-{case.name}-{extension}-{role}'
                result.append(dict(**record, name=name, sources=sources, format=extension,
                    materialized=materialized, sql=workload.sql(case, sources, materialized),
                    spill_workspace=root + '/spill/' + name,
                    require_spilled_relation=case.spill and not semantic,
                    actions=2 if semantic and case.name == 'two_consumers' else 1))
    return result


def replay(bundle, item, runtime, external, frozen_files):
    from shared_performance_replay import linked
    root = str(Path(item['protocol']['path']).parent)
    protocol, prepared, summary = (linked(bundle, item[name]) for name in ('protocol', 'prepared', 'observations'))
    semantic = prepared['semantic']
    need(type(semantic) is bool and prepared['status'] == 'passed'
         and prepared['comparison_engine_invoked'] is False and prepared['performance_executions'] == 0,
         'materialization preparation incomplete')
    need(summary['status'] == 'passed' and summary['complete'] is True and summary['posthash_verified'] is True
         and summary['semantic'] is semantic and summary['comparison_engine_invoked'] is False,
         'materialization observation incomplete')
    need(summary['protocol']['sha256'] == item['protocol']['sha256'], 'materialization protocol changed')
    frozen_files(bundle, protocol['frozen_files'], external)
    frozen_files(bundle, prepared['input_files'], external)
    frozen_files(bundle, prepared['runtime'], external)
    bundle.identity(prepared['generator'])
    products = {Path(row['path']).name: row for row in prepared['runtime']
                if Path(row['path']).name in runtime['files']}
    need(set(products) == set(runtime['files']) and all(
        products[name]['path'] == runtime['path'] + '/' + name
        and products[name]['sha256'] == identity['sha256']
        and products[name]['generation']['bytes'] == identity['bytes']
        for name, identity in runtime['files'].items()), 'materialization frozen product changed')
    expected_jobs = declared_jobs(root, prepared, semantic)
    need(len(protocol['jobs']) == len(expected_jobs) == len(summary['observations']) == (60 if semantic else 20),
         'materialization job grid incomplete')
    if semantic:
        need(protocol['status'] == 'frozen_semantic_acceptance' and protocol['performance_claim'] is False,
             'semantic controls gained timing credit')
    else:
        need(protocol['status'] == 'frozen_before_execution' and protocol['performance_executions_before_freeze'] == 0
             and protocol['repeats'] == 1 and protocol['parallelism'] == 2
             and protocol['clock'] == 'engine_execution_seconds', 'mechanism protocol changed')
    need(protocol['comparison_engine_invoked'] is False, 'unexpected comparison engine')
    pairs, actions = {}, 0
    for job, wanted, record in zip(protocol['jobs'], expected_jobs, summary['observations']):
        need({key: value for key, value in job.items() if key not in {'repository', 'library'}} == wanted,
             'materialization job declaration changed')
        need(job['library'] == runtime['path'] + '/_native.abi3.so', 'materialization runtime changed')
        need((record['name'], record['case_id'], record['materialized'], record['format'], record['status']) ==
             (job['name'], job['case']['name'], job['materialized'], job['format'], 'passed'),
             'materialization observation order/role changed')
        receipt = strict_json(bundle.raw(record['receipt']))
        stdout = bundle.raw(record['stdout'])
        need(receipt['stdout'] == record['stdout'] and receipt['stdout_sha256'] == record['stdout_sha256'] == sha(stdout)
             and sha(bundle.raw(receipt['stderr'])) == receipt['stderr_sha256'], 'materialization output identity changed')
        need(receipt['command'][-2] == '--job' and sha(bundle.raw(receipt['command'][-1])) == receipt['job_sha256']
             and strict_json(bundle.raw(receipt['command'][-1])) == job, 'materialization request changed')
        value = strict_json(stdout)
        need(worker_outcome(receipt, stdout) == value and value['status'] == 'passed', 'materialization supervision failed')
        reference_bytes = bundle.raw(job['reference_path'])
        need(sha(reference_bytes) == job['reference_sha256'], 'materialization reference identity changed')
        expected = validate_result(value, job, strict_json(reference_bytes))
        actions += len(value['observations'])
        if not semantic:
            observed = value['observations'][0]
            role = 'materialized' if job['materialized'] else 'ordinary'
            pair = pairs.setdefault(job['case']['name'], dict(case=job['case'], fixture=job['fixture']))
            need(role not in pair, 'duplicate materialization role')
            pair[role] = dict(engine_execution_seconds=observed['clocks']['engine_execution_seconds'],
                rows=len(expected.rows), fields=observed['fields'], stdout=dict(path=record['stdout'],
                    sha256=record['stdout_sha256']))
    if semantic:
        controls = strict_json(bundle.raw(root + '/controls.json'))
        need(actions == 66 and controls == dict(status='passed', semantic_jobs=60, complete_actions=66,
             rejection_controls=CONTROLS, performance_execution=False, comparison_engine_invoked=False),
             'materialization semantic control coverage changed')
        return dict(jobs=60, complete_actions=66, rejection_controls=CONTROLS, performance_claim=False)
    need(actions == 20 and len(pairs) == 10, 'materialization pair coverage changed')
    saved = strict_json(bundle.raw(root + '/replay.json'))
    need(saved['status'] == 'passed' and saved['complete_pairs'] == 10 and saved['executions'] == 20
         and saved['comparison_engine_invoked'] is False and saved['pairs'].keys() == pairs.keys(),
         'saved materialization pair summary changed')
    concise = {}
    for name, pair in pairs.items():
        previous = saved['pairs'][name]
        need(pair['case'] == previous['case'] and pair['fixture'] == previous['fixture'], 'saved mechanism case changed')
        for role in ('ordinary', 'materialized'):
            need({key: pair[role][key] for key in ('engine_execution_seconds', 'rows', 'fields')} ==
                 {key: previous[role][key] for key in ('engine_execution_seconds', 'rows', 'fields')},
                 'saved mechanism result/counters changed')
            need(pair[role]['stdout']['path'] == previous[role]['stdout']['path']
                 and pair[role]['stdout']['sha256'] == previous[role]['stdout']['sha256'], 'saved mechanism output changed')
        delta = pair['materialized']['engine_execution_seconds'] - pair['ordinary']['engine_execution_seconds']
        need(delta == previous['materialized_minus_ordinary_engine_seconds'], 'saved mechanism clock delta changed')
        concise[name] = {role: {key: pair[role][key] for key in ('engine_execution_seconds', 'rows')}
                        for role in ('ordinary', 'materialized')}
    return dict(complete_pairs=10, executions=20, pairs=concise,
                scope='separate mechanism observations; no overall engine gain decision')
