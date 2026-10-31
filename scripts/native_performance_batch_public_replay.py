# SPDX-License-Identifier: Apache-2.0
"""Replay the ten-owner batch's public controls without a query provider."""
import gzip
import importlib.util
import json
from pathlib import Path

from native_workflow_protocol import strict_json
from query_surface.evidence import no_fallback
from query_surface.replay import need, sha
from query_surface.semantics import ResultContract, compare, result_from_record, result_record


def fixture():
    path = Path(__file__).resolve().parents[1] / 'python/tests/native_performance_batch_cases.py'
    spec = importlib.util.spec_from_file_location('performance_batch_reference_fixture', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def raw(value):
    return json.dumps(value, separators=(',', ':'), allow_nan=False).encode()


def public_cases(reference):
    cases = []
    for family, _, shapes in reference.GRID:
        if family == 'B3':
            continue
        for shape in shapes:
            rows = 4099 if family in ('B7', 'B8') else 2051 if family == 'B9' else 33
            if family == 'B9' and shape == 'pressured':
                rows = 16385
            cases.append(dict(family=family, shape=shape, rows=rows,
                              case_id=f'control_{family}_{shape}_{rows}'))
    need(len(cases) == 36, 'public control grid changed')
    return cases


def resource_fields(fields, policy):
    def number(name):
        value = fields['execution_resource_' + name]
        need(isinstance(value, str) and value.isdecimal(), 'invalid resource observation: ' + name)
        return int(value)
    need(number('admitted_memory_bytes') == policy['memory_bytes']
         and number('declared_memory_bytes') == policy['memory_bytes']
         and number('admitted_max_parallelism') == number('declared_max_parallelism') == policy['parallelism']
         and number('observed_native_denied_reservations') == 0
         and 0 <= number('observed_native_reserved_bytes')
         <= number('observed_native_peak_reserved_bytes') <= policy['memory_bytes'],
         'batch resource contract changed')


def replay_trace(case, report, *, public=False):
    reference = fixture()
    policy = reference.resources(case)
    fields = report['fields']
    no_fallback(fields)
    resource_fields(fields, policy)
    need(report['complete_consumption'] is True
         and report['source_session_released_before_export'] is True,
         'batch result consumption or source release changed')
    traces = report['producer_traces']
    if case['family'] != 'B9':
        need(traces == {}, 'unexpected batch producer')
    else:
        need(set(traces) == {'input'}, 'replay producer coverage changed')
        trace = traces['input']
        width = 1 if case['shape'] == 'pressured' else int(case['shape'].split('_')[1])
        need(trace == dict(opened=1, rows=case['rows'],
                           batches=(case['rows'] + width - 1) // width, ended=True, closed=True),
             'source was not consumed exactly once')
        need(int(fields['native_input_replay_sources']) == 1
             and int(fields['native_input_replay_rows_detached']) == case['rows']
             and int(fields['native_input_replay_rows']) >= case['rows'],
             'native replay was not exercised')
        if public and case['shape'] == 'pressured':
            need(int(fields['native_input_replay_spilled_sources']) == 1,
                 'pressure control did not exercise native spilled replay')
    if policy['spill']:
        need(fields['relational_spill_owned_cleanup_completed'] == 'true', 'spill cleanup incomplete')
        if case['family'] in ('B7', 'B8'):
            need(int(fields['relational_spill_runs_written']) > 0, 'spill owner was not exercised')


def public_replay(bundle, value, declaration, runtime, *, reference_harness=False):
    reference = fixture()
    cases = public_cases(reference)
    need(declaration['status'] == 'declared_before_control_queries'
         and declaration['cases'] == cases and declaration['acceptance_workflows_executed'] == 0,
         'public controls were not separately declared')
    need(value['status'] == 'passed' and value['complete_controls'] == len(value['parts']) == 36
         and value['errors'] == value['failures'] == value['skipped'] == []
         and value['sources_unchanged'] is True and value['acceptance_workflows_executed'] == 0
         and value['comparison_engine_invoked'] is value['full43_executed'] is False
         and value['source_identities'] == declaration['sources']
         and value['runtime_library_sha256'] == runtime['products']['_native.abi3.so'],
         'public control acceptance incomplete')
    exercised_runtime = strict_json(bundle.identity(value['runtime_manifest']))
    need(exercised_runtime['products'] == runtime['products'], 'public controls used another runtime')
    sources = {}
    for item in value['source_identities']:
        name = Path(item['path']).name
        need(name not in sources, 'duplicate public source identity')
        sources[name] = item
        if name == '_native.abi3.so':
            need(item['sha256'] == runtime['products'][name], 'public library identity changed')
        else:
            bundle.identity(item)
    need(set(sources) == {'check_batch_public.py', 'native_performance_batch_cases.py',
                         'test_native_performance_batch_binding.py', '_native.abi3.so'},
         'public source inventory changed')
    for name in ('native_performance_batch_cases.py', 'test_native_performance_batch_binding.py'):
        if reference_harness:
            need(runtime.get('reference_kind') == 'accepted_sources_with_only_required_metadata_admission_repair',
                 'external public harness is only admitted for the corrected reference')
            data = (Path(__file__).resolve().parents[1] / 'python/tests' / name).read_bytes()
            expected_source = dict(bytes=len(data), sha256=sha(data))
        else:
            expected_source = runtime['source_snapshot']['python/tests/' + name]
        need({key: sources[name][key] for key in ('bytes', 'sha256')} == expected_source,
             'public fixture differs from the independently retained harness')
    rows = 0
    for case, item in zip(cases, value['parts'], strict=True):
        need(item['case_id'] == case['case_id'] and 0 < item['bytes'] < 8 << 20,
             'public part identity changed')
        data = gzip.decompress(bundle.identity(item))
        need(len(data) < 32 << 20, 'oversized public result')
        record = strict_json(data)
        need(record['case'] == case, 'public result case changed')
        expected = reference.reference(case, reference.records(case))
        need(sha(raw(result_record(expected))) == declaration['references'][case['case_id']],
             'independent public reference changed')
        wanted = ResultContract(expected.schema, ordered=True)
        compare(expected, result_from_record(record['expected']), wanted)
        compare(expected, result_from_record(record['actual']), wanted)
        need(item['rows'] == len(expected.rows), 'public result is incomplete')
        replay_trace(case, record['report'], public=True)
        rows += item['rows']
    need(set(declaration['references']) == {case['case_id'] for case in cases},
         'public reference coverage changed')
    return dict(status='passed', complete_controls=36, complete_rows=rows,
                independent_references_regenerated=36, acceptance_workflows_executed=0)
