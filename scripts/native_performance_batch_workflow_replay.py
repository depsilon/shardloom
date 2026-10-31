# SPDX-License-Identifier: Apache-2.0
"""Replay complete 80-workflow observations, sources, resources and engine clocks."""
import gzip
import json
import math
from pathlib import Path

from native_workflow_protocol import strict_json
from native_performance_batch_public_replay import fixture, replay_trace
from query_surface.evidence import worker_outcome
from query_surface.replay import need, sha
from query_surface.semantics import ResultContract, compare, result_from_record
from query_timing_report import query_phase_timings
from shared_followthrough_replay import frozen_files
from shared_performance_replay import complete_gate, linked


def product_sources(bundle, root, inputs, runtime):
    need(runtime['source_revision'] == inputs['source_revision'] and runtime['profile'] == 'release'
         and runtime['features'] == ['release-user-surfaces'], 'workflow runtime build changed')
    build_bytes = bundle.raw(root + '/' + runtime['build_receipt'])
    need(sha(build_bytes) == runtime['build_receipt_sha256'], 'workflow build receipt changed')
    build = strict_json(build_bytes)
    complete_gate(build)
    command = ['cargo', 'build', '--locked', '--offline', '--release', '--features',
               'release-user-surfaces', '-p', 'shardloom-cli', '-p', 'shardloom-python']
    if 'reference_derivation' in runtime:
        derivation = linked(bundle, runtime['reference_derivation'])
        need(derivation['build_command'] == command + ['--manifest-path', runtime['source_root'] + '/Cargo.toml']
             and len(build['command']) == 4 and build['command'][1:] ==
             ['-B', root + '/tools/batch_metadata_reference.py', 'build'],
             'metadata reference build command changed')
        need(bundle.raw(build['command'][2], derivation['helper']['sha256']) == bundle.identity(derivation['helper']),
             'metadata reference build helper changed')
    else:
        need(build['command'] == command, 'workflow release command changed')
    need(build['head'] == runtime['source_revision'], 'workflow release revision changed')
    wanted = {name: item for name, item in runtime['source_snapshot'].items()
              if name.endswith(('.rs', '.toml', '.lock')) or name.startswith('python/src/')}
    checks = inputs['product_input_identity_checks']
    need(len(checks) > 2000 and set(checks) == set(wanted), 'workflow product source coverage changed')
    for name, expected in wanted.items():
        item = checks[name]
        need({key: item[key] for key in ('bytes', 'sha256')} == expected, 'workflow product source changed')
        proof = item['proof']
        need(proof['kind'] in {'current_source', 'retained_source', 'git_source'}, 'unknown product source proof')
        if proof['kind'] == 'git_source':
            need(proof['object'] == runtime['source_revision'] + ':' + name,
                 'historical product source revision changed')
        elif proof['kind'] == 'retained_source':
            need(inputs['role'] == 'accepted' and proof['path'] == root + '/source-blobs/' + item['sha256'],
                 'retained product source identity changed')
        else:
            need(proof['path'] == build['cwd'] + '/' + name, 'current product source binding changed')
        bundle.identity(dict(path=build['cwd'] + '/' + name, **expected))
    need({Path(item['path']).name: item['sha256'] for item in inputs['runtime']} == runtime['products']
         and set(runtime['products']) == {'shardloom', '_native.abi3.so'}, 'workflow executable identity changed')


def declaration(bundle, case, template, protocol, identities, reference):
    need({key: case[key] for key in template} == template and case['ordered'] is True
         and case['resources'] == reference.resources(template), 'workflow case grid or resources changed')
    identity_record = {key: value for key, value in case.items() if key != 'case_identity'}
    need(sha(json.dumps(identity_record, sort_keys=True, separators=(',', ':')).encode()) == case['case_identity'],
         'workflow case identity changed')
    records = reference.records(template)
    need(set(case['sources']) == set(records)
         and case['input_rows'] == {name: len(rows) for name, rows in records.items()},
         'workflow input population changed')
    kind = reference.source_kind(template)
    for name, source in case['sources'].items():
        need(source['kind'] == kind and source['path'] in identities, 'workflow source binding changed')
        if kind in ('parquet', 'vortex'):
            need(source['path'] == source['reference'] and Path(source['path']).suffix == '.' + kind,
                 'workflow file binding changed')
        else:
            names = ['id', 'k', 'v'] if kind == 'memory' else reference.batch_columns(template)
            fields = [[field, json.dumps({'Primitive': ['i64', True]})] for field in names]
            need(source['fields'] == fields and source['reference'] == f"memory://{case['case_id']}/{name}",
                 'workflow resident/producer schema changed')
            need(strict_json(gzip.decompress(bundle.identity(identities[source['path']]))) == records[name],
                 'workflow resident/producer values changed')
    need(case['sql'] == reference.statement(template, {
        name: source['reference'] for name, source in case['sources'].items()}), 'workflow SQL changed')
    if kind == 'vortex':
        manifest = linked(bundle, protocol['encoded_input_manifest'])
        matches = [row for row in manifest['files'] if row['rows'] == case['rows'] and row['shape'] == case['shape']]
        need(len(matches) == 1 and case['sources']['input']['physical'] == matches[0]['physical']
             and case['sources']['input']['path'] == matches[0]['path']
             and matches[0]['complete_provider_readback'] is True, 'workflow actual encoded input changed')
    independent = reference.reference(template, records)
    expected = result_from_record(strict_json(gzip.decompress(bundle.identity(identities[case['reference']]))))
    wanted = ResultContract(independent.schema, ordered=True)
    compare(independent, expected, wanted)
    need(len(independent.rows) == case['expected_rows'], 'workflow independent result population changed')
    return independent, wanted


def workflow_replay(bundle, root, external, *, baseline_seconds=None, failed_history=False,
                    interrupted_history=False, retained_candidate=False):
    inputs = strict_json(bundle.raw(root + '/inputs.json'))
    observations = strict_json(bundle.raw(root + '/native-observations.json'))
    need(not (failed_history and interrupted_history), 'ambiguous historical attempt')
    need(not (retained_candidate and (failed_history or interrupted_history)),
         'retained candidate cannot replace a historical attempt')
    saved = None if failed_history or interrupted_history else strict_json(bundle.raw(root + '/replay.json'))
    role = inputs['role']
    need(role in {'accepted', 'candidate'} and inputs['schema_version'] == 1
         and inputs['status'] == 'frozen_before_execution'
         and inputs['new_harness_outside_existing_product'] is True
         and ((role == 'accepted') == (baseline_seconds is None)), 'workflow role or input declaration changed')
    need(linked(bundle, observations['input_manifest']) == inputs and observations['role'] == role
         and observations['status'] == ('failed' if interrupted_history else
                                       'complete_with_failures' if failed_history else 'passed')
         and observations['complete'] is (not interrupted_history)
         and observations.get('posthash_verified', False) is (not interrupted_history)
         and observations['comparison_engine_invoked'] is False,
         'workflow observations incomplete')
    protocol = linked(bundle, inputs['protocol'])
    reference = fixture()
    original = protocol.get('revision', 1) == 1
    need(protocol['schema_version'] == 1
         and protocol['status'] == ('declared_before_retained_candidate_execution' if retained_candidate
                                   else 'declared_before_either_runtime_execution' if original
                                   else 'declared_before_either_revised_runtime_execution')
         and protocol['performance_executions_before_declaration'] ==
             (241 if retained_candidate else 0 if original else 80 if interrupted_history else 81)
         and protocol['full43_executed'] is protocol['comparison_engine_invoked'] is False
         and protocol['cases'] == inputs['cases'] and protocol['generator']['seed'] == reference.SEED
         and protocol['generator']['grid'] == [[family, list(sizes), list(shapes)]
                                               for family, sizes, shapes in reference.GRID],
         'workflow prospective protocol changed')
    if retained_candidate:
        from native_performance_batch_subset_replay import retained_protocol
        need(role == 'candidate', 'retained declaration may not rerun the reference')
        retained_protocol(bundle, protocol)
    elif failed_history:
        need(original and role == 'accepted' and observations['engine_execution_seconds'] is None,
             'failed historical cohort cannot receive a complete total')
    elif interrupted_history:
        cache = protocol['harness_root'] + '/python/src/shardloom/__pycache__/__init__.cpython-313.pyc'
        need(protocol['revision'] == 2 and role == 'accepted'
             and observations['engine_execution_seconds'] is None
             and observations['error'] == repr(AssertionError(cache)),
             'interrupted cache-identity attempt cannot receive a complete total')
    else:
        need(not original and protocol['revision'] == 3
             and protocol['revised_pair_executions_before_declaration'] == 0
             and protocol['historical_performance_executions'] == 81
             and protocol['input_sql_resource_reference_order_changes'] is False,
             'corrected pair was not declared prospectively')
    frozen_files(bundle, protocol['frozen_files'], external)
    frozen_files(bundle, inputs['frozen_files'], external)
    need(inputs['frozen_files'][:len(protocol['frozen_files'])] == protocol['frozen_files'],
         'workflow source or harness inputs changed')
    maintained = Path(__file__).resolve().parents[1] / 'python/tests/native_performance_batch_cases.py'
    frozen_fixture = protocol['harness_root'] + '/python/tests/native_performance_batch_cases.py'
    need(bundle.raw(frozen_fixture) == maintained.read_bytes(), 'maintained independent fixture differs')
    runtime = linked(bundle, inputs['runtime_manifest'])
    product_sources(bundle, str(Path(root).parent), inputs, runtime)
    cases = inputs['cases']
    templates = reference.cases()
    need(len(cases) == len(templates) == 80
         and len(observations['observations']) == (1 if interrupted_history else 80),
         'workflow cohort incomplete')
    identities = {item['path']: item for item in inputs['frozen_files']}
    rows, seconds, families, per_case, errors = 0, [], {}, [], []
    count = len(observations['observations'])
    for case, template, observation in zip(cases[:count], templates[:count], observations['observations'], strict=True):
        independent, wanted = declaration(bundle, case, template, protocol, identities, reference)
        need(observation['case_id'] == case['case_id'] and observation['family'] == case['family']
             and observation['shape'] == case['shape'] and observation['input_rows'] == case['rows'],
             'workflow observation identity changed')
        stdout = bundle.identity(observation['stdout'])
        receipt = linked(bundle, observation['receipt'])
        need(receipt['returncode'] == 0 and receipt['guard_failures'] == []
             and receipt['stdout'] == observation['stdout']['path'] and receipt['stdout_sha256'] == sha(stdout)
             and sha(bundle.raw(receipt['stderr'])) == receipt['stderr_sha256'], 'workflow supervision changed')
        command = receipt['command']
        need(len(command) == 6 and command[1] == '-B' and Path(command[2]).name == 'batch_cohort.py'
             and command[3:5] == ['worker', '--job'], 'workflow worker command changed')
        need(bundle.raw(command[2], protocol['observer_driver']['sha256'])
             == bundle.identity(protocol['observer_driver']), 'executed workflow observer changed')
        job_bytes = bundle.raw(command[5], receipt['job_sha256'])
        job = strict_json(job_bytes)
        output = observation['stdout']['path'].removesuffix('.stdout.json') + '.actual.json.gz'
        need(job == dict(case=case, library=next(item['path'] for item in inputs['runtime']
                if Path(item['path']).name == '_native.abi3.so'), harness=protocol['harness_root'],
                output=output, reference_sha256=identities[case['reference']]['sha256']),
             'workflow job binding changed')
        result = worker_outcome(receipt, stdout)
        if failed_history and observation['status'] != 'passed':
            expected_status = ('harness_error' if case['family'] in ('B6', 'B9') else 'engine_error')
            need(result['status'] == observation['status'] == expected_status
                 and result['detail'] == observation['detail']
                 and observation['clocks'] is observation['actual'] is observation['output_rows'] is None
                 and result['comparison_engine_invoked'] is False,
                 'historical failure changed')
            if expected_status == 'harness_error':
                need(result['exception'] == 'TypeError' and result['detail'] ==
                     "'list' object is not an instance of 'tuple'", 'historical binding failure changed')
            else:
                need(case['family'] in ('B7', 'B8') and case['rows'] == 65537
                     and case['shape'] != 'resident_sort' and case['resources']['memory_bytes'] == 16 << 20
                     and result['exception'] == 'RuntimeError'
                     and 'requested=19207008,' in result['detail']
                     and 'limit=16777216; fallback execution was not attempted' in result['detail'],
                     'historical metadata admission failure changed')
            errors.append(dict(case_id=case['case_id'], status=expected_status))
            continue
        need(observation['status'] == 'passed' and observation['detail'] is None,
             'workflow result did not pass')
        need(result['status'] == 'passed' and result['case_id'] == case['case_id']
             and result['case_identity'] == case['case_identity'] and result['family'] == case['family']
             and result['shape'] == case['shape'] and result['input_rows'] == case['input_rows']
             and result['comparison_engine_invoked'] is False and result['final_reserved_bytes'] == 0,
             'workflow result identity or final ownership changed')
        replay_trace(case, result)
        need(result['actual'] == observation['actual'] and result['clocks'] == observation['clocks'],
             'workflow result or clock binding changed')
        actual = result_from_record(strict_json(gzip.decompress(bundle.identity(result['actual']))))
        compare(independent, actual, wanted)
        need(len(actual.rows) == result['output_rows'] == observation['output_rows'] == case['expected_rows'],
             'workflow result population changed')
        clocks = query_phase_timings(result['fields'], result['clocks']['process_wall_seconds'])
        need(result['clocks'] == clocks and clocks['engine_timing_reported'] is True,
             'workflow engine phases changed')
        elapsed = clocks['engine_execution_seconds']
        need(type(elapsed) in (int, float) and math.isfinite(elapsed) and elapsed >= 0,
             'workflow engine time unavailable')
        rows += len(actual.rows)
        seconds.append(elapsed)
        families.setdefault(case['family'], []).append(elapsed)
        per_case.append(dict(case_id=case['case_id'], family=case['family'], engine_seconds=elapsed))
    if interrupted_history:
        need(len(seconds) == 1 and not errors and rows == 1,
             'interrupted reference observation population changed')
        return dict(status='interrupted', attempted=1, passed=1, complete_engine_seconds=None,
                    reason='generated_bytecode_identity_changed',
                    all_raw_observations_preserved=True, query_executions=0)
    if failed_history:
        need(len(seconds) == 57 and len(errors) == 23
             and sum(row['status'] == 'harness_error' for row in errors) == 16,
             'historical failure population changed')
        return dict(status='complete_with_failures', attempted=80, passed=57, harness_errors=16,
                    engine_errors=7, complete_engine_seconds=None, failures=errors,
                    all_raw_observations_preserved=True, query_executions=0)
    total = math.fsum(seconds)
    need(rows == 871209 and observations['engine_execution_seconds'] == total,
         'workflow complete result or engine total changed')
    expected = dict(status='passed', role=role, complete_comparisons=80, complete_rows=rows,
        engine_execution_seconds=total, family_engine_seconds={key: math.fsum(value) for key, value in families.items()},
        full43_executed=False, comparison_engine_invoked=False)
    if role == 'candidate':
        expected.update(baseline_engine_seconds=baseline_seconds, benefit_gate_passed=total < baseline_seconds,
            decision='supplemental_gain_requires_347_gain' if total < baseline_seconds else 'drop_supplemental_no_gain')
    need(saved == expected, 'workflow saved disposition differs from complete replay')
    return dict(**saved, independent_references_regenerated=80, cases=per_case)
