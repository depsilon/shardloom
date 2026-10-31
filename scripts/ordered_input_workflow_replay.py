# SPDX-License-Identifier: Apache-2.0
"""Replay the separately frozen 48-workflow ordering cohort from complete evidence."""
import gzip
import json
import math
from pathlib import Path

from native_workflow_protocol import strict_json
from ordered_input_public_replay import reference_fixture
from query_surface.evidence import no_fallback, worker_outcome
from query_surface.replay import need, sha
from query_surface.semantics import ResultContract, TypedResult, compare, result_from_record
from shared_followthrough_replay import frozen_files
from shared_performance_replay import linked

BASELINE_SECONDS = 1.307041040


def workflow_replay(bundle, root, external):
    inputs = strict_json(bundle.raw(root + '/inputs.json'))
    observations = strict_json(bundle.raw(root + '/native-observations.json'))
    saved = strict_json(bundle.raw(root + '/replay.json'))
    role = inputs['role']
    need(role in {'accepted', 'candidate'} and inputs['schema_version'] == 1
         and inputs['status'] == 'frozen_before_execution'
         and inputs['new_harness_outside_existing_product'] is True,
         'supplemental input declaration changed')
    need(linked(bundle, observations['input_manifest']) == inputs
         and observations['role'] == role and observations['complete'] is True
         and observations['posthash_verified'] is True and observations['status'] == 'passed'
         and observations['comparison_engine_invoked'] is False,
         'supplemental observations incomplete')
    protocol = linked(bundle, inputs['protocol'])
    fixture = reference_fixture()
    need(protocol['schema_version'] == 1
         and protocol['status'] == 'declared_before_either_runtime_execution'
         and protocol['performance_executions_before_declaration'] == 0
         and protocol['full43_executed'] is protocol['comparison_engine_invoked'] is False
         and protocol['cases'] == inputs['cases']
         and protocol['resources'] == inputs['resources']
         == dict(memory_bytes=2 << 30, parallelism=2, timeout_seconds=180),
         'supplemental prospective protocol changed')
    need(protocol['generator'] == dict(seed=fixture.SEED, sizes=list(fixture.SIZES),
         templates=list(fixture.TEMPLATES), parquet_row_group_rows=2048, compression='zstd',
         dictionary=True, complete_input_readback='every schema and value reopened independently by PyArrow'),
         'supplemental generator changed')
    need(protocol['observer_driver']['sha256'] == '82a674b8c0f4186dfe764b59a3d4fd62409cc9574c87d428c5473f13d2760c73'
         and protocol['source_contract']['sha256'] == '358174355170a114b34b73c3dd2a072edbb6784e72d29c5baf75e3de975feae1',
         'supplemental frozen observer or prospective contract changed')
    bundle.identity(protocol['observer_driver'])
    bundle.identity(protocol['source_contract'])
    frozen_files(bundle, protocol['frozen_files'], external)
    frozen_files(bundle, inputs['frozen_files'], external)
    need(inputs['frozen_files'][:len(protocol['frozen_files'])] == protocol['frozen_files'],
         'supplemental original inputs or harness changed')
    fixture_path = str(Path(protocol['harness_root']) / 'python/tests/native_ordered_input_cases.py')
    source = Path(__file__).resolve().parents[1] / 'python/tests/native_ordered_input_cases.py'
    need(bundle.raw(fixture_path) == source.read_bytes(), 'maintained independent fixture changed')
    runtime = linked(bundle, inputs['runtime_manifest'])
    need(runtime['source_revision'] == inputs['source_revision']
         and runtime['profile'] == 'release' and runtime['features'] == ['release-user-surfaces']
         and len(inputs['product_input_identity_checks']) > 2000,
         'supplemental runtime identity changed')
    for relative, item in inputs['product_input_identity_checks'].items():
        need(runtime['source_snapshot'][relative] == item, 'supplemental product source changed')
    products = {Path(item['path']).name: item['sha256'] for item in inputs['runtime']}
    need(products == runtime['products'] and set(products) == {'shardloom', '_native.abi3.so'},
         'supplemental executable identity changed')
    rows, seconds, per_case = 0, [], []
    cases = inputs['cases']
    need([(case['template'], case['rows']) for case in cases]
         == [(template, size) for size in fixture.SIZES for template in fixture.TEMPLATES]
         and len(observations['observations']) == 48, 'supplemental case grid changed')
    identities = {item['path']: item for item in inputs['frozen_files']}
    for case, observation in zip(cases, observations['observations'], strict=True):
        identity_record = {name: value for name, value in case.items() if name != 'case_identity'}
        need(sha(json.dumps(identity_record, sort_keys=True, separators=(',', ':')).encode())
             == case['case_identity'] and case['case_id'] == f"{case['template']}_{case['rows']}"
             and case['ordered'] is True and case['sql'] == fixture.statement(case['template'], case['sources']),
             'supplemental query identity changed')
        generated_sources = fixture.records(case['template'], case['rows'])
        need(set(case['sources']) == set(generated_sources)
             and all(path in identities and Path(path).suffix == '.parquet' for path in case['sources'].values()),
             'supplemental source binding changed')
        expected = result_from_record(strict_json(gzip.decompress(bundle.identity(identities[case['reference']]))))
        independent = TypedResult(expected.schema, fixture.reference_rows(case['template'], generated_sources))
        contract = ResultContract(expected.schema, ordered=True)
        compare(independent, expected, contract)
        need(observation['case_id'] == case['case_id'] and observation['template'] == case['template']
             and observation['input_rows'] == case['rows'] and observation['status'] == 'passed'
             and observation['detail'] is None, 'supplemental case observation changed')
        stdout = bundle.identity(observation['stdout'])
        receipt = linked(bundle, observation['receipt'])
        need(receipt['stdout'] == observation['stdout']['path'] and receipt['returncode'] == 0
             and not receipt['guard_failures'] and receipt['stdout_sha256'] == sha(stdout)
             and sha(bundle.raw(receipt['stderr'])) == receipt['stderr_sha256'],
             'supplemental supervision or output changed')
        command = receipt['command']
        need(len(command) == 6 and command[1] == '-B'
             and Path(command[2]).name == 'ordered_input_cohort.py'
             and command[3:5] == ['worker', '--job'], 'supplemental worker changed')
        need(bundle.raw(command[2], protocol['observer_driver']['sha256'])
             == bundle.identity(protocol['observer_driver']), 'supplemental executed observer changed')
        job = strict_json(bundle.raw(command[5], receipt['job_sha256']))
        need(job == dict(case=case, library=next(item['path'] for item in inputs['runtime']
                 if Path(item['path']).name == '_native.abi3.so'), memory_bytes=2 << 30,
                 parallelism=2, timeout=180, reference_sha256=identities[case['reference']]['sha256']),
             'supplemental job inputs changed')
        result = worker_outcome(receipt, stdout)
        need(result['status'] == 'passed' and result['case_identity'] == case['case_identity']
             and result['case_id'] == case['case_id'] and result['template'] == case['template']
             and result['input_rows'] == case['rows'] and result['complete_consumption'] is True
             and result['source_session_released_before_export'] is True
             and result['comparison_engine_invoked'] is False, 'supplemental complete execution changed')
        no_fallback(result['fields'])
        actual = result_from_record(result['actual'])
        compare(independent, actual, contract)
        need(len(actual.rows) == result['output_rows'] == observation['output_rows'] == case['expected_rows']
             and result['clocks'] == observation['clocks']
             and result['clocks']['engine_timing_reported'] is True,
             'supplemental output or engine clock changed')
        elapsed = result['clocks']['engine_execution_seconds']
        need(type(elapsed) in (int, float) and math.isfinite(elapsed) and elapsed >= 0,
             'supplemental engine clock unavailable')
        fields = result['fields']
        need(fields['execution_resource_admitted_memory_bytes'] == str(2 << 30)
             and fields['execution_resource_declared_max_parallelism'] == '2'
             and fields['execution_resource_observed_native_denied_reservations'] == '0',
             'supplemental resources changed')
        seconds.append(elapsed)
        rows += len(actual.rows)
        per_case.append(dict(case_id=case['case_id'], engine_seconds=elapsed))
    total = math.fsum(seconds)
    need(rows == 557420 and total == observations['engine_execution_seconds'],
         'supplemental complete engine total changed')
    expected_saved = dict(status='passed', role=role, complete_comparisons=48, complete_rows=rows,
        engine_execution_seconds=total, full43_executed=False, comparison_engine_invoked=False)
    if role == 'accepted':
        need(total == BASELINE_SECONDS, 'accepted supplemental observation changed')
    else:
        expected_saved.update(baseline_engine_seconds=BASELINE_SECONDS,
            benefit_gate_passed=total < BASELINE_SECONDS,
            decision='supplemental_gain_requires_347_gain' if total < BASELINE_SECONDS else 'drop_supplemental_no_gain')
    need(saved == expected_saved, 'supplemental saved disposition differs from complete replay')
    return dict(**saved, independent_references_regenerated=48, cases=per_case)
