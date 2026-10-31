# SPDX-License-Identifier: Apache-2.0
"""Bind ordered-input observations to the original immutable 347-case harness."""
from pathlib import Path

from query_surface.replay import need
from shared_performance_replay import linked
from single_predicate_mask_cohort_replay import APPLICATION_FILES, APPLICATION_HEAD

SUPPLEMENTAL_GATE = ('Final retention additionally requires the separate frozen 48-workflow candidate '
    'total to be lower than the accepted 1.307041040 engine seconds; unchanged 347-case results alone '
    'cannot accept the candidate. This is protocol metadata only; the original replay result schema is unchanged.')


def harness_continuity(bundle, inputs, protocol, runtime):
    previous = linked(bundle, inputs['baseline_inputs'])
    harness = Path(inputs['harness_root'])
    need(inputs['harness_root'] == previous['harness_root'], 'benchmark harness root changed')

    def inventory(value):
        rows = [row for row in value['frozen_files'] if Path(row['path']).is_relative_to(harness)]
        need(len(rows) == len({row['path'] for row in rows}), 'duplicate harness path')
        return {row['path']: row for row in rows}

    retained = inventory(inputs)
    need(retained and retained == inventory(previous), 'original harness inventory changed')
    entries = protocol['frozen_application_harness_compatibility']
    need(len(entries) == len(APPLICATION_FILES) and {row['path'] for row in entries} == APPLICATION_FILES,
         'application harness exception set changed')
    for row in entries:
        relative = row['path']
        source, old = row['current_repository_source'], row['executed_frozen_harness']
        need(row['retained_source_revision'] == '65be06c9'
             and row['current_source_revision'] == APPLICATION_HEAD
             and row['execution_change'] is False and row['supervision_change'] is False,
             'application exception changed execution or revision')
        need(dict(bytes=source.get('bytes', source.get('generation', {}).get('bytes')),
                  sha256=source['sha256']) == runtime['source_snapshot'][relative]
             and source['path'].endswith('/' + relative), 'application source binding changed')
        bundle.identity(source)
        path = str(harness / relative)
        if relative.endswith('/_batch_function.py'):
            need(old is None and path not in retained, 'new callback module entered historical harness')
        else:
            need(old == retained[path], 'retained application bytes differ from baseline')
            bundle.identity(old)
    report = protocol['frozen_reporting_harness_compatibility']
    need(len(report) == 1 and report[0]['path'] == 'scripts/run_clickbench_query_uat.py'
         and report[0]['execution_change'] is False and report[0]['supervision_change'] is False,
         'reporting exception changed')
    old, new = (bundle.identity(report[0][key]) for key in ('executed_frozen_harness', 'current_repository_source'))
    before = b'    "local_primitive_", "native_source_demand_", "resident_", "relational_",\n'
    after = (b'    "local_primitive_", "native_source_demand_", "native_source_ranked_",\n'
             b'    "native_input_ordering_", "resident_", "relational_",\n')
    need(old.count(before) == 1 and old.replace(before, after) == new, 'reporting delta not exact')
    driver = str(harness.parent / 'tools/performance_cohort_grouped_state.py')
    original = bundle.raw(driver, '410fe441137ec9c778053e352d4814146730c6494f0772c49b4fff60903a580c')
    current = bundle.identity(protocol['observer_driver'])
    label = b'grouped-state candidate'
    contract = b'native-grouped-state-access-2026-10-31.md'
    insertion = b'        performance_executions_before_freeze=0)'
    addition = ('        supplemental_gate=' + repr(SUPPLEMENTAL_GATE) + ',\n').encode()
    need(original.count(label) == original.count(contract) == original.count(insertion) == 1
         and original.replace(label, b'ordered-input candidate').replace(
             contract, b'native-ordered-inputs-2026-10-31.md').replace(insertion, addition + insertion) == current,
         'ordered observer changed beyond its label, contract and supplemental gate')
    need(protocol['supplemental_gate'] == SUPPLEMENTAL_GATE
         and protocol['original_execution_observer'] in previous['frozen_files'],
         'original observer or separate supplemental gate changed')
    expected = [retained[str(harness / 'benchmarks/traditional_analytics/query_surface' / name)]
                for name in ('worker.py', 'adapters.py')]
    need(protocol['native_sql_entrypoints'] == expected, 'native SQL entrypoints changed')
    return dict(retained_harness_files=len(retained), application_source_differences=len(entries),
                observer_only_changes=['candidate label', 'frozen contract path', 'supplemental gate metadata'],
                observation_and_supervision_unchanged=True)
