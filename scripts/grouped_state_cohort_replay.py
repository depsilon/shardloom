# SPDX-License-Identifier: Apache-2.0
"""Keep the grouped-state observation on the original, immutable 347 harness."""
from pathlib import Path

from query_surface.replay import need
from shared_performance_replay import linked
from single_predicate_mask_cohort_replay import APPLICATION_FILES, APPLICATION_HEAD


def harness_continuity(bundle, inputs, protocol, runtime, declaration):
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

    driver = str(harness.parent / 'tools/performance_cohort_masks.py')
    original = bundle.raw(driver, declaration['sources'][driver])
    current = bundle.identity(protocol['observer_driver'])
    label = b'single-predicate mask candidate'
    contract = b'native-single-predicate-masks-2026-10-30.md'
    need(original.count(label) == original.count(contract) == 1
         and original.replace(label, b'grouped-state candidate').replace(
             contract, b'native-grouped-state-access-2026-10-31.md') == current,
         'grouped observer changed more than its label and frozen contract path')
    need(protocol['original_execution_observer'] in previous['frozen_files'], 'original observer changed')
    expected = [retained[str(harness / 'benchmarks/traditional_analytics/query_surface' / name)]
                for name in ('worker.py', 'adapters.py')]
    need(protocol['native_sql_entrypoints'] == expected, 'native SQL entrypoints changed')
    return dict(retained_harness_files=len(retained), application_source_differences=len(entries),
                observer_only_changes=['candidate label', 'frozen contract path'],
                observation_and_supervision_unchanged=True)
