# SPDX-License-Identifier: Apache-2.0
"""Bind typed-ordering observations to the original immutable 347-case harness."""
from pathlib import Path

from query_surface.replay import need
from shared_performance_replay import linked
from single_predicate_mask_cohort_replay import APPLICATION_FILES, APPLICATION_HEAD

SUPPLEMENTAL_GATE = ('Final retention additionally requires the separate frozen 48-workflow candidate '
    'total to be lower than the accepted 0.920503372 engine seconds; unchanged 347-case results alone '
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
    driver = str(harness.parent / 'tools/performance_cohort_ordered_inputs.py')
    original = bundle.raw(driver, '8a7bdbfe2317778c9d6ff3d1b934fbf43bbf716c7e11175bc1c9a0a68eaa6b8e')
    current = bundle.identity(protocol['observer_driver'])
    label = b'ordered-input candidate'
    contract = b'native-ordered-inputs-2026-10-31.md'
    assertion = b"    baseline_seconds = math.fsum(row['clocks']['engine_execution_seconds'] for row in observations['observations'])\n"
    addition = (b"    assert previous == OUT / 'broad-ordered-input-r1'\n"
                b"    assert baseline_seconds == 11.979219492\n"
                b"    assert role == 'candidate'\n")
    need(original.count(label) == original.count(contract) == original.count(assertion) == 1
         and original.replace(label, b'typed numeric ordering candidate').replace(
             contract, b'native-typed-ordering-2026-10-31.md').replace(
             b'1.307041040', b'0.920503372').replace(assertion, assertion + addition) == current,
         'typed observer changed beyond declared labels and baseline assertions')
    need(protocol['supplemental_gate'] == SUPPLEMENTAL_GATE
         and protocol['original_execution_observer'] in previous['frozen_files'],
         'original observer or separate supplemental gate changed')
    expected = [retained[str(harness / 'benchmarks/traditional_analytics/query_surface' / name)]
                for name in ('worker.py', 'adapters.py')]
    need(protocol['native_sql_entrypoints'] == expected, 'native SQL entrypoints changed')
    return dict(retained_harness_files=len(retained), application_source_differences=len(entries),
                observer_only_changes=['candidate label', 'frozen contract path', 'supplemental baseline metadata', 'explicit baseline assertions'],
                observation_and_supervision_unchanged=True)
