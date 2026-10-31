# SPDX-License-Identifier: Apache-2.0
"""Verify frozen harness continuity and the rejected candidate's source retirement."""
import ast
from pathlib import Path

from native_workflow_protocol import strict_json
from query_surface.replay import need
from shared_performance_replay import linked

APPLICATION_FILES = {'python/src/shardloom/' + name for name in (
    '_batch_function.py', '_connected.py', '_exports.py', '_native_backend.py',
    '_relational_sql.py', 'context.py', 'native.py', 'query.py', 'session.py')}
APPLICATION_HEAD = '7cdb2d643c5915d689f422ea18c108b556ee1f9c'
APPLICATION_MERGE = 'b6883adc1d2a5193eb60f74b0b4c05c3cb75adfa'


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
    driver_path = str(harness.parent / 'tools/performance_cohort_masks.py')
    original = bundle.raw(driver_path, declaration['sources'][driver_path])
    current = bundle.identity(protocol['observer_driver'])

    def execution_tree(raw):
        tree = ast.parse(raw)
        tree.body = [node for node in tree.body if not (
            isinstance(node, ast.FunctionDef) and node.name in {'freeze', 'preserved_application_harness'}
            or isinstance(node, ast.Import) and [item.name for item in node.names] == ['subprocess']
            or isinstance(node, ast.Assign) and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name) and node.targets[0].id in {
                'APPLICATION_HARNESS_FILES', 'PRE_APPLICATION_REVISION', 'ACCEPTED_APPLICATION_REVISION'})]
        return ast.dump(tree, include_attributes=False)

    need(execution_tree(original) == execution_tree(current), 'observation or supervision execution changed')
    need(protocol['original_execution_observer'] in previous['frozen_files'], 'original observer changed')
    entrypoints = protocol['native_sql_entrypoints']
    expected = [retained[str(harness / 'benchmarks/traditional_analytics/query_surface' / name)]
                for name in ('worker.py', 'adapters.py')]
    need(entrypoints == expected, 'native SQL entrypoints changed')
    return dict(retained_harness_files=len(retained), application_source_differences=len(entries),
                observation_and_supervision_unchanged=True)


def application_acceptance(bundle, root, closeout_items):
    hosted = strict_json(bundle.raw(root + '/hosted-application-merged.json'))
    pr = hosted['pull_request']
    checks = pr['statusCheckRollup']
    need(hosted['status'] == 'passed' and hosted['stage'] == 'merged' and pr['number'] == 1628
         and pr['headRefOid'] == APPLICATION_HEAD and pr['mergeCommit']['oid'] == APPLICATION_MERGE
         and pr['state'] == 'MERGED' and hosted['complete_checks'] == len(checks) == 57
         and all(row['status'] == 'COMPLETED' and row['conclusion'] == 'SUCCESS' for row in checks),
         'application hosted prerequisite incomplete')
    closeouts = {Path(item['path']).name: item for item in closeout_items}
    need(set(closeouts) == {'closeout.json', 'validation.json.gz', 'cleanup-plan.json.gz'},
         'application closeout files changed')
    closeout = linked(bundle, closeouts['closeout.json'])
    for key in ('packet', 'cleanup_plan'):
        identity = closeout[key]
        retained = closeouts[identity['file']]
        need(all(retained[field] == identity[field] for field in ('bytes', 'sha256')),
             'application closeout payload changed')
        bundle.identity(retained)
    need(closeout['status'] == 'passed' and closeout['head'] == APPLICATION_HEAD
         and closeout['merge_commit'] == APPLICATION_MERGE and closeout['complete_checks'] == 57
         and closeout['final_installed_tests'] == 283 and closeout['query_executions'] == 0,
         'application closeout status changed')
    return dict(pull_request=1628, complete_checks=57, merge_commit=APPLICATION_MERGE)


def rejection(bundle, proof, runtime, accepted_runtime, comparison):
    need(proof['schema_version'] == 'shardloom.single_mask_rejection.v1' and proof['status'] == 'passed'
         and proof['disposition'] == 'drop_no_complete_engine_gain'
         and proof['accepted_baseline_advanced'] is False and proof['source_restored_to'] == APPLICATION_MERGE
         and proof['retained_runtime'] == 'application-batch-release-r1'
         and proof['retained_performance_baseline'] == 'bounded-ordering-release-r2'
         and proof['unrelated_files_changed'] is False and proof['query_executions'] == 0,
         'rejection disposition changed')
    need(comparison['benefit_gate_passed'] is False
         and all(proof[key] == comparison[key] for key in ('complete_comparisons', 'complete_rows',
             'baseline_engine_seconds', 'candidate_engine_seconds')), 'rejection clocks or coverage changed')
    rows = proof['candidate_source_retained']
    need(len(rows) == len({row['path'] for row in rows}) == 8, 'rejected source inventory changed')
    restored, removed = 0, set()
    for row in rows:
        path, candidate = row['path'], row['candidate']
        need({key: candidate[key] for key in ('bytes', 'sha256')} == runtime['source_snapshot'][path],
             'rejected source differs from measured runtime')
        bundle.identity(candidate)
        if row['restored'] is None:
            removed.add(path)
            need(path not in accepted_runtime['source_snapshot'], 'removed file was accepted source')
        else:
            restored += 1
            original = row['restored']
            need({key: original[key] for key in ('bytes', 'sha256')} == accepted_runtime['source_snapshot'][path],
                 'restored source differs from accepted runtime')
            bundle.identity(original)
    need(restored == 6 and removed == {
        'shardloom-vortex/src/local_primitives/native_relational_predicate_single_tests.rs',
        'python/tests/test_native_single_predicate_mask_binding.py'}, 'rejected file retirement changed')
    patch = bundle.identity(proof['candidate_patch'])
    need(patch.count(b'diff --git ') == 8, 'rejected runtime patch incomplete')
    return dict(runtime_changes_retained=False, accepted_baseline_advanced=False,
                restored_source_files=restored, removed_candidate_files=len(removed))
