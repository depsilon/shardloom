# SPDX-License-Identifier: Apache-2.0
"""Accept saved aggregate/order improvements and their separate mechanism checks."""
import ast
from pathlib import Path

from native_workflow_protocol import strict_json
from parquet_demand_completion_replay import replay as replay_cohort
from query_surface.evidence import no_fallback
from query_surface.replay import contract, need, sha
from query_surface.polars_adapter import compare_result as compare_polars_result
from query_surface.semantics import compare, result_from_record
from shared_performance_replay import complete_gate, linked, public_comparisons, test_totals
from shared_followthrough_join_replay import replay_log
from shared_followthrough_materialized_replay import replay as replay_materialized

BUILD = 'build --locked --offline --release --features release-user-surfaces -p shardloom-cli -p shardloom-python'
GATE_SETS = {
    'mixed-aggregate-release-r1': {
        'p161-mixed-aggregate-final-native-clippy.json': 'clippy --locked --offline --workspace --all-targets --features release-user-surfaces -- -D warnings',
        'p162-mixed-aggregate-native-workspace.json': 'test --locked --offline --workspace --all-targets --features release-user-surfaces --profile ci-native',
        'p163-mixed-aggregate-default-clippy.json': 'clippy --locked --offline --workspace --all-targets -- -D warnings',
        'p164-mixed-aggregate-default-workspace.json': 'test --locked --offline --workspace --all-targets',
        'p165-mixed-aggregate-format.json': 'fmt --all -- --check',
    },
    'bounded-ordering-release-r1': {
        'p182-bounded-ordering-clippy.json': 'clippy --locked --offline --workspace --all-targets --features release-user-surfaces -- -D warnings',
        'p186-bounded-ordering-native-workspace.json': 'test --locked --offline --workspace --all-targets --features release-user-surfaces --profile ci-native',
        'p187-bounded-ordering-default-clippy.json': 'clippy --locked --offline --workspace --all-targets -- -D warnings',
        'p188-bounded-ordering-default-workspace.json': 'test --locked --offline --workspace --all-targets',
        'p189-bounded-ordering-fmt.json': 'fmt --all -- --check',
    },
}
TEST_TOTALS = {
    'p162-mixed-aggregate-native-workspace.json': (119, 6551, 0, 35),
    'p164-mixed-aggregate-default-workspace.json': (107, 3289, 0, 0),
    'p186-bounded-ordering-native-workspace.json': (119, 6558, 0, 35),
    'p188-bounded-ordering-default-workspace.json': (107, 3289, 0, 0),
}


def frozen_files(bundle, items, external):
    for item in items:
        size = item.get('bytes', item.get('generation', {}).get('bytes'))
        if (item['path'], item['sha256'], size) in external:
            path = Path(item['path'])
            need(path.suffix in {'.parquet', '.arrow', '.vortex', '.so', '.dylib', '.whl'}
                 or path.name == 'shardloom', 'nonpayload evidence cannot be external')
        else:
            bundle.identity(item)


def source_matches(gate, runtime):
    for relative, item in gate['changed_source_inventory'].items():
        if relative.endswith(('.rs', '.py', '.toml', '.lock')) and 'sha256' in item:
            need(runtime['source_snapshot'].get(relative) == item, 'tested source differs from build: ' + relative)


def harness_compatibility(bundle, proof):
    need(proof['status'] == 'passed' and proof['query_executions'] == 0
         and proof['controls_passed'] == 6 and len(set(proof['rejected_controls'])) == 6
         and proof['query_execution_supervision_replay_ast_unchanged'] is True, 'harness controls incomplete')
    old_driver, new_driver = (bundle.identity(proof[name]) for name in ('original_driver', 'current_driver'))

    def execution_tree(data):
        tree = ast.parse(data)
        tree.body = [node for node in tree.body if not (isinstance(node, ast.FunctionDef)
                     and node.name in {'freeze', 'preserved_reporting_harness'})]
        return ast.dump(tree, include_attributes=False)

    need(execution_tree(old_driver) == execution_tree(new_driver), 'harness execution/supervision changed')
    delta = proof['exact_reporting_delta']
    need(delta['path'] == 'scripts/run_clickbench_query_uat.py'
         and delta['execution_change'] is False and delta['supervision_change'] is False, 'wrong reporting exception')
    old, new = (bundle.identity(delta[name]) for name in ('executed_frozen_harness', 'current_repository_source'))
    before = b'    "local_primitive_", "native_source_demand_", "resident_", "relational_",\n'
    after = (b'    "local_primitive_", "native_source_demand_", "native_source_ranked_",\n'
             b'    "native_input_ordering_", "resident_", "relational_",\n')
    need(old.count(before) == 1 and old.replace(before, after, 1) == new, 'reporting delta is not exact')


def fixture_correction(bundle, proof, before, after, failed):
    fixture = 'python/tests/test_native_bounded_ordering_binding.py'
    changed = [name for name in before['source_snapshot'] if before['source_snapshot'][name] != after['source_snapshot'].get(name)]
    need(set(before['source_snapshot']) == set(after['source_snapshot']) and changed == [fixture]
         and before['files'] == after['files'] and before['products'] == after['products'],
         'fixture correction changed runtime source/products')
    for key, snapshot in (('original_fixture', before['source_snapshot']), ('corrected_fixture', after['source_snapshot'])):
        item = proof[key]
        need({field: item[field] for field in ('bytes', 'sha256')} == snapshot[fixture], 'fixture identity changed')
        bundle.identity(item)
    need(bundle.identity(proof['retained_original']) == bundle.identity(proof['original_fixture']), 'original fixture lost')
    need(linked(bundle, proof['failed_public_report']) == failed and proof['status'] == 'passed'
         and proof['failed_before_execution'] == failed['errors'] == 42
         and proof['completed_comparisons_retained'] == failed['positive_public_executions'] == 318
         and failed['failures'] == 0 and failed['posthash_verified'] is True
         and proof['runtime_source_changes'] == [] and proof['query_executions'] == 0
         and proof['frozen_347_changed'] is False and proof['benchmark_execution'] is False,
         'fixture correction credited invalid execution')
    error = 'query_surface.adapters.AdapterGap: DataFrame sort adapter requires a shared direction and null placement'
    need(len(failed['failure_details']) == 42
         and all(row['detail'].splitlines()[-1] == error for row in failed['failure_details']),
         'fixture correction hides another failure')


def installed_package(bundle, path, runtime, count):
    package = strict_json(bundle.raw(path + '/report.json'))
    integration = strict_json(bundle.raw(path + '/integration.json'))
    need(integration == dict(tests=count, failures=0, errors=0, skipped=0, status='passed')
         and package['integration'] == integration and package['status'] == 'passed'
         and package['uninstall_status'] == 'passed' and package['installed_load']['isolated_install'] is True
         and package['installation_performed'] is True and package['publication_attempted'] is False,
         'installed-package acceptance incomplete')
    need(package['integrity']['cli_sha256'] == runtime['products']['shardloom']
         and package['integrity']['native_library_sha256'] == runtime['products']['_native.abi3.so'],
         'installed-package runtime differs')
    need([step['name'] for step in package['steps']] == ['build', 'create_environment', 'install_optional_consumer',
         'install', 'load', 'integration', 'uninstall', 'verify_uninstalled'], 'installed-package lifecycle changed')
    for step in package['steps']:
        need(step['returncode'] == 0 and sha(bundle.raw(path + '/' + step['log'])) == step['log_sha256'],
             'installed-package log/result changed')
    return integration


def failed_public_values(value):
    """Retain valid completed comparisons without accepting an incomplete grid."""
    need(value['status'] == 'failed' and value['errors'] > 0 and value['posthash_verified'] is True
         and value['comparison_engine_invoked'] is False, 'failed public outcome changed')
    rows = 0
    for item in value['complete_comparisons']:
        compare(result_from_record(item['expected']), result_from_record(item['actual']), contract(item['contract']))
        need(item['report']['complete_consumption'] is True and item['report']['surface'] == item['surface']
             and item['rows'] == len(item['actual']['rows']), 'failed-attempt completed comparison changed')
        no_fallback(item['report']['fields'])
        rows += item['rows']
    need(rows == value['compared_rows'] and len(value['complete_comparisons']) == value['positive_public_executions'],
         'failed-attempt completed coverage changed')


def diagnostic_observations(bundle, root, supplements):
    predicate = supplements['predicate-followthrough-observations.json']
    cases = {row['id']: row for row in strict_json(bundle.raw(root + '/broad-mixed-aggregate-r1/corpus.json'))}
    wanted = {key for key, row in cases.items() if row['family'] in {'predicates', 'global_aggregation', 'nullable'}}
    need(predicate['status'] == 'COMPLETE' and predicate['case_count'] == 91
         and {row['case_id'] for row in predicate['cases']} == wanted and len(predicate['cases']) == len(wanted),
         'predicate diagnostic coverage changed')
    losses = {'predicates': [], 'global_aggregation': []}
    for row in predicate['cases']:
        native, polars = row['current_native'], row['original_polars']
        current = bundle.raw(native['stdout_path'])
        original = bundle.raw(polars['evidence_path'])
        need(sha(current) == native['stdout_sha256_recorded'] and sha(original) == polars['sha256_recorded'],
             'diagnostic output identity changed')
        current, original = strict_json(current), strict_json(original)
        need(current['case_id'] == original['case_id'] == row['case_id']
             and current['case_identity'] == original['case_identity'] == row['case_identity']
             and current['status'] == original['status'] == 'passed', 'diagnostic case identity changed')
        declaration = cases[row['case_id']]
        compare_polars_result(result_from_record(current['actual']), result_from_record(original['actual']), contract(declaration['contract']))
        measured, observed = current['observation'], original['observation']
        need(current['comparison_engine_invoked'] is False and original['comparison_engine_invoked'] is True
             and observed['execution_engine'] == 'polars' and observed['input_format'] == 'original_arrow_ipc'
             and observed['complete_consumption'] is True and observed['clocks']['engine_execution_seconds'] is None
             and observed['version'] == polars['version'], 'original comparator boundary changed')
        need(measured['clocks']['complete_workflow_seconds'] == native['complete_workflow_seconds']
             and measured['clocks']['engine_execution_seconds'] == native['engine_execution_seconds']
             and observed['clocks']['complete_workflow_seconds'] == polars['complete_workflow_seconds'],
             'diagnostic clock changed')
        need(all(measured['fields'].get(key) == value for key, value in native['fields'].items()),
             'diagnostic work counters changed')
        delta = native['complete_workflow_seconds'] - polars['complete_workflow_seconds']
        need(row['comparison']['native_minus_polars_seconds'] == delta
             and row['comparison']['native_slower'] is (delta > 0)
             and row['comparison']['format_mismatch'] is True
             and row['comparison']['performance_claim_eligible'] is False, 'diagnostic comparison changed')
        if delta > 0 and row['family'] in losses:
            losses[row['family']].append(row['case_id'])
    need(losses['predicates'] == predicate['predicate_losses']
         and losses['global_aggregation'] == predicate['global_aggregation_losses'], 'diagnostic loss inventory changed')
    topk = supplements['topk-followthrough-retained-observations.json']
    need(topk['status'] == 'verified' and topk['ordering_cases'] == len(topk['cases']) == 10, 'ordering diagnostic incomplete')
    need({row['case_id'] for row in topk['cases']} == {key for key, row in cases.items() if row['family'] == 'ordering'},
         'ordering diagnostic case set changed')
    for row in topk['cases']:
        data = bundle.raw(row['stdout'])
        need(sha(data) == row['stdout_sha256'], 'ordering diagnostic identity changed')
        outcome = strict_json(data)
        need(outcome['case_id'] == row['case_id'] and outcome['status'] == 'passed'
             and outcome['observation']['clocks']['engine_execution_seconds'] == row['engine_seconds']
             and outcome['actual']['schema'] == row['schema']
             and all(outcome['observation']['fields'].get(key) == value for key, value in row['fields'].items()),
             'ordering diagnostic counters changed')
    return dict(predicate_cases=91, ordering_cases=10, predicate_losses=losses['predicates'],
                global_aggregation_losses=losses['global_aggregation'],
                scope='historical mixed-format API-clock attribution; no engine-clock comparator claim')


def verify(bundle, anchors):
    evidence = bundle.index['evidence']
    need(evidence == anchors, 'follow-through evidence anchors changed')
    root = evidence['root']
    external = {(item['path'], item['sha256'], item['bytes']) for item in evidence['external_artifacts']}
    need(len(external) == len(evidence['external_artifacts']), 'duplicate external identity')
    gates, failures = {}, []
    for item in evidence['gates']:
        name = Path(item['path']).name
        need(name not in gates, 'duplicate process gate')
        gate = gates[name] = linked(bundle, item)
        need(gate['status'] == item['status'] and (gate['returncode'] == 0) == (gate['status'] == 'passed'),
             'historical process outcome changed')
        need(sha(bundle.raw(root + '/' + name[:-5] + '.log')) == gate['log_sha256'], 'historical process log changed')
        if gate['status'] != 'passed':
            failures.append(name)
        for relative, source in gate['changed_source_inventory'].items():
            if 'sha256' in source:
                bundle.identity(dict(path=gate['cwd'] + '/' + relative, **source))
        need(sha(bundle.raw(root + '/source-blobs/' + gate['driver_sha256'])) == gate['driver_sha256'],
             'historical process driver changed')
    runtimes = {}
    for item in evidence['runtimes']:
        name = Path(item['path']).parent.name
        need(name not in runtimes, 'duplicate runtime')
        runtime = runtimes[name] = linked(bundle, item)
        build = gates[runtime['build_receipt']]
        complete_gate(build)
        need(build['command'] == ['cargo', *BUILD.split()] and runtime['profile'] == 'release'
             and runtime['features'] == ['release-user-surfaces'] and runtime['source_revision'] == build['head']
             and sha(bundle.raw(root + '/' + runtime['build_receipt'])) == runtime['build_receipt_sha256']
             and set(runtime['files']) == {'shardloom', '_native.abi3.so'}
             and runtime['products'] == {key: value['sha256'] for key, value in runtime['files'].items()}
             and len(runtime['source_snapshot']) > 3000, 'runtime/build binding changed')
        bundle.identity(dict(path=root + '/' + name + '/source-overlay.patch', **runtime['source_overlay']))
        for relative, source in runtime['source_snapshot'].items():
            bundle.identity(dict(path=build['cwd'] + '/' + relative, **source))
        source_matches(build, runtime)
    totals = {}
    for runtime_name, commands in GATE_SETS.items():
        for name, command in commands.items():
            gate = gates[name]
            complete_gate(gate)
            need(gate['command'] == ['cargo', *command.split()], 'required gate command changed')
            source_matches(gate, runtimes[runtime_name])
            if name in TEST_TOTALS:
                totals[name] = test_totals(bundle.raw(root + '/' + name[:-5] + '.log').decode(), TEST_TOTALS[name])
    publics, public_values = {}, {}
    for item in evidence['public_reports']:
        name = Path(item['path']).name
        value = public_values[name] = linked(bundle, item)
        publics[name] = public_comparisons(value)
        if value['status'] == 'failed':
            failed_public_values(value)
        runtime = runtimes[value['runtime']]
        need(value['build_receipt'] == runtime['build_receipt']
             and value['build_receipt_sha256'] == runtime['build_receipt_sha256'], 'public build binding changed')
        for product, identity in runtime['files'].items():
            need(value['native_product_and_source_identity'][root + '/' + value['runtime'] + '/' + product] == identity,
                 'public product changed')
    need(publics['mixed-aggregate-public-r1.json'] == dict(status='passed', accepted_executions=360, complete_rows=106875)
         and publics['bounded-ordering-public-r2.json'] == dict(status='passed', accepted_executions=360, complete_rows=45195),
         'fresh public acceptance grid changed')
    supplements = {Path(item['path']).name: linked(bundle, item) for item in evidence['supplements']
                   if item['path'].endswith('.json')}
    harness_compatibility(bundle, supplements['mixed-aggregate-harness-compatibility.json'])
    fixture_correction(bundle, supplements['bounded-ordering-public-fixture-correction.json'],
        runtimes['bounded-ordering-release-r1'], runtimes['bounded-ordering-release-r2'],
        public_values['bounded-ordering-public-r1.json'])
    packages = {
        name: installed_package(bundle, root + '/' + name, runtimes[runtime], count)
        for name, runtime, count in [('installed-mixed-aggregate-r1', 'mixed-aggregate-release-r1', 265),
                                     ('installed-bounded-ordering-r1', 'bounded-ordering-release-r2', 266)]}
    comparisons = {}
    for row in evidence['cohorts']:
        name = row['name']
        need(name not in comparisons, 'duplicate cohort')
        inputs, protocol = linked(bundle, row['inputs']), linked(bundle, row['protocol'])
        frozen_files(bundle, inputs['frozen_files'], external)
        frozen_files(bundle, protocol['prerequisites'], external)
        frozen_files(bundle, [protocol[key] for key in ('original_execution_observer', 'original_inputs',
                     'source_contract', 'observer_driver')], external)
        runtime = runtimes[Path(inputs['runtime_manifest']['path']).parent.name]
        need(linked(bundle, inputs['runtime_manifest']) == runtime,
             'cohort runtime manifest changed')
        for product in inputs['runtime']:
            need(runtime['files'][Path(product['path']).name] == dict(bytes=product['generation']['bytes'],
                 sha256=product['sha256']), 'cohort product changed')
        versions = {item['path']: item['sha256'] for item in inputs['frozen_files']}
        raw = lambda path: bundle.raw(path, versions.get(path))
        result = replay_cohort(raw, root + '/' + name)
        saved = strict_json(raw(root + '/' + name + '/disposition.json'))
        need(all(saved[key] == value for key, value in result.items())
             and saved['accepted_baseline'] is True and saved['decision'] == 'retain_lower_complete_engine_total'
             and saved['role'] == inputs['role'] == 'candidate' and result['benefit_gate_passed'] is True,
             'cohort disposition changed')
        comparisons[name] = dict(complete_queries=result['complete_comparisons'], complete_rows=result['complete_rows'],
            engine_seconds=result['candidate_engine_seconds'], baseline_engine_seconds=result['baseline_engine_seconds'],
            case_flags=result['material_case_regressions'], family_flags=result['material_family_regressions'])
    need(set(comparisons) == {'broad-mixed-aggregate-r1', 'broad-bounded-ordering-r1'}
         and comparisons['broad-bounded-ordering-r1']['baseline_engine_seconds'] == comparisons['broad-mixed-aggregate-r1']['engine_seconds'],
         'accepted baseline chain changed')
    previous = linked(bundle, evidence['previous_acceptance'])
    previous_manifest = linked(bundle, evidence['previous_packet_manifest'])
    need(previous['status'] == 'passed' and previous['archived_code_executed'] is False
         and previous_manifest['schema_version'] == 'shardloom.shared_performance_bundle.v1'
         and previous['cohorts']['broad-review-integration-r1']['engine_seconds'] == comparisons['broad-mixed-aggregate-r1']['baseline_engine_seconds'],
         'prior accepted baseline binding changed')
    runtime = dict(path=root + '/mixed-aggregate-release-r1', **runtimes['mixed-aggregate-release-r1'])
    mechanisms = {row['name']: replay_materialized(bundle, row, runtime, external, frozen_files)
                  for row in evidence['mechanisms']}
    join = supplements['join-movement-measurements.json']
    need(join['status'] == 'passed' and join['complete_cases'] == 16 and join['implementation_changed'] is False
         and join['timed_query_executions'] == 0 and join['performance_gain_claim'] is False
         and join['comparison_engine_invoked'] is False, 'join diagnostic gained performance credit')
    for item in join['gates'].values():
        gate = linked(bundle, item['receipt'])
        complete_gate(gate)
        need(sha(bundle.identity(item['log'])) == gate['log_sha256'], 'join measurement log changed')
    test_totals(bundle.raw(root + '/p206-join-movement-final-focused.log').decode(), (1, 2, 0, 0))
    test_totals(bundle.raw(root + '/p209-join-movement-production-composition.log').decode(), (1, 10, 0, 0))
    need(replay_log(bundle.raw(root + '/p206-join-movement-final-focused.log')) == join['measurements'],
         'join measurements differ from independent replay')
    diagnostics = diagnostic_observations(bundle, root, supplements)
    return dict(schema_version='shardloom.shared_performance_followthrough_acceptance.v1', status='passed',
        cohorts=comparisons, workspace_tests=totals, public_reports=publics, installed_packages=packages,
        materialized_reuse=mechanisms, join_measurement_cases=16, production_join_tests=10, diagnostics=diagnostics,
        runtime_files=runtimes['bounded-ordering-release-r2']['files'],
        retained_process_receipts=len(gates), historical_failures=failures,
        archived_code_executed=False, new_query_executions=0, full43_executed=False,
        limitations=['Single observations; no statistical confidence or new competitor comparisons.',
            'Source/runtime/wheel payload identities are retained; no regeneration or execution occurs.',
            'Materialization and join diagnostics remain separate from the unchanged 347-query decisions.',
            'Full43 remains held until item18 universal I/O acceptance; prior Full43 evidence is unchanged.',
            'Content hashes prove internal consistency, not signed provenance.'])
