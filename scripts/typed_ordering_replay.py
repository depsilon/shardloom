# SPDX-License-Identifier: Apache-2.0
"""Verify saved typed-ordering correctness before any performance disposition."""
from pathlib import Path

from native_workflow_protocol import strict_json
from ordered_input_public_replay import public_replay
from ordered_input_replay import CONTROLS, RECOVERY_CONTROLS, FAMILIES, combined_disposition
from ordered_input_workflow_replay import workflow_replay as baseline_workflow_replay
from parquet_demand_completion_replay import replay as broad_replay
from typed_ordering_cohort_replay import harness_continuity
from typed_ordering_workflow_replay import workflow_replay
from query_surface.replay import need, sha
from shared_followthrough_replay import frozen_files, installed_package, source_matches
from shared_performance_replay import complete_gate, linked, test_totals

BUILD = 'build --locked --offline --release --features release-user-surfaces -p shardloom-cli -p shardloom-python'
REQUIRED = {
    't004-numeric-order-native-clippy.json': 'clippy --locked --offline --workspace --all-targets --features release-user-surfaces -- -D warnings',
    't005-numeric-order-native-workspace.json': 'test --locked --offline --workspace --all-targets --features release-user-surfaces --profile ci-native',
    't006-numeric-order-default-clippy.json': 'clippy --locked --offline --workspace --all-targets -- -D warnings',
    't007-numeric-order-default-workspace.json': 'test --locked --offline --workspace --all-targets',
    't008-numeric-order-fmt.json': 'fmt --all -- --check',
    't010-numeric-order-release-build.json': BUILD,
}
FAILURES = ['t003-numeric-order-native-clippy.json', 't022-numeric-order-complete-acceptance.json']
NUMERIC_CONTROLS = (
    'numeric_order_comparisons_remove_scalar_cells_without_a_memory_grant',
    'numeric_order_declines_changed_domains_and_preserves_bounds_and_errors',
    'numeric_order_without_validation_keeps_nonfinite_first_errors',
)
PREFIX_CONTROLS = (
    'numeric_prefix_preserves_original_integer_widths_nulls_ties_and_endpoints',
    'numeric_prefix_preserves_analytical_float_identity_and_both_policies',
    'numeric_prefix_reuses_prepared_encoded_numeric_owners',
    'numeric_prefix_declines_mixed_domains_and_text_without_changing_general_errors',
    'numeric_prefix_optional_memory_decline_and_cancel_preserve_state_and_credits',
    'numeric_prefix_uses_parent_validity_and_avoids_generic_row_locations',
    'numeric_prefix_declines_large_prefix_without_allocating_or_mutating_ordinals',
)


def gates_replay(bundle, evidence):
    gates, failures = {}, []
    root = evidence['root']
    for item in evidence['gates']:
        name = Path(item['path']).name
        need(name not in gates, 'duplicate gate')
        gate = gates[name] = linked(bundle, item)
        need(gate['status'] == item['status'] and (gate['returncode'] == 0) == (gate['status'] == 'passed')
             and sha(bundle.raw(root + '/' + name[:-5] + '.log')) == gate['log_sha256'],
             'gate log/result changed')
        need(gate['process_group_drained'] is True and gate['direct_child_reaped'] is True
             and gate['timed_out'] is False and gate['interrupted_signal'] is None,
             'gate did not finish under original supervision')
        if gate['status'] == 'passed':
            complete_gate(gate)
        else:
            failures.append(name)
        for relative, source in gate['changed_source_inventory'].items():
            if 'sha256' in source:
                bundle.identity(dict(path=gate['cwd'] + '/' + relative, **source))
        need(sha(bundle.raw(root + '/source-blobs/' + gate['driver_sha256'])) == gate['driver_sha256'],
             'executed gate supervisor changed')
    need(failures == FAILURES and set(REQUIRED) <= gates.keys(), 'gate inventory changed')
    return gates


def runtime_replay(bundle, evidence, gates):
    runtime = linked(bundle, evidence['runtime'])
    build = gates[runtime['build_receipt']]
    root = evidence['root']
    need(runtime['build_receipt'] == 't010-numeric-order-release-build.json'
         and runtime['source_revision'] == build['head']
         and runtime['build_receipt_sha256'] == sha(bundle.raw(root + '/' + runtime['build_receipt']))
         and runtime['profile'] == 'release' and runtime['features'] == ['release-user-surfaces']
         and set(runtime['files']) == {'shardloom', '_native.abi3.so'}
         and runtime['products'] == {name: item['sha256'] for name, item in runtime['files'].items()},
         'frozen release provenance changed')
    folder = str(Path(evidence['runtime']['path']).parent)
    bundle.identity(dict(path=folder + '/source-overlay.patch', **runtime['source_overlay']))
    need(len(runtime['source_snapshot']) == 3198, 'incomplete build source inventory')
    for relative, item in runtime['source_snapshot'].items():
        bundle.identity(dict(path=build['cwd'] + '/' + relative, **item))
    for name, command in REQUIRED.items():
        need(gates[name]['command'] == ['cargo', *command.split()], 'validation command changed')
        source_matches(gates[name], runtime)
    for name in ('t009-numeric-order-python-source.json', 't011-numeric-order-runtime-freeze.json',
                 't012-numeric-order-public-declaration.json', 't013-numeric-order-public-results.json',
                 't014-numeric-order-installed-package.json', 't015-numeric-order-assembly.json',
                 't016-numeric-order-assembly-review.json'):
        source_matches(gates[name], runtime)
    # Keep early focused observations but require the full final workspace for
    # acceptance. The sole failed strict check is exactly a documentation fix.
    failed = gates[FAILURES[0]]
    need(failed['returncode'] == 101, 'original lint failure changed')
    changed = []
    for relative, item in failed['changed_source_inventory'].items():
        if 'sha256' in item and item != runtime['source_snapshot'].get(relative):
            changed.append(relative)
    relative = 'shardloom-vortex/src/local_primitives/native_relational_keys.rs'
    need(changed == [relative], 'lint correction changed other source')
    path = failed['cwd'] + '/' + relative
    before = bundle.identity(dict(path=path, **failed['changed_source_inventory'][relative]))
    after = bundle.identity(dict(path=path, **runtime['source_snapshot'][relative]))
    old = b'Other domains keep compare_at.'
    need(before.count(old) == 1 and before.replace(old, b'Other domains keep `compare_at`.', 1) == after,
         'lint correction was not documentation-only')
    failed_log = bundle.raw(root + '/t003-numeric-order-native-clippy.log').decode()
    need('error: item in documentation is missing backticks' in failed_log
         and 'due to 1 previous error' in failed_log, 'original lint diagnosis lost')
    return runtime


def controls(log):
    names = list(CONTROLS)
    names += ['local_primitives::native_relational_keys::numeric_order_tests::' + name
              for name in NUMERIC_CONTROLS]
    names += ['local_primitives::native_relational_sort::tests::numeric_tests::' + name
              for name in PREFIX_CONTROLS]
    names += ['local_primitives::native_relational_sort::tests::native_relational_sort_resolves_each_row_once_for_tied_multikey_comparisons']
    lines = log.splitlines()
    for name in names:
        need(lines.count('test ' + name + ' ... ok') == 1, 'missing final native control: ' + name)
    for name in RECOVERY_CONTROLS:
        need(sum(line.endswith('::' + name + ' ... ok') for line in lines) == 1,
             'missing final pressure/recovery control: ' + name)
    return dict(passed_controls=len(names), names=names, recovery_controls=len(RECOVERY_CONTROLS))


def assembly_replay(bundle, evidence, runtime):
    folder = evidence['root'] + '/typed-ordering-assembly-r1'
    capture = strict_json(bundle.raw(folder + '/manifest.json'))
    review = linked(bundle, evidence['assembly_review'])
    need(capture['status'] == 'captured_for_manual_review' and capture['queries_executed'] is False
         and capture['debug_observation_symbols_absent'] is True
         and capture['binary_sha256'] == runtime['products']['_native.abi3.so']
         and capture['runtime_manifest_sha256'] == evidence['runtime']['sha256']
         and review['capture_sha256'] == sha(bundle.raw(folder + '/manifest.json'))
         and review['binary_sha256'] == capture['binary_sha256']
         and review['runtime_manifest_sha256'] == capture['runtime_manifest_sha256']
         and review['status'] == 'passed' and review['query_executions'] == 0,
         'optimized code review binding changed')
    bundle.identity(capture['helper'])
    bundle.identity(review['helper'])
    owners = {'typed_view', 'typed_comparison', 'typed_null', 'comparison_admission',
        'original_comparison', 'sort_owner', 'sort_comparison', 'window_owner',
        'window_comparison', 'asof_owner', 'asof_comparison', 'collection_owner'}
    need(len(capture['records']) == len(owners)
         and {row['owner'] for row in capture['records']} == owners
         and review['reviewed_symbols'] == [row['symbol'] for row in capture['records']],
         'optimized symbol coverage changed')
    for row in capture['records']:
        data = bundle.identity(dict(path=folder + '/' + row['file'], bytes=row['bytes'], sha256=row['sha256']))
        need(row['symbol'].encode() in data and data.count(b'\n') == row['lines']
             and all(address.encode() in data for address in review['anchors'][row['owner']]),
             'reviewed machine code changed')
    for field in ('admitted_numeric_path_avoids_scalar_cells', 'admitted_numeric_path_adds_no_allocation',
                  'original_comparator_retained_on_decline', 'debug_observation_symbols_absent',
                  'integer_width_and_signedness_preserved', 'floating_normalization_preserved'):
        need(review[field] is True, 'optimized mechanism review incomplete')
    need(review['owners_covered'] == ['full_sort', 'window_ordering', 'asof_build_ordering', 'ordered_collections'],
         'ordering owner review changed')
    return dict(symbols=12, debug_observation_symbols_absent=True, owners=review['owners_covered'])


def baseline_binding(bundle, evidence):
    binding = linked(bundle, evidence['baseline_binding'])
    need(binding['status'] == 'passed' and binding['query_executions'] == 0
         and binding['saved_observations_byte_exact'] is True
         and binding['original_protocol_and_observations_unchanged'] is True
         and binding['complete347_engine_seconds'] == 11.979219492
         and binding['supplemental48_engine_seconds'] == 0.920503372,
         'accepted baseline binding changed')
    bundle.identity(binding['helper'])
    expected = {'manifest.json': '3962bd8db9aaa5cbe8b24556e76c195acb35ba7305c1949799c3c0f362596312',
                'acceptance.json': '35e7ceec8267cc1d41ab81282e10f507874086f20bdc892d64bb2944fa5351e2'}
    for name, digest in expected.items():
        need(binding['packet'][name]['sha256'] == digest, 'accepted packet anchor changed')
        bundle.identity(binding['packet'][name])
    manifest = linked(bundle, binding['packet']['manifest.json'])
    need(binding['observations'] == evidence['baseline_observations']
         and len(binding['observations']) == 6, 'accepted observation inventory changed')
    for path, item in binding['observations'].items():
        data = bundle.identity(item)
        need(path == item['path'] and sha(data) in manifest['path_map'][path],
             'accepted observation differs from immutable packet')
    return dict(complete347_engine_seconds=11.979219492, supplemental48_engine_seconds=0.920503372,
                accepted_observation_files=6, baseline_reexecuted=False)


def driver_revision(bundle, evidence):
    root = evidence['root']
    proof = strict_json(bundle.raw(root + '/typed-ordering-driver-review-r2.json'))
    need(proof['status'] == 'passed' and proof['execution_and_supervision_ast_unchanged'] is True
         and proof['candidate_queries_executed'] == proof['baseline_queries_reexecuted'] == 0,
         'prospective driver revision changed query execution')
    import ast

    def without_freeze(data):
        tree = ast.parse(data)
        tree.body = [node for node in tree.body
                     if not isinstance(node, ast.FunctionDef) or node.name != 'freeze']
        return ast.dump(tree, include_attributes=False)

    before, after = (bundle.identity(proof[key]) for key in ('original_driver', 'current_driver'))
    need(without_freeze(before) == without_freeze(after), 'observer/worker/replay changed at hosted-state revision')
    bundle.identity(proof['original_review'])
    local = linked(bundle, proof['local_acceptance'])
    need(local['status'] == 'passed' and local['disposition'] == 'local_correctness_only_performance_pending',
         'local correctness was not complete before timing')
    hosted = linked(bundle, proof['hosted_state'])
    need(hosted['status'] == 'recorded_before_candidate_execution'
         and hosted['merge_still_required_for_hosted_closeout'] is True,
         'hosted integration requirement lost')
    sources = strict_json(bundle.raw(root + '/typed-ordering-local-verifier-sources.json'))
    need(sources['status'] == 'passed' and sources['gate'] == 't017-numeric-order-local-acceptance.json'
         and sources['query_executions'] == 0 and len(sources['sources']) == 3,
         'initial local verifier inventory changed')
    for item in sources['sources']:
        need(bundle.identity(item) == bundle.raw(item['snapshot'], item['sha256']),
             'initial local verifier bytes lost')
    correction = strict_json(bundle.raw(root + '/typed-ordering-verifier-fixture-correction.json'))
    before, after = (bundle.identity(correction[key]) for key in ('before', 'after'))
    old = b"source = Path(__file__).resolve().parents[1] / 'python/tests/native_ordered_input_cases.py'"
    need(correction['status'] == 'passed' and correction['query_executions'] == 0
         and correction['runtime_source_changed'] is False
         and before.count(old) == 1 and before.replace(old, b'source = Path(fixture.__file__).resolve()', 1) == after,
         'saved-replay fixture resolution correction changed semantics')
    failed_log = bundle.raw(root + '/t022-numeric-order-complete-acceptance.log').decode()
    need('FileNotFoundError' in failed_log
         and '/python/tests/native_ordered_input_cases.py' in failed_log,
         'original saved-replay failure missing')
    return dict(revision_before_candidate_execution=True, query_execution_unchanged=True,
                supervision_unchanged=True, hosted_merge_not_yet_claimed=True)


def performance_replay(bundle, evidence, runtime, external, summary):
    root = evidence['root']
    supplemental = evidence['supplemental_cohort']
    if supplemental is None:
        need(evidence['cohort'] is None, 'missing supplemental candidate')
        return dict(**summary, disposition='local_correctness_only_performance_pending',
                    accepted_baseline_advanced=False)
    inputs = linked(bundle, supplemental['inputs'])
    protocol = linked(bundle, supplemental['protocol'])
    contract = '3e9fc4c4aa0771859a904a5271330910da17f4ad48034930ee68746c01333bc7'
    need(linked(bundle, inputs['runtime_manifest']) == runtime
         and protocol['source_contract']['sha256'] == contract
         and linked(bundle, inputs['candidate_protocol']) == protocol,
         'supplemental candidate build or prospective contract changed')
    revised_driver = driver_revision(bundle, evidence)
    baseline = baseline_workflow_replay(bundle, root + '/ordered-input-candidate-r1', external)
    need(baseline['engine_execution_seconds'] == 0.920503372, 'accepted supplemental clock changed')
    result48 = workflow_replay(bundle, root + '/' + supplemental['name'], external)
    if evidence['cohort'] is None:
        return dict(**summary, supplemental_baseline=baseline, supplemental_comparison=result48,
            disposition='supplemental_complete_broad_performance_pending', accepted_baseline_advanced=False)
    cohort = evidence['cohort']
    inputs, protocol = linked(bundle, cohort['inputs']), linked(bundle, cohort['protocol'])
    frozen_files(bundle, inputs['frozen_files'] + protocol['prerequisites'], external)
    need(linked(bundle, inputs['runtime_manifest']) == runtime and inputs['repeats'] == 1
         and inputs['memory_bytes'] == 2 << 30 and inputs['parallelism'] == 2
         and protocol['performance_executions_before_freeze'] == 0
         and protocol['source_contract']['sha256'] == contract,
         'broad candidate resources or prospective contract changed')
    need(protocol['retention_decision'] == dict(rule='strict_complete_cohort_gain',
        declared_families=FAMILIES, material_case_regression=dict(fraction=0.20, absolute_seconds=0.010),
        material_family_regression=dict(fraction=0.20, absolute_seconds=0.050)),
        'binary complete-cohort rule changed')
    continuity = harness_continuity(bundle, inputs, protocol, runtime)
    versions = {item['path']: item['sha256'] for item in inputs['frozen_files']}
    raw = lambda path: bundle.raw(path, versions.get(path))
    result347 = broad_replay(raw, root + '/' + cohort['name'])
    saved = strict_json(raw(root + '/' + cohort['name'] + '/disposition.json'))
    need(result347['complete_comparisons'] == 347 and result347['complete_rows'] == 12565
         and result347['baseline_engine_seconds'] == 11.979219492
         and all(saved[key] == value for key, value in result347.items())
         and saved['accepted_baseline'] is result347['benefit_gate_passed'] and saved['role'] == 'candidate'
         and saved['decision'] == ('retain_lower_complete_engine_total' if result347['benefit_gate_passed']
                                   else 'drop_no_complete_engine_gain'),
         'complete broad disposition differs from replay')
    return dict(**summary, supplemental_baseline=baseline, supplemental_comparison=result48,
        comparison=result347, driver_prerequisite_revision=revised_driver,
        harness_continuity=continuity, **combined_disposition(result347, result48))


def verify(bundle, evidence):
    need(evidence['kind'] == 'typed_ordering', 'wrong evidence family')
    gates = gates_replay(bundle, evidence)
    runtime = runtime_replay(bundle, evidence, gates)
    root = evidence['root']
    native_log = bundle.raw(root + '/t005-numeric-order-native-workspace.log').decode()
    native = test_totals(native_log, (119, 6593, 0, 36))
    default = test_totals(bundle.raw(root + '/t007-numeric-order-default-workspace.log').decode(), (107, 3290, 0, 0))
    mechanism = controls(native_log)
    python = gates['t009-numeric-order-python-source.json']
    need(python['command'][1:] == ['-B', '-m', 'unittest', 'discover', '-s', 'python/tests', '-v'],
         'Python source command changed')
    python_log = bundle.raw(root + '/t009-numeric-order-python-source.log').decode()
    need('Ran 918 tests in ' in python_log and 'OK (skipped=40)' in python_log,
         'Python source coverage changed')
    public = linked(bundle, evidence['public'])
    declaration = linked(bundle, evidence['declaration'])
    need(linked(bundle, public['runtime']) == runtime, 'public executable build changed')
    public_result = public_replay(bundle, public, declaration)
    folder = str(Path(evidence['runtime']['path']).parent)
    for name, item in runtime['files'].items():
        need(public['prehash'][folder + '/' + name] == dict(path=folder + '/' + name, **item),
             'public executable identity changed')
    external = {(item['path'], item['sha256'], item['bytes']) for item in evidence['external_artifacts']}
    frozen_files(bundle, list(public['prehash'].values()), external)
    package = installed_package(bundle, root + '/installed-typed-ordering-r1', runtime, 285)
    assembly = assembly_replay(bundle, evidence, runtime)
    baseline = baseline_binding(bundle, evidence)
    summary = dict(status='passed',
        native_workspace=native, default_workspace=default, source_python=dict(tests=918, skipped=40),
        public_acceptance=public_result, installed_package=package,
        mechanism=dict(**mechanism, assembly=assembly), baseline_binding=baseline,
        failed_validation_attempts=FAILURES,
        early_focused_checks='retained; final workspace source and controls supersede earlier test-source revisions',
        source_inputs=len(runtime['source_snapshot']), query_executions=0, external_engine_invoked=False,
        full43_run=False)
    return performance_replay(bundle, evidence, runtime, external, summary)
