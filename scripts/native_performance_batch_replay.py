# SPDX-License-Identifier: Apache-2.0
"""Verify the combined batch's retained correctness evidence without execution."""
import ast
import gzip
from pathlib import Path
import re

from native_workflow_protocol import strict_json
from native_performance_batch_public_replay import fixture, public_replay
from native_performance_batch_metadata_replay import (
    METADATA_TESTS, binding_correction, metadata_reference, revised_protocol,
)
from native_performance_batch_provenance import baseline_binding, fixture_corrections, rich_merge_correction
from native_performance_batch_workflow_replay import workflow_replay
from ordered_input_public_replay import public_replay as format_replay
from parquet_demand_completion_replay import replay as replay_cohort
from query_surface.replay import need, sha
from shared_followthrough_replay import frozen_files, installed_package
from shared_performance_replay import complete_gate, linked, test_totals

BUILD = 'build --locked --offline --release --features release-user-surfaces -p shardloom-cli -p shardloom-python'
REQUIRED = {
    'b066-batch-metadata-format-final.json': 'fmt --all -- --check',
    'b064-batch-metadata-native-clippy.json': 'clippy --locked --offline --workspace --all-targets --features release-user-surfaces --profile ci-native -- -D warnings',
    'b065-batch-metadata-native-workspace.json': 'test --locked --offline --workspace --all-targets --features release-user-surfaces --profile ci-native',
    'b067-batch-metadata-default-clippy.json': 'clippy --locked --offline --workspace --all-targets -- -D warnings',
    'b068-batch-metadata-default-workspace.json': 'test --locked --offline --workspace --all-targets',
    'b075-batch-metadata-candidate-release-rebuilt.json': BUILD,
}
FAILURES = [
    'b002-batch-native-tests.json', 'b003-batch-native-tests.json',
    'b005-batch-native-clippy.json', 'b007-batch-native-clippy.json',
    'b009-batch-native-clippy.json', 'b012-batch-native-workspace.json',
    'b014-batch-native-workspace.json', 'b015-batch-file-boundary-diagnosis.json',
    'b018-batch-file-boundary-tests.json', 'b025-batch-independent-references.json',
    'b030-batch-public-controls.json', 'b031-batch-public-controls-corrected.json',
    'b037-batch-installed-package.json', 'b039-batch-rich-merge-clippy.json',
    'b040-batch-rich-merge-format.json',
    'b042-batch-rich-merge-native-workspace.json',
    'b059-batch-metadata-format.json', 'b060-batch-metadata-native-controls.json',
    'b061-batch-metadata-native-controls.json', 'b062-batch-metadata-native-controls.json',
    'b073-batch-metadata-reference-package.json',
    'b074-batch-metadata-reference-proof.json',
    'b083-batch-metadata-local-acceptance.json',
    'b088-batch-harness-local-acceptance.json',
]
CONTROLS = {
    'B1': ['parquet_prefix_fetches_requested_pages_and_keeps_complete_group_control'],
    'B2': ['projection_input_views_share_distinct_eager_functions_and_release_before_return',
           'projection_input_views_keep_identity_single_use_and_lazy_domains_separate'],
    'B3': ['composed_encoded_integer_reductions_preserve_sliced_runs_nulls_and_counts',
           'composed_encoded_file_reductions_use_actual_constant_and_runend_inputs',
           'single_chunk_projection_probe_never_executes_unknown_children',
           'encoded_numeric_probe_unsupported_slice_never_executes_child_on_miss',
           'encoded_numeric_probe_unknown_struct_projection_never_executes_child_on_miss'],
    'B4': ['appended_utf8_concat_keeps_nulls_unicode_long_values_and_owned_outputs',
           'prepared_utf8_functions_match_scalar_owner_for_unicode_dictionary_slices_and_constants',
           'prepared_utf8_preserves_unreferenced_domain_and_observed_code_errors',
           'prepared_utf8_memory_refusal_cancellation_and_fresh_retry_release_credits'],
    'B5': ['direct_join_conditions_keep_duplicates_nulls_wide_integers_and_complete_candidates',
           'native_relational_on_precedes_null_extension_for_every_join_kind',
           'native_relational_on_and_post_join_where_have_distinct_results_through_all_writers',
           'native_relational_on_candidates_cross_batches_and_release_after_cancellation_and_pressure'],
    'B6': ['pure_equality_correlation_builds_once_and_preserves_null_membership_and_guards',
           'pure_correlation_cross_filter_aliases_preserve_complete_projection',
           'pure_correlation_does_not_reorder_opaque_file_value_validation',
           'native_relational_sql_pure_equality_predicates_build_resident_inner_once',
           'integer_boolean_proof_accepts_only_bounded_canonical_owners',
           'bound_memory_proof_keeps_filters_limits_and_generated_sources_inert'],
    'B7': ['relational_merge_adapts_fan_in_and_preserves_stable_complete_output'],
    'B8': ['ordered_join_hash_directory_keeps_cross_block_collisions_exact'],
    'B9': ['replay_capture_keeps_nullable_fragments_large_controls_and_pressure_declines',
           'replay_capture_releases_every_fragment_and_preserves_raw_float_bits_and_alias_credit'],
    'B10': ['finite_global_quantiles_share_domains_and_preserve_stable_selected_float_bits',
            'finite_multiselection_matches_complete_stable_reference_and_propagates_errors'],
    'metadata_repair': METADATA_TESTS,
}


def gates_replay(bundle, evidence):
    gates, failures = {}, []
    for item in evidence['gates']:
        name = Path(item['path']).name
        need(name not in gates, 'duplicate batch gate')
        gate = gates[name] = linked(bundle, item)
        need(item['status'] == gate['status'] and (gate['returncode'] == 0) == (gate['status'] == 'passed'),
             'gate status changed')
        need(sha(bundle.raw(evidence['root'] + '/' + name[:-5] + '.log')) == gate['log_sha256'],
             'gate log changed')
        if gate['status'] == 'passed':
            complete_gate(gate)
        else:
            failures.append(name)
        for relative, source in gate['changed_source_inventory'].items():
            if 'sha256' in source:
                bundle.identity(dict(path=gate['cwd'] + '/' + relative, **source))
        need(sha(bundle.raw(evidence['root'] + '/source-blobs/' + gate['driver_sha256']))
             == gate['driver_sha256'], 'gate driver changed')
    need(failures == FAILURES and set(REQUIRED) <= gates.keys(), 'gate history is incomplete')
    return gates


def runtime_replay(bundle, evidence, gates):
    runtime = linked(bundle, evidence['runtime'])
    build = gates[runtime['build_receipt']]
    need(runtime['build_receipt'] == 'b075-batch-metadata-candidate-release-rebuilt.json'
         and runtime['source_revision'] == build['head'] and runtime['profile'] == 'release'
         and runtime['features'] == ['release-user-surfaces']
         and sha(bundle.raw(evidence['root'] + '/' + runtime['build_receipt'])) == runtime['build_receipt_sha256']
         and set(runtime['files']) == {'shardloom', '_native.abi3.so'}
         and runtime['products'] == {key: value['sha256'] for key, value in runtime['files'].items()}
         and len(runtime['source_snapshot']) > 3000, 'runtime build identity changed')
    build_log = bundle.raw(evidence['root'] + '/' + runtime['build_receipt'][:-5] + '.log').decode()
    need(all('Compiling ' + package + ' v' in build_log and build['cwd'] + '/' + package in build_log
             for package in ('shardloom-vortex', 'shardloom-cli', 'shardloom-python')),
         'candidate engine and entry points were not freshly compiled')
    folder = str(Path(evidence['runtime']['path']).parent)
    bundle.identity(dict(path=folder + '/source-overlay.patch', **runtime['source_overlay']))
    for relative, item in runtime['source_snapshot'].items():
        bundle.identity(dict(path=build['cwd'] + '/' + relative, **item))
    for name, command in REQUIRED.items():
        gate = gates[name]
        complete_gate(gate)
        need(gate['command'] == ['cargo', *command.split()], 'required gate command changed')
        # Python fixture construction is not a Cargo compiler input. Its final
        # version is checked by complete public controls and installed-wheel tests.
        for relative, item in gate['changed_source_inventory'].items():
            if relative.endswith(('.rs', '.toml', '.lock')) and 'sha256' in item:
                need(runtime['source_snapshot'].get(relative) == item, 'compiled input changed after validation')
    return runtime


def native_controls(bundle, proof, runtime):
    need(proof['status'] == 'passed' and proof['controls'] == 32
         and proof['query_executions'] == proof['new_test_executions'] == 0,
         'mechanism proof is incomplete')
    gate = linked(bundle, proof['native_gate'])
    complete_gate(gate)
    log = bundle.identity(proof['native_log']).decode()
    need(sha(log.encode()) == gate['log_sha256'], 'mechanism log differs from workspace test')
    totals = test_totals(log, (119, 6615, 0, 37))
    need(set(proof['families']) == set(CONTROLS), 'mechanism families changed')
    for family, names in CONTROLS.items():
        rows = proof['families'][family]
        need([row['test'].split('::')[-1] for row in rows] == names, 'mechanism controls changed')
        for row in rows:
            need(log.splitlines().count('test ' + row['test'] + ' ... ok') == 1,
                 'missing mechanism test: ' + row['test'])
            item = proof['source_identities'][row['source']]
            data = bundle.identity(item)
            need(re.search(rb'\bfn\s+' + row['test'].split('::')[-1].encode() + rb'\s*\(', data)
                 and {key: item[key] for key in ('bytes', 'sha256')} == runtime['source_snapshot'][row['source']],
                 'mechanism source differs from measured runtime')
    bundle.identity(proof['helper'])
    return dict(controls=32, families=10, metadata_controls=4, workspace=totals)


def release_hooks(bundle, proof, runtime):
    need(proof['status'] == 'passed' and proof['query_executions'] == 0
         and proof['observation_owner_absent'] is True
         and linked(bundle, proof['runtime_manifest']) == runtime
         and {row['product'] for row in proof['records']} == set(runtime['products']),
         'release observation check is incomplete')
    for row in proof['records']:
        need(row['product_identity']['sha256'] == runtime['products'][row['product']],
             'release inspection used another binary')
        symbols = gzip.decompress(bundle.identity(row['symbols']))
        need(len(symbols) == row['symbol_bytes'] and b'shardloom_vortex::' in symbols
             and b'native_relational_observation' not in symbols,
             'release contains test observation symbols')
    source = bundle.identity(proof['source'])
    need((b'#[cfg(all(test, debug_assertions, feature = "vortex-local-primitives", unix))]\n'
          b'#[path = "local_primitives/native_relational_observation.rs"]\n'
          b'pub(crate) mod native_relational_observation;') in source,
         'test-only observation boundary changed')
    bundle.identity(proof['helper'])
    return dict(products=2, debug_observation_owner_absent=True)


def independent_references(bundle, proof, runtime):
    reference = fixture()
    need(proof['status'] == 'passed' and proof['query_engine_executions'] == 0
         and proof['provider_imports_and_execution_forbidden'] is True,
         'independent reference proof incomplete')
    for item in proof['sources']:
        bundle.identity(item)
        if Path(item['path']).name == 'native_performance_batch_cases.py':
            need({key: item[key] for key in ('bytes', 'sha256')}
                 == runtime['source_snapshot']['python/tests/native_performance_batch_cases.py'],
                 'independent reference fixture differs')
    cases = reference.cases()
    need(len(proof['cases']) == len(cases) == 80, 'independent reference grid incomplete')
    rows = 0
    for case, saved in zip(cases, proof['cases'], strict=True):
        records = reference.records(case)
        expected = reference.reference(case, records)
        paths = {name: f"memory://{case['case_id']}/{name}" for name in records}
        need(saved == dict(case=case, input_rows={name: len(data) for name, data in records.items()},
                           expected_rows=len(expected.rows), sql=reference.statement(case, paths),
                           resources=reference.resources(case)), 'independent reference declaration changed')
        rows += len(expected.rows)
    need(rows == proof['expected_rows'] == 871209, 'independent result population changed')
    return dict(complete_cases=80, expected_rows=rows, query_executions=0)


def encoded_inputs(proof):
    need(proof['query_engine_executions'] == proof['performance_observations'] == 0
         and proof['seed'] == 0x31_2026_BA7 and len(proof['files']) == 8,
         'encoded fixture declaration changed')
    pairs = [(size, shape) for size in (4097, 131101)
             for shape in ('constant', 'runend', 'sliced_runend', 'primitive')]
    need([(row['rows'], row['shape']) for row in proof['files']] == pairs,
         'encoded fixture coverage incomplete')
    for row in proof['files']:
        physical = row['physical']
        need(row['complete_provider_readback'] is True and physical['complete_flat_inspection'] is True
             and physical['external_engine_invoked'] is physical['fallback_attempted'] is False
             and physical['provider_version'] == '=0.90.0', 'encoded fixture was not inspected and read back')
        encoding = 'vortex.' + ('runend' if row['shape'] == 'sliced_runend' else row['shape'])
        nodes = [node for part in physical['flat_references'] for node in part['stored_array_nodes']
                 if node['physical_child_path'] == ['n']]
        need(nodes and all(node['encoding_id'] == encoding for node in nodes)
             and sum(node['rows'] for node in nodes) == row['rows'], 'physical encoding changed')
    return dict(native_files=8, complete_provider_readbacks=8, query_executions=0)


def driver_review(bundle, proof):
    need(proof['status'] == 'passed' and proof['query_executions'] == 0
         and proof['original347_execution_and_supervision_ast_unchanged'] is True
         and ((proof['fixture_corrections_before_performance'] is True) or
              (proof.get('revision') == 3 and proof.get('corrections_before_revised_pair') is True
               and linked(bundle, proof['prior_failed_campaign'])['status'] == 'complete_with_failures')),
         'driver review incomplete')
    sources = {Path(item['path']).name: bundle.identity(item) for item in proof['source_identities']}
    def functions(data):
        return {node.name: ast.dump(node, include_attributes=False) for node in ast.parse(data).body
                if isinstance(node, ast.FunctionDef)}
    before = functions(sources['performance_cohort_ordered_inputs.py'])
    after = functions(sources['performance_cohort_batch.py'])
    names = ['read', 'digest', 'write', 'preserved_reporting_harness',
             'preserved_application_harness', 'configure', 'observe', 'replay']
    need(proof['unchanged347_functions'] == names and all(before[name] == after[name] for name in names),
         '347 observation or supervision changed')
    need(set(proof['reviewed80_boundaries']) == {'population', 'binding', 'clock', 'ownership',
                                              'supervision', 'decision', 'correctness'},
         '80-workflow driver review incomplete')
    old_protocol = linked(bundle, proof['original80_protocol'])
    interrupted = linked(bundle, proof['interrupted_campaign'])
    need(interrupted['status'] == 'failed' and interrupted['complete'] is False
         and interrupted['engine_execution_seconds'] is None
         and len(interrupted['observations']) == 1,
         'driver review omits the interrupted reference attempt')
    correction = binding_correction(bundle.identity(old_protocol['observer_driver']), sources['batch_cohort.py'])
    need(proof['binding_correction'] == correction, 'binding correction review changed')
    bundle.identity(proof['helper'])
    return dict(unchanged347_functions=len(names), query_executions=0)


def local_replay(bundle, evidence):
    need(evidence['kind'] == 'native_performance_batch', 'wrong batch evidence kind')
    root = evidence['root']
    gates = gates_replay(bundle, evidence)
    runtime = runtime_replay(bundle, evidence, gates)
    native = native_controls(bundle, linked(bundle, evidence['native_controls']), runtime)
    defaults = test_totals(bundle.raw(root + '/b068-batch-metadata-default-workspace.log').decode(), (107, 3290, 0, 0))
    controls = public_replay(bundle, linked(bundle, evidence['public_controls']),
                             linked(bundle, evidence['public_declaration']), runtime)
    formats = format_replay(bundle, linked(bundle, evidence['format_report']),
                            linked(bundle, evidence['format_declaration']))
    need(linked(bundle, linked(bundle, evidence['format_report'])['runtime']) == runtime,
         'interface/format proof used another runtime')
    complete_gate(gates['b077-batch-metadata-rich-set-regression.json'])
    complete_gate(gates['b078-batch-metadata-public-controls.json'])
    complete_gate(gates['b079-batch-metadata-surface-format.json'])
    complete_gate(gates['b080-batch-metadata-installed-package.json'])
    package = installed_package(bundle, str(Path(evidence['package']['path']).parent), runtime, 286)
    external = {(item['path'], item['sha256'], item['bytes']) for item in evidence['external_artifacts']}
    need(len(external) == len(evidence['external_artifacts']), 'duplicate external payload identity')
    return dict(status='passed', disposition='local_correctness_only_performance_pending',
        native=native, default_workspace=defaults, public_controls=controls, format_controls=formats,
        installed_package=package,
        release_hooks=release_hooks(bundle, linked(bundle, evidence['release_hooks']), runtime),
        independent_references=independent_references(bundle, linked(bundle, evidence['independent_references']), runtime),
        encoded_inputs=encoded_inputs(linked(bundle, evidence['encoded_inputs'])),
        driver_review=driver_review(bundle, linked(bundle, evidence['driver_review'])),
        fixture_corrections=fixture_corrections(bundle, evidence, runtime),
        rich_merge_correction=rich_merge_correction(bundle, evidence, runtime, gates),
        metadata_reference=metadata_reference(bundle, evidence, runtime),
        accepted_baseline=baseline_binding(bundle, evidence, external),
        retained_failed_attempts=FAILURES, query_executions=0)


def harness_continuity(bundle, inputs, protocol):
    previous = linked(bundle, inputs['baseline_inputs'])
    old_protocol = linked(bundle, previous['protocol'])
    harness = Path(inputs['harness_root'])
    need(inputs['harness_root'] == previous['harness_root'], '347 harness root changed')

    def inventory(value):
        rows = [row for row in value['frozen_files'] if Path(row['path']).is_relative_to(harness)]
        need(len(rows) == len({row['path'] for row in rows}), 'duplicate 347 harness path')
        return {row['path']: row for row in rows}

    retained = inventory(inputs)
    need(retained and retained == inventory(previous), 'original 347 harness inventory changed')
    for key in ('frozen_reporting_harness_compatibility', 'frozen_application_harness_compatibility',
                'native_sql_entrypoints', 'original_execution_observer', 'original_inputs'):
        need(protocol[key] == old_protocol[key], '347 compatibility boundary changed: ' + key)
    for item in retained.values():
        bundle.identity(item)
    sources = [bundle.identity(value['observer_driver']) for value in (old_protocol, protocol)]
    trees = [{node.name: ast.dump(node, include_attributes=False) for node in ast.parse(source).body
              if isinstance(node, ast.FunctionDef)} for source in sources]
    names = ['read', 'digest', 'write', 'preserved_reporting_harness',
             'preserved_application_harness', 'configure', 'observe', 'replay']
    need(all(trees[0][name] == trees[1][name] for name in names),
         '347 execution, supervision or replay changed')
    need(protocol['supplemental_gate'] ==
         'Final retention additionally requires the new complete frozen 80-workflow candidate total to be lower than its single corrected-reference observation. Both complete cohorts must improve. The ten owners share one batch disposition; there is no per-owner timing loop. The original 347-case observer and result schema remain unchanged.',
         'combined retention gate changed')
    return dict(retained_harness_files=len(retained), unchanged_observer_functions=len(names),
                observation_and_supervision_unchanged=True)


def combined_disposition(broad, supplemental):
    need(type(broad['benefit_gate_passed']) is bool and type(supplemental['benefit_gate_passed']) is bool,
         'both complete benefit gates are required')
    retained = broad['benefit_gate_passed'] and supplemental['benefit_gate_passed']
    return dict(accepted_baseline_advanced=retained,
        disposition='retain_both_complete_cohort_gains' if retained else 'drop_one_or_both_cohorts_no_gain')


def verify(bundle, anchors):
    evidence = bundle.index['evidence']
    need(evidence == anchors, 'combined batch evidence anchors changed')
    summary = local_replay(bundle, evidence)
    root = evidence['root']
    external = {(item['path'], item['sha256'], item['bytes']) for item in evidence['external_artifacts']}
    need([item['name'] for item in evidence['failed_cohorts']] ==
         ['performance-batch-accepted-r1', 'performance-batch-reference-r1'],
         'failed or interrupted campaign is missing')
    historical = strict_json(bundle.raw(root + '/performance-batch-accepted-r1/inputs.json'))
    established = strict_json(bundle.raw(root + '/broad-ordered-input-r1/inputs.json'))
    need(historical['runtime_manifest'] == established['runtime_manifest'],
         'failed first campaign no longer uses the original accepted runtime')
    summary['failed_original_campaign'] = workflow_replay(bundle,
        root + '/performance-batch-accepted-r1', external, failed_history=True)
    summary['interrupted_reference_campaign'] = workflow_replay(bundle,
        root + '/performance-batch-reference-r1', external, interrupted_history=True)
    roles = evidence['supplemental_cohorts']
    need(set(roles) == {'accepted', 'candidate'}, 'supplemental roles changed')
    if roles['accepted'] is None:
        need(roles['candidate'] is evidence['cohort'] is None, 'candidate lacks accepted supplemental observation')
        return summary
    baseline = workflow_replay(bundle, root + '/' + roles['accepted']['name'], external)
    accepted_inputs = linked(bundle, roles['accepted']['inputs'])
    reference = linked(bundle, evidence['metadata_reference'])
    declared = linked(bundle, accepted_inputs['protocol'])
    need(accepted_inputs['runtime_manifest'] == reference['runtime_manifest']
         and accepted_inputs['runtime_manifest'] == declared['reference_runtime'],
         'supplemental reference differs from the metadata-only runtime')
    summary['protocol_revision'] = revised_protocol(bundle, declared)
    summary['supplemental_baseline'] = baseline
    if roles['candidate'] is None:
        need(evidence['cohort'] is None, 'broad result cannot replace a missing supplemental candidate')
        return summary
    candidate_inputs = linked(bundle, roles['candidate']['inputs'])
    runtime = linked(bundle, evidence['runtime'])
    need(candidate_inputs['protocol'] == accepted_inputs['protocol']
         and candidate_inputs['cases'] == accepted_inputs['cases']
         and linked(bundle, candidate_inputs['runtime_manifest']) == runtime,
         'paired supplemental inputs, resources, protocol or runtime changed')
    supplemental = workflow_replay(bundle, root + '/' + roles['candidate']['name'], external,
                                   baseline_seconds=baseline['engine_execution_seconds'])
    summary['supplemental_comparison'] = supplemental
    if evidence['cohort'] is None:
        summary['disposition'] = 'supplemental_complete_broad_performance_pending'
        return summary
    cohort = evidence['cohort']
    inputs, protocol = linked(bundle, cohort['inputs']), linked(bundle, cohort['protocol'])
    frozen_files(bundle, inputs['frozen_files'] + protocol['prerequisites'], external)
    need(linked(bundle, inputs['runtime_manifest']) == runtime and inputs['repeats'] == 1
         and inputs['memory_bytes'] == 2 << 30 and inputs['parallelism'] == 2
         and protocol['performance_executions_before_freeze'] == 0,
         '347 cohort runtime, resources or prospective admission changed')
    declared = linked(bundle, accepted_inputs['protocol'])
    need(bundle.identity(protocol['source_contract']) == bundle.identity(declared['source_contract']),
         '80 and 347 cohorts use different source contracts')
    need(protocol['retention_decision'] == dict(rule='strict_complete_cohort_gain',
        declared_families=['composite', 'distinct', 'global_aggregation', 'grouped_aggregation', 'joins',
            'multiway_joins', 'nullable', 'numeric', 'ordering', 'predicates', 'projection', 'sets', 'strings', 'temporal'],
        material_case_regression=dict(fraction=0.20, absolute_seconds=0.010),
        material_family_regression=dict(fraction=0.20, absolute_seconds=0.050)), '347 retention rule changed')
    continuity = harness_continuity(bundle, inputs, protocol)
    versions = {item['path']: item['sha256'] for item in inputs['frozen_files']}
    raw = lambda path: bundle.raw(path, versions.get(path))
    broad = replay_cohort(raw, root + '/' + cohort['name'])
    saved = strict_json(raw(root + '/' + cohort['name'] + '/disposition.json'))
    need(broad['complete_comparisons'] == 347 and broad['complete_rows'] == 12565
         and broad['baseline_engine_seconds'] == 11.979219492
         and all(saved[key] == value for key, value in broad.items())
         and saved['accepted_baseline'] is broad['benefit_gate_passed'] and saved['role'] == 'candidate'
         and saved['decision'] == ('retain_lower_complete_engine_total' if broad['benefit_gate_passed']
             else 'drop_no_complete_engine_gain'), '347 complete-cohort result changed')
    return dict(summary, comparison=broad, harness_continuity=continuity,
                **combined_disposition(broad, supplemental))
