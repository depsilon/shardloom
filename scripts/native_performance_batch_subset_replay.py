# SPDX-License-Identifier: Apache-2.0
"""Additive proof for B6/B9 retention; the original ten-owner decision is immutable."""
import ast
from collections import Counter
from pathlib import Path
import re

from native_workflow_protocol import strict_json
from native_performance_batch_public_replay import public_replay
from native_performance_batch_replay import (
    BUILD, CONTROLS, combined_disposition, release_hooks, verify as original_verify,
)
from native_performance_batch_workflow_replay import workflow_replay
from ordered_input_public_replay import public_replay as format_replay
from parquet_demand_completion_replay import replay as replay_cohort
from query_surface.replay import need, sha
from shared_followthrough_replay import frozen_files, installed_package
from shared_performance_replay import complete_gate, linked, test_totals

OWNERS = ['B6', 'B9']
DROPPED = ['B1', 'B2', 'B3', 'B4', 'B5', 'B7', 'B8', 'B10']
SHARED = {
    'shardloom-vortex/src/local_primitive_result_batch.rs',
    'shardloom-vortex/src/local_primitives/native_relational_observation.rs',
}
RETAINED = {
    'shardloom-cli/src/sql_native_relational_memory_tests.rs',
    *('shardloom-vortex/src/' + name + '.rs' for name in (
        'local_primitive_prepared_relational', 'local_primitive_relational_batch_input_tests',
        'local_primitive_relational_batch_replay', 'local_primitive_relational_correlated',
        'local_primitive_relational_correlated_tests', 'local_primitive_relational_subquery_bind',
        'local_primitive_relational_decorrelate', 'local_primitive_relational_replay_capture',
        'local_primitive_relational_replay_capture_tests', 'resident_memory_source',
        'resident_memory_infallible_tests')),
}
REQUIRED = {
    'b097-batch-subset-native-clippy.json': 'clippy --locked --offline --workspace --all-targets --features release-user-surfaces --profile ci-native -- -D warnings',
    'b100-batch-subset-native-workspace.json': 'test --locked --offline --workspace --all-targets --features release-user-surfaces --profile ci-native',
    'b101-batch-subset-format.json': 'fmt --all -- --check',
    'b102-batch-subset-default-clippy.json': 'clippy --locked --offline --workspace --all-targets -- -D warnings',
    'b103-batch-subset-default-workspace.json': 'test --locked --offline --workspace --all-targets',
    'b104-batch-subset-release.json': BUILD,
}
CONTROL_NAMES = [*CONTROLS['B6'], *CONTROLS['B9'], *CONTROLS['metadata_repair'],
    'completion_input_tiny_primitive_replay_compacts_before_each_producer_return',
    'ordered_aggregate_recovery_refuses_live_owner_cleans_dead_runs_and_restarts_exactly',
    'ordered_aggregate_spilled_faults_refund_state_and_never_report_completion',
    'ordered_aggregate_spilled_write_reopens_and_refuses_partial_publication']
FUNCTIONS80 = ['configure', 'read', 'digest', 'write', 'write_gzip', 'read_gzip',
               'snapshot', 'product_sources', 'make_producer', 'worker', 'observe', 'replay']
FUNCTIONS347 = ['read', 'digest', 'write', 'preserved_reporting_harness',
                'preserved_application_harness', 'configure', 'observe', 'replay']
SUPPLEMENTAL_GATE = ('Retain B6/B9 only if the new complete 80-workflow candidate improves on '
    'the reused corrected reference and this complete 347 candidate improves on the reused '
    'accepted baseline. Keep shared metadata repair independently. Preserve the rejected '
    'ten-owner observation. No per-owner timing loop; original 347 observer and result schema remain unchanged.')


def product_map(runtime):
    return {name: item for name, item in runtime['source_snapshot'].items()
            if name.endswith(('.rs', '.toml', '.lock')) or name.startswith('python/src/')}


def source_reduction(bundle, evidence, subset, runtime):
    reference = linked(bundle, linked(bundle, evidence['metadata_reference'])['runtime_manifest'])
    previous = linked(bundle, evidence['runtime'])
    proof = linked(bundle, subset['source_reduction'])
    plan = linked(bundle, subset['source_reduction_plan'])
    need(proof['status'] == 'passed' and proof['retained_owners'] == plan['retained_owners'] == OWNERS
         and proof['dropped_owners'] == DROPPED and proof['query_executions'] == plan['query_executions'] == 0
         and plan['status'] == 'declared_before_source_reduction' and plan['metadata_repair_retained'] is True
         and linked(bundle, proof['reduction_plan']) == plan
         and sha(bundle.identity(subset['source_contract'])) == plan['source_contract_sha256'],
         'subset source reduction was not explicitly declared')
    for field, item in (('original_candidate_manifest', evidence['runtime']),
                        ('reference_manifest', linked(bundle, evidence['metadata_reference'])['runtime_manifest'])):
        data = bundle.identity(item)
        need(plan[field] == dict(bytes=len(data), sha256=sha(data)), 'reduction predecessor changed')
    wanted, before, current = product_map(reference), product_map(previous), product_map(runtime)
    need(current == proof['production_inputs'] and len(current) == proof['production_input_count'] == 2662
         and proof['all_other_inputs_equal_metadata_reference'] is True,
         'subset compiled source inventory changed')
    changed = {name for name in current.keys() | wanted.keys() if current.get(name) != wanted.get(name)}
    need(changed == RETAINED | SHARED == set(proof['only_changes_from_metadata_reference']),
         'subset contains other optimizations or omits retained owners')
    need(all(current[name] == before[name] for name in RETAINED), 'retained owner implementation changed')
    reference_root = reference['source_root']
    candidate_root = strict_json(bundle.raw(evidence['root'] + '/' + runtime['build_receipt']))['cwd']
    for name in SHARED:
        old = bundle.identity(dict(path=reference_root + '/' + name, **wanted[name]))
        if name.endswith('local_primitive_result_batch.rs'):
            token = b'\nfn primitive(ptype:'
            need(old.count(token) == 1, 'native primitive boundary changed')
            expected = old.replace(token, b'\npub(super) fn primitive(ptype:', 1)
        else:
            token = b'    pub(crate) join_retained_right_rows: u64,\n'
            need(old.count(token) == 1, 'observation owner boundary changed')
            expected = old.replace(token, token + b'    pub(crate) subquery_equality_rewrites: u64,\n'
                b'    pub(crate) replay_captured_fragments: u64,\n'
                b'    pub(crate) replay_captured_rows: u64,\n', 1)
        need(bundle.identity(dict(path=candidate_root + '/' + name, **current[name])) == expected,
             'shared file retains an unrelated owner hunk')
    actions = Counter()
    need(len(plan['records']) == len({row['path'] for row in plan['records']}) == 59,
         'source reduction file coverage changed')
    for row in plan['records']:
        name, action = row['path'], row['action']
        actions[action] += 1
        need(row['before'] == before[name] and row['after'] == current.get(name),
             'source reduction before or after bytes changed')
        if action == 'restore_metadata_reference':
            need(row['after'] == wanted[name], 'dropped owner was not restored')
        elif action == 'remove_other_owner':
            need(name not in wanted and row['after'] is None, 'new dropped owner remains')
        elif action == 'retain_metadata_repair':
            need(row['before'] == row['after'] == wanted[name], 'metadata repair changed')
        elif action == 'retain_B6_B9':
            need(name in RETAINED and row['before'] == row['after'], 'retained source drift')
        else:
            need(action == 'retain_only_shared_B6_B9_hunks' and name in SHARED, 'unknown reduction action')
    need(actions == dict(retain_B6_B9=12, retain_metadata_repair=9,
        retain_only_shared_B6_B9_hunks=2, restore_metadata_reference=28, remove_other_owner=8),
        'source reduction action coverage changed')
    return dict(production_inputs=2662, retained_owner_files=14, restored_files=28,
                removed_files=8, metadata_files_preserved=9)


def same_functions(old, new, names):
    def functions(data):
        return {node.name: ast.dump(node, include_attributes=False) for node in ast.parse(data).body
                if isinstance(node, ast.FunctionDef)}
    first, second = functions(old), functions(new)
    need(all(name in first and name in second and first[name] == second[name] for name in names),
         'subset changed execution, supervision or replay')


def driver_review(bundle, proof):
    need(proof['status'] == 'passed' and proof['query_executions'] == 0
         and set(proof['drivers']) == {'batch_cohort.py', 'performance_cohort_batch.py'},
         'subset driver review incomplete')
    for name, names in (('batch_cohort.py', FUNCTIONS80), ('performance_cohort_batch.py', FUNCTIONS347)):
        row = proof['drivers'][name]
        need(row['unchanged_functions'] == names, 'subset observer coverage changed')
        same_functions(bundle.identity(row['previous']), bundle.identity(row['current']), names)
    bundle.identity(proof['helper'])
    return dict(unchanged80_functions=12, unchanged347_functions=8, query_executions=0)


def retained_protocol(bundle, protocol):
    old = linked(bundle, protocol['original_protocol'])
    need(old['revision'] == 3 and protocol['revision'] == 4
         and protocol['status'] == 'declared_before_retained_candidate_execution'
         and protocol['retained_owners'] == OWNERS and protocol['dropped_owners'] == DROPPED
         and protocol['retained_candidate_executions_before_declaration'] == protocol['reference_reexecutions'] == 0
         and protocol['performance_executions_before_declaration'] == protocol['historical_performance_executions'] == 241
         and protocol['original_ten_owner_disposition'] == 'drop_one_or_both_cohorts_no_gain'
         and protocol['shared_correctness_repair'] == 'parquet_metadata_admission'
         and protocol['retention_rule'] == 'Retain B6/B9 only if both unchanged complete 80 and 347 engine totals improve; keep shared metadata repair independently.'
         and protocol['input_sql_resource_reference_order_changes'] is False,
         'retained composition was not declared prospectively')
    for key in ('cases', 'harness_root', 'generator', 'encoded_input_manifest', 'reference_runtime',
                'reference_derivation', 'reference_kind', 'operation', 'supervision', 'decision',
                'cache_policy', 'result_contract', 'reference_method'):
        need(protocol[key] == old[key], 'retained cohort changed frozen inputs: ' + key)
    need(protocol['frozen_files'][:len(old['frozen_files'])] == old['frozen_files'],
         'retained cohort omitted original source or harness identities')
    for key, seconds in (('reference_observations', 40.838371451), ('rejected_observations', 27.771706382)):
        result = linked(bundle, protocol[key])
        need(result['status'] == 'passed' and result['complete'] and result['posthash_verified']
             and len(result['observations']) == 80 and result['engine_execution_seconds'] == seconds,
             'retained cohort changed prior observations')
    need(linked(bundle, protocol['original_complete_acceptance'])['disposition'] ==
         'drop_one_or_both_cohorts_no_gain', 'original batch rejection was changed')
    same_functions(bundle.identity(old['observer_driver']), bundle.identity(protocol['observer_driver']), FUNCTIONS80)
    return dict(unchanged_cases=80, reference_reexecutions=0, new_candidate_executions_before_declaration=0)


def native_controls(bundle, proof, runtime):
    need(proof['status'] == 'passed' and proof['query_executions'] == proof['new_test_executions'] == 0
         and proof['controls'] == len(CONTROL_NAMES), 'subset native controls incomplete')
    gate = linked(bundle, proof['native_gate'])
    complete_gate(gate)
    log = bundle.identity(proof['native_log']).decode()
    need(sha(log.encode()) == gate['log_sha256'], 'subset native log changed')
    totals = test_totals(log, (119, 6603, 0, 36))
    need([row['test'].split('::')[-1] for row in proof['tests']] == CONTROL_NAMES,
         'subset required native controls changed')
    for row in proof['tests']:
        need(log.splitlines().count('test ' + row['test'] + ' ... ok') == 1,
             'missing subset native test: ' + row['test'])
        item = proof['source_identities'][row['source']]
        source = bundle.identity(item)
        need({key: item[key] for key in ('bytes', 'sha256')} == runtime['source_snapshot'][row['source']]
             and re.search(rb'\bfn\s+' + row['test'].split('::')[-1].encode() + rb'\s*\(', source),
             'subset native control source changed')
    bundle.identity(proof['helper'])
    return dict(controls=len(CONTROL_NAMES), workspace=totals)


def local_replay(bundle, evidence, subset):
    root, gates = evidence['root'], {}
    runtime = linked(bundle, subset['runtime'])
    for item in subset['gates']:
        name = Path(item['path']).name
        need(name not in gates and int(name[1:4]) >= 97, 'subset gate history changed')
        gate = gates[name] = linked(bundle, item)
        need(item['status'] == gate['status']
             and (gate['returncode'] == 0) == (gate['status'] == 'passed')
             and sha(bundle.raw(root + '/' + name[:-5] + '.log')) == gate['log_sha256'],
             'subset gate outcome or log changed')
        if gate['status'] == 'passed':
            complete_gate(gate)
        for relative, expected in gate['changed_source_inventory'].items():
            if 'sha256' in expected:
                bundle.identity(dict(path=gate['cwd'] + '/' + relative, **expected))
        need(sha(bundle.raw(root + '/source-blobs/' + gate['driver_sha256'])) == gate['driver_sha256'],
             'subset guard changed')
    need(set(REQUIRED) <= gates.keys(), 'subset required gates missing')
    for name, totals in (('b098-batch-subset-correlation-controls.json', (1, 8, 0, 0)),
                         ('b099-batch-subset-lifecycle-controls.json', (6, 68, 0, 0))):
        need(name in gates, 'subset focused gate missing')
        complete_gate(gates[name])
        test_totals(bundle.raw(root + '/' + name[:-5] + '.log').decode(), totals)
    for name, command in REQUIRED.items():
        gate = gates[name]
        complete_gate(gate)
        need(gate['command'] == ['cargo', *command.split()], 'subset required command changed')
        for relative, expected in gate['changed_source_inventory'].items():
            if relative.endswith(('.rs', '.toml', '.lock')) and 'sha256' in expected:
                need(runtime['source_snapshot'].get(relative) == expected, 'subset source changed after validation')
    build = gates['b104-batch-subset-release.json']
    need(runtime['build_receipt'] == 'b104-batch-subset-release.json'
         and sha(bundle.raw(root + '/' + runtime['build_receipt'])) == runtime['build_receipt_sha256']
         and runtime['source_revision'] == build['head'] and runtime['profile'] == 'release'
         and runtime['features'] == ['release-user-surfaces']
         and runtime['products'] == {key: value['sha256'] for key, value in runtime['files'].items()}
         and set(runtime['products']) == {'shardloom', '_native.abi3.so'}, 'subset runtime identity changed')
    for relative, item in runtime['source_snapshot'].items():
        bundle.identity(dict(path=build['cwd'] + '/' + relative, **item))
    previous = linked(bundle, evidence['runtime'])
    reference = linked(bundle, linked(bundle, evidence['metadata_reference'])['runtime_manifest'])
    need(all(runtime['products'][key] not in {previous['products'][key], reference['products'][key]}
             for key in runtime['products']), 'subset reused predecessor products')
    build_log = bundle.raw(root + '/b104-batch-subset-release.log').decode()
    need(all('Compiling ' + package + ' v' in build_log and build['cwd'] + '/' + package in build_log
             for package in ('shardloom-vortex', 'shardloom-cli', 'shardloom-python')),
         'subset engine and entry points were not freshly compiled')
    native = native_controls(bundle, linked(bundle, subset['native_controls']), runtime)
    defaults = test_totals(bundle.raw(root + '/b103-batch-subset-default-workspace.log').decode(), (107, 3290, 0, 0))
    controls = public_replay(bundle, linked(bundle, subset['public_controls']),
        linked(bundle, subset['public_declaration']), runtime)
    formats = format_replay(bundle, linked(bundle, subset['format_report']), linked(bundle, subset['format_declaration']))
    need(linked(bundle, linked(bundle, subset['format_report'])['runtime']) == runtime,
         'subset format proof used another runtime')
    return dict(status='passed', disposition='local_correctness_only_performance_pending',
        retained_owners=OWNERS, dropped_owners=DROPPED, metadata_repair_retained=True,
        source_reduction=source_reduction(bundle, evidence, subset, runtime), native=native,
        default_workspace=defaults, public_controls=controls, format_controls=formats,
        installed_package=installed_package(bundle, str(Path(subset['package']['path']).parent), runtime, 286),
        release_hooks=release_hooks(bundle, linked(bundle, subset['release_hooks']), runtime),
        driver_review=driver_review(bundle, linked(bundle, subset['driver_review'])), query_executions=0)


def verify(bundle, anchors):
    original = original_verify(bundle, anchors)
    if 'retained_subset' not in anchors:
        return original
    need(original['disposition'] == 'drop_one_or_both_cohorts_no_gain'
         and original['accepted_baseline_advanced'] is False, 'original whole-batch outcome changed')
    subset = anchors['retained_subset']
    historical = linked(bundle, subset['original_acceptance'])
    need(all(historical[key] == original[key] for key in original), 'original complete proof changed')
    result = local_replay(bundle, anchors, subset)
    if subset['supplemental_cohort'] is None:
        need(subset['cohort'] is None, 'subset broad cohort lacks supplemental acceptance')
        return dict(original, retained_subset=result)
    root = anchors['root']
    external = {(item['path'], item['sha256'], item['bytes']) for item in anchors['external_artifacts']}
    candidate = subset['supplemental_cohort']
    inputs = linked(bundle, candidate['inputs'])
    protocol = linked(bundle, inputs['protocol'])
    reference_inputs = linked(bundle, anchors['supplemental_cohorts']['accepted']['inputs'])
    need(inputs['runtime_manifest']['sha256'] == subset['runtime']['sha256']
         and protocol['source_contract']['sha256'] == subset['source_contract']['sha256']
         and protocol['source_reduction']['sha256'] == subset['source_reduction']['sha256']
         and protocol['source_reduction_plan']['sha256'] == subset['source_reduction_plan']['sha256']
         and protocol['original_protocol'] == reference_inputs['protocol']
         and protocol['reference_observations']['sha256'] ==
             anchors['supplemental_cohorts']['accepted']['observations']['sha256']
         and protocol['original_complete_acceptance']['sha256'] == subset['original_acceptance']['sha256'],
         'subset supplemental runtime or source contract changed')
    supplemental = workflow_replay(bundle, root + '/' + candidate['name'], external,
        baseline_seconds=original['supplemental_baseline']['engine_execution_seconds'], retained_candidate=True)
    result.update(supplemental_comparison=supplemental, disposition='supplemental_complete_broad_performance_pending')
    if subset['cohort'] is None:
        return dict(original, retained_subset=result)
    cohort = subset['cohort']
    broad_inputs, broad_protocol = linked(bundle, cohort['inputs']), linked(bundle, cohort['protocol'])
    frozen_files(bundle, broad_inputs['frozen_files'] + broad_protocol['prerequisites'], external)
    previous = linked(bundle, broad_inputs['baseline_inputs'])
    old_protocol = linked(bundle, previous['protocol'])
    need(broad_inputs['runtime_manifest'] == inputs['runtime_manifest']
         and broad_protocol['source_contract'] == protocol['source_contract']
         and broad_inputs['repeats'] == 1 and broad_inputs['memory_bytes'] == 2 << 30
         and broad_inputs['parallelism'] == 2 and broad_protocol['performance_executions_before_freeze'] == 0
         and broad_protocol['retention_decision'] == linked(bundle, anchors['cohort']['protocol'])['retention_decision']
         and broad_protocol['supplemental_gate'] == SUPPLEMENTAL_GATE, 'subset broad protocol changed')
    for key in ('cases', 'sources', 'harness_root', 'dataset', 'logical_identity'):
        need(broad_inputs[key] == previous[key], 'subset broad inputs changed: ' + key)
    harness = Path(previous['harness_root'])
    def harness_files(value):
        return [item for item in value['frozen_files'] if Path(item['path']).is_relative_to(harness)]
    need(harness_files(broad_inputs) == harness_files(previous), 'subset broad harness changed')
    for key in ('frozen_reporting_harness_compatibility', 'frozen_application_harness_compatibility',
                'native_sql_entrypoints', 'original_execution_observer', 'original_inputs'):
        need(broad_protocol[key] == old_protocol[key], 'subset broad execution boundary changed')
    same_functions(bundle.identity(old_protocol['observer_driver']),
                   bundle.identity(broad_protocol['observer_driver']), FUNCTIONS347)
    versions = {item['path']: item['sha256'] for item in broad_inputs['frozen_files']}
    raw = lambda path: bundle.raw(path, versions.get(path))
    broad = replay_cohort(raw, root + '/' + cohort['name'])
    saved = strict_json(raw(root + '/' + cohort['name'] + '/disposition.json'))
    need(broad['complete_comparisons'] == 347 and broad['complete_rows'] == 12565
         and broad['baseline_engine_seconds'] == 11.979219492
         and all(saved[key] == value for key, value in broad.items())
         and saved['accepted_baseline'] is broad['benefit_gate_passed'] and saved['role'] == 'candidate'
         and saved['decision'] == ('retain_lower_complete_engine_total' if broad['benefit_gate_passed']
                                   else 'drop_no_complete_engine_gain'), 'subset complete broad outcome changed')
    result.update(comparison=broad, **combined_disposition(broad, supplemental))
    return dict(original, retained_subset=result)
