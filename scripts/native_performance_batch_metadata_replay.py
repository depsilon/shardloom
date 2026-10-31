# SPDX-License-Identifier: Apache-2.0
"""Prove the metadata-only reference and preserve the invalid first observation."""
import ast
from pathlib import Path

from native_workflow_protocol import strict_json
from native_performance_batch_public_replay import public_replay
from query_surface.replay import need, sha
from shared_followthrough_replay import installed_package
from shared_performance_replay import complete_gate, linked, test_totals

METADATA_FILES = {
    'shardloom-vortex/src/universal_format_io/demanded/source.rs',
    'shardloom-vortex/src/universal_format_io/demanded/metadata_admission.rs',
    'shardloom-vortex/src/local_primitive_relational_source_demand_tests.rs',
    'shardloom-vortex/src/local_primitive_relational_source_demand_metadata_tests.rs',
    'vendor/parquet/src/arrow/arrow_reader/mod.rs',
    'vendor/parquet/src/file/metadata/mod.rs',
    'vendor/parquet/src/file/metadata/options.rs',
    'vendor/parquet/src/file/metadata/thrift/mod.rs',
    'vendor/parquet/src/file/metadata/memory_reservation.rs',
    'vendor/parquet/README.shardloom.md',
    'vendor/parquet/upstream-provenance.json',
}
METADATA_TESTS = [
    'parquet_metadata_admission_preserves_complete_metadata_under_the_original_ceiling',
    'parquet_metadata_admission_failure_cancellation_and_retry_refund_shared_credits',
    'parquet_metadata_admission_rejects_malformed_footer_without_retained_credit',
    'parquet_metadata_admission_keeps_complete_values_and_pruning_with_small_grants',
]
REFERENCE_KIND = 'accepted_sources_with_only_required_metadata_admission_repair'


def product_inputs(runtime):
    return {name: item for name, item in runtime['source_snapshot'].items()
            if name.endswith(('.rs', '.toml', '.lock')) or name.startswith('python/src/')}


def metadata_reference(bundle, evidence, candidate):
    proof = linked(bundle, evidence['metadata_reference'])
    reference = linked(bundle, proof['runtime_manifest'])
    derivation = linked(bundle, reference['reference_derivation'])
    accepted = linked(bundle, derivation['accepted_runtime_manifest'])
    baseline_inputs = strict_json(bundle.raw(evidence['root'] + '/broad-ordered-input-r1/inputs.json'))
    need(linked(bundle, baseline_inputs['runtime_manifest']) == accepted,
         'reference derivation does not use the established accepted runtime')
    need(proof['status'] == 'passed' and proof['query_executions'] == 0
         and linked(bundle, proof['source_derivation']) == derivation
         and reference['reference_kind'] == REFERENCE_KIND
         and reference['profile'] == 'release' and reference['features'] == ['release-user-surfaces']
         and reference['products'] == {name: item['sha256'] for name, item in reference['files'].items()}
         and set(reference['products']) == {'shardloom', '_native.abi3.so'}
         and reference['source_root'] == derivation['source_root']
         and reference['source_snapshot'] == derivation['source_snapshot']
         and reference['source_revision'] == derivation['source_revision']
         and derivation['accepted_source_revision'] == accepted['source_revision']
         and derivation['all_accepted_product_sources_match_git'] is True
         and derivation['resource_change'] is False and derivation['query_executions'] == 0,
         'metadata reference identity or repair boundary changed')
    need(reference['products'] != candidate['products']
         and reference['products'] != accepted['products'], 'metadata reference aliases another runtime')
    rejected = linked(bundle, next(item for item in evidence['supplements']
                                   if Path(item['path']).name == 'batch-candidate-cache-rejection.json'))
    need(rejected['status'] == 'rejected_before_freeze_or_queries'
         and rejected['products_identical_to_reference'] is True
         and rejected['candidate_query_executions'] == 0 and rejected['candidate_frozen'] is False
         and linked(bundle, rejected['reference_manifest']) == reference
         and {name: item['sha256'] for name, item in rejected['products'].items()} == reference['products'],
         'invalid cached candidate build was not preserved and rejected')
    complete_gate(linked(bundle, rejected['build_receipt']))
    invalidation = strict_json(bundle.raw(evidence['root'] + '/b075-batch-metadata-candidate-cache-invalidation.json'))
    complete_gate(invalidation)
    need(invalidation['command'] == ['cargo', 'clean', '--locked', '--offline', '--release',
         '-p', 'shardloom-vortex', '-p', 'shardloom-cli', '-p', 'shardloom-python'],
         'candidate cache invalidation changed scope')
    originals = product_inputs(accepted)
    checks = derivation['accepted_source_checks']
    need(len(originals) == len(checks) == 2655 and set(originals) == set(checks),
         'accepted production source coverage changed')
    for name, item in originals.items():
        need({key: checks[name][key] for key in ('bytes', 'sha256')} == item,
             'reference is not derived from the accepted production sources')
    changes = derivation['exact_allowed_changes']
    need(set(changes) == METADATA_FILES, 'metadata-only source delta changed')
    expected = dict(originals)
    previous_candidate = linked(bundle, next(item for item in evidence['runtimes']
                                            if '/performance-batch-release-r3/' in item['path']))
    expected_candidate = product_inputs(previous_candidate)
    for name, change in changes.items():
        after = {key: change['after'][key] for key in ('bytes', 'sha256')}
        bundle.identity(change['after'])
        bundle.identity(dict(path=reference['source_root'] + '/' + name, **after))
        need(change['before'] != after, 'metadata patch contains an unchanged file')
        if name.endswith('.rs'):
            need(change['before'] == originals.get(name)
                 and candidate['source_snapshot'].get(name) == after,
                 'reference and candidate do not contain the identical metadata repair')
            expected[name] = after
            expected_candidate[name] = after
    need(product_inputs(reference) == expected, 'reference contains an optimization or missing source')
    need(product_inputs(candidate) == expected_candidate,
         'candidate changed additional production inputs after the failed first observation')
    build = strict_json(bundle.raw(evidence['root'] + '/' + reference['build_receipt']))
    complete_gate(build)
    command = ['cargo', 'build', '--locked', '--offline', '--release', '--features',
               'release-user-surfaces', '-p', 'shardloom-cli', '-p', 'shardloom-python',
               '--manifest-path', reference['source_root'] + '/Cargo.toml']
    need(reference['build_receipt'] == 'b070-batch-metadata-reference-release.json'
         and derivation['build_command'] == command
         and len(build['command']) == 4 and build['command'][1:] ==
         ['-B', evidence['root'] + '/tools/batch_metadata_reference.py', 'build']
         and build['head'] == reference['source_revision']
         and sha(bundle.raw(evidence['root'] + '/' + reference['build_receipt'])) == reference['build_receipt_sha256'],
         'reference build command or receipt changed')
    need(bundle.raw(build['command'][2], derivation['helper']['sha256']) == bundle.identity(derivation['helper']),
         'reference helper differs from the prepared derivation')
    log = bundle.raw(evidence['root'] + '/b070-batch-metadata-reference-release.log').decode()
    need(sha(log.encode()) == build['log_sha256']
         and all(('Compiling ' + package + ' v') in log
                 and reference['source_root'] + '/' + package in log
                 for package in ('shardloom-vortex', 'shardloom-cli', 'shardloom-python')),
         'reference packages were not freshly compiled from the isolated sources')
    native = linked(bundle, proof['native_gate'])
    complete_gate(native)
    native_log = bundle.identity(proof['native_log']).decode()
    need(sha(native_log.encode()) == native['log_sha256']
         and '--manifest-path' in native['command']
         and native['command'][native['command'].index('--manifest-path') + 1] == reference['source_root'] + '/Cargo.toml',
         'reference controls used another source tree')
    test_totals(native_log, (1, 4, 0, 0))
    for name in METADATA_TESTS:
        need(sum(line.endswith('::' + name + ' ... ok') for line in native_log.splitlines()) == 1,
             'reference metadata regression control missing')
    for name in expected:
        bundle.identity(dict(path=reference['source_root'] + '/' + name, **expected[name]))
    bundle.identity(derivation['tracked_patch'])
    bundle.identity(proof['helper'])
    public = public_replay(bundle, linked(bundle, proof['public_controls']),
                           linked(bundle, proof['public_declaration']), reference, reference_harness=True)
    package = installed_package(bundle, str(Path(proof['package']['path']).parent), reference, 286)
    return dict(status='passed', accepted_product_sources=2655, allowed_changed_files=11,
                metadata_controls=4, public_controls=public, installed_package=package,
                shared_repair_identical=True, user_ceiling_unchanged=True,
                invalid_cached_build_rejected_before_queries=True, query_executions=0)


def revised_protocol(bundle, protocol):
    old = linked(bundle, protocol['previous_protocol'])
    failed = linked(bundle, protocol['prior_failed_campaign'])
    need(protocol['revision'] == 3 and protocol['reference_kind'] == REFERENCE_KIND
         and protocol['revised_pair_executions_before_declaration'] == 0
         and protocol['historical_performance_executions'] == 81
         and protocol['performance_executions_before_declaration'] == 81
         and protocol['input_sql_resource_reference_order_changes'] is False
         and protocol['cases'] == old['cases'] and protocol['generator'] == old['generator']
         and protocol['encoded_input_manifest'] == old['encoded_input_manifest']
         and failed['status'] == 'complete_with_failures'
         and failed['engine_execution_seconds'] is None,
         'revised protocol changes inputs or rewrites the failed observation')
    previous = {item['path']: item for item in old['frozen_files']}
    updated = {item['path']: item for item in protocol['frozen_files']}
    need(len(updated) == len(protocol['frozen_files'])
         and all(updated.get(path) == item for path, item in previous.items()),
         'revised protocol drops original frozen evidence')
    def harness(value):
        root = Path(value['harness_root'])
        return {str(Path(item['path']).relative_to(root)): (item['sha256'], item['generation']['bytes'])
                for item in value['frozen_files'] if Path(item['path']).is_relative_to(root)}
    need(harness(old) == harness(protocol) and harness(old), 'revised harness changed frozen source bytes')
    interrupted_protocol = linked(bundle, protocol['interrupted_protocol'])
    interrupted = linked(bundle, protocol['interrupted_campaign'])
    need(interrupted_protocol['revision'] == 2
         and interrupted_protocol['previous_protocol'] == protocol['previous_protocol']
         and interrupted_protocol['prior_failed_campaign'] == protocol['prior_failed_campaign']
         and interrupted_protocol['reference_runtime'] == protocol['reference_runtime']
         and interrupted_protocol['reference_derivation'] == protocol['reference_derivation']
         and interrupted_protocol['cases'] == protocol['cases']
         and interrupted_protocol['historical_performance_executions'] == 80
         and interrupted['status'] == 'failed' and interrupted['complete'] is False
         and interrupted['engine_execution_seconds'] is None
         and len(interrupted['observations']) == 1
         and interrupted['observations'][0]['status'] == 'passed'
         and interrupted['error'] == repr(AssertionError(interrupted_protocol['harness_root'] +
             '/python/src/shardloom/__pycache__/__init__.cpython-313.pyc')),
         'interrupted reference attempt or original metadata revision changed')
    cached = harness(interrupted_protocol)
    sources = harness(old)
    extras = set(cached) - set(sources)
    need(len(extras) == 13 and all('__pycache__' in Path(name).parts and name.endswith('.pyc') for name in extras)
         and all(cached.get(name) == item for name, item in sources.items())
         and all(name.endswith('.py') for name in harness(protocol))
         and protocol['harness_correction'] ==
             'Copy exactly the original declared Python source inventory; exclude generated bytecode caches.',
         'harness correction changes more than generated bytecode inventory')
    need(all(updated.get(item['path']) == item
             for item in (protocol['interrupted_protocol'], protocol['interrupted_campaign'])),
         'interrupted attempt is not frozen before the final pair')
    return dict(revision=3, original_inputs_and_resources_unchanged=True,
                historical_attempts_preserved=81, excluded_generated_caches=13,
                revised_pair_attempts_before_declaration=0)


def binding_correction(before, after):
    """Only the two JSON-list to native-tuple conversions may change the worker."""
    def functions(data):
        return {node.name: node for node in ast.parse(data).body if isinstance(node, ast.FunctionDef)}
    original, revised = functions(before), functions(after)
    replacements = 0
    class Fields(ast.NodeTransformer):
        def visit_Subscript(self, node):
            nonlocal replacements
            if (isinstance(node.value, ast.Name) and node.value.id == 'source'
                    and isinstance(node.slice, ast.Constant) and node.slice.value == 'fields'):
                replacements += 1
                return ast.parse("[tuple(field) for field in source['fields']]", mode='eval').body
            return self.generic_visit(node)
    expected = Fields().visit(original['worker'])
    need(replacements == 2 and ast.dump(expected, include_attributes=False) ==
         ast.dump(revised['worker'], include_attributes=False), 'worker changed beyond binding repair')
    names = ['configure', 'read', 'digest', 'write', 'write_gzip', 'read_gzip', 'snapshot',
             'make_case', 'product_sources', 'make_producer', 'observe', 'replay']
    need(all(ast.dump(original[name], include_attributes=False) ==
             ast.dump(revised[name], include_attributes=False) for name in names),
         '80-case execution, supervision or replay changed')
    return dict(binding_conversions=2, other_worker_code_unchanged=True,
                unchanged_execution_functions=len(names))
