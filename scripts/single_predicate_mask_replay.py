# SPDX-License-Identifier: Apache-2.0
"""Replay complete single-predicate results without loading an execution engine."""
import ast
import copy
import json
from pathlib import Path
import re

from native_workflow_protocol import strict_json
from parquet_demand_completion_replay import replay as replay_cohort
from query_surface.evidence import no_fallback
from query_surface.replay import contract, need, sha
from query_surface.semantics import compare, result_from_record
from shared_followthrough_replay import frozen_files, installed_package, source_matches
from shared_performance_replay import complete_gate, linked, test_totals
from single_predicate_mask_cohort_replay import application_acceptance, harness_continuity, rejection

PART_BYTES = 8 << 20
METHOD = 'test_fresh_single_masks_and_declines_across_sizes_formats_and_surfaces'
SIZES = (0, 65, 4097)
FORMATS = ('parquet', 'arrow', 'vortex')
SURFACES = ('cli_sql', 'embedded_workflow', 'python_sql', 'dataframe', 'typed_relation')
CASES = (
    'mask_eq', 'mask_ne', 'mask_lt', 'mask_le', 'mask_gt', 'mask_ge', 'mask_reversed',
    'mask_optional_value_is_null', 'mask_optional_value_is_not_null',
    'mask_all_null_is_null', 'mask_all_null_is_not_null',
    'mask_nullable_no_null_is_null', 'mask_nullable_no_null_is_not_null',
    'mask_not', 'mask_null_or_positive', 'mask_valid_and_positive', 'mask_projected_truth',
    'mask_renamed', 'mask_aggregate_global', 'mask_aggregate_grouped',
    'mask_computed_is_null', 'mask_computed_is_not_null',
    # Preserve the executed historical identifier; the recorded small_value
    # schema is I16. I32 admission refusal is covered by the native unit suite.
    'mask_decline_i32', 'mask_decline_u64', 'mask_decline_f64', 'mask_decline_text', 'mask_decline_nested',
)
BUILD = 'build --locked --offline --release --features release-user-surfaces -p shardloom-cli -p shardloom-python'
REQUIRED = {
    'm009-mask-native-clippy.json': 'clippy --locked --offline --workspace --all-targets --features release-user-surfaces -- -D warnings',
    'm010-mask-native-workspace.json': 'test --locked --offline --workspace --all-targets --features release-user-surfaces --profile ci-native',
    'm011-mask-default-clippy.json': 'clippy --locked --offline --workspace --all-targets -- -D warnings',
    'm012-mask-default-workspace.json': 'test --locked --offline --workspace --all-targets',
    'm013-mask-format-check.json': 'fmt --all -- --check',
    'm015-mask-release-build.json': BUILD,
    'm020-mask-fixture-build.json': BUILD,
}


def fingerprint(value):
    return sha(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode())


def public_parts(bundle, value):
    need(value['comparison_part_max_bytes'] == PART_BYTES
         and value['comparison_parts_posthash_verified'] is True, 'public partition contract changed')
    paths = set()
    need(0 < len(value['comparison_parts']) <= 1305, 'invalid public part count')
    for item in value['comparison_parts']:
        need(item['path'] not in paths and 0 < item['bytes'] <= PART_BYTES, 'invalid public part identity')
        paths.add(item['path'])
        rows = strict_json(bundle.identity(item))
        need(isinstance(rows, list) and len(rows) == item['comparisons'] > 0
             and sum(row['rows'] for row in rows) == item['rows'], 'public part totals changed')
        yield from rows


def operand_order(row):
    report = row['report']
    order = report['fixture_operand_order']
    typed = row['surface'] == 'typed_relation'
    need(order['reference'] == 'literal_left'
         and order['executed'] == ('literal_left' if typed else 'column_left')
         and order['equivalent_supported_sql_form'] is (not typed), 'operand-order declaration changed')
    declaration = copy.deepcopy(order['original_declaration'])
    need(declaration['op'] == 'project' and declaration['inputs'][0]['op'] == 'filter',
         'reversed comparison declaration changed')
    predicate = declaration['inputs'][0]['args'][0]
    need(predicate['op'] == 'gt' and predicate['args'][0]['op'] == 'literal'
         and predicate['args'][0]['value'] == 17 and predicate['args'][1]['op'] == 'column'
         and predicate['args'][1]['value'] == 'optional_value', 'reversed reference predicate changed')
    if not typed:
        predicate['op'] = 'lt'
        predicate['args'].reverse()
    need(declaration == report['public_declaration'], 'submitted predicate is not the recorded exact equivalent')


def public_replay(bundle, value):
    passed = value['status'] == 'passed'
    need(value['schema_version'] == 'shardloom.single_predicate_mask_public_acceptance.v2'
         and value['status'] in ('passed', 'failed') and value['tests'] == 1
         and value['test_names'] == [METHOD] and value['failures'] == value['skipped'] == 0
         and value['posthash_verified'] is True and value['comparison_engine_invoked'] is False
         and value['performance_claim'] is False, 'public acceptance envelope changed')
    grid = {(f'rows_{size}_{case}', form, surface) for size in SIZES
            for case in (*CASES, 'mask_named_0', 'mask_named_1')
            for form in FORMATS for surface in SURFACES}
    if not passed:
        need(value['errors'] == len(value['failure_details']) == 36, 'unexpected failed public attempt')
        failures = set()
        for failure in value['failure_details']:
            match = re.search(r"rows=(\d+), format='(\w+)', case='mask_reversed', surface='(\w+)'", failure['test'])
            need(match is not None and 'SQL identifiers must start with an ASCII letter or underscore' in failure['detail']
                 and 'external_engine_invoked=false' in failure['detail'], 'public failure cause changed')
            failures.add((f'rows_{int(match[1])}_mask_reversed', match[2], match[3]))
        declined = {(f'rows_{size}_mask_reversed', form, surface) for size in SIZES
                    for form in FORMATS for surface in SURFACES if surface != 'typed_relation'}
        need(failures == declined, 'SQL spelling refusal coverage changed')
        grid -= declined
    else:
        need(value['errors'] == 0 and value['failure_details'] == [], 'accepted public errors present')
    fixtures = {}
    for source in value['source_fixtures']:
        dataset = source['dataset']
        key = (dataset['fact_rows'], source['format'])
        need(key not in fixtures and source['method'] == METHOD
             and dataset == dict(seed=0x3020269A1, fact_rows=key[0], entity_rows=37,
                 product_rows=19, relationship_rows=89, calendar_rows=11, profile='null_heavy')
             and source['layout'] == 'odd original indexes followed by reversed even indexes'
             and set(source['files']) == {'fact'} and len(source['files']['fact']) == 1,
             'fresh public source declaration changed')
        fixtures[key] = source['files']['fact'][0]
    need(set(fixtures) == {(size, form) for size in SIZES for form in FORMATS}, 'fresh source grid incomplete')
    coverage, references, rows = set(), {}, 0
    for row in public_parts(bundle, value):
        key = (row['case'], row['format'], row['surface'])
        need(key in grid and key not in coverage and row['method'] == METHOD, 'public comparison coverage changed')
        coverage.add(key)
        compare(result_from_record(row['expected']), result_from_record(row['actual']), contract(row['contract']))
        report = row['report']
        need(row['rows'] == len(row['actual']['rows']) and report['complete_consumption'] is True
             and report['surface'] == row['surface'], 'public complete consumption changed')
        no_fallback(report['fields'])
        size = int(row['case'].split('_', 2)[1])
        source = fixtures[(size, row['format'])]
        need(source['path'] in report['sql_declaration'], 'public source binding changed')
        if row['format'] == 'parquet':
            fields = report['fields']
            need(int(fields['native_source_demand_sources']) > 0
                 and fields['native_source_demand_prepared_input_bytes_written'] == '0'
                 and fields['native_input_end_observed'] == fields['native_input_output_ownership_detached'] == 'true',
                 'public Parquet completion or ownership changed')
        if '_mask_named_' in row['case']:
            need(report['fields']['relational_materialized_relations'] == '1'
                 and report['consumed_after_session_close'] is True
                 and report['repeated_action'] == int(row['case'].rsplit('_', 1)[1]), 'named output lifetime changed')
        if passed and row['case'].endswith('_mask_reversed'):
            operand_order(row)
        references[key] = fingerprint(dict(expected=row['expected'], contract=row['contract']))
        rows += row['rows']
    need(coverage == grid and len(coverage) == value['positive_public_executions']
         == (1305 if passed else 1269) and rows == value['compared_rows']
         == (779070 if passed else 765822), 'public complete grid or row total changed')
    return dict(status=value['status'], accepted_executions=len(coverage) if passed else 0,
                completed_comparisons_retained=len(coverage), complete_rows=rows), references


def fixture_correction(bundle, proof, before, after):
    fixture = 'python/tests/test_native_single_predicate_mask_binding.py'
    changed = [name for name in before['source_snapshot'] if before['source_snapshot'][name] != after['source_snapshot'].get(name)]
    need(set(before['source_snapshot']) == set(after['source_snapshot']) and changed == [fixture]
         and before['files'] == after['files'] and before['products'] == after['products'], 'fixture correction changed runtime')
    old, new = (bundle.identity(proof[key]) for key in ('original_fixture', 'corrected_fixture'))
    need(old == bundle.identity(proof['retained_original']) and proof['status'] == 'passed'
         and proof['runtime_source_changes'] == [] and proof['query_executions'] == 0
         and proof['frozen_347_changed'] is False and proof['sql_syntax_added'] is False
         and proof['planned_public_executions'] == 1305 and proof['expected_compared_rows'] == 779070,
         'fixture correction provenance changed')
    for key, runtime in (('original_fixture', before), ('corrected_fixture', after)):
        need({field: proof[key][field] for field in ('bytes', 'sha256')} == runtime['source_snapshot'][fixture],
             'fixture correction source identity changed')
    def declaration(raw):
        return ast.dump(next(node for node in ast.parse(raw).body
            if isinstance(node, ast.FunctionDef) and node.name == 'mask_cases'), include_attributes=False)
    need(declaration(old) == declaration(new) and proof['original_case_declaration_ast_unchanged'] is True,
         'original independent declarations changed')


def mechanism_replay(bundle, root):
    log = bundle.raw(root + '/m006-mask-focused.log').decode()
    test_totals(log, (1, 94, 0, 1))
    records = {'mask': [], 'general': [], 'pipeline': []}
    for label, body in re.findall(r'single_predicate_(mask|general|pipeline)_work=(\{[^\n]+\})', log):
        records[label].append(strict_json(body.encode()))
    need({name: len(rows) for name, rows in records.items()} == dict(mask=2, general=2, pipeline=6),
         'predicate work observation coverage changed')
    for row in records['mask']:
        need(row['key_columns'] == row['predicate_mask_batches'] == 1
             and row['predicate_mask_rows'] == 65 and row['predicate_mask_bytes'] == 9
             and row['predicate_boolean_rows'] == row['predicate_generic_selection_rows'] == 0,
             'single-predicate mask work changed')
    need([row['key_columns'] for row in records['general']] == [2, 1]
         and all(row['predicate_mask_rows'] == 0 for row in records['general']), 'ordinary preparation controls changed')
    for row in records['pipeline']:
        need(row['predicate_mask_batches'] == row['predicate_mask_bytes'] == 3
             and row['predicate_mask_rows'] == 9
             and row['predicate_boolean_rows'] == row['predicate_generic_selection_rows'] == 0,
             'streamed predicate mask activation changed')
    full = bundle.raw(root + '/m010-mask-native-workspace.log').decode()
    for name in ('single_predicate_filter_keeps_admitted_native_scan_pushdown',
                 'single_predicate_filter_masks_feed_complete_global_and_grouped_aggregates',
                 'single_predicate_mask_selection_preserves_u16_extent_boundary',
                 'single_predicate_masks_preserve_schema_checks_denials_cancellation_and_alias_credits'):
        need(name + ' ... ok' in full, 'required ownership or consumer test missing: ' + name)
    failed = bundle.raw(root + '/m002-mask-focused.log').decode()
    need('test result: FAILED. 91 passed; 2 failed; 1 ignored;' in failed,
         'original activation fixture failure changed')
    return dict(private_mask_observations=2, private_generic_controls=2,
                streamed_mask_observations=6, debug_only=True, production_timing_claim=False)


def verify(bundle, anchors):
    evidence = bundle.index['evidence']
    need(evidence == anchors and evidence['kind'] == 'single_predicate_masks', 'single-mask anchors changed')
    root = evidence['root']
    external = {(item['path'], item['sha256'], item['bytes']) for item in evidence['external_artifacts']}
    need(len(external) == len(evidence['external_artifacts']), 'duplicate external payload identity')
    gates, failures = {}, []
    for item in evidence['gates']:
        name = Path(item['path']).name
        need(name not in gates, 'duplicate process gate')
        gate = gates[name] = linked(bundle, item)
        need(gate['status'] == item['status'] and (gate['returncode'] == 0) == (gate['status'] == 'passed')
             and sha(bundle.raw(root + '/' + name[:-5] + '.log')) == gate['log_sha256'], 'process result or log changed')
        if gate['status'] != 'passed':
            failures.append(name)
        for relative, source in gate['changed_source_inventory'].items():
            if 'sha256' in source:
                bundle.identity(dict(path=gate['cwd'] + '/' + relative, **source))
        need(sha(bundle.raw(root + '/source-blobs/' + gate['driver_sha256'])) == gate['driver_sha256'],
             'process driver changed')
    expected_failures = ['m002-mask-focused.json', 'm018-mask-public-results.json', 'm024-mask-local-replay.json']
    if evidence['cohort'] is not None:
        expected_failures.append('m026-mask-cohort-freeze.json')
    need(failures == expected_failures, 'failed attempt inventory changed')
    need('ValueError: missing retained path: ' + root + '/source-blobs/cca74b83d7e6beb4252eac3f01721d8ae674bc62b3cca1176242608678e5bb11'
         in bundle.raw(root + '/m024-mask-local-replay.log').decode(), 'original collector failure changed')
    need(set(REQUIRED) <= gates.keys(), 'required process gate missing')
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
             and runtime['products'] == {key: value['sha256'] for key, value in runtime['files'].items()},
             'runtime build binding changed')
        bundle.identity(dict(path=root + '/' + name + '/source-overlay.patch', **runtime['source_overlay']))
        for relative, source in runtime['source_snapshot'].items():
            bundle.identity(dict(path=build['cwd'] + '/' + relative, **source))
        source_matches(build, runtime)
    need(set(runtimes) == {'single-predicate-mask-release-r1', 'single-predicate-mask-release-r2'}, 'runtime set changed')
    before, final = (runtimes['single-predicate-mask-release-' + name] for name in ('r1', 'r2'))
    for name, command in REQUIRED.items():
        gate = gates[name]
        complete_gate(gate)
        need(gate['command'] == ['cargo', *command.split()], 'required gate command changed')
        source_matches(gate, final if name == 'm020-mask-fixture-build.json' else before)
    native = test_totals(bundle.raw(root + '/m010-mask-native-workspace.log').decode(), (119, 6581, 0, 35))
    default = test_totals(bundle.raw(root + '/m012-mask-default-workspace.log').decode(), (107, 3290, 0, 0))
    for name in ('m006-mask-focused.json', 'm014-mask-python-source.json', 'm019-mask-fixture-correction.json',
                 'm022-mask-public-corrected.json', 'm023-mask-installed-package.json'):
        complete_gate(gates[name])
    publics, references = {}, {}
    for item in evidence['public_reports']:
        value = linked(bundle, item)
        name = Path(item['path']).name
        need(name not in publics, 'duplicate public attempt')
        publics[name], references[name] = public_replay(bundle, value)
        runtime = runtimes[value['runtime']]
        need(value['build_receipt'] == runtime['build_receipt']
             and value['build_receipt_sha256'] == runtime['build_receipt_sha256'], 'public build binding changed')
        for product, identity in runtime['files'].items():
            need(value['native_product_and_source_identity'][root + '/' + value['runtime'] + '/' + product] == identity,
                 'public product identity changed')
        frozen_files(bundle, [dict(path=path, **item) for path, item in value['native_product_and_source_identity'].items()], external)
    need(set(publics) == {'single-predicate-mask-public-r1.json', 'single-predicate-mask-public-r2.json'}, 'public attempts changed')
    old, new = (references['single-predicate-mask-public-' + name + '.json'] for name in ('r1', 'r2'))
    for key, digest in new.items():
        prior = key if key in old else (key[0], key[1], 'typed_relation')
        need(old[prior] == digest, 'independent public reference changed across fixture correction')
    supplements = {Path(item['path']).name: item for item in evidence['supplements']}
    fixture_correction(bundle, linked(bundle, supplements['single-mask-public-fixture-correction.json']), before, final)
    package = installed_package(bundle, root + '/installed-single-predicate-mask-r1', final, 284)
    summary = dict(status='passed', native_workspace=native, default_workspace=default,
        public_attempts=publics, installed_package=package, failed_validation_attempts=failures,
        mechanism=mechanism_replay(bundle, root),
        external_engine_invoked=False, full43_run=False, query_executions=0)
    cohort = evidence['cohort']
    if cohort is None:
        return dict(**summary, disposition='local_correctness_only_performance_pending')
    inputs, protocol = linked(bundle, cohort['inputs']), linked(bundle, cohort['protocol'])
    frozen_files(bundle, inputs['frozen_files'] + protocol['prerequisites'], external)
    need(linked(bundle, inputs['runtime_manifest']) == final and inputs['repeats'] == 1
         and inputs['memory_bytes'] == 2 << 30 and inputs['parallelism'] == 2
         and protocol['performance_executions_before_freeze'] == 0, 'cohort admission changed')
    need(protocol['retention_decision'] == dict(rule='strict_complete_cohort_gain',
        declared_families=['predicates', 'nullable', 'global_aggregation', 'grouped_aggregation'],
        material_case_regression=dict(fraction=0.20, absolute_seconds=0.010),
        material_family_regression=dict(fraction=0.20, absolute_seconds=0.050)), 'frozen benefit rule changed')
    for name in ('m027-mask-cohort-freeze-corrected.json', 'm028-mask-cohort-replay.json', 'm029-mask-runtime-drop.json'):
        complete_gate(gates[name])
    need('FileNotFoundError' in bundle.raw(root + '/m026-mask-cohort-freeze.log').decode()
         and '/benchmark-harness-r2/python/src/shardloom/_batch_function.py' in
         bundle.raw(root + '/m026-mask-cohort-freeze.log').decode(), 'original freeze failure changed')
    continuity = harness_continuity(bundle, inputs, protocol, final,
        linked(bundle, supplements['single-mask-declarations-r2.json']))
    hosted = application_acceptance(bundle, root, evidence['application_closeout'])
    versions = {item['path']: item['sha256'] for item in inputs['frozen_files']}
    raw = lambda path: bundle.raw(path, versions.get(path))
    result = replay_cohort(raw, root + '/' + cohort['name'])
    saved = strict_json(raw(root + '/' + cohort['name'] + '/disposition.json'))
    need(result['complete_comparisons'] == 347 and result['complete_rows'] == 12565
         and result['baseline_engine_seconds'] == 12.055132613
         and all(saved[key] == value for key, value in result.items())
         and saved['accepted_baseline'] is result['benefit_gate_passed'] and saved['role'] == 'candidate'
         and saved['decision'] == ('retain_lower_complete_engine_total' if result['benefit_gate_passed']
             else 'drop_no_complete_engine_gain'), 'complete-cohort disposition changed')
    need(result['candidate_engine_seconds'] == 12.199579628 and result['benefit_gate_passed'] is False,
         'observed rejected candidate disposition changed')
    retirement = rejection(bundle, linked(bundle, supplements['single-mask-rejection.json']), final,
        linked(bundle, evidence['accepted_application_runtime']), result)
    return dict(**summary, disposition=saved['decision'], comparison=result,
                harness_continuity=continuity, application_prerequisite=hosted, candidate_retirement=retirement)
