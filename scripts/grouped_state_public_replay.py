# SPDX-License-Identifier: Apache-2.0
"""Replay complete grouped-state public results against pre-execution references."""
import json

from native_workflow_protocol import strict_json
from query_surface.evidence import no_fallback
from query_surface.replay import contract, need, sha
from query_surface.semantics import compare, result_from_record

PART_BYTES = 8 << 20
METHOD = 'test_grouped_state_page_boundaries_and_compositions_match_all_surfaces'
GROUPS = (1, 16, 512, 513, 1024, 1025, 4097)
FORMATS = ('parquet', 'arrow', 'vortex')
SURFACES = ('cli_sql', 'embedded_workflow', 'python_sql', 'dataframe', 'typed_relation')


def fingerprint(value):
    return sha(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode())


def case_names(groups):
    if groups in (1, 16):
        names = ['mixed_' + column for column in (
            'small_value', 'signed_value', 'unsigned_value', 'metric', 'optional_value', 'all_null')]
        names += ['filtered_optional', 'renamed_reverse_extrema']
    elif groups == 4097:
        names = ['mixed_optional_value', 'mixed_unsigned_value']
    else:
        names = ['mixed_signed_value', 'mixed_metric', 'count_rows']
    return [f'grouped_state_g{groups}_' + name for name in names]


def declarations(value):
    need(value['schema_version'] == 'shardloom.grouped_state_declarations.v1'
         and value['status'] == 'passed' and value['native_query_executions'] == value['performance_executions'] == 0
         and value['observe_and_replay_ast_unchanged'] is True
         and value['formats'] == list(FORMATS), 'grouped declarations incomplete')
    ids = [name for group in GROUPS for name in case_names(group)]
    need(value['case_ids'] == ids and len(ids) == 30
         and [row['groups'] for row in value['sizes']] == list(GROUPS), 'grouped declaration coverage changed')
    references, datasets = {}, {}
    for row in value['sizes']:
        group = row['groups']
        dataset = dict(seed=0x31_2026_C7, fact_rows=group * 3 + 11, entity_rows=11,
                       product_rows=7, relationship_rows=13, calendar_rows=5, profile='null_heavy')
        need(row['dataset'] == dataset and [item['case'] for item in row['references']] == case_names(group),
             'grouped source or reference order changed')
        datasets[group] = dataset
        for item in row['references']:
            need(type(item['rows']) is int and 0 <= item['rows'] <= group
                 and (item['rows'] == group or 'filtered' in item['case']), 'grouped reference population changed')
            references[item['case']] = (group, item)
    rows = sum(item['rows'] for _, item in references.values()) * 15
    need(value['expected_complete_comparisons'] == 450 and value['expected_source_fixtures'] == 21
         and rows == value['expected_complete_rows'] == 263265, 'declared result totals changed')
    return references, datasets


def public_parts(bundle, value):
    need(value['comparison_part_max_bytes'] == PART_BYTES
         and value['comparison_parts_posthash_verified'] is True, 'public partition contract changed')
    paths = set()
    need(0 < len(value['comparison_parts']) <= 450, 'invalid public part count')
    for item in value['comparison_parts']:
        need(item['path'] not in paths and type(item['bytes']) is int and 0 < item['bytes'] <= PART_BYTES,
             'invalid public part identity')
        paths.add(item['path'])
        rows = strict_json(bundle.identity(item))
        need(isinstance(rows, list) and len(rows) == item['comparisons'] > 0
             and sum(row['rows'] for row in rows) == item['rows'], 'public part totals changed')
        yield from rows


def resource_fields(fields):
    def number(name):
        value = fields['execution_resource_' + name]
        need(isinstance(value, str) and value.isdecimal(), 'invalid resource observation: ' + name)
        return int(value)
    grant = number('admitted_memory_bytes')
    need(grant == 256 << 20 and number('declared_max_parallelism') == 2
         and number('observed_native_denied_reservations') == 0
         and 0 <= number('observed_native_reserved_bytes')
         <= number('observed_native_peak_reserved_bytes') <= grant, 'public resource contract changed')
    # The report is taken while results/input owners can still be live. Final
    # zero-credit release is proven by the native ownership tests, not this value.


def declaration_identity(row):
    name = row['case']
    record = dict(id=name, family='grouped_aggregation', archetype=name, factors={},
        partition='held_out' if int(sha(name.encode())[:8], 16) % 5 == 0 else 'development',
        neighbor_of=None, relation=row['report']['public_declaration'], desired_sql=None,
        pending_reason=None, surfaces=list(SURFACES), contract=row['contract'])
    return sha(json.dumps(record, sort_keys=True, allow_nan=False).encode())


def public_replay(bundle, value, declaration):
    references, datasets = declarations(declaration)
    need(value['schema_version'] == 'shardloom.grouped_state_public_acceptance.v1'
         and value['status'] == 'passed' and value['tests'] == 1
         and value['test_names'] == [METHOD]
         and value['failures'] == value['errors'] == value['skipped'] == 0
         and value['failure_details'] == [] and value['posthash_verified'] is True
         and value['comparison_engine_invoked'] is False and value['performance_claim'] is False,
         'public grouped acceptance incomplete')
    fixtures = {}
    for source in value['source_fixtures']:
        groups = (source['dataset']['fact_rows'] - 11) // 3
        key = (groups, source['format'])
        need(key not in fixtures and groups in datasets and source['dataset'] == datasets[groups]
             and source['method'] == METHOD and source['format'] in FORMATS
             and source['layout'] == 'odd original indexes followed by reversed even indexes'
             and set(source['files']) == {'fact'} and len(source['files']['fact']) == 1,
             'public grouped fixture changed')
        fixtures[key] = source['files']['fact'][0]
    need(set(fixtures) == {(group, form) for group in GROUPS for form in FORMATS}, 'public source grid incomplete')
    grid = {(case, form, surface) for case in references for form in FORMATS for surface in SURFACES}
    coverage, complete_rows = set(), 0
    for row in public_parts(bundle, value):
        key = (row['case'], row['format'], row['surface'])
        need(key in grid and key not in coverage and row['method'] == METHOD, 'public grouped comparison coverage changed')
        coverage.add(key)
        group, reference = references[row['case']]
        need(fingerprint(dict(expected=row['expected'], contract=row['contract']))
             == reference['expected_and_contract_sha256'], 'pre-execution public reference changed')
        need(declaration_identity(row) == reference['identity'], 'executed grouped declaration changed')
        compare(result_from_record(row['expected']), result_from_record(row['actual']), contract(row['contract']))
        report = row['report']
        need(row['rows'] == len(row['actual']['rows']) == reference['rows']
             and report['complete_consumption'] is True and report['surface'] == row['surface'],
             'public grouped result incomplete')
        fields = report['fields']
        no_fallback(fields)
        resource_fields(fields)
        need(fixtures[(group, row['format'])]['path'] in report['sql_declaration'], 'public source binding changed')
        if row['format'] == 'parquet':
            need(int(fields['native_source_demand_sources']) > 0
                 and fields['native_source_demand_prepared_input_bytes_written'] == '0'
                 and fields['native_input_end_observed'] == fields['native_input_output_ownership_detached'] == 'true',
                 'public Parquet completion or ownership changed')
        complete_rows += row['rows']
    need(coverage == grid and len(coverage) == value['positive_public_executions'] == 450
         and complete_rows == value['compared_rows'] == 263265, 'complete public totals changed')
    return dict(status='passed', complete_comparisons=450, complete_rows=complete_rows,
                source_fixtures=len(fixtures), pre_execution_references_verified=True)
