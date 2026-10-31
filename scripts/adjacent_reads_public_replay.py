# SPDX-License-Identifier: Apache-2.0
"""Replay complete adjacent-read results and their independent read-work checks."""
import json

from grouped_state_public_replay import fingerprint, public_parts, resource_fields
from query_surface.evidence import no_fallback
from query_surface.replay import contract, need, sha
from query_surface.semantics import compare, result_from_record

METHOD = 'test_adjacent_reads_and_complete_consumers_match_all_surfaces'
SIZES = (0, 37, 2053)
FORMATS = ('parquet', 'arrow', 'vortex')
SURFACES = ('cli_sql', 'embedded_workflow', 'python_sql', 'dataframe', 'typed_relation')
NAMES = ('adjacent_keys', 'adjacent_numeric', 'separated', 'rich', 'wide_prefix',
         'filtered_rich', 'mixed', 'join_population_inner_mixed',
         'join_population_left_count', 'star_join')
PROJECTIONS = {
    'adjacent_keys': ('row_id', 'entity_id', 'product_id'),
    'adjacent_numeric': ('row_id', 'signed_value', 'small_value', 'unsigned_value'),
    'separated': ('row_id', 'small_value', 'short_text'),
}
COMPOSITIONS = {
    'join_population_inner_mixed': ('joins', dict(kind='inner', probe='full_fact',
                                               build='full_entities', measures='mixed')),
    'join_population_left_count': ('joins', dict(kind='left', probe='full_fact', build='full_entities')),
    'star_join': ('multiway_joins', dict(join_order='entities_then_products', probe='full_fact')),
}


def case_names(size):
    return [f'adjacent_read_n{size}_' + name for name in NAMES]


def declarations(value):
    need(value['schema_version'] == 'shardloom.adjacent_reads_declarations.v1'
         and value['status'] == 'passed'
         and value['native_query_executions'] == value['performance_executions'] == 0
         and value['observe_and_replay_ast_unchanged'] is True
         and value['formats'] == list(FORMATS), 'adjacent declarations incomplete')
    ids = [name for size in SIZES for name in case_names(size)]
    need(value['case_ids'] == ids and len(ids) == 30
         and [row['fact_rows'] for row in value['sizes']] == list(SIZES), 'adjacent declaration grid changed')
    references, datasets = {}, {}
    for row in value['sizes']:
        size = row['fact_rows']
        dataset = dict(seed=0x31_2026_C8, fact_rows=size, entity_rows=11,
                       product_rows=7, relationship_rows=13, calendar_rows=5, profile='null_heavy')
        need(row['dataset'] == dataset
             and [item['case'] for item in row['references']] == case_names(size),
             'adjacent source or reference order changed')
        datasets[size] = dataset
        for item in row['references']:
            need(type(item['rows']) is int and 0 <= item['rows'] <= max(1, size),
                 'adjacent reference population changed')
            references[item['case']] = (size, item)
    rows = sum(item['rows'] for _, item in references.values()) * 15
    need(value['expected_complete_comparisons'] == 450 and value['expected_source_fixtures'] == 9
         and rows == value['expected_complete_rows'] == 143235, 'adjacent declaration totals changed')
    return references, datasets


def declaration_identity(row, size):
    name = row['case']
    short = name.removeprefix(f'adjacent_read_n{size}_')
    family, factors = COMPOSITIONS.get(short, ('grouped_aggregation' if short == 'mixed' else 'projection', {}))
    original = short if short in COMPOSITIONS else name
    record = dict(id=name, family=family, archetype=original, factors=factors,
        partition='held_out' if int(sha(original.encode())[:8], 16) % 5 == 0 else 'development',
        neighbor_of=None, relation=row['report']['public_declaration'], desired_sql=None,
        pending_reason=None, surfaces=list(SURFACES), contract=row['contract'])
    return sha(json.dumps(record, sort_keys=True, allow_nan=False).encode())


def read_work(work, size, short, fields, source_bytes):
    """Check the independent metadata-derived prediction and actual request/byte counts."""
    columns = PROJECTIONS[short]
    group_rows = 127 if size > 37 else 7
    groups = work['groups']
    need(len(groups) == max(1, (size + group_rows - 1) // group_rows)
         and [row['index'] for row in groups] == list(range(len(groups))),
         'physical read row-group coverage changed')
    requests, individual, useful = 0, 0, 0
    previous_end = 0
    for group in groups:
        chunks = group['chunks']
        need([row['column'] for row in chunks] == list(columns), 'selected physical columns changed')
        for chunk in chunks:
            need(type(chunk['start']) is int and type(chunk['bytes']) is int
                 and 0 < chunk['start'] and 0 < chunk['bytes']
                 and chunk['start'] >= previous_end
                 and chunk['start'] + chunk['bytes'] <= source_bytes, 'invalid selected compressed extent')
            previous_end = chunk['start'] + chunk['bytes']
        joins = [left['start'] + left['bytes'] == right['start']
                 for left, right in zip(chunks, chunks[1:])]
        expected_joins = {'adjacent_keys': [True, True],
                          'adjacent_numeric': [False, True, True],
                          'separated': [False, False]}[short]
        expected_requests = len(columns) - sum(expected_joins)
        need(joins == expected_joins and group['expected_requests'] == expected_requests
             and sum(row['bytes'] for row in chunks) < 4 << 20,
             'exact adjacency or combined-capacity prediction changed')
        requests += expected_requests
        individual += len(chunks)
        useful += sum(row['bytes'] for row in chunks)
    need(work['expected_requests'] == requests and work['individual_chunk_requests'] == individual
         and work['expected_bytes'] == useful, 'independent physical read totals changed')
    need(int(fields['native_source_demand_data_read_requests']) == requests
         and int(fields['native_source_demand_data_read_bytes']) == useful,
         'actual reads differ from the independent selected-extent prediction')
    need(requests == individual if short == 'separated' else requests < individual,
         'adjacency activation or separated control changed')
    return dict(requests=requests, individual_requests=individual, selected_bytes=useful)


def public_replay(bundle, value, declaration):
    references, datasets = declarations(declaration)
    need(value['schema_version'] == 'shardloom.adjacent_reads_public_acceptance.v1'
         and value['status'] == 'passed' and value['tests'] == 1 and value['test_names'] == [METHOD]
         and value['failures'] == value['errors'] == value['skipped'] == 0
         and value['failure_details'] == [] and value['posthash_verified'] is True
         and value['comparison_engine_invoked'] is False and value['performance_claim'] is False,
         'public adjacent-read acceptance incomplete')
    fixtures = {}
    for source in value['source_fixtures']:
        size = source['dataset']['fact_rows']
        key = (size, source['format'])
        need(key not in fixtures and size in datasets and source['dataset'] == datasets[size]
             and source['method'] == METHOD and source['format'] in FORMATS
             and source['layout'] == 'odd original indexes followed by reversed even indexes'
             and set(source['files']) == {'fact', 'entities', 'products'}
             and all(len(items) == 1 for items in source['files'].values()),
             'public adjacent-read fixture changed')
        fixtures[key] = {name: items[0] for name, items in source['files'].items()}
    need(set(fixtures) == {(size, form) for size in SIZES for form in FORMATS}, 'public source grid incomplete')
    grid = {(case, form, surface) for case in references for form in FORMATS for surface in SURFACES}
    coverage, metadata, complete_rows, work_checks = set(), {}, 0, []
    for row in public_parts(bundle, value):
        key = (row['case'], row['format'], row['surface'])
        need(key in grid and key not in coverage and row['method'] == METHOD,
             'public adjacent comparison coverage changed')
        coverage.add(key)
        size, reference = references[row['case']]
        short = row['case'].removeprefix(f'adjacent_read_n{size}_')
        need(fingerprint(dict(expected=row['expected'], contract=row['contract']))
             == reference['expected_and_contract_sha256'], 'pre-execution public reference changed')
        need(declaration_identity(row, size) == reference['identity'], 'executed adjacent declaration changed')
        compare(result_from_record(row['expected']), result_from_record(row['actual']), contract(row['contract']))
        report = row['report']
        need(row['rows'] == len(row['actual']['rows']) == reference['rows']
             and report['complete_consumption'] is True and report['surface'] == row['surface'],
             'public adjacent result incomplete')
        fields = report['fields']
        no_fallback(fields)
        resource_fields(fields)
        sources = fixtures[(size, row['format'])]
        required = {'fact'} | ({'entities'} if short in COMPOSITIONS else set())
        if short == 'star_join':
            required.add('products')
        need(all(sources[name]['path'] in report['sql_declaration'] for name in required),
             'public original source binding changed')
        if row['format'] == 'parquet':
            need(int(fields['native_source_demand_sources']) > 0
                 and fields['native_source_demand_prepared_input_bytes_written'] == '0'
                 and fields['native_input_end_observed'] == fields['native_input_output_ownership_detached'] == 'true',
                 'public Parquet completion or ownership changed')
        observed_work = report.get('acceptance_expected_parquet_read_work')
        if row['format'] == 'parquet' and short in PROJECTIONS:
            need(isinstance(observed_work, dict), 'independent physical read prediction missing')
            work_checks.append(read_work(observed_work, size, short, fields, sources['fact']['bytes']))
            metadata_key = (size, short)
            digest = fingerprint(observed_work)
            need(metadata.setdefault(metadata_key, digest) == digest,
                 'independent source metadata differs across public interfaces')
        else:
            need(observed_work is None, 'unexpected physical-read prediction')
        complete_rows += row['rows']
    need(coverage == grid and len(coverage) == value['positive_public_executions'] == 450
         and complete_rows == value['compared_rows'] == 143235, 'complete public totals changed')
    need(len(work_checks) == 45 and len(metadata) == 9, 'physical-read controls incomplete')
    physical = {name: sum(row[name] for row in work_checks)
                for name in ('requests', 'individual_requests', 'selected_bytes')}
    return dict(status='passed', complete_comparisons=450, complete_rows=complete_rows,
                source_fixtures=len(fixtures), pre_execution_references_verified=True,
                physical_read_controls=len(work_checks), physical_read_totals=physical)
