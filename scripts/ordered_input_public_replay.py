# SPDX-License-Identifier: Apache-2.0
"""Replay complete ordered-input public controls without importing a query engine."""
import gzip
import importlib.util
import json
from pathlib import Path

from grouped_state_public_replay import resource_fields
from native_workflow_protocol import strict_json
from query_surface.evidence import no_fallback
from query_surface.replay import contract, need, sha
from query_surface.semantics import (
    ResultContract, TypedResult, compare, result_from_record, result_record, schema_from_record,
)

FORMATS = ('parquet', 'arrow', 'vortex')
SURFACES = ('dataframe', 'typed_relation', 'python_sql', 'embedded_workflow', 'cli_sql')
TEMPLATES = ('sort_ascending', 'sort_shuffled', 'window_ordered', 'window_shuffled',
             'asof_ordered', 'asof_shuffled', 'collections_ordered', 'collections_shuffled',
             'selectors_ordered', 'selectors_shuffled')


def raw(value):
    return json.dumps(value, separators=(',', ':'), allow_nan=False).encode()


def reference_fixture():
    # This fixed, maintained fixture module contains standard-library reference
    # operations. Its PyArrow file/schema helpers are never called by replay.
    path = Path(__file__).resolve().parents[1] / 'python/tests/native_ordered_input_cases.py'
    spec = importlib.util.spec_from_file_location('ordered_input_reference_fixture', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def declarations(value):
    need((value['status'], value['native_queries_before_declaration']) in {
             ('declared_before_public_execution', 0), ('fixture_corrected_before_retry', 480)}
         and value['comparison_engine_invoked'] is False
         and value['formats'] == list(FORMATS) and value['surfaces'] == list(SURFACES)
         and value['resources'] == dict(memory_bytes=256 << 20, parallelism=2, timeout_seconds=60),
         'public declaration boundary changed')
    pairs = [(name, size) for name in TEMPLATES for size in (0, 33, 4097)]
    pairs.extend((name, 65537) for name in ('sort_ascending', 'sort_shuffled'))
    need([(row['template'], row['input_rows']) for row in value['cases']] == pairs,
         'public declaration grid changed')
    fixture, references = reference_fixture(), {}
    for row in value['cases']:
        template, size = row['template'], row['input_rows']
        label = f'{template}_{size}'
        need(row['label'] == label and label not in references, 'public case identity changed')
        generated = TypedResult(schema_from_record(row['schema']),
            fixture.reference_rows(template, fixture.records(template, size)))
        need(sha(raw(result_record(generated))) == row['reference_sha256']
             and len(generated.rows) == row['output_rows'], 'independent public reference changed')
        references[label] = (row, generated)
    need(len(references) == 32 and value['complete_comparisons'] == 480
         and sum(row['output_rows'] for row, _ in references.values()) * 15
         == value['expected_complete_rows'] == 2338650, 'public declaration totals changed')
    return fixture, references


def public_replay(bundle, value, declaration):
    fixture, references = declarations(declaration)
    need(value['schema_version'] == 1 and value['status'] == 'passed'
         and value['errors'] == value['failures'] == value['skipped'] == []
         and value['posthash_unchanged'] is True and value['full43_executed'] is False
         and value['comparison_engine_invoked'] is False, 'public controls incomplete')
    need(strict_json(bundle.identity(value['declaration'])) == declaration,
         'executed public declaration changed')
    for item, snapshot in zip(declaration['source_identities'], declaration['source_snapshots'], strict=True):
        need(bundle.identity(item) == bundle.identity(snapshot), 'public source snapshot changed')
    need(value['source_snapshots'] == declaration['source_snapshots'], 'public sources changed')
    need(len(value['prehash']) > 2000, 'public source/runtime inventory incomplete')
    for path, item in value['prehash'].items():
        need(path == item['path'], 'public prehash identity changed')
        # Runtime payloads may be retired after acceptance; their immutable
        # product hashes remain in the runtime and package manifests.
        if Path(path).name not in {'shardloom', '_native.abi3.so'}:
            bundle.identity(item)
    fixtures = {}
    for source in value['sources']:
        form = source['format']
        suffix = '-' + form
        need(form in FORMATS and source['fixture'].endswith(suffix), 'public format changed')
        template_size = source['fixture'][:-len(suffix)]
        template, size_text = template_size.rsplit('-', 1)
        label = f'{template}_{int(size_text)}'
        key = (label, form)
        need(label in references and key not in fixtures, 'public source fixture changed')
        expected_names = {'left', 'right'} if template.startswith('asof_') else {'input'}
        need(set(source['inputs']) == expected_names, 'public input binding changed')
        paths = {}
        for name, item in source['inputs'].items():
            need(item['rows'] == references[label][0]['input_rows'] and len(item['files']) == 1,
                 'public source population changed')
            identity = item['files'][0]
            path = Path(identity['path'])
            need(path.name == name + '.' + form and path.parent.name == source['fixture']
                 and type(identity['bytes']) is int and identity['bytes'] > 0
                 and len(identity['sha256']) == 64, 'public source file identity changed')
            paths[name] = identity['path']
        fixtures[key] = fixture.statement(template, paths)
    need(set(fixtures) == {(label, form) for label in references for form in FORMATS}
         and len(value['sources']) == 96, 'public fixture coverage incomplete')
    grid = [(label, form, surface) for label in references for form in FORMATS for surface in SURFACES]
    need(value['coverage'] == [list(row) for row in grid] and len(value['parts']) == 480,
         'public comparison coverage changed')
    rows, paths = 0, set()
    for item, key in zip(value['parts'], grid, strict=True):
        need((item['label'], item['format'], item['surface']) == key and item['path'] not in paths
             and type(item['bytes']) is int and 0 < item['bytes'] <= 8 << 20
             and type(item['raw_bytes']) is int and 0 < item['raw_bytes'] <= 32 << 20,
             'public result part identity changed')
        paths.add(item['path'])
        data = gzip.decompress(bundle.identity(item))
        need(len(data) == item['raw_bytes'] and sha(data) == item['raw_sha256'], 'public result part changed')
        record = strict_json(data)
        label, form, surface = key
        reference, generated = references[label]
        need((record['label'], record['format'], record['surface']) == key
             and sha(raw(record['expected'])) == reference['reference_sha256'],
             'public comparison declaration changed')
        expected, actual = result_from_record(record['expected']), result_from_record(record['actual'])
        wanted = ResultContract(generated.schema, ordered=True)
        need(contract(record['contract']) == wanted, 'public result contract changed')
        compare(generated, expected, wanted)
        compare(generated, actual, wanted)
        report = record['report']
        need(report['surface'] == surface and report['case'] == label
             and report['complete_consumption'] is True
             and report['source_session_released_before_export'] is True
             and report['sql_declaration'] == fixtures[(label, form)]
             and record['rows'] == item['rows'] == len(actual.rows) == reference['output_rows'],
             'public execution or complete consumption changed')
        no_fallback(report['fields'])
        resource_fields(report['fields'])
        if surface == 'cli_sql' and reference['output_rows'] > 65536:
            need(report['row_export'] == dict(method='arrow_ipc_file', complete_rows=reference['output_rows'],
                                             source_client_closed_before_read=True),
                 'large CLI result did not use complete post-close file export')
        rows += len(actual.rows)
    need(rows == value['complete_rows'] == 2338650 and value['comparisons'] == 480,
         'public complete-result totals changed')
    return dict(status='passed', complete_comparisons=480, complete_rows=rows,
                source_fixtures=96, independent_references_regenerated=32,
                source_sessions_released=True)
