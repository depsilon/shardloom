# SPDX-License-Identifier: Apache-2.0
"""Replay the immutable r17/r8 local I/O collection without original files or engines."""
from collections import Counter, defaultdict
import hashlib
import json
import math
from pathlib import Path
import statistics

from local_io_replay_values import compare, compare_wire, ipc, read_output, translated
from native_workflow_protocol import strict_json
from parquet_demand_completion_replay import replay as replay_347
from query_surface.evidence import no_fallback
from query_surface.replay import MAX_FILE, bounded_bytes, need, sha
from query_timing_report import query_phase_timings
from shared_performance_bundle import Bundle

COLLECTION_SHA256 = '677b350b8322e3a8112417e9b4aaff37b8826fe6f669e4734a5fe112ece7f738'
DECLARATION_SHA256 = '45cf7545e45993aa54d2b6d024de08bbb8b4418a20b33adcc3a93195b0cfcd8a'
COUNTS = {'smoke': 205, 'correctness': 4555, 'fidelity': 400, 'timing': 4455, 'lifecycle': 18}
SURFACES = {'cli_sql', 'embedded_workflow', 'python_sql', 'dataframe', 'typed_relation'}
OUTPUTS = {'vortex', 'parquet', 'arrow-ipc', 'orc', 'avro', 'csv', 'json', 'jsonl'}


class Collection:
    """Original paths are identifiers; only the supplied packet is opened."""

    def __init__(self, folder, progress=lambda message: None):
        folder = Path(folder)
        need(folder.is_dir() and not folder.is_symlink(), 'invalid collection directory')
        raw = bounded_bytes(folder / 'manifest.json', MAX_FILE)
        need(sha(raw) == COLLECTION_SHA256, 'collection identity differs')
        index = strict_json(raw)
        need(index['schema_version'] == 'shardloom.local_io_portable_collection.v1'
             and [row['name'] for row in index['bundles']] == ['metadata', 'values', 'binary', 'text'],
             'collection bundle declarations differ')
        need({path.name for path in folder.iterdir()} - {'acceptance.json'}
             == {'manifest.json', 'metadata', 'values', 'binary', 'text'}, 'extra collection entries')
        self.bundles, self.paths = [], {}
        for row in index['bundles']:
            item = row['manifest']
            need(item['path'] == row['name'] + '/manifest.json', 'unsafe bundle location')
            data = bounded_bytes(folder / item['path'], MAX_FILE)
            need((len(data), sha(data)) == (item['bytes'], item['sha256']), 'bundle manifest differs')
            bundle = Bundle(folder / row['name'])
            need(bundle.index['evidence']['archive_code_execution'] is False, 'archive code is not inert')
            self.bundles.append(bundle)
            for path in bundle.paths:
                need(path not in self.paths, 'ambiguous path across bundles')
                self.paths[path] = bundle
            progress('verified bundle ' + row['name'])
        self.declaration = self.linked(index['declaration'])
        need(index['declaration']['sha256'] == DECLARATION_SHA256, 'declaration anchor differs')

    def raw(self, path, digest=None):
        need(path in self.paths, 'missing archived path: ' + str(path))
        return self.paths[path].raw(path, digest)

    def identity(self, item):
        need(item['path'] in self.paths, 'missing linked archived path')
        return self.paths[item['path']].identity(item)

    def linked(self, item):
        return strict_json(self.identity(item))


def same(actual, expected):
    """Float aggregates tolerate only arithmetic-rounding noise, never new clocks."""
    if type(actual) is float and type(expected) in (int, float):
        return math.isfinite(actual) and math.isfinite(expected) and math.isclose(actual, expected, rel_tol=0, abs_tol=1e-9)
    if isinstance(actual, dict) and isinstance(expected, dict):
        return actual.keys() == expected.keys() and all(same(actual[key], expected[key]) for key in actual)
    if isinstance(actual, list) and isinstance(expected, list):
        return len(actual) == len(expected) and all(same(a, b) for a, b in zip(actual, expected))
    return type(actual) is type(expected) and actual == expected


def complete_gate(gate):
    need(gate['status'] == 'passed' and gate['returncode'] == 0
         and gate['process_group_drained'] is True and gate['direct_child_reaped'] is True
         and gate['timed_out'] is False and gate['interrupted_signal'] is None,
         'required original process did not complete')


def safe_fallback(value):
    need(isinstance(value, dict) and value.get('attempted') is False
         and value.get('allowed') is False and value.get('engine', 'missing') is None,
         'unsafe or absent fallback contract')


def refusal(record):
    need(record.get('expected_refusal') is True, 'expected refusal was not observed')
    evidence = record['refusal_evidence']
    if 'envelope' in evidence:
        envelope = evidence['envelope']
        need(envelope['status'] == 'error', 'refusal returned success')
        safe_fallback(envelope.get('fallback'))
        diagnostics = envelope['diagnostics']
    else:
        need(evidence['external_engine_invoked'] is False, 'refusal used an external engine')
        diagnostics = [evidence['diagnostic']]
    need(any(d.get('code') == 'SL_INVALID_INPUT' and 'i64::MAX' in d.get('reason', '')
             for d in diagnostics), 'unsigned refusal diagnostic differs')
    for diagnostic in diagnostics:
        safe_fallback(diagnostic.get('fallback'))
    need('actual' not in record and 'readback' not in record and 'follow_on' not in record,
         'refused write claims successful output')


def execution(record, source, protocol):
    fields = record['fields']
    no_fallback(fields)
    for key, expected in (('execution_resource_admitted_memory_bytes', protocol['memory_bytes']),
                          ('execution_resource_admitted_max_parallelism', protocol['max_parallelism'])):
        need(fields.get(key) == str(expected), 'caller resource admission differs: ' + key)
    if source['format'] != 'vortex':
        need(fields.get('native_source_demand_prepared_input_bytes_written') == '0',
             'direct source depends on a prepared artifact')
    targets = record['targets']
    if targets:
        for key, expected in (('native_vortex_result_export_execution_count', '1'),
                              ('native_vortex_result_export_target_count', str(len(targets))),
                              ('native_vortex_result_export_all_targets_committed', 'true')):
            need(fields.get(key) == expected, 'writer completion differs: ' + key)
    else:
        need(any(key.endswith('native_io_certified') and value == 'true' for key, value in fields.items()),
             'collection has no complete native I/O certificate')
    timings = query_phase_timings(fields, record['process_wall_seconds'], allow_missing=bool(targets))
    need(all(record.get(key) == value for key, value in timings.items()), 'saved clock differs from native evidence')
    if record['case'] == 'count' and source['format'] != 'vortex':
        metadata = source['format'] == 'parquet' and source.get('collection') not in {'hive', 'union_by_name'}
        need(fields.get('native_source_metadata_count_sources') == ('1' if metadata else '0'),
             'source count strategy differs')
        if metadata:
            for key in ('native_source_demand_projected_columns', 'native_source_demand_physical_leaf_columns',
                        'native_source_demand_rows_decoded', 'native_source_demand_row_groups_read',
                        'native_source_demand_data_read_bytes', 'native_source_demand_data_read_requests',
                        'native_input_batches', 'native_input_batch_rows'):
                need(fields.get(key) == '0', 'metadata count performed payload work')
            need(fields.get('native_input_end_observed') == 'true', 'metadata source incomplete')


class Values:
    def __init__(self, collection, export):
        self.collection, self.export = collection, export
        self.pairs, self.outputs = {}, {}
        self.comparisons = self.rows = self.readbacks = 0
        self.vortex_observations = []

    def pair(self, actual, expected):
        key = (actual['sha256'], expected['sha256'])
        # Hash identities are still checked on every declared link.
        actual_bytes, expected_bytes = self.collection.identity(actual), self.collection.identity(expected)
        if key not in self.pairs:
            self.pairs[key] = compare(ipc(actual_bytes), ipc(expected_bytes))
        count = self.pairs[key]
        self.comparisons += 1
        self.rows += count
        return count

    def readback(self, item, expected, fmt, *, rich, observation):
        c, output = self.collection, item['output']
        data = c.identity(output)
        reference_bytes = c.identity(expected)
        key = (fmt, output['sha256'], expected['sha256'], rich)
        report = item['report']
        if fmt == 'vortex':
            need(all(report.get(name) is True for name in (
                'complete_value_equality', 'native_schema_equal', 'source_generations_validated')),
                'original Vortex comparison incomplete')
            export_key = output['sha256'] + '-' + expected['sha256']
            need(export_key in self.export['reports'], 'Vortex decode export missing')
            exported = self.export['reports'][export_key]
            self.pair(exported['actual'], expected)
            need(exported['compared_rows'] == report['compared_rows']
                 and exported['compared_columns'] == report['compared_columns'], 'Vortex decode coverage differs')
            self.vortex_observations.append(dict(observation=observation, output=output,
                                                 expected=expected, export_key=export_key))
        else:
            wire = rich and fmt in {'csv', 'json', 'jsonl'}
            if wire:
                need(report['complete_wire_values_and_order_equal'] is True
                     and report['logical_schema_embedded'] is False
                     and report['exact_typed_roundtrip_claim'] is False, 'rich wire scope differs')
            else:
                need(report['complete_values_schema_and_order_equal'] is True, 'format readback incomplete')
                need(report['format'] == fmt and all(report[name] == output[name]
                     for name in ('path', 'bytes', 'sha256')), 'format readback identity differs')
            # Observations may share identical bytes and an independent decode.
            # Each must still carry its own successful, correctly bound report.
            if key not in self.outputs:
                reference = ipc(reference_bytes)
                if wire:
                    compare_wire(data, fmt, reference)
                else:
                    reference = translated(reference, fmt) if rich else reference
                    compare(read_output(data, fmt, reference.schema), reference)
                self.outputs[key] = reference.num_rows
            if not wire:
                need(report['rows'] == self.outputs[key], 'format readback row count differs')
        self.readbacks += 1


def lifecycle(c, values, record, item, source, protocol):
    prefix = 'public_workflow_preparation_'
    for name, hit in (('prepare', 'false'), ('reuse', 'true')):
        proof = record[name]
        need(proof['envelope']['status'] == 'success', 'preparation/reuse failed')
        safe_fallback(proof['envelope']['fallback'])
        no_fallback(proof['fields'])
        need(proof['fields'].get(prefix + 'prepared_state_reuse_hit') == hit, 'preparation reuse state differs')
        need(proof['fields']['execution_resource_admitted_memory_bytes'] == str(protocol['memory_bytes'])
             and proof['fields']['execution_resource_admitted_max_parallelism'] == str(protocol['max_parallelism']),
             'preparation resources differ')
    need(record['prepare']['fields'].get(prefix + 'input_row_count') == str(len(source['expected_rows'])),
         'preparation did not validate the complete source')
    c.identity(record['source_before'])
    c.identity(record['prepared_before']['identity'])
    child = record['separate_process']
    need(child['status'] == 'passed' and child['pid'] != child['parent_pid']
         and record['separate_process_call']['returncode'] == 0, 'separate-process reopening failed')
    need(strict_json(record['separate_process_call']['stdout']) == {
        'status': 'passed', 'pid': child['pid'], 'queries': 10}, 'separate-process stdout differs')
    for queries, repetitions in ((record['queries'], 2), (child['queries'], 1)):
        identities = [(row['surface'], row['case'], row['repetition']) for row in queries]
        wanted = {(s, case, rep) for s in SURFACES for case in ('broad', 'aggregate') for rep in range(repetitions)}
        need(len(identities) == len(wanted) and set(identities) == wanted, 'lifecycle query grid incomplete')
        for query in queries:
            execution(query, {'format': 'vortex'}, protocol)
            values.pair(query['actual'], query['expected'])
    broad = [query['expected'] for query in record['queries'] if query['case'] == 'broad']
    need(len({(row['bytes'], row['sha256']) for row in broad}) == 1, 'prepared broad references disagree')
    values.readback(record['independent_readback'], broad[0], 'vortex', rich=False, observation=item)
    replacement = record['source_replacement']
    need(replacement['before'] != replacement['after'] and replacement['identity'] == record['source_before'],
         'generation replacement control differs')
    rejected = record['changed_source_rejection']
    need(rejected['envelope']['status'] == 'error', 'changed source accepted')
    safe_fallback(rejected['envelope']['fallback'])
    need('existing target has no matching source binding' in json.dumps(rejected).lower(),
         'changed-source refusal diagnostic differs')
    if record['preparation'] == 'cli_prepare':
        need(rejected['returncode'] != 0, 'changed-source CLI succeeded')


def stage_replay(c, name, declaration, manifest, values):
    summary, jobs = c.linked(declaration['summary']), c.linked(declaration['jobs'])
    size = COUNTS[name]
    need(summary['status'] == 'passed' and summary['completed'] == summary['required'] == size
         and summary['failures'] == [] and len(jobs) == len(declaration['observations']) == size,
         'stage coverage incomplete: ' + name)
    sources = {s['id']: s for group in ('small_inputs', 'bulk_inputs', 'empty_inputs', 'fidelity_inputs')
               for s in manifest[group]}
    digest, length, timings = hashlib.sha256(), 0, []
    counts = Counter()
    for ordinal, (job, item) in enumerate(zip(jobs, declaration['observations'], strict=True)):
        record = c.linked(item)
        need(record['status'] == 'passed' and record['ordinal'] == ordinal, 'failed or reordered observation')
        for key, value in job.items():
            if key in {'outputs', 'refusal'}:
                continue
            need(record.get(key) == value, 'job/observation mismatch: ' + key)
        wire = (json.dumps(record, allow_nan=False, ensure_ascii=False) + '\n').encode()
        digest.update(wire)
        length += len(wire)
        source = sources[record['source']]
        if name == 'lifecycle':
            lifecycle(c, values, record, item, source, manifest['protocol'])
            counts['lifecycle'] += 1
            continue
        counts[record['surface']] += 1
        if job.get('refusal'):
            refusal(record)
            counts['expected_refusals'] += 1
            continue
        need(not record.get('expected_refusal'), 'undeclared refusal')
        execution(record, source, manifest['protocol'])
        targets = record['targets']
        need([fmt for fmt, _ in targets] == job.get('outputs', []), 'output targets differ')
        if not targets:
            need('actual' in record, 'collection omitted complete values')
            values.pair(record['actual'], record['expected'])
            counts['collections'] += 1
        else:
            need(all(fmt in OUTPUTS for fmt, _ in targets), 'unknown output format')
            readbacks = [record['readback']] if name == 'fidelity' else record['readbacks']
            follow_on = [record['follow_on']] if name == 'fidelity' else record['follow_on']
            need(len(readbacks) == len(follow_on) == len(targets), 'output/follow-on coverage incomplete')
            for (fmt, path), back, follow in zip(targets, readbacks, follow_on, strict=True):
                need(back['output']['path'] == path, 'readback bound to another output')
                values.readback(back, record['expected'], fmt, rich=name == 'fidelity', observation=item)
                execution(follow, {'format': fmt}, manifest['protocol'])
                values.pair(follow['actual'], follow['expected'])
            counts['writes'] += 1
            counts['outputs'] += len(targets)
        if name == 'timing':
            timings.append(record)
    combined = declaration['combined_jsonl']
    if combined is not None:
        need((length, digest.hexdigest()) == (combined['bytes'], combined['sha256']),
             'original combined observation stream differs')
    if name != 'lifecycle':
        need(set(SURFACES) <= counts.keys(), 'public surface omitted')
    return dict(observations=size, coverage=dict(counts), original_jsonl_reconstructed=combined is not None), timings


def timing_replay(records, protocol, saved):
    groups, families = defaultdict(lambda: defaultdict(list)), defaultdict(lambda: defaultdict(list))
    for record in records:
        target = record['targets'][0][0] if record['targets'] else None
        groups[(record['kind'], record['source'], record['case'], target)][record['surface']].append(record)

    def gap(values, cells=1):
        fastest = min(values, key=values.get)
        threshold = max(protocol['absolute_material_gap_seconds'] * cells,
                        protocol['relative_material_gap'] * values[fastest])
        return dict(values=values, fastest=fastest, threshold_seconds=threshold,
                    gaps=[dict(surface=s, gap_seconds=v-values[fastest]) for s, v in values.items()
                          if v-values[fastest] > threshold])

    cases = []
    for (kind, source, case, target), surfaces in groups.items():
        need(set(surfaces) == SURFACES, 'timing surface omitted')
        metric = 'engine_execution_seconds' if kind == 'collect' else 'complete_write_api_seconds'
        medians = {}
        for surface, rows in surfaces.items():
            need(len(rows) == protocol['repetitions'] == 3
                 and {row['repetition'] for row in rows} == {0, 1, 2}, 'timing repetitions differ')
            numbers = [row[metric] for row in rows]
            need(all(type(v) in (int, float) and math.isfinite(v) and v >= 0 for v in numbers), 'invalid saved clock')
            medians[surface] = statistics.median(numbers)
            families[(kind, case if kind == 'collect' else target, metric)][surface].append(medians[surface])
        cases.append(dict(kind=kind, source=source, case=case, target=target, metric=metric,
            engine_clock_available=kind == 'collect' or all(row['engine_timing_reported'] for rows in surfaces.values() for row in rows),
            **gap(medians)))
    family_rows = []
    for (kind, family, metric), values in families.items():
        counts = {len(v) for v in values.values()}
        need(len(counts) == 1, 'timing family cells differ')
        cells = counts.pop()
        family_rows.append(dict(kind=kind, family=family, metric=metric, cases=cells,
            aggregation='sum_of_per_case_medians_not_production_workload_weighting',
            **gap({s: math.fsum(v) for s, v in values.items()}, cells)))
    need(same(cases, saved['cases']) and same(family_rows, saved['families']), 'original timing screen differs')
    need(sum(bool(row['gaps']) for row in cases) == saved['material_case_gaps'] == 216
         and sum(bool(row['gaps']) for row in family_rows) == saved['material_family_gaps'] == 10,
         'diagnostic gap disposition differs')
    return dict(cases=len(cases), families=len(family_rows), observations=len(records),
                material_case_gaps=216, material_family_gaps=10, disposition='diagnostic_only_by_recorded_maintainer_decision')


def source_bindings(c, manifest):
    d = c.declaration
    external = {item['path']: item for item in d['external_artifacts']}
    need(len(external) == len(d['external_artifacts']), 'duplicate external identity')

    def bound(item):
        if item['path'] in c.paths:
            c.identity(item)
        else:
            declared = external.get(item['path'], {})
            need(all(declared.get(key) == item[key] for key in ('path', 'bytes', 'sha256')),
                 'unrepresented external input or executable: ' + item['path'])

    runtime = c.linked(d['runtime'])
    need(manifest['runtime'] == d['runtime'] and runtime['profile'] == 'release'
         and runtime['features'] == ['release-user-surfaces'], 'campaign runtime binding differs')
    root = d['artifact_root']
    build_bytes = c.raw(root + '/' + runtime['build_receipt'])
    need(sha(build_bytes) == runtime['build_receipt_sha256'], 'runtime build receipt differs')
    build = strict_json(build_bytes)
    complete_gate(build)
    need(build['head'] == runtime['source_revision'], 'runtime source revision differs')
    report = c.linked(d['source_verification'])
    need(report['status'] == 'passed' and report['manifest'] == d['campaign']
         and report['total_observations'] == sum(COUNTS.values()), 'source verification incomplete')
    need(set(report['product_sources']) == set(runtime['source_snapshot']), 'compiled source coverage differs')
    for name, item in report['product_sources'].items():
        need(item['path'] == d['source_root'] + '/' + name
             and {key: item[key] for key in ('bytes', 'sha256')} == runtime['source_snapshot'][name],
             'compiled source identity differs')
        c.identity(item)
    for item in report['frozen_paths'].values():
        bound(item)
    for item in manifest['products']:
        bound(item)
        need({key: item[key] for key in ('bytes', 'sha256')} == runtime['files'][Path(item['path']).name],
             'campaign executable differs from built runtime')
    for before, after in zip(manifest['code'], manifest['code_snapshots'], strict=True):
        need(c.identity(before) == c.identity(after), 'harness snapshot differs')
    for item in manifest['package_files'] + manifest['oracle_artifacts'] + [manifest['reader']]:
        bound(item)
    for key in ('lifecycle_mapping', 'input_fixture_origin', 'harness_evidence_update'):
        c.identity(manifest[key])
    for group, expected_count in (('small_inputs', 65), ('bulk_inputs', 11),
                                  ('empty_inputs', 10), ('fidelity_inputs', 9)):
        need(len(manifest[group]) == expected_count, 'input fixture grid differs')
        for source in manifest[group]:
            for item in source['physical_files']:
                bound(item)
            if source.get('expected_path'):
                need(source['expected_path'] in c.paths, 'complete source reference missing')
    receipts, outcomes = {}, Counter()
    for item in d['receipts']:
        gate = c.linked(item['receipt'])
        need(item['status'] == gate['status'] in {'passed', 'failed'}, 'historical outcome changed')
        need(sha(c.identity(item['log'])) == gate['log_sha256'], 'historical log differs')
        need(item['receipt']['path'] not in receipts, 'duplicate process receipt')
        if gate['status'] == 'passed':
            complete_gate(gate)
        receipts[item['receipt']['path']] = gate
        outcomes[gate['status']] += 1
    history = c.linked(report['failed_attempt_inventory'])
    history_counts = Counter()
    for item in history['all_receipts_through208']:
        gate = receipts[item['path']]
        need(gate['status'] == item['status'] and gate['returncode'] == item['returncode']
             and sha(c.raw(item['path'])) == item['receipt_sha256'], 'failed-attempt inventory differs')
        history_counts[gate['status']] += 1
    need(dict(history_counts) == report['history_counts'], 'historical outcome coverage differs')
    for name, stage in report['stages'].items():
        need(stage['summary'] == d['stages'][name]['summary'] and stage['completed'] == COUNTS[name],
             'stage source verification differs')
        gate = c.linked(stage['receipt'])
        complete_gate(gate)
        need(sha(c.identity(stage['log'])) == gate['log_sha256'], 'stage log differs')
    package = strict_json(c.raw(root + '/installed-wheel-r7/report.json'))
    need(package['status'] == package['uninstall_status'] == 'passed'
         and package['publication_attempted'] is False and package['installed_load']['isolated_install'] is True,
         'installed package acceptance incomplete')
    need(package['integration'] == dict(tests=289, failures=0, errors=0, skipped=0, status='passed'),
         'installed package coverage differs')
    need(package['integrity']['cli_sha256'] == runtime['files']['shardloom']['sha256']
         and package['integrity']['native_library_sha256'] == runtime['files']['_native.abi3.so']['sha256'],
         'installed package used another runtime')
    for step in package['steps']:
        need(step['returncode'] == 0
             and sha(c.raw(root + '/installed-wheel-r7/' + step['log'])) == step['log_sha256'],
             'package step differs')
    provider = strict_json(c.raw(root + '/provider-formats-r6/report.json'))
    need(provider['status'] == 'passed' and provider['group'] == 'formats', 'format provider checks failed')
    for step in provider['commands']:
        need(step['returncode'] == 0
             and sha(c.raw(root + '/provider-formats-r6/' + step['log'])) == step['log_sha256'],
             'provider step differs')
    return dict(compiled_sources=len(runtime['source_snapshot']), external_artifacts=len(external),
                process_receipts=len(receipts), original_outcomes=dict(outcomes),
                package_tests=289, source_revision=runtime['source_revision'])


def vortex_exports(c, values):
    export = values.export
    d = c.linked(export['declaration'])
    need(export['status'] == 'passed' and export['engine_queries'] == 0
         and export['original_observations_unchanged'] is True
         and d['manifest'] == c.declaration['campaign'], 'independent decode export binding differs')
    order = lambda item: (item['observation']['path'], item['output']['path'])
    need(sorted(values.vortex_observations, key=order) == sorted(d['mappings'], key=order)
         and len(d['mappings']) == export['covered_observations'] == 348, 'Vortex observation coverage differs')
    need(set(export['reports']) == set(d['unique_readbacks'])
         and len(export['reports']) == export['unique_decodes'] == 33, 'Vortex decode coverage differs')
    need(d['library'] in c.declaration['external_artifacts'], 'independent provider identity absent')
    c.identity(d['verifier'])
    for key, report in export['reports'].items():
        source = d['unique_readbacks'][key]
        need(key == source['output']['sha256'] + '-' + source['expected']['sha256']
             and source['expected'] == report['expected'], 'independent decode source differs')
        c.identity(source['output'])
        need(report['status'] == 'passed' and report['provider'] == 'upstream_vortex_native_file_reader'
             and report['provider_version'] == '0.90.0'
             and report['provider_library_sha256'] == d['library']['sha256'], 'independent provider differs')
        need(all(report[name] is True for name in ('complete_value_equality', 'native_schema_equal',
                                                  'source_generations_validated')),
             'independent decode incomplete')
        expected = ipc(c.identity(source['expected']))
        need(report['compared_rows'] == expected.num_rows and report['compared_columns'] == expected.num_columns
             and report['source_generation']['bytes'] == source['output']['bytes'], 'independent decode dimensions differ')
    return dict(observations=348, saved_independent_decodes=33, new_vortex_decodes=0)


def regressions(c):
    reports = []
    for root in c.declaration['original_regression_roots']:
        result = replay_347(c.raw, root)
        saved = strict_json(c.raw(root + '/disposition.json'))
        need(same(result, {key: saved[key] for key in result}), 'original regression replay differs')
        need(result['benefit_gate_passed'] is False and saved['accepted_baseline'] is False
             and saved['decision'] == 'drop_no_complete_engine_gain', 'original strict-gain failure changed')
        reports.append({key: result[key] for key in ('complete_comparisons', 'complete_rows',
                       'baseline_engine_seconds', 'candidate_engine_seconds', 'benefit_gate_passed')})
    need(len(reports) == 3, 'historical candidate coverage differs')
    tradeoff, decision = c.linked(c.declaration['scoped_tradeoff']), c.linked(c.declaration['decision'])
    need(tradeoff['runtime'] == c.declaration['runtime'] and tradeoff['original_benefit_gate_passed'] is False
         and tradeoff['original_decision'] == 'drop_no_complete_engine_gain'
         and tradeoff['performance_gain_claim'] is False and tradeoff['global_binary_performance_rule_changed'] is False
         and tradeoff['preserve_original_protocol_and_observations'] is True, 'scoped tradeoff changed')
    need(decision['prior_scoped_tradeoff'] == c.declaration['scoped_tradeoff']
         and decision['performance_optimization_deferred_until_both_complete'] is True
         and decision['engine_runtime_changed'] is False and decision['new_query_executions'] == 0,
         'later diagnostic-only disposition changed')
    return reports


def verify(c, progress=lambda message: None):
    manifest = c.linked(c.declaration['campaign'])
    need(manifest['required_jobs'] == COUNTS and set(c.declaration['stages']) == set(COUNTS),
         'campaign stages differ')
    need(manifest['protocol']['memory_bytes'] == 512 << 20
         and manifest['protocol']['max_parallelism'] == 2
         and set(manifest['protocol']['surfaces']) == SURFACES, 'campaign resources or surfaces differ')
    bindings = source_bindings(c, manifest)
    progress('verified source, runtime, package and process bindings')
    values = Values(c, c.linked(c.declaration['vortex_export']))
    stages, timing = {}, []
    for name in COUNTS:
        stages[name], records = stage_replay(c, name, c.declaration['stages'][name], manifest, values)
        timing.extend(records)
        progress('replayed ' + name + ': ' + str(COUNTS[name]) + ' observations')
    vortex = vortex_exports(c, values)
    screen = timing_replay(timing, manifest['protocol']['timing'], c.linked(c.declaration['diagnostic_screen']))
    comparison = regressions(c)
    progress('replayed original timing screen and three complete 347-case candidates')
    return dict(schema_version='shardloom.local_io_saved_replay.v1', status='passed',
        collection_sha256=COLLECTION_SHA256, declaration_sha256=DECLARATION_SHA256,
        observations=sum(COUNTS.values()), stages=stages, bindings=bindings,
        saved_value_comparisons=values.comparisons, saved_value_rows=values.rows,
        unique_arrow_pairs=len(values.pairs), output_readbacks=values.readbacks,
        unique_compatibility_decodes=len(values.outputs), vortex=vortex,
        diagnostic_timing_screen=screen, historical_347_candidates=comparison,
        new_engine_queries=0, archived_code_executed=False,
        limitations=[
            'Saved observations are replayed; no new performance measurement or causal claim.',
            'Original inputs and executable/provider binaries remain external hash-bound objects.',
            'Vortex checks replay saved independent decodes; compatibility outputs are decoded again.',
            'Rich text checks wire values without claiming embedded logical types or typed roundtrip.',
            'Content hashes establish consistency, not signed provenance.',
        ])
