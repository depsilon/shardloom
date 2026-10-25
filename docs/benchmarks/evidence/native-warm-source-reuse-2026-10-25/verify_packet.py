#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Replay the finite warm-source decision from saved values, never query code."""
from __future__ import annotations

import argparse
import ast
import copy
import gzip
import hashlib
import importlib
import io
import json
import math
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys
import tarfile
import tempfile

ORIGINAL = Path('/Users/dylan/LocalData/shardloom/native-warm-reuse-20261025')
PROVIDER = Path('/Users/dylan/LocalData/shardloom/vortex-provider-refresh-20261024')
ORIGINAL_REPO = Path('/Users/dylan/.codex/worktrees/public-docs-api/shardloom-local-repo')
BASE = '82d9732eb7652671a01b0f223cf62c63f01ffe47'
REFERENCE_SHA = 'fc5780da415dc33bd06ed0879db07fc040db5dd419e3c37ecde7cd40a315696b'
TEST_RESULT = re.compile(r'^test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;', re.M)


def require(value, message):
    if not value:
        raise ValueError(message)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def strict(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, 'duplicate JSON key')
            result[key] = value
        return result

    def number(text):
        value = float(text)
        require(math.isfinite(value), 'nonfinite JSON number')
        return value

    def reject(text):
        raise ValueError('nonfinite JSON constant: ' + text)
    return json.loads(raw, object_pairs_hook=unique, parse_float=number, parse_constant=reject)


class Packet:
    def __init__(self, root):
        self.manifest = strict((root / 'manifest.json').read_bytes())
        manifest = self.manifest
        require(manifest['schema'] == 'shardloom.warm_source.packet.v1', 'packet schema')
        require(manifest['decision'] == 'rejected_and_removed', 'packet decision')
        require(digest(Path(__file__).read_bytes()) == manifest['verifier_sha256'], 'verifier identity')
        self.files = {}
        expected = {}
        for row in manifest['files']:
            require(row['path'] not in self.files, 'duplicate original path')
            require(row['member'] == 'objects/' + row['sha256'], 'unsafe member')
            self.files[row['path']] = row
            identity = (row['bytes'], row['sha256'])
            require(expected.setdefault(row['member'], identity) == identity, 'inconsistent object')
        require(sum(size for size, _ in expected.values()) < 256 * (1 << 20), 'packet expansion ceiling')
        archive = manifest['archive']
        require(Path(archive['path']).name == archive['path'], 'unsafe archive path')
        packed = (root / archive['path']).read_bytes()
        require((len(packed), digest(packed)) == (archive['bytes'], archive['sha256']), 'archive identity')
        self.objects = {}
        with tarfile.open(fileobj=io.BytesIO(packed), mode='r:xz') as stream:
            for member in stream:
                require(member.name in expected and member.name not in self.objects, 'unexpected archive member')
                size, sha = expected[member.name]
                require(member.isfile() and member.size == size, 'archive member type or size')
                raw = stream.extractfile(member).read()
                require((len(raw), digest(raw)) == (size, sha), 'archive member identity')
                self.objects[member.name] = raw
        require(set(self.objects) == set(expected), 'missing archived objects')
        selection_raw = self.raw(ORIGINAL / 'evidence-selection-r1.json')
        require(digest(selection_raw) == manifest['selection_sha256'], 'selection inventory identity')
        selection = strict(selection_raw)
        require(not selection['missing_inputs'] and not selection['stale_or_mismatched_references'], 'incomplete evidence selection')
        selected = {row['path']: (row['bytes'], row['sha256']) for row in selection['files']}
        additions = {row['path']: (row['bytes'], row['sha256']) for row in manifest['primary_additions']}
        require(not selected.keys() & additions.keys(), 'duplicate selection additions')
        require({name: (row['bytes'], row['sha256']) for name, row in self.files.items()} ==
                {**selected, **additions}, 'selected evidence coverage changed')

    def raw(self, path):
        row = self.files[str(path)]
        return self.objects[row['member']]

    def read(self, path):
        raw = self.raw(path)
        return strict(gzip.decompress(raw) if str(path).endswith('.gz') else raw)

    def linked(self, reference):
        raw = self.raw(reference['path'])
        require(digest(raw) == reference['sha256'], 'linked identity: ' + reference['path'])
        require('bytes' not in reference or len(raw) == reference['bytes'], 'linked size')
        return raw


def trusted_helpers(packet, repository):
    for relative, sha in packet.manifest['trusted_helpers'].items():
        require(not Path(relative).is_absolute() and '..' not in Path(relative).parts, 'unsafe helper path')
        raw = (repository / relative).read_bytes()
        require(digest(raw) == sha, 'trusted repository helper changed: ' + relative)
        require(packet.raw(ORIGINAL_REPO / relative) == raw, 'archived helper differs')
    sys.path[:0] = [str(repository / 'scripts'), str(repository / 'python/src')]
    protocol = importlib.import_module('native_workflow_protocol')
    uat = importlib.import_module('run_clickbench_query_uat')
    timing = importlib.import_module('query_timing_report')
    reference = importlib.import_module('clickbench_reference_packet')
    return protocol, uat, timing, reference


def expected_groups():
    groups = []
    for index, query in enumerate((1, 19, 17, 23, 28, 29, 34, 35, 20)):
        patterns = ('fresh_process', 'same_plan', 'equivalent_new_plan')
        for pattern in patterns if index % 2 == 0 else reversed(patterns):
            if pattern == 'fresh_process':
                groups.extend({'id': f'q{query:02d}-fresh-{trial}', 'pattern': pattern,
                               'queries': [query], 'fresh_trial': trial} for trial in (1, 2, 3))
            else:
                groups.append({'id': f'q{query:02d}-{pattern}', 'pattern': pattern, 'queries': [query] * 3})
    groups.extend({'id': f'related-{index}', 'pattern': 'related_new_plan', 'queries': list(queries)}
                  for index, queries in enumerate(((19, 17, 19), (23, 28, 29), (34, 35, 34), (1, 20, 1)), 1))
    return groups


def oracle_values(packet, helpers, repository):
    frozen = packet.read(ORIGINAL / 'protocol-r1.json')
    reference = packet.linked(frozen['reference'])
    require(digest(reference) == REFERENCE_SHA, 'independent reference identity')
    require(frozen['reference']['reference_kind'] == 'independent_reference', 'reference kind')
    query = packet.linked(frozen['queries'])
    require(query == (repository / 'benchmarks/clickbench/queries.sql').read_bytes(), 'pinned query bytes')
    with tempfile.TemporaryDirectory(prefix='warm-reference-') as temporary:
        path = Path(temporary) / 'reference.json'
        path.write_bytes(reference)
        return helpers[3].load_reference_packet(path, repository / 'benchmarks/clickbench/queries.sql')['values']


def check_result(row, expected_schema, reference, helpers):
    protocol, uat, timing, _ = helpers
    envelope = row['envelope']
    require(envelope['status'] == 'success' and envelope['fallback']['attempted'] is False, 'successful native result required')
    fields = protocol.report_fields(envelope)
    require(fields.get('external_engine_invoked') == 'false', 'external execution boundary')
    values = protocol.extract_result(envelope)
    require(uat.equivalent(values, reference), 'independent complete values differ')
    require(strict(fields['result_schema_json']) == expected_schema, 'result schema differs')
    clock = row.get('complete_call_seconds', row.get('client_response_seconds'))
    require(isinstance(clock, (int, float)) and math.isfinite(clock) and clock > 0, 'complete call clock')
    engine = row['engine_execution_seconds']
    require(math.isfinite(engine) and engine > 0, 'engine clock')
    require(timing.query_phase_timings(fields, clock)['engine_execution_seconds'] == engine, 'native report engine clock differs')
    before, after = row['before'], row['after']
    delta = after['completed_executions'] - (before.get('completed_executions') or 0)
    require(delta == 1, 'each call must complete exactly one fresh execution')
    return len(values)


def verify_calls(packet, directory, reference, helpers):
    protocol = packet.read(directory / 'protocol-r1.json')
    receipt = packet.read(directory / 'baseline-receipt-r1.json')
    post = packet.read(directory / 'baseline-postflight-r1.json')
    require(protocol['groups'] == expected_groups() and protocol['calls'] == 93, 'frozen call schedule')
    require(packet.linked(receipt['protocol']) == packet.raw(directory / 'protocol-r1.json'), 'receipt protocol binding')
    require(receipt['status'] == post['status'] == 'passed' and post['calls'] == 93, 'complete comparison and postflight')
    require(post['source_generation'] == protocol['generation'], 'postflight source generation')
    expected = [(g['id'], n, q) for g in protocol['groups'] for n, q in enumerate(g['queries'], 1)]
    actual = [(r['group'], r['call'], r['query']) for r in receipt['calls']]
    require(actual == expected, 'ordered call coverage differs')
    require(len(receipt['completed_groups']) == 49, 'group coverage differs')
    total_rows, opens, completions, dictionary_hits, dictionary_insertions = 0, 0, 0, 0, 0
    for group, completed in zip(protocol['groups'], receipt['completed_groups'], strict=True):
        require(completed['group'] == group and completed['process']['returncode'] == 0 and
                not completed['process']['guard_failures'], 'process or guard failure')
        records = [strict(line) for line in packet.raw(directory / 'baseline-r1' / (group['id'] + '.stdout.json')).splitlines()]
        require(len(records) == len(group['queries']) + 1, 'raw group coverage differs')
        final = records[-1]
        require(final['group'] == group['id'] and final['lifecycle_complete'] is True and
                final['calls'] == len(group['queries']), 'group lifecycle')
        require(final['final_snapshot']['reserved_bytes'] == 0 and
                final['final_snapshot'].get('reusable_sources', 0) == 0, 'retained state after close')
        saved = [row for row in receipt['calls'] if row['group'] == group['id']]
        require(saved == records[:-1], 'receipt differs from raw stdout')
        for row in saved:
            require(row['passed'] is True and row['pattern'] == group['pattern'], 'call identity or outcome')
            total_rows += check_result(row, protocol['schemas'][str(row['query'])], reference[row['query']], helpers)
            require(row['completed_executions_delta'] == 1, 'reported completion differs')
            before, after = row['before'], row['after']
            delta = after['prepared_source_opens'] - (before.get('prepared_source_opens') or 0)
            require(delta == row['source_opens_delta'] and delta in (0, 1), 'source open counter differs')
            opens += delta
            completions += row['completed_executions_delta']
            if directory.name == 'candidate-r1':
                dictionary_hits += after['reused_dictionary_values'] - (before.get('reused_dictionary_values') or 0)
                dictionary_insertions += after['dictionary_value_insertions'] - (before.get('dictionary_value_insertions') or 0)
    return protocol, receipt, {'calls': completions, 'groups': 49, 'complete_rows': total_rows, 'actual_source_opens': opens,
                               'dictionary_hits': dictionary_hits if directory.name == 'candidate-r1' else None,
                               'dictionary_insertions': dictionary_insertions if directory.name == 'candidate-r1' else None}


def recompute_scores(baseline, candidate, definition):
    def keyed(receipt):
        return {(r['group'], r['call'], r['query']): r for r in receipt['calls']}
    old, new = keyed(baseline), keyed(candidate)
    require(list(old) == list(new) and len(old) == 93, 'paired call coverage')

    def compare(query, mode):
        selected = []
        for key, row in old.items():
            if row['query'] != query:
                continue
            match = (row['pattern'] == 'fresh_process' if mode == 'fresh' else
                     row['pattern'] == 'related_new_plan' and row['call'] > 1 if mode == 'related' else
                     row['pattern'] in ('same_plan', 'equivalent_new_plan') and
                     (row['call'] == 1 if mode == 'nonreuse' else row['call'] > 1))
            if match:
                selected.append(key)
        if not selected:
            return None
        first = statistics.median(old[k]['complete_call_seconds'] for k in selected)
        second = statistics.median(new[k]['complete_call_seconds'] for k in selected)
        attribution = []
        for key in selected:
            row = new[key]
            fewer = row['source_opens_delta'] < old[key]['source_opens_delta']
            hits = (row['after'].get('reused_dictionary_values') or 0) - (row['before'].get('reused_dictionary_values') or 0)
            if fewer or hits > 0:
                attribution.append({'call': list(key), 'fewer_actual_source_opens': fewer, 'dictionary_source_hits': hits})
        return {'query': query, 'mode': mode, 'calls': [list(k) for k in selected],
                'baseline_median_complete_call_seconds': first, 'candidate_median_complete_call_seconds': second,
                'relative_reduction': 1 - second / first, 'control_passed': second - first <= max(first * .10, .0001),
                'attribution': attribution}

    families = []
    for name, queries in definition['families'].items():
        warm = [compare(q, 'warm') for q in queries]
        related = [row for q in queries if (row := compare(q, 'related')) is not None]
        reduction = statistics.median(row['relative_reduction'] for row in warm)
        related_reduction = statistics.median(row['relative_reduction'] for row in related) if related else None
        families.append({'family': name, 'warm': warm, 'related': related,
                         'warm_relative_reduction': reduction, 'related_relative_reduction': related_reduction,
                         'warm_passed': reduction >= .05 and any(row['attribution'] for row in warm),
                         'related_passed': related_reduction is not None and related_reduction >= .05 and any(row['attribution'] for row in related)})
    queries = sorted({row['query'] for row in old.values()})
    controls = [compare(q, mode) for mode in ('fresh', 'nonreuse') for q in queries]
    controls += [compare(q, 'warm') for q in definition['metadata_control_queries']]
    qualified = [family for family in families if family['warm_passed']]
    benefit = len(qualified) >= 2 and any(family['related_passed'] for family in qualified)
    return families, controls, benefit, all(row['control_passed'] for row in controls)


def verify_scores(packet, old_protocol, new_protocol, baseline, candidate):
    for name in ('groups', 'source', 'generation', 'schemas', 'next_candidate_gates', 'resources', 'limits'):
        require(old_protocol[name] == new_protocol[name], 'candidate changed frozen input: ' + name)
    require(packet.linked(new_protocol['baseline_protocol']) == packet.raw(ORIGINAL / 'protocol-r1.json'), 'baseline protocol binding')
    require(packet.linked(new_protocol['baseline_results']) == packet.raw(ORIGINAL / 'baseline-receipt-r1.json'), 'baseline results binding')
    score_source = next(ref for ref in new_protocol['helpers'] if ref['path'].endswith('/score_candidate.py'))
    parsed = ast.parse(packet.linked(score_source))
    definitions = [node.value for node in parsed.body if isinstance(node, ast.Assign) and
                   any(isinstance(target, ast.Name) and target.id == 'DEFINITION' for target in node.targets)]
    require(len(definitions) == 1, 'frozen scoring definition')
    definition = ast.literal_eval(definitions[0])
    require(new_protocol['scoring_definition'] == definition, 'frozen scoring rule changed')
    families, controls, benefit, controls_passed = recompute_scores(baseline, candidate, definition)
    score = packet.read(ORIGINAL / 'candidate-r1/scoring-r1.json')
    require(score['families'] == families and score['controls'] == controls, 'saved scores differ from all observations')
    require(score['benefit_passed'] == benefit and score['controls_passed'] == controls_passed, 'saved gate outcome differs')
    require(not benefit and not controls_passed and score['status'] == 'rejected_by_frozen_gates', 'rejection was changed')
    for name, path in (('baseline_receipt', ORIGINAL / 'baseline-receipt-r1.json'),
                       ('candidate_receipt', ORIGINAL / 'candidate-r1/baseline-receipt-r1.json'),
                       ('candidate_protocol', ORIGINAL / 'candidate-r1/protocol-r1.json')):
        require(packet.linked(score[name]) == packet.raw(path), 'score input identity')
    engine = [{'call': [old['group'], old['call'], old['query']], 'baseline': old['engine_execution_seconds'],
               'candidate': new['engine_execution_seconds']} for old, new in zip(baseline['calls'], candidate['calls'], strict=True)]
    require(score['engine_times'] == engine, 'separate engine observations changed')
    return {'status': score['status'], 'qualifying_families': [f['family'] for f in families if f['warm_passed']],
            'failed_controls': [{'query': c['query'], 'mode': c['mode']} for c in controls if not c['control_passed']]}


def verify_triplets(packet, reference, helpers):
    root = ORIGINAL / 'clickbench-triplets-r1'
    protocol, receipt = packet.read(root / 'protocol.json'), packet.read(root / 'receipt.json')
    require(receipt['status'] == 'passed' and protocol['calls'] == 129 and protocol['queries'] == list(range(1, 44)), 'triplet coverage')
    require(packet.linked(receipt['protocol']) == packet.raw(root / 'protocol.json'), 'triplet protocol identity')
    require(len(receipt['groups']) == 43 and len(receipt['records']) == 129, 'triplet process coverage')
    selected, total_rows, source_opens = [], 0, 0
    for query, group in enumerate(receipt['groups'], 1):
        require(group['query'] == query and group['process']['returncode'] == 0 and not group['process']['guard_failures'], 'triplet group completion')
        lines = [strict(line) for line in packet.raw(root / 'logs' / f'q{query:02d}.stdout.json').splitlines()]
        require(len(lines) == 7 and lines[-1]['kind'] == 'lifecycle' and lines[-1]['complete'] is True and
                lines[-1]['query'] == query and lines[-1]['final_snapshot']['reserved_bytes'] == 0, 'triplet final lifecycle')
        rows = receipt['records'][(query - 1) * 3:query * 3]
        for run, row in enumerate(rows, 1):
            result, timing = lines[(run - 1) * 2:run * 2]
            require(result['kind'] == 'result' and timing['kind'] == 'timing', 'triplet raw record order')
            require(result['query'] == timing['query'] == row['query'] == query and
                    result['run'] == timing['run'] == row['run'] == run, 'triplet call identity')
            count = check_result(row, protocol['schemas'][str(query)], reference[query], helpers)
            require(row == {**timing, 'passed': True, 'complete_rows': count, 'envelope': result['envelope']}, 'triplet raw record differs')
            total_rows += count
            source_opens += row['after']['prepared_source_opens'] - (row['before'].get('prepared_source_opens') or 0)
        hot = min(rows[1:], key=lambda row: (row['client_response_seconds'], row['run']))
        selected.append({'query': query, 'first_use_run': 1, 'hot_run': hot['run'],
                         'first_use_client_seconds': rows[0]['client_response_seconds'], 'hot_client_seconds': hot['client_response_seconds'],
                         'first_use_engine_seconds': rows[0]['engine_execution_seconds'], 'hot_engine_seconds': hot['engine_execution_seconds'],
                         'client_triple': [r['client_response_seconds'] for r in rows], 'engine_triple': [r['engine_execution_seconds'] for r in rows]})
    totals = {key: sum(row[key] for row in selected) for key in ('first_use_client_seconds', 'hot_client_seconds', 'first_use_engine_seconds', 'hot_engine_seconds')}
    require(receipt['scores']['per_query'] == selected and receipt['scores']['totals'] == totals, 'triplet selected scores differ')
    require(receipt['scores']['first_use_is_true_cold'] is False, 'false cold-cache claim')
    require(total_rows == 1143 and source_opens == 129, 'triplet complete values or source opens')
    summary = packet.read(root / 'verified-summary.json')
    require(summary['totals'] == totals and summary['complete_executions'] == 129 and summary['complete_rows'] == total_rows, 'triplet summary differs')
    return {'queries': 43, 'calls': 129, 'complete_rows': total_rows, 'actual_source_opens': source_opens,
            'first_use_engine_seconds': totals['first_use_engine_seconds'], 'selected_later_engine_seconds': totals['hot_engine_seconds']}


def verify_sources(packet, repository, current_source, reconstruct):
    baseline = packet.read(PROVIDER / 'artifacts-r2/build-summary.json')
    candidate = packet.read(PROVIDER / 'warm-artifacts-r1/build-summary.json')
    preservation = packet.read(ORIGINAL / 'rejected-source-preservation-r1.json')
    removal = packet.read(ORIGINAL / 'rejected-source-removal-r1.json')
    rows = {row['path']: row for row in preservation['files']}
    require(preservation['status'] == removal['status'] == 'passed' and preservation['base'] == removal['base'] == BASE, 'preservation or removal status')
    require(len(rows) == 63 and len(removal['restored']) == 58 and len(removal['removed_added']) == 4, 'candidate removal coverage')
    require(set(removal['restored']) == {name for name, row in rows.items() if row['action'] == 'restore_baseline'} and
            set(removal['removed_added']) == {name for name, row in rows.items() if row['action'] == 'remove_added'}, 'removal action mapping')
    require(removal['preservation_sha256'] == digest(packet.raw(ORIGINAL / 'rejected-source-preservation-r1.json')), 'removal source binding')
    require(removal['all_restored_bytes_match_base'] and not removal['remaining_runtime_diff'], 'runtime not restored')
    patch = gzip.decompress(packet.linked(preservation['patch']))
    require(len(patch) == preservation['patch']['raw_bytes'] and digest(patch) == preservation['patch']['raw_sha256'], 'candidate patch identity')
    for build in (baseline, candidate):
        require(build['status'] == 'passed' and build['package_feature_union'] == 'release-user-surfaces', 'matched native build')
        require(build['source_before'] == build['source_after'], 'source changed while building')
    old = {**baseline['source_after'], **baseline['runtime_python_sources']}
    new = {**candidate['source_after'], **candidate['runtime_python_sources']}
    for name in old.keys() | new.keys():
        if old.get(name) != new.get(name):
            require(name in rows and rows[name]['baseline_sha256'] == old.get(name) and rows[name]['sha256'] == new.get(name), 'unpreserved candidate source: ' + name)
    for root, build in ((ORIGINAL, baseline), (ORIGINAL / 'candidate-r1', candidate), (ORIGINAL / 'clickbench-triplets-r1', baseline)):
        protocol = packet.read(root / ('protocol.json' if root.name == 'clickbench-triplets-r1' else 'protocol-r1.json'))
        require(strict(packet.linked(protocol['build'])) == build and protocol['library'] == build['artifacts']['library'], 'observations use another native build')
        for helper in protocol['helpers']:
            packet.linked(helper)
    require(strict(packet.linked(candidate['build']))['source_after'] == candidate['source_after'], 'candidate build receipt binding')
    checked = 0
    if current_source:
        for name, sha in old.items():
            path = repository / name
            if name.startswith('vendor/') and name.endswith('/Cargo.lock'):
                continue  # Historical standalone test locks are generated, not runtime sources.
            require(path.is_file() and digest(path.read_bytes()) == sha, 'current accepted runtime differs: ' + name)
            checked += 1
        for row in rows.values():
            path = repository / row['path']
            if row['action'] == 'restore_baseline':
                require(path.is_file() and digest(path.read_bytes()) == row['baseline_sha256'], 'restored file differs')
            elif row['action'] == 'remove_added':
                require(not path.exists(), 'candidate file remains')
    if reconstruct:
        with tempfile.TemporaryDirectory(prefix='warm-source-index-') as temporary:
            env = dict(os.environ, GIT_INDEX_FILE=str(Path(temporary) / 'index'))
            subprocess.run(['git', 'read-tree', BASE], cwd=repository, env=env, check=True, capture_output=True)
            subprocess.run(['git', 'apply', '--cached', '--binary', '-'], cwd=repository, env=env, input=patch, check=True, capture_output=True)
            for name, row in rows.items():
                raw = subprocess.check_output(['git', 'show', ':' + name], cwd=repository, env=env)
                require(digest(raw) == row['sha256'], 'reconstructed candidate differs: ' + name)
    return {'preserved_files': 63, 'restored_files': 58, 'removed_files': 4, 'current_runtime_files_checked': checked,
            'candidate_reconstructed': reconstruct}


def verify_validation(packet):
    receipts = {}
    for path in packet.files:
        if Path(path).parent != PROVIDER or not Path(path).name.startswith('warm-') or not path.endswith('.json'):
            continue
        receipt = packet.read(path)
        if 'source_before' not in receipt or 'command' not in receipt:
            continue
        log = packet.raw(Path(path).with_suffix('.log'))
        require(digest(log) == receipt['log_sha256'], 'validation log identity')
        require(receipt['status'] in ('passed', 'failed'), 'validation outcome')
        if receipt['status'] == 'passed':
            require(receipt['returncode'] == 0 and receipt['process_group_drained'] is True and not receipt.get('source_changes'), 'validation completion')
        totals = [tuple(map(int, row)) for row in TEST_RESULT.findall(log.decode())]
        receipts[Path(path).stem] = {'status': receipt['status'], 'tests': [sum(row[n] for row in totals) for n in range(3)]}
    required = {'warm-native-clippy-r3': None, 'warm-session-tests-r6': [107, 0, 7], 'warm-regressions-r4': [53, 0, 0],
                'warm-release-matched-r1': None, 'warm-installed-wheel-r1': None, 'warm-restored-fmt-r1': None,
                'warm-restored-clippy-r1': None, 'warm-restored-tests-r1': [3246, 0, 0]}
    expected_history = set(required) | {'warm-native-check-r1', 'warm-native-clippy-r1', 'warm-native-clippy-r2',
                       'warm-native-tests-r1', 'warm-native-tests-r2', 'warm-native-tests-r3',
                       'warm-session-tests-r1', 'warm-session-tests-r2', 'warm-session-tests-r3',
                       'warm-session-tests-r4', 'warm-session-tests-r5'}
    require(set(receipts) == expected_history, 'validation history coverage changed')
    for label, tests in required.items():
        require(receipts[label]['status'] == 'passed', 'required validation failed: ' + label)
        require(tests is None or receipts[label]['tests'] == tests, 'test coverage differs: ' + label)
    wheel = packet.read(PROVIDER / 'warm-wheel-proof-r1/report.json')
    build = packet.read(PROVIDER / 'warm-artifacts-r1/build-summary.json')
    require(wheel['status'] == wheel['uninstall_status'] == 'passed' and wheel['publication_attempted'] is False, 'local wheel lifecycle')
    require(wheel['integration'] == {'tests': 142, 'failures': 0, 'errors': 0, 'skipped': 0, 'status': 'passed'}, 'installed native integration')
    require(wheel['integrity']['native_library_sha256'] == build['artifacts']['library']['sha256'] and
            wheel['integrity']['cli_sha256'] == build['artifacts']['cli']['sha256'], 'installed wheel native bytes')
    license_check = packet.read(ORIGINAL / 'restored-license-check-r1.json')
    require(license_check == {'status': 'passed', 'registry_packages': 437, 'vendored_packages': 8,
                             'license_file_records': 827, 'unique_text_files': 248}, 'restored license bundle')
    return {'receipts': len(receipts), 'failed_history': [name for name, row in receipts.items() if row['status'] == 'failed'],
            'candidate_focused_tests': 107, 'candidate_regression_tests': 53, 'installed_tests': 142, 'restored_default_tests': 3246}


def self_test(packet, reference, helpers):
    directory = ORIGINAL / 'candidate-r1'
    receipt_path = directory / 'baseline-receipt-r1.json'
    original = packet.read(receipt_path)
    group = original['calls'][0]['group']
    raw_path = directory / 'baseline-r1' / (group + '.stdout.json')
    cases = []
    for mode in ('missing_call', 'duplicate_call', 'wrong_value', 'wrong_schema', 'no_execution', 'wrong_open', 'wrong_engine', 'retained_memory'):
        altered = copy.copy(packet)
        altered.objects = dict(packet.objects)
        receipt = copy.deepcopy(original)
        records = [strict(line) for line in packet.raw(raw_path).splitlines()]
        if mode == 'missing_call':
            receipt['calls'].pop()
        elif mode == 'duplicate_call':
            receipt['calls'][-1] = copy.deepcopy(receipt['calls'][0])
        elif mode == 'retained_memory':
            records[-1]['final_snapshot']['reserved_bytes'] = 1
        else:
            row = receipt['calls'][0]
            if mode == 'wrong_value':
                fields = row['envelope']['fields']
                field = next(field for field in fields if field['key'] == 'result_values_json')
                values = strict(field['value'])
                values[0][next(iter(values[0]))] += 1
                field['value'] = json.dumps(values)
            elif mode == 'wrong_schema':
                next(field for field in row['envelope']['fields'] if field['key'] == 'result_schema_json')['value'] = '[]'
            elif mode == 'no_execution':
                row['after']['completed_executions'] = 0
            elif mode == 'wrong_open':
                row['source_opens_delta'] = 99
            else:
                row['engine_execution_seconds'] *= 2
            records[0] = copy.deepcopy(row)
        altered.objects[packet.files[str(receipt_path)]['member']] = json.dumps(receipt, allow_nan=False).encode()
        altered.objects[packet.files[str(raw_path)]['member']] = b'\n'.join(json.dumps(row, allow_nan=False).encode() for row in records) + b'\n'
        try:
            verify_calls(altered, directory, reference, helpers)
        except ValueError as error:
            cases.append({'case': mode, 'rejected': True, 'reason': str(error)})
        else:
            raise ValueError('corruption accepted: ' + mode)
    old_protocol = packet.read(ORIGINAL / 'protocol-r1.json')
    new_protocol = packet.read(directory / 'protocol-r1.json')
    baseline = packet.read(ORIGINAL / 'baseline-receipt-r1.json')
    for mode in ('forged_acceptance', 'changed_frozen_rule'):
        altered = copy.copy(packet)
        altered.objects = dict(packet.objects)
        changed_protocol = copy.deepcopy(new_protocol)
        if mode == 'forged_acceptance':
            path = directory / 'scoring-r1.json'
            score = packet.read(path)
            score['benefit_passed'] = True
            score['controls_passed'] = True
            score['status'] = 'focused_benefit_and_controls_passed'
            altered.objects[packet.files[str(path)]['member']] = json.dumps(score).encode()
        else:
            changed_protocol['scoring_definition']['benefit_gate'] = 'one family is sufficient'
        try:
            verify_scores(altered, old_protocol, changed_protocol, baseline, original)
        except ValueError as error:
            cases.append({'case': mode, 'rejected': True, 'reason': str(error)})
        else:
            raise ValueError('corruption accepted: ' + mode)
    return cases


def main():
    if sys.version_info < (3, 10):
        raise SystemExit('Python 3.10 or newer is required by the trusted ShardLoom result helpers.')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository', type=Path, default=Path(__file__).resolve().parents[4])
    parser.add_argument('--current-source', action='store_true')
    parser.add_argument('--reconstruct-source', action='store_true', help='requires the recorded base Git commit; uses only a private index')
    parser.add_argument('--self-test', action='store_true')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    packet = Packet(Path(__file__).resolve().parent)
    helpers = trusted_helpers(packet, args.repository)
    reference = oracle_values(packet, helpers, args.repository)
    old, before, baseline = verify_calls(packet, ORIGINAL, reference, helpers)
    new, after, candidate = verify_calls(packet, ORIGINAL / 'candidate-r1', reference, helpers)
    result = {'status': 'passed', 'scope': 'saved complete values, schemas, fresh completion, original gates, source restoration and validation; no engine execution',
              'packet_identity': {'manifest_sha256': digest((Path(__file__).resolve().parent / 'manifest.json').read_bytes()),
                                  'verifier_sha256': digest(Path(__file__).read_bytes()),
                                  'archive_sha256': packet.manifest['archive']['sha256']},
              'baseline': baseline, 'candidate': candidate, 'decision': verify_scores(packet, old, new, before, after),
              'earlier_accepted_runtime_triplets': verify_triplets(packet, reference, helpers),
              'sources': verify_sources(packet, args.repository, args.current_source, args.reconstruct_source),
              'validation': verify_validation(packet), 'corruption_controls': self_test(packet, reference, helpers) if args.self_test else []}
    if args.output:
        with args.output.open('x') as stream:
            json.dump(result, stream, indent=2, allow_nan=False)
            stream.write('\n')
    print(json.dumps(result, indent=2, allow_nan=False))


if __name__ == '__main__':
    main()
