# SPDX-License-Identifier: Apache-2.0
"""Semantic replay of the retained shared-performance candidates and integration."""
from collections import Counter
import gzip
import io
import json
import math
from pathlib import Path
import re

from clickbench_native_sql import PINNED_QUERIES_SHA256, native_query_statements
from clickbench_reference_packet import canonical_values_sha256
from native_workflow_protocol import extract_result, public_workflow_command, report_fields, strict_json
from parquet_demand_completion_replay import replay
from query_surface.evidence import no_fallback
from query_surface.replay import MAX_FILE, contract, need, sha
from query_surface.semantics import compare, result_from_record
from query_timing_report import query_phase_timings
from run_clickbench_query_uat import equivalent

FINAL_RUNTIME = 'review-integration-release-r1'
FINAL_COHORT = 'broad-review-integration-r1'
FINAL_COMMANDS = {
    'p122-review-native-clippy.json': 'clippy --locked --offline --workspace --all-targets --features release-user-surfaces --profile ci-native -- -D warnings',
    'p123-review-native-workspace.json': 'test --locked --offline --workspace --all-targets --features release-user-surfaces --profile ci-native',
    'p124-review-default-clippy.json': 'clippy --locked --offline --workspace --all-targets -- -D warnings',
    'p125-review-default-workspace.json': 'test --locked --offline --workspace --all-targets',
    'p126-review-fmt.json': 'fmt --all -- --check',
    'p127-review-release.json': 'build --locked --offline --release --features release-user-surfaces -p shardloom-cli -p shardloom-python',
}
DRAFT_DIGESTS = {
    '59aa20fe790e77957185ad759a38b0ca4abf6b2f89654004675fec3b41402cba': 6557,
    '342810cbbc7994ae867c83658da73ebde6016d5c4d5913aa14d5f22a9f55bb06': 6617,
    'd0c0db75dbc44d2cd0a3519ac87f23791918c06457fc61b9feb5c2ac2908cb3a': 9728,
}
DRAFT_PATH = 'docs/architecture/native-shared-performance-2026-10-29.md'


def linked(bundle, item):
    return strict_json(bundle.identity(item))


def complete_gate(gate):
    need(gate['status'] == 'passed' and gate['returncode'] == 0
         and gate['process_group_drained'] is True and gate['direct_child_reaped'] is True
         and gate['timed_out'] is False and gate['interrupted_signal'] is None,
         'required process gate did not complete')


def test_totals(log, expected):
    rows = re.findall(r'test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;', log)
    actual = (len(rows), *(sum(int(row[i]) for row in rows) for i in (1, 2, 3)))
    need(actual == expected and all(row[0] == 'ok' for row in rows), 'workspace test coverage changed')
    return dict(binaries=actual[0], passed=actual[1], failed=actual[2], ignored=actual[3])


def tested_source(bundle, gate_name, gate, relative, source, built):
    if source == built:
        return
    need(gate_name == 'p122-review-native-clippy.json'
         and relative == 'shardloom-vortex/src/local_primitive_relational_ranked.rs',
         'tested source differs from final build: ' + gate_name + ':' + relative)
    # cargo fmt collapsed this one unchanged condition after strict lint.
    # Require the exact byte transformation, not general whitespace removal.
    before = (b'        if !declaration.usage.admits_ranked()\n'
              b'            || (plan.filter.is_some() && predicate.is_none())\n        {')
    after = b'        if !declaration.usage.admits_ranked() || (plan.filter.is_some() && predicate.is_none()) {'
    path = gate['cwd'] + '/' + relative
    old = bundle.identity(dict(path=path, **source))
    new = bundle.identity(dict(path=path, **built))
    need(old.count(before) == 1 and old.replace(before, after, 1) == new,
         'post-lint source change is not the recorded formatting-only edit')


def public_comparisons(value):
    """Check complete saved public values; failed attempts never gain acceptance."""
    if value['status'] != 'passed':
        need(value['status'] == 'failed' and value['failures'] + value['errors'] > 0,
             'unrecognized public failure')
        return dict(status='failed', accepted_executions=0,
                    completed_comparisons_retained=len(value['complete_comparisons']))
    need(value['tests'] > 0 and value['failures'] == value['errors'] == value['skipped'] == 0
         and value['posthash_verified'] is True and value['comparison_engine_invoked'] is False,
         'public suite did not complete')
    rows = 0
    for item in value['complete_comparisons']:
        if 'contract' in item:
            compare(result_from_record(item['expected']), result_from_record(item['actual']),
                    contract(item['contract']))
            need(item['report']['complete_consumption'] is True
                 and item['report']['surface'] == item['surface'], 'public consumption changed')
            no_fallback(item['report']['fields'])
            need(item['rows'] == len(item['actual']['rows']), 'public row count changed')
        else:
            # The first candidate also retained six direct ownership/readback
            # comparisons; these are not additional public executions.
            need(item['actual'] == item['expected'], 'direct retained values differ')
        rows += item['rows']
    need(rows == value['compared_rows'], 'public compared rows changed')
    need(value['positive_public_executions'] > 0, 'public execution coverage absent')
    if 'contract' in value['complete_comparisons'][0]:
        need(len(value['complete_comparisons']) == value['positive_public_executions'],
             'public execution count differs from comparisons')
        identities = [(item['method'], item['case'], item['format'], item['surface'])
                      for item in value['complete_comparisons']]
        need(len(set(identities)) == len(identities), 'duplicate public comparison identity')
        formats = Counter(item['format'] for item in value['complete_comparisons'])
        surfaces = Counter(item['surface'] for item in value['complete_comparisons'])
        need(set(formats) == {'vortex', 'parquet', 'arrow'} and len(set(formats.values())) == 1,
             'public format controls incomplete')
        need(set(surfaces) == {'cli_sql', 'embedded_workflow', 'python_sql', 'dataframe', 'typed_relation'}
             and len(set(surfaces.values())) == 1, 'public interface controls incomplete')
        cases = {(method, case) for method, case, _, _ in identities}
        need(set(identities) == {(method, case, form, surface) for method, case in cases
             for form in formats for surface in surfaces}, 'public case interface/format grid incomplete')
    return dict(status='passed', accepted_executions=value['positive_public_executions'],
                complete_rows=rows)


def installed_package(bundle, root, runtime):
    path = root + '/installed-review-integration-r1'
    package = strict_json(bundle.raw(path + '/report.json'))
    integration = strict_json(bundle.raw(path + '/integration.json'))
    need(integration == dict(tests=264, failures=0, errors=0, skipped=0, status='passed')
         and package['integration'] == integration and package['status'] == 'passed'
         and package['uninstall_status'] == 'passed' and package['installed_load']['isolated_install']
         and package['installation_performed'] and not package['publication_attempted'],
         'installed-package acceptance incomplete')
    need(package['integrity']['cli_sha256'] == runtime['products']['shardloom']
         and package['integrity']['native_library_sha256'] == runtime['products']['_native.abi3.so'],
         'installed-package runtime differs')
    need([step['name'] for step in package['steps']] == ['build', 'create_environment',
         'install_optional_consumer', 'install', 'load', 'integration', 'uninstall', 'verify_uninstalled'],
         'installed-package lifecycle changed')
    for step in package['steps']:
        need(step['returncode'] == 0 and sha(bundle.raw(path + '/' + step['log'])) == step['log_sha256'],
             'installed-package log/result changed')
    return integration


def frozen_files(bundle, items, external):
    for item in items:
        size = item.get('bytes', item.get('generation', {}).get('bytes'))
        if (item['path'], item['sha256'], size) in external:
            path = Path(item['path'])
            need(path.suffix in {'.parquet', '.arrow', '.so', '.dylib', '.whl'} or path.name == 'shardloom',
                 'non-payload evidence cannot be external')
            continue
        bundle.identity(item)


def full43(bundle, root, anchors, runtime, external):
    frozen = linked(bundle, dict(path=root + '/full43-review-r1-freeze.json', **anchors['full43']))
    accepted = strict_json(bundle.raw(root + '/full43-review-r1-accepted.json'))
    driver, summary, reference = (linked(bundle, accepted[name]) for name in ('driver', 'summary', 'reference'))
    complete_gate(driver)
    need(driver['native_process_cleanup_verified'] and driver['input_before'] == driver['input_after']
         == frozen['source'] and driver['ingest_performed'] is False
         and frozen['exact_source_commit_binding_verified'] is True, 'Full43 source identity/cleanup failed')
    need(frozen['queries'] == 43 and frozen['repeats'] == 1 and frozen['input_format'] == 'parquet'
         and frozen['memory_gb'] == 24 and frozen['max_parallelism'] == 12
         and frozen['per_query_deadline_seconds'] == 120 and frozen['ingest_performed'] is False
         and frozen['recursive_inventory_during_execution'] is False, 'Full43 procedure changed')
    need(driver['source_revision'] == frozen['source_revision'] == summary['build_commit']
         == runtime['source_revision'] == anchors['source_head'], 'Full43 source revision changed')
    need(driver['freeze'] == accepted['freeze']
         and driver['summaries'] == [accepted['summary']], 'Full43 evidence linkage changed')
    need(summary['source'] == frozen['source']['path']
         and summary['source_bytes'] == frozen['source']['bytes']
         and summary['source_generation'] == {
             ('size_bytes' if key == 'bytes' else key): value
             for key, value in frozen['source']['generation'].items()}, 'Full43 source summary changed')
    need(summary['complete'] and summary['runs_completed'] == summary['runs_passed'] == 43
         and summary['selected_query_ids'] == list(range(1, 44)) and summary['repeats'] == 1
         and summary['score_scope'] == 'full_43' and summary['input_format'] == 'parquet'
         and summary['timing_available'] and summary['full_result_validation']
         and summary['completed_identity_check'] and summary['reference_packet_identity_verified']
         and summary['harness_identity_verified'], 'Full43 completion changed')
    frozen_files(bundle, frozen['helpers'] + frozen['runtime'] + [frozen['source']], external)
    helpers = {item['path']: item['sha256'] for item in frozen['helpers']}
    need(all(helpers.get(path) == digest for path, digest in summary['harness_sha256'].items()),
         'Full43 harness summary changed')
    binary = next(item for item in frozen['runtime'] if Path(item['path']).name == 'shardloom')
    need(summary['binary_sha256'] == binary['sha256'] == runtime['products']['shardloom'],
         'Full43 binary differs')
    queries_path = driver['cwd'] + '/benchmarks/clickbench/queries.sql'
    queries = bundle.raw(queries_path)
    need(sha(queries) == PINNED_QUERIES_SHA256 == summary['queries_sha256'], 'Full43 query file differs')
    statements = [q.strip() for q in '\n'.join(line for line in queries.decode().splitlines()
        if not line.lstrip().startswith('--')).split(';') if q.strip()]
    submitted, dialect = native_query_statements(statements, PINNED_QUERIES_SHA256, input_format='parquet')
    need(summary['query_dialect'] == dialect, 'Full43 submitted dialect changed')
    need(summary['reference_packet'] == dict(path=frozen['reference']['path'],
         sha256=frozen['reference']['sha256'], reference_kind='independent_reference',
         queries_sha256=PINNED_QUERIES_SHA256), 'Full43 reference summary changed')
    need(driver['command'][1:] == ['-B', 'scripts/run_clickbench_query_uat.py',
         '--binary', binary['path'], '--input', frozen['source']['path'], '--input-format', 'parquet',
         '--uat-root', root + '/full43-review-r1', '--reference-packet', frozen['reference']['path'],
         '--build-commit', anchors['source_head'], '--memory-gb', '24', '--max-parallelism', '12',
         '--timeout', '120', '--repeats', '1', '--compress-logs'], 'Full43 supervisor command changed')
    need(accepted['reference'] == frozen['reference']
         and reference['reference_kind'] == 'independent_reference'
         and reference['queries_sha256'] == PINNED_QUERIES_SHA256
         and len(reference['records']) == len(summary['records']) == len(accepted['rows']) == 43,
         'Full43 independent reference changed')
    refs = {}
    for item in reference['records']:
        query = item['query_id']
        need(type(query) is int and 1 <= query <= 43 and query not in refs
             and item['statement'] == statements[query - 1]
             and item['values_sha256'] == canonical_values_sha256(item['values']), 'invalid reference row')
        refs[query] = item['values']
    rows, narrowed, pruned, ranked, ordering, prefixes = [], [], [], [], [], []
    for query, (record, old) in enumerate(zip(summary['records'], accepted['rows']), 1):
        need(record['query'] == old['query'] == query and record['run'] == 1
             and record['passed'] and record['returncode'] == 0 and not record['guard_failures']
             and record['validation'] == 'complete_values', 'Full43 failed/repeated/missing query')
        need(record['query_statement'] == statements[query - 1]
             and record['executed_query_statement'] == submitted[query - 1], 'Full43 SQL changed')
        expected_command = public_workflow_command(Path(binary['path']), submitted[query - 1],
            input_path=Path(frozen['source']['path']), input_format='parquet', memory_gb=24, max_parallelism=12)
        need(record['command'] == expected_command, 'Full43 execution command changed')
        archive = record['completed_log_archives'][0]
        packed = bundle.identity(old['archive'])
        need(archive['path'] == old['archive']['path'] and sha(packed) == archive['gzip_sha256'],
             'Full43 archived output changed')
        with gzip.GzipFile(fileobj=io.BytesIO(packed)) as stream:
            raw = stream.read(MAX_FILE + 1)
        need(len(raw) <= MAX_FILE and len(raw) == archive['raw_bytes']
             and sha(raw) == archive['raw_sha256'] == old['raw_output_sha256'], 'Full43 raw output changed')
        data = strict_json(raw)
        fields, actual = report_fields(data), extract_result(data)
        need(data['status'] == 'success' and data['fallback']['attempted'] is False, 'Full43 native output failed')
        no_fallback(fields)
        need(equivalent(actual, refs[query]) and len(actual) == old['complete_result_rows'],
             'Full43 complete values differ')
        need(sha(json.dumps(actual, sort_keys=True, allow_nan=False).encode()) == record['result_sha256'],
             'Full43 result digest differs')
        need(all(fields.get(key) == value for key, value in record['execution_report'].items()),
             'Full43 execution summary differs from raw report')
        need(fields['native_input_end_observed'] == fields['native_input_output_ownership_detached'] == 'true'
             and fields['native_source_demand_prepared_input_bytes_written'] == '0', 'Full43 ownership/preparation changed')
        clocks = query_phase_timings(fields, record['process_wall_seconds'], allow_missing=False)
        need(clocks['engine_execution_seconds'] == record['engine_execution_seconds'] == old['engine_execution_seconds']
             and clocks['result_serialization_seconds'] == record['result_serialization_seconds'], 'Full43 clock changed')
        rows.append(dict(query=query, rows=len(actual), engine_seconds=clocks['engine_execution_seconds']))
        def number(name):
            value = fields[name]
            need(isinstance(value, str) and value.isdecimal(), 'invalid Full43 source counter')
            return int(value)
        need(number('native_source_demand_sources') == number('native_source_demand_files') == 1
             and number('native_source_demand_available_columns') == 105
             and 0 < number('native_source_demand_projected_columns') <= 105
             and number('native_source_demand_row_groups_total') == 226
             and 0 < number('native_source_demand_row_groups_read') <= 226,
             'Full43 source coverage changed')
        if number('native_source_demand_projected_columns') < 105:
            narrowed.append(query)
        if number('native_source_demand_row_groups_read') < 226:
            pruned.append(query)
        if number('native_source_ranked_sources'):
            ranked.append(query)
            need(number('native_source_ranked_sources') == 1
                 and number('native_source_ranked_rank_columns') == 2
                 and number('native_source_ranked_rank_group_visits') == 226
                 and number('native_source_ranked_payload_group_visits') == 7
                 and number('native_source_ranked_selected_rows') == 10
                 and number('native_source_demand_rows_decoded') == number('native_source_ranked_rank_rows')
                     + number('native_source_ranked_payload_rows')
                 and number('native_source_demand_data_read_bytes') == number('native_source_ranked_rank_read_bytes')
                     + number('native_source_ranked_payload_read_bytes'), 'Full43 ranked work accounting changed')
        if number('native_input_ordering_prefix_batches'):
            ordering.append(query)
        if number('native_source_demand_requested_prefix_rows'):
            prefixes.append(query)
    seconds = math.fsum(row['engine_seconds'] for row in rows)
    need(sum(row['rows'] for row in rows) == accepted['completed_result_rows'] == 381
         and seconds == accepted['full43_engine_seconds']
         and math.isclose(seconds, summary['query_total_seconds'], abs_tol=1e-9, rel_tol=0), 'Full43 total changed')
    need(narrowed == [q for q in range(1, 44) if q != 24]
         and pruned == [2, 8, 20, 37, 38, 39, 40, 41, 42, 43] and ranked == [24]
         and len(ordering) == 29 and not prefixes, 'Full43 activation coverage changed')
    return dict(queries=43, complete_rows=381, engine_seconds=seconds,
        narrowed_projection_queries=narrowed, pruned_queries=pruned, ranked_queries=ranked,
        bounded_ordering_queries=ordering, source_prefix_queries=prefixes, input_format='parquet')


def verify(bundle, anchors):
    evidence, root = bundle.index['evidence'], anchors['root']
    need(evidence['root'] == root, 'original evidence root changed')
    external = {(item['path'], item['sha256'], item['bytes']) for item in evidence['external_artifacts']}
    need(len(external) == len(evidence['external_artifacts']), 'duplicate external identity')
    sections = [('gates', 'gates', lambda item: Path(item['path']).name),
                ('public_reports', 'publics', lambda item: Path(item['path']).name),
                ('runtimes', 'runtimes', lambda item: Path(item['path']).parent.name),
                ('packages', 'packages', lambda item: Path(item['path']).parent.name)]
    indexed = {}
    for key, anchor_key, name_of in sections:
        values = {name_of(item): item for item in evidence[key]}
        need(len(values) == len(evidence[key]) and values.keys() == anchors[anchor_key].keys(),
             'historical evidence coverage changed: ' + key)
        for name, item in values.items():
            wanted = anchors[anchor_key][name]
            need({k: item[k] for k in ('bytes', 'sha256')} == {k: wanted[k] for k in ('bytes', 'sha256')},
                 'original evidence anchor changed: ' + name)
            bundle.identity(item)
        indexed[key] = values
    draft = evidence['historical_doc_digest_only']
    need(draft['relative'] == DRAFT_PATH and draft['versions'] == DRAFT_DIGESTS, 'historical doc gap changed')
    gates, failures = {}, []
    for name, item in indexed['gates'].items():
        gate = gates[name] = linked(bundle, item)
        need(gate['status'] == anchors['gates'][name]['status'], 'historical outcome changed')
        if name == 'p021-join-demand-admission.json':
            need(gate['status'] == 'failed_before_execution' and gate['no_native_execution']
                 and 'log_sha256' not in gate, 'admission refusal credited as execution')
            failures.append(name)
            continue
        need(sha(bundle.raw(root + '/' + name[:-5] + '.log')) == gate['log_sha256'], 'historical log changed')
        need((gate['returncode'] == 0) == (gate['status'] == 'passed'), 'historical exit status changed')
        if gate['status'] != 'passed':
            failures.append(name)
        for relative, item in gate['changed_source_inventory'].items():
            if 'sha256' not in item:
                continue
            if relative == DRAFT_PATH and DRAFT_DIGESTS.get(item['sha256']) == item['bytes']:
                continue
            bundle.identity(dict(path=gate['cwd'] + '/' + relative, **item))
    runtimes = {}
    for name, item in indexed['runtimes'].items():
        runtime = runtimes[name] = linked(bundle, item)
        build = gates[runtime['build_receipt']]
        complete_gate(build)
        need(runtime['profile'] == 'release' and runtime['features'] == ['release-user-surfaces']
             and runtime['source_revision'] == build['head']
             and runtime['build_receipt_sha256'] == indexed['gates'][runtime['build_receipt']]['sha256']
             and set(runtime['files']) == {'shardloom', '_native.abi3.so'}
             and runtime['products'] == {key: value['sha256'] for key, value in runtime['files'].items()}
             and len(runtime['source_snapshot']) > 3000, 'runtime/build binding changed')
        need(build['command'] == ['cargo', *FINAL_COMMANDS['p127-review-release.json'].split()],
             'runtime build command changed')
        bundle.identity(dict(path=root + '/' + name + '/source-overlay.patch', **runtime['source_overlay']))
        for relative, source in runtime['source_snapshot'].items():
            bundle.identity(dict(path=build['cwd'] + '/' + relative, **source))
        for relative, source in build['changed_source_inventory'].items():
            if relative.endswith(('.rs', '.py', '.toml', '.lock')) and 'sha256' in source:
                need(runtime['source_snapshot'].get(relative) == source, 'runtime source differs from build')
    runtime = runtimes[FINAL_RUNTIME]
    need(runtime['source_revision'] == anchors['source_head'], 'final runtime revision changed')
    totals = {}
    for name, command in FINAL_COMMANDS.items():
        gate = gates[name]
        complete_gate(gate)
        need(gate['command'] == ['cargo', *command.split()], 'final gate command changed')
        for relative, source in gate['changed_source_inventory'].items():
            if relative.endswith(('.rs', '.py', '.toml', '.lock')) and 'sha256' in source:
                tested_source(bundle, name, gate, relative, source, runtime['source_snapshot'].get(relative))
        if name in ('p123-review-native-workspace.json', 'p125-review-default-workspace.json'):
            expected = (119, 6539, 0, 35) if 'native' in name else (107, 3289, 0, 0)
            totals[name] = test_totals(bundle.raw(root + '/' + name[:-5] + '.log').decode(), expected)
    publics = {}
    for name, item in indexed['public_reports'].items():
        value = linked(bundle, item)
        publics[name] = public_comparisons(value)
        if value['status'] != 'passed':
            continue
        measured = runtimes[value['runtime']]
        need(value['build_receipt'] == measured['build_receipt']
             and value['build_receipt_sha256'] == measured['build_receipt_sha256'], 'public build binding changed')
        identities = value['native_product_and_source_identity']
        for product, identity in measured['files'].items():
            need(identities[root + '/' + value['runtime'] + '/' + product] == identity, 'public product changed')
    need(publics['ranked-demand-public-review1.json'] == dict(status='passed', accepted_executions=180,
         complete_rows=12465), 'final public acceptance changed')
    package = installed_package(bundle, root, runtime)
    cohorts = {row['name']: row for row in evidence['cohorts']}
    need(len(cohorts) == len(evidence['cohorts']) and cohorts.keys() == anchors['cohorts'].keys(),
         'cohort history incomplete')
    comparisons = {}
    for name, row in cohorts.items():
        for key in ('protocol', 'inputs'):
            need({k: row[key][k] for k in ('bytes', 'sha256')} == anchors['cohorts'][name][key],
                 'frozen cohort anchor changed')
        inputs, protocol = linked(bundle, row['inputs']), linked(bundle, row['protocol'])
        frozen_files(bundle, inputs['frozen_files'], external)
        frozen_files(bundle, protocol['prerequisites'], external)
        frozen_files(bundle, [protocol[key] for key in ('original_execution_observer', 'original_inputs',
                     'source_contract', 'observer_driver')], external)
        measured = linked(bundle, inputs['runtime_manifest'])
        need(measured == runtimes[Path(inputs['runtime_manifest']['path']).parent.name], 'cohort runtime changed')
        for item in inputs['runtime']:
            need(measured['files'][Path(item['path']).name] == dict(bytes=item['generation']['bytes'],
                 sha256=item['sha256']), 'cohort product identity changed')
        versions = {item['path']: item['sha256'] for item in inputs['frozen_files']}
        raw = lambda path: bundle.raw(path, versions.get(path))
        summary = strict_json(raw(root + '/' + name + '/native-observations.json'))
        if name == 'broad-composition-baseline-r1':
            need(summary['status'] == 'failed' and summary['complete'] is False
                 and summary['comparison_engine_invoked'] is False and len(summary['observations']) == 1,
                 'original failed baseline was replaced')
            observation = summary['observations'][0]
            receipt = strict_json(raw(observation['receipt']))
            need(observation['status'] == 'guard_failed' and observation['clocks'] is None
                 and raw(observation['stdout']) == b'' and receipt['stdout_sha256'] == sha(b''),
                 'original incomplete observation gained timing credit')
            comparisons[name] = dict(status='failed', accepted_baseline=False, complete_queries=0)
            continue
        result = replay(raw, root + '/' + name)
        saved = strict_json(raw(root + '/' + name + '/disposition.json'))
        need(all(saved[key] == value for key, value in result.items()), 'replayed candidate decision differs')
        refreshed = inputs['role'] == 'baseline_refresh'
        accepted = refreshed or result['benefit_gate_passed']
        decision = ('baseline_refresh_without_performance_attribution' if refreshed else
                    'retain_lower_complete_engine_total' if accepted else 'drop_no_complete_engine_gain')
        need(saved['accepted_baseline'] is accepted and saved['role'] == inputs['role']
             and saved['decision'] == decision, 'candidate disposition changed')
        comparisons[name] = dict(status='passed', accepted_baseline=accepted,
            engine_seconds=result['candidate_engine_seconds'], baseline_engine_seconds=result['baseline_engine_seconds'],
            complete_queries=result['complete_comparisons'], complete_rows=result['complete_rows'],
            case_flags=result['material_case_regressions'], family_flags=result['material_family_regressions'])
    need(comparisons['broad-projected-order-r1']['accepted_baseline'] is False
         and all(value['accepted_baseline'] for name, value in comparisons.items()
             if name not in {'broad-projected-order-r1', 'broad-composition-baseline-r1'}),
         'original retain/drop history changed')
    full = full43(bundle, root, anchors, runtime, external)
    return dict(schema_version='shardloom.shared_performance_acceptance.v1', status='passed',
        measured_source_head=anchors['source_head'], cohorts=comparisons, full43=full,
        runtime_files=runtime['files'], workspace_tests=totals, public_reports=publics,
        installed_package=package, retained_process_receipts=len(gates), historical_failures=failures,
        historical_doc_digest_only=draft, archived_code_executed=False, new_query_executions=0,
        limitations=['Single observations; no statistical confidence or new competitor comparisons.',
            'Large source/runtime/wheel payloads retain identities only.',
            'Full43 covers direct Parquet; item18 format/surface/output acceptance remains open.',
            'Three early nonruntime doc draft payloads unavailable; receipt identities and all frozen benchmark contracts retained.',
            'Content hashes prove internal consistency, not signed provenance.'])
