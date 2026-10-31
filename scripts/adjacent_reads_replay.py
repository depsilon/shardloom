# SPDX-License-Identifier: Apache-2.0
"""Verify adjacent-read semantics, physical work, ownership and complete-cohort evidence."""
from collections import Counter
from pathlib import Path

from adjacent_reads_cohort_replay import harness_continuity
from adjacent_reads_public_replay import public_replay, read_work
from grouped_state_public_replay import public_parts
from native_workflow_protocol import strict_json
from parquet_demand_completion_replay import replay as replay_cohort
from query_surface.replay import contract as result_contract, need, sha
from query_surface.semantics import compare, result_from_record
from shared_followthrough_replay import frozen_files, installed_package, source_matches
from shared_performance_replay import complete_gate, linked, test_totals

BUILD = 'build --locked --offline --release --features release-user-surfaces -p shardloom-cli -p shardloom-python'
BASELINE_SECONDS = 11.997152207
REQUIRED = {
    'a005-adjacent-native-clippy.json': 'clippy --locked --offline --workspace --all-targets --features release-user-surfaces --profile ci-native -- -D warnings',
    'a007-adjacent-native-workspace.json': 'test --locked --offline --workspace --all-targets --features release-user-surfaces --profile ci-native',
    'a008-adjacent-default-clippy.json': 'clippy --locked --offline --workspace --all-targets -- -D warnings',
    'a009-adjacent-default-workspace.json': 'test --locked --offline --workspace --all-targets',
    'a010-adjacent-format-check.json': 'fmt --all -- --check',
    'a016-adjacent-release-build.json': BUILD,
}
FAILURES = ['a001-adjacent-chunks-focused.json', 'a003-adjacent-native-clippy.json',
            'a004-adjacent-native-clippy.json', 'a014-adjacent-public-results.json']
READ_CONTROLS = (
    'universal_format_io::demanded::chunks::read_tests::selected_reads_combine_only_exact_adjacent_nonempty_ranges_in_metadata_order',
    'universal_format_io::demanded::chunks::read_tests::coalesced_slices_keep_decoder_boundaries_and_last_alias_credit',
    'universal_format_io::demanded::chunks::read_tests::selected_reads_preserve_individual_reads_at_combined_capacity_boundary',
    'universal_format_io::demanded::chunks::read_tests::selected_reads_preserve_prefix_before_later_metadata_or_memory_failure',
    'universal_format_io::demanded::chunks::read_tests::selected_read_errors_and_cancellation_precede_malformed_later_neighbors',
    'universal_format_io::demanded::chunks::read_tests::adjacent_selected_reads_fit_the_actual_shared_io_envelope',
    'universal_format_io::demanded::chunks::read_tests::parquet_tests::coalesced_real_parquet_nested_dictionary_leaves_keep_every_file_group_and_value',
    'resident_session::io_ownership::tests::optional_coalescing_observes_remaining_bytes_and_request_slots_without_admission',
)


def tested_source_matches(gate, runtime):
    # Saved-result validators are not product inputs or Cargo tests. Preserve
    # their gate-time bytes while keeping all actual engine/test inputs exact.
    evidence_only = {'scripts/' + name for name in ('adjacent_reads_replay.py',
        'adjacent_reads_cohort_replay.py', 'adjacent_reads_public_replay.py',
        'verify_adjacent_reads_packet.py', 'test_adjacent_reads_packet.py')}
    checked = dict(gate, changed_source_inventory={name: row
        for name, row in gate['changed_source_inventory'].items() if name not in evidence_only})
    source_matches(checked, runtime)


def gates_replay(bundle, evidence):
    root = evidence['root']
    gates, failures = {}, []
    for item in evidence['gates']:
        name = Path(item['path']).name
        need(name not in gates, 'duplicate process gate')
        gate = gates[name] = linked(bundle, item)
        need(gate['status'] == item['status'] and (gate['returncode'] == 0) == (gate['status'] == 'passed')
             and sha(bundle.raw(root + '/' + name[:-5] + '.log')) == gate['log_sha256'],
             'process result or log changed')
        if gate['status'] == 'passed':
            complete_gate(gate)
        else:
            failures.append(name)
        for relative, source in gate['changed_source_inventory'].items():
            if 'sha256' in source:
                bundle.identity(dict(path=gate['cwd'] + '/' + relative, **source))
        need(sha(bundle.raw(root + '/source-blobs/' + gate['driver_sha256'])) == gate['driver_sha256'],
             'process driver changed')
    need(failures == FAILURES and set(REQUIRED) <= gates.keys(), 'required gate or failure inventory changed')
    return gates


def fixture_correction(bundle, evidence, runtime):
    before = linked(bundle, evidence['original_runtime'])
    proof = linked(bundle, evidence['fixture_correction'])
    failed = linked(bundle, evidence['failed_public_report'])
    need(before['files'] == runtime['files'] and before['products'] == runtime['products'],
         'fixture correction changed compiled products')
    for name, item in before['source_snapshot'].items():
        if name.endswith(('.rs', '.toml', '.lock')) or name.startswith('python/src/'):
            need(runtime['source_snapshot'].get(name) == item, 'fixture correction changed runtime input')
    fixture = 'python/tests/test_native_adjacent_reads_binding.py'
    original, corrected = (bundle.identity(proof[key]) for key in ('original_fixture', 'corrected_fixture'))
    removed = b'            if group.num_rows == 0:\n                continue\n'
    need(original.count(removed) == 1 and original.replace(removed, b'', 1) == corrected
         and bundle.identity(proof['retained_original']) == original
         and proof['exact_removed_text'] == removed.decode(), 'fixture correction changed more than empty-group prediction')
    for key, snapshot in (('original_fixture', before), ('corrected_fixture', runtime)):
        need({field: proof[key][field] for field in ('bytes', 'sha256')} == snapshot['source_snapshot'][fixture],
             'fixture correction source identity changed')
    need(proof['status'] == 'passed' and proof['runtime_source_changes'] == []
         and proof['query_executions'] == 0 and proof['frozen_347_changed'] is False
         and proof['benchmark_execution'] is False and linked(bundle, proof['failed_public_report']) == failed
         and linked(bundle, proof['original_runtime']) == before, 'fixture proof boundary changed')
    need(failed['status'] == 'failed' and failed['failures'] == 16 and failed['errors'] == failed['skipped'] == 0
         and failed['posthash_verified'] is True and failed['positive_public_executions'] == 450
         and failed['compared_rows'] == 143235 and Counter(row['detail'].splitlines()[-1]
         for row in failed['failure_details']) == {'AssertionError: 1 != 0': 5, 'AssertionError: 2 != 0': 5,
         'AssertionError: 3 != 0': 5, 'AssertionError: 435 != 450': 1}, 'public failure history changed')
    regenerated = {row['fact_rows']: row for row in proof['regenerated_sources']}
    need(set(regenerated) == {0, 37, 2053} and len(proof['regenerated_sources']) == 3,
         'metadata regeneration coverage changed')
    for size, row in regenerated.items():
        source = next(item['files']['fact'][0] for item in failed['source_fixtures']
                      if item['format'] == 'parquet' and item['dataset']['fact_rows'] == size)
        need(row['original'] == source and row['independently_regenerated'] ==
             {key: source[key] for key in ('bytes', 'sha256')}, 'metadata source identity changed')
    need(regenerated[0]['metadata_row_groups'] == 1 and regenerated[0]['metadata_group_rows'] == [0],
         'empty encoded row group omitted')
    comparisons = rows = physical = 0
    for row in public_parts(bundle, failed):
        compare(result_from_record(row['expected']), result_from_record(row['actual']), result_contract(row['contract']))
        comparisons += 1
        rows += row['rows']
        size = next(size for size in regenerated if row['case'].startswith(f'adjacent_read_n{size}_'))
        name = row['case'].removeprefix(f'adjacent_read_n{size}_')
        if row['format'] == 'parquet' and name in regenerated[size]['read_predictions']:
            work = regenerated[size]['read_predictions'][name]
            read_work(work, size, name, row['report']['fields'], regenerated[size]['original']['bytes'])
            need(row['report']['acceptance_expected_parquet_read_work'] == (work if size else
                 dict(groups=[], expected_requests=0, individual_chunk_requests=0, expected_bytes=0)),
                 'original failed prediction changed')
            physical += 1
    need((comparisons, rows, physical) == (450, 143235, 45)
         == (proof['complete_saved_comparisons'], proof['complete_saved_rows'], proof['physical_read_controls']),
         'saved failure comparisons incomplete')
    return before


def runtime_replay(bundle, evidence, gates):
    root = evidence['root']
    runtime = linked(bundle, evidence['runtime'])
    build = gates[runtime['build_receipt']]
    need(build['command'] == ['cargo', *BUILD.split()] and runtime['profile'] == 'release'
         and runtime['features'] == ['release-user-surfaces'] and runtime['source_revision'] == build['head']
         and sha(bundle.raw(root + '/' + runtime['build_receipt'])) == runtime['build_receipt_sha256']
         and set(runtime['files']) == {'shardloom', '_native.abi3.so'}
         and runtime['products'] == {key: value['sha256'] for key, value in runtime['files'].items()},
         'runtime build binding changed')
    folder = str(Path(evidence['runtime']['path']).parent)
    bundle.identity(dict(path=folder + '/source-overlay.patch', **runtime['source_overlay']))
    for relative, source in runtime['source_snapshot'].items():
        bundle.identity(dict(path=build['cwd'] + '/' + relative, **source))
    before = fixture_correction(bundle, evidence, runtime)
    original_build = gates[before['build_receipt']]
    need(original_build['command'] == ['cargo', *BUILD.split()] and
         sha(bundle.raw(root + '/' + before['build_receipt'])) == before['build_receipt_sha256'],
         'original build receipt changed')
    tested_source_matches(original_build, before)
    for name, command in REQUIRED.items():
        gate = gates[name]
        need(gate['command'] == ['cargo', *command.split()], 'required gate command changed')
        tested_source_matches(gate, runtime if name == runtime['build_receipt'] else before)
    return runtime


def native_controls(log):
    lines = log.splitlines()
    for name in READ_CONTROLS:
        need(lines.count('test ' + name + ' ... ok') == 1, 'missing native read/ownership control: ' + name)
    return dict(passed_controls=len(READ_CONTROLS), names=list(READ_CONTROLS))


def verify(bundle, anchors):
    evidence = bundle.index['evidence']
    need(evidence == anchors and evidence['kind'] == 'adjacent_reads', 'adjacent-read anchors changed')
    root = evidence['root']
    external = {(item['path'], item['sha256'], item['bytes']) for item in evidence['external_artifacts']}
    need(len(external) == len(evidence['external_artifacts']), 'duplicate external payload identity')
    gates = gates_replay(bundle, evidence)
    runtime = runtime_replay(bundle, evidence, gates)
    native_log = bundle.raw(root + '/a007-adjacent-native-workspace.log').decode()
    native = test_totals(native_log, (119, 6587, 0, 36))
    default = test_totals(bundle.raw(root + '/a009-adjacent-default-workspace.log').decode(), (107, 3290, 0, 0))
    for name in ('a002-adjacent-chunks-focused.json', 'a006-adjacent-public-declarations.json',
                 'a011-adjacent-python-source.json', 'a015-adjacent-fixture-correction.json',
                 'a017-adjacent-runtime-freeze.json', 'a018-adjacent-public-results.json',
                 'a019-adjacent-installed-package.json'):
        complete_gate(gates[name])
    mechanism = native_controls(native_log)
    declaration = linked(bundle, evidence['declarations'])
    for path, digest in declaration['sources'].items():
        bundle.raw(path, digest)
    contract = linked(bundle, evidence['contract'])
    need(contract['runtime_edited'] is False
         and contract['head'] == '56722887e9980bcc48ff2a293905b4acab2e308c'
         and contract['path'] == 'docs/architecture/native-adjacent-parquet-reads-2026-10-31.md'
         and sha(contract['text'].encode()) == contract['sha256']
         == '3789a746d0c9f8d6120d3c94853d1ea88772e44976945cfc8a96bf83e99cd36e',
         'prospective adjacent contract changed')
    bundle.raw(gates[runtime['build_receipt']]['cwd'] + '/' + contract['path'], contract['sha256'])
    public = linked(bundle, evidence['public_report'])
    public_result = public_replay(bundle, public, declaration)
    need(public['build_receipt'] == runtime['build_receipt']
         and public['build_receipt_sha256'] == runtime['build_receipt_sha256'], 'public build binding changed')
    folder = str(Path(evidence['runtime']['path']).parent)
    for name, identity in runtime['files'].items():
        need(public['native_product_and_source_identity'][folder + '/' + name] == identity,
             'public product identity changed')
    frozen_files(bundle, [dict(path=path, **item)
        for path, item in public['native_product_and_source_identity'].items()], external)
    package = installed_package(bundle, root + '/installed-adjacent-reads-r1', runtime, 285)
    summary = dict(status='passed', native_workspace=native, default_workspace=default,
        public_acceptance=public_result, installed_package=package, failed_validation_attempts=FAILURES,
        mechanism=mechanism, external_engine_invoked=False, full43_run=False, query_executions=0)
    cohort = evidence['cohort']
    if cohort is None:
        return dict(**summary, disposition='local_correctness_only_performance_pending')
    for name in ('a021-adjacent-cohort-freeze.json', 'a022-adjacent-cohort-replay.json'):
        complete_gate(gates[name])
    inputs, protocol = linked(bundle, cohort['inputs']), linked(bundle, cohort['protocol'])
    frozen_files(bundle, inputs['frozen_files'] + protocol['prerequisites'], external)
    need(linked(bundle, inputs['runtime_manifest']) == runtime and inputs['repeats'] == 1
         and inputs['memory_bytes'] == 2 << 30 and inputs['parallelism'] == 2
         and protocol['performance_executions_before_freeze'] == 0
         and protocol['source_contract']['sha256'] == contract['sha256'], 'cohort admission changed')
    need(protocol['retention_decision'] == dict(rule='strict_complete_cohort_gain',
        declared_families=['projection', 'joins', 'multiway_joins', 'grouped_aggregation'],
        material_case_regression=dict(fraction=0.20, absolute_seconds=0.010),
        material_family_regression=dict(fraction=0.20, absolute_seconds=0.050)), 'frozen benefit rule changed')
    continuity = harness_continuity(bundle, inputs, protocol, runtime, declaration)
    versions = {item['path']: item['sha256'] for item in inputs['frozen_files']}
    raw = lambda path: bundle.raw(path, versions.get(path))
    result = replay_cohort(raw, root + '/' + cohort['name'])
    saved = strict_json(raw(root + '/' + cohort['name'] + '/disposition.json'))
    need(result['complete_comparisons'] == 347 and result['complete_rows'] == 12565
         and result['baseline_engine_seconds'] == BASELINE_SECONDS
         and all(saved[key] == value for key, value in result.items())
         and saved['accepted_baseline'] is result['benefit_gate_passed'] and saved['role'] == 'candidate'
         and saved['decision'] == ('retain_lower_complete_engine_total' if result['benefit_gate_passed']
             else 'drop_no_complete_engine_gain'), 'complete-cohort disposition changed')
    return dict(**summary, disposition=saved['decision'], comparison=result, harness_continuity=continuity)
