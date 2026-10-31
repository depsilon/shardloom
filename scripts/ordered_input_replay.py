# SPDX-License-Identifier: Apache-2.0
"""Replay ordered-input correctness and both independently required benefit gates."""
import gzip
from pathlib import Path

from native_workflow_protocol import strict_json
from ordered_input_cohort_replay import harness_continuity
from ordered_input_public_replay import public_replay
from ordered_input_workflow_replay import workflow_replay
from parquet_demand_completion_replay import replay as replay_cohort
from query_surface.replay import need, sha
from shared_followthrough_replay import frozen_files, installed_package, source_matches
from shared_performance_replay import complete_gate, linked, test_totals

BUILD = 'build --locked --offline --release --features release-user-surfaces -p shardloom-cli -p shardloom-python'
REQUIRED = {
    'o016-ordered-input-native-clippy.json': 'clippy --locked --offline --workspace --all-targets --features release-user-surfaces --profile ci-native -- -D warnings',
    'o018-ordered-input-native-workspace.json': 'test --locked --offline --workspace --all-targets --features release-user-surfaces --profile ci-native',
    'o020-ordered-input-default-clippy.json': 'clippy --locked --offline --workspace --all-targets -- -D warnings',
    'o021-ordered-input-default-workspace.json': 'test --locked --offline --workspace --all-targets',
    'o022-ordered-input-format-check.json': 'fmt --all -- --check',
    'o024-ordered-input-release-build.json': BUILD,
    'o028-ordered-input-release-confirm.json': BUILD,
}
FAILURES = ['o005-ordered-input-native-focused.json', 'o008-ordered-input-native-clippy.json',
            'o014-ordered-input-native-workspace.json', 'o026-ordered-input-public-results.json',
            'o032-ordered-input-assembly.json']
FIXTURE = 'python/tests/test_native_ordered_input_binding.py'
FIXTURE_BEFORE = '33a03dab52eb75a7c925d2415b44be5f953512508b5cd67ff083d6ea07a39353'
FIXTURE_AFTER = '11845491f5a9898621685b550eaeaba5b0f25e04824a0d568ef51f480a8b9b18'
CONTROLS = (
    'local_primitives::native_relational_order::order_check_tests::ordered_input_check_polls_cancellation_on_scan_heap_and_empty_boundaries',
    'local_primitives::native_relational_order::order_check_tests::ordered_input_decline_preserves_exact_original_comparison_and_error_trace',
    'local_primitives::native_relational_order::order_check_tests::ordered_input_check_is_linear_stable_and_keeps_heap_results_for_every_shape',
    'local_primitives::native_relational_keys::order_domain_tests::ordered_input_domain_proof_does_not_replace_strict_float_validation',
    'local_primitives::native_relational_keys::order_domain_tests::ordered_input_table_proof_declines_missing_and_changed_late_domains_without_allocation',
    'local_primitives::native_relational_keys::order_domain_tests::ordered_input_flat_domain_proof_keeps_width_kind_decimal_and_temporal_metadata',
    'local_primitives::native_relational_sort::tests::ordered_input_full_sort_declines_cross_batch_types_before_the_unchanged_heap_error',
    'local_primitives::native_relational_sort::tests::ordered_input_full_sort_keeps_memory_denial_before_comparison',
    'local_primitives::native_relational_aggregate::ordered_output_tests::ordered_input_window_domain_proof_is_shared_across_order_groups',
    'local_primitives::native_relational_aggregate::ordered_output_tests::ordered_input_collection_domain_proof_is_once_per_state_across_output_chunks',
    'local_primitives::prepared_relational::tests::aggregate_tests::ordered_input_tests::ordered_input_all_five_owners_activate_or_return_to_heap_with_complete_results',
    'local_primitives::native_relational_sort::tests::native_relational_sort_validates_payload_after_the_consumer_has_enough_rows',
    'local_primitives::native_relational_sort::tests::native_relational_sort_validates_every_key_before_ordering',
)
FAMILIES = ['composite', 'distinct', 'global_aggregation', 'grouped_aggregation', 'joins',
    'multiway_joins', 'nullable', 'numeric', 'ordering', 'predicates', 'projection', 'sets', 'strings', 'temporal']
RECOVERY_CONTROLS = (
    'numeric_sort_spill_process_death_cleanup_and_exact_restart',
    'exact_distinct_spill_process_death_cleanup_and_exact_restart',
    'weighted_count_spill_process_death_cleanup_and_exact_restart',
    'streaming_join_recovery_refuses_live_and_unknown_owners_then_restarts_exactly',
    'streaming_replay_faults_recovery_preserves_live_and_unknown_owners_then_restarts_exactly',
    'streaming_set_membership_recovery_preserves_live_and_unknown_owners_then_restarts_exactly',
    'streaming_window_recovery_preserves_live_and_unknown_owners_then_restarts_complete_values',
    'native_pivot_spill_recovery_preserves_live_and_unknown_owners_then_restarts',
    'streamed_pivot_spill_recovery_preserves_live_and_unknown_owners_then_restarts',
    'sampling_population_recovery_preserves_live_unknown_and_foreign_owners_then_restarts',
    'subquery_membership_recovery_preserves_live_and_unknown_owners_then_restarts_exactly',
    'unary_membership_recovery_preserves_live_unknown_and_foreign_owners_then_restarts',
)
RECOVERY_FILES = {'shardloom-vortex/src/' + name for name in (
    'local_primitive_operator_recovery_tests.rs',
    'local_primitive_relational_batch_join_recovery_tests.rs',
    'local_primitive_relational_batch_replay_recovery_tests.rs',
    'local_primitive_relational_batch_set_membership_recovery_tests.rs',
    'local_primitive_relational_batch_window_recovery_tests.rs',
    'local_primitive_relational_pivot_spill_recovery_tests.rs',
    'local_primitive_relational_sampling_population_recovery_tests.rs',
    'local_primitive_relational_subquery_membership_recovery_tests.rs',
    'local_primitive_relational_unary_membership_recovery_tests.rs')}


def tested_source_matches(gate, runtime):
    evidence_only = {'scripts/' + name for name in ('ordered_input_replay.py',
        'ordered_input_cohort_replay.py', 'ordered_input_public_replay.py',
        'ordered_input_workflow_replay.py', 'verify_ordered_input_packet.py', 'test_ordered_input_packet.py')}
    checked = dict(gate, changed_source_inventory={name: row
        for name, row in gate['changed_source_inventory'].items() if name not in evidence_only
        and not (name == FIXTURE and row.get('sha256') == FIXTURE_BEFORE
                 and runtime['source_snapshot'][FIXTURE]['sha256'] == FIXTURE_AFTER)})
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
    for name, command in REQUIRED.items():
        gate = gates[name]
        need(gate['command'] == ['cargo', *command.split()], 'required gate command changed')
        tested_source_matches(gate, runtime)
    return runtime


def assembly_replay(bundle, evidence, runtime):
    folder = evidence['root'] + '/ordered-input-assembly-r2'
    capture = strict_json(bundle.raw(folder + '/manifest.json'))
    review = strict_json(bundle.raw(folder + '/review.json'))
    need(capture['binary_sha256'] == runtime['products']['_native.abi3.so']
         and capture['queries_executed'] is False and capture['debug_observation_symbols_absent'] is True
         and review['status'] == 'passed' and review['query_executions'] == 0
         and review['binary_sha256'] == capture['binary_sha256']
         and review['capture_sha256'] == sha(bundle.raw(folder + '/manifest.json')),
         'optimized-code inspection binding changed')
    records = capture['records']
    need(len(records) == 7 and {row['owner'] for row in records} == {
        'shared_check', 'full_sort', 'asof', 'collections', 'window', 'grouped_selectors', 'metadata_proof'},
        'optimized owner coverage incomplete')
    need(capture['failed_capture_gate'] == 'o032-ordered-input-assembly.json'
         and len(capture['helper_snapshots']) == 2, 'failed capture provenance missing')
    for item in capture['helper_snapshots']:
        bundle.identity(item)
    need(records and len({row['symbol'] for row in records}) == len(records)
         and review['reviewed_symbols'] == [row['symbol'] for row in records],
         'optimized-code inspection incomplete')
    for row in records:
        data = bundle.identity(row['identity'])
        need(row['identity']['path'] == folder + '/' + row['file']
             and row['symbol'].encode() in data and data.count(b'\n') == row['lines'],
             'optimized-code evidence changed')
    need(review['ordered_return_before_heap'] is True
         and review['inversion_enters_original_heap'] is True
         and review['owners_covered'] == ['full_sort', 'window', 'asof', 'collections', 'grouped_selectors'],
         'optimized-code mechanism was not established')
    return dict(symbols=len(records), debug_observation_symbols_absent=True, owners=review['owners_covered'])


def native_controls(log):
    lines = log.splitlines()
    for name in CONTROLS:
        need(lines.count('test ' + name + ' ... ok') == 1, 'missing native ordering control: ' + name)
    return dict(passed_controls=len(CONTROLS), names=list(CONTROLS))


def recovery_correction(bundle, evidence, runtime, native_log):
    proof = linked(bundle, evidence['recovery_correction'])
    need(proof['schema_version'] == 1 and proof['status'] == 'passed'
         and proof['runtime_source_changed'] is proof['performance_supervision_changed'] is False
         and proof['query_executions'] == 0, 'recovery correction scope changed')
    bundle.identity(proof['helper'])
    failed = linked(bundle, proof['failed_gate'])
    need(failed['status'] == 'failed' and failed['returncode'] == 101 and failed['head'] == proof['head'],
         'recovery failure was not retained')
    log = bundle.raw(evidence['root'] + '/o014-ordered-input-native-workspace.log').decode()
    need('Error { kind: UnexpectedEof, message: "failed to fill whole buffer" }' in log
         and 'local_primitive_relational_unary_membership_recovery_tests.rs:119:64' in log
         and 'Os { code: 2, kind: NotFound, message: "No such file or directory" }' in log
         and log.count(' ... FAILED') == 1, 'original recovery failure changed')
    sources = proof['corrected_sources']
    need(len(sources) == 9 and {row['path'] for row in sources} == RECOVERY_FILES,
         'recovery correction inventory changed')
    for row in sources:
        relative = row['path']
        need(relative.startswith('shardloom-vortex/src/local_primitive_')
             and relative.endswith('_recovery_tests.rs') and relative not in failed['changed_source_inventory'],
             'recovery correction changed a runtime source')
        before, after = bundle.identity(row['before']), bundle.identity(row['after'])
        old_drop = b'    fn drop(&mut self) {\n        let _ = self.0.kill();'
        new_drop = (b'    fn drop(&mut self) {\n'
            b'        // Child::wait closes its stdin; retain the pipe until termination.\n'
            b'        let _stdin = self.0.stdin.take();\n        let _ = self.0.kill();')
        lines = [line for line in before.splitlines() if line.strip() == b'child.0.kill().unwrap();']
        need(before.count(old_drop) == 1 and len(lines) == 1, 'recovery stop boundary changed')
        line = lines[0]
        indent = line[:line.index(b'child')]
        replacement = (indent + b'// Prevent EOF from unwinding the child and deleting the abandoned runs.\n'
            + indent + b'let _stdin = child.0.stdin.take();\n' + line)
        need(before.replace(old_drop, new_drop, 1).replace(line, replacement, 1) == after
             and runtime['source_snapshot'][relative] == dict(bytes=len(after), sha256=sha(after)),
             'recovery fix changed beyond stdin lifetime or differs from tested source')
    need([Path(item['path']).name for item in proof['passed_gates']] == [
        'o015-ordered-input-recovery-focused.json', 'o016-ordered-input-native-clippy.json',
        'o017-ordered-input-recovery-focused.json', 'o018-ordered-input-native-workspace.json'],
        'recovery correction checks changed')
    need(proof['standard_library']['wait_source'] ==
         '    pub fn wait(&mut self) -> io::Result<ExitStatus> {\n        drop(self.stdin.take());\n        self.handle.wait().map(ExitStatus)\n    }',
         'standard-library pipe-lifetime evidence changed')
    for item in proof['passed_gates']:
        complete_gate(linked(bundle, item))
    for name in RECOVERY_CONTROLS:
        need(sum(line.endswith('::' + name + ' ... ok') for line in native_log.splitlines()) == 1,
             'corrected process lifecycle was not tested: ' + name)
    return dict(test_only_files=9, corrected_lifecycle_controls=len(RECOVERY_CONTROLS),
                original_failure_preserved=True, runtime_source_changed=False)


def combined_disposition(broad, supplemental):
    need(type(broad['benefit_gate_passed']) is bool and type(supplemental['benefit_gate_passed']) is bool,
         'both complete benefit gates are required')
    retained = broad['benefit_gate_passed'] and supplemental['benefit_gate_passed']
    return dict(accepted_baseline_advanced=retained,
        disposition='retain_both_complete_cohort_gains' if retained else 'drop_one_or_both_cohorts_no_gain')


def fixture_correction(bundle, evidence, runtime):
    proof = linked(bundle, evidence['fixture_correction'])
    need(proof['schema_version'] == 1 and proof['status'] == 'passed'
         and proof['query_executions'] == 0 and proof['runtime_source_changed'] is False
         and proof['reference_values_changed'] is False and proof['benchmark_protocol_changed'] is False,
         'public fixture correction scope changed')
    bundle.identity(proof['helper'])
    original = linked(bundle, proof['original_declaration'])
    corrected = linked(bundle, proof['corrected_declaration'])
    need(corrected == linked(bundle, evidence['declaration'])
         and corrected == dict(original, status='fixture_corrected_before_retry',
             native_queries_before_declaration=480, reference_declaration=proof['original_declaration'],
             source_identities=corrected['source_identities'], source_snapshots=corrected['source_snapshots']),
         'public cases or independent references changed during fixture correction')
    expected = {
        'check_ordered_input_public.py': ('31b940244b51cc342552e1979478b81e3929ab417944378d2c922f7d839ffbd2',
                                        'f471159328d61d8b578d4495adba01535b14cc6e9932ef3967efb748e993eeea'),
        'test_native_ordered_input_binding.py': (FIXTURE_BEFORE, FIXTURE_AFTER),
    }
    need(len(proof['changed_sources']) == 2
         and {Path(row['path']).name for row in proof['changed_sources']} == set(expected),
         'public fixture correction inventory changed')
    for old, new in zip(original['source_identities'], corrected['source_identities'], strict=True):
        need(old['path'] == new['path'], 'public source inventory changed')
        if Path(old['path']).name not in expected:
            need(old == new, 'independent fixture source changed')
    for row in proof['changed_sources']:
        before, after = bundle.identity(row['before']), bundle.identity(row['after'])
        need((sha(before), sha(after)) == expected[Path(row['path']).name],
             'public fixture correction changed beyond the reviewed export repair')
        matches = [(old, new) for old, new in zip(original['source_identities'], corrected['source_identities'], strict=True)
                   if old['path'] == new['path'] == row['path']]
        need(len(matches) == 1 and (matches[0][0]['sha256'], matches[0][1]['sha256'])
             == (sha(before), sha(after)), 'corrected execution source was not declared')
    old_runtime = linked(bundle, proof['original_runtime'])
    need(old_runtime['products'] == runtime['products'] and old_runtime['files'] == runtime['files'],
         'fixture correction changed a release product')
    for relative, source in old_runtime['source_snapshot'].items():
        if not relative.startswith('scripts/') and relative != FIXTURE:
            need(runtime['source_snapshot'][relative] == source, 'fixture correction changed runtime source')
    need(runtime['source_snapshot'][FIXTURE]['sha256'] == FIXTURE_AFTER, 'corrected fixture was not frozen')
    failed_gate = linked(bundle, proof['failed_gate'])
    need(failed_gate['status'] == 'failed' and failed_gate['returncode'] == 1
         and failed_gate['process_group_drained'] is True, 'failed public attempt missing')
    failed = linked(bundle, proof['failed_public'])
    need(failed['status'] == 'failed' and failed['comparisons'] == len(failed['parts']) == 474
         and failed['complete_rows'] == 1945428 and len(failed['errors']) == 6
         and failed['failures'] == failed['skipped'] == [] and failed['posthash_unchanged'] is True
         and linked(bundle, failed['runtime']) == old_runtime
         and linked(bundle, failed['declaration']) == original, 'original public failure changed')
    errors = {f"template='{template}', rows=65537, format='{form}', surface='cli_sql'"
              for template in ('sort_ascending', 'sort_shuffled') for form in ('parquet', 'arrow', 'vortex')}
    for test, error in failed['errors']:
        matches = [key for key in errors if key in test]
        need(len(matches) == 1 and 'collect exceeds 65,536 rows; use an explicit streaming export' in error,
             'original public failure cause changed')
        errors.remove(matches[0])
    need(not errors, 'original public failure coverage changed')
    for item in failed['parts']:
        data = gzip.decompress(bundle.identity(item))
        need(len(data) == item['raw_bytes'] and sha(data) == item['raw_sha256'],
             'failed public attempt lost a complete observation')
    return dict(preserved_complete_comparisons=474, corrected_cli_exports=6,
                references_unchanged=True, release_products_identical=True)


def verify(bundle, anchors):
    evidence = bundle.index['evidence']
    need(evidence == anchors and evidence['kind'] == 'ordered_input', 'ordered-input anchors changed')
    root = evidence['root']
    external = {(item['path'], item['sha256'], item['bytes']) for item in evidence['external_artifacts']}
    need(len(external) == len(evidence['external_artifacts']), 'duplicate external payload identity')
    gates = gates_replay(bundle, evidence)
    runtime = runtime_replay(bundle, evidence, gates)
    native_log = bundle.raw(root + '/o018-ordered-input-native-workspace.log').decode()
    native = test_totals(native_log, (119, 6590, 0, 36))
    default = test_totals(bundle.raw(root + '/o021-ordered-input-default-workspace.log').decode(), (107, 3290, 0, 0))
    mechanism = native_controls(native_log)
    correction = recovery_correction(bundle, evidence, runtime, native_log)
    public_correction = fixture_correction(bundle, evidence, runtime)
    for name in ('o001-ordered-input-fixture-controls.json', 'o002-ordered-input-declarations.json',
                 'o003-ordered-input-accepted-freeze.json', 'o004-ordered-input-accepted-replay.json',
                 'o007-ordered-input-public-declarations.json', 'o012-ordered-input-saved-baseline.json',
                 'o013-ordered-input-native-focused.json', 'o015-ordered-input-recovery-focused.json',
                 'o017-ordered-input-recovery-focused.json', 'o019-ordered-input-recovery-correction.json',
                 'o023-ordered-input-python-source.json', 'o025-ordered-input-runtime-freeze.json',
                 'o027-ordered-input-fixture-correction.json', 'o029-ordered-input-runtime-freeze-r2.json',
                 'o030-ordered-input-public-results-r2.json', 'o031-ordered-input-installed-package.json',
                 'o033-ordered-input-assembly-r2.json'):
        complete_gate(gates[name])
    python = gates['o023-ordered-input-python-source.json']
    need(python['command'][1:] == ['-B', '-m', 'unittest', 'discover', '-s', 'python/tests', '-v'],
         'Python source check command changed')
    tested_source_matches(python, runtime)
    need('Ran 918 tests in ' in bundle.raw(root + '/o023-ordered-input-python-source.log').decode()
         and 'OK (skipped=40)' in bundle.raw(root + '/o023-ordered-input-python-source.log').decode(),
         'Python source coverage changed')
    declaration = linked(bundle, evidence['declaration'])
    public = linked(bundle, evidence['public_report'])
    public_result = public_replay(bundle, public, declaration)
    need(linked(bundle, public['runtime']) == runtime, 'public runtime binding changed')
    folder = str(Path(evidence['runtime']['path']).parent)
    for name, identity in runtime['files'].items():
        need(public['prehash'][folder + '/' + name] == dict(path=folder + '/' + name, **identity),
             'public product identity changed')
    frozen_files(bundle, list(public['prehash'].values()), external)
    package = installed_package(bundle, root + '/installed-ordered-input-r1', runtime, 285)
    assembly = assembly_replay(bundle, evidence, runtime)
    supplemental = evidence['supplemental_cohorts']
    baseline = workflow_replay(bundle, root + '/' + supplemental['accepted']['name'], external)
    need(linked(bundle, evidence['protocol']) == linked(bundle, supplemental['accepted']['protocol']),
         'supplemental original protocol changed')
    summary = dict(status='passed', native_workspace=native, default_workspace=default,
        public_acceptance=public_result, installed_package=package, failed_validation_attempts=FAILURES,
        mechanism=dict(**mechanism, assembly=assembly), recovery_test_correction=correction,
        public_fixture_correction=public_correction,
        supplemental_baseline=baseline, external_engine_invoked=False, full43_run=False, query_executions=0)
    if supplemental['candidate'] is None:
        need(evidence['cohort'] is None, 'broad result cannot replace missing supplemental candidate')
        return dict(**summary, disposition='local_correctness_only_performance_pending')
    candidate = supplemental['candidate']
    candidate_inputs = linked(bundle, candidate['inputs'])
    need(linked(bundle, candidate_inputs['runtime_manifest']) == runtime,
         'supplemental candidate runtime changed')
    supplemental_result = workflow_replay(bundle, root + '/' + candidate['name'], external)
    if evidence['cohort'] is None:
        return dict(**summary, supplemental_comparison=supplemental_result,
                    disposition='supplemental_complete_broad_performance_pending')
    cohort = evidence['cohort']
    inputs, protocol = linked(bundle, cohort['inputs']), linked(bundle, cohort['protocol'])
    frozen_files(bundle, inputs['frozen_files'] + protocol['prerequisites'], external)
    need(linked(bundle, inputs['runtime_manifest']) == runtime and inputs['repeats'] == 1
         and inputs['memory_bytes'] == 2 << 30 and inputs['parallelism'] == 2
         and protocol['performance_executions_before_freeze'] == 0
         and protocol['source_contract']['sha256'] == '6ce7842b3719312f25336fa19318fb30ae922984f122d9adb0487b655baa7db2',
         'broad cohort admission changed')
    need(protocol['retention_decision'] == dict(rule='strict_complete_cohort_gain',
        declared_families=FAMILIES,
        material_case_regression=dict(fraction=0.20, absolute_seconds=0.010),
        material_family_regression=dict(fraction=0.20, absolute_seconds=0.050)), 'frozen benefit rule changed')
    continuity = harness_continuity(bundle, inputs, protocol, runtime)
    versions = {item['path']: item['sha256'] for item in inputs['frozen_files']}
    raw = lambda path: bundle.raw(path, versions.get(path))
    result = replay_cohort(raw, root + '/' + cohort['name'])
    saved = strict_json(raw(root + '/' + cohort['name'] + '/disposition.json'))
    need(result['complete_comparisons'] == 347 and result['complete_rows'] == 12565
         and result['baseline_engine_seconds'] == 11.997152207
         and all(saved[key] == value for key, value in result.items())
         and saved['accepted_baseline'] is result['benefit_gate_passed'] and saved['role'] == 'candidate'
         and saved['decision'] == ('retain_lower_complete_engine_total' if result['benefit_gate_passed']
             else 'drop_no_complete_engine_gain'), 'broad complete-cohort disposition changed')
    return dict(**summary, supplemental_comparison=supplemental_result, comparison=result,
        harness_continuity=continuity, **combined_disposition(result, supplemental_result))
