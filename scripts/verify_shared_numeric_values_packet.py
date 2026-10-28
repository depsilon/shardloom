# SPDX-License-Identifier: Apache-2.0
"""Verify numeric value execution acceptance from complete saved results and checks."""
from pathlib import Path
import argparse
import json
import re
import sys

REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(REPO / 'scripts'), str(REPO / 'python/src'),
               str(REPO / 'benchmarks/traditional_analytics')]
from native_workflow_protocol import strict_json
from parquet_demand_completion_replay import need, replay
from query_surface.replay import MAX_ARCHIVE, MAX_FILE, bounded_bytes, object_files, sha
from numeric_values_replay import replay_heldout
from verify_shared_analytical_performance_packet import diagnostic
from verify_shared_key_lookup_packet import verify as verify_previous

SOURCE_HEAD = 'e13fe9d36b9ff3cca3a51733dc49b14bee356aec'
BASELINE_HEAD = '74c7cd652b2fccaeb4fa12292be40137b2dd6a58'
PROTOCOL_SHA = '605ad9bea227e7aa86f748699f2e4d9a5414745c0711ccc627dfb5d61132f090'
PREVIOUS_SHA = '65c9af8f100581f17fec2a5b68a851c94fc243264fe0acfe5082b4e9de6b0361'
HELDOUT_SHA = '61197eb0bfaf817e20b7a5bcd411da3cf7b0a23c21ba9ac4ef09843f1f8fe24a'
DIAGNOSTIC_SHA = 'e1c9cb96016fc843b2e01a5969aba7375cf84094cae292322f5df986e73478b5'
ORIGINAL_FLAG = 'aggregate_signed_value_group_key_sum_arithmetic_one_percent'
FAILED = {'r01-native-typed-tests', 'r02-native-numeric-tests', 'r04-native-clippy'}
EXPECTED_GATES = {
    'r01-native-typed-tests', 'r02-native-numeric-tests', 'r03-native-numeric-tests',
    'r04-native-clippy', 'r04b-native-clippy', 'r05-native-tests', 'r06-workspace-fmt',
    'r07-workspace-clippy', 'r08-workspace-tests', 'r09-prepare-wheel-tools',
    'r10-candidate-numeric-release-build', 'r11-freeze-runtime', 'r12-public-numeric-surfaces',
    'r13-heldout-freeze', 'r14-heldout-bind', 'r15-heldout-replay',
    'r16-native-surface-clippy', 'r17-native-cli-suite', 'r18-installed-wheel-proof',
    'r19-broad-freeze', 'r20-broad-replay',
    'r21-diagnostic-freeze', 'r22-diagnostic-replay',
}
COMMANDS = {
    'r04b-native-clippy': 'clippy --locked --offline --profile ci-native -p shardloom-vortex --features release-user-surfaces --all-targets -- -D warnings',
    'r05-native-tests': 'test --locked --offline --profile ci-native -p shardloom-vortex --features release-user-surfaces --all-targets -- --test-threads=2',
    'r06-workspace-fmt': 'fmt --all -- --check',
    'r07-workspace-clippy': 'clippy --locked --offline --workspace --all-targets -- -D warnings',
    'r08-workspace-tests': 'test --locked --offline --workspace --all-targets -- --test-threads=2',
    'r10-candidate-numeric-release-build': 'build --locked --offline -p shardloom-cli -p shardloom-python --features release-user-surfaces --release',
    'r16-native-surface-clippy': 'clippy --locked --offline -p shardloom-vortex -p shardloom-cli -p shardloom-python --features release-user-surfaces --all-targets --profile ci-native -- -D warnings',
    'r17-native-cli-suite': 'test --locked --offline -p shardloom-cli --features release-user-surfaces --profile ci-native --lib -- --test-threads=2',
}
ACTIVATION_TESTS = [
    'numeric_extrema_match_all_widths_nulls_slices_empty_and_exact_limits',
    'numeric_extrema_keep_encoded_inputs_and_first_equal_zero_bits',
    'numeric_extrema_preserve_validation_overflow_precedence_and_partial_state',
    'numeric_extrema_keep_exact_cross_batch_mixed_numeric_comparisons',
    'numeric_extrema_preserve_filtered_domain_and_activate_shared_consumer',
    'numeric_extrema_allocate_no_kernel_and_check_cancellation_on_null_rows',
    'numeric_output_copy_matches_all_widths_empty_null_sliced_and_encoded_values',
    'numeric_output_copy_validates_only_nonnull_values_and_preserves_float_bits',
    'numeric_output_copy_preserves_temporal_metadata_tick_checks_and_null_payloads',
    'numeric_output_copy_keeps_cancellation_error_order_and_releases_refused_buffers',
    'numeric_output_copy_survives_producer_drop_and_releases_its_own_clone_slice_credits',
    'numeric_output_take_preserves_duplicate_order_null_indices_and_selected_validation',
]


def verify(files, manifest):
    need(manifest['verification_kind'] == 'shared_numeric_values', 'wrong verification kind')
    root = manifest['original_evidence_root']

    def raw(path):
        if path in manifest.get('chunked_files', {}):
            record = manifest['chunked_files'][path]
            need(type(record['bytes']) is int and 0 <= record['bytes'] <= MAX_ARCHIVE,
                 'chunked evidence exceeds bound')
            parts = record['parts']
            need(0 < len(parts) <= MAX_ARCHIVE // MAX_FILE
                 and len({part['name'] for part in parts}) == len(parts), 'invalid evidence chunks')
            chunks = []
            for part in parts:
                need(part['name'] in files, 'missing evidence chunk')
                data = files[part['name']]
                need(len(data) == part['bytes'] and 0 < len(data) <= MAX_FILE
                     and sha(data) == part['sha256'], 'evidence chunk changed')
                chunks.append(data)
            need(sum(map(len, chunks)) == record['bytes'], 'chunked evidence size changed')
            data = b''.join(chunks)
            need(sha(data) == record['sha256'], 'chunked evidence digest changed')
            return data
        name = manifest['path_map'].get(path)
        need(name in files, 'missing linked evidence: ' + path)
        return files[name]

    def local(name):
        return strict_json(raw(root + '/' + name))

    external = {(item['path'], item['sha256'], item['bytes']) for item in manifest['external_artifacts']}

    def check_frozen(items):
        for item in items:
            size = item['generation']['bytes']
            if item['path'] in manifest['path_map'] or item['path'] in manifest.get('chunked_files', {}):
                data = raw(item['path'])
                need(sha(data) == item['sha256'] and len(data) == size,
                     'frozen source or evidence changed: ' + item['path'])
            else:
                need((item['path'], item['sha256'], size) in external,
                     'unrepresented frozen payload: ' + item['path'])

    need(sha(raw(root + '/protocol-numeric-r1.json')) == PROTOCOL_SHA, 'prospective protocol changed')
    protocol = local('protocol-numeric-r1.json')
    prior_manifest = strict_json(files['previous/manifest.json'])
    prior_archive = raw(manifest['previous_archive_path'])
    need(sha(prior_archive) == PREVIOUS_SHA, 'previous accepted packet changed')
    prior_files = object_files(prior_manifest, prior_archive)
    previous = verify_previous(prior_files, prior_manifest)
    baseline = previous['comparison']
    need(baseline['candidate_engine_seconds'] == protocol['baseline_engine_seconds']
         and protocol['baseline_head'] == BASELINE_HEAD, 'accepted baseline changed')
    frozen = local('broad-numeric-r1/inputs.json')
    need(frozen['role'] == 'candidate', 'runtime role changed')
    check_frozen(frozen['frozen_files'])
    for name in ('baseline_inputs', 'baseline_observations'):
        path = frozen[name]['path']
        original = next((value for value in prior_manifest['path_map'] if value.casefold() == path.casefold()), None)
        need(original is not None and raw(path) == prior_files[prior_manifest['path_map'][original]],
             'previous accepted observation changed')
    comparison = replay(raw, root + '/broad-numeric-r1')
    need(comparison == local('broad-numeric-r1-comparison.json'), 'comparison differs from complete results')
    need(frozen['baseline_complete_result_replay']['verification'] == baseline,
         'candidate changed the accepted baseline')
    need(comparison['benefit_gate_passed'] is True
         and comparison['material_case_regressions'] == [ORIGINAL_FLAG]
         and not comparison['material_family_regressions'], 'original retention evidence changed')
    diagnostic_root = root + '/numeric-aggregate-diagnostic-r1'
    need(sha(raw(diagnostic_root + '/protocol.json')) == DIAGNOSTIC_SHA,
         'prospective diagnostic protocol changed')
    diagnostic_protocol = strict_json(raw(diagnostic_root + '/protocol.json'))
    check_frozen(diagnostic_protocol['frozen_files'])
    need([case['case_id'] for case in diagnostic_protocol['cases']] == [
        'aggregate_signed_value_group_key_sum_direct_one_percent', ORIGINAL_FLAG,
        'aggregate_signed_value_group_key_sum_coalesce_one_percent'], 'diagnostic neighbors changed')
    disposition = diagnostic(raw, diagnostic_root, frozen,
                             strict_json(raw(frozen['baseline_inputs']['path'])))
    need(disposition == local('numeric-aggregate-diagnostic-r1-replay.json'),
         'diagnostic replay summary changed')

    gates = {}
    for path in manifest['path_map']:
        if not path.startswith(root + '/'):
            continue
        name = path[len(root) + 1:]
        match = re.fullmatch(r'r(\d+)[a-z]?-[^/]+\.json', name)
        if not match:
            continue
        need(name[:-5] in EXPECTED_GATES and name[:-5] not in gates, 'duplicate or unexpected original gate')
        value = gates[name[:-5]] = local(name)
        need(value['head'] == SOURCE_HEAD
             and sha(raw(root + '/' + name[:-5] + '.log')) == value['log_sha256'],
             'gate revision or log changed')
        need(value['process_group_drained'] is True and value['direct_child_reaped'] is True
             and value['timed_out'] is False and value['interrupted_signal'] is None,
             'gate process cleanup incomplete')
        if name[:-5] in FAILED:
            need(value['status'] == 'failed' and value['returncode'] != 0, 'historical failure replaced')
        else:
            need(value['status'] == 'passed' and value['returncode'] == 0, 'required gate failed')
    need(gates.keys() == EXPECTED_GATES, 'original build/test history incomplete')
    for name, command in COMMANDS.items():
        need(gates[name]['command'] == ['cargo', *command.split()], 'wrong gate command: ' + name)
    totals = {}
    for name, expected in (('r05-native-tests', (3471, 0, 32)),
                           ('r08-workspace-tests', (3274, 0, 0)), ('r17-native-cli-suite', (1032, 0, 0))):
        rows = re.findall(r'test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;',
                          raw(root + '/' + name + '.log').decode())
        count = tuple(sum(int(row[index]) for row in rows) for index in (1, 2, 3))
        need(rows and all(row[0] == 'ok' for row in rows) and count == expected, 'test coverage changed')
        totals[name] = dict(zip(('passed', 'failed', 'ignored'), count))
    test_log = raw(root + '/r05-native-tests.log').decode()
    need(all(name + ' ... ok' in test_log for name in ACTIVATION_TESTS), 'numeric activation test absent')

    runtime = local('candidate-numeric-release-r1/manifest.json')
    build_name = 'r10-candidate-numeric-release-build'
    build = gates[build_name]
    need(runtime['profile'] == 'release' and runtime['build_receipt'] == build_name + '.json'
         and runtime['build_receipt_sha256'] == sha(raw(root + '/' + build_name + '.json'))
         and runtime['changed_source_inventory'] == build['changed_source_inventory'], 'runtime build changed')
    expected_sources = {name for name, item in build['changed_source_inventory'].items()
                        if 'sha256' in item and not name.startswith(('docs/', 'website'))}
    need(set(runtime['source_snapshot']) == expected_sources, 'runtime source snapshot coverage changed')
    for name, item in runtime['source_snapshot'].items():
        source = raw(root + '/candidate-numeric-release-r1/source/' + name)
        need(sha(source) == item['sha256'] and len(source) == item['bytes']
             and item == build['changed_source_inventory'][name], 'runtime source differs from build')
        if name.endswith(('.rs', '.toml', '.lock')):
            for gate_name in ('r04b-native-clippy', 'r05-native-tests', 'r07-workspace-clippy',
                              'r08-workspace-tests', 'r16-native-surface-clippy', 'r17-native-cli-suite'):
                need(gates[gate_name]['changed_source_inventory'].get(name) == item,
                     'compiled source differs from runtime: ' + gate_name + ':' + name)
    need(set(runtime['files']) == {'shardloom', '_native.abi3.so'}, 'runtime product coverage changed')
    for item in frozen['runtime']:
        need(runtime['files'][Path(item['path']).name] == dict(sha256=item['sha256'], bytes=item['generation']['bytes']),
             'observed runtime differs from build')
    need(all(runtime['files'][name]['sha256'] != previous['runtime_files'][name]['sha256']
             for name in runtime['files']), 'candidate reused baseline products')

    public = local('public-numeric-r1.json')
    need({key: public[key] for key in ('status', 'tests', 'failures', 'errors', 'skipped')}
         == dict(status='passed', tests=1, failures=0, errors=0, skipped=0)
         and public['posthash_verified'] is True and public['public_interfaces'] == 5
         and public['declarations_per_interface'] == 3 and public['retained_reuses'] == 2
         and public['comparison_engine_invoked'] is False,
         'five-interface proof incomplete')
    source_test = 'python/tests/test_native_shared_provider_binding.py'
    gate = gates['r12-public-numeric-surfaces']
    expected_public = {root + '/candidate-numeric-release-r1/' + name: item['sha256']
                       for name, item in runtime['files'].items()}
    expected_public[gate['cwd'] + '/' + source_test] = runtime['source_snapshot'][source_test]['sha256']
    need(public['runtime_and_test_sha256'] == expected_public, 'public proof runtime or source changed')
    need(gate['changed_source_inventory'][source_test] == runtime['source_snapshot'][source_test],
         'public test source changed')
    need('test_numeric_extrema_and_selected_outputs_match_all_five_surfaces' in
         raw(root + '/r12-public-numeric-surfaces.log').decode(), 'public proof test absent')

    heldout_inputs = local('heldout-r1/inputs.json')
    need(sha(raw(root + '/heldout-r1/inputs.json')) == HELDOUT_SHA, 'frozen held-out population changed')
    check_frozen(heldout_inputs['frozen_files'])
    bound = local('heldout-r1/runtime.json')
    check_frozen(bound['runtime'] + [bound['input_manifest'], bound['runtime_manifest'], bound['build_receipt']])
    need(raw(bound['runtime_manifest']['path']) == raw(root + '/candidate-numeric-release-r1/manifest.json')
         and raw(bound['build_receipt']['path']) == raw(root + '/' + build_name + '.json'),
         'held-out runtime binding changed')
    heldout = replay_heldout(raw, root + '/heldout-r1', protocol, frozen['runtime'])
    need(heldout == local('heldout-r1-replay.json'), 'held-out replay summary changed')
    package = local('installed-wheel-r1/report.json')
    need(package['status'] == package['uninstall_status'] == 'passed'
         and package['publication_attempted'] is False and package['installation_performed'] is True
         and package['installed_load']['isolated_install'] is True, 'installed package proof incomplete')
    need(package['integration'] == dict(tests=202, failures=0, errors=0, skipped=0, status='passed'),
         'installed package coverage changed')
    need(package['integrity']['cli_sha256'] == runtime['files']['shardloom']['sha256']
         and package['integrity']['native_library_sha256'] == runtime['files']['_native.abi3.so']['sha256'],
         'installed package used another runtime')
    need(gates['r18-installed-wheel-proof']['changed_source_inventory'][source_test]
         == runtime['source_snapshot'][source_test], 'installed package tested another source')
    for step in package['steps']:
        need(step['returncode'] == 0 and sha(raw(root + '/installed-wheel-r1/' + step['log'])) == step['log_sha256'],
             'package step failed or log changed')
    return dict(schema_version='shardloom.shared_numeric_values_acceptance.v1', status='passed',
        source_head=SOURCE_HEAD, dirty_runtime_source_recorded=True, candidate_retained=True,
        comparison=comparison, diagnostic_disposition=disposition, heldout=heldout,
        activation_tests=ACTIVATION_TESTS, test_totals=totals,
        failed_validation_attempts=sorted(FAILED), public_interface_cases=15, installed_package_tests=202,
        runtime_files=runtime['files'], wheel=package['integrity'], new_competitor_observations=0,
        full43_observations=0, item_20_complete=False, publication_attempted=False,
        limitations=['One complete candidate observation with the accepted baseline reused; no statistical confidence claim.',
            'The original material case flag remains in the complete total; its bounded diagnostic did not reproduce it, and its cause is unassigned.',
            'Fresh held-out results prove correctness; native debug assertions prove activation.',
            '32 native opt-in measurement/fixture tests remain ignored; hosted acceptance is recorded separately.',
            'Saved complete results replay without queries, archived code, PyArrow or live historical paths.',
            'Hashes establish internal consistency, not signed provenance; source inputs, runtime and wheel remain external.',
            'Broader item-20 requirements and the item-18 I/O/default/performance/documentation gate remain open.'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('packet', type=Path)
    args = parser.parse_args()
    manifest = strict_json(bounded_bytes(args.packet / 'manifest.json', MAX_FILE))
    files = object_files(manifest, bounded_bytes(args.packet / 'packet.tar.xz', MAX_ARCHIVE))
    result = verify(files, manifest)
    need(result == strict_json(bounded_bytes(args.packet / 'acceptance.json', MAX_FILE)),
         'acceptance summary differs from replay')
    print(json.dumps(dict(status='passed', files=len(files), broad_cases=347,
        heldout_cases=result['heldout']['observations'], heldout_rows=result['heldout']['complete_rows'],
        candidate_engine_seconds=result['comparison']['candidate_engine_seconds'],
        public_interface_cases=15, installed_package_tests=202)))


if __name__ == '__main__':
    main()
