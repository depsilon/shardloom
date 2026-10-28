# SPDX-License-Identifier: Apache-2.0
"""Verify shared key lookup acceptance from complete saved results and checks."""
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
from shared_key_replay import replay_heldout
from verify_rich_bounded_retention_packet import verify as verify_previous

SOURCE_HEAD = '328d3bf0a1e81a6818b3addc2e1c35d32e1777f6'
BASELINE_HEAD = '1022a366bcd2a205a241e68cbf08421fa0cb0750'
PROTOCOL_SHA = '66bc7a8ae8a3bfe11834c3ac3f58d829483834dfa7d24eeaccbf6337700756b4'
PREVIOUS_SHA = '46690d83187de1c676529f3c0dd9fea09916c36923ede7e8d2fb6ad14041e6cb'
HELDOUT_SHA = 'df677ef97ff2c751ee56efb4fa2826cc0055ddcfc2d2ee149ef4b11290c3e2b9'
FAILED = {5}
COMMANDS = {
    'r06-native-clippy': 'clippy --locked --offline -p shardloom-vortex -p shardloom-cli -p shardloom-python --features release-user-surfaces --all-targets --profile ci-native -- -D warnings',
    'r08-native-vortex-suite': 'test --locked --offline -p shardloom-vortex --features release-user-surfaces --profile ci-native --lib -- --test-threads=2',
    'r09-candidate-key-release-build': 'build --locked --offline -p shardloom-cli -p shardloom-python --features release-user-surfaces --release',
    'r16-workspace-clippy': 'clippy --locked --offline --workspace --all-targets -- -D warnings',
    'r17-workspace-tests': 'test --locked --offline --workspace --all-targets -- --test-threads=2',
    'r18-native-cli-suite': 'test --locked --offline -p shardloom-cli --features release-user-surfaces --profile ci-native --lib -- --test-threads=2',
    'r20-format-check': 'fmt --all -- --check',
}
ACTIVATION_TESTS = [
    'cached_set_rejects_late_invalid_codes_and_releases_all_credits',
    'dictionary_key_cache_keeps_duplicate_values_null_codes_and_changing_dictionaries',
    'dense_key_cache_preserves_negative_extreme_null_and_changed_batch_domains',
    'key_cache_preserves_all_integer_widths_and_encoded_numeric_inputs',
    'key_directory_reservation_declines_low_memory_and_checks_cancellation',
    'membership_cache_does_not_intern_nulls_under_join_null_policy',
    'zero_measure_grouping_keeps_first_occurrence_without_row_ordinals',
    'key_cache_declines_sparse_high_cardinality_and_tiny_domains',
    'join_build_uses_current_batch_and_keeps_cross_batch_duplicate_order',
]


def verify(files, manifest):
    need(manifest['verification_kind'] == 'shared_key_lookup', 'wrong verification kind')
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

    need(sha(raw(root + '/protocol-key-r1.json')) == PROTOCOL_SHA, 'prospective protocol changed')
    protocol = local('protocol-key-r1.json')
    prior_manifest = strict_json(files['previous/manifest.json'])
    prior_archive = raw(manifest['previous_archive_path'])
    need(sha(prior_archive) == PREVIOUS_SHA, 'previous accepted packet changed')
    prior_files = object_files(prior_manifest, prior_archive)
    previous = verify_previous(prior_files, prior_manifest)
    baseline = previous['comparisons']['candidate']
    need(baseline['candidate_engine_seconds'] == protocol['baseline_engine_seconds']
         and protocol['baseline_head'] == BASELINE_HEAD, 'accepted baseline changed')
    frozen = local('broad-key-r1/inputs.json')
    need(frozen['role'] == 'candidate', 'runtime role changed')
    check_frozen(frozen['frozen_files'])
    for name in ('baseline_inputs', 'baseline_observations'):
        path = frozen[name]['path']
        original = next((value for value in prior_manifest['path_map'] if value.casefold() == path.casefold()), None)
        need(original is not None and raw(path) == prior_files[prior_manifest['path_map'][original]],
             'previous accepted observation changed')
    comparison = replay(raw, root + '/broad-key-r1')
    need(comparison == local('broad-key-r1-comparison.json'), 'comparison differs from complete results')
    need(frozen['baseline_complete_result_replay']['verification'] == baseline,
         'candidate changed the accepted baseline')
    need(comparison['benefit_gate_passed'] is True and not comparison['material_case_regressions']
         and not comparison['material_family_regressions'], 'candidate retention gate failed')

    gates, numbers = {}, set()
    for path in manifest['path_map']:
        if not path.startswith(root + '/'):
            continue
        name = path[len(root) + 1:]
        match = re.fullmatch(r'r(\d+)-[^/]+\.json', name)
        if not match:
            continue
        number = int(match.group(1))
        need(number not in numbers and 1 <= number <= 20, 'duplicate or unexpected original gate')
        numbers.add(number)
        value = gates[name[:-5]] = local(name)
        expected_head = BASELINE_HEAD if number <= 8 else SOURCE_HEAD
        need(value['head'] == expected_head and sha(raw(root + '/' + name[:-5] + '.log')) == value['log_sha256'],
             'gate revision or log changed')
        need(value['process_group_drained'] is True and value['direct_child_reaped'] is True
             and value['timed_out'] is False and value['interrupted_signal'] is None,
             'gate process cleanup incomplete')
        if number in FAILED:
            need(value['status'] == 'failed' and value['returncode'] != 0, 'historical failure replaced')
        else:
            need(value['status'] == 'passed' and value['returncode'] == 0, 'required gate failed')
    need(numbers == set(range(1, 21)), 'original build/test history incomplete')
    for name, command in COMMANDS.items():
        need(gates[name]['command'] == ['cargo', *command.split()], 'wrong gate command: ' + name)
    totals = {}
    for name, expected in (('r08-native-vortex-suite', (3442, 0, 32)),
                           ('r17-workspace-tests', (3274, 0, 0)), ('r18-native-cli-suite', (1032, 0, 0))):
        rows = re.findall(r'test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;',
                          raw(root + '/' + name + '.log').decode())
        count = tuple(sum(int(row[index]) for row in rows) for index in (1, 2, 3))
        need(rows and all(row[0] == 'ok' for row in rows) and count == expected, 'test coverage changed')
        totals[name] = dict(zip(('passed', 'failed', 'ignored'), count))
    test_log = raw(root + '/r08-native-vortex-suite.log').decode()
    need(all(name + ' ... ok' in test_log for name in ACTIVATION_TESTS), 'key activation test absent')

    runtime = local('candidate-key-release-r1/manifest.json')
    build_name = 'r09-candidate-key-release-build'
    build = gates[build_name]
    need(runtime['profile'] == 'release' and runtime['build_receipt'] == build_name + '.json'
         and runtime['build_receipt_sha256'] == sha(raw(root + '/' + build_name + '.json'))
         and runtime['changed_source_inventory'] == build['changed_source_inventory'], 'runtime build changed')
    expected_sources = {name for name, item in build['changed_source_inventory'].items()
                        if 'sha256' in item and not name.startswith(('docs/', 'website'))}
    need(set(runtime['source_snapshot']) == expected_sources, 'runtime source snapshot coverage changed')
    for name, item in runtime['source_snapshot'].items():
        source = raw(root + '/candidate-key-release-r1/source/' + name)
        need(sha(source) == item['sha256'] and len(source) == item['bytes']
             and item == build['changed_source_inventory'][name], 'runtime source differs from build')
        if name.endswith(('.rs', '.toml', '.lock')):
            for gate_name in ('r06-native-clippy', 'r08-native-vortex-suite', 'r16-workspace-clippy',
                              'r17-workspace-tests', 'r18-native-cli-suite'):
                need(gates[gate_name]['changed_source_inventory'].get(name) == item,
                     'compiled source differs from runtime: ' + gate_name + ':' + name)
    need(set(runtime['files']) == {'shardloom', '_native.abi3.so'}, 'runtime product coverage changed')
    for item in frozen['runtime']:
        need(runtime['files'][Path(item['path']).name] == dict(sha256=item['sha256'], bytes=item['generation']['bytes']),
             'observed runtime differs from build')
    need(all(runtime['files'][name]['sha256'] != previous['runtime_files'][name]['sha256']
             for name in runtime['files']), 'candidate reused baseline products')

    public = local('public-keys-r1.json')
    need({key: public[key] for key in ('status', 'tests', 'failures', 'errors', 'skipped')}
         == dict(status='passed', tests=1, failures=0, errors=0, skipped=0)
         and public['posthash_verified'] is True and public['public_interfaces'] == 5
         and public['declarations_per_interface'] == 5 and public['comparison_engine_invoked'] is False,
         'five-interface proof incomplete')
    source_test = 'python/tests/test_native_shared_provider_binding.py'
    gate = gates['r12-public-key-surfaces']
    expected_public = {root + '/candidate-key-release-r1/' + name: item['sha256']
                       for name, item in runtime['files'].items()}
    expected_public[gate['cwd'] + '/' + source_test] = runtime['source_snapshot'][source_test]['sha256']
    need(public['runtime_and_test_sha256'] == expected_public, 'public proof runtime or source changed')
    need(gate['changed_source_inventory'][source_test] == runtime['source_snapshot'][source_test],
         'public test source changed')
    need('test_shared_key_domains_preserve_grouping_distinct_and_join_on_all_five_surfaces' in
         raw(root + '/r12-public-key-surfaces.log').decode(), 'public proof test absent')

    heldout_inputs = local('heldout-r1/inputs.json')
    need(sha(raw(root + '/heldout-r1/inputs.json')) == HELDOUT_SHA, 'frozen held-out population changed')
    check_frozen(heldout_inputs['frozen_files'])
    bound = local('heldout-r1/runtime.json')
    check_frozen(bound['runtime'] + [bound['input_manifest'], bound['runtime_manifest'], bound['build_receipt']])
    need(raw(bound['runtime_manifest']['path']) == raw(root + '/candidate-key-release-r1/manifest.json')
         and raw(bound['build_receipt']['path']) == raw(root + '/' + build_name + '.json'),
         'held-out runtime binding changed')
    heldout = replay_heldout(raw, root + '/heldout-r1', protocol, frozen['runtime'])
    need(heldout == local('heldout-r1-replay.json'), 'held-out replay summary changed')
    package = local('installed-wheel-r1/report.json')
    need(package['status'] == package['uninstall_status'] == 'passed'
         and package['publication_attempted'] is False and package['installation_performed'] is True
         and package['installed_load']['isolated_install'] is True, 'installed package proof incomplete')
    need(package['integration'] == dict(tests=201, failures=0, errors=0, skipped=0, status='passed'),
         'installed package coverage changed')
    need(package['integrity']['cli_sha256'] == runtime['files']['shardloom']['sha256']
         and package['integrity']['native_library_sha256'] == runtime['files']['_native.abi3.so']['sha256'],
         'installed package used another runtime')
    need(gates['r19-installed-wheel-proof']['changed_source_inventory'][source_test]
         == runtime['source_snapshot'][source_test], 'installed package tested another source')
    for step in package['steps']:
        need(step['returncode'] == 0 and sha(raw(root + '/installed-wheel-r1/' + step['log'])) == step['log_sha256'],
             'package step failed or log changed')
    return dict(schema_version='shardloom.shared_key_lookup_acceptance.v1', status='passed',
        source_head=SOURCE_HEAD, dirty_runtime_source_recorded=True, candidate_retained=True,
        comparison=comparison, heldout=heldout, activation_tests=ACTIVATION_TESTS, test_totals=totals,
        failed_validation_attempts=sorted(FAILED), public_interface_cases=25, installed_package_tests=201,
        runtime_files=runtime['files'], wheel=package['integrity'], new_competitor_observations=0,
        full43_observations=0, item_20_complete=False, publication_attempted=False,
        limitations=['One complete candidate observation with the accepted baseline reused; no statistical confidence claim.',
            'Fresh held-out results prove correctness; native debug assertions prove activation.',
            '32 native opt-in measurement/fixture tests remain ignored; hosted acceptance is recorded separately.',
            'Saved complete results replay without queries, archived code, PyArrow or live historical paths.',
            'Hashes establish internal consistency, not signed provenance; source inputs, runtime and wheel remain external.',
            'Typed extrema, independently owned numeric gathering and broader item-20 work remain open.'])


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
        public_interface_cases=25, installed_package_tests=201)))


if __name__ == '__main__':
    main()
