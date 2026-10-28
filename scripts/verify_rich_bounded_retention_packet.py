# SPDX-License-Identifier: Apache-2.0
"""Verify rich ordering acceptance using complete saved results and source checks."""
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
from rich_ordering_replay import replay_heldout
from verify_shared_analytical_performance_packet import verify as verify_previous

SOURCE_HEAD = '51cf54fdb1af561c18f5ea498d3e5309a38c29d7'
PROTOCOL_SHA = '61c4f459f771540deb55cee2ba59d5fe41880e18738246260835d9b4f22b5e8d'
PREVIOUS_SHA = '5564d2680d322a5e68a4dc2c2837212b82615bb25aec2f739f22906a124493da'
HELDOUT_SHA = 'cf0ed4c90711b88cdae0d172cd78943f8fad270558cb8cbc2252f1ee2cf735e4'
FAILED = {4, 6, 7, 9, 18, 27, 28}
RELEASE = 'build --locked --offline -p shardloom-cli -p shardloom-python --features release-user-surfaces --release'
COMMANDS = {
    'r01-release-baseline-build': RELEASE, 'r26-candidate-release-build': RELEASE,
    'r31-confirm-candidate-release': RELEASE,
    'r20-native-clippy': 'clippy --locked --offline -p shardloom-vortex -p shardloom-cli -p shardloom-python --features release-user-surfaces --all-targets --profile ci-native -- -D warnings',
    'r21-native-vortex-suite': 'test --locked --offline -p shardloom-vortex --features release-user-surfaces --profile ci-native --lib -- --test-threads=2',
    'r23-workspace-clippy': 'clippy --locked --offline --workspace --all-targets -- -D warnings',
    'r24-workspace-tests': 'test --locked --offline --workspace --all-targets -- --test-threads=2',
    'r25-final-format-check': 'fmt --all -- --check',
    'r30-native-cli-suite': 'test --locked --offline -p shardloom-cli --features release-user-surfaces --profile ci-native --lib -- --test-threads=2',
}
OLD_COMMENT = '    /// Excludes keys, metadata, provider scratch, spill internals and delivery.\n'
NEW_COMMENT = ('    /// Includes key columns when copied as payload. Excludes separately built\n'
               '    /// keys, metadata, provider scratch, spill internals and delivery.\n')
COMMENT_FILE = 'shardloom-vortex/src/local_primitive_relational_batch_input.rs'


def verify(files, manifest):
    need(manifest['verification_kind'] == 'rich_bounded_retention', 'wrong verification kind')
    root = manifest['original_evidence_root']

    def raw(path):
        name = manifest['path_map'].get(path)
        need(name in files, 'missing linked evidence: ' + path)
        return files[name]

    def local(name):
        return strict_json(raw(root + '/' + name))

    external = {(item['path'], item['sha256'], item['bytes']) for item in manifest['external_artifacts']}

    def check_frozen(items):
        for item in items:
            size = item['generation']['bytes']
            if item['path'] in manifest['path_map']:
                data = raw(item['path'])
                need(sha(data) == item['sha256'] and len(data) == size,
                     'frozen source or evidence changed: ' + item['path'])
            else:
                need((item['path'], item['sha256'], size) in external,
                     'unrepresented frozen payload: ' + item['path'])

    need(sha(raw(root + '/protocol-r1.json')) == PROTOCOL_SHA, 'prospective protocol changed')
    protocol = local('protocol-r1.json')
    prior_manifest = strict_json(files['previous/manifest.json'])
    prior_archive = files['previous/packet.tar.xz']
    need(sha(prior_archive) == PREVIOUS_SHA, 'previous accepted packet changed')
    prior_files = object_files(prior_manifest, prior_archive)
    previous = verify_previous(prior_files, prior_manifest)
    need(previous['engine_seconds']['broad-ordering-r1'] == protocol['previous_engine_seconds'],
         'previous accepted engine total changed')
    frozen = {role: local(f'broad-{role}-r1/inputs.json') for role in ('baseline', 'candidate')}
    for role, value in frozen.items():
        need(value['role'] == role, 'runtime role changed')
        check_frozen(value['frozen_files'])
    for name in ('baseline_inputs', 'baseline_observations'):
        path = frozen['baseline'][name]['path']
        need(raw(path) == prior_files[prior_manifest['path_map'][path]], 'previous accepted observation changed')
    comparisons = {role: replay(raw, root + f'/broad-{role}-r1') for role in frozen}
    for role, value in comparisons.items():
        need(value == local(f'broad-{role}-r1-comparison.json'), 'comparison differs from complete results')
    baseline, candidate = comparisons['baseline'], comparisons['candidate']
    need(frozen['candidate']['baseline_complete_result_replay']['verification'] == baseline,
         'candidate changed the fresh temporal baseline')
    need(baseline['material_case_regressions'] == ['temporal_timestamp_hour', 'temporal_timestamp_second']
         and not baseline['material_family_regressions'], 'historical temporal flags changed')
    need(candidate['benefit_gate_passed'] is True and not candidate['material_case_regressions']
         and not candidate['material_family_regressions'], 'candidate retention gate failed')

    gates, numbers = {}, set()
    for path in manifest['path_map']:
        if not path.startswith(root + '/'):
            continue
        name = path[len(root) + 1:]
        match = re.fullmatch(r'r(\d+)-[^/]+\.json', name)
        if not match:
            continue
        number = int(match.group(1))
        need(number not in numbers and 1 <= number <= 36, 'duplicate or unexpected original gate')
        numbers.add(number)
        value = gates[name[:-5]] = local(name)
        need(value['head'] == SOURCE_HEAD and sha(raw(root + '/' + name[:-5] + '.log')) == value['log_sha256'],
             'gate revision or log changed')
        need(value['process_group_drained'] is True and value['direct_child_reaped'] is True
             and value['timed_out'] is False and value['interrupted_signal'] is None,
             'gate process cleanup incomplete')
        if number in FAILED:
            need(value['status'] == 'failed' and value['returncode'] != 0, 'historical failure replaced')
        else:
            need(value['status'] == 'passed' and value['returncode'] == 0, 'required gate failed')
    need(numbers == set(range(1, 37)), 'original build/test history incomplete')
    for name, command in COMMANDS.items():
        need(gates[name]['command'] == ['cargo', *command.split()], 'wrong gate command: ' + name)
    totals = {}
    for name, expected in (('r21-native-vortex-suite', (3433, 0, 32)),
                           ('r24-workspace-tests', (3274, 0, 0)), ('r30-native-cli-suite', (1032, 0, 0))):
        rows = re.findall(r'test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;',
                          raw(root + '/' + name + '.log').decode())
        count = tuple(sum(int(row[index]) for row in rows) for index in (1, 2, 3))
        need(rows and all(row[0] == 'ok' for row in rows) and count == expected, 'test coverage changed')
        totals[name] = dict(zip(('passed', 'failed', 'ignored'), count))

    runtimes = {}
    for role, build_name in (('baseline', 'r01-release-baseline-build'), ('candidate', 'r31-confirm-candidate-release')):
        runtime = runtimes[role] = local(f'{role}-release-r1/manifest.json')
        build = gates[build_name]
        need(runtime['profile'] == 'release' and runtime['build_receipt'] == build_name + '.json'
             and runtime['build_receipt_sha256'] == sha(raw(root + '/' + build_name + '.json'))
             and runtime['changed_source_inventory'] == build['changed_source_inventory'], 'runtime build changed')
        expected_sources = {name for name, item in build['changed_source_inventory'].items()
                            if 'sha256' in item and not name.startswith(('docs/', 'website'))}
        need(set(runtime['source_snapshot']) == expected_sources, 'runtime source snapshot coverage changed')
        if role == 'baseline':
            need(not any(name.endswith(('.rs', '.toml', '.lock')) for name in build['changed_source_inventory']),
                 'baseline runtime was modified')
        for name, item in runtime['source_snapshot'].items():
            source = raw(root + f'/{role}-release-r1/source/' + name)
            need(sha(source) == item['sha256'] and len(source) == item['bytes']
                 and item == build['changed_source_inventory'][name], 'runtime source differs from build')
            if role == 'candidate' and name.endswith(('.rs', '.toml', '.lock')):
                for gate_name in ('r20-native-clippy', 'r21-native-vortex-suite', 'r23-workspace-clippy',
                                  'r24-workspace-tests', 'r30-native-cli-suite'):
                    expected = gates[gate_name]['changed_source_inventory'].get(name)
                    if name == COMMENT_FILE and gate_name in ('r20-native-clippy', 'r21-native-vortex-suite'):
                        text = source.decode()
                        need(text.count(NEW_COMMENT) == 1, 'comment-only source boundary changed')
                        original = text.replace(NEW_COMMENT, OLD_COMMENT).encode()
                        need(expected == dict(sha256=sha(original), bytes=len(original)),
                             'compiled source differs beyond documented comment')
                    else:
                        need(expected == item, 'compiled source differs from runtime: ' + gate_name + ':' + name)
        need(set(runtime['files']) == {'shardloom', '_native.abi3.so'}, 'runtime product coverage changed')
        for item in frozen[role]['runtime']:
            need(runtime['files'][Path(item['path']).name] == dict(sha256=item['sha256'], bytes=item['generation']['bytes']),
                 'observed runtime differs from build')
    need(all(runtimes['baseline']['files'][name]['sha256'] != runtimes['candidate']['files'][name]['sha256']
             for name in runtimes['candidate']['files']), 'candidate reused baseline products')

    public = local('public-prefix-r3.json')
    need({key: public[key] for key in ('status', 'tests', 'failures', 'errors', 'skipped')}
         == dict(status='passed', tests=1, failures=0, errors=0, skipped=0)
         and public['posthash_verified'] is True and public['public_interfaces'] == 5
         and public['declarations_per_interface'] == 6 and public['comparison_engine_invoked'] is False,
         'five-interface proof incomplete')
    source_test = 'python/tests/test_native_shared_provider_binding.py'
    gate = gates['r29-public-rich-prefix']
    build_products = gate['environment']['CARGO_TARGET_DIR'] + '/release/'
    native_sha = runtimes['candidate']['files']['_native.abi3.so']['sha256']
    expected_public = {
        build_products + 'lib_native.dylib': native_sha,
        build_products + 'shardloom': runtimes['candidate']['files']['shardloom']['sha256'],
        root + '/public-prefix-staging-r2/_native.abi3.so': native_sha,
        gate['cwd'] + '/' + source_test: runtimes['candidate']['source_snapshot'][source_test]['sha256'],
    }
    need(public['runtime_and_test_sha256'] == expected_public, 'public proof runtime or source changed')
    need(gate['changed_source_inventory'][source_test] == runtimes['candidate']['source_snapshot'][source_test],
         'public test differs from the tested runtime source snapshot')
    need('test_rich_bounded_ordering_preserves_values_and_schema_on_all_five_surfaces' in
         raw(root + '/r29-public-rich-prefix.log').decode(), 'public proof test absent')
    need(local('public-prefix-r1.json')['status'] == local('public-prefix-r2.json')['status'] == 'failed',
         'public setup failures replaced')

    heldout_inputs = local('heldout-r1/inputs.json')
    need(sha(raw(root + '/heldout-r1/inputs.json')) == HELDOUT_SHA, 'frozen held-out population changed')
    check_frozen(heldout_inputs['frozen_files'])
    bound = local('heldout-r1/runtime.json')
    check_frozen(bound['runtime'] + [bound['input_manifest'], bound['runtime_manifest'], bound['build_receipt']])
    need(raw(bound['runtime_manifest']['path']) == raw(root + '/candidate-release-r1/manifest.json')
         and raw(bound['build_receipt']['path']) == raw(root + '/r31-confirm-candidate-release.json'),
         'held-out runtime binding changed')
    heldout = replay_heldout(raw, root + '/heldout-r1', protocol, frozen['candidate']['runtime'])
    package = local('installed-wheel-r1/report.json')
    need(package['status'] == package['uninstall_status'] == 'passed'
         and package['publication_attempted'] is False and package['installation_performed'] is True
         and package['installed_load']['isolated_install'] is True, 'installed package proof incomplete')
    need(package['integration'] == dict(tests=200, failures=0, errors=0, skipped=0, status='passed'),
         'installed package coverage changed')
    need(package['integrity']['cli_sha256'] == runtimes['candidate']['files']['shardloom']['sha256']
         and package['integrity']['native_library_sha256'] == runtimes['candidate']['files']['_native.abi3.so']['sha256'],
         'installed package used another runtime')
    for step in package['steps']:
        need(step['returncode'] == 0 and sha(raw(root + '/installed-wheel-r1/' + step['log'])) == step['log_sha256'],
             'package step failed or log changed')

    activity = []
    observations = {role: local(f'broad-{role}-r1/native-observations.json') for role in frozen}
    for case_id in ('projection_rich', 'projection_wide'):
        result = {'case_id': case_id}
        for role in frozen:
            item = next(row for row in observations[role]['observations'] if row['case_id'] == case_id)
            fields = strict_json(raw(item['stdout']))['observation']['fields']
            result[role + '_rows_copied'] = int(fields['native_input_ordering_rows_detached'])
            if role == 'candidate':
                result['candidate_buffer_bytes_copied'] = int(fields['native_input_ordering_payload_buffer_bytes_copied'])
                need(int(fields['native_input_ordering_prefix_rows_validated']) == 1_000_000,
                     'rich prefix incomplete payload validation')
        need(result['baseline_rows_copied'] == 1_000_000 and result['candidate_rows_copied'] == 15159,
             'rich prefix did not remove population copying')
        activity.append(result)
    return dict(schema_version='shardloom.rich_bounded_retention_acceptance.v1', status='passed',
        source_head=SOURCE_HEAD, dirty_runtime_source_recorded=True, candidate_retained=True,
        comparisons=comparisons, heldout=heldout, activation=activity, test_totals=totals,
        comment_only_source_change=COMMENT_FILE, failed_validation_attempts=sorted(FAILED),
        public_interface_cases=30, installed_package_tests=200, runtime_files=runtimes['candidate']['files'],
        wheel=package['integrity'], new_competitor_observations=0, full43_observations=0,
        item_20_complete=False, publication_attempted=False,
        limitations=['One complete observation per role; no statistical confidence or isolated causal attribution.',
            'The fresh temporal baseline retains its two flags against the earlier checkpoint; this candidate compares against that fresh baseline.',
            'Held-out observations prove correctness and activation, not a second benefit cohort.',
            '32 native opt-in measurement/fixture tests remain ignored; hosted platform checks are recorded separately.',
            'Complete saved results replay without queries, archived code, PyArrow or live historical paths.',
            'Content hashes establish internal consistency, not signed provenance; large inputs, runtime and wheel stay external.',
            'Broader item-20 analytical and performance work remains open.'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('packet', type=Path)
    args = parser.parse_args()
    manifest = strict_json(bounded_bytes(args.packet / 'manifest.json', MAX_FILE))
    files = object_files(manifest, bounded_bytes(args.packet / 'packet.tar.xz', MAX_ARCHIVE))
    result = verify(files, manifest)
    need(result == strict_json(bounded_bytes(args.packet / 'acceptance.json', MAX_FILE)),
         'acceptance summary differs from replay')
    print(json.dumps(dict(status='passed', files=len(files), broad_cases_per_role=347,
        heldout_cases=result['heldout']['observations'], heldout_rows=result['heldout']['complete_rows'],
        candidate_engine_seconds=result['comparisons']['candidate']['candidate_engine_seconds'],
        public_interface_cases=30, installed_package_tests=200)))


if __name__ == '__main__':
    main()
