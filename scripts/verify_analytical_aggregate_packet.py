# SPDX-License-Identifier: Apache-2.0
"""Replay saved analytical aggregate acceptance without executing packet code."""
from pathlib import Path
import argparse
import json
import sys

REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(REPO / 'scripts'), str(REPO / 'python/src'),
               str(REPO / 'benchmarks/traditional_analytics')]
from native_workflow_protocol import strict_json
from parquet_demand_completion_replay import need, replay
from query_surface.replay import bounded_bytes, object_files, sha, MAX_ARCHIVE, MAX_FILE
from verify_parquet_demand_completion_packet import verify as verify_baseline

PROTOCOL_SHA = '425b37eaf2d2ef8b6d9ca03e8f0e4e2f6dff1aa62f771b66c6c3afb4ee463d86'
BASELINE_SHA = 'e327f54ed9af5dde9c3f5a8c89a48405092f7133bcd5c9a86f7a1d06bd12c77e'
CARGO_GATES = {
    'r131-grouping-native-clippy': 'clippy --locked --offline -p shardloom-vortex -p shardloom-cli -p shardloom-python --features shardloom-python/python-native --all-targets --profile ci-native -- -D warnings',
    'r133-analytical-release-build': 'build --locked --offline --release -p shardloom-cli -p shardloom-python --features shardloom-cli/release-user-surfaces,shardloom-python/python-native',
    'r138-native-vortex-full-suite': 'test --locked --offline --profile ci-native -p shardloom-vortex --features release-user-surfaces --lib -- --test-threads=2',
    'r139-native-cli-suite': 'test --locked --offline --profile ci-native -p shardloom-cli --features release-user-surfaces --lib -- --test-threads=2',
    'r140-workspace-clippy': 'clippy --locked --offline --workspace --all-targets -- -D warnings',
    'r141-workspace-tests': 'test --locked --offline --workspace --all-targets -- --test-threads=2',
    'r142-format-check': 'fmt --all -- --check',
    'r143-all-features': 'check --locked --offline --workspace --all-features',
    'r144-no-default-features': 'check --locked --offline --workspace --no-default-features',
    'r145-reader-feature-boundary': 'check --locked --offline -p shardloom-cli --no-default-features --features vortex-local-primitives,universal-format-io',
}
PUBLIC_TESTS = (
    'statistical_reductions_match_exact_reference_on_all_five_surfaces',
    'statistical_windows_match_exact_frames_on_all_five_surfaces',
    'distinct_additive_grouped_and_windows_match_all_five_surfaces',
    'statistical_scaled_results_match_all_five_surfaces',
    'exact_quantiles_match_all_five_surfaces', 'ordered_selectors_match_all_five_surfaces',
    'ordered_collections_match_all_five_surfaces', 'aggregate_filters_match_all_five_surfaces',
    'grouping_sets_match_all_five_surfaces',
)


def verify(files, manifest):
    need(manifest['verification_kind'] == 'analytical_aggregates', 'wrong verification kind')
    root = manifest['original_evidence_root']

    def raw(path):
        name = manifest['path_map'].get(path)
        need(name in files, 'missing linked evidence: ' + path)
        return files[name]

    def local(name):
        return strict_json(raw(root + '/' + name))

    frozen = local('broad-analytical-r1/inputs.json')
    external = {(item['path'], item['sha256'], item['bytes'])
                for item in manifest['external_artifacts']}
    for item in frozen['frozen_files']:
        if item['path'] in manifest['path_map']:
            content = raw(item['path'])
            need(sha(content) == item['sha256'] and len(content) == item['generation']['bytes'],
                 'frozen source or evidence changed: ' + item['path'])
        else:
            need((item['path'], item['sha256'], item['generation']['bytes']) in external,
                 'unrepresented frozen payload: ' + item['path'])
    need(sha(raw(root + '/broad-protocol-r1.json')) == PROTOCOL_SHA, 'prospective protocol changed')

    baseline_manifest = strict_json(files['baseline/manifest.json'])
    baseline_archive = files['baseline/packet.tar.xz']
    need(sha(baseline_archive) == BASELINE_SHA, 'baseline archive changed')
    baseline_files = object_files(baseline_manifest, baseline_archive)
    baseline = verify_baseline(baseline_files, baseline_manifest)
    need(baseline == frozen['baseline_complete_result_replay']['verification'], 'baseline replay changed')
    for key in ('baseline_inputs', 'baseline_observations'):
        original = frozen[key]['path']
        need(raw(original) == baseline_files[baseline_manifest['path_map'][original]],
             'baseline summary differs from complete retained outputs')

    comparison = replay(raw, root + '/broad-analytical-r1')
    need(comparison == local('broad-analytical-r1-comparison.json'),
         'comparison is not derived from complete outputs')
    need(not comparison['material_case_regressions'] and not comparison['material_family_regressions'],
         'unresolved material regression')

    other_gates = ('r134-analytical-release-freeze', 'r135-grouping-release-public-surfaces',
                   'r136-broad-analytical-freeze', 'r137-broad-analytical-complete-replay',
                   'r146-wheel-tools', 'r147-installed-wheel', 'r148-python-source-suite',
                   'r149-aggregate-documentation-checks')
    gates = {}
    for name in (*CARGO_GATES, *other_gates):
        gate = gates[name] = local(name + '.json')
        need(gate['status'] == 'passed' and gate['returncode'] == 0
             and gate['process_group_drained'] is True and gate['direct_child_reaped'] is True
             and not gate['timed_out'] and gate['interrupted_signal'] is None,
             'required gate failed: ' + name)
        need(sha(raw(root + '/' + name + '.log')) == gate['log_sha256'], 'gate log changed')
        if name in CARGO_GATES:
            need(gate['command'] == ['cargo', *CARGO_GATES[name].split()], 'wrong gate command: ' + name)

    runtime = local('analytical-release-runtime-r1/manifest.json')
    build = gates['r133-analytical-release-build']
    need(runtime['profile'] == 'release' and runtime['build_receipt'] == 'r133-analytical-release-build.json',
         'wrong runtime build')
    need(sha(raw(root + '/' + runtime['build_receipt'])) == runtime['build_receipt_sha256'],
         'build receipt changed')
    for name, identity in runtime['source_snapshot'].items():
        source = raw(root + '/analytical-release-runtime-r1/source/' + name)
        need(sha(source) == identity['sha256'] and len(source) == identity['bytes']
             and identity == build['changed_source_inventory'][name], 'runtime source differs from build')
        if name.endswith(('.rs', '.toml', '.lock')):
            for gate_name in ('r131-grouping-native-clippy', 'r138-native-vortex-full-suite',
                              'r139-native-cli-suite', 'r140-workspace-clippy', 'r141-workspace-tests',
                              'r143-all-features', 'r144-no-default-features', 'r145-reader-feature-boundary'):
                need(gates[gate_name]['changed_source_inventory'].get(name) == identity,
                     'compiled source differs from runtime: ' + gate_name + ':' + name)
    for item in frozen['runtime']:
        name = Path(item['path']).name
        need(item['sha256'] == runtime['files'][name]['sha256']
             and item['generation']['bytes'] == runtime['files'][name]['bytes'], 'benchmark runtime changed')

    public = local('analytical-surfaces-r11.json')
    need({key: public[key] for key in ('status', 'tests', 'failures', 'errors', 'skipped')}
         == dict(status='passed', tests=9, failures=0, errors=0, skipped=0), 'public matrix incomplete')
    names = ['test_native_shared_provider_binding.NativeSharedProviderBindingTests.test_'
             + name for name in PUBLIC_TESTS]
    need(public['test_names'] == names, 'public matrix coverage changed')
    need(public['runtime_manifest_sha256'] == sha(raw(root + '/analytical-release-runtime-r1/manifest.json')),
         'public matrix runtime manifest changed')
    need(public['products'] == {root + '/analytical-release-runtime-r1/' + name: item['sha256']
                               for name, item in runtime['files'].items()}, 'public matrix used another runtime')

    package = local('installed-wheel-r1/report.json')
    need(package['status'] == package['uninstall_status'] == 'passed'
         and package['publication_attempted'] is False and package['installed_load']['isolated_install'] is True,
         'installed package proof incomplete')
    need(package['integration'] == dict(tests=194, failures=0, errors=0, skipped=0, status='passed'),
         'package integration coverage changed')
    need(package['integrity']['cli_sha256'] == runtime['files']['shardloom']['sha256']
         and package['integrity']['native_library_sha256'] == runtime['files']['_native.abi3.so']['sha256'],
         'installed package used another runtime')
    for step in package['steps']:
        need(step['returncode'] == 0 and sha(raw(root + '/installed-wheel-r1/' + step['log'])) == step['log_sha256'],
             'package step failed or log changed')
    return dict(status='passed', complete_comparisons=347, complete_rows=12565,
        baseline_engine_seconds=comparison['baseline_engine_seconds'],
        candidate_engine_seconds=comparison['candidate_engine_seconds'],
        reduction_fraction=comparison['reduction_fraction'],
        benefit_gate_passed=comparison['benefit_gate_passed'],
        material_case_regressions=[], material_family_regressions=[],
        public_feature_suites=9, package_tests=194, new_competitor_queries=0,
        item_20_complete=False,
        limitations=['One observation per case per candidate; no statistical confidence or isolated causal claim.',
            'The 347-case corpus protects established behavior; new features have separate acceptance tests.',
            'Complete retained outputs replay without executing queries or archived code.',
            'Content hashes establish internal consistency, not signed provenance.',
            'Large data and binaries are represented by identity; this packet does not reconstruct them.'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('packet', type=Path)
    args = parser.parse_args()
    manifest = strict_json(bounded_bytes(args.packet / 'manifest.json', MAX_FILE))
    files = object_files(manifest, bounded_bytes(args.packet / 'packet.tar.xz', MAX_ARCHIVE))
    print(json.dumps(verify(files, manifest), indent=2))


if __name__ == '__main__':
    main()
