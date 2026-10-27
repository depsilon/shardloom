# SPDX-License-Identifier: Apache-2.0
"""Verify the saved Parquet completion results without executing packet code."""
from pathlib import Path
import argparse
import json
import sys

REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(REPO / 'scripts'), str(REPO / 'python/src'),
               str(REPO / 'benchmarks/traditional_analytics')]
from native_source_demand_checkpoint_replay import verify_broad
from native_workflow_protocol import strict_json
from parquet_demand_completion_replay import need, replay
from query_surface.replay import bounded_bytes, object_files, sha, MAX_ARCHIVE, MAX_FILE
from query_timing_report import query_phase_timings

PROTOCOL_SHA = 'fa2791c2eb8c961d49413ae0c8f7a4e1a79656a01b3eabf2a62f8e8b3a60ee78'
BASELINE_SHA = 'cf8aba5bf7cbf8bc07a8ed9071b838369f334f0c80765e092a792252464ff68b'
CARGO_GATES = {
    'r09-native-vortex-suite': 'test --locked --offline --profile ci-native -p shardloom-vortex --features release-user-surfaces --lib -- --test-threads=2',
    'r10-native-cli-tests': 'test --locked --offline --profile ci-native -p shardloom-cli --features release-user-surfaces --lib public_parquet -- --test-threads=2',
    'r12-workspace-clippy': 'clippy --locked --offline --workspace --all-targets -- -D warnings',
    'r13-workspace-tests': 'test --locked --offline --workspace --all-targets -- --test-threads=2',
    'r14-format-check': 'fmt --all -- --check',
    'r15-all-features': 'check --locked --offline --workspace --all-features',
    'r16-no-default-features': 'check --locked --offline --workspace --no-default-features',
    'r17-reader-feature-boundary': 'check --locked --offline -p shardloom-cli --no-default-features --features vortex-local-primitives,universal-format-io',
    'r18-release-build': 'build --locked --offline --release -p shardloom-cli -p shardloom-python --features shardloom-cli/release-user-surfaces,shardloom-python/python-native',
    'r22-native-clippy-correction': 'clippy --locked --offline -p shardloom-cli -p shardloom-vortex --all-targets --features release-user-surfaces -- -D warnings',
}


def verify(files, manifest):
    need(manifest['verification_kind'] == 'parquet_demand_completion', 'wrong verification kind')
    root = manifest['original_evidence_root']

    def raw(path):
        name = manifest['path_map'].get(path)
        need(name in files, 'missing linked evidence: ' + path)
        return files[name]

    def local(name):
        return strict_json(raw(root + '/' + name))

    frozen = local('broad-r1/inputs.json')
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
    need(sha(raw(root + '/protocol.json')) == PROTOCOL_SHA, 'prospective protocol changed')

    # The previous packet contains the full independent references and native
    # outputs. Read it as bounded data, never import its archived Python files.
    baseline_manifest = strict_json(files['baseline/manifest.json'])
    baseline_archive = files['baseline/packet.tar.xz']
    need(sha(baseline_archive) == BASELINE_SHA, 'baseline archive changed')
    baseline_files = object_files(baseline_manifest, baseline_archive)
    baseline = verify_broad(baseline_files, baseline_manifest, 2)
    need(baseline == frozen['baseline_complete_result_replay'], 'baseline replay changed')
    for key in ('baseline_inputs', 'baseline_observations'):
        original = frozen[key]['path']
        need(raw(original) == baseline_files[baseline_manifest['path_map'][original]],
             'baseline summary differs from complete retained outputs')
    old_summary = strict_json(raw(frozen['baseline_observations']['path']))
    for item in old_summary['observations']:
        outcome = strict_json(baseline_files[baseline_manifest['path_map'][item['stdout']]])
        observed = outcome['observation']
        derived = query_phase_timings(observed['fields'], observed['clocks']['complete_workflow_seconds'], allow_missing=False)
        need(derived['engine_execution_seconds'] == observed['clocks']['engine_execution_seconds'],
             'baseline clock disagrees with native report')

    comparison = replay(raw, root + '/broad-r1')
    need(comparison == local('broad-r1-comparison.json'), 'comparison is not derived from complete outputs')
    need(comparison['benefit_gate_passed'] and not comparison['material_case_regressions']
         and not comparison['material_family_regressions'], 'retention gate failed')

    other_gates = ('r19-runtime-freeze', 'r20-wheel-tools', 'r21-installed-wheel',
                   'r24-broad-freeze', 'r25-broad-complete-result-replay')
    for name in (*CARGO_GATES, *other_gates):
        gate = local(name + '.json')
        need(gate['status'] == 'passed' and gate['returncode'] == 0
             and gate['process_group_drained'] is True and gate['direct_child_reaped'] is True
             and not gate['timed_out'] and gate['interrupted_signal'] is None,
             'required gate failed: ' + name)
        need(sha(raw(root + '/' + name + '.log')) == gate['log_sha256'], 'gate log changed')
        if name in CARGO_GATES:
            need(gate['command'] == ['cargo', *CARGO_GATES[name].split()], 'wrong gate command: ' + name)

    runtime = local('candidate-runtime-r1/manifest.json')
    build = local(runtime['build_receipt'])
    need(runtime['profile'] == 'release' and runtime['build_receipt'] == 'r18-release-build.json', 'wrong runtime build')
    need(sha(raw(root + '/' + runtime['build_receipt'])) == runtime['build_receipt_sha256'], 'build receipt changed')
    for name, identity in runtime['source_snapshot'].items():
        source = raw(root + '/candidate-runtime-r1/source/' + name)
        need(sha(source) == identity['sha256'] and len(source) == identity['bytes']
             and identity == build['changed_source_inventory'][name], 'runtime source differs from tested build')
    for item in frozen['runtime']:
        name = Path(item['path']).name
        need(item['sha256'] == runtime['files'][name]['sha256']
             and item['generation']['bytes'] == runtime['files'][name]['bytes'], 'benchmark runtime changed')
    package = local('installed-wheel-r1/report.json')
    need(package['status'] == package['uninstall_status'] == 'passed'
         and package['publication_attempted'] is False and package['installed_load']['isolated_install'] is True,
         'installed package proof incomplete')
    need(package['integration'] == dict(tests=185, failures=0, errors=0, skipped=0, status='passed'),
         'package integration coverage changed')
    need(package['integrity']['cli_sha256'] == runtime['files']['shardloom']['sha256']
         and package['integrity']['native_library_sha256'] == runtime['files']['_native.abi3.so']['sha256'],
         'installed package used another runtime')
    return dict(status='passed', complete_comparisons=347, complete_rows=12565,
        baseline_engine_seconds=comparison['baseline_engine_seconds'],
        candidate_engine_seconds=comparison['candidate_engine_seconds'],
        reduction_fraction=comparison['reduction_fraction'],
        benefit_gate_passed=True, material_case_regressions=[], material_family_regressions=[],
        package_tests=185, new_competitor_queries=0,
        limitations=['One observation per case per candidate; no statistical confidence or isolated causal claim.',
            'Complete retained outputs are replayed without executing queries or packet code.',
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
