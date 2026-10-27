# SPDX-License-Identifier: Apache-2.0
"""Replay retained/dropped analytical optimizations without executing packet code."""
from pathlib import Path
import argparse
import json
import math
import re
import statistics
import sys

REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(REPO / 'scripts'), str(REPO / 'python/src'),
               str(REPO / 'benchmarks/traditional_analytics')]
from native_workflow_protocol import strict_json
from parquet_demand_completion_replay import need, replay
from query_surface.evidence import no_fallback, worker_outcome
from query_surface.replay import bounded_bytes, contract, object_files, sha, MAX_ARCHIVE, MAX_FILE
from query_surface.semantics import compare, result_from_record
from query_timing_report import query_phase_timings
from verify_analytical_aggregate_packet import PUBLIC_TESTS, verify as verify_baseline

BASELINE_SHA = 'c16f885c7bbaba82e945c7d4dbe077bb9af11efc7e49ada9ccccf4a0b1c58e02'
PROTOCOLS = {
    'broad-protocol-r1.json': 'b5341483fa3aabf7abcad3e6eacaa15a8864089749fbe0446b574ab2b1c06d82',
    'broad-variable-protocol-r1.json': '69eea176304ab2afc7bcaf39c959f734afd232fe64b91dc310578a32c88d95dd',
    'broad-ordering-protocol-r1.json': 'e9500023de5b5f6010310634dbadc515baa8ca9f766f5bd74f07f5acdd8b7d84',
    'projection-diagnostic-r1/protocol.json': 'e8928e97528e2ecf0402c24dda0d759c83606c3a085917aabd1d9098209e03eb',
    'ordering-aggregate-diagnostic-r1/protocol.json': 'f5e7998df5e24dc57eabc2e6507451bc5ab612517df1d3419f65e49b97c4ab50',
}
NATIVE_CLIPPY = 'clippy --locked --offline -p shardloom-vortex -p shardloom-cli -p shardloom-python --features shardloom-python/python-native --all-targets --profile ci-native -- -D warnings'
NATIVE_TESTS = 'test --locked --offline -p shardloom-vortex --features release-user-surfaces --profile ci-native --lib -- --test-threads=2'
RELEASE_BUILD = 'build --locked --offline --release -p shardloom-cli -p shardloom-python --features shardloom-cli/release-user-surfaces,shardloom-python/python-native'
WORKSPACE_CLIPPY = 'clippy --locked --offline --workspace --all-targets -- -D warnings'
WORKSPACE_TESTS = 'test --locked --offline --workspace --all-targets -- --test-threads=2'
CARGO_GATES = {
    'r05-native-clippy': NATIVE_CLIPPY,
    'r06-native-vortex-tests': NATIVE_TESTS,
    'r07-aggregate-release-build': RELEASE_BUILD,
    'r08-workspace-clippy': WORKSPACE_CLIPPY,
    'r09-workspace-tests': WORKSPACE_TESTS,
    'r12-native-clippy': NATIVE_CLIPPY,
    'r13-native-vortex-tests': NATIVE_TESTS,
    'r14-workspace-clippy': WORKSPACE_CLIPPY,
    'r15-workspace-tests': 'test --locked --offline --workspace --all-targets',
    'r16-variable-release-build': RELEASE_BUILD,
    'r20-ordering-native-clippy': NATIVE_CLIPPY,
    'r21-ordering-native-tests': NATIVE_TESTS,
    'r22-ordering-workspace-clippy': WORKSPACE_CLIPPY,
    'r23-ordering-workspace-tests': WORKSPACE_TESTS,
    'r24-ordering-format': 'fmt --all -- --check',
    'r25-ordering-release-build': RELEASE_BUILD,
    'r31-ordering-reader-feature': 'check --locked --offline -p shardloom-cli --no-default-features --features vortex-local-primitives,universal-format-io',
    'r35-ordering-native-cli-tests': 'test --locked --offline --profile ci-native -p shardloom-cli --features release-user-surfaces --lib -- --test-threads=2',
}
RUNTIMES = (
    ('aggregate-release-runtime-r1', 'r07-aggregate-release-build',
     ('r05-native-clippy', 'r06-native-vortex-tests', 'r08-workspace-clippy', 'r09-workspace-tests')),
    ('variable-release-runtime-r1', 'r16-variable-release-build',
     ('r12-native-clippy', 'r13-native-vortex-tests', 'r14-workspace-clippy', 'r15-workspace-tests')),
    ('ordering-release-runtime-r1', 'r25-ordering-release-build',
     ('r20-ordering-native-clippy', 'r21-ordering-native-tests', 'r22-ordering-workspace-clippy',
      'r23-ordering-workspace-tests', 'r31-ordering-reader-feature', 'r35-ordering-native-cli-tests')),
)
COHORTS = ('broad-aggregate-r1', 'broad-variable-r1', 'broad-ordering-r1')
ORDER_CASES = [
    'order-dataframe', 'order-nested', 'order-limit-before', 'order-limit-after',
    'order-limit-zero', 'limit-zero-before-order', 'global-range', 'order-filter-project',
    'order-sql-range-16-23', 'order-sql-range-256-3', 'order-sql-range-300-1',
    'order-sql-range-0-0', 'order-empty', 'order-empty-batches', 'order-all-null',
    'order-filtered', 'order-write',
] + [f'range-late-{kind}-{count}-sort-{sort}' for kind in ('producer', 'schema', 'expression')
     for count in (0, 1) for sort in (False, True)]


def diagnostic(raw, path, candidate, baseline):
    """Recompute every diagnostic result and median from supervised outputs."""
    def read(name):
        return strict_json(raw(name))
    protocol, report = read(path + '/protocol.json'), read(path + '/report.json')
    need(report['status'] == 'passed' and report['posthash_verified'] is True
         and report['original_material_flag_preserved'] is True
         and report['comparison_engine_invoked'] is False, 'incomplete diagnostic')
    need(report['protocol']['sha256'] == sha(raw(path + '/protocol.json')), 'diagnostic protocol changed')
    need(protocol['original_complete_observations_replaced'] is False
         and protocol['comparison_engine_invoked'] is False, 'diagnostic replaced original observation')
    orders = [['baseline', 'candidate'], ['candidate', 'baseline'], ['baseline', 'candidate']]
    need(protocol['pair_orders'] == orders and protocol['expected_observations'] == 18,
         'diagnostic coverage changed')
    cases = protocol['cases']
    need(len(cases) == len({case['case_id'] for case in cases}) == 3, 'diagnostic neighbors changed')
    need(protocol['sources'] == candidate['sources'] == baseline['sources']
         and protocol['dataset'] == candidate['dataset'] == baseline['dataset']
         and protocol['surface'] == 'python_sql', 'diagnostic inputs changed')
    need(protocol['runtimes'] == {'candidate': candidate['runtime'], 'baseline': baseline['runtime']},
         'diagnostic runtime changed')
    need(protocol['resources'] == dict(memory_bytes=2 << 30, parallelism=2, timeout=180),
         'diagnostic resources changed')
    catalog = {row['id']: row for row in read(candidate['corpus']['path'])}
    for case in cases:
        need(case in candidate['cases'] and case in baseline['cases'], 'diagnostic case changed')
    expected = [(pair, case['case_id'], role) for pair, roles in enumerate(orders)
                for case in cases for role in roles]
    observations = report['observations']
    need([(row['pair'], row['case_id'], row['role']) for row in observations] == expected,
         'diagnostic execution order changed')
    times = {case['case_id']: {'baseline': [], 'candidate': []} for case in cases}
    by_id = {case['case_id']: case for case in cases}
    for row in observations:
        receipt = read(row['receipt'])
        outcome = worker_outcome(receipt, raw(receipt['stdout']))
        need(sha(raw(receipt['stdout'])) == row['stdout_sha256']
             and sha(raw(receipt['stderr'])) == receipt['stderr_sha256'], 'diagnostic output changed')
        need(receipt['command'][-2] == '--job', 'diagnostic request absent')
        job = read(receipt['command'][-1])
        need(sha(raw(receipt['command'][-1])) == receipt['job_sha256'], 'diagnostic request changed')
        case = by_id[row['case_id']]
        runtime = {Path(item['path']).name: item['path'] for item in protocol['runtimes'][row['role']]}
        need(job['kind'] == 'execute' and job['case_id'] == outcome['case_id'] == case['case_id']
             and job['case_identity'] == outcome['case_identity'] == case['case_identity'],
             'diagnostic case identity changed')
        need(job['binary'] == runtime['shardloom'] and job['library'] == runtime['_native.abi3.so'],
             'diagnostic worker runtime changed')
        need(job['sources'] == protocol['sources'] and job['dataset'] == protocol['dataset']
             and job['surface'] == 'python_sql' and job['memory_bytes'] == 2 << 30
             and job['parallelism'] == 2 and job['timeout'] == 180, 'diagnostic worker inputs changed')
        need(row['status'] == outcome['status'] == 'passed'
             and outcome['comparison_engine_invoked'] is False, 'failed diagnostic accepted')
        need(job['reference_path'] == case['reference_path']
             and job['reference_sha256'] == case['reference_sha256'], 'diagnostic reference replaced')
        reference = raw(case['reference_path'])
        need(sha(reference) == case['reference_sha256'], 'diagnostic reference changed')
        reference = strict_json(reference)
        need(reference['status'] == 'passed' and reference['case_identity'] == case['case_identity'],
             'diagnostic reference identity changed')
        record = catalog[case['case_id']]
        compare(result_from_record(reference['reference']), result_from_record(outcome['actual']),
                contract(record['contract']))
        observed = outcome['observation']
        need(observed['complete_consumption'] and observed['public_declaration'] == case['relation']
             and row['clocks'] == observed['clocks'], 'diagnostic result or clocks changed')
        fields = observed['fields']
        no_fallback(fields)
        need(fields['native_input_end_observed'] == fields['native_input_output_ownership_detached'] == 'true',
             'diagnostic source ownership incomplete')
        clocks = query_phase_timings(fields, observed['clocks']['complete_workflow_seconds'], allow_missing=False)
        for name in ('engine_execution_seconds', 'result_serialization_seconds'):
            need(clocks[name] == observed['clocks'][name], 'diagnostic clock disagrees with engine report')
        seconds = clocks['engine_execution_seconds']
        need(type(seconds) in (int, float) and math.isfinite(seconds) and seconds > 0,
             'invalid diagnostic engine clock')
        times[case['case_id']][row['role']].append(seconds)
    summaries = {}
    for name, values in times.items():
        medians = {role: statistics.median(items) for role, items in values.items()}
        delta = medians['candidate'] - medians['baseline']
        summaries[name] = dict(engine_seconds=values, median_engine_seconds=medians,
            median_ratio=medians['candidate'] / medians['baseline'],
            material_regression_reproduced=delta > .01 and delta > .20 * medians['baseline'])
    reproduced = [name for name, item in summaries.items() if item['material_regression_reproduced']]
    need(report['cases'] == summaries and report['reproduced'] == reproduced, 'diagnostic summary changed')
    need(not reproduced, 'material regression reproduced')
    return dict(observations=18, reproduced=reproduced, cases=summaries,
                disposition='not_reproduced_in_bounded_diagnostic', original_cause='unassigned')


def verify(files, manifest):
    need(manifest['verification_kind'] == 'shared_analytical_performance', 'wrong verification kind')
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
    for name, digest in PROTOCOLS.items():
        need(sha(raw(root + '/' + name)) == digest, 'prospective protocol changed')
    baseline_manifest = strict_json(files['baseline/manifest.json'])
    baseline_archive = files['baseline/packet.tar.xz']
    need(sha(baseline_archive) == BASELINE_SHA, 'baseline archive changed')
    baseline_files = object_files(baseline_manifest, baseline_archive)
    accepted_baseline = verify_baseline(baseline_files, baseline_manifest)
    frozen = {name: local(name + '/inputs.json') for name in COHORTS}
    for item in frozen.values():
        check_frozen(item['frozen_files'])
    first = frozen[COHORTS[0]]
    need(first['baseline_complete_result_replay']['verification'] == accepted_baseline,
         'accepted baseline replay changed')
    for key in ('baseline_inputs', 'baseline_observations'):
        path = first[key]['path']
        need(raw(path) == baseline_files[baseline_manifest['path_map'][path]], 'accepted baseline changed')
    comparisons = {name: replay(raw, root + '/' + name) for name in COHORTS}
    for name, comparison in comparisons.items():
        need(comparison == local(name + '-comparison.json'), 'comparison not derived from complete outputs')
    numeric, variable, ordering = (comparisons[name] for name in COHORTS)
    for name in COHORTS[1:]:
        item = frozen[name]
        need(item['baseline_complete_result_replay']['verification'] == numeric,
             'continuation changed accepted numeric baseline')
        for key, suffix in (('baseline_inputs', 'inputs.json'), ('baseline_observations', 'native-observations.json')):
            need(raw(item[key]['path']) == raw(root + '/broad-aggregate-r1/' + suffix),
                 'continuation changed baseline contents')
    need(numeric['benefit_gate_passed'] and ordering['benefit_gate_passed'], 'retained candidate lacks benefit')
    need(variable['benefit_gate_passed'] is False and not variable['material_case_regressions']
         and not variable['material_family_regressions'], 'dropped candidate disposition changed')
    need(numeric['material_case_regressions'] == ['projection_limit_direct_31']
         and ordering['material_case_regressions'] == ['aggregate_metric_scalar_mixed_arithmetic_all']
         and not numeric['material_family_regressions'] and not ordering['material_family_regressions'],
         'original regression flags changed')
    diagnostics = {}
    for name, candidate, baseline in (
        ('projection-diagnostic-r1', first, strict_json(raw(first['baseline_inputs']['path']))),
        ('ordering-aggregate-diagnostic-r1', frozen[COHORTS[2]], first),
    ):
        check_frozen(local(name + '/protocol.json')['frozen_files'])
        diagnostics[name] = diagnostic(raw, root + '/' + name, candidate, baseline)

    gates, indices, failed = {}, [], []
    for path in manifest['path_map']:
        if not path.startswith(root + '/'):
            continue
        name = path[len(root) + 1:]
        match = re.fullmatch(r'r(\d+)-[^/]+\.json', name)
        if not match:
            continue
        number = int(match.group(1))
        indices.append(number)
        gate = gates[name[:-5]] = local(name)
        need(sha(raw(root + '/' + name[:-5] + '.log')) == gate['log_sha256'], 'gate log changed')
        if number in (2, 4, 11, 17, 19):
            need(gate['status'] == 'failed' and gate['returncode'] != 0, 'historical failure replaced')
            failed.append(name[:-5])
        else:
            need(gate['status'] == 'passed' and gate['returncode'] == 0
                 and gate['process_group_drained'] is True and gate['direct_child_reaped'] is True
                 and not gate['timed_out'] and gate['interrupted_signal'] is None,
                 'required gate failed: ' + name)
    need(sorted(indices) == list(range(1, 36)), 'build/test history incomplete')
    for name, command in CARGO_GATES.items():
        need(gates[name]['command'] == ['cargo', *command.split()], 'wrong gate command: ' + name)
    runtimes = {}
    for (runtime_name, build_name, compile_gates), cohort in zip(RUNTIMES, COHORTS):
        runtime = runtimes[runtime_name] = local(runtime_name + '/manifest.json')
        build = gates[build_name]
        need(runtime['profile'] == 'release' and runtime['build_receipt'] == build_name + '.json'
             and runtime['build_receipt_sha256'] == sha(raw(root + '/' + build_name + '.json')),
             'runtime build receipt changed')
        for name, identity in runtime['source_snapshot'].items():
            source = raw(root + '/' + runtime_name + '/source/' + name)
            need(sha(source) == identity['sha256'] and len(source) == identity['bytes']
                 and identity == build['changed_source_inventory'][name], 'runtime source differs from build')
            if name.endswith(('.rs', '.toml', '.lock')):
                for gate_name in compile_gates:
                    need(gates[gate_name]['changed_source_inventory'].get(name) == identity,
                         'compiled source differs from runtime: ' + gate_name + ':' + name)
        for item in frozen[cohort]['runtime']:
            identity = runtime['files'][Path(item['path']).name]
            need(item['sha256'] == identity['sha256'] and item['generation']['bytes'] == identity['bytes'],
                 'benchmark runtime changed')
    runtime_name = 'ordering-release-runtime-r1'
    runtime = runtimes[runtime_name]
    public = local('analytical-surfaces-r1.json')
    need({key: public[key] for key in ('status', 'tests', 'failures', 'errors', 'skipped')}
         == dict(status='passed', tests=9, failures=0, errors=0, skipped=0), 'public matrix incomplete')
    need(public['test_names'] == ['test_native_shared_provider_binding.NativeSharedProviderBindingTests.test_'
                                 + name for name in PUBLIC_TESTS], 'public matrix coverage changed')
    need(public['runtime_manifest_sha256'] == sha(raw(root + '/' + runtime_name + '/manifest.json'))
         and public['products'] == {root + '/' + runtime_name + '/' + name: item['sha256']
                                   for name, item in runtime['files'].items()}, 'public matrix runtime changed')
    streamed = local('ordering-public-r1/summary.json')
    need(streamed['status'] == 'passed' and streamed['families'] == ['ordering']
         and [row['name'] for row in streamed['cases']] == ORDER_CASES
         and all(row['status'] == 'passed' for row in streamed['cases']), 'streamed ordering coverage changed')
    need(streamed['sources'][root + '/' + runtime_name + '/shardloom'] == runtime['files']['shardloom']['sha256'],
         'streamed ordering runtime changed')
    for path, digest in streamed['sources'].items():
        if Path(path).name != 'shardloom':
            need(sha(raw(root + '/ordering-public-r1/logs/' + Path(path).name)) == digest,
                 'streamed ordering harness changed')
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
    return dict(status='passed', complete_comparisons_per_cohort=347, complete_rows_per_cohort=12565,
        retained_candidates=['numeric_aggregate_reuse', 'bounded_streaming_ordering'],
        dropped_candidates=['borrowed_variable_width_output'],
        engine_seconds={name: result['candidate_engine_seconds'] for name, result in comparisons.items()},
        original_material_case_regressions={name: result['material_case_regressions'] for name, result in comparisons.items()},
        diagnostic_dispositions=diagnostics, failed_validation_attempts=sorted(failed),
        public_feature_suites=9, public_ordering_cases=29, package_tests=194,
        new_competitor_queries=0, item_20_complete=False,
        limitations=['One complete observation per candidate; no statistical confidence or isolated causal claim.',
            'Original regression flags remain in original totals; bounded diagnostics did not reproduce them.',
            'Complete saved results replay without executing queries or archived code.',
            'Content hashes establish internal consistency, not signed provenance.',
            'Large data and binaries are represented by identity, not reconstructed by this packet.'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('packet', type=Path)
    args = parser.parse_args()
    manifest = strict_json(bounded_bytes(args.packet / 'manifest.json', MAX_FILE))
    files = object_files(manifest, bounded_bytes(args.packet / 'packet.tar.xz', MAX_ARCHIVE))
    print(json.dumps(verify(files, manifest), indent=2))


if __name__ == '__main__':
    main()
