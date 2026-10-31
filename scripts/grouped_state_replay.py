# SPDX-License-Identifier: Apache-2.0
"""Verify grouped-state semantics, ownership, provenance and complete-cohort evidence."""
from pathlib import Path

from grouped_state_cohort_replay import harness_continuity
from grouped_state_mechanism_replay import assembly, work
from grouped_state_public_replay import public_replay
from native_workflow_protocol import strict_json
from parquet_demand_completion_replay import replay as replay_cohort
from query_surface.replay import need, sha
from shared_followthrough_replay import frozen_files, installed_package, source_matches
from shared_performance_replay import complete_gate, linked, test_totals

BUILD = 'build --locked --offline --release --features release-user-surfaces -p shardloom-cli -p shardloom-python'
REQUIRED = {
    's010-grouped-state-native-clippy-final.json': 'clippy --locked --offline --workspace --all-targets --features release-user-surfaces --profile ci-native -- -D warnings',
    's011-grouped-state-native-workspace.json': 'test --locked --offline --workspace --all-targets --features release-user-surfaces --profile ci-native',
    's013-grouped-state-default-clippy.json': 'clippy --locked --offline --workspace --all-targets -- -D warnings',
    's014-grouped-state-default-workspace.json': 'test --locked --offline --workspace --all-targets',
    's015-grouped-state-format-check.json': 'fmt --all -- --check',
    's017-grouped-state-release-build.json': BUILD,
}
FAILURES = ['s002-grouped-state-existing.json', 's007-grouped-state-native-clippy.json',
            's009-grouped-state-native-clippy-corrected.json', 's021-grouped-state-assembly.json']


def tested_source_matches(gate, runtime):
    # These saved-result validators are developed after the native test run.
    # Their gate-time bytes remain in the packet, but they are neither product
    # inputs nor tests run by Cargo. All engine, public fixture, harness and
    # package sources retain the ordinary exact source comparison.
    evidence_only = {
        'scripts/' + name for name in ('grouped_state_replay.py',
        'grouped_state_cohort_replay.py', 'grouped_state_mechanism_replay.py',
        'grouped_state_public_replay.py', 'verify_grouped_state_packet.py',
        'test_grouped_state_packet.py')
    }
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
    need('native-vortex' in bundle.raw(root + '/s002-grouped-state-existing.log').decode()
         and 'error' in bundle.raw(root + '/s007-grouped-state-native-clippy.log').decode()
         and 'E028' in bundle.raw(root + '/s009-grouped-state-native-clippy-corrected.log').decode(),
         'historical validation failure changed')
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


def verify(bundle, anchors):
    evidence = bundle.index['evidence']
    need(evidence == anchors and evidence['kind'] == 'grouped_state_access', 'grouped-state anchors changed')
    root = evidence['root']
    external = {(item['path'], item['sha256'], item['bytes']) for item in evidence['external_artifacts']}
    need(len(external) == len(evidence['external_artifacts']), 'duplicate external payload identity')
    gates = gates_replay(bundle, evidence)
    runtime = runtime_replay(bundle, evidence, gates)
    native = test_totals(bundle.raw(root + '/s011-grouped-state-native-workspace.log').decode(), (119, 6579, 0, 36))
    default = test_totals(bundle.raw(root + '/s014-grouped-state-default-workspace.log').decode(), (107, 3290, 0, 0))
    for name in ('s012-grouped-state-mechanism.json', 's016-grouped-state-python-source.json',
                 's019-grouped-state-public-results.json', 's020-grouped-state-installed-package.json',
                 's022-grouped-state-assembly-corrected.json'):
        complete_gate(gates[name])
    binding = work(bundle.raw(root + '/s012-grouped-state-mechanism.log').decode())
    declaration = linked(bundle, evidence['declarations'])
    for path, digest in declaration['sources'].items():
        bundle.raw(path, digest)
    contract = linked(bundle, evidence['contract'])
    need(contract['schema_version'] == 'shardloom.grouped_state_contract.v1'
         and contract['status'] == 'frozen_before_runtime_edits'
         and contract['accepted_engine_seconds'] == 12.055132613
         and contract['queries'] == 347 and contract['rows'] == 12565
         and contract['full43'] == 'held_until_item18', 'prospective grouped contract changed')
    bundle.identity({key: contract[key] for key in ('path', 'bytes', 'sha256')})
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
    package = installed_package(bundle, root + '/installed-grouped-state-r1', runtime, 284)
    compiled = assembly(bundle, linked(bundle, evidence['assembly_manifest']),
                        linked(bundle, evidence['assembly_review']), runtime)
    summary = dict(status='passed', native_workspace=native, default_workspace=default,
        public_acceptance=public_result, installed_package=package, failed_validation_attempts=FAILURES,
        mechanism=binding, compiled_code=compiled, external_engine_invoked=False,
        full43_run=False, query_executions=0)
    cohort = evidence['cohort']
    if cohort is None:
        return dict(**summary, disposition='local_correctness_only_performance_pending')
    for name in ('s024-grouped-state-cohort-freeze.json', 's025-grouped-state-cohort-replay.json'):
        complete_gate(gates[name])
    inputs, protocol = linked(bundle, cohort['inputs']), linked(bundle, cohort['protocol'])
    frozen_files(bundle, inputs['frozen_files'] + protocol['prerequisites'], external)
    need(linked(bundle, inputs['runtime_manifest']) == runtime and inputs['repeats'] == 1
         and inputs['memory_bytes'] == 2 << 30 and inputs['parallelism'] == 2
         and protocol['performance_executions_before_freeze'] == 0
         and protocol['source_contract']['sha256'] == contract['sha256'], 'cohort admission changed')
    need(protocol['retention_decision'] == dict(rule='strict_complete_cohort_gain',
        declared_families=['grouped_aggregation', 'global_aggregation'],
        material_case_regression=dict(fraction=0.20, absolute_seconds=0.010),
        material_family_regression=dict(fraction=0.20, absolute_seconds=0.050)), 'frozen benefit rule changed')
    continuity = harness_continuity(bundle, inputs, protocol, runtime, declaration)
    versions = {item['path']: item['sha256'] for item in inputs['frozen_files']}
    raw = lambda path: bundle.raw(path, versions.get(path))
    result = replay_cohort(raw, root + '/' + cohort['name'])
    saved = strict_json(raw(root + '/' + cohort['name'] + '/disposition.json'))
    need(result['complete_comparisons'] == 347 and result['complete_rows'] == 12565
         and result['baseline_engine_seconds'] == 12.055132613
         and result['candidate_engine_seconds'] == 11.997152207
         and all(saved[key] == value for key, value in result.items())
         and saved['accepted_baseline'] is result['benefit_gate_passed'] and saved['role'] == 'candidate'
         and saved['decision'] == ('retain_lower_complete_engine_total' if result['benefit_gate_passed']
             else 'drop_no_complete_engine_gain'), 'complete-cohort disposition changed')
    return dict(**summary, disposition=saved['decision'], comparison=result, harness_continuity=continuity)
