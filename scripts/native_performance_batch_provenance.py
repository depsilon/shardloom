# SPDX-License-Identifier: Apache-2.0
"""Preserve intake corrections, the real merge regression and accepted baseline."""
import gzip
import math
from pathlib import Path

from native_workflow_protocol import strict_json
from native_performance_batch_workflow_replay import product_sources
from query_surface.replay import need, sha
from shared_performance_replay import complete_gate, linked

FIXTURES = {
    'python/tests/native_performance_batch_cases.py': (
        '449938924ec0cfe0301f47f61fce585c1fda4954947cb019c1ac4b11731f9602',
        'f4cb92c0482d0f020dc8fc9e080717777ab04c749c4095beff6a93243cab385e'),
    'python/tests/test_native_performance_batch_binding.py': (
        '39121b7066ceef4fcc444675b95b04dd859edcb3b5605817e8cb56b14107eb61',
        'e258487730cdad2d41f0fed787db061dec5c360bea1dec230f168b545f0e83df'),
}
BASELINE_PACKET = {
    'manifest.json': '3962bd8db9aaa5cbe8b24556e76c195acb35ba7305c1949799c3c0f362596312',
    'acceptance.json': '35e7ceec8267cc1d41ab81282e10f507874086f20bdc892d64bb2944fa5351e2',
}


def supplemental(bundle, evidence, name):
    matches = [item for item in evidence['supplements'] if Path(item['path']).name == name]
    need(len(matches) == 1, 'missing or duplicate proof: ' + name)
    return linked(bundle, matches[0])


def fixture_corrections(bundle, evidence, runtime):
    proof = supplemental(bundle, evidence, 'batch-fixture-corrections.json')
    need(proof['status'] == 'passed' and proof['query_executions'] == 0
         and proof['runtime_sources_changed'] is proof['runtime_products_changed'] is False,
         'fixture correction scope changed')
    bundle.identity(proof['helper'])
    need(len(proof['manifests']) == 2, 'fixture runtime pair missing')
    before, after = [linked(bundle, item) for item in proof['manifests']]
    need(before['products'] == after['products'] and before['files'] == after['files']
         and set(before['source_snapshot']) == set(after['source_snapshot']),
         'fixture correction changed the runtime products or source inventory')
    changed = {name for name, item in before['source_snapshot'].items()
               if after['source_snapshot'][name] != item}
    need(changed == set(FIXTURES) and {row['path'] for row in proof['sources']} == changed
         and len(proof['sources']) == 2, 'fixture correction changed additional sources')
    for row in proof['sources']:
        name = row['path']
        original, updated = [bundle.identity(row[key]) for key in ('before', 'after')]
        need((sha(original), sha(updated)) == FIXTURES[name]
             and row['before']['sha256'] == before['source_snapshot'][name]['sha256']
             and row['after']['sha256'] == after['source_snapshot'][name]['sha256']
             and runtime['source_snapshot'][name] == after['source_snapshot'][name],
             'fixture correction differs from reviewed exact bytes')
    need(len(proof['public_attempts']) == 3, 'public intake attempts missing')
    retained = []
    for item, expected in zip(proof['public_attempts'], [(28, 8, 0), (35, 1, 1), (36, 0, 0)], strict=True):
        report = linked(bundle, item)
        need((report['complete_controls'], len(report['errors']), len(report['failures'])) == expected
             and report['status'] == ('failed' if expected[1] or expected[2] else 'passed')
             and report['acceptance_workflows_executed'] == 0, 'public intake outcome changed')
        for part in report['parts']:
            data = gzip.decompress(bundle.identity(part))
            need(len(data) < 32 << 20, 'intake result exceeds bound')
            record = strict_json(data)
            need(set(record) == {'case', 'expected', 'actual', 'report'}
                 and record['case']['case_id'] == part['case_id'], 'intake result identity changed')
        retained.append(dict(complete_controls=expected[0], errors=expected[1], failures=expected[2]))
    return dict(fixture_files=2, runtime_products_identical=True, retained_attempts=retained)


def rich_merge_correction(bundle, evidence, runtime, gates):
    root = evidence['root']
    old = linked(bundle, next(item for item in evidence['runtimes']
                             if '/performance-batch-release-r2/' in item['path']))
    corrected = linked(bundle, next(item for item in evidence['runtimes']
                                   if '/performance-batch-release-r3/' in item['path']))
    changed = {name for name, item in old['source_snapshot'].items()
               if name.endswith(('.rs', '.toml', '.lock')) and corrected['source_snapshot'].get(name) != item}
    need(changed == {'shardloom-vortex/src/local_primitives/native_relational_spill.rs',
                    'shardloom-vortex/src/local_primitives/native_relational_spill_merge_tests.rs',
                    'shardloom-vortex/src/local_primitive_relational_typed_key_tests.rs'},
         'post-package runtime correction scope changed')
    need(all(runtime['source_snapshot'].get(name) == corrected['source_snapshot'][name] for name in changed),
         'candidate dropped the rich merge correction')
    test = 'python/tests/test_native_set_binding.py'
    need(old['source_snapshot'][test] == runtime['source_snapshot'][test],
         'rich-set regression fixture or resource grant changed')
    failed = gates['b037-batch-installed-package.json']
    need(failed['status'] == 'failed' and failed['returncode'] == 1,
         'original package regression missing')
    log = bundle.raw(root + '/installed-performance-batch-r1/integration.log').decode()
    need('requested=8484160, reserved=8322184, limit=16777216' in log
         and "(kind='union') ... ERROR" in log and 'FAILED (errors=1)' in log,
         'original rich-set memory regression changed')
    passing = gates['b050-batch-rich-set-regression.json']
    complete_gate(passing)
    final_log = bundle.raw(root + '/b050-batch-rich-set-regression.log').decode()
    name = 'test_native_set_rich_membership_spill_preserves_complete_nested_and_typed_rows'
    need(name in final_log and 'Ran 1 test in ' in final_log
         and '\nOK\n' in final_log and 'skipped' not in final_log,
         'original rich-set regression has not passed')
    return dict(original_memory_bytes=16 << 20, original_fixture_unchanged=True,
                runtime_files_changed=1, native_test_files_changed=2,
                original_failure_preserved=True, original_public_control_passed=True)


def baseline_binding(bundle, evidence, external):
    proof = linked(bundle, evidence['baseline_binding'])
    need(proof['status'] == 'passed' and proof['query_executions'] == 0
         and proof['saved_observations_byte_exact'] is proof['original_protocol_and_observations_unchanged'] is True
         and proof['complete347_engine_seconds'] == 11.979219492
         and proof['supplemental48_engine_seconds'] == 0.920503372,
         'accepted baseline binding changed')
    bundle.identity(proof['helper'])
    packet = {}
    for name, digest in BASELINE_PACKET.items():
        need(proof['packet'][name]['sha256'] == digest, 'accepted packet anchor changed')
        packet[name] = linked(bundle, proof['packet'][name])
    accepted = packet['acceptance.json']
    need(accepted['status'] == 'passed' and accepted['disposition'] == 'retain_both_complete_cohort_gains'
         and accepted['comparison']['candidate_engine_seconds'] == 11.979219492
         and accepted['supplemental_comparison']['engine_execution_seconds'] == 0.920503372,
         'previous acceptance changed')
    need(len(proof['observations']) == 6, 'accepted observation inventory changed')
    observations = {}
    for path, item in proof['observations'].items():
        need(item['path'] == path and packet['manifest.json']['path_map'][path] == [item['sha256']]
             and packet['manifest.json']['records'][item['sha256']]['bytes'] == item['bytes'],
             'saved baseline bytes differ from previous packet')
        observations[path] = linked(bundle, item)
    root = evidence['root']
    baseline_inputs = None
    for folder, final, count, rows, seconds, field in (
        ('broad-ordered-input-r1', 'disposition.json', 347, 12565, 11.979219492, 'candidate_engine_seconds'),
        ('ordered-input-candidate-r1', 'replay.json', 48, 557420, 0.920503372, 'engine_execution_seconds'),
    ):
        inputs, measured, saved = [observations[root + '/' + folder + '/' + name]
                                  for name in ('inputs.json', 'native-observations.json', final)]
        need(measured['status'] == saved['status'] == 'passed'
             and measured['complete'] is measured['posthash_verified'] is True
             and len(inputs['cases']) == len(measured['observations']) == saved['complete_comparisons'] == count
             and saved['complete_rows'] == rows and saved[field] == seconds and saved['benefit_gate_passed'] is True
             and [row['case_id'] for row in inputs['cases']] == [row['case_id'] for row in measured['observations']]
             and all(row['status'] == 'passed' and row['clocks']['engine_timing_reported'] is True
                     for row in measured['observations'])
             and math.fsum(row['clocks']['engine_execution_seconds'] for row in measured['observations']) == seconds,
             'saved baseline is incomplete or has different clocks')
        if count == 347:
            baseline_inputs = inputs
    runtime = linked(bundle, baseline_inputs['runtime_manifest'])
    need(set(proof['runtime_products']) == set(runtime['products']), 'accepted runtime product coverage changed')
    for name, item in proof['runtime_products'].items():
        need(item['sha256'] == runtime['products'][name]
             and (item['path'], item['sha256'], item['bytes']) in external,
             'accepted runtime identity changed')
    source_map = linked(bundle, proof['source_map'])
    need(source_map['source_revision'] == runtime['source_revision']
         and len(source_map['source_entries']) == len(runtime['source_snapshot'])
         and {row['path'] for row in source_map['source_entries']} == set(runtime['source_snapshot']),
         'accepted source inventory changed')
    for row in source_map['source_entries']:
        need(row['status'] == 'proved'
             and {key: row[key] for key in ('bytes', 'sha256')} == runtime['source_snapshot'][row['path']],
             'accepted source proof changed')
    product_sources(bundle, root, dict(baseline_inputs, role='accepted',
        product_input_identity_checks=proof['independently_rechecked_product_sources']), runtime)
    return dict(complete347_engine_seconds=11.979219492, historical48_engine_seconds=0.920503372,
                saved_observations_byte_exact=True, query_executions=0)
