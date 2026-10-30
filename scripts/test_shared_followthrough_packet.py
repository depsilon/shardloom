# SPDX-License-Identifier: Apache-2.0
"""Adversarial saved-result controls; neither native nor comparator code executes."""
import copy
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'scripts'), str(ROOT / 'python/src'),
               str(ROOT / 'benchmarks/traditional_analytics')]
from native_workflow_protocol import strict_json
from query_surface.replay import sha
from query_surface.semantics import ResultMismatch
from shared_performance_bundle import Bundle
from shared_performance_replay import linked
from shared_followthrough_join_replay import encoded, replay_log, validate_report
from shared_followthrough_materialized_replay import declared_jobs, validate_result
from shared_followthrough_replay import (
    diagnostic_observations, failed_public_values, fixture_correction, frozen_files,
    harness_compatibility, installed_package, source_matches, verify,
)


class FollowthroughTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bundle = Bundle(ROOT / 'docs/benchmarks/evidence/native-shared-followthrough-2026-10-30')
        cls.anchors = strict_json((ROOT / 'scripts/shared_followthrough_anchors.json').read_bytes())
        cls.root = cls.anchors['root']
        cls.supplements = {Path(item['path']).name: linked(cls.bundle, item)
                           for item in cls.anchors['supplements'] if item['path'].endswith('.json')}
        cls.join_log = cls.bundle.raw(cls.root + '/p206-join-movement-final-focused.log')
        cls.join_rows = [strict_json(line[len('JOIN_MOVEMENT '):]) for line in cls.join_log.decode().splitlines()
                         if line.startswith('JOIN_MOVEMENT ')]
        cls.semantic_root = cls.root + '/materialized-reuse-semantics-r1/attempt-r1'
        cls.semantic = strict_json(cls.bundle.raw(cls.semantic_root + '/protocol.json'))
        cls.jobs = {job['name']: job for job in cls.semantic['jobs']}
        cls.runtimes = {Path(item['path']).parent.name: linked(cls.bundle, item) for item in cls.anchors['runtimes']}

    def materialized(self, name='00-one_consumer-parquet-materialized'):
        job = copy.deepcopy(self.jobs[name])
        value = strict_json(self.bundle.raw(self.semantic_root + '/logs/' + name + '.stdout.json'))
        reference = strict_json(self.bundle.raw(job['reference_path']))
        return value, job, reference

    def test_complete_join_grid_rebuilds_from_independent_formulas(self):
        self.assertEqual(replay_log(self.join_log), self.supplements['join-movement-measurements.json']['measurements'])

    def test_changed_join_values_cannot_be_repaired_by_changing_saved_expected_and_digest(self):
        value = copy.deepcopy(self.join_rows[1])
        value['actual'][0]['count'] += 1
        value['expected'] = copy.deepcopy(value['actual'])
        value['complete_result_sha256'] = sha(encoded(value['actual']).encode())
        with self.assertRaisesRegex(ValueError, 'complete join measurement result'):
            validate_report(value)

    def test_join_multiplicity_cells_source_release_and_alias_credits_are_required(self):
        changes = {
            'multiplicity': lambda value: value['stages'][0].update(rows=value['stages'][0]['rows'] + 1),
            'cells': lambda value: value['stages'][0].update(cells=0),
            'buffer_scope': lambda value: value['stages'][0].update(largest_output_buffer_bytes=1 << 40),
            'producer_coverage': lambda value: value.update(producer_batches_released=0),
            'producer_release': lambda value: value.update(source_release_before_next_batch=False),
            'retained_output': lambda value: value.update(retained_values_after_session_close=False),
            'last_alias': lambda value: value.update(final_output_credits_released=False),
        }
        for name, edit in changes.items():
            with self.subTest(name=name):
                value = copy.deepcopy(self.join_rows[1])
                edit(value)
                with self.assertRaises(ValueError):
                    validate_report(value)

    def test_missing_duplicate_or_reordered_join_measurements_are_rejected(self):
        rows = self.join_rows
        for values in (rows[:-1], rows + [rows[0]], [rows[1], rows[0], *rows[2:]]):
            data = '\n'.join('JOIN_MOVEMENT ' + json.dumps(row) for row in values).encode()
            with self.assertRaisesRegex(ValueError, 'grid incomplete or reordered'):
                replay_log(data)

    def test_materialized_complete_result_and_repeated_action_pass(self):
        for name in ('00-one_consumer-parquet-materialized', '01-two_consumers-parquet-materialized',
                     '06-zero_rows-parquet-materialized'):
            validate_result(*self.materialized(name))

    def test_materialized_eleven_corruptions_are_rejected(self):
        changes = {
            'changed_complete_value': lambda value, _: value['observations'][0]['actual']['rows'][0].update(count=999999),
            'missing_complete_row': lambda value, _: value['observations'][0]['actual']['rows'].pop(),
            'changed_schema': lambda value, _: value['observations'][0]['actual']['schema'][0]['type'].update(nullable=True),
            'changed_engine_clock': lambda value, _: value['observations'][0]['clocks'].update(engine_execution_seconds=0),
            'hidden_external_execution': lambda value, _: value['observations'][0]['fields'].update(external_engine_invoked='true'),
            'false_reuse': lambda value, _: value['observations'][0]['fields'].update(relational_materialized_reuses='7'),
            'changed_role': lambda value, _: value.update(materialized=False),
            'changed_statement': lambda value, _: value.update(sql_declaration='SELECT 1'),
            'unproved_cleanup': lambda value, _: value.update(spill_workspace_clean=False),
        }
        for name, edit in changes.items():
            with self.subTest(name=name):
                value, job, reference = self.materialized()
                edit(value, job)
                with self.assertRaises((ValueError, ResultMismatch)):
                    validate_result(value, job, reference)
        value, job, reference = self.materialized('06-zero_rows-parquet-materialized')
        value['observations'][0]['fields']['relational_materialized_rows'] = '0'
        with self.assertRaisesRegex(ValueError, 'capture/reuse'):
            validate_result(value, job, reference)
        value, job, reference = self.materialized('01-two_consumers-parquet-materialized')
        value['observations'].pop()
        with self.assertRaisesRegex(ValueError, 'action coverage'):
            validate_result(value, job, reference)

    def test_changed_materialization_reference_cannot_hide_value_corruption(self):
        value, job, reference = self.materialized()
        value['observations'][0]['actual']['rows'][0]['count'] += 1
        reference['reference'] = copy.deepcopy(value['observations'][0]['actual'])
        with self.assertRaises((ValueError, ResultMismatch)):
            validate_result(value, job, reference)

    def test_materialized_fixture_grid_cannot_be_narrowed(self):
        prepared = strict_json(self.bundle.raw(self.root + '/materialized-reuse-semantics-r1/prepared.json'))
        self.assertEqual(len(declared_jobs(self.semantic_root, prepared, True)), 60)
        prepared['cases'].pop()
        with self.assertRaisesRegex(ValueError, 'case coverage'):
            declared_jobs(self.semantic_root, prepared, True)

    def test_installed_package_requires_exact_runtime_and_test_count(self):
        runtime = self.runtimes['bounded-ordering-release-r2']
        path = self.root + '/installed-bounded-ordering-r1'
        self.assertEqual(installed_package(self.bundle, path, runtime, 266)['tests'], 266)
        with self.assertRaisesRegex(ValueError, 'acceptance incomplete'):
            installed_package(self.bundle, path, runtime, 265)
        bad = copy.deepcopy(runtime)
        bad['products']['shardloom'] = '0' * 64
        with self.assertRaisesRegex(ValueError, 'runtime differs'):
            installed_package(self.bundle, path, bad, 266)

    def test_failed_public_attempt_retains_complete_values_without_acceptance(self):
        value = strict_json(self.bundle.raw(self.root + '/bounded-ordering-public-r1.json'))
        failed_public_values(value)
        value['complete_comparisons'][0]['actual']['rows'][0]['row_id'] += 1
        with self.assertRaises((ValueError, ResultMismatch)):
            failed_public_values(value)

    def test_fixture_only_correction_cannot_exempt_an_engine_change(self):
        proof = self.supplements['bounded-ordering-public-fixture-correction.json']
        before = self.runtimes['bounded-ordering-release-r1']
        after = copy.deepcopy(self.runtimes['bounded-ordering-release-r2'])
        failed = strict_json(self.bundle.raw(self.root + '/bounded-ordering-public-r1.json'))
        fixture_correction(self.bundle, proof, before, after, failed)
        key = next(name for name in after['source_snapshot'] if name.endswith('.rs'))
        after['source_snapshot'][key]['sha256'] = '0' * 64
        with self.assertRaisesRegex(ValueError, 'runtime source/products'):
            fixture_correction(self.bundle, proof, before, after, failed)

    def test_required_gates_must_match_measured_source(self):
        gate = strict_json(self.bundle.raw(self.root + '/p186-bounded-ordering-native-workspace.json'))
        runtime = copy.deepcopy(self.runtimes['bounded-ordering-release-r1'])
        source_matches(gate, runtime)
        key = next(name for name in gate['changed_source_inventory'] if name.endswith('.rs'))
        runtime['source_snapshot'][key]['sha256'] = '0' * 64
        with self.assertRaisesRegex(ValueError, 'tested source differs'):
            source_matches(gate, runtime)

    def test_reporting_exception_requires_exact_path_and_unchanged_execution(self):
        proof = copy.deepcopy(self.supplements['mixed-aggregate-harness-compatibility.json'])
        harness_compatibility(self.bundle, proof)
        proof['exact_reporting_delta']['execution_change'] = True
        with self.assertRaisesRegex(ValueError, 'wrong reporting exception'):
            harness_compatibility(self.bundle, proof)

    def test_protocols_cannot_be_removed_as_external_payloads(self):
        item = dict(path='/evidence/protocol.json', bytes=2, sha256=sha(b'{}'))
        with self.assertRaisesRegex(ValueError, 'nonpayload'):
            frozen_files(self.bundle, [item], {(item['path'], item['sha256'], item['bytes'])})

    def test_historical_predicate_and_ordering_clocks_remain_bound_to_raw_outputs(self):
        observed = diagnostic_observations(self.bundle, self.root, self.supplements)
        self.assertEqual((observed['predicate_cases'], observed['ordering_cases']), (91, 10))
        bad = copy.deepcopy(self.supplements)
        bad['predicate-followthrough-observations.json']['cases'][0]['original_polars']['complete_workflow_seconds'] = 0
        with self.assertRaisesRegex(ValueError, 'diagnostic clock'):
            diagnostic_observations(self.bundle, self.root, bad)

    def test_failed_attempts_cannot_be_dropped_from_anchors(self):
        bad = copy.deepcopy(self.anchors)
        bad['gates'] = [item for item in bad['gates'] if item['status'] == 'passed']
        with self.assertRaisesRegex(ValueError, 'anchors changed'):
            verify(self.bundle, bad)


if __name__ == '__main__':
    unittest.main()
