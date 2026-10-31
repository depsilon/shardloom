# SPDX-License-Identifier: Apache-2.0
"""Reject altered complete-batch evidence without running native queries."""
import copy
import gzip
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
PACKET = ROOT / 'docs/benchmarks/evidence/native-performance-batch-2026-10-31'
sys.path[:0] = [str(ROOT / 'scripts'), str(ROOT / 'python/src'),
               str(ROOT / 'benchmarks/traditional_analytics')]
from native_workflow_protocol import strict_json
from native_performance_batch_public_replay import public_replay, replay_trace
from native_performance_batch_metadata_replay import binding_correction, metadata_reference, revised_protocol
from native_performance_batch_provenance import baseline_binding, fixture_corrections, rich_merge_correction
from native_performance_batch_replay import (
    combined_disposition, driver_review, encoded_inputs, gates_replay, harness_continuity,
    native_controls, release_hooks, runtime_replay,
)
from native_performance_batch_workflow_replay import workflow_replay
from parquet_demand_completion_replay import replay as replay_cohort
from query_surface.replay import sha
from query_surface.semantics import ResultMismatch
from shared_followthrough_replay import installed_package
from shared_performance_bundle import Bundle
from shared_performance_replay import linked


def raw(value):
    return json.dumps(value, separators=(',', ':'), allow_nan=False).encode()


class ChangedRecords:
    """Test semantic rejection after bypassing transport identity verification."""
    def __init__(self, bundle, replacements):
        self.bundle, self.replacements = bundle, replacements

    def raw(self, path, digest=None):
        return self.replacements[path] if path in self.replacements else self.bundle.raw(path, digest)

    def identity(self, item):
        return self.replacements[item['path']] if item['path'] in self.replacements else self.bundle.identity(item)


class NativePerformanceBatchPacketTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bundle = Bundle(PACKET)
        cls.anchors = strict_json((ROOT / 'scripts/native_performance_batch_anchors.json').read_bytes())
        cls.root = cls.anchors['root']
        cls.runtime = linked(cls.bundle, cls.anchors['runtime'])
        cls.public = linked(cls.bundle, cls.anchors['public_controls'])
        cls.declaration = linked(cls.bundle, cls.anchors['public_declaration'])
        cls.gates = gates_replay(cls.bundle, cls.anchors)
        cls.external = {(row['path'], row['sha256'], row['bytes']) for row in cls.anchors['external_artifacts']}

    def test_failed_attempts_cannot_be_removed(self):
        bad = copy.deepcopy(self.anchors)
        bad['gates'] = [item for item in bad['gates'] if item['status'] == 'passed']
        with self.assertRaises(ValueError):
            gates_replay(self.bundle, bad)

    def test_compiled_inputs_must_match_final_validation(self):
        bad = copy.deepcopy(self.runtime)
        source = 'shardloom-vortex/src/local_primitives/native_relational_spill.rs'
        bad['source_snapshot'][source]['sha256'] = '0' * 64
        view = ChangedRecords(self.bundle, {self.anchors['runtime']['path']: raw(bad)})
        with self.assertRaises(ValueError):
            runtime_replay(view, self.anchors, self.gates)

    def test_all_ten_mechanisms_require_passing_controls(self):
        proof = linked(self.bundle, self.anchors['native_controls'])
        self.assertEqual(native_controls(self.bundle, proof, self.runtime)['families'], 10)
        for family in proof['families']:
            bad = copy.deepcopy(proof)
            bad['families'][family].pop()
            with self.subTest(family=family), self.assertRaises(ValueError):
                native_controls(self.bundle, bad, self.runtime)

    def test_public_values_schema_ownership_resources_and_fallback(self):
        part = self.public['parts'][0]
        original = strict_json(gzip.decompress(self.bundle.identity(part)))
        changes = {
            'value': lambda row: row['actual']['rows'][0].update(id=-100),
            'schema': lambda row: row['actual']['schema'][0].update(name='wrong'),
            'completion': lambda row: row['report'].update(complete_consumption=False),
            'source_release': lambda row: row['report'].update(source_session_released_before_export=False),
            'fallback': lambda row: row['report']['fields'].update(external_engine_invoked='true'),
            'memory': lambda row: row['report']['fields'].update(execution_resource_admitted_memory_bytes='1'),
            'parallelism': lambda row: row['report']['fields'].update(execution_resource_admitted_max_parallelism='3'),
        }
        for label, change in changes.items():
            record = copy.deepcopy(original)
            change(record)
            compressed = gzip.compress(raw(record), mtime=0)
            bad = copy.deepcopy(self.public)
            bad['parts'][0].update(bytes=len(compressed), sha256=sha(compressed))
            with self.subTest(label=label), self.assertRaises((ValueError, ResultMismatch)):
                public_replay(ChangedRecords(self.bundle, {part['path']: compressed}),
                              bad, self.declaration, self.runtime)

    def test_public_grid_and_independent_reference_are_required(self):
        for change in (
            lambda row: row['cases'].pop(),
            lambda row: row.update(acceptance_workflows_executed=1),
            lambda row: row['references'].update({row['cases'][0]['case_id']: '0' * 64}),
        ):
            bad = copy.deepcopy(self.declaration)
            change(bad)
            with self.assertRaises(ValueError):
                public_replay(self.bundle, self.public, bad, self.runtime)

    def test_streaming_control_requires_one_producer_and_real_spill(self):
        part = next(row for row in self.public['parts'] if row['case_id'] == 'control_B9_pressured_16385')
        record = strict_json(gzip.decompress(self.bundle.identity(part)))
        for change in (
            lambda row: row['producer_traces']['input'].update(opened=2),
            lambda row: row['producer_traces']['input'].update(closed=False),
            lambda row: row['fields'].update(native_input_replay_spilled_sources='0'),
        ):
            bad = copy.deepcopy(record['report'])
            change(bad)
            with self.assertRaises(ValueError):
                replay_trace(record['case'], bad, public=True)

    def test_encoded_fixtures_require_physical_inspection_and_full_readback(self):
        original = linked(self.bundle, self.anchors['encoded_inputs'])
        for change in (
            lambda row: row['files'].pop(),
            lambda row: row['files'][0].update(complete_provider_readback=False),
            lambda row: row['files'][0]['physical'].update(external_engine_invoked=True),
        ):
            bad = copy.deepcopy(original)
            change(bad)
            with self.assertRaises(ValueError):
                encoded_inputs(bad)

    def test_installed_package_has_no_skip_and_uses_exact_products(self):
        directory = str(Path(self.anchors['package']['path']).parent)
        bad = copy.deepcopy(self.runtime)
        bad['products']['_native.abi3.so'] = '0' * 64
        with self.assertRaises(ValueError):
            installed_package(self.bundle, directory, bad, 286)
        path = directory + '/integration.json'
        integration = strict_json(self.bundle.raw(path))
        integration['skipped'] = 1
        with self.assertRaises(ValueError):
            installed_package(ChangedRecords(self.bundle, {path: raw(integration)}), directory, self.runtime, 286)

    def test_release_products_exclude_test_observation_owner(self):
        proof = linked(self.bundle, self.anchors['release_hooks'])
        original = proof['records'][0]['symbols']
        data = gzip.decompress(self.bundle.identity(original)) + b'\nnative_relational_observation\n'
        compressed = gzip.compress(data, mtime=0)
        bad = copy.deepcopy(proof)
        bad['records'][0]['symbols'].update(bytes=len(compressed), sha256=sha(compressed))
        bad['records'][0]['symbol_bytes'] = len(data)
        with self.assertRaises(ValueError):
            release_hooks(ChangedRecords(self.bundle, {original['path']: compressed}), bad, self.runtime)

    def test_fixture_failures_and_real_runtime_regression_are_distinct(self):
        item = next(row for row in self.anchors['supplements']
                    if Path(row['path']).name == 'batch-fixture-corrections.json')
        bad = linked(self.bundle, item)
        bad['runtime_products_changed'] = True
        with self.assertRaises(ValueError):
            fixture_corrections(ChangedRecords(self.bundle, {item['path']: raw(bad)}), self.anchors, self.runtime)
        bad = copy.deepcopy(self.runtime)
        bad['source_snapshot']['python/tests/test_native_set_binding.py']['sha256'] = '0' * 64
        with self.assertRaises(ValueError):
            rich_merge_correction(self.bundle, self.anchors, bad, self.gates)

    def test_baseline_history_cannot_be_replaced(self):
        item = self.anchors['baseline_binding']
        original = linked(self.bundle, item)
        for change in (
            lambda row: row.update(complete347_engine_seconds=12.5),
            lambda row: row['packet']['manifest.json'].update(sha256='0' * 64),
            lambda row: row['observations'].pop(next(iter(row['observations']))),
        ):
            bad = copy.deepcopy(original)
            change(bad)
            with self.assertRaises(ValueError):
                baseline_binding(ChangedRecords(self.bundle, {item['path']: raw(bad)}), self.anchors, self.external)

    def test_driver_review_requires_all_execution_boundaries(self):
        bad = linked(self.bundle, self.anchors['driver_review'])
        bad['reviewed80_boundaries'].pop('clock')
        with self.assertRaises(ValueError):
            driver_review(self.bundle, bad)

    def test_supplemental_coverage_clock_and_completion(self):
        directory = self.root + '/' + self.anchors['supplemental_cohorts']['candidate']['name']
        path = directory + '/native-observations.json'
        original = strict_json(self.bundle.raw(path))
        baseline = strict_json(self.bundle.raw(self.root + '/' +
            self.anchors['supplemental_cohorts']['accepted']['name'] + '/replay.json'))['engine_execution_seconds']
        for change in (
            lambda row: row['observations'].pop(),
            lambda row: row.update(complete=False),
            lambda row: row['observations'][0]['clocks'].update(engine_execution_seconds=1000),
        ):
            bad = copy.deepcopy(original)
            change(bad)
            with self.assertRaises(ValueError):
                workflow_replay(ChangedRecords(self.bundle, {path: raw(bad)}), directory,
                                self.external, baseline_seconds=baseline)

    def test_supplemental_reference_is_independently_regenerated(self):
        directory = self.root + '/' + self.anchors['supplemental_cohorts']['accepted']['name']
        inputs = strict_json(self.bundle.raw(directory + '/inputs.json'))
        path = inputs['cases'][0]['reference']
        result = strict_json(gzip.decompress(self.bundle.raw(path)))
        result['rows'][0]['id'] = -100
        with self.assertRaises((ValueError, ResultMismatch)):
            workflow_replay(ChangedRecords(self.bundle, {path: gzip.compress(raw(result), mtime=0)}),
                            directory, self.external)

    def test_broad_harness_and_raw_clocks_are_unchanged(self):
        cohort = self.anchors['cohort']
        inputs, protocol = [linked(self.bundle, cohort[key]) for key in ('inputs', 'protocol')]
        bad = copy.deepcopy(protocol)
        bad['native_sql_entrypoints'].pop()
        with self.assertRaises(ValueError):
            harness_continuity(self.bundle, inputs, bad)
        directory = self.root + '/' + cohort['name']
        path = directory + '/native-observations.json'
        bad = strict_json(self.bundle.raw(path))
        bad['observations'][0]['clocks']['engine_execution_seconds'] += 1
        view = ChangedRecords(self.bundle, {path: raw(bad)})
        with self.assertRaises(ValueError):
            replay_cohort(view.raw, directory)

    def test_no_cohort_gain_can_rescue_the_other_cohort(self):
        for broad, supplemental in ((False, False), (False, True), (True, False), (True, True)):
            actual = combined_disposition(dict(benefit_gate_passed=broad), dict(benefit_gate_passed=supplemental))
            self.assertEqual(actual['accepted_baseline_advanced'], broad and supplemental)
        with self.assertRaises(ValueError):
            combined_disposition(dict(benefit_gate_passed=1), dict(benefit_gate_passed=True))

    def test_metadata_reference_requires_exact_shared_repair_only(self):
        proof = linked(self.bundle, self.anchors['metadata_reference'])
        reference = linked(self.bundle, proof['runtime_manifest'])
        original = linked(self.bundle, reference['reference_derivation'])
        for change in (
            lambda value: value['exact_allowed_changes'].pop(next(iter(value['exact_allowed_changes']))),
            lambda value: value['source_snapshot']['shardloom-core/src/lib.rs'].update(sha256='0' * 64),
            lambda value: value.update(resource_change=True),
        ):
            bad = copy.deepcopy(original)
            change(bad)
            view = ChangedRecords(self.bundle, {reference['reference_derivation']['path']: raw(bad)})
            with self.assertRaises(ValueError):
                metadata_reference(view, self.anchors, self.runtime)
        bad = copy.deepcopy(self.runtime)
        bad['source_snapshot']['vendor/parquet/src/file/metadata/memory_reservation.rs']['sha256'] = '0' * 64
        with self.assertRaises(ValueError):
            metadata_reference(self.bundle, self.anchors, bad)

    def test_revised_protocol_cannot_raise_original_ceiling_or_rewrite_history(self):
        role = self.anchors['supplemental_cohorts']['accepted']
        original = linked(self.bundle, role['protocol'])
        for change in (
            lambda value: next(case for case in value['cases'] if case['family'] == 'B7')['resources'].update(memory_bytes=64 << 20),
            lambda value: value.update(historical_performance_executions=0),
            lambda value: value['frozen_files'].pop(0),
        ):
            bad = copy.deepcopy(original)
            change(bad)
            with self.assertRaises(ValueError):
                revised_protocol(self.bundle, bad)

    def test_failed_first_pass_cannot_become_a_complete_timing(self):
        directory = self.root + '/performance-batch-accepted-r1'
        path = directory + '/native-observations.json'
        original = strict_json(self.bundle.raw(path))
        for change in (
            lambda value: value.update(engine_execution_seconds=0.1),
            lambda value: value['observations'].pop(),
            lambda value: value.update(status='passed'),
        ):
            bad = copy.deepcopy(original)
            change(bad)
            with self.assertRaises(ValueError):
                workflow_replay(ChangedRecords(self.bundle, {path: raw(bad)}), directory,
                                self.external, failed_history=True)

    def test_interrupted_reference_cannot_supply_a_complete_timing(self):
        directory = self.root + '/performance-batch-reference-r1'
        path = directory + '/native-observations.json'
        original = strict_json(self.bundle.raw(path))
        for change in (
            lambda value: value.update(engine_execution_seconds=0.1),
            lambda value: value.update(complete=True, posthash_verified=True),
            lambda value: value['observations'].clear(),
            lambda value: value.update(error='unrelated failure'),
        ):
            bad = copy.deepcopy(original)
            change(bad)
            with self.assertRaises(ValueError):
                workflow_replay(ChangedRecords(self.bundle, {path: raw(bad)}), directory,
                                self.external, interrupted_history=True)

    def test_harness_revision_only_removes_generated_cache_inputs(self):
        original = linked(self.bundle, self.anchors['supplemental_cohorts']['accepted']['protocol'])
        interrupted = linked(self.bundle, original['interrupted_protocol'])
        for change in (
            lambda value: value['frozen_files'].pop(next(index for index, item in enumerate(value['frozen_files'])
                if item['path'].startswith(value['harness_root'] + '/') and item['path'].endswith('.py'))),
            lambda value: next(item for item in value['frozen_files']
                if item['path'].endswith('.pyc')).update(path=value['harness_root'] + '/unreviewed.py'),
        ):
            bad = copy.deepcopy(interrupted)
            change(bad)
            view = ChangedRecords(self.bundle, {original['interrupted_protocol']['path']: raw(bad)})
            with self.assertRaises(ValueError):
                revised_protocol(view, original)

    def test_binding_repair_does_not_change_execution_or_supervision(self):
        proof = linked(self.bundle, self.anchors['driver_review'])
        protocol = linked(self.bundle, proof['original80_protocol'])
        before = self.bundle.identity(protocol['observer_driver'])
        source = next(item for item in proof['source_identities'] if Path(item['path']).name == 'batch_cohort.py')
        after = self.bundle.identity(source)
        self.assertEqual(binding_correction(before, after)['binding_conversions'], 2)
        changed = after.replace(b'plan.collect(timeout=', b'plan.collect(unrecognized_timeout=')
        self.assertNotEqual(changed, after)
        with self.assertRaises(ValueError):
            binding_correction(before, changed)


if __name__ == '__main__':
    unittest.main()
