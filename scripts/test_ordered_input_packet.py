# SPDX-License-Identifier: Apache-2.0
"""Reject altered ordering evidence without running native queries."""
import copy
import gzip
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
PACKET = ROOT / 'docs/benchmarks/evidence/native-ordered-inputs-2026-10-31'
sys.path[:0] = [str(ROOT / 'scripts'), str(ROOT / 'python/src'),
               str(ROOT / 'benchmarks/traditional_analytics')]
from native_workflow_protocol import strict_json
from ordered_input_cohort_replay import harness_continuity
from ordered_input_public_replay import declarations, public_replay, raw
from ordered_input_replay import (
    assembly_replay, combined_disposition, fixture_correction, gates_replay, native_controls,
    recovery_correction, tested_source_matches,
)
from ordered_input_workflow_replay import workflow_replay
from parquet_demand_completion_replay import replay as replay_cohort
from query_surface.replay import sha
from query_surface.semantics import ResultMismatch
from shared_followthrough_replay import installed_package
from shared_performance_bundle import Bundle


class ChangedRecords:
    """Bypass transport hashes to test semantic rejection of rehashed evidence."""
    def __init__(self, bundle, replacements):
        self.bundle, self.replacements = bundle, replacements

    def raw(self, path, digest=None):
        return self.replacements[path] if path in self.replacements else self.bundle.raw(path, digest)

    def identity(self, item):
        return self.replacements[item['path']] if item['path'] in self.replacements else self.bundle.identity(item)


class OrderedInputPacketTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bundle = Bundle(PACKET)
        cls.anchors = strict_json((ROOT / 'scripts/ordered_input_anchors.json').read_bytes())
        cls.root = cls.anchors['root']
        cls.runtime = strict_json(cls.bundle.identity(cls.anchors['runtime']))
        cls.public = strict_json(cls.bundle.identity(cls.anchors['public_report']))
        cls.declaration = strict_json(cls.bundle.identity(cls.anchors['declaration']))
        cls.references = declarations(cls.declaration)
        cls.external = {(row['path'], row['sha256'], row['bytes']) for row in cls.anchors['external_artifacts']}

    def test_failed_validation_attempts_cannot_be_removed(self):
        bad = copy.deepcopy(self.anchors)
        bad['gates'] = [row for row in bad['gates'] if row['status'] == 'passed']
        with self.assertRaises(ValueError):
            gates_replay(self.bundle, bad)

    def test_workspace_sources_must_match_built_runtime(self):
        gate = strict_json(self.bundle.raw(self.root + '/o018-ordered-input-native-workspace.json'))
        tested_source_matches(gate, self.runtime)
        bad = copy.deepcopy(self.runtime)
        name = next(name for name in gate['changed_source_inventory'] if name.endswith('.rs'))
        bad['source_snapshot'][name]['sha256'] = '0' * 64
        with self.assertRaises(ValueError):
            tested_source_matches(gate, bad)

    def test_every_native_ordering_and_validation_control_is_required(self):
        log = self.bundle.raw(self.root + '/o018-ordered-input-native-workspace.log').decode()
        self.assertEqual(native_controls(log)['passed_controls'], 13)
        for name in native_controls(log)['names']:
            with self.subTest(name=name), self.assertRaises(ValueError):
                native_controls(log.replace('test ' + name + ' ... ok', 'omitted_control', 1))

    def test_public_references_must_be_independent_and_predeclared(self):
        for change in (
            lambda value: value.update(native_queries_before_declaration=1),
            lambda value: value['cases'][0].update(reference_sha256='0' * 64),
            lambda value: value['cases'].pop(),
        ):
            bad = copy.deepcopy(self.declaration)
            change(bad)
            with self.assertRaises(ValueError):
                declarations(bad)

    def test_recovery_fix_preserves_failure_and_only_changes_pipe_lifetime(self):
        proof = strict_json(self.bundle.identity(self.anchors['recovery_correction']))
        log = self.bundle.raw(self.root + '/o018-ordered-input-native-workspace.log').decode()
        recovery_correction(self.bundle, self.anchors, self.runtime, log)
        for change in (
            lambda row: row.update(runtime_source_changed=True),
            lambda row: row['corrected_sources'].pop(),
        ):
            bad = copy.deepcopy(proof)
            change(bad)
            view = ChangedRecords(self.bundle, {self.anchors['recovery_correction']['path']: raw(bad)})
            with self.assertRaises(ValueError):
                recovery_correction(view, self.anchors, self.runtime, log)
        source = proof['corrected_sources'][0]['after']
        view = ChangedRecords(self.bundle, {source['path']: self.bundle.identity(source) + b'\n// additional change\n'})
        with self.assertRaises(ValueError):
            recovery_correction(view, self.anchors, self.runtime, log)

    def test_public_values_schema_completion_ownership_resources_and_fallback(self):
        index, part = next((index, row) for index, row in enumerate(self.public['parts'])
                          if row['label'] == 'sort_ascending_33' and row['format'] == 'parquet'
                          and row['surface'] == 'dataframe')
        original = strict_json(gzip.decompress(self.bundle.identity(part)))

        def value(row):
            row['actual']['rows'][0]['id'] += 1

        changes = {
            'value': value,
            'schema': lambda row: row['actual']['schema'][0].update(name='wrong_column'),
            'completion': lambda row: row['report'].update(complete_consumption=False),
            'release': lambda row: row['report'].update(source_session_released_before_export=False),
            'fallback': lambda row: row['report']['fields'].update(external_engine_invoked='true'),
            'memory': lambda row: row['report']['fields'].update(execution_resource_admitted_memory_bytes='1'),
            'parallelism': lambda row: row['report']['fields'].update(execution_resource_declared_max_parallelism='3'),
            'query': lambda row: row['report'].update(sql_declaration='SELECT 1'),
        }
        # References are regenerated once above. These controls alter only the
        # result; separate tests exercise declaration regeneration itself.
        with patch('ordered_input_public_replay.declarations', return_value=self.references):
            for label, change in changes.items():
                with self.subTest(label=label):
                    row = copy.deepcopy(original)
                    change(row)
                    data = raw(row)
                    compressed = gzip.compress(data, mtime=0)
                    bad = copy.deepcopy(self.public)
                    bad['parts'][index].update(bytes=len(compressed), sha256=sha(compressed),
                                              raw_bytes=len(data), raw_sha256=sha(data))
                    view = ChangedRecords(self.bundle, {part['path']: compressed})
                    with self.assertRaises((ValueError, ResultMismatch)):
                        public_replay(view, bad, self.declaration)

    def test_public_fixture_repair_preserves_references_failure_and_runtime(self):
        fixture_correction(self.bundle, self.anchors, self.runtime)
        proof = strict_json(self.bundle.identity(self.anchors['fixture_correction']))
        for change in (
            lambda row: row.update(runtime_source_changed=True),
            lambda row: row['changed_sources'].pop(),
        ):
            bad = copy.deepcopy(proof)
            change(bad)
            view = ChangedRecords(self.bundle, {self.anchors['fixture_correction']['path']: raw(bad)})
            with self.assertRaises(ValueError):
                fixture_correction(view, self.anchors, self.runtime)
        bad = copy.deepcopy(self.declaration)
        bad['cases'][0]['reference_sha256'] = '0' * 64
        view = ChangedRecords(self.bundle, {self.anchors['declaration']['path']: raw(bad)})
        with self.assertRaises(ValueError):
            fixture_correction(view, self.anchors, self.runtime)
        bad = copy.deepcopy(self.runtime)
        bad['products']['shardloom'] = '0' * 64
        with self.assertRaises(ValueError):
            fixture_correction(self.bundle, self.anchors, bad)

    def test_public_grid_requires_every_bounded_complete_part(self):
        with patch('ordered_input_public_replay.declarations', return_value=self.references):
            for kind in ('oversized', 'duplicate', 'omitted', 'source'):
                with self.subTest(kind=kind):
                    bad = copy.deepcopy(self.public)
                    if kind == 'oversized':
                        bad['parts'][0]['bytes'] = (8 << 20) + 1
                    elif kind == 'duplicate':
                        bad['parts'][1] = bad['parts'][0]
                    elif kind == 'source':
                        bad['sources'].pop()
                    else:
                        bad['parts'].pop()
                    with self.assertRaises(ValueError):
                        public_replay(self.bundle, bad, self.declaration)

    def test_installed_package_binds_tests_and_exact_runtime(self):
        path = self.root + '/installed-ordered-input-r1'
        installed_package(self.bundle, path, self.runtime, 285)
        with self.assertRaises(ValueError):
            installed_package(self.bundle, path, self.runtime, 284)
        bad = copy.deepcopy(self.runtime)
        bad['products']['_native.abi3.so'] = '0' * 64
        with self.assertRaises(ValueError):
            installed_package(self.bundle, path, bad, 285)

    def test_supplemental_coverage_engine_clock_and_resources(self):
        root = self.root + '/ordered-input-candidate-r1'
        path = root + '/native-observations.json'
        original = strict_json(self.bundle.raw(path))
        for kind in ('coverage', 'clock', 'complete'):
            bad = copy.deepcopy(original)
            if kind == 'coverage':
                bad['observations'].pop()
            elif kind == 'clock':
                bad['observations'][0]['clocks']['engine_execution_seconds'] += 1
            else:
                bad['complete'] = False
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                workflow_replay(ChangedRecords(self.bundle, {path: raw(bad)}), root, self.external)
        path = root + '/inputs.json'
        bad = strict_json(self.bundle.raw(path))
        bad['resources']['memory_bytes'] = 1
        with self.assertRaises(ValueError):
            workflow_replay(ChangedRecords(self.bundle, {path: raw(bad)}), root, self.external)

    def test_supplemental_saved_reference_is_regenerated(self):
        root = self.root + '/ordered-input-candidate-r1'
        inputs = strict_json(self.bundle.raw(root + '/inputs.json'))
        case = next(row for row in inputs['cases'] if row['case_id'] == 'sort_ascending_33')
        reference = strict_json(gzip.decompress(self.bundle.raw(case['reference'])))
        reference['rows'][0]['id'] += 1
        with self.assertRaises((ValueError, ResultMismatch)):
            workflow_replay(ChangedRecords(self.bundle, {
                case['reference']: gzip.compress(raw(reference), mtime=0)}), root, self.external)

    def test_neither_complete_cohort_can_rescue_a_loss_in_the_other(self):
        for broad, supplemental in ((False, False), (False, True), (True, False)):
            result = combined_disposition(dict(benefit_gate_passed=broad), dict(benefit_gate_passed=supplemental))
            self.assertFalse(result['accepted_baseline_advanced'])
            self.assertEqual(result['disposition'], 'drop_one_or_both_cohorts_no_gain')
        self.assertTrue(combined_disposition(dict(benefit_gate_passed=True),
                                            dict(benefit_gate_passed=True))['accepted_baseline_advanced'])

    def test_optimized_code_and_harness_bind_exact_mechanism(self):
        bad = copy.deepcopy(self.runtime)
        bad['products']['_native.abi3.so'] = '0' * 64
        with self.assertRaises(ValueError):
            assembly_replay(self.bundle, self.anchors, bad)
        cohort = self.anchors['cohort']
        inputs, protocol = (strict_json(self.bundle.identity(cohort[key])) for key in ('inputs', 'protocol'))
        harness_continuity(self.bundle, inputs, protocol, self.runtime)
        for change in (
            lambda row: row['frozen_application_harness_compatibility'].pop(),
            lambda row: row['frozen_reporting_harness_compatibility'][0].update(supervision_change=True),
            lambda row: row['native_sql_entrypoints'].pop(),
            lambda row: row.update(supplemental_gate='not_required'),
        ):
            bad = copy.deepcopy(protocol)
            change(bad)
            with self.assertRaises(ValueError):
                harness_continuity(self.bundle, inputs, bad, self.runtime)

    def test_broad_coverage_and_clock_match_complete_raw_results(self):
        root = self.root + '/' + self.anchors['cohort']['name']
        path = root + '/native-observations.json'
        original = strict_json(self.bundle.raw(path))
        for kind in ('coverage', 'clock'):
            bad = copy.deepcopy(original)
            if kind == 'coverage':
                bad['observations'].pop()
            else:
                bad['observations'][0]['clocks']['engine_execution_seconds'] += 1
            data = raw(bad)
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                replay_cohort(lambda name: data if name == path else self.bundle.raw(name), root)


if __name__ == '__main__':
    unittest.main()
