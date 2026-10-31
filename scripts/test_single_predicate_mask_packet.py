# SPDX-License-Identifier: Apache-2.0
"""Adversarial saved-result controls; no execution engine is loaded."""
import copy
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
PACKET = ROOT / 'docs/benchmarks/evidence/native-single-predicate-masks-2026-10-30'
sys.path[:0] = [str(ROOT / 'scripts'), str(ROOT / 'python/src'),
               str(ROOT / 'benchmarks/traditional_analytics')]
from native_workflow_protocol import strict_json
from parquet_demand_completion_replay import replay as replay_cohort
from query_surface.semantics import ResultMismatch
from shared_performance_bundle import Bundle
from single_predicate_mask_cohort_replay import harness_continuity, rejection
from single_predicate_mask_replay import (
    fixture_correction, installed_package, mechanism_replay, operand_order,
    public_parts, public_replay, source_matches, verify,
)


class FakeBundle:
    def __init__(self, rows=None):
        self.rows = [] if rows is None else rows

    def identity(self, item):
        return json.dumps(self.rows).encode()


class SinglePredicateMaskTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bundle = Bundle(PACKET)
        cls.anchors = strict_json((ROOT / 'scripts/single_predicate_mask_anchors.json').read_bytes())
        cls.root = cls.anchors['root']
        cls.runtimes = {Path(item['path']).parent.name: strict_json(cls.bundle.identity(item))
                        for item in cls.anchors['runtimes']}

    def test_failed_gates_are_anchor_bound(self):
        bad = copy.deepcopy(self.anchors)
        bad['gates'] = [item for item in bad['gates'] if item['status'] == 'passed']
        with self.assertRaises(ValueError):
            verify(self.bundle, bad)

    def test_installed_package_count_runtime_and_native_digest_are_bound(self):
        runtime = self.runtimes['single-predicate-mask-release-r2']
        path = self.root + '/installed-single-predicate-mask-r1'
        self.assertEqual(installed_package(self.bundle, path, runtime, 284)['tests'], 284)
        for count in (283,):
            with self.assertRaises(ValueError):
                installed_package(self.bundle, path, runtime, count)
        bad = copy.deepcopy(runtime)
        bad['products']['shardloom'] = '0' * 64
        with self.assertRaises(ValueError):
            installed_package(self.bundle, path, bad, 284)

    def test_workspace_gate_is_bound_to_r1_source(self):
        gate = strict_json(self.bundle.raw(self.root + '/m010-mask-native-workspace.json'))
        runtime = copy.deepcopy(self.runtimes['single-predicate-mask-release-r1'])
        source_matches(gate, runtime)
        key = next(name for name in gate['changed_source_inventory'] if name.endswith('.rs'))
        runtime['source_snapshot'][key]['sha256'] = '0' * 64
        with self.assertRaises(ValueError):
            source_matches(gate, runtime)

    def test_fixture_correction_cannot_exempt_runtime_changes(self):
        supplements = {Path(i['path']).name: i for i in self.anchors['supplements']}
        proof = self.bundle.identity(supplements['single-mask-public-fixture-correction.json'])
        proof = strict_json(proof)
        before = copy.deepcopy(self.runtimes['single-predicate-mask-release-r1'])
        after = copy.deepcopy(self.runtimes['single-predicate-mask-release-r2'])
        fixture_correction(self.bundle, proof, before, after)
        key = next(name for name in after['source_snapshot'] if name.endswith('.rs'))
        after['source_snapshot'][key]['sha256'] = '0' * 64
        with self.assertRaises(ValueError):
            fixture_correction(self.bundle, proof, before, after)
        after = copy.deepcopy(self.runtimes['single-predicate-mask-release-r2'])
        after['products']['shardloom'] = '0' * 64
        with self.assertRaises(ValueError):
            fixture_correction(self.bundle, proof, before, after)

    def test_reversed_operand_requires_executed_equivalent_declaration(self):
        report = next(item for item in self.anchors['public_reports']
                      if Path(item['path']).name == 'single-predicate-mask-public-r2.json')
        public = strict_json(self.bundle.identity(report))
        rows = list(public_parts(self.bundle, public))
        row = next(row for row in rows if row['case'].endswith('_mask_reversed') and row['surface'] != 'typed_relation')
        operand_order(row)
        bad = copy.deepcopy(row)
        bad['report']['fixture_operand_order']['executed'] = 'literal_left'
        with self.assertRaises(ValueError):
            operand_order(bad)
        bad = copy.deepcopy(row)
        bad['report']['public_declaration']['inputs'][0]['args'][0]['op'] = 'ge'
        with self.assertRaises(ValueError):
            operand_order(bad)

    def test_public_part_sizes_and_duplicate_paths_are_rejected(self):
        value = dict(comparison_part_max_bytes=8 << 20, comparison_parts_posthash_verified=True,
                     comparison_parts=[dict(path='/one', bytes=(8 << 20) + 1, comparisons=1, rows=1,
                                            sha256='0' * 64)])
        with self.assertRaises(ValueError):
            list(public_parts(FakeBundle([{'rows': 1}]), value))
        value['comparison_parts'] = [dict(path='/one', bytes=1, comparisons=1, rows=1, sha256='0' * 64)] * 2
        with self.assertRaises(ValueError):
            list(public_parts(FakeBundle([{'rows': 1}]), value))

    def test_mechanism_replay_rejects_mutated_mask_count_in_saved_log(self):
        mechanism_replay(self.bundle, self.root)
        original = self.bundle.raw(self.root + '/m006-mask-focused.log')
        text = original.decode()
        needle = '"predicate_mask_rows":65'
        if needle not in text:
            self.fail('expected serialized mask-row observation was absent')
        class DelegatedBundle:
            def raw(inner, path, *args, **kwargs):
                data = self.bundle.raw(path, *args, **kwargs)
                return data.replace(needle.encode(), b'"predicate_mask_rows":64', 1) if path.endswith('/m006-mask-focused.log') else data
        with self.assertRaises(ValueError):
            mechanism_replay(DelegatedBundle(), self.root)

    def test_complete_public_values_completion_and_ownership_are_required(self):
        item = next(item for item in self.anchors['public_reports']
                    if item['path'].endswith('/single-predicate-mask-public-r2.json'))
        public = strict_json(self.bundle.identity(item))
        part = public['comparison_parts'][0]
        original = strict_json(self.bundle.identity(part))
        index = next(i for i, row in enumerate(original)
                     if row['rows'] and row['format'] == 'parquet')
        def corrupt_value(row):
            value = row['actual']['rows'][0]
            key = next(key for key, item in value.items() if type(item) is int)
            value[key] += 1
        mutations = {
            'value': corrupt_value,
            'completion': lambda row: row['report'].update(complete_consumption=False),
            'ownership': lambda row: row['report']['fields'].update(native_input_output_ownership_detached='false'),
            'fallback': lambda row: row['report']['fields'].update(external_engine_invoked='true'),
            'omitted_comparison': lambda row: row.update(case='unrecorded_case'),
        }
        for label, edit in mutations.items():
            with self.subTest(label=label):
                rows = copy.deepcopy(original)
                edit(rows[index])
                changed = json.dumps(rows).encode()
                class ChangedPart:
                    def identity(inner, value):
                        return changed if value['path'] == part['path'] else self.bundle.identity(value)
                with self.assertRaises((ValueError, ResultMismatch)):
                    public_replay(ChangedPart(), public)

    def test_harness_inventory_and_exceptions_cannot_change_execution(self):
        cohort = self.anchors['cohort']
        inputs, protocol = (strict_json(self.bundle.identity(cohort[key])) for key in ('inputs', 'protocol'))
        declarations = next(item for item in self.anchors['supplements']
                            if item['path'].endswith('/single-mask-declarations-r2.json'))
        declaration = strict_json(self.bundle.identity(declarations))
        runtime = self.runtimes['single-predicate-mask-release-r2']
        harness_continuity(self.bundle, inputs, protocol, runtime, declaration)
        edits = {
            'missing_exception': lambda value: value['frozen_application_harness_compatibility'].pop(),
            'execution_change': lambda value: value['frozen_application_harness_compatibility'][0].update(execution_change=True),
            'supervision_change': lambda value: value['frozen_reporting_harness_compatibility'][0].update(supervision_change=True),
            'native_entrypoint': lambda value: value['native_sql_entrypoints'].pop(),
        }
        for label, edit in edits.items():
            with self.subTest(label=label):
                bad = copy.deepcopy(protocol)
                edit(bad)
                with self.assertRaises(ValueError):
                    harness_continuity(self.bundle, inputs, bad, runtime, declaration)
        bad = copy.deepcopy(inputs)
        bad['frozen_files'] = [row for row in bad['frozen_files']
                               if row['path'] != protocol['native_sql_entrypoints'][0]['path']]
        with self.assertRaises(ValueError):
            harness_continuity(self.bundle, bad, protocol, runtime, declaration)

    def test_query_coverage_and_engine_clocks_remain_bound_to_raw_observations(self):
        root = self.root + '/' + self.anchors['cohort']['name']
        path = root + '/native-observations.json'
        original = strict_json(self.bundle.raw(path))
        for label in ('coverage', 'clock'):
            with self.subTest(label=label):
                value = copy.deepcopy(original)
                if label == 'coverage':
                    value['observations'].pop()
                else:
                    value['observations'][0]['clocks']['engine_execution_seconds'] = 0
                raw = lambda name: json.dumps(value).encode() if name == path else self.bundle.raw(name)
                with self.assertRaisesRegex(ValueError, 'case coverage|clock'):
                    replay_cohort(raw, root)

    def test_failed_performance_cannot_advance_baseline_or_keep_candidate_source(self):
        item = next(item for item in self.anchors['supplements']
                    if item['path'].endswith('/single-mask-rejection.json'))
        proof = strict_json(self.bundle.identity(item))
        runtime = self.runtimes['single-predicate-mask-release-r2']
        accepted = strict_json(self.bundle.identity(self.anchors['accepted_application_runtime']))
        comparison = strict_json(self.bundle.raw(self.root + '/' + self.anchors['cohort']['name'] + '/disposition.json'))
        rejection(self.bundle, proof, runtime, accepted, comparison)
        edits = {
            'advance_baseline': lambda value: value.update(accepted_baseline_advanced=True),
            'retain_candidate': lambda value: value.update(retained_runtime='single-predicate-mask-release-r2'),
            'omit_source': lambda value: value['candidate_source_retained'].pop(),
            'change_clock': lambda value: value.update(candidate_engine_seconds=1),
        }
        for label, edit in edits.items():
            with self.subTest(label=label):
                bad = copy.deepcopy(proof)
                edit(bad)
                with self.assertRaises(ValueError):
                    rejection(self.bundle, bad, runtime, accepted, comparison)


if __name__ == '__main__':
    unittest.main()
