# SPDX-License-Identifier: Apache-2.0
"""Corrupt saved grouped-state evidence; reject it without running queries."""
import copy
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
PACKET = ROOT / 'docs/benchmarks/evidence/native-grouped-state-access-2026-10-31'
sys.path[:0] = [str(ROOT / 'scripts'), str(ROOT / 'python/src'),
               str(ROOT / 'benchmarks/traditional_analytics')]
from grouped_state_cohort_replay import harness_continuity
from grouped_state_mechanism_replay import assembly, work
from grouped_state_public_replay import public_parts, public_replay
from grouped_state_replay import tested_source_matches, verify
from native_workflow_protocol import strict_json
from parquet_demand_completion_replay import replay as replay_cohort
from query_surface.semantics import ResultMismatch
from shared_followthrough_replay import installed_package
from shared_performance_bundle import Bundle


class GroupedStatePacketTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bundle = Bundle(PACKET)
        cls.anchors = strict_json((ROOT / 'scripts/grouped_state_anchors.json').read_bytes())
        cls.root = cls.anchors['root']
        cls.runtime = strict_json(cls.bundle.identity(cls.anchors['runtime']))
        cls.public = strict_json(cls.bundle.identity(cls.anchors['public_report']))
        cls.declarations = strict_json(cls.bundle.identity(cls.anchors['declarations']))

    def test_failed_gates_cannot_be_omitted(self):
        bad = copy.deepcopy(self.anchors)
        bad['gates'] = [row for row in bad['gates'] if row['status'] == 'passed']
        with self.assertRaises(ValueError):
            verify(self.bundle, bad)

    def test_workspace_source_must_match_built_runtime(self):
        gate = strict_json(self.bundle.raw(self.root + '/s011-grouped-state-native-workspace.json'))
        tested_source_matches(gate, self.runtime)
        runtime = copy.deepcopy(self.runtime)
        name = next(name for name in gate['changed_source_inventory'] if name.endswith('.rs'))
        runtime['source_snapshot'][name]['sha256'] = '0' * 64
        with self.assertRaises(ValueError):
            tested_source_matches(gate, runtime)

    def test_binding_and_headroom_observations_cannot_be_forged(self):
        log = self.bundle.raw(self.root + '/s012-grouped-state-mechanism.log').decode()
        self.assertEqual(work(log)['binding_controls'], 42)
        for label, needle, replacement in (
            ('binding', '"contiguous_binds":3', '"contiguous_binds":2'),
            ('headroom', '"admitted_batches":1', '"admitted_batches":0'),
            ('missing_control', 'grouped_state_binding_work=', 'omitted_binding_work='),
        ):
            with self.subTest(label=label):
                self.assertIn(needle, log)
                with self.assertRaises(ValueError):
                    work(log.replace(needle, replacement, 1))

    def test_public_values_schema_completion_resources_and_identity_are_checked(self):
        part = self.public['comparison_parts'][0]
        original = strict_json(self.bundle.identity(part))
        index = next(i for i, row in enumerate(original) if row['rows'] and row['format'] == 'parquet')

        def corrupt_value(row):
            values = row['actual']['rows'][0]
            name = next(key for key, value in values.items() if type(value) is int)
            values[name] += 1

        edits = {
            'value': corrupt_value,
            'schema': lambda row: row['actual']['schema'][0].update(name='wrong_column'),
            'completion': lambda row: row['report'].update(complete_consumption=False),
            'ownership': lambda row: row['report']['fields'].update(native_input_output_ownership_detached='false'),
            'fallback': lambda row: row['report']['fields'].update(external_engine_invoked='true'),
            'memory': lambda row: row['report']['fields'].update(execution_resource_admitted_memory_bytes='1'),
            'parallelism': lambda row: row['report']['fields'].update(execution_resource_declared_max_parallelism='3'),
            'case': lambda row: row.update(case='unrecorded_case'),
            'declaration': lambda row: row['report']['public_declaration'].update(op='wrong_operator'),
        }
        for label, edit in edits.items():
            with self.subTest(label=label):
                rows = copy.deepcopy(original)
                edit(rows[index])
                data = json.dumps(rows).encode()

                class ChangedPart:
                    def identity(inner, item):
                        return data if item['path'] == part['path'] else self.bundle.identity(item)

                with self.assertRaises((ValueError, ResultMismatch)):
                    public_replay(ChangedPart(), self.public, self.declarations)

    def test_pre_execution_references_and_part_coverage_are_required(self):
        bad = copy.deepcopy(self.declarations)
        bad['sizes'][0]['references'][0]['expected_and_contract_sha256'] = '0' * 64
        with self.assertRaises(ValueError):
            public_replay(self.bundle, self.public, bad)
        for kind in ('oversized', 'duplicate', 'omitted'):
            with self.subTest(kind=kind):
                bad = copy.deepcopy(self.public)
                if kind == 'oversized':
                    bad['comparison_parts'][0]['bytes'] = (8 << 20) + 1
                elif kind == 'duplicate':
                    bad['comparison_parts'].append(bad['comparison_parts'][0])
                else:
                    bad['comparison_parts'].pop()
                with self.assertRaises(ValueError):
                    public_replay(self.bundle, bad, self.declarations)

    def test_compiled_inspection_must_bind_actual_runtime_and_preserved_checks(self):
        manifest = strict_json(self.bundle.identity(self.anchors['assembly_manifest']))
        review = strict_json(self.bundle.identity(self.anchors['assembly_review']))
        assembly(self.bundle, manifest, review, self.runtime)
        for key in ('contiguous_page_lookup_removed', 'paged_path_preserved',
                    'paired_count_overflow_checks_removed_after_admission',
                    'element_bounds_checks_preserved', 'ordinary_count_overflow_checks_preserved'):
            bad = copy.deepcopy(review)
            bad[key] = False
            with self.assertRaises(ValueError):
                assembly(self.bundle, manifest, bad, self.runtime)
        bad = copy.deepcopy(manifest)
        bad['binary_sha256'] = '0' * 64
        with self.assertRaises(ValueError):
            assembly(self.bundle, bad, review, self.runtime)

    def test_installed_package_binds_test_count_and_runtime(self):
        path = self.root + '/installed-grouped-state-r1'
        installed_package(self.bundle, path, self.runtime, 284)
        with self.assertRaises(ValueError):
            installed_package(self.bundle, path, self.runtime, 283)
        bad = copy.deepcopy(self.runtime)
        bad['products']['_native.abi3.so'] = '0' * 64
        with self.assertRaises(ValueError):
            installed_package(self.bundle, path, bad, 284)

    def test_harness_and_supervision_remain_original(self):
        cohort = self.anchors['cohort']
        inputs, protocol = (strict_json(self.bundle.identity(cohort[key])) for key in ('inputs', 'protocol'))
        harness_continuity(self.bundle, inputs, protocol, self.runtime, self.declarations)
        for edit in (
            lambda row: row['frozen_application_harness_compatibility'].pop(),
            lambda row: row['frozen_application_harness_compatibility'][0].update(execution_change=True),
            lambda row: row['frozen_reporting_harness_compatibility'][0].update(supervision_change=True),
            lambda row: row['native_sql_entrypoints'].pop(),
        ):
            bad = copy.deepcopy(protocol)
            edit(bad)
            with self.assertRaises(ValueError):
                harness_continuity(self.bundle, inputs, bad, self.runtime, self.declarations)

    def test_cohort_coverage_and_engine_clock_match_raw_results(self):
        root = self.root + '/' + self.anchors['cohort']['name']
        path = root + '/native-observations.json'
        original = strict_json(self.bundle.raw(path))
        for kind in ('coverage', 'clock'):
            bad = copy.deepcopy(original)
            if kind == 'coverage':
                bad['observations'].pop()
            else:
                bad['observations'][0]['clocks']['engine_execution_seconds'] = 0
            data = json.dumps(bad).encode()
            with self.assertRaises(ValueError):
                replay_cohort(lambda name: data if name == path else self.bundle.raw(name), root)


if __name__ == '__main__':
    unittest.main()
