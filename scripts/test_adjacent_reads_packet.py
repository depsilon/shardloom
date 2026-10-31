# SPDX-License-Identifier: Apache-2.0
"""Reject altered adjacent-read evidence without rerunning native queries."""
import copy
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
PACKET = ROOT / 'docs/benchmarks/evidence/native-adjacent-parquet-reads-2026-10-31'
sys.path[:0] = [str(ROOT / 'scripts'), str(ROOT / 'python/src'),
               str(ROOT / 'benchmarks/traditional_analytics')]
from adjacent_reads_cohort_replay import harness_continuity
from adjacent_reads_public_replay import public_replay, read_work
from adjacent_reads_replay import fixture_correction, native_controls, tested_source_matches, verify
from native_workflow_protocol import strict_json
from parquet_demand_completion_replay import replay as replay_cohort
from query_surface.semantics import ResultMismatch
from shared_followthrough_replay import installed_package
from shared_performance_bundle import Bundle


class AdjacentReadsPacketTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bundle = Bundle(PACKET)
        cls.anchors = strict_json((ROOT / 'scripts/adjacent_reads_anchors.json').read_bytes())
        cls.root = cls.anchors['root']
        cls.runtime = strict_json(cls.bundle.identity(cls.anchors['runtime']))
        cls.original_runtime = strict_json(cls.bundle.identity(cls.anchors['original_runtime']))
        cls.public = strict_json(cls.bundle.identity(cls.anchors['public_report']))
        cls.declarations = strict_json(cls.bundle.identity(cls.anchors['declarations']))

    def test_failed_gates_cannot_be_omitted(self):
        bad = copy.deepcopy(self.anchors)
        bad['gates'] = [row for row in bad['gates'] if row['status'] == 'passed']
        with self.assertRaises(ValueError):
            verify(self.bundle, bad)

    def test_workspace_source_must_match_built_runtime(self):
        gate = strict_json(self.bundle.raw(self.root + '/a007-adjacent-native-workspace.json'))
        tested_source_matches(gate, self.original_runtime)
        runtime = copy.deepcopy(self.original_runtime)
        name = next(name for name in gate['changed_source_inventory'] if name.endswith('.rs'))
        runtime['source_snapshot'][name]['sha256'] = '0' * 64
        with self.assertRaises(ValueError):
            tested_source_matches(gate, runtime)

    def test_fixture_correction_cannot_change_runtime_or_other_assertions(self):
        fixture_correction(self.bundle, self.anchors, self.runtime)
        bad = copy.deepcopy(self.runtime)
        bad['products']['shardloom'] = '0' * 64
        with self.assertRaises(ValueError):
            fixture_correction(self.bundle, self.anchors, bad)
        proof = strict_json(self.bundle.identity(self.anchors['fixture_correction']))
        path = proof['corrected_fixture']['path']
        digest = proof['corrected_fixture']['sha256']

        class ChangedFixture:
            def identity(inner, item):
                raw = self.bundle.identity(item)
                return raw + b'\n# unrecorded change\n' if item['path'] == path and item['sha256'] == digest else raw

        with self.assertRaises(ValueError):
            fixture_correction(ChangedFixture(), self.anchors, self.runtime)

    def test_empty_input_still_requires_encoded_column_headers(self):
        proof = strict_json(self.bundle.identity(self.anchors['fixture_correction']))
        empty = next(row for row in proof['regenerated_sources'] if row['fact_rows'] == 0)
        work = empty['read_predictions']['adjacent_keys']
        fields = dict(native_source_demand_data_read_requests=str(work['expected_requests']),
                      native_source_demand_data_read_bytes=str(work['expected_bytes']))
        read_work(work, 0, 'adjacent_keys', fields, empty['original']['bytes'])
        bad = dict(groups=[], expected_requests=0, individual_chunk_requests=0, expected_bytes=0)
        with self.assertRaises(ValueError):
            read_work(bad, 0, 'adjacent_keys', fields, empty['original']['bytes'])

    def test_native_read_and_ownership_controls_cannot_be_omitted(self):
        log = self.bundle.raw(self.root + '/a007-adjacent-native-workspace.log').decode()
        self.assertEqual(native_controls(log)['passed_controls'], 8)
        for name in native_controls(log)['names']:
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    native_controls(log.replace('test ' + name + ' ... ok', 'omitted_control', 1))

    def physical_example(self):
        for part in self.public['comparison_parts']:
            rows = strict_json(self.bundle.identity(part))
            for index, row in enumerate(rows):
                if row['format'] == 'parquet' and row['case'] == 'adjacent_read_n37_adjacent_keys':
                    return part, rows, index
        self.fail('missing independent read-work example')

    def test_public_values_completion_resources_identity_and_actual_reads_are_checked(self):
        part, original, index = self.physical_example()

        def corrupt_value(row):
            row['actual']['rows'][0]['row_id'] += 1

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
            'source': lambda row: row['report'].update(sql_declaration='SELECT 1'),
            'read_requests': lambda row: row['report']['fields'].update(native_source_demand_data_read_requests='0'),
            'read_bytes': lambda row: row['report']['fields'].update(native_source_demand_data_read_bytes='0'),
            'missing_read_prediction': lambda row: row['report'].pop('acceptance_expected_parquet_read_work'),
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

    def test_read_prediction_preserves_exact_extents_boundaries_and_totals(self):
        _, rows, index = self.physical_example()
        row = rows[index]
        original = row['report']['acceptance_expected_parquet_read_work']
        fields = row['report']['fields']
        source = next(source for source in self.public['source_fixtures']
                      if source['dataset']['fact_rows'] == 37 and source['format'] == 'parquet')
        size = source['files']['fact'][0]['bytes']
        read_work(original, 37, 'adjacent_keys', fields, size)
        for label, edit in {
            'gap': lambda work: work['groups'][0]['chunks'][1].update(
                start=work['groups'][0]['chunks'][1]['start'] + 1),
            'columns': lambda work: work['groups'][0]['chunks'][0].update(column='unselected'),
            'oversized': lambda work: work['groups'][0]['chunks'][0].update(bytes=4 << 20),
            'group': lambda work: work['groups'].pop(),
            'requests': lambda work: work.update(expected_requests=0),
            'bytes': lambda work: work.update(expected_bytes=0),
            'individual': lambda work: work.update(individual_chunk_requests=0),
        }.items():
            with self.subTest(label=label):
                bad = copy.deepcopy(original)
                edit(bad)
                with self.assertRaises(ValueError):
                    read_work(bad, 37, 'adjacent_keys', fields, size)

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

    def test_installed_package_binds_test_count_and_runtime(self):
        path = self.root + '/installed-adjacent-reads-r1'
        installed_package(self.bundle, path, self.runtime, 285)
        with self.assertRaises(ValueError):
            installed_package(self.bundle, path, self.runtime, 284)
        bad = copy.deepcopy(self.runtime)
        bad['products']['_native.abi3.so'] = '0' * 64
        with self.assertRaises(ValueError):
            installed_package(self.bundle, path, bad, 285)

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
