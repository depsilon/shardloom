"""Reject incomplete, substituted and misrepresented corrected-matrix evidence."""
import ast
import copy
import gzip
import importlib.util
import json
import lzma
import os
from pathlib import Path
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('matrix_replay', HERE / 'verify_matrix_packet_951.py')
matrix = importlib.util.module_from_spec(spec)
spec.loader.exec_module(matrix)
ROOT = Path(os.environ.get('SHARDLOOM_MATRIX_PACKET', str(HERE)))
REPO = Path(os.environ.get('SHARDLOOM_CORRECTNESS_REPOSITORY', str(HERE.parents[3])))


def set_field(key, value):
    return lambda record: record.__setitem__(key, value)


class MatrixPacketTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.helpers = matrix.helpers(REPO)
        cls.correction = cls.helpers[2]
        cls.manifest = cls.correction.strict_json((ROOT / 'matrix-manifest.json').read_bytes())
        raw = (ROOT / 'matrix-records.json.xz').read_bytes()
        assert matrix.digest(raw) == cls.manifest['archive']['sha256']
        cls.packet = cls.correction.strict_json(lzma.decompress(raw))
        cls.evidence = cls.helpers[0].Evidence(cls.packet)
        cls.protocol = cls.evidence.read(cls.manifest['protocol']['path'])
        cls.accepted = cls.evidence.read(cls.manifest['acceptance']['path'])
        cls.first_report = cls.accepted['cohorts'][0]['report']['path']
        cls.report = cls.evidence.read(cls.first_report)
        cls.native_index = next(i for i, row in enumerate(cls.report['records'])
                                if row['engine'] == 'shardloom')

    def verify(self, packet=None, *, check_current_runtime=False):
        with patch.object(matrix, 'helpers', return_value=self.helpers):
            return matrix.replay(REPO, self.manifest, self.packet if packet is None else packet,
                                 check_current_runtime=check_current_runtime)

    def reject_decoded_changes(self, changes, pattern):
        # Decode mutations happen after byte validation, so each case exercises
        # the semantic verifier rather than merely failing the member hash.
        selected = []
        for path, action in changes.items():
            raw = self.evidence.raw(path)
            selected.append((gzip.decompress(raw) if path.endswith('.gz') else raw, action))
        decoder = self.correction.strict_json
        changed = []

        def decode(raw):
            value = decoder(raw)
            for expected, action in selected:
                if raw == expected:
                    action(value)
                    changed.append(True)
            return value

        with patch.object(self.correction, 'strict_json', side_effect=decode):
            with self.assertRaisesRegex(ValueError, pattern):
                self.verify()
        self.assertTrue(changed, 'the intended evidence member was not exercised')

    def test_complete_matrix_replays(self):
        result = self.verify()
        self.assertEqual(result['complete_result_pairs'], 704)
        self.assertEqual(result['logical_schema_statements'], 368)
        self.assertEqual(result['terminal_order_statements'], 48)
        self.assertFalse(result['runtime_executed'])

    def test_current_frontend_drift_preserves_historical_replay(self):
        frontend = REPO / 'python/src/shardloom/query.py'
        read_bytes = Path.read_bytes

        def changed(path):
            raw = read_bytes(path)
            return raw + b'\n# changed current frontend\n' if path == frontend else raw

        with patch.object(Path, 'read_bytes', changed):
            result = self.verify()
            self.assertEqual(result['complete_result_pairs'], 704)
            self.assertFalse(result['current_runtime_source_checked'])
            with self.assertRaisesRegex(ValueError, 'current runtime source changed'):
                self.verify(check_current_runtime=True)

    def test_recorded_frontend_identity_is_still_required(self):
        self.reject_decoded_changes({self.manifest['protocol']['path']:
            lambda d: d['harness_source_hashes'].__setitem__('python/src/shardloom/query.py', '0' * 64)},
            'recorded harness source changed')

    def test_current_replay_harness_drift_is_rejected(self):
        workload = REPO / 'benchmarks/traditional_analytics/workloads.py'
        read_bytes = Path.read_bytes

        def changed(path):
            raw = read_bytes(path)
            return raw + b'\n# changed workload semantics\n' if path == workload else raw

        with patch.object(Path, 'read_bytes', changed):
            with self.assertRaisesRegex(ValueError, 'replay harness source changed'):
                self.verify()

    def test_maintained_query_runner_and_adapter_drift_are_rejected(self):
        read_bytes = Path.read_bytes
        for name in ('scripts/run_clickbench_query_uat.py', 'scripts/clickbench_native_sql.py'):
            with self.subTest(source=name):
                source = REPO / name

                def changed(path):
                    raw = read_bytes(path)
                    return raw + b'\n# changed maintained replay helper\n' if path == source else raw

                with patch.object(Path, 'read_bytes', changed):
                    with self.assertRaisesRegex(ValueError, 'trusted replay source changed'):
                        self.verify()

    def test_maintained_query_comparison_matches_recorded_function(self):
        name = 'scripts/run_clickbench_query_uat.py'
        recorded = self.evidence.raw(self.manifest['original_repository'] + '/' + name)

        def comparison(raw):
            functions = [node for node in ast.parse(raw).body
                         if isinstance(node, ast.FunctionDef) and node.name == 'equivalent']
            self.assertEqual(len(functions), 1)
            return ast.dump(functions[0], include_attributes=False)

        self.assertEqual(comparison(recorded), comparison((REPO / name).read_bytes()))

    def test_recorded_query_runner_identity_is_still_required(self):
        self.reject_decoded_changes({self.manifest['protocol']['path']:
            lambda d: d['harness_source_hashes'].__setitem__('scripts/run_clickbench_query_uat.py', '0' * 64)},
            'recorded harness source changed')

    def test_maintained_verifier_identity_is_required(self):
        manifest = copy.deepcopy(self.manifest)
        manifest['replay_verifier']['sha256'] = '0' * 64
        with patch.object(self, 'manifest', manifest):
            with self.assertRaisesRegex(ValueError, 'matrix replay verifier source differs'):
                self.verify()

    def test_recorded_harness_inventory_must_be_complete(self):
        self.reject_decoded_changes({self.first_report:
            lambda d: d['harness_sources'].pop('python/src/shardloom/query.py')},
            'harness source inventory is incomplete or changed')

    def test_altered_member_bytes_are_rejected(self):
        packet = dict(self.packet, members=list(self.packet['members']))
        packet['members'][0] = dict(packet['members'][0], data=packet['members'][0]['data'] + ' ')
        with self.assertRaisesRegex(ValueError, 'changed member'):
            self.verify(packet)

    def test_duplicate_member_identity_is_rejected(self):
        packet = dict(self.packet, members=self.packet['members'] + [self.packet['members'][0]])
        with self.assertRaisesRegex(ValueError, 'duplicate stored member'):
            self.verify(packet)

    def test_unearned_packet_claim_is_rejected(self):
        packet = dict(self.packet, claims={**self.packet['claims'], 'performance': True})
        with self.assertRaisesRegex(ValueError, 'changed packet scope'):
            self.verify(packet)

    def test_acceptance_scope_corruptions(self):
        cases = [
            (set_field('uninterrupted_single_run_claimed', True), 'unearned acceptance'),
            (set_field('incomplete_cohort_records_counted_as_acceptance', 1), 'unearned acceptance'),
            (set_field('guard_limits_changed', True), 'unearned acceptance'),
            (lambda d: d['claims'].__setitem__('broad_parity', True), 'unearned acceptance'),
            (set_field('lock_released', False), 'completion verification'),
            (set_field('binary_sha256', '0' * 64), 'executable differs'),
        ]
        for action, reason in cases:
            with self.subTest(reason=reason):
                self.reject_decoded_changes({self.manifest['acceptance']['path']: action}, reason)

    def test_runtime_source_and_build_corruptions(self):
        binding_path = self.protocol['runtime_binding']['path']
        binding = self.evidence.read(binding_path)
        cases = [
            (binding_path, set_field('fresh_build_for_matrix_claimed', True), 'runtime binding'),
            (binding_path, lambda d: d['source_hashes'].pop(next(iter(d['source_hashes']))), 'source coverage'),
            (binding['build_receipt']['path'], set_field('process_group_drained', False), 'build verification'),
        ]
        for path, action, reason in cases:
            with self.subTest(reason=reason):
                self.reject_decoded_changes({path: action}, reason)

    def test_interruption_and_continuation_corruptions(self):
        cases = [
            (self.accepted['matrix']['path'], set_field('status', 'passed'), 'interruption erased'),
            (self.accepted['log_retention_interruption']['path'], set_field('completed_native_reference_pairs', 595), 'interruption attribution'),
            (self.accepted['continuation_plan']['path'], set_field('completed_cohorts_repeated', True), 'continuation contract'),
            (self.accepted['continuation']['path'], lambda d: d['cohorts'].pop(), 'remaining-cohort completion'),
        ]
        for path, action, reason in cases:
            with self.subTest(reason=reason):
                self.reject_decoded_changes({path: action}, reason)

    def test_baseline_environment_changes(self):
        # Before/after observations are byte-identical. Mutating their shared
        # decoded bytes must still fail the independently frozen inventory hash.
        self.reject_decoded_changes({self.accepted['environment_after']['path']:
            lambda d: d['files'][0].__setitem__('sha256', '0' * 64)}, 'baseline file inventory differs')
        self.reject_decoded_changes({self.manifest['protocol']['path']:
            lambda d: d['baseline'].__setitem__('version', '0.0.0')}, 'baseline engine/version')

    def test_incomplete_task_and_record_coverage(self):
        self.reject_decoded_changes({self.manifest['acceptance']['path']:
            lambda d: d['tasks'].pop(next(iter(d['tasks'])))}, 'task count differs')
        self.reject_decoded_changes({self.first_report: lambda d: d['records'].pop()},
                                    'validator rejected report|record coverage differs')

    def test_resource_and_sql_drift(self):
        self.reject_decoded_changes({self.first_report:
            lambda d: d['resource_policy'].__setitem__('native_max_parallelism', 3)},
            'validator rejected report|resource policy differs')

        def changed_sql(report):
            evidence = report['records'][self.native_index]['evidence']
            calls = json.loads(evidence['benchmark_native_calls'])
            command = calls[0]['command']
            command[command.index('--sql') + 1] = 'SELECT 0'
            evidence['benchmark_native_calls'] = json.dumps(calls)

        self.reject_decoded_changes({self.first_report: changed_sql}, 'declared complete workload')

    def test_changed_complete_values(self):
        def altered(report):
            report['records'][self.native_index]['result']['row_count'] += 1

        self.reject_decoded_changes({self.first_report: altered}, 'complete result differs')

    def test_native_fallback_is_rejected(self):
        calls = json.loads(self.report['records'][self.native_index]['evidence']['benchmark_native_calls'])
        self.reject_decoded_changes({calls[0]['stdout']:
            lambda d: d['fallback'].__setitem__('attempted', True)}, 'unsafe fallback evidence')

    def test_missing_writer_readback_is_rejected(self):
        path = self.accepted['cohorts'][2]['report']['path']

        def altered(report):
            row = next(row for row in report['records'] if row['engine'] == 'shardloom')
            row['evidence']['benchmark_output_validation_calls'] = '[]'

        self.reject_decoded_changes({path: altered}, 'writer lacks readback evidence')

    def test_original_payload_type_order_and_completeness(self):
        from benchmarks.traditional_analytics.workloads import WORKLOADS
        payload = self.helpers[1]
        row = next(row for row in self.report['records'] if row['engine'] == 'shardloom'
                   and WORKLOADS[row['scenario']].result_kind == 'top')
        workload = WORKLOADS[row['scenario']]
        call = json.loads(row['evidence']['benchmark_native_calls'])[0]
        positive = self.evidence.read(call['stdout'])
        payload.check_payload(positive, workload, 0)
        for case, reason in [('order', 'terminal order'), ('type', 'logical type'),
                             ('field', 'row fields'), ('complete', 'payload incomplete'),
                             ('duplicate', 'duplicate envelope field')]:
            with self.subTest(case=case):
                envelope = copy.deepcopy(positive)
                values = payload.fields(envelope['result']['fields'])
                if case == 'order':
                    values['result_jsonl'] = '\n'.join(reversed(values['result_jsonl'].splitlines()))
                elif case == 'type':
                    schema = json.loads(values['result_schema_json'])
                    schema['Struct'][0]['dtypes'][0]['Primitive'][0] = 'f64'
                    values['result_schema_json'] = json.dumps(schema)
                elif case == 'field':
                    rows = [json.loads(line) for line in values['result_jsonl'].splitlines()]
                    del rows[0]['id']
                    values['result_jsonl'] = '\n'.join(json.dumps(row) for row in rows)
                elif case == 'complete':
                    values['result_payload_complete'] = 'false'
                items = [{'key': key, 'value': value} for key, value in values.items()]
                if case == 'duplicate':
                    items.append(copy.deepcopy(items[0]))
                envelope['fields'] = copy.deepcopy(items)
                envelope['result']['fields'] = copy.deepcopy(items)
                with self.assertRaisesRegex(ValueError, reason):
                    payload.check_payload(envelope, workload, 0)


if __name__ == '__main__':
    unittest.main()
