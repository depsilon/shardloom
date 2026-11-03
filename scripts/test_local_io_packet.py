# SPDX-License-Identifier: Apache-2.0
"""Corruption controls for the historical local I/O saved-evidence contract."""
import copy
from decimal import Decimal
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'scripts'), str(ROOT / 'benchmarks/traditional_analytics')]
from local_io_portable_replay import Collection, Values, complete_gate, execution, refusal, safe_fallback, same
from local_io_replay_values import avro_type, compare, compare_wire, exact_json, read_output
from query_timing_report import TIMING_BOUNDARY, TIMING_CLOCK, query_phase_timings


def record(targets=()):
    fields = dict(fallback_attempted='false', external_engine_invoked='false',
        execution_resource_admitted_memory_bytes=str(512 << 20),
        execution_resource_admitted_max_parallelism='2',
        native_source_demand_prepared_input_bytes_written='0', native_io_certified='true',
        native_query_timing_reported='true', native_query_timing_clock=TIMING_CLOCK,
        native_query_timing_boundary=TIMING_BOUNDARY, native_query_execution_nanos='123456789',
        native_result_serialization_nanos='100')
    if targets:
        fields.update(native_vortex_result_export_execution_count='1',
                      native_vortex_result_export_target_count=str(len(targets)),
                      native_vortex_result_export_all_targets_committed='true')
    return dict(fields=fields, targets=list(targets), case='broad', **query_phase_timings(fields, 0.5))


class LocalIoContractTests(unittest.TestCase):
    def setUp(self):
        self.protocol = dict(memory_bytes=512 << 20, max_parallelism=2)
        self.source = dict(format='parquet')

    def test_collection_rejects_replacement_manifest_before_opening_members(self):
        with tempfile.TemporaryDirectory() as folder:
            (Path(folder) / 'manifest.json').write_text('{}')
            with self.assertRaisesRegex(ValueError, 'collection identity'):
                Collection(folder)

    def test_original_external_paths_are_never_read_on_missing_archive_lookup(self):
        collection = Collection.__new__(Collection)
        collection.paths = {}
        with tempfile.NamedTemporaryFile() as existing:
            with self.assertRaisesRegex(ValueError, 'missing archived path'):
                collection.raw(existing.name)

    def test_resource_fallback_and_preparation_substitutions_fail(self):
        original = record()
        execution(original, self.source, self.protocol)
        changes = [('fallback_attempted', 'true'), ('external_engine_invoked', 'true'),
            ('execution_resource_admitted_memory_bytes', str(1 << 30)),
            ('execution_resource_admitted_max_parallelism', '3'),
            ('native_source_demand_prepared_input_bytes_written', '17'),
            ('native_io_certified', 'false')]
        for key, value in changes:
            changed = copy.deepcopy(original)
            changed['fields'][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                execution(changed, self.source, self.protocol)

    def test_writer_requires_one_execution_and_every_commit(self):
        original = record([['parquet', '/evidence/a'], ['vortex', '/evidence/b']])
        execution(original, self.source, self.protocol)
        for suffix, value in [('execution_count', '2'), ('target_count', '1'),
                              ('all_targets_committed', 'false')]:
            changed = copy.deepcopy(original)
            changed['fields']['native_vortex_result_export_' + suffix] = value
            with self.subTest(suffix=suffix), self.assertRaises(ValueError):
                execution(changed, self.source, self.protocol)

    def test_missing_writer_clock_stays_unavailable(self):
        original = record([['parquet', '/evidence/a']])
        original['fields'].pop('native_query_timing_reported')
        original.update(query_phase_timings(original['fields'], 0.5, allow_missing=True))
        execution(original, self.source, self.protocol)
        original['engine_execution_seconds'] = 0.5
        with self.assertRaisesRegex(ValueError, 'saved clock'):
            execution(original, self.source, self.protocol)

    def test_collection_clock_or_scope_cannot_be_replaced(self):
        for mutation in ('missing', 'value', 'scope'):
            changed = record()
            if mutation == 'missing':
                changed['fields'].pop('native_query_timing_reported')
            elif mutation == 'value':
                changed['engine_execution_seconds'] = 0.5
            else:
                changed['fields']['native_query_timing_boundary'] = 'whole_api'
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                execution(changed, self.source, self.protocol)

    def test_metadata_count_rejects_payload_work(self):
        original = record()
        original['case'] = 'count'
        keys = ('native_source_demand_projected_columns', 'native_source_demand_physical_leaf_columns',
            'native_source_demand_rows_decoded', 'native_source_demand_row_groups_read',
            'native_source_demand_data_read_bytes', 'native_source_demand_data_read_requests',
            'native_input_batches', 'native_input_batch_rows')
        original['fields'].update({key: '0' for key in keys})
        original['fields'].update(native_source_metadata_count_sources='1', native_input_end_observed='true')
        execution(original, self.source, self.protocol)
        for key in (*keys, 'native_input_end_observed'):
            changed = copy.deepcopy(original)
            changed['fields'][key] = 'false' if key.endswith('observed') else '1'
            with self.subTest(key=key), self.assertRaises(ValueError):
                execution(changed, self.source, self.protocol)

    def test_passed_receipt_requires_process_cleanup(self):
        gate = dict(status='passed', returncode=0, process_group_drained=True,
                    direct_child_reaped=True, timed_out=False, interrupted_signal=None)
        complete_gate(gate)
        for key, value in [('returncode', 1), ('process_group_drained', False),
                           ('direct_child_reaped', False), ('timed_out', True), ('interrupted_signal', 15)]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                complete_gate({**gate, key: value})

    def test_refusal_cannot_hide_success_or_fallback(self):
        fallback = dict(attempted=False, allowed=False, engine=None)
        item = dict(expected_refusal=True, refusal_evidence=dict(external_engine_invoked=False,
            diagnostic=dict(code='SL_INVALID_INPUT', reason='value exceeds i64::MAX', fallback=fallback)))
        refusal(item)
        with self.assertRaises(ValueError):
            refusal({**item, 'actual': {}})
        for changed in (dict(attempted=False, allowed=True, engine=None), {},
                        dict(attempted=False, allowed=False, engine='external')):
            with self.subTest(fallback=changed), self.assertRaises(ValueError):
                safe_fallback(changed)

    def test_numeric_tolerance_does_not_accept_missing_results_or_nonfinite_clocks(self):
        self.assertTrue(same({'x': [1.0]}, {'x': [1.0 + 1e-12]}))
        for changed in ({}, {'x': []}, {'x': [1.1]}, {'x': [float('nan')]}):
            self.assertFalse(same({'x': [1.0]}, changed))
        self.assertFalse(exact_json({'x': [True]}, {'x': [1]}))
        self.assertFalse(exact_json({'x': [1.0]}, {'x': [1]}))


@unittest.skipUnless(importlib.util.find_spec('pyarrow'), 'PyArrow is an optional evidence-reader dependency')
class LocalIoValueTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import pyarrow
        cls.pa = pyarrow

    def test_complete_rows_order_and_nulls_are_compared(self):
        pa = self.pa
        expected = pa.table({'id': [1, 2, 3], 'text': ['a', None, 'z']})
        self.assertEqual(compare(expected, expected), 3)
        for changed in (expected.slice(0, 2), expected.take([2, 1, 0]),
                        pa.table({'id': [1, 2, 3], 'text': ['a', '', 'z']}),
                        pa.table({'id': [1, 2, 4], 'text': ['a', None, 'z']})):
            with self.subTest(changed=changed.schema), self.assertRaises(ValueError):
                compare(changed, expected)

    def test_reused_decode_still_validates_each_output_report(self):
        pa = self.pa
        expected = pa.table({'id': [1, 2]})
        sink = pa.BufferOutputStream()
        with pa.ipc.new_file(sink, expected.schema) as writer:
            writer.write_table(expected)
        raw = sink.getvalue().to_pybytes()
        digest = hashlib.sha256(raw).hexdigest()
        first = dict(path='/first.arrow', bytes=len(raw), sha256=digest)
        second = {**first, 'path': '/second.arrow'}
        reference = {**first, 'path': '/reference.arrow'}
        values = Values(SimpleNamespace(identity=lambda item: raw), {})
        report = dict(**first, format='arrow-ipc', rows=2, complete_values_schema_and_order_equal=True)
        for output in (first, second):
            values.readback(dict(output=output, report={**report, **output}), reference,
                            'arrow-ipc', rich=False, observation={})
        self.assertEqual(len(values.outputs), 1)
        for change in (dict(complete_values_schema_and_order_equal=False), dict(rows=1),
                       dict(path=first['path']), dict(format='parquet')):
            with self.subTest(change=change), self.assertRaises(ValueError):
                values.readback(dict(output=second, report={**report, **second, **change}),
                                reference, 'arrow-ipc', rich=False, observation={})

    def test_representation_normalization_preserves_metadata_and_nullability(self):
        pa = self.pa
        schema = pa.schema([pa.field('x', pa.string(), False, {b'unit': b'tag'})], {b'owner': b'test'})
        expected = pa.Table.from_pylist([{'x': 'a'}], schema=schema)
        self.assertEqual(compare(expected.cast(pa.schema([
            pa.field('x', pa.large_string(), False, {b'unit': b'tag'})], schema.metadata)), expected), 1)
        for changed in (expected.replace_schema_metadata(None),
                        expected.cast(pa.schema([pa.field('x', pa.string(), True, {b'unit': b'tag'})], schema.metadata))):
            with self.assertRaisesRegex(ValueError, 'schema'):
                compare(changed, expected)

    def test_nested_schema_and_temporal_units_are_not_erased(self):
        pa = self.pa
        expected = pa.table({'x': pa.array([[1], None], type=pa.list_(pa.field('item', pa.int64(), False)))})
        changed = pa.table({'x': pa.array([[1], None], type=pa.list_(pa.int64()))})
        with self.assertRaisesRegex(ValueError, 'schema'):
            compare(changed, expected)
        expected = pa.table({'x': pa.array([1000], type=pa.timestamp('ns', 'UTC'))})
        for dtype in (pa.timestamp('us', 'UTC'), pa.timestamp('ns')):
            with self.assertRaisesRegex(ValueError, 'schema'):
                compare(pa.table({'x': pa.array([1000], type=dtype)}), expected)

    def test_json_cannot_coerce_fractional_integers_or_booleans(self):
        pa = self.pa
        schema = pa.schema([('id', pa.int64()), ('flag', pa.bool_())])
        table = read_output(b'[{"id":1,"flag":true}]', 'json', schema)
        self.assertEqual(table.to_pylist(), [{'id': 1, 'flag': True}])
        for data in (b'[{"id":1.1,"flag":true}]', b'[{"id":true,"flag":true}]',
                     b'[{"id":1,"flag":1}]', b'[{"id":1,"id":2,"flag":true}]',
                     b'[{"id":1,"flag":true,"extra":0}]'):
            with self.subTest(data=data), self.assertRaises(ValueError):
                read_output(data, 'json', schema)

    def test_avro_nullability_precision_and_temporal_logical_type_are_checked(self):
        pa = self.pa
        field = pa.field('x', pa.decimal128(20, 2), True)
        wire = ['null', dict(type='bytes', logicalType='decimal', precision=20, scale=2)]
        avro_type(wire, field)
        for changed in (wire[1], ['null', 'long'], ['null', {**wire[1], 'scale': 3}],
                        ['null', {**wire[1], 'precision': 21}]):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                avro_type(changed, field)
        with self.assertRaises(ValueError):
            avro_type(['null', {'type': 'long', 'logicalType': 'timestamp-millis'}],
                      pa.field('x', pa.timestamp('us', '+00:00')))

    def test_rich_wire_preserves_decimal_binary_ticks_and_last_row(self):
        pa = self.pa
        expected = pa.table({'d': pa.array([Decimal('-1.23'), None], type=pa.decimal128(20, 2)),
            'b': pa.array([b'\x00\xff', None], type=pa.binary()),
            't': pa.array([(1 << 63) - 1, 1], type=pa.timestamp('ns'))})
        rows = [{'d': 'decimal128(20,2):-123', 'b': '00ff', 't': (1 << 63) - 1},
                {'d': None, 'b': None, 't': 1}]
        compare_wire(json.dumps(rows).encode(), 'json', expected)
        for changed in (rows[:-1], list(reversed(rows)), [rows[0], {**rows[1], 't': True}],
                        [{**rows[0], 'd': 'decimal128(20,2):-124'}, rows[1]]):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                compare_wire(json.dumps(changed).encode(), 'json', expected)


if __name__ == '__main__':
    unittest.main()
