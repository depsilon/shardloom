"""Native opt-in output-window diagnostics and ownership contracts."""
import gc
import hashlib
import json
import os
from pathlib import Path
import struct
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

if __package__:
    from .test_native_binding import _load_native
else:
    from test_native_binding import _load_native

NATIVE = None
pa = None
WINDOW = 4 << 20
_DERIVED_WIDTHS = {0, 8, 32}
_I64_MIN = -(1 << 63)
_I64_MAX = (1 << 63) - 1


def source_fields():
    """Return source schema in protocol order."""
    return [
        {"name": "k", "type": "int64", "nullable": True},
        {"name": "id", "type": "int64", "nullable": False},
        {"name": "x", "type": "int64", "nullable": True},
        {"name": "y", "type": "int64", "nullable": True},
    ]


def _row_index(r: int) -> int:
    if isinstance(r, bool) or not isinstance(r, int) or r < 0:
        raise ValueError(f"row index must be a nonnegative integer: {r!r}")
    return r


def source_row(r: int) -> tuple[int | None, int, int | None, int | None]:
    """Return source scalar values for original row ordinal ``r``."""
    r = _row_index(r)
    k = None if r % 31 == 0 else (r * 17 % 257) - 128
    x = None if r % 11 == 0 else (r * 13 % 100003) - 50001
    y = None if r % 17 == 0 else (r * 7 % 10007) - 5003
    return k, r, x, y


def fixture(arrow, rows):
    fields = source_fields()
    schema = arrow.schema([arrow.field(f['name'], arrow.int64(), nullable=f['nullable']) for f in fields])
    scalars = [source_row(r) for r in range(rows)]
    arrays = [arrow.array([row[i] for row in scalars], type=arrow.int64()) for i in range(4)]
    return arrow.RecordBatch.from_arrays(arrays, schema=schema)


def _cell_parts(cell):
    if not isinstance(cell, dict):
        raise TypeError("cell must be a mapping with plan and derived fields")
    plan = cell.get("plan")
    width = cell.get("derived")
    if plan not in {"project", "sort"}:
        raise ValueError(f"unsupported plan: {plan!r}")
    if isinstance(width, bool) or not isinstance(width, int) or width not in _DERIVED_WIDTHS:
        raise ValueError(f"invalid derived width: {width!r}")
    rows = cell.get("rows")
    if isinstance(rows, bool) or not isinstance(rows, int) or rows < 0:
        raise ValueError(f"invalid row count: {rows!r}")
    return plan, width


def output_row(cell, r):
    """Return projected values while preserving SQL NULL arithmetic."""
    plan, width = _cell_parts(cell)
    if plan not in {"project", "sort"}:  # defensive if plan policy grows
        raise ValueError(f"unsupported plan: {plan!r}")
    k, row_id, x, y = source_row(r)
    derived = tuple(None if x is None or y is None else x * (j + 1) + y for j in range(width))
    return k, row_id, derived


def _scalar_bytes(value):
    if value is None:
        return b"\x00" + bytes(8)
    if isinstance(value, bool) or not isinstance(value, int) or not _I64_MIN <= value <= _I64_MAX:
        raise ValueError(f"value is not a signed I64 scalar: {value!r}")
    return b"\x01" + struct.pack("<q", value)


def _encode_rows(rows, cell):
    encoded = bytearray()
    if cell is None:
        for r in rows:
            for value in source_row(r):
                encoded.extend(_scalar_bytes(value))
        return bytes(encoded)

    plan, _ = _cell_parts(cell)
    output = [output_row(cell, r) for r in rows]
    if plan == "sort":
        # Python's sort is stable; only k participates, with NULLS LAST.
        output.sort(key=lambda row: (row[0] is None, 0 if row[0] is None else row[0]))
    for k, row_id, derived in output:
        for value in (k, row_id, *derived):
            encoded.extend(_scalar_bytes(value))
    return bytes(encoded)


def expected_bytes(cell, rows):
    """Encode expected query output in row-major scalar format."""
    return _encode_rows(rows, cell)


def scalar_bytes(batch):
    columns = [column.to_pylist() for column in batch.columns]
    result = bytearray()
    for row in zip(*columns, strict=True):
        for value in row:
            if value is None:
                result.extend(b'\0' * 9)
            else:
                assert type(value) is int and -(1 << 63) <= value < (1 << 63)
                result.extend(b'\1' + struct.pack('<q', value))
    return bytes(result)


def raw(batch):
    with pa.RecordBatchReader.from_stream(batch) as reader:
        return b''.join(scalar_bytes(value) for value in reader)


def values(batch):
    with pa.RecordBatchReader.from_stream(batch) as reader:
        return reader.read_all().to_pylist()


class NativeOutputWindowObservationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        global NATIVE, pa
        if os.environ.get('SHARDLOOM_REQUIRE_OUTPUT_WINDOW_OBSERVATION') != '1':
            raise unittest.SkipTest(
                'diagnostic-only native build required; set SHARDLOOM_REQUIRE_OUTPUT_WINDOW_OBSERVATION=1'
            )
        import pyarrow
        pa = pyarrow
        NATIVE = _load_native()
        assert hasattr(NATIVE.ExecutionStream, 'output_window_observation')
        assert hasattr(NATIVE.ExecutionStream, '_output_window_sent_batches')

    def setUp(self):
        self.environment = patch.dict(os.environ, SHARDLOOM_OUTPUT_WINDOW_OBSERVATION='trace')
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def make_plan(self, rows=4097, query=None):
        session = NATIVE.Session(NATIVE.ExecutionResources(64 << 20, 1))
        source = session.from_arrow(fixture(pa, rows))
        query = query or ('SELECT k,id,' + ','.join(f'x*{j+1}+y AS d{j:03d}' for j in range(8)) + " FROM 'memory://focused'")
        plan = session.sql(query, sources=[('memory://focused', source)])
        plan.prepare()
        return session, source, plan

    def sent_barrier(self, stream, count):
        # The diagnostic counter is published after the real channel send.
        # Poll an observed condition with a deadline, never assume a sleep made
        # the producer reach a particular state. No demand is sent here.
        deadline = time.monotonic() + 10
        while stream._output_window_sent_batches < count:
            if time.monotonic() >= deadline:
                self.fail(f'native send barrier {count} was not reached')
            threading.Event().wait(0.001)
        self.assertEqual(stream._output_window_sent_batches, count)

    def observation(self, stream, succeeded, active=True):
        value = json.loads(stream.output_window_observation)
        self.assertEqual(value['schema_version'], 'shardloom.output_window_observation.v2')
        self.assertIs(value['operation_succeeded'], succeeded)
        self.assertIs(value['engine_succeeded'], succeeded)
        self.assertEqual(value['engine_calls'], 1)
        self.assertIs(value['valid'], True, value)
        self.assertIsNone(value['error'])
        window = value['window']
        self.assertTrue(window['initialized'])
        self.assertTrue(window['released'])
        self.assertEqual(window['selected_policy'], 'two_batch_window' if active else 'ordinary_ineligible')
        self.assertEqual(window['metadata_reserved_bytes'], 1024 if active else 0)
        for key in ('metadata_final_bytes', 'outstanding_batches', 'outstanding_referenced_bytes'):
            self.assertEqual(window[key], 0)
        return value

    def assert_reuse(self, session):
        later = session.from_range('later', 0, 1, 1)
        self.assertEqual(later.row_count, 1)

    def test_request_validation_precedes_worker_and_keeps_zero_ordinary(self):
        session, source, plan = self.make_plan(17)
        before = session.snapshot()
        for value in (True, False, -1, 1, WINDOW - 1, WINDOW + 1, 1 << 80, 0.0, float('nan'), '4194304', None):
            with self.subTest(value=value), self.assertRaises((TypeError, ValueError)):
                plan.iter_batches(output_window_bytes=value)
            self.assertEqual(session.snapshot(), before)
        for arguments in ({}, {'output_window_bytes': 0}):
            with plan.iter_batches(**arguments) as stream:
                batches = list(stream)
                self.assertEqual(b''.join(raw(batch) for batch in batches),
                    expected_bytes(dict(plan='project', derived=8, rows=17), range(17)))
                observation = json.loads(stream.output_window_observation)
                self.assertEqual(observation['window']['selected_policy'], 'ordinary_not_requested')
                self.assertEqual(observation['window']['metadata_reserved_bytes'], 0)
            del batches, stream
        plan.release_prepared()
        del plan, source
        gc.collect()
        self.assertEqual(session.snapshot()['reserved_bytes'], 0)
        session.close()

    def test_complete_values_final_acknowledgment_and_last_exported_owner_credit(self):
        cell = dict(plan='project', derived=8, rows=4097)
        for mode in ('clock', 'trace'):
            with self.subTest(mode=mode), patch.dict(os.environ, SHARDLOOM_OUTPUT_WINDOW_OBSERVATION=mode):
                session, source, plan = self.make_plan()
                before = session.snapshot()
                with plan.iter_batches(output_window_bytes=WINDOW) as stream:
                    self.assertEqual(session.snapshot(), before)
                    self.assertEqual(stream._output_window_sent_batches, 0)
                    self.assertIsNone(stream.report)
                    self.assertIsNone(stream.output_window_observation)
                    first = next(stream)
                    alias = first.slice(0, first.row_count)
                    sliced = first.slice(2, 3)
                    table = pa.RecordBatchReader.from_stream(first).read_all()
                    observed = bytearray(raw(first))
                    for _ in range(2):
                        batch = next(stream)
                        observed.extend(raw(batch))
                    del batch
                    self.sent_barrier(stream, 3)
                    self.assertIsNone(stream.report)
                    self.assertIsNone(stream.output_window_observation)
                    self.assertEqual(session.snapshot()['completed_executions'], 0)
                    self.assertIsNone(next(stream, None))
                    self.assertEqual(bytes(observed), expected_bytes(cell, range(4097)))
                    self.assertTrue(stream.report['columnar_export']['complete'])
                    observation = self.observation(stream, True)
                    self.assertEqual(observation['acknowledged_count'], 3)
                    self.assertEqual(observation['window']['maximum_unacknowledged_batches'], 2)
                    self.assertEqual(observation['window']['abandoned_batches'], 0)
                del stream
                plan.release_prepared()
                del plan, source
                session.close()
                self.assertGreater(session.snapshot()['reserved_bytes'], 0)
                self.assertEqual(raw(alias), expected_bytes(cell, range(first.row_count)))
                del first, alias
                self.assertGreater(session.snapshot()['reserved_bytes'], 0)
                self.assertEqual(raw(sliced), expected_bytes(cell, range(2, 5)))
                del sliced
                self.assertGreater(session.snapshot()['reserved_bytes'], 0)
                self.assertEqual(b''.join(scalar_bytes(batch) for batch in table.to_batches()),
                    expected_bytes(cell, range(2048)))
                del table
                gc.collect()
                self.assertEqual(session.snapshot()['reserved_bytes'], 0)

    def test_empty_single_partial_and_all_null_values_preserve_schema_and_partition(self):
        for count in (0, 1, 17, 2048, 2049, 4097):
            for all_null in (False, True):
                with self.subTest(count=count, all_null=all_null):
                    session = NATIVE.Session(NATIVE.ExecutionResources(64 << 20, 1))
                    numbers = [None if all_null or index % 7 == 0 else index for index in range(count)]
                    source = session.from_arrow(pa.record_batch([pa.array(numbers, type=pa.int64())], names=['n']))
                    plan = session.sql("SELECT n AS alias,n+1 AS value FROM 'memory://values'", sources=[('memory://values', source)])
                    with plan.iter_batches(output_window_bytes=WINDOW) as stream:
                        got, partitions = [], []
                        for batch in stream:
                            partitions.append(batch.row_count)
                            self.assertEqual(batch.schema, [('alias', {'type': 'int64', 'nullable': True}),
                                                            ('value', {'type': 'int64', 'nullable': True})])
                            got.extend(values(batch))
                        del batch
                        self.assertEqual(got, [dict(alias=n, value=None if n is None else n + 1) for n in numbers])
                        self.assertEqual(partitions, [0] if count == 0 else [min(2048, count-start) for start in range(0,count,2048)])
                        self.observation(stream, True)
                    del stream
                    plan.release_prepared()
                    del plan, source
                    gc.collect()
                    self.assertEqual(session.snapshot()['reserved_bytes'], 0)
                    session.close()

    def test_actual_row_and_field_admission_boundaries(self):
        for count, fields, admitted in ((65536, 1, True), (65537, 1, False), (17, 64, True), (17, 65, False)):
            with self.subTest(count=count, fields=fields):
                session = NATIVE.Session(NATIVE.ExecutionResources(64 << 20, 1))
                source = session.from_range('n', 0, 1, count)
                columns = ','.join(f'n AS n{index}' for index in range(fields))
                plan = session.sql(f"SELECT {columns} FROM 'memory://bounds'", sources=[('memory://bounds', source)])
                with plan.iter_batches(output_window_bytes=WINDOW) as stream:
                    observed = []
                    for batch in stream:
                        observed.extend(values(batch))
                    del batch
                    self.assertEqual(observed, [{f'n{index}': row for index in range(fields)} for row in range(count)])
                    self.observation(stream, True, admitted)
                del stream
                plan.release_prepared()
                del plan, source
                gc.collect()
                self.assertEqual(session.snapshot()['reserved_bytes'], 0)
                session.close()

    def test_filter_limit_sort_and_non_int64_keep_ordinary_policy(self):
        queries = (
            ("SELECT id FROM 'memory://focused' WHERE id >= 1", [{'id': n} for n in range(1, 17)]),
            ("SELECT id FROM 'memory://focused' LIMIT 3", [{'id': n} for n in range(3)]),
            ("SELECT id FROM 'memory://focused' ORDER BY id DESC", [{'id': n} for n in reversed(range(17))]),
            ("SELECT CAST(id AS DOUBLE) AS value FROM 'memory://focused'", [{'value': float(n)} for n in range(17)]),
        )
        for query, expected in queries:
            with self.subTest(query=query):
                session, source, plan = self.make_plan(17, query)
                with plan.iter_batches(output_window_bytes=WINDOW) as stream:
                    got = [row for batch in stream for row in values(batch)]
                    self.assertEqual(got, expected)
                    self.observation(stream, True, False)
                del stream
                plan.release_prepared()
                del plan, source
                gc.collect()
                self.assertEqual(session.snapshot()['reserved_bytes'], 0)
                session.close()

    def test_lazy_and_streaming_producers_remain_ordinary_with_no_pre_demand_effect(self):
        for streaming in (False, True):
            with self.subTest(streaming=streaming):
                events = []
                def producer():
                    events.append('open')
                    try:
                        for index in range(3):
                            events.append(index)
                            yield pa.record_batch([pa.array([index], type=pa.int64())], names=['n'])
                    finally:
                        events.append('close')
                session = NATIVE.Session(NATIVE.ExecutionResources(64 << 20, 1))
                plan = session.sql("SELECT n FROM 'memory://lazy'", batches=[
                    ('memory://lazy', producer, [('n', json.dumps({'Primitive': ['i64', True]}))], streaming)])
                with plan.iter_batches(output_window_bytes=WINDOW) as stream:
                    self.assertEqual(events, [])
                    self.assertEqual(stream._output_window_sent_batches, 0)
                    first = next(stream)
                    self.sent_barrier(stream, 1)
                    self.assertIsNone(stream.report)
                    if streaming:
                        self.assertEqual(events, ['open', 0])
                    else:
                        self.assertEqual(events, ['open', 0, 1, 2, 'close'])
                    got = values(first) + [row for batch in stream for row in values(batch)]
                    self.assertEqual(got, [{'n': 0}, {'n': 1}, {'n': 2}])
                    self.assertEqual(events, ['open', 0, 1, 2, 'close'])
                    self.observation(stream, True, False)
                del first, stream
                plan.release_prepared()
                del plan
                gc.collect()
                self.assertEqual(session.snapshot()['reserved_bytes'], 0)
                self.assert_reuse(session)
                session.close()

    def test_full_queue_close_exception_cancel_and_cross_thread_close_release_owners(self):
        for action in ('close', 'exception', 'cancel', 'thread_close'):
            with self.subTest(action=action):
                session, source, plan = self.make_plan()
                with plan.iter_batches(output_window_bytes=WINDOW) as stream:
                    first = next(stream)
                    self.sent_barrier(stream, 2)
                    self.assertEqual(session.snapshot()['completed_executions'], 0)
                    self.assertIsNone(stream.report)
                    if action == 'exception':
                        marker = RuntimeError('frozen consumer exception')
                        try:
                            with stream:
                                raise marker
                        except RuntimeError as error:
                            self.assertIs(error, marker)
                    elif action == 'cancel':
                        session.cancel()
                        with self.assertRaisesRegex(RuntimeError, 'cancelled'):
                            next(stream)
                        stream.close()
                    elif action == 'thread_close':
                        errors = []
                        def close():
                            try:
                                stream.close()
                            except BaseException as error:
                                errors.append(error)
                        closer = threading.Thread(target=close)
                        closer.start()
                        closer.join(10)
                        self.assertFalse(closer.is_alive())
                        self.assertEqual(errors, [])
                    else:
                        stream.close()
                    self.assertIsNone(stream.report)
                    observation = self.observation(stream, False)
                    self.assertEqual(observation['sent_count'], 2)
                    self.assertEqual(observation['acknowledged_count'], 0)
                    self.assertEqual(observation['window']['abandoned_batches'], 2)
                    self.assertEqual(raw(first), expected_bytes(dict(plan='project',derived=8,rows=4097), range(2048)))
                    self.assert_reuse(session)
                del stream
                plan.release_prepared()
                del plan, source
                self.assertGreater(session.snapshot()['reserved_bytes'], 0)
                del first
                gc.collect()
                self.assertEqual(session.snapshot()['reserved_bytes'], 0)
                session.close()

    def test_full_window_refuses_reentrant_same_session_and_prepared_release(self):
        session, source, plan = self.make_plan()
        with plan.iter_batches(output_window_bytes=WINDOW) as stream:
            first = next(stream)
            self.sent_barrier(stream, 2)
            with self.assertRaisesRegex(RuntimeError, 'incremental consumer'):
                session.from_range('interleaved', 0, 1, 1)
            with self.assertRaisesRegex(RuntimeError, 'prepared plan is executing'):
                plan.release_prepared()
        self.observation(stream, False)
        self.assert_reuse(session)
        del first, stream
        plan.release_prepared()
        del plan, source
        gc.collect()
        self.assertEqual(session.snapshot()['reserved_bytes'], 0)
        session.close()

    def test_pre_cancel_has_no_worker_or_output_and_session_is_reusable(self):
        session, source, plan = self.make_plan()
        with plan.iter_batches(output_window_bytes=WINDOW) as stream:
            session.cancel()
            with self.assertRaisesRegex(RuntimeError, 'cancelled'):
                next(stream)
            self.assertIsNone(stream.report)
            self.assertIsNone(stream.output_window_observation)
            self.assertEqual(stream._output_window_sent_batches, 0)
        self.assert_reuse(session)
        del stream
        plan.release_prepared()
        del plan, source
        gc.collect()
        self.assertEqual(session.snapshot()['reserved_bytes'], 0)
        session.close()

    def test_deadline_starts_on_demand_and_expires_with_a_full_window(self):
        session, source, plan = self.make_plan()
        with plan.iter_batches(output_window_bytes=WINDOW, timeout=0.25) as stream:
            threading.Event().wait(0.3)  # Explicit idle-deadline test, outside comparisons.
            first = next(stream)
            self.sent_barrier(stream, 2)
            threading.Event().wait(0.3)
            with self.assertRaisesRegex(RuntimeError, 'deadline exceeded'):
                next(stream)
            stream.close()
            self.assertIsNone(stream.report)
            observation = self.observation(stream, False)
            self.assertEqual(observation['sent_count'], 2)
            self.assertEqual(observation['acknowledged_count'], 0)
            self.assertEqual(observation['window']['abandoned_batches'], 2)
            self.assertEqual(raw(first), expected_bytes(dict(plan='project',derived=8,rows=4097), range(2048)))
        self.assert_reuse(session)
        del first, stream
        plan.release_prepared()
        del plan, source
        gc.collect()
        self.assertEqual(session.snapshot()['reserved_bytes'], 0)
        session.close()

    def test_late_overflow_keeps_provisional_prefix_without_fabricating_ack_or_success(self):
        session = NATIVE.Session(NATIVE.ExecutionResources(64 << 20, 1))
        source = session.from_arrow(pa.record_batch([pa.array(list(range(2048)) + [(1 << 63)-1], type=pa.int64())], names=['n']))
        plan = session.sql("SELECT n+1 AS value FROM 'memory://overflow'", sources=[('memory://overflow', source)])
        with plan.iter_batches(output_window_bytes=WINDOW) as stream:
            first = next(stream)
            self.assertEqual(values(first), [{'value': n} for n in range(1, 2049)])
            with self.assertRaisesRegex(RuntimeError, 'overflow'):
                next(stream)
            self.assertIsNone(stream.report)
            observation = self.observation(stream, False)
            self.assertEqual(observation['sent_count'], 1)
            self.assertEqual(observation['acknowledged_count'], 0)
            self.assertEqual(observation['window']['abandoned_batches'], 1)
            self.assertEqual(session.snapshot()['completed_executions'], 0)
        self.assert_reuse(session)
        del first, stream
        plan.release_prepared()
        del plan, source
        gc.collect()
        self.assertEqual(session.snapshot()['reserved_bytes'], 0)
        session.close()

    def test_parquet_and_native_file_sources_and_spill_requests_are_ineligible(self):
        import pyarrow.parquet as pq
        with tempfile.TemporaryDirectory(prefix='output-credit-window-files-') as directory:
            root = Path(directory)
            parquet = root / 'source.parquet'
            pq.write_table(pa.table({'n': list(range(17))}), parquet)
            session = NATIVE.Session(NATIVE.ExecutionResources(64 << 20, 1))
            source = session.from_range('n', 0, 1, 17)
            original = session.sql("SELECT n FROM 'memory://files'", sources=[('memory://files', source)])
            vortex = root / 'source.vortex'
            original.write(vortex, 'write_vortex')
            for path, format_name in ((parquet, 'parquet'), (vortex, 'vortex')):
                with self.subTest(format=format_name):
                    plan = session.sql(f"SELECT n FROM '{path}'", files=[(str(path), format_name, None)])
                    with plan.iter_batches(output_window_bytes=WINDOW) as stream:
                        self.assertEqual([row for batch in stream for row in values(batch)], [{'n': n} for n in range(17)])
                        self.observation(stream, True, False)
                    del stream
                    plan.release_prepared()
                    del plan
            before_spill = {path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
                            for path in root.rglob('*') if path.is_file()}
            spill = json.dumps(dict(workspace=str(root), quota_bytes=64 << 20, buffer_bytes=1 << 20))
            plan = session.sql("SELECT n FROM 'memory://files' ORDER BY n DESC", sources=[('memory://files', source)], spill=spill)
            with plan.iter_batches(output_window_bytes=WINDOW) as stream:
                self.assertEqual([row for batch in stream for row in values(batch)], [{'n': n} for n in reversed(range(17))])
                self.observation(stream, True, False)
            del stream
            original.release_prepared()
            plan.release_prepared()
            del original, plan, source
            gc.collect()
            self.assertEqual(session.snapshot()['reserved_bytes'], 0)
            self.assertEqual({path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
                              for path in root.rglob('*') if path.is_file()}, before_spill)
            session.close()

    def test_invalid_observation_refuses_before_worker_and_absent_observation_is_inert(self):
        session, source, plan = self.make_plan(17)
        before = session.snapshot()
        with patch.dict(os.environ, SHARDLOOM_OUTPUT_WINDOW_OBSERVATION='invalid'):
            with plan.iter_batches(output_window_bytes=WINDOW) as stream:
                with self.assertRaisesRegex(RuntimeError, 'requires clock or trace'):
                    next(stream)
                self.assertIsNone(stream.report)
                self.assertIsNone(stream.output_window_observation)
                self.assertEqual(stream._output_window_sent_batches, 0)
                self.assertEqual(session.snapshot(), before)
            del stream
        with patch.dict(os.environ):
            os.environ.pop('SHARDLOOM_OUTPUT_WINDOW_OBSERVATION', None)
            with plan.iter_batches(output_window_bytes=WINDOW) as stream:
                self.assertEqual(b''.join(raw(batch) for batch in stream),
                    expected_bytes(dict(plan='project',derived=8,rows=17), range(17)))
                self.assertIsNotNone(stream.report)
                self.assertIsNone(stream.output_window_observation)
                self.assertEqual(stream._output_window_sent_batches, 0)
            del stream
        plan.release_prepared()
        del plan, source
        gc.collect()
        self.assertEqual(session.snapshot()['reserved_bytes'], 0)
        session.close()


if __name__ == '__main__':
    unittest.main()
