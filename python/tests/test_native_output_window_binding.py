"""Normal-package output-window policy, public slicing and native owner contracts."""

import gc
import os
import unittest
from unittest.mock import patch

from test_native_binding import _load_native


WINDOW = 4 << 20


class NativeOutputWindowBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()
        import pyarrow
        cls.pa = pyarrow

    def session(self):
        return self.native.Session(self.native.ExecutionResources(64 << 20, 1))

    def rows(self, batch):
        with self.pa.RecordBatchReader.from_stream(batch) as reader:
            return reader.read_all().to_pylist()

    def assert_policy(self, stream, policy, requested=WINDOW):
        report = stream.report
        self.assertIsNotNone(report)
        self.assertEqual(report["columnar_output_window_requested_bytes"], str(requested))
        self.assertEqual(report["columnar_output_window_selected_policy"], policy)
        self.assertTrue(report["columnar_export"]["complete"])

    def test_normal_build_validation_and_disabled_observation(self):
        class IntegerOverrides(int):
            def forbidden(self, *args):
                raise AssertionError("integer override invoked")

            __eq__ = __ne__ = __bool__ = __int__ = __index__ = forbidden

        session = self.session()
        source = session.from_range("n", 0, 1, 17)
        plan = session.sql("SELECT n FROM 'memory://window'", sources=[("memory://window", source)])
        plan.prepare()
        before = session.snapshot()
        for value in (True, False, None, 0.0, "4194304", -1, 1, WINDOW + 1, 1 << 80, IntegerOverrides(7)):
            with self.subTest(value=value), self.assertRaises((TypeError, ValueError)):
                plan.iter_batches(output_window_bytes=value)
            self.assertEqual(session.snapshot(), before)
        # The normal package must neither expose nor interpret opt-in diagnostics.
        with patch.dict(os.environ, SHARDLOOM_OUTPUT_WINDOW_OBSERVATION="invalid"):
            for options in ({}, {"output_window_bytes": 0}, {"output_window_bytes": WINDOW},
                            {"output_window_bytes": IntegerOverrides(WINDOW)}):
                with plan.iter_batches(**options) as stream:
                    self.assertFalse(hasattr(stream, "output_window_observation"))
                    self.assertFalse(hasattr(stream, "_output_window_sent_batches"))
                    self.assertEqual([row for batch in stream for row in self.rows(batch)],
                                     [{"n": n} for n in range(17)])
                    requested = int.__int__(options.get("output_window_bytes", 0))
                    self.assert_policy(stream, "two_batch_window" if requested else "ordinary_not_requested", requested)
                del stream
        plan.release_prepared()
        del plan, source
        gc.collect()
        self.assertEqual(session.snapshot()["reserved_bytes"], 0)
        session.close()

    def test_empty_null_and_partial_output_wait_for_final_demand(self):
        for count in (0, 1, 17, 2048, 2049, 4097):
            with self.subTest(count=count):
                session = self.session()
                numbers = [None if n % 7 == 0 else n for n in range(count)]
                source = session.from_arrow(self.pa.record_batch(
                    [self.pa.array(numbers, type=self.pa.int64())], names=["n"]))
                plan = session.sql("SELECT n AS alias,n+1 AS value FROM 'memory://window'",
                                   sources=[("memory://window", source)])
                before = session.snapshot()
                with plan.iter_batches(output_window_bytes=WINDOW) as stream:
                    self.assertEqual(session.snapshot(), before)
                    actual = []
                    partitions = [min(2048, count - start) for start in range(0, count, 2048)] or [0]
                    for size in partitions:
                        batch = next(stream)
                        self.assertEqual(batch.row_count, size)
                        self.assertEqual([name for name, _ in batch.schema], ["alias", "value"])
                        actual.extend(self.rows(batch))
                        self.assertIsNone(stream.report)
                    del batch
                    self.assertIsNone(next(stream, None))
                    self.assertEqual(actual, [{"alias": n, "value": None if n is None else n + 1} for n in numbers])
                    self.assert_policy(stream, "two_batch_window")
                del stream
                plan.release_prepared()
                del plan, source
                gc.collect()
                self.assertEqual(session.snapshot()["reserved_bytes"], 0)
                session.close()

    def test_normal_package_checks_actual_admission_boundaries(self):
        for count, width, admitted in ((65536, 1, True), (65537, 1, False), (17, 64, True), (17, 65, False)):
            with self.subTest(count=count, width=width):
                session = self.session()
                source = session.from_range("n", 0, 1, count)
                columns = ",".join(f"n AS c{i}" for i in range(width))
                plan = session.sql(f"SELECT {columns} FROM 'memory://window'", sources=[("memory://window", source)])
                with plan.iter_batches(output_window_bytes=WINDOW) as stream:
                    actual = [row for batch in stream for row in self.rows(batch)]
                    self.assertEqual(actual, [{f"c{i}": n for i in range(width)} for n in range(count)])
                    self.assert_policy(stream, "two_batch_window" if admitted else "ordinary_ineligible")
                del stream
                plan.release_prepared()
                del plan, source
                gc.collect()
                self.assertEqual(session.snapshot()["reserved_bytes"], 0)
                session.close()

    def test_late_error_keeps_prefix_and_never_certifies_completion(self):
        session = self.session()
        source = session.from_arrow(self.pa.record_batch([
            self.pa.array([*range(2048), (1 << 63) - 1], type=self.pa.int64())], names=["n"]))
        plan = session.sql("SELECT n+1 AS value FROM 'memory://window'", sources=[("memory://window", source)])
        with plan.iter_batches(output_window_bytes=WINDOW) as stream:
            first = next(stream)
            with self.assertRaisesRegex(RuntimeError, "overflow"):
                next(stream)
            self.assertIsNone(stream.report)
            self.assertEqual(self.rows(first), [{"value": n} for n in range(1, 2049)])
        del stream
        plan.release_prepared()
        del plan, source
        self.assertGreater(session.snapshot()["reserved_bytes"], 0)
        del first
        gc.collect()
        self.assertEqual(session.snapshot()["reserved_bytes"], 0)
        self.assertEqual(session.from_range("later", 0, 1, 1).row_count, 1)
        session.close()

    def test_public_sql_and_dataframe_slicing_preserves_values_and_policy(self):
        import shardloom as sl
        from shardloom.query import SqlWorkflow

        for surface in ("sql", "dataframe_scan", "dataframe_projection"):
            for batch_rows in (257, 4096):
                with self.subTest(surface=surface, batch_rows=batch_rows):
                    client = sl.ShardLoomClient(transport="native", memory_bytes=64 << 20, max_parallelism=1)
                    self.addCleanup(client.close)
                    frame = sl.range(0, 4097, column="id", client=client)
                    if surface == "sql":
                        query = SqlWorkflow(f"SELECT id,id+1 AS value FROM '{frame.source.uri}'", client,
                                            source_bindings=(frame.source,))
                    elif surface == "dataframe_scan":
                        query = frame
                    else:
                        query = frame.select("id", "id+1 AS value")
                    # Composing this DataFrame projection retains SELECT over
                    # SELECT. The frozen actual-plan admission rejects that
                    # extra layer; public forwarding must not bypass admission.
                    policy = "ordinary_ineligible" if surface == "dataframe_projection" else "two_batch_window"
                    expected = [{"id": n} if surface == "dataframe_scan" else {"id": n, "value": n + 1}
                                for n in range(4097)]
                    with patch("subprocess.Popen", side_effect=AssertionError("execution process")):
                        with query.iter_batches(batch_rows=batch_rows, output_window_bytes=WINDOW) as stream:
                            self.assertIsNone(stream.report)
                            first = next(stream)
                            actual, lengths = self.rows(first), [first.native_result.row_count]
                            for batch in stream:
                                self.assertIsNone(stream.report)
                                lengths.append(batch.native_result.row_count)
                                actual.extend(self.rows(batch))
                            del batch
                            self.assertEqual(actual, expected)
                            self.assertEqual(lengths, ([257] * 7 + [249]) * 2 + [1]
                                             if batch_rows == 257 else [2048, 2048, 1])
                            self.assertEqual(stream.report.envelope.field("columnar_output_window_selected_policy"), policy)
                            self.assertEqual(stream.report.envelope.field_int("columnar_output_window_requested_bytes"), WINDOW)
                            self.assertFalse(stream.report.envelope.fallback.attempted)
                    native_session = client._native_backend.session
                    alias = first.native_result.slice(2, 3)
                    table = first.to_arrow()
                    del first, stream, query, frame
                    client.close()
                    gc.collect()
                    self.assertGreater(native_session.snapshot()["reserved_bytes"], 0)
                    self.assertEqual(self.rows(alias), expected[2:5])
                    del alias
                    self.assertGreater(native_session.snapshot()["reserved_bytes"], 0)
                    self.assertEqual(table.to_pylist(), actual[:len(table)])
                    del table
                    gc.collect()
                    self.assertEqual(native_session.snapshot()["reserved_bytes"], 0)

    def test_public_producer_remains_lazy_and_ordinary(self):
        import shardloom as sl

        for streaming in (False, True):
            with self.subTest(streaming=streaming):
                client = sl.ShardLoomClient(transport="native", memory_bytes=64 << 20, max_parallelism=1)
                self.addCleanup(client.close)
                events = []

                def producer():
                    events.append("open")
                    try:
                        for n in range(3):
                            events.append(n)
                            yield self.pa.record_batch([self.pa.array([n], type=self.pa.int64())], names=["n"])
                    finally:
                        events.append("close")

                frame = sl.from_batches(producer, schema={"n": "int64"}, streaming=streaming, client=client)
                with frame.iter_batches(batch_rows=1, output_window_bytes=WINDOW) as stream:
                    self.assertEqual(events, [])
                    first = next(stream)
                    self.assertEqual(events, ["open", 0] if streaming else ["open", 0, 1, 2, "close"])
                    self.assertIsNone(stream.report)
                    actual = self.rows(first) + [row for batch in stream for row in self.rows(batch)]
                    self.assertEqual(actual, [{"n": n} for n in range(3)])
                    self.assertEqual(events, ["open", 0, 1, 2, "close"])
                    self.assertEqual(stream.report.envelope.field("columnar_output_window_selected_policy"), "ordinary_ineligible")
                del first, stream, frame
                native_session = client._native_backend.session
                client.close()
                gc.collect()
                self.assertEqual(native_session.snapshot()["reserved_bytes"], 0)


if __name__ == "__main__":
    unittest.main()
