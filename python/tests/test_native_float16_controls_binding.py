"""Float16 lifecycle resource, visibility and failed-publication controls."""

from __future__ import annotations

import gc
from pathlib import Path
import tempfile
import unittest

if __package__:
    from .test_native_binding import _load_native
    from .float16_fixture import _fixture, _rows, _digest, _independent_schema_equal, _wide
else:
    from test_native_binding import _load_native
    from float16_fixture import _fixture, _rows, _digest, _independent_schema_equal, _wide


class NativeFloat16ControlsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()

    def context(self):
        import shardloom as sl
        context = sl.context(transport="native", memory_bytes=128 << 20,
                             max_parallelism=1, float_policy="extended")
        self.addCleanup(context.close)
        return context

    def assert_table(self, actual, expected):
        self.assertTrue(_independent_schema_equal(actual.schema, expected.schema),
                        (actual.schema, expected.schema))
        self.assertEqual(_digest(_rows(actual)), _digest(_rows(expected)))

    def test_float16_streaming_fanout_and_failed_publication(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        context = self.context()
        table = _fixture(39).slice(1, 33)
        schema = dict(context.from_arrow_table(table).collect(check=True).result_schema)
        events = []

        def producer():
            events.append("opened")
            try:
                yield table.slice(0, 0)
                yield from table.to_batches(max_chunksize=7)
                events.append("ended")
            finally:
                events.append("closed")

        frame = context.from_batches(producer, schema=schema, streaming=True)
        with tempfile.TemporaryDirectory(prefix="shardloom-half-stream-") as directory:
            root = Path(directory)
            with self.assertRaisesRegex(Exception, "embedding|tensor"):
                frame.fanout({"vortex": root / "denied.vortex", "orc": root / "denied.orc"})
            self.assertEqual(events, [])
            self.assertEqual(list(root.iterdir()), [])
            paths = {name: root / ("mixed." + name) for name in ("vortex", "arrow_ipc", "parquet")}
            frame.fanout(paths)
            self.assertEqual(events, ["opened", "ended", "closed"])
            for name, path in paths.items():
                self.assert_table(getattr(context, "read_" + name)(path).collect(check=True).to_arrow(), table)
            with pa.ipc.open_file(paths["arrow_ipc"]) as reader:
                independent = reader.read_all()
            self.assertTrue(_independent_schema_equal(independent.schema, table.schema))
            self.assertEqual(_digest(_rows(independent)), _digest(_rows(table)))
            external = root / "independent.parquet"
            pq.write_table(table, external, use_dictionary=False)
            self.assert_table(context.read_parquet(external).collect(check=True).to_arrow(), table)
            failure = LookupError("Float16 producer stopped after a complete batch")

            def broken():
                try:
                    yield table
                    raise failure
                finally:
                    events.append("failure closed")

            with self.assertRaises(LookupError) as raised:
                context.from_batches(broken, schema=schema, streaming=True).write_vortex(root / "failed.vortex", check=True)
            self.assertIs(raised.exception, failure)
            self.assertEqual(events[-1], "failure closed")
            self.assertEqual(set(root.iterdir()), set(paths.values()) | {external})

    def test_float16_retained_views_denied_shrink_cancel_and_source_change(self):
        import pyarrow as pa
        from shardloom._native_backend import NativeResultBatch
        table = _fixture(13).slice(1, 11)
        session = self.native.Session(self.native.ExecutionResources(128 << 20, 1), float_policy="extended")
        self.addCleanup(session.close)
        with self.assertRaisesRegex(Exception, "memory"):
            session.from_arrow(table, resources=self.native.ExecutionResources(1024, 1))
        self.assertEqual(session.snapshot()["reserved_bytes"], 0)
        source = session.from_arrow(pa.RecordBatchReader.from_batches(table.schema, table.to_batches(max_chunksize=3)))
        plan = session.sql("SELECT * FROM 'memory://half'", sources=[("memory://half", source)])
        result = plan.collect()
        plan.release_prepared()
        reserved = session.snapshot()["reserved_bytes"]
        self.assertGreater(reserved, 0)
        with self.assertRaisesRegex(Exception, "memory"):
            plan.collect(resources=self.native.ExecutionResources(reserved - 1, 1))
        self.assertEqual(session.snapshot()["reserved_bytes"], reserved)
        with tempfile.TemporaryDirectory(prefix="shardloom-half-generation-") as directory:
            path = Path(directory) / "source.vortex"
            plan.write(path, "write_vortex")
            file_plan = session.sql(f"SELECT * FROM '{path}'", files=[(str(path), "vortex", None)])
            retained_file = file_plan.collect()
            path.write_bytes(b"changed source generation")
            with self.assertRaisesRegex(Exception, "prepared source changed"):
                file_plan.collect()
            retained_table = pa.RecordBatchReader.from_stream(retained_file).read_all()
            self.assert_table(retained_table, table)
        stream = plan.iter_batches()
        first = next(stream)
        stream.close()
        self.assertIsNone(stream.report)
        cancelled = plan.iter_batches()
        kept = next(cancelled)
        session.cancel()
        with self.assertRaisesRegex(Exception, "cancelled"):
            next(cancelled)
        cancelled.close()
        self.assertIsNone(cancelled.report)
        del source, plan, result, file_plan, retained_file, retained_table, stream, cancelled
        session.close()
        self.assertGreater(session.snapshot()["reserved_bytes"], 0)
        for batch in (first, kept):
            retained = NativeResultBatch(batch, 0).to_arrow()
            self.assert_table(retained, table.slice(0, retained.num_rows))
        del batch, retained, first, kept
        gc.collect()
        self.assertEqual(session.snapshot()["reserved_bytes"], 0)

    def test_float16_zero_width_and_empty_recursive_types_keep_metadata(self):
        from shardloom import types as t
        from shardloom._input_schema import normalize_schema
        context = self.context()
        schema = {"scalar": t.fixed_shape_tensor(t.float16(nullable=False), [], permutation=[], dim_names=[]),
                  "empty": t.fixed_shape_tensor(t.float16(nullable=False), [3, 0]),
                  "variable": t.variable_shape_tensor(t.float16(nullable=False), 4),
                  "vectors": t.list_(t.fixed_size_list(t.float16(), 3)),
                  "embeddings": t.list_(t.embedding(t.float16(), 3))}
        rows = [{"scalar": [_wide(0x8000)], "empty": [],
                 "variable": {"data": [], "shape": [(1 << 31) - 1] * 3 + [0]},
                 "vectors": [None, [1.0, None, -0.0]], "embeddings": []},
                {"scalar": None, "empty": None, "variable": None, "vectors": None, "embeddings": None}]
        source = context.from_rows(rows, schema=schema)
        expected = source.collect(check=True).to_arrow()
        self.assertEqual(source.collect(check=True).python_objects, tuple(rows))
        self.assertEqual(source.collect(check=True).result_schema, normalize_schema(schema))
        with tempfile.TemporaryDirectory(prefix="shardloom-half-empty-") as directory:
            for count in (2, 0):
                for name in ("vortex", "arrow_ipc", "parquet"):
                    target = expected.slice(0, count)
                    path = Path(directory) / f"degenerate-{count}.{name}"
                    getattr(context.from_arrow_table(target), "write_" + name)(path, check=True)
                    self.assert_table(getattr(context, "read_" + name)(path).collect(check=True).to_arrow(), target)
            empty = _fixture(0)
            for name in ("vortex", "arrow_ipc", "parquet"):
                path = Path(directory) / ("empty-mixed." + name)
                getattr(context.from_arrow_table(empty), "write_" + name)(path, check=True)
                self.assert_table(getattr(context, "read_" + name)(path).collect(check=True).to_arrow(), empty)

    def test_float16_unsupported_metadata_outputs_fail_before_publication(self):
        context = self.context()
        source = context.from_arrow_table(_fixture(3, finite=True))
        with tempfile.TemporaryDirectory(prefix="shardloom-half-refusal-") as directory:
            root = Path(directory)
            for name in ("csv", "json", "jsonl", "avro", "orc"):
                with self.subTest(format=name), self.assertRaisesRegex(Exception, "embedding|tensor"):
                    getattr(source, "write_" + name)(root / ("denied." + name), check=True)
                self.assertEqual(list(root.iterdir()), [])
            scalar = source.select("id", "x")
            for name in ("avro", "orc"):
                with self.subTest(format=name), self.assertRaisesRegex(Exception, "Float16|float16|f16"):
                    getattr(scalar, "write_" + name)(root / ("scalar." + name), check=True)
                self.assertEqual(list(root.iterdir()), [])
            # Schema rejection precedes producer invocation for scalar and
            # recursively nested Float16, including a multi-output action.
            for selected in ("x", "values", "vector", "vectors"):
                table = _fixture(3, finite=True).select([selected])
                declaration = dict(context.from_arrow_table(table).collect(check=True).result_schema)
                events = []

                def producer():
                    events.append("opened")
                    yield table

                frame = context.from_batches(producer, schema=declaration, streaming=True)
                for name in ("avro", "orc"):
                    with self.subTest(column=selected, format=name), self.assertRaisesRegex(Exception, "Float16"):
                        frame.fanout({"vortex": root / "denied.vortex", name: root / ("denied." + name)})
                    self.assertEqual(events, [])
                    self.assertEqual(list(root.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
