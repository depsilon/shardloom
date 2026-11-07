"""Variable tensor refusal, visibility, publication and ownership controls."""

from __future__ import annotations

import gc
import json
from pathlib import Path
import tempfile
import unittest

if __package__:
    from .test_native_binding import _load_native
    from .test_native_tensor_variable_binding import (
        METADATA_BYTES, _field, _fixture, _rows, _digest, _independent_schema_equal,
    )
else:
    from test_native_binding import _load_native
    from test_native_tensor_variable_binding import (
        METADATA_BYTES, _field, _fixture, _rows, _digest, _independent_schema_equal,
    )


class NativeVariableTensorControlsTests(unittest.TestCase):
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
        self.assertTrue(actual.schema.equals(expected.schema, check_metadata=True),
                        (actual.schema, expected.schema))
        self.assertEqual(_digest(_rows(actual)), _digest(_rows(expected)))

    def test_variable_tensor_hidden_shapes_are_unobserved_before_child_mask_normalization(self):
        import pyarrow as pa
        context = self.context()
        data = pa.list_(pa.field("item", pa.int32(), nullable=False))
        shape = pa.list_(pa.field("item", pa.int32(), nullable=False), 2)
        storage = pa.struct([pa.field("data", data, nullable=False), pa.field("shape", shape, nullable=False)])
        field = _field(pa, storage, b'{}')
        # Null storage beneath an absent tensor is legal, even for nonnullable
        # child declarations. Provider mask normalization must follow validation.
        tensor = pa.StructArray.from_arrays([
            pa.array([None, [1, 2]], data), pa.array([None, [1, 2]], shape)],
            fields=list(storage), mask=pa.array([True, False]))
        table = pa.Table.from_arrays([tensor], schema=pa.schema([field]))
        result = context.from_arrow_table(table).collect(check=True)
        self.assertEqual(result.python_objects, ({"tensor": None}, {"tensor": {"data": [1, 2], "shape": [1, 2]}}))
        self.assert_table(result.to_arrow(), table)
        # A present tensor with a negative dimension is hidden by each outer
        # container in turn. Removing that outer null must expose the error.
        bad = pa.array([{"data": [9], "shape": [-1, 1]}, {"data": [1, 2], "shape": [1, 2]}], storage)
        for visible in (False, True):
            mask = pa.array([not visible, False])
            parents = [pa.StructArray.from_arrays([bad], fields=[field], mask=mask),
                pa.ListArray.from_arrays(pa.array([0, 1, 2], pa.int32()), bad,
                    type=pa.list_(field.with_name("item")), mask=mask),
                pa.FixedSizeListArray.from_arrays(bad, type=pa.list_(field.with_name("item"), 1), mask=mask)]
            for parent in parents:
                containing = pa.table({"parent": parent})
                if visible:
                    with self.assertRaisesRegex(Exception, "nonnegative"):
                        context.from_arrow_table(containing)
                else:
                    self.assert_table(context.from_arrow_table(containing).collect(check=True).to_arrow(), containing)
                sliced = containing.slice(1, 1)
                self.assert_table(context.from_arrow_table(sliced).collect(check=True).to_arrow(), sliced)

    def test_variable_tensor_bad_shapes_metadata_and_denied_grants_release(self):
        import pyarrow as pa
        from shardloom import types as t
        from shardloom._result_schema import _arrow_field
        session = self.native.Session(self.native.ExecutionResources(32 << 20, 1), float_policy="extended")
        self.addCleanup(session.close)
        # Nullable structural fields allow us to prove value-level rejection.
        storage = pa.struct([pa.field("data", pa.list_(pa.int32())),
                             pa.field("shape", pa.list_(pa.int32(), 2))])
        field = _field(pa, storage, b'{"uniform_shape":[null,2]}')
        for value in ({"data": None, "shape": [0, 2]}, {"data": [], "shape": None},
                      {"data": [], "shape": [0, None]}, {"data": [], "shape": [0, -1]},
                      {"data": [], "shape": [0, 3]}, {"data": [1], "shape": [1, 2]}):
            invalid = pa.Table.from_arrays([pa.array([value], storage)], schema=pa.schema([field]))
            with self.subTest(value=value), self.assertRaises(Exception):
                session.from_arrow(invalid)
            self.assertEqual(session.snapshot()["reserved_bytes"], 0)
        plain = pa.array([{"data": [1, 2], "shape": [1, 2]}], storage)
        for raw in (b'{"uniform_shape":[2]}', b'{"uniform_shape":[null,-2]}',
                    b'{"uniform_shape":[null,2147483648]}', b'{"permutation":[0,0]}',
                    b'{"uniform_shape":[],"uniform_shape":[]}', b'{"permutations":[1,0]}',
                    b'{"shape":[1,2]}', b'{"dim_names":["x","y"],"extra":0}'):
            invalid = pa.Table.from_arrays([plain], schema=pa.schema([_field(pa, storage, raw)]))
            with self.subTest(metadata=raw), self.assertRaises(Exception):
                session.from_arrow(invalid)
            self.assertEqual(session.snapshot()["reserved_bytes"], 0)
        for raw in (None, b'', b' { } '):
            metadata = {b"ARROW:extension:name": b"arrow.variable_shape_tensor"}
            if raw is not None:
                metadata[b"ARROW:extension:metadata"] = raw
            table = pa.Table.from_arrays([plain], schema=pa.schema([field.with_metadata(metadata)]))
            source = session.from_arrow(table)
            del source
            gc.collect()
            self.assertEqual(session.snapshot()["reserved_bytes"], 0)
        huge = _arrow_field("tensor", t.variable_shape_tensor(t.int32(), 1, dim_names=["x" * 16384]), pa)
        table = pa.Table.from_arrays([pa.array([{"data": [1], "shape": [1]}], huge.type)], schema=pa.schema([huge]))
        with self.assertRaisesRegex(Exception, "memory"):
            session.from_arrow(table, resources=self.native.ExecutionResources(1 << 20, 1))
        self.assertEqual(session.snapshot()["reserved_bytes"], 0)
        source = session.from_arrow(table)
        self.assertGreater(session.snapshot()["reserved_bytes"], 16384)
        del source
        gc.collect()
        self.assertEqual(session.snapshot()["reserved_bytes"], 0)
        strict = self.native.Session(self.native.ExecutionResources(32 << 20, 1))
        self.addCleanup(strict.close)
        with self.assertRaisesRegex(Exception, "finite|float_policy"):
            strict.from_arrow(_fixture(3).slice(1, 2))
        self.assertEqual(strict.snapshot()["reserved_bytes"], 0)

    def test_variable_tensor_unsupported_wrappers_keys_and_outputs_refuse(self):
        import pyarrow as pa
        from shardloom.errors import ShardLoomCommandError
        context = self.context()
        table = _fixture(3)
        tensor = table.column("tensor").combine_chunks()
        for storage, field in [
            (tensor, table.schema.field("tensor").with_metadata({b"ARROW:extension:name": b"foreign.tensor"})),
            (pa.DictionaryArray.from_arrays(pa.array([0, 1, 2], pa.int8()), tensor),
             _field(pa, pa.dictionary(pa.int8(), tensor.type))),
        ]:
            with self.assertRaises(Exception):
                context.from_arrow_table(pa.Table.from_arrays([storage], schema=pa.schema([field])))
        nested = pa.StructArray.from_arrays([tensor], fields=[table.schema.field("tensor")])
        frame = context.from_arrow_table(table.append_column("nested", nested))
        for query in (
            "SELECT tensor FROM tensors ORDER BY tensor",
            "SELECT tensor, COUNT(*) AS n FROM tensors GROUP BY tensor",
            "SELECT DISTINCT tensor FROM tensors",
            "SELECT id FROM tensors WHERE tensor = tensor",
            "SELECT l.id AS id FROM tensors AS l JOIN tensors AS r ON l.tensor = r.tensor",
            "SELECT id, ROW_NUMBER() OVER (PARTITION BY tensor ORDER BY id) AS n FROM tensors",
            "SELECT nested FROM tensors ORDER BY nested",
        ):
            with self.subTest(query=query), self.assertRaisesRegex(Exception, "tensor values are payload-only"):
                context.sql(query, relations={"tensors": frame}).collect(check=True)
        with self.assertRaisesRegex(Exception, "arithmetic requires integer, F16/F32/F64 or admitted decimal operands"):
            context.sql("SELECT tensor + tensor AS total FROM tensors", relations={"tensors": frame}).collect(check=True)
        with tempfile.TemporaryDirectory(prefix="shardloom-variable-refusal-") as directory:
            root = Path(directory)
            for name in ("csv", "json", "jsonl", "avro", "orc"):
                with self.subTest(format=name), self.assertRaisesRegex(ShardLoomCommandError, "tensor metadata"):
                    getattr(frame, "write_" + name)(root / ("denied." + name), check=True)
                self.assertEqual(list(root.iterdir()), [])

    def test_variable_tensor_streaming_writes_and_partial_failure_cleanup(self):
        import pyarrow as pa
        import pyarrow.parquet as parquet
        from shardloom.errors import ShardLoomCommandError
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
        with tempfile.TemporaryDirectory(prefix="shardloom-variable-stream-") as directory:
            root = Path(directory)
            with self.assertRaisesRegex(ShardLoomCommandError, "tensor metadata"):
                frame.fanout({"vortex": root / "denied.vortex", "orc": root / "denied.orc"})
            self.assertEqual(events, [])
            self.assertEqual(list(root.iterdir()), [])
            targets = {name: root / ("tensor." + name) for name in ("vortex", "arrow_ipc", "parquet")}
            frame.fanout(targets)
            self.assertEqual(events, ["opened", "ended", "closed"])
            for name, path in targets.items():
                self.assert_table(getattr(context, "read_" + name)(path).collect(check=True).to_arrow(), table)
            with pa.ipc.open_file(targets["arrow_ipc"]) as reader:
                independent_arrow = reader.read_all()
            self.assertTrue(_independent_schema_equal(independent_arrow.schema, table.schema))
            self.assertEqual(_digest(_rows(independent_arrow)), _digest(_rows(table)))
            independent = root / "independent.parquet"
            parquet.write_table(table, independent, use_dictionary=False)
            self.assert_table(context.read_parquet(independent).collect(check=True).to_arrow(), table)
            failure = LookupError("variable tensor producer stopped after a complete batch")

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
            self.assertEqual(set(root.iterdir()), set(targets.values()) | {independent})

    def test_variable_tensor_retained_readers_cancel_source_change_and_last_owner(self):
        import pyarrow as pa
        from shardloom._native_backend import NativeResultBatch
        table = _fixture(13).slice(1, 11)
        session = self.native.Session(self.native.ExecutionResources(128 << 20, 1), float_policy="extended")
        self.addCleanup(session.close)
        source = session.from_arrow(pa.RecordBatchReader.from_batches(table.schema, table.to_batches(max_chunksize=3)))
        plan = session.sql("SELECT * FROM 'memory://tensors'", sources=[("memory://tensors", source)])
        result = plan.collect()
        plan.release_prepared()
        reserved = session.snapshot()["reserved_bytes"]
        self.assertGreater(reserved, 0)
        with self.assertRaisesRegex(Exception, "memory"):
            plan.collect(resources=self.native.ExecutionResources(reserved - 1, 1))
        self.assertEqual(session.snapshot()["reserved_bytes"], reserved)
        with tempfile.TemporaryDirectory(prefix="shardloom-variable-generation-") as directory:
            path = Path(directory) / "source.vortex"
            plan.write(path, "write_vortex")
            file_plan = session.sql(f"SELECT * FROM '{path}'", files=[(str(path), "vortex", None)])
            retained_file = file_plan.collect()
            path.write_bytes(b"changed source generation")
            with self.assertRaisesRegex(Exception, "prepared source changed"):
                file_plan.collect()
            retained_table = pa.RecordBatchReader.from_stream(retained_file).read_all()
            self.assertEqual(_digest(_rows(retained_table)), _digest(_rows(table)))
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

    def test_variable_tensor_cli_json_and_degenerate_shapes_persist(self):
        from shardloom import types as t
        from shardloom._input_schema import normalize_schema
        from shardloom._result_schema import schema_fields, python_rows
        context = self.context()
        schema = {"id": t.int64(), "tensor": t.variable_shape_tensor(t.int16(), 2,
                    dim_names=["rows", "cols"], permutation=[1, 0]),
                  "scalar": t.variable_shape_tensor(t.int64(nullable=False), 0,
                    dim_names=[], permutation=[], uniform_shape=[])}
        rows = [{"id": 1, "tensor": {"data": [1, -2, None, 4], "shape": [2, 2]},
                 "scalar": {"data": [-(1 << 63)], "shape": []}},
                {"id": 2, "tensor": None, "scalar": None},
                {"id": 3, "tensor": {"data": [], "shape": [2, 0]},
                 "scalar": {"data": [(1 << 63) - 1], "shape": []}}]
        session = self.native.Session(self.native.ExecutionResources(32 << 20, 1))
        self.addCleanup(session.close)
        with tempfile.TemporaryDirectory(prefix="shardloom-variable-degenerate-") as directory:
            root = Path(directory)
            for name in ("vortex", "arrow_ipc", "parquet"):
                with self.subTest(format=name):
                    path = root / ("values." + name)
                    getattr(context.from_rows(rows, schema=schema), "write_" + name)(path, check=True)
                    result = getattr(context, "read_" + name)(path).collect(check=True)
                    self.assertEqual(result.result_schema, normalize_schema(schema))
                    self.assertEqual(result.python_objects, tuple(rows))
                    self.assertEqual(result.to_arrow().num_rows, 3)
            path = root / "values.vortex"
            envelope = json.loads(session.command(["run", "cli", "--input", str(path),
                "--input-format", "vortex", "--request", "collect", "--bounded", "true",
                "--execution-policy", "native_vortex", "--native-vortex-operation-family",
                "filter_project_limit", "--vortex-primitive", "project", "--vortex-columns", "id,tensor,scalar"]))
            self.assertEqual(envelope["status"], "success", envelope)
            self.assertFalse(envelope["fallback"]["attempted"])
            fields = {field["key"]: field["value"] for field in
                      [*envelope.get("fields", []), *envelope.get("result", {}).get("fields", [])]}
            declared = schema_fields(fields["result_schema_json"], fields["result_schema_format"])
            self.assertEqual(declared, normalize_schema(schema))
            self.assertEqual(python_rows(json.loads(fields["result_values_json"]), declared), rows)
            huge = t.variable_shape_tensor(t.int32(), 4)
            value = {"data": [], "shape": [(1 << 31) - 1] * 3 + [0]}
            path = root / "zero-after-overflow.vortex"
            context.from_rows([{"tensor": value}, {"tensor": None}], schema={"tensor": huge}).write_vortex(path, check=True)
            result = context.read_vortex(path).collect(check=True)
            self.assertEqual(result.result_schema, (("tensor", huge),))
            self.assertEqual(result.python_objects, ({"tensor": value}, {"tensor": None}))


if __name__ == "__main__":
    unittest.main()
