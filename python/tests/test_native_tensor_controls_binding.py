"""Fixed tensor adapter fidelity, refusals and native ownership controls."""

from __future__ import annotations

import gc
import json
from pathlib import Path
import tempfile
import unittest

if __package__:
    from .test_native_binding import _load_native
    from .test_native_tensor_binding import METADATA_BYTES, _field, _fixture, _rows, _digest
else:
    from test_native_binding import _load_native
    from test_native_tensor_binding import METADATA_BYTES, _field, _fixture, _rows, _digest


class NativeTensorControlsTests(unittest.TestCase):
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

    def test_fixed_tensor_streaming_capable_formats_and_failed_publication(self):
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
        with tempfile.TemporaryDirectory(prefix="shardloom-tensor-formats-") as directory:
            root = Path(directory)
            with self.assertRaisesRegex(ShardLoomCommandError, "tensor metadata"):
                frame.fanout({"vortex": root / "denied.vortex", "orc": root / "denied.orc"})
            self.assertEqual(events, [])
            self.assertEqual(list(root.iterdir()), [])
            targets = {name: root / ("tensors." + name) for name in ("vortex", "arrow_ipc", "parquet")}
            frame.fanout(targets)
            self.assertEqual(events, ["opened", "ended", "closed"])
            for name, path in targets.items():
                with self.subTest(format=name):
                    self.assert_table(getattr(context, "read_" + name)(path).collect(check=True).to_arrow(), table)
                    if name == "arrow_ipc":
                        with pa.ipc.open_file(path) as reader:
                            self.assertEqual(_digest(_rows(reader.read_all())), _digest(_rows(table)))
            # Independent Parquet production includes null tensor parents.
            # PyArrow cannot independently read those fixed-list null parents;
            # its readback control therefore selects present parents explicitly.
            independent = root / "independent.parquet"
            parquet.write_table(table, independent, use_dictionary=False)
            self.assert_table(context.read_parquet(independent).collect(check=True).to_arrow(), table)
            present = root / "present.parquet"
            context.from_arrow_table(table).where("id % 17 <> 0").write_parquet(present, check=True)
            expected = [row for row in _rows(table) if row["id"] % 17]
            self.assertEqual(_digest(_rows(parquet.read_table(present))), _digest(expected))

            failure = LookupError("tensor producer failure after a complete batch")

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
            self.assertEqual(set(root.iterdir()), set(targets.values()) | {independent, present})

    def test_fixed_tensor_unknown_metadata_strict_floats_and_denied_memory_release(self):
        import pyarrow as pa
        strict = self.native.Session(self.native.ExecutionResources(32 << 20, 1))
        self.addCleanup(strict.close)
        table = _fixture(3).slice(1, 2)
        with self.assertRaisesRegex(Exception, "finite|float_policy"):
            strict.from_arrow(table)
        self.assertEqual(strict.snapshot()["reserved_bytes"], 0)
        session = self.native.Session(self.native.ExecutionResources(32 << 20, 1), float_policy="extended")
        self.addCleanup(session.close)
        for metadata in (b'{"shape":[2,3,5]}', b'{"shape":[2,3,4],"shape":[2,3,4]}',
                         b'{"shape":[2,3,4],"permutation":[0,0,2]}', b'{"shape":[2,3,4],"unknown":null}',
                         b'{"shape":[-2,3,4]}'):
            field = _field(pa, "tensor", table.column("tensor").type, metadata=metadata)
            invalid = pa.Table.from_arrays([table.column("tensor")], schema=pa.schema([field]))
            with self.subTest(metadata=metadata), self.assertRaises(Exception):
                session.from_arrow(invalid)
            self.assertEqual(session.snapshot()["reserved_bytes"], 0)
        foreign = table.schema.field("tensor").with_metadata({
            b"ARROW:extension:name": b"unrecognized.tensor.v1", b"ARROW:extension:metadata": METADATA_BYTES})
        with self.assertRaises(Exception):
            session.from_arrow(pa.Table.from_arrays([table.column("tensor")], schema=pa.schema([foreign])))
        self.assertEqual(session.snapshot()["reserved_bytes"], 0)
        with self.assertRaisesRegex(Exception, "memory"):
            session.from_arrow(table, resources=self.native.ExecutionResources(4096, 1))
        self.assertEqual(session.snapshot()["reserved_bytes"], 0)

    def test_fixed_tensor_metadata_grants_and_dictionary_wrappers_are_explicit(self):
        import pyarrow as pa
        session = self.native.Session(self.native.ExecutionResources(16 << 20, 1))
        self.addCleanup(session.close)
        storage = pa.list_(pa.field("item", pa.int32(), nullable=False), 1)
        raw = json.dumps({"shape": [1], "dim_names": ["x" * 16384]},
                         sort_keys=True, separators=(",", ":")).encode()
        table = pa.Table.from_arrays([pa.array([[17]], type=storage)],
                                    schema=pa.schema([_field(pa, "tensor", storage, metadata=raw)]))
        with self.assertRaisesRegex(Exception, "memory"):
            session.from_arrow(table, resources=self.native.ExecutionResources(1 << 20, 1))
        self.assertEqual(session.snapshot()["reserved_bytes"], 0)
        source = session.from_arrow(table)
        self.assertGreater(session.snapshot()["reserved_bytes"], len(raw))
        del source
        gc.collect()
        self.assertEqual(session.snapshot()["reserved_bytes"], 0)
        canonical = pa.fixed_shape_tensor(pa.int32(), [1])
        values = pa.ExtensionArray.from_storage(canonical, pa.array([[17]], type=canonical.storage_type))
        dictionary = pa.DictionaryArray.from_arrays(pa.array([0, 0], type=pa.int8()), values)
        with self.assertRaises(Exception):
            session.from_arrow(pa.table({"tensor": dictionary}))
        self.assertEqual(session.snapshot()["reserved_bytes"], 0)
        oversized = json.dumps({"shape": [1], "dim_names": ["x" * (8 << 20)]}).encode()
        invalid = pa.Table.from_arrays([pa.array([[17]], type=storage)],
            schema=pa.schema([_field(pa, "tensor", storage, metadata=oversized)]))
        with self.assertRaises(Exception):
            session.from_arrow(invalid)
        self.assertEqual(session.snapshot()["reserved_bytes"], 0)

    def test_fixed_tensor_key_and_metadata_losing_outputs_refuse_before_publication(self):
        import pyarrow as pa
        from shardloom.errors import ShardLoomCommandError
        context = self.context()
        table = _fixture(3)
        nested = pa.StructArray.from_arrays([table.column("tensor").combine_chunks()],
                                            fields=[table.schema.field("tensor")])
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
        with self.assertRaisesRegex(Exception, "arithmetic requires integer, F32/F64 or admitted decimal operands"):
            context.sql("SELECT tensor + tensor AS total FROM tensors", relations={"tensors": frame}).collect(check=True)
        with tempfile.TemporaryDirectory(prefix="shardloom-tensor-refusal-") as directory:
            root = Path(directory)
            for name in ("csv", "json", "jsonl", "avro", "orc"):
                with self.subTest(format=name), self.assertRaisesRegex(ShardLoomCommandError, "tensor metadata"):
                    getattr(frame, "write_" + name)(root / ("denied." + name), check=True)
                self.assertEqual(list(root.iterdir()), [])

    def test_fixed_tensor_retained_readers_survive_cancel_source_change_and_owner_drop(self):
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
        with tempfile.TemporaryDirectory(prefix="shardloom-tensor-generation-") as directory:
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

    def test_fixed_tensor_cli_json_dispatch_preserves_schema_and_values(self):
        from shardloom import types as t
        from shardloom._input_schema import normalize_schema
        from shardloom._result_schema import schema_fields, python_rows
        context = self.context()
        schema = {"id": t.int64(), "tensor": t.fixed_shape_tensor(t.int16(), [2, 2],
                    dim_names=["rows", "cols"], permutation=[1, 0])}
        expected = [{"id": 1, "tensor": [1, -2, None, 4]}, {"id": 2, "tensor": None}]
        session = self.native.Session(self.native.ExecutionResources(32 << 20, 1))
        self.addCleanup(session.close)
        with tempfile.TemporaryDirectory(prefix="shardloom-tensor-command-") as directory:
            path = Path(directory) / "values.vortex"
            context.from_rows(expected, schema=schema).write_vortex(path, check=True)
            envelope = json.loads(session.command(["run", "cli", "--input", str(path),
                "--input-format", "vortex", "--request", "collect", "--bounded", "true",
                "--execution-policy", "native_vortex", "--native-vortex-operation-family",
                "filter_project_limit", "--vortex-primitive", "project", "--vortex-columns", "id,tensor"]))
            self.assertEqual(envelope["status"], "success", envelope)
            self.assertFalse(envelope["fallback"]["attempted"])
            fields = {field["key"]: field["value"] for field in
                      [*envelope.get("fields", []), *envelope.get("result", {}).get("fields", [])]}
            declared = schema_fields(fields["result_schema_json"], fields["result_schema_format"])
            self.assertEqual(declared, normalize_schema(schema))
            self.assertEqual(python_rows(json.loads(fields["result_values_json"]), declared), expected)

    def test_fixed_tensor_scalar_and_zero_dimensions_persist_complete_rows(self):
        from shardloom import types as t
        from shardloom._input_schema import normalize_schema
        context = self.context()
        schema = {"id": t.int64(),
            "scalar": t.fixed_shape_tensor(t.int64(nullable=False), [], dim_names=[], permutation=[]),
            "empty": t.fixed_shape_tensor(t.boolean(nullable=False), [2, 0, 3])}
        rows = [{"id": 1, "scalar": [-(1 << 63)], "empty": []},
                {"id": 2, "scalar": None, "empty": None},
                {"id": 3, "scalar": [(1 << 63) - 1], "empty": []}]
        frame = context.from_rows(rows, schema=schema).sort("id", descending=True)
        with tempfile.TemporaryDirectory(prefix="shardloom-tensor-degenerate-") as directory:
            root = Path(directory)
            for name in ("vortex", "arrow_ipc", "parquet"):
                with self.subTest(format=name):
                    path = root / ("values." + name)
                    getattr(frame, "write_" + name)(path, check=True)
                    result = getattr(context, "read_" + name)(path).collect(check=True)
                    self.assertEqual(result.result_schema, normalize_schema(schema))
                    self.assertEqual(result.python_objects, tuple(reversed(rows)))
                    self.assertEqual(result.to_arrow().num_rows, 3)
                    self.assertFalse(result.to_arrow().schema.field("scalar").type.value_field.nullable)
            huge = t.fixed_shape_tensor(t.int32(), [(1 << 63) - 1, (1 << 63) - 1, 0])
            path = root / "zero-product.vortex"
            context.from_rows([{"tensor": []}, {"tensor": None}], schema={"tensor": huge}).write_vortex(path, check=True)
            result = context.read_vortex(path).collect(check=True)
            self.assertEqual(result.result_schema, (("tensor", huge),))
            self.assertEqual(result.python_objects, ({"tensor": []}, {"tensor": None}))


if __name__ == "__main__":
    unittest.main()
