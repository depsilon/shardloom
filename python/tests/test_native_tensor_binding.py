"""Complete fixed tensor values and metadata across native workflow boundaries."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

if __package__:
    from .test_native_binding import _load_native
else:
    from test_native_binding import _load_native


METADATA = {"shape": [2, 3, 4], "permutation": [2, 0, 1], "dim_names": ["行", "height", "channel"]}
METADATA_BYTES = json.dumps(METADATA, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def _field(pa, name, storage, nullable=True, metadata=METADATA_BYTES):
    return pa.field(name, storage, nullable=nullable, metadata={
        b"ARROW:extension:name": b"arrow.fixed_shape_tensor",
        b"ARROW:extension:metadata": metadata})


def _fixture(count):
    import pyarrow as pa
    special = [0xff800042, 0x80000000, 0, 0x7f800000, 0xff800000, 0x3fc00000, 0x7fc01234, 1]
    bits = [special[index % 24] if index % 24 < len(special) else 0x3f800000 + index * 31
            for index in range(count * 24)]
    raw = b"".join(struct.pack("<I", bit) for bit in bits)
    validity = bytearray((len(bits) + 7) // 8)
    for index in range(len(bits)):
        if index % 37:
            validity[index // 8] |= 1 << (index % 8)
    values = pa.Array.from_buffers(pa.float32(), len(bits), [pa.py_buffer(validity), pa.py_buffer(raw)])
    storage = pa.list_(pa.field("item", pa.float32()), 24)
    tensor = pa.FixedSizeListArray.from_arrays(values, type=storage,
        mask=pa.array([row % 17 == 0 for row in range(count)]))
    return pa.Table.from_arrays([
        pa.array(range(count), pa.int64()), tensor,
        pa.array([b"" if row % 7 == 0 else bytes((row % 251, 0, 255)) * 192
                  for row in range(count)], pa.binary()),
    ], schema=pa.schema([pa.field("id", pa.int64()), _field(pa, "tensor", storage),
                         pa.field("content", pa.binary())]))


def _value(array, row):
    import pyarrow as pa
    if isinstance(array.type, pa.BaseExtensionType):
        return _value(array.storage, row)
    if not array[row].is_valid:
        return None
    dtype = array.type
    if pa.types.is_floating(dtype):
        code, width = {16: ("H", 2), 32: ("I", 4), 64: ("Q", 8)}[dtype.bit_width]
        return (str(dtype), struct.unpack_from("<" + code, array.buffers()[1], (array.offset + row) * width)[0])
    if pa.types.is_fixed_size_list(dtype) or pa.types.is_list(dtype):
        child = array[row].values
        return [_value(child, index) for index in range(len(child))]
    if pa.types.is_struct(dtype):
        return {field.name: _value(array.field(index), row) for index, field in enumerate(dtype)}
    value = array[row].as_py()
    return ("bytes", value.hex()) if isinstance(value, bytes) else value


def _rows(table):
    return [{field.name: _value(column, row) for field, column in zip(batch.schema, batch.columns)}
            for batch in table.to_batches() for row in range(batch.num_rows)]


def _digest(rows):
    return hashlib.sha256(json.dumps(rows, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":")).encode()).hexdigest()


def _reopen_job(paths):
    _load_native()
    import shardloom as sl
    import pyarrow as pa
    result = []
    with sl.context(transport="native", memory_bytes=128 << 20, max_parallelism=1,
                    float_policy="extended") as context:
        for path in paths:
            frame = context.read_vortex(path)
            retained = frame.collect(check=True)
            table = retained.to_arrow()
            exported = Path(path).with_suffix(".arrow")
            frame.write_arrow_ipc(exported, check=True)
            with pa.ipc.open_file(exported) as reader:
                independent = reader.read_all()
            result.append({"rows": table.num_rows, "native": _digest(_rows(table)),
                "arrow": _digest(_rows(independent)),
                "metadata": table.schema.field("tensor").metadata[b"ARROW:extension:metadata"].decode(),
                "schema": [(name, repr(dtype)) for name, dtype in retained.result_schema]})
    return result


class NativeTensorBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()

    def context(self, policy="extended"):
        import shardloom as sl
        context = sl.context(transport="native", memory_bytes=128 << 20,
                             max_parallelism=1, float_policy=policy)
        self.addCleanup(context.close)
        return context

    def assert_table(self, actual, expected):
        self.assertTrue(actual.schema.equals(expected.schema, check_metadata=True),
                        (actual.schema, expected.schema))
        self.assertEqual(_digest(_rows(actual)), _digest(_rows(expected)))

    def test_fixed_tensor_object_intake_scalar_empty_and_exact_metadata(self):
        from shardloom import types as t
        from shardloom._input_schema import normalize_schema
        context = self.context()
        shape, names, permutation = [], [], []
        schema = {"id": t.int64(),
            "tensor": t.fixed_shape_tensor(t.float64(), [2, 2], permutation=[1, 0]),
            "scalar": t.fixed_shape_tensor(t.int32(nullable=False), shape,
                                            dim_names=names, permutation=permutation),
            "empty": t.fixed_shape_tensor(t.boolean(nullable=False), [3, 0])}
        rows = [{"id": 1, "tensor": [-0.0, None, float("inf"), 1.5], "scalar": [17], "empty": []},
                {"id": 2, "tensor": None, "scalar": None, "empty": None}]
        with patch("shardloom._input_schema.encode_cell", side_effect=AssertionError("JSON cells")):
            frame = context.from_rows(rows, schema=schema)
            result = frame.collect(check=True)
        self.assertEqual(result.result_schema, normalize_schema(schema))
        self.assertEqual(result.python_objects, tuple(rows))
        exported = result.to_arrow()
        scalar = exported.schema.field("scalar")
        self.assertFalse(scalar.type.value_field.nullable)
        self.assertEqual(scalar.metadata[b"ARROW:extension:metadata"], b'{"dim_names":[],"permutation":[],"shape":[]}')
        self.assertEqual(exported.column("empty").to_pylist(), [[], None])
        self.assertEqual(_digest(_rows(context.from_arrow_table(exported).collect(check=True).to_arrow())),
                         _digest(_rows(exported)))
        context.close()
        self.assertEqual(result.python_objects, tuple(rows))
        self.assert_table(result.to_arrow(), exported)

    def test_fixed_tensor_arrow_canonical_input_and_nested_slices(self):
        import pyarrow as pa
        from shardloom import types as t
        from shardloom._result_schema import _arrow_field
        context = self.context()
        tensor_type = pa.fixed_shape_tensor(pa.int32(), [2, 3, 4],
                                            dim_names=METADATA["dim_names"], permutation=[2, 0, 1])
        storage = pa.array([list(range(index, index + 24)) for index in range(5)], type=tensor_type.storage_type)
        tensor = pa.ExtensionArray.from_storage(tensor_type, storage)
        imported = context.from_arrow_table(pa.table({"tensor": tensor})).collect(check=True)
        self.assertEqual(dict(imported.result_schema[0][1].parameters[2]),
                         {key: tuple(value) for key, value in METADATA.items()})
        self.assertEqual(imported.python_objects[1]["tensor"], list(range(1, 25)))
        dtype = t.fixed_shape_tensor(t.int32(nullable=False), [2, 3, 4],
                                     dim_names=METADATA["dim_names"], permutation=[2, 0, 1])
        field = _arrow_field("tensor", dtype, pa)
        values = pa.array([list(range(index, index + 24)) for index in range(5)], type=field.type)
        nested = pa.StructArray.from_arrays([values], fields=[field],
                                            mask=pa.array([False, True, False, False, False]))
        table = pa.Table.from_arrays([nested], names=["record"]).slice(1, 3)
        self.assert_table(context.from_arrow_table(table).collect(check=True).to_arrow(), table)
        empty = table.slice(0, 0)
        self.assert_table(context.from_arrow_table(empty).collect(check=True).to_arrow(), empty)
        for constructor, value in [(t.boolean, True), (t.int8, -128), (t.int16, -32768),
            (t.int32, -(1 << 31)), (t.int64, -(1 << 63)), (t.uint8, 255), (t.uint16, 65535),
            (t.uint32, (1 << 32) - 1), (t.uint64, (1 << 64) - 1), (t.float32, -0.0), (t.float64, -0.0)]:
            with self.subTest(element=constructor.__name__):
                item = t.fixed_shape_tensor(constructor(), [1])
                result = context.from_rows([{"tensor": [value]}, {"tensor": [None]}, {"tensor": None}],
                                           schema={"tensor": item}).collect(check=True)
                self.assertEqual(result.python_objects, ({"tensor": [value]}, {"tensor": [None]}, {"tensor": None}))

    def test_fixed_tensor_nested_list_slices_and_arrow_restoration_share_numeric_buffers(self):
        import pyarrow as pa
        from shardloom import types as t
        from shardloom._result_schema import _arrow_field
        from shardloom._tensor import restore_arrow_schema
        context = self.context()
        tensor = t.fixed_shape_tensor(t.float32(nullable=False), [1, 2],
                                      dim_names=["row", "column"], permutation=[1, 0])
        fields = (("lists", t.list_(tensor)),
                  ("fixed", t.fixed_size_list(tensor, 2)),
                  ("records", t.list_(t.struct({"tensor": tensor}))))
        rows = [
            {"lists": [[1.0, -0.0]], "fixed": [[1.0, 2.0], None], "records": [{"tensor": [3.0, 4.0]}]},
            {"lists": None, "fixed": None, "records": None},
            {"lists": [None, [5.0, 6.0]], "fixed": [None, [7.0, 8.0]], "records": [None, {"tensor": None}]},
            {"lists": [], "fixed": [[9.0, 10.0], [11.0, 12.0]], "records": []},
            {"lists": [[13.0, 14.0]], "fixed": [[15.0, 16.0], None], "records": [{"tensor": [17.0, 18.0]}]},
        ]
        schema = pa.schema([_arrow_field(name, dtype, pa) for name, dtype in fields])
        table = pa.Table.from_pylist(rows, schema=schema).slice(1, 3)
        result = context.from_arrow_table(table).collect(check=True)
        self.assertEqual(result.result_schema, fields)
        self.assert_table(result.to_arrow(), table)
        # Exercise restoration directly on nonzero parent offsets, in addition
        # to the native result whose structural provider may compact a slice.
        restored = restore_arrow_schema(table, fields, pa)
        self.assert_table(restored, table)
        for name in ("lists", "fixed"):
            original_values = table.column(name).chunk(0).values.values
            restored_values = restored.column(name).chunk(0).values.values
            self.assertEqual(original_values.buffers()[1].address, restored_values.buffers()[1].address)
        original_values = table.column("records").chunk(0).values.field(0).values
        restored_values = restored.column("records").chunk(0).values.field(0).values
        self.assertEqual(original_values.buffers()[1].address, restored_values.buffers()[1].address)
        with tempfile.TemporaryDirectory(prefix="shardloom-tensor-nesting-") as directory:
            for count in (3, 0):
                expected = table.slice(0, count)
                for format_name in ("vortex", "arrow_ipc", "parquet"):
                    path = Path(directory) / (f"nested-{count}." + format_name)
                    getattr(context.from_arrow_table(expected), "write_" + format_name)(path, check=True)
                    self.assert_table(getattr(context, "read_" + format_name)(path).collect(check=True).to_arrow(), expected)

    def test_fixed_tensor_relational_spill_stream_reopen_and_context_drop(self):
        import pyarrow as pa
        context = self.context()
        table = _fixture(4101).slice(1, 4097)
        left = context.from_arrow_table(table)
        reports = [{"id": row, "tag": tag} for row in range(1, 4098)
                   for tag in ([1, 2] if row % 5 == 0 else [1] if row % 3 == 0 else [])]
        right = context.from_arrow_table(pa.Table.from_pylist(reports))
        selection = ["f.id AS id", "f.tensor AS tensor", "f.content AS content", "d.tag AS tag"]
        dataframe = left.where("id >= 3").join(right, on="id", how="left").select(*selection).sort(
            "id", "tag", descending=True, nulls="last")
        sql = context.sql("SELECT " + ", ".join(selection) + " FROM tensors AS f LEFT JOIN reports AS d "
                          "ON f.id = d.id WHERE f.id >= 3 ORDER BY f.id DESC, d.tag DESC NULLS LAST",
                          relations={"tensors": left, "reports": right})
        expected = [{**row, "tag": tag} for row in reversed(_rows(table)) if row["id"] >= 3
                    for tag in ([2, 1] if row["id"] % 5 == 0 else [1] if row["id"] % 3 == 0 else [None])]
        with tempfile.TemporaryDirectory(prefix="shardloom-tensor-lifecycle-") as directory:
            root = Path(directory)
            workspace = root / "spill"
            workspace.mkdir()
            spill = {"workspace": str(workspace), "quota_bytes": 64 << 20, "buffer_bytes": 1 << 20}
            for query in (dataframe, sql):
                result = query.collect(spill=spill, check=True)
                self.assertGreater(result.envelope.field_int("relational_spill_runs_written"), 0)
                self.assertTrue(result.envelope.field_bool("relational_spill_owned_cleanup_completed"))
                self.assertEqual(_digest(_rows(result.to_arrow())), _digest(expected))
                self.assertEqual(result.to_arrow().schema.field("tensor").metadata[
                    b"ARROW:extension:metadata"], METADATA_BYTES)
                self.assertFalse(result.envelope.field_bool("fallback_attempted"))
                self.assertEqual(list(workspace.iterdir()), [])
            streamed = []
            for batch in dataframe.iter_batches(batch_rows=257, spill=spill):
                self.assertLessEqual(batch.to_arrow().num_rows, 257)
                streamed.extend(_rows(batch.to_arrow()))
            self.assertEqual(_digest(streamed), _digest(expected))
            with dataframe.iter_batches(batch_rows=257, spill=spill) as early:
                retained = next(early)
            self.assertIsNone(early.report)
            self.assertEqual(_digest(_rows(retained.to_arrow())), _digest(expected[:257]))
            self.assertEqual(list(workspace.iterdir()), [])
            path = root / "selected.vortex"
            dataframe.write_vortex(path, spill=spill, check=True)
            completed = subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()), "--reopen", str(path)],
                env=os.environ.copy(), text=True, capture_output=True, timeout=120, check=True)
            report = json.loads(completed.stdout)[0]
            self.assertEqual(report["native"], _digest(expected))
            self.assertEqual(report["arrow"], _digest(expected))
            self.assertEqual(report["metadata"].encode(), METADATA_BYTES)
            self.assertEqual(report["rows"], len(expected))
            context.close()
            self.assertEqual(_digest(_rows(retained.to_arrow())), _digest(expected[:257]))
            self.assertEqual(list(workspace.iterdir()), [])

    def test_fixed_tensor_portable_writes_across_all_five_interfaces(self):
        import shardloom as sl
        from shardloom import types as t
        context = self.context("strict")
        dtype = t.fixed_shape_tensor(t.int32(nullable=False), [2, 3, 4],
                                     dim_names=METADATA["dim_names"], permutation=[2, 0, 1])
        rows = [{"id": row, "tensor": None if row == 3 else list(range(row, row + 24))} for row in range(1, 5)]
        with tempfile.TemporaryDirectory(prefix="shardloom-tensor-interfaces-") as directory:
            root, outputs = Path(directory), []
            source = root / "input.vortex"
            context.from_rows(rows, schema={"id": "int64", "tensor": dtype}).write_vortex(source, check=True)
            statement = "SELECT id, tensor FROM '" + str(source).replace("'", "''") + "' WHERE id <> 2 ORDER BY id DESC"
            native = self.native.Session(self.native.ExecutionResources(64 << 20, 1))
            cli = sl.ShardLoomClient(binary=os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY"),
                transport="subprocess", use_persistent_worker=False, memory_bytes=64 << 20, max_parallelism=1)
            files = [(str(source), "vortex", None)]
            expression = self.native.Expression
            relation = self.native.Relation.scan(str(source)).filter(
                expression.column("id") != expression.literal(2)).sort([("id", True, None)])
            arguments = ["sql", "--sql", statement, "--request", "collect", "--bounded", "true",
                         "--input", str(source), "--input-format", "vortex",
                         "--memory-bytes", str(64 << 20), "--max-parallelism", "1"]
            try:
                for surface in ("dataframe", "sql", "typed_plan", "embedded_workflow", "cli_sql"):
                    path = root / (surface + ".vortex")
                    with self.subTest(surface=surface):
                        if surface == "dataframe":
                            report = context.read_vortex(source).where("id <> 2").sort("id", descending=True).write_vortex(path, check=True)
                            fields = report.envelope.field
                        elif surface == "cli_sql":
                            report = sl.sql(statement, client=cli, input=str(source), input_format="vortex").write_vortex(path, check=True)
                            fields = report.envelope.field
                        else:
                            plan = (native.sql(statement, files=files) if surface == "sql" else
                                    native.plan(relation, files=files) if surface == "typed_plan" else native.workflow(arguments))
                            with patch("subprocess.Popen", side_effect=AssertionError("external execution")):
                                report = plan.write(path, "write_vortex")
                            fields = report.get
                        self.assertEqual(fields("fallback_attempted"), "false")
                        self.assertEqual(fields("external_engine_invoked"), "false")
                        outputs.append(path)
            finally:
                native.close()
                cli.close()
                context.close()
            source.unlink()
            completed = subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()), "--reopen", *map(str, outputs)],
                env=os.environ.copy(), text=True, capture_output=True, timeout=120, check=True)
            expected = [row for row in reversed(rows) if row["id"] != 2]
            for report in json.loads(completed.stdout):
                self.assertEqual(report["native"], _digest(expected))
                self.assertEqual(report["arrow"], _digest(expected))
                self.assertEqual(report["metadata"].encode(), METADATA_BYTES)
                self.assertIn("nullable=False", report["schema"][1][1])


if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == "--reopen":
        print(json.dumps(_reopen_job(sys.argv[2:]), sort_keys=True))
    else:
        unittest.main()
