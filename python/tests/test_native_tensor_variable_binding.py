"""Complete variable tensor values across native selection, spill and persistence."""

from __future__ import annotations

import base64
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
    from .test_native_tensor_binding import _rows, _digest
else:
    from test_native_binding import _load_native
    from test_native_tensor_binding import _rows, _digest


METADATA = {"permutation": [2, 0, 1], "dim_names": ["行", "height", "channel"],
            "uniform_shape": [None, 2, None]}
METADATA_BYTES = json.dumps(METADATA, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def _field(pa, storage, metadata=METADATA_BYTES, nullable=True):
    return pa.field("tensor", storage, nullable=nullable, metadata={
        b"ARROW:extension:name": b"arrow.variable_shape_tensor",
        b"ARROW:extension:metadata": metadata})


def _fixture(count):
    import pyarrow as pa
    shapes = [[1 + row % 3, 2, 0 if row % 11 == 0 else 1 + row % 4] for row in range(count)]
    offsets = [0]
    for shape in shapes:
        offsets.append(offsets[-1] + shape[0] * shape[1] * shape[2])
    special = [0xff800042, 0x80000000, 0, 0x7f800000, 0xff800000, 0x3fc00000, 0x7fc01234, 1]
    bits = [special[index % 13] if index % 13 < len(special) else 0x3f800000 + index * 31
            for index in range(offsets[-1])]
    validity = bytearray((len(bits) + 7) // 8)
    for index in range(len(bits)):
        if index % 37:
            validity[index // 8] |= 1 << (index % 8)
    values = pa.Array.from_buffers(pa.float32(), len(bits), [pa.py_buffer(validity),
        pa.py_buffer(b"".join(struct.pack("<I", bit) for bit in bits))])
    data_type = pa.list_(pa.field("item", pa.float32()))
    data = pa.ListArray.from_arrays(pa.array(offsets, pa.int32()), values, type=data_type)
    shape_type = pa.list_(pa.field("item", pa.int32(), nullable=False), 3)
    shape = pa.FixedSizeListArray.from_arrays(pa.array([n for row in shapes for n in row], pa.int32()),
                                             type=shape_type)
    storage = pa.struct([pa.field("data", data_type, nullable=False),
                         pa.field("shape", shape_type, nullable=False)])
    tensor = pa.StructArray.from_arrays([data, shape], fields=list(storage),
        mask=pa.array([row % 17 == 0 for row in range(count)]))
    return pa.Table.from_arrays([pa.array(range(count), pa.int64()), tensor,
        pa.array([bytes((row % 251, 0, 255)) * 192 for row in range(count)], pa.binary())],
        schema=pa.schema([pa.field("id", pa.int64()), _field(pa, storage), pa.field("content", pa.binary())]))


def _independent_schema_equal(actual, expected):
    import pyarrow as pa
    # PyArrow's IPC reader promotes registered extension field metadata into
    # its extension DataType. Apply that same independent interpretation to
    # both schemas; retain the complete storage, nullability and metadata check.
    return pa.ipc.read_schema(actual.serialize()).equals(
        pa.ipc.read_schema(expected.serialize()), check_metadata=True)


def _reopen_job(paths):
    _load_native()
    import shardloom as sl
    import pyarrow as pa
    import pyarrow.parquet as pq
    reports = []
    with sl.context(transport="native", memory_bytes=128 << 20, max_parallelism=1,
                    float_policy="extended") as context:
        for raw in paths:
            path = Path(raw)
            frame = context.read_vortex(path)
            result = frame.collect(check=True)
            table = result.to_arrow()
            arrow, parquet_path = path.with_suffix(".arrow"), path.with_suffix(".parquet")
            frame.write_arrow_ipc(arrow, check=True)
            frame.write_parquet(parquet_path, check=True)
            with pa.ipc.open_file(arrow) as reader:
                arrow = reader.read_all()
            parquet = context.read_parquet(parquet_path).collect(check=True).to_arrow()
            # PyArrow 25 cannot read fixed-list storage beneath null parents.
            # Attempt the full independent read, retain its exact limitation,
            # and independently check all rows with present tensor parents.
            independent_full = None
            limitation = None
            try:
                independent_full = pq.read_table(parquet_path)
            except pa.ArrowInvalid as error:
                if "Expected all lists to be of size=" not in str(error):
                    raise
                limitation = str(error)
            present = table.filter(table.column("tensor").is_valid())
            present_path = path.with_suffix(".present.parquet")
            context.from_arrow_table(present).write_parquet(present_path, check=True)
            independent_default = pq.read_table(present_path)
            # The C++ Parquet reader can drop this struct extension even though
            # the file's standard ARROW:schema retains it. Read that schema
            # independently from the footer and use it for the typed reopen.
            embedded = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(
                pq.read_metadata(present_path).metadata[b"ARROW:schema"], validate=True)))
            independent_present = pq.read_table(present_path, schema=embedded)
            schema_matches = {"parquet_native": parquet.schema.equals(table.schema, check_metadata=True),
                **{kind: _independent_schema_equal(value.schema, table.schema)
                   for kind, value in (("arrow", arrow), ("parquet_independent_present", independent_present))}}
            reports.append({"rows": table.num_rows, "native": _digest(_rows(table)),
                "arrow": _digest(_rows(arrow)), "parquet_native": _digest(_rows(parquet)),
                "parquet_independent_full": None if independent_full is None else _digest(_rows(independent_full)),
                "parquet_independent_present": _digest(_rows(independent_present)),
                "parquet_default_present": _digest(_rows(independent_default)),
                "parquet_default_schema_retained": _independent_schema_equal(
                    independent_default.schema, table.schema),
                "parquet_reader_limitation": limitation,
                "schemas_equal": all(schema_matches.values()), "schema_matches": schema_matches,
                "schema_mismatches": {kind: str(value.schema) for kind, value in (
                    ("arrow", arrow), ("parquet_native", parquet), ("parquet_independent_present", independent_present))
                    if not schema_matches[kind]},
                "metadata": table.schema.field("tensor").metadata[b"ARROW:extension:metadata"].decode(),
                "schema": [(name, repr(dtype)) for name, dtype in result.result_schema]})
    return reports


class NativeVariableTensorBindingTests(unittest.TestCase):
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

    def test_variable_tensor_typed_rows_scalar_empty_and_owner_drop(self):
        from shardloom import types as t
        from shardloom._input_schema import normalize_schema
        context = self.context()
        schema = {"id": t.int64(),
            "tensor": t.variable_shape_tensor(t.float64(), 2, permutation=[1, 0], uniform_shape=[None, 2]),
            "scalar": t.variable_shape_tensor(t.int32(nullable=False), 0, dim_names=[], permutation=[], uniform_shape=[]),
            "empty": t.variable_shape_tensor(t.boolean(nullable=False), 4)}
        rows = [{"id": 1, "tensor": {"data": [-0.0, None, float("inf"), 1.5], "shape": [2, 2]},
                 "scalar": {"data": [17], "shape": []}, "empty": {"data": [], "shape": [(1 << 31) - 1] * 3 + [0]}},
                {"id": 2, "tensor": {"data": [3.5, 4.0], "shape": [1, 2]}, "scalar": None, "empty": None},
                {"id": 3, "tensor": None, "scalar": None, "empty": None}]
        with patch("shardloom._input_schema.encode_cell", side_effect=AssertionError("JSON cells")):
            frame = context.from_rows(rows, schema=schema)
            result = frame.collect(check=True)
        self.assertEqual(result.result_schema, normalize_schema(schema))
        self.assertEqual(result.python_objects, tuple(rows))
        exported = result.to_arrow()
        self.assertEqual(exported.schema.field("scalar").metadata[b"ARROW:extension:metadata"],
                         b'{"dim_names":[],"permutation":[],"uniform_shape":[]}')
        self.assert_table(context.from_arrow_table(exported).collect(check=True).to_arrow(), exported)
        context.close()
        del frame
        self.assertEqual(result.python_objects, tuple(rows))
        self.assert_table(result.to_arrow(), exported)

    def test_variable_tensor_arrow_nullability_flags_and_all_component_types(self):
        import pyarrow as pa
        from shardloom import types as t
        context = self.context()
        for constructor, value in [(t.boolean, True), (t.int8, -128), (t.int16, -32768),
            (t.int32, -(1 << 31)), (t.int64, -(1 << 63)), (t.uint8, 255), (t.uint16, 65535),
            (t.uint32, (1 << 32) - 1), (t.uint64, (1 << 64) - 1), (t.float32, -0.0), (t.float64, -0.0)]:
            with self.subTest(element=constructor.__name__):
                dtype = t.variable_shape_tensor(constructor(), 1)
                rows = [{"tensor": {"data": [value], "shape": [1]}},
                        {"tensor": {"data": [None], "shape": [1]}}, {"tensor": None}]
                result = context.from_rows(rows, schema={"tensor": dtype}).collect(check=True)
                self.assertEqual(result.python_objects, tuple(rows))
        for flags in range(32):
            data = pa.list_(pa.field("item", pa.float32(), nullable=bool(flags & 16)))
            shape = pa.list_(pa.field("item", pa.int32(), nullable=bool(flags & 4)), 2)
            storage = pa.struct([pa.field("data", data, nullable=bool(flags & 2)),
                                 pa.field("shape", shape, nullable=bool(flags & 8))])
            field = _field(pa, storage, b'{"uniform_shape":[null,2]}', bool(flags & 1))
            values = [{"data": [1.0, -0.0], "shape": [1, 2]}]
            if flags & 1:
                values.append(None)
            table = pa.Table.from_arrays([pa.array(values, type=storage)], schema=pa.schema([field]))
            result = context.from_arrow_table(table).collect(check=True)
            self.assert_table(result.to_arrow(), table)
            # Reusing an imported result schema must preserve all structural flags.
            self.assert_table(context.from_rows(result.python_objects,
                schema=dict(result.result_schema)).collect(check=True).to_arrow(), table)

    def test_variable_tensor_nested_slices_restore_without_copying_numeric_buffers(self):
        import pyarrow as pa
        from shardloom import types as t
        from shardloom._result_schema import _arrow_field
        from shardloom._tensor import restore_arrow_schema
        context = self.context()
        tensor = t.variable_shape_tensor(t.float32(nullable=False), 2, permutation=[1, 0])
        fields = (("lists", t.list_(tensor)), ("fixed", t.fixed_size_list(tensor, 2)),
                  ("records", t.list_(t.struct({"tensor": tensor}))))
        a, b, empty = {"data": [1.0, -0.0], "shape": [1, 2]}, {"data": [3.0], "shape": [1, 1]}, {"data": [], "shape": [2, 0]}
        rows = [{"lists": [a], "fixed": [a, None], "records": [{"tensor": b}]},
                {"lists": None, "fixed": None, "records": None},
                {"lists": [None, b], "fixed": [None, a], "records": [None, {"tensor": None}]},
                {"lists": [empty], "fixed": [a, b], "records": []},
                {"lists": [b, a], "fixed": [a, None], "records": [{"tensor": a}]}]
        schema = pa.schema([_arrow_field(name, dtype, pa) for name, dtype in fields])
        table = pa.Table.from_pylist(rows, schema=schema).slice(1, 3)
        result = context.from_arrow_table(table).collect(check=True)
        self.assertEqual(result.result_schema, fields)
        self.assert_table(result.to_arrow(), table)
        restored = restore_arrow_schema(table, fields, pa)
        self.assert_table(restored, table)
        for name in ("lists", "fixed"):
            original = table.column(name).chunk(0).values.field("data").values
            rebuilt = restored.column(name).chunk(0).values.field("data").values
            self.assertEqual(original.buffers()[1].address, rebuilt.buffers()[1].address)
        # Direct tensor struct slicing exercises the offset of both list fields.
        direct = _fixture(9).slice(2, 4)
        direct_result = context.from_arrow_table(direct).collect(check=True)
        self.assert_table(restore_arrow_schema(direct, direct_result.result_schema, pa), direct)
        with tempfile.TemporaryDirectory(prefix="shardloom-variable-nested-") as directory:
            for count in (3, 0):
                expected = table.slice(0, count)
                for format_name in ("vortex", "arrow_ipc", "parquet"):
                    path = Path(directory) / (f"nested-{count}." + format_name)
                    getattr(context.from_arrow_table(expected), "write_" + format_name)(path, check=True)
                    self.assert_table(getattr(context, "read_" + format_name)(path).collect(check=True).to_arrow(), expected)

    def test_variable_tensor_relational_spill_stream_and_separate_process_reopen(self):
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
        with tempfile.TemporaryDirectory(prefix="shardloom-variable-lifecycle-") as directory:
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
                env=os.environ.copy(), text=True, capture_output=True, timeout=120, check=False)
            self.assertEqual(completed.returncode, 0, completed.stderr)
            report = json.loads(completed.stdout)[0]
            for kind in ("native", "arrow", "parquet_native"):
                self.assertEqual(report[kind], _digest(expected))
            self.assert_independent_parquet(report, expected)
            self.assertTrue(report["schemas_equal"], report)
            self.assertEqual(report["metadata"].encode(), METADATA_BYTES)
            self.assertEqual(report["rows"], len(expected))
            context.close()
            del left, right, dataframe, sql
            self.assertEqual(_digest(_rows(retained.to_arrow())), _digest(expected[:257]))
            self.assertEqual(list(workspace.iterdir()), [])

    def test_variable_tensor_writes_across_all_five_public_interfaces(self):
        import shardloom as sl
        from shardloom import types as t
        context = self.context("strict")
        dtype = t.variable_shape_tensor(t.int32(nullable=False), 3,
            dim_names=METADATA["dim_names"], permutation=[2, 0, 1], uniform_shape=[None, 2, None])
        rows = [{"id": row, "tensor": None if row == 3 else {
            "data": list(range(row * 4)), "shape": [row, 2, 2]}} for row in range(1, 5)]
        with tempfile.TemporaryDirectory(prefix="shardloom-variable-interfaces-") as directory:
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
                env=os.environ.copy(), text=True, capture_output=True, timeout=120, check=False)
            self.assertEqual(completed.returncode, 0, completed.stderr)
            expected = [row for row in reversed(rows) if row["id"] != 2]
            for report in json.loads(completed.stdout):
                for kind in ("native", "arrow", "parquet_native"):
                    self.assertEqual(report[kind], _digest(expected))
                self.assert_independent_parquet(report, expected)
                self.assertTrue(report["schemas_equal"], report)
                self.assertEqual(report["metadata"].encode(), METADATA_BYTES)
                self.assertIn("nullable=False", report["schema"][1][1])

    def assert_independent_parquet(self, report, expected):
        present = _digest([row for row in expected if row["tensor"] is not None])
        self.assertEqual(report["parquet_independent_present"], present)
        self.assertEqual(report["parquet_default_present"], present)
        if report["parquet_independent_full"] is not None:
            self.assertEqual(report["parquet_independent_full"], _digest(expected))
            self.assertIsNone(report["parquet_reader_limitation"])
        else:
            self.assertIn("Expected all lists to be of size=", report["parquet_reader_limitation"])


if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == "--reopen":
        print(json.dumps(_reopen_job(sys.argv[2:]), sort_keys=True))
    else:
        unittest.main()
