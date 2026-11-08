"""Exact sparse coordinates and affine codes across native user workflows."""

from __future__ import annotations

import base64
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

if __package__:
    from .test_native_binding import _load_native
    from .test_native_tensor_binding import _rows, _digest
    from .vector_fixture import fixture
else:
    from test_native_binding import _load_native
    from test_native_tensor_binding import _rows, _digest
    from vector_fixture import fixture


def _schema_shape(schema):
    import pyarrow as pa

    def field_shape(field, list_child=False):
        dtype, extension = field.type, None
        if isinstance(dtype, pa.BaseExtensionType):
            extension, dtype = dtype, dtype.storage_type
        if pa.types.is_list(dtype) or pa.types.is_large_list(dtype) or pa.types.is_fixed_size_list(dtype):
            shape = ["fixed" if pa.types.is_fixed_size_list(dtype) else "list",
                     dtype.list_size if pa.types.is_fixed_size_list(dtype) else None,
                     field_shape(dtype.value_field, True)]
        elif pa.types.is_struct(dtype):
            shape = ["struct", [field_shape(child) for child in dtype]]
        else:
            shape = str(dtype)
        return ["item" if list_child else field.name, field.nullable, shape, extension,
                sorted((key, value) for key, value in (field.metadata or {}).items())]

    # Promote canonical Arrow types independently; normalize only generated list
    # child names and physical list offset widths, never primitive widths.
    return [field_shape(field) for field in pa.ipc.read_schema(schema.serialize())]


def _reopen_job(paths):
    _load_native()
    import shardloom as sl
    import pyarrow as pa
    import pyarrow.parquet as pq
    reports = []
    with sl.context(transport="native", memory_bytes=128 << 20, max_parallelism=1) as context:
        for raw in paths:
            path = Path(raw)
            frame = context.read_vortex(path)
            result = frame.collect(check=True)
            native = result.to_arrow()
            arrow_path, parquet_path = path.with_suffix(".arrow"), path.with_suffix(".parquet")
            frame.write_arrow_ipc(arrow_path, check=True)
            frame.write_parquet(parquet_path, check=True)
            with pa.ipc.open_file(arrow_path) as reader:
                arrow = reader.read_all()
            embedded = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(
                pq.read_metadata(parquet_path).metadata[b"ARROW:schema"], validate=True)))
            parquet = pq.read_table(parquet_path, schema=embedded)
            native_parquet = context.read_parquet(parquet_path).collect(check=True).to_arrow()
            tables = {"native": native, "arrow": arrow, "parquet": parquet,
                      "native_parquet": native_parquet}
            reports.append({"rows": native.num_rows,
                "digests": {kind: _digest(_rows(table)) for kind, table in tables.items()},
                "schemas_equal": {kind: _schema_shape(table.schema) == _schema_shape(native.schema)
                                  for kind, table in tables.items()},
                "schema": [(name, repr(dtype)) for name, dtype in result.result_schema]})
    return reports


class NativeVectorRepresentationsBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()

    def context(self, policy="strict"):
        import shardloom as sl
        context = sl.context(transport="native", memory_bytes=128 << 20,
                             max_parallelism=1, float_policy=policy)
        self.addCleanup(context.close)
        return context

    def assert_table(self, actual, expected):
        self.assertEqual(_schema_shape(actual.schema), _schema_shape(expected.schema))
        self.assertEqual(_digest(_rows(actual)), _digest(_rows(expected)))

    def test_vector_frozen_mixed_reference_typed_arrow_empty_and_retained_owners(self):
        import pyarrow as pa
        from shardloom._tensor import restore_arrow_schema
        context = self.context()
        table, rows = fixture(pa)
        self.assertEqual(json.dumps(table.to_pylist(), sort_keys=True), json.dumps(rows, sort_keys=True))
        result = context.from_arrow_table(table).collect(check=True)
        self.assert_table(result.to_arrow(), table)
        self.assertEqual(result.python_objects, tuple(rows))
        with patch("shardloom._input_schema.encode_cell", side_effect=AssertionError("JSON cells")):
            typed = context.from_rows(rows, schema=dict(result.result_schema)).collect(check=True)
        self.assertEqual(typed.result_schema, result.result_schema)
        self.assert_table(typed.to_arrow(), table)
        for selected in [table.slice(2, 3), table.slice(0, 0)]:
            actual = context.from_arrow_table(selected).collect(check=True)
            self.assert_table(actual.to_arrow(), selected)
        view = result.to_arrow()
        restored = restore_arrow_schema(table, result.result_schema, pa)
        self.assert_table(restored, table)
        for name, child in [("sparse", "values"), ("quantized", "codes"), ("blocked", "codes")]:
            original = table.column(name).chunk(0).field(child).values
            rebuilt = restored.column(name).chunk(0).field(child).values
            self.assertEqual(original.buffers()[1].address, rebuilt.buffers()[1].address)
        context.close()
        del typed, result, restored
        self.assert_table(view, table)

    def test_vector_every_integer_and_float_width_retains_typed_and_arrow_storage(self):
        from shardloom import types as t
        context = self.context()
        for integer, minimum, maximum in [
            (t.int8, -128, 127), (t.int16, -32768, 32767),
            (t.int32, -(1 << 31), (1 << 31) - 1), (t.int64, -(1 << 63), (1 << 63) - 1),
            (t.uint8, 0, 255), (t.uint16, 0, 65535),
            (t.uint32, 0, (1 << 32) - 1), (t.uint64, 0, (1 << 64) - 1),
        ]:
            for real in [t.float16, t.float32, t.float64]:
                with self.subTest(integer=integer.__name__, real=real.__name__):
                    size = min(maximum + 1, (1 << 64) - 1)
                    coordinate = size - 1
                    schema = {"s": t.sparse_vector(real(), index=integer(), metadata={"space_id": "Ω", "model_id": None}),
                        "q": t.quantized_vector(integer(), scale=real()),
                        "b": t.quantized_vector(integer(), scale=real(), block_size=2)}
                    rows = [{"s": {"size": size, "indices": [coordinate, 0, coordinate], "values": [-0.0, None, 1.5]},
                             "q": {"codes": [minimum, None, maximum], "scale": 0.25, "zero_point": maximum},
                             "b": {"codes": [maximum, None, minimum], "scale": [0.5, 2.0], "zero_point": [minimum, maximum]}},
                            {"s": {"size": 0, "indices": [], "values": []},
                             "q": {"codes": [], "scale": 0.5, "zero_point": minimum},
                             "b": {"codes": [], "scale": [], "zero_point": []}},
                            {"s": None, "q": None, "b": None}]
                    result = context.from_rows(rows, schema=schema).collect(check=True)
                    self.assertEqual(result.python_objects, tuple(rows))
                    self.assertEqual(result.result_schema, tuple(schema.items()))
                    table = result.to_arrow()
                    self.assert_table(context.from_arrow_table(table).collect(check=True).to_arrow(), table)
                    self.assert_table(context.from_rows([], schema=schema).collect(check=True).to_arrow(), table.slice(0, 0))
                    self.assert_table(context.from_rows(rows[-1:], schema=schema).collect(check=True).to_arrow(), table.slice(2, 1))
                    with tempfile.TemporaryDirectory(prefix="shardloom-vector-widths-") as directory:
                        for format_name in ("vortex", "arrow_ipc", "parquet"):
                            path = Path(directory) / ("values." + format_name)
                            getattr(context.from_rows(rows, schema=schema), "write_" + format_name)(path, check=True)
                            reopened = getattr(context, "read_" + format_name)(path).collect(check=True)
                            self.assert_table(reopened.to_arrow(), table)

    def test_vector_scalar_join_spill_stream_duplicates_and_separate_process_reopen(self):
        import pyarrow as pa
        context = self.context()
        table, _ = fixture(pa, repetitions=700)
        left = context.from_arrow_table(table)
        reports = [{"id": row, "tag": tag} for row in range(table.num_rows)
                   for tag in ([1, 2] if row % 5 == 0 else [1] if row % 3 == 0 else [])]
        right = context.from_arrow_table(pa.Table.from_pylist(reports))
        selection = ["f." + name + " AS " + name for name in table.schema.names] + ["d.tag AS tag"]
        dataframe = left.where("id >= 2").join(right, on="id", how="left").select(*selection).sort(
            "id", "tag", descending=True, nulls="last")
        sql = context.sql("SELECT " + ", ".join(selection) + " FROM vectors AS f LEFT JOIN reports AS d "
                          "ON f.id = d.id WHERE f.id >= 2 ORDER BY f.id DESC, d.tag DESC NULLS LAST",
                          relations={"vectors": left, "reports": right})
        expected = [{**row, "tag": tag} for row in reversed(_rows(table)) if row["id"] >= 2
                    for tag in ([2, 1] if row["id"] % 5 == 0 else [1] if row["id"] % 3 == 0 else [None])]
        with tempfile.TemporaryDirectory(prefix="shardloom-vector-lifecycle-") as directory:
            root, workspace = Path(directory), Path(directory) / "spill"
            workspace.mkdir()
            spill = {"workspace": str(workspace), "quota_bytes": 64 << 20, "buffer_bytes": 1 << 20}
            for query in (dataframe, sql):
                result = query.collect(spill=spill, check=True)
                self.assertGreater(result.envelope.field_int("relational_spill_runs_written"), 0)
                self.assertTrue(result.envelope.field_bool("relational_spill_owned_cleanup_completed"))
                self.assertFalse(result.envelope.field_bool("fallback_attempted"))
                self.assertFalse(result.envelope.field_bool("external_engine_invoked"))
                self.assertEqual(_digest(_rows(result.to_arrow())), _digest(expected))
                self.assertEqual(_schema_shape(result.to_arrow().schema)[:-1], _schema_shape(table.schema))
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
            report = self.reopen([path])[0]
            self.assert_reopened(report, expected)
            context.close()
            del left, right, dataframe, sql
            self.assertEqual(_digest(_rows(retained.to_arrow())), _digest(expected[:257]))
            self.assertEqual(list(workspace.iterdir()), [])

    def test_vector_writes_through_all_five_interfaces_then_reopens_without_source(self):
        import pyarrow as pa
        import shardloom as sl
        context = self.context()
        table, _ = fixture(pa)
        with tempfile.TemporaryDirectory(prefix="shardloom-vector-interfaces-") as directory:
            root, outputs = Path(directory), []
            source = root / "input.vortex"
            context.from_arrow_table(table).write_vortex(source, check=True)
            statement = "SELECT * FROM '" + str(source).replace("'", "''") + "' WHERE id <> 2 ORDER BY id DESC"
            native = self.native.Session(self.native.ExecutionResources(64 << 20, 1))
            cli = sl.ShardLoomClient(binary=os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY"),
                transport="subprocess", use_persistent_worker=False, memory_bytes=64 << 20, max_parallelism=1)
            files, expression = [(str(source), "vortex", None)], self.native.Expression
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
            expected = [row for row in reversed(_rows(table)) if row["id"] != 2]
            for report in self.reopen(outputs):
                self.assert_reopened(report, expected)

    def reopen(self, paths):
        completed = subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()), "--reopen", *map(str, paths)],
            env=os.environ.copy(), text=True, capture_output=True, timeout=180, check=False)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        return json.loads(completed.stdout)

    def assert_reopened(self, report, expected):
        self.assertEqual(report["rows"], len(expected))
        for kind, digest in report["digests"].items():
            self.assertEqual(digest, _digest(expected), kind)
        self.assertTrue(all(report["schemas_equal"].values()), report)


if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == "--reopen":
        print(json.dumps(_reopen_job(sys.argv[2:]), sort_keys=True))
    else:
        unittest.main()
