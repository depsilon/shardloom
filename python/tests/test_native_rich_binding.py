"""Complete dense-vector / inline-byte / embedding lifecycle in the native package."""

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

from test_native_binding import _load_native


IDENTITY = {"version": 1, "space_id": "documents/未知", "model_id": "caller-supplied",
            "model_version": None, "representation_version": "dense-1",
            "normalization": "none", "preprocessing_id": "raw-v3",
            "source_provenance": "fixture/full-bytes"}
IDENTITY_BYTES = json.dumps(IDENTITY, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def _field(pa, name, dtype, nullable=True):
    return pa.field(name, dtype, nullable=nullable, metadata={
        b"ARROW:extension:name": b"shardloom.embedding.v1",
        b"ARROW:extension:metadata": IDENTITY_BYTES,
    })


def _fixture(count):
    import pyarrow as pa
    # Build floating buffers from independent bit patterns. Converting a Python
    # float to Float32 could quiet a signaling NaN before ShardLoom sees it.
    f32 = [0xff800042, 0x80000000, 0, 0x7f800000, 0xff800000, 0x3fc00000, 0x7fc01234, 0x00800000]
    f64 = [0xfff0000000000042, 0x8000000000000000, 0, 0x7ff0000000000000,
           0xfff0000000000000, 0x3ff8000000000000, 0x7ff8000000001234, 1]
    vectors = []
    for dtype, pattern, code in [(pa.float32(), f32, "I"), (pa.float64(), f64, "Q")]:
        raw = b"".join(struct.pack("<" + code, pattern[index % 8]) for index in range(count * 8))
        values = pa.Array.from_buffers(dtype, count * 8, [None, pa.py_buffer(raw)])
        mask = pa.array([row % 17 == 0 for row in range(count)])
        vectors.append(pa.FixedSizeListArray.from_arrays(
            values, type=pa.list_(pa.field("item", dtype, nullable=False), 8), mask=mask))
    nested_type = pa.struct([pa.field("tag", pa.string()),
        pa.field("values", pa.list_(pa.field("item", pa.int16())))])
    arrays = [
        pa.array(range(count), pa.int64()),
        pa.array([None if row % 11 == 0 else b"" if row % 7 == 0
                  else bytes((row % 251, 0, 255, 128)) * 128 for row in range(count)], pa.binary()),
        vectors[0], vectors[1],
        pa.array([None if row % 13 == 0 else {"tag": None if row % 3 == 0 else f"文-{row}",
            "values": [row % 32767, None, -(row % 32767)] if row % 5 else []}
            for row in range(count)], nested_type),
    ]
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("content", pa.binary()),
        _field(pa, "embedding", vectors[0].type), pa.field("vector", vectors[1].type),
        pa.field("attributes", nested_type)])
    return pa.Table.from_arrays(arrays, schema=schema)


def _value(array, row):
    import pyarrow as pa
    if not array[row].is_valid:
        return None
    dtype = array.type
    if pa.types.is_float32(dtype) or pa.types.is_float64(dtype):
        code, width = ("I", 4) if pa.types.is_float32(dtype) else ("Q", 8)
        return (str(dtype), struct.unpack_from("<" + code, array.buffers()[1], (array.offset + row) * width)[0])
    if pa.types.is_fixed_size_list(dtype) or pa.types.is_list(dtype):
        child = array[row].values
        return [_value(child, index) for index in range(len(child))]
    if pa.types.is_struct(dtype):
        return {field.name: _value(array.field(index), row) for index, field in enumerate(dtype)}
    value = array[row].as_py()
    return ("bytes", value.hex()) if isinstance(value, bytes) else value


def _rows(table):
    result = []
    for batch in table.to_batches():
        for row in range(batch.num_rows):
            result.append({field.name: _value(column, row)
                           for field, column in zip(batch.schema, batch.columns)})
    return result


def _digest(rows):
    return hashlib.sha256(json.dumps(rows, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":")).encode()).hexdigest()


def _reopen_job(path):
    _load_native()
    import shardloom as sl
    import pyarrow as pa
    client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20,
                               max_parallelism=1, float_policy="extended")
    try:
        frame = sl.read_vortex(path, client=client)
        table = frame.collect(check=True).to_arrow()
        exported = Path(path).with_suffix(".arrow")
        frame.write_arrow_ipc(exported, check=True)
        with pa.ipc.open_file(exported) as reader:
            independent = reader.read_all()
        assert table.schema.equals(independent.schema, check_metadata=True)
        return {"rows": table.num_rows, "native": _digest(_rows(table)),
                "arrow": _digest(_rows(independent)),
                "metadata": dict(table.schema.field("embedding").metadata)[b"ARROW:extension:metadata"].decode()}
    finally:
        client.close()


class NativeRichBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()

    def client(self, policy="extended"):
        import shardloom as sl
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20,
                                   max_parallelism=1, float_policy=policy)
        self.addCleanup(client.close)
        return client

    def assert_rows(self, actual, expected):
        self.assertEqual(len(actual), len(expected))
        # Compare every complete value without constructing a quadratic diff
        # of thousands of rows when one field is wrong.
        for index, (received, wanted) in enumerate(zip(actual, expected)):
            self.assertEqual(received, wanted, f"row {index}")

    def test_dense_embedding_object_intake_and_retained_special_values(self):
        import shardloom as sl
        from shardloom import types as t
        from shardloom._input_schema import normalize_schema
        nan32 = struct.unpack("<d", struct.pack("<Q", 0xfff0000840000000))[0]
        nan64 = struct.unpack("<d", struct.pack("<Q", 0x7ff0000000000042))[0]
        schema = {"id": "int64", "content": "binary",
            "embedding": t.embedding(t.float32(), 2, metadata=IDENTITY),
            "vector": {"type": "fixed_size_list", "size": 2, "item": "float64"}}
        rows = [{"id": 1, "content": b"\x00\xff", "embedding": [nan32, -0.0], "vector": [nan64, float("inf")]},
                {"id": 2, "content": b"", "embedding": [None, 1.5], "vector": None},
                {"id": 3, "content": None, "embedding": None, "vector": [-0.0, float("-inf")]}]
        client = self.client()
        with patch("shardloom._input_schema.encode_cell", side_effect=AssertionError("JSON cells")):
            frame = sl.from_rows(rows, schema=schema, client=client)
            result = frame.collect(check=True)
        self.assertEqual(result.result_schema, normalize_schema(schema))
        actual = result.python_objects
        self.assertEqual(struct.pack("<d", actual[0]["embedding"][0]), struct.pack("<d", nan32))
        self.assertEqual(struct.pack("<d", actual[0]["vector"][0]), struct.pack("<d", nan64))
        self.assertEqual(struct.pack("<d", actual[0]["embedding"][1]), struct.pack("<d", -0.0))
        self.assertEqual(actual[1], rows[1])
        self.assertEqual(actual[2], rows[2])
        before = _digest(_rows(result.to_arrow()))
        client.close()
        self.assertEqual(_digest(_rows(result.to_arrow())), before)

    def test_dense_rich_sql_dataframe_spill_write_reopen_and_export(self):
        import shardloom as sl
        import pyarrow as pa
        table = _fixture(4101).slice(1, 4097)
        client = self.client()
        left = sl.from_arrow_table(table, client=client)
        report_ids, tags = [], []
        for row in range(1, 4098):
            for tag in ([1, 2] if row % 5 == 0 else [1] if row % 3 == 0 else []):
                report_ids.append(row)
                tags.append(tag)
        right = sl.from_arrow_table(pa.table({"id": pa.array(report_ids, pa.int64()),
                                             "tag": pa.array(tags, pa.int64())}), client=client)
        selection = ["f.id AS id", "f.content AS content", "f.embedding AS embedding",
                     "f.vector AS vector", "f.attributes AS attributes", "d.tag AS tag"]
        dataframe = left.where("id >= 3").join(right, on="id", how="left").select(*selection).sort(
            "id", "tag", descending=True, nulls="last")
        sql = sl.sql("SELECT " + ", ".join(selection) + " "
                     "FROM documents AS f LEFT JOIN reports AS d ON f.id = d.id "
                     "WHERE f.id >= 3 ORDER BY f.id DESC, d.tag DESC NULLS LAST",
                     relations={"documents": left, "reports": right}, client=client)
        expected = []
        for row in reversed(_rows(table)):
            if row["id"] < 3:
                continue
            for tag in ([2, 1] if row["id"] % 5 == 0 else [1] if row["id"] % 3 == 0 else [None]):
                expected.append({**row, "tag": tag})
        with tempfile.TemporaryDirectory(prefix="shardloom-dense-lifecycle-") as directory:
            root = Path(directory)
            workspace = root / "spill"
            workspace.mkdir()
            spill = {"workspace": str(workspace), "quota_bytes": 64 << 20, "buffer_bytes": 1 << 20}
            for query in (dataframe, sql):
                result = query.collect(spill=spill, check=True)
                self.assertGreater(result.envelope.field_int("relational_spill_runs_written"), 0)
                self.assertTrue(result.envelope.field_bool("relational_spill_owned_cleanup_completed"))
                actual = result.to_arrow()
                self.assert_rows(_rows(actual), expected)
                self.assertEqual(actual.schema.field("embedding").metadata[b"ARROW:extension:metadata"], IDENTITY_BYTES)
                self.assertEqual(list(workspace.iterdir()), [])
            streamed = []
            for batch in dataframe.iter_batches(batch_rows=257, spill=spill):
                exported = batch.to_arrow()
                self.assertLessEqual(exported.num_rows, 257)
                streamed.extend(_rows(exported))
            self.assert_rows(streamed, expected)
            self.assertEqual(list(workspace.iterdir()), [])
            with dataframe.iter_batches(batch_rows=257, spill=spill) as early:
                retained = next(early)
            self.assertIsNone(early.report)
            self.assert_rows(_rows(retained.to_arrow()), expected[:257])
            self.assertEqual(list(workspace.iterdir()), [])
            cancelled = dataframe.iter_batches(batch_rows=257, spill=spill)
            next(cancelled)
            client.cancel()
            with self.assertRaisesRegex(Exception, "cancelled"):
                next(cancelled)
            cancelled.close()
            self.assertIsNone(cancelled.report)
            self.assertEqual(list(workspace.iterdir()), [])
            path = root / "selected.vortex"
            dataframe.write_vortex(path, spill=spill, check=True)
            self.assertEqual(list(workspace.iterdir()), [])
            reopened = subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()), "reopen", str(path)],
                env=os.environ.copy(), text=True, capture_output=True, timeout=120, check=True)
            report = json.loads(reopened.stdout)
            self.assertEqual(report["rows"], len(expected))
            self.assertEqual(report["native"], _digest(expected))
            self.assertEqual(report["arrow"], _digest(expected))
            self.assertEqual(report["metadata"].encode(), IDENTITY_BYTES)
            client.close()
            self.assert_rows(_rows(retained.to_arrow()), expected[:257])

    def test_dense_embedding_empty_nested_and_metadata_losing_sinks(self):
        import shardloom as sl
        import pyarrow as pa
        from shardloom.errors import ShardLoomCommandError
        client = self.client()
        table = _fixture(3)
        nested = pa.StructArray.from_arrays([table.column("embedding").combine_chunks()],
            fields=[table.schema.field("embedding")])
        frame = sl.from_arrow_table(pa.Table.from_arrays([nested], names=["record"]), client=client)
        result = frame.collect(check=True)
        self.assertEqual(_rows(result.to_arrow()), _rows(pa.Table.from_arrays([nested], names=["record"])))
        self.assertEqual(result.to_arrow().schema.field("record").type.field("embedding").metadata[
            b"ARROW:extension:metadata"], IDENTITY_BYTES)
        empty = sl.from_arrow_table(table.slice(0, 0), client=client).collect(check=True).to_arrow()
        self.assertEqual(empty.num_rows, 0)
        self.assertTrue(empty.schema.equals(table.schema, check_metadata=True))
        with tempfile.TemporaryDirectory(prefix="shardloom-dense-refusal-") as directory:
            root = Path(directory)
            for format_name in ("csv", "json", "jsonl", "avro", "orc"):
                with self.subTest(format=format_name), self.assertRaisesRegex(ShardLoomCommandError, "embedding identity"):
                    getattr(frame, "write_" + format_name)(root / ("target." + format_name), check=True)
                self.assertEqual(list(root.iterdir()), [])


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "reopen":
        print(json.dumps(_reopen_job(sys.argv[2]), sort_keys=True))
    else:
        unittest.main()
