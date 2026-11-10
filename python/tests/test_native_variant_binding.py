"""Complete encoded Variant values through ordinary native user workflows."""

from __future__ import annotations

import gc
import hashlib
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import tempfile
import unittest

if __package__:
    from .test_native_binding import _load_native
else:
    from test_native_binding import _load_native


GRANT = 128 << 20
IDENTITY = {b"ARROW:extension:name": b"arrow.parquet.variant",
            b"ARROW:extension:metadata": b""}
NAMES = ("blob", "empty", "items", "name")
# Version 1, sorted one-byte dictionary offsets. These fixtures are independent
# of the runtime's VariantBuilder and compare every persisted encoded byte.
METADATA = bytes([0x11, len(NAMES), 0, 4, 9, 14, 18]) + b"blobemptyitemsname"


def _list(values):
    offsets, data = [0], b""
    for value in values:
        data += value
        offsets.append(len(data))
    return bytes([0x0f, len(values)]) + b"".join(struct.pack("<I", offset) for offset in offsets) + data


def _object(values):
    fields = sorted(values, key=NAMES.index)
    offsets, data = [0], b""
    for field in fields:
        data += values[field]
        offsets.append(len(data))
    return (bytes([0x0e, len(fields), *[NAMES.index(field) for field in fields]])
            + b"".join(struct.pack("<I", offset) for offset in offsets) + data)


def _binary(value):
    return b"\x3c" + struct.pack("<I", len(value)) + value


def _text(value):
    value = value.encode()
    return b"\x40" + struct.pack("<I", len(value)) + value


def _raw(ordinal):
    variants = [b"\x00", b"\x04", b"\x08", b"\x18" + struct.pack("<q", -(1 << 63)),
                b"\x18" + struct.pack("<q", (1 << 63) - 1),
                b"\x1c" + struct.pack("<Q", 0x8000000000000000),
                b"\x1c" + struct.pack("<Q", 0x7ff8000000000042),
                b"\x38" + struct.pack("<I", 0x7fc00042), _binary(b"\x00\xff\x80"),
                _binary(b""), _text("東京\nquoted\""), _list([]), _object({}),
                _list([b"\x00", b"\x04", _text("item")]),
                b"\x50" + bytes(range(16))]
    return variants[ordinal % len(variants)]


def _field(pa, name, dtype=None):
    dtype = dtype or pa.struct([pa.field("metadata", pa.binary(), nullable=False),
                                pa.field("value", pa.binary())])
    return pa.field(name, dtype, metadata=IDENTITY)


def _fixture(count, *, blob_bytes=32, nulls=False, constant=False):
    import pyarrow as pa
    payload = _field(pa, "payload")
    nested = pa.struct([_field(pa, "wrapped")])
    rows = []
    for ordinal in range(count):
        variant = None if nulls or ordinal % 19 == 0 else {
            "metadata": METADATA, "value": _raw(4 if constant else ordinal)}
        inner = _object({"blob": _binary(bytes([ordinal % 251]) * blob_bytes),
                         "empty": _object({}), "items": _list([_raw(ordinal), b"\x00"]),
                         "name": _text(f"文-{ordinal}")})
        rows.append({"id": ordinal, "payload": variant,
                     "detail": None if nulls or ordinal % 23 == 0 else {
                         "wrapped": {"metadata": METADATA, "value": inner}}})
    return pa.Table.from_pylist(rows, schema=pa.schema([
        pa.field("id", pa.int64()), payload, pa.field("detail", nested)]))


def _digest(rows):
    def encode(value):
        if isinstance(value, bytes):
            return {"bytes": value.hex()}
        raise TypeError(type(value).__name__)
    return hashlib.sha256(json.dumps(rows, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":"), default=encode).encode()).hexdigest()


def _decode_shredded_fixture(storage):
    """Independent format readback for this test's null/int/text/binary trees."""
    metadata = storage["metadata"]
    width = (metadata[0] >> 6) + 1
    integer = lambda data, start, size: int.from_bytes(data[start:start + size], "little")
    count = integer(metadata, 1, width)
    start = 1 + width * (count + 2)
    names = [metadata[start + integer(metadata, 1 + width * (index + 1), width):
                      start + integer(metadata, 1 + width * (index + 2), width)].decode()
             for index in range(count)]

    def decode(data):
        basic = data[0] & 3
        if basic == 1:
            return data[1:1 + (data[0] >> 2)].decode()
        if basic == 0:
            kind = data[0] >> 2
            if kind == 0:
                return None
            if kind in (1, 2):
                return kind == 1
            if kind in (3, 4, 5, 6):
                return int.from_bytes(data[1:1 + (1 << (kind - 3))], "little", signed=True)
            if kind in (15, 16):
                value = data[5:5 + integer(data, 1, 4)]
                return value if kind == 15 else value.decode()
            raise AssertionError(f"unexpected fixture primitive {kind}")
        object_ = basic == 2
        count_width = 4 if data[0] & (64 if object_ else 16) else 1
        count = integer(data, 1, count_width)
        offset_width = ((data[0] >> 2) & 3) + 1
        id_width = ((data[0] >> 4) & 3) + 1 if object_ else 0
        ids = 1 + count_width
        offsets = ids + count * id_width
        values = offsets + (count + 1) * offset_width
        decoded = [decode(data[values + integer(data, offsets + index * offset_width, offset_width):])
                   for index in range(count)]
        return ({names[integer(data, ids + index * id_width, id_width)]: value
                 for index, value in enumerate(decoded)} if object_ else decoded)

    return decode(storage["value"]) if storage["value"] is not None else None


def _identities(schema):
    import pyarrow as pa
    found = []
    def field(value, path):
        if value.metadata and value.metadata.get(b"ARROW:extension:name") == IDENTITY[b"ARROW:extension:name"]:
            assert value.metadata == IDENTITY, value
            assert value.type == _field(pa, "expected").type, value
            found.append(path)
        elif pa.types.is_struct(value.type):
            for child in value.type:
                field(child, path + "." + child.name)
    for value in schema:
        field(value, value.name)
    return found


def _reopen(paths):
    _load_native()
    import pyarrow as pa
    import pyarrow.parquet as pq
    import shardloom as sl
    reports = []
    with sl.context(transport="native", memory_bytes=GRANT, max_parallelism=1) as context:
        for raw in paths:
            source = context.read_vortex(raw)
            native = source.collect(check=True)
            tables = {"native": native.to_arrow()}
            for name in ("arrow_ipc", "parquet"):
                destination = Path(raw).with_name(Path(raw).name + ".reexport." + name)
                getattr(source, "write_" + name)(destination, check=True)
                tables[name] = getattr(context, "read_" + name)(destination).collect(check=True).to_arrow()
                if name == "parquet":
                    tables["independent_parquet"] = pq.read_table(destination)
                else:
                    with pa.ipc.open_file(destination) as reader:
                        tables["independent_arrow"] = reader.read_all()
            assert not native.envelope.field_bool("fallback_attempted")
            assert not native.envelope.field_bool("external_engine_invoked")
            reports.append({"rows": native.to_arrow().num_rows,
                "digests": {name: _digest(table.to_pylist()) for name, table in tables.items()},
                "identities": {name: _identities(table.schema) for name, table in tables.items()}})
    return reports


class NativeVariantBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()

    def context(self, memory=GRANT):
        import shardloom as sl
        context = sl.context(transport="native", memory_bytes=memory, max_parallelism=1)
        self.addCleanup(context.close)
        return context

    def assert_values(self, result, expected):
        table = result.to_arrow()
        self.assertEqual(_identities(table.schema), ["payload", "detail.wrapped"])
        self.assertEqual(_digest(table.to_pylist()), _digest(expected))
        self.assertEqual(table.num_rows, len(expected))
        self.assertFalse(result.envelope.field_bool("fallback_attempted"))
        self.assertFalse(result.envelope.field_bool("external_engine_invoked"))

    def queries(self, context, count, *, blob_bytes=32):
        import pyarrow as pa
        table = _fixture(count + 2, blob_bytes=blob_bytes).slice(1, count)
        self.assertEqual(table.column("id").chunk(0).offset, 1)
        left = context.from_arrow_table(table)
        tags = lambda row: [1, 2] if row % 5 == 0 else [1] if row % 3 == 0 else [None]
        right = context.from_arrow_table(pa.Table.from_pylist([
            {"id": row, "tag": tag} for row in range(1, count + 1) for tag in tags(row) if tag is not None]))
        selection = ["f.id AS id", "f.payload AS payload", "f.detail AS detail", "d.tag AS tag"]
        frame = left.where("id >= 2").join(right, on="id", how="left").select(*selection).sort(
            "id", "tag", descending=True, nulls="last")
        sql = context.sql("SELECT " + ", ".join(selection) + " FROM documents AS f "
            "LEFT JOIN tags AS d ON f.id = d.id WHERE f.id >= 2 "
            "ORDER BY f.id DESC, d.tag DESC NULLS LAST", relations={"documents": left, "tags": right})
        expected = [{**row, "tag": tag} for row in reversed(table.to_pylist()) if row["id"] >= 2
                    for tag in reversed(tags(row["id"]))]
        return (frame, sql), expected

    def test_variant_sql_dataframe_stream_write_and_separate_process_reopen(self):
        context = self.context()
        queries, expected = self.queries(context, 33)
        with tempfile.TemporaryDirectory(prefix="shardloom-variant-public-") as directory:
            paths = []
            for name, query in zip(("dataframe", "sql"), queries):
                result = query.collect(check=True)
                self.assert_values(result, expected)
                self.assertEqual(_digest(result.python_objects), _digest(expected))
                self.assertEqual(dict(result.result_schema)["payload"].name, "variant")
                streamed = []
                with query.iter_batches(batch_rows=7) as batches:
                    for batch in batches:
                        arrow = batch.to_arrow()
                        self.assertLessEqual(arrow.num_rows, 7)
                        self.assertEqual(_identities(arrow.schema), ["payload", "detail.wrapped"])
                        streamed.extend(arrow.to_pylist())
                self.assertIsNotNone(batches.report)
                self.assertEqual(_digest(streamed), _digest(expected))
                with query.iter_batches(batch_rows=7) as early:
                    retained = next(early).to_arrow()
                self.assertIsNone(early.report)
                self.assertEqual(_digest(retained.to_pylist()), _digest(expected[:7]))
                for suffix, source in [("query", query), ("completed", context.from_result(result))]:
                    path = Path(directory) / (name + "-" + suffix + ".vortex")
                    source.write_vortex(path, check=True)
                    paths.append(str(path))
            child = subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()), "--reopen", *paths],
                env=os.environ.copy(), capture_output=True, text=True, timeout=180, check=False)
            self.assertEqual(child.returncode, 0, child.stderr)
            reports = json.loads(child.stdout)
            self.assertEqual(len(reports), 4)
            for report in reports:
                self.assertEqual(report["rows"], len(expected))
                self.assertEqual(set(report["digests"].values()), {_digest(expected)})
                self.assertTrue(all(value == ["payload", "detail.wrapped"] for value in report["identities"].values()))
            context.close()
            self.assertEqual(_digest(retained.to_pylist()), _digest(expected[:7]))

    def test_variant_native_fanout_keeps_all_values_and_typed_empty_outputs(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        session = self.native.Session(self.native.ExecutionResources(GRANT, 1))
        self.addCleanup(session.close)
        with tempfile.TemporaryDirectory(prefix="shardloom-variant-fanout-") as directory:
            paths, expected = [], []
            for count in (0, 33):
                table = _fixture(count)
                source = session.from_arrow(pa.RecordBatchReader.from_batches(table.schema, table.to_batches()))
                plan = session.sql("SELECT * FROM 'memory://variant-fanout'",
                                   sources=[("memory://variant-fanout", source)])
                targets = [(Path(directory) / f"{count}.{name}", "write_" + name)
                           for name in ("vortex", "arrow_ipc", "parquet")]
                report = plan.write_many(targets)
                self.assertEqual(report["native_vortex_result_export_execution_count"], "1")
                self.assertEqual(report["native_vortex_result_export_replay_count"], "3")
                self.assertEqual(report["fallback_attempted"], "false")
                self.assertEqual(report["external_engine_invoked"], "false")
                paths.append(str(targets[0][0]))
                expected.append(_digest(table.to_pylist()))
                with pa.ipc.open_file(targets[1][0]) as reader:
                    ipc = reader.read_all()
                for actual in (ipc, pq.read_table(targets[2][0])):
                    self.assertEqual(_digest(actual.to_pylist()), expected[-1])
                    self.assertEqual(_identities(actual.schema), ["payload", "detail.wrapped"])
            child = subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()), "--reopen", *paths],
                env=os.environ.copy(), capture_output=True, text=True, timeout=180, check=False)
            self.assertEqual(child.returncode, 0, child.stderr)
            for report, digest in zip(json.loads(child.stdout), expected):
                self.assertEqual(set(report["digests"].values()), {digest})

    def test_variant_shredded_objects_and_lists_survive_separate_process_reopen(self):
        import pyarrow as pa
        shell = lambda dtype: pa.struct([pa.field("value", pa.binary()), pa.field("typed_value", dtype)])
        typed = pa.struct([pa.field("items", shell(pa.list_(shell(pa.int64())))),
                           pa.field("name", shell(pa.string()))])
        field = _field(pa, "payload", pa.struct([pa.field("metadata", pa.binary(), nullable=False),
                                                pa.field("value", pa.binary()), pa.field("typed_value", typed)]))
        variants = [
            {"metadata": METADATA, "value": _object({"blob": _binary(b"\x00\xff")}),
             "typed_value": {"items": {"typed_value": [{"typed_value": 1}, {"value": b"\x00"},
                                                       {"typed_value": -7}]},
                             "name": {"typed_value": "東京"}}},
            {"metadata": METADATA, "value": _object({"empty": _object({})}),
             "typed_value": {"items": {"typed_value": []}, "name": {"value": b"\x00"}}},
            {"metadata": METADATA, "typed_value": {"items": None, "name": None}},
            None, {"metadata": METADATA, "value": b"\x00"},
        ]
        expected = [("variant", {"blob": b"\x00\xff", "items": [1, None, -7], "name": "東京"}),
                    ("variant", {"empty": {}, "items": [], "name": None}), ("variant", {}),
                    None, ("variant", None)]
        table = pa.Table.from_pylist([{"id": row, "payload": value} for row, value in enumerate(variants)],
                                    schema=pa.schema([pa.field("id", pa.int64()), field]))
        context = self.context()
        source = context.from_arrow_table(table).where("id >= 0").sort("id", descending=True)
        complete = source.collect(check=True).to_arrow()
        rows = complete.to_pylist()
        actual = [None if row["payload"] is None else ("variant", _decode_shredded_fixture(row["payload"]))
                  for row in rows]
        self.assertEqual(actual, list(reversed(expected)))
        self.assertEqual(_identities(complete.schema), ["payload"])
        with tempfile.TemporaryDirectory(prefix="shardloom-variant-shredded-") as directory:
            path = Path(directory) / "shredded.vortex"
            source.write_vortex(path, check=True)
            child = subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()), "--reopen", str(path)],
                env=os.environ.copy(), capture_output=True, text=True, timeout=180, check=False)
            self.assertEqual(child.returncode, 0, child.stderr)
            report, = json.loads(child.stdout)
            self.assertEqual(report["rows"], len(rows))
            self.assertEqual(set(report["digests"].values()), {_digest(rows)})
            self.assertTrue(all(value == ["payload"] for value in report["identities"].values()))

    def test_variant_native_spill_preserves_duplicates_and_cleans_cancellation(self):
        context = self.context()
        queries, expected = self.queries(context, 2049, blob_bytes=1024)
        with tempfile.TemporaryDirectory(prefix="shardloom-variant-spill-") as directory:
            workspace = Path(directory) / "spill"
            workspace.mkdir()
            spill = {"workspace": str(workspace), "quota_bytes": 64 << 20, "buffer_bytes": 1 << 20}
            for query in queries:
                result = query.collect(spill=spill, check=True)
                self.assertGreater(result.envelope.field_int("relational_spill_runs_written"), 0)
                self.assertTrue(result.envelope.field_bool("relational_spill_owned_cleanup_completed"))
                self.assert_values(result, expected)
                self.assertEqual(list(workspace.iterdir()), [])
            with queries[0].iter_batches(batch_rows=31, spill=spill) as early:
                retained = next(early).to_arrow()
            self.assertIsNone(early.report)
            self.assertEqual(_digest(retained.to_pylist()), _digest(expected[:31]))
            self.assertEqual(list(workspace.iterdir()), [])
            cancelled = queries[0].iter_batches(batch_rows=31, spill=spill)
            next(cancelled)
            context.client.cancel()
            with self.assertRaisesRegex(Exception, "cancelled"):
                next(cancelled)
            cancelled.close()
            self.assertIsNone(cancelled.report)
            self.assertEqual(list(workspace.iterdir()), [])

    def test_variant_empty_null_and_constant_native_files_keep_identity(self):
        context = self.context()
        with tempfile.TemporaryDirectory(prefix="shardloom-variant-shapes-") as directory:
            paths, expected = [], []
            for name, table in [("empty", _fixture(0)), ("null", _fixture(7, nulls=True)),
                                ("constant", _fixture(258, constant=True))]:
                source = context.from_arrow_table(table)
                result = source.collect(check=True)
                self.assert_values(result, table.to_pylist())
                path = Path(directory) / (name + ".vortex")
                source.write_vortex(path, check=True)
                paths.append(str(path))
                expected.append((table.num_rows, _digest(table.to_pylist())))
            child = subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()), "--reopen", *paths],
                env=os.environ.copy(), capture_output=True, text=True, timeout=180, check=False)
            self.assertEqual(child.returncode, 0, child.stderr)
            for report, (count, digest) in zip(json.loads(child.stdout), expected):
                self.assertEqual(report["rows"], count)
                self.assertEqual(set(report["digests"].values()), {digest})
                self.assertTrue(all(value == ["payload", "detail.wrapped"] for value in report["identities"].values()))

    def test_variant_last_exported_child_retains_credit_after_context_close(self):
        for count in (0, 17):
            with self.subTest(count=count):
                context = self.context()
                frame = context.from_arrow_table(_fixture(count))
                session = context.client._native_backend.session
                result = frame.collect(check=True)
                table = result.to_arrow()
                # Empty results still carry a physical child owner.
                child = table.column("payload").chunk(0).field("value")
                context.close()
                del frame, result, table, context
                gc.collect()
                self.assertGreater(session.snapshot()["reserved_bytes"], 0)
                del child
                gc.collect()
                self.assertEqual(session.snapshot()["reserved_bytes"], 0)

    def test_variant_malformed_storage_denial_and_unsupported_keys_fail_explicitly(self):
        import pyarrow as pa
        from shardloom.errors import ShardLoomCommandError
        context = self.context()
        for metadata, value in [(b"", b"\x00"), (METADATA, b""), (METADATA, b"\xff")]:
            field = _field(pa, "payload")
            invalid = pa.Table.from_pylist([{"payload": {"metadata": metadata, "value": value}}],
                                          schema=pa.schema([field]))
            with self.assertRaises(ShardLoomCommandError) as raised:
                context.from_arrow_table(invalid).collect(check=True)
            self.assertFalse(raised.exception.envelope.fallback.attempted)
        for count in (0, 17):
            valid = context.from_arrow_table(_fixture(count))
            for sql in (
                "SELECT payload FROM data ORDER BY payload",
                "SELECT detail FROM data ORDER BY detail",
                "SELECT payload, COUNT(*) FROM data GROUP BY payload",
                "SELECT DISTINCT payload FROM data",
                "SELECT l.id FROM data AS l JOIN data AS r ON l.payload = r.payload",
            ):
                with self.subTest(count=count, sql=sql):
                    with self.assertRaises(ShardLoomCommandError) as raised:
                        context.sql(sql, relations={"data": valid}).collect(check=True)
                    self.assertIn("Variant values are payload-only", str(raised.exception))
                    self.assertFalse(raised.exception.envelope.fallback.attempted)
        denied = self.context(32 << 10)
        with self.assertRaises(ShardLoomCommandError) as raised:
            denied.from_arrow_table(_fixture(2, blob_bytes=8192)).collect(check=True)
        session = denied.client._native_backend.session
        self.assertIn("reservation denied", str(raised.exception))
        denied.close()
        gc.collect()
        self.assertEqual(session.snapshot()["reserved_bytes"], 0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--reopen":
        print(json.dumps(_reopen(sys.argv[2:])))
    else:
        unittest.main()
