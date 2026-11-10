"""Canonical UUID values through shared native operators and user boundaries."""

from __future__ import annotations

from collections import Counter
import gc
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import uuid

if __package__:
    from .test_native_binding import _load_native
else:
    from test_native_binding import _load_native


GRANT = 128 << 20
# Frozen complete bytes. Values intentionally include arbitrary version/variant
# bits, the unsigned high bit and identities differing only in the final byte.
UUIDS = tuple(uuid.UUID(bytes=bytes.fromhex(value)) for value in (
    "00000000000000000000000000000000", "ffffffffffffffffffffffffffffffff",
    "0123456789abcdeffedcba9876543210", "0123456789abcdeffedcba9876543211",
    "80000000000000000000000000000000", "7fffffffffffffffffffffffffffffff",
    "00000000000000000000000000000001"))
DOMAIN = (*UUIDS, None)


def _uuid_array(values):
    import pyarrow as pa
    storage = pa.array([None if value is None else value.bytes for value in values], type=pa.binary(16))
    return pa.ExtensionArray.from_storage(pa.uuid(), storage)


def _fixture(count, *, padding=0, nulls=False, constant=False):
    import pyarrow as pa
    detail = pa.struct([pa.field("wrapped", pa.uuid()), pa.field("items", pa.list_(pa.uuid()))])
    schema = pa.schema([pa.field("ordinal", pa.int64()), pa.field("uid", pa.uuid()),
                        pa.field("other", pa.uuid()), pa.field("raw", pa.binary()),
                        pa.field("detail", detail), pa.field("padding", pa.binary())])
    rows = []
    for row in range(count):
        value = None if nulls else UUIDS[2] if constant else DOMAIN[row % len(DOMAIN)]
        other = None if nulls else DOMAIN[(row + 3) % len(DOMAIN)]
        rows.append({"ordinal": row, "uid": value, "other": other,
                     "raw": None if value is None else value.bytes,
                     "detail": None if row % 5 == 0 else {"wrapped": value, "items": [other, None, value]},
                     "padding": bytes([row % 251]) * padding})
    # Build extension children explicitly: PyArrow's nested Python-object
    # converter does not implement UUID extension arrays.
    wrapped = _uuid_array([None if row["detail"] is None else row["detail"]["wrapped"] for row in rows])
    items = pa.ListArray.from_arrays(pa.array(range(0, 3 * count + 1, 3), type=pa.int32()),
        _uuid_array([value for row in rows for value in
                     ([None] * 3 if row["detail"] is None else row["detail"]["items"])]))
    nested = pa.StructArray.from_arrays([wrapped, items], fields=list(detail),
        mask=pa.array([row["detail"] is None for row in rows], type=pa.bool_()))
    return pa.Table.from_arrays([
        pa.array([row["ordinal"] for row in rows], type=pa.int64()),
        _uuid_array([row["uid"] for row in rows]), _uuid_array([row["other"] for row in rows]),
        pa.array([row["raw"] for row in rows], type=pa.binary()), nested,
        pa.array([row["padding"] for row in rows], type=pa.binary())], schema=schema)


def _encoded(value):
    if isinstance(value, uuid.UUID):
        return {"uuid_bytes": value.bytes.hex()}
    if isinstance(value, bytes):
        return {"bytes": value.hex()}
    raise TypeError(type(value).__name__)


def _digest(rows, *, ordered=True):
    encoded = [json.dumps(row, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=_encoded)
               for row in rows]
    if not ordered:
        encoded.sort()
    return hashlib.sha256(json.dumps(encoded, separators=(",", ":")).encode()).hexdigest()


def _identities(schema):
    import pyarrow as pa
    found = []
    def field(value, path):
        if value.type == pa.uuid():
            found.append(path)
        elif pa.types.is_struct(value.type):
            for child in value.type:
                field(child, path + "." + child.name)
        elif pa.types.is_list(value.type) or pa.types.is_fixed_size_list(value.type):
            field(value.type.value_field, path + "[]")
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
                try:
                    tables[name] = getattr(context, "read_" + name)(destination).collect(check=True).to_arrow()
                except Exception as error:
                    raise AssertionError(f"UUID {name} native reimport failed") from error
                if name == "parquet":
                    tables["independent_parquet"] = pq.read_table(destination)
                else:
                    with pa.ipc.open_file(destination) as reader:
                        tables["independent_arrow"] = reader.read_all()
            assert not native.envelope.field_bool("fallback_attempted")
            assert not native.envelope.field_bool("external_engine_invoked")
            reports.append({"rows": tables["native"].num_rows,
                "digests": {name: _digest(table.to_pylist()) for name, table in tables.items()},
                "identities": {name: _identities(table.schema) for name, table in tables.items()}})
    return reports


class NativeUuidBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()

    def context(self, memory=GRANT):
        import shardloom as sl
        context = sl.context(transport="native", memory_bytes=memory, max_parallelism=1)
        self.addCleanup(context.close)
        return context

    def assert_values(self, result, expected, *, ordered=True):
        table = result.to_arrow()
        self.assertEqual(_digest(table.to_pylist(), ordered=ordered), _digest(expected, ordered=ordered))
        self.assertEqual(_digest(result.python_objects, ordered=ordered), _digest(expected, ordered=ordered))
        self.assertFalse(result.envelope.field_bool("fallback_attempted"))
        self.assertFalse(result.envelope.field_bool("external_engine_invoked"))
        return table

    def queries(self, context, count, *, padding=0):
        import pyarrow as pa
        table = _fixture(count + 2, padding=padding).slice(1, count)
        self.assertEqual(table.column("uid").chunk(0).offset, 1)
        left = context.from_arrow_table(table)
        tags = [{"uid": UUIDS[0], "tag": 10}, {"uid": UUIDS[0], "tag": 11},
                {"uid": UUIDS[2], "tag": 20}, {"uid": UUIDS[3], "tag": 30}, {"uid": None, "tag": 99}]
        right = context.from_arrow_table(pa.Table.from_arrays([
            _uuid_array([row["uid"] for row in tags]), pa.array([row["tag"] for row in tags], type=pa.int64())],
            names=["uid", "tag"]))
        selection = ["f.ordinal AS ordinal", "f.uid AS uid", "f.detail AS detail", "f.padding AS padding", "d.tag AS tag"]
        frame = left.where("ordinal >= 2").join(right, on="uid", how="left").select(*selection).sort(
            "ordinal", "tag", nulls="last")
        sql = context.sql("SELECT " + ", ".join(selection) + " FROM facts AS f LEFT JOIN tags AS d "
            "ON f.uid = d.uid WHERE f.ordinal >= 2 ORDER BY f.ordinal, d.tag NULLS LAST",
            relations={"facts": left, "tags": right})
        expected = []
        for row in table.to_pylist():
            if row["ordinal"] < 2:
                continue
            matching = [tag["tag"] for tag in tags if row["uid"] is not None and row["uid"] == tag["uid"]] or [None]
            expected.extend({**{name: row[name] for name in ("ordinal", "uid", "detail", "padding")}, "tag": tag}
                            for tag in matching)
        return (frame, sql), expected

    def test_uuid_sql_dataframe_join_stream_and_separate_process_roundtrip(self):
        context = self.context()
        queries, expected = self.queries(context, 41)
        identities = ["uid", "detail.wrapped", "detail.items[]"]
        with tempfile.TemporaryDirectory(prefix="shardloom-uuid-public-") as directory:
            paths = []
            for name, query in zip(("dataframe", "sql"), queries):
                result = query.collect(check=True)
                self.assertEqual(_identities(self.assert_values(result, expected).schema), identities)
                self.assertEqual(dict(result.result_schema)["uid"].name, "uuid")
                streamed = []
                with query.iter_batches(batch_rows=7) as batches:
                    for batch in batches:
                        arrow = batch.to_arrow()
                        self.assertLessEqual(arrow.num_rows, 7)
                        self.assertEqual(_identities(arrow.schema), identities)
                        streamed.extend(arrow.to_pylist())
                self.assertIsNotNone(batches.report)
                self.assertEqual(_digest(streamed), _digest(expected))
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
                self.assertTrue(all(value == identities for value in report["identities"].values()))

    def test_uuid_comparisons_group_distinct_extrema_and_selected_values(self):
        context = self.context()
        table = _fixture(41)
        source = context.from_arrow_table(table)
        rows = table.to_pylist()
        counts = Counter(row["uid"] for row in rows)
        order = sorted((value for value in counts if value is not None), key=lambda value: value.bytes)
        cases = [
            ("SELECT uid, COUNT(*) AS n FROM facts GROUP BY uid ORDER BY uid NULLS LAST",
             [{"uid": value, "n": counts[value]} for value in [*order, None]]),
            ("SELECT DISTINCT uid FROM facts ORDER BY uid DESC NULLS FIRST",
             [{"uid": value} for value in [None, *reversed(order)]]),
            ("SELECT MIN(uid) AS lo, MAX(uid) AS hi, COUNT(DISTINCT uid) AS n FROM facts",
             [{"lo": order[0], "hi": order[-1], "n": len(order)}]),
            ("SELECT ordinal, uid FROM facts WHERE uid = uid ORDER BY ordinal",
             [{"ordinal": row["ordinal"], "uid": row["uid"]} for row in rows if row["uid"] is not None]),
            ("SELECT ordinal FROM facts WHERE uid < other ORDER BY ordinal",
             [{"ordinal": row["ordinal"]} for row in rows if row["uid"] is not None and row["other"] is not None
              and row["uid"].bytes < row["other"].bytes]),
            ("SELECT COALESCE(uid, other) AS chosen, CASE WHEN ordinal % 2 = 0 THEN uid ELSE other END AS branch FROM facts ORDER BY ordinal",
             [{"chosen": row["uid"] if row["uid"] is not None else row["other"],
               "branch": row["uid"] if row["ordinal"] % 2 == 0 else row["other"]} for row in rows]),
        ]
        for sql, expected in cases:
            with self.subTest(sql=sql):
                self.assert_values(context.sql(sql, relations={"facts": source}).collect(check=True), expected)
        groups = {}
        for row in rows:
            key = json.dumps(row["detail"], sort_keys=True, default=_encoded)
            if key not in groups:
                groups[key] = {"detail": row["detail"], "n": 0}
            groups[key]["n"] += 1
        self.assert_values(context.sql("SELECT detail, COUNT(*) AS n FROM facts GROUP BY detail",
            relations={"facts": source}).collect(check=True), list(groups.values()), ordered=False)

    def test_uuid_unsupported_operations_are_bound_on_empty_and_nonempty_input(self):
        from shardloom.errors import ShardLoomCommandError
        context = self.context()
        for count in (0, 17):
            source = context.from_arrow_table(_fixture(count))
            for sql in ("SELECT CAST(uid AS VARCHAR) AS bad FROM facts", "SELECT TRY_CAST(uid AS VARCHAR) AS bad FROM facts",
                        "SELECT uid = raw AS bad FROM facts", "SELECT uid + 1 AS bad FROM facts", "SELECT SUM(uid) AS bad FROM facts",
                        "SELECT uid = '00000000-0000-0000-0000-000000000000' AS bad FROM facts",
                        "SELECT uid = ordinal AS bad FROM facts", "SELECT uid = DATE '2026-11-10' AS bad FROM facts"):
                with self.subTest(count=count, sql=sql):
                    with self.assertRaises(ShardLoomCommandError) as raised:
                        context.sql(sql, relations={"facts": source}).collect(check=True)
                    self.assertRegex(str(raised.exception), "UUID|admit|numeric|compatible|cast|dtype|type")
                    if "CAST(" in sql:
                        self.assertIn("UUID casts are not admitted", str(raised.exception))
                    self.assertFalse(raised.exception.envelope.fallback.attempted)

    def test_uuid_dictionary_and_parquet_logical_annotation_keep_declared_identity(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        context = self.context()
        indices = [0, 2, 7, None, 3, 1, 0, 6, 4, 5, 2, 3]
        encoded = pa.DictionaryArray.from_arrays(pa.array(indices, type=pa.int8()), _uuid_array(DOMAIN))
        table = pa.Table.from_arrays([encoded], names=["uid"]).slice(1, len(indices) - 2)
        expected = [{"uid": None if index is None else DOMAIN[index]} for index in indices[1:-1]]
        with tempfile.TemporaryDirectory(prefix="shardloom-uuid-formats-") as directory:
            ipc = Path(directory) / "dictionary.arrow"
            with pa.ipc.new_file(ipc, table.schema) as writer:
                writer.write_table(table)
            for route in ("caller", "arrow_ipc"):
                with self.subTest(route=route):
                    frame = context.from_arrow_table(table) if route == "caller" else context.read_arrow_ipc(ipc)
                    actual = self.assert_values(frame.collect(check=True), expected)
                    self.assertEqual(_identities(actual.schema), ["uid"])
                    distinct = sorted({row["uid"] for row in expected if row["uid"] is not None}, key=lambda value: value.bytes)
                    self.assert_values(context.sql("SELECT DISTINCT uid FROM facts ORDER BY uid NULLS LAST",
                        relations={"facts": frame}).collect(check=True), [{"uid": value} for value in [*distinct, None]])
            plain = _fixture(17)
            parquet = Path(directory) / "logical-only.parquet"
            pq.write_table(plain, parquet, store_schema=False)
            self.assertNotIn(b"ARROW:schema", pq.read_metadata(parquet).metadata or {})
            actual = self.assert_values(context.read_parquet(parquet).collect(check=True), plain.to_pylist())
            self.assertEqual(_identities(actual.schema), ["uid", "other", "detail.wrapped", "detail.items[]"])

    def test_uuid_nested_dictionary_values_keep_declared_identity(self):
        import pyarrow as pa
        context = self.context()
        indices = [0, 2, 7, None, 3, 1, 0, 6, 4, 5, 2, 3]
        encoded = pa.DictionaryArray.from_arrays(pa.array(indices, type=pa.int16()), _uuid_array(DOMAIN))
        nested = pa.StructArray.from_arrays([encoded], names=["wrapped"],
            mask=pa.array([row % 4 == 0 for row in range(len(indices))]))
        items = pa.ListArray.from_arrays(pa.array(range(len(indices) + 1), type=pa.int32()), encoded)
        table = pa.Table.from_arrays([encoded, nested, items], names=["uid", "nested", "items"]).slice(1, 9)
        result = context.from_arrow_table(table).collect(check=True)
        self.assertEqual(_identities(self.assert_values(result, table.to_pylist()).schema),
                         ["uid", "nested.wrapped", "items[]"])

    def test_uuid_malformed_declarations_fail_even_for_empty_input(self):
        import pyarrow as pa
        context = self.context()
        for count in (0, 2):
            for width, name, metadata in [(15, "arrow.uuid", b""), (17, "arrow.uuid", b""),
                    (16, "arrow.uuid", b"unexpected"), (16, "unadmitted.uuid", b"")]:
                with self.subTest(count=count, width=width, extension=name, metadata=metadata):
                    field = pa.field("uid", pa.binary(width), metadata={
                        b"ARROW:extension:name": name.encode(), b"ARROW:extension:metadata": metadata})
                    table = pa.Table.from_arrays([pa.array([bytes(width)] * count, type=field.type)],
                                                schema=pa.schema([field]))
                    with self.assertRaisesRegex(Exception, "UUID|extension|admit|unsupported"):
                        context.from_arrow_table(table).collect(check=True)

    def test_uuid_resource_denial_preserves_existing_owners(self):
        from shardloom.errors import ShardLoomCommandError
        denied = self.context(32 << 10)
        with self.assertRaises(ShardLoomCommandError) as raised:
            denied.from_arrow_table(_fixture(2, padding=8192)).collect(check=True)
        session = denied.client._native_backend.session
        self.assertIn("reservation denied", str(raised.exception))
        self.assertFalse(raised.exception.envelope.fallback.attempted)
        denied.close()
        gc.collect()
        self.assertEqual(session.snapshot()["reserved_bytes"], 0)
        context = self.context(1 << 20)
        fixture = _fixture(9)
        frame = context.from_arrow_table(fixture)
        result = frame.collect(check=True)
        retained = result.to_arrow()
        session = context.client._native_backend.session
        before = session.snapshot()["reserved_bytes"]
        with self.assertRaises(ShardLoomCommandError) as raised:
            context.from_arrow_table(_fixture(65, padding=32768))
        self.assertIn("reservation denied", str(raised.exception))
        gc.collect()
        self.assertEqual(session.snapshot()["reserved_bytes"], before)
        self.assert_values(result, fixture.to_pylist())
        context.close()
        del frame, result, retained
        gc.collect()
        self.assertEqual(session.snapshot()["reserved_bytes"], 0)

    def test_uuid_spilled_join_keys_cleanup_early_close_and_cancellation(self):
        context = self.context()
        queries, expected = self.queries(context, 2049, padding=2048)
        with tempfile.TemporaryDirectory(prefix="shardloom-uuid-spill-") as directory:
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
            session = context.client._native_backend.session
            context.close()
            del result, retained, queries, query, cancelled, early
            gc.collect()
            self.assertEqual(session.snapshot()["reserved_bytes"], 0)

    def test_uuid_empty_null_constant_results_and_final_child_ownership(self):
        for name, table in [("empty", _fixture(0)), ("null", _fixture(9, nulls=True)),
                            ("constant", _fixture(258, constant=True))]:
            with self.subTest(name=name):
                context = self.context()
                frame = context.from_arrow_table(table)
                session = context.client._native_backend.session
                result = frame.collect(check=True)
                exported = self.assert_values(result, table.to_pylist())
                self.assertEqual(_identities(exported.schema), ["uid", "other", "detail.wrapped", "detail.items[]"])
                child = exported.column("detail").chunk(0).field("items").values
                context.close()
                del frame, result, exported, context
                gc.collect()
                self.assertGreater(session.snapshot()["reserved_bytes"], 0)
                del child
                gc.collect()
                self.assertEqual(session.snapshot()["reserved_bytes"], 0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--reopen":
        print(json.dumps(_reopen(sys.argv[2:]), sort_keys=True))
    else:
        unittest.main()

