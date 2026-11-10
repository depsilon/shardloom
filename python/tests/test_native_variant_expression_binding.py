"""Variant extraction through public declarations, native operators and persistence."""

from __future__ import annotations

from collections import Counter
from decimal import Decimal
import gc
import hashlib
import json
import math
import os
from pathlib import Path
import struct
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
from uuid import UUID

if __package__:
    from .test_native_binding import _load_native
    from .test_native_variant_binding import _binary, _field, _list, _text
else:
    from test_native_binding import _load_native
    from test_native_variant_binding import _binary, _field, _list, _text


GRANT = 128 << 20
NAMES = ("a.b[猫]", "blob", "flag", "items", "key", "name", "number", "uid")
_name_bytes = [name.encode() for name in NAMES]
_offsets = [sum(map(len, _name_bytes[:index])) for index in range(len(NAMES) + 1)]
METADATA = bytes([0x11, len(NAMES), *_offsets]) + b"".join(_name_bytes)


def _int(value):
    return b"\x18" + struct.pack("<q", value)


def _object(values):
    """Independent format-v1 object encoder; no engine value conversion."""
    fields = sorted(values, key=NAMES.index)
    offsets, data = [0], b""
    for name in fields:
        data += values[name]
        offsets.append(len(data))
    return (bytes([0x0e, len(fields), *[NAMES.index(name) for name in fields]])
            + b"".join(struct.pack("<I", offset) for offset in offsets) + data)


def _fixture(count, *, blob_bytes=19, all_null=False):
    import pyarrow as pa
    rows, expected = [], []
    for index in range(count):
        absent = all_null or index % 11 == 0
        uid = UUID(bytes=bytes((index + byte) % 256 for byte in range(16)))
        blob = bytes([index % 251]) * blob_bytes
        value = _object({"a.b[猫]": _int(-index), "blob": _binary(blob),
            "flag": b"\x04" if index % 2 else b"\x08",
            "items": _list([_int(index), b"\x00", _object({"name": _text(f"文-{index}")})]),
            "key": _int(index % 5), "name": _text(f"文-{index}"),
            "number": _int(index), "uid": b"\x50" + uid.bytes})
        rows.append({"id": index, "payload": None if absent else {"metadata": METADATA, "value": value}})
        expected.append({"id": index, "n": None if absent else index,
            "quoted": None if absent else -index, "name": None if absent else f"文-{index}",
            "blob": None if absent else blob, "uid": None if absent else uid,
            "flag": None if absent else bool(index % 2), "missing": None,
            "embedded_null": not absent, "missing_null": False})
    return pa.Table.from_pylist(rows, schema=pa.schema([
        pa.field("id", pa.int64()), _field(pa, "payload")])), expected


def _expressions(f):
    return [f.try_variant_get("payload", "$.number", "int64").alias("n"),
        f.try_variant_get("payload", '$["a.b[猫]"]', "int64").alias("quoted"),
        f.try_variant_get("payload", "$.items[2].name", "utf8").alias("name"),
        f.try_variant_get("payload", "$.blob", "binary").alias("blob"),
        f.try_variant_get(f.variant_get("payload", "$.uid"), "$", "uuid").alias("uid"),
        f.try_variant_get("payload", "$.flag", "boolean").alias("flag"),
        f.try_variant_get("payload", "$.absent", "int32").alias("missing"),
        f.is_variant_null(f.variant_get("payload", "$.items[1]")).alias("embedded_null"),
        f.is_variant_null(f.variant_get("payload", "$.absent")).alias("missing_null")]


def _digest(rows):
    def encode(value):
        if isinstance(value, bytes):
            return {"bytes": value.hex()}
        if isinstance(value, UUID):
            return {"uuid": value.hex}
        raise TypeError(type(value).__name__)
    return hashlib.sha256(json.dumps(rows, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), default=encode).encode()).hexdigest()


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
            tables = {"vortex": native.to_arrow()}
            for name in ("arrow_ipc", "parquet"):
                path = Path(raw).with_suffix("." + name)
                getattr(source, "write_" + name)(path, check=True)
                tables[name] = getattr(context, "read_" + name)(path).collect(check=True).to_arrow()
                if name == "parquet":
                    tables["independent_parquet"] = pq.read_table(path)
                else:
                    with pa.ipc.open_file(path) as reader:
                        tables["independent_arrow"] = reader.read_all()
            assert not native.envelope.field_bool("fallback_attempted")
            assert not native.envelope.field_bool("external_engine_invoked")
            reports.append({"rows": tables["vortex"].num_rows,
                "digests": {name: _digest(table.to_pylist()) for name, table in tables.items()},
                "uuid_types": {name: str(table.schema.field("uid").type) for name, table in tables.items()}})
    return reports


class NativeVariantExpressionBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()

    def context(self, memory=GRANT, policy="strict"):
        import shardloom as sl
        context = sl.context(transport="native", memory_bytes=memory,
                             max_parallelism=1, float_policy=policy)
        self.addCleanup(context.close)
        return context

    def assert_values(self, result, expected):
        table = result.to_arrow()
        self.assertEqual(table.to_pylist(), expected)
        self.assertFalse(result.envelope.field_bool("fallback_attempted"))
        self.assertFalse(result.envelope.field_bool("external_engine_invoked"))
        return table

    def test_variant_expressions_sql_dataframe_nulls_chunks_and_selected_values(self):
        import pyarrow as pa
        from shardloom import functions as f
        context = self.context()
        for count, all_null in [(0, False), (17, True), (33, False), (259, False)]:
            table, expected = _fixture(count + 2, all_null=all_null)
            table = table.slice(1, count)
            table = pa.Table.from_batches(table.to_batches(max_chunksize=7), schema=table.schema)
            source = context.from_arrow_table(table)
            projection = _expressions(f)
            queries = [source.select("id", *projection).sort("id", descending=True),
                context.sql("SELECT id," + ",".join(map(str, projection))
                            + " FROM data ORDER BY id DESC", relations={"data": source})]
            for query in queries:
                with self.subTest(count=count, all_null=all_null):
                    with patch("subprocess.Popen", side_effect=AssertionError("external execution")):
                        result = query.collect(check=True)
                    self.assert_values(result, list(reversed(expected[1:count + 1])))
                    dtypes = dict(result.result_schema)
                    self.assertEqual(dtypes["uid"].name, "uuid")
                    self.assertEqual(dtypes["missing"].name, "int32")
                    self.assertFalse(dtypes["embedded_null"].nullable)

    def test_variant_extracted_values_feed_join_group_window_and_topk(self):
        from shardloom import functions as f
        context = self.context()
        table, expected = _fixture(71)
        source = context.from_arrow_table(table).select("id",
            f.try_variant_get("payload", "$.key", "int64").alias("k"),
            f.try_variant_get("payload", "$.number", "int64").alias("n"))
        dimensions = context.from_rows([{"k": key, "tag": key * 10} for key in range(4)])
        joined = source.join(dimensions, on="k", how="left").select(
            "f.id AS id", "f.n AS n", "d.tag AS tag").sort("id", descending=True).limit(13)
        wanted = [{"id": row["id"], "n": row["n"],
                   "tag": None if row["n"] is None or row["id"] % 5 == 4 else row["id"] % 5 * 10}
                  for row in reversed(expected[-13:])]
        self.assert_values(joined.collect(check=True), wanted)
        counts = Counter(None if row["n"] is None else row["id"] % 5 for row in expected)
        grouped = context.sql("SELECT k,COUNT(*) AS n FROM values GROUP BY k ORDER BY k NULLS LAST",
                              relations={"values": source})
        self.assert_values(grouped.collect(check=True), [{"k": key, "n": counts[key]} for key in [0, 1, 2, 3, 4, None]])
        ranked = context.sql("SELECT id,n,ROW_NUMBER() OVER (ORDER BY n DESC NULLS LAST,id) AS rank "
                             "FROM values ORDER BY rank LIMIT 7", relations={"values": source})
        candidates = sorted((row for row in expected if row["n"] is not None), key=lambda row: -row["n"])[:7]
        self.assert_values(ranked.collect(check=True), [{"id": row["id"], "n": row["n"], "rank": index + 1}
                                                       for index, row in enumerate(candidates)])

    def test_variant_all_functions_write_five_routes_and_reopen_without_source(self):
        import shardloom as sl
        from shardloom import functions as f
        context = self.context()
        table, expected = _fixture(17)
        with tempfile.TemporaryDirectory(prefix="shardloom-variant-expressions-") as directory:
            root, paths = Path(directory), []
            source = root / "input.vortex"
            context.from_arrow_table(table).write_vortex(source, check=True)
            projection = _expressions(f)
            statement = "SELECT id," + ",".join(map(str, projection)) + " FROM '" + str(source).replace("'", "''") + "' ORDER BY id DESC"
            native = self.native.Session(self.native.ExecutionResources(GRANT, 1))
            cli = sl.ShardLoomClient(binary=os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY"),
                transport="subprocess", use_persistent_worker=False, memory_bytes=GRANT, max_parallelism=1)
            e = self.native.Expression
            get = lambda path, target: e.call("try_variant_get", [e.column("payload"), e.literal(path), e.literal(target)])
            raw = lambda path: e.call("variant_get", [e.column("payload"), e.literal(path)])
            relation = self.native.Relation.scan(str(source)).project([
                ("id", e.column("id")), ("n", get("$.number", "int64")),
                ("quoted", get('$["a.b[猫]"]', "int64")), ("name", get("$.items[2].name", "utf8")),
                ("blob", get("$.blob", "binary")),
                ("uid", e.call("try_variant_get", [raw("$.uid"), e.literal("$"), e.literal("uuid")])),
                ("flag", get("$.flag", "boolean")), ("missing", get("$.absent", "int32")),
                ("embedded_null", e.call("is_variant_null", [raw("$.items[1]")])),
                ("missing_null", e.call("is_variant_null", [raw("$.absent")]))]).sort([("id", True, None)])
            files = [(str(source), "vortex", None)]
            arguments = ["sql", "--sql", statement, "--request", "collect", "--bounded", "true",
                "--input", str(source), "--input-format", "vortex", "--memory-bytes", str(GRANT), "--max-parallelism", "1"]
            try:
                for route in ("dataframe", "sql", "typed_plan", "embedded_workflow", "cli_sql"):
                    path = root / (route + ".vortex")
                    if route == "dataframe":
                        report = context.read_vortex(source).select("id", *projection).sort("id", descending=True).write_vortex(path, check=True)
                        field = report.envelope.field
                    elif route == "cli_sql":
                        report = sl.sql(statement, client=cli, input=str(source), input_format="vortex").write_vortex(path, check=True)
                        field = report.envelope.field
                    else:
                        plan = (native.sql(statement, files=files) if route == "sql" else
                                native.plan(relation, files=files) if route == "typed_plan" else native.workflow(arguments))
                        with patch("subprocess.Popen", side_effect=AssertionError("external execution")):
                            report = plan.write(path, "write_vortex")
                        field = report.get
                    self.assertEqual(field("fallback_attempted"), "false", route)
                    self.assertEqual(field("external_engine_invoked"), "false", route)
                    paths.append(str(path))
            finally:
                native.close()
                cli.close()
                context.close()
            source.unlink()
            child = subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()), "--reopen", *paths],
                env=os.environ.copy(), capture_output=True, text=True, timeout=180, check=False)
            self.assertEqual(child.returncode, 0, child.stderr)
            reports = json.loads(child.stdout)
            self.assertEqual(len(reports), 5)
            for report in reports:
                self.assertEqual(report["rows"], len(expected))
                self.assertEqual(set(report["digests"].values()), {_digest(list(reversed(expected)))})
                self.assertEqual(set(report["uuid_types"].values()), {"extension<arrow.uuid>"})

    def test_variant_extracted_payloads_spill_and_release_after_stream_cancellation(self):
        from shardloom import functions as f
        context = self.context()
        table, expected = _fixture(2049, blob_bytes=1024)
        source = context.from_arrow_table(table)
        query = source.select("id", *_expressions(f)).sort("id", descending=True)
        expected.reverse()
        with tempfile.TemporaryDirectory(prefix="shardloom-variant-expression-spill-") as directory:
            workspace = Path(directory)
            spill = {"workspace": str(workspace), "quota_bytes": 64 << 20, "buffer_bytes": 1 << 20}
            result = query.collect(spill=spill, check=True)
            self.assertGreater(result.envelope.field_int("relational_spill_runs_written"), 0)
            self.assertTrue(result.envelope.field_bool("relational_spill_owned_cleanup_completed"))
            self.assert_values(result, expected)
            self.assertEqual(list(workspace.iterdir()), [])
            with query.iter_batches(batch_rows=31, spill=spill) as early:
                retained = next(early).to_arrow()
            self.assertIsNone(early.report)
            self.assertEqual(retained.to_pylist(), expected[:31])
            cancelled = query.iter_batches(batch_rows=31, spill=spill)
            next(cancelled)
            context.client.cancel()
            with self.assertRaisesRegex(Exception, "cancelled"):
                next(cancelled)
            cancelled.close()
            self.assertIsNone(cancelled.report)
            self.assertEqual(list(workspace.iterdir()), [])

    def test_variant_typed_targets_exact_widths_and_tolerant_casts(self):
        import pyarrow as pa
        from shardloom import functions as f
        context = self.context()
        values = [_int(-1), _int(7), _int(256), _text("7"), b"\x00", None]
        table = pa.Table.from_pylist([{"payload": None if value is None else {"metadata": METADATA, "value": value}}
                                     for value in values], schema=pa.schema([_field(pa, "payload")]))
        source = context.from_arrow_table(table)
        targets = [("int8", [-1, 7, None, None, None, None]),
                   ("uint8", [None, 7, None, None, None, None])]
        targets += [(name, [-1, 7, 256, None, None, None]) for name in ("int16", "int32", "int64")]
        targets += [(name, [None, 7, 256, None, None, None]) for name in ("uint16", "uint32", "uint64")]
        targets += [(name, [-1.0, 7.0, 256.0, None, None, None]) for name in ("float16", "float32", "float64")]
        targets += [("decimal(8,2)", [Decimal("-1.00"), Decimal("7.00"), Decimal("256.00"), Decimal("7.00"), None, None])]
        for target, expected in targets:
            expression = f.try_variant_get("payload", "$", target).alias("value")
            for query in (source.select(expression), context.sql(f"SELECT {expression} FROM data", relations={"data": source})):
                with self.subTest(target=target):
                    self.assertEqual(query.collect(check=True).to_arrow().column("value").to_pylist(), expected)

    def test_variant_invalid_controls_and_lazy_wrong_type_rejected_before_producer(self):
        table, _ = _fixture(3)
        session = self.native.Session(self.native.ExecutionResources(GRANT, 1))
        self.addCleanup(session.close)
        # from_arrow is an explicit eager import; schema inspection must not
        # execute the subsequently declared expressions.
        source = session.from_arrow(table)
        for expression in ("variant_get(payload)", "variant_get(payload,'$', 'int64')",
            "variant_get(payload,id)", "variant_get(payload,NULL)", "variant_get(id,'$')",
            "variant_get(payload,'$[*]')", "variant_get(payload,'$..name')", "variant_get(payload,'$[-1]')",
            "try_variant_get(payload,'$',id)", "try_variant_get(payload,'$',NULL)",
            "try_variant_get(payload,'$','struct<x:int>')", "try_variant_get(payload,'$','duration(us)')",
            "is_variant_null(id)"):
            with self.subTest(expression=expression), self.assertRaisesRegex(Exception, "(?i)variant|argument|literal|arity"):
                session.sql(f"SELECT {expression} AS bad FROM 'memory://variant-controls'",
                            sources=[("memory://variant-controls", source)]).schema_json()
        opened = []
        def batches():
            opened.append(True)
            yield [{"payload": 7}]
        context = self.context()
        lazy = context.from_batches(batches, schema={"payload": "int64"}, streaming=True)
        for expression in ("variant_get(payload,'$')", "try_variant_get(payload,'$','int64')",
                           "is_variant_null(payload)"):
            with self.subTest(expression=expression), self.assertRaisesRegex(Exception, "native Variant operand"):
                context.sql(f"SELECT {expression} AS bad FROM data", relations={"data": lazy}).schema()
            self.assertEqual(opened, [])

    def test_variant_raw_and_shredded_text_have_identical_typed_conversions(self):
        import pyarrow as pa
        from shardloom import functions as f
        context = self.context()
        encoded = _field(pa, "payload", pa.struct([pa.field("metadata", pa.binary(), nullable=False),
            pa.field("value", pa.binary()), pa.field("typed_value", pa.string())]))
        table = pa.Table.from_pylist([
            {"payload": {"metadata": METADATA, "value": _text("7")}},
            {"payload": {"metadata": METADATA, "typed_value": "7"}},
            {"payload": {"metadata": METADATA, "value": b"\x00"}},
            {"payload": None}], schema=pa.schema([encoded]))
        source = context.from_arrow_table(table)
        projection = [f.try_variant_get("payload", "$", "int64").alias("integer"),
            f.try_variant_get("payload", "$", "decimal(8,2)").alias("decimal"),
            f.try_variant_get("payload", "$", "utf8").alias("text"),
            f.is_variant_null("payload").alias("null")]
        expected = [{"integer": None, "decimal": Decimal("7.00"), "text": "7", "null": False}] * 2
        expected += [{"integer": None, "decimal": None, "text": None, "null": value} for value in (True, False)]
        for query in (source.select(*projection), context.sql("SELECT " + ",".join(map(str, projection))
                       + " FROM data", relations={"data": source})):
            self.assert_values(query.collect(check=True), expected)

    def test_variant_temporal_targets_preserve_units_zones_and_safe_conversion_pairs(self):
        import pyarrow as pa
        from shardloom import functions as f
        context = self.context()
        cases = [(0x2c, "<i", [2, -1], "date32", pa.date32(), [2, -1]),
                 (0x2c, "<i", [2, -1], "date64", pa.date64(), [172800000, -86400000])]
        for unit, ticks in [("s", [2, None]), ("ms", [2000, None]),
                            ("us", [2000000, 2000001]), ("ns", [2000000000, 2000001000])]:
            cases.append((0x44, "<q", [2000000, 2000001], f"time({unit})",
                          pa.time32(unit) if unit in ("s", "ms") else pa.time64(unit), ticks))
            for code, zone in [(0x30, "UTC"), (0x34, None)]:
                target = f"timestamp({unit},'UTC')" if zone else f"timestamp({unit})"
                cases.append((code, "<q", [2000000, 2000001], target, pa.timestamp(unit, tz=zone), ticks))
        for code, zone in [(0x48, "UTC"), (0x4c, None)]:
            target = "timestamp(ns,'UTC')" if zone else "timestamp(ns)"
            cases.append((code, "<q", [2000000000, 2000000001], target,
                          pa.timestamp("ns", tz=zone), [2000000000, 2000000001]))
            # The pinned provider's microsecond accessor only admits its
            # corresponding microsecond Variant tag, even for aligned nanos.
            target = "timestamp(us,'UTC')" if zone else "timestamp(us)"
            cases.append((code, "<q", [2000000000, 2000000001], target,
                          pa.timestamp("us", tz=zone), [None, None]))
        for code, packing, values, target, dtype, expected in cases:
            payloads = [bytes([code]) + struct.pack(packing, value) for value in values] + [b"\x00", None]
            table = pa.Table.from_pylist([
                {"payload": None if value is None else {"metadata": METADATA, "value": value}}
                for value in payloads], schema=pa.schema([_field(pa, "payload")]))
            source = context.from_arrow_table(table)
            expression = f.try_variant_get("payload", "$", target).alias("value")
            for query in (source.select(expression), context.sql(f"SELECT {expression} FROM data", relations={"data": source})):
                with self.subTest(tag=code, target=target):
                    actual = query.collect(check=True).to_arrow().column("value")
                    self.assertEqual(actual.type, dtype)
                    integer = pa.int32() if pa.types.is_date32(dtype) or pa.types.is_time32(dtype) else pa.int64()
                    self.assertEqual(actual.cast(integer).to_pylist(), [*expected, None, None])

    def test_variant_extracted_last_child_owns_credit_after_context_close(self):
        from shardloom import functions as f
        for count in (0, 17):
            with self.subTest(count=count):
                context = self.context()
                table, expected = _fixture(count)
                frame = context.from_arrow_table(table).select(
                    f.try_variant_get("payload", "$.blob", "binary").alias("blob"))
                result = frame.collect(check=True)
                arrow = result.to_arrow()
                self.assertEqual(arrow.column("blob").to_pylist(), [row["blob"] for row in expected])
                child = arrow.column("blob").chunk(0)
                session = context.client._native_backend.session
                context.close()
                del table, frame, result, arrow, context
                gc.collect()
                self.assertGreater(session.snapshot()["reserved_bytes"], 0)
                del child
                gc.collect()
                self.assertEqual(session.snapshot()["reserved_bytes"], 0)

    def test_variant_float_observation_policy_and_workspace_denial(self):
        import pyarrow as pa
        from shardloom import functions as f
        from shardloom.errors import ShardLoomCommandError
        raw = b"\x1c" + struct.pack("<Q", 0x7ff8000000000042)
        table = pa.Table.from_pylist([{"payload": {"metadata": METADATA, "value": raw}}],
                                    schema=pa.schema([_field(pa, "payload")]))
        strict = self.context()
        source = strict.from_arrow_table(table)
        self.assertEqual(source.select(f.variant_get("payload", "$").alias("raw")).collect(check=True).to_arrow().num_rows, 1)
        with self.assertRaisesRegex(ShardLoomCommandError, "nonfinite|finite|NaN"):
            source.select(f.try_variant_get("payload", "$", "float64").alias("n")).collect(check=True)
        extended = self.context(policy="extended")
        actual = extended.from_arrow_table(table).select(f.try_variant_get("payload", "$", "float64").alias("n")).collect(check=True)
        self.assertTrue(math.isnan(actual.to_arrow().column("n")[0].as_py()))
        denied = self.context(memory=2 << 20)
        # Select a present value; the regular fixture's first row is SQL NULL.
        table, _ = _fixture(2, blob_bytes=16 << 10)
        frame = denied.from_arrow_table(table.slice(1)).select(f.try_variant_get("payload", "$.blob", "binary").alias("blob"))
        with self.assertRaisesRegex(ShardLoomCommandError, "reservation denied"):
            frame.collect(check=True)
        session = denied.client._native_backend.session
        denied.close()
        del frame
        gc.collect()
        self.assertEqual(session.snapshot()["reserved_bytes"], 0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--reopen":
        print(json.dumps(_reopen(sys.argv[2:])))
    else:
        unittest.main()
