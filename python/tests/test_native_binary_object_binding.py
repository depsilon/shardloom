"""Complete managed byte ownership through the real native public interface."""

from __future__ import annotations

import gc
import hashlib
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
else:
    from test_native_binding import _load_native


def _payload(length):
    return (bytes(range(251)) * (length // 251 + 1))[:length]


def _reopen_job(path):
    _load_native()
    import shardloom as sl

    context = sl.context(transport="native", memory_bytes=64 << 20, max_parallelism=1)
    frame = context.read_vortex(path)
    result = frame.collect(check=True)
    rows = result.python_objects
    before = context.binary_object_snapshot()
    assert before["payload_read_bytes"] == 0, before
    context.close()
    return {"rows": [{"id": row["id"], "ownership": row["payload"].ownership,
                       "length": row["payload"].byte_length,
                       "sha256": hashlib.sha256(row["payload"].read()).hexdigest()}
                      for row in rows], "before": before}


class NativeBinaryObjectTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()

    def context(self, memory=64 << 20):
        import shardloom as sl
        result = sl.context(transport="native", memory_bytes=memory, max_parallelism=1)
        self.addCleanup(result.close)
        return result

    def test_binary_object_public_reads_are_exact_lazy_bounded_and_retained(self):
        from shardloom import types as t

        context = self.context()
        with tempfile.TemporaryDirectory(prefix="shardloom-managed-objects-") as directory:
            root = Path(directory)
            data = _payload((2 << 20) + 3)
            original = root / "caller.bin"
            original.write_bytes(data)
            store = context.binary_object_store(root, max_bytes=len(data) + 8)
            managed = store.import_file(original)
            external = context.external_binary_object(original, start=7, end=400)
            empty = context.binary_object(b"")
            small = context.binary_object(b"\x00\xff\x80small")
            self.assertEqual(managed.byte_length, len(data))
            self.assertEqual(managed.ownership, "managed")
            self.assertEqual(external.ownership, "external")
            self.assertEqual(store.snapshot["retained_bytes"], len(data))
            result = context.from_rows([
                {"id": 1, "inline": b"\x00\xff", "payload": managed},
                {"id": 2, "inline": b"", "payload": external},
                {"id": 3, "inline": None, "payload": empty},
                {"id": 4, "inline": b"inline", "payload": None},
                {"id": 5, "inline": b"x", "payload": small},
            ], schema={"id": "int64", "inline": "binary", "payload": t.binary_object()}).collect(check=True)
            rows = result.python_objects
            self.assertEqual(result.result_schema[-1][1], t.binary_object())
            self.assertEqual(context.binary_object_snapshot()["payload_read_bytes"], 0)
            self.assertEqual(rows[0]["payload"].read_range(65530, 65550), data[65530:65550])
            self.assertEqual(context.binary_object_snapshot()["payload_read_bytes"], 2 * 65536)
            self.assertEqual(rows[1]["payload"].read(), data[7:400])
            self.assertEqual(rows[2]["payload"].read(), b"")
            self.assertIsNone(rows[3]["payload"])
            self.assertEqual(rows[4]["payload"].read(), b"\x00\xff\x80small")
            self.assertEqual([row["inline"] for row in rows], [b"\x00\xff", b"", None, b"inline", b"x"])
            self.assertEqual(managed.read_range(len(data), len(data)), b"")
            for start, end in [(10, 9), (0, len(data) + 1), (-1, 2), (True, 2), (0, 1 << 64)]:
                with self.subTest(range=(start, end)), self.assertRaises(Exception):
                    managed.read_range(start, end)
            with self.assertRaisesRegex(Exception, "cancelled|deadline"):
                managed.read(timeout=0)
            with self.assertRaisesRegex(Exception, "ceiling"):
                store.put_bytes(b"overflow!")
            with self.assertRaisesRegex(Exception, "unsigned 64-bit integer"):
                context.binary_object_store(root, max_bytes=True)
            context.close()
            del result, managed, small, empty, store
            gc.collect()
            self.assertEqual(rows[0]["payload"].read(), data)
            original.unlink()
            with self.assertRaises(Exception):
                rows[1]["payload"].read()
            self.assertEqual(rows[0]["payload"].read_range(0, 5), data[:5])

    def test_binary_object_nested_rows_join_sort_spill_and_stream_keep_handles(self):
        from shardloom import types as t

        context = self.context(128 << 20)
        first = context.binary_object(b"first\x00\xff")
        second = context.binary_object(b"second")
        schema = {"id": "int64", "payload": t.binary_object(),
                  "nested": t.struct({"items": t.list_(t.binary_object(nullable=False)),
                                      "required": t.binary_object(nullable=False)})}
        rows = [{"id": index, "payload": None if index % 13 == 0 else first if index % 2 else second,
                 "nested": None if index % 7 == 0 else {"items": [first, second], "required": first}}
                for index in range(4101)]
        with patch("shardloom._input_schema.encode_cell", side_effect=AssertionError("JSON cells")):
            source = context.from_rows(rows, schema=schema)
            # Materialize once before releasing ordinary Python input handles.
            base = source.collect(check=True)
            left = context.from_result(base)
        right = context.from_rows([{"id": index, "tag": tag}
                                   for index in range(1, 4101, 3) for tag in (1, 2)])
        sql = context.sql(
            "SELECT f.id AS id, f.payload AS payload, f.nested AS nested, d.tag AS tag "
            "FROM objects AS f LEFT JOIN duplicates AS d ON f.id = d.id "
            "WHERE f.id > 2 ORDER BY f.id DESC, d.tag DESC NULLS LAST",
            relations={"objects": left, "duplicates": right},
        )
        query = left.where("id > 2").join(right, on="id", how="left").select(
            "f.id AS id", "f.payload AS payload", "f.nested AS nested", "d.tag AS tag"
        ).sort("id", "tag", descending=True, nulls="last")
        expected = [(index, tag) for index in range(4100, 2, -1)
                    for tag in ((2, 1) if index % 3 == 1 else (None,))]
        with tempfile.TemporaryDirectory(prefix="shardloom-object-spill-") as directory:
            spill = {"workspace": directory, "quota_bytes": 64 << 20, "buffer_bytes": 1 << 20}
            for frame in (query, sql):
                result = frame.collect(spill=spill, check=True)
                self.assertGreater(result.envelope.field_int("relational_spill_runs_written"), 0)
                actual = result.python_objects
                self.assertEqual([(row["id"], row["tag"]) for row in actual], expected)
                for row in actual:
                    index = row["id"]
                    if index % 13 == 0:
                        self.assertIsNone(row["payload"])
                    else:
                        self.assertEqual(row["payload"].read(), b"first\x00\xff" if index % 2 else b"second")
                    if index % 7 == 0:
                        self.assertIsNone(row["nested"])
                    else:
                        self.assertEqual([value.read() for value in row["nested"]["items"]], [b"first\x00\xff", b"second"])
                self.assertEqual(list(Path(directory).iterdir()), [])
            with query.iter_batches(batch_rows=31, spill=spill) as stream:
                retained = next(stream)
                self.assertEqual(retained.result_schema[1][1], t.binary_object())
                self.assertEqual(len(retained.python_objects), 31)
            cancelled = query.iter_batches(batch_rows=31, spill=spill)
            next(cancelled)
            context.client.cancel()
            with self.assertRaisesRegex(Exception, "cancelled"):
                next(cancelled)
            cancelled.close()
            self.assertIsNone(cancelled.report)
            self.assertEqual(list(Path(directory).iterdir()), [])
            context.close()
            escaped = retained.native_result.slice(1, 2).row_export()
            self.assertEqual([(row["id"], row["tag"]) for row in escaped], expected[1:3])
            self.assertEqual(list(Path(directory).iterdir()), [])
            self.assertTrue(all(row["nested"] is None or row["nested"]["required"].read() == b"first\x00\xff" for row in escaped))

    def test_binary_object_explicit_cross_session_transfer_and_bare_arrow_refusal(self):
        import pyarrow as pa
        from shardloom import types as t

        source, target = self.context(), self.context()
        payload = source.binary_object(b"shared ownership\x00\xff")
        frame = source.from_rows([{"payload": payload}], schema={"payload": t.binary_object()})
        result = frame.collect(check=True)
        table = result.to_arrow()
        field = table.schema.field("payload")
        self.assertEqual(field.metadata[b"ARROW:extension:name"], b"shardloom.binary-object.v1")
        with self.assertRaisesRegex(Exception, "another session"):
            target.from_rows([{"payload": payload}], schema={"payload": t.binary_object()}).collect(check=True)
        with self.assertRaisesRegex(Exception, "not bound|unbound|unavailable|not admitted|unresolved"):
            target.from_arrow_table(table).collect(check=True)
        admitted = target.import_binary_object(payload)
        target_result = target.from_rows([{"payload": admitted}], schema={"payload": t.binary_object()}).collect(check=True)
        self.assertEqual(target_result.python_objects[0]["payload"].read(), payload.read())
        fresh = self.context()
        transferred = fresh.from_result(result).collect(check=True)
        source.close()
        del payload, result, frame, table
        gc.collect()
        self.assertEqual(transferred.python_objects[0]["payload"].read(), b"shared ownership\x00\xff")
        self.assertEqual(fresh.binary_object_snapshot()["payload_read_bytes"], 0)
        malformed = pa.Table.from_arrays([pa.array([b"invalid descriptor"], pa.binary())], schema=pa.schema([field]))
        with self.assertRaises(Exception):
            fresh.from_arrow_table(malformed).collect(check=True)

    def test_binary_object_portable_export_reopens_in_a_fresh_process_after_relocation(self):
        from shardloom import types as t

        context = self.context()
        with tempfile.TemporaryDirectory(prefix="shardloom-object-portable-") as directory:
            root = Path(directory)
            input_path = root / "input.bin"
            data = _payload((2 << 20) + 3)
            input_path.write_bytes(data)
            store = context.binary_object_store(root, max_bytes=8 << 20)
            wanted = store.import_file(input_path)
            unused = store.put_bytes(b"unselected" * 400000)
            empty = context.binary_object(b"")
            frame = context.from_rows([{"id": 1, "payload": wanted}, {"id": 2, "payload": unused},
                                       {"id": 3, "payload": wanted}, {"id": 4, "payload": empty}],
                                      schema={"id": "int64", "payload": t.binary_object(nullable=False)})
            output = root / "original.vortex"
            frame.where("id <> 2").sort("id", descending=True).write_vortex(output, check=True)
            self.assertLess(output.stat().st_size, len(data) + (256 << 10))
            context.close()
            del frame, wanted, unused, empty, store
            gc.collect()
            input_path.unlink()
            moved = root / "moved.vortex"
            output.rename(moved)
            child = subprocess.run([sys.executable, str(Path(__file__).resolve()), "--reopen", str(moved)],
                                   capture_output=True, text=True, timeout=120, check=True)
            observed = json.loads(child.stdout)
            self.assertEqual(observed["before"]["managed_objects"], 2)
            self.assertEqual(observed["before"]["managed_bytes"], len(data))
            self.assertEqual(observed["rows"], [
                {"id": 4, "ownership": "managed", "length": 0, "sha256": hashlib.sha256(b"").hexdigest()},
                *[{"id": index, "ownership": "managed", "length": len(data), "sha256": hashlib.sha256(data).hexdigest()}
                  for index in (3, 1)],
            ])

    def test_binary_object_incompatible_sinks_and_failed_producers_do_not_publish(self):
        from shardloom import types as t

        context = self.context()
        value = context.binary_object(b"owned")
        frame = context.from_rows([{"id": 1, "payload": value}], schema={"id": "int64", "payload": t.binary_object()})
        result = frame.collect(check=True)
        produced = result.to_arrow()
        with tempfile.TemporaryDirectory(prefix="shardloom-object-failures-") as directory:
            root = Path(directory)
            for name in ("parquet", "arrow_ipc", "orc", "jsonl", "csv"):
                with self.subTest(format=name), self.assertRaisesRegex(Exception, "binary object|binary-object|owned binary"):
                    frame.write(root / ("denied." + name), output_format=name, check=True)
                self.assertEqual(list(root.iterdir()), [])
            events = []
            failure = LookupError("object producer failed")

            def broken():
                events.append("opened")
                try:
                    yield produced
                    raise failure
                finally:
                    events.append("closed")

            streamed = context.from_batches(broken, schema=dict(result.result_schema), streaming=True)
            with self.assertRaises(LookupError) as raised:
                streamed.write_vortex(root / "failed.vortex", check=True)
            self.assertIs(raised.exception, failure)
            self.assertEqual(events, ["opened", "closed"])
            self.assertEqual(list(root.iterdir()), [])

    def test_binary_object_keys_comparison_and_arithmetic_fail_without_payload_reads(self):
        from shardloom import types as t

        context = self.context()
        with tempfile.TemporaryDirectory(prefix="shardloom-object-operands-") as directory:
            store = context.binary_object_store(directory, max_bytes=1 << 20)
            payload = store.put_bytes(_payload(65539))
            frame = context.from_rows([
                {"id": 1, "payload": payload}, {"id": 2, "payload": None},
            ], schema={"id": "int64", "payload": t.binary_object()})
            statements = [
                "SELECT payload FROM objects ORDER BY payload",
                "SELECT payload, COUNT(*) AS n FROM objects GROUP BY payload",
                "SELECT payload + 1 AS value FROM objects",
                "SELECT id FROM objects WHERE payload = payload",
            ]
            for statement in statements:
                with self.subTest(sql=statement), self.assertRaises(Exception):
                    context.sql(statement, relations={"objects": frame}).collect(check=True)
                self.assertEqual(context.binary_object_snapshot()["payload_read_bytes"], 0)
            self.assertEqual(payload.read_range(0, 3), _payload(3))

    def test_binary_object_native_writes_are_portable_across_all_five_interfaces(self):
        import shardloom as sl
        from shardloom import types as t

        context = self.context()
        payload = _payload(131079)
        owned = context.binary_object(payload)
        empty = context.binary_object(b"")
        with tempfile.TemporaryDirectory(prefix="shardloom-object-interfaces-") as directory:
            root = Path(directory)
            source = root / "input.vortex"
            context.from_rows(
                [{"id": 1, "payload": owned}, {"id": 2, "payload": None},
                 {"id": 3, "payload": owned}, {"id": 4, "payload": empty}],
                schema={"id": "int64", "payload": t.binary_object()},
            ).write_vortex(source, check=True)
            statement = "SELECT id, payload FROM '" + str(source).replace("'", "''") + "' WHERE id <> 2 ORDER BY id DESC"
            native = self.native.Session(self.native.ExecutionResources(64 << 20, 1))
            cli = sl.ShardLoomClient(
                binary=os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY"),
                transport="subprocess", use_persistent_worker=False,
                memory_bytes=64 << 20, max_parallelism=1,
            )
            files = [(str(source), "vortex", None)]
            expression = self.native.Expression
            relation = self.native.Relation.scan(str(source)).filter(
                expression.column("id") != expression.literal(2)
            ).sort([("id", True, None)])
            arguments = ["sql", "--sql", statement, "--request", "collect", "--bounded", "true",
                         "--input", str(source), "--input-format", "vortex",
                         "--memory-bytes", str(64 << 20), "--max-parallelism", "1"]
            outputs = []
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
                                    native.plan(relation, files=files) if surface == "typed_plan" else
                                    native.workflow(arguments))
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
            for path in outputs:
                with self.subTest(reopen=path.stem):
                    fresh = self.context()
                    rows = fresh.read_vortex(path).collect(check=True).python_objects
                    self.assertEqual(fresh.binary_object_snapshot()["payload_read_bytes"], 0)
                    self.assertEqual([(row["id"], row["payload"].read()) for row in rows],
                                     [(4, b""), (3, payload), (1, payload)])
                    fresh.close()


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--reopen":
        print(json.dumps(_reopen_job(sys.argv[2]), sort_keys=True))
    else:
        unittest.main()
