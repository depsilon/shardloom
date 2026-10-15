"""Finite native set operations through the installed Python binding."""

from __future__ import annotations

from decimal import Decimal
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

if __package__:
    from .test_native_binding import _load_native
else:
    from test_native_binding import _load_native


KINDS = ("union_all", "union", "intersect", "except_")
OPERATORS = dict(zip(KINDS, ("UNION ALL", "UNION", "INTERSECT", "EXCEPT")))
LEFT = [{"n": 3, "s": "港"}, {"n": None, "s": None}, {"n": 3, "s": "港"},
        {"n": 2, "s": "β"}, {"n": 1, "s": ""}]
RIGHT = [{"n": None, "s": None}, {"n": 2, "s": "β"}, {"n": 9, "s": " "},
         {"n": 9, "s": " "}]


def reference(left, right, kind):
    candidates = list(left) + list(right) if kind in {"union_all", "union"} else [
        row for row in left if (row in right) == (kind == "intersect")]
    if kind == "union_all":
        return candidates
    result = []
    for row in candidates:
        if row not in result:
            result.append(row)
    return result


def combine(left, right, kind, surface="dataframe"):
    from shardloom.query import SqlWorkflow

    if surface == "dataframe":
        return getattr(left, kind)(right)
    bindings = left._declared_sources()
    if left is not right:
        bindings += right._declared_sources()
    return SqlWorkflow(f"SELECT * FROM ({left._relation_statement()}) AS a {OPERATORS[kind]} "
                       f"SELECT * FROM ({right._relation_statement()}) AS b",
                       left.client, source_bindings=bindings)


class NativeSetBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()

    def client(self):
        import shardloom as sl

        client = sl.ShardLoomClient(transport="native", memory_bytes=32 << 20, max_parallelism=1)
        self.addCleanup(client.close)
        return client

    @staticmethod
    def source(client, rows, schema, arrow_schema=None):
        import shardloom as sl
        import pyarrow as pa

        events = []

        def produce():
            events.append("opened")
            try:
                for batch in ([], rows[:2], [], rows[2:], []):
                    yield pa.Table.from_pylist(batch, schema=arrow_schema) if arrow_schema else batch
                events.append("ended")
            finally:
                events.append("closed")

        return sl.from_batches(produce, schema=schema, streaming=True, client=client), events

    def completed(self, report, events, repeated=False):
        self.assertEqual(events, [["opened", "ended", "closed"]] * len(events))
        envelope = report.envelope
        self.assertEqual(envelope.field_int("native_input_batch_sources"), len(events))
        self.assertTrue(envelope.field_bool("native_input_end_observed"))
        self.assertEqual(envelope.field_int("native_input_replay_sources"), int(repeated))
        self.assertEqual(envelope.field_int("native_input_replay_passes"), int(repeated))
        self.assertFalse(envelope.fallback.attempted)
        self.assertFalse(envelope.fallback.allowed)
        self.assertEqual(envelope.field("external_engine_invoked"), "false")

    def test_native_set_independent_repeated_and_positional_sources_across_terminals(self):
        import pyarrow as pa
        import shardloom as sl

        client = self.client()
        for kind in KINDS:
            for shape in ("independent", "repeated", "positional"):
                for surface in ("dataframe", "sql"):
                    for terminal in ("collect", "batches", "single", "fanout"):
                        with self.subTest(kind=kind, shape=shape, surface=surface, terminal=terminal), \
                             tempfile.TemporaryDirectory(prefix="shardloom-native-set-") as directory:
                            root = Path(directory)
                            left, a = self.source(client, LEFT, {"n": "int16", "s": "utf8"})
                            if shape == "repeated":
                                right, events, rows = left, [a], LEFT
                            else:
                                names = ("other", "label") if shape == "positional" else ("n", "s")
                                incoming = [dict(zip(names, row.values())) for row in RIGHT]
                                right, b = self.source(client, incoming, {names[0]: "int32", names[1]: "utf8"},
                                    pa.schema([(names[0], pa.int32()), (names[1], pa.string())]))
                                events, rows = [a, b], RIGHT
                            query = combine(left, right, kind, surface)
                            expected = reference(LEFT, rows, kind)
                            self.assertEqual(events, [[]] * len(events))
                            held = []
                            with patch("subprocess.Popen", side_effect=AssertionError("child process")), \
                                 patch("shardloom._batches.BatchInput.encode", side_effect=AssertionError("JSON input")):
                                if terminal == "collect":
                                    report = query.collect(check=True)
                                    table = report.to_arrow()
                                    actual = table.to_pylist()
                                elif terminal == "batches":
                                    actual = []
                                    with query.iter_batches(batch_rows=1) as stream:
                                        for batch in stream:
                                            self.assertIsNone(stream.report)
                                            held.append(batch)
                                            actual.extend(batch.to_arrow().to_pylist())
                                        report = stream.report
                                    table = held[0].to_arrow() if held else None
                                else:
                                    formats = ("vortex", "parquet", "jsonl") if terminal == "fanout" else ("vortex",)
                                    targets = {name: root / ("result." + name) for name in formats}
                                    report = query.fanout(targets) if terminal == "fanout" else query.write_vortex(targets["vortex"])
                                    self.assertEqual(set(root.iterdir()), set(targets.values()))
                                    for name, path in targets.items():
                                        if name == "jsonl":
                                            actual = [json.loads(line) for line in path.read_text().splitlines()]
                                        else:
                                            table = getattr(sl, "read_" + name)(path, client=client).collect(check=True).to_arrow()
                                            actual = table.to_pylist()
                                        self.assertCountEqual(actual, expected)
                                self.assertCountEqual(actual, expected)
                                if table is not None:
                                    self.assertEqual(table.schema.names, ["n", "s"])
                                    self.assertEqual(table.schema.field("n").type,
                                                     pa.int16() if shape == "repeated" else pa.int32())
                                self.completed(report, events, shape == "repeated")
                                if terminal == "fanout":
                                    self.assertEqual(report.envelope.field_int("native_vortex_result_export_execution_count"), 1)
                                    self.assertEqual(report.envelope.field_int("native_vortex_result_export_replay_count"), 3)
                            del query, left, right, report
                            if held:
                                self.assertCountEqual([row for batch in held for row in batch.to_arrow().to_pylist()], expected)

    def test_native_set_late_failure_zero_limit_and_early_consumer_close(self):
        import pyarrow as pa
        import shardloom as sl
        from shardloom.errors import ShardLoomCommandError

        client = self.client()
        for kind in KINDS:
            for side in (0, 1):
                for mode in ("producer", "schema", "cancel"):
                    for terminal in ("collect", "zero", "fanout"):
                        with self.subTest(kind=kind, side=side, mode=mode, terminal=terminal), \
                             tempfile.TemporaryDirectory(prefix="shardloom-native-set-fault-") as directory:
                            events = []
                            sentinel = LookupError("set producer sentinel")

                            def broken():
                                events.append("opened")
                                try:
                                    yield LEFT[:2]
                                    events.append("fault")
                                    if mode == "producer":
                                        raise sentinel
                                    if mode == "schema":
                                        yield pa.table({"changed": pa.array([1], pa.int64())})
                                    else:
                                        client.cancel()
                                finally:
                                    events.append("closed")

                            source = sl.from_batches(broken, schema={"n": "int64", "s": "utf8"}, streaming=True, client=client)
                            other, _ = self.source(client, RIGHT, {"n": "int64", "s": "utf8"})
                            query = combine(source, other, kind) if side == 0 else combine(other, source, kind)
                            root = Path(directory)
                            foreign = root / "foreign"
                            foreign.write_bytes(b"preserve")
                            expected_error = {"producer": LookupError, "schema": ValueError, "cancel": ShardLoomCommandError}[mode]
                            with patch("subprocess.Popen", side_effect=AssertionError("child process")), \
                                 patch("shardloom._batches.BatchInput.encode", side_effect=AssertionError("JSON input")), \
                                 self.assertRaises(expected_error) as raised:
                                if terminal == "fanout":
                                    query.fanout({"vortex": root / "result.vortex", "jsonl": root / "result.jsonl"})
                                elif terminal == "zero":
                                    with query.limit(0).iter_batches(batch_rows=1) as stream:
                                        list(stream)
                                else:
                                    query.collect(check=True)
                            if mode == "producer":
                                self.assertIs(raised.exception, sentinel)
                            self.assertEqual(events, ["opened", "fault", "closed"])
                            self.assertEqual(list(root.iterdir()), [foreign])
                            self.assertEqual(foreign.read_bytes(), b"preserve")
                            self.assertEqual(sl.from_rows([{"n": 7}], schema={"n": "int64"}, client=client)
                                             .collect(check=True).to_arrow().to_pylist(), [{"n": 7}])

            events = [[], []]
            def source(index):
                def produce():
                    events[index].append("opened")
                    try:
                        for number in range(20):
                            yield [{"n": number + index * 10}]
                        events[index].append("ended")
                    finally:
                        events[index].append("closed")
                return sl.from_batches(produce, schema={"n": "int64"}, streaming=True, client=client)
            query = combine(source(0), source(1), kind)
            with query.iter_batches(batch_rows=1) as stream:
                retained = next(stream)
                self.assertIsNone(stream.report)
            self.assertIsNone(stream.report)
            self.assertEqual(retained.to_arrow().to_pylist(), [{"n": 10 if kind == "intersect" else 0}])
            self.assertTrue(any("opened" in trace and "ended" not in trace for trace in events))
            self.assertTrue(all(not trace or trace[-1] == "closed" for trace in events))

    def test_native_set_rich_full_row_equality_preserves_owned_output(self):
        import pyarrow as pa
        import shardloom as sl

        client = self.client()
        schema = {"small": "int16", "blob": "binary", "amount": "decimal128(12,2)",
                  "items": {"type": "list", "item": "int16"},
                  "record": {"type": "struct", "fields": {"label": "utf8", "value": "int32"}}}
        arrow_schema = pa.schema([
            ("small", pa.int16()), ("blob", pa.binary()), ("amount", pa.decimal128(12, 2)),
            ("items", pa.list_(pa.int16())),
            ("record", pa.struct([("label", pa.string()), ("value", pa.int32())])),
        ])
        rows = [{"small": -32768, "blob": b"\x00\xff", "amount": Decimal("-12.34"),
                 "items": [1, None, -2], "record": {"label": "港", "value": 7}},
                {"small": 32767, "blob": b"", "amount": Decimal("0.01"), "items": [], "record": None},
                dict.fromkeys(schema)]
        left_rows, right_rows = [rows[i] for i in (0, 2, 0)], [rows[i] for i in (2, 1, 1)]
        retained = []
        for kind in KINDS:
            for terminal in ("collect", "batches", "vortex"):
                with self.subTest(kind=kind, terminal=terminal), \
                     tempfile.TemporaryDirectory(prefix="shardloom-native-set-rich-") as directory, \
                     patch("subprocess.Popen", side_effect=AssertionError("child process")), \
                     patch("shardloom._batches.BatchInput.encode", side_effect=AssertionError("JSON input")):
                    left, a = self.source(client, left_rows, schema, arrow_schema)
                    right, b = self.source(client, right_rows, schema, arrow_schema)
                    query = combine(left, right, kind)
                    wanted = pa.Table.from_pylist(reference(left_rows, right_rows, kind), schema=arrow_schema)
                    if terminal == "collect":
                        report = query.collect(check=True)
                        tables = [report.to_arrow()]
                    elif terminal == "batches":
                        with query.iter_batches(batch_rows=1) as stream:
                            tables = [batch.to_arrow() for batch in stream]
                            report = stream.report
                    else:
                        path = Path(directory) / "result.vortex"
                        report = query.write_vortex(path)
                        tables = [sl.read_vortex(path, client=client).collect(check=True).to_arrow()]
                    actual = pa.concat_tables(tables) if tables else pa.Table.from_pylist([], schema=arrow_schema)
                    self.assertTrue(actual.equals(wanted, check_metadata=True), (actual, wanted))
                    self.completed(report, [a, b])
                    retained.append((actual, wanted))
        client.close()
        for actual, expected in retained:
            self.assertTrue(actual.equals(expected, check_metadata=True))
