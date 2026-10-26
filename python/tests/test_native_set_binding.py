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
    from shardloom.query import LazyFrame

    if surface == "dataframe":
        return getattr(left, kind)(right)
    bindings = left._declared_sources()
    if left is not right:
        bindings += right._declared_sources()
    return LazyFrame.from_sql(f"SELECT * FROM ({left._relation_statement()}) AS a {OPERATORS[kind]} "
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
        for spill, kind in ((spill, kind) for spill in (False, True) for kind in KINDS):
            for terminal in ("collect", "batches", "vortex"):
                with self.subTest(kind=kind, terminal=terminal, spill=spill), \
                     tempfile.TemporaryDirectory(prefix="shardloom-native-set-rich-") as directory, \
                     patch("subprocess.Popen", side_effect=AssertionError("child process")), \
                     patch("shardloom._batches.BatchInput.encode", side_effect=AssertionError("JSON input")):
                    left, a = self.source(client, left_rows, schema, arrow_schema)
                    right, b = self.source(client, right_rows, schema, arrow_schema)
                    query = combine(left, right, kind)
                    workspace = Path(directory) / "runs"
                    workspace.mkdir()
                    options = {"spill": {"workspace": str(workspace), "quota_bytes": 64 << 20,
                                          "buffer_bytes": 1 << 20}} if spill else {}
                    wanted = pa.Table.from_pylist(reference(left_rows, right_rows, kind), schema=arrow_schema)
                    if terminal == "collect":
                        report = query.collect(check=True, **options)
                        tables = [report.to_arrow()]
                    elif terminal == "batches":
                        with query.iter_batches(batch_rows=1, **options) as stream:
                            tables = [batch.to_arrow() for batch in stream]
                            report = stream.report
                    else:
                        path = Path(directory) / "result.vortex"
                        report = query.write_vortex(path, **options)
                        tables = [sl.read_vortex(path, client=client).collect(check=True).to_arrow()]
                    actual = pa.concat_tables(tables) if tables else pa.Table.from_pylist([], schema=arrow_schema)
                    self.assertTrue(actual.equals(wanted, check_metadata=True), (actual, wanted))
                    self.completed(report, [a, b])
                    self.assertEqual(report.envelope.field_int("relational_ordered_set_stages"),
                                     int(spill and kind != "union_all"))
                    self.assertEqual(list(workspace.iterdir()), [])
                    retained.append((actual, wanted))
        client.close()
        for actual, expected in retained:
            self.assertTrue(actual.equals(expected, check_metadata=True))

    def test_native_set_membership_spill_bounded_batches_and_fanout_complete_exactly(self):
        import shardloom as sl

        count, width, offset, grant = 8193, 2048, 4096, 16 << 20
        client = sl.ShardLoomClient(transport="native", memory_bytes=grant, max_parallelism=1)
        self.addCleanup(client.close)
        # File readback has its own native scan/Arrow admission. Python's
        # batch_rows slices the admitted producer batch; it does not shrink
        # that file-reader allocation or alter the constrained set grant.
        verification_client = sl.ShardLoomClient(
            transport="native", memory_bytes=128 << 20, max_parallelism=1)
        self.addCleanup(verification_client.close)

        def row(key):
            return {"n": key, "s": f"{key:08d}" + "p" * (width - 8)}

        def source(side):
            events = []
            def produce():
                events.append("opened")
                try:
                    for start in range(0, count, 251):
                        yield [row(count - 1 - position if side == 0 else offset + position)
                               for position in range(start, min(start + 251, count))]
                    events.append("ended")
                finally:
                    events.append("closed")
            return sl.from_batches(produce, schema={"n": "int64", "s": "utf8"},
                                   streaming=True, client=client), events

        held = None
        for kind in ("union", "intersect", "except_"):
            keys = [key for key in range(count - 1, -1, -1)
                    if kind == "union" or (key >= offset) == (kind == "intersect")]
            if kind == "union":
                keys.extend(range(count, count + offset))
            for surface in ("dataframe", "sql"):
                for terminal in ("batches", "fanout"):
                    with self.subTest(kind=kind, surface=surface, terminal=terminal), \
                         tempfile.TemporaryDirectory(prefix="shardloom-native-set-pressure-") as directory, \
                         patch("subprocess.Popen", side_effect=AssertionError("child process")), \
                         patch("shardloom._batches.BatchInput.encode", side_effect=AssertionError("JSON input")):
                        held = None
                        root = Path(directory)
                        workspace = root / "runs"
                        workspace.mkdir()
                        foreign = workspace / "foreign"
                        foreign.write_bytes(b"preserve")
                        spill = {"workspace": str(workspace), "quota_bytes": 128 << 20,
                                 "buffer_bytes": 1 << 20}
                        left, a = source(0)
                        right, b = source(1)
                        query = combine(left, right, kind, surface)

                        def checked_stream(frame, **options):
                            nonlocal held
                            seen = 0
                            with frame.iter_batches(batch_rows=127, **options) as stream:
                                for batch in stream:
                                    self.assertIsNone(stream.report)
                                    table = batch.to_arrow()
                                    self.assertEqual(table.schema.names, ["n", "s"])
                                    if seen == 0:
                                        held = table
                                    for value in table.to_pylist():
                                        self.assertLess(seen, len(keys))
                                        self.assertEqual(value, row(keys[seen]))
                                        seen += 1
                                report = stream.report
                            self.assertEqual(seen, len(keys))
                            return report

                        if terminal == "batches":
                            report = checked_stream(query, spill=spill)
                        else:
                            targets = {name: root / ("result." + name)
                                       for name in ("vortex", "parquet", "jsonl")}
                            report = query.fanout(targets, spill=spill)
                            self.assertEqual(report.envelope.field_int("native_vortex_result_export_execution_count"), 1)
                            self.assertEqual(report.envelope.field_int("native_vortex_result_export_replay_count"), 3)
                            for name in ("vortex", "parquet"):
                                checked_stream(getattr(sl, "read_" + name)(
                                    targets[name], client=verification_client))
                            seen = 0
                            with targets["jsonl"].open() as stream:
                                for line in stream:
                                    self.assertLess(seen, len(keys))
                                    self.assertEqual(json.loads(line), row(keys[seen]))
                                    seen += 1
                            self.assertEqual(seen, len(keys))
                        self.completed(report, [a, b])
                        envelope = report.envelope
                        self.assertEqual(envelope.field_int("relational_ordered_set_stages"), 1)
                        self.assertEqual(envelope.field_int("relational_ordered_set_input_rows"), 2 * count)
                        self.assertEqual(envelope.field_int("relational_ordered_set_groups"), count + offset)
                        self.assertEqual(envelope.field_int("relational_ordered_set_output_rows"), len(keys))
                        self.assertGreater(envelope.field_int("native_input_logical_bytes"), 2 * grant)
                        self.assertGreater(envelope.field_int("relational_spill_runs_written"), 3)
                        self.assertTrue(envelope.field_bool("relational_spill_owned_cleanup_completed"))
                        self.assertEqual(list(workspace.iterdir()), [foreign])
                        self.assertEqual(foreign.read_bytes(), b"preserve")
                        del query, left, right, report
                        self.assertEqual(held.to_pylist(), [row(key) for key in keys[:held.num_rows]])
        client.close()
        verification_client.close()
        self.assertEqual(held.to_pylist(), [row(key) for key in keys[:held.num_rows]])

    def test_native_set_rich_membership_spill_preserves_complete_nested_and_typed_rows(self):
        import pyarrow as pa
        import shardloom as sl

        count, offset, grant = 4097, 2048, 16 << 20
        schema = {"small": "int16", "blob": "binary", "amount": "decimal128(12,2)",
                  "items": {"type": "list", "item": "int16"},
                  "record": {"type": "struct", "fields": {"label": "utf8", "value": "int32"}}}
        arrow_schema = pa.schema([
            ("small", pa.int16()), ("blob", pa.binary()), ("amount", pa.decimal128(12, 2)),
            ("items", pa.list_(pa.int16())),
            ("record", pa.struct([("label", pa.string()), ("value", pa.int32())])),
        ])
        def row(key):
            return {"small": key - 32768, "blob": key.to_bytes(4, "big") + b"\x00\xff" * 2046,
                    "amount": None if key % 7 == 0 else Decimal(key - 5000).scaleb(-2),
                    "items": None if key % 5 == 0 else [key, None, -key],
                    "record": None if key % 3 == 0 else {"label": "港", "value": key}}
        client = sl.ShardLoomClient(transport="native", memory_bytes=grant, max_parallelism=1)
        self.addCleanup(client.close)
        retained = None
        for kind in ("union", "intersect", "except_"):
            with self.subTest(kind=kind), \
                 tempfile.TemporaryDirectory(prefix="shardloom-native-set-rich-pressure-") as directory, \
                 patch("subprocess.Popen", side_effect=AssertionError("child process")), \
                 patch("shardloom._batches.BatchInput.encode", side_effect=AssertionError("JSON input")):
                retained = None
                events = [[], []]
                def source(side):
                    def produce():
                        events[side].append("opened")
                        try:
                            for start in range(0, count, 127):
                                rows = [row(count - 1 - value if side == 0 else offset + value)
                                        for value in range(start, min(start + 127, count))]
                                yield pa.Table.from_pylist(rows, schema=arrow_schema)
                            events[side].append("ended")
                        finally:
                            events[side].append("closed")
                    return sl.from_batches(produce, schema=schema, streaming=True, client=client)
                keys = [key for key in range(count - 1, -1, -1)
                        if kind == "union" or (key >= offset) == (kind == "intersect")]
                if kind == "union":
                    keys.extend(range(count, count + offset))
                seen = 0
                query = combine(source(0), source(1), kind)
                spill = {"workspace": directory, "quota_bytes": 128 << 20, "buffer_bytes": 1 << 20}
                with query.iter_batches(batch_rows=31, spill=spill) as stream:
                    for batch in stream:
                        self.assertIsNone(stream.report)
                        table = batch.to_arrow()
                        self.assertTrue(table.schema.equals(arrow_schema, check_metadata=True))
                        wanted = pa.Table.from_pylist([row(key) for key in keys[seen:seen + table.num_rows]],
                                                     schema=arrow_schema)
                        self.assertTrue(table.equals(wanted, check_metadata=True))
                        if seen == 0:
                            retained = (table, wanted)
                        seen += table.num_rows
                    report = stream.report
                self.assertEqual(seen, len(keys))
                self.completed(report, events)
                self.assertGreater(report.envelope.field_int("native_input_logical_bytes"), 2 * grant)
                self.assertGreater(report.envelope.field_int("relational_spill_runs_written"), 3)
                self.assertEqual(report.envelope.field_int("relational_ordered_set_groups"), count + offset)
                self.assertEqual(list(Path(directory).iterdir()), [])
                del query, report
                self.assertTrue(retained[0].equals(retained[1], check_metadata=True))
        client.close()
        self.assertTrue(retained[0].equals(retained[1], check_metadata=True))
