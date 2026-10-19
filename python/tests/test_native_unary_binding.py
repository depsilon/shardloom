# SPDX-License-Identifier: Apache-2.0
"""Finite unary operations through the installed in-process Python binding."""

from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

if __package__:
    from .test_native_binding import _load_native
else:
    from test_native_binding import _load_native


ROWS = [dict(n=n, s=s, items=items) for n, s, items in (
    (3, "α", [7, None, 8]), (None, None, None), (3, "α", [7, None, 8]),
    (1, "β", []), (2, "γ", [9]), (None, "δ", [10]), (1, "ε", [11, 12]), (4, "", [13]))]
KINDS = ("distinct", "dedup-first", "dedup-last", "dedup-false",
         "mask-first", "mask-last", "mask-false", "tail", "sample",
         "expression", "melt", "rolling", "explode")
SCHEMA = {"n": "int16", "s": "utf8", "items": {"type": "list", "item": "int16"}}


def apply(source, kind):
    if kind == "distinct":
        return source.distinct()
    if kind.startswith(("dedup-", "mask-")):
        keep = kind.split("-", 1)[1]
        method = source.drop_duplicates if kind.startswith("dedup-") else source.duplicated
        return method("n", keep=False if keep == "false" else keep)
    if kind == "tail":
        return source.tail(3)
    if kind == "sample":
        return source.sample(3, random_state=7)
    if kind == "expression":
        return source.select("n", "s").fillna(method="ffill").reset_index()
    if kind == "melt":
        return source.melt(id_vars="s", value_vars="n")
    if kind == "rolling":
        return source.rolling(3, min_periods=1, center=True).sum("n", alias="total")
    assert kind == "explode"
    return source.explode("items")


def expected(kind, rows):
    if kind == "distinct":
        return [row for index, row in enumerate(rows) if row not in rows[:index]]
    if kind.startswith(("dedup-", "mask-")):
        flags, keep = [], kind.split("-", 1)[1]
        for index, row in enumerate(rows):
            prior = any(value["n"] == row["n"] for value in rows[:index])
            later = any(value["n"] == row["n"] for value in rows[index + 1:])
            flags.append(prior if keep == "first" else later if keep == "last" else prior or later)
        return ([{"duplicated": flag} for flag in flags] if kind.startswith("mask-") else
                [row for row, flag in zip(rows, flags, strict=True) if not flag])
    if kind == "tail":
        return rows[-3:]
    if kind == "sample":
        # Independently predeclared seed-7 selection over this eight-row population.
        return [rows[index] for index in (1, 2, 4)] if len(rows) == 8 else list(rows)
    if kind == "expression":
        prior, result = {"n": None, "s": None}, []
        for index, row in enumerate(rows):
            for name in prior:
                if row[name] is not None:
                    prior[name] = row[name]
            result.append(dict(prior, index=index))
        return result
    if kind == "melt":
        return [{"s": row["s"], "variable": "n", "value": row["n"]} for row in rows]
    if kind == "rolling":
        result = []
        for index in range(len(rows)):
            values = [row["n"] for row in rows[max(0, index - 1):index + 2] if row["n"] is not None]
            if values:
                result.append({"total": float(sum(values))})
        return result
    return [dict(row, items=value) for row in rows
            for value in ([None] if row["items"] is None else row["items"])]


def arrow_schema(kind=None):
    import pyarrow as pa

    fields = [pa.field("n", pa.int16()), pa.field("s", pa.string()), pa.field("items", pa.list_(pa.int16()))]
    if kind and kind.startswith("mask-"):
        fields = [pa.field("duplicated", pa.bool_(), nullable=False)]
    elif kind == "expression":
        fields = fields[:2] + [pa.field("index", pa.uint64(), nullable=False)]
    elif kind == "melt":
        fields = [fields[1], pa.field("variable", pa.string(), nullable=False), pa.field("value", pa.int16())]
    elif kind == "rolling":
        fields = [pa.field("total", pa.float64(), nullable=False)]
    elif kind == "explode":
        fields[-1] = pa.field("items", pa.int16())
    return pa.schema(fields)


class NativeUnaryBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()

    def client(self):
        import shardloom as sl

        client = sl.ShardLoomClient(transport="native", memory_bytes=32 << 20, max_parallelism=1)
        self.addCleanup(client.close)
        return client

    @staticmethod
    def source(client, rows, columnar):
        import pyarrow as pa
        import shardloom as sl

        events = []

        def produce():
            events.append("opened")
            try:
                for batch in ([], rows[:2], [], rows[2:5], rows[5:], []):
                    yield pa.Table.from_pylist(batch, schema=arrow_schema()) if columnar else batch
                events.append("ended")
            finally:
                events.append("closed")

        return sl.from_batches(produce, schema=SCHEMA, streaming=True, client=client), events

    def completed(self, report, events, *, input_batches, input_rows, repeated=False):
        self.assertEqual(events, ["opened", "ended", "closed"])
        envelope = report.envelope
        self.assertEqual(envelope.field_int("native_input_batch_sources"), 1)
        self.assertEqual(envelope.field_int("native_input_batches"), input_batches)
        self.assertEqual(envelope.field_int("native_input_batch_rows"), input_rows)
        self.assertTrue(envelope.field_bool("native_input_end_observed"))
        self.assertTrue(envelope.field_bool("native_input_output_ownership_detached"))
        self.assertEqual(envelope.field_int("native_input_replay_sources"), int(repeated))
        self.assertEqual(envelope.field_int("native_input_replay_passes"), int(repeated))
        self.assertFalse(envelope.fallback.attempted)
        self.assertFalse(envelope.fallback.allowed)
        self.assertEqual(envelope.field("external_engine_invoked"), "false")

    def test_native_unary_families_columnar_and_scalar_input_across_all_terminals(self):
        import pyarrow as pa
        import shardloom as sl
        from shardloom.query import SqlWorkflow

        client, retained = self.client(), []
        for kind in KINDS:
            for columnar in (False, True):
                for shape in ("mixed", "empty", "nulls"):
                    rows = ROWS if shape == "mixed" else [] if shape == "empty" else [dict.fromkeys(SCHEMA)] * 3
                    wanted = pa.Table.from_pylist(expected(kind, rows), schema=arrow_schema(kind))
                    for terminal in ("collect", "batches", "vortex", "fanout"):
                        with self.subTest(kind=kind, columnar=columnar, shape=shape, terminal=terminal), \
                             tempfile.TemporaryDirectory(prefix="shardloom-native-unary-") as directory, \
                             patch("subprocess.Popen", side_effect=AssertionError("child process")), \
                             patch("shardloom._batches.BatchInput.encode", side_effect=AssertionError("JSON input")):
                            source, events = self.source(client, rows, columnar)
                            query = apply(source, kind)
                            if columnar:
                                query = SqlWorkflow(query._relation_statement(), client,
                                                    source_bindings=query._declared_sources())
                            self.assertEqual(events, [])
                            if terminal == "collect":
                                report = query.collect(check=True)
                                tables = [report.to_arrow()]
                            elif terminal == "batches":
                                tables = []
                                with query.iter_batches(batch_rows=1) as stream:
                                    for batch in stream:
                                        self.assertIsNone(stream.report)
                                        tables.append(batch.to_arrow())
                                    report = stream.report
                            else:
                                extensions = ("vortex", "parquet", "jsonl") if terminal == "fanout" else ("vortex",)
                                targets = {ext: Path(directory) / ("result." + ext) for ext in extensions}
                                report = query.fanout(targets) if terminal == "fanout" else query.write_vortex(targets["vortex"])
                                tables = []
                                for ext, path in targets.items():
                                    if ext == "jsonl":
                                        self.assertEqual([json.loads(line) for line in path.read_text().splitlines()], expected(kind, rows))
                                    else:
                                        table = getattr(sl, "read_" + ext)(path, client=client).collect(check=True).to_arrow()
                                        self.assertEqual(table.to_pylist(), wanted.to_pylist())
                                        if ext == "vortex":
                                            tables.append(table)
                                if terminal == "fanout":
                                    self.assertEqual(report.envelope.field_int("native_vortex_result_export_execution_count"), 1)
                                    self.assertEqual(report.envelope.field_int("native_vortex_result_export_replay_count"), 3)
                            self.assertTrue(tables, "empty native output must deliver its bound schema")
                            actual = pa.concat_tables(tables)
                            self.assertTrue(actual.equals(wanted, check_metadata=True), (actual, wanted))
                            # Empty Arrow tables expose no record batches; each
                            # empty row sequence still produces one native batch.
                            input_batches = {"mixed": 3, "empty": 0, "nulls": 2}[shape] if columnar else 6
                            self.completed(report, events, input_batches=input_batches, input_rows=len(rows))
                            retained.append((actual, wanted))
        client.close()
        for actual, wanted in retained:
            self.assertTrue(actual.equals(wanted, check_metadata=True))

    def test_native_unary_repeated_branch_uses_one_producer_and_explicit_replay(self):
        client = self.client()
        for kind in KINDS:
            for terminal in ("collect", "batches"):
                with self.subTest(kind=kind, terminal=terminal), \
                     patch("subprocess.Popen", side_effect=AssertionError("child process")), \
                     patch("shardloom._batches.BatchInput.encode", side_effect=AssertionError("JSON input")):
                    source, events = self.source(client, ROWS, True)
                    stage = apply(source, kind)
                    query = stage.union_all(stage)
                    if terminal == "collect":
                        report = query.collect(check=True)
                        actual = report.to_arrow().to_pylist()
                    else:
                        with query.iter_batches(batch_rows=1) as stream:
                            actual = [row for batch in stream for row in batch.to_arrow().to_pylist()]
                            report = stream.report
                    self.assertEqual(actual, expected(kind, ROWS) * 2)
                    self.completed(report, events, input_batches=3, input_rows=len(ROWS), repeated=True)

    def test_native_unary_late_failure_zero_limit_cancellation_and_clean_reuse(self):
        import pyarrow as pa
        import shardloom as sl
        from shardloom.errors import ShardLoomCommandError

        client = self.client()
        for kind in KINDS:
            for failure in ("producer", "schema", "cancel"):
                for terminal in ("collect", "zero", "fanout"):
                    with self.subTest(kind=kind, failure=failure, terminal=terminal), \
                         tempfile.TemporaryDirectory(prefix="shardloom-native-unary-fault-") as directory, \
                         patch("subprocess.Popen", side_effect=AssertionError("child process")), \
                         patch("shardloom._batches.BatchInput.encode", side_effect=AssertionError("JSON input")):
                        events, sentinel = [], LookupError("unary producer sentinel")

                        def broken():
                            events.append("opened")
                            try:
                                yield pa.Table.from_pylist(ROWS[:2], schema=arrow_schema())
                                events.append("fault")
                                if failure == "producer":
                                    raise sentinel
                                if failure == "schema":
                                    yield pa.table({"changed": pa.array([1], pa.int64())})
                                else:
                                    client.cancel()
                            finally:
                                events.append("closed")

                        source = sl.from_batches(broken, schema=SCHEMA, streaming=True, client=client)
                        query = apply(source, kind)
                        root = Path(directory)
                        foreign = root / "foreign"
                        foreign.write_bytes(b"preserve")
                        expected_error = {"producer": LookupError, "schema": ValueError,
                                          "cancel": ShardLoomCommandError}[failure]
                        with self.assertRaises(expected_error) as raised:
                            if terminal == "collect":
                                query.collect(check=True)
                            elif terminal == "zero":
                                with query.limit(0).iter_batches(batch_rows=1) as stream:
                                    list(stream)
                            else:
                                query.fanout({"vortex": root / "result.vortex", "jsonl": root / "result.jsonl"})
                        if failure == "producer":
                            self.assertIs(raised.exception, sentinel)
                        self.assertEqual(events, ["opened", "fault", "closed"])
                        self.assertEqual(list(root.iterdir()), [foreign])
                        self.assertEqual(foreign.read_bytes(), b"preserve")
                        self.assertEqual(sl.from_rows([{"n": 7}], schema={"n": "int64"}, client=client)
                                         .collect(check=True).to_arrow().to_pylist(), [{"n": 7}])

    def test_native_unary_early_close_retains_output_without_completion_report(self):
        client, held = self.client(), []
        for kind in KINDS:
            with self.subTest(kind=kind), \
                 patch("subprocess.Popen", side_effect=AssertionError("child process")), \
                 patch("shardloom._batches.BatchInput.encode", side_effect=AssertionError("JSON input")):
                source, events = self.source(client, ROWS, True)
                query = apply(source, kind)
                with query.iter_batches(batch_rows=1) as stream:
                    batch = next(stream)
                    self.assertIsNone(stream.report)
                self.assertIsNone(stream.report)
                self.assertEqual(events[0], "opened")
                self.assertEqual(events[-1], "closed")
                table = batch.to_arrow()
                self.assertEqual(table.to_pylist(), expected(kind, ROWS)[:1])
                held.append((table, expected(kind, ROWS)[:1]))
        client.close()
        for table, wanted in held:
            self.assertEqual(table.to_pylist(), wanted)

    def test_native_unary_membership_spill_preserves_exact_rows_across_terminals(self):
        import pyarrow as pa
        import shardloom as sl

        keys = (list(range(257)) + list(reversed(range(0, 257, 2))) +
                list(range(257, 321)))
        rows = [
            {"n": ordinal, "s": f"{key:08d}" + "u" * (8192 - 8),
             "items": [ordinal, None]}
            for ordinal, key in enumerate(keys)
        ]
        kinds = ("distinct", "dedup-first", "dedup-last", "dedup-false",
                 "mask-first", "mask-last", "mask-false")
        input_batches = (len(rows) + 40) // 41

        def expected(kind):
            if kind == "distinct":
                seen, result = set(), []
                for value in rows:
                    if value["s"] not in seen:
                        seen.add(value["s"])
                        result.append({"s": value["s"]})
                return result
            if kind.startswith(("dedup-", "mask-")):
                keep = kind.split("-", 1)[1]
                flags = []
                for index, value in enumerate(rows):
                    prior = any(other["s"] == value["s"] for other in rows[:index])
                    later = any(other["s"] == value["s"] for other in rows[index + 1:])
                    flags.append(prior if keep == "first" else
                                 later if keep == "last" else prior or later)
                if kind.startswith("mask-"):
                    return [{"duplicated": flag} for flag in flags]
                return [value for value, duplicate in zip(rows, flags, strict=True) if not duplicate]
            raise AssertionError(kind)

        def result_schema(kind):
            if kind == "distinct":
                return pa.schema([pa.field("s", pa.string())])
            if kind.startswith("mask-"):
                return pa.schema([pa.field("duplicated", pa.bool_(), nullable=False)])
            return arrow_schema()

        for kind in kinds:
            wanted_rows = expected(kind)
            wanted = pa.Table.from_pylist(wanted_rows, schema=result_schema(kind))
            for columnar in (False, True):
                for terminal in ("collect", "batches", "vortex", "fanout"):
                    with self.subTest(kind=kind, columnar=columnar, terminal=terminal), \
                         tempfile.TemporaryDirectory(prefix="shardloom-native-unary-membership-") as directory, \
                         patch("subprocess.Popen", side_effect=AssertionError("child process")), \
                         patch("shardloom._batches.BatchInput.encode", side_effect=AssertionError("JSON input")):
                        client, retained = self.client(), None
                        verification_client = None
                        root = Path(directory)
                        workspace = root / "spill"
                        workspace.mkdir()
                        foreign = workspace / "foreign"
                        foreign.write_bytes(b"preserve")
                        spill = {"workspace": str(workspace), "quota_bytes": 128 << 20,
                                 "buffer_bytes": 1 << 20}
                        events = []

                        def produce():
                            events.append("opened")
                            try:
                                for start in range(0, len(rows), 41):
                                    batch = rows[start:start + 41]
                                    yield pa.Table.from_pylist(batch, schema=arrow_schema()) if columnar else batch
                                events.append("ended")
                            finally:
                                events.append("closed")

                        source = sl.from_batches(produce, schema=SCHEMA, streaming=True, client=client)
                        if kind == "distinct":
                            query = source.select("s").distinct()
                        else:
                            keep = kind.split("-", 1)[1]
                            method = source.drop_duplicates if kind.startswith("dedup-") else source.duplicated
                            query = method("s", keep=False if keep == "false" else keep)

                        if terminal == "collect":
                            result = query.collect(check=True, spill=spill)
                            report = result
                            retained = result.to_arrow()
                        elif terminal == "batches":
                            tables = []
                            with query.iter_batches(batch_rows=17, spill=spill) as stream:
                                for batch in stream:
                                    self.assertIsNone(stream.report)
                                    table = batch.to_arrow()
                                    self.assertTrue(table.schema.equals(wanted.schema, check_metadata=True))
                                    tables.append(table)
                                report = stream.report
                            retained = pa.concat_tables(tables)
                        else:
                            extensions = ("vortex",) if terminal == "vortex" else ("vortex", "parquet", "jsonl")
                            targets = {extension: root / ("result." + extension) for extension in extensions}
                            report = (query.fanout(targets, spill=spill) if terminal == "fanout" else
                                      query.write_vortex(targets["vortex"], spill=spill))
                            if terminal == "fanout":
                                self.assertEqual(report.envelope.field_int(
                                    "native_vortex_result_export_execution_count"), 1)
                                self.assertEqual(report.envelope.field_int(
                                    "native_vortex_result_export_target_count"), 3)
                                self.assertEqual(report.envelope.field_int(
                                    "native_vortex_result_export_replay_count"), 3)
                            # Persisted-file scan and Arrow conversion have their
                            # own admission, independent of the 32 MiB execution
                            # and publication grant asserted below.
                            verification_client = sl.ShardLoomClient(
                                transport="native", memory_bytes=128 << 20, max_parallelism=1)
                            self.addCleanup(verification_client.close)
                            for extension, path in targets.items():
                                if extension == "jsonl":
                                    self.assertEqual(
                                        [json.loads(line) for line in path.read_text().splitlines()],
                                        wanted_rows,
                                    )
                                else:
                                    reopened = getattr(sl, "read_" + extension)(
                                        path, client=verification_client)
                                    table = reopened.collect(check=True).to_arrow()
                                    self.assertTrue(table.schema.equals(wanted.schema, check_metadata=True))
                                    self.assertTrue(table.equals(wanted, check_metadata=True))
                                    if extension == "vortex":
                                        retained = table

                        self.assertIsNotNone(retained)
                        self.assertTrue(retained.schema.equals(wanted.schema, check_metadata=True))
                        self.assertTrue(retained.equals(wanted, check_metadata=True), (kind, terminal))
                        self.completed(report, events, input_batches=input_batches, input_rows=len(rows))
                        envelope = report.envelope
                        self.assertEqual(envelope.field_int(
                            "relational_spillable_unary_membership_stages"), 1)
                        self.assertEqual(envelope.field_int(
                            "relational_spillable_unary_input_rows"), len(rows))
                        self.assertEqual(envelope.field_int(
                            "relational_spillable_unary_observed_rows"), len(rows))
                        self.assertEqual(envelope.field_int(
                            "relational_spillable_unary_groups"), 321)
                        self.assertEqual(envelope.field_int(
                            "relational_spillable_unary_output_rows"), len(wanted_rows))
                        self.assertGreater(envelope.field_int("relational_spill_runs_written"), 0)
                        self.assertTrue(envelope.field_bool("relational_spill_owned_cleanup_completed"))
                        self.assertLessEqual(envelope.field_int("relational_spill_peak_disk_bytes"),
                                             spill["quota_bytes"])
                        self.assertLessEqual(envelope.field_int("resident_peak_reserved_buffer_bytes"),
                                             32 << 20)
                        self.assertEqual(envelope.field_int("native_input_replay_sources"), 0)
                        self.assertEqual(envelope.field("local_primitive_native_io_certificate_status"),
                                         "certified")
                        self.assertEqual(list(workspace.iterdir()), [foreign])
                        self.assertEqual(foreign.read_bytes(), b"preserve")
                        client.close()
                        if verification_client is not None:
                            verification_client.close()
                        self.assertTrue(retained.equals(wanted, check_metadata=True))
