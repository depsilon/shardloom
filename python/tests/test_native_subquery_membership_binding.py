# SPDX-License-Identifier: Apache-2.0
"""Native predicate spill with row/Arrow producers and retained results."""

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


ROWS, WIDTH, GRANT = 1025, 2048, 8 << 20
SCHEMA = {"n": "int64", "s": "utf8"}
ANSWERS = {
    "in": [True, None, True, None, True],
    "row-in": [True, None, None, None, None],
    "exists": [True, False, True, False, True],
    "any": [True, None, False, None, None],
    "all": [False, None, True, None, None],
    "hot-in": [True, False, False, False, False],
}


def text(index):
    return f"{index:08d}" + "p" * WIDTH


def fixture(kind):
    outer = [dict(n=n, s=s) for n, s in (
        (1, text(0)), (4, text(ROWS)), (None, text(1)),
        (2, None), (0, text(ROWS // 2)))]
    inner = [dict(n=None if index % 13 == 0 else 1 if kind == "hot-in" else index % 5,
                  s=None if index % 17 == 0 else text(index // 2)) for index in range(ROWS)]
    return outer, inner


def apply(left, right, kind):
    import shardloom as sl

    correlation = sl.col("n") == sl.outer("n")
    if kind in ("in", "hot-in"):
        answer = sl.col("s").isin_source(right, "s", where=correlation if kind == "hot-in" else None)
    elif kind == "row-in":
        answer = sl.row_in_source(["n", "s"], right, ["n", "s"])
    elif kind == "exists":
        answer = sl.exists_source(right, where=sl.col("s") == sl.outer("s"))
    else:
        answer = getattr(sl.col("s"), kind + "_source")(
            "<" if kind == "any" else ">=", right, "s", where=correlation)
    return left.with_column("answer", answer)


class NativeSubqueryMembershipBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()

    def client(self, memory_bytes=GRANT):
        import shardloom as sl

        client = sl.ShardLoomClient(transport="native", memory_bytes=memory_bytes,
                                    max_parallelism=1)
        self.addCleanup(client.close)
        return client

    def test_spilled_predicates_rows_arrow_collect_batches_and_publication(self):
        import pyarrow as pa
        import shardloom as sl

        schema = pa.schema([pa.field("n", pa.int64()), pa.field("s", pa.string())])
        for kind, answers in ANSWERS.items():
            outer, inner = fixture(kind)
            wanted_rows = [dict(row, answer=answer) for row, answer in zip(outer, answers, strict=True)]
            wanted = pa.Table.from_pylist(wanted_rows, schema=schema.append(
                pa.field("answer", pa.bool_(), nullable=kind != "exists")))
            for columnar in (False, True):
                for terminal in ("collect", "batches", "vortex", "fanout"):
                    with self.subTest(kind=kind, columnar=columnar, terminal=terminal), \
                         tempfile.TemporaryDirectory(prefix="shardloom-native-predicate-") as directory, \
                         patch("subprocess.Popen", side_effect=AssertionError("child process")), \
                         patch("shardloom._batches.BatchInput.encode", side_effect=AssertionError("JSON input")):
                        root, client = Path(directory), self.client()
                        workspace = root / "spill"
                        workspace.mkdir()
                        foreign = workspace / "foreign"
                        foreign.write_bytes(b"preserve")
                        spill = dict(workspace=str(workspace), quota_bytes=128 << 20, buffer_bytes=1 << 20)
                        traces = []

                        def source(rows, width):
                            trace = dict(opened=0, ended=False, closed=False, batches=0, rows=0)
                            traces.append(trace)

                            def produce():
                                trace["opened"] += 1
                                try:
                                    for start in range(0, len(rows), width):
                                        batch = rows[start:start + width]
                                        trace["batches"] += 1
                                        trace["rows"] += len(batch)
                                        yield pa.Table.from_pylist(batch, schema=schema) if columnar else batch
                                    trace["ended"] = True
                                finally:
                                    trace["closed"] = True

                            return sl.from_batches(produce, schema=SCHEMA, streaming=True, client=client)

                        query = apply(source(outer, 2), source(inner, 31), kind)
                        self.assertTrue(all(trace["opened"] == 0 for trace in traces))
                        retained, verification_client = None, None
                        if terminal == "collect":
                            report = query.collect(check=True, spill=spill)
                            retained = report.to_arrow()
                        elif terminal == "batches":
                            tables = []
                            with query.iter_batches(batch_rows=2, spill=spill) as stream:
                                for batch in stream:
                                    self.assertIsNone(stream.report)
                                    tables.append(batch.to_arrow())
                                report = stream.report
                            self.assertIsNotNone(report)
                            retained = pa.concat_tables(tables)
                        else:
                            formats = ("vortex", "parquet", "jsonl") if terminal == "fanout" else ("vortex",)
                            targets = {extension: root / ("result." + extension) for extension in formats}
                            report = (query.fanout(targets, spill=spill) if terminal == "fanout" else
                                      query.write_vortex(targets["vortex"], spill=spill))
                            if terminal == "fanout":
                                self.assertEqual(report.envelope.field_int(
                                    "native_vortex_result_export_execution_count"), 1)
                                self.assertEqual(report.envelope.field_int(
                                    "native_vortex_result_export_replay_count"), 3)
                            verification_client = self.client(128 << 20)
                            for extension, path in targets.items():
                                if extension == "jsonl":
                                    self.assertEqual([json.loads(line) for line in path.read_text().splitlines()], wanted_rows)
                                else:
                                    table = getattr(sl, "read_" + extension)(
                                        path, client=verification_client).collect(check=True).to_arrow()
                                    self.assertTrue(table.equals(wanted, check_metadata=True))
                                    retained = table
                        self.assertIsNotNone(retained)
                        self.assertTrue(retained.schema.equals(wanted.schema, check_metadata=True))
                        self.assertTrue(retained.equals(wanted, check_metadata=True))
                        for trace in traces:
                            self.assertEqual(trace["opened"], 1)
                            self.assertTrue(trace["ended"] and trace["closed"])
                        envelope = report.envelope
                        self.assertEqual(envelope.field_int("native_input_batch_sources"), 2)
                        self.assertEqual(envelope.field_int("native_input_batches"), sum(t["batches"] for t in traces))
                        self.assertEqual(envelope.field_int("native_input_batch_rows"), ROWS + 5)
                        self.assertTrue(envelope.field_bool("native_input_end_observed"))
                        self.assertTrue(envelope.field_bool("native_input_output_ownership_detached"))
                        parameterized = kind not in ("in", "row-in")
                        population = {"in": ROWS, "row-in": ROWS, "exists": 4,
                                      "any": 757, "all": 757, "hot-in": 946}[kind]
                        self.assertEqual(envelope.field_int("native_input_replay_sources"), int(parameterized))
                        self.assertEqual(envelope.field_int("native_input_replay_spilled_sources"), int(parameterized))
                        self.assertEqual(envelope.field_int("relational_spillable_subquery_stages"), 5 if parameterized else 1)
                        self.assertEqual(envelope.field_int("relational_spillable_subquery_input_rows"), population)
                        self.assertEqual(envelope.field_int("relational_spillable_subquery_indexed_rows"), population)
                        self.assertEqual(envelope.field_int("relational_spilled_subquery_stages") > 0, kind != "exists")
                        self.assertEqual(envelope.field_int("relational_spillable_subquery_probe_rows"), 5)
                        self.assertGreater(envelope.field_int("relational_spillable_subquery_lookup_blocks"), 0)
                        self.assertGreater(envelope.field_int("relational_spill_runs_written"), 2)
                        self.assertGreater(envelope.field_int("relational_spill_merge_passes"), 0)
                        self.assertTrue(envelope.field_bool("relational_spill_owned_cleanup_completed"))
                        self.assertLessEqual(envelope.field_int("resident_peak_reserved_buffer_bytes"), GRANT)
                        self.assertFalse(envelope.fallback.attempted)
                        self.assertFalse(envelope.fallback.allowed)
                        self.assertEqual(envelope.field("external_engine_invoked"), "false")
                        self.assertEqual(envelope.field("local_primitive_native_io_certificate_status"), "certified")
                        self.assertEqual(list(workspace.iterdir()), [foreign])
                        self.assertEqual(foreign.read_bytes(), b"preserve")
                        client.close()
                        if verification_client is not None:
                            verification_client.close()
                        self.assertTrue(retained.equals(wanted, check_metadata=True))
