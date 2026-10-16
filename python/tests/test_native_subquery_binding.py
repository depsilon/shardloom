# SPDX-License-Identifier: Apache-2.0
"""Finite subqueries through direct native plans and the public in-process client."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

if __package__:
    from .test_native_binding import _load_native
else:
    from test_native_binding import _load_native


LEFT = [dict(n=n, s=s) for n, s in ((1, "a"), (2, "b"), (3, "c"), (None, "z"))]
RIGHT = [dict(n=n, s=s) for n, s in ((2, "b"), (None, "q"), (2, "b"))]
SCHEMA = {"n": "int64", "s": "utf8"}
URIS = ("memory://outer", "memory://inner")
CASES = (
    ("in", "n IN (SELECT n FROM 'memory://inner')", [None, True, None, None]),
    ("row-in", "(n,s) IN (SELECT n,s FROM 'memory://inner')", [False, True, False, False]),
    ("exists", "EXISTS (SELECT * FROM 'memory://inner')", [True] * 4),
    ("any", "n > ANY (SELECT n FROM 'memory://inner')", [None, None, True, None]),
    ("all", "n < ALL (SELECT n FROM 'memory://inner')", [None, False, False, None]),
    ("scalar", "(SELECT n FROM 'memory://inner' LIMIT 1)", [2] * 4),
    ("parameterized", "(SELECT MAX(n) FROM 'memory://inner' WHERE n <= outer.n)", [None, 2, 2, None]),
    ("guarded", "CASE WHEN n < 0 THEN (SELECT 1/0 AS bad FROM 'memory://inner') ELSE 7 END", [7] * 4),
    ("shared", "(SELECT n FROM 'memory://outer' LIMIT 1)", [1] * 4),
)


def declaration(uri, rows, columnar, schema=SCHEMA):
    import pyarrow as pa
    from shardloom._input_schema import normalize_schema
    from shardloom._native_backend import _native_fields
    from shardloom._result_schema import _arrow_type

    fields = normalize_schema(schema)
    arrow = pa.schema([pa.field(name, _arrow_type(dtype, pa), nullable=dtype.nullable) for name, dtype in fields])
    trace = dict(opened=0, ended=0, closed=0, batches=0, rows=0)

    def produce():
        trace["opened"] += 1
        try:
            for batch in ([], rows[:2], [], rows[2:], []):
                trace["batches"] += int(bool(batch)) if columnar else 1
                trace["rows"] += len(batch)
                yield pa.Table.from_pylist(batch, schema=arrow) if columnar else batch
            trace["ended"] += 1
        finally:
            trace["closed"] += 1

    return (uri, produce, _native_fields(fields), True), trace


class NativeSubqueryBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()

    def session(self):
        session = self.native.Session(self.native.ExecutionResources(32 << 20, 1))
        self.addCleanup(session.close)
        return session

    def complete(self, report, traces, execution=1):
        for trace in traces:
            self.assertEqual((trace["opened"], trace["ended"], trace["closed"]), (execution,) * 3)
        self.assertEqual(int(report["native_input_batch_sources"]), len(traces))
        self.assertEqual(int(report["native_input_batches"]), sum(t["batches"] for t in traces) // execution)
        self.assertEqual(int(report["native_input_batch_rows"]), sum(t["rows"] for t in traces) // execution)
        self.assertEqual(report["native_input_end_observed"], "true")
        self.assertEqual(report["native_input_output_ownership_detached"], "true")
        self.assertEqual(report["fallback_attempted"], "false")
        self.assertEqual(report["external_engine_invoked"], "false")

    def test_direct_subquery_plans_rows_arrow_reuse_and_retained_results(self):
        import pyarrow as pa

        session, held = self.session(), []
        for kind, expression, answers in CASES:
            for columnar in (False, True):
                for terminal in ("collect", "batches"):
                    with self.subTest(kind=kind, columnar=columnar, terminal=terminal), \
                         patch("subprocess.Popen", side_effect=AssertionError("child process")), \
                         patch("shardloom._batches.BatchInput.encode", side_effect=AssertionError("JSON input")):
                        left, a = declaration(URIS[0], LEFT, columnar)
                        right, b = declaration(URIS[1], RIGHT, columnar)
                        batches, traces = ([left], [a]) if kind == "shared" else ([left, right], [a, b])
                        plan = session.sql(f"SELECT n,s,{expression} AS answer FROM 'memory://outer'", batches=batches)
                        self.assertTrue(all(trace["opened"] == 0 for trace in traces))
                        wanted = [dict(row, answer=answer) for row, answer in zip(LEFT, answers, strict=True)]
                        for execution in (1, 2):
                            if terminal == "collect":
                                result = plan.collect()
                                actual = pa.RecordBatchReader.from_stream(result).read_all()
                                report = result.report
                            else:
                                tables, stream = [], plan.iter_batches()
                                for batch in stream:
                                    self.assertIsNone(stream.report)
                                    tables.append(pa.RecordBatchReader.from_stream(batch).read_all())
                                report = stream.report
                                self.assertIsNotNone(report)
                                stream.close()
                                actual = pa.concat_tables(tables)
                            self.assertEqual(actual.to_pylist(), wanted)
                            self.complete(report, traces, execution)
                            if kind == "shared":
                                self.assertEqual(report["native_input_replay_prefilled_sources"], "1")
                                self.assertEqual(report["native_input_replay_passes"], "2")
                            elif kind == "parameterized":
                                self.assertEqual(report["native_input_replay_sources"], "1")
                                self.assertEqual(report["native_input_replay_passes"], "3")
                            elif kind == "guarded":
                                self.assertEqual(report["native_input_subquery_validation_sources"], "1")
                                self.assertEqual(report["native_input_subquery_build_rows_detached"], "0")
                            held.append((actual, wanted))
        session.close()
        for actual, wanted in held:
            self.assertEqual(actual.to_pylist(), wanted)

    def test_direct_scalar_typed_values_empty_inner_and_native_relation_plan(self):
        import pyarrow as pa
        from shardloom._input_schema import normalize_schema
        from shardloom._result_schema import _arrow_type
        from shardloom.native import Relation

        session, held = self.session(), []
        fixtures = (
            ("uint64", (1 << 64) - 1),
            ("decimal128(38,2)", Decimal("999999999999999999999999999999999999.99")),
            ("binary", b"\0\xff"), ("utf8", "東京 λ"),
            ("date32", -(1 << 31)), ("timestamp_micros", -(1 << 63)),
            ({"type": "list", "item": "int16"}, [1, None, -32768]),
            ({"type": "struct", "fields": {"tag": {"type": "utf8", "nullable": False},
                                             "values": {"type": "list", "item": "decimal128(8,2)"}}},
             {"tag": "λ", "values": [Decimal("-12.34"), None]}),
        )
        for dtype, value in fixtures:
            for columnar in (False, True):
                for empty in (False, True):
                    with self.subTest(dtype=dtype, columnar=columnar, empty=empty), \
                         patch("subprocess.Popen", side_effect=AssertionError("child process")), \
                         patch("shardloom._batches.BatchInput.encode", side_effect=AssertionError("JSON input")):
                        left, a = declaration(URIS[0], LEFT, columnar)
                        right, b = declaration(URIS[1], [] if empty else [{"v": value}], columnar, {"v": dtype})
                        relation = Relation.scan(URIS[0]).subquery(Relation.scan(URIS[1]), "answer")
                        result = session.plan(relation, batches=[left, right]).collect()
                        actual = pa.RecordBatchReader.from_stream(result).read_all()
                        schema = pa.schema([pa.field(name, _arrow_type(value, pa), nullable=value.nullable)
                                            for name, value in normalize_schema(dict(SCHEMA, answer=dtype))])
                        wanted = pa.Table.from_pylist([dict(row, answer=None if empty else value) for row in LEFT], schema=schema)
                        self.assertTrue(actual.equals(wanted, check_metadata=True), str(dtype))
                        self.complete(result.report, [a, b])
                        held.append((actual, wanted))
        session.close()
        for actual, wanted in held:
            self.assertTrue(actual.equals(wanted, check_metadata=True))

    def test_direct_unused_arrow_nan_is_validated_without_scalar_evaluation(self):
        import pyarrow as pa

        session = self.session()
        for nonfinite in (False, True):
            with self.subTest(nonfinite=nonfinite):
                left, a = declaration(URIS[0], LEFT, True)
                values = [float("nan"), float("inf")] if nonfinite else [1.0, 2.0]
                right, b = declaration(URIS[1], [{"v": value} for value in values], True, {"v": "float64"})
                plan = session.sql("SELECT n,CASE WHEN FALSE THEN (SELECT v FROM 'memory://inner') "
                                   "ELSE 7.0 END AS answer FROM 'memory://outer'", batches=[left, right])
                before = session.snapshot()["completed_executions"] or 0
                if nonfinite:
                    with self.assertRaisesRegex(RuntimeError, "contains non-finite float64"):
                        plan.collect()
                    self.assertEqual(session.snapshot()["completed_executions"], before)
                    self.assertEqual((a["closed"], b["closed"]), (1, 1))
                    self.assertEqual(b["ended"], 0)
                    self.assertEqual(session.from_range("fresh", 0, 1, 1).row_count, 1)
                else:
                    result = plan.collect()
                    self.assertEqual(pa.RecordBatchReader.from_stream(result).read_all().to_pylist(),
                                     [{"n": row["n"], "answer": 7.0} for row in LEFT])
                    self.complete(result.report, [a, b])
                    self.assertEqual(result.report["native_input_subquery_validation_sources"], "1")
                    self.assertEqual(result.report["native_input_subquery_build_rows_detached"], "0")

    def test_direct_unused_inner_faults_cancel_and_scalar_errors_prevent_publication(self):
        import pyarrow as pa

        session = self.session()
        for failure in ("producer", "schema", "cancel", "cardinality", "expression"):
            for terminal in ("collect", "zero", "fanout"):
                with self.subTest(failure=failure, terminal=terminal), \
                     tempfile.TemporaryDirectory(prefix="shardloom-subquery-fault-") as directory:
                    left, a = declaration(URIS[0], LEFT, True)
                    events, sentinel = [], LookupError("unused inner sentinel")

                    def produce():
                        events.append("opened")
                        try:
                            yield pa.table({"n": pa.array([2], pa.int64()), "s": ["b"]})
                            events.append("fault")
                            if failure == "producer":
                                raise sentinel
                            if failure == "schema":
                                yield pa.table({"changed": [1]})
                            elif failure == "cancel":
                                session.cancel()
                            else:
                                yield pa.table({"n": pa.array([3], pa.int64()), "s": ["c"]})
                        finally:
                            events.append("closed")

                    right = (URIS[1], produce, left[2], True)
                    scalar = "SELECT 1/0 AS bad FROM 'memory://inner'" if failure == "expression" else "SELECT n FROM 'memory://inner'"
                    expression = f"({scalar})" if failure in ("cardinality", "expression") else f"CASE WHEN FALSE THEN ({scalar}) ELSE 7 END"
                    plan = session.sql(f"SELECT n,{expression} AS answer FROM 'memory://outer'" +
                                       (" LIMIT 0" if terminal == "zero" else ""), batches=[left, right])
                    before = session.snapshot()["completed_executions"] or 0
                    root = Path(directory)
                    foreign = root / "foreign"
                    foreign.write_bytes(b"keep")
                    expected = LookupError if failure == "producer" else ValueError if failure == "schema" else RuntimeError
                    with self.assertRaises(expected) as raised:
                        if terminal == "fanout":
                            plan.write_many([(root / "result.vortex", "write_vortex"), (root / "result.jsonl", "write_jsonl")])
                        else:
                            plan.collect()
                    if failure == "producer":
                        self.assertIs(raised.exception, sentinel)
                    elif failure == "schema":
                        self.assertIn("schema", str(raised.exception))
                    else:
                        diagnostic = {"cancel": "cancel", "cardinality": "scalar subquery cardinality", "expression": "division by zero"}[failure]
                        self.assertIn(diagnostic, str(raised.exception))
                    self.assertEqual(events[-1], "closed")
                    self.assertEqual(a["closed"], 1)
                    self.assertEqual(session.snapshot()["completed_executions"], before)
                    self.assertEqual(list(root.iterdir()), [foreign])
                    self.assertEqual(foreign.read_bytes(), b"keep")
                    self.assertEqual(session.from_range("fresh", 0, 1, 1).row_count, 1)

    def test_public_native_scalar_close_and_consumer_exception_leave_retained_rows_valid(self):
        import shardloom as sl

        client = sl.ShardLoomClient(transport="native", memory_bytes=32 << 20, max_parallelism=1)
        self.addCleanup(client.close)
        held = []
        for shared in (False, True):
            for failure in ("close", "consumer", "cancel"):
                with self.subTest(shared=shared, failure=failure), \
                     patch("subprocess.Popen", side_effect=AssertionError("child process")), \
                     patch("shardloom._batches.BatchInput.encode", side_effect=AssertionError("JSON input")):
                    left, a = declaration(URIS[0], LEFT, True)
                    right, b = declaration(URIS[1], RIGHT, True)
                    outer = sl.from_batches(left[1], schema=SCHEMA, streaming=True, client=client)
                    inner = outer if shared else sl.from_batches(right[1], schema=SCHEMA, streaming=True, client=client)
                    query = outer.with_column("answer", sl.scalar_subquery(inner.select("n").limit(1)))
                    sentinel = LookupError("consumer sentinel")
                    try:
                        with query.iter_batches(batch_rows=1) as stream:
                            batch = next(stream)
                            self.assertIsNone(stream.report)
                            if failure == "consumer":
                                raise sentinel
                            if failure == "cancel":
                                client.cancel()
                                with self.assertRaisesRegex(sl.ShardLoomCommandError, "cancel"):
                                    list(stream)
                    except LookupError as error:
                        self.assertIs(error, sentinel)
                    self.assertIsNone(stream.report)
                    self.assertEqual(a["closed"], 1)
                    if not shared:
                        self.assertEqual(b["closed"], 1)
                    table = batch.to_arrow()
                    wanted = [dict(LEFT[0], answer=1 if shared else 2)]
                    self.assertEqual(table.to_pylist(), wanted)
                    held.append((table, wanted))
        client.close()
        for table, wanted in held:
            self.assertEqual(table.to_pylist(), wanted)
