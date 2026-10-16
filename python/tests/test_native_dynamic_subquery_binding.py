# SPDX-License-Identifier: Apache-2.0
"""Parameter-specific pivot predicates through the installed native Python boundary."""

from __future__ import annotations

from decimal import Decimal
import operator
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

if __package__:
    from .test_native_binding import _load_native
    from .test_native_subquery_binding import declaration, LEFT, RIGHT, SCHEMA, URIS
else:
    from test_native_binding import _load_native
    from test_native_subquery_binding import declaration, LEFT, RIGHT, SCHEMA, URIS


COMPARISONS = {"eq": ("=", operator.eq), "ne": ("!=", operator.ne),
               "lt": ("<", operator.lt), "le": ("<=", operator.le),
               "gt": (">", operator.gt), "ge": (">=", operator.ge)}
KINDS = ("in", "not-in", "row-in", "row-not-in", "exists", "not-exists",
         *(f"{quantifier}-{comparison}" for quantifier in ("any", "all") for comparison in COMPARISONS))


def inner_sql(shared=False):
    source = URIS[0] if shared else URIS[1]
    source_sql = (f"SELECT n AS entity,CASE WHEN n=1 THEN 'a' ELSE 'b' END AS category,"
                  f"n AS amount FROM '{source}' WHERE n <= outer.n")
    options = '{"index":"entity","columns":"category","values":"amount","aggregate":"sum"}'
    return ("SELECT CASE WHEN entity=2 THEN CAST(NULL AS BIGINT) ELSE entity END AS n,"
            "CASE WHEN entity=1 THEN 'a' ELSE 'b' END AS s "
            f"FROM PIVOT(({source_sql}), '{options}') AS dynamic_inner")


def statement(kind, shared=False):
    inner = f"({inner_sql(shared)}) AS i"
    if kind in ("in", "not-in", "row-in", "row-not-in"):
        left, columns = ("(n,s)", "n,s") if kind.startswith("row-") else ("n", "n")
        expression = f"{left} {'NOT IN' if 'not-in' in kind else 'IN'} (SELECT {columns} FROM {inner})"
    elif kind in ("exists", "not-exists"):
        expression = f"{'NOT EXISTS' if kind == 'not-exists' else 'EXISTS'} (SELECT * FROM {inner})"
    else:
        quantifier, comparison = kind.split("-")
        expression = f"n {COMPARISONS[comparison][0]} {quantifier.upper()} (SELECT n FROM {inner})"
    return f"SELECT n,s,{expression} AS answer FROM '{URIS[0]}'"


def expected(kind, shared=False):
    result = []
    for row in LEFT:
        groups = list(dict.fromkeys(other["n"] for other in (LEFT if shared else RIGHT)
                                   if row["n"] is not None and other["n"] is not None and other["n"] <= row["n"]))
        keys = [(None if value == 2 else value, "a" if value == 1 else "b") for value in groups]
        if kind in ("exists", "not-exists"):
            answer = bool(keys) if kind == "exists" else not keys
        elif kind.startswith(("any-", "all-")):
            quantifier, comparison = kind.split("-")
            values = [None if row["n"] is None or key[0] is None else COMPARISONS[comparison][1](row["n"], key[0])
                      for key in keys]
            decisive = quantifier == "any"
            answer = decisive if decisive in values else None if None in values else not decisive
        else:
            matches = []
            for key in keys:
                left_values = (row["n"], row["s"]) if kind.startswith("row-") else (row["n"],)
                right_values = key if kind.startswith("row-") else key[:1]
                components = [None if left_value is None or right_value is None else left_value == right_value
                              for left_value, right_value in zip(left_values, right_values, strict=True)]
                matches.append(False if False in components else None if None in components else True)
            answer = True if True in matches else None if None in matches else False
            if "not-in" in kind and answer is not None:
                answer = not answer
        result.append(dict(row, answer=answer))
    return result


class NativeDynamicSubqueryBindingTests(unittest.TestCase):
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
        self.assertEqual(int(report["native_input_batch_rows"]), sum(t["rows"] for t in traces) // execution)
        self.assertEqual(report["native_input_end_observed"], "true")
        self.assertEqual(report["native_input_output_ownership_detached"], "true")
        self.assertEqual(report["fallback_attempted"], "false")
        self.assertEqual(report["external_engine_invoked"], "false")

    def test_dynamic_predicates_rows_arrow_repeated_calls_and_owned_results(self):
        import pyarrow as pa

        for shared in (False, True):
            for kind in KINDS:
                session, held = self.session(), []
                for columnar in (False, True):
                    for terminal in ("collect", "batches"):
                        with self.subTest(shared=shared, kind=kind, columnar=columnar, terminal=terminal), \
                             patch("subprocess.Popen", side_effect=AssertionError("child process")), \
                             patch("shardloom._batches.BatchInput.encode", side_effect=AssertionError("JSON input")):
                            left, a = declaration(URIS[0], LEFT, columnar)
                            right, b = declaration(URIS[1], RIGHT, columnar)
                            batches, traces = ([left], [a]) if shared else ([left, right], [a, b])
                            plan = session.sql(statement(kind, shared), batches=batches)
                            self.assertTrue(all(trace["opened"] == 0 for trace in traces))
                            wanted = expected(kind, shared)
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
                                self.assertEqual(actual.schema, pa.schema([
                                    pa.field("n", pa.int64()), pa.field("s", pa.string()),
                                    pa.field("answer", pa.bool_(), nullable=kind not in ("exists", "not-exists")),
                                ]))
                                self.complete(report, traces, execution)
                                self.assertEqual(report["relational_dynamic_schema_stages"], "4")
                                self.assertEqual(report["native_input_replay_sources"], "1")
                                self.assertEqual(int(report["native_input_replay_prefilled_sources"]), int(shared))
                                held.append((actual, wanted))
                session.close()
                for actual, wanted in held:
                    self.assertEqual(actual.to_pylist(), wanted)
                del held

    def test_dynamic_inner_late_faults_and_unselected_queries_prevent_publication(self):
        import pyarrow as pa

        session = self.session()
        for failure in ("producer", "schema", "cancel"):
            for selection in ("selected", "guarded", "empty"):
                with self.subTest(failure=failure, selection=selection), \
                     tempfile.TemporaryDirectory(prefix="shardloom-dynamic-subquery-fault-") as directory:
                    left, a = declaration(URIS[0], LEFT, True)
                    events, sentinel = [], LookupError("dynamic inner sentinel")

                    def produce():
                        events.append("opened")
                        try:
                            yield pa.table({"n": pa.array([2], pa.int64()), "s": ["b"]})
                            events.append("fault")
                            if failure == "producer":
                                raise sentinel
                            if failure == "schema":
                                yield pa.table({"changed": [1]})
                            else:
                                session.cancel()
                        finally:
                            events.append("closed")

                    expression = f"EXISTS (SELECT * FROM ({inner_sql()}) AS i)"
                    if selection == "guarded":
                        expression = f"CASE WHEN FALSE THEN {expression} ELSE TRUE END"
                    sql = f"SELECT n,{expression} AS answer FROM '{URIS[0]}'" + (" LIMIT 0" if selection == "empty" else "")
                    plan = session.sql(sql, batches=[left, (URIS[1], produce, left[2], True)])
                    before = session.snapshot()["completed_executions"] or 0
                    root = Path(directory)
                    foreign = root / "foreign"
                    foreign.write_bytes(b"keep")
                    error_type = LookupError if failure == "producer" else ValueError if failure == "schema" else RuntimeError
                    with self.assertRaises(error_type) as raised:
                        plan.write_many([(root / "result.vortex", "write_vortex"), (root / "result.jsonl", "write_jsonl")])
                    if failure == "producer":
                        self.assertIs(raised.exception, sentinel)
                    else:
                        self.assertIn("schema" if failure == "schema" else "cancel", str(raised.exception))
                    self.assertEqual(events, ["opened", "fault", "closed"])
                    self.assertEqual(a["closed"], 1)
                    self.assertEqual(session.snapshot()["completed_executions"], before)
                    self.assertEqual(list(root.iterdir()), [foreign])
                    self.assertEqual(foreign.read_bytes(), b"keep")
                    self.assertEqual(session.from_range("fresh", 0, 1, 1).row_count, 1)

    def test_dynamic_predicate_preserves_exact_typed_outer_values(self):
        import pyarrow as pa
        from shardloom._input_schema import normalize_schema
        from shardloom._result_schema import _arrow_type

        session, held = self.session(), []
        schema = {"n": "int64", "wide": "uint64", "amount": "decimal128(38,2)",
                  "items": {"type": "list", "item": "int16"}, "bytes": "binary"}
        rows = [dict(n=1, wide=(1 << 64) - 1, amount=Decimal("999999999999999999999999999999999999.99"),
                     items=[1, None, -32768], bytes=b"\x00\xff"),
                dict(n=3, wide=0, amount=Decimal("-1.25"), items=[], bytes=b"")]
        wanted = [dict(rows[0], answer=False), dict(rows[1], answer=True)]
        output_schema = pa.schema([pa.field(name, _arrow_type(dtype, pa), nullable=dtype.nullable)
                                  for name, dtype in normalize_schema(schema)] + [pa.field("answer", pa.bool_(), nullable=False)])
        for columnar in (False, True):
            with self.subTest(columnar=columnar), \
                 patch("subprocess.Popen", side_effect=AssertionError("child process")), \
                 patch("shardloom._batches.BatchInput.encode", side_effect=AssertionError("JSON input")):
                left, a = declaration(URIS[0], rows, columnar, schema)
                right, b = declaration(URIS[1], RIGHT, columnar)
                sql = f"SELECT n,wide,amount,items,bytes,EXISTS (SELECT * FROM ({inner_sql()}) AS i) AS answer FROM '{URIS[0]}'"
                result = session.sql(sql, batches=[left, right]).collect()
                actual = pa.RecordBatchReader.from_stream(result).read_all()
                reference = pa.Table.from_pylist(wanted, schema=output_schema)
                self.assertTrue(actual.equals(reference, check_metadata=True))
                self.complete(result.report, [a, b])
                held.append((actual, reference))
        session.close()
        for actual, reference in held:
            self.assertTrue(actual.equals(reference, check_metadata=True))

    def test_public_dynamic_subquery_close_cancel_and_consumer_error_preserve_retained_batch(self):
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
                    inner = inner.filter(sl.col("n") <= sl.outer("n")).select(
                        "n AS entity", "s AS category", "n AS amount").pivot_table(
                            index="entity", columns="category", values="amount", aggfunc="sum")
                    query = outer.with_column("answer", sl.exists_source(inner))
                    sentinel = LookupError("dynamic consumer sentinel")
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
                    actual = batch.to_arrow()
                    wanted = [dict(LEFT[0], answer=shared)]
                    self.assertEqual(actual.to_pylist(), wanted)
                    held.append((actual, wanted))
        client.close()
        for actual, wanted in held:
            self.assertEqual(actual.to_pylist(), wanted)
