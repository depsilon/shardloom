# SPDX-License-Identifier: Apache-2.0
"""Current typed unary and provider coverage beyond the ClickBench workload."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

if __package__:
    from .test_native_expression_binding import _load_native
    from .test_native_unary_binding import ROWS, KINDS, SCHEMA, arrow_schema, expected
else:
    from test_native_expression_binding import _load_native
    from test_native_unary_binding import ROWS, KINDS, SCHEMA, arrow_schema, expected


def typed_unary(source, kind):
    if kind == "distinct":
        return source.unary("distinct_rows")
    if kind.startswith(("dedup-", "mask-")):
        operation = "drop_duplicates" if kind.startswith("dedup-") else "duplicated"
        return source.unary(operation, "n", keep=kind.split("-", 1)[1])
    if kind == "tail":
        return source.unary("tail", "3")
    if kind == "sample":
        return source.unary("sample", '{"n":3,"seed":7}')
    if kind == "expression":
        columns = ["n", "s"]
        return source.unary("rewrite", json.dumps({
            "columns": columns,
            "rewrites": [{"kind": "forward_fill_null", "target_column": name} for name in columns],
        })).unary("rewrite", json.dumps({
            "columns": columns,
            "rewrites": [{"kind": "row_number", "target_column": "index"}],
        }))
    if kind == "melt":
        return source.unary("melt", '{"id_columns":["s"],"value_columns":["n"]}')
    if kind == "rolling":
        return source.unary("rolling", json.dumps({
            "source_column": "n", "output_column": "total", "window_size": 3,
            "min_periods": 1, "center": True, "aggregate": "sum",
        }))
    assert kind == "explode"
    return source.unary("explode", '{"column":"items"}')


class NativeSharedProviderBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()
        import pyarrow as pa
        cls.pa = pa

    def setUp(self):
        self.real_popen = subprocess.Popen
        self.process = patch("subprocess.Popen", side_effect=AssertionError("external process"))
        self.process.start()
        self.addCleanup(self.process.stop)
        self.session = self.native.Session(self.native.ExecutionResources(128 << 20, 2))
        self.addCleanup(self.session.close)

    def table(self, result):
        self.assertEqual(result.report["fallback_attempted"], "false")
        self.assertEqual(result.report["external_engine_invoked"], "false")
        return self.pa.RecordBatchReader.from_stream(result).read_all()

    def all_surfaces(self, frame, relation, statement):
        """Execute complete declarations through five real public entry points."""
        import shardloom as sl
        tables = [frame.collect(check=True).to_arrow(),
                  self.table(self.session.plan(relation).collect()),
                  self.table(self.session.sql(statement).collect())]
        arguments = ["sql", "--sql", statement, "--request", "collect", "--bounded", "true",
                     "--memory-bytes", str(128 << 20), "--max-parallelism", "2"]
        tables.append(self.table(self.session.workflow(arguments).collect()))
        # Native front doors above must not launch a process. The fifth entry
        # point explicitly exercises the CLI bundled in the installed wheel,
        # or an exact local test binary selected by the caller.
        with patch("subprocess.Popen", self.real_popen):
            cli = sl.ShardLoomClient(binary=os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY"),
                                    transport="subprocess", use_persistent_worker=False,
                                    memory_bytes=128 << 20, max_parallelism=2, timeout=30)
            try:
                result = sl.sql(statement, client=cli).collect(check=True)
                self.assertFalse(result.envelope.fallback.attempted)
                self.assertEqual(result.envelope.field("external_engine_invoked"), "false")
                tables.append(result.to_arrow())
            finally:
                cli.close()
        return tables

    def test_typed_unary_families_preserve_nulls_schema_order_and_reuse(self):
        native, session = self.native, self.session
        for shape, rows in (("mixed", ROWS), ("empty", []), ("nulls", [dict.fromkeys(SCHEMA)] * 3)):
            source = session.from_arrow(self.pa.Table.from_pylist(rows, schema=arrow_schema()))
            scan = native.Relation.scan("memory://unary").limit(len(rows))
            for kind in KINDS:
                with self.subTest(kind=kind, shape=shape):
                    relation = typed_unary(scan, kind)
                    plan = session.plan(relation, sources=[("memory://unary", source)])
                    wanted = self.pa.Table.from_pylist(expected(kind, rows), schema=arrow_schema(kind))
                    for repetition in range(2):
                        result = plan.collect()
                        actual = self.table(result)
                        self.assertTrue(actual.equals(wanted, check_metadata=True), (actual, wanted))
                        self.assertEqual(result.report["resident_relational_declaration_reused"],
                                         "true" if repetition else "false")

    def test_sql_arithmetic_uses_shared_signed_float_and_nullable_binding(self):
        import shardloom as sl
        native, session, pa = self.native, self.session, self.pa
        expr = native.Expression
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        cases = [(pa.uint32(), [None, 0, 1, 2**32 - 1]),
                 (pa.int64(), [None, -(2**53) - 1, -1, 0]),
                 (pa.float64(), [None, -1.5, 0.0, 2.25])]
        with tempfile.TemporaryDirectory(prefix="shardloom-shared-arithmetic-") as directory:
            for index, (dtype, values) in enumerate(cases):
                with self.subTest(dtype=dtype):
                    path = Path(directory) / f"input-{index}.vortex"
                    source = session.from_arrow(pa.table({"n": pa.array(values, type=dtype)}))
                    session.plan(native.Relation.scan("memory://input"),
                                 sources=[("memory://input", source)]).write(path, "write_vortex")
                    frame = (sl.read_vortex(path, client=client).with_column("shifted", sl.col("n") - 3)
                             .group_by("n", "shifted").agg(rows="COUNT(*)").sort("n", nulls="first"))
                    relation = (native.Relation.scan(str(path)).project([
                        ("n", expr.column("n")), ("shifted", expr.column("n") - expr.literal(3))])
                        .aggregate(["n", "shifted"], [("count", None, "rows")])
                        .sort([("n", False, "first")]))
                    sql = (f"SELECT n,n - 3 AS shifted,COUNT(*) AS rows FROM '{path}' "
                           "GROUP BY n,n - 3 ORDER BY n - 3 NULLS FIRST")
                    tables = self.all_surfaces(frame, relation, sql)
                    wanted = [dict(n=value, shifted=None if value is None else value-3, rows=1)
                              for value in values]
                    for table in tables:
                        self.assertEqual(table.to_pylist(), wanted)
                        self.assertEqual(table.schema, tables[0].schema)
                    # The primitive SUM offset also has to bind the expression
                    # before aggregation; cancellation and overflow stay terminal.
                    frame = sl.read_vortex(path, client=client).agg(total="SUM(n - 3)")
                    relation = (native.Relation.scan(str(path))
                                .project([("shifted", expr.column("n") - expr.literal(3))])
                                .aggregate([], [("sum", "shifted", "total")]))
                    tables = self.all_surfaces(frame, relation, f"SELECT SUM(n - 3) AS total FROM '{path}'")
                    for table in tables[1:]:
                        self.assertTrue(table.equals(tables[0], check_metadata=True), (table, tables[0]))
                    # Preserve the established unaliased primitive SQL names
                    # even when the complete statement needs typed binding.
                    frame = sl.read_vortex(path, client=client).agg(
                        sum_n_0="SUM(n)", sum_n_1="SUM(n - 3)")
                    relation = (native.Relation.scan(str(path)).project([
                        ("n", expr.column("n")), ("shifted", expr.column("n") - expr.literal(3))])
                        .aggregate([], [("sum", "n", "sum_n_0"), ("sum", "shifted", "sum_n_1")]))
                    tables = self.all_surfaces(
                        frame, relation, f"SELECT SUM(n),SUM(n - 3) FROM '{path}' ORDER BY SUM(n-3) NULLS FIRST")
                    for table in tables[1:]:
                        self.assertTrue(table.equals(tables[0], check_metadata=True), (table, tables[0]))

    def test_typed_unary_file_input_matches_existing_dataframe_and_sql(self):
        import shardloom as sl
        if __package__:
            from .test_native_unary_binding import apply
        else:
            from test_native_unary_binding import apply

        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        with tempfile.TemporaryDirectory(prefix="shardloom-shared-unary-") as directory:
            path = Path(directory) / "input.vortex"
            source = self.session.from_arrow(self.pa.Table.from_pylist(ROWS, schema=arrow_schema()))
            self.session.plan(self.native.Relation.scan("memory://rows"),
                              sources=[("memory://rows", source)]).write(path, "write_vortex")
            for kind in KINDS:
                with self.subTest(kind=kind):
                    typed = typed_unary(self.native.Relation.scan(str(path)), kind)
                    frame = apply(sl.read_vortex(path, client=client), kind)
                    statement = frame._relation_statement()
                    wanted = self.pa.Table.from_pylist(expected(kind, ROWS), schema=arrow_schema(kind))
                    tables = self.all_surfaces(frame, typed, statement)
                    for table in tables:
                        self.assertTrue(table.equals(wanted, check_metadata=True), (table, wanted))

    def test_typed_dynamic_pivot_keeps_downstream_binding_and_reuse(self):
        native, session = self.native, self.session
        expr = native.Expression
        rows = [{"entity": 1, "category": "a", "amount": 3},
                {"entity": 1, "category": "b", "amount": 5},
                {"entity": 2, "category": "a", "amount": 7}]
        source = session.from_rows(rows)
        options = json.dumps({"index": "entity", "columns": "category", "values": "amount", "aggregate": "sum"})
        relation = (native.Relation.scan("memory://pivot").unary("pivot", options)
                    .filter(expr.column("pivot_a") > expr.literal(4.0))
                    .project([("entity", expr.column("entity")), ("amount", expr.column("pivot_a"))]))
        plan = session.plan(relation, sources=[("memory://pivot", source)])
        for _ in range(2):
            self.assertEqual(self.table(plan.collect()).to_pylist(), [{"entity": 2, "amount": 7.0}])
        ranged = (native.Relation.scan("memory://pivot").unary("pivot", options)
                  .sort([("entity", False, "first")]).limit(1, offset=1)
                  .project([("entity", expr.column("entity")), ("amount", expr.column("pivot_a"))]))
        sql = ("SELECT entity,pivot_a AS amount FROM PIVOT((SELECT * FROM 'memory://pivot'), "
               f"'{options}') AS p ORDER BY entity NULLS FIRST LIMIT 1 OFFSET 1")
        for ranged_plan in (session.plan(ranged, sources=[("memory://pivot", source)]),
                            session.sql(sql, sources=[("memory://pivot", source)])):
            self.assertEqual(self.table(ranged_plan.collect()).to_pylist(), [{"entity": 2, "amount": 7.0}])
        invalid = native.Relation.scan("memory://pivot").unary("pivot", options).project(
            [("missing", expr.column("pivot_missing"))])
        with self.assertRaises(Exception):
            session.plan(invalid, sources=[("memory://pivot", source)]).collect()

    def test_typed_unary_rejects_invalid_metadata_before_source_access(self):
        scan = self.native.Relation.scan("missing-file-that-must-not-be-opened.vortex")
        cases = [("unknown", "*", None), ("tail", "0", None), ("tail", "-1", None),
                 ("tail", "1", "last"), ("drop_duplicates", "n", "middle"),
                 ("sample", '{"n":1,"fraction":0.5}', None),
                 ("sample", '{"n":1,"unknown":true}', None),
                 ("melt", '{"value_columns":["n"],"unknown":true}', None),
                 ("melt", '{"value_columns":["n"],"value_vars":["n"]}', None),
                 ("explode", '{"column":"n","column":"n"}', None),
                 ("pivot", " " * 65_537, None)]
        for operation, options, keep in cases:
            with self.subTest(operation=operation, options=options[:80]), self.assertRaises(Exception) as caught:
                scan.unary(operation, options, keep=keep)
            self.assertNotIn("No such file", str(caught.exception))

    def test_sql_collection_membership_is_rebound_for_directory_and_manifest(self):
        session, native = self.session, self.native
        with tempfile.TemporaryDirectory(prefix="shardloom-sql-collection-") as directory:
            root = Path(directory)
            parts = root / "parts"
            parts.mkdir()
            def write_part(name, start, count):
                source = session.from_range("value", start, 1, count)
                session.plan(native.Relation.scan("memory://part"),
                             sources=[("memory://part", source)]).write(parts / name, "write_vortex")

            write_part("a.vortex", 0, 2)
            manifest = root / "parts.json"
            manifest.write_text(json.dumps({"inputs": ["parts/a.vortex"]}), encoding="utf8")
            plans = [session.sql(f"SELECT COUNT(*) AS total FROM '{path}'", files=[(str(path), "vortex", None)])
                     for path in (parts, manifest)]
            for plan in plans:
                self.assertEqual(self.table(plan.collect()).to_pylist(), [{"total": 2}])
            write_part("b.vortex", 2, 3)
            manifest.write_text(json.dumps({"inputs": ["parts/b.vortex", "parts/a.vortex"]}), encoding="utf8")
            for plan in plans:
                result = plan.collect()
                self.assertEqual(self.table(result).to_pylist(), [{"total": 5}])
                self.assertEqual(result.report["resident_native_preparation_reused"], "false")

    def test_dataframe_offsets_keep_operator_position_and_match_typed_sql(self):
        import shardloom as sl
        native, session, pa = self.native, self.session, self.pa
        expr = native.Expression
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        with tempfile.TemporaryDirectory(prefix="shardloom-shared-offset-") as directory:
            path = Path(directory) / "input.vortex"
            data = pa.table({"value": [5, 0, 4, 1, 3, 2]},
                            schema=pa.schema([pa.field("value", pa.int64(), nullable=False)]))
            source = session.from_arrow(data)
            session.plan(native.Relation.scan("memory://input"),
                         sources=[("memory://input", source)]).write(path, "write_vortex")
            frame = sl.read_vortex(path, client=client).sort("value")
            typed = native.Relation.scan(str(path)).sort([("value", False, None)])
            ordered = f"SELECT * FROM '{path}' ORDER BY value"
            ranged = f"SELECT * FROM ({ordered} LIMIT 4 OFFSET 1) AS input"
            cases = [
                (frame.limit(2, offset=2), typed.limit(2, offset=2), ordered + " LIMIT 2 OFFSET 2",
                 [{"value": 2}, {"value": 3}]),
                (frame.limit(0, offset=3), typed.limit(0, offset=3), ordered + " LIMIT 0 OFFSET 3", []),
                (frame.limit(4, offset=1).filter(sl.col("value") > 2).sort("value", descending=True),
                 typed.limit(4, offset=1).filter(expr.column("value") > expr.literal(2)).sort([("value", True, None)]),
                 ranged + " WHERE value > 2 ORDER BY value DESC", [{"value": 4}, {"value": 3}]),
                (frame.limit(4, offset=1).agg("SUM(value) AS total"),
                 typed.limit(4, offset=1).aggregate([], [("sum", "value", "total")]),
                 f"SELECT SUM(value) AS total FROM ({ordered} LIMIT 4 OFFSET 1) AS input", [{"total": 10}]),
                (frame.limit(4, offset=1).limit(2, offset=1), typed.limit(4, offset=1).limit(2, offset=1),
                 ranged + " LIMIT 2 OFFSET 1", [{"value": 2}, {"value": 3}]),
            ]
            for index, (declaration, relation, sql, wanted) in enumerate(cases):
                with self.subTest(case=index):
                    results = self.all_surfaces(declaration, relation, sql)
                    for result in results:
                        self.assertEqual(result.to_pylist(), wanted)
                    self.assertTrue(all(table.schema == results[0].schema for table in results))

    def test_shared_scalar_spellings_preserve_nulls_units_and_unicode(self):
        import shardloom as sl
        native, session, pa = self.native, self.session, self.pa
        expr = native.Expression
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        rows = [{"time": -1, "text": "https://www.example.com/α"},
                {"time": 3661, "text": "http://other.test/path"},
                {"time": None, "text": None}]
        with tempfile.TemporaryDirectory(prefix="shardloom-shared-scalars-") as directory:
            path = Path(directory) / "input.vortex"
            source = session.from_arrow(pa.Table.from_pylist(rows, schema=pa.schema([
                pa.field("time", pa.int64()), pa.field("text", pa.string())])))
            session.plan(native.Relation.scan("memory://input"),
                         sources=[("memory://input", source)]).write(path, "write_vortex")
            functions = [("m", "extract_minute", "time"), ("minute", "date_trunc_minute", "time"),
                         ("domain", "url_domain", "text"), ("regex", "regex_domain", "text"),
                         ("bytes", "octet_length", "text"), ("characters", "length", "text")]
            frame = sl.read_vortex(path, client=client)
            for alias, function, column in functions[:4]:
                frame = frame.with_column(alias, getattr(sl.col(column), function)())
            frame = frame.select("m", "minute", "domain", "regex", "octet_length(text) AS bytes", "length(text) AS characters")
            relation = native.Relation.scan(str(path)).project([
                (alias, expr.call(function, [expr.column(column)])) for alias, function, column in functions])
            sql = "SELECT " + ",".join(f"{function}({column}) AS {alias}" for alias, function, column in functions) + f" FROM '{path}'"
            wanted = [{"m": 59, "minute": -60, "domain": "example.com", "regex": "example.com",
                       "bytes": len(rows[0]["text"].encode()), "characters": len(rows[0]["text"])},
                      {"m": 1, "minute": 3660, "domain": "other.test", "regex": "other.test",
                       "bytes": len(rows[1]["text"].encode()), "characters": len(rows[1]["text"])},
                      dict.fromkeys(alias for alias, _, _ in functions)]
            results = self.all_surfaces(frame, relation, sql)
            for result in results:
                self.assertEqual(result.to_pylist(), wanted)
            self.assertTrue(all(table.schema == results[0].schema for table in results))

    def test_relational_families_share_five_frontdoors(self):
        import shardloom as sl
        native, session, pa = self.native, self.session, self.pa
        expr = native.Expression
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        left_rows = [{"key": 1, "value": 5, "tag": "a"}, {"key": 2, "value": 7, "tag": "b"},
                     {"key": 2, "value": 3, "tag": "b"}, {"key": 3, "value": 11, "tag": "c"}]
        right_rows = [{"key": 2, "value": 20, "tag": "b"}, {"key": 4, "value": 40, "tag": "d"}]
        schema = pa.schema([pa.field("key", pa.int64(), nullable=False),
                            pa.field("value", pa.int64(), nullable=False), pa.field("tag", pa.string(), nullable=False)])
        with tempfile.TemporaryDirectory(prefix="shardloom-five-surfaces-") as directory:
            paths = [Path(directory) / (name + ".vortex") for name in ("left", "right")]
            for path, rows in zip(paths, (left_rows, right_rows), strict=True):
                source = session.from_arrow(pa.Table.from_pylist(rows, schema=schema))
                session.plan(native.Relation.scan("memory://input"),
                             sources=[("memory://input", source)]).write(path, "write_vortex")
            left, right = [sl.read_vortex(path, client=client) for path in paths]
            a, b = [native.Relation.scan(str(path)) for path in paths]
            names = [(name, expr.column(name)) for name in ("key", "value", "tag")]
            inner = b.project([("key", expr.column("key"))])
            cases = [
                ("filter_project", left.filter(sl.col("value") > 4).select("key AS id", "value + 1 AS shifted", "UPPER(tag) AS text"),
                 a.filter(expr.column("value") > expr.literal(4)).project([
                     ("id", expr.column("key")), ("shifted", expr.column("value") + expr.literal(1)),
                     ("text", expr.call("upper", [expr.column("tag")]))]),
                 f"SELECT key AS id,value + 1 AS shifted,UPPER(tag) AS text FROM '{paths[0]}' WHERE value > 4",
                 [{"id": 1, "shifted": 6, "text": "A"}, {"id": 2, "shifted": 8, "text": "B"}, {"id": 3, "shifted": 12, "text": "C"}]),
                ("aggregate", left.group_by("tag").agg(total="SUM(value)", rows="COUNT(*)").sort("tag"),
                 a.aggregate(["tag"], [("sum", "value", "total"), ("count", None, "rows")]).sort([("tag", False, None)]),
                 f"SELECT tag,SUM(value) AS total,COUNT(*) AS rows FROM '{paths[0]}' GROUP BY tag ORDER BY tag",
                 [{"tag": "a", "total": 5, "rows": 1}, {"tag": "b", "total": 10, "rows": 2}, {"tag": "c", "total": 11, "rows": 1}]),
                ("join", left.join(right, on="key", how="left").select("f.key AS key", "f.value AS left_value", "d.value AS right_value"),
                 a.join(b, [("key", "key")], [("left", "key", "key"), ("left", "value", "left_value"), ("right", "value", "right_value")], kind="left"),
                 f"SELECT f.key AS key,f.value AS left_value,d.value AS right_value FROM '{paths[0]}' AS f LEFT JOIN '{paths[1]}' AS d ON f.key = d.key",
                 [dict(key=row["key"], left_value=row["value"], right_value=20 if row["key"] == 2 else None) for row in left_rows]),
                ("window", left.select("key", "value").window(sl.row_number(order_by="value", alias="position")).sort("value"),
                 a.window(["key", "value"], [native.WindowExpression("row_number", "position", order_by=[("value", False, None)])]).sort([("value", False, None)]),
                 f"SELECT key,value,ROW_NUMBER() OVER (ORDER BY value) AS position FROM '{paths[0]}' ORDER BY value",
                 [dict(key=row["key"], value=row["value"], position=i) for i, row in enumerate(sorted(left_rows, key=lambda row: row["value"]), 1)]),
            ]
            for method, kind, sql_op, keys in (("union_all", "union_all", "UNION ALL", [1, 2, 2, 2, 3, 4]),
                                              ("union", "union_distinct", "UNION", [1, 2, 3, 4]),
                                              ("intersect", "intersect", "INTERSECT", [2]),
                                              ("except_", "except", "EXCEPT", [1, 3])):
                cases.append((kind, getattr(left.select("key"), method)(right.select("key")).sort("key"),
                              a.project([("key", expr.column("key"))]).set_operation(inner, kind=kind).sort([("key", False, None)]),
                              f"SELECT key FROM '{paths[0]}' {sql_op} SELECT key FROM '{paths[1]}' ORDER BY key",
                              [{"key": key} for key in keys]))
            subqueries = [
                ("in", sl.col("key").isin_source(str(paths[1]), "key"), "key IN", {"kind": "in", "keys": [("key", "key")]}, [row for row in left_rows if row["key"] == 2]),
                ("exists", sl.exists_source(str(paths[1])), "EXISTS", {"kind": "exists"}, left_rows),
                ("any", sl.any_source("key", ">", str(paths[1]), "key"), "key > ANY", {"kind": "quantified", "keys": [("key", "key")], "comparison": "gt"}, [left_rows[3]]),
                ("all", sl.all_source("key", "<", str(paths[1]), "key"), "key < ALL", {"kind": "quantified", "keys": [("key", "key")], "comparison": "lt", "quantifier": "all"}, [left_rows[0]]),
            ]
            for name, predicate, sql_predicate, options, wanted in subqueries:
                relation = a.subquery(inner, "member", **options).filter(expr.column("member")).project(names)
                cases.append((name, left.filter(predicate), relation,
                              f"SELECT * FROM '{paths[0]}' WHERE {sql_predicate} (SELECT key FROM '{paths[1]}')", wanted))
            options = json.dumps({"index": "key", "columns": "tag", "values": "value", "aggregate": "sum"})
            cases.append(("pivot", left.pivot_table(index="key", columns="tag", values="value", aggfunc="sum").sort("key"),
                          a.unary("pivot", options).sort([("key", False, None)]),
                          f"SELECT * FROM PIVOT((SELECT * FROM '{paths[0]}'), '{options}') AS p ORDER BY key",
                          [{"key": 1, "pivot_a": 5.0, "pivot_b": None, "pivot_c": None},
                           {"key": 2, "pivot_a": None, "pivot_b": 10.0, "pivot_c": None},
                           {"key": 3, "pivot_a": None, "pivot_b": None, "pivot_c": 11.0}]))
            for name, frame, relation, sql, wanted in cases:
                with self.subTest(family=name):
                    tables = self.all_surfaces(frame, relation, sql)
                    for table in tables:
                        self.assertEqual(table.to_pylist(), wanted)
                    self.assertTrue(all(table.schema == tables[0].schema for table in tables),
                                    [str(table.schema) for table in tables])


if __name__ == "__main__":
    unittest.main()
