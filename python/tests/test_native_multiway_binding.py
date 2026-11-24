"""Three-input declarations and retained sources preserve exact native join semantics."""

import gc
import json
import unittest
from unittest.mock import patch

if __package__:
    from .test_native_binding import _load_native
else:
    from test_native_binding import _load_native


SOURCES = {
    "r": [(1, 5, 0), (2, 5, 1), (1, 5, 2), (None, 5, 3), (1, None, 4)],
    "s": [(5, 9, 0), (5, 9, 1), (5, 10, 2), (None, 9, 3)],
    "t": [(1, 9, 0), (2, 10, 1), (1, 9, 2), (None, 9, 3)],
}
EXPECTED = [dict(rid=r, sid=s, tid=t) for r, s, t in (
    (0, 0, 0), (0, 0, 2), (0, 1, 0), (0, 1, 2), (1, 2, 1),
    (2, 0, 0), (2, 0, 2), (2, 1, 0), (2, 1, 2))]


def statement(lower="INNER"):
    return ("SELECT q.rid AS rid,q.sid AS sid,t.id AS tid FROM "
            "(SELECT r.x AS a,s.y AS c,r.id AS rid,s.id AS sid FROM 'memory://r' AS r "
            f"{lower} JOIN 'memory://s' AS s ON r.y = s.x) AS q "
            "INNER JOIN 'memory://t' AS t ON q.c = t.y AND q.a = t.x")


class NativeMultiwayBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()

    def session(self):
        session = self.native.Session(self.native.ExecutionResources(64 << 20, 1))
        self.addCleanup(session.close)
        return session

    def test_installed_public_command_preserves_three_input_join_semantics(self):
        session = self.session()
        bindings = {"memory://" + name: {"input_format": "memory", "memory_input": {
            "kind": "rows", "schema": [[name, "int64"] for name in ("x", "y", "id")],
            "rows": [[None if value is None else str(value) for value in row] for row in rows],
        }} for name, rows in SOURCES.items()}
        with patch("subprocess.Popen", side_effect=AssertionError("child process")):
            for surface in ("sql", "python", "dataframe", "cli"):
                for lower in ("INNER", "LEFT"):
                    with self.subTest(surface=surface, lower=lower):
                        response = json.loads(session.command([
                            "run", surface, "--sql", statement(lower), "--request", "collect",
                            "--bounded", "true", "--source-bindings", json.dumps(bindings),
                            "--memory-bytes", str(64 << 20), "--max-parallelism", "1", "--format", "json",
                        ]))
                        self.assertEqual(response["status"], "success", response)
                        fields = {item["key"]: item["value"] for item in response["fields"]}
                        self.assertEqual([json.loads(row) for row in fields["result_jsonl"].splitlines()], EXPECTED)
                        self.assertEqual(fields["external_engine_invoked"], "false")
                        self.assertFalse(response["fallback"]["attempted"])
                        self.assertFalse(response["fallback"]["allowed"])
                        self.assertEqual(session.snapshot()["reserved_bytes"], 0)

    def test_chunked_object_sources_preserve_ordinary_exact_repeated_execution(self):
        session = self.session()
        sources = [("memory://" + name, session.from_rows([
            dict(zip(("x", "y", "id"), row)) for row in rows])) for name, rows in SOURCES.items()]
        plan = session.sql(statement(), sources=sources)
        with patch("subprocess.Popen", side_effect=AssertionError("child process")):
            for _ in range(2):
                result = plan.collect()
                self.assertEqual(result.row_export()[0], EXPECTED)
                self.assertEqual(result.report["external_engine_invoked"], "false")
                del result
        plan.release_prepared()
        del plan, sources
        gc.collect()
        self.assertEqual(session.snapshot()["reserved_bytes"], 0)

    def test_cyclic_existence_native_sized_sources_preserve_exact_semantics(self):
        from shardloom._result_schema import ResultType, schema_fields

        session = self.session()
        inputs = {
            "r": [dict(x=None if n % 37 == 0 else n % 16,
                       y=None if n % 41 == 0 else (n // 16) % 16, id=n)
                  for n in range(512)],
            "s": [dict(x=None if n % 43 == 0 else n % 16,
                       y=None if n % 47 == 0 else n // 16, id=n)
                  for n in range(256)],
            "t": [dict(x=None if n % 53 == 0 else 2 * (n % 8),
                       y=None if n % 59 == 0 else (n // 8) % 16, id=n)
                  for n in range(256)],
        }
        # An independent scalar oracle preserves each outer row and its order.
        witnesses = {(middle["x"], last["x"])
                     for middle in inputs["s"] for last in inputs["t"]
                     if middle["x"] is not None and last["x"] is not None
                     and middle["y"] is not None and middle["y"] == last["y"]}
        sources = [("memory://" + name, session.from_rows(rows, fields=[
            (field, json.dumps({"Primitive": ["i64", field != "id"]}))
            for field in ("x", "y", "id")])) for name, rows in inputs.items()]
        baseline = session.snapshot()["reserved_bytes"]
        expected_schema = (("outer_id", ResultType("int64", False)),
                           ("b", ResultType("int64", True)),
                           ("a", ResultType("int64", True)))
        inner = self.native.Relation.scan("memory://s").join(
            self.native.Relation.scan("memory://t"), [("y", "y")],
            [("left", "x", "witness_b"), ("right", "x", "witness_a")],
        )
        with patch("subprocess.Popen", side_effect=AssertionError("child process")):
            for kind in ("left_semi", "left_anti"):
                expected = [dict(outer_id=row["id"], b=row["y"], a=row["x"])
                            for row in inputs["r"]
                            if ((row["y"], row["x"]) in witnesses) == (kind == "left_semi")]
                self.assertGreater(len(expected), 0)
                self.assertLess(len(expected), len(inputs["r"]))
                for reverse in (False, True):
                    with self.subTest(kind=kind, reverse=reverse):
                        keys = [("y", "witness_b"), ("x", "witness_a")]
                        relation = self.native.Relation.scan("memory://r").join(
                            inner, list(reversed(keys)) if reverse else keys,
                            [("left", "id", "outer_id"), ("left", "y", "b"),
                             ("left", "x", "a")], kind=kind,
                        )
                        plan = session.plan(relation, sources=sources)
                        plan.prepare()
                        for _ in range(2):
                            result = plan.collect()
                            rows, export = result.row_export()
                            self.assertEqual(rows, expected)
                            self.assertTrue(export["complete"])
                            self.assertEqual(schema_fields(result.schema_json, "vortex.dtype.serde.v1"),
                                             expected_schema)
                            self.assertEqual(result.report["external_engine_invoked"], "false")
                            self.assertEqual(result.report["fallback_attempted"], "false")
                            self.assertEqual(result.report["resident_relational_declaration_reused"], "true")
                            del result
                        plan.release_prepared()
                        del plan
                        self.assertEqual(session.snapshot()["reserved_bytes"], baseline)
        del sources
        gc.collect()
        self.assertEqual(session.snapshot()["reserved_bytes"], 0)

    def test_typed_cyclic_existence_preserves_outer_rows_and_prepared_reuse(self):
        from shardloom._result_schema import ResultType, schema_fields

        session = self.session()
        sources = [("memory://" + name, session.from_rows(
            [dict(zip(("x", "y", "id"), row)) for row in rows],
            fields=[(field, json.dumps({"Primitive": ["i64", True]}))
                    for field in ("x", "y", "id")],
        )) for name, rows in SOURCES.items()]
        baseline = session.snapshot()["reserved_bytes"]
        expected_schema = tuple((name, ResultType("int64", True))
                                for name in ("outer_id", "b", "a"))
        inner = self.native.Relation.scan("memory://s").join(
            self.native.Relation.scan("memory://t"), [("y", "y")],
            [("left", "x", "witness_b"), ("right", "x", "witness_a")],
        )
        with patch("subprocess.Popen", side_effect=AssertionError("child process")):
            for kind, expected in (
                ("left_semi", [dict(outer_id=0, b=5, a=1),
                               dict(outer_id=1, b=5, a=2),
                               dict(outer_id=2, b=5, a=1)]),
                ("left_anti", [dict(outer_id=3, b=5, a=None),
                               dict(outer_id=4, b=None, a=1)]),
            ):
                for reverse in (False, True):
                    with self.subTest(kind=kind, reverse=reverse):
                        keys = [("y", "witness_b"), ("x", "witness_a")]
                        relation = self.native.Relation.scan("memory://r").join(
                            inner, list(reversed(keys)) if reverse else keys,
                            [("left", "id", "outer_id"), ("left", "y", "b"),
                             ("left", "x", "a")], kind=kind,
                        )
                        plan = session.plan(relation, sources=sources)
                        plan.prepare()
                        for _ in range(2):
                            result = plan.collect()
                            rows, export = result.row_export()
                            self.assertEqual(rows, expected)
                            self.assertTrue(export["complete"])
                            self.assertEqual(schema_fields(result.schema_json, "vortex.dtype.serde.v1"),
                                             expected_schema)
                            self.assertEqual(result.report["external_engine_invoked"], "false")
                            self.assertEqual(result.report["fallback_attempted"], "false")
                            self.assertEqual(result.report["resident_relational_declaration_reused"], "true")
                            del result
                        plan.release_prepared()
                        del plan
                        self.assertEqual(session.snapshot()["reserved_bytes"], baseline)
        del sources
        gc.collect()
        self.assertEqual(session.snapshot()["reserved_bytes"], 0)

    def test_canonical_flat_three_and_four_way_sql_joins_preserve_exact_semantics(self):
        from pathlib import Path
        import tempfile

        session = self.session()
        i64 = json.dumps({"Primitive": ["i64", True]})
        utf8 = json.dumps({"Utf8": True})
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fact_rows = [
                {"id": 1, "a_id": 10, "b_id": 100, "amount": 5},
                {"id": 2, "a_id": 20, "b_id": 200, "amount": 10},
                {"id": 3, "a_id": 10, "b_id": 100, "amount": 15},
                {"id": 4, "a_id": 30, "b_id": 200, "amount": 20},
                {"id": 5, "a_id": None, "b_id": 100, "amount": 25},
            ]
            dim_a_rows = [{"id": 10, "grp": "alpha"}, {"id": 20, "grp": "beta"}]
            dim_b_rows = [{"id": 100, "mult": 2}, {"id": 200, "mult": 3}]
            dim_c_rows = [{"grp": "alpha", "bonus": 7}, {"grp": "gamma", "bonus": 9}]

            fact_pq = root / "fact.parquet"
            dim_a_vx = root / "dim_a.vortex"
            dim_b_vx = root / "dim_b.vortex"
            for rows, fields, out_path, write_fmt in (
                (
                    fact_rows,
                    [("id", i64), ("a_id", i64), ("b_id", i64), ("amount", i64)],
                    fact_pq,
                    "write_parquet",
                ),
                (dim_a_rows, [("id", i64), ("grp", utf8)], dim_a_vx, "write_vortex"),
                (dim_b_rows, [("id", i64), ("mult", i64)], dim_b_vx, "write_vortex"),
            ):
                src = session.from_rows(rows, fields=fields)
                write_plan = session.plan(
                    self.native.Relation.scan("memory://fixture"),
                    sources=[("memory://fixture", src)],
                )
                write_plan.write(out_path, write_fmt)
                write_plan.release_prepared()
                del write_plan, src
            gc.collect()
            self.assertEqual(session.snapshot()["reserved_bytes"], 0)

            sql_files = [
                (str(fact_pq), "parquet", None),
                (str(dim_a_vx), "vortex", None),
                (str(dim_b_vx), "vortex", None),
            ]
            sql_3way = (
                f"SELECT a.grp AS grp, SUM(f.amount * b.mult) AS total, COUNT(*) AS rows_n "
                f"FROM '{fact_pq}' AS f "
                f"INNER JOIN '{dim_a_vx}' AS a ON a.id = f.a_id "
                f"INNER JOIN '{dim_b_vx}' AS b ON f.b_id = b.id "
                f"WHERE f.amount >= 5 "
                f"GROUP BY a.grp HAVING SUM(f.amount * b.mult) > 35 "
                f"ORDER BY grp"
            )
            with patch("subprocess.Popen", side_effect=AssertionError("child process")):
                plan_3way = session.sql(sql_3way, files=sql_files)
                res_3way = plan_3way.collect()
                self.assertEqual(res_3way.row_export()[0], [{"grp": "alpha", "total": 40, "rows_n": 2}])
                self.assertEqual(res_3way.report["external_engine_invoked"], "false")
                self.assertEqual(res_3way.report["fallback_attempted"], "false")
                del res_3way
                plan_3way.release_prepared()
                del plan_3way
                gc.collect()
                cached_bytes = session.snapshot()["reserved_bytes"]

                dim_c_src = session.from_rows(dim_c_rows, fields=[("grp", utf8), ("bonus", i64)])
                dim_c_plan = session.plan(
                    self.native.Relation.scan("memory://dim_c"),
                    sources=[("memory://dim_c", dim_c_src)],
                )
                sql_4way = (
                    f"SELECT f.id AS fid, a.grp AS grp, b.mult AS mult, "
                    f"CASE WHEN c.bonus IS NULL THEN 0 ELSE c.bonus END AS bonus, "
                    f"COALESCE(c.bonus, 0) + f.amount AS adj "
                    f"FROM '{fact_pq}' AS f "
                    f"INNER JOIN '{dim_a_vx}' AS a ON a.id = f.a_id "
                    f"INNER JOIN '{dim_b_vx}' AS b ON b.id = f.b_id "
                    f"LEFT JOIN dim_c AS c ON a.grp = c.grp "
                    f"WHERE f.amount >= :min_amount "
                    f"ORDER BY f.id"
                )
                plan_4way = session.sql(
                    sql_4way,
                    files=sql_files,
                    relations=[("dim_c", dim_c_plan)],
                    parameters=[("min_amount", self.native.Expression.literal(5))],
                )
                res_4way = plan_4way.collect()
                self.assertEqual(
                    res_4way.row_export()[0],
                    [
                        {"fid": 1, "grp": "alpha", "mult": 2, "bonus": 7, "adj": 12},
                        {"fid": 2, "grp": "beta", "mult": 3, "bonus": 0, "adj": 10},
                        {"fid": 3, "grp": "alpha", "mult": 2, "bonus": 7, "adj": 22},
                    ],
                )
                self.assertEqual(res_4way.report["external_engine_invoked"], "false")
                self.assertEqual(res_4way.report["fallback_attempted"], "false")
                del res_4way
                plan_4way.release_prepared()
                dim_c_plan.release_prepared()
                del plan_4way, dim_c_plan, dim_c_src
                gc.collect()
                self.assertEqual(session.snapshot()["reserved_bytes"], cached_bytes)
        session.close()
        gc.collect()
        self.assertEqual(session.snapshot()["reserved_bytes"], 0)


if __name__ == "__main__":
    unittest.main()
