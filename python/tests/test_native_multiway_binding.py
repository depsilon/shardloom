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


if __name__ == "__main__":
    unittest.main()
