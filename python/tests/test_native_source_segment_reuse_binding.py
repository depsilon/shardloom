# SPDX-License-Identifier: Apache-2.0
"""Automatic immutable segment reuse follows public prepared-source lifetimes."""
import gc
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

if __package__:
    from .test_native_optimization_activation_binding import _load_native, evidence, write_sources
else:
    from test_native_optimization_activation_binding import _load_native, evidence, write_sources


class NativeSourceSegmentReuseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()

    def consume(self, plan, public):
        from shardloom._result_schema import ResultType, schema_fields

        result = plan.collect(check=True) if public else plan.collect()
        if public:
            rows, schema = result.to_rows(), result.schema
            fields = dict(result.envelope.field_map)
        else:
            rows, exported = result.row_export()
            self.assertTrue(exported["complete"])
            schema = schema_fields(result.schema_json, "vortex.dtype.serde.v1")
            fields = dict(result.report)
        self.assertEqual(list(rows), [{"id": n} for n in range(4096) if n % 16 == 3])
        self.assertEqual(schema, (("id", ResultType("int64", False)),))
        self.assertEqual(fields["fallback_attempted"], "false")
        self.assertEqual(fields["external_engine_invoked"], "false")
        report = evidence(fields, "file_scans")
        del result
        return report

    def check_pair(self, cold, reused):
        self.assertEqual(len(cold["sources"]), 1)
        self.assertEqual(len(reused["sources"]), 1)
        first, second = cold["sources"][0], reused["sources"][0]
        self.assertEqual(first["generation_fingerprint"], second["generation_fingerprint"])
        self.assertEqual(first["segment_reuse"]["cross_call_hits"], 0)
        self.assertGreater(first["segment_reuse"]["stored_segments"], 0)
        self.assertGreater(first["completed_read_requests"], 0)
        self.assertGreater(second["segment_reuse"]["cross_call_hits"], 0)
        self.assertGreater(second["segment_reuse"]["cross_call_hit_segment_bytes"], 0)
        self.assertLess(second["completed_read_bytes"], first["completed_read_bytes"])
        for report in (cold, reused):
            cache = report["session_segment_cache"]
            self.assertTrue(cache["enabled"])
            self.assertFalse(cache["counters_overflowed"])
            self.assertGreater(cache["retained_entries"], 0)
            self.assertLessEqual(cache["retained_entries"], 128)
            self.assertLessEqual(cache["retained_capacity_bytes"], cache["retention_limit_bytes"])

    def test_five_public_surfaces_reuse_automatically_then_release_owned_segments(self):
        import shardloom as sl

        with tempfile.TemporaryDirectory(prefix="shardloom-source-segment-reuse-") as directory:
            paths = write_sources(self.native, Path(directory), "vortex")
            path = paths["counts"][0]
            sql = f"SELECT id FROM '{path}' WHERE key = 3 ORDER BY id"
            for surface in ("python_sql", "typed_relation", "embedded_workflow", "dataframe", "cli_sql"):
                with self.subTest(surface=surface):
                    public = surface in {"dataframe", "cli_sql"}
                    client = session = plan = None
                    try:
                        if public:
                            client = sl.ShardLoomClient(
                                binary=os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY"),
                                transport="subprocess" if surface == "cli_sql" else "native",
                                use_persistent_worker=surface == "cli_sql",
                                memory_bytes=64 << 20, max_parallelism=1)
                            plan = (sl.sql(sql, client=client) if surface == "cli_sql" else
                                    sl.read_vortex(path, client=client).filter(sl.col("key") == 3)
                                      .select("id").sort("id"))
                        else:
                            session = self.native.Session(self.native.ExecutionResources(64 << 20, 1))
                            if surface == "python_sql":
                                plan = session.sql(sql, files=[(str(path), "vortex", None)])
                            elif surface == "typed_relation":
                                E = self.native.Expression
                                relation = self.native.Relation.scan(str(path)).filter(E.column("key") == E.literal(3))
                                relation = relation.project([("id", E.column("id"))]).sort([("id", False, "last")])
                                plan = session.plan(relation, files=[(str(path), "vortex", None)])
                            else:
                                plan = session.workflow(["sql", "--sql", sql, "--request", "collect", "--bounded", "true",
                                    "--memory-bytes", str(64 << 20), "--max-parallelism", "1"])
                        # These are two declared calls through the ordinary API;
                        # no explicit cache option, warmup or native-only shortcut.
                        if surface == "cli_sql":
                            cold, reused = self.consume(plan, public), self.consume(plan, public)
                        else:
                            with patch.object(subprocess, "Popen", side_effect=AssertionError("external execution")):
                                cold, reused = self.consume(plan, public), self.consume(plan, public)
                        self.check_pair(cold, reused)
                        if public:
                            client.release_prepared()
                            if surface == "dataframe":
                                self.assertEqual(client._native_backend.session.snapshot()["reserved_bytes"], 0)
                        else:
                            plan.release_prepared()
                            self.assertEqual(session.snapshot()["reserved_bytes"], 0)
                    finally:
                        del plan
                        if client is not None:
                            client.close()
                        if session is not None:
                            session.close()
                    gc.collect()

    def test_independent_prepared_owners_stay_cold_and_generation_changes_refuse_reuse(self):
        with tempfile.TemporaryDirectory(prefix="shardloom-source-segment-generation-") as directory:
            root = Path(directory)
            path = write_sources(self.native, root, "vortex")["counts"][0]
            session = self.native.Session(self.native.ExecutionResources(64 << 20, 1))
            self.addCleanup(session.close)
            sql = f"SELECT id FROM '{path}' WHERE key = 3 ORDER BY id"
            plans = [session.sql(sql, files=[(str(path), "vortex", None)]) for _ in range(2)]
            cold = self.consume(plans[0], False)
            independent = self.consume(plans[1], False)
            self.assertEqual(independent["sources"][0]["segment_reuse"]["cross_call_hits"], 0)
            self.assertGreater(independent["sources"][0]["completed_read_bytes"], 0)
            reused = self.consume(plans[0], False)
            self.check_pair(cold, reused)
            replacement = root / "replacement.vortex"
            replacement.write_bytes(path.read_bytes())
            replacement.replace(path)
            for plan in plans:
                with self.assertRaises(Exception) as error:
                    plan.collect()
                self.assertIn("prepared source changed", str(error.exception).lower())
                plan.release_prepared()
            del plan, plans
            gc.collect()
            self.assertEqual(session.snapshot()["reserved_bytes"], 0)


if __name__ == "__main__":
    unittest.main()
