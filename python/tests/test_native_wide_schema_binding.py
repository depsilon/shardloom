"""Complete wide values across the shared native query, sink and owner lifecycle."""

from __future__ import annotations

import gc
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

if __package__:
    from .test_native_binding import _load_native
    from .test_native_vector_representations_binding import _schema_shape
    from .wide_schema_fixture import KINDS, arrow_schema, digest, joined_rows, row, schema, tags
else:
    from test_native_binding import _load_native
    from test_native_vector_representations_binding import _schema_shape
    from wide_schema_fixture import KINDS, arrow_schema, digest, joined_rows, row, schema, tags

GRANT = 512 << 20


def _reopen(kind, paths):
    _load_native()
    import pyarrow as pa
    import shardloom as sl
    reports = []
    expected = digest(joined_rows(kind))
    expected_schema = _schema_shape(arrow_schema(pa, kind, joined=True))
    with sl.context(transport="native", memory_bytes=GRANT, max_parallelism=1) as context:
        for raw in paths:
            frame = context.read_vortex(raw)
            result = frame.collect(check=True)
            tables = {"vortex": result.to_arrow()}
            for format_name in ("arrow_ipc", "parquet"):
                destination = Path(raw).with_suffix("." + format_name)
                getattr(frame, "write_" + format_name)(destination, check=True)
                tables[format_name] = getattr(context, "read_" + format_name)(destination).collect(check=True).to_arrow()
            values = {name: digest(table.to_pylist()) for name, table in tables.items()}
            assert all(value == expected for value in values.values()), values
            assert all(_schema_shape(table.schema) == expected_schema for table in tables.values())
            assert not result.envelope.field_bool("fallback_attempted")
            assert not result.envelope.field_bool("external_engine_invoked")
            reports.append({"digests": values, "schema_verified": True, "rows": len(joined_rows(kind))})
    return reports


def _prior_refusals():
    _load_native()
    import shardloom as sl
    from shardloom.errors import ShardLoomCommandError
    observations = []
    for kind in KINDS:
        with sl.context(transport="native", memory_bytes=GRANT, max_parallelism=1) as context:
            try:
                context.from_rows([row(kind, ordinal) for ordinal in range(1, 9)], schema=schema(kind)).collect(check=True)
            except ShardLoomCommandError as error:
                expected = "1..=1024 fields" if kind == "siblings" else "4096 nodes"
                if expected not in str(error):
                    raise AssertionError("predecessor failed outside the former schema boundary") from error
                assert not error.envelope.fallback.attempted
                observations.append({"kind": kind, "refused": True, "expected_boundary": expected,
                                     "diagnostic_codes": [item.code for item in error.diagnostics],
                                     "error": str(error)})
            else:
                raise AssertionError("accepted predecessor unexpectedly admitted " + kind)
    return observations


def _cold_reopen(kind, path, grant):
    _load_native()
    import shardloom as sl
    from shardloom.errors import ShardLoomCommandError
    with sl.context(transport="native", memory_bytes=grant, max_parallelism=1) as context:
        try:
            result = context.read_vortex(path).collect(check=True)
        except ShardLoomCommandError as error:
            assert "memory reservation denied" in str(error), str(error)
            assert not error.envelope.fallback.attempted
            assert not error.envelope.field_bool("external_engine_invoked")
            report = {"status": "refused", "codes": [item.code for item in error.diagnostics]}
        else:
            import pyarrow as pa
            table = result.to_arrow()
            assert digest(table.to_pylist()) == digest([row(kind, 1), row(kind, 2)])
            assert _schema_shape(table.schema) == _schema_shape(arrow_schema(pa, kind))
            assert not result.envelope.field_bool("fallback_attempted")
            assert not result.envelope.field_bool("external_engine_invoked")
            report = {"status": "values_passed"}
            del table, result
        session = context.client._native_backend.session
    gc.collect()
    assert session.snapshot()["reserved_bytes"] == 0, session.snapshot()
    report.update(kind=kind, grant=grant, released=True)
    return report


class NativeWideSchemaBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()

    def context(self):
        import shardloom as sl
        context = sl.context(transport="native", memory_bytes=GRANT, max_parallelism=1)
        self.addCleanup(context.close)
        return context

    def queries(self, context, kind, count):
        import pyarrow as pa
        # Preserve a real nonzero offset through the Arrow import boundary.
        table = pa.Table.from_pylist([row(kind, ordinal) for ordinal in range(count + 2)],
                                    schema=arrow_schema(pa, kind)).slice(1, count)
        self.assertEqual(table.column("id").chunk(0).offset, 1)
        left = context.from_arrow_table(table)
        right = context.from_arrow_table(pa.Table.from_pylist(
            [{"id": ordinal, "tag": tag} for ordinal in range(1, count + 1)
             for tag in tags(ordinal) if tag is not None]))
        selection = ["f.id AS id", "f.payload AS payload", "f.extra AS extra", "d.tag AS tag"]
        dataframe = left.where("id >= 2").join(right, on="id", how="left").select(*selection).sort(
            "id", "tag", descending=True, nulls="last")
        sql = context.sql("SELECT " + ", ".join(selection) +
            " FROM wide AS f LEFT JOIN reports AS d ON f.id = d.id WHERE f.id >= 2 "
            "ORDER BY f.id DESC, d.tag DESC NULLS LAST", relations={"wide": left, "reports": right})
        return dataframe, sql

    def assert_values(self, table, kind, expected, *, joined=False):
        import pyarrow as pa
        self.assertEqual(_schema_shape(table.schema), _schema_shape(arrow_schema(pa, kind, joined=joined)))
        self.assertEqual(digest(table.to_pylist()), digest(expected))
        self.assertEqual(table.num_rows, len(expected))

    def exercise_lifecycle(self, kind):
        context = self.context()
        declaration = schema(kind)
        expected_input = [row(kind, ordinal) for ordinal in range(1, 9)]
        with patch("shardloom._input_schema.encode_cell", side_effect=AssertionError("JSON cells")):
            typed = context.from_rows(expected_input, schema=declaration).collect(check=True)
        self.assertEqual(typed.result_schema, tuple(declaration.items()))
        self.assertEqual(digest(typed.python_objects), digest(expected_input))
        self.assert_values(typed.to_arrow(), kind, expected_input)
        empty = context.from_rows([], schema=declaration).collect(check=True)
        self.assertEqual(empty.result_schema, tuple(declaration.items()))
        self.assert_values(empty.to_arrow(), kind, [])
        del typed, empty
        dataframe, sql = self.queries(context, kind, 8)
        expected = joined_rows(kind)
        retained = None
        with tempfile.TemporaryDirectory(prefix="shardloom-wide-lifecycle-") as directory:
            paths = []
            for name, query in (("dataframe", dataframe), ("sql", sql)):
                result = query.collect(check=True)
                self.assert_values(result.to_arrow(), kind, expected, joined=True)
                self.assertEqual(digest(result.python_objects), digest(expected))
                self.assertFalse(result.envelope.field_bool("fallback_attempted"))
                self.assertFalse(result.envelope.field_bool("external_engine_invoked"))
                streamed = []
                with query.iter_batches(batch_rows=2) as batches:
                    for batch in batches:
                        arrow = batch.to_arrow()
                        self.assertLessEqual(arrow.num_rows, 2)
                        streamed.extend(arrow.to_pylist())
                self.assertIsNotNone(batches.report)
                self.assertEqual(digest(streamed), digest(expected))
                with query.iter_batches(batch_rows=2) as early:
                    retained = next(early).to_arrow()
                self.assertIsNone(early.report)
                destination = Path(directory) / (name + ".vortex")
                query.write_vortex(destination, check=True)
                paths.append(str(destination))
            reopened = subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()),
                "--reopen", kind, *paths], env=os.environ.copy(), capture_output=True,
                text=True, timeout=300, check=False)
            self.assertEqual(reopened.returncode, 0, reopened.stderr)
            reports = json.loads(reopened.stdout)
            self.assertEqual(len(reports), 2)
            for report in reports:
                self.assertEqual(report["rows"], len(expected))
                self.assertTrue(report["schema_verified"])
                self.assertTrue(all(value == digest(expected) for value in report["digests"].values()))
        context.close()
        del result, dataframe, sql, query, batch, arrow, early, batches
        self.assert_values(retained, kind, expected[:2], joined=True)

    def test_wide_siblings_complete_lifecycle(self):
        self.exercise_lifecycle("siblings")

    def test_wide_logical_nodes_complete_lifecycle(self):
        self.exercise_lifecycle("nodes")

    def test_wide_metadata_complete_lifecycle(self):
        self.exercise_lifecycle("metadata")

    def test_cold_wide_files_refuse_small_grants_without_panics_and_release_owners(self):
        context = self.context()
        with tempfile.TemporaryDirectory(prefix="shardloom-cold-wide-") as directory:
            for kind in KINDS:
                path = Path(directory) / (kind + ".vortex")
                context.from_rows([row(kind, 1), row(kind, 2)], schema=schema(kind)).write_vortex(path, check=True)
                for grant in (128 << 10, 256 << 10, 512 << 10, 1 << 20, 2 << 20, 4 << 20, 8 << 20, GRANT):
                    with self.subTest(kind=kind, grant=grant):
                        reopened = subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()),
                            "--cold-reopen", kind, str(path), str(grant)], env=os.environ.copy(),
                            capture_output=True, text=True, timeout=120, check=False)
                        self.assertEqual(reopened.returncode, 0, reopened.stderr)
                        self.assertNotIn("panicked at", reopened.stderr)
                        report = json.loads(reopened.stdout)
                        self.assertTrue(report["released"])
                        self.assertIn(report["status"], ("refused", "values_passed"))
                        if grant <= 512 << 10:
                            self.assertEqual(report["status"], "refused")
                        if grant == GRANT:
                            self.assertEqual(report["status"], "values_passed")

    def test_wide_siblings_activate_native_spill_and_clean_owned_runs(self):
        context = self.context()
        dataframe, sql = self.queries(context, "siblings", 257)
        expected = joined_rows("siblings", 257)
        with tempfile.TemporaryDirectory(prefix="shardloom-wide-spill-") as directory:
            workspace = Path(directory) / "spill"
            workspace.mkdir()
            spill = {"workspace": str(workspace), "quota_bytes": 64 << 20, "buffer_bytes": 1 << 20}
            for query in (dataframe, sql):
                result = query.collect(spill=spill, check=True)
                self.assertGreater(result.envelope.field_int("relational_spill_runs_written"), 0)
                self.assertTrue(result.envelope.field_bool("relational_spill_owned_cleanup_completed"))
                self.assertFalse(result.envelope.field_bool("fallback_attempted"))
                self.assertFalse(result.envelope.field_bool("external_engine_invoked"))
                self.assert_values(result.to_arrow(), "siblings", expected, joined=True)
                self.assertEqual(list(workspace.iterdir()), [])

    def test_wide_empty_and_selected_arrow_children_retain_the_last_memory_owner(self):
        import pyarrow as pa
        for kind in KINDS:
            for count in (0, 8):
                with self.subTest(kind=kind, count=count):
                    expected = pa.Table.from_pylist([row(kind, ordinal) for ordinal in range(1, count + 1)],
                                                   schema=arrow_schema(pa, kind))
                    session = self.native.Session(self.native.ExecutionResources(GRANT, 1))
                    self.addCleanup(session.close)
                    source = session.from_arrow(pa.RecordBatchReader.from_batches(expected.schema, expected.to_batches()))
                    plan = session.sql("SELECT * FROM 'memory://wide'", sources=[("memory://wide", source)])
                    result = plan.collect()
                    plan.release_prepared()
                    table = pa.RecordBatchReader.from_stream(result).read_all()
                    self.assert_values(table, kind, expected.to_pylist())
                    # Keep one exported child after both native and Arrow parents go away.
                    self.assertGreater(table.column("payload").num_chunks, 0)
                    child = table.column("payload").chunk(0).field(0)
                    child_values = child.to_pylist()
                    before = session.snapshot()["reserved_bytes"]
                    with self.assertRaisesRegex(RuntimeError, "memory allocation cannot cover retained owners") as denied:
                        plan.collect(resources=self.native.ExecutionResources(before - 1, 1))
                    self.assertFalse(denied.exception.diagnostic["fallback"]["attempted"])
                    self.assertFalse(denied.exception.external_engine_invoked)
                    self.assertEqual(session.snapshot()["reserved_bytes"], before)
                    del source, plan, result, table
                    session.close()
                    self.assertGreater(session.snapshot()["reserved_bytes"], 0)
                    self.assertEqual(child.to_pylist(), child_values)
                    del child
                    gc.collect()
                    self.assertEqual(session.snapshot()["reserved_bytes"], 0)


if __name__ == "__main__":
    if len(sys.argv) == 5 and sys.argv[1] == "--cold-reopen":
        print(json.dumps(_cold_reopen(sys.argv[2], sys.argv[3], int(sys.argv[4])), sort_keys=True))
    elif len(sys.argv) >= 4 and sys.argv[1] == "--reopen":
        print(json.dumps(_reopen(sys.argv[2], sys.argv[3:]), sort_keys=True))
    elif sys.argv[1:] == ["--prior-refusals"]:
        print(json.dumps(_prior_refusals(), sort_keys=True))
    else:
        unittest.main()
