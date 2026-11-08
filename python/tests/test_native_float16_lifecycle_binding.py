"""Complete mixed Float16 values across relational and public lifecycle boundaries."""

from __future__ import annotations

import base64
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
    from .float16_fixture import _fixture, _rows, _digest, _independent_schema_equal, _independent_mask
else:
    from test_native_binding import _load_native
    from float16_fixture import _fixture, _rows, _digest, _independent_schema_equal, _independent_mask


def _schema_bytes(schema):
    return base64.b64encode(schema.serialize().to_pybytes()).decode("ascii")


def _reopen_job(paths, expected_schema):
    _load_native()
    import pyarrow as pa
    import pyarrow.parquet as pq
    import shardloom as sl

    expected = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(expected_schema, validate=True)))
    reports = []
    with sl.context(transport="native", memory_bytes=128 << 20, max_parallelism=1,
                    float_policy="extended") as context:
        for raw in paths:
            path = Path(raw)
            frame = context.read_vortex(path)
            result = frame.collect(check=True)
            table = result.to_arrow()
            arrow_path, parquet_path = path.with_suffix(".arrow"), path.with_suffix(".parquet")
            frame.write_arrow_ipc(arrow_path, check=True)
            frame.write_parquet(parquet_path, check=True)
            with pa.ipc.open_file(arrow_path) as reader:
                arrow = reader.read_all()
            parquet_native = context.read_parquet(parquet_path).collect(check=True).to_arrow()

            independent_full = None
            limitation = None
            try:
                independent_full = pq.read_table(parquet_path)
            except pa.ArrowInvalid as error:
                if "Expected all lists to be of size=" not in str(error):
                    raise
                limitation = str(error)

            # Independently exercise present parents when Arrow cannot decode the
            # complete fixed-list/null-parent Parquet representation.
            present = table.filter(pa.array(_independent_mask(table), pa.bool_()))
            assert present.num_rows > 0, "independent subset must contain real rows"
            present_path = path.with_suffix(".present.parquet")
            context.from_arrow_table(present).write_parquet(present_path, check=True)
            independent_default = pq.read_table(present_path)
            embedded = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(
                pq.read_metadata(present_path).metadata[b"ARROW:schema"], validate=True)))
            independent_present = pq.read_table(present_path, schema=embedded)

            schema_matches = {
                "native_expected": _independent_schema_equal(table.schema, expected),
                "arrow_expected": _independent_schema_equal(arrow.schema, expected),
                "parquet_native_expected": _independent_schema_equal(parquet_native.schema, expected),
                "parquet_independent_present_expected": _independent_schema_equal(
                    independent_present.schema, present.schema),
            }
            reports.append({
                "rows": table.num_rows,
                "digest": _digest(_rows(table)),
                "arrow_digest": _digest(_rows(arrow)),
                "parquet_native_digest": _digest(_rows(parquet_native)),
                "parquet_independent_full_digest": None if independent_full is None else
                    _digest(_rows(independent_full)),
                "parquet_independent_present_digest": _digest(_rows(independent_present)),
                "parquet_default_present_digest": _digest(_rows(independent_default)),
                "parquet_reader_limitation": limitation,
                "schema_matches": schema_matches,
                "schema": _schema_bytes(table.schema),
                "expected_schema": expected_schema,
            })
    return reports


class NativeFloat16LifecycleBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()

    def context(self):
        import shardloom as sl
        context = sl.context(transport="native", memory_bytes=128 << 20,
                             max_parallelism=1, float_policy="extended")
        self.addCleanup(context.close)
        return context

    def test_mixed_float16_join_spill_stream_retention_and_reopen(self):
        import pyarrow as pa

        context = self.context()
        table = _fixture(4101).slice(1, 4097)
        left = context.from_arrow_table(table)
        reports = [{"id": row, "tag": tag} for row in range(1, 4098)
                   for tag in ([1, 2] if row % 5 == 0 else [1] if row % 3 == 0 else [])]
        right = context.from_arrow_table(pa.Table.from_pylist(reports))
        columns = [f"f.{name} AS {name}" for name in table.column_names]
        selection = [*columns, "d.tag AS tag"]
        dataframe = left.where("id >= 3").join(right, on="id", how="left").select(*selection).sort(
            "id", "tag", descending=True, nulls="last")
        sql = context.sql("SELECT " + ", ".join(selection) +
            " FROM mixed AS f LEFT JOIN reports AS d ON f.id = d.id WHERE f.id >= 3 "
            "ORDER BY f.id DESC, d.tag DESC NULLS LAST", relations={"mixed": left, "reports": right})
        expected = [{**row, "tag": tag} for row in reversed(_rows(table))
                    if row["id"] >= 3
                    for tag in ([2, 1] if row["id"] % 5 == 0 else
                                [1] if row["id"] % 3 == 0 else [None])]
        expected_schema = pa.schema([*table.schema, pa.field("tag", pa.int64())])
        independent_ids = {row["id"] for row, include in zip(_rows(table), _independent_mask(table)) if include}

        with tempfile.TemporaryDirectory(prefix="shardloom-float16-lifecycle-") as directory:
            root = Path(directory)
            workspace = root / "spill"
            workspace.mkdir()
            spill = {"workspace": str(workspace), "quota_bytes": 64 << 20, "buffer_bytes": 1 << 20}
            for query in (dataframe, sql):
                result = query.collect(spill=spill, check=True)
                actual = result.to_arrow()
                self.assertGreater(result.envelope.field_int("relational_spill_runs_written"), 0)
                self.assertTrue(result.envelope.field_bool("relational_spill_owned_cleanup_completed"))
                self.assertFalse(result.envelope.field_bool("fallback_attempted"))
                self.assertFalse(result.envelope.field_bool("external_engine_invoked"))
                self.assertEqual(_digest(_rows(actual)), _digest(expected))
                self.assertEqual(actual.num_rows, len(expected))
                self.assertTrue(actual.schema.equals(expected_schema, check_metadata=True),
                                (actual.schema, expected_schema))
                self.assertEqual(list(workspace.iterdir()), [])

            streamed = []
            for batch in dataframe.iter_batches(batch_rows=257, spill=spill):
                self.assertLessEqual(batch.to_arrow().num_rows, 257)
                streamed.extend(_rows(batch.to_arrow()))
            self.assertEqual(_digest(streamed), _digest(expected))
            with dataframe.iter_batches(batch_rows=257, spill=spill) as early:
                retained = next(early)
            retained_table = retained.to_arrow()
            self.assertIsNone(early.report)
            self.assertEqual(_digest(_rows(retained_table)), _digest(expected[:257]))
            self.assertEqual(list(workspace.iterdir()), [])

            output = root / "selected.vortex"
            dataframe.write_vortex(output, spill=spill, check=True)
            self.assertEqual(list(workspace.iterdir()), [])
            context.close()
            del left, right, dataframe, sql
            self.assertEqual(_digest(_rows(retained.to_arrow())), _digest(expected[:257]))
            completed = subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()),
                "--reopen", str(output), _schema_bytes(expected_schema)], env=os.environ.copy(),
                text=True, capture_output=True, timeout=120, check=False)
            self.assertEqual(completed.returncode, 0, completed.stderr)
            report = json.loads(completed.stdout)[0]
            for key in ("digest", "arrow_digest", "parquet_native_digest"):
                self.assertEqual(report[key], _digest(expected))
            self.assertEqual(report["rows"], len(expected))
            self.assertTrue(all(report["schema_matches"].values()), report)
            present_digest = _digest([row for row in expected if row["id"] in independent_ids])
            self.assertEqual(report["parquet_independent_present_digest"], present_digest)
            self.assertEqual(report["parquet_default_present_digest"], present_digest)
            if report["parquet_independent_full_digest"] is not None:
                self.assertEqual(report["parquet_independent_full_digest"], _digest(expected))
            else:
                self.assertIn("Expected all lists to be of size=", report["parquet_reader_limitation"])

    def test_mixed_float16_write_through_all_five_front_doors_and_reopen(self):
        import shardloom as sl

        context = self.context()
        source_table = _fixture(9).slice(1, 7)
        source_frame = context.from_arrow_table(source_table)
        with tempfile.TemporaryDirectory(prefix="shardloom-float16-interfaces-") as directory:
            root = Path(directory)
            source = root / "input.vortex"
            source_frame.write_vortex(source, check=True)
            statement = ("SELECT * FROM '" + str(source).replace("'", "''") +
                         "' WHERE id <> 2 ORDER BY id DESC")
            native = self.native.Session(self.native.ExecutionResources(64 << 20, 1),
                                         float_policy="extended")
            cli = sl.ShardLoomClient(binary=os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY"),
                transport="subprocess", use_persistent_worker=False, memory_bytes=64 << 20,
                max_parallelism=1, float_policy="extended")
            files = [(str(source), "vortex", None)]
            expression = self.native.Expression
            relation = self.native.Relation.scan(str(source)).filter(
                expression.column("id") != expression.literal(2)).sort([("id", True, None)])
            arguments = ["sql", "--sql", statement, "--request", "collect", "--bounded", "true",
                "--input", str(source), "--input-format", "vortex",
                "--memory-bytes", str(64 << 20), "--max-parallelism", "1"]
            outputs = []
            try:
                for surface in ("dataframe", "sql", "typed_plan", "embedded_workflow", "cli_sql"):
                    path = root / (surface + ".vortex")
                    with self.subTest(surface=surface):
                        if surface == "dataframe":
                            report = context.read_vortex(source).where("id <> 2").sort(
                                "id", descending=True).write_vortex(path, check=True)
                            fields = report.envelope.field
                        elif surface == "cli_sql":
                            report = sl.sql(statement, client=cli, input=str(source),
                                input_format="vortex").write_vortex(path, check=True)
                            fields = report.envelope.field
                        else:
                            plan = (native.sql(statement, files=files) if surface == "sql" else
                                    native.plan(relation, files=files) if surface == "typed_plan" else
                                    native.workflow(arguments))
                            with patch("subprocess.Popen", side_effect=AssertionError("external execution")):
                                report = plan.write(path, "write_vortex")
                            fields = report.get
                        self.assertEqual(fields("fallback_attempted"), "false")
                        self.assertEqual(fields("external_engine_invoked"), "false")
                        outputs.append(path)
            finally:
                native.close()
                cli.close()
                context.close()

            expected = [row for row in reversed(_rows(source_table)) if row["id"] != 2]
            independent_ids = {row["id"] for row, include in zip(_rows(source_table), _independent_mask(source_table)) if include}
            expected_schema = _schema_bytes(source_table.schema)
            del source_frame
            source.unlink()
            completed = subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()),
                "--reopen", *map(str, outputs), expected_schema], env=os.environ.copy(),
                text=True, capture_output=True, timeout=120, check=False)
            self.assertEqual(completed.returncode, 0, completed.stderr)
            for report in json.loads(completed.stdout):
                for key in ("digest", "arrow_digest", "parquet_native_digest"):
                    self.assertEqual(report[key], _digest(expected))
                self.assertEqual(report["rows"], len(expected))
                self.assertTrue(all(report["schema_matches"].values()), report)
                import pyarrow as pa
                actual_schema = pa.ipc.read_schema(pa.BufferReader(
                    base64.b64decode(report["schema"], validate=True)))
                expected_arrow_schema = pa.ipc.read_schema(pa.BufferReader(
                    base64.b64decode(expected_schema, validate=True)))
                self.assertTrue(actual_schema.equals(expected_arrow_schema, check_metadata=True))
                present_digest = _digest([row for row in expected if row["id"] in independent_ids])
                self.assertEqual(report["parquet_independent_present_digest"], present_digest)
                self.assertEqual(report["parquet_default_present_digest"], present_digest)
                if report["parquet_independent_full_digest"] is None:
                    self.assertIn("Expected all lists to be of size=", report["parquet_reader_limitation"])
                else:
                    self.assertEqual(report["parquet_independent_full_digest"], _digest(expected))


if __name__ == "__main__":
    if len(sys.argv) >= 4 and sys.argv[1] == "--reopen":
        print(json.dumps(_reopen_job(sys.argv[2:-1], sys.argv[-1]), sort_keys=True))
    else:
        unittest.main()
