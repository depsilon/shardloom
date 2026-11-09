"""Exact filtered retrieval and persisted vector computations across public routes."""

import gc
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
    from .test_native_vector_representations_binding import _schema_shape, _rows, _digest
    from .vector_expression_fixture import assert_numeric, corpus, dense_array, floating
    from .vector_fixture import tagged
else:
    from test_native_binding import _load_native
    from test_native_vector_representations_binding import _schema_shape, _rows, _digest
    from vector_expression_fixture import assert_numeric, corpus, dense_array, floating
    from vector_fixture import tagged


def expressions(f):
    return [f.vector_dimension("v").alias("dimension"), f.tensor_shape("tensor").alias("shape"),
            f.vector_values("v").alias("values"), f.vector_element("v", -1).alias("element"),
            f.vector_slice("v", 0, 1).alias("slice"), f.vector_dot("v", "w").alias("dot"),
            f.vector_l2_distance("v", "w").alias("distance"),
            f.vector_cosine_similarity("v", "w").alias("cosine"),
            f.vector_norm("v").alias("norm"), f.vector_normalize("v").alias("normalized")]


def expected_digest(expected, schema):
    """Encode independent values at their declared widths for raw-bit checks."""
    import pyarrow as pa
    return _digest(_rows(pa.Table.from_pylist(expected, schema=schema)))


def _reopen_job(paths):
    _load_native()
    import shardloom as sl
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    reports = []
    with sl.context(transport="native", memory_bytes=128 << 20, max_parallelism=1) as context:
        for raw in paths:
            path = Path(raw)
            frame = context.read_vortex(path)
            native = frame.collect(check=True).to_arrow()
            arrow_path, parquet_path = path.with_suffix(".arrow"), path.with_suffix(".parquet")
            frame.write_arrow_ipc(arrow_path, check=True)
            frame.write_parquet(parquet_path, check=True)
            with pa.ipc.open_file(arrow_path) as reader:
                arrow = reader.read_all()
            def read_parquet(path):
                schema = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(
                    pq.read_metadata(path).metadata[b"ARROW:schema"], validate=True)))
                return pq.read_table(path, schema=schema)
            tables = {"native": native, "arrow": arrow,
                      "native_parquet": context.read_parquet(parquet_path).collect(check=True).to_arrow()}
            limitation = None
            try:
                tables["parquet"] = read_parquet(parquet_path)
            except pa.ArrowInvalid as error:
                # The pinned independent reader also fails on its own nullable
                # fixed-list files; retain the exact observed limitation.
                if "Expected all lists to be of size=" not in str(error):
                    raise
                limitation = str(error)
            fixed = [field.name for field in native.schema if pa.types.is_fixed_size_list(field.type)]
            present = native
            if fixed:
                mask = pa.array([True] * native.num_rows)
                for name in fixed:
                    mask = pc.and_(mask, pc.is_valid(native.column(name)))
                present = native.filter(mask)
            present_path = path.with_suffix(".present.parquet")
            context.from_arrow_table(present).write_parquet(present_path, check=True)
            independent = read_parquet(present_path)
            reports.append({"rows": native.num_rows,
                "digests": {kind: _digest(_rows(table)) for kind, table in tables.items()},
                "schemas_equal": {kind: _schema_shape(table.schema) == _schema_shape(native.schema)
                                  for kind, table in tables.items()},
                "independent_present_rows": independent.num_rows,
                "independent_present_equal": _digest(_rows(independent)) == _digest(_rows(present)),
                "independent_present_schema_equal": _schema_shape(independent.schema) == _schema_shape(native.schema),
                "independent_full_parquet_limitation": limitation})
    return reports


class NativeVectorExpressionWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()

    def context(self, policy="strict", memory_bytes=128 << 20):
        import shardloom as sl
        context = sl.context(transport="native", memory_bytes=memory_bytes,
                             max_parallelism=1, float_policy=policy)
        self.addCleanup(context.close)
        return context

    def test_vector_exact_filtered_retrieval_1536_components_all_widths_and_shapes(self):
        import pyarrow as pa
        from shardloom import functions as f
        reference = corpus("retrieval_reference_r2")
        self.assertEqual(reference["dimension"], 1536)
        query_values = [floating(64, value) for value in reference["query_bits"]]
        self.assertEqual(len(query_values), 1536)
        literal = f.array(query_values)
        with tempfile.TemporaryDirectory(prefix="shardloom-vector-retrieval-") as directory:
            root = Path(directory)
            writer = self.context("extended")
            for group in reference["cases"]:
                width, rows = group["component_width"], group["rows"]
                for route in reference["representation_routes"]:
                    values = dense_array(pa, width, [row["vector_bits"] for row in rows],
                                         None if route == "plain_variable" else 1536)
                    field = (tagged(pa, "v", values.type, "shardloom.embedding.v1", {"version": 1, "space_id": "retrieval Ω"})
                             if route == "embedding_with_explicit_vector_values" else pa.field("v", values.type))
                    table = pa.Table.from_arrays([pa.array([r["id"] for r in rows], pa.int64()),
                        pa.array([r["eligible"] for r in rows]), values],
                        schema=pa.schema([pa.field("id", pa.int64()), pa.field("eligible", pa.bool_()), field]))
                    path = root / f"{width}-{route}.vortex"
                    writer.from_arrow_table(table).write_vortex(path, check=True)
                    for policy in ("strict", "extended"):
                        # Three 1536-component literal expressions and the
                        # DataFrame ordering declaration retain conservative
                        # per-node preparation credits. Keep those charges.
                        context = self.context(policy, memory_bytes=256 << 20)
                        source = context.read_vortex(path)
                        vector = f.vector_values("v") if "embedding" in route else f.col("v")
                        projections = [f.vector_dot(vector, literal).alias("dot"),
                            f.vector_norm(vector).alias("norm_left"),
                            f.vector_l2_distance(vector, literal).alias("l2_distance"),
                            f.vector_cosine_similarity(vector, literal).alias("cosine"),
                            f.vector_normalize(vector).alias("normalized_left")]
                        names = ["id", "dot", "norm_left", "l2_distance", "cosine", "normalized_left"]
                        # The public DataFrame sort has one direction. Negate
                        # the finite cosine key to express score DESC, id ASC.
                        frame = source.where("eligible").select("id", *projections)
                        frame = frame.with_columns(_rank=f.lit(0.0) - f.col("cosine")).sort("_rank", "id", nulls="last").select(*names)
                        sql = context.sql("SELECT id," + ",".join(map(str, projections)) +
                            " FROM input WHERE eligible ORDER BY cosine DESC NULLS LAST,id ASC",
                            relations={"input": source})
                        for frontdoor, query in [("dataframe", frame), ("sql", sql)]:
                            with self.subTest(width=width, route=route, policy=policy, frontdoor=frontdoor):
                                result = query.collect(check=True, reuse=False)
                                actual = result.to_rows()
                                self.assertEqual([r["id"] for r in actual], group["expected_order"])
                                expected_by_id = {r["id"]: r for r in rows}
                                for row in actual:
                                    original = expected_by_id[row["id"]]
                                    case = {"id": str(row["id"]), "left_width": width, "right_width": 64,
                                            "left": original["vector_bits"], "right": reference["query_bits"], "expected": original["expected"]}
                                    for name in names[1:]:
                                        assert_numeric(self, case, name, row[name])
                                dtype = dict(result.result_schema)["normalized_left"]
                                self.assertEqual(dtype.name, "list" if route == "plain_variable" else "fixed_size_list")
                                self.assertEqual(dtype.parameters[0].name, "float64")
                                self.assertFalse(dtype.parameters[0].nullable)
                                self.assertEqual(result.envelope.field("fallback_attempted"), "false")
                                self.assertEqual(result.envelope.field("external_engine_invoked"), "false")
                                del result
                                self.assertEqual([r["id"] for r in query.limit(3).collect(check=True, reuse=False).to_rows()], group["expected_top_three"])
                        context.close()

    def test_vector_all_ten_functions_write_five_routes_and_reopen_without_source(self):
        import shardloom as sl
        from shardloom import functions as f, types as t
        context = self.context()
        source_frame = context.from_rows([
            {"id": 1, "v": [3.0, 4.0], "w": [3.0, 4.0], "tensor": list(range(6))},
            {"id": 2, "v": [0.0, 0.0], "w": [0.0, 0.0], "tensor": list(range(6))},
            {"id": 3, "v": None, "w": None, "tensor": None}],
            schema={"id": t.int64(), "v": t.fixed_size_list(t.float32(), 2), "w": t.list_(t.float64()),
                    "tensor": t.fixed_shape_tensor(t.int16(), [2, 3], permutation=[1, 0])})
        expected = [{"id": 3, **{name: None for name in ("dimension", "shape", "values", "element", "slice", "dot", "distance", "cosine", "norm", "normalized")}},
                    {"id": 2, "dimension": 2, "shape": [3, 2], "values": [0.0, 0.0], "element": 0.0, "slice": [0.0], "dot": 0.0, "distance": 0.0, "cosine": None, "norm": 0.0, "normalized": None},
                    {"id": 1, "dimension": 2, "shape": [3, 2], "values": [3.0, 4.0], "element": 4.0, "slice": [3.0], "dot": 25.0, "distance": 0.0, "cosine": 1.0, "norm": 5.0, "normalized": [0.6, 0.8]}]
        with tempfile.TemporaryDirectory(prefix="shardloom-vector-expression-writes-") as directory:
            root, outputs = Path(directory), []
            source = root / "input.vortex"
            source_frame.write_vortex(source, check=True)
            projection = expressions(f)
            schema = context.read_vortex(source).select("id", *projection).collect(check=True).to_arrow().schema
            expected_bits = expected_digest(expected, schema)
            statement = "SELECT id," + ",".join(map(str, projection)) + " FROM '" + str(source).replace("'", "''") + "' ORDER BY id DESC"
            native = self.native.Session(self.native.ExecutionResources(128 << 20, 1))
            cli = sl.ShardLoomClient(binary=os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY"), transport="subprocess",
                use_persistent_worker=False, memory_bytes=128 << 20, max_parallelism=1)
            e = self.native.Expression
            relation = self.native.Relation.scan(str(source)).project([
                ("id", e.column("id")), ("dimension", e.call("vector_dimension", [e.column("v")])),
                ("shape", e.call("tensor_shape", [e.column("tensor")])),
                ("values", e.call("vector_values", [e.column("v")])),
                ("element", e.call("vector_element", [e.column("v"), e.literal(-1)])),
                ("slice", e.call("vector_slice", [e.column("v"), e.literal(0), e.literal(1)])),
                ("dot", e.call("vector_dot", [e.column("v"), e.column("w")])),
                ("distance", e.call("vector_l2_distance", [e.column("v"), e.column("w")])),
                ("cosine", e.call("vector_cosine_similarity", [e.column("v"), e.column("w")])),
                ("norm", e.call("vector_norm", [e.column("v")])),
                ("normalized", e.call("vector_normalize", [e.column("v")]))]).sort([("id", True, None)])
            files = [(str(source), "vortex", None)]
            arguments = ["sql", "--sql", statement, "--request", "collect", "--bounded", "true",
                         "--input", str(source), "--input-format", "vortex", "--memory-bytes", str(128 << 20), "--max-parallelism", "1"]
            try:
                for route in ("dataframe", "sql", "typed_plan", "embedded_workflow", "cli_sql"):
                    path = root / (route + ".vortex")
                    if route == "dataframe":
                        report = context.read_vortex(source).select("id", *projection).sort("id", descending=True).write_vortex(path, check=True)
                        field = report.envelope.field
                    elif route == "cli_sql":
                        report = sl.sql(statement, client=cli, input=str(source), input_format="vortex").write_vortex(path, check=True)
                        field = report.envelope.field
                    else:
                        plan = (native.sql(statement, files=files) if route == "sql" else
                                native.plan(relation, files=files) if route == "typed_plan" else native.workflow(arguments))
                        with patch("subprocess.Popen", side_effect=AssertionError("external execution")):
                            report = plan.write(path, "write_vortex")
                        field = report.get
                    self.assertEqual(field("fallback_attempted"), "false", route)
                    self.assertEqual(field("external_engine_invoked"), "false", route)
                    outputs.append(path)
            finally:
                native.close()
                cli.close()
                context.close()
            source.unlink()
            reports = self.reopen(outputs)
            self.assertEqual(len(reports), 5)
            for report in reports:
                self.assertEqual(report["rows"], len(expected))
                for kind, digest in report["digests"].items():
                    self.assertEqual(digest, expected_bits, kind)
                self.assertTrue(all(report["schemas_equal"].values()), report)
                self.assertTrue(report["independent_present_equal"], report)
                self.assertTrue(report["independent_present_schema_equal"], report)
                self.assertEqual(report["independent_present_rows"], 1)
                if "parquet" not in report["digests"]:
                    self.assertIn("Expected all lists to be of size=", report["independent_full_parquet_limitation"])

    def test_vector_computed_lists_spill_stream_close_and_reopen(self):
        from shardloom import functions as f, types as t
        context = self.context()
        count = 24_009
        source = context.from_rows([{"id": index, "v": [3.0, 4.0]} for index in range(count)],
                                   schema={"id": t.int64(), "v": t.list_(t.float32())})
        projection = [f.vector_norm("v").alias("norm"), f.vector_normalize("v").alias("unit"),
                      f.vector_slice("v", 1).alias("tail")]
        frame = source.select("id", *projection).sort("id", descending=True)
        sql = context.sql("SELECT id," + ",".join(map(str, projection)) + " FROM input ORDER BY id DESC", relations={"input": source})
        expected = [{"id": index, "norm": 5.0, "unit": [0.6, 0.8], "tail": [4.0]} for index in reversed(range(count))]
        with tempfile.TemporaryDirectory(prefix="shardloom-vector-expression-spill-") as directory:
            root = Path(directory)
            workspace = root / "spill"
            workspace.mkdir()
            spill = {"workspace": str(workspace), "quota_bytes": 64 << 20, "buffer_bytes": 1 << 20}
            for query in (frame, sql):
                result = query.collect(spill=spill, check=True)
                self.assertGreater(result.envelope.field_int("relational_spill_runs_written"), 0)
                self.assertTrue(result.envelope.field_bool("relational_spill_owned_cleanup_completed"))
                expected_bits = expected_digest(expected, result.to_arrow().schema)
                self.assertEqual(_digest(_rows(result.to_arrow())), expected_bits)
                self.assertEqual(list(workspace.iterdir()), [])
            actual = []
            for batch in frame.iter_batches(batch_rows=257, spill=spill):
                self.assertLessEqual(batch.to_arrow().num_rows, 257)
                actual.extend(_rows(batch.to_arrow()))
            self.assertEqual(_digest(actual), expected_bits)
            with frame.iter_batches(batch_rows=257, spill=spill) as early:
                kept = next(early)
            self.assertIsNone(early.report)
            self.assertEqual(list(workspace.iterdir()), [])
            path = root / "computed.vortex"
            frame.write_vortex(path, spill=spill, check=True)
            for report in self.reopen([path]):
                for kind, digest in report["digests"].items():
                    self.assertEqual(digest, expected_bits, kind)
                self.assertTrue(all(report["schemas_equal"].values()))
                self.assertTrue(report["independent_present_equal"])
                self.assertTrue(report["independent_present_schema_equal"])
                self.assertEqual(report["independent_present_rows"], count)
                self.assertIsNone(report["independent_full_parquet_limitation"])
            context.close()
            del source, frame, sql
            self.assertEqual(_digest(_rows(kept.to_arrow())), expected_digest(expected[:257], kept.to_arrow().schema))

    def test_vector_computed_result_retains_credit_after_denial_cancel_and_producer_drop(self):
        import pyarrow as pa
        from shardloom._native_backend import NativeResultBatch
        session = self.native.Session(self.native.ExecutionResources(64 << 20, 1))
        self.addCleanup(session.close)
        table = pa.table({"id": range(129), "v": [[3.0, 4.0]] * 129})
        source = session.from_arrow(pa.RecordBatchReader.from_batches(table.schema, table.to_batches(max_chunksize=17)))
        plan = session.sql("SELECT id,VECTOR_NORMALIZE(v) AS unit,VECTOR_NORM(v) AS norm FROM 'memory://vectors'", sources=[("memory://vectors", source)])
        result = plan.collect()
        plan.release_prepared()
        reserved = session.snapshot()["reserved_bytes"]
        with self.assertRaisesRegex(Exception, "memory"):
            plan.collect(resources=self.native.ExecutionResources(reserved - 1, 1))
        self.assertEqual(session.snapshot()["reserved_bytes"], reserved)
        retry = plan.collect()
        del retry
        stream = plan.iter_batches()
        first = next(stream)
        session.cancel()
        with self.assertRaisesRegex(Exception, "cancel"):
            next(stream)
        stream.close()
        self.assertIsNone(stream.report)
        table = NativeResultBatch(first, 0).to_arrow()
        expected = [{"id": index, "unit": [0.6, 0.8], "norm": 5.0} for index in range(table.num_rows)]
        self.assertEqual(_digest(_rows(table)), expected_digest(expected, table.schema))
        child = table.column("unit").chunk(0).values
        buffer = child.buffers()[1]
        del source, plan, result, stream, first, table, child
        session.close()
        gc.collect()
        self.assertGreater(session.snapshot()["reserved_bytes"], 0)
        self.assertGreater(buffer.size, 0)
        del buffer
        gc.collect()
        self.assertEqual(session.snapshot()["reserved_bytes"], 0)

    def reopen(self, paths):
        result = subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()), "--reopen", *map(str, paths)],
                                env=os.environ.copy(), text=True, capture_output=True, timeout=180, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)


if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == "--reopen":
        print(json.dumps(_reopen_job(sys.argv[2:]), sort_keys=True))
    else:
        unittest.main()
