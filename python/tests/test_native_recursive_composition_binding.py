# SPDX-License-Identifier: Apache-2.0
"""Complete recursive/materialized workflows through the shared native owners."""

from contextlib import nullcontext
from decimal import Decimal
import gc
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

if __package__:
    from .test_native_binding import _load_native
else:
    from test_native_binding import _load_native


def describe(table):
    """Complete typed values, with exact physical floating and temporal values."""
    import pyarrow as pa

    def value(array, index):
        if not array[index].is_valid:
            return None
        dtype = array.type
        if pa.types.is_dictionary(dtype):
            return value(array.dictionary, array.indices[index].as_py())
        if pa.types.is_floating(dtype):
            width = dtype.bit_width // 8
            offset = (array.offset + index) * width
            return {"bits": bytes(memoryview(array.buffers()[1])[offset:offset + width]).hex()}
        if pa.types.is_struct(dtype):
            return {field.name: value(array.field(i), index) for i, field in enumerate(dtype)}
        if pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
            start, end = array.offsets[index].as_py(), array.offsets[index + 1].as_py()
            return [value(array.values, i) for i in range(start, end)]
        if pa.types.is_decimal(dtype):
            return {"decimal": str(array[index].as_py())}
        if pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype):
            return {"bytes": array[index].as_py().hex()}
        if pa.types.is_temporal(dtype):
            target = pa.int32() if pa.types.is_date32(dtype) or pa.types.is_time32(dtype) else pa.int64()
            return {"ticks": array.cast(target)[index].as_py()}
        return array[index].as_py()

    return [{"name": field.name, "type": str(field.type), "nullable": field.nullable,
             "values": [value(array, i) for array in chunked.chunks for i in range(len(array))]}
            for field, chunked in zip(table.schema, table.columns)]


class NativeRecursiveCompositionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()
        import pyarrow as pa
        import shardloom as sl
        cls.pa, cls.sl = pa, sl

    def setUp(self):
        self.real_popen = subprocess.Popen
        self.process = patch("subprocess.Popen", side_effect=AssertionError("external execution"))
        self.process.start()
        self.addCleanup(self.process.stop)
        self.directory = tempfile.TemporaryDirectory(prefix="shardloom-recursive-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.sequence = 0
        self.policy = "strict"
        self.session = self.native.Session(self.native.ExecutionResources(128 << 20, 2))
        self.addCleanup(self.session.close)
        self.context = self.sl.context(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(self.context.close)

    def equal(self, actual, expected):
        self.assertEqual(actual.schema, expected.schema)
        self.assertEqual(describe(actual), describe(expected))

    def check_fields(self, get, checks):
        self.assertEqual(get("fallback_attempted"), "false")
        self.assertEqual(get("external_engine_invoked"), "false")
        self.assertEqual(get("float_policy"), self.policy)
        self.assertEqual(get("recursion_limit"), "1000")
        proof = get("local_primitive_native_io_pushdown_proof_basis")
        self.assertIsInstance(proof, str)
        for name, value in checks.items():
            self.assertEqual(get(name), str(value), name)
            self.assertIn(f"{name}={value}", proof)

    def five(self, frame, relation, statement, expected, checks):
        result = frame.collect(check=True)
        self.check_fields(result.envelope.field, checks)
        tables = [("dataframe", result.to_arrow())]
        for label, plan in (("typed", self.session.plan(relation)),
                            ("sql", self.session.sql(statement)),
                            ("workflow", self.session.workflow([
                                "sql", "--sql", statement, "--request", "collect", "--bounded", "true",
                                "--memory-bytes", str(128 << 20), "--max-parallelism", "2"]))):
            result = plan.collect()
            self.check_fields(result.report.get, checks)
            tables.append((label, self.pa.RecordBatchReader.from_stream(result).read_all()))
        with patch("subprocess.Popen", self.real_popen):
            cli = self.sl.ShardLoomClient(binary=os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY"),
                transport="subprocess", use_persistent_worker=False, memory_bytes=128 << 20,
                max_parallelism=2, float_policy=self.policy, timeout=30)
            try:
                self.sequence += 1
                output = self.root / f"cli-{self.sequence}.arrow"
                result = self.sl.sql(statement, client=cli).write(output, output_format="arrow-ipc", check=True)
                self.check_fields(result.envelope.field, checks)
                tables.append(("cli", self.pa.ipc.open_file(str(output)).read_all()))
            finally:
                cli.close()
        for label, table in tables:
            with self.subTest(interface=label):
                self.equal(table, expected)
        return tables

    def write_input(self, table, extension, name="input"):
        path = self.root / f"{name}.{extension}"
        if extension == "parquet":
            import pyarrow.parquet as parquet
            parquet.write_table(table, path, row_group_size=3, use_dictionary=True)
        elif extension == "vortex":
            source = self.session.from_arrow(table)
            self.session.sql("SELECT * FROM 'memory://fixture'", sources=[("memory://fixture", source)]).write(path, "write_vortex")
        else:
            with self.pa.OSFile(str(path), "wb") as stream, self.pa.ipc.new_file(stream, table.schema) as writer:
                writer.write_table(table, max_chunksize=3)
        return path

    def sequence_surfaces(self, prefix, values, checks):
        base = prefix + " SELECT n FROM numbers"
        frame = self.context.sql(base).select("n", (self.sl.col("n") + 1).alias("next")).sort("n", nulls="last")
        expr = self.native.Expression
        relation = self.session.sql(base).as_relation().project([
            ("n", expr.column("n")), ("next", expr.column("n") + expr.literal(1))
        ]).sort([("n", False, "last")])
        statement = prefix + " SELECT n,n+1 AS next FROM numbers ORDER BY n NULLS LAST"
        schema = self.pa.schema([self.pa.field("n", self.pa.int64()), self.pa.field("next", self.pa.int64())])
        expected = self.pa.Table.from_pylist([dict(n=n, next=None if n is None else n+1) for n in values], schema=schema)
        return self.five(frame, relation, statement, expected, checks)

    def test_finite_recursive_frontiers_duplicates_nulls_and_empty_anchor_match_five_surfaces(self):
        cases = [
            ("SELECT 0 AS n UNION ALL SELECT n+1 FROM numbers WHERE n<37", list(range(38)), 38),
            ("SELECT column_1 AS n FROM (VALUES (0),(0),(NULL)) AS seed UNION ALL SELECT n+1 FROM numbers WHERE n<2",
             [0, 0, 1, 1, 2, 2, None], 3),
            ("SELECT column_1 AS n FROM (VALUES (1),(1),(NULL)) AS seed UNION SELECT n FROM numbers", [1, None], 1),
            ("SELECT value AS n FROM range(0,0) UNION ALL SELECT n+1 FROM numbers", [], 0),
            ("SELECT 1 AS n UNION ALL SELECT n+1 FROM numbers WHERE n<1", [1], 1),
        ]
        for definition, values, iterations in cases:
            with self.subTest(definition=definition):
                self.sequence_surfaces(f"WITH RECURSIVE numbers(n) AS ({definition})", values, {
                    "relational_recursive_relations": 1, "relational_recursive_iterations": iterations,
                    "relational_recursive_output_rows": len(values), "relational_materialized_relations": 0,
                })

    def test_branching_cycle_uses_materialized_edges_across_native_and_compatibility_inputs(self):
        edges = [(0, 1), (0, 2), (1, 3), (2, 3), (3, 0), (3, None), (None, 7)]
        schema = self.pa.schema([self.pa.field("src", self.pa.int64()), self.pa.field("dst", self.pa.int64())])
        table = self.pa.Table.from_pylist([dict(src=a, dst=b) for a, b in edges], schema=schema)
        # Finite reachability is computed independently using ordinary Python values.
        visited, pending, iterations = {0}, [0], 0
        while pending:
            candidates = {b for a, b in edges if a is not None and a in pending}
            pending = sorted(candidates - visited, key=lambda n: (n is None, n or 0))
            visited.update(pending)
            iterations += 1
        expected = sorted(visited, key=lambda n: (n is None, n or 0))
        for extension in ("arrow", "parquet", "vortex"):
            with self.subTest(format=extension):
                path = self.write_input(table, extension)
                prefix = (f"WITH RECURSIVE edges AS MATERIALIZED (SELECT src,dst FROM '{path}'), "
                          "numbers(n) AS (SELECT 0 AS n UNION SELECT e.dst FROM numbers AS r JOIN edges AS e ON r.n=e.src)")
                self.sequence_surfaces(prefix, expected, {
                    "relational_recursive_relations": 1, "relational_recursive_iterations": iterations,
                    "relational_recursive_output_rows": len(expected), "relational_materialized_relations": 1,
                    "relational_materialized_rows": len(edges), "relational_materialized_reuses": iterations-1,
                })

    def test_materialized_rich_values_keep_domains_and_survive_independent_reopen(self):
        pa = self.pa
        payload = pa.struct([pa.field("label", pa.string()), pa.field("items", pa.list_(pa.int64())),
                             pa.field("amount", pa.decimal128(16, 3))])
        schema = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("payload", payload),
                            pa.field("bytes", pa.binary()), pa.field("instant", pa.timestamp("ns", "America/New_York"))])
        source = pa.Table.from_arrays([
            pa.array([0, 1, 2], pa.int64()),
            pa.array([{"label":"東京", "items":[1,None,-2], "amount":Decimal("12.340")}, None,
                      {"label":"", "items":[], "amount":Decimal("-0.001")}], payload),
            pa.array([b"\0\xff", None, b""], pa.binary()),
            pa.array([-1, None, 1730615400000000000], pa.timestamp("ns", "America/New_York")),
        ], schema=schema)
        path = self.write_input(source, "arrow")
        base = (f"WITH retained AS MATERIALIZED (SELECT * FROM '{path}') "
                "SELECT a.id AS id,a.payload AS payload,a.bytes AS bytes,a.instant AS instant "
                "FROM retained AS a JOIN retained AS b ON a.id=b.id")
        frame = self.context.sql(base).sort("id")
        relation = self.session.sql(base).as_relation().sort([("id", False, None)])
        self.five(frame, relation, base+" ORDER BY a.id", source, {
            "relational_materialized_relations": 1, "relational_materialized_rows": 3,
            "relational_materialized_reuses": 1, "relational_recursive_relations": 0,
        })
        recursive = (f"WITH RECURSIVE rows(id,generation,payload,bytes,instant) AS "
                     f"(SELECT id,0 AS generation,payload,bytes,instant FROM '{path}' UNION ALL "
                     "SELECT id,generation+1,payload,bytes,instant FROM rows WHERE generation<1) "
                     "SELECT id,generation,payload,bytes,instant FROM rows ORDER BY generation,id")
        recursive_schema = pa.schema([pa.field("id", pa.int64()), pa.field("generation", pa.int64()),
                                      *list(schema)[1:]])
        expected = pa.Table.from_arrays([
            pa.concat_arrays([source["id"].combine_chunks()] * 2),
            pa.array([0, 0, 0, 1, 1, 1], pa.int64()),
            *[pa.concat_arrays([source[name].combine_chunks()] * 2) for name in ("payload", "bytes", "instant")],
        ], schema=recursive_schema)
        self.five(self.context.sql(recursive), self.session.sql(recursive).as_relation(), recursive, expected, {
            "relational_recursive_relations": 1, "relational_recursive_iterations": 2,
            "relational_recursive_output_rows": 6,
        })
        outputs = []
        for method, extension in (("write_vortex", "vortex"), ("write_parquet", "parquet"), ("write_arrow_ipc", "arrow")):
            destination = self.root / f"retained.{extension}"
            getattr(frame, method)(destination)
            outputs.append(destination)
        path.unlink()
        self.context.close()
        for destination in outputs:
            reader = self.native.Session(self.native.ExecutionResources(128 << 20, 2))
            try:
                result = reader.sql(f"SELECT * FROM '{destination}' ORDER BY id").collect()
                actual = pa.RecordBatchReader.from_stream(result).read_all()
                self.equal(actual, source)
            finally:
                reader.close()
        import pyarrow.parquet as parquet
        self.equal(parquet.read_table(outputs[1]), source)
        self.equal(pa.ipc.open_file(str(outputs[2])).read_all(), source)

    def test_extended_float_materialization_preserves_bits_and_recursive_union_uses_native_identity(self):
        pa = self.pa
        self.policy = "extended"
        self.session = self.native.Session(self.native.ExecutionResources(128 << 20, 2), float_policy=self.policy)
        self.addCleanup(self.session.close)
        self.context = self.sl.context(transport="native", memory_bytes=128 << 20,
                                       max_parallelism=2, float_policy=self.policy)
        self.addCleanup(self.context.close)
        bits = [0x8000000000000000, 0, 0xfff0000000000000, 0x7ff0000000000000,
                0x7ff8000000000042, 0x7ff8000000000081, 1]
        values = pa.Array.from_buffers(pa.float64(), len(bits), [None, pa.py_buffer(
            b"".join(n.to_bytes(8, "little") for n in bits))])
        source = pa.Table.from_arrays([pa.array(list(range(len(bits))), pa.int64()), values], names=["id", "f"])
        path = self.write_input(source, "arrow")
        statement = f"WITH kept AS MATERIALIZED (SELECT * FROM '{path}') SELECT * FROM kept ORDER BY id"
        self.five(self.context.sql(statement), self.session.sql(statement).as_relation(), statement, source, {
            "relational_materialized_relations": 1, "relational_materialized_rows": len(bits),
        })
        statement = (f"WITH RECURSIVE kept(f) AS (SELECT f FROM '{path}' UNION SELECT f FROM kept) "
                     "SELECT f FROM kept ORDER BY f")
        # Equality coalesces signed zeros and NaNs; the first representation survives.
        expected = source.select(["f"]).take(pa.array([2, 0, 6, 3, 4], pa.int64()))
        self.five(self.context.sql(statement), self.session.sql(statement).as_relation(), statement, expected, {
            "relational_recursive_relations": 1, "relational_recursive_iterations": 1,
            "relational_recursive_output_rows": 5,
        })

    def test_parameters_named_snapshots_aggregate_filter_grouping_and_window_compose(self):
        context = self.context
        context.register("seed", context.sql("SELECT 0 AS value"))
        query = context.sql(
            "WITH RECURSIVE start(n) AS MATERIALIZED (SELECT value FROM seed), "
            "seq(n) AS (SELECT n FROM start UNION ALL SELECT n+1 FROM seq WHERE n<:stop), "
            "g AS (SELECT n,GROUPING(n) AS mask,COUNT(*) AS cnt,SUM(n) FILTER (WHERE n%2=0) AS even_total "
            "FROM seq GROUP BY GROUPING SETS ((n),())) "
            "SELECT n,mask,cnt,even_total,SUM(cnt) OVER () AS total FROM g ORDER BY mask,n NULLS LAST",
            parameters={"stop": 3})
        context.register("seed", context.sql("SELECT 99 AS value"), replace=True)
        context.drop_view("seed")
        expected = [dict(n=n, mask=0, cnt=1, even_total=float(n) if n%2 == 0 else None, total=8.0) for n in range(4)]
        expected.append(dict(n=None, mask=1, cnt=4, even_total=2.0, total=8.0))
        for _ in range(2):
            result = query.collect()
            self.assertEqual(list(result.to_rows()), expected)
            self.assertEqual(result.envelope.field("relational_recursive_relations"), "1")
            self.assertEqual(result.envelope.field("relational_materialized_relations"), "1")

    def test_read_once_producer_materialization_is_lazy_shared_and_fresh_for_each_action(self):
        events = []
        def batches():
            generation = events.count("open")
            try:
                events.append("open")
                for n in range(3):
                    yield [{"n": n, "payload": f"{generation}:{n}"}]
            finally:
                events.append("close")
        source = self.context.from_batches(batches, schema={"n":"int64", "payload":"utf8"}, streaming=True)
        query = source.sql("WITH r AS MATERIALIZED (SELECT * FROM this) SELECT a.n AS n,a.payload AS payload FROM r AS a JOIN r AS b ON a.n=b.n ORDER BY a.n")
        self.assertEqual(events, [])
        retained = []
        for generation in range(2):
            result = query.collect()
            self.assertEqual(list(result.to_rows()), [dict(n=n, payload=f"{generation}:{n}") for n in range(3)])
            self.assertEqual(result.envelope.field("relational_materialized_relations"), "1")
            self.assertEqual(result.envelope.field("relational_materialized_reuses"), "1")
            retained.append(result)
        self.assertEqual(events, ["open", "close", "open", "close"])
        self.context.close()
        gc.collect()
        self.assertEqual(retained[0].to_rows()[0]["payload"], "0:0")

    def test_iteration_policy_refuses_partial_success_even_under_zero_limit(self):
        for invalid in (True, False, 0, -1, 1 << 64, 1.5, "4"):
            with self.subTest(invalid=invalid), self.assertRaises((ValueError, RuntimeError)):
                self.native.Session(recursion_limit=invalid)
        for transport in ("native", "subprocess"):
            process = patch("subprocess.Popen", self.real_popen) if transport == "subprocess" else nullcontext()
            with process:
                for allowed in (2, 3):
                    with self.subTest(transport=transport, allowed=allowed):
                        client = self.sl.ShardLoomClient(transport=transport,
                            binary=os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY"),
                            use_persistent_worker=False, memory_bytes=32 << 20,
                            max_parallelism=1, recursion_limit=allowed)
                        context = self.sl.context(client=client)
                        self.addCleanup(context.close)
                        query = "WITH RECURSIVE seq(n) AS (SELECT 0 AS n UNION ALL SELECT n+1 FROM seq WHERE n<2) SELECT n FROM seq"
                        if allowed == 2:
                            for limit in (0, 1):
                                with self.assertRaisesRegex((RuntimeError, self.sl.ShardLoomCommandError), "SL_RECURSION_LIMIT_EXCEEDED"):
                                    context.sql(query + f" LIMIT {limit}").collect()
                            destination = self.root / f"invalid-fixpoint-{transport}.vortex"
                            with self.assertRaisesRegex(self.sl.ShardLoomCommandError, "SL_RECURSION_LIMIT_EXCEEDED"):
                                context.sql(query).write_vortex(destination)
                            self.assertFalse(destination.exists())
                        else:
                            result = context.sql(query).collect()
                            self.assertEqual(result.to_rows(), ({"n":0},{"n":1},{"n":2}))
                            self.assertEqual(result.envelope.field("recursion_limit"), "3")
        native = self.native.Session(self.native.ExecutionResources(32 << 20, 1), recursion_limit=3)
        self.addCleanup(native.close)
        response = native.command(["run", "sql", "--sql", "SELECT 1 AS n", "--request", "collect",
            "--recursion-limit", "4"])
        envelope = json.loads(response)
        self.assertEqual(envelope["status"], "error")
        self.assertFalse(envelope["fallback"]["attempted"])
        self.assertIn("immutable native session policy", response)
        result = native.workflow(["sql", "--sql", query, "--request", "collect", "--bounded", "true"]).collect()
        self.assertEqual(result.report["recursion_limit"], "3")
        self.assertEqual(self.pa.RecordBatchReader.from_stream(result).read_all().to_pylist(),
                         [{"n":0}, {"n":1}, {"n":2}])

    def test_late_producer_errors_close_cancel_and_fresh_retry_never_publish_partial_results(self):
        pa = self.pa
        for recursive in (False, True):
            with self.subTest(recursive=recursive):
                events, fail = [], [True]
                def batches():
                    try:
                        events.append("open")
                        for index in range(3):
                            yield [{"n":0, "id":index, "label":f"row-{index}"}]
                        if fail[0]:
                            raise LookupError("retained producer tail failed")
                        events.append("end")
                    finally:
                        events.append("close")
                source = self.context.from_batches(batches,
                    schema={"n":"int64", "id":"int64", "label":"utf8"}, streaming=True)
                statement = (
                    "WITH RECURSIVE kept(n,id,label) AS (SELECT n,id,label FROM this UNION ALL "
                    "SELECT n+1,id,label FROM kept WHERE n<2) SELECT n,id,label FROM kept"
                    if recursive else
                    "WITH kept AS MATERIALIZED (SELECT n,id,label FROM this) SELECT n,id,label FROM kept")
                query = source.sql(statement)
                self.assertEqual(events, [])
                with self.assertRaisesRegex(LookupError, "retained producer tail failed"):
                    query.limit(0).collect()
                target = self.root / f"unpublished-{recursive}"
                with self.assertRaisesRegex(LookupError, "retained producer tail failed"):
                    query.fanout({"vortex":target / "one.vortex", "arrow-ipc":target / "two.arrow"})
                self.assertFalse(target.exists())
                self.assertEqual(events, ["open", "close"] * 2)
                fail[0] = False
                for cancel in (False, True):
                    with query.iter_batches(batch_rows=1) as stream:
                        first = next(stream)
                        first_table = first.to_arrow()
                        self.assertEqual(events[-3:], ["open", "end", "close"])
                        self.assertIsNone(stream.report)
                        if cancel:
                            self.context.client.cancel()
                            with self.assertRaisesRegex(self.sl.ShardLoomCommandError, "cancel"):
                                next(stream)
                    self.assertIsNone(stream.report)
                    self.assertEqual(first_table.to_pylist(), [{"n":0,"id":0,"label":"row-0"}])
                result = query.collect()
                expected = pa.Table.from_pylist([
                    dict(n=n, id=index, label=f"row-{index}")
                    for n in range(3 if recursive else 1) for index in range(3)
                ], schema=pa.schema([pa.field("n",pa.int64()),pa.field("id",pa.int64()),pa.field("label",pa.string())]))
                self.equal(result.to_arrow(), expected)
                self.check_fields(result.envelope.field, {
                    "relational_recursive_relations": int(recursive),
                    "relational_materialized_relations": int(not recursive),
                })
                self.assertEqual(events, ["open", "close"] * 2 + ["open", "end", "close"] * 3)

    def test_prepared_file_generation_change_is_rejected_and_new_plan_can_retry(self):
        pa = self.pa
        for recursive in (False, True):
            with self.subTest(recursive=recursive):
                table = pa.table({"n":pa.array([0], pa.int64())})
                path = self.write_input(table, "vortex", name=f"generation-{recursive}")
                statement = (
                    f"WITH RECURSIVE kept(n) AS (SELECT n FROM '{path}' UNION ALL "
                    "SELECT n+1 FROM kept WHERE n<2) SELECT n FROM kept"
                    if recursive else
                    f"WITH kept AS MATERIALIZED (SELECT n FROM '{path}') SELECT n FROM kept")
                plan = self.session.sql(statement)
                plan.prepare()
                path.write_bytes(b"changed retained input")
                with self.assertRaisesRegex(RuntimeError, "changed|identity|generation"):
                    plan.collect()
                plan.release_prepared()
                path.unlink()
                replacement = pa.table({"n":pa.array([1], pa.int64())})
                self.write_input(replacement, "vortex", name=f"generation-{recursive}")
                result = self.session.sql(statement).collect()
                actual = pa.RecordBatchReader.from_stream(result).read_all()
                self.equal(actual, pa.table({"n":pa.array([1,2] if recursive else [1], pa.int64())}))


if __name__ == "__main__":
    unittest.main()
