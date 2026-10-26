"""Connected SQL/DataFrame binding through the actual native execution owner."""

from datetime import date
from decimal import Decimal
import gc
import unittest

try:
    from .test_native_binding import _load_native
except ImportError:
    from test_native_binding import _load_native


class ConnectedProgrammingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()
        import shardloom as sl
        cls.sl = sl

    def context(self, memory_bytes=64 << 20):
        context = self.sl.context(transport="native", memory_bytes=memory_bytes, max_parallelism=2)
        self.addCleanup(context.close)
        return context

    def test_plan_transform_builds_once_without_consuming_data_and_rejects_foreign_context(self):
        context = self.context()
        foreign = self.context().sql("SELECT 99 AS value")
        for method in ("pipe", "apply", "transform"):
            with self.subTest(method=method):
                events = []
                def batches():
                    events.append("opened")
                    yield [{"value": 2}, {"value": 5}]
                frame = context.from_batches(batches, schema={"value": "int64"}, streaming=True)
                def increment(plan, amount):
                    events.append("planned")
                    return plan.with_column("value", self.sl.col("value") + amount)
                result = getattr(frame, method)(self.sl.plan_transform(increment), 7)
                self.assertEqual(events, ["planned"])
                self.assertEqual(result.collect().to_rows(), ({"value": 9}, {"value": 12}))
                self.assertEqual(events, ["planned", "opened"])
                with self.assertRaisesRegex(ValueError, "another context"):
                    getattr(frame, method)(self.sl.plan_transform(lambda plan: foreign))

    def test_sql_keywords_in_literal_values_do_not_become_operators(self):
        context = self.context()
        result = context.sql(
            "SELECT 'count(*) AS rows', 'sum(metric) AS total_metric', "
            "'WHERE metric >= 0', 'GROUP BY group_key' FROM input LIMIT 1",
            relations={"input": context.range(0, 3)},
        ).collect()
        self.assertEqual(result.to_rows(), ({"expression_1": "count(*) AS rows",
            "expression_2": "sum(metric) AS total_metric", "expression_3": "WHERE metric >= 0",
            "expression_4": "GROUP BY group_key"},))

    def test_deep_mixed_pipeline_preserves_domains_and_preflight_before_producer(self):
        context = self.context(256 << 20)
        opened = []
        def batches():
            opened.append(True)
            yield [{"value": value} for value in range(16)]
        source = context.from_batches(batches, schema={"value": "int64"}, streaming=True)
        frame = context.sql("SELECT value FROM input", relations={"input": source})
        for index in range(40):
            frame = frame.with_column("value", self.sl.col("value") + 1).filter(
                self.sl.col("value") >= index + 4).limit(7)
        frame = frame.sort("value")
        self.assertEqual(opened, [])
        self.assertEqual(dict(frame.schema())["value"].name, "int64")
        self.assertEqual(opened, [])
        with self.assertRaises(self.sl.ShardLoomCommandError):
            frame.collect(memory_bytes=1024, max_parallelism=1)
        self.assertEqual(opened, [])
        self.assertEqual(frame.collect().to_rows(), tuple({"value": value} for value in range(43, 50)))
        self.assertEqual(opened, [True])

    def test_wide_cte_chain_and_large_sql_use_shared_declarations_and_resource_admission(self):
        context = self.context(512 << 20)
        definitions = ["q0 AS (SELECT 0 AS value)"]
        for index in range(1, 129):
            definitions.append(f"q{index} AS (SELECT value+1 AS value FROM q{index-1} WHERE value>=0)")
        query = context.sql("WITH " + ",".join(definitions) + " SELECT value FROM q128")
        self.assertEqual(query.collect().to_rows(), ({"value": 128},))
        text = "x" * (20 << 10)
        # A wide statement crosses the former query-text ceiling while each
        # scalar stays within its independent expression-intake contract.
        statement = "SELECT " + ",".join(f"LENGTH('{text}') AS size_{index}" for index in range(16))
        self.assertGreater(len(statement), 256 << 10)
        query = context.sql(statement)
        with self.assertRaises(self.sl.ShardLoomCommandError):
            query.collect(memory_bytes=16 << 20, max_parallelism=1)
        self.assertEqual(query.collect().to_rows(), ({f"size_{index}": len(text) for index in range(16)},))

    def test_deep_pipeline_drains_stream_and_propagates_late_producer_errors(self):
        context = self.context(256 << 20)
        events = []
        def batches():
            try:
                events.append("opened")
                yield [{"value": 2}]
                raise LookupError("late deep pipeline failure")
            finally:
                events.append("closed")
        frame = context.from_batches(batches, schema={"value": "int64"}, streaming=True)
        for _ in range(36):
            frame = frame.with_column("value", self.sl.col("value") + 1).limit(1)
        frame = frame.limit(0)
        with self.assertRaisesRegex(LookupError, "late deep pipeline failure"):
            frame.collect()
        self.assertEqual(events, ["opened", "closed"])

    def test_deep_branches_compose_and_cancellation_releases_stream_ownership(self):
        context = self.context(256 << 20)
        branches = []
        for start in (0, 10, 20, 30):
            branch = context.range(start, start + 3, column="value")
            for _ in range(36):
                branch = branch.with_column("value", self.sl.col("value") + 1).limit(3)
            branches.append(branch)
        combined = context.sql(
            "SELECT value FROM a UNION ALL SELECT value FROM b "
            "UNION ALL SELECT value FROM c UNION ALL SELECT value FROM d",
            relations=dict(zip(("a", "b", "c", "d"), branches)),
        ).sort("value")
        self.assertEqual(combined.collect().to_rows(), tuple(
            {"value": value} for start in (36, 46, 56, 66) for value in range(start, start + 3)))

        events = []
        def batches():
            try:
                events.append("opened")
                for index in range(20):
                    yield [{"value": index}]
            finally:
                events.append("closed")
        frame = context.from_batches(batches, schema={"value": "int64"}, streaming=True)
        for _ in range(36):
            frame = frame.with_column("value", self.sl.col("value") + 1).filter(self.sl.col("value") >= 0)
        stream = frame.iter_batches(batch_rows=1)
        retained = next(stream)
        context.cancel()
        with self.assertRaises(self.sl.ShardLoomCommandError) as failure:
            next(stream)
        self.assertEqual(failure.exception.diagnostics[0].code, "SL_EXECUTION_CANCELLED")
        stream.close()
        self.assertEqual(events, ["opened", "closed"])
        self.assertIsNone(stream.report)
        self.assertEqual(retained.to_rows(), ({"value": 36},))

    def test_catalog_snapshots_and_query_local_shadowing_use_one_dataframe(self):
        context = self.context()
        original = context.sql("SELECT 5 AS value")
        self.assertIs(type(original), self.sl.LazyFrame)
        context.register("Values", original)
        bound = context.sql("WITH a(x) AS (SELECT value FROM values) SELECT x+:add AS answer FROM a",
                            parameters={"add": 2})
        context.register("values", context.sql("SELECT 99 AS value"), replace=True)
        current = context.table("VALUES")
        local = context.sql("SELECT value FROM values", relations={"values": context.sql("SELECT 11 AS value")})
        self.assertEqual(context.list_tables(), ("values",))
        context.drop_view("values")
        self.assertEqual(context.list_tables(), ())
        with self.assertRaisesRegex(KeyError, "unknown relation"):
            context.table("values")
        snapshot = context.client._native_backend.session.snapshot()
        self.assertFalse(snapshot["native_owner_initialized"])
        del original
        gc.collect()
        for _ in range(2):
            self.assertEqual(bound.collect().to_rows(), ({"answer": 7},))
        self.assertEqual(current.collect().to_rows(), ({"value": 99},))
        self.assertEqual(local.collect().to_rows(), ({"value": 11},))

    def test_dataframe_sql_join_and_typed_parameters_are_one_native_computation(self):
        context = self.context()
        source = context.sequence(0, 5).filter(self.sl.col("value") >= 2)
        context.register("numbers", source)
        query = context.sql("SELECT value, value+:amount AS amount FROM numbers", parameters={"amount": 10})
        dimension = context.sequence(2, 4).select("value", "value+100 AS label")
        result = query.join(dimension, on="value").select("f.value AS value", "f.amount AS amount", "d.label AS label").sort("value")
        self.assertIs(type(result), self.sl.LazyFrame)
        self.assertFalse(context.client._native_backend.session.snapshot()["native_owner_initialized"])
        self.assertEqual(result.collect().to_rows(), (
            {"value": 2, "amount": 12, "label": 102},
            {"value": 3, "amount": 13, "label": 103},
        ))

    def test_exact_scalar_values_and_parameter_errors(self):
        context = self.context()
        value = "x'); SELECT bad FROM secret; --"
        query = context.sql("SELECT :money AS money,:day AS day,:empty AS empty,:text AS text",
            parameters={"money": Decimal("123.450"), "day": date(1970, 1, 2), "empty": None, "text": value})
        self.assertEqual(query.collect().to_rows(), ({"money": Decimal("123.450"), "day": date(1970, 1, 2), "empty": None, "text": value},))
        for statement, values in [("SELECT :missing AS value", {}), ("SELECT 1 AS value", {"extra": 1}),
                                  ("SELECT :a AS value", {"A": 1}), ("SELECT * FROM :name", {"name": "numbers"})]:
            with self.subTest(statement=statement, parameters=values), self.assertRaises((ValueError, RuntimeError)):
                context.sql(statement, parameters=values)

    def test_expression_subqueries_retain_bound_values_and_context_ownership(self):
        context = self.context()
        base = context.range(0, 4, column="value")
        for minimum in (1, 3, 1):
            child = context.sql("SELECT value FROM input WHERE value>=:minimum",
                                relations={"input": base}, parameters={"minimum": minimum})
            scalar = self.sl.scalar_subquery(child.agg(total="SUM(value)"))
            query = base.filter(self.sl.col("value").isin_source(child, "value")).select(
                "value", scalar.alias("total")).sort("value")
            self.assertNotIn(":minimum", query.statement)
            self.assertEqual(query.collect().to_rows(), tuple(
                {"value": value, "total": sum(range(minimum, 4))} for value in range(minimum, 4)))
        foreign = self.context().sql("SELECT 2 AS value")
        with self.assertRaisesRegex(ValueError, "another context"):
            base.filter(self.sl.col("value").isin_source(foreign, "value")).collect()

    def test_unused_catalog_binding_does_not_hide_a_later_expression_source(self):
        context = self.context()
        opened = []
        def batches():
            opened.append(True)
            yield [{"value": 9}]
        source = context.from_batches(batches, schema={"value": "int64"}, streaming=True)
        context.register("unused", source)
        query = context.sql("SELECT 1 AS value")
        context.drop_view("unused")
        query = query.select(self.sl.scalar_subquery(source).alias("answer"))
        self.assertEqual(opened, [])
        self.assertEqual(query.collect().to_rows(), ({"answer": 9},))
        self.assertEqual(opened, [True])

    def test_registration_does_not_open_producers_and_unused_views_are_inert(self):
        context = self.context()
        opened = []
        def batches():
            opened.append("open")
            yield [{"value": 2}, {"value": 3}]
        source = context.from_batches(batches, schema={"value": "int64"})
        context.register("batch_values", source)
        result = context.sql("SELECT SUM(value) AS total FROM batch_values")
        unused = context.sql("SELECT 1 AS value")
        self.assertEqual(opened, [])
        self.assertEqual(unused.collect().to_rows(), ({"value": 1},))
        self.assertEqual(opened, [])
        self.assertEqual(result.collect().to_rows(), ({"total": 5},))
        self.assertEqual(opened, ["open"])

    def test_cross_context_and_duplicate_name_fail_before_input_effects(self):
        context, other = self.context(), self.context()
        original = context.sql("SELECT 1 AS value")
        context.register("one", original)
        with self.assertRaisesRegex(ValueError, "already exists"):
            context.register("ONE", original)
        with self.assertRaisesRegex(ValueError, "another context"):
            other.register("one", original)
        with self.assertRaisesRegex(ValueError, "duplicate relation"):
            context.sql("SELECT * FROM one", relations={"one": original, "ONE": original})
        with self.assertRaisesRegex(Exception, "unknown relation"):
            context.sql("SELECT * FROM unknown_name")
        result = context.table("one").collect()
        context.close()
        self.assertEqual(result.to_rows(), ({"value": 1},))
        with self.assertRaises(Exception):
            original.collect()

    def test_catalog_schema_binds_types_without_opening_any_batch_producer(self):
        context = self.context()
        opened = []
        def batches():
            opened.append(True)
            yield [{"value": 2, "text": None}]
        for streaming in (False, True):
            with self.subTest(streaming=streaming):
                source = context.from_batches(batches,
                    schema={"value": {"type": "int64", "nullable": False}, "text": "utf8"},
                    streaming=streaming)
                context.register("values", source, replace=True)
                query = context.sql("SELECT value+:increment AS total,text FROM values",
                                    parameters={"increment": 3})
                fields = dict(query.schema())
                self.assertEqual((fields["total"].name, fields["total"].nullable), ("int64", False))
                self.assertEqual((fields["text"].name, fields["text"].nullable), ("utf8", True))
                self.assertEqual(dict(context.catalog.schema("values"))["value"].name, "int64")
                self.assertEqual(opened, [])
                self.assertEqual(query.collect().to_rows(), ({"total": 5, "text": None},))
                self.assertEqual(opened, [True])
                opened.clear()

    def test_schema_does_not_start_compatibility_preparation(self):
        context = self.context()
        frame = context.read_csv("missing-schema-inspection.csv")
        with self.assertRaisesRegex(Exception, "compatibility schema inference"):
            frame.schema()

    def test_columnar_schema_uses_footer_without_decoding_damaged_data(self):
        import tempfile
        from pathlib import Path
        import pyarrow as pa
        import pyarrow.parquet as parquet
        context = self.context()
        table = pa.table({"value": pa.array([1, None, 3], pa.int64()), "label": ["é", None, "猫"]})
        with tempfile.TemporaryDirectory(prefix="shardloom-schema-metadata-") as directory:
            for format, trailer in (("parquet", 8), ("arrow_ipc", 10)):
                path = Path(directory) / f"source.{format}"
                if format == "parquet":
                    parquet.write_table(table, path)
                else:
                    with pa.OSFile(str(path), "wb") as output, pa.ipc.new_file(output, table.schema) as writer:
                        writer.write_table(table)
                data = bytearray(path.read_bytes())
                length = int.from_bytes(data[-trailer:-trailer + 4], "little")
                data[6:len(data)-trailer-length] = bytes(len(data)-trailer-length-6)
                path.write_bytes(data)
                frame = getattr(context, "read_" + format)(path)
                context.register("metadata", frame, replace=True)
                fields = dict(context.sql("SELECT value+1 AS next,label FROM metadata").schema())
                self.assertEqual(fields["next"].name, "int64")
                self.assertEqual(fields["label"].name, "utf8")
                self.assertTrue(fields["next"].nullable)
                self.assertEqual(set(Path(directory).iterdir()), {Path(directory) / "source.parquet"} |
                                 ({path} if format == "arrow_ipc" else set()))

    def test_function_types_and_reusable_window_specs_share_native_semantics(self):
        from shardloom import functions as f, types as t, Window
        context = self.context()
        frame = context.from_rows([
            {"key": 1, "value": 10, "label": "é"},
            {"key": 1, "value": 20, "label": None},
            {"key": 2, "value": None, "label": "猫"},
        ], schema={"key": t.int64(nullable=False), "value": t.int64(), "label": t.string()})
        context.register("values", frame)
        spec = Window.order_by("key", nulls="last")
        peer_frame = spec.range_between(0, 0)
        query = frame.window(
            f.sum("value").over(peer_frame).alias("peer_sum"),
            f.rank().over(spec).alias("position"),
            f.dense_rank().over(spec).alias("dense_position"),
        ).select("key", "value", "peer_sum", "position", "dense_position",
                 f.length(f.col("label")).alias("chars"),
                 f.byte_length(f.lit(b"\xc3\xa9")).alias("bytes")).sort("key", "value", nulls="last")
        reference = context.sql("""SELECT key,value,
            SUM(value) OVER (ORDER BY key NULLS LAST RANGE BETWEEN CURRENT ROW AND CURRENT ROW) AS peer_sum,
            RANK() OVER (ORDER BY key NULLS LAST) AS position,
            DENSE_RANK() OVER (ORDER BY key NULLS LAST) AS dense_position,
            LENGTH(label) AS chars,BYTE_LENGTH(X'c3a9') AS bytes FROM values ORDER BY key,value NULLS LAST""")
        expected = (
            {"key": 1, "value": 10, "peer_sum": 30, "position": 1, "dense_position": 1, "chars": 1, "bytes": 2},
            {"key": 1, "value": 20, "peer_sum": 30, "position": 1, "dense_position": 1, "chars": None, "bytes": 2},
            {"key": 2, "value": None, "peer_sum": None, "position": 3, "dense_position": 2, "chars": 1, "bytes": 2},
        )
        self.assertEqual(query.collect().to_rows(), expected)
        self.assertEqual(reference.collect().to_rows(), expected)
        self.assertNotIn("RANGE", spec.sql)
        aggregate = frame.aggregate(f.count().alias("rows"), f.count("value").alias("present"),
                                    f.count_distinct("value").alias("distinct_values"), f.sum("value").alias("total"))
        self.assertEqual(aggregate.collect().to_rows(), ({"rows": 3, "present": 2, "distinct_values": 2, "total": 30},))

    def test_type_namespace_preserves_nested_nullability_and_decimal_scale(self):
        from shardloom import types as t
        context = self.context()
        amount = t.decimal128(12, 3, nullable=False)
        nested = t.list_(t.struct({"amount": amount}, nullable=False))
        frame = context.from_rows([{"items": [{"amount": Decimal("1.230")}]}], schema={"items": nested})
        fields = dict(frame.schema())
        self.assertEqual(fields["items"], nested)
        self.assertEqual(frame.collect().to_rows(), ({"items": [{"amount": Decimal("1.230")}]},))
        deep = t.int64()
        for _ in range(100):
            deep = t.ResultType("list", True, (deep,))
        with self.assertRaisesRegex(ValueError, "depth 24"):
            t.list_(deep)
        with self.assertRaisesRegex(ValueError, "distinct"):
            t.list_(t.ResultType("struct", True, (("same", t.int64()), ("same", t.int64()))))


if __name__ == "__main__":
    unittest.main()
