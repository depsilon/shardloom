"""Trusted application functions compose through the shared native pipeline."""

from decimal import Decimal
from pathlib import Path
import gc
import tempfile
import traceback
import unittest
from unittest.mock import patch

try:
    from .test_native_binding import _load_native
except ImportError:
    from test_native_binding import _load_native


class NativeApplicationBatchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()
        import pyarrow as pa
        import shardloom as sl
        cls.pa, cls.sl = pa, sl

    def setUp(self):
        process = patch("subprocess.Popen", side_effect=AssertionError("external execution"))
        process.start()
        self.addCleanup(process.stop)

    def context(self, memory=32 << 20):
        ctx = self.sl.context(transport="native", memory_bytes=memory, max_parallelism=2)
        self.addCleanup(ctx.close)
        return ctx

    def register(self, ctx, function, *, name="transform", **options):
        settings = dict(input_schema={"n": "int64", "s": "utf8"},
            output_schema={"n": "int64", "s": "utf8"}, version="1",
            max_batch_rows=3, max_output_bytes=1 << 20, workspace_bytes=32 << 10,
            trusted=True, rowwise=True)
        settings.update(options)
        return ctx.batch_functions.register(name, function, **settings)

    def transform(self, events, *, factor=3, renamed=False):
        pa = self.pa
        def apply(batch, invocation):
            invocation.check_cancelled()
            self.assertEqual(invocation.float_policy, "strict")
            self.assertGreater(batch.num_rows, 0)
            self.assertLessEqual(batch.num_rows, 3)
            events.append(batch.num_rows)
            numbers = [None if n is None else n * factor for n in batch.column("n").to_pylist()]
            strings = [None if word is None else word + ":λ" for word in batch.column("s").to_pylist()]
            return pa.RecordBatch.from_arrays([pa.array(numbers, type=pa.int64()),
                pa.array(strings, type=pa.string())], names=["triple", "label"] if renamed else ["n", "s"])
        return apply

    def source(self, ctx, rows):
        return ctx.from_rows(rows, schema={"n": "int64", "s": "utf8"})

    def test_documented_example_executes_with_the_loaded_package(self):
        path = Path(__file__).resolve().parents[2] / "docs/reference/application-batch-functions.md"
        example = path.read_text().split("```python\n", 1)[1].split("\n```", 1)[0]
        exec(compile(example, str(path), "exec"), {"__name__": "application_batch_documentation"})

    def test_session_registry_captures_a_function_for_its_shared_context(self):
        ctx, calls = self.context(), []
        session = ctx.session()
        function = self.register(session, self.transform(calls))
        self.assertEqual(session.batch_functions.list(), ("transform",))
        self.assertIs(session.batch_functions.get("TRANSFORM"), function)
        source = self.source(ctx, [{"n": 2, "s": "x"}])
        session.register("transformed", source.map_batches(function))
        self.assertTrue(session.batch_functions.drop("transform"))
        self.assertEqual(calls, [])
        self.assertEqual(session.sql("SELECT * FROM transformed").collect().to_rows(),
            ({"n": 6, "s": "x:λ"},))
        self.assertEqual(calls, [1])
        session.close()
        with self.assertRaises(RuntimeError):
            session.batch_functions.list()

    def test_registration_binding_schema_and_explain_are_inert(self):
        ctx = self.context()
        opened, invoked = [], []
        def producer():
            opened.append(True)
            yield [{"n": 2, "s": "é"}]
        handle = self.register(ctx, self.transform(invoked),
            output_schema={"triple": "int64", "label": "utf8"})
        frame = ctx.from_batches(producer, schema={"n": "int64", "s": "utf8"}, streaming=True).map_batches(handle)
        self.assertEqual(ctx.batch_functions.list(), ("transform",))
        self.assertIs(ctx.batch_functions.get("TRANSFORM"), handle)
        self.assertEqual(tuple(name for name, _ in frame.schema()), ("triple", "label"))
        frame.explain()
        frame.estimate()
        self.assertEqual(opened, [])
        self.assertEqual(invoked, [])
        self.assertIn("application_batch(transform,1)", frame.operation_summary)
        # Plain text cannot stand in for the live callback, even with a relation
        # whose name collides with the nonportable table-function marker.
        ctx.register("application_batch_local_handle_required", ctx.from_rows([{"n": 9}]))
        with self.assertRaises((RuntimeError, ValueError, self.sl.ShardLoomCommandError)):
            ctx.sql(frame.statement).collect()
        self.assertEqual(invoked, [])

    def test_complete_nullable_results_partitions_repeated_actions_and_empty_input(self):
        rows = [{"n": None if index % 5 == 0 else index,
                 "s": None if index % 7 == 0 else "東京"} for index in range(37)]
        expected = tuple({"n": None if row["n"] is None else row["n"] * 3,
            "s": None if row["s"] is None else row["s"] + ":λ"} for row in rows)
        for partition in (1, 5, 37):
            with self.subTest(partition=partition):
                ctx, events, lifecycle = self.context(), [], []
                def producer():
                    lifecycle.append("open")
                    try:
                        yield []
                        for start in range(0, len(rows), partition):
                            yield rows[start:start + partition]
                    finally:
                        lifecycle.append("close")
                handle = self.register(ctx, self.transform(events))
                frame = ctx.from_batches(producer, schema={"n": "int64", "s": "utf8"}, streaming=True).map_batches(handle)
                for action in (1, 2):
                    result = frame.collect()
                    self.assertEqual(result.to_rows(), expected)
                    self.assertEqual(sum(events), 37 * action)
                    self.assertEqual(lifecycle, ["open", "close"] * action)
                    proof = result.envelope.field("local_primitive_native_io_pushdown_proof_basis")
                    self.assertIn("application_rows=37", proof)
                    self.assertIn("application_arrow_export_batches=", proof)
                    self.assertEqual(result.envelope.field("fallback_attempted"), "false")
                    self.assertEqual(result.envelope.field("external_engine_invoked"), "false")
        ctx, events = self.context(), []
        empty = self.source(ctx, []).map_batches(self.register(ctx, self.transform(events)))
        result = empty.collect()
        self.assertEqual(result.to_rows(), ())
        self.assertEqual(events, [])
        self.assertIn("application_completed_batches=0", result.envelope.field("local_primitive_native_io_pushdown_proof_basis"))

    def test_replaced_and_removed_names_keep_bound_snapshots(self):
        ctx, first, second = self.context(), [], []
        source = self.source(ctx, [{"n": 2, "s": "x"}])
        old = self.register(ctx, self.transform(first))
        bound_old = source.map_batches(old)
        with self.assertRaisesRegex(ValueError, "already exists"):
            self.register(ctx, self.transform(second))
        new = self.register(ctx, self.transform(second, factor=5), replace=True)
        self.assertIsNot(old, new)
        self.assertNotEqual(old, new)
        bound_new = source.map_batches(ctx.batch_functions.get("transform"))
        self.assertTrue(ctx.batch_functions.drop("transform"))
        with self.assertRaises(KeyError):
            ctx.batch_functions.get("transform")
        self.assertFalse(ctx.batch_functions.drop("transform", if_exists=True))
        self.assertEqual(bound_old.collect().to_rows(), ({"n": 6, "s": "x:λ"},))
        self.assertEqual(bound_new.collect().to_rows(), ({"n": 10, "s": "x:λ"},))
        self.assertEqual(first, [1])
        self.assertEqual(second, [1])
        foreign = self.context()
        with self.assertRaisesRegex(ValueError, "another context"):
            self.source(foreign, []).map_batches(old)

    def test_join_aggregation_materialized_sql_and_write_reopen(self):
        ctx, events = self.context(), []
        rows = [{"n": n, "s": "group"} for n in range(8)]
        function = self.register(ctx, self.transform(events, renamed=True),
            output_schema={"triple": "int64", "label": "utf8"})
        frame = self.source(ctx, rows).filter(self.sl.col("n") >= 2).map_batches(function)
        dimension = ctx.from_rows([{"triple": n * 3, "region": n % 2} for n in range(8)],
            schema={"triple": "int64", "region": "int64"})
        joined = frame.join(dimension, on="triple").select("f.triple AS amount", "d.region AS region")
        from shardloom import functions as f
        report = joined.group_by("region").agg(f.sum("amount").alias("total"), f.count().alias("rows")).sort("region")
        expected = ({"region": 0, "total": 36.0, "rows": 3}, {"region": 1, "total": 45.0, "rows": 3})
        self.assertEqual(report.collect().to_rows(), expected)
        self.assertEqual(sum(events), 6)
        ctx.register("transformed", frame)
        repeated = ctx.sql("WITH owned AS MATERIALIZED (SELECT * FROM transformed) "
            "SELECT SUM(triple) AS total FROM owned UNION ALL SELECT SUM(triple) AS total FROM owned")
        before = sum(events)
        self.assertEqual(repeated.collect().to_rows(), ({"total": 81.0}, {"total": 81.0}))
        self.assertEqual(sum(events) - before, 6)
        with tempfile.TemporaryDirectory() as directory:
            for format in ("vortex", "parquet", "arrow_ipc"):
                target = Path(directory) / ("output." + format)
                receipt = getattr(report, "write_" + format)(target)
                self.assertEqual(receipt.output_commit_status, "committed")
                self.assertEqual(getattr(ctx, "read_" + format)(target).collect().to_rows(), expected)

    def test_python_exception_identity_traceback_no_retry_and_failed_write_cleanup(self):
        ctx, calls = self.context(), []
        original = LookupError("application-specific failure")
        original.diagnostic = {"application_owned": "preserve this attribute too"}
        fail = [True]
        def callback(batch, invocation):
            calls.append(batch.num_rows)
            if fail[0]:
                raise original
            return batch
        frame = self.source(ctx, [{"n": 2, "s": "x"}]).map_batches(self.register(ctx, callback))
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "failed.vortex"
            for action in (frame.collect, lambda: frame.write_vortex(target)):
                with self.assertRaises(LookupError) as failure:
                    action()
                self.assertIs(failure.exception, original)
                self.assertTrue(hasattr(original, "resource_fields"))
            self.assertEqual(calls, [1, 1])
            self.assertFalse(target.exists())
            self.assertEqual(list(Path(directory).iterdir()), [])
            fail[0] = False
            self.assertEqual(frame.collect().to_rows(), ({"n": 2, "s": "x"},))
            self.assertEqual(calls, [1, 1, 1])
        # unittest clears caught tracebacks; inspect identity within a direct catch.
        fail[0] = True
        try:
            frame.collect()
        except LookupError as error:
            self.assertIs(error, original)
            self.assertEqual(error.diagnostic, {"application_owned": "preserve this attribute too"})
            self.assertIn("callback", [entry.name for entry in traceback.extract_tb(error.__traceback__)])

    def test_dropna_observes_callback_outputs_after_existing_filters(self):
        pa = self.pa
        for renamed in (False, True):
            with self.subTest(renamed=renamed):
                ctx, seen = self.context(), []
                names = ("output_n", "output_s") if renamed else ("n", "s")
                def callback(batch, invocation):
                    values = batch.column("n").to_pylist()
                    seen.extend(values)
                    return pa.RecordBatch.from_arrays([
                        pa.array([None if n in (1, 4) else n for n in values], type=pa.int64()),
                        pa.array([None if n in (2, 4) else "kept" for n in values], type=pa.string()),
                    ], names=list(names))
                function = self.register(ctx, callback,
                    output_schema={names[0]: "int64", names[1]: "utf8"})
                source = self.source(ctx, [{"n": n, "s": "input"} for n in (-1, 1, 2, 3, 4)])
                frame = source.filter("n > 0").filter("n < 5").map_batches(function)
                outputs = {
                    1: {names[0]: None, names[1]: "kept"},
                    2: {names[0]: 2, names[1]: None},
                    3: {names[0]: 3, names[1]: "kept"},
                }
                for options, kept in [
                    ({"subset": [names[0]]}, (2, 3)),
                    ({"subset": list(names), "how": "any"}, (3,)),
                    ({"subset": list(names), "how": "all"}, (1, 2, 3)),
                    ({"subset": list(names), "thresh": 1}, (1, 2, 3)),
                    ({"subset": list(names), "thresh": 2}, (3,)),
                ]:
                    with self.subTest(options=options):
                        seen.clear()
                        self.assertEqual(frame.dropna(**options).collect().to_rows(),
                            tuple(outputs[n] for n in kept))
                        self.assertEqual(seen, [1, 2, 3, 4])
                seen.clear()
                downstream = frame.filter(f"{names[0]} >= 2").dropna(subset=[names[1]])
                self.assertEqual(downstream.collect().to_rows(), (outputs[3],))
                self.assertEqual(seen, [1, 2, 3, 4])

    def test_dropna_cannot_remove_rows_that_fail_inside_the_callback(self):
        ctx, seen = self.context(), []
        original = LookupError("null input must reach the application function")
        def callback(batch, invocation):
            values = batch.column("n").to_pylist()
            seen.extend(values)
            if None in values:
                raise original
            return batch
        source = self.source(ctx, [{"n": None, "s": "present"}, {"n": 1, "s": "present"}])
        frame = source.filter("s IS NOT NULL").map_batches(self.register(ctx, callback))
        with self.assertRaises(LookupError) as failure:
            frame.dropna(subset=["n"]).collect()
        self.assertIs(failure.exception, original)
        self.assertEqual(seen, [None, 1])

    def test_output_schema_nullability_rows_and_bytes_fail_before_publication(self):
        pa = self.pa
        for fault in ("rows", "name", "type", "nullability", "bytes", "return_type"):
            with self.subTest(fault=fault):
                ctx, calls = self.context(), []
                def callback(batch, invocation):
                    calls.append(True)
                    if fault == "rows":
                        return batch.slice(0, 0)
                    if fault == "name":
                        return pa.RecordBatch.from_arrays(batch.columns, names=["wrong", "s"])
                    if fault == "type":
                        return pa.RecordBatch.from_arrays([pa.array([1.0]), batch.column("s")], names=["n", "s"])
                    if fault == "nullability":
                        return pa.RecordBatch.from_arrays(batch.columns, schema=pa.schema([
                            pa.field("n", pa.int64(), nullable=False), pa.field("s", pa.string())]))
                    if fault == "return_type":
                        return [{"n": 1, "s": "x"}]
                    return batch
                handle = self.register(ctx, callback, max_output_bytes=1 if fault == "bytes" else 1 << 20)
                frame = self.source(ctx, [{"n": 1, "s": "x"}]).map_batches(handle)
                with self.assertRaises((RuntimeError, ValueError, TypeError, self.sl.ShardLoomCommandError)):
                    frame.collect()
                self.assertEqual(calls, [True])

    def test_cancellation_reentry_denial_and_invocation_context_expiry(self):
        ctx, invocations = self.context(), []
        nested = self.source(ctx, [{"n": 2, "s": "x"}])
        def callback(batch, invocation):
            invocations.append(invocation)
            with self.assertRaises((RuntimeError, self.sl.ShardLoomCommandError)) as failure:
                nested.collect()
            self.assertIn("cannot start another native action", str(failure.exception))
            return batch
        frame = nested.map_batches(self.register(ctx, callback))
        self.assertEqual(frame.collect().to_rows(), ({"n": 2, "s": "x"},))
        with self.assertRaisesRegex(RuntimeError, "invocation has ended"):
            invocations[0].check_cancelled()
        denied_calls = []
        denied = self.register(ctx, lambda batch, call: denied_calls.append(True) or batch,
            name="denied", workspace_bytes=64 << 20)
        with self.assertRaises(self.sl.ShardLoomCommandError):
            nested.map_batches(denied).collect()
        self.assertEqual(denied_calls, [])
        cancel_calls = []
        def cancel(batch, invocation):
            cancel_calls.append(True)
            ctx.cancel()
            invocation.check_cancelled()
            return batch
        cancelled = nested.map_batches(self.register(ctx, cancel, name="cancelled"))
        with self.assertRaises((RuntimeError, self.sl.ShardLoomCommandError)):
            cancelled.collect()
        self.assertEqual(cancel_calls, [True])
        self.assertEqual(frame.collect().to_rows(), ({"n": 2, "s": "x"},))

    def test_late_failure_and_original_position_survive_limits(self):
        for count in (0, 1):
            ctx, calls = self.context(), []
            original = ArithmeticError("late batch")
            def callback(batch, invocation):
                calls.append(batch.num_rows)
                if len(calls) == 2:
                    raise original
                return batch
            source = self.source(ctx, [{"n": n, "s": "x"} for n in range(9)])
            handle = self.register(ctx, callback)
            with self.assertRaises(ArithmeticError) as failure:
                source.map_batches(handle).limit(count).collect()
            self.assertIs(failure.exception, original)
            self.assertEqual(calls, [3, 3])
            calls.clear()
            self.assertEqual(source.limit(0).map_batches(handle).collect().to_rows(), ())
            self.assertEqual(calls, [])

    def test_retained_input_output_and_rich_types_survive_context_close(self):
        ctx, retained = self.context(), []
        schema = {"n": "int64", "amount": "decimal128(12,2)",
            "items": {"type": "list", "item": "int16"},
            "record": {"type": "struct", "fields": {"name": "utf8", "flag": "bool"}},
            "stamp": "timestamp(ns)"}
        rows = [{"n": -(1 << 63), "amount": Decimal("123.45"), "items": [1, None, -3],
                 "record": {"name": "λ", "flag": True}, "stamp": 1234567890123456789},
                {"n": None, "amount": None, "items": None, "record": None, "stamp": None}]
        # Readback uses the API's exact temporal scalar representation; derive
        # it once from the original typed source, without calling the function.
        source = ctx.from_rows(rows, schema=schema)
        expected = source.collect().to_rows()
        def identity(batch, invocation):
            retained.append(batch)
            return batch
        handle = self.register(ctx, identity, input_schema=schema, output_schema=schema)
        frame = source.map_batches(handle)
        result = frame.collect()
        self.assertEqual(result.to_rows(), expected)
        ctx.close()
        self.assertEqual(result.to_rows(), expected)
        self.assertEqual(retained[0].column("items").to_pylist(), [[1, None, -3], None])
        with self.assertRaises(RuntimeError):
            frame.collect()
        retained.clear()
        del result, frame, source, handle
        gc.collect()

    def test_chained_position_count_and_streamed_input_failure(self):
        ctx, first, second = self.context(), [], []
        source = self.source(ctx, [{"n": n, "s": "x"} for n in range(11)])
        triple = self.register(ctx, self.transform(first), name="triple")
        five = self.register(ctx, self.transform(second, factor=5), name="five")
        transformed = source.map_batches(triple).filter(self.sl.col("n") >= 21).map_batches(five)
        expected = tuple({"n": n * 15, "s": "x:λ:λ"} for n in range(7, 11))
        self.assertEqual(transformed.collect().to_rows(), expected)
        self.assertEqual(sum(first), 11)
        self.assertEqual(sum(second), 4)
        ctx.register("computed", transformed)
        self.assertEqual(ctx.sql("SELECT COUNT(*) AS total FROM computed").collect().to_rows(), ({"total": 4},))
        self.assertEqual(sum(first), 22)
        self.assertEqual(sum(second), 8)
        batches = []
        with transformed.iter_batches(batch_rows=2) as stream:
            for batch in stream:
                batches.extend(batch.to_rows())
        self.assertEqual(tuple(batches), expected)
        self.assertEqual(sum(first), 33)
        self.assertEqual(sum(second), 12)

        original = OSError("producer failed after a valid callback output")
        fail, lifecycle, calls = [True], [], []
        def producer():
            lifecycle.append("open")
            try:
                yield [{"n": 2, "s": "x"}]
                if fail[0]:
                    raise original
                yield [{"n": 3, "s": "y"}]
            finally:
                lifecycle.append("close")
        frame = ctx.from_batches(producer, schema={"n": "int64", "s": "utf8"}, streaming=True)
        frame = frame.map_batches(self.register(ctx, self.transform(calls), name="source_failure"))
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "partial.vortex"
            with self.assertRaises(OSError) as failure:
                frame.write_vortex(target)
            self.assertIs(failure.exception, original)
            self.assertEqual(calls, [1])
            self.assertEqual(lifecycle, ["open", "close"])
            self.assertEqual(list(Path(directory).iterdir()), [])
            fail[0] = False
            self.assertEqual(frame.collect().to_rows(), ({"n": 6, "s": "x:λ"}, {"n": 9, "s": "y:λ"}))
            self.assertEqual(calls, [1, 1, 1])
            self.assertEqual(lifecycle, ["open", "close", "open", "close"])

    def test_invalid_trust_declarations_and_subprocess_transport_are_explicit(self):
        ctx, invoked = self.context(), []
        for overrides in ({"trusted": False}, {"rowwise": False}, {"deterministic": False},
                          {"effect": "external_read"}, {"null_behavior": "null_propagating"},
                          {"max_batch_rows": 0}, {"max_batch_rows": True}, {"workspace_bytes": -1},
                          {"input_schema": {}}, {"version": ""}):
            with self.subTest(overrides=overrides), self.assertRaises((ValueError, TypeError)):
                self.register(ctx, lambda batch, call: invoked.append(True) or batch, **overrides)
        self.assertEqual(invoked, [])
        cli = self.sl.context(transport="subprocess", memory_bytes=1 << 20, max_parallelism=1)
        self.addCleanup(cli.close)
        with self.assertRaisesRegex(ValueError, "in-process native transport"):
            self.register(cli, lambda batch, call: batch)

    def test_typed_embedding_keeps_session_ownership_and_inert_declared_output(self):
        from shardloom.native import BatchFunction, ExecutionResources, Relation, Session
        session = Session(ExecutionResources(16 << 20, 1))
        self.addCleanup(session.close)
        calls = []
        def renamed(batch, invocation):
            calls.append(batch.num_rows)
            return self.pa.RecordBatch.from_arrays(batch.columns,
                schema=self.pa.schema([self.pa.field("renamed", self.pa.int64(), nullable=False)]))
        function = BatchFunction(session, "rename", "1", renamed,
            [("value", '{"Primitive":["i64",false]}')],
            [("renamed", '{"Primitive":["i64",false]}')],
            3, 1 << 20, 1024, trusted=True, rowwise=True)
        source = session.from_range("value", 0, 1, 11)
        plan = session.plan(Relation.scan("memory://typed"), sources=[("memory://typed", source)])
        mapped = plan.map_batches(function)
        self.assertEqual(calls, [])
        self.assertEqual(mapped.collect().row_export()[0], [{"renamed": n} for n in range(11)])
        self.assertEqual(sum(calls), 11)
        foreign = Session(ExecutionResources(16 << 20, 1))
        self.addCleanup(foreign.close)
        with self.assertRaisesRegex(ValueError, "another context"):
            foreign.sql("SELECT 1 AS value").map_batches(function)


if __name__ == "__main__":
    unittest.main()
