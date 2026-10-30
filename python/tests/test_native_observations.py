"""Plan-position observations share native execution and action completion."""

from decimal import Decimal
from pathlib import Path
import tempfile
import unittest

try:
    from .test_native_binding import _load_native
except ImportError:
    from test_native_binding import _load_native


class NativeObservationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()
        import shardloom as sl
        from shardloom import functions as f
        cls.sl, cls.f = sl, f

    def context(self):
        context = self.sl.context(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(context.close)
        return context

    def test_streamed_metrics_preserve_position_types_and_one_input_pass(self):
        ctx, f = self.context(), self.f
        opened = []
        rows = [{"id": index, "amount": None if index % 3 == 0 else Decimal(index).scaleb(-2)}
                for index in range(4101)]
        def producer():
            opened.append("open")
            try:
                for start in range(0, len(rows), 137):
                    yield rows[start:start + 137]
            finally:
                opened.append("closed")
        frame = ctx.from_batches(producer, schema={"id": "int64", "amount": "decimal128(12,2)"}, streaming=True)
        before = frame.observe("source", rows=f.count(), present=f.count("amount"),
            nulls=f.sum(f.case_when(f.is_null("amount"), 1, 0)), total=f.sum("amount"),
            smallest=f.min("amount"), largest=f.max("amount"))
        final = before.filter(self.sl.col("id") >= 4097).observe("filtered", rows=f.count()).limit(2).observe(
            "selected", rows=f.count(), total=f.sum("amount"))
        self.assertEqual(opened, [])
        self.assertEqual(dict(final.schema())["amount"].parameters, (12, 2))
        self.assertEqual(opened, [])
        with self.assertRaises(self.sl.ShardLoomCommandError):
            final.collect(memory_bytes=1024, max_parallelism=1)
        self.assertEqual(opened, [])
        result = final.collect()
        self.assertEqual(result.to_rows(), tuple(rows[4097:4099]))
        self.assertEqual(opened, ["open", "closed"])
        self.assertEqual(result.observations, {
            "source": {"rows": 4101, "present": 2734, "nulls": 1367,
                "total": sum(row["amount"] or Decimal(0) for row in rows),
                "smallest": Decimal("0.01"), "largest": Decimal("41.00")},
            "filtered": {"rows": 4}, "selected": {"rows": 2, "total": Decimal("40.97")},
        })
        ctx.close()
        self.assertEqual(result.observations["source"]["rows"], 4101)

    def test_connected_files_catalog_parameters_window_observed_write_and_reopen(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        ctx, f = self.context(), self.f
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            facts, dimension, target = root / "facts.csv", root / "dimension.parquet", root / "output.vortex"
            facts.write_text("id,value\n0,0\n1,10\n2,20\n3,30\n", encoding="utf-8")
            pq.write_table(pa.table({"id": [1, 2, 3], "label": ["one", "two", "three"]}), dimension)
            source = ctx.read_csv(facts).filter(self.sl.col("id") >= 1)
            ctx.register("facts", source)
            query = ctx.sql("WITH a AS (SELECT id,value FROM facts), b(x,total) AS "
                "(SELECT id,value+:extra FROM a) SELECT x AS id,total FROM b", parameters={"extra": 5})
            frame = query.join(ctx.read_parquet(dimension), on="id").select(
                "f.id AS id", "f.total AS total", "d.label AS label").sort("id")
            frame = frame.select("id", "total", "label",
                f.row_number().over(self.sl.Window.order_by("id")).alias("position"))
            frame = frame.observe("matched", rows=f.count(), total=f.sum("total")).filter(
                self.sl.col("id") >= 2).observe("kept", rows=f.count(), total=f.sum("total"))
            frame = frame.limit(1).observe("published", rows=f.count())
            self.assertFalse(target.exists())
            self.assertFalse(ctx.client._native_backend.session.snapshot()["native_owner_initialized"])
            receipt = frame.write_vortex(target)
            self.assertEqual(receipt.output_commit_status, "committed")
            self.assertEqual(receipt.observations, {"matched": {"rows": 3, "total": 75},
                "kept": {"rows": 2, "total": 60}, "published": {"rows": 1}})
            self.assertEqual(ctx.read_vortex(target).collect().to_rows(),
                ({"id": 2, "total": 25, "label": "two", "position": 2},))

    def test_empty_output_keeps_prior_observation_and_empty_metric_nulls(self):
        ctx, f = self.context(), self.f
        frame = ctx.range(0, 4, column="id").observe("before", rows=f.count(), total=f.sum("id"))
        result = frame.limit(0).observe("empty", rows=f.count(), total=f.sum("id"),
            smallest=f.min("id"), largest=f.max("id")).collect()
        self.assertEqual(result.to_rows(), ())
        self.assertEqual(result.observations, {"before": {"rows": 4, "total": 6},
            "empty": {"rows": 0, "total": None, "smallest": None, "largest": None}})
        for _ in range(2):
            self.assertEqual(frame.collect().observations["before"], {"rows": 4, "total": 6})

    def test_dropna_preserves_prior_observation_and_renamed_values(self):
        for rename in (False, True):
            with self.subTest(rename=rename):
                ctx = self.context()
                source = ctx.from_rows([{"id": 1, "amount": None}, {"id": 2, "amount": 7}],
                    schema={"id": "int64", "amount": "int64"})
                frame = source.filter("id > 0").observe("before", rows=self.f.count())
                name = "renamed" if rename else "amount"
                if rename:
                    frame = frame.rename({"amount": name})
                result = frame.dropna(subset=[name]).observe("after", rows=self.f.count()).collect()
                self.assertEqual(result.to_rows(), ({"id": 2, name: 7},))
                self.assertEqual(result.observations, {"before": {"rows": 2}, "after": {"rows": 1}})

    def test_invalid_and_duplicate_metrics_fail_without_opening_input(self):
        ctx, f = self.context(), self.f
        opened = []
        def producer():
            opened.append(True)
            yield [{"id": 1}]
        source = ctx.from_batches(producer, schema={"id": "int64"}, streaming=True)
        for metric in ("id", "COUNT(DISTINCT id)", "AVG(id)", "SUM((SELECT 1 AS x))"):
            with self.subTest(metric=metric), self.assertRaises((ValueError, self.sl.ShardLoomCommandError)):
                source.observe("invalid", metric)
        observed = source.observe("quality", rows=f.count())
        with self.assertRaises((ValueError, self.sl.ShardLoomCommandError)):
            observed.observe("QUALITY", rows=f.count())
        with self.assertRaises((ValueError, self.sl.ShardLoomCommandError)):
            ctx.sql("SELECT * FROM a UNION ALL SELECT * FROM b", relations={"a": observed, "b": observed})
        self.assertEqual(opened, [])

    def test_late_producer_failure_and_failed_publication_never_complete_metrics(self):
        ctx, f = self.context(), self.f
        events = []
        def failing():
            try:
                yield [{"id": 1}]
                raise LookupError("late observation input failure")
            finally:
                events.append("closed")
        frame = ctx.from_batches(failing, schema={"id": "int64"}, streaming=True).observe("quality", rows=f.count())
        with self.assertRaisesRegex(LookupError, "late observation input failure"):
            frame.limit(0).collect()
        self.assertEqual(events, ["closed"])
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "result.vortex"
            def interfering():
                yield [{"id": 1}]
                target.write_bytes(b"independent destination")
            frame = ctx.from_batches(interfering, schema={"id": "int64"}, streaming=True).observe("quality", rows=f.count())
            report = frame.write_vortex(target, check=False)
            self.assertTrue(report.is_error)
            self.assertEqual(report.observations, {})
            self.assertEqual(target.read_bytes(), b"independent destination")
            self.assertEqual(list(Path(directory).iterdir()), [target])

    def test_early_stream_close_and_cancellation_do_not_publish_metrics(self):
        ctx, f = self.context(), self.f
        for cancel in (False, True):
            events = []
            def producer():
                try:
                    for index in range(5):
                        yield [{"id": index}]
                finally:
                    events.append("closed")
            frame = ctx.from_batches(producer, schema={"id": "int64"}, streaming=True).observe("quality", rows=f.count())
            stream = frame.iter_batches(batch_rows=1)
            retained = next(stream)
            if cancel:
                ctx.cancel()
                with self.assertRaises(self.sl.ShardLoomCommandError) as failure:
                    next(stream)
                self.assertEqual(failure.exception.diagnostics[0].code, "SL_EXECUTION_CANCELLED")
            stream.close()
            self.assertIsNone(stream.report)
            self.assertEqual(events, ["closed"])
            self.assertEqual(retained.to_rows(), ({"id": 0},))


if __name__ == "__main__":
    unittest.main()
