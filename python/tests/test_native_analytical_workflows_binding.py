# SPDX-License-Identifier: Apache-2.0
"""Complete local analytical workflows with independently specified results."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo

if __package__:
    from .test_native_binding import _load_native
    from .test_native_recursive_composition_binding import describe
else:
    from test_native_binding import _load_native
    from test_native_recursive_composition_binding import describe


class NativeAnalyticalWorkflowTests(unittest.TestCase):
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
        directory = tempfile.TemporaryDirectory(prefix="shardloom-analytical-workflows-")
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.inputs = []
        self.context = self.sl.context(transport="native", memory_bytes=128 << 20,
                                       max_parallelism=1)
        self.addCleanup(self.context.close)

    def source(self, table, name):
        import pyarrow.parquet as parquet
        path = self.root / (name + ".parquet")
        parquet.write_table(table, path, row_group_size=2)
        self.inputs.append(path)
        return self.context.read_parquet(path), path

    def equal(self, actual, expected):
        self.assertTrue(actual.schema.equals(expected.schema, check_metadata=True),
                        (actual.schema, expected.schema))
        self.assertEqual(describe(actual), describe(expected))

    def complete(self, cases):
        """Collect, stream, persist and independently read every declared result."""
        outputs, retained = [], []
        for name, frame, expected in cases:
            with self.subTest(workflow=name):
                result = frame.collect(check=True)
                self.assertEqual(result.envelope.field("fallback_attempted"), "false")
                self.assertEqual(result.envelope.field("external_engine_invoked"), "false")
                self.equal(result.to_arrow(), expected)
                retained.append((result, expected))
                batches = []
                with frame.iter_batches(batch_rows=2) as stream:
                    for batch in stream:
                        table = batch.to_arrow()
                        self.assertLessEqual(table.num_rows, 2)
                        self.assertTrue(table.schema.equals(expected.schema, check_metadata=True),
                                        (table.schema, expected.schema))
                        batches.extend(table.to_batches())
                self.equal(self.pa.Table.from_batches(batches, schema=expected.schema), expected)
                native_path, arrow_path = self.root / (name + ".vortex"), self.root / (name + ".arrow")
                for method, path in (("write_vortex", native_path), ("write_arrow_ipc", arrow_path)):
                    report = getattr(frame, method)(path, check=True)
                    self.assertEqual(report.output_commit_status, "committed")
                    self.assertEqual(report.envelope.field("fallback_attempted"), "false")
                    self.assertEqual(report.envelope.field("external_engine_invoked"), "false")
                outputs.append((native_path, arrow_path, expected))
        self.context.close()
        for path in self.inputs:
            path.unlink()
        for result, expected in retained:
            self.equal(result.to_arrow(), expected)
        with self.sl.context(transport="native", memory_bytes=128 << 20,
                             max_parallelism=1) as reader:
            for native_path, arrow_path, expected in outputs:
                self.equal(reader.read_vortex(native_path).collect(check=True).to_arrow(), expected)
                with self.pa.ipc.open_file(arrow_path) as independent:
                    self.equal(independent.read_all(), expected)

    def test_per_fund_statistics_complete_nullable_workflow(self):
        from shardloom import functions as f
        pa = self.pa
        rows = [{"fund": fund, "amount": amount} for fund, amount in
                [("A", 1.0), ("A", 5.0), ("A", None), ("B", None),
                 ("C", 7.0), (None, -1.0), (None, 1.0)]]
        schema = pa.schema([pa.field("fund", pa.string()), pa.field("amount", pa.float64())])
        source, path = self.source(pa.Table.from_pylist(rows, schema=schema), "funds")
        frame = source.group_by("fund").agg(observed=f.count("amount"), mean=f.avg("amount"),
            variance=f.var_pop("amount"), deviation=f.stddev_pop("amount")).sort("fund", nulls="last")
        sql = self.context.sql("SELECT fund,COUNT(amount) AS observed,AVG(amount) AS mean,"
            "VAR_POP(amount) AS variance,STDDEV_POP(amount) AS deviation "
            f"FROM '{path}' GROUP BY fund ORDER BY fund NULLS LAST")
        expected = pa.Table.from_pylist([
            dict(fund="A", observed=2, mean=3.0, variance=4.0, deviation=2.0),
            dict(fund="B", observed=0, mean=None, variance=None, deviation=None),
            dict(fund="C", observed=1, mean=7.0, variance=0.0, deviation=0.0),
            dict(fund=None, observed=2, mean=0.0, variance=1.0, deviation=1.0),
        ], schema=pa.schema([pa.field("fund", pa.string()), pa.field("observed", pa.uint64(), nullable=False),
            *[pa.field(name, pa.float64()) for name in ("mean", "variance", "deviation")]]))
        self.complete([("fund-dataframe", frame, expected), ("fund-sql", sql, expected)])

    def test_entity_currency_asof_valuation_complete_workflow(self):
        pa = self.pa
        positions = [
            dict(id=1, entity="X", currency="USD", instant=15, quantity=2, payload=["one", None]),
            dict(id=2, entity="X", currency="EUR", instant=25, quantity=3, payload=[]),
            dict(id=3, entity="Y", currency="USD", instant=15, quantity=4, payload=None),
            dict(id=4, entity="X", currency="USD", instant=5, quantity=1, payload=["early"]),
            dict(id=5, entity=None, currency="USD", instant=15, quantity=5, payload=["null key"]),
            dict(id=6, entity="X", currency="USD", instant=None, quantity=1, payload=["null time"]),
        ]
        quotes = [
            dict(quote_id=101, entity="X", currency="USD", instant=10, price=1.5),
            dict(quote_id=102, entity="X", currency="USD", instant=10, price=2.0),
            dict(quote_id=103, entity="X", currency="EUR", instant=20, price=4.0),
            dict(quote_id=104, entity="Y", currency="USD", instant=20, price=9.0),
        ]
        keys = [pa.field("entity", pa.string()), pa.field("currency", pa.string(), nullable=False),
                pa.field("instant", pa.timestamp("ns", "UTC"))]
        left_schema = pa.schema([pa.field("id", pa.int64(), nullable=False), *keys,
            pa.field("quantity", pa.int64(), nullable=False), pa.field("payload", pa.list_(pa.string()))])
        right_schema = pa.schema([pa.field("quote_id", pa.int64(), nullable=False), *keys,
                                 pa.field("price", pa.float64(), nullable=False)])
        left, left_path = self.source(pa.Table.from_pylist(positions, schema=left_schema), "positions")
        right, right_path = self.source(pa.Table.from_pylist(quotes, schema=right_schema), "quotes")
        projection = ["f.id AS id", "f.quantity AS quantity", "d.quote_id AS quote_id",
                      "d.price AS price", "f.quantity * d.price AS valuation", "f.payload AS payload"]
        frame = left.join_asof(right, on="instant", by=["entity", "currency"], strategy="backward").select(
            *projection).sort("id")
        sql = self.context.sql("SELECT " + ",".join(projection) +
            f" FROM '{left_path}' AS f ASOF LEFT JOIN '{right_path}' AS d "
            "MATCH_CONDITION (f.instant >= d.instant) ON f.entity=d.entity AND f.currency=d.currency ORDER BY f.id")
        reference = []
        for position in positions:
            eligible = [(quote["instant"], index, quote) for index, quote in enumerate(quotes)
                        if position["entity"] is not None and position["instant"] is not None
                        and (quote["entity"], quote["currency"]) == (position["entity"], position["currency"])
                        and quote["instant"] <= position["instant"]]
            selected = max(eligible, key=lambda item: item[:2])[2] if eligible else {}
            price = selected.get("price")
            reference.append(dict(id=position["id"], quantity=position["quantity"],
                quote_id=selected.get("quote_id"), price=price,
                valuation=None if price is None else position["quantity"] * price, payload=position["payload"]))
        expected = pa.Table.from_pylist(reference, schema=pa.schema([
            left_schema.field("id"), left_schema.field("quantity"), pa.field("quote_id", pa.int64()),
            pa.field("price", pa.float64()), pa.field("valuation", pa.float64()), left_schema.field("payload")]))
        self.complete([("valuation-dataframe", frame, expected), ("valuation-sql", sql, expected)])

    def test_daily_report_preserves_spring_and_fall_calendar_days(self):
        from shardloom import functions as f
        pa, sl = self.pa, self.sl
        zone = ZoneInfo("America/New_York")
        instants = [
            "2024-03-09T23:30:00-05:00", "2024-03-10T01:30:00-05:00",
            "2024-03-10T03:30:00-04:00", "2024-03-11T00:30:00-04:00",
            "2024-11-03T01:30:00-04:00", "2024-11-03T01:30:00-05:00",
            "2024-11-04T00:30:00-05:00", None,
        ]
        amounts = [3, 5, None, 7, 11, 13, 17, 99]
        moments = [None if text is None else datetime.fromisoformat(text) for text in instants]
        source, path = self.source(pa.Table.from_arrays([
            pa.array(["fund/東京"] * len(moments)), pa.array(moments, pa.timestamp("ns", zone.key)),
            pa.array(amounts, pa.int64()),
        ], schema=pa.schema([pa.field("fund", pa.string(), nullable=False),
                            pa.field("instant", pa.timestamp("ns", zone.key)), pa.field("amount", pa.int64())])), "daily")
        frame = source.where("instant IS NOT NULL").select("fund", "amount",
            sl.col("instant").temporal_trunc("day").alias("day")).group_by("fund", "day").agg(
                total=f.sum("amount"), observed=f.count("amount"), events=f.count())
        frame = frame.select("fund", "day", "total", "observed", "events",
            sl.col("day").calendar_add_days(1).timestamp_difference(sl.col("day")).alias("day_length")).sort("fund", "day")
        sql = self.context.sql("WITH bucketed AS (SELECT fund,temporal_trunc(instant,'day') AS day,amount "
            f"FROM '{path}' WHERE instant IS NOT NULL), daily AS (SELECT fund,day,SUM(amount) AS total,"
            "COUNT(amount) AS observed,COUNT(*) AS events FROM bucketed GROUP BY fund,day) "
            "SELECT fund,day,total,observed,events,timestamp_difference(calendar_add_days(day,1),day) AS day_length "
            "FROM daily ORDER BY fund,day")
        groups = {}
        for moment, amount in zip(moments, amounts):
            if moment is not None:
                groups.setdefault(moment.astimezone(zone).date(), []).append(amount)
        reference = []
        for day, values in sorted(groups.items()):
            start = datetime(day.year, day.month, day.day, tzinfo=zone)
            following = day + timedelta(days=1)
            end = datetime(following.year, following.month, following.day, tzinfo=zone)
            present = [value for value in values if value is not None]
            reference.append(dict(fund="fund/東京", day=start, total=float(sum(present)),
                observed=len(present), events=len(values), day_length=int(end.timestamp() - start.timestamp()) * 10**9))
        self.assertEqual({row["day_length"] // 10**9 for row in reference}, {82800, 86400, 90000})
        expected = pa.Table.from_pylist(reference, schema=pa.schema([
            pa.field("fund", pa.string(), nullable=False), pa.field("day", pa.timestamp("ns", zone.key)),
            pa.field("total", pa.float64()), pa.field("observed", pa.uint64(), nullable=False),
            pa.field("events", pa.uint64(), nullable=False),
            pa.field("day_length", pa.duration("ns"), metadata={
                b"ARROW:extension:name": b"shardloom.duration.v1"})]))
        self.complete([("daily-dataframe", frame, expected), ("daily-sql", sql, expected)])

    def test_parameterized_multi_cte_matches_composed_dataframe_and_reopens(self):
        from shardloom import functions as f
        pa = self.pa
        attributes = pa.struct([pa.field("label", pa.string()), pa.field("ranks", pa.list_(pa.int16()))])
        rows = [dict(id=index, fund=fund, amount=amount, attributes=payload) for index, fund, amount, payload in (
            (1, "A", 1, {"label": "excluded", "ranks": []}),
            (2, "A", 3, {"label": "東京", "ranks": [2, None]}),
            (3, "A", 5, None), (4, "B", 7, {"label": "", "ranks": []}),
            (5, None, 9, {"label": None, "ranks": [5]}))]
        source, path = self.source(pa.Table.from_pylist(rows, schema=pa.schema([
            pa.field("id", pa.int64(), nullable=False), pa.field("fund", pa.string()),
            pa.field("amount", pa.int64(), nullable=False), pa.field("attributes", attributes)])), "nested")
        statement = ("WITH selected AS MATERIALIZED (SELECT * FROM '" + str(path) + "' WHERE amount>=:floor), "
            "report AS (SELECT fund,SUM(amount) AS total,COUNT(*) AS n FROM selected GROUP BY fund), "
            "joined AS (SELECT s.id AS id,r.total AS total,r.n AS n,s.attributes AS attributes "
            "FROM selected AS s JOIN report AS r ON s.fund=r.fund) "
            "SELECT id,total,n,attributes FROM joined WHERE n>=:minimum ORDER BY id DESC")
        cases = []
        for floor, minimum in ((2, 2), (5, 1)):
            selected = source.where(f"amount >= {floor}")
            grouped = selected.group_by("fund").agg(total=f.sum("amount"), n=f.count())
            frame = selected.join(grouped, on="fund").select(
                "f.id AS id", "d.total AS total", "d.n AS n", "f.attributes AS attributes").where(
                    f"n >= {minimum}").sort("id", descending=True)
            sql = self.context.sql(statement, parameters={"floor": floor, "minimum": minimum})
            reference = []
            for row in reversed(rows):
                peers = [other for other in rows if other["fund"] == row["fund"] and other["amount"] >= floor]
                if row["fund"] is not None and row["amount"] >= floor and len(peers) >= minimum:
                    reference.append(dict(id=row["id"], total=float(sum(other["amount"] for other in peers)),
                                          n=len(peers), attributes=row["attributes"]))
            expected = pa.Table.from_pylist(reference, schema=pa.schema([
                pa.field("id", pa.int64(), nullable=False), pa.field("total", pa.float64()),
                pa.field("n", pa.uint64(), nullable=False), pa.field("attributes", attributes)]))
            cases.extend([(f"cte-dataframe-{floor}", frame, expected), (f"cte-sql-{floor}", sql, expected)])
        self.complete(cases)


if __name__ == "__main__":
    unittest.main()
