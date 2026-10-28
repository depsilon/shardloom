# SPDX-License-Identifier: Apache-2.0
"""Current typed unary and provider coverage beyond the ClickBench workload."""

from __future__ import annotations

from fractions import Fraction
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

if __package__:
    from .test_native_expression_binding import _load_native
    from .test_native_unary_binding import ROWS, KINDS, SCHEMA, arrow_schema, expected
else:
    from test_native_expression_binding import _load_native
    from test_native_unary_binding import ROWS, KINDS, SCHEMA, arrow_schema, expected


def typed_unary(source, kind):
    if kind == "distinct":
        return source.unary("distinct_rows")
    if kind.startswith(("dedup-", "mask-")):
        operation = "drop_duplicates" if kind.startswith("dedup-") else "duplicated"
        return source.unary(operation, "n", keep=kind.split("-", 1)[1])
    if kind == "tail":
        return source.unary("tail", "3")
    if kind == "sample":
        return source.unary("sample", '{"n":3,"seed":7}')
    if kind == "expression":
        columns = ["n", "s"]
        return source.unary("rewrite", json.dumps({
            "columns": columns,
            "rewrites": [{"kind": "forward_fill_null", "target_column": name} for name in columns],
        })).unary("rewrite", json.dumps({
            "columns": columns,
            "rewrites": [{"kind": "row_number", "target_column": "index"}],
        }))
    if kind == "melt":
        return source.unary("melt", '{"id_columns":["s"],"value_columns":["n"]}')
    if kind == "rolling":
        return source.unary("rolling", json.dumps({
            "source_column": "n", "output_column": "total", "window_size": 3,
            "min_periods": 1, "center": True, "aggregate": "sum",
        }))
    assert kind == "explode"
    return source.unary("explode", '{"column":"items"}')


def statistical_reference(rows):
    """Independent exact rational moments before the public Float64 boundary."""
    from fractions import Fraction
    import math
    numbers = [Fraction(row["number"]) for row in rows if row["number"] is not None]
    pairs = [(Fraction(row["number"]), Fraction(row["paired"])) for row in rows
             if row["number"] is not None and row["paired"] is not None]
    mean = sum(numbers) / len(numbers) if numbers else Fraction(0)
    total = sum((value - mean) ** 2 for value in numbers)
    population = float(total / len(numbers)) if numbers else None
    sample = float(total / (len(numbers) - 1)) if len(numbers) > 1 else None
    mx = sum(x for x, _ in pairs) / len(pairs) if pairs else Fraction(0)
    my = sum(y for _, y in pairs) / len(pairs) if pairs else Fraction(0)
    cross = sum((x - mx) * (y - my) for x, y in pairs)
    xx = sum((x - mx) ** 2 for x, _ in pairs)
    yy = sum((y - my) ** 2 for _, y in pairs)
    return {
        "var_pop": population, "var_samp": sample,
        "stddev_pop": math.sqrt(population) if population is not None else None,
        "stddev_samp": math.sqrt(sample) if sample is not None else None,
        "covar_pop": float(cross / len(pairs)) if pairs else None,
        "covar_samp": float(cross / (len(pairs) - 1)) if len(pairs) > 1 else None,
        "corr": float(cross) / math.sqrt(float(xx * yy)) if xx and yy else None,
    }


class NativeSharedProviderBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()
        import pyarrow as pa
        cls.pa = pa

    def setUp(self):
        self.real_popen = subprocess.Popen
        self.process = patch("subprocess.Popen", side_effect=AssertionError("external process"))
        self.process.start()
        self.addCleanup(self.process.stop)
        self.session = self.native.Session(self.native.ExecutionResources(128 << 20, 2))
        self.addCleanup(self.session.close)

    def table(self, result):
        self.assertEqual(result.report["fallback_attempted"], "false")
        self.assertEqual(result.report["external_engine_invoked"], "false")
        return self.pa.RecordBatchReader.from_stream(result).read_all()

    def all_surfaces(self, frame, relation, statement, report_checks=None):
        """Execute complete declarations through five real public entry points."""
        import shardloom as sl

        def check(get_field):
            for name, expected_value in (report_checks or {}).items():
                self.assertEqual(get_field(name), expected_value, name)

        result = frame.collect(check=True)
        self.assertFalse(result.envelope.fallback.attempted)
        self.assertEqual(result.envelope.field("external_engine_invoked"), "false")
        check(result.envelope.field)
        tables = [result.to_arrow()]
        for plan in (self.session.plan(relation), self.session.sql(statement)):
            result = plan.collect()
            check(result.report.get)
            tables.append(self.table(result))
        arguments = ["sql", "--sql", statement, "--request", "collect", "--bounded", "true",
                     "--memory-bytes", str(128 << 20), "--max-parallelism", "2"]
        result = self.session.workflow(arguments).collect()
        check(result.report.get)
        tables.append(self.table(result))
        # Native front doors above must not launch a process. The fifth entry
        # point explicitly exercises the CLI bundled in the installed wheel,
        # or an exact local test binary selected by the caller.
        with patch("subprocess.Popen", self.real_popen):
            cli = sl.ShardLoomClient(binary=os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY"),
                                    transport="subprocess", use_persistent_worker=False,
                                    memory_bytes=128 << 20, max_parallelism=2, timeout=30)
            try:
                result = sl.sql(statement, client=cli).collect(check=True)
                self.assertFalse(result.envelope.fallback.attempted)
                self.assertEqual(result.envelope.field("external_engine_invoked"), "false")
                check(result.envelope.field)
                tables.append(result.to_arrow())
            finally:
                cli.close()
        return tables

    def test_rich_bounded_ordering_preserves_values_and_schema_on_all_five_surfaces(self):
        from decimal import Decimal
        import shardloom as sl
        import pyarrow.parquet as parquet

        pa, native = self.pa, self.native
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        rows = [{
            "id": index,
            "key": None if index % 13 == 0 else (index * 37) % 41 - 20,
            "amount": None if index % 17 == 0 else Decimal(index * 29 - 901).scaleb(-3),
            "payload": None if index % 11 == 0 else {
                "items": [] if index % 5 == 0 else ["λ\0猫" * 16, None, str(index)],
                "label": "text-" + str(index % 7),
            },
            "instant": None if index % 19 == 0 else index * 1_000_003 - 11_000_000,
        } for index in range(2053)]
        schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("key", pa.int64()),
            pa.field("amount", pa.decimal128(18, 3)),
            pa.field("payload", pa.struct([
                pa.field("items", pa.list_(pa.string())), pa.field("label", pa.string(), nullable=False)])),
            pa.field("instant", pa.timestamp("us", tz="America/New_York")),
        ])
        source = pa.Table.from_pylist(rows, schema=schema)
        with tempfile.TemporaryDirectory(prefix="shardloom-rich-prefix-") as directory:
            path = Path(directory) / "rich.parquet"
            parquet.write_table(source, path, row_group_size=257)
            for column in ("key", "amount"):
                # Python's stable sort provides the independent row-order oracle.
                positions = sorted(range(len(rows)), key=lambda index: (
                    rows[index][column] is None,
                    rows[index][column] if rows[index][column] is not None else 0))
                for count, offset in ((0, 0), (7, 3), (16_385, 0)):
                    with self.subTest(column=column, count=count, offset=offset):
                        selected = positions[offset:offset + count]
                        expected = source.take(pa.array(selected, type=pa.int64()))
                        frame = sl.read_parquet(path, client=client).sort(column, nulls="last").limit(count, offset=offset)
                        relation = native.Relation.scan(str(path)).sort([(column, False, "last")]).limit(count, offset=offset)
                        statement = f"SELECT * FROM '{path}' ORDER BY {column} NULLS LAST LIMIT {count} OFFSET {offset}"
                        checks = {
                            "native_input_end_observed": "true",
                            "native_input_output_ownership_detached": "true",
                            "native_input_ordering_prefix_rows_validated": str(len(rows) if count <= 16_384 else 0),
                        }
                        for result in self.all_surfaces(frame, relation, statement, checks):
                            self.assertTrue(result.schema.equals(expected.schema, check_metadata=False),
                                            (result.schema, expected.schema))
                            self.assertEqual(result.to_pylist(), expected.to_pylist())

    def test_temporal_calendar_workflows_match_all_five_surfaces(self):
        import shardloom as sl
        import pyarrow.parquet as parquet

        native, session, pa = self.native, self.session, self.pa
        expr = native.Expression
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        zone = pa.timestamp("s", tz="America/New_York")
        inputs = [
            ("instant", zone, [1_710_046_800, 1_730_606_400, None]),
            ("span", pa.duration("s"), [86_400, 86_400, None]),
            ("naive", pa.timestamp("s"), [0, 0, None]),
            ("date", pa.date64(), [1_706_659_200_000, 1_706_659_200_000, None]),
        ]
        table = pa.Table.from_arrays([pa.array(values, type=kind) for _, kind, values in inputs],
                                    names=[name for name, _, _ in inputs])
        expected = [
            ("hour", pa.int64(), [0, 0, None]),
            ("day", zone, [1_710_046_800, 1_730_606_400, None]),
            ("calendar", zone, [1_710_129_600, 1_730_696_400, None]),
            ("fixed", zone, [1_710_133_200, 1_730_692_800, None]),
            ("back", zone, [1_709_960_400, 1_730_520_000, None]),
            ("elapsed", pa.duration("s"), [82_800, 90_000, None]),
            ("utc", pa.timestamp("s", tz="UTC"), [1_710_046_800, 1_730_606_400, None]),
            ("civil", pa.timestamp("s"), [1_710_028_800, 1_730_592_000, None]),
            ("assigned", zone, [18_000, 18_000, None]),
            ("month", pa.date64(), [1_709_164_800_000, 1_709_164_800_000, None]),
        ]
        c = sl.col("instant")
        expressions = [
            c.temporal_extract("hour"), c.temporal_trunc("day"), c.calendar_add_days(1),
            c.temporal_add_duration(sl.col("span")), c.temporal_sub_duration(sl.col("span")),
            c.calendar_add_days(1).timestamp_difference(c), c.timestamp_convert_timezone("UTC"),
            c.timestamp_remove_timezone(), sl.col("naive").timestamp_assign_timezone("America/New_York"),
            sl.col("date").calendar_add_months(1, month_end="clamp"),
        ]
        n = expr.column("instant")
        typed = [
            expr.call("temporal_extract", [n, expr.literal("hour")]),
            expr.call("temporal_trunc", [n, expr.literal("day")]),
            expr.call("calendar_add_days", [n, expr.literal(1)]),
            expr.call("temporal_add_duration", [n, expr.column("span")]),
            expr.call("temporal_sub_duration", [n, expr.column("span")]),
            expr.call("timestamp_difference", [expr.call("calendar_add_days", [n, expr.literal(1)]), n]),
            expr.call("timestamp_convert_timezone", [n, expr.literal("UTC")]),
            expr.call("timestamp_remove_timezone", [n]),
            expr.call("timestamp_assign_timezone", [expr.column("naive"), expr.literal("America/New_York")]),
            expr.call("calendar_add_months", [expr.column("date"), expr.literal(1), expr.literal("clamp")]),
        ]
        sql_expressions = [
            "temporal_extract(instant,'hour')", "temporal_trunc(instant,'day')", "calendar_add_days(instant,1)",
            "temporal_add_duration(instant,span)", "temporal_sub_duration(instant,span)",
            "timestamp_difference(calendar_add_days(instant,1),instant)",
            "timestamp_convert_timezone(instant,'UTC')", "timestamp_remove_timezone(instant)",
            "timestamp_assign_timezone(naive,'America/New_York')", "calendar_add_months(date,1,'clamp')",
        ]
        with tempfile.TemporaryDirectory(prefix="shardloom-temporal-surfaces-") as directory:
            for empty in (False, True):
                data = table.slice(0, 0) if empty else table
                for fmt, reader in (("vortex", sl.read_vortex), ("parquet", sl.read_parquet),
                                    ("arrow_ipc", sl.read_arrow_ipc)):
                    with self.subTest(empty=empty, format=fmt):
                        extension = "arrow" if fmt == "arrow_ipc" else fmt
                        path = Path(directory) / f"input-{empty}.{extension}"
                        if fmt == "vortex":
                            source = session.from_arrow(data)
                            session.plan(native.Relation.scan("memory://temporal"),
                                sources=[("memory://temporal", source)]).write(path, "write_vortex")
                        elif fmt == "parquet":
                            parquet.write_table(data, path, row_group_size=1)
                        else:
                            with pa.OSFile(str(path), "wb") as output, pa.ipc.new_file(output, data.schema) as writer:
                                writer.write_table(data)
                        frame = reader(path, client=client).select(*[
                            expression.alias(name) for (name, _, _), expression in zip(expected, expressions, strict=True)])
                        relation = native.Relation.scan(str(path)).project([
                            (name, expression) for (name, _, _), expression in zip(expected, typed, strict=True)])
                        statement = "SELECT " + ",".join(
                            f"{expression} AS {name}" for (name, _, _), expression in zip(expected, sql_expressions, strict=True)
                        ) + f" FROM '{path}'"
                        # PyArrow writes second timestamps as Parquet milliseconds.
                        # Preserve that stored unit and the original zone, without
                        # reinterpreting its integer payload as seconds.
                        checks = expected
                        if fmt == "parquet":
                            checks = [(name, pa.timestamp("ms", tz=kind.tz) if pa.types.is_timestamp(kind)
                                       else pa.duration("ms") if name == "elapsed" else kind,
                                       [None if value is None else value * 1000 for value in values]
                                       if pa.types.is_timestamp(kind) or name == "elapsed" else values)
                                      for name, kind, values in expected]
                        for result in self.all_surfaces(frame, relation, statement):
                            self.assertEqual(result.column_names, [name for name, _, _ in expected])
                            for name, kind, values in checks:
                                self.assertEqual(result.schema.field(name).type, kind)
                                self.assertEqual(result[name].cast(pa.int64()).to_pylist(), [] if empty else values, name)

    def test_temporal_text_casts_preserve_whitespace_on_all_five_surfaces(self):
        import shardloom as sl

        pa, native = self.pa, self.native
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        source = pa.table({
            "day": [" \t1970-01-02\r", "\u20031969-12-31\u00a0", None],
            "instant": [" \n1970-01-01T00:00:00.000001Z\t", "\u20031969-12-31T23:59:59.999999Z\u00a0", None],
            "bad": ["1970- 01-01", "1970-01-01T00: 00:00Z", " \t"],
        })
        declarations = [
            ("day", "date32", False, [1, -1, None]),
            ("day", "date32", True, [1, -1, None]),
            ("instant", "timestamp_micros", False, [1, -1, None]),
            ("instant", "timestamp_micros", True, [1, -1, None]),
            ("bad", "date32", True, [None, None, None]),
            ("bad", "timestamp_micros", True, [None, None, None]),
        ]
        with tempfile.TemporaryDirectory(prefix="shardloom-temporal-text-") as directory:
            path = Path(directory) / "source.arrow"
            with pa.OSFile(str(path), "wb") as output, pa.ipc.new_file(output, source.schema) as writer:
                writer.write_table(source)
            frame = sl.read_arrow_ipc(path, client=client).select(*[
                (sl.col(column).try_cast(kind) if tolerant else sl.col(column).cast(kind)).alias(f"v{index}")
                for index, (column, kind, tolerant, _) in enumerate(declarations)
            ])
            relation = native.Relation.scan(str(path)).project([
                (f"v{index}", native.Expression.column(column).cast(kind, tolerant=tolerant))
                for index, (column, kind, tolerant, _) in enumerate(declarations)
            ])
            statement = "SELECT " + ",".join(
                f"{'TRY_CAST' if tolerant else 'CAST'}({column} AS {kind}) AS v{index}"
                for index, (column, kind, tolerant, _) in enumerate(declarations)
            ) + f" FROM '{path}'"
            for result in self.all_surfaces(frame, relation, statement):
                for index, (_, kind, _, expected) in enumerate(declarations):
                    physical = pa.int32() if kind == "date32" else pa.int64()
                    self.assertEqual(result[f"v{index}"].cast(physical).to_pylist(), expected)

    def test_temporal_python_rows_batches_and_retained_arrow_keep_exact_ticks(self):
        import shardloom as sl
        from shardloom import types as t
        from shardloom.errors import ShardLoomError

        pa = self.pa
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        declarations = [("date64", t.date64(), pa.date64(), -86_400_000, 86_400_000)]
        for unit in ("s", "ms", "us", "ns"):
            time_type = pa.time32(unit) if unit in ("s", "ms") else pa.time64(unit)
            declarations += [(f"time_{unit}", t.time(unit), time_type, 0, 1),
                (f"stamp_{unit}", t.timestamp(unit), pa.timestamp(unit), -(1 << 63), (1 << 63) - 1),
                (f"duration_{unit}", t.duration(unit), pa.duration(unit), -(1 << 63), (1 << 63) - 1)]
        declarations.append(("zoned", t.timestamp("ns", "America/New_York"),
                             pa.timestamp("ns", "America/New_York"), -1, 1))
        schema = {name: declared for name, declared, _, _, _ in declarations}
        rows = [{name: values[index] for name, _, _, *values in declarations} for index in (0, 1)]
        rows.append(dict.fromkeys(schema))
        for frame in (sl.from_rows(rows, schema=schema, client=client),
                      sl.from_batches([rows[:1], rows[1:]], schema=schema, client=client, streaming=True)):
            result = frame.collect(check=True)
            self.assertFalse(result.envelope.fallback.attempted)
            table = result.to_arrow()
            for name, _, kind, low, high in declarations:
                self.assertEqual(table.schema.field(name).type, kind)
                physical = pa.int32() if pa.types.is_time32(kind) else pa.int64()
                self.assertEqual(table[name].cast(physical).to_pylist(), [low, high, None], name)
        for name, value in (("date64", 1), ("time_ns", 86_400_000_000_000)):
            with self.subTest(invalid=name), self.assertRaises((ValueError, ShardLoomError)):
                sl.from_rows([{name: value}], schema={name: schema[name]}, client=client).collect(check=True)

    def test_temporal_nested_null_parents_persist_exact_types_and_ticks(self):
        import shardloom as sl
        from shardloom import types as t

        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        fields = {"date32": t.date32(), "date64": t.date64()}
        low = {"date32": -(1 << 31), "date64": -9_223_372_036_828_800_000}
        high = {"date32": (1 << 31) - 1, "date64": 9_223_372_036_828_800_000}
        for unit, scale in (("s", 1), ("ms", 1000), ("us", 1_000_000), ("ns", 1_000_000_000)):
            fields[f"time_{unit}"] = t.time(unit)
            low[f"time_{unit}"], high[f"time_{unit}"] = 0, 86_400 * scale - 1
            for name, declared in (("stamp", t.timestamp(unit)), ("span", t.duration(unit))):
                fields[f"{name}_{unit}"] = declared
                low[f"{name}_{unit}"], high[f"{name}_{unit}"] = -(1 << 63), (1 << 63) - 1
        fields["zoned"] = t.timestamp("ns", "America/New_York")
        low["zoned"], high["zoned"] = -1, 1
        schema = {"record": t.struct(fields), "spans": t.list_(t.duration("ns")),
                  "fixed": t.fixed_size_list(t.timestamp("ns", "America/New_York"), 2)}
        rows = [
            {"record": low, "spans": [-1, None, (1 << 63) - 1], "fixed": [-1, 1]},
            {"record": None, "spans": None, "fixed": None},
            {"record": dict.fromkeys(fields), "spans": [], "fixed": [None, None]},
            {"record": high, "spans": [-(1 << 63)], "fixed": [0, None]},
        ]
        frame = sl.from_rows(rows, schema=schema, client=client)
        result = frame.collect(check=True)
        self.assertEqual(result.result_schema, tuple(schema.items()))
        self.assertEqual(result.result_rows, tuple(rows))
        with tempfile.TemporaryDirectory(prefix="shardloom-temporal-nested-") as directory:
            for fmt, reader in (("vortex", sl.read_vortex), ("parquet", sl.read_parquet),
                                ("arrow-ipc", sl.read_arrow_ipc)):
                with self.subTest(format=fmt):
                    path = Path(directory) / f"nested.{fmt}"
                    written = frame.write(path, output_format=fmt, check=True)
                    self.assertFalse(written.envelope.fallback.attempted)
                    reopened = reader(path, client=client).collect(check=True)
                    self.assertEqual(reopened.result_schema, tuple(schema.items()))
                    self.assertEqual(reopened.result_rows, tuple(rows))

    def test_temporal_bad_streamed_values_preserve_destination_and_zone_admission(self):
        import shardloom as sl
        from shardloom.errors import ShardLoomError

        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        with tempfile.TemporaryDirectory(prefix="shardloom-temporal-validation-") as directory:
            target = Path(directory) / "prior.vortex"
            for name, invalid in (("date64", 1), ("time(s)", -1), ("time(ns)", 86_400_000_000_000)):
                demanded = []
                def batches():
                    demanded.append("valid")
                    yield [{"value": 0}]
                    demanded.append("invalid")
                    yield [{"value": invalid}]
                frame = sl.from_batches(batches, schema={"value": name}, client=client, streaming=True).limit(1)
                target.write_bytes(b"preserve existing destination")
                with self.subTest(dtype=name), self.assertRaises((ValueError, ShardLoomError)):
                    frame.write_vortex(target, check=True)
                self.assertEqual(demanded, [])
                self.assertEqual(target.read_bytes(), b"preserve existing destination")
                target.unlink()
                demanded.clear()
                destination = Path(directory) / f"rejected-{name}.vortex"
                with self.subTest(dtype=name, destination="new"), self.assertRaises((ValueError, ShardLoomError)):
                    frame.write_vortex(destination, check=True)
                self.assertEqual(demanded, ["valid", "invalid"])
                self.assertFalse(destination.exists())
                self.assertEqual(list(Path(directory).iterdir()), [])
            with self.assertRaises((ValueError, ShardLoomError)):
                sl.from_rows([{"value": 0}], schema={"value": "timestamp(ns,No/Such_Zone)"},
                             client=client).collect(check=True)
            # A rejected value must not poison the session's later operations.
            self.assertEqual(sl.from_rows([{"value": 0}], schema={"value": "duration(ns)"},
                                         client=client).collect(check=True).result_rows, ({"value": 0},))

    def test_grouping_sets_match_all_five_surfaces(self):
        import shardloom as sl
        from shardloom import functions as f
        import pyarrow.parquet as parquet

        native, session, pa = self.native, self.session, self.pa
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        rows = [{"id": i, "fund": ["A", "A", "B", None][i % 4],
                 "currency": ["USD", "EUR", "JPY"][i % 3], "value": i % 7,
                 "keep": None if i % 5 == 0 else i % 2 == 0, "unused": "not requested"}
                for i in range(19)]
        schema = pa.schema([pa.field("id", pa.uint64(), nullable=False),
            pa.field("fund", pa.string()), pa.field("currency", pa.string(), nullable=False),
            pa.field("value", pa.uint64(), nullable=False), pa.field("keep", pa.bool_()),
            pa.field("unused", pa.string(), nullable=False)])
        expected_schema = pa.schema([pa.field("fund", pa.string()), pa.field("currency", pa.string()),
            pa.field("mask", pa.uint64(), nullable=False), pa.field("n", pa.uint64(), nullable=False),
            pa.field("observed", pa.uint64(), nullable=False), pa.field("total", pa.float64()),
            pa.field("variance", pa.float64()), pa.field("items", pa.list_(pa.field("item", pa.uint64(), nullable=False)))])

        def reference(sets, empty):
            result = []
            for keys in sets:
                groups = {(): []} if not keys else {}
                for row in ([] if empty else rows):
                    groups.setdefault(tuple(row[key] for key in keys), []).append(row)
                for key, values in groups.items():
                    group = dict(zip(keys, key))
                    chosen = [row["value"] for row in reversed(values) if row["keep"] is True]
                    mean = Fraction(sum(chosen), len(chosen)) if chosen else Fraction(0)
                    variance = float(sum((Fraction(value) - mean) ** 2 for value in chosen) / len(chosen)) if chosen else None
                    result.append({"fund": group.get("fund"), "currency": group.get("currency"),
                        "mask": (2 if "fund" not in keys else 0) + (1 if "currency" not in keys else 0),
                        "n": len(values), "observed": sum(row["fund"] is not None for row in values),
                        "total": float(sum(chosen)) if chosen else None, "variance": variance,
                        "items": chosen or None})
            return sorted(result, key=lambda row: (row["mask"], row["fund"] is None, row["fund"] or "",
                                                   row["currency"] is None, row["currency"] or ""))

        with tempfile.TemporaryDirectory(prefix="shardloom-grouping-sets-") as directory:
            table = pa.Table.from_pylist(rows, schema=schema)
            for fmt in ("vortex", "parquet"):
                path = Path(directory) / f"input.{fmt}"
                if fmt == "vortex":
                    source = session.from_arrow(table)
                    session.plan(native.Relation.scan("memory://grouping-sets"),
                        sources=[("memory://grouping-sets", source)]).write(path, "write_vortex")
                else:
                    parquet.write_table(table, path, row_group_size=2)
                read = sl.read_vortex if fmt == "vortex" else sl.read_parquet
                full, fund, currency, total = ["fund", "currency"], ["fund"], ["currency"], []
                cases = [("sets", [full, fund, total, fund], False),
                         ("rollup", [full, fund, total], False),
                         ("cube", [full, fund, currency, total], False),
                         ("distinct", [full, fund, total], False),
                         ("empty", [full, total, total], True)]
                for kind, sets, empty in cases:
                    base = read(path, client=client)
                    typed = native.Relation.scan(str(path))
                    if empty:
                        base = base.filter(f.col("id") < 0)
                        typed = typed.filter(native.Expression.column("id") < native.Expression.literal(0))
                    if kind in {"rollup", "cube"}:
                        grouped = getattr(base, kind)("fund", "currency")
                        clause = f"{kind.upper()}(fund,currency)"
                    else:
                        declared_sets = [full, fund, total, fund] if kind == "distinct" else sets
                        grouped = base.grouping_sets(*declared_sets, distinct=kind == "distinct")
                        clause = ("DISTINCT " if kind == "distinct" else "") + "GROUPING SETS(" + ",".join("(" + ",".join(keys) + ")" for keys in declared_sets) + ")"
                    frame = grouped.agg(mask=f.grouping_id("fund", "currency"), n=f.count(),
                        observed=f.count("fund"), total=f.sum("value").filter(f.col("keep")),
                        variance=f.var_pop("value").filter(f.col("keep")),
                        items=f.list("value", order_by=[("id", True, "last")]).filter(f.col("keep")))
                    frame = frame.sort("mask", "fund", "currency", nulls="last")
                    relation = typed.aggregate(["fund", "currency"], [
                        native.AggregateMeasure("count", None, "n"), native.AggregateMeasure("count", "fund", "observed"),
                        native.AggregateMeasure("sum", "value", "total", filter_column="keep"),
                        native.AggregateMeasure("var_pop", "value", "variance", filter_column="keep"),
                        native.AggregateMeasure("list", "value", "items", filter_column="keep", order_by=[("id", True, "last")])],
                        grouping_sets=sets, grouping_indicators=[("mask", ["fund", "currency"])])
                    relation = relation.sort([(name, False, "last") for name in ("mask", "fund", "currency")])
                    sql = ("SELECT fund,currency,GROUPING_ID(fund,currency) AS mask,COUNT(*) AS n,COUNT(fund) AS observed,"
                           "SUM(value) FILTER (WHERE keep) AS total,VAR_POP(value) FILTER (WHERE keep) AS variance,"
                           f"LIST(value ORDER BY id DESC NULLS LAST) FILTER (WHERE keep) AS items FROM '{path}'"
                           + (" WHERE id < 0" if empty else "") + f" GROUP BY {clause} ORDER BY mask,fund NULLS LAST,currency NULLS LAST")
                    wanted = pa.Table.from_pylist(reference(sets, empty), schema=expected_schema)
                    demand = {"native_source_demand_projected_columns": "5", "native_source_demand_available_columns": "6"} if fmt == "parquet" else None
                    for actual in self.all_surfaces(frame, relation, sql, demand):
                        self.assertEqual(actual.schema, wanted.schema)
                        self.assertEqual(actual.num_rows, wanted.num_rows)
                        for got, expected in zip(actual.to_pylist(), wanted.to_pylist()):
                            if expected["variance"] is not None:
                                self.assertAlmostEqual(got["variance"], expected["variance"], delta=2e-12 * max(1, abs(expected["variance"])))
                                got["variance"] = expected["variance"]
                            self.assertEqual(got, expected, (fmt, kind))

    def test_aggregate_filters_match_all_five_surfaces(self):
        import shardloom as sl
        from shardloom import functions as f
        import pyarrow.parquet as parquet

        native, session, pa = self.native, self.session, self.pa
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        values = [None, 1, 1, 3, 5, 8, None, 9, None, None, 0, 1]
        texts = ["z", None, "東京", "", "a", "b", None, "b", None, None, "", ""]
        local_order = [3, 2, 1, None, 1, 2, 0, 2, 0, 1, None, 0]
        keep = [True, True, False, True, None, False, None, False, True, False, None, True]
        other = [False, False, True, False, True, True, False, False, False, False, True, False]
        rows = [{"id": i, "fund": "A" if i < 5 else "B" if i < 8 else "C" if i < 10 else "D",
                 "value": values[i], "text": texts[i], "local_order": local_order[i],
                 "keep": keep[i], "other": other[i], "unused": "not requested"}
                for i in range(12)]
        schema = pa.schema([pa.field("id", pa.uint64(), nullable=False),
            pa.field("fund", pa.string(), nullable=False), pa.field("value", pa.uint64()),
            pa.field("text", pa.string()), pa.field("local_order", pa.int32()),
            pa.field("keep", pa.bool_()), pa.field("other", pa.bool_()),
            pa.field("unused", pa.string(), nullable=False)])

        def reference(selected, predicate):
            chosen = [r for r in selected if r[predicate] is True]
            nums = [Fraction(r["value"]) for r in chosen if r["value"] is not None]
            if nums:
                mean = sum(nums, Fraction(0)) / len(nums)
                variance = float(sum(((n - mean) ** 2 for n in nums), Fraction(0)) / len(nums))
                ordered = sorted(nums)
                mid = Fraction(len(ordered) - 1, 2)
                lo = mid.numerator // mid.denominator
                median = float(ordered[lo] if lo == mid else (ordered[lo] + ordered[lo + 1]) / 2)
                total = float(sum(set(nums), Fraction(0)))
            else:
                variance = median = total = None
            ordered_rows = sorted(chosen, key=lambda r: (r["local_order"] is None,
                -(r["local_order"] or 0), r["id"]))
            observed_text = [r["text"] for r in ordered_rows if r["text"] is not None]
            return {"n": len(chosen), "total": total, "variance": variance,
                "median": median, "items": [r["value"] for r in ordered_rows] if chosen else None,
                "joined": "|".join(observed_text) if observed_text else None}

        def close(actual, expected):
            self.assertEqual(actual.column_names, expected.column_names)
            self.assertEqual(actual.schema, expected.schema)
            self.assertEqual(actual.num_rows, expected.num_rows)
            for got, wanted in zip(actual.to_pylist(), expected.to_pylist()):
                self.assertEqual(got.keys(), wanted.keys())
                for key in got:
                    if key in {"total", "variance", "other_variance", "median", "other_median"} and got[key] is not None:
                        self.assertAlmostEqual(got[key], wanted[key], delta=2e-12 * max(1.0, abs(wanted[key])))
                    else:
                        self.assertEqual(got[key], wanted[key], key)

        with tempfile.TemporaryDirectory(prefix="shardloom-aggregate-filters-") as directory:
            table = pa.Table.from_pylist(rows, schema=schema)
            for fmt in ("vortex", "parquet"):
                path = Path(directory) / f"input.{fmt}"
                if fmt == "vortex":
                    source = session.from_arrow(table)
                    session.plan(native.Relation.scan("memory://aggregate-filters"),
                        sources=[("memory://aggregate-filters", source)]).write(path, "write_vortex")
                else:
                    parquet.write_table(table, path, row_group_size=1)
                read = sl.read_vortex if fmt == "vortex" else sl.read_parquet
                measures = [
                    f.count().filter(f.col("keep")).alias("n"),
                    f.sum("value", distinct=True).filter(f.col("keep")).alias("total"),
                    f.var_pop("value").filter(f.col("keep")).alias("variance"),
                    f.var_pop("value").filter(f.col("other")).alias("other_variance"),
                    f.median("value").filter(f.col("keep")).alias("median"),
                    f.median("value").filter(f.col("other")).alias("other_median"),
                    f.list("value", order_by=[("local_order", True, "last")]).filter(f.col("keep")).alias("items"),
                    f.string_agg("text", "|", order_by=[("local_order", True, "last")]).filter(f.col("keep")).alias("joined"),
                ]
                frame = read(path, client=client).group_by("fund").agg(*measures).sort("fund")
                typed_measures = [native.AggregateMeasure("count", None, "n", filter_column="keep"),
                    native.AggregateMeasure("sum_distinct", "value", "total", filter_column="keep"),
                    native.AggregateMeasure("var_pop", "value", "variance", filter_column="keep"),
                    native.AggregateMeasure("var_pop", "value", "other_variance", filter_column="other"),
                    native.AggregateMeasure("median", "value", "median", filter_column="keep"),
                    native.AggregateMeasure("median", "value", "other_median", filter_column="other"),
                    native.AggregateMeasure("list", "value", "items", filter_column="keep", order_by=[("local_order", True, "last")]),
                    native.AggregateMeasure("string_agg", "text", "joined", separator="|", filter_column="keep", order_by=[("local_order", True, "last")])]
                relation = native.Relation.scan(str(path)).aggregate(["fund"], typed_measures).sort([("fund", False, None)])
                agg = "COUNT(*) FILTER (WHERE keep) AS n,SUM(DISTINCT value) FILTER (WHERE keep) AS total,VAR_POP(value) FILTER (WHERE keep) AS variance,VAR_POP(value) FILTER (WHERE other) AS other_variance,MEDIAN(value) FILTER (WHERE keep) AS median,MEDIAN(value) FILTER (WHERE other) AS other_median,LIST(value ORDER BY local_order DESC NULLS LAST) FILTER (WHERE keep) AS items,STRING_AGG(text, '|' ORDER BY local_order DESC NULLS LAST) FILTER (WHERE keep) AS joined"
                sql = f"SELECT fund,{agg} FROM '{path}' GROUP BY fund ORDER BY fund"
                expected_rows = [{"fund": fund, **reference([r for r in rows if r["fund"] == fund], "keep"),
                    "other_variance": reference([r for r in rows if r["fund"] == fund], "other")["variance"],
                    "other_median": reference([r for r in rows if r["fund"] == fund], "other")["median"]}
                    for fund in ("A", "B", "C", "D")]
                expected_schema = pa.schema([pa.field("fund", pa.string(), nullable=False),
                    pa.field("n", pa.uint64(), nullable=False), pa.field("total", pa.float64()),
                    pa.field("variance", pa.float64()), pa.field("other_variance", pa.float64()),
                    pa.field("median", pa.float64()), pa.field("other_median", pa.float64()),
                    pa.field("items", pa.list_(pa.field("item", pa.uint64(), nullable=True))),
                    pa.field("joined", pa.string())])
                expected_table = pa.Table.from_pylist(expected_rows, schema=expected_schema)
                group_demand = {"native_source_demand_projected_columns": "6",
                                "native_source_demand_available_columns": "8"} if fmt == "parquet" else None
                for actual in self.all_surfaces(frame, relation, sql, group_demand):
                    close(actual, expected_table)

                # Typed descriptor construction is exercised independently from SQL parsing.
                with self.assertRaises(ValueError):
                    native.WindowExpression("row_number", "bad", filter_column="keep")

                spec = sl.Window.partition_by("fund").order_by("id").rows_between(-1, 1)
                window_frame = native.WindowFrame("rows", ("preceding", native.FrameOffset.number(
                    native.Expression.literal(1))), ("following", native.FrameOffset.number(
                    native.Expression.literal(1))))
                window_exprs = [
                    f.count().filter(f.col("keep")).over(spec).alias("n"),
                    f.sum("value", distinct=True).filter(f.col("keep")).over(spec).alias("total"),
                    f.var_pop("value").filter(f.col("keep")).over(spec).alias("variance"),
                    f.var_pop("value").filter(f.col("other")).over(spec).alias("other_variance"),
                    f.median("value").filter(f.col("keep")).over(spec).alias("median"),
                    f.median("value").filter(f.col("other")).over(spec).alias("other_median"),
                    f.list("value", order_by=[("local_order", True, "last")]).filter(f.col("keep")).over(spec).alias("items"),
                    f.string_agg("text", "|", order_by=[("local_order", True, "last")]).filter(f.col("keep")).over(spec).alias("joined"),
                ]
                typed_windows = [
                    native.WindowExpression("count_all", "n", filter_column="keep", partition_by=["fund"],
                        order_by=[("id", False, None)], frame=window_frame),
                    native.WindowExpression("sum_distinct", "total", source_column="value", filter_column="keep",
                        partition_by=["fund"], order_by=[("id", False, None)], frame=window_frame),
                    native.WindowExpression("var_pop", "variance", source_column="value", filter_column="keep",
                        partition_by=["fund"], order_by=[("id", False, None)], frame=window_frame),
                    native.WindowExpression("var_pop", "other_variance", source_column="value", filter_column="other",
                        partition_by=["fund"], order_by=[("id", False, None)], frame=window_frame),
                    native.WindowExpression("median", "median", source_column="value", filter_column="keep",
                        partition_by=["fund"], order_by=[("id", False, None)], frame=window_frame),
                    native.WindowExpression("median", "other_median", source_column="value", filter_column="other",
                        partition_by=["fund"], order_by=[("id", False, None)], frame=window_frame),
                    native.WindowExpression("list", "items", source_column="value", filter_column="keep",
                        aggregate_order_by=[("local_order", True, "last")], partition_by=["fund"],
                        order_by=[("id", False, None)], frame=window_frame),
                    native.WindowExpression("string_agg", "joined", source_column="text", separator="|",
                        filter_column="keep", aggregate_order_by=[("local_order", True, "last")],
                        partition_by=["fund"], order_by=[("id", False, None)], frame=window_frame),
                ]
                window_relation = native.Relation.scan(str(path)).window(["id", "fund"], typed_windows).sort([("id", False, None)])
                over = " OVER (PARTITION BY fund ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)"
                window_sql = (f"SELECT id,fund,COUNT(*) FILTER (WHERE keep){over} AS n,"
                    f"SUM(DISTINCT value) FILTER (WHERE keep){over} AS total,"
                    f"VAR_POP(value) FILTER (WHERE keep){over} AS variance,"
                    f"VAR_POP(value) FILTER (WHERE other){over} AS other_variance,"
                    f"MEDIAN(value) FILTER (WHERE keep){over} AS median,"
                    f"MEDIAN(value) FILTER (WHERE other){over} AS other_median,"
                    f"LIST(value ORDER BY local_order DESC NULLS LAST) FILTER (WHERE keep){over} AS items,"
                    f"STRING_AGG(text, '|' ORDER BY local_order DESC NULLS LAST) FILTER (WHERE keep){over} AS joined "
                    f"FROM '{path}' ORDER BY id")
                window_frame_api = read(path, client=client).select("id", "fund", *window_exprs).sort("id")
                window_schema = pa.schema([pa.field("id", pa.uint64(), nullable=False),
                    pa.field("fund", pa.string(), nullable=False), pa.field("n", pa.uint64(), nullable=False),
                    pa.field("total", pa.float64()), pa.field("variance", pa.float64()),
                    pa.field("other_variance", pa.float64()), pa.field("median", pa.float64()),
                    pa.field("other_median", pa.float64()),
                    pa.field("items", pa.list_(pa.field("item", pa.uint64(), nullable=True))),
                    pa.field("joined", pa.string())])
                expected_window = []
                for row in rows:
                    selected = [r for r in rows if r["fund"] == row["fund"] and abs(r["id"] - row["id"]) <= 1]
                    a, b = reference(selected, "keep"), reference(selected, "other")
                    expected_window.append({"id": row["id"], "fund": row["fund"], "n": a["n"],
                        "total": a["total"], "variance": a["variance"], "other_variance": b["variance"],
                        "median": a["median"], "other_median": b["median"], "items": a["items"],
                        "joined": a["joined"]})
                expected_window_table = pa.Table.from_pylist(expected_window, schema=window_schema)
                window_demand = {"native_source_demand_projected_columns": "7",
                                 "native_source_demand_available_columns": "8"} if fmt == "parquet" else None
                for actual in self.all_surfaces(window_frame_api, window_relation, window_sql, window_demand):
                    close(actual, expected_window_table)

                empty_frame = read(path, client=client).filter(sl.col("id") < 0).agg(
                    n=f.count().filter(f.col("keep")),
                    items=f.list("value").filter(f.col("keep")))
                empty_relation = native.Relation.scan(str(path)).filter(
                    native.Expression.column("id") < native.Expression.literal(0)).aggregate([], [
                        native.AggregateMeasure("count", None, "n", filter_column="keep"),
                        native.AggregateMeasure("list", "value", "items", filter_column="keep")])
                empty_sql = f"SELECT COUNT(*) FILTER (WHERE keep) AS n,LIST(value) FILTER (WHERE keep) AS items FROM '{path}' WHERE id < 0"
                for actual in self.all_surfaces(empty_frame, empty_relation, empty_sql):
                    self.assertEqual(actual.column_names, ["n", "items"])
                    self.assertEqual(actual.to_pylist(), [{"n": 0, "items": None}])

    def test_statistical_reductions_match_exact_reference_on_all_five_surfaces(self):
        import shardloom as sl
        from shardloom import functions as f
        import pyarrow.parquet as parquet
        native, session, pa = self.native, self.session, self.pa
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        a, b = 1 << 60, -(1 << 59)
        values = [(0, a + 1, b + 2), (0, a + 2, b + 1), (0, a + 3, b + 4),
                  (0, a + 4, b + 3), (0, None, b + 100), (0, a + 99, None),
                  (1, None, b), (1, None, None), (2, a, b),
                  (3, a + 7, b), (3, a + 7, b + 1)]
        rows = [{"fund": group, "number": x, "paired": y, "unused": "unrequested payload"}
                for group, x, y in values]
        schema = pa.schema([pa.field("fund", pa.uint8(), nullable=False),
                            pa.field("number", pa.uint64()), pa.field("paired", pa.int64()),
                            pa.field("unused", pa.string(), nullable=False)])
        table = pa.Table.from_pylist(rows, schema=schema)
        expected = [{"fund": group, **statistical_reference([row for row in rows if row["fund"] == group])}
                    for group in range(4)]
        names = tuple(name for name in expected[0] if name != "fund")
        with tempfile.TemporaryDirectory(prefix="shardloom-statistical-surfaces-") as directory:
            for format in ("vortex", "parquet"):
                path = Path(directory) / f"input.{format}"
                if format == "vortex":
                    source = session.from_arrow(table)
                    session.plan(native.Relation.scan("memory://statistics"),
                                 sources=[("memory://statistics", source)]).write(path, "write_vortex")
                else:
                    parquet.write_table(table, path, row_group_size=3)
                read = sl.read_vortex if format == "vortex" else sl.read_parquet
                frame = read(path, client=client).group_by("fund").agg(
                    var_pop=f.var_pop("number"), var_samp=f.variance("number"),
                    stddev_pop=f.stddev_pop("number"), stddev_samp=f.std("number"),
                    covar_pop=f.covar_pop("number", "paired"),
                    covar_samp=f.covar_samp("number", "paired"), corr=f.corr("number", "paired"),
                ).sort("fund")
                measures = [(name, "number", name) for name in names[:4]] + [
                    sl.native.AggregateMeasure(name, "number", name, secondary_column="paired")
                    for name in names[4:]]
                relation = native.Relation.scan(str(path)).aggregate(["fund"], measures).sort([("fund", False, None)])
                projection = ",".join(f"{name.upper()}(number{',paired' if name in names[4:] else ''}) AS {name}"
                                      for name in names)
                statement = f"SELECT fund,{projection} FROM '{path}' GROUP BY fund ORDER BY fund"
                for surface, actual in zip(("dataframe", "typed", "python_sql", "workflow", "cli_sql"),
                                           self.all_surfaces(frame, relation, statement)):
                    with self.subTest(format=format, surface=surface):
                        self.assertEqual(actual.column_names, ["fund", *names])
                        for name in names:
                            self.assertEqual(actual.schema.field(name).type, pa.float64())
                            self.assertTrue(actual.schema.field(name).nullable)
                        self.assertEqual(actual.num_rows, len(expected))
                        for observed, reference in zip(actual.to_pylist(), expected):
                            self.assertEqual(observed["fund"], reference["fund"])
                            for name in names:
                                if reference[name] is None:
                                    self.assertIsNone(observed[name], name)
                                else:
                                    self.assertAlmostEqual(observed[name], reference[name],
                                        delta=1e-12 * max(1.0, abs(reference[name])), msg=name)

    def test_statistical_windows_match_exact_frames_on_all_five_surfaces(self):
        import shardloom as sl
        from shardloom import functions as f
        import pyarrow.parquet as parquet
        native, session, pa = self.native, self.session, self.pa
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        rows = [{"id": index, "fund": index // 3,
                 "number": None if x is None else (1 << 60) + x, "paired": y}
                for index, (x, y) in enumerate([(1, 4), (3, None), (5, 8), (2, 9), (None, 10), (6, 7)])]
        schema = pa.schema([pa.field("id", pa.uint64(), nullable=False),
                            pa.field("fund", pa.uint8(), nullable=False),
                            pa.field("number", pa.uint64()), pa.field("paired", pa.int64())])
        names = tuple(statistical_reference(rows))
        with tempfile.TemporaryDirectory(prefix="shardloom-statistical-windows-") as directory:
            for format in ("vortex", "parquet"):
                path = Path(directory) / f"input.{format}"
                table = pa.Table.from_pylist(rows, schema=schema)
                if format == "vortex":
                    source = session.from_arrow(table)
                    session.plan(native.Relation.scan("memory://windows"),
                                 sources=[("memory://windows", source)]).write(path, "write_vortex")
                else:
                    parquet.write_table(table, path, row_group_size=2)
                for exclusion in ("no_others", "current_row"):
                    spec = sl.Window.partition_by("fund").order_by("id").rows_between(-1, 0).exclude(exclusion)
                    expressions = [(getattr(f, name)("number", "paired") if name in names[4:]
                                    else getattr(f, name)("number")).over(spec).alias(name) for name in names]
                    read = sl.read_vortex if format == "vortex" else sl.read_parquet
                    frame = read(path, client=client).select("id", *expressions).sort("id")
                    typed_frame = native.WindowFrame("rows", ("preceding", native.FrameOffset.number(
                        native.Expression.literal(1))), ("current_row", None), exclusion=exclusion)
                    relation = native.Relation.scan(str(path)).window(["id"], [
                        native.WindowExpression(name, name, source_column="number",
                            secondary_column="paired" if name in names[4:] else None,
                            partition_by=["fund"], order_by=[("id", False, None)], frame=typed_frame)
                        for name in names]).sort([("id", False, None)])
                    projection = ",".join(str(expression) for expression in expressions)
                    statement = f"SELECT id,{projection} FROM '{path}' ORDER BY id"
                    expected = [{"id": row["id"], **statistical_reference([
                        other for other in rows if other["fund"] == row["fund"]
                        and row["id"] - 1 <= other["id"] <= row["id"]
                        and (exclusion == "no_others" or other["id"] != row["id"])])} for row in rows]
                    for surface, actual in zip(("dataframe", "typed", "python_sql", "workflow", "cli_sql"),
                                               self.all_surfaces(frame, relation, statement)):
                        with self.subTest(format=format, exclusion=exclusion, surface=surface):
                            self.assertEqual(actual.column_names, ["id", *names])
                            self.assertEqual(actual.num_rows, len(expected))
                            for name in names:
                                self.assertEqual(actual.schema.field(name).type, pa.float64())
                                self.assertTrue(actual.schema.field(name).nullable)
                            for observed, reference in zip(actual.to_pylist(), expected):
                                self.assertEqual(observed["id"], reference["id"])
                                for name in names:
                                    if reference[name] is None:
                                        self.assertIsNone(observed[name], name)
                                    else:
                                        self.assertAlmostEqual(observed[name], reference[name], places=12, msg=name)

    def test_statistical_scaled_results_match_all_five_surfaces(self):
        import shardloom as sl
        from shardloom import functions as f
        import pyarrow.parquet as parquet

        native, session, pa = self.native, self.session, self.pa
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        deltas = (1e-200, 2.0**1000, 2.0**-500)
        paired_deltas = (-1e-200, 2.0**-1000, 2.0**-572)
        rows = []
        for fund, (number, paired) in enumerate(zip(deltas, paired_deltas)):
            rows.extend([
                {"id": 2 * fund, "fund": fund, "number": 0.0, "paired": 0.0},
                {"id": 2 * fund + 1, "fund": fund, "number": number, "paired": paired},
            ])
        schema = pa.schema([pa.field("id", pa.uint64(), nullable=False),
                            pa.field("fund", pa.uint8(), nullable=False),
                            pa.field("number", pa.float64()), pa.field("paired", pa.float64())])
        grouped_expected = []
        for fund, (number, paired) in enumerate(zip(deltas, paired_deltas)):
            grouped_expected.append({
                "fund": fund,
                "deviation": abs(number) / 2,
                "covariance": float(Fraction.from_float(number) * Fraction.from_float(paired) / 4),
                "correlation": -1.0 if fund == 0 else 1.0,
            })
        output_names = ("deviation", "covariance", "correlation")

        def compare_number(actual, expected, name):
            if expected is None:
                self.assertIsNone(actual, name)
            elif expected == 0:
                self.assertEqual(actual, expected, name)
            else:
                self.assertLessEqual(abs(actual / expected - 1.0), 1e-12, name)

        with tempfile.TemporaryDirectory(prefix="shardloom-statistical-scaled-") as directory:
            table = pa.Table.from_pylist(rows, schema=schema)
            for format in ("vortex", "parquet"):
                path = Path(directory) / f"input.{format}"
                if format == "vortex":
                    source = session.from_arrow(table)
                    session.plan(native.Relation.scan("memory://scaled-statistics"),
                                 sources=[("memory://scaled-statistics", source)]).write(path, "write_vortex")
                else:
                    parquet.write_table(table, path, row_group_size=1)
                read = sl.read_vortex if format == "vortex" else sl.read_parquet
                frame = read(path, client=client).group_by("fund").agg(
                    deviation=f.stddev_pop("number"),
                    covariance=f.covar_pop("number", "paired"),
                    correlation=f.corr("number", "paired"),
                ).sort("fund")
                measures = [
                    ("stddev_pop", "number", "deviation"),
                    sl.native.AggregateMeasure("covar_pop", "number", "covariance",
                                               secondary_column="paired"),
                    sl.native.AggregateMeasure("corr", "number", "correlation",
                                               secondary_column="paired"),
                ]
                relation = native.Relation.scan(str(path)).aggregate(["fund"], measures).sort(
                    [("fund", False, None)])
                statement = (f"SELECT fund,STDDEV_POP(number) AS deviation,"
                             f"COVAR_POP(number,paired) AS covariance,CORR(number,paired) AS correlation "
                             f"FROM '{path}' GROUP BY fund ORDER BY fund")
                for surface, actual in zip(("dataframe", "typed", "python_sql", "workflow", "cli_sql"),
                                           self.all_surfaces(frame, relation, statement)):
                    with self.subTest(format=format, surface=surface, operation="grouped"):
                        self.assertEqual(actual.column_names, ["fund", *output_names])
                        self.assertEqual(actual.num_rows, len(grouped_expected))
                        for name in output_names:
                            self.assertEqual(actual.schema.field(name).type, pa.float64())
                            self.assertTrue(actual.schema.field(name).nullable)
                        for observed, expected in zip(actual.to_pylist(), grouped_expected):
                            self.assertEqual(observed["fund"], expected["fund"])
                            for name in output_names:
                                compare_number(observed[name], expected[name], name)

                spec = sl.Window.partition_by("fund").order_by("id").rows_between(-1, 0)
                expressions = [f.stddev_pop("number").over(spec).alias("deviation"),
                               f.covar_pop("number", "paired").over(spec).alias("covariance"),
                               f.corr("number", "paired").over(spec).alias("correlation")]
                frame = read(path, client=client).select("id", "fund", *expressions).sort("id")
                typed_frame = native.WindowFrame("rows", ("preceding", native.FrameOffset.number(
                    native.Expression.literal(1))), ("current_row", None))
                relation = native.Relation.scan(str(path)).window(["id", "fund"], [
                    native.WindowExpression("stddev_pop", "deviation", source_column="number",
                        partition_by=["fund"], order_by=[("id", False, None)], frame=typed_frame),
                    native.WindowExpression("covar_pop", "covariance", source_column="number",
                        secondary_column="paired", partition_by=["fund"],
                        order_by=[("id", False, None)], frame=typed_frame),
                    native.WindowExpression("corr", "correlation", source_column="number",
                        secondary_column="paired", partition_by=["fund"],
                        order_by=[("id", False, None)], frame=typed_frame),
                ]).sort([("id", False, None)])
                statement = (f"SELECT id,fund,{','.join(map(str, expressions))} "
                             f"FROM '{path}' ORDER BY id")
                expected_windows = []
                for fund in range(len(deltas)):
                    expected_windows.extend([
                        {"id": 2 * fund, "fund": fund, "deviation": 0.0,
                         "covariance": 0.0, "correlation": None},
                        {"id": 2 * fund + 1, "fund": fund, **grouped_expected[fund]},
                    ])
                for surface, actual in zip(("dataframe", "typed", "python_sql", "workflow", "cli_sql"),
                                           self.all_surfaces(frame, relation, statement)):
                    with self.subTest(format=format, surface=surface, operation="window"):
                        self.assertEqual(actual.column_names, ["id", "fund", *output_names])
                        self.assertEqual(actual.num_rows, len(expected_windows))
                        for name in output_names:
                            self.assertEqual(actual.schema.field(name).type, pa.float64())
                            self.assertTrue(actual.schema.field(name).nullable)
                        for observed, expected in zip(actual.to_pylist(), expected_windows):
                            self.assertEqual(observed["id"], expected["id"])
                            self.assertEqual(observed["fund"], expected["fund"])
                            for name in output_names:
                                compare_number(observed[name], expected[name], name)

    def test_distinct_additive_grouped_and_windows_match_all_five_surfaces(self):
        from decimal import Decimal
        import shardloom as sl
        from shardloom import functions as f
        import pyarrow.parquet as parquet
        native, session, pa = self.native, self.session, self.pa
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        for helper in (f.sum, f.avg, f.mean):
            with self.assertRaises(TypeError):
                helper("value", distinct=1)
        with tempfile.TemporaryDirectory(prefix="shardloom-distinct-additive-") as directory:
            for decimal in (False, True):
                rows = [{"id": index, "fund": 0 if index < 6 else 1 if index < 8 else 2,
                         "value": None if value is None else Decimal(value) / 100 if decimal else value}
                        for index, value in enumerate((3, 3, -3, 3, None, -3, 12, 12, None))]
                schema = pa.schema([pa.field("id", pa.uint64(), nullable=False),
                                    pa.field("fund", pa.uint8(), nullable=False),
                                    pa.field("value", pa.decimal128(10, 2) if decimal else pa.int64())])
                def reference(selected):
                    values = {row["value"] for row in selected if row["value"] is not None}
                    total = sum(values) if values else None
                    average = total / len(values) if values else None
                    if not decimal and total is not None:
                        total, average = float(total), float(average)
                    return {"total": total, "average": average, "distinct": len(values)}
                for format in ("vortex", "parquet"):
                    path = Path(directory) / f"input-{decimal}.{format}"
                    table = pa.Table.from_pylist(rows, schema=schema)
                    if format == "vortex":
                        source = session.from_arrow(table)
                        session.plan(native.Relation.scan("memory://distinct"),
                                     sources=[("memory://distinct", source)]).write(path, "write_vortex")
                    else:
                        parquet.write_table(table, path, row_group_size=2)
                    read = sl.read_vortex if format == "vortex" else sl.read_parquet
                    frame = read(path, client=client).group_by("fund").agg(
                        total=f.sum("value", distinct=True), average=f.mean_distinct("value"),
                        distinct=f.count_distinct("value")).sort("fund")
                    relation = native.Relation.scan(str(path)).aggregate(["fund"], [
                        ("sum_distinct", "value", "total"), ("avg_distinct", "value", "average"),
                        ("count_distinct", "value", "distinct")]).sort([("fund", False, None)])
                    statement = (f"SELECT fund,SUM(DISTINCT value) AS total,AVG(DISTINCT value) AS average,"
                                 f"COUNT(DISTINCT value) AS distinct FROM '{path}' GROUP BY fund ORDER BY fund")
                    expected = [{"fund": group, **reference([row for row in rows if row["fund"] == group])}
                                for group in range(3)]
                    for actual in self.all_surfaces(frame, relation, statement):
                        with self.subTest(decimal=decimal, format=format, operation="grouped"):
                            self.assertEqual(actual.to_pylist(), expected)
                            self.assertEqual(actual.schema.field("total").type, pa.decimal128(38, 2) if decimal else pa.float64())
                            self.assertEqual(actual.schema.field("average").type, pa.decimal128(38, 6) if decimal else pa.float64())
                    spec = sl.Window.partition_by("fund").order_by("id").rows_between(-2, 0)
                    expressions = [f.sum_distinct("value").over(spec).alias("total"),
                                   f.avg("value", distinct=True).over(spec).alias("average"),
                                   f.count_distinct("value").over(spec).alias("distinct")]
                    frame = read(path, client=client).select("id", *expressions).sort("id")
                    typed_frame = native.WindowFrame("rows", ("preceding", native.FrameOffset.number(
                        native.Expression.literal(2))), ("current_row", None))
                    relation = native.Relation.scan(str(path)).window(["id"], [
                        native.WindowExpression(name, alias, source_column="value", partition_by=["fund"],
                            order_by=[("id", False, None)], frame=typed_frame)
                        for name, alias in (("sum_distinct", "total"), ("avg_distinct", "average"),
                                            ("count_distinct", "distinct"))]).sort([("id", False, None)])
                    statement = f"SELECT id,{','.join(map(str, expressions))} FROM '{path}' ORDER BY id"
                    expected = [{"id": row["id"], **reference([other for other in rows
                        if other["fund"] == row["fund"] and row["id"] - 2 <= other["id"] <= row["id"]])}
                        for row in rows]
                    for actual in self.all_surfaces(frame, relation, statement):
                        with self.subTest(decimal=decimal, format=format, operation="window"):
                            self.assertEqual(actual.to_pylist(), expected)
                            self.assertEqual(actual.schema.field("total").type, pa.decimal128(38, 2) if decimal else pa.float64())
                            self.assertEqual(actual.schema.field("average").type, pa.decimal128(38, 6) if decimal else pa.float64())

    def test_exact_quantiles_match_all_five_surfaces(self):
        import math
        import shardloom as sl
        from shardloom import functions as f
        import pyarrow.parquet as parquet

        native, session, pa = self.native, self.session, self.pa
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        with self.assertRaises(TypeError):
            f.quantile_cont("number", True)
        for level in (math.nan, math.inf, -0.1, 1.1):
            with self.subTest(level=level), self.assertRaises(ValueError):
                f.quantile_disc("number", level)
        for function, level, error in (("quantile_cont", True, TypeError),
                                       ("quantile_disc", math.nan, ValueError),
                                       ("quantile_cont", -0.1, ValueError),
                                       ("quantile_disc", 1.1, ValueError),
                                       ("median", 0.5, ValueError),
                                       ("sum", 0.5, ValueError)):
            with self.subTest(function=function, level=level):
                with self.assertRaises(error):
                    native.AggregateMeasure(function, "number", "out", quantile_level=level)
                with self.assertRaises(error):
                    native.WindowExpression(function, "out", source_column="number", quantile_level=level)
        for function in ("quantile_cont", "quantile_disc"):
            with self.assertRaises(ValueError):
                native.AggregateMeasure(function, "number", "out")
            with self.assertRaises(ValueError):
                native.WindowExpression(function, "out", source_column="number")

        rows = [
            {"fund": "A", "id": 0, "number": 1, "unused": "unused"},
            {"fund": "A", "id": 1, "number": 1, "unused": "unused"},
            {"fund": "A", "id": 2, "number": 5, "unused": "unused"},
            {"fund": "A", "id": 3, "number": None, "unused": "unused"},
            {"fund": "A", "id": 4, "number": 9, "unused": "unused"},
            {"fund": "B", "id": 5, "number": 2**60 + 3, "unused": "unused"},
            {"fund": "B", "id": 6, "number": 2**60 + 1, "unused": "unused"},
            {"fund": "B", "id": 7, "number": None, "unused": "unused"},
            {"fund": "B", "id": 8, "number": 2**60 + 2, "unused": "unused"},
            {"fund": None, "id": 9, "number": None, "unused": "unused"},
            {"fund": None, "id": 10, "number": None, "unused": "unused"},
        ]
        schema = pa.schema([pa.field("fund", pa.string()),
                            pa.field("id", pa.uint64(), nullable=False),
                            pa.field("number", pa.uint64()),
                            pa.field("unused", pa.string(), nullable=False)])
        continuous = ("median", "cont25", "cont75", "shifted_cont50")
        discrete = ("disc50", "disc100")
        names = ("median", "cont25", "cont75", "disc50", "disc100", "shifted_cont50")

        def reference(values, level, *, discrete_result=False):
            values = sorted(value for value in values if value is not None)
            if not values:
                return None
            probability = Fraction.from_float(level)
            if discrete_result:
                rank = probability * len(values)
                index = (rank.numerator + rank.denominator - 1) // rank.denominator - 1
                return values[min(max(index, 0), len(values) - 1)]
            rank = probability * (len(values) - 1)
            lower = rank.numerator // rank.denominator
            upper = min(lower + 1, len(values) - 1)
            fraction = rank - lower
            exact = Fraction(values[lower]) * (1 - fraction) + Fraction(values[upper]) * fraction
            return float(exact)

        levels = {"median": 0.5, "cont25": 0.25, "cont75": 0.75,
                  "disc50": 0.5, "disc100": 1.0, "shifted_cont50": 0.5}

        def expected_for(selected, identity):
            values = [row["number"] for row in selected]
            shifted = [None if value is None else value + 1 for value in values]
            return {
                **identity,
                "median": reference(values, levels["median"]),
                "cont25": reference(values, levels["cont25"]),
                "cont75": reference(values, levels["cont75"]),
                "disc50": reference(values, levels["disc50"], discrete_result=True),
                "disc100": reference(values, levels["disc100"], discrete_result=True),
                "shifted_cont50": reference(shifted, levels["shifted_cont50"]),
            }

        def check_rows(actual, expected, identity_columns):
            self.assertEqual(actual.num_rows, len(expected))
            for name in continuous:
                self.assertEqual(actual.schema.field(name).type, pa.float64())
                self.assertTrue(actual.schema.field(name).nullable)
            for name in discrete:
                self.assertEqual(actual.schema.field(name).type, pa.uint64())
                self.assertTrue(actual.schema.field(name).nullable)
            observed_rows = actual.to_pylist()
            expected_by_key = {tuple(row[key] for key in identity_columns): row for row in expected}
            observed_by_key = {tuple(row[key] for key in identity_columns): row for row in observed_rows}
            self.assertEqual(observed_by_key.keys(), expected_by_key.keys())
            for key, reference_row in expected_by_key.items():
                observed = observed_by_key[key]
                for name in continuous:
                    wanted = reference_row[name]
                    if wanted is None:
                        self.assertIsNone(observed[name], name)
                    elif wanted == 0:
                        self.assertEqual(observed[name], wanted, name)
                    else:
                        self.assertLessEqual(abs(observed[name] / wanted - 1.0), 1e-12, name)
                for name in discrete:
                    self.assertEqual(observed[name], reference_row[name], name)

        with tempfile.TemporaryDirectory(prefix="shardloom-exact-quantiles-") as directory:
            table = pa.Table.from_pylist(rows, schema=schema)
            for format in ("vortex", "parquet"):
                path = Path(directory) / f"input.{format}"
                if format == "vortex":
                    source = session.from_arrow(table)
                    session.plan(native.Relation.scan("memory://quantiles"),
                                 sources=[("memory://quantiles", source)]).write(path, "write_vortex")
                else:
                    parquet.write_table(table, path, row_group_size=1)
                read = sl.read_vortex if format == "vortex" else sl.read_parquet

                frame = read(path, client=client).group_by("fund").agg(
                    median=f.median("number"),
                    cont25=f.quantile_cont("number", 0.25),
                    cont75=f.quantile_cont("number", 0.75),
                    disc50=f.quantile_disc("number", 0.5),
                    disc100=f.quantile_disc("number", 1.0),
                    shifted_cont50=f.quantile_cont(sl.col("number") + f.lit(1).cast("uint64"), 0.5),
                ).sort("fund", nulls="last")
                expression = native.Expression
                grouped_measures = [
                    native.AggregateMeasure("median", "number", "median"),
                    native.AggregateMeasure("quantile_cont", "number", "cont25", quantile_level=0.25),
                    native.AggregateMeasure("quantile_cont", "number", "cont75", quantile_level=0.75),
                    native.AggregateMeasure("quantile_disc", "number", "disc50", quantile_level=0.5),
                    native.AggregateMeasure("quantile_disc", "number", "disc100", quantile_level=1.0),
                    native.AggregateMeasure("quantile_cont", "__quantile_shifted", "shifted_cont50",
                                             quantile_level=0.5),
                ]
                relation = (native.Relation.scan(str(path)).project([
                    ("fund", expression.column("fund")),
                    ("number", expression.column("number")),
                    ("__quantile_shifted", expression.column("number") + expression.uint64(1)),
                ]).aggregate(["fund"], grouped_measures).sort([("fund", False, "last")]))
                grouped_sql = (f"SELECT fund,MEDIAN(number) AS median,QUANTILE_CONT(number,0.25) AS cont25,"
                               f"QUANTILE_CONT(number,0.75) AS cont75,QUANTILE_DISC(number,0.5) AS disc50,"
                               f"QUANTILE_DISC(number,1.0) AS disc100,QUANTILE_CONT(number+CAST(1 AS UINT64),0.5) AS shifted_cont50 "
                               f"FROM '{path}' GROUP BY fund ORDER BY fund NULLS LAST")
                grouped_expected = [expected_for([row for row in rows if row["fund"] == fund], {"fund": fund})
                                    for fund in ("A", "B", None)]
                demand = {"native_source_demand_projected_columns": "2",
                          "native_source_demand_available_columns": "4"} if format == "parquet" else None
                for actual in self.all_surfaces(frame, relation, grouped_sql, demand):
                    self.assertEqual(actual.column_names, ["fund", *names])
                    self.assertTrue(actual.schema.field("fund").nullable)
                    self.assertEqual(actual.schema.field("fund").type, pa.string())
                    check_rows(actual, grouped_expected, ("fund",))
                    self.assertNotIn("unused", actual.column_names)

                spec = sl.Window.partition_by("fund").order_by("id").rows_between(-1, 1)
                window_expressions = [
                    f.median("number").over(spec).alias("median"),
                    f.quantile_cont("number", 0.25).over(spec).alias("cont25"),
                    f.quantile_cont("number", 0.75).over(spec).alias("cont75"),
                    f.quantile_disc("number", 0.5).over(spec).alias("disc50"),
                    f.quantile_disc("number", 1.0).over(spec).alias("disc100"),
                    f.quantile_cont(sl.col("number") + f.lit(1).cast("uint64"), 0.5).over(spec).alias("shifted_cont50"),
                ]
                window_frame = native.WindowFrame(
                    "rows", ("preceding", native.FrameOffset.number(expression.literal(1))),
                    ("following", native.FrameOffset.number(expression.literal(1))),
                )
                window_measures = [
                    native.WindowExpression("median", "median", source_column="number",
                        partition_by=["fund"], order_by=[("id", False, None)], frame=window_frame),
                    native.WindowExpression("quantile_cont", "cont25", source_column="number",
                        quantile_level=0.25, partition_by=["fund"], order_by=[("id", False, None)], frame=window_frame),
                    native.WindowExpression("quantile_cont", "cont75", source_column="number",
                        quantile_level=0.75, partition_by=["fund"], order_by=[("id", False, None)], frame=window_frame),
                    native.WindowExpression("quantile_disc", "disc50", source_column="number",
                        quantile_level=0.5, partition_by=["fund"], order_by=[("id", False, None)], frame=window_frame),
                    native.WindowExpression("quantile_disc", "disc100", source_column="number",
                        quantile_level=1.0, partition_by=["fund"], order_by=[("id", False, None)], frame=window_frame),
                    native.WindowExpression("quantile_cont", "shifted_cont50", source_column="__quantile_shifted",
                        quantile_level=0.5, partition_by=["fund"], order_by=[("id", False, None)], frame=window_frame),
                ]
                window_relation = (native.Relation.scan(str(path)).project([
                    ("id", expression.column("id")), ("fund", expression.column("fund")),
                    ("number", expression.column("number")),
                    ("__quantile_shifted", expression.column("number") + expression.uint64(1)),
                ]).window(["id", "fund"], window_measures).sort([("id", False, None)]))
                window_sql = (f"SELECT id,fund,{','.join(map(str, window_expressions))} FROM '{path}' "
                              "ORDER BY id")
                window_frame_rows = []
                for row in rows:
                    selected = [other for other in rows if other["fund"] == row["fund"]
                                and abs(other["id"] - row["id"]) <= 1]
                    window_frame_rows.append(expected_for(selected, {"id": row["id"], "fund": row["fund"]}))
                demand = {"native_source_demand_projected_columns": "3",
                          "native_source_demand_available_columns": "4"} if format == "parquet" else None
                for actual in self.all_surfaces(read(path, client=client).select(
                    "id", "fund", *window_expressions).sort("id"), window_relation, window_sql, demand):
                    self.assertEqual(actual.column_names, ["id", "fund", *names])
                    self.assertEqual(actual.schema.field("id").type, pa.uint64())
                    self.assertFalse(actual.schema.field("id").nullable)
                    self.assertEqual(actual.schema.field("fund").type, pa.string())
                    self.assertTrue(actual.schema.field("fund").nullable)
                    check_rows(actual, window_frame_rows, ("id", "fund"))
                    self.assertNotIn("unused", actual.column_names)

    def test_ordered_selectors_match_all_five_surfaces(self):
        import shardloom as sl
        from shardloom import functions as f
        import pyarrow.parquet as parquet

        native, session, pa = self.native, self.session, self.pa
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        values = [None, 2**60 + 1, 2**60 + 3, 2**60 + 2, None, 8, 9, None, None, None]
        keys = [0, 5, 5, 1, 4, None, 4, 8, None, 9]
        local_order = [3, 2, 1, None, 0, 2, 2, 1, 0, 1]
        rows = [
            {"id": index, "fund": "A" if index < 4 else "B" if index < 8 else "C",
             "value": values[index], "arg_key": keys[index], "local_order": local_order[index],
             "unused": "not requested"}
            for index in range(10)
        ]
        schema = pa.schema([
            pa.field("id", pa.uint64(), nullable=False),
            pa.field("fund", pa.string(), nullable=False),
            pa.field("value", pa.uint64()),
            pa.field("arg_key", pa.uint64()),
            pa.field("local_order", pa.int32()),
            pa.field("unused", pa.string(), nullable=False),
        ])
        table = pa.Table.from_pylist(rows, schema=schema)
        functions = ("first", "last", "arg_min", "arg_max")
        modes = (("default", None), ("asc", False), ("desc", True))
        names = [f"{function}_{mode}" for function in functions for mode, _ in modes]

        def ordered(candidates, descending):
            if descending is None:
                return sorted(candidates, key=lambda row: row["id"])
            return sorted(candidates, key=lambda row: (
                row["local_order"] is None,
                -(row["local_order"] or 0) if descending else (row["local_order"] or 0),
                row["id"],
            ))

        def reduce_rows(candidates, function, descending):
            selected = ordered(candidates, descending)
            if function == "first":
                return selected[0]["value"] if selected else None
            if function == "last":
                return selected[-1]["value"] if selected else None
            eligible = [row for row in selected if row["value"] is not None and row["arg_key"] is not None]
            if not eligible:
                return None
            extreme = min(row["arg_key"] for row in eligible) if function == "arg_min" else max(
                row["arg_key"] for row in eligible
            )
            return next(row["value"] for row in eligible if row["arg_key"] == extreme)

        with tempfile.TemporaryDirectory(prefix="shardloom-ordered-selectors-") as directory:
            for source_format in ("vortex", "parquet"):
                path = Path(directory) / f"input.{source_format}"
                if source_format == "vortex":
                    source = session.from_arrow(table)
                    session.plan(native.Relation.scan("memory://ordered-selectors"),
                                 sources=[("memory://ordered-selectors", source)]).write(path, "write_vortex")
                else:
                    parquet.write_table(table, path, row_group_size=1)
                read = sl.read_vortex if source_format == "vortex" else sl.read_parquet

                def dataframe_expression(function, mode):
                    order = () if mode is None else [("local_order", mode, "last")]
                    args = ("value", "arg_key") if function.startswith("arg_") else ("value",)
                    return getattr(f, function)(*args, order_by=order)

                frame = read(path, client=client).group_by("fund").agg(**{
                    name: dataframe_expression(function, direction)
                    for function in functions for mode, direction in modes
                    for name in [f"{function}_{mode}"]
                }).sort("fund")
                typed_measures = []
                sql_measures = []
                for function in functions:
                    for mode, direction in modes:
                        alias = f"{function}_{mode}"
                        order = [] if direction is None else [("local_order", direction, "last")]
                        kwargs = {"secondary_column": "arg_key"} if function.startswith("arg_") else {}
                        typed_measures.append(native.AggregateMeasure(
                            function, "value", alias, order_by=order, **kwargs
                        ))
                        suffix = "" if direction is None else (
                            f" ORDER BY local_order {'DESC' if direction else 'ASC'} NULLS LAST"
                        )
                        args = "value,arg_key" if function.startswith("arg_") else "value"
                        sql_measures.append(f"{function.upper()}({args}{suffix}) AS {alias}")
                relation = native.Relation.scan(str(path)).aggregate(["fund"], typed_measures).sort(
                    [("fund", False, None)]
                )
                grouped_sql = f"SELECT fund,{','.join(sql_measures)} FROM '{path}' GROUP BY fund ORDER BY fund"
                grouped_expected = []
                for fund in ("A", "B", "C"):
                    candidates = [row for row in rows if row["fund"] == fund]
                    grouped_expected.append({
                        "fund": fund,
                        **{f"{function}_{mode}": reduce_rows(candidates, function, direction)
                           for function in functions for mode, direction in modes},
                    })
                demand = {"native_source_demand_projected_columns": "4",
                          "native_source_demand_available_columns": "6"} if source_format == "parquet" else None
                for actual in self.all_surfaces(frame, relation, grouped_sql, demand):
                    self.assertEqual(actual.column_names, ["fund", *names])
                    self.assertEqual(actual.schema.field("fund").type, pa.string())
                    self.assertFalse(actual.schema.field("fund").nullable)
                    for name in names:
                        field = actual.schema.field(name)
                        self.assertEqual(field.type, pa.uint64())
                        self.assertTrue(field.nullable)
                    self.assertEqual(actual.to_pylist(), grouped_expected)

                spec = sl.Window.partition_by("fund").order_by("id").rows_between(-1, 1)
                window_exprs = [
                    dataframe_expression(function, direction).over(spec).alias(f"{function}_{mode}")
                    for function in functions for mode, direction in modes
                ]
                native_frame = native.WindowFrame(
                    "rows", ("preceding", native.FrameOffset.number(native.Expression.literal(1))),
                    ("following", native.FrameOffset.number(native.Expression.literal(1))),
                )
                window_measures = []
                window_sql_measures = []
                for function in functions:
                    for mode, direction in modes:
                        alias = f"{function}_{mode}"
                        order = [] if direction is None else [("local_order", direction, "last")]
                        kwargs = {"secondary_column": "arg_key"} if function.startswith("arg_") else {}
                        window_measures.append(native.WindowExpression(
                            function, alias, source_column="value", aggregate_order_by=order,
                            partition_by=["fund"], order_by=[("id", False, None)], frame=native_frame,
                            **kwargs,
                        ))
                        suffix = "" if direction is None else (
                            f" ORDER BY local_order {'DESC' if direction else 'ASC'} NULLS LAST"
                        )
                        args = "value,arg_key" if function.startswith("arg_") else "value"
                        window_sql_measures.append(f"{function.upper()}({args}{suffix}) OVER "
                                                   "(PARTITION BY fund ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) "
                                                   f"AS {alias}")
                window_relation = native.Relation.scan(str(path)).window(
                    ["id", "fund"], window_measures
                ).sort([("id", False, None)])
                window_sql = f"SELECT id,fund,{','.join(window_sql_measures)} FROM '{path}' ORDER BY id"
                window_frame_rows = []
                for row in rows:
                    candidates = [other for other in rows if other["fund"] == row["fund"]
                                  and abs(other["id"] - row["id"]) <= 1]
                    window_frame_rows.append({
                        "id": row["id"], "fund": row["fund"],
                        **{f"{function}_{mode}": reduce_rows(candidates, function, direction)
                           for function in functions for mode, direction in modes},
                    })
                demand = {"native_source_demand_projected_columns": "5",
                          "native_source_demand_available_columns": "6"} if source_format == "parquet" else None
                for actual in self.all_surfaces(
                    read(path, client=client).select("id", "fund", *window_exprs).sort("id"),
                    window_relation, window_sql, demand,
                ):
                    self.assertEqual(actual.column_names, ["id", "fund", *names])
                    self.assertEqual(actual.schema.field("id").type, pa.uint64())
                    self.assertFalse(actual.schema.field("id").nullable)
                    self.assertEqual(actual.schema.field("fund").type, pa.string())
                    self.assertFalse(actual.schema.field("fund").nullable)
                    for name in names:
                        field = actual.schema.field(name)
                        self.assertEqual(field.type, pa.uint64())
                        self.assertTrue(field.nullable)
                    self.assertEqual(actual.to_pylist(), window_frame_rows)

        with self.assertRaises(ValueError):
            native.WindowExpression("first", "bad", source_column="value", secondary_column="arg_key")
        with self.assertRaises(ValueError):
            native.WindowExpression("arg_min", "bad", source_column="value")
        with self.assertRaises(ValueError):
            native.WindowExpression("sum", "bad", source_column="value",
                                    aggregate_order_by=[("id", False, None)])

    def test_renamed_grouped_count_filter_selects_shared_provider_on_every_surface(self):
        import shardloom as sl
        native, session, pa = self.native, self.session, self.pa
        expr = native.Expression
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        schema = pa.schema([pa.field("entity", pa.uint32(), nullable=False)])
        rows = [{"entity": value} for value in (2, 1, 2, 1, 3)]
        with tempfile.TemporaryDirectory(prefix="shardloom-shared-alias-") as directory:
            path = Path(directory) / "input.vortex"
            source = session.from_arrow(pa.Table.from_pylist(rows, schema=schema))
            session.plan(native.Relation.scan("memory://input"),
                         sources=[("memory://input", source)]).write(path, "write_vortex")
            frame = (sl.read_vortex(path, client=client).group_by("entity").agg(rows="COUNT(*)")
                     .select("entity AS key", "rows AS total")
                     .filter(sl.col("total") > 1).sort("key").limit(2))
            relation = (native.Relation.scan(str(path)).aggregate(["entity"], [("count", None, "rows")])
                        .project([("first_key", expr.column("entity")), ("first_total", expr.column("rows"))])
                        .project([("key", expr.column("first_key")), ("total", expr.column("first_total"))])
                        .filter(expr.column("total") > expr.literal(1))
                        .sort([("key", False, None)]).limit(2))
            sql = (f"SELECT entity AS key,COUNT(*) AS total FROM '{path}' "
                   "GROUP BY entity HAVING total > 1 ORDER BY key LIMIT 2")
            tables = self.all_surfaces(frame, relation, sql, {
                "local_primitive_report_present": "true",
                "local_primitive_native_io_certificate_path_id": "native_vortex_source_to_scalar_aggregate_result",
                "local_primitive_scan_projection_columns": '["entity"]',
                "local_primitive_scan_predicate_columns": '[]',
            })
            for table in tables:
                self.assertEqual(table.to_pylist(), [{"key": 1, "total": 2}, {"key": 2, "total": 2}])
                self.assertEqual(table.schema, tables[0].schema)

    def test_aliased_scalar_count_star_selects_shared_provider_on_every_surface(self):
        import shardloom as sl
        native, session, pa = self.native, self.session, self.pa
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        schema = pa.schema([pa.field("entity", pa.uint32(), nullable=False)])
        with tempfile.TemporaryDirectory(prefix="shardloom-aliased-count-star-") as directory:
            for name, rows, expected in (("nonempty", [{"entity": 4}, {"entity": 9}], 2),
                                         ("empty", [], 0)):
                path = Path(directory) / f"{name}.vortex"
                source = session.from_arrow(pa.Table.from_pylist(rows, schema=schema))
                session.plan(native.Relation.scan("memory://input"),
                             sources=[("memory://input", source)]).write(path, "write_vortex")
                frame = sl.read_vortex(path, client=client).agg("COUNT(*) AS record_total")
                relation = native.Relation.scan(str(path)).aggregate(
                    [], [("count", None, "record_total")])
                sql = f"SELECT COUNT(*) AS record_total FROM '{path}'"
                tables = self.all_surfaces(frame, relation, sql, {
                    "local_primitive_arrays_read_count": "0",
                    "local_primitive_native_io_certificate_path_id": "native_vortex_source_to_scalar_count_result",
                    "local_primitive_scan_projection_columns": "[]",
                    "local_primitive_scan_metadata_completed": "true",
                    "local_primitive_footer_dtype_summary": "not_required_for_exact_footer_count",
                    "local_primitive_embedded_layout_status": "footer_row_count_available",
                })
                for table in tables:
                    self.assertEqual(table.to_pylist(), [{"record_total": expected}])
                    self.assertEqual(table.schema.field("record_total").type, pa.uint64())
                    self.assertEqual(table.schema, tables[0].schema)

    def test_constant_group_reads_only_real_keys_on_every_surface(self):
        import shardloom as sl
        native, session, pa = self.native, self.session, self.pa
        expr = native.Expression
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        schema = pa.schema([pa.field("unrelated", pa.uint64(), nullable=False),
                            pa.field("label", pa.string(), nullable=False)])
        with tempfile.TemporaryDirectory(prefix="shardloom-constant-group-") as directory:
            for empty in (False, True):
                path = Path(directory) / f"input-{empty}.vortex"
                rows = [] if empty else [dict(unrelated=index, label=label)
                                         for index, label in enumerate(("a", "b", "a", "a"))]
                source = session.from_arrow(pa.Table.from_pylist(rows, schema=schema))
                session.plan(native.Relation.scan("memory://input"),
                             sources=[("memory://input", source)]).write(path, "write_vortex")
                frame = (sl.read_vortex(path, client=client).select("1 AS literal_1", "label")
                         .group_by("literal_1", "label").agg(c="COUNT(*)")
                         .sort("c", descending=True).limit(10))
                relation = (native.Relation.scan(str(path)).project([
                    ("literal_1", expr.literal(1)), ("label", expr.column("label"))])
                    .aggregate(["literal_1", "label"], [("count", None, "c")])
                    .sort([("c", True, None)]).limit(10))
                sql = (f"SELECT 1,label,COUNT(*) AS c FROM '{path}' "
                       "GROUP BY 1,label ORDER BY c DESC LIMIT 10")
                tables = self.all_surfaces(frame, relation, sql, {
                    "local_primitive_scan_projection_columns": '["label"]',
                    "local_primitive_scan_predicate_columns": '[]',
                })
                wanted = [] if empty else [dict(literal_1=1, label="a", c=3),
                                          dict(literal_1=1, label="b", c=1)]
                for table in tables:
                    self.assertEqual(table.to_pylist(), wanted)
                    self.assertEqual(table.schema, tables[0].schema)

    def test_nullable_projection_filter_preserves_rows_on_every_surface(self):
        import shardloom as sl
        native, session, pa = self.native, self.session, self.pa
        expr = native.Expression
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        schema = pa.schema([pa.field("entity", pa.uint32(), nullable=True),
                            pa.field("amount", pa.uint64(), nullable=False)])
        rows = [{"entity": 3, "amount": 10}, {"entity": None, "amount": 20},
                {"entity": 7, "amount": 30}, {"entity": 2, "amount": 40},
                {"entity": None, "amount": 5}]
        with tempfile.TemporaryDirectory(prefix="shardloom-nullable-filter-project-") as directory:
            path = Path(directory) / "input.vortex"
            source = session.from_arrow(pa.Table.from_pylist(rows, schema=schema))
            session.plan(native.Relation.scan("memory://input"),
                         sources=[("memory://input", source)]).write(path, "write_vortex")
            source_frame = sl.read_vortex(path, client=client)
            source_relation = native.Relation.scan(str(path))
            cases = [
                (source_frame.filter(sl.col("amount") >= 20).select("entity AS key", "amount AS paid"),
                 source_relation.filter(expr.column("amount") >= expr.literal(20)).project([
                     ("key", expr.column("entity")), ("paid", expr.column("amount"))]),
                 f"SELECT entity AS key,amount AS paid FROM '{path}' WHERE amount >= 20",
                 [{"key": None, "paid": 20}, {"key": 7, "paid": 30},
                  {"key": 2, "paid": 40}]),
                (source_frame.filter(sl.col("amount") > 100).select("entity AS key", "amount AS paid"),
                 source_relation.filter(expr.column("amount") > expr.literal(100)).project([
                     ("key", expr.column("entity")), ("paid", expr.column("amount"))]),
                 f"SELECT entity AS key,amount AS paid FROM '{path}' WHERE amount > 100", []),
            ]
            for index, (frame, relation, sql, wanted) in enumerate(cases):
                with self.subTest(case=index):
                    tables = self.all_surfaces(frame, relation, sql)
                    for table in tables:
                        self.assertEqual(table.to_pylist(), wanted)
                        self.assertEqual(table.schema, tables[0].schema)
                    self.assertTrue(tables[0].schema.field("key").nullable)
                    self.assertEqual(tables[0].schema.field("key").type, pa.uint32())
                    self.assertEqual(tables[0].schema.field("paid").type, pa.uint64())

    def test_grouped_nonnull_mean_ordering_selects_shared_provider_on_every_surface(self):
        import shardloom as sl
        native, session, pa = self.native, self.session, self.pa
        expr = native.Expression
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        schema = pa.schema([pa.field("key", pa.uint32(), nullable=False),
                            pa.field("text", pa.string(), nullable=False)])
        rows = [{"key": 2, "text": "é"}, {"key": 1, "text": "a"},
                {"key": 2, "text": "long"}, {"key": 1, "text": "longer"}]
        with tempfile.TemporaryDirectory(prefix="shardloom-shared-grouped-") as directory:
            path = Path(directory) / "input.vortex"
            source = session.from_arrow(pa.Table.from_pylist(rows, schema=schema))
            session.plan(native.Relation.scan("memory://input"),
                         sources=[("memory://input", source)]).write(path, "write_vortex")
            frame = (sl.read_vortex(path, client=client)
                     .with_column("bytes", sl.col("text").byte_length())
                     .group_by("key").agg(mean="AVG(bytes)", rows="COUNT(*)")
                     .filter(sl.col("rows") > 1).sort("mean", descending=True).limit(2))
            relation = (native.Relation.scan(str(path)).project([
                ("key", expr.column("key")),
                ("bytes", expr.call("octet_length", [expr.column("text")]))])
                .aggregate(["key"], [("avg", "bytes", "mean"), ("count", None, "rows")])
                .filter(expr.column("rows") > expr.literal(1))
                .sort([("mean", True, None)]).limit(2))
            for predicate in ("rows > 1", "COUNT(*) > 1"):
                with self.subTest(predicate=predicate):
                    sql = (f"SELECT key,AVG(octet_length(text)) AS mean,COUNT(*) AS rows FROM '{path}' "
                           f"GROUP BY key HAVING {predicate} ORDER BY mean DESC LIMIT 2")
                    tables = self.all_surfaces(frame, relation, sql, {
                        "local_primitive_report_present": "true",
                        "local_primitive_native_io_certificate_path_id": "native_vortex_source_to_scalar_aggregate_result",
                        "local_primitive_projected_columns": "key,mean,rows",
                    })
                    for table in tables:
                        self.assertEqual(table.to_pylist(), [{"key": 1, "mean": 3.5, "rows": 2},
                                                             {"key": 2, "mean": 3.0, "rows": 2}])
                        self.assertEqual(table.schema, tables[0].schema)

    def test_mixed_numeric_having_is_exact_on_every_surface(self):
        import operator
        import shardloom as sl
        native, session, pa = self.native, self.session, self.pa
        expr = native.Expression
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        cases = [
            ("sum", pa.float64(), [9007199254740992.0, 9007199254740994.0], 9007199254740993),
            ("avg", pa.float64(), [9007199254740992.0, 9007199254740994.0], 9007199254740993),
            ("min", pa.uint64(), [9223372036854775808, 9223372036854775809], 9223372036854775808.0),
            ("max", pa.int64(), [-9007199254740993, -9007199254740992], -9007199254740992.0),
        ]
        for case, (function, dtype, values, threshold) in enumerate(cases):
            schema = pa.schema([pa.field("key", pa.uint32(), nullable=False),
                                pa.field("value", dtype, nullable=False)])
            rows = [{"key": key, "value": value} for key, value in enumerate(values)]
            with tempfile.TemporaryDirectory(prefix="shardloom-numeric-having-") as directory:
                path = Path(directory) / "input.vortex"
                source = session.from_arrow(pa.Table.from_pylist(rows, schema=schema))
                session.plan(native.Relation.scan("memory://input"),
                             sources=[("memory://input", source)]).write(path, "write_vortex")
                for op, spelling in [(operator.eq, "="), (operator.ne, "!="),
                                     (operator.lt, "<"), (operator.le, "<="),
                                     (operator.gt, ">"), (operator.ge, ">=")]:
                    with self.subTest(case=case, function=function, comparison=spelling):
                        frame = (sl.read_vortex(path, client=client).group_by("key")
                                 .agg(reduced=f"{function.upper()}(value)")
                                 .filter(op(sl.col("reduced"), threshold)).sort("key"))
                        relation = (native.Relation.scan(str(path))
                                    .aggregate(["key"], [(function, "value", "reduced")])
                                    .filter(op(expr.column("reduced"), expr.literal(threshold)))
                                    .sort([("key", False, None)]))
                        statement = (f"SELECT key,{function.upper()}(value) AS reduced FROM '{path}' "
                                     f"GROUP BY key HAVING reduced {spelling} {threshold!r} ORDER BY key")
                        tables = self.all_surfaces(frame, relation, statement)
                        # Python's int/float comparison also retains the exact integer.
                        expected_rows = [{"key": key, "reduced": value}
                                         for key, value in enumerate(values) if op(value, threshold)]
                        for surface, table in zip(
                                ("dataframe", "typed", "python_sql", "workflow", "cli_sql"), tables):
                            with self.subTest(surface=surface):
                                self.assertEqual(table.to_pylist(), expected_rows)
                                self.assertEqual(table.schema, tables[0].schema)

    def test_nullable_having_comparisons_preserve_unknown_on_every_surface(self):
        import operator
        import shardloom as sl
        native, session, pa = self.native, self.session, self.pa
        expr = native.Expression
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        rows = [{"key": 1, "value": None}, {"key": 1, "value": None},
                {"key": 2, "value": 0}, {"key": 2, "value": None},
                {"key": 2, "value": 2}, {"key": 3, "value": 1},
                {"key": None, "value": 1}]
        schema = pa.schema([pa.field("key", pa.uint32()), pa.field("value", pa.int64())])
        with tempfile.TemporaryDirectory(prefix="shardloom-nullable-having-") as directory:
            path = Path(directory) / "input.vortex"
            source = session.from_arrow(pa.Table.from_pylist(rows, schema=schema))
            session.plan(native.Relation.scan("memory://input"),
                         sources=[("memory://input", source)]).write(path, "write_vortex")
            groups = {1: {"key": 1, "minimum": None, "rows": 2},
                      2: {"key": 2, "minimum": 0, "rows": 3},
                      3: {"key": 3, "minimum": 1, "rows": 1}}
            for column, op, spelling, threshold, keys in [
                ("minimum", operator.lt, "<", 1, [2]),
                ("minimum", operator.ne, "!=", 1, [2]),
                ("key", operator.lt, "<", 2, [1]),
                ("key", operator.ne, "!=", 2, [1, 3]),
            ]:
                with self.subTest(column=column, comparison=spelling):
                    frame = (sl.read_vortex(path, client=client).group_by("key")
                             .agg(minimum="MIN(value)", rows="COUNT(*)")
                             .filter(op(sl.col(column), threshold)).sort("key", nulls="first"))
                    relation = (native.Relation.scan(str(path))
                                .aggregate(["key"], [("min", "value", "minimum"),
                                                     ("count", None, "rows")])
                                .filter(op(expr.column(column), expr.literal(threshold)))
                                .sort([("key", False, "first")]))
                    statement = (f"SELECT key,MIN(value) AS minimum,COUNT(*) AS rows FROM '{path}' "
                                 f"GROUP BY key HAVING {column} {spelling} {threshold} "
                                 "ORDER BY key NULLS FIRST")
                    tables = self.all_surfaces(frame, relation, statement)
                    for surface, table in zip(
                            ("dataframe", "typed", "python_sql", "workflow", "cli_sql"), tables):
                        with self.subTest(surface=surface):
                            self.assertEqual(table.to_pylist(), [groups[key] for key in keys])
                            self.assertEqual(table.schema, tables[0].schema)

            # Moving SQL through typed admission must retain the existing names
            # assigned by the primitive frontend to unaliased aggregate outputs.
            for predicate in ("MIN(value) < 1", "min_value_1 < 1"):
                with self.subTest(unaliased_having=predicate):
                    frame = (sl.read_vortex(path, client=client).group_by("key")
                             .agg(min_value_1="MIN(value)", count_all_2="COUNT(*)")
                             .filter(sl.col("min_value_1") < 1).sort("key", nulls="first"))
                    relation = (native.Relation.scan(str(path))
                                .aggregate(["key"], [("min", "value", "min_value_1"),
                                                     ("count", None, "count_all_2")])
                                .filter(expr.column("min_value_1") < expr.literal(1))
                                .sort([("key", False, "first")]))
                    statement = (f"SELECT key,MIN(value),COUNT(*) FROM '{path}' "
                                 f"GROUP BY key HAVING {predicate} ORDER BY key NULLS FIRST")
                    tables = self.all_surfaces(frame, relation, statement)
                    for table in tables:
                        self.assertEqual(table.to_pylist(),
                                         [{"key": 2, "min_value_1": 0, "count_all_2": 3}])
                        self.assertEqual(table.schema, tables[0].schema)

    def test_extended_having_keeps_existing_implicit_output_names(self):
        from decimal import Decimal
        import shardloom as sl
        native, session, pa = self.native, self.session, self.pa
        expr = native.Expression
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        with tempfile.TemporaryDirectory(prefix="shardloom-extended-having-") as directory:
            path = Path(directory) / "input.vortex"
            source = session.from_arrow(pa.table({
                "value": pa.array([Decimal("1.00"), None, Decimal("2.00")], pa.decimal128(10, 2)),
            }))
            session.plan(native.Relation.scan("memory://input"),
                         sources=[("memory://input", source)]).write(path, "write_vortex")
            frame = (sl.read_vortex(path, client=client)
                     .agg(min_value="MIN(value)", count_all="COUNT(*)")
                     .filter(sl.col("count_all") > 0))
            relation = (native.Relation.scan(str(path))
                        .aggregate([], [("min", "value", "min_value"), ("count", None, "count_all")])
                        .filter(expr.column("count_all") > expr.literal(0)))
            statement = f"SELECT MIN(value),COUNT(*) FROM '{path}' HAVING COUNT(*) > 0"
            tables = self.all_surfaces(frame, relation, statement)
            for table in tables:
                self.assertEqual(table.to_pylist(), [{"min_value": Decimal("1.00"), "count_all": 3}])
                self.assertEqual(table.schema, tables[0].schema)

    def test_string_comparisons_preserve_literal_content_on_every_surface(self):
        import shardloom as sl
        native, session, pa = self.native, self.session, self.pa
        expr = native.Expression
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        needles = ("", " ", "  text  ", "é", " O'Reilly ", "123", "a,b", "a;b", "a|b")
        values = [None, "text", *needles]
        with tempfile.TemporaryDirectory(prefix="shardloom-shared-literals-") as directory:
            path = Path(directory) / "input.vortex"
            source = session.from_arrow(pa.table({"text": pa.array(values, type=pa.string())}))
            session.plan(native.Relation.scan("memory://input"),
                         sources=[("memory://input", source)]).write(path, "write_vortex")
            for needle in needles:
                for equal in (True, False):
                    with self.subTest(needle=needle, equal=equal):
                        predicate = sl.col("text") == needle if equal else sl.col("text") != needle
                        frame = sl.read_vortex(path, client=client).filter(predicate).select("text")
                        left, right = expr.column("text"), expr.literal(needle)
                        relation = native.Relation.scan(str(path)).filter(left == right if equal else left != right)
                        literal = "'" + needle.replace("'", "''") + "'"
                        sql = f"SELECT text FROM '{path}' WHERE text {'=' if equal else '!='} {literal}"
                        tables = self.all_surfaces(frame, relation, sql)
                        wanted = [{"text": value} for value in values
                                  if value is not None and (value == needle) == equal]
                        for surface, table in zip(
                            ("dataframe", "typed", "python_sql", "workflow", "cli_sql"), tables
                        ):
                            with self.subTest(surface=surface):
                                self.assertEqual(table.to_pylist(), wanted)
                                self.assertEqual(table.schema, tables[0].schema)
            for needle in (" ", "123", "a,b", "a;b", "a|b"):
                with self.subTest(contains=needle):
                    frame = sl.read_vortex(path, client=client).filter(sl.col("text").contains(needle))
                    relation = native.Relation.scan(str(path)).filter(
                        expr.call("contains", [expr.column("text"), expr.literal(needle)]))
                    sql = f"SELECT * FROM '{path}' WHERE text LIKE '%{needle}%'"
                    wanted = [{"text": value} for value in values if value is not None and needle in value]
                    for table in self.all_surfaces(frame, relation, sql):
                        self.assertEqual(table.to_pylist(), wanted)
            frame = sl.read_vortex(path, client=client).filter(sl.col("text").isin("", "123", "a,b"))
            column = expr.column("text")
            predicate = (column == expr.literal("")) | (column == expr.literal("123")) | (column == expr.literal("a,b"))
            relation = native.Relation.scan(str(path)).filter(predicate)
            sql = f"SELECT * FROM '{path}' WHERE text IN ('', '123', 'a,b')"
            wanted = [{"text": value} for value in values if value in ("", "123", "a,b")]
            for table in self.all_surfaces(frame, relation, sql):
                self.assertEqual(table.to_pylist(), wanted)

    def test_typed_unary_families_preserve_nulls_schema_order_and_reuse(self):
        native, session = self.native, self.session
        for shape, rows in (("mixed", ROWS), ("empty", []), ("nulls", [dict.fromkeys(SCHEMA)] * 3)):
            source = session.from_arrow(self.pa.Table.from_pylist(rows, schema=arrow_schema()))
            scan = native.Relation.scan("memory://unary").limit(len(rows))
            for kind in KINDS:
                with self.subTest(kind=kind, shape=shape):
                    relation = typed_unary(scan, kind)
                    plan = session.plan(relation, sources=[("memory://unary", source)])
                    wanted = self.pa.Table.from_pylist(expected(kind, rows), schema=arrow_schema(kind))
                    for repetition in range(2):
                        result = plan.collect()
                        actual = self.table(result)
                        self.assertTrue(actual.equals(wanted, check_metadata=True), (actual, wanted))
                        self.assertEqual(result.report["resident_relational_declaration_reused"],
                                         "true" if repetition else "false")

    def test_sql_arithmetic_uses_shared_signed_float_and_nullable_binding(self):
        import shardloom as sl
        native, session, pa = self.native, self.session, self.pa
        expr = native.Expression
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        cases = [(pa.uint32(), [None, 0, 1, 2**32 - 1]),
                 (pa.int64(), [None, -(2**53) - 1, -1, 0]),
                 (pa.float64(), [None, -1.5, 0.0, 2.25])]
        with tempfile.TemporaryDirectory(prefix="shardloom-shared-arithmetic-") as directory:
            for index, (dtype, values) in enumerate(cases):
                with self.subTest(dtype=dtype):
                    path = Path(directory) / f"input-{index}.vortex"
                    source = session.from_arrow(pa.table({"n": pa.array(values, type=dtype)}))
                    session.plan(native.Relation.scan("memory://input"),
                                 sources=[("memory://input", source)]).write(path, "write_vortex")
                    frame = (sl.read_vortex(path, client=client).with_column("shifted", sl.col("n") - 3)
                             .group_by("n", "shifted").agg(rows="COUNT(*)").sort("n", nulls="first"))
                    relation = (native.Relation.scan(str(path)).project([
                        ("n", expr.column("n")), ("shifted", expr.column("n") - expr.literal(3))])
                        .aggregate(["n", "shifted"], [("count", None, "rows")])
                        .sort([("n", False, "first")]))
                    sql = (f"SELECT n,n - 3 AS shifted,COUNT(*) AS rows FROM '{path}' "
                           "GROUP BY n,n - 3 ORDER BY n - 3 NULLS FIRST")
                    tables = self.all_surfaces(frame, relation, sql)
                    wanted = [dict(n=value, shifted=None if value is None else value-3, rows=1)
                              for value in values]
                    for table in tables:
                        self.assertEqual(table.to_pylist(), wanted)
                        self.assertEqual(table.schema, tables[0].schema)
                    # The primitive SUM offset also has to bind the expression
                    # before aggregation; cancellation and overflow stay terminal.
                    frame = sl.read_vortex(path, client=client).agg(total="SUM(n - 3)")
                    relation = (native.Relation.scan(str(path))
                                .project([("shifted", expr.column("n") - expr.literal(3))])
                                .aggregate([], [("sum", "shifted", "total")]))
                    tables = self.all_surfaces(frame, relation, f"SELECT SUM(n - 3) AS total FROM '{path}'")
                    for table in tables[1:]:
                        self.assertTrue(table.equals(tables[0], check_metadata=True), (table, tables[0]))
                    # Preserve the established unaliased primitive SQL names
                    # even when the complete statement needs typed binding.
                    frame = sl.read_vortex(path, client=client).agg(
                        sum_n_0="SUM(n)", sum_n_1="SUM(n - 3)")
                    relation = (native.Relation.scan(str(path)).project([
                        ("n", expr.column("n")), ("shifted", expr.column("n") - expr.literal(3))])
                        .aggregate([], [("sum", "n", "sum_n_0"), ("sum", "shifted", "sum_n_1")]))
                    tables = self.all_surfaces(
                        frame, relation, f"SELECT SUM(n),SUM(n - 3) FROM '{path}' ORDER BY SUM(n-3) NULLS FIRST")
                    for table in tables[1:]:
                        self.assertTrue(table.equals(tables[0], check_metadata=True), (table, tables[0]))

    def test_typed_unary_file_input_matches_existing_dataframe_and_sql(self):
        import shardloom as sl
        if __package__:
            from .test_native_unary_binding import apply
        else:
            from test_native_unary_binding import apply

        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        with tempfile.TemporaryDirectory(prefix="shardloom-shared-unary-") as directory:
            path = Path(directory) / "input.vortex"
            source = self.session.from_arrow(self.pa.Table.from_pylist(ROWS, schema=arrow_schema()))
            self.session.plan(self.native.Relation.scan("memory://rows"),
                              sources=[("memory://rows", source)]).write(path, "write_vortex")
            for kind in KINDS:
                with self.subTest(kind=kind):
                    typed = typed_unary(self.native.Relation.scan(str(path)), kind)
                    frame = apply(sl.read_vortex(path, client=client), kind)
                    statement = frame._relation_statement()
                    wanted = self.pa.Table.from_pylist(expected(kind, ROWS), schema=arrow_schema(kind))
                    tables = self.all_surfaces(frame, typed, statement)
                    for table in tables:
                        self.assertTrue(table.equals(wanted, check_metadata=True), (table, wanted))

    def test_typed_dynamic_pivot_keeps_downstream_binding_and_reuse(self):
        native, session = self.native, self.session
        expr = native.Expression
        rows = [{"entity": 1, "category": "a", "amount": 3},
                {"entity": 1, "category": "b", "amount": 5},
                {"entity": 2, "category": "a", "amount": 7}]
        source = session.from_rows(rows)
        options = json.dumps({"index": "entity", "columns": "category", "values": "amount", "aggregate": "sum"})
        relation = (native.Relation.scan("memory://pivot").unary("pivot", options)
                    .filter(expr.column("pivot_a") > expr.literal(4.0))
                    .project([("entity", expr.column("entity")), ("amount", expr.column("pivot_a"))]))
        plan = session.plan(relation, sources=[("memory://pivot", source)])
        for _ in range(2):
            self.assertEqual(self.table(plan.collect()).to_pylist(), [{"entity": 2, "amount": 7.0}])
        ranged = (native.Relation.scan("memory://pivot").unary("pivot", options)
                  .sort([("entity", False, "first")]).limit(1, offset=1)
                  .project([("entity", expr.column("entity")), ("amount", expr.column("pivot_a"))]))
        sql = ("SELECT entity,pivot_a AS amount FROM PIVOT((SELECT * FROM 'memory://pivot'), "
               f"'{options}') AS p ORDER BY entity NULLS FIRST LIMIT 1 OFFSET 1")
        for ranged_plan in (session.plan(ranged, sources=[("memory://pivot", source)]),
                            session.sql(sql, sources=[("memory://pivot", source)])):
            self.assertEqual(self.table(ranged_plan.collect()).to_pylist(), [{"entity": 2, "amount": 7.0}])
        invalid = native.Relation.scan("memory://pivot").unary("pivot", options).project(
            [("missing", expr.column("pivot_missing"))])
        with self.assertRaises(Exception):
            session.plan(invalid, sources=[("memory://pivot", source)]).collect()

    def test_typed_unary_rejects_invalid_metadata_before_source_access(self):
        scan = self.native.Relation.scan("missing-file-that-must-not-be-opened.vortex")
        cases = [("unknown", "*", None), ("tail", "0", None), ("tail", "-1", None),
                 ("tail", "1", "last"), ("drop_duplicates", "n", "middle"),
                 ("sample", '{"n":1,"fraction":0.5}', None),
                 ("sample", '{"n":1,"unknown":true}', None),
                 ("melt", '{"value_columns":["n"],"unknown":true}', None),
                 ("melt", '{"value_columns":["n"],"value_vars":["n"]}', None),
                 ("explode", '{"column":"n","column":"n"}', None),
                 ("pivot", " " * 65_537, None)]
        for operation, options, keep in cases:
            with self.subTest(operation=operation, options=options[:80]), self.assertRaises(Exception) as caught:
                scan.unary(operation, options, keep=keep)
            self.assertNotIn("No such file", str(caught.exception))

    def test_sql_collection_membership_is_rebound_for_directory_and_manifest(self):
        session, native = self.session, self.native
        with tempfile.TemporaryDirectory(prefix="shardloom-sql-collection-") as directory:
            root = Path(directory)
            parts = root / "parts"
            parts.mkdir()
            def write_part(name, start, count):
                source = session.from_range("value", start, 1, count)
                session.plan(native.Relation.scan("memory://part"),
                             sources=[("memory://part", source)]).write(parts / name, "write_vortex")

            write_part("a.vortex", 0, 2)
            manifest = root / "parts.json"
            manifest.write_text(json.dumps({"inputs": ["parts/a.vortex"]}), encoding="utf8")
            plans = [session.sql(f"SELECT COUNT(*) AS total FROM '{path}'", files=[(str(path), "vortex", None)])
                     for path in (parts, manifest)]
            for plan in plans:
                self.assertEqual(self.table(plan.collect()).to_pylist(), [{"total": 2}])
            write_part("b.vortex", 2, 3)
            manifest.write_text(json.dumps({"inputs": ["parts/b.vortex", "parts/a.vortex"]}), encoding="utf8")
            for plan in plans:
                result = plan.collect()
                self.assertEqual(self.table(result).to_pylist(), [{"total": 5}])
                self.assertEqual(result.report["resident_native_preparation_reused"], "false")

    def test_dataframe_offsets_keep_operator_position_and_match_typed_sql(self):
        import shardloom as sl
        native, session, pa = self.native, self.session, self.pa
        expr = native.Expression
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        with tempfile.TemporaryDirectory(prefix="shardloom-shared-offset-") as directory:
            path = Path(directory) / "input.vortex"
            data = pa.table({"value": [5, 0, 4, 1, 3, 2]},
                            schema=pa.schema([pa.field("value", pa.int64(), nullable=False)]))
            source = session.from_arrow(data)
            session.plan(native.Relation.scan("memory://input"),
                         sources=[("memory://input", source)]).write(path, "write_vortex")
            frame = sl.read_vortex(path, client=client).sort("value")
            typed = native.Relation.scan(str(path)).sort([("value", False, None)])
            ordered = f"SELECT * FROM '{path}' ORDER BY value"
            ranged = f"SELECT * FROM ({ordered} LIMIT 4 OFFSET 1) AS input"
            cases = [
                (frame.limit(2, offset=2), typed.limit(2, offset=2), ordered + " LIMIT 2 OFFSET 2",
                 [{"value": 2}, {"value": 3}]),
                (frame.limit(0, offset=3), typed.limit(0, offset=3), ordered + " LIMIT 0 OFFSET 3", []),
                (frame.limit(4, offset=1).filter(sl.col("value") > 2).sort("value", descending=True),
                 typed.limit(4, offset=1).filter(expr.column("value") > expr.literal(2)).sort([("value", True, None)]),
                 ranged + " WHERE value > 2 ORDER BY value DESC", [{"value": 4}, {"value": 3}]),
                (frame.limit(4, offset=1).agg("SUM(value) AS total"),
                 typed.limit(4, offset=1).aggregate([], [("sum", "value", "total")]),
                 f"SELECT SUM(value) AS total FROM ({ordered} LIMIT 4 OFFSET 1) AS input", [{"total": 10}]),
                (frame.limit(4, offset=1).limit(2, offset=1), typed.limit(4, offset=1).limit(2, offset=1),
                 ranged + " LIMIT 2 OFFSET 1", [{"value": 2}, {"value": 3}]),
            ]
            for index, (declaration, relation, sql, wanted) in enumerate(cases):
                with self.subTest(case=index):
                    results = self.all_surfaces(declaration, relation, sql)
                    for result in results:
                        self.assertEqual(result.to_pylist(), wanted)
                    self.assertTrue(all(table.schema == results[0].schema for table in results))

    def test_shared_scalar_spellings_preserve_nulls_units_and_unicode(self):
        import shardloom as sl
        native, session, pa = self.native, self.session, self.pa
        expr = native.Expression
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        rows = [{"time": -1, "text": "https://www.example.com/α"},
                {"time": 3661, "text": "http://other.test/path"},
                {"time": None, "text": None}]
        with tempfile.TemporaryDirectory(prefix="shardloom-shared-scalars-") as directory:
            path = Path(directory) / "input.vortex"
            source = session.from_arrow(pa.Table.from_pylist(rows, schema=pa.schema([
                pa.field("time", pa.int64()), pa.field("text", pa.string())])))
            session.plan(native.Relation.scan("memory://input"),
                         sources=[("memory://input", source)]).write(path, "write_vortex")
            functions = [("m", "extract_minute", "time"), ("minute", "date_trunc_minute", "time"),
                         ("domain", "url_domain", "text"), ("regex", "regex_domain", "text"),
                         ("bytes", "octet_length", "text"), ("characters", "length", "text")]
            frame = sl.read_vortex(path, client=client)
            for alias, function, column in functions[:4]:
                frame = frame.with_column(alias, getattr(sl.col(column), function)())
            frame = frame.select("m", "minute", "domain", "regex", "octet_length(text) AS bytes", "length(text) AS characters")
            relation = native.Relation.scan(str(path)).project([
                (alias, expr.call(function, [expr.column(column)])) for alias, function, column in functions])
            sql = "SELECT " + ",".join(f"{function}({column}) AS {alias}" for alias, function, column in functions) + f" FROM '{path}'"
            wanted = [{"m": 59, "minute": -60, "domain": "example.com", "regex": "example.com",
                       "bytes": len(rows[0]["text"].encode()), "characters": len(rows[0]["text"])},
                      {"m": 1, "minute": 3660, "domain": "other.test", "regex": "other.test",
                       "bytes": len(rows[1]["text"].encode()), "characters": len(rows[1]["text"])},
                      dict.fromkeys(alias for alias, _, _ in functions)]
            results = self.all_surfaces(frame, relation, sql)
            for result in results:
                self.assertEqual(result.to_pylist(), wanted)
            self.assertTrue(all(table.schema == results[0].schema for table in results))

    def test_relational_families_share_five_frontdoors(self):
        import shardloom as sl
        native, session, pa = self.native, self.session, self.pa
        expr = native.Expression
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        left_rows = [{"key": 1, "value": 5, "tag": "a"}, {"key": 2, "value": 7, "tag": "b"},
                     {"key": 2, "value": 3, "tag": "b"}, {"key": 3, "value": 11, "tag": "c"}]
        right_rows = [{"key": 2, "value": 20, "tag": "b"}, {"key": 4, "value": 40, "tag": "d"}]
        schema = pa.schema([pa.field("key", pa.int64(), nullable=False),
                            pa.field("value", pa.int64(), nullable=False), pa.field("tag", pa.string(), nullable=False)])
        with tempfile.TemporaryDirectory(prefix="shardloom-five-surfaces-") as directory:
            paths = [Path(directory) / (name + ".vortex") for name in ("left", "right")]
            for path, rows in zip(paths, (left_rows, right_rows), strict=True):
                source = session.from_arrow(pa.Table.from_pylist(rows, schema=schema))
                session.plan(native.Relation.scan("memory://input"),
                             sources=[("memory://input", source)]).write(path, "write_vortex")
            left, right = [sl.read_vortex(path, client=client) for path in paths]
            a, b = [native.Relation.scan(str(path)) for path in paths]
            names = [(name, expr.column(name)) for name in ("key", "value", "tag")]
            inner = b.project([("key", expr.column("key"))])
            cases = [
                ("filter_project", left.filter(sl.col("value") > 4).select("key AS id", "value + 1 AS shifted", "UPPER(tag) AS text"),
                 a.filter(expr.column("value") > expr.literal(4)).project([
                     ("id", expr.column("key")), ("shifted", expr.column("value") + expr.literal(1)),
                     ("text", expr.call("upper", [expr.column("tag")]))]),
                 f"SELECT key AS id,value + 1 AS shifted,UPPER(tag) AS text FROM '{paths[0]}' WHERE value > 4",
                 [{"id": 1, "shifted": 6, "text": "A"}, {"id": 2, "shifted": 8, "text": "B"}, {"id": 3, "shifted": 12, "text": "C"}]),
                ("aggregate", left.group_by("tag").agg(total="SUM(value)", rows="COUNT(*)").sort("tag"),
                 a.aggregate(["tag"], [("sum", "value", "total"), ("count", None, "rows")]).sort([("tag", False, None)]),
                 f"SELECT tag,SUM(value) AS total,COUNT(*) AS rows FROM '{paths[0]}' GROUP BY tag ORDER BY tag",
                 [{"tag": "a", "total": 5, "rows": 1}, {"tag": "b", "total": 10, "rows": 2}, {"tag": "c", "total": 11, "rows": 1}]),
                ("join", left.join(right, on="key", how="left").select("f.key AS key", "f.value AS left_value", "d.value AS right_value"),
                 a.join(b, [("key", "key")], [("left", "key", "key"), ("left", "value", "left_value"), ("right", "value", "right_value")], kind="left"),
                 f"SELECT f.key AS key,f.value AS left_value,d.value AS right_value FROM '{paths[0]}' AS f LEFT JOIN '{paths[1]}' AS d ON f.key = d.key",
                 [dict(key=row["key"], left_value=row["value"], right_value=20 if row["key"] == 2 else None) for row in left_rows]),
                ("window", left.select("key", "value").window(sl.row_number(order_by="value", alias="position")).sort("value"),
                 a.window(["key", "value"], [native.WindowExpression("row_number", "position", order_by=[("value", False, None)])]).sort([("value", False, None)]),
                 f"SELECT key,value,ROW_NUMBER() OVER (ORDER BY value) AS position FROM '{paths[0]}' ORDER BY value",
                 [dict(key=row["key"], value=row["value"], position=i) for i, row in enumerate(sorted(left_rows, key=lambda row: row["value"]), 1)]),
            ]
            for method, kind, sql_op, keys in (("union_all", "union_all", "UNION ALL", [1, 2, 2, 2, 3, 4]),
                                              ("union", "union_distinct", "UNION", [1, 2, 3, 4]),
                                              ("intersect", "intersect", "INTERSECT", [2]),
                                              ("except_", "except", "EXCEPT", [1, 3])):
                cases.append((kind, getattr(left.select("key"), method)(right.select("key")).sort("key"),
                              a.project([("key", expr.column("key"))]).set_operation(inner, kind=kind).sort([("key", False, None)]),
                              f"SELECT key FROM '{paths[0]}' {sql_op} SELECT key FROM '{paths[1]}' ORDER BY key",
                              [{"key": key} for key in keys]))
            subqueries = [
                ("in", sl.col("key").isin_source(str(paths[1]), "key"), "key IN", {"kind": "in", "keys": [("key", "key")]}, [row for row in left_rows if row["key"] == 2]),
                ("exists", sl.exists_source(str(paths[1])), "EXISTS", {"kind": "exists"}, left_rows),
                ("any", sl.any_source("key", ">", str(paths[1]), "key"), "key > ANY", {"kind": "quantified", "keys": [("key", "key")], "comparison": "gt"}, [left_rows[3]]),
                ("all", sl.all_source("key", "<", str(paths[1]), "key"), "key < ALL", {"kind": "quantified", "keys": [("key", "key")], "comparison": "lt", "quantifier": "all"}, [left_rows[0]]),
            ]
            for name, predicate, sql_predicate, options, wanted in subqueries:
                relation = a.subquery(inner, "member", **options).filter(expr.column("member")).project(names)
                cases.append((name, left.filter(predicate), relation,
                              f"SELECT * FROM '{paths[0]}' WHERE {sql_predicate} (SELECT key FROM '{paths[1]}')", wanted))
            options = json.dumps({"index": "key", "columns": "tag", "values": "value", "aggregate": "sum"})
            cases.append(("pivot", left.pivot_table(index="key", columns="tag", values="value", aggfunc="sum").sort("key"),
                          a.unary("pivot", options).sort([("key", False, None)]),
                          f"SELECT * FROM PIVOT((SELECT * FROM '{paths[0]}'), '{options}') AS p ORDER BY key",
                          [{"key": 1, "pivot_a": 5.0, "pivot_b": None, "pivot_c": None},
                           {"key": 2, "pivot_a": None, "pivot_b": 10.0, "pivot_c": None},
                           {"key": 3, "pivot_a": None, "pivot_b": None, "pivot_c": 11.0}]))
            for name, frame, relation, sql, wanted in cases:
                with self.subTest(family=name):
                    tables = self.all_surfaces(frame, relation, sql)
                    for table in tables:
                        self.assertEqual(table.to_pylist(), wanted)
                    self.assertTrue(all(table.schema == tables[0].schema for table in tables),
                                    [str(table.schema) for table in tables])


    def test_ordered_collections_match_all_five_surfaces(self):
        import shardloom as sl
        from shardloom import functions as f
        import pyarrow.parquet as parquet

        native, session, pa = self.native, self.session, self.pa
        for construct in (
            lambda: native.AggregateMeasure("string_agg", "text", "invalid"),
            lambda: native.AggregateMeasure("list", "value", "invalid", separator=""),
            lambda: native.WindowExpression("string_agg", "invalid", source_column="text"),
            lambda: native.WindowExpression("list", "invalid", source_column="value", separator=""),
        ):
            with self.assertRaisesRegex(ValueError, "separator"):
                construct()
        for separator in (3, True, [], {}):
            with self.assertRaises(TypeError):
                native.AggregateMeasure("string_agg", "text", "invalid", separator=separator)
            with self.assertRaises(TypeError):
                native.WindowExpression("string_agg", "invalid", source_column="text", separator=separator)
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        values = [None, 2**60 + 1, 2**60 + 1, 2**60 + 3, 2**60 + 2,
                  8, None, 9, None, None, 0, 1]
        texts = ["z", None, "東京", "", "a", "b", None, "b", None, None, "", ""]
        orders = [3, 2, 1, None, 1, 2, 0, 2, 0, 1, None, 0]
        rows = [{"id": i, "fund": "A" if i < 5 else "B" if i < 8 else "C" if i < 10 else "D",
                 "value": values[i], "text": texts[i], "local_order": orders[i], "unused": "not requested"}
                for i in range(12)]
        schema = pa.schema([pa.field("id", pa.uint64(), nullable=False),
                            pa.field("fund", pa.string(), nullable=False),
                            pa.field("value", pa.uint64()), pa.field("text", pa.string()),
                            pa.field("local_order", pa.int32()),
                            pa.field("unused", pa.string(), nullable=False)])
        table = pa.Table.from_pylist(rows, schema=schema)
        orderspecs = (("default", None), ("asc", False), ("desc", True))

        def ordered(candidates, direction):
            if direction is None:
                return sorted(candidates, key=lambda r: r["id"])
            return sorted(candidates, key=lambda r: (r["local_order"] is None,
                -(r["local_order"] or 0) if direction else (r["local_order"] or 0), r["id"]))

        def list_ref(candidates, direction):
            selected = ordered(candidates, direction)
            return [r["value"] for r in selected] if selected else None

        def string_ref(candidates, direction, separator):
            selected = [r["text"] for r in ordered(candidates, direction) if r["text"] is not None]
            return separator.join(selected) if selected else None

        with tempfile.TemporaryDirectory(prefix="shardloom-ordered-collections-") as directory:
            for source_format in ("vortex", "parquet"):
                path = Path(directory) / f"input.{source_format}"
                if source_format == "vortex":
                    source = session.from_arrow(table)
                    session.plan(native.Relation.scan("memory://ordered-collections"),
                        sources=[("memory://ordered-collections", source)]).write(path, "write_vortex")
                else:
                    parquet.write_table(table, path, row_group_size=1)
                read = sl.read_vortex if source_format == "vortex" else sl.read_parquet
                aliases = ["list_default", "list_asc", "list_desc", "string_default", "string_asc", "string_desc"]
                frame = read(path, client=client).group_by("fund").agg(
                    list_default=f.array_agg("value"),
                    list_asc=f.list("value", order_by=[("local_order", False, "last")]),
                    list_desc=f.array_agg("value", order_by=[("local_order", True, "last")]),
                    string_default=f.string_agg("text", " / "),
                    string_asc=f.string_agg("text", "'東京,", order_by=[("local_order", False, "last")]),
                    string_desc=f.string_agg("text", "", order_by=[("local_order", True, "last")]),
                ).sort("fund")
                measures = [native.AggregateMeasure("list", "value", "list_default"),
                    native.AggregateMeasure("array_agg", "value", "list_asc", order_by=[("local_order", False, "last")]),
                    native.AggregateMeasure("list", "value", "list_desc", order_by=[("local_order", True, "last")]),
                    native.AggregateMeasure("string_agg", "text", "string_default", separator=" / "),
                    native.AggregateMeasure("string_agg", "text", "string_asc", separator="'東京,", order_by=[("local_order", False, "last")]),
                    native.AggregateMeasure("string_agg", "text", "string_desc", separator="", order_by=[("local_order", True, "last")])]
                relation = native.Relation.scan(str(path)).aggregate(["fund"], measures).sort([("fund", False, None)])
                sql_items = ["LIST(value) AS list_default",
                    "ARRAY_AGG(value ORDER BY local_order ASC NULLS LAST) AS list_asc",
                    "LIST(value ORDER BY local_order DESC NULLS LAST) AS list_desc",
                    "STRING_AGG(text, ' / ') AS string_default",
                    "STRING_AGG(text, '''東京,' ORDER BY local_order ASC NULLS LAST) AS string_asc",
                    "STRING_AGG(text, '' ORDER BY local_order DESC NULLS LAST) AS string_desc"]
                grouped_sql = f"SELECT fund,{','.join(sql_items)} FROM '{path}' GROUP BY fund ORDER BY fund"
                expected = [{"fund": fund,
                    **{f"list_{mode}": list_ref([r for r in rows if r["fund"] == fund], direction)
                       for mode, direction in orderspecs},
                    "string_default": string_ref([r for r in rows if r["fund"] == fund], None, " / "),
                    "string_asc": string_ref([r for r in rows if r["fund"] == fund], False, "'東京,"),
                    "string_desc": string_ref([r for r in rows if r["fund"] == fund], True, "")}
                    for fund in ("A", "B", "C", "D")]
                demand = {"native_source_demand_projected_columns": "4",
                          "native_source_demand_available_columns": "6"} if source_format == "parquet" else None
                for actual in self.all_surfaces(frame, relation, grouped_sql, demand):
                    self.assertEqual(actual.column_names, ["fund", *aliases])
                    self.assertEqual(actual.schema.field("fund").type, pa.string())
                    self.assertFalse(actual.schema.field("fund").nullable)
                    for name in ("list_default", "list_asc", "list_desc"):
                        self.assertEqual(actual.schema.field(name).type, pa.list_(pa.field("item", pa.uint64(), nullable=True)))
                        self.assertTrue(actual.schema.field(name).nullable)
                    for name in aliases[3:]:
                        self.assertEqual(actual.schema.field(name).type, pa.string())
                        self.assertTrue(actual.schema.field(name).nullable)
                    self.assertEqual(actual.to_pylist(), expected)

                window_spec = sl.Window.partition_by("fund").order_by("id").rows_between(-1, 1)
                window_exprs = [
                    f.array_agg("value").over(window_spec).alias("list_default"),
                    f.list("value", order_by=[("local_order", False, "last")]).over(window_spec).alias("list_asc"),
                    f.array_agg("value", order_by=[("local_order", True, "last")]).over(window_spec).alias("list_desc"),
                    f.string_agg("text", " / ").over(window_spec).alias("string_default"),
                    f.string_agg("text", "'東京,", order_by=[("local_order", False, "last")]).over(window_spec).alias("string_asc"),
                    f.string_agg("text", "", order_by=[("local_order", True, "last")]).over(window_spec).alias("string_desc"),
                ]
                frame = native.WindowFrame(
                    "rows", ("preceding", native.FrameOffset.number(native.Expression.literal(1))),
                    ("following", native.FrameOffset.number(native.Expression.literal(1))),
                )
                window_measures = [
                    native.WindowExpression("list", "list_default", source_column="value",
                        partition_by=["fund"], order_by=[("id", False, None)], frame=frame),
                    native.WindowExpression("array_agg", "list_asc", source_column="value",
                        aggregate_order_by=[("local_order", False, "last")], partition_by=["fund"],
                        order_by=[("id", False, None)], frame=frame),
                    native.WindowExpression("list", "list_desc", source_column="value",
                        aggregate_order_by=[("local_order", True, "last")], partition_by=["fund"],
                        order_by=[("id", False, None)], frame=frame),
                    native.WindowExpression("string_agg", "string_default", source_column="text", separator=" / ",
                        partition_by=["fund"], order_by=[("id", False, None)], frame=frame),
                    native.WindowExpression("string_agg", "string_asc", source_column="text", separator="'東京,",
                        aggregate_order_by=[("local_order", False, "last")], partition_by=["fund"],
                        order_by=[("id", False, None)], frame=frame),
                    native.WindowExpression("string_agg", "string_desc", source_column="text", separator="",
                        aggregate_order_by=[("local_order", True, "last")], partition_by=["fund"],
                        order_by=[("id", False, None)], frame=frame),
                ]
                window_relation = native.Relation.scan(str(path)).window(["id", "fund"], window_measures).sort(
                    [("id", False, None)])
                clause = " OVER (PARTITION BY fund ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)"
                window_sql_items = [
                    f"LIST(value){clause} AS list_default",
                    f"ARRAY_AGG(value ORDER BY local_order ASC NULLS LAST){clause} AS list_asc",
                    f"LIST(value ORDER BY local_order DESC NULLS LAST){clause} AS list_desc",
                    f"STRING_AGG(text, ' / '){clause} AS string_default",
                    f"STRING_AGG(text, '''東京,' ORDER BY local_order ASC NULLS LAST){clause} AS string_asc",
                    f"STRING_AGG(text, '' ORDER BY local_order DESC NULLS LAST){clause} AS string_desc",
                ]
                window_sql = f"SELECT id,fund,{','.join(window_sql_items)} FROM '{path}' ORDER BY id"
                window_expected = []
                for row in rows:
                    candidates = [other for other in rows if other["fund"] == row["fund"]
                                  and abs(other["id"] - row["id"]) <= 1]
                    window_expected.append({"id": row["id"], "fund": row["fund"],
                        "list_default": list_ref(candidates, None),
                        "list_asc": list_ref(candidates, False),
                        "list_desc": list_ref(candidates, True),
                        "string_default": string_ref(candidates, None, " / "),
                        "string_asc": string_ref(candidates, False, "'東京,"),
                        "string_desc": string_ref(candidates, True, "")})
                window_demand = {"native_source_demand_projected_columns": "5",
                                 "native_source_demand_available_columns": "6"} if source_format == "parquet" else None
                for actual in self.all_surfaces(
                    read(path, client=client).select("id", "fund", *window_exprs).sort("id"),
                    window_relation, window_sql, window_demand,
                ):
                    self.assertEqual(actual.column_names, ["id", "fund", *aliases])
                    self.assertEqual(actual.schema.field("id").type, pa.uint64())
                    self.assertFalse(actual.schema.field("id").nullable)
                    self.assertEqual(actual.schema.field("fund").type, pa.string())
                    self.assertFalse(actual.schema.field("fund").nullable)
                    for name in ("list_default", "list_asc", "list_desc"):
                        self.assertEqual(actual.schema.field(name).type, pa.list_(pa.field("item", pa.uint64(), nullable=True)))
                        self.assertTrue(actual.schema.field(name).nullable)
                    for name in aliases[3:]:
                        self.assertEqual(actual.schema.field(name).type, pa.string())
                        self.assertTrue(actual.schema.field(name).nullable)
                    self.assertEqual(actual.to_pylist(), window_expected)

                # The empty filtered scalar aggregate checks empty-input NULLs on every surface.
                empty_frame = read(path, client=client).filter("id < 0").agg(
                    list_empty=f.array_agg("value"), string_empty=f.string_agg("text", ""))
                empty_relation = native.Relation.scan(str(path)).filter(
                    native.Expression.column("id") < native.Expression.literal(0)).aggregate([], [
                    native.AggregateMeasure("list", "value", "list_empty"),
                    native.AggregateMeasure("string_agg", "text", "string_empty", separator="")])
                empty_sql = f"SELECT LIST(value) AS list_empty, STRING_AGG(text, '') AS string_empty FROM '{path}' WHERE id < 0"
                for actual in self.all_surfaces(empty_frame, empty_relation, empty_sql):
                    self.assertEqual(actual.column_names, ["list_empty", "string_empty"])
                    self.assertEqual(actual.schema.field("list_empty").type,
                                     pa.list_(pa.field("item", pa.uint64(), nullable=True)))
                    self.assertTrue(actual.schema.field("list_empty").nullable)
                    self.assertEqual(actual.schema.field("string_empty").type, pa.string())
                    self.assertTrue(actual.schema.field("string_empty").nullable)
                    self.assertEqual(actual.to_pylist(), [{"list_empty": None, "string_empty": None}])


if __name__ == "__main__":
    unittest.main()
