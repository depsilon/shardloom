# SPDX-License-Identifier: Apache-2.0
"""Independent time-grid and interpolation references for five public interfaces."""

from bisect import bisect_right
from datetime import datetime, timezone
from decimal import Decimal
from fractions import Fraction
import os
import math
from pathlib import Path
import random
import tempfile
from unittest.mock import patch
from zoneinfo import ZoneInfo


def _client(case):
    import shardloom as sl
    client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
    case.addCleanup(client.close)
    return client


def _write(case, path, arrays, fields):
    pa = case.pa
    source = pa.Table.from_arrays(arrays, schema=pa.schema(fields))
    with pa.OSFile(str(path), "wb") as stream, pa.ipc.new_file(stream, source.schema) as writer:
        for batch in source.to_batches(max_chunksize=7):
            writer.write_batch(batch)
    return source


def _source(case, path, dtype, ticks):
    pa = case.pa
    values = [None if i % 7 == 0 else (i * 13 % 31) - 15 for i in range(len(ticks))]
    funds = [None if i % 3 == 0 else "東京" for i in range(len(ticks))]
    arrays = [pa.array(range(len(ticks)), pa.uint64()), pa.array(ticks, dtype),
              pa.array(funds, pa.string()), pa.array(values, pa.int64())]
    fields = [pa.field("id", pa.uint64(), nullable=False), pa.field("instant", dtype),
              pa.field("fund", pa.string()), pa.field("value", pa.int64())]
    return _write(case, path, arrays, fields), values, funds


def _equal(case, tables, expected):
    for actual in tables:
        case.assertEqual(actual.schema, expected.schema)
        if "linear" not in expected.column_names:
            case.assertTrue(actual.equals(expected), f"{actual}\n{expected}")
            continue
        case.assertTrue(actual.drop(["linear"]).equals(expected.drop(["linear"])),
                        f"{actual}\n{expected}")
        for observed, wanted in zip(actual["linear"].to_pylist(), expected["linear"].to_pylist()):
            if wanted is None:
                case.assertIsNone(observed)
            else:
                case.assertTrue(observed is not None and math.isclose(observed, wanted, rel_tol=2e-14, abs_tol=2e-14),
                                (observed, wanted))


def _fills(times, values, descending=False):
    """Independent bracket search and exact rational interpolation."""
    ordered = sorted(range(len(times)), key=lambda i: times[i], reverse=descending)
    present = [i for i in ordered if values[i] is not None]
    answer = {name: [] for name in ("forward", "backward", "linear")}
    rank = {index: position for position, index in enumerate(ordered)}
    for i, value in enumerate(values):
        left = [j for j in present if rank[j] <= rank[i]]
        right = [j for j in present if rank[j] >= rank[i]]
        left, right = (left[-1] if left else None), (right[0] if right else None)
        answer["forward"].append(None if left is None else values[left])
        answer["backward"].append(None if right is None else values[right])
        if value is not None:
            linear = float(value)
        elif left is None or right is None:
            linear = None
        else:
            weight = Fraction(times[i] - times[left], times[right] - times[left])
            linear = float(Fraction(values[left]) + (Fraction(values[right]) - Fraction(values[left])) * weight)
        answer["linear"].append(linear)
    return answer


def _series(case, path, client, every, unit, origin, *, start=None, end=None,
            month_end="reject", ambiguity="reject", empty=False):
    import shardloom as sl
    from shardloom import functions as f
    native, expr = case.native, case.native.Expression
    col, lit = expr.column, expr.literal
    options = dict(origin=origin, month_end=month_end, ambiguity=ambiguity)
    raw = sl.read_arrow_ipc(path, client=client).filter(sl.col("instant").is_not_null())
    typed = native.Relation.scan(str(path)).filter(col("instant").is_not_null())
    where = "instant IS NOT NULL"
    if empty:
        raw, typed = raw.filter("id < 0"), typed.filter(col("id") < lit(0))
        where += " AND id < 0"
    frame = raw.resample("instant", every, unit, by="fund", **options).agg(total=f.sum("value"))
    frame = frame.gap_fill("instant", every, unit, by="fund", start=start, end=end, **options)
    bucket = expr.call("temporal_bucket", [col("instant"), lit(every), lit(unit),
                                            lit(origin), lit(month_end), lit(ambiguity)])
    relation = typed.project([("fund", col("fund")), ("instant", bucket), ("value", col("value"))])
    relation = relation.aggregate(["fund", "instant"], [("sum", "value", "total")])
    relation = relation.gap_fill("instant", every, unit, by=["fund"], start=start, end=end, **options)
    tick = lambda value: "NULL" if value is None else str(value)
    bucket_sql = f"TEMPORAL_BUCKET(instant,{every},'{unit}',{tick(origin)},'{month_end}','{ambiguity}')"
    grouped = f"SELECT fund,instant,SUM(value) AS total FROM (SELECT fund,{bucket_sql} AS instant,value FROM '{path}' WHERE {where}) AS bucketed GROUP BY fund,instant"
    filled = f"GAP_FILL(({grouped}), 'instant',{every},'{unit}','fund',{tick(start)},{tick(end)},{tick(origin)},'{month_end}','{ambiguity}') AS filled"
    window = sl.Window.partition_by("fund").order_by("instant")
    declarations = [getattr(sl.col("total"), name)().over(window).alias(alias)
                    for name, alias in (("forward_fill", "forward"), ("backward_fill", "backward"), ("interpolate_linear", "linear"))]
    frame = frame.select("fund", "instant", "total", *declarations).sort("fund", "instant", nulls="first")
    relation = relation.window(["fund", "instant", "total"], [native.WindowExpression(
        name, alias, source_column="total", partition_by=["fund"], order_by=[("instant", False, None)])
        for name, alias in (("forward_fill", "forward"), ("backward_fill", "backward"), ("interpolate_linear", "linear"))])
    relation = relation.sort([("fund", False, "first"), ("instant", False, "first")])
    statement = f"SELECT fund,instant,total,{','.join(map(str, declarations))} FROM {filled} ORDER BY fund NULLS FIRST,instant"
    return frame, relation, statement


def _series_expected(case, dtype, ticks, values, funds, bucket, points, *, start=None, end=None):
    grouped = {}
    for tick, value, fund in zip(ticks, values, funds):
        if tick is None:
            continue
        key = (fund, bucket(tick))
        grouped.setdefault(key, [])
        if value is not None:
            grouped[key].append(value)
    output = {name: [] for name in ("fund", "instant", "total", "forward", "backward", "linear")}
    for fund in sorted({key[0] for key in grouped}, key=lambda item: (item is not None, item or "")):
        observed = [time for key, time in grouped if key == fund]
        first = min(observed) if start is None else start
        last = max(observed) if end is None else end
        times = list(points(first, last))
        totals = [float(sum(grouped[(fund, time)])) if grouped.get((fund, time)) else None for time in times]
        output["fund"].extend([fund] * len(times))
        output["instant"].extend(times)
        output["total"].extend(totals)
        for name, result in _fills(times, totals).items():
            output[name].extend(result)
    pa = case.pa
    fields = [pa.field("fund", pa.string()), pa.field("instant", dtype, nullable=False)]
    fields += [pa.field(name, pa.float64()) for name in ("total", "forward", "backward", "linear")]
    return pa.Table.from_arrays([pa.array(output[field.name], field.type) for field in fields], schema=pa.schema(fields))


def check_units(case):
    import shardloom as sl
    pa, native, client = case.pa, case.native, _client(case)
    domains = []
    for unit, factor in (("s", 1), ("ms", 1000), ("us", 10**6), ("ns", 10**9)):
        domains.extend((dtype, factor, 2, "s", False) for dtype in
                       (pa.timestamp(unit), pa.timestamp(unit, "America/Chicago"), pa.duration(unit)))
        domains.append((pa.time32(unit) if unit in ("s", "ms") else pa.time64(unit), factor, 2, "s", True))
    domains.extend([(pa.date32(), 1, 172_800, "s", False), (pa.date64(), 86_400_000, 172_800, "s", False)])
    with tempfile.TemporaryDirectory(prefix="shardloom-grid-units-") as directory:
        for index, (dtype, factor, every, unit, positive) in enumerate(domains):
            origin, width = (21 if positive else 1) * factor, 2 * factor
            ticks = [None if i % 13 == 0 else (((i * 17) % 31 - 15) + (20 if positive else 0)) * factor for i in range(47)]
            path = Path(directory) / f"unit-{index}.arrow"
            source, values, funds = _source(case, path, dtype, ticks)
            bucket = lambda tick: origin + (tick-origin) // width * width
            expected = pa.Table.from_arrays([source["id"], pa.array([None if tick is None else bucket(tick) for tick in ticks], dtype)],
                schema=pa.schema([source.schema.field("id"), pa.field("bucket", dtype)]))
            expression = sl.col("instant").temporal_bucket(every, unit, origin=origin)
            frame = sl.read_arrow_ipc(path, client=client).select("id", expression.alias("bucket"))
            native_expr = native.Expression.call("temporal_bucket", [native.Expression.column("instant"),
                native.Expression.literal(every), native.Expression.literal(unit), native.Expression.literal(origin)])
            relation = native.Relation.scan(str(path)).project([("id", native.Expression.column("id")), ("bucket", native_expr)])
            statement = f"SELECT id,{expression} AS bucket FROM '{path}'"
            with case.subTest(domain=str(dtype), operation="bucket"):
                _equal(case, case.all_surfaces(frame, relation, statement), expected)
            observed = [bucket(tick) for tick in ticks if tick is not None]
            start, end = min(observed) - width, max(observed) + width
            declarations = _series(case, path, client, every, unit, origin, start=start, end=end)
            wanted = _series_expected(case, dtype, ticks, values, funds, bucket,
                                      lambda first, last: range(first, last+1, width), start=start, end=end)
            with case.subTest(domain=str(dtype), operation="resample-gap-interpolate"):
                _equal(case, case.all_surfaces(*declarations), wanted)
            if index == 0:
                _equal(case, case.all_surfaces(*_series(case, path, client, every, unit, origin, empty=True)), wanted.slice(0, 0))


def check_calendar(case):
    import shardloom as sl
    pa, native, client = case.pa, case.native, _client(case)
    zone = ZoneInfo("America/New_York")
    populations = []
    for label, dates in [("spring", [(2024, 3, day) for day in range(7, 16)]),
                         ("fold", [(2024, 11, day) for day in range(1, 10)]),
                         ("month", [(2024, 1, 31), (2024, 2, 29), (2024, 3, 31), (2024, 4, 30), (2024, 5, 31), (2024, 6, 30), (2024, 7, 31), (2024, 8, 31), (2024, 9, 30)])]:
        grid = [int(datetime(*date, tzinfo=zone).timestamp()) * 10**9 for date in dates]
        populations.append((label, grid, "calendar_months" if label == "month" else "calendar_days", "reject"))
    for label, month, first in (("gap", 3, 9), ("ambiguous", 11, 2)):
        for policy in ("earlier", "later"):
            grid = []
            for day in range(first, first + 9):
                local = datetime(2024, month, day, 2 if month == 3 else 1, 30)
                candidates = [int(local.replace(tzinfo=zone, fold=fold).timestamp()) * 10**9 for fold in (0, 1)]
                grid.append(min(candidates) if policy == "earlier" else max(candidates))
            populations.append((f"{label}-{policy}", grid, "calendar_days", policy))
    with tempfile.TemporaryDirectory(prefix="shardloom-grid-calendar-") as directory:
        for label, grid, unit, ambiguity in populations:
            dtype = pa.timestamp("ns", "America/New_York")
            # Include absent bins, multiple events per bin, negative values and partition NULLs.
            ticks = [grid[i] + offset for i in (1, 2, 5, 7) for offset in (0, 1, 3_600_000_000_000)]
            ticks += [None]
            random.Random(0x6A1D).shuffle(ticks)
            path = Path(directory) / f"{label}.arrow"
            source, values, funds = _source(case, path, dtype, ticks)
            bucket = lambda tick: grid[bisect_right(grid, tick)-1]
            points = lambda first, last: [point for point in grid if first <= point <= last]
            start, end = grid[0], grid[-1]
            wanted = _series_expected(case, dtype, ticks, values, funds, bucket, points, start=start, end=end)
            declarations = _series(case, path, client, 1, unit, grid[0], start=start, end=end,
                                   month_end="clamp" if unit == "calendar_months" else "reject", ambiguity=ambiguity)
            with case.subTest(population=label):
                _equal(case, case.all_surfaces(*declarations), wanted)
            clipped = _series(case, path, client, 1, unit, grid[0], start=grid[2], end=grid[5],
                              month_end="clamp" if unit == "calendar_months" else "reject", ambiguity=ambiguity)
            clipped_expected = _series_expected(case, dtype, ticks, values, funds, bucket, points, start=grid[2], end=grid[5])
            _equal(case, case.all_surfaces(*clipped), clipped_expected)
            frame, relation, statement = declarations
            producer = native.Session(native.ExecutionResources(128 << 20, 2))
            retained = producer.plan(relation).collect()
            producer.close()
            outputs = []
            for fmt, reader in (("vortex", sl.read_vortex), ("parquet", sl.read_parquet), ("arrow-ipc", sl.read_arrow_ipc)):
                output = Path(directory) / f"{label}.{fmt}"
                written = frame.write(output, output_format=fmt, check=True)
                case.assertFalse(written.envelope.fallback.attempted)
                case.assertEqual(written.envelope.field("external_engine_invoked"), "false")
                outputs.append((output, reader))
            path.unlink()
            for output, reader in outputs:
                _equal(case, [reader(output, client=client).collect(check=True).to_arrow()], wanted)
            receiver = native.Session(native.ExecutionResources(128 << 20, 2))
            reused = receiver.sql("SELECT * FROM 'memory://grid'", sources=[("memory://grid", retained.as_source())]).collect()
            receiver.close()
            _equal(case, [case.table(reused), case.table(retained)], wanted)
        _check_empty_and_rich_gaps(case, Path(directory), client)
        _check_streamed_grid(case, Path(directory))


def _check_streamed_grid(case, directory):
    import shardloom as sl
    from shardloom import functions as f, types as t
    pa = case.pa
    source = pa.table({"fund": ["east", "east", "east"],
                       "instant": pa.array([4, 0, 0], pa.timestamp("s")),
                       "value": pa.array([30, 4, 6], pa.int64())})
    fields = [pa.field("fund", pa.string()), pa.field("instant", pa.timestamp("s"), nullable=False)]
    fields += [pa.field(name, pa.float64()) for name in ("total", "linear")]
    expected = pa.Table.from_arrays([
        pa.array(["east"] * 3), pa.array([0, 2, 4], pa.timestamp("s")),
        pa.array([10, None, 30], pa.float64()), pa.array([10, 20, 30], pa.float64()),
    ], schema=pa.schema(fields))
    for mode in ("collect", "batches", "write", "empty", "late_error", "zero_error"):
        with case.subTest(streamed_grid=mode):
            client, trace = _client(case), []
            def producer():
                trace.append("opened")
                try:
                    yield source.slice(0, 0)
                    if mode != "empty":
                        yield source.slice(0, 1)
                        yield source.slice(0, 0)
                        yield source.slice(1)
                    if mode.endswith("error"):
                        raise LookupError("grid producer refused")
                    trace.append("ended")
                finally:
                    trace.append("closed")
            raw = sl.from_batches(producer, schema={"fund": t.string(), "instant": t.timestamp("s"),
                                                    "value": t.int64()}, streaming=True, client=client)
            query = raw.resample("instant", 2, "s", by="fund").agg(total=f.sum("value"))
            query = query.gap_fill("instant", 2, "s", by="fund", start=0, end=4)
            window = sl.Window.partition_by("fund").order_by("instant")
            query = query.select("fund", "instant", "total", sl.col("total").interpolate_linear().over(window).alias("linear"))
            output = directory / f"stream-{mode}.vortex"
            if mode.endswith("error"):
                if mode == "zero_error":
                    query = query.limit(0)
                with case.assertRaisesRegex(LookupError, "grid producer refused"):
                    query.write_vortex(output)
                case.assertFalse(output.exists())
                case.assertEqual(trace, ["opened", "closed"])
                client.close()
                continue
            if mode == "batches":
                with query.iter_batches(batch_rows=1) as stream:
                    chunks = [batch.to_arrow() for batch in stream]
                    report = stream.report
                actual = pa.concat_tables(chunks)
            elif mode == "write":
                report = query.write_vortex(output)
                actual = sl.read_vortex(output, client=client).collect(check=True).to_arrow()
            else:
                report = query.collect(check=True)
                client.close()
                actual = report.to_arrow()
            _equal(case, [actual], expected.slice(0, 0) if mode == "empty" else expected)
            case.assertEqual(trace, ["opened", "ended", "closed"])
            case.assertTrue(report.envelope.field_bool("native_input_end_observed"))
            case.assertEqual(report.envelope.field_int("native_input_batch_sources"), 1)
            case.assertFalse(report.envelope.fallback.attempted)
            case.assertEqual(report.envelope.field("external_engine_invoked"), "false")
            client.close()


def _check_empty_and_rich_gaps(case, directory, client):
    import shardloom as sl
    pa, native = case.pa, case.native
    path = directory / "empty.arrow"
    source, _, _ = _source(case, path, pa.timestamp("s"), [])
    fields = [pa.field(field.name, field.type, nullable=field.name != "instant") for field in source.schema]
    for by in ([], ["fund"]):
        frame = sl.read_arrow_ipc(path, client=client).gap_fill("instant", 2, "s", by=by, start=-2, end=2)
        relation = native.Relation.scan(str(path)).gap_fill("instant", 2, "s", by=by, start=-2, end=2)
        statement = f"SELECT * FROM GAP_FILL((SELECT * FROM '{path}'),'instant',2,'s','{','.join(by)}',-2,2) AS grid"
        ticks = [] if by else [-2, 0, 2]
        arrays = [pa.array(ticks if field.name == "instant" else [None]*len(ticks), field.type) for field in fields]
        _equal(case, case.all_surfaces(frame, relation, statement), pa.Table.from_arrays(arrays, schema=pa.schema(fields)))
    fields = [pa.field("instant", pa.timestamp("ns")), pa.field("fund", pa.string()),
              pa.field("payload", pa.struct([pa.field("tags", pa.list_(pa.string())),
                                             pa.field("money", pa.decimal128(18, 3))]), nullable=False)]
    payloads = [dict(tags=["東京", None], money=Decimal("1.125")), dict(tags=[], money=None)]
    path = directory / "rich-gap.arrow"
    source = _write(case, path, [pa.array([4, 0], fields[0].type), pa.array(["a", "a"]),
                               pa.array(payloads, fields[2].type)], fields)
    frame = sl.read_arrow_ipc(path, client=client).gap_fill("instant", 2, "ns", by="fund", start=-2, end=6)
    relation = native.Relation.scan(str(path)).gap_fill("instant", 2, "ns", by=["fund"], start=-2, end=6)
    statement = f"SELECT * FROM GAP_FILL((SELECT * FROM '{path}'),'instant',2,'ns','fund',-2,6) AS grid"
    expected_fields = [fields[0].with_nullable(False), fields[1], fields[2].with_nullable(True)]
    expected = pa.Table.from_arrays([pa.array([-2, 0, 2, 4, 6], fields[0].type), pa.array(["a"]*5),
        pa.array([None, payloads[1], None, payloads[0], None], fields[2].type)], schema=pa.schema(expected_fields))
    _equal(case, case.all_surfaces(frame, relation, statement), expected)


def check_fill(case):
    import shardloom as sl
    pa, native, client = case.pa, case.native, _client(case)
    fields = [pa.field("id", pa.uint64(), nullable=False), pa.field("instant", pa.timestamp("ns", "UTC")),
              pa.field("fund", pa.string()), pa.field("amount", pa.decimal128(18, 3)),
              pa.field("tags", pa.list_(pa.string()))]
    ticks = [9_000_000_000_000_000_000 + i for i in [3, 0, 0, 5, 7, 1, 2, 6, 0, 4]]
    groups = ["a", None, "a", "a", None, "a", None, "a", "all_null", "all_null"]
    amounts = [None, Decimal("0.005"), None, Decimal("0.030"), Decimal("0.019"), Decimal("0.010"), None, None, None, None]
    tags = [None if value is None else [str(value), None, "東京"] for value in amounts]
    with tempfile.TemporaryDirectory(prefix="shardloom-grid-fill-") as directory:
        path = Path(directory) / "fill.arrow"
        source = _write(case, path, [pa.array(values, field.type) for values, field in zip(
            [range(len(ticks)), ticks, groups, amounts, tags], fields)], fields)
        for descending in (False, True):
            window = sl.Window.partition_by("fund").order_by("instant", descending=descending)
            declarations = [sl.col("tags").forward_fill().over(window).alias("previous"),
                            sl.col("tags").backward_fill().over(window).alias("following"),
                            sl.col("amount").interpolate_linear().over(window).alias("linear")]
            frame = sl.read_arrow_ipc(path, client=client).select("id", *declarations).sort("id")
            relation = native.Relation.scan(str(path)).window(["id"], [native.WindowExpression(
                name, alias, source_column=column, partition_by=["fund"], order_by=[("instant", descending, None)])
                for name, alias, column in (("forward_fill", "previous", "tags"), ("backward_fill", "following", "tags"), ("interpolate_linear", "linear", "amount"))]).sort([("id", False, None)])
            statement = f"SELECT id,{','.join(map(str, declarations))} FROM '{path}' ORDER BY id"
            result = {name: [None] * len(ticks) for name in ("previous", "following", "linear")}
            for group in set(groups):
                indices = [i for i, key in enumerate(groups) if key == group]
                fills = _fills([ticks[i] for i in indices], [amounts[i] for i in indices], descending)
                for position, i in enumerate(indices):
                    for name, key in (("previous", "forward"), ("following", "backward")):
                        value = fills[key][position]
                        result[name][i] = None if value is None else [str(value), None, "東京"]
                    result["linear"][i] = fills["linear"][position]
            schema = pa.schema([fields[0], pa.field("previous", fields[-1].type),
                                pa.field("following", fields[-1].type), pa.field("linear", pa.float64())])
            expected = pa.Table.from_arrays([source["id"], pa.array(result["previous"], fields[-1].type),
                pa.array(result["following"], fields[-1].type), pa.array(result["linear"], pa.float64())], schema=schema)
            # Decimal conversion explicitly rounds at the Float64 boundary; compare exact nested values and tight numeric tolerance.
            for actual in case.all_surfaces(frame, relation, statement):
                case.assertEqual(actual.schema, expected.schema)
                case.assertTrue(actual.drop(["linear"]).equals(expected.drop(["linear"])))
                for observed, wanted in zip(actual["linear"].to_pylist(), expected["linear"].to_pylist()):
                    if wanted is None:
                        case.assertIsNone(observed)
                    else:
                        case.assertAlmostEqual(observed, wanted, delta=2e-17)


def _refuse(case, frame, relation, statement, message):
    import shardloom as sl
    from shardloom.errors import ShardLoomCommandError
    arguments = ["sql", "--sql", statement, "--request", "collect", "--bounded", "true",
                 "--memory-bytes", str(128 << 20), "--max-parallelism", "2"]
    for surface, error, run in [
        ("dataframe", ShardLoomCommandError, lambda: frame.collect(check=True)),
        ("typed", RuntimeError, lambda: case.session.plan(relation).collect()),
        ("sql", RuntimeError, lambda: case.session.sql(statement).collect()),
        ("workflow", RuntimeError, lambda: case.session.workflow(arguments).collect()),
    ]:
        with case.subTest(surface=surface), case.assertRaisesRegex(error, message):
            run()
    with patch("subprocess.Popen", case.real_popen):
        cli = sl.ShardLoomClient(binary=os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY"), transport="subprocess",
                                use_persistent_worker=False, memory_bytes=128 << 20, max_parallelism=2, timeout=30)
        try:
            with case.assertRaisesRegex(ShardLoomCommandError, message) as raised:
                sl.sql(statement, client=cli).collect(check=True)
            case.assertFalse(raised.exception.envelope.fallback.attempted)
        finally:
            cli.close()


def check_refusals(case):
    import shardloom as sl
    pa, native, client = case.pa, case.native, _client(case)
    cases = [(pa.int64(), [], 1, "s", None, None, "typed temporal"),
             (pa.timestamp("s"), [0, 0], 1, "s", 2, 4, "unique"),
             (pa.timestamp("s"), [0, None], 1, "s", None, None, "non-NULL"),
             (pa.timestamp("s"), [0, 1], 2, "s", 4, 6, "aligned"),
             (pa.timestamp("s"), [], 1, "ns", None, None, "precision"),
             (pa.timestamp("s"), [], 1, "s", 2, 1, "start"),
             (pa.timestamp("s"), [], 2, "s", 1, 4, "aligned"),
             (pa.duration("s"), [], 1, "calendar_days", None, None, "dates or timestamps")]
    with tempfile.TemporaryDirectory(prefix="shardloom-grid-refusals-") as directory:
        for index, (dtype, ticks, every, unit, start, end, message) in enumerate(cases):
            path = Path(directory) / f"invalid-{index}.arrow"
            _source(case, path, dtype, ticks)
            frame = sl.read_arrow_ipc(path, client=client).gap_fill("instant", every, unit, start=start, end=end)
            relation = native.Relation.scan(str(path)).gap_fill("instant", every, unit, start=start, end=end)
            tick = lambda value: "NULL" if value is None else str(value)
            statement = f"SELECT * FROM GAP_FILL((SELECT * FROM '{path}'),'instant',{every},'{unit}','',{tick(start)},{tick(end)}) AS grid"
            with case.subTest(case=index):
                _refuse(case, frame, relation, statement, message)
        for index, (dtype, ticks, value_type, values, message) in enumerate([
            (pa.timestamp("s"), [0, 0], pa.int64(), [1, None], "unique"),
            (pa.timestamp("s"), [0, None], pa.int64(), [1, None], "non-NULL"),
            (pa.int64(), [], pa.int64(), [], "temporal ordering"),
            (pa.timestamp("s"), [0, 1], pa.uint64(), [2**53+1, None], "precision|exact|2\\^53"),
            (pa.timestamp("s"), [0, 1], pa.float64(), [float("inf"), None], "finite"),
            (pa.timestamp("s"), [], pa.string(), [], "numeric"),
        ]):
            path = Path(directory) / f"fill-invalid-{index}.arrow"
            _write(case, path, [pa.array(ticks, dtype), pa.array(values, value_type)],
                   [pa.field("instant", dtype), pa.field("value", value_type)])
            declaration = sl.col("value").interpolate_linear().over(sl.Window.order_by("instant", nulls="last")).alias("linear")
            frame = sl.read_arrow_ipc(path, client=client).select(declaration)
            relation = native.Relation.scan(str(path)).window([], [native.WindowExpression("interpolate_linear", "linear", source_column="value", order_by=[("instant", False, "last")])])
            statement = f"SELECT {declaration} FROM '{path}'"
            with case.subTest(fill_case=index):
                _refuse(case, frame, relation, statement, message)
