# SPDX-License-Identifier: Apache-2.0
"""Independent calendar references used by the shared five-interface suite."""

import calendar
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import random
import tempfile
from unittest.mock import patch
from zoneinfo import ZoneInfo


def _calendar_shift(ticks, zone, days=0, months=0, ambiguity="earlier"):
    """Reference local-field arithmetic, independent of ShardLoom's kernel."""
    if days == months == 0:
        return ticks
    current = datetime.fromtimestamp(ticks, timezone.utc).astimezone(zone)
    local = current.replace(tzinfo=None)
    if months:
        month = local.year * 12 + local.month - 1 + months
        year, month = divmod(month, 12)
        month += 1
        local = local.replace(year=year, month=month,
                              day=min(local.day, calendar.monthrange(year, month)[1]))
    else:
        local += timedelta(days=days)
    candidates = [int(local.replace(tzinfo=zone, fold=fold).timestamp()) for fold in (0, 1)]
    return min(candidates) if ambiguity == "earlier" else max(candidates)


def _declarations(case, path, client, start, finish, native_offset, *, descending=False, exclusion="no_others", native_bounds=None):
    import shardloom as sl
    from shardloom import functions as f
    native = case.native
    spec = sl.Window.partition_by("fund").order_by("instant", descending=descending, nulls="last").range_between(start, finish).exclude(exclusion)
    declarations = [f.count().over(spec).alias("n"),
                    f.count_distinct("value").over(spec).alias("distinct_values"),
                    f.sum("value").over(spec).alias("total"),
                    f.min("value").over(spec).alias("smallest"),
                    f.max("value").over(spec).alias("largest"),
                    f.first_value("value").over(spec).alias("first_value")]
    fields = ["id", "instant", "fund", "money", "tags"]
    frame = sl.read_arrow_ipc(path, client=client).select(*fields, *declarations).sort("id")
    native_bounds = native_bounds or (("preceding", native_offset), ("following", native_offset))
    typed_frame = native.WindowFrame("range", *native_bounds, exclusion=exclusion)
    functions = [("count_all", "n", None), ("count_distinct", "distinct_values", "value"),
                 ("sum", "total", "value"), ("min", "smallest", "value"),
                 ("max", "largest", "value"), ("first_value", "first_value", "value")]
    relation = native.Relation.scan(str(path)).window(fields, [native.WindowExpression(
        function, alias, source_column=column, partition_by=["fund"],
        order_by=[("instant", descending, "last")], frame=typed_frame,
    ) for function, alias, column in functions]).sort([("id", False, None)])
    statement = f"SELECT {','.join(fields)},{','.join(map(str, declarations))} FROM '{path}' ORDER BY id"
    return frame, relation, statement


def _source(case, path, dtype, ticks):
    pa = case.pa
    values = [None if i % 11 == 0 else (i * 7 % 19) - 9 for i in range(len(ticks))]
    funds = [None if i % 3 == 0 else "東京" for i in range(len(ticks))]
    schema = pa.schema([pa.field("id", pa.uint64(), nullable=False), pa.field("instant", dtype),
        pa.field("fund", pa.string()), pa.field("money", pa.decimal128(18, 3)),
        pa.field("tags", pa.list_(pa.string())), pa.field("value", pa.int64())])
    source = pa.Table.from_arrays([
        pa.array(range(len(ticks)), pa.uint64()), pa.array(ticks, dtype), pa.array(funds),
        pa.array([None if i % 9 == 0 else Decimal(i).scaleb(-3) for i in range(len(ticks))], pa.decimal128(18, 3)),
        pa.array([None if i % 5 == 0 else ["é", None, str(i)] for i in range(len(ticks))], pa.list_(pa.string())),
        pa.array(values, pa.int64()),
    ], schema=schema)
    with pa.OSFile(str(path), "wb") as stream, pa.ipc.new_file(stream, schema) as writer:
        for batch in source.to_batches(max_chunksize=7):
            writer.write_batch(batch)
    return source, values, funds


def _expected(case, source, ticks, values, funds, bounds, descending, exclusion):
    columns = {name: [] for name in ("n", "distinct_values", "total", "smallest", "largest", "first_value")}
    for i, tick in enumerate(ticks):
        lower, upper = bounds(tick) if tick is not None else (0, 0)
        selected = [j for j, candidate in enumerate(ticks) if funds[j] == funds[i]
                    and ((tick is None and candidate is None)
                         or (tick is not None and candidate is not None and lower <= candidate <= upper))
                    and (exclusion == "no_others" or (exclusion == "current_row" and i != j)
                         or (exclusion == "group" and tick != candidate)
                         or (exclusion == "ties" and (i == j or tick != candidate)))]
        selected.sort(key=lambda j: (ticks[j] is None, (-ticks[j] if descending else ticks[j]) if ticks[j] is not None else 0, j))
        present = [values[j] for j in selected if values[j] is not None]
        for name, value in dict(n=len(selected), distinct_values=len(set(present)),
            total=float(sum(present)) if present else None,
            smallest=min(present) if present else None, largest=max(present) if present else None,
            first_value=values[selected[0]] if selected else None).items():
            columns[name].append(value)
    expected = source.select(["id", "instant", "fund", "money", "tags"])
    for name, values in columns.items():
        dtype = case.pa.uint64() if name in ("n", "distinct_values") else case.pa.float64() if name == "total" else case.pa.int64()
        expected = expected.append_column(case.pa.field(name, dtype, nullable=name not in ("n", "distinct_values")), case.pa.array(values, dtype))
    return expected


def check_fixed(case):
    import shardloom as sl
    pa, native = case.pa, case.native
    client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
    case.addCleanup(client.close)
    with tempfile.TemporaryDirectory(prefix="shardloom-calendar-unit-matrix-") as directory:
        for unit, per_second in (("s", 1), ("ms", 1000), ("us", 10**6), ("ns", 10**9)):
            domains = [pa.timestamp(unit), pa.timestamp(unit, "America/Chicago"), pa.duration(unit),
                       pa.time32(unit) if unit in ("s", "ms") else pa.time64(unit)]
            for index, dtype in enumerate(domains):
                ticks = [None if i % 17 == 0 else ((i * 13) % 29) * per_second for i in range(43)]
                path = Path(directory) / f"unit-{unit}-{index}.arrow"
                source, values, funds = _source(case, path, dtype, ticks)
                for offset, offset_unit, numerator, denominator in ((1500, "ms", per_second * 3, 2), (500, "ns", per_second, 2_000_000)):
                    # Rational boundaries retain fractional source ticks exactly.
                    from fractions import Fraction
                    radius = Fraction(numerator, denominator)
                    wanted = _expected(case, source, ticks, values, funds, lambda tick: (tick-radius, tick+radius), False, "no_others")
                    frame, relation, statement = _declarations(case, path, client,
                        sl.Window.duration(-offset, unit=offset_unit), sl.Window.duration(offset, unit=offset_unit),
                        native.FrameOffset.duration(offset, unit=offset_unit))
                    with case.subTest(dtype=str(dtype), unit=offset_unit):
                        for actual in case.all_surfaces(frame, relation, statement):
                            case.assertEqual(actual.schema, wanted.schema)
                            case.assertTrue(actual.equals(wanted), (str(dtype), actual.to_pylist(), wanted.to_pylist()))


def check_zero(case):
    import shardloom as sl
    pa, native = case.pa, case.native
    client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
    case.addCleanup(client.close)
    zone = ZoneInfo("America/New_York")
    early, late = [int(datetime(2024, 11, 3, 1, 30, tzinfo=zone, fold=fold).timestamp()) for fold in (0, 1)]
    ticks = [early, late, early, late, None, early, None, late, early, late+1, early-1, early, late]
    ticks = [tick * 10**9 if tick is not None else None for tick in ticks]
    with tempfile.TemporaryDirectory(prefix="shardloom-calendar-zero-") as directory:
        path = Path(directory) / "fold.arrow"
        source, values, funds = _source(case, path, pa.timestamp("ns", "America/New_York"), ticks)
        wanted = _expected(case, source, ticks, values, funds, lambda tick: (tick, tick), False, "no_others")
        for offset, typed in [
            (sl.Window.calendar_days(0), native.FrameOffset.calendar_days(0)),
            (sl.Window.calendar_months(0), native.FrameOffset.calendar_months(0)),
            (sl.Window.duration(0, unit="ns"), native.FrameOffset.duration(0, unit="ns")),
            (sl.Window.duration(0, unit="us"), native.FrameOffset.duration_micros(0)),
        ]:
            for start, finish, native_bounds in [
                (offset, 0, (("following", typed), ("current_row", None))),
                (0, offset, (("current_row", None), ("preceding", typed))),
                (offset, offset, (("following", typed), ("preceding", typed))),
            ]:
                frame, relation, statement = _declarations(case, path, client, start, finish, typed,
                    native_bounds=native_bounds)
                with case.subTest(offset=str(offset), bounds=native_bounds):
                    for actual in case.all_surfaces(frame, relation, statement):
                        case.assertEqual(actual.schema, wanted.schema)
                        case.assertTrue(actual.equals(wanted), (actual.to_pylist(), wanted.to_pylist()))


def check_calendar(case):
    import shardloom as sl
    pa, native = case.pa, case.native
    client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
    case.addCleanup(client.close)
    zone = ZoneInfo("America/New_York")
    with tempfile.TemporaryDirectory(prefix="shardloom-calendar-five-") as directory:
        populations = [("spring", int(datetime(2024, 3, 9, tzinfo=timezone.utc).timestamp()), 3600, 97, 0),
                       ("fold", int(datetime(2024, 11, 2, tzinfo=timezone.utc).timestamp()), 1800, 161, 0),
                       ("month", int(datetime(2024, 1, 28, 12, tzinfo=timezone.utc).timestamp()), 86_400, 97, 1)]
        for label, first, step, count, months in populations:
            ticks = [first + i*step for i in range(count)]
            ticks.extend([ticks[5], ticks[9], ticks[13], None, None, None])
            random.Random(0xCA1E + count).shuffle(ticks)
            dtype = pa.timestamp("ns", "America/New_York")
            source_ticks = [tick * 10**9 if tick is not None else None for tick in ticks]
            path = Path(directory) / f"{label}.arrow"
            source, values, funds = _source(case, path, dtype, source_ticks)
            for ambiguity in ("earlier", "later"):
                for descending in (False, True):
                    for exclusion in ("no_others", "current_row", "group", "ties"):
                        if months:
                            start = sl.Window.calendar_months(-1, month_end="clamp", ambiguity=ambiguity)
                            finish = sl.Window.calendar_months(1, month_end="clamp", ambiguity=ambiguity)
                            offset = native.FrameOffset.calendar_months(1, month_end="clamp", ambiguity=ambiguity)
                        else:
                            start, finish = (sl.Window.calendar_days(sign, ambiguity=ambiguity) for sign in (-1, 1))
                            offset = native.FrameOffset.calendar_days(1, ambiguity=ambiguity)
                        bounds = lambda tick: tuple(_calendar_shift(tick, zone, months=sign*months, days=sign*(1-months), ambiguity=ambiguity) for sign in (-1, 1))
                        wanted = _expected(case, source, ticks, values, funds, bounds, descending, exclusion)
                        frame, relation, statement = _declarations(case, path, client, start, finish, offset,
                            descending=descending, exclusion=exclusion)
                        with case.subTest(population=label, ambiguity=ambiguity, descending=descending, exclusion=exclusion):
                            for actual in case.all_surfaces(frame, relation, statement):
                                case.assertEqual(actual.schema, wanted.schema)
                                case.assertTrue(actual.equals(wanted), (actual.to_pylist(), wanted.to_pylist()))
            retained = case.session.sql(statement).collect()
            outputs = []
            for fmt, reader in (("vortex", sl.read_vortex), ("parquet", sl.read_parquet), ("arrow-ipc", sl.read_arrow_ipc)):
                output = Path(directory) / f"output-{label}.{fmt}"
                written = frame.write(output, output_format=fmt, check=True)
                case.assertFalse(written.envelope.fallback.attempted)
                case.assertEqual(written.envelope.field("external_engine_invoked"), "false")
                outputs.append((output, reader))
            path.unlink()
            for output, reader in outputs:
                actual = reader(output, client=client).collect(check=True).to_arrow()
                case.assertEqual(actual.schema, wanted.schema)
                case.assertTrue(actual.equals(wanted))
            producer = native.Session(native.ExecutionResources(128 << 20, 2))
            reused = producer.sql("SELECT * FROM 'memory://calendar'", sources=[("memory://calendar", retained.as_source())]).collect()
            producer.close()
            case.assertTrue(case.table(reused).equals(wanted))
            case.assertTrue(case.table(retained).equals(wanted))


def check_refusals(case):
    import shardloom as sl
    from shardloom.errors import ShardLoomCommandError
    pa, native = case.pa, case.native
    client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
    case.addCleanup(client.close)
    cases = [
        (pa.int64(), [], sl.Window.calendar_days(1), native.FrameOffset.calendar_days(1), "RANGE offset"),
        (pa.duration("s"), [0], sl.Window.calendar_days(1), native.FrameOffset.calendar_days(1), "temporal operation"),
        (pa.time32("s"), [0], sl.Window.calendar_months(1), native.FrameOffset.calendar_months(1), "temporal operation"),
        (pa.date32(), [0], sl.Window.duration(1), native.FrameOffset.duration(1), "whole elapsed days"),
        (pa.timestamp("s", "America/New_York"), [1_730_701_800], sl.Window.calendar_days(1), native.FrameOffset.calendar_days(1), "ambiguous|nonexistent"),
        (pa.timestamp("s", "America/New_York"), [1_710_138_600], sl.Window.calendar_days(1), native.FrameOffset.calendar_days(1), "ambiguous|nonexistent"),
        (pa.timestamp("s"), [int(datetime(2024, 1, 31, tzinfo=timezone.utc).timestamp())],
         sl.Window.calendar_months(1), native.FrameOffset.calendar_months(1), "month|day"),
        (pa.timestamp("s"), [2**63-1], sl.Window.calendar_days(1), native.FrameOffset.calendar_days(1), "exceeds signed 64-bit ticks"),
    ]
    with tempfile.TemporaryDirectory(prefix="shardloom-calendar-refusals-") as directory:
        for index, (dtype, ticks, offset, typed, message) in enumerate(cases):
            path = Path(directory) / f"invalid-{index}.arrow"
            _source(case, path, dtype, ticks)
            # Use the same magnitude on each side so rejected calendar
            # destinations are actually evaluated for a nonempty frame.
            from dataclasses import replace
            frame, relation, statement = _declarations(case, path, client,
                replace(offset, signed_value=-1), offset, typed)
            arguments = ["sql", "--sql", statement, "--request", "collect", "--bounded", "true",
                         "--memory-bytes", str(128 << 20), "--max-parallelism", "2"]
            for surface, error, run in [
                ("dataframe", ShardLoomCommandError, lambda: frame.collect(check=True)),
                ("typed", RuntimeError, lambda: case.session.plan(relation).collect()),
                ("python_sql", RuntimeError, lambda: case.session.sql(statement).collect()),
                ("workflow", RuntimeError, lambda: case.session.workflow(arguments).collect()),
            ]:
                with case.subTest(case=index, surface=surface), case.assertRaisesRegex(error, message) as raised:
                    run()
                if isinstance(raised.exception, ShardLoomCommandError):
                    case.assertFalse(raised.exception.envelope.fallback.attempted)
            with patch("subprocess.Popen", case.real_popen):
                cli = sl.ShardLoomClient(binary=os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY"),
                    transport="subprocess", use_persistent_worker=False, memory_bytes=128 << 20,
                    max_parallelism=2, timeout=30)
                try:
                    with case.assertRaisesRegex(ShardLoomCommandError, message) as raised:
                        sl.sql(statement, client=cli).collect(check=True)
                    case.assertFalse(raised.exception.envelope.fallback.attempted)
                finally:
                    cli.close()
