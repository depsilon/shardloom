# SPDX-License-Identifier: Apache-2.0
"""Typed expression declarations and independent complete workflow oracles."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json


def run(context, output, guard, exercise, exercise_workflow, remember, original,
        schema, native, raw, large_native, count):
    """Use the existing public workflow consumer for every expression and writer."""
    import shardloom as sl

    cases = []
    direct = []

    def decimal(precision, scale, value):
        return None if value is None else f"decimal128({precision},{scale}):{value}"

    def rows(values):
        assert all(len(column) == len(original) for column in values.values())
        return [dict(id=row["id"], **{name: values[name][index] for name in values})
                for index, row in enumerate(original)]

    def add(name, base, expressions, expected, *, typed_orc=True, json_cells=()):
        columns = ["id", *expressions]
        frame = base.with_columns(expressions).select(*columns)
        options = {"typed_orc": typed_orc, "json_cells": json_cells}
        cases.append((name, frame, expected, columns, options))
        cases.append((name + "-empty", frame.limit(0), [], columns, options))
        return columns, options

    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    days = [None if row["day"] is None else epoch.date() + timedelta(days=row["day"])
            for row in original]
    instants = [None if row["instant"] is None else epoch + timedelta(microseconds=row["instant"])
                for row in original]
    narrow_values = [1_234_567, None, None, 0]
    narrow = sl.col("amount").try_cast("decimal128(10,6)")
    good = sl.ColumnExpression("CAST('2.500000' AS decimal128(10,6))")
    bad = sl.ColumnExpression("CAST('bad' AS decimal128(10,6))")
    for source_name, source in (
        ("native", context.read_vortex(native, schema=schema)),
        ("declared-arrow", context.read_arrow_ipc(raw)),
    ):
        base = source.limit(4)
        expressions = {
            "narrowed": narrow,
            "added": narrow + Decimal("1.000000"),
            "subtracted": narrow - Decimal("1.000000"),
            "multiplied": narrow * Decimal("2.000000"),
            "divided": narrow / sl.col("id"),
            "negated": -narrow,
            "absolute": narrow.abs(),
            "floored": narrow.floor(),
            "ceiled": narrow.ceil(),
            "rounded": narrow.round(),
        }
        expected = rows({
            "narrowed": [decimal(10, 6, value) for value in narrow_values],
            "added": [decimal(11, 6, value) for value in [2_234_567, None, None, 1_000_000]],
            "subtracted": [decimal(11, 6, value) for value in [234_567, None, None, -1_000_000]],
            "multiplied": [decimal(17, 12, value) for value in [2_469_134_000_000, None, None, 0]],
            "divided": [decimal(38, 6, value) for value in narrow_values],
            "negated": [decimal(10, 6, value) for value in [-1_234_567, None, None, 0]],
            "absolute": [decimal(10, 6, value) for value in narrow_values],
            "floored": [decimal(5, 0, value) for value in [1, None, None, 0]],
            "ceiled": [decimal(5, 0, value) for value in [2, None, None, 0]],
            "rounded": [decimal(5, 0, value) for value in [1, None, None, 0]],
        })
        add(f"typed-expression-{source_name}-decimal", base, expressions, expected,
            json_cells=tuple(expressions))

        expressions = {
            "text": narrow.cast("utf8"),
            "binary": narrow.cast("binary"),
            "floating": narrow.cast("float64"),
            "integer": narrow.try_cast("int64"),
            "unsigned": narrow.try_cast("uint64"),
            "rescaled": narrow.try_cast("decimal128(6,2)"),
            "exact_rescaled": good.cast("decimal128(4,2)"),
        }
        expected = rows({
            "text": ["1.234567", None, None, "0.000000"],
            "binary": ["312e323334353637", None, None, "302e303030303030"],
            "floating": [1.234567, None, None, 0.0],
            "integer": [None, None, None, 0],
            "unsigned": [None, None, None, 0],
            "rescaled": [None, None, None, decimal(6, 2, 0)],
            "exact_rescaled": [decimal(4, 2, 250)] * 4,
        })
        add(f"typed-expression-{source_name}-casts", base, expressions, expected,
            json_cells=("binary", "rescaled", "exact_rescaled"))

        expressions = {
            "text": sl.col("payload").try_cast("utf8"),
            "bytes": sl.col("payload").byte_length(),
            "text_bytes": sl.col("payload").try_cast("utf8").byte_length(),
            "hex": sl.ColumnExpression("CAST('00ff10' AS binary)").cast("utf8").unhex(),
            "base64": sl.ColumnExpression("CAST('w6k=' AS binary)").cast("utf8").from_base64(),
        }
        expected = rows({"text": [None, "", None, "é"], "bytes": [3, 0, None, 2],
                         "text_bytes": [None, 0, None, 2], "hex": ["00ff10"] * 4,
                         "base64": ["c3a9"] * 4})
        add(f"typed-expression-{source_name}-binary", base, expressions, expected,
            typed_orc=False, json_cells=("hex", "base64"))

        expressions = {
            "at_midnight": sl.col("day").cast("timestamp_micros"),
            "calendar_day": sl.col("instant").cast("date32"),
            "day_added": sl.col("day").date_add_days(sl.col("id")),
            "day_subtracted": sl.col("day").date_sub_days(sl.col("id")),
            "instant_added": sl.col("instant").timestamp_add_seconds(sl.col("id")),
            "instant_subtracted": sl.col("instant").timestamp_sub_seconds(sl.col("id")),
            "day_text": sl.col("day").cast("utf8"),
            "instant_text": sl.col("instant").cast("utf8"),
        }
        expected = []
        for row, day, instant in zip(original, days, instants):
            expected.append({
                "id": row["id"],
                "at_midnight": None if day is None else row["day"] * 86_400_000_000,
                "calendar_day": None if instant is None else row["instant"] // 86_400_000_000,
                "day_added": None if day is None else row["day"] + row["id"],
                "day_subtracted": None if day is None else row["day"] - row["id"],
                "instant_added": None if instant is None else row["instant"] + row["id"] * 1_000_000,
                "instant_subtracted": None if instant is None else row["instant"] - row["id"] * 1_000_000,
                "day_text": None if day is None else day.isoformat(),
                "instant_text": None if instant is None else instant.isoformat(
                    timespec="microseconds" if instant.microsecond else "seconds").replace("+00:00", "Z"),
            })
        add(f"typed-expression-{source_name}-calendar", base, expressions, expected)

        expressions = {
            "year": sl.col("day").date_year(), "month": sl.col("day").date_month(),
            "day_of_month": sl.col("day").date_day(), "hour": sl.col("instant").timestamp_hour(),
            "minute": sl.col("instant").timestamp_minute(), "second": sl.col("instant").timestamp_second(),
            "date_delta": sl.col("day").date_diff_days(date(1970, 1, 1)),
            "seconds_delta": sl.col("instant").timestamp_diff_seconds(epoch),
        }
        expected = rows({
            "year": [None if value is None else value.year for value in days],
            "month": [None if value is None else value.month for value in days],
            "day_of_month": [None if value is None else value.day for value in days],
            "hour": [None if value is None else value.hour for value in instants],
            "minute": [None if value is None else value.minute for value in instants],
            "second": [None if value is None else value.second for value in instants],
            "date_delta": [row["day"] for row in original],
            "seconds_delta": [0, 1_700_000_000, None, 0],
        })
        add(f"typed-expression-{source_name}-calendar-fields", base, expressions, expected, typed_orc=False)

        expressions = {
            "nullable": sl.case_when(sl.col("id") < 3, narrow, None),
            "selected": sl.case_when(sl.col("id") > 0, narrow, bad),
            "coalesced": narrow.fill_null(good), "lazy": good.fill_null(bad),
            "null_if": narrow.null_if(None),
        }
        expected = rows({
            "nullable": [decimal(10, 6, value) for value in [1_234_567, None, None, None]],
            "selected": [decimal(10, 6, value) for value in narrow_values],
            "coalesced": [decimal(10, 6, value) for value in [1_234_567, 2_500_000, 2_500_000, 0]],
            "lazy": [decimal(10, 6, 2_500_000)] * 4,
            "null_if": [decimal(10, 6, value) for value in narrow_values],
        })
        add(f"typed-expression-{source_name}-selection", base, expressions, expected,
            json_cells=tuple(expressions))

        expressions = {"literal_decimal": Decimal("-12.3000"), "literal_binary": b"\x00\xff",
                       "literal_day": date(1969, 12, 31), "literal_instant": epoch - timedelta(microseconds=1)}
        expected = [dict(id=row["id"], literal_decimal=decimal(6, 4, -123000),
                         literal_binary="00ff", literal_day=-1, literal_instant=-1) for row in original]
        add(f"typed-expression-{source_name}-literals", base, expressions, expected,
            json_cells=("literal_decimal", "literal_binary"))
        add(f"typed-expression-{source_name}-source-literals", source, expressions, expected,
            json_cells=("literal_decimal", "literal_binary"))

    # Preserve ordinary source statements so the normal source dispatch participates.
    source_sql = "'" + str(native).replace("'", "''") + "'"
    for name, expression, values in [
        ("decimal", "ROUND(TRY_CAST(amount AS decimal128(10,6)))", [decimal(5, 0, value) for value in [1, None, None, 0]]),
        ("binary", "BYTE_LENGTH(payload)", [3, 0, None, 2]),
        ("calendar", "CAST(instant AS date32)", [-1, 19_675, None, 0]),
        ("lazy", "COALESCE(CAST('2.500000' AS decimal128(10,6)),CAST('bad' AS decimal128(10,6)))", [decimal(10, 6, 2_500_000)] * 4),
    ]:
        expected = rows({"changed": values})
        options = {"typed_orc": name != "binary", "json_cells": ("changed",) if name in ("decimal", "lazy") else ()}
        direct.append((f"typed-expression-direct-{name}", context.sql(
            f"SELECT id,{expression} AS changed FROM {source_sql}"), expected, ["id", "changed"], options))

    for source_name, source in (
        ("native", context.read_vortex(large_native, schema=schema)),
        ("declared-arrow", context.read_arrow_ipc(output / "typed-large.data")),
    ):
        expressions = {
            "payload": sl.col("id").cast("binary"), "amount": sl.col("id").cast("decimal128(8,2)"),
            "day": sl.ColumnExpression("DATE '1970-01-01'").date_add_days(sl.col("id")),
            "instant": sl.ColumnExpression("TIMESTAMP '1970-01-01T00:00:00Z'").timestamp_add_seconds(sl.col("id")),
        }
        expected = [dict(id=index, payload=str(index).encode().hex(), amount=decimal(8, 2, index * 100),
                         day=index, instant=index * 1_000_000) for index in range(count)]
        add(f"typed-expression-large-{source_name}", source.limit(count).select("id"), expressions, expected,
            json_cells=("payload", "amount"))

    # Freeze every expected row before invoking a producer; Python is only an oracle.
    oracle = output / "typed-expression-expected.json"
    oracle.write_text(json.dumps({name: {"columns": columns, "rows": expected}
                                 for name, _, expected, columns, _ in [*cases, *direct]}) + "\n")
    remember(oracle)
    for name, frame, expected, columns, options in cases:
        guard()
        exercise(name, frame, expected, columns, **options)
    for name, workflow, expected, columns, options in direct:
        guard()
        exercise_workflow(name, workflow, expected, columns, **options)
