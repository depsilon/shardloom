# SPDX-License-Identifier: Apache-2.0
"""Typed unary public workflows with independent complete-value expectations."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
import json


def run(context, output, guard, exercise, remember, original, fields, schema, native, raw):
    """Build direct and composed typed unary cases for existing public consumers."""
    from shardloom.query import UnsupportedWorkflowOperationReport

    cases = []

    def add(name, frame, expected, columns, *, typed_orc=True, json_cells=("payload", "amount"), empty=True):
        if isinstance(frame, UnsupportedWorkflowOperationReport):
            raise RuntimeError(f"NEEDS_REASONING: {name} declaration is unsupported")
        options = {"typed_orc": typed_orc, "json_cells": json_cells}
        cases.append((name, frame, expected, columns, options))
        if empty:
            cases.append((name + "-empty", frame.limit(0), [], columns, options))

    for source_name, source, composed in (
        ("native", context.read_vortex(native, schema=schema), True),
        ("declared-arrow", context.read_arrow_ipc(raw), True),
        ("direct-native", context.read_vortex(native, schema=schema), False),
    ):
        base = (source.limit(4) if composed else source).select(*fields)
        duplicate_source = (context.read_arrow_ipc(output / "typed-unary-duplicates.data")
                            if source_name == "declared-arrow" else context.read_vortex(
                                output / "typed-unary-duplicates.vortex", schema=schema))
        duplicated_input = (duplicate_source.limit(6) if composed else duplicate_source).select(*fields)
        duplicated_rows = list(original) + list(original[:2])
        key_columns = list(fields[1:])

        projected_distinct = [
            {name: row[name] for name in key_columns}
            for row in original
        ]
        add(
            f"typed-unary-{source_name}-distinct-projection",
            duplicated_input.select(*key_columns).distinct(),
            projected_distinct,
            key_columns,
        )

        for key in key_columns:
            add(
                f"typed-unary-{source_name}-dedup-{key}-last",
                duplicated_input.drop_duplicates(subset=[key], keep="last"),
                [original[index] for index in (2, 3, 0, 1)],
                list(fields),
            )

        add(
            f"typed-unary-{source_name}-dedup-composite-first",
            duplicated_input.drop_duplicates(subset=key_columns, keep="first"),
            list(original),
            list(fields),
        )
        add(
            f"typed-unary-{source_name}-dedup-composite-remove-all",
            duplicated_input.drop_duplicates(subset=key_columns, keep=False),
            list(original[2:]),
            list(fields),
        )

        for keep, expected_mask in (
            ("first", [False, False, False, False, True, True]),
            ("last", [True, True, False, False, False, False]),
            (False, [True, True, False, False, True, True]),
        ):
            add(
                f"typed-unary-{source_name}-duplicate-mask-composite-{keep}",
                duplicated_input.duplicated(subset=key_columns, keep=keep),
                [{"duplicated": value} for value in expected_mask],
                ["duplicated"],
                typed_orc=False,
                json_cells=(),
            )

        add(
            f"typed-unary-{source_name}-tail",
            duplicated_input.tail(2),
            list(original[:2]),
            list(fields),
        )
        add(
            f"typed-unary-{source_name}-sample",
            duplicated_input.sample(n=len(duplicated_rows), random_state=7),
            duplicated_rows,
            list(fields),
        )

        filled_rows = [dict(row) for row in original]
        for key in key_columns:
            filled_rows[2][key] = original[1][key]
        add(
            f"typed-unary-{source_name}-forward-fill",
            base.fillna(method="ffill", limit=1),
            filled_rows,
            list(fields),
        )

        replacements = {
            "payload": (b"\x00\xff\x10", b"\x0a\xfe"),
            "amount": (Decimal("1.234567"), Decimal("2.500000")),
            "day": (date(1969, 12, 31), date(1970, 1, 2)),
            "instant": (
                datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc),
                datetime(1970, 1, 1, 0, 0, 0, 7, tzinfo=timezone.utc),
            ),
        }
        replacement_expected = [dict(row) for row in original]
        replacement_expected[0].update(
            payload="0afe",
            amount="decimal128(38,6):2500000",
            day=1,
            instant=7,
        )
        add(
            f"typed-unary-{source_name}-replace-typed-scalars",
            base.replace(
                {key: replacements[key][0] for key in key_columns},
                {key: replacements[key][1] for key in key_columns},
            ),
            replacement_expected,
            list(fields),
        )

        for key in key_columns:
            json_cells = ("value",) if key in ("payload", "amount") else ()
            add(
                f"typed-unary-{source_name}-melt-{key}",
                base.select("id", key).melt(
                    id_vars=["id"], value_vars=[key], var_name="variable", value_name="value"
                ),
                [{"id": row["id"], "variable": key, "value": row[key]} for row in original],
                ["id", "variable", "value"],
                typed_orc=key != "payload",
                json_cells=json_cells,
            )

        for key in key_columns:
            add(
                f"typed-unary-{source_name}-rolling-count-{key}",
                base.select(key).rolling(2, min_periods=1).count(key, alias="valid"),
                [{"valid": value} for value in (1, 2, 1, 1)],
                ["valid"],
                typed_orc=False,
                json_cells=(),
            )

        pivot_frame = base.select("id", "payload", "amount").pivot_table(
            index="id",
            columns="payload",
            values="amount",
            aggfunc="first",
            dropna=False,
            fill_value=Decimal("0.125000"),
        )
        pivot_columns = [
            "id",
            "pivot_binary",
            "pivot_binary_00ff10",
            "pivot_binary_c3a9",
            "pivot_value",
        ]
        pivot_rows = []
        value_column = {
            "": "pivot_binary",
            "00ff10": "pivot_binary_00ff10",
            "c3a9": "pivot_binary_c3a9",
            None: "pivot_value",
        }
        fill = "decimal128(38,6):125000"
        for row in original:
            values = {column: fill for column in pivot_columns[1:]}
            values[value_column[row["payload"]]] = row["amount"]
            pivot_rows.append({"id": row["id"], **values})
        add(
            f"typed-unary-{source_name}-dynamic-pivot",
            pivot_frame,
            pivot_rows,
            pivot_columns,
            json_cells=tuple(pivot_columns[1:]),
            empty=False,
        )

    # Freeze independent rows and schemas before the consumer starts producing results.
    oracle = output / "typed-unary-expected.json"
    oracle.write_text(
        json.dumps(
            {
                name: {"columns": columns, "rows": expected}
                for name, _, expected, columns, _ in cases
            },
            indent=2,
        )
        + "\n"
    )
    remember(oracle)

    for name, frame, expected, columns, options in cases:
        guard()
        exercise(name, frame, expected, columns, **options)
