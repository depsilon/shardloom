# SPDX-License-Identifier: Apache-2.0
"""Exact public workflows for typed relational keys and value semantics."""

from __future__ import annotations

import json
from pathlib import Path


def run(context, output: Path, guard, exercise, remember, original, fields, schema, native, raw):
    """Run the predetermined typed-key matrix on native and declared Arrow sources.

    ``exercise`` owns the common SQL/DataFrame collect, writer, and reopen checks;
    this module only declares plans and independently specified expected rows.
    """
    import shardloom as sl

    output.mkdir(parents=True, exist_ok=True)

    order = {
        "payload": [3, 0, 1, 2],
        "amount": [0, 3, 1, 2],
        "day": [1, 3, 0, 2],
        "instant": [1, 3, 0, 2],
    }

    def writer_options(columns, *, key=None, typed_aliases=()):
        json_cells = [name for name in columns if name in ("payload", "amount")]
        if key in ("payload", "amount"):
            json_cells.extend(typed_aliases)
        # A text logical value uses the existing JSON cell convention in CSV;
        # temporal values remain signed numeric units.
        json_cells = tuple(dict.fromkeys(json_cells))
        typed_orc = key in ("amount", "day", "instant") or any(
            name in ("amount", "day", "instant") for name in columns
        )
        return {"json_cells": json_cells, "typed_orc": typed_orc}

    cases = []

    def add(family, frame, expected, columns, *, key=None, typed_aliases=()):
        cases.append((family, frame, expected, columns,
                      writer_options(columns, key=key, typed_aliases=typed_aliases)))

    for source_name, source in (
        ("native", context.read_vortex(native, schema=schema)),
        ("declared-arrow", context.read_arrow_ipc(raw)),
    ):
        prefix = source.limit(4)
        for key in ("payload", "amount", "day", "instant"):
            rank_by_row = {row_index: rank for rank, row_index in enumerate(order[key], 1)}

            sorted_frame = prefix.sort(key, descending=True, nulls="last")
            add(
                f"typed-key-{source_name}-{key}-sort-desc-null-last",
                sorted_frame,
                [original[index] for index in order[key]],
                fields,
                key=key,
            )
            add(
                f"typed-key-{source_name}-{key}-sort-desc-null-last-empty",
                sorted_frame.limit(0),
                [],
                fields,
                key=key,
            )

            joined = prefix.join(prefix, on=key, how="inner").select(
                "f.id AS id", "d.id AS matched", f"f.{key} AS {key}"
            )
            join_columns = ["id", "matched", key]
            add(
                f"typed-key-{source_name}-{key}-inner-self-join",
                joined,
                [
                    {"id": original[index]["id"],
                     "matched": original[index]["id"],
                     key: original[index][key]}
                    for index in (0, 1, 3)
                ],
                join_columns,
                key=key,
            )

            grouped = prefix.union_all(prefix).group_by(key).agg(
                rows="count(*)",
                present=f"count({key})",
                unique=f"count_distinct({key})",
                low=f"min({key})",
                high=f"max({key})",
            )
            group_columns = [key, "rows", "present", "unique", "low", "high"]
            group_rows = []
            seen = set()
            for row in original:
                value = row[key]
                marker = json.dumps(value, sort_keys=True)
                if marker in seen:
                    continue
                seen.add(marker)
                is_null = value is None
                group_rows.append({
                    key: value,
                    "rows": 2,
                    "present": 0 if is_null else 2,
                    "unique": 0 if is_null else 1,
                    "low": None if is_null else value,
                    "high": None if is_null else value,
                })
            add(
                f"typed-key-{source_name}-{key}-union-all-grouped-aggregates",
                grouped,
                group_rows,
                group_columns,
                key=key,
                typed_aliases=("low", "high"),
            )

            ranked = prefix.window(
                f"RANK() OVER (ORDER BY {key} DESC NULLS LAST) AS typed_rank"
            )
            add(
                f"typed-key-{source_name}-{key}-rank-desc-null-last",
                ranked,
                [dict(row, typed_rank=rank_by_row[index])
                 for index, row in enumerate(original)],
                [*fields, "typed_rank"],
                key=key,
            )

            duplicated = prefix.union_all(prefix).window(
                f"ROW_NUMBER() OVER (PARTITION BY {key} ORDER BY id) AS rn"
            )
            add(
                f"typed-key-{source_name}-{key}-duplicate-row-number-partition",
                duplicated,
                ([dict(row, rn=1) for row in original]
                 + [dict(row, rn=2) for row in original]),
                [*fields, "rn"],
                key=key,
            )

            membership = prefix.filter(
                sl.col(key).isin_source(prefix.limit(2), key)
            )
            add(
                f"typed-key-{source_name}-{key}-source-membership-first-two",
                membership,
                list(original[:2]),
                fields,
                key=key,
            )

            expressed = prefix.select(
                "id",
                key,
                f"{key} IS NULL AS missing",
                f"NULLIF({key},{key}) AS erased",
                f"CASE WHEN id=3 THEN {key} ELSE {key} END AS chosen",
                f"COALESCE({key},{key}) AS restored",
            )
            expression_columns = ["id", key, "missing", "erased", "chosen", "restored"]
            expression_rows = [
                {
                    "id": row["id"],
                    key: row[key],
                    "missing": row[key] is None,
                    "erased": None,
                    "chosen": row[key],
                    "restored": row[key],
                }
                for row in original
            ]
            add(
                f"typed-key-{source_name}-{key}-same-type-null-case-coalesce",
                expressed,
                expression_rows,
                expression_columns,
                key=key,
                typed_aliases=("erased", "chosen", "restored"),
            )

        add(
            f"typed-key-{source_name}-set-union-distinct",
            prefix.union(prefix),
            list(original),
            fields,
        )
        add(
            f"typed-key-{source_name}-set-intersect",
            prefix.intersect(prefix),
            list(original),
            fields,
        )
        add(
            f"typed-key-{source_name}-set-except",
            prefix.except_rows(prefix),
            [],
            fields,
        )

    # Freeze the full independent oracle before executing any case.
    oracle = output / "typed-key-expected.json"
    oracle.write_text(json.dumps(
        {
            family: {"columns": columns, "rows": expected}
            for family, _, expected, columns, _ in cases
        },
        indent=2,
    ) + "\n")
    remember(oracle)

    for family, frame, expected, columns, options in cases:
        guard()
        exercise(family, frame, expected, columns, **options)

