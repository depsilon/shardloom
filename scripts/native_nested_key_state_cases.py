# SPDX-License-Identifier: Apache-2.0
"""Complete public nested keys/state with separately specified logical row order."""

from __future__ import annotations

import json


def run(context, output, guard, exercise, remember, *, prefix, original, fields,
        schema, native, raw, duplicate_native, duplicate_raw, orders):
    """Declare every expected value before invoking either public front end.

    ``orders`` contains source-row indices in ascending order with NULL first;
    it is supplied literally by the fixture's independent expected-value owner.
    """
    import shardloom as sl
    from shardloom.query import UnsupportedWorkflowOperationReport

    cases = []
    keys = list(orders)

    def add(name, frame, rows, columns, *, nested=True, empty=False):
        if isinstance(frame, UnsupportedWorkflowOperationReport):
            raise ValueError(f"{name}: nested workflow declaration was rejected")
        cases.append((name, frame, rows, columns, nested))
        if empty:
            cases.append((name + "-empty", frame.limit(0), [], columns, nested))

    for source_name, source in (
        ("native", context.read_vortex(native, schema=schema)),
        ("declared-arrow", context.read_arrow_ipc(raw)),
    ):
        base = source.limit(len(original))
        for key in keys:
            name = f"{prefix}-{source_name}-{key}"
            null_rows = [index for index in orders[key] if original[index][key] is None]
            valid_rows = [index for index in orders[key] if original[index][key] is not None]
            for descending, nulls, sequence in (
                (False, "first", null_rows + valid_rows),
                (False, "last", valid_rows + null_rows),
                (True, "first", null_rows + list(reversed(valid_rows))),
                (True, "last", list(reversed(valid_rows)) + null_rows),
            ):
                add(f"{name}-sort-{descending}-null-{nulls}",
                    base.sort(key, descending=descending, nulls=nulls),
                    [original[index] for index in sequence], fields,
                    empty=not descending and nulls == "first")

            joined = base.join(base, on=key, how="inner").select(
                "f.id AS id", "d.id AS matched", f"f.{key} AS value")
            add(f"{name}-inner-join", joined,
                [{"id": row["id"], "matched": row["id"], "value": row[key]}
                 for row in original if row[key] is not None], ["id", "matched", "value"])
            outer = base.join(base, on=key, how="full").select(
                "f.id AS id", "d.id AS matched", f"f.{key} AS value")
            add(f"{name}-full-join", outer,
                [{"id": row["id"], "matched": row["id"] if row[key] is not None else None,
                  "value": row[key]} for row in original]
                + [{"id": None, "matched": original[index]["id"], "value": None}
                   for index in null_rows], ["id", "matched", "value"])

            group_rows = [{key: row[key], "rows": 2,
                           "present": 0 if row[key] is None else 2,
                           "unique": 0 if row[key] is None else 1,
                           "low": row[key], "high": row[key]} for row in original]
            add(f"{name}-groups", base.union_all(base).group_by(key).agg(
                rows="count(*)", present=f"count({key})", unique=f"count(DISTINCT {key})",
                low=f"min({key})", high=f"max({key})"), group_rows,
                [key, "rows", "present", "unique", "low", "high"], empty=True)
            add(f"{name}-global-extrema", base.agg(
                present=f"count({key})", unique=f"count(DISTINCT {key})",
                low=f"min({key})", high=f"max({key})"),
                [{"present": len(valid_rows), "unique": len(valid_rows),
                  "low": original[valid_rows[0]][key], "high": original[valid_rows[-1]][key]}],
                ["present", "unique", "low", "high"])
            add(f"{name}-global-empty", base.limit(0).agg(
                present=f"count({key})", unique=f"count(DISTINCT {key})",
                low=f"min({key})", high=f"max({key})"),
                [{"present": 0, "unique": 0, "low": None, "high": None}],
                ["present", "unique", "low", "high"])

            ranks = {row: rank for rank, row in enumerate(orders[key], 1)}
            add(f"{name}-rank", base.window(f"RANK() OVER (ORDER BY {key} NULLS FIRST) AS rank"),
                [dict(row, rank=ranks[index]) for index, row in enumerate(original)],
                [*fields, "rank"])
            add(f"{name}-partition", base.union_all(base).window(
                f"ROW_NUMBER() OVER (PARTITION BY {key} ORDER BY id) AS rn"),
                [dict(row, rn=1) for row in original] + [dict(row, rn=2) for row in original],
                [*fields, "rn"])
            add(f"{name}-membership", base.filter(sl.col(key).isin_source(base.limit(2), key)),
                [row for row in original[:2] if row[key] is not None], fields)
            add(f"{name}-expressions", base.select(
                "id", f"{key} IS NULL AS missing", f"{key}={key} AS same",
                f"{key}<{key} AS less", f"NULLIF({key},{key}) AS erased",
                f"CASE WHEN id<=2 THEN {key} ELSE NULL END AS chosen",
                f"COALESCE(NULL,{key}) AS restored"),
                [{"id": row["id"], "missing": row[key] is None,
                  "same": None if row[key] is None else True,
                  "less": None if row[key] is None else False,
                  "erased": None, "chosen": row[key] if row["id"] <= 2 else None,
                  "restored": row[key]} for row in original],
                ["id", "missing", "same", "less", "erased", "chosen", "restored"], empty=True)

        for label, frame, expected in (
            ("union", base.union(base), original),
            ("intersect", base.intersect(base), original),
            ("except", base.except_rows(base), []),
        ):
            add(f"{prefix}-{source_name}-{label}", frame, list(expected), fields)

    for source_name, source, duplicates, composed in (
        ("native", context.read_vortex(native, schema=schema),
         context.read_vortex(duplicate_native, schema=schema), True),
        ("declared-arrow", context.read_arrow_ipc(raw), context.read_arrow_ipc(duplicate_raw), True),
        ("direct-native", context.read_vortex(native, schema=schema),
         context.read_vortex(duplicate_native, schema=schema), False),
    ):
        duplicate_source = duplicates
        base = (source.limit(len(original)) if composed else source).select(*fields)
        duplicates = (duplicates.limit(len(original) + 2) if composed else duplicates).select(*fields)
        name = f"{prefix}-{source_name}-state"
        add(name + "-distinct", duplicates.select(*keys).distinct(),
            [{key: row[key] for key in keys} for row in original], keys, empty=True)
        for key in keys:
            add(name + f"-dedup-{key}-last", duplicates.drop_duplicates(subset=[key], keep="last"),
                list(original[2:]) + list(original[:2]), fields, empty=True)
            add(name + f"-group-counts-{key}", duplicate_source.group_by(key).count(alias="rows"),
                [{key: row[key], "rows": 2 if index < 2 else 1}
                 for index, row in enumerate(original)], [key, "rows"], empty=True)
            add(name + f"-count-distinct-{key}", duplicate_source.agg(unique_count=f"count(DISTINCT {key})"),
                [{"unique_count": sum(row[key] is not None for row in original)}],
                ["unique_count"], nested=False, empty=True)
        for keep, indices, mask in (
            ("first", [0, 1, 2, 3], [False, False, False, False, True, True]),
            ("last", [2, 3, 0, 1], [True, True, False, False, False, False]),
            (False, [2, 3], [True, True, False, False, True, True]),
        ):
            add(name + f"-dedup-composite-{keep}", duplicates.drop_duplicates(subset=keys, keep=keep),
                [original[index] for index in indices], fields)
            add(name + f"-mask-{keep}", duplicates.duplicated(subset=keys, keep=keep),
                [{"duplicated": value} for value in mask], ["duplicated"], nested=False)
        add(name + "-tail", duplicates.tail(2), list(original[:2]), fields, empty=True)
        add(name + "-sample", duplicates.sample(n=len(original) + 2, random_state=7),
            list(original) + list(original[:2]), fields, empty=True)
        filled = [dict(row) for row in original]
        for key in keys:
            previous = None
            for row in filled:
                if row[key] is None:
                    row[key] = previous
                else:
                    previous = row[key]
        add(name + "-fill", base.fillna(method="ffill", limit=1), filled, fields, empty=True)
        for key in keys:
            add(name + f"-melt-{key}", base.select("id", key).melt(
                id_vars=["id"], value_vars=[key], var_name="variable", value_name="value"),
                [{"id": row["id"], "variable": key, "value": row[key]} for row in original],
                ["id", "variable", "value"], empty=True)
            counts = [sum(row[key] is not None for row in original[max(0, index - 1):index + 1])
                      for index in range(len(original))]
            add(name + f"-rolling-count-{key}", base.select(key).rolling(2, min_periods=1).count(key, alias="valid"),
                [{"valid": value} for value in counts], ["valid"], nested=False, empty=True)

    oracle = output / f"{prefix}-expected.json"
    oracle.write_text(json.dumps({
        name: {"columns": columns, "rows": expected, "nested": nested}
        for name, _, expected, columns, nested in cases
    }, ensure_ascii=False, indent=2) + "\n")
    remember(oracle)
    for name, frame, expected, columns, nested in cases:
        guard()
        exercise(name, frame, expected, columns, nested=nested)
