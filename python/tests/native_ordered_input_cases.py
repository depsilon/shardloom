# SPDX-License-Identifier: Apache-2.0
"""Frozen, independent complete workflows for the native ordered-input experiment.

This module neither imports nor executes a query engine. PyArrow only writes and
reads fixture files; Python lists, stable sorting and bisect define expectations.
Physical source position, rather than the logical ``id``, resolves ordering ties.
"""

from bisect import bisect_right
from collections import defaultdict
import random


SIZES = (0, 33, 4097, 65537)
TEMPLATES = (
    "sort_ascending", "sort_shuffled", "sort_reversed", "sort_nullable_mixed",
    "window_ordered", "window_shuffled", "asof_ordered", "asof_shuffled",
    "collections_ordered", "collections_shuffled",
    "selectors_ordered", "selectors_shuffled",
)
SEED = 202610310731
ROW_GROUP_ROWS = 2048
SOURCE_FIELDS = ("id", "grp", "ord", "minor", "value", "text", "payload")


def null_last(value):
    return value is None, value if value is not None else 0


def null_first(value):
    return value is not None, value if value is not None else 0


def order_key(row, *, mixed=False):
    key = null_last(row["ord"])
    if mixed:
        # NULLS FIRST is independent of DESC; only non-NULL values reverse.
        key += (row["minor"] is not None, -row["minor"] if row["minor"] is not None else 0)
    return key


def records(template, count):
    if template not in TEMPLATES or count < 0:
        raise ValueError("unknown template or invalid row count")
    rows = [dict(id=i, grp=i % 7,
                 ord=None if i % 29 == 0 else i // 21 - 17,
                 minor=None if i % 23 == 0 else (i * 11) % 19 - 9,
                 value=None if i % 11 == 0 else (1 << 54) + i * 13 - 41,
                 text=None if i % 13 == 0 else ("" if i % 17 == 0 else f"t{i % 31}東京"),
                 payload=f"row-{i}-λ") for i in range(count)]
    if template.startswith("sort_"):
        rows.sort(key=lambda row: order_key(row, mixed=template == "sort_nullable_mixed"))
        if template == "sort_reversed":
            rows.reverse()
    elif template.startswith("asof_"):
        # The native as-of build comparator orders NULL before non-NULL keys.
        rows.sort(key=lambda row: (row["grp"], null_first(row["ord"])))
    else:
        rows.sort(key=lambda row: (row["grp"], order_key(row)))
    if template.endswith("shuffled"):
        random.Random(SEED + count).shuffle(rows)
    sources = {"input": rows}
    if template.startswith("asof_"):
        # Preserve a different, deterministic probe order and include probes
        # between right keys and before the first key. Build ties span batches.
        sources = {"right": rows, "left": [
            dict(row, ord=None if row["id"] % 19 == 0 else row["id"] // 21 - 18)
            for row in reversed(sorted(rows, key=lambda row: row["id"]))
        ]}
    return sources


def source_schema():
    import pyarrow as pa
    return pa.schema([
        pa.field("id", pa.uint64(), nullable=False),
        pa.field("grp", pa.int16(), nullable=False),
        pa.field("ord", pa.int64()), pa.field("minor", pa.int32()),
        pa.field("value", pa.int64()), pa.field("text", pa.string()),
        pa.field("payload", pa.string(), nullable=False),
    ])


def output_schema(template):
    import pyarrow as pa
    source = source_schema()
    if template.startswith("sort_"):
        return pa.schema([source.field("id"), pa.field("ordered_key", pa.int64()),
                          *(source.field(name) for name in ("minor", "value", "payload"))])
    if template.startswith("window_"):
        # Native analytic ROW_NUMBER has an exact nonnullable i64 result.
        return pa.schema([*(source.field(name) for name in ("id", "grp", "ord", "value")),
                          pa.field("position", pa.int64(), nullable=False)])
    if template.startswith("asof_"):
        return pa.schema([pa.field("left_id", pa.uint64(), nullable=False),
                          pa.field("right_id", pa.uint64()), pa.field("selected", pa.int64())])
    if template.startswith("collections_"):
        return pa.schema([source.field("grp"),
                          pa.field("items", pa.list_(pa.field("item", pa.int64(), nullable=True))),
                          pa.field("joined", pa.string())])
    if template.startswith("selectors_"):
        return pa.schema([source.field("grp"),
                          *(pa.field(name, pa.int64()) for name in ("initial", "ending", "least", "greatest"))])
    raise ValueError("unknown template")


def statement(template, paths):
    def source(name):
        return "'" + str(paths[name]).replace("'", "''") + "'"

    if template.startswith("asof_"):
        return ("SELECT f.id AS left_id,d.id AS right_id,d.value AS selected "
                f"FROM {source('left')} AS f ASOF LEFT JOIN {source('right')} AS d "
                "MATCH_CONDITION(f.ord>=d.ord) ON f.grp=d.grp")
    path = source("input")
    if template.startswith("sort_"):
        suffix = ",minor DESC NULLS FIRST" if template == "sort_nullable_mixed" else ""
        return (f"SELECT id,ord AS ordered_key,minor,value,payload FROM {path} "
                f"ORDER BY ordered_key ASC NULLS LAST{suffix}")
    if template.startswith("window_"):
        return ("SELECT id,grp,ord,value,ROW_NUMBER() OVER "
                f"(PARTITION BY grp ORDER BY ord ASC NULLS LAST) AS position FROM {path}")
    if template.startswith("collections_"):
        return ("SELECT grp,ARRAY_AGG(value ORDER BY ord ASC NULLS LAST) AS items,"
                "STRING_AGG(text, ' / ' ORDER BY ord ASC NULLS LAST) AS joined "
                f"FROM {path} GROUP BY grp ORDER BY grp")
    if template.startswith("selectors_"):
        return ("SELECT grp,FIRST(value ORDER BY ord ASC NULLS LAST) AS initial,"
                "LAST(value ORDER BY ord ASC NULLS LAST) AS ending,"
                "ARG_MIN(value,minor ORDER BY ord ASC NULLS LAST) AS least,"
                "ARG_MAX(value,minor ORDER BY ord ASC NULLS LAST) AS greatest "
                f"FROM {path} GROUP BY grp ORDER BY grp")
    raise ValueError("unknown template")


def reference_rows(template, sources):
    if template.startswith("sort_"):
        return [dict(id=row["id"], ordered_key=row["ord"], minor=row["minor"],
                     value=row["value"], payload=row["payload"])
                for row in sorted(sources["input"], key=lambda row: order_key(
                    row, mixed=template == "sort_nullable_mixed"))]
    if template.startswith("asof_"):
        by_group = defaultdict(list)
        for position, row in enumerate(sources["right"]):
            if row["ord"] is not None:
                by_group[row["grp"]].append((row["ord"], position, row))
        ordered = {group: sorted(rows, key=lambda item: item[:2]) for group, rows in by_group.items()}
        keys = {group: [item[0] for item in rows] for group, rows in ordered.items()}
        result = []
        for row in sources["left"]:
            at = -1 if row["ord"] is None else bisect_right(keys.get(row["grp"], []), row["ord"]) - 1
            selected = {} if at < 0 else ordered[row["grp"]][at][2]
            result.append(dict(left_id=row["id"], right_id=selected.get("id"), selected=selected.get("value")))
        return result
    groups = defaultdict(list)
    for row in sources["input"]:
        groups[row["grp"]].append(row)
    ordered = {group: sorted(rows, key=order_key) for group, rows in groups.items()}
    if template.startswith("window_"):
        positions = {row["id"]: position for rows in ordered.values()
                     for position, row in enumerate(rows, 1)}
        return [dict(id=row["id"], grp=row["grp"], ord=row["ord"], value=row["value"],
                     position=positions[row["id"]]) for row in sources["input"]]
    result = []
    for group, rows in sorted(ordered.items()):
        if template.startswith("collections_"):
            texts = [row["text"] for row in rows if row["text"] is not None]
            result.append(dict(grp=group, items=[row["value"] for row in rows],
                               joined=" / ".join(texts) if texts else None))
        elif template.startswith("selectors_"):
            eligible = [row for row in rows if row["value"] is not None and row["minor"] is not None]
            result.append(dict(grp=group, initial=rows[0]["value"], ending=rows[-1]["value"],
                               least=min(eligible, key=lambda row: row["minor"])["value"] if eligible else None,
                               greatest=max(eligible, key=lambda row: row["minor"])["value"] if eligible else None))
        else:
            raise ValueError("unknown template")
    return result


def reference(template, sources):
    from query_surface.semantics import TypedResult, arrow_schema
    return TypedResult(arrow_schema(output_schema(template)), reference_rows(template, sources))


def write_parquet(directory, sources):
    import pyarrow as pa
    import pyarrow.parquet as pq
    paths = {}
    for name, rows in sorted(sources.items()):
        path = directory / f"{name}.parquet"
        if path.exists():
            raise ValueError("fixture destination already exists")
        table = pa.Table.from_pylist(rows, schema=source_schema())
        pq.write_table(table, path, row_group_size=ROW_GROUP_ROWS, compression="zstd", use_dictionary=True)
        reopened = pq.read_table(path)
        if reopened.schema != table.schema or not reopened.equals(table):
            raise AssertionError("complete independent source readback differs")
        paths[name] = path
    return paths
