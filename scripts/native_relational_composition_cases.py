# SPDX-License-Identifier: Apache-2.0
"""Independent complete-value expectations for ordered public relation composition."""

from __future__ import annotations


def cases(context, left, right, raw_right, typed_left, typed_right):
    import shardloom as sl

    frame, dimension = context.read_vortex(left), context.read_vortex(right)
    named = context.sql(
        "SELECT cargo_id,load_units FROM cargo ORDER BY load_units DESC LIMIT 2", input=left,
    )
    left_input = frame.sort("load_units").limit(3)
    right_input = dimension.filter(sl.col("label") != "two-b")
    joined_columns = ("f.cargo_id AS left_key", "f.load_units AS load_units",
                      "d.cargo_id AS right_key", "d.label AS label")

    def joined(rows):
        return [dict(zip(("left_key", "load_units", "right_key", "label"), row)) for row in rows]

    matched = [(2, 10, 2, "two-a"), (2, 11, 2, "two-a")]
    missing_left = [(1, 20, None, None)]
    missing_right = [(None, None, 3, "three"), (None, None, None, "null-key"), (None, None, 5, "five")]
    result = [
        (f"composition-{kind}-join", left_input.join(right_input, on="cargo_id", how=kind)
         .select(*joined_columns), joined(rows), 2)
        for kind, rows in [
            ("inner", matched), ("left", matched + missing_left),
            ("right", matched + missing_right), ("full", matched + missing_left + missing_right),
        ]
    ]
    result.extend([
        ("composition-declared-sql", named.filter(sl.col("load_units") < 60).select("cargo_id", "load_units"),
         [{"cargo_id": None, "load_units": 50}], 1),
        ("composition-declared-join", named.join(dimension, on="cargo_id", how="left")
         .select("f.cargo_id AS cargo_id", "f.load_units AS load_units", "d.label AS label"),
         [{"cargo_id": 4, "load_units": 60, "label": None},
          {"cargo_id": None, "load_units": 50, "label": None}], 2),
        ("composition-declared-set", named.select("cargo_id").union_all(frame.select("cargo_id").limit(1))
         .sort("cargo_id", nulls="last"), [{"cargo_id": 2}, {"cargo_id": 4}, {"cargo_id": None}], 1),
        ("composition-duplicate-join", left_input.join(
            dimension.filter(sl.col("cargo_id") == 2), on="cargo_id").select(*joined_columns),
         joined([(2, 10, 2, "two-a"), (2, 10, 2, "two-b"),
                 (2, 11, 2, "two-a"), (2, 11, 2, "two-b")]), 2),
        ("composition-semi-join", left_input.join(right_input, on="cargo_id", how="semi")
         .select("f.cargo_id AS cargo_id", "f.load_units AS load_units"),
         [{"cargo_id": 2, "load_units": 10}, {"cargo_id": 2, "load_units": 11}], 2),
        ("composition-anti-join", left_input.join(right_input, on="cargo_id", how="anti")
         .select("f.cargo_id AS cargo_id", "f.load_units AS load_units"),
         [{"cargo_id": 1, "load_units": 20}], 2),
        ("composition-repeated-join", frame.filter(sl.col("load_units") > 10)
         .join(dimension, on="cargo_id", how="left")
         .select("f.cargo_id AS cargo_id", "f.load_units AS load_units", "d.label AS label")
         .join(dimension.filter(sl.col("label") == "two-a"), on="cargo_id")
         .select("f.load_units AS load_units", "f.label AS first_label", "d.label AS second_label"),
         [{"load_units": 11, "first_label": "two-a", "second_label": "two-a"},
          {"load_units": 11, "first_label": "two-b", "second_label": "two-a"}], 2),
        ("composition-limit-filter-order", frame.sort("load_units", descending=True).limit(4)
         .filter(sl.col("load_units") >= 30).select("load_units AS amount").sort("amount").limit(2),
         [{"amount": 30}, {"amount": 50}], 1),
        ("composition-replace-order", frame.select("load_units", "cargo_id")
         .with_column("load_units", sl.col("load_units") + 1)
         .with_column("copy", sl.col("load_units"))
         .with_column("load_units", sl.col("load_units") * 2).limit(2),
         [{"load_units": 22, "cargo_id": 2, "copy": 11},
          {"load_units": 42, "cargo_id": 1, "copy": 21}], 1),
        ("composition-unknown-replace", frame.limit(2).with_column("cargo_id", sl.col("cargo_id") + 100),
         [{"cargo_id": 102, "load_units": 10}, {"cargo_id": 101, "load_units": 20}], 1),
        ("composition-regroup", frame.group_by("cargo_id").agg(subtotal="sum(load_units)")
         .filter(sl.col("subtotal") > 20.0).with_column("bucket", 1).group_by("bucket")
         .agg(total="sum(subtotal)", groups="count(*)").select("total", "groups"),
         [{"total": 161.0, "groups": 4}], 1),
        ("composition-window-filter", frame.window("ROW_NUMBER() OVER (ORDER BY load_units DESC) AS position")
         .filter(sl.col("position") <= 2).select("cargo_id", "position").sort("position"),
         [{"cargo_id": 4, "position": 1}, {"cargo_id": None, "position": 2}], 1),
        ("composition-empty-window", frame.filter(sl.col("load_units") < 0).select("cargo_id AS renamed")
         .window("ROW_NUMBER() OVER (ORDER BY renamed) AS rn").filter(sl.col("rn") > 0), [], 1),
    ])

    limited_set = frame.select("cargo_id").limit(2).union_all(
        dimension.select("cargo_id").sort("cargo_id", descending=True, nulls="last").limit(1))
    result.extend([
        ("composition-post-set", limited_set.filter(sl.col("cargo_id") > 1).sort("cargo_id", descending=True)
         .with_column("cargo_id", sl.col("cargo_id") + 10), [{"cargo_id": 15}, {"cargo_id": 12}], 2),
        ("composition-set-window", limited_set.window("ROW_NUMBER() OVER (ORDER BY cargo_id DESC) AS rn")
         .filter(sl.col("rn") <= 2).select("cargo_id").sort("cargo_id"), [{"cargo_id": 2}, {"cargo_id": 5}], 2),
        ("composition-set-join", limited_set.join(dimension.filter(sl.col("cargo_id") == 2), on="cargo_id")
         .select("f.cargo_id AS cargo_id", "d.label AS label"),
         [{"cargo_id": 2, "label": "two-a"}, {"cargo_id": 2, "label": "two-b"}], 2),
        ("composition-set-right", frame.filter(sl.col("load_units") < 20).join(limited_set, on="cargo_id")
         .select("f.cargo_id AS cargo_id", "f.load_units AS load_units"),
         [{"cargo_id": 2, "load_units": 10}, {"cargo_id": 2, "load_units": 11}], 2),
        ("composition-mixed-sets", limited_set.intersect(dimension.select("cargo_id"))
         .union_all(frame.filter(sl.col("cargo_id") == 3).select("cargo_id"))
         .except_(dimension.filter(sl.col("cargo_id") == 5).select("cargo_id")).sort("cargo_id"),
         [{"cargo_id": 2}, {"cargo_id": 3}], 2),
        ("composition-set-membership", frame.filter(sl.col("cargo_id").isin_source(limited_set, "cargo_id"))
         .select("cargo_id", "load_units"),
         [{"cargo_id": 2, "load_units": 10}, {"cargo_id": 1, "load_units": 20},
          {"cargo_id": 2, "load_units": 11}], 2),
    ])

    transformed = dimension.filter(sl.col("cargo_id").is_not_null()).sort("cargo_id", descending=True).limit(2)
    for name, predicate, expected in [
        ("in", sl.col("cargo_id").isin_source(transformed, "cargo_id"), [3]),
        ("not-in", sl.col("cargo_id").not_in_source(transformed, "cargo_id"), [2, 1, 2, 4]),
        ("any", sl.col("cargo_id").any_source("<", transformed, "cargo_id"), [2, 1, 2, 3, 4]),
        ("all", sl.col("cargo_id").all_source("<", transformed, "cargo_id"), [2, 1, 2]),
        ("exists", sl.exists_source(transformed, select=1, where=sl.col("cargo_id") == sl.outer("cargo_id")), [3]),
    ]:
        result.append((f"composition-transformed-{name}", frame.filter(predicate).select("cargo_id"),
                       [{"cargo_id": value} for value in expected], 2))
    strings = context.read_csv(typed_left, schema={"key": "utf8", "amount": "int64"})
    string_dimension = context.read_csv(typed_right, schema={"key": "utf8", "label": "utf8"})
    result.extend([
        ("composition-typed-inputs", strings.sort("amount", descending=True).limit(1)
         .join(string_dimension.filter(sl.col("label") == "0010"), on="key")
         .select("f.key AS key", "d.label AS label").with_column("copy", sl.col("key")),
         [{"key": "1", "label": "0010", "copy": "1"}], 2),
        ("composition-mixed-inputs", frame.limit(3).join(
            context.read_json(raw_right).filter(sl.col("label") == "two-a"), on="cargo_id", how="left")
         .select("f.cargo_id AS cargo_id", "f.load_units AS load_units", "d.label AS label"),
         [{"cargo_id": 2, "load_units": 10, "label": "two-a"},
          {"cargo_id": 1, "load_units": 20, "label": None},
          {"cargo_id": 2, "load_units": 11, "label": "two-a"}], 2),
    ])
    return result
