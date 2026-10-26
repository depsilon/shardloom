# SPDX-License-Identifier: Apache-2.0
"""Unary operation position across finite native relational and rich input stages."""

from __future__ import annotations

from shardloom._result_schema import ResultType as T
from shardloom.query import LazyFrame
from native_streaming_unary_cases import (
    KINDS, POLICY, apply, consume, fixture, frame, oracle, output_fields, payload,
)
from native_streaming_set_cases import complete
from native_streaming_dynamic_cases import (
    ROWS as PIVOT_ROWS, SCHEMA as PIVOT_SCHEMA, EXPECTED as PIVOT_VALUES, pivot,
)
from native_typed_input_fixtures import rich_fixture, rich_python_values
from native_typed_input_cases import assert_python_values


def sql(source, statement, extra=()):
    return LazyFrame.from_sql(statement, source.client,
                       source_bindings=source._declared_sources() + tuple(extra))


def ordered(rows, column):
    return sorted(rows, key=lambda row: (row[column] is None, row[column]))


def before(harness, source, rows, fields, stage):
    relation = source._relation_statement()
    names = ",".join(name for name, _ in fields)
    extra_traces, replay = [], 0
    if stage == "filter":
        query = source.filter("n IS NOT NULL AND n >= 1")
        rows = [row for row in rows if row["n"] is not None and row["n"] >= 1]
    elif stage == "project":
        query = source.select("n * 2 AS n", *(name for name, _ in fields if name != "n"))
        rows = [dict(row, n=None if row["n"] is None else row["n"] * 2) for row in rows]
    elif stage == "sort":
        query = sql(source, f"SELECT * FROM ({relation}) AS q ORDER BY n ASC NULLS LAST")
        rows = ordered(rows, "n")
    elif stage == "limit":
        query, rows = source.limit(5), rows[:5]
    elif stage == "aggregate":
        query = sql(source, f"SELECT MAX(n) AS n,s FROM ({relation}) AS q GROUP BY s ORDER BY s ASC NULLS LAST")
        keys = sorted({row["s"] for row in rows}, key=lambda key: (key is None, key))
        rows = [{"n": max(values) if values else None, "s": key} for key in keys
                for values in [[row["n"] for row in rows if row["s"] == key and row["n"] is not None]]]
    elif stage == "window":
        query = sql(source, f"SELECT {names} FROM (SELECT {names},ROW_NUMBER() OVER "
                    f"(ORDER BY n ASC NULLS LAST) AS position FROM ({relation}) AS q) AS w "
                    "WHERE position <= 5 ORDER BY position")
        rows = ordered(rows, "n")[:5]
    elif stage == "join":
        dimension, trace = frame(harness, [{"tag": 1}, {"tag": 2}], {"tag": "int16"}, width=1)
        extra_traces.append(trace)
        projection = ",".join(f"q.{name} AS {name}" for name, _ in fields)
        query = sql(source, f"SELECT {projection} FROM ({relation}) AS q CROSS JOIN "
                    f"({dimension._relation_statement()}) AS d", dimension._declared_sources())
        rows = [row for row in rows for _ in range(2)]
    elif stage == "set":
        query, rows, replay = source.union_all(source), rows * 2, 1
    else:
        assert stage == "unary"
        query = source.distinct()
        rows = [row for index, row in enumerate(rows) if row not in rows[:index]]
    return query, rows, extra_traces, replay


def after(harness, query, rows, fields, stage):
    relation, extra_traces, replay = query._relation_statement(), [], 0
    if stage.startswith("limit-"):
        count = int(stage.split("-", 1)[1])
        query, rows = query.limit(count), rows[:count]
    elif stage == "project":
        names = [name for name, _ in fields]
        query = query.select(*(f"{name} AS renamed_{index}" for index, name in enumerate(names)))
        rows = [{f"renamed_{index}": row[name] for index, name in enumerate(names)} for row in rows]
        fields = tuple((f"renamed_{index}", dtype) for index, (_, dtype) in enumerate(fields))
    elif stage == "sort":
        key = fields[0][0]
        query = sql(query, f"SELECT * FROM ({relation}) AS q ORDER BY {key} ASC NULLS LAST")
        rows = ordered(rows, key)
    elif stage == "aggregate":
        query = sql(query, f"SELECT COUNT(*) AS rows FROM ({relation}) AS q")
        rows, fields = [{"rows": len(rows)}], (("rows", T("uint64", False)),)
    elif stage == "window":
        query = sql(query, f"SELECT *,ROW_NUMBER() OVER (ORDER BY {fields[0][0]} ASC NULLS LAST) "
                    f"AS position FROM ({relation}) AS q ORDER BY position")
        rows = [dict(row, position=index) for index, row in enumerate(ordered(rows, fields[0][0]), 1)]
        fields += (("position", T("int64", False)),)
    elif stage == "join":
        dimension, trace = frame(harness, [{"tag": 1}, {"tag": 2}], {"tag": "int16"}, width=1)
        extra_traces.append(trace)
        projection = ",".join(f"q.{name} AS {name}" for name, _ in fields)
        query = sql(query, f"SELECT {projection} FROM ({relation}) AS q CROSS JOIN "
                    f"({dimension._relation_statement()}) AS d", dimension._declared_sources())
        rows = [row for row in rows for _ in range(2)]
    elif stage == "set":
        query, rows, replay = query.union_all(query), rows * 2, 1
    else:
        assert stage == "unary"
        query = sql(query, f"SELECT * FROM TAIL(({relation}), 2) AS u")
        rows = rows[-2:]
    return query, rows, fields, extra_traces, replay


def run(harness):
    stages = {
        "before": ("filter", "project", "sort", "limit", "aggregate", "window", "join", "set", "unary"),
        "after": ("limit-0", "limit-2", "project", "sort", "aggregate", "window", "join", "set", "unary"),
    }
    for kind in KINDS:
        for position, choices in stages.items():
            for stage in choices:
                if kind == "explode" and position == "before" and stage == "aggregate":
                    continue  # That grouped projection intentionally has no list column.
                name = f"unary-{kind}-{position}-{stage}"

                def check(name=name, kind=kind, position=position, stage=stage):
                    rows, schema, fields = fixture(kind)
                    source, trace = frame(harness, rows, schema, width=2)
                    if position == "before":
                        source, rows, extra, replay = before(harness, source, rows, fields, stage)
                        query = apply(source, kind, "sql")
                        expected, fields = oracle(kind, rows), output_fields(kind, fields)
                    else:
                        query = apply(source, kind, "sql")
                        query, expected, fields, extra, replay = after(
                            harness, query, oracle(kind, rows), output_fields(kind, fields), stage)
                    harness.json(harness.logs / (name + "-predeclared.json"), expected)
                    consume(harness, name, query, expected, fields, [trace, *extra], replay=replay)
                harness.case(name, check)
    dynamic(harness)
    rich(harness)


def dynamic(harness):
    for position in ("before", "after"):
        for kind in ("distinct", "tail", "sample", "expression", "rolling"):
            # Each scenario explicitly maps the static unary output into a pivot schema.
            name = f"unary-dynamic-{kind}-{position}"

            def check(name=name, kind=kind, position=position):
                if position == "before":
                    source, trace = frame(harness, PIVOT_ROWS, PIVOT_SCHEMA, width=1)
                    if kind == "distinct":
                        prior, rows = source.distinct(), PIVOT_ROWS
                    elif kind == "tail":
                        prior, rows = source.tail(3), PIVOT_ROWS[-3:]
                    elif kind == "sample":
                        prior, rows = source.sample(4, random_state=7), PIVOT_ROWS
                    elif kind == "expression":
                        prior = source.eval("amount = amount + 1")
                        rows = [dict(row, amount=row["amount"] + 1) for row in PIVOT_ROWS]
                    else:
                        prior = source.rolling(2, min_periods=1).sum("amount", alias="total").select(
                            "1 AS entity", "'a' AS category", "total AS amount")
                        rows = [{"entity": 1, "category": "a", "amount": value} for value in (3, 8, 12, 18)]
                    query = sql(prior, f"SELECT * FROM PIVOT(({prior._relation_statement()}), "
                                f"{payload({'aggregate':'sum','index_column':'entity','pivot_column':'category','value_column':'amount'})}) AS p")
                    categories = sorted({row["category"] for row in rows})
                    expected = [{"entity": entity, **{
                        "pivot_" + category: float(sum(values)) if values else None
                        for category in categories
                        for values in [[row["amount"] for row in rows
                                        if row["entity"] == entity and row["category"] == category]]}}
                        for entity in sorted({row["entity"] for row in rows})]
                    # The literal entity in the rolling projection is non-nullable.
                    fields = (("entity", T("int64", kind != "rolling")),) + tuple(
                        ("pivot_" + category, T("float64", True)) for category in categories)
                else:
                    source, trace = frame(harness, PIVOT_ROWS, PIVOT_SCHEMA, width=1)
                    prior = pivot(source)
                    if kind == "distinct":
                        query, expected = prior.distinct(), PIVOT_VALUES
                    elif kind == "tail":
                        query, expected = prior.tail(1), PIVOT_VALUES[-1:]
                    elif kind == "sample":
                        query, expected = prior.sample(2, random_state=7), PIVOT_VALUES
                    elif kind == "expression":
                        query = prior.eval("pivot_a = pivot_a + 1")
                        expected = [dict(row, pivot_a=row["pivot_a"] + 1) for row in PIVOT_VALUES]
                    else:
                        query = prior.rolling(2, min_periods=1).sum("pivot_a", alias="total")
                        expected = [{"total": 3.0}, {"total": 21.0}]
                    fields = ((("total", T("float64", False)),) if kind == "rolling" else
                              (("entity", T("int64", True)), ("pivot_a", T("float64", True)),
                               ("pivot_b", T("float64", True))))
                harness.json(harness.logs / (name + "-predeclared.json"), expected)
                envelope = consume(harness, name, query, expected, fields, [trace])
                assert envelope.field("relational_schema_binding") == "during_execution"
                assert envelope.field_int("relational_dynamic_schema_stages") == 1
            harness.case(name, check)


def rich(harness):
    schema, fields, rows, expected = rich_fixture()
    objects = [{name: row[name] for name, _ in fields} for row in rich_python_values()]
    indices = [0, 3, 0, 2, 1, 3, 2]
    selections = {
        "distinct": [0, 3, 2, 1], "dedup-first": [0, 3, 2, 1],
        "dedup-last": [0, 1, 3, 2], "dedup-false": [1], "tail": [1, 3, 2],
        # The independently frozen seed-7 mapping over seven rows selects 1,2,4.
        "sample": [3, 0, 1],
    }
    for kind, selected in selections.items():
        for terminal in ("collect", "batches", "vortex"):
            name = f"unary-rich-{kind}-{terminal}"

            def check(name=name, kind=kind, selected=selected, terminal=terminal):
                source, trace = frame(harness, [rows[index] for index in indices], schema, width=2)
                if kind == "distinct":
                    query = source.distinct()
                elif kind.startswith("dedup-"):
                    keep = kind.split("-", 1)[1]
                    query = source.drop_duplicates(["samples", "detail"], keep=False if keep == "false" else keep)
                elif kind == "tail":
                    query = source.tail(3)
                else:
                    query = source.sample(3, random_state=7)
                retained = []
                if terminal == "collect":
                    result = query.collect(check=True, **POLICY)
                    envelope, actual, logical = result.envelope, result.result_rows, result.python_objects
                    assert result.result_schema == fields
                elif terminal == "batches":
                    actual, logical = [], []
                    with query.iter_batches(batch_rows=1, **POLICY) as iterator:
                        for batch in iterator:
                            assert batch.result_schema == fields and iterator.report is None
                            actual.extend(batch.result_rows)
                            logical.extend(batch.python_objects)
                            retained.append(batch)
                        assert iterator.report is not None and iterator._schema == fields
                        envelope = iterator.report.envelope
                else:
                    path = harness.output / (name + ".vortex")
                    report = query.write_vortex(path, **POLICY)
                    envelope = report.envelope
                    result = harness.context.read_vortex(path).collect(check=True, **POLICY)
                    harness.envelope(name + "-reopen", result.envelope)
                    actual, logical = result.result_rows, result.python_objects
                    assert result.result_schema == fields
                harness.values(name, actual, [expected[index] for index in selected])
                assert_python_values(logical, [objects[index] for index in selected])
                complete(harness, name, envelope, [trace], [True])
                if retained:
                    del query, source
                    assert [row for batch in retained for row in batch.result_rows] == list(actual)
            harness.case(name, check)
