# SPDX-License-Identifier: Apache-2.0
"""Complete finite unary acceptance through the public SQL and DataFrame surfaces.

All expected rows are computed and saved before the associated execution.
"""

from __future__ import annotations

import json

from shardloom._result_schema import ResultType as T
from shardloom.query import LazyFrame
from native_streaming_set_cases import complete
from native_workflow_outputs import LOCAL_FORMATS, verify_output
from native_unary_reference import rolling_values, sample_ordinals

SCHEMA = {"n": "int64", "s": "utf8"}
FIELDS = (("n", T("int64", True)), ("s", T("utf8", True)))
ROWS = [dict(n=n, s=s) for n, s in (
    (3, "α"), (None, None), (3, "α"), (1, "β"),
    (2, "γ"), (None, "δ"), (1, "ε"), (4, ""))]
LISTS = [[7, None, 8], None, [7, None, 8], [], [9], [10], [11, 12], [13]]
KINDS = ("distinct", "dedup-first", "dedup-last", "dedup-false",
         "mask-first", "mask-last", "mask-false", "tail", "sample",
         "expression", "melt", "rolling", "explode")
POLICY = {"memory_bytes": 32 << 20, "max_parallelism": 1}


def fixture(kind, shape="mixed"):
    rows = [dict(row) for row in ROWS]
    schema, fields = dict(SCHEMA), FIELDS
    if kind == "explode":
        rows = [dict(row, items=value) for row, value in zip(rows, LISTS, strict=True)]
        schema["items"] = {"type": "list", "item": "int16"}
        fields += (("items", T("list", True, (T("int16", True),))),)
    if shape == "empty":
        rows = []
    elif shape == "all-null":
        rows = [dict.fromkeys(schema) for _ in range(3)]
    else:
        assert shape == "mixed"
    return rows, schema, fields


def frame(harness, rows, schema, *, width=3, streaming=True):
    assert width > 0
    trace = {"opened": 0, "batches": 0, "rows": 0, "ended": False, "closed": False}

    def produce():
        trace["opened"] += 1
        try:
            for start in range(0, len(rows), width):
                for batch in ([], rows[start:start + width]):
                    trace["batches"] += 1
                    trace["rows"] += len(batch)
                    yield batch
            trace["batches"] += 1
            yield []
            trace["ended"] = True
        finally:
            trace["closed"] = True

    return harness.context.from_batches(produce, schema=schema, streaming=streaming), trace


def oracle(kind, rows):
    if kind == "distinct":
        return [row for index, row in enumerate(rows) if row not in rows[:index]]
    if kind.startswith(("dedup-", "mask-")):
        keep = kind.split("-", 1)[1]
        flags = []
        for index, row in enumerate(rows):
            before = any(other["n"] == row["n"] for other in rows[:index])
            after = any(other["n"] == row["n"] for other in rows[index + 1:])
            flags.append(before if keep == "first" else after if keep == "last" else before or after)
        if kind.startswith("mask-"):
            return [{"duplicated": flag} for flag in flags]
        return [row for row, flag in zip(rows, flags, strict=True) if not flag]
    if kind == "tail":
        return rows[-3:]
    if kind == "sample":
        return [rows[index] for index in sample_ordinals(len(rows), 3, 7)]
    if kind == "expression":
        prior, result = dict.fromkeys(SCHEMA), []
        for index, row in enumerate(rows):
            for name, value in row.items():
                if value is not None:
                    prior[name] = value
            result.append(dict(prior, index=index))
        return result
    if kind == "melt":
        return [{"s": row["s"], "variable": "n", "value": row["n"]} for row in rows]
    if kind == "rolling":
        return [{"total": value} for value in rolling_values(
            [row["n"] for row in rows], 3, 1, "sum", center=True)]
    assert kind == "explode"
    return [dict(row, items=item) for row in rows
            for item in ([None] if row["items"] is None else row["items"])]


def output_fields(kind, fields):
    if kind.startswith("mask-"):
        return (("duplicated", T("bool", False)),)
    if kind == "expression":
        return fields + (("index", T("uint64", False)),)
    if kind == "melt":
        return (("s", T("utf8", True)), ("variable", T("utf8", False)),
                ("value", T("int64", True)))
    if kind == "rolling":
        return (("total", T("float64", False)),)
    if kind == "explode":
        return FIELDS + (("items", T("int16", True)),)
    return fields


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def payload(value):
    return literal(json.dumps(value, separators=(",", ":"), allow_nan=False))


def apply(source, kind, surface="dataframe"):
    if surface == "sql":
        if kind == "distinct":
            function, arguments = "DISTINCT_ROWS", "'n,s'"
        elif kind.startswith(("dedup-", "mask-")):
            function = "DROP_DUPLICATES" if kind.startswith("dedup-") else "DUPLICATED"
            arguments = "'n', " + literal(kind.split("-", 1)[1])
        elif kind == "tail":
            function, arguments = "TAIL", "3"
        elif kind == "sample":
            function, arguments = "SAMPLE", payload({"n": 3, "seed": 7})
        elif kind == "expression":
            function, arguments = "REWRITE", payload({"columns": list(SCHEMA), "rewrites": [
                {"kind": "forward_fill_null", "target_column": "n"},
                {"kind": "forward_fill_null", "target_column": "s"},
                {"kind": "row_number", "target_column": "index", "start": 0}]})
        elif kind == "melt":
            function, arguments = "MELT", payload({"id_columns": ["s"], "value_columns": ["n"],
                "variable_column": "variable", "value_column": "value"})
        elif kind == "rolling":
            function, arguments = "ROLLING", payload({"source_column": "n", "output_column": "total",
                "window_size": 3, "min_periods": 1, "aggregate": "sum", "center": True})
        else:
            assert kind == "explode"
            function, arguments = "EXPLODE", payload({"column": "items", "output_columns": ["n", "s", "items"]})
        return LazyFrame.from_sql(f"SELECT * FROM {function}(({source._relation_statement()}), {arguments}) AS u",
                           source.client, source_bindings=source._declared_sources())
    assert surface == "dataframe"
    if kind == "distinct":
        return source.distinct()
    if kind.startswith(("dedup-", "mask-")):
        keep = kind.split("-", 1)[1]
        keep = False if keep == "false" else keep
        method = source.drop_duplicates if kind.startswith("dedup-") else source.duplicated
        return method("n", keep=keep)
    if kind == "tail":
        return source.tail(3)
    if kind == "sample":
        return source.sample(3, random_state=7)
    if kind == "expression":
        return source.fillna(method="ffill").reset_index()
    if kind == "melt":
        return source.melt(id_vars="s", value_vars="n")
    if kind == "rolling":
        return source.rolling(3, min_periods=1, center=True).sum("n", alias="total")
    assert kind == "explode"
    return source.explode("items")


def consume(harness, name, query, expected, fields, traces, terminal="batches", *,
            streaming=None, replay=0, policy=None):
    requested = policy or POLICY
    assert all(trace["opened"] == 0 for trace in traces)
    if terminal == "collect":
        report = query.collect(check=True, **requested)
        actual, envelope = report.result_rows, report.envelope
        assert report.result_schema == fields, (name, report.result_schema, fields)
    else:
        assert terminal == "batches"
        actual, retained = [], []
        with query.iter_batches(batch_rows=2, **requested) as iterator:
            for batch in iterator:
                assert iterator.report is None and len(batch.result_rows) <= 2
                assert batch.result_schema == fields, (name, batch.result_schema, fields)
                actual.extend(batch.result_rows)
                retained.append(batch)
            assert iterator._schema == fields, (name, iterator._schema, fields)
            assert iterator.report is not None and iterator._process.poll() == 0
            envelope = iterator.report.envelope
        assert [row for batch in retained for row in batch.result_rows] == actual
    harness.values(name, actual, expected)
    complete(harness, name, envelope, traces, streaming or [True] * len(traces),
             replay=replay, grant=requested["memory_bytes"])
    return envelope


def run(harness):
    declared = []
    for kind in KINDS:
        for shape in ("mixed", "empty", "all-null"):
            rows, schema, fields = fixture(kind, shape)
            declared.append({"kind": kind, "shape": shape, "rows": rows,
                             "schema": schema, "expected": oracle(kind, rows),
                             "fields": repr(output_fields(kind, fields))})
    harness.json(harness.logs / "unary-predeclared-values.json", declared)
    for entry in declared:
        kind, shape, rows, schema = (entry[key] for key in ("kind", "shape", "rows", "schema"))
        fields = output_fields(kind, fixture(kind, shape)[2])
        for width in ((1, 3, 8) if shape == "mixed" else (3,)):
            for surface in ("dataframe", "sql"):
                for terminal in ("collect", "batches"):
                    name = f"unary-{kind}-{shape}-width-{width}-{surface}-{terminal}"

                    def check(name=name, kind=kind, rows=rows, schema=schema, fields=fields,
                              width=width, surface=surface, terminal=terminal, expected=entry["expected"]):
                        source, trace = frame(harness, rows, schema, width=width)
                        consume(harness, name, apply(source, kind, surface), expected, fields, [trace], terminal)
                    harness.case(name, check)
    destinations(harness)


def destinations(harness):
    def accepted(name, report):
        harness.envelope(name, report.envelope)
        return report.envelope

    def values(name, actual, expected, _path):
        harness.values(name, actual, expected)

    for kind in KINDS:
        for empty in (False, True):
            for destination in (*LOCAL_FORMATS, "fanout"):
                name = f"unary-{kind}-empty-{empty}-write-{destination}"

                def write(name=name, kind=kind, empty=empty, destination=destination):
                    rows, schema, fields = fixture(kind)
                    expected = [] if empty else oracle(kind, rows)
                    fields = output_fields(kind, fields)
                    source, trace = frame(harness, rows, schema)
                    query = apply(source, kind)
                    if empty:
                        query = query.limit(0)
                    formats = LOCAL_FORMATS if destination == "fanout" else (destination,)
                    targets = {extension: harness.output / f"{name}.{extension}" for extension in formats}
                    report = (query.fanout(targets, **POLICY) if destination == "fanout" else
                              getattr(query, "write_" + destination)(targets[destination], **POLICY))
                    complete(harness, name, report.envelope, [trace], [True])
                    if destination == "fanout":
                        assert report.envelope.field_int("native_vortex_result_export_execution_count") == 1
                        assert report.envelope.field_int("native_vortex_result_export_target_count") == len(formats)
                        assert report.envelope.field_int("native_vortex_result_export_replay_count") == len(formats)
                    for extension, path in targets.items():
                        verify_output(harness.context, path, extension, expected, [field[0] for field in fields],
                                      name=name + "-" + extension, accepted=accepted, complete=values)
                    return {"artifacts": [harness.artifact(path) for path in targets.values()],
                            "complete_rows_per_output": len(expected), "producer_executions": trace["opened"]}
                harness.case(name, write)
