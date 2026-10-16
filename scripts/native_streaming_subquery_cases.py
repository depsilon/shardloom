# SPDX-License-Identifier: Apache-2.0
"""Finite subquery results and completion through public SQL/DataFrame consumers."""

from __future__ import annotations

import operator

import shardloom as sl
from shardloom._result_schema import ResultType as T
from shardloom.query import SqlWorkflow
from native_report_evidence import require_native_allocation_peak
from native_streaming_unary_cases import frame
from native_workflow_outputs import LOCAL_FORMATS, verify_output

SCHEMA = {"n": "int64", "s": "utf8"}
LEFT = [dict(n=n, s=s) for n, s in ((1, "a"), (2, "b"), (3, "c"), (None, "z"))]
RIGHT = [dict(n=n, s=s) for n, s in ((2, "b"), (None, "q"), (2, "b"))]
COMPARISONS = {"eq": ("=", operator.eq), "ne": ("!=", operator.ne),
               "lt": ("<", operator.lt), "le": ("<=", operator.le),
               "gt": (">", operator.gt), "ge": (">=", operator.ge)}
KINDS = ("in", "not-in", "row-in", "row-not-in", "exists", "not-exists",
         *(f"{quantifier}-{comparison}" for quantifier in ("any", "all") for comparison in COMPARISONS),
         "scalar-one", "scalar-zero", "parameterized-scalar", "parameterized-in",
         "parameterized-exists", "correlated-exists", "guarded-scalar", "coalesce", "shared-scalar")
POLICY = {"memory_bytes": 32 << 20, "max_parallelism": 1}


def fixture(shape):
    left, right = list(LEFT), list(RIGHT)
    if shape == "empty-outer":
        left = []
    elif shape == "empty-inner":
        right = []
    elif shape == "all-null":
        left = [dict.fromkeys(SCHEMA) for _ in range(3)]
        right = [dict.fromkeys(SCHEMA) for _ in range(2)]
    else:
        assert shape == "mixed"
    return left, right


def sql_equal(left, right):
    """SQL row equality: a known unequal component dominates unknown components."""
    values = [None if a is None or b is None else a == b for a, b in zip(left, right, strict=True)]
    return False if False in values else None if None in values else True


def in_set(left, right):
    comparisons = [sql_equal(left, row) for row in right]
    return True if True in comparisons else None if None in comparisons else False


def oracle(kind, left, right):
    """Small scalar reference, independent of the native retained/indexed implementation."""
    result = []
    for row in left:
        if kind in ("guarded-scalar", "coalesce"):
            answer = 7
        elif kind in ("scalar-one", "shared-scalar"):
            rows = left if kind == "shared-scalar" else right
            answer = rows[0]["n"] if rows else None
        elif kind == "scalar-zero":
            answer = None
        elif kind.startswith("parameterized-"):
            selected = [value["n"] for value in right if row["n"] is not None
                        and value["n"] is not None and value["n"] <= row["n"]]
            if kind == "parameterized-scalar":
                answer = max(selected, default=None)
            elif kind == "parameterized-in":
                answer = in_set((row["n"],), [(value,) for value in selected])
            else:
                answer = bool(selected)
        elif kind == "correlated-exists":
            answer = any(row["s"] is not None and other["s"] is not None
                         and row["s"] == other["s"] for other in right)
        elif kind in ("exists", "not-exists"):
            answer = bool(right) if kind == "exists" else not right
        elif kind.startswith(("any-", "all-")):
            quantifier, comparison = kind.split("-")
            compare = COMPARISONS[comparison][1]
            values = [None if row["n"] is None or other["n"] is None
                      else compare(row["n"], other["n"]) for other in right]
            decisive = quantifier == "any"
            answer = decisive if decisive in values else None if None in values else not decisive
        else:
            columns = ("n", "s") if kind.startswith("row-") else ("n",)
            answer = in_set(tuple(row[name] for name in columns),
                            [tuple(other[name] for name in columns) for other in right])
            if "not-in" in kind and answer is not None:
                answer = not answer
        result.append(dict(row, answer=answer))
    return result


def apply(left, right, kind, surface="dataframe"):
    if kind in ("in", "not-in"):
        expression = getattr(sl.col("n"), "isin_source" if kind == "in" else "not_in_source")(right, "n")
    elif kind in ("row-in", "row-not-in"):
        expression = getattr(sl, "row_in_source" if kind == "row-in" else "row_not_in_source")(["n", "s"], right, ["n", "s"])
    elif kind in ("exists", "not-exists"):
        expression = getattr(sl, "exists_source" if kind == "exists" else "not_exists_source")(right)
    elif kind.startswith(("any-", "all-")):
        quantifier, comparison = kind.split("-")
        expression = getattr(sl.col("n"), quantifier + "_source")(COMPARISONS[comparison][0], right, "n")
    elif kind in ("scalar-one", "shared-scalar", "scalar-zero"):
        inner = left if kind == "shared-scalar" else right
        expression = sl.scalar_subquery(inner.select("n").limit(0 if kind == "scalar-zero" else 1))
    elif kind.startswith("parameterized-"):
        inner = right.filter(sl.col("n") <= sl.outer("n"))
        if kind == "parameterized-scalar":
            expression = sl.scalar_subquery(inner.agg(answer="MAX(n)"))
        elif kind == "parameterized-in":
            expression = sl.col("n").isin_source(inner, "n")
        else:
            expression = sl.exists_source(inner)
    elif kind == "correlated-exists":
        expression = sl.exists_source(right, where=sl.col("s") == sl.outer("s"))
    elif kind == "guarded-scalar":
        expression = sl.case_when(sl.col("n") < 0, sl.scalar_subquery(right.select("1/0 AS bad")), 7)
    else:
        assert kind == "coalesce"
        expression = sl.case_when(sl.col("n").is_null(), 7, 7).fill_null(
            sl.scalar_subquery(right.select("n"))
        )
    result = left.with_column("answer", expression)
    if surface == "sql":
        result = SqlWorkflow(sql(left, right, kind), result.client, source_bindings=result._declared_sources())
    return result


def sql(left, right, kind):
    """Independently authored SQL with the same declared source bindings."""
    relation = f"({right._relation_statement()}) AS r"
    if kind in ("in", "not-in", "row-in", "row-not-in"):
        columns = "n,s" if kind.startswith("row-") else "n"
        value = f"({columns})" if kind.startswith("row-") else columns
        operation = "NOT IN" if "not-in" in kind else "IN"
        expression = f"{value} {operation} (SELECT {columns} FROM {relation})"
    elif kind in ("exists", "not-exists", "correlated-exists"):
        operation = "NOT EXISTS" if kind == "not-exists" else "EXISTS"
        predicate = " WHERE s = outer.s" if kind == "correlated-exists" else ""
        expression = f"{operation} (SELECT * FROM {relation}{predicate})"
    elif kind.startswith(("any-", "all-")):
        quantifier, comparison = kind.split("-")
        expression = f"n {COMPARISONS[comparison][0]} {quantifier.upper()} (SELECT n FROM {relation})"
    elif kind.startswith("parameterized-"):
        filtered = f"(SELECT * FROM {relation} WHERE n <= outer.n) AS filtered"
        if kind == "parameterized-scalar":
            expression = f"(SELECT MAX(n) FROM {filtered})"
        elif kind == "parameterized-in":
            expression = f"n IN (SELECT n FROM {filtered})"
        else:
            expression = f"EXISTS (SELECT * FROM {filtered})"
    elif kind in ("scalar-one", "scalar-zero", "shared-scalar"):
        count = 0 if kind == "scalar-zero" else 1
        expression = f"(SELECT n FROM {relation} LIMIT {count})"
    elif kind == "guarded-scalar":
        expression = f"CASE WHEN n < 0 THEN (SELECT 1/0 AS bad FROM {relation}) ELSE 7 END"
    else:
        assert kind == "coalesce"
        expression = f"COALESCE(CASE WHEN n IS NULL THEN 7 ELSE 7 END, (SELECT n FROM {relation}))"
    return f"SELECT n,s,{expression} AS answer FROM ({left._relation_statement()}) AS l"


def declaration(harness, kind, shape, flags, surface="dataframe", width=2):
    left_rows, right_rows = fixture(shape)
    left, left_trace = frame(harness, left_rows, SCHEMA, width=width, streaming=flags[0])
    if kind == "shared-scalar":
        right, traces, enabled = left, [left_trace], [flags[0]]
    else:
        right, right_trace = frame(harness, right_rows, SCHEMA, width=width, streaming=flags[1])
        traces, enabled = [left_trace, right_trace], list(flags)
    return apply(left, right, kind, surface), oracle(kind, left_rows, right_rows), traces, enabled


def complete_inputs(harness, name, envelope, traces, flags=None, *, grant=POLICY["memory_bytes"]):
    assert all(trace["opened"] == 1 and trace["ended"] and trace["closed"] for trace in traces), traces
    flags = flags or [True] * len(traces)
    streaming = [trace for trace, enabled in zip(traces, flags, strict=True) if enabled]
    harness.completed(name, envelope, sources=len(streaming),
                      batches=sum(trace["batches"] for trace in streaming),
                      rows=sum(trace["rows"] for trace in streaming))
    require_native_allocation_peak(name, envelope, grant)


def completed(harness, name, envelope, traces, flags, kind, shape):
    complete_inputs(harness, name, envelope, traces, flags)
    left, _ = fixture(shape)
    if kind == "shared-scalar":
        assert envelope.field_int("native_input_replay_sources") == 1
        assert envelope.field_int("native_input_replay_prefilled_sources") == 1
        assert envelope.field_int("native_input_replay_passes") == 1 + bool(left)
    elif kind.startswith("parameterized-"):
        repeated = bool(left) and flags[1]
        assert envelope.field_int("native_input_replay_sources") == int(repeated)
        assert envelope.field_int("native_input_replay_passes") == (len(left) - 1 if repeated else 0)
    if kind in ("guarded-scalar", "coalesce"):
        assert envelope.field_int("native_input_subquery_build_rows_detached") == 0
        assert envelope.field_int("native_input_subquery_validation_sources") == int(flags[1])


def run(harness):
    references = {kind: {shape: oracle(kind, *fixture(shape))
                         for shape in ("mixed", "empty-outer", "empty-inner", "all-null")}
                  for kind in KINDS}
    harness.json(harness.logs / "subquery-predeclared-results.json", references)
    for kind in KINDS:
        for shape in ("mixed", "empty-outer", "empty-inner", "all-null"):
            flag_sets = [(True, True)] if kind == "shared-scalar" else [(True, True), (True, False), (False, True)]
            for flags in flag_sets:
                for surface in ("dataframe", "sql"):
                    terminals = ("collect", "batches") if shape == "mixed" and all(flags) else ("collect",)
                    for terminal in terminals:
                        name = f"subquery-{kind}-{shape}-{int(flags[0])}{int(flags[1])}-{surface}-{terminal}"

                        def exercise(name=name, kind=kind, shape=shape, flags=flags, surface=surface, terminal=terminal):
                            query, expected, traces, enabled = declaration(harness, kind, shape, flags, surface)
                            assert all(trace["opened"] == 0 for trace in traces)
                            harness.json(harness.logs / (name + "-declaration.json"),
                                         {"sql": query._relation_statement(), "expected": expected})
                            if terminal == "collect":
                                report = query.collect(check=True, **POLICY)
                                actual, schema = list(report.result_rows), report.result_schema
                            else:
                                actual, held, schema = [], [], None
                                with query.iter_batches(batch_rows=2, **POLICY) as iterator:
                                    for batch in iterator:
                                        assert iterator.report is None and len(batch.result_rows) <= 2
                                        schema = schema or batch.result_schema
                                        assert schema == batch.result_schema
                                        actual.extend(batch.result_rows)
                                        held.append(batch)
                                    report = iterator.report
                                    assert report is not None and iterator._process.poll() == 0
                                assert [row for batch in held for row in batch.result_rows] == actual
                            harness.values(name, actual, expected)
                            scalar = "scalar" in kind or kind == "coalesce"
                            nullable = kind not in ("exists", "not-exists", "parameterized-exists",
                                                    "correlated-exists", "coalesce")
                            assert schema == (("n", T("int64", True)), ("s", T("utf8", True)),
                                              ("answer", T("int64" if scalar else "bool", nullable))), schema
                            completed(harness, name, report.envelope, traces, enabled, kind, shape)
                            return {"complete_rows": len(actual), "traces": traces,
                                    "schema": [(column, dtype.name, dtype.nullable) for column, dtype in schema]}
                        harness.case(name, exercise)

    def accepted(name, report):
        harness.envelope(name, report.envelope)
        return report.envelope

    def checked_values(name, actual, expected, _path=None):
        harness.values(name, actual, expected)

    for kind in ("in", "parameterized-scalar", "guarded-scalar", "shared-scalar"):
        for destination in (*LOCAL_FORMATS, "fanout"):
            name = f"subquery-{kind}-write-{destination}"

            def write(name=name, kind=kind, destination=destination):
                query, expected, traces, flags = declaration(harness, kind, "mixed", (True, True))
                formats = LOCAL_FORMATS if destination == "fanout" else (destination,)
                targets = {extension: harness.output / f"{name}.{extension}" for extension in formats}
                report = (query.fanout(targets, **POLICY) if destination == "fanout" else
                          getattr(query, "write_" + destination)(targets[destination], **POLICY))
                completed(harness, name, report.envelope, traces, flags, kind, "mixed")
                if destination == "fanout":
                    assert report.envelope.field_int("native_vortex_result_export_execution_count") == 1
                    assert report.envelope.field_int("native_vortex_result_export_replay_count") == len(formats)
                for extension, path in targets.items():
                    verify_output(harness.context, path, extension, expected, ["n", "s", "answer"],
                                  name=name + "-" + extension, accepted=accepted, complete=checked_values)
                return {"artifacts": [harness.artifact(path) for path in targets.values()], "traces": traces}
            harness.case(name, write)
