# SPDX-License-Identifier: Apache-2.0
"""Parameter-specific pivot predicates through public SQL and DataFrame terminals."""

from __future__ import annotations

import shardloom as sl
from shardloom._result_schema import ResultType as T
from shardloom.query import SqlWorkflow
from native_streaming_subquery_cases import (
    COMPARISONS, POLICY, SCHEMA, complete_inputs, fixture, in_set,
)
from native_streaming_unary_cases import frame
from native_workflow_outputs import LOCAL_FORMATS, verify_output

KINDS = ("in", "not-in", "row-in", "row-not-in", "exists", "not-exists",
         *(f"{quantifier}-{comparison}" for quantifier in ("any", "all") for comparison in COMPARISONS))
FIELDS = (("n", T("int64", True)), ("s", T("utf8", True)))


def inner(source):
    return source.filter(sl.col("n") <= sl.outer("n")).select(
        "n AS entity", "CASE WHEN n=1 THEN 'a' ELSE 'b' END AS category", "n AS amount",
    ).pivot_table(index="entity", columns="category", values="amount", aggfunc="sum").select(
        "CASE WHEN entity=2 THEN CAST(NULL AS BIGINT) ELSE entity END AS n",
        "CASE WHEN entity=1 THEN 'a' ELSE 'b' END AS s")


def predicate(relation, kind):
    if kind in ("in", "not-in"):
        return getattr(sl.col("n"), "isin_source" if kind == "in" else "not_in_source")(relation, "n")
    if kind in ("row-in", "row-not-in"):
        return getattr(sl, "row_in_source" if kind == "row-in" else "row_not_in_source")(
            ["n", "s"], relation, ["n", "s"])
    if kind in ("exists", "not-exists"):
        return getattr(sl, "exists_source" if kind == "exists" else "not_exists_source")(relation)
    quantifier, comparison = kind.split("-")
    return getattr(sl.col("n"), quantifier + "_source")(COMPARISONS[comparison][0], relation, "n")


def sql(left, right, kind):
    """Independently authored SQL, including dynamic lowering and null projection."""
    source = ("SELECT n AS entity,CASE WHEN n=1 THEN 'a' ELSE 'b' END AS category,n AS amount "
              f"FROM ({right._relation_statement()}) AS r WHERE n <= outer.n")
    options = '{"index":"entity","columns":"category","values":"amount","aggregate":"sum"}'
    relation = ("(SELECT CASE WHEN entity=2 THEN CAST(NULL AS BIGINT) ELSE entity END AS n,"
                "CASE WHEN entity=1 THEN 'a' ELSE 'b' END AS s "
                f"FROM PIVOT(({source}), '{options}') AS p) AS i")
    if kind in ("in", "not-in", "row-in", "row-not-in"):
        value, columns = ("(n,s)", "n,s") if kind.startswith("row-") else ("n", "n")
        expression = f"{value} {'NOT IN' if 'not-in' in kind else 'IN'} (SELECT {columns} FROM {relation})"
    elif kind in ("exists", "not-exists"):
        expression = f"{'NOT EXISTS' if kind == 'not-exists' else 'EXISTS'} (SELECT * FROM {relation})"
    else:
        quantifier, comparison = kind.split("-")
        expression = f"n {COMPARISONS[comparison][0]} {quantifier.upper()} (SELECT n FROM {relation})"
    return f"SELECT n,s,{expression} AS answer FROM ({left._relation_statement()}) AS l"


def apply(left, right, kind, surface="dataframe"):
    if surface == "sql":
        return SqlWorkflow(sql(left, right, kind), left.client,
                           source_bindings=left._declared_sources() + right._declared_sources())
    assert surface == "dataframe"
    return left.with_column("answer", predicate(inner(right), kind))


def oracle(kind, left, right):
    """Scalar reference: select parameters, group pivot keys, then SQL truth values."""
    result = []
    for row in left:
        groups = list(dict.fromkeys(other["n"] for other in right if row["n"] is not None
                                   and other["n"] is not None and other["n"] <= row["n"]))
        keys = [(None if value == 2 else value, "a" if value == 1 else "b") for value in groups]
        if kind in ("exists", "not-exists"):
            answer = bool(keys) if kind == "exists" else not keys
        elif kind.startswith(("any-", "all-")):
            quantifier, comparison = kind.split("-")
            values = [None if row["n"] is None or key[0] is None
                      else COMPARISONS[comparison][1](row["n"], key[0]) for key in keys]
            decisive = quantifier == "any"
            answer = decisive if decisive in values else None if None in values else not decisive
        else:
            row_key = (row["n"], row["s"]) if kind.startswith("row-") else (row["n"],)
            answer = in_set(row_key, [key if kind.startswith("row-") else key[:1] for key in keys])
            if "not-in" in kind and answer is not None:
                answer = not answer
        result.append(dict(row, answer=answer))
    return result


def fields(kind):
    return FIELDS + (("answer", T("bool", kind not in ("exists", "not-exists"))),)


def declaration(harness, kind, shape="mixed", flags=(True, True), surface="dataframe", *, shared=False):
    left_rows, right_rows = fixture(shape)
    left, a = frame(harness, left_rows, SCHEMA, width=1, streaming=flags[0])
    if shared:
        right, right_rows, traces, enabled = left, left_rows, [a], [flags[0]]
    else:
        right, b = frame(harness, right_rows, SCHEMA, width=2, streaming=flags[1])
        traces, enabled = [a, b], list(flags)
    return apply(left, right, kind, surface), oracle(kind, left_rows, right_rows), traces, enabled


def consume(harness, name, query, expected, schema, traces, flags=None, terminal="collect", requested=None):
    requested = requested or POLICY
    harness.json(harness.logs / (name + "-predeclared.json"),
                 {"sql": query._relation_statement(), "expected": expected,
                  "schema": [(column, dtype.name, dtype.nullable) for column, dtype in schema]})
    if terminal == "collect":
        report = query.collect(check=True, **requested)
        actual, observed = list(report.result_rows), report.result_schema
    else:
        assert terminal == "batches"
        actual, held = [], []
        with query.iter_batches(batch_rows=2, **requested) as iterator:
            for batch in iterator:
                assert iterator.report is None and len(batch.result_rows) <= 2
                assert batch.result_schema == schema, (name, batch.result_schema, schema)
                actual.extend(batch.result_rows)
                held.append(batch)
            report, observed = iterator.report, iterator._schema
            assert report is not None and iterator._process.poll() == 0
        assert [row for batch in held for row in batch.result_rows] == actual
    harness.values(name, actual, expected)
    assert observed == schema, (name, observed, schema)
    complete_inputs(harness, name, report.envelope, traces, flags, grant=requested["memory_bytes"])
    return report


def run(harness):
    shapes = ("mixed", "empty-outer", "empty-inner", "all-null")
    harness.json(harness.logs / "dynamic-subquery-predeclared-results.json", {
        kind: {shape: oracle(kind, *fixture(shape)) for shape in shapes} for kind in KINDS})
    for kind in KINDS:
        for shared in (False, True):
            for shape in (shapes if not shared else ("mixed", "empty-outer", "all-null")):
                for flags in ([(True, True)] if shared else [(True, True), (True, False), (False, True)]):
                    for surface in ("dataframe", "sql"):
                        for terminal in (("collect", "batches") if shape == "mixed" and all(flags) else ("collect",)):
                            name = f"dynamic-subquery-{kind}-{shape}-{int(flags[0])}{int(flags[1])}-{surface}-{terminal}-shared-{shared}"

                            def check(name=name, kind=kind, shape=shape, flags=flags, surface=surface, terminal=terminal, shared=shared):
                                query, expected, traces, enabled = declaration(harness, kind, shape, flags, surface, shared=shared)
                                assert all(trace["opened"] == 0 for trace in traces)
                                report = consume(harness, name, query, expected, fields(kind), traces, enabled, terminal)
                                envelope = report.envelope
                                parameters = len(fixture(shape)[0])
                                assert envelope.field_int("relational_dynamic_schema_stages", 0) == parameters
                                if parameters:
                                    assert envelope.field_int("native_input_replay_sources") == int(flags[1])
                                    assert envelope.field_int("native_input_replay_prefilled_sources") == int(shared)
                                    assert envelope.field_int("native_input_replay_passes") == (
                                        parameters + 1 if shared else (parameters - 1) * int(flags[1]))
                                elif not shared:
                                    assert envelope.field_int("native_input_replay_sources") == 0
                                    assert envelope.field_int("native_input_subquery_validation_sources") == int(flags[1])
                                return {"traces": traces, "parameters": parameters, "complete_rows": len(expected)}
                            harness.case(name, check)
    writers(harness)
    scalar_values(harness)


def scalar_values(harness):
    for shared in (False, True):
        for kind in ("in", "row-not-in", "exists", "all-ge"):
            for position in ("then", "else", "coalesce", "nullif"):
                name = f"dynamic-subquery-scalar-{position}-{kind}-shared-{shared}"

                def check(name=name, shared=shared, kind=kind, position=position):
                    left_rows, right_rows = fixture("mixed")
                    left, a = frame(harness, left_rows, SCHEMA, width=1)
                    if shared:
                        right, right_rows, traces = left, left_rows, [a]
                    else:
                        right, b = frame(harness, right_rows, SCHEMA, width=2)
                        traces = [a, b]
                    value = predicate(inner(right), kind)
                    expected = oracle(kind, left_rows, right_rows)
                    schema = fields(kind)
                    if position == "then":
                        expression = sl.case_when("TRUE", value, False)
                    elif position == "else":
                        expression = sl.case_when("FALSE", False, value)
                    elif position == "coalesce":
                        expression = sl.ColumnExpression("CAST(NULL AS BOOLEAN)").fill_null(value)
                    else:
                        expression = sl.ColumnExpression("TRUE").null_if(value)
                        expected = [dict(row, answer=None if row["answer"] is True else True)
                                    for row in expected]
                        schema = FIELDS + (("answer", T("bool", True)),)
                    report = consume(harness, name, left.with_column("answer", expression),
                                     expected, schema, traces)
                    assert report.envelope.field_int("relational_dynamic_schema_stages") == 4
                    assert report.envelope.field_int("native_input_replay_sources") == 1
                    assert report.envelope.field_int("native_input_replay_prefilled_sources") == int(shared)
                    return {"traces": traces, "scalar_position": position, "shared": shared}
                harness.case(name, check)


def writers(harness):
    def accepted(name, report):
        harness.envelope(name, report.envelope)
        return report.envelope

    def checked_values(name, actual, expected, _path=None):
        harness.values(name, actual, expected)

    for kind in ("in", "row-not-in", "exists", "all-ge"):
        for shared in (False, True):
            for destination in (*LOCAL_FORMATS, "fanout"):
                name = f"dynamic-subquery-{kind}-write-{destination}-shared-{shared}"

                def write(name=name, kind=kind, destination=destination, shared=shared):
                    query, expected, traces, flags = declaration(harness, kind, shared=shared)
                    harness.json(harness.logs / (name + "-predeclared.json"),
                                 {"sql": query._relation_statement(), "expected": expected})
                    formats = LOCAL_FORMATS if destination == "fanout" else (destination,)
                    targets = {extension: harness.output / f"{name}.{extension}" for extension in formats}
                    report = (query.fanout(targets, **POLICY) if destination == "fanout" else
                              getattr(query, "write_" + destination)(targets[destination], **POLICY))
                    complete_inputs(harness, name, report.envelope, traces, flags)
                    assert report.envelope.field_int("relational_dynamic_schema_stages") == 4
                    if destination == "fanout":
                        assert report.envelope.field_int("native_vortex_result_export_execution_count") == 1
                        assert report.envelope.field_int("native_vortex_result_export_replay_count") == len(formats)
                    for extension, path in targets.items():
                        verify_output(harness.context, path, extension, expected, ["n", "s", "answer"],
                                      name=name + "-" + extension, accepted=accepted, complete=checked_values)
                    return {"artifacts": [harness.artifact(path) for path in targets.values()], "traces": traces}
                harness.case(name, write)
