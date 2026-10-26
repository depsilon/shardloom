# SPDX-License-Identifier: Apache-2.0
"""Spill-capable predicates, complete public results and owned failure cleanup."""

from __future__ import annotations

import json
import operator

import shardloom as sl
import native_streaming_dynamic_subquery_cases as dynamic
from shardloom.errors import ShardLoomCommandError
from shardloom.query import LazyFrame

from native_streaming_dynamic_subquery_cases import consume, fields
from native_streaming_subquery_cases import (
    SCHEMA, complete_inputs, declaration, fixture, in_set,
)
from native_streaming_unary_cases import frame
from native_workflow_outputs import LOCAL_FORMATS, verify_output

POLICY = {"memory_bytes": 8 << 20, "max_parallelism": 1}
KINDS = ("in", "row-in", "exists", "any", "all", "hot-in")
ROWS, WIDTH = 1025, 2048


def text(index, width=WIDTH):
    return f"{index:08d}" + "p" * width


def wide_rows(kind):
    left = [dict(n=n, s=s) for n, s in (
        (1, text(0)), (4, text(ROWS)), (None, text(1)),
        (2, None), (0, text(ROWS // 2)))]
    right = [dict(n=None if index % 13 == 0 else 1 if kind == "hot-in" else index % 5,
                  s=None if index % 17 == 0 else text(index // 2)) for index in range(ROWS)]
    return left, right


def wide_expected(kind, left, right, *, negated=False):
    result = []
    for row in left:
        group = right
        correlation = "s" if kind == "exists" else "n" if kind in ("any", "all", "hot-in") else None
        if correlation:
            group = [other for other in right if row[correlation] is not None
                     and other[correlation] is not None and row[correlation] == other[correlation]]
        if kind == "exists":
            answer = bool(group)
        elif kind in ("any", "all"):
            compare = operator.lt if kind == "any" else operator.ge
            values = [None if row["s"] is None or other["s"] is None
                      else compare(row["s"], other["s"]) for other in group]
            decisive = kind == "any"
            answer = decisive if decisive in values else None if None in values else not decisive
        else:
            columns = ("n", "s") if kind == "row-in" else ("s",)
            answer = in_set(tuple(row[column] for column in columns),
                            [tuple(other[column] for column in columns) for other in group])
        result.append(dict(row, answer=not answer if negated and answer is not None else answer))
    return result


def wide_query(left, right, kind, *, surface="dataframe", negated=False):
    correlation = sl.col("n") == sl.outer("n")
    if kind in ("in", "hot-in"):
        expression = sl.col("s").isin_source(right, "s", where=correlation if kind == "hot-in" else None)
    elif kind == "row-in":
        expression = sl.row_in_source(["n", "s"], right, ["n", "s"])
    elif kind == "exists":
        expression = sl.exists_source(right, where=sl.col("s") == sl.outer("s"))
    else:
        expression = getattr(sl.col("s"), kind + "_source")(
            "<" if kind == "any" else ">=", right, "s", where=correlation)
    if negated:
        expression = ~expression
    query = left.with_column("answer", expression)
    if surface == "sql":
        relation = f"({right._relation_statement()}) AS r"
        where = " WHERE s = outer.s" if kind == "exists" else (
            " WHERE n = outer.n" if kind in ("any", "all", "hot-in") else "")
        if kind == "exists":
            expression = f"EXISTS (SELECT * FROM {relation}{where})"
        elif kind in ("any", "all"):
            expression = f"s {'<' if kind == 'any' else '>='} {kind.upper()} (SELECT s FROM {relation}{where})"
        else:
            value, columns = ("(n,s)", "n,s") if kind == "row-in" else ("s", "s")
            expression = f"{value} IN (SELECT {columns} FROM {relation}{where})"
        if negated:
            expression = f"NOT ({expression})"
        query = LazyFrame.from_sql(f"SELECT n,s,{expression} AS answer FROM ({left._relation_statement()}) AS l",
                            query.client, source_bindings=query._declared_sources())
    return query


def proof(envelope, kind):
    parameterized = kind not in ("in", "row-in")
    population = {"in": ROWS, "row-in": ROWS, "exists": 4, "any": 757, "all": 757, "hot-in": 946}[kind]
    assert envelope.field_int("relational_spillable_subquery_stages") == (5 if parameterized else 1)
    assert envelope.field_int("relational_spillable_subquery_input_rows") == population
    assert envelope.field_int("relational_spillable_subquery_indexed_rows") == population
    assert envelope.field_int("relational_spillable_subquery_probe_rows") == 5
    assert envelope.field_int("native_input_replay_sources") == int(parameterized)
    assert envelope.field_int("native_input_replay_spilled_sources") == int(parameterized)
    # Public outer references lower to fresh parameter builds. The EXISTS
    # population is cardinality-only; its disk activity comes from source replay.
    assert (envelope.field_int("relational_spilled_subquery_stages") > 0) == (kind != "exists")
    assert envelope.field_int("relational_spill_runs_written") > 2
    assert envelope.field_int("relational_spill_merge_passes") > 0
    assert envelope.field_int("relational_spillable_subquery_lookup_blocks") > 0
    assert envelope.field_bool("relational_spill_owned_cleanup_completed") is True


def run(harness):
    workspace = harness.output / "subquery-membership-spill"
    workspace.mkdir()
    foreign = workspace / "foreign"
    foreign.write_bytes(b"preserve")
    spill = dict(workspace=str(workspace), quota_bytes=128 << 20, buffer_bytes=1 << 20)
    requested = dict(POLICY, spill=spill)

    def clean():
        assert foreign.read_bytes() == b"preserve" and list(workspace.iterdir()) == [foreign]

    # The existing independent scalar oracle covers every admitted comparison,
    # nullable row equality and both negation spellings under the new strategy.
    kinds = ("in", "not-in", "row-in", "row-not-in", "exists", "not-exists",
             *(f"{q}-{c}" for q in ("any", "all") for c in ("eq", "ne", "lt", "le", "gt", "ge")),
             "parameterized-in", "parameterized-exists", "correlated-exists")
    for kind in kinds:
        for shape in ("mixed", "empty-outer", "empty-inner", "all-null"):
            for surface in ("dataframe", "sql"):
                name = f"subquery-membership-small-{kind}-{shape}-{surface}"

                def small(name=name, kind=kind, shape=shape, surface=surface):
                    query, expected, traces, flags = declaration(harness, kind, shape, (True, True), surface)
                    report = consume(harness, name, query, expected, fields(
                        "exists" if kind in ("exists", "not-exists", "parameterized-exists", "correlated-exists") else kind),
                        traces, flags, requested=requested)
                    stages = len(fixture(shape)[0]) if kind.startswith("parameterized-") or kind == "correlated-exists" else 1
                    assert report.envelope.field_int("relational_spillable_subquery_stages") == stages
                    assert report.envelope.field_int("relational_spilled_subquery_stages") == 0
                    assert report.envelope.field_int("relational_spill_runs_written") == 0
                    clean()
                    return {"complete_rows": len(expected), "predicate_stages": stages}

                harness.case(name, small)

    for kind in dynamic.KINDS:
        for shape in ("mixed", "empty-outer", "empty-inner", "all-null"):
            for surface in ("dataframe", "sql"):
                name = f"subquery-membership-deferred-{kind}-{shape}-{surface}"

                def deferred(name=name, kind=kind, shape=shape, surface=surface):
                    query, expected, traces, flags = dynamic.declaration(
                        harness, kind, shape, (True, True), surface)
                    # Preserve the established 32-MiB pivot/replay contract; its
                    # concurrent inner owners need more than the wide-key grant.
                    report = consume(harness, name, query, expected, fields(kind), traces,
                                     flags, requested=dict(dynamic.POLICY, spill=spill))
                    stages = len(fixture(shape)[0])
                    assert report.envelope.field_int("relational_spillable_subquery_stages") == stages
                    assert report.envelope.field_int("relational_spilled_subquery_stages") == 0
                    assert report.envelope.field_int("relational_dynamic_schema_stages", 0) == stages
                    clean()
                    return {"complete_rows": len(expected), "parameter_builds": stages,
                            "predicate_runs": 0,
                            "other_state_runs": report.envelope.field_int("relational_spill_runs_written")}

                harness.case(name, deferred)

    harness.json(harness.logs / "subquery-membership-wide-predeclared.json", {
        "rows": ROWS, "width": WIDTH, "grant": POLICY["memory_bytes"], "spill": spill,
        "expected": {kind: wide_expected(kind, *wide_rows(kind)) for kind in KINDS},
    })
    for kind in KINDS:
        for surface in ("dataframe", "sql"):
            for terminal in ("collect", "batches"):
                for negated in (False, True):
                    name = f"subquery-membership-wide-{kind}-{surface}-{terminal}-negated-{negated}"

                    def wide(name=name, kind=kind, surface=surface, terminal=terminal, negated=negated):
                        left_rows, right_rows = wide_rows(kind)
                        left, a = frame(harness, left_rows, SCHEMA, width=2)
                        right, b = frame(harness, right_rows, SCHEMA, width=31)
                        expected = wide_expected(kind, left_rows, right_rows, negated=negated)
                        report = consume(harness, name, wide_query(left, right, kind, surface=surface, negated=negated),
                                         expected, fields(kind), [a, b], terminal=terminal, requested=requested)
                        proof(report.envelope, kind)
                        clean()
                        return {"complete_rows": len(expected), "retained_results": True}

                    harness.case(name, wide)
    destinations(harness, requested, clean)
    failures(harness, workspace, spill, clean)


def destinations(harness, requested, clean):
    for kind in KINDS:
        for destination in (*LOCAL_FORMATS, "fanout"):
            name = f"subquery-membership-{kind}-write-{destination}"

            def write(name=name, kind=kind, destination=destination):
                left_rows, right_rows = wide_rows(kind)
                left, a = frame(harness, left_rows, SCHEMA, width=2)
                right, b = frame(harness, right_rows, SCHEMA, width=31)
                query = wide_query(left, right, kind)
                expected = wide_expected(kind, left_rows, right_rows)
                formats = LOCAL_FORMATS if destination == "fanout" else (destination,)
                targets = {extension: harness.output / f"{name}.{extension}" for extension in formats}
                report = (query.fanout(targets, **requested) if destination == "fanout" else
                          getattr(query, "write_" + destination)(targets[destination], **requested))
                complete_inputs(harness, name, report.envelope, [a, b], grant=POLICY["memory_bytes"])
                proof(report.envelope, kind)
                if destination == "fanout":
                    assert report.envelope.field_int("native_vortex_result_export_execution_count") == 1
                    assert report.envelope.field_int("native_vortex_result_export_replay_count") == len(formats)
                for extension, path in targets.items():
                    verify_output(harness.context, path, extension, expected, ["n", "s", "answer"],
                                  name=name + "-" + extension,
                                  accepted=lambda label, result: harness.envelope(label, result.envelope) or result.envelope,
                                  complete=lambda label, actual, wanted, _path=None: harness.values(label, actual, wanted))
                clean()
                return {"artifacts": [harness.artifact(path) for path in targets.values()],
                        "complete_rows_per_output": len(expected)}

            harness.case(name, write)


def failures(harness, workspace, spill, clean):
    for kind in KINDS:
        for failure in ("producer", "corrupt", "quota", "existing"):
            name = f"subquery-membership-{kind}-{failure}"

            def fail(name=name, kind=kind, failure=failure):
                left_rows, right_rows = wide_rows(kind)
                left, a = frame(harness, left_rows, SCHEMA)
                state = dict(opened=0, closed=False, fault=False, saw_run=False)

                def produce():
                    state["opened"] += 1
                    try:
                        for start in range(0, len(right_rows), 31):
                            runs = sorted(workspace.glob("*/run-*.vortex"))
                            state["saw_run"] |= bool(runs)
                            if start >= 775 and not state["fault"] and failure in ("producer", "corrupt"):
                                assert runs
                                state["fault"] = True
                                if failure == "producer":
                                    raise RuntimeError("subquery membership producer sentinel")
                                with runs[0].open("r+b") as stream:
                                    stream.truncate(7)
                            yield right_rows[start:start + 31]
                    finally:
                        state["closed"] = True

                right = harness.context.from_batches(produce, schema=SCHEMA, streaming=True)
                targets = {extension: harness.output / f"{name}.{extension}" for extension in ("vortex", "jsonl")}
                if failure == "existing":
                    for path in targets.values():
                        path.write_bytes(b"preserve existing predicate target")
                before = set(harness.output.iterdir())
                selected = dict(spill, quota_bytes=128 << 10) if failure == "quota" else spill
                try:
                    wide_query(left, right, kind).fanout(targets, spill=selected, **POLICY)
                except (RuntimeError, ShardLoomCommandError) as error:
                    if failure == "producer":
                        assert type(error) is RuntimeError and str(error) == "subquery membership producer sentinel"
                    else:
                        assert isinstance(error, ShardLoomCommandError)
                        harness.envelope(name, error.envelope, success=False)
                        message = json.dumps(error.envelope.raw).lower()
                        assert ("quota" in message if failure == "quota" else
                                "already exists and overwrite is disabled" in message if failure == "existing" else
                                "byte length changed" in message or "prepared source changed" in message)
                else:
                    raise AssertionError("failed predicate published a result")
                assert set(harness.output.iterdir()) == before
                assert a["opened"] == int(failure != "existing" and kind not in ("in", "row-in"))
                for path in targets.values():
                    if failure == "existing":
                        assert path.read_bytes() == b"preserve existing predicate target"
                    else:
                        assert not path.exists()
                assert state["opened"] == int(failure != "existing")
                assert state["closed"] == (failure != "existing")
                assert state["fault"] == (failure in ("producer", "corrupt"))
                clean()
                left, a2 = frame(harness, left_rows, SCHEMA)
                right, b2 = frame(harness, right_rows, SCHEMA, width=31)
                result = wide_query(left, right, kind).collect(check=True, spill=spill, **POLICY)
                harness.values(name + "-restart", result.result_rows, wide_expected(kind, left_rows, right_rows))
                complete_inputs(harness, name + "-restart", result.envelope, [a2, b2], grant=POLICY["memory_bytes"])
                proof(result.envelope, kind)
                clean()
                return {"failure": failure, "trace": state, "fresh_complete_rows": len(left_rows)}

            harness.case(name, fail)
