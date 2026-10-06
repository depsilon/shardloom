# SPDX-License-Identifier: Apache-2.0
"""Frozen scalar-query values through the common public native workflow."""

from __future__ import annotations

import json
import os

from native_report_evidence import has_diagnostic_detail, require_native_resource_admission
from native_workflow_outputs import LOCAL_FORMATS, write_outputs
from run_clickbench_query_uat import file_sha256


def run(context, output, guard, accepted, complete, sources, identity):
    import shardloom as sl
    from shardloom.query import SqlWorkflow

    output.mkdir(parents=True)
    resources = {"memory_gb": 1, "max_parallelism": 2}
    cases = []

    def add(name, workflow, rows, columns, *, dtype=None, opens=0, json_columns=(), denials=None):
        cases.append((name, workflow, rows, list(columns), dtype, opens, json_columns, denials))

    for name, expression, value, dtype, json_cell, denials in [
        ("integer", "7", 7, "int64", False, None),
        ("null", "CAST(NULL AS int64)", None, "int64", False, None),
        ("unsigned", "CAST('9007199254740993' AS uint64)", 9_007_199_254_740_993, "uint64", False, None),
        ("minimum", "-9223372036854775808", -9_223_372_036_854_775_808, "int64", False, None),
        ("decimal", "CAST('123456789012345678901234.567890' AS decimal128(34,6))",
         "decimal128(34,6):123456789012345678901234567890", "decimal128(34,6)", True,
         {"orc": "ORC does not admit decimal or temporal"}),
        ("text", "'東京'", "東京", "utf8", False, None),
        ("binary", "UNHEX('00ff01')", "00ff01", "binary", True, None),
        ("date", "CAST('1969-12-31' AS date32)", -1, "date32", False,
         {"orc": "ORC does not admit decimal or temporal"}),
        ("timestamp", "CAST('1969-12-31T23:59:58.765433Z' AS timestamp_micros)",
         -1_234_567, "timestamp_micros", False, {"orc": "ORC does not admit decimal or temporal"}),
        ("list", "ARRAY[1,2,NULL]", [1, 2, None], "list<int64>", False, {"orc": "nested"}),
        ("struct", "STRUCT(value)", {"value": 7}, "struct", False, {"orc": "nested"}),
    ]:
        for empty in (False, True):
            inner = context.range(7, 8).select(f"{expression} AS result")
            if empty:
                inner = inner.limit(0)
            workflow = context.range(1, 3).with_column("scalar", sl.scalar_subquery(inner)).select("scalar")
            add(f"{name}-{'empty' if empty else 'one'}", workflow,
                [{"scalar": None if empty else value}] * 2, ["scalar"], dtype=dtype,
                json_columns=("scalar",) if json_cell else (), denials=denials)

    for name, statement, expected in [
        ("source-free", "SELECT (SELECT 9) AS scalar", [{"scalar": 9}]),
        ("duplicates-distinct", "SELECT (SELECT DISTINCT column_1 FROM (VALUES (4),(4)) AS v) AS scalar", [{"scalar": 4}]),
        ("set-limit", "SELECT (SELECT value FROM (SELECT value FROM range(3,4) UNION ALL SELECT value FROM range(1,2)) AS u ORDER BY value LIMIT 1) AS scalar", [{"scalar": 1}]),
        ("correlated-aggregate", "SELECT value,(SELECT MAX(value) AS best FROM range(1,5) WHERE value<=outer.value) AS scalar FROM range(0,4)",
         [{"value": 0, "scalar": None}, {"value": 1, "scalar": 1}, {"value": 2, "scalar": 2}, {"value": 3, "scalar": 3}]),
        ("nested-scope", "SELECT (SELECT (SELECT outer.value AS captured) AS inner_value FROM range(5,6)) AS scalar FROM range(1,3)", [{"scalar": 5}] * 2),
        ("cte", "WITH one AS (SELECT value FROM range(8,9)) SELECT (SELECT value FROM one) AS scalar", [{"scalar": 8}]),
        ("lazy-case", "SELECT CASE WHEN FALSE THEN (SELECT 1/0 AS bad) ELSE (SELECT 8) END AS scalar", [{"scalar": 8}]),
        ("lazy-coalesce", "SELECT COALESCE((SELECT 7),(SELECT value FROM range(1,3))) AS scalar", [{"scalar": 7}]),
        ("lazy-membership", "SELECT CASE WHEN TRUE THEN 7 ELSE (CASE WHEN EXISTS (SELECT 1/0 AS bad FROM range(1,2)) THEN (SELECT 8) ELSE (SELECT 9) END) END AS scalar", [{"scalar": 7}]),
        ("aggregate-argument", "SELECT SUM(value + (SELECT 2)) AS scalar FROM range(1,4)", [{"scalar": 12.0}]),
        ("grouped", "SELECT (SELECT 7) AS scalar,COUNT(*) AS n FROM range(1,4) GROUP BY scalar", [{"scalar": 7, "n": 3}]),
        ("having", "SELECT COUNT(*) AS scalar FROM range(1,4) HAVING COUNT(*)=(SELECT 3)", [{"scalar": 3}]),
        ("framed-argument", "SELECT LAST_VALUE((SELECT outer.value AS selected)) OVER (ORDER BY value ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS scalar FROM range(1,4)", [{"scalar": 3}] * 3),
    ]:
        add(name, context.sql(statement), expected, list(expected[0]))

    outer = context.from_rows([{"value": 2}, {"value": 2}, {"value": 3}, {"value": None}], schema={"value": "int64"})
    correlated = sl.scalar_subquery(context.sql("SELECT outer.value + 10 AS selected"))
    add("repeated-parameters", outer.with_column("scalar", correlated),
        [{"value": 2, "scalar": 12}, {"value": 2, "scalar": 12},
         {"value": 3, "scalar": 13}, {"value": None, "scalar": None}], ["value", "scalar"])
    add("empty-outer", outer.limit(0).with_column("scalar", correlated), [], ["value", "scalar"])
    seven = sl.scalar_subquery(context.sql("SELECT 7"))
    absent = sl.scalar_subquery(context.sql("SELECT 1 AS scalar WHERE FALSE"))
    base = context.range(1, 4)
    add("python-case", base.with_column("scalar", sl.case_when(sl.col("value") == 1, seven, absent)),
        [{"value": 1, "scalar": 7}, {"value": 2, "scalar": None}, {"value": 3, "scalar": None}], ["value", "scalar"])
    add("python-predicate", base.filter(sl.col("value") < sl.scalar_subquery(context.sql("SELECT 3"))),
        [{"value": 1}, {"value": 2}], ["value"])
    add("python-null-predicate", base.filter(absent.is_null()), [{"value": i} for i in (1, 2, 3)], ["value"])
    add("python-count-distinct", base.agg(n=sl.count_distinct(seven)), [{"n": 1}], ["n"])
    add("python-string", base.with_column("scalar", sl.scalar_subquery(context.sql("SELECT '東京'")).left(1)),
        [{"value": i, "scalar": "東"} for i in (1, 2, 3)], ["value", "scalar"])

    options = '{"index":"entity","columns":"category","values":"amount","aggregate":"sum"}'
    dynamic = f"SELECT pivot_a FROM PIVOT((SELECT value AS entity,'a' AS category,value AS amount FROM 'unopened.vortex'), '{options}') AS p"
    invalid = [
        ("many", "SELECT (SELECT value FROM range(1,3)) AS scalar", "scalar subquery cardinality"),
        ("duplicates", "SELECT (SELECT column_1 FROM (VALUES (4),(4)) AS v) AS scalar", "scalar subquery cardinality"),
        ("selected-many", "SELECT COALESCE((SELECT value FROM range(1,1)),(SELECT value FROM range(1,3))) AS scalar", "scalar subquery cardinality"),
        ("selected-error", "SELECT CASE WHEN TRUE THEN (SELECT 1/0 AS bad) ELSE 7 END AS scalar", "division by zero"),
        ("empty-arity", "SELECT (SELECT value,value AS other FROM range(1,2)) AS scalar FROM range(1,1)", "exactly one output column"),
        ("unused-column", "SELECT CASE WHEN TRUE THEN 7 ELSE (SELECT absent FROM range(1,2)) END AS scalar", "absent"),
        ("unused-dynamic", f"SELECT CASE WHEN TRUE THEN 7 ELSE ({dynamic}) END AS scalar", "statically bound output schema"),
        ("empty-dynamic", f"SELECT ({dynamic}) AS scalar FROM range(1,1)", "statically bound output schema"),
        ("late-cardinality", "SELECT (SELECT 7 AS n UNION ALL SELECT 8 AS n FROM range(1,2) WHERE outer.value=2049) AS scalar FROM range(1,2050)", "scalar subquery cardinality"),
    ]
    input_rows = [{"key": 1, "payload": 10}, {"key": 2, "payload": 20}, {"key": 3, "payload": None}]
    input_schema = {"key": "int64", "payload": "int64"}
    format_expected = [{"value": 1, "scalar": 10}, {"value": 2, "scalar": 20},
                       {"value": 3, "scalar": None}, {"value": 4, "scalar": None}]
    oracle = output / "scalar-subqueries-expected.json"
    oracle.write_text(json.dumps({
        "declarations": {name: {"sql": workflow._relation_statement(), "rows": rows, "columns": columns,
                                "surfaces": ["sql"] if isinstance(workflow, SqlWorkflow) else ["dataframe", "sql"],
                                "scalar_dtype": dtype, "source_opens": opens, "writer_denials": denials,
                                "json_columns": list(json_columns)}
                         for name, workflow, rows, columns, dtype, opens, json_columns, denials in cases},
        "negative_declarations": {name: {"sql": sql, "reason": reason} for name, sql, reason in invalid},
        "format_input": input_rows, "format_expected": format_expected,
        "input_formats": list(LOCAL_FORMATS), "output_formats": list(LOCAL_FORMATS),
    }, indent=2, ensure_ascii=False) + "\n")
    sources.append((oracle, file_sha256(oracle), identity(oracle)))

    def verified(name, report):
        envelope = accepted(name, report)
        require_native_resource_admission(name, envelope)
        return envelope

    def denied(name, report, destination=None, reason=None):
        envelope = report.envelope
        with (output / f"{name}-denial.json").open("x") as stream:
            json.dump(envelope.raw, stream, indent=2)
            stream.write("\n")
        if (envelope.status not in ("error", "unsupported") or envelope.fallback.attempted
                or envelope.raw.get("certificates") or envelope.raw.get("artifacts")
                or (destination is not None and destination.exists())
                or (reason is not None and not has_diagnostic_detail(envelope, reason))):
            raise ValueError(f"{name}: scalar denial published output or lost its diagnostic")
        complete(name, [], [])

    def exercise(name, workflow, expected, columns, dtype=None, opens=0, json_columns=(), denials=None):
        variants = [("sql", workflow)] if isinstance(workflow, SqlWorkflow) else [
            ("dataframe", workflow), ("sql", SqlWorkflow(workflow._relation_statement(), context.client,
                                                       source_bindings=workflow._declared_sources()))]
        for surface, declared in variants:
            label = f"scalar-{name}-{surface}"
            for execution in (1, 2):
                guard()
                run_name = f"{label}-collect-{execution}"
                report = declared.collect(check=False, **resources)
                envelope = verified(run_name, report)
                complete(run_name, list(report.result_rows), expected)
                if report.result_columns != tuple(columns):
                    raise ValueError(f"{run_name}: column order differs")
                for field, value in {"resident_source_opens": str(opens),
                                     "source_io_performed": str(opens > 0).lower(),
                                     "result_payload_complete": "true"}.items():
                    if envelope.field(field) != value:
                        raise ValueError(f"{run_name}: {field} differs from its declaration")
                if dtype:
                    actual = dict(report.result_schema)["scalar"]
                    if actual.label != dtype or not actual.nullable:
                        raise ValueError(f"{run_name}: scalar type or zero-row nullability differs")
            write_outputs(context, output, declared, expected, columns, name=label, guard=guard,
                          accepted=accepted, written=verified, complete=complete, execution=resources,
                          csv_json_columns=json_columns, denied_formats=denials, denied=denied)

    for case in cases:
        exercise(*case)
    inputs = write_outputs(context, output, context.from_rows(input_rows, schema=input_schema),
                           input_rows, list(input_schema), name="scalar-input", guard=guard,
                           accepted=accepted, complete=complete, execution=resources)
    for extension in LOCAL_FORMATS:
        path = inputs[extension]
        if extension != "jsonl":
            renamed = output / f"declared-{extension}.data"
            os.link(path, renamed)
            path = renamed
        sources.append((path, file_sha256(path), identity(path)))
        reader = "read_json" if extension == "jsonl" else f"read_{extension}"
        source = getattr(context, reader)(path, schema=input_schema)
        inner = source.rename({"key": "lookup", "payload": "amount"}).filter("lookup=outer.value").select("amount")
        selected = sl.scalar_subquery(inner)
        exercise(f"input-{extension}", context.range(1, 5).with_column("scalar", selected),
                 format_expected, ["value", "scalar"], opens=1)
        # Source ownership must also survive function/predicate/aggregate builders.
        exercise(f"input-{extension}-count", context.range(1, 5).agg(n=sl.count_distinct(selected)),
                 [{"n": 2}], ["n"], opens=1)

    for name, statement, reason in invalid:
        workflow = context.sql(statement)
        for execution in (1, 2):
            guard()
            denied(f"scalar-invalid-{name}-collect-{execution}", workflow.collect(check=False, **resources), reason=reason)
        for extension in LOCAL_FORMATS:
            guard()
            label = f"scalar-invalid-{name}-{extension}"
            destination = output / f"{label}.{extension}"
            before = set(output.iterdir())
            report = getattr(workflow, f"write_{extension}")(destination, check=False, **resources)
            if set(output.iterdir()) != before:
                raise ValueError(f"{label}: failed scalar writer retained a staged or published file")
            denied(label, report, destination, reason)
