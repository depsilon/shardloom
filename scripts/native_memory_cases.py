# SPDX-License-Identifier: Apache-2.0
"""Literal memory/source-free oracles through the common public native engine."""

from __future__ import annotations

import json
import os

from native_workflow_outputs import LOCAL_FORMATS, write_outputs
from native_workflow_materialization import verify_materializations
from run_clickbench_query_uat import file_sha256


def run(context, output, guard, accepted, complete, sources, identity, *, materializations=("python",)):
    import shardloom as sl
    from shardloom.query import SqlWorkflow

    output.mkdir(parents=True)
    resources = {"memory_gb": 1, "max_parallelism": 2}
    schema = {"n": "int64", "amount": "float64", "flag": "bool", "label": "utf8"}
    rows = [
        {"n": None, "amount": None, "flag": None, "label": "null"},
        {"n": 9_223_372_036_854_775_807, "amount": 1.25, "flag": True, "label": "λ,;\"\n%=港"},
        {"n": -9_223_372_036_854_775_808, "amount": -2.5, "flag": False, "label": None},
    ]
    simple = [{"n": 1, "label": "one"}, {"n": 3, "label": "three"}, {"n": 2, "label": "two"}]
    simple_schema = {"n": "int64", "label": "utf8"}
    simple_frame = context.from_rows(simple, schema=simple_schema)
    numeric = context.from_rows([
        {"n": 9_007_199_254_740_993, "floating": 9_007_199_254_740_992.0},
        {"n": 9_223_372_036_854_775_807, "floating": 9_223_372_036_854_775_808.0},
        {"n": -9_223_372_036_854_775_808, "floating": -9_223_372_036_854_775_808.0},
        {"n": -1, "floating": -1.25},
        {"n": 0, "floating": -0.0},
        {"n": None, "floating": None},
    ], schema={"n": "int64", "floating": "float64"})
    integer_keys = context.from_rows([{"n": 1}, {"n": 9_007_199_254_740_993},
                                     {"n": 9_223_372_036_854_775_807}], schema={"n": "int64"})
    float_keys = context.from_rows([{"n": 1.0}, {"n": 9_007_199_254_740_992.0},
                                   {"n": 9_223_372_036_854_775_808.0}], schema={"n": "float64"})
    declarations = [
        ("nullable", context.from_rows(rows, schema=schema), rows, list(schema), 0),
        ("empty", context.from_rows([], schema=schema), [], list(schema), 0),
        ("all-null", context.from_rows([dict.fromkeys(schema)], schema=schema),
         [dict.fromkeys(schema)], list(schema), 0),
        ("inferred-null", context.from_rows([{"n": None}]), [{"n": None}], ["n"], 0),
        ("rows-composed", simple_frame.filter(sl.col("n") > 1)
         .with_column("doubled", sl.col("n") * 2).sort("doubled", descending=True)
         .select("label", "doubled"),
         [{"label": "three", "doubled": 6}, {"label": "two", "doubled": 4}], ["label", "doubled"], 0),
        ("rows-projection-stages", simple_frame.rename({"n": "key"}).limit(2)
         .with_column("absent", "lit(null)").drop("label").sort("key", descending=True),
         [{"key": 3, "absent": None}, {"key": 1, "absent": None}], ["key", "absent"], 0),
        ("sql-null-column", context.sql("SELECT 7 AS n").with_column("absent", None),
         [{"n": 7, "absent": None}], ["n", "absent"], 0),
        ("range-composed", context.range(1, 5).filter(sl.col("value") > 1)
         .with_column("n", sl.col("value") * 2).sort("n", descending=True).select("n").limit(2),
         [{"n": 8}, {"n": 6}], ["n"], 0),
        ("source-free", context.sql("SELECT 9223372036854775807 AS n, 'λ,;''%' AS label, NULL AS absent"),
         [{"n": 9_223_372_036_854_775_807, "label": "λ,;'%", "absent": None}], ["n", "label", "absent"], 0),
        ("source-free-empty", context.sql("SELECT 1 AS n WHERE FALSE"), [], ["n"], 0),
        ("values-composed", context.sql(
            "SELECT column_1 * 2 AS n FROM (VALUES (1), (3), (2), (NULL)) AS v "
            "WHERE column_1 > 1 ORDER BY n DESC"), [{"n": 6}, {"n": 4}], ["n"], 0),
        ("empty-reductions", context.from_rows([], schema={"n": "int64"}).aggregate(
            "COUNT(*) AS count", "SUM(n) AS total", "MIN(n) AS minimum", "MAX(n) AS maximum"),
         [{"count": 0, "total": None, "minimum": None, "maximum": None}],
         ["count", "total", "minimum", "maximum"], 0),
        ("numeric-key-comparisons", numeric.select("n", "n = floating AS equal", "n > floating AS greater"),
         [{"n": 9_007_199_254_740_993, "equal": False, "greater": True},
          {"n": 9_223_372_036_854_775_807, "equal": False, "greater": False},
          {"n": -9_223_372_036_854_775_808, "equal": True, "greater": False},
          {"n": -1, "equal": False, "greater": True},
          {"n": 0, "equal": True, "greater": False},
          {"n": None, "equal": None, "greater": None}], ["n", "equal", "greater"], 0),
        ("numeric-key-equi-join", integer_keys.join(float_keys, on="n").select("f.n AS n", "d.n AS floating"),
         [{"n": 1, "floating": 1.0}], ["n", "floating"], 0),
    ]
    oracle = output / "memory-expected.json"
    oracle.write_text(json.dumps({
        "declarations": {name: {"rows": expected, "columns": columns, "file_opens": opens}
                         for name, _, expected, columns, opens in declarations},
        "format_sources": simple,
        "format_transform": [{"n": 3, "label": "three"}, {"n": 2, "label": "two"}],
        "format_projection_stages": [{"key": 3, "absent": None}, {"key": 1, "absent": None}],
        "mixed_join": [{"n": 1, "label": "one", "weight": 10},
                       {"n": 2, "label": "two", "weight": 20}],
    }, indent=2, ensure_ascii=False) + "\n")
    sources.append((oracle, file_sha256(oracle), identity(oracle)))

    def exercise(name, workflow, expected, columns, opens):
        variants = [("sql", workflow)] if isinstance(workflow, SqlWorkflow) else [
            ("dataframe", workflow), ("sql", SqlWorkflow(
                workflow._relation_statement(), context.client, source_bindings=workflow._declared_sources()))]
        for surface, declared in variants:
            label = f"memory-{name}-{surface}"
            guard()
            report = declared.collect(check=False, **resources)
            envelope = accepted(label, report)
            complete(label, list(report.result_rows), expected)
            if report.result_columns != tuple(columns):
                raise ValueError(f"{label}: structured result column order differs")
            for field, value in {
                "output_columns": ",".join(columns), "resident_source_opens": str(opens),
                "source_io_performed": str(opens > 0).lower(), "result_payload_complete": "true",
            }.items():
                if envelope.field(field) != value:
                    raise ValueError(f"{label}: {field}={envelope.field(field)!r}; expected {value!r}")
            write_outputs(context, output, declared, expected, columns, name=label,
                          guard=guard, accepted=accepted, complete=complete, execution=resources)
            verify_materializations(context, declared, expected, columns, name=label,
                                    materializations=materializations, guard=guard,
                                    accepted=accepted, complete=complete)

    for declaration in declarations:
        exercise(*declaration)

    # Optional input boundaries are explicit data normalization, followed by the
    # same declaration/execution/output checks as all other source providers.
    converted = verify_materializations(context, declarations[0][1], rows, list(schema),
                                        name="memory-boundary-input", materializations=materializations,
                                        guard=guard, accepted=accepted, complete=complete)
    for conversion, constructor in [("pandas", context.from_pandas), ("arrow", context.from_arrow_table),
                                    ("arrow_ipc", context.from_arrow_ipc)]:
        if conversion in converted:
            exercise(f"from-{conversion}", constructor(converted[conversion], schema=schema),
                     rows, list(schema), 0)

    # All format inputs carry one identical declaration to the same engine.
    # Writer results are checked first; the transform oracle above is independent.
    destinations = write_outputs(context, output, simple_frame, simple, list(simple_schema),
                                 name="memory-input-fixture", guard=guard, accepted=accepted,
                                 complete=complete, execution=resources)
    for extension in LOCAL_FORMATS:
        path = destinations[extension]
        # Explicit reader declarations must not depend on a filename suffix.
        # read_json uses .jsonl to distinguish its two admitted text encodings.
        if extension != "jsonl":
            declared_path = output / f"declared-{extension}.data"
            os.link(path, declared_path)
            path = declared_path
        sources.append((path, file_sha256(path), identity(path)))
        reader = "read_json" if extension == "jsonl" else f"read_{extension}"
        source = getattr(context, reader)(path, schema=simple_schema)
        workflow = source.filter(sl.col("n") > 1).sort("n", descending=True)
        exercise(f"input-{extension}", workflow,
                 [{"n": 3, "label": "three"}, {"n": 2, "label": "two"}], list(simple_schema), 1)
        projected = source.rename({"n": "key"}).limit(2).with_column("absent", None)
        projected = projected.drop("label").sort("key", descending=True)
        exercise(f"input-{extension}-projection-stages", projected,
                 [{"key": 3, "absent": None}, {"key": 1, "absent": None}], ["key", "absent"], 1)

    raw = output / "memory-join-dimension.jsonl"
    raw.write_text('{"n":1,"weight":10}\n{"n":2,"weight":20}\n')
    sources.append((raw, file_sha256(raw), identity(raw)))
    mixed = simple_frame.join(context.read_json(raw), on="n").select(
        "f.n AS n", "f.label AS label", "d.weight AS weight").sort("n")
    exercise("mixed-file-join", mixed,
             [{"n": 1, "label": "one", "weight": 10}, {"n": 2, "label": "two", "weight": 20}],
             ["n", "label", "weight"], 1)
