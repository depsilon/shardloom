# SPDX-License-Identifier: Apache-2.0
"""Complete public analytic frames with independently frozen membership oracles."""

from __future__ import annotations

import json
import subprocess
from decimal import Decimal
from itertools import product

from native_report_evidence import has_diagnostic_detail, require_native_resource_admission
from native_window_frame_reference import fixture_rows, frame_rows
from native_workflow_materialization import verify_materializations
from native_workflow_outputs import LOCAL_FORMATS, write_outputs
from run_clickbench_query_uat import file_sha256


MEASURES = (
    ("count_all", "COUNT(*)"), ("nonnull", "COUNT(value)"),
    ("distinct", "COUNT(DISTINCT value)"), ("total", "SUM(value)"),
    ("mean", "AVG(value)"), ("minimum", "MIN(value)"), ("maximum", "MAX(value)"),
    ("first", "FIRST_VALUE(value)"), ("last", "LAST_VALUE(value)"),
    ("nth", "NTH_VALUE(value,2)"),
)
COLUMNS = ["id", *(name for name, _ in MEASURES)]
SCHEMA = {"id": "int64", "cohort": "utf8", "priority": "int64", "value": "int64"}


def declarations():
    """Freeze SQL declarations and values before invoking the native engine."""
    for unit, exclusion, descending, nulls_first in product(
        ("ROWS", "GROUPS", "RANGE"), ("NO OTHERS", "CURRENT ROW", "GROUP", "TIES"),
        (False, True), (False, True),
    ):
        direction, nulls = ("DESC" if descending else "ASC"), ("FIRST" if nulls_first else "LAST")
        name = f"{unit}-{exclusion}-{direction}-{nulls}".lower().replace(" ", "-")
        clause = (f"PARTITION BY cohort ORDER BY priority {direction} NULLS {nulls} "
                  f"{unit} BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE {exclusion}")
        expressions = [f"{function} OVER ({clause}) AS {name}" for name, function in MEASURES]
        yield name, expressions, frame_rows(unit, exclusion, descending, nulls_first)


def run(context, output, guard, accepted, complete, sources, identity, *,
        materializations=("python",), nested_fixture_generator):
    from shardloom.query import SqlWorkflow

    output.mkdir(parents=True)
    resources = {"memory_gb": 1, "max_parallelism": 2}
    matrix = list(declarations())
    source_rows = fixture_rows()
    source = context.from_rows(source_rows, schema=SCHEMA)
    cases = []

    def add(name, workflow, expected, columns, *, opens=0, typed_columns=(), conversions=True, nested=False):
        cases.append((name, workflow, expected, list(columns), opens, typed_columns, conversions, nested))

    for name, expressions, expected in matrix:
        workflow = source.window(*expressions).select(*COLUMNS)
        add(name, workflow, expected, COLUMNS)
    add("empty", source.limit(0).window(*matrix[0][1]).select(*COLUMNS), [], COLUMNS)

    # Default peer frames and absence of ORDER BY are separate declarations from
    # the bounded matrix. The literal oracle also tests NULL-respecting selection.
    default = context.sql(
        "SELECT column_1 AS id,COUNT(*) OVER (ORDER BY column_1) AS n,"
        "SUM(column_2) OVER (ORDER BY column_1) AS total,"
        "COUNT(DISTINCT column_2) OVER () AS unique_values,"
        "FIRST_VALUE(column_2) OVER () AS first,NTH_VALUE(column_2,2) OVER () AS second "
        "FROM (VALUES (2,20),(1,NULL),(1,10),(3,20)) AS v")
    add("default", default, [
        {"id": key, "n": count, "total": total, "unique_values": 2, "first": 20, "second": None}
        for key, count, total in [(2, 3, 30.0), (1, 2, 10.0), (1, 2, 10.0), (3, 4, 50.0)]
    ], ["id", "n", "total", "unique_values", "first", "second"])
    computed = context.range(1, 5).window(
        "SUM(value*2+1) OVER (ORDER BY value ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING "
        "EXCLUDE CURRENT ROW) AS total",
        "COUNT(NULLIF(value,2)) OVER (ORDER BY value ROWS UNBOUNDED PRECEDING) AS n",
        "FIRST_VALUE(CASE WHEN value=2 THEN NULL ELSE value*10 END) OVER "
        "(ORDER BY value ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS chosen")
    add("computed", computed, [
        {"value": value, "total": total, "n": count, "chosen": chosen}
        for value, total, count, chosen in [(1, 5.0, 1, 10), (2, 10.0, 1, None),
                                            (3, 14.0, 2, 30), (4, 7.0, 3, 40)]
    ], ["value", "total", "n", "chosen"])
    add("composed", computed.filter("value>1").aggregate("SUM(total) AS total"),
        [{"total": 31.0}], ["total"])
    cancellation = context.from_rows(
        [{"id": index, "value": value} for index, value in enumerate([1e300, 1.0, 0.0])],
        schema={"id": "int64", "value": "float64"},
    ).window(
        "SUM(value) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS total",
        "AVG(value) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS mean",
    ).filter("id=2").select("total", "mean")
    add("floating-removal", cancellation, [{"total": 1.0, "mean": 0.5}], ["total", "mean"])
    add("empty-bounds", context.range(1, 4).window(
        "COUNT(*) OVER (ORDER BY value ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) AS n",
        "SUM(value) OVER (ORDER BY value ROWS BETWEEN 18446744073709551615 FOLLOWING "
        "AND UNBOUNDED FOLLOWING) AS absent"),
        [{"value": value, "n": 0, "absent": None} for value in range(1, 4)],
        ["value", "n", "absent"])

    for name, dtype, values, offset, expected in [
        ("date", "date32", ["2025-01-01", "2025-01-02", "2025-01-04"],
         "INTERVAL '1' DAY", [1, 2, 1]),
        ("timestamp", "timestamp_micros", ["2025-01-01T00:00:00Z", "2025-01-01T00:00:01Z",
                                          "2025-01-01T00:00:03Z"],
         "INTERVAL '1500' MILLISECOND", [1, 2, 1]),
        ("decimal", "decimal128(6,3)", ["1.000", "1.099", "1.101"],
         "CAST('0.10' AS decimal128(4,2))", [1, 2, 2]),
    ]:
        values = context.from_rows([{"text": value} for value in values])
        typed = values.select(f"CAST(text AS {dtype}) AS key").window(
            f"COUNT(*) OVER (ORDER BY key RANGE BETWEEN {offset} PRECEDING AND CURRENT ROW) AS n"
        ).select("n")
        add(f"range-{name}", typed, [{"n": value} for value in expected], ["n"])

    decimal = context.from_rows([{"text": value} for value in ["1.00", "2.00", "3.00"]])
    decimal = decimal.select("CAST(text AS decimal128(6,2)) AS value").window(
        "SUM(value) OVER (ORDER BY value ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS total",
        "AVG(value) OVER () AS mean", "FIRST_VALUE(value) OVER () AS first").select("total", "mean", "first")
    add("decimal-values", decimal, [
        {"total": f"decimal128(38,2):{value}", "mean": "decimal128(38,6):2000000",
         "first": "decimal128(6,2):100"} for value in [100, 300, 500]
    ], ["total", "mean", "first"], typed_columns=("total", "mean", "first"))

    # A framed writer must emit every row across the small-result collection
    # boundary. This independent series is exact in the admitted F64 domain.
    large_count = 65_541
    large = context.range(0, large_count).window(
        "SUM(value) OVER (ORDER BY value ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS total")
    large_expected = [{"value": value, "total": float(0 if value == 0 else 2 * value - 1)}
                      for value in range(large_count)]
    add("large", large, large_expected, ["value", "total"], conversions=False)

    # Reuse the existing typed fixture builder; the literal expected values are
    # independent of both its output bytes and the engine's selected arrays.
    nested_input = output / "nested-input"
    nested_raw, nested_native = nested_input / "nested.data", nested_input / "nested.vortex"
    nested_values = {
        "items": [[[9, None], [], None], [], None, [[-4, None], []]],
        "records": [[{"code": [9, None], "label": "a'b"}, None], [], None,
                    [{"code": [-4], "label": "東京"}]],
        "detail": [{"tag": "a'b", "enabled": True}, {"tag": "", "enabled": None},
                   {"tag": None, "enabled": False}, None],
    }
    for source_name, nested_source in [
        ("native", context.read_vortex(nested_native)),
        ("arrow", context.read_arrow_ipc(nested_raw)),
    ]:
        for field, values in nested_values.items():
            framed = nested_source.window(
                f"FIRST_VALUE({field}) OVER () AS first", f"LAST_VALUE({field}) OVER () AS last",
                f"NTH_VALUE({field},2) OVER () AS second", f"MIN({field}) OVER () AS minimum",
                f"MAX({field}) OVER () AS maximum", f"COUNT(DISTINCT {field}) OVER () AS distinct_values",
            ).select("id", "first", "last", "second", "minimum", "maximum", "distinct_values")
            minimum = values[2] if field == "detail" else values[1]
            expected = [{"id": index + 1, "first": values[0], "last": values[3], "second": values[1],
                         "minimum": minimum, "maximum": values[0], "distinct_values": 3}
                        for index in range(4)]
            add(f"nested-{source_name}-{field}", framed, expected,
                ["id", "first", "last", "second", "minimum", "maximum", "distinct_values"],
                opens=1, nested=True)

    invalid = [
        (name, context.range(0, 0).window(expression).select("n"), reason)
        for name, expression, reason in [
            ("negative-offset", "COUNT(*) OVER (ROWS -1 PRECEDING) AS n", "nonnegative integer"),
            ("missing-group-order", "COUNT(*) OVER (GROUPS CURRENT ROW) AS n", "GROUPS frames require"),
            ("missing-range-order", "COUNT(*) OVER (RANGE 1 PRECEDING) AS n", "exactly one ORDER BY"),
            ("zero-nth", "NTH_VALUE(value,0) OVER () AS n", "positive"),
        ]
    ]
    oracle = output / "analytic-frames-expected.json"
    oracle.write_text(json.dumps({
        "input": source_rows,
        "declarations": {
            name: {"sql": workflow._relation_statement(), "rows": rows, "columns": columns,
                   "surfaces": ["sql"] if isinstance(workflow, SqlWorkflow) else ["dataframe", "sql"],
                   "source_opens": opens, "typed_columns": list(typed_columns),
                   "conversions": conversions, "nested": nested}
            for name, workflow, rows, columns, opens, typed_columns, conversions, nested in cases
        },
        "negative_declarations": {
            name: {"sql": workflow._relation_statement(), "reason": reason}
            for name, workflow, reason in invalid
        },
        "format_matrix_case": matrix[0][0],
        "input_formats": list(LOCAL_FORMATS), "output_formats": list(LOCAL_FORMATS),
        "materializations": list(materializations),
    }, indent=2, ensure_ascii=False) + "\n")
    sources.append((oracle, file_sha256(oracle), identity(oracle)))

    guard()
    nested_input.mkdir()
    subprocess.run([str(nested_fixture_generator), str(nested_input)], check=True, timeout=30)
    sources.append((nested_raw, file_sha256(nested_raw), identity(nested_raw)))
    accepted("frames-nested-prepare", context.read_arrow_ipc(nested_raw).prepare(nested_native, check=False))
    sources.append((nested_native, file_sha256(nested_native), identity(nested_native)))

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
            raise ValueError(f"{name}: denied frame published output or lost its diagnostic")
        complete(name, [], [])

    def exercise(name, workflow, expected, columns, opens=0, typed_columns=(), conversions=True, nested=False):
        variants = [("sql", workflow)] if isinstance(workflow, SqlWorkflow) else [
            ("dataframe", workflow), ("sql", SqlWorkflow(workflow._relation_statement(), context.client,
                                                        source_bindings=workflow._declared_sources()))]
        for surface, declared in variants:
            label = f"frames-{name}-{surface}"
            for run in (1, 2):
                guard()
                collect_name = f"{label}-collect-{run}"
                if len(expected) > 65_536:
                    denied(collect_name, declared.collect(check=False, **resources),
                           reason="collect exceeds 65,536 rows")
                    report = declared.limit(97).collect(check=False, **resources)
                    verified(collect_name + "-limited", report)
                    complete(collect_name + "-limited", list(report.result_rows), expected[:97])
                    continue
                report = declared.collect(check=False, **resources)
                envelope = verified(collect_name, report)
                complete(collect_name, list(report.result_rows), expected)
                if report.result_columns != tuple(columns):
                    raise ValueError(f"{collect_name}: result column order differs")
                for field, value in {
                    "resident_source_opens": str(opens), "source_io_performed": str(opens > 0).lower(),
                    "result_payload_complete": "true",
                }.items():
                    if envelope.field(field) != value:
                        raise ValueError(f"{collect_name}: {field} differs from the declared source")
                if columns == COLUMNS:
                    expected_types = ["int64", "uint64", "uint64", "uint64", "float64", "float64",
                                      "int64", "int64", "int64", "int64", "int64"]
                    if [dtype.label for _, dtype in report.result_schema] != expected_types:
                        raise ValueError(f"{collect_name}: frame result types differ")
            write_outputs(
                context, output, declared, expected, columns, name=label, guard=guard, accepted=accepted,
                written=verified, complete=complete, execution=resources, csv_json_columns=typed_columns,
                denied_formats=({"orc": "nested"} if nested else
                                {"orc": "ORC does not admit decimal or temporal"} if typed_columns else None),
                denied=denied,
            )
            if conversions:
                materialized = expected
                if typed_columns:
                    def decimal(value):
                        if value is None:
                            return None
                        dtype, coefficient = value.split(":", 1)
                        scale = int(dtype.removesuffix(")").rsplit(",", 1)[1])
                        return Decimal(coefficient).scaleb(-scale)
                    materialized = [
                        {key: decimal(value) if key in typed_columns else value for key, value in row.items()}
                        for row in expected
                    ]
                verify_materializations(context, declared, materialized, columns, name=label,
                                        materializations=materializations, guard=guard,
                                        accepted=accepted, complete=complete)

    for case in cases:
        exercise(*case)

    # Each input format is verified before the same framed transformation runs.
    # Explicit schemas retain NULL and integer domains through text boundaries.
    inputs = write_outputs(context, output, source, source_rows, list(SCHEMA), name="frames-input",
                           guard=guard, accepted=accepted, complete=complete, execution=resources)
    for extension in LOCAL_FORMATS:
        path = inputs[extension]
        sources.append((path, file_sha256(path), identity(path)))
        reader = "read_json" if extension == "jsonl" else f"read_{extension}"
        declared = getattr(context, reader)(path, schema=SCHEMA)
        exercise(f"input-{extension}", declared.window(*matrix[0][1]).select(*COLUMNS),
                 matrix[0][2], COLUMNS, opens=1)

    for name, workflow, reason in invalid:
        guard()
        denied(f"frames-invalid-{name}", workflow.collect(check=False, **resources), reason=reason)
        for extension in LOCAL_FORMATS:
            guard()
            label = f"frames-invalid-{name}-{extension}"
            destination = output / f"{label}.{extension}"
            denied(label, getattr(workflow, f"write_{extension}")(destination, check=False, **resources),
                   destination, reason)
