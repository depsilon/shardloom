# SPDX-License-Identifier: Apache-2.0
"""Public nested pivots, independent complete values, and native schema fidelity."""

from __future__ import annotations

from dataclasses import asdict
import json
import subprocess

from native_dynamic_pivot_cases import consume_pivot_batches, literal, pivot_sql, validate_dynamic_fields
from native_nested_pivot_reference import ORACLES, arrow_schema, fixtures, output_schema, records
from native_report_evidence import has_diagnostic_detail, require_native_pivot_spill
from native_workflow_materialization import verify_materializations
from native_workflow_outputs import LOCAL_FORMATS, write_outputs
from run_clickbench_query_uat import file_sha256


def run(context, output, guard, accepted, complete, sources, identity, fixture_generator, *,
        materializations=("python",), spill_strategy=False):
    from shardloom._result_schema import ResultType as Type, schema_fields
    from shardloom.query import SqlWorkflow, UnsupportedWorkflowOperationReport
    from shardloom.runtime_defaults import DEFAULT_LOCAL_RUNTIME_MEMORY_GB

    output.mkdir(parents=True)
    resources = {"memory_gb": 1, "max_parallelism": 2}
    case_prefix = "nested-pivot-pressure" if spill_strategy else "nested-pivot"
    workspace = output / "pivot-spill"
    if spill_strategy:
        workspace.mkdir()
        resources["spill"] = {"workspace": str(workspace), "quota_bytes": 256 << 20,
                              "buffer_bytes": 1 << 20}
    inputs = fixtures()
    cases, invalid, declaration_denials = [], [], []

    def sql(statement, source):
        return SqlWorkflow(statement, context.client, source_bindings=source._declared_sources())

    def add(name, workflow, rows, schema, stages=1, *, sql_statement=None, large=False):
        if isinstance(workflow, UnsupportedWorkflowOperationReport):
            raise ValueError(f"{name}: nested pivot declaration was rejected")
        variants = [("sql", workflow)] if isinstance(workflow, SqlWorkflow) else [
            ("dataframe", workflow),
            ("sql", sql(sql_statement or workflow._relation_statement(), workflow)),
        ]
        for surface, declared in variants:
            cases.append({"name": f"{case_prefix}-{name}-{surface}", "workflow": declared,
                          "rows": rows, "schema": tuple(schema), "stages": stages, "large": large,
                          "streamable": all(binding.source_format in ("vortex", "memory")
                                            for binding in declared._declared_sources())})

    def reject(name, workflow, reason):
        variants = [("sql", workflow)] if isinstance(workflow, SqlWorkflow) else [
            ("dataframe", workflow), ("sql", sql(workflow._relation_statement(), workflow)),
        ]
        invalid.extend((f"{case_prefix}-invalid-{name}-{surface}", declared, reason)
                       for surface, declared in variants)

    def pivot(frame, aggregate, **options):
        return frame.pivot_table(index="entity", columns="category", values="amount",
                                 aggfunc=aggregate, **options)

    for fixture_name, fixture in inputs.items():
        fixture["raw"] = output / f"pivot-{fixture_name}.data"
        fixture["native"] = output / f"pivot-{fixture_name}.vortex"
        oracle = fixture["oracle"]
        columns = oracle["output_columns"]
        for provider, source in (
            ("native", context.read_vortex(fixture["native"])),
            ("declared-arrow", context.read_arrow_ipc(fixture["raw"])),
        ):
            name = f"{fixture_name}-{provider}"
            prefix = source.sort("position").select("entity", "category", "amount")
            if fixture_name in {"list_index_and_cells", "struct_index_fixed_domains"}:
                source_sql = literal(source.source.uri)
                inner = f"SELECT entity,category,amount FROM {source_sql} WHERE position<outer.position"
                correlated = (f"SELECT position FROM {source_sql} WHERE EXISTS (SELECT 1 FROM "
                              f"({pivot_sql(inner, aggregate='min')}) AS p) ORDER BY position")
                add(f"{name}-correlated", sql(correlated, source),
                    [{"position": position} for position in range(1, len(fixture["rows"]))],
                    (("position", Type("int64", False)),), stages=len(fixture["rows"]))

            def normal(label, aggregate, values, *, frame=None, options=None, out_columns=None):
                frame = prefix if frame is None else frame
                options = options or {}
                names = columns if out_columns is None else out_columns
                add(f"{name}-{label}", pivot(frame, aggregate, **options), records(names, values),
                    output_schema(fixture, aggregate, names),
                    sql_statement=pivot_sql(frame._relation_statement(),
                                            aggregate="first_unique" if aggregate == "first" else aggregate,
                                            **options))

            if fixture_name == "list_index_and_cells":
                for aggregate in ("count", "min", "max"):
                    normal(aggregate, aggregate, oracle[aggregate])
                add(f"{name}-first", sql(pivot_sql(prefix._relation_statement(), aggregate="first"), prefix),
                    records(columns, oracle["first"]), output_schema(fixture, "first"))
                unique = prefix.drop_duplicates(["entity", "category"], keep="first")
                normal("first-alias-unique", "first", oracle["first"], frame=unique)
                add(f"{name}-pivot-unique", unique.pivot(index="entity", columns="category", values="amount"),
                    records(columns, oracle["first"]), output_schema(fixture, "first_unique"))
                # Both public Python spellings deliberately mean first_unique.
                reject(f"{name}-first-alias-conflict", pivot(prefix, "first"), "multiple values")
                reject(f"{name}-pivot-conflict",
                       prefix.pivot(index="entity", columns="category", values="amount"), "multiple values")
                minimum = pivot(prefix, "min")
                add(f"{name}-filter-order-project", minimum.filter("pivot_a IS NOT NULL")
                    .sort("entity", descending=True, nulls="last").select("entity", "pivot_a"),
                    records(["entity", "pivot_a"], [[[2], [4]], [[1], [6]], [[None], [None]], [[], []]]),
                    output_schema(fixture, "min", ["entity", "pivot_a"]))
                add(f"{name}-aggregate", minimum.aggregate(
                    "MIN(pivot_a) AS low", "MAX(pivot_a) AS high", "COUNT(*) AS groups"),
                    [{"low": [], "high": [6], "groups": 5}],
                    (("low", dict(fixture["schema"])["amount"]),
                     ("high", dict(fixture["schema"])["amount"]), ("groups", Type("uint64", False))))
                melted = minimum.melt(id_vars="entity", value_vars=["pivot_a", "pivot_b"],
                                      var_name="category", value_name="amount")
                repeated_columns = ["entity", "pivot_pivot_a", "pivot_pivot_b"]
                add(f"{name}-repeat", melted.pivot(index="entity", columns="category", values="amount"),
                    records(repeated_columns, oracle["min"]),
                    output_schema(fixture, "min", repeated_columns), stages=2)
                add(f"{name}-union-limit", minimum.union_all(minimum).limit(7),
                    records(columns, oracle["min"] + oracle["min"][:2]),
                    output_schema(fixture, "min"), stages=2)
                for dropna in (False, True):
                    normal(f"null-fill-dropna-{dropna}", "min", oracle["min"],
                           options={"fill_value": None, "dropna": dropna})
                for aggregate in ("first", "min", "count"):
                    normal(f"empty-{aggregate}", aggregate, [], frame=prefix.limit(0), out_columns=["entity"])
                for empty in (False, True):
                    selected = prefix.limit(0) if empty else prefix
                    for aggregate in ("sum", "mean"):
                        reject(f"{name}-{aggregate}-empty-{empty}", pivot(selected, aggregate),
                               "numeric value column")
                    reject(f"{name}-fill-empty-{empty}", pivot(selected, "min", fill_value=0),
                           "absent or NULL fill")
                    reject(f"{name}-margins-empty-{empty}", pivot(selected, "min", margins=True),
                           "cannot label a nested index")
                missing = pivot(prefix.limit(0), "min").select("pivot_a")
                reject(f"{name}-empty-undiscovered-field", missing, "pivot_a")
                scalar_index = source.sort("position").select("position AS entity", "category", "amount")
                reject(f"{name}-numeric-index-margins", pivot(scalar_index, "min", margins=True),
                       "UTF8 index")
            elif fixture_name == "list_domains":
                for aggregate in ("sum", "count"):
                    normal(aggregate, aggregate, oracle[aggregate])
                for aggregate, middle in (("mean", 12.5), ("min", 5.0), ("max", 20.0)):
                    normal(aggregate, aggregate, [["a", 10.0, middle, None, None],
                                                   ["b", None, None, 30.0, 40.0]])
                add(f"{name}-first", sql(pivot_sql(prefix._relation_statement(), aggregate="first"), prefix),
                    records(columns, [["a", 10, 20, None, None], ["b", None, None, 30, 40]]),
                    output_schema(fixture, "first"))
                normal("empty", "sum", [], frame=prefix.limit(0), out_columns=["entity"])
            elif fixture_name == "nested_extrema_margins":
                for aggregate in ("min", "max"):
                    normal(aggregate, aggregate, oracle[aggregate],
                           options={"margins": True, "margins_name": "total"})
                    selected = ([["a", [1, None], [], []], ["total", [1, None], [], []]]
                                if aggregate == "min" else
                                [["a", [4], [], [4]], ["total", [4], [], [4]]])
                    normal(f"{aggregate}-limit-before", aggregate, selected, frame=prefix.limit(4),
                           options={"margins": True, "margins_name": "total"})
                    normal(f"{aggregate}-empty", aggregate, [], frame=prefix.limit(0),
                           options={"margins": True, "margins_name": "total"},
                           out_columns=["entity", "pivot_total"])
                normal("count", "count", [["a", 3, 1, 4], ["b", 1, 1, 2],
                                             ["c", 1, 1, 2], ["total", 5, 3, 8]],
                       options={"margins": True, "margins_name": "total"})
                declaration_denials.append((f"{case_prefix}-invalid-{name}-first-margins-dataframe", prefix))
                for aggregate in ("first", "first_unique"):
                    reject(f"{name}-{aggregate}-margins",
                           sql(pivot_sql(prefix._relation_statement(), aggregate=aggregate, margins=True), prefix),
                           "margins require count")
                reject(f"{name}-margin-domain-collision", pivot(prefix, "min", margins=True, margins_name="x"),
                       "duplicate output field name")
            else:
                for aggregate in ("first", "count", "min", "max"):
                    expected = oracle["count" if aggregate == "count" else "first_unique"]
                    normal(aggregate, aggregate, expected)
                add(f"{name}-unique", prefix.pivot(index="entity", columns="category", values="amount"),
                    records(columns, oracle["first_unique"]), output_schema(fixture, "first_unique"))
                for aggregate in ("first", "count", "min", "max"):
                    normal(f"{aggregate}-empty", aggregate, [], frame=prefix.limit(0), out_columns=["entity"])

    # Domain count and name limits apply equally to recursive keys.
    wide = context.range(0, 128).select("ARRAY[0] AS entity", "STRUCT(value) AS category", "value AS amount")
    reject("domain-width", pivot(wide, "sum"), "128 result columns")
    long_name = context.range(0, 1).select("value AS entity", f"ARRAY[{literal('x' * 250)}] AS category",
                                           "value AS amount")
    reject("domain-name", pivot(long_name, "sum"), "field-name boundary")
    collision = context.range(0, 1).select("STRUCT(value) AS pivot_a", "'a' AS category", "value AS amount")
    reject("index-domain-collision", collision.pivot_table(index="pivot_a", columns="category",
                                                           values="amount", aggfunc="sum"), "duplicate output field name")

    # Complete streaming output crosses the existing small-result row boundary.
    count = 65_537
    large_source = context.range(0, count).select("value AS entity", "'a' AS category", "STRUCT(value) AS amount")
    large = pivot(large_source, "first").sort("entity")
    large_rows = [{"entity": value, "pivot_a": {"value": value}} for value in range(count)]
    large_schema = (("entity", Type("int64", False)),
                    ("pivot_a", Type("struct", True, (("value", Type("int64", False)),))))
    # A single large declaration is sufficient for the shared emission boundary;
    # the small complete matrix above checks both frontend spellings separately.
    cases.append({"name": f"{case_prefix}-large-dataframe", "workflow": large, "rows": large_rows,
                  "schema": large_schema, "stages": 1, "large": True, "streamable": True})
    add("large-limited", large.limit(97), large_rows[:97], large_schema)

    oracle_path = output / "nested-pivot-expected.json"
    with oracle_path.open("x") as stream:
        json.dump({
            "sources": {name: {"rows": item["rows"],
                               "schema": [(name, asdict(dtype)) for name, dtype in item["schema"]]}
                        for name, item in inputs.items()},
            "declarations": {case["name"]: {
                "sql": case["workflow"]._relation_statement(), "rows": case["rows"],
                "schema": [(name, asdict(dtype)) for name, dtype in case["schema"]],
                "dynamic_stages": case["stages"], "large": case["large"],
                "incremental_results": spill_strategy and case["streamable"],
            } for case in cases},
            "negative_declarations": {name: {"sql": workflow._relation_statement(), "reason": reason}
                                      for name, workflow, reason in invalid},
            "python_declaration_denials": {name: {"source_sql": frame._relation_statement(),
                                                   "aggregate": "first", "margins": True}
                                           for name, frame in declaration_denials},
            "output_formats": LOCAL_FORMATS,
            "materializations": [] if spill_strategy else materializations,
            "collect_and_writer_resources": resources,
            "materialization_memory_gb": None if spill_strategy else DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
            "spill_strategy": spill_strategy,
        }, stream, ensure_ascii=False, indent=2)
        stream.write("\n")

    def remember(*paths):
        sources.extend((path, file_sha256(path), identity(path)) for path in paths)

    remember(oracle_path, *ORACLES)

    def check_schema(name, report, expected):
        actual = schema_fields(report.envelope.field("result_schema_json"),
                               report.envelope.field("result_schema_format"))
        if actual != tuple(expected):
            raise ValueError(f"{name}: native logical schema differs: {actual!r} != {expected!r}")

    def denied(name, report, destination=None, reason=None):
        envelope = report.envelope
        with (output / f"{name}-denial.json").open("x") as stream:
            json.dump(envelope.raw, stream, indent=2)
            stream.write("\n")
        if (envelope.status not in ("error", "unsupported") or envelope.fallback.attempted
                or envelope.raw.get("certificates") or envelope.raw.get("artifacts")
                or (destination is not None and destination.exists())
                or (reason is not None and not has_diagnostic_detail(envelope, reason))):
            raise ValueError(f"{name}: nested pivot denial published output or lost its diagnostic")
        if spill_strategy and list(workspace.iterdir()):
            raise ValueError(f"{name}: denied nested pivot retained owned spill state")
        complete(name, [], [])

    guard()
    subprocess.run([str(fixture_generator), str(output), "--pivot"], check=True, timeout=30)
    for fixture_name, fixture in inputs.items():
        raw, native = fixture["raw"], fixture["native"]
        remember(raw)
        guard()
        accepted(f"{case_prefix}-{fixture_name}-prepare", context.read_arrow_ipc(raw).prepare(native, check=False))
        remember(native)
        for provider, source in (("native", context.read_vortex(native)),
                                 ("arrow", context.read_arrow_ipc(raw))):
            name = f"{case_prefix}-{fixture_name}-{provider}-source"
            report = source.sort("position").collect(check=False, **resources)
            accepted(name, report)
            check_schema(name, report, fixture["schema"])
            complete(name, list(report.result_rows), fixture["rows"])

    for case in cases:
        name, workflow, rows, schema = (case[key] for key in ("name", "workflow", "rows", "schema"))
        columns = [name for name, _ in schema]
        nested = any(dtype.name in {"list", "fixed_size_list", "struct"} for _, dtype in schema)

        def verified(label, report, reused=None, *, memory_gb=1):
            envelope = accepted(label, report)
            validate_dynamic_fields(label, envelope, stages=case["stages"], reused=reused, memory_gb=memory_gb)
            if spill_strategy:
                require_native_pivot_spill(label, envelope, resources["spill"], workspace,
                                          stages=case["stages"])
            return envelope

        route = workflow.route(bounded=True, check=False, **resources)
        with (output / f"{name}-route.json").open("x") as stream:
            json.dump(route.envelope.raw, stream, indent=2)
            stream.write("\n")
        if (route.route_status != "admitted" or not route.side_effect_free
                or route.fallback_attempted or route.external_engine_invoked):
            raise ValueError(f"{name}: route inspection was not admitted and inert")
        if case["large"]:
            denied(f"{name}-collect", workflow.collect(check=False, **resources),
                   reason="collect exceeds 65,536 rows")
        else:
            for execution, parallelism in enumerate((1, 1, 2)):
                guard()
                label = f"{name}-collect-{execution}"
                report = workflow.collect(check=False, **dict(resources, max_parallelism=parallelism))
                envelope = verified(label, report, reused=execution == 1)
                if envelope.field("result_payload_complete") != "true":
                    raise ValueError(f"{label}: native payload was incomplete")
                check_schema(label, report, schema)
                complete(label, list(report.result_rows), rows)
        destinations = write_outputs(
            context, output, workflow, rows, columns, name=name, guard=guard, accepted=accepted,
            written=verified, complete=complete, execution=resources,
            denied_formats={"orc": "nested"} if nested else None, denied=denied,
        )
        if spill_strategy and case["streamable"]:
            guard()
            label = f"{name}-batches"
            report, actual = consume_pivot_batches(label, workflow, resources, columns,
                                                  output=output, schema=schema)
            verified(label, report)
            complete(label, actual, rows, output / f"{label}.json")
        # These two boundaries preserve the exact native nested DType. Other
        # formats are checked for complete values within their translation scope.
        for extension in ("vortex", "arrow_ipc"):
            destination = (destinations[extension] if extension == "vortex" else
                           output / f"{name}-{extension}-normalized.vortex")
            label = f"{name}-{extension}-typed-reopen"
            report = context.sql(f"SELECT * FROM (SELECT * FROM {literal(destination)}) AS p LIMIT 0").collect(check=False)
            accepted(label, report)
            check_schema(label, report, schema)
        # Convenience conversions retain their independently accepted resident
        # policy. Do not count those calls as execution under this spill policy.
        if not case["large"] and not spill_strategy:
            def converted_report(label, report):
                verified(label, report, memory_gb=DEFAULT_LOCAL_RUNTIME_MEMORY_GB)
                check_schema(label, report, schema)

            converted = verify_materializations(
                context, workflow, rows, columns, name=name, materializations=materializations,
                guard=guard, accepted=converted_report, complete=complete,
            )
            if "arrow" in converted or "arrow_ipc" in converted:
                import pyarrow as arrow

                expected_schema = arrow_schema(schema, arrow)
                for conversion in ("arrow", "arrow_ipc"):
                    if conversion in converted:
                        table = (converted[conversion] if conversion == "arrow" else
                                 arrow.ipc.open_stream(arrow.BufferReader(converted[conversion])).read_all())
                        if table.schema != expected_schema:
                            raise ValueError(f"{name}-{conversion}: complete nested Arrow schema differs")

    for name, frame in declaration_denials:
        guard()
        report = pivot(frame, "first", margins=True)
        if (not isinstance(report, UnsupportedWorkflowOperationReport) or report.runtime_execution
                or report.data_read or report.write_io or report.external_engine_invoked):
            raise ValueError(f"{name}: unsupported Python declaration performed work")
        denied(name, report)

    for name, workflow, reason in invalid:
        guard()
        denied(f"{name}-collect", workflow.collect(check=False, **resources), reason=reason)
        for extension in LOCAL_FORMATS:
            guard()
            label = f"{name}-{extension}"
            destination = output / f"{label}.{extension}"
            denied(label, getattr(workflow, f"write_{extension}")(destination, check=False, **resources),
                   destination, reason)
