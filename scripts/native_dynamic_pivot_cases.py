# SPDX-License-Identifier: Apache-2.0
"""Public pivot composition with frozen values, schema scopes and complete sinks."""

from __future__ import annotations

import csv
import json

from run_clickbench_query_uat import file_sha256, strict_json
from run_native_unary_uat import csv_cell
from native_report_evidence import has_diagnostic_detail, require_native_resource_admission


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def pivot_sql(statement, **options):
    request = {"index": "entity", "columns": "category", "values": "amount", "aggregate": "sum", **options}
    return f"SELECT * FROM PIVOT(({statement}), {literal(json.dumps(request, separators=(',', ':')))}) AS pivoted"


def expected_cases():
    # Frozen independently of any engine invocation. Domain order is a, b, c;
    # key order is p1..p4. Null below means a genuinely absent sparse cell.
    sums = [
        {"entity": "p1", "pivot_a": 2.0, "pivot_b": 8.0, "pivot_c": None},
        {"entity": "p2", "pivot_a": 10.0, "pivot_b": 5.0, "pivot_c": None},
        {"entity": "p3", "pivot_a": None, "pivot_b": 7.0, "pivot_c": 11.0},
        {"entity": "p4", "pivot_a": 3.0, "pivot_b": None, "pivot_c": None},
    ]
    expected = {"sum": sums, "limit-after": sums[:2], "empty": [], "empty-margins": []}
    for aggregate, value in [("mean", 1.5), ("min", -1.0), ("max", 4.0)]:
        expected[aggregate] = [dict(row) for row in sums]
        expected[aggregate][-1]["pivot_a"] = value
    expected["count"] = [
        {"entity": "p1", "pivot_a": 1, "pivot_b": 1, "pivot_c": None},
        {"entity": "p2", "pivot_a": 1, "pivot_b": 1, "pivot_c": None},
        {"entity": "p3", "pivot_a": None, "pivot_b": 1, "pivot_c": 1},
        {"entity": "p4", "pivot_a": 2, "pivot_b": None, "pivot_c": None},
    ]
    expected["unique"] = [dict(row) for row in sums]
    expected["unique"][-1]["pivot_a"] = -1
    expected["margins"] = [
        {"entity": "p1", "pivot_a": 2.0, "pivot_b": 8.0, "pivot_c": 0.0, "pivot_total": 10.0},
        {"entity": "p2", "pivot_a": 10.0, "pivot_b": 5.0, "pivot_c": 0.0, "pivot_total": 15.0},
        {"entity": "p3", "pivot_a": 0.0, "pivot_b": 7.0, "pivot_c": 11.0, "pivot_total": 18.0},
        {"entity": "p4", "pivot_a": 3.0, "pivot_b": 0.0, "pivot_c": 0.0, "pivot_total": 3.0},
        {"entity": "total", "pivot_a": 15.0, "pivot_b": 20.0, "pivot_c": 11.0, "pivot_total": 46.0},
    ]
    expected["filter-order-project"] = [{"id": "p2", "adjusted": 11.0}, {"id": "p4", "adjusted": 4.0}]
    expected["aggregate"] = [{"total": 15.0, "groups": 4}]
    expected["join"] = [{"entity": "p2", "amount": 10.0}, {"entity": "p1", "amount": 2.0}]
    expected["set"] = [{"entity": key} for key in ["p1", "p2", "p3", "p4"]]
    expected["window"] = [{"entity": key, "position": rank} for key, rank in [("p1", 3), ("p2", 1), ("p3", 4), ("p4", 2)]]
    expected["repeat"] = [
        {"entity": row["entity"], "pivot_pivot_a": row["pivot_a"] or 0.0,
         "pivot_pivot_b": row["pivot_b"] or 0.0, "pivot_pivot_c": row["pivot_c"] or 0.0}
        for row in sums
    ]
    expected["limit-before"] = [
        {"entity": "p3", "pivot_a": None, "pivot_c": 11.0},
        {"entity": "p4", "pivot_a": 4.0, "pivot_c": None},
    ]
    expected["correlated"] = [{"position": position} for position in range(1, 8)]
    expected["null-collision"] = [
        {"entity": None, "pivot_value": 0, "pivot_a": 0, "pivot_a_2": 9, "pivot_can_t": 0, "pivot_value_2": 0},
        {"entity": "n1", "pivot_value": 0, "pivot_a": None, "pivot_a_2": 0, "pivot_can_t": 0, "pivot_value_2": 0},
        {"entity": "n2", "pivot_value": 4, "pivot_a": 0, "pivot_a_2": 0, "pivot_can_t": 0, "pivot_value_2": 7},
        {"entity": "n3", "pivot_value": 0, "pivot_a": 0, "pivot_a_2": 0, "pivot_can_t": 12, "pivot_value_2": 0},
    ]
    expected["wide"] = [{"entity": 1, **{f"pivot_d{value:03d}": float(value - 63) for value in range(127)}}]
    expected["large"] = [{"entity": value, "pivot_a": float(value % 19 - 9)} for value in range(65_541)]
    return expected


def validate_dynamic_fields(name, envelope, stages=None, scans=None, reused=None):
    require_native_resource_admission(name, envelope)
    required = {
        "resident_relational_handle_retained": "true",
        "resident_relational_lowering_reused": "false",
        "relational_schema_binding": "during_execution",
    }
    if stages is not None:
        required["relational_dynamic_schema_stages"] = str(stages)
    if scans is not None:
        required["relational_scan_rows_delivered"] = str(scans)
    if reused is not None:
        required["resident_relational_declaration_reused"] = str(reused).lower()
    for key, value in required.items():
        if envelope.field(key) != value:
            raise ValueError(f"{name}: {key} differs: {envelope.field(key)!r} != {value!r}")


def run(context, output, guard, accepted, complete, sources, identity):
    import shardloom as sl
    from shardloom.query import SqlWorkflow

    output.mkdir(parents=True)
    resources = {"memory_gb": 1, "max_parallelism": 2}
    expected = expected_cases()
    expected_path = output / "expected.json"
    expected_path.write_text(json.dumps(expected, ensure_ascii=False, indent=2) + "\n")

    def remember(*paths):
        sources.extend((path, file_sha256(path), identity(path)) for path in paths)

    remember(expected_path)

    def verified(name, report, stages=None, scans=None, reused=None):
        envelope = accepted(name, report)
        validate_dynamic_fields(name, envelope, stages, scans, reused)
        return envelope

    def denied(name, report, destination=None, reason=None):
        envelope = report.envelope
        (output / f"{name}-denial.json").write_text(json.dumps(envelope.raw, indent=2) + "\n")
        if (envelope.status not in ("error", "unsupported") or envelope.fallback.attempted
                or envelope.raw.get("certificates") or envelope.raw.get("artifacts")
                or (destination is not None and destination.exists())
                or (reason is not None and not has_diagnostic_detail(envelope, reason))):
            raise ValueError(f"{name}: invalid dynamic request published output or success evidence")
        complete(name, [], [])

    def equal(name, actual, reference, destination=None):
        if actual != reference:
            mismatch = next((i for i, pair in enumerate(zip(actual, reference)) if pair[0] != pair[1]),
                            min(len(actual), len(reference)))
            raise ValueError(f"{name}: {len(actual)}/{len(reference)} rows, first mismatch "
                             f"{mismatch}: {actual[mismatch:mismatch + 1]!r} != {reference[mismatch:mismatch + 1]!r}")
        complete(name, actual, reference, destination)

    def write_all(family, workflow, reference, columns, stages):
        for extension in ("vortex", "parquet", "arrow_ipc", "avro", "orc", "json", "jsonl", "csv"):
            guard()
            name = f"{family}-{extension}"
            destination = output / f"{name}.{extension}"
            verified(name, getattr(workflow, f"write_{extension}")(destination, check=False, **resources), stages)
            if extension == "csv":
                with destination.open(newline="") as stream:
                    reader = csv.DictReader(stream)
                    if reader.fieldnames != columns:
                        raise ValueError(f"{name}: CSV field order differs")
                    actual = list(reader)
                equal(name, actual, [{key: csv_cell(value) for key, value in row.items()} for row in reference], destination)
                continue
            if extension == "json":
                actual = strict_json(destination.read_text())
            else:
                decoded = destination
                if extension != "jsonl":
                    reopened = destination
                    if extension != "vortex":
                        reopened = output / f"{name}-normalized.vortex"
                        accepted(f"{name}-normalize", getattr(context, f"read_{extension}")(
                            destination).prepare(reopened, check=False))
                    schema = accepted(f"{name}-schema", context.sql(
                        f"SELECT * FROM (SELECT * FROM {literal(reopened)}) AS reopened LIMIT 0"
                    ).collect(check=False))
                    if schema.field("output_columns") != ",".join(columns):
                        raise ValueError(f"{name}: reopened field order differs")
                    decoded = output / f"{name}-reopened.jsonl"
                    accepted(f"{name}-read", context.sql(
                        f"SELECT * FROM (SELECT * FROM {literal(reopened)}) AS reopened"
                    ).write_jsonl(decoded, check=False))
                actual = [strict_json(line) for line in decoded.read_text().splitlines()]
            equal(name, actual, reference, destination)

    raw, native, typed = (output / name for name in ("measurements.jsonl", "measurements.vortex", "measurements.data"))
    original = [("p2", "a", 10), ("p1", "a", 2), ("p2", "b", 5), ("p3", "b", 7),
                ("p1", "b", 8), ("p4", "a", -1), ("p4", "a", 4), ("p3", "c", 11)]
    raw.write_text("".join(json.dumps({"position": i, "who": key, "kind": domain, "reading": value}) + "\n"
                           for i, (key, domain, value) in enumerate(original)))
    with typed.open("x", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["position", "who", "kind", "reading"])
        writer.writerows((i, *row) for i, row in enumerate(original))
    remember(raw, typed)
    guard()
    accepted("dynamic-pivot-prepare", context.read_json(raw).prepare(native, check=False))
    remember(native)

    for source_name, base in [
        ("native", context.read_vortex(native)),
        ("declared-csv", context.read_csv(typed, schema={"position": "int64", "who": "utf8", "kind": "utf8", "reading": "int64"})),
    ]:
        source = literal(base.source.uri)
        input_sql = f"SELECT who AS entity,kind AS category,reading AS amount FROM {source} ORDER BY position"
        prefix = base.sort("position").select("who AS entity", "kind AS category", "reading AS amount")
        frame = prefix.pivot_table(index="entity", columns="category", values="amount", aggfunc="sum")
        statement = pivot_sql(input_sql)
        cases = [("sum", frame, statement, 1)]
        for aggregate in ("mean", "min", "max", "count"):
            cases.append((aggregate,
                          prefix.pivot_table(index="entity", columns="category", values="amount", aggfunc=aggregate),
                          pivot_sql(input_sql, aggregate=aggregate), 1))
        unique = prefix.drop_duplicates(["entity", "category"], keep="first")
        cases.append(("unique", unique.pivot(index="entity", columns="category", values="amount"),
                      pivot_sql(unique._relation_statement(), aggregate="first_unique"), 1))
        margins = {"fill_value": 0.0, "margins": True, "margins_name": "total"}
        cases.append(("margins", prefix.pivot_table(index="entity", columns="category", values="amount", aggfunc="sum", **margins),
                      pivot_sql(input_sql, **margins), 1))
        cases.extend([
            ("filter-order-project", frame.with_column("adjusted", sl.col("pivot_a") + 1.0)
             .filter(sl.col("pivot_a") >= 3.0).sort("pivot_a", descending=True).select("entity AS id", "adjusted"),
             f"SELECT entity AS id,pivot_a + 1.0 AS adjusted FROM ({statement}) AS p WHERE pivot_a >= 3.0 ORDER BY pivot_a DESC", 1),
            ("aggregate", frame.aggregate("SUM(pivot_a) AS total", "COUNT(*) AS groups"),
             f"SELECT SUM(pivot_a) AS total,COUNT(*) AS groups FROM ({statement}) AS p", 1),
            ("set", frame.select("entity").union(base.select("who AS entity")),
             f"SELECT entity FROM ({statement}) AS p UNION SELECT who AS entity FROM {source}", 1),
            ("window", frame.window("ROW_NUMBER() OVER (ORDER BY pivot_a DESC NULLS LAST) AS position").select("entity", "position"),
             f"SELECT entity,ROW_NUMBER() OVER (ORDER BY pivot_a DESC NULLS LAST) AS position FROM ({statement}) AS p", 1),
            ("limit-after", frame.limit(2), f"SELECT * FROM ({statement}) AS p LIMIT 2", 1),
        ])
        lookup = base.filter(sl.col("position") <= 1).select("who AS entity", "position")
        cases.append(("join", frame.join(lookup, on="entity").sort("d.position").select("f.entity AS entity", "f.pivot_a AS amount"),
                      f"SELECT p.entity AS entity,p.pivot_a AS amount FROM ({statement}) AS p JOIN ({lookup._relation_statement()}) AS d ON p.entity = d.entity ORDER BY d.position", 1))
        filled = prefix.pivot_table(index="entity", columns="category", values="amount", aggfunc="sum", fill_value=0.0)
        melted = filled.melt(id_vars="entity", value_vars=["pivot_a", "pivot_b", "pivot_c"], var_name="category", value_name="amount")
        repeated = melted.pivot_table(index="entity", columns="category", values="amount", aggfunc="sum")
        cases.append(("repeat", repeated, repeated._relation_statement(), 2))
        limited = base.sort("position", descending=True).limit(2).select("who AS entity", "kind AS category", "reading AS amount")
        cases.append(("limit-before", limited.pivot_table(index="entity", columns="category", values="amount", aggfunc="sum"),
                      pivot_sql(limited._relation_statement()), 1))
        for label, options in [("empty", {}), ("empty-margins", margins)]:
            empty = prefix.limit(0)
            cases.append((label, empty.pivot_table(index="entity", columns="category", values="amount", aggfunc="sum", **options),
                          pivot_sql(empty._relation_statement(), **options), 1))

        for label, dataframe, sql, stages in cases:
            columns = list(expected[label][0]) if expected[label] else ["entity"] + (["pivot_total"] if label == "empty-margins" else [])
            for spelling, workflow in [("dataframe", dataframe), ("sql", SqlWorkflow(sql, context.client, source_bindings=base._declared_sources()))]:
                family = f"dynamic-pivot-{source_name}-{spelling}-{label}"
                route = workflow.route(bounded=True, check=False, **resources)
                (output / f"{family}-route.json").write_text(json.dumps(route.envelope.raw, indent=2) + "\n")
                if route.route_status != "admitted" or not route.side_effect_free or route.fallback_attempted or route.external_engine_invoked:
                    raise ValueError(f"{family}: inspection was not admitted and inert")
                for execution, parallelism in enumerate((1, 1, 2)):
                    guard()
                    name = f"{family}-collect-{execution}"
                    report = workflow.collect(check=False, **dict(resources, max_parallelism=parallelism))
                    envelope = verified(name, report, stages, 8 if label == "sum" else None, execution == 1)
                    if envelope.field("result_payload_complete") != "true" or envelope.field("output_columns") != ",".join(columns):
                        raise ValueError(f"{name}: incomplete payload or wrong dynamic schema")
                    equal(name, list(report.result_rows), expected[label])
                write_all(family, workflow, expected[label], columns, stages)

        correlated_input = f"SELECT who AS entity,kind AS category,reading AS amount FROM {source} WHERE position < outer.position"
        correlated = f"SELECT position FROM {source} WHERE EXISTS (SELECT 1 FROM ({pivot_sql(correlated_input)}) AS p) ORDER BY position"
        workflow = SqlWorkflow(correlated, context.client, source_bindings=base._declared_sources())
        family = f"dynamic-pivot-{source_name}-correlated"
        report = workflow.collect(check=False, **resources)
        verified(family, report, 8, 72)
        equal(family, list(report.result_rows), expected["correlated"])
        write_all(family, workflow, expected["correlated"], ["position"], 8)

    nullable = output / "o'clock.data"
    with nullable.open("x", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["entity", "category", "amount"])
        writer.writerows([("n1", "A", None), (None, "a", 9), ("n2", None, 4), ("n2", "東京", 7), ("n3", "can't", 12)])
    remember(nullable)
    base = context.read_csv(nullable, schema={"entity": "utf8", "category": "utf8", "amount": "int64"})
    for dropna in (True, False):
        # Existing first-unique preserves explicit null cells; fill applies to
        # absent cells. Both declared dropna settings retain observed domains.
        frame = base.limit(5).pivot(index="entity", columns="category", values="amount", fill_value=0, dropna=dropna)
        for spelling, workflow in [("dataframe", frame), ("sql", SqlWorkflow(frame._relation_statement(), context.client, source_bindings=base._declared_sources()))]:
            family = f"dynamic-pivot-null-collision-{dropna}-{spelling}"
            report = workflow.collect(check=False, **resources)
            verified(family, report, 1, 5)
            equal(family, list(report.result_rows), expected["null-collision"])
            write_all(family, workflow, expected["null-collision"], list(expected["null-collision"][0]), 1)

    # The inclusive 128-field limit is independent of input batch boundaries.
    wide_raw, wide = output / "wide.jsonl", output / "wide.vortex"
    wide_raw.write_text("".join(json.dumps({"entity": 1, "category": f"d{value:03d}", "amount": value - 63}) + "\n"
                                for value in range(128)))
    remember(wide_raw)
    guard()
    accepted("dynamic-pivot-wide-prepare", context.read_json(wide_raw).prepare(wide, check=False))
    remember(wide)
    wide_base = context.read_vortex(wide)
    frame = wide_base.limit(127).pivot_table(index="entity", columns="category", values="amount", aggfunc="sum")
    for spelling, workflow in [("dataframe", frame), ("sql", context.sql(frame._relation_statement()))]:
        family = f"dynamic-pivot-wide-{spelling}"
        report = workflow.collect(check=False, **resources)
        verified(family, report, 1)
        equal(family, list(report.result_rows), expected["wide"])
        write_all(family, workflow, expected["wide"], list(expected["wide"][0]), 1)
    destination = output / "must-not-publish-domain-width.vortex"
    denied("dynamic-pivot-domain-width", wide_base.limit(128).pivot_table(
        index="entity", columns="category", values="amount", aggfunc="sum"
    ).write_vortex(destination, check=False, **resources), destination, "128 result columns")

    # Full sinks must carry all rows beyond the unchanged small-collection limit.
    large_raw, large = output / "large.jsonl", output / "large.vortex"
    with large_raw.open("x") as stream:
        for value in range(65_541):
            stream.write(json.dumps({"n": value, "k": "a", "v": value % 19 - 9}) + "\n")
    remember(large_raw)
    guard()
    accepted("dynamic-pivot-large-prepare", context.read_json(large_raw).prepare(large, check=False))
    remember(large)
    frame = (context.read_vortex(large).select("n AS entity", "k AS category", "v AS amount")
             .pivot_table(index="entity", columns="category", values="amount", aggfunc="sum").sort("entity"))
    for spelling, workflow in [("dataframe", frame), ("sql", context.sql(frame._relation_statement()))]:
        family = f"dynamic-pivot-large-{spelling}"
        guard()
        report = workflow.limit(97).collect(check=False, **resources)
        verified(f"{family}-limited", report, 1, 65_541)
        equal(f"{family}-limited", list(report.result_rows), expected["large"][:97])
        guard()
        denied(f"{family}-collection", workflow.collect(check=False, **resources),
               reason="collect exceeds 65,536 rows")
        write_all(family, workflow, expected["large"], ["entity", "pivot_a"], 1)

    negative_input = f"SELECT who AS entity,kind AS category,reading AS amount FROM {literal(native)} ORDER BY position"
    for label, statement, reason in [
        ("duplicate-cell", pivot_sql(negative_input, aggregate="first_unique"), "multiple values"),
        ("unobserved-column", f"SELECT pivot_missing FROM ({pivot_sql(negative_input)}) AS p", "not present"),
        ("empty-unobserved", f"SELECT pivot_a FROM ({pivot_sql(negative_input + ' LIMIT 0')}) AS p", "not present"),
        ("margin-collision", pivot_sql(negative_input, margins=True, margins_name="a"), "duplicate"),
    ]:
        destination = output / f"must-not-publish-{label}.vortex"
        report = SqlWorkflow(statement, context.client).write_vortex(destination, check=False, **resources)
        denied(f"dynamic-pivot-{label}", report, destination, reason)

    # Inspection remains syntax-only even when neither input nor observed fields exist.
    missing = output / "never-created.vortex"
    inspected = context.sql(pivot_sql(f"SELECT entity,category,amount FROM {literal(missing)}")).route(bounded=True, check=False, **resources)
    if not inspected.side_effect_free or missing.exists() or inspected.fallback_attempted or inspected.external_engine_invoked:
        raise ValueError("dynamic pivot inspection accessed a missing source")
    complete("dynamic-pivot-missing-source-inspection", [], [])
    guard()
