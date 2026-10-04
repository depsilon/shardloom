# SPDX-License-Identifier: Apache-2.0
"""Exact typed payload transport through public sources, composition and writers."""

from __future__ import annotations

import csv
import json
import subprocess

from run_clickbench_query_uat import file_sha256, strict_json
from run_native_unary_uat import csv_cell


def run(context, output, guard, accepted, complete, sources, identity, fixture_generator):
    import shardloom as sl
    from shardloom.query import SqlWorkflow

    output.mkdir(parents=True)
    guard()
    subprocess.run([str(fixture_generator), str(output)], check=True, timeout=30)
    resources = {"memory_gb": 1, "max_parallelism": 2}
    fields = ["id", "payload", "amount", "day", "instant"]
    schema = dict(zip(fields, ["int64", "binary", "decimal128(38,6)", "date32", "timestamp_micros"]))

    def literal(value):
        return "'" + str(value).replace("'", "''") + "'"

    def remember(*paths):
        sources.extend((path, file_sha256(path), identity(path)) for path in paths)

    def verified(name, report):
        envelope = accepted(name, report)
        if (envelope.field("public_workflow_memory_gb") != "1"
                or envelope.field("public_workflow_native_vortex_provider_scenario") != "none"
                or int(envelope.field("resident_peak_reserved_buffer_bytes")) > 1 << 30):
            raise ValueError(f"{name}: shared native resource admission differs")
        return envelope

    def equal(name, actual, expected, destination=None):
        if actual != expected:
            first = next((index for index, pair in enumerate(zip(actual, expected)) if pair[0] != pair[1]), None)
            raise ValueError(f"{name}: complete values differ; rows={len(actual)}/{len(expected)}, first={first}")
        complete(name, actual, expected, destination)

    def denied(name, report, destination=None, reason=None):
        envelope = report.envelope
        (output / f"{name}-denial.json").write_text(json.dumps(envelope.raw, indent=2) + "\n")
        if (envelope.status not in ("error", "unsupported") or envelope.fallback.attempted
                or envelope.raw.get("certificates") or envelope.raw.get("artifacts")
                or (destination is not None and destination.exists())
                or (reason is not None and not any(reason in item.get("reason", "")
                                                    for item in envelope.raw.get("diagnostics", [])))):
            raise ValueError(f"{name}: invalid typed request published output or success evidence")
        complete(name, [], [])

    def write_all(family, workflow, expected, columns, *, nested=False, typed_orc=True,
                  json_cells=("payload", "amount")):
        for extension in ("vortex", "parquet", "arrow_ipc", "avro", "json", "jsonl", "csv", "orc"):
            guard()
            name = f"{family}-{extension}"
            destination = output / f"{name}.{extension}"
            report = getattr(workflow, f"write_{extension}")(destination, check=False, **resources)
            if nested and extension in ("csv", "orc"):
                denied(name, report, destination, "nested")
                continue
            if typed_orc and extension == "orc":
                denied(name, report, destination, "ORC does not admit decimal or temporal")
                continue
            verified(name, report)
            if extension == "csv":
                with destination.open(newline="") as stream:
                    reader = csv.DictReader(stream)
                    if reader.fieldnames != columns:
                        raise ValueError(f"{name}: CSV field order differs")
                    actual = list(reader)
                csv_expected = [{key: json.dumps(value) if key in json_cells and value is not None
                                 else csv_cell(value) for key, value in row.items()} for row in expected]
                equal(name, actual, csv_expected, destination)
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
                    empty = accepted(f"{name}-schema", context.sql(
                        f"SELECT * FROM (SELECT * FROM {literal(reopened)}) AS reopened LIMIT 0"
                    ).collect(check=False))
                    if empty.field("output_columns") != ",".join(columns):
                        raise ValueError(f"{name}: reopened field order differs")
                    decoded = output / f"{name}-reopened.jsonl"
                    accepted(f"{name}-read", context.sql(
                        f"SELECT * FROM (SELECT * FROM {literal(reopened)}) AS reopened"
                    ).write_jsonl(decoded, check=False))
                actual = [strict_json(line) for line in decoded.read_text().splitlines()]
            equal(name, actual, expected, destination)

    def exercise(family, frame, expected, columns, **writers):
        for spelling, workflow in [("dataframe", frame), ("sql", SqlWorkflow(
                frame._relation_statement(), context.client, source_bindings=frame._declared_sources()))]:
            name = f"{family}-{spelling}"
            route = workflow.route(bounded=True, check=False, **resources)
            (output / f"{name}-route.json").write_text(json.dumps(route.envelope.raw, indent=2) + "\n")
            if (route.route_status != "admitted" or not route.side_effect_free
                    or route.fallback_attempted or route.external_engine_invoked):
                raise ValueError(f"{name}: route was not admitted and inert")
            for parallelism in (1, 2):
                guard()
                label = f"{name}-collect-{parallelism}"
                report = workflow.collect(check=False, **dict(resources, max_parallelism=parallelism))
                if verified(label, report).field("result_payload_complete") != "true":
                    raise ValueError(f"{label}: collection was incomplete")
                equal(label, list(report.result_rows), expected)
            write_all(name, workflow, expected, columns, **writers)

    original = [
        {"id": 1, "payload": "00ff10", "amount": "decimal128(38,6):1234567", "day": -1, "instant": -1},
        {"id": 2, "payload": "", "amount": "decimal128(38,6):-99999999999999999999999999999999999999",
         "day": 20000, "instant": 1700000000123456},
        {"id": 3, "payload": None, "amount": None, "day": None, "instant": None},
        {"id": 4, "payload": "c3a9", "amount": "decimal128(38,6):0", "day": 0, "instant": 0},
    ]
    oracle = output / "source-expected.json"
    oracle.write_text(json.dumps(original, indent=2) + "\n")
    remember(oracle)
    native = None
    for fixture_name, expected_source in [("typed", original), ("typed-empty", [])]:
        raw, prepared = output / f"{fixture_name}.data", output / f"{fixture_name}.vortex"
        remember(raw)
        guard()
        accepted(f"{fixture_name}-prepare", context.read_arrow_ipc(raw).prepare(prepared, check=False))
        remember(prepared)
        if fixture_name == "typed":
            native = prepared
        for source_name, base in [("native", context.read_vortex(prepared, schema=schema)),
                                  ("declared-arrow", context.read_arrow_ipc(raw))]:
            prefix = base.limit(4).sort("id", descending=True)
            ordered = list(reversed(expected_source))
            family = f"{fixture_name}-{source_name}"
            exercise(family + "-payload", prefix.select(*fields), ordered, fields)
            if not expected_source:
                continue
            exercise(family + "-filtered", prefix.filter(sl.col("id") >= 3), ordered[:2], fields)
            exercise(family + "-union", prefix.limit(2).union_all(prefix.limit(2)), ordered[:2] * 2, fields)
            windows = prefix.window("LAG(payload,1) OVER (ORDER BY id) AS prior")
            exercise(family + "-window", windows,
                     [dict(row, prior=prior) for row, prior in zip(ordered, [None, "", "00ff10", None])],
                     fields + ["prior"], json_cells=("payload", "amount", "prior"))
            right = prefix.filter(sl.col("id") >= 3)
            joined = prefix.select("id").join(right, on="id", how="left").select(
                "f.id AS id", *(f"d.{name} AS {name}" for name in fields[1:]))
            join_rows = [row if row["id"] >= 3 else dict(id=row["id"], **dict.fromkeys(fields[1:]))
                         for row in ordered]
            exercise(family + "-outer-join", joined, join_rows, fields)
            exercise(family + "-membership", prefix.filter(sl.col("id").isin_source(right, "id")),
                     ordered[:2], fields)
            exercise(family + "-binary", prefix.select("id", "payload"),
                     [{"id": row["id"], "payload": row["payload"]} for row in ordered],
                     ["id", "payload"], typed_orc=False)
            exercise(family + "-empty", prefix.limit(0), [], fields)

    def leaf(index):
        return {name: original[index][name] for name in fields[1:]}

    nested_rows = [
        {"id": 1, "details": leaf(0), "records": [leaf(0), None]},
        {"id": 2, "details": None, "records": []},
        {"id": 3, "details": leaf(2), "records": None},
        {"id": 4, "details": leaf(3), "records": [leaf(3)]},
    ]
    nested_oracle = output / "nested-expected.json"
    nested_oracle.write_text(json.dumps(nested_rows, indent=2) + "\n")
    raw, prepared = output / "typed-nested.data", output / "typed-nested.vortex"
    remember(raw, nested_oracle)
    accepted("typed-nested-prepare", context.read_arrow_ipc(raw).prepare(prepared, check=False))
    remember(prepared)
    for source_name, base in [("native", context.read_vortex(prepared)),
                              ("declared-arrow", context.read_arrow_ipc(raw))]:
        prefix = base.limit(4).sort("id", descending=True)
        exercise(f"typed-nested-{source_name}-payload", prefix.select("id", "details", "records"),
                 list(reversed(nested_rows)), ["id", "details", "records"], nested=True)
        expanded = prefix.select("id", "records").explode("records").select("id", "records")
        exercise(f"typed-nested-{source_name}-explode", expanded,
                 [{"id": key, "records": value} for key, value in [(4, leaf(3)), (3, None), (1, leaf(0)), (1, None)]],
                 ["id", "records"], nested=True)
        for name in fields[1:]:
            projected = prefix.select("id", "records").explode(f"records.{name}").select("id", name)
            projected_rows = [{"id": key, name: value} for key, value in [
                (4, leaf(3)[name]), (3, None), (1, leaf(0)[name]), (1, None)]]
            family = f"typed-nested-{source_name}-field-{name}"
            exercise(family, projected, projected_rows, ["id", name], typed_orc=name != "payload")
            exercise(family + "-empty", projected.limit(0), [], ["id", name], typed_orc=name != "payload")

    count = 65_541
    raw, prepared = output / "typed-large.data", output / "typed-large.vortex"
    remember(raw)
    accepted("typed-large-prepare", context.read_arrow_ipc(raw).prepare(prepared, check=False))
    remember(prepared)
    large = context.read_vortex(prepared, schema=schema).limit(count).sort("id").select(*fields)
    expected = [{"id": index, "payload": f"{index % 251:02x}00ff",
                 "amount": f"decimal128(38,6):{(index - 32770) * 1000001}",
                 "day": index - 32770, "instant": (index - 32770) * 1000001} for index in range(count)]
    large_oracle = output / "large-expected.json"
    large_oracle.write_text(json.dumps(expected) + "\n")
    remember(large_oracle)
    for spelling, workflow in [("dataframe", large), ("sql", SqlWorkflow(
            large._relation_statement(), context.client, source_bindings=large._declared_sources()))]:
        family = f"typed-large-{spelling}"
        guard()
        report = workflow.limit(97).collect(check=False, **resources)
        verified(family + "-limited", report)
        equal(family + "-limited", list(report.result_rows), expected[:97])
        denied(family + "-collect", workflow.collect(check=False, **resources), reason="collect exceeds 65,536 rows")
        write_all(family, workflow, expected, fields)

    for name in fields[1:]:
        for operation, statement in [
            ("order", f"SELECT * FROM {literal(native)} ORDER BY {name} LIMIT 0"),
            ("group", f"SELECT {name},COUNT(*) AS n FROM {literal(native)} GROUP BY {name}"),
            ("distinct", f"SELECT {name} FROM {literal(native)} UNION SELECT {name} FROM {literal(native)}"),
            ("cast", f"SELECT CAST({name} AS float64) AS changed FROM (SELECT * FROM {literal(native)} LIMIT 0) AS empty"),
            ("arithmetic", f"SELECT {name}+1 AS changed FROM (SELECT * FROM {literal(native)} LIMIT 0) AS empty"),
            ("predicate", f"SELECT * FROM (SELECT * FROM {literal(native)} LIMIT 0) AS empty WHERE {name} IS NULL"),
        ]:
            guard()
            label = f"typed-denied-{name}-{operation}"
            destination = output / f"{label}.vortex"
            denied(label, context.sql(statement).write_vortex(destination, check=False, **resources), destination)
    guard()
