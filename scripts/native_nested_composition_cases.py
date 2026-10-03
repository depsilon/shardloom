# SPDX-License-Identifier: Apache-2.0
"""Public nested payload/explode composition, complete writers and grant checks.

Fixed-size-list native dtype preservation is covered by the typed Rust fixtures;
these public fixtures carry typed lists/structs through a declared Arrow adapter.
"""

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
            raise ValueError(f"{name}: invalid nested request published output or success evidence")
        complete(name, [], [])

    def write_all(family, workflow, expected, columns, nested, denials=None):
        formats = ("vortex", "parquet", "arrow_ipc", "avro", "json", "jsonl")
        for extension in (*formats, "csv", "orc"):
            guard()
            name = f"{family}-{extension}"
            destination = output / f"{name}.{extension}"
            report = getattr(workflow, f"write_{extension}")(destination, check=False, **resources)
            if denials and extension in denials:
                denied(name, report, destination, denials[extension])
                continue
            if nested and extension in ("csv", "orc"):
                denied(name, report, destination, "nested")
                continue
            verified(name, report)
            if extension == "csv":
                with destination.open(newline="") as stream:
                    reader = csv.DictReader(stream)
                    if reader.fieldnames != columns:
                        raise ValueError(f"{name}: CSV field order differs")
                    actual = list(reader)
                equal(name, actual, [{key: csv_cell(value) for key, value in row.items()} for row in expected], destination)
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
                    # Keep reopening on the complete shared native relation too.
                    accepted(f"{name}-read", context.sql(
                        f"SELECT * FROM (SELECT * FROM {literal(reopened)}) AS reopened"
                    ).write_jsonl(decoded, check=False))
                actual = [strict_json(line) for line in decoded.read_text().splitlines()]
            equal(name, actual, expected, destination)

    schema = {"id": "int64", "items": "list<list<int64>>",
              "records": "list<struct<code:list<int64>,label:utf8>>",
              "detail": "struct<tag:utf8,enabled:boolean>"}
    original = [
        {"id": 1, "items": [[9, None], [], None],
         "records": [{"code": [9, None], "label": "a'b"}, None],
         "detail": {"tag": "a'b", "enabled": True}},
        {"id": 2, "items": [], "records": [], "detail": {"tag": "", "enabled": None}},
        {"id": 3, "items": None, "records": None, "detail": {"tag": None, "enabled": False}},
        {"id": 4, "items": [[-4, None], []],
         "records": [{"code": [-4], "label": "東京"}], "detail": None},
    ]
    typed, native = output / "nested.data", output / "nested.vortex"
    oracle = output / "source-expected.json"
    oracle.write_text(json.dumps(original, ensure_ascii=False, indent=2) + "\n")
    remember(typed, oracle)
    guard()
    accepted("nested-prepare", context.read_arrow_ipc(typed).prepare(native, check=False))
    remember(native)
    ordered = list(reversed(original))
    flattened = [{"id": key, "items": value} for key, value in [(4, -4), (4, None), (3, None), (1, 9), (1, None), (1, None)]]
    field_values = [{"id": key, "code": value} for key, value in [(4, -4), (3, None), (1, 9), (1, None), (1, None)]]

    for source_name, base in [("native", context.read_vortex(native, schema=schema)),
                              ("declared-arrow", context.read_arrow_ipc(typed))]:
        prefix = base.sort("id", descending=True).limit(4)
        payload = prefix.select("id AS key", "items AS groups", "detail")
        payload_rows = [{"key": row["id"], "groups": row["items"], "detail": row["detail"]} for row in ordered]
        selected = prefix.select("id", "items")
        selected_rows = [{"id": row["id"], "items": row["items"]} for row in ordered]
        once = selected.explode("items")
        once_rows = [{"id": key, "items": value} for key, value in
                     [(4, [-4, None]), (4, []), (3, None), (1, [9, None]), (1, []), (1, None)]]
        windows = selected.window("LAG(items,1) OVER (ORDER BY id) AS prior")
        window_rows = [dict(row, prior=prior) for row, prior in zip(selected_rows,
                       [None, [], [[9, None], [], None], None])]
        right = prefix.filter(sl.col("id") >= 3).select("id", "records")
        joined = selected.join(right, on="id", how="left").select(
            "f.id AS id", "f.items AS items", "d.records AS records")
        join_rows = [dict(row, records=original[row["id"] - 1]["records"] if row["id"] >= 3 else None)
                     for row in selected_rows]
        membership = selected.filter(sl.col("id").isin_source(right, "id"))
        cases = [
            ("payload", payload, payload_rows, ["key", "groups", "detail"], True),
            ("filtered", payload.filter(sl.col("key") >= 3), payload_rows[:2], ["key", "groups", "detail"], True),
            ("union", selected.limit(2).union_all(selected.limit(2)), selected_rows[:2] * 2, ["id", "items"], True),
            ("window", windows, window_rows, ["id", "items", "prior"], True),
            ("join", joined, join_rows, ["id", "items", "records"], True),
            ("membership", membership, selected_rows[:2], ["id", "items"], True),
            ("explode-once", once, once_rows, ["id", "items"], True),
            ("explode-twice", once.explode("items").select("id", "items"), flattened, ["id", "items"], False),
            ("explode-field", prefix.select("id", "records").explode("records.code").explode("code").select("id", "code"), field_values, ["id", "code"], False),
            ("flattened-tail", once.explode("items").select("id", "items").tail(2), flattened[-2:], ["id", "items"], False),
            ("empty", payload.limit(0), [], ["key", "groups", "detail"], True),
        ]
        for label, frame, expected, columns, nested in cases:
            statement = frame._relation_statement()
            for spelling, workflow in [("dataframe", frame), ("sql", SqlWorkflow(statement, context.client, source_bindings=frame._declared_sources()))]:
                family = f"nested-{source_name}-{spelling}-{label}"
                route = workflow.route(bounded=True, check=False, **resources)
                (output / f"{family}-route.json").write_text(json.dumps(route.envelope.raw, indent=2) + "\n")
                if (route.route_status != "admitted" or not route.side_effect_free
                        or route.fallback_attempted or route.external_engine_invoked):
                    raise ValueError(f"{family}: route was not admitted and inert")
                for parallelism in (1, 2):
                    guard()
                    name = f"{family}-collect-{parallelism}"
                    report = workflow.collect(check=False, **dict(resources, max_parallelism=parallelism))
                    if verified(name, report).field("result_payload_complete") != "true":
                        raise ValueError(f"{name}: collection was incomplete")
                    equal(name, list(report.result_rows), expected)
                write_all(family, workflow, expected, columns, nested)

    # One source feeds a nested payload and two expansions above small-collect's
    # row limit. Full results are independently specified before execution.
    count = 65_541
    raw, large = output / "large.data", output / "large.vortex"
    remember(raw)
    guard()
    accepted("nested-large-prepare", context.read_arrow_ipc(raw).prepare(large, check=False))
    remember(large)
    base = context.read_vortex(large, schema={"id": "uint64", "items": "list<list<int64>>"})
    payload = base.limit(count).sort("id").select("id", "items")
    flat = payload.explode("items").explode("items").select("id", "items")
    payload_rows = [{"id": index, "items": [[index], [None]]} for index in range(count)]
    flat_rows = [{"id": index, "items": value} for index in range(count) for value in (index, None)]
    for label, frame, expected, nested in [("payload", payload, payload_rows, True), ("expanded", flat, flat_rows, False)]:
        for spelling, workflow in [("dataframe", frame), ("sql", SqlWorkflow(frame._relation_statement(), context.client, source_bindings=frame._declared_sources()))]:
            family = f"nested-large-{label}-{spelling}"
            guard()
            report = workflow.limit(97).collect(check=False, **resources)
            verified(f"{family}-limited", report)
            equal(f"{family}-limited", list(report.result_rows), expected[:97])
            denied(f"{family}-collect", workflow.collect(check=False, **resources), reason="collect exceeds 65,536 rows")
            write_all(family, workflow, expected, ["id", "items"], nested)

    # Avro's signed long cannot represent the whole native uint64 domain.
    # Verify the value boundary inside a list and cleanup after writer failure.
    unsigned = output / "unsigned.data"
    unsigned_rows = [{"id": 1, "items": [0, (1 << 63) - 1, 1 << 63, (1 << 64) - 1, None]}]
    remember(unsigned)
    unsigned_workflow = (context.read_arrow_ipc(unsigned)
                         .limit(1).select("id", "items"))
    unsigned_report = unsigned_workflow.collect(check=False, **resources)
    verified("nested-uint64-collect", unsigned_report)
    equal("nested-uint64-collect", list(unsigned_report.result_rows), unsigned_rows)
    write_all("nested-uint64", unsigned_workflow, unsigned_rows, ["id", "items"], True,
              denials={"avro": "exceeds i64::MAX"})

    # CSV has no native list type and the declared source-schema grammar currently
    # accepts scalar hints. It must not silently ignore a requested nested hint.
    unsupported_csv = output / "nested-hint.csv"
    unsupported_csv.write_text('id,items\n1,"[9,null]"\n')
    remember(unsupported_csv)
    destination = output / "must-not-publish-nested-csv-hint.vortex"
    denied("nested-invalid-csv-hint", context.read_csv(
        unsupported_csv, schema={"id": "int64", "items": "list<int64>"}
    ).prepare(destination, check=False), destination)

    labels, prepared_labels = output / "labels.csv", output / "labels.vortex"
    labels.write_text("label\n001\n0009\n")
    remember(labels)
    accepted("nested-scalar-hint-prepare", context.read_csv(
        labels, schema={"label": "utf8"}
    ).prepare(prepared_labels, check=False))
    remember(prepared_labels)
    report = context.sql(f"SELECT * FROM (SELECT * FROM {literal(prepared_labels)}) AS typed").collect(
        check=False, **resources)
    verified("nested-scalar-hint-collect", report)
    equal("nested-scalar-hint-collect", list(report.result_rows), [{"label": "001"}, {"label": "0009"}])

    for label, statement in [
        ("nested-order", f"SELECT * FROM {literal(native)} ORDER BY items LIMIT 0"),
        ("nested-set", f"SELECT items FROM {literal(native)} UNION SELECT items FROM {literal(native)}"),
        ("nested-group", f"SELECT items,COUNT(*) AS n FROM {literal(native)} GROUP BY items"),
        ("nested-distinct-count", f"SELECT COUNT(DISTINCT items) AS n FROM {literal(native)}"),
        ("zip-mismatch", f"SELECT * FROM EXPLODE((SELECT * FROM {literal(native)} ORDER BY id), '{{\"explode_columns\":[\"items\",\"records\"]}}') AS expanded"),
    ]:
        guard()
        destination = output / f"must-not-publish-{label}.vortex"
        denied(f"nested-invalid-{label}", context.sql(statement).write_vortex(destination, check=False, **resources), destination)
    guard()
