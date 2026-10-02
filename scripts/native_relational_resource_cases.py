# SPDX-License-Identifier: Apache-2.0
"""Finite public spill checks, using the shared relational acceptance driver."""

from __future__ import annotations

import csv
import json

from run_clickbench_query_uat import file_sha256, strict_json
from run_native_unary_uat import csv_cell


def run(context, output, guard, accepted, complete, sources, identity):
    import shardloom as sl
    from shardloom.query import SqlWorkflow

    output.mkdir(parents=True)
    workspace = output / "spill"
    workspace.mkdir()
    raw, native = output / "records.jsonl", output / "records.vortex"
    count = 70_017
    original = [
        {"identifier": value, "category": None if value % 17 == 0 else value % 7,
         "label": ["001", "東京", "", "a\u0000z", "ω"][value % 5],
         "sample": (value % 13) / 2.0 - 3.0}
        for value in reversed(range(count))
    ]
    # Freeze an independent stable reference before invoking any engine route.
    expected = sorted(original, key=lambda row: (
        row["category"] is None, row["category"] or 0, row["label"],
    ))
    expected = [dict(row, quantity=row["identifier"] + 3) for row in expected]
    with raw.open("x") as stream:
        for row in original:
            stream.write(json.dumps(row, ensure_ascii=False) + "\n")
    guard()
    accepted("resources-prepare", context.read_json(raw).prepare(native, check=False))
    sources.extend((path, file_sha256(path), identity(path)) for path in (raw, native))
    spill = {"workspace": str(workspace), "quota_bytes": 64 << 20, "buffer_bytes": 1 << 20}
    resources = {"memory_gb": 1, "max_parallelism": 1, "spill": spill}
    frame = (context.read_vortex(native).limit(count).sort("category", "label", nulls="last")
             .with_column("quantity", sl.col("identifier") + 3))
    sql = SqlWorkflow(frame._relation_statement(), context.client,
                      source_bindings=frame._declared_sources())

    def clean():
        if list(workspace.iterdir()):
            raise ValueError("public relational execution left a spill entry behind")

    def verified(name, report, *, spilled=True):
        envelope = accepted(name, report)
        for field, value in {
            "public_workflow_memory_gb": "1", "public_workflow_max_parallelism": "1",
            "public_workflow_spill_requested": "true", "relational_spill_requested": "true",
            "resident_memory_limit_bytes": str(1 << 30),
            "relational_spill_quota_bytes": str(64 << 20),
            "relational_spill_buffer_bytes": str(1 << 20),
            "relational_spill_owned_cleanup_completed": "true",
            "spill_io_performed": str(spilled).lower(),
        }.items():
            if envelope.field(field) != value:
                raise ValueError(f"{name}: {field}={envelope.field(field)!r}; expected {value!r}")
        if spilled and int(envelope.field("relational_spill_merge_passes")) < 1:
            raise ValueError(f"{name}: no native run merge was exercised")
        if int(envelope.field("resident_peak_reserved_buffer_bytes")) > 1 << 30:
            raise ValueError(f"{name}: reserved memory exceeds the declared grant")
        if int(envelope.field("relational_spill_peak_disk_bytes")) > spill["quota_bytes"]:
            raise ValueError(f"{name}: run storage exceeds the declared quota")
        clean()
        return envelope

    def equal(name, actual, wanted, destination=None):
        if actual != wanted:
            index = next((i for i, pair in enumerate(zip(actual, wanted)) if pair[0] != pair[1]), None)
            raise ValueError(f"{name}: complete result differs; rows={len(actual)}/{len(wanted)}, first mismatch={index}")
        complete(name, actual, wanted, destination)

    # Inspection with an absent workspace is inert, through both public spellings.
    absent = output / "absent-inspection-workspace"
    for family, workflow in [("dataframe", frame), ("sql", sql)]:
        guard()
        inspected = workflow.limit(97).route(check=False, **dict(resources, spill=dict(spill, workspace=str(absent))))
        (output / f"{family}-route.json").write_text(json.dumps(inspected.envelope.raw, indent=2) + "\n")
        if (inspected.route_status != "admitted" or not inspected.side_effect_free
                or inspected.fallback_attempted or inspected.external_engine_invoked):
            raise ValueError(f"{family}: spill route inspection was not admitted and inert")
        if absent.exists():
            raise ValueError("route inspection created the configured spill workspace")
        for call in range(2):
            name = f"resources-{family}-collect-{call + 1}"
            report = workflow.limit(97).collect(check=False, **resources)
            verified(name, report)
            equal(name, list(report.result_rows), expected[:97])
        denied = workflow.collect(check=False, **resources).envelope
        if (denied.status != "error" or denied.fallback.attempted
                or denied.raw.get("certificates") or denied.raw.get("artifacts")
                or not any("collect exceeds 65,536 rows" in item.get("reason", "")
                           for item in denied.raw.get("diagnostics", []))):
            raise ValueError(f"{family}: spill changed the collection boundary")
        (output / f"{family}-collect-denial.json").write_text(json.dumps(denied.raw, indent=2) + "\n")
        clean()
        for extension in ("vortex", "parquet", "arrow_ipc", "avro", "orc", "json", "jsonl", "csv"):
            guard()
            name = f"resources-{family}-{extension}"
            destination = output / f"{name}.{extension}"
            verified(name, getattr(workflow, f"write_{extension}")(
                destination, check=False, **resources))
            if extension == "csv":
                with destination.open(newline="") as stream:
                    reader = csv.DictReader(stream)
                    if reader.fieldnames != list(expected[0]):
                        raise ValueError(f"{name}: CSV schema/order changed")
                    actual = list(reader)
                equal(name, actual, [{key: csv_cell(value) for key, value in row.items()}
                                     for row in expected], destination)
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
                    decoded = output / f"{name}-decoded.jsonl"
                    accepted(f"{name}-read", context.read_vortex(reopened).write_jsonl(decoded, check=False))
                actual = [strict_json(line) for line in decoded.read_text().splitlines()]
            equal(name, actual, expected, destination)

    # A compatible input is normalized once, then shares the exact same kernel.
    compatible = (context.read_json(raw).limit(count).sort("category", "label", nulls="last")
                  .with_column("quantity", sl.col("identifier") + 3).limit(97))
    name = "resources-compatible-collect"
    report = compatible.collect(check=False, **resources)
    verified(name, report)
    equal(name, list(report.result_rows), expected[:97])

    # Each transformed input reaches the same ordering kernel with enough rows
    # for multiple native runs. The references are independent Python values.
    dimension_raw = output / "categories.jsonl"
    dimension_native = output / "categories.vortex"
    dimension_raw.write_text("".join(json.dumps({"category": value, "category_name": f"c{value}"}) + "\n"
                                     for value in range(7)))
    accepted("resources-dimension", context.read_json(dimension_raw).prepare(dimension_native, check=False))
    sources.extend((path, file_sha256(path), identity(path)) for path in (dimension_raw, dimension_native))
    base = context.read_vortex(native)
    joined = base.join(context.read_vortex(dimension_native), on="category", how="left").select(
        "f.identifier AS identifier", "d.category_name AS category_name")
    joined_expected = sorted([
        {"identifier": row["identifier"],
         "category_name": None if row["category"] is None else f"c{row['category']}"}
        for row in original
    ], key=lambda row: (row["category_name"] is None, row["category_name"] or "", row["identifier"]))
    subset = base.select("identifier", "label")
    union_expected = sorted([
        {"identifier": row["identifier"], "label": row["label"]}
        for row in original + original
    ], key=lambda row: (row["label"], row["identifier"]))
    aggregate_expected = sorted([
        {"identifier": row["identifier"], "total": row["sample"], "entries": 1}
        for row in original
    ], key=lambda row: (row["total"], row["identifier"]))
    nested_expected = sorted(sorted(original, key=lambda row: row["identifier"]), key=lambda row: (
        row["category"] is None, row["category"] or 0, row["label"],
    ))
    for shape, transformed, wanted in [
        ("join", joined.sort("category_name", "identifier", nulls="last"), joined_expected),
        ("set", subset.union_all(subset).sort("label", "identifier"), union_expected),
        ("aggregate", base.limit(count).group_by("identifier").agg(total="sum(sample)", entries="count(*)")
         .sort("total", "identifier", nulls="last"), aggregate_expected),
        ("nested", base.sort("identifier").limit(count).sort("category", "label", nulls="last"),
         nested_expected),
        ("order-limit-filter", base.sort("identifier").limit(30_000)
         .filter(sl.col("identifier") >= 29_903),
         [row for row in sorted(original, key=lambda row: row["identifier"])[:30_000]
          if row["identifier"] >= 29_903]),
    ]:
        limited = transformed.limit(97)
        spelling = SqlWorkflow(limited._relation_statement(), context.client,
                               source_bindings=limited._declared_sources())
        for family, workflow in [("dataframe", limited), ("sql", spelling)]:
            guard()
            name = f"resources-{shape}-{family}"
            report = workflow.collect(check=False, **resources)
            verified(name, report)
            equal(name, list(report.result_rows), wanted[:97])

    # Permission alone does not require disk I/O when no state reaches the threshold.
    small = context.read_vortex(native).limit(2).sort("identifier")
    name = "resources-under-threshold"
    report = small.collect(check=False, **resources)
    verified(name, report, spilled=False)
    equal(name, list(report.result_rows), list(reversed(original[:2])))

    # Quota and invalid-workspace failures cannot publish a destination or leak runs.
    for reason, policy in [("quota", dict(spill, quota_bytes=32 << 10)),
                           ("workspace", dict(spill, workspace=str(absent)))]:
        guard()
        destination = output / f"must-not-publish-{reason}.vortex"
        before = set(output.iterdir())
        report = frame.write_vortex(destination, check=False, **dict(resources, spill=policy))
        envelope = report.envelope
        created = set(output.iterdir()) - before
        (output / f"{reason}-denial.json").write_text(json.dumps(envelope.raw, indent=2) + "\n")
        if (envelope.status != "error" or envelope.fallback.attempted or destination.exists()
                or envelope.raw.get("certificates") or envelope.raw.get("artifacts") or created):
            raise ValueError(f"{reason}: invalid spill request published output or did not fail")
        if absent.exists():
            raise ValueError("execution created the caller's absent workspace")
        clean()
    guard()
