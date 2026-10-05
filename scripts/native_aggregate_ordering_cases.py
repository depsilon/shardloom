# SPDX-License-Identifier: Apache-2.0
"""Complete-value aggregate admission and explicit-null ordering acceptance."""

from __future__ import annotations

import csv
import json

from run_clickbench_query_uat import file_sha256, strict_json
from run_native_unary_uat import csv_cell
from native_report_evidence import require_native_resource_admission


def run(context, output, guard, accepted, complete, sources, identity):
    from shardloom.query import SqlWorkflow

    output.mkdir(parents=True)
    raw, native, typed = (output / name for name in ("readings.jsonl", "readings.vortex", "readings.data"))
    original = list(zip(
        [None, "a", "b", "c", "d", "東京"] * 2,
        [1, None, 2, None, 1, -5, 7, None, 6, None, 7, -1],
    ))
    grouped = [
        {"cohort": None, "mean": 4.0, "entries": 2},
        {"cohort": "a", "mean": None, "entries": 2},
        {"cohort": "b", "mean": 4.0, "entries": 2},
        {"cohort": "c", "mean": None, "entries": 2},
        {"cohort": "d", "mean": 4.0, "entries": 2},
        {"cohort": "東京", "mean": -3.0, "entries": 2},
    ]
    # Both keys have the same direction/null policy in the DataFrame spelling.
    # Explicit matrices avoid deriving the ordering from an engine result.
    orders = [
        (False, "first", [1, 3, 5, 0, 2, 4]),
        (True, "first", [3, 1, 0, 4, 2, 5]),
        (False, "last", [5, 2, 4, 0, 1, 3]),
        (True, "last", [4, 2, 0, 5, 3, 1]),
    ]
    expected_file = output / "expected.json"
    expected_file.write_text(json.dumps({"grouped": grouped, "orders": orders}, ensure_ascii=False, indent=2) + "\n")
    raw.write_text("".join(json.dumps({"cohort": key, "reading": value}, ensure_ascii=False) + "\n"
                           for key, value in original))
    with typed.open("x", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["cohort", "reading"])
        writer.writerows(original)
    sources.extend((path, file_sha256(path), identity(path)) for path in (expected_file, raw, typed))
    guard()
    accepted("aggregate-ordering-prepare", context.read_json(raw).prepare(native, check=False))
    sources.append((native, file_sha256(native), identity(native)))
    resources = {"memory_gb": 1, "max_parallelism": 2}

    def verified(name, report):
        envelope = accepted(name, report)
        require_native_resource_admission(name, envelope)
        return envelope

    def equal(name, actual, wanted, destination=None):
        if actual != wanted:
            mismatch = next((i for i, pair in enumerate(zip(actual, wanted)) if pair[0] != pair[1]), None)
            raise ValueError(f"{name}: complete values differ; rows={len(actual)}/{len(wanted)}, first mismatch={mismatch}")
        complete(name, actual, wanted, destination)

    def write_all(family, workflow, expected, columns):
        for extension in ("vortex", "parquet", "arrow_ipc", "avro", "orc", "json", "jsonl", "csv"):
            guard()
            name = f"{family}-{extension}"
            destination = output / f"{name}.{extension}"
            verified(name, getattr(workflow, f"write_{extension}")(destination, check=False, **resources))
            if extension == "csv":
                with destination.open(newline="") as stream:
                    reader = csv.DictReader(stream)
                    if reader.fieldnames != columns:
                        raise ValueError(f"{name}: CSV column order differs")
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
                        f"SELECT * FROM (SELECT * FROM '{reopened}') AS reopened LIMIT 0"
                    ).collect(check=False))
                    if schema.field("output_columns") != ",".join(columns):
                        raise ValueError(f"{name}: reopened column order differs")
                    decoded = output / f"{name}-reopened.jsonl"
                    accepted(f"{name}-read", context.read_vortex(reopened).write_jsonl(decoded, check=False))
                actual = [strict_json(line) for line in decoded.read_text().splitlines()]
            equal(name, actual, expected, destination)

    for source_name, base in [
        ("native", context.read_vortex(native)),
        ("declared-csv", context.read_csv(typed, schema={"cohort": "utf8", "reading": "float64"})),
    ]:
        for descending, nulls, indices in orders:
            direction = "DESC" if descending else "ASC"
            frame = base.group_by("cohort").agg(mean="avg(reading)", entries="count(*)").sort(
                "mean", "cohort", descending=descending, nulls=nulls)
            statement = (f"SELECT cohort, AVG(reading) AS mean, COUNT(*) AS entries FROM '{base.source.uri}' "
                         f"GROUP BY cohort ORDER BY mean {direction} NULLS {nulls.upper()}, "
                         f"cohort {direction} NULLS {nulls.upper()}")
            sql = SqlWorkflow(statement, context.client, source_bindings=base._declared_sources())
            expected = [grouped[index] for index in indices]
            for spelling, workflow in [("dataframe", frame), ("sql", sql)]:
                family = f"aggregate-{source_name}-{spelling}-{direction.lower()}-{nulls}"
                inspected = workflow.route(bounded=True, check=False, **resources)
                (output / f"{family}-route.json").write_text(json.dumps(inspected.envelope.raw, indent=2) + "\n")
                if (inspected.route_status != "admitted" or not inspected.side_effect_free
                        or inspected.fallback_attempted or inspected.external_engine_invoked):
                    raise ValueError(f"{family}: route inspection was not admitted and inert")
                for parallelism in (1, 2):
                    guard()
                    name = f"{family}-collect-{parallelism}"
                    report = workflow.collect(check=False, **dict(resources, max_parallelism=parallelism))
                    envelope = verified(name, report)
                    if envelope.field("result_payload_complete") != "true":
                        raise ValueError(f"{name}: incomplete collection")
                    equal(name, list(report.result_rows), expected)
                write_all(family, workflow, expected, ["cohort", "mean", "entries"])

    # Scalar, HAVING, offset, and empty grouped results retain their whole plan.
    finite = [
        ("scalar", f"SELECT AVG(reading) AS mean, COUNT(*) AS entries FROM '{native}' "
         "ORDER BY mean DESC NULLS FIRST LIMIT 1", [{"mean": 2.25, "entries": 12}], ["mean", "entries"]),
        ("having-offset", f"SELECT cohort, AVG(reading) AS mean, COUNT(*) AS entries FROM '{native}' "
         "GROUP BY cohort HAVING entries > 1 ORDER BY mean ASC NULLS FIRST, cohort ASC NULLS LAST LIMIT 3 OFFSET 1",
         [grouped[index] for index in [3, 5, 2]], ["cohort", "mean", "entries"]),
        ("empty", f"SELECT cohort, AVG(reading) AS mean, COUNT(*) AS entries FROM '{native}' "
         "GROUP BY cohort HAVING entries > 2 ORDER BY mean ASC NULLS FIRST", [], ["cohort", "mean", "entries"]),
    ]
    for label, statement, expected, columns in finite:
        workflow = context.sql(statement)
        name = f"aggregate-{label}"
        report = workflow.collect(check=False, **resources)
        verified(name, report)
        equal(name, list(report.result_rows), expected)
        write_all(name, workflow, expected, columns)

    # Complete aggregate groups exceed collection's independent row limit.
    count = 65_541
    large_raw, large = output / "groups.jsonl", output / "groups.vortex"
    large_expected = [{"identifier": value, "entries": 1} for value in range(count)]
    with large_raw.open("x") as stream:
        for value in reversed(range(count)):
            stream.write(json.dumps({"identifier": value}) + "\n")
    guard()
    accepted("aggregate-large-prepare", context.read_json(large_raw).prepare(large, check=False))
    sources.extend((path, file_sha256(path), identity(path)) for path in (large_raw, large))
    frame = context.read_vortex(large).group_by("identifier").agg(entries="count(*)").sort("identifier", nulls="last")
    sql = context.sql(f"SELECT identifier, COUNT(*) AS entries FROM '{large}' GROUP BY identifier ORDER BY identifier ASC NULLS LAST")
    for spelling, workflow in [("dataframe", frame), ("sql", sql)]:
        family = f"aggregate-large-{spelling}"
        report = workflow.limit(97).collect(check=False, **resources)
        verified(f"{family}-limited", report)
        equal(f"{family}-limited", list(report.result_rows), large_expected[:97])
        denial = workflow.collect(check=False, **resources).envelope
        (output / f"{family}-denial.json").write_text(json.dumps(denial.raw, indent=2) + "\n")
        if (denial.status != "error" or denial.fallback.attempted
                or denial.raw.get("certificates") or denial.raw.get("artifacts")
                or not any("collect exceeds 65,536 rows" in item.get("reason", "")
                           for item in denial.raw.get("diagnostics", []))):
            raise ValueError(f"{family}: aggregate collection ignored its row limit")
        write_all(family, workflow, large_expected, ["identifier", "entries"])

    for label, statement in [
        ("invalid-null", f"SELECT cohort, AVG(reading) AS mean FROM '{native}' GROUP BY cohort ORDER BY mean NULLS MIDDLE"),
        ("missing-column", f"SELECT cohort, AVG(missing) AS mean FROM '{native}' GROUP BY cohort ORDER BY mean NULLS LAST"),
    ]:
        guard()
        destination = output / f"must-not-publish-{label}.vortex"
        denial = context.sql(statement).write_vortex(destination, check=False, **resources).envelope
        (output / f"{label}-denial.json").write_text(json.dumps(denial.raw, indent=2) + "\n")
        if (denial.status not in ("error", "unsupported") or denial.fallback.attempted
                or denial.raw.get("certificates") or denial.raw.get("artifacts") or destination.exists()):
            raise ValueError(f"{label}: invalid aggregate published output or success evidence")
    guard()
