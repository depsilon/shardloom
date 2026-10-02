#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Check complete relational workflows through Python and all eight local writers.

Run under the serial workload/deadline supervisor. This checks availability and
correctness against literal expectations; it makes no performance or RSS claim.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
from pathlib import Path
import signal
import sys

from local_uat_storage import GIB, MIB, check_budgets, require_local_path
from run_clickbench_query_uat import file_sha256, strict_json
from run_native_unary_uat import csv_cell
from native_relational_composition_cases import cases as composition_cases
from native_relational_resource_cases import run as resource_cases
from native_aggregate_ordering_cases import run as aggregate_cases
from native_unary_composition_cases import run as unary_cases


def cases(context, left: Path, right: Path, raw_right: Path, typed_left: Path, typed_right: Path):
    import shardloom as sl

    frame = context.read_vortex(left, schema={"cargo_id": "int64", "load_units": "int64"})
    dimension = context.read_vortex(right)
    key = frame.select("cargo_id")
    other_key = dimension.select("cargo_id")
    typed_frame = context.read_csv(typed_left, schema={"key": "utf8", "amount": "int64"})
    typed_dimension = context.read_csv(typed_right, schema={"key": "utf8", "label": "utf8"})
    sql = context.sql
    left_rows = [(2, 10), (1, 20), (2, 11), (3, 30), (None, 50), (4, 60)]
    left_join = [
        (2, 10, "two-a"), (2, 10, "two-b"), (1, 20, None),
        (2, 11, "two-a"), (2, 11, "two-b"), (3, 30, "three"),
        (None, 50, None), (4, 60, None),
    ]
    full_on = [
        (2, 10, None), (1, 20, None), (2, 11, "two-a"),
        (2, 11, "two-b"), (3, 30, "three"), (None, 50, None),
        (4, 60, None), (None, None, "null-key"), (None, None, "five"),
    ]

    def joined(rows):
        return [{"cargo_id": key, "load_units": load, "label": label}
                for key, load, label in rows]

    def keys(values):
        return [{"cargo_id": value} for value in values]

    return [
        ("sql-full-on", sql(
            f"SELECT l.cargo_id AS cargo_id,l.load_units AS load_units,r.label AS label FROM '{left}' AS l FULL JOIN '{right}' AS r "
            "ON l.cargo_id = r.cargo_id AND l.load_units > 10"), joined(full_on), 2),
        ("frame-left", frame.join(dimension, on="cargo_id", how="left")
         .select("f.cargo_id AS cargo_id", "f.load_units AS load_units", "d.label AS label"), joined(left_join), 2),
        ("frame-mixed-left", frame.join(context.read_json(raw_right), on="cargo_id", how="left")
         .select("f.cargo_id AS cargo_id", "f.load_units AS load_units", "d.label AS label"), joined(left_join), 2),
        ("sql-union-all", sql(f"SELECT cargo_id FROM '{left}' UNION ALL SELECT cargo_id FROM '{right}'"),
         keys([2, 1, 2, 3, None, 4, 2, 2, 3, None, 5]), 2),
        ("frame-union", key.union(other_key), keys([2, 1, 3, None, 4, 5]), 2),
        ("frame-intersect", key.intersect(other_key), keys([2, 3, None]), 2),
        ("frame-except", key.except_(other_key), keys([1, 4]), 2),
        ("frame-window", frame.select("cargo_id", "load_units").window(
            "ROW_NUMBER() OVER (ORDER BY load_units DESC) AS position",
            "LAG(load_units,1) OVER (ORDER BY load_units) AS previous"),
         [{"cargo_id": k, "load_units": v, "position": rank, "previous": prev}
          for (k, v), rank, prev in zip(left_rows, [6, 4, 5, 3, 2, 1], [None, 11, 10, 20, 30, 50])], 1),
        ("sql-correlated-count", sql(
            f"SELECT cargo_id,load_units FROM '{left}' WHERE cargo_id IN "
            f"(SELECT COUNT(*) AS n FROM '{left}' WHERE load_units <= outer.load_units)"),
         [{"cargo_id": 2, "load_units": 11}], 1),
        ("frame-membership", key.filter(sl.col("cargo_id").isin_source(right, "cargo_id")),
         keys([2, 2, 3]), 2),
        ("sql-nested", sql(
            f"SELECT cargo_id FROM '{left}' WHERE cargo_id IN (SELECT cargo_id FROM '{raw_right}' "
            f"WHERE cargo_id IN (SELECT cargo_id FROM '{left}' WHERE load_units >= 20))"),
         keys([3]), 2),
        ("sql-empty", sql(f"SELECT cargo_id FROM '{left}' EXCEPT SELECT cargo_id FROM '{left}'"), [], 1),
        ("frame-typed-join", typed_frame.join(typed_dimension, on="key")
         .select("f.key AS key", "f.amount AS amount", "d.label AS label"),
         [{"key": "001", "amount": 2, "label": "0009"}, {"key": "1", "amount": 3, "label": "0010"}], 2),
        ("frame-typed-union", typed_frame.select("key").union(typed_dimension.select("key")).limit(8),
         [{"key": "001"}, {"key": "1"}], 2),
        ("frame-typed-membership", typed_frame.select("key").filter(sl.col("key").isin_source(
            typed_dimension, "key", where=sl.col("label") == "0009")), [{"key": "001"}], 2),
        ("frame-typed-nested-exists", typed_frame.select("key").filter(sl.exists_source(
            typed_dimension, select=1, where=(sl.col("key") == sl.col("outer.key"))
            & sl.col("key").isin_source(typed_frame, "key", where=sl.col("amount") == 3))),
         [{"key": "1"}], 2),
    ] + composition_cases(context, left, right, raw_right, typed_left, typed_right)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--uat-root", type=Path, required=True)
    parser.add_argument("--build-commit", required=True)
    parser.add_argument("--family", choices=("all", "unary"), default="all")
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    root = require_local_path(args.uat_root, Path.home(), sys.platform)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    output = root / "logs" / f"native_relational_{stamp}"
    left, right = output / "cargo.vortex", output / "dimension.vortex"

    def guard():
        check_budgets(root, left, output, min_free_bytes=12 * GIB, reserve_bytes=64 * MIB,
                      max_workspace_bytes=100 * GIB, max_log_bytes=192 * MIB)

    guard()
    root.mkdir(parents=True, exist_ok=True)
    code = Path(__file__).resolve()
    query = code.parents[1] / "python/src/shardloom/query.py"
    client_code = code.parents[1] / "python/src/shardloom/client.py"
    composition_code = code.with_name("native_relational_composition_cases.py")
    resource_code = code.with_name("native_relational_resource_cases.py")
    aggregate_code = code.with_name("native_aggregate_ordering_cases.py")
    unary_code = code.with_name("native_unary_composition_cases.py")
    renderer_code = query.with_name("_relational_sql.py")
    summary = {
        "schema_version": "shardloom.native_relational_python_acceptance.v1",
        "status": "running", "build_commit": args.build_commit, "cases": [],
        "acceptance_family": args.family,
        "binary_sha256": file_sha256(binary), "harness_sha256": file_sha256(code),
        "python_query_sha256": file_sha256(query), "fallback_attempted": False,
        "python_client_sha256": file_sha256(client_code),
        "composition_cases_sha256": file_sha256(composition_code),
        "resource_cases_sha256": file_sha256(resource_code),
        "aggregate_cases_sha256": file_sha256(aggregate_code),
        "unary_cases_sha256": file_sha256(unary_code),
        "python_relational_renderer_sha256": file_sha256(renderer_code),
        "external_engine_invoked": False, "performance_claim": False,
        "total_rss_bound": False, "csv_contract": "complete header/row text; null is an empty field",
    }
    lock = root / ".ingest-uat.lock"
    lock.mkdir()
    client = None
    sources = []

    def accepted(name, report):
        envelope = report.envelope
        (output / f"{name}.envelope.json").write_text(json.dumps(envelope.raw, indent=2) + "\n")
        if envelope.status != "success" or envelope.fallback.attempted:
            raise ValueError(f"{name}: request failed: {envelope.raw}")
        if envelope.field("public_workflow_external_engine_invoked") != "false":
            raise ValueError(f"{name}: missing no-external-engine proof")
        return envelope

    def complete(name, actual, expected, destination=None):
        if actual != expected:
            raise ValueError(f"{name}: complete values differ: {actual!r} != {expected!r}")
        entry = {"name": name, "complete_rows_verified": len(actual), "passed": True}
        if destination:
            entry["output_sha256"] = file_sha256(destination)
        summary["cases"].append(entry)
        print(f"PASS {name}: {len(actual)} complete rows", flush=True)

    def identity(path):
        info = path.stat()
        return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)

    try:
        output.mkdir(parents=True)
        sys.path.insert(0, str(code.parents[1] / "python/src"))
        import shardloom as sl
        from shardloom.query import SqlWorkflow

        context = sl.context(binary=str(binary), cwd=output, timeout=120)
        client = context.client
        if args.family == "all":
            raw_left, raw_right = output / "cargo.jsonl", output / "dimension.jsonl"
            typed_left, typed_right = output / "typed-left.csv", output / "typed-right.data"
            typed_left.write_text("key,amount\n001,2\n1,3\n")
            typed_right.write_text("key,label\n001,0009\n1,0010\n")
            sources.extend((path, file_sha256(path), identity(path)) for path in [typed_left, typed_right])
            raw_left.write_text("".join(json.dumps({"cargo_id": k, "load_units": v}) + "\n"
                                        for k, v in [(2, 10), (1, 20), (2, 11), (3, 30), (None, 50), (4, 60)]))
            raw_right.write_text("".join(json.dumps({"cargo_id": k, "label": v}) + "\n"
                                         for k, v in [(2, "two-a"), (2, "two-b"), (3, "three"), (None, "null-key"), (5, "five")]))
            for raw, native in [(raw_left, left), (raw_right, right)]:
                accepted(f"prepare-{native.stem}", context.read_json(raw).prepare(native, check=False))
                sources.extend((path, file_sha256(path), identity(path)) for path in [raw, native])

            for family, workflow, expected, opens in cases(context, left, right, raw_right, typed_left, typed_right):
                columns = list(expected[0]) if expected else {
                    "sql-empty": ["cargo_id"], "composition-empty-window": ["renamed", "rn"],
                }[family]
                for execution in range(1, 4):
                    guard()
                    name = f"{family}-collect-{execution}"
                    report = workflow.collect(check=False)
                    envelope = accepted(name, report)
                    complete(name, list(report.result_rows), expected)
                    for field, value in {
                        "output_columns": ",".join(columns),
                        "resident_source_opens": str(opens), "resident_completed_executions": str(execution),
                        "resident_relational_handle_retained": "true", "result_payload_complete": "true",
                        "resident_relational_lowering_reused": str(execution > 1).lower(),
                    }.items():
                        if envelope.field(field) != value:
                            raise ValueError(f"{name}: {field}={envelope.field(field)!r}; expected {value!r}")
                sql_workflow = SqlWorkflow(
                    workflow._relation_statement(), client, source_bindings=workflow._declared_sources(),
                )
                name = f"{family}-sql-parity"
                sql_report = sql_workflow.collect(check=False)
                accepted(name, sql_report)
                complete(name, list(sql_report.result_rows), expected)
                for extension in ["vortex", "parquet", "arrow_ipc", "avro", "orc", "json", "jsonl", "csv"]:
                    guard()
                    name = f"{family}-{extension}"
                    destination = output / f"{name}.{extension}"
                    accepted(name, getattr(workflow, f"write_{extension}")(destination, check=False))
                    if extension == "csv":
                        with destination.open(newline="") as stream:
                            reader = csv.DictReader(stream)
                            if reader.fieldnames != columns:
                                raise ValueError(f"{name}: CSV column order differs: {reader.fieldnames!r} != {columns!r}")
                            actual = list(reader)
                        complete(name, actual, [{k: csv_cell(v) for k, v in row.items()} for row in expected], destination)
                        continue
                    if extension == "json":
                        actual = strict_json(destination.read_text())
                    else:
                        decoded = destination
                        if extension != "jsonl":
                            native = destination
                            if extension != "vortex":
                                native = output / f"{name}-normalized.vortex"
                                accepted(f"{name}-prepare", getattr(context, f"read_{extension}")(
                                    destination).prepare(native, check=False))
                            schema = accepted(f"{name}-schema", context.sql(
                                f"SELECT * FROM (SELECT * FROM '{native}') AS reopened LIMIT 0"
                            ).collect(check=False))
                            if schema.field("output_columns") != ",".join(columns):
                                raise ValueError(f"{name}: reopened schema column order differs")
                            decoded = output / f"{name}-reopened.jsonl"
                            accepted(f"{name}-reopen", context.read_vortex(native).write_jsonl(decoded, check=False))
                        actual = [strict_json(line) for line in decoded.read_text().splitlines()]
                    complete(name, actual, expected, destination)
            # A finite input can still exceed the small collection boundary. Derived
            # stages must preserve complete streaming output beyond that boundary.
            large_raw, large = output / "large.jsonl", output / "large.vortex"
            count = 65_541
            with large_raw.open("x") as stream:
                for value in range(count):
                    stream.write(json.dumps({"identifier": value}) + "\n")
            accepted("large-prepare", context.read_json(large_raw).prepare(large, check=False))
            sources.extend((path, file_sha256(path), identity(path)) for path in [large_raw, large])
            workflow = (context.read_vortex(large).limit(count)
                        .with_column("identifier", sl.col("identifier") + 1).filter(sl.col("identifier") > 0))
            denial = workflow.collect(check=False).envelope
            (output / "large-collect-denial.envelope.json").write_text(json.dumps(denial.raw, indent=2) + "\n")
            if (denial.status != "error" or denial.fallback.attempted
                    or not any("collect exceeds 65,536 rows" in item.get("reason", "")
                               for item in denial.raw.get("diagnostics", []))):
                raise ValueError("large derived collection did not enforce its existing row boundary")
            expected = [{"identifier": value} for value in range(1, count + 1)]
            for extension in ["vortex", "parquet", "arrow_ipc", "avro", "orc", "json", "jsonl", "csv"]:
                guard()
                name = f"composition-large-{extension}"
                destination = output / f"{name}.{extension}"
                accepted(name, getattr(workflow, f"write_{extension}")(destination, check=False))
                if extension == "csv":
                    with destination.open(newline="") as stream:
                        reader = csv.DictReader(stream)
                        if reader.fieldnames != ["identifier"]:
                            raise ValueError("large CSV schema differs")
                        actual = list(reader)
                    complete(name, actual, [{"identifier": str(row["identifier"])} for row in expected], destination)
                    continue
                if extension == "json":
                    actual = strict_json(destination.read_text())
                else:
                    decoded = destination
                    if extension != "jsonl":
                        native = destination
                        if extension != "vortex":
                            native = output / f"{name}-normalized.vortex"
                            accepted(f"{name}-prepare", getattr(context, f"read_{extension}")(
                                destination).prepare(native, check=False))
                        decoded = output / f"{name}-reopened.jsonl"
                        accepted(f"{name}-reopen", context.read_vortex(native).write_jsonl(decoded, check=False))
                    actual = [strict_json(line) for line in decoded.read_text().splitlines()]
                complete(name, actual, expected, destination)

            resource_cases(context, root / "data" / f"resources_{stamp}", guard,
                           accepted, complete, sources, identity)
            aggregate_cases(context, root / "data" / f"aggregates_{stamp}", guard,
                            accepted, complete, sources, identity)
        unary_cases(context, root / "data" / f"unary_{stamp}", guard,
                    accepted, complete, sources, identity)
        for path, digest, generation in sources:
            if generation != identity(path) or digest != file_sha256(path):
                raise ValueError("a source changed during acceptance")
        summary["source_sha256"] = {path.name: digest for path, digest, _ in sources}
        summary["source_files"] = [
            {"path": str(path), "sha256": digest, "identity": generation}
            for path, digest, generation in sources
        ]
        for path, key in [(binary, "binary_sha256"), (code, "harness_sha256"), (query, "python_query_sha256"),
                          (client_code, "python_client_sha256"), (composition_code, "composition_cases_sha256"),
                          (resource_code, "resource_cases_sha256"),
                          (aggregate_code, "aggregate_cases_sha256"),
                          (unary_code, "unary_cases_sha256"),
                          (renderer_code, "python_relational_renderer_sha256")]:
            if file_sha256(path) != summary[key]:
                raise ValueError(f"{key} changed during acceptance")
        summary["status"] = "passed"
        guard()
        return 0
    except BaseException as error:
        summary.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        try:
            if client is not None:
                client.close()
        finally:
            try:
                if output.exists():
                    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
                    print(output / "summary.json", flush=True)
            finally:
                lock.rmdir()


if __name__ == "__main__":
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
