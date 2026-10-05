#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Verify all ten unary families through the real Python public worker and writers.

Complete renamed-schema results are checked against independent expectations. This
is bounded availability/correctness acceptance, not a performance comparison.
Run under a serial process/deadline supervisor as documented in the evidence packet.
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


def csv_cell(value):
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value).lower()
    # This fixture's finite floating outputs are integral; CSV has no dtype tag.
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def require_unique_report_fields(raw):
    """Reject ambiguous evidence before a field accessor can choose one value."""
    keys = set()
    for field in raw["fields"]:
        key = field["key"]
        if key in keys:
            raise ValueError(f"repeated native report field: {key}")
        keys.add(key)


def cases(context, source: Path, exploded_source: Path):
    import shardloom as sl

    unfiltered = context.read_vortex(source, schema={
        "cargo_id": "int64", "load_units": "int64", "dock_zone": "utf8",
    })
    frame = unfiltered.filter(sl.col("cargo_id") >= 1)
    selected = frame.select(["cargo_id", "load_units"])
    selected_unfiltered = unfiltered.select(["cargo_id", "load_units"])
    all_rows = [
        {"cargo_id": 2, "load_units": 10}, {"cargo_id": 1, "load_units": 20},
        {"cargo_id": 2, "load_units": 10}, {"cargo_id": 3, "load_units": 30},
        {"cargo_id": 1, "load_units": 40},
    ]
    regular = [
        ("distinct", selected.distinct(), [all_rows[i] for i in [0, 1, 3, 4]]),
        ("dedup", selected.drop_duplicates(subset=["cargo_id"], keep="last"),
         [all_rows[i] for i in [2, 3, 4]]),
        ("duplicate_mask", selected_unfiltered.duplicated(subset=["cargo_id"], keep=False),
         [{"duplicated": v} for v in [True, True, True, False, True]]),
        ("tail", selected_unfiltered.tail(2), all_rows[-2:]),
        ("sample", selected.sample(n=2, random_state=7), [all_rows[1], all_rows[4]]),
        ("expression", selected.eval("load_units = load_units + 1"),
         [{"cargo_id": row["cargo_id"], "load_units": row["load_units"] + 1}
          for row in all_rows]),
        ("melt", selected.melt(id_vars=["cargo_id"], value_vars=["load_units"]),
         [{"cargo_id": row["cargo_id"], "variable": "load_units", "value": row["load_units"]}
          for row in all_rows]),
        ("rolling", frame.select("load_units").rolling(window=2).sum("load_units", alias="total"),
         [{"total": v} for v in [30.0, 30.0, 40.0, 70.0]]),
        ("pivot", frame.pivot_table(index="cargo_id", columns="dock_zone", values="load_units", aggfunc="sum"),
         [{"cargo_id": 1, "pivot_east": 60.0, "pivot_south": None, "pivot_west": None},
          {"cargo_id": 2, "pivot_east": None, "pivot_south": 20.0, "pivot_west": None},
          {"cargo_id": 3, "pivot_east": None, "pivot_south": None, "pivot_west": 30.0}]),
        ("explode", context.read_vortex(exploded_source, schema={
            "cargo_id": "int64", "items": "list<int64>",
        }).select(["cargo_id", "items"]).explode("items"),
         [{"cargo_id": key, "items": value} for key in [2, 1] for value in [7, None, 8]]),
    ]
    filtered = unfiltered.filter(sl.col("cargo_id") >= 2)
    filtered_selected = filtered.select(["cargo_id", "load_units"])
    filtered_rows = [all_rows[i] for i in [0, 2, 3]]
    # Tail and duplicate-mask predicates retain their existing explicit exclusion.
    # Each admitted predicate below removes rows and must survive collect and write.
    return regular + [
        ("filtered-distinct", filtered_selected.distinct(), [all_rows[i] for i in [0, 3]]),
        ("filtered-dedup", filtered_selected.drop_duplicates(subset=["cargo_id"], keep="last"),
         [all_rows[i] for i in [2, 3]]),
        ("filtered-sample", filtered_selected.sample(n=3, random_state=7), filtered_rows),
        ("filtered-expression", filtered_selected.eval("load_units = load_units + 1"),
         [{"cargo_id": row["cargo_id"], "load_units": row["load_units"] + 1}
          for row in filtered_rows]),
        ("filtered-melt", filtered_selected.melt(id_vars=["cargo_id"], value_vars=["load_units"]),
         [{"cargo_id": row["cargo_id"], "variable": "load_units", "value": row["load_units"]}
          for row in filtered_rows]),
        ("filtered-rolling", filtered.select("load_units").rolling(window=2).sum("load_units", alias="total"),
         [{"total": 20.0}, {"total": 40.0}]),
        ("filtered-pivot", filtered.pivot_table(index="cargo_id", columns="dock_zone", values="load_units", aggfunc="sum"),
         [{"cargo_id": 2, "pivot_south": 20.0, "pivot_west": None},
          {"cargo_id": 3, "pivot_south": None, "pivot_west": 30.0}]),
        ("filtered-explode", context.read_vortex(exploded_source, schema={
            "cargo_id": "int64", "items": "list<int64>",
        }).filter(sl.col("cargo_id") >= 2).select(["cargo_id", "items"]).explode("items"),
         [{"cargo_id": 2, "items": value} for value in [7, None, 8]]),
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--uat-root", type=Path, required=True)
    parser.add_argument("--build-commit", required=True)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    root = require_local_path(args.uat_root, Path.home(), sys.platform)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    output = root / "logs" / f"native_unary_{stamp}"
    source = output / "cargo.vortex"

    def guard() -> None:
        check_budgets(root, source, output, min_free_bytes=12 * GIB,
                      reserve_bytes=64 * MIB, max_workspace_bytes=100 * GIB,
                      max_log_bytes=192 * MIB)

    guard()
    root.mkdir(parents=True, exist_ok=True)
    summary = {
        "schema_version": "shardloom.native_unary_python_acceptance.v1",
        "status": "running", "build_commit": args.build_commit,
        "binary_sha256": file_sha256(binary), "cases": [],
        "harness_sha256": file_sha256(Path(__file__).resolve()),
        "python_query_sha256": file_sha256(Path(__file__).resolve().parents[1] / "python/src/shardloom/query.py"),
        "fallback_attempted": False, "external_engine_invoked": False,
        "performance_claim": False, "total_rss_bound": False,
        "csv_contract": "complete header/row text; null is an empty field",
    }
    lock = root / ".ingest-uat.lock"
    lock.mkdir()
    client = None
    sources = []

    def accepted(name, report):
        envelope = report.envelope
        (output / f"{name}.envelope.json").write_text(json.dumps(envelope.raw, indent=2) + "\n")
        require_unique_report_fields(envelope.raw)
        if envelope.status != "success" or envelope.fallback.attempted:
            raise ValueError(f"{name}: native public request failed: {envelope.raw}")
        if envelope.field("public_workflow_external_engine_invoked") != "false":
            raise ValueError(f"{name}: missing public no-external-engine proof")
        return envelope

    def freeze(path):
        info = path.stat()
        sources.append((path, file_sha256(path), (info.st_dev, info.st_ino, info.st_size,
                                                 info.st_mtime_ns, info.st_ctime_ns)))

    def complete(name, actual, expected, destination=None):
        if actual != expected:
            if not isinstance(actual, list):
                raise ValueError(f"{name}: expected complete rows, got {type(actual).__name__}")
            mismatch = next((i for i, pair in enumerate(zip(actual, expected))
                             if pair[0] != pair[1]), min(len(actual), len(expected)))
            detail = repr((actual[mismatch:mismatch + 1], expected[mismatch:mismatch + 1]))[:2048]
            raise ValueError(f"{name}: {len(actual)} vs {len(expected)} rows; first mismatch {mismatch}: {detail}")
        entry = {"name": name, "complete_rows_verified": len(actual), "passed": True}
        if destination is not None:
            entry["output_sha256"] = file_sha256(destination)
        summary["cases"].append(entry)
        print(f"PASS {name}: {len(actual)} complete rows", flush=True)

    try:
        output.mkdir(parents=True)
        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python/src"))
        import shardloom as sl

        context = sl.context(binary=str(binary), cwd=output, timeout=120)
        client = context.client
        raw = output / "cargo.jsonl"
        raw.write_text("".join(json.dumps(row) + "\n" for row in [
            {"cargo_id": 2, "load_units": 10, "dock_zone": "south"},
            {"cargo_id": 1, "load_units": 20, "dock_zone": "east"},
            {"cargo_id": 2, "load_units": 10, "dock_zone": "south"},
            {"cargo_id": 3, "load_units": 30, "dock_zone": "west"},
            {"cargo_id": 1, "load_units": 40, "dock_zone": "east"},
        ]))
        accepted("prepare", context.read_json(raw).prepare(source, check=False))
        freeze(source)
        exploded = output / "lists.vortex"
        structured = (context.read_vortex(source).select("cargo_id")
                      .with_columns({"items": sl.array(7, None, 8)}).limit(2))
        accepted("prepare-lists", structured.write_vortex(exploded, check=False))
        freeze(exploded)

        for family, workflow, expected in cases(context, source, exploded):
            dynamic_schema = family in {"pivot", "filtered-pivot"}
            for execution in range(1, 4):
                guard()
                name = f"{family}-collect-{execution}"
                report = workflow.collect(check=False)
                envelope = accepted(name, report)
                complete(name, list(report.result_rows), expected)
                for field, value in {
                    "resident_source_opens": "1", "resident_relational_handle_retained": "true",
                    "resident_completed_executions": str(execution), "result_payload_complete": "true",
                    "resident_relational_declaration_reused": str(execution > 1).lower(),
                    "resident_relational_lowering_reused": str(execution > 1 and not dynamic_schema).lower(),
                    "relational_schema_binding": "during_execution" if dynamic_schema else "during_preparation",
                    "relational_dynamic_schema_stages": "1" if dynamic_schema else "0",
                    "public_workflow_native_vortex_plan_route_family": "native_vortex_unified_plan",
                }.items():
                    if envelope.field(field) != value:
                        raise ValueError(f"{name}: {field} is {envelope.field(field)!r}, expected {value!r}")
            for extension in ["vortex", "parquet", "arrow_ipc", "avro", "orc", "json", "jsonl", "csv"]:
                guard()
                name = f"{family}-{extension}"
                destination = output / f"{name}.{extension}"
                accepted(name, getattr(workflow, f"write_{extension}")(destination, check=False))
                if extension == "csv":
                    with destination.open(newline="") as stream:
                        actual = list(csv.DictReader(stream))
                    text_rows = [{key: csv_cell(value) for key, value in row.items()}
                                 for row in expected]
                    complete(name, actual, text_rows, destination)
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

        sql = context.sql(f"SELECT DISTINCT cargo_id FROM '{source}'")
        complete("sql-distinct", list(sql.to_python_objects(check=True)),
                 [{"cargo_id": 2}, {"cargo_id": 1}, {"cargo_id": 3}])
        empty = context.read_vortex(source).filter(sl.col("cargo_id") > 99).select("cargo_id").distinct()
        complete("empty-distinct", list(empty.to_python_objects(check=True)), [])

        large_raw = output / "large.jsonl"
        large = output / "large.vortex"
        count = 65_541
        with large_raw.open("x") as stream:
            for value in range(count):
                stream.write(json.dumps({"shipment_identifier": value}) + "\n")
        accepted("large-prepare", context.read_json(large_raw).prepare(large, check=False))
        freeze(large)
        workflow = context.read_vortex(large).select("shipment_identifier").distinct()
        denial = workflow.collect(check=False).envelope
        (output / "large-collect-denial.envelope.json").write_text(json.dumps(denial.raw, indent=2) + "\n")
        row_bound = any(
            diagnostic.get("code") == "SL_INVALID_INPUT"
            and "collect exceeds 65,536 rows" in diagnostic.get("reason", "")
            for diagnostic in denial.raw.get("diagnostics", [])
        )
        if denial.status != "error" or denial.fallback.attempted or not row_bound:
            raise ValueError("large collection did not reject its bounded in-memory request")
        expected = [{"shipment_identifier": value} for value in range(count)]
        for extension in ["vortex", "jsonl"]:
            guard()
            destination = output / f"large-result.{extension}"
            accepted(f"large-{extension}", getattr(workflow, f"write_{extension}")(destination, check=False))
            decoded = destination
            if extension == "vortex":
                decoded = output / "large-reopened.jsonl"
                accepted("large-reopen", context.read_vortex(destination).write_jsonl(decoded, check=False))
            complete(f"large-{extension}", [strict_json(line) for line in decoded.read_text().splitlines()],
                     expected, destination)
        for path, digest, generation in sources:
            info = path.stat()
            after = (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
            if generation != after or digest != file_sha256(path):
                raise ValueError("a source changed during acceptance")
        summary["source_sha256"] = {path.name: digest for path, digest, _ in sources}
        if file_sha256(binary) != summary["binary_sha256"]:
            raise ValueError("the native executable changed during acceptance")
        if file_sha256(Path(__file__).resolve()) != summary["harness_sha256"]:
            raise ValueError("the acceptance harness changed during execution")
        if file_sha256(Path(__file__).resolve().parents[1] / "python/src/shardloom/query.py") != summary["python_query_sha256"]:
            raise ValueError("the Python query implementation changed during acceptance")
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
