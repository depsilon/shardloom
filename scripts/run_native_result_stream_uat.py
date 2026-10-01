#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Verify complete computed file results through the real Python public client.

This bounded, renamed-schema fixture crosses the small collection row limit.
It certifies the specified SQL/DataFrame aggregate and sort writes only; it is
not a throughput benchmark or an engine-wide resource-envelope measurement.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
import sys

from local_uat_storage import GIB, MIB, check_budgets, require_local_path
from run_clickbench_query_uat import file_sha256, run_command, strict_json


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--uat-root", type=Path, required=True)
    parser.add_argument("--build-commit", required=True)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    root = require_local_path(args.uat_root, Path.home(), sys.platform)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    output = root / "logs" / f"native_result_stream_{stamp}"
    source = output / "shipments.vortex"
    row_count, first_key = 70_017, 13
    expected_count = row_count - first_key

    def guard() -> None:
        check_budgets(root, source, output, min_free_bytes=12 * GIB,
                      reserve_bytes=64 * MIB, max_workspace_bytes=100 * GIB,
                      max_log_bytes=192 * MIB)

    guard()
    root.mkdir(parents=True, exist_ok=True)
    summary = {
        "schema_version": "shardloom.native_result_stream_python_acceptance.v1",
        "status": "running", "build_commit": args.build_commit,
        "binary": str(binary), "binary_sha256": file_sha256(binary),
        "source_rows": row_count, "result_rows_per_case": expected_count,
        "cases": [], "fallback_attempted": False, "external_engine_invoked": False,
        "claim_gate_status": "not_claim_grade",
        "scope": "bounded real Python SQL/DataFrame read-filter-aggregate/order-write-reopen",
        "total_rss_bound": False, "performance_claim": False,
    }
    lock = root / ".ingest-uat.lock"
    lock.mkdir()
    client = None
    try:
        output.mkdir(parents=True)
        raw = output / "shipments.jsonl"
        with raw.open("x") as target:
            for key in range(row_count):
                target.write(json.dumps({"delivery_zone": key, "package_identifier": 1}) + "\n")
        command = [str(binary), "prepare", "dataframe", "--input", str(raw),
                   "--input-format", "jsonl", "--output", str(source),
                   "--memory-gb", "1", "--max-parallelism", "2", "--format", "json"]
        preparation = run_command(command, output / "prepare.stdout.json",
                                  output / "prepare.stderr.txt", 120, guard)
        envelope = strict_json((output / "prepare.stdout.json").read_text())
        if (preparation["returncode"] or preparation["guard_failures"]
                or envelope["status"] != "success" or envelope["fallback"]["attempted"]):
            raise ValueError("public compatibility-to-native preparation failed")
        summary["preparation"] = preparation
        summary["source_sha256"] = file_sha256(source)
        identity = source.stat()
        generation = (identity.st_dev, identity.st_ino, identity.st_size,
                      identity.st_mtime_ns, identity.st_ctime_ns)
        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python/src"))
        import shardloom as sl

        context = sl.context(binary=str(binary), cwd=output, timeout=120)
        client = context.client
        for operation in ("aggregate", "sort"):
            keys = range(first_key, row_count)
            expected = ([{"delivery_zone": key, "n": 1} for key in keys]
                        if operation == "aggregate" else
                        [{"delivery_zone": key, "package_identifier": 1}
                         for key in reversed(keys)])
            for surface in ("sql", "dataframe"):
                for extension in ("vortex", "jsonl"):
                    guard()
                    name = f"{surface}-{operation}-{extension}"
                    destination = output / f"{name}.{extension}"
                    if surface == "sql":
                        statement = (
                            f"SELECT delivery_zone, COUNT(*) AS n FROM '{source}' "
                            f"WHERE delivery_zone >= {first_key} GROUP BY delivery_zone "
                            f"ORDER BY delivery_zone ASC LIMIT {expected_count}"
                            if operation == "aggregate" else
                            f"SELECT * FROM '{source}' WHERE delivery_zone >= {first_key} "
                            f"ORDER BY delivery_zone DESC LIMIT {expected_count}"
                        )
                        workflow = context.sql(statement, input=source, input_format="vortex")
                    else:
                        workflow = context.read_vortex(source).filter(sl.col("delivery_zone") >= first_key)
                        if operation == "aggregate":
                            workflow = workflow.group_by("delivery_zone").count(alias="n")
                        workflow = workflow.sort("delivery_zone", descending=operation == "sort").limit(expected_count)
                    report = getattr(workflow, f"write_{extension}")(destination)
                    result = report.envelope
                    if (result.status != "success" or result.fallback.attempted
                            or result.field("public_workflow_fallback_attempted") != "false"
                            or result.field("public_workflow_external_engine_invoked") != "false"):
                        raise ValueError(f"{name}: public native execution evidence failed")
                    (output / f"{name}.envelope.json").write_text(json.dumps(result.raw, indent=2) + "\n")
                    decoded = destination
                    if extension == "vortex":
                        decoded = output / f"{name}-reopened.jsonl"
                        reopened = context.read_vortex(destination).write_jsonl(decoded).envelope
                        if reopened.status != "success" or reopened.fallback.attempted:
                            raise ValueError(f"{name}: native output reopen failed")
                        (output / f"{name}.reopen-envelope.json").write_text(json.dumps(reopened.raw, indent=2) + "\n")
                    actual = [strict_json(line) for line in decoded.read_text().splitlines()]
                    if actual != expected:
                        raise ValueError(f"{name}: complete ordered values differ, {len(actual)} rows")
                    summary["cases"].append({"name": name, "passed": True,
                                             "complete_rows_verified": len(actual),
                                             "output_sha256": file_sha256(destination)})
                    print(f"PASS {name}: {len(actual)} complete rows", flush=True)
        after = source.stat()
        if ((after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns) != generation
                or file_sha256(source) != summary["source_sha256"]):
            raise ValueError("source changed during acceptance")
        summary["status"] = "passed"
        guard()
        return 0
    except BaseException as error:
        summary["status"] = "failed"
        summary["error"] = f"{type(error).__name__}: {error}"
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
    raise SystemExit(main())
