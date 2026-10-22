#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Exercise the native-exchange report verifier against frozen positive and negative evidence."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
from pathlib import Path
import sys
from typing import Any, Callable

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
from native_exchange_result_checks import verify_workflow  # noqa: E402


def identity(path: Path) -> dict[str, Any]:
    before = path.stat()
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            digest.update(block)
    after = path.stat()
    before_identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    after_identity = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if before_identity != after_identity:
        raise ValueError(f"input changed while hashing: {path}")
    return {"path": str(path), "bytes": after.st_size, "sha256": digest.hexdigest()}


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as source:
        return json.load(source)


def rows_for_mode(records: list[dict[str, Any]], mode: str) -> dict[str, Any]:
    matches = [record for record in records if record.get("configuration", {}).get("mode") == mode]
    if not matches:
        raise ValueError(f"no positive source workflow for mode {mode!r}")
    return matches[0]


def run_negative_matrix(
    records: list[dict[str, Any]], manifest: dict[str, Any], freeze: dict[str, Any]
) -> list[dict[str, Any]]:
    native_fragments = rows_for_mode(records, "native_fragments")
    native_assembled = rows_for_mode(records, "native_assembled")
    arrow_zstd = rows_for_mode(records, "arrow_zstd")
    sql = rows_for_mode(records, "sql")

    def set_path(record: dict[str, Any], path: tuple[str | int, ...], value: Any) -> None:
        node: Any = record
        for key in path[:-1]:
            node = node[key]
        node[path[-1]] = value

    mutations: list[tuple[str, dict[str, Any], Callable[[dict[str, Any]], None], str]] = [
        ("missing_configuration_hash", native_fragments,
         lambda row: row.pop("configuration_sha256"), "record.configuration_sha256"),
        ("changed_configuration_hash", native_fragments,
         lambda row: row.__setitem__("configuration_sha256", "0" * 64), "record.configuration_sha256"),
        ("equal_child_pids", native_fragments,
         lambda row: row.__setitem__("receiver_pid", row["sender_pid"]), "record.receiver_pid"),
        ("sender_reaped_before_consume_false", native_fragments,
         lambda row: row.__setitem__("sender_reaped_before_consume", False), "record.sender_reaped_before_consume"),
        ("received_before_consumer_already_executed", native_fragments,
         lambda row: set_path(row, ("received_before_consumer", "completed_executions"), 1),
         "record.received_before_consumer.completed_executions"),
        ("owned_processes_not_drained", native_fragments,
         lambda row: row.__setitem__("owned_processes_drained", False), "record.owned_processes_drained"),
        ("sender_reservation_not_released", native_fragments,
         lambda row: set_path(row, ("sender", "final_reserved_bytes"), 1), "record.sender.final_reserved_bytes"),
        ("service_queue_not_drained", native_fragments,
         lambda row: set_path(row, ("sender", "service", "queued_bytes"), 1), "record.sender.service.queued_bytes"),
        ("sender_connection_not_released", native_fragments,
         lambda row: set_path(row, ("sender", "final_live_connections"), 1), "record.sender.final_live_connections"),
        ("native_query_digest_changed", native_fragments,
         lambda row: set_path(row, ("receiver", "consumer", "queries", 0, "complete_jsonl_sha256"), "0" * 64),
         "record.receiver.consumer.queries[0].complete_jsonl_sha256"),
        ("native_query_dtype_changed", native_fragments,
         lambda row: set_path(row, ("receiver", "consumer", "queries", 0, "dtype"), "wrong dtype"),
         "record.receiver.consumer.queries[0].dtype"),
        ("native_query_order_reversed", native_fragments,
         lambda row: row["receiver"]["consumer"]["queries"].reverse(),
         "record.receiver.consumer.queries[0].query"),
        ("native_query_engine_time_nan", native_fragments,
         lambda row: set_path(row, ("receiver", "consumer", "queries", 0, "engine_execution_seconds"), math.nan),
         "engine_execution_seconds"),
        ("rss_sample_unknown_pid", native_fragments,
         lambda row: set_path(row, ("resources", "rss_samples", 0, "processes", 0, "pid"), 2**31),
         "unknown or duplicate process"),
        ("rss_sample_duplicate_pid", native_fragments,
         lambda row: set_path(row, ("resources", "rss_samples", 0, "processes"),
                              [row["resources"]["rss_samples"][0]["processes"][0],
                               row["resources"]["rss_samples"][0]["processes"][0]]),
         "unknown or duplicate process"),
        ("sampled_peak_increased", native_fragments,
         lambda row: set_path(row, ("resources", "peak_combined_sampled_rss_bytes"),
                              row["resources"]["peak_combined_sampled_rss_bytes"] + 1),
         "peak_combined_sampled_rss_bytes"),
        ("receiver_fallback_attempted", native_fragments,
         lambda row: set_path(row, ("receiver", "fallback_attempted"), True), "record.receiver.fallback_attempted"),
        ("provider_copy_bytes_reported", native_fragments,
         lambda row: set_path(row, ("receiver", "provider_copy_bytes"), 0), "record.receiver.provider_copy_bytes"),
        ("receiver_wire_bytes_changed", arrow_zstd,
         lambda row: set_path(row, ("receiver", "receive", "application_wire_bytes"),
                              row["receiver"]["receive"]["application_wire_bytes"] + 1),
         "application wire bytes"),
        ("native_fragments_sender_assembly_nonzero", native_fragments,
         lambda row: set_path(row, ("sender", "service", "transfers", 0, "send", "complete_message_assembly_bytes"), 1),
         "complete_message_assembly_bytes"),
        ("native_assembled_sender_assembly_zero", native_assembled,
         lambda row: set_path(row, ("sender", "service", "transfers", 0, "send", "complete_message_assembly_bytes"), 0),
         "complete_message_assembly_bytes"),
        ("sql_unsupported_sql_not_refused", sql,
         lambda row: set_path(row, ("receiver", "receive", "unsupported_sql_refused"), False),
         "unsupported_sql_refused"),
        ("sql_client_identity_changed", sql,
         lambda row: set_path(row, ("receiver", "receive", "client"), "wrong client"),
         "record.receiver.receive.client"),
        ("sql_sender_native_execution_missing", sql,
         lambda row: set_path(row, ("sender", "service", "transfers", 0, "send", "native_execution", "completed_executions"), 0),
         "native_execution.completed_executions"),
    ]

    results = []
    for name, source, mutate, expected_failure in mutations:
        candidate = copy.deepcopy(source)
        try:
            mutate(candidate)
            verify_workflow(candidate, manifest, freeze)
        except ValueError as error:
            detail = str(error)
            rejected_at_expected_check = expected_failure in detail
            results.append({"mutation": name, "rejected": True, "expected_failure": expected_failure,
                            "diagnostic": detail, "specific_check_matched": rejected_at_expected_check})
        except Exception as error:  # unexpected failures must be visible and fail the matrix
            results.append({"mutation": name, "rejected": False, "expected_failure": expected_failure,
                            "diagnostic": f"unexpected {type(error).__name__}: {error}",
                            "specific_check_matched": False})
        else:
            results.append({"mutation": name, "rejected": False, "expected_failure": expected_failure,
                            "diagnostic": "mutation was accepted", "specific_check_matched": False})
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", action="append", required=True, type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--freeze", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    inputs = [*args.report, args.manifest, args.freeze]
    if len(args.report) != 2:
        parser.error("exactly two --report inputs are required (65 workflows and 2 workflows)")
    for path in inputs:
        if not path.is_absolute() or not path.is_file():
            parser.error(f"input must be an existing absolute file: {path}")
    if not args.output.is_absolute():
        parser.error("--output must be an absolute path")
    if args.output.exists():
        parser.error(f"--output must be fresh and must not already exist: {args.output}")
    if not args.output.parent.is_dir():
        parser.error(f"--output parent directory must exist: {args.output.parent}")

    input_identities = [identity(path) for path in inputs]
    reports = [load_json(path) for path in args.report]
    manifest = load_json(args.manifest)
    freeze = load_json(args.freeze)
    all_records: list[dict[str, Any]] = []
    report_results = []
    for path, report in zip(args.report, reports):
        if not isinstance(report, dict) or report.get("status") != "passed":
            raise ValueError(f"report must have top-level status=passed: {path}")
        rows = report.get("workflows")
        if not isinstance(rows, list):
            raise ValueError(f"report workflows must be an array: {path}")
        verified_rows = []
        for index, row in enumerate(rows):
            if not isinstance(row, dict) or row.get("status") != "passed":
                raise ValueError(f"report row {index} must have status=passed: {path}")
            verified_rows.append(verify_workflow(row, manifest, freeze))
        report_results.append({"path": str(path), "workflow_count": len(rows),
                               "positive_verification_count": len(verified_rows),
                               "verifier_checks": sum(row["total_checks"] for row in verified_rows)})
        all_records.extend(rows)
    sorted_counts = sorted(len(report.get("workflows", [])) for report in reports)
    if sorted_counts != [2, 65]:
        raise ValueError(f"expected exact input report workflow counts [65, 2], got {sorted_counts}")

    mutations = run_negative_matrix(all_records, manifest, freeze)
    if len(mutations) != 24:
        raise ValueError("negative matrix must contain exactly 24 required mutations")
    rejected = sum(result["rejected"] and result["specific_check_matched"] for result in mutations)
    failures = [result for result in mutations if not result["rejected"] or not result["specific_check_matched"]]
    if [identity(path) for path in inputs] != input_identities:
        raise ValueError("one or more verifier inputs changed during verification")
    result = {
        "version": 1,
        "status": "failed" if failures else "passed",
        "checker": identity(Path(__file__)),
        "verifier": identity(SCRIPT_DIR / "native_exchange_result_checks.py"),
        "inputs": [{**item, "role": role} for item, role in zip(
            input_identities, ["report"] * len(args.report) + ["manifest", "freeze"]
        )],
        "positive_verification": {
            "report_count": len(report_results), "workflow_count": len(all_records),
            "verified_count": sum(item["positive_verification_count"] for item in report_results),
            "verifier_check_count": sum(item["verifier_checks"] for item in report_results),
            "reports": report_results,
        },
        "negative_matrix": {"required_count": 24, "executed_count": len(mutations),
                             "rejected_at_specific_check_count": rejected, "cases": mutations},
        "failures": failures,
    }
    with args.output.open("x", encoding="utf-8") as output:
        json.dump(result, output, indent=2, sort_keys=True, allow_nan=False)
        output.write("\n")
    print(json.dumps({"status": result["status"], "positive_workflows": len(all_records),
                      "negative_cases": len(mutations), "output": str(args.output)}, allow_nan=False))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
